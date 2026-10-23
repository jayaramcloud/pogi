package lambda

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The Lambda Extensions API, served beside the Runtime API of each instance. An extension registers for
// INVOKE and SHUTDOWN events and then asks for them one at a time. Executables in a layer's extensions/
// folder (external extensions) are started with the runtime; an extension that lives in the runtime's
// own process (an internal one) registers the same way.
//
// ponytail: an invocation does not wait for the extensions to finish their INVOKE work, and the Logs and
// Telemetry APIs are not served.

const extHeader = "Lambda-Extension-Identifier"

// maxExtensions is how many extensions one instance accepts, as on AWS.
const maxExtensions = 10

// shutdownWait is how long external extensions get to stop after SHUTDOWN, as on AWS.
const shutdownWait = 2 * time.Second

type extension struct {
	name              string
	events            map[string]bool
	ch                chan []byte // events not yet asked for
	polling           bool        // it has asked for an event, so it has an event loop
	registered, ready time.Time   // when it registered, and first asked for an event (its init done)
}

// extensions is the Extensions API state of one instance; the zero value is ready to use.
type extensions struct {
	mu       sync.Mutex
	list     map[string]*extension // by identifier
	procs    []*extProc            // external extensions this instance started
	expected int                   // processes that will register
	ready    chan struct{}         // one signal per process that asked for its first event, or ended
	waited   sync.Once
}

type extProc struct {
	cmd  *exec.Cmd
	done chan struct{}
}

func (e *extensions) init(expected int) {
	e.mu.Lock()
	e.expected, e.ready = expected, make(chan struct{}, 2*expected+1)
	e.mu.Unlock()
}

// signalReady says one extension process is past its init (or gone).
func (e *extensions) signalReady() {
	e.mu.Lock()
	ready := e.ready
	e.mu.Unlock()
	select {
	case ready <- struct{}{}:
	default:
	}
}

// waitInit holds the first invocation until every extension process has registered and asked for its first
// event, or ended, as Lambda's init phase does; it gives up after limit (lambda.initTimeout).
func (e *extensions) waitInit(ctx context.Context, dead <-chan struct{}, limit time.Duration) {
	e.waited.Do(func() {
		e.mu.Lock()
		expected, ready := e.expected, e.ready
		e.mu.Unlock()
		timeout := time.After(limit)
		for range expected {
			select {
			case <-ready:
			case <-timeout:
				return
			case <-dead:
				return
			case <-ctx.Done():
				return
			}
		}
	})
}

// broadcast gives an event to every extension that registered for its type. An extension that is not
// asking for events (the buffer is full) misses them.
func (e *extensions) broadcast(typ string, ev any) {
	body, _ := json.Marshal(ev)
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, x := range e.list {
		if x.events[typ] {
			select {
			case x.ch <- body:
			default:
			}
		}
	}
}

// invoke tells the extensions about an invocation.
func (e *extensions) invoke(inv *invocation, trace string) {
	e.broadcast("INVOKE", map[string]any{"eventType": "INVOKE", "deadlineMs": inv.deadline.UnixMilli(), "requestId": inv.id, "invokedFunctionArn": inv.arn,
		"tracing": map[string]string{"type": "X-Amzn-Trace-Id", "value": trace}})
}

// shutdown sends SHUTDOWN, gives the extensions two seconds to take it (and the external ones to stop) and
// then ends them. Extensions that never asked for an event (internal ones with no event loop) are not waited for.
// ponytail: an extension in a container gets 100 ms after it takes SHUTDOWN, as pogi cannot see it exit.
func (e *extensions) shutdown(reason string) {
	e.mu.Lock()
	procs := e.procs
	var polling []*extension
	for _, x := range e.list {
		if x.polling && x.events["SHUTDOWN"] {
			polling = append(polling, x)
		}
	}
	e.mu.Unlock()
	e.broadcast("SHUTDOWN", map[string]any{"eventType": "SHUTDOWN", "shutdownReason": reason, "deadlineMs": time.Now().Add(shutdownWait).UnixMilli()})
	deadline := time.Now().Add(shutdownWait)
	for _, x := range polling {
		for len(x.ch) > 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	if len(polling) > 0 && len(procs) == 0 {
		time.Sleep(100 * time.Millisecond)
	}
	for _, p := range procs {
		select {
		case <-p.done:
		case <-time.After(time.Until(deadline)):
		}
		core.KillGroup(p.cmd)
	}
}

// extError is an error body of the Extensions API.
func extError(w http.ResponseWriter, status int, typ, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]string{"errorMessage": msg, "errorType": typ})
}

// mountExtensions adds the Extensions API to an instance's Runtime API.
func (in *instance) mountExtensions(m *http.ServeMux) {
	e := &in.ext
	m.HandleFunc("POST /2020-01-01/extension/register", func(w http.ResponseWriter, r *http.Request) {
		name := r.Header.Get("Lambda-Extension-Name")
		var body struct{ Events []string }
		if b, _ := io.ReadAll(io.LimitReader(r.Body, 1<<16)); len(b) > 0 {
			if err := json.Unmarshal(b, &body); err != nil {
				extError(w, http.StatusBadRequest, "InvalidRequest", "The register body is not valid JSON.")
				return
			}
		}
		x := &extension{name: name, events: map[string]bool{}, ch: make(chan []byte, 64), registered: time.Now()}
		for _, ev := range body.Events {
			if ev != "INVOKE" && ev != "SHUTDOWN" {
				extError(w, http.StatusBadRequest, "Extension.InvalidEventType", "Unknown event type "+ev+": use INVOKE or SHUTDOWN.")
				return
			}
			x.events[ev] = true
		}
		e.mu.Lock()
		defer e.mu.Unlock()
		switch {
		case name == "" || strings.ContainsAny(name, "/\\"):
			extError(w, http.StatusBadRequest, "Extension.InvalidExtensionName", "The Lambda-Extension-Name header is missing or not valid.")
			return
		case len(e.list) >= maxExtensions:
			extError(w, http.StatusBadRequest, "Extension.MaxExtensionsReached", "A function can have at most 10 extensions.")
			return
		}
		id := core.UUID()
		if e.list == nil {
			e.list = map[string]*extension{}
		}
		e.list[id] = x
		w.Header().Set(extHeader, id)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"functionName": in.spec.name, "functionVersion": in.spec.version, "handler": in.spec.handler})
	})
	m.HandleFunc("GET /2020-01-01/extension/event/next", func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		x := e.list[r.Header.Get(extHeader)]
		e.mu.Unlock()
		if x == nil {
			extError(w, http.StatusForbidden, "Extension.InvalidExtensionId", "The Lambda-Extension-Identifier header is missing or not a registered extension.")
			return
		}
		e.mu.Lock()
		if !x.polling {
			x.polling, x.ready = true, time.Now()
		}
		e.mu.Unlock()
		e.signalReady() // an extension that asks for events has finished its init; more signals than processes do no harm
		select {
		case ev := <-x.ch:
			var id struct{ RequestId string }
			if json.Unmarshal(ev, &id) == nil && id.RequestId != "" {
				w.Header().Set("Lambda-Extension-Event-Identifier", id.RequestId)
			}
			w.Header().Set("Content-Type", "application/json")
			w.Write(ev)
		case <-r.Context().Done():
		case <-in.dead:
		}
	})
	for _, kind := range []string{"init", "exit"} {
		m.HandleFunc("POST /2020-01-01/extension/"+kind+"/error", func(w http.ResponseWriter, r *http.Request) {
			e.mu.Lock()
			x := e.list[r.Header.Get(extHeader)]
			e.mu.Unlock()
			if x == nil {
				extError(w, http.StatusForbidden, "Extension.InvalidExtensionId", "The Lambda-Extension-Identifier header is missing or not a registered extension.")
				return
			}
			if r.Header.Get("Lambda-Extension-Function-Error-Type") == "" {
				extError(w, http.StatusBadRequest, "Extension.MissingErrorType", "The Lambda-Extension-Function-Error-Type header is required.")
				return
			}
			io.Copy(io.Discard, r.Body)
			in.log("EXTENSION Name: " + x.name + " State: Error")
			accepted(w)
		})
	}
}

// extensionFiles lists the executables in a layer directory's extensions folder.
func extensionFiles(opt string) []string {
	if opt == "" {
		return nil
	}
	entries, _ := os.ReadDir(filepath.Join(opt, "extensions"))
	var out []string
	for _, f := range entries {
		if !f.IsDir() {
			out = append(out, f.Name())
		}
	}
	return out
}

// startExtensions runs the external extensions of a native function beside its runtime; their output goes
// to the function's log. It returns how many it started.
func (p *pool) startExtensions(s *spec, in *instance, env map[string]string, opt string) {
	files := extensionFiles(opt)
	in.ext.init(len(files))
	for _, name := range files {
		cmd := exec.Command(filepath.Join(opt, "extensions", name))
		cmd.Dir = opt
		cmd.Env = hostEnv()
		for k, v := range env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		for k, v := range s.env {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
		stdout, _ := cmd.StdoutPipe()
		cmd.Stderr = cmd.Stdout
		proc := &extProc{cmd: cmd, done: make(chan struct{})}
		core.OwnGroup(cmd)
		if err := cmd.Start(); err != nil {
			in.log("EXTENSION Name: " + name + " State: Error Cause: " + err.Error())
			in.ext.signalReady()
			continue
		}
		in.ext.mu.Lock()
		in.ext.procs = append(in.ext.procs, proc)
		in.ext.mu.Unlock()
		core.Go("lambda.startExtensions", func() {
			defer func() {
				close(proc.done)
				in.ext.signalReady() // an extension that ended will not register
			}()
			sc := bufio.NewScanner(stdout)
			sc.Buffer(make([]byte, 64<<10), 1<<20)
			for sc.Scan() {
				in.log(sc.Text())
			}
			cmd.Wait()
		})
	}
}

// dockerExtensions starts the extensions of a function's layers inside its container, before the runtime's
// own entrypoint, with a small wrapper script.
// ponytail: only layers of zip functions (the extensions folder is on the host); a container image's own
// /opt/extensions are not started.
func dockerExtensions(opt string) (entrypoint []string, n int) {
	n = len(extensionFiles(opt))
	if n == 0 {
		return nil, 0
	}
	script := `for f in /opt/extensions/*; do [ -f "$f" ] && "$f" & done; exec /lambda-entrypoint.sh "$@"`
	return []string{"/bin/sh", "-c", script, "--"}, n
}

// shutdownReason is what SHUTDOWN tells the extensions.
func (in *instance) shutdownReason() string {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.reason != "" {
		return in.reason
	}
	select {
	case <-in.dead:
		return "failure"
	default:
		return "spindown"
	}
}
