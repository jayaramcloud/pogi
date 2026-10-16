package generic

import (
	"archive/tar"
	"bytes"
	"cmp"
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// A box is the real containers of one resource (a Flink cluster, an Airflow
// environment) and the background work that starts and watches them. The
// resource keeps the box's id in Data["_box"]; its status follows the box.
// Without a container engine nothing starts and the service keeps its timed
// status (see the Derive of each service).
type box struct {
	mu     sync.Mutex
	state  string // starting, running, stopping, stopped, failed
	ready  bool   // it was running once
	addr   string // where the service of the containers answers on this machine, set with ready
	err    error
	cancel context.CancelFunc
	done   chan struct{}
}

type boxEntry struct {
	svc *Service
	*box
}

var boxes sync.Map // id → *boxEntry

// boxInfo is what a box is doing: its state ("" for no box, for instance after a restart) and, when failed, why.
type boxInfo struct {
	state, addr string
	ready       bool
	err         error
}

func boxOf(id string) boxInfo {
	e, ok := boxes.Load(id)
	if !ok {
		return boxInfo{}
	}
	b := e.(*boxEntry).box
	b.mu.Lock()
	defer b.mu.Unlock()
	return boxInfo{b.state, b.addr, b.ready, b.err}
}

// runBox runs up in the background and returns the box's id. up starts the containers, calls ready(addr)
// when they work, then watches them until its context ends (a stop) or they die (an error).
// Whatever up did, the containers called names are removed when it returns.
// Boxes live in memory and the containers are removed on shutdown, even when state is kept. After a
// restart a resource has no box until resume starts it again.
func (s *Service) runBox(d *docker.Client, names []string, up func(ctx context.Context, ready func(addr string)) error) string {
	id := core.UUID()
	ctx, cancel := context.WithCancel(context.Background())
	b := &box{state: "starting", cancel: cancel, done: make(chan struct{})}
	boxes.Store(id, &boxEntry{s, b})
	core.Go("generic.runBox", func() {
		defer close(b.done)
		err := up(ctx, func(addr string) {
			b.mu.Lock()
			if b.state == "starting" {
				b.state, b.ready, b.addr = "running", true, addr
			}
			b.mu.Unlock()
		})
		rm, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		for _, n := range names {
			d.Remove(rm, n)
		}
		b.mu.Lock()
		defer b.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			b.state, b.err = "failed", err
			s.h.Log.Warn("containers did not start", "service", s.meta.Name, "containers", names, "err", err)
		} else {
			b.state = "stopped"
		}
	})
	return id
}

// after stops the box prev (if there is one) and returns up changed to begin only once prev's
// containers are gone, so the two never use the same container name.
func after(prev string, up func(context.Context, func(string)) error) func(context.Context, func(string)) error {
	e, ok := boxes.Load(prev)
	if !ok {
		return up
	}
	stopBox(prev)
	done := e.(*boxEntry).done
	return func(ctx context.Context, ready func(string)) error {
		select {
		case <-done:
		case <-ctx.Done():
			return nil
		}
		return up(ctx, ready)
	}
}

// resume starts again, in the background, the resources of type typ that wasUp says had containers
// when pogi stopped and that have no box now. It uses start, the code of the API call. It waits until
// pogi serves (the containers call it), and does nothing without a container engine.
func (s *Service) resume(typ string, wasUp func(*Resource) bool, start func(c *Call, r *Resource) error) {
	if s.h == nil || !s.h.Docker().Available() {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.stops = append(s.stops, cancel) // Close ends the wait
	s.mu.Unlock()
	core.Go("generic.resume", func() {
		m, err := s.model()
		if err != nil || !until(ctx, time.Minute, s.h.Ready.Load) {
			return
		}
		var todo []*Resource
		s.mu.Lock()
		for _, r := range s.st.Res {
			if r.Type == typ && wasUp(r) && boxOf(str(r.Data["_box"])).state == "" {
				todo = append(todo, r.clone())
			}
		}
		s.mu.Unlock()
		for _, r := range todo {
			c := &Call{Svc: s, Account: r.Account, Region: r.Region, m: m}
			if err := start(c, r); err != nil {
				s.h.Log.Warn("could not start the containers again", "service", s.meta.Name, "resource", r.Name, "err", err)
				continue
			}
			s.mu.Lock()
			_, still := s.st.Res[r.skey()]
			if still {
				s.st.Res[r.skey()] = r
			}
			s.mu.Unlock()
			if !still { // deleted while its containers were starting
				stopBox(str(r.Data["_box"]))
			}
		}
	})
}

// stopBox tells a box to stop; its containers are gone when its state is stopped.
func stopBox(id string) {
	if e, ok := boxes.Load(id); ok {
		b := e.(*boxEntry).box
		b.mu.Lock()
		if b.state == "starting" || b.state == "running" {
			b.state = "stopping"
		}
		b.mu.Unlock()
		b.cancel()
	}
}

// closeBoxes stops every box of the service and waits (a little) for the containers to go.
func (s *Service) closeBoxes() {
	var wait []chan struct{}
	boxes.Range(func(id, v any) bool {
		if e := v.(*boxEntry); e.svc == s {
			stopBox(id.(string))
			wait = append(wait, e.done)
			boxes.Delete(id)
		}
		return true
	})
	end := time.After(30 * time.Second)
	for _, w := range wait {
		select {
		case <-w:
		case <-end:
			return
		}
	}
}

// podEnv is the environment that points a container's AWS SDK at pogi (with the account's
// test credentials), and whether the container must share the host network to reach it.
func (c *Call) podEnv(d *docker.Client) (env []string, host bool) {
	h := c.Hub()
	u, _ := url.Parse(cmp.Or(h.URL, "http://localhost:"+strconv.Itoa(cmp.Or(h.Cfg.Port, 7644))))
	addr := d.HostAddr()
	return core.AWSEnv("http://"+addr+":"+u.Port(), c.Region), addr == "127.0.0.1"
}

// tarFiles is a tar archive of files (path without a leading slash → content), for docker.CopyTo at "/".
func tarFiles(files map[string][]byte) io.Reader {
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	for name, data := range files {
		tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), ModTime: time.Now()})
		tw.Write(data)
	}
	tw.Close()
	return &b
}

// gone returns an error when the container is not running (and ctx is not over).
func gone(ctx context.Context, d *docker.Client, name string) error {
	if i, err := d.Inspect(ctx, name); ctx.Err() == nil && (err != nil || !i.State.Running) {
		return fmt.Errorf("container %s is not running (docker logs %s tells why)", name, name)
	}
	return nil
}

// logTail is the last lines of a container's output, for an error message.
func logTail(d *docker.Client, name string) string {
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	var ring []string
	d.Logs(ctx, name, false, func(l string) {
		if ring = append(ring, l); len(ring) > 8 {
			ring = ring[1:]
		}
	})
	return fmt.Sprint(ring)
}

// until calls ok every second until it is true, the time is up or ctx ends; it reports whether ok was true.
func until(ctx context.Context, max time.Duration, ok func() bool) bool {
	for end := time.Now().Add(max); ; {
		if ok() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(time.Second):
		}
		if time.Now().After(end) {
			return false
		}
	}
}

// dig is the value at a path of keys in nested maps, or nil.
func dig(v any, keys ...string) any {
	for _, k := range keys {
		v = asMap(v)[k]
	}
	return v
}

// freePort is a TCP port nobody listens on.
func freePort() int {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
