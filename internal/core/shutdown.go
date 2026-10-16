package core

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"text/tabwriter"
	"time"
)

// shutdownLog collects what pogi stops on the way out, for the list the pogi
// command prints. It records nothing until BeginShutdown, so a process that
// stops while pogi runs (a deleted function) is not listed.
type shutdownLog struct {
	mu    sync.Mutex
	start time.Time
	took  time.Duration
	items []closed
}

// closed is one thing that was stopped: noun is container, process or port.
type closed struct {
	service, noun, verb, name string
	port                      int
}

// BeginShutdown starts the clock and the record; Run calls it once the stop signal came.
func (h *Hub) BeginShutdown() {
	h.shut.mu.Lock()
	defer h.shut.mu.Unlock()
	if h.shut.start.IsZero() {
		h.shut.start = time.Now()
	}
}

// Closed records that a service stopped a local process, or removed a
// container (noun "process" or "container", verb "stopped" or "removed").
func (h *Hub) Closed(service, noun, verb, name string) {
	h.record(closed{service: service, noun: noun, verb: verb, name: name})
}

// ClosedPort records a TCP port that pogi opened for a service and closed; name says what it served.
func (h *Hub) ClosedPort(service, name string, port int) {
	h.record(closed{service: service, noun: "port", verb: "closed", name: name, port: port})
}

func (h *Hub) record(c closed) {
	if h == nil {
		return
	}
	h.shut.mu.Lock()
	defer h.shut.mu.Unlock()
	if !h.shut.start.IsZero() {
		h.shut.items = append(h.shut.items, c)
	}
}

// shuttingDown reports whether BeginShutdown was called.
func (h *Hub) shuttingDown() bool {
	h.shut.mu.Lock()
	defer h.shut.mu.Unlock()
	return !h.shut.start.IsZero()
}

// ShutdownList is the text for the end of a run: what was stopped, where the
// state was saved and how long it took. It is empty if shutdown never began.
func (h *Hub) ShutdownList() string {
	h.shut.mu.Lock()
	defer h.shut.mu.Unlock()
	if h.shut.start.IsZero() {
		return ""
	}
	state := ""
	if len(h.stores) > 0 {
		state = h.Cfg.DataDir
	}
	return formatShutdown(h.shut.items, state, h.shut.took)
}

// endShutdown fixes the time shutdown took.
func (h *Hub) endShutdown() {
	h.shut.mu.Lock()
	defer h.shut.mu.Unlock()
	if !h.shut.start.IsZero() {
		h.shut.took = time.Since(h.shut.start)
	}
}

// formatShutdown lays out the list: one line per service and kind of thing,
// then the state dir (if state was saved) and the time.
func formatShutdown(items []closed, stateDir string, took time.Duration) string {
	type key struct {
		service, noun, verb string
		port                int
	}
	var keys []key
	names := map[key][]string{}
	for _, c := range items {
		k := key{c.service, c.noun, c.verb, c.port}
		if _, ok := names[k]; !ok {
			keys = append(keys, k)
		}
		names[k] = append(names[k], c.name)
	}
	slices.SortStableFunc(keys, func(a, b key) int {
		return strings.Compare(a.service+"\x00"+a.noun, b.service+"\x00"+b.noun)
	})
	var b strings.Builder
	b.WriteString("pogi stopped:\n")
	tw := tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
	for _, k := range keys {
		n := names[k]
		what := fmt.Sprintf("%d %s %s", len(n), plural(k.noun, len(n)), k.verb)
		if k.port > 0 {
			what = fmt.Sprintf("port %d %s", k.port, k.verb)
		}
		fmt.Fprintf(tw, "  %s\t%s\t%s\n", k.service, what, nameList(n))
	}
	tw.Flush()
	if stateDir != "" {
		fmt.Fprintf(&b, "  state saved to %s\n", stateDir)
	}
	fmt.Fprintf(&b, "  done in %.1fs\n", took.Seconds())
	return b.String()
}

func plural(noun string, n int) string {
	switch {
	case n == 1:
		return noun
	case strings.HasSuffix(noun, "s"):
		return noun + "es"
	}
	return noun + "s"
}

// nameList joins names (sorted) for a line of output: the first five, then "+N more".
func nameList(names []string) string {
	names = slices.Sorted(slices.Values(names))
	if len(names) <= 5 {
		return strings.Join(names, ", ")
	}
	return strings.Join(names[:5], ", ") + fmt.Sprintf(", +%d more", len(names)-5)
}
