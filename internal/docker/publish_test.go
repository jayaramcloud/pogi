package docker

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// publishEngine answers inspect without a host port for 8161 until late inspects have passed (never, when late
// is negative), the way Docker does for a moment after a start or restart under load.
func publishEngine(t *testing.T, late int32, running bool) (*Client, *atomic.Int32) {
	var inspects atomic.Int32
	_, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/json") {
			w.WriteHeader(http.StatusNoContent) // start, restart
			return
		}
		port := ""
		if n := inspects.Add(1); late >= 0 && n > late {
			port = "55973"
		}
		fmt.Fprintf(w, `{"State":{"Running":%v},"HostConfig":{"PortBindings":{"8161/tcp":[{"HostIp":"127.0.0.1","HostPort":""}]}},`+
			`"NetworkSettings":{"Ports":{"8161/tcp":[{"HostIp":"127.0.0.1","HostPort":%q}]}}}`, running, port)
	})
	return c, &inspects
}

// The ActiveMQ reboot flake: an Inspect right after Restart read port 0 for the web console, and pogi waited on
// 127.0.0.1:0 for 5 minutes. Start and Restart now return once every published port has its host port.
func TestStartWaitsForPublishedPorts(t *testing.T) {
	ctx := context.Background()
	for _, call := range []func(*Client) error{
		func(c *Client) error { return c.Start(ctx, "b") },
		func(c *Client) error { return c.Restart(ctx, "b") },
	} {
		c, _ := publishEngine(t, 3, true)
		if err := call(c); err != nil {
			t.Fatal(err)
		}
		in, err := c.Inspect(ctx, "b")
		if err != nil || in.HostPort(8161) != 55973 {
			t.Fatalf("after start: port %d, %v", in.HostPort(8161), err)
		}
	}
}

func TestStartSaysWhenAPortIsNeverPublished(t *testing.T) {
	defer func(d time.Duration) { publishWait = d }(publishWait)
	publishWait = 200 * time.Millisecond
	c, _ := publishEngine(t, -1, true)
	if err := c.Start(context.Background(), "b"); err == nil || !strings.Contains(err.Error(), "did not publish its port 8161/tcp") {
		t.Fatalf("got %v", err)
	}
	// a container that stopped at once is the caller's to explain (its watcher says why), not a wait
	c, n := publishEngine(t, -1, false)
	if err := c.Start(context.Background(), "b"); err != nil || n.Load() != 1 {
		t.Fatalf("stopped container: %v after %d inspects", err, n.Load())
	}
}

// A container another call is already removing (409) is waited for: its named volume is in use until it is gone,
// and the volume removal right after Remove returned would fail. Box.Close racing a delete leaked volumes so.
func TestRemoveWaitsForARemovalInProgress(t *testing.T) {
	var inspects atomic.Int32
	_, c := failEngine(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete:
			w.WriteHeader(http.StatusConflict)
			fmt.Fprint(w, `{"message":"removal of container db is already in progress"}`)
		case inspects.Add(1) < 4: // still there for three inspects
			fmt.Fprint(w, `{"State":{"Status":"removing"}}`)
		default:
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, `{"message":"No such container: db"}`)
		}
	})
	if err := c.Remove(context.Background(), "db"); err != nil || inspects.Load() != 4 {
		t.Fatalf("Remove: %v after %d inspects", err, inspects.Load())
	}
}
