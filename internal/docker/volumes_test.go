package docker

// Data volumes of killed pogis (real engine). Every container and volume here carries the label
// pogi.vol=<tag>, and the Sweep under test sees the engine only through a proxy that shows nothing else,
// so nobody else's containers or volumes are touched.

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"
	"time"
)

func volTag() string { return rand.Text()[:10] }

func volCleanup(t *testing.T, tg string) {
	t.Cleanup(func() {
		out, _ := exec.Command("docker", "ps", "-aq", "--filter", "label=pogi.vol="+tg).Output()
		for _, id := range strings.Fields(string(out)) {
			exec.Command("docker", "rm", "-f", id).Run()
		}
		out, _ = exec.Command("docker", "volume", "ls", "-q", "--filter", "label=pogi.vol="+tg).Output()
		for _, v := range strings.Fields(string(out)) {
			exec.Command("docker", "volume", "rm", "-f", v).Run()
		}
	})
}

func volLeft(tg string) []string {
	out, _ := exec.Command("docker", "volume", "ls", "-q", "--filter", "label=pogi.vol="+tg).Output()
	return strings.Fields(string(out))
}

// TestVolChild is the pogi that gets killed: it runs one container with a data volume and waits.
func TestVolChild(t *testing.T) {
	tg := os.Getenv("POGI_VOL_TAG")
	if tg == "" {
		t.Skip("helper of the kill -9 tests")
	}
	opt := Options{}
	if d := os.Getenv("POGI_VOL_DATA"); d != "" {
		opt.Keep, opt.DataDir = true, d
	}
	name := Name("vol", tg)
	_, err := New(opt).Run(context.Background(), &Container{Name: name, Image: "alpine/socat:latest", Service: "vol", Resource: tg,
		Labels: map[string]string{"pogi.vol": tg}, Binds: []string{name + "-data:/data"}, Entrypoint: []string{"sleep"}, Cmd: []string{"600"}})
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(1)
	}
	fmt.Println("READY")
	time.Sleep(10 * time.Minute)
}

// volKilledChild starts the helper pogi, waits until its container runs, and kills it with SIGKILL.
func volKilledChild(t *testing.T, tg, data string) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestVolChild$", "-test.v")
	cmd.Env = append(os.Environ(), "POGI_VOL_TAG="+tg, "POGI_VOL_DATA="+data)
	out, _ := cmd.StdoutPipe()
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cmd.Process.Kill(); cmd.Wait() })
	ready := make(chan bool, 1)
	go func() {
		buf, got := make([]byte, 4096), ""
		for {
			n, err := out.Read(buf)
			got += string(buf[:n])
			if strings.Contains(got, "READY") {
				ready <- true
				io.Copy(io.Discard, out)
				return
			}
			if err != nil {
				ready <- false
				return
			}
		}
	}()
	select {
	case ok := <-ready:
		if !ok {
			t.Fatal("the helper pogi did not start its container")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the helper pogi did not start in 60s")
	}
	cmd.Process.Signal(syscall.SIGKILL)
	cmd.Wait()
}

// volProxy is an engine address that shows only the containers and volumes labelled pogi.vol=tg.
func volProxy(t *testing.T, tg string) string {
	t.Helper()
	e, ok := New(Options{}).Engine()
	if !ok {
		t.Skip("no engine")
	}
	u, _ := url.Parse(e.Socket)
	addr := u.Path
	if addr == "" {
		addr = u.Host
	}
	rp := &httputil.ReverseProxy{
		Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, u.Scheme, addr)
		}},
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(&url.URL{Scheme: "http", Host: "docker"})
			r.Out.URL.Path, r.Out.URL.RawPath = r.In.URL.Path, ""
			q := r.In.URL.Query()
			if p := r.In.URL.Path; r.In.Method == "GET" && (strings.HasSuffix(p, "/containers/json") || strings.HasSuffix(p, "/volumes")) {
				var f map[string][]string
				json.Unmarshal([]byte(q.Get("filters")), &f)
				if f == nil {
					f = map[string][]string{}
				}
				f["label"] = append(f["label"], "pogi.vol="+tg)
				b, _ := json.Marshal(f)
				q.Set("filters", string(b))
			}
			r.Out.URL.RawQuery = q.Encode()
		},
	}
	srv := httptest.NewServer(rp)
	t.Cleanup(srv.Close)
	return "tcp://" + srv.Listener.Addr().String()
}

// A memory-only pogi killed with kill -9 leaves its container and its data volume; the next pogi's Sweep removes both.
func TestSweepRemovesVolumeOfKilledMemoryPogi(t *testing.T) {
	if !New(Options{}).Available() {
		t.Skip("no engine")
	}
	tg := volTag()
	volCleanup(t, tg)
	volKilledChild(t, tg, "")
	if len(volLeft(tg)) != 1 {
		t.Fatalf("expected the volume to outlive kill -9, got %v", volLeft(tg))
	}
	New(Options{Host: volProxy(t, tg)}).Sweep()
	if left := volLeft(tg); len(left) != 0 {
		t.Errorf("the volume is still there after Sweep: %v", left)
	}
}

// With saved state the volume stays with its data dir: another pogi's Sweep leaves it, like the container.
func TestSweepKeepsVolumeOfSavedState(t *testing.T) {
	if !New(Options{}).Available() {
		t.Skip("no engine")
	}
	tg := volTag()
	volCleanup(t, tg)
	volKilledChild(t, tg, t.TempDir())
	New(Options{Host: volProxy(t, tg)}).Sweep()
	if len(volLeft(tg)) != 1 {
		t.Errorf("the volume of a saved-state data dir was swept: %v", volLeft(tg))
	}
}
