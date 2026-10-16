package docker

import (
	"archive/tar"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

var testMounts = []Mount{
	{Type: "bind", Source: "/srv/host/pogi-data", Destination: "/var/lib/pogi"},
	{Type: "volume", Name: "vol1", Source: "/var/lib/docker/volumes/vol1/_data", Destination: "/data"},
	{Type: "bind", Source: "/srv/host/deep", Destination: "/var/lib/pogi/deep"},
	{Type: "tmpfs", Destination: "/run"},
}

func TestRebind(t *testing.T) {
	sub := func(name, sub, target, opts string) map[string]any {
		return map[string]any{"Type": "volume", "Source": name, "Target": target, "ReadOnly": strings.Contains(opts, "ro"),
			"VolumeOptions": map[string]any{"Subpath": sub}}
	}
	for _, c := range []struct {
		name, src, dst, opts string
		subpath              bool
		want                 plan
	}{
		{"under a bind mount", "/var/lib/pogi/lambda/code/abc", "/var/task", "ro", true, plan{bind: "/srv/host/pogi-data/lambda/code/abc:/var/task:ro"}},
		{"the bind mount itself", "/var/lib/pogi", "/x", "", true, plan{bind: "/srv/host/pogi-data:/x"}},
		{"the deepest mount wins", "/var/lib/pogi/deep/a", "/x", "", true, plan{bind: "/srv/host/deep/a:/x"}},
		{"under a volume", "/data/ecs/w", "/work", "", true, plan{mount: sub("vol1", "ecs/w", "/work", "")}},
		{"under a volume, read only", "/data/f", "/work", "ro", true, plan{mount: sub("vol1", "f", "/work", "ro")}},
		{"a whole volume", "/data", "/work", "ro", true, plan{bind: "vol1:/work:ro"}},
		{"under a volume, old engine", "/data/ecs/w", "/work", "", false, plan{copy: true}},
		{"outside any mount", "/tmp/src-1", "/workspace", "", true, plan{copy: true}},
		{"a name that only starts like a mount", "/var/lib/pogi2/x", "/x", "", true, plan{copy: true}},
		{"under tmpfs", "/run/x", "/x", "", true, plan{copy: true}},
	} {
		if got := rebind(c.src, c.dst, c.opts, testMounts, c.subpath); !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %+v, want %+v", c.name, got, c.want)
		}
	}
}

func TestSelfIDs(t *testing.T) {
	id := strings.Repeat("ab12", 16)
	mi := "700 650 0:50 /docker/containers/" + id + "/hostname /etc/hostname rw - ext4 /dev/vda1 rw\n"
	if got := selfIDs(mi, "abc"); !reflect.DeepEqual(got, []string{id, "abc"}) {
		t.Errorf("got %v", got)
	}
	if got := selfIDs("nothing", "abc"); !reflect.DeepEqual(got, []string{"abc"}) {
		t.Errorf("got %v", got)
	}
}

// A pogi in a container joins the pogi network, is reached at its address there, and its containers are too.
func TestNested(t *testing.T) {
	dir := t.TempDir()
	mark, mi := filepath.Join(dir, "dockerenv"), filepath.Join(dir, "mountinfo")
	os.WriteFile(mark, nil, 0o600)
	id := strings.Repeat("cd34", 16)
	os.WriteFile(mi, []byte("1 2 0:5 /docker/containers/"+id+"/hostname /etc/hostname rw - ext4 x rw\n"), 0o600)
	oldMarks, oldMI := containerMarks, mountInfoFile
	containerMarks, mountInfoFile = []string{mark}, mi
	t.Cleanup(func() { containerMarks, mountInfoFile = oldMarks, oldMI })

	connected := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path[strings.Index(r.URL.Path[1:], "/")+1:]
		switch {
		case p == "/_ping":
		case p == "/version":
			io.WriteString(w, `{"Version":"x","ApiVersion":"1.54"}`)
		case p == "/networks/pogi" && r.Method == "GET":
		case p == "/networks/pogi/connect":
			connected = true
		case p == "/containers/"+id+"/json":
			ip := ""
			if connected {
				ip = "172.30.0.2"
			}
			json.NewEncoder(w).Encode(map[string]any{"Id": id, "Name": "/pogi", "Mounts": testMounts,
				"NetworkSettings": map[string]any{"Networks": map[string]any{"pogi": map[string]any{"IPAddress": ip}}}})
		case p == "/containers/json":
			io.WriteString(w, `[{"Id":"c1","Ports":[{"PrivatePort":5432,"PublicPort":49170,"Type":"tcp"}],"NetworkSettings":{"Networks":{"pogi":{"IPAddress":"172.30.0.7"}}}}]`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := New(Options{Host: "tcp://" + srv.Listener.Addr().String()})
	if got := c.HostAddr(); got != "172.30.0.2" || !connected {
		t.Errorf("HostAddr = %q, connected = %v", got, connected)
	}
	if got := c.ListenAddr(); got != "172.30.0.2" {
		t.Errorf("ListenAddr = %q", got)
	}
	if got := c.Addr(49170); got != "172.30.0.7:5432" {
		t.Errorf("Addr of a child = %q", got)
	}
	if got := c.Addr(1234); got != "127.0.0.1:1234" {
		t.Errorf("Addr of an unknown port = %q", got)
	}
	src := filepath.Join(dir, "src")
	os.Mkdir(src, 0o755)
	keep, mounts, copies := c.rebinds(c.find(), []string{"/var/task-missing:/a", "named:/b", src + ":/c:ro"})
	if !reflect.DeepEqual(keep, []string{"/var/task-missing:/a", "named:/b"}) || len(mounts) != 0 || !reflect.DeepEqual(copies, [][2]string{{src, "/c"}}) {
		t.Errorf("rebinds = %v %v %v", keep, mounts, copies)
	}
}

// Not in a container: nothing changes.
func TestNotNested(t *testing.T) {
	old := containerMarks
	containerMarks = []string{filepath.Join(t.TempDir(), "none")}
	t.Cleanup(func() { containerMarks = old })
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/version") {
			io.WriteString(w, `{"Version":"x","ApiVersion":"1.54"}`)
		}
	}))
	defer srv.Close()
	c := New(Options{Host: "tcp://" + srv.Listener.Addr().String()})
	if c.ListenAddr() != "127.0.0.1" || c.Addr(80) != "127.0.0.1:80" {
		t.Errorf("ListenAddr %q, Addr %q", c.ListenAddr(), c.Addr(80))
	}
	if keep, _, copies := c.rebinds(c.find(), []string{"/tmp:/x"}); len(keep) != 1 || len(copies) != 0 {
		t.Errorf("binds changed: %v %v", keep, copies)
	}
}

func TestTarFiles(t *testing.T) {
	src := t.TempDir()
	os.MkdirAll(filepath.Join(src, "sub"), 0o755)
	os.WriteFile(filepath.Join(src, "sub", "a.txt"), []byte("hi"), 0o640)
	var buf bytes.Buffer
	if err := tarFiles(&buf, src, "/var/task"); err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for tr := tar.NewReader(&buf); ; {
		h, err := tr.Next()
		if err != nil {
			break
		}
		b, _ := io.ReadAll(tr)
		got[h.Name] = string(b)
		if h.Uid != 0 {
			t.Errorf("%s owner %d", h.Name, h.Uid)
		}
	}
	want := map[string]string{"var/task/": "", "var/task/sub/": "", "var/task/sub/a.txt": "hi"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v", got)
	}
}
