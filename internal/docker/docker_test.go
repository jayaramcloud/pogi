package docker

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestDemux(t *testing.T) {
	frame := func(stream byte, s string) []byte {
		h := []byte{stream, 0, 0, 0, 0, 0, 0, 0}
		binary.BigEndian.PutUint32(h[4:], uint32(len(s)))
		return append(h, s...)
	}
	in := slices.Concat(frame(1, "hel"), frame(2, "oops\n"), frame(1, "lo\nwor"), frame(1, "ld\r\n"), frame(2, "tail"))
	var got []string
	out, errs := &lines{fn: func(l string) { got = append(got, l) }}, &lines{fn: func(l string) { got = append(got, l) }}
	if err := demux(bytes.NewReader(in), out, errs); err != nil {
		t.Fatal(err)
	}
	out.flush()
	errs.flush()
	if want := []string{"oops", "hello", "world", "tail"}; !slices.Equal(got, want) {
		t.Fatalf("lines %q, want %q", got, want)
	}
	if demux(bytes.NewReader(frame(1, "abc")[:6]), io.Discard, io.Discard) == nil {
		t.Fatal("a cut header must be an error")
	}
}

func TestAuth(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOCKER_CONFIG", dir)
	cfg := `{"auths": {"https://index.docker.io/v1/": {}, "registry.example.com": {"auth": "` +
		base64.StdEncoding.EncodeToString([]byte("user:pa:ss")) + `"}}, "credHelpers": {"helper.example.com": "pogitest"}}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	decode := func(h string) (a map[string]string) {
		b, _ := base64.URLEncoding.DecodeString(h)
		json.Unmarshal(b, &a)
		return a
	}
	if a := decode(auth("registry.example.com/team/app:1")); a["username"] != "user" || a["password"] != "pa:ss" || a["serveraddress"] != "registry.example.com" {
		t.Fatalf("auths entry: %v", a)
	}
	if h := auth("alpine"); h != "" {
		t.Fatalf("Docker Hub has no credentials here, got %v", decode(h))
	}
	if runtime.GOOS != "windows" {
		bin := t.TempDir()
		helper := "#!/bin/sh\nread -r server\necho '{\"Username\":\"<token>\",\"Secret\":\"tok-'\"$server\"'\"}'\n"
		if err := os.WriteFile(filepath.Join(bin, "docker-credential-pogitest"), []byte(helper), 0o755); err != nil {
			t.Fatal(err)
		}
		t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
		if a := decode(auth("helper.example.com/app")); a["identitytoken"] != "tok-helper.example.com" || a["username"] != "" {
			t.Fatalf("credential helper: %v", a)
		}
	}
	hub := "https://index.docker.io/v1/"
	for image, want := range map[string]string{"alpine": hub, "library/alpine:3": hub, "docker.io/library/alpine": hub, "bitnami/redis": hub,
		"localhost:5000/app": "localhost:5000", "public.ecr.aws/lambda/python:3.12": "public.ecr.aws"} {
		if got := registry(image); got != want {
			t.Errorf("registry(%s) = %s, want %s", image, got, want)
		}
	}
}

func TestNamesAndOwners(t *testing.T) {
	if n := Name("lambda", "fn:1/a b"); n != "pogi-lambda-fn-1-a-b" {
		t.Fatalf("Name = %s", n)
	}
	host, _ := os.Hostname()
	for owner, want := range map[string]bool{
		host + "/" + strconv.Itoa(os.Getpid()) + "/x": true,
		host + "/" + strconv.Itoa(deadPID(t)) + "/x":  false,
		"another-host/1/x":                            true, // cannot be checked from here
		"":                                            true,
	} {
		if alive(owner) != want {
			t.Errorf("alive(%q) = %v", owner, !want)
		}
	}
}

// deadPID is the pid of a process that has exited.
func deadPID(t *testing.T) int {
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	if err := cmd.Run(); err != nil {
		t.Fatal(err)
	}
	return cmd.Process.Pid
}

// engine returns a Client of the running engine on a network of its own, or skips.
func engine(t *testing.T) *Client {
	c := New(Options{Network: "pogi-test"})
	if testing.Short() || !c.Available() {
		t.Skip("needs a running container engine")
	}
	t.Cleanup(func() { c.do(context.Background(), "DELETE", "/networks/pogi-test", nil, nil) })
	t.Cleanup(c.Close)
	return c
}

func TestEngine(t *testing.T) {
	c := engine(t)
	ctx := context.Background()
	if e, _ := c.Engine(); e.Name == "" || e.Version == "" || !strings.HasPrefix(e.API, "1.") || e.Socket == "" {
		t.Fatalf("engine %+v", e)
	}
	if err := c.Pull(ctx, "alpine"); err != nil {
		t.Fatal(err)
	}

	ct := &Container{Name: Name("test", rand.Text()[:8]), Image: "alpine", Service: "test", Resource: "r1",
		Labels: map[string]string{"pogi.test": "yes"}, Cmd: []string{"sh", "-c", "echo hello; echo oops >&2; exec sleep 300"},
		Ports: map[int]int{8080: 0}, Aliases: []string{"svc"}}
	id, err := c.Run(ctx, ct)
	if err != nil {
		t.Fatal(err)
	}
	info, err := c.Inspect(ctx, ct.Name)
	if err != nil || info.ID != id || !info.State.Running || info.HostPort(8080) == 0 || info.Config.Labels[LabelManaged] != "true" ||
		info.Config.Labels[LabelOwner] != c.owner || info.Config.Labels[LabelService] != "test" || info.Config.Labels[LabelResource] != "r1" ||
		info.Config.Labels["pogi.test"] != "yes" || !slices.Contains(info.NetworkSettings.Networks["pogi-test"].Aliases, "svc") {
		t.Fatalf("inspect %+v, %v", info, err)
	}
	for end := time.Now().Add(10 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		var got []string
		if err := c.Logs(ctx, id, false, func(l string) { got = append(got, l) }); err != nil {
			t.Fatal(err)
		}
		if slices.Contains(got, "hello") && slices.Contains(got, "oops") {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("logs %q", got)
		}
	}
	if out, code, err := c.Exec(ctx, id, "sh", "-c", "echo hi; echo err >&2; exit 3"); err != nil || code != 3 ||
		!strings.Contains(string(out), "hi\n") || !strings.Contains(string(out), "err\n") {
		t.Fatalf("exec = %q %d %v", out, code, err)
	}
	var tarball bytes.Buffer
	tw := tar.NewWriter(&tarball)
	tw.WriteHeader(&tar.Header{Name: "hello.txt", Mode: 0o644, Size: 5})
	tw.Write([]byte("howdy"))
	tw.Close()
	if err := c.CopyTo(ctx, id, "/tmp", &tarball); err != nil {
		t.Fatal(err)
	}
	if out, _, err := c.Exec(ctx, id, "cat", "/tmp/hello.txt"); string(out) != "howdy" {
		t.Fatalf("copied file = %q, %v", out, err)
	}
	if list, err := c.List(ctx, "pogi.test=yes", LabelOwner+"="+c.owner); err != nil || len(list) != 1 || list[0].ID != id {
		t.Fatalf("list %+v, %v", list, err)
	}

	done, err := c.Run(ctx, &Container{Image: "alpine", Service: "test", Cmd: []string{"sh", "-c", "echo done; exit 7"}})
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	if err := c.Logs(ctx, done, true, func(l string) { got = append(got, l) }); err != nil || !slices.Equal(got, []string{"done"}) {
		t.Fatalf("followed logs %q, %v", got, err)
	}
	if code, err := c.Wait(ctx, done); err != nil || code != 7 {
		t.Fatalf("wait = %d, %v", code, err)
	}

	for _, x := range []string{id, done} {
		if err := c.Remove(ctx, x); err != nil {
			t.Fatal(err)
		}
		if _, err := c.Inspect(ctx, x); status(err) != 404 {
			t.Fatalf("inspect after remove: %v", err)
		}
		if err := c.Remove(ctx, x); err != nil {
			t.Fatalf("removing a missing container: %v", err)
		}
	}
}

// TestCleanup checks the startup sweep (only containers of a pogi that is
// gone, never with persisted state) and Close (only this Client's).
func TestCleanup(t *testing.T) {
	c := engine(t)
	ctx := context.Background()
	host, _ := os.Hostname()
	gone := New(Options{Network: "pogi-test"})
	gone.owner = host + "/" + strconv.Itoa(deadPID(t)) + "/x"
	sleep := func(c *Client) string {
		id, err := c.Run(ctx, &Container{Image: "alpine", Service: "test", Cmd: []string{"sleep", "300"}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Remove(ctx, id) })
		return id
	}
	left, mine := sleep(gone), sleep(c)
	exists := func(id string) bool { _, err := c.Inspect(ctx, id); return err == nil }

	New(Options{Keep: true}).Sweep()
	if !exists(left) {
		t.Fatal("persisted state keeps leftovers for the services to resume")
	}
	New(Options{}).Sweep()
	if exists(left) || !exists(mine) {
		t.Fatalf("sweep: leftover exists %v, live owner's exists %v", exists(left), exists(mine))
	}
	c.Close()
	if exists(mine) {
		t.Fatal("Close removes the Client's containers")
	}
}

func TestRegistryHosts(t *testing.T) {
	for in, want := range map[string]string{
		"000000000000.dkr.ecr.us-east-1.amazonaws.com/app:1":     "localhost:4510/app:1",
		"123456789012.dkr.ecr.cn-north-1.amazonaws.com.cn/a/b@x": "localhost:4510/a/b@x",
		"myreg.azurecr.io/team/app:2":                            "localhost:4510/myreg/team/app:2",
		"us-central1-docker.pkg.dev/proj/repo/img:3":             "localhost:4510/proj/repo/img:3",
		"nginx:alpine":            "nginx:alpine",
		"docker.io/library/nginx": "docker.io/library/nginx",
		"azurecr.io/x":            "azurecr.io/x",
	} {
		if got := localImage(in, "localhost:4510"); got != want {
			t.Errorf("%s → %s, want %s", in, got, want)
		}
	}
}

func TestEmulated(t *testing.T) {
	noArm := errors.New("docker: no matching manifest for linux/arm64/v8 in the manifest list entries")
	for _, c := range []struct {
		arch string
		err  error
		want string
	}{
		{"arm64", noArm, "linux/amd64"},
		{"arm64", errors.New("pull x: no match for platform in manifest: not found"), "linux/amd64"},
		{"arm64", errors.New("docker: pull access denied"), ""}, // not a missing build
		{"amd64", noArm, ""}, // nothing to emulate
		{"arm64", nil, ""},
	} {
		if got := emulated(c.arch, c.err); got != c.want {
			t.Errorf("emulated(%s, %v) = %q, want %q", c.arch, c.err, got, c.want)
		}
	}
}

// floci #4608: a TLS engine (DOCKER_HOST=tcp://, DOCKER_TLS_VERIFY, DOCKER_CERT_PATH) answers dial.
func TestDialTLS(t *testing.T) {
	// one CA signs the server and the client certificate
	newKey := func() *ecdsa.PrivateKey { k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader); return k }
	caKey := newKey()
	caTmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test ca"}, NotBefore: time.Now().Add(-time.Hour),
		NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	caDER, _ := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	ca, _ := x509.ParseCertificate(caDER)
	issue := func(serial int64, usage x509.ExtKeyUsage) (certPEM, keyPEM []byte, pair tls.Certificate) {
		k := newKey()
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "test"}, NotBefore: time.Now().Add(-time.Hour),
			NotAfter: time.Now().Add(time.Hour), ExtKeyUsage: []x509.ExtKeyUsage{usage}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca, &k.PublicKey, caKey)
		kb, _ := x509.MarshalECPrivateKey(k)
		certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
		keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kb})
		pair, _ = tls.X509KeyPair(certPEM, keyPEM)
		return
	}
	_, _, serverPair := issue(2, x509.ExtKeyUsageServerAuth)
	clientCert, clientKey, _ := issue(3, x509.ExtKeyUsageClientAuth)
	pool := x509.NewCertPool()
	pool.AddCert(ca)

	mux := http.NewServeMux()
	mux.HandleFunc("/_ping", func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "OK") })
	mux.HandleFunc("/version", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"Version":"29.0.0","ApiVersion":"1.54","Arch":"arm64"}`)
	})
	srv := httptest.NewUnstartedServer(mux)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{serverPair}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	srv.StartTLS()
	defer srv.Close()

	dir := t.TempDir()
	for name, b := range map[string][]byte{"ca.pem": pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), "cert.pem": clientCert, "key.pem": clientKey} {
		if err := os.WriteFile(filepath.Join(dir, name), b, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	addr := "tcp://" + srv.Listener.Addr().String()

	t.Setenv("DOCKER_TLS_VERIFY", "1")
	t.Setenv("DOCKER_CERT_PATH", dir)
	cn, err := dial(addr)
	if err != nil || cn.Version != "29.0.0" || !strings.HasPrefix(cn.base, "https://") {
		t.Fatalf("dial with certs: %v %+v", err, cn)
	}
	t.Setenv("DOCKER_CERT_PATH", t.TempDir()) // no client certificate: the server refuses
	if _, err := dial(addr); err == nil {
		t.Fatal("dial without the client certificate succeeded")
	}
}

// TestKeepStopsAndAdopts checks that with saved state Close stops the
// containers of the data dir (those of an earlier run too) instead of
// removing them, that a later run starts one again by name, and that
// Orphan spares a data dir that is in use.
func TestKeepStopsAndAdopts(t *testing.T) {
	engine(t) // skips without an engine
	ctx := context.Background()
	opt := Options{Network: "pogi-test", Keep: true, DataDir: t.TempDir()}
	first, second := New(opt), New(opt)
	name := Name("test", "keep-"+strconv.Itoa(os.Getpid()))
	vol := name + "-vol"
	t.Cleanup(func() { second.Remove(ctx, name); second.RemoveVolume(ctx, vol) })
	state := func() string {
		i, err := second.Inspect(ctx, name)
		if err != nil {
			return err.Error()
		}
		return i.State.Status
	}

	if _, err := first.Run(ctx, &Container{Name: name, Image: "alpine", Service: "test", Resource: "keep", Cmd: []string{"sleep", "300"},
		Binds: []string{vol + ":/data"}}); err != nil {
		t.Fatal(err)
	}
	if vs, err := first.Volumes(ctx, LabelData+"="+opt.DataDir); err != nil || len(vs) != 1 || vs[0].Name != vol || vs[0].Labels[LabelService] != "test" {
		t.Fatalf("a named volume carries the labels: %v, %v", vs, err)
	}
	first.Close()
	if s := state(); s != "exited" {
		t.Fatalf("after Close with saved state: %s, want exited (stopped, not removed)", s)
	}
	if err := second.Start(ctx, name); err != nil || state() != "running" {
		t.Fatalf("the next run starts it again: %v, %s", err, state())
	}
	second.find() // the second Client has used the engine now
	second.Close()
	if s := state(); s != "exited" {
		t.Fatalf("a container of an earlier run is stopped too: %s", s)
	}
	if n := len(second.Mine(ctx)); n != 1 {
		t.Fatalf("Mine finds %d containers of the data dir, want 1", n)
	}

	New(Options{Network: "pogi-test"}).Close() // without saved state, nothing is touched that it did not make
	if state() != "exited" {
		t.Fatal("Close of another Client must leave the container alone")
	}
}

func TestOrphan(t *testing.T) {
	host, _ := os.Hostname()
	dead := host + "/" + strconv.Itoa(deadPID(t)) + "/x"
	live := host + "/" + strconv.Itoa(os.Getpid()) + "/x"
	inUse, idle := t.TempDir(), t.TempDir()
	os.WriteFile(filepath.Join(inUse, ownerFile), []byte(live), 0o600)
	os.WriteFile(filepath.Join(idle, ownerFile), []byte(dead), 0o600)
	for name, c := range map[string]struct {
		labels map[string]string
		want   bool
	}{
		"dead owner":       {map[string]string{LabelManaged: "true", LabelOwner: dead}, true},
		"live owner":       {map[string]string{LabelManaged: "true", LabelOwner: live}, false},
		"no label":         {map[string]string{LabelOwner: dead}, false},
		"none at all":      {nil, false},
		"data dir in use":  {map[string]string{LabelManaged: "true", LabelOwner: dead, LabelData: inUse}, false},
		"data dir idle":    {map[string]string{LabelManaged: "true", LabelOwner: dead, LabelData: idle}, true},
		"data dir unknown": {map[string]string{LabelManaged: "true", LabelOwner: dead, LabelData: t.TempDir()}, true},
	} {
		if got := Orphan(c.labels); got != c.want {
			t.Errorf("%s: Orphan = %v, want %v", name, got, c.want)
		}
	}
	for bind, want := range map[string]string{"vol-1:/data": "vol-1", "/host:/data": "", "./rel:/data": "", `C:\dir:/data`: "", "my.vol:/d:ro": "my.vol"} {
		if got := volumeName(bind); got != want {
			t.Errorf("volumeName(%q) = %q, want %q", bind, got, want)
		}
	}
}
