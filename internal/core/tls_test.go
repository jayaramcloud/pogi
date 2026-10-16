package core

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
)

func TestSniffTLS(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	cfg, err := h.TLS()
	if err != nil {
		t.Fatal(err)
	}
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	srv := &http.Server{Protocols: Protocols(), Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, map[bool]string{true: "secure", false: "plain"}[r.TLS != nil])
	})}
	go srv.Serve(SniffTLS(ln, cfg))
	defer srv.Close()
	addr := ln.Addr().String()

	caPath, _, _ := h.CertPaths()
	pool := x509.NewCertPool()
	pem, _ := os.ReadFile(caPath)
	pool.AppendCertsFromPEM(pem)
	secure := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool}, ForceAttemptHTTP2: true}}
	for _, c := range []struct {
		client *http.Client
		url    string
		want   string
	}{{http.DefaultClient, "http://" + addr, "plain"}, {secure, "https://localhost:" + addr[len("127.0.0.1:"):], "secure"}} {
		res, err := c.client.Get(c.url)
		if err != nil {
			t.Fatal(err)
		}
		b, _ := io.ReadAll(res.Body)
		if string(b) != c.want {
			t.Fatalf("%s = %q (proto %s)", c.url, b, res.Proto)
		}
	}
	// A second call reuses the CA and certificate on disk.
	if _, err := h.TLS(); err != nil {
		t.Fatal(err)
	}
}
