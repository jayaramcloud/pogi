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

// TestCertCoversServiceHosts: the HTTPS addresses Azure services hand out (Event Grid topic endpoints and the
// like) are covered by pogi's certificate; *.localhost covers one label only.
func TestCertCoversServiceHosts(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	cfg, err := h.TLS()
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cfg.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"localhost", "orders.eventgrid.localhost", "app.azurewebsites.localhost", "app.scm.azurewebsites.localhost",
		"hub.service.signalr.localhost", "gw.azure-api.localhost", "mail.communication.localhost", "acct.blob.localhost"} {
		if err := leaf.VerifyHostname(host); err != nil {
			t.Error(err)
		}
	}
}
