package core

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
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

// TestSniffPostgres: a first byte of 0x00 (a PostgreSQL client) goes to Hub.PG, with or without TLS configured;
// HTTP keeps working beside it; with no handler the connection is closed; a client that sends nothing
// does not hold up the next one.
func TestSniffPostgres(t *testing.T) {
	h := NewHub(Config{DataDir: t.TempDir()}, nil)
	cfg, _ := h.TLS()
	ssl := []byte{0, 0, 0, 8, 4, 0xd2, 0x16, 0x2f}
	// serve runs an HTTP server behind a hub whose PG is pg.
	serve := func(tc *tls.Config, pg func(net.Conn)) (addr string, stop func()) {
		ln, _ := net.Listen("tcp", "127.0.0.1:0")
		srv := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "http") })}
		go srv.Serve((&Hub{PG: pg}).Sniff(ln, tc))
		return ln.Addr().String(), func() { srv.Close() }
	}
	for _, tc := range []*tls.Config{nil, cfg} {
		addr, stop := serve(tc, nil)
		c, _ := net.Dial("tcp", addr)
		c.Write(ssl)
		if n, err := c.Read(make([]byte, 1)); n != 0 || err == nil {
			t.Fatalf("no handler: read %d, %v; want the connection closed", n, err)
		}
		c.Close()
		stop()

		addr, stop = serve(tc, func(c net.Conn) { // answers an SSLRequest with N, as a server without TLS does
			defer c.Close()
			b := make([]byte, 8)
			if _, err := io.ReadFull(c, b); err == nil && string(b) == string(ssl) {
				c.Write([]byte("N"))
			}
		})
		idle, _ := net.Dial("tcp", addr) // sends nothing
		defer idle.Close()
		c, _ = net.Dial("tcp", addr)
		c.Write(ssl)
		got := make([]byte, 1)
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(c, got); err != nil || got[0] != 'N' {
			t.Fatalf("postgres client: %q, %v", got, err)
		}
		c.Close()

		res, err := http.Get("http://" + addr)
		if err != nil {
			t.Fatal(err)
		}
		if b, _ := io.ReadAll(res.Body); string(b) != "http" {
			t.Fatalf("http = %q", b)
		}
		stop()
	}
}
