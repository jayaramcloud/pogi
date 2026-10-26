package core

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"slices"
	"time"
)

// certNames are the host names pogi's certificate covers; a stored certificate
// with a different list is reissued.
// A wildcard covers one label, so every <name>.<service>.localhost address a service hands out over HTTPS
// needs its own entry.
var certNames = []string{"localhost", "*.localhost", "host.docker.internal", "*.lambda-url.us-east-1.localhost", "*.execute-api.localhost", "*.vault.localhost", "*.blob.localhost", "*.queue.localhost", "*.table.localhost", "*.file.localhost", "*.dfs.localhost",
	"*.eventgrid.localhost", "*.azurewebsites.localhost", "*.scm.azurewebsites.localhost", "*.service.signalr.localhost", "*.azure-api.localhost", "*.communication.localhost"}

// CertPaths are the files of pogi's local certificate authority.
func (h *Hub) CertPaths() (ca, cert, key string) {
	dir := filepath.Join(h.Cfg.DataDir, "certs")
	return filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
}

// TLS returns a server config: Cfg.TLSCert and TLSKey when set, otherwise a
// certificate for localhost signed by a local CA, both created on first use
// (trust ca.pem once, see `pogi cert`).
func (h *Hub) TLS() (*tls.Config, error) {
	if h.Cfg.TLSCert != "" || h.Cfg.TLSKey != "" {
		cert, err := tls.LoadX509KeyPair(h.Cfg.TLSCert, h.Cfg.TLSKey)
		if err != nil {
			return nil, fmt.Errorf("tls certificate %q, key %q: %w", h.Cfg.TLSCert, h.Cfg.TLSKey, err)
		}
		return serverTLS(cert), nil
	}
	caPath, certPath, keyPath := h.CertPaths()
	if cert, err := tls.LoadX509KeyPair(certPath, keyPath); err == nil && time.Now().Before(cert.Leaf.NotAfter) && slices.Equal(cert.Leaf.DNSNames, certNames) {
		return serverTLS(cert), nil
	}
	if err := os.MkdirAll(filepath.Dir(caPath), 0o700); err != nil {
		return nil, err
	}
	caCert, caKey, err := loadOrCreateCA(caPath, filepath.Join(filepath.Dir(caPath), "ca-key.pem"))
	if err != nil {
		return nil, err
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "localhost", Organization: []string{"pogi"}},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(800 * 24 * time.Hour), // under Apple's 825-day limit
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    certNames,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1"), net.ParseIP("::1")},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, err
	}
	if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
		return nil, err
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	if err := writePEM(keyPath, "EC PRIVATE KEY", kb); err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		return nil, err
	}
	return serverTLS(cert), nil
}

func serverTLS(cert tls.Certificate) *tls.Config {
	return &tls.Config{Certificates: []tls.Certificate{cert}, NextProtos: []string{"h2", "http/1.1"}, MinVersion: tls.VersionTLS12}
}

func serial() *big.Int {
	n, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 120))
	return n
}

func loadOrCreateCA(certPath, keyPath string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	if cb, err := os.ReadFile(certPath); err == nil {
		if kb, err := os.ReadFile(keyPath); err == nil {
			c, _ := pem.Decode(cb)
			k, _ := pem.Decode(kb)
			if c != nil && k != nil {
				cert, err1 := x509.ParseCertificate(c.Bytes)
				key, err2 := x509.ParseECPrivateKey(k.Bytes)
				if err1 == nil && err2 == nil {
					return cert, key, nil
				}
			}
		}
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: serial(), Subject: pkix.Name{CommonName: "pogi local CA", Organization: []string{"pogi"}},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().AddDate(10, 0, 0),
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign, BasicConstraintsValid: true, IsCA: true, MaxPathLenZero: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	kb, _ := x509.MarshalECPrivateKey(key)
	if err := writePEM(certPath, "CERTIFICATE", der); err != nil {
		return nil, nil, err
	}
	if err := writePEM(keyPath, "EC PRIVATE KEY", kb); err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	return cert, key, err
}

func writePEM(path, typ string, der []byte) error {
	return os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600)
}

// SniffTLS lets one port speak both HTTP and HTTPS: connections that open
// with a TLS handshake get TLS, everything else stays plain (incl. h2c).
func SniffTLS(ln net.Listener, cfg *tls.Config) net.Listener {
	s := &sniffer{Listener: ln, cfg: cfg, conns: make(chan net.Conn), done: make(chan struct{}), errs: make(chan error, 1)}
	Go("core.loop", func() { s.loop() })
	return s
}

type sniffer struct {
	net.Listener
	cfg   *tls.Config
	conns chan net.Conn
	done  chan struct{}
	errs  chan error
}

func (s *sniffer) loop() {
	for {
		c, err := s.Listener.Accept()
		if err != nil {
			s.errs <- err
			return
		}
		Go("core.loop", func() {
			br := bufio.NewReader(c)
			c.SetReadDeadline(time.Now().Add(10 * time.Second))
			first, err := br.Peek(1)
			c.SetReadDeadline(time.Time{})
			if err != nil {
				c.Close()
				return
			}
			var out net.Conn = &peeked{Conn: c, r: br}
			if first[0] == 0x16 { // TLS handshake record
				out = tls.Server(out, s.cfg)
			}
			select {
			case s.conns <- out:
			case <-s.done:
				c.Close()
			}
		})
	}
}

func (s *sniffer) Accept() (net.Conn, error) {
	select {
	case c := <-s.conns:
		return c, nil
	case err := <-s.errs:
		s.errs <- err
		return nil, err
	case <-s.done:
		return nil, net.ErrClosed
	}
}

func (s *sniffer) Close() error {
	select {
	case <-s.done:
		return errors.New("already closed")
	default:
		close(s.done)
	}
	return s.Listener.Close()
}

// peeked is a conn whose first bytes were read ahead.
type peeked struct {
	net.Conn
	r *bufio.Reader
}

func (p *peeked) Read(b []byte) (int, error) { return p.r.Read(b) }
