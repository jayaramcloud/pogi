package elasticache

// IAM authentication. A server whose users include IAM users is reached
// through a RESP proxy: AUTH and HELLO … AUTH with a valid IAM auth token get
// the user's password in the server instead; everything else, a bad token
// too (the server then refuses it), passes as is.
// ponytail: cluster-mode clients follow the port the server announces, past
// the proxy; IAM users sign in there only on the configuration endpoint.

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

type proxy struct {
	ln   net.Listener
	port int
}

// listen starts the proxy of the server in a container, or returns nil; lock held.
func (s *Service) listen(container string) *proxy {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		s.hub.Log.Warn("elasticache IAM proxy", "container", container, "err", err)
		return nil
	}
	core.Go("elasticache.listen", func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			core.Go("elasticache.relay", func() { s.relay(conn, container) })
		}
	})
	return &proxy{ln, ln.Addr().(*net.TCPAddr).Port}
}

// endpoint is where clients reach a server: its proxy when it has one. Lock held.
func (s *Service) endpoint(srv *rds.Server) *rds.Endpoint {
	ep := srv.Addr()
	if p := s.proxies[srv.Container]; p != nil && ep != nil {
		ep.Port = p.port
	}
	return ep
}

func (s *Service) relay(client net.Conn, container string) {
	defer client.Close()
	s.mu.Lock()
	port := 0
	for _, r := range s.rbacs() {
		if r.srv.Container == container {
			port = r.srv.HostPort
		}
	}
	s.mu.Unlock()
	server, err := net.Dial("tcp", s.hub.Docker().Addr(port))
	if err != nil {
		return
	}
	defer server.Close()
	core.Go("elasticache.relay", func() {
		io.Copy(client, server)
		client.Close()
	})
	r := bufio.NewReader(client)
	for {
		args, raw, err := readCommand(r)
		if err != nil {
			return
		}
		if i := authAt(args); i > 0 {
			if pw := s.iamPassword(container, args[i], args[i+1]); pw != "" {
				args[i+1] = pw
				raw = encode(args)
			}
		}
		if _, err := server.Write(raw); err != nil {
			return
		}
	}
}

// iamPassword is the server password of an IAM user of the server in
// container when token is a valid IAM auth token for it, else "".
func (s *Service) iamPassword(container, user, token string) string {
	s.mu.Lock()
	var name, region, pw string
	for _, r := range s.rbacs() {
		if r.srv.Container != container {
			continue
		}
		if u := s.users(r)[user]; u != nil && u.Authentication.Type == "iam" && len(u.Passwords) > 0 {
			name, region, pw = r.name, r.c.Region, u.Passwords[0]
		}
	}
	s.mu.Unlock()
	if pw == "" || checkToken(token, name, region, user) != nil {
		return ""
	}
	if err := s.hub.VerifyPresigned("elasticache", "http://"+token); err != nil {
		return ""
	}
	return pw
}

// checkToken checks what every IAM auth token must be, signature aside: a
// presigned GET of http://<cache>/?Action=connect&User=<user> for
// elasticache in the cache's region, at most 15 minutes old.
func checkToken(token, cache, region, user string) error {
	u, err := url.Parse("http://" + token)
	if err != nil {
		return err
	}
	q := u.Query()
	cred := strings.Split(q.Get("X-Amz-Credential"), "/")
	signed, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	ttl, _ := strconv.Atoi(q.Get("X-Amz-Expires"))
	switch {
	case err != nil || len(cred) != 5 || q.Get("X-Amz-Signature") == "":
		return errors.New("not a presigned URL")
	case q.Get("Action") != "connect" || q.Get("User") != user || !strings.EqualFold(u.Host, cache) || cred[2] != region || cred[3] != "elasticache":
		return errors.New("for another user, cache or service")
	case time.Since(signed) > time.Duration(min(ttl, 900))*time.Second || time.Until(signed) > 15*time.Minute:
		return errors.New("expired")
	}
	return nil
}

// authAt is the index of the user name in AUTH user pass or HELLO … AUTH user pass, else 0.
func authAt(args []string) int {
	switch {
	case len(args) == 3 && strings.EqualFold(args[0], "AUTH"):
		return 1
	case len(args) > 0 && strings.EqualFold(args[0], "HELLO"):
		for i := 1; i+2 < len(args); i++ {
			if strings.EqualFold(args[i], "AUTH") {
				return i + 1
			}
		}
	}
	return 0
}

// readCommand reads a client's command: a RESP array of bulk strings or an
// inline command. raw is what was read.
func readCommand(r *bufio.Reader) (args []string, raw []byte, err error) {
	if raw, err = r.ReadBytes('\n'); err != nil {
		return nil, nil, err
	}
	if raw[0] != '*' {
		return strings.Fields(string(raw)), raw, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(raw[1:])))
	if err != nil || n > 1<<20 {
		return nil, nil, errors.New("protocol error")
	}
	for range n {
		h, err := r.ReadBytes('\n')
		if err != nil {
			return nil, nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(string(h[1:])))
		if err != nil || h[0] != '$' || size < 0 || size > 512<<20 {
			return nil, nil, errors.New("protocol error")
		}
		b := make([]byte, size+2)
		if _, err := io.ReadFull(r, b); err != nil {
			return nil, nil, err
		}
		raw = append(append(raw, h...), b...)
		args = append(args, string(b[:size]))
	}
	return args, raw, nil
}

func encode(args []string) []byte {
	b := fmt.Appendf(nil, "*%d\r\n", len(args))
	for _, a := range args {
		b = fmt.Appendf(b, "$%d\r\n%s\r\n", len(a), a)
	}
	return b
}
