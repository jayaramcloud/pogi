package functions

import "strings"

// Fake makes an app's host a server that is already running: tests stand an
// httptest server in for the Functions host.
func (s *Service) Fake(app, addr, key string) {
	h := &host{ready: make(chan struct{}), cancel: func() {}, addr: addr, key: key, how: "fake"}
	close(h.ready)
	s.mu.Lock()
	s.hosts[strings.ToLower(app)] = h
	s.mu.Unlock()
}

// Env exposes the settings the host would run with.
func (s *Service) Env(app, host string) map[string]string { return s.env(*s.app(app), host) }

// Background exposes the trigger scan.
var Background = hasBackgroundTrigger
