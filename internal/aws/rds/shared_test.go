package rds

import (
	"errors"
	"sync"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

// A resource outside the default account and region has a longer container name, but its row key is its identifier.
func TestResourceKeyIsIdentifier(t *testing.T) {
	hub := core.NewHub(core.Config{}, nil)
	var mu sync.Mutex
	b := NewBox(hub, "rds", "failed", &mu, func(*Server) bool { return true })
	defer b.Close(nil)
	srv := &Server{}
	b.Serve(srv, "111122223333", "eu-west-1", "uxdemo", 0, 5432, func() Spec { return Spec{} }, false)
	name := b.Name("111122223333", "eu-west-1", "uxdemo")
	if name == "pogi-rds-uxdemo" {
		t.Fatal("expected a qualified container name")
	}
	if got := b.resource(srv, name); got != "uxdemo" {
		t.Errorf("resource = %q, want uxdemo", got)
	}
	if got := b.resource(&Server{}, "pogi-rds-old"); got != "old" { // state saved before Server.ID
		t.Errorf("fallback = %q", got)
	}
}

// The engine's words for a published port someone else took, which make the box try another free port.
func TestPortTaken(t *testing.T) {
	for msg, want := range map[string]bool{
		"docker: failed to set up container networking: driver failed programming external connectivity on endpoint x: Bind for 127.0.0.1:55056 failed: port is already allocated": true,
		"docker: listen tcp 127.0.0.1:5432: bind: address already in use": true,
		"docker: pull access denied for postgres":                         false,
	} {
		if got := portTaken(errors.New(msg)); got != want {
			t.Errorf("portTaken(%q) = %v", msg, got)
		}
	}
	if portTaken(nil) {
		t.Error("portTaken(nil)")
	}
}
