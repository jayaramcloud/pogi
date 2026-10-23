package rds

import (
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
