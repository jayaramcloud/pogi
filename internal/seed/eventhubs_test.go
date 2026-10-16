package seed_test

import (
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/seed"
	"go.yaml.in/yaml/v3"
)

func TestEventHubs(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"eventhubs"}})
	apply := func(src string) error {
		var r config.Resources
		if err := yaml.Unmarshal([]byte(src), &r); err != nil {
			t.Fatal(err)
		}
		return seed.ApplyResources(srv.Hub, r, "test.yaml")
	}
	const src = `
azure:
  eventhubs:
    hubs:
      - {name: orders, partitions: 6, retentionHours: 48, consumerGroups: [billing, shipping]}
      - {name: audit, namespace: acme}
    consumerGroups:
      - {name: late, eventHub: orders}
`
	for range 2 { // seeding again changes nothing
		if err := apply(src); err != nil {
			t.Fatal(err)
		}
	}
	insp := func(table, id string) map[string]any {
		t.Helper()
		m, _ := srv.Hub.Service("eventhubs").(interface{ Inspect(string, string) any }).Inspect(table, id).(map[string]any)
		if m == nil {
			t.Fatalf("%s %s is not there", table, id)
		}
		return m
	}
	hub := insp("Event hubs", "localhost/orders")
	if hub["retention"] != "48h0m0s" || len(hub["partitions"].([]map[string]any)) != 6 || strings.Join(hub["consumerGroups"].([]string), ",") != "$Default,billing,shipping,late" {
		t.Fatalf("%v", hub)
	}
	if audit := insp("Event hubs", "acme/audit"); len(audit["partitions"].([]map[string]any)) != 4 {
		t.Fatalf("defaults: %v", audit)
	}
	for _, bad := range []string{
		"azure: {eventhubs: {hubs: [{name: x, partitions: 99}]}}",
		"azure: {eventhubs: {hubs: [{name: x, colour: red}]}}",
		"azure: {eventhubs: {consumerGroups: [{name: g}]}}",
		"azure: {eventhubs: {consumerGroups: [{name: g, eventHub: nope}]}}",
	} {
		if err := apply(bad); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
}

func TestEventHubsReseededAfterReset(t *testing.T) {
	f := &config.File{}
	if err := yaml.Unmarshal([]byte("resources:\n  azure:\n    eventhubs:\n      hubs: [{name: orders, partitions: 2}]\n"), f); err != nil {
		t.Fatal(err)
	}
	srv := pogi.Start(t, pogi.Config{Services: []string{"eventhubs"}, File: f})
	eh := srv.Hub.Service("eventhubs").(interface{ Inspect(string, string) any })
	if eh.Inspect("Event hubs", "localhost/orders") == nil {
		t.Fatal("not seeded at start")
	}
	srv.Hub.Service("eventhubs").(interface {
		CreateHub(ns, name string, partitions int, retention time.Duration) error
	}).CreateHub("localhost", "adhoc", 1, 0)
	srv.Reset("eventhubs")
	if eh.Inspect("Event hubs", "localhost/orders") == nil || eh.Inspect("Event hubs", "localhost/adhoc") != nil {
		t.Fatal("reset must clear what was made since, and seed the file's hubs again")
	}
}
