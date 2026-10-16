package servicebus_test

import (
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/servicebus"
	"github.com/nrndev/pogi/internal/config"
	"go.yaml.in/yaml/v3"
)

const seedYAML = `resources:
  azure:
    servicebus:
      queues:
        - jobs
        - {name: orders, lockDuration: 45s, maxDeliveryCount: 3, requiresSession: true, defaultMessageTimeToLive: PT1H}
      topics:
        - name: events
          requiresDuplicateDetection: true
          subscriptions:
            - audit
            - {name: big, maxDeliveryCount: 2, sqlFilter: "price > 100", sqlAction: "SET sys.Label = 'big'"}
`

func TestSeedFromConfig(t *testing.T) {
	f := &config.File{}
	if err := yaml.Unmarshal([]byte(seedYAML), f); err != nil {
		t.Fatal(err)
	}
	srv := pogi.Start(t, pogi.Config{File: f})
	svc := srv.Hub.Service("servicebus").(*servicebus.Service)

	q, ok := svc.GetQueue("localhost", "orders")
	if !ok || time.Duration(q.LockDuration) != 45*time.Second || q.MaxDeliveryCount != 3 || !q.RequiresSession || time.Duration(q.DefaultMessageTimeToLive) != time.Hour {
		t.Fatalf("orders = %+v", q)
	}
	if j, ok := svc.GetQueue("localhost", "jobs"); !ok || j.MaxDeliveryCount != 10 {
		t.Fatalf("jobs = %+v", j)
	}
	if tp, ok := svc.GetTopic("localhost", "events"); !ok || !tp.RequiresDuplicateDetection || tp.Subscriptions != 2 {
		t.Fatalf("events = %+v", tp)
	}
	r, err := svc.GetRule("localhost", "events", "big", "$Default")
	if err != nil || r.Filter.SQL != "price > 100" || r.Action != "SET sys.Label = 'big'" {
		t.Fatalf("rule = %+v %v", r, err)
	}

	// a reset seeds again, and seeding twice changes nothing
	srv.Reset("servicebus")
	if _, ok := svc.GetQueue("localhost", "orders"); !ok {
		t.Fatal("not seeded again after reset")
	}
	if err := svc.Seed("queue", "localhost", "jobs", map[string]any{}); err != nil {
		t.Fatalf("seeding an existing queue: %v", err)
	}
	if err := svc.Seed("queue", "localhost", "bad", map[string]any{"nope": 1}); err == nil {
		t.Fatal("unknown property accepted")
	}
}
