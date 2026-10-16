package core

import "testing"

func TestDeliverTarget(t *testing.T) {
	for target, want := range map[string]string{
		"arn:aws:sqs:us-east-1:000000000000:q":                                                   "sqs",
		"//pubsub.googleapis.com/projects/p/topics/t":                                            "pubsub",
		"//run.googleapis.com/projects/p/locations/l/services/s":                                 "run",
		"/subscriptions/s/resourceGroups/g/providers/Microsoft.ServiceBus/namespaces/n/queues/q": "microsoft.servicebus",
		"projects/p/topics/t":             "",
		"//example.com/projects/p":        "",
		"/subscriptions/s/resourceGroups": "",
	} {
		got, ok := DeliverTarget(target)
		if ok != (want != "") || got != want && ok {
			t.Errorf("DeliverTarget(%q) = %q, %v; want %q", target, got, ok, want)
		}
	}
}
