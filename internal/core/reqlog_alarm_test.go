package core

import "testing"

func TestEntryAlarming(t *testing.T) {
	for _, c := range []struct {
		e    Entry
		want bool
	}{
		{Entry{Method: "GET", Status: 404}, false},
		{Entry{Method: "HEAD", Status: 404}, false},
		{Entry{Method: "POST", Op: "GetItem", Status: 404}, false},
		{Entry{Method: "POST", Op: "DescribeTable", Status: 404}, false},
		{Entry{Method: "POST", Op: "ListQueues", Status: 404}, false},
		{Entry{Method: "PUT", Op: "PutObject", Status: 404}, true},
		{Entry{Method: "POST", Op: "DeleteItem", Status: 404}, true},
		{Entry{Method: "GET", Status: 500}, true},
		{Entry{Method: "GET", Status: 403}, true},
		{Entry{Method: "POST", Op: "GetItem", Status: 200}, false},
		{Entry{Method: "POST", Op: "Chaos", Status: 503, Chaos: true}, true},
		{Entry{Method: "GET", Status: 200, IAM: "deny: no"}, false},
	} {
		if got := c.e.Alarming(); got != c.want {
			t.Errorf("%+v: Alarming = %v, want %v", c.e, got, c.want)
		}
	}
}
