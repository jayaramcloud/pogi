package servicebus

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/core"
)

// deliver puts a hub message (from Event Grid, say) on a queue or topic. The target is a
// resource ID ending in /namespaces/<ns>/queues/<queue> or /topics/<topic>; Body is the
// message body and Attributes become application properties.
func (s *Service) deliver(target string, m *core.Message) error {
	parts := strings.Split(strings.Trim(target, "/"), "/")
	i := slices.IndexFunc(parts, func(p string) bool { return strings.EqualFold(p, "namespaces") })
	if i < 0 || len(parts) != i+4 || !slices.Contains([]string{"queues", "topics"}, strings.ToLower(parts[i+2])) {
		return fmt.Errorf("servicebus: %q is not a queue or topic resource ID", target)
	}
	am := &amqp.Message{Data: [][]byte{[]byte(m.Body)}, Properties: &amqp.Properties{Subject: m.Subject, GroupID: m.GroupID}}
	if m.DedupID != "" {
		am.Properties.MessageID = m.DedupID
	}
	if b := bytes.TrimSpace([]byte(m.Body)); len(b) > 0 && json.Valid(b) && (b[0] == '{' || b[0] == '[') {
		am.Properties.ContentType = "application/json"
	}
	if len(m.Attributes) > 0 {
		if am.ApplicationProperties == nil {
			am.ApplicationProperties = map[string]any{}
		}
		for k, a := range m.Attributes {
			switch {
			case len(a.BinaryValue) > 0:
				am.ApplicationProperties[k] = a.BinaryValue
			case strings.HasPrefix(a.DataType, "Number"):
				if n, err := strconv.ParseInt(a.StringValue, 10, 64); err == nil {
					am.ApplicationProperties[k] = n
				} else if f, err := strconv.ParseFloat(a.StringValue, 64); err == nil {
					am.ApplicationProperties[k] = f
				} else {
					am.ApplicationProperties[k] = a.StringValue
				}
			default:
				am.ApplicationProperties[k] = a.StringValue
			}
		}
	}
	sm := ingest(am)
	sm.Trace = cmp.Or(sm.Trace, m.Trace)
	// ponytail: a resource ID names an ARM namespace, AMQP clients connecting by IP or localhost are in
	// "localhost"; look there when the named one has no such entity.
	for _, ns := range []string{strings.ToLower(parts[i+1]), "localhost"} {
		if err := s.sendMsg(ns, parts[i+3], sm); err == nil {
			return nil
		} else if ae, ok := err.(*core.Error); !ok || ae.Status != 404 {
			return err
		}
	}
	return fmt.Errorf("servicebus: %s does not exist", target)
}

// Seed creates a queue ("queue") or topic ("topic") from the friendly properties of pogi.yaml unless it
// exists: lockDuration, maxDeliveryCount, requiresSession, defaultMessageTimeToLive and the rest of
// Props under their JSON names. A topic may list subscriptions: names, or maps with a name, the same
// properties, and sqlFilter and sqlAction for the default rule.
func (s *Service) Seed(kind, ns, name string, props map[string]any) error {
	var subs []any
	if kind == "topic" {
		subs, _ = props["subscriptions"].([]any)
		props = withoutKey(props, "subscriptions")
	}
	p, err := decodeProps(props)
	if err != nil {
		return err
	}
	switch kind {
	case "queue":
		_, err = s.CreateQueue(ns, name, p)
	default:
		_, err = s.CreateTopic(ns, name, p)
	}
	if isConflict(err) {
		err = nil // already there: still add its missing subscriptions
	}
	if err != nil {
		return err
	}
	for _, x := range subs {
		sp, _ := x.(map[string]any)
		if sp == nil {
			sp = map[string]any{"name": x}
		}
		subName, _ := sp["name"].(string)
		if subName == "" {
			return fmt.Errorf("subscription without a name")
		}
		rest := withoutKey(withoutKey(withoutKey(sp, "name"), "sqlFilter"), "sqlAction")
		props, err := decodeProps(rest)
		if err != nil {
			return err
		}
		var def *Rule
		if f, ok := sp["sqlFilter"].(string); ok {
			def = &Rule{Name: "$Default", Filter: Filter{Type: "SqlFilter", SQL: f}}
			def.Action, _ = sp["sqlAction"].(string)
		}
		if _, err := s.CreateSubscription(ns, name, subName, props, def); err != nil && !isConflict(err) {
			return err
		}
	}
	return nil
}

func isConflict(err error) bool {
	e, ok := err.(*core.Error)
	return ok && e.Status == 409
}

func withoutKey(m map[string]any, k string) map[string]any {
	out := make(map[string]any, len(m))
	for key, v := range m {
		if key != k {
			out[key] = v
		}
	}
	return out
}

// decodeProps reads friendly properties, refusing names Props does not have.
func decodeProps(m map[string]any) (Props, error) {
	var p Props
	b, _ := json.Marshal(m)
	d := json.NewDecoder(bytes.NewReader(b))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, fmt.Errorf("%v", err)
	}
	return p, nil
}
