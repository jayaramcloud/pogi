package eventhubs

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/amqp"
	"github.com/nrndev/pogi/internal/core"
)

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Namespaces == nil {
		st.Namespaces = map[string]*Namespace{}
	}
	for _, n := range st.Namespaces {
		if n.Hubs == nil {
			n.Hubs = map[string]*Hub{}
		}
		for _, h := range n.Hubs {
			for _, p := range h.Partitions {
				for _, e := range p.Events {
					normalizeMsg(e.Msg)
				}
			}
		}
	}
	s.mu.Lock()
	closing := s.takeAll()
	s.st = st
	s.kafka = map[string]string{}
	s.mu.Unlock()
	closing.detach()
	return nil
}

// normalizeMsg restores the integers JSON turned into floats.
// ponytail: other types in application properties and annotations come back
// as JSON's (a []byte is a base64 string, a time a string); only bodies are exact.
func normalizeMsg(m *amqp.Message) {
	if m == nil {
		return
	}
	m.Value = normalize(m.Value)
	for _, x := range []map[string]any{m.ApplicationProperties, m.MessageAnnotations, m.DeliveryAnnotations, m.Footer} {
		normalize(x)
	}
}

func normalize(v any) any {
	switch x := v.(type) {
	case float64:
		if x == math.Trunc(x) && math.Abs(x) < 1<<62 {
			return int64(x)
		}
	case map[string]any:
		for k, e := range x {
			x[k] = normalize(e)
		}
	case []any:
		for i, e := range x {
			x[i] = normalize(e)
		}
	}
	return v
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	ns := core.Table{Name: "Namespaces", Columns: []string{"Name", "Event hubs", "Kafka"}}
	hubs := core.Table{Name: "Event hubs", Columns: []string{"Name", "Namespace", "Partitions", "Retention", "Events", "Consumer groups"}}
	groups := core.Table{Name: "Consumer groups", Columns: []string{"Name", "Namespace", "Event hub"}}
	for _, nk := range core.SortedKeys(s.st.Namespaces) {
		n := s.st.Namespaces[nk]
		kafka := ""
		if n.Container != "" {
			if kafka = s.kafka[nk]; kafka == "" {
				kafka = "stopped"
			}
		}
		ns.Rows = append(ns.Rows, []string{n.Name, strconv.Itoa(len(n.Hubs)), kafka})
		for _, hk := range core.SortedKeys(n.Hubs) {
			h := n.Hubs[hk]
			events := 0
			for _, p := range h.Partitions {
				events += len(p.Events)
			}
			hubs.Rows = append(hubs.Rows, []string{n.Name + "/" + h.Name, n.Name, strconv.Itoa(len(h.Partitions)), h.Retention.String(),
				strconv.Itoa(events), strconv.Itoa(len(h.ConsumerGroups))})
			for _, g := range h.ConsumerGroups {
				groups.Rows = append(groups.Rows, []string{n.Name + "/" + h.Name + "/" + g, n.Name, h.Name})
			}
		}
	}
	return []core.Table{ns, hubs, groups}
}

// Inspect describes a namespace (id "ns"), an event hub ("ns/hub") with its
// partitions, or a consumer group ("ns/hub/group"); table is not needed.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := strings.SplitN(id, "/", 3)
	n, ok := s.st.Namespaces[k(p[0])]
	if !ok {
		return nil
	}
	if len(p) == 1 {
		var names []string
		for _, hk := range core.SortedKeys(n.Hubs) {
			names = append(names, n.Hubs[hk].Name)
		}
		return map[string]any{"name": n.Name, "created": n.Created, "eventHubs": names, "kafkaContainer": n.Container, "kafkaBootstrap": s.kafka[k(p[0])]}
	}
	h, ok := n.Hubs[k(p[1])]
	if !ok {
		return nil
	}
	if len(p) == 3 {
		if !h.hasGroup(p[2]) {
			return nil
		}
		return map[string]any{"name": p[2], "namespace": n.Name, "eventHub": h.Name}
	}
	var parts []map[string]any
	for _, pt := range h.Partitions {
		parts = append(parts, map[string]any{"id": pt.ID, "events": len(pt.Events), "beginSequenceNumber": pt.begin(),
			"lastEnqueuedSequenceNumber": pt.Next - 1, "lastEnqueuedOffset": fmt.Sprint(pt.LastOffset), "lastEnqueuedTime": pt.LastTime})
	}
	return map[string]any{"name": h.Name, "namespace": n.Name, "created": h.Created, "retention": h.Retention.String(),
		"consumerGroups": h.ConsumerGroups, "partitions": parts}
}
