package eventgrid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

func itoa(n int) string { return strconv.Itoa(n) }

// target finds the topic or domain a data-plane request is for: the host <name>.eventgrid.localhost, or
// the path /_azure/eventgrid/<name>/. path is what is left of the URL path.
func target(r *http.Request) (name, path string, ok bool) {
	if rest, ok := strings.CutPrefix(r.URL.Path, "/_azure/eventgrid/"); ok {
		name, path, _ = strings.Cut(rest, "/")
		return name, "/" + path, name != ""
	}
	label, base := core.SplitHost(r.Host, "")
	if label != "" && (base == "eventgrid.localhost" || strings.HasSuffix(base, ".eventgrid.localhost")) {
		return label, r.URL.Path, true
	}
	return "", "", false
}

func writeError(w http.ResponseWriter, c *core.Ctx, status int, code, msg string) {
	c.ErrCode = code
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code, "message": msg}})
}

// ServeHTTP is the data plane: POST /api/events.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	name, path, _ := target(r)
	c.Op = "PublishEvents"
	if path != "/api/events" || r.Method != http.MethodPost {
		writeError(w, c, http.StatusNotFound, "NotFound", "pogi serves POST /api/events on an Event Grid endpoint.")
		return
	}
	if err := c.Intercept(); err != nil {
		e := core.AsError(err)
		writeError(w, c, e.Status, e.Code, e.Msg)
		return
	}
	events, err := parseEvents(core.Body(r), strings.Contains(r.Header.Get("Content-Type"), "cloudevents"))
	if err != nil {
		writeError(w, c, http.StatusBadRequest, "BadRequest", err.Error())
		return
	}
	s.mu.Lock()
	topic, domain := s.st.Topics[name], s.st.Domains[name]
	s.mu.Unlock()
	switch {
	case topic != nil:
		for _, e := range events {
			s.fanout([]string{"topics/" + name}, resourceID("topics/"+name), e, c.Trace)
		}
	case domain != nil:
		for _, e := range events {
			t := e.Topic // the domain topic is named by the event
			if t == "" {
				writeError(w, c, http.StatusBadRequest, "BadRequest", "An event published to a domain must name its domain topic in topic.")
				return
			}
			s.fanout([]string{"domains/" + name, "domains/" + name + "/topics/" + t}, resourceID("domains/"+name+"/topics/"+t), e, c.Trace)
		}
	default:
		writeError(w, c, http.StatusNotFound, "ResourceNotFound", "The topic or domain "+name+" does not exist.")
		return
	}
	core.Go("eventgrid.Tick", func() { s.Tick(time.Now()) })
	w.WriteHeader(http.StatusOK)
}

// parseEvents reads a batch of Event Grid events, or CloudEvents (one object, or an array).
func parseEvents(body []byte, cloud bool) ([]Event, error) {
	body = bytes.TrimSpace(body)
	if len(body) > 0 && body[0] == '{' {
		body = append(append([]byte{'['}, body...), ']')
	}
	var in []map[string]json.RawMessage
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("The request body is not a JSON array of events: %v", err)
	}
	if len(in) == 0 {
		return nil, fmt.Errorf("The request has no events.")
	}
	str := func(m map[string]json.RawMessage, k string) string {
		var v string
		json.Unmarshal(m[k], &v)
		return v
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	var out []Event
	for i, m := range in {
		var e Event
		if _, ce := m["specversion"]; ce || cloud {
			e = Event{ID: str(m, "id"), Source: str(m, "source"), EventType: str(m, "type"), Subject: str(m, "subject"),
				EventTime: orStr(str(m, "time"), now), DataVersion: str(m, "dataversion"), Data: m["data"], DataContentType: str(m, "datacontenttype")}
			e.Topic = e.Source // for domains: the domain topic
			for k, v := range map[string]string{"id": e.ID, "source": e.Source, "type": e.EventType, "specversion": str(m, "specversion")} {
				if v == "" {
					return nil, fmt.Errorf("Event %d is not a CloudEvent: the required attribute %s is missing.", i, k)
				}
			}
		} else {
			e = Event{ID: str(m, "id"), Topic: str(m, "topic"), Subject: str(m, "subject"), EventType: str(m, "eventType"),
				EventTime: str(m, "eventTime"), DataVersion: str(m, "dataVersion"), Data: m["data"]}
			for k, v := range map[string]string{"id": e.ID, "subject": e.Subject, "eventType": e.EventType, "eventTime": e.EventTime, "dataVersion": e.DataVersion} {
				if v == "" {
					return nil, fmt.Errorf("Event %d is not an Event Grid event: the required property %s is missing.", i, k)
				}
			}
		}
		out = append(out, e)
	}
	return out, nil
}

// fanout queues e for every subscription of the scopes whose filter it passes. topicID is the topic it was
// published to, by resource ID. Call Tick afterwards to deliver.
func (s *Service) fanout(scopes []string, topicID string, e Event, trace string) {
	e.Topic = topicID
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	for _, k := range core.SortedKeys(s.st.Subs) {
		sub := s.st.Subs[k]
		if !contains(scopes, sub.Scope) || !sub.Filter.match(e) {
			continue
		}
		s.st.Pending = append(s.st.Pending, &Delivery{Sub: k, Event: e, Trace: trace, Due: now, Expires: now.Add(time.Duration(sub.Retry.TTLMinutes) * time.Minute)})
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// system takes an event of an Azure resource (Blob's BlobCreated) to the system topics of that resource.
func (s *Service) system(_ string, m *core.Message) error {
	e := Event{ID: core.UUID(), Subject: m.Subject, EventType: m.DetailType, EventTime: time.Now().UTC().Format(time.RFC3339Nano), Data: json.RawMessage(m.Body)}
	if !json.Valid(e.Data) {
		e.Data, _ = json.Marshal(m.Body)
	}
	s.mu.Lock()
	var scopes []string
	for _, k := range core.SortedKeys(s.st.System) {
		if strings.EqualFold(s.st.System[k].Source, m.Source) {
			scopes = append(scopes, "systemTopics/"+k)
		}
	}
	s.mu.Unlock()
	for _, scope := range scopes {
		s.fanout([]string{scope}, m.Source, e, m.Trace)
	}
	if len(scopes) > 0 {
		core.Go("eventgrid.Tick", func() { s.Tick(time.Now()) })
	}
	return nil
}
