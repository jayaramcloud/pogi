package eventgrid

import (
	"encoding/json"
	"errors"
	"net/http"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: custom topics, domains and their topics, system
// topics, and event subscriptions on all of them. An event subscription's ID
// carries its scope: .../topics/t/providers/Microsoft.EventGrid/eventSubscriptions/s.
// The resource manager hands such an ID to the Provider of Microsoft.EventGrid/eventSubscriptions
// (arm.ParseID reads the scope as a type chain with a "providers" link in it); scope() finds the
// scope again. Subscriptions of a system topic are a child type of it.
// ponytail: event subscriptions on storage accounts or resource groups are not
// served (blob events reach a system topic instead).

const eg = "Microsoft.EventGrid/"

func (s *Service) registerARM() {
	h := s.hub
	keys := map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": s.listKeys, "regenerateKey": s.listKeys}
	arm.Register(h, arm.Provider{Type: eg + "topics", Async: true, Put: s.putTopic, Get: s.getTopic, Delete: s.deleteTopic, List: s.listTopics(eg + "topics"), Actions: keys})
	arm.Register(h, arm.Provider{Type: eg + "domains", Async: true, Put: s.putTopic, Get: s.getTopic, Delete: s.deleteTopic, List: s.listTopics(eg + "domains"), Actions: keys})
	arm.Register(h, arm.Provider{Type: eg + "domains/topics", Put: s.putTopic, Get: s.getTopic, Delete: s.deleteTopic, List: s.listTopics(eg + "domains/topics")})
	arm.Register(h, arm.Provider{Type: eg + "systemTopics", Put: s.putTopic, Get: s.getTopic, Delete: s.deleteTopic, List: s.listTopics(eg + "systemTopics")})
	sub := arm.Provider{Put: s.putSub, Get: s.getSub, Delete: s.deleteSub, List: s.listSubs}
	for _, t := range []string{"eventSubscriptions", "systemTopics/eventSubscriptions"} {
		sub.Type = eg + t
		arm.Register(h, sub)
	}
}

// scope is the Event Grid scope an ID is under or names: topics/t, domains/d, domains/d/topics/t, systemTopics/s.
func scope(id arm.ID) string {
	s := id.String()
	if _, rest, ok := strings.Cut(s, "/providers/"+eg); ok {
		s = rest
	}
	s, _, _ = strings.Cut(s, "/providers/") // topics/t/providers/Microsoft.EventGrid/eventSubscriptions/x
	s, _, _ = strings.Cut(s, "/eventSubscriptions")
	return strings.Trim(s, "/")
}

// armErr gives the engine's 404 and 409 their ARM codes.
func armErr(err error) error {
	var e *core.Error
	if errors.As(err, &e) && e.Status == http.StatusNotFound {
		return core.Errorf(http.StatusNotFound, "ResourceNotFound", "%s", e.Msg)
	}
	return err
}

// ---- topics, domains, domain topics and system topics ----

func exists(err error) bool {
	var e *core.Error
	return errors.As(err, &e) && e.Status == http.StatusConflict
}

func (s *Service) putTopic(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	p, name := armres.Props(body), id.Name()
	var err error
	switch id.Type() {
	case eg + "topics":
		err = s.CreateTopic(name, armres.Str(p, "inputSchema"))
	case eg + "domains":
		err = s.CreateDomain(name, armres.Str(p, "inputSchema"))
	case eg + "domains/topics":
		err = s.CreateDomainTopic(id.Names[0], name)
	default:
		err = s.CreateSystemTopic(name, armres.Str(p, "source"), armres.Str(p, "topicType"))
	}
	if err != nil && !exists(err) {
		return nil, armErr(err)
	}
	s.mu.Lock()
	// events name their topic by its ID, in this resource group (nil: deleted meanwhile)
	if t := s.st.Topics[name]; t != nil && id.Type() == eg+"topics" {
		t.Group = id.ResourceGroup
	} else if d := s.st.Domains[name]; d != nil && id.Type() == eg+"domains" {
		d.Group = id.ResourceGroup
	} else if t := s.st.System[name]; t != nil && id.Type() == eg+"systemTopics" {
		t.Group = id.ResourceGroup
	}
	s.st.ARM.Put(id, armres.Envelope(id, body))
	s.mu.Unlock()
	return s.getTopic(c, id)
}

func (s *Service) getTopic(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var inputSchema, source, topicType string
	switch id.Type() {
	case eg + "topics":
		t := s.st.Topics[id.Name()]
		if t == nil {
			return nil, arm.NotFound(id)
		}
		inputSchema = t.InputSchema
	case eg + "domains":
		d := s.st.Domains[id.Name()]
		if d == nil {
			return nil, arm.NotFound(id)
		}
		inputSchema = d.InputSchema
	case eg + "domains/topics":
		if d := s.st.Domains[id.Names[0]]; d == nil || !slices.Contains(d.Topics, id.Name()) {
			return nil, arm.NotFound(id)
		}
	default:
		t := s.st.System[id.Name()]
		if t == nil {
			return nil, arm.NotFound(id)
		}
		source, topicType = t.Source, t.TopicType
	}
	r, _ := s.st.ARM.Get(id)
	r = armres.Envelope(id, r)
	if id.Type() == eg+"domains/topics" {
		delete(r, "location")
		delete(r, "tags")
	}
	p := armres.Props(r)
	p["provisioningState"] = "Succeeded"
	switch id.Type() {
	case eg + "topics", eg + "domains":
		p["endpoint"] = armres.URL(c, "https", id.Name()+".eventgrid", "/_azure/eventgrid/"+id.Name()) + "/api/events"
		p["inputSchema"], p["publicNetworkAccess"], p["disableLocalAuth"] = inputSchema, "Enabled", false
		p["metricResourceId"] = core.AzureSubscription
		armres.Defaults(p, map[string]any{"inboundIpRules": []any{}, "dataResidencyBoundary": "WithinGeopair", "minimumTlsVersionAllowed": "1.2"})
	case eg + "systemTopics":
		p["source"], p["topicType"], p["metricResourceId"] = source, topicType, core.AzureSubscription
	}
	return r, nil
}

func (s *Service) deleteTopic(c *core.Ctx, id arm.ID) error {
	var err error
	switch id.Type() {
	case eg + "topics":
		err = s.DeleteTopic(id.Name())
	case eg + "domains":
		err = s.DeleteDomain(id.Name())
	case eg + "domains/topics":
		err = s.DeleteDomainTopic(id.Names[0], id.Name())
	default:
		err = s.DeleteSystemTopic(id.Name())
	}
	if err != nil && core.AsError(err).Status != http.StatusNotFound {
		return err
	}
	s.mu.Lock()
	s.st.ARM.Delete(id)
	s.mu.Unlock()
	return nil
}

// listTopics lists the topics, domains, domain topics or system topics (typ) the resource manager made under parent.
func (s *Service) listTopics(typ string) func(*core.Ctx, arm.ID) ([]map[string]any, error) {
	return func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
		s.mu.Lock()
		stored := s.st.ARM.List(parent, typ)
		s.mu.Unlock()
		var out []map[string]any
		for _, r := range stored {
			id, _ := arm.ParseID(armres.Str(r, "id"))
			if r, err := s.getTopic(c, id); err == nil {
				out = append(out, r)
			}
		}
		return out, nil
	}
}

func (s *Service) listKeys(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	if _, err := s.getTopic(c, id); err != nil {
		return nil, err
	}
	return map[string]any{"key1": armres.Key("eventgrid/1/" + id.Name())[:44], "key2": armres.Key("eventgrid/2/" + id.Name())[:44]}, nil
}

// ---- event subscriptions ----

func (s *Service) subID(sc, name string) arm.ID {
	suffix := "/providers/" + eg + "eventSubscriptions/" + name
	if strings.HasPrefix(sc, "systemTopics/") {
		suffix = "/eventSubscriptions/" + name
	}
	s.mu.Lock()
	id, _ := arm.ParseID(s.topicID(sc) + suffix)
	s.mu.Unlock()
	return id
}

func (s *Service) subView(c *core.Ctx, id arm.ID, sub Subscription) map[string]any {
	dest := map[string]any{"endpointType": sub.Destination.Type}
	if sub.Destination.Type == "WebHook" {
		dest["properties"] = map[string]any{"endpointUrl": sub.Destination.Endpoint}
	} else if sub.Destination.QueueName != "" {
		dest["properties"] = map[string]any{"resourceId": sub.Destination.ResourceID, "queueName": sub.Destination.QueueName}
	} else {
		dest["properties"] = map[string]any{"resourceId": sub.Destination.ResourceID}
	}
	filter := map[string]any{"isSubjectCaseSensitive": sub.Filter.IsSubjectCaseSensitive, "includedEventTypes": nilEmpty(sub.Filter.IncludedEventTypes)}
	if sub.Filter.SubjectBeginsWith != "" {
		filter["subjectBeginsWith"] = sub.Filter.SubjectBeginsWith
	}
	if sub.Filter.SubjectEndsWith != "" {
		filter["subjectEndsWith"] = sub.Filter.SubjectEndsWith
	}
	if len(sub.Filter.Advanced) > 0 {
		filter["advancedFilters"] = sub.Filter.Advanced
	}
	s.mu.Lock()
	topic := s.topicID(sub.Scope)
	s.mu.Unlock()
	p := map[string]any{"topic": topic, "provisioningState": "Succeeded",
		"destination": dest, "filter": filter, "eventDeliverySchema": sub.Schema,
		"retryPolicy": map[string]any{"maxDeliveryAttempts": sub.Retry.MaxAttempts, "eventTimeToLiveInMinutes": sub.Retry.TTLMinutes}, "labels": sub.Labels}
	if sub.Labels == nil {
		p["labels"] = []any{}
	}
	if sub.DeadLetter != nil {
		p["deadLetterDestination"] = map[string]any{"endpointType": "StorageBlob", "properties": map[string]any{
			"resourceId": sub.DeadLetter.ResourceID, "blobContainerName": sub.DeadLetter.Container}}
	}
	return map[string]any{"id": id.String(), "name": id.Name(), "type": eg + "eventSubscriptions", "properties": p}
}

func nilEmpty(l []string) []string {
	if l == nil {
		return []string{}
	}
	return l
}

// subscription reads the engine's Subscription from a request body.
func subscription(id arm.ID, body map[string]any) (Subscription, error) {
	p := armres.Props(body)
	sub := Subscription{Name: id.Name()}
	if d, ok := p["destination"].(map[string]any); ok {
		dp := armres.Props(d)
		sub.Destination = Destination{Type: armres.Str(d, "endpointType"), Endpoint: armres.Str(dp, "endpointUrl"), ResourceID: armres.Str(dp, "resourceId"),
			QueueName: armres.Str(dp, "queueName")}
	}
	for k, v := range map[string]any{"filter": &sub.Filter, "retryPolicy": &sub.Retry} {
		if raw, ok := p[k]; ok {
			b, _ := json.Marshal(raw)
			if err := json.Unmarshal(b, v); err != nil {
				return sub, core.Errorf(http.StatusBadRequest, "InvalidRequest", "%s: %v", k, err)
			}
		}
	}
	sub.Schema = armres.Str(p, "eventDeliverySchema")
	if l, ok := p["labels"].([]any); ok {
		for _, x := range l {
			sub.Labels = append(sub.Labels, x.(string))
		}
	}
	if d, ok := p["deadLetterDestination"].(map[string]any); ok {
		dp := armres.Props(d)
		sub.DeadLetter = &DeadLetter{ResourceID: armres.Str(dp, "resourceId"), Container: armres.Str(dp, "blobContainerName")}
	}
	return sub, nil
}

func (s *Service) putSub(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	sub, err := subscription(id, body)
	if err != nil {
		return nil, err
	}
	sc := scope(id)
	if cur := s.find(sc, id.Name()); cur != nil { // a PUT replaces
		s.DeleteSubscription(sc, id.Name())
	}
	if err := s.CreateSubscription(sc, sub); err != nil {
		return nil, armErr(err)
	}
	return s.getSub(c, id)
}

func (s *Service) find(sc, name string) *Subscription {
	for _, sub := range s.Subscriptions(sc) {
		if sub.Name == name {
			return &sub
		}
	}
	return nil
}

func (s *Service) getSub(c *core.Ctx, id arm.ID) (map[string]any, error) {
	sub := s.find(scope(id), id.Name())
	if sub == nil {
		return nil, arm.NotFound(id)
	}
	return s.subView(c, id, *sub), nil
}

func (s *Service) deleteSub(c *core.Ctx, id arm.ID) error {
	if err := s.DeleteSubscription(scope(id), id.Name()); err != nil && core.AsError(err).Status != http.StatusNotFound {
		return err
	}
	return nil
}

func (s *Service) listSubs(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	sc := scope(parent)
	var out []map[string]any
	for _, sub := range s.Subscriptions(sc) {
		out = append(out, s.subView(c, s.subID(sc, sub.Name), sub))
	}
	return out, nil
}
