package servicebus

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: namespaces, their authorization rules and keys,
// queues, topics and subscriptions. Entities are the engine's own, reached
// through the exported methods; a namespace is made through CreateNamespace.
// ponytail: topic and subscription rules (Microsoft.ServiceBus/.../rules) are
// not served; the engine has AddRule and friends for when they are.

const namespaces = "Microsoft.ServiceBus/namespaces"

func (s *Service) registerARM() {
	h := s.hub
	arm.Register(h, arm.Provider{Type: namespaces, Async: true, Put: s.putNamespace, Get: s.getNamespace, Delete: s.deleteNamespace, List: s.listNamespaces})
	arm.Register(h, armres.ServiceProvider(namespaces+"/networkRuleSets", func(p map[string]any) map[string]any {
		return armres.Defaults(p, map[string]any{"defaultAction": "Allow", "publicNetworkAccess": "Enabled", "trustedServiceAccessEnabled": false,
			"virtualNetworkRules": []any{}, "ipRules": []any{}})
	}, &s.mu, &s.st.ARM))
	for _, t := range []string{namespaces, namespaces + "/queues", namespaces + "/topics"} {
		arm.Register(h, armres.RuleProvider(t+"/authorizationRules", "amqp", 5672, &s.mu, &s.st.ARM))
	}
	arm.Register(h, arm.Provider{Type: namespaces + "/queues", Created: 200, Put: s.putEntity("queue"), Get: s.getEntity("queue"),
		Delete: s.deleteEntity("queue"), List: s.listEntities("queue")})
	arm.Register(h, arm.Provider{Type: namespaces + "/topics", Created: 200, Put: s.putEntity("topic"), Get: s.getEntity("topic"),
		Delete: s.deleteEntity("topic"), List: s.listEntities("topic")})
	arm.Register(h, arm.Provider{Type: namespaces + "/topics/subscriptions", Created: 200, Put: s.putEntity("subscription"), Get: s.getEntity("subscription"),
		Delete: s.deleteEntity("subscription"), List: s.listEntities("subscription")})
}

func iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.0000000Z") }

// ---- namespaces ----

func (s *Service) namespaceView(c *core.Ctx, r map[string]any, created time.Time) map[string]any {
	p, name := armres.Props(r), armres.Str(r, "name")
	armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Standard", "tier": "Standard"}})
	sku := r["sku"].(map[string]any)
	armres.Defaults(sku, map[string]any{"tier": sku["name"]})
	armres.Defaults(p, map[string]any{"disableLocalAuth": false, "zoneRedundant": false, "minimumTlsVersion": "1.2",
		"publicNetworkAccess": "Enabled", "premiumMessagingPartitions": 0, "privateEndpointConnections": []any{}})
	p["provisioningState"], p["status"] = "Succeeded", "Active"
	p["createdAt"], p["updatedAt"] = iso(created), iso(created)
	p["metricId"] = core.AzureSubscription + ":" + name
	p["serviceBusEndpoint"] = armres.URL(c, "https", name, "") + "/"
	return r
}

func (s *Service) putNamespace(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	r := armres.Envelope(id, body)
	s.CreateNamespace(id.Name())
	s.mu.Lock()
	s.st.ARM.Put(id, r)
	s.mu.Unlock()
	return s.getNamespace(c, id)
}

func (s *Service) getNamespace(c *core.Ctx, id arm.ID) (map[string]any, error) {
	created, ok := s.GetNamespace(id.Name())
	s.mu.Lock()
	r, stored := s.st.ARM.Get(id)
	s.mu.Unlock()
	if !ok || !stored {
		return nil, arm.NotFound(id)
	}
	return s.namespaceView(c, r, created), nil
}

func (s *Service) deleteNamespace(c *core.Ctx, id arm.ID) error {
	s.DeleteNamespace(id.Name())
	s.mu.Lock()
	s.st.ARM.Delete(id)
	s.mu.Unlock()
	return nil
}

func (s *Service) listNamespaces(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	stored := s.st.ARM.List(parent, namespaces)
	s.mu.Unlock()
	var out []map[string]any
	for _, r := range stored {
		if created, ok := s.GetNamespace(armres.Str(r, "name")); ok {
			out = append(out, s.namespaceView(c, r, created))
		}
	}
	return out, nil
}

// ---- queues, topics and subscriptions ----

func entityView(id arm.ID, e Entity) map[string]any {
	p := map[string]any{
		"status": e.Status, "enableBatchedOperations": e.EnableBatchedOperations, "autoDeleteOnIdle": e.AutoDeleteOnIdle.ISO(),
		"defaultMessageTimeToLive": e.DefaultMessageTimeToLive.ISO(), "createdAt": iso(e.Created), "updatedAt": iso(e.Updated),
		"accessedAt": "0001-01-01T00:00:00", "messageCount": e.Active + e.Dead + e.Scheduled + e.Deferred, "sizeInBytes": e.Size,
		"countDetails": map[string]any{"activeMessageCount": e.Active, "deadLetterMessageCount": e.Dead, "scheduledMessageCount": e.Scheduled,
			"transferMessageCount": 0, "transferDeadLetterMessageCount": 0},
	}
	if e.Kind != "subscription" {
		p["maxSizeInMegabytes"], p["requiresDuplicateDetection"] = e.MaxSizeInMegabytes, e.RequiresDuplicateDetection
		p["duplicateDetectionHistoryTimeWindow"], p["enablePartitioning"] = e.DuplicateDetectionHistoryWindow.ISO(), e.EnablePartitioning
		p["maxMessageSizeInKilobytes"], p["enableExpress"] = e.MaxMessageSizeInKilobytes, false
	}
	if e.Kind != "topic" {
		p["lockDuration"], p["maxDeliveryCount"], p["requiresSession"] = e.LockDuration.ISO(), e.MaxDeliveryCount, e.RequiresSession
		p["deadLetteringOnMessageExpiration"] = e.DeadLetteringOnMessageExpiration
		if e.ForwardTo != "" {
			p["forwardTo"] = e.ForwardTo
		}
		if e.ForwardDeadLetteredMessagesTo != "" {
			p["forwardDeadLetteredMessagesTo"] = e.ForwardDeadLetteredMessagesTo
		}
	}
	switch e.Kind {
	case "topic":
		p["subscriptionCount"], p["supportOrdering"] = e.Subscriptions, e.SupportOrdering
	case "subscription":
		p["deadLetteringOnFilterEvaluationExceptions"] = e.DeadLetteringOnFilterEvalFailures
		delete(p, "sizeInBytes")
	}
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": "eastus", "properties": p}
}

// props reads the settings of a request body; durations are ISO 8601.
func props(body map[string]any) (Props, error) {
	var p Props
	b, _ := json.Marshal(armres.Props(body))
	if err := json.Unmarshal(b, &p); err != nil {
		return p, core.Errorf(http.StatusBadRequest, "BadRequest", "%v", err)
	}
	if _, ok := armres.Props(body)["enableBatchedOperations"]; !ok {
		p.EnableBatchedOperations = true // Service Bus's default
	}
	return p, nil
}

// armErr maps the engine's errors, which already have ARM-like codes.
func armErr(err error) error {
	var e *core.Error
	if errors.As(err, &e) && e.Status == http.StatusNotFound {
		return core.Errorf(http.StatusNotFound, "ResourceNotFound", "%s", e.Msg)
	}
	return err
}

func (s *Service) putEntity(kind string) func(*core.Ctx, arm.ID, map[string]any) (map[string]any, error) {
	return func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
		p, err := props(body)
		if err != nil {
			return nil, err
		}
		ns, name := id.Names[0], id.Name()
		var e Entity
		_, exists := s.entity(kind, id)
		switch {
		case kind == "queue" && exists:
			e, err = s.UpdateQueue(ns, name, p)
		case kind == "queue":
			e, err = s.CreateQueue(ns, name, p)
		case kind == "topic" && exists:
			e, err = s.UpdateTopic(ns, name, p)
		case kind == "topic":
			e, err = s.CreateTopic(ns, name, p)
		case exists:
			e, err = s.UpdateSubscription(ns, id.Names[1], name, p)
		default:
			e, err = s.CreateSubscription(ns, id.Names[1], name, p, nil)
		}
		if err != nil {
			return nil, armErr(err)
		}
		return entityView(id, e), nil
	}
}

func (s *Service) entity(kind string, id arm.ID) (Entity, bool) {
	ns, name := id.Names[0], id.Name()
	switch kind {
	case "queue":
		return s.GetQueue(ns, name)
	case "topic":
		return s.GetTopic(ns, name)
	}
	return s.GetSubscription(ns, id.Names[1], name)
}

func (s *Service) getEntity(kind string) func(*core.Ctx, arm.ID) (map[string]any, error) {
	return func(c *core.Ctx, id arm.ID) (map[string]any, error) {
		e, ok := s.entity(kind, id)
		if !ok {
			return nil, arm.NotFound(id)
		}
		return entityView(id, e), nil
	}
}

func (s *Service) deleteEntity(kind string) func(*core.Ctx, arm.ID) error {
	return func(c *core.Ctx, id arm.ID) error {
		ns, name := id.Names[0], id.Name()
		switch kind {
		case "queue":
			s.DeleteQueue(ns, name)
		case "topic":
			s.DeleteTopic(ns, name)
		default:
			s.DeleteSubscription(ns, id.Names[1], name)
		}
		s.mu.Lock()
		s.st.ARM.Delete(id) // its authorization rules
		s.mu.Unlock()
		return nil
	}
}

func (s *Service) listEntities(kind string) func(*core.Ctx, arm.ID) ([]map[string]any, error) {
	return func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
		ns := parent.Names[0]
		var es []Entity
		switch kind {
		case "queue":
			es = s.ListQueues(ns)
		case "topic":
			es = s.ListTopics(ns)
		default:
			var err error
			if es, err = s.ListSubscriptions(ns, parent.Name()); err != nil {
				return nil, armErr(err)
			}
		}
		var out []map[string]any
		for _, e := range es {
			out = append(out, entityView(parent.Child(map[string]string{"queue": "queues", "topic": "topics", "subscription": "subscriptions"}[kind], e.Name), e))
		}
		return out, nil
	}
}
