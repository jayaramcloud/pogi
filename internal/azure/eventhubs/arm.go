package eventhubs

import (
	"errors"
	"net/http"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: namespaces, their authorization rules, event hubs
// and consumer groups, through the control-plane methods.

const namespaces = "Microsoft.EventHub/namespaces"

func (s *Service) registerARM() {
	h := s.h
	arm.Register(h, arm.Provider{Type: namespaces, Async: true, Put: s.putNamespace, Get: s.getNamespace, Delete: s.deleteNamespace, List: s.listNamespaces})
	arm.Register(h, armres.ServiceProvider(namespaces+"/networkRuleSets", func(p map[string]any) map[string]any {
		return armres.Defaults(p, map[string]any{"defaultAction": "Allow", "publicNetworkAccess": "Enabled", "trustedServiceAccessEnabled": false,
			"virtualNetworkRules": []any{}, "ipRules": []any{}})
	}, &s.mu, &s.st.ARM))
	for _, t := range []string{namespaces, namespaces + "/eventhubs"} {
		arm.Register(h, armres.RuleProvider(t+"/authorizationRules", "amqp", 5672, &s.mu, &s.st.ARM))
	}
	arm.Register(h, arm.Provider{Type: namespaces + "/eventhubs", Created: 200, Put: s.putHub, Get: s.getHub, Delete: s.deleteHub, List: s.listHubs})
	arm.Register(h, arm.Provider{Type: namespaces + "/eventhubs/consumergroups", Created: 200, Put: s.putGroup, Get: s.getGroup, Delete: s.deleteGroup, List: s.listGroups})
}

func iso(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.0000000Z") }

// armErr gives the control plane's errors their ARM codes.
func armErr(err error) error {
	switch {
	case errors.Is(err, ErrNotFound):
		return core.Errorf(http.StatusNotFound, "ResourceNotFound", "%v", err)
	case errors.Is(err, ErrExists):
		return core.Errorf(http.StatusConflict, "Conflict", "%v", err)
	case errors.Is(err, ErrInvalid):
		return core.Errorf(http.StatusBadRequest, "BadRequest", "%v", err)
	}
	return err
}

// ---- namespaces ----

func (s *Service) namespaceView(c *core.Ctx, r map[string]any, created time.Time) map[string]any {
	p, name := armres.Props(r), armres.Str(r, "name")
	armres.Defaults(r, map[string]any{"sku": map[string]any{"name": "Standard", "tier": "Standard", "capacity": 1}})
	armres.Defaults(armres.Props(r), map[string]any{"isAutoInflateEnabled": false, "maximumThroughputUnits": 0, "kafkaEnabled": true,
		"zoneRedundant": false, "disableLocalAuth": false, "minimumTlsVersion": "1.2", "publicNetworkAccess": "Enabled",
		"privateEndpointConnections": []any{}})
	p["provisioningState"], p["status"] = "Succeeded", "Active"
	p["createdAt"], p["updatedAt"] = iso(created), iso(created)
	p["metricId"] = core.AzureSubscription + ":" + name
	p["serviceBusEndpoint"] = armres.URL(c, "https", name, "") + "/"
	return r
}

func (s *Service) created(name string) (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.st.Namespaces[k(name)]
	if !ok {
		return time.Time{}, false
	}
	return n.Created, true
}

func (s *Service) putNamespace(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	if err := s.CreateNamespace(id.Name()); err != nil && !errors.Is(err, ErrExists) {
		return nil, armErr(err)
	}
	s.mu.Lock()
	s.st.ARM.Put(id, armres.Envelope(id, body))
	s.mu.Unlock()
	return s.getNamespace(c, id)
}

func (s *Service) getNamespace(c *core.Ctx, id arm.ID) (map[string]any, error) {
	created, ok := s.created(id.Name())
	s.mu.Lock()
	r, stored := s.st.ARM.Get(id)
	s.mu.Unlock()
	if !ok || !stored {
		return nil, arm.NotFound(id)
	}
	return s.namespaceView(c, r, created), nil
}

func (s *Service) deleteNamespace(c *core.Ctx, id arm.ID) error {
	if err := s.DeleteNamespace(id.Name()); err != nil && !errors.Is(err, ErrNotFound) {
		return armErr(err)
	}
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
		if created, ok := s.created(armres.Str(r, "name")); ok {
			out = append(out, s.namespaceView(c, r, created))
		}
	}
	return out, nil
}

// ---- event hubs ----

func hubView(id arm.ID, h HubInfo) map[string]any {
	hours := int(h.Retention / time.Hour)
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": "eastus", "properties": map[string]any{
		"partitionCount": len(h.PartitionIDs), "partitionIds": h.PartitionIDs, "status": "Active",
		"messageRetentionInDays": (hours + 23) / 24, "retentionDescription": map[string]any{"cleanupPolicy": "Delete", "retentionTimeInHours": hours},
		"createdAt": iso(h.Created), "updatedAt": iso(h.Created),
	}}
}

func (s *Service) putHub(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	p := armres.Props(body)
	partitions, _ := p["partitionCount"].(float64)
	var retention time.Duration
	if d, ok := p["messageRetentionInDays"].(float64); ok {
		retention = time.Duration(d) * 24 * time.Hour
	}
	if rd, ok := p["retentionDescription"].(map[string]any); ok {
		if hrs, ok := rd["retentionTimeInHours"].(float64); ok {
			retention = time.Duration(hrs) * time.Hour
		}
	}
	ns, name := id.Names[0], id.Name()
	// the partition count is fixed for life: a PUT of an existing hub keeps it
	if err := s.CreateHub(ns, name, int(partitions), retention); err != nil && !errors.Is(err, ErrExists) {
		return nil, armErr(err)
	}
	return s.getHub(c, id)
}

func (s *Service) getHub(c *core.Ctx, id arm.ID) (map[string]any, error) {
	h, err := s.GetHub(id.Names[0], id.Name())
	if err != nil {
		return nil, armErr(err)
	}
	return hubView(id, h), nil
}

func (s *Service) deleteHub(c *core.Ctx, id arm.ID) error {
	if err := s.DeleteHub(id.Names[0], id.Name()); err != nil && !errors.Is(err, ErrNotFound) {
		return armErr(err)
	}
	s.mu.Lock()
	s.st.ARM.Delete(id) // its authorization rules
	s.mu.Unlock()
	return nil
}

func (s *Service) listHubs(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	hubs, err := s.Hubs(parent.Name())
	if err != nil {
		return nil, armErr(err)
	}
	var out []map[string]any
	for _, h := range hubs {
		out = append(out, hubView(parent.Child("eventhubs", h.Name), h))
	}
	return out, nil
}

// ---- consumer groups ----

func groupView(id arm.ID, h HubInfo) map[string]any {
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": "eastus",
		"properties": map[string]any{"createdAt": iso(h.Created), "updatedAt": iso(h.Created)}}
}

func (s *Service) putGroup(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	if err := s.CreateConsumerGroup(id.Names[0], id.Names[1], id.Name()); err != nil && !errors.Is(err, ErrExists) {
		return nil, armErr(err)
	}
	return s.getGroup(c, id)
}

func (s *Service) getGroup(c *core.Ctx, id arm.ID) (map[string]any, error) {
	h, err := s.GetHub(id.Names[0], id.Names[1])
	if err != nil {
		return nil, armErr(err)
	}
	if indexFold(h.ConsumerGroups, id.Name()) < 0 {
		return nil, arm.NotFound(id)
	}
	return groupView(id, h), nil
}

func (s *Service) deleteGroup(c *core.Ctx, id arm.ID) error {
	if err := s.DeleteConsumerGroup(id.Names[0], id.Names[1], id.Name()); err != nil && !errors.Is(err, ErrNotFound) {
		return armErr(err)
	}
	return nil
}

func (s *Service) listGroups(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	h, err := s.GetHub(parent.Names[0], parent.Name())
	if err != nil {
		return nil, armErr(err)
	}
	var out []map[string]any
	for _, g := range h.ConsumerGroups {
		out = append(out, groupView(parent.Child("consumergroups", g), h))
	}
	return out, nil
}
