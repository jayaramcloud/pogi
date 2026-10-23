package generic

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// IoT Hub: a hub has a host name, the Event Hubs-compatible endpoint its device messages go to, shared access
// policies with keys, and a $Default consumer group. No device can connect and no message is kept.

const iotHubs = "Microsoft.Devices/IotHubs"

var iotPolicies = []struct {
	name   string
	rights string
}{
	{"iothubowner", "RegistryWrite, ServiceConnect, DeviceConnect"},
	{"service", "ServiceConnect"},
	{"device", "DeviceConnect"},
	{"registryRead", "RegistryRead"},
	{"registryReadWrite", "RegistryWrite"},
}

func init() {
	Override(iotHubs+"/eventHubEndpoints/ConsumerGroups", Hook{Created: http.StatusOK}) // the client accepts only 200
	Override(iotHubs, Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			if sku, _ := r["sku"].(map[string]any); sku != nil && sku["tier"] == nil {
				name, _ := sku["name"].(string)
				if name != "" {
					sku["tier"] = map[string]string{"F": "Free", "B": "Basic", "S": "Standard"}[name[:1]]
				}
			}
			p := armres.Props(r)
			old, exists := s.Raw(id)
			armres.Defaults(p, map[string]any{
				"routing": map[string]any{
					"endpoints": map[string]any{"serviceBusQueues": []any{}, "serviceBusTopics": []any{}, "eventHubs": []any{}, "storageContainers": []any{}},
					"routes":    []any{}, "enrichments": []any{},
					"fallbackRoute": map[string]any{"name": "$fallback", "source": "DeviceMessages", "condition": "true", "endpointNames": []any{"events"}, "isEnabled": true},
				},
				"storageEndpoints":              map[string]any{"$default": map[string]any{"sasTtlAsIso8601": "PT1H", "connectionString": "", "containerName": ""}},
				"messagingEndpoints":            map[string]any{"fileNotifications": map[string]any{"lockDurationAsIso8601": "PT1M", "ttlAsIso8601": "PT1H", "maxDeliveryCount": 10}},
				"enableFileUploadNotifications": false,
				"cloudToDevice": map[string]any{"maxDeliveryCount": 10, "defaultTtlAsIso8601": "PT1H",
					"feedback": map[string]any{"lockDurationAsIso8601": "PT1M", "ttlAsIso8601": "PT1H", "maxDeliveryCount": 10}},
				"features": "None", "ipFilterRules": []any{}, "state": "Active",
			})
			ev, _ := p["eventHubEndpoints"].(map[string]any)
			if ev == nil {
				ev = map[string]any{}
			}
			events, _ := ev["events"].(map[string]any)
			if events == nil {
				events = map[string]any{}
			}
			armres.Defaults(events, map[string]any{"retentionTimeInDays": 1, "partitionCount": 4})
			ev["events"], p["eventHubEndpoints"] = events, ev
			// Policies the client sends replace the defaults; a PUT without any keeps the ones the hub has.
			pol, _ := p["authorizationPolicies"].([]any)
			if len(pol) == 0 && exists {
				pol, _ = armres.Props(old)["authorizationPolicies"].([]any)
			}
			if len(pol) == 0 {
				for _, d := range iotPolicies {
					pol = append(pol, map[string]any{"keyName": d.name, "rights": d.rights})
				}
			}
			for i, v := range pol {
				m, _ := v.(map[string]any)
				if m == nil {
					continue
				}
				kid := id.Child("IotHubKeys", fmt.Sprint(m["keyName"]))
				a, b := accessKeys(kid)
				armres.Defaults(m, map[string]any{"primaryKey": a[:44], "secondaryKey": b[:44]})
				pol[i] = m
			}
			p["authorizationPolicies"] = pol
			if exists {
				return nil
			}
			return s.Create(c, id.Child("eventHubEndpoints", "events").Child("ConsumerGroups", "$Default"), map[string]any{"properties": map[string]any{"name": "$Default"}})
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			p["hostName"] = id.Name() + ".azure-devices.net"
			if ev, _ := p["eventHubEndpoints"].(map[string]any); ev != nil {
				if events, _ := ev["events"].(map[string]any); events != nil {
					n, _ := events["partitionCount"].(float64)
					ids := []any{}
					for i := 0; i < int(n); i++ {
						ids = append(ids, fmt.Sprint(i))
					}
					events["partitionIds"], events["path"] = ids, id.Name()
					events["endpoint"] = fmt.Sprintf("sb://iothub-ns-%s-%s.servicebus.windows.net/", strings.ToLower(id.Name()), hash8(id.String()))
				}
			}
			r["properties"] = p
		},
		Actions: map[string]func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error){
			// the policies with their keys, as a list
			"listkeys": func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				hub, _ := s.Raw(id)
				pol, _ := armres.Props(hub)["authorizationPolicies"].([]any)
				if pol == nil {
					pol = []any{}
				}
				return map[string]any{"value": pol}, nil
			},
		},
		// The endpoint a consumer group lives under: every hub has "events".
		Extra: func(s *Service) []arm.Provider {
			return []arm.Provider{
				{Type: iotHubs + "/eventHubEndpoints", Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
					if !s.Has(id.Parent()) || !strings.EqualFold(id.Name(), "events") {
						return nil, arm.NotFound(id)
					}
					return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "properties": map[string]any{}}, nil
				}},
				// POST .../IotHubs/<hub>/IotHubKeys/<policy>/listkeys: one policy with its keys
				{Type: iotHubs + "/IotHubKeys", Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
					hub, ok := s.Raw(id.Parent())
					pol, _ := armres.Props(hub)["authorizationPolicies"].([]any)
					for _, v := range pol {
						if m, _ := v.(map[string]any); ok && m != nil && strings.EqualFold(fmt.Sprint(m["keyName"]), id.Name()) {
							return m, nil
						}
					}
					return nil, arm.NotFound(id)
				}, Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
					"listkeys": func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
						hub, _ := s.Raw(id.Parent())
						pol, _ := armres.Props(hub)["authorizationPolicies"].([]any)
						for _, v := range pol {
							if m, _ := v.(map[string]any); m != nil && strings.EqualFold(fmt.Sprint(m["keyName"]), id.Name()) {
								return m, nil
							}
						}
						return nil, arm.NotFound(id)
					},
				}},
			}
		},
	})
}
