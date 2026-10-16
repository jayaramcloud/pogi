package seed

import (
	"cmp"
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
)

// eventGrid is the Event Grid service; seeding reaches it through its Go methods.
type eventGrid interface {
	CreateTopic(name, inputSchema string) error
	CreateSystemTopic(name, source, topicType string) error
	AddSubscription(scope string, raw []byte) error
}

func eventGridSvc(s *seeder) (eventGrid, error) {
	eg, ok := s.h.Service("eventgrid").(eventGrid)
	if !ok {
		return nil, errors.New("service eventgrid is not enabled")
	}
	return eg, nil
}

// skipExisting turns "it exists" into success: seeding never updates.
func skipExisting(err error) error {
	if err != nil && core.AsError(err).Status == http.StatusConflict {
		return nil
	}
	return err
}

// eventGridTopic: inputSchema.
func eventGridTopic(s *seeder, it config.Item) error {
	eg, err := eventGridSvc(s)
	if err != nil {
		return err
	}
	if err := allow(it.Props, "inputSchema"); err != nil {
		return err
	}
	return skipExisting(eg.CreateTopic(it.Name, text(it.Props["inputSchema"])))
}

// eventGridSystemTopic: source (the resource ID of a storage account), topicType.
func eventGridSystemTopic(s *seeder, it config.Item) error {
	eg, err := eventGridSvc(s)
	if err != nil {
		return err
	}
	if err := allow(it.Props, "source", "topicType"); err != nil {
		return err
	}
	return skipExisting(eg.CreateSystemTopic(it.Name, text(it.Props["source"]), cmp.Or(text(it.Props["topicType"]), "Microsoft.Storage.StorageAccounts")))
}

// eventGridSubscription: topic (a custom topic's name, or a scope such as systemTopics/x or
// domains/d), one destination (endpoint for a webhook, or serviceBusQueue, serviceBusTopic, eventHub,
// storageQueue or function with a resource ID), schema (EventGridSchema or CloudEventSchemaV1_0),
// eventTypes, subjectBeginsWith, subjectEndsWith, advancedFilters, maxAttempts, ttlMinutes and
// deadLetterAccount with deadLetterContainer. A webhook is not validated when seeded.
func eventGridSubscription(s *seeder, it config.Item) error {
	eg, err := eventGridSvc(s)
	if err != nil {
		return err
	}
	p := it.Props
	if err := allow(p, "topic", "endpoint", "serviceBusQueue", "serviceBusTopic", "eventHub", "storageQueue", "function", "schema",
		"eventTypes", "subjectBeginsWith", "subjectEndsWith", "advancedFilters", "maxAttempts", "ttlMinutes", "deadLetterAccount", "deadLetterContainer"); err != nil {
		return err
	}
	scope := text(p["topic"])
	if scope == "" {
		return errors.New("topic is required")
	}
	if !strings.Contains(scope, "/") {
		scope = "topics/" + scope
	}
	var dest map[string]any
	for _, d := range [][2]string{{"endpoint", "WebHook"}, {"serviceBusQueue", "ServiceBusQueue"}, {"serviceBusTopic", "ServiceBusTopic"},
		{"eventHub", "EventHub"}, {"storageQueue", "StorageQueue"}, {"function", "AzureFunction"}} {
		if v := text(p[d[0]]); v != "" {
			if dest != nil {
				return errors.New("give one destination only")
			}
			dest = map[string]any{"endpointType": d[1], "endpointUrl": v, "resourceId": v}
		}
	}
	if dest == nil {
		return errors.New("a destination is required: endpoint, serviceBusQueue, serviceBusTopic, eventHub, storageQueue or function")
	}
	filter := map[string]any{}
	rename(filter, p, "eventTypes=includedEventTypes", "subjectBeginsWith=subjectBeginsWith", "subjectEndsWith=subjectEndsWith", "advancedFilters=advancedFilters")
	body := map[string]any{"name": it.Name, "destination": dest, "filter": filter, "eventDeliverySchema": text(p["schema"]),
		"retryPolicy": map[string]any{"maxDeliveryAttempts": p["maxAttempts"], "eventTimeToLiveInMinutes": p["ttlMinutes"]}}
	if v := text(p["deadLetterAccount"]); v != "" {
		body["deadLetterDestination"] = map[string]any{"resourceId": v, "blobContainerName": text(p["deadLetterContainer"])}
	}
	raw, _ := json.Marshal(body)
	return skipExisting(eg.AddSubscription(scope, raw))
}
