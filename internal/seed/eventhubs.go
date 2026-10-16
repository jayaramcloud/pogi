package seed

import (
	"cmp"
	"errors"
	"strconv"
	"time"

	"github.com/nrndev/pogi/internal/config"
)

// eventHubs is the Event Hubs control plane the seed functions call.
type eventHubs interface {
	Inspect(table, id string) any
	CreateNamespace(name string) error
	CreateHub(ns, name string, partitions int, retention time.Duration) error
	CreateConsumerGroup(ns, hub, name string) error
}

func eventHubsService(s *seeder) (eventHubs, error) {
	eh, ok := s.h.Service("eventhubs").(eventHubs)
	if !ok {
		return nil, errors.New("service eventhubs is not enabled")
	}
	return eh, nil
}

// eventHub: namespace (default localhost, made when missing), partitions
// (default 4), retentionHours (default 24), consumerGroups [names].
func eventHub(s *seeder, it config.Item) error {
	if err := allow(it.Props, "namespace", "partitions", "retentionHours", "consumerGroups"); err != nil {
		return err
	}
	eh, err := eventHubsService(s)
	if err != nil {
		return err
	}
	ns := cmp.Or(text(it.Props["namespace"]), "localhost")
	if eh.Inspect("Namespaces", ns) == nil {
		if err := eh.CreateNamespace(ns); err != nil {
			return err
		}
	}
	if eh.Inspect("Event hubs", ns+"/"+it.Name) == nil {
		partitions, _ := strconv.Atoi(text(it.Props["partitions"]))
		hours, _ := strconv.Atoi(text(it.Props["retentionHours"]))
		if err := eh.CreateHub(ns, it.Name, partitions, time.Duration(hours)*time.Hour); err != nil {
			return err
		}
	}
	groups, _ := it.Props["consumerGroups"].([]any)
	for _, g := range groups {
		if err := seedGroup(eh, ns, it.Name, text(g)); err != nil {
			return err
		}
	}
	return nil
}

// consumerGroup: eventHub (required), namespace (default localhost).
func consumerGroup(s *seeder, it config.Item) error {
	if err := allow(it.Props, "namespace", "eventHub"); err != nil {
		return err
	}
	eh, err := eventHubsService(s)
	if err != nil {
		return err
	}
	if text(it.Props["eventHub"]) == "" {
		return errors.New("eventHub is required")
	}
	return seedGroup(eh, cmp.Or(text(it.Props["namespace"]), "localhost"), text(it.Props["eventHub"]), it.Name)
}

func seedGroup(eh eventHubs, ns, hub, name string) error {
	if eh.Inspect("Consumer groups", ns+"/"+hub+"/"+name) != nil {
		return nil
	}
	return eh.CreateConsumerGroup(ns, hub, name)
}
