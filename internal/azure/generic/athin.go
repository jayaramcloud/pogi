package generic

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Shared by the application services (Static Web Apps, Web PubSub, Notification Hubs, Relay, IoT Hub, Maps,
// Automation, Recovery Services). Nothing runs behind any of them: names and keys are made up, deterministic and
// stay the same across reads.

// hash8 is eight hex digits that depend only on s, for the generated parts of host names.
func hash8(s string) string {
	h := fnv.New32a()
	h.Write([]byte(strings.ToLower(s)))
	return fmt.Sprintf("%08x", h.Sum32())
}

// accessKeys are the two keys of a resource, the same on every read.
func accessKeys(id arm.ID) (primary, secondary string) {
	k := strings.ToLower(id.String())
	return armres.Key(k + "/primary"), armres.Key(k + "/secondary")
}

// listKeysRule answers listKeys of a shared access rule the way Service Bus style services do: keys and
// connection strings for the namespace at endpoint (for example "sb://ns.servicebus.windows.net/").
func listKeysRule(endpoint func(arm.ID) string) func(*Service, *core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(s *Service, c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		p, q := accessKeys(id)
		cs := func(k string) string {
			return fmt.Sprintf("Endpoint=%s;SharedAccessKeyName=%s;SharedAccessKey=%s", endpoint(id), id.Name(), k)
		}
		return map[string]any{"keyName": id.Name(), "primaryKey": p, "secondaryKey": q,
			"primaryConnectionString": cs(p), "secondaryConnectionString": cs(q)}, nil
	}
}

// rootRule is the RootManageSharedAccessKey rule that every Service Bus style namespace starts with.
func rootRule(s *Service, c *core.Ctx, ns arm.ID, typ string) error {
	id := ns.Child(typ, "RootManageSharedAccessKey")
	if s.Has(id) {
		return nil
	}
	return s.Create(c, id, map[string]any{"properties": map[string]any{"rights": []any{"Listen", "Manage", "Send"}}})
}
