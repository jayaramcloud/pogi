// Package arm is pogi's Azure Resource Manager control plane. Services that
// have a management plane (storage accounts, Key Vaults, Service Bus
// namespaces, ...) register a Provider per resource type; the ARM service
// owns everything around them: the metadata endpoint, subscriptions,
// resource groups, provider registration, routing by resource ID,
// api-version, list wrapping and long-running operation headers.
//
// This file is the contract providers build against. The HTTP surface
// lives in the rest of the package.
package arm

import (
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/core"
)

// A Provider serves one resource type, e.g. "Microsoft.ServiceBus/namespaces"
// or the child type "Microsoft.ServiceBus/namespaces/queues". Resources are
// JSON objects as ARM returns them: id, name, type, location, tags,
// properties, sku, kind, ... Providers return errors as *core.Error with ARM
// codes (ResourceNotFound 404, Conflict 409, BadRequest 400 ...); ARM renders
// them as {"error": {"code", "message"}}.
type Provider struct {
	Type string // resource type, matched case-insensitively
	// APIVersions are listed as the type's apiVersions in the provider's resource types; empty lists one default.
	// Requests are accepted whatever their api-version.
	APIVersions []string

	// Put creates or updates the resource at id from the request body and
	// returns it. ARM checks the parent exists first.
	Put func(c *core.Ctx, id ID, body map[string]any) (map[string]any, error)
	// Get returns the resource, or a 404 *core.Error.
	Get func(c *core.Ctx, id ID) (map[string]any, error)
	// Delete removes it (and its children); a missing resource is not an error.
	Delete func(c *core.Ctx, id ID) error
	// List returns the resources of this type under parent: a resource
	// group, a subscription (Parent.ResourceGroup == ""), or a parent
	// resource for child types.
	List func(c *core.Ctx, parent ID) ([]map[string]any, error)
	// Actions are POSTs on a resource, by name: "listKeys", "regenerateKey",
	// "listConnectionStrings", ...
	Actions map[string]func(c *core.Ctx, id ID, body map[string]any) (any, error)
	// Async makes PUT and DELETE answer like a long-running operation:
	// 201/202 with Azure-AsyncOperation and Location headers that point at an
	// operation that has already succeeded. Off, they answer 200/201 at once.
	Async bool
	// ParentNotFound makes a PUT of a child whose parent does not exist answer ParentResourceNotFound, as the
	// resource manager does for types that have no service of their own; off, it is ResourceNotFound, which is
	// what services that check their own parents (AKS, Service Bus) answer.
	ParentNotFound bool
	// AsyncPut and AsyncDelete make only PUT or only DELETE long-running, for types where the other is a plain call
	// (a DNS zone is deleted by a long-running operation and created by a plain one).
	AsyncPut, AsyncDelete bool
	// Created is the status of a PUT that creates the resource: 201 when 0. The SDKs
	// accept only the statuses the real service answers with (200 for a Service Bus
	// queue, 202 for a storage account).
	Created int
	// LongActions are the actions that answer like a long-running operation that has already
	// succeeded (202, Azure-AsyncOperation and Location headers), such as "purge".
	LongActions []string
	// NoContent are the actions that answer 204 with no body, as the SDKs of some services insist
	// (regeneratePrimaryKey on an API Management subscription, a container group's "stop").
	NoContent []string
	// Doc, when set, answers a GET of the collection path of this type with the document it returns, as
	// it is and not wrapped in a list: the logs of a container, .../containers/<name>/logs. It gets the parent and the query.
	Doc func(c *core.Ctx, parent ID, query url.Values) (any, error)
	// Deleted is the status of a DELETE of a resource that exists: 200 when 0, or 202 with the headers of an
	// operation when Async. Some types are long-running to create and not to delete.
	Deleted int
}

// TenantID is the one Entra tenant pogi has; subscriptions, tokens and the
// Graph directory all belong to it.
const TenantID = "00000000-0000-0000-0000-000000000000"

// ID is a parsed resource ID:
// /subscriptions/<sub>/resourceGroups/<rg>/providers/<Namespace>/<type>/<name>[/<type>/<name>...]
type ID struct {
	Subscription  string
	ResourceGroup string   // "" for subscription-level resources
	Namespace     string   // "Microsoft.ServiceBus"
	Types         []string // {"namespaces", "queues"}
	Names         []string // {"ns1", "q1"}; len(Names) == len(Types), or one less for a collection
}

// ParseID parses a resource ID or a collection path (the last name missing). A path that starts at
// /providers/ (a management group or the tenant) has no Subscription.
func ParseID(s string) (ID, bool) {
	seg := strings.Split(strings.Trim(s, "/"), "/")
	var id ID
	if len(seg) >= 3 && strings.EqualFold(seg[0], "providers") {
		// no subscription: the tenant's own scope
	} else if len(seg) < 2 || !strings.EqualFold(seg[0], "subscriptions") {
		return id, false
	} else {
		id.Subscription, seg = seg[1], seg[2:]
		if len(seg) >= 2 && strings.EqualFold(seg[0], "resourceGroups") {
			id.ResourceGroup, seg = seg[1], seg[2:]
		}
		if len(seg) == 0 {
			return id, true
		}
	}
	if !strings.EqualFold(seg[0], "providers") || len(seg) < 3 {
		return id, false
	}
	id.Namespace, seg = seg[1], seg[2:]
	for i := 0; i < len(seg); i += 2 {
		id.Types = append(id.Types, seg[i])
		if i+1 < len(seg) {
			id.Names = append(id.Names, seg[i+1])
		}
	}
	return id, true
}

// String formats the ID in ARM's casing.
func (id ID) String() string {
	var b strings.Builder
	if id.Subscription != "" {
		b.WriteString("/subscriptions/" + id.Subscription)
	}
	if id.ResourceGroup != "" {
		b.WriteString("/resourceGroups/" + id.ResourceGroup)
	}
	if id.Namespace != "" {
		b.WriteString("/providers/" + id.Namespace)
	}
	for i, t := range id.Types {
		b.WriteString("/" + t)
		if i < len(id.Names) {
			b.WriteString("/" + id.Names[i])
		}
	}
	return b.String()
}

// Type is the resource type, "Microsoft.ServiceBus/namespaces/queues".
func (id ID) Type() string {
	if id.Namespace == "" {
		return ""
	}
	return id.Namespace + "/" + strings.Join(id.Types, "/")
}

// Name is the last name, "q1".
func (id ID) Name() string {
	if len(id.Names) == 0 {
		return ""
	}
	return id.Names[len(id.Names)-1]
}

// Parent is the enclosing resource, or the resource group for top-level
// resources.
func (id ID) Parent() ID {
	p := id
	if len(id.Types) <= 1 {
		p.Namespace, p.Types, p.Names = "", nil, nil
		return p
	}
	n := len(id.Types) - 1
	p.Types, p.Names = id.Types[:n], id.Names[:min(n, len(id.Names))]
	return p
}

// Child is the ID of a child resource.
func (id ID) Child(typ, name string) ID {
	c := id
	c.Types = append(append([]string(nil), id.Types...), typ)
	c.Names = append(append([]string(nil), id.Names...), name)
	return c
}

// Resource builds the common resource envelope; providers add properties,
// sku, kind and so on.
func Resource(id ID, location string, tags any, properties map[string]any) map[string]any {
	if tags == nil {
		tags = map[string]any{}
	}
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": location,
		"tags": tags, "properties": properties}
}

// NotFound is ARM's 404 for a resource.
func NotFound(id ID) *core.Error {
	return core.Errorf(http.StatusNotFound, "ResourceNotFound", "The Resource '%s' under resource group '%s' was not found.",
		id.Type()+"/"+strings.Join(id.Names, "/"), id.ResourceGroup)
}

// ---- registry ----

// ponytail: one registry per hub in a package map, never freed; hubs live
// for the process except in tests.
var (
	regMu sync.Mutex
	regs  = map[*core.Hub]map[string]*Provider{}
)

// Register adds a provider for its resource type on the hub. Services call
// it from their constructor.
func Register(h *core.Hub, p Provider) {
	regMu.Lock()
	defer regMu.Unlock()
	if regs[h] == nil {
		regs[h] = map[string]*Provider{}
	}
	regs[h][strings.ToLower(p.Type)] = &p
}

// Lookup finds the provider of a resource type.
func Lookup(h *core.Hub, typ string) *Provider {
	regMu.Lock()
	defer regMu.Unlock()
	return regs[h][strings.ToLower(typ)]
}

// Types lists the registered resource types.
func Types(h *core.Hub) []string {
	regMu.Lock()
	defer regMu.Unlock()
	return core.SortedKeys(regs[h])
}
