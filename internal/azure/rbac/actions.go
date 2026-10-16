package rbac

import (
	"net"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// act is what a request needs permission for: an action (Resource Manager, and the container, queue
// and table level calls of storage, which Azure lists under actions) or a data action, over a scope.
type act struct {
	Name  string
	Data  bool
	Scope string
}

const (
	blobs  = "Microsoft.Storage/storageAccounts/blobServices/containers/"
	queues = "Microsoft.Storage/storageAccounts/queueServices/queues/"
	tables = "Microsoft.Storage/storageAccounts/tableServices/tables/"
	vaults = "Microsoft.KeyVault/vaults/"
	bus    = "Microsoft.ServiceBus/namespaces/messages/"
)

// actionOf is the action and scope of a request; false for the calls RBAC does not cover (sign-in,
// Graph, polling an operation, a service pogi has no action map for).
func (s *Service) actionOf(c *core.Ctx) (act, bool) {
	path := strings.Trim(c.Path, "/")
	seg := strings.Split(path, "/")
	switch c.Service {
	case "arm":
		method, _, _ := strings.Cut(c.Op, " ")
		name, scope, ok := ArmAction(method, c.Path)
		return act{name, false, scope}, ok
	case "azblob":
		acct, rest := storageAccount(c, "blob", seg)
		scope := s.resolve("Microsoft.Storage/storageAccounts", acct)
		if container := part(rest, 0); container != "" {
			scope += "/blobServices/default/containers/" + container
		}
		a, ok := blobAction(c.Op)
		return act{a.Name, a.Data, scope}, ok
	case "azqueue":
		acct, rest := storageAccount(c, "queue", seg)
		scope := s.resolve("Microsoft.Storage/storageAccounts", acct)
		if q := part(rest, 0); q != "" {
			scope += "/queueServices/default/queues/" + q
		}
		a, ok := queueAction(c.Op)
		return act{a.Name, a.Data, scope}, ok
	case "aztable":
		acct, rest := storageAccount(c, "table", seg)
		scope := s.resolve("Microsoft.Storage/storageAccounts", acct)
		if t, _, _ := strings.Cut(part(rest, 0), "("); t != "" && !strings.EqualFold(t, "Tables") && t != "$batch" {
			scope += "/tableServices/default/tables/" + t
		}
		a, ok := tableAction(c.Op)
		return act{a.Name, a.Data, scope}, ok
	case "keyvault":
		vault, _, _ := strings.Cut(strings.ToLower(c.Host), ".")
		scope := s.resolve("Microsoft.KeyVault/vaults", vault)
		if i := strings.TrimPrefix(seg[0], "deleted"); (i == "secrets" || i == "keys" || i == "certificates") && len(seg) > 1 {
			scope += "/" + i + "/" + seg[1]
		}
		a, ok := vaultAction(c.Op)
		return act{a.Name, true, scope}, ok
	case "servicebus": // AMQP: the broker calls the hub with these operations
		ns := namespaceOf(c.Host)
		switch c.Op {
		case "amqp send":
			return act{bus + "send/action", true, s.resolve("Microsoft.ServiceBus/namespaces", ns)}, true
		case "amqp receive":
			return act{bus + "receive/action", true, s.resolve("Microsoft.ServiceBus/namespaces", ns)}, true
		}
	}
	return act{}, false
}

// namespaceOf is the Service Bus namespace a host names; an IP or localhost is the default namespace.
func namespaceOf(host string) string {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.Trim(strings.ToLower(host), "[]")
	first, _, _ := strings.Cut(host, ".")
	if host == "" || net.ParseIP(host) != nil || first == "localhost" {
		return "localhost"
	}
	return first
}

func part(l []string, i int) string {
	if i < len(l) {
		return l[i]
	}
	return ""
}

// storageAccount is the account of a data plane request and the path after it: the host names it
// (<account>.blob.localhost), or else the first path segment does (Azurite's path style).
func storageAccount(c *core.Ctx, component string, seg []string) (string, []string) {
	if a, ok := armres.HostAccount(c.Host, component); ok {
		return a, seg
	}
	return seg[0], seg[min(1, len(seg)):]
}

// ArmAction is the Resource Manager action a request needs, as Azure derives it: the resource type
// path of the innermost provider segment plus read, write or delete from GET, PUT or PATCH, and
// DELETE; a POST on <name>/<verb> is <type>/<verb>/action. The scope is the resource's own ID; for a
// collection or a verb it is what holds them (the group, the parent resource). It is false for the calls everybody may make: the
// tenant, subscription and provider listings and polling a long-running operation.
func ArmAction(method, path string) (name, scope string, ok bool) {
	seg := strings.Split(strings.Trim(path, "/"), "/")
	verb := map[string]string{"GET": "read", "HEAD": "read", "PUT": "write", "PATCH": "write", "DELETE": "delete"}[method]
	i := len(seg) - 1
	for ; i >= 0 && !strings.EqualFold(seg[i], "providers"); i-- {
	}
	if i < 0 { // no provider: resource groups only
		if len(seg) >= 3 && len(seg) <= 4 && strings.EqualFold(seg[0], "subscriptions") && strings.EqualFold(seg[2], "resourceGroups") && verb != "" {
			return "Microsoft.Resources/subscriptions/resourceGroups/" + verb, "/" + strings.Join(seg, "/"), true
		}
		return "", "", false
	}
	if len(seg) < i+3 {
		return "", "", false // /providers and /providers/<namespace> are registration lookups
	}
	typ, rest := seg[i+1], seg[i+2:]
	for j := 0; j < len(rest); j += 2 {
		typ += "/" + rest[j]
	}
	scope = "/" + strings.Join(seg, "/")
	if len(rest)%2 == 1 { // a collection or a verb: the scope is what holds it
		n := len(seg) - 1
		if len(rest) == 1 {
			n = i
		}
		scope = "/" + strings.Join(seg[:n], "/")
	}
	switch {
	case method == "POST":
		return typ + "/action", scope, true
	case verb == "":
		return "", "", false
	}
	return typ + "/" + verb, scope, true
}

// resolve is the Resource Manager ID of a top-level resource named name, searching every
// subscription; one pogi does not know has the ID it would have in pogi's default resource group.
func (s *Service) resolve(typ, name string) string {
	a := s.arm()
	if p := arm.Lookup(s.h, typ); p != nil && p.List != nil && a != nil {
		for _, sub := range a.SubscriptionIDs() {
			items, _ := p.List(&core.Ctx{Hub: s.h, Internal: true}, arm.ID{Subscription: sub})
			for _, it := range items {
				if n, _ := it["name"].(string); strings.EqualFold(n, name) {
					if id, _ := it["id"].(string); id != "" {
						return id
					}
				}
			}
		}
	}
	return "/subscriptions/" + core.AzureSubscription + "/resourceGroups/" + core.AzureResourceGroup + "/providers/" + typ + "/" + name
}

type da struct {
	Name string
	Data bool
}

// blobAction maps Blob Storage operations. Container calls are actions, blob calls data actions.
func blobAction(op string) (da, bool) {
	switch op {
	case "ListContainers", "GetContainerProperties", "GetContainerACL":
		return da{blobs + "read", false}, true
	case "CreateContainer", "SetContainerMetadata", "SetContainerACL", "LeaseContainer":
		return da{blobs + "write", false}, true
	case "DeleteContainer":
		return da{blobs + "delete", false}, true
	case "AppendBlock":
		return da{blobs + "blobs/add/action", true}, true
	case "GetBlobTags":
		return da{blobs + "blobs/tags/read", true}, true
	case "SetBlobTags":
		return da{blobs + "blobs/tags/write", true}, true
	case "ListBlobs", "GetBlob", "GetBlobProperties", "GetBlockList", "GetPageRanges":
		return da{blobs + "blobs/read", true}, true
	case "DeleteBlob":
		return da{blobs + "blobs/delete", true}, true
	case "PutBlob", "StageBlock", "CommitBlockList", "PutPage", "SetBlobMetadata", "SetBlobProperties", "LeaseBlob", "CreateSnapshot", "SetBlobTier", "CopyBlob":
		return da{blobs + "blobs/write", true}, true
	}
	return da{}, false // service properties, account info and stats
}

func queueAction(op string) (da, bool) {
	switch op {
	case "ListQueues", "GetQueueMetadata", "GetQueueACL":
		return da{queues + "read", false}, true
	case "CreateQueue", "SetQueueMetadata", "SetQueueACL":
		return da{queues + "write", false}, true
	case "DeleteQueue":
		return da{queues + "delete", false}, true
	case "PutMessage":
		return da{queues + "messages/add/action", true}, true
	case "PeekMessages":
		return da{queues + "messages/read", true}, true
	case "GetMessages", "UpdateMessage", "DeleteMessage":
		return da{queues + "messages/process/action", true}, true
	case "ClearMessages":
		return da{queues + "messages/delete", true}, true
	}
	return da{}, false
}

func tableAction(op string) (da, bool) {
	switch op {
	case "QueryTables", "GetTableACL":
		return da{tables + "read", false}, true
	case "CreateTable", "SetTableACL":
		return da{tables + "write", false}, true
	case "DeleteTable":
		return da{tables + "delete", false}, true
	case "QueryEntities", "GetEntity":
		return da{tables + "entities/read", true}, true
	case "InsertEntity":
		return da{tables + "entities/add/action", true}, true
	case "UpdateEntity", "MergeEntity":
		return da{tables + "entities/update/action", true}, true
	case "DeleteEntity":
		return da{tables + "entities/delete", true}, true
	case "Batch":
		return da{tables + "entities/write", true}, true
	}
	return da{}, false
}

// vaultAction maps Key Vault operations ("GetSecret", "CreateKey", "WrapKey"...) to its data actions.
func vaultAction(op string) (da, bool) {
	coll := ""
	for n, c := range map[string]string{"Secret": "secrets", "Key": "keys", "Certificate": "certificates"} {
		if strings.Contains(op, n) {
			coll = c
		}
	}
	if strings.HasSuffix(op, "Key") || strings.HasSuffix(op, "Verify") || op == "Encrypt" || op == "Decrypt" || op == "Sign" {
		coll = "keys"
	}
	if coll == "" {
		return da{}, false // random bytes
	}
	p := vaults + coll + "/"
	switch {
	case op == "GetSecret":
		return da{p + "getSecret/action", true}, true
	case op == "SetSecret":
		return da{p + "setSecret/action", true}, true
	case coll == "secrets" && strings.HasPrefix(op, "Get"): // lists, versions and deleted secrets: metadata only
		return da{p + "readMetadata/action", true}, true
	case strings.HasPrefix(op, "Get"):
		return da{p + "read", true}, true
	case strings.HasPrefix(op, "Delete"):
		return da{p + "delete", true}, true
	}
	v := strings.ToLower(op)
	switch {
	case strings.HasPrefix(op, "Purge"):
		v = "purge"
	case strings.HasPrefix(op, "Recover"):
		v = "recover"
	case strings.Contains(op, "RotationPolicy"):
		v = "update"
	default:
		for _, n := range []string{"secret", "key", "certificate"} {
			v = strings.TrimSuffix(v, n)
		}
	}
	return da{p + v + "/action", true}, true
}
