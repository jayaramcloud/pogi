package web_test

import (
	"html"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/docker/dockertest"
)

var (
	pathRe = regexp.MustCompile(`name="path" value="([^"]*)"`)
	bodyRe = regexp.MustCompile(`(?s)<textarea[^>]*name="body"[^>]*>(.*?)</textarea>`)
)

// editor reads the starting ID and body the editor offers for a type, with name filled in.
func editor(c *console, typ, name string) (path, body string) {
	page := c.get("/_pogi/arm/new?type=" + url.QueryEscape(typ) + "&name=" + name)
	p, b := pathRe.FindStringSubmatch(page), bodyRe.FindStringSubmatch(page)
	if p == nil || b == nil {
		c.t.Fatalf("%s: no editor in the page", typ)
	}
	return html.UnescapeString(p[1]), html.UnescapeString(b[1])
}

// Every starting body creates its resource through the editor as it is: only the name is filled in.
func TestArmTemplatesCreate(t *testing.T) {
	dockertest.Start(t, 0, true) // no engine: clusters, servers and containers keep to metadata
	c := start(t, "arm", "azaks", "azblob", "keyvault", "servicebus", "eventgrid", "aznetwork", "azdatabases", "azcontainerapps", "azaci", "azappservice", "azfunctions", "cosmos")
	for _, typ := range []string{ // parents first
		"Microsoft.ContainerService/managedClusters", "Microsoft.Storage/storageAccounts", "Microsoft.KeyVault/vaults",
		"Microsoft.ServiceBus/namespaces", "Microsoft.ServiceBus/namespaces/queues", "Microsoft.EventGrid/topics",
		"Microsoft.Network/virtualNetworks", "Microsoft.DBforPostgreSQL/flexibleServers", "Microsoft.App/managedEnvironments",
		"Microsoft.App/containerApps", "Microsoft.ContainerInstance/containerGroups", "Microsoft.Web/serverfarms",
		"Microsoft.Web/sites", "Microsoft.DocumentDB/databaseAccounts",
	} {
		path, body := editor(c, typ, "t1")
		path = strings.Replace(path, "<name>", "t1", 1) // a queue's namespace is t1 too
		body = strings.NewReplacer("<environment>", "t1", "<plan>", "t1").Replace(body)
		out, _ := c.post("/_pogi/arm/new", url.Values{"type": {typ}, "path": {path}, "body": {body}})
		if strings.Contains(out, `class="flash"`) || !strings.Contains(out, `id="armresult"`) {
			t.Errorf("%s: %s", typ, flash(out))
		}
	}
}

func TestArmEditorNameAndBack(t *testing.T) {
	c := start(t, "arm", "azblob")
	page := c.get("/_pogi/arm/new?type=Microsoft.Storage/storageAccounts&back=/_pogi/resources/azblob")
	for _, want := range []string{`name="back" value="/_pogi/resources/azblob"`, `name="name"`, "/providers/Microsoft.Storage/storageAccounts/&lt;name&gt;", "/resourceGroups/pogi/"} {
		if !strings.Contains(page, want) {
			t.Errorf("editor lacks %q", want)
		}
	}
	// the Name field fills the last segment; the save returns to the page the editor came from
	path, body := editor(c, "Microsoft.Storage/storageAccounts", "")
	req := url.Values{"type": {"Microsoft.Storage/storageAccounts"}, "path": {path}, "body": {body}, "name": {" acct1 "}}
	if out, _ := c.post("/_pogi/arm/new", req); !strings.Contains(out, "storageAccounts/acct1") {
		t.Errorf("name not used: %s", flash(out))
	}
	// without a name the ID still has <name> and the save is refused
	req.Del("name")
	if out, _ := c.post("/_pogi/arm/new", req); !strings.Contains(flash(out), "fill in") {
		t.Errorf("an ID with <name> was accepted: %s", flash(out))
	}
}

func TestCreateMenusAndGroups(t *testing.T) {
	c := start(t, "arm", "azblob", "servicebus")
	if page := c.get("/_pogi/resources/azblob"); !strings.Contains(strings.ToLower(page), "type=microsoft.storage%2fstorageaccounts&amp;back=") {
		t.Errorf("blob page has no Create storage account button")
	}
	if page := c.get("/_pogi/resources/servicebus"); !strings.Contains(page, "Create queue") || !strings.Contains(page, "Create namespace") {
		t.Errorf("service bus page has no Create menu")
	}
	// a resource group: a bad name says what is wrong; a good one is created and lists what is in it
	if out, _ := c.post("/_pogi/arm/group", url.Values{"name": {"bad name"}, "location": {"eastus"}}); !strings.Contains(flash(out), "1 to 90") {
		t.Errorf("bad group name: %s", flash(out))
	}
	c.post("/_pogi/arm/group", url.Values{"name": {"web1"}, "location": {"westus"}, "back": {"/_pogi/resources/arm"}})
	_, body := editor(c, "Microsoft.ServiceBus/namespaces", "")
	path := "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/web1/providers/Microsoft.ServiceBus/namespaces/bus1"
	if out, _ := c.post("/_pogi/arm/new", url.Values{"type": {"Microsoft.ServiceBus/namespaces"}, "body": {body}, "path": {path}}); !strings.Contains(out, "armresult") {
		t.Fatalf("namespace not created: %s", flash(out))
	}
	got := c.drawer("/_pogi/resources/arm/" + url.PathEscape("Resource groups") + "/web1")
	if !strings.Contains(got, "Resources in this group") || !strings.Contains(got, "bus1") || !strings.Contains(got, "/_pogi/resources/servicebus") {
		t.Errorf("group drawer does not list bus1: %s", got)
	}
}
