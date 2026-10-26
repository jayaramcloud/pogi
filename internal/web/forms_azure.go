package web

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// Azure has no schema to build a form from, so the console offers a JSON editor for the PUT body of any
// ARM resource, started from a template for its type. A submit is checked by a round trip: the body
// must be a JSON object, the ID must be a resource ID of the chosen type, and the resource must read
// back after the PUT.

const (
	rgType     = "Microsoft.Resources/resourceGroups"
	armVersion = "2024-03-01"
)

// armTemplates are the starting bodies; any other type gets armDefault. "eastus" in a body is replaced by the
// scope's location. Names such as dnsPrefix are the same for every resource: only the ID's name is unique.
var armTemplates = map[string]string{
	rgType:                                        `{"location": "eastus", "tags": {}}`,
	"Microsoft.Storage/storageAccounts":           `{"location": "eastus", "kind": "StorageV2", "sku": {"name": "Standard_LRS"}, "properties": {}}`,
	"Microsoft.KeyVault/vaults":                   `{"location": "eastus", "properties": {"tenantId": "` + arm.TenantID + `", "sku": {"family": "A", "name": "standard"}, "accessPolicies": []}}`,
	"Microsoft.ServiceBus/namespaces":             `{"location": "eastus", "sku": {"name": "Standard", "tier": "Standard"}}`,
	"Microsoft.ServiceBus/namespaces/queues":      `{"properties": {"maxDeliveryCount": 10}}`,
	"Microsoft.ContainerService/managedClusters":  `{"location": "eastus", "identity": {"type": "SystemAssigned"}, "properties": {"dnsPrefix": "pogi", "agentPoolProfiles": [{"name": "nodepool1", "count": 1, "vmSize": "Standard_DS2_v2", "mode": "System", "osType": "Linux"}]}}`,
	"Microsoft.EventGrid/topics":                  `{"location": "eastus", "properties": {}}`,
	"Microsoft.Network/virtualNetworks":           `{"location": "eastus", "properties": {"addressSpace": {"addressPrefixes": ["10.0.0.0/16"]}}}`,
	"Microsoft.DBforPostgreSQL/flexibleServers":   `{"location": "eastus", "sku": {"name": "Standard_B1ms", "tier": "Burstable"}, "properties": {"version": "16", "administratorLogin": "pogiadmin", "administratorLoginPassword": "Pogi-pass-1", "storage": {"storageSizeGB": 32}}}`,
	"Microsoft.App/managedEnvironments":           `{"location": "eastus", "properties": {}}`,
	"Microsoft.App/containerApps":                 `{"location": "eastus", "properties": {"managedEnvironmentId": "/subscriptions/{sub}/resourceGroups/{group}/providers/Microsoft.App/managedEnvironments/<environment>", "configuration": {"ingress": {"external": true, "targetPort": 80}}, "template": {"containers": [{"name": "app", "image": "nginx:alpine"}]}}}`,
	"Microsoft.ContainerInstance/containerGroups": `{"location": "eastus", "properties": {"osType": "Linux", "containers": [{"name": "app", "properties": {"image": "nginx:alpine", "resources": {"requests": {"cpu": 1, "memoryInGB": 1.5}}, "ports": [{"port": 80}]}}], "ipAddress": {"type": "Public", "ports": [{"protocol": "TCP", "port": 80}]}}}`,
	"Microsoft.Web/serverfarms":                   `{"location": "eastus", "sku": {"name": "B1", "tier": "Basic"}, "properties": {}}`,
	"Microsoft.Web/sites":                         `{"location": "eastus", "kind": "app", "properties": {"serverFarmId": "/subscriptions/{sub}/resourceGroups/{group}/providers/Microsoft.Web/serverfarms/<plan>"}}`,
	"Microsoft.DocumentDB/databaseAccounts":       `{"location": "eastus", "kind": "GlobalDocumentDB", "properties": {"databaseAccountOfferType": "Standard", "locations": [{"locationName": "eastus", "failoverPriority": 0}]}}`,
}

const armDefault = `{"location": "eastus", "properties": {}}`

// serviceTypes are the ARM types a service page offers in its Create menu, the main one first.
var serviceTypes = map[string][]string{
	"azaks":           {"Microsoft.ContainerService/managedClusters"},
	"azblob":          {"Microsoft.Storage/storageAccounts"},
	"azqueue":         {"Microsoft.Storage/storageAccounts"},
	"aztable":         {"Microsoft.Storage/storageAccounts"},
	"azfiles":         {"Microsoft.Storage/storageAccounts"},
	"azdatalake":      {"Microsoft.Storage/storageAccounts"},
	"keyvault":        {"Microsoft.KeyVault/vaults"},
	"servicebus":      {"Microsoft.ServiceBus/namespaces", "Microsoft.ServiceBus/namespaces/queues"},
	"eventgrid":       {"Microsoft.EventGrid/topics"},
	"aznetwork":       {"Microsoft.Network/virtualNetworks"},
	"azdatabases":     {"Microsoft.DBforPostgreSQL/flexibleServers", "Microsoft.Sql/servers", "Microsoft.DBforMySQL/flexibleServers", "Microsoft.Cache/redis"},
	"azcontainerapps": {"Microsoft.App/managedEnvironments", "Microsoft.App/containerApps"},
	"azaci":           {"Microsoft.ContainerInstance/containerGroups"},
	"azappservice":    {"Microsoft.Web/serverfarms", "Microsoft.Web/sites"},
	"cosmos":          {"Microsoft.DocumentDB/databaseAccounts"},
	"azacr":           {"Microsoft.ContainerRegistry/registries"},
	"azappconfig":     {"Microsoft.AppConfiguration/configurationStores"},
	"signalr":         {"Microsoft.SignalRService/signalR"},
}

// armCreateItems are a service page's ARM Create entries; back is the page to return to after a save.
func (ui *UI) armCreateItems(svc, back string) (items []menuItem) {
	have := ui.armTypes()
	for _, t := range serviceTypes[svc] {
		if slices.Contains(have, t) {
			title := core.Sentence(t[strings.LastIndex(t, "/")+1:])
			items = append(items, menuItem{Title: "Create " + strings.ToLower(strings.TrimSuffix(title, "s")), Href: armHref(t, back)})
		}
	}
	return
}

func armHref(typ, back string) string {
	return "/_pogi/arm/new?type=" + url.QueryEscape(typ) + "&back=" + url.QueryEscape(back)
}

type armData struct {
	Types            []string
	Type, Path, Body string
	Name, Back       string // Name is the ID's last segment; Back is the page to return to after a save
	Err, Result      string
	Scope            scope
}

// armTypes are the resource types the editor offers.
func (ui *UI) armTypes() []string {
	out := []string{rgType}
	for _, t := range arm.Types(ui.hub) {
		out = append(out, arm.Lookup(ui.hub, t).Type)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// armPath is where a resource of the type would go: the scope's subscription and group, and name (or <name>).
func armPath(sc scope, typ, name string) string {
	if name == "" {
		name = "<name>"
	}
	base := "/subscriptions/" + sc.sub() + "/resourceGroups/"
	if typ == rgType {
		return base + name
	}
	ns, rest, _ := strings.Cut(typ, "/")
	p := base + sc.group() + "/providers/" + ns
	types := strings.Split(rest, "/")
	for i, t := range types {
		n := "<name>"
		if i == len(types)-1 {
			n = name
		}
		p += "/" + t + "/" + n
	}
	return p
}

// armFillName puts the Name field in the ID's last segment when that is still <name>.
func armFillName(path, name string) string {
	if name = strings.TrimSpace(name); name != "" && strings.HasSuffix(path, "/<name>") {
		return strings.TrimSuffix(path, "<name>") + name
	}
	return path
}

// armBack is the page to return to: a console address, else none.
func armBack(r *http.Request) string {
	if b := r.FormValue("back"); strings.HasPrefix(b, "/_pogi/") {
		return b
	}
	return ""
}

func (ui *UI) armPage(w http.ResponseWriter, r *http.Request) {
	sc := ui.scope(r)
	typ, name := r.URL.Query().Get("type"), strings.TrimSpace(r.URL.Query().Get("name"))
	if !slices.Contains(ui.armTypes(), typ) {
		typ = rgType
	}
	ui.renderArm(w, r, armData{Type: typ, Name: name, Back: armBack(r), Path: armPath(sc, typ, name), Body: indent(armBody(sc, typ))})
}

// armBody is the starting body of a type, in the scope's location, subscription and group.
func armBody(sc scope, typ string) string {
	t, ok := armTemplates[typ]
	if !ok {
		t = armDefault
	}
	return strings.NewReplacer(`"eastus"`, strconv.Quote(sc.azLocation()), "{sub}", sc.sub(), "{group}", sc.group()).Replace(t)
}

func indent(s string) string {
	var buf bytes.Buffer
	json.Indent(&buf, []byte(s), "", "  ")
	return buf.String()
}

func (ui *UI) renderArm(w http.ResponseWriter, r *http.Request, d armData) {
	d.Types, d.Scope = ui.armTypes(), ui.scope(r)
	ui.renderX(w, r, "arm", "New Azure resource", "arm", "Azure Resource Manager · "+d.Scope.Subscription, d)
}

func (ui *UI) armSubmit(w http.ResponseWriter, r *http.Request) {
	d := armData{Type: r.FormValue("type"), Path: strings.TrimSpace(r.FormValue("path")), Body: r.FormValue("body"), Back: armBack(r)}
	d.Path = armFillName(d.Path, r.FormValue("name"))
	res, err := ui.armPut(d)
	if err != nil {
		d.Err = errText(err)
	} else {
		if inDrawer(r) {
			to, msg := ui.armResult(d)
			if d.Back != "" {
				to = d.Back
			}
			done(w, r, to, msg, nil)
			return
		}
		d.Result = res
	}
	ui.renderArm(w, r, d)
}

// armResult is the toast and the page for a saved resource: its own service page when some service lists
// it, else Resource Manager. ponytail: found by name in the Azure services' tables; an ARM type has no service link.
func (ui *UI) armResult(d armData) (to, msg string) {
	id, _ := arm.ParseID(d.Path)
	typ := d.Type
	if i := strings.LastIndex(typ, "/"); i >= 0 {
		typ = typ[i+1:]
	}
	msg = core.Sentence(strings.TrimSuffix(typ, "s")) + " " + id.Name() + " saved" // managedClusters: "Managed cluster aks1 saved"
	to = "/_pogi/resources/arm"
	for _, s := range ui.hub.Services() {
		b, ok := s.(core.Browsable)
		if !ok || s.Info().Cloud != core.Azure || s.Info().Name == "arm" {
			continue
		}
		for _, t := range b.Resources() {
			for _, row := range t.Rows {
				if !t.Log && len(row) > 0 && row[0] == id.Name() {
					return "/_pogi/resources/" + s.Info().Name, msg
				}
			}
		}
	}
	return to, msg
}

// armPut validates and sends the resource, then reads it back.
func (ui *UI) armPut(d armData) (string, error) {
	var body map[string]any
	if err := json.Unmarshal([]byte(d.Body), &body); err != nil || body == nil {
		return "", fmt.Errorf("the body is not a JSON object")
	}
	id, ok := arm.ParseID(d.Path)
	switch {
	case !ok || strings.ContainsAny(d.Path, "<>?"):
		return "", fmt.Errorf("%q is not a resource ID (fill in <name>)", d.Path)
	case d.Type == rgType && (id.ResourceGroup == "" || id.Namespace != ""):
		return "", fmt.Errorf("a resource group's ID is /subscriptions/<id>/resourceGroups/<name>")
	case d.Type != rgType && !strings.EqualFold(id.Type(), d.Type):
		return "", fmt.Errorf("%q is not a %s", d.Path, d.Type)
	case id.Name() == "" && d.Type != rgType:
		return "", fmt.Errorf("%q has no resource name", d.Path)
	}
	b, _ := json.Marshal(body)
	status, resp := ui.armDo(http.MethodPut, d.Path, b)
	if status >= 300 {
		return "", armError(status, resp)
	}
	status, resp = ui.armDo(http.MethodGet, d.Path, nil)
	if status != http.StatusOK {
		return "", fmt.Errorf("created, but reading it back says %d: %s", status, bytes.TrimSpace(resp))
	}
	return indent(string(resp)), nil
}

func (ui *UI) armDo(method, path string, body []byte) (int, []byte) {
	req, _ := http.NewRequest(method, "http://localhost"+path+"?api-version="+armVersion, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if method != http.MethodGet && method != http.MethodHead {
		req.Header.Set(core.ConsoleHeader, "1")
	}
	return ui.hub.DoService("arm", req)
}

// armError is ARM's {"error": {"code", "message"}} as a console error.
func armError(status int, body []byte) error {
	var e struct {
		Error struct{ Code, Message string }
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Code != "" {
		return &core.Error{Status: status, Code: e.Error.Code, Msg: e.Error.Message}
	}
	return fmt.Errorf("%d %s", status, bytes.TrimSpace(body))
}

// ---- resource groups ----

var groupName = regexp.MustCompile(`^[A-Za-z0-9._()-]{1,90}$`)

type groupData struct {
	Name, Location, Back, Err string
	Locations                 []string
}

func (ui *UI) groupPage(w http.ResponseWriter, r *http.Request) {
	sc := ui.scope(r)
	ui.renderX(w, r, "armgroup", "New resource group", "arm", "Azure Resource Manager · "+sc.Subscription,
		groupData{Location: sc.azLocation(), Back: armBack(r), Locations: azLocations(sc.azLocation())})
}

// azLocations is the scope's location first, then the common regions.
func azLocations(first string) []string {
	out := []string{first}
	for _, l := range []string{"eastus", "eastus2", "westus", "westus2", "westeurope", "northeurope", "centralindia", "southeastasia"} {
		if l != first {
			out = append(out, l)
		}
	}
	return out
}

func (ui *UI) groupSubmit(w http.ResponseWriter, r *http.Request) {
	sc := ui.scope(r)
	d := groupData{Name: strings.TrimSpace(r.FormValue("name")), Location: strings.TrimSpace(r.FormValue("location")), Back: armBack(r)}
	var err error
	switch {
	case !groupName.MatchString(d.Name):
		err = fmt.Errorf("a resource group name has 1 to 90 letters, digits, dots, dashes, underscores or brackets")
	case d.Location == "" || strings.ContainsAny(d.Location, " \"\\"):
		err = fmt.Errorf("pick a location, such as eastus")
	default:
		body, _ := json.Marshal(map[string]any{"location": d.Location})
		_, err = ui.armPut(armData{Type: rgType, Path: armPath(sc, rgType, d.Name), Body: string(body)})
	}
	if err != nil {
		d.Err, d.Locations = errText(err), azLocations(d.Location)
		ui.renderX(w, r, "armgroup", "New resource group", "arm", "Azure Resource Manager · "+sc.Subscription, d)
		return
	}
	done(w, r, cmp.Or(d.Back, "/_pogi/resources/arm"), "Resource group "+d.Name+" created", nil)
}

type groupMember struct{ Service, Table, Name, Href string }

// groupMembers lists the resources in a group, as Resource Manager lists them. Each links to the page of the
// service that lists its type (see serviceTypes), else to Resource Manager's own list.
func (ui *UI) groupMembers(sub, group string) (out []groupMember) {
	status, b := ui.armDo(http.MethodGet, "/subscriptions/"+sub+"/resourceGroups/"+url.PathEscape(group)+"/resources", nil)
	var list struct{ Value []struct{ Name, Type string } }
	if status != http.StatusOK || json.Unmarshal(b, &list) != nil {
		return nil
	}
	for _, r := range list.Value {
		m := groupMember{"Resource Manager", r.Type, r.Name, "/_pogi/resources/arm?tab=Resources"}
	find:
		for _, s := range ui.hub.Services() {
			for _, t := range serviceTypes[s.Info().Name] {
				if strings.EqualFold(t, r.Type) {
					m.Service, m.Href = s.Info().Title, "/_pogi/resources/"+s.Info().Name
					break find
				}
			}
		}
		out = append(out, m)
	}
	return
}
