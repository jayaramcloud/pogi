package web

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
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

// armTemplates are the starting bodies; any other type gets armDefault.
var armTemplates = map[string]string{
	rgType:                              `{"location": "eastus", "tags": {}}`,
	"Microsoft.Storage/storageAccounts": `{"location": "eastus", "kind": "StorageV2", "sku": {"name": "Standard_LRS"}, "properties": {}}`,
	"Microsoft.KeyVault/vaults":         `{"location": "eastus", "properties": {"tenantId": "` + arm.TenantID + `", "sku": {"family": "A", "name": "standard"}, "accessPolicies": []}}`,
	"Microsoft.ServiceBus/namespaces":   `{"location": "eastus", "sku": {"name": "Standard", "tier": "Standard"}}`,
}

const armDefault = `{"location": "eastus", "properties": {}}`

type armData struct {
	Types            []string
	Type, Path, Body string
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

// armPath is where a resource of the type would go, with <name> for the user to fill in.
func armPath(sc scope, typ string) string {
	base := "/subscriptions/" + sc.sub() + "/resourceGroups/"
	if typ == rgType {
		return base + "<name>"
	}
	ns, rest, _ := strings.Cut(typ, "/")
	p := base + core.AzureResourceGroup + "/providers/" + ns
	for _, t := range strings.Split(rest, "/") {
		p += "/" + t + "/<name>"
	}
	return p
}

func (ui *UI) armPage(w http.ResponseWriter, r *http.Request) {
	sc := ui.scope(r)
	typ := r.URL.Query().Get("type")
	if !slices.Contains(ui.armTypes(), typ) {
		typ = rgType
	}
	ui.renderArm(w, r, armData{Type: typ, Path: armPath(sc, typ), Body: indent(armBody(typ))})
}

func armBody(typ string) string {
	if t, ok := armTemplates[typ]; ok {
		return t
	}
	return armDefault
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
	d := armData{Type: r.FormValue("type"), Path: strings.TrimSpace(r.FormValue("path")), Body: r.FormValue("body")}
	res, err := ui.armPut(d)
	if err != nil {
		d.Err = errText(err)
	} else {
		if inDrawer(r) {
			done(w, r, "/_pogi/resources/arm", "Resource saved", nil)
			return
		}
		d.Result = res
	}
	ui.renderArm(w, r, d)
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
