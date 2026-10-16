package rbac_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	goamqp "github.com/Azure/go-amqp"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armtest"
	"github.com/nrndev/pogi/internal/azure/rbac"
	"github.com/nrndev/pogi/internal/azure/servicebus"
	"github.com/nrndev/pogi/internal/core"
)

const (
	sub     = core.AzureSubscription
	rg      = "/subscriptions/" + sub + "/resourceGroups/" + core.AzureResourceGroup
	blobRW  = "ba92f5b4-2d11-453d-a403-e96b0029c9fe" // Storage Blob Data Contributor
	sbSend  = "69a216fc-b8fb-44d8-bc22-1f3c2cd27a39" // Azure Service Bus Data Sender
	readerR = "acdd72a7-3385-48ef-bd42-f606fba81ae7" // Reader
)

func TestArmAction(t *testing.T) {
	p := "/subscriptions/s/resourceGroups/g/providers/"
	for _, c := range []struct{ method, path, action, scope string }{
		{"GET", p + "Microsoft.Storage/storageAccounts/sa", "Microsoft.Storage/storageAccounts/read", p + "Microsoft.Storage/storageAccounts/sa"},
		{"GET", p + "Microsoft.Storage/storageAccounts", "Microsoft.Storage/storageAccounts/read", "/subscriptions/s/resourceGroups/g"},
		{"GET", p + "Microsoft.ServiceBus/namespaces/ns/queues", "Microsoft.ServiceBus/namespaces/queues/read", p + "Microsoft.ServiceBus/namespaces/ns"},
		{"PUT", p + "Microsoft.ServiceBus/namespaces/ns/queues/q", "Microsoft.ServiceBus/namespaces/queues/write", p + "Microsoft.ServiceBus/namespaces/ns/queues/q"},
		{"PATCH", p + "Microsoft.Storage/storageAccounts/sa", "Microsoft.Storage/storageAccounts/write", p + "Microsoft.Storage/storageAccounts/sa"},
		{"DELETE", p + "Microsoft.KeyVault/vaults/v", "Microsoft.KeyVault/vaults/delete", p + "Microsoft.KeyVault/vaults/v"},
		{"POST", p + "Microsoft.Storage/storageAccounts/sa/listKeys", "Microsoft.Storage/storageAccounts/listKeys/action", p + "Microsoft.Storage/storageAccounts/sa"},
		{"POST", p + "Microsoft.Compute/virtualMachines/vm/start", "Microsoft.Compute/virtualMachines/start/action", p + "Microsoft.Compute/virtualMachines/vm"},
		{"POST", "/subscriptions/s/providers/Microsoft.Storage/checkNameAvailability", "Microsoft.Storage/checkNameAvailability/action", "/subscriptions/s"},
		{"PUT", p + "Microsoft.Storage/storageAccounts/sa/providers/Microsoft.Authorization/roleAssignments/x", "Microsoft.Authorization/roleAssignments/write",
			p + "Microsoft.Storage/storageAccounts/sa/providers/Microsoft.Authorization/roleAssignments/x"},
		{"GET", "/subscriptions/s/resourceGroups/g", "Microsoft.Resources/subscriptions/resourceGroups/read", "/subscriptions/s/resourceGroups/g"},
		{"PUT", "/subscriptions/s/resourceGroups/g", "Microsoft.Resources/subscriptions/resourceGroups/write", "/subscriptions/s/resourceGroups/g"},
		{"GET", "/subscriptions/s/resourceGroups", "Microsoft.Resources/subscriptions/resourceGroups/read", "/subscriptions/s/resourceGroups"},
	} {
		action, scope, ok := rbac.ArmAction(c.method, c.path)
		if !ok || action != c.action || scope != c.scope {
			t.Errorf("%s %s = %q at %q (%v), want %q at %q", c.method, c.path, action, scope, ok, c.action, c.scope)
		}
	}
	for _, c := range []struct{ method, path string }{
		{"GET", "/subscriptions"}, {"GET", "/subscriptions/s"}, {"GET", "/providers"}, {"GET", "/subscriptions/s/providers/Microsoft.Storage"},
		{"GET", "/subscriptions/s/operationresults/x"}, {"GET", "/subscriptions/s/operationStatuses/x"}, {"GET", "/tenants"},
	} {
		if a, _, ok := rbac.ArmAction(c.method, c.path); ok {
			t.Errorf("%s %s needs %q: nobody needs a role for it", c.method, c.path, a)
		}
	}
}

func TestRoleTable(t *testing.T) {
	guid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	ids, names := map[string]bool{}, map[string]bool{}
	for _, r := range arm.Roles {
		if !guid.MatchString(r.GUID) || ids[r.GUID] || names[r.Name] || r.Desc == "" {
			t.Errorf("role %q (%s) is malformed or repeated", r.Name, r.GUID)
		}
		ids[r.GUID], names[r.Name] = true, true
		if len(r.Actions)+len(r.Data) == 0 {
			t.Errorf("role %q grants nothing", r.Name)
		}
	}
	if len(arm.Roles) < 60 {
		t.Errorf("only %d built-in roles", len(arm.Roles))
	}
	for guid, name := range map[string]string{"8e3af657-a8ff-443c-a75c-2fe8c4bcb635": "Owner", "b24988ac-6180-42a0-ab88-20f7382dd24c": "Contributor",
		"acdd72a7-3385-48ef-bd42-f606fba81ae7": "Reader", blobRW: "Storage Blob Data Contributor", sbSend: "Azure Service Bus Data Sender"} {
		if i := indexRole(guid); i < 0 || arm.Roles[i].Name != name {
			t.Errorf("%s should be %s", guid, name)
		}
	}
}

func indexRole(guid string) int {
	for i, r := range arm.Roles {
		if r.GUID == guid {
			return i
		}
	}
	return -1
}

// az is a pogi in strict mode and a client of its Resource Manager and Blob Storage.
type az struct {
	t    *testing.T
	env  *armtest.Env
	port string
}

func start(t *testing.T) *az {
	t.Helper()
	e := armtest.Start(t, pogi.Config{IAM: "strict"}, "azblob", "azqueue", "servicebus")
	return &az{t: t, env: e, port: e.Base[strings.LastIndexByte(e.Base, ':')+1:]}
}

// do sends a request with an optional bearer token and host.
func (a *az) do(method, path, host, token string, body any) (int, map[string]any) {
	a.t.Helper()
	var rd io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		rd = strings.NewReader(string(b))
	}
	req, _ := http.NewRequest(method, a.env.Base+path, rd)
	if host != "" {
		req.Host = host
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Metadata", "true")
	resp, err := a.env.Opts.Transport.Do(req)
	if err != nil {
		a.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	json.Unmarshal(b, &out)
	return resp.StatusCode, out
}

func (a *az) arm(method, path, token string, body any) (int, map[string]any) {
	return a.do(method, path+"?api-version=2022-04-01", "", token, body)
}

// identity creates a user-assigned managed identity and returns its principal ID and a token.
func (a *az) identity(name string) (oid, token string) {
	a.t.Helper()
	st, doc := a.arm("PUT", rg+"/providers/Microsoft.ManagedIdentity/userAssignedIdentities/"+name, "", map[string]any{"location": "eastus"})
	if st/100 != 2 {
		a.t.Fatalf("create identity: %d %v", st, doc)
	}
	p := doc["properties"].(map[string]any)
	st, tok := a.do("GET", "/metadata/identity/oauth2/token?api-version=2018-02-01&resource="+url.QueryEscape(a.env.Base)+"&client_id="+p["clientId"].(string), "", "", nil)
	if st != 200 {
		a.t.Fatalf("identity token: %d %v", st, tok)
	}
	return p["principalId"].(string), tok["access_token"].(string)
}

// assign gives the principal a built-in role at a scope.
func (a *az) assign(oid, role, scope string) string {
	a.t.Helper()
	id := scope + "/providers/Microsoft.Authorization/roleAssignments/" + core.UUID()
	st, doc := a.arm("PUT", id, "", map[string]any{"properties": map[string]any{"roleDefinitionId": "/subscriptions/" + sub + "/providers/Microsoft.Authorization/roleDefinitions/" + role,
		"principalId": oid, "principalType": "ServicePrincipal"}})
	if st/100 != 2 {
		a.t.Fatalf("assign: %d %v", st, doc)
	}
	return id
}

func (a *az) listBlobs(token string) (int, map[string]any) {
	return a.do("GET", "/c1?restype=container&comp=list", "pogistore.blob.localhost:"+a.port, token, nil)
}

func TestStrictDeniesUntilAssigned(t *testing.T) {
	a := start(t)
	oid, tok := a.identity("worker")

	// a managed identity the user created has no role: both planes refuse it
	st, doc := a.listBlobs(tok)
	if st != 403 {
		t.Fatalf("blob list with no role: %d %v", st, doc)
	}
	st, doc = a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", tok, nil)
	if e, _ := doc["error"].(map[string]any); st != 403 || e["code"] != "AuthorizationFailed" || !strings.Contains(e["message"].(string), "Microsoft.Storage/storageAccounts/read") {
		t.Fatalf("ARM list with no role: %d %v", st, doc)
	}

	// default credentials, and calls without a token, always pass
	cred := armtest.Must(a.env.Cred.GetToken(context.Background(), policy.TokenRequestOptions{Scopes: []string{a.env.Base + "/.default"}}))
	if st, doc = a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", cred.Token, nil); st != 200 {
		t.Fatalf("default credential: %d %v", st, doc)
	}
	if st, doc = a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", "", nil); st != 200 {
		t.Fatalf("no token: %d %v", st, doc)
	}

	// Storage Blob Data Contributor on the resource group opens the data plane, and only that
	a.assign(oid, blobRW, rg)
	if st, doc = a.listBlobs(tok); st == 403 {
		t.Fatalf("blob list after the assignment: %d %v", st, doc)
	}
	if st, _ = a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", tok, nil); st != 403 {
		t.Fatalf("ARM read needs Reader: %d", st)
	}
	// the data role grants data actions only: a container delete is an action it has, a read of a queue is not
	if st, _ = a.do("GET", "/?comp=list", "pogistore.queue.localhost:"+a.port, tok, nil); st != 403 {
		t.Fatalf("queue list should be denied: %d", st)
	}
	a.assign(oid, readerR, rg+"/providers/Microsoft.Storage/storageAccounts/other")
	if st, _ = a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", tok, nil); st != 403 {
		t.Fatalf("Reader on another resource must not grant the group list: %d", st)
	}
	a.assign(oid, readerR, rg)
	if st, _ = a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", tok, nil); st != 200 {
		t.Fatalf("Reader on the group: %d", st)
	}
}

func TestOffAndSoftRecordButAllow(t *testing.T) {
	a := start(t)
	_, tok := a.identity("w")
	for _, mode := range []string{"off", "soft"} {
		a.env.Srv.Hub.SetIAMMode(mode)
		if st, doc := a.listBlobs(tok); st == 403 {
			t.Fatalf("%s mode denied: %v", mode, doc)
		}
	}
	ps := rbac.Recorded(a.env.Srv.Hub)
	if len(ps) != 1 || ps[0].Name != "w" || ps[0].Kind != "managedIdentity" || len(ps[0].WouldDeny()) != 1 {
		t.Fatalf("recorded %+v", ps)
	}
	c := ps[0].Actions[0]
	if c.Action != "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read" || !c.Data || c.Count != 2 || !strings.HasSuffix(c.Scope, "/pogistore/blobServices/default/containers/c1") {
		t.Fatalf("call %+v", c)
	}
	if n, _ := ps[0].Closest(); n != "Storage Blob Data Reader" {
		t.Fatalf("closest built-in %q", n)
	}
}

func TestGenerateAttachUndo(t *testing.T) {
	a := start(t)
	hub := a.env.Srv.Hub
	oid, tok := a.identity("orders")
	a.listBlobs(tok)
	a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", tok, nil)
	ps := rbac.Recorded(hub)
	if len(ps) != 1 || len(ps[0].WouldDeny()) != 2 {
		t.Fatalf("recorded %+v", ps)
	}
	d := ps[0].Definition()
	if d.Name != "pogi-orders" || d.Scope != rg || d.Assignable != rg || len(d.Actions) != 1 || len(d.DataActions) != 1 {
		t.Fatalf("definition %+v", d)
	}
	if !strings.Contains(d.CLI(), "az role assignment create --assignee-object-id "+oid) || !strings.Contains(d.JSON(), `"CustomRole"`) {
		t.Fatalf("snippets:\n%s\n%s", d.CLI(), d.JSON())
	}
	pl, err := rbac.Preview(hub, oid)
	if err != nil || pl.Existing || len(pl.Add) != 2 {
		t.Fatalf("plan %+v %v", pl, err)
	}

	at, err := rbac.Attach(hub, oid)
	if err != nil {
		t.Fatal(err)
	}
	if st, doc := a.listBlobs(tok); st == 403 {
		t.Fatalf("after attach: %d %v", st, doc)
	}
	if st, doc := a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", tok, nil); st != 200 {
		t.Fatalf("after attach, ARM: %d %v", st, doc)
	}
	// the role is a real custom role definition and the assignment a real one, both visible through ARM
	st, def := a.arm("GET", "/subscriptions/"+sub+"/providers/Microsoft.Authorization/roleDefinitions/"+at.RoleID, "", nil)
	props, _ := def["properties"].(map[string]any)
	if st != 200 || props["roleName"] != "pogi-orders" || props["type"] != "CustomRole" || !strings.Contains(toJSON(props), "blobs/read") {
		t.Fatalf("role definition %d %v", st, def)
	}
	if st, as := a.arm("GET", at.Assignment, "", nil); st != 200 || as["properties"].(map[string]any)["principalId"] != oid {
		t.Fatalf("assignment %d %v", st, as)
	}
	if left := rbac.Recorded(hub)[0].WouldDeny(); len(left) != 0 {
		t.Fatalf("still denied: %+v", left)
	}

	// attaching again updates the same role and keeps what it granted
	if pl, _ = rbac.Preview(hub, oid); !pl.Existing || len(pl.Add) != 0 || !pl.AssignmentExists {
		t.Fatalf("second plan %+v", pl)
	}
	if _, err := rbac.Attach(hub, oid); err != nil {
		t.Fatal(err)
	}
	if n := len(assignments(a, oid)); n != 1 {
		t.Fatalf("%d assignments after attaching twice", n)
	}

	// undo removes the assignment and, as the attach created it, the role
	if err := rbac.Undo(hub, at); err != nil {
		t.Fatal(err)
	}
	if st, _ := a.listBlobs(tok); st != 403 {
		t.Fatalf("after undo: %d", st)
	}
	if st, _ := a.arm("GET", "/subscriptions/"+sub+"/providers/Microsoft.Authorization/roleDefinitions/"+at.RoleID, "", nil); st != 404 {
		t.Fatalf("role definition after undo: %d", st)
	}
}

func toJSON(v any) string { b, _ := json.Marshal(v); return string(b) }

func assignments(a *az, oid string) []any {
	_, l := a.arm("GET", rg+"/providers/Microsoft.Authorization/roleAssignments", "", nil)
	var out []any
	for _, x := range l["value"].([]any) {
		if x.(map[string]any)["properties"].(map[string]any)["principalId"] == oid {
			out = append(out, x)
		}
	}
	return out
}

// Custom role definitions made through ARM count like built-in ones.
func TestCustomRoleDefinition(t *testing.T) {
	a := start(t)
	oid, tok := a.identity("reader2")
	id := "/subscriptions/" + sub + "/providers/Microsoft.Authorization/roleDefinitions/" + core.UUID()
	st, doc := a.arm("PUT", id, "", map[string]any{"properties": map[string]any{"roleName": "list-accounts", "assignableScopes": []string{rg},
		"permissions": []any{map[string]any{"actions": []string{"Microsoft.Storage/storageAccounts/read"}}}}})
	if st/100 != 2 {
		t.Fatalf("custom role: %d %v", st, doc)
	}
	guid := id[strings.LastIndexByte(id, '/')+1:]
	st, _ = a.arm("PUT", rg+"/providers/Microsoft.Authorization/roleAssignments/"+core.UUID(), "", map[string]any{"properties": map[string]any{"roleDefinitionId": id, "principalId": oid}})
	if st/100 != 2 {
		t.Fatal("assign custom role")
	}
	if st, doc = a.arm("GET", rg+"/providers/Microsoft.Storage/storageAccounts", tok, nil); st != 200 {
		t.Fatalf("custom role allows the read: %d %v", st, doc)
	}
	if st, _ = a.arm("GET", rg+"/providers/Microsoft.KeyVault/vaults", tok, nil); st != 403 {
		t.Fatalf("custom role must not allow other reads: %d", st)
	}
	if st, _ = a.arm("DELETE", id, "", nil); st/100 != 2 {
		t.Fatalf("delete role: %d", st)
	}
	if st, _ = a.arm("GET", "/subscriptions/"+sub+"/providers/Microsoft.Authorization/roleDefinitions/"+guid, "", nil); st != 404 {
		t.Fatalf("deleted role: %d", st)
	}
}

// Service Bus over AMQP is checked at attach, send and receive, with the JWT put on $cbs.
func TestServiceBusAMQP(t *testing.T) {
	a := start(t)
	hub := a.env.Srv.Hub
	if _, err := hub.Service("servicebus").(*servicebus.Service).CreateQueue("localhost", "q", servicebus.Props{}); err != nil {
		t.Fatal(err)
	}
	oid, tok := a.identity("sender")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, err := goamqp.Dial(ctx, a.env.Srv.Ports["amqp"], &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	sess := armtest.Must(conn.NewSession(ctx, nil))
	cbs := armtest.Must(sess.NewSender(ctx, "$cbs", nil))
	reply := "cbs-reply"
	rcv := armtest.Must(sess.NewReceiver(ctx, "$cbs", &goamqp.ReceiverOptions{TargetAddress: reply, Credit: 10,
		SettlementMode: goamqp.ReceiverSettleModeFirst.Ptr(), RequestedSenderSettleMode: goamqp.SenderSettleModeSettled.Ptr()}))
	if err := cbs.Send(ctx, &goamqp.Message{Value: tok, Properties: &goamqp.MessageProperties{MessageID: "1", ReplyTo: &reply},
		ApplicationProperties: map[string]any{"operation": "put-token", "type": "jwt", "name": "sb://localhost/q"}}, nil); err != nil {
		t.Fatal(err)
	}
	if m := armtest.Must(rcv.Receive(ctx, nil)); m.ApplicationProperties["status-code"] != int32(202) {
		t.Fatalf("put-token %v", m.ApplicationProperties)
	}

	send := func() error {
		snd, err := sess.NewSender(ctx, "q", nil)
		if err != nil {
			return err
		}
		defer snd.Close(ctx)
		return snd.Send(ctx, goamqp.NewMessage([]byte("hi")), nil)
	}
	if err := send(); err == nil {
		t.Fatal("a send without Azure Service Bus Data Sender went through")
	}
	ps := rbac.Recorded(hub)
	if len(ps) != 1 || ps[0].Actions[0].Action != "Microsoft.ServiceBus/namespaces/messages/send/action" || ps[0].Actions[0].Verdict == "allowed" {
		t.Fatalf("recorded %+v", ps)
	}
	a.assign(oid, sbSend, rg)
	if err := send(); err != nil {
		t.Fatalf("send with the role: %v", err)
	}
	// the role sends but does not receive
	rc, err := sess.NewReceiver(ctx, "q", &goamqp.ReceiverOptions{Credit: 1})
	if err == nil {
		_, err = rc.Receive(ctx, nil)
	}
	if err == nil {
		t.Fatal("receive without Azure Service Bus Data Receiver went through")
	}
}
