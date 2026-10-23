package generic_test

import (
	"encoding/json"
	"strings"
	"testing"

	chronicle "cloud.google.com/go/chronicle/apiv1"
	chroniclepb "cloud.google.com/go/chronicle/apiv1/chroniclepb"
	compliance "cloud.google.com/go/cloudsecuritycompliance/apiv1"
	compliancepb "cloud.google.com/go/cloudsecuritycompliance/apiv1/cloudsecuritycompliancepb"
	pam "cloud.google.com/go/privilegedaccessmanager/apiv1"
	pampb "cloud.google.com/go/privilegedaccessmanager/apiv1/privilegedaccessmanagerpb"
	"github.com/nrndev/pogi"
	acm "google.golang.org/api/accesscontextmanager/v1"
	aw "google.golang.org/api/assuredworkloads/v1"
	"google.golang.org/api/beyondcorp/v1"
	"google.golang.org/api/cloudidentity/v1"
	"google.golang.org/api/dlp/v2"
	"google.golang.org/api/iap/v1"
	"google.golang.org/api/ids/v1"
	mi "google.golang.org/api/managedidentities/v1"
	"google.golang.org/api/publicca/v1"
	"google.golang.org/api/recaptchaenterprise/v1"
	scc "google.golang.org/api/securitycenter/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

func startSec(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"accesscontextmanager", "managedidentities", "assuredworkloads", "beyondcorp",
		"chronicle", "cloudidentity", "ids", "cloudsecuritycompliance", "dlp", "iap", "privilegedaccessmanager", "recaptchaenterprise",
		"securitycenter", "publicca", "operations"}})
}

// opResponse reads the resource a finished operation holds.
func opResponse(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		t.Fatalf("no response in the operation: %s %v", raw, err)
	}
	return m
}

func TestAccessContextManager(t *testing.T) {
	svc := must(acm.NewService(ctx, opts(startSec(t), "/accesscontextmanager/")...))
	op := must(svc.AccessPolicies.Create(&acm.AccessPolicy{Parent: "organizations/1", Title: "p"}).Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	name, _ := opResponse(t, op.Response)["name"].(string)
	if !strings.HasPrefix(name, "accessPolicies/") {
		t.Fatalf("name %q", name)
	}
	if got := must(svc.AccessPolicies.Get(name).Do()); got.Title != "p" || got.Name != name || got.Etag == "" {
		t.Fatalf("%+v", got)
	}
	_, err := svc.AccessPolicies.Create(&acm.AccessPolicy{Title: "p"}).Do()
	wantCode(t, err, 400, "parent")
	per := &acm.ServicePerimeter{Name: name + "/servicePerimeters/per", Title: "per", Status: &acm.ServicePerimeterConfig{RestrictedServices: []string{"storage.googleapis.com"}}}
	must(svc.AccessPolicies.ServicePerimeters.Create(name, per).Do())
	if got := must(svc.AccessPolicies.ServicePerimeters.Get(per.Name).Do()); got.Status.RestrictedServices[0] != "storage.googleapis.com" {
		t.Fatalf("%+v", got)
	}
	_, err = svc.AccessPolicies.ServicePerimeters.Create(name, per).Do()
	wantCode(t, err, 409, "already exists")
	_, err = svc.AccessPolicies.ServicePerimeters.Create("accessPolicies/nope", per).Do()
	wantCode(t, err, 404, "nope")
	_, err = svc.AccessPolicies.Get("accessPolicies/nope").Do()
	wantCode(t, err, 404, "nope")
	must(svc.AccessPolicies.ServicePerimeters.Delete(per.Name).Do())
	must(svc.AccessPolicies.Delete(name).Do())
	_, err = svc.AccessPolicies.Delete(name).Do()
	wantCode(t, err, 404, name)
}

func TestManagedAD(t *testing.T) {
	svc := must(mi.NewService(ctx, opts(startSec(t), "/managedidentities/")...))
	domains := svc.Projects.Locations.Global.Domains
	const parent = "projects/p/locations/global"
	op := must(domains.Create(parent, &mi.Domain{Locations: []string{"us-central1"}, ReservedIpRange: "192.168.255.0/24"}).DomainName("corp.example.com").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(domains.Get(parent + "/domains/corp.example.com").Do())
	if got.State != "READY" || got.Fqdn != "corp.example.com" || got.ReservedIpRange != "192.168.255.0/24" {
		t.Fatalf("%+v", got)
	}
	_, err := domains.Create(parent, &mi.Domain{Locations: []string{"us-central1"}, ReservedIpRange: "192.168.255.0/24"}).DomainName("corp.example.com").Do()
	wantCode(t, err, 409, "already exists")
	_, err = domains.Create(parent, &mi.Domain{Locations: []string{"us-central1"}, ReservedIpRange: "10.0.0.0/24"}).Do()
	wantCode(t, err, 400, "domainName")
	_, err = domains.Create(parent, &mi.Domain{}).DomainName("x.example.com").Do()
	wantCode(t, err, 400, "locations")
	_, err = domains.Get(parent + "/domains/nope").Do()
	wantCode(t, err, 404, "nope")
	must(domains.Delete(parent + "/domains/corp.example.com").Do())
	_, err = domains.Delete(parent + "/domains/corp.example.com").Do()
	wantCode(t, err, 404, "corp.example.com")
}

func TestAssuredWorkloads(t *testing.T) {
	svc := must(aw.NewService(ctx, opts(startSec(t), "/assuredworkloads/")...))
	ws := svc.Organizations.Locations.Workloads
	const parent = "organizations/1/locations/us-central1"
	op := must(ws.Create(parent, &aw.GoogleCloudAssuredworkloadsV1Workload{DisplayName: "w", ComplianceRegime: "FEDRAMP_MODERATE"}).Do())
	name, _ := opResponse(t, op.Response)["name"].(string)
	if !op.Done || !strings.HasPrefix(name, parent+"/workloads/") {
		t.Fatalf("%+v %q", op, name)
	}
	if got := must(ws.Get(name).Do()); got.DisplayName != "w" || got.CreateTime == "" {
		t.Fatalf("%+v", got)
	}
	_, err := ws.Create(parent, &aw.GoogleCloudAssuredworkloadsV1Workload{DisplayName: "w"}).Do()
	wantCode(t, err, 400, "complianceRegime")
	_, err = ws.Get(parent + "/workloads/nope").Do()
	wantCode(t, err, 404, "nope")
	must(ws.Delete(name).Do())
	_, err = ws.Delete(name).Do()
	wantCode(t, err, 404, name)
}

func TestChromeEnterprisePremium(t *testing.T) {
	svc := must(beyondcorp.NewService(ctx, opts(startSec(t), "/beyondcorp/")...))
	gws := svc.Projects.Locations.SecurityGateways
	const parent = "projects/p/locations/global"
	must(gws.Create(parent, &beyondcorp.GoogleCloudBeyondcorpSecuritygatewaysV1SecurityGateway{DisplayName: "g"}).SecurityGatewayId("gw1").Do())
	got := must(gws.Get(parent + "/securityGateways/gw1").Do())
	if got.DisplayName != "g" || got.State != "RUNNING" && got.State != "ACTIVE" && got.State != "" {
		t.Fatalf("%+v", got)
	}
	_, err := gws.Create(parent, &beyondcorp.GoogleCloudBeyondcorpSecuritygatewaysV1SecurityGateway{}).SecurityGatewayId("gw1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = gws.Create(parent, &beyondcorp.GoogleCloudBeyondcorpSecuritygatewaysV1SecurityGateway{}).Do()
	wantCode(t, err, 400, "securityGatewayId")
	_, err = gws.Get(parent + "/securityGateways/nope").Do()
	wantCode(t, err, 404, "nope")
	must(gws.Delete(parent + "/securityGateways/gw1").Do())
	_, err = gws.Delete(parent + "/securityGateways/gw1").Do()
	wantCode(t, err, 404, "gw1")
}

func TestSecurityOperations(t *testing.T) {
	srv := startSec(t)
	c := must(chronicle.NewReferenceListRESTClient(ctx, opts(srv, "/chronicle")...))
	defer c.Close()
	const parent = "projects/p/locations/us/instances/inst1"
	lists := &chroniclepb.ReferenceList{Description: "d", SyntaxType: chroniclepb.ReferenceListSyntaxType_REFERENCE_LIST_SYNTAX_TYPE_PLAIN_TEXT_STRING,
		Entries: []*chroniclepb.ReferenceListEntry{{Value: "evil.example.com"}}}
	got := must(c.CreateReferenceList(ctx, &chroniclepb.CreateReferenceListRequest{Parent: parent, ReferenceListId: "l1", ReferenceList: lists}))
	if got.Name != parent+"/referenceLists/l1" || got.Entries[0].Value != "evil.example.com" {
		t.Fatalf("%+v", got)
	}
	if got := must(c.GetReferenceList(ctx, &chroniclepb.GetReferenceListRequest{Name: got.Name})); got.Description != "d" {
		t.Fatalf("%+v", got)
	}
	_, err := c.CreateReferenceList(ctx, &chroniclepb.CreateReferenceListRequest{Parent: parent, ReferenceListId: "l1", ReferenceList: lists})
	wantCode(t, err, 409, "already exists")
	_, err = c.CreateReferenceList(ctx, &chroniclepb.CreateReferenceListRequest{Parent: parent, ReferenceListId: "l2", ReferenceList: &chroniclepb.ReferenceList{}})
	wantCode(t, err, 400, "description")
	_, err = c.CreateReferenceList(ctx, &chroniclepb.CreateReferenceListRequest{Parent: parent, ReferenceList: lists})
	wantCode(t, err, 400, "referenceListId")
	_, err = c.GetReferenceList(ctx, &chroniclepb.GetReferenceListRequest{Name: parent + "/referenceLists/nope"})
	wantCode(t, err, 404, "nope")
}

func TestCloudIdentity(t *testing.T) {
	svc := must(cloudidentity.NewService(ctx, opts(startSec(t), "/cloudidentity/")...))
	labels := map[string]string{"cloudidentity.googleapis.com/groups.discussion_forum": ""}
	g := &cloudidentity.Group{Parent: "customers/C1", GroupKey: &cloudidentity.EntityKey{Id: "team@example.com"}, Labels: labels, DisplayName: "team"}
	op := must(svc.Groups.Create(g).InitialGroupConfig("EMPTY").Do())
	name, _ := opResponse(t, op.Response)["name"].(string)
	if !op.Done || op.Name != "" || !strings.HasPrefix(name, "groups/") { // the operation has no name: Terraform reads the group's from the response
		t.Fatalf("%+v %q", op, name)
	}
	if got := must(svc.Groups.Get(name).Do()); got.GroupKey.Id != "team@example.com" || got.DisplayName != "team" || got.CreateTime == "" {
		t.Fatalf("%+v", got)
	}
	must(svc.Groups.Patch(name, &cloudidentity.Group{DisplayName: "team2"}).UpdateMask("displayName").Do())
	if got := must(svc.Groups.Get(name).Do()); got.DisplayName != "team2" {
		t.Fatalf("%+v", got)
	}
	_, err := svc.Groups.Create(&cloudidentity.Group{Parent: "customers/C1", Labels: labels}).Do()
	wantCode(t, err, 400, "groupKey")
	_, err = svc.Groups.Get("groups/nope").Do()
	wantCode(t, err, 404, "nope")
	must(svc.Groups.Delete(name).Do())
	_, err = svc.Groups.Delete(name).Do()
	wantCode(t, err, 404, name)
}

func TestCloudIDS(t *testing.T) {
	svc := must(ids.NewService(ctx, opts(startSec(t), "/ids/")...))
	eps := svc.Projects.Locations.Endpoints
	const parent = "projects/p/locations/us-central1-a"
	must(eps.Create(parent, &ids.Endpoint{Network: "projects/p/global/networks/default", Severity: "INFORMATIONAL"}).EndpointId("e1").Do())
	got := must(eps.Get(parent + "/endpoints/e1").Do())
	if got.State != "READY" || got.Severity != "INFORMATIONAL" || got.CreateTime == "" {
		t.Fatalf("%+v", got)
	}
	_, err := eps.Create(parent, &ids.Endpoint{}).EndpointId("e1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = eps.Create(parent, &ids.Endpoint{}).Do()
	wantCode(t, err, 400, "endpointId")
	_, err = eps.Get(parent + "/endpoints/nope").Do()
	wantCode(t, err, 404, "nope")
	must(eps.Delete(parent + "/endpoints/e1").Do())
	_, err = eps.Delete(parent + "/endpoints/e1").Do()
	wantCode(t, err, 404, "e1")
}

func TestComplianceManager(t *testing.T) {
	c := must(compliance.NewConfigRESTClient(ctx, opts(startSec(t), "/cloudsecuritycompliance")...))
	defer c.Close()
	const parent = "organizations/1/locations/global"
	cc := &compliancepb.CloudControl{Description: "d", Severity: compliancepb.Severity_MEDIUM}
	got := must(c.CreateCloudControl(ctx, &compliancepb.CreateCloudControlRequest{Parent: parent, CloudControlId: "cc1", CloudControl: cc}))
	if got.Name != parent+"/cloudControls/cc1" || got.Severity != compliancepb.Severity_MEDIUM {
		t.Fatalf("%+v", got)
	}
	if got := must(c.GetCloudControl(ctx, &compliancepb.GetCloudControlRequest{Name: got.Name})); got.Description != "d" {
		t.Fatalf("%+v", got)
	}
	_, err := c.CreateCloudControl(ctx, &compliancepb.CreateCloudControlRequest{Parent: parent, CloudControlId: "cc1", CloudControl: cc})
	wantCode(t, err, 409, "already exists")
	_, err = c.CreateCloudControl(ctx, &compliancepb.CreateCloudControlRequest{Parent: parent, CloudControl: cc})
	wantCode(t, err, 400, "cloudControlId")
	_, err = c.GetCloudControl(ctx, &compliancepb.GetCloudControlRequest{Name: parent + "/cloudControls/nope"})
	wantCode(t, err, 404, "nope")
	if err := c.DeleteCloudControl(ctx, &compliancepb.DeleteCloudControlRequest{Name: got.Name}); err != nil {
		t.Fatal(err)
	}
	err = c.DeleteCloudControl(ctx, &compliancepb.DeleteCloudControlRequest{Name: got.Name})
	wantCode(t, err, 404, "cc1")
}

func TestSensitiveDataProtection(t *testing.T) {
	svc := must(dlp.NewService(ctx, opts(startSec(t), "/dlp/")...))
	tmpls := svc.Projects.Locations.InspectTemplates
	const parent = "projects/p/locations/global"
	body := &dlp.GooglePrivacyDlpV2CreateInspectTemplateRequest{TemplateId: "t1", InspectTemplate: &dlp.GooglePrivacyDlpV2InspectTemplate{DisplayName: "t",
		InspectConfig: &dlp.GooglePrivacyDlpV2InspectConfig{InfoTypes: []*dlp.GooglePrivacyDlpV2InfoType{{Name: "EMAIL_ADDRESS"}}}}}
	got := must(tmpls.Create(parent, body).Do())
	if got.Name != parent+"/inspectTemplates/t1" || got.CreateTime == "" || got.InspectConfig.InfoTypes[0].Name != "EMAIL_ADDRESS" {
		t.Fatalf("%+v", got)
	}
	// without an id the server makes one
	auto := must(tmpls.Create(parent, &dlp.GooglePrivacyDlpV2CreateInspectTemplateRequest{InspectTemplate: &dlp.GooglePrivacyDlpV2InspectTemplate{}}).Do())
	if !strings.HasPrefix(auto.Name, parent+"/inspectTemplates/") || auto.Name == got.Name {
		t.Fatalf("%+v", auto)
	}
	up := must(tmpls.Patch(got.Name, &dlp.GooglePrivacyDlpV2UpdateInspectTemplateRequest{InspectTemplate: &dlp.GooglePrivacyDlpV2InspectTemplate{Description: "d2"}, UpdateMask: "description"}).Do())
	if up.Description != "d2" || up.DisplayName != "t" {
		t.Fatalf("%+v", up)
	}
	trig := must(svc.Projects.JobTriggers.Create("projects/p", &dlp.GooglePrivacyDlpV2CreateJobTriggerRequest{TriggerId: "jt", JobTrigger: &dlp.GooglePrivacyDlpV2JobTrigger{DisplayName: "j"}}).Do())
	if trig.Name != "projects/p/jobTriggers/jt" {
		t.Fatalf("%+v", trig)
	}
	_, err := tmpls.Create(parent, body).Do()
	wantCode(t, err, 409, "already exists")
	_, err = tmpls.Create(parent, &dlp.GooglePrivacyDlpV2CreateInspectTemplateRequest{TemplateId: "x"}).Do()
	wantCode(t, err, 400, "inspectTemplate")
	_, err = tmpls.Create(parent, &dlp.GooglePrivacyDlpV2CreateInspectTemplateRequest{TemplateId: "bad id", InspectTemplate: &dlp.GooglePrivacyDlpV2InspectTemplate{}}).Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = tmpls.Get(parent + "/inspectTemplates/nope").Do()
	wantCode(t, err, 404, "nope")
	must(tmpls.Delete(got.Name).Do())
	_, err = tmpls.Delete(got.Name).Do()
	wantCode(t, err, 404, got.Name)
}

func TestIAP(t *testing.T) {
	svc := must(iap.NewService(ctx, opts(startSec(t), "/iap/")...))
	groups := svc.Projects.IapTunnel.Locations.DestGroups
	const parent = "projects/p/iap_tunnel/locations/us-central1"
	got := must(groups.Create(parent, &iap.TunnelDestGroup{Cidrs: []string{"10.1.2.0/24"}}).TunnelDestGroupId("g1").Do())
	if got.Name != parent+"/destGroups/g1" || got.Cidrs[0] != "10.1.2.0/24" {
		t.Fatalf("%+v", got)
	}
	must(groups.Patch(got.Name, &iap.TunnelDestGroup{Fqdns: []string{"db.example.com"}}).UpdateMask("fqdns").Do())
	if got := must(groups.Get(got.Name).Do()); got.Fqdns[0] != "db.example.com" || got.Cidrs[0] != "10.1.2.0/24" {
		t.Fatalf("%+v", got)
	}
	if l := must(groups.List(parent).Do()); len(l.TunnelDestGroups) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := groups.Create(parent, &iap.TunnelDestGroup{}).TunnelDestGroupId("g1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = groups.Create(parent, &iap.TunnelDestGroup{}).Do()
	wantCode(t, err, 400, "tunnelDestGroupId")
	_, err = groups.Get(parent + "/destGroups/nope").Do()
	wantCode(t, err, 404, "nope")
	must(groups.Delete(got.Name).Do())
	_, err = groups.Delete(got.Name).Do()
	wantCode(t, err, 404, "g1")
}

func TestPrivilegedAccessManager(t *testing.T) {
	c := must(pam.NewRESTClient(ctx, opts(startSec(t), "/privilegedaccessmanager")...))
	defer c.Close()
	const parent = "projects/p/locations/global"
	ent := func() *pampb.Entitlement {
		return &pampb.Entitlement{MaxRequestDuration: &durationpb.Duration{Seconds: 3600},
			EligibleUsers: []*pampb.AccessControlEntry{{Principals: []string{"group:g@example.com"}}},
			PrivilegedAccess: &pampb.PrivilegedAccess{AccessType: &pampb.PrivilegedAccess_GcpIamAccess_{GcpIamAccess: &pampb.PrivilegedAccess_GcpIamAccess{
				ResourceType: "cloudresourcemanager.googleapis.com/Project", Resource: "//cloudresourcemanager.googleapis.com/projects/p",
				RoleBindings: []*pampb.PrivilegedAccess_GcpIamAccess_RoleBinding{{Role: "roles/storage.admin"}}}}},
			RequesterJustificationConfig: &pampb.Entitlement_RequesterJustificationConfig{
				JustificationType: &pampb.Entitlement_RequesterJustificationConfig_NotMandatory_{NotMandatory: &pampb.Entitlement_RequesterJustificationConfig_NotMandatory{}}}}
	}
	op := must(c.CreateEntitlement(ctx, &pampb.CreateEntitlementRequest{Parent: parent, EntitlementId: "e1", Entitlement: ent()}))
	got := must(op.Wait(ctx))
	if got.Name != parent+"/entitlements/e1" || got.State != pampb.Entitlement_AVAILABLE || got.Etag == "" {
		t.Fatalf("%+v", got)
	}
	if got := must(c.GetEntitlement(ctx, &pampb.GetEntitlementRequest{Name: got.Name})); got.PrivilegedAccess.GetGcpIamAccess().RoleBindings[0].Role != "roles/storage.admin" {
		t.Fatalf("%+v", got)
	}
	_, err := c.CreateEntitlement(ctx, &pampb.CreateEntitlementRequest{Parent: parent, EntitlementId: "e1", Entitlement: ent()})
	wantCode(t, err, 409, "already exists")
	_, err = c.CreateEntitlement(ctx, &pampb.CreateEntitlementRequest{Parent: parent, EntitlementId: "e2", Entitlement: &pampb.Entitlement{}})
	wantCode(t, err, 400, "maxRequestDuration")
	_, err = c.GetEntitlement(ctx, &pampb.GetEntitlementRequest{Name: parent + "/entitlements/nope"})
	wantCode(t, err, 404, "nope")
	del := must(c.DeleteEntitlement(ctx, &pampb.DeleteEntitlementRequest{Name: got.Name}))
	must(del.Wait(ctx))
	_, err = c.DeleteEntitlement(ctx, &pampb.DeleteEntitlementRequest{Name: got.Name})
	wantCode(t, err, 404, "e1")
}

func TestRecaptcha(t *testing.T) {
	svc := must(recaptchaenterprise.NewService(ctx, opts(startSec(t), "/recaptchaenterprise/")...))
	keys := svc.Projects.Keys
	key := must(keys.Create("projects/p", &recaptchaenterprise.GoogleCloudRecaptchaenterpriseV1Key{DisplayName: "k",
		WebSettings: &recaptchaenterprise.GoogleCloudRecaptchaenterpriseV1WebKeySettings{IntegrationType: "SCORE", AllowAllDomains: true}}).Do())
	if !strings.HasPrefix(key.Name, "projects/p/keys/6L") || len(key.Name) != len("projects/p/keys/")+40 || key.CreateTime == "" {
		t.Fatalf("%+v", key)
	}
	if got := must(keys.Get(key.Name).Do()); got.WebSettings.IntegrationType != "SCORE" {
		t.Fatalf("%+v", got)
	}
	_, err := keys.Create("projects/p", &recaptchaenterprise.GoogleCloudRecaptchaenterpriseV1Key{}).Do()
	wantCode(t, err, 400, "displayName")
	_, err = keys.Get("projects/p/keys/nope").Do()
	wantCode(t, err, 404, "nope")
	must(keys.Delete(key.Name).Do())
	_, err = keys.Delete(key.Name).Do()
	wantCode(t, err, 404, key.Name)
}

func TestSecurityCommandCenter(t *testing.T) {
	svc := must(scc.NewService(ctx, opts(startSec(t), "/securitycenter/")...))
	src := must(svc.Organizations.Sources.Create("organizations/1", &scc.Source{DisplayName: "s", Description: "d"}).Do())
	if !strings.HasPrefix(src.Name, "organizations/1/sources/") || src.DisplayName != "s" {
		t.Fatalf("%+v", src)
	}
	must(svc.Organizations.Sources.Patch(src.Name, &scc.Source{Description: "d2"}).UpdateMask("description").Do())
	if got := must(svc.Organizations.Sources.Get(src.Name).Do()); got.Description != "d2" || got.DisplayName != "s" {
		t.Fatalf("%+v", got)
	}
	nc := must(svc.Organizations.NotificationConfigs.Create("organizations/1", &scc.NotificationConfig{PubsubTopic: "projects/p/topics/t",
		StreamingConfig: &scc.StreamingConfig{Filter: "state=\"ACTIVE\""}}).ConfigId("nc1").Do())
	if nc.Name != "organizations/1/notificationConfigs/nc1" {
		t.Fatalf("%+v", nc)
	}
	_, err := svc.Organizations.NotificationConfigs.Create("organizations/1", &scc.NotificationConfig{PubsubTopic: "t"}).ConfigId("nc1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = svc.Organizations.NotificationConfigs.Create("organizations/1", &scc.NotificationConfig{}).ConfigId("nc2").Do()
	wantCode(t, err, 400, "pubsubTopic")
	_, err = svc.Organizations.Sources.Create("organizations/1", &scc.Source{}).Do()
	wantCode(t, err, 400, "displayName")
	_, err = svc.Organizations.Sources.Get("organizations/1/sources/nope").Do()
	wantCode(t, err, 404, "nope")
}

func TestPublicCA(t *testing.T) {
	svc := must(publicca.NewService(ctx, opts(startSec(t), "/publicca/")...))
	keys := svc.Projects.Locations.ExternalAccountKeys
	const parent = "projects/p/locations/global"
	k := must(keys.Create(parent, &publicca.ExternalAccountKey{}).Do())
	if !strings.HasPrefix(k.Name, parent+"/externalAccountKeys/") || k.KeyId != strings.TrimPrefix(k.Name, parent+"/externalAccountKeys/") || k.B64MacKey == "" {
		t.Fatalf("%+v", k)
	}
	k2 := must(keys.Create(parent, &publicca.ExternalAccountKey{}).Do())
	if k2.Name == k.Name || k2.B64MacKey == k.B64MacKey {
		t.Fatalf("two keys alike: %+v %+v", k, k2)
	}
}
