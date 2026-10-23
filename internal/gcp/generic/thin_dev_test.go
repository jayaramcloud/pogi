package generic_test

import (
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"google.golang.org/api/apikeys/v2"
	ba "google.golang.org/api/binaryauthorization/v1"
	"google.golang.org/api/cloudasset/v1"
	cb "google.golang.org/api/cloudbuild/v1"
	"google.golang.org/api/clouddeploy/v1"
	ca "google.golang.org/api/containeranalysis/v1"
	ec "google.golang.org/api/essentialcontacts/v1"
	"google.golang.org/api/orgpolicy/v2"
	sd "google.golang.org/api/servicedirectory/v1"
)

func startDev(t *testing.T) *pogi.Running {
	return pogi.Start(t, pogi.Config{Services: []string{"cloudbuild", "clouddeploy", "apikeys", "servicedirectory",
		"binaryauthorization", "containeranalysis", "essentialcontacts", "orgpolicy", "cloudasset", "operations"}})
}

func TestCloudBuild(t *testing.T) {
	svc := must(cb.NewService(ctx, opts(startDev(t), "/cloudbuild/")...))
	// a global trigger: the server makes its id
	trig := must(svc.Projects.Triggers.Create("p", &cb.BuildTrigger{Name: "t1", Filename: "cloudbuild.yaml",
		TriggerTemplate: &cb.RepoSource{RepoName: "r", BranchName: "main"}}).Do())
	if trig.Id == "" || trig.Name != "t1" || trig.CreateTime == "" || trig.ResourceName != "projects/p/locations/global/triggers/"+trig.Id {
		t.Fatalf("%+v", trig)
	}
	if got := must(svc.Projects.Triggers.Get("p", trig.Id).Do()); got.Filename != "cloudbuild.yaml" {
		t.Fatalf("%+v", got)
	}
	loc := must(svc.Projects.Locations.Triggers.Create("projects/p/locations/us-central1", &cb.BuildTrigger{Name: "t2"}).Do())
	if loc.Id == "" || loc.Id == trig.Id || loc.Name != "t2" {
		t.Fatalf("%+v", loc)
	}
	must(svc.Projects.Locations.Triggers.Patch("projects/p/locations/us-central1/triggers/"+loc.Id, &cb.BuildTrigger{Description: "d"}).Do())
	if l := must(svc.Projects.Triggers.List("p").Do()); len(l.Triggers) != 1 {
		t.Fatalf("%+v", l)
	}
	must(svc.Projects.Triggers.Delete("p", trig.Id).Do())
	_, err := svc.Projects.Triggers.Get("p", trig.Id).Do()
	wantCode(t, err, 404, trig.Id)
	_, err = svc.Projects.Triggers.Delete("p", "nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = svc.Projects.Triggers.Create("p", &cb.BuildTrigger{Name: "x"}).Do() // a second create makes a second trigger
	if err != nil {
		t.Fatal(err)
	}

	// worker pools
	parent := "projects/p/locations/us-central1"
	pools := svc.Projects.Locations.WorkerPools
	op := must(pools.Create(parent, &cb.WorkerPool{DisplayName: "pool", PrivatePoolV1Config: &cb.PrivatePoolV1Config{WorkerConfig: &cb.WorkerConfig{MachineType: "e2-medium"}}}).WorkerPoolId("wp1").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	wp := must(pools.Get(parent + "/workerPools/wp1").Do())
	if wp.State != "RUNNING" || wp.Uid == "" || wp.PrivatePoolV1Config.WorkerConfig.MachineType != "e2-medium" {
		t.Fatalf("%+v", wp)
	}
	_, err = pools.Create(parent, &cb.WorkerPool{}).WorkerPoolId("wp1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = pools.Create(parent, &cb.WorkerPool{}).Do()
	wantCode(t, err, 400, "workerPoolId")
	_, err = pools.Create(parent, &cb.WorkerPool{}).WorkerPoolId("Bad Id").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = pools.Get(parent + "/workerPools/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = pools.Delete(parent + "/workerPools/nope").Do()
	wantCode(t, err, 404, "nope")
	must(pools.Delete(parent + "/workerPools/wp1").Do())
}

func TestCloudDeploy(t *testing.T) {
	svc := must(clouddeploy.NewService(ctx, opts(startDev(t), "/clouddeploy/")...))
	parent := "projects/p/locations/us-central1"
	tg := svc.Projects.Locations.Targets
	op := must(tg.Create(parent, &clouddeploy.Target{Description: "dev", Gke: &clouddeploy.GkeCluster{Cluster: "projects/p/locations/l/clusters/c"}}).TargetId("dev").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	got := must(tg.Get(parent + "/targets/dev").Do())
	if got.Uid == "" || got.Gke.Cluster == "" || got.CreateTime == "" {
		t.Fatalf("%+v", got)
	}
	pl := svc.Projects.Locations.DeliveryPipelines
	must(pl.Create(parent, &clouddeploy.DeliveryPipeline{SerialPipeline: &clouddeploy.SerialPipeline{Stages: []*clouddeploy.Stage{{TargetId: "dev"}}}}).DeliveryPipelineId("pipe").Do())
	if l := must(pl.List(parent).Do()); len(l.DeliveryPipelines) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := tg.Create(parent, &clouddeploy.Target{}).TargetId("dev").Do()
	wantCode(t, err, 409, "already exists")
	_, err = tg.Create(parent, &clouddeploy.Target{}).Do()
	wantCode(t, err, 400, "targetId")
	_, err = pl.Create(parent, &clouddeploy.DeliveryPipeline{}).DeliveryPipelineId("-bad").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = tg.Get(parent + "/targets/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = pl.Delete(parent + "/deliveryPipelines/nope").Do()
	wantCode(t, err, 404, "nope")
	must(tg.Delete(parent + "/targets/dev").Do())
}

func TestAPIKeys(t *testing.T) {
	svc := must(apikeys.NewService(ctx, opts(startDev(t), "/apikeys/")...))
	keys := svc.Projects.Locations.Keys
	parent := "projects/p/locations/global"
	op := must(keys.Create(parent, &apikeys.V2Key{DisplayName: "k", Restrictions: &apikeys.V2Restrictions{ApiTargets: []*apikeys.V2ApiTarget{{Service: "maps.googleapis.com"}}}}).KeyId("key1").Do())
	if !op.Done {
		t.Fatalf("%+v", op)
	}
	k := must(keys.Get(parent + "/keys/key1").Do())
	if k.Uid == "" || k.DisplayName != "k" || k.CreateTime == "" || len(k.Restrictions.ApiTargets) != 1 {
		t.Fatalf("%+v", k)
	}
	s1 := must(keys.GetKeyString(parent + "/keys/key1").Do())
	s2 := must(keys.GetKeyString(parent + "/keys/key1").Do())
	if !strings.HasPrefix(s1.KeyString, "AIza") || len(s1.KeyString) != 39 || s1.KeyString != s2.KeyString {
		t.Fatalf("%+v", s1)
	}
	_, err := keys.Create(parent, &apikeys.V2Key{}).KeyId("key1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = keys.Create(parent, &apikeys.V2Key{}).Do()
	wantCode(t, err, 400, "keyId")
	_, err = keys.Create(parent, &apikeys.V2Key{}).KeyId("UPPER case").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = keys.Get(parent + "/keys/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = keys.GetKeyString(parent + "/keys/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = keys.Delete(parent + "/keys/nope").Do()
	wantCode(t, err, 404, "nope")
	must(keys.Patch(parent+"/keys/key1", &apikeys.V2Key{DisplayName: "k2"}).UpdateMask("displayName").Do())
	must(keys.Delete(parent + "/keys/key1").Do())
}

func TestServiceDirectory(t *testing.T) {
	svc := must(sd.NewService(ctx, opts(startDev(t), "/servicedirectory/")...))
	ns := svc.Projects.Locations.Namespaces
	parent := "projects/p/locations/us-central1"
	n := must(ns.Create(parent, &sd.Namespace{Labels: map[string]string{"a": "b"}}).NamespaceId("ns1").Do())
	if n.Name != parent+"/namespaces/ns1" || n.Labels["a"] != "b" {
		t.Fatalf("%+v", n)
	}
	s := must(ns.Services.Create(n.Name, &sd.Service{Annotations: map[string]string{"x": "y"}}).ServiceId("svc1").Do())
	e := must(ns.Services.Endpoints.Create(s.Name, &sd.Endpoint{Address: "10.0.0.1", Port: 8080}).EndpointId("ep1").Do())
	if e.Name != s.Name+"/endpoints/ep1" || e.Port != 8080 {
		t.Fatalf("%+v", e)
	}
	must(ns.Services.Endpoints.Patch(e.Name, &sd.Endpoint{Port: 9090}).UpdateMask("port").Do())
	if l := must(ns.Services.Endpoints.List(s.Name).Do()); len(l.Endpoints) != 1 || l.Endpoints[0].Port != 9090 {
		t.Fatalf("%+v", l)
	}
	_, err := ns.Create(parent, &sd.Namespace{}).NamespaceId("ns1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = ns.Create(parent, &sd.Namespace{}).Do()
	wantCode(t, err, 400, "namespaceId")
	_, err = ns.Create(parent, &sd.Namespace{}).NamespaceId("Bad_Id!").Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = ns.Services.Create(parent+"/namespaces/none", &sd.Service{}).ServiceId("s").Do()
	wantCode(t, err, 404, "none")
	_, err = ns.Get(parent + "/namespaces/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = ns.Delete(parent + "/namespaces/nope").Do()
	wantCode(t, err, 404, "nope")
	// deleting a namespace removes what is in it
	must(ns.Delete(n.Name).Do())
	_, err = ns.Services.Endpoints.Get(e.Name).Do()
	wantCode(t, err, 404, "ep1")
}

func TestBinaryAuthorization(t *testing.T) {
	svc := must(ba.NewService(ctx, opts(startDev(t), "/binaryauthorization/")...))
	// the policy exists before anything is set
	p := must(svc.Projects.GetPolicy("projects/p/policy").Do())
	if p.Name != "projects/p/policy" || p.DefaultAdmissionRule.EvaluationMode != "ALWAYS_ALLOW" {
		t.Fatalf("%+v", p)
	}
	up := must(svc.Projects.UpdatePolicy("projects/p/policy", &ba.Policy{DefaultAdmissionRule: &ba.AdmissionRule{
		EvaluationMode: "REQUIRE_ATTESTATION", EnforcementMode: "ENFORCED_BLOCK_AND_AUDIT_LOG", RequireAttestationsBy: []string{"projects/p/attestors/a1"}}}).Do())
	if up.UpdateTime == "" || up.Name != "projects/p/policy" {
		t.Fatalf("%+v", up)
	}
	if p = must(svc.Projects.GetPolicy("projects/p/policy").Do()); p.DefaultAdmissionRule.EvaluationMode != "REQUIRE_ATTESTATION" {
		t.Fatalf("%+v", p)
	}
	_, err := svc.Projects.UpdatePolicy("projects/p/policy", &ba.Policy{}).Do()
	wantCode(t, err, 400, "defaultAdmissionRule")

	at := svc.Projects.Attestors
	a := must(at.Create("projects/p", &ba.Attestor{Description: "d", UserOwnedGrafeasNote: &ba.UserOwnedGrafeasNote{NoteReference: "projects/p/notes/n1"}}).AttestorId("a1").Do())
	if a.Name != "projects/p/attestors/a1" || a.UpdateTime == "" || a.UserOwnedGrafeasNote.NoteReference != "projects/p/notes/n1" {
		t.Fatalf("%+v", a)
	}
	must(at.Update("projects/p/attestors/a1", &ba.Attestor{Description: "d2", UserOwnedGrafeasNote: a.UserOwnedGrafeasNote}).Do())
	if got := must(at.Get("projects/p/attestors/a1").Do()); got.Description != "d2" {
		t.Fatalf("%+v", got)
	}
	_, err = at.Create("projects/p", &ba.Attestor{UserOwnedGrafeasNote: a.UserOwnedGrafeasNote}).AttestorId("a1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = at.Create("projects/p", &ba.Attestor{}).AttestorId("a2").Do()
	wantCode(t, err, 400, "userOwnedGrafeasNote")
	_, err = at.Create("projects/p", &ba.Attestor{UserOwnedGrafeasNote: a.UserOwnedGrafeasNote}).Do()
	wantCode(t, err, 400, "attestorId")
	_, err = at.Get("projects/p/attestors/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = at.Delete("projects/p/attestors/nope").Do()
	wantCode(t, err, 404, "nope")
	must(at.Delete("projects/p/attestors/a1").Do())
}

func TestContainerAnalysis(t *testing.T) {
	svc := must(ca.NewService(ctx, opts(startDev(t), "/containeranalysis/")...))
	n := must(svc.Projects.Notes.Create("projects/p", &ca.Note{ShortDescription: "s", Attestation: &ca.AttestationNote{Hint: &ca.Hint{HumanReadableName: "h"}}}).NoteId("n1").Do())
	if n.Name != "projects/p/notes/n1" || n.Kind != "ATTESTATION" || n.CreateTime == "" {
		t.Fatalf("%+v", n)
	}
	o := must(svc.Projects.Occurrences.Create("projects/p", &ca.Occurrence{NoteName: n.Name, ResourceUri: "https://gcr.io/p/img@sha256:1",
		Attestation: &ca.AttestationOccurrence{SerializedPayload: "e30=", Signatures: []*ca.Signature{{Signature: "c2ln", PublicKeyId: "k"}}}}).Do())
	if !strings.HasPrefix(o.Name, "projects/p/occurrences/") || o.Kind != "ATTESTATION" || o.CreateTime == "" {
		t.Fatalf("%+v", o)
	}
	o2 := must(svc.Projects.Occurrences.Create("projects/p", &ca.Occurrence{NoteName: n.Name, ResourceUri: "u"}).Do())
	if o2.Name == o.Name {
		t.Fatal("two occurrences share a name")
	}
	if got := must(svc.Projects.Occurrences.Get(o.Name).Do()); got.NoteName != n.Name {
		t.Fatalf("%+v", got)
	}
	if l := must(svc.Projects.Occurrences.List("projects/p").Do()); len(l.Occurrences) != 2 {
		t.Fatalf("%+v", l)
	}
	_, err := svc.Projects.Notes.Create("projects/p", &ca.Note{}).NoteId("n1").Do()
	wantCode(t, err, 409, "already exists")
	_, err = svc.Projects.Notes.Create("projects/p", &ca.Note{}).Do()
	wantCode(t, err, 400, "noteId")
	_, err = svc.Projects.Occurrences.Create("projects/p", &ca.Occurrence{ResourceUri: "u"}).Do()
	wantCode(t, err, 400, "noteName")
	_, err = svc.Projects.Notes.Get("projects/p/notes/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = svc.Projects.Occurrences.Delete("projects/p/occurrences/nope").Do()
	wantCode(t, err, 404, "nope")
	must(svc.Projects.Occurrences.Delete(o.Name).Do())
}

func TestEssentialContacts(t *testing.T) {
	svc := must(ec.NewService(ctx, opts(startDev(t), "/essentialcontacts/")...))
	contact := func(email string) *ec.GoogleCloudEssentialcontactsV1Contact {
		return &ec.GoogleCloudEssentialcontactsV1Contact{Email: email, LanguageTag: "en", NotificationCategorySubscriptions: []string{"ALL"}}
	}
	c := must(svc.Projects.Contacts.Create("projects/p", contact("a@example.com")).Do())
	if !strings.HasPrefix(c.Name, "projects/p/contacts/") || c.Email != "a@example.com" {
		t.Fatalf("%+v", c)
	}
	// organizations and folders are parents too
	oc := must(svc.Organizations.Contacts.Create("organizations/123", contact("o@example.com")).Do())
	if !strings.HasPrefix(oc.Name, "organizations/123/contacts/") {
		t.Fatalf("%+v", oc)
	}
	got := must(svc.Projects.Contacts.Patch(c.Name, &ec.GoogleCloudEssentialcontactsV1Contact{LanguageTag: "fr"}).UpdateMask("languageTag").Do())
	if got.LanguageTag != "fr" || got.Email != "a@example.com" {
		t.Fatalf("%+v", got)
	}
	if l := must(svc.Projects.Contacts.List("projects/p").Do()); len(l.Contacts) != 1 {
		t.Fatalf("%+v", l)
	}
	for _, bad := range []*ec.GoogleCloudEssentialcontactsV1Contact{
		{LanguageTag: "en", NotificationCategorySubscriptions: []string{"ALL"}},
		{Email: "x@example.com", NotificationCategorySubscriptions: []string{"ALL"}},
		{Email: "x@example.com", LanguageTag: "en"},
	} {
		_, err := svc.Projects.Contacts.Create("projects/p", bad).Do()
		wantCode(t, err, 400, "Missing required field")
	}
	_, err := svc.Projects.Contacts.Get("projects/p/contacts/999").Do()
	wantCode(t, err, 404, "999")
	_, err = svc.Projects.Contacts.Delete("projects/p/contacts/999").Do()
	wantCode(t, err, 404, "999")
	_, err = svc.Projects.Contacts.SendTestMessage("projects/p", &ec.GoogleCloudEssentialcontactsV1SendTestMessageRequest{}).Do()
	wantCode(t, err, 501, "sendTestMessage")
	must(svc.Projects.Contacts.Delete(c.Name).Do())
}

func TestOrgPolicy(t *testing.T) {
	svc := must(orgpolicy.NewService(ctx, opts(startDev(t), "/orgpolicy/")...))
	pol := svc.Projects.Policies
	name := "projects/p/policies/gcp.resourceLocations"
	p := must(pol.Create("projects/p", &orgpolicy.GoogleCloudOrgpolicyV2Policy{Name: name, Spec: &orgpolicy.GoogleCloudOrgpolicyV2PolicySpec{
		Rules: []*orgpolicy.GoogleCloudOrgpolicyV2PolicySpecPolicyRule{{Values: &orgpolicy.GoogleCloudOrgpolicyV2PolicySpecPolicyRuleStringValues{AllowedValues: []string{"in:us-locations"}}}}}}).Do())
	if p.Name != name || p.Spec.Etag == "" || p.Spec.UpdateTime == "" {
		t.Fatalf("%+v", p)
	}
	old := p.Spec.Etag
	up := must(pol.Patch(name, &orgpolicy.GoogleCloudOrgpolicyV2Policy{Name: name, Spec: &orgpolicy.GoogleCloudOrgpolicyV2PolicySpec{Reset: true}}).Do())
	if !up.Spec.Reset || up.Spec.Etag == old {
		t.Fatalf("%+v", up)
	}
	if l := must(pol.List("projects/p").Do()); len(l.Policies) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := pol.Create("projects/p", &orgpolicy.GoogleCloudOrgpolicyV2Policy{Name: name}).Do()
	wantCode(t, err, 409, "already exists")
	_, err = pol.Create("projects/p", &orgpolicy.GoogleCloudOrgpolicyV2Policy{}).Do()
	wantCode(t, err, 400, `"name"`)
	_, err = pol.Create("projects/p", &orgpolicy.GoogleCloudOrgpolicyV2Policy{Name: "projects/other/policies/x"}).Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = pol.Get("projects/p/policies/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = pol.Delete("projects/p/policies/nope").Do()
	wantCode(t, err, 404, "nope")
	must(pol.Delete(name).Do())
}

func TestCloudAsset(t *testing.T) {
	svc := must(cloudasset.NewService(ctx, opts(startDev(t), "/cloudasset/")...))
	feeds := svc.Feeds
	feed := func() *cloudasset.CreateFeedRequest {
		return &cloudasset.CreateFeedRequest{FeedId: "f1", Feed: &cloudasset.Feed{AssetTypes: []string{"compute.googleapis.com/Instance"}, ContentType: "RESOURCE",
			FeedOutputConfig: &cloudasset.FeedOutputConfig{PubsubDestination: &cloudasset.PubsubDestination{Topic: "projects/p/topics/t"}}}}
	}
	f := must(feeds.Create("projects/p", feed()).Do())
	if f.Name != "projects/p/feeds/f1" || f.ContentType != "RESOURCE" || f.FeedOutputConfig.PubsubDestination.Topic == "" {
		t.Fatalf("%+v", f)
	}
	got := must(feeds.Patch(f.Name, &cloudasset.UpdateFeedRequest{Feed: &cloudasset.Feed{ContentType: "IAM_POLICY"}, UpdateMask: "contentType"}).Do())
	if got.ContentType != "IAM_POLICY" || got.AssetTypes[0] == "" || got.Name != f.Name {
		t.Fatalf("%+v", got)
	}
	if l := must(feeds.List("projects/p").Do()); len(l.Feeds) != 1 {
		t.Fatalf("%+v", l)
	}
	_, err := feeds.Create("projects/p", feed()).Do()
	wantCode(t, err, 409, "already exists")
	r := feed()
	r.FeedId = ""
	_, err = feeds.Create("projects/p", r).Do()
	wantCode(t, err, 400, "feedId")
	r = feed()
	r.Feed.FeedOutputConfig = nil
	r.FeedId = "f2"
	_, err = feeds.Create("projects/p", r).Do()
	wantCode(t, err, 400, "feedOutputConfig")
	r = feed()
	r.FeedId = "bad id"
	_, err = feeds.Create("projects/p", r).Do()
	wantCode(t, err, 400, "Invalid id")
	_, err = feeds.Create("projects/p", &cloudasset.CreateFeedRequest{FeedId: "f3"}).Do()
	wantCode(t, err, 400, "feed")
	_, err = feeds.Get("projects/p/feeds/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = feeds.Delete("projects/p/feeds/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = feeds.Patch("projects/p/feeds/nope", &cloudasset.UpdateFeedRequest{Feed: &cloudasset.Feed{}}).Do()
	wantCode(t, err, 404, "nope")
	must(feeds.Delete(f.Name).Do())
}
