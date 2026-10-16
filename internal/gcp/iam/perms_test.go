package iam

import (
	"slices"
	"strings"
	"testing"
)

func TestPermission(t *testing.T) {
	for _, c := range []struct{ svc, op, res, want string }{
		{"pubsub", "CreateTopic", "projects/p/topics/t", "pubsub.topics.create"},
		{"pubsub", "Publish", "projects/p/topics/t", "pubsub.topics.publish"},
		{"pubsub", "Pull", "projects/p/subscriptions/s", "pubsub.subscriptions.consume"},
		{"pubsub", "ListTopics", "projects/p", "pubsub.topics.list"},
		{"pubsub", "SetIamPolicy", "projects/p/topics/t", "pubsub.topics.setIamPolicy"},
		{"gcs", "objects.insert", "projects/_/buckets/b", "storage.objects.create"},
		{"gcs", "buckets.getIamPolicy", "projects/_/buckets/b", "storage.buckets.getIamPolicy"},
		{"secretmanager", "AccessSecretVersion", "projects/p/secrets/s/versions/1", "secretmanager.versions.access"},
		{"secretmanager", "CreateSecret", "projects/p", "secretmanager.secrets.create"},
		{"firestore", "RunQuery", "projects/p/databases/(default)", "datastore.entities.list"},
		{"run", "RunJob", "projects/p/locations/l/jobs/j", "run.jobs.run"},
		{"run", "UpdateService", "projects/p/locations/l/services/s", "run.services.update"},
		{"cloudfunctions", "GenerateUploadUrl", "projects/p/locations/l", "cloudfunctions.functions.sourceCodeSet"},
		{"cloudkms", "Encrypt", "projects/p/locations/l/keyRings/r/cryptoKeys/k", "cloudkms.cryptoKeyVersions.useToEncrypt"},
		{"cloudkms", "GetIamPolicy", "projects/p/locations/l/keyRings/r", "cloudkms.keyRings.getIamPolicy"},
		{"cloudtasks", "PauseQueue", "projects/p/locations/l/queues/q", "cloudtasks.queues.pause"},
		{"cloudtasks", "CreateTask", "projects/p/locations/l/queues/q", "cloudtasks.tasks.create"},
		{"resourcemanager", "SetIamPolicy", "projects/p", "resourcemanager.projects.setIamPolicy"},
	} {
		if got, ok := Permission(c.svc, c.op, c.res); got != c.want || !ok {
			t.Errorf("%s %s: got %q (%v), want %q", c.svc, c.op, got, ok, c.want)
		}
	}
	for _, c := range [][3]string{{"pubsub", "Nope", ""}, {"gcpiam", "CreateServiceAccount", ""}, {"pubsub", "GetIamPolicy", "projects/p"}} {
		if got, ok := Permission(c[0], c[1], c[2]); ok {
			t.Errorf("%v is modelled as %q", c, got)
		}
	}
}

func TestScoped(t *testing.T) {
	if got := scoped("pubsub.topics.create", "projects/p/topics/t"); got != "projects/p" {
		t.Errorf("creating a topic is governed by %q", got)
	}
	if got := scoped("pubsub.topics.publish", "projects/p/topics/t"); got != "projects/p/topics/t" {
		t.Errorf("publishing is governed by %q", got)
	}
}

// Every permission a modelled call can need is carried by some predefined role, and the table the
// authorizer reads is the one GetRole serves.
func TestRoles(t *testing.T) {
	have := map[string]bool{}
	for _, r := range predefined {
		if !slices.IsSorted(r.Perms) || len(r.Perms) == 0 {
			t.Errorf("%s: permissions are empty or unsorted", r.Name)
		}
		if r.Name != "roles/owner" && r.Name != "roles/editor" && r.Name != "roles/viewer" {
			for _, p := range r.Perms {
				have[p] = true
			}
		}
		if predefinedByName[r.Name] != r {
			t.Errorf("%s is not in the lookup", r.Name)
		}
	}
	for p := range callPerms() {
		if !have[p] && !strings.HasPrefix(p, "resourcemanager.projects.") {
			t.Errorf("no predefined role carries %s", p)
		}
	}
	has := func(role, perm string) bool {
		_, ok := slices.BinarySearch(predefinedByName[role].Perms, perm)
		return ok
	}
	for _, c := range []struct {
		role, perm string
		want       bool
	}{
		{"roles/owner", "resourcemanager.projects.setIamPolicy", true},
		{"roles/editor", "pubsub.topics.publish", true},
		{"roles/editor", "pubsub.topics.setIamPolicy", false},
		{"roles/viewer", "pubsub.topics.get", true},
		{"roles/viewer", "pubsub.topics.publish", false},
		{"roles/pubsub.subscriber", "pubsub.subscriptions.consume", true},
		{"roles/pubsub.admin", "pubsub.topics.setIamPolicy", true},
		{"roles/pubsub.editor", "pubsub.topics.getIamPolicy", false},
		{"roles/storage.objectViewer", "storage.objects.create", false},
		{"roles/storage.objectAdmin", "storage.objects.delete", true},
		{"roles/secretmanager.secretAccessor", "secretmanager.versions.access", true},
	} {
		if has(c.role, c.perm) != c.want {
			t.Errorf("%s has %s: want %v", c.role, c.perm, c.want)
		}
	}
}

func TestPlan(t *testing.T) {
	call := func(perm, res string) Call {
		return Call{Service: "pubsub", Perm: perm, Resource: res, Project: "pogi", Verdict: "denied"}
	}
	// publishing to one topic: publisher on that topic, not editor, not the project
	p := Principal{Member: "serviceAccount:orders@pogi.iam.gserviceaccount.com", Actions: []Call{call("pubsub.topics.publish", "projects/pogi/topics/orders")}}
	pl := p.Plan(false)
	if len(pl.Bindings) != 1 || pl.Bindings[0].Role != "roles/pubsub.publisher" || pl.Bindings[0].Resource != "projects/pogi/topics/orders" {
		t.Fatalf("plan: %+v", pl.Bindings)
	}
	// publishing and consuming: publisher and subscriber, each on what it touched; two topics move it to the project
	p.Actions = append(p.Actions, call("pubsub.subscriptions.consume", "projects/pogi/subscriptions/s"), call("pubsub.topics.publish", "projects/pogi/topics/audit"))
	var got []string
	for _, b := range p.Plan(false).Bindings {
		got = append(got, b.Role+" "+b.Resource)
	}
	if want := []string{"roles/pubsub.publisher projects/pogi", "roles/pubsub.subscriber projects/pogi/subscriptions/s"}; !slices.Equal(got, want) {
		t.Fatalf("plan: %v want %v", got, want)
	}
	// the custom alternative is one role with exactly the permissions
	c := p.Plan(true)
	if c.Custom != "projects/pogi/roles/pogi_orders" || len(c.Bindings) != 1 || len(c.Perms) != 2 {
		t.Fatalf("custom plan: %+v", c)
	}
	if !strings.Contains(c.Terraform(), "google_project_iam_custom_role") || !strings.Contains(c.Gcloud(), "gcloud iam roles create pogi_orders") {
		t.Fatalf("snippets:\n%s\n%s", c.Terraform(), c.Gcloud())
	}
}
