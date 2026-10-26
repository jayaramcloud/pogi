package web

import (
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

func TestScopeFiltersPerCloud(t *testing.T) {
	sc := scope{Account: "000000000000", Region: "us-east-1", Project: "pogi", Subscription: "sub1", Location: "us-central1", Group: "pogi", AzLocation: "eastus"}
	for _, c := range []struct {
		cloud     string
		cols, row []string
		want      bool
	}{
		{core.GCP, []string{"Name", "Project", "Location"}, []string{"k", "pogi", "us-central1"}, true},
		{core.GCP, []string{"Name", "Project", "Location"}, []string{"k", "other", "us-central1"}, false},
		{core.GCP, []string{"Name", "Project", "Location"}, []string{"k", "pogi", "europe-west1"}, false},
		{core.GCP, []string{"Name", "Project", "Location"}, []string{"k", "pogi", "global"}, true},        // global is everywhere
		{core.GCP, []string{"Name", "Project", "Location"}, []string{"k", "pogi", "US-CENTRAL1"}, true},   // Cloud Storage's capitals
		{core.GCP, []string{"Name", "Project", "Location"}, []string{"k", "pogi", "us-central1-a"}, true}, // a zone is in its region
		{core.GCP, []string{"Name", "Project", "Location"}, []string{"k", "pogi", "us-central10"}, false},
		{core.GCP, []string{"Name"}, []string{"projects/pogi/locations/europe-west1/keyRings/r"}, false},
		{core.GCP, []string{"Name"}, []string{"projects/pogi/locations/us-central1/keyRings/r"}, true},
		{core.GCP, []string{"Name", "Region"}, []string{"b", "eu-west-1"}, true}, // an AWS-style column is not Google's
		{core.Azure, []string{"Name", "Resource group", "Location"}, []string{"a", "pogi", "eastus"}, true},
		{core.Azure, []string{"Name", "Resource group", "Location"}, []string{"a", "demo", "eastus"}, false},
		{core.Azure, []string{"Name", "Resource group", "Location"}, []string{"a", "pogi", "westus"}, false},
		{core.Azure, []string{"Name", "Location"}, []string{"a", "eastus"}, true}, // says no group: stays
		{core.Azure, []string{"ID"}, []string{"/subscriptions/sub1/resourceGroups/demo/providers/x/y"}, false},
		{core.Azure, []string{"ID"}, []string{"/subscriptions/sub2/resourceGroups/pogi/providers/x/y"}, false},
		{core.Azure, []string{"ID"}, []string{"/subscriptions/sub1/resourceGroups/POGI/providers/x/y"}, false}, // names are exact
		{core.Azure, []string{"Name", "Location", "Project"}, []string{"a", "eastus", "other"}, true},          // not Azure's column
		{core.AWS, []string{"Name", "Location"}, []string{"a", "Mars"}, true},                                  // AWS ignores Google's and Azure's fields
		{core.AWS, []string{"Name", "Resource group"}, []string{"a", "demo"}, true},
	} {
		if got := sc.forCloud(c.cloud).allows(c.cols, c.row); got != c.want {
			t.Errorf("%s %v %v: allows = %v, want %v", c.cloud, c.cols, c.row, got, c.want)
		}
	}
}

func TestScopeCreateDefaults(t *testing.T) {
	var zero scope
	if zero.location() != "us-central1" || zero.group() != "pogi" || zero.azLocation() != "eastus" {
		t.Error("an empty scope should create in the defaults")
	}
	s := scope{Location: all, Group: all, AzLocation: all}
	if s.location() != "us-central1" || s.group() != "pogi" || s.azLocation() != "eastus" {
		t.Error("* should create in the defaults")
	}
	s = scope{Location: "europe-west1", Group: "demo", AzLocation: "westus"}
	if s.location() != "europe-west1" || s.group() != "demo" || s.azLocation() != "westus" {
		t.Error("a chosen value should be used for creates")
	}
}

func TestScopeTextPerCloud(t *testing.T) {
	s := scope{Account: "000000000000", Region: "us-east-1", Project: "pogi", Location: "us-central1", Group: "pogi", AzLocation: all}
	for cloud, want := range map[string]string{core.AWS: "000000000000 / us-east-1", core.GCP: "pogi · us-central1", core.Azure: "pogi · all locations"} {
		if got := s.text(cloud); got != want {
			t.Errorf("text(%s) = %q, want %q", cloud, got, want)
		}
	}
}

func TestScopeBarHidden(t *testing.T) {
	sc := scope{Project: "pogi", Location: "us-central1"}
	b := scopeFor(core.GCP, sc, "/_pogi/resources/pubsub", "Topics", 2)
	if b.Hidden != "2 topics in other projects or locations" || !strings.Contains(b.Vals, `"project":"*"`) || !strings.Contains(b.Vals, `"location":"*"`) {
		t.Errorf("scope bar = %+v", b)
	}
	if b := scopeFor(core.GCP, scope{Project: all, Location: all}, "/", "Topics", 0); b.Vals != "" || b.Hidden != "" {
		t.Errorf("nothing hidden and all shown, but the bar offers %+v", b)
	}
}
