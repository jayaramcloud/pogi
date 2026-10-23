package generic_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armpolicy"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

func TestPolicy(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azpolicy", "azlogic")
	defs := armtest.Must(armpolicy.NewDefinitionsClient(sub, e.Cred, e.Opts))
	asg := armtest.Must(armpolicy.NewAssignmentsClient(sub, e.Cred, e.Opts))

	rule := map[string]any{"if": map[string]any{"field": "location", "notEquals": "eastus"}, "then": map[string]any{"effect": "audit"}}
	d := armtest.Must(defs.CreateOrUpdate(ctx, "only-eastus", armpolicy.Definition{Properties: &armpolicy.DefinitionProperties{
		DisplayName: to.Ptr("Only East US"), Mode: to.Ptr("All"), PolicyRule: rule}}, nil)).Definition
	if *d.Name != "only-eastus" || *d.Properties.PolicyType != armpolicy.PolicyTypeCustom || *d.Properties.Mode != "All" ||
		*d.ID != "/subscriptions/"+sub+"/providers/Microsoft.Authorization/policyDefinitions/only-eastus" {
		t.Fatalf("definition: %+v %s", d.Properties, *d.ID)
	}
	if got := armtest.Must(defs.Get(ctx, "only-eastus", nil)).Properties.PolicyRule.(map[string]any); got["then"].(map[string]any)["effect"] != "audit" {
		t.Fatalf("the rule changed: %v", got)
	}
	if n := drain(t, defs.NewListPager(nil), func(r armpolicy.DefinitionsClientListResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("definitions: %d", n)
	}

	// assignments at subscription, resource group, resource and management group scope
	scopes := map[string]string{
		"sub": "/subscriptions/" + sub,
		"rg":  "/subscriptions/" + sub + "/resourceGroups/" + rg,
		"res": "/subscriptions/" + sub + "/resourceGroups/" + rg + "/providers/Microsoft.Logic/workflows/wf/triggers/t",
		"mg":  "/providers/Microsoft.Management/managementGroups/mg1",
	}
	for name, scope := range scopes {
		a := armtest.Must(asg.Create(ctx, scope, "audit-"+name, armpolicy.Assignment{Properties: &armpolicy.AssignmentProperties{
			PolicyDefinitionID: d.ID, DisplayName: to.Ptr("Audit " + name)}}, nil)).Assignment
		if *a.Name != "audit-"+name || *a.Properties.Scope != scope || *a.Properties.EnforcementMode != armpolicy.EnforcementModeDefault ||
			*a.ID != scope+"/providers/Microsoft.Authorization/policyAssignments/audit-"+name {
			t.Fatalf("%s assignment: %+v %s", name, a.Properties, *a.ID)
		}
		got := armtest.Must(asg.Get(ctx, scope, "audit-"+name, nil)).Assignment
		if *got.Properties.DisplayName != "Audit "+name || *got.Properties.PolicyDefinitionID != *d.ID {
			t.Fatalf("%s get: %+v", name, got.Properties)
		}
	}
	if n := drain(t, asg.NewListForResourceGroupPager(rg, nil), func(r armpolicy.AssignmentsClientListForResourceGroupResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("assignments of the group: %d", n)
	}
	if n := drain(t, asg.NewListForManagementGroupPager("mg1", nil), func(r armpolicy.AssignmentsClientListForManagementGroupResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("assignments of the management group: %d", n)
	}
	if n := drain(t, asg.NewListForResourcePager(rg, "Microsoft.Logic", "workflows/wf", "triggers", "t", nil), func(r armpolicy.AssignmentsClientListForResourceResponse) int { return len(r.Value) }); n != 1 {
		t.Fatalf("assignments of the resource: %d", n)
	}
	for name, scope := range scopes {
		armtest.Must(asg.Delete(ctx, scope, "audit-"+name, nil))
		_, err := asg.Get(ctx, scope, "audit-"+name, nil)
		armtest.WantErr(t, err, 404, "ResourceNotFound")
	}
	armtest.Must(defs.Delete(ctx, "only-eastus", nil))
	_, err := defs.Get(ctx, "only-eastus", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}

func TestPolicyFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azpolicy")
	defs := armtest.Must(armpolicy.NewDefinitionsClient(sub, e.Cred, e.Opts))
	asg := armtest.Must(armpolicy.NewAssignmentsClient(sub, e.Cred, e.Opts))
	scope := "/subscriptions/" + sub

	// the spec's name rule: no < > % & : ? or a trailing space
	_, err := defs.CreateOrUpdate(ctx, "bad:name", armpolicy.Definition{Properties: &armpolicy.DefinitionProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = asg.Create(ctx, scope, "bad name ", armpolicy.Assignment{Properties: &armpolicy.AssignmentProperties{}}, nil)
	armtest.WantErr(t, err, 400, "InvalidResourceName")
	_, err = defs.Get(ctx, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	_, err = asg.Get(ctx, scope+"/resourceGroups/missing", "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceGroupNotFound")
	armtest.Must(asg.Delete(ctx, scope, "nope", nil)) // deleting what is not there is not an error
}
