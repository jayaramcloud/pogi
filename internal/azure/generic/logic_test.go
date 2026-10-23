package generic_test

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/logic/armlogic"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/azure/armtest"
)

func TestLogicApps(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azlogic")
	wf := armtest.Must(armlogic.NewWorkflowsClient(sub, e.Cred, e.Opts))

	def := map[string]any{"$schema": "https://schema.management.azure.com/providers/Microsoft.Logic/schemas/2016-06-01/workflowdefinition.json#",
		"contentVersion": "1.0.0.0", "triggers": map[string]any{}, "actions": map[string]any{}, "outputs": map[string]any{}}
	w := armtest.Must(wf.CreateOrUpdate(ctx, rg, "flow", armlogic.Workflow{Location: to.Ptr("eastus"), Tags: map[string]*string{"a": to.Ptr("b")},
		Properties: &armlogic.WorkflowProperties{Definition: def}}, nil)).Workflow
	if *w.Name != "flow" || *w.Type != "Microsoft.Logic/workflows" || *w.Properties.ProvisioningState != armlogic.WorkflowProvisioningStateSucceeded ||
		*w.Properties.State != armlogic.WorkflowStateEnabled || w.Properties.CreatedTime == nil || *w.Tags["a"] != "b" {
		t.Fatalf("workflow: %+v", w.Properties)
	}
	got := armtest.Must(wf.Get(ctx, rg, "flow", nil)).Workflow
	if got.Properties.Definition.(map[string]any)["contentVersion"] != "1.0.0.0" || *got.Properties.Version != *w.Properties.Version {
		t.Fatalf("get: %+v", got.Properties)
	}
	// a change makes a new version and keeps the creation time; the state can be turned off
	up := armtest.Must(wf.CreateOrUpdate(ctx, rg, "flow", armlogic.Workflow{Location: to.Ptr("eastus"),
		Properties: &armlogic.WorkflowProperties{Definition: def, State: to.Ptr(armlogic.WorkflowStateDisabled)}}, nil)).Workflow
	if *up.Properties.State != armlogic.WorkflowStateDisabled || *up.Properties.Version == *w.Properties.Version || !up.Properties.CreatedTime.Equal(*w.Properties.CreatedTime) {
		t.Fatalf("update: %+v", up.Properties)
	}
	armtest.Must(wf.CreateOrUpdate(ctx, rg, "second", armlogic.Workflow{Location: to.Ptr("eastus"), Properties: &armlogic.WorkflowProperties{}}, nil))
	if n := drain(t, wf.NewListByResourceGroupPager(rg, nil), func(r armlogic.WorkflowsClientListByResourceGroupResponse) int { return len(r.Value) }); n != 2 {
		t.Fatalf("workflows of the group: %d", n)
	}
	if n := drain(t, wf.NewListBySubscriptionPager(nil), func(r armlogic.WorkflowsClientListBySubscriptionResponse) int { return len(r.Value) }); n != 2 {
		t.Fatalf("workflows of the subscription: %d", n)
	}
	armtest.Must(wf.Delete(ctx, rg, "flow", nil))
	_, err := wf.Get(ctx, rg, "flow", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
	armtest.Must(wf.Delete(ctx, rg, "flow", nil))
}

func TestLogicFailures(t *testing.T) {
	e := armtest.Start(t, pogi.Config{}, "azlogic")
	wf := armtest.Must(armlogic.NewWorkflowsClient(sub, e.Cred, e.Opts))
	_, err := wf.CreateOrUpdate(ctx, rg, "nolocation", armlogic.Workflow{Properties: &armlogic.WorkflowProperties{}}, nil)
	armtest.WantErr(t, err, 400, "LocationRequired")
	_, err = wf.CreateOrUpdate(ctx, "missing-group", "w", armlogic.Workflow{Location: to.Ptr("eastus")}, nil)
	armtest.WantErr(t, err, 404, "ResourceGroupNotFound")
	_, err = wf.Get(ctx, rg, "nope", nil)
	armtest.WantErr(t, err, 404, "ResourceNotFound")
}
