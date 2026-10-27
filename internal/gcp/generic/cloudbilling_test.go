package generic_test

import (
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
	"google.golang.org/api/cloudbilling/v1"
)

func TestCloudBilling(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"cloudbilling"}})
	svc := must(cloudbilling.NewService(ctx, opts(srv, "/cloudbilling/")...))
	const acct = "billingAccounts/000000-000000-000000"

	// one open billing account to start with
	l := must(svc.BillingAccounts.List().Do())
	if len(l.BillingAccounts) != 1 || l.BillingAccounts[0].Name != acct || !l.BillingAccounts[0].Open || l.BillingAccounts[0].DisplayName != "My Billing Account" {
		t.Fatalf("%+v", l.BillingAccounts)
	}
	if a := must(svc.BillingAccounts.Get(acct).Do()); a.Name != acct {
		t.Fatalf("%+v", a)
	}
	// Terraform checks this permission before it links a project
	perm := []string{"billing.resourceAssociations.create"}
	if got := must(svc.BillingAccounts.TestIamPermissions(acct, &cloudbilling.TestIamPermissionsRequest{Permissions: perm}).Do()); len(got.Permissions) != 1 || got.Permissions[0] != perm[0] {
		t.Fatalf("%+v", got)
	}

	// a project starts with no billing account, then is linked and unlinked
	info := must(svc.Projects.GetBillingInfo("projects/p").Do())
	if info.Name != "projects/p/billingInfo" || info.ProjectId != "p" || info.BillingEnabled || info.BillingAccountName != "" {
		t.Fatalf("%+v", info)
	}
	info = must(svc.Projects.UpdateBillingInfo("projects/p", &cloudbilling.ProjectBillingInfo{BillingAccountName: acct}).Do())
	if !info.BillingEnabled || info.BillingAccountName != acct {
		t.Fatalf("%+v", info)
	}
	if info = must(svc.Projects.GetBillingInfo("projects/p").Do()); !info.BillingEnabled || info.BillingAccountName != acct {
		t.Fatalf("%+v", info)
	}
	if ps := must(svc.BillingAccounts.Projects.List(acct).Do()); len(ps.ProjectBillingInfo) != 1 || ps.ProjectBillingInfo[0].ProjectId != "p" {
		t.Fatalf("%+v", ps.ProjectBillingInfo)
	}

	// the link survives a save and a load
	st := srv.Hub.Service("cloudbilling").(core.Stateful)
	saved := must(st.Save())
	st.Reset()
	if info = must(svc.Projects.GetBillingInfo("projects/p").Do()); info.BillingEnabled {
		t.Fatalf("after reset: %+v", info)
	}
	if err := st.Load(saved); err != nil {
		t.Fatal(err)
	}
	if info = must(svc.Projects.GetBillingInfo("projects/p").Do()); info.BillingAccountName != acct {
		t.Fatalf("after load: %+v", info)
	}
	must(svc.BillingAccounts.Get(acct).Do()) // the default account is still there

	// an empty request unlinks, as Terraform sends it
	info = must(svc.Projects.UpdateBillingInfo("projects/p", &cloudbilling.ProjectBillingInfo{}).Do())
	if info.BillingEnabled || info.BillingAccountName != "" {
		t.Fatalf("%+v", info)
	}
	if ps := must(svc.BillingAccounts.Projects.List(acct).Do()); len(ps.ProjectBillingInfo) != 0 {
		t.Fatalf("%+v", ps.ProjectBillingInfo)
	}

	// failures
	_, err := svc.BillingAccounts.Get("billingAccounts/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = svc.Projects.UpdateBillingInfo("projects/p", &cloudbilling.ProjectBillingInfo{BillingAccountName: "billingAccounts/nope"}).Do()
	wantCode(t, err, 404, "billingAccounts/nope")
	_, err = svc.Projects.UpdateBillingInfo("projects/p", &cloudbilling.ProjectBillingInfo{BillingAccountName: "000000-000000-000000"}).Do()
	wantCode(t, err, 400, "billingAccounts/")
	_, err = svc.BillingAccounts.TestIamPermissions("billingAccounts/nope", &cloudbilling.TestIamPermissionsRequest{Permissions: perm}).Do()
	wantCode(t, err, 404, "nope")
	_, err = svc.BillingAccounts.Projects.List("billingAccounts/nope").Do()
	wantCode(t, err, 404, "nope")
	_, err = svc.BillingAccounts.Create(&cloudbilling.BillingAccount{DisplayName: "x"}).Do()
	wantCode(t, err, 501, "one billing account")
}
