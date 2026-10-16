package generic

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// IAM Identity Center administration. Every account has one default instance;
// assigning a permission set to an account creates its AWSReservedSSO_ role in IAM.

const iamTarget = "AWSIdentityManagementV20100508"

// ssoInstance makes sure the account's default instance exists and returns it.
func ssoInstance(c *Call) *Resource {
	sum := sha256.Sum256([]byte(c.Account))
	arn := fmt.Sprintf("arn:%s:sso:::instance/ssoins-%x", core.Partition(c.Region), sum[:8])
	if r := c.Get("Instance", arn); r != nil {
		return r
	}
	now := time.Now().UTC()
	r := &Resource{Type: "Instance", Key: arn, IDs: map[string]string{"InstanceArn": arn}, ARN: arn, Name: arn, Account: c.Account, Region: c.key(), Created: now, Updated: now,
		Data: map[string]any{"InstanceArn": arn, "IdentityStoreId": fmt.Sprintf("d-%x", sum[8:13]), "OwnerAccountId": c.Account, "Status": "ACTIVE",
			"CreatedDate": now.Format(time.RFC3339Nano), "Name": "pogi"}}
	c.Put(r)
	return r
}

// psOf finds the permission set a request names.
func psOf(c *Call) (*Resource, error) {
	r := c.Get("PermissionSet", str(c.In["InstanceArn"]), str(c.In["PermissionSetArn"]))
	if r == nil {
		return nil, c.m.modelErr(c.op, eNotFound, "PermissionSet", "Could not find PermissionSet with arn "+str(c.In["PermissionSetArn"]))
	}
	return r, nil
}

// assignmentAccounts are the accounts a permission set is assigned in.
func assignmentAccounts(c *Call, psArn string) []string {
	var out []string
	for _, a := range c.List("AccountAssignment") {
		if str(a.Data["PermissionSetArn"]) == psArn && str(a.Data["TargetType"]) == "AWS_ACCOUNT" && !slices.Contains(out, str(a.Data["TargetId"])) {
			out = append(out, str(a.Data["TargetId"]))
		}
	}
	return out
}

// ssoRole makes the account's IAM role of a permission set match it (create it, set its policies), or removes it with gone.
// ponytail: IAM errors are ignored, so assignments work with IAM off; the role is their only effect.
func ssoRole(c *Call, ps *Resource, acct string, gone bool) {
	name := ssoRoleName(str(ps.Data["Name"]), ps.IDs["PermissionSetArn"])
	h, region := c.Hub(), c.Region
	var att struct{ AttachedPolicies []struct{ PolicyArn string } }
	h.Call(acct, region, iamTarget, "ListAttachedRolePolicies", map[string]any{"RoleName": name}, &att)
	if gone {
		for _, p := range att.AttachedPolicies {
			h.Call(acct, region, iamTarget, "DetachRolePolicy", map[string]any{"RoleName": name, "PolicyArn": p.PolicyArn}, nil)
		}
		h.Call(acct, region, iamTarget, "DeleteRolePolicy", map[string]any{"RoleName": name, "PolicyName": "AwsSSOInlinePolicy"}, nil)
		h.Call(acct, region, iamTarget, "DeleteRole", map[string]any{"RoleName": name}, nil)
		return
	}
	trust := fmt.Sprintf(`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Federated":"arn:%s:iam::%s:saml-provider/AWSSSO_%s_DO_NOT_DELETE"},"Action":["sts:AssumeRoleWithSAML","sts:TagSession"],"Condition":{"StringEquals":{"SAML:aud":"https://signin.aws.amazon.com/saml"}}}]}`,
		core.Partition(region), acct, name[len(name)-16:])
	h.Call(acct, region, iamTarget, "CreateRole", map[string]any{"RoleName": name, "Path": "/aws-reserved/sso.amazonaws.com/", "AssumeRolePolicyDocument": trust,
		"Description": "Created by IAM Identity Center for permission set " + str(ps.Data["Name"])}, nil)
	want := map[string]bool{}
	for _, m := range asList(ps.Data["ManagedPolicies"]) {
		want[str(m.(map[string]any)["Arn"])] = true
	}
	for _, m := range asList(ps.Data["CustomerManagedPolicies"]) {
		mm := m.(map[string]any)
		path := str(mm["Path"])
		if path == "" {
			path = "/"
		}
		want[fmt.Sprintf("arn:%s:iam::%s:policy%s%s", core.Partition(region), acct, path, str(mm["Name"]))] = true
	}
	for _, p := range att.AttachedPolicies {
		if !want[p.PolicyArn] {
			h.Call(acct, region, iamTarget, "DetachRolePolicy", map[string]any{"RoleName": name, "PolicyArn": p.PolicyArn}, nil)
		}
		delete(want, p.PolicyArn)
	}
	for arn := range want {
		h.Call(acct, region, iamTarget, "AttachRolePolicy", map[string]any{"RoleName": name, "PolicyArn": arn}, nil)
	}
	if in := c.Get("InlinePolicy", ps.IDs["InstanceArn"], ps.IDs["PermissionSetArn"]); in != nil && str(in.Data["InlinePolicy"]) != "" {
		h.Call(acct, region, iamTarget, "PutRolePolicy", map[string]any{"RoleName": name, "PolicyName": "AwsSSOInlinePolicy", "PolicyDocument": str(in.Data["InlinePolicy"])}, nil)
	} else {
		h.Call(acct, region, iamTarget, "DeleteRolePolicy", map[string]any{"RoleName": name, "PolicyName": "AwsSSOInlinePolicy"}, nil)
	}
}

// ssoResync pushes a permission set's policies to the roles of the accounts it is assigned in.
func ssoResync(c *Call, ps *Resource) {
	for _, acct := range assignmentAccounts(c, ps.IDs["PermissionSetArn"]) {
		ssoRole(c, ps, acct, false)
	}
}

// opStatus records an assignment or provisioning request, which finishes at once.
func opStatus(c *Call, kind string, f map[string]any) map[string]any {
	id, now := core.UUID(), c.now()
	f["RequestId"], f["Status"], f["CreatedDate"] = id, "SUCCEEDED", now.Format(time.RFC3339Nano)
	c.Put(&Resource{Type: "OpStatus", Key: id, IDs: map[string]string{"RequestId": id}, Name: id, Account: c.Account, Region: c.key(), Created: now, Updated: now,
		Data: map[string]any{"Kind": kind, "Status": f}})
	return f
}

func statusOf(c *Call, kind, idMember, outMember string) (map[string]any, error) {
	r := c.Get("OpStatus", str(c.In[idMember]))
	if r == nil || r.Data["Kind"] != kind {
		return nil, c.NotFound("Request %s not found", str(c.In[idMember]))
	}
	return map[string]any{outMember: r.Data["Status"]}, nil
}

func statusList(c *Call, kind, outMember string) map[string]any {
	out := []any{}
	for _, r := range c.List("OpStatus") {
		if r.Data["Kind"] == kind {
			s := r.Data["Status"].(map[string]any)
			out = append(out, map[string]any{"RequestId": s["RequestId"], "Status": s["Status"], "CreatedDate": s["CreatedDate"]})
		}
	}
	return map[string]any{outMember: out}
}

func assignmentItem(a *Resource) map[string]any {
	return map[string]any{"AccountId": a.Data["TargetId"], "PermissionSetArn": a.Data["PermissionSetArn"], "PrincipalId": a.Data["PrincipalId"], "PrincipalType": a.Data["PrincipalType"]}
}

func init() {
	Override("sso-admin", "ListInstances", func(c *Call) (map[string]any, error) {
		ssoInstance(c)
		return c.Default()
	})
	Override("sso-admin", "DescribeInstance", func(c *Call) (map[string]any, error) {
		ssoInstance(c)
		return c.Default()
	})

	// A permission set's ARN is arn:aws:sso:::permissionSet/<instance id>/ps-<hex>, and the list holds ARNs.
	Override("sso-admin", "CreatePermissionSet", func(c *Call) (map[string]any, error) {
		inst := str(c.In["InstanceArn"])
		c.In["PermissionSetArn"] = fmt.Sprintf("arn:%s:sso:::permissionSet/%s/ps-%s", core.Partition(c.Region), inst[strings.LastIndex(inst, "/")+1:], randHex(16))
		return c.Default()
	})
	Override("sso-admin", "ListPermissionSets", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, ps := range c.List("PermissionSet") {
			if ps.IDs["InstanceArn"] == str(c.In["InstanceArn"]) {
				out = append(out, ps.IDs["PermissionSetArn"])
			}
		}
		return map[string]any{"PermissionSets": out}, nil
	})
	// An assignment creates the role in the account (the last one's deletion removes it).
	for kind, op := range map[string]string{"Creation": "CreateAccountAssignment", "Deletion": "DeleteAccountAssignment"} {
		Override("sso-admin", op, func(c *Call) (map[string]any, error) {
			ps, err := psOf(c)
			if err != nil {
				return nil, err
			}
			if _, err := c.Default(); err != nil {
				return nil, err
			}
			if str(c.In["TargetType"]) == "AWS_ACCOUNT" {
				acct := str(c.In["TargetId"])
				ssoRole(c, ps, acct, kind == "Deletion" && !slices.Contains(assignmentAccounts(c, ps.IDs["PermissionSetArn"]), acct))
			}
			st := opStatus(c, kind, map[string]any{"TargetId": c.In["TargetId"], "TargetType": c.In["TargetType"], "PermissionSetArn": c.In["PermissionSetArn"],
				"PrincipalType": c.In["PrincipalType"], "PrincipalId": c.In["PrincipalId"]})
			return map[string]any{"AccountAssignment" + kind + "Status": st}, nil
		})
		Override("sso-admin", "DescribeAccountAssignment"+kind+"Status", func(c *Call) (map[string]any, error) {
			return statusOf(c, kind, "AccountAssignment"+kind+"RequestId", "AccountAssignment"+kind+"Status")
		})
		Override("sso-admin", "ListAccountAssignment"+kind+"Status", func(c *Call) (map[string]any, error) {
			return statusList(c, kind, "AccountAssignments"+kind+"Status"), nil
		})
	}
	Override("sso-admin", "ProvisionPermissionSet", func(c *Call) (map[string]any, error) {
		ps, err := psOf(c)
		if err != nil {
			return nil, err
		}
		ssoResync(c, ps)
		return map[string]any{"PermissionSetProvisioningStatus": opStatus(c, "Provision", map[string]any{"PermissionSetArn": c.In["PermissionSetArn"], "AccountId": c.In["TargetId"]})}, nil
	})
	Override("sso-admin", "DescribePermissionSetProvisioningStatus", func(c *Call) (map[string]any, error) {
		return statusOf(c, "Provision", "ProvisionPermissionSetRequestId", "PermissionSetProvisioningStatus")
	})
	Override("sso-admin", "ListPermissionSetProvisioningStatus", func(c *Call) (map[string]any, error) {
		return statusList(c, "Provision", "PermissionSetsProvisioningStatus"), nil
	})

	Override("sso-admin", "ListAccountAssignments", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, a := range c.List("AccountAssignment") {
			if str(a.Data["TargetId"]) == str(c.In["AccountId"]) && str(a.Data["PermissionSetArn"]) == str(c.In["PermissionSetArn"]) {
				out = append(out, assignmentItem(a))
			}
		}
		return map[string]any{"AccountAssignments": out}, nil
	})
	Override("sso-admin", "ListAccountAssignmentsForPrincipal", func(c *Call) (map[string]any, error) {
		f, _ := c.In["Filter"].(map[string]any)
		out := []any{}
		for _, a := range c.List("AccountAssignment") {
			if str(a.Data["PrincipalId"]) == str(c.In["PrincipalId"]) && str(a.Data["PrincipalType"]) == str(c.In["PrincipalType"]) && (f["AccountId"] == nil || str(a.Data["TargetId"]) == str(f["AccountId"])) {
				out = append(out, assignmentItem(a))
			}
		}
		return map[string]any{"AccountAssignments": out}, nil
	})
	Override("sso-admin", "ListAccountsForProvisionedPermissionSet", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, a := range assignmentAccounts(c, str(c.In["PermissionSetArn"])) {
			out = append(out, a)
		}
		return map[string]any{"AccountIds": out}, nil
	})
	Override("sso-admin", "ListPermissionSetsProvisionedToAccount", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, ps := range c.List("PermissionSet") {
			if slices.Contains(assignmentAccounts(c, ps.IDs["PermissionSetArn"]), str(c.In["AccountId"])) {
				out = append(out, ps.IDs["PermissionSetArn"])
			}
		}
		return map[string]any{"PermissionSets": out}, nil
	})

	// Managed policies are two lists in the permission set.
	for _, p := range []struct{ noun, field, list, key, lop string }{
		{"ManagedPolicy", "ManagedPolicies", "AttachedManagedPolicies", "ManagedPolicyArn", "ListManagedPoliciesInPermissionSet"},
		{"CustomerManagedPolicyReference", "CustomerManagedPolicies", "CustomerManagedPolicyReferences", "CustomerManagedPolicyReference", "ListCustomerManagedPolicyReferencesInPermissionSet"},
	} {
		same := func(a, b any) bool {
			if p.noun == "ManagedPolicy" {
				return str(a) == str(b.(map[string]any)["Arn"])
			}
			am, bm := a.(map[string]any), b.(map[string]any)
			return str(am["Name"]) == str(bm["Name"]) && str(am["Path"]) == str(bm["Path"])
		}
		item := func(v any) any {
			if p.noun == "ManagedPolicy" {
				return map[string]any{"Arn": v, "Name": str(v)[strings.LastIndex(str(v), "/")+1:]}
			}
			return v
		}
		Override("sso-admin", "Attach"+p.noun+"ToPermissionSet", func(c *Call) (map[string]any, error) {
			ps, err := psOf(c)
			if err != nil {
				return nil, err
			}
			for _, e := range asList(ps.Data[p.field]) {
				if same(c.In[p.key], e) {
					return nil, c.Exists("%s is already attached", p.noun)
				}
			}
			ps.Data[p.field] = append(asList(ps.Data[p.field]), item(c.In[p.key]))
			c.Put(ps)
			ssoResync(c, ps)
			return map[string]any{}, nil
		})
		Override("sso-admin", "Detach"+p.noun+"FromPermissionSet", func(c *Call) (map[string]any, error) {
			ps, err := psOf(c)
			if err != nil {
				return nil, err
			}
			l := asList(ps.Data[p.field])
			i := slices.IndexFunc(l, func(e any) bool { return same(c.In[p.key], e) })
			if i < 0 {
				return nil, c.NotFound("%s is not attached", p.noun)
			}
			ps.Data[p.field] = slices.Delete(l, i, i+1)
			c.Put(ps)
			ssoResync(c, ps)
			return map[string]any{}, nil
		})
		Override("sso-admin", p.lop, func(c *Call) (map[string]any, error) {
			ps, err := psOf(c)
			if err != nil {
				return nil, err
			}
			l := asList(ps.Data[p.field])
			if l == nil {
				l = []any{}
			}
			return map[string]any{p.list: l}, nil
		})
	}
	// Inline policy changes reach the roles too.
	for _, op := range []string{"PutInlinePolicyToPermissionSet", "DeleteInlinePolicyFromPermissionSet"} {
		Hook("sso-admin", op, func(c *Call) {
			if ps, err := psOf(c); err == nil {
				ssoResync(c, ps)
			}
		})
	}
}
