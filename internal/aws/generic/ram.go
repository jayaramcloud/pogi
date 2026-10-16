package generic

import (
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Resource Access Manager. A share keeps its associations (resources and
// principals) as records of their own. A principal in the owner's organization
// (an organization or OU ARN, or an account Organizations knows) is associated
// at once; any other account gets an invitation, which Accept turns into an
// association. Sharing is only recorded: nothing enforces it.

// ramPermissions are the managed permissions every account has.
var ramPermissions = []struct{ name, typ string }{
	{"AWSRAMDefaultPermissionSubnet", "ec2:Subnet"},
	{"AWSRAMDefaultPermissionTransitGateway", "ec2:TransitGateway"},
	{"AWSRAMDefaultPermissionResolverRule", "route53resolver:ResolverRule"},
	{"AWSRAMDefaultPermissionLicenseConfiguration", "license-manager:LicenseConfiguration"},
	{"AWSRAMDefaultPermissionPrefixList", "ec2:PrefixList"},
}

func ramPermissionARN(c *Call, name string) string {
	return "arn:" + core.Partition(c.Region) + ":ram::aws:permission/" + name
}

func ramPermissionSummary(c *Call, name, typ string) map[string]any {
	return map[string]any{"arn": ramPermissionARN(c, name), "version": "1", "defaultVersion": true, "name": name, "resourceType": typ, "status": "ATTACHABLE",
		"creationTime": "2020-01-01T00:00:00Z", "lastUpdatedTime": "2020-01-01T00:00:00Z", "isResourceTypeDefault": true, "permissionType": "AWS_MANAGED", "featureSet": "STANDARD"}
}

// ramResourceType names the type of a resource from its ARN: arn:aws:ec2:r:a:subnet/x is ec2:Subnet.
func ramResourceType(arn string) string {
	p := strings.SplitN(arn, ":", 6)
	if len(p) < 6 {
		return ""
	}
	t := strings.FieldsFunc(p[5], func(r rune) bool { return r == '/' || r == ':' })[0]
	words := strings.Split(t, "-")
	for i, w := range words {
		words[i] = strings.ToUpper(w[:1]) + w[1:]
	}
	return p[2] + ":" + strings.Join(words, "")
}

// ramShareOut is a share as the API describes it.
func ramShareOut(r *Resource) map[string]any {
	out := map[string]any{"name": r.Data["name"], "resourceShareArn": r.ARN, "owningAccountId": r.Account, "status": r.Data["status"], "featureSet": "STANDARD",
		"allowExternalPrincipals": true, "creationTime": r.Created.Format(time.RFC3339Nano), "lastUpdatedTime": r.Updated.Format(time.RFC3339Nano)}
	if v, ok := r.Data["allowExternalPrincipals"]; ok {
		out["allowExternalPrincipals"] = v
	}
	if cfg, ok := r.Data["resourceShareConfiguration"]; ok {
		out["resourceShareConfiguration"] = cfg
	}
	var tags []any
	for _, k := range core.SortedKeys(r.Tags) {
		tags = append(tags, map[string]any{"key": k, "value": r.Tags[k]})
	}
	if tags != nil {
		out["tags"] = tags
	}
	return out
}

func ramAssocOut(a *Resource) map[string]any {
	return map[string]any{"resourceShareArn": a.Data["resourceShareArn"], "resourceShareName": a.Data["resourceShareName"], "associatedEntity": a.Data["associatedEntity"],
		"associationType": a.Data["associationType"], "status": a.Data["status"], "external": a.Data["external"], "creationTime": a.Created.Format(time.RFC3339Nano),
		"lastUpdatedTime": a.Updated.Format(time.RFC3339Nano)}
}

// ramShare finds a share by ARN in any account.
func (c *Call) ramShare(arn string) (*Resource, error) {
	for _, r := range c.Svc.all("ResourceShare") {
		if r.ARN == arn {
			return r, nil
		}
	}
	return nil, c.m.modelErr(c.op, eNotFound, "", "ResourceShare "+arn+" could not be found.")
}

// ramPrincipal says whether a principal is outside the owner's organization, and the account it names.
func (c *Call) ramPrincipal(p string) (external bool) {
	if strings.HasPrefix(p, "arn:") {
		_, _, acct, _, ok := core.ParseARN(p) // service, region, account, resource
		if strings.Contains(p, ":organizations::") || !ok {
			return false // an organization or OU: its members get the share without invitations
		}
		p = acct
	}
	if p == c.Account {
		return false
	}
	return c.Hub().Call(c.Account, c.Region, "AWSOrganizationsV20161128", "DescribeAccount", map[string]any{"AccountId": p}, nil) != nil
}

// ramAssociate adds associations to a share, with an invitation for each external account.
func (c *Call) ramAssociate(share *Resource, principals, resources []any) []any {
	var out []any
	add := func(kind, entity string, external bool) {
		key := share.ARN + "|" + kind + "|" + entity
		if old := c.Svc.find("Association", key); old != nil && old.Data["status"] != "DISASSOCIATED" {
			out = append(out, ramAssocOut(old))
			return
		}
		status := "ASSOCIATED"
		if external {
			status = "ASSOCIATING"
		}
		now := c.now()
		a := &Resource{Type: "Association", Key: key, Name: entity, Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: map[string]any{
			"resourceShareArn": share.ARN, "resourceShareName": share.Data["name"], "associatedEntity": entity, "associationType": kind, "status": status, "external": external}}
		c.Put(a)
		out = append(out, ramAssocOut(a))
		if external {
			c.ramInvite(share, entity)
		}
	}
	for _, p := range principals {
		add("PRINCIPAL", str(p), c.ramPrincipal(str(p)))
	}
	for _, r := range resources {
		add("RESOURCE", str(r), false)
	}
	return out
}

// ramInvite makes the invitation of an external account, once per share.
func (c *Call) ramInvite(share *Resource, principal string) {
	receiver := principal
	if strings.HasPrefix(principal, "arn:") {
		_, _, receiver, _, _ = core.ParseARN(principal)
	}
	for _, i := range c.Svc.all("Invitation") {
		if i.Data["resourceShareArn"] == share.ARN && i.Data["receiverAccountId"] == receiver {
			return
		}
	}
	arn := core.ARN("ram", c.Region, c.Account, "resource-share-invitation/"+core.UUID())
	now := c.now()
	c.Put(&Resource{Type: "Invitation", Key: arn, ARN: arn, Name: arn, Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: map[string]any{
		"resourceShareArn": share.ARN, "resourceShareName": share.Data["name"], "senderAccountId": c.Account, "receiverAccountId": receiver, "status": "PENDING"}})
}

func ramInvitationOut(c *Call, i *Resource) map[string]any {
	var assocs []any
	for _, a := range c.Svc.all("Association") {
		if a.Data["resourceShareArn"] == i.Data["resourceShareArn"] {
			assocs = append(assocs, ramAssocOut(a))
		}
	}
	return map[string]any{"resourceShareInvitationArn": i.ARN, "resourceShareArn": i.Data["resourceShareArn"], "resourceShareName": i.Data["resourceShareName"],
		"senderAccountId": i.Data["senderAccountId"], "receiverAccountId": i.Data["receiverAccountId"], "status": i.Data["status"],
		"invitationTimestamp": i.Created.Format(time.RFC3339Nano), "resourceShareAssociations": assocs}
}

// ramVisible says whether a share is one the caller asked about: owned (SELF) or shared with it (OTHER-ACCOUNTS).
func (c *Call) ramVisible(share *Resource) bool {
	if str(c.In["resourceOwner"]) != "OTHER-ACCOUNTS" {
		return share.Account == c.Account
	}
	for _, a := range c.Svc.all("Association") {
		if a.Data["resourceShareArn"] == share.ARN && a.Data["associationType"] == "PRINCIPAL" && a.Data["status"] == "ASSOCIATED" && a.Data["associatedEntity"] == c.Account {
			return share.Account != c.Account
		}
	}
	return false
}

func ramWantArns(c *Call, member string) func(string) bool {
	l := asList(c.In[member])
	return func(arn string) bool {
		return len(l) == 0 || slices.ContainsFunc(l, func(e any) bool { return str(e) == arn })
	}
}

func init() {
	Override("ram", "CreateResourceShare", func(c *Call) (map[string]any, error) {
		for _, s := range c.List("ResourceShare") {
			if s.Data["name"] == c.In["name"] && s.Data["status"] != "DELETED" {
				return nil, c.m.modelErr(c.op, eExists, "", "Resource share "+str(c.In["name"])+" already exists")
			}
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["status"] = "ACTIVE"
		delete(r.Data, "principals")
		delete(r.Data, "resourceArns")
		c.Put(r)
		c.ramAssociate(r, asList(c.In["principals"]), asList(c.In["resourceArns"]))
		return map[string]any{"resourceShare": ramShareOut(r), "clientToken": c.In["clientToken"]}, nil
	})
	Override("ram", "UpdateResourceShare", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		for _, a := range c.Svc.all("Association") { // associations carry the share's name
			if a.Data["resourceShareArn"] == r.ARN {
				a.Data["resourceShareName"] = r.Data["name"]
				c.Svc.putRes(a)
			}
		}
		return map[string]any{"resourceShare": ramShareOut(r), "clientToken": c.In["clientToken"]}, nil
	})
	Override("ram", "DeleteResourceShare", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		for _, typ := range []string{"Association", "Invitation"} {
			for _, a := range c.Svc.all(typ) {
				if a.Data["resourceShareArn"] == c.Res.ARN {
					c.Svc.dropRes(a)
				}
			}
		}
		return map[string]any{"returnValue": true, "clientToken": c.In["clientToken"]}, nil
	})
	Override("ram", "GetResourceShares", func(c *Call) (map[string]any, error) {
		want := ramWantArns(c, "resourceShareArns")
		out := []any{}
		for _, s := range c.Svc.all("ResourceShare") {
			if c.ramVisible(s) && want(s.ARN) && (c.In["name"] == nil || c.In["name"] == s.Data["name"]) && (c.In["resourceShareStatus"] == nil || c.In["resourceShareStatus"] == s.Data["status"]) {
				out = append(out, ramShareOut(s))
			}
		}
		return map[string]any{"resourceShares": out}, nil
	})
	Override("ram", "AssociateResourceShare", func(c *Call) (map[string]any, error) {
		share, err := c.ramShare(str(c.In["resourceShareArn"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"resourceShareAssociations": c.ramAssociate(share, asList(c.In["principals"]), asList(c.In["resourceArns"])), "clientToken": c.In["clientToken"]}, nil
	})
	Override("ram", "DisassociateResourceShare", func(c *Call) (map[string]any, error) {
		share, err := c.ramShare(str(c.In["resourceShareArn"]))
		if err != nil {
			return nil, err
		}
		out := []any{}
		for kind, entities := range map[string][]any{"PRINCIPAL": asList(c.In["principals"]), "RESOURCE": asList(c.In["resourceArns"])} {
			for _, e := range entities {
				a := c.Svc.find("Association", share.ARN+"|"+kind+"|"+str(e))
				if a == nil || a.Data["status"] == "DISASSOCIATED" {
					return nil, c.m.modelErr(c.op, eNotFound, "", str(e)+" is not associated with the resource share")
				}
				c.Svc.dropRes(a) // ponytail: associations vanish instead of lingering as DISASSOCIATED
				a.Data["status"] = "DISASSOCIATED"
				out = append(out, ramAssocOut(a))
			}
		}
		return map[string]any{"resourceShareAssociations": out, "clientToken": c.In["clientToken"]}, nil
	})
	Override("ram", "GetResourceShareAssociations", func(c *Call) (map[string]any, error) {
		want := ramWantArns(c, "resourceShareArns")
		out := []any{}
		for _, a := range c.Svc.all("Association") {
			share, _ := c.ramShare(str(a.Data["resourceShareArn"]))
			if share == nil || !c.ramVisible(share) || a.Data["associationType"] != c.In["associationType"] || !want(str(a.Data["resourceShareArn"])) ||
				c.In["associationStatus"] != nil && c.In["associationStatus"] != a.Data["status"] ||
				c.In["principal"] != nil && c.In["principal"] != a.Data["associatedEntity"] || c.In["resourceArn"] != nil && c.In["resourceArn"] != a.Data["associatedEntity"] {
				continue
			}
			out = append(out, ramAssocOut(a))
		}
		return map[string]any{"resourceShareAssociations": out}, nil
	})
	Override("ram", "ListPrincipals", func(c *Call) (map[string]any, error) {
		want, wantP := ramWantArns(c, "resourceShareArns"), ramWantArns(c, "principals")
		out := []any{}
		for _, a := range c.Svc.all("Association") {
			share, _ := c.ramShare(str(a.Data["resourceShareArn"]))
			if share != nil && c.ramVisible(share) && a.Data["associationType"] == "PRINCIPAL" && want(share.ARN) && wantP(str(a.Data["associatedEntity"])) {
				out = append(out, map[string]any{"id": a.Data["associatedEntity"], "resourceShareArn": share.ARN, "external": a.Data["external"],
					"creationTime": a.Created.Format(time.RFC3339Nano), "lastUpdatedTime": a.Updated.Format(time.RFC3339Nano)})
			}
		}
		return map[string]any{"principals": out}, nil
	})
	Override("ram", "ListResources", func(c *Call) (map[string]any, error) {
		want, wantR := ramWantArns(c, "resourceShareArns"), ramWantArns(c, "resourceArns")
		out := []any{}
		for _, a := range c.Svc.all("Association") {
			share, _ := c.ramShare(str(a.Data["resourceShareArn"]))
			arn := str(a.Data["associatedEntity"])
			if share != nil && c.ramVisible(share) && a.Data["associationType"] == "RESOURCE" && want(share.ARN) && wantR(arn) && (c.In["resourceType"] == nil || c.In["resourceType"] == ramResourceType(arn)) {
				out = append(out, map[string]any{"arn": arn, "type": ramResourceType(arn), "resourceShareArn": share.ARN, "status": "AVAILABLE", "resourceRegionScope": "REGIONAL",
					"creationTime": a.Created.Format(time.RFC3339Nano), "lastUpdatedTime": a.Updated.Format(time.RFC3339Nano)})
			}
		}
		return map[string]any{"resources": out}, nil
	})

	Override("ram", "GetResourceShareInvitations", func(c *Call) (map[string]any, error) {
		want, wantI := ramWantArns(c, "resourceShareArns"), ramWantArns(c, "resourceShareInvitationArns")
		out := []any{}
		for _, i := range c.Svc.all("Invitation") {
			if (i.Data["receiverAccountId"] == c.Account || i.Data["senderAccountId"] == c.Account) && want(str(i.Data["resourceShareArn"])) && wantI(i.ARN) {
				out = append(out, ramInvitationOut(c, i))
			}
		}
		if len(asList(c.In["resourceShareInvitationArns"])) > 0 && len(out) == 0 {
			return nil, c.m.modelErr(c.op, eNotFound, "ResourceShareInvitationArn", "The invitation was not found")
		}
		return map[string]any{"resourceShareInvitations": out}, nil
	})
	for op, to := range map[string]string{"AcceptResourceShareInvitation": "ACCEPTED", "RejectResourceShareInvitation": "REJECTED"} {
		Override("ram", op, func(c *Call) (map[string]any, error) {
			i := c.Svc.find("Invitation", str(c.In["resourceShareInvitationArn"]))
			if i == nil {
				return nil, c.m.modelErr(c.op, eNotFound, "ResourceShareInvitationArn", "The invitation "+str(c.In["resourceShareInvitationArn"])+" was not found")
			}
			switch i.Data["status"] {
			case "ACCEPTED":
				return nil, core.Errorf(400, "ResourceShareInvitationAlreadyAcceptedException", "The invitation was already accepted")
			case "REJECTED":
				return nil, core.Errorf(400, "ResourceShareInvitationAlreadyRejectedException", "The invitation was already rejected")
			}
			i.Data["status"], i.Updated = to, c.now()
			c.Svc.putRes(i)
			for _, a := range c.Svc.all("Association") {
				if a.Data["resourceShareArn"] == i.Data["resourceShareArn"] && a.Data["status"] == "ASSOCIATING" {
					a.Data["status"], a.Updated = map[string]string{"ACCEPTED": "ASSOCIATED", "REJECTED": "FAILED"}[to], c.now()
					c.Svc.putRes(a)
				}
			}
			return map[string]any{"resourceShareInvitation": ramInvitationOut(c, i), "clientToken": c.In["clientToken"]}, nil
		})
	}
	Override("ram", "ListPendingInvitationResources", func(c *Call) (map[string]any, error) {
		i := c.Svc.find("Invitation", str(c.In["resourceShareInvitationArn"]))
		if i == nil {
			return nil, c.m.modelErr(c.op, eNotFound, "ResourceShareInvitationArn", "The invitation was not found")
		}
		out := []any{}
		for _, a := range c.Svc.all("Association") {
			if a.Data["resourceShareArn"] == i.Data["resourceShareArn"] && a.Data["associationType"] == "RESOURCE" {
				arn := str(a.Data["associatedEntity"])
				out = append(out, map[string]any{"arn": arn, "type": ramResourceType(arn), "resourceShareArn": i.Data["resourceShareArn"], "status": "PENDING"})
			}
		}
		return map[string]any{"resources": out}, nil
	})

	// Permissions: the managed ones are fixed, customer-managed ones are the engine's.
	Override("ram", "ListPermissions", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, p := range ramPermissions {
			if c.In["resourceType"] == nil || strings.EqualFold(str(c.In["resourceType"]), p.typ) {
				out = append(out, ramPermissionSummary(c, p.name, p.typ))
			}
		}
		d, err := c.Default()
		if err != nil {
			return nil, err
		}
		return map[string]any{"permissions": append(out, asList(d["permissions"])...)}, nil
	})
	Override("ram", "GetPermission", func(c *Call) (map[string]any, error) {
		for _, p := range ramPermissions {
			if ramPermissionARN(c, p.name) == str(c.In["permissionArn"]) {
				d := ramPermissionSummary(c, p.name, p.typ)
				d["permission"] = `{"Effect":"Allow","Action":["` + strings.ToLower(p.typ[:strings.Index(p.typ, ":")]) + `:*"]}`
				return map[string]any{"permission": d}, nil
			}
		}
		return c.Default()
	})
	Override("ram", "ListResourceSharePermissions", func(c *Call) (map[string]any, error) {
		share, err := c.ramShare(str(c.In["resourceShareArn"]))
		if err != nil {
			return nil, err
		}
		out := []any{}
		for _, p := range ramPermissions {
			if slices.ContainsFunc(asList(share.Data["permissionArns"]), func(a any) bool { return str(a) == ramPermissionARN(c, p.name) }) {
				out = append(out, ramPermissionSummary(c, p.name, p.typ))
			}
		}
		for _, a := range c.Svc.all("Association") { // no permission named: the default of each resource type shared
			if len(out) == 0 && a.Data["resourceShareArn"] == share.ARN && a.Data["associationType"] == "RESOURCE" {
				for _, p := range ramPermissions {
					if p.typ == ramResourceType(str(a.Data["associatedEntity"])) {
						out = append(out, ramPermissionSummary(c, p.name, p.typ))
					}
				}
			}
		}
		return map[string]any{"permissions": out}, nil
	})
	Override("ram", "ListResourceTypes", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, p := range ramPermissions {
			out = append(out, map[string]any{"serviceName": p.typ[:strings.Index(p.typ, ":")], "resourceType": p.typ[strings.Index(p.typ, ":")+1:], "resourceRegionScope": "REGIONAL"})
		}
		return map[string]any{"resourceTypes": out}, nil
	})
	Override("ram", "EnableSharingWithAwsOrganization", func(c *Call) (map[string]any, error) {
		return map[string]any{"returnValue": true}, nil
	})
}
