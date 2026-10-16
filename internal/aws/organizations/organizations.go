// Package organizations emulates AWS Organizations (JSON 1.1): one
// organization per management account, with a root, OUs, member accounts,
// policies and their attachments, invitations, delegated administrators and
// trusted service access. CreateAccount mints a fresh account whose
// OrganizationAccountAccessRole exists in IAM at once, so the management
// account can assume it right away. IAM enforcement reads the service
// control policies through SCPs.
package organizations

import (
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Organizations emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Orgs map[string]*org `json:"orgs"` // by management account
}

// org is one organization. Targets (the root, OUs and accounts) are known by
// id; Parents maps every OU and account to its parent.
type org struct {
	Organization
	Root       Root                            `json:"root"`
	OUs        map[string]*OU                  `json:"ous"`
	Accounts   map[string]*Account             `json:"accounts"` // the management account too
	Parents    map[string]string               `json:"parents"`
	Policies   map[string]*Policy              `json:"policies"`
	Attached   map[string][]string             `json:"attached"` // target → policy ids
	Statuses   map[string]*CreateAccountStatus `json:"statuses"`
	Handshakes map[string]*Handshake           `json:"handshakes"`
	Delegated  map[string]map[string]core.Time `json:"delegated"` // account → service principal → since
	Services   map[string]core.Time            `json:"services"`  // trusted service principals → since
	Tags       map[string]map[string]string    `json:"tags"`      // target or policy id → tags
}

// The types below use the AWS member names: they are the stored state and
// the wire shapes.

// Organization is the DescribeOrganization shape.
type Organization struct {
	Id, Arn, FeatureSet                                   string
	MasterAccountArn, MasterAccountId, MasterAccountEmail string
}

// PolicyTypeSummary is a policy type's status in a root.
type PolicyTypeSummary struct{ Type, Status string }

// Root is the organization's root.
type Root struct {
	Id, Arn, Name string
	PolicyTypes   []PolicyTypeSummary
}

// OU is an organizational unit.
type OU struct{ Id, Arn, Name string }

// Account is a member account.
type Account struct {
	Id, Arn, Email, Name, Status, State, JoinedMethod string
	JoinedTimestamp                                   core.Time
}

// Policy is an organization policy.
type Policy struct {
	PolicySummary PolicySummary
	Content       string
}

// PolicySummary describes a policy without its content.
type PolicySummary struct {
	Id, Arn, Name, Description, Type string
	AwsManaged                       bool
}

// CreateAccountStatus tracks a CreateAccount request.
type CreateAccountStatus struct {
	Id, AccountName, State   string
	AccountId, FailureReason string `json:",omitempty"`
	RequestedTimestamp       core.Time
	CompletedTimestamp       core.Time `json:",omitzero"`
}

// Handshake is an invitation; pogi keeps it as metadata.
type Handshake struct {
	Id, Arn, Action, State                  string
	Parties                                 []Party
	Resources                               []HandshakeResource
	RequestedTimestamp, ExpirationTimestamp core.Time
}

// Party is a handshake participant (also InviteAccountToOrganization's Target).
type Party struct{ Id, Type string }

// HandshakeResource is a detail of a handshake.
type HandshakeResource struct{ Type, Value string }

// Tag is an organization tag.
type Tag struct{ Key, Value string }

const (
	scp        = "SERVICE_CONTROL_POLICY"
	fullAccess = "p-FullAWSAccess"
	iamTarget  = "AWSIdentityManagementV20100508"
	lowerNum   = "abcdefghijklmnopqrstuvwxyz0123456789"
)

// policyTypes are the policy types pogi supports.
var policyTypes = []string{scp, "RESOURCE_CONTROL_POLICY", "TAG_POLICY", "BACKUP_POLICY", "AISERVICES_OPT_OUT_POLICY"}

// New creates the Organizations service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateOrganization":                  core.H(s.createOrganization),
		"DescribeOrganization":                core.H(s.describeOrganization),
		"DeleteOrganization":                  core.H(s.deleteOrganization),
		"ListRoots":                           core.H(s.listRoots),
		"CreateOrganizationalUnit":            core.H(s.createOU),
		"DescribeOrganizationalUnit":          core.H(s.ou),
		"UpdateOrganizationalUnit":            core.H(s.ou),
		"DeleteOrganizationalUnit":            core.H(s.ou),
		"ListOrganizationalUnitsForParent":    core.H(s.listOUs),
		"ListChildren":                        core.H(s.listChildren),
		"ListParents":                         core.H(s.listParents),
		"CreateAccount":                       core.H(s.createAccount),
		"DescribeCreateAccountStatus":         core.H(s.describeCreateAccountStatus),
		"ListCreateAccountStatus":             core.H(s.listCreateAccountStatus),
		"DescribeAccount":                     core.H(s.describeAccount),
		"ListAccounts":                        core.H(s.listAccounts),
		"ListAccountsForParent":               core.H(s.listAccounts),
		"MoveAccount":                         core.H(s.moveAccount),
		"RemoveAccountFromOrganization":       core.H(s.removeAccount),
		"LeaveOrganization":                   core.H(s.removeAccount),
		"CloseAccount":                        core.H(s.closeAccount),
		"InviteAccountToOrganization":         core.H(s.invite),
		"AcceptHandshake":                     core.H(s.handshake),
		"DeclineHandshake":                    core.H(s.handshake),
		"CancelHandshake":                     core.H(s.handshake),
		"DescribeHandshake":                   core.H(s.handshake),
		"ListHandshakesForAccount":            core.H(s.listHandshakes),
		"ListHandshakesForOrganization":       core.H(s.listHandshakes),
		"CreatePolicy":                        core.H(s.createPolicy),
		"DescribePolicy":                      core.H(s.policy),
		"UpdatePolicy":                        core.H(s.policy),
		"DeletePolicy":                        core.H(s.policy),
		"ListPolicies":                        core.H(s.listPolicies),
		"AttachPolicy":                        core.H(s.attachment),
		"DetachPolicy":                        core.H(s.attachment),
		"ListPoliciesForTarget":               core.H(s.listPolicies),
		"ListTargetsForPolicy":                core.H(s.listTargets),
		"EnablePolicyType":                    core.H(s.policyType),
		"DisablePolicyType":                   core.H(s.policyType),
		"DescribeEffectivePolicy":             core.H(s.effectivePolicy),
		"RegisterDelegatedAdministrator":      core.H(s.delegate),
		"DeregisterDelegatedAdministrator":    core.H(s.delegate),
		"ListDelegatedAdministrators":         core.H(s.listDelegatedAdministrators),
		"ListDelegatedServicesForAccount":     core.H(s.listDelegatedServices),
		"EnableAWSServiceAccess":              core.H(s.serviceAccess),
		"DisableAWSServiceAccess":             core.H(s.serviceAccess),
		"ListAWSServiceAccessForOrganization": core.H(s.serviceAccess),
		"TagResource":                         core.H(s.tags),
		"UntagResource":                       core.H(s.tags),
		"ListTagsForResource":                 core.H(s.tags),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "organizations", Cloud: core.AWS, Title: "Organizations", Targets: []string{"AWSOrganizationsV20161128"},
		Signing: []string{"organizations"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Orgs: map[string]*org{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Orgs: map[string]*org{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// SCPs returns the service control policies that govern a member account,
// one list of documents per level from the root down to the account, for
// IAM's evaluator. It is nil for management accounts, accounts outside any
// organization and organizations whose root has SCPs disabled.
// ponytail: IAM never checks root principals, so SCPs do not restrict a
// member account's root user as they do on AWS.
func (s *Service) SCPs(account string) [][]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	o := s.orgOf(account)
	if o == nil || o.MasterAccountId == account || !o.enabled(scp) {
		return nil
	}
	var levels [][]string
	for _, target := range o.path(account) {
		docs := []string{}
		for _, id := range o.Attached[target] {
			if p := o.Policies[id]; p != nil && p.PolicySummary.Type == scp {
				docs = append(docs, p.Content)
			}
		}
		levels = append(levels, docs)
	}
	return levels
}

func errf(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }

func invalid(msg string) error { return errf("InvalidInputException", "%s", msg) }

func now() core.Time { return core.Time(time.Now().UTC().Truncate(time.Second)) }

func newID(prefix string, n int) string { return prefix + core.RandString(n, lowerNum) }

// orgOf finds the organization an account belongs to; lock held.
func (s *Service) orgOf(account string) *org {
	for _, o := range s.st.Orgs {
		if o.Accounts[account] != nil {
			return o
		}
	}
	return nil
}

// orgFor returns the caller's organization; manage requires the caller to be
// its management account. Lock held.
func (s *Service) orgFor(c *core.Ctx, manage bool) (*org, error) {
	o := s.orgOf(c.Account)
	switch {
	case o == nil:
		return nil, errf("AWSOrganizationsNotInUseException", "Your account is not a member of an organization.")
	case manage && o.MasterAccountId != c.Account:
		return nil, core.Errorf(403, "AccessDeniedException", "You don't have permissions to access this resource.")
	}
	return o, nil
}

// locked runs fn on the caller's organization under the lock.
func (s *Service) locked(c *core.Ctx, manage bool, fn func(o *org) (any, error)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, err := s.orgFor(c, manage)
	if err != nil {
		return nil, err
	}
	return fn(o)
}

func (o *org) arn(c *core.Ctx, kind, id string) string {
	return core.GlobalARN("organizations", c.Region, o.MasterAccountId, kind+"/"+o.Id+"/"+id)
}

// kind is the target type of an id: ROOT, ORGANIZATIONAL_UNIT, ACCOUNT or "".
func (o *org) kind(id string) string {
	switch {
	case id == o.Root.Id:
		return "ROOT"
	case o.OUs[id] != nil:
		return "ORGANIZATIONAL_UNIT"
	case o.Accounts[id] != nil:
		return "ACCOUNT"
	}
	return ""
}

func (o *org) parent(id string) error {
	if k := o.kind(id); k != "ROOT" && k != "ORGANIZATIONAL_UNIT" {
		return errf("ParentNotFoundException", "We can't find a root or OU with the ParentId that you specified.")
	}
	return nil
}

func (o *org) account(id string) (*Account, error) {
	if a := o.Accounts[id]; a != nil {
		return a, nil
	}
	return nil, errf("AccountNotFoundException", "You specified an account that doesn't exist.")
}

// children lists the ids of a parent's children of one kind, in order.
func (o *org) children(parent, kind string) []string {
	var out []string
	for _, id := range core.SortedKeys(o.Parents) {
		if o.Parents[id] == parent && o.kind(id) == kind {
			out = append(out, id)
		}
	}
	return out
}

// path lists the targets from the root down to id.
func (o *org) path(id string) []string {
	p := []string{id}
	for id = o.Parents[id]; id != ""; id = o.Parents[id] {
		p = append(p, id)
	}
	slices.Reverse(p)
	return p
}

func (o *org) enabled(typ string) bool {
	return slices.Contains(o.Root.PolicyTypes, PolicyTypeSummary{typ, "ENABLED"})
}

// join adds a target under parent with the FullAWSAccess SCP; lock held.
func (o *org) join(id, parent string, tags []Tag) {
	o.Parents[id] = parent
	o.Attached[id] = []string{fullAccess}
	o.tag(id, tags)
}

// leave forgets a target; lock held.
func (o *org) leave(id string) {
	delete(o.Accounts, id)
	delete(o.OUs, id)
	delete(o.Parents, id)
	delete(o.Attached, id)
	delete(o.Tags, id)
	delete(o.Delegated, id)
}

func (o *org) tag(id string, tags []Tag) {
	for _, t := range tags {
		if o.Tags[id] == nil {
			o.Tags[id] = map[string]string{}
		}
		o.Tags[id][t.Key] = t.Value
	}
}

func email(account string) string { return account + "@example.com" }

// ---- organization ----

func (s *Service) createOrganization(c *core.Ctx, in *struct{ FeatureSet string }) (any, error) {
	fs := cmp.Or(in.FeatureSet, "ALL")
	if fs != "ALL" && fs != "CONSOLIDATED_BILLING" {
		return nil, invalid("FeatureSet must be ALL or CONSOLIDATED_BILLING.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.orgOf(c.Account) != nil {
		return nil, errf("AlreadyInOrganizationException", "The provided account is already a member of an organization.")
	}
	o := &org{OUs: map[string]*OU{}, Accounts: map[string]*Account{}, Parents: map[string]string{}, Policies: map[string]*Policy{},
		Attached: map[string][]string{}, Statuses: map[string]*CreateAccountStatus{}, Handshakes: map[string]*Handshake{},
		Delegated: map[string]map[string]core.Time{}, Services: map[string]core.Time{}, Tags: map[string]map[string]string{}}
	o.Organization = Organization{Id: newID("o-", 10), FeatureSet: fs, MasterAccountId: c.Account, MasterAccountEmail: email(c.Account)}
	o.Arn = c.GlobalARN("organizations", "organization/"+o.Id)
	o.MasterAccountArn = o.arn(c, "account", c.Account)
	o.Root = Root{Id: newID("r-", 4), Name: "Root", PolicyTypes: []PolicyTypeSummary{}}
	o.Root.Arn = o.arn(c, "root", o.Root.Id)
	o.Policies[fullAccess] = &Policy{PolicySummary{Id: fullAccess, Arn: core.GlobalARN("organizations", c.Region, "aws", "policy/service_control_policy/"+fullAccess),
		Name: "FullAWSAccess", Description: "Allows access to every operation", Type: scp, AwsManaged: true},
		`{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"*","Resource":"*"}]}`}
	o.Attached[o.Root.Id] = []string{fullAccess}
	o.Accounts[c.Account] = &Account{Id: c.Account, Arn: o.MasterAccountArn, Email: o.MasterAccountEmail, Name: c.Account,
		Status: "ACTIVE", State: "ACTIVE", JoinedMethod: "CREATED", JoinedTimestamp: now()}
	o.join(c.Account, o.Root.Id, nil)
	s.st.Orgs[c.Account] = o
	return map[string]any{"Organization": o.Organization}, nil
}

func (s *Service) describeOrganization(c *core.Ctx, _ *struct{}) (any, error) {
	return s.locked(c, false, func(o *org) (any, error) { return map[string]any{"Organization": o.Organization}, nil })
}

func (s *Service) deleteOrganization(c *core.Ctx, _ *struct{}) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if len(o.Accounts) > 1 {
			return nil, errf("OrganizationNotEmptyException", "To delete an organization you must first remove all member accounts (except the management account).")
		}
		delete(s.st.Orgs, c.Account)
		return nil, nil
	})
}

func (s *Service) listRoots(c *core.Ctx, _ *struct{}) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) { return map[string]any{"Roots": []Root{o.Root}}, nil })
}

// ---- organizational units ----

func (s *Service) createOU(c *core.Ctx, in *struct {
	ParentId, Name string
	Tags           []Tag
}) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if err := o.parent(in.ParentId); err != nil {
			return nil, err
		}
		if err := o.uniqueOU(in.ParentId, in.Name); err != nil {
			return nil, err
		}
		ou := &OU{Id: "ou-" + o.Root.Id[2:] + "-" + core.RandString(8, lowerNum), Name: in.Name}
		ou.Arn = o.arn(c, "ou", ou.Id)
		o.OUs[ou.Id] = ou
		o.join(ou.Id, in.ParentId, in.Tags)
		return map[string]any{"OrganizationalUnit": ou}, nil
	})
}

func (o *org) uniqueOU(parent, name string) error {
	if name == "" || len(name) > 128 {
		return invalid("The OU name must be 1 to 128 characters long.")
	}
	for _, id := range o.children(parent, "ORGANIZATIONAL_UNIT") {
		if o.OUs[id].Name == name {
			return errf("DuplicateOrganizationalUnitException", "An OU with the same name already exists.")
		}
	}
	return nil
}

// ou serves DescribeOrganizationalUnit, UpdateOrganizationalUnit and DeleteOrganizationalUnit.
func (s *Service) ou(c *core.Ctx, in *struct{ OrganizationalUnitId, Name string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		ou := o.OUs[in.OrganizationalUnitId]
		if ou == nil {
			return nil, errf("OrganizationalUnitNotFoundException", "We can't find an OU with the OrganizationalUnitId that you specified.")
		}
		switch c.Op {
		case "UpdateOrganizationalUnit":
			if in.Name != "" && in.Name != ou.Name {
				if err := o.uniqueOU(o.Parents[ou.Id], in.Name); err != nil {
					return nil, err
				}
				ou.Name = in.Name
			}
		case "DeleteOrganizationalUnit":
			if len(o.children(ou.Id, "ACCOUNT"))+len(o.children(ou.Id, "ORGANIZATIONAL_UNIT")) > 0 {
				return nil, errf("OrganizationalUnitNotEmptyException", "The specified OU is not empty. Move all accounts to another root or to other OUs, remove all child OUs, and try the operation again.")
			}
			o.leave(ou.Id)
			return nil, nil
		}
		return map[string]any{"OrganizationalUnit": ou}, nil
	})
}

func (s *Service) listOUs(c *core.Ctx, in *struct{ ParentId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if err := o.parent(in.ParentId); err != nil {
			return nil, err
		}
		out := []*OU{}
		for _, id := range o.children(in.ParentId, "ORGANIZATIONAL_UNIT") {
			out = append(out, o.OUs[id])
		}
		return map[string]any{"OrganizationalUnits": out}, nil
	})
}

func (s *Service) listChildren(c *core.Ctx, in *struct{ ParentId, ChildType string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if err := o.parent(in.ParentId); err != nil {
			return nil, err
		}
		if in.ChildType != "ACCOUNT" && in.ChildType != "ORGANIZATIONAL_UNIT" {
			return nil, invalid("ChildType must be ACCOUNT or ORGANIZATIONAL_UNIT.")
		}
		out := []Party{}
		for _, id := range o.children(in.ParentId, in.ChildType) {
			out = append(out, Party{id, in.ChildType})
		}
		return map[string]any{"Children": out}, nil
	})
}

func (s *Service) listParents(c *core.Ctx, in *struct{ ChildId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if k := o.kind(in.ChildId); k != "ACCOUNT" && k != "ORGANIZATIONAL_UNIT" {
			return nil, errf("ChildNotFoundException", "We can't find an organizational unit (OU) or AWS account with the ChildId that you specified.")
		}
		p := o.Parents[in.ChildId]
		return map[string]any{"Parents": []Party{{p, o.kind(p)}}}, nil
	})
}

// ---- accounts ----

func (s *Service) createAccount(c *core.Ctx, in *struct {
	Email, AccountName, RoleName string
	Tags                         []Tag
}) (any, error) {
	if in.Email == "" || in.AccountName == "" {
		return nil, invalid("Email and AccountName are required.")
	}
	s.mu.Lock()
	o, err := s.orgFor(c, true)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	id := core.RandString(12, "0123456789")
	for id == core.DefaultAccount || s.orgOf(id) != nil {
		id = core.RandString(12, "0123456789")
	}
	st := &CreateAccountStatus{Id: newID("car-", 32), AccountName: in.AccountName, State: "IN_PROGRESS", RequestedTimestamp: now()}
	o.Statuses[st.Id] = st
	o.Accounts[id] = &Account{Id: id, Arn: o.arn(c, "account", id), Email: in.Email, Name: in.AccountName,
		Status: "ACTIVE", State: "ACTIVE", JoinedMethod: "CREATED", JoinedTimestamp: now()}
	o.join(id, o.Root.Id, in.Tags)
	out, mgmt := *st, o.MasterAccountId
	s.mu.Unlock()

	// The account is usable at once: its admin role trusts the management account.
	// ponytail: without the IAM service the account is created with no role.
	role := cmp.Or(in.RoleName, "OrganizationAccountAccessRole")
	trust := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"` +
		core.GlobalARN("iam", c.Region, mgmt, "root") + `"},"Action":"sts:AssumeRole"}]}`
	if s.hub.Call(id, c.Region, iamTarget, "CreateRole", map[string]string{"RoleName": role, "AssumeRolePolicyDocument": trust}, nil) == nil {
		s.hub.Call(id, c.Region, iamTarget, "AttachRolePolicy", map[string]string{"RoleName": role,
			"PolicyArn": core.GlobalARN("iam", c.Region, "aws", "policy/AdministratorAccess")}, nil)
	}
	s.mu.Lock()
	st.State, st.AccountId, st.CompletedTimestamp = "SUCCEEDED", id, now()
	s.mu.Unlock()
	return map[string]any{"CreateAccountStatus": out}, nil
}

func (s *Service) describeCreateAccountStatus(c *core.Ctx, in *struct{ CreateAccountRequestId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if st := o.Statuses[in.CreateAccountRequestId]; st != nil {
			return map[string]any{"CreateAccountStatus": st}, nil
		}
		return nil, errf("CreateAccountStatusNotFoundException", "We can't find a create account request with the CreateAccountRequestId that you specified.")
	})
}

func (s *Service) listCreateAccountStatus(c *core.Ctx, in *struct{ States []string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		out := []*CreateAccountStatus{}
		for _, id := range core.SortedKeys(o.Statuses) {
			if st := o.Statuses[id]; len(in.States) == 0 || slices.Contains(in.States, st.State) {
				out = append(out, st)
			}
		}
		return map[string]any{"CreateAccountStatuses": out}, nil
	})
}

func (s *Service) describeAccount(c *core.Ctx, in *struct{ AccountId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		a, err := o.account(in.AccountId)
		if err != nil {
			return nil, err
		}
		return map[string]any{"Account": a}, nil
	})
}

// listAccounts serves ListAccounts and ListAccountsForParent.
// ponytail: every List operation returns one page.
func (s *Service) listAccounts(c *core.Ctx, in *struct{ ParentId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		ids := core.SortedKeys(o.Accounts)
		if c.Op == "ListAccountsForParent" {
			if err := o.parent(in.ParentId); err != nil {
				return nil, err
			}
			ids = o.children(in.ParentId, "ACCOUNT")
		}
		out := []*Account{}
		for _, id := range ids {
			out = append(out, o.Accounts[id])
		}
		return map[string]any{"Accounts": out}, nil
	})
}

func (s *Service) moveAccount(c *core.Ctx, in *struct{ AccountId, SourceParentId, DestinationParentId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if _, err := o.account(in.AccountId); err != nil {
			return nil, err
		}
		switch cur := o.Parents[in.AccountId]; {
		case o.parent(in.SourceParentId) != nil || cur != in.SourceParentId:
			return nil, errf("SourceParentNotFoundException", "We can't find a source root or OU with the ParentId that you specified.")
		case o.parent(in.DestinationParentId) != nil:
			return nil, errf("DestinationParentNotFoundException", "We can't find the destination container (a root or OU) with the ParentId that you specified.")
		case cur == in.DestinationParentId:
			return nil, errf("DuplicateAccountException", "That account is already present in the specified destination.")
		}
		o.Parents[in.AccountId] = in.DestinationParentId
		return nil, nil
	})
}

// removeAccount serves RemoveAccountFromOrganization and LeaveOrganization.
func (s *Service) removeAccount(c *core.Ctx, in *struct{ AccountId string }) (any, error) {
	manage := c.Op == "RemoveAccountFromOrganization"
	if !manage {
		in.AccountId = c.Account
	}
	return s.locked(c, manage, func(o *org) (any, error) {
		if _, err := o.account(in.AccountId); err != nil {
			return nil, err
		}
		if in.AccountId == o.MasterAccountId {
			return nil, errf("MasterCannotLeaveOrganizationException", "You can't remove a management account from an organization. If you want the management account to become a member account in another organization, you must first delete the current organization of the management account.")
		}
		o.leave(in.AccountId)
		return nil, nil
	})
}

func (s *Service) closeAccount(c *core.Ctx, in *struct{ AccountId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		a, err := o.account(in.AccountId)
		switch {
		case err != nil:
			return nil, err
		case a.Id == o.MasterAccountId:
			return nil, errf("ConstraintViolationException", "You can't close the management account of an organization with CloseAccount.")
		case a.Status == "SUSPENDED":
			return nil, errf("AccountAlreadyClosedException", "You attempted to close an account that is already closed.")
		}
		a.Status, a.State = "SUSPENDED", "SUSPENDED"
		return nil, nil
	})
}

// ---- invitations ----

func (s *Service) invite(c *core.Ctx, in *struct {
	Target *Party
	Notes  string
	Tags   []Tag
}) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		switch {
		case in.Target == nil || in.Target.Id == "":
			return nil, invalid("Target is required.")
		case in.Target.Type == "ACCOUNT" && s.orgOf(in.Target.Id) != nil:
			return nil, errf("HandshakeConstraintViolationException", "The account is already a member of an organization.")
		}
		t := time.Time(now())
		h := &Handshake{Id: newID("h-", 10), Action: "INVITE", State: "OPEN", Parties: []Party{{o.Id, "ORGANIZATION"}, *in.Target},
			Resources:          []HandshakeResource{{"ORGANIZATION", o.Id}, {"NOTES", in.Notes}},
			RequestedTimestamp: core.Time(t), ExpirationTimestamp: core.Time(t.Add(15 * 24 * time.Hour))}
		h.Arn = o.arn(c, "handshake", "invite/"+h.Id)
		o.Handshakes[h.Id] = h
		return map[string]any{"Handshake": h}, nil
	})
}

// handshake serves AcceptHandshake, DeclineHandshake, CancelHandshake and
// DescribeHandshake: the invited account answers, the management account
// cancels, both may look.
func (s *Service) handshake(c *core.Ctx, in *struct{ HandshakeId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var o *org
	var h *Handshake
	for _, x := range s.st.Orgs {
		if h = x.Handshakes[in.HandshakeId]; h != nil {
			o = x
			break
		}
	}
	if h == nil {
		return nil, errf("HandshakeNotFoundException", "We can't find a handshake with the HandshakeId that you specified.")
	}
	invitee, owner := h.Parties[1].Id == c.Account, o.MasterAccountId == c.Account
	switch {
	case c.Op == "DescribeHandshake" && !invitee && !owner, c.Op == "CancelHandshake" && !owner,
		(c.Op == "AcceptHandshake" || c.Op == "DeclineHandshake") && !invitee:
		return nil, core.Errorf(403, "AccessDeniedException", "You don't have permissions to access this resource.")
	case c.Op != "DescribeHandshake" && h.State != "OPEN":
		return nil, errf("InvalidHandshakeTransitionException", "You can't perform the operation on the handshake in its current state.")
	}
	switch c.Op {
	case "AcceptHandshake":
		if s.orgOf(c.Account) != nil {
			return nil, errf("HandshakeConstraintViolationException", "The account is already a member of an organization.")
		}
		o.Accounts[c.Account] = &Account{Id: c.Account, Arn: o.arn(c, "account", c.Account), Email: email(c.Account), Name: c.Account,
			Status: "ACTIVE", State: "ACTIVE", JoinedMethod: "INVITED", JoinedTimestamp: now()}
		o.join(c.Account, o.Root.Id, nil)
		h.State = "ACCEPTED"
	case "DeclineHandshake":
		h.State = "DECLINED"
	case "CancelHandshake":
		h.State = "CANCELED"
	}
	return map[string]any{"Handshake": h}, nil
}

// listHandshakes serves ListHandshakesForAccount (invitations to the caller)
// and ListHandshakesForOrganization.
func (s *Service) listHandshakes(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []*Handshake{}
	if c.Op == "ListHandshakesForOrganization" {
		o, err := s.orgFor(c, true)
		if err != nil {
			return nil, err
		}
		for _, id := range core.SortedKeys(o.Handshakes) {
			out = append(out, o.Handshakes[id])
		}
		return map[string]any{"Handshakes": out}, nil
	}
	for _, o := range s.st.Orgs {
		for _, h := range o.Handshakes {
			if h.Parties[1].Id == c.Account {
				out = append(out, h)
			}
		}
	}
	slices.SortFunc(out, func(a, b *Handshake) int { return strings.Compare(a.Id, b.Id) })
	return map[string]any{"Handshakes": out}, nil
}

// ---- policies ----

func validType(t string) error {
	if !slices.Contains(policyTypes, t) {
		return invalid("You specified an invalid value for PolicyType.")
	}
	return nil
}

func checkContent(content string) error {
	if !json.Valid([]byte(content)) {
		return errf("MalformedPolicyDocumentException", "The provided policy document doesn't meet the requirements of the specified policy type.")
	}
	return nil
}

func (o *org) policy(id string) (*Policy, error) {
	if p := o.Policies[id]; p != nil {
		return p, nil
	}
	return nil, errf("PolicyNotFoundException", "We can't find a policy with the PolicyId that you specified.")
}

func (o *org) uniquePolicy(typ, name string) error {
	for _, p := range o.Policies {
		if p.PolicySummary.Type == typ && p.PolicySummary.Name == name {
			return errf("DuplicatePolicyException", "A policy with the same name already exists.")
		}
	}
	return nil
}

func (s *Service) createPolicy(c *core.Ctx, in *struct {
	Content, Description, Name, Type string
	Tags                             []Tag
}) (any, error) {
	if err := validType(in.Type); err != nil {
		return nil, err
	}
	if err := checkContent(in.Content); err != nil {
		return nil, err
	}
	return s.locked(c, true, func(o *org) (any, error) {
		if err := o.uniquePolicy(in.Type, in.Name); err != nil {
			return nil, err
		}
		id := newID("p-", 8)
		p := &Policy{PolicySummary{Id: id, Arn: o.arn(c, "policy", strings.ToLower(in.Type)+"/"+id), Name: in.Name,
			Description: in.Description, Type: in.Type}, in.Content}
		o.Policies[id] = p
		o.tag(id, in.Tags)
		return map[string]any{"Policy": p}, nil
	})
}

// policy serves DescribePolicy, UpdatePolicy and DeletePolicy.
func (s *Service) policy(c *core.Ctx, in *struct{ PolicyId, Name, Description, Content string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		p, err := o.policy(in.PolicyId)
		if err != nil {
			return nil, err
		}
		if c.Op != "DescribePolicy" && p.PolicySummary.AwsManaged {
			return nil, core.Errorf(403, "AccessDeniedException", "You can't change or delete an AWS managed policy.")
		}
		switch c.Op {
		case "UpdatePolicy":
			if in.Content != "" {
				if err := checkContent(in.Content); err != nil {
					return nil, err
				}
				p.Content = in.Content
			}
			if in.Name != "" && in.Name != p.PolicySummary.Name {
				if err := o.uniquePolicy(p.PolicySummary.Type, in.Name); err != nil {
					return nil, err
				}
				p.PolicySummary.Name = in.Name
			}
			p.PolicySummary.Description = cmp.Or(in.Description, p.PolicySummary.Description)
		case "DeletePolicy":
			for _, ids := range o.Attached {
				if slices.Contains(ids, p.PolicySummary.Id) {
					return nil, errf("PolicyInUseException", "The policy is attached to one or more entities. You must detach it from all roots, OUs, and accounts before performing this operation.")
				}
			}
			delete(o.Policies, p.PolicySummary.Id)
			delete(o.Tags, p.PolicySummary.Id)
			return nil, nil
		}
		return map[string]any{"Policy": p}, nil
	})
}

// listPolicies serves ListPolicies and ListPoliciesForTarget.
func (s *Service) listPolicies(c *core.Ctx, in *struct{ Filter, TargetId string }) (any, error) {
	if err := validType(in.Filter); err != nil {
		return nil, err
	}
	return s.locked(c, true, func(o *org) (any, error) {
		ids := core.SortedKeys(o.Policies)
		if c.Op == "ListPoliciesForTarget" {
			if o.kind(in.TargetId) == "" {
				return nil, errf("TargetNotFoundException", "We can't find a root, OU, account, or policy with the TargetId that you specified.")
			}
			ids = o.Attached[in.TargetId]
		}
		out := []PolicySummary{}
		for _, id := range ids {
			if p := o.Policies[id]; p.PolicySummary.Type == in.Filter {
				out = append(out, p.PolicySummary)
			}
		}
		return map[string]any{"Policies": out}, nil
	})
}

// attachment serves AttachPolicy and DetachPolicy.
func (s *Service) attachment(c *core.Ctx, in *struct{ PolicyId, TargetId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		p, err := o.policy(in.PolicyId)
		if err != nil {
			return nil, err
		}
		if o.kind(in.TargetId) == "" {
			return nil, errf("TargetNotFoundException", "We can't find a root, OU, account, or policy with the TargetId that you specified.")
		}
		typ, ids := p.PolicySummary.Type, o.Attached[in.TargetId]
		i := slices.Index(ids, p.PolicySummary.Id)
		if c.Op == "AttachPolicy" {
			switch {
			case !o.enabled(typ):
				return nil, errf("PolicyTypeNotEnabledException", "This operation can be performed only for enabled policy types.")
			case i >= 0:
				return nil, errf("DuplicatePolicyAttachmentException", "The selected policy is already attached to the specified target.")
			}
			o.Attached[in.TargetId] = append(ids, p.PolicySummary.Id)
			return nil, nil
		}
		if i < 0 {
			return nil, errf("PolicyNotAttachedException", "The policy isn't attached to the specified target in the specified root.")
		}
		scps := 0
		for _, id := range ids {
			if o.Policies[id].PolicySummary.Type == scp {
				scps++
			}
		}
		if typ == scp && scps == 1 && o.enabled(scp) {
			return nil, errf("ConstraintViolationException", "You can't detach the last service control policy (SCP) from a root, OU or account: every entity must have at least one SCP attached.")
		}
		o.Attached[in.TargetId] = slices.Delete(slices.Clone(ids), i, i+1)
		return nil, nil
	})
}

func (s *Service) listTargets(c *core.Ctx, in *struct{ PolicyId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if _, err := o.policy(in.PolicyId); err != nil {
			return nil, err
		}
		type target struct{ TargetId, Arn, Name, Type string }
		out := []target{}
		for _, id := range core.SortedKeys(o.Attached) {
			if !slices.Contains(o.Attached[id], in.PolicyId) {
				continue
			}
			switch k := o.kind(id); k {
			case "ROOT":
				out = append(out, target{id, o.Root.Arn, o.Root.Name, k})
			case "ORGANIZATIONAL_UNIT":
				out = append(out, target{id, o.OUs[id].Arn, o.OUs[id].Name, k})
			case "ACCOUNT":
				out = append(out, target{id, o.Accounts[id].Arn, o.Accounts[id].Name, k})
			}
		}
		return map[string]any{"Targets": out}, nil
	})
}

// policyType serves EnablePolicyType and DisablePolicyType. Disabling a type
// detaches its policies from every target; SCPs then fall back to
// FullAWSAccess alone, which is what AWS attaches when they are enabled again.
func (s *Service) policyType(c *core.Ctx, in *struct{ RootId, PolicyType string }) (any, error) {
	if err := validType(in.PolicyType); err != nil {
		return nil, err
	}
	return s.locked(c, true, func(o *org) (any, error) {
		if in.RootId != o.Root.Id {
			return nil, errf("RootNotFoundException", "We can't find a root with the RootId that you specified.")
		}
		on := o.enabled(in.PolicyType)
		switch {
		case c.Op == "EnablePolicyType" && on:
			return nil, errf("PolicyTypeAlreadyEnabledException", "The specified policy type is already enabled.")
		case c.Op == "DisablePolicyType" && !on:
			return nil, errf("PolicyTypeNotEnabledException", "This operation can be performed only for enabled policy types.")
		case c.Op == "EnablePolicyType":
			o.Root.PolicyTypes = append(o.Root.PolicyTypes, PolicyTypeSummary{in.PolicyType, "ENABLED"})
		default:
			o.Root.PolicyTypes = slices.DeleteFunc(o.Root.PolicyTypes, func(t PolicyTypeSummary) bool { return t.Type == in.PolicyType })
			for target, ids := range o.Attached {
				ids = slices.DeleteFunc(ids, func(id string) bool { return o.Policies[id].PolicySummary.Type == in.PolicyType })
				if in.PolicyType == scp {
					ids = append(ids, fullAccess)
				}
				o.Attached[target] = ids
			}
		}
		return map[string]any{"Root": o.Root}, nil
	})
}

// effectivePolicy merges the policies of a type attached from the root down
// to the target. ponytail: a plain JSON merge, children overriding parents;
// the inheritance operators (@@assign, @@append, @@remove) are not applied.
func (s *Service) effectivePolicy(c *core.Ctx, in *struct{ PolicyType, TargetId string }) (any, error) {
	if in.PolicyType == scp || in.PolicyType == "RESOURCE_CONTROL_POLICY" || validType(in.PolicyType) != nil {
		return nil, invalid("You specified an invalid value for PolicyType.")
	}
	target := cmp.Or(in.TargetId, c.Account)
	return s.locked(c, false, func(o *org) (any, error) {
		switch {
		case o.MasterAccountId != c.Account && target != c.Account:
			return nil, core.Errorf(403, "AccessDeniedException", "You don't have permissions to access this resource.")
		case o.kind(target) == "":
			return nil, errf("TargetNotFoundException", "We can't find a root, OU, account, or policy with the TargetId that you specified.")
		}
		merged, found := map[string]any{}, false
		for _, t := range o.path(target) {
			for _, id := range o.Attached[t] {
				var doc map[string]any
				if p := o.Policies[id]; p.PolicySummary.Type == in.PolicyType && json.Unmarshal([]byte(p.Content), &doc) == nil {
					merge(merged, doc)
					found = true
				}
			}
		}
		if !found {
			return nil, errf("EffectivePolicyNotFoundException", "We can't find an effective policy of the specified type for the target.")
		}
		b, _ := json.Marshal(merged)
		return map[string]any{"EffectivePolicy": map[string]any{"PolicyContent": string(b), "LastUpdatedTimestamp": now(),
			"TargetId": target, "PolicyType": in.PolicyType}}, nil
	})
}

func merge(dst, src map[string]any) {
	for k, v := range src {
		if sm, ok := v.(map[string]any); ok {
			if dm, ok := dst[k].(map[string]any); ok {
				merge(dm, sm)
				continue
			}
		}
		dst[k] = v
	}
}

// ---- delegated administrators and trusted services ----

// delegate serves RegisterDelegatedAdministrator and DeregisterDelegatedAdministrator.
func (s *Service) delegate(c *core.Ctx, in *struct{ AccountId, ServicePrincipal string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if _, err := o.account(in.AccountId); err != nil {
			return nil, err
		}
		_, registered := o.Delegated[in.AccountId][in.ServicePrincipal]
		switch {
		case in.AccountId == o.MasterAccountId:
			return nil, errf("ConstraintViolationException", "You can't register the management account of the organization as a delegated administrator.")
		case c.Op == "RegisterDelegatedAdministrator" && registered:
			return nil, errf("AccountAlreadyRegisteredException", "The provided account is already a delegated administrator for your organization.")
		case c.Op == "DeregisterDelegatedAdministrator" && !registered:
			return nil, errf("AccountNotRegisteredException", "The provided account is not a registered delegated administrator for your organization.")
		case c.Op == "RegisterDelegatedAdministrator":
			if o.Delegated[in.AccountId] == nil {
				o.Delegated[in.AccountId] = map[string]core.Time{}
			}
			o.Delegated[in.AccountId][in.ServicePrincipal] = now()
		default:
			delete(o.Delegated[in.AccountId], in.ServicePrincipal)
			if len(o.Delegated[in.AccountId]) == 0 {
				delete(o.Delegated, in.AccountId)
			}
		}
		return nil, nil
	})
}

func (s *Service) listDelegatedAdministrators(c *core.Ctx, in *struct{ ServicePrincipal string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		type admin struct {
			Account
			DelegationEnabledDate core.Time
		}
		out := []admin{}
		for _, id := range core.SortedKeys(o.Delegated) {
			for _, sp := range core.SortedKeys(o.Delegated[id]) {
				if in.ServicePrincipal == "" || sp == in.ServicePrincipal {
					out = append(out, admin{*o.Accounts[id], o.Delegated[id][sp]})
					break
				}
			}
		}
		return map[string]any{"DelegatedAdministrators": out}, nil
	})
}

func (s *Service) listDelegatedServices(c *core.Ctx, in *struct{ AccountId string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if _, err := o.account(in.AccountId); err != nil {
			return nil, err
		}
		svcs := o.Delegated[in.AccountId]
		if len(svcs) == 0 {
			return nil, errf("AccountNotRegisteredException", "The provided account is not a registered delegated administrator for your organization.")
		}
		type service struct {
			ServicePrincipal      string
			DelegationEnabledDate core.Time
		}
		out := []service{}
		for _, sp := range core.SortedKeys(svcs) {
			out = append(out, service{sp, svcs[sp]})
		}
		return map[string]any{"DelegatedServices": out}, nil
	})
}

// serviceAccess serves Enable/DisableAWSServiceAccess and ListAWSServiceAccessForOrganization.
func (s *Service) serviceAccess(c *core.Ctx, in *struct{ ServicePrincipal string }) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		switch c.Op {
		case "EnableAWSServiceAccess":
			if in.ServicePrincipal == "" {
				return nil, invalid("ServicePrincipal is required.")
			}
			if _, ok := o.Services[in.ServicePrincipal]; !ok {
				o.Services[in.ServicePrincipal] = now()
			}
			return nil, nil
		case "DisableAWSServiceAccess":
			delete(o.Services, in.ServicePrincipal)
			return nil, nil
		}
		type enabled struct {
			ServicePrincipal string
			DateEnabled      core.Time
		}
		out := []enabled{}
		for _, sp := range core.SortedKeys(o.Services) {
			out = append(out, enabled{sp, o.Services[sp]})
		}
		return map[string]any{"EnabledServicePrincipals": out}, nil
	})
}

// tags serves TagResource, UntagResource and ListTagsForResource on roots,
// OUs, accounts and policies.
func (s *Service) tags(c *core.Ctx, in *struct {
	ResourceId string
	Tags       []Tag
	TagKeys    []string
}) (any, error) {
	return s.locked(c, true, func(o *org) (any, error) {
		if o.kind(in.ResourceId) == "" && o.Policies[in.ResourceId] == nil {
			return nil, errf("TargetNotFoundException", "We can't find a root, OU, account, or policy with the TargetId that you specified.")
		}
		switch c.Op {
		case "TagResource":
			o.tag(in.ResourceId, in.Tags)
			return nil, nil
		case "UntagResource":
			for _, k := range in.TagKeys {
				delete(o.Tags[in.ResourceId], k)
			}
			return nil, nil
		}
		out, tags := []Tag{}, o.Tags[in.ResourceId]
		for _, k := range core.SortedKeys(tags) {
			out = append(out, Tag{k, tags[k]})
		}
		return map[string]any{"Tags": out}, nil
	})
}

// ---- console ----

// Resources lists accounts, OUs and policies of every organization.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	at := core.Table{Name: "Accounts", Columns: []string{"Account", "Name", "Email", "Status", "Parent", "Organization"}}
	ot := core.Table{Name: "Organizational units", Columns: []string{"OU", "Name", "Parent", "Organization"}}
	pt := core.Table{Name: "Policies", Columns: []string{"Policy", "Name", "Type", "Targets", "Organization"}}
	for _, m := range core.SortedKeys(s.st.Orgs) {
		o := s.st.Orgs[m]
		for _, id := range core.SortedKeys(o.Accounts) {
			a := o.Accounts[id]
			at.Rows = append(at.Rows, []string{a.Id, a.Name, a.Email, a.Status, o.Parents[id], o.Id})
		}
		for _, id := range core.SortedKeys(o.OUs) {
			ot.Rows = append(ot.Rows, []string{id, o.OUs[id].Name, o.Parents[id], o.Id})
		}
		for _, id := range core.SortedKeys(o.Policies) {
			n := 0
			for _, ids := range o.Attached {
				if slices.Contains(ids, id) {
					n++
				}
			}
			p := o.Policies[id].PolicySummary
			pt.Rows = append(pt.Rows, []string{id, p.Name, p.Type, strconv.Itoa(n), o.Id})
		}
	}
	return []core.Table{at, ot, pt}
}

// Inspect returns an account, OU or policy with its attachments and tags.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.st.Orgs {
		var v any
		switch {
		case o.Accounts[id] != nil:
			v = o.Accounts[id]
		case o.OUs[id] != nil:
			v = o.OUs[id]
		case o.Policies[id] != nil:
			v = o.Policies[id]
		default:
			continue
		}
		return map[string]any{"resource": v, "parent": o.Parents[id], "policies": o.Attached[id], "tags": maps.Clone(o.Tags[id])}
	}
	return nil
}
