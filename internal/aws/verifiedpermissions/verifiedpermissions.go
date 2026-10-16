// Package verifiedpermissions emulates Amazon Verified Permissions (JSON 1.0)
// with real Cedar evaluation (cedar-go): policy stores, Cedar JSON schemas
// (STRICT stores validate policies against them), static and template-linked
// policies, policy templates and Cognito identity sources. The token
// operations verify ID and access tokens from pogi's Cognito against the user
// pool's JWKS.
package verifiedpermissions

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"math/big"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	cedar "github.com/cedar-policy/cedar-go"
	"github.com/cedar-policy/cedar-go/types"
	xast "github.com/cedar-policy/cedar-go/x/exp/ast"
	"github.com/cedar-policy/cedar-go/x/exp/schema"
	"github.com/cedar-policy/cedar-go/x/exp/schema/resolved"
	"github.com/cedar-policy/cedar-go/x/exp/schema/validate"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Verified Permissions emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Stores map[string]*store `json:"stores"` // by id
}

// store is a policy store. Its compiled policy set and resolved schema are
// caches, rebuilt after a change.
type store struct {
	Account            string `json:"account"`
	Region             string `json:"region"`
	PolicyStoreId      string `json:"policyStoreId"`
	Arn                string `json:"arn"`
	Description        string `json:"description,omitempty"`
	DeletionProtection string `json:"deletionProtection"`
	ValidationSettings struct {
		Mode string `json:"mode"`
	} `json:"validationSettings"`
	CreatedDate     time.Time            `json:"createdDate"`
	LastUpdatedDate time.Time            `json:"lastUpdatedDate"`
	Schema          *schemaDoc           `json:"schema,omitempty"`
	Policies        map[string]*policy   `json:"policies"`
	Templates       map[string]*template `json:"templates"`
	Sources         map[string]*source   `json:"sources"`
	Tags            map[string]string    `json:"tags,omitempty"`

	ps  *cedar.PolicySet
	res *resolved.Schema
}

type schemaDoc struct {
	Schema          string    `json:"schema"`
	Namespaces      []string  `json:"namespaces"`
	CreatedDate     time.Time `json:"createdDate"`
	LastUpdatedDate time.Time `json:"lastUpdatedDate"`
}

// The types below use the API's member names: they are stored and returned.

type entityID struct {
	EntityType string `json:"entityType"`
	EntityId   string `json:"entityId"`
}

func (e *entityID) uid() types.EntityUID {
	if e == nil {
		return types.EntityUID{}
	}
	return types.NewEntityUID(types.EntityType(e.EntityType), types.String(e.EntityId))
}

type actionID struct {
	ActionType string `json:"actionType"`
	ActionId   string `json:"actionId"`
}

func (a *actionID) uid() types.EntityUID {
	if a == nil {
		return types.EntityUID{}
	}
	return types.NewEntityUID(types.EntityType(a.ActionType), types.String(a.ActionId))
}

type policy struct {
	PolicyStoreId   string     `json:"policyStoreId"`
	PolicyId        string     `json:"policyId"`
	Name            string     `json:"name,omitempty"`
	PolicyType      string     `json:"policyType"` // STATIC or TEMPLATE_LINKED
	Principal       *entityID  `json:"principal,omitempty"`
	Resource        *entityID  `json:"resource,omitempty"`
	Actions         []actionID `json:"actions"`
	Effect          string     `json:"effect"`
	Definition      definition `json:"definition"`
	CreatedDate     time.Time  `json:"createdDate"`
	LastUpdatedDate time.Time  `json:"lastUpdatedDate"`
}

// definition is the PolicyDefinition union.
type definition struct {
	Static *struct {
		Description string `json:"description,omitempty"`
		Statement   string `json:"statement"`
	} `json:"static,omitempty"`
	TemplateLinked *struct {
		PolicyTemplateId string    `json:"policyTemplateId"`
		Principal        *entityID `json:"principal,omitempty"`
		Resource         *entityID `json:"resource,omitempty"`
	} `json:"templateLinked,omitempty"`
}

type template struct {
	PolicyStoreId    string    `json:"policyStoreId"`
	PolicyTemplateId string    `json:"policyTemplateId"`
	Name             string    `json:"name,omitempty"`
	Description      string    `json:"description,omitempty"`
	Statement        string    `json:"statement"`
	CreatedDate      time.Time `json:"createdDate"`
	LastUpdatedDate  time.Time `json:"lastUpdatedDate"`
}

type source struct {
	PolicyStoreId       string        `json:"policyStoreId"`
	IdentitySourceId    string        `json:"identitySourceId"`
	PrincipalEntityType string        `json:"principalEntityType"`
	Configuration       configuration `json:"configuration"`
	CreatedDate         time.Time     `json:"createdDate"`
	LastUpdatedDate     time.Time     `json:"lastUpdatedDate"`
}

// configuration is the Configuration union; OpenID Connect sources are kept
// as given. ponytail: only Cognito tokens are verified.
type configuration struct {
	CognitoUserPoolConfiguration *struct {
		UserPoolArn        string   `json:"userPoolArn"`
		ClientIds          []string `json:"clientIds"`
		GroupConfiguration *struct {
			GroupEntityType string `json:"groupEntityType"`
		} `json:"groupConfiguration,omitempty"`
	} `json:"cognitoUserPoolConfiguration,omitempty"`
	OpenIdConnectConfiguration json.RawMessage `json:"openIdConnectConfiguration,omitempty"`
}

// New creates the Verified Permissions service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.0", Ops: map[string]core.Handler{
		"CreatePolicyStore":          core.H(s.createPolicyStore),
		"GetPolicyStore":             core.H(s.policyStore),
		"UpdatePolicyStore":          core.H(s.policyStore),
		"DeletePolicyStore":          core.H(s.policyStore),
		"ListPolicyStores":           core.H(s.listPolicyStores),
		"PutSchema":                  core.H(s.putSchema),
		"GetSchema":                  core.H(s.getSchema),
		"CreatePolicy":               core.H(s.putPolicy),
		"UpdatePolicy":               core.H(s.putPolicy),
		"GetPolicy":                  core.H(s.policy),
		"DeletePolicy":               core.H(s.policy),
		"ListPolicies":               core.H(s.listPolicies),
		"CreatePolicyTemplate":       core.H(s.putTemplate),
		"UpdatePolicyTemplate":       core.H(s.putTemplate),
		"GetPolicyTemplate":          core.H(s.template),
		"DeletePolicyTemplate":       core.H(s.template),
		"ListPolicyTemplates":        core.H(s.listTemplates),
		"CreateIdentitySource":       core.H(s.putSource),
		"UpdateIdentitySource":       core.H(s.putSource),
		"GetIdentitySource":          core.H(s.source),
		"DeleteIdentitySource":       core.H(s.source),
		"ListIdentitySources":        core.H(s.listSources),
		"IsAuthorized":               core.H(s.isAuthorized),
		"BatchIsAuthorized":          core.H(s.isAuthorized),
		"IsAuthorizedWithToken":      core.H(s.isAuthorized),
		"BatchIsAuthorizedWithToken": core.H(s.isAuthorized),
		"TagResource":                core.H(s.tags),
		"UntagResource":              core.H(s.tags),
		"ListTagsForResource":        core.H(s.tags),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "verifiedpermissions", Cloud: core.AWS, Title: "Verified Permissions", Targets: []string{"VerifiedPermissions"},
		Signing: []string{"verifiedpermissions"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Stores: map[string]*store{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := state{Stores: map[string]*store{}}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

func notFound(kind, id string) error {
	return core.Errorf(404, "ResourceNotFoundException", "%s %s not found.", kind, id)
}

func newID() string { return core.RandString(22, core.AlphaNum) }

func now() time.Time { return time.Now().UTC().Truncate(time.Millisecond) }

// store finds one of the caller's policy stores; lock held.
func (s *Service) store(c *core.Ctx, id string) (*store, error) {
	if st := s.st.Stores[id]; st != nil && st.Account == c.Account && st.Region == c.Region {
		return st, nil
	}
	return nil, notFound("Policy store", id)
}

// withStore runs fn on a policy store under the lock.
func (s *Service) withStore(c *core.Ctx, id string, fn func(st *store) (any, error)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.store(c, id)
	if err != nil {
		return nil, err
	}
	return fn(st)
}

// changed drops the store's caches after its policies, templates or schema change.
func (st *store) changed() {
	st.ps, st.LastUpdatedDate = nil, now()
}

// ---- policy stores ----

type storeInput struct {
	PolicyStoreId      string
	Description        string
	DeletionProtection string
	ValidationSettings *struct{ Mode string }
	Tags               map[string]string
}

func (in *storeInput) apply(st *store) error {
	if v := in.ValidationSettings; v != nil {
		if v.Mode != "OFF" && v.Mode != "STRICT" {
			return invalid("validationSettings.mode must be OFF or STRICT.")
		}
		st.ValidationSettings.Mode = v.Mode
	}
	if d := in.DeletionProtection; d != "" {
		if d != "ENABLED" && d != "DISABLED" {
			return invalid("deletionProtection must be ENABLED or DISABLED.")
		}
		st.DeletionProtection = d
	}
	st.Description = in.Description
	return nil
}

func (st *store) out() map[string]any {
	return map[string]any{"policyStoreId": st.PolicyStoreId, "arn": st.Arn, "validationSettings": st.ValidationSettings,
		"description": st.Description, "deletionProtection": st.DeletionProtection, "cedarVersion": "CEDAR_4",
		"createdDate": st.CreatedDate, "lastUpdatedDate": st.LastUpdatedDate}
}

func (s *Service) createPolicyStore(c *core.Ctx, in *storeInput) (any, error) {
	if in.ValidationSettings == nil {
		return nil, invalid("validationSettings is required.")
	}
	t := now()
	st := &store{Account: c.Account, Region: c.Region, PolicyStoreId: newID(), DeletionProtection: "DISABLED", CreatedDate: t, LastUpdatedDate: t,
		Policies: map[string]*policy{}, Templates: map[string]*template{}, Sources: map[string]*source{}, Tags: in.Tags}
	st.Arn = c.GlobalARN("verifiedpermissions", "policy-store/"+st.PolicyStoreId)
	if err := in.apply(st); err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.st.Stores[st.PolicyStoreId] = st
	s.mu.Unlock()
	return st.out(), nil
}

// policyStore serves GetPolicyStore, UpdatePolicyStore and DeletePolicyStore.
func (s *Service) policyStore(c *core.Ctx, in *storeInput) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		switch c.Op {
		case "UpdatePolicyStore":
			if in.ValidationSettings == nil {
				return nil, invalid("validationSettings is required.")
			}
			if err := in.apply(st); err != nil {
				return nil, err
			}
			st.LastUpdatedDate = now()
		case "DeletePolicyStore":
			if st.DeletionProtection == "ENABLED" {
				return nil, core.Errorf(400, "InvalidStateException", "Policy store %s has deletion protection enabled.", st.PolicyStoreId)
			}
			delete(s.st.Stores, st.PolicyStoreId)
			return nil, nil
		}
		return st.out(), nil
	})
}

// ponytail: list operations return one page.
func (s *Service) listPolicyStores(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []map[string]any{}
	for _, id := range core.SortedKeys(s.st.Stores) {
		if st := s.st.Stores[id]; st.Account == c.Account && st.Region == c.Region {
			out = append(out, st.out())
		}
	}
	return map[string]any{"policyStores": out}, nil
}

// ---- schemas ----

func (s *Service) putSchema(c *core.Ctx, in *struct {
	PolicyStoreId string
	Definition    struct{ CedarJson string }
}) (any, error) {
	var sch schema.Schema
	if err := sch.UnmarshalJSON([]byte(in.Definition.CedarJson)); err != nil {
		return nil, invalid("The schema is not valid Cedar JSON: %v", err)
	}
	res, err := sch.Resolve()
	if err != nil {
		return nil, invalid("The schema is not valid: %v", err)
	}
	var top map[string]json.RawMessage
	json.Unmarshal([]byte(in.Definition.CedarJson), &top)
	namespaces := []string{}
	for _, ns := range core.SortedKeys(top) {
		if ns != "" {
			namespaces = append(namespaces, ns)
		}
	}
	// ponytail: existing policies are not revalidated against a new schema.
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		t := now()
		created := t
		if st.Schema != nil {
			created = st.Schema.CreatedDate
		}
		st.Schema, st.res = &schemaDoc{in.Definition.CedarJson, namespaces, created, t}, res
		st.changed()
		return map[string]any{"policyStoreId": st.PolicyStoreId, "namespaces": namespaces, "createdDate": created, "lastUpdatedDate": t}, nil
	})
}

func (s *Service) getSchema(c *core.Ctx, in *struct{ PolicyStoreId string }) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		if st.Schema == nil {
			return nil, notFound("Schema of policy store", st.PolicyStoreId)
		}
		d := st.Schema
		return map[string]any{"policyStoreId": st.PolicyStoreId, "schema": d.Schema, "namespaces": d.Namespaces,
			"createdDate": d.CreatedDate, "lastUpdatedDate": d.LastUpdatedDate}, nil
	})
}

// resolved returns the store's resolved schema, or nil without one; lock held.
func (st *store) resolved() *resolved.Schema {
	if st.res == nil && st.Schema != nil {
		var sch schema.Schema
		if sch.UnmarshalJSON([]byte(st.Schema.Schema)) == nil {
			st.res, _ = sch.Resolve()
		}
	}
	return st.res
}

// ---- policies ----

// parse compiles a statement holding exactly one Cedar policy.
func parse(statement string) (*cedar.Policy, error) {
	ps, err := cedar.NewPolicySetFromBytes("", []byte(statement))
	if err != nil {
		return nil, invalid("The policy statement is not valid Cedar: %v", err)
	}
	var out []*cedar.Policy
	for _, p := range ps.All() {
		out = append(out, p)
	}
	if len(out) != 1 {
		return nil, invalid("A policy statement must contain exactly one policy.")
	}
	return out[0], nil
}

// link fills a template's ?principal and ?resource slots.
// ponytail: plain text substitution; a slot name inside a string literal is replaced too.
func link(statement string, principal, resource *entityID) string {
	lit := func(e *entityID) string {
		if e == nil {
			return "?"
		}
		return string(e.uid().MarshalCedar())
	}
	return strings.NewReplacer("?principal", lit(principal), "?resource", lit(resource)).Replace(statement)
}

// statement is a policy's Cedar text; lock held.
func (st *store) statement(p *policy) string {
	if d := p.Definition.TemplateLinked; d != nil {
		return link(st.Templates[d.PolicyTemplateId].Statement, d.Principal, d.Resource)
	}
	return p.Definition.Static.Statement
}

// check validates a policy against the schema of a STRICT store; lock held.
func (st *store) check(p *cedar.Policy) error {
	res := st.resolved()
	if st.ValidationSettings.Mode != "STRICT" || res == nil {
		return nil
	}
	if err := validate.New(res).Policy("policy", (*xast.Policy)(p.AST())); err != nil {
		return invalid("The policy failed validation against the schema: %v", err)
	}
	return nil
}

// describe fills a policy's effect, scope and actions from its compiled form.
func describe(p *policy, cp *cedar.Policy) {
	ref := func(n xast.IsScopeNode) *entityID {
		var uid types.EntityUID
		switch s := n.(type) {
		case xast.ScopeTypeEq:
			uid = s.Entity
		case xast.ScopeTypeIn:
			uid = s.Entity
		case xast.ScopeTypeIsIn:
			uid = s.Entity
		default:
			return nil
		}
		return &entityID{string(uid.Type), string(uid.ID)}
	}
	a := (*xast.Policy)(cp.AST())
	p.Effect = map[xast.Effect]string{xast.EffectPermit: "Permit", xast.EffectForbid: "Forbid"}[a.Effect]
	p.Principal, p.Resource, p.Actions = ref(a.Principal), ref(a.Resource), []actionID{}
	var actions []types.EntityUID
	switch s := a.Action.(type) {
	case xast.ScopeTypeEq:
		actions = []types.EntityUID{s.Entity}
	case xast.ScopeTypeIn:
		actions = []types.EntityUID{s.Entity}
	case xast.ScopeTypeInSet:
		actions = s.Entities
	}
	for _, uid := range actions {
		p.Actions = append(p.Actions, actionID{string(uid.Type), string(uid.ID)})
	}
}

// find looks a policy or template up by id or by "name/…"; lock held.
func find[T any](m map[string]*T, id string, name func(*T) string) *T {
	if v := m[id]; v != nil {
		return v
	}
	for _, v := range m {
		if strings.HasPrefix(id, "name/") && name(v) == id {
			return v
		}
	}
	return nil
}

// putPolicy serves CreatePolicy and UpdatePolicy.
func (s *Service) putPolicy(c *core.Ctx, in *struct {
	PolicyStoreId, PolicyId string
	Name                    *string
	Definition              definition
}) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		p := &policy{PolicyStoreId: st.PolicyStoreId, PolicyId: newID(), CreatedDate: now()}
		var old *policy
		if c.Op == "UpdatePolicy" {
			if old = find(st.Policies, in.PolicyId, func(p *policy) string { return p.Name }); old == nil {
				return nil, notFound("Policy", in.PolicyId)
			}
			if old.PolicyType != "STATIC" || in.Definition.Static == nil {
				return nil, invalid("Only static policies can be updated, with a static definition.")
			}
			upd := *old
			p = &upd
		}
		if in.Name != nil {
			if *in.Name != "" && !strings.HasPrefix(*in.Name, "name/") {
				return nil, invalid("A policy name must start with name/.")
			}
			for _, o := range st.Policies {
				if *in.Name != "" && o.Name == *in.Name && o.PolicyId != p.PolicyId {
					return nil, core.Errorf(409, "ConflictException", "A policy named %s already exists in policy store %s.", *in.Name, st.PolicyStoreId)
				}
			}
			p.Name = *in.Name
		}
		d := in.Definition
		switch {
		case d.Static != nil && d.TemplateLinked == nil:
			p.PolicyType = "STATIC"
		case d.TemplateLinked != nil && d.Static == nil:
			t := st.Templates[d.TemplateLinked.PolicyTemplateId]
			if t == nil {
				return nil, notFound("Policy template", d.TemplateLinked.PolicyTemplateId)
			}
			if strings.Contains(t.Statement, "?principal") != (d.TemplateLinked.Principal != nil) ||
				strings.Contains(t.Statement, "?resource") != (d.TemplateLinked.Resource != nil) {
				return nil, invalid("The principal and resource must fill exactly the slots of template %s.", t.PolicyTemplateId)
			}
			p.PolicyType = "TEMPLATE_LINKED"
		default:
			return nil, invalid("definition must have exactly one of static or templateLinked.")
		}
		p.Definition = d
		cp, err := parse(st.statement(p))
		if err != nil {
			return nil, err
		}
		if err := st.check(cp); err != nil {
			return nil, err
		}
		describe(p, cp)
		if old != nil && (p.Effect != old.Effect || !reflect.DeepEqual(p.Principal, old.Principal) || !reflect.DeepEqual(p.Resource, old.Resource)) {
			return nil, invalid("An update can't change a policy's effect, principal or resource.")
		}
		p.LastUpdatedDate = now()
		st.Policies[p.PolicyId] = p
		st.changed()
		return p, nil
	})
}

// policy serves GetPolicy and DeletePolicy.
func (s *Service) policy(c *core.Ctx, in *struct{ PolicyStoreId, PolicyId string }) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		p := find(st.Policies, in.PolicyId, func(p *policy) string { return p.Name })
		if p == nil {
			return nil, notFound("Policy", in.PolicyId)
		}
		if c.Op == "DeletePolicy" {
			delete(st.Policies, p.PolicyId)
			st.changed()
			return nil, nil
		}
		return p, nil
	})
}

type entityRef struct {
	Unspecified bool
	Identifier  *entityID
}

func (r *entityRef) matches(e *entityID) bool {
	return r == nil || r.Unspecified && e == nil || r.Identifier != nil && e != nil && *r.Identifier == *e
}

func (s *Service) listPolicies(c *core.Ctx, in *struct {
	PolicyStoreId string
	Filter        struct {
		Principal, Resource          *entityRef
		PolicyType, PolicyTemplateId string
	}
}) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		f, out := in.Filter, []*policy{}
		for _, id := range core.SortedKeys(st.Policies) {
			p := st.Policies[id]
			tl := p.Definition.TemplateLinked
			if f.Principal.matches(p.Principal) && f.Resource.matches(p.Resource) && (f.PolicyType == "" || f.PolicyType == p.PolicyType) &&
				(f.PolicyTemplateId == "" || tl != nil && tl.PolicyTemplateId == f.PolicyTemplateId) {
				out = append(out, p)
			}
		}
		return map[string]any{"policies": out}, nil
	})
}

// ---- policy templates ----

// putTemplate serves CreatePolicyTemplate and UpdatePolicyTemplate; linked
// policies follow the new statement.
func (s *Service) putTemplate(c *core.Ctx, in *struct {
	PolicyStoreId, PolicyTemplateId, Description, Statement string
	Name                                                    *string
}) (any, error) {
	slot := &entityID{"Slot", "slot"}
	if _, err := parse(link(in.Statement, slot, slot)); err != nil {
		return nil, err
	}
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		t := &template{PolicyStoreId: st.PolicyStoreId, PolicyTemplateId: newID(), CreatedDate: now()}
		if c.Op == "UpdatePolicyTemplate" {
			if t = find(st.Templates, in.PolicyTemplateId, func(t *template) string { return t.Name }); t == nil {
				return nil, notFound("Policy template", in.PolicyTemplateId)
			}
		}
		if in.Name != nil {
			for _, o := range st.Templates {
				if *in.Name != "" && o.Name == *in.Name && o.PolicyTemplateId != t.PolicyTemplateId {
					return nil, core.Errorf(409, "ConflictException", "A policy template named %s already exists in policy store %s.", *in.Name, st.PolicyStoreId)
				}
			}
			t.Name = *in.Name
		}
		t.Description, t.Statement, t.LastUpdatedDate = in.Description, in.Statement, now()
		st.Templates[t.PolicyTemplateId] = t
		st.changed()
		return t, nil
	})
}

// template serves GetPolicyTemplate and DeletePolicyTemplate, which also
// deletes the policies linked to the template.
func (s *Service) template(c *core.Ctx, in *struct{ PolicyStoreId, PolicyTemplateId string }) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		t := find(st.Templates, in.PolicyTemplateId, func(t *template) string { return t.Name })
		if t == nil {
			return nil, notFound("Policy template", in.PolicyTemplateId)
		}
		if c.Op == "DeletePolicyTemplate" {
			maps.DeleteFunc(st.Policies, func(_ string, p *policy) bool {
				return p.Definition.TemplateLinked != nil && p.Definition.TemplateLinked.PolicyTemplateId == t.PolicyTemplateId
			})
			delete(st.Templates, t.PolicyTemplateId)
			st.changed()
			return nil, nil
		}
		return t, nil
	})
}

func (s *Service) listTemplates(c *core.Ctx, in *struct{ PolicyStoreId string }) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		out := []*template{}
		for _, id := range core.SortedKeys(st.Templates) {
			out = append(out, st.Templates[id])
		}
		return map[string]any{"policyTemplates": out}, nil
	})
}

// ---- identity sources ----

// putSource serves CreateIdentitySource and UpdateIdentitySource.
func (s *Service) putSource(c *core.Ctx, in *struct {
	PolicyStoreId, IdentitySourceId, PrincipalEntityType string
	Configuration, UpdateConfiguration                   *configuration
}) (any, error) {
	cfg := in.Configuration
	if c.Op == "UpdateIdentitySource" {
		cfg = in.UpdateConfiguration
	}
	if cfg == nil || (cfg.CognitoUserPoolConfiguration == nil) == (cfg.OpenIdConnectConfiguration == nil) {
		return nil, invalid("The configuration must have exactly one of cognitoUserPoolConfiguration or openIdConnectConfiguration.")
	}
	if cp := cfg.CognitoUserPoolConfiguration; cp != nil {
		if _, _, _, res, ok := core.ParseARN(cp.UserPoolArn); !ok || !strings.HasPrefix(res, "userpool/") {
			return nil, invalid("userPoolArn %q is not a user pool ARN.", cp.UserPoolArn)
		}
	}
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		src := &source{PolicyStoreId: st.PolicyStoreId, IdentitySourceId: newID(), CreatedDate: now()}
		if c.Op == "UpdateIdentitySource" {
			if src = st.Sources[in.IdentitySourceId]; src == nil {
				return nil, notFound("Identity source", in.IdentitySourceId)
			}
		}
		if in.PrincipalEntityType != "" {
			src.PrincipalEntityType = in.PrincipalEntityType
		}
		if src.PrincipalEntityType == "" {
			return nil, invalid("principalEntityType is required.")
		}
		src.Configuration, src.LastUpdatedDate = *cfg, now()
		st.Sources[src.IdentitySourceId] = src
		return map[string]any{"policyStoreId": st.PolicyStoreId, "identitySourceId": src.IdentitySourceId,
			"createdDate": src.CreatedDate, "lastUpdatedDate": src.LastUpdatedDate}, nil
	})
}

// source serves GetIdentitySource and DeleteIdentitySource.
func (s *Service) source(c *core.Ctx, in *struct{ PolicyStoreId, IdentitySourceId string }) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		src := st.Sources[in.IdentitySourceId]
		if src == nil {
			return nil, notFound("Identity source", in.IdentitySourceId)
		}
		if c.Op == "DeleteIdentitySource" {
			delete(st.Sources, src.IdentitySourceId)
			return nil, nil
		}
		return src, nil
	})
}

func (s *Service) listSources(c *core.Ctx, in *struct {
	PolicyStoreId string
	Filters       []struct{ PrincipalEntityType string }
}) (any, error) {
	return s.withStore(c, in.PolicyStoreId, func(st *store) (any, error) {
		out := []*source{}
		for _, id := range core.SortedKeys(st.Sources) {
			src := st.Sources[id]
			if len(in.Filters) == 0 || slices.ContainsFunc(in.Filters, func(f struct{ PrincipalEntityType string }) bool {
				return f.PrincipalEntityType == src.PrincipalEntityType
			}) {
				out = append(out, src)
			}
		}
		return map[string]any{"identitySources": out}, nil
	})
}

// ---- authorization ----

// attr is the AttributeValue union.
type attr struct {
	Boolean                             *bool
	Long                                *int64
	String                              *string
	EntityIdentifier                    *entityID
	Set                                 []attr
	Record                              map[string]attr
	Ipaddr, Decimal, Datetime, Duration *string
}

func (a attr) value() (types.Value, error) {
	switch {
	case a.Boolean != nil:
		return types.Boolean(*a.Boolean), nil
	case a.Long != nil:
		return types.Long(*a.Long), nil
	case a.String != nil:
		return types.String(*a.String), nil
	case a.EntityIdentifier != nil:
		return a.EntityIdentifier.uid(), nil
	case a.Set != nil:
		vs := make([]types.Value, len(a.Set))
		for i, x := range a.Set {
			v, err := x.value()
			if err != nil {
				return nil, err
			}
			vs[i] = v
		}
		return types.NewSet(vs...), nil
	case a.Record != nil:
		return record(a.Record)
	case a.Ipaddr != nil:
		return types.ParseIPAddr(*a.Ipaddr)
	case a.Decimal != nil:
		return types.ParseDecimal(*a.Decimal)
	case a.Datetime != nil:
		return types.ParseDatetime(*a.Datetime)
	case a.Duration != nil:
		return types.ParseDuration(*a.Duration)
	}
	return nil, invalid("An attribute value must set one of its members.")
}

func record(m map[string]attr) (types.Record, error) {
	rm := types.RecordMap{}
	for k, a := range m {
		v, err := a.value()
		if err != nil {
			return types.Record{}, invalid("Attribute %s: %v", k, err)
		}
		rm[types.String(k)] = v
	}
	return types.NewRecord(rm), nil
}

type entitiesInput struct {
	CedarJson  string
	EntityList []struct {
		Identifier       entityID
		Attributes, Tags map[string]attr
		Parents          []entityID
	}
}

func (e *entitiesInput) build() (types.EntityMap, error) {
	out := types.EntityMap{}
	if e == nil {
		return out, nil
	}
	if e.CedarJson != "" {
		if err := json.Unmarshal([]byte(e.CedarJson), &out); err != nil {
			return nil, invalid("entities.cedarJson is not valid: %v", err)
		}
		return out, nil
	}
	for _, it := range e.EntityList {
		attrs, err := record(it.Attributes)
		if err != nil {
			return nil, err
		}
		tags, err := record(it.Tags)
		if err != nil {
			return nil, err
		}
		var parents []types.EntityUID
		for _, p := range it.Parents {
			parents = append(parents, p.uid())
		}
		uid := it.Identifier.uid()
		out[uid] = types.Entity{UID: uid, Parents: types.NewEntityUIDSet(parents...), Attributes: attrs, Tags: tags}
	}
	return out, nil
}

type request struct {
	Principal *entityID `json:"principal,omitempty"`
	Action    *actionID `json:"action,omitempty"`
	Resource  *entityID `json:"resource,omitempty"`
	Context   *struct {
		ContextMap map[string]attr `json:"contextMap,omitempty"`
		CedarJson  string          `json:"cedarJson,omitempty"`
	} `json:"context,omitempty"`
}

type result struct {
	Request             *request            `json:"request,omitempty"`
	Principal           *entityID           `json:"principal,omitempty"` // IsAuthorizedWithToken
	Decision            string              `json:"decision"`
	DeterminingPolicies []map[string]string `json:"determiningPolicies"`
	Errors              []map[string]string `json:"errors"`
}

// authorize evaluates one request against a compiled policy set.
func authorize(ps *cedar.PolicySet, entities types.EntityMap, principal types.EntityUID, r *request) (result, error) {
	ctx, err := record(nil)
	if r.Context != nil && r.Context.CedarJson != "" {
		err = json.Unmarshal([]byte(r.Context.CedarJson), &ctx)
	} else if r.Context != nil {
		ctx, err = record(r.Context.ContextMap)
	}
	if err != nil {
		return result{}, invalid("context is not valid: %v", err)
	}
	if r.Principal != nil {
		principal = r.Principal.uid()
	}
	d, diag := cedar.Authorize(ps, entities, cedar.Request{Principal: principal, Action: r.Action.uid(), Resource: r.Resource.uid(), Context: ctx})
	out := result{Decision: "DENY", DeterminingPolicies: []map[string]string{}, Errors: []map[string]string{}}
	if d == cedar.Allow {
		out.Decision = "ALLOW"
	}
	for _, reason := range diag.Reasons {
		out.DeterminingPolicies = append(out.DeterminingPolicies, map[string]string{"policyId": string(reason.PolicyID)})
	}
	for _, e := range diag.Errors {
		out.Errors = append(out.Errors, map[string]string{"errorDescription": e.String()})
	}
	return out, nil
}

// compiled returns the store's policies as a Cedar policy set; lock held.
func (st *store) compiled() (*cedar.PolicySet, error) {
	if st.ps == nil {
		ps := cedar.NewPolicySet()
		for id, p := range st.Policies {
			cp, err := parse(st.statement(p))
			if err != nil {
				return nil, err
			}
			ps.Add(cedar.PolicyID(id), cp)
		}
		st.ps = ps
	}
	return st.ps, nil
}

// isAuthorized serves IsAuthorized, IsAuthorizedWithToken and their batch
// forms. With a token the principal comes from the token.
func (s *Service) isAuthorized(c *core.Ctx, in *struct {
	PolicyStoreId              string
	IdentityToken, AccessToken string
	Entities                   *entitiesInput
	Requests                   []*request
	request
}) (any, error) {
	batch, withToken := strings.HasPrefix(c.Op, "Batch"), strings.HasSuffix(c.Op, "WithToken")
	if !batch {
		in.Requests = []*request{&in.request}
	}
	if batch && (len(in.Requests) == 0 || len(in.Requests) > 30) {
		return nil, invalid("requests must hold between 1 and 30 requests.")
	}
	entities, err := in.Entities.build()
	if err != nil {
		return nil, err
	}
	var principal *types.Entity
	if withToken {
		s.mu.Lock()
		st, err := s.store(c, in.PolicyStoreId)
		var sources []source
		if err == nil {
			for _, src := range st.Sources {
				sources = append(sources, *src)
			}
		}
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
		if principal, err = s.principal(sources, in.IdentityToken, in.AccessToken); err != nil {
			return nil, err
		}
		entities[principal.UID] = *principal
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.store(c, in.PolicyStoreId)
	if err != nil {
		return nil, err
	}
	ps, err := st.compiled()
	if err != nil {
		return nil, err
	}
	if res := st.resolved(); res != nil { // action groups come from the schema
		for uid, a := range res.Actions {
			if _, ok := entities[uid]; !ok {
				entities[uid] = a.Entity
			}
		}
	}
	var results []result
	for _, r := range in.Requests {
		var p types.EntityUID
		if principal != nil {
			p, r.Principal = principal.UID, nil
		}
		res, err := authorize(ps, entities, p, r)
		if err != nil {
			return nil, err
		}
		if batch {
			res.Request = r
		}
		results = append(results, res)
	}
	var pid *entityID
	if principal != nil {
		pid = &entityID{string(principal.UID.Type), string(principal.UID.ID)}
	}
	if batch {
		out := map[string]any{"results": results}
		if pid != nil {
			out["principal"] = pid
		}
		return out, nil
	}
	results[0].Principal = pid
	return results[0], nil
}

// principal verifies an ID or access token from pogi's Cognito and returns
// the user's entity: <principalEntityType>::"<userPoolId>|<sub>", with the
// user's groups as parents and, for an ID token, its claims as attributes.
// ponytail: access-token claims are not mapped into the context.
func (s *Service) principal(sources []source, idToken, accessToken string) (*types.Entity, error) {
	token, use := idToken, "id"
	if token == "" {
		token, use = accessToken, "access"
	}
	bad := func(why string) error { return invalid("The %s token is not valid: %s.", use, why) }
	part := strings.Split(token, ".")
	if len(part) != 3 {
		return nil, bad("it is not a JWT")
	}
	var head struct{ Kid string }
	var claims map[string]any
	if h, err := base64.RawURLEncoding.DecodeString(part[0]); err != nil || json.Unmarshal(h, &head) != nil {
		return nil, bad("malformed header")
	}
	if b, err := base64.RawURLEncoding.DecodeString(part[1]); err != nil || json.Unmarshal(b, &claims) != nil {
		return nil, bad("malformed claims")
	}
	iss, _ := claims["iss"].(string)
	pool := iss[strings.LastIndexByte(iss, '/')+1:]
	i := slices.IndexFunc(sources, func(src source) bool {
		cp := src.Configuration.CognitoUserPoolConfiguration
		return cp != nil && strings.HasSuffix(cp.UserPoolArn, ":userpool/"+pool)
	})
	if pool == "" || i < 0 {
		return nil, bad("its issuer is not an identity source of the policy store")
	}
	src, cfg := sources[i], sources[i].Configuration.CognitoUserPoolConfiguration

	// the signature, against the pool's published keys
	req, _ := http.NewRequest(http.MethodGet, "/"+pool+"/.well-known/jwks.json", nil)
	status, body := s.hub.DoService("cognito-idp", req)
	var jwks struct{ Keys []struct{ Kid, N, E string } }
	if status != http.StatusOK || json.Unmarshal(body, &jwks) != nil {
		return nil, bad("the user pool's keys are unavailable")
	}
	sig, err := base64.RawURLEncoding.DecodeString(part[2])
	sum := sha256.Sum256([]byte(part[0] + "." + part[1]))
	verified := false
	for _, k := range jwks.Keys {
		n, err1 := base64.RawURLEncoding.DecodeString(k.N)
		e, err2 := base64.RawURLEncoding.DecodeString(k.E)
		if k.Kid == head.Kid && err == nil && err1 == nil && err2 == nil {
			pub := &rsa.PublicKey{N: new(big.Int).SetBytes(n), E: int(new(big.Int).SetBytes(e).Int64())}
			verified = rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig) == nil
		}
	}
	exp, _ := claims["exp"].(float64)
	aud, _ := claims["aud"].(string)
	if use == "access" {
		aud, _ = claims["client_id"].(string)
	}
	switch {
	case !verified:
		return nil, bad("its signature does not verify")
	case float64(time.Now().Unix()) >= exp:
		return nil, bad("it has expired")
	case claims["token_use"] != use:
		return nil, bad("it is a token of another use")
	case len(cfg.ClientIds) > 0 && !slices.Contains(cfg.ClientIds, aud):
		return nil, bad("its client is not allowed by the identity source")
	}

	sub, _ := claims["sub"].(string)
	uid := types.NewEntityUID(types.EntityType(src.PrincipalEntityType), types.String(pool+"|"+sub))
	var parents []types.EntityUID
	if g := cfg.GroupConfiguration; g != nil && g.GroupEntityType != "" {
		groups, _ := claims["cognito:groups"].([]any)
		for _, name := range groups {
			parents = append(parents, types.NewEntityUID(types.EntityType(g.GroupEntityType), types.String(pool+"|"+fmt.Sprint(name))))
		}
	}
	attrs := types.RecordMap{}
	for k, v := range claims {
		if use != "id" || slices.Contains([]string{"iss", "aud", "exp", "iat", "auth_time", "jti", "origin_jti", "event_id", "nonce", "at_hash"}, k) {
			continue
		}
		switch v := v.(type) {
		case string:
			attrs[types.String(k)] = types.String(v)
		case bool:
			attrs[types.String(k)] = types.Boolean(v)
		case float64:
			attrs[types.String(k)] = types.Long(int64(v))
		}
	}
	return &types.Entity{UID: uid, Parents: types.NewEntityUIDSet(parents...), Attributes: types.NewRecord(attrs)}, nil
}

// ---- tags ----

func (s *Service) tags(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        map[string]string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st *store
	for _, x := range s.st.Stores {
		if x.Arn == in.ResourceArn && x.Account == c.Account {
			st = x
		}
	}
	if st == nil {
		return nil, notFound("Resource", in.ResourceArn)
	}
	switch c.Op {
	case "TagResource":
		if st.Tags == nil {
			st.Tags = map[string]string{}
		}
		maps.Copy(st.Tags, in.Tags)
		return nil, nil
	case "UntagResource":
		for _, k := range in.TagKeys {
			delete(st.Tags, k)
		}
		return nil, nil
	}
	return map[string]any{"tags": maps.Clone(st.Tags)}, nil
}

// ---- console ----

// Resources lists policy stores and their policies.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := core.Table{Name: "Policy stores", Columns: []string{"Policy store", "Description", "Validation", "Policies", "Templates", "Identity sources", "Region"}}
	pt := core.Table{Name: "Policies", Columns: []string{"Policy", "Policy store", "Type", "Effect", "Principal", "Resource"}}
	for _, id := range core.SortedKeys(s.st.Stores) {
		x := s.st.Stores[id]
		st.Rows = append(st.Rows, []string{id, x.Description, x.ValidationSettings.Mode, strconv.Itoa(len(x.Policies)),
			strconv.Itoa(len(x.Templates)), strconv.Itoa(len(x.Sources)), x.Region})
		for _, pid := range core.SortedKeys(x.Policies) {
			p := x.Policies[pid]
			pt.Rows = append(pt.Rows, []string{pid, id, p.PolicyType, p.Effect, string(p.Principal.uid().MarshalCedar()), string(p.Resource.uid().MarshalCedar())})
		}
	}
	return []core.Table{st, pt}
}

// Inspect returns a policy store with its schema, policies and templates.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, x := range s.st.Stores {
		if x.PolicyStoreId == id {
			return x
		}
		if p := x.Policies[id]; p != nil {
			return map[string]any{"policy": p, "statement": x.statement(p)}
		}
	}
	return nil
}
