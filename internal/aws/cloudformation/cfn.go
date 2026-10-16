// Package cloudformation emulates AWS CloudFormation (Query protocol):
// stacks, change sets, YAML/JSON templates with intrinsic functions,
// conditions, mappings, rules and SSM-typed parameters, exports, nested
// stacks and custom resources, deploying through pogi's own services. It is
// what `cdk bootstrap` and `cdk deploy` talk to.
package cloudformation

import (
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/core"
)

const xmlns = "http://cloudformation.amazonaws.com/doc/2010-05-15/"

// Service is the CloudFormation emulator.
type Service struct {
	hub     *core.Hub
	rpc     *core.RPC
	mu      sync.Mutex
	st      state
	waiting map[string]chan customResponse // custom resource callbacks by token
}

type state struct {
	Stacks []*Stack `json:"stacks"` // oldest first; deleted stacks stay for DescribeStacks by id
}

// Stack is one stack and everything it deployed.
type Stack struct {
	ID              string               `json:"id"`
	Name            string               `json:"name"`
	Account         string               `json:"account"`
	Region          string               `json:"region"`
	Status          string               `json:"status"`
	Reason          string               `json:"reason,omitempty"`
	Description     string               `json:"description,omitempty"`
	Body            string               `json:"body"`
	Params          []Param              `json:"params,omitempty"`
	Tags            []Tag                `json:"tags,omitempty"`
	Capabilities    []string             `json:"capabilities,omitempty"`
	DisableRollback bool                 `json:"disableRollback,omitempty"`
	OnFailure       string               `json:"onFailure,omitempty"`
	Protected       bool                 `json:"protected,omitempty"`
	Policy          string               `json:"policy,omitempty"`
	Parent          string               `json:"parent,omitempty"` // nested stacks
	Created         time.Time            `json:"created"`
	Updated         time.Time            `json:"updated,omitzero"`
	Deleted         time.Time            `json:"deleted,omitzero"`
	Resources       map[string]*Resource `json:"resources"`
	Outputs         []Output             `json:"outputs,omitempty"`
	Events          []Event              `json:"events,omitempty"`
	ChangeSets      []*ChangeSet         `json:"changeSets,omitempty"`
}

// Param is a resolved stack parameter.
type Param struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Resolved string `json:"resolved,omitempty"` // SSM-typed parameters
	NoEcho   bool   `json:"noEcho,omitempty"`
}

// Tag is a stack tag.
type Tag struct {
	Key   string
	Value string
}

// Resource is a deployed (or deploying) resource.
type Resource struct {
	LogicalID  string         `json:"logicalId"`
	Type       string         `json:"type"`
	PhysicalID string         `json:"physicalId,omitempty"`
	Status     string         `json:"status"`
	Reason     string         `json:"reason,omitempty"`
	Props      map[string]any `json:"props,omitempty"`
	Attrs      map[string]any `json:"attrs,omitempty"`
	Updated    time.Time      `json:"updated"`
}

// Output is a stack output.
type Output struct {
	Key         string `json:"key"`
	Value       string `json:"value"`
	Description string `json:"description,omitempty"`
	Export      string `json:"export,omitempty"`
}

// Event is a stack event.
type Event struct {
	ID         string    `json:"id"`
	LogicalID  string    `json:"logicalId"`
	PhysicalID string    `json:"physicalId,omitempty"`
	Type       string    `json:"type"`
	Status     string    `json:"status"`
	Reason     string    `json:"reason,omitempty"`
	Time       time.Time `json:"time"`
}

// ChangeSet is a reviewed set of changes.
type ChangeSet struct {
	ID           string       `json:"id"`
	Name         string       `json:"name"`
	Type         string       `json:"type"` // CREATE | UPDATE
	Status       string       `json:"status"`
	ExecStatus   string       `json:"execStatus"`
	Reason       string       `json:"reason,omitempty"`
	Description  string       `json:"description,omitempty"`
	Body         string       `json:"body"`
	Inputs       []paramInput `json:"inputs,omitempty"`
	Tags         []Tag        `json:"tags,omitempty"`
	Capabilities []string     `json:"capabilities,omitempty"`
	Changes      []Change     `json:"changes,omitempty"`
	Created      time.Time    `json:"created"`
}

// Change is one resource change in a change set.
type Change struct {
	Action, LogicalID, PhysicalID, Type, Replacement string
}

// New creates the CloudFormation service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, waiting: map[string]chan customResponse{}}
	s.rpc = &core.RPC{XMLNS: xmlns, Ops: map[string]core.Handler{
		"CreateStack":                       core.H(s.createStack),
		"UpdateStack":                       core.H(s.updateStack),
		"DeleteStack":                       core.H(s.deleteStack),
		"DescribeStacks":                    core.H(s.describeStacks),
		"ListStacks":                        core.H(s.listStacks),
		"DescribeStackEvents":               core.H(s.describeEvents),
		"DescribeStackResources":            core.H(s.describeResources),
		"DescribeStackResource":             core.H(s.describeResource),
		"ListStackResources":                core.H(s.listResources),
		"GetTemplate":                       core.H(s.getTemplate),
		"GetTemplateSummary":                core.H(s.templateSummary),
		"ValidateTemplate":                  core.H(s.validateTemplate),
		"CreateChangeSet":                   core.H(s.createChangeSet),
		"DescribeChangeSet":                 core.H(s.describeChangeSet),
		"ExecuteChangeSet":                  core.H(s.executeChangeSet),
		"DeleteChangeSet":                   core.H(s.deleteChangeSet),
		"ListChangeSets":                    core.H(s.listChangeSets),
		"ListExports":                       core.H(s.listExports),
		"ListImports":                       core.H(s.listImports),
		"UpdateTerminationProtection":       core.H(s.terminationProtection),
		"SetStackPolicy":                    core.H(s.setPolicy),
		"GetStackPolicy":                    core.H(s.getPolicy),
		"DetectStackDrift":                  core.H(s.detectDrift),
		"DescribeStackDriftDetectionStatus": core.H(s.driftStatus),
		"CancelUpdateStack":                 core.H(s.noop),
		"ContinueUpdateRollback":            core.H(s.noop),
		"SignalResource":                    core.H(s.noop),
	}}
	if hub != nil {
		hub.Mount("/_pogi/cfn-response/", http.HandlerFunc(s.customCallback))
	}
	s.Reset()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "cloudformation", Cloud: core.AWS, Title: "CloudFormation", Signing: []string{"cloudformation"},
		Targets: []string{"CloudFormation"}, Protocol: "query"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	for _, stk := range st.Stacks { // operations cut short by a restart
		if strings.HasSuffix(stk.Status, "_IN_PROGRESS") && stk.Status != "REVIEW_IN_PROGRESS" {
			stk.Status, stk.Reason = strings.Replace(stk.Status, "_IN_PROGRESS", "_FAILED", 1), "pogi restarted during the operation"
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- lookups (lock held) ----

func (s *Service) find(c *core.Ctx, ref string) *Stack {
	for i := len(s.st.Stacks) - 1; i >= 0; i-- {
		st := s.st.Stacks[i]
		if st.ID == ref {
			return st
		}
		if st.Name == ref && st.Account == c.Account && st.Region == c.Region && st.Deleted.IsZero() {
			return st
		}
	}
	return nil
}

func (s *Service) mustFind(c *core.Ctx, ref string) (*Stack, error) {
	if st := s.find(c, ref); st != nil {
		return st, nil
	}
	return nil, validation("Stack with id %s does not exist", ref)
}

func busy(st *Stack) error {
	if strings.HasSuffix(st.Status, "_IN_PROGRESS") && st.Status != "REVIEW_IN_PROGRESS" {
		return validation("Stack:%s is in %s state and can not be updated.", st.ID, st.Status)
	}
	return nil
}

// unupdatable is busy, plus: a stack in ROLLBACK_COMPLETE (its create failed)
// can only be deleted, never updated.
func unupdatable(st *Stack) error {
	if st.Status == "ROLLBACK_COMPLETE" {
		return validation("Stack:%s is in %s state and can not be updated.", st.ID, st.Status)
	}
	return busy(st)
}

// exports collects Export names of live stacks; lock held.
func (s *Service) exports(account, region string) map[string]string {
	out := map[string]string{}
	for _, st := range s.st.Stacks {
		if st.Account == account && st.Region == region && st.Deleted.IsZero() {
			for _, o := range st.Outputs {
				if o.Export != "" {
					out[o.Export] = o.Value
				}
			}
		}
	}
	return out
}

// ---- inputs ----

type paramInput struct {
	ParameterKey     string
	ParameterValue   string
	UsePreviousValue bool
}

type stackInput struct {
	StackName                   string
	TemplateBody                string
	TemplateURL                 string
	UsePreviousTemplate         bool
	Parameters                  []paramInput
	Capabilities                []string
	Tags                        []Tag
	DisableRollback             bool
	OnFailure                   string
	RoleARN                     string
	NotificationARNs            []string
	EnableTerminationProtection bool
	ClientRequestToken          string
	TimeoutInMinutes            int
}

// templateBody returns the template text from TemplateBody or TemplateURL (read from pogi's S3).
func (s *Service) templateBody(body, tmplURL string) (string, error) {
	if body != "" || tmplURL == "" {
		return body, nil
	}
	u, err := url.Parse(tmplURL)
	if err != nil {
		return "", validation("TemplateURL must be a supported URL.")
	}
	path := strings.TrimPrefix(u.Path, "/")
	bucket, key, _ := strings.Cut(path, "/")
	if i := strings.Index(u.Host, ".s3"); i > 0 { // virtual-hosted style
		bucket, key = u.Host[:i], path
	} else if u.Scheme == "s3" { // s3://bucket/key
		bucket, key = u.Host, path
	}
	s3svc, _ := s.hub.Service("s3").(*s3.Service)
	if s3svc == nil {
		return "", validation("TemplateURL needs the S3 service, which is not enabled")
	}
	o, err := s3svc.Get(bucket, key)
	if err != nil {
		return "", validation("TemplateURL must reference a valid S3 object to which you have access. (%s/%s: %v)", bucket, key, err)
	}
	return string(o.Data), nil
}

func (s *Service) createStack(c *core.Ctx, in *stackInput) (any, error) {
	body, err := s.templateBody(in.TemplateBody, in.TemplateURL)
	if err != nil {
		return nil, err
	}
	tmpl, err := s.template(body)
	if err != nil {
		return nil, err
	}
	if err := s.checkTypes(tmpl); err != nil {
		return nil, err
	}
	if err := checkCapabilities(tmpl, in.Capabilities); err != nil {
		return nil, err
	}
	params, resolved, err := s.params(c, tmpl, in.Parameters, nil)
	if err == nil {
		err = s.rules(c, in.StackName, tmpl, params)
	}
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.find(c, in.StackName) != nil {
		s.mu.Unlock()
		return nil, core.Errorf(400, "AlreadyExistsException", "Stack [%s] already exists", in.StackName)
	}
	st := s.newStack(c, in.StackName, body, in.Tags, in.Capabilities)
	st.DisableRollback, st.OnFailure, st.Protected = in.DisableRollback, in.OnFailure, in.EnableTerminationProtection
	st.Params, st.Status = resolved, "CREATE_IN_PROGRESS"
	s.mu.Unlock()
	core.Go("cloudformation.runCreate", func() { s.runCreate(st, tmpl, params) })
	return map[string]string{"StackId": st.ID}, nil
}

// newStack registers an empty stack; lock held.
func (s *Service) newStack(c *core.Ctx, name, body string, tags []Tag, caps []string) *Stack {
	st := &Stack{ID: core.ARN("cloudformation", c.Region, c.Account, "stack/"+name+"/"+core.UUID()), Name: name, Account: c.Account,
		Region: c.Region, Body: body, Tags: tags, Capabilities: caps, Created: time.Now().UTC(), Resources: map[string]*Resource{}}
	if t, err := parseTemplate(body); err == nil { // no snippet fetch under the lock
		st.Description = str(t["Description"])
	}
	s.st.Stacks = append(s.st.Stacks, st)
	return st
}

func (s *Service) drop(st *Stack) {
	for i, x := range s.st.Stacks {
		if x == st {
			s.st.Stacks = append(s.st.Stacks[:i], s.st.Stacks[i+1:]...)
			return
		}
	}
}

func (s *Service) updateStack(c *core.Ctx, in *stackInput) (any, error) {
	s.mu.Lock()
	st, err := s.mustFind(c, in.StackName)
	if err == nil {
		err = unupdatable(st)
	}
	var prevBody string
	var prevParams []Param
	if err == nil {
		prevBody, prevParams = st.Body, st.Params
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	body := prevBody
	if !in.UsePreviousTemplate {
		if body, err = s.templateBody(in.TemplateBody, in.TemplateURL); err != nil {
			return nil, err
		}
	}
	tmpl, err := s.template(body)
	if err != nil {
		return nil, err
	}
	if err := s.checkTypes(tmpl); err != nil {
		return nil, err
	}
	if err := checkCapabilities(tmpl, in.Capabilities); err != nil {
		return nil, err
	}
	params, resolved, err := s.params(c, tmpl, in.Parameters, prevParams)
	if err == nil {
		err = s.rules(c, in.StackName, tmpl, params)
	}
	if err != nil {
		return nil, err
	}
	if body == prevBody && sameParams(resolved, prevParams) {
		return nil, validation("No updates are to be performed.")
	}
	s.mu.Lock()
	if err := unupdatable(st); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	st.Status = "UPDATE_IN_PROGRESS"
	if in.Tags != nil {
		st.Tags = in.Tags
	}
	s.mu.Unlock()
	core.Go("cloudformation.runUpdate", func() { s.runUpdate(st, body, resolved, tmpl, params) })
	return map[string]string{"StackId": st.ID}, nil
}

func sameParams(a, b []Param) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || a[i].Value != b[i].Value || a[i].Resolved != b[i].Resolved {
			return false
		}
	}
	return true
}

func (s *Service) deleteStack(c *core.Ctx, in *struct {
	StackName       string
	RetainResources []string
}) (any, error) {
	s.mu.Lock()
	st := s.find(c, in.StackName)
	if st == nil || !st.Deleted.IsZero() {
		s.mu.Unlock()
		return nil, nil // deleting a missing stack succeeds
	}
	if st.Protected {
		s.mu.Unlock()
		return nil, validation("Stack [%s] cannot be deleted while TerminationProtection is enabled", st.Name)
	}
	if st.Status == "DELETE_IN_PROGRESS" {
		s.mu.Unlock()
		return nil, nil
	}
	if err := busy(st); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	st.Status = "DELETE_IN_PROGRESS"
	s.mu.Unlock()
	core.Go("cloudformation.runDelete", func() { s.runDelete(st, in.RetainResources) })
	return nil, nil
}

// ---- outputs ----

type paramOut struct {
	ParameterKey   string
	ParameterValue string
	ResolvedValue  string `json:",omitempty"`
}

type outputOut struct {
	OutputKey   string
	OutputValue string
	Description string `json:",omitempty"`
	ExportName  string `json:",omitempty"`
}

type stackOut struct {
	StackId                     string
	StackName                   string
	ChangeSetId                 string `json:",omitempty"`
	Description                 string `json:",omitempty"`
	Parameters                  []paramOut
	CreationTime                core.Time
	LastUpdatedTime             *core.Time `json:",omitempty"`
	DeletionTime                *core.Time `json:",omitempty"`
	StackStatus                 string
	StackStatusReason           string `json:",omitempty"`
	DisableRollback             bool
	NotificationARNs            []string
	Capabilities                []string
	Outputs                     []outputOut
	Tags                        []Tag
	EnableTerminationProtection bool
	ParentId                    string `json:",omitempty"`
	DriftInformation            struct{ StackDriftStatus string }
}

func (st *Stack) out() stackOut {
	o := stackOut{StackId: st.ID, StackName: st.Name, Description: st.Description, CreationTime: core.Time(st.Created),
		StackStatus: st.Status, StackStatusReason: st.Reason, DisableRollback: st.DisableRollback, Capabilities: st.Capabilities,
		Tags: st.Tags, EnableTerminationProtection: st.Protected, ParentId: st.Parent, NotificationARNs: []string{}}
	o.DriftInformation.StackDriftStatus = "NOT_CHECKED"
	for _, p := range st.Params {
		v := p.Value
		if p.NoEcho {
			v = "****"
		}
		o.Parameters = append(o.Parameters, paramOut{p.Key, v, p.Resolved})
	}
	for _, out := range st.Outputs {
		o.Outputs = append(o.Outputs, outputOut{out.Key, out.Value, out.Description, out.Export})
	}
	if !st.Updated.IsZero() {
		t := core.Time(st.Updated)
		o.LastUpdatedTime = &t
	}
	if !st.Deleted.IsZero() {
		t := core.Time(st.Deleted)
		o.DeletionTime = &t
	}
	return o
}

func (s *Service) describeStacks(c *core.Ctx, in *struct{ StackName, NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []stackOut
	if in.StackName != "" {
		st, err := s.mustFind(c, in.StackName)
		if err != nil {
			return nil, err
		}
		list = append(list, st.out())
	} else {
		for i := len(s.st.Stacks) - 1; i >= 0; i-- {
			st := s.st.Stacks[i]
			if st.Account == c.Account && st.Region == c.Region && st.Deleted.IsZero() {
				list = append(list, st.out())
			}
		}
	}
	return map[string]any{"Stacks": list}, nil
}

func (s *Service) listStacks(c *core.Ctx, in *struct{ StackStatusFilter []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type summary struct {
		StackId, StackName, StackStatus string
		TemplateDescription             string `json:",omitempty"`
		CreationTime                    core.Time
		DeletionTime                    *core.Time `json:",omitempty"`
	}
	var list []summary
	for i := len(s.st.Stacks) - 1; i >= 0; i-- {
		st := s.st.Stacks[i]
		if st.Account != c.Account || st.Region != c.Region || len(in.StackStatusFilter) > 0 && !contains(in.StackStatusFilter, st.Status) {
			continue
		}
		sm := summary{StackId: st.ID, StackName: st.Name, StackStatus: st.Status, TemplateDescription: st.Description, CreationTime: core.Time(st.Created)}
		if !st.Deleted.IsZero() {
			t := core.Time(st.Deleted)
			sm.DeletionTime = &t
		}
		list = append(list, sm)
	}
	return map[string]any{"StackSummaries": list}, nil
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

type eventOut struct {
	StackId, EventId, StackName, LogicalResourceId string
	PhysicalResourceId                             string `json:",omitempty"`
	ResourceType                                   string
	Timestamp                                      core.Time
	ResourceStatus                                 string
	ResourceStatusReason                           string `json:",omitempty"`
}

func (s *Service) describeEvents(c *core.Ctx, in *struct{ StackName, NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	var list []eventOut
	for i := len(st.Events) - 1; i >= 0; i-- {
		e := st.Events[i]
		list = append(list, eventOut{st.ID, e.ID, st.Name, e.LogicalID, e.PhysicalID, e.Type, core.Time(e.Time), e.Status, e.Reason})
	}
	return map[string]any{"StackEvents": list}, nil
}

type resourceOut struct {
	StackName, StackId, LogicalResourceId string
	PhysicalResourceId                    string `json:",omitempty"`
	ResourceType                          string
	Timestamp                             *core.Time `json:",omitempty"`
	LastUpdatedTimestamp                  *core.Time `json:",omitempty"`
	ResourceStatus                        string
	ResourceStatusReason                  string                                     `json:",omitempty"`
	DriftInformation                      *struct{ StackResourceDriftStatus string } `json:",omitempty"`
}

func (st *Stack) resources(logical, physical string) []resourceOut {
	var out []resourceOut
	for _, id := range core.SortedKeys(st.Resources) {
		r := st.Resources[id]
		if logical != "" && id != logical || physical != "" && r.PhysicalID != physical {
			continue
		}
		t := core.Time(r.Updated)
		out = append(out, resourceOut{StackName: st.Name, StackId: st.ID, LogicalResourceId: id, PhysicalResourceId: r.PhysicalID,
			ResourceType: r.Type, Timestamp: &t, ResourceStatus: r.Status, ResourceStatusReason: r.Reason})
	}
	return out
}

func (s *Service) describeResources(c *core.Ctx, in *struct{ StackName, LogicalResourceId, PhysicalResourceId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.StackName == "" && in.PhysicalResourceId != "" {
		for _, st := range s.st.Stacks {
			if out := st.resources("", in.PhysicalResourceId); len(out) > 0 && st.Deleted.IsZero() {
				return map[string]any{"StackResources": out}, nil
			}
		}
		return nil, validation("Stack for %s does not exist", in.PhysicalResourceId)
	}
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"StackResources": st.resources(in.LogicalResourceId, in.PhysicalResourceId)}, nil
}

func (s *Service) describeResource(c *core.Ctx, in *struct{ StackName, LogicalResourceId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	out := st.resources(in.LogicalResourceId, "")
	if len(out) == 0 {
		return nil, validation("Resource %s does not exist for stack %s", in.LogicalResourceId, in.StackName)
	}
	d := out[0]
	d.LastUpdatedTimestamp, d.Timestamp = d.Timestamp, nil
	return map[string]any{"StackResourceDetail": d}, nil
}

func (s *Service) listResources(c *core.Ctx, in *struct{ StackName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	list := st.resources("", "")
	for i := range list {
		list[i].LastUpdatedTimestamp, list[i].Timestamp = list[i].Timestamp, nil
		list[i].StackName, list[i].StackId = "", ""
		list[i].DriftInformation = &struct{ StackResourceDriftStatus string }{"NOT_CHECKED"}
	}
	return map[string]any{"StackResourceSummaries": list}, nil
}

func (s *Service) getTemplate(c *core.Ctx, in *struct{ StackName, ChangeSetName, TemplateStage string }) (any, error) {
	s.mu.Lock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	body := st.Body
	if in.ChangeSetName != "" {
		cs := st.changeSet(in.ChangeSetName)
		if cs == nil {
			s.mu.Unlock()
			return nil, core.Errorf(404, "ChangeSetNotFound", "ChangeSet [%s] does not exist", in.ChangeSetName)
		}
		body = cs.Body
	}
	s.mu.Unlock()
	if in.TemplateStage == "Processed" {
		// ponytail: AWS keeps the processed text; this reads the include snippets from S3 and expands SAM again
		if tmpl, err := s.template(body); err == nil && strings.Contains(body, "Transform") {
			b, _ := json.Marshal(tmpl)
			body = string(b)
		}
	}
	return map[string]any{"TemplateBody": body, "StagesAvailable": []string{"Original", "Processed"}}, nil
}

type paramDecl struct {
	ParameterKey  string
	DefaultValue  string `json:",omitempty"`
	ParameterType string `json:",omitempty"`
	NoEcho        bool
	Description   string `json:",omitempty"`
}

func declarations(tmpl map[string]any) []paramDecl {
	defs, _ := tmpl["Parameters"].(map[string]any)
	var out []paramDecl
	for _, k := range core.SortedKeys(defs) {
		d, _ := defs[k].(map[string]any)
		out = append(out, paramDecl{k, str(d["Default"]), str(d["Type"]), str(d["NoEcho"]) == "true", str(d["Description"])})
	}
	return out
}

func (s *Service) templateSummary(c *core.Ctx, in *struct{ TemplateBody, TemplateURL, StackName string }) (any, error) {
	body := in.TemplateBody
	if in.StackName != "" {
		s.mu.Lock()
		st, err := s.mustFind(c, in.StackName)
		if err == nil {
			body = st.Body
		}
		s.mu.Unlock()
		if err != nil {
			return nil, err
		}
	}
	body, err := s.templateBody(body, in.TemplateURL)
	if err != nil {
		return nil, err
	}
	tmpl, err := s.template(body)
	if err != nil {
		return nil, err
	}
	types := map[string]bool{}
	for _, def := range tmpl["Resources"].(map[string]any) {
		d, _ := def.(map[string]any)
		types[str(d["Type"])] = true
	}
	return map[string]any{"Parameters": declarations(tmpl), "Description": str(tmpl["Description"]), "Capabilities": neededCapabilities(tmpl),
		"ResourceTypes": core.SortedKeys(types), "Version": str(tmpl["AWSTemplateFormatVersion"])}, nil
}

func (s *Service) validateTemplate(c *core.Ctx, in *struct{ TemplateBody, TemplateURL string }) (any, error) {
	body, err := s.templateBody(in.TemplateBody, in.TemplateURL)
	if err != nil {
		return nil, err
	}
	tmpl, err := s.template(body)
	if err != nil {
		return nil, err
	}
	if err := s.checkTypes(tmpl); err != nil {
		return nil, err
	}
	if _, err := order(tmpl["Resources"].(map[string]any)); err != nil {
		return nil, err
	}
	out := map[string]any{"Parameters": declarations(tmpl), "Description": str(tmpl["Description"])}
	if caps := neededCapabilities(tmpl); len(caps) > 0 {
		out["Capabilities"] = caps
		out["CapabilitiesReason"] = "The following resource(s) require capabilities: [AWS::IAM::*]"
	}
	return out, nil
}

// neededCapabilities follows CloudFormation's IAM capability rules.
func neededCapabilities(tmpl map[string]any) []string {
	named, iam := false, false
	for _, def := range tmpl["Resources"].(map[string]any) {
		d, _ := def.(map[string]any)
		if t := str(d["Type"]); strings.HasPrefix(t, "AWS::IAM::") {
			iam = true
			props, _ := d["Properties"].(map[string]any)
			for _, k := range []string{"RoleName", "UserName", "GroupName", "ManagedPolicyName", "InstanceProfileName"} {
				if props[k] != nil {
					named = true
				}
			}
		}
	}
	switch {
	case named:
		return []string{"CAPABILITY_NAMED_IAM"}
	case iam:
		return []string{"CAPABILITY_IAM"}
	}
	return nil
}

// checkTypes refuses a template with a resource type pogi cannot create, as AWS does for an unknown type.
func (s *Service) checkTypes(tmpl map[string]any) error {
	var bad []string
	for _, def := range tmpl["Resources"].(map[string]any) {
		d, _ := def.(map[string]any)
		if t := str(d["Type"]); !s.supportedType(t) && !contains(bad, t) {
			bad = append(bad, t)
		}
	}
	if len(bad) == 0 {
		return nil
	}
	slices.Sort(bad)
	return validation("Template format error: Unrecognized resource types: [%s]. pogi does not support %s yet.", strings.Join(bad, ", "), strings.Join(bad, ", "))
}

func checkCapabilities(tmpl map[string]any, given []string) error {
	for _, need := range neededCapabilities(tmpl) {
		if !contains(given, need) && !(need == "CAPABILITY_IAM" && contains(given, "CAPABILITY_NAMED_IAM")) {
			return core.Errorf(400, "InsufficientCapabilitiesException", "Requires capabilities : [%s]", need)
		}
	}
	return nil
}

// ---- change sets ----

func (st *Stack) changeSet(ref string) *ChangeSet {
	for _, cs := range st.ChangeSets {
		if cs.Name == ref || cs.ID == ref {
			return cs
		}
	}
	return nil
}

// changeSetStack finds the stack of a change set given by name or ARN; lock held.
func (s *Service) changeSetStack(c *core.Ctx, stackRef, csRef string) (*Stack, *ChangeSet, error) {
	if stackRef == "" {
		for _, st := range s.st.Stacks {
			if cs := st.changeSet(csRef); cs != nil {
				return st, cs, nil
			}
		}
	} else if st := s.find(c, stackRef); st != nil {
		if cs := st.changeSet(csRef); cs != nil {
			return st, cs, nil
		}
	}
	return nil, nil, core.Errorf(404, "ChangeSetNotFound", "ChangeSet [%s] does not exist", csRef)
}

func (s *Service) createChangeSet(c *core.Ctx, in *struct {
	stackInput
	ChangeSetName string
	ChangeSetType string
	Description   string
}) (any, error) {
	s.mu.Lock()
	st := s.find(c, in.StackName)
	switch {
	case in.ChangeSetType == "CREATE" && st != nil && st.Status != "REVIEW_IN_PROGRESS":
		s.mu.Unlock()
		return nil, core.Errorf(400, "AlreadyExistsException", "Stack [%s] already exists and cannot be created again with the changeSet [%s].", in.StackName, in.ChangeSetName)
	case in.ChangeSetType != "CREATE" && st == nil:
		s.mu.Unlock()
		return nil, validation("Stack [%s] does not exist", in.StackName)
	case st != nil && st.changeSet(in.ChangeSetName) != nil:
		s.mu.Unlock()
		return nil, core.Errorf(400, "AlreadyExistsException", "ChangeSet [%s] already exists", in.ChangeSetName)
	}
	var prevBody string
	var prevParams []Param
	var current map[string]*Resource
	if st != nil {
		prevBody, prevParams = st.Body, st.Params
		current = map[string]*Resource{}
		for k, v := range st.Resources {
			current[k] = v
		}
		if err := unupdatable(st); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}
	s.mu.Unlock()
	body := prevBody
	var err error
	if !in.UsePreviousTemplate {
		if body, err = s.templateBody(in.TemplateBody, in.TemplateURL); err != nil {
			return nil, err
		}
	}
	tmpl, err := s.template(body)
	if err != nil {
		return nil, err
	}
	if err := s.checkTypes(tmpl); err != nil {
		return nil, err
	}
	if err := checkCapabilities(tmpl, in.Capabilities); err != nil {
		return nil, err
	}
	params, resolved, err := s.params(c, tmpl, in.Parameters, prevParams)
	if err == nil {
		err = s.rules(c, in.StackName, tmpl, params)
	}
	if err != nil {
		return nil, err
	}
	typ := in.ChangeSetType
	if typ == "" {
		typ = "UPDATE"
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if st == nil {
		st = s.newStack(c, in.StackName, body, in.Tags, in.Capabilities)
		st.Status, st.Body = "REVIEW_IN_PROGRESS", ""
		st.Events = append(st.Events, Event{ID: core.UUID(), LogicalID: st.Name, PhysicalID: st.ID, Type: "AWS::CloudFormation::Stack",
			Status: "REVIEW_IN_PROGRESS", Reason: "User Initiated", Time: time.Now().UTC()})
	}
	cs := &ChangeSet{ID: core.ARN("cloudformation", c.Region, c.Account, "changeSet/"+in.ChangeSetName+"/"+core.UUID()), Name: in.ChangeSetName,
		Type: typ, Status: "CREATE_COMPLETE", ExecStatus: "AVAILABLE", Description: in.Description, Body: body, Inputs: in.Parameters,
		Tags: in.Tags, Capabilities: in.Capabilities, Created: time.Now().UTC()}
	cs.Changes = s.diff(st, current, tmpl, params)
	if len(cs.Changes) == 0 && body == prevBody && sameParams(resolved, prevParams) {
		cs.Status, cs.ExecStatus = "FAILED", "UNAVAILABLE"
		cs.Reason = "The submitted information didn't contain changes. Submit different information to create a change set."
	}
	st.ChangeSets = append(st.ChangeSets, cs)
	return map[string]string{"Id": cs.ID, "StackId": st.ID}, nil
}

// diff compares a template with what a stack has deployed (best effort: props that
// depend on other resources' attributes are compared after evaluation when possible).
func (s *Service) diff(st *Stack, current map[string]*Resource, tmpl map[string]any, params map[string]any) []Change {
	resources, _ := tmpl["Resources"].(map[string]any)
	e := s.evaluator(st, tmpl, params)
	e.resources = current
	var out []Change
	for _, id := range core.SortedKeys(resources) {
		d, _ := resources[id].(map[string]any)
		typ := str(d["Type"])
		if cond := str(d["Condition"]); cond != "" {
			if ok, _ := e.condition(cond); !ok {
				continue
			}
		}
		cur := current[id]
		switch {
		case cur == nil || cur.PhysicalID == "":
			out = append(out, Change{"Add", id, "", typ, ""})
		case cur.Type != typ:
			out = append(out, Change{"Modify", id, cur.PhysicalID, typ, "True"})
		default:
			raw, err := e.eval(d["Properties"])
			props, _ := raw.(map[string]any)
			if props == nil {
				props = map[string]any{}
			}
			if err == nil {
				err = s.resolveDynamic(st, props)
			}
			if err != nil || !equal(props, cur.Props) {
				out = append(out, Change{"Modify", id, cur.PhysicalID, typ, "Conditional"})
			}
		}
	}
	for _, id := range core.SortedKeys(current) {
		if _, ok := resources[id]; !ok {
			out = append(out, Change{"Remove", id, current[id].PhysicalID, current[id].Type, ""})
		}
	}
	return out
}

func (s *Service) describeChangeSet(c *core.Ctx, in *struct{ ChangeSetName, StackName, NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, cs, err := s.changeSetStack(c, in.StackName, in.ChangeSetName)
	if err != nil {
		return nil, err
	}
	type resourceChange struct {
		Action             string
		LogicalResourceId  string
		PhysicalResourceId string `json:",omitempty"`
		ResourceType       string
		Replacement        string `json:",omitempty"`
		Scope              []string
	}
	type change struct {
		Type           string
		ResourceChange resourceChange
	}
	var changes []change
	for _, ch := range cs.Changes {
		rc := resourceChange{ch.Action, ch.LogicalID, ch.PhysicalID, ch.Type, ch.Replacement, nil}
		if ch.Action == "Modify" {
			rc.Scope = []string{"Properties"}
		}
		changes = append(changes, change{"Resource", rc})
	}
	var params []paramOut
	for _, p := range cs.Inputs {
		params = append(params, paramOut{ParameterKey: p.ParameterKey, ParameterValue: p.ParameterValue})
	}
	return map[string]any{"ChangeSetName": cs.Name, "ChangeSetId": cs.ID, "StackId": st.ID, "StackName": st.Name, "Description": cs.Description,
		"Parameters": params, "CreationTime": core.Time(cs.Created), "ExecutionStatus": cs.ExecStatus, "Status": cs.Status,
		"StatusReason": cs.Reason, "Capabilities": cs.Capabilities, "Tags": cs.Tags, "Changes": changes, "NotificationARNs": []string{},
		"IncludeNestedStacks": false}, nil
}

func (s *Service) executeChangeSet(c *core.Ctx, in *struct{ ChangeSetName, StackName string }) (any, error) {
	s.mu.Lock()
	st, cs, err := s.changeSetStack(c, in.StackName, in.ChangeSetName)
	if err == nil && cs.ExecStatus != "AVAILABLE" {
		err = core.Errorf(400, "InvalidChangeSetStatus", "ChangeSet [%s] cannot be executed in its current status of [%s]", cs.ID, cs.ExecStatus)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	cs.ExecStatus = "EXECUTE_IN_PROGRESS"
	prevParams, creating := st.Params, st.Status == "REVIEW_IN_PROGRESS"
	s.mu.Unlock()
	tmpl, err := s.template(cs.Body)
	if err != nil {
		return nil, err
	}
	params, resolved, err := s.params(c, tmpl, cs.Inputs, prevParams)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	st.Capabilities = cs.Capabilities
	if cs.Tags != nil {
		st.Tags = cs.Tags
	}
	if creating {
		st.Body, st.Params, st.Description, st.Status = cs.Body, resolved, str(tmpl["Description"]), "CREATE_IN_PROGRESS"
	} else {
		st.Status = "UPDATE_IN_PROGRESS"
	}
	for _, other := range st.ChangeSets { // executing one change set obsoletes the others
		if other != cs {
			other.ExecStatus = "OBSOLETE"
		}
	}
	s.mu.Unlock()
	core.Go("cloudformation.executeChangeSet", func() {
		if creating {
			s.runCreate(st, tmpl, params)
		} else {
			s.runUpdate(st, cs.Body, resolved, tmpl, params)
		}
		s.mu.Lock()
		cs.ExecStatus = "EXECUTE_COMPLETE"
		if strings.Contains(st.Status, "ROLLBACK") || strings.HasSuffix(st.Status, "FAILED") {
			cs.ExecStatus = "EXECUTE_FAILED"
		}
		s.mu.Unlock()
	})
	return nil, nil
}

func (s *Service) deleteChangeSet(c *core.Ctx, in *struct{ ChangeSetName, StackName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, cs, err := s.changeSetStack(c, in.StackName, in.ChangeSetName)
	if err != nil {
		return nil, err
	}
	for i, x := range st.ChangeSets {
		if x == cs {
			st.ChangeSets = append(st.ChangeSets[:i], st.ChangeSets[i+1:]...)
		}
	}
	if st.Status == "REVIEW_IN_PROGRESS" && len(st.ChangeSets) == 0 {
		s.drop(st)
	}
	return nil, nil
}

func (s *Service) listChangeSets(c *core.Ctx, in *struct{ StackName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	type summary struct {
		StackId, StackName, ChangeSetId, ChangeSetName, ExecutionStatus, Status string
		StatusReason                                                            string `json:",omitempty"`
		CreationTime                                                            core.Time
		Description                                                             string `json:",omitempty"`
	}
	var list []summary
	for _, cs := range st.ChangeSets {
		list = append(list, summary{st.ID, st.Name, cs.ID, cs.Name, cs.ExecStatus, cs.Status, cs.Reason, core.Time(cs.Created), cs.Description})
	}
	return map[string]any{"Summaries": list}, nil
}

func (s *Service) listExports(c *core.Ctx, in *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type export struct{ ExportingStackId, Name, Value string }
	var list []export
	for _, st := range s.st.Stacks {
		if st.Account != c.Account || st.Region != c.Region || !st.Deleted.IsZero() {
			continue
		}
		for _, o := range st.Outputs {
			if o.Export != "" {
				list = append(list, export{st.ID, o.Export, o.Value})
			}
		}
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Name < list[j].Name })
	return map[string]any{"Exports": list}, nil
}

func (s *Service) listImports(c *core.Ctx, in *struct{ ExportName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for _, st := range s.st.Stacks {
		if st.Account == c.Account && st.Region == c.Region && st.Deleted.IsZero() && strings.Contains(st.Body, in.ExportName) {
			names = append(names, st.Name)
		}
	}
	if len(names) == 0 {
		return nil, validation("Export '%s' is not imported by any stack.", in.ExportName)
	}
	return map[string]any{"Imports": names}, nil
}

func (s *Service) terminationProtection(c *core.Ctx, in *struct {
	StackName                   string
	EnableTerminationProtection bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	st.Protected = in.EnableTerminationProtection
	return map[string]string{"StackId": st.ID}, nil
}

func (s *Service) setPolicy(c *core.Ctx, in *struct{ StackName, StackPolicyBody string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	st.Policy = in.StackPolicyBody
	return nil, nil
}

func (s *Service) getPolicy(c *core.Ctx, in *struct{ StackName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.mustFind(c, in.StackName)
	if err != nil {
		return nil, err
	}
	return map[string]string{"StackPolicyBody": st.Policy}, nil
}

func (s *Service) detectDrift(c *core.Ctx, in *struct{ StackName string }) (any, error) {
	return map[string]string{"StackDriftDetectionId": core.UUID()}, nil
}

func (s *Service) driftStatus(c *core.Ctx, in *struct{ StackDriftDetectionId string }) (any, error) {
	return map[string]any{"StackDriftDetectionId": in.StackDriftDetectionId, "StackDriftStatus": "IN_SYNC",
		"DetectionStatus": "DETECTION_COMPLETE", "Timestamp": core.Time(time.Now()), "DriftedStackResourceCount": 0}, nil
}

func (s *Service) noop(c *core.Ctx, in *struct{}) (any, error) { return nil, nil }

// ---- console ----

// Resources lists stacks.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Stacks", Columns: []string{"Name", "Region", "Account", "Status", "Resources", "Updated"}}
	for i := len(s.st.Stacks) - 1; i >= 0; i-- {
		st := s.st.Stacks[i]
		if !st.Deleted.IsZero() {
			continue
		}
		when := st.Created
		if !st.Updated.IsZero() {
			when = st.Updated
		}
		t.Rows = append(t.Rows, []string{st.Name, st.Region, st.Account, st.Status, strconvItoa(len(st.Resources)), when.Format(time.DateTime)})
	}
	return []core.Table{t}
}

// Inspect returns a stack with its resources, outputs and recent events.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := len(s.st.Stacks) - 1; i >= 0; i-- {
		st := s.st.Stacks[i]
		if st.Name == id || st.ID == id {
			events := st.Events
			if len(events) > 50 {
				events = events[len(events)-50:]
			}
			return map[string]any{"stack": st.out(), "resources": st.resources("", ""), "events": events}
		}
	}
	return nil
}
