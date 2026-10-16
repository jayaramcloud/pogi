// Package codepipeline emulates AWS CodePipeline (JSON 1.1): V1 and V2
// pipelines with variables and namespaces, and executions that run their
// stages through pogi's own services: S3 and ECR sources, CodeBuild builds,
// CloudFormation, S3, CodeDeploy and ECS deploys, manual approvals, and
// Lambda and Step Functions invokes. Webhooks and custom action types are
// metadata.
package codepipeline

import (
	"cmp"
	"encoding/json"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3io"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the CodePipeline emulator.
type Service struct {
	hub  *core.Hub
	rpc  *core.RPC
	wg   sync.WaitGroup // running executions
	quit chan struct{}  // closed by Close: executions stop waiting
	mu   sync.Mutex
	st   state
	seen map[string]string // S3 source (pipeline key + object) → its size and time at the last look
	stop func()            // stops the source poller
}

type state struct {
	Pipelines   map[string]*Pipeline      `json:"pipelines"`   // account/region/name
	Executions  map[string]*Execution     `json:"executions"`  // account/region/id
	Webhooks    map[string]map[string]any `json:"webhooks"`    // account/region/name, as ListWebhooks shows it
	ActionTypes map[string]map[string]any `json:"actionTypes"` // account/region/category/provider/version
	Tags        map[string][]Tag          `json:"tags,omitempty"`
}

func newState() state {
	return state{Pipelines: map[string]*Pipeline{}, Executions: map[string]*Execution{}, Webhooks: map[string]map[string]any{},
		ActionTypes: map[string]map[string]any{}, Tags: map[string][]Tag{}}
}

// Pipeline is a pipeline's declaration, as given plus what CodePipeline
// fills in, and its metadata.
type Pipeline struct {
	Decl    map[string]any `json:"declaration"`
	Arn     string         `json:"arn"`
	Created time.Time      `json:"created"`
	Updated time.Time      `json:"updated"`
}

// Tag is a resource tag.
type Tag struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// decl is the part of a declaration executions follow.
type decl struct {
	Name           string
	RoleArn        string
	ArtifactStore  *store
	ArtifactStores map[string]store
	Stages         []stage
	Version        int
	Variables      []struct{ Name, DefaultValue string }
}

type store struct{ Type, Location string }

type stage struct {
	Name    string
	Actions []action
}

type action struct {
	Name         string
	ActionTypeId struct {
		Category string `json:"category"`
		Owner    string `json:"owner"`
		Provider string `json:"provider"`
		Version  string `json:"version"`
	}
	RunOrder        int
	Configuration   map[string]string
	InputArtifacts  []struct{ Name string }
	OutputArtifacts []struct{ Name string }
	Namespace       string
	Region          string
	RoleArn         string
}

func (p *Pipeline) decl() decl {
	var d decl
	copyJSON(&d, p.Decl)
	return d
}

func (p *Pipeline) version() int {
	v, _ := p.Decl["version"].(float64)
	return int(v)
}

// New creates the CodePipeline service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, quit: make(chan struct{}), st: newState(), seen: map[string]string{}}
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreatePipeline":         core.H(s.createPipeline),
		"GetPipeline":            core.H(s.getPipeline),
		"UpdatePipeline":         core.H(s.updatePipeline),
		"DeletePipeline":         core.H(s.deletePipeline),
		"ListPipelines":          core.H(s.listPipelines),
		"StartPipelineExecution": core.H(s.startExecution),
		"GetPipelineExecution":   core.H(s.getExecution),
		"ListPipelineExecutions": core.H(s.listExecutions),
		"StopPipelineExecution":  core.H(s.stopExecution),
		"RetryStageExecution":    core.H(s.retryStage),
		"GetPipelineState":       core.H(s.getState),
		"ListActionExecutions":   core.H(s.listActionExecutions),
		"PutApprovalResult":      core.H(s.putApprovalResult),
		"PutJobSuccessResult":    core.H(s.putJobSuccess),
		"PutJobFailureResult":    core.H(s.putJobFailure),
		"PutWebhook":             core.H(s.putWebhook),
		"ListWebhooks":           core.H(s.listWebhooks),
		"DeleteWebhook":          core.H(s.deleteWebhook),
		"CreateCustomActionType": core.H(s.createActionType),
		"ListActionTypes":        core.H(s.listActionTypes),
		"DeleteCustomActionType": core.H(s.deleteActionType),
		"TagResource":            core.H(s.tagResource),
		"UntagResource":          core.H(s.untagResource),
		"ListTagsForResource":    core.H(s.listTags),
	}}
	s.stop = core.Every(time.Second, s.pollSources)
	return s
}

// pollSources starts an execution when the S3 object of a source action with
// PollForSourceChanges "true" has changed. The first look only records the object.
// ponytail: looks every second where AWS polls about every minute; a pipeline that leaves
// the setting out is not polled (AWS polls by default), so tests that start runs by hand stay as they are.
func (s *Service) pollSources(time.Time) {
	type src struct{ account, region, pipeline, bucket, object string }
	var list []src
	s.mu.Lock()
	for k, p := range s.st.Pipelines {
		account, rest, _ := strings.Cut(k, "/")
		region, name, _ := strings.Cut(rest, "/")
		for _, st := range p.decl().Stages {
			for _, a := range st.Actions {
				if t := a.ActionTypeId; t.Category == "Source" && t.Owner == "AWS" && t.Provider == "S3" && a.Configuration["PollForSourceChanges"] == "true" {
					list = append(list, src{account, region, name, a.Configuration["S3Bucket"], a.Configuration["S3ObjectKey"]})
				}
			}
		}
	}
	s.mu.Unlock()
	for _, l := range list {
		o, err := (s3io.Client{Hub: s.hub, Account: l.account, Region: l.region}).Head(l.bucket, l.object)
		if err != nil {
			continue
		}
		k, sig := l.account+"/"+l.region+"/"+l.pipeline+"/"+l.bucket+"/"+l.object, o.Modified.String()+"/"+strconv.FormatInt(o.Size, 10)
		s.mu.Lock()
		prev, known := s.seen[k]
		s.seen[k] = sig
		s.mu.Unlock()
		if known && prev != sig {
			s.hub.Call(l.account, l.region, "CodePipeline_20150709", "StartPipelineExecution", map[string]any{"name": l.pipeline}, nil)
		}
	}
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "codepipeline", Cloud: core.AWS, Title: "CodePipeline", Targets: []string{"CodePipeline_20150709"},
		Signing: []string{"codepipeline"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

// Close stops running executions.
func (s *Service) Close() error {
	s.stop()
	close(s.quit)
	s.wg.Wait()
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.seen = map[string]string{}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores pipelines and executions; executions that were running
// fail, as nothing runs them any more.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	for _, e := range st.Executions {
		if e.Status == "InProgress" || e.Status == "Stopping" {
			e.Status, e.Summary = "Failed", "pogi restarted while the execution ran"
		}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func invalid(code, format string, a ...any) error { return core.Errorf(400, code, format, a...) }

func key(c *core.Ctx, parts ...string) string {
	return c.Account + "/" + c.Region + "/" + strings.Join(parts, "/")
}

// copyJSON deep-copies src into dst.
func copyJSON(dst, src any) {
	b, _ := json.Marshal(src)
	json.Unmarshal(b, dst)
}

// snap renders stored state under the lock, so running executions cannot
// change it while the response is written.
func snap(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

// ---- pipelines ----

// normalize checks a declaration and fills in what CodePipeline adds:
// pipeline type, execution mode and each action's run order.
func normalize(d map[string]any) error {
	var p decl
	copyJSON(&p, d)
	switch {
	case p.Name == "":
		return invalid("ValidationException", "pipeline.name must not be empty")
	case p.RoleArn == "":
		return invalid("InvalidStructureException", "pipeline.roleArn must not be empty")
	case p.ArtifactStore == nil && len(p.ArtifactStores) == 0:
		return invalid("InvalidStructureException", "Pipeline must have an artifactStore or artifactStores")
	case len(p.Stages) < 2:
		return invalid("InvalidStructureException", "Pipeline has only %d stage(s). There should be a minimum of 2 stages in a pipeline", len(p.Stages))
	}
	for i, stage := range p.Stages {
		if len(stage.Actions) == 0 {
			return invalid("InvalidStageDeclarationException", "Stage %s must have at least one action", stage.Name)
		}
		for _, a := range stage.Actions {
			if (a.ActionTypeId.Category == "Source") != (i == 0) {
				return invalid("InvalidStructureException", "Action %s: source actions belong in the first stage, and only there", a.Name)
			}
		}
	}
	for k, v := range map[string]string{"pipelineType": "V1", "executionMode": "SUPERSEDED"} {
		if str(d[k]) == "" {
			d[k] = v
		}
	}
	for _, stage := range d["stages"].([]any) {
		for _, a := range stage.(map[string]any)["actions"].([]any) {
			if a := a.(map[string]any); a["runOrder"] == nil {
				a["runOrder"] = 1
			}
		}
	}
	return nil
}

// pipeline finds a pipeline; lock held.
func (s *Service) pipeline(c *core.Ctx, name string) (*Pipeline, error) {
	if p := s.st.Pipelines[key(c, name)]; p != nil {
		return p, nil
	}
	return nil, invalid("PipelineNotFoundException", "Account '%s' does not have a pipeline with name '%s'", c.Account, name)
}

func (s *Service) createPipeline(c *core.Ctx, in *struct {
	Pipeline map[string]any
	Tags     []Tag
}) (any, error) {
	if in.Pipeline == nil {
		return nil, invalid("ValidationException", "pipeline must not be null")
	}
	if err := normalize(in.Pipeline); err != nil {
		return nil, err
	}
	name, _ := in.Pipeline["name"].(string)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Pipelines[key(c, name)] != nil {
		return nil, invalid("PipelineNameInUseException", "A pipeline with the name '%s' already exists in account '%s'", name, c.Account)
	}
	in.Pipeline["version"] = 1.0 // a JSON number, as version reads it
	now := time.Now()
	p := &Pipeline{Decl: in.Pipeline, Arn: c.ARN("codepipeline", name), Created: now, Updated: now}
	s.st.Pipelines[key(c, name)] = p
	if len(in.Tags) > 0 {
		s.st.Tags[p.Arn] = in.Tags
	}
	return map[string]any{"pipeline": snap(p.Decl), "tags": append([]Tag{}, in.Tags...)}, nil
}

func (s *Service) getPipeline(c *core.Ctx, in *struct {
	Name    string
	Version int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pipeline(c, in.Name)
	if err != nil {
		return nil, err
	}
	if in.Version != 0 && in.Version != p.version() {
		return nil, invalid("PipelineVersionNotFoundException", "Pipeline %s has no version %d; pogi keeps only the latest", in.Name, in.Version)
	}
	return map[string]any{"pipeline": snap(p.Decl), "metadata": map[string]any{"pipelineArn": p.Arn, "created": core.Time(p.Created),
		"updated": core.Time(p.Updated)}}, nil
}

func (s *Service) updatePipeline(c *core.Ctx, in *struct{ Pipeline map[string]any }) (any, error) {
	if in.Pipeline == nil {
		return nil, invalid("ValidationException", "pipeline must not be null")
	}
	if err := normalize(in.Pipeline); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	name, _ := in.Pipeline["name"].(string)
	p, err := s.pipeline(c, name)
	if err != nil {
		return nil, err
	}
	in.Pipeline["version"] = float64(p.version() + 1)
	p.Decl, p.Updated = in.Pipeline, time.Now()
	return map[string]any{"pipeline": snap(p.Decl)}, nil
}

func (s *Service) deletePipeline(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p := s.st.Pipelines[key(c, in.Name)]; p != nil {
		delete(s.st.Tags, p.Arn)
	}
	delete(s.st.Pipelines, key(c, in.Name))
	return nil, nil
}

func (s *Service) listPipelines(c *core.Ctx, _ *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []map[string]any{}
	for _, k := range core.SortedKeys(s.st.Pipelines) {
		if p := s.st.Pipelines[k]; strings.HasPrefix(k, key(c)) {
			list = append(list, map[string]any{"name": p.Decl["name"], "version": p.version(), "pipelineType": p.Decl["pipelineType"],
				"executionMode": p.Decl["executionMode"], "created": core.Time(p.Created), "updated": core.Time(p.Updated)})
		}
	}
	return map[string]any{"pipelines": list}, nil
}

// ---- executions ----

// startExecution runs a pipeline. ponytail: executions run side by side
// whatever the execution mode, and CreatePipeline does not start one.
func (s *Service) startExecution(c *core.Ctx, in *struct {
	Name      string
	Variables []struct{ Name, Value string }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pipeline(c, in.Name)
	if err != nil {
		return nil, err
	}
	d := p.decl()
	vars := map[string]string{}
	for _, v := range d.Variables {
		vars[v.Name] = v.DefaultValue
	}
	for _, v := range in.Variables {
		if _, ok := vars[v.Name]; !ok {
			return nil, invalid("ValidationException", "Variable %s is not declared in pipeline %s", v.Name, in.Name)
		}
		vars[v.Name] = v.Value
	}
	now := time.Now()
	e := &Execution{ID: core.UUID(), Pipeline: d.Name, Version: d.Version, Status: "InProgress", Started: now, Updated: now,
		Trigger: c.Principal, Stages: map[string]string{}, Artifacts: map[string]location{}, Outputs: map[string]map[string]string{"variables": vars},
		Mode: p.Decl["executionMode"].(string)}
	s.st.Executions[key(c, e.ID)] = e
	s.wg.Add(1)
	core.Go("codepipeline.run", func() { s.run(c.Account, c.Region, e, d, 0, "ALL_ACTIONS") })
	return map[string]string{"pipelineExecutionId": e.ID}, nil
}

// execution finds an execution of a pipeline; lock held.
func (s *Service) execution(c *core.Ctx, pipeline, id string) (*Execution, error) {
	if _, err := s.pipeline(c, pipeline); err != nil {
		return nil, err
	}
	if e := s.st.Executions[key(c, id)]; e != nil && e.Pipeline == pipeline {
		return e, nil
	}
	return nil, invalid("PipelineExecutionNotFoundException", "Pipeline execution %s for pipeline %s does not exist", id, pipeline)
}

func (s *Service) getExecution(c *core.Ctx, in *struct{ PipelineName, PipelineExecutionId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.execution(c, in.PipelineName, in.PipelineExecutionId)
	if err != nil {
		return nil, err
	}
	vars := []map[string]string{}
	for _, k := range core.SortedKeys(e.Outputs["variables"]) {
		vars = append(vars, map[string]string{"name": k, "resolvedValue": e.Outputs["variables"][k]})
	}
	return map[string]any{"pipelineExecution": snap(map[string]any{"pipelineName": e.Pipeline, "pipelineVersion": e.Version,
		"pipelineExecutionId": e.ID, "status": e.Status, "statusSummary": e.Summary, "artifactRevisions": e.Revisions, "variables": vars,
		"trigger": e.trigger(), "executionMode": e.Mode, "executionType": "STANDARD"})}, nil
}

func (e *Execution) trigger() map[string]string {
	return map[string]string{"triggerType": "StartPipelineExecution", "triggerDetail": e.Trigger}
}

// executions lists a pipeline's executions, newest first; lock held.
func (s *Service) executions(c *core.Ctx, pipeline string) []*Execution {
	var list []*Execution
	for k, e := range s.st.Executions {
		if strings.HasPrefix(k, key(c)) && e.Pipeline == pipeline {
			list = append(list, e)
		}
	}
	slices.SortFunc(list, func(a, b *Execution) int { return b.Started.Compare(a.Started) })
	return list
}

func (s *Service) listExecutions(c *core.Ctx, in *struct {
	PipelineName string
	MaxResults   int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.pipeline(c, in.PipelineName); err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, e := range s.executions(c, in.PipelineName) {
		sum := map[string]any{"pipelineExecutionId": e.ID, "status": e.Status, "statusSummary": e.Summary, "startTime": core.Time(e.Started),
			"lastUpdateTime": core.Time(e.Updated), "trigger": e.trigger(), "executionMode": e.Mode, "executionType": "STANDARD"}
		if e.Stopped != "" {
			sum["stopTrigger"] = map[string]string{"reason": e.Stopped}
		}
		var revs []map[string]string
		for _, r := range e.Revisions {
			revs = append(revs, map[string]string{"actionName": r.Action, "revisionId": r.RevisionID, "revisionSummary": r.RevisionSummary})
		}
		sum["sourceRevisions"] = revs
		list = append(list, sum)
	}
	if in.MaxResults > 0 && len(list) > in.MaxResults {
		list = list[:in.MaxResults]
	}
	return map[string]any{"pipelineExecutionSummaries": list}, nil
}

// stopExecution stops an execution at its next wait or stage. ponytail: a
// stop always abandons the actions that wait, as if abandon were set.
func (s *Service) stopExecution(c *core.Ctx, in *struct {
	PipelineName, PipelineExecutionId, Reason string
	Abandon                                   bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.execution(c, in.PipelineName, in.PipelineExecutionId)
	if err != nil {
		return nil, err
	}
	switch e.Status {
	case "Stopping":
		return nil, invalid("DuplicatedStopRequestException", "Execution %s is already stopping", e.ID)
	case "InProgress":
	default:
		return nil, invalid("PipelineExecutionNotStoppableException", "Execution %s is %s and cannot be stopped", e.ID, e.Status)
	}
	e.Status, e.Stopped, e.Updated = "Stopping", cmp.Or(in.Reason, "Stopped by the user"), time.Now()
	return map[string]string{"pipelineExecutionId": e.ID}, nil
}

func (s *Service) retryStage(c *core.Ctx, in *struct{ PipelineName, StageName, PipelineExecutionId, RetryMode string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, err := s.execution(c, in.PipelineName, in.PipelineExecutionId)
	if err != nil {
		return nil, err
	}
	p, _ := s.pipeline(c, in.PipelineName)
	d := p.decl()
	i := slices.IndexFunc(d.Stages, func(st stage) bool { return st.Name == in.StageName })
	switch {
	case i < 0:
		return nil, invalid("StageNotFoundException", "Stage %s not found in pipeline %s", in.StageName, in.PipelineName)
	case e.Status != "Failed" && e.Status != "Stopped" || e.Stages[in.StageName] != "Failed" && e.Stages[in.StageName] != "Stopped":
		return nil, invalid("StageNotRetryableException", "Stage %s of execution %s did not fail", in.StageName, e.ID)
	case d.Version != e.Version:
		return nil, invalid("NotLatestPipelineExecutionException", "The pipeline changed since execution %s", e.ID)
	}
	e.Status, e.Summary, e.Stopped, e.Updated = "InProgress", "", "", time.Now()
	s.wg.Add(1)
	mode := cmp.Or(in.RetryMode, "FAILED_ACTIONS")
	core.Go("codepipeline.run", func() { s.run(c.Account, c.Region, e, d, i, mode) })
	return map[string]string{"pipelineExecutionId": e.ID}, nil
}

// latest is the newest run of an action across executions; lock held.
func latest(list []*Execution, stage, name string) (*Execution, *ActionRun) {
	for _, e := range list {
		for i := len(e.Actions) - 1; i >= 0; i-- {
			if r := e.Actions[i]; r.Stage == stage && r.Name == name {
				return e, r
			}
		}
	}
	return nil, nil
}

func (s *Service) getState(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pipeline(c, in.Name)
	if err != nil {
		return nil, err
	}
	list := s.executions(c, in.Name)
	stages := []map[string]any{}
	for _, stage := range p.decl().Stages {
		st := map[string]any{"stageName": stage.Name, "inboundTransitionState": map[string]any{"enabled": true}}
		for _, e := range list {
			if status := e.Stages[stage.Name]; status != "" {
				st["latestExecution"] = map[string]string{"pipelineExecutionId": e.ID, "status": status}
				break
			}
		}
		actions := []map[string]any{}
		for _, a := range stage.Actions {
			as := map[string]any{"actionName": a.Name}
			if _, r := latest(list, stage.Name, a.Name); r != nil {
				x := map[string]any{"actionExecutionId": r.ID, "status": r.Status, "lastStatusChange": core.Time(r.Updated)}
				if r.Summary != "" {
					x["summary"] = r.Summary
				}
				if r.Token != "" && r.Status == "InProgress" {
					x["token"] = r.Token
				}
				if r.External != "" {
					x["externalExecutionId"] = r.External
				}
				if r.Error != nil {
					x["errorDetails"] = *r.Error
				}
				as["latestExecution"] = x
			}
			actions = append(actions, as)
		}
		st["actionStates"] = actions
		stages = append(stages, st)
	}
	return map[string]any{"pipelineName": in.Name, "pipelineVersion": p.version(), "stageStates": stages, "created": core.Time(p.Created),
		"updated": core.Time(p.Updated)}, nil
}

func (s *Service) listActionExecutions(c *core.Ctx, in *struct {
	PipelineName string
	Filter       struct{ PipelineExecutionId string }
	MaxResults   int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.pipeline(c, in.PipelineName); err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, e := range s.executions(c, in.PipelineName) {
		if in.Filter.PipelineExecutionId != "" && e.ID != in.Filter.PipelineExecutionId {
			continue
		}
		for i := len(e.Actions) - 1; i >= 0; i-- {
			r := e.Actions[i]
			result := map[string]any{"externalExecutionId": r.External, "externalExecutionSummary": r.Summary}
			if r.Error != nil {
				result["errorDetails"] = *r.Error
			}
			list = append(list, map[string]any{"pipelineExecutionId": e.ID, "actionExecutionId": r.ID, "pipelineVersion": e.Version,
				"stageName": r.Stage, "actionName": r.Name, "startTime": core.Time(r.Started), "lastUpdateTime": core.Time(r.Updated),
				"status": r.Status, "input": r.Input, "output": map[string]any{"outputArtifacts": r.Outputs, "executionResult": result,
					"outputVariables": r.Variables}})
		}
	}
	if in.MaxResults > 0 && len(list) > in.MaxResults {
		list = list[:in.MaxResults]
	}
	return map[string]any{"actionExecutionDetails": snap(list)}, nil
}

// ---- approvals and jobs ----

func (s *Service) putApprovalResult(c *core.Ctx, in *struct {
	PipelineName, StageName, ActionName, Token string
	Result                                     struct{ Summary, Status string }
}) (any, error) {
	if in.Result.Status != "Approved" && in.Result.Status != "Rejected" {
		return nil, invalid("ValidationException", "result.status must be Approved or Rejected")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.pipeline(c, in.PipelineName); err != nil {
		return nil, err
	}
	_, r := latest(s.executions(c, in.PipelineName), in.StageName, in.ActionName)
	switch {
	case r == nil:
		return nil, invalid("ActionNotFoundException", "Action %s in stage %s has not run", in.ActionName, in.StageName)
	case r.Token == "" || r.Token != in.Token:
		return nil, invalid("InvalidApprovalTokenException", "The approval token %s is not valid", in.Token)
	case r.Status != "InProgress" || r.Result != "":
		return nil, invalid("ApprovalAlreadyCompletedException", "The approval of action %s already completed", in.ActionName)
	}
	r.Result, r.ResultSummary = in.Result.Status, in.Result.Summary
	return map[string]any{"approvedAt": core.Time(time.Now())}, nil
}

// job finds the action run waiting for a job; lock held.
func (s *Service) job(c *core.Ctx, id string) (*ActionRun, error) {
	for k, e := range s.st.Executions {
		for _, r := range e.Actions {
			if r.Job != "" && r.Job == id && strings.HasPrefix(k, key(c)) {
				if r.Result != "" || r.Status != "InProgress" {
					return nil, invalid("InvalidJobStateException", "Job %s is not in progress", id)
				}
				return r, nil
			}
		}
	}
	return nil, invalid("JobNotFoundException", "Job %s does not exist", id)
}

func (s *Service) putJobSuccess(c *core.Ctx, in *struct {
	JobId            string
	OutputVariables  map[string]string
	ExecutionDetails struct{ Summary, ExternalExecutionId string }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.job(c, in.JobId)
	if err != nil {
		return nil, err
	}
	r.Result, r.ResultSummary, r.Variables = "Succeeded", in.ExecutionDetails.Summary, in.OutputVariables
	return nil, nil
}

func (s *Service) putJobFailure(c *core.Ctx, in *struct {
	JobId          string
	FailureDetails struct{ Type, Message string }
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.job(c, in.JobId)
	if err != nil {
		return nil, err
	}
	r.Result, r.ResultSummary = "Failed", cmp.Or(in.FailureDetails.Message, in.FailureDetails.Type)
	return nil, nil
}

// ---- webhooks and custom action types, as metadata ----

func (s *Service) putWebhook(c *core.Ctx, in *struct {
	Webhook map[string]any
	Tags    []Tag
}) (any, error) {
	name, _ := in.Webhook["name"].(string)
	if name == "" {
		return nil, invalid("ValidationException", "webhook.name must not be empty")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arn := c.ARN("codepipeline", "webhook:"+name)
	w := map[string]any{"definition": in.Webhook, "arn": arn, "url": "http://localhost:7644/webhooks/" + core.NewID()}
	if old := s.st.Webhooks[key(c, name)]; old != nil {
		w["url"] = old["url"]
	}
	s.st.Webhooks[key(c, name)] = w
	if len(in.Tags) > 0 {
		s.st.Tags[arn] = in.Tags
	}
	return map[string]any{"webhook": snap(w)}, nil
}

func (s *Service) listWebhooks(c *core.Ctx, _ *struct{ NextToken string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []map[string]any{}
	for _, k := range core.SortedKeys(s.st.Webhooks) {
		if strings.HasPrefix(k, key(c)) {
			list = append(list, s.st.Webhooks[k])
		}
	}
	return map[string]any{"webhooks": snap(list)}, nil
}

func (s *Service) deleteWebhook(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.st.Webhooks, key(c, in.Name))
	delete(s.st.Tags, c.ARN("codepipeline", "webhook:"+in.Name))
	return nil, nil
}

type actionTypeInput struct {
	Category, Provider, Version                 string
	Settings, ConfigurationProperties           json.RawMessage
	InputArtifactDetails, OutputArtifactDetails json.RawMessage
	Tags                                        []Tag
}

func (s *Service) createActionType(c *core.Ctx, in *actionTypeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := map[string]any{"id": map[string]string{"category": in.Category, "owner": "Custom", "provider": in.Provider, "version": in.Version},
		"settings": in.Settings, "actionConfigurationProperties": in.ConfigurationProperties,
		"inputArtifactDetails": in.InputArtifactDetails, "outputArtifactDetails": in.OutputArtifactDetails}
	s.st.ActionTypes[key(c, in.Category, in.Provider, in.Version)] = t
	if len(in.Tags) > 0 {
		s.st.Tags[c.ARN("codepipeline", "actiontype:Custom/"+in.Category+"/"+in.Provider+"/"+in.Version)] = in.Tags
	}
	return map[string]any{"actionType": snap(t), "tags": append([]Tag{}, in.Tags...)}, nil
}

func (s *Service) listActionTypes(c *core.Ctx, in *struct{ ActionOwnerFilter string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []map[string]any{}
	for _, k := range core.SortedKeys(s.st.ActionTypes) {
		if strings.HasPrefix(k, key(c)) && (in.ActionOwnerFilter == "" || in.ActionOwnerFilter == "Custom") {
			list = append(list, s.st.ActionTypes[k])
		}
	}
	return map[string]any{"actionTypes": snap(list)}, nil
}

func (s *Service) deleteActionType(c *core.Ctx, in *actionTypeInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.ActionTypes[key(c, in.Category, in.Provider, in.Version)] == nil {
		return nil, invalid("ActionTypeNotFoundException", "Custom action type %s/%s/%s not found", in.Category, in.Provider, in.Version)
	}
	delete(s.st.ActionTypes, key(c, in.Category, in.Provider, in.Version))
	return nil, nil
}

// ---- tags ----

// tagged checks that a pipeline, webhook or custom action type has this
// ARN and returns it whole: Terraform may leave the account out. Lock held.
func (s *Service) tagged(c *core.Ctx, arn string) (string, error) {
	_, region, account, res, _ := core.ParseARN(arn)
	c = &core.Ctx{Account: cmp.Or(account, c.Account), Region: cmp.Or(region, c.Region)}
	kind, name, _ := strings.Cut(res, ":")
	parts := strings.Split(name, "/") // Custom/Category/Provider/Version
	switch {
	case !strings.Contains(res, ":") && s.st.Pipelines[key(c, res)] != nil,
		kind == "webhook" && s.st.Webhooks[key(c, name)] != nil,
		kind == "actiontype" && len(parts) == 4 && s.st.ActionTypes[key(c, parts[1], parts[2], parts[3])] != nil:
		return c.ARN("codepipeline", res), nil
	}
	return "", invalid("ResourceNotFoundException", "The resource %s was not found", arn)
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	ResourceArn string
	Tags        []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	tags := s.st.Tags[arn]
	for _, t := range in.Tags {
		tags = append(slices.DeleteFunc(tags, func(x Tag) bool { return x.Key == t.Key }), t)
	}
	s.st.Tags[arn] = tags
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	ResourceArn string
	TagKeys     []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	s.st.Tags[arn] = slices.DeleteFunc(s.st.Tags[arn], func(x Tag) bool { return slices.Contains(in.TagKeys, x.Key) })
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, in *struct{ ResourceArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, in.ResourceArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"tags": append([]Tag{}, s.st.Tags[arn]...)}, nil
}

// ---- console ----

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	pipes := core.Table{Name: "Pipelines", Columns: []string{"Name", "Version", "Type", "Stages"}}
	for _, k := range core.SortedKeys(s.st.Pipelines) {
		p := s.st.Pipelines[k]
		var stages []string
		for _, st := range p.decl().Stages {
			stages = append(stages, st.Name)
		}
		pipes.Rows = append(pipes.Rows, []string{str(p.Decl["name"]), strconv.Itoa(p.version()), str(p.Decl["pipelineType"]), strings.Join(stages, " → ")})
	}
	execs := core.Table{Name: "Executions", Columns: []string{"ID", "Pipeline", "Status", "Started"}}
	for _, k := range core.SortedKeys(s.st.Executions) {
		e := s.st.Executions[k]
		execs.Rows = append(execs.Rows, []string{e.ID, e.Pipeline, e.Status, e.Started.Format(time.RFC3339)})
	}
	return []core.Table{pipes, execs}
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.st.Pipelines {
		if table == "Pipelines" && p.Decl["name"] == id {
			return snap(p)
		}
	}
	for _, e := range s.st.Executions {
		if e.ID == id {
			return snap(e)
		}
	}
	return nil
}

func str(v any) string {
	s, _ := v.(string)
	return s
}
