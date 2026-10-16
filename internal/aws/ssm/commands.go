package ssm

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// Command is a Run Command request and what each target did with it.
type Command struct {
	ID              string              `json:"id"`
	Account         string              `json:"account"`
	Region          string              `json:"region"`
	DocumentName    string              `json:"documentName"`
	DocumentVersion string              `json:"documentVersion,omitempty"`
	Comment         string              `json:"comment,omitempty"`
	Parameters      map[string][]string `json:"parameters,omitempty"`
	InstanceIDs     []string            `json:"instanceIds"`
	Targets         []target            `json:"targets"`
	Requested       time.Time           `json:"requested"`
	Timeout         int                 `json:"timeout"`
	MaxConcurrency  string              `json:"maxConcurrency,omitempty"`
	MaxErrors       string              `json:"maxErrors,omitempty"`
	ServiceRole     string              `json:"serviceRole,omitempty"`
	S3Bucket        string              `json:"s3Bucket,omitempty"`
	S3Prefix        string              `json:"s3Prefix,omitempty"`
	Notification    json.RawMessage     `json:"notification,omitempty"`
	CloudWatchOut   json.RawMessage     `json:"cloudWatchOut,omitempty"`
	Invocations     []*Invocation       `json:"invocations"`
}

// Invocation is what one instance did with a command; it has a plugin per step of the document.
type Invocation struct {
	Instance string    `json:"instance"`
	Status   string    `json:"status"` // Pending, InProgress, Success, Failed, TimedOut or Cancelled
	Plugins  []*Plugin `json:"plugins"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
}

// Plugin is one step of a document on one instance.
type Plugin struct {
	Name   string    `json:"name"`
	Status string    `json:"status"`
	Code   int       `json:"code"`
	Output string    `json:"output"`
	Start  time.Time `json:"start"`
	End    time.Time `json:"end"`
}

const outputLimit = 24000 // GetCommandInvocation cuts the output here

func (c *Command) done(status string) bool {
	return !slices.Contains([]string{"Pending", "InProgress", "Delayed"}, status)
}

// abandon fails what was still waiting when pogi stopped.
func (c *Command) abandon() {
	for _, inv := range c.Invocations {
		if !c.done(inv.Status) {
			inv.Status = "Failed"
			for _, p := range inv.Plugins {
				if !c.done(p.Status) {
					p.Status = "Failed"
				}
			}
		}
	}
}

// status is the command's own status, from its invocations.
func (c *Command) status() string {
	var n = map[string]int{}
	for _, inv := range c.Invocations {
		n[inv.Status]++
	}
	switch total := len(c.Invocations); {
	case n["Pending"] == total:
		return "Pending"
	case n["Pending"]+n["InProgress"] > 0:
		return "InProgress"
	case n["Success"] == total:
		return "Success"
	case n["Cancelled"] == total:
		return "Cancelled"
	case n["TimedOut"] > 0 && n["Failed"] == 0:
		return "TimedOut"
	}
	return "Failed"
}

func (c *Command) out() map[string]any {
	st := c.status()
	errs, timeouts := 0, 0
	for _, inv := range c.Invocations {
		switch inv.Status {
		case "Failed", "Cancelled":
			errs++
		case "TimedOut":
			errs++
			timeouts++
		}
	}
	completed := 0
	for _, inv := range c.Invocations {
		if c.done(inv.Status) {
			completed++
		}
	}
	m := map[string]any{"CommandId": c.ID, "DocumentName": c.DocumentName, "Parameters": orParams(c.Parameters), "InstanceIds": c.InstanceIDs,
		"Targets": orEmptyTargets(c.Targets), "RequestedDateTime": core.Time(c.Requested), "ExpiresAfter": core.Time(c.Requested.Add(time.Duration(c.Timeout) * time.Second)),
		"Status": st, "StatusDetails": details(st), "TargetCount": len(c.Invocations), "CompletedCount": completed, "ErrorCount": errs,
		"DeliveryTimedOutCount": timeouts, "TimeoutSeconds": c.Timeout, "MaxConcurrency": defaultStr(c.MaxConcurrency, "50"), "MaxErrors": defaultStr(c.MaxErrors, "0")}
	for k, v := range map[string]string{"DocumentVersion": c.DocumentVersion, "Comment": c.Comment, "ServiceRole": c.ServiceRole,
		"OutputS3BucketName": c.S3Bucket, "OutputS3KeyPrefix": c.S3Prefix} {
		if v != "" {
			m[k] = v
		}
	}
	if len(c.Notification) > 0 {
		m["NotificationConfig"] = c.Notification
	}
	if len(c.CloudWatchOut) > 0 {
		m["CloudWatchOutputConfig"] = c.CloudWatchOut
	}
	return m
}

func defaultStr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

func orParams(p map[string][]string) map[string][]string {
	if p == nil {
		return map[string][]string{}
	}
	return p
}

// details is the StatusDetails that goes with a status.
func details(status string) string {
	if status == "Failed" {
		return "Failed"
	}
	return status
}

// ---- running ----

var subst = regexp.MustCompile(`\{\{\s*([A-Za-z0-9_.:-]+)\s*\}\}`)

// render fills {{ name }} in one value of a step's inputs. A value that is just a reference to a list parameter becomes the list.
func render(v any, params map[string][]string) []string {
	var in []string
	switch x := v.(type) {
	case string:
		in = []string{x}
	case []any:
		for _, e := range x {
			in = append(in, fmt.Sprint(e))
		}
	}
	var out []string
	for _, e := range in {
		if m := subst.FindStringSubmatch(e); m != nil && m[0] == e {
			out = append(out, params[m[1]]...)
			continue
		}
		out = append(out, subst.ReplaceAllStringFunc(e, func(r string) string {
			return strings.Join(params[subst.FindStringSubmatch(r)[1]], ",")
		}))
	}
	return out
}

// plan lists the steps a document runs on a Linux instance: the plugin name and the script. Steps with other
// actions are listed with an empty script and run as a no-op.
// ponytail: only aws:runShellScript runs; other actions finish at once without output.
func plan(d *Document, v *DocVersion, params map[string][]string) ([]struct{ Name, Script string }, error) {
	spec, err := parseDoc(v.Content, v.Format)
	if err != nil {
		return nil, err
	}
	merged := map[string][]string{}
	for name, p := range spec.Parameters {
		switch x := p.Default.(type) {
		case nil:
		case string:
			merged[name] = []string{x}
		case []any:
			merged[name] = render(x, nil)
		default:
			merged[name] = []string{fmt.Sprint(x)}
		}
	}
	for k, v := range params {
		merged[k] = v
	}
	var steps []struct{ Name, Script string }
	for _, st := range spec.MainSteps {
		step := struct{ Name, Script string }{Name: st.Name}
		if d.Owner == "Amazon" { // AWS's own documents show the action as the plugin name
			step.Name = st.Action
		}
		if st.Action == "aws:runShellScript" {
			step.Script = strings.Join(render(st.Inputs["runCommand"], merged), "\n")
			if dir := strings.Join(merged["workingDirectory"], ""); dir != "" {
				step.Script = "cd " + shellQuote(dir) + " || exit 1\n" + step.Script
			}
		}
		steps = append(steps, step)
	}
	return steps, nil
}

func shellQuote(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }

// running says whether the instance's container runs, which is what makes it an instance the agent can reach.
func (s *Service) running(id string) bool {
	d := s.hub.Docker()
	if !d.Available() {
		return false
	}
	info, err := d.Inspect(context.Background(), docker.Name("ec2", id))
	return err == nil && info.State.Running
}

// execute runs the document on one instance and records the outcome. It runs in its own goroutine.
func (s *Service) execute(cmd *Command, inv *Invocation, steps []struct{ Name, Script string }, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(s.ctx, timeout)
	defer cancel()
	update := func(f func()) bool { // false: the command was cancelled meanwhile
		s.mu.Lock()
		defer s.mu.Unlock()
		if inv.Status == "Cancelled" {
			return false
		}
		f()
		return true
	}
	if !update(func() { inv.Status, inv.Start = "InProgress", time.Now().UTC() }) {
		return
	}
	failed := false
	for i, step := range steps {
		p := inv.Plugins[i]
		if failed {
			update(func() { p.Status = "Cancelled" })
			continue
		}
		update(func() { p.Status, p.Start = "InProgress", time.Now().UTC() })
		out, code, err := []byte(nil), 0, error(nil)
		if step.Script != "" {
			out, code, err = s.hub.Docker().Exec(ctx, docker.Name("ec2", inv.Instance), "sh", "-c", step.Script)
		}
		status := "Success"
		switch {
		case ctx.Err() != nil && s.ctx.Err() == nil:
			status = "TimedOut"
		case err != nil:
			status, code, out = "Failed", -1, append(out, err.Error()...)
		case code != 0:
			status = "Failed"
		}
		text := string(out)
		if len(text) > outputLimit {
			text = text[:outputLimit-len("--output truncated--")] + "--output truncated--"
		}
		failed = status != "Success"
		if !update(func() { p.Status, p.Code, p.Output, p.End = status, code, text, time.Now().UTC() }) {
			return
		}
	}
	update(func() {
		inv.End = time.Now().UTC()
		inv.Status = "Success"
		for _, p := range inv.Plugins {
			if p.Status != "Success" && p.Status != "Cancelled" {
				inv.Status = p.Status
			}
		}
	})
}

// start makes the command's invocations and runs them. Lock held.
func (s *Service) start(cmd *Command, d *Document, v *DocVersion) error {
	steps, err := plan(d, v, cmd.Parameters)
	if err != nil {
		return err
	}
	timeout := time.Duration(max(cmd.Timeout, 1)) * time.Second
	if n, err := strconv.Atoi(strings.Join(cmd.Parameters["executionTimeout"], "")); err == nil && n > 0 {
		timeout = time.Duration(n) * time.Second
	}
	for _, id := range cmd.InstanceIDs {
		inv := &Invocation{Instance: id, Status: "Pending"}
		for _, st := range steps {
			inv.Plugins = append(inv.Plugins, &Plugin{Name: st.Name, Status: "Pending"})
		}
		cmd.Invocations = append(cmd.Invocations, inv)
		core.Go("ssm.execute", func() { s.execute(cmd, inv, steps, timeout) })
	}
	return nil
}

// targetIDs resolves a request's InstanceIds and Targets to instance ids.
func targetIDs(ids []string, targets []target) ([]string, error) {
	out := slices.Clone(ids)
	for _, t := range targets {
		if t.Key != "InstanceIds" {
			return nil, core.Errorf(400, "InvalidTarget", "pogi runs commands on instances named by InstanceIds; the target key %s is not supported.", t.Key)
		}
		out = append(out, t.Values...)
	}
	return slices.Compact(out), nil
}

// applyAssociation runs an association's document once, on its instances, and sets the association's status
// from the result. Lock held.
// ponytail: runs once, when the association is made; a schedule expression is stored but not repeated.
func (s *Service) applyAssociation(c *core.Ctx, a *Association, d *Document) {
	ids, err := targetIDs(nil, append([]target{{"InstanceIds", []string{a.InstanceID}}}, a.Targets...))
	ids = slices.DeleteFunc(ids, func(id string) bool { return id == "" })
	if err != nil || len(ids) == 0 || d.Type != "Command" {
		return
	}
	for _, id := range ids {
		if !s.running(id) {
			return
		}
	}
	v := d.version(a.DocumentVersion, "")
	if v == nil {
		return
	}
	cmd := &Command{ID: core.UUID(), Account: a.Account, Region: a.Region, DocumentName: d.Name, Parameters: a.Parameters, InstanceIDs: ids, Requested: time.Now().UTC(), Timeout: 3600}
	if s.start(cmd, d, v) != nil {
		return
	}
	core.Go("ssm.applyAssociation", func() { // the association is Success once every instance succeeded, Failed otherwise
		for {
			select {
			case <-s.ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			s.mu.Lock()
			if st := cmd.status(); cmd.done(st) {
				now := time.Now().UTC()
				a.Status, a.LastRun = map[bool]string{true: "Success", false: "Failed"}[st == "Success"], &now
				s.mu.Unlock()
				return
			}
			s.mu.Unlock()
		}
	})
}

// ---- operations ----

func (s *Service) sendCommand(c *core.Ctx, in *struct {
	DocumentName, DocumentVersion, Comment, MaxConcurrency, MaxErrors, ServiceRoleArn, OutputS3BucketName, OutputS3KeyPrefix string
	InstanceIds                                                                                                              []string
	Targets                                                                                                                  []target
	Parameters                                                                                                               map[string][]string
	TimeoutSeconds                                                                                                           int
	NotificationConfig, CloudWatchOutputConfig                                                                               json.RawMessage
}) (any, error) {
	switch {
	case in.DocumentName == "":
		return nil, validation("1 validation error detected: Value null at 'documentName' failed to satisfy constraint: Member must not be null")
	case len(in.InstanceIds) > 50:
		return nil, validation("1 validation error detected: Value '%v' at 'instanceIds' failed to satisfy constraint: Member must have length less than or equal to 50", in.InstanceIds)
	case in.TimeoutSeconds != 0 && (in.TimeoutSeconds < 30 || in.TimeoutSeconds > 2592000):
		return nil, validation("1 validation error detected: Value '%d' at 'timeoutSeconds' failed to satisfy constraint: Member must have value between 30 and 2592000", in.TimeoutSeconds)
	case len(in.InstanceIds) > 0 && len(in.Targets) > 0:
		return nil, core.Errorf(400, "ValidationException", "Use either InstanceIds or Targets, not both.")
	}
	ids, err := targetIDs(in.InstanceIds, in.Targets)
	switch {
	case err != nil:
		return nil, err
	case len(ids) == 0:
		return nil, core.Errorf(400, "InvalidTarget", "The command must name InstanceIds or Targets.")
	case len(ids) > 50:
		return nil, validation("1 validation error detected: Value '%v' at 'instanceIds' failed to satisfy constraint: Member must have length less than or equal to 50", ids)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.document(c, in.DocumentName)
	if d == nil || d.Type != "Command" {
		return nil, core.Errorf(400, "InvalidDocument", "The specified document does not exist or is not a Command document: %s.", in.DocumentName)
	}
	v := d.version(in.DocumentVersion, "")
	if v == nil {
		return nil, core.Errorf(400, "InvalidDocumentVersion", "The document version is not valid or does not exist.")
	}
	var bad []string
	for _, id := range ids {
		if !s.running(id) {
			bad = append(bad, id)
		}
	}
	if len(bad) > 0 {
		return nil, core.Errorf(400, "InvalidInstanceId", "Instances [%s] not in a valid state for account %s", strings.Join(bad, ", "), c.Account)
	}
	cmd := &Command{ID: core.UUID(), Account: c.Account, Region: c.Region, DocumentName: d.Name, DocumentVersion: in.DocumentVersion, Comment: in.Comment,
		Parameters: in.Parameters, InstanceIDs: ids, Targets: in.Targets, Requested: time.Now().UTC(), Timeout: defaultInt(in.TimeoutSeconds, 3600),
		MaxConcurrency: in.MaxConcurrency, MaxErrors: in.MaxErrors, ServiceRole: in.ServiceRoleArn, S3Bucket: in.OutputS3BucketName, S3Prefix: in.OutputS3KeyPrefix,
		Notification: in.NotificationConfig, CloudWatchOut: in.CloudWatchOutputConfig}
	if err := s.start(cmd, d, v); err != nil {
		return nil, err
	}
	s.st.Commands[key(c.Account, c.Region, cmd.ID)] = cmd
	if len(s.st.Commands) > 1000 { // the oldest finished commands go
		s.trim()
	}
	return map[string]any{"Command": cmd.out()}, nil
}

func defaultInt(v, def int) int {
	if v == 0 {
		return def
	}
	return v
}

// trim drops the oldest finished commands down to 900. Lock held.
func (s *Service) trim() {
	keys := core.SortedKeys(s.st.Commands)
	slices.SortFunc(keys, func(a, b string) int { return s.st.Commands[a].Requested.Compare(s.st.Commands[b].Requested) })
	for _, k := range keys[:len(keys)-900] {
		if c := s.st.Commands[k]; c.done(c.status()) {
			delete(s.st.Commands, k)
		}
	}
}

// commands lists the caller's commands, newest first. Lock held.
func (s *Service) commands(c *core.Ctx) []*Command {
	var out []*Command
	for _, cmd := range s.st.Commands {
		if cmd.Account == c.Account && cmd.Region == c.Region {
			out = append(out, cmd)
		}
	}
	slices.SortFunc(out, func(a, b *Command) int {
		return b.Requested.Compare(a.Requested)
	})
	return out
}

type cmdFilter struct{ Key, Value string }

// keep says whether a command or invocation passes the filters; at is when it was requested.
func keep(filters []cmdFilter, status, doc string, at time.Time) bool {
	for _, f := range filters {
		switch f.Key {
		case "Status":
			if f.Value != status {
				return false
			}
		case "DocumentName":
			if f.Value != doc {
				return false
			}
		case "InvokedAfter", "InvokedBefore":
			t, err := time.Parse(time.RFC3339, f.Value)
			if err == nil && (f.Key == "InvokedAfter") == at.Before(t) {
				return false
			}
		}
	}
	return true
}

func (s *Service) listCommands(c *core.Ctx, in *struct {
	CommandId, InstanceId, NextToken string
	MaxResults                       int
	Filters                          []cmdFilter
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Command
	for _, cmd := range s.commands(c) {
		if (in.CommandId == "" || cmd.ID == in.CommandId) && (in.InstanceId == "" || slices.Contains(cmd.InstanceIDs, in.InstanceId)) &&
			keep(in.Filters, cmd.status(), cmd.DocumentName, cmd.Requested) {
			list = append(list, cmd)
		}
	}
	start, end, next, err := page(len(list), in.NextToken, in.MaxResults, 50)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, cmd := range list[start:end] {
		out = append(out, cmd.out())
	}
	return map[string]any{"Commands": out, "NextToken": next}, nil
}

func (p *Plugin) out() map[string]any {
	m := map[string]any{"Name": p.Name, "Status": p.Status, "StatusDetails": details(p.Status), "ResponseCode": p.Code, "Output": p.Output,
		"StandardOutputUrl": "", "StandardErrorUrl": ""}
	if !p.Start.IsZero() {
		m["ResponseStartDateTime"] = core.Time(p.Start)
	}
	if !p.End.IsZero() {
		m["ResponseFinishDateTime"] = core.Time(p.End)
	}
	return m
}

func (s *Service) listCommandInvocations(c *core.Ctx, in *struct {
	CommandId, InstanceId, NextToken string
	MaxResults                       int
	Filters                          []cmdFilter
	Details                          bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	type row struct {
		cmd *Command
		inv *Invocation
	}
	var list []row
	for _, cmd := range s.commands(c) {
		if in.CommandId != "" && cmd.ID != in.CommandId {
			continue
		}
		for _, inv := range cmd.Invocations {
			if (in.InstanceId == "" || inv.Instance == in.InstanceId) && keep(in.Filters, inv.Status, cmd.DocumentName, cmd.Requested) {
				list = append(list, row{cmd, inv})
			}
		}
	}
	start, end, next, err := page(len(list), in.NextToken, in.MaxResults, 50)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, r := range list[start:end] {
		m := map[string]any{"CommandId": r.cmd.ID, "InstanceId": r.inv.Instance, "DocumentName": r.cmd.DocumentName, "Comment": r.cmd.Comment,
			"RequestedDateTime": core.Time(r.cmd.Requested), "Status": r.inv.Status, "StatusDetails": details(r.inv.Status),
			"StandardOutputUrl": "", "StandardErrorUrl": ""}
		if r.cmd.DocumentVersion != "" {
			m["DocumentVersion"] = r.cmd.DocumentVersion
		}
		if in.Details {
			plugins := []map[string]any{}
			for _, p := range r.inv.Plugins {
				plugins = append(plugins, p.out())
			}
			m["CommandPlugins"] = plugins
		}
		out = append(out, m)
	}
	return map[string]any{"CommandInvocations": out, "NextToken": next}, nil
}

func (s *Service) getCommandInvocation(c *core.Ctx, in *struct{ CommandId, InstanceId, PluginName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := s.st.Commands[key(c.Account, c.Region, in.CommandId)]
	var inv *Invocation
	if cmd != nil {
		if i := slices.IndexFunc(cmd.Invocations, func(x *Invocation) bool { return x.Instance == in.InstanceId }); i >= 0 {
			inv = cmd.Invocations[i]
		}
	}
	if inv == nil {
		return nil, core.Errorf(400, "InvocationDoesNotExist", "An error occurred (InvocationDoesNotExist) when calling the GetCommandInvocation operation")
	}
	status, code, output, name := inv.Status, 0, "", "Unknown"
	start, end := inv.Start, inv.End
	switch {
	case in.PluginName != "":
		i := slices.IndexFunc(inv.Plugins, func(p *Plugin) bool { return p.Name == in.PluginName })
		if i < 0 {
			return nil, core.Errorf(400, "InvalidPluginName", "The plugin name is not valid: %s.", in.PluginName)
		}
		p := inv.Plugins[i]
		status, code, output, name, start, end = p.Status, p.Code, p.Output, p.Name, p.Start, p.End
	default: // the whole invocation: the output of its plugins together, the code of the first that failed
		for _, p := range inv.Plugins {
			output += p.Output
			if code == 0 {
				code = p.Code
			}
		}
		if len(inv.Plugins) == 1 {
			name = inv.Plugins[0].Name
		}
	}
	m := map[string]any{"CommandId": cmd.ID, "InstanceId": inv.Instance, "Comment": cmd.Comment, "DocumentName": cmd.DocumentName, "PluginName": name,
		"ResponseCode": code, "Status": status, "StatusDetails": details(status), "StandardOutputContent": output, "StandardOutputUrl": "",
		"StandardErrorContent": "", "StandardErrorUrl": ""}
	if cmd.DocumentVersion != "" {
		m["DocumentVersion"] = cmd.DocumentVersion
	}
	if !start.IsZero() {
		m["ExecutionStartDateTime"] = start.Format(time.RFC3339Nano)
		m["ExecutionElapsedTime"] = "PT0S"
	}
	if !end.IsZero() {
		m["ExecutionEndDateTime"] = end.Format(time.RFC3339Nano)
		m["ExecutionElapsedTime"] = "PT" + strconv.FormatFloat(end.Sub(start).Seconds(), 'f', 3, 64) + "S"
	}
	if len(cmd.CloudWatchOut) > 0 {
		m["CloudWatchOutputConfig"] = cmd.CloudWatchOut
	}
	return m, nil
}

func (s *Service) cancelCommand(c *core.Ctx, in *struct {
	CommandId   string
	InstanceIds []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cmd := s.st.Commands[key(c.Account, c.Region, in.CommandId)]
	if cmd == nil {
		return nil, core.Errorf(400, "InvalidCommandId", "The command id %s does not exist.", in.CommandId)
	}
	for _, inv := range cmd.Invocations {
		if (len(in.InstanceIds) == 0 || slices.Contains(in.InstanceIds, inv.Instance)) && !cmd.done(inv.Status) {
			inv.Status, inv.End = "Cancelled", time.Now().UTC()
			for _, p := range inv.Plugins {
				if !cmd.done(p.Status) {
					p.Status = "Cancelled"
				}
			}
		}
	}
	return map[string]any{}, nil
}
