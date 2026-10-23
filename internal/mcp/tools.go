package mcp

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nrndev/pogi/internal/aws/ses"
	"github.com/nrndev/pogi/internal/aws/sns"
	"github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/azure/email"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/cost"
	"github.com/nrndev/pogi/internal/terraform"
)

// Tool inputs. The jsonschema tags become the argument descriptions agents read.
type (
	none       struct{}
	serviceArg struct {
		Service string `json:"service,omitempty" jsonschema:"pogi service name such as s3, sqs, dynamodb, lambda, pubsub or azblob (status lists them); omit for every service"`
	}
	inspectArg struct {
		Service string `json:"service" jsonschema:"pogi service name, e.g. sqs"`
		Table   string `json:"table" jsonschema:"table name from list_resources, e.g. Queues"`
		ID      string `json:"id" jsonschema:"first column of the row in list_resources, e.g. a queue name"`
	}
	logArg struct {
		Service    string `json:"service,omitempty" jsonschema:"only calls to this pogi service, e.g. sqs"`
		ErrorsOnly bool   `json:"errors_only,omitempty" jsonschema:"only calls that failed (HTTP status 400 or above)"`
		Query      string `json:"query,omitempty" jsonschema:"case-insensitive text matched against service, operation, path and error code"`
		Limit      int    `json:"limit,omitempty" jsonschema:"how many calls to return (default 50)"`
	}
	idArg struct {
		ID int64 `json:"id" jsonschema:"request id from request_log"`
	}
	limitArg struct {
		Limit int `json:"limit,omitempty" jsonschema:"maximum entries per list, newest first (default 20)"`
	}
	queueArg struct {
		Queue  string `json:"queue" jsonschema:"SQS queue name, URL or ARN"`
		Region string `json:"region,omitempty" jsonschema:"AWS region of the queue (default: pogi's region)"`
	}
	resetArg struct {
		Services []string `json:"services,omitempty" jsonschema:"pogi service names to clear, e.g. sqs and s3; omit to clear every service"`
	}
	snapArg struct {
		Name string `json:"name" jsonschema:"snapshot name: letters, digits, dot, underscore and dash"`
	}
	chaosArg struct {
		Service   string  `json:"service" jsonschema:"pogi service name or glob, e.g. dynamodb or s3; * for every service"`
		Op        string  `json:"op,omitempty" jsonschema:"operation name or glob, e.g. PutItem or Put*; omit for every operation"`
		Rate      float64 `json:"rate,omitempty" jsonschema:"probability between 0 and 1 that a matching call is affected; omit for every call"`
		Status    int     `json:"status,omitempty" jsonschema:"HTTP status to inject, e.g. 500, 503 or 429"`
		Code      string  `json:"code,omitempty" jsonschema:"error code to inject (default ThrottlingException for 429, else InternalFailure)"`
		LatencyMs int     `json:"latency_ms,omitempty" jsonschema:"latency to add, in milliseconds"`
	}
	stackArg struct {
		Name string `json:"name" jsonschema:"stack name or stack ID"`
	}
	deployArg struct {
		Name         string            `json:"name" jsonschema:"stack name; an existing stack with this name is updated"`
		TemplateBody string            `json:"template_body" jsonschema:"CloudFormation template, YAML or JSON"`
		Parameters   map[string]string `json:"parameters,omitempty" jsonschema:"template parameter values by parameter name"`
	}
	invokeArg struct {
		Name    string `json:"name" jsonschema:"Lambda function name (optionally name:alias) or ARN"`
		Payload string `json:"payload,omitempty" jsonschema:"the event, as a JSON document (default {})"`
		Region  string `json:"region,omitempty" jsonschema:"AWS region of the function (default: pogi's region)"`
	}
	logsArg struct {
		Name   string `json:"name" jsonschema:"Lambda function name"`
		Limit  int    `json:"limit,omitempty" jsonschema:"how many of the latest lines to return (default 100)"`
		Region string `json:"region,omitempty" jsonschema:"AWS region of the function (default: pogi's region)"`
	}
	tfCostArg struct {
		Path      string   `json:"path" jsonschema:"a Terraform folder or a plan.json (terraform show -json), on the machine pogi runs on"`
		VarFiles  []string `json:"var_files,omitempty" jsonschema:"extra .tfvars files, relative to the folder or absolute, applied in order"`
		UsageFile string   `json:"usage_file,omitempty" jsonschema:"a usage.yaml with usage numbers by resource address, and os: windows for VMs"`
	}
	sizesArg struct {
		ResourceType string  `json:"resource_type" jsonschema:"Terraform resource type, e.g. aws_instance, google_compute_instance, azurerm_linux_virtual_machine"`
		Region       string  `json:"region,omitempty" jsonschema:"cloud region, e.g. eu-west-1 (default: the cloud's usual region)"`
		OS           string  `json:"os,omitempty" jsonschema:"linux (default) or windows"`
		MinVCPU      float64 `json:"min_vcpu,omitempty" jsonschema:"only sizes with at least this many vCPUs"`
		MinMemoryGB  float64 `json:"min_memory_gb,omitempty" jsonschema:"only sizes with at least this much memory, in GB"`
	}
	seedArg struct {
		YAML string `json:"yaml" jsonschema:"YAML shaped like the resources section of pogi.yaml, e.g. aws: {s3: {buckets: [uploads]}, sqs: {queues: [jobs]}}"`
	}
)

func register(s *sdk.Server, h *core.Hub) {
	// ---- inspect ----
	add(s, "status", "Overview of the running pogi: version, uptime, default region, IAM and Lambda modes, and every enabled service with its cloud, resource count and request count.", true,
		func(none) (any, error) { return h.Status(), nil })

	add(s, "list_resources", "Resources that exist in pogi, as tables per service: {service: [{name, columns, rows}]}. Empty tables are left out. The first column of a row is the id inspect_resource takes.", true,
		func(in serviceArg) (any, error) {
			if in.Service != "" && h.Service(in.Service) == nil {
				return nil, unknown(h, in.Service)
			}
			out := map[string][]core.Table{}
			for svc, tables := range h.Resources(in.Service) {
				for _, t := range tables {
					if len(t.Rows) > 0 {
						out[svc] = append(out[svc], t)
					}
				}
			}
			return out, nil
		})

	add(s, "inspect_resource", "Everything about one resource: attributes, configuration, tags and, for some services, recent contents (queue messages, table items, stack events). Take service, table and id from list_resources.", true,
		func(in inspectArg) (any, error) {
			svc := h.Service(in.Service)
			if svc == nil {
				return nil, unknown(h, in.Service)
			}
			if i, ok := svc.(core.Inspectable); ok {
				if v := i.Inspect(in.Table, in.ID); v != nil {
					return obj{"resource": v}, nil
				}
			}
			return nil, fmt.Errorf("%s has no %s %q (list_resources shows the tables and ids)", in.Service, in.Table, in.ID)
		})

	add(s, "request_log", "Recent API calls pogi received (it keeps the last 1000), newest first: id, time, cloud, service, op, path, HTTP status, latency, error code, chaos and IAM decision. Bodies are left out; get_request has them. Use errors_only after a failing call.", true,
		func(in logArg) (any, error) {
			out := h.Requests.Query(0, positive(in.Limit, 50), in.Service, in.Query, in.ErrorsOnly)
			slices.Reverse(out)
			return obj{"requests": out}, nil
		})

	add(s, "get_request", "One API call from request_log in full: request headers and body, response body (secrets redacted), status, error code and IAM decision.", true,
		func(in idArg) (any, error) {
			if e, ok := h.Requests.Get(in.ID); ok {
				return e, nil
			}
			return nil, fmt.Errorf("request %d not found (the log keeps the last 1000)", in.ID)
		})

	add(s, "iam_decisions", "Recent IAM policy decisions from the request log, newest first: allow, or deny with the reason. pogi records them only with --iam soft (log) or --iam strict (log and deny).", true,
		func(in limitArg) (any, error) {
			var out []core.Entry
			for _, e := range slices.Backward(h.Requests.Query(0, 0, "", "", false)) {
				if len(out) == positive(in.Limit, 20) {
					break
				}
				if e.IAM != "" {
					out = append(out, e)
				}
			}
			res := obj{"mode": h.IAMMode(), "decisions": out}
			if m := h.IAMMode(); m != "soft" && m != "strict" {
				res["note"] = "IAM enforcement is off: start pogi with --iam soft to record decisions, or --iam strict to also deny"
			}
			return res, nil
		})

	add(s, "connection_info", "How to point code and tools at pogi: the endpoint, the web console, the environment variables for the AWS, Google Cloud and Azure SDKs, and hints for the AWS CLI, Terraform and CDK.", true,
		func(none) (any, error) {
			return obj{
				"endpoint": h.URL, "console": h.URL + "/_pogi/", "mcp": h.URL + "/_pogi/mcp",
				"region": h.Cfg.Region, "account": core.DefaultAccount, "env": core.Env(h.URL, h.Cfg.Region),
				"hints": obj{
					"shell":     `eval "$(pogi env)" exports env`,
					"aws_cli":   "pogi aws <args>, or the aws CLI with env set",
					"terraform": "pogi tf > pogi_override.tf writes an AWS provider block with every endpoint pointed at pogi",
					"cdk":       "with env set, cdk bootstrap and cdk deploy work unchanged; run pogi lambda-sdks once so custom resources find the AWS SDKs",
				},
			}, nil
		})

	add(s, "peek_queue", "Messages in an SQS queue without receiving them (visibility and receive counts are untouched), each with its state: visible, inflight or delayed.", true,
		func(in queueArg) (any, error) {
			q, _ := h.Service("sqs").(*sqs.Service)
			if q == nil {
				return nil, errors.New("sqs is not enabled")
			}
			account, region, name := core.DefaultAccount, cmp.Or(in.Region, h.Cfg.Region), in.Queue
			if _, r, a, res, ok := core.ParseARN(name); ok {
				account, region, name = a, r, res
			} else if p := strings.Split(strings.TrimRight(name, "/"), "/"); len(p) > 1 { // queue URL
				name = p[len(p)-1]
				if len(p[len(p)-2]) == 12 {
					account = p[len(p)-2]
				}
			}
			msgs, err := q.Peek(account, region, name)
			if err != nil {
				return nil, fmt.Errorf("queue %s in account %s, region %s: %w", name, account, region, err)
			}
			return obj{"queue": name, "region": region, "messages": msgs}, nil
		})

	add(s, "inbox", "What pogi captured instead of sending it out, newest first: SES and Azure Communication Services email, and SNS email, SMS and mobile push messages.", true,
		func(in limitArg) (any, error) {
			out := obj{}
			if sv, ok := h.Service("ses").(*ses.Service); ok {
				out["email"] = newest(sv.Outbox(), positive(in.Limit, 20))
			}
			if sv, ok := h.Service("sns").(*sns.Service); ok {
				out["sns"] = newest(sv.Outbox(), positive(in.Limit, 20))
			}
			if sv, ok := h.Service("azemail").(*email.Service); ok {
				out["azureEmail"] = newest(sv.Outbox(), positive(in.Limit, 20))
			}
			return out, nil
		})

	// ---- control ----
	add(s, "reset", "Delete all state of the given services, or of every service. Resources from pogi.yaml are seeded again; chaos rules and the request log stay.", false,
		func(in resetArg) (any, error) {
			for _, n := range in.Services {
				if h.Service(n) == nil {
					return nil, unknown(h, n)
				}
			}
			h.Reset(in.Services...)
			return obj{"reset": cmp.Or(strings.Join(in.Services, ", "), "all services")}, nil
		})

	add(s, "snapshot_save", "Save pogi's whole state as a named snapshot, replacing one with the same name.", false,
		func(in snapArg) (any, error) {
			p, err := h.SnapshotPath(in.Name)
			if err == nil {
				err = h.SaveFile(p)
			}
			return obj{"saved": in.Name}, err
		})

	add(s, "snapshot_load", "Replace pogi's state with a saved snapshot; services missing from it are reset.", false,
		func(in snapArg) (any, error) {
			p, err := h.SnapshotPath(in.Name)
			if err == nil {
				err = h.LoadFile(p)
			}
			if errors.Is(err, os.ErrNotExist) {
				err = fmt.Errorf("no snapshot named %q (snapshot_list shows them)", in.Name)
			}
			return obj{"loaded": in.Name}, err
		})

	add(s, "snapshot_list", "Saved snapshots, newest first.", true,
		func(none) (any, error) { return obj{"snapshots": h.Snapshots()}, nil })

	add(s, "chaos_add", "Inject faults: calls matching service and op fail with status and code, and/or get latency_ms added, until chaos_clear. Each service answers in its own error format. Returns the rule with its id.", false,
		func(in chaosArg) (any, error) {
			rule := core.Rule{Service: in.Service, Op: in.Op, Probability: in.Rate, Status: in.Status, Code: in.Code, LatencyMs: in.LatencyMs}
			if err := rule.Check(); err != nil {
				return nil, err
			}
			return h.Chaos.Add(rule), nil
		})

	add(s, "chaos_list", "Active chaos rules and how often each has fired.", true,
		func(none) (any, error) { return obj{"rules": h.Chaos.List()}, nil })

	add(s, "chaos_clear", "Remove every chaos rule.", false,
		func(none) (any, error) {
			n := len(h.Chaos.List())
			h.Chaos.Remove("")
			return obj{"removed": n}, nil
		})

	add(s, "deploy_stack", "Create a CloudFormation stack, or update the stack with that name, then wait up to 2 minutes for it to finish. Returns the final status, the outputs and, on failure, the first resource that failed and why. IAM capabilities are granted.", false,
		func(in deployArg) (any, error) {
			req := obj{"StackName": in.Name, "TemplateBody": in.TemplateBody,
				"Capabilities": []string{"CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"}}
			var params []obj
			for _, k := range core.SortedKeys(in.Parameters) {
				params = append(params, obj{"ParameterKey": k, "ParameterValue": in.Parameters[k]})
			}
			req["Parameters"] = params
			op := "CreateStack"
			if _, err := describeStack(h, in.Name); err == nil {
				op = "UpdateStack"
			}
			var res struct{ StackId string }
			err := cfn(h, op, req, &res)
			if err != nil && strings.Contains(err.Error(), "No updates are to be performed") {
				out, err := waitStack(h, in.Name)
				if err == nil {
					out["note"] = "no changes: template and parameters match the deployed stack"
				}
				return out, err
			}
			if err != nil {
				return nil, err
			}
			return waitStack(h, res.StackId)
		})

	add(s, "delete_stack", "Delete a CloudFormation stack and the resources it created, and wait for the deletion to finish.", false,
		func(in stackArg) (any, error) {
			st, err := describeStack(h, in.Name)
			if err == nil {
				err = cfn(h, "DeleteStack", obj{"StackName": st.StackId}, nil)
			}
			if err != nil {
				return nil, err
			}
			return waitStack(h, st.StackId)
		})

	add(s, "stack_status", "A CloudFormation stack's status, outputs, the first failure of its latest operation and its 10 most recent events.", true,
		func(in stackArg) (any, error) {
			st, err := describeStack(h, in.Name)
			if err != nil {
				return nil, err
			}
			return report(h, st, 10)
		})

	add(s, "invoke_function", "Invoke a Lambda function synchronously and return its response payload. functionError is true when the function failed; payload then holds the error. function_logs has its output.", false,
		func(in invokeArg) (any, error) {
			payload := cmp.Or(strings.TrimSpace(in.Payload), "{}")
			if !json.Valid([]byte(payload)) {
				return nil, errors.New("payload is not valid JSON")
			}
			arn := in.Name
			if !strings.HasPrefix(arn, "arn:") {
				arn = core.ARN("lambda", cmp.Or(in.Region, h.Cfg.Region), core.DefaultAccount, "function:"+in.Name)
			}
			b, err := h.Invoke(arn, []byte(payload))
			var fe *core.FunctionError
			if err != nil && !errors.As(err, &fe) {
				return nil, err
			}
			out := obj{"payload": string(b)}
			if json.Valid(b) {
				out["payload"] = json.RawMessage(b)
			}
			if fe != nil {
				out["functionError"] = true
			}
			return out, nil
		})

	add(s, "function_logs", "The latest log lines of a Lambda function, oldest first, from CloudWatch Logs group /aws/lambda/<name>.", true,
		func(in logsArg) (any, error) {
			group := "/aws/lambda/" + in.Name
			var res struct {
				Events []struct {
					Timestamp int64
					Message   string
				}
			}
			// ponytail: FilterLogEvents returns up to 10000 events and we keep the tail; page with nextToken if functions log more.
			err := h.Call(core.DefaultAccount, cmp.Or(in.Region, h.Cfg.Region), "Logs_20140328", "FilterLogEvents", obj{"logGroupName": group}, &res)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", group, err)
			}
			lines := []string{}
			for _, e := range res.Events[max(0, len(res.Events)-positive(in.Limit, 100)):] {
				lines = append(lines, time.UnixMilli(e.Timestamp).UTC().Format("2006-01-02T15:04:05.000Z ")+strings.TrimRight(e.Message, "\n"))
			}
			return obj{"logGroup": group, "lines": lines}, nil
		})

	add(s, "estimate_terraform_cost", "What Terraform code would cost a month on the real cloud (on-demand list prices, 730 h/month, USD). Give a folder of .tf files or a plan.json; a plan also shows the cost before, after and the change. Each row says how sure it is (known, assumed, usage, needs). Use it before you suggest or make a change that adds or resizes resources. The path must be on the machine pogi runs on.", true,
		func(in tfCostArg) (any, error) {
			if in.Path == "" {
				return nil, errors.New("path is required: a Terraform folder or a plan.json")
			}
			_, dockerErr := os.Stat("/.dockerenv")
			if h.Cfg.AllowExternal && dockerErr != nil {
				// Other machines can reach this pogi, so it won't read files on this one for them.
				return nil, errors.New("pogi accepts connections from other machines here (allow-external), so this tool doesn't read files on this machine. Run `pogi cost " + in.Path + "` in a terminal instead")
			}
			if _, err := os.Stat(in.Path); err != nil {
				msg := fmt.Sprintf("path %q: %v", in.Path, err)
				if dockerErr == nil {
					msg += ". pogi runs in Docker here and cannot see your files: mount the folder (docker run -v ./infra:/srv/infra ...) and pass the path inside the container"
				}
				return nil, errors.New(msg)
			}
			opt := cost.Options{}
			if in.UsageFile != "" {
				f, err := os.Open(in.UsageFile)
				if err != nil {
					return nil, fmt.Errorf("usage_file: %w", err)
				}
				data, err := io.ReadAll(io.LimitReader(f, cost.MaxUsageFile+1))
				f.Close()
				if err != nil {
					return nil, fmt.Errorf("usage_file: %w", err)
				}
				if opt.Usage, err = cost.ReadUsage(data); err != nil {
					return nil, err
				}
			}
			cfg, err := terraform.Load(in.Path, in.VarFiles)
			if err != nil {
				return nil, err
			}
			return cost.Price(cfg, opt)
		})

	add(s, "list_sizes", "The machine, database or cache sizes of a Terraform resource type in one region, cheapest first, with vCPU, memory, GPU and the price per hour and month. Use it to pick a size, or to see what a smaller one would cost. Older generations are left out.", true,
		func(in sizesArg) (any, error) {
			db, err := cost.Default()
			if err != nil {
				return nil, err
			}
			sizes, region, err := cost.ListSizes(db, cost.SizeQuery{Type: in.ResourceType, OS: in.OS, Region: in.Region, VCPU: in.MinVCPU, MemGB: in.MinMemoryGB})
			if err != nil {
				return nil, err
			}
			type size struct {
				Name    string  `json:"name"`
				Family  string  `json:"family,omitempty"`
				VCPU    float64 `json:"vcpu"`
				MemGB   float64 `json:"memoryGb"`
				GPU     int     `json:"gpu,omitempty"`
				Hourly  float64 `json:"hourly"`
				Monthly float64 `json:"monthly"`
			}
			out := make([]size, len(sizes))
			for i, z := range sizes {
				out[i] = size{z.Name, z.Family, z.VCPU, z.MemGB, z.GPU, z.Price, z.Price * cost.HoursPerMonth}
			}
			return obj{"resourceType": in.ResourceType, "region": region, "sizes": out}, nil
		})

	add(s, "seed", "Create resources from YAML shaped like the resources section of pogi.yaml (top-level keys aws, gcp, azure, typed). Resources that already exist are skipped.", false,
		func(in seedArg) (any, error) {
			if h.Seed == nil {
				return nil, errors.New("seeding is not available in this pogi build (no seeder is wired up)")
			}
			if err := h.Seed([]byte(in.YAML)); err != nil {
				return nil, err
			}
			return obj{"seeded": true}, nil
		})
}

func unknown(h *core.Hub, name string) error {
	var names []string
	for _, s := range h.Services() {
		names = append(names, s.Info().Name)
	}
	return fmt.Errorf("no enabled service %q; enabled: %s", name, strings.Join(names, ", "))
}

// positive returns n, or def when n is not positive.
func positive(n, def int) int {
	if n <= 0 {
		return def
	}
	return n
}

// newest reverses list (oldest first) and keeps the first n.
func newest[T any](list []T, n int) []T {
	slices.Reverse(list)
	return list[:min(n, len(list))]
}

// ---- CloudFormation, through the hub like any other in-process caller ----

type stack struct {
	StackId, StackName, StackStatus, StackStatusReason string
	Outputs                                            []struct{ OutputKey, OutputValue string }
}

type stackEvent struct {
	LogicalResourceId, ResourceType, ResourceStatus string
	ResourceStatusReason                            string `json:",omitempty"`
}

func cfn(h *core.Hub, op string, in, out any) error {
	return h.Call(core.DefaultAccount, h.Cfg.Region, "CloudFormation", op, in, out)
}

func describeStack(h *core.Hub, ref string) (stack, error) {
	if ref == "" { // DescribeStacks would list every stack
		return stack{}, errors.New("give a stack name")
	}
	var out struct{ Stacks []stack }
	if err := cfn(h, "DescribeStacks", obj{"StackName": ref}, &out); err != nil {
		return stack{}, err
	}
	if len(out.Stacks) == 0 {
		return stack{}, fmt.Errorf("stack %s does not exist", ref)
	}
	return out.Stacks[0], nil
}

// waitStack polls a stack until its operation ends, for at most 2 minutes.
func waitStack(h *core.Hub, ref string) (obj, error) {
	deadline := time.Now().Add(2 * time.Minute)
	for {
		st, err := describeStack(h, ref)
		if err != nil {
			return nil, err
		}
		busy := strings.HasSuffix(st.StackStatus, "_IN_PROGRESS")
		if !busy || time.Now().After(deadline) {
			out, err := report(h, st, 0)
			if busy && err == nil {
				out["note"] = "still in progress after 2 minutes; check again with stack_status"
			}
			return out, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// report summarizes a stack: status, outputs, the first failure of its
// latest operation and, if events > 0, that many recent events.
func report(h *core.Hub, st stack, events int) (obj, error) {
	var ev struct{ StackEvents []stackEvent } // newest first
	if err := cfn(h, "DescribeStackEvents", obj{"StackName": st.StackId}, &ev); err != nil {
		return nil, err
	}
	out := obj{"stack": st.StackName, "status": st.StackStatus}
	if st.StackStatusReason != "" {
		out["reason"] = st.StackStatusReason
	}
	if len(st.Outputs) > 0 {
		o := map[string]string{}
		for _, x := range st.Outputs {
			o[x.OutputKey] = x.OutputValue
		}
		out["outputs"] = o
	}
	for _, e := range ev.StackEvents {
		if strings.HasSuffix(e.ResourceStatus, "_FAILED") {
			out["firstFailure"] = e // the oldest failure wins
		}
		if e.ResourceType == "AWS::CloudFormation::Stack" && e.ResourceStatusReason == "User Initiated" {
			break // start of the latest operation
		}
	}
	if events > 0 {
		out["events"] = ev.StackEvents[:min(events, len(ev.StackEvents))]
	}
	return out, nil
}
