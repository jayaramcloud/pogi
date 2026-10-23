package web

import (
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/terraform"
)

// Data-flow lines for the Terraform diagram. A wiring resource (an SNS subscription, a Lambda
// event source mapping...) is not a box: it is a line from its source to its target. This file
// finds those lines and says which ones pogi saw traffic on. It draws nothing.

// flowRule describes one wiring resource type. The labels agree with the core.Linker links.
type flowRule struct {
	Label    string
	Src, Dst []string // resource types that can be the source and the target
	Box      bool     // the wiring block holds messages (a pull subscription): it stays a box
	Out      string   // Box only: the label of the line from the box to its targets
}

var (
	tfQueue    = "aws_sqs_queue"
	tfTopic    = "aws_sns_topic"
	tfLambda   = "aws_lambda_function"
	tfKinesis  = "aws_kinesis_stream"
	tfFirehose = "aws_kinesis_firehose_delivery_stream"
	tfSfn      = "aws_sfn_state_machine"
	gcpRun     = []string{"google_cloud_run_service", "google_cloud_run_v2_service", "google_cloudfunctions_function", "google_cloudfunctions2_function"}
)

var flowRules = map[string]flowRule{
	"aws_s3_bucket_notification":      {Label: "notification", Src: []string{"aws_s3_bucket"}, Dst: []string{tfQueue, tfTopic, tfLambda}},
	"aws_sns_topic_subscription":      {Label: "subscription", Src: []string{tfTopic}, Dst: []string{tfQueue, tfLambda, tfFirehose}},
	"aws_lambda_event_source_mapping": {Label: "event source", Src: []string{tfQueue, tfKinesis, "aws_dynamodb_table"}, Dst: []string{tfLambda}},
	"aws_cloudwatch_event_target":     {Label: "target", Src: []string{"aws_cloudwatch_event_rule"}, Dst: []string{tfQueue, tfTopic, tfLambda, tfKinesis, tfFirehose, tfSfn}},
	"aws_pipes_pipe":                  {Label: "pipe", Src: []string{tfQueue, tfKinesis, "aws_dynamodb_table"}, Dst: []string{tfTopic, tfLambda, tfKinesis, tfFirehose, tfSfn, "aws_cloudwatch_event_bus"}},

	"google_pubsub_subscription":  {Label: "subscription", Src: []string{"google_pubsub_topic"}, Dst: gcpRun, Box: true, Out: "push"},
	"google_eventarc_trigger":     {Label: "event source", Src: []string{"google_storage_bucket", "google_pubsub_topic"}, Dst: []string{"google_workflows_workflow", "google_cloud_run_v2_service", "google_cloudfunctions2_function"}},
	"google_storage_notification": {Label: "notification", Src: []string{"google_storage_bucket"}, Dst: []string{"google_pubsub_topic"}},

	"azurerm_eventgrid_event_subscription": {Label: "subscription", Src: []string{"azurerm_eventgrid_topic", "azurerm_eventgrid_system_topic"},
		Dst: []string{"azurerm_servicebus_queue", "azurerm_servicebus_topic", "azurerm_eventhub", "azurerm_storage_queue"}},
	"azurerm_servicebus_subscription": {Label: "subscription", Src: []string{"azurerm_servicebus_topic"}, Dst: []string{"azurerm_servicebus_queue"}, Box: true, Out: "forward"},
}

// ponytail: aws_sqs_queue_redrive_policy is left out. Its two ends are both queues and Deps does
// not say which is which, so it can never resolve. Add it when Resource keeps per-attribute refs.

// flowLine is one data-flow line between two blocks.
type flowLine struct {
	Wiring   string // the wiring block that makes the line
	Label    string
	From, To string // blocks
	N        int    // the wiring block's instances (count or for_each): drawn "×N" when above 1
	Box      bool   // the wiring block stays a box
	Live     bool   // pogi saw a span from From to To in the live window
	Bad      bool   // ...and one of them failed
}

// tfFlows resolves the wiring blocks of cfg into lines. Among a wiring block's Deps, a block of a
// source type is the source and a block of a target type is a target. It never guesses: no
// source, no target, two sources, or a block that fits both lists gives no line, and the
// wiring resource stays a box.
func tfFlows(cfg *terraform.Config) []flowLine {
	typeOf := map[string]string{}
	inst := map[string]int{}
	for _, r := range cfg.Resources {
		if !r.Data {
			typeOf[r.Block] = r.Type
			inst[r.Block]++
		}
	}
	var out []flowLine
	seen := map[string]bool{} // a block with count or for_each gives one set of lines, not one per instance
	for _, r := range cfg.Resources {
		rule, ok := flowRules[r.Type]
		if r.Data || !ok || seen[r.Block] {
			continue
		}
		seen[r.Block] = true
		var src, dst []string
		ambiguous := false
		for _, d := range r.Deps {
			t := typeOf[d]
			isSrc, isDst := slices.Contains(rule.Src, t), slices.Contains(rule.Dst, t)
			switch {
			case isSrc && isDst:
				ambiguous = true
			case isSrc:
				src = append(src, d)
			case isDst:
				dst = append(dst, d)
			}
		}
		if ambiguous {
			continue
		}
		mk := func(label, from, to string) flowLine {
			return flowLine{Wiring: r.Block, Label: label, From: from, To: to, N: inst[r.Block], Box: rule.Box}
		}
		if rule.Box { // topic → the subscription box, and the box → its push or forward targets
			if len(src) == 1 {
				out = append(out, mk(rule.Label, src[0], r.Block))
			}
			for _, d := range dst {
				out = append(out, mk(rule.Out, r.Block, d))
			}
			continue
		}
		if len(src) != 1 {
			continue
		}
		for _, d := range dst {
			out = append(out, mk(rule.Label, src[0], d))
		}
	}
	return out
}

// flowSvc names the service of a resource type the way classify does.
var flowSvc = map[string]string{
	"aws_s3_bucket": "s3", tfQueue: "sqs", tfTopic: "sns", tfLambda: "lambda", "aws_cloudwatch_event_rule": "events",
	tfKinesis: "kinesis", tfFirehose: "firehose", "aws_dynamodb_table": "dynamodb",
	"google_pubsub_topic": "pubsub", "google_pubsub_subscription": "pubsub", "google_storage_bucket": "gcs",
	"google_eventarc_trigger": "eventarc",
}

// flowRuntime is a resource id from the span log, split into what matching needs.
type flowRuntime struct{ id, svc, region, name string }

// flowKey is the same key for the same resource, whoever names it: an ARN, a Google full
// resource name (//pubsub.googleapis.com/projects/p/topics/t) or an Azure id. "" when it is none.
func flowKey(id string) string {
	id = normID(id)
	switch {
	case strings.HasPrefix(id, "arn:"):
		return id
	case strings.HasPrefix(id, "//"):
		host, rest, _ := strings.Cut(strings.TrimPrefix(id, "//"), "/")
		svc := strings.TrimSuffix(host, ".googleapis.com")
		if svc == "storage" {
			svc = "gcs"
		}
		return "gcp:" + svc + "/" + rest
	case strings.HasPrefix(strings.ToLower(id), "/subscriptions/"):
		return strings.ToLower(id)
	}
	return ""
}

// flowParse reads a runtime id for name matching. Azure ids are left to exact matching: a bare
// name is not unique across namespaces.
func flowParse(id string) flowRuntime {
	rt := flowRuntime{id: flowKey(id)}
	switch {
	case strings.HasPrefix(rt.id, "arn:"):
		rt.svc, rt.region, _, _, _ = core.ParseARN(rt.id)
		rt.name = rt.id[strings.LastIndexAny(rt.id, "/:")+1:]
	case strings.HasPrefix(rt.id, "gcp:"):
		svc, rest, _ := strings.Cut(strings.TrimPrefix(rt.id, "gcp:"), "/")
		rt.svc = svc
		if i := strings.Index(rest, "/topics/"); i >= 0 {
			rt.name = "topics/" + rest[i+len("/topics/"):]
		} else if i := strings.Index(rest, "/subscriptions/"); i >= 0 {
			rt.name = "subscriptions/" + rest[i+len("/subscriptions/"):]
		} else if i := strings.Index(rest, "/buckets/"); i >= 0 {
			rt.name = "buckets/" + rest[i+len("/buckets/"):]
		}
	}
	return rt
}

// flowIndex maps each runtime id seen in spans to the one block it is, from the exact ids in the
// plan or files (arn, id, name path) and, failing that, from service + region + name. A key two
// blocks share, or a name that fits several runtime ids, matches nothing.
func flowIndex(cfg *terraform.Config, spans []core.Span) map[string]string {
	var seen []flowRuntime
	ids := map[string]bool{}
	for _, sp := range spans {
		for _, id := range []string{sp.From, sp.To} {
			if rt := flowParse(id); rt.id != "" && !ids[rt.id] {
				ids[rt.id] = true
				seen = append(seen, rt)
			}
		}
	}
	owner := map[string]string{} // runtime id → block
	taken := map[string]bool{}   // runtime ids two blocks claim
	claim := func(rt, block string) {
		if o, ok := owner[rt]; ok && o != block {
			taken[rt] = true
		}
		owner[rt] = block
	}
	for _, r := range cfg.Resources {
		if r.Data {
			continue
		}
		exact := false
		for _, a := range []string{"arn", "id", "name"} {
			s, _ := r.Attrs[a].(string)
			if k := tfExactKey(r, a, s); k != "" && ids[k] {
				claim(k, r.Block)
				exact = true
			}
		}
		if exact {
			continue
		}
		svc, name := flowSvc[r.Type], tfName(r)
		if svc == "" || name == "" {
			continue
		}
		if svc == "pubsub" || svc == "gcs" { // runtime names read topics/x, subscriptions/x, buckets/x
			name = map[string]string{"google_pubsub_topic": "topics/", "google_pubsub_subscription": "subscriptions/", "google_storage_bucket": "buckets/"}[r.Type] + name
		}
		var hit []string
		for _, rt := range seen {
			if rt.svc == svc && rt.name == name && (rt.region == "" || r.Region == "" || rt.region == r.Region) {
				hit = append(hit, rt.id)
			}
		}
		if len(hit) == 1 {
			claim(hit[0], r.Block)
		}
	}
	for k := range taken {
		delete(owner, k)
	}
	return owner
}

// tfExactKey is the key of an id attribute when the attribute is a full id, "" otherwise. A
// Google id is a path (projects/p/topics/t), so it takes its service from the type.
func tfExactKey(r *terraform.Resource, attr, v string) string {
	if k := flowKey(v); k != "" {
		return k
	}
	if svc := flowSvc[r.Type]; r.Cloud == "gcp" && strings.HasPrefix(v, "projects/") && (attr == "id" || attr == "name") && svc != "" {
		return "gcp:" + svc + "/" + v
	}
	return ""
}

// tfName is the name a resource has at runtime, when the files give it as plain text.
func tfName(r *terraform.Resource) string {
	for _, a := range []string{"name", "bucket", "function_name"} {
		if s, ok := r.Attrs[a].(string); ok && s != "" && !strings.HasPrefix(s, "projects/") {
			return s
		}
	}
	return ""
}

// markLive sets Live and Bad on the lines that have a span from their source to their target
// within liveWindow of now. No spans, or no matching resources, leaves every line quiet.
func markLive(lines []flowLine, cfg *terraform.Config, spans []core.Span, now time.Time) {
	if len(lines) == 0 || len(spans) == 0 {
		return
	}
	idx := flowIndex(cfg, spans)
	for _, sp := range spans {
		if now.Sub(sp.Time) > liveWindow || sp.From == "" || sp.To == "" {
			continue
		}
		f, t := idx[flowKey(sp.From)], idx[flowKey(sp.To)]
		if f == "" || t == "" {
			continue
		}
		for i := range lines {
			if lines[i].From == f && lines[i].To == t {
				lines[i].Live = true
				lines[i].Bad = lines[i].Bad || sp.Err != ""
			}
		}
	}
}
