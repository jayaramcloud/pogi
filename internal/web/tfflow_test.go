package web

import (
	"fmt"
	"net/http/httptest"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/cost"
	"github.com/nrndev/pogi/internal/terraform"
)

func fres(typ, name string, deps []string, attrs map[string]any) *terraform.Resource {
	return &terraform.Resource{Address: typ + "." + name, Block: typ + "." + name, Type: typ, Name: name, Deps: deps, Attrs: attrs}
}

func flowCfg(rs ...*terraform.Resource) *terraform.Config {
	for _, r := range rs {
		r.Cloud = map[bool]string{true: "gcp", false: "aws"}[strings.HasPrefix(r.Type, "google")]
	}
	return &terraform.Config{Resources: rs}
}

func TestFlowRules(t *testing.T) {
	topic, queue, fn := fres(tfTopic, "t", nil, nil), fres(tfQueue, "q", nil, nil), fres(tfLambda, "f", nil, nil)
	cases := []struct {
		name string
		cfg  *terraform.Config
		want []string // "from>to label"
	}{
		{"sns", flowCfg(topic, queue, fres("aws_sns_topic_subscription", "s", []string{topic.Block, queue.Block}, nil)), []string{"aws_sns_topic.t>aws_sqs_queue.q subscription"}},
		{"two targets", flowCfg(topic, queue, fn, fres("aws_sns_topic_subscription", "s", []string{topic.Block, queue.Block, fn.Block}, nil)),
			[]string{"aws_sns_topic.t>aws_sqs_queue.q subscription", "aws_sns_topic.t>aws_lambda_function.f subscription"}},
		{"event source", flowCfg(queue, fn, fres("aws_lambda_event_source_mapping", "m", []string{queue.Block, fn.Block}, nil)), []string{"aws_sqs_queue.q>aws_lambda_function.f event source"}},
		{"s3", flowCfg(fres("aws_s3_bucket", "b", nil, nil), queue, fres("aws_s3_bucket_notification", "n", []string{"aws_s3_bucket.b", queue.Block}, nil)), []string{"aws_s3_bucket.b>aws_sqs_queue.q notification"}},
		{"event target", flowCfg(fres("aws_cloudwatch_event_rule", "r", nil, nil), fn, fres("aws_cloudwatch_event_target", "t", []string{"aws_cloudwatch_event_rule.r", fn.Block}, nil)), []string{"aws_cloudwatch_event_rule.r>aws_lambda_function.f target"}},
		{"missing target", flowCfg(topic, fres("aws_sns_topic_subscription", "s", []string{topic.Block}, nil)), nil},
		{"missing source", flowCfg(queue, fres("aws_sns_topic_subscription", "s", []string{queue.Block}, nil)), nil},
		{"two sources", flowCfg(topic, fres(tfTopic, "t2", nil, nil), queue, fres("aws_sns_topic_subscription", "s", []string{topic.Block, "aws_sns_topic.t2", queue.Block}, nil)), nil},
		{"fits both lists", flowCfg(queue, fres(tfQueue, "q2", nil, nil), fres("aws_pipes_pipe", "p", []string{queue.Block, "aws_sqs_queue.q2"}, nil)), nil},
		{"pipe queue to lambda", flowCfg(queue, fn, fres("aws_pipes_pipe", "p", []string{queue.Block, fn.Block}, nil)), []string{"aws_sqs_queue.q>aws_lambda_function.f pipe"}},
		{"data source ignored", flowCfg(queue, fn, &terraform.Resource{Address: "data.aws_sqs_queue.d", Block: "data.aws_sqs_queue.d", Type: tfQueue, Data: true},
			fres("aws_lambda_event_source_mapping", "m", []string{queue.Block, fn.Block, "data.aws_sqs_queue.d"}, nil)), []string{"aws_sqs_queue.q>aws_lambda_function.f event source"}},
		{"pubsub pull keeps its box", flowCfg(fres("google_pubsub_topic", "t", nil, nil), fres("google_pubsub_subscription", "s", []string{"google_pubsub_topic.t"}, nil)),
			[]string{"google_pubsub_topic.t>google_pubsub_subscription.s subscription"}},
		{"pubsub push", flowCfg(fres("google_pubsub_topic", "t", nil, nil), fres("google_cloud_run_v2_service", "r", nil, nil), fres("google_pubsub_subscription", "s", []string{"google_pubsub_topic.t", "google_cloud_run_v2_service.r"}, nil)),
			[]string{"google_pubsub_topic.t>google_pubsub_subscription.s subscription", "google_pubsub_subscription.s>google_cloud_run_v2_service.r push"}},
		{"gcs notification", flowCfg(fres("google_storage_bucket", "b", nil, nil), fres("google_pubsub_topic", "t", nil, nil), fres("google_storage_notification", "n", []string{"google_storage_bucket.b", "google_pubsub_topic.t"}, nil)),
			[]string{"google_storage_bucket.b>google_pubsub_topic.t notification"}},
		{"servicebus forward", flowCfg(fres("azurerm_servicebus_topic", "t", nil, nil), fres("azurerm_servicebus_queue", "q", nil, nil), fres("azurerm_servicebus_subscription", "s", []string{"azurerm_servicebus_topic.t", "azurerm_servicebus_queue.q"}, nil)),
			[]string{"azurerm_servicebus_topic.t>azurerm_servicebus_subscription.s subscription", "azurerm_servicebus_subscription.s>azurerm_servicebus_queue.q forward"}},
		{"eventgrid", flowCfg(fres("azurerm_eventgrid_topic", "t", nil, nil), fres("azurerm_servicebus_queue", "q", nil, nil), fres("azurerm_eventgrid_event_subscription", "e", []string{"azurerm_eventgrid_topic.t", "azurerm_servicebus_queue.q"}, nil)),
			[]string{"azurerm_eventgrid_topic.t>azurerm_servicebus_queue.q subscription"}},
	}
	for _, c := range cases {
		var got []string
		for _, l := range tfFlows(c.cfg) {
			got = append(got, l.From+">"+l.To+" "+l.Label)
		}
		if !slices.Equal(got, c.want) {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
}

func TestFlowCount(t *testing.T) {
	topic, queue := fres(tfTopic, "t", nil, nil), fres(tfQueue, "q", nil, nil)
	a, b := fres("aws_sns_topic_subscription", "s", []string{topic.Block, queue.Block}, nil), fres("aws_sns_topic_subscription", "s", []string{topic.Block, queue.Block}, nil)
	a.Address, b.Address = a.Block+"[0]", a.Block+"[1]"
	l := tfFlows(flowCfg(topic, queue, a, b))
	if len(l) != 1 || l[0].N != 2 {
		t.Fatalf("want one line x2, got %+v", l)
	}
}

func TestFlowLive(t *testing.T) {
	now := time.Now()
	const tarn, qarn = "arn:aws:sns:us-east-1:000000000000:orders", "arn:aws:sqs:us-east-1:000000000000:worker"
	cfg := func(topicAttrs, queueAttrs map[string]any) *terraform.Config {
		topic, queue := fres(tfTopic, "t", nil, topicAttrs), fres(tfQueue, "q", nil, queueAttrs)
		return flowCfg(topic, queue, fres("aws_sns_topic_subscription", "s", []string{topic.Block, queue.Block}, nil))
	}
	span := func(from, to string, age time.Duration, err string) core.Span {
		return core.Span{Time: now.Add(-age), From: from, To: to, Err: err, Kind: core.SpanDeliver}
	}
	live := func(c *terraform.Config, spans ...core.Span) flowLine {
		l := tfFlows(c)
		markLive(l, c, spans, now)
		return l[0]
	}
	exact := cfg(map[string]any{"arn": tarn}, map[string]any{"arn": qarn})
	if l := live(exact, span(tarn, qarn, time.Second, "")); !l.Live || l.Bad {
		t.Errorf("exact ARN: %+v", l)
	}
	if l := live(exact, span(tarn, qarn, time.Second, "boom")); !l.Live || !l.Bad {
		t.Errorf("failed span: %+v", l)
	}
	if l := live(exact, span(tarn, qarn, time.Minute, "")); l.Live {
		t.Errorf("old span lit the line: %+v", l)
	}
	if l := live(exact); l.Live {
		t.Errorf("no spans lit the line: %+v", l)
	}
	byName := cfg(map[string]any{"name": "orders"}, map[string]any{"name": "worker"})
	if l := live(byName, span(tarn, qarn, time.Second, "")); !l.Live {
		t.Errorf("name match: %+v", l)
	}
	// the same name in two regions, and the files give no region: no match
	other := "arn:aws:sqs:eu-west-1:000000000000:worker"
	if l := live(byName, span(tarn, qarn, time.Second, ""), span(tarn, other, time.Second, "")); l.Live {
		t.Errorf("ambiguous region matched: %+v", l)
	}
	// ...but a block that names its region matches its own
	byName.Resources[1].Region = "eu-west-1"
	if l := live(byName, span(tarn, qarn, time.Second, ""), span(tarn, other, time.Minute, "")); l.Live {
		t.Errorf("wrong region matched: %+v", l)
	}
	if l := live(byName, span(tarn, other, time.Second, "")); !l.Live {
		t.Errorf("region match: %+v", l)
	}
	// two blocks with the same name share a key: neither matches
	c := cfg(map[string]any{"name": "orders"}, map[string]any{"name": "worker"})
	c.Resources = append(c.Resources, fres(tfQueue, "q2", nil, map[string]any{"name": "worker"}))
	if l := live(c, span(tarn, qarn, time.Second, "")); l.Live {
		t.Errorf("shared name matched: %+v", l)
	}
	// Google: a full resource name matches the id path of the block
	g := flowCfg(fres("google_pubsub_topic", "t", nil, map[string]any{"id": "projects/p/topics/orders"}), fres("google_pubsub_subscription", "s", []string{"google_pubsub_topic.t"}, map[string]any{"name": "sub"}))
	gl := tfFlows(g)
	markLive(gl, g, []core.Span{span("//pubsub.googleapis.com/projects/p/topics/orders", "//pubsub.googleapis.com/projects/p/subscriptions/sub", time.Second, "")}, now)
	if len(gl) != 1 || !gl[0].Live {
		t.Errorf("pubsub: %+v", gl)
	}
}

func flowEst(cfg *terraform.Config) *cost.Estimate {
	est := &cost.Estimate{}
	for _, r := range cfg.Resources {
		est.Rows = append(est.Rows, cost.Row{Address: r.Address})
	}
	return est
}

func snsStack() (*terraform.Config, *flowStack) {
	topic, queue := fres(tfTopic, "orders", nil, map[string]any{"arn": "arn:aws:sns:us-east-1:000000000000:orders"}), fres(tfQueue, "worker", nil, map[string]any{"arn": "arn:aws:sqs:us-east-1:000000000000:worker"})
	sub := fres("aws_sns_topic_subscription", "orders_to_worker", []string{topic.Block, queue.Block}, nil)
	return flowCfg(topic, queue, sub), &flowStack{}
}

type flowStack struct{}

func TestFlowDrawing(t *testing.T) {
	for _, network := range []bool{false, true} {
		cfg, _ := snsStack()
		g := buildTFGraphBy(cfg, flowEst(cfg), nil, network)
		if g.byID["aws_sns_topic_subscription.orders_to_worker"] != nil || len(g.Nodes) != 2 || len(g.Lines) != 1 {
			t.Fatalf("network=%v: a resolved wiring block must be a line, not a box: %d nodes, %d lines", network, len(g.Nodes), len(g.Lines))
		}
		svg := g.svg(false)
		for _, w := range []string{`class="flow"`, `role="button"`, `aria-label="sns topic subscription orders_to_worker, orders → worker, data flow"`, `data-id="flow:0"`, "marker-end"} {
			if !strings.Contains(svg, w) {
				t.Errorf("network=%v: the svg lacks %q", network, w)
			}
		}
		if strings.Contains(svg, "pulse") {
			t.Error("a quiet line has dots")
		}
		// the target sits to the right of the source, as the dashes move
		if g.byID["aws_sqs_queue.worker"].col <= g.byID["aws_sns_topic.orders"].col {
			t.Error("the target is not right of the source")
		}
	}
	// a wiring block with no resolved ends stays a box
	topic := fres(tfTopic, "t", nil, nil)
	cfg := flowCfg(topic, fres("aws_sns_topic_subscription", "s", []string{topic.Block}, nil))
	g := buildTFGraphBy(cfg, flowEst(cfg), nil, false)
	if len(g.Lines) != 0 || g.byID["aws_sns_topic_subscription.s"] == nil {
		t.Fatalf("an unresolved wiring block must stay a box: %d lines", len(g.Lines))
	}
	// a pull subscription stays a box and has a line from its topic
	cfg = flowCfg(fres("google_pubsub_topic", "t", nil, nil), fres("google_pubsub_subscription", "s", []string{"google_pubsub_topic.t"}, nil))
	g = buildTFGraphBy(cfg, flowEst(cfg), nil, false)
	if len(g.Lines) != 1 || g.byID["google_pubsub_subscription.s"] == nil || g.Lines[0].Rows[0] != 1 {
		t.Fatalf("pull subscription: %d lines", len(g.Lines))
	}
}

// An end in a collapsed module draws the line to the module's box.
func TestFlowCollapsedEnd(t *testing.T) {
	topic := fres(tfTopic, "t", nil, nil)
	cfg := &terraform.Config{Resources: []*terraform.Resource{topic}}
	var q *terraform.Resource
	for i := 0; i <= tfCollapseAt; i++ { // a module with more blocks than tfCollapseAt
		r := fres(tfQueue, "q"+strings.Repeat("x", i), nil, nil)
		r.Module, r.Block, r.Address = "module.m", "module.m."+r.Type+"."+r.Name, "module.m."+r.Type+"."+r.Name
		cfg.Resources = append(cfg.Resources, r)
		q = r
	}
	cfg.Resources = append(cfg.Resources, fres("aws_sns_topic_subscription", "s", []string{topic.Block, q.Block}, nil))
	g := buildTFGraphBy(cfg, flowEst(cfg), nil, false)
	if len(g.Lines) != 1 || g.Lines[0].To != "module.m" || !g.byID["module.m"].Collapsed {
		t.Fatalf("lines: %+v", g.Lines)
	}
	if !strings.Contains(g.svg(false), `data-t="module.m"`) {
		t.Error("the line does not end at the module box")
	}
}

func TestFlowExportIsStill(t *testing.T) {
	cfg, _ := snsStack()
	g := buildTFGraphBy(cfg, flowEst(cfg), nil, false)
	g.markLive(cfg, []core.Span{{Time: time.Now(), From: "arn:aws:sns:us-east-1:000000000000:orders", To: "arn:aws:sqs:us-east-1:000000000000:worker", Kind: core.SpanDeliver}}, time.Now())
	if !g.Lines[0].Live || !strings.Contains(g.svg(false), `class="pulse"`) {
		t.Fatal("a live line has no dots on the page")
	}
	out := g.export()
	for _, bad := range []string{`class="pulse`, "animateMotion", "animation", "keyframes", "flowhit", "tabindex"} {
		if strings.Contains(out, bad) {
			t.Errorf("the export has %q", bad)
		}
	}
	if !strings.Contains(out, `class="flow"`) || !strings.Contains(out, "stroke-dasharray:2 5") {
		t.Error("the export does not show the dotted line")
	}
}

func TestFlowLiveWindow(t *testing.T) {
	cfg, _ := snsStack()
	g := buildTFGraphBy(cfg, flowEst(cfg), nil, false)
	now := time.Now()
	sp := core.Span{Time: now.Add(-time.Minute), From: "arn:aws:sns:us-east-1:000000000000:orders", To: "arn:aws:sqs:us-east-1:000000000000:worker"}
	g.markLive(cfg, []core.Span{sp}, now)
	if g.Lines[0].Live || strings.Contains(g.svg(false), "pulse") {
		t.Error("a span outside the window lit the line")
	}
	sp.Time, sp.Err = now.Add(-time.Second), "boom"
	g.markLive(cfg, []core.Span{sp}, now)
	if !g.Lines[0].Bad || !strings.Contains(g.svg(false), `class="pulse bad"`) {
		t.Error("a failed span did not make a bad dot")
	}
}

// The console's reduced-motion rule ends CSS animations and hides the dots; the lines' own motion is
// a CSS animation, so it stays dotted and still.
func TestFlowReducedMotion(t *testing.T) {
	b, err := os.ReadFile("static/pogi.css")
	if err != nil {
		t.Fatal(err)
	}
	css := string(b)
	i := strings.Index(css, "@media (prefers-reduced-motion: reduce)")
	if i < 0 {
		t.Fatal("no reduced-motion rule")
	}
	rule := css[i:]
	rule = rule[:strings.Index(rule, "/* ---------- credit")]
	for _, w := range []string{"animation-duration: .001ms !important", "animation-iteration-count: 1 !important", "svg.map .pulse { display: none; }"} {
		if !strings.Contains(rule, w) {
			t.Errorf("the reduced-motion rule lacks %q", w)
		}
	}
	if !strings.Contains(css, ".tfmap .flow {") || !strings.Contains(css, "stroke-dasharray: 2 5") || strings.Contains(strings.Split(css, "@keyframes tfflow")[1][:60], "stroke-dasharray") {
		t.Error("the dotted line must be dotted by dasharray and moved only by the animation")
	}
}

// The live refresh reads the stack kept with the held one, and answers {} with no stack, no spans, or no hub.
func TestFlowLiveEndpoint(t *testing.T) {
	ui := tfTestUI(t)
	get := func() string {
		w := httptest.NewRecorder()
		ui.tfLive(w, httptest.NewRequest("GET", "/_pogi/terraform/live", nil))
		return strings.TrimSpace(w.Body.String())
	}
	if got := get(); got != "{}" {
		t.Fatalf("no stack: %s", got)
	}
	files := map[string]string{"main.tf": `
resource "aws_sns_topic" "orders" { name = "orders" }
resource "aws_sqs_queue" "worker" { name = "worker" }
resource "aws_sns_topic_subscription" "s" {
  topic_arn = aws_sns_topic.orders.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.worker.arn
}`}
	if w := tfUpload(t, ui, files, map[string]string{"view": "diagram"}); w.Code != 200 || !strings.Contains(w.Body.String(), `class="flow"`) || strings.Contains(w.Body.String(), `data-id="aws_sns_topic_subscription.s"`) {
		t.Fatalf("upload: %d", w.Code)
	}
	h := ui.tf.get()
	if h.cfg == nil || len(h.flows) != 1 {
		t.Fatalf("the held stack lacks its parsed files and lines: %+v", h.flows)
	}
	if got := get(); got != "{}" { // no hub in the test console: quiet, not an error
		t.Fatalf("no hub: %s", got)
	}
}

// A subscription box depends on the queue it forwards to, and its forward line runs the other way.
// The line must not push the columns into a loop: the drawing stays a few columns wide.
func TestFlowLoopKeepsLayout(t *testing.T) {
	cfg := flowCfg(
		fres("google_pubsub_topic", "t", nil, nil),
		fres("google_cloud_run_v2_service", "run", nil, nil),
		fres("google_pubsub_subscription", "push", []string{"google_cloud_run_v2_service.run", "google_pubsub_topic.t"}, nil),
	)
	for i := range 20 { // enough boxes that a runaway would be wide
		cfg.Resources = append(cfg.Resources, fres("google_storage_bucket", fmt.Sprint("b", i), nil, nil))
	}
	g := buildTFGraphBy(cfg, flowEst(cfg), nil, false)
	if len(g.Lines) != 2 {
		t.Fatalf("want the topic and push lines, got %d", len(g.Lines))
	}
	if most := 4 * tfColStep; g.w > most {
		t.Fatalf("the layout ran away: %d px wide, want at most %d", g.w, most)
	}
}
