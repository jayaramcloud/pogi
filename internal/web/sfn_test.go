package web_test

import (
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
)

var nodeClass = regexp.MustCompile(`<g class="node ?([a-z]*)" data-state="([^"]+)"`)

// states maps each drawn state to its status class ("" when it has none).
func states(page string) map[string]string {
	out := map[string]string{}
	for _, m := range nodeClass.FindAllStringSubmatch(page, -1) {
		out[m[2]] = m[1]
	}
	return out
}

func TestStepFunctionsGraph(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"stepfunctions"}})
	def := `{"StartAt":"Start","States":{
	  "Start":{"Type":"Pass","Next":"Check"},
	  "Check":{"Type":"Choice","Choices":[{"Variable":"$.n","NumericGreaterThan":5,"Next":"Big"}],"Default":"Small"},
	  "Small":{"Type":"Succeed"},
	  "Big":{"Type":"Fail","Error":"TooBig","Cause":"n is above 5"}}}`
	sm := call(t, srv, "AWSStepFunctions", "CreateStateMachine", map[string]any{"name": "sizes", "definition": def, "roleArn": "arn:aws:iam::000000000000:role/r"})
	arn := sm["stateMachineArn"].(string)
	start := func(name, input string) {
		ex := call(t, srv, "AWSStepFunctions", "StartExecution", map[string]any{"stateMachineArn": arn, "name": name, "input": input})
		for i := 0; i < 100; i++ {
			if call(t, srv, "AWSStepFunctions", "DescribeExecution", map[string]any{"executionArn": ex["executionArn"]})["status"] != "RUNNING" {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatal("execution did not finish")
	}
	start("big", `{"n": 9}`)
	start("small", `{"n": 1}`)

	// without an execution: all four states and the edges, no status
	p := page(t, srv, "/_pogi/sfn?machine=sizes")
	got := states(p)
	if len(got) != 4 || got["Start"] != "" || got["Big"] != "" {
		t.Fatalf("plain graph: %v", got)
	}
	if n := strings.Count(p, `class="edge `); n != 3 {
		t.Errorf("edges = %d, want 3 (Start>Check, Check>Big, Check>Small)", n)
	}
	has(t, p, "execution=big", "execution=small")

	// the failing execution: the path it took is coloured, and Small never ran
	got = states(page(t, srv, "/_pogi/sfn?machine=sizes&execution=big"))
	if got["Start"] != "succeeded" || got["Check"] != "succeeded" || got["Big"] != "failed" || got["Small"] != "" {
		t.Errorf("failed execution: %v", got)
	}
	got = states(page(t, srv, "/_pogi/sfn?machine=sizes&execution=small"))
	if got["Small"] != "succeeded" || got["Big"] != "" || got["Check"] != "succeeded" {
		t.Errorf("succeeded execution: %v", got)
	}
}
