package web_test

import (
	"os/exec"
	"testing"
)

// feedback.js (busy buttons, the confirm dialog, live rows, the task indicator) runs against a small fake DOM in node.
func TestFeedbackJS(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed")
	}
	out, err := exec.Command(node, "testdata/feedback_test.mjs", "static/feedback.js").CombinedOutput()
	if err != nil {
		t.Fatalf("%v\n%s", err, out)
	}
}
