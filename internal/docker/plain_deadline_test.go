package docker

import (
	"context"
	"strings"
	"testing"
)

// A pull that ends on a deadline reads like a registry that does not answer.
func TestPlainPullDeadlineBlamesRegistry(t *testing.T) {
	err := &pullError{"example.invalid/x:1", context.DeadlineExceeded}
	if p := Plain(err); !strings.HasPrefix(p, "The registry can't be reached") {
		t.Fatalf("Plain = %q", p)
	}
}
