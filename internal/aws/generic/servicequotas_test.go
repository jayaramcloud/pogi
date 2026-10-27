package generic

import (
	"regexp"
	"strings"
	"testing"
)

// Every catalog entry names an AWS page as its source.
func TestServiceQuotaSources(t *testing.T) {
	for _, q := range quotaCatalog {
		if !strings.HasPrefix(q.src, "https://docs.aws.amazon.com/") || !regexp.MustCompile(`^L-[0-9A-F]{8}$`).MatchString(q.code) {
			t.Errorf("%s %s: src %q", q.svc, q.code, q.src)
		}
	}
}
