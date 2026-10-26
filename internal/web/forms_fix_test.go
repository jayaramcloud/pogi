package web

import (
	"testing"

	"github.com/nrndev/pogi/internal/core"
)

func TestSentence(t *testing.T) {
	for in, want := range map[string]string{
		"CreateNodePool":   "Create node pool",
		"CreateDBInstance": "Create DB instance",
		"PutBucketPolicy":  "Put bucket policy",
		"CreateCluster":    "Create cluster",
		"":                 "",
	} {
		if got := core.Sentence(in); got != want {
			t.Errorf("Sentence(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPrefillLocation(t *testing.T) {
	sc := scope{Project: "pogi"}
	for in, want := range map[string]string{
		"projects/*/locations/*":            "projects/pogi/locations/us-central1",
		"projects/*/locations/*/keyRings/*": "projects/pogi/locations/us-central1/keyRings/",
		"projects/*/topics/*":               "projects/pogi/topics/",
	} {
		if got := prefill(in, sc); got != want {
			t.Errorf("prefill(%q) = %q, want %q", in, got, want)
		}
	}
}
