package terraform

import (
	"strings"
	"testing"
)

// A bad plan.json is refused in plain words; Go's own text may only follow after " — ".
func TestReadPlanErrorsArePlain(t *testing.T) {
	for name, body := range map[string]string{
		"cut off":     `{"format_version":"1.2","terraform_version":"1.9.5","planned_values":{"root_module":{"resources":[{"address":"aws_ins`,
		"binary":      "\x00\x01\x02",
		"wrong type":  `{"format_version":"1.2","terraform_version":5,"planned_values":"x"}`,
		"wrong entry": `{"format_version":"1.2","terraform_version":"1.9.5","resource_changes":[{"address":5}]}`,
	} {
		_, err := ReadPlan([]byte(body))
		if err == nil {
			t.Errorf("%s: want an error", name)
			continue
		}
		plain, _, _ := strings.Cut(err.Error(), " — ")
		for _, jargon := range []string{"json:", "Go struct", "unmarshal", "invalid character", "unexpected end"} {
			if strings.Contains(plain, jargon) {
				t.Errorf("%s: %q shows Go text before the dash", name, err)
			}
		}
	}
}
