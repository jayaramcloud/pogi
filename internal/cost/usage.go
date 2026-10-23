package cost

import (
	"bytes"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// MaxUsageFile is the largest usage file pogi reads.
const MaxUsageFile = 1 << 20

var (
	reAddress = regexp.MustCompile(`^[A-Za-z0-9_.\-\[\]"/:*]+$`)
	reKey     = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

// ReadUsage reads a usage file: YAML keyed by resource address, each value a map of snake_case keys
// to numbers (or to a string for "os", which is linux or windows).
func ReadUsage(data []byte) (map[string]map[string]any, error) {
	if len(data) > MaxUsageFile {
		return nil, fmt.Errorf("usage file is larger than %d MB", MaxUsageFile>>20)
	}
	var raw map[string]map[string]any
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("usage file: %w", err)
	}
	for addr, keys := range raw {
		if !reAddress.MatchString(addr) || !strings.Contains(addr, ".") {
			return nil, fmt.Errorf("usage file: %q is not a resource address (like aws_instance.web)", addr)
		}
		for k, v := range keys {
			if !reKey.MatchString(k) {
				return nil, fmt.Errorf("usage file: %s: key %q must be snake_case (like monthly_requests)", addr, k)
			}
			if k == "os" {
				if s, _ := v.(string); s != "linux" && s != "windows" {
					return nil, fmt.Errorf("usage file: %s: os must be linux or windows, not %v", addr, v)
				}
				continue
			}
			switch n := v.(type) {
			case int:
				if n < 0 {
					return nil, fmt.Errorf("usage file: %s: %s must not be negative", addr, k)
				}
			case float64:
				if n < 0 {
					return nil, fmt.Errorf("usage file: %s: %s must not be negative", addr, k)
				}
			default:
				return nil, fmt.Errorf("usage file: %s: %s must be a number, not %v", addr, k, v)
			}
		}
	}
	return raw, nil
}

// UsageTemplate returns a usage file with every key the estimate's rows read, set to 0.
func UsageTemplate(e *Estimate) []byte {
	var b bytes.Buffer
	b.WriteString("# Usage numbers for pogi cost. Fill in the zeros; pogi prices usage as 0 until you do.\n")
	rows := map[string][]string{}
	for _, r := range e.Rows {
		if len(r.UsageKeys) > 0 {
			rows[r.Address] = slices.Sorted(slices.Values(r.UsageKeys))
		}
	}
	for _, addr := range slices.Sorted(maps.Keys(rows)) {
		fmt.Fprintf(&b, "%s:\n", addr)
		for _, k := range rows[addr] {
			if k == "os" {
				fmt.Fprintf(&b, "  os: linux\n")
			} else {
				fmt.Fprintf(&b, "  %s: 0\n", k)
			}
		}
	}
	return b.Bytes()
}
