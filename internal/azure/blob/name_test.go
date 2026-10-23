package blob

import "testing"

func TestContainerNames(t *testing.T) {
	for name, ok := range map[string]bool{
		"logs": true, "a-b-1": true, "$root": true, "$web": true, "$logs": true,
		"ab": false, "Logs": false, "a--b": false, "-ab": false, "a_b": false, "$other": false,
	} {
		if got := checkContainerName(name) == nil; got != ok {
			t.Errorf("%q: allowed=%v, want %v", name, got, ok)
		}
	}
}
