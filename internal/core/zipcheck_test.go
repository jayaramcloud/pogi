package core

import (
	"archive/zip"
	"bytes"
	"testing"
)

func TestCheckZipPathsRefusesEscapes(t *testing.T) {
	mk := func(name string) []byte {
		var b bytes.Buffer
		w := zip.NewWriter(&b)
		w.Create(name)
		w.Close()
		return b.Bytes()
	}
	for name, bad := range map[string]bool{"index.py": false, "pkg/a.py": false, "../x": true, "a/../../x": true, "/abs": true} {
		if err := CheckZipPaths(mk(name)); (err != nil) != bad {
			t.Errorf("%q: err = %v, want refused = %v", name, err, bad)
		}
	}
	if err := CheckZipPaths([]byte("not a zip")); err != nil {
		t.Errorf("non-zip data is the caller's to report, got %v", err)
	}
}
