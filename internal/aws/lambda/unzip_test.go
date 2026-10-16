package lambda

import (
	"archive/zip"
	"bytes"
	"testing"
)

// The unzipped size is counted across all entries and capped.
func TestUnzipCap(t *testing.T) {
	defer func(old int64) { maxUnzipped = old }(maxUnzipped)
	maxUnzipped = 100
	zipOf := func(sizes ...int) []byte {
		var b bytes.Buffer
		zw := zip.NewWriter(&b)
		for i, n := range sizes {
			w, _ := zw.Create(string(rune('a'+i)) + ".txt")
			w.Write(make([]byte, n))
		}
		zw.Close()
		return b.Bytes()
	}
	if err := unzip(zipOf(60, 40), t.TempDir()); err != nil {
		t.Errorf("exactly at the cap: %v", err)
	}
	if err := unzip(zipOf(60, 41), t.TempDir()); err == nil {
		t.Error("101 bytes over two entries was accepted")
	}
}
