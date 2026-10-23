package core

import (
	"os"
	"path/filepath"
	"testing"
)

// The fsync calls can't be tested without cutting power; this checks the
// writer still creates the file, replaces it whole and leaves no temp file.
func TestWriteFileReplaces(t *testing.T) {
	p := filepath.Join(t.TempDir(), "sub", "a.json")
	for _, want := range []string{"first", "second"} {
		if err := writeFile(p, []byte(want)); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(p); string(got) != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	if _, err := os.Stat(p + ".tmp"); err == nil {
		t.Fatal("temp file left behind")
	}
}
