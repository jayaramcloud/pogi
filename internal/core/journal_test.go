package core

import (
	"encoding/json"
	"errors"
	"maps"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// open replays the journal at path over a copy of base, as a restart would.
func open(t *testing.T, path string, base map[string]int) (*Journal, map[string]int) {
	t.Helper()
	m := maps.Clone(base)
	if m == nil {
		m = map[string]int{}
	}
	j, err := OpenJournal(path, func(k string, v []byte) error { return Replay(m, k, v) })
	if err != nil {
		t.Fatal(err)
	}
	return j, m
}

func TestJournalReplay(t *testing.T) {
	var none *Journal // memory mode: services call it anyway
	none.Put("a", 1)
	none.Delete("a")

	path := filepath.Join(t.TempDir(), "svc.wal")
	j, _ := open(t, path, nil)
	j.Put("a", 1)
	j.Put("b", 2)
	j.Delete("a")
	j.Put("b", 3)
	j.Put("c", 4)
	j.Close()
	var m map[string]int
	if j, m = open(t, path, nil); !maps.Equal(m, map[string]int{"b": 3, "c": 4}) {
		t.Fatalf("replayed %v", m)
	}
	j.Close()

	// A crash mid-write leaves a torn last line: it is dropped, and the next
	// record starts on a line of its own.
	f, _ := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	f.WriteString(`{"op":"put","key":"d","val`)
	f.Close()
	j, m = open(t, path, nil)
	if !maps.Equal(m, map[string]int{"b": 3, "c": 4}) {
		t.Fatalf("torn tail: %v", m)
	}
	j.Put("e", 5)
	j.Close()
	if j, m = open(t, path, nil); !maps.Equal(m, map[string]int{"b": 3, "c": 4, "e": 5}) {
		t.Fatalf("after the torn tail: %v", m)
	}
	j.Close()
}

func TestJournalCompaction(t *testing.T) {
	dir := t.TempDir()
	path, snap := filepath.Join(dir, "svc.wal"), filepath.Join(dir, "svc.json")
	// state is the service: the journal mirrors every change made to it.
	state := map[string]int{}
	j, _ := open(t, path, nil)
	set := func(k string, v int) { state[k] = v; j.Put(k, v) }
	snapshot := func() error {
		b, _ := json.Marshal(state)
		return writeFile(snap, b)
	}
	restart := func() map[string]int {
		t.Helper()
		j.Close()
		var base map[string]int
		if b, err := os.ReadFile(snap); err == nil {
			json.Unmarshal(b, &base)
		}
		var m map[string]int
		j, m = open(t, path, base)
		return m
	}

	set("a", 1)
	set("b", 2)
	// A crash between the rotation and the deletion of .wal.old.
	if err := j.Compact(func() error { return errors.New("killed") }); err == nil {
		t.Fatal("want the snapshot error")
	}
	set("c", 3)
	if m := restart(); !maps.Equal(m, state) {
		t.Fatalf("after a crash in compaction: %v, want %v", m, state)
	}

	// The next compaction keeps the old .wal.old and finishes the job.
	if err := j.Compact(snapshot); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".old"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf(".wal.old is still there: %v", err)
	}
	set("a", 10)
	delete(state, "b")
	j.Delete("b")
	if m := restart(); !maps.Equal(m, state) {
		t.Fatalf("after compaction: %v, want %v", m, state)
	}

	// A normal compaction rotates: the journal starts empty.
	if err := j.Compact(snapshot); err != nil {
		t.Fatal(err)
	}
	if j.Size() != 0 {
		t.Fatalf("size after compaction = %d", j.Size())
	}
	set("d", 4)
	if m := restart(); !maps.Equal(m, state) {
		t.Fatalf("after rotation: %v, want %v", m, state)
	}

	// A crash after the snapshot is written, before .wal.old goes: its
	// records replay over a snapshot that already has them.
	set("d", 5)
	j.Compact(func() error { snapshot(); return errors.New("killed") })
	set("e", 6)
	if m := restart(); !maps.Equal(m, state) {
		t.Fatalf("after a crash past the snapshot: %v, want %v", m, state)
	}
	j.Close()
}

// TestPrivateFiles: state and journals are readable by the owner only.
func TestPrivateFiles(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no unix modes")
	}
	dir := filepath.Join(t.TempDir(), "state")
	if err := writeFile(filepath.Join(dir, "a.json"), []byte("{}")); err != nil {
		t.Fatal(err)
	}
	j, _ := open(t, filepath.Join(dir, "a.wal"), nil)
	j.Close()
	for path, want := range map[string]os.FileMode{dir: 0o700, filepath.Join(dir, "a.json"): 0o600, filepath.Join(dir, "a.wal"): 0o600} {
		if fi, err := os.Stat(path); err != nil || fi.Mode().Perm() != want {
			t.Errorf("%s = %v, %v; want %v", path, fi, err, want)
		}
	}
}
