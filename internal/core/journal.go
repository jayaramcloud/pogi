package core

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
)

// Journaled services log each change of their high-write records to a
// Journal in wal mode. The hub calls SetJournal after Load, and Apply to
// replay a record at startup; a nil value means delete.
type Journaled interface {
	SetJournal(*Journal)
	Apply(key string, value []byte) error
}

// Journal is an append-only file of newline-delimited JSON records, one per
// change: {"op":"put"|"del","key":…,"value":…}. A nil *Journal (memory,
// hybrid and persistent modes) ignores everything, so services just call it.
type Journal struct {
	mu   sync.Mutex
	f    *os.File
	path string
	size int64
	log  *slog.Logger
}

type record struct {
	Op    string          `json:"op"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value,omitempty"`
}

// OpenJournal replays path.old (left by an interrupted compaction), then
// path, through apply, and opens path for appending. A torn last line from a
// crash is dropped; other bad records are skipped and reported in err, and
// the journal is still returned.
func OpenJournal(path string, apply func(key string, value []byte) error) (*Journal, error) {
	var errs []error
	for _, p := range []string{path + ".old", path} {
		b, err := os.ReadFile(p)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		n := bytes.LastIndexByte(b, '\n') + 1 // past the last complete line
		for line := range bytes.Lines(b[:n]) {
			var r record
			if err := json.Unmarshal(line, &r); err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", p, err))
				continue
			}
			if r.Op == "del" {
				r.Value = nil
			}
			if err := apply(r.Key, r.Value); err != nil {
				errs = append(errs, fmt.Errorf("%s: %s: %w", p, r.Key, err))
			}
		}
		if p == path && n < len(b) {
			if err := os.Truncate(p, int64(n)); err != nil { // appends must not extend the torn line
				return nil, err
			}
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Journal{f: f, path: path, size: st.Size()}, errors.Join(errs...)
}

// Put records the new value of key. Services call it under their own lock,
// so records land in the order of the changes.
func (j *Journal) Put(key string, v any) {
	if j == nil {
		return
	}
	b, err := json.Marshal(v)
	if err != nil {
		j.fail(err)
		return
	}
	j.append(record{"put", key, b})
}

// Delete records that key is gone.
func (j *Journal) Delete(key string) {
	if j == nil {
		return
	}
	j.append(record{Op: "del", Key: key})
}

// append writes one line. A completed write(2) survives kill -9; there is no
// fsync per record.
func (j *Journal) append(r record) {
	line, err := json.Marshal(r)
	if err != nil {
		j.fail(err)
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	n, err := j.f.Write(append(line, '\n'))
	j.size += int64(n)
	if err != nil {
		j.fail(err)
	}
}

// fail reports a record that did not reach the file; the change is still in
// memory and lands in the next snapshot.
func (j *Journal) fail(err error) {
	if j.log != nil {
		j.log.Error("journal write", "file", j.path, "err", err)
	}
}

// Size is the journal's length in bytes.
func (j *Journal) Size() int64 {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.size
}

// Compact folds the journal into a snapshot: it moves path to path.old under
// the journal lock, then, without it (services call Put under their own
// lock), runs snapshot, which saves the service and writes its file, and
// finally deletes path.old. If a failed compaction left a path.old, that one
// is kept and path is not rotated; replaying both stays correct.
func (j *Journal) Compact(snapshot func() error) error {
	old := j.path + ".old"
	j.mu.Lock()
	if _, err := os.Stat(old); err != nil {
		j.f.Close() // Windows cannot rename an open file
		moved := os.Rename(j.path, old)
		f, err := os.OpenFile(j.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600) // new, or path again if not moved
		if err == nil {
			j.f = f
		}
		if err := errors.Join(moved, err); err != nil {
			j.mu.Unlock()
			return err
		}
		j.size = 0
	}
	j.mu.Unlock()
	if err := snapshot(); err != nil {
		return err
	}
	return os.Remove(old)
}

// Close closes the file.
func (j *Journal) Close() error {
	if j == nil {
		return nil
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.f.Close()
}

// Replay is the usual body of Apply: it sets m[key] from a record's value,
// or deletes the key when the value is nil.
func Replay[V any](m map[string]V, key string, value []byte) error {
	if value == nil {
		delete(m, key)
		return nil
	}
	var v V
	if err := json.Unmarshal(value, &v); err != nil {
		return err
	}
	m[key] = v
	return nil
}

// JournalTest gives svc a journal for a test and returns a check that
// replays it into fresh, which must then save the same state as svc.
func JournalTest[S interface {
	Journaled
	Stateful
}](tb TB, svc, fresh S) func() {
	path := filepath.Join(tb.TempDir(), "test.wal")
	j, err := OpenJournal(path, svc.Apply)
	if err != nil {
		tb.Fatalf("journal: %v", err)
	}
	svc.SetJournal(j)
	return func() {
		tb.Helper()
		j.Close()
		again, err := OpenJournal(path, fresh.Apply)
		if err != nil {
			tb.Fatalf("replay: %v", err)
		}
		again.Close()
		want, _ := svc.Save()
		got, _ := fresh.Save()
		if !bytes.Equal(got, want) {
			tb.Fatalf("replayed state differs\n got: %s\nwant: %s", got, want)
		}
	}
}
