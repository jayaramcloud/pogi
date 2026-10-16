package core

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/docker"
)

// ponytail: whole-state JSON snapshots keep blobs inline (base64); move large
// blobs to content-addressed files if snapshots get big.
type snapshotFile struct {
	Version  int                        `json:"version"`
	Saved    time.Time                  `json:"saved"`
	Pogi     string                     `json:"pogi"`
	Services map[string]json.RawMessage `json:"services"`
}

// Snapshot serializes the state of every stateful service.
func (h *Hub) Snapshot() ([]byte, error) {
	f := snapshotFile{Version: 1, Saved: time.Now().UTC(), Pogi: h.Cfg.Version, Services: map[string]json.RawMessage{}}
	for _, s := range h.services {
		st, ok := s.(Stateful)
		if !ok {
			continue
		}
		b, err := st.Save()
		if err != nil {
			return nil, fmt.Errorf("%s: %w", s.Info().Name, err)
		}
		f.Services[s.Info().Name] = b
	}
	return json.Marshal(f)
}

// Restore loads a snapshot; services missing from it are reset.
func (h *Hub) Restore(b []byte) error {
	var f snapshotFile
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("bad snapshot: %w", err)
	}
	defer h.syncAll() // the files and journals on disk no longer match
	for _, s := range h.services {
		st, ok := s.(Stateful)
		if !ok {
			continue
		}
		raw, ok := f.Services[s.Info().Name]
		if !ok {
			st.Reset()
			continue
		}
		if err := st.Load(raw); err != nil {
			return fmt.Errorf("%s: %w", s.Info().Name, err)
		}
	}
	return nil
}

// Reset clears the named services (all when none given), then runs the
// OnReset hooks (seeding from pogi.yaml).
func (h *Hub) Reset(names ...string) {
	for _, s := range h.services {
		if st, ok := s.(Stateful); ok && (len(names) == 0 || contains(names, s.Info().Name)) {
			st.Reset()
		}
	}
	h.mu.RLock()
	hooks := h.resets
	h.mu.RUnlock()
	for _, f := range hooks {
		f(names)
	}
	h.syncAll(names...)
}

// OnReset registers f to run after every Reset; services is empty for a full reset.
func (h *Hub) OnReset(f func(services []string)) {
	h.mu.Lock()
	h.resets = append(h.resets, f)
	h.mu.Unlock()
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

// MarkDirty does nothing: the storage loop finds changes made outside
// requests by hashing each service's state. Kept for the services calling it.
func (h *Hub) MarkDirty() {}

// Close stops background work of services that have any (Lambda processes,
// containers), then writes on-disk state one last time; called on shutdown.
func (h *Hub) Close() {
	d, before := h.Docker(), []docker.Summary(nil)
	if h.shuttingDown() {
		before = d.Mine(context.Background()) // to tell afterwards what was stopped or removed
	}
	h.AMQP().Close()
	for _, s := range h.services {
		if c, ok := s.(interface{ Close() error }); ok {
			if err := c.Close(); err != nil {
				h.Log.Warn("close", "service", s.Info().Name, "err", err)
			}
		}
	}
	h.syncAll()
	for _, st := range h.stores {
		st.j.Close()
	}
	d.Close()
	if before != nil {
		h.recordContainers(before, d.Mine(context.Background()))
	}
	h.endShutdown()
}

// recordContainers adds the containers that ran before shutdown to the shutdown
// list: gone from after means removed, there but not running means stopped.
func (h *Hub) recordContainers(before, after []docker.Summary) {
	left := map[string]string{}
	for _, s := range after {
		left[s.ID] = s.State
	}
	for _, s := range before {
		state, there := left[s.ID]
		if s.State != "running" || state == "running" {
			continue
		}
		verb := map[bool]string{true: "stopped", false: "removed"}[there]
		name := s.Labels[docker.LabelResource]
		if name == "" && len(s.Names) > 0 {
			name = strings.TrimPrefix(s.Names[0], "/")
		}
		h.Closed(cmp.Or(s.Labels[docker.LabelService], "docker"), "container", verb, name)
	}
}

// SaveFile writes a snapshot atomically.
func (h *Hub) SaveFile(path string) error {
	b, err := h.Snapshot()
	if err != nil {
		return err
	}
	return writeFile(path, b)
}

// writeFile replaces path atomically: a temp file, then a rename.
func writeFile(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadFile restores a snapshot file.
func (h *Hub) LoadFile(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return h.Restore(b)
}

var snapName = regexp.MustCompile(`^[A-Za-z0-9._-]{1,100}$`)

// SnapshotPath validates a snapshot name and returns its file path.
func (h *Hub) SnapshotPath(name string) (string, error) {
	if !snapName.MatchString(name) || strings.HasPrefix(name, ".") {
		return "", fmt.Errorf("invalid snapshot name %q (use letters, digits, . _ -)", name)
	}
	return filepath.Join(h.Cfg.DataDir, "snapshots", name+".json"), nil
}

// SnapshotInfo describes a saved snapshot.
type SnapshotInfo struct {
	Name  string    `json:"name"`
	Size  int64     `json:"size"`
	Saved time.Time `json:"saved"`
}

// Snapshots lists saved snapshots, newest first.
func (h *Hub) Snapshots() []SnapshotInfo {
	files, _ := filepath.Glob(filepath.Join(h.Cfg.DataDir, "snapshots", "*.json"))
	var out []SnapshotInfo
	for _, f := range files {
		if st, err := os.Stat(f); err == nil {
			out = append(out, SnapshotInfo{strings.TrimSuffix(filepath.Base(f), ".json"), st.Size(), st.ModTime()})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Saved.After(out[j].Saved) })
	return out
}

// ImportSnapshot validates an uploaded snapshot and stores it at path.
func (h *Hub) ImportSnapshot(path string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(b, new(snapshotFile)); err != nil {
		return fmt.Errorf("not a pogi snapshot: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

// DeleteSnapshot removes a saved snapshot.
func (h *Hub) DeleteSnapshot(path string) error { return os.Remove(path) }

// store is where one stateful service keeps its state on disk:
// DataDir/state/<service>.json, and <service>.wal for a journal.
type store struct {
	name, mode, path string // mode: hybrid | persistent | wal
	svc              Stateful
	j                *Journal   // wal mode, for services that journal
	mu               sync.Mutex // one save at a time
	sum              [32]byte   // of the file's bytes
}

// storageMode is memory, hybrid, persistent or wal; --persist means hybrid.
func (h *Hub) storageMode(name string) string {
	return cmp.Or(h.Cfg.StorageModes[name], h.Cfg.Storage, map[bool]string{true: "hybrid"}[h.Cfg.Persist], "memory")
}

// OpenStorage loads every service kept on disk from DataDir/state, after
// migrating the single DataDir/state.json of older versions, and replays the
// journals of wal-mode services. Call it once, before serving.
func (h *Hub) OpenStorage() error {
	dir := filepath.Join(h.Cfg.DataDir, "state")
	stores := map[string]*store{}
	for _, s := range h.services {
		name, mode := s.Info().Name, h.storageMode(s.Info().Name)
		svc, ok := s.(Stateful)
		if !ok || mode == "memory" {
			continue
		}
		if _, ok := s.(Journaled); !ok && mode == "wal" {
			mode = "persistent" // no journal: the whole file is written per change
		}
		stores[name] = &store{name: name, mode: mode, svc: svc, path: filepath.Join(dir, name+".json")}
	}
	if len(stores) == 0 {
		return nil
	}
	if err := migrate(h.Cfg.DataDir); err != nil {
		return err // stay in memory and leave state.json alone
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	h.stores = stores
	var errs []error
	for _, st := range stores {
		b, err := os.ReadFile(st.path)
		if err == nil {
			st.sum = sha256.Sum256(b)
			err = st.svc.Load(b)
		}
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, fmt.Errorf("%s: %w", st.name, err))
		}
		jr, ok := st.svc.(Journaled)
		if !ok {
			continue
		}
		wal := strings.TrimSuffix(st.path, ".json") + ".wal"
		j, err := OpenJournal(wal, jr.Apply)
		if err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", st.name, err))
		}
		if j == nil {
			if st.mode == "wal" {
				st.mode = "persistent" // no journal to write to: the whole file per change
			}
			continue
		}
		j.log = h.Log
		if st.mode == "wal" {
			jr.SetJournal(j)
			st.j = j
			continue
		}
		j.Close() // left by a wal-mode run: fold it into the file
		if st.save() == nil {
			os.Remove(wal)
			os.Remove(wal + ".old")
		}
	}
	return errors.Join(errs...)
}

// migrate splits DataDir/state.json into per-service files, once.
func migrate(dataDir string) error {
	old, dir := filepath.Join(dataDir, "state.json"), filepath.Join(dataDir, "state")
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	b, err := os.ReadFile(old)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var f snapshotFile
	if err := json.Unmarshal(b, &f); err != nil {
		return fmt.Errorf("migrate %s: %w", old, err)
	}
	tmp := dir + ".tmp" // renamed into place whole: a crash leaves no half-migrated state/
	os.RemoveAll(tmp)
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return err
	}
	for name, raw := range f.Services {
		if err := os.WriteFile(filepath.Join(tmp, name+".json"), raw, 0o600); err != nil {
			return err
		}
	}
	if err := os.Rename(tmp, dir); err != nil {
		return err
	}
	return os.Rename(old, old+".migrated")
}

// Flush keeps the files of on-disk services current until ctx ends: hybrid
// services every FlushInterval (default 5s), the others every second, which
// catches what services change in each other outside requests (SNS to SQS,
// CloudFormation). Journals past 8 MiB are compacted. Close writes once more.
// ponytail: each tick re-marshals every service to hash it; add per-service
// dirty bits if big states make the idle CPU matter.
func (h *Hub) Flush(ctx context.Context) {
	if len(h.stores) == 0 {
		return
	}
	slow := time.NewTicker(cmp.Or(h.Cfg.FlushInterval, 5*time.Second))
	fast := time.NewTicker(time.Second)
	defer slow.Stop()
	defer fast.Stop()
	for {
		hybrid := false
		select {
		case <-ctx.Done():
			return
		case <-slow.C:
			hybrid = true
		case <-fast.C:
		}
		for _, st := range h.stores {
			if (st.mode == "hybrid") == hybrid {
				func() { defer Recover("core.flush"); h.sync(st, false) }() // one bad save does not stop the rest
			}
		}
	}
}

// sync brings a service's file up to date; a journaled service compacts
// instead, when forced or once its journal passes 8 MiB.
func (h *Hub) sync(st *store, force bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	var err error
	switch {
	case st.j == nil:
		err = st.save()
	case force || st.j.Size() > 8<<20:
		err = st.j.Compact(st.save)
	}
	if err != nil {
		h.Log.Error("save state", "service", st.name, "err", err)
	}
}

// syncAll syncs the named services (all when none given), compacting
// journals: for changes that bypass them (reset, snapshot load) and on exit.
func (h *Hub) syncAll(names ...string) {
	for _, st := range h.stores {
		if len(names) == 0 || contains(names, st.name) {
			h.sync(st, true)
		}
	}
}

// save writes the service's file if its bytes changed; st.mu held.
func (st *store) save() error {
	b, err := st.svc.Save()
	if err != nil {
		return err
	}
	sum := sha256.Sum256(b)
	if sum == st.sum {
		return nil
	}
	if err := writeFile(st.path, b); err != nil {
		return err
	}
	st.sum = sum
	return nil
}

// InitHooks runs LocalStack-style init scripts and remembers their status.
type InitHooks struct {
	mu        sync.Mutex
	Completed map[string]bool `json:"completed"`
	Scripts   []InitScript    `json:"scripts"`
}

// InitScript is one executed hook.
type InitScript struct {
	Stage  string `json:"stage"`
	Name   string `json:"name"`
	State  string `json:"state"`
	Output string `json:"output,omitempty"`
}

// Run executes the scripts of a stage (boot, start, ready, shutdown) from dirs, in name order.
func (ih *InitHooks) Run(stage string, env []string, dirs ...string) {
	var files []string
	for _, d := range dirs {
		m, _ := filepath.Glob(filepath.Join(d, stage+".d", "*"))
		files = append(files, m...)
	}
	sort.Strings(files)
	for _, f := range files {
		st, err := os.Stat(f)
		if err != nil || st.IsDir() {
			continue
		}
		var cmd *exec.Cmd
		switch {
		case strings.HasSuffix(f, ".sh"):
			cmd = exec.Command("sh", f)
		case strings.HasSuffix(f, ".py"):
			cmd = exec.Command("python3", f)
		case st.Mode()&0o111 != 0:
			cmd = exec.Command(f)
		default:
			continue
		}
		cmd.Env = append(os.Environ(), env...)
		out, err := cmd.CombinedOutput()
		s := InitScript{Stage: strings.ToUpper(stage), Name: filepath.Base(f), State: "SUCCESSFUL", Output: tail(string(out), 4000)}
		if err != nil {
			s.State = "ERROR"
			s.Output += "\n" + err.Error()
		}
		ih.mu.Lock()
		ih.Scripts = append(ih.Scripts, s)
		ih.mu.Unlock()
	}
	ih.mu.Lock()
	if ih.Completed == nil {
		ih.Completed = map[string]bool{"BOOT": false, "START": false, "READY": false, "SHUTDOWN": false}
	}
	ih.Completed[strings.ToUpper(stage)] = true
	ih.mu.Unlock()
}

// Status returns a copy for the /_localstack/init endpoint.
func (ih *InitHooks) Status() map[string]any {
	ih.mu.Lock()
	defer ih.mu.Unlock()
	done := map[string]bool{"BOOT": false, "START": false, "READY": false, "SHUTDOWN": false}
	for k, v := range ih.Completed {
		done[k] = v
	}
	return map[string]any{"completed": done, "scripts": append([]InitScript{}, ih.Scripts...)}
}

func tail(s string, n int) string {
	if len(s) > n {
		return "…" + s[len(s)-n:]
	}
	return s
}
