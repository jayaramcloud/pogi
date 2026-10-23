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
	"strconv"
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
	f := snapshotFile{Version: snapshotFormat, Saved: time.Now().UTC(), Pogi: h.Cfg.Version, Services: map[string]json.RawMessage{}}
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

// snapshotFormat is the newest snapshot format this pogi reads and writes.
const snapshotFormat = 1

// parseSnapshot checks that b is a snapshot this pogi can read. what names it
// in messages ("this snapshot" or "snapshot <name>").
func parseSnapshot(what string, b []byte) (snapshotFile, error) {
	var f snapshotFile
	if err := json.Unmarshal(b, &f); err != nil {
		return f, fmt.Errorf("%s is not valid: it is not a pogi snapshot — %v", what, err)
	}
	if f.Version > snapshotFormat {
		return f, fmt.Errorf("%s is from a newer pogi (format %d; this pogi reads up to %d)", what, f.Version, snapshotFormat)
	}
	if len(f.Services) == 0 {
		return f, fmt.Errorf("%s is not valid: it has no services", what)
	}
	return f, nil
}

// Restore loads a snapshot; services missing from it are reset.
func (h *Hub) Restore(b []byte) error { return h.restore("this snapshot", b) }

// restore is all or nothing: if any service fails to load, every service is
// put back as it was and the error names the one that failed.
func (h *Hub) restore(what string, b []byte) error {
	f, err := parseSnapshot(what, b)
	if err != nil {
		return err
	}
	known := map[string]bool{}
	var undo []func()
	defer h.syncAll() // the files and journals on disk no longer match
	for _, s := range h.services {
		st, ok := s.(Stateful)
		if !ok {
			continue
		}
		name := s.Info().Name
		known[name] = true
		raw, ok := f.Services[name]
		if !ok {
			continue
		}
		before, err := st.Save()
		if err == nil {
			undo = append(undo, func() { st.Load(before) })
			err = st.Load(raw)
		}
		if err != nil {
			for i := len(undo) - 1; i >= 0; i-- {
				undo[i]()
			}
			return fmt.Errorf("%s is not valid: %s — %v", what, name, err)
		}
	}
	for _, s := range h.services {
		if st, ok := s.(Stateful); ok && f.Services[s.Info().Name] == nil {
			st.Reset()
		}
	}
	for name := range f.Services {
		if !known[name] {
			msg := fmt.Sprintf("%s has data for %q, which this pogi does not know; that part was skipped", what, name)
			h.Log.Warn(msg)
			h.probs.set("snapshot:"+name, "state", msg)
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
// A second call does nothing, so a test may stop a server early and still
// leave its cleanup in place (a service closing twice used to panic).
func (h *Hub) Close() { h.closeOnce.Do(h.close) }

func (h *Hub) close() {
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
	if h.lock != nil {
		h.lock.Close() // drops the data folder lock
		h.lock = nil
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

// writeFile replaces path atomically: write a temp file and fsync it, rename
// it over path, then fsync the folder so the rename survives a power cut.
// (No test can cut power; the test only checks the file is written and replaced.)
func writeFile(path string, b []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	if d, err := os.Open(dir); err == nil { // folders can't be synced on Windows; ignore that
		d.Sync()
		d.Close()
	}
	return nil
}

// LoadFile restores a snapshot file.
func (h *Hub) LoadFile(path string) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return h.restore("snapshot "+strings.TrimSuffix(filepath.Base(path), ".json"), b)
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
	if _, err := parseSnapshot("this snapshot", b); err != nil {
		return err
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
	failed           string     // the last save error, so one kind is logged once; st.mu held
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
	if err := h.lockDataDir(); err != nil {
		return err
	}
	fail := func(err error) error {
		h.lock.Close()
		h.lock = nil
		return err
	}
	if err := migrate(h.Cfg.DataDir); err != nil {
		return fail(err) // state.json is left alone
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fail(fmt.Errorf("pogi can't use the data folder %s: %w — fix its permissions or use another --data-dir", h.Cfg.DataDir, err))
	}
	h.stores = stores
	var errs []error
	for _, st := range stores {
		b, err := os.ReadFile(st.path)
		if errors.Is(err, os.ErrNotExist) {
			err = nil
		} else if err != nil {
			return fail(fmt.Errorf("pogi can't read the saved state of %s: %w", st.name, err))
		} else {
			st.sum = sha256.Sum256(b)
			if err = st.svc.Load(b); err != nil {
				if _, qerr := h.quarantine(st, err); qerr != nil {
					return fail(qerr)
				}
				err = nil
			}
		}
		if err != nil {
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

// lockDataDir takes an exclusive lock on DataDir/.lock, so two pogis never
// write the same state files. The file holds the pid of the owner.
func (h *Hub) lockDataDir() error {
	dir := h.Cfg.DataDir
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("pogi can't use the data folder %s: %w — fix its permissions or use another --data-dir", dir, err)
	}
	path := filepath.Join(dir, ".lock")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return fmt.Errorf("pogi can't use the data folder %s: %w — fix its permissions or use another --data-dir", dir, err)
	}
	if err := lockFile(f); err != nil {
		pid, _ := os.ReadFile(path)
		f.Close()
		if p := strings.TrimSpace(string(pid)); p != "" {
			return fmt.Errorf("another pogi (pid %s) is using %s; stop it or use another --data-dir", p, dir)
		}
		return fmt.Errorf("another pogi is using %s; stop it or use another --data-dir", dir)
	}
	f.Truncate(0)
	f.WriteAt([]byte(strconv.Itoa(os.Getpid())+"\n"), 0)
	h.lock = f
	return nil
}

// quarantine keeps a state file that will not load as <name>.corrupt-<time>,
// empties the service and tells the user. It returns the new name. An error
// means the file could not be moved: then pogi must not start, or the next
// save would overwrite it.
func (h *Hub) quarantine(st *store, loadErr error) (string, error) {
	kept := st.path + ".corrupt-" + time.Now().Format("20060102-150405")
	if err := os.Rename(st.path, kept); err != nil {
		return "", fmt.Errorf("could not read the saved state of %s (%v) and could not move it aside: %w", st.name, loadErr, err)
	}
	st.svc.Reset()
	st.sum = [32]byte{}
	msg := fmt.Sprintf("could not read the saved state of %s (kept as %s); it starts empty", st.name, kept)
	h.Log.Error(msg, "err", loadErr)
	h.probs.set("corrupt:"+st.name, "state", msg)
	return kept, nil
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
	tried := true
	switch {
	case st.j == nil:
		err = st.save()
	case force || st.j.Size() > 8<<20:
		err = st.j.Compact(st.save)
	default:
		tried = false
	}
	switch {
	case err != nil:
		if err.Error() != st.failed { // one line per kind of error, not one per tick
			h.Log.Error("save state", "service", st.name, "err", err)
		}
		st.failed = err.Error()
		h.probs.set("save:"+st.name, "state", fmt.Sprintf("pogi can't save its state: %v. Changes made now are lost if it stops.", err))
	case tried && st.failed != "":
		h.Log.Info("saving state works again", "service", st.name)
		st.failed = ""
		h.probs.clear("save:" + st.name)
	}
}

// Problem is something wrong with pogi as a whole, shown in status, the
// console banner and the bell.
type Problem struct {
	Time time.Time `json:"time"`
	Kind string    `json:"kind"` // state: saved state can't be read or written; port: a port pogi could not open
	Text string    `json:"text"`
}

type problems struct {
	mu   sync.Mutex
	list map[string]Problem
}

func (p *problems) set(key, kind, text string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if old, ok := p.list[key]; ok && old.Text == text {
		return
	}
	if p.list == nil {
		p.list = map[string]Problem{}
	}
	p.list[key] = Problem{time.Now(), kind, text}
}

// PortProblem records that a service's own port could not be opened, so that part of pogi is off.
// It stays for as long as pogi runs: the port is not tried again.
func (h *Hub) PortProblem(service, text string) { h.probs.set("port:"+service, "port", text) }

func (p *problems) clear(key string) {
	p.mu.Lock()
	delete(p.list, key)
	p.mu.Unlock()
}

// Problems lists what is wrong with pogi now, oldest first.
func (h *Hub) Problems() []Problem {
	h.probs.mu.Lock()
	defer h.probs.mu.Unlock()
	var out []Problem
	for _, p := range h.probs.list {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Time.Before(out[j].Time) })
	return out
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
