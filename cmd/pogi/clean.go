package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/nrndev/pogi/internal/docker"
)

// clean deletes the containers and volumes that a pogi which no longer runs
// left in Docker, and, when asked, the saved state. It works without a
// running pogi and talks to Docker directly.
func clean(args []string) error {
	fs := flag.NewFlagSet("clean", flag.ContinueOnError)
	setUsage(fs, "Delete the containers and volumes a pogi that is no longer running left in Docker, and, if asked, its saved state.",
		"pogi clean [flags]", "pogi clean --dry-run")
	dataDir := fs.String("data-dir", "", "pogi's data dir (default: what pogi start uses)")
	yes := fs.Bool("yes", false, "delete the containers and volumes without asking")
	dry := fs.Bool("dry-run", false, "only list what would be deleted")
	state := fs.Bool("state", false, "also delete the saved state, without asking")
	if _, err := parseMax(fs, args, 0); err != nil {
		return err
	}
	term := isTerminal(os.Stdin)
	if !term && !*yes && !*dry {
		return usagef("clean: stdin is not a terminal, so pogi cannot ask: pass --yes to delete, or --dry-run to only list")
	}

	dir := *dataDir
	if dir == "" {
		cfg, err := envConfig("")
		if err != nil {
			return err
		}
		dir = cfg.DataDir
	}
	dir, err := filepath.Abs(defaultDataDir(dir))
	if err != nil {
		return err
	}
	in := bufio.NewReader(os.Stdin)
	ask := func(q string) bool {
		if !term {
			return false // no one to ask: --yes covers the containers, --state the saved state
		}
		fmt.Print(q + " [y/N] ")
		line, _ := in.ReadString('\n')
		return strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "y")
	}

	ctx := context.Background()
	d := docker.New(docker.Options{})
	failed := 0
	switch {
	case !d.Available():
		fmt.Println("No container engine is running, so there are no containers to check.")
	default:
		cs, err := d.List(ctx, docker.LabelManaged+"=true")
		if err != nil {
			return err
		}
		vs, err := d.Volumes(ctx, docker.LabelManaged+"=true")
		if err != nil {
			return err
		}
		cs, vs = orphans(cs, func(c docker.Summary) map[string]string { return c.Labels }), orphans(vs, func(v docker.Volume) map[string]string { return v.Labels })
		if len(cs)+len(vs) == 0 {
			fmt.Println("Nothing to clean: no containers or volumes were left by a pogi that is no longer running.")
			break
		}
		// Those kept for the saved state of another data dir are listed, but only `--data-dir <that dir>` deletes them.
		cs, otherC := splitKept(cs, dir, func(c docker.Summary) map[string]string { return c.Labels })
		vs, otherV := splitKept(vs, dir, func(v docker.Volume) map[string]string { return v.Labels })
		if len(cs)+len(vs) > 0 {
			fmt.Print("Left by a pogi that is no longer running:\n", describeLeft(cs, vs))
		}
		fmt.Print(describeKept(otherC, otherV))
		if len(cs)+len(vs) == 0 || *dry || !*yes && !ask("Delete these?") {
			break
		}
		nc, fc := deleteEach(os.Stderr, cs, func(c docker.Summary) string { return "container " + containerName(c) },
			func(c docker.Summary) error { return d.Remove(ctx, c.ID) })
		nv, fv := deleteEach(os.Stderr, vs, func(v docker.Volume) string { return "volume " + v.Name },
			func(v docker.Volume) error { return d.RemoveVolume(ctx, v.Name) })
		failed = fc + fv
		fmt.Printf("Deleted %d %s and %d %s.\n", nc, plural("container", nc), nv, plural("volume", nv))
	}

	files := stateFiles(dir)
	switch {
	case len(files) == 0:
	case docker.DataInUse(dir):
		msg := "A pogi is running on " + dir + ": stop it before deleting its saved state."
		if *state {
			return fmt.Errorf("%s", msg)
		}
		fmt.Println(msg)
	case *dry:
		fmt.Println("Saved state in", dir, "would be asked about.")
	case *state || ask("Also delete saved state in "+dir+"?"):
		for _, f := range files {
			if err := os.RemoveAll(f); err != nil {
				return err
			}
		}
		fmt.Println("Deleted the saved state in", dir+".")
	}
	if failed > 0 {
		return fmt.Errorf("%d could not be deleted (see why above)", failed)
	}
	return nil
}

// deleteEach deletes each item with del, writes to w why any could not be deleted, and returns how
// many were deleted and how many were not.
func deleteEach[T any](w io.Writer, list []T, name func(T) string, del func(T) error) (deleted, failed int) {
	for _, x := range list {
		if err := del(x); err != nil {
			fmt.Fprintf(w, "could not delete %s: %v\n", name(x), err)
			failed++
			continue
		}
		deleted++
	}
	return deleted, failed
}

// containerName is a container's name without Docker's leading slash, or its ID when it has none.
func containerName(c docker.Summary) string {
	if len(c.Names) > 0 {
		return strings.TrimPrefix(c.Names[0], "/")
	}
	return c.ID
}

// stateFiles are what pogi saved in a data dir: its state folder (and the single file of older versions).
// The CA, snapshots, init scripts and SDKs stay.
func stateFiles(dir string) []string {
	var out []string
	for _, n := range []string{"state", "state.json"} {
		if _, err := os.Stat(filepath.Join(dir, n)); err == nil {
			out = append(out, filepath.Join(dir, n))
		}
	}
	return out
}

// splitKept takes out of list what is kept for the saved state of a data dir other than dir.
func splitKept[T any](list []T, dir string, labels func(T) map[string]string) (mine []T, kept map[string][]T) {
	kept = map[string][]T{}
	for _, x := range list {
		if d := labels(x)[docker.LabelData]; d != "" && d != dir {
			kept[d] = append(kept[d], x)
		} else {
			mine = append(mine, x)
		}
	}
	return mine, kept
}

// describeKept lists, by data dir, the containers and volumes kept for saved state that no pogi runs on, and
// says how to delete them.
func describeKept(cs map[string][]docker.Summary, vs map[string][]docker.Volume) string {
	dirs := map[string]bool{}
	for d := range cs {
		dirs[d] = true
	}
	for d := range vs {
		dirs[d] = true
	}
	var b strings.Builder
	for _, d := range slices.Sorted(maps.Keys(dirs)) {
		fmt.Fprintf(&b, "Kept for the saved state in %s (no pogi runs on it; it resumes them when it starts):\n%s  To delete them: pogi clean --data-dir %s\n",
			d, describeLeft(cs[d], vs[d]), d)
	}
	return b.String()
}

// orphans keeps what a pogi that is gone left behind (see docker.Orphan).
func orphans[T any](list []T, labels func(T) map[string]string) []T {
	return slices.DeleteFunc(slices.Clone(list), func(x T) bool { return !docker.Orphan(labels(x)) })
}

// describeLeft lists what clean would delete: a line per service and kind, with every name.
func describeLeft(cs []docker.Summary, vs []docker.Volume) string {
	type key struct{ service, kind string }
	var keys []key
	names := map[key][]string{}
	add := func(service, kind, name string) {
		k := key{service, kind}
		if _, ok := names[k]; !ok {
			keys = append(keys, k)
		}
		names[k] = append(names[k], name)
	}
	for _, c := range cs {
		add(c.Labels[docker.LabelService], "container", containerName(c))
	}
	for _, v := range vs {
		add(v.Labels[docker.LabelService], "volume", v.Name)
	}
	slices.SortFunc(keys, func(a, b key) int { return strings.Compare(a.service+"\x00"+a.kind, b.service+"\x00"+b.kind) })
	var b strings.Builder
	tw := tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
	for _, k := range keys {
		n := slices.Sorted(slices.Values(names[k]))
		noun := k.kind
		if len(n) != 1 {
			noun += "s"
		}
		fmt.Fprintf(tw, "  %s\t%d %s\t%s\n", k.service, len(n), noun, strings.Join(n, ", "))
	}
	tw.Flush()
	return b.String()
}

func plural(noun string, n int) string {
	if n == 1 {
		return noun
	}
	return noun + "s"
}
