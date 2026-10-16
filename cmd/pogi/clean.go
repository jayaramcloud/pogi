package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
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
	fs := flag.NewFlagSet("clean", flag.ExitOnError)
	dataDir := fs.String("data-dir", "", "pogi's data dir (default: what `pogi start` uses)")
	yes := fs.Bool("yes", false, "delete the containers and volumes without asking")
	dry := fs.Bool("dry-run", false, "only list what would be deleted")
	state := fs.Bool("state", false, "also delete the saved state, without asking")
	fs.Parse(args)

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
		fmt.Print(q + " [y/N] ")
		line, _ := in.ReadString('\n')
		return strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "y")
	}

	ctx := context.Background()
	d := docker.New(docker.Options{})
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
		fmt.Print("Left by a pogi that is no longer running:\n", describeLeft(cs, vs))
		if *dry || !*yes && !ask("Delete these?") {
			break
		}
		failed := 0
		for _, c := range cs {
			if d.Remove(ctx, c.ID) != nil {
				failed++
			}
		}
		for _, v := range vs {
			if d.RemoveVolume(ctx, v.Name) != nil {
				failed++
			}
		}
		fmt.Printf("Deleted %d %s and %d %s", len(cs), plural("container", len(cs)), len(vs), plural("volume", len(vs)))
		if failed > 0 {
			fmt.Printf(" (%d could not be deleted)", failed)
		}
		fmt.Println(".")
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
	return nil
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
		name := c.ID
		if len(c.Names) > 0 {
			name = strings.TrimPrefix(c.Names[0], "/")
		}
		add(c.Labels[docker.LabelService], "container", name)
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
