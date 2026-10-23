package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"time"

	"github.com/nrndev/pogi/internal/aws/eks"
	"github.com/nrndev/pogi/internal/aws/elasticache"
	"github.com/nrndev/pogi/internal/aws/kafka"
	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/nrndev/pogi/internal/gcp/spanner"
	gcpsql "github.com/nrndev/pogi/internal/gcp/sql"
	tfmodel "github.com/nrndev/pogi/internal/terraform"
)

// pull downloads the images a stack needs before pogi runs it, so the first apply does not wait for them.
func pull(args []string, out io.Writer) error {
	fs := flag.NewFlagSet("pull", flag.ContinueOnError)
	setUsage(fs, "Download the container images a Terraform folder or plan needs, so the first apply does not wait.",
		"pogi pull <folder | plan.json>\npogi pull --all\npogi pull --list [folder | plan.json]", "pogi pull ./infra")
	all := fs.Bool("all", false, "the default image of every container-backed service (CI warm-up)")
	list := fs.Bool("list", false, "list the images pogi can use (or the stack's), and whether each is present")
	pos, err := parseArgs(fs, args) // flags may come before or after the path
	if err != nil {
		return err
	}
	if len(pos) > 1 || len(pos) == 0 && !*all && !*list || len(pos) == 1 && *all {
		return usagef("usage: pogi pull <terraform folder | plan.json>, or pogi pull --all, or pogi pull --list [path]")
	}
	var wants []want
	if len(pos) == 1 {
		if wants, err = stackImages(pos[0]); err != nil {
			return err
		}
		if len(wants) == 0 {
			fmt.Fprintf(out, "Nothing to pull: %s has no resources that run in a container.\n", pos[0])
			return nil
		}
	} else {
		wants = allImages()
	}
	d, err := pullClient()
	if err != nil {
		return err
	}
	images := resolve(d, wants)
	if !d.Available() {
		return errors.New("Docker isn't running: start Docker, Podman, Colima or OrbStack, or set DOCKER_HOST")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if *list {
		for _, im := range images {
			have, err := d.HasImage(ctx, im)
			if err != nil {
				return err
			}
			fmt.Fprintf(out, "%-45s %s\n", im, map[bool]string{true: "present", false: "not downloaded"}[have])
		}
		return nil
	}
	failed := 0
	for _, im := range images {
		if !pullOne(ctx, d, im, out) {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d images could not be downloaded", failed, len(images))
	}
	return nil
}

// pullClient is a Docker client set up like the server's: pogi.yaml's docker.host and docker.images apply.
func pullClient() (*docker.Client, error) {
	cfg, err := envConfig("")
	if err != nil {
		return nil, err
	}
	return docker.New(docker.Options{Host: cfg.DockerHost, Network: cfg.DockerNetwork, Images: cfg.DockerImages}), nil
}

// want is an image and the service that runs it, whose docker.images override applies.
type want struct{ service, image string }

// resolve applies the overrides and returns the images sorted and without repeats, so pogi pulls what pogi runs.
func resolve(d *docker.Client, wants []want) []string {
	var out []string
	for _, w := range wants {
		out = append(out, d.Resolve(w.service, w.image))
	}
	slices.Sort(out)
	return slices.Compact(out)
}

// pullOne downloads one image and prints one line for it; on a terminal the line shows progress.
func pullOne(ctx context.Context, d *docker.Client, image string, out io.Writer) bool {
	have, err := d.HasImage(ctx, image)
	if err != nil {
		fmt.Fprintf(out, "%-45s failed: %s\n", image, docker.Plain(err))
		return false
	}
	if have {
		fmt.Fprintf(out, "%-45s already present\n", image)
		return true
	}
	live := out == io.Writer(os.Stdout) && styled(os.Stdout)
	done := make(chan error, 1)
	go func() { done <- d.Pull(ctx, image) }()
	for tick := time.NewTicker(300 * time.Millisecond); ; {
		select {
		case err := <-done:
			tick.Stop()
			if live {
				fmt.Fprint(out, "\r\033[K")
			}
			if err != nil {
				fmt.Fprintf(out, "%-45s failed: %s\n", image, docker.Plain(err))
				return false
			}
			fmt.Fprintf(out, "%-45s downloaded\n", image)
			return true
		case <-tick.C:
			if live {
				fmt.Fprintf(out, "\r\033[K%-45s %s", image, progress(d.Tasks().List()))
			}
		}
	}
}

// progress describes the newest running download.
func progress(ts []docker.Task) string {
	for _, t := range slices.Backward(ts) {
		if t.State == "running" && t.Kind == "download" {
			if t.Total > 0 {
				return fmt.Sprintf("%d%% of %s", 100*t.Done/t.Total, mb(t.Total))
			}
			return "downloading"
		}
	}
	return "starting"
}

func mb(n int64) string { return fmt.Sprintf("%.0f MB", float64(n)/1e6) }

// allImages are the default images of every container-backed service the stack mapping knows.
func allImages() []want {
	var w []want
	for _, i := range rds.Images() {
		w = append(w, want{"rds", i})
	}
	for _, i := range elasticache.Images() {
		w = append(w, want{"elasticache", i})
	}
	w = append(w, want{"eks", eks.DefaultImage()}, want{"kafka", kafka.RedpandaImage}, want{"spanner", spanner.Image})
	for _, v := range []string{"MYSQL_8_0", "POSTGRES_16"} {
		if i, ok := gcpsql.Image(v); ok {
			w = append(w, want{"cloudsql", i})
		}
	}
	return w
}

// stackImages are the images the container-backed resources of a Terraform folder or plan need, found
// through the functions the services run, with the service that runs each.
func stackImages(path string) ([]want, error) {
	cfg, err := tfmodel.Load(path, nil)
	if err != nil {
		return nil, err
	}
	var out []want
	add := func(service, image string) {
		if image != "" {
			out = append(out, want{service, image})
		}
	}
	for _, r := range cfg.Resources {
		if r.Data {
			continue
		}
		str := func(k string) string { s, _ := r.Attrs[k].(string); return s }
		switch r.Type {
		case "aws_db_instance", "aws_rds_cluster":
			add("rds", rds.Image(str("engine"), str("engine_version")))
		case "aws_elasticache_cluster", "aws_elasticache_replication_group", "aws_elasticache_serverless_cache":
			add("elasticache", elasticache.Image(cmp.Or(str("engine"), "redis")))
		case "google_sql_database_instance":
			i, _ := gcpsql.Image(cmp.Or(str("database_version"), "MYSQL_8_0"))
			add("cloudsql", i)
		case "aws_eks_cluster":
			add("eks", eks.DefaultImage())
		case "google_container_cluster":
			add("container", eks.DefaultImage())
		case "azurerm_kubernetes_cluster":
			add("azaks", eks.DefaultImage())
		case "aws_msk_cluster":
			add("kafka", kafka.RedpandaImage)
		case "google_spanner_instance":
			add("spanner", spanner.Image)
		}
	}
	return out, nil
}
