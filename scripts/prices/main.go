// Command prices builds pogi's embedded price lists from the clouds' official price lists. A
// maintainer runs it before a release; it is not part of the pogi binary.
//
//	go run ./scripts/prices -cloud aws                       # writes internal/cost/prices/aws.json.gz
//	go run ./scripts/prices -cloud azure -regions eastus     # one region, for a quick try
//
// AWS reads the public Price List bulk files. Azure reads the public Retail Prices API, and VM
// sizes with "az vm list-skus" (an az login). Google Cloud reads the Cloud Billing Catalog API
// and machine types with gcloud (a gcloud login). Downloads are kept in -cache between runs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/cost"
)

// opts are the flags every cloud's builder gets.
type opts struct {
	cache   string   // folder for downloads
	regions []string // only these regions; empty means all public ones
}

// builders build one cloud's price list each; aws.go, gcp.go and azure.go add theirs from init.
var builders = map[string]func(context.Context, opts) (*cost.DB, error){}

func main() {
	cache, _ := os.UserCacheDir()
	cloud := flag.String("cloud", "all", "aws, gcp, azure or all")
	flag.StringVar(&cache, "cache", filepath.Join(cache, "pogi-prices"), "folder for downloads, kept between runs")
	regions := flag.String("regions", "", "comma-separated regions (default: all public regions)")
	out := flag.String("out", "internal/cost/prices", "folder to write <cloud>.json.gz to")
	flag.Parse()
	o := opts{cache: cache}
	if *regions != "" {
		o.regions = strings.Split(*regions, ",")
	}
	clouds := []string{"aws", "gcp", "azure"}
	if *cloud != "all" {
		clouds = []string{*cloud}
	}
	for _, c := range clouds {
		if err := run(c, o, *out); err != nil {
			fmt.Fprintf(os.Stderr, "prices: %s: %v\n", c, err)
			os.Exit(1)
		}
	}
}

func run(cloud string, o opts, out string) error {
	build := builders[cloud]
	if build == nil {
		return errors.New("unknown cloud; want aws, gcp or azure")
	}
	if err := os.MkdirAll(o.cache, 0o755); err != nil {
		return err
	}
	db, err := build(context.Background(), o)
	if err != nil {
		return err
	}
	b, err := cost.Encode(db)
	if err != nil {
		return err
	}
	if _, err := cost.Decode(b); err != nil { // the same check pogi makes when it loads the file
		return fmt.Errorf("the price list doesn't read back: %w", err)
	}
	path := filepath.Join(out, cloud+".json.gz")
	if err := os.WriteFile(path, b, 0o644); err != nil {
		return err
	}
	fmt.Printf("%s: %d tables, %d columns, %d rates, %d KB → %s\n", cloud, len(db.Tables), len(db.Columns), len(db.Rates), len(b)/1024, path)
	return nil
}

var client = &http.Client{Timeout: 30 * time.Minute} // the biggest AWS file is a few GB

// fetch downloads url into the cache as name, unless it is already there, and returns its path.
// It writes to a temporary file first, so a stopped run never leaves half a file behind.
func fetch(ctx context.Context, o opts, url, name string, header http.Header) (string, error) {
	path := filepath.Join(o.cache, name)
	if _, err := os.Stat(path); err == nil {
		return path, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
		return "", fmt.Errorf("GET %s: %s: %s", url, resp.Status, b)
	}
	tmp := path + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return "", err
	}
	if _, err := io.Copy(f, resp.Body); err != nil {
		f.Close()
		os.Remove(tmp)
		return "", fmt.Errorf("GET %s: %w", url, err)
	}
	if err := f.Close(); err != nil {
		return "", err
	}
	return path, os.Rename(tmp, path)
}

// wanted reports whether a region is in -regions (or -regions is empty).
func (o opts) wanted(region string) bool {
	if len(o.regions) == 0 {
		return true
	}
	for _, r := range o.regions {
		if r == region {
			return true
		}
	}
	return false
}
