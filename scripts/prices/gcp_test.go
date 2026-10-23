package main

import (
	"encoding/json"
	"math"
	"os"
	"slices"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/cost"
)

// The files in testdata are samples shaped like the real API answers, with made-up prices.
func gcpSample(t *testing.T) ([]gcpSKU, []gcpMachine) {
	t.Helper()
	var s struct{ SKUs []gcpSKU }
	var m []gcpMachine
	for f, v := range map[string]any{"testdata/gcp-skus-sample.json": &s, "testdata/gcp-machine-types-sample.json": &m} {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(b, v); err != nil {
			t.Fatal(err)
		}
	}
	return s.SKUs, m
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-9*math.Max(1, math.Abs(b)) }

func TestGCPBuild(t *testing.T) {
	skus, ms := gcpSample(t)
	db, err := gcpBuild(skus, ms, "2026-10-20", opts{})
	if err != nil {
		t.Fatal(err)
	}
	if db, err = encodeCheck(db); err != nil { // the list as pogi reads it back
		t.Fatal(err)
	}
	// n2-standard-4 = 4 vCPUs and 16 GB; Windows adds the licence for 4 vCPUs.
	lin, ok := db.Hourly("gcp/compute/linux", "us-central1", "n2-standard-4")
	if want := 4*0.031611 + 16*0.004237; !ok || !near(lin, sig6(want)) {
		t.Errorf("n2-standard-4 linux = %v, %v; want %v", lin, ok, want)
	}
	if win, _ := db.Hourly("gcp/compute/windows", "us-central1", "n2-standard-4"); !near(win, sig6(lin+0.046*4)) {
		t.Errorf("windows = %v; want linux + 4 vCPUs of licence", win)
	}
	// Windows bills at least 4 vCPUs.
	lin2, _ := db.Hourly("gcp/compute/linux", "us-central1", "n2-standard-2")
	if win, _ := db.Hourly("gcp/compute/windows", "us-central1", "n2-standard-2"); !near(win, sig6(lin2+0.046*4)) {
		t.Errorf("windows 2 vCPU = %v; want the 4-vCPU minimum", win)
	}
	// A shared-core type is a flat price and has no Windows price.
	if p, _ := db.Hourly("gcp/compute/linux", "us-central1", "e2-micro"); !near(p, 0.0084) {
		t.Errorf("e2-micro = %v", p)
	}
	if _, ok := db.Hourly("gcp/compute/windows", "us-central1", "e2-micro"); ok {
		t.Error("e2-micro has a Windows price")
	}
	// Left out: a type with no rates, the local-SSD type, and a region without the type's SKUs.
	for _, n := range []string{"z9-weird-4", "n2-standard-4-lssd"} {
		if _, ok := db.Hourly("gcp/compute/linux", "us-central1", n); ok {
			t.Errorf("%s has a price", n)
		}
	}
	if _, ok := db.Hourly("gcp/compute/linux", "europe-west1", "n2-standard-4"); !ok {
		t.Error("n2-standard-4 has no price in europe-west1")
	}
	// Spot and commitment SKUs are not on-demand prices.
	if r := db.Rates["gcp/compute/n2/vcpu"]; r.Prices["us-central1"] != 0.031611 || r.Unit != "vCPU-hour" {
		t.Errorf("n2 vcpu rate = %+v", r)
	}
	// Units: ops are priced per 1M requests, Pub/Sub per GB (the first paid tier, per TiB / 1024), and it is in every region.
	if r := db.Rates["gcp/gcs/standard/class-a"]; r.Unit != "1M requests" || !near(r.Prices["us-central1"], 5) {
		t.Errorf("class A = %+v", r)
	}
	if r := db.Rates["gcp/pubsub/throughput"]; r.Unit != "GB" || !near(r.Prices["europe-west1"], sig6(40.0/1024)) {
		t.Errorf("pubsub = %+v", r)
	}
	if r := db.Rates["gcp/run/cpu"]; r.Unit != "vCPU-second" {
		t.Errorf("run cpu = %+v", r)
	}
	// Cloud SQL tiers: 2 vCPUs and 7.5 GB, and the shared-core tier is flat.
	if p, ok := db.Hourly("gcp/sql/mysql/zonal", "us-central1", "db-n1-standard-2"); !ok || !near(p, sig6(2*0.0413+7.5*0.007)) {
		t.Errorf("db-n1-standard-2 = %v, %v", p, ok)
	}
	if p, _ := db.Hourly("gcp/sql/mysql/zonal", "us-central1", "db-f1-micro"); !near(p, 0.015) {
		t.Errorf("db-f1-micro = %v", p)
	}
	if _, ok := db.Columns["gcp/sql/mysql/regional"]; ok {
		t.Error("a regional column was made without regional SKUs")
	}
}

func TestGCPBuildRegionsFilter(t *testing.T) {
	skus, ms := gcpSample(t)
	db, err := gcpBuild(skus, ms, "2026-10-20", opts{regions: []string{"us-central1"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := db.Columns["gcp/compute/linux"].Prices["europe-west1"]; ok {
		t.Error("europe-west1 was kept")
	}
	if _, err := gcpBuild(skus, ms, "x", opts{regions: []string{"mars-1"}}); err == nil {
		t.Error("no machine types for the region, and no error")
	}
}

// A SKU pattern that stops matching must stop the build, not leave a hole.
func TestGCPBuildMissingRate(t *testing.T) {
	skus, ms := gcpSample(t)
	skus = slices.DeleteFunc(skus, func(s gcpSKU) bool { return strings.HasPrefix(s.Description, "Cloud NAT Data") })
	_, err := gcpBuild(skus, ms, "x", opts{})
	if err == nil || !strings.Contains(err.Error(), "gcp/nat/data") {
		t.Errorf("err = %v; want it to name gcp/nat/data", err)
	}
}

func TestGCPBadSKU(t *testing.T) {
	skus, ms := gcpSample(t)
	skus[0].PricingInfo[0].PricingExpression.UsageUnit = "weird"
	if _, err := gcpBuild(skus, ms, "x", opts{}); err == nil || !strings.Contains(err.Error(), "unknown usage unit") {
		t.Errorf("err = %v; want an unknown-unit error", err)
	}
	skus[0].PricingInfo = nil
	if _, err := gcpBuild(skus, ms, "x", opts{}); err == nil {
		t.Error("a SKU with no pricing was accepted")
	}
}

func TestGCPClassify(t *testing.T) {
	for desc, want := range map[string]string{
		"N2D AMD Instance Core running in Americas":                                               "gcp/compute/n2d/vcpu",
		"N1 Predefined Instance Ram running in Americas":                                          "gcp/compute/n1/ram",
		"Compute optimized Core running in Americas":                                              "gcp/compute/c2/vcpu",
		"Custom Extended Instance Ram running in Americas":                                        "gcp/compute/n1-custom-ext/ram",
		"Tau T2A Arm Instance Core running in Americas":                                           "gcp/compute/t2a/vcpu",
		"Memory-optimized Upgrade Premium for Memory-optimized Instance Core running in Americas": "",
		"Micro Instance with burstable CPU running in Americas":                                   "gcp/compute/flat/f1-micro",
		"Nvidia Tesla A100 GPU running in Americas":                                               "gcp/compute/gpu/teslaa100",
	} {
		s := &gcpSKU{Description: desc}
		s.Category.ServiceDisplayName, s.Category.UsageType = "Compute Engine", "OnDemand"
		if got := gcpClassify(s); got != want {
			t.Errorf("%q → %q, want %q", desc, got, want)
		}
	}
}

// encodeCheck makes sure the list reads back, as run() does before it writes the file.
func encodeCheck(db *cost.DB) (*cost.DB, error) {
	b, err := cost.Encode(db)
	if err != nil {
		return nil, err
	}
	return cost.Decode(b)
}
