package cost

import (
	"math"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/terraform"
)

// gcpTestDB is a small Google price list with made-up (sample) prices in two regions. The
// real list is written by scripts/prices; TestGCPDefaultData checks it when it is there.
func gcpTestDB(t *testing.T) *DB {
	t.Helper()
	regs := []string{"us-central1", "europe-west1"}
	rate := func(unit string, p float64) *Rate {
		r := &Rate{Unit: unit, Prices: map[string]float64{}}
		for i, g := range regs {
			r.Prices[g] = p * (1 + float64(i)/10)
		}
		return r
	}
	col := func(table string, p ...float64) *Column {
		c := &Column{Table: table, Unit: "hour", Prices: map[string][]float64{}}
		for _, g := range regs {
			c.Prices[g] = p
		}
		return c
	}
	db := &DB{
		Published: map[string]string{"gcp": "2026-10-20"},
		Tables: map[string][]Size{
			"gcp/compute": {{Name: "e2-medium", VCPU: 2, MemGB: 4}, {Name: "n2-standard-2", VCPU: 2, MemGB: 8}, {Name: "n2-standard-4", VCPU: 4, MemGB: 16}},
			"gcp/sql":     {{Name: "db-f1-micro"}, {Name: "db-n1-standard-2", VCPU: 2, MemGB: 7.5}},
		},
		Columns: map[string]*Column{
			"gcp/compute/linux":   col("gcp/compute", 0.03, 0.1, 0.2),
			"gcp/compute/windows": col("gcp/compute", 0.05, 0.3, 0.4),
			"gcp/sql/mysql/zonal": col("gcp/sql", 0.015, 0.2),
		},
		Rates: map[string]*Rate{},
	}
	for k, r := range map[string]*Rate{
		"gcp/compute/n2-custom/vcpu": rate("vCPU-hour", 0.033), "gcp/compute/n2-custom/ram": rate("GB-hour", 0.0044),
		"gcp/compute/n1-custom-ext/ram": rate("GB-hour", 0.009), "gcp/compute/n1-custom/vcpu": rate("vCPU-hour", 0.03),
		"gcp/compute/windows/vcpu": rate("vCPU-hour", 0.046), "gcp/compute/gpu/teslat4": rate("hour", 0.35),
		"gcp/disk/pd-standard/storage": rate("GB-month", 0.04), "gcp/disk/pd-ssd/storage": rate("GB-month", 0.17),
		"gcp/disk/pd-balanced/storage": rate("GB-month", 0.1), "gcp/disk/pd-extreme/storage": rate("GB-month", 0.125),
		"gcp/disk/pd-extreme/iops": rate("IOPS-month", 0.065), "gcp/disk/local-ssd/storage": rate("GB-month", 0.08),
		"gcp/address/inuse": rate("hour", 0.005), "gcp/nat/hour": rate("hour", 0.0014), "gcp/nat/data": rate("GB", 0.045),
		"gcp/lb/external/rule": rate("hour", 0.025), "gcp/lb/external/data": rate("GB", 0.008),
		"gcp/lb/internal/rule": rate("hour", 0.025), "gcp/lb/internal/data": rate("GB", 0.008),
		"gcp/gke/cluster":          rate("hour", 0.1),
		"gcp/sql/mysql/zonal/vcpu": rate("vCPU-hour", 0.0413), "gcp/sql/mysql/zonal/ram": rate("GB-hour", 0.007),
		"gcp/sql/mysql/zonal/ssd": rate("GB-month", 0.17), "gcp/sql/mysql/zonal/hdd": rate("GB-month", 0.09),
		"gcp/sql/postgres/regional/vcpu": rate("vCPU-hour", 0.08), "gcp/sql/postgres/regional/ram": rate("GB-hour", 0.014),
		"gcp/sql/postgres/regional/ssd": rate("GB-month", 0.34),
		"gcp/redis/basic/m1":            rate("GB-hour", 0.016), "gcp/redis/standard/m2": rate("GB-hour", 0.03), "gcp/redis/basic/m5": rate("GB-hour", 0.01),
		"gcp/gcs/standard/storage": rate("GB-month", 0.02), "gcp/gcs/standard/class-a": rate("1M requests", 5), "gcp/gcs/standard/class-b": rate("1M requests", 0.4),
		"gcp/gcs/nearline/storage": rate("GB-month", 0.01), "gcp/gcs/nearline/class-a": rate("1M requests", 10), "gcp/gcs/nearline/class-b": rate("1M requests", 1),
		"gcp/run/cpu": rate("vCPU-second", 0.000024), "gcp/run/memory": rate("GB-second", 0.0000025), "gcp/run/requests": rate("1M requests", 0.4),
		"gcp/pubsub/throughput": rate("GB", 0.04), "gcp/bigquery/active-logical": rate("GB-month", 0.02), "gcp/bigquery/long-term-logical": rate("GB-month", 0.01),
	} {
		db.Rates[k] = r
	}
	// US multi-region prices, for buckets and datasets.
	for _, k := range []string{"gcp/gcs/standard/storage", "gcp/gcs/standard/class-a", "gcp/gcs/standard/class-b", "gcp/bigquery/active-logical", "gcp/bigquery/long-term-logical"} {
		db.Rates[k].Prices["us"] = db.Rates[k].Prices["us-central1"] * 1.3
	}
	b, err := Encode(db)
	if err != nil {
		t.Fatal(err)
	}
	if db, err = Decode(b); err != nil {
		t.Fatal(err)
	}
	return db
}

type attrs = map[string]any

// gcpRow prices one resource and returns its row.
func gcpRow(t *testing.T, db *DB, typ, region string, a attrs, usage attrs, pick string) Row {
	t.Helper()
	tr := &terraform.Resource{Address: typ + ".x", Block: typ + ".x", Type: typ, Name: "x", Cloud: "gcp", Region: region, Attrs: a}
	opt := Options{DB: db, Usage: map[string]map[string]any{tr.Address: usage}}
	if pick != "" {
		opt.Sizes = map[string]string{tr.Address: pick}
	}
	e, err := Price(&terraform.Config{Source: "folder", Resources: []*terraform.Resource{tr}}, opt)
	if err != nil {
		t.Fatal(err)
	}
	return e.Rows[0]
}

func gcpNear(a, b float64) bool { return math.Abs(a-b) < 1e-9*math.Max(1, math.Abs(b)) }

func rateOf(t *testing.T, db *DB, key, region string) float64 {
	t.Helper()
	p, _, ok := db.Rate(key, region)
	if !ok {
		t.Fatalf("no rate %s in %s", key, region)
	}
	return p
}

func TestGCPInstance(t *testing.T) {
	db := gcpTestDB(t)
	const us = "us-central1"
	hr, _ := db.Hourly("gcp/compute/linux", us, "n2-standard-4")
	disk := rateOf(t, db, "gcp/disk/pd-standard/storage", us)
	ip := rateOf(t, db, "gcp/address/inuse", us)

	// Linux, a 50 GB ssd boot disk, an external IP: every input is in the files.
	r := gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-standard-4",
		"boot_disk":         []any{attrs{"initialize_params": []any{attrs{"type": "pd-ssd", "size": 50.0, "image": "debian-cloud/debian-12"}}}},
		"network_interface": []any{attrs{"access_config": []any{attrs{}}}}}, nil, "")
	want := 730*hr + 50*rateOf(t, db, "gcp/disk/pd-ssd/storage", us) + 730*ip
	if r.Status != Known || !gcpNear(r.Monthly, want) || r.Sizes == nil || r.Sizes.Column != "gcp/compute/linux" {
		t.Errorf("linux: %+v, want %v", r, want)
	}

	// Defaults for the boot disk are assumptions, and they say so.
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-standard-4"}, nil, "")
	if r.Status != Assumed || !hasNote(r, "10 GB boot disk") || !gcpNear(r.Monthly, 730*hr+10*disk) {
		t.Errorf("defaults: %+v", r)
	}

	// Windows comes from the image name, and uses the Windows column.
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-standard-4",
		"boot_disk": []any{attrs{"initialize_params": []any{attrs{"image": "windows-cloud/windows-2022", "size": 50.0, "type": "pd-standard"}}}}}, nil, "")
	win, _ := db.Hourly("gcp/compute/windows", us, "n2-standard-4")
	if r.Status != Known || !gcpNear(r.Monthly, 730*win+50*disk) || r.Sizes.Column != "gcp/compute/windows" {
		t.Errorf("windows: %+v", r)
	}

	// An image that isn't known before apply is taken as Linux.
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-standard-4",
		"boot_disk": []any{attrs{"initialize_params": []any{attrs{"image": terraform.Unknown{}}}}}}, nil, "")
	if r.Status != Assumed || !hasNote(r, "assumed Linux") {
		t.Errorf("unknown image: %+v", r)
	}

	// A custom type is priced from the family rates: 4 vCPUs, 16 GB; extended memory uses its own rate.
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-custom-4-16384",
		"boot_disk": []any{attrs{"initialize_params": []any{attrs{"size": 10.0, "type": "pd-standard"}}}}}, nil, "")
	want = 730*(4*rateOf(t, db, "gcp/compute/n2-custom/vcpu", us)+16*rateOf(t, db, "gcp/compute/n2-custom/ram", us)) + 10*disk
	if r.Status != Known || !gcpNear(r.Monthly, want) {
		t.Errorf("custom: %+v, want %v", r, want)
	}
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "custom-2-13312-ext"}, nil, "")
	want = 730*(2*rateOf(t, db, "gcp/compute/n1-custom/vcpu", us)+13*rateOf(t, db, "gcp/compute/n1-custom-ext/ram", us)) + 10*disk
	if !gcpNear(r.Monthly, want) {
		t.Errorf("custom ext: %+v, want %v", r, want)
	}
	// A custom Windows machine pays the licence for at least 4 vCPUs.
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-custom-2-8192",
		"boot_disk": []any{attrs{"initialize_params": []any{attrs{"image": "windows-cloud/windows-2022", "size": 10.0, "type": "pd-standard"}}}}}, nil, "")
	want = 730*(2*rateOf(t, db, "gcp/compute/n2-custom/vcpu", us)+8*rateOf(t, db, "gcp/compute/n2-custom/ram", us)+4*rateOf(t, db, "gcp/compute/windows/vcpu", us)) + 10*disk
	if !gcpNear(r.Monthly, want) {
		t.Errorf("custom windows: %+v, want %v", r, want)
	}

	// GPUs, local SSD and spot.
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-standard-2",
		"guest_accelerator": []any{attrs{"type": "nvidia-tesla-t4", "count": 2.0}}, "scratch_disk": []any{attrs{}, attrs{}},
		"scheduling": []any{attrs{"provisioning_model": "SPOT"}}}, nil, "")
	if !hasNote(r, "spot") || r.Status != Assumed {
		t.Errorf("spot: %+v", r)
	}
	var gpu, ssd bool
	for _, l := range r.Lines {
		gpu = gpu || (strings.Contains(l.What, "GPU") && gcpNear(l.Qty, 2*730))
		ssd = ssd || (strings.Contains(l.What, "Local SSD") && l.Qty == 750)
	}
	if !gpu || !ssd {
		t.Errorf("gpu or local SSD line missing: %+v", r.Lines)
	}

	// What-if size pick, and the ways it fails.
	r = gcpRow(t, db, "google_compute_instance", us, attrs{"machine_type": "n2-standard-4"}, nil, "n2-standard-2")
	if r.Sizes.Current != "n2-standard-2" || r.Sizes.File != "n2-standard-4" {
		t.Errorf("pick: %+v", r.Sizes)
	}
	for name, c := range map[string]struct {
		region string
		a      attrs
		note   string
	}{
		"unknown size":     {us, attrs{"machine_type": "zz-huge-9"}, "no price for zz-huge-9"},
		"unknown region":   {"mars-north1", attrs{"machine_type": "n2-standard-4"}, "no price for n2-standard-4 in mars-north1"},
		"missing size":     {us, attrs{}, "needs a value for machine_type"},
		"unknown size val": {us, attrs{"machine_type": terraform.Unknown{}}, "needs a value for machine_type"},
		"bad disk type":    {us, attrs{"machine_type": "n2-standard-4", "boot_disk": []any{attrs{"initialize_params": []any{attrs{"type": "hyperdisk-balanced"}}}}}, "hyperdisk-balanced"},
	} {
		if r := gcpRow(t, db, "google_compute_instance", c.region, c.a, nil, ""); r.Status != Needs || !hasNote(r, c.note) {
			t.Errorf("%s: %+v", name, r)
		}
	}
	if col, ok := SizeColumn("google_compute_instance", "windows"); !ok || col != "gcp/compute/windows" {
		t.Errorf("SizeColumn = %q, %v", col, ok)
	}
}

func TestGCPDisksAddressesNetwork(t *testing.T) {
	db := gcpTestDB(t)
	const us = "us-central1"
	r := gcpRow(t, db, "google_compute_disk", us, attrs{"type": "pd-extreme", "size": 100.0, "provisioned_iops": 5000.0}, nil, "")
	want := 100*rateOf(t, db, "gcp/disk/pd-extreme/storage", us) + 5000*rateOf(t, db, "gcp/disk/pd-extreme/iops", us)
	if r.Status != Known || !gcpNear(r.Monthly, want) {
		t.Errorf("extreme disk: %+v, want %v", r, want)
	}
	// A disk type given as a URL, and the assumed size.
	r = gcpRow(t, db, "google_compute_disk", us, attrs{"type": "projects/p/zones/z/diskTypes/pd-ssd"}, nil, "")
	if r.Status != Assumed || !gcpNear(r.Monthly, 100*rateOf(t, db, "gcp/disk/pd-ssd/storage", us)) {
		t.Errorf("disk url: %+v", r)
	}
	if r = gcpRow(t, db, "google_compute_disk", us, attrs{"type": "pd-ssd", "size": terraform.Unknown{}}, nil, ""); r.Status != Assumed {
		t.Errorf("unknown size: %+v", r)
	}

	r = gcpRow(t, db, "google_compute_address", us, attrs{}, nil, "")
	if r.Status != Assumed || !gcpNear(r.Monthly, 730*rateOf(t, db, "gcp/address/inuse", us)) {
		t.Errorf("address: %+v", r)
	}
	if r = gcpRow(t, db, "google_compute_address", us, attrs{"address_type": "INTERNAL"}, nil, ""); r.Monthly != 0 || r.Status != Known {
		t.Errorf("internal address: %+v", r)
	}

	// NAT: one VM assumed, then 40 VMs from the usage file are capped at 32; data is a usage line.
	r = gcpRow(t, db, "google_compute_router_nat", us, attrs{}, nil, "")
	if r.Status != Assumed || !gcpNear(r.Lines[0].Monthly, 730*rateOf(t, db, "gcp/nat/hour", us)) {
		t.Errorf("nat: %+v", r)
	}
	r = gcpRow(t, db, "google_compute_router_nat", us, attrs{}, attrs{"nat_vm_count": 40.0, "monthly_gb_processed": 100.0}, "")
	want = 32*730*rateOf(t, db, "gcp/nat/hour", us) + 100*rateOf(t, db, "gcp/nat/data", us)
	if r.Status != Known || !gcpNear(r.Monthly, want) {
		t.Errorf("nat usage: %+v, want %v", r, want)
	}

	r = gcpRow(t, db, "google_compute_forwarding_rule", us, attrs{"load_balancing_scheme": "INTERNAL"}, attrs{"monthly_gb_processed": 10.0}, "")
	want = 730*rateOf(t, db, "gcp/lb/internal/rule", us) + 10*rateOf(t, db, "gcp/lb/internal/data", us)
	if !gcpNear(r.Monthly, want) {
		t.Errorf("internal rule: %+v", r)
	}
	if r = gcpRow(t, db, "google_compute_forwarding_rule", us, attrs{}, nil, ""); r.Status != Known || !gcpNear(r.Monthly, 730*rateOf(t, db, "gcp/lb/external/rule", us)) {
		t.Errorf("external rule: %+v", r)
	}
	for _, typ := range []string{"google_compute_network", "google_compute_subnetwork", "google_compute_firewall", "google_service_account", "google_project_iam_member"} {
		if r := gcpRow(t, db, typ, us, attrs{}, nil, ""); r.Status != Free {
			t.Errorf("%s is %s, want free", typ, r.Status)
		}
	}
}

func TestGCPKubernetes(t *testing.T) {
	db := gcpTestDB(t)
	const us = "us-central1"
	n2, _ := db.Hourly("gcp/compute/linux", us, "n2-standard-2")
	bal := rateOf(t, db, "gcp/disk/pd-balanced/storage", us)
	fee := 730 * rateOf(t, db, "gcp/gke/cluster", us)

	// A regional pool: 2 nodes in each of 3 zones, with the default 100 GB balanced disks.
	r := gcpRow(t, db, "google_container_node_pool", us, attrs{"location": us, "node_count": 2.0,
		"node_config": []any{attrs{"machine_type": "n2-standard-2"}}}, nil, "")
	if want := 6 * (730*n2 + 100*bal); !gcpNear(r.Monthly, want) || !hasNote(r, "3 zones") || r.Sizes == nil {
		t.Errorf("regional pool: %+v, want %v", r, want)
	}
	// node_locations decides the zone count; a zone means one zone.
	r = gcpRow(t, db, "google_container_node_pool", us, attrs{"location": us, "node_count": 1.0, "node_locations": []any{"a", "b"},
		"node_config": []any{attrs{"machine_type": "n2-standard-2", "disk_size_gb": 20.0, "disk_type": "pd-ssd"}}}, nil, "")
	if want := 2 * (730*n2 + 20*rateOf(t, db, "gcp/disk/pd-ssd/storage", us)); !gcpNear(r.Monthly, want) || r.Status != Known {
		t.Errorf("node_locations: %+v, want %v", r, want)
	}
	r = gcpRow(t, db, "google_container_node_pool", us, attrs{"location": "us-central1-a", "node_count": 3.0,
		"node_config": []any{attrs{"machine_type": "n2-standard-2"}}}, nil, "")
	if want := 3 * (730*n2 + 100*bal); !gcpNear(r.Monthly, want) || r.Status != Known {
		t.Errorf("zonal pool: %+v, want %v", r, want)
	}
	// An autoscaled pool is priced at its minimum, and says so; no count at all is an assumption.
	r = gcpRow(t, db, "google_container_node_pool", us, attrs{"location": "us-central1-a", "autoscaling": []any{attrs{"min_node_count": 1.0}},
		"node_config": []any{attrs{"machine_type": "n2-standard-2"}}}, nil, "")
	if !hasNote(r, "autoscaler's minimum") {
		t.Errorf("autoscaling: %+v", r)
	}
	if r = gcpRow(t, db, "google_container_node_pool", us, attrs{"location": "us-central1-a"}, nil, ""); r.Status != Assumed || !hasNote(r, "1 node per zone") {
		t.Errorf("no count: %+v", r)
	}
	if r = gcpRow(t, db, "google_container_node_pool", us, attrs{"location": "us-central1-a", "node_count": 1.0, "node_config": []any{attrs{"machine_type": "zz-9"}}}, nil, ""); r.Status != Needs {
		t.Errorf("unknown size: %+v", r)
	}

	// A cluster: the fee, plus the default pool unless it is removed.
	r = gcpRow(t, db, "google_container_cluster", us, attrs{"location": "us-central1-a", "remove_default_node_pool": true, "initial_node_count": 1.0}, nil, "")
	if r.Status != Known || !gcpNear(r.Monthly, fee) {
		t.Errorf("cluster: %+v", r)
	}
	r = gcpRow(t, db, "google_container_cluster", us, attrs{"location": "us-central1-a", "initial_node_count": 2.0}, nil, "")
	e2, _ := db.Hourly("gcp/compute/linux", us, "e2-medium")
	if want := fee + 2*(730*e2+100*bal); !gcpNear(r.Monthly, want) {
		t.Errorf("cluster with default pool: %+v, want %v", r, want)
	}
	r = gcpRow(t, db, "google_container_cluster", us, attrs{"location": "us-central1-a", "node_pool": []any{
		attrs{"initial_node_count": 1.0, "node_config": []any{attrs{"machine_type": "n2-standard-2"}}}}}, nil, "")
	if want := fee + 730*n2 + 100*bal; !gcpNear(r.Monthly, want) {
		t.Errorf("cluster with an inline pool: %+v, want %v", r, want)
	}
	if r = gcpRow(t, db, "google_container_cluster", us, attrs{"enable_autopilot": true}, nil, ""); r.Status != Needs || !hasNote(r, "Autopilot") {
		t.Errorf("autopilot: %+v", r)
	}
}

func TestGCPDatabases(t *testing.T) {
	db := gcpTestDB(t)
	const us = "us-central1"
	vcpu, ram, ssd := rateOf(t, db, "gcp/sql/mysql/zonal/vcpu", us), rateOf(t, db, "gcp/sql/mysql/zonal/ram", us), rateOf(t, db, "gcp/sql/mysql/zonal/ssd", us)
	tier, _ := db.Hourly("gcp/sql/mysql/zonal", us, "db-n1-standard-2")

	r := gcpRow(t, db, "google_sql_database_instance", us, attrs{"database_version": "MYSQL_8_0",
		"settings": []any{attrs{"tier": "db-n1-standard-2", "disk_size": 50.0}}}, nil, "")
	if want := 730*tier + 50*ssd; r.Status != Known || !gcpNear(r.Monthly, want) || r.Sizes.Column != "gcp/sql/mysql/zonal" {
		t.Errorf("tier: %+v, want %v", r, want)
	}
	// The default disk is 10 GB, and a custom tier is priced from the vCPU and memory rates.
	r = gcpRow(t, db, "google_sql_database_instance", us, attrs{"database_version": "MYSQL_8_0", "settings": []any{attrs{"tier": "db-custom-2-7680"}}}, nil, "")
	if want := 730*(2*vcpu+7.5*ram) + 10*ssd; r.Status != Known || !gcpNear(r.Monthly, want) {
		t.Errorf("custom tier: %+v, want %v", r, want)
	}
	// Regional Postgres uses the regional rates.
	r = gcpRow(t, db, "google_sql_database_instance", us, attrs{"database_version": "POSTGRES_15",
		"settings": []any{attrs{"tier": "db-custom-4-16384", "availability_type": "REGIONAL", "disk_size": 20.0}}}, nil, "")
	want := 730*(4*rateOf(t, db, "gcp/sql/postgres/regional/vcpu", us)+16*rateOf(t, db, "gcp/sql/postgres/regional/ram", us)) + 20*rateOf(t, db, "gcp/sql/postgres/regional/ssd", us)
	if !gcpNear(r.Monthly, want) {
		t.Errorf("regional: %+v, want %v", r, want)
	}
	for name, c := range map[string]struct {
		a    attrs
		note string
	}{
		"sql server":                  {attrs{"database_version": "SQLSERVER_2019_STANDARD", "settings": []any{attrs{"tier": "db-custom-2-7680"}}}, "SQLSERVER"},
		"no version":                  {attrs{"settings": []any{attrs{"tier": "db-f1-micro"}}}, "database_version"},
		"no tier":                     {attrs{"database_version": "MYSQL_8_0"}, "settings.tier"},
		"unknown tier":                {attrs{"database_version": "MYSQL_8_0", "settings": []any{attrs{"tier": terraform.Unknown{}}}}, "settings.tier"},
		"bad tier":                    {attrs{"database_version": "MYSQL_8_0", "settings": []any{attrs{"tier": "db-nope-1"}}}, "no price for db-nope-1"},
		"bad disk":                    {attrs{"database_version": "MYSQL_8_0", "settings": []any{attrs{"tier": "db-f1-micro", "disk_type": "PD_FAST"}}}, "PD_FAST"},
		"bad avail":                   {attrs{"database_version": "MYSQL_8_0", "settings": []any{attrs{"tier": "db-f1-micro", "availability_type": "BOTH"}}}, "availability_type"},
		"mysql regional has no rates": {attrs{"database_version": "MYSQL_8_0", "settings": []any{attrs{"tier": "db-f1-micro", "availability_type": "REGIONAL"}}}, "no price"},
	} {
		if r := gcpRow(t, db, "google_sql_database_instance", us, c.a, nil, ""); r.Status != Needs || !hasNote(r, c.note) {
			t.Errorf("%s: %+v", name, r)
		}
	}

	// Redis: the capacity tier follows the size.
	for gb, key := range map[float64]string{2: "gcp/redis/basic/m1", 150: "gcp/redis/basic/m5"} {
		r := gcpRow(t, db, "google_redis_instance", us, attrs{"memory_size_gb": gb}, nil, "")
		if !gcpNear(r.Monthly, gb*730*rateOf(t, db, key, us)) || r.Status != Known {
			t.Errorf("redis %v GB: %+v", gb, r)
		}
	}
	r = gcpRow(t, db, "google_redis_instance", us, attrs{"memory_size_gb": 10.0, "tier": "STANDARD_HA"}, nil, "")
	if !gcpNear(r.Monthly, 10*730*rateOf(t, db, "gcp/redis/standard/m2", us)) {
		t.Errorf("redis standard: %+v", r)
	}
	for name, a := range map[string]attrs{"missing": {}, "unknown": {"memory_size_gb": terraform.Unknown{}}, "negative": {"memory_size_gb": -1.0}, "text": {"memory_size_gb": "lots"},
		"no price for the tier": {"memory_size_gb": 20.0}} {
		if r := gcpRow(t, db, "google_redis_instance", us, a, nil, ""); r.Status != Needs {
			t.Errorf("redis %s: %+v", name, r)
		}
	}
}

func TestGCPServerless(t *testing.T) {
	db := gcpTestDB(t)
	const us = "us-central1"

	// A bucket is priced by usage in its own location; with no usage it shows unit prices and $0.
	r := gcpRow(t, db, "google_storage_bucket", us, attrs{"location": "US"}, nil, "")
	if r.Status != Usage || r.Monthly != 0 || len(r.Lines) != 3 {
		t.Errorf("bucket without usage: %+v", r)
	}
	r = gcpRow(t, db, "google_storage_bucket", us, attrs{"location": "US"}, attrs{"storage_gb": 100.0, "monthly_class_a_requests": 2e6}, "")
	want := 100*rateOf(t, db, "gcp/gcs/standard/storage", "us") + 2*rateOf(t, db, "gcp/gcs/standard/class-a", "us")
	if !gcpNear(r.Monthly, want) {
		t.Errorf("bucket: %+v, want %v", r, want)
	}
	r = gcpRow(t, db, "google_storage_bucket", us, attrs{"location": "US-CENTRAL1", "storage_class": "NEARLINE"}, attrs{"storage_gb": 10.0}, "")
	if r.Status != Known || r.Lines[0].UnitPrice != rateOf(t, db, "gcp/gcs/nearline/storage", "us-central1") {
		t.Errorf("nearline in a lower-case region: %+v", r)
	}
	for name, a := range map[string]attrs{"no location": {}, "unknown location": {"location": terraform.Unknown{}},
		"cold class": {"location": "US", "storage_class": "COLDLINE"}, "no region price": {"location": "ASIA"}} {
		if r := gcpRow(t, db, "google_storage_bucket", us, a, nil, ""); r.Status != Needs {
			t.Errorf("bucket %s: %+v", name, r)
		}
	}

	// Cloud Run: 1M requests of 200 ms, with 2 vCPU and 1 GiB.
	r = gcpRow(t, db, "google_cloud_run_v2_service", us, attrs{"template": []any{attrs{"containers": []any{attrs{"resources": []any{
		attrs{"limits": attrs{"cpu": "2", "memory": "1Gi"}}}}}}}}, attrs{"monthly_requests": 1e6, "avg_duration_ms": 200.0}, "")
	want = rateOf(t, db, "gcp/run/requests", us) + 1e6*0.2*2*rateOf(t, db, "gcp/run/cpu", us) + 1e6*0.2*1*(1.0*rateOf(t, db, "gcp/run/memory", us))
	if r.Status != Known || !gcpNear(r.Monthly, want) {
		t.Errorf("run: %+v, want %v", r, want)
	}
	// The defaults are Cloud Run's own (1 vCPU, 512 MiB), and no usage means unit prices and $0.
	if r = gcpRow(t, db, "google_cloud_run_v2_service", us, attrs{}, nil, ""); r.Status != Usage || r.Monthly != 0 {
		t.Errorf("run without usage: %+v", r)
	}
	if r = gcpRow(t, db, "google_cloud_run_v2_service", us, attrs{"template": []any{attrs{"containers": []any{attrs{"resources": []any{attrs{"limits": attrs{"memory": "lots"}}}}}}}}, nil, ""); r.Status != Needs {
		t.Errorf("bad memory: %+v", r)
	}

	// A 2nd-generation function: 256M with no available_cpu takes Google's default CPU, which is noted.
	r = gcpRow(t, db, "google_cloudfunctions2_function", us, attrs{}, attrs{"monthly_requests": 1e6, "avg_duration_ms": 100.0}, "")
	want = rateOf(t, db, "gcp/run/requests", us) + 1e6*0.1*0.167*rateOf(t, db, "gcp/run/cpu", us) + 1e6*0.1*(256e6/(1<<30))*rateOf(t, db, "gcp/run/memory", us)
	if r.Status != Assumed || !gcpNear(r.Monthly, want) || !hasNote(r, "default CPU") {
		t.Errorf("function: %+v, want %v", r, want)
	}
	r = gcpRow(t, db, "google_cloudfunctions2_function", us, attrs{"service_config": []any{attrs{"available_memory": "2Gi", "available_cpu": "1000m"}}}, nil, "")
	if r.Status != Usage {
		t.Errorf("function with cpu: %+v", r)
	}
	if r = gcpRow(t, db, "google_cloudfunctions2_function", us, attrs{"service_config": []any{attrs{"available_memory": "x"}}}, nil, ""); r.Status != Needs {
		t.Errorf("bad function memory: %+v", r)
	}

	r = gcpRow(t, db, "google_pubsub_topic", us, attrs{}, attrs{"monthly_gb_published": 500.0}, "")
	if r.Status != Known || !gcpNear(r.Monthly, 500*rateOf(t, db, "gcp/pubsub/throughput", us)) {
		t.Errorf("pubsub: %+v", r)
	}
	r = gcpRow(t, db, "google_bigquery_dataset", us, attrs{}, attrs{"storage_gb": 1000.0, "long_term_storage_gb": 500.0}, "")
	want = 1000*rateOf(t, db, "gcp/bigquery/active-logical", "us") + 500*rateOf(t, db, "gcp/bigquery/long-term-logical", "us")
	if !gcpNear(r.Monthly, want) {
		t.Errorf("bigquery: %+v, want %v", r, want)
	}
}

func TestGCPParsers(t *testing.T) {
	for in, want := range map[string]float64{"512Mi": 0.5, "2Gi": 2, "256M": 256e6 / (1 << 30), "1G": 1e9 / (1 << 30)} {
		if got, ok := gcpMemGB(in); !ok || !gcpNear(got, want) {
			t.Errorf("gcpMemGB(%q) = %v, %v", in, got, ok)
		}
	}
	for _, in := range []string{"", "Gi", "-1Gi", "abc", "0"} {
		if _, ok := gcpMemGB(in); ok {
			t.Errorf("gcpMemGB(%q) was accepted", in)
		}
	}
	for in, want := range map[string]float64{"1": 1, "0.5": 0.5, "1500m": 1.5} {
		if got, ok := gcpCPU(in); !ok || !gcpNear(got, want) {
			t.Errorf("gcpCPU(%q) = %v, %v", in, got, ok)
		}
	}
	if _, ok := gcpCPU("x"); ok {
		t.Error("gcpCPU accepted x")
	}
}

// With the real list generated, a standard machine must price in the default region.
func TestGCPDefaultData(t *testing.T) {
	db, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	if len(db.Tables["gcp/compute"]) == 0 {
		t.Skip("the Google price list has not been generated (needs gcloud auth login)")
	}
	p, ok := db.Hourly("gcp/compute/linux", DefaultRegion["gcp"], "n2-standard-4")
	if !ok || p < 0.05 || p > 1 {
		t.Errorf("n2-standard-4 in the default region = %v, %v", p, ok)
	}
	if db.Published["gcp"] == "" {
		t.Error("the list has no published date")
	}
}
