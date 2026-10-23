package cost

// Google Cloud mappings. The price keys below are written by scripts/prices/gcp.go.
//
// Tables:  gcp/compute (predefined machine types), gcp/sql (Cloud SQL tiers)
// Columns: gcp/compute/{linux,windows}, gcp/sql/{mysql,postgres}/{zonal,regional}   (per hour)
// Rates:
//
//	gcp/compute/<family>/{vcpu,ram}   per vCPU-hour and GB-hour; family n2, n2-custom, n1-custom-ext, ...
//	gcp/compute/windows/vcpu          the Windows licence, per vCPU-hour
//	gcp/compute/gpu/<model>           per GPU-hour (teslat4, a10080gb, ...)
//	gcp/disk/<pd-standard|pd-ssd|pd-balanced|pd-extreme>/storage, gcp/disk/pd-extreme/iops, gcp/disk/local-ssd/storage
//	gcp/address/{inuse,unused}, gcp/nat/{hour,data}, gcp/lb/{external,internal}/{rule,data}, gcp/gke/cluster
//	gcp/sql/<engine>/<zonal|regional>/{vcpu,ram,ssd,hdd,micro,small}
//	gcp/redis/<basic|standard>/<m1..m5>   per GB-hour
//	gcp/gcs/<class>/{storage,class-a,class-b}, gcp/run/{cpu,memory,requests}
//	gcp/pubsub/throughput, gcp/bigquery/{active-logical,long-term-logical}

import (
	"cmp"
	"fmt"
	"path"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

func init() {
	mappings["google_compute_instance"] = mapping{service: "Compute Engine", price: gcpInstance, sizes: "gcp/compute/%s"}
	mappings["google_compute_disk"] = mapping{service: "Compute Engine", price: gcpDisk}
	mappings["google_compute_address"] = mapping{service: "Compute Engine", price: gcpAddress}
	mappings["google_compute_router_nat"] = mapping{service: "Cloud NAT", price: gcpNAT}
	mappings["google_compute_forwarding_rule"] = mapping{service: "Cloud Load Balancing", price: gcpForwardingRule}
	mappings["google_container_cluster"] = mapping{service: "Kubernetes Engine", price: gcpCluster, sizes: "gcp/compute/%s"}
	mappings["google_container_node_pool"] = mapping{service: "Kubernetes Engine", price: gcpNodePool, sizes: "gcp/compute/%s"}
	// The picker lists MySQL zonal tiers; a row names its own column (engine and availability) in its SizeRef.
	mappings["google_sql_database_instance"] = mapping{service: "Cloud SQL", price: gcpSQL, sizes: "gcp/sql/mysql/zonal"}
	mappings["google_redis_instance"] = mapping{service: "Memorystore", price: gcpRedis}
	mappings["google_storage_bucket"] = mapping{service: "Cloud Storage", price: gcpBucket}
	mappings["google_cloud_run_v2_service"] = mapping{service: "Cloud Run", price: gcpRunService}
	mappings["google_cloudfunctions2_function"] = mapping{service: "Cloud Functions", price: gcpFunction}
	mappings["google_pubsub_topic"] = mapping{service: "Pub/Sub", price: gcpPubSub}
	mappings["google_bigquery_dataset"] = mapping{service: "BigQuery", price: gcpBigQuery}

	// Costs nothing by itself: networks, firewalls, routes, the router (its NAT is priced), service
	// accounts, IAM, API switches, instance groups and health checks. Load balancers charge
	// through their forwarding rules.
	for _, t := range []string{"google_compute_network", "google_compute_subnetwork", "google_compute_firewall",
		"google_compute_route", "google_compute_router", "google_compute_network_peering", "google_compute_instance_group",
		"google_compute_health_check", "google_service_account", "google_service_account_key", "google_project_service",
		"google_project_iam_member", "google_project_iam_binding", "google_project_iam_policy", "google_project_iam_custom_role",
		"google_service_account_iam_member", "google_service_account_iam_binding", "google_storage_bucket_iam_member",
		"google_storage_bucket_iam_binding"} {
		free[t] = true
	}
}

// ---- helpers

var (
	reGCPCustom = regexp.MustCompile(`^(?:([a-z]\d[a-z\d]*)-)?custom-(\d+)-(\d+)(-ext)?$`)
	reGCPZone   = regexp.MustCompile(`^[a-z]+-[a-z]+\d+-[a-z]$`)
	reSQLCustom = regexp.MustCompile(`^db-custom-(\d+)-(\d+)$`)
	reNonAlnum  = regexp.MustCompile(`[^a-z0-9]`)
)

// gcpStr is str, and notes an assumption when the attribute isn't set (a default that the
// provider doesn't document).
func gcpStr(r *res, p, def, note string) string {
	if _, ok := r.get(p); !ok {
		r.assume(note)
		return def
	}
	return r.str(p, def)
}

func gcpNum(r *res, p string, def float64, note string) float64 {
	if _, ok := r.get(p); !ok {
		r.assume(note)
		return def
	}
	return r.num(p, def)
}

// gcpReqNum returns a positive number the resource can't be priced without.
func gcpReqNum(r *res, p string) (float64, bool) {
	if _, ok := r.get(p); !ok {
		r.need(p)
		return 0, false
	}
	s := r.str(p, "") // an Unknown value is a need
	if s == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || f <= 0 {
		r.miss(p + " isn't a positive number")
		return 0, false
	}
	return f, true
}

// gcpCount returns the length of a list attribute (a nested block, or a list of strings).
func gcpCount(r *res, p string) int {
	v, ok := r.get(p)
	if !ok {
		return 0
	}
	l, _ := v.([]any)
	return len(l)
}

// usageNum returns a number from the usage file, and records that this row reads the key.
func usageNum(r *res, key string, def float64) float64 {
	if !slices.Contains(r.usageKeys, key) {
		r.usageKeys = append(r.usageKeys, key)
	}
	if v := toNum(r.usageIn[key]); v > 0 {
		return v
	}
	return def
}

// gcpWindows reports whether a boot image is a Windows one. An image that isn't known before
// apply is taken as Linux, and says so.
func gcpWindows(r *res, p string) bool {
	if v, ok := r.get(p); ok {
		if s, ok := v.(string); ok {
			return strings.Contains(strings.ToLower(s), "windows")
		}
		r.assume("assumed Linux (the boot image isn't known before apply)")
	}
	return false
}

// gcpDiskLine adds a persistent disk's storage (and, for pd-extreme, its provisioned IOPS).
func gcpDiskLine(r *res, typ string, gb, iops float64, what string) {
	typ = path.Base(strings.ToLower(typ)) // a disk type can be a URL
	if !slices.Contains([]string{"pd-standard", "pd-ssd", "pd-balanced", "pd-extreme"}, typ) {
		r.miss("pogi doesn't price the disk type " + typ + " yet")
		return
	}
	r.rate("gcp/disk/"+typ+"/storage", gb, fmt.Sprintf("%s (%s, %g GB)", what, typ, gb))
	if typ == "pd-extreme" && iops > 0 {
		r.rate("gcp/disk/pd-extreme/iops", iops, "Provisioned IOPS ("+what+")")
	}
}

// gcpMachineHours adds n machines' hours for a month: a predefined type from the column, or a
// custom type (custom-4-16384, n2-custom-4-16384, custom-4-16384-ext) from the family rates.
func gcpMachineHours(r *res, os, col, mt string, n float64) {
	hours := n * HoursPerMonth
	m := reGCPCustom.FindStringSubmatch(mt)
	if m == nil {
		r.hourly(col, mt, hours, fmt.Sprintf("Instance hours (%s, %s)", mt, osName[os]))
		return
	}
	fam := cmp.Or(m[1], "n1") + "-custom"
	vcpu, _ := strconv.ParseFloat(m[2], 64)
	gb, _ := strconv.ParseFloat(m[3], 64)
	gb /= 1024
	ramKey := "gcp/compute/" + fam + "/ram"
	if m[4] != "" {
		ramKey = "gcp/compute/" + fam + "-ext/ram" // extended memory is only a different memory rate
	}
	r.rate("gcp/compute/"+fam+"/vcpu", vcpu*hours, fmt.Sprintf("vCPU hours (%s)", mt))
	r.rate(ramKey, gb*hours, fmt.Sprintf("Memory GB-hours (%s)", mt))
	if os == "windows" {
		r.rate("gcp/compute/windows/vcpu", max(vcpu, 4)*hours, "Windows licence (at least 4 vCPUs)")
	}
}

// ---- Compute Engine

func gcpInstance(r *res) {
	os := "linux"
	if gcpWindows(r, "boot_disk.initialize_params.image") {
		os = "windows"
	}
	col := "gcp/compute/" + os
	if mt := r.size(col, "machine_type", ""); mt != "" {
		gcpMachineHours(r, os, col, mt, 1)
	}
	typ := gcpStr(r, "boot_disk.initialize_params.type", "pd-standard", "assumed a pd-standard boot disk (type isn't set)")
	gb := gcpNum(r, "boot_disk.initialize_params.size", 10, "assumed a 10 GB boot disk (size isn't set; it follows the image)")
	gcpDiskLine(r, typ, gb, r.num("boot_disk.initialize_params.provisioned_iops", 0), "Boot disk")
	if n := gcpCount(r, "scratch_disk"); n > 0 { // each local SSD is 375 GB
		r.rate("gcp/disk/local-ssd/storage", float64(n)*375, fmt.Sprintf("Local SSD (%d × 375 GB)", n))
	}
	if gcpCount(r, "network_interface.access_config") > 0 { // ponytail: first network interface only
		r.rate("gcp/address/inuse", HoursPerMonth, "External IPv4 address")
	}
	for i := 0; ; i++ {
		tp := fmt.Sprintf("guest_accelerator.%d.type", i)
		if _, ok := r.get(tp); !ok {
			break
		}
		model := gcpAccel(r.str(tp, ""))
		n := r.num(fmt.Sprintf("guest_accelerator.%d.count", i), 1)
		r.rate("gcp/compute/gpu/"+model, n*HoursPerMonth, fmt.Sprintf("GPU hours (%s × %g)", model, n))
	}
	if r.str("scheduling.provisioning_model", "") == "SPOT" || r.flag("scheduling.preemptible", false) {
		r.assume("priced at the on-demand rate (spot and preemptible VMs cost less)")
	}
}

// gcpAccel turns "nvidia-tesla-t4" into "teslat4", the form the price keys use.
func gcpAccel(s string) string {
	return reNonAlnum.ReplaceAllString(strings.TrimPrefix(strings.ToLower(path.Base(s)), "nvidia"), "")
}

func gcpDisk(r *res) {
	typ := gcpStr(r, "type", "pd-standard", "assumed a pd-standard disk (type isn't set)")
	gb := gcpNum(r, "size", 100, "assumed 100 GB (size isn't set)")
	gcpDiskLine(r, typ, gb, r.num("provisioned_iops", 0), "Disk")
}

func gcpAddress(r *res) {
	if strings.HasPrefix(strings.ToUpper(r.str("address_type", "EXTERNAL")), "INTERNAL") {
		return // internal addresses are free
	}
	r.assume("assumed attached to a VM or load balancer (an unused address costs more)")
	r.rate("gcp/address/inuse", HoursPerMonth, "External IPv4 address")
}

func gcpNAT(r *res) {
	n := usageNum(r, "nat_vm_count", 0)
	if n == 0 {
		n = 1
		r.assume("assumed 1 VM behind the NAT (set nat_vm_count in the usage file)")
	}
	n = min(n, 32) // the gateway charge stops at 32 VMs
	r.rate("gcp/nat/hour", n*HoursPerMonth, fmt.Sprintf("NAT gateway uptime (%g VMs)", n))
	r.usage("monthly_gb_processed", "gcp/nat/data", 1, "NAT data processed")
}

// ponytail: the first 5 rules of a project share one minimum charge; every rule is priced at the
// minimum-charge rate, which over-counts when there are more than 5.
func gcpForwardingRule(r *res) {
	side := "external"
	if strings.HasPrefix(strings.ToUpper(r.str("load_balancing_scheme", "EXTERNAL")), "INTERNAL") {
		side = "internal"
	}
	r.rate("gcp/lb/"+side+"/rule", HoursPerMonth, "Forwarding rule ("+side+")")
	r.usage("monthly_gb_processed", "gcp/lb/"+side+"/data", 1, "Load balancer data processed")
}

// ---- Kubernetes Engine

// gcpZones is how many zones each node count is multiplied by: node counts are per zone.
func gcpZones(r *res, p string) float64 {
	if n := gcpCount(r, p+"node_locations"); n > 0 {
		return float64(n)
	}
	loc := r.str("location", "")
	switch {
	case reGCPZone.MatchString(loc):
		return 1
	case loc != "":
		r.assume("assumed 3 zones for a regional cluster (set node_locations to change)")
		return 3
	}
	r.assume("assumed 1 zone (the location isn't in the files)")
	return 1
}

// gcpNodes adds the machines and boot disks of a pool of count nodes per zone. p is the prefix of
// the pool's attributes ("" or "node_pool.0.").
func gcpNodes(r *res, p string, count float64) {
	mt := r.size("gcp/compute/linux", p+"node_config.machine_type", "e2-medium")
	n := count * gcpZones(r, p)
	if mt != "" {
		gcpMachineHours(r, "linux", "gcp/compute/linux", mt, n)
	}
	typ := r.str(p+"node_config.disk_type", "pd-balanced")
	gb := r.num(p+"node_config.disk_size_gb", 100)
	gcpDiskLine(r, typ, gb*n, 0, "Node boot disks")
	if r.flag(p+"node_config.spot", false) || r.flag(p+"node_config.preemptible", false) {
		r.assume("priced at the on-demand rate (spot nodes cost less)")
	}
}

func gcpCluster(r *res) {
	r.rate("gcp/gke/cluster", HoursPerMonth, "Cluster management fee")
	if r.flag("enable_autopilot", false) {
		r.miss("Autopilot clusters are priced by pod resources, which pogi doesn't price yet")
		return
	}
	if pools := gcpCount(r, "node_pool"); pools > 0 {
		for i := range pools {
			p := fmt.Sprintf("node_pool.%d.", i)
			gcpNodes(r, p, gcpNum(r, p+"initial_node_count", 1, "assumed 1 node per zone (the node count isn't set)"))
		}
		return
	}
	if r.flag("remove_default_node_pool", false) {
		return
	}
	gcpNodes(r, "", gcpNum(r, "initial_node_count", 1, "assumed 1 node per zone (initial_node_count isn't set)"))
}

func gcpNodePool(r *res) {
	count := 0.0
	switch _, hasCount := r.get("node_count"); {
	case hasCount:
		count = r.num("node_count", 1)
	default:
		if _, ok := r.get("autoscaling.min_node_count"); ok {
			count = r.num("autoscaling.min_node_count", 1)
			r.assume("priced at the autoscaler's minimum node count")
		} else {
			count = gcpNum(r, "initial_node_count", 1, "assumed 1 node per zone (the node count isn't set)")
		}
	}
	gcpNodes(r, "", count)
}

// ---- Databases

func gcpSQL(r *res) {
	ver := strings.ToUpper(r.str("database_version", ""))
	eng := ""
	switch {
	case strings.HasPrefix(ver, "MYSQL"):
		eng = "mysql"
	case strings.HasPrefix(ver, "POSTGRES"):
		eng = "postgres"
	case ver == "":
		if !r.needs {
			r.need("database_version")
		}
		return
	default:
		r.miss("pogi doesn't price " + ver + " (the licence is included in its price) yet")
		return
	}
	av := strings.ToLower(r.str("settings.availability_type", "ZONAL"))
	if av != "zonal" && av != "regional" {
		r.miss("availability_type must be ZONAL or REGIONAL")
		return
	}
	col := "gcp/sql/" + eng + "/" + av
	file := r.str("settings.tier", "")
	if m := reSQLCustom.FindStringSubmatch(file); m != nil && r.pick == "" { // db-custom-<vCPUs>-<MB>
		vcpu, _ := strconv.ParseFloat(m[1], 64)
		mb, _ := strconv.ParseFloat(m[2], 64)
		r.rate(col+"/vcpu", vcpu*HoursPerMonth, fmt.Sprintf("vCPU hours (%s)", file))
		r.rate(col+"/ram", mb/1024*HoursPerMonth, fmt.Sprintf("Memory GB-hours (%s)", file))
	} else if file == "" && r.pick == "" {
		if !r.needs {
			r.need("settings.tier")
		}
	} else if tier := r.size(col, "settings.tier", ""); tier != "" {
		r.hourly(col, tier, HoursPerMonth, fmt.Sprintf("Instance hours (%s, %s)", tier, av))
	}
	gb := r.num("settings.disk_size", 10)
	switch dt := strings.ToUpper(r.str("settings.disk_type", "PD_SSD")); dt {
	case "PD_SSD":
		r.rate(col+"/ssd", gb, fmt.Sprintf("SSD storage (%g GB)", gb))
	case "PD_HDD":
		r.rate(col+"/hdd", gb, fmt.Sprintf("HDD storage (%g GB)", gb))
	default:
		r.miss("pogi doesn't price the disk type " + dt)
	}
}

func gcpRedis(r *res) {
	tier := "basic"
	if t := strings.ToUpper(r.str("tier", "BASIC")); strings.HasPrefix(t, "STANDARD") {
		tier = "standard"
	}
	gb, ok := gcpReqNum(r, "memory_size_gb")
	if !ok {
		return
	}
	m := "m5"
	for i, top := range []float64{4, 10, 35, 100} { // capacity tiers M1 to M4, by GB
		if gb <= top {
			m = fmt.Sprintf("m%d", i+1)
			break
		}
	}
	r.rate(fmt.Sprintf("gcp/redis/%s/%s", tier, m), gb*HoursPerMonth, fmt.Sprintf("Memory (%g GB, %s, %s)", gb, strings.ToUpper(m), tier))
}

// ---- Storage, serverless, messaging

func gcpBucket(r *res) {
	loc := strings.ToLower(r.str("location", ""))
	if loc == "" {
		if !r.needs {
			r.need("location")
		}
		return
	}
	r.region = loc // a multi-region such as "us" is priced under that name
	class := strings.ToLower(r.str("storage_class", "STANDARD"))
	if class == "multi_regional" || class == "regional" {
		class = "standard"
	}
	if !slices.Contains([]string{"standard", "nearline", "coldline", "archive"}, class) {
		r.miss("pogi doesn't price the storage class " + class)
		return
	}
	r.usage("storage_gb", "gcp/gcs/"+class+"/storage", 1, "Storage ("+class+")")
	r.usage("monthly_class_a_requests", "gcp/gcs/"+class+"/class-a", 1e-6, "Class A operations")
	r.usage("monthly_class_b_requests", "gcp/gcs/"+class+"/class-b", 1e-6, "Class B operations")
}

// gcpRunLines prices a request-based Cloud Run service: requests, and the CPU and memory time
// each request uses (avg_duration_ms from the usage file).
// ponytail: request-based billing only; always-on CPU and minimum instances are not priced.
func gcpRunLines(r *res, vcpu, memGB float64) {
	secs := usageNum(r, "avg_duration_ms", 0) / 1000
	r.usage("monthly_requests", "gcp/run/requests", 1e-6, "Requests")
	r.usage("monthly_requests", "gcp/run/cpu", vcpu*secs, fmt.Sprintf("CPU time (%g vCPU)", vcpu))
	r.usage("monthly_requests", "gcp/run/memory", memGB*secs, fmt.Sprintf("Memory time (%g GB)", memGB))
}

// gcpMemGB reads "512Mi", "2Gi" or "256M" into GB.
func gcpMemGB(s string) (float64, bool) {
	for _, u := range []struct {
		suffix string
		bytes  float64
	}{{"Ki", 1 << 10}, {"Mi", 1 << 20}, {"Gi", 1 << 30}, {"Ti", 1 << 40}, {"K", 1e3}, {"M", 1e6}, {"G", 1e9}, {"T", 1e12}} {
		if n, ok := strings.CutSuffix(s, u.suffix); ok {
			f, err := strconv.ParseFloat(n, 64)
			return f * u.bytes / (1 << 30), err == nil && f > 0
		}
	}
	f, err := strconv.ParseFloat(s, 64)
	return f / (1 << 30), err == nil && f > 0
}

// gcpCPU reads "1", "0.5" or "1000m" into vCPUs.
func gcpCPU(s string) (float64, bool) {
	if n, ok := strings.CutSuffix(s, "m"); ok {
		f, err := strconv.ParseFloat(n, 64)
		return f / 1000, err == nil && f > 0
	}
	f, err := strconv.ParseFloat(s, 64)
	return f, err == nil && f > 0
}

func gcpRunService(r *res) {
	cpu, ok1 := gcpCPU(r.str("template.containers.resources.limits.cpu", "1"))
	mem, ok2 := gcpMemGB(r.str("template.containers.resources.limits.memory", "512Mi"))
	if !ok1 || !ok2 {
		r.miss("can't read the CPU or memory limit")
		return
	}
	gcpRunLines(r, cpu, mem)
}

// gcpFunction prices a 2nd-generation function: it runs on Cloud Run, at the same rates.
func gcpFunction(r *res) {
	mem, ok := gcpMemGB(r.str("service_config.available_memory", "256M"))
	if !ok {
		r.miss("can't read available_memory")
		return
	}
	cpu, ok := 0.0, false
	if _, set := r.get("service_config.available_cpu"); set {
		cpu, ok = gcpCPU(r.str("service_config.available_cpu", "1"))
	}
	if !ok { // Google's default CPU for each memory size
		switch {
		case mem <= 0.25:
			cpu = 0.167
		case mem <= 0.5:
			cpu = 0.333
		case mem <= 1:
			cpu = 0.583
		case mem <= 2:
			cpu = 1
		case mem <= 8:
			cpu = 2
		case mem <= 16:
			cpu = 4
		default:
			cpu = 8
		}
		r.assume("assumed the default CPU for this memory size (available_cpu isn't set)")
	}
	gcpRunLines(r, cpu, mem)
	r.notes = append(r.notes, "2nd generation functions are priced at Cloud Run's rates")
}

func gcpPubSub(r *res) {
	r.usage("monthly_gb_published", "gcp/pubsub/throughput", 1, "Message throughput")
}

func gcpBigQuery(r *res) {
	r.region = strings.ToLower(r.str("location", "US"))
	r.usage("storage_gb", "gcp/bigquery/active-logical", 1, "Active storage")
	r.usage("long_term_storage_gb", "gcp/bigquery/long-term-logical", 1, "Long-term storage")
}
