package main

// Google Cloud prices. Two sources, both through the owner's gcloud login:
//   - the Cloud Billing Catalog API (every on-demand SKU of the services pogi prices);
//   - "gcloud compute machine-types list" (vCPU, memory, GPUs and the regions of each machine type).
//
// Compute Engine sells vCPUs and memory by family, not whole machines. So the generator keeps the
// family rates (gcp/compute/<family>/vcpu and /ram) for custom machine types, and multiplies them
// out into a price per predefined machine type and region (the gcp/compute/<os> columns).
//
// The key names are listed in internal/cost/gcp.go.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/cost"
)

func init() { builders["gcp"] = buildGCP }

const gcpAPI = "https://cloudbilling.googleapis.com/v1"

// gcpServices are the catalog services we read. A name that isn't found is skipped here, and the
// check for required rates at the end of gcpBuild fails if that left a price out.
var gcpServices = []string{"Compute Engine", "Networking", "Cloud SQL", "Memorystore for Redis", "Cloud Memorystore for Redis",
	"Cloud Storage", "Kubernetes Engine", "Cloud Run", "Cloud Pub/Sub", "BigQuery"}

// gcpSKU is the part of a catalog SKU we use.
type gcpSKU struct {
	Description string `json:"description"`
	Category    struct {
		ServiceDisplayName string `json:"serviceDisplayName"`
		ResourceFamily     string `json:"resourceFamily"`
		UsageType          string `json:"usageType"`
	} `json:"category"`
	ServiceRegions []string `json:"serviceRegions"`
	PricingInfo    []struct {
		PricingExpression struct {
			UsageUnit   string `json:"usageUnit"`
			TieredRates []struct {
				StartUsageAmount float64 `json:"startUsageAmount"`
				UnitPrice        struct {
					Units string `json:"units"`
					Nanos int64  `json:"nanos"`
				} `json:"unitPrice"`
			} `json:"tieredRates"`
		} `json:"pricingExpression"`
	} `json:"pricingInfo"`
}

// gcpMachine is one row of "gcloud compute machine-types list --format=json".
type gcpMachine struct {
	Name         string  `json:"name"`
	Zone         string  `json:"zone"`
	GuestCpus    float64 `json:"guestCpus"`
	MemoryMb     float64 `json:"memoryMb"`
	Accelerators []struct {
		Type  string `json:"guestAcceleratorType"`
		Count int    `json:"guestAcceleratorCount"`
	} `json:"accelerators"`
	Deprecated *struct {
		State string `json:"state"`
	} `json:"deprecated"`
}

func buildGCP(ctx context.Context, o opts) (*cost.DB, error) {
	start := time.Now()
	project, err := gcloud(ctx, "config", "get-value", "project")
	if err != nil {
		return nil, err
	}
	// The token is only ever in this request header. It is never written to the cache or a log.
	token, err := gcloud(ctx, "auth", "print-access-token")
	if err != nil {
		return nil, err
	}
	hdr := http.Header{"Authorization": {"Bearer " + token}, "X-Goog-User-Project": {project}}

	type service struct{ Name, DisplayName string }
	var services []service
	if err := gcpPages(ctx, o, hdr, gcpAPI+"/services?pageSize=5000", "gcp/services", func(b []byte) (string, error) {
		var p struct {
			Services []service `json:"services"`
			Next     string    `json:"nextPageToken"`
		}
		err := json.Unmarshal(b, &p)
		services = append(services, p.Services...)
		return p.Next, err
	}); err != nil {
		return nil, err
	}
	var skus []gcpSKU
	for _, want := range gcpServices {
		for _, s := range services {
			if s.DisplayName != want {
				continue
			}
			n0 := len(skus)
			err := gcpPages(ctx, o, hdr, gcpAPI+"/"+s.Name+"/skus?currencyCode=USD&pageSize=5000", "gcp/"+path.Base(s.Name), func(b []byte) (string, error) {
				var p struct {
					SKUs []gcpSKU `json:"skus"`
					Next string   `json:"nextPageToken"`
				}
				err := json.Unmarshal(b, &p)
				skus = append(skus, p.SKUs...)
				return p.Next, err
			})
			if err != nil {
				return nil, err
			}
			fmt.Fprintf(os.Stderr, "gcp: %s: %d SKUs\n", want, len(skus)-n0)
		}
	}

	mpath := filepath.Join(o.cache, "gcp", "machine-types.json")
	raw, err := os.ReadFile(mpath)
	if err != nil {
		if raw, err = gcloudRaw(ctx, "compute", "machine-types", "list", "--format=json"); err != nil {
			return nil, err
		}
		if err := os.MkdirAll(filepath.Dir(mpath), 0o755); err != nil {
			return nil, err
		}
		if err := os.WriteFile(mpath, raw, 0o644); err != nil {
			return nil, err
		}
	}
	var machines []gcpMachine
	if err := json.Unmarshal(raw, &machines); err != nil {
		return nil, fmt.Errorf("machine types: %w", err)
	}
	db, err := gcpBuild(skus, machines, time.Now().Format("2006-01-02"), o)
	fmt.Fprintf(os.Stderr, "gcp: took %s\n", time.Since(start).Round(time.Second))
	return db, err
}

// gcloud runs gcloud and returns its trimmed output. A failure usually means the login expired.
func gcloud(ctx context.Context, args ...string) (string, error) {
	b, err := gcloudRaw(ctx, args...)
	return strings.TrimSpace(string(b)), err
}

func gcloudRaw(ctx context.Context, args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, "gcloud", args...)
	cmd.Stderr = &stderr
	b, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("gcloud %s failed (data not generated: needs gcloud auth login?): %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return b, nil
}

// gcpPages fetches a paged list (cached page by page) and hands each page to decode, which
// returns the next page token.
func gcpPages(ctx context.Context, o opts, hdr http.Header, base, name string, decode func([]byte) (string, error)) error {
	for n, tok := 0, ""; ; n++ {
		u := base
		if tok != "" {
			u += "&pageToken=" + url.QueryEscape(tok)
		}
		p, err := fetch(ctx, o, u, fmt.Sprintf("%s-%d.json", name, n), hdr)
		if err != nil {
			return fmt.Errorf("%w (a 403 means the gcloud login or the billing API access is missing)", err)
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		if tok, err = decode(b); err != nil {
			return fmt.Errorf("%s: %w", p, err)
		}
		if tok == "" {
			return nil
		}
	}
}

// price returns a SKU's price per usage unit, its unit in pogi's words, and the number to
// multiply the price by to get pogi's unit. Of tiered prices it keeps the first paid tier.
// ponytail: first paid tier only; the later, cheaper tiers (high volume) are not modelled.
func (s *gcpSKU) price() (p float64, unit string, scale float64, err error) {
	if len(s.PricingInfo) == 0 {
		return 0, "", 0, fmt.Errorf("no pricing")
	}
	pe := s.PricingInfo[0].PricingExpression
	for _, t := range pe.TieredRates {
		u, _ := strconv.ParseFloat(t.UnitPrice.Units, 64)
		if v := u + float64(t.UnitPrice.Nanos)/1e9; v > 0 {
			p = v
			break
		}
	}
	if p == 0 {
		return 0, "", 0, fmt.Errorf("no paid tier")
	}
	unit, scale, ok := gcpUnit(pe.UsageUnit)
	if !ok {
		return 0, "", 0, fmt.Errorf("unknown usage unit %q", pe.UsageUnit)
	}
	return p, unit, scale, nil
}

var reCountUnit = regexp.MustCompile(`^10\^(\d+)\.count$`)

// gcpUnit turns a catalog usage unit into pogi's unit, and the number to multiply the price by.
// "TiBy" becomes GB (the price is divided by 1024), and "10^3.count" becomes a price per request.
func gcpUnit(u string) (string, float64, bool) {
	switch u {
	case "h":
		return "hour", 1, true
	case "mo":
		return "month", 1, true
	case "s":
		return "second", 1, true
	case "GiBy":
		return "GB", 1, true
	case "TiBy":
		return "GB", 1.0 / 1024, true
	case "GiBy.mo":
		return "GB-month", 1, true
	case "GiBy.h":
		return "GB-hour", 1, true
	case "GiBy.s":
		return "GB-second", 1, true
	case "count":
		return "count", 1, true
	}
	if m := reCountUnit.FindStringSubmatch(u); m != nil {
		n, _ := strconv.Atoi(m[1])
		return "count", math.Pow(10, -float64(n)), true
	}
	return "", 0, false
}

// gcpUnitNames replaces the unit of keys whose catalog unit is only "h", "s" or "mo".
var gcpUnitNames = map[string]string{"/vcpu": "vCPU-hour", "/iops": "IOPS-month", "gcp/run/cpu": "vCPU-second"}

var (
	reCompute  = regexp.MustCompile(`^(.*?)\s*(?:predefined )?(?:instance )?(core|ram) running in `)
	reSkipComp = regexp.MustCompile(`upgrade|sole|commit|reserved`)
	reFlat     = regexp.MustCompile(`^(e2 )?(micro|small|medium) instance with burstable cpu`)
	reGPU      = regexp.MustCompile(`^nvidia ([a-z0-9 -]+?) gpu running in `)
	reFamily   = regexp.MustCompile(`^[a-z]\d[a-z\d]*$`)
	reSQL      = regexp.MustCompile(`^cloud sql for (mysql|postgresql): (zonal|regional) - (.+?) in `)
	reRedis    = regexp.MustCompile(`(basic|standard)\D*?(m[1-5])\b`)
	reGCS      = regexp.MustCompile(`^(standard|nearline|coldline|archive) (storage|class a operations|class b operations)\b`)
	reNat      = regexp.MustCompile(`\bnat\b`)
	reNonAlnum = regexp.MustCompile(`[^a-z0-9]`)
)

// gcpAccel turns "nvidia-tesla-a100" or "Tesla A100" into "teslaa100", so the machine types'
// names and the catalog's descriptions meet.
func gcpAccel(s string) string {
	return reNonAlnum.ReplaceAllString(strings.TrimPrefix(strings.ToLower(s), "nvidia"), "")
}

// gcpClassify says which pogi rate a SKU is, or "" if it isn't one pogi uses. The patterns are the
// catalog's wording; if Google changes it, the required-rate check in gcpBuild says what is missing.
func gcpClassify(s *gcpSKU) string {
	if s.Category.UsageType != "OnDemand" {
		return ""
	}
	d := strings.ToLower(s.Description)
	svc := s.Category.ServiceDisplayName
	has := func(subs ...string) bool {
		for _, x := range subs {
			if !strings.Contains(d, x) {
				return false
			}
		}
		return true
	}
	switch {
	case has(" running in ") && svc == "Compute Engine":
		if m := reFlat.FindStringSubmatch(d); m != nil {
			name := map[string]string{"micro": "f1-micro", "small": "g1-small"}[m[2]]
			if m[1] != "" {
				name = "e2-" + m[2]
			}
			if name == "" {
				return ""
			}
			return "gcp/compute/flat/" + name
		}
		if m := reGPU.FindStringSubmatch(d); m != nil {
			if has("workstation", "commit") {
				return ""
			}
			return "gcp/compute/gpu/" + gcpAccel(m[1])
		}
		m := reCompute.FindStringSubmatch(d)
		if m == nil || reSkipComp.MatchString(m[1]) {
			return ""
		}
		fam := ""
		for _, w := range strings.Fields(m[1]) {
			if reFamily.MatchString(w) {
				fam = w
				break
			}
		}
		switch {
		case fam != "":
		case strings.HasPrefix(m[1], "compute optimized"):
			fam = "c2"
		case strings.HasPrefix(m[1], "memory-optimized"):
			fam = "m1"
		case strings.Contains(m[1], "custom"):
			fam = "n1"
		default:
			return ""
		}
		if strings.Contains(m[1], "custom") {
			fam += "-custom"
		}
		if strings.Contains(m[1], "extended") {
			fam += "-ext"
		}
		return "gcp/compute/" + fam + "/" + map[string]string{"core": "vcpu", "ram": "ram"}[m[2]]
	case has("windows", "licens") && !has("sql", "f1", "g1", "micro", "small"):
		return "gcp/compute/windows/vcpu"
	case strings.HasPrefix(d, "storage pd capacity"):
		return "gcp/disk/pd-standard/storage"
	case strings.HasPrefix(d, "ssd backed pd capacity"):
		return "gcp/disk/pd-ssd/storage"
	case strings.HasPrefix(d, "balanced pd capacity"):
		return "gcp/disk/pd-balanced/storage"
	case strings.HasPrefix(d, "extreme pd capacity"):
		return "gcp/disk/pd-extreme/storage"
	case strings.HasPrefix(d, "extreme pd iops"):
		return "gcp/disk/pd-extreme/iops"
	case strings.HasPrefix(d, "ssd backed local storage"):
		return "gcp/disk/local-ssd/storage"
	case strings.HasPrefix(d, "static ip charge"):
		return "gcp/address/unused"
	case strings.HasPrefix(d, "external ip charge on a standard vm"):
		return "gcp/address/inuse"
	case reNat.MatchString(d) && has("gateway") && !has("data"):
		return "gcp/nat/hour"
	case reNat.MatchString(d) && has("data processing"):
		return "gcp/nat/data"
	case has("forwarding rule", "minimum") && !has("internal", "protocol"):
		return "gcp/lb/external/rule"
	case has("internal load balancing", "forwarding rule"):
		return "gcp/lb/internal/rule"
	case has("internal load balancing", "data processing"):
		return "gcp/lb/internal/data"
	case has("load balancing", "data processing"):
		return "gcp/lb/external/data"
	case has("management fee") && !has("autopilot"):
		return "gcp/gke/cluster"
	case reSQL.MatchString(d):
		m := reSQL.FindStringSubmatch(d)
		base := "gcp/sql/" + map[string]string{"mysql": "mysql", "postgresql": "postgres"}[m[1]] + "/" + m[2] + "/"
		switch t := m[3]; {
		case t == "vcpu":
			return base + "vcpu"
		case t == "ram":
			return base + "ram"
		case strings.Contains(t, "ssd"):
			return base + "ssd"
		case strings.Contains(t, "hdd"):
			return base + "hdd"
		case strings.HasPrefix(t, "micro"):
			return base + "micro"
		case strings.HasPrefix(t, "small"):
			return base + "small"
		}
	case has("redis", "capacity") && !has("replica"):
		if m := reRedis.FindStringSubmatch(d); m != nil {
			return "gcp/redis/" + m[1] + "/" + m[2]
		}
	case reGCS.MatchString(d) && !has("dual-region", "early delete", "retrieval"):
		m := reGCS.FindStringSubmatch(d)
		return "gcp/gcs/" + m[1] + "/" + map[string]string{"storage": "storage", "class a operations": "class-a", "class b operations": "class-b"}[m[2]]
	case svc == "Cloud Run" && has("cpu") && !has("idle", "instance-based", "job", "gpu", "commit"):
		return "gcp/run/cpu"
	case svc == "Cloud Run" && has("memory") && !has("idle", "instance-based", "job", "gpu", "commit"):
		return "gcp/run/memory"
	case svc == "Cloud Run" && has("request") && !has("job", "commit"):
		return "gcp/run/requests"
	case strings.HasPrefix(d, "message delivery basic"):
		return "gcp/pubsub/throughput"
	case strings.HasPrefix(d, "active logical storage"):
		return "gcp/bigquery/active-logical"
	case strings.HasPrefix(d, "long term logical storage"):
		return "gcp/bigquery/long-term-logical"
	}
	return ""
}

// gcpRequired are the rates every price list must have. A missing one means a SKU pattern above no
// longer matches, and the build stops instead of writing a list with a hole in it.
var gcpRequired = []string{"gcp/compute/n2/vcpu", "gcp/compute/n2/ram", "gcp/compute/e2/vcpu", "gcp/compute/e2/ram",
	"gcp/compute/windows/vcpu", "gcp/disk/pd-standard/storage", "gcp/disk/pd-ssd/storage", "gcp/disk/pd-balanced/storage",
	"gcp/address/inuse", "gcp/nat/hour", "gcp/nat/data", "gcp/lb/external/rule", "gcp/lb/external/data",
	"gcp/gke/cluster", "gcp/sql/mysql/zonal/vcpu", "gcp/sql/mysql/zonal/ram", "gcp/sql/mysql/zonal/ssd",
	"gcp/sql/postgres/zonal/vcpu", "gcp/sql/postgres/zonal/ram", "gcp/sql/postgres/zonal/ssd",
	"gcp/gcs/standard/storage", "gcp/gcs/standard/class-a", "gcp/gcs/standard/class-b",
	"gcp/run/cpu", "gcp/run/memory", "gcp/run/requests", "gcp/pubsub/throughput", "gcp/bigquery/active-logical"}

// gcpSQLTiers are Cloud SQL's predefined Enterprise tiers: name, vCPUs, memory in GB. The shapes
// are from Google's Cloud SQL docs; the prices come from the catalog.
func gcpSQLTiers() []cost.Size {
	t := []cost.Size{{Name: "db-f1-micro", Family: "shared", VCPU: 1, MemGB: 0.6}, {Name: "db-g1-small", Family: "shared", VCPU: 1, MemGB: 1.7}}
	for _, c := range []float64{1, 2, 4, 8, 16, 32, 64, 96} {
		t = append(t, cost.Size{Name: fmt.Sprintf("db-n1-standard-%g", c), Family: "n1-standard", VCPU: c, MemGB: c * 3.75})
	}
	for _, c := range []float64{2, 4, 8, 16, 32, 64, 96} {
		t = append(t, cost.Size{Name: fmt.Sprintf("db-n1-highmem-%g", c), Family: "n1-highmem", VCPU: c, MemGB: c * 6.5})
	}
	return t
}

// machine is a machine type with the regions it is sold in.
type machine struct {
	cost.Size
	accel   string
	count   int
	regions map[string]bool
}

// gcpBuild turns the catalog SKUs and the machine-type list into a price list. It makes no
// network calls, so the tests run it on small samples.
func gcpBuild(skus []gcpSKU, machines []gcpMachine, published string, o opts) (*cost.DB, error) {
	byName := map[string]*machine{}
	var names, regions []string
	for _, m := range machines {
		zone := path.Base(m.Zone) // "us-central1-a" belongs to region "us-central1"
		i := strings.LastIndex(zone, "-")
		// ponytail: types that bundle local SSD (-lssd) are left out; their SSD is priced apart.
		if i < 0 || strings.Contains(m.Name, "lssd") || !o.wanted(zone[:i]) {
			continue
		}
		region := zone[:i]
		if !slices.Contains(regions, region) {
			regions = append(regions, region)
		}
		x := byName[m.Name]
		if x == nil {
			fam, _, _ := strings.Cut(m.Name, "-")
			x = &machine{Size: cost.Size{Name: m.Name, Family: fam, VCPU: m.GuestCpus, MemGB: m.MemoryMb / 1024,
				Old: fam == "n1" || fam == "f1" || fam == "g1" || m.Deprecated != nil}, regions: map[string]bool{}}
			if fam == "t2a" || fam == "c4a" {
				x.Arch = "arm64"
			}
			for _, a := range m.Accelerators {
				x.GPU, x.GPUModel, x.accel, x.count = a.Count, a.Type, a.Type, a.Count
			}
			byName[m.Name] = x
			names = append(names, m.Name)
		}
		x.regions[region] = true
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("no machine types for the wanted regions")
	}
	slices.Sort(names)
	slices.Sort(regions)

	// Classify the SKUs into rates: key → region → price.
	rates := map[string]map[string]float64{}
	units := map[string]string{}
	var warns []string
	for i := range skus {
		s := &skus[i]
		key := gcpClassify(s)
		if key == "" {
			continue
		}
		p, unit, scale, err := s.price()
		if err != nil {
			return nil, fmt.Errorf("SKU %q: %w", s.Description, err)
		}
		p *= scale
		if unit == "count" { // request counts are sold per 1M requests
			p, unit = p*1e6, "1M requests"
		}
		for suffix, name := range gcpUnitNames {
			if strings.HasSuffix(key, suffix) {
				unit = name
			}
		}
		if prev, ok := units[key]; ok && prev != unit {
			return nil, fmt.Errorf("SKU %q: unit %s, but other SKUs for %s use %s", s.Description, unit, key, prev)
		}
		units[key] = unit
		if rates[key] == nil {
			rates[key] = map[string]float64{}
		}
		for _, r := range s.ServiceRegions {
			r = strings.ToLower(r)
			targets := []string{r}
			if r == "global" { // the Windows licence and Pub/Sub have no region
				targets = regions
			}
			for _, t := range targets {
				if !o.wanted(t) && r != "global" {
					continue
				}
				if old, ok := rates[key][t]; ok {
					if math.Abs(old-p) > 1e-9 {
						warns = append(warns, fmt.Sprintf("%s in %s: %g and %g (%s); kept the first", key, t, old, p, s.Description))
					}
					continue
				}
				rates[key][t] = p
			}
		}
	}
	for _, w := range warns[:min(len(warns), 20)] {
		fmt.Fprintln(os.Stderr, "gcp: ambiguous:", w)
	}
	var missing []string
	for _, k := range gcpRequired {
		if len(rates[k]) == 0 {
			missing = append(missing, k)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("no SKU matched these rates (did the catalog's wording change?): %s", strings.Join(missing, ", "))
	}

	db := &cost.DB{
		Published: map[string]string{"gcp": published},
		Source:    map[string]string{"gcp": "Google Cloud Billing Catalog API"},
		Tables:    map[string][]cost.Size{}, Columns: map[string]*cost.Column{}, Rates: map[string]*cost.Rate{},
	}

	// Predefined machine types: vCPUs × the family's vCPU rate + GB × its memory rate (+ GPUs).
	sizes := make([]cost.Size, len(names))
	for i, n := range names {
		sizes[i] = byName[n].Size
	}
	linux := &cost.Column{Table: "gcp/compute", Unit: "hour", Prices: map[string][]float64{}}
	windows := &cost.Column{Table: "gcp/compute", Unit: "hour", Prices: map[string][]float64{}}
	unpriced := map[string]bool{}
	for _, region := range regions {
		lp, wp := make([]float64, len(names)), make([]float64, len(names))
		for i, n := range names {
			m := byName[n]
			if !m.regions[region] {
				continue
			}
			var p float64
			_, flat := rates["gcp/compute/flat/"+n]
			if flat {
				p = rates["gcp/compute/flat/"+n][region]
			} else {
				v, r := rates["gcp/compute/"+m.Family+"/vcpu"][region], rates["gcp/compute/"+m.Family+"/ram"][region]
				if v == 0 || r == 0 {
					unpriced[m.Family] = true
					continue
				}
				p = v*m.VCPU + r*m.MemGB
				if m.count > 0 {
					g := rates["gcp/compute/gpu/"+gcpAccel(m.accel)][region]
					if g == 0 {
						unpriced[m.Family] = true
						continue
					}
					p += g * float64(m.count)
				}
			}
			if p == 0 {
				continue
			}
			lp[i] = sig6(p)
			// ponytail: Windows is billed per vCPU with at least 4 vCPUs (Google's Windows pricing page);
			// the shared-core types get no Windows price.
			if w := rates["gcp/compute/windows/vcpu"][region]; w > 0 && !flat {
				wp[i] = sig6(p + w*math.Max(m.VCPU, 4))
			}
		}
		linux.Prices[region], windows.Prices[region] = lp, wp
	}
	if len(unpriced) > 0 {
		fam := make([]string, 0, len(unpriced))
		for f := range unpriced {
			fam = append(fam, f)
		}
		slices.Sort(fam)
		fmt.Fprintf(os.Stderr, "gcp: no vCPU, memory or GPU SKU for these machine families, so they have no price: %s\n", strings.Join(fam, " "))
	}
	db.Tables["gcp/compute"] = sizes
	db.Columns["gcp/compute/linux"], db.Columns["gcp/compute/windows"] = linux, windows

	// Cloud SQL tiers: vCPUs × vCPU rate + GB × memory rate; the shared-core tiers are flat.
	tiers := gcpSQLTiers()
	db.Tables["gcp/sql"] = tiers
	for _, eng := range []string{"mysql", "postgres"} {
		for _, av := range []string{"zonal", "regional"} {
			base := "gcp/sql/" + eng + "/" + av + "/"
			col := &cost.Column{Table: "gcp/sql", Unit: "hour", Prices: map[string][]float64{}}
			for region, v := range rates[base+"vcpu"] {
				r := rates[base+"ram"][region]
				row := make([]float64, len(tiers))
				for i, t := range tiers {
					switch {
					case t.Name == "db-f1-micro":
						row[i] = rates[base+"micro"][region]
					case t.Name == "db-g1-small":
						row[i] = rates[base+"small"][region]
					case r > 0:
						row[i] = sig6(v*t.VCPU + r*t.MemGB)
					}
				}
				col.Prices[region] = row
			}
			if len(col.Prices) > 0 {
				db.Columns["gcp/sql/"+eng+"/"+av] = col
			}
		}
	}

	// Every other rate is kept as it is. Custom machine types and Cloud SQL custom tiers need the
	// family rates; the flat shared-core prices are already in the columns.
	for key, byRegion := range rates {
		if strings.HasPrefix(key, "gcp/compute/flat/") {
			continue
		}
		r := &cost.Rate{Unit: units[key], Prices: map[string]float64{}}
		for region, p := range byRegion {
			r.Prices[region] = sig6(p)
		}
		db.Rates[key] = r
	}
	return db, nil
}

// sig6 rounds to 6 significant digits, which keeps the file small. Real prices have fewer digits.
func sig6(x float64) float64 {
	v, _ := strconv.ParseFloat(strconv.FormatFloat(x, 'g', 6, 64), 64)
	return v
}
