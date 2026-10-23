package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/cost"
)

func init() { builders["azure"] = buildAzure }

// item is the part of a Retail Prices API row the generator reads.
type item struct {
	ArmRegionName string  `json:"armRegionName"`
	Location      string  `json:"location"`
	MeterName     string  `json:"meterName"`
	ProductName   string  `json:"productName"`
	SkuName       string  `json:"skuName"`
	ArmSkuName    string  `json:"armSkuName"`
	ServiceName   string  `json:"serviceName"`
	Unit          string  `json:"unitOfMeasure"`
	Price         float64 `json:"retailPrice"`
	Tier          float64 `json:"tierMinimumUnits"`
	Type          string  `json:"type"`
}

type page struct {
	Items        []item `json:"Items"`
	NextPageLink string `json:"NextPageLink"`
}

// azureServices are the Retail Prices API service names the generator reads.
var azureServices = []string{
	"Virtual Machines", "Storage", "Virtual Network", "Load Balancer", "NAT Gateway", "Application Gateway",
	"Azure Kubernetes Service", "Azure App Service", "Functions", "SQL Database",
	"Azure Database for PostgreSQL", "Azure Database for MySQL", "Redis Cache", "Azure Cosmos DB", "Service Bus",
}

const retailURL = "https://prices.azure.com/api/retail/prices?api-version=2023-01-01-preview&$filter="

// serviceItems reads every on-demand ("Consumption") row of one service, page by page. Each page
// is kept in the cache, so a second run reads no network.
func serviceItems(ctx context.Context, o opts, service string) ([]item, error) {
	slug := strings.ReplaceAll(strings.ToLower(service), " ", "-")
	next := retailURL + url.QueryEscape(fmt.Sprintf("serviceName eq '%s' and priceType eq 'Consumption'", service))
	var all []item
	for n := 0; next != ""; n++ {
		name := fmt.Sprintf("azure/%s/%04d.json", slug, n)
		var path string
		var err error
		for try := 0; try < 5; try++ { // the API answers 429 when it is busy
			if path, err = fetch(ctx, o, next, name, nil); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(try+1) * 5 * time.Second):
			}
		}
		if err != nil {
			return nil, fmt.Errorf("%s page %d: %w", service, n, err)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		var p page
		if err := json.Unmarshal(b, &p); err != nil {
			return nil, fmt.Errorf("%s page %d: %w (delete %s and rerun)", service, n, err, path)
		}
		all = append(all, p.Items...)
		next = p.NextPageLink
	}
	return all, nil
}

// allItems reads every service, four at a time.
func allItems(ctx context.Context, o opts) (map[string][]item, error) {
	out := map[string][]item{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	sem := make(chan struct{}, 4)
	errs := make(chan error, len(azureServices))
	for _, s := range azureServices {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			items, err := serviceItems(ctx, o, s)
			if err != nil {
				errs <- err
				return
			}
			fmt.Printf("azure: %s: %d rows\n", s, len(items))
			mu.Lock()
			out[s] = items
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return nil, err
	}
	return out, nil
}

// ---- regions ----

// azureRegions lists the public Azure regions with VM prices. The list of regions comes from
// "az account list-locations" (an az login); staging and early-access regions are left out, and so
// are edge zones, Government, China and sovereign partitions, which that command doesn't list.
func azureRegions(ctx context.Context, o opts, items []item) ([]string, error) {
	path := filepath.Join(o.cache, "azure", "locations.json")
	if _, err := os.Stat(path); err != nil {
		out, err := exec.CommandContext(ctx, "az", "account", "list-locations", "--query", "[?metadata.regionType=='Physical'].name", "-o", "json").Output()
		if err != nil {
			return nil, fmt.Errorf("az account list-locations (needs an az login): %w", err)
		}
		if err := os.WriteFile(path, out, 0o644); err != nil {
			return nil, err
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var names []string
	if err := json.Unmarshal(b, &names); err != nil {
		return nil, err
	}
	public := map[string]bool{}
	for _, n := range names {
		if !strings.HasSuffix(n, "stg") && !strings.HasSuffix(n, "euap") {
			public[n] = true
		}
	}
	has := map[string]bool{}
	for _, it := range items {
		if public[it.ArmRegionName] {
			has[it.ArmRegionName] = true
		}
	}
	var out []string
	for r := range has {
		if o.wanted(r) {
			out = append(out, r)
		}
	}
	slices.Sort(out)
	if len(out) == 0 {
		return nil, fmt.Errorf("no public region has VM prices (regions asked for: %v)", o.regions)
	}
	return out, nil
}

// ---- picking one price per key and region ----

// picker collects prices as key → region → price. A key's price in a region is its first paid
// tier: the lowest tierMinimumUnits with a price above zero. Rows priced in "Global" count for
// every region that has no price of its own.
type picker struct {
	regions   map[string]bool
	p         map[string]map[string]float64
	tier      map[string]float64 // key|region → the tier chosen
	units     map[string]string  // key → unit
	conflicts []string
}

func newPicker(regions []string) *picker {
	pk := &picker{regions: map[string]bool{}, p: map[string]map[string]float64{}, tier: map[string]float64{}, units: map[string]string{}}
	for _, r := range regions {
		pk.regions[r] = true
	}
	return pk
}

// add records one row under key. scale turns the row's unit into the key's unit.
func (pk *picker) add(key string, it item, scale float64) {
	region := it.ArmRegionName
	switch strings.ToLower(region) {
	case "", "global":
		region = "global"
	default:
		if !pk.regions[region] {
			return
		}
	}
	if it.Price <= 0 {
		return
	}
	price := it.Price * scale
	if pk.p[key] == nil {
		pk.p[key] = map[string]float64{}
	}
	tk := key + "|" + region
	old, seen := pk.p[key][region]
	switch {
	case !seen || it.Tier < pk.tier[tk]:
		pk.p[key][region], pk.tier[tk] = price, it.Tier
	case it.Tier == pk.tier[tk] && math.Abs(old-price) > 1e-9*math.Max(old, price):
		// Two meters for one price: Azure keeps a stale one beside the current one in a few regions
		// (the current one is the higher, as a region's price is never below the base). Keep the higher.
		pk.conflicts = append(pk.conflicts, fmt.Sprintf("%s in %s: %v and %v (%s / %s)", key, region, old, price, it.ProductName, it.MeterName))
		pk.p[key][region] = math.Max(old, price)
	}
}

// addRate is add for a rate, which names its unit.
func (pk *picker) addRate(key, unit string, it item, scale float64) {
	pk.units[key] = unit
	pk.add(key, it, scale)
}

// price returns a key's price in a region.
func (pk *picker) price(key, region string) float64 {
	if p, ok := pk.p[key][region]; ok {
		return p
	}
	return pk.p[key]["global"]
}

// check reports rows that disagreed about a price. A few are expected (see add); a lot means a filter is too loose.
func (pk *picker) check(what string) error {
	if len(pk.conflicts) == 0 {
		return nil
	}
	slices.Sort(pk.conflicts)
	fmt.Printf("azure: %s: %d prices had two meters; kept the higher, for example: %s\n", what, len(pk.conflicts), pk.conflicts[0])
	if len(pk.conflicts) > 20 {
		return fmt.Errorf("%s: %d prices disagree, a filter is too loose:\n  %s", what, len(pk.conflicts), strings.Join(pk.conflicts[:8], "\n  "))
	}
	return nil
}

// rates writes every key as a rate. It fails when a key has no price anywhere.
func (pk *picker) rates(db *cost.DB, regions []string) {
	for key, byRegion := range pk.p {
		r := &cost.Rate{Unit: pk.units[key], Prices: map[string]float64{}}
		for _, reg := range regions {
			if p := pk.price(key, reg); p > 0 {
				r.Prices[reg] = round6(p)
			}
		}
		if len(r.Prices) == 0 && len(byRegion) > 0 {
			continue
		}
		db.Rates[key] = r
	}
}

// round6 keeps six significant digits, which is below the precision Azure publishes and keeps the data small.
func round6(x float64) float64 {
	if x == 0 {
		return 0
	}
	e := math.Pow(10, 5-math.Floor(math.Log10(math.Abs(x))))
	return math.Round(x*e) / e
}

// table writes a table of sizes and one column per variant. pk keys are "<column>/<size name>".
func (pk *picker) table(db *cost.DB, table, unit string, regions []string, info func(name string) cost.Size) {
	names := map[string]bool{}
	cols := map[string]bool{}
	for key := range pk.p {
		col, name, _ := strings.Cut(key, "/")
		cols[col], names[name] = true, true
	}
	sorted := make([]string, 0, len(names))
	for n := range names {
		sorted = append(sorted, n)
	}
	slices.Sort(sorted)
	sizes := make([]cost.Size, len(sorted))
	for i, n := range sorted {
		sizes[i] = info(n)
		sizes[i].Name = n
	}
	db.Tables[table] = sizes
	for col := range cols {
		c := &cost.Column{Table: table, Unit: unit, Prices: map[string][]float64{}}
		for _, reg := range regions {
			row := make([]float64, len(sorted))
			any := false
			for i, n := range sorted {
				if p := pk.price(col+"/"+n, reg); p > 0 {
					row[i], any = round6(p), true
				}
			}
			if any {
				c.Prices[reg] = row
			}
		}
		db.Columns[table+"/"+col] = c
	}
}

// ---- VM sizes ----

type skuCap struct{ Name, Value string }
type sku struct {
	Name         string   `json:"name"`
	Family       string   `json:"family"`
	Capabilities []skuCap `json:"capabilities"`
}

// vmSizes reads "az vm list-skus", once, into the cache.
func vmSizes(ctx context.Context, o opts) (map[string]cost.Size, error) {
	path := filepath.Join(o.cache, "azure", "skus.json")
	if _, err := os.Stat(path); err != nil {
		tmp := path + ".part"
		cmd := exec.CommandContext(ctx, "az", "vm", "list-skus", "--all", "--resource-type", "virtualMachines", "-o", "json")
		out, err := os.Create(tmp)
		if err != nil {
			return nil, err
		}
		cmd.Stdout = out
		err = cmd.Run()
		out.Close()
		if err != nil {
			os.Remove(tmp)
			return nil, fmt.Errorf("az vm list-skus (needs an az login): %w", err)
		}
		if err := os.Rename(tmp, path); err != nil {
			return nil, err
		}
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var skus []sku
	if err := json.NewDecoder(f).Decode(&skus); err != nil {
		return nil, fmt.Errorf("%s: %w (delete it and rerun)", path, err)
	}
	out := map[string]cost.Size{}
	for _, s := range skus {
		if _, done := out[s.Name]; done {
			continue
		}
		sz := cost.Size{Name: s.Name, Family: vmFamily(s.Name)}
		for _, c := range s.Capabilities {
			switch c.Name {
			case "vCPUs":
				sz.VCPU, _ = strconv.ParseFloat(c.Value, 64)
			case "MemoryGB":
				sz.MemGB, _ = strconv.ParseFloat(c.Value, 64)
			case "GPUs":
				n, _ := strconv.Atoi(c.Value)
				sz.GPU = n
			case "CpuArchitectureType":
				if strings.EqualFold(c.Value, "Arm64") {
					sz.Arch = "arm64"
				}
			}
		}
		if sz.GPU > 0 {
			sz.GPUModel = gpuModel(s.Name)
		}
		out[s.Name] = sz
	}
	return out, nil
}

var sizeNumRE = regexp.MustCompile(`\d+(-\d+)?`)

// vmFamily names a size's family from its name, without the size number: Standard_D2s_v5 → Ds_v5.
func vmFamily(name string) string {
	name = strings.TrimPrefix(strings.TrimPrefix(name, "Standard_"), "Basic_")
	if i := sizeNumRE.FindStringIndex(name); i != nil {
		name = name[:i[0]] + name[i[1]:]
	}
	return name
}

// vmInfo returns a size's specs. Sizes the SKU list doesn't show get their family and, from the number in their
// name, their vCPUs (the last number of a constrained name such as E16-4as); memory stays empty.
func vmInfo(vms map[string]cost.Size, name string) cost.Size {
	if sz, ok := vms[name]; ok {
		return sz
	}
	sz := cost.Size{Name: name, Family: vmFamily(name)}
	if m := sizeNumRE.FindString(strings.TrimPrefix(strings.TrimPrefix(name, "Standard_"), "Basic_")); m != "" {
		_, last, _ := strings.Cut(m, "-")
		if last == "" {
			last = m
		}
		sz.VCPU, _ = strconv.ParseFloat(last, 64)
	}
	return sz
}

// gpuModels maps a piece of a size's name to its GPU. Azure's SKU list gives the count, not the model.
var gpuModels = []struct{ in, model string }{
	{"A100", "A100"}, {"H100", "H100"}, {"H200", "H200"}, {"MI300X", "MI300X"}, {"A10", "A10"}, {"T4", "T4"}, {"RTX6k", "RTX PRO 6000"}, {"RTXPRO6000", "RTX PRO 6000"},
	{"NC6s_v3", "V100"}, {"NC12s_v3", "V100"}, {"NC24s_v3", "V100"}, {"NC24rs_v3", "V100"}, {"ND40rs_v2", "V100"}, {"NDs_v2", "V100"},
	{"NC_v2", "P100"}, {"NCs_v2", "P100"}, {"ND6s", "P40"}, {"ND12s", "P40"}, {"ND24s", "P40"}, {"ND24rs", "P40"},
	{"NV12s_v3", "M60"}, {"NV24s_v3", "M60"}, {"NV48s_v3", "M60"}, {"NV6", "M60"}, {"NV12", "M60"}, {"NV24", "M60"},
	{"NC6", "K80"}, {"NC12", "K80"}, {"NC24", "K80"}, {"NVs_v4", "MI25"}, {"NV4as_v4", "MI25"}, {"NV8as_v4", "MI25"}, {"NV16as_v4", "MI25"}, {"NV32as_v4", "MI25"},
}

func gpuModel(name string) string {
	for _, g := range gpuModels {
		if strings.Contains(name, g.in) {
			return g.model
		}
	}
	return ""
}

var versionRE = regexp.MustCompile(`_v(\d+)$`)

// markOld flags a VM size as a previous generation when a newer version of its series is sold,
// for example Standard_D2s_v3 when Standard_D2s_v5 exists.
func markOld(sizes map[string]cost.Size) {
	series := func(name string) (string, int) {
		v := 1
		if m := versionRE.FindStringSubmatch(name); m != nil {
			v, _ = strconv.Atoi(m[1])
			name = name[:len(name)-len(m[0])]
		}
		return strings.Map(func(r rune) rune {
			if r >= '0' && r <= '9' || r == '-' {
				return -1
			}
			return r
		}, name), v
	}
	newest := map[string]int{}
	for n := range sizes {
		s, v := series(n)
		newest[s] = max(newest[s], v)
	}
	for n, sz := range sizes {
		if s, v := series(n); v+1 < newest[s] { // two or more generations behind
			sz.Old = true
			sizes[n] = sz
		}
	}
}

// ---- the build ----

func contains(s string, subs ...string) bool {
	for _, x := range subs {
		if strings.Contains(s, x) {
			return true
		}
	}
	return false
}

func buildAzure(ctx context.Context, o opts) (*cost.DB, error) {
	all, err := allItems(ctx, o)
	if err != nil {
		return nil, err
	}
	regions, err := azureRegions(ctx, o, all["Virtual Machines"])
	if err != nil {
		return nil, err
	}
	vms, err := vmSizes(ctx, o)
	if err != nil {
		return nil, err
	}
	markOld(vms)
	db := &cost.DB{
		Published: map[string]string{"azure": time.Now().Format("2006-01-02")},
		Source:    map[string]string{"azure": "Azure Retail Prices API (pay-as-you-go list prices, USD)"},
		Tables:    map[string][]cost.Size{}, Columns: map[string]*cost.Column{}, Rates: map[string]*cost.Rate{},
	}
	for _, step := range []func(*cost.DB, map[string][]item, []string, map[string]cost.Size) error{
		vmStep, diskStep, networkStep, kubernetesAndAppsStep, databaseStep, dataStep,
	} {
		if err := step(db, all, regions, vms); err != nil {
			return nil, err
		}
	}
	fmt.Printf("azure: %d regions\n", len(regions))
	return db, nil
}

// vmStep: virtual machine sizes, Linux and Windows, on-demand.
func vmStep(db *cost.DB, all map[string][]item, regions []string, vms map[string]cost.Size) error {
	pk := newPicker(regions)
	for _, it := range all["Virtual Machines"] {
		n := it.ArmSkuName
		if !strings.HasPrefix(n, "Standard_") && !strings.HasPrefix(n, "Basic_") || it.Type != "Consumption" {
			continue
		}
		if contains(it.SkuName+it.MeterName, "Spot", "Low Priority") ||
			contains(it.ProductName, "Dedicated", "Cloud Services", "CloudServices", "Licenses", "Red Hat", "SUSE", "SQL", "Ubuntu Pro", "RHEL") {
			continue
		}
		os := "linux"
		if strings.Contains(it.ProductName, "Windows") || strings.HasSuffix(it.ProductName, " Win") || strings.HasSuffix(it.ProductName, " Srs Win") {
			os = "windows"
		}
		pk.add(os+"/"+n, it, 1)
	}
	if err := pk.check("virtual machines"); err != nil {
		return err
	}
	pk.table(db, "azure/vm", "hour", regions, func(n string) cost.Size { return vmInfo(vms, n) })
	return nil
}

// diskStep: managed disks by tier, and the per-use prices of the other disk kinds.
func diskStep(db *cost.DB, all map[string][]item, regions []string, _ map[string]cost.Size) error {
	pk := newPicker(regions)
	kinds := map[string]string{"Premium SSD Managed Disks": "premium", "Standard SSD Managed Disks": "standardssd", "Standard HDD Managed Disks": "standardhdd"}
	tierRE := regexp.MustCompile(`^([PES]\d+) (LRS|ZRS) Disk$`)
	for _, it := range all["Storage"] {
		if kind := kinds[it.ProductName]; kind != "" {
			if m := tierRE.FindStringSubmatch(it.MeterName); m != nil {
				pk.addRate("azure/disk/"+kind+"/"+strings.ToLower(m[2])+"/"+strings.ToLower(m[1]), "disk-month", it, 1)
			} else if red, ok := strings.CutPrefix(it.SkuName, "Disk Transactions "); ok && strings.HasSuffix(it.MeterName, "Disk Operations") && kind != "premium" {
				pk.addRate("azure/disk/"+kind+"/"+strings.ToLower(red)+"/ops", "10K operations", it, 1)
			}
			continue
		}
		var kind string
		switch it.ProductName {
		case "Azure Premium SSD v2":
			kind = "premiumv2"
		case "Ultra Disks":
			kind = "ultra"
		default:
			continue
		}
		// Azure prices these per hour; the data keeps them per month (730 hours).
		switch {
		case strings.HasSuffix(it.MeterName, "Provisioned Capacity") && !strings.Contains(it.MeterName, "Encryption"):
			pk.addRate("azure/disk/"+kind+"/storage", "GB-month", it, HoursPerMonth)
		case strings.HasSuffix(it.MeterName, "Provisioned IOPS"):
			pk.addRate("azure/disk/"+kind+"/iops", "IOPS-month", it, HoursPerMonth)
		case strings.HasSuffix(it.MeterName, "Provisioned Throughput (MBps)"):
			pk.addRate("azure/disk/"+kind+"/mbps", "MBps-month", it, HoursPerMonth)
		}
	}
	if err := pk.check("disks"); err != nil {
		return err
	}
	pk.rates(db, regions)
	return nil
}

// HoursPerMonth is the month every price uses.
const HoursPerMonth = cost.HoursPerMonth

// networkStep: public IPs, NAT gateways, load balancers and application gateways.
func networkStep(db *cost.DB, all map[string][]item, regions []string, _ map[string]cost.Size) error {
	pk := newPicker(regions)
	for _, it := range all["Virtual Network"] {
		if it.ProductName != "IP Addresses" {
			continue
		}
		switch it.MeterName {
		case "Basic IPv4 Dynamic Public IP":
			pk.addRate("azure/pip/basic/dynamic", "hour", it, 1)
		case "Basic IPv4 Static Public IP":
			pk.addRate("azure/pip/basic/static", "hour", it, 1)
		case "Standard IPv4 Static Public IP":
			pk.addRate("azure/pip/standard/static", "hour", it, 1)
		}
	}
	for _, it := range all["NAT Gateway"] {
		switch it.MeterName {
		case "Standard Gateway":
			pk.addRate("azure/natgw/hour", "hour", it, 1)
		case "Standard Data Processed":
			pk.addRate("azure/natgw/data", "GB", it, 1)
		}
	}
	lb := map[string]string{
		"Standard Included LB Rules and Outbound Rules": "azure/lb/standard/hour", "Standard Overage LB Rules and Outbound Rules": "azure/lb/standard/extra-rule-hour",
		"Standard Data Processed": "azure/lb/standard/data", "Global Included LB Rules and Outbound Rules": "azure/lb/global/hour",
		"Global Overage LB Rules and Outbound Rules": "azure/lb/global/extra-rule-hour", "Global Data Processed": "azure/lb/global/data",
		"Gateway": "azure/lb/gateway/hour", "Gateway Chain": "azure/lb/gateway/chain-hour", "Gateway Data Processed": "azure/lb/gateway/data",
	}
	for _, it := range all["Load Balancer"] {
		if key := lb[it.MeterName]; key != "" {
			unit := "hour"
			if strings.HasSuffix(key, "data") {
				unit = "GB"
			}
			pk.addRate(key, unit, it, 1)
		}
	}
	for _, it := range all["Application Gateway"] {
		var sku string
		switch it.ProductName {
		case "Application Gateway Standard v2":
			sku = "standard_v2"
		case "Application Gateway WAF v2":
			sku = "waf_v2"
		case "Application Gateway Basic v2":
			sku = "basic_v2"
		case "Basic Application Gateway":
			sku = "standard"
		case "WAF Application Gateway":
			sku = "waf"
		default:
			continue
		}
		f := strings.Fields(it.MeterName) // "Medium Gateway", "Small Data Processed", "Standard Fixed Cost", "Basic Capacity Units"
		switch {
		case strings.HasSuffix(it.MeterName, "Fixed Cost"):
			pk.addRate("azure/appgw/"+sku+"/fixed", "hour", it, 1)
		case strings.HasSuffix(it.MeterName, "Capacity Units"):
			pk.addRate("azure/appgw/"+sku+"/cu", "capacity-unit-hour", it, 1)
		case strings.HasSuffix(it.MeterName, " Gateway") && len(f) == 2:
			pk.addRate("azure/appgw/"+sku+"/"+strings.ToLower(f[0]), "hour", it, 1)
		case strings.HasSuffix(it.MeterName, " Data Processed") && sku == "standard":
			pk.addRate("azure/appgw/"+sku+"/"+strings.ToLower(f[0])+"/data", "GB", it, 1)
		}
	}
	if err := pk.check("network"); err != nil {
		return err
	}
	pk.rates(db, regions)
	return nil
}

var appPlanRE = regexp.MustCompile(`^Azure App Service (\w+(?: v\d)?) Plan( - Linux)?$`)
var appSkuRE = regexp.MustCompile(`^([BSPI]\d?m?v?\d?|Shared)$`)

// kubernetesAndAppsStep: AKS cluster fees, App Service plans and Functions.
func kubernetesAndAppsStep(db *cost.DB, all map[string][]item, regions []string, _ map[string]cost.Size) error {
	pk := newPicker(regions)
	for _, it := range all["Azure Kubernetes Service"] {
		if it.ProductName != "Azure Kubernetes Service" {
			continue
		}
		switch it.MeterName {
		case "Standard Uptime SLA":
			pk.addRate("azure/aks/standard", "hour", it, 1)
		case "Standard Long Term Support":
			pk.addRate("azure/aks/premium", "hour", it, 1)
		}
	}
	for _, it := range all["Functions"] {
		switch {
		case it.ProductName == "Functions" && it.MeterName == "Standard Total Executions":
			pk.addRate("azure/functions/executions", "1M executions", it, 1e5) // Azure lists them per 10
		case it.ProductName == "Functions" && it.MeterName == "Standard Execution Time":
			pk.addRate("azure/functions/gb-seconds", "GB-second", it, 1)
		case it.ProductName == "Premium Functions" && it.MeterName == "Premium vCPU Duration":
			pk.addRate("azure/functions/premium/vcpu", "vCPU-hour", it, 1)
		case it.ProductName == "Premium Functions" && it.MeterName == "Premium Memory Duration":
			pk.addRate("azure/functions/premium/memory", "GB-hour", it, 1)
		}
	}
	if err := pk.check("kubernetes and functions"); err != nil {
		return err
	}
	pk.rates(db, regions)

	plans := newPicker(regions)
	for _, it := range all["Azure App Service"] {
		m := appPlanRE.FindStringSubmatch(it.ProductName)
		if m == nil || strings.Contains(it.ProductName, "Container") {
			continue
		}
		sku := strings.ReplaceAll(it.SkuName, " ", "")
		if sku == "Shared" {
			sku = "D1" // Terraform's name for the Shared plan
		}
		if !appSkuRE.MatchString(strings.ReplaceAll(sku, "D1", "Shared")) || it.MeterName != it.SkuName+" App" && it.MeterName != it.SkuName {
			continue
		}
		os := "windows"
		if m[2] != "" {
			os = "linux"
		}
		plans.add(os+"/"+sku, it, 1)
	}
	if err := plans.check("app service plans"); err != nil {
		return err
	}
	plans.table(db, "azure/appservice", "hour", regions, func(n string) cost.Size { return cost.Size{Family: n[:1]} })
	return nil
}

var (
	sqlVcoreRE  = regexp.MustCompile(`^(\d+) vCore$`)
	sqlVcoreZRE = regexp.MustCompile(`^(\d+) vCore Zone Redundancy$`)
	dtuRE       = regexp.MustCompile(`^(S\d+|P\d+)$`)
	vmSeriesRE  = regexp.MustCompile(`^Standard_([A-Z]+)(\d+)([a-z]*)_(v\d+)$`)
	burstableRE = regexp.MustCompile(`^b\d+(ms|s)$`)
	flexTokenRE = regexp.MustCompile(`^[A-Z][A-Za-z]*v\d+$`)
	// flexVcores are the vCore counts Azure sells for flexible servers.
	flexVcores = map[float64]bool{2: true, 4: true, 8: true, 16: true, 20: true, 32: true, 48: true, 64: true, 96: true}
)

// databaseStep: Azure SQL Database, PostgreSQL and MySQL flexible servers, and Redis.
func databaseStep(db *cost.DB, all map[string][]item, regions []string, vms map[string]cost.Size) error {
	// Azure SQL Database. A vCore size is priced per size; Hyperscale per vCore, so its sizes are
	// built from the General Purpose list of vCore counts.
	sql := newPicker(regions)
	counts := map[int]bool{}
	for _, it := range all["SQL Database"] {
		if m := sqlVcoreRE.FindStringSubmatch(it.SkuName); m != nil && it.ProductName == "SQL Database Single/Elastic Pool General Purpose - Compute Gen5" {
			n, _ := strconv.Atoi(m[1])
			counts[n] = true
		}
	}
	for _, it := range all["SQL Database"] {
		var tier string
		switch it.ProductName {
		case "SQL Database Single/Elastic Pool General Purpose - Compute Gen5":
			tier = "GP_Gen5_"
		case "SQL Database Single/Elastic Pool Business Critical - Compute Gen5":
			tier = "BC_Gen5_"
		case "SQL Database SingleDB/Elastic Pool Hyperscale - Compute Gen5":
			if it.SkuName == "1 vCore" {
				for n := range counts {
					sql.add("hour/HS_Gen5_"+strconv.Itoa(n), it, float64(n))
				}
			}
			continue
		case "SQL Database Single Basic":
			if it.SkuName == "B" {
				sql.add("hour/Basic", it, 1.0/24) // Azure prices these tiers per day
			}
			continue
		case "SQL Database Single Standard", "SQL Database Single Premium":
			if dtuRE.MatchString(it.SkuName) {
				sql.add("hour/"+it.SkuName, it, 1.0/24)
			}
			continue
		default:
			continue
		}
		if m := sqlVcoreRE.FindStringSubmatch(it.SkuName); m != nil {
			sql.add("hour/"+tier+m[1], it, 1)
		} else if m := sqlVcoreZRE.FindStringSubmatch(it.SkuName); m != nil {
			sql.add("zr/"+tier+m[1], it, 1) // what zone redundancy adds
		}
	}
	if err := sql.check("sql database"); err != nil {
		return err
	}
	sql.table(db, "azure/sqldb", "hour", regions, func(n string) cost.Size {
		var v float64
		if i := strings.LastIndex(n, "_"); i > 0 && strings.Contains(n, "Gen5") {
			v, _ = strconv.ParseFloat(n[i+1:], 64)
		}
		return cost.Size{VCPU: v, Family: strings.SplitN(n, "_", 2)[0]}
	})

	rest := newPicker(regions)
	for _, it := range all["SQL Database"] {
		switch {
		case it.ProductName == "SQL Database Single/Elastic Pool General Purpose - Storage" && it.MeterName == "General Purpose Data Stored":
			rest.addRate("azure/sqldb/storage/gp", "GB-month", it, 1)
		case it.ProductName == "SQL Database Single/Elastic Pool General Purpose - Storage" && it.MeterName == "General Purpose Zone Redundancy Data Stored":
			rest.addRate("azure/sqldb/storage/gp-zr", "GB-month", it, 1)
		case it.ProductName == "SQL Database Single/Elastic Pool Business Critical - Storage" && it.MeterName == "Business Critical Data Stored":
			rest.addRate("azure/sqldb/storage/bc", "GB-month", it, 1)
		case it.ProductName == "SQL Database SingleDB Hyperscale - Storage" && it.MeterName == "Hyperscale Data Stored":
			rest.addRate("azure/sqldb/storage/hs", "GB-month", it, 1)
		case it.ProductName == "SQL Database General Purpose - Serverless - Compute Gen5" && it.SkuName == "1 vCore" && it.MeterName == "vCore":
			rest.addRate("azure/sqldb/serverless", "vCore-hour", it, 1)
		}
	}

	vmNames := map[string]bool{}
	for _, it := range all["Virtual Machines"] {
		if strings.HasPrefix(it.ArmSkuName, "Standard_") {
			vmNames[it.ArmSkuName] = true
		}
	}
	// PostgreSQL and MySQL flexible servers run on VM sizes: a GP, MO or BC size is the VM size with the
	// price Azure lists for that vCore count and series; burstable sizes are priced per server.
	for _, f := range []struct{ cloudKey, service, product, storage string }{
		{"pgflex", "Azure Database for PostgreSQL", "Azure Database for PostgreSQL Flexible Server ", "Azure Database for PostgreSQL Flex Server Storage"},
		{"mysqlflex", "Azure Database for MySQL", "Azure Database for MySQL Flexible Server ", "Azure Database for MySQL Flexible Server Storage"},
	} {
		per := map[string][]item{} // "GP|Ddsv5|16" → the rows pricing that size
		fx := newPicker(regions)
		for _, it := range all[f.service] {
			if it.ProductName == f.storage && it.SkuName == "Storage" && it.MeterName == "Storage Data Stored" {
				rest.addRate("azure/"+f.cloudKey+"/storage", "GB-month", it, 1)
				continue
			}
			rest2 := strings.TrimPrefix(it.ProductName, f.product)
			if rest2 == it.ProductName {
				continue
			}
			var tier string
			switch {
			case strings.HasPrefix(rest2, "General Purpose "):
				tier = "GP"
			case strings.HasPrefix(rest2, "Memory Optimized "):
				tier = "MO"
			case strings.HasPrefix(rest2, "Business Critical "):
				tier = "BC"
			case strings.HasPrefix(rest2, "Burstable BS Series Compute"):
				if m := burstableRE.FindString(strings.ToLower(it.SkuName)); m != "" {
					fx.add("hour/B_Standard_B"+m[1:], it, 1) // "b2ms" → Standard_B2ms
				}
				continue
			default:
				continue
			}
			words := strings.Fields(strings.TrimSuffix(strings.TrimSuffix(rest2, " Compute"), " Series"))
			if len(words) == 0 || !flexTokenRE.MatchString(words[len(words)-1]) || it.MeterName != "vCore" && it.MeterName != "1 vCore" {
				continue
			}
			m := sqlVcoreRE.FindStringSubmatch(it.SkuName) // "16 vCore": the price of the whole size
			if m == nil {
				continue
			}
			k := tier + "|" + words[len(words)-1] + "|" + m[1]
			per[k] = append(per[k], it)
		}
		for name := range vmNames {
			sz := vmInfo(vms, name)
			m := vmSeriesRE.FindStringSubmatch(name)
			if m == nil || !flexVcores[sz.VCPU] {
				continue
			}
			token := m[1] + m[3] + m[4]
			for _, tier := range []string{"GP", "MO", "BC"} {
				k := tier + "|" + token + "|"
				exact := map[string]bool{}
				for _, it := range per[k+strconv.Itoa(int(sz.VCPU))] {
					fx.add("hour/"+tier+"_"+name, it, 1)
					exact[it.ArmRegionName] = true
				}
				// Where Azure lists no price for the whole size, it is the one-vCore price times the vCores.
				for _, it := range per[k+"1"] {
					if !exact[it.ArmRegionName] {
						fx.add("hour/"+tier+"_"+name, it, sz.VCPU)
					}
				}
			}
		}
		if err := fx.check(f.cloudKey); err != nil {
			return err
		}
		fx.table(db, "azure/"+f.cloudKey, "hour", regions, func(n string) cost.Size {
			if _, vm, ok := strings.Cut(n, "_"); ok {
				s := vmInfo(vms, vm)
				s.Old = false
				s.Family = strings.SplitN(n, "_", 2)[0] + " " + s.Family
				s.Old = false
				return s
			}
			return cost.Size{}
		})
	}

	// Redis: Basic, Standard and Premium caches by size (C0-C6, P1-P5).
	rd := newPicker(regions)
	for _, it := range all["Redis Cache"] {
		tier, ok := strings.CutPrefix(it.ProductName, "Azure Redis Cache ")
		if !ok || (tier != "Basic" && tier != "Standard" && tier != "Premium") || it.MeterName != it.SkuName+" Cache" {
			continue
		}
		rd.add(strings.ToLower(tier)+"/"+it.SkuName, it, 1)
	}
	if err := rd.check("redis"); err != nil {
		return err
	}
	rd.table(db, "azure/redis", "hour", regions, func(string) cost.Size { return cost.Size{} })
	if err := rest.check("databases"); err != nil {
		return err
	}
	rest.rates(db, regions)
	return nil
}

// dataStep: storage accounts (blob prices), Cosmos DB and Service Bus.
func dataStep(db *cost.DB, all map[string][]item, regions []string, _ map[string]cost.Size) error {
	pk := newPicker(regions)
	for _, it := range all["Storage"] {
		if it.ProductName != "General Block Blob v2" && it.ProductName != "Premium Block Blob" {
			continue
		}
		parts := strings.Fields(strings.ToLower(it.SkuName)) // "hot ra-grs", "premium lrs"
		if len(parts) != 2 || it.Unit == "" {
			continue
		}
		tier, red := parts[0], strings.ReplaceAll(parts[1], "-", "")
		if tier != "hot" && tier != "cool" && tier != "cold" && tier != "premium" { // the account's access tiers
			continue
		}
		key := "azure/storage/" + tier + "/" + red + "/"
		switch {
		case strings.HasSuffix(it.MeterName, "Data Stored"):
			pk.addRate(key+"capacity", "GB-month", it, 1)
		case strings.HasSuffix(it.MeterName, "Write Operations"):
			pk.addRate(key+"write", "10K operations", it, 1)
		case strings.HasSuffix(it.MeterName, "Read Operations"):
			pk.addRate(key+"read", "10K operations", it, 1)
		}
	}
	for _, it := range all["Azure Cosmos DB"] {
		switch {
		case it.ProductName == "Azure Cosmos DB" && it.SkuName == "RUs" && it.MeterName == "100 RU/s":
			pk.addRate("azure/cosmos/throughput", "100 RU/s-hour", it, 1)
		case it.ProductName == "Azure Cosmos DB" && it.SkuName == "mRUs" && it.MeterName == "100 Multi-master RU/s":
			pk.addRate("azure/cosmos/throughput-multiwrite", "100 RU/s-hour", it, 1)
		case it.ProductName == "Azure Cosmos DB" && it.SkuName == "RUs" && it.MeterName == "Data Stored":
			pk.addRate("azure/cosmos/storage", "GB-month", it, 1)
		case it.ProductName == "Azure Cosmos DB autoscale" && it.SkuName == "AP1" && it.MeterName == "AP1 100 RUs":
			pk.addRate("azure/cosmos/autoscale", "100 RU/s-hour", it, 1)
		case it.ProductName == "Azure Cosmos DB serverless" && it.MeterName == "1M RUs":
			pk.addRate("azure/cosmos/serverless", "1M RUs", it, 1)
		}
	}
	for _, it := range all["Service Bus"] {
		if it.ProductName != "Service Bus" {
			continue
		}
		switch {
		case it.MeterName == "Basic Messaging Operations":
			pk.addRate("azure/servicebus/basic/operations", "1M operations", it, 1)
		case it.MeterName == "Standard Base Unit" && it.Unit == "1/Month":
			pk.addRate("azure/servicebus/standard/base", "month", it, 1)
		case it.MeterName == "Standard Messaging Operations":
			pk.addRate("azure/servicebus/standard/operations", "1M operations", it, 1)
		case it.MeterName == "Premium Messaging Unit":
			pk.addRate("azure/servicebus/premium/unit", "hour", it, 1)
		}
	}
	if err := pk.check("storage, cosmos and service bus"); err != nil {
		return err
	}
	pk.rates(db, regions)
	return nil
}
