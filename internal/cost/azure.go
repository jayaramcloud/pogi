package cost

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/terraform"
)

// Azure prices. Every number comes from the embedded price list (scripts/prices/azure.go). The
// disk size ladders and plan sizes below are Azure's documented sizes, not prices.

func init() {
	vm := func(os string) mapping {
		return mapping{service: "Virtual Machines", sizes: "azure/vm/%s", price: func(r *res) { azVM(r, os) }}
	}
	mappings["azurerm_linux_virtual_machine"] = vm("linux")
	mappings["azurerm_windows_virtual_machine"] = vm("windows")
	mappings["azurerm_managed_disk"] = mapping{service: "Managed Disks", price: azManagedDisk}
	mappings["azurerm_public_ip"] = mapping{service: "Public IP", price: azPublicIP}
	mappings["azurerm_nat_gateway"] = mapping{service: "NAT Gateway", price: azNATGateway}
	mappings["azurerm_lb"] = mapping{service: "Load Balancer", price: azLoadBalancer}
	mappings["azurerm_application_gateway"] = mapping{service: "Application Gateway", price: azAppGateway}
	mappings["azurerm_kubernetes_cluster"] = mapping{service: "Kubernetes Service", sizes: "azure/vm/%s", price: azAKS}
	mappings["azurerm_kubernetes_cluster_node_pool"] = mapping{service: "Kubernetes Service", sizes: "azure/vm/%s", price: azNodePool}
	mappings["azurerm_service_plan"] = mapping{service: "App Service", sizes: "azure/appservice/%s", price: azServicePlan}
	mappings["azurerm_linux_function_app"] = mapping{service: "Functions", price: azFunctionApp}
	mappings["azurerm_windows_function_app"] = mapping{service: "Functions", price: azFunctionApp}
	mappings["azurerm_mssql_database"] = mapping{service: "SQL Database", sizes: "azure/sqldb/hour", price: azSQLDatabase}
	mappings["azurerm_postgresql_flexible_server"] = mapping{service: "PostgreSQL", sizes: "azure/pgflex/hour", price: azFlexServer("pgflex", "PostgreSQL", "storage_mb", 32768, 1024)}
	mappings["azurerm_mysql_flexible_server"] = mapping{service: "MySQL", sizes: "azure/mysqlflex/hour", price: azFlexServer("mysqlflex", "MySQL", "storage.size_gb", 20, 1)}
	mappings["azurerm_redis_cache"] = mapping{service: "Redis Cache", price: azRedis}
	mappings["azurerm_cosmosdb_account"] = mapping{service: "Cosmos DB", price: azCosmos}
	mappings["azurerm_storage_account"] = mapping{service: "Storage", price: azStorageAccount}
	mappings["azurerm_servicebus_namespace"] = mapping{service: "Service Bus", price: azServiceBus}

	// Free on their own. Anything billed by use (data transfer, a disk, a server's hours) is
	// priced on the resource that carries it.
	for _, t := range []string{
		"azurerm_resource_group", "azurerm_virtual_network", "azurerm_subnet", "azurerm_network_security_group",
		"azurerm_network_security_rule", "azurerm_subnet_network_security_group_association", "azurerm_route_table", "azurerm_route",
		"azurerm_subnet_route_table_association", "azurerm_network_interface", "azurerm_network_interface_security_group_association",
		"azurerm_network_interface_backend_address_pool_association", "azurerm_application_security_group",
		"azurerm_virtual_network_peering", "azurerm_subnet_nat_gateway_association", "azurerm_nat_gateway_public_ip_association",
		"azurerm_user_assigned_identity", "azurerm_role_assignment", "azurerm_role_definition", "azurerm_management_lock",
		"azurerm_virtual_machine_data_disk_attachment", "azurerm_lb_rule", "azurerm_lb_backend_address_pool", "azurerm_lb_probe",
		"azurerm_lb_nat_rule", "azurerm_lb_outbound_rule", "azurerm_storage_container", "azurerm_mssql_server", "azurerm_mssql_firewall_rule",
		"azurerm_postgresql_flexible_server_database", "azurerm_postgresql_flexible_server_configuration", "azurerm_postgresql_flexible_server_firewall_rule",
		"azurerm_mysql_flexible_database", "azurerm_mysql_flexible_server_configuration", "azurerm_mysql_flexible_server_firewall_rule",
		"azurerm_servicebus_queue", "azurerm_servicebus_topic", "azurerm_servicebus_subscription",
		"azurerm_linux_web_app", "azurerm_windows_web_app", "azurerm_network_watcher",
	} {
		free[t] = true
	}
}

// azTitle is an OS name for a line's label.
func azTitle(os string) string { return strings.ToUpper(os[:1]) + os[1:] }

// azNum returns a numeric attribute that has no default. The row needs it when it is missing or unknown.
func azNum(r *res, path string) (float64, bool) {
	v, ok := r.get(path)
	if !ok {
		r.need(path)
		return 0, false
	}
	if _, unknown := v.(terraform.Unknown); unknown {
		r.need(path)
		return 0, false
	}
	return r.num(path, 0), true
}

// azSize returns a size attribute in GB. When the files don't set it, or its value isn't known, it
// returns def and says so.
func azSize(r *res, path string, def float64, what string) float64 {
	if v, set := r.get(path); set {
		if _, unknown := v.(terraform.Unknown); !unknown {
			return r.num(path, def)
		}
	}
	r.assume(fmt.Sprintf("assumed %v GB %s (not set in the files)", def, what))
	return def
}

// azRequired returns a string attribute that has no default. The row needs it when it is missing or unknown.
func azRequired(r *res, path string) string {
	if _, ok := r.get(path); !ok {
		r.need(path)
		return ""
	}
	return r.str(path, "")
}

// azCount returns how many blocks a nested block attribute holds.
func azCount(r *res, path string) int {
	if v, ok := r.get(path); ok {
		if l, ok := v.([]any); ok {
			return len(l)
		}
		if _, unknown := v.(terraform.Unknown); !unknown {
			return 1
		}
	}
	return 0
}

// ---- disks ----

// Azure's disk sizes: each tier holds up to this many GiB. Standard HDD starts at S4 (32 GiB).
var (
	azDiskGiB  = []float64{4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32767}
	azDiskTier = []int{1, 2, 3, 4, 6, 10, 15, 20, 30, 40, 50, 60, 70, 80}
)

// azDiskType splits a storage_account_type: "Premium_ZRS" → premium, zrs.
func azDiskType(t string) (kind, red string, ok bool) {
	switch t {
	case "Standard_LRS":
		return "standardhdd", "lrs", true
	case "StandardSSD_LRS":
		return "standardssd", "lrs", true
	case "StandardSSD_ZRS":
		return "standardssd", "zrs", true
	case "Premium_LRS":
		return "premium", "lrs", true
	case "Premium_ZRS":
		return "premium", "zrs", true
	case "PremiumV2_LRS":
		return "premiumv2", "lrs", true
	case "UltraSSD_LRS":
		return "ultra", "lrs", true
	}
	return "", "", false
}

// azDisk prices a managed disk of a tier-priced kind (Premium SSD, Standard SSD, Standard HDD) by
// size: the month's disk, and its operations by use where Azure bills them.
func azDisk(r *res, accountType string, gb, n float64, what string) {
	kind, red, ok := azDiskType(accountType)
	if !ok || kind == "premiumv2" || kind == "ultra" {
		r.miss(fmt.Sprintf("no price for %s disk type %q", what, accountType))
		return
	}
	if gb <= 0 || gb > azDiskGiB[len(azDiskGiB)-1] {
		r.miss(fmt.Sprintf("%s: %v GB is not a size Azure sells", what, gb))
		return
	}
	i := 0
	for gb > azDiskGiB[i] || kind == "standardhdd" && azDiskTier[i] < 4 {
		i++
	}
	prefix := map[string]string{"premium": "p", "standardssd": "e", "standardhdd": "s"}[kind]
	name := strings.ToUpper(prefix) + strconv.Itoa(azDiskTier[i])
	r.rate("azure/disk/"+kind+"/"+red+"/"+prefix+strconv.Itoa(azDiskTier[i]), n, fmt.Sprintf("%s (%s %s, %v GB)", what, name, strings.ToUpper(red), gb))
	if kind != "premium" {
		r.usage("monthly_disk_operations", "azure/disk/"+kind+"/"+red+"/ops", 1e-4, what+" operations")
	}
}

func azManagedDisk(r *res) {
	t := azRequired(r, "storage_account_type")
	if t == "" {
		return
	}
	kind, _, ok := azDiskType(t)
	if !ok {
		r.miss("no price for disk type " + t)
		return
	}
	gb, ok := azNum(r, "disk_size_gb")
	if !ok {
		return
	}
	switch kind {
	case "premiumv2", "ultra":
		r.rate("azure/disk/"+kind+"/storage", gb, fmt.Sprintf("Disk capacity (%v GB)", gb))
		iops, mbps := 3000.0, 125.0 // what Premium SSD v2 includes, and the provider's defaults
		if kind == "ultra" {
			var ok1, ok2 bool
			iops, ok1 = azNum(r, "disk_iops_read_write")
			mbps, ok2 = azNum(r, "disk_mbps_read_write")
			if !ok1 || !ok2 {
				return
			}
		} else {
			iops, mbps = r.num("disk_iops_read_write", iops), r.num("disk_mbps_read_write", mbps)
			// ponytail: the first 3,000 IOPS and 125 MBps are free; the list shows them as a free tier.
			iops, mbps = max(iops-3000, 0), max(mbps-125, 0)
		}
		r.rate("azure/disk/"+kind+"/iops", iops, "Provisioned IOPS")
		r.rate("azure/disk/"+kind+"/mbps", mbps, "Provisioned throughput (MBps)")
	default:
		azDisk(r, t, gb, 1, "Disk")
	}
}

// ---- virtual machines ----

func azVM(r *res, os string) {
	hybrid := false
	if os == "windows" {
		// Azure Hybrid Benefit: the licence is the customer's, so the VM costs the Linux price.
		switch r.str("license_type", "") {
		case "Windows_Server", "Windows_Client":
			hybrid = true
		}
	}
	priceOS := os
	if hybrid {
		priceOS = "linux"
	}
	col := "azure/vm/" + priceOS
	size := r.size(col, "size", "")
	what := fmt.Sprintf("Virtual machine hours (%s, %s)", size, azTitle(os))
	if hybrid {
		what = fmt.Sprintf("Virtual machine hours (%s, Windows with Azure Hybrid Benefit)", size)
	}
	r.hourly(col, size, HoursPerMonth, what)
	if r.str("priority", "Regular") == "Spot" {
		r.notes = append(r.notes, "Spot VM priced at the on-demand list price")
	}
	azOSDisk(r, "os_disk", os)
}

// azOSDisk prices a VM's or node pool's operating system disk. An ephemeral disk lives on the host and is free.
func azOSDisk(r *res, block, os string) {
	if r.str(block+".diff_disk_settings.option", "") == "Local" {
		return
	}
	t := r.str(block+".storage_account_type", "")
	if _, set := r.get(block + ".storage_account_type"); !set {
		r.need(block + ".storage_account_type")
		return
	}
	def := 30.0 // the size of a standard Linux image's disk
	if os == "windows" {
		def = 127
	}
	gb := azSize(r, block+".disk_size_gb", def, "OS disk (the image's default size)")
	azDisk(r, t, gb, 1, "OS disk")
}

// ---- network ----

func azPublicIP(r *res) {
	sku := r.str("sku", "Standard")
	alloc := azRequired(r, "allocation_method")
	if alloc == "" {
		return
	}
	// A Standard address is always static.
	kind := strings.ToLower(alloc)
	if sku == "Standard" {
		kind = "static"
	}
	r.rate("azure/pip/"+strings.ToLower(sku)+"/"+kind, HoursPerMonth, fmt.Sprintf("Public IP hours (%s, %s)", sku, kind))
}

func azNATGateway(r *res) {
	r.rate("azure/natgw/hour", HoursPerMonth, "NAT gateway hours")
	r.usage("monthly_gb_processed", "azure/natgw/data", 1, "Data processed (GB)")
}

func azLoadBalancer(r *res) {
	sku := r.str("sku", "Basic")
	switch sku {
	case "Basic":
		r.notes = append(r.notes, "a Basic load balancer is free")
	case "Standard":
		tier := "standard"
		if r.str("sku_tier", "Regional") == "Global" {
			tier = "global"
		}
		r.rate("azure/lb/"+tier+"/hour", HoursPerMonth, "Load balancer hours (includes 5 rules)")
		r.assume("assumed 5 or fewer load-balancing rules (each extra rule is billed)")
		r.usage("monthly_gb_processed", "azure/lb/"+tier+"/data", 1, "Data processed (GB)")
	case "Gateway":
		r.rate("azure/lb/gateway/hour", HoursPerMonth, "Gateway load balancer hours")
		r.usage("monthly_gb_processed", "azure/lb/gateway/data", 1, "Data processed (GB)")
	default:
		r.miss("no price for load balancer sku " + sku)
	}
}

func azAppGateway(r *res) {
	name := azRequired(r, "sku.name")
	if name == "" {
		return
	}
	switch name {
	case "Standard_v2", "WAF_v2", "Basic":
		sku := map[string]string{"Standard_v2": "standard_v2", "WAF_v2": "waf_v2", "Basic": "basic_v2"}[name]
		r.rate("azure/appgw/"+sku+"/fixed", HoursPerMonth, "Gateway hours ("+name+")")
		r.usage("avg_capacity_units", "azure/appgw/"+sku+"/cu", HoursPerMonth, "Capacity units (average × hours)")
	default:
		kind, size, _ := strings.Cut(strings.ToLower(name), "_")
		if kind != "standard" && kind != "waf" || size == "" {
			r.miss("no price for application gateway sku " + name)
			return
		}
		n := r.num("sku.capacity", 1)
		r.rate("azure/appgw/"+kind+"/"+size, n*HoursPerMonth, fmt.Sprintf("Gateway hours (%s, %v instances)", name, n))
		if kind == "standard" {
			r.usage("monthly_gb_processed", "azure/appgw/standard/"+size+"/data", 1, "Data processed (GB)")
		}
	}
}

// ---- Kubernetes ----

func azAKS(r *res) {
	switch tier := r.str("sku_tier", "Free"); tier {
	case "Free":
		r.notes = append(r.notes, "the Free tier has no cluster fee")
	case "Standard":
		r.rate("azure/aks/standard", HoursPerMonth, "Cluster fee (Standard tier)")
	case "Premium":
		r.rate("azure/aks/premium", HoursPerMonth, "Cluster fee (Premium tier)")
	default:
		r.miss("no price for AKS tier " + tier)
	}
	azPool(r, "default_node_pool", "linux")
}

func azNodePool(r *res) {
	os := strings.ToLower(r.str("os_type", "Linux"))
	if os != "linux" && os != "windows" {
		r.miss("no price for node pool os_type " + os)
		return
	}
	azPool(r, "", os)
}

// azPool prices the nodes of a pool: VM hours and each node's OS disk. block is "default_node_pool"
// or "" for a separate pool resource.
func azPool(r *res, block, os string) {
	p := func(a string) string {
		if block == "" {
			return a
		}
		return block + "." + a
	}
	col := "azure/vm/" + os
	size := r.size(col, p("vm_size"), "")
	nodes := r.num(p("node_count"), 1)
	if r.flag(p("auto_scaling_enabled"), false) || r.flag(p("enable_auto_scaling"), false) {
		nodes = r.num(p("min_count"), 1)
		r.assume(fmt.Sprintf("priced at the minimum of %v nodes (autoscaling)", nodes))
	}
	r.hourly(col, size, nodes*HoursPerMonth, fmt.Sprintf("Node hours (%s, %v nodes, %s)", size, nodes, azTitle(os)))
	if r.str(p("priority"), "Regular") == "Spot" {
		r.notes = append(r.notes, "Spot nodes priced at the on-demand list price")
	}
	if r.str(p("os_disk_type"), "Managed") == "Ephemeral" {
		return
	}
	gb := azSize(r, p("os_disk_size_gb"), 128, "OS disk per node")
	azDisk(r, "Premium_LRS", gb, nodes, "OS disks")
}

// ---- App Service and Functions ----

// Premium Functions plans: vCPUs and GB of memory per instance.
var azPremiumPlans = map[string][2]float64{"EP1": {1, 3.5}, "EP2": {2, 7}, "EP3": {4, 14}}

func azServicePlan(r *res) {
	sku := azRequired(r, "sku_name")
	if sku == "" {
		return
	}
	os := strings.ToLower(r.str("os_type", ""))
	workers := r.num("worker_count", 1)
	switch {
	case sku == "F1":
		r.notes = append(r.notes, "F1 is the free plan")
		return
	case sku == "Y1":
		r.notes = append(r.notes, "the Consumption plan has no hourly fee; its use is priced on the function app")
		return
	case os == "windowscontainer":
		r.miss("no price for Windows container plans")
		return
	}
	if p, ok := azPremiumPlans[sku]; ok {
		r.rate("azure/functions/premium/vcpu", p[0]*workers*HoursPerMonth, fmt.Sprintf("vCPU hours (%s, %v instances)", sku, workers))
		r.rate("azure/functions/premium/memory", p[1]*workers*HoursPerMonth, fmt.Sprintf("Memory GB hours (%s, %v instances)", sku, workers))
		return
	}
	if os != "linux" && os != "windows" {
		if _, set := r.get("os_type"); !set {
			r.need("os_type")
		} else {
			r.miss("no price for plan os_type " + os)
		}
		return
	}
	col := "azure/appservice/" + os
	size := r.size(col, "sku_name", "")
	r.hourly(col, size, workers*HoursPerMonth, fmt.Sprintf("Plan hours (%s, %s, %v instances)", size, azTitle(os), workers))
}

func azFunctionApp(r *res) {
	r.notes = append(r.notes, "priced for the Consumption plan; on other plans the cost is on the service plan")
	r.usage("monthly_executions", "azure/functions/executions", 1e-6, "Executions (per million)")
	r.usage("monthly_gb_seconds", "azure/functions/gb-seconds", 1, "Execution time (GB-seconds)")
}

// ---- databases ----

func azSQLDatabase(r *res) {
	def := ""
	if _, set := r.get("sku_name"); !set {
		def = "GP_Gen5_2"
		r.assume("assumed sku_name GP_Gen5_2 (not set in the files)")
	}
	sku := r.str("sku_name", def)
	switch {
	case sku == "":
		r.need("sku_name")
		return
	case sku == "ElasticPool":
		r.miss("billed through its elastic pool, which pogi doesn't price yet")
		return
	case strings.HasPrefix(sku, "DW") || strings.HasPrefix(sku, "DS"):
		r.miss("no price for data warehouse sku " + sku)
		return
	}
	// A serverless database is billed for the vCores it uses.
	if strings.HasPrefix(sku, "GP_S_Gen5_") {
		r.usage("monthly_vcore_hours", "azure/sqldb/serverless", 1, "Compute (vCore hours used)")
		azSQLStorage(r, "gp", false)
		return
	}
	size := r.size("azure/sqldb/hour", "sku_name", def)
	r.hourly("azure/sqldb/hour", size, HoursPerMonth, "Compute hours ("+size+")")
	tier := strings.ToLower(strings.SplitN(size, "_", 2)[0])
	if r.flag("zone_redundant", false) {
		r.hourly("azure/sqldb/zr", size, HoursPerMonth, "Zone redundancy ("+size+")")
	}
	if r.str("license_type", "") == "BasePrice" {
		r.notes = append(r.notes, "list price includes the SQL licence; Azure Hybrid Benefit would lower it")
	}
	switch tier {
	case "gp", "bc":
		azSQLStorage(r, tier, r.flag("zone_redundant", false))
	case "hs":
		r.usage("storage_gb", "azure/sqldb/storage/hs", 1, "Storage (GB used)")
	default:
		r.notes = append(r.notes, "storage up to the tier's limit is included")
	}
}

func azSQLStorage(r *res, tier string, zr bool) {
	gb := azSize(r, "max_size_gb", 32, "maximum size")
	key := "azure/sqldb/storage/" + tier
	if zr && tier == "gp" {
		key += "-zr"
	}
	r.rate(key, gb, fmt.Sprintf("Storage (%v GB)", gb))
}

// azFlexServer prices a PostgreSQL or MySQL flexible server: compute, storage, and a standby copy
// of both when high availability is on.
func azFlexServer(table, name, storageAttr string, defStorage, perGB float64) func(*res) {
	return func(r *res) {
		col := "azure/" + table + "/hour"
		size := r.size(col, "sku_name", "")
		ha := 1.0
		if azCount(r, "high_availability") > 0 {
			ha = 2
			r.notes = append(r.notes, "high availability keeps a standby, so compute and storage are doubled")
		}
		r.hourly(col, size, ha*HoursPerMonth, fmt.Sprintf("%s compute hours (%s)", name, size))
		var gb float64
		if table == "mysqlflex" {
			gb = azSize(r, storageAttr, defStorage, "storage") // the provider documents no default
		} else {
			gb = r.num(storageAttr, defStorage) / perGB // storage_mb, default 32768
		}
		r.rate("azure/"+table+"/storage", ha*gb, fmt.Sprintf("Storage (%v GB)", gb))
	}
}

func azRedis(r *res) {
	sku := azRequired(r, "sku_name")
	capacity, ok := azNum(r, "capacity")
	if sku == "" || !ok {
		return
	}
	family := r.str("family", "")
	if family == "" {
		family = "C"
		if sku == "Premium" {
			family = "P"
		}
	}
	shards := 1.0
	if sku == "Premium" {
		shards = r.num("shard_count", 1)
	}
	size := family + strconv.Itoa(int(capacity))
	col := "azure/redis/" + strings.ToLower(sku)
	r.hourly(col, size, shards*HoursPerMonth, fmt.Sprintf("Cache hours (%s %s, %v shards)", sku, size, shards))
}

func azCosmos(r *res) {
	n := float64(max(azCount(r, "geo_location"), 1))
	serverless := false
	if v, ok := r.get("capabilities"); ok {
		if l, ok := v.([]any); ok {
			for _, c := range l {
				if m, ok := c.(map[string]any); ok && m["name"] == "EnableServerless" {
					serverless = true
				}
			}
		}
	}
	if serverless {
		r.usage("monthly_request_units", "azure/cosmos/serverless", 1e-6, "Request units (per million)")
	} else {
		rate := "azure/cosmos/throughput"
		if r.flag("multiple_write_locations_enabled", false) || r.flag("enable_multiple_write_locations", false) {
			rate = "azure/cosmos/throughput-multiwrite"
		}
		r.usage("ru_per_second", rate, n*HoursPerMonth/100, fmt.Sprintf("Provisioned throughput (RU/s × %v regions)", n))
		r.usage("autoscale_max_ru_per_second", "azure/cosmos/autoscale", n*HoursPerMonth/100, fmt.Sprintf("Autoscale throughput (max RU/s × %v regions)", n))
	}
	r.usage("storage_gb", "azure/cosmos/storage", n, fmt.Sprintf("Storage (GB × %v regions)", n))
}

// ---- storage and messaging ----

func azStorageAccount(r *res) {
	tier := strings.ToLower(azRequired(r, "account_tier"))
	red := strings.ToLower(azRequired(r, "account_replication_type"))
	if tier == "" || red == "" {
		return
	}
	if tier == "standard" {
		tier = strings.ToLower(r.str("access_tier", "Hot"))
	}
	key := "azure/storage/" + tier + "/" + red + "/"
	r.usage("storage_gb", key+"capacity", 1, fmt.Sprintf("Blob storage (GB, %s %s)", tier, strings.ToUpper(red)))
	r.usage("monthly_write_operations", key+"write", 1e-4, "Write operations (per 10,000)")
	r.usage("monthly_read_operations", key+"read", 1e-4, "Read operations (per 10,000)")
	r.notes = append(r.notes, "priced at blob prices; files, queues and tables are not included")
}

func azServiceBus(r *res) {
	switch sku := azRequired(r, "sku"); sku {
	case "":
	case "Basic":
		r.usage("monthly_operations", "azure/servicebus/basic/operations", 1e-6, "Operations (per million)")
	case "Standard":
		r.rate("azure/servicebus/standard/base", 1, "Base charge")
		r.usage("monthly_operations", "azure/servicebus/standard/operations", 1e-6, "Operations (per million)")
	case "Premium":
		units := r.num("capacity", 1)
		r.rate("azure/servicebus/premium/unit", units*HoursPerMonth, fmt.Sprintf("Messaging unit hours (%v units)", units))
	default:
		r.miss("no price for Service Bus sku " + sku)
	}
}
