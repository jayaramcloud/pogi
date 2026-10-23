package cost

import (
	"math"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/terraform"
)

// azRow prices one Azure resource against the embedded price list.
func azRow(t *testing.T, typ, region string, attrs map[string]any, usage map[string]any) Row {
	t.Helper()
	res := &terraform.Resource{Address: typ + ".x", Type: typ, Cloud: "azure", Region: region, Attrs: attrs}
	e, err := Price(&terraform.Config{Resources: []*terraform.Resource{res}}, Options{Usage: map[string]map[string]any{res.Address: usage}})
	if err != nil {
		t.Fatal(err)
	}
	return e.Rows[0]
}

func azDB(t *testing.T) *DB {
	t.Helper()
	db, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// azRate is a rate's price in eastus.
func azRate(t *testing.T, key string) float64 {
	t.Helper()
	p, _, ok := azDB(t).Rate(key, "eastus")
	if !ok {
		t.Fatalf("no rate %s in eastus", key)
	}
	return p
}

func azHour(t *testing.T, col, size string) float64 {
	t.Helper()
	p, ok := azDB(t).Hourly(col, "eastus", size)
	if !ok {
		t.Fatalf("no price for %s in %s", size, col)
	}
	return p
}

func noteHas(r Row, s string) bool {
	for _, n := range r.Notes {
		if strings.Contains(n, s) {
			return true
		}
	}
	return false
}

func TestAzureVM(t *testing.T) {
	os := map[string]any{"storage_account_type": "Premium_LRS", "disk_size_gb": 128.0}
	r := azRow(t, "azurerm_linux_virtual_machine", "eastus", map[string]any{"size": "Standard_D2s_v5", "os_disk": []any{os}}, nil)
	near(t, "linux D2s_v5", r.Monthly, 730*azHour(t, "azure/vm/linux", "Standard_D2s_v5")+azRate(t, "azure/disk/premium/lrs/p10"))
	if r.Status != Known || r.Sizes == nil || r.Sizes.Column != "azure/vm/linux" || r.Sizes.Attr != "size" {
		t.Errorf("linux: %+v", r)
	}

	// Windows costs more, and the Azure Hybrid Benefit brings it back to the Linux price.
	w := azRow(t, "azurerm_windows_virtual_machine", "eastus", map[string]any{"size": "Standard_D2s_v5", "os_disk": []any{os}}, nil)
	near(t, "windows", w.Monthly-r.Monthly, 730*(azHour(t, "azure/vm/windows", "Standard_D2s_v5")-azHour(t, "azure/vm/linux", "Standard_D2s_v5")))
	h := azRow(t, "azurerm_windows_virtual_machine", "eastus", map[string]any{"size": "Standard_D2s_v5", "license_type": "Windows_Server", "os_disk": []any{os}}, nil)
	near(t, "hybrid", h.Monthly, r.Monthly)

	// No disk size: the image's default is assumed (30 GB is a P4 disk; Windows 127 GB is a P10).
	a := azRow(t, "azurerm_linux_virtual_machine", "eastus", map[string]any{"size": "Standard_D2s_v5", "os_disk": []any{map[string]any{"storage_account_type": "Premium_LRS"}}}, nil)
	near(t, "default disk", a.Monthly, 730*azHour(t, "azure/vm/linux", "Standard_D2s_v5")+azRate(t, "azure/disk/premium/lrs/p4"))
	if a.Status != Assumed {
		t.Errorf("default disk status %q", a.Status)
	}
	// A local (ephemeral) OS disk is free.
	e := azRow(t, "azurerm_linux_virtual_machine", "eastus", map[string]any{"size": "Standard_D2s_v5", "os_disk": []any{map[string]any{"storage_account_type": "Standard_LRS", "diff_disk_settings": []any{map[string]any{"option": "Local"}}}}}, nil)
	near(t, "ephemeral", e.Monthly, 730*azHour(t, "azure/vm/linux", "Standard_D2s_v5"))

	// Failures: no size, an unknown size, an unknown value, a region without the size, no OS disk.
	for name, attrs := range map[string]map[string]any{
		"no size":      {"os_disk": []any{os}},
		"unknown size": {"size": terraform.Unknown{}, "os_disk": []any{os}},
		"bad size":     {"size": "Standard_Nope", "os_disk": []any{os}},
		"no disk":      {"size": "Standard_D2s_v5"},
	} {
		if row := azRow(t, "azurerm_linux_virtual_machine", "eastus", attrs, nil); row.Status != Needs {
			t.Errorf("%s: status %q, notes %v", name, row.Status, row.Notes)
		}
	}
	if row := azRow(t, "azurerm_linux_virtual_machine", "nowhere", map[string]any{"size": "Standard_D2s_v5", "os_disk": []any{os}}, nil); row.Status != Needs || !noteHas(row, "nowhere") {
		t.Errorf("unknown region: %+v", row)
	}
}

func TestAzureManagedDisk(t *testing.T) {
	disk := func(typ string, gb any) Row {
		return azRow(t, "azurerm_managed_disk", "eastus", map[string]any{"storage_account_type": typ, "disk_size_gb": gb}, nil)
	}
	near(t, "100 GB premium is P10", disk("Premium_LRS", 100.0).Monthly, azRate(t, "azure/disk/premium/lrs/p10"))
	near(t, "129 GB premium is P15", disk("Premium_LRS", 129.0).Monthly, azRate(t, "azure/disk/premium/lrs/p15"))
	near(t, "zone redundant", disk("Premium_ZRS", 128.0).Monthly, azRate(t, "azure/disk/premium/zrs/p10"))
	near(t, "10 GB hdd is S4 (the smallest HDD)", disk("Standard_LRS", 10.0).Monthly, azRate(t, "azure/disk/standardhdd/lrs/s4"))
	if r := disk("StandardSSD_LRS", "128"); r.Status != Usage && r.Status != Known || math.Abs(r.Monthly-azRate(t, "azure/disk/standardssd/lrs/e10")) > 1e-9 {
		t.Errorf("standard ssd as a string size: %+v", r) // the operations charge is by use and adds nothing
	}
	for name, r := range map[string]Row{
		"too big":        disk("Premium_LRS", 40000.0),
		"zero":           disk("Premium_LRS", 0.0),
		"unknown size":   disk("Premium_LRS", terraform.Unknown{}),
		"unknown type":   disk("Gold_LRS", 10.0),
		"no size":        azRow(t, "azurerm_managed_disk", "eastus", map[string]any{"storage_account_type": "Premium_LRS"}, nil),
		"no type":        azRow(t, "azurerm_managed_disk", "eastus", map[string]any{"disk_size_gb": 10.0}, nil),
		"ultra, no iops": disk("UltraSSD_LRS", 100.0),
	} {
		if r.Status != Needs {
			t.Errorf("%s: status %q, notes %v", name, r.Status, r.Notes)
		}
	}
	// Premium SSD v2: 3,000 IOPS and 125 MBps are included.
	v2 := azRow(t, "azurerm_managed_disk", "eastus", map[string]any{"storage_account_type": "PremiumV2_LRS", "disk_size_gb": 100.0, "disk_iops_read_write": 5000.0, "disk_mbps_read_write": 200.0}, nil)
	near(t, "premium v2", v2.Monthly, 100*azRate(t, "azure/disk/premiumv2/storage")+2000*azRate(t, "azure/disk/premiumv2/iops")+75*azRate(t, "azure/disk/premiumv2/mbps"))
	u := azRow(t, "azurerm_managed_disk", "eastus", map[string]any{"storage_account_type": "UltraSSD_LRS", "disk_size_gb": 100.0, "disk_iops_read_write": 4000.0, "disk_mbps_read_write": 100.0}, nil)
	near(t, "ultra", u.Monthly, 100*azRate(t, "azure/disk/ultra/storage")+4000*azRate(t, "azure/disk/ultra/iops")+100*azRate(t, "azure/disk/ultra/mbps"))
}

func TestAzureNetwork(t *testing.T) {
	near(t, "standard public ip", azRow(t, "azurerm_public_ip", "eastus", map[string]any{"allocation_method": "Static"}, nil).Monthly, 730*azRate(t, "azure/pip/standard/static"))
	near(t, "basic dynamic ip", azRow(t, "azurerm_public_ip", "eastus", map[string]any{"allocation_method": "Dynamic", "sku": "Basic"}, nil).Monthly, 730*azRate(t, "azure/pip/basic/dynamic"))
	if r := azRow(t, "azurerm_public_ip", "eastus", map[string]any{}, nil); r.Status != Needs {
		t.Errorf("ip without allocation_method: %+v", r)
	}

	nat := azRow(t, "azurerm_nat_gateway", "eastus", nil, map[string]any{"monthly_gb_processed": 100.0})
	near(t, "nat", nat.Monthly, 730*azRate(t, "azure/natgw/hour")+100*azRate(t, "azure/natgw/data"))

	if r := azRow(t, "azurerm_lb", "eastus", map[string]any{}, nil); r.Monthly != 0 || r.Status != Known {
		t.Errorf("basic lb: %+v", r)
	}
	lb := azRow(t, "azurerm_lb", "eastus", map[string]any{"sku": "Standard"}, map[string]any{"monthly_gb_processed": 10.0})
	near(t, "standard lb", lb.Monthly, 730*azRate(t, "azure/lb/standard/hour")+10*azRate(t, "azure/lb/standard/data"))
	if lb.Status != Assumed {
		t.Errorf("standard lb assumes its rule count: %+v", lb)
	}
	if r := azRow(t, "azurerm_lb", "eastus", map[string]any{"sku": "Silver"}, nil); r.Status != Needs {
		t.Errorf("unknown lb sku: %+v", r)
	}

	gw := azRow(t, "azurerm_application_gateway", "eastus", map[string]any{"sku": []any{map[string]any{"name": "WAF_v2"}}}, map[string]any{"avg_capacity_units": 4.0})
	near(t, "waf v2", gw.Monthly, 730*azRate(t, "azure/appgw/waf_v2/fixed")+4*730*azRate(t, "azure/appgw/waf_v2/cu"))
	v1 := azRow(t, "azurerm_application_gateway", "eastus", map[string]any{"sku": []any{map[string]any{"name": "Standard_Medium", "capacity": 2.0}}}, nil)
	near(t, "v1 medium, 2 instances", v1.Monthly, 2*730*azRate(t, "azure/appgw/standard/medium"))
	for _, attrs := range []map[string]any{{}, {"sku": []any{map[string]any{"name": "Gold_v9"}}}, {"sku": []any{map[string]any{"name": "WAF_Small"}}}} {
		if r := azRow(t, "azurerm_application_gateway", "eastus", attrs, nil); r.Status != Needs {
			t.Errorf("bad gateway sku %v: %+v", attrs, r)
		}
	}
}

func TestAzureKubernetes(t *testing.T) {
	pool := map[string]any{"vm_size": "Standard_D4s_v5", "node_count": 3.0}
	free := azRow(t, "azurerm_kubernetes_cluster", "eastus", map[string]any{"default_node_pool": []any{pool}}, nil)
	disks := 3 * azRate(t, "azure/disk/premium/lrs/p10") // an assumed 128 GB OS disk per node
	near(t, "free tier", free.Monthly, 3*730*azHour(t, "azure/vm/linux", "Standard_D4s_v5")+disks)
	if free.Status != Assumed || free.Sizes == nil || free.Sizes.Attr != "default_node_pool.vm_size" {
		t.Errorf("free tier: %+v", free)
	}
	std := azRow(t, "azurerm_kubernetes_cluster", "eastus", map[string]any{"sku_tier": "Standard", "default_node_pool": []any{pool}}, nil)
	near(t, "standard tier adds the cluster fee", std.Monthly-free.Monthly, 730*azRate(t, "azure/aks/standard"))
	prem := azRow(t, "azurerm_kubernetes_cluster", "eastus", map[string]any{"sku_tier": "Premium", "default_node_pool": []any{pool}}, nil)
	near(t, "premium tier", prem.Monthly-free.Monthly, 730*azRate(t, "azure/aks/premium"))

	auto := map[string]any{"vm_size": "Standard_D4s_v5", "auto_scaling_enabled": true, "min_count": 2.0, "os_disk_size_gb": 64.0, "os_disk_type": "Ephemeral"}
	a := azRow(t, "azurerm_kubernetes_cluster", "eastus", map[string]any{"default_node_pool": []any{auto}}, nil)
	near(t, "autoscaling at the minimum, ephemeral disks", a.Monthly, 2*730*azHour(t, "azure/vm/linux", "Standard_D4s_v5"))
	if !noteHas(a, "minimum of 2 nodes") {
		t.Errorf("autoscale note: %v", a.Notes)
	}
	if r := azRow(t, "azurerm_kubernetes_cluster", "eastus", map[string]any{"sku_tier": "Gold", "default_node_pool": []any{pool}}, nil); r.Status != Needs {
		t.Errorf("unknown tier: %+v", r)
	}
	if r := azRow(t, "azurerm_kubernetes_cluster", "eastus", map[string]any{}, nil); r.Status != Needs {
		t.Errorf("no node pool: %+v", r)
	}

	np := azRow(t, "azurerm_kubernetes_cluster_node_pool", "eastus", map[string]any{"vm_size": "Standard_D4s_v5", "node_count": 2.0, "os_type": "Windows", "os_disk_size_gb": 128.0}, nil)
	near(t, "windows node pool", np.Monthly, 2*730*azHour(t, "azure/vm/windows", "Standard_D4s_v5")+2*azRate(t, "azure/disk/premium/lrs/p10"))
	if r := azRow(t, "azurerm_kubernetes_cluster_node_pool", "eastus", map[string]any{"vm_size": "Standard_D4s_v5", "os_type": "Plan9"}, nil); r.Status != Needs {
		t.Errorf("bad os_type: %+v", r)
	}
}

func TestAzureAppService(t *testing.T) {
	lin := azRow(t, "azurerm_service_plan", "eastus", map[string]any{"os_type": "Linux", "sku_name": "P1v3", "worker_count": 2.0}, nil)
	near(t, "P1v3 x2", lin.Monthly, 2*730*azHour(t, "azure/appservice/linux", "P1v3"))
	win := azRow(t, "azurerm_service_plan", "eastus", map[string]any{"os_type": "Windows", "sku_name": "P1v3"}, nil)
	near(t, "windows P1v3", win.Monthly, 730*azHour(t, "azure/appservice/windows", "P1v3"))
	for _, sku := range []string{"F1", "Y1"} {
		if r := azRow(t, "azurerm_service_plan", "eastus", map[string]any{"os_type": "Linux", "sku_name": sku}, nil); r.Monthly != 0 || r.Status != Known || len(r.Notes) == 0 {
			t.Errorf("%s: %+v", sku, r)
		}
	}
	ep := azRow(t, "azurerm_service_plan", "eastus", map[string]any{"os_type": "Linux", "sku_name": "EP2"}, nil)
	near(t, "EP2: 2 vCPU, 7 GB", ep.Monthly, 730*(2*azRate(t, "azure/functions/premium/vcpu")+7*azRate(t, "azure/functions/premium/memory")))
	for name, attrs := range map[string]map[string]any{
		"no sku":        {"os_type": "Linux"},
		"unknown sku":   {"os_type": "Linux", "sku_name": terraform.Unknown{}},
		"bad sku":       {"os_type": "Linux", "sku_name": "Z9"},
		"no os":         {"sku_name": "P1v3"},
		"win container": {"os_type": "WindowsContainer", "sku_name": "P1v3"},
	} {
		if r := azRow(t, "azurerm_service_plan", "eastus", attrs, nil); r.Status != Needs {
			t.Errorf("%s: %+v", name, r)
		}
	}

	fn := azRow(t, "azurerm_linux_function_app", "eastus", map[string]any{}, map[string]any{"monthly_executions": 5e6, "monthly_gb_seconds": 1e6})
	near(t, "function app", fn.Monthly, 5*azRate(t, "azure/functions/executions")+1e6*azRate(t, "azure/functions/gb-seconds"))
	if r := azRow(t, "azurerm_windows_function_app", "eastus", map[string]any{}, nil); r.Status != Usage {
		t.Errorf("function app without usage: %+v", r)
	}
}

func TestAzureSQLDatabase(t *testing.T) {
	gp := azRow(t, "azurerm_mssql_database", "eastus", map[string]any{"sku_name": "GP_Gen5_4", "max_size_gb": 100.0}, nil)
	near(t, "GP_Gen5_4", gp.Monthly, 730*azHour(t, "azure/sqldb/hour", "GP_Gen5_4")+100*azRate(t, "azure/sqldb/storage/gp"))
	zr := azRow(t, "azurerm_mssql_database", "eastus", map[string]any{"sku_name": "GP_Gen5_4", "max_size_gb": 100.0, "zone_redundant": true}, nil)
	near(t, "zone redundant", zr.Monthly-gp.Monthly, 730*azHour(t, "azure/sqldb/zr", "GP_Gen5_4")+100*(azRate(t, "azure/sqldb/storage/gp-zr")-azRate(t, "azure/sqldb/storage/gp")))
	dtu := azRow(t, "azurerm_mssql_database", "eastus", map[string]any{"sku_name": "S0"}, nil)
	near(t, "S0 is priced per day", dtu.Monthly, 730*azHour(t, "azure/sqldb/hour", "S0"))
	hs := azRow(t, "azurerm_mssql_database", "eastus", map[string]any{"sku_name": "HS_Gen5_2"}, map[string]any{"storage_gb": 50.0})
	near(t, "hyperscale", hs.Monthly, 730*azHour(t, "azure/sqldb/hour", "HS_Gen5_2")+50*azRate(t, "azure/sqldb/storage/hs"))
	sv := azRow(t, "azurerm_mssql_database", "eastus", map[string]any{"sku_name": "GP_S_Gen5_2", "max_size_gb": 10.0}, map[string]any{"monthly_vcore_hours": 100.0})
	near(t, "serverless", sv.Monthly, 100*azRate(t, "azure/sqldb/serverless")+10*azRate(t, "azure/sqldb/storage/gp"))
	def := azRow(t, "azurerm_mssql_database", "eastus", map[string]any{}, nil)
	if def.Status != Assumed || math.Abs(def.Monthly-(730*azHour(t, "azure/sqldb/hour", "GP_Gen5_2")+32*azRate(t, "azure/sqldb/storage/gp"))) > 1e-6 {
		t.Errorf("no sku_name: %+v", def)
	}
	for name, attrs := range map[string]map[string]any{
		"unknown sku":    {"sku_name": terraform.Unknown{}},
		"bad sku":        {"sku_name": "GP_Gen5_3"},
		"elastic pool":   {"sku_name": "ElasticPool"},
		"data warehouse": {"sku_name": "DW100c"},
	} {
		if r := azRow(t, "azurerm_mssql_database", "eastus", attrs, nil); r.Status != Needs {
			t.Errorf("%s: %+v", name, r)
		}
	}
	// Unknown max size: assumed, with a note.
	if r := azRow(t, "azurerm_mssql_database", "eastus", map[string]any{"sku_name": "GP_Gen5_2", "max_size_gb": terraform.Unknown{}}, nil); r.Status != Assumed {
		t.Errorf("unknown max size: %+v", r)
	}
}

func TestAzureFlexServers(t *testing.T) {
	pg := azRow(t, "azurerm_postgresql_flexible_server", "eastus", map[string]any{"sku_name": "GP_Standard_D4ds_v5", "storage_mb": 65536.0}, nil)
	near(t, "postgres", pg.Monthly, 730*azHour(t, "azure/pgflex/hour", "GP_Standard_D4ds_v5")+64*azRate(t, "azure/pgflex/storage"))
	if pg.Sizes == nil || pg.Sizes.Column != "azure/pgflex/hour" {
		t.Errorf("postgres sizes: %+v", pg.Sizes)
	}
	ha := azRow(t, "azurerm_postgresql_flexible_server", "eastus", map[string]any{"sku_name": "GP_Standard_D4ds_v5", "storage_mb": 65536.0, "high_availability": []any{map[string]any{"mode": "ZoneRedundant"}}}, nil)
	near(t, "high availability doubles", ha.Monthly, 2*pg.Monthly)
	def := azRow(t, "azurerm_postgresql_flexible_server", "eastus", map[string]any{"sku_name": "B_Standard_B1ms"}, nil)
	near(t, "default 32 GB storage, burstable", def.Monthly, 730*azHour(t, "azure/pgflex/hour", "B_Standard_B1ms")+32*azRate(t, "azure/pgflex/storage"))
	my := azRow(t, "azurerm_mysql_flexible_server", "eastus", map[string]any{"sku_name": "GP_Standard_D2ds_v5"}, nil)
	near(t, "mysql, 20 GB assumed", my.Monthly, 730*azHour(t, "azure/mysqlflex/hour", "GP_Standard_D2ds_v5")+20*azRate(t, "azure/mysqlflex/storage"))
	if my.Status != Assumed {
		t.Errorf("mysql default storage is an assumption: %+v", my)
	}
	for _, typ := range []string{"azurerm_postgresql_flexible_server", "azurerm_mysql_flexible_server"} {
		for _, attrs := range []map[string]any{{}, {"sku_name": "GP_Standard_D3ds_v9"}, {"sku_name": terraform.Unknown{}}} {
			if r := azRow(t, typ, "eastus", attrs, nil); r.Status != Needs {
				t.Errorf("%s %v: %+v", typ, attrs, r)
			}
		}
	}
}

func TestAzureRedis(t *testing.T) {
	std := azRow(t, "azurerm_redis_cache", "eastus", map[string]any{"sku_name": "Standard", "family": "C", "capacity": 1.0}, nil)
	near(t, "standard C1", std.Monthly, 730*azHour(t, "azure/redis/standard", "C1"))
	pre := azRow(t, "azurerm_redis_cache", "eastus", map[string]any{"sku_name": "Premium", "family": "P", "capacity": 2.0, "shard_count": 3.0}, nil)
	near(t, "premium P2, 3 shards", pre.Monthly, 3*730*azHour(t, "azure/redis/premium", "P2"))
	// The family can be left out: Premium is P, the others C.
	near(t, "family from the sku", azRow(t, "azurerm_redis_cache", "eastus", map[string]any{"sku_name": "Basic", "capacity": 0.0}, nil).Monthly, 730*azHour(t, "azure/redis/basic", "C0"))
	for _, attrs := range []map[string]any{{"capacity": 1.0}, {"sku_name": "Basic"}, {"sku_name": "Basic", "capacity": terraform.Unknown{}}, {"sku_name": "Basic", "capacity": 9.0}, {"sku_name": "Gold", "capacity": 1.0}} {
		if r := azRow(t, "azurerm_redis_cache", "eastus", attrs, nil); r.Status != Needs {
			t.Errorf("%v: %+v", attrs, r)
		}
	}
}

func TestAzureDataServices(t *testing.T) {
	cosmos := azRow(t, "azurerm_cosmosdb_account", "eastus", map[string]any{"geo_location": []any{map[string]any{}, map[string]any{}}}, map[string]any{"ru_per_second": 1000.0, "storage_gb": 50.0})
	near(t, "cosmos in 2 regions", cosmos.Monthly, 2*10*730*azRate(t, "azure/cosmos/throughput")+2*50*azRate(t, "azure/cosmos/storage"))
	mw := azRow(t, "azurerm_cosmosdb_account", "eastus", map[string]any{"multiple_write_locations_enabled": true}, map[string]any{"ru_per_second": 400.0})
	near(t, "multi-region writes", mw.Monthly, 4*730*azRate(t, "azure/cosmos/throughput-multiwrite"))
	sl := azRow(t, "azurerm_cosmosdb_account", "eastus", map[string]any{"capabilities": []any{map[string]any{"name": "EnableServerless"}}}, map[string]any{"monthly_request_units": 5e6})
	near(t, "serverless", sl.Monthly, 5*azRate(t, "azure/cosmos/serverless"))

	st := azRow(t, "azurerm_storage_account", "eastus", map[string]any{"account_tier": "Standard", "account_replication_type": "GRS"}, map[string]any{"storage_gb": 100.0, "monthly_write_operations": 1e6})
	near(t, "storage account", st.Monthly, 100*azRate(t, "azure/storage/hot/grs/capacity")+100*azRate(t, "azure/storage/hot/grs/write"))
	cool := azRow(t, "azurerm_storage_account", "eastus", map[string]any{"account_tier": "Standard", "account_replication_type": "LRS", "access_tier": "Cool"}, map[string]any{"storage_gb": 10.0})
	near(t, "cool", cool.Monthly, 10*azRate(t, "azure/storage/cool/lrs/capacity"))
	pre := azRow(t, "azurerm_storage_account", "eastus", map[string]any{"account_tier": "Premium", "account_replication_type": "LRS"}, map[string]any{"storage_gb": 10.0})
	near(t, "premium", pre.Monthly, 10*azRate(t, "azure/storage/premium/lrs/capacity"))
	for _, attrs := range []map[string]any{{}, {"account_tier": "Standard"}, {"account_tier": "Premium", "account_replication_type": "GRS"}} {
		if r := azRow(t, "azurerm_storage_account", "eastus", attrs, nil); r.Status != Needs {
			t.Errorf("%v: %+v", attrs, r)
		}
	}

	sb := func(sku string, attrs map[string]any, usage map[string]any) Row {
		attrs["sku"] = sku
		return azRow(t, "azurerm_servicebus_namespace", "eastus", attrs, usage)
	}
	near(t, "basic", sb("Basic", map[string]any{}, map[string]any{"monthly_operations": 3e6}).Monthly, 3*azRate(t, "azure/servicebus/basic/operations"))
	near(t, "standard", sb("Standard", map[string]any{}, map[string]any{"monthly_operations": 3e6}).Monthly, azRate(t, "azure/servicebus/standard/base")+3*azRate(t, "azure/servicebus/standard/operations"))
	near(t, "premium, 2 units", sb("Premium", map[string]any{"capacity": 2.0}, nil).Monthly, 2*730*azRate(t, "azure/servicebus/premium/unit"))
	for _, r := range []Row{sb("Gold", map[string]any{}, nil), azRow(t, "azurerm_servicebus_namespace", "eastus", map[string]any{}, nil)} {
		if r.Status != Needs {
			t.Errorf("bad service bus sku: %+v", r)
		}
	}
}

func TestAzureFreeAndSizes(t *testing.T) {
	for _, typ := range []string{"azurerm_resource_group", "azurerm_virtual_network", "azurerm_subnet", "azurerm_network_security_group", "azurerm_linux_web_app"} {
		if r := azRow(t, typ, "eastus", nil, nil); r.Status != Free {
			t.Errorf("%s: %+v", typ, r)
		}
	}
	if r := azRow(t, "azurerm_dns_zone", "eastus", nil, nil); r.Status != Unpriced {
		t.Errorf("a type pogi doesn't price: %+v", r)
	}
	for typ, os := range map[string]string{"azurerm_linux_virtual_machine": "linux", "azurerm_windows_virtual_machine": "windows", "azurerm_service_plan": "linux", "azurerm_mssql_database": ""} {
		col, ok := SizeColumn(typ, os)
		if !ok || len(azDB(t).Sizes(col, "eastus")) == 0 {
			t.Errorf("%s: no sizes in %q", typ, col)
		}
	}
}

// The data must cover the regions and sizes the brief asks for.
func TestAzureData(t *testing.T) {
	db := azDB(t)
	if n := len(db.Regions("azure")); n < 50 {
		t.Errorf("%d azure regions", n)
	}
	if got := db.Published["azure"]; got == "" || db.Source["azure"] == "" {
		t.Errorf("published %q, source %q", got, db.Source["azure"])
	}
	sz, ok := db.Size("azure/vm", "Standard_NC24ads_A100_v4")
	if !ok || sz.GPU != 1 || sz.GPUModel != "A100" || sz.VCPU != 24 {
		t.Errorf("GPU size: %+v", sz)
	}
	if arm, _ := db.Size("azure/vm", "Standard_D2ps_v5"); arm.Arch != "arm64" {
		t.Errorf("Arm size: %+v", arm)
	}
}
