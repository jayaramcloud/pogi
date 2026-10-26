package databases

import (
	"strconv"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// The capabilities of a region list the tiers, SKUs, disk sizes and versions a new server can have
// (GET <Namespace>/locations/<region>/capabilities). The Azure CLI reads them before it creates a
// PostgreSQL or MySQL server: it takes the default SKU and version from them and checks its arguments
// against them. Before it updates a PostgreSQL server, it reads the server's own capabilities
// (.../flexibleServers/<name>/capabilities), which are the same. pogi runs every SKU the same way, as one
// container, so the lists are Azure's names only.

// sku is a server size: vCores, memory per vCore in MB, and the most IOPS it supports.
type sku struct {
	name                string
	vCores, memMB, iops int
}

type tier struct {
	name string
	skus []sku
}

// The SKUs of a tier all have a version suffix (_v5) or none: the Azure CLI's sort fails on a mix.
var pgTiers = []tier{
	{"Burstable", []sku{{"Standard_B1ms", 1, 2048, 640}, {"Standard_B2s", 2, 2048, 1280}, {"Standard_B2ms", 2, 4096, 1920}}},
	{"GeneralPurpose", []sku{{"Standard_D2s_v3", 2, 4096, 3200}, {"Standard_D4s_v3", 4, 4096, 6400},
		{"Standard_D2ds_v5", 2, 4096, 3750}, {"Standard_D4ds_v5", 4, 4096, 6400}}},
	{"MemoryOptimized", []sku{{"Standard_E2s_v3", 2, 8192, 3200}, {"Standard_E4s_v3", 4, 8192, 6400},
		{"Standard_E2ds_v5", 2, 8192, 3750}, {"Standard_E4ds_v5", 4, 8192, 6400}}},
}

var mysqlTiers = []tier{
	{"Burstable", []sku{{"Standard_B1s", 1, 1024, 400}, {"Standard_B1ms", 1, 2048, 640}, {"Standard_B2s", 2, 2048, 1280}, {"Standard_B2ms", 2, 4096, 1700}}},
	{"GeneralPurpose", []sku{{"Standard_D2ads_v5", 2, 4096, 3200}, {"Standard_D4ads_v5", 4, 4096, 6400}}},
	{"MemoryOptimized", []sku{{"Standard_E2ads_v5", 2, 8192, 3200}, {"Standard_E4ads_v5", 4, 8192, 6400}}},
}

// pgDisks are the PostgreSQL disk sizes in GiB, with the performance tier and IOPS each has by default.
var pgDisks = []struct {
	gb   int
	tier string
	iops int
}{{32, "P4", 120}, {64, "P6", 240}, {128, "P10", 500}, {256, "P15", 1100}, {512, "P20", 2300}, {1024, "P30", 5000},
	{2048, "P40", 7500}, {4096, "P50", 7500}, {8192, "P60", 16000}, {16384, "P70", 18000}, {32767, "P80", 20000}}

// Major versions: PostgreSQL's oldest first (the CLI's default is the last), and MySQL's as Azure names them.
// Both are versions pogi runs.
var (
	pgVersions    = []int{13, 14, 15, 16, 17, 18}
	mysqlVersions = []string{"8.0.21", "8.4"}
)

func registerCapabilities(hub *core.Hub) {
	for typ, doc := range map[string]func() map[string]any{
		"Microsoft.DBforPostgreSQL/locations/capabilities":       pgCapabilities,
		"Microsoft.DBforPostgreSQL/flexibleServers/capabilities": pgCapabilities,
		"Microsoft.DBforMySQL/locations/capabilities":            mysqlCapabilities,
	} {
		arm.Register(hub, arm.Provider{Type: typ, List: func(*core.Ctx, arm.ID) ([]map[string]any, error) {
			return []map[string]any{doc()}, nil
		}})
	}
}

func pgCapabilities() map[string]any {
	var disks []any
	for _, d := range pgDisks {
		disks = append(disks, map[string]any{"storageSizeMb": d.gb * 1024, "supportedIops": d.iops, "defaultIopsTier": d.tier,
			"supportedIopsTiers": []any{map[string]any{"name": d.tier, "iops": d.iops}}})
	}
	var editions []any
	for _, t := range pgTiers {
		var skus []any
		for _, s := range t.skus {
			skus = append(skus, map[string]any{"name": s.name, "vCores": s.vCores, "supportedIops": s.iops, "supportedMemoryPerVcoreMb": s.memMB,
				"supportedZones": []string{"1", "2", "3"}, "supportedHaMode": []string{}})
		}
		defaultMB := 131072
		if t.name == "Burstable" {
			defaultMB = 32768
		}
		editions = append(editions, map[string]any{"name": t.name, "defaultSkuName": t.skus[0].name, "supportedServerSkus": skus,
			"supportedStorageEditions": []any{map[string]any{"name": "ManagedDisk", "defaultStorageSizeMb": defaultMB, "supportedStorageMb": disks}}})
	}
	var versions []any
	for i, v := range pgVersions {
		up := []string{}
		for _, w := range pgVersions[i+1:] {
			up = append(up, strconv.Itoa(w))
		}
		versions = append(versions, map[string]any{"name": strconv.Itoa(v), "supportedVersionsToUpgrade": up})
	}
	// The CLI refuses a region whose OfferRestricted feature is missing. Without ZoneRedundantHa and GeoBackup,
	// it treats them as unsupported, which is true of pogi.
	return map[string]any{"name": "FlexibleServerCapabilities", "supportedServerEditions": editions, "supportedServerVersions": versions,
		"supportedFeatures": []any{map[string]any{"name": "OfferRestricted", "status": "Disabled"}}}
}

func mysqlCapabilities() map[string]any {
	var editions []any
	for _, t := range mysqlTiers {
		var skus []any
		for _, s := range t.skus {
			skus = append(skus, map[string]any{"name": s.name, "vCores": s.vCores, "supportedIops": s.iops, "supportedMemoryPerVCoreMB": s.memMB})
		}
		var versions []any
		for _, v := range mysqlVersions {
			versions = append(versions, map[string]any{"name": v, "supportedSkus": skus})
		}
		editions = append(editions, map[string]any{"name": t.name, "supportedServerVersions": versions,
			"supportedStorageEditions": []any{map[string]any{"name": "Premium", "minStorageSize": 20 * 1024, "maxStorageSize": 16 * 1024 * 1024,
				"minBackupRetentionDays": 1, "maxBackupRetentionDays": 35}}})
	}
	// No high availability mode and no geo-backup region: pogi has neither.
	return map[string]any{"zone": "none", "supportedHAMode": []string{}, "supportedGeoBackupRegions": []string{}, "supportedFlexibleServerEditions": editions}
}
