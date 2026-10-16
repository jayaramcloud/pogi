package web

// Static prices for the services the embedded AWS price list does not carry.
//
// ponytail: public on-demand list prices as of 2026-10-12, no free tier, no discounts: AWS from
// aws.amazon.com/<service>/pricing (us-east-1, scaled by the region multiplier), Google Cloud from
// cloud.google.com/<service>/pricing (us-central1), Azure from azure.microsoft.com/pricing
// (East US, Linux, pay-as-you-go). Traffic assumptions are marked where they are made. Upgrade
// path: add a service to the price list generator instead of growing this table.

import (
	"cmp"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// staticPriced are the tables staticPrice prices: "service/Table".
var staticPriced = map[string]bool{
	"sns/Topics": true, "kinesis/Streams": true, "stepfunctions/State machines": true,
	"apigateway/REST APIs": true, "apigateway/HTTP APIs": true, "logs/Log groups": true,
	"secretsmanager/Secrets": true, "kms/Keys": true,
	"gcs/Buckets": true, "pubsub/Topics": true, "run/services": true, "cloudfunctions/functions": true,
	"firestore/Collections": true, "sqladmin/Instances": true,
	"azblob/Containers": true, "azfunctions/Function apps": true, "servicebus/Namespaces": true,
	"cosmos/Containers": true, "azcompute/Virtual machines": true,
}

const (
	perM = 1e-6 // a price quoted per million

	gcpStorageGB   = 0.020 // Cloud Storage standard, regional
	gcpClassA      = 5.00 * perM
	gcpClassB      = 0.40 * perM
	azBlobGB       = 0.0184 // hot LRS, first 50 TB
	azBlobWrite    = 5.50 * perM
	azBlobRead     = 0.44 * perM
	writeShare     = 0.25 // ponytail: a quarter of storage requests are writes/lists
	kbPerEvent     = 0.5  // ponytail: an average log event or message is this small
	sfnTransitions = 5    // ponytail: states a state machine run passes through, when its definition is not read
	cosmosRUPerReq = 5    // ponytail: request units an average Cosmos DB request costs
)

// staticIn is what staticPrice knows about one resource row.
type staticIn struct {
	share    float64 // the service's requests a month, divided among its priced rows
	perMonth float64 // multiplies a count since start to a month
	mult     float64 // region multiplier (AWS only)
	table    string
	col      func(string) string
	inspect  func(table, id string) any
}

func (in staticIn) num(c string) float64 {
	f, _ := strconv.ParseFloat(in.col(c), 64)
	return f
}

// staticPrice prices one row. It returns what drives the price and the dollars a month; ok is false
// when it cannot (detail then says why).
func staticPrice(svc string, in staticIn) (detail string, monthly float64, ok bool) {
	req := in.share
	reqs := count(req) + " requests"
	switch svc {
	case "sns":
		return count(req) + " publishes", req * 0.50 * perM * in.mult, true
	case "kinesis":
		if strings.Contains(in.col("Mode"), "ON_DEMAND") {
			return "on-demand · 730 h", 0.04 * 730 * in.mult, true
		}
		return fmt.Sprintf("%.0f shards · 730 h", in.num("Shards")), in.num("Shards") * 0.015 * 730 * in.mult, true
	case "stepfunctions":
		runs := in.num("Executions") * in.perMonth
		if in.col("Type") == "EXPRESS" {
			return count(runs) + " executions", runs * 1.00 * perM * in.mult, true
		}
		return fmt.Sprintf("%s executions × %d transitions", count(runs), sfnTransitions), runs * sfnTransitions * 0.025 / 1000 * in.mult, true
	case "apigateway":
		per := 3.50
		if in.table == "HTTP APIs" {
			per = 1.00
		}
		return reqs, req * per * perM * in.mult, true
	case "logs":
		gb := in.num("Events") * in.perMonth * kbPerEvent / (1 << 20)
		return fmt.Sprintf("%.2f GB ingested", gb), gb * 0.50 * in.mult, true
	case "secretsmanager":
		return "1 secret · " + reqs, (0.40 + req*0.05/1e4) * in.mult, true
	case "kms":
		if in.col("State") != "Enabled" || strings.HasPrefix(in.col("Alias"), "alias/aws/") {
			return "AWS managed or disabled key", 0, true
		}
		return "1 key · " + reqs, (1.00 + req*0.03/1e4) * in.mult, true
	case "gcs":
		gb := parseSize(in.col("Size")) / (1 << 30)
		return fmt.Sprintf("%s stored · %s", in.col("Size"), reqs), gb*gcpStorageGB + req*(writeShare*gcpClassA+(1-writeShare)*gcpClassB), true
	case "pubsub":
		gb := in.num("Messages") * in.perMonth * kbPerEvent / (1 << 20)
		return fmt.Sprintf("%.2f GB published", gb), gb * 0.04, true
	case "run", "cloudfunctions":
		return reqs, req * 0.40 * perM, true
	case "firestore":
		return reqs, req * (0.8*0.30 + 0.2*0.90) * perM, true // ponytail: 80% reads, 20% writes
	case "sqladmin":
		h, ok := cloudSQLHour(in.col("Tier"))
		if !ok {
			return in.col("Tier") + " is not in the price table", 0, false
		}
		return in.col("Tier") + " · 730 h", h * 730, true
	case "azblob":
		gb := parseSize(in.col("Size")) / (1 << 30)
		return fmt.Sprintf("%s stored · %s", in.col("Size"), reqs), gb*azBlobGB + req*(writeShare*azBlobWrite+(1-writeShare)*azBlobRead), true
	case "azfunctions":
		return count(req) + " executions", req * 0.20 * perM, true
	case "servicebus":
		return "Standard · " + reqs, 0.0135*730 + req*0.80*perM, true
	case "cosmos":
		ru := req * cosmosRUPerReq
		return count(ru) + " RU (serverless)", ru * 0.25 * perM, true
	case "azcompute":
		if in.col("State") != "running" {
			return "", 0, false
		}
		var vm struct {
			Properties struct{ HardwareProfile struct{ VMSize string } }
		}
		b, _ := json.Marshal(in.inspect(in.table, in.col("ID")))
		_ = json.Unmarshal(b, &vm)
		size := vm.Properties.HardwareProfile.VMSize
		h, ok := azureVMHour[strings.ToLower(size)]
		if !ok {
			return cmp.Or(size, "unknown size") + " is not in the price table", 0, false
		}
		return size + " · 730 h", h * 730, true
	}
	return "", 0, false
}

// azureVMHour is $/h, Linux, East US, pay-as-you-go.
var azureVMHour = map[string]float64{
	"standard_b1s": 0.0104, "standard_b1ms": 0.0207, "standard_b2s": 0.0416, "standard_b2ms": 0.0832,
	"standard_d2s_v3": 0.096, "standard_d4s_v3": 0.192, "standard_d2s_v5": 0.096, "standard_d4s_v5": 0.192,
	"standard_ds1_v2": 0.073, "standard_ds2_v2": 0.146, "standard_f2s_v2": 0.085, "standard_e2s_v3": 0.126,
}

// cloudSQLHour is the Enterprise edition MySQL/Postgres price: shared-core tiers are flat, the
// others are per vCPU plus per GB of memory ($0.0413 and $0.0070 an hour).
func cloudSQLHour(tier string) (float64, bool) {
	switch tier {
	case "db-f1-micro":
		return 0.0150, true
	case "db-g1-small":
		return 0.0500, true
	}
	var cpu, gb float64
	var err1, err2 error
	switch p := strings.Split(tier, "-"); {
	case len(p) == 4 && p[1] == "custom": // db-custom-2-7680
		cpu, err1 = strconv.ParseFloat(p[2], 64)
		var mb float64
		mb, err2 = strconv.ParseFloat(p[3], 64)
		gb = mb / 1024
	case len(p) == 4 && p[1] == "n1" && (p[2] == "standard" || p[2] == "highmem"): // db-n1-standard-2
		cpu, err1 = strconv.ParseFloat(p[3], 64)
		gb = cpu * map[string]float64{"standard": 3.75, "highmem": 6.5}[p[2]]
	default:
		return 0, false
	}
	if err1 != nil || err2 != nil || cpu <= 0 {
		return 0, false
	}
	return cpu*0.0413 + gb*0.0070, true
}

// rdsStorage is the $/GB-month of allocated RDS storage (Single-AZ MySQL and PostgreSQL, us-east-1).
func rdsStorage(kind string) float64 {
	switch kind {
	case "io1", "io2":
		return 0.125
	}
	return 0.115 // gp2, gp3 and anything unknown
}
