package web

// Prices for the services the Cost page lists beyond S3, SQS, DynamoDB, Lambda, EC2 and RDS (those
// are in cost.go).
//
// AWS and Azure prices come from the embedded price list (internal/cost); nothing here types one in.
// Google prices are the one exception, in the marked block below.
//
// Not priced yet, because the price list has no key for them: AWS Step Functions and API Gateway.
// Their rows are listed as not priced.

import (
	"cmp"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/cost"
)

// staticPriced are the tables staticPrice handles: "service/Table".
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

	writeShare     = 0.25 // ponytail: a quarter of storage requests are writes/lists
	kbPerEvent     = 0.5  // ponytail: an average log event or message is this small
	sfnTransitions = 5    // ponytail: states a state machine run passes through, when its definition is not read
	cosmosRUPerReq = 5    // ponytail: request units an average Cosmos DB request costs
)

// ---- Google Cloud: typed-in prices (us-central1) ----------------------------------------------
// ponytail: move to gcp/... keys when the Google price list is generated (it waits for a gcloud login).
const (
	gcpStorageGB = 0.020 // Cloud Storage standard, regional
	gcpClassA    = 5.00 * perM
	gcpClassB    = 0.40 * perM
)

// staticIn is what staticPrice knows about one resource row.
type staticIn struct {
	share    float64 // the service's requests a month, divided among its priced rows
	perMonth float64 // multiplies a count since start to a month
	db       *cost.DB
	region   string // the AWS region; Azure rows are priced in East US
	table    string
	col      func(string) string
	inspect  func(table, id string) any
}

// rate is a unit price: AWS keys in the chosen region, Azure keys in East US.
func (in staticIn) rate(key string) float64 {
	region := in.region
	if strings.HasPrefix(key, "azure/") {
		region = "eastus"
	}
	p, _, _ := in.db.Rate(key, region)
	return p
}

// azureVM is the hourly Linux price of a VM size in East US; the size's case may differ from the list's.
func (in staticIn) azureVM(size string) (float64, bool) {
	const col = "azure/vm/linux"
	c := in.db.Columns[col]
	if c == nil {
		return 0, false
	}
	for _, s := range in.db.Tables[c.Table] {
		if strings.EqualFold(s.Name, size) {
			return in.db.Hourly(col, "eastus", s.Name)
		}
	}
	return 0, false
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
		return count(req) + " publishes", req * in.rate("aws/sns/request") * perM, true
	case "kinesis":
		if strings.Contains(in.col("Mode"), "ON_DEMAND") {
			return "on-demand · 730 h", in.rate("aws/kinesis/ondemand/stream-hour") * 730, true
		}
		return fmt.Sprintf("%.0f shards · 730 h", in.num("Shards")), in.num("Shards") * in.rate("aws/kinesis/shard-hour") * 730, true
	case "stepfunctions":
		runs := in.num("Executions") * in.perMonth
		if in.col("Type") == "EXPRESS" { // ponytail: Express duration isn't counted, only requests
			return count(runs) + " executions", runs * in.rate("aws/sfn/express/request") * perM, true
		}
		return fmt.Sprintf("%s executions × %d transitions", count(runs), sfnTransitions), runs * sfnTransitions * in.rate("aws/sfn/transition") / 1000, true
	case "apigateway":
		key := "aws/apigw/rest/request"
		if in.table == "HTTP APIs" {
			key = "aws/apigw/http/request"
		}
		return reqs, req * in.rate(key) * perM, true
	case "logs":
		gb := in.num("Events") * in.perMonth * kbPerEvent / (1 << 20)
		return fmt.Sprintf("%.2f GB ingested", gb), gb * in.rate("aws/cloudwatch/logs/ingest"), true
	case "secretsmanager":
		return "1 secret · " + reqs, in.rate("aws/secretsmanager/secret") + req*in.rate("aws/secretsmanager/request")*perM, true
	case "kms":
		if in.col("State") != "Enabled" || strings.HasPrefix(in.col("Alias"), "alias/aws/") {
			return "AWS managed or disabled key", 0, true
		}
		return "1 key · " + reqs, in.rate("aws/kms/key") + req*in.rate("aws/kms/request")*perM, true
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
		const k = "azure/storage/hot/lrs/" // hot, locally redundant; a write or read is billed per 10,000
		return fmt.Sprintf("%s stored · %s", in.col("Size"), reqs), gb*in.rate(k+"capacity") + req*(writeShare*in.rate(k+"write")+(1-writeShare)*in.rate(k+"read"))/1e4, true
	case "azfunctions":
		return count(req) + " executions", req * in.rate("azure/functions/executions") * perM, true
	case "servicebus":
		return "Standard · " + reqs, in.rate("azure/servicebus/standard/base") + req*in.rate("azure/servicebus/standard/operations")*perM, true
	case "cosmos":
		ru := req * cosmosRUPerReq
		return count(ru) + " RU (serverless)", ru * in.rate("azure/cosmos/serverless") * perM, true
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
		h, ok := in.azureVM(size)
		if !ok {
			return cmp.Or(size, "unknown size") + " has no price in East US", 0, false
		}
		return size + " · 730 h", h * 730, true
	}
	return "", 0, false
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
