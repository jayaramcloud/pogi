package main

// The AWS price list: the public Price List bulk files, one CSV per offer (service) and region.
// Only on-demand rows are read. A run downloads about 10 GB the first time (EC2 is 300 MB per
// region); -cache keeps it for the next run.

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/cost"
)

func init() { builders["aws"] = buildAWS }

const awsHost = "https://pricing.us-east-1.amazonaws.com"

// Commercial regions only: this leaves out us-gov-*, cn-*, and the local and wavelength zones
// (us-east-1-atl-1, us-east-1-wl1-bos1, ...).
var awsRegionRE = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-\d+$`)

// The usage types carry a region prefix (USE1-, APS1-, and EU- for Ireland), but not always, and
// not in every offer. Prefixes end in a digit, which keeps out qualifiers such as IA- and TS-.
const awsPfx = `(?:(?:[A-Z]{2,4}[0-9]|EU)-)?`

// awsRow is one on-demand line of a price file.
type awsRow struct {
	rec          []string
	col          map[string]int
	fam, ut, op  string
	unit         string
	start, price float64
}

func (r *awsRow) get(name string) string {
	if i, ok := r.col[name]; ok && i < len(r.rec) {
		return r.rec[i]
	}
	return ""
}

// awsRule picks one unit price (a "rate") out of an offer's rows.
type awsRule struct {
	key, unit string
	scale     float64 // multiplies the price; 1e6 turns a per-request price into "1M requests"
	fam, op   string  // when set, the row's product family and operation must be equal to these
	ut        *regexp.Regexp
	rank      func(*awsRow) int // when set: -1 rejects the row; a row with a higher rank wins over one with a lower rank
}

// rate builds a rule. ut is matched against the usage type, after an optional region prefix; a
// pattern that starts with ^ is used as it is.
func rate(key, unit string, scale float64, fam, op, ut string) awsRule {
	if !strings.HasPrefix(ut, "^") {
		ut = "^" + awsPfx + "(?:" + ut + ")$"
	}
	return awsRule{key: key, unit: unit, scale: scale, fam: fam, op: op, ut: regexp.MustCompile(ut)}
}

func (u awsRule) matches(r *awsRow) bool {
	return (u.fam == "" || u.fam == r.fam) && (u.op == "" || u.op == r.op || u.op == "-" && r.op == "") && u.ut.MatchString(r.ut) &&
		(u.rank == nil || u.rank(r) >= 0)
}

// awsRDSRates lists the RDS rates of each engine pogi prices. A price listed for "Any" engine is
// used unless the engine has its own, because a few prices differ by engine (by a tenth of a cent).
func awsRDSRates() []awsRule {
	type kind struct {
		rest, unit string
		scale      float64
		fam, ut    string
	}
	const store, iops = "Database Storage", "Provisioned IOPS"
	common := []kind{
		{"storage/gp2", "GB-month", 1, store, `RDS:GP2-Storage`},
		{"storage/gp3", "GB-month", 1, store, `RDS:GP3-Storage`},
		{"storage/io1", "GB-month", 1, store, `RDS:PIOPS-Storage`},
		{"storage/io2", "GB-month", 1, store, `RDS:PIOPS-Storage-IO2`},
		{"storage/standard", "GB-month", 1, store, `RDS:StorageUsage`},
		{"storage/gp2/multi-az", "GB-month", 1, store, `RDS:Multi-AZ-GP2-Storage`},
		{"storage/gp3/multi-az", "GB-month", 1, store, `RDS:Multi-AZ-GP3-Storage`},
		{"storage/io1/multi-az", "GB-month", 1, store, `RDS:Multi-AZ-PIOPS-Storage`},
		{"storage/io2/multi-az", "GB-month", 1, store, `RDS:Multi-AZ-PIOPS-Storage-IO2`},
		{"storage/standard/multi-az", "GB-month", 1, store, `RDS:Multi-AZ-StorageUsage`},
		{"iops/gp3", "IOPS-month", 1, iops, `RDS:GP3-PIOPS`},
		{"iops/io1", "IOPS-month", 1, iops, `RDS:PIOPS`},
		{"iops/io2", "IOPS-month", 1, iops, `RDS:IO2-PIOPS`},
		{"iops/gp3/multi-az", "IOPS-month", 1, iops, `RDS:Multi-AZ-GP3-PIOPS`},
		{"iops/io1/multi-az", "IOPS-month", 1, iops, `RDS:Multi-AZ-PIOPS`},
		{"iops/io2/multi-az", "IOPS-month", 1, iops, `RDS:Multi-AZ-IO2-PIOPS`},
		{"throughput/gp3", "MBps-month", 1, "Provisioned Throughput", `RDS:GP3-Throughput`},
		{"throughput/gp3/multi-az", "MBps-month", 1, "Provisioned Throughput", `RDS:Multi-AZ-GP3-Throughput`},
	}
	aurora := []kind{
		{"storage/standard", "GB-month", 1, store, `Aurora:StorageUsage`},
		{"storage/io-optimized", "GB-month", 1, store, `Aurora:IO-OptimizedStorageUsage`},
		{"io", "1M requests", 1e6, "System Operation", `Aurora:StorageIOUsage`},
		{"acu/standard", "ACU-hour", 1, "ServerlessV2", `Aurora:ServerlessV2Usage`},
		{"acu/io-optimized", "ACU-hour", 1, "ServerlessV2", `Aurora:ServerlessV2IOOptimizedUsage`},
	}
	var out []awsRule
	add := func(eng, name string, kinds []kind) {
		for _, k := range kinds {
			r := rate("aws/rds/"+eng+"/"+k.rest, k.unit, k.scale, k.fam, "", k.ut)
			r.rank = func(r *awsRow) int {
				d := r.get("Deployment Option")
				if d != "" && d != "Single-AZ" && d != "Multi-AZ" { // not a SQL Server mirror or a readable-standby cluster
					return -1
				}
				switch r.get("Database Engine") {
				case name:
					return 2
				case "Any":
					return 1
				}
				return -1
			}
			out = append(out, r)
		}
	}
	add("mysql", "MySQL", common)
	add("postgres", "PostgreSQL", common)
	add("mariadb", "MariaDB", common)
	add("aurora-mysql", "Aurora MySQL", aurora)
	add("aurora-postgresql", "Aurora PostgreSQL", aurora)
	return out
}

// awsSize is a row of a size table: one size at one price in one column.
type awsSize struct {
	table, col string
	size       cost.Size
	price      float64
}

// awsOffer is one service of the price list.
type awsOffer struct {
	code   string
	rates  []awsRule
	global []awsRule // read from the "aws-other" file; the price is the same in every region
	size   func(*awsRow) *awsSize
}

func awsOffers() []awsOffer {
	return []awsOffer{
		{code: "AmazonEC2", size: awsEC2Size, rates: []awsRule{
			rate("aws/ebs/gp2/storage", "GB-month", 1, "Storage", "", `EBS:VolumeUsage\.gp2`),
			rate("aws/ebs/gp3/storage", "GB-month", 1, "Storage", "", `EBS:VolumeUsage\.gp3`),
			rate("aws/ebs/io1/storage", "GB-month", 1, "Storage", "", `EBS:VolumeUsage\.piops`),
			rate("aws/ebs/io2/storage", "GB-month", 1, "Storage", "", `EBS:VolumeUsage\.io2`),
			rate("aws/ebs/st1/storage", "GB-month", 1, "Storage", "", `EBS:VolumeUsage\.st1`),
			rate("aws/ebs/sc1/storage", "GB-month", 1, "Storage", "", `EBS:VolumeUsage\.sc1`),
			rate("aws/ebs/standard/storage", "GB-month", 1, "Storage", "", `EBS:VolumeUsage`),
			rate("aws/ebs/gp3/iops", "IOPS-month", 1, "System Operation", "", `EBS:VolumeP-IOPS\.gp3`),
			rate("aws/ebs/io1/iops", "IOPS-month", 1, "System Operation", "", `EBS:VolumeP-IOPS\.piops`),
			rate("aws/ebs/io2/iops", "IOPS-month", 1, "System Operation", "", `EBS:VolumeP-IOPS\.io2`), // ponytail: the first tier (up to 32,000 IOPS); the mapping notes higher counts
			// The file prices gp3 throughput per GiBps-month; MBps is a thousandth of that.
			rate("aws/ebs/gp3/throughput", "MBps-month", 1.0/1024, "Provisioned Throughput", "", `EBS:VolumeP-Throughput\.gp3`),
			rate("aws/natgw/hour", "hour", 1, "NAT Gateway", "NatGateway", `NatGateway-Hours`),
			rate("aws/natgw/data", "GB", 1, "NAT Gateway", "NatGateway", `NatGateway-Bytes`),
		}},
		{code: "AmazonVPC", rates: []awsRule{
			rate("aws/ipv4/hour", "hour", 1, "", "", `PublicIPv4:InUseAddress`),
			rate("aws/vpce/hour", "hour", 1, "VpcEndpoint", "VpcEndpoint", `VpcEndpoint-Hours`),
			rate("aws/vpce/data", "GB", 1, "VpcEndpoint", "VpcEndpoint", `VpcEndpoint-Bytes`),
			rate("aws/vpce/gwlb/hour", "hour", 1, "VpcEndpoint", "VpcEndpoint", `VpcEndpoint-GWLBE-Hours`),
			rate("aws/vpce/gwlb/data", "GB", 1, "VpcEndpoint", "VpcEndpoint", `VpcEndpoint-GWLBE-Bytes`),
		}},
		{code: "AWSELB", rates: []awsRule{
			rate("aws/elb/application/hour", "hour", 1, "Load Balancer-Application", "LoadBalancing:Application", `LoadBalancerUsage`),
			rate("aws/elb/application/lcu", "LCU-hour", 1, "Load Balancer-Application", "LoadBalancing:Application", `LCUUsage`),
			rate("aws/elb/network/hour", "hour", 1, "Load Balancer-Network", "LoadBalancing:Network", `LoadBalancerUsage`),
			rate("aws/elb/network/lcu", "LCU-hour", 1, "Load Balancer-Network", "LoadBalancing:Network", `LCUUsage`),
			rate("aws/elb/gateway/hour", "hour", 1, "Load Balancer-Gateway", "LoadBalancing:Gateway", `LoadBalancerUsage`),
			rate("aws/elb/gateway/lcu", "LCU-hour", 1, "Load Balancer-Gateway", "LoadBalancing:Gateway", `LCUUsage`),
		}},
		{code: "AmazonRDS", size: awsRDSSize, rates: awsRDSRates()},
		{code: "AmazonElastiCache", size: awsCacheSize},
		{code: "AmazonEKS", rates: []awsRule{
			rate("aws/eks/cluster/hour", "hour", 1, "Compute", "CreateOperation", `AmazonEKS-Hours:perCluster`),
		}},
		{code: "AmazonECS", rates: []awsRule{
			rate("aws/fargate/linux/vcpu", "vCPU-hour", 1, "Compute", "", `Fargate-vCPU-Hours:perCPU`),
			rate("aws/fargate/linux/gb", "GB-hour", 1, "Compute", "", `Fargate-GB-Hours`),
			rate("aws/fargate/arm/vcpu", "vCPU-hour", 1, "Compute", "", `Fargate-ARM-vCPU-Hours:perCPU`),
			rate("aws/fargate/arm/gb", "GB-hour", 1, "Compute", "", `Fargate-ARM-GB-Hours`),
			rate("aws/fargate/windows/vcpu", "vCPU-hour", 1, "Compute", "", `Fargate-Windows-vCPU-Hours:perCPU`),
			rate("aws/fargate/windows/os", "vCPU-hour", 1, "Compute", "", `Fargate-Windows-OS-Hours:perCPU`),
			rate("aws/fargate/windows/gb", "GB-hour", 1, "Compute", "", `Fargate-Windows-GB-Hours`),
			rate("aws/fargate/ephemeral/gb", "GB-hour", 1, "Compute", "", `Fargate-EphemeralStorage-GB-Hours`),
		}},
		{code: "AWSLambda", rates: []awsRule{
			// ponytail: Lambda's compute price falls in tiers after 6 billion GB-seconds; the first tier is kept.
			rate("aws/lambda/request", "1M requests", 1e6, "Serverless", "", `Request`),
			rate("aws/lambda/gb-second", "GB-second", 1, "Serverless", "", `Lambda-GB-Second`),
			rate("aws/lambda/arm/request", "1M requests", 1e6, "Serverless", "", `Request-ARM`),
			rate("aws/lambda/arm/gb-second", "GB-second", 1, "Serverless", "", `Lambda-GB-Second-ARM`),
			rate("aws/lambda/ephemeral/gb-second", "GB-second", 1, "Serverless", "", `Lambda-Storage-GB-Second`),
		}},
		{code: "AmazonS3", rates: []awsRule{
			// ponytail: S3 storage falls in tiers after 50 TB; the first tier is kept. Only the Standard class is priced.
			rate("aws/s3/standard/storage", "GB-month", 1, "Storage", "-", `TimedStorage-ByteHrs`),
			rate("aws/s3/put", "1M requests", 1e6, "API Request", "", `Requests-Tier1`),
			rate("aws/s3/get", "1M requests", 1e6, "API Request", "", `Requests-Tier2`),
		}},
		{code: "AmazonDynamoDB", rates: []awsRule{
			// The price file lists the free tier (25 GB, 25 units) as a row at $0; the paid row is kept.
			rate("aws/dynamodb/rcu", "hour", 1, "Provisioned IOPS", "CommittedThroughput", `ReadCapacityUnit-Hrs`),
			rate("aws/dynamodb/wcu", "hour", 1, "Provisioned IOPS", "CommittedThroughput", `WriteCapacityUnit-Hrs`),
			rate("aws/dynamodb/read-request", "1M request units", 1e6, "", "PayPerRequestThroughput", `ReadRequestUnits`),
			rate("aws/dynamodb/write-request", "1M request units", 1e6, "", "PayPerRequestThroughput", `WriteRequestUnits`),
			rate("aws/dynamodb/storage", "GB-month", 1, "Database Storage", "", `TimedStorage-ByteHrs`),
			rate("aws/dynamodb/pitr", "GB-month", 1, "Database Storage", "", `TimedPITRStorage-ByteHrs`),
			rate("aws/dynamodb/ia/rcu", "hour", 1, "Provisioned IOPS", "CommittedThroughput", `IA-ReadCapacityUnit-Hrs`),
			rate("aws/dynamodb/ia/wcu", "hour", 1, "Provisioned IOPS", "CommittedThroughput", `IA-WriteCapacityUnit-Hrs`),
			rate("aws/dynamodb/ia/read-request", "1M request units", 1e6, "", "PayPerRequestThroughput", `IA-ReadRequestUnits`),
			rate("aws/dynamodb/ia/write-request", "1M request units", 1e6, "", "PayPerRequestThroughput", `IA-WriteRequestUnits`),
			rate("aws/dynamodb/ia/storage", "GB-month", 1, "Database Storage", "", `IA-TimedStorage-ByteHrs`),
		}},
		{code: "AWSQueueService", rates: []awsRule{
			// The usage types differ between regions (Requests-RBP, Requests-Tier1); both are standard queues.
			rate("aws/sqs/standard/request", "1M requests", 1e6, "API Request", "", `Requests-(?:RBP|Tier1)`),
			rate("aws/sqs/fifo/request", "1M requests", 1e6, "API Request", "", `Requests-FIFO-(?:RBP|Tier1)`),
		}},
		{code: "AmazonStates", rates: []awsRule{
			rate("aws/sfn/transition", "1K transitions", 1e3, "AWS Step Functions", "", `StateTransition`),
			rate("aws/sfn/express/request", "1M requests", 1e6, "AWS Step Functions", "", `StepFunctions-Request`),
			rate("aws/sfn/express/gb-second", "GB-second", 1, "AWS Step Functions", "", `StepFunctions-GB-Second`),
		}},
		{code: "AmazonApiGateway", rates: []awsRule{
			rate("aws/apigw/rest/request", "1M requests", 1e6, "API Calls", "ApiGatewayRequest", `ApiGatewayRequest`),
			rate("aws/apigw/http/request", "1M requests", 1e6, "API Calls", "ApiGatewayHttpApi", `ApiGatewayHttpRequest`),
			rate("aws/apigw/websocket/message", "1M messages", 1e6, "WebSocket", "ApiGatewayWebSocket", `ApiGatewayMessage`),
			rate("aws/apigw/websocket/minute", "1M connection-minutes", 1e6, "WebSocket", "ApiGatewayWebSocket", `ApiGatewayMinute`),
		}},
		{code: "AmazonSNS", rates: []awsRule{
			rate("aws/sns/request", "1M requests", 1e6, "API Request", "", `Requests-Tier1`),
			rate("aws/sns/http", "1M notifications", 1e6, "Message Delivery", "", `DeliveryAttempts-HTTP`),
			rate("aws/sns/email", "1M notifications", 1e6, "Message Delivery", "", `DeliveryAttempts-SMTP`),
			rate("aws/sns/mobile-push", "1M notifications", 1e6, "Message Delivery", "", `DeliveryAttempts-GCM`),
			rate("aws/sns/fifo/request", "1M requests", 1e6, "", "", `F-Request-Tier1`),
		}},
		{code: "AmazonKinesis", rates: []awsRule{
			rate("aws/kinesis/shard-hour", "hour", 1, "Kinesis Streams", "shardHourStorage", `Storage-ShardHour`),
			rate("aws/kinesis/extended-retention-shard-hour", "hour", 1, "Kinesis Streams", "shardHourStorage", `Extended-ShardHour`),
			rate("aws/kinesis/put-payload", "1M payload units", 1e6, "Kinesis Streams", "PutRequest", `PutRequestPayloadUnits`),
			rate("aws/kinesis/ondemand/stream-hour", "hour", 1, "Kinesis Streams", "OnDemandStreamHr", `OnDemand-StreamHour`),
			rate("aws/kinesis/ondemand/data-in", "GB", 1, "Kinesis Streams", "OnDemandDataIngested", `OnDemand-BilledIncomingBytes`),
			rate("aws/kinesis/ondemand/data-out", "GB", 1, "Kinesis Streams", "OnDemandDataRetrieval", `OnDemand-BilledOutgoingBytes`),
		}},
		{code: "AmazonCloudWatch", rates: []awsRule{
			rate("aws/cloudwatch/logs/ingest", "GB", 1, "Data Payload", "PutLogEvents", `DataProcessing-Bytes`),
			rate("aws/cloudwatch/logs/storage", "GB-month", 1, "Storage Snapshot", "", `TimedStorage-ByteHrs`),
			rate("aws/cloudwatch/metric", "metric-month", 1, "Metric", "", `CW:MetricMonitorUsage`), // ponytail: the first tier (10,000 metrics)
		}},
		{code: "AmazonCloudFront", global: []awsRule{
			// ponytail: CloudFront prices by where viewers are. The US edge prices are used; Europe is the same
			// for data and 20% higher for HTTPS requests. The mapping says so.
			rate("aws/cloudfront/data-out", "GB", 1, "Data Transfer", "", `^US-DataTransfer-Out-Bytes$`),
			rate("aws/cloudfront/http-requests", "1M requests", 1e6, "Request", "", `^US-Requests-Tier1$`),
			rate("aws/cloudfront/https-requests", "1M requests", 1e6, "Request", "", `^US-Requests-Tier2-HTTPS$`),
			rate("aws/cloudfront/invalidation", "path", 1, "Fee", "", `^Invalidations$`),
		}},
		{code: "AmazonES", size: awsSearchSize, rates: []awsRule{
			rate("aws/opensearch/storage/gp2", "GB-month", 1, "Amazon OpenSearch Service Volume", "", `ES:GP2-Storage`),
			rate("aws/opensearch/storage/gp3", "GB-month", 1, "Amazon OpenSearch Service Volume", "", `ES:GP3-Storage`),
			rate("aws/opensearch/storage/io1", "GB-month", 1, "Amazon OpenSearch Service Volume", "", `ES:PIOPS-Storage`),
			rate("aws/opensearch/storage/magnetic", "GB-month", 1, "Amazon OpenSearch Service Volume", "", `ES:Magnetic-Storage`),
			rate("aws/opensearch/iops/gp3", "IOPS-month", 1, "Amazon OpenSearch Service Volume", "", `ES:GP3-PIOPS`),
			rate("aws/opensearch/iops/io1", "IOPS-month", 1, "Amazon OpenSearch Service Volume", "", `ES:PIOPS`),
			rate("aws/opensearch/throughput/gp3", "MBps-month", 1, "Amazon OpenSearch Service Volume", "", `ES:GP3-Provisioned-ThroughPut`),
			rate("aws/opensearch/warm-storage", "GB-month", 1, "Amazon OpenSearch Service Volume", "", `ES:Managed-Storage`),
		}},
		{code: "AmazonMSK", size: awsMSKSize, rates: []awsRule{
			rate("aws/msk/storage", "GB-month", 1, "Managed Streaming for Apache Kafka (MSK)", "RunVolume", `Kafka\.Storage\.GP2`),
			rate("aws/msk/express-storage", "GB-month", 1, "Managed Streaming for Apache Kafka (MSK)", "Storage", `Express\.Storage`),
			rate("aws/msk/throughput", "MBps-month", 1, "Managed Streaming for Apache Kafka (MSK)", "RunVolume", `Kafka\.Throughput`),
		}},
		{code: "AWSSecretsManager", rates: []awsRule{
			rate("aws/secretsmanager/secret", "secret-month", 1, "Secret", "", `AWSSecretsManager-Secrets`),
			rate("aws/secretsmanager/request", "1M requests", 1e6, "API Request", "", `AWSSecretsManagerAPIRequest`),
		}},
		{code: "awskms", rates: []awsRule{
			// These usage types start with the region's name (eu-west-1-KMS-Keys), not a short prefix.
			rate("aws/kms/key", "key-month", 1, "Encryption Key", "", `^(?:[a-z0-9-]+-)?KMS-Keys$`),
			rate("aws/kms/request", "1M requests", 1e6, "API Request", "", `^(?:[a-z0-9-]+-)?KMS-Requests$`),
			rate("aws/kms/request/rsa2048", "1M requests", 1e6, "", "", `^(?:[a-z0-9-]+-)?KMS-Requests-Asymmetric-RSA_2048$`),
			rate("aws/kms/request/asymmetric", "1M requests", 1e6, "", "", `^(?:[a-z0-9-]+-)?KMS-Requests-Asymmetric$`),
		}},
		{code: "AmazonRoute53", global: []awsRule{
			// ponytail: the first 25 zones cost $0.50 each, then $0.10; the first tier is kept.
			rate("aws/route53/zone", "zone-month", 1, "DNS Zone", "", `^HostedZone$`),
			rate("aws/route53/query", "1M queries", 1e6, "DNS Query", "", `^DNS-Queries$`),
		}},
	}
}

var (
	awsNumUnit = regexp.MustCompile(`^\s*([0-9]+(?:\.[0-9]+)?)`)
	// Graviton (Arm) instance names carry a g after the generation: m6g, c7gn, r6gd, im4gn, x2gd.
	awsArmName = regexp.MustCompile(`(?:^|\.)[a-z]+[0-9]+g[a-z]*\.`)
	awsBoxUse  = regexp.MustCompile(`^` + awsPfx + `BoxUsage(?::|$)`)
	awsRDSUse  = regexp.MustCompile(`^` + awsPfx + `(InstanceUsage|Multi-AZUsage|InstanceUsageIOOptimized):(db\.[a-z0-9.\-]+)$`)
	awsNodeUse = regexp.MustCompile(`^` + awsPfx + `NodeUsage:(cache\.[a-z0-9.\-]+)$`)
	awsESUse   = regexp.MustCompile(`^` + awsPfx + `ESInstance:`)
	awsKafka   = regexp.MustCompile(`^` + awsPfx + `((?:Kafka|Express)\.[a-z0-9]+\.[a-z0-9]+)$`)
)

// awsNum reads the number at the start of "16 GiB" or "2".
func awsNum(s string) float64 {
	m := awsNumUnit.FindStringSubmatch(s)
	if m == nil {
		return 0
	}
	f, _ := strconv.ParseFloat(m[1], 64)
	return f
}

// awsFamily is the series of a size: m5.large, db.m5.large and r6g.large.search are m5, m5 and r6g.
func awsFamily(name string) string {
	parts := strings.Split(name, ".")
	for _, p := range parts {
		if p != "db" && p != "cache" && p != "kafka" && p != "express" {
			return p
		}
	}
	return name
}

// awsGPUModel names the GPU of the families that have one. The price list gives the count but not
// the model, so this list is short and only holds the models I'm sure of.
// ponytail: a new GPU family shows its count and no model until it is added here.
var awsGPUModel = map[string]string{
	"g2": "NVIDIA GRID K520", "g3": "NVIDIA Tesla M60", "g3s": "NVIDIA Tesla M60", "g4ad": "AMD Radeon Pro V520",
	"g4dn": "NVIDIA T4", "g5": "NVIDIA A10G", "g5g": "NVIDIA T4G", "g6": "NVIDIA L4", "g6e": "NVIDIA L40S",
	"p2": "NVIDIA K80", "p3": "NVIDIA V100", "p3dn": "NVIDIA V100", "p4d": "NVIDIA A100", "p4de": "NVIDIA A100",
	"p5": "NVIDIA H100", "p5en": "NVIDIA H200", "p6-b200": "NVIDIA B200", "p6-b300": "NVIDIA B300",
}

func awsEC2Size(r *awsRow) *awsSize {
	if !strings.HasPrefix(r.fam, "Compute Instance") || r.get("Tenancy") != "Shared" || r.get("CapacityStatus") != "Used" ||
		r.get("Pre Installed S/W") != "NA" || !awsBoxUse.MatchString(r.ut) {
		return nil
	}
	var col string
	switch {
	case r.op == "RunInstances":
		col = "aws/ec2/linux"
	case r.op == "RunInstances:0002" && r.get("License Model") == "No License required":
		col = "aws/ec2/windows"
	default:
		return nil
	}
	name := r.get("Instance Type")
	if name == "" || r.price <= 0 {
		return nil
	}
	s := cost.Size{Name: name, Family: awsFamily(name), VCPU: awsNum(r.get("vCPU")), MemGB: awsNum(r.get("Memory")),
		Old: r.get("Current Generation") == "No"}
	if strings.Contains(r.get("Physical Processor"), "Graviton") || awsArmName.MatchString(name) {
		s.Arch = "arm64"
	}
	if g := awsNum(r.get("GPU")); g >= 1 { // g6f and gr6f sell fractions of a GPU; those show none
		s.GPU = int(g)
		s.GPUModel = awsGPUModel[s.Family]
	}
	return &awsSize{"aws/ec2", col, s, r.price}
}

var awsRDSEngine = map[string]string{
	"MySQL": "mysql", "PostgreSQL": "postgres", "MariaDB": "mariadb", "Aurora MySQL": "aurora-mysql", "Aurora PostgreSQL": "aurora-postgresql",
}

func awsRDSSize(r *awsRow) *awsSize {
	m := awsRDSUse.FindStringSubmatch(r.ut)
	eng := awsRDSEngine[r.get("Database Engine")]
	if m == nil || eng == "" || r.fam != "Database Instance" || r.get("License Model") != "No license required" || r.price <= 0 {
		return nil
	}
	var variant string
	switch dep := r.get("Deployment Option"); {
	case strings.HasPrefix(eng, "aurora") && m[1] == "InstanceUsage" && dep == "Single-AZ":
		variant = "standard"
	case strings.HasPrefix(eng, "aurora") && m[1] == "InstanceUsageIOOptimized" && dep == "Single-AZ":
		variant = "io-optimized"
	case m[1] == "InstanceUsage" && dep == "Single-AZ":
		variant = "single-az"
	case m[1] == "Multi-AZUsage" && dep == "Multi-AZ":
		variant = "multi-az"
	default:
		return nil
	}
	s := cost.Size{Name: m[2], Family: awsFamily(m[2]), VCPU: awsNum(r.get("vCPU")), MemGB: awsNum(r.get("Memory")),
		Old: r.get("Current Generation") == "No"}
	if strings.Contains(r.get("Physical Processor"), "Graviton") || awsArmName.MatchString(m[2]) {
		s.Arch = "arm64"
	}
	return &awsSize{"aws/rds", "aws/rds/" + eng + "/" + variant, s, r.price}
}

func awsCacheSize(r *awsRow) *awsSize {
	m := awsNodeUse.FindStringSubmatch(r.ut)
	if m == nil || r.fam != "Cache Instance" || r.price <= 0 {
		return nil
	}
	eng := strings.ToLower(r.get("Cache Engine"))
	if eng != "redis" && eng != "valkey" && eng != "memcached" {
		return nil
	}
	s := cost.Size{Name: m[1], Family: awsFamily(m[1]), VCPU: awsNum(r.get("vCPU")), MemGB: awsNum(r.get("Memory")),
		Old: r.get("Current Generation") == "No"}
	if awsArmName.MatchString(m[1]) {
		s.Arch = "arm64"
	}
	return &awsSize{"aws/elasticache", "aws/elasticache/" + eng, s, r.price}
}

func awsSearchSize(r *awsRow) *awsSize {
	name := r.get("Instance Type")
	if r.fam != "Amazon OpenSearch Service Instance" || name == "" || r.unit != "Hrs" || r.price <= 0 || !awsESUse.MatchString(r.ut) {
		return nil
	}
	s := cost.Size{Name: name, Family: awsFamily(name), VCPU: awsNum(r.get("vCPU")), MemGB: awsNum(r.get("Memory (GiB)")),
		Old: r.get("Current Generation") == "No"}
	if awsArmName.MatchString(name) {
		s.Arch = "arm64"
	}
	return &awsSize{"aws/opensearch", "aws/opensearch/instance", s, r.price}
}

func awsMSKSize(r *awsRow) *awsSize {
	m := awsKafka.FindStringSubmatch(r.ut)
	if m == nil || r.op != "RunBroker" || r.price <= 0 {
		return nil
	}
	name := strings.ToLower(m[1])
	s := cost.Size{Name: name, Family: awsFamily(name), VCPU: awsNum(r.get("vCPU")), MemGB: awsNum(r.get("Memory (GiB)"))}
	if awsArmName.MatchString(name) {
		s.Arch = "arm64"
	}
	return &awsSize{"aws/msk", "aws/msk/broker", s, r.price}
}

// awsGen collects what the files say, from several goroutines.
type awsGen struct {
	mu      sync.Mutex
	rates   map[string]map[string]float64 // rate key → region → price
	units   map[string]string
	global  map[string]float64 // rate key → price, from the global files
	sizes   map[string]map[string]cost.Size
	colTbl  map[string]string                        // column → table
	cols    map[string]map[string]map[string]float64 // column → region → size name → price
	regions map[string]bool                          // regions of the EC2 offer: the full public list
}

func buildAWS(ctx context.Context, o opts) (*cost.DB, error) {
	g := &awsGen{rates: map[string]map[string]float64{}, units: map[string]string{}, global: map[string]float64{},
		sizes: map[string]map[string]cost.Size{}, colTbl: map[string]string{}, cols: map[string]map[string]map[string]float64{}, regions: map[string]bool{}}
	offers := awsOffers()

	type job struct {
		offer  *awsOffer
		region string // "aws-other" for the global file
		url    string
	}
	var jobs []job
	for i := range offers {
		of := &offers[i]
		var idx struct {
			Regions map[string]struct {
				URL string `json:"currentVersionUrl"`
			} `json:"regions"`
		}
		p, err := awsFetch(ctx, o, awsHost+"/offers/v1.0/aws/"+of.code+"/current/region_index.json", "aws/"+of.code+"/region_index.json")
		if err != nil {
			return nil, err
		}
		b, err := os.ReadFile(p)
		if err != nil {
			return nil, err
		}
		if err := json.Unmarshal(b, &idx); err != nil {
			return nil, fmt.Errorf("%s region index: %w", of.code, err)
		}
		for region, v := range idx.Regions {
			isGlobal := region == "aws-other" && len(of.global) > 0
			if !isGlobal && (!awsRegionRE.MatchString(region) || strings.HasPrefix(region, "us-gov-") || strings.HasPrefix(region, "cn-")) {
				continue
			}
			if !isGlobal && (len(of.rates) == 0 && of.size == nil || !o.wanted(region)) {
				continue
			}
			if !strings.HasSuffix(v.URL, "index.json") {
				return nil, fmt.Errorf("%s %s: unexpected url %q", of.code, region, v.URL)
			}
			jobs = append(jobs, job{of, region, awsHost + strings.TrimSuffix(v.URL, "json") + "csv"})
		}
		if len(of.global) > 0 {
			if _, ok := idx.Regions["aws-other"]; !ok {
				return nil, fmt.Errorf("%s: no aws-other file for its global prices", of.code)
			}
		}
	}
	// The biggest files first, so the long downloads overlap with the short ones.
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].offer.code == "AmazonEC2" && jobs[j].offer.code != "AmazonEC2" })

	var (
		wg   sync.WaitGroup
		sem  = make(chan struct{}, 4) // about 4 downloads at a time
		errs = make(chan error, len(jobs))
	)
	for _, j := range jobs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			p, err := awsFetch(ctx, o, j.url, "aws/"+j.offer.code+"/"+j.region+".csv")
			if err == nil {
				err = g.scan(j.offer, j.region, p)
			}
			if err != nil {
				errs <- fmt.Errorf("%s %s: %w", j.offer.code, j.region, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return nil, err
	}
	return g.build(offers)
}

// awsFetch downloads one file, trying 3 times.
func awsFetch(ctx context.Context, o opts, url, name string) (string, error) {
	var err error
	for i := 0; i < 3; i++ {
		var p string
		if p, err = fetch(ctx, o, url, name, nil); err == nil {
			return p, nil
		}
		if ctx.Err() != nil {
			break
		}
		time.Sleep(time.Duration(i+1) * 2 * time.Second)
	}
	return "", err
}

// awsCand is the best price found so far for a rule in a file.
type awsCand struct {
	rank         int
	start, price float64
	desc         string
}

// scan reads one file and adds what the offer's rules and size function pick to g.
func (g *awsGen) scan(of *awsOffer, region, path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	cr := csv.NewReader(bufio.NewReaderSize(f, 1<<20))
	cr.FieldsPerRecord = -1
	cr.ReuseRecord = true
	for i := 0; i < 5; i++ { // five lines of notes (format, disclaimer, date, version, offer)
		if _, err := cr.Read(); err != nil {
			return fmt.Errorf("reading the header lines: %w", err)
		}
	}
	hdr, err := cr.Read()
	if err != nil {
		return fmt.Errorf("reading the column names: %w", err)
	}
	col := make(map[string]int, len(hdr))
	for i, h := range hdr {
		col[h] = i
	}
	for _, need := range []string{"TermType", "StartingRange", "Unit", "PricePerUnit", "Product Family", "usageType", "operation"} {
		if _, ok := col[need]; !ok {
			return fmt.Errorf("the file has no %q column", need)
		}
	}
	rules := of.rates
	if region == "aws-other" {
		rules = of.global
	}
	best := map[string]awsCand{}
	type cell struct{ table, col, size string }
	sizes := map[cell]awsSize{}
	row := awsRow{col: col}
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if len(rec) <= col["PricePerUnit"] || rec[col["TermType"]] != "OnDemand" {
			continue
		}
		row.rec = rec
		row.fam, row.ut, row.op, row.unit = rec[col["Product Family"]], rec[col["usageType"]], rec[col["operation"]], rec[col["Unit"]]
		if row.price, err = strconv.ParseFloat(rec[col["PricePerUnit"]], 64); err != nil {
			return fmt.Errorf("price %q of %s: %w", rec[col["PricePerUnit"]], row.ut, err)
		}
		if row.start, err = strconv.ParseFloat(rec[col["StartingRange"]], 64); err != nil {
			row.start = 0 // a few lines have no range
		}
		for i := range rules {
			if r := &rules[i]; row.price > 0 && r.matches(&row) {
				// Keep the first paid tier: the lowest start with a price above zero. (The free tiers are at $0.)
				rank := 0
				if r.rank != nil {
					rank = r.rank(&row)
				}
				price := awsRound(row.price * r.scale)
				c, ok := best[r.key]
				switch {
				case !ok || rank > c.rank || rank == c.rank && row.start < c.start:
					best[r.key] = awsCand{rank, row.start, price, row.get("PriceDescription")}
				case rank == c.rank && row.start == c.start && !awsSame(c.price, price):
					return fmt.Errorf("rate %s has two prices at the same tier: %v (%s) and %v (%s)", r.key, c.price, c.desc, price, row.get("PriceDescription"))
				}
			}
		}
		if of.size != nil {
			if s := of.size(&row); s != nil {
				k := cell{s.table, s.col, s.size.Name}
				if old, ok := sizes[k]; ok && !awsSame(old.price, s.price) {
					return fmt.Errorf("%s %s has two prices: %v and %v (%s)", s.col, s.size.Name, old.price, s.price, row.ut)
				}
				sizes[k] = *s
			}
		}
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	for _, r := range rules {
		c, ok := best[r.key]
		if !ok {
			continue
		}
		g.units[r.key] = r.unit
		if region == "aws-other" {
			g.global[r.key] = c.price
			continue
		}
		if g.rates[r.key] == nil {
			g.rates[r.key] = map[string]float64{}
		}
		g.rates[r.key][region] = c.price
	}
	if of.code == "AmazonEC2" {
		g.regions[region] = true
	}
	for k, s := range sizes {
		if g.sizes[k.table] == nil {
			g.sizes[k.table] = map[string]cost.Size{}
		}
		if old, ok := g.sizes[k.table][k.size]; !ok || old.VCPU == 0 {
			g.sizes[k.table][k.size] = s.size
		}
		g.colTbl[k.col] = k.table
		if g.cols[k.col] == nil {
			g.cols[k.col] = map[string]map[string]float64{}
		}
		if g.cols[k.col][region] == nil {
			g.cols[k.col][region] = map[string]float64{}
		}
		g.cols[k.col][region][k.size] = s.price
	}
	return nil
}

// awsRound drops the float noise that scaling adds (0.19999999999999998 becomes 0.2).
func awsRound(x float64) float64 {
	y, _ := strconv.ParseFloat(strconv.FormatFloat(x, 'g', 10, 64), 64)
	return y
}

func awsSame(a, b float64) bool {
	d := a - b
	return d < 1e-12 && -d < 1e-12
}

// build puts everything into one price list, and checks that every rule found something.
func (g *awsGen) build(offers []awsOffer) (*cost.DB, error) {
	db := &cost.DB{
		Published: map[string]string{"aws": time.Now().Format("2006-01-02")},
		Source:    map[string]string{"aws": "AWS Price List bulk files (pricing.us-east-1.amazonaws.com), on-demand rows"},
		Tables:    map[string][]cost.Size{}, Columns: map[string]*cost.Column{}, Rates: map[string]*cost.Rate{},
	}
	var missing []string
	for _, of := range offers {
		for _, r := range of.rates {
			if len(g.rates[r.key]) == 0 {
				missing = append(missing, r.key)
			}
		}
		for _, r := range of.global {
			if _, ok := g.global[r.key]; !ok {
				missing = append(missing, r.key)
			}
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("no price found for: %s", strings.Join(missing, ", "))
	}
	for key, byRegion := range g.rates {
		db.Rates[key] = &cost.Rate{Unit: g.units[key], Prices: byRegion}
	}
	// A global price is the same everywhere, so every region gets it.
	for key, p := range g.global {
		r := &cost.Rate{Unit: g.units[key], Prices: map[string]float64{}}
		for region := range g.regions {
			r.Prices[region] = p
		}
		db.Rates[key] = r
	}
	// Tables list each size once, in name order; each column follows that order.
	for table, set := range g.sizes {
		list := make([]cost.Size, 0, len(set))
		for _, s := range set {
			list = append(list, s)
		}
		slices.SortFunc(list, func(a, b cost.Size) int { return strings.Compare(a.Name, b.Name) })
		db.Tables[table] = list
	}
	for col, byRegion := range g.cols {
		table := g.colTbl[col]
		c := &cost.Column{Table: table, Unit: "hour", Prices: map[string][]float64{}}
		for region, m := range byRegion {
			p := make([]float64, len(db.Tables[table]))
			for i, s := range db.Tables[table] {
				p[i] = m[s.Name]
			}
			c.Prices[region] = p
		}
		db.Columns[col] = c
	}
	for _, want := range []string{"aws/ec2/linux", "aws/ec2/windows", "aws/rds/postgres/single-az", "aws/rds/aurora-postgresql/standard",
		"aws/elasticache/redis", "aws/opensearch/instance", "aws/msk/broker"} {
		if db.Columns[want] == nil {
			return nil, fmt.Errorf("no prices for the column %s", want)
		}
	}
	return db, nil
}
