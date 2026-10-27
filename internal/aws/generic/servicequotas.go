package generic

import (
	"cmp"
	"encoding/base64"
	"slices"
	"strconv"

	"github.com/nrndev/pogi/internal/core"
)

// quota is one default quota. src is the AWS page that lists its value and code.
type quota struct {
	svc, svcName, code, name string
	value                    float64
	unit, src                string
	adjustable               bool
}

const (
	srcLambda = "https://docs.aws.amazon.com/general/latest/gr/lambda-service.html"
	srcDDB    = "https://docs.aws.amazon.com/general/latest/gr/ddb.html"
	srcS3     = "https://docs.aws.amazon.com/general/latest/gr/s3.html"
	srcAPIGW  = "https://docs.aws.amazon.com/general/latest/gr/apigateway.html"
	srcCFN    = "https://docs.aws.amazon.com/general/latest/gr/cfn.html"
	srcEC2    = "https://docs.aws.amazon.com/general/latest/gr/ec2-service.html"
	srcECR    = "https://docs.aws.amazon.com/general/latest/gr/ecr.html"
	srcECS    = "https://docs.aws.amazon.com/general/latest/gr/ecs-service.html"
	srcEvents = "https://docs.aws.amazon.com/general/latest/gr/ev.html"
	srcIAM    = "https://docs.aws.amazon.com/general/latest/gr/iam-service.html"
	srcLogs   = "https://docs.aws.amazon.com/general/latest/gr/cwl_region.html"
	srcRDS    = "https://docs.aws.amazon.com/general/latest/gr/rds-service.html"
	srcSFN    = "https://docs.aws.amazon.com/general/latest/gr/step-functions.html"
)

// Only quotas with one value in every Region, whose page shows the code in the
// Service Quotas console link, are listed. SQS, SNS, Kinesis and Secrets Manager
// have none: their pages show no code, or the value differs by Region. All are
// Regional (ponytail: add more from the same "endpoints and quotas" pages).
var quotaCatalog = []quota{
	{"apigateway", "Amazon API Gateway", "L-379E48B0", "Stages per API", 10, "None", srcAPIGW, true},
	{"apigateway", "Amazon API Gateway", "L-E8693075", "Usage plans", 300, "None", srcAPIGW, true},
	{"apigateway", "Amazon API Gateway", "L-A4C7274F", "VPC links", 20, "None", srcAPIGW, true},
	{"apigateway", "Amazon API Gateway", "L-A93447B8", "Custom Domain Names", 120, "None", srcAPIGW, true},
	{"apigateway", "Amazon API Gateway", "L-01C8A9E0", "Resources/Routes per REST/WebSocket API", 300, "None", srcAPIGW, true},
	{"apigateway", "Amazon API Gateway", "L-E5AE38E3", "Maximum integration timeout in milliseconds", 29000, "Milliseconds", srcAPIGW, true},
	{"cloudformation", "AWS CloudFormation", "L-0485CB21", "Stacks", 2000, "None", srcCFN, true},
	{"cloudformation", "AWS CloudFormation", "L-EC62D81A", "Stack sets per administrator account", 1000, "None", srcCFN, true},
	{"cloudformation", "AWS CloudFormation", "L-C8225BA5", "Stack instances per stack set", 100000, "None", srcCFN, true},
	{"cloudformation", "AWS CloudFormation", "L-6A4B2F69", "Stack instance operations per administrator account", 10000, "None", srcCFN, true},
	{"cloudformation", "AWS CloudFormation", "L-9DE8E4FB", "Resource limit per account", 50, "None", srcCFN, true},
	{"cloudformation", "AWS CloudFormation", "L-DCC58E6D", "Module limit per account", 100, "None", srcCFN, true},
	{"dynamodb", "Amazon DynamoDB", "L-34F6A552", "Account-level read throughput limit (Provisioned mode)", 80000, "None", srcDDB, true},
	{"dynamodb", "Amazon DynamoDB", "L-34F8CCC8", "Account-level write throughput limit (Provisioned mode)", 80000, "None", srcDDB, true},
	{"dynamodb", "Amazon DynamoDB", "L-F7858A77", "Global Secondary Indexes per table", 20, "None", srcDDB, true},
	{"dynamodb", "Amazon DynamoDB", "L-F98FE922", "Maximum number of tables", 2500, "None", srcDDB, true},
	{"dynamodb", "Amazon DynamoDB", "L-CF0CBE56", "Table-level read throughput limit", 40000, "None", srcDDB, true},
	{"dynamodb", "Amazon DynamoDB", "L-AB614373", "Table-level write throughput limit", 40000, "None", srcDDB, true},
	{"ec2", "Amazon EC2", "L-0263D0A3", "EC2-VPC Elastic IPs", 5, "None", srcEC2, true},
	{"ec2", "Amazon EC2", "L-B665C33B", "AMIs", 50000, "None", srcEC2, true},
	{"ec2", "Amazon EC2", "L-70015FFA", "AMI sharing", 1000, "None", srcEC2, true},
	{"ec2", "Amazon EC2", "L-0E3CBAB9", "Public AMIs", 5, "None", srcEC2, true},
	{"ec2", "Amazon EC2", "L-8FBBDF0C", "Amazon FPGA images (AFIs)", 100, "None", srcEC2, true},
	{"ec2", "Amazon EC2", "L-D0B7243C", "New Reserved Instances per month", 20, "None", srcEC2, true},
	{"ecr", "Amazon Elastic Container Registry", "L-CFEB8E8D", "Registered repositories", 100000, "None", srcECR, true},
	{"ecr", "Amazon Elastic Container Registry", "L-03A36CE1", "Images per repository", 100000, "None", srcECR, true},
	{"ecr", "Amazon Elastic Container Registry", "L-A60A366D", "Rate of GetDownloadUrlForLayer requests", 3000, "None", srcECR, true},
	{"ecr", "Amazon Elastic Container Registry", "L-16E70933", "Rate of BatchGetImage requests", 2000, "None", srcECR, true},
	{"ecr", "Amazon Elastic Container Registry", "L-AD52DFB2", "Rate of PutImage requests", 10, "None", srcECR, true},
	{"ecr", "Amazon Elastic Container Registry", "L-55A41110", "Rate of GetAuthorizationToken requests", 500, "None", srcECR, true},
	{"ecs", "Amazon Elastic Container Service", "L-21C621EB", "Clusters per account", 10000, "None", srcECS, true},
	{"ecs", "Amazon Elastic Container Service", "L-43D4E76A", "Sustained rate of cluster read actions (or bucket refill rate)", 20, "None", srcECS, true},
	{"ecs", "Amazon Elastic Container Service", "L-D5AA0F33", "Sustained rate of cluster modify actions (or bucket refill rate)", 1, "None", srcECS, true},
	{"ecs", "Amazon Elastic Container Service", "L-321A0372", "Sustained rate of service read actions (or bucket refill rate)", 20, "None", srcECS, true},
	{"ecs", "Amazon Elastic Container Service", "L-C5297B3A", "Sustained rate of service modify actions (or bucket refill rate)", 5, "None", srcECS, true},
	{"ecs", "Amazon Elastic Container Service", "L-538D7426", "Sustained rate of task definition read actions (or bucket refill rate)", 20, "None", srcECS, true},
	{"events", "Amazon EventBridge", "L-658A4FD9", "Event buses", 100, "None", srcEvents, true},
	{"events", "Amazon EventBridge", "L-FB1C3A6D", "Api destinations", 3000, "None", srcEvents, true},
	{"events", "Amazon EventBridge", "L-595D6D42", "Connections", 3000, "None", srcEvents, true},
	{"events", "Amazon EventBridge", "L-EAC9A2AC", "Endpoints", 100, "None", srcEvents, true},
	{"events", "Amazon EventBridge", "L-664C5505", "Event pattern size", 2048, "None", srcEvents, true},
	{"events", "Amazon EventBridge", "L-FC354966", "Event bus policy size", 10240, "None", srcEvents, true},
	{"iam", "AWS Identity and Access Management", "L-FE177D64", "Roles per account", 1000, "None", srcIAM, true},
	{"iam", "AWS Identity and Access Management", "L-F4A5425F", "Groups per account", 300, "None", srcIAM, true},
	{"iam", "AWS Identity and Access Management", "L-E95E4862", "Customer managed policies per account", 1500, "None", srcIAM, true},
	{"iam", "AWS Identity and Access Management", "L-6E65F664", "Instance profiles per account", 1000, "None", srcIAM, true},
	{"iam", "AWS Identity and Access Management", "L-0DA4ABF3", "Managed policies per role", 20, "None", srcIAM, true},
	{"iam", "AWS Identity and Access Management", "L-858F3967", "OpenId connect providers per account", 100, "None", srcIAM, true},
	{"lambda", "AWS Lambda", "L-B99A9384", "Concurrent executions", 1000, "Count", srcLambda, true},
	{"lambda", "AWS Lambda", "L-2ACBD22F", "Function and layer storage", 300, "Gigabytes", srcLambda, true},
	{"lambda", "AWS Lambda", "L-9FEE3D26", "Elastic network interfaces per VPC", 3000, "None", srcLambda, true},
	{"lambda", "AWS Lambda", "L-14320ECE", "Network bandwidth per execution environment", 625, "Megabits/Second", srcLambda, true},
	{"logs", "Amazon CloudWatch Logs", "L-C7B9AAAB", "Log groups", 1000000, "None", srcLogs, true},
	{"logs", "Amazon CloudWatch Logs", "L-7E1FAE88", "PutLogEvents throttle limit in transactions per second", 5000, "None", srcLogs, true},
	{"logs", "Amazon CloudWatch Logs", "L-D2832119", "CreateLogGroup throttle limit in transactions per second", 10, "None", srcLogs, true},
	{"logs", "Amazon CloudWatch Logs", "L-76507CEF", "CreateLogStream throttle limit in transactions per second", 50, "None", srcLogs, true},
	{"logs", "Amazon CloudWatch Logs", "L-4284EEDE", "DescribeLogGroups throttle limit in transactions per second", 10, "None", srcLogs, true},
	{"logs", "Amazon CloudWatch Logs", "L-3F243AD0", "DescribeLogStreams throttle limit in transactions per second", 25, "None", srcLogs, true},
	{"rds", "Amazon Relational Database Service", "L-952B80B8", "DB clusters", 40, "None", srcRDS, true},
	{"rds", "Amazon Relational Database Service", "L-272F1212", "Manual DB instance snapshots", 100, "None", srcRDS, true},
	{"rds", "Amazon Relational Database Service", "L-9B510759", "Manual DB cluster snapshots", 100, "None", srcRDS, true},
	{"rds", "Amazon Relational Database Service", "L-A59F4C87", "Event subscriptions", 20, "None", srcRDS, true},
	{"rds", "Amazon Relational Database Service", "L-D94C7EA3", "Proxies", 20, "None", srcRDS, true},
	{"rds", "Amazon Relational Database Service", "L-5BC124EF", "Read replicas per primary", 15, "None", srcRDS, true},
	{"s3", "Amazon Simple Storage Service", "L-DC2B2D3D", "General purpose buckets", 10000, "None", srcS3, true},
	{"s3", "Amazon Simple Storage Service", "L-FAABEEBA", "Access Points", 10000, "None", srcS3, true},
	{"s3", "Amazon Simple Storage Service", "L-775A314D", "Directory buckets", 100, "None", srcS3, true},
	{"states", "AWS Step Functions", "L-B66B0F91", "Registered state machines", 100000, "None", srcSFN, true},
	{"states", "AWS Step Functions", "L-A9562A73", "Registered activities", 100000, "None", srcSFN, true},
	{"states", "AWS Step Functions", "L-B8A5B662", "Open executions", 1000000, "None", srcSFN, true},
	{"states", "AWS Step Functions", "L-5AD63623", "Execution history retention time", 90, "None", srcSFN, true},
	{"states", "AWS Step Functions", "L-6AFCB355", "Published state machine versions per state machine", 1000, "None", srcSFN, true},
	{"states", "AWS Step Functions", "L-C1D5EBBB", "State machine aliases per state machine", 100, "None", srcSFN, true},
}

// item is the ServiceQuota shape; account "" gives the default (no account in the ARN).
func (q quota) item(region, account string) map[string]any {
	m := map[string]any{"ServiceCode": q.svc, "ServiceName": q.svcName, "QuotaCode": q.code, "QuotaName": q.name,
		"Value": q.value, "Unit": q.unit, "Adjustable": q.adjustable, "GlobalQuota": false,
		"QuotaArn": core.ARN("servicequotas", region, account, q.svc+"/"+q.code)}
	if account != "" {
		m["QuotaAppliedAtLevel"] = "ACCOUNT"
	}
	return m
}

func findQuota(svc, code string) *quota {
	i := slices.IndexFunc(quotaCatalog, func(q quota) bool { return q.svc == svc && q.code == code })
	if i < 0 {
		return nil
	}
	return &quotaCatalog[i]
}

// page cuts items by MaxResults (1-100, else IllegalArgumentException) and an offset token.
func (c *Call) page(items []any, key string) (map[string]any, error) {
	limit := 100
	if v, ok := c.In["MaxResults"]; ok && v != nil {
		n, err := strconv.Atoi(str(v))
		if err != nil || n < 1 || n > 100 {
			return nil, c.Invalid("Invalid MaxResults: %s. It must be between 1 and 100.", str(v))
		}
		limit = n
	}
	off := 0
	if tok := str(c.In["NextToken"]); tok != "" {
		b, err := base64.StdEncoding.DecodeString(tok)
		if off, _ = strconv.Atoi(string(b)); err != nil || off < 0 || off > len(items) {
			return nil, c.Err("InvalidPaginationTokenException", "Invalid NextToken: %s", tok)
		}
	}
	end := min(off+limit, len(items))
	out := map[string]any{key: items[off:end]}
	if end < len(items) {
		out["NextToken"] = base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	return out, nil
}

func (c *Call) quotas(account string) (map[string]any, error) {
	sc := str(c.In["ServiceCode"])
	var items []any
	for _, q := range quotaCatalog {
		if q.svc == sc && (str(c.In["QuotaCode"]) == "" || q.code == str(c.In["QuotaCode"])) {
			items = append(items, q.item(c.Region, account))
		}
	}
	if len(items) == 0 && !slices.ContainsFunc(quotaCatalog, func(q quota) bool { return q.svc == sc }) {
		return nil, c.NotFound("Service %s was not found.", sc)
	}
	return c.page(items, "Quotas")
}

// Service Quotas: a quota increase request gets an Id and stays PENDING, so
// GetRequestedServiceQuotaChange can read it back; an unknown Id is not found.
// Nothing approves a request (ponytail).
func init() {
	Override("service-quotas", "RequestServiceQuotaIncrease", func(c *Call) (map[string]any, error) {
		now, id := c.now(), core.NewID()
		sc, qc := str(c.In["ServiceCode"]), str(c.In["QuotaCode"])
		q := map[string]any{"Id": id, "CaseId": id, "ServiceCode": sc, "ServiceName": sc, "QuotaCode": qc, "QuotaName": qc,
			"DesiredValue": c.In["DesiredValue"], "Status": "PENDING", "Created": float64(now.Unix()), "LastUpdated": float64(now.Unix()),
			"Requester": "arn:aws:iam::" + c.Account + ":root", "QuotaArn": core.ARN("servicequotas", c.Region, c.Account, sc+"/"+qc), "GlobalQuota": false,
			"QuotaRequestedAtLevel": cmp.Or(str(c.In["ContextId"]), "ACCOUNT")}
		c.Put(&Resource{Type: "RequestedServiceQuotaChange", Key: id, IDs: map[string]string{"RequestId": id}, ID: id, Name: id, Account: c.Account,
			Region: c.key(), Created: now, Updated: now, Data: q})
		return map[string]any{"RequestedQuota": q}, nil
	})
	Override("service-quotas", "GetRequestedServiceQuotaChange", func(c *Call) (map[string]any, error) {
		r := c.Get("RequestedServiceQuotaChange", str(c.In["RequestId"]))
		if r == nil {
			return nil, c.NotFound("Resource %s was not found.", str(c.In["RequestId"]))
		}
		return map[string]any{"RequestedQuota": r.Data}, nil
	})
	getQuota := func(applied bool) func(*Call) (map[string]any, error) {
		return func(c *Call) (map[string]any, error) {
			a := ""
			if applied {
				a = c.Account
			}
			q := findQuota(str(c.In["ServiceCode"]), str(c.In["QuotaCode"]))
			if q == nil {
				return nil, c.NotFound("Resource %s/%s was not found.", str(c.In["ServiceCode"]), str(c.In["QuotaCode"]))
			}
			return map[string]any{"Quota": q.item(c.Region, a)}, nil
		}
	}
	Override("service-quotas", "GetAWSDefaultServiceQuota", getQuota(false))
	Override("service-quotas", "GetServiceQuota", getQuota(true))
	Override("service-quotas", "ListAWSDefaultServiceQuotas", func(c *Call) (map[string]any, error) { return c.quotas("") })
	Override("service-quotas", "ListServiceQuotas", func(c *Call) (map[string]any, error) { return c.quotas(c.Account) })
	Override("service-quotas", "ListServices", func(c *Call) (map[string]any, error) {
		var items []any
		for _, q := range quotaCatalog {
			if len(items) == 0 || items[len(items)-1].(map[string]any)["ServiceCode"] != q.svc {
				items = append(items, map[string]any{"ServiceCode": q.svc, "ServiceName": q.svcName})
			}
		}
		return c.page(items, "Services")
	})
}
