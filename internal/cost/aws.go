package cost

// The AWS resource types pogi prices, and what each one costs. Prices come from the embedded price
// list (scripts/prices/aws.go); nothing here holds a price.

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/terraform"
)

func init() {
	for _, t := range awsFree {
		free[t] = true
	}
	m := mappings
	m["aws_instance"] = mapping{"EC2", priceInstance, "aws/ec2/%s"}
	m["aws_autoscaling_group"] = mapping{"EC2 Auto Scaling", priceASG, "aws/ec2/%s"}
	m["aws_ebs_volume"] = mapping{"EBS", priceEBSVolume, ""}
	m["aws_eip"] = mapping{"VPC", priceEIP, ""}
	m["aws_nat_gateway"] = mapping{"NAT Gateway", priceNAT, ""}
	m["aws_lb"] = mapping{"Elastic Load Balancing", priceLB, ""}
	m["aws_alb"] = m["aws_lb"]
	m["aws_vpc_endpoint"] = mapping{"VPC", priceVPCEndpoint, ""}
	m["aws_db_instance"] = mapping{"RDS", priceDBInstance, "aws/rds/postgres/single-az"}
	m["aws_rds_cluster"] = mapping{"Aurora", priceRDSCluster, ""}
	m["aws_rds_cluster_instance"] = mapping{"Aurora", priceClusterInstance, "aws/rds/aurora-postgresql/standard"}
	m["aws_elasticache_cluster"] = mapping{"ElastiCache", priceCacheCluster, "aws/elasticache/redis"}
	m["aws_elasticache_replication_group"] = mapping{"ElastiCache", priceReplicationGroup, "aws/elasticache/redis"}
	m["aws_eks_cluster"] = mapping{"EKS", priceEKSCluster, ""}
	m["aws_eks_node_group"] = mapping{"EKS", priceNodeGroup, "aws/ec2/linux"}
	m["aws_ecs_service"] = mapping{"ECS", priceECSService, ""}
	m["aws_lambda_function"] = mapping{"Lambda", priceLambda, ""}
	m["aws_s3_bucket"] = mapping{"S3", priceS3, ""}
	m["aws_dynamodb_table"] = mapping{"DynamoDB", priceDynamoDB, ""}
	m["aws_sqs_queue"] = mapping{"SQS", priceSQS, ""}
	m["aws_sfn_state_machine"] = mapping{"Step Functions", priceSFN, ""}
	m["aws_api_gateway_rest_api"] = mapping{"API Gateway", priceRESTAPI, ""}
	m["aws_apigatewayv2_api"] = mapping{"API Gateway", priceAPIv2, ""}
	m["aws_sns_topic"] = mapping{"SNS", priceSNS, ""}
	m["aws_kinesis_stream"] = mapping{"Kinesis", priceKinesis, ""}
	m["aws_cloudwatch_log_group"] = mapping{"CloudWatch", priceLogGroup, ""}
	m["aws_cloudfront_distribution"] = mapping{"CloudFront", priceCloudFront, ""}
	m["aws_opensearch_domain"] = mapping{"OpenSearch", priceOpenSearch, "aws/opensearch/instance"}
	m["aws_msk_cluster"] = mapping{"MSK", priceMSK, "aws/msk/broker"}
	m["aws_secretsmanager_secret"] = mapping{"Secrets Manager", priceSecret, ""}
	m["aws_kms_key"] = mapping{"KMS", priceKMS, ""}
	m["aws_route53_zone"] = mapping{"Route 53", priceZone, ""}
}

// awsFree lists the AWS types that cost nothing on their own. Each one is free to create and
// hold; what runs in or behind it is priced on its own resources.
var awsFree = []string{
	// networking: VPCs, subnets, routing, security groups
	"aws_vpc", "aws_subnet", "aws_internet_gateway", "aws_egress_only_internet_gateway", "aws_route_table",
	"aws_route_table_association", "aws_main_route_table_association", "aws_route", "aws_security_group",
	"aws_security_group_rule", "aws_vpc_security_group_ingress_rule", "aws_vpc_security_group_egress_rule",
	"aws_network_acl", "aws_network_acl_rule", "aws_network_acl_association", "aws_vpc_dhcp_options",
	"aws_vpc_dhcp_options_association", "aws_vpc_ipv4_cidr_block_association", "aws_default_vpc", "aws_default_subnet",
	"aws_default_security_group", "aws_default_route_table", "aws_default_network_acl", "aws_eip_association",
	// identity
	"aws_iam_role", "aws_iam_policy", "aws_iam_role_policy", "aws_iam_role_policy_attachment", "aws_iam_instance_profile",
	"aws_iam_user", "aws_iam_group", "aws_iam_group_membership", "aws_iam_user_policy_attachment", "aws_iam_openid_connect_provider",
	"aws_key_pair",
	// templates and settings: the instances they start are priced on the group
	"aws_launch_template", "aws_launch_configuration", "aws_autoscaling_policy", "aws_db_subnet_group", "aws_db_parameter_group",
	"aws_db_option_group", "aws_rds_cluster_parameter_group", "aws_elasticache_subnet_group", "aws_elasticache_parameter_group",
	"aws_lb_listener", "aws_lb_listener_rule", "aws_lb_target_group", "aws_lb_target_group_attachment",
	"aws_ecs_cluster", "aws_ecs_task_definition", "aws_eks_addon", "aws_lambda_permission", "aws_lambda_alias",
	"aws_s3_bucket_policy", "aws_s3_bucket_versioning", "aws_s3_bucket_public_access_block", "aws_s3_bucket_acl",
	"aws_s3_bucket_ownership_controls", "aws_s3_bucket_server_side_encryption_configuration",
	"aws_sqs_queue_policy", "aws_sns_topic_policy", "aws_sns_topic_subscription", "aws_cloudwatch_log_stream",
	"aws_kms_alias", "aws_secretsmanager_secret_version", "aws_route53_record",
}

// ---- helpers ----------------------------------------------------------------------------------

func hours(n float64) float64 { return n * HoursPerMonth }

// awsNote adds a note without making the row "assumed": it says something true that isn't a guess.
func awsNote(r *res, s string) {
	if !slices.Contains(r.notes, s) {
		r.notes = append(r.notes, s)
	}
}

// awsUsing runs f with the attributes of another resource (a launch template, a task definition),
// so the usual attribute helpers read it.
func awsUsing(r *res, attrs map[string]any, f func()) {
	old := r.attrs
	r.attrs = attrs
	defer func() { r.attrs = old }()
	f()
}

// awsSizeFrom is r.size for a size that comes from another resource: file is what that
// resource says.
func awsSizeFrom(r *res, column, attr, file string) string {
	cur := cmp.Or(r.pick, file)
	if cur == "" {
		if !r.needs {
			r.need(attr)
		}
		return ""
	}
	r.sizes = &SizeRef{Column: column, Attr: attr, Current: cur, File: file}
	return cur
}

// awsOS is the operating system for a VM: Linux unless the usage file says windows.
func awsOS(r *res) string {
	switch os := r.usageStr("os", ""); os {
	case "linux", "windows":
		return os
	case "":
		if os := awsAMIOS(r); os != "" {
			return os
		}
		r.assume("assumed Linux (the files don't say; set os: windows in the usage file for Windows)")
		return "linux"
	default:
		r.miss("the operating system " + os + " isn't priced; use linux or windows")
		return ""
	}
}

// awsCount reads the first of paths that has a known value, such as desired_capacity then
// min_size. When none has, the row needs the first one.
func awsCount(r *res, paths ...string) (float64, bool) {
	for _, p := range paths {
		if v, ok := r.get(p); ok {
			if _, unknown := v.(terraform.Unknown); !unknown {
				return r.num(p, 0), true
			}
		}
	}
	r.need(paths[0])
	return 0, false
}

// awsList returns an attribute that holds a list, such as ebs_block_device.
func awsList(r *res, path string) []any {
	v, _ := r.get(path)
	l, _ := v.([]any)
	return l
}

// awsUsageLine adds a usage-priced line whose quantity the mapping works out from usage keys. The
// quantity is 0 when the usage file doesn't give them.
func awsUsageLine(r *res, rateKey string, qty float64, what string, keys ...string) {
	for _, k := range keys {
		if !slices.Contains(r.usageKeys, k) {
			r.usageKeys = append(r.usageKeys, k)
		}
	}
	p, unit, ok := r.db.Rate(rateKey, r.region)
	if !ok {
		r.miss(fmt.Sprintf("no price for %s in %s", what, r.region))
		return
	}
	r.lines = append(r.lines, Line{What: what, Qty: qty, Unit: unit, UnitPrice: p, Monthly: qty * p, Usage: true})
}

// awsUsageNum returns a number from the usage file, and records that the row reads it.
func awsUsageNum(r *res, key string) float64 {
	if !slices.Contains(r.usageKeys, key) {
		r.usageKeys = append(r.usageKeys, key)
	}
	return toNum(r.usageIn[key])
}

// ebsVolume adds the lines of count EBS volumes: the storage, and the IOPS and throughput that
// cost extra.
func ebsVolume(r *res, label, typ string, gb, iops, mbps, count float64) {
	if !slices.Contains([]string{"gp2", "gp3", "io1", "io2", "st1", "sc1", "standard"}, typ) {
		r.miss("the volume type " + typ + " isn't priced")
		return
	}
	r.rate("aws/ebs/"+typ+"/storage", gb*count, fmt.Sprintf("%s (%s, %g GB)", label, typ, gb))
	switch typ {
	case "gp3":
		// gp3 includes 3,000 IOPS and 125 MB/s; the rest is billed.
		if iops > 3000 {
			r.rate("aws/ebs/gp3/iops", (iops-3000)*count, fmt.Sprintf("%s: extra IOPS (%g above 3,000)", label, iops))
		}
		if mbps > 125 {
			r.rate("aws/ebs/gp3/throughput", (mbps-125)*count, fmt.Sprintf("%s: extra throughput (%g above 125 MB/s)", label, mbps))
		}
	case "io1", "io2":
		if iops <= 0 {
			r.miss("needs a value for iops (" + typ + " volumes are priced by IOPS)")
			return
		}
		r.rate("aws/ebs/"+typ+"/iops", iops*count, fmt.Sprintf("%s: provisioned IOPS (%g)", label, iops))
		if typ == "io2" && iops > 32000 {
			r.assume("io2 IOPS above 32,000 cost less each; all are priced at the first tier")
		}
	}
}

// awsRootDisk prices the root disk of an instance or launch configuration. The attributes are
// usually missing, so the size and type are assumed from Amazon Linux's defaults.
func awsRootDisk(r *res, p string, count float64) {
	if _, ok := r.get(p + ".volume_size"); !ok {
		r.assume("assumed an 8 GB root disk (the AMI sets the size)")
	}
	typ := "gp3"
	if _, ok := r.get(p + ".volume_type"); !ok {
		r.assume("assumed a gp3 root disk (the AMI sets the type)")
	} else {
		typ = r.str(p+".volume_type", "gp3")
	}
	ebsVolume(r, "Root disk", typ, r.num(p+".volume_size", 8), r.num(p+".iops", 0), r.num(p+".throughput", 0), count)
}

// awsBlockDevices prices the ebs_block_device blocks of an instance or launch configuration.
func awsBlockDevices(r *res, count float64) {
	for i := range awsList(r, "ebs_block_device") {
		p := fmt.Sprintf("ebs_block_device.%d", i)
		if _, ok := r.get(p + ".volume_size"); !ok {
			r.assume("assumed 8 GB for " + p + " (the snapshot or AMI sets the size)")
		}
		ebsVolume(r, fmt.Sprintf("Disk %d", i+1), r.str(p+".volume_type", "gp2"), r.num(p+".volume_size", 8), r.num(p+".iops", 0), r.num(p+".throughput", 0), count)
	}
}

// awsPublicIP prices the public IPv4 address of count instances.
func awsPublicIP(r *res, path string, count float64) {
	if _, ok := r.get(path); !ok {
		r.assume("assumed no public IPv4 address (" + path + " isn't set)")
		return
	}
	if r.flag(path, false) {
		r.rate("aws/ipv4/hour", hours(count), "Public IPv4 address")
	}
}

// ---- compute ----------------------------------------------------------------------------------

func priceInstance(r *res) {
	os := awsOS(r)
	if os == "" {
		return
	}
	col := "aws/ec2/" + os
	t := r.size(col, "instance_type", "")
	r.hourly(col, t, HoursPerMonth, fmt.Sprintf("Instance hours (%s, %s)", t, osName[os]))
	if ten := r.str("tenancy", "default"); ten != "default" {
		r.assume("tenancy " + ten + " is priced as shared tenancy, which costs less")
	}
	awsRootDisk(r, "root_block_device.0", 1)
	awsBlockDevices(r, 1)
	if r.flag("monitoring", false) {
		r.rate("aws/cloudwatch/metric", 7, "Detailed monitoring (7 metrics)")
	}
	awsPublicIP(r, "associate_public_ip_address", 1)
}

func priceASG(r *res) {
	os := awsOS(r)
	if os == "" {
		return
	}
	n, ok := awsCount(r, "desired_capacity", "min_size")
	if !ok {
		return
	}
	col := "aws/ec2/" + os
	tmpl := r.dep("aws_launch_template", "aws_launch_configuration")
	if tmpl == nil {
		r.need("launch_template (the group's template wasn't found in the files)")
		return
	}
	// The group can override the template's type (mixed_instances_policy).
	typ, over := "", false
	if v, ok := r.get("mixed_instances_policy.launch_template.override.instance_type"); ok {
		if s, ok := v.(string); ok {
			typ, over = s, true
			r.assume("priced as " + s + " only (the first mixed-instances override)")
		}
	}
	awsUsing(r, tmpl.Attrs, func() {
		if !over {
			typ = r.str("instance_type", "")
		}
		t := awsSizeFrom(r, col, "instance_type", typ)
		r.hourly(col, t, hours(n), fmt.Sprintf("Instance hours (%d × %s, %s)", int(n), t, osName[os]))
		if tmpl.Type == "aws_launch_configuration" {
			awsRootDisk(r, "root_block_device.0", n)
			awsBlockDevices(r, n)
			if r.flag("enable_monitoring", true) {
				awsNote(r, "the launch configuration turns on detailed monitoring by default; set enable_monitoring = false to drop it")
				r.rate("aws/cloudwatch/metric", 7*n, "Detailed monitoring (7 metrics each)")
			}
			return
		}
		devs := awsList(r, "block_device_mappings")
		if len(devs) == 0 {
			r.assume("assumed an 8 GB gp3 root disk (the template sets no disk)")
			ebsVolume(r, "Root disk", "gp3", 8, 0, 0, n)
		}
		for i := range devs {
			p := fmt.Sprintf("block_device_mappings.%d.ebs", i)
			if _, ok := r.get(p + ".volume_size"); !ok {
				continue
			}
			ebsVolume(r, fmt.Sprintf("Disk %d", i+1), r.str(p+".volume_type", "gp2"), r.num(p+".volume_size", 8), r.num(p+".iops", 0), r.num(p+".throughput", 0), n)
		}
		if r.flag("monitoring.enabled", false) {
			r.rate("aws/cloudwatch/metric", 7*n, "Detailed monitoring (7 metrics each)")
		}
	})
	r.assume("assumed no public IPv4 addresses (the template's network settings aren't read)")
}

func priceEBSVolume(r *res) {
	size := 0.0
	if _, ok := r.get("size"); ok {
		size = r.num("size", 8)
	} else if r.str("snapshot_id", "") != "" {
		r.assume("assumed 8 GB (the snapshot sets the size)")
		size = 8
	} else {
		r.need("size")
		return
	}
	ebsVolume(r, "Volume", r.str("type", "gp2"), size, r.num("iops", 0), r.num("throughput", 0), 1)
}

func priceEIP(r *res) {
	r.rate("aws/ipv4/hour", HoursPerMonth, "Public IPv4 address")
}

func priceNAT(r *res) {
	r.rate("aws/natgw/hour", HoursPerMonth, "NAT gateway hours")
	r.usage("monthly_gb_processed", "aws/natgw/data", 1, "Data processed (GB)")
}

func priceLB(r *res) {
	t := r.str("load_balancer_type", "application")
	unit := map[string]string{"application": "LCU", "network": "NLCU", "gateway": "GLCU"}[t]
	if unit == "" {
		r.miss("the load balancer type " + t + " isn't priced")
		return
	}
	r.rate("aws/elb/"+t+"/hour", HoursPerMonth, "Load balancer hours ("+t+")")
	r.usage("monthly_lcu_hours", "aws/elb/"+t+"/lcu", 1, "Capacity units ("+unit+"-hours)")
}

func priceVPCEndpoint(r *res) {
	switch t := r.str("vpc_endpoint_type", "Gateway"); t {
	case "Gateway":
		awsNote(r, "gateway endpoints (S3 and DynamoDB) are free")
	case "Interface", "GatewayLoadBalancer":
		key := "aws/vpce/"
		if t == "GatewayLoadBalancer" {
			key += "gwlb/"
		}
		n := 1.0
		if l := awsList(r, "subnet_ids"); len(l) > 0 {
			n = float64(len(l))
		} else {
			r.assume("assumed 1 subnet (subnet_ids isn't known)")
		}
		r.rate(key+"hour", hours(n), fmt.Sprintf("Endpoint hours (%g subnets)", n))
		r.usage("monthly_gb_processed", key+"data", 1, "Data processed (GB)")
	default:
		r.miss("the endpoint type " + t + " isn't priced")
	}
}

// ---- databases and caches ---------------------------------------------------------------------

// rdsStorage prices the storage, IOPS and throughput of an RDS instance.
func rdsStorage(r *res, eng string, multi bool) {
	gb := 0.0
	if _, ok := r.get("allocated_storage"); ok {
		gb = r.num("allocated_storage", 20)
	} else if r.str("snapshot_identifier", "") != "" || r.str("replicate_source_db", "") != "" {
		r.assume("assumed 20 GB of storage (allocated_storage isn't set; it comes from the snapshot or source)")
		gb = 20
	} else {
		r.need("allocated_storage")
		return
	}
	iops := r.num("iops", 0)
	def := "gp2" // the provider's default; io1 when iops is set
	if iops > 0 {
		def = "io1"
	}
	typ := r.str("storage_type", def)
	sfx := ""
	if multi {
		sfx = "/multi-az"
	}
	key := "aws/rds/" + eng + "/"
	if !slices.Contains([]string{"gp2", "gp3", "io1", "io2", "standard"}, typ) {
		r.miss("the storage type " + typ + " isn't priced")
		return
	}
	r.rate(key+"storage/"+typ+sfx, gb, fmt.Sprintf("Storage (%s, %g GB)", typ, gb))
	switch typ {
	case "gp3":
		// gp3 includes 3,000 IOPS and 125 MB/s below 400 GB, and 12,000 and 500 above.
		baseIOPS, baseMBps := 3000.0, 125.0
		if gb >= 400 {
			baseIOPS, baseMBps = 12000, 500
		}
		if iops > baseIOPS {
			r.rate(key+"iops/gp3"+sfx, iops-baseIOPS, fmt.Sprintf("Extra IOPS (%g above %g)", iops, baseIOPS))
		}
		if t := r.num("storage_throughput", 0); t > baseMBps {
			r.rate(key+"throughput/gp3"+sfx, t-baseMBps, fmt.Sprintf("Extra throughput (%g above %g MB/s)", t, baseMBps))
		}
	case "io1", "io2":
		if iops <= 0 {
			r.need("iops")
			return
		}
		r.rate(key+"iops/"+typ+sfx, iops, fmt.Sprintf("Provisioned IOPS (%g)", iops))
	}
}

func priceDBInstance(r *res) {
	eng := r.str("engine", "")
	if eng == "" {
		if !r.needs {
			r.need("engine")
		}
		return
	}
	switch eng {
	case "mysql", "mariadb", "postgres":
	case "aurora-mysql", "aurora-postgresql", "aurora":
		r.miss("Aurora is priced on aws_rds_cluster and aws_rds_cluster_instance")
		return
	default:
		r.miss("the engine " + eng + " isn't priced; pogi prices mysql, mariadb, postgres and Aurora")
		return
	}
	multi := r.flag("multi_az", false)
	variant := "single-az"
	if multi {
		variant = "multi-az"
	}
	col := "aws/rds/" + eng + "/" + variant
	class := r.size(col, "instance_class", "")
	r.hourly(col, class, HoursPerMonth, fmt.Sprintf("Instance hours (%s, %s, %s)", class, eng, variant))
	rdsStorage(r, eng, multi)
}

// auroraEngine maps a cluster's engine to its price keys; "" means pogi doesn't price it.
func auroraEngine(e string) string {
	switch e {
	case "aurora-mysql", "aurora":
		return "aurora-mysql"
	case "aurora-postgresql":
		return "aurora-postgresql"
	}
	return ""
}

func auroraVariant(storageType string) string {
	if storageType == "aurora-iopt1" {
		return "io-optimized"
	}
	return "standard"
}

func priceRDSCluster(r *res) {
	e := r.str("engine", "")
	eng := auroraEngine(e)
	switch {
	case e == "":
		if !r.needs {
			r.need("engine")
		}
		return
	case eng == "":
		r.miss("the engine " + e + " isn't priced on a cluster; pogi prices Aurora clusters")
		return
	case r.str("engine_mode", "provisioned") == "serverless":
		r.miss("Aurora Serverless v1 isn't priced")
		return
	}
	v := auroraVariant(r.str("storage_type", ""))
	r.usage("storage_gb", "aws/rds/"+eng+"/storage/"+v, 1, "Storage ("+v+")")
	if v == "standard" {
		r.usage("monthly_io_requests", "aws/rds/"+eng+"/io", 1e-6, "I/O requests")
	}
}

func priceClusterInstance(r *res) {
	e := r.str("engine", "")
	eng := auroraEngine(e)
	switch {
	case e == "":
		if !r.needs {
			r.need("engine")
		}
		return
	case eng == "":
		r.miss("the engine " + e + " isn't priced; pogi prices aurora-mysql and aurora-postgresql")
		return
	}
	cluster := r.dep("aws_rds_cluster")
	v := "standard"
	if cluster != nil {
		awsUsing(r, cluster.Attrs, func() { v = auroraVariant(r.str("storage_type", "")) })
	}
	if class := r.str("instance_class", ""); class != "db.serverless" {
		col := "aws/rds/" + eng + "/" + v
		class = r.size(col, "instance_class", "")
		r.hourly(col, class, HoursPerMonth, fmt.Sprintf("Instance hours (%s, %s)", class, eng))
		return
	}
	// Serverless v2: priced by capacity units (ACUs). The usage file can say the average; the
	// cluster's minimum is the fallback.
	acu := awsUsageNum(r, "avg_acus")
	if acu <= 0 {
		if cluster == nil {
			r.need("serverlessv2_scaling_configuration (the cluster wasn't found in the files)")
			return
		}
		awsUsing(r, cluster.Attrs, func() { acu = r.num("serverlessv2_scaling_configuration.min_capacity", 0) })
		if acu <= 0 {
			r.need("serverlessv2_scaling_configuration.min_capacity")
			return
		}
		r.assume(fmt.Sprintf("priced at the cluster's minimum of %g ACUs; set avg_acus in the usage file for the real average", acu))
	}
	r.rate("aws/rds/"+eng+"/acu/"+v, hours(acu), fmt.Sprintf("Serverless capacity (%g ACUs)", acu))
}

// cacheEngine returns the engine of a cache, or "" after asking for it.
func cacheEngine(r *res, def string) string {
	e := r.str("engine", def)
	switch e {
	case "redis", "valkey", "memcached":
		return e
	case "":
		if !r.needs {
			r.need("engine")
		}
	default:
		r.miss("the engine " + e + " isn't priced")
	}
	return ""
}

func priceCacheCluster(r *res) {
	if id := r.str("replication_group_id", ""); id != "" {
		awsNote(r, "a member of replication group "+id+"; the group is priced")
		return
	}
	eng := cacheEngine(r, "")
	if eng == "" {
		return
	}
	col := "aws/elasticache/" + eng
	node := r.size(col, "node_type", "")
	n, ok := awsCount(r, "num_cache_nodes")
	if !ok {
		return
	}
	r.hourly(col, node, hours(n), fmt.Sprintf("Node hours (%g × %s, %s)", n, node, eng))
}

func priceReplicationGroup(r *res) {
	eng := cacheEngine(r, "redis")
	if eng == "" {
		return
	}
	col := "aws/elasticache/" + eng
	node := r.size(col, "node_type", "")
	var n float64
	if _, ok := r.get("num_cache_clusters"); ok {
		n, _ = awsCount(r, "num_cache_clusters")
	} else if _, ok := r.get("num_node_groups"); ok {
		groups, _ := awsCount(r, "num_node_groups")
		replicas := 0.0
		if _, ok := r.get("replicas_per_node_group"); ok {
			replicas = r.num("replicas_per_node_group", 0)
		} else {
			r.assume("assumed no replicas (replicas_per_node_group isn't set)")
		}
		n = groups * (1 + replicas)
	} else {
		r.need("num_cache_clusters")
		return
	}
	r.hourly(col, node, hours(n), fmt.Sprintf("Node hours (%g × %s, %s)", n, node, eng))
}

// ---- containers -------------------------------------------------------------------------------

func priceEKSCluster(r *res) {
	r.rate("aws/eks/cluster/hour", HoursPerMonth, "Cluster hours")
}

func priceNodeGroup(r *res) {
	n, ok := awsCount(r, "scaling_config.desired_size", "scaling_config.min_size")
	if !ok {
		return
	}
	os := "linux"
	if strings.Contains(r.str("ami_type", ""), "WINDOWS") {
		os = "windows"
	}
	col := "aws/ec2/" + os
	t := r.size(col, "instance_types.0", "t3.medium")
	if len(awsList(r, "instance_types")) > 1 {
		r.assume("priced as " + t + " only (the group lists more than one instance type)")
	}
	r.hourly(col, t, hours(n), fmt.Sprintf("Node hours (%g × %s, %s)", n, t, os))
	if r.str("capacity_type", "ON_DEMAND") == "SPOT" {
		r.assume("priced at the on-demand rate (Spot prices aren't in the price list)")
	}
	if len(awsList(r, "launch_template")) > 0 {
		r.assume("the disks come from the launch template and aren't priced")
		return
	}
	gb := r.num("disk_size", 20)
	r.assume("assumed gp3 node disks")
	ebsVolume(r, "Node disk", "gp3", gb, 0, 0, n)
}

// awsTaskSize reads a Fargate cpu or memory value: "256" (CPU units, or MiB), "0.25 vCPU" or "1 GB".
func awsTaskSize(s string) (n float64, withUnit bool) {
	f := strings.Fields(strings.ToLower(s))
	if len(f) == 0 {
		return 0, false
	}
	n, err := strconv.ParseFloat(f[0], 64)
	if err != nil {
		return 0, false
	}
	return n, len(f) > 1
}

func priceECSService(r *res) {
	fargate := r.str("launch_type", "") == "FARGATE"
	for i := range awsList(r, "capacity_provider_strategy") {
		cp := r.str(fmt.Sprintf("capacity_provider_strategy.%d.capacity_provider", i), "")
		fargate = fargate || strings.HasPrefix(cp, "FARGATE")
		if cp == "FARGATE_SPOT" {
			r.assume("FARGATE_SPOT is priced at the on-demand Fargate rate (Spot prices aren't in the price list)")
		}
	}
	if !fargate {
		awsNote(r, "not a Fargate service: the EC2 instances it runs on are priced on their own")
		return
	}
	td := r.dep("aws_ecs_task_definition")
	if td == nil {
		r.need("task_definition (its cpu and memory)")
		return
	}
	var tasks float64
	if _, ok := r.get("desired_count"); ok {
		tasks = r.num("desired_count", 0)
	} else {
		awsNote(r, "desired_count isn't set, so no tasks run (the provider's default is 0)")
	}
	awsUsing(r, td.Attrs, func() {
		cpu, cpuU := awsTaskSize(r.str("cpu", ""))
		mem, memU := awsTaskSize(r.str("memory", ""))
		if cpu == 0 || mem == 0 {
			r.need("cpu and memory of the task definition")
			return
		}
		if !cpuU {
			cpu /= 1024 // CPU units to vCPUs
		}
		if !memU {
			mem /= 1024 // MiB to GB
		}
		kind := "linux"
		if strings.EqualFold(r.str("runtime_platform.cpu_architecture", ""), "ARM64") {
			kind = "arm"
		}
		if strings.HasPrefix(strings.ToUpper(r.str("runtime_platform.operating_system_family", "")), "WINDOWS") {
			kind = "windows"
		}
		what := fmt.Sprintf("%g tasks × %g vCPU, %g GB", tasks, cpu, mem)
		r.rate("aws/fargate/"+kind+"/vcpu", hours(tasks*cpu), "vCPU hours ("+what+")")
		if kind == "windows" {
			r.rate("aws/fargate/windows/os", hours(tasks*cpu), "Windows licence hours")
		}
		r.rate("aws/fargate/"+kind+"/gb", hours(tasks*mem), "Memory hours ("+what+")")
		if gib := r.num("ephemeral_storage.size_in_gib", 20); gib > 20 {
			r.rate("aws/fargate/ephemeral/gb", hours(tasks*(gib-20)), fmt.Sprintf("Extra disk (%g GB above 20)", gib))
		}
	})
}

// ---- serverless, storage and messaging --------------------------------------------------------

func priceLambda(r *res) {
	mem := r.num("memory_size", 128)
	arch := "aws/lambda/"
	if l := awsList(r, "architectures"); len(l) > 0 && l[0] == "arm64" {
		arch += "arm/"
	}
	r.usage("monthly_requests", arch+"request", 1e-6, "Requests")
	secs := awsUsageNum(r, "monthly_requests") * awsUsageNum(r, "avg_duration_ms") / 1000
	awsUsageLine(r, arch+"gb-second", secs*mem/1024, fmt.Sprintf("Compute (%g MB, GB-seconds)", mem), "monthly_requests", "avg_duration_ms")
	if eph := r.num("ephemeral_storage.size", 512); eph > 512 {
		awsUsageLine(r, "aws/lambda/ephemeral/gb-second", secs*(eph-512)/1024, fmt.Sprintf("Extra disk (%g MB, GB-seconds)", eph), "monthly_requests", "avg_duration_ms")
	}
}

func priceS3(r *res) {
	r.usage("storage_gb", "aws/s3/standard/storage", 1, "Storage (Standard, GB)")
	r.usage("monthly_put_requests", "aws/s3/put", 1e-6, "PUT, COPY, POST and LIST requests")
	r.usage("monthly_get_requests", "aws/s3/get", 1e-6, "GET and other requests")
}

func priceDynamoDB(r *res) {
	pre := "aws/dynamodb/"
	if r.str("table_class", "STANDARD") == "STANDARD_INFREQUENT_ACCESS" {
		pre += "ia/"
	}
	if r.str("billing_mode", "PROVISIONED") == "PAY_PER_REQUEST" {
		r.usage("monthly_read_request_units", pre+"read-request", 1e-6, "Read request units")
		r.usage("monthly_write_request_units", pre+"write-request", 1e-6, "Write request units")
	} else {
		rcu, ok1 := awsCount(r, "read_capacity")
		wcu, ok2 := awsCount(r, "write_capacity")
		if ok1 && ok2 {
			r.rate(pre+"rcu", hours(rcu), fmt.Sprintf("Read capacity (%g units)", rcu))
			r.rate(pre+"wcu", hours(wcu), fmt.Sprintf("Write capacity (%g units)", wcu))
		}
		for i := range awsList(r, "global_secondary_index") {
			p := fmt.Sprintf("global_secondary_index.%d.", i)
			name := r.str(p+"name", fmt.Sprint(i+1))
			r.rate(pre+"rcu", hours(r.num(p+"read_capacity", 0)), "Index "+name+": read capacity")
			r.rate(pre+"wcu", hours(r.num(p+"write_capacity", 0)), "Index "+name+": write capacity")
		}
	}
	r.usage("storage_gb", pre+"storage", 1, "Storage (GB)")
	if r.flag("point_in_time_recovery.enabled", false) {
		r.usage("storage_gb", "aws/dynamodb/pitr", 1, "Point-in-time recovery (GB)")
	}
	if len(awsList(r, "replica")) > 0 {
		r.assume("global table replicas aren't priced")
	}
}

func priceSQS(r *res) {
	k := "standard"
	if r.flag("fifo_queue", false) {
		k = "fifo"
	}
	r.usage("monthly_requests", "aws/sqs/"+k+"/request", 1e-6, "Requests ("+k+")")
}

// priceSFN prices a state machine: Standard by state transition, Express by request and by
// duration × memory.
func priceSFN(r *res) {
	if r.str("type", "STANDARD") != "EXPRESS" {
		r.usage("monthly_transitions", "aws/sfn/transition", 1e-3, "State transitions (Standard)")
		return
	}
	r.usage("monthly_requests", "aws/sfn/express/request", 1e-6, "Requests (Express)")
	secs := awsUsageNum(r, "monthly_requests") * awsUsageNum(r, "avg_duration_ms") / 1000
	mem := cmp.Or(awsUsageNum(r, "avg_memory_mb"), 64) // Express bills memory in 64 MB steps; 64 MB is the least
	awsUsageLine(r, "aws/sfn/express/gb-second", secs*mem/1024, fmt.Sprintf("Duration (%g MB, GB-seconds)", mem), "monthly_requests", "avg_duration_ms")
}

func priceRESTAPI(r *res) {
	r.usage("monthly_requests", "aws/apigw/rest/request", 1e-6, "Requests (REST API)")
}

// priceAPIv2 prices an HTTP API by request, and a WebSocket API by message and connection-minute.
func priceAPIv2(r *res) {
	if r.str("protocol_type", "") == "WEBSOCKET" {
		r.usage("monthly_messages", "aws/apigw/websocket/message", 1e-6, "Messages (WebSocket)")
		r.usage("monthly_connection_minutes", "aws/apigw/websocket/minute", 1e-6, "Connection minutes (WebSocket)")
		return
	}
	r.usage("monthly_requests", "aws/apigw/http/request", 1e-6, "Requests (HTTP API)")
}

func priceSNS(r *res) {
	if r.flag("fifo_topic", false) {
		r.usage("monthly_requests", "aws/sns/fifo/request", 1e-6, "Publish requests (FIFO)")
		return
	}
	r.usage("monthly_requests", "aws/sns/request", 1e-6, "API requests")
	r.usage("monthly_http_deliveries", "aws/sns/http", 1e-6, "HTTP and HTTPS deliveries")
	r.usage("monthly_email_deliveries", "aws/sns/email", 1e-6, "Email deliveries")
	r.usage("monthly_mobile_push_deliveries", "aws/sns/mobile-push", 1e-6, "Mobile push deliveries")
}

func priceKinesis(r *res) {
	if r.str("stream_mode_details.stream_mode", "PROVISIONED") == "ON_DEMAND" {
		r.rate("aws/kinesis/ondemand/stream-hour", HoursPerMonth, "Stream hours (on-demand)")
		r.usage("monthly_gb_in", "aws/kinesis/ondemand/data-in", 1, "Data written (GB)")
		r.usage("monthly_gb_out", "aws/kinesis/ondemand/data-out", 1, "Data read (GB)")
		return
	}
	shards, ok := awsCount(r, "shard_count")
	if !ok {
		return
	}
	r.rate("aws/kinesis/shard-hour", hours(shards), fmt.Sprintf("Shard hours (%g shards)", shards))
	switch ret := r.num("retention_period", 24); {
	case ret > 168:
		r.assume("retention over 7 days adds long-term storage charges, which aren't priced")
		fallthrough
	case ret > 24:
		r.rate("aws/kinesis/extended-retention-shard-hour", hours(shards), fmt.Sprintf("Extended retention shard hours (%g hours)", ret))
	}
	r.usage("monthly_put_payload_units", "aws/kinesis/put-payload", 1e-6, "PUT payload units")
}

func priceLogGroup(r *res) {
	if r.str("log_group_class", "STANDARD") == "INFREQUENT_ACCESS" {
		r.assume("priced at the Standard class rates; Infrequent Access costs less")
	}
	r.usage("monthly_ingested_gb", "aws/cloudwatch/logs/ingest", 1, "Log data ingested (GB)")
	r.usage("storage_gb", "aws/cloudwatch/logs/storage", 1, "Log data stored (GB)")
}

func priceCloudFront(r *res) {
	r.assume("priced at the US edge rates; viewers in other places can pay more")
	r.usage("monthly_gb_out", "aws/cloudfront/data-out", 1, "Data transferred out (GB)")
	r.usage("monthly_https_requests", "aws/cloudfront/https-requests", 1e-6, "HTTPS requests")
	r.usage("monthly_http_requests", "aws/cloudfront/http-requests", 1e-6, "HTTP requests")
}

// ---- search and streaming ---------------------------------------------------------------------

func priceOpenSearch(r *res) {
	const col = "aws/opensearch/instance"
	n := r.num("cluster_config.instance_count", 1)
	t := r.size(col, "cluster_config.instance_type", "m4.large.search") // the provider's default
	r.hourly(col, t, hours(n), fmt.Sprintf("Data node hours (%g × %s)", n, t))
	if r.flag("cluster_config.dedicated_master_enabled", false) {
		mt := r.str("cluster_config.dedicated_master_type", "")
		if mt == "" {
			if !r.needs {
				r.need("cluster_config.dedicated_master_type")
			}
		} else {
			mc := 3.0
			if _, ok := r.get("cluster_config.dedicated_master_count"); ok {
				mc = r.num("cluster_config.dedicated_master_count", 3)
			} else {
				r.assume("assumed 3 dedicated master nodes (dedicated_master_count isn't set)")
			}
			r.hourly(col, mt, hours(mc), fmt.Sprintf("Dedicated master hours (%g × %s)", mc, mt))
		}
	}
	if r.flag("cluster_config.warm_enabled", false) {
		wt := r.str("cluster_config.warm_type", "")
		if wt == "" {
			if !r.needs {
				r.need("cluster_config.warm_type")
			}
		} else {
			wc := r.num("cluster_config.warm_count", 0)
			r.hourly(col, wt, hours(wc), fmt.Sprintf("UltraWarm node hours (%g × %s)", wc, wt))
		}
		r.usage("warm_storage_gb", "aws/opensearch/warm-storage", 1, "Warm storage (GB)")
	}
	if r.flag("ebs_options.ebs_enabled", false) {
		typ := r.str("ebs_options.volume_type", "gp2")
		gb, ok := awsCount(r, "ebs_options.volume_size")
		if !ok {
			return
		}
		if !slices.Contains([]string{"gp2", "gp3", "io1", "standard"}, typ) {
			r.miss("the volume type " + typ + " isn't priced")
			return
		}
		if typ == "standard" {
			typ = "magnetic"
		}
		r.rate("aws/opensearch/storage/"+typ, gb*n, fmt.Sprintf("Data node storage (%s, %g GB each)", typ, gb))
		iops := r.num("ebs_options.iops", 0)
		switch typ {
		case "gp3":
			// gp3 includes 3,000 IOPS and 125 MB/s.
			if iops > 3000 {
				r.rate("aws/opensearch/iops/gp3", (iops-3000)*n, "Extra IOPS")
			}
			if t := r.num("ebs_options.throughput", 0); t > 125 {
				r.rate("aws/opensearch/throughput/gp3", (t-125)*n, "Extra throughput")
			}
		case "io1":
			if iops <= 0 {
				r.need("ebs_options.iops")
				return
			}
			r.rate("aws/opensearch/iops/io1", iops*n, fmt.Sprintf("Provisioned IOPS (%g each)", iops))
		}
	}
}

func priceMSK(r *res) {
	n, ok := awsCount(r, "number_of_broker_nodes")
	if !ok {
		return
	}
	const col = "aws/msk/broker"
	t := r.size(col, "broker_node_group_info.instance_type", "")
	r.hourly(col, t, hours(n), fmt.Sprintf("Broker hours (%g × %s)", n, t))
	if strings.HasPrefix(t, "express.") {
		r.usage("storage_gb", "aws/msk/express-storage", 1, "Storage (GB)")
		return
	}
	const storage = "broker_node_group_info.storage_info.ebs_storage_info"
	gb := 0.0
	switch _, newer := r.get(storage + ".volume_size"); {
	case newer:
		gb = r.num(storage+".volume_size", 0)
	default:
		if _, older := r.get("broker_node_group_info.ebs_volume_size"); older {
			gb = r.num("broker_node_group_info.ebs_volume_size", 0)
		} else {
			r.assume("assumed 1000 GB of storage per broker (the volume size isn't set)")
			gb = 1000
		}
	}
	r.rate("aws/msk/storage", gb*n, fmt.Sprintf("Storage (%g GB × %g brokers)", gb, n))
	if r.flag(storage+".provisioned_throughput.enabled", false) {
		// Brokers include 250 MB/s; the rest is billed.
		if mbps := r.num(storage+".provisioned_throughput.volume_throughput", 0); mbps > 250 {
			r.rate("aws/msk/throughput", (mbps-250)*n, fmt.Sprintf("Extra throughput (%g MB/s above 250)", mbps))
		}
	}
}

// ---- security and DNS -------------------------------------------------------------------------

func priceSecret(r *res) {
	r.rate("aws/secretsmanager/secret", 1, "Secret")
	r.usage("monthly_requests", "aws/secretsmanager/request", 1e-6, "API requests")
}

func priceKMS(r *res) {
	r.rate("aws/kms/key", 1, "Key")
	if r.flag("enable_key_rotation", false) {
		awsNote(r, "key rotation adds a charge for each rotated version (not priced)")
	}
	key := "aws/kms/request"
	switch spec := r.str("customer_master_key_spec", "SYMMETRIC_DEFAULT"); {
	case spec == "SYMMETRIC_DEFAULT", strings.HasPrefix(spec, "HMAC_"):
	case spec == "RSA_2048":
		key += "/rsa2048"
	default:
		key += "/asymmetric"
	}
	r.usage("monthly_requests", key, 1e-6, "Requests")
}

func priceZone(r *res) {
	r.rate("aws/route53/zone", 1, "Hosted zone")
	awsNote(r, "priced at the rate of the first 25 zones; zones after that cost less")
	r.usage("monthly_queries", "aws/route53/query", 1e-6, "Queries")
}

// osName is how an OS is shown in a price line.
var osName = map[string]string{"linux": "Linux", "windows": "Windows"}

// awsLinuxAMIs are names in AMI lookups of Linux images that have no licence fee. RHEL and SUSE
// are left out on purpose: they have one, which pogi doesn't price.
var awsLinuxAMIs = []string{"al2023", "amzn", "amazon linux", "amazon-eks", "ubuntu", "debian", "bottlerocket",
	"fedora", "centos", "rocky", "almalinux", "flatcar"}

// awsAMIOS reads the OS from the data "aws_ami" lookup an instance or its launch template uses:
// its name_regex and name filters usually say it ("al2023-ami-*", "Windows_Server-2022-*").
// It returns "" when there is no lookup or the name doesn't tell.
func awsAMIOS(r *res) string {
	ami := r.dep("aws_ami")
	if ami == nil {
		if lt := r.dep("aws_launch_template", "aws_launch_configuration"); lt != nil {
			for _, d := range lt.Deps {
				if t := r.byBlock[d]; t != nil && t.Type == "aws_ami" {
					ami = t
					break
				}
			}
		}
	}
	if ami == nil {
		return ""
	}
	var names []string
	if s, ok := ami.Attrs["name_regex"].(string); ok {
		names = append(names, s)
	}
	filters, _ := ami.Attrs["filter"].([]any)
	for _, f := range filters {
		if f, ok := f.(map[string]any); ok && f["name"] == "name" {
			vals, _ := f["values"].([]any)
			for _, v := range vals {
				if s, ok := v.(string); ok {
					names = append(names, s)
				}
			}
		}
	}
	text := strings.ToLower(strings.Join(names, " "))
	switch {
	case text == "":
		return ""
	case strings.Contains(text, "windows"):
		return "windows"
	}
	for _, n := range awsLinuxAMIs {
		if strings.Contains(text, n) {
			return "linux"
		}
	}
	return ""
}
