package cost

import (
	"math"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/terraform"
)

type m = map[string]any
type l = []any

// awsRes builds an AWS resource in us-east-1. deps are the blocks it references.
func awsRes(typ, name string, attrs m, deps ...string) *terraform.Resource {
	b := typ + "." + name
	return &terraform.Resource{Address: b, Block: b, Type: typ, Name: name, Cloud: "aws", Region: "us-east-1", Attrs: attrs, Deps: deps}
}

// awsRows prices resources against the embedded list and returns the rows by address.
func awsRows(t *testing.T, usage map[string]map[string]any, rs ...*terraform.Resource) map[string]Row {
	t.Helper()
	e, err := Price(&terraform.Config{Resources: rs}, Options{Usage: usage})
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]Row{}
	for _, r := range e.Rows {
		out[r.Address] = r
	}
	return out
}

// want returns what the price list says a quantity of a rate costs in us-east-1.
type price struct {
	t  *testing.T
	db *DB
}

func newPrice(t *testing.T) price {
	db, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	return price{t, db}
}

func (p price) rate(key string, qty float64) float64 {
	v, _, ok := p.db.Rate(key, "us-east-1")
	if !ok {
		p.t.Fatalf("the price list has no %s in us-east-1", key)
	}
	return v * qty
}

func (p price) size(col, size string, qty float64) float64 {
	v, ok := p.db.Hourly(col, "us-east-1", size)
	if !ok {
		p.t.Fatalf("the price list has no %s in %s", size, col)
	}
	return v * qty
}

func near(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-6*math.Max(1, math.Abs(want)) {
		t.Errorf("%s: got %.6f, want %.6f", what, got, want)
	}
}

func hasNote(r Row, sub string) bool {
	for _, n := range r.Notes {
		if strings.Contains(n, sub) {
			return true
		}
	}
	return false
}

func TestAWSMapsEveryType(t *testing.T) {
	for _, typ := range []string{"aws_instance", "aws_ebs_volume", "aws_eip", "aws_nat_gateway", "aws_lb", "aws_alb", "aws_db_instance",
		"aws_rds_cluster", "aws_rds_cluster_instance", "aws_elasticache_cluster", "aws_elasticache_replication_group", "aws_eks_cluster",
		"aws_eks_node_group", "aws_ecs_service", "aws_autoscaling_group", "aws_lambda_function", "aws_s3_bucket", "aws_dynamodb_table",
		"aws_sqs_queue", "aws_sns_topic", "aws_kinesis_stream", "aws_cloudwatch_log_group", "aws_cloudfront_distribution",
		"aws_opensearch_domain", "aws_msk_cluster", "aws_secretsmanager_secret", "aws_kms_key", "aws_route53_zone", "aws_vpc_endpoint"} {
		if _, ok := mappings[typ]; !ok {
			t.Errorf("%s has no mapping", typ)
		}
	}
	rows := awsRows(t, nil, awsRes("aws_vpc", "main", nil), awsRes("aws_subnet", "a", nil), awsRes("aws_iam_role", "r", nil), awsRes("aws_security_group", "sg", nil))
	for a, r := range rows {
		if r.Status != Free {
			t.Errorf("%s is %s, want free", a, r.Status)
		}
	}
}

func TestAWSSizeColumns(t *testing.T) {
	if c, ok := SizeColumn("aws_instance", "windows"); !ok || c != "aws/ec2/windows" {
		t.Errorf("aws_instance windows: %q %v", c, ok)
	}
	if _, ok := SizeColumn("aws_s3_bucket", ""); ok {
		t.Error("a bucket has no sizes")
	}
}

func TestAWSInstance(t *testing.T) {
	p := newPrice(t)
	// Everything the files say: hours, a 50 GB gp3 root disk, one 100 GB gp3 disk with extra IOPS and
	// throughput, detailed monitoring and a public address.
	full := awsRes("aws_instance", "full", m{
		"instance_type": "m5.large", "monitoring": true, "associate_public_ip_address": true,
		"root_block_device": l{m{"volume_size": 50.0, "volume_type": "gp3"}},
		"ebs_block_device":  l{m{"volume_size": 100.0, "volume_type": "gp3", "iops": 5000.0, "throughput": 200.0}},
	})
	// Only the type: the root disk and the public address are assumed.
	bare := awsRes("aws_instance", "bare", m{"instance_type": "t3.micro"})
	win := awsRes("aws_instance", "win", m{"instance_type": "m5.large", "root_block_device": l{m{"volume_size": 30.0, "volume_type": "gp3"}}, "associate_public_ip_address": false})
	rows := awsRows(t, map[string]map[string]any{"aws_instance.win": {"os": "windows"}, "aws_instance.full": {"os": "linux"}}, full, bare, win)

	near(t, "full", rows["aws_instance.full"].Monthly,
		p.size("aws/ec2/linux", "m5.large", 730)+p.rate("aws/ebs/gp3/storage", 150)+p.rate("aws/ebs/gp3/iops", 2000)+
			p.rate("aws/ebs/gp3/throughput", 75)+p.rate("aws/cloudwatch/metric", 7)+p.rate("aws/ipv4/hour", 730))
	if s := rows["aws_instance.full"].Status; s != Known {
		t.Errorf("full: status %s, notes %v", s, rows["aws_instance.full"].Notes)
	}
	b := rows["aws_instance.bare"]
	near(t, "bare", b.Monthly, p.size("aws/ec2/linux", "t3.micro", 730)+p.rate("aws/ebs/gp3/storage", 8))
	if b.Status != Assumed || !hasNote(b, "Linux") || !hasNote(b, "root disk") || !hasNote(b, "public IPv4") {
		t.Errorf("bare: %s %v", b.Status, b.Notes)
	}
	if b.Sizes == nil || b.Sizes.Column != "aws/ec2/linux" || b.Sizes.Attr != "instance_type" {
		t.Errorf("bare sizes: %+v", b.Sizes)
	}
	near(t, "windows", rows["aws_instance.win"].Monthly, p.size("aws/ec2/windows", "m5.large", 730)+p.rate("aws/ebs/gp3/storage", 30))
	if rows["aws_instance.win"].Sizes.Column != "aws/ec2/windows" {
		t.Errorf("windows picks sizes from %s", rows["aws_instance.win"].Sizes.Column)
	}
}

func TestAWSInstanceFailures(t *testing.T) {
	big := awsRes("aws_instance", "big", m{"instance_type": "z9.huge"})
	other := awsRes("aws_instance", "other", m{"instance_type": "t3.micro"})
	other.Region = "xx-nowhere-1"
	none := awsRes("aws_instance", "none", m{})
	unknown := awsRes("aws_instance", "unknown", m{"instance_type": terraform.Unknown{}})
	solaris := awsRes("aws_instance", "solaris", m{"instance_type": "t3.micro"})
	// An unknown size with a default: the root disk size isn't known, so 8 GB is assumed and said.
	rootUnknown := awsRes("aws_instance", "rootunknown", m{"instance_type": "t3.micro", "associate_public_ip_address": false,
		"root_block_device": l{m{"volume_size": terraform.Unknown{}, "volume_type": "gp3"}}})
	iops := awsRes("aws_instance", "iops", m{"instance_type": "t3.micro", "ebs_block_device": l{m{"volume_size": 10.0, "volume_type": "io1"}}})
	badType := awsRes("aws_instance", "badtype", m{"instance_type": "t3.micro", "ebs_block_device": l{m{"volume_size": 10.0, "volume_type": "fancy"}}})
	rows := awsRows(t, map[string]map[string]any{"aws_instance.solaris": {"os": "solaris"}}, big, other, none, unknown, solaris, rootUnknown, iops, badType)

	for addr, sub := range map[string]string{
		"aws_instance.big": "no price for z9.huge", "aws_instance.other": "no price for t3.micro in xx-nowhere-1",
		"aws_instance.none": "needs a value for instance_type", "aws_instance.unknown": "needs a value for instance_type",
		"aws_instance.solaris": "solaris", "aws_instance.iops": "iops", "aws_instance.badtype": "fancy",
	} {
		if r := rows[addr]; r.Status != Needs || !hasNote(r, sub) {
			t.Errorf("%s: status %s, notes %v; want needs and %q", addr, r.Status, r.Notes, sub)
		}
	}
	r := rows["aws_instance.rootunknown"]
	if r.Status != Assumed || !hasNote(r, "volume_size") {
		t.Errorf("rootunknown: %s %v", r.Status, r.Notes)
	}
}

func TestAWSEBSEIPNAT(t *testing.T) {
	p := newPrice(t)
	rows := awsRows(t, map[string]map[string]any{"aws_nat_gateway.n": {"monthly_gb_processed": 200.0}},
		awsRes("aws_ebs_volume", "v", m{"size": 100.0, "type": "io2", "iops": 4000.0}),
		awsRes("aws_ebs_volume", "snap", m{"snapshot_id": "snap-1"}),
		awsRes("aws_ebs_volume", "nosize", m{}),
		awsRes("aws_eip", "e", m{}),
		awsRes("aws_nat_gateway", "n", m{}),
		awsRes("aws_nat_gateway", "idle", m{}))
	near(t, "io2", rows["aws_ebs_volume.v"].Monthly, p.rate("aws/ebs/io2/storage", 100)+p.rate("aws/ebs/io2/iops", 4000))
	near(t, "snapshot volume", rows["aws_ebs_volume.snap"].Monthly, p.rate("aws/ebs/gp2/storage", 8))
	if r := rows["aws_ebs_volume.snap"]; r.Status != Assumed {
		t.Errorf("snapshot volume: %s", r.Status)
	}
	if r := rows["aws_ebs_volume.nosize"]; r.Status != Needs || !hasNote(r, "size") {
		t.Errorf("nosize: %s %v", r.Status, r.Notes)
	}
	near(t, "eip", rows["aws_eip.e"].Monthly, p.rate("aws/ipv4/hour", 730))
	near(t, "nat with usage", rows["aws_nat_gateway.n"].Monthly, p.rate("aws/natgw/hour", 730)+p.rate("aws/natgw/data", 200))
	if r := rows["aws_nat_gateway.idle"]; r.Status != Usage && r.Status != Known {
		t.Errorf("nat without usage: %s", r.Status)
	}
	if r := rows["aws_nat_gateway.idle"]; len(r.UsageKeys) != 1 || r.UsageKeys[0] != "monthly_gb_processed" {
		t.Errorf("nat usage keys: %v", r.UsageKeys)
	}
}

func TestAWSLoadBalancerAndEndpoint(t *testing.T) {
	p := newPrice(t)
	rows := awsRows(t, map[string]map[string]any{"aws_lb.app": {"monthly_lcu_hours": 1000.0}, "aws_vpc_endpoint.i": {"monthly_gb_processed": 50.0}},
		awsRes("aws_lb", "app", m{}),
		awsRes("aws_lb", "net", m{"load_balancer_type": "network"}),
		awsRes("aws_lb", "odd", m{"load_balancer_type": "classic"}),
		awsRes("aws_alb", "old", m{}),
		awsRes("aws_vpc_endpoint", "g", m{"vpc_endpoint_type": "Gateway"}),
		awsRes("aws_vpc_endpoint", "i", m{"vpc_endpoint_type": "Interface", "subnet_ids": l{"a", "b", "c"}}),
		awsRes("aws_vpc_endpoint", "unk", m{"vpc_endpoint_type": "Interface", "subnet_ids": terraform.Unknown{}}))
	near(t, "app", rows["aws_lb.app"].Monthly, p.rate("aws/elb/application/hour", 730)+p.rate("aws/elb/application/lcu", 1000))
	near(t, "net", rows["aws_lb.net"].Monthly, p.rate("aws/elb/network/hour", 730))
	if r := rows["aws_lb.odd"]; r.Status != Needs {
		t.Errorf("odd: %s", r.Status)
	}
	near(t, "alb alias", rows["aws_alb.old"].Monthly, p.rate("aws/elb/application/hour", 730))
	if r := rows["aws_vpc_endpoint.g"]; r.Monthly != 0 || r.Status == Needs {
		t.Errorf("gateway endpoint: %+v", r)
	}
	near(t, "interface endpoint", rows["aws_vpc_endpoint.i"].Monthly, p.rate("aws/vpce/hour", 3*730)+p.rate("aws/vpce/data", 50))
	if r := rows["aws_vpc_endpoint.unk"]; r.Status != Assumed || !hasNote(r, "1 subnet") {
		t.Errorf("unknown subnets: %s %v", r.Status, r.Notes)
	}
}

func TestAWSDatabases(t *testing.T) {
	p := newPrice(t)
	rows := awsRows(t, map[string]map[string]any{"aws_rds_cluster.c": {"storage_gb": 100.0, "monthly_io_requests": 5e6}},
		awsRes("aws_db_instance", "multi", m{"engine": "postgres", "instance_class": "db.m5.large", "multi_az": true, "allocated_storage": 100.0, "storage_type": "gp3", "iops": 6000.0, "storage_throughput": 200.0}),
		awsRes("aws_db_instance", "big", m{"engine": "mysql", "instance_class": "db.m5.large", "allocated_storage": 500.0, "storage_type": "gp3", "iops": 15000.0}),
		awsRes("aws_db_instance", "oracle", m{"engine": "oracle-se2", "instance_class": "db.m5.large", "allocated_storage": 20.0}),
		awsRes("aws_db_instance", "nostore", m{"engine": "mysql", "instance_class": "db.m5.large"}),
		awsRes("aws_db_instance", "noeng", m{"instance_class": "db.m5.large", "allocated_storage": 20.0}),
		awsRes("aws_db_instance", "nosize", m{"engine": "mysql", "allocated_storage": 20.0}),
		awsRes("aws_db_instance", "io1", m{"engine": "mariadb", "instance_class": "db.m5.large", "allocated_storage": 100.0, "iops": 1000.0}),
		awsRes("aws_rds_cluster", "c", m{"engine": "aurora-postgresql"}),
		awsRes("aws_rds_cluster", "opt", m{"engine": "aurora-mysql", "storage_type": "aurora-iopt1"}),
		awsRes("aws_rds_cluster", "pg", m{"engine": "postgres"}),
		awsRes("aws_rds_cluster_instance", "w", m{"engine": "aurora-postgresql", "instance_class": "db.r6g.large"}, "aws_rds_cluster.c"),
		awsRes("aws_rds_cluster_instance", "opt", m{"engine": "aurora-mysql", "instance_class": "db.r6g.large"}, "aws_rds_cluster.opt"),
		awsRes("aws_rds_cluster_instance", "sv2", m{"engine": "aurora-postgresql", "instance_class": "db.serverless"}, "aws_rds_cluster.sv2"),
		awsRes("aws_rds_cluster_instance", "lost", m{"engine": "aurora-postgresql", "instance_class": "db.serverless"}),
		awsRes("aws_rds_cluster", "sv2", m{"engine": "aurora-postgresql", "serverlessv2_scaling_configuration": l{m{"min_capacity": 0.5, "max_capacity": 4.0}}}))

	near(t, "multi-az postgres gp3", rows["aws_db_instance.multi"].Monthly, p.size("aws/rds/postgres/multi-az", "db.m5.large", 730)+
		p.rate("aws/rds/postgres/storage/gp3/multi-az", 100)+p.rate("aws/rds/postgres/iops/gp3/multi-az", 3000)+p.rate("aws/rds/postgres/throughput/gp3/multi-az", 75))
	// 500 GB gp3 includes 12,000 IOPS.
	near(t, "big mysql gp3", rows["aws_db_instance.big"].Monthly, p.size("aws/rds/mysql/single-az", "db.m5.large", 730)+p.rate("aws/rds/mysql/storage/gp3", 500)+p.rate("aws/rds/mysql/iops/gp3", 3000))
	// iops without a storage type means io1.
	near(t, "io1 mariadb", rows["aws_db_instance.io1"].Monthly, p.size("aws/rds/mariadb/single-az", "db.m5.large", 730)+p.rate("aws/rds/mariadb/storage/io1", 100)+p.rate("aws/rds/mariadb/iops/io1", 1000))
	for addr, sub := range map[string]string{
		"aws_db_instance.oracle": "oracle-se2", "aws_db_instance.nostore": "allocated_storage", "aws_db_instance.noeng": "engine",
		"aws_db_instance.nosize": "instance_class", "aws_rds_cluster.pg": "postgres", "aws_rds_cluster_instance.lost": "serverlessv2_scaling_configuration",
	} {
		if r := rows[addr]; r.Status != Needs || !hasNote(r, sub) {
			t.Errorf("%s: %s %v; want needs and %q", addr, r.Status, r.Notes, sub)
		}
	}
	near(t, "aurora cluster", rows["aws_rds_cluster.c"].Monthly, p.rate("aws/rds/aurora-postgresql/storage/standard", 100)+p.rate("aws/rds/aurora-postgresql/io", 5))
	if r := rows["aws_rds_cluster.opt"]; r.Status != Usage || len(r.Lines) != 1 || r.Lines[0].UnitPrice != p.rate("aws/rds/aurora-mysql/storage/io-optimized", 1) {
		t.Errorf("io-optimized cluster has no I/O line and the io-optimized storage rate: %+v", r)
	}
	near(t, "aurora instance", rows["aws_rds_cluster_instance.w"].Monthly, p.size("aws/rds/aurora-postgresql/standard", "db.r6g.large", 730))
	near(t, "io-optimized instance", rows["aws_rds_cluster_instance.opt"].Monthly, p.size("aws/rds/aurora-mysql/io-optimized", "db.r6g.large", 730))
	near(t, "serverless v2", rows["aws_rds_cluster_instance.sv2"].Monthly, p.rate("aws/rds/aurora-postgresql/acu/standard", 0.5*730))
	if r := rows["aws_rds_cluster_instance.sv2"]; r.Status != Assumed || !hasNote(r, "minimum of 0.5 ACUs") {
		t.Errorf("serverless v2 notes: %s %v", r.Status, r.Notes)
	}
	// The usage file's average wins over the cluster's minimum.
	rows = awsRows(t, map[string]map[string]any{"aws_rds_cluster_instance.sv2": {"avg_acus": 2.0}},
		awsRes("aws_rds_cluster_instance", "sv2", m{"engine": "aurora-postgresql", "instance_class": "db.serverless"}, "aws_rds_cluster.sv2"),
		awsRes("aws_rds_cluster", "sv2", m{"engine": "aurora-postgresql", "serverlessv2_scaling_configuration": l{m{"min_capacity": 0.5}}}))
	near(t, "serverless v2 with usage", rows["aws_rds_cluster_instance.sv2"].Monthly, p.rate("aws/rds/aurora-postgresql/acu/standard", 2*730))
}

func TestAWSCaches(t *testing.T) {
	p := newPrice(t)
	rows := awsRows(t, nil,
		awsRes("aws_elasticache_cluster", "mc", m{"engine": "memcached", "node_type": "cache.t3.micro", "num_cache_nodes": 3.0}),
		awsRes("aws_elasticache_cluster", "member", m{"replication_group_id": "rg"}),
		awsRes("aws_elasticache_cluster", "nonodes", m{"engine": "redis", "node_type": "cache.t3.micro"}),
		awsRes("aws_elasticache_cluster", "dynamo", m{"engine": "dynamodb", "node_type": "cache.t3.micro", "num_cache_nodes": 1.0}),
		awsRes("aws_elasticache_replication_group", "a", m{"node_type": "cache.m5.large", "num_cache_clusters": 2.0}),
		awsRes("aws_elasticache_replication_group", "v", m{"engine": "valkey", "node_type": "cache.m5.large", "num_node_groups": 2.0, "replicas_per_node_group": 1.0}),
		awsRes("aws_elasticache_replication_group", "none", m{"node_type": "cache.m5.large"}))
	near(t, "memcached", rows["aws_elasticache_cluster.mc"].Monthly, p.size("aws/elasticache/memcached", "cache.t3.micro", 3*730))
	if r := rows["aws_elasticache_cluster.member"]; r.Monthly != 0 || r.Status != Known {
		t.Errorf("member: %+v", r)
	}
	if r := rows["aws_elasticache_cluster.nonodes"]; r.Status != Needs || !hasNote(r, "num_cache_nodes") {
		t.Errorf("nonodes: %s %v", r.Status, r.Notes)
	}
	if r := rows["aws_elasticache_cluster.dynamo"]; r.Status != Needs {
		t.Errorf("unknown engine: %s", r.Status)
	}
	near(t, "redis group", rows["aws_elasticache_replication_group.a"].Monthly, p.size("aws/elasticache/redis", "cache.m5.large", 2*730))
	near(t, "valkey group", rows["aws_elasticache_replication_group.v"].Monthly, p.size("aws/elasticache/valkey", "cache.m5.large", 4*730))
	if r := rows["aws_elasticache_replication_group.none"]; r.Status != Needs {
		t.Errorf("none: %s", r.Status)
	}
}

func TestAWSContainers(t *testing.T) {
	p := newPrice(t)
	rows := awsRows(t, nil,
		awsRes("aws_eks_cluster", "c", m{}),
		awsRes("aws_eks_node_group", "ng", m{"scaling_config": l{m{"desired_size": 3.0, "min_size": 1.0}}, "instance_types": l{"t3.medium", "t3.large"}, "capacity_type": "SPOT"}),
		awsRes("aws_eks_node_group", "nonum", m{}),
		awsRes("aws_ecs_task_definition", "app", m{"cpu": "512", "memory": "1024"}),
		awsRes("aws_ecs_task_definition", "arm", m{"cpu": "1024", "memory": "2048", "runtime_platform": l{m{"cpu_architecture": "ARM64"}}, "ephemeral_storage": l{m{"size_in_gib": 50.0}}}),
		awsRes("aws_ecs_task_definition", "win", m{"cpu": "1024", "memory": "2048", "runtime_platform": l{m{"operating_system_family": "WINDOWS_SERVER_2022_CORE"}}}),
		awsRes("aws_ecs_service", "app", m{"launch_type": "FARGATE", "desired_count": 2.0}, "aws_ecs_task_definition.app"),
		awsRes("aws_ecs_service", "arm", m{"launch_type": "FARGATE", "desired_count": 1.0}, "aws_ecs_task_definition.arm"),
		awsRes("aws_ecs_service", "win", m{"launch_type": "FARGATE", "desired_count": 1.0}, "aws_ecs_task_definition.win"),
		awsRes("aws_ecs_service", "ec2", m{"launch_type": "EC2", "desired_count": 2.0}, "aws_ecs_task_definition.app"),
		awsRes("aws_ecs_service", "lost", m{"launch_type": "FARGATE", "desired_count": 2.0}),
		awsRes("aws_ecs_service", "spot", m{"capacity_provider_strategy": l{m{"capacity_provider": "FARGATE_SPOT"}}, "desired_count": 1.0}, "aws_ecs_task_definition.app"))
	near(t, "eks cluster", rows["aws_eks_cluster.c"].Monthly, p.rate("aws/eks/cluster/hour", 730))
	ng := rows["aws_eks_node_group.ng"]
	near(t, "node group", ng.Monthly, p.size("aws/ec2/linux", "t3.medium", 3*730)+p.rate("aws/ebs/gp3/storage", 60))
	if ng.Status != Assumed || !hasNote(ng, "Spot") || !hasNote(ng, "t3.medium only") {
		t.Errorf("node group: %s %v", ng.Status, ng.Notes)
	}
	if r := rows["aws_eks_node_group.nonum"]; r.Status != Needs {
		t.Errorf("nonum: %s", r.Status)
	}
	// Default size: t3.medium, 20 GB disks, named in the picker.
	if ng.Sizes == nil || ng.Sizes.Attr != "instance_types.0" || ng.Sizes.File != "t3.medium" {
		t.Errorf("node group sizes: %+v", ng.Sizes)
	}
	near(t, "fargate", rows["aws_ecs_service.app"].Monthly, p.rate("aws/fargate/linux/vcpu", 2*0.5*730)+p.rate("aws/fargate/linux/gb", 2*1*730))
	near(t, "fargate arm with extra disk", rows["aws_ecs_service.arm"].Monthly,
		p.rate("aws/fargate/arm/vcpu", 730)+p.rate("aws/fargate/arm/gb", 2*730)+p.rate("aws/fargate/ephemeral/gb", 30*730))
	near(t, "fargate windows", rows["aws_ecs_service.win"].Monthly,
		p.rate("aws/fargate/windows/vcpu", 730)+p.rate("aws/fargate/windows/os", 730)+p.rate("aws/fargate/windows/gb", 2*730))
	if r := rows["aws_ecs_service.ec2"]; r.Monthly != 0 || r.Status == Needs {
		t.Errorf("ec2 service: %+v", r)
	}
	if r := rows["aws_ecs_service.lost"]; r.Status != Needs || !hasNote(r, "task_definition") {
		t.Errorf("lost: %s %v", r.Status, r.Notes)
	}
	if r := rows["aws_ecs_service.spot"]; r.Status != Assumed || !hasNote(r, "FARGATE_SPOT") {
		t.Errorf("spot: %s %v", r.Status, r.Notes)
	}
}

func TestAWSAutoScalingGroup(t *testing.T) {
	p := newPrice(t)
	rows := awsRows(t, map[string]map[string]any{"aws_autoscaling_group.win": {"os": "windows"}},
		awsRes("aws_launch_template", "web", m{"instance_type": "t3.small", "block_device_mappings": l{m{"device_name": "/dev/xvda", "ebs": l{m{"volume_size": 30.0, "volume_type": "gp3"}}}}, "monitoring": l{m{"enabled": true}}}),
		awsRes("aws_launch_configuration", "old", m{"instance_type": "t3.small", "root_block_device": l{m{"volume_size": 20.0, "volume_type": "gp2"}}, "enable_monitoring": false}),
		awsRes("aws_autoscaling_group", "web", m{"desired_capacity": 3.0, "min_size": 1.0}, "aws_launch_template.web"),
		awsRes("aws_autoscaling_group", "min", m{"min_size": 2.0}, "aws_launch_configuration.old"),
		awsRes("aws_autoscaling_group", "win", m{"desired_capacity": 1.0}, "aws_launch_template.web"),
		awsRes("aws_autoscaling_group", "lost", m{"desired_capacity": 3.0}),
		awsRes("aws_autoscaling_group", "nosize", m{}, "aws_launch_template.web"),
		awsRes("aws_autoscaling_group", "mixed", m{"desired_capacity": 1.0, "mixed_instances_policy": l{m{"launch_template": l{m{"override": l{m{"instance_type": "t3.large"}, m{"instance_type": "t3.xlarge"}}}}}}}, "aws_launch_template.web"))
	near(t, "group from a template", rows["aws_autoscaling_group.web"].Monthly,
		p.size("aws/ec2/linux", "t3.small", 3*730)+p.rate("aws/ebs/gp3/storage", 90)+p.rate("aws/cloudwatch/metric", 21))
	if r := rows["aws_autoscaling_group.web"]; r.Sizes == nil || r.Sizes.File != "t3.small" {
		t.Errorf("sizes: %+v", r.Sizes)
	}
	near(t, "group from a launch configuration", rows["aws_autoscaling_group.min"].Monthly,
		p.size("aws/ec2/linux", "t3.small", 2*730)+p.rate("aws/ebs/gp2/storage", 40))
	near(t, "windows group", rows["aws_autoscaling_group.win"].Monthly, p.size("aws/ec2/windows", "t3.small", 730)+p.rate("aws/ebs/gp3/storage", 30)+p.rate("aws/cloudwatch/metric", 7))
	near(t, "mixed", rows["aws_autoscaling_group.mixed"].Monthly, p.size("aws/ec2/linux", "t3.large", 730)+p.rate("aws/ebs/gp3/storage", 30)+p.rate("aws/cloudwatch/metric", 7))
	for _, a := range []string{"aws_autoscaling_group.lost", "aws_autoscaling_group.nosize"} {
		if r := rows[a]; r.Status != Needs {
			t.Errorf("%s: %s %v", a, r.Status, r.Notes)
		}
	}
	// What-if: another size changes the group's price.
	e, _ := Price(&terraform.Config{Resources: []*terraform.Resource{
		awsRes("aws_launch_template", "web", m{"instance_type": "t3.small"}),
		awsRes("aws_autoscaling_group", "web", m{"desired_capacity": 1.0}, "aws_launch_template.web"),
	}}, Options{Sizes: map[string]string{"aws_autoscaling_group.web": "t3.xlarge"}})
	if r := e.Rows[1]; r.Sizes == nil || r.Sizes.Current != "t3.xlarge" || r.Sizes.File != "t3.small" || r.Lines[0].UnitPrice != p.size("aws/ec2/linux", "t3.xlarge", 1) {
		t.Errorf("what-if: %+v", r)
	}
}

func TestAWSUsagePriced(t *testing.T) {
	p := newPrice(t)
	usage := map[string]map[string]any{
		"aws_lambda_function.fn":        {"monthly_requests": 2e6, "avg_duration_ms": 100.0},
		"aws_lambda_function.arm":       {"monthly_requests": 1e6, "avg_duration_ms": 200.0},
		"aws_s3_bucket.b":               {"storage_gb": 500.0, "monthly_put_requests": 1e6, "monthly_get_requests": 4e6},
		"aws_dynamodb_table.od":         {"monthly_read_request_units": 10e6, "monthly_write_request_units": 2e6, "storage_gb": 10.0},
		"aws_sqs_queue.f":               {"monthly_requests": 3e6},
		"aws_sns_topic.t":               {"monthly_requests": 2e6, "monthly_http_deliveries": 1e6},
		"aws_kinesis_stream.od":         {"monthly_gb_in": 100.0, "monthly_gb_out": 50.0},
		"aws_cloudwatch_log_group.g":    {"monthly_ingested_gb": 20.0, "storage_gb": 100.0},
		"aws_cloudfront_distribution.d": {"monthly_gb_out": 1000.0, "monthly_https_requests": 5e6},
		"aws_secretsmanager_secret.s":   {"monthly_requests": 20000.0},
		"aws_kms_key.rsa":               {"monthly_requests": 1e6},
		"aws_route53_zone.z":            {"monthly_queries": 3e6},
	}
	rows := awsRows(t, usage,
		awsRes("aws_lambda_function", "fn", m{"memory_size": 512.0, "ephemeral_storage": l{m{"size": 2048.0}}}),
		awsRes("aws_lambda_function", "arm", m{"architectures": l{"arm64"}}),
		awsRes("aws_lambda_function", "idle", m{}),
		awsRes("aws_s3_bucket", "b", m{}),
		awsRes("aws_dynamodb_table", "od", m{"billing_mode": "PAY_PER_REQUEST", "table_class": "STANDARD_INFREQUENT_ACCESS", "point_in_time_recovery": l{m{"enabled": true}}}),
		awsRes("aws_dynamodb_table", "prov", m{"read_capacity": 10.0, "write_capacity": 5.0, "global_secondary_index": l{m{"name": "by_user", "read_capacity": 4.0, "write_capacity": 2.0}}}),
		awsRes("aws_dynamodb_table", "noprov", m{}),
		awsRes("aws_sqs_queue", "f", m{"fifo_queue": true}),
		awsRes("aws_sns_topic", "t", m{}),
		awsRes("aws_kinesis_stream", "od", m{"stream_mode_details": l{m{"stream_mode": "ON_DEMAND"}}}),
		awsRes("aws_kinesis_stream", "prov", m{"shard_count": 2.0, "retention_period": 48.0}),
		awsRes("aws_kinesis_stream", "long", m{"shard_count": 1.0, "retention_period": 400.0}),
		awsRes("aws_kinesis_stream", "noshards", m{}),
		awsRes("aws_cloudwatch_log_group", "g", m{"log_group_class": "INFREQUENT_ACCESS"}),
		awsRes("aws_cloudfront_distribution", "d", m{}),
		awsRes("aws_secretsmanager_secret", "s", m{}),
		awsRes("aws_kms_key", "rsa", m{"customer_master_key_spec": "RSA_2048", "enable_key_rotation": true}),
		awsRes("aws_kms_key", "sym", m{}),
		awsRes("aws_route53_zone", "z", m{}))

	// 2M requests × 100 ms × 0.5 GB = 100,000 GB-seconds, and 1.5 GB more disk for the same seconds.
	near(t, "lambda", rows["aws_lambda_function.fn"].Monthly, p.rate("aws/lambda/request", 2)+p.rate("aws/lambda/gb-second", 100000)+p.rate("aws/lambda/ephemeral/gb-second", 2e6*0.1*1.5))
	// 1M requests × 200 ms × 0.125 GB (the default 128 MB) = 25,000 GB-seconds, on the Arm prices.
	near(t, "lambda arm", rows["aws_lambda_function.arm"].Monthly, p.rate("aws/lambda/arm/request", 1)+p.rate("aws/lambda/arm/gb-second", 25000))
	if r := rows["aws_lambda_function.idle"]; r.Status != Usage || r.Monthly != 0 || len(r.Lines) != 2 {
		t.Errorf("lambda without usage: %+v", r)
	}
	near(t, "s3", rows["aws_s3_bucket.b"].Monthly, p.rate("aws/s3/standard/storage", 500)+p.rate("aws/s3/put", 1)+p.rate("aws/s3/get", 4))
	near(t, "dynamodb on demand, infrequent access", rows["aws_dynamodb_table.od"].Monthly,
		p.rate("aws/dynamodb/ia/read-request", 10)+p.rate("aws/dynamodb/ia/write-request", 2)+p.rate("aws/dynamodb/ia/storage", 10)+p.rate("aws/dynamodb/pitr", 10))
	near(t, "dynamodb provisioned", rows["aws_dynamodb_table.prov"].Monthly, p.rate("aws/dynamodb/rcu", 14*730)+p.rate("aws/dynamodb/wcu", 7*730))
	if r := rows["aws_dynamodb_table.noprov"]; r.Status != Needs {
		t.Errorf("noprov: %s", r.Status)
	}
	near(t, "sqs fifo", rows["aws_sqs_queue.f"].Monthly, p.rate("aws/sqs/fifo/request", 3))
	near(t, "sns", rows["aws_sns_topic.t"].Monthly, p.rate("aws/sns/request", 2)+p.rate("aws/sns/http", 1))
	near(t, "kinesis on demand", rows["aws_kinesis_stream.od"].Monthly, p.rate("aws/kinesis/ondemand/stream-hour", 730)+p.rate("aws/kinesis/ondemand/data-in", 100)+p.rate("aws/kinesis/ondemand/data-out", 50))
	near(t, "kinesis provisioned", rows["aws_kinesis_stream.prov"].Monthly, p.rate("aws/kinesis/shard-hour", 2*730)+p.rate("aws/kinesis/extended-retention-shard-hour", 2*730))
	if r := rows["aws_kinesis_stream.long"]; !hasNote(r, "long-term") {
		t.Errorf("long retention: %v", r.Notes)
	}
	if r := rows["aws_kinesis_stream.noshards"]; r.Status != Needs || !hasNote(r, "shard_count") {
		t.Errorf("noshards: %s %v", r.Status, r.Notes)
	}
	near(t, "cloudwatch logs", rows["aws_cloudwatch_log_group.g"].Monthly, p.rate("aws/cloudwatch/logs/ingest", 20)+p.rate("aws/cloudwatch/logs/storage", 100))
	if r := rows["aws_cloudwatch_log_group.g"]; r.Status != Assumed {
		t.Errorf("infrequent access log group: %s", r.Status)
	}
	near(t, "cloudfront", rows["aws_cloudfront_distribution.d"].Monthly, p.rate("aws/cloudfront/data-out", 1000)+p.rate("aws/cloudfront/https-requests", 5))
	near(t, "secret", rows["aws_secretsmanager_secret.s"].Monthly, p.rate("aws/secretsmanager/secret", 1)+p.rate("aws/secretsmanager/request", 0.02))
	near(t, "rsa kms key", rows["aws_kms_key.rsa"].Monthly, p.rate("aws/kms/key", 1)+p.rate("aws/kms/request/rsa2048", 1))
	if r := rows["aws_kms_key.sym"]; r.Status != Known || r.Monthly != p.rate("aws/kms/key", 1) || len(r.UsageKeys) != 1 {
		t.Errorf("symmetric key: %+v", r)
	}
	near(t, "zone", rows["aws_route53_zone.z"].Monthly, p.rate("aws/route53/zone", 1)+p.rate("aws/route53/query", 3))
}

func TestAWSSearchAndStreaming(t *testing.T) {
	p := newPrice(t)
	rows := awsRows(t, map[string]map[string]any{"aws_opensearch_domain.w": {"warm_storage_gb": 100.0}},
		awsRes("aws_opensearch_domain", "d", m{
			"cluster_config": l{m{"instance_type": "r6g.large.search", "instance_count": 3.0, "dedicated_master_enabled": true, "dedicated_master_type": "r6g.large.search", "dedicated_master_count": 3.0}},
			"ebs_options":    l{m{"ebs_enabled": true, "volume_type": "gp3", "volume_size": 100.0, "iops": 4000.0, "throughput": 250.0}},
		}),
		awsRes("aws_opensearch_domain", "w", m{
			"cluster_config": l{m{"instance_type": "r6g.large.search", "warm_enabled": true, "warm_type": "ultrawarm1.medium.search", "warm_count": 2.0, "dedicated_master_enabled": true, "dedicated_master_type": "r6g.large.search"}},
		}),
		awsRes("aws_opensearch_domain", "noebs", m{"cluster_config": l{m{"instance_type": "r6g.large.search"}}, "ebs_options": l{m{"ebs_enabled": true}}}),
		awsRes("aws_msk_cluster", "k", m{"number_of_broker_nodes": 3.0, "broker_node_group_info": l{m{"instance_type": "kafka.m5.large", "storage_info": l{m{"ebs_storage_info": l{m{"volume_size": 500.0, "provisioned_throughput": l{m{"enabled": true, "volume_throughput": 300.0}}}}}}}}}),
		awsRes("aws_msk_cluster", "old", m{"number_of_broker_nodes": 2.0, "broker_node_group_info": l{m{"instance_type": "kafka.t3.small", "ebs_volume_size": 100.0}}}),
		awsRes("aws_msk_cluster", "nostore", m{"number_of_broker_nodes": 2.0, "broker_node_group_info": l{m{"instance_type": "kafka.t3.small"}}}),
		awsRes("aws_msk_cluster", "nobrokers", m{"broker_node_group_info": l{m{"instance_type": "kafka.t3.small"}}}),
		awsRes("aws_msk_cluster", "badtype", m{"number_of_broker_nodes": 2.0, "broker_node_group_info": l{m{"instance_type": "kafka.zz.large"}}}))
	near(t, "opensearch", rows["aws_opensearch_domain.d"].Monthly, p.size("aws/opensearch/instance", "r6g.large.search", 6*730)+
		p.rate("aws/opensearch/storage/gp3", 300)+p.rate("aws/opensearch/iops/gp3", 3000)+p.rate("aws/opensearch/throughput/gp3", 375))
	w := rows["aws_opensearch_domain.w"]
	near(t, "opensearch warm", w.Monthly, p.size("aws/opensearch/instance", "r6g.large.search", 730)+p.size("aws/opensearch/instance", "ultrawarm1.medium.search", 2*730)+
		p.size("aws/opensearch/instance", "r6g.large.search", 3*730)+p.rate("aws/opensearch/warm-storage", 100))
	if !hasNote(w, "3 dedicated master") {
		t.Errorf("warm notes: %v", w.Notes)
	}
	if r := rows["aws_opensearch_domain.noebs"]; r.Status != Needs || !hasNote(r, "volume_size") {
		t.Errorf("noebs: %s %v", r.Status, r.Notes)
	}
	near(t, "msk", rows["aws_msk_cluster.k"].Monthly, p.size("aws/msk/broker", "kafka.m5.large", 3*730)+p.rate("aws/msk/storage", 1500)+p.rate("aws/msk/throughput", 150))
	near(t, "msk older volume attribute", rows["aws_msk_cluster.old"].Monthly, p.size("aws/msk/broker", "kafka.t3.small", 2*730)+p.rate("aws/msk/storage", 200))
	if r := rows["aws_msk_cluster.nostore"]; r.Status != Assumed || !hasNote(r, "1000 GB") {
		t.Errorf("nostore: %s %v", r.Status, r.Notes)
	}
	for _, a := range []string{"aws_msk_cluster.nobrokers", "aws_msk_cluster.badtype"} {
		if r := rows[a]; r.Status != Needs {
			t.Errorf("%s: %s %v", a, r.Status, r.Notes)
		}
	}
}

// Every type that picks a size must offer sizes that the price list actually sells in us-east-1.
func TestAWSSizeColumnsHavePrices(t *testing.T) {
	db, err := Default()
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range SizeTypes() {
		if !strings.HasPrefix(typ, "aws_") {
			continue
		}
		col, _ := SizeColumn(typ, "linux")
		if len(db.Sizes(col, "us-east-1")) < 5 {
			t.Errorf("%s: the column %s has fewer than 5 sizes in us-east-1", typ, col)
		}
	}
}

// The OS comes from the AMI lookup when its name says it; anything else stays "assumed Linux".
func TestAWSOSFromAMILookup(t *testing.T) {
	ami := func(name, pattern string) *terraform.Resource {
		return &terraform.Resource{Address: "data.aws_ami." + name, Block: "data.aws_ami." + name, Type: "aws_ami", Cloud: "aws", Data: true,
			Attrs: m{"filter": l{m{"name": "name", "values": l{pattern}}}}}
	}
	inst := func(name, amiName string) *terraform.Resource {
		r := awsRes("aws_instance", name, m{"instance_type": "m5.large", "root_block_device": l{m{"volume_size": 30.0, "volume_type": "gp3"}}, "associate_public_ip_address": false})
		r.Deps = []string{"data.aws_ami." + amiName}
		return r
	}
	lt := &terraform.Resource{Address: "aws_launch_template.lt", Block: "aws_launch_template.lt", Type: "aws_launch_template", Cloud: "aws", Region: "us-east-1",
		Attrs: m{"instance_type": "t3.small"}, Deps: []string{"data.aws_ami.win"}}
	asg := awsRes("aws_autoscaling_group", "g", m{"desired_capacity": 1.0})
	asg.Deps = []string{"aws_launch_template.lt"}
	rows := awsRows(t, nil, ami("al", "al2023-ami-*-x86_64"), ami("win", "Windows_Server-2022-English-Full-Base-*"), ami("rhel", "RHEL-9*"),
		inst("a", "al"), inst("w", "win"), inst("r", "rhel"), lt, asg)
	if r := rows["aws_instance.a"]; hasNote(r, "Linux") {
		t.Errorf("an al2023 lookup is Linux, not assumed: %+v", r.Notes)
	}
	if r := rows["aws_instance.w"]; hasNote(r, "Linux") || !strings.Contains(r.Lines[0].What, "Windows") {
		t.Errorf("a Windows_Server lookup is Windows: %+v", r)
	}
	if r := rows["aws_instance.r"]; !hasNote(r, "assumed Linux") {
		t.Errorf("RHEL has a licence fee pogi doesn't price, so it stays assumed: %+v", r.Notes)
	}
	if r := rows["aws_autoscaling_group.g"]; !strings.Contains(r.Lines[0].What, "Windows") {
		t.Errorf("a group takes the OS from its template's AMI: %+v", r.Lines)
	}
}

// Step Functions and API Gateway are priced by use. The unit prices are the us-east-1 list
// prices (checked against the AmazonStates and AmazonApiGateway price files, 2026-10-20).
func TestAWSStepFunctionsAndAPIGateway(t *testing.T) {
	db, _ := Default()
	for key, want := range map[string]float64{
		"aws/sfn/transition": 0.025, "aws/sfn/express/request": 1.00, "aws/sfn/express/gb-second": 0.00001667,
		"aws/apigw/rest/request": 3.50, "aws/apigw/http/request": 1.00, "aws/apigw/websocket/message": 1.00, "aws/apigw/websocket/minute": 0.25,
	} {
		if got, _, ok := db.Rate(key, "us-east-1"); !ok || math.Abs(got-want) > 1e-9 {
			t.Errorf("%s: %v %v, want %v", key, got, ok, want)
		}
	}
	rows := awsRows(t, map[string]map[string]any{
		"aws_sfn_state_machine.std":  {"monthly_transitions": 2e6},
		"aws_sfn_state_machine.exp":  {"monthly_requests": 1e6, "avg_duration_ms": 1000},
		"aws_api_gateway_rest_api.r": {"monthly_requests": 1e6},
		"aws_apigatewayv2_api.ws":    {"monthly_messages": 2e6, "monthly_connection_minutes": 4e6},
	},
		awsRes("aws_sfn_state_machine", "std", m{}),
		awsRes("aws_sfn_state_machine", "exp", m{"type": "EXPRESS"}),
		awsRes("aws_api_gateway_rest_api", "r", m{}),
		awsRes("aws_apigatewayv2_api", "http", m{"protocol_type": "HTTP"}),
		awsRes("aws_apigatewayv2_api", "ws", m{"protocol_type": "WEBSOCKET"}),
	)
	near(t, "standard", rows["aws_sfn_state_machine.std"].Monthly, 2000*0.025)
	near(t, "express", rows["aws_sfn_state_machine.exp"].Monthly, 1.00+1e6*1*64.0/1024*0.00001667)
	near(t, "rest", rows["aws_api_gateway_rest_api.r"].Monthly, 3.50)
	near(t, "websocket", rows["aws_apigatewayv2_api.ws"].Monthly, 2*1.00+4*0.25)
	if r := rows["aws_apigatewayv2_api.http"]; r.Status != Usage || r.UsageKeys[0] != "monthly_requests" {
		t.Errorf("an HTTP API with no usage: %+v", r)
	}
}
