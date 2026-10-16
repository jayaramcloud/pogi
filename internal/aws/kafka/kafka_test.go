package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafka/types"
	"github.com/aws/smithy-go"
	pogikafka "github.com/nrndev/pogi/internal/aws/kafka"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ctx = context.Background()

func start(t *testing.T) (*core.Hub, *pogikafka.Service, *kafka.Client) {
	var svc *pogikafka.Service
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = pogikafka.New(h); return svc })
	return h, svc, kafka.NewFromConfig(awstest.Config(t, url))
}

func wantCode(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func provisioned(name string) *kafka.CreateClusterInput {
	return &kafka.CreateClusterInput{ClusterName: aws.String(name), KafkaVersion: aws.String("3.8.x"), NumberOfBrokerNodes: aws.Int32(2),
		BrokerNodeGroupInfo: &types.BrokerNodeGroupInfo{InstanceType: aws.String("kafka.t3.small"), ClientSubnets: []string{"subnet-1", "subnet-2"},
			SecurityGroups: []string{"sg-1"}}, Tags: map[string]string{"team": "a"}}
}

func describe(t *testing.T, c *kafka.Client, arn *string) *types.ClusterInfo {
	t.Helper()
	return must(c.DescribeCluster(ctx, &kafka.DescribeClusterInput{ClusterArn: arn})).ClusterInfo
}

func TestClusters(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // metadata only
	_, svc, c := start(t)
	out, err := c.CreateCluster(ctx, provisioned("demo"))
	if err != nil || out.State != types.ClusterStateCreating || !strings.HasPrefix(*out.ClusterArn, "arn:aws:kafka:us-east-1:000000000000:cluster/demo/") {
		t.Fatalf("create %+v, %v", out, err)
	}
	arn := out.ClusterArn
	info := describe(t, c, arn)
	bn, enc := info.BrokerNodeGroupInfo, info.EncryptionInfo
	if info.State != types.ClusterStateActive || *info.NumberOfBrokerNodes != 2 || *info.CurrentBrokerSoftwareInfo.KafkaVersion != "3.8.x" ||
		bn.BrokerAZDistribution != types.BrokerAZDistributionDefault || *bn.InstanceType != "kafka.t3.small" || *bn.StorageInfo.EbsStorageInfo.VolumeSize != 1000 ||
		enc.EncryptionInTransit.ClientBroker != types.ClientBrokerTls || !*enc.EncryptionInTransit.InCluster || !strings.Contains(*enc.EncryptionAtRest.DataVolumeKMSKeyId, ":kms:") ||
		info.EnhancedMonitoring != types.EnhancedMonitoringDefault || info.StorageMode != types.StorageModeLocal || info.CreationTime == nil ||
		len(*info.CurrentVersion) != 14 || info.Tags["team"] != "a" || *info.ZookeeperConnectString == "" || info.ClientAuthentication != nil ||
		info.CurrentBrokerSoftwareInfo.ConfigurationArn != nil {
		b, _ := json.Marshal(info)
		t.Fatalf("describe %s", b)
	}
	v2 := must(c.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{ClusterArn: arn})).ClusterInfo
	if v2.ClusterType != types.ClusterTypeProvisioned || *v2.Provisioned.NumberOfBrokerNodes != 2 || v2.Serverless != nil || v2.State != types.ClusterStateActive ||
		*v2.CurrentVersion != *info.CurrentVersion {
		t.Fatalf("describe v2 %+v", v2)
	}
	bb := must(c.GetBootstrapBrokers(ctx, &kafka.GetBootstrapBrokersInput{ClusterArn: arn}))
	if !strings.HasPrefix(*bb.BootstrapBrokerString, "127.0.0.1:") || *bb.BootstrapBrokerStringTls != *bb.BootstrapBrokerString ||
		*bb.BootstrapBrokerStringSaslScram != *bb.BootstrapBrokerString || *bb.BootstrapBrokerStringSaslIam != *bb.BootstrapBrokerString {
		t.Fatalf("bootstrap brokers %+v", bb)
	}
	nodes := must(c.ListNodes(ctx, &kafka.ListNodesInput{ClusterArn: arn})).NodeInfoList
	if len(nodes) != 2 || *nodes[1].BrokerNodeInfo.BrokerId != 2 || *nodes[1].BrokerNodeInfo.ClientSubnet != "subnet-2" || nodes[0].NodeType != types.NodeTypeBroker {
		t.Fatalf("nodes %+v", nodes)
	}

	sl := must(c.CreateClusterV2(ctx, &kafka.CreateClusterV2Input{ClusterName: aws.String("lambda"),
		Serverless: &types.ServerlessRequest{VpcConfigs: []types.VpcConfig{{SubnetIds: []string{"subnet-1"}}}}}))
	if sl.ClusterType != types.ClusterTypeServerless || sl.State != types.ClusterStateCreating {
		t.Fatalf("create serverless %+v", sl)
	}
	slv := must(c.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{ClusterArn: sl.ClusterArn})).ClusterInfo
	if slv.Provisioned != nil || !*slv.Serverless.ClientAuthentication.Sasl.Iam.Enabled || slv.Serverless.VpcConfigs[0].SubnetIds[0] != "subnet-1" {
		t.Fatalf("describe serverless %+v", slv)
	}
	_, err = c.DescribeCluster(ctx, &kafka.DescribeClusterInput{ClusterArn: sl.ClusterArn})
	wantCode(t, err, "BadRequestException")
	if l := must(c.ListClusters(ctx, &kafka.ListClustersInput{})).ClusterInfoList; len(l) != 1 || *l[0].ClusterName != "demo" {
		t.Fatalf("list %+v", l)
	}
	if l := must(c.ListClustersV2(ctx, &kafka.ListClustersV2Input{})).ClusterInfoList; len(l) != 2 {
		t.Fatalf("list v2 %+v", l)
	}
	for filter, want := range map[string]int{"de": 1, "x": 0} {
		if l := must(c.ListClustersV2(ctx, &kafka.ListClustersV2Input{ClusterNameFilter: aws.String(filter)})).ClusterInfoList; len(l) != want {
			t.Fatalf("list v2 named %s* %+v", filter, l)
		}
	}
	if l := must(c.ListClustersV2(ctx, &kafka.ListClustersV2Input{ClusterTypeFilter: aws.String("SERVERLESS")})).ClusterInfoList; len(l) != 1 ||
		*l[0].ClusterName != "lambda" {
		t.Fatalf("list serverless %+v", l)
	}

	_, err = c.CreateCluster(ctx, provisioned("demo"))
	wantCode(t, err, "ConflictException")
	odd := provisioned("odd")
	odd.NumberOfBrokerNodes = aws.Int32(3)
	_, err = c.CreateCluster(ctx, odd)
	wantCode(t, err, "BadRequestException")
	_, err = c.DescribeCluster(ctx, &kafka.DescribeClusterInput{ClusterArn: aws.String(*arn + "x")})
	wantCode(t, err, "NotFoundException")

	must(c.TagResource(ctx, &kafka.TagResourceInput{ResourceArn: arn, Tags: map[string]string{"env": "dev"}}))
	must(c.UntagResource(ctx, &kafka.UntagResourceInput{ResourceArn: arn, TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &kafka.ListTagsForResourceInput{ResourceArn: arn})).Tags; len(tags) != 1 || tags["env"] != "dev" {
		t.Fatalf("tags %v", tags)
	}
	_, err = c.ListTagsForResource(ctx, &kafka.ListTagsForResourceInput{ResourceArn: aws.String("arn:aws:kafka:us-east-1:000000000000:cluster/nope/x")})
	wantCode(t, err, "NotFoundException")

	secrets := []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_alice-abc", "arn:aws:secretsmanager:us-east-1:000000000000:secret:AmazonMSK_bob-def"}
	must(c.BatchAssociateScramSecret(ctx, &kafka.BatchAssociateScramSecretInput{ClusterArn: arn, SecretArnList: secrets}))
	must(c.BatchDisassociateScramSecret(ctx, &kafka.BatchDisassociateScramSecretInput{ClusterArn: arn, SecretArnList: secrets[:1]}))
	if l := must(c.ListScramSecrets(ctx, &kafka.ListScramSecretsInput{ClusterArn: arn})).SecretArnList; !slices.Equal(l, secrets[1:]) {
		t.Fatalf("scram secrets %v", l)
	}

	vc := must(c.CreateVpcConnection(ctx, &kafka.CreateVpcConnectionInput{TargetClusterArn: arn, Authentication: aws.String("SASL_IAM"),
		VpcId: aws.String("vpc-1"), ClientSubnets: []string{"subnet-9"}, SecurityGroups: []string{"sg-9"}, Tags: map[string]string{"a": "b"}}))
	if vc.State != types.VpcConnectionStateCreating || vc.ClientSubnets[0] != "subnet-9" {
		t.Fatalf("create vpc connection %+v", vc)
	}
	dv := must(c.DescribeVpcConnection(ctx, &kafka.DescribeVpcConnectionInput{Arn: vc.VpcConnectionArn}))
	if dv.State != types.VpcConnectionStateAvailable || *dv.TargetClusterArn != *arn || dv.Subnets[0] != "subnet-9" || dv.Tags["a"] != "b" {
		t.Fatalf("describe vpc connection %+v", dv)
	}
	if l := must(c.ListVpcConnections(ctx, &kafka.ListVpcConnectionsInput{})).VpcConnections; len(l) != 1 {
		t.Fatalf("vpc connections %+v", l)
	}
	must(c.DeleteVpcConnection(ctx, &kafka.DeleteVpcConnectionInput{Arn: vc.VpcConnectionArn}))
	_, err = c.DescribeVpcConnection(ctx, &kafka.DescribeVpcConnectionInput{Arn: vc.VpcConnectionArn})
	wantCode(t, err, "NotFoundException")

	// state survives Save and Load
	b := must(svc.Save())
	svc.Reset()
	if l := must(c.ListClustersV2(ctx, &kafka.ListClustersV2Input{})).ClusterInfoList; len(l) != 0 {
		t.Fatalf("after reset %+v", l)
	}
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	if again := describe(t, c, arn); *again.CurrentVersion != *info.CurrentVersion || again.Tags["env"] != "dev" {
		t.Fatalf("after load %+v", again)
	}

	del := must(c.DeleteCluster(ctx, &kafka.DeleteClusterInput{ClusterArn: arn}))
	if del.State != types.ClusterStateDeleting {
		t.Fatalf("delete %+v", del)
	}
	_, err = c.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{ClusterArn: arn})
	wantCode(t, err, "NotFoundException")
}

func TestUpdates(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := start(t)
	arn := must(c.CreateCluster(ctx, provisioned("up"))).ClusterArn
	cfg := must(c.CreateConfiguration(ctx, &kafka.CreateConfigurationInput{Name: aws.String("up"),
		ServerProperties: []byte("auto.create.topics.enable=true\n")}))
	current := func() *string { return describe(t, c, arn).CurrentVersion }

	_, err := c.UpdateBrokerCount(ctx, &kafka.UpdateBrokerCountInput{ClusterArn: arn, CurrentVersion: aws.String("K0"), TargetNumberOfBrokerNodes: aws.Int32(4)})
	wantCode(t, err, "BadRequestException")
	first := must(c.UpdateBrokerCount(ctx, &kafka.UpdateBrokerCountInput{ClusterArn: arn, CurrentVersion: current(),
		TargetNumberOfBrokerNodes: aws.Int32(4)})).ClusterOperationArn
	must(c.UpdateBrokerType(ctx, &kafka.UpdateBrokerTypeInput{ClusterArn: arn, CurrentVersion: current(),
		TargetInstanceType: aws.String("kafka.m7g.large")}))
	must(c.UpdateBrokerStorage(ctx, &kafka.UpdateBrokerStorageInput{ClusterArn: arn, CurrentVersion: current(),
		TargetBrokerEBSVolumeInfo: []types.BrokerEBSVolumeInfo{{KafkaBrokerNodeId: aws.String("All"), VolumeSizeGB: aws.Int32(2000)}}}))
	must(c.UpdateClusterConfiguration(ctx, &kafka.UpdateClusterConfigurationInput{ClusterArn: arn, CurrentVersion: current(),
		ConfigurationInfo: &types.ConfigurationInfo{Arn: cfg.Arn, Revision: aws.Int64(1)}}))
	_, err = c.UpdateClusterConfiguration(ctx, &kafka.UpdateClusterConfigurationInput{ClusterArn: arn, CurrentVersion: current(),
		ConfigurationInfo: &types.ConfigurationInfo{Arn: cfg.Arn, Revision: aws.Int64(2)}})
	wantCode(t, err, "BadRequestException")
	must(c.UpdateClusterKafkaVersion(ctx, &kafka.UpdateClusterKafkaVersionInput{ClusterArn: arn, CurrentVersion: current(),
		TargetKafkaVersion: aws.String("3.9.x")}))

	info := describe(t, c, arn)
	if *info.NumberOfBrokerNodes != 4 || *info.BrokerNodeGroupInfo.InstanceType != "kafka.m7g.large" || *info.BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize != 2000 ||
		*info.CurrentBrokerSoftwareInfo.ConfigurationArn != *cfg.Arn || *info.CurrentBrokerSoftwareInfo.ConfigurationRevision != 1 ||
		*info.CurrentBrokerSoftwareInfo.KafkaVersion != "3.9.x" {
		b, _ := json.Marshal(info)
		t.Fatalf("after updates %s", b)
	}
	op := must(c.DescribeClusterOperation(ctx, &kafka.DescribeClusterOperationInput{ClusterOperationArn: first})).ClusterOperationInfo
	if *op.OperationState != "UPDATE_COMPLETE" || *op.OperationType != "UPDATE_BROKER_COUNT" || *op.SourceClusterInfo.NumberOfBrokerNodes != 2 ||
		*op.TargetClusterInfo.NumberOfBrokerNodes != 4 || *op.ClusterArn != *arn || op.EndTime == nil {
		t.Fatalf("operation %+v", op)
	}
	ops := must(c.ListClusterOperations(ctx, &kafka.ListClusterOperationsInput{ClusterArn: arn})).ClusterOperationInfoList
	if len(ops) != 5 || *ops[0].OperationType != "UPDATE_CLUSTER_KAFKA_VERSION" || *ops[4].OperationArn != *first {
		t.Fatalf("operations %+v", ops)
	}
	_, err = c.DescribeClusterOperation(ctx, &kafka.DescribeClusterOperationInput{ClusterOperationArn: aws.String(*first + "x")})
	wantCode(t, err, "NotFoundException")

	compat := must(c.GetCompatibleKafkaVersions(ctx, &kafka.GetCompatibleKafkaVersionsInput{ClusterArn: arn})).CompatibleKafkaVersions
	if len(compat) != 1 || *compat[0].SourceVersion != "3.9.x" || len(compat[0].TargetVersions) != 0 {
		t.Fatalf("compatible versions %+v", compat)
	}
	vs := must(c.ListKafkaVersions(ctx, &kafka.ListKafkaVersionsInput{})).KafkaVersions
	if !slices.ContainsFunc(vs, func(v types.KafkaVersion) bool {
		return *v.Version == "3.9.x.kraft" && v.Status == types.KafkaVersionStatusActive
	}) {
		t.Fatalf("versions %+v", vs)
	}
}

func TestConfigurations(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	_, _, c := start(t)
	props := []byte("auto.create.topics.enable = true\ndelete.topic.enable = true\n")
	cfg := must(c.CreateConfiguration(ctx, &kafka.CreateConfigurationInput{Name: aws.String("app"),
		KafkaVersions: []string{"3.8.x"}, ServerProperties: props, Description: aws.String("first")}))
	if *cfg.LatestRevision.Revision != 1 || cfg.State != types.ConfigurationStateActive || !strings.HasPrefix(*cfg.Arn, "arn:aws:kafka:us-east-1:000000000000:configuration/app/") {
		t.Fatalf("create %+v", cfg)
	}
	_, err := c.CreateConfiguration(ctx, &kafka.CreateConfigurationInput{Name: aws.String("app"), ServerProperties: props})
	wantCode(t, err, "ConflictException")
	d := must(c.DescribeConfiguration(ctx, &kafka.DescribeConfigurationInput{Arn: cfg.Arn}))
	if *d.Name != "app" || !slices.Equal(d.KafkaVersions, []string{"3.8.x"}) || *d.Description != "first" || d.CreationTime == nil {
		t.Fatalf("describe %+v", d)
	}
	rev := must(c.DescribeConfigurationRevision(ctx, &kafka.DescribeConfigurationRevisionInput{Arn: cfg.Arn, Revision: aws.Int64(1)}))
	if !bytes.Equal(rev.ServerProperties, props) || *rev.Arn != *cfg.Arn || *rev.Description != "first" {
		t.Fatalf("revision %+v", rev)
	}
	up := must(c.UpdateConfiguration(ctx, &kafka.UpdateConfigurationInput{Arn: cfg.Arn,
		ServerProperties: []byte("delete.topic.enable = false\n"), Description: aws.String("second")}))
	if *up.LatestRevision.Revision != 2 || *up.LatestRevision.Description != "second" {
		t.Fatalf("update %+v", up)
	}
	if revs := must(c.ListConfigurationRevisions(ctx, &kafka.ListConfigurationRevisionsInput{Arn: cfg.Arn})).Revisions; len(revs) != 2 {
		t.Fatalf("revisions %+v", revs)
	}
	if l := must(c.ListConfigurations(ctx, &kafka.ListConfigurationsInput{})).Configurations; len(l) != 1 || *l[0].LatestRevision.Revision != 2 {
		t.Fatalf("list %+v", l)
	}
	_, err = c.DescribeConfigurationRevision(ctx, &kafka.DescribeConfigurationRevisionInput{Arn: cfg.Arn, Revision: aws.Int64(3)})
	wantCode(t, err, "BadRequestException")

	in := provisioned("configured")
	in.ConfigurationInfo = &types.ConfigurationInfo{Arn: cfg.Arn, Revision: aws.Int64(2)}
	arn := must(c.CreateCluster(ctx, in)).ClusterArn
	if sw := describe(t, c, arn).CurrentBrokerSoftwareInfo; *sw.ConfigurationArn != *cfg.Arn || *sw.ConfigurationRevision != 2 {
		t.Fatalf("cluster configuration %+v", sw)
	}
	in = provisioned("misconfigured")
	in.ConfigurationInfo = &types.ConfigurationInfo{Arn: cfg.Arn, Revision: aws.Int64(3)}
	_, err = c.CreateCluster(ctx, in)
	wantCode(t, err, "BadRequestException")

	del := must(c.DeleteConfiguration(ctx, &kafka.DeleteConfigurationInput{Arn: cfg.Arn}))
	if del.State != types.ConfigurationStateDeleting {
		t.Fatalf("delete %+v", del)
	}
	_, err = c.DescribeConfiguration(ctx, &kafka.DescribeConfigurationInput{Arn: cfg.Arn})
	var bad *types.BadRequestException // Terraform takes this error, with this message, for a deleted configuration
	if !errors.As(err, &bad) || !strings.Contains(*bad.Message, "Configuration ARN does not exist") {
		t.Fatalf("describe deleted: %v", err)
	}
}

// TestBrokerInDocker runs a cluster on Redpanda when a container engine
// answers, and produces and consumes through its bootstrap brokers.
func TestBrokerInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	h, svc, c := start(t)
	name := "e2e-" + core.RandHex(3)
	arn := must(c.CreateCluster(ctx, provisioned(name))).ClusterArn
	active := func() {
		t.Helper()
		for end := time.Now().Add(5 * time.Minute); ; time.Sleep(250 * time.Millisecond) { // the first run pulls the image
			cl := must(c.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{ClusterArn: arn})).ClusterInfo
			if cl.State == types.ClusterStateActive {
				return
			}
			if cl.State != types.ClusterStateCreating || time.Now().After(end) {
				t.Fatalf("cluster %s: %s", cl.State, svc.Inspect("Clusters", name))
			}
		}
	}
	active()
	bootstrap := *must(c.GetBootstrapBrokers(ctx, &kafka.GetBootstrapBrokersInput{ClusterArn: arn})).BootstrapBrokerString
	internal, ok := h.Endpoint("kafka:" + *arn)
	if !ok || !strings.HasSuffix(internal, ":9092") {
		t.Fatalf("endpoint for containers %q, %v", internal, ok)
	}
	t.Logf("broker on %s for the host, %s for containers", bootstrap, internal)

	kc, err := kgo.NewClient(kgo.SeedBrokers(bootstrap), kgo.DefaultProduceTopic("orders"), kgo.AllowAutoTopicCreation(),
		kgo.ConsumeTopics("orders"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer kc.Close()
	tctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var records []*kgo.Record
	for i := range 10 {
		records = append(records, &kgo.Record{Value: fmt.Appendf(nil, "order-%d", i)})
	}
	if err := kc.ProduceSync(tctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	var got []string
	for len(got) < 10 {
		fs := kc.PollFetches(tctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("consume: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	if len(got) != 10 || got[0] != "order-0" || got[9] != "order-9" {
		t.Fatalf("consumed %q", got)
	}
	// the internal listener answers under the container's name on pogi's network
	container, _, _ := strings.Cut(internal, ":")
	out, code, err := d.Exec(ctx, container, "rpk", "topic", "list", "-X", "brokers="+internal)
	if err != nil || code != 0 || !strings.Contains(string(out), "orders") {
		t.Fatalf("rpk topic list over %s: %d %v\n%s", internal, code, err, out)
	}

	// persisted state starts the container again, at the same address
	b := must(svc.Save())
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	active()
	if again := *must(c.GetBootstrapBrokers(ctx, &kafka.GetBootstrapBrokersInput{ClusterArn: arn})).BootstrapBrokerString; again != bootstrap {
		t.Fatalf("bootstrap brokers after load %s, want %s", again, bootstrap)
	}

	must(c.DeleteCluster(ctx, &kafka.DeleteClusterInput{ClusterArn: arn}))
	for end := time.Now().Add(time.Minute); ; time.Sleep(250 * time.Millisecond) {
		list, err := d.List(ctx, docker.LabelService+"=kafka", docker.LabelResource+"="+name)
		if err == nil && len(list) == 0 {
			break
		}
		if time.Now().After(end) {
			t.Fatalf("container left behind: %+v, %v", list, err)
		}
	}
	if _, ok := h.Endpoint("kafka:" + *arn); ok {
		t.Fatal("endpoint left behind")
	}
}
