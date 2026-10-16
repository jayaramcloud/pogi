package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2"
	"github.com/aws/aws-sdk-go-v2/service/cloudhsmv2/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestCloudHSMv2(t *testing.T) {
	_, base := start(t, "cloudhsmv2")
	ctx := context.Background()
	c := cloudhsmv2.NewFromConfig(awstest.Config(t, base))

	cl, err := c.CreateCluster(ctx, &cloudhsmv2.CreateClusterInput{HsmType: aws.String("hsm2m.medium"), SubnetIds: []string{"subnet-a", "subnet-b"},
		TagList: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	id := aws.ToString(cl.Cluster.ClusterId)
	if id == "" || cl.Cluster.State != types.ClusterStateUninitialized || aws.ToString(cl.Cluster.HsmType) != "hsm2m.medium" || len(cl.Cluster.SubnetMapping) != 2 || cl.Cluster.SubnetMapping["us-east-1a"] != "subnet-a" {
		t.Fatalf("create: %+v", cl.Cluster)
	}
	d, err := c.DescribeClusters(ctx, &cloudhsmv2.DescribeClustersInput{Filters: map[string][]string{"clusterIds": {id}}})
	if err != nil || len(d.Clusters) != 1 || aws.ToString(d.Clusters[0].ClusterId) != id || len(d.Clusters[0].TagList) != 1 {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if d, _ = c.DescribeClusters(ctx, &cloudhsmv2.DescribeClustersInput{Filters: map[string][]string{"clusterIds": {"cluster-nope"}}}); len(d.Clusters) != 0 {
		t.Fatalf("filter by id: %+v", d.Clusters)
	}
	if d, _ = c.DescribeClusters(ctx, &cloudhsmv2.DescribeClustersInput{Filters: map[string][]string{"states": {"ACTIVE"}}}); len(d.Clusters) != 0 {
		t.Fatalf("filter by state: %+v", d.Clusters)
	}
	if d, _ = c.DescribeClusters(ctx, &cloudhsmv2.DescribeClustersInput{}); len(d.Clusters) != 1 {
		t.Fatalf("list: %+v", d.Clusters)
	}
	if m, err := c.ModifyCluster(ctx, &cloudhsmv2.ModifyClusterInput{ClusterId: aws.String(id), BackupRetentionPolicy: &types.BackupRetentionPolicy{Type: types.BackupRetentionTypeDays, Value: aws.String("30")}}); err != nil ||
		aws.ToString(m.Cluster.BackupRetentionPolicy.Value) != "30" {
		t.Fatalf("modify: %v %+v", err, m)
	}
	if _, err := c.TagResource(ctx, &cloudhsmv2.TagResourceInput{ResourceId: aws.String(id), TagList: []types.Tag{{Key: aws.String("team"), Value: aws.String("sec")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &cloudhsmv2.UntagResourceInput{ResourceId: aws.String(id), TagKeyList: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTags(ctx, &cloudhsmv2.ListTagsInput{ResourceId: aws.String(id)})
	if err != nil || len(tags.TagList) != 1 || aws.ToString(tags.TagList[0].Key) != "team" {
		t.Fatalf("tags: %v %+v", err, tags)
	}

	// HSMs: one per Availability Zone of the cluster; the cluster cannot go while they exist.
	h, err := c.CreateHsm(ctx, &cloudhsmv2.CreateHsmInput{ClusterId: aws.String(id), AvailabilityZone: aws.String("us-east-1a")})
	if err != nil || aws.ToString(h.Hsm.HsmId) == "" || h.Hsm.State != types.HsmStateActive || aws.ToString(h.Hsm.SubnetId) != "subnet-a" {
		t.Fatalf("create hsm: %v %+v", err, h)
	}
	if _, err := c.CreateHsm(ctx, &cloudhsmv2.CreateHsmInput{ClusterId: aws.String(id), AvailabilityZone: aws.String("us-east-1a")}); !errors.As(err, new(*types.CloudHsmInvalidRequestException)) {
		t.Fatalf("second hsm in a zone: %T %v", err, err)
	}
	if _, err := c.CreateHsm(ctx, &cloudhsmv2.CreateHsmInput{ClusterId: aws.String(id), AvailabilityZone: aws.String("us-east-1z")}); !errors.As(err, new(*types.CloudHsmInvalidRequestException)) {
		t.Fatalf("hsm in a foreign zone: %T %v", err, err)
	}
	if d, _ = c.DescribeClusters(ctx, &cloudhsmv2.DescribeClustersInput{}); len(d.Clusters[0].Hsms) != 1 {
		t.Fatalf("hsms in cluster: %+v", d.Clusters[0].Hsms)
	}
	if _, err := c.DeleteCluster(ctx, &cloudhsmv2.DeleteClusterInput{ClusterId: aws.String(id)}); !errors.As(err, new(*types.CloudHsmInvalidRequestException)) {
		t.Fatalf("delete cluster with hsm: %T %v", err, err)
	}
	if _, err := c.InitializeCluster(ctx, &cloudhsmv2.InitializeClusterInput{ClusterId: aws.String(id), SignedCert: aws.String("cert"), TrustAnchor: aws.String("anchor")}); err != nil {
		t.Fatal(err)
	}
	if d, _ = c.DescribeClusters(ctx, &cloudhsmv2.DescribeClustersInput{}); d.Clusters[0].State != types.ClusterStateInitialized {
		t.Fatalf("after initialize: %+v", d.Clusters[0].State)
	}
	if dh, err := c.DeleteHsm(ctx, &cloudhsmv2.DeleteHsmInput{ClusterId: aws.String(id), HsmId: h.Hsm.HsmId}); err != nil || aws.ToString(dh.HsmId) != aws.ToString(h.Hsm.HsmId) {
		t.Fatalf("delete hsm: %v %+v", err, dh)
	}
	if _, err := c.DeleteHsm(ctx, &cloudhsmv2.DeleteHsmInput{ClusterId: aws.String(id), HsmId: h.Hsm.HsmId}); !errors.As(err, new(*types.CloudHsmResourceNotFoundException)) {
		t.Fatalf("delete hsm twice: %T %v", err, err)
	}

	del, err := c.DeleteCluster(ctx, &cloudhsmv2.DeleteClusterInput{ClusterId: aws.String(id)})
	if err != nil || del.Cluster.State != types.ClusterStateDeleted {
		t.Fatalf("delete cluster: %v %+v", err, del)
	}
	if d, _ = c.DescribeClusters(ctx, &cloudhsmv2.DescribeClustersInput{}); len(d.Clusters) != 0 {
		t.Fatalf("cluster left: %+v", d.Clusters)
	}
	if _, err := c.DeleteCluster(ctx, &cloudhsmv2.DeleteClusterInput{ClusterId: aws.String(id)}); !errors.As(err, new(*types.CloudHsmResourceNotFoundException)) {
		t.Fatalf("delete cluster twice: %T %v", err, err)
	}
}
