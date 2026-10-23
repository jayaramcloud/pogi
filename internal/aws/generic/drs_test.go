package generic_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/drs"
	"github.com/aws/aws-sdk-go-v2/service/drs/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestDRS(t *testing.T) {
	_, url := start(t, "drs")
	ctx := context.Background()
	c := drs.NewFromConfig(awstest.Config(t, url))

	in := &drs.CreateReplicationConfigurationTemplateInput{
		AssociateDefaultSecurityGroup: aws.Bool(true), BandwidthThrottling: 0, CreatePublicIP: aws.Bool(false),
		DataPlaneRouting: types.ReplicationConfigurationDataPlaneRoutingPrivateIp, DefaultLargeStagingDiskType: types.ReplicationConfigurationDefaultLargeStagingDiskTypeGp3,
		EbsEncryption: types.ReplicationConfigurationEbsEncryptionDefault, ReplicationServerInstanceType: aws.String("t3.small"),
		ReplicationServersSecurityGroupsIDs: []string{"sg-0123"}, StagingAreaSubnetId: aws.String("subnet-0123"),
		StagingAreaTags: map[string]string{"role": "staging"}, UseDedicatedReplicationServer: aws.Bool(false),
		PitPolicy: []types.PITPolicyRule{{Enabled: aws.Bool(true), Interval: aws.Int32(10), RetentionDuration: aws.Int32(60), Units: types.PITPolicyRuleUnitsMinute, RuleID: 1}, {Enabled: aws.Bool(true), Interval: aws.Int32(1), RetentionDuration: aws.Int32(24), Units: types.PITPolicyRuleUnitsHour, RuleID: 2}, {Enabled: aws.Bool(true), Interval: aws.Int32(1), RetentionDuration: aws.Int32(3), Units: types.PITPolicyRuleUnitsDay, RuleID: 3}},
		Tags:      map[string]string{"env": "dev"},
	}
	o, err := c.CreateReplicationConfigurationTemplate(ctx, in)
	if err != nil || aws.ToString(o.ReplicationConfigurationTemplateID) == "" || aws.ToString(o.Arn) == "" {
		t.Fatalf("create: %v %+v", err, o)
	}
	d, err := c.DescribeReplicationConfigurationTemplates(ctx, &drs.DescribeReplicationConfigurationTemplatesInput{ReplicationConfigurationTemplateIDs: []string{*o.ReplicationConfigurationTemplateID}})
	if err != nil || len(d.Items) != 1 || aws.ToString(d.Items[0].ReplicationServerInstanceType) != "t3.small" || d.Items[0].DataPlaneRouting != types.ReplicationConfigurationDataPlaneRoutingPrivateIp {
		t.Fatalf("describe: %v %+v", err, d)
	}
	if st := d.Items[0].StagingAreaTags; len(st) != 1 || st["role"] != "staging" || d.Items[0].Tags["env"] != "dev" || len(d.Items[0].Tags) != 1 {
		t.Fatalf("tags mixed up: staging %v, own %v", st, d.Items[0].Tags)
	}
	if _, err := c.UpdateReplicationConfigurationTemplate(ctx, &drs.UpdateReplicationConfigurationTemplateInput{ReplicationConfigurationTemplateID: o.ReplicationConfigurationTemplateID, ReplicationServerInstanceType: aws.String("t3.large")}); err != nil {
		t.Fatal(err)
	}
	if d, _ := c.DescribeReplicationConfigurationTemplates(ctx, &drs.DescribeReplicationConfigurationTemplatesInput{}); len(d.Items) != 1 || aws.ToString(d.Items[0].ReplicationServerInstanceType) != "t3.large" || aws.ToString(d.Items[0].StagingAreaSubnetId) != "subnet-0123" {
		t.Fatalf("update: %+v", d)
	}
	if _, err := c.CreateReplicationConfigurationTemplate(ctx, &drs.CreateReplicationConfigurationTemplateInput{}); err == nil {
		t.Fatal("empty create succeeded")
	}
	if _, err := c.DeleteReplicationConfigurationTemplate(ctx, &drs.DeleteReplicationConfigurationTemplateInput{ReplicationConfigurationTemplateID: o.ReplicationConfigurationTemplateID}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteReplicationConfigurationTemplate(ctx, &drs.DeleteReplicationConfigurationTemplateInput{ReplicationConfigurationTemplateID: o.ReplicationConfigurationTemplateID}); err == nil {
		t.Fatal("second delete succeeded")
	}
}
