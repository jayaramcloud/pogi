package ec2_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	r53 "github.com/aws/aws-sdk-go-v2/service/route53"
	r53types "github.com/aws/aws-sdk-go-v2/service/route53/types"
	ec2svc "github.com/nrndev/pogi/internal/aws/ec2"
	r53svc "github.com/nrndev/pogi/internal/aws/route53"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// TestInstanceResolvesPrivateZone: an instance in a VPC resolves a name of the
// VPC's private hosted zone, a record added later, and a name outside the
// zone's domain is still left to the usual resolver.
func TestInstanceResolvesPrivateZone(t *testing.T) {
	if testing.Short() {
		t.Skip("needs Docker")
	}
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ec2svc.New(h) }, func(h *core.Hub) core.Service { return r53svc.New(h) })
	if !hub.Docker().Available() || hub.Docker().HostAddr() == "127.0.0.1" {
		t.Skip("no container engine that reaches pogi from its network")
	}
	cfg := awstest.Config(t, url)
	c, rc := ec2.NewFromConfig(cfg), r53.NewFromConfig(cfg)
	vpc := must(c.CreateVpc(ctx, &ec2.CreateVpcInput{CidrBlock: aws.String("10.7.0.0/16")})).Vpc
	sn := must(c.CreateSubnet(ctx, &ec2.CreateSubnetInput{VpcId: vpc.VpcId, CidrBlock: aws.String("10.7.1.0/24")})).Subnet
	zone := must(rc.CreateHostedZone(ctx, &r53.CreateHostedZoneInput{Name: aws.String("corp.test"), CallerReference: aws.String("dns-1"),
		VPC: &r53types.VPC{VPCId: vpc.VpcId, VPCRegion: r53types.VPCRegionUsEast1}})).HostedZone
	record := func(name, ip string) {
		must(rc.ChangeResourceRecordSets(ctx, &r53.ChangeResourceRecordSetsInput{HostedZoneId: zone.Id, ChangeBatch: &r53types.ChangeBatch{Changes: []r53types.Change{{
			Action: r53types.ChangeActionUpsert, ResourceRecordSet: &r53types.ResourceRecordSet{Name: aws.String(name), Type: r53types.RRTypeA, TTL: aws.Int64(1),
				ResourceRecords: []r53types.ResourceRecord{{Value: aws.String(ip)}}}}}}}))
	}
	record("db.corp.test", "10.7.1.50")
	run := must(c.RunInstances(ctx, &ec2.RunInstancesInput{ImageId: aws.String(alpine), MinCount: aws.Int32(1), MaxCount: aws.Int32(1), SubnetId: sn.SubnetId}))
	id := *run.Instances[0].InstanceId
	t.Cleanup(func() { c.TerminateInstances(ctx, &ec2.TerminateInstancesInput{InstanceIds: []string{id}}) })
	waitRunning(t, c, id)
	lookup := func(name string) string {
		out, _, _ := hub.Docker().Exec(ctx, docker.Name("ec2", id), "sh", "-c", "nslookup "+name+" 2>&1")
		return string(out)
	}
	deadline := time.Now().Add(30 * time.Second)
	for !strings.Contains(lookup("db.corp.test"), "10.7.1.50") {
		if time.Now().After(deadline) {
			t.Fatalf("db.corp.test did not resolve:\n%s", lookup("db.corp.test"))
		}
		time.Sleep(500 * time.Millisecond)
	}
	record("late.corp.test", "10.7.1.51") // added after the instance started
	if out := lookup("late.corp.test"); !strings.Contains(out, "10.7.1.51") {
		t.Fatalf("a record added later did not resolve:\n%s", out)
	}
	if out := lookup("nothing.corp.test"); strings.Contains(out, "Address: 1") || !strings.Contains(out, "NXDOMAIN") {
		t.Fatalf("a missing name in the zone should be NXDOMAIN:\n%s", out)
	}
}
