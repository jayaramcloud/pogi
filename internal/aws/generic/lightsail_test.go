package generic_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	"github.com/aws/aws-sdk-go-v2/service/lightsail/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestLightsail(t *testing.T) {
	s := pogi.Start(t)
	ctx := context.Background()
	c := lightsail.NewFromConfig(awstest.Config(t, s.URL))
	lifecycle(t, time.Hour)

	// key pair: a real key, listed, and imported ones too
	kp, err := c.CreateKeyPair(ctx, &lightsail.CreateKeyPairInput{KeyPairName: aws.String("kp1"), Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("b")}}})
	if err != nil || aws.ToString(kp.KeyPair.Name) != "kp1" || !strings.HasPrefix(aws.ToString(kp.PublicKeyBase64), "ssh-ed25519 ") ||
		!strings.Contains(aws.ToString(kp.PrivateKeyBase64), "PRIVATE KEY") || aws.ToString(kp.KeyPair.Fingerprint) == "" || kp.Operation == nil {
		t.Fatalf("create key pair: %v %+v", err, kp)
	}
	if _, err := c.CreateKeyPair(ctx, &lightsail.CreateKeyPairInput{KeyPairName: aws.String("kp1")}); err == nil {
		t.Fatal("duplicate key pair")
	}
	if _, err := c.ImportKeyPair(ctx, &lightsail.ImportKeyPairInput{KeyPairName: aws.String("kp2"), PublicKeyBase64: kp.PublicKeyBase64}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.GetKeyPairs(ctx, &lightsail.GetKeyPairsInput{}); err != nil || len(l.KeyPairs) != 2 {
		t.Fatalf("key pairs: %v %+v", err, l)
	}
	if g, err := c.GetKeyPair(ctx, &lightsail.GetKeyPairInput{KeyPairName: aws.String("kp1")}); err != nil || aws.ToString(g.KeyPair.Arn) == "" || len(g.KeyPair.Tags) != 1 {
		t.Fatalf("get key pair: %v %+v", err, g)
	}

	// instance: pending, then running
	ci, err := c.CreateInstances(ctx, &lightsail.CreateInstancesInput{InstanceNames: []string{"web"}, AvailabilityZone: aws.String("us-east-1a"), BlueprintId: aws.String("ubuntu_22_04"),
		BundleId: aws.String("micro_3_0"), KeyPairName: aws.String("kp1"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil || len(ci.Operations) != 1 || ci.Operations[0].Status != types.OperationStatusSucceeded {
		t.Fatalf("create instance: %v %+v", err, ci)
	}
	op, err := c.GetOperation(ctx, &lightsail.GetOperationInput{OperationId: ci.Operations[0].Id})
	if err != nil || aws.ToString(op.Operation.ResourceName) != "web" || op.Operation.OperationType != types.OperationTypeCreateInstance {
		t.Fatalf("operation: %v %+v", err, op)
	}
	if _, err := c.CreateInstances(ctx, &lightsail.CreateInstancesInput{InstanceNames: []string{"web"}, AvailabilityZone: aws.String("us-east-1a"), BlueprintId: aws.String("ubuntu_22_04"), BundleId: aws.String("micro_3_0")}); err == nil {
		t.Fatal("duplicate instance")
	}
	get := func() *types.Instance {
		g, err := c.GetInstance(ctx, &lightsail.GetInstanceInput{InstanceName: aws.String("web")})
		if err != nil {
			t.Fatal(err)
		}
		return g.Instance
	}
	in := get()
	if aws.ToString(in.Name) != "web" || in.State == nil || aws.ToString(in.State.Name) != "pending" || aws.ToString(in.Username) != "ubuntu" || aws.ToString(in.SshKeyName) != "kp1" ||
		aws.ToString(in.BundleId) != "micro_3_0" || in.Hardware == nil || aws.ToFloat32(in.Hardware.RamSizeInGb) != 1 || aws.ToString(in.PublicIpAddress) == "" ||
		in.Location == nil || in.Location.RegionName != types.RegionNameUsEast1 || len(in.Tags) != 1 || in.ResourceType != types.ResourceTypeInstance {
		t.Fatalf("pending: %+v", in)
	}
	lifecycle(t, 0)
	if st := aws.ToString(get().State.Name); st != "running" {
		t.Fatalf("state %s", st)
	}
	if l, err := c.GetInstances(ctx, &lightsail.GetInstancesInput{}); err != nil || len(l.Instances) != 1 {
		t.Fatalf("instances: %v %+v", err, l)
	}
	lifecycle(t, time.Hour)
	if _, err := c.StopInstance(ctx, &lightsail.StopInstanceInput{InstanceName: aws.String("web")}); err != nil {
		t.Fatal(err)
	}
	if st, err := c.GetInstanceState(ctx, &lightsail.GetInstanceStateInput{InstanceName: aws.String("web")}); err != nil || aws.ToString(st.State.Name) != "stopping" {
		t.Fatalf("stopping: %v %+v", err, st)
	}
	lifecycle(t, 0)
	if st := aws.ToString(get().State.Name); st != "stopped" {
		t.Fatalf("stopped: %s", st)
	}
	if _, err := c.StartInstance(ctx, &lightsail.StartInstanceInput{InstanceName: aws.String("web")}); err != nil {
		t.Fatal(err)
	}
	if st := aws.ToString(get().State.Name); st != "running" {
		t.Fatalf("restarted: %s", st)
	}

	// static IP: allocate, attach, the instance takes the address, detach, release
	if _, err := c.AllocateStaticIp(ctx, &lightsail.AllocateStaticIpInput{StaticIpName: aws.String("ip1")}); err != nil {
		t.Fatal(err)
	}
	ipv, err := c.GetStaticIp(ctx, &lightsail.GetStaticIpInput{StaticIpName: aws.String("ip1")})
	if err != nil || aws.ToBool(ipv.StaticIp.IsAttached) || aws.ToString(ipv.StaticIp.IpAddress) == "" {
		t.Fatalf("static ip: %v %+v", err, ipv)
	}
	if _, err := c.AttachStaticIp(ctx, &lightsail.AttachStaticIpInput{StaticIpName: aws.String("ip1"), InstanceName: aws.String("web")}); err != nil {
		t.Fatal(err)
	}
	ipv, _ = c.GetStaticIp(ctx, &lightsail.GetStaticIpInput{StaticIpName: aws.String("ip1")})
	if !aws.ToBool(ipv.StaticIp.IsAttached) || aws.ToString(ipv.StaticIp.AttachedTo) != "web" || aws.ToString(get().PublicIpAddress) != aws.ToString(ipv.StaticIp.IpAddress) || !aws.ToBool(get().IsStaticIp) {
		t.Fatalf("attached: %+v %+v", ipv.StaticIp, get())
	}
	if l, err := c.GetStaticIps(ctx, &lightsail.GetStaticIpsInput{}); err != nil || len(l.StaticIps) != 1 {
		t.Fatalf("static ips: %v %+v", err, l)
	}
	if _, err := c.DetachStaticIp(ctx, &lightsail.DetachStaticIpInput{StaticIpName: aws.String("ip1")}); err != nil {
		t.Fatal(err)
	}
	if ipv, _ = c.GetStaticIp(ctx, &lightsail.GetStaticIpInput{StaticIpName: aws.String("ip1")}); aws.ToBool(ipv.StaticIp.IsAttached) {
		t.Fatalf("detached: %+v", ipv.StaticIp)
	}
	if _, err := c.ReleaseStaticIp(ctx, &lightsail.ReleaseStaticIpInput{StaticIpName: aws.String("ip1")}); err != nil {
		t.Fatal(err)
	}

	// tags
	arn := aws.ToString(get().Arn)
	if _, err := c.TagResource(ctx, &lightsail.TagResourceInput{ResourceName: aws.String("web"), Tags: []types.Tag{{Key: aws.String("x"), Value: aws.String("y")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &lightsail.UntagResourceInput{ResourceName: aws.String("web"), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	if tg := get().Tags; len(tg) != 1 || aws.ToString(tg[0].Key) != "x" || !strings.Contains(arn, ":Instance/") {
		t.Fatalf("tags: %+v %s", tg, arn)
	}

	// delete, then the typed not-found error
	if _, err := c.DeleteInstance(ctx, &lightsail.DeleteInstanceInput{InstanceName: aws.String("web")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetInstance(ctx, &lightsail.GetInstanceInput{InstanceName: aws.String("web")})
	var nf *types.NotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("get deleted instance: %v", err)
	}
	if _, err := c.DeleteKeyPair(ctx, &lightsail.DeleteKeyPairInput{KeyPairName: aws.String("kp1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.GetKeyPair(ctx, &lightsail.GetKeyPairInput{KeyPairName: aws.String("kp1")}); !errors.As(err, &nf) {
		t.Fatalf("get deleted key pair: %v", err)
	}
	if _, err := c.GetStaticIp(ctx, &lightsail.GetStaticIpInput{StaticIpName: aws.String("ip1")}); !errors.As(err, &nf) {
		t.Fatalf("get released static ip: %v", err)
	}

	// a plain engine resource: a bucket
	b, err := c.CreateBucket(ctx, &lightsail.CreateBucketInput{BucketName: aws.String("lsb"), BundleId: aws.String("small_1_0")})
	if err != nil || aws.ToString(b.Bucket.Name) != "lsb" {
		t.Fatalf("bucket: %v %+v", err, b)
	}
	if l, err := c.GetBuckets(ctx, &lightsail.GetBucketsInput{}); err != nil || len(l.Buckets) != 1 {
		t.Fatalf("buckets: %v %+v", err, l)
	}
	if _, err := c.DeleteBucket(ctx, &lightsail.DeleteBucketInput{BucketName: aws.String("lsb")}); err != nil {
		t.Fatal(err)
	}
}
