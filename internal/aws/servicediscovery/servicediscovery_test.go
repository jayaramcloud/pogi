package servicediscovery_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/aws/aws-sdk-go-v2/service/route53"
	sd "github.com/aws/aws-sdk-go-v2/service/servicediscovery"
	"github.com/aws/aws-sdk-go-v2/service/servicediscovery/types"
	"github.com/aws/smithy-go"
	ecssvc "github.com/nrndev/pogi/internal/aws/ecs"
	route53svc "github.com/nrndev/pogi/internal/aws/route53"
	"github.com/nrndev/pogi/internal/aws/servicediscovery"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantErr(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func setup(t *testing.T) (string, *sd.Client) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // ECS tasks as metadata only
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return servicediscovery.New(h) },
		func(h *core.Hub) core.Service { return route53svc.New(h) },
		func(h *core.Hub) core.Service { return ecssvc.New(h) })
	// DiscoverInstances goes to data-<host>: dial pogi whatever the host, to see that it routes
	addr := strings.TrimPrefix(url, "http://")
	hc := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}}
	_, port, _ := net.SplitHostPort(addr)
	return url, sd.NewFromConfig(awstest.Config(t, "http://localhost:"+port), func(o *sd.Options) { o.HTTPClient = hc })
}

// done waits for an operation and returns its target of kind.
func done(t *testing.T, c *sd.Client, id *string, kind string) string {
	t.Helper()
	op := must(c.GetOperation(ctx, &sd.GetOperationInput{OperationId: id})).Operation
	if op.Status != types.OperationStatusSuccess {
		t.Fatalf("operation %s = %s", *id, op.Status)
	}
	return op.Targets[kind]
}

func TestHTTPNamespace(t *testing.T) {
	_, c := setup(t)
	op := must(c.CreateHttpNamespace(ctx, &sd.CreateHttpNamespaceInput{Name: aws.String("shop"), Description: aws.String("apps"),
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("a")}}}))
	nsID := done(t, c, op.OperationId, "NAMESPACE")
	ns := must(c.GetNamespace(ctx, &sd.GetNamespaceInput{Id: aws.String(nsID)})).Namespace
	if ns.Type != types.NamespaceTypeHttp || *ns.Properties.HttpProperties.HttpName != "shop" || !strings.HasPrefix(*ns.Arn, "arn:aws:servicediscovery:us-east-1:000000000000:namespace/ns-") {
		t.Fatalf("namespace = %+v", ns)
	}
	_, err := c.CreateHttpNamespace(ctx, &sd.CreateHttpNamespaceInput{Name: aws.String("shop")})
	wantErr(t, err, "NamespaceAlreadyExists")

	svc := must(c.CreateService(ctx, &sd.CreateServiceInput{Name: aws.String("cart"), NamespaceId: aws.String(nsID),
		HealthCheckCustomConfig: &types.HealthCheckCustomConfig{FailureThreshold: aws.Int32(1)}})).Service
	if svc.Type != types.ServiceTypeHttp || *svc.InstanceCount != 0 {
		t.Fatalf("service = %+v", svc)
	}
	_, err = c.CreateService(ctx, &sd.CreateServiceInput{Name: aws.String("cart"), NamespaceId: aws.String(nsID)})
	wantErr(t, err, "ServiceAlreadyExists")
	_, err = c.CreateService(ctx, &sd.CreateServiceInput{Name: aws.String("dns"), NamespaceId: aws.String(nsID),
		DnsConfig: &types.DnsConfig{DnsRecords: []types.DnsRecord{{Type: types.RecordTypeA, TTL: aws.Int64(60)}}}})
	wantErr(t, err, "InvalidInput")

	for id, env := range map[string]string{"cart-1": "prod", "cart-2": "dev", "cart-3": "prod"} {
		op := must(c.RegisterInstance(ctx, &sd.RegisterInstanceInput{ServiceId: svc.Id, InstanceId: aws.String(id),
			Attributes: map[string]string{"AWS_INSTANCE_IPV4": "10.0.0.1", "AWS_INSTANCE_PORT": "8080", "env": env}}))
		done(t, c, op.OperationId, "INSTANCE")
	}
	disc := func(in *sd.DiscoverInstancesInput) []string {
		in.NamespaceName, in.ServiceName = aws.String("shop"), aws.String("cart")
		var ids []string
		for _, x := range must(c.DiscoverInstances(ctx, in)).Instances {
			ids = append(ids, *x.InstanceId)
		}
		slices.Sort(ids)
		return ids
	}
	if ids := disc(&sd.DiscoverInstancesInput{QueryParameters: map[string]string{"env": "prod"}}); !slices.Equal(ids, []string{"cart-1", "cart-3"}) {
		t.Fatalf("prod = %v", ids)
	}
	if ids := disc(&sd.DiscoverInstancesInput{OptionalParameters: map[string]string{"env": "dev"}}); !slices.Equal(ids, []string{"cart-2"}) {
		t.Fatalf("optional dev = %v", ids)
	}
	if ids := disc(&sd.DiscoverInstancesInput{OptionalParameters: map[string]string{"env": "test"}}); len(ids) != 3 {
		t.Fatalf("unmatched optional = %v", ids)
	}

	// custom health takes unhealthy instances out of discovery and changes the revision
	rev := must(c.DiscoverInstancesRevision(ctx, &sd.DiscoverInstancesRevisionInput{NamespaceName: aws.String("shop"), ServiceName: aws.String("cart")})).InstancesRevision
	must(c.UpdateInstanceCustomHealthStatus(ctx, &sd.UpdateInstanceCustomHealthStatusInput{ServiceId: svc.Id, InstanceId: aws.String("cart-1"), Status: types.CustomHealthStatusUnhealthy}))
	if ids := disc(&sd.DiscoverInstancesInput{QueryParameters: map[string]string{"env": "prod"}}); !slices.Equal(ids, []string{"cart-3"}) {
		t.Fatalf("healthy prod = %v", ids)
	}
	if ids := disc(&sd.DiscoverInstancesInput{HealthStatus: types.HealthStatusFilterAll}); len(ids) != 3 {
		t.Fatalf("all = %v", ids)
	}
	if again := must(c.DiscoverInstancesRevision(ctx, &sd.DiscoverInstancesRevisionInput{NamespaceName: aws.String("shop"), ServiceName: aws.String("cart")})).InstancesRevision; again == rev {
		t.Fatal("revision did not change")
	}
	st := must(c.GetInstancesHealthStatus(ctx, &sd.GetInstancesHealthStatusInput{ServiceId: svc.Id})).Status
	if st["cart-1"] != types.HealthStatusUnhealthy || st["cart-2"] != types.HealthStatusHealthy {
		t.Fatalf("health = %v", st)
	}

	got := must(c.GetInstance(ctx, &sd.GetInstanceInput{ServiceId: svc.Id, InstanceId: aws.String("cart-2")})).Instance
	if got.Attributes["env"] != "dev" {
		t.Fatalf("instance = %+v", got)
	}
	_, err = c.DeleteService(ctx, &sd.DeleteServiceInput{Id: svc.Id})
	wantErr(t, err, "ResourceInUse")
	_, err = c.DeleteNamespace(ctx, &sd.DeleteNamespaceInput{Id: aws.String(nsID)})
	wantErr(t, err, "ResourceInUse")
	for _, id := range []string{"cart-1", "cart-2", "cart-3"} {
		must(c.DeregisterInstance(ctx, &sd.DeregisterInstanceInput{ServiceId: svc.Id, InstanceId: aws.String(id)}))
	}
	_, err = c.GetInstance(ctx, &sd.GetInstanceInput{ServiceId: svc.Id, InstanceId: aws.String("cart-2")})
	wantErr(t, err, "InstanceNotFound")
	if tags := must(c.ListTagsForResource(ctx, &sd.ListTagsForResourceInput{ResourceARN: ns.Arn})).Tags; len(tags) != 1 || *tags[0].Value != "a" {
		t.Fatalf("tags = %+v", tags)
	}
	must(c.DeleteService(ctx, &sd.DeleteServiceInput{Id: svc.Id}))
	_, err = c.GetService(ctx, &sd.GetServiceInput{Id: svc.Id})
	wantErr(t, err, "ServiceNotFound")
	done(t, c, must(c.DeleteNamespace(ctx, &sd.DeleteNamespaceInput{Id: aws.String(nsID)})).OperationId, "NAMESPACE")
	if ops := must(c.ListOperations(ctx, &sd.ListOperationsInput{Filters: []types.OperationFilter{{Name: types.OperationFilterNameType,
		Values: []string{"REGISTER_INSTANCE"}, Condition: types.FilterConditionEq}}})).Operations; len(ops) != 3 {
		t.Fatalf("register operations = %d", len(ops))
	}
}

func TestPrivateDNSNamespace(t *testing.T) {
	url, c := setup(t)
	r53 := route53.NewFromConfig(awstest.Config(t, url))
	nsID := done(t, c, must(c.CreatePrivateDnsNamespace(ctx, &sd.CreatePrivateDnsNamespaceInput{Name: aws.String("corp.local"), Vpc: aws.String("vpc-123")})).OperationId, "NAMESPACE")
	zone := *must(c.GetNamespace(ctx, &sd.GetNamespaceInput{Id: aws.String(nsID)})).Namespace.Properties.DnsProperties.HostedZoneId
	hz := must(r53.GetHostedZone(ctx, &route53.GetHostedZoneInput{Id: aws.String(zone)}))
	if *hz.HostedZone.Name != "corp.local." || !hz.HostedZone.Config.PrivateZone {
		t.Fatalf("hosted zone = %+v", hz.HostedZone)
	}
	svc := must(c.CreateService(ctx, &sd.CreateServiceInput{Name: aws.String("api"), DnsConfig: &types.DnsConfig{NamespaceId: aws.String(nsID),
		DnsRecords: []types.DnsRecord{{Type: types.RecordTypeA, TTL: aws.Int64(30)}, {Type: types.RecordTypeSrv, TTL: aws.Int64(30)}}}})).Service
	if svc.Type != types.ServiceTypeDnsHttp || svc.DnsConfig.RoutingPolicy != types.RoutingPolicyMultivalue {
		t.Fatalf("service = %+v", svc)
	}
	_, err := c.RegisterInstance(ctx, &sd.RegisterInstanceInput{ServiceId: svc.Id, InstanceId: aws.String("i-1"), Attributes: map[string]string{"AWS_INSTANCE_IPV4": "10.1.0.5"}})
	wantErr(t, err, "InvalidInput") // SRV needs a port
	must(c.RegisterInstance(ctx, &sd.RegisterInstanceInput{ServiceId: svc.Id, InstanceId: aws.String("i-1"),
		Attributes: map[string]string{"AWS_INSTANCE_IPV4": "10.1.0.5", "AWS_INSTANCE_PORT": "443"}}))
	sets := func() map[string]string {
		out := map[string]string{}
		for _, rs := range must(r53.ListResourceRecordSets(ctx, &route53.ListResourceRecordSetsInput{HostedZoneId: aws.String(zone)})).ResourceRecordSets {
			if rs.Type != "NS" && rs.Type != "SOA" {
				out[string(rs.Type)+" "+*rs.Name] = *rs.ResourceRecords[0].Value
			}
		}
		return out
	}
	want := map[string]string{"A api.corp.local.": "10.1.0.5", "SRV api.corp.local.": "1 1 443 i-1.api.corp.local.", "A i-1.api.corp.local.": "10.1.0.5"}
	if got := sets(); len(got) != 3 || got["A api.corp.local."] != want["A api.corp.local."] || got["SRV api.corp.local."] != want["SRV api.corp.local."] || got["A i-1.api.corp.local."] != want["A i-1.api.corp.local."] {
		t.Fatalf("records = %v", got)
	}
	must(c.DeregisterInstance(ctx, &sd.DeregisterInstanceInput{ServiceId: svc.Id, InstanceId: aws.String("i-1")}))
	if got := sets(); len(got) != 0 {
		t.Fatalf("records after deregister = %v", got)
	}
	must(c.DeleteService(ctx, &sd.DeleteServiceInput{Id: svc.Id}))
	done(t, c, must(c.DeleteNamespace(ctx, &sd.DeleteNamespaceInput{Id: aws.String(nsID)})).OperationId, "NAMESPACE")
	_, err = r53.GetHostedZone(ctx, &route53.GetHostedZoneInput{Id: aws.String(zone)})
	wantErr(t, err, "NoSuchHostedZone")
}

// An ECS service with serviceRegistries registers its running tasks, and
// deregisters them when they stop.
func TestECSServiceRegistries(t *testing.T) {
	url, c := setup(t)
	e := ecs.NewFromConfig(awstest.Config(t, url))
	nsID := done(t, c, must(c.CreateHttpNamespace(ctx, &sd.CreateHttpNamespaceInput{Name: aws.String("apps")})).OperationId, "NAMESPACE")
	svc := must(c.CreateService(ctx, &sd.CreateServiceInput{Name: aws.String("web"), NamespaceId: aws.String(nsID)})).Service
	must(e.CreateCluster(ctx, &ecs.CreateClusterInput{ClusterName: aws.String("dev")}))
	td := must(e.RegisterTaskDefinition(ctx, &ecs.RegisterTaskDefinitionInput{Family: aws.String("web"), Memory: aws.String("512"),
		ContainerDefinitions: []ecstypes.ContainerDefinition{{Name: aws.String("web"), Image: aws.String("nginx:alpine"),
			PortMappings: []ecstypes.PortMapping{{ContainerPort: aws.Int32(80)}}}}})).TaskDefinition
	must(e.CreateService(ctx, &ecs.CreateServiceInput{Cluster: aws.String("dev"), ServiceName: aws.String("web"), TaskDefinition: td.TaskDefinitionArn,
		DesiredCount: aws.Int32(1), ServiceRegistries: []ecstypes.ServiceRegistry{{RegistryArn: svc.Arn, Port: aws.Int32(8080)}}}))
	instances := func(n int) []types.InstanceSummary {
		t.Helper()
		for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
			if got := must(c.ListInstances(ctx, &sd.ListInstancesInput{ServiceId: svc.Id})).Instances; len(got) == n {
				return got
			}
		}
		t.Fatalf("no %d instances", n)
		return nil
	}
	x := instances(1)[0]
	task := must(e.ListTasks(ctx, &ecs.ListTasksInput{Cluster: aws.String("dev"), ServiceName: aws.String("web")})).TaskArns[0]
	if !strings.HasSuffix(task, "/"+*x.Id) || x.Attributes["AWS_INSTANCE_PORT"] != "8080" || x.Attributes["AWS_INSTANCE_IPV4"] == "" ||
		x.Attributes["ECS_SERVICE_NAME"] != "web" || x.Attributes["ECS_CLUSTER_NAME"] != "dev" {
		t.Fatalf("instance = %+v (task %s)", x, task)
	}
	must(e.DeleteService(ctx, &ecs.DeleteServiceInput{Cluster: aws.String("dev"), Service: aws.String("web"), Force: aws.Bool(true)}))
	instances(0)
}
