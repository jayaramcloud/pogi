package redshiftserverless_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi/internal/aws/rds"
	serverless "github.com/nrndev/pogi/internal/aws/redshiftserverless"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func fails(t *testing.T, err error, code string) {
	t.Helper()
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != code {
		t.Fatalf("want %s, got %v", code, err)
	}
}

func TestNamespacesAndWorkgroupsWithoutDocker(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	var svc *serverless.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = serverless.New(h); return svc })
	c, ctx := redshiftserverless.NewFromConfig(awstest.Config(t, url)), context.Background()

	ns := must(c.CreateNamespace(ctx, &redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("sales"), AdminUsername: aws.String("admin"),
		AdminUserPassword: aws.String("Secret123"), DbName: aws.String("shop"), IamRoles: []string{"arn:aws:iam::000000000000:role/copy"},
		LogExports: []types.LogExport{types.LogExportUserLog}, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})).Namespace
	if *ns.NamespaceArn != "arn:aws:redshift-serverless:us-east-1:000000000000:namespace/"+*ns.NamespaceId || ns.Status != types.NamespaceStatusAvailable ||
		*ns.DbName != "shop" || *ns.AdminUsername != "admin" || *ns.KmsKeyId != "AWS_OWNED_KMS_KEY" || ns.CreationDate.IsZero() || len(ns.LogExports) != 1 {
		t.Fatalf("namespace %+v", ns)
	}
	_, err := c.CreateNamespace(ctx, &redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("sales")})
	fails(t, err, "ConflictException")
	_, err = c.CreateNamespace(ctx, &redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("Bad_Name")})
	fails(t, err, "ValidationException")
	_, err = c.CreateNamespace(ctx, &redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("weak"), AdminUsername: aws.String("admin"),
		AdminUserPassword: aws.String("weak")})
	fails(t, err, "ValidationException")
	_, err = c.GetNamespace(ctx, &redshiftserverless.GetNamespaceInput{NamespaceName: aws.String("nope")})
	fails(t, err, "ResourceNotFoundException")
	must(c.CreateNamespace(ctx, &redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("bare")})) // pogi gives it an admin
	if p := svc.Inspect("Namespaces", "bare").(*serverless.Namespace); p.AdminUsername != "admin" || len(p.Password) < 8 {
		t.Fatalf("bare namespace %+v", p)
	}
	if list := must(c.ListNamespaces(ctx, &redshiftserverless.ListNamespacesInput{})).Namespaces; len(list) != 2 || *list[0].NamespaceName != "bare" {
		t.Fatalf("namespaces %+v", list)
	}
	up := must(c.UpdateNamespace(ctx, &redshiftserverless.UpdateNamespaceInput{NamespaceName: aws.String("sales"), AdminUsername: aws.String("admin"),
		AdminUserPassword: aws.String("Changed123"), LogExports: []types.LogExport{}})).Namespace
	if len(up.LogExports) != 0 || svc.Inspect("Namespaces", "sales").(*serverless.Namespace).Password != "Changed123" {
		t.Fatalf("updated %+v", up)
	}

	wg := must(c.CreateWorkgroup(ctx, &redshiftserverless.CreateWorkgroupInput{WorkgroupName: aws.String("etl"), NamespaceName: aws.String("sales"),
		BaseCapacity: aws.Int32(32), ConfigParameters: []types.ConfigParameter{{ParameterKey: aws.String("search_path"), ParameterValue: aws.String("public")}},
		SubnetIds: []string{"subnet-1"}, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("data")}}})).Workgroup
	if wg.Status != types.WorkgroupStatusAvailable || *wg.Endpoint.Address != "localhost" || *wg.Endpoint.Port != 5439 || *wg.Port != 5439 ||
		*wg.BaseCapacity != 32 || *wg.ConfigParameters[0].ParameterKey != "search_path" || *wg.NamespaceName != "sales" || *wg.TrackName != "current" ||
		*wg.WorkgroupArn != "arn:aws:redshift-serverless:us-east-1:000000000000:workgroup/"+*wg.WorkgroupId {
		t.Fatalf("workgroup %+v", wg)
	}
	if db := svc.DB("000000000000", "us-east-1", *wg.WorkgroupArn); db == nil || db.Note != rds.NoDocker || db.User != "admin" || db.Password != "Changed123" || db.Name != "shop" {
		t.Fatalf("db %+v", db)
	}
	_, err = c.CreateWorkgroup(ctx, &redshiftserverless.CreateWorkgroupInput{WorkgroupName: aws.String("etl2"), NamespaceName: aws.String("sales")})
	fails(t, err, "ConflictException") // one workgroup per namespace
	_, err = c.CreateWorkgroup(ctx, &redshiftserverless.CreateWorkgroupInput{WorkgroupName: aws.String("odd"), NamespaceName: aws.String("bare"), Port: aws.Int32(6000)})
	fails(t, err, "ValidationException")
	_, err = c.CreateWorkgroup(ctx, &redshiftserverless.CreateWorkgroupInput{WorkgroupName: aws.String("lost"), NamespaceName: aws.String("nope")})
	fails(t, err, "ResourceNotFoundException")
	if w := must(c.UpdateWorkgroup(ctx, &redshiftserverless.UpdateWorkgroupInput{WorkgroupName: aws.String("etl"), MaxCapacity: aws.Int32(256),
		PubliclyAccessible: aws.Bool(true)})).Workgroup; *w.MaxCapacity != 256 || !*w.PubliclyAccessible {
		t.Fatalf("updated %+v", w)
	}
	if list := must(c.ListWorkgroups(ctx, &redshiftserverless.ListWorkgroupsInput{})).Workgroups; len(list) != 1 || *list[0].WorkgroupName != "etl" {
		t.Fatalf("workgroups %+v", list)
	}

	cr := must(c.GetCredentials(ctx, &redshiftserverless.GetCredentialsInput{WorkgroupName: aws.String("etl")}))
	if *cr.DbUser != "IAM:root" || len(*cr.DbPassword) != 40 || cr.Expiration.IsZero() || cr.NextRefreshTime.IsZero() {
		t.Fatalf("credentials %+v", cr)
	}
	_, err = c.GetCredentials(ctx, &redshiftserverless.GetCredentialsInput{WorkgroupName: aws.String("etl"), DurationSeconds: aws.Int32(10)})
	fails(t, err, "ValidationException")

	// snapshots, usage limits and tags
	sn := must(c.CreateSnapshot(ctx, &redshiftserverless.CreateSnapshotInput{NamespaceName: aws.String("sales"), SnapshotName: aws.String("daily"),
		RetentionPeriod: aws.Int32(7)})).Snapshot
	if sn.Status != types.SnapshotStatusAvailable || *sn.SnapshotRetentionPeriod != 7 || *sn.NamespaceArn != *ns.NamespaceArn {
		t.Fatalf("snapshot %+v", sn)
	}
	_, err = c.CreateSnapshot(ctx, &redshiftserverless.CreateSnapshotInput{NamespaceName: aws.String("sales"), SnapshotName: aws.String("daily")})
	fails(t, err, "ConflictException")
	if got := must(c.GetSnapshot(ctx, &redshiftserverless.GetSnapshotInput{SnapshotArn: sn.SnapshotArn})).Snapshot; *got.SnapshotName != "daily" {
		t.Fatalf("snapshot by ARN %+v", got)
	}
	must(c.UpdateSnapshot(ctx, &redshiftserverless.UpdateSnapshotInput{SnapshotName: aws.String("daily"), RetentionPeriod: aws.Int32(14)}))
	if list := must(c.ListSnapshots(ctx, &redshiftserverless.ListSnapshotsInput{NamespaceName: aws.String("sales")})).Snapshots; len(list) != 1 || *list[0].SnapshotRetentionPeriod != 14 {
		t.Fatalf("snapshots %+v", list)
	}
	l := must(c.CreateUsageLimit(ctx, &redshiftserverless.CreateUsageLimitInput{ResourceArn: wg.WorkgroupArn, UsageType: types.UsageLimitUsageTypeServerlessCompute,
		Amount: aws.Int64(100)})).UsageLimit
	if l.Period != types.UsageLimitPeriodMonthly || l.BreachAction != types.UsageLimitBreachActionLog {
		t.Fatalf("usage limit %+v", l)
	}
	must(c.UpdateUsageLimit(ctx, &redshiftserverless.UpdateUsageLimitInput{UsageLimitId: l.UsageLimitId, Amount: aws.Int64(200)}))
	if got := must(c.GetUsageLimit(ctx, &redshiftserverless.GetUsageLimitInput{UsageLimitId: l.UsageLimitId})).UsageLimit; *got.Amount != 200 {
		t.Fatalf("usage limit %+v", got)
	}
	_, err = c.CreateUsageLimit(ctx, &redshiftserverless.CreateUsageLimitInput{ResourceArn: aws.String("arn:aws:redshift-serverless:us-east-1:000000000000:workgroup/x"),
		UsageType: types.UsageLimitUsageTypeServerlessCompute, Amount: aws.Int64(1)})
	fails(t, err, "ResourceNotFoundException")
	must(c.TagResource(ctx, &redshiftserverless.TagResourceInput{ResourceArn: wg.WorkgroupArn, Tags: []types.Tag{{Key: aws.String("owner"), Value: aws.String("me")}}}))
	must(c.UntagResource(ctx, &redshiftserverless.UntagResourceInput{ResourceArn: wg.WorkgroupArn, TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &redshiftserverless.ListTagsForResourceInput{ResourceArn: wg.WorkgroupArn})).Tags; len(tags) != 1 || *tags[0].Key != "owner" {
		t.Fatalf("tags %+v", tags)
	}
	if tags := must(c.ListTagsForResource(ctx, &redshiftserverless.ListTagsForResourceInput{ResourceArn: ns.NamespaceArn})).Tags; len(tags) != 1 || *tags[0].Value != "dev" {
		t.Fatalf("namespace tags %+v", tags)
	}

	// deleting: the namespace only once its workgroup is gone
	_, err = c.DeleteNamespace(ctx, &redshiftserverless.DeleteNamespaceInput{NamespaceName: aws.String("sales")})
	fails(t, err, "ConflictException")
	if w := must(c.DeleteWorkgroup(ctx, &redshiftserverless.DeleteWorkgroupInput{WorkgroupName: aws.String("etl")})).Workgroup; w.Status != types.WorkgroupStatusDeleting {
		t.Fatalf("deleted %+v", w)
	}
	_, err = c.GetUsageLimit(ctx, &redshiftserverless.GetUsageLimitInput{UsageLimitId: l.UsageLimitId})
	fails(t, err, "ResourceNotFoundException") // went with its workgroup
	if n := must(c.DeleteNamespace(ctx, &redshiftserverless.DeleteNamespaceInput{NamespaceName: aws.String("sales"),
		FinalSnapshotName: aws.String("last")})).Namespace; n.Status != types.NamespaceStatusDeleting {
		t.Fatalf("deleted %+v", n)
	}
	_, err = c.GetWorkgroup(ctx, &redshiftserverless.GetWorkgroupInput{WorkgroupName: aws.String("etl")})
	fails(t, err, "ResourceNotFoundException")
	must(c.DeleteSnapshot(ctx, &redshiftserverless.DeleteSnapshotInput{SnapshotName: aws.String("last")}))
	_, err = c.GetSnapshot(ctx, &redshiftserverless.GetSnapshotInput{SnapshotName: aws.String("last")})
	fails(t, err, "ResourceNotFoundException")

	b := must(svc.Save())
	again := serverless.New(core.NewHub(core.Config{}, nil))
	if err := again.Load(b); err != nil {
		t.Fatal(err)
	}
	if v, _ := again.Inspect("Namespaces", "bare").(*serverless.Namespace); v == nil || v.Password == "" {
		t.Fatalf("loaded %+v", v)
	}
}

// floci #5359: a tag with no key is refused, not dropped.
func TestTagWithoutKeyIsRefused(t *testing.T) {
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { return serverless.New(h) })
	c, ctx := redshiftserverless.NewFromConfig(awstest.Config(t, url)), context.Background()
	_, err := c.CreateNamespace(ctx, &redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("nokey"),
		Tags: []types.Tag{{Key: aws.String(""), Value: aws.String("x")}}})
	fails(t, err, "ValidationException")
	ns := must(c.CreateNamespace(ctx, &redshiftserverless.CreateNamespaceInput{NamespaceName: aws.String("nokey2")})).Namespace
	_, err = c.TagResource(ctx, &redshiftserverless.TagResourceInput{ResourceArn: ns.NamespaceArn,
		Tags: []types.Tag{{Key: aws.String(""), Value: aws.String("x")}}})
	fails(t, err, "ValidationException")
}
