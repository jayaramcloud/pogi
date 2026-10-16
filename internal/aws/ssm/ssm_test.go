package ssm_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	ssmsvc "github.com/nrndev/pogi/internal/aws/ssm"
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

func setup(t *testing.T) (*ssm.Client, *ssmsvc.Service) {
	var svc *ssmsvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = ssmsvc.New(h); return svc })
	return ssm.NewFromConfig(awstest.Config(t, url)), svc
}

func put(c *ssm.Client, name, value string, typ types.ParameterType) *ssm.PutParameterOutput {
	return must(c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String(name), Value: aws.String(value), Type: typ}))
}

func get(t *testing.T, c *ssm.Client, name string) *types.Parameter {
	t.Helper()
	out, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(name), WithDecryption: aws.Bool(true)})
	if err != nil {
		t.Fatalf("GetParameter(%s): %v", name, err)
	}
	return out.Parameter
}

func TestVersionsLabelsAndErrors(t *testing.T) {
	c, svc := setup(t)
	if v := put(c, "/app/db/host", "db1", types.ParameterTypeString); v.Version != 1 || v.Tier != types.ParameterTierStandard {
		t.Fatalf("put = %+v", v)
	}
	_, err := c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/app/db/host"), Value: aws.String("db2")})
	var exists *types.ParameterAlreadyExists
	if !errors.As(err, &exists) {
		t.Fatalf("want ParameterAlreadyExists, got %v", err)
	}
	out := must(c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/app/db/host"), Value: aws.String("db2"), Overwrite: aws.Bool(true),
		Description: aws.String("primary")}))
	p := get(t, c, "/app/db/host")
	if out.Version != 2 || aws.ToString(p.Value) != "db2" || p.Version != 2 || p.Type != types.ParameterTypeString ||
		aws.ToString(p.ARN) != "arn:aws:ssm:us-east-1:000000000000:parameter/app/db/host" || aws.ToString(p.DataType) != "text" || p.LastModifiedDate == nil {
		t.Fatalf("get = %+v", p)
	}
	if p := get(t, c, "/app/db/host:1"); aws.ToString(p.Value) != "db1" || aws.ToString(p.Selector) != ":1" || aws.ToString(p.Name) != "/app/db/host" {
		t.Fatalf("selector :1 = %+v", p)
	}
	if p := get(t, c, "arn:aws:ssm:us-east-1:000000000000:parameter/app/db/host"); aws.ToString(p.Value) != "db2" {
		t.Fatalf("by ARN = %+v", p)
	}

	lab := must(c.LabelParameterVersion(ctx, &ssm.LabelParameterVersionInput{Name: aws.String("/app/db/host"), ParameterVersion: aws.Int64(1),
		Labels: []string{"stable", "awsbad", "1st"}}))
	if lab.ParameterVersion != 1 || strings.Join(lab.InvalidLabels, ",") != "awsbad,1st" {
		t.Fatalf("label = %+v", lab)
	}
	if p := get(t, c, "/app/db/host:stable"); aws.ToString(p.Value) != "db1" || aws.ToString(p.Selector) != ":stable" {
		t.Fatalf("label selector = %+v", p)
	}
	must(c.LabelParameterVersion(ctx, &ssm.LabelParameterVersionInput{Name: aws.String("/app/db/host"), Labels: []string{"stable"}})) // moves to v2
	hist := must(c.GetParameterHistory(ctx, &ssm.GetParameterHistoryInput{Name: aws.String("/app/db/host")}))
	if len(hist.Parameters) != 2 || len(hist.Parameters[0].Labels) != 0 || strings.Join(hist.Parameters[1].Labels, ",") != "stable" ||
		aws.ToString(hist.Parameters[1].Description) != "primary" || aws.ToString(hist.Parameters[0].Value) != "db1" {
		t.Fatalf("history = %+v", hist.Parameters)
	}
	if err := svc.Load(must(svc.Save())); err != nil { // state survives a snapshot round trip
		t.Fatal(err)
	}
	un := must(c.UnlabelParameterVersion(ctx, &ssm.UnlabelParameterVersionInput{Name: aws.String("/app/db/host"), ParameterVersion: aws.Int64(2), Labels: []string{"stable", "nope"}}))
	if strings.Join(un.RemovedLabels, ",") != "stable" || strings.Join(un.InvalidLabels, ",") != "nope" {
		t.Fatalf("unlabel = %+v", un)
	}

	var nf *types.ParameterNotFound
	if _, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/missing")}); !errors.As(err, &nf) {
		t.Fatalf("want ParameterNotFound, got %v", err)
	}
	var vnf *types.ParameterVersionNotFound
	if _, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/app/db/host:9")}); !errors.As(err, &vnf) {
		t.Fatalf("want ParameterVersionNotFound, got %v", err)
	}
	invalid := func(err error) bool { // not modeled for PutParameter, so the SDK keeps it generic
		var api smithy.APIError
		return errors.As(err, &api) && api.ErrorCode() == "ValidationException"
	}
	for _, name := range []string{"/aws/mine", "ssm-thing", "a/b", "/a//b"} {
		if _, err := c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String(name), Value: aws.String("x")}); !invalid(err) {
			t.Errorf("name %q: want ValidationException, got %v", name, err)
		}
	}
	if _, err := c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/app/db/host"), Value: aws.String("x"), Overwrite: aws.Bool(true),
		Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}); !invalid(err) {
		t.Fatalf("tags with overwrite: %v", err)
	}
	var deep *types.HierarchyLevelLimitExceededException
	if _, err := c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String(strings.Repeat("/l", 16)), Value: aws.String("x")}); !errors.As(err, &deep) {
		t.Fatalf("16 levels: %v", err)
	}
	var mismatch *types.ParameterPatternMismatchException
	if _, err := c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/port"), Value: aws.String("80a"), AllowedPattern: aws.String(`\d+`)}); !errors.As(err, &mismatch) {
		t.Fatalf("pattern: %v", err)
	}
	put(c, "/port", "8080", types.ParameterTypeString)
	var badKey *types.InvalidKeyId
	if _, err := c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/k"), Value: aws.String("x"), Type: types.ParameterTypeSecureString, KeyId: aws.String("not a key")}); !errors.As(err, &badKey) {
		t.Fatalf("key id: %v", err)
	}

	must(c.AddTagsToResource(ctx, &ssm.AddTagsToResourceInput{ResourceType: types.ResourceTypeForTaggingParameter, ResourceId: aws.String("/port"),
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("web")}, {Key: aws.String("env"), Value: aws.String("dev")}}}))
	must(c.RemoveTagsFromResource(ctx, &ssm.RemoveTagsFromResourceInput{ResourceType: types.ResourceTypeForTaggingParameter, ResourceId: aws.String("/port"), TagKeys: []string{"env"}}))
	tags := must(c.ListTagsForResource(ctx, &ssm.ListTagsForResourceInput{ResourceType: types.ResourceTypeForTaggingParameter, ResourceId: aws.String("/port")}))
	if len(tags.TagList) != 1 || aws.ToString(tags.TagList[0].Key) != "team" {
		t.Fatalf("tags = %+v", tags.TagList)
	}
	var badID *types.InvalidResourceId
	if _, err := c.ListTagsForResource(ctx, &ssm.ListTagsForResourceInput{ResourceType: types.ResourceTypeForTaggingParameter, ResourceId: aws.String("/nope")}); !errors.As(err, &badID) {
		t.Fatalf("tags of a missing parameter: %v", err)
	}

	if v, err := svc.Value("000000000000", "us-east-1", "/app/db/host:1"); err != nil || v != "db1" {
		t.Fatalf("Value = %q %v", v, err)
	}
	must(c.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: aws.String("/port")}))
	if _, err := c.DeleteParameter(ctx, &ssm.DeleteParameterInput{Name: aws.String("/port")}); !errors.As(err, &nf) {
		t.Fatalf("delete twice: %v", err)
	}
}

func TestSecureString(t *testing.T) {
	c, svc := setup(t)
	put(c, "/app/password", "hunter2", types.ParameterTypeSecureString)
	enc := must(c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/app/password")})).Parameter
	if enc.Type != types.ParameterTypeSecureString || aws.ToString(enc.Value) == "hunter2" || aws.ToString(enc.Value) == "" {
		t.Fatalf("without decryption = %+v", enc)
	}
	if p := get(t, c, "/app/password"); aws.ToString(p.Value) != "hunter2" {
		t.Fatalf("with decryption = %q", aws.ToString(p.Value))
	}
	d := must(c.DescribeParameters(ctx, &ssm.DescribeParametersInput{}))
	if len(d.Parameters) != 1 || aws.ToString(d.Parameters[0].KeyId) != "alias/aws/ssm" {
		t.Fatalf("describe = %+v", d.Parameters)
	}
	if v, _ := svc.Value("000000000000", "us-east-1", "/app/password"); v != "hunter2" {
		t.Fatalf("Value of a SecureString = %q", v)
	}
	if info, _ := svc.Inspect("Parameters", "/app/password").(map[string]any); info == nil || info["value"] != nil {
		t.Fatalf("Inspect must hide SecureString values: %v", info)
	}
}

// floci #4807: DescribeParameters returns the policies a parameter was put with, and a later put keeps them.
func TestParameterPolicies(t *testing.T) {
	c, _ := setup(t)
	pol := `[{"Type":"Expiration","Version":"1.0","Attributes":{"Timestamp":"2099-12-02T21:34:33.000Z"}}]`
	must(c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/p"), Value: aws.String("v"), Type: types.ParameterTypeString, Tier: types.ParameterTierAdvanced, Policies: aws.String(pol)}))
	must(c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/p"), Value: aws.String("w"), Overwrite: aws.Bool(true)}))
	p := must(c.DescribeParameters(ctx, &ssm.DescribeParametersInput{})).Parameters[0]
	if len(p.Policies) != 1 || aws.ToString(p.Policies[0].PolicyType) != "Expiration" || !strings.Contains(aws.ToString(p.Policies[0].PolicyText), "2099-12-02") {
		t.Errorf("policies = %+v", p.Policies)
	}
	if _, err := c.PutParameter(ctx, &ssm.PutParameterInput{Name: aws.String("/q"), Value: aws.String("v"), Policies: aws.String("nope")}); err == nil {
		t.Error("policies that are not JSON accepted")
	}
}

func TestPathsAndDescribe(t *testing.T) {
	c, _ := setup(t)
	put(c, "/app/a", "1", types.ParameterTypeString)
	put(c, "/app/b", "2", types.ParameterTypeSecureString)
	put(c, "/app/sub/c", "3", types.ParameterTypeStringList)
	put(c, "/other/x", "4", types.ParameterTypeString)
	put(c, "top", "5", types.ParameterTypeString)
	names := func(ps []types.Parameter) string {
		var out []string
		for _, p := range ps {
			out = append(out, aws.ToString(p.Name))
		}
		return strings.Join(out, ",")
	}
	if got := names(must(c.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{Path: aws.String("/app")})).Parameters); got != "/app/a,/app/b" {
		t.Fatalf("one level = %s", got)
	}
	if got := names(must(c.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{Path: aws.String("/app/"), Recursive: aws.Bool(true)})).Parameters); got != "/app/a,/app/b,/app/sub/c" {
		t.Fatalf("recursive = %s", got)
	}
	secure := must(c.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{Path: aws.String("/"), Recursive: aws.Bool(true), WithDecryption: aws.Bool(true),
		ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Type"), Values: []string{"SecureString"}}}}))
	if names(secure.Parameters) != "/app/b" || aws.ToString(secure.Parameters[0].Value) != "2" {
		t.Fatalf("type filter = %+v", secure.Parameters)
	}
	var badKey *types.InvalidFilterKey
	if _, err := c.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{Path: aws.String("/"), ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Name"), Values: []string{"x"}}}}); !errors.As(err, &badKey) {
		t.Fatalf("Name filter on GetParametersByPath: %v", err)
	}
	var all []string
	pager := ssm.NewGetParametersByPathPaginator(c, &ssm.GetParametersByPathInput{Path: aws.String("/"), Recursive: aws.Bool(true), MaxResults: aws.Int32(1)})
	for pager.HasMorePages() {
		all = append(all, names(must(pager.NextPage(ctx)).Parameters))
	}
	if strings.Join(all, ",") != "/app/a,/app/b,/app/sub/c,/other/x" {
		t.Fatalf("paginated = %v", all)
	}

	describe := func(in *ssm.DescribeParametersInput) string {
		var out []string
		for _, p := range must(c.DescribeParameters(ctx, in)).Parameters {
			out = append(out, aws.ToString(p.Name))
		}
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		in   *ssm.DescribeParametersInput
		want string
	}{
		{&ssm.DescribeParametersInput{}, "/app/a,/app/b,/app/sub/c,/other/x,top"},
		{&ssm.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Name"), Option: aws.String("BeginsWith"), Values: []string{"/app/"}}}}, "/app/a,/app/b,/app/sub/c"},
		{&ssm.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Name"), Option: aws.String("Contains"), Values: []string{"sub"}}}}, "/app/sub/c"},
		{&ssm.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Name"), Values: []string{"top"}}}}, "top"},
		{&ssm.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Path"), Values: []string{"/app"}}}}, "/app/a,/app/b"},
		{&ssm.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Path"), Option: aws.String("Recursive"), Values: []string{"/app"}},
			{Key: aws.String("Type"), Values: []string{"StringList"}}}}, "/app/sub/c"},
		{&ssm.DescribeParametersInput{ParameterFilters: []types.ParameterStringFilter{{Key: aws.String("Tier"), Values: []string{"Standard"}}}}, "/app/a,/app/b,/app/sub/c,/other/x,top"},
		{&ssm.DescribeParametersInput{Filters: []types.ParametersFilter{{Key: types.ParametersFilterKeyName, Values: []string{"/other"}}}}, "/other/x"},
		{&ssm.DescribeParametersInput{MaxResults: aws.Int32(2)}, "/app/a,/app/b"},
	} {
		if got := describe(tc.in); got != tc.want {
			t.Errorf("describe %+v = %s, want %s", tc.in, got, tc.want)
		}
	}

	gp := must(c.GetParameters(ctx, &ssm.GetParametersInput{Names: []string{"/app/a", "/nope", "top"}}))
	if names(gp.Parameters) != "/app/a,top" || strings.Join(gp.InvalidParameters, ",") != "/nope" {
		t.Fatalf("get parameters = %+v %v", gp.Parameters, gp.InvalidParameters)
	}
	del := must(c.DeleteParameters(ctx, &ssm.DeleteParametersInput{Names: []string{"/app/a", "/nope"}}))
	if strings.Join(del.DeletedParameters, ",") != "/app/a" || strings.Join(del.InvalidParameters, ",") != "/nope" {
		t.Fatalf("delete parameters = %+v", del)
	}
}

func TestPublicParameters(t *testing.T) {
	c, svc := setup(t)
	for _, name := range []string{
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64",
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64",
		"/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id",
	} {
		if p := get(t, c, name); !strings.HasPrefix(aws.ToString(p.Value), "ami-") || len(aws.ToString(p.Value)) != 21 {
			t.Fatalf("%s = %+v", name, p)
		}
	}
	regions := must(c.GetParametersByPath(ctx, &ssm.GetParametersByPathInput{Path: aws.String("/aws/service/global-infrastructure/regions"), MaxResults: aws.Int32(10)}))
	if len(regions.Parameters) != 10 || regions.NextToken == nil || !strings.HasPrefix(aws.ToString(regions.Parameters[0].Name), "/aws/service/global-infrastructure/regions/") {
		t.Fatalf("regions = %+v", regions)
	}
	if v, err := svc.Value("123456789012", "eu-west-1", "/aws/service/global-infrastructure/regions/eu-west-1"); err != nil || v != "eu-west-1" {
		t.Fatalf("public Value = %q %v", v, err)
	}
	if d := must(c.DescribeParameters(ctx, &ssm.DescribeParametersInput{})); len(d.Parameters) != 0 {
		t.Fatalf("public parameters must not be described: %d", len(d.Parameters))
	}
}
