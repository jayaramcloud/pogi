package secretsmanager_test

import (
	"archive/zip"
	"bytes"
	"context"
	"errors"
	"os/exec"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	sm "github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	smsvc "github.com/nrndev/pogi/internal/aws/secretsmanager"
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

func setup(t *testing.T) *sm.Client {
	c, _ := setupHub(t)
	return c
}

func setupHub(t *testing.T) (*sm.Client, *core.Hub) {
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return smsvc.New(h) })
	return sm.NewFromConfig(awstest.Config(t, url)), h
}

func value(t *testing.T, c *sm.Client, in *sm.GetSecretValueInput) string {
	t.Helper()
	out, err := c.GetSecretValue(ctx, in)
	if err != nil {
		t.Fatalf("GetSecretValue(%s): %v", aws.ToString(in.SecretId), err)
	}
	return aws.ToString(out.SecretString)
}

func TestVersionsAndStages(t *testing.T) {
	c := setup(t)
	created := must(c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("app/db"), SecretString: aws.String("v1"),
		Description: aws.String("Database login"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}}}))
	arn := aws.ToString(created.ARN)
	if !strings.HasPrefix(arn, "arn:aws:secretsmanager:us-east-1:000000000000:secret:app/db-") || len(arn) != len("arn:aws:secretsmanager:us-east-1:000000000000:secret:app/db-")+6 {
		t.Fatalf("arn = %s", arn)
	}
	_, err := c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("app/db"), SecretString: aws.String("x")})
	var exists *types.ResourceExistsException
	if !errors.As(err, &exists) {
		t.Fatalf("want ResourceExistsException, got %v", err)
	}
	partial := strings.TrimSuffix(arn, arn[len(arn)-7:])
	for _, id := range []string{"app/db", arn, partial} {
		if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String(id)}); v != "v1" {
			t.Fatalf("%s = %q", id, v)
		}
	}
	_, err = c.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretId: aws.String("nope")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) || !strings.Contains(err.Error(), "can't find the specified secret") {
		t.Fatalf("want ResourceNotFoundException, got %v", err)
	}

	// A new value becomes AWSCURRENT and the old one AWSPREVIOUS; the token makes it idempotent.
	put := must(c.PutSecretValue(ctx, &sm.PutSecretValueInput{SecretId: aws.String("app/db"), SecretString: aws.String("v2"), ClientRequestToken: aws.String("token-v2-00000000000000000000000000")}))
	must(c.PutSecretValue(ctx, &sm.PutSecretValueInput{SecretId: aws.String("app/db"), SecretString: aws.String("v2"), ClientRequestToken: aws.String("token-v2-00000000000000000000000000")}))
	if _, err := c.PutSecretValue(ctx, &sm.PutSecretValueInput{SecretId: aws.String("app/db"), SecretString: aws.String("other"), ClientRequestToken: put.VersionId}); !errors.As(err, &exists) {
		t.Fatalf("changing a version must fail, got %v", err)
	}
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("app/db")}); v != "v2" {
		t.Fatalf("current = %q", v)
	}
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("app/db"), VersionStage: aws.String("AWSPREVIOUS")}); v != "v1" {
		t.Fatalf("previous = %q", v)
	}
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("app/db"), VersionId: created.VersionId}); v != "v1" {
		t.Fatalf("by version id = %q", v)
	}
	pending := must(c.PutSecretValue(ctx, &sm.PutSecretValueInput{SecretId: aws.String("app/db"), SecretString: aws.String("v3"), VersionStages: []string{"AWSPENDING"}}))
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("app/db")}); v != "v2" {
		t.Fatalf("a pending value must not become current, got %q", v)
	}

	d := must(c.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretId: aws.String(arn)}))
	stages := d.VersionIdsToStages
	if len(stages) != 3 || stages[*created.VersionId][0] != "AWSPREVIOUS" || stages[*put.VersionId][0] != "AWSCURRENT" || stages[*pending.VersionId][0] != "AWSPENDING" ||
		aws.ToString(d.Description) != "Database login" || len(d.Tags) != 1 || d.CreatedDate == nil || d.LastChangedDate == nil || d.LastAccessedDate == nil || d.DeletedDate != nil {
		t.Fatalf("describe = %+v", d)
	}

	// Promote the pending version by hand; AWSPREVIOUS follows.
	_, err = c.UpdateSecretVersionStage(ctx, &sm.UpdateSecretVersionStageInput{SecretId: aws.String("app/db"), VersionStage: aws.String("AWSCURRENT"), MoveToVersionId: pending.VersionId})
	var bad *types.InvalidParameterException
	if !errors.As(err, &bad) {
		t.Fatalf("moving AWSCURRENT without RemoveFromVersionId must fail, got %v", err)
	}
	must(c.UpdateSecretVersionStage(ctx, &sm.UpdateSecretVersionStageInput{SecretId: aws.String("app/db"), VersionStage: aws.String("AWSCURRENT"),
		MoveToVersionId: pending.VersionId, RemoveFromVersionId: put.VersionId}))
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("app/db")}); v != "v3" {
		t.Fatalf("current after move = %q", v)
	}
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("app/db"), VersionStage: aws.String("AWSPREVIOUS")}); v != "v2" {
		t.Fatalf("previous after move = %q", v)
	}
	ids := must(c.ListSecretVersionIds(ctx, &sm.ListSecretVersionIdsInput{SecretId: aws.String("app/db")}))
	all := must(c.ListSecretVersionIds(ctx, &sm.ListSecretVersionIdsInput{SecretId: aws.String("app/db"), IncludeDeprecated: aws.Bool(true)}))
	if len(ids.Versions) != 2 || len(all.Versions) != 3 {
		t.Fatalf("versions = %d, with deprecated = %d", len(ids.Versions), len(all.Versions))
	}

	must(c.UpdateSecret(ctx, &sm.UpdateSecretInput{SecretId: aws.String("app/db"), Description: aws.String("changed")}))
	upd := must(c.UpdateSecret(ctx, &sm.UpdateSecretInput{SecretId: aws.String("app/db"), SecretString: aws.String("v4")}))
	if upd.VersionId == nil || value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("app/db")}) != "v4" {
		t.Fatalf("update = %+v", upd)
	}

	bin := must(c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("blob"), SecretBinary: []byte{0, 1, 2}}))
	got := must(c.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretId: bin.ARN}))
	if !bytes.Equal(got.SecretBinary, []byte{0, 1, 2}) || got.SecretString != nil {
		t.Fatalf("binary = %v %v", got.SecretBinary, got.SecretString)
	}
	batch := must(c.BatchGetSecretValue(ctx, &sm.BatchGetSecretValueInput{SecretIdList: []string{"app/db", "blob", "missing"}}))
	if len(batch.SecretValues) != 2 || len(batch.Errors) != 1 || aws.ToString(batch.Errors[0].ErrorCode) != "ResourceNotFoundException" {
		t.Fatalf("batch = %+v", batch)
	}
	byFilter := must(c.BatchGetSecretValue(ctx, &sm.BatchGetSecretValueInput{Filters: []types.Filter{{Key: types.FilterNameStringTypeName, Values: []string{"app/"}}}}))
	if len(byFilter.SecretValues) != 1 || aws.ToString(byFilter.SecretValues[0].SecretString) != "v4" {
		t.Fatalf("batch by filter = %+v", byFilter)
	}
}

func TestListSecretsFilters(t *testing.T) {
	c := setup(t)
	for _, s := range []struct{ name, desc, tag string }{{"prod/api", "API key", "team-a"}, {"prod/db", "Database", "team-b"}, {"dev/db", "database for dev", "team-b"}} {
		must(c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String(s.name), Description: aws.String(s.desc), SecretString: aws.String("x"),
			Tags: []types.Tag{{Key: aws.String("owner"), Value: aws.String(s.tag)}}}))
	}
	names := func(f ...types.Filter) string {
		var out []string
		for _, e := range must(c.ListSecrets(ctx, &sm.ListSecretsInput{Filters: f, SortBy: types.SortByTypeName})).SecretList {
			out = append(out, *e.Name)
		}
		return strings.Join(out, ",")
	}
	for _, tc := range []struct {
		filters []types.Filter
		want    string
	}{
		{nil, "dev/db,prod/api,prod/db"},
		{[]types.Filter{{Key: types.FilterNameStringTypeName, Values: []string{"prod"}}}, "prod/api,prod/db"},
		{[]types.Filter{{Key: types.FilterNameStringTypeName, Values: []string{"!prod"}}}, "dev/db"},
		{[]types.Filter{{Key: types.FilterNameStringTypeDescription, Values: []string{"database"}}}, "dev/db,prod/db"},
		{[]types.Filter{{Key: types.FilterNameStringTypeTagValue, Values: []string{"team-b"}}, {Key: types.FilterNameStringTypeName, Values: []string{"prod"}}}, "prod/db"},
		{[]types.Filter{{Key: types.FilterNameStringTypeTagKey, Values: []string{"owner"}}}, "dev/db,prod/api,prod/db"},
		{[]types.Filter{{Key: types.FilterNameStringTypeAll, Values: []string{"API"}}}, "prod/api"},
	} {
		if got := names(tc.filters...); got != tc.want {
			t.Errorf("filters %+v = %s, want %s", tc.filters, got, tc.want)
		}
	}
	page := must(c.ListSecrets(ctx, &sm.ListSecretsInput{MaxResults: aws.Int32(2)}))
	rest := must(c.ListSecrets(ctx, &sm.ListSecretsInput{MaxResults: aws.Int32(2), NextToken: page.NextToken}))
	if len(page.SecretList) != 2 || page.NextToken == nil || len(rest.SecretList) != 1 || rest.NextToken != nil {
		t.Fatalf("pagination = %d %v / %d %v", len(page.SecretList), page.NextToken, len(rest.SecretList), rest.NextToken)
	}
	if len(page.SecretList[0].SecretVersionsToStages) != 1 {
		t.Fatalf("list entry = %+v", page.SecretList[0])
	}
}

func TestDeleteAndRestore(t *testing.T) {
	c, h := setupHub(t)
	must(c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("old"), SecretString: aws.String("s")}))
	if err := h.Restore(must(h.Snapshot())); err != nil { // state survives a snapshot round trip
		t.Fatal(err)
	}
	if _, err := c.DeleteSecret(ctx, &sm.DeleteSecretInput{SecretId: aws.String("old"), RecoveryWindowInDays: aws.Int64(3)}); err == nil {
		t.Fatal("a 3-day recovery window must be refused")
	}
	del := must(c.DeleteSecret(ctx, &sm.DeleteSecretInput{SecretId: aws.String("old"), RecoveryWindowInDays: aws.Int64(7)}))
	if del.DeletionDate == nil {
		t.Fatal("no DeletionDate")
	}
	_, err := c.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretId: aws.String("old")})
	var inv *types.InvalidRequestException
	if !errors.As(err, &inv) || !strings.Contains(err.Error(), "marked for deletion") {
		t.Fatalf("read of a deleted secret = %v", err)
	}
	if d := must(c.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretId: aws.String("old")})); d.DeletedDate == nil {
		t.Fatal("describe must show DeletedDate")
	}
	if l := must(c.ListSecrets(ctx, &sm.ListSecretsInput{})); len(l.SecretList) != 0 {
		t.Fatalf("deleted secrets are hidden by default: %d", len(l.SecretList))
	}
	if l := must(c.ListSecrets(ctx, &sm.ListSecretsInput{IncludePlannedDeletion: aws.Bool(true)})); len(l.SecretList) != 1 {
		t.Fatalf("IncludePlannedDeletion: %d", len(l.SecretList))
	}
	if _, err := c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("old"), SecretString: aws.String("s")}); !errors.As(err, &inv) {
		t.Fatalf("recreate while scheduled for deletion = %v", err)
	}
	// floci #2549: tagging a secret that is scheduled for deletion is refused too
	if _, err := c.TagResource(ctx, &sm.TagResourceInput{SecretId: aws.String("old"), Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}}}); !errors.As(err, &inv) {
		t.Fatalf("TagResource while scheduled for deletion = %v", err)
	}
	if _, err := c.UntagResource(ctx, &sm.UntagResourceInput{SecretId: aws.String("old"), TagKeys: []string{"a"}}); !errors.As(err, &inv) {
		t.Fatalf("UntagResource while scheduled for deletion = %v", err)
	}
	must(c.RestoreSecret(ctx, &sm.RestoreSecretInput{SecretId: aws.String("old")}))
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("old")}); v != "s" {
		t.Fatalf("restored = %q", v)
	}
	must(c.DeleteSecret(ctx, &sm.DeleteSecretInput{SecretId: aws.String("old"), ForceDeleteWithoutRecovery: aws.Bool(true)}))
	_, err = c.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretId: aws.String("old")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("force-deleted secret = %v", err)
	}
	must(c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("old"), SecretString: aws.String("new")}))

	must(c.TagResource(ctx, &sm.TagResourceInput{SecretId: aws.String("old"), Tags: []types.Tag{{Key: aws.String("a"), Value: aws.String("1")}, {Key: aws.String("b"), Value: aws.String("2")}}}))
	must(c.UntagResource(ctx, &sm.UntagResourceInput{SecretId: aws.String("old"), TagKeys: []string{"a"}}))
	if d := must(c.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretId: aws.String("old")})); len(d.Tags) != 1 || *d.Tags[0].Key != "b" {
		t.Fatalf("tags = %+v", d.Tags)
	}
	policy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000000:root"},"Action":"secretsmanager:GetSecretValue","Resource":"*"}]}`
	must(c.PutResourcePolicy(ctx, &sm.PutResourcePolicyInput{SecretId: aws.String("old"), ResourcePolicy: aws.String(policy)}))
	if p := must(c.GetResourcePolicy(ctx, &sm.GetResourcePolicyInput{SecretId: aws.String("old")})); aws.ToString(p.ResourcePolicy) != policy {
		t.Fatalf("policy = %v", p.ResourcePolicy)
	}
	if v := must(c.ValidateResourcePolicy(ctx, &sm.ValidateResourcePolicyInput{ResourcePolicy: aws.String(policy)})); !v.PolicyValidationPassed {
		t.Fatal("policy should validate")
	}
	var malformed *types.MalformedPolicyDocumentException
	if _, err := c.PutResourcePolicy(ctx, &sm.PutResourcePolicyInput{SecretId: aws.String("old"), ResourcePolicy: aws.String("{nope")}); !errors.As(err, &malformed) {
		t.Fatalf("malformed policy = %v", err)
	}
}

func TestRandomPassword(t *testing.T) {
	c := setup(t)
	pw := aws.ToString(must(c.GetRandomPassword(ctx, &sm.GetRandomPasswordInput{})).RandomPassword)
	if len(pw) != 32 || !strings.ContainsAny(pw, "abcdefghijklmnopqrstuvwxyz") || !strings.ContainsAny(pw, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") ||
		!strings.ContainsAny(pw, "0123456789") || strings.Contains(pw, " ") {
		t.Fatalf("default password %q", pw)
	}
	for range 20 {
		pw = aws.ToString(must(c.GetRandomPassword(ctx, &sm.GetRandomPasswordInput{PasswordLength: aws.Int64(4), ExcludePunctuation: aws.Bool(true),
			ExcludeCharacters: aws.String("abcdefghijklmnopqrstuvwxyz"), IncludeSpace: aws.Bool(true)})).RandomPassword)
		if len(pw) != 4 || strings.ContainsAny(pw, "abcdefghijklmnopqrstuvwxyz!#$%") || !strings.ContainsAny(pw, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") || !strings.ContainsAny(pw, "0123456789") {
			t.Fatalf("constrained password %q", pw)
		}
	}
	if _, err := c.GetRandomPassword(ctx, &sm.GetRandomPasswordInput{ExcludeLowercase: aws.Bool(true), ExcludeUppercase: aws.Bool(true),
		ExcludeNumbers: aws.Bool(true), ExcludePunctuation: aws.Bool(true)}); err == nil {
		t.Fatal("excluding everything must fail")
	}
}

// rotator follows the AWS rotation template, talking to pogi with raw JSON-protocol requests.
const rotator = `
import json, os, urllib.request, urllib.error

def call(op, body):
    req = urllib.request.Request(os.environ["AWS_ENDPOINT_URL"], data=json.dumps(body).encode(), headers={
        "X-Amz-Target": "secretsmanager." + op, "Content-Type": "application/x-amz-json-1.1"})
    try:
        return json.loads(urllib.request.urlopen(req).read() or b"{}")
    except urllib.error.HTTPError as e:
        return {"error": json.loads(e.read())["__type"]}

def handler(event, context):
    arn, token, step = event["SecretId"], event["ClientRequestToken"], event["Step"]
    print("rotating", step)
    if step == "createSecret":
        current = call("GetSecretValue", {"SecretId": arn, "VersionStage": "AWSCURRENT"})["SecretString"]
        if call("GetSecretValue", {"SecretId": arn, "VersionId": token, "VersionStage": "AWSPENDING"}).get("error") == "ResourceNotFoundException":
            call("PutSecretValue", {"SecretId": arn, "ClientRequestToken": token, "SecretString": current + "-rotated", "VersionStages": ["AWSPENDING"]})
    elif step == "testSecret":
        if "error" in call("GetSecretValue", {"SecretId": arn, "VersionId": token, "VersionStage": "AWSPENDING"}):
            raise ValueError("pending value missing")
    elif step == "finishSecret":
        stages = call("DescribeSecret", {"SecretId": arn})["VersionIdsToStages"]
        current = [v for v, s in stages.items() if "AWSCURRENT" in s][0]
        if current != token:
            call("UpdateSecretVersionStage", {"SecretId": arn, "VersionStage": "AWSCURRENT", "MoveToVersionId": token, "RemoveFromVersionId": current})
`

func TestRotation(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return smsvc.New(h) })
	cfg := awstest.Config(t, url)
	c, l := sm.NewFromConfig(cfg), lambda.NewFromConfig(cfg)

	var zipped bytes.Buffer
	zw := zip.NewWriter(&zipped)
	w, _ := zw.Create("rotate.py")
	w.Write([]byte(rotator))
	zw.Close()
	fn := must(l.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("rotate"), Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("rotate.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &lambdatypes.FunctionCode{ZipFile: zipped.Bytes()}}))

	must(c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("rotated"), SecretString: aws.String("pw")}))
	r := must(c.RotateSecret(ctx, &sm.RotateSecretInput{SecretId: aws.String("rotated"), RotationLambdaARN: fn.FunctionArn,
		RotationRules: &types.RotationRulesType{AutomaticallyAfterDays: aws.Int64(30)}}))
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("rotated")}); v != "pw-rotated" {
		t.Fatalf("after rotation = %q", v)
	}
	if v := value(t, c, &sm.GetSecretValueInput{SecretId: aws.String("rotated"), VersionStage: aws.String("AWSPREVIOUS")}); v != "pw" {
		t.Fatalf("previous after rotation = %q", v)
	}
	d := must(c.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretId: aws.String("rotated")}))
	if !aws.ToBool(d.RotationEnabled) || aws.ToString(d.RotationLambdaARN) != *fn.FunctionArn || d.LastRotatedDate == nil || d.NextRotationDate == nil ||
		strings.Join(d.VersionIdsToStages[*r.VersionId], ",") != "AWSCURRENT" {
		t.Fatalf("describe after rotation = %+v", d)
	}
	must(c.CancelRotateSecret(ctx, &sm.CancelRotateSecretInput{SecretId: aws.String("rotated")}))
	if d := must(c.DescribeSecret(ctx, &sm.DescribeSecretInput{SecretId: aws.String("rotated")})); aws.ToBool(d.RotationEnabled) {
		t.Fatal("rotation should be off after CancelRotateSecret")
	}

	// Without a function, rotation copies the current value into a new version.
	must(c.CreateSecret(ctx, &sm.CreateSecretInput{Name: aws.String("plain"), SecretString: aws.String("same")}))
	pr := must(c.RotateSecret(ctx, &sm.RotateSecretInput{SecretId: aws.String("plain")}))
	got := must(c.GetSecretValue(ctx, &sm.GetSecretValueInput{SecretId: aws.String("plain")}))
	if aws.ToString(got.SecretString) != "same" || aws.ToString(got.VersionId) != aws.ToString(pr.VersionId) {
		t.Fatalf("copy rotation = %+v", got)
	}
}
