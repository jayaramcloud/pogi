package ssm_test

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	"github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	smsvc "github.com/nrndev/pogi/internal/aws/secretsmanager"
	ssmsvc "github.com/nrndev/pogi/internal/aws/ssm"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// setupAll serves SSM and Secrets Manager on one hub.
func setupAll(t *testing.T) (*ssm.Client, *secretsmanager.Client, *core.Hub) {
	hub, url := core.ServeTest(t, func(h *core.Hub) core.Service { return ssmsvc.New(h) }, func(h *core.Hub) core.Service { return smsvc.New(h) })
	cfg := awstest.Config(t, url)
	return ssm.NewFromConfig(cfg), secretsmanager.NewFromConfig(cfg), hub
}

func code(err error) string {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		return ae.ErrorCode()
	}
	return fmt.Sprint(err)
}

// floci #4580: /aws/reference/secretsmanager/<id> reads a secret, with decryption, with a stage selector.
func TestSecretsManagerReference(t *testing.T) {
	c, sm, _ := setupAll(t)
	must(sm.CreateSecret(ctx, &secretsmanager.CreateSecretInput{Name: aws.String("db/pass"), SecretString: aws.String("s3cret")}))
	got := must(c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/aws/reference/secretsmanager/db/pass"), WithDecryption: aws.Bool(true)})).Parameter
	if aws.ToString(got.Value) != "s3cret" || got.Type != types.ParameterTypeSecureString || got.Version != 0 {
		t.Errorf("got %+v", got)
	}
	if _, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/aws/reference/secretsmanager/db/pass")}); code(err) != "ValidationException" {
		t.Errorf("without decryption: %v", err)
	}
	if _, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/aws/reference/secretsmanager/db/pass:AWSCURRENT"), WithDecryption: aws.Bool(true)}); err != nil {
		t.Errorf("stage selector: %v", err)
	}
	if _, err := c.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String("/aws/reference/secretsmanager/nope"), WithDecryption: aws.Bool(true)}); code(err) != "ParameterNotFound" {
		t.Errorf("missing secret: %v", err)
	}
	many := must(c.GetParameters(ctx, &ssm.GetParametersInput{Names: []string{"/aws/reference/secretsmanager/db/pass", "/aws/reference/secretsmanager/nope"}, WithDecryption: aws.Bool(true)}))
	if len(many.Parameters) != 1 || len(many.InvalidParameters) != 1 {
		t.Errorf("GetParameters: %+v", many)
	}
}

// floci #2235: GetServiceSetting, UpdateServiceSetting and ResetServiceSetting.
func TestServiceSettings(t *testing.T) {
	c, _, _ := setupAll(t)
	id := aws.String("/ssm/documents/console/public-sharing-permission")
	g := must(c.GetServiceSetting(ctx, &ssm.GetServiceSettingInput{SettingId: id})).ServiceSetting
	if aws.ToString(g.SettingValue) != "Enable" || aws.ToString(g.Status) != "Default" || !strings.HasSuffix(aws.ToString(g.ARN), ":servicesetting/ssm/documents/console/public-sharing-permission") {
		t.Fatalf("default = %+v", g)
	}
	must(c.UpdateServiceSetting(ctx, &ssm.UpdateServiceSettingInput{SettingId: id, SettingValue: aws.String("Disable")}))
	g = must(c.GetServiceSetting(ctx, &ssm.GetServiceSettingInput{SettingId: id})).ServiceSetting
	if aws.ToString(g.SettingValue) != "Disable" || aws.ToString(g.Status) != "Customized" {
		t.Fatalf("after update = %+v", g)
	}
	if _, err := c.UpdateServiceSetting(ctx, &ssm.UpdateServiceSettingInput{SettingId: id, SettingValue: aws.String("Maybe")}); code(err) != "ValidationException" {
		t.Errorf("bad value: %v", err)
	}
	g = must(c.ResetServiceSetting(ctx, &ssm.ResetServiceSettingInput{SettingId: id})).ServiceSetting
	if aws.ToString(g.SettingValue) != "Enable" || aws.ToString(g.Status) != "Default" {
		t.Fatalf("after reset = %+v", g)
	}
	if _, err := c.GetServiceSetting(ctx, &ssm.GetServiceSettingInput{SettingId: aws.String("/ssm/nope")}); code(err) != "ServiceSettingNotFound" {
		t.Errorf("unknown setting: %v", err)
	}
}

const echoDoc = `{"schemaVersion":"2.2","description":"d","parameters":{"who":{"type":"String","default":"world","description":"whom"}},
"mainSteps":[{"action":"aws:runShellScript","name":"x","inputs":{"runCommand":["echo hello {{ who }}"]}}]}`

// floci #2306: documents (versions, describe, list, tags, delete) and associations.
func TestDocumentsAndAssociations(t *testing.T) {
	c, _, _ := setupAll(t)
	created := must(c.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: aws.String("my-doc"), DocumentType: types.DocumentTypeCommand, DocumentFormat: types.DocumentFormatJson,
		Content: aws.String(echoDoc), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})).DocumentDescription
	if aws.ToString(created.DocumentVersion) != "1" || aws.ToString(created.Description) != "d" || len(created.Parameters) != 1 || aws.ToString(created.Parameters[0].DefaultValue) != "world" ||
		aws.ToString(created.Hash) == "" || len(created.Tags) != 1 {
		t.Fatalf("created = %+v", created)
	}
	if _, err := c.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: aws.String("my-doc"), Content: aws.String(echoDoc)}); code(err) != "DocumentAlreadyExists" {
		t.Errorf("duplicate: %v", err)
	}
	if _, err := c.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: aws.String("bad-doc"), Content: aws.String("{not json")}); code(err) != "InvalidDocumentContent" {
		t.Errorf("bad content: %v", err)
	}
	if _, err := c.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: aws.String("AWS-mine"), Content: aws.String(echoDoc)}); code(err) != "ValidationException" {
		t.Errorf("aws prefix: %v", err)
	}
	// a second version, YAML content, then the default moves
	up := must(c.UpdateDocument(ctx, &ssm.UpdateDocumentInput{Name: aws.String("my-doc"), DocumentVersion: aws.String("$LATEST"), VersionName: aws.String("two"),
		Content: aws.String(strings.Replace(echoDoc, "d\"", "d2\"", 1))})).DocumentDescription
	if aws.ToString(up.DocumentVersion) != "2" || aws.ToString(up.DefaultVersion) != "1" || aws.ToString(up.LatestVersion) != "2" {
		t.Fatalf("updated = %+v", up)
	}
	if _, err := c.UpdateDocument(ctx, &ssm.UpdateDocumentInput{Name: aws.String("my-doc"), Content: aws.String(strings.Replace(echoDoc, "d\"", "d2\"", 1))}); code(err) != "DuplicateDocumentContent" {
		t.Errorf("same content: %v", err)
	}
	must(c.UpdateDocumentDefaultVersion(ctx, &ssm.UpdateDocumentDefaultVersionInput{Name: aws.String("my-doc"), DocumentVersion: aws.String("2")}))
	d := must(c.DescribeDocument(ctx, &ssm.DescribeDocumentInput{Name: aws.String("my-doc")})).Document
	if aws.ToString(d.DocumentVersion) != "2" || aws.ToString(d.Description) != "d2" || aws.ToString(d.VersionName) != "two" {
		t.Errorf("describe default = %+v", d)
	}
	g := must(c.GetDocument(ctx, &ssm.GetDocumentInput{Name: aws.String("my-doc"), DocumentVersion: aws.String("1")}))
	if g.Content == nil || !strings.Contains(*g.Content, "hello") || aws.ToString(g.DocumentVersion) != "1" {
		t.Errorf("get v1 = %+v", g)
	}
	if v := must(c.ListDocumentVersions(ctx, &ssm.ListDocumentVersionsInput{Name: aws.String("my-doc")})).DocumentVersions; len(v) != 2 || !v[1].IsDefaultVersion {
		t.Errorf("versions = %+v", v)
	}
	if _, err := c.DeleteDocument(ctx, &ssm.DeleteDocumentInput{Name: aws.String("my-doc"), DocumentVersion: aws.String("2")}); code(err) != "InvalidDocumentOperation" {
		t.Errorf("delete default version: %v", err)
	}
	must(c.DeleteDocument(ctx, &ssm.DeleteDocumentInput{Name: aws.String("my-doc"), DocumentVersion: aws.String("1")}))

	own := must(c.ListDocuments(ctx, &ssm.ListDocumentsInput{Filters: []types.DocumentKeyValuesFilter{{Key: aws.String("Owner"), Values: []string{"Self"}}}})).DocumentIdentifiers
	if len(own) != 1 || aws.ToString(own[0].Name) != "my-doc" {
		t.Errorf("Owner=Self: %+v", own)
	}
	if all := must(c.ListDocuments(ctx, &ssm.ListDocumentsInput{MaxResults: aws.Int32(50)})).DocumentIdentifiers; len(all) < 3 {
		t.Errorf("all documents: %d", len(all))
	}
	tags := must(c.ListTagsForResource(ctx, &ssm.ListTagsForResourceInput{ResourceType: types.ResourceTypeForTaggingDocument, ResourceId: aws.String("my-doc")})).TagList
	if len(tags) != 1 {
		t.Errorf("tags = %v", tags)
	}
	if p := must(c.DescribeDocumentPermission(ctx, &ssm.DescribeDocumentPermissionInput{Name: aws.String("my-doc"), PermissionType: types.DocumentPermissionTypeShare})); len(p.AccountIds) != 0 {
		t.Errorf("permission = %+v", p)
	}
	if _, err := c.DeleteDocument(ctx, &ssm.DeleteDocumentInput{Name: aws.String("AWS-RunShellScript")}); code(err) != "InvalidDocumentOperation" {
		t.Errorf("delete AWS document: %v", err)
	}

	// associations
	if _, err := c.CreateAssociation(ctx, &ssm.CreateAssociationInput{Name: aws.String("nope"), InstanceId: aws.String("i-0123456789abcdef0")}); code(err) != "InvalidDocument" {
		t.Errorf("association of a missing document: %v", err)
	}
	in := &ssm.CreateAssociationInput{Name: aws.String("my-doc"), AssociationName: aws.String("a1"), Targets: []types.Target{{Key: aws.String("InstanceIds"), Values: []string{"i-0123456789abcdef0"}}}}
	a := must(c.CreateAssociation(ctx, in)).AssociationDescription
	if aws.ToString(a.AssociationId) == "" || aws.ToString(a.AssociationVersion) != "1" || a.Overview == nil || aws.ToString(a.Overview.Status) != "Pending" {
		t.Fatalf("association = %+v", a)
	}
	if _, err := c.CreateAssociation(ctx, in); code(err) != "AssociationAlreadyExists" {
		t.Errorf("duplicate association: %v", err)
	}
	if l := must(c.ListAssociations(ctx, &ssm.ListAssociationsInput{})).Associations; len(l) != 1 {
		t.Errorf("list = %+v", l)
	}
	u := must(c.UpdateAssociation(ctx, &ssm.UpdateAssociationInput{AssociationId: a.AssociationId, ScheduleExpression: aws.String("rate(1 hour)")})).AssociationDescription
	if aws.ToString(u.AssociationVersion) != "2" || aws.ToString(u.ScheduleExpression) != "rate(1 hour)" {
		t.Errorf("updated association = %+v", u)
	}
	must(c.AddTagsToResource(ctx, &ssm.AddTagsToResourceInput{ResourceType: types.ResourceTypeForTaggingAssociation, ResourceId: a.AssociationId, Tags: []types.Tag{{Key: aws.String("k"), Value: aws.String("v")}}}))
	must(c.DeleteAssociation(ctx, &ssm.DeleteAssociationInput{AssociationId: a.AssociationId}))
	if _, err := c.DescribeAssociation(ctx, &ssm.DescribeAssociationInput{AssociationId: a.AssociationId}); code(err) != "AssociationDoesNotExist" {
		t.Errorf("describe after delete: %v", err)
	}
	must(c.DeleteDocument(ctx, &ssm.DeleteDocumentInput{Name: aws.String("my-doc")}))
	if _, err := c.GetDocument(ctx, &ssm.GetDocumentInput{Name: aws.String("my-doc")}); code(err) != "InvalidDocument" {
		t.Errorf("get after delete: %v", err)
	}
}

// floci #4741: SendCommand refuses more than 50 InstanceIds and instances that are not running.
func TestSendCommandLimits(t *testing.T) {
	c, _, _ := setupAll(t)
	var ids []string
	for i := 1; i <= 51; i++ {
		ids = append(ids, fmt.Sprintf("i-%017d", i))
	}
	_, err := c.SendCommand(ctx, &ssm.SendCommandInput{DocumentName: aws.String("AWS-RunShellScript"), InstanceIds: ids, Parameters: map[string][]string{"commands": {"echo hi"}}})
	if code(err) != "ValidationException" || !strings.Contains(err.Error(), "less than or equal to 50") {
		t.Errorf("51 ids: %v", err)
	}
	_, err = c.SendCommand(ctx, &ssm.SendCommandInput{DocumentName: aws.String("AWS-RunShellScript"), InstanceIds: ids[:1], Parameters: map[string][]string{"commands": {"echo hi"}}})
	if code(err) != "InvalidInstanceId" {
		t.Errorf("instance that does not run: %v", err)
	}
	_, err = c.SendCommand(ctx, &ssm.SendCommandInput{DocumentName: aws.String("No-Such-Doc"), InstanceIds: ids[:1]})
	if code(err) != "InvalidDocument" {
		t.Errorf("missing document: %v", err)
	}
}

// TestRunCommand runs commands in the container of an instance (needs a container engine).
func TestRunCommand(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	c, _, hub := setupAll(t)
	const id = "i-0aabbccddeeff0011"
	name := docker.Name("ec2", id)
	if _, err := hub.Docker().Run(ctx, &docker.Container{Name: name, Image: "alpine:3", Service: "ec2", Resource: id, Cmd: []string{"sleep", "600"}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Remove(ctx, name) })

	wait := func(cmd, instance string) *ssm.GetCommandInvocationOutput {
		t.Helper()
		for end := time.Now().Add(30 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
			out, err := c.GetCommandInvocation(ctx, &ssm.GetCommandInvocationInput{CommandId: aws.String(cmd), InstanceId: aws.String(instance)})
			if err != nil {
				t.Fatal(err)
			}
			if out.Status != types.CommandInvocationStatusPending && out.Status != types.CommandInvocationStatusInProgress {
				return out
			}
		}
		t.Fatal("command did not finish")
		return nil
	}
	sent := must(c.SendCommand(ctx, &ssm.SendCommandInput{DocumentName: aws.String("AWS-RunShellScript"), InstanceIds: []string{id}, Comment: aws.String("hi"),
		Parameters: map[string][]string{"commands": {"echo hello", "echo world"}}})).Command
	if aws.ToString(sent.CommandId) == "" || sent.Status != types.CommandStatusPending || sent.TargetCount != 1 {
		t.Fatalf("sent = %+v", sent)
	}
	out := wait(*sent.CommandId, id)
	if out.Status != types.CommandInvocationStatusSuccess || aws.ToString(out.StandardOutputContent) != "hello\nworld\n" || out.ResponseCode != 0 || aws.ToString(out.Comment) != "hi" {
		t.Fatalf("invocation = %+v", out)
	}
	// failure, with the exit code
	sent = must(c.SendCommand(ctx, &ssm.SendCommandInput{DocumentName: aws.String("AWS-RunShellScript"), InstanceIds: []string{id}, Parameters: map[string][]string{"commands": {"echo oops; exit 3"}}})).Command
	if out = wait(*sent.CommandId, id); out.Status != types.CommandInvocationStatusFailed || out.ResponseCode != 3 || !strings.Contains(*out.StandardOutputContent, "oops") {
		t.Fatalf("failed invocation = %+v", out)
	}
	if l := must(c.ListCommands(ctx, &ssm.ListCommandsInput{CommandId: sent.CommandId})).Commands; len(l) != 1 || l[0].Status != types.CommandStatusFailed || l[0].ErrorCount != 1 {
		t.Errorf("ListCommands = %+v", l)
	}
	li := must(c.ListCommandInvocations(ctx, &ssm.ListCommandInvocationsInput{InstanceId: aws.String(id), Details: true})).CommandInvocations
	if len(li) != 2 || len(li[0].CommandPlugins) != 1 || aws.ToString(li[0].CommandPlugins[0].Name) != "aws:runShellScript" {
		t.Errorf("ListCommandInvocations = %+v", li)
	}
	// a custom document with a parameter default and an override
	must(c.CreateDocument(ctx, &ssm.CreateDocumentInput{Name: aws.String("greet"), DocumentType: types.DocumentTypeCommand, Content: aws.String(echoDoc)}))
	for who, want := range map[string]string{"": "hello world\n", "pogi": "hello pogi\n"} {
		in := &ssm.SendCommandInput{DocumentName: aws.String("greet"), Targets: []types.Target{{Key: aws.String("InstanceIds"), Values: []string{id}}}}
		if who != "" {
			in.Parameters = map[string][]string{"who": {who}}
		}
		sent = must(c.SendCommand(ctx, in)).Command
		if out = wait(*sent.CommandId, id); aws.ToString(out.StandardOutputContent) != want || aws.ToString(out.PluginName) != "x" {
			t.Errorf("greet %q = %+v", who, out)
		}
	}
	// an association runs its document once on its instances
	a := must(c.CreateAssociation(ctx, &ssm.CreateAssociationInput{Name: aws.String("greet"), InstanceId: aws.String(id)})).AssociationDescription
	for end := time.Now().Add(20 * time.Second); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if got := must(c.DescribeAssociation(ctx, &ssm.DescribeAssociationInput{AssociationId: a.AssociationId})).AssociationDescription; aws.ToString(got.Overview.Status) == "Success" {
			return
		}
	}
	t.Error("association never reached Success")
}
