package generic_test

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/backup"
	backuptypes "github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/aws/aws-sdk-go-v2/service/codeartifact"
	catypes "github.com/aws/aws-sdk-go-v2/service/codeartifact/types"
	"github.com/aws/aws-sdk-go-v2/service/swf"
	"github.com/aws/smithy-go"
	"github.com/aws/smithy-go/middleware"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// without client-side validation, so the server's answer is what the SDK reports
func noClientChecks(o *codeartifact.Options) {
	o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
		_, err := s.Initialize.Remove("OperationInputValidation")
		return err
	})
}

func TestValidation(t *testing.T) {
	_, url := start(t, "codeartifact", "swf")
	ctx := context.Background()
	// the model's own ValidationException, typed
	c := codeartifact.NewFromConfig(awstest.Config(t, url), noClientChecks)
	_, err := c.CreateDomain(ctx, &codeartifact.CreateDomainInput{})
	var ve *catypes.ValidationException
	if !errors.As(err, &ve) {
		t.Fatalf("%T %v", err, err)
	}
	// a model without one: an API error with the protocol's generic code
	s := swf.NewFromConfig(awstest.Config(t, url), func(o *swf.Options) {
		o.APIOptions = append(o.APIOptions, func(st *middleware.Stack) error {
			_, err := st.Initialize.Remove("OperationInputValidation")
			return err
		})
	})
	_, err = s.RegisterDomain(ctx, &swf.RegisterDomainInput{})
	var ae smithy.APIError
	if !errors.As(err, &ae) || ae.ErrorCode() != "ValidationException" {
		t.Fatalf("%T %v", err, err)
	}
	// unknown operations and malformed bodies are errors too
	for _, r := range []struct {
		method, path string
		code         int
	}{{"GET", "/v1/nothing", 404}} {
		req, _ := http.NewRequest(r.method, url+r.path, nil)
		req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/codeartifact/aws4_request")
		resp, err := http.DefaultClient.Do(req)
		if err != nil || resp.StatusCode != r.code {
			t.Fatalf("%s: %v %v", r.path, err, resp)
		}
		resp.Body.Close()
	}
}

var hooked struct {
	sync.Mutex
	names []string
	once  sync.Once
}

// An override changes one operation's output, and a hook runs after a create.
func TestOverrideAndHook(t *testing.T) {
	hooked.once.Do(func() {
		generic.Override("backup", "ListBackupPlans", func(c *generic.Call) (map[string]any, error) {
			out, err := c.Default()
			if err != nil {
				return nil, err
			}
			out["NextToken"] = "from-override"
			return out, nil
		})
		generic.Hook("backup", "CreateBackupPlan", func(c *generic.Call) {
			hooked.Lock()
			hooked.names = append(hooked.names, c.Res.ID)
			hooked.Unlock()
		})
	})
	_, url := start(t, "backup")
	ctx := context.Background()
	c := backup.NewFromConfig(awstest.Config(t, url))
	plan, err := c.CreateBackupPlan(ctx, &backup.CreateBackupPlanInput{BackupPlan: &backuptypes.BackupPlanInput{BackupPlanName: aws.String("p"), Rules: []backuptypes.BackupRuleInput{{RuleName: aws.String("r"), TargetBackupVaultName: aws.String("v")}}}})
	if err != nil {
		t.Fatal(err)
	}
	hooked.Lock()
	got := append([]string(nil), hooked.names...)
	hooked.Unlock()
	if len(got) == 0 || got[len(got)-1] != aws.ToString(plan.BackupPlanId) {
		t.Fatalf("hook saw %v, created %s", got, aws.ToString(plan.BackupPlanId))
	}
	l, err := c.ListBackupPlans(ctx, &backup.ListBackupPlansInput{})
	if err != nil || aws.ToString(l.NextToken) != "from-override" || len(l.BackupPlansList) != 1 {
		t.Fatalf("%v %+v", err, l)
	}
}

func TestPersistence(t *testing.T) {
	h, url := start(t, "efs")
	_ = url
	svc := h.Service("efs").(*generic.Service)
	out, err := svc.Do("000000000000", "us-east-1", "CreateFileSystem", map[string]any{"CreationToken": "tok", "Tags": []any{map[string]any{"Key": "k", "Value": "v"}}})
	if err != nil {
		t.Fatal(err)
	}
	id := out["FileSystemId"].(string)
	b, err := svc.Save()
	if err != nil {
		t.Fatal(err)
	}
	h2, _ := start(t, "efs")
	other := h2.Service("efs").(*generic.Service)
	if err := other.Load(b); err != nil {
		t.Fatal(err)
	}
	r := other.Get("000000000000", "us-east-1", "FileSystem", id)
	if r == nil || r.Tags["k"] != "v" || r.Data["CreationToken"] != "tok" {
		t.Fatalf("%+v", r)
	}
	got, err := other.Do("000000000000", "us-east-1", "DescribeFileSystems", map[string]any{"FileSystemId": id})
	if err != nil || len(got["FileSystems"].([]any)) != 1 {
		t.Fatalf("%v %v", err, got)
	}
	// Cloud Control's view: list, get and delete by type
	if types := other.Types(); len(types) == 0 {
		t.Fatal("no types")
	}
	if len(other.List("000000000000", "us-east-1", "FileSystem")) != 1 || !other.Delete("000000000000", "us-east-1", "FileSystem", id) || other.Get("000000000000", "us-east-1", "FileSystem", id) != nil {
		t.Fatal("list/get/delete")
	}
	other.Reset()
	if len(other.List("000000000000", "us-east-1", "FileSystem")) != 0 {
		t.Fatal("reset")
	}
	var _ core.Stateful = other
	var _ core.Browsable = other
	var _ core.Inspectable = other
}
