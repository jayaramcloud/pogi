package test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/smithy-go"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// Input that breaks a limit of the AWS API model is refused the way AWS refuses it, whether the
// service is hand-written (Kinesis: JSON, IAM: query) or generated (EFS), and in every IAM mode.
func TestInputLimits(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t, pogi.Config{IAM: "strict"})
	cfg := awstest.Config(t, srv.URL)
	for _, c := range []struct {
		name, code, member, rule string
		call                     func() error
	}{
		{"kinesis (json)", "ValidationException", "'limit'", "greater than or equal to 1", func() error {
			_, err := kinesis.NewFromConfig(cfg).ListStreams(ctx, &kinesis.ListStreamsInput{Limit: aws.Int32(0)})
			return err
		}},
		{"iam (query)", "ValidationError", "'maxItems'", "greater than or equal to 1", func() error {
			_, err := iam.NewFromConfig(cfg).ListUsers(ctx, &iam.ListUsersInput{MaxItems: aws.Int32(0)})
			return err
		}},
		{"lambda (its own code, enum)", "InvalidParameterValueException", "'compatibleRuntime'", "satisfy enum value set", func() error {
			_, err := lambda.NewFromConfig(cfg).ListLayers(ctx, &lambda.ListLayersInput{CompatibleRuntime: "bogus9.x"})
			return err
		}},
		{"efs (generated, its own error shape)", "BadRequest", "'maxItems'", "greater than or equal to 1", func() error {
			_, err := efs.NewFromConfig(cfg).DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{MaxItems: aws.Int32(0)})
			return err
		}},
	} {
		err := c.call()
		var ae smithy.APIError
		if err == nil || !strings.Contains(err.Error(), c.member) || !strings.Contains(err.Error(), c.rule) ||
			!strings.Contains(err.Error(), "1 validation error detected") {
			t.Errorf("%s: %v", c.name, err)
			continue
		}
		if !errors.As(err, &ae) || ae.ErrorCode() != c.code {
			t.Errorf("%s: code %v, want %s", c.name, err, c.code)
		}
	}
}
