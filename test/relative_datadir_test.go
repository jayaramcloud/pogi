package test

import (
	"context"
	"os"
	"os/exec"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// A relative data dir must not break native Lambda: the runtime starts in a
// folder under the data dir, so a relative path to its bootstrap pointed nowhere.
func TestNativeLambdaWithRelativeDataDir(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not installed")
	}
	t.Chdir(t.TempDir())
	if err := os.Mkdir("data", 0o755); err != nil {
		t.Fatal(err)
	}
	srv := pogi.Start(t, pogi.Config{DataDir: "./data", Lambda: "native"})
	lc := lambda.NewFromConfig(awstest.Config(t, srv.URL))
	ctx := context.Background()
	orFail(lc.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("f"), Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("index.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &lambdatypes.FunctionCode{ZipFile: pyZip("def handler(event, context):\n    return 'ok'\n")}}))
	out := orFail(lc.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("f")}))
	if out.FunctionError != nil || string(out.Payload) != `"ok"` {
		t.Fatalf("invoke = %v %s", aws.ToString(out.FunctionError), out.Payload)
	}
}
