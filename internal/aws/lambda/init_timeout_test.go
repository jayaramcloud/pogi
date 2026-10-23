package lambda_test

import (
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/aws/aws-sdk-go-v2/service/lambda/types"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// lambda.initTimeout bounds a cold start (AWS's 10 seconds by default; pogi's own tests use a minute, so a slow
// start on a busy machine passes): a function whose init outlasts it fails, saying the runtime never asked for work.
func TestInitTimeoutBoundsAColdStart(t *testing.T) {
	need(t, "python3")
	h, url := core.ServeTest(t, func(h *core.Hub) core.Service { return lambdasvc.New(h) })
	if h.Cfg.LambdaInit != core.TestLambdaInit {
		t.Fatalf("tests start Lambda with a limit of %v, want %v", h.Cfg.LambdaInit, core.TestLambdaInit)
	}
	h.Cfg.LambdaInit = time.Second
	c := lambda.NewFromConfig(awstest.Config(t, url))
	must(c.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String("slow"), Runtime: types.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &types.FunctionCode{ZipFile: zipFiles(map[string]string{"app.py": "import time\ntime.sleep(30)\n\ndef handler(e, c):\n    return 1\n"})}}))
	start := time.Now()
	_, err := c.Invoke(ctx, &lambda.InvokeInput{FunctionName: aws.String("slow")})
	if err == nil || !strings.Contains(err.Error(), "did not start within 1s: its runtime never asked for an invocation") {
		t.Fatalf("got %v", err)
	}
	if took := time.Since(start); took > 10*time.Second {
		t.Errorf("took %v: the limit of 1s was not used", took)
	}
}
