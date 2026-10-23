package test

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

// A code zip with a "../" or absolute entry is refused when the function is created or its code is
// updated, with Lambda's InvalidParameterValueException, not only when it first runs.
func TestLambdaZipWithEscapingPathIsRefusedAtUpload(t *testing.T) {
	srv := pogi.Start(t)
	zipWith := func(names ...string) []byte {
		var b bytes.Buffer
		zw := zip.NewWriter(&b)
		for _, n := range names {
			w, _ := zw.Create(n)
			io.WriteString(w, "def handler(e, c):\n    return 1\n")
		}
		zw.Close()
		return b.Bytes()
	}
	fn := lambda.NewFromConfig(awstest.Config(t, srv.URL))
	ctx := context.Background()
	create := func(name string, z []byte) error {
		_, err := fn.CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: aws.String(name), Runtime: lambdatypes.RuntimePython312,
			Handler: aws.String("index.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"), Code: &lambdatypes.FunctionCode{ZipFile: z}})
		return err
	}
	for _, bad := range []string{"../../zipslip.txt", "/abs-zipslip.txt"} {
		err := create("slip", zipWith("index.py", bad))
		if err == nil || !strings.Contains(err.Error(), "InvalidParameterValueException") || !strings.Contains(err.Error(), bad) {
			t.Errorf("CreateFunction with %q: err = %v, want InvalidParameterValueException naming the entry", bad, err)
		}
	}
	if err := create("fine", zipWith("index.py")); err != nil {
		t.Fatal(err)
	}
	_, err := fn.UpdateFunctionCode(ctx, &lambda.UpdateFunctionCodeInput{FunctionName: aws.String("fine"), ZipFile: zipWith("index.py", "../x")})
	if err == nil || !strings.Contains(err.Error(), "InvalidParameterValueException") {
		t.Errorf("UpdateFunctionCode with ../x: err = %v, want InvalidParameterValueException", err)
	}
}
