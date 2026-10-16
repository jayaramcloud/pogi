package generic_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/mwaa"
	"github.com/aws/aws-sdk-go-v2/service/mwaa/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/docker"
)

func TestMWAA(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent") // statuses only; TestMWAAAirflow has the real Airflow
	s := pogi.Start(t)
	ctx := context.Background()
	c := mwaa.NewFromConfig(awstest.Config(t, s.URL), noHostPrefix)
	lifecycle(t, time.Hour)

	create, err := c.CreateEnvironment(ctx, &mwaa.CreateEnvironmentInput{Name: aws.String("airflow"), ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/mwaa"),
		SourceBucketArn: aws.String("arn:aws:s3:::dags"), DagS3Path: aws.String("dags"), AirflowVersion: aws.String("2.9.2"),
		NetworkConfiguration: &types.NetworkConfiguration{SubnetIds: []string{"subnet-1", "subnet-2"}, SecurityGroupIds: []string{"sg-1"}},
		Tags:                 map[string]string{"team": "data"}})
	if err != nil || aws.ToString(create.Arn) != "arn:aws:airflow:us-east-1:000000000000:environment/airflow" {
		t.Fatalf("create: %v %+v", err, create)
	}
	if _, err := c.CreateEnvironment(ctx, &mwaa.CreateEnvironmentInput{Name: aws.String("airflow"), ExecutionRoleArn: aws.String("x"), SourceBucketArn: aws.String("x"), DagS3Path: aws.String("x"),
		NetworkConfiguration: &types.NetworkConfiguration{SubnetIds: []string{"a", "b"}, SecurityGroupIds: []string{"s"}}}); err == nil {
		t.Fatal("duplicate environment")
	}
	get := func() *types.Environment {
		g, err := c.GetEnvironment(ctx, &mwaa.GetEnvironmentInput{Name: aws.String("airflow")})
		if err != nil {
			t.Fatal(err)
		}
		return g.Environment
	}
	if e := get(); e.Status != types.EnvironmentStatusCreating {
		t.Fatalf("creating: %+v", e)
	}
	if _, err := c.CreateCliToken(ctx, &mwaa.CreateCliTokenInput{Name: aws.String("airflow")}); err == nil {
		t.Fatal("token for an environment that is still creating")
	}
	lifecycle(t, 0)
	e := get()
	if e.Status != types.EnvironmentStatusAvailable || aws.ToString(e.AirflowVersion) != "2.9.2" || aws.ToString(e.WebserverUrl) == "" || e.Tags["team"] != "data" || aws.ToString(e.DagS3Path) != "dags" ||
		len(e.NetworkConfiguration.SubnetIds) != 2 {
		t.Fatalf("available: %+v", e)
	}
	cli, err := c.CreateCliToken(ctx, &mwaa.CreateCliTokenInput{Name: aws.String("airflow")})
	if err != nil || aws.ToString(cli.CliToken) == "" || aws.ToString(cli.WebServerHostname) != aws.ToString(e.WebserverUrl) {
		t.Fatalf("cli token: %v %+v", err, cli)
	}
	// floci #4755: the Airflow CLI endpoint takes the CLI token; pogi runs no Airflow, and says so.
	post := func(token, body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, s.URL+"/aws_mwaa/cli", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	if code, body := post("made-up", "dags list"); code != http.StatusForbidden {
		t.Fatalf("CLI with a made-up token: %d %s", code, body)
	}
	if code, body := post(aws.ToString(cli.CliToken), "dags list"); code != http.StatusNotImplemented || !strings.Contains(body, "does not run Apache Airflow") || !strings.Contains(body, "dags list") {
		t.Fatalf("CLI with a token: %d %s", code, body)
	}
	if code, _ := post(aws.ToString(cli.CliToken), " "); code != http.StatusBadRequest {
		t.Fatalf("CLI without a command: %d", code)
	}
	web, err := c.CreateWebLoginToken(ctx, &mwaa.CreateWebLoginTokenInput{Name: aws.String("airflow")})
	if err != nil || aws.ToString(web.WebToken) == "" || aws.ToString(web.AirflowIdentity) == "" {
		t.Fatalf("web token: %v %+v", err, web)
	}
	if l, err := c.ListEnvironments(ctx, &mwaa.ListEnvironmentsInput{}); err != nil || len(l.Environments) != 1 || l.Environments[0] != "airflow" {
		t.Fatalf("list: %v %+v", err, l)
	}

	// update goes through UPDATING
	lifecycle(t, time.Hour)
	if _, err := c.UpdateEnvironment(ctx, &mwaa.UpdateEnvironmentInput{Name: aws.String("airflow"), MaxWorkers: aws.Int32(20), EnvironmentClass: aws.String("mw1.medium")}); err != nil {
		t.Fatal(err)
	}
	if e := get(); e.Status != types.EnvironmentStatusUpdating || aws.ToInt32(e.MaxWorkers) != 20 || aws.ToString(e.EnvironmentClass) != "mw1.medium" {
		t.Fatalf("updating: %+v", e)
	}
	lifecycle(t, 0)
	if e := get(); e.Status != types.EnvironmentStatusAvailable {
		t.Fatalf("updated: %+v", e)
	}

	// tags
	arn := aws.ToString(create.Arn)
	if _, err := c.TagResource(ctx, &mwaa.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &mwaa.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListTagsForResource(ctx, &mwaa.ListTagsForResourceInput{ResourceArn: aws.String(arn)}); err != nil || len(l.Tags) != 1 || l.Tags["env"] != "dev" {
		t.Fatalf("tags: %v %+v", err, l)
	}

	// delete and not found
	if _, err := c.DeleteEnvironment(ctx, &mwaa.DeleteEnvironmentInput{Name: aws.String("airflow")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetEnvironment(ctx, &mwaa.GetEnvironmentInput{Name: aws.String("airflow")})
	var nf *types.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("get deleted: %v", err)
	}
}

// noHostPrefix keeps the calls on the endpoint host instead of api.<host>.
func noHostPrefix(o *mwaa.Options) {
	o.APIOptions = append(o.APIOptions, func(s *middleware.Stack) error {
		return s.Initialize.Add(middleware.InitializeMiddlewareFunc("noHostPrefix",
			func(ctx context.Context, in middleware.InitializeInput, next middleware.InitializeHandler) (middleware.InitializeOutput, middleware.Metadata, error) {
				return next.HandleInitialize(smithyhttp.DisableEndpointHostPrefix(ctx, true), in)
			}), middleware.Before)
	})
}

// floci #4755, #3436: with a container engine the environment runs the official Airflow image with the DAGs of
// its S3 path and its configuration options; the CLI endpoint and the web server host go to it; deleting it
// removes the container. Needs Docker (and the Airflow image).
func TestMWAAAirflow(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a container engine")
	}
	s := pogi.Start(t)
	ctx := context.Background()
	cfg := awstest.Config(t, s.URL)
	s3c := s3.NewFromConfig(cfg, func(o *s3.Options) { o.UsePathStyle = true })
	if _, err := s3c.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("dags")}); err != nil {
		t.Fatal(err)
	}
	dag := func(id string) {
		t.Helper()
		src := "from airflow import DAG\nfrom datetime import datetime\nwith DAG('" + id + "', start_date=datetime(2024, 1, 1), schedule=None):\n    pass\n"
		if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("dags"), Key: aws.String("dags/" + id + ".py"), Body: strings.NewReader(src)}); err != nil {
			t.Fatal(err)
		}
	}
	dag("first_dag")
	var plugin bytes.Buffer
	zw := zip.NewWriter(&plugin)
	w, _ := zw.Create("pogi_plugin.py")
	w.Write([]byte("PLUGIN = 1\n"))
	zw.Close()
	for key, body := range map[string]string{"requirements.txt": "# a small package\ncowsay==6.1\n", "plugins.zip": plugin.String(),
		"startup.sh": "echo ran > /tmp/startup-ran\nexport POGI_STARTUP=yes\n"} {
		if _, err := s3c.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("dags"), Key: aws.String(key), Body: strings.NewReader(body)}); err != nil {
			t.Fatal(err)
		}
	}
	c := mwaa.NewFromConfig(cfg, noHostPrefix)
	if _, err := c.CreateEnvironment(ctx, &mwaa.CreateEnvironmentInput{Name: aws.String("real"), ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/mwaa"),
		SourceBucketArn: aws.String("arn:aws:s3:::dags"), DagS3Path: aws.String("dags"), AirflowVersion: aws.String("2.10.5"),
		AirflowConfigurationOptions: map[string]string{"core.default_timezone": "Asia/Kolkata"},
		RequirementsS3Path:          aws.String("requirements.txt"), PluginsS3Path: aws.String("plugins.zip"), StartupScriptS3Path: aws.String("startup.sh"),
		NetworkConfiguration: &types.NetworkConfiguration{SubnetIds: []string{"subnet-1", "subnet-2"}, SecurityGroupIds: []string{"sg-1"}}}); err != nil {
		t.Fatal(err)
	}
	get := func() *types.Environment {
		g, err := c.GetEnvironment(ctx, &mwaa.GetEnvironmentInput{Name: aws.String("real")})
		if err != nil {
			t.Fatal(err)
		}
		return g.Environment
	}
	if st := get().Status; st != types.EnvironmentStatusCreating {
		t.Fatalf("creating: %s", st)
	}
	for end := time.Now().Add(8 * time.Minute); get().Status != types.EnvironmentStatusAvailable; time.Sleep(2 * time.Second) {
		if e := get(); time.Now().After(end) || e.Status == types.EnvironmentStatusCreateFailed {
			t.Fatalf("never became available: %+v", e)
		}
	}
	cli, err := c.CreateCliToken(ctx, &mwaa.CreateCliTokenInput{Name: aws.String("real")})
	if err != nil {
		t.Fatal(err)
	}
	post := func(body string) (int, string) {
		req, _ := http.NewRequest(http.MethodPost, s.URL+"/aws_mwaa/cli", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+aws.ToString(cli.CliToken))
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		var out struct{ Stdout string }
		if json.Unmarshal(b, &out) != nil {
			return res.StatusCode, string(b)
		}
		o, _ := base64.StdEncoding.DecodeString(out.Stdout)
		return res.StatusCode, string(o)
	}
	if code, out := post("dags list"); code != 200 || !strings.Contains(out, "first_dag") {
		t.Fatalf("dags list: %d %s", code, out)
	}
	if code, out := post("config get-value core default_timezone"); code != 200 || !strings.Contains(out, "Asia/Kolkata") {
		t.Fatalf("option: %d %s", code, out)
	}
	if code, _ := post("webserver"); code != http.StatusBadRequest {
		t.Fatalf("a command MWAA does not run: %d", code)
	}
	// the web server host goes to Airflow
	req, _ := http.NewRequest(http.MethodGet, s.URL+"/health", nil)
	req.Host = aws.ToString(get().WebserverUrl)
	if res, err := http.DefaultClient.Do(req); err != nil || res.StatusCode != 200 {
		t.Fatalf("web server: %v %v", err, res)
	}
	// a DAG added to S3 later shows up (the sync runs every 15 seconds)
	dag("second_dag")
	found := false
	for end := time.Now().Add(2 * time.Minute); !found && time.Now().Before(end); time.Sleep(5 * time.Second) {
		_, out := post("dags list")
		found = strings.Contains(out, "second_dag")
	}
	if !found {
		t.Fatal("the DAG added later never showed up")
	}

	// requirements, plugins and the startup script were applied
	name := docker.Name("mwaa", "real")
	for _, cmd := range [][]string{{"python", "-c", "import cowsay"}, {"cat", "/opt/airflow/plugins/pogi_plugin.py"}, {"cat", "/tmp/startup-ran"}} {
		if out, code, err := d.Exec(ctx, name, cmd...); err != nil || code != 0 {
			t.Fatalf("%v: %v %d %s", cmd, err, code, out)
		}
	}

	// an update runs Airflow again with the new settings
	if _, err := c.UpdateEnvironment(ctx, &mwaa.UpdateEnvironmentInput{Name: aws.String("real"),
		AirflowConfigurationOptions: map[string]string{"core.default_timezone": "Europe/Paris"}}); err != nil {
		t.Fatal(err)
	}
	if st := get().Status; st != types.EnvironmentStatusUpdating {
		t.Fatalf("updating: %s", st)
	}
	for end := time.Now().Add(8 * time.Minute); get().Status != types.EnvironmentStatusAvailable; time.Sleep(2 * time.Second) {
		if e := get(); time.Now().After(end) || e.Status == types.EnvironmentStatusUpdateFailed {
			t.Fatalf("update never finished: %+v", e)
		}
	}
	cli, err = c.CreateCliToken(ctx, &mwaa.CreateCliTokenInput{Name: aws.String("real")})
	if err != nil {
		t.Fatal(err)
	}
	if code, out := post("config get-value core default_timezone"); code != 200 || !strings.Contains(out, "Europe/Paris") {
		t.Fatalf("option after the update: %d %s", code, out)
	}

	if _, err := c.DeleteEnvironment(ctx, &mwaa.DeleteEnvironmentInput{Name: aws.String("real")}); err != nil {
		t.Fatal(err)
	}
	for end := time.Now().Add(time.Minute); ; time.Sleep(time.Second) {
		if _, err := d.Inspect(ctx, name); err != nil {
			break
		}
		if time.Now().After(end) {
			t.Fatal("the Airflow container is still there after the delete")
		}
	}
}
