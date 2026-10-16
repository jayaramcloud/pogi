package timestreaminfluxdb_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	tsi "github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb"
	"github.com/aws/aws-sdk-go-v2/service/timestreaminfluxdb/types"
	"github.com/aws/smithy-go"

	sm "github.com/nrndev/pogi/internal/aws/secretsmanager"
	"github.com/nrndev/pogi/internal/aws/timestreaminfluxdb"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

func code(err error) string {
	var api smithy.APIError
	if errors.As(err, &api) {
		return api.ErrorCode()
	}
	return fmt.Sprint(err)
}

func setup(t *testing.T) (*tsi.Client, *secretsmanager.Client) {
	_, base := core.ServeTest(t, func(h *core.Hub) core.Service { return timestreaminfluxdb.New(h) },
		func(h *core.Hub) core.Service { return sm.New(h) })
	cfg := awstest.Config(t, base)
	return tsi.NewFromConfig(cfg), secretsmanager.NewFromConfig(cfg)
}

func create(name string) *tsi.CreateDbInstanceInput {
	return &tsi.CreateDbInstanceInput{Name: aws.String(name), Username: aws.String("admin"), Password: aws.String("secret123"),
		Organization: aws.String("acme"), Bucket: aws.String("metrics"), DbInstanceType: types.DbInstanceTypeDbInfluxMedium,
		AllocatedStorage: aws.Int32(20), VpcSubnetIds: []string{"subnet-1"}, VpcSecurityGroupIds: []string{"sg-1"},
		Tags: map[string]string{"team": "core"}}
}

// TestMetadata runs Timestream for InfluxDB without a container engine.
func TestMetadata(t *testing.T) {
	t.Setenv("DOCKER_HOST", "unix:///nonexistent")
	db, secrets := setup(t)
	ctx := context.Background()
	out, err := db.CreateDbInstance(ctx, create("metrics"))
	if err != nil {
		t.Fatal(err)
	}
	if out.Status != types.StatusAvailable || *out.Endpoint != "localhost" || *out.Port != 8086 || out.DbStorageType != types.DbStorageTypeInfluxIoIncludedT1 ||
		out.DeploymentType != types.DeploymentTypeSingleAz || *out.AvailabilityZone != "us-east-1a" ||
		!strings.HasPrefix(*out.Arn, "arn:aws:timestream-influxdb:us-east-1:000000000000:db-instance/") {
		t.Fatalf("created %+v", out)
	}
	sec, err := secrets.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: out.InfluxAuthParametersSecretArn})
	var auth map[string]string
	if err != nil || json.Unmarshal([]byte(*sec.SecretString), &auth) != nil || auth["organization"] != "acme" || auth["password"] != "secret123" || len(auth["token"]) != 64 {
		t.Fatalf("auth parameters %v %v", sec, err)
	}
	id := out.Id
	got, err := db.GetDbInstance(ctx, &tsi.GetDbInstanceInput{Identifier: id})
	if err != nil || *got.Name != "metrics" || *got.AllocatedStorage != 20 {
		t.Fatalf("get %+v %v", got, err)
	}
	list, err := db.ListDbInstances(ctx, &tsi.ListDbInstancesInput{})
	if err != nil || len(list.Items) != 1 || *list.Items[0].Id != *id || list.Items[0].Status != types.StatusAvailable {
		t.Fatalf("list %+v %v", list, err)
	}
	pg, err := db.CreateDbParameterGroup(ctx, &tsi.CreateDbParameterGroupInput{Name: aws.String("tuned"), Description: aws.String("d"),
		Parameters: &types.ParametersMemberInfluxDBv2{Value: types.InfluxDBv2Parameters{FluxLogEnabled: aws.Bool(true)}}})
	if err != nil {
		t.Fatal(err)
	}
	if g, err := db.GetDbParameterGroup(ctx, &tsi.GetDbParameterGroupInput{Identifier: pg.Id}); err != nil || *g.Name != "tuned" ||
		!*g.Parameters.(*types.ParametersMemberInfluxDBv2).Value.FluxLogEnabled {
		t.Fatalf("parameter group %+v %v", g, err)
	}
	if l, err := db.ListDbParameterGroups(ctx, &tsi.ListDbParameterGroupsInput{}); err != nil || len(l.Items) != 1 {
		t.Fatalf("parameter groups %+v %v", l, err)
	}
	up, err := db.UpdateDbInstance(ctx, &tsi.UpdateDbInstanceInput{Identifier: id, DbInstanceType: types.DbInstanceTypeDbInfluxLarge,
		AllocatedStorage: aws.Int32(40), DbParameterGroupIdentifier: pg.Id})
	if err != nil || up.DbInstanceType != types.DbInstanceTypeDbInfluxLarge || *up.AllocatedStorage != 40 || *up.DbParameterGroupIdentifier != *pg.Id {
		t.Fatalf("update %+v %v", up, err)
	}
	if _, err := db.TagResource(ctx, &tsi.TagResourceInput{ResourceArn: out.Arn, Tags: map[string]string{"env": "dev"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.UntagResource(ctx, &tsi.UntagResourceInput{ResourceArn: out.Arn, TagKeys: []string{"team"}}); err != nil {
		t.Fatal(err)
	}
	if tags, err := db.ListTagsForResource(ctx, &tsi.ListTagsForResourceInput{ResourceArn: out.Arn}); err != nil || len(tags.Tags) != 1 || tags.Tags["env"] != "dev" {
		t.Fatalf("tags %+v %v", tags, err)
	}

	bad := create("weak")
	bad.Password = aws.String("short")
	for want, err := range map[string]error{
		"ConflictException":         second(db.CreateDbInstance(ctx, create("metrics"))),
		"ValidationException":       second(db.CreateDbInstance(ctx, bad)),
		"ResourceNotFoundException": second(db.GetDbInstance(ctx, &tsi.GetDbInstanceInput{Identifier: aws.String("nope")})),
	} {
		if code(err) != want {
			t.Fatalf("want %s, got %v", want, err)
		}
	}
	del, err := db.DeleteDbInstance(ctx, &tsi.DeleteDbInstanceInput{Identifier: id})
	if err != nil || del.Status != types.StatusDeleting {
		t.Fatalf("delete %+v %v", del, err)
	}
	if _, err := db.GetDbInstance(ctx, &tsi.GetDbInstanceInput{Identifier: id}); code(err) != "ResourceNotFoundException" {
		t.Fatalf("deleted: %v", err)
	}
	if _, err := secrets.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: out.InfluxAuthParametersSecretArn}); err == nil {
		t.Fatal("the auth parameters secret goes with the instance")
	}
}

func second[T any](_ T, err error) error { return err }

// TestInfluxInDocker writes a point to a real InfluxDB and reads it back.
func TestInfluxInDocker(t *testing.T) {
	d := docker.New(docker.Options{})
	if testing.Short() || !d.Available() {
		t.Skip("needs a running container engine")
	}
	db, secrets := setup(t)
	ctx := context.Background()
	out, err := db.CreateDbInstance(ctx, create(fmt.Sprintf("it-%d", os.Getpid())))
	if err != nil {
		t.Fatal(err)
	}
	var got *tsi.GetDbInstanceOutput
	for end := time.Now().Add(5 * time.Minute); ; time.Sleep(250 * time.Millisecond) {
		if got, err = db.GetDbInstance(ctx, &tsi.GetDbInstanceInput{Identifier: out.Id}); err != nil {
			t.Fatal(err)
		}
		if got.Status == types.StatusAvailable {
			break
		}
		if got.Status == types.StatusFailed || time.Now().After(end) {
			t.Fatalf("instance %s", got.Status)
		}
	}
	sec, err := secrets.GetSecretValue(ctx, &secretsmanager.GetSecretValueInput{SecretId: got.InfluxAuthParametersSecretArn})
	if err != nil {
		t.Fatal(err)
	}
	var auth map[string]string
	json.Unmarshal([]byte(*sec.SecretString), &auth)
	base := "http://" + *got.Endpoint + ":" + strconv.Itoa(int(*got.Port))
	do := func(path, ctype, body string) string {
		req, _ := http.NewRequest(http.MethodPost, base+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Token "+auth["token"])
		req.Header.Set("Content-Type", ctype)
		req.Header.Set("Accept", "application/csv")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 300 {
			t.Fatalf("%s: %d %s", path, resp.StatusCode, b)
		}
		return string(b)
	}
	q := url.Values{"org": {"acme"}, "bucket": {"metrics"}, "precision": {"s"}}
	do("/api/v2/write?"+q.Encode(), "text/plain", fmt.Sprintf("cpu,host=a usage=42.5 %d", time.Now().Unix()))
	csv := do("/api/v2/query?org=acme", "application/vnd.flux", `from(bucket: "metrics") |> range(start: -1h) |> filter(fn: (r) => r._measurement == "cpu")`)
	if !strings.Contains(csv, ",42.5,usage,cpu,a") {
		t.Fatalf("query: %s", csv)
	}
	if _, err := db.DeleteDbInstance(ctx, &tsi.DeleteDbInstanceInput{Identifier: out.Id}); err != nil {
		t.Fatal(err)
	}
}
