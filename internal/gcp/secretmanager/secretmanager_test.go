package secretmanager_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"hash/crc32"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/iam/apiv1/iampb"
	sm "cloud.google.com/go/secretmanager/apiv1"
	smpb "cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/secretmanager"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func wantCode(t *testing.T, err error, want codes.Code) {
	t.Helper()
	if status.Code(err) != want {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func setup(t *testing.T) (*secretmanager.Service, *sm.Client, string) {
	t.Helper()
	var svc *secretmanager.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = secretmanager.New(h); return svc })
	c := must(sm.NewClient(ctx, option.WithEndpoint(strings.TrimPrefix(url, "http://")), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials()))))
	t.Cleanup(func() { c.Close() })
	return svc, c, url
}

var automatic = &smpb.Replication{Replication: &smpb.Replication_Automatic_{Automatic: &smpb.Replication_Automatic{}}}

func crc(b string) *int64 {
	c := int64(crc32.Checksum([]byte(b), crc32.MakeTable(crc32.Castagnoli)))
	return &c
}

func TestSecrets(t *testing.T) {
	svc, c, _ := setup(t)
	sec := must(c.CreateSecret(ctx, &smpb.CreateSecretRequest{Parent: "projects/proj", SecretId: "db-password",
		Secret: &smpb.Secret{Replication: automatic, Labels: map[string]string{"env": "dev"}}}))
	if sec.Name != "projects/proj/secrets/db-password" || sec.CreateTime == nil || sec.Etag == "" {
		t.Fatalf("created = %v", sec)
	}
	_, err := c.CreateSecret(ctx, &smpb.CreateSecretRequest{Parent: "projects/proj", SecretId: "db-password", Secret: &smpb.Secret{Replication: automatic}})
	wantCode(t, err, codes.AlreadyExists)
	_, err = c.CreateSecret(ctx, &smpb.CreateSecretRequest{Parent: "projects/proj", SecretId: "no-replication", Secret: &smpb.Secret{}})
	wantCode(t, err, codes.InvalidArgument)
	_, err = c.CreateSecret(ctx, &smpb.CreateSecretRequest{Parent: "projects/proj", SecretId: "bad/id", Secret: &smpb.Secret{Replication: automatic}})
	wantCode(t, err, codes.InvalidArgument)

	add := func(data string, sum *int64) (*smpb.SecretVersion, error) {
		return c.AddSecretVersion(ctx, &smpb.AddSecretVersionRequest{Parent: sec.Name, Payload: &smpb.SecretPayload{Data: []byte(data), DataCrc32C: sum}})
	}
	v1 := must(add("hunter2", crc("hunter2")))
	if v1.Name != sec.Name+"/versions/1" || v1.State != smpb.SecretVersion_ENABLED || !v1.ClientSpecifiedPayloadChecksum {
		t.Fatalf("v1 = %v", v1)
	}
	_, err = add("tampered", crc("hunter2"))
	wantCode(t, err, codes.InvalidArgument)
	if !strings.Contains(err.Error(), "Checksum mismatch") {
		t.Fatalf("checksum error = %v", err)
	}
	v2 := must(add("correct-horse", nil))

	access := func(v string) (string, error) {
		res, err := c.AccessSecretVersion(ctx, &smpb.AccessSecretVersionRequest{Name: sec.Name + "/versions/" + v})
		if err != nil {
			return "", err
		}
		if *res.Payload.DataCrc32C != *crc(string(res.Payload.Data)) {
			t.Fatalf("payload checksum = %d", *res.Payload.DataCrc32C)
		}
		return string(res.Payload.Data), nil
	}
	if got := must(access("latest")); got != "correct-horse" {
		t.Fatalf("latest = %q", got)
	}
	if got := must(access("1")); got != "hunter2" {
		t.Fatalf("version 1 = %q", got)
	}
	regional := must(c.AccessSecretVersion(ctx, &smpb.AccessSecretVersionRequest{Name: "projects/proj/locations/us-east1/secrets/db-password/versions/latest"}))
	if regional.Name != v2.Name {
		t.Fatalf("regional alias resolved to %q", regional.Name)
	}
	_, err = access("3")
	wantCode(t, err, codes.NotFound)

	must(c.DisableSecretVersion(ctx, &smpb.DisableSecretVersionRequest{Name: v2.Name}))
	_, err = access("latest")
	wantCode(t, err, codes.FailedPrecondition)
	must(c.EnableSecretVersion(ctx, &smpb.EnableSecretVersionRequest{Name: v2.Name}))
	if got := must(access("latest")); got != "correct-horse" {
		t.Fatalf("after enable = %q", got)
	}

	up := must(c.UpdateSecret(ctx, &smpb.UpdateSecretRequest{
		Secret:     &smpb.Secret{Name: sec.Name, Labels: map[string]string{"env": "prod"}, VersionAliases: map[string]int64{"stable": 1}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"labels", "version_aliases"}}}))
	if up.Labels["env"] != "prod" || up.Etag == sec.Etag || must(c.GetSecret(ctx, &smpb.GetSecretRequest{Name: sec.Name})).Labels["env"] != "prod" {
		t.Fatalf("updated = %v", up)
	}
	if got := must(access("stable")); got != "hunter2" {
		t.Fatalf("alias = %q", got)
	}
	_, err = c.UpdateSecret(ctx, &smpb.UpdateSecretRequest{Secret: &smpb.Secret{Name: sec.Name, VersionAliases: map[string]int64{"x": 9}},
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"version_aliases"}}})
	wantCode(t, err, codes.InvalidArgument)

	destroyed := must(c.DestroySecretVersion(ctx, &smpb.DestroySecretVersionRequest{Name: v1.Name}))
	if destroyed.State != smpb.SecretVersion_DESTROYED || destroyed.DestroyTime == nil {
		t.Fatalf("destroyed = %v", destroyed)
	}
	_, err = access("1")
	wantCode(t, err, codes.FailedPrecondition)
	_, err = c.EnableSecretVersion(ctx, &smpb.EnableSecretVersionRequest{Name: v1.Name})
	wantCode(t, err, codes.FailedPrecondition)
	if b, _ := json.Marshal(svc.Inspect("Secrets", sec.Name)); strings.Contains(string(b), base64.StdEncoding.EncodeToString([]byte("correct-horse"))) {
		t.Fatalf("Inspect leaks payloads: %s", b)
	}

	var versions []string
	for it := c.ListSecretVersions(ctx, &smpb.ListSecretVersionsRequest{Parent: sec.Name}); ; {
		v, err := it.Next()
		if err == iterator.Done {
			break
		}
		versions = append(versions, must(v, err).Name[len(sec.Name):]+"="+v.State.String())
	}
	if strings.Join(versions, ",") != "/versions/2=ENABLED,/versions/1=DESTROYED" {
		t.Fatalf("versions = %v", versions)
	}

	for _, id := range []string{"api-key", "tls-cert"} {
		must(c.CreateSecret(ctx, &smpb.CreateSecretRequest{Parent: "projects/proj", SecretId: id, Secret: &smpb.Secret{Replication: automatic}}))
	}
	var firstPage []*smpb.Secret
	token := must(iterator.NewPager(c.ListSecrets(ctx, &smpb.ListSecretsRequest{Parent: "projects/proj"}), 2, "").NextPage(&firstPage))
	var secondPage []*smpb.Secret
	must(iterator.NewPager(c.ListSecrets(ctx, &smpb.ListSecretsRequest{Parent: "projects/proj"}), 2, token).NextPage(&secondPage))
	if len(firstPage) != 2 || token == "" || len(secondPage) != 1 || secondPage[0].Name != "projects/proj/secrets/tls-cert" {
		t.Fatalf("pages = %v / %q / %v", firstPage, token, secondPage)
	}
	if got := must(c.ListSecrets(ctx, &smpb.ListSecretsRequest{Parent: "projects/proj", Filter: "labels.env=prod"}).Next()); got.Name != sec.Name {
		t.Fatalf("filtered = %v", got)
	}

	policy := &iampb.Policy{Bindings: []*iampb.Binding{{Role: "roles/secretmanager.secretAccessor", Members: []string{"user:dev@example.com"}}}}
	must(c.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: sec.Name, Policy: policy}))
	if got := must(c.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: sec.Name})); got.Bindings[0].Members[0] != "user:dev@example.com" {
		t.Fatalf("policy = %v", got)
	}
	perms := must(c.TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: sec.Name, Permissions: []string{"secretmanager.versions.access"}}))
	if len(perms.Permissions) != 1 {
		t.Fatalf("permissions = %v", perms)
	}

	cur := must(c.GetSecret(ctx, &smpb.GetSecretRequest{Name: sec.Name}))
	wantCode(t, c.DeleteSecret(ctx, &smpb.DeleteSecretRequest{Name: sec.Name, Etag: `"stale"`}), codes.Aborted)
	if err := c.DeleteSecret(ctx, &smpb.DeleteSecretRequest{Name: sec.Name, Etag: cur.Etag}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetSecret(ctx, &smpb.GetSecretRequest{Name: sec.Name})
	wantCode(t, err, codes.NotFound)
}

func TestREST(t *testing.T) {
	_, _, url := setup(t)
	call := func(method, path, body string) (int, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(method, url+path, strings.NewReader(body))
		resp := must(http.DefaultClient.Do(req))
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("%s %s: %d %s", method, path, resp.StatusCode, raw)
		}
		return resp.StatusCode, out
	}
	if code, out := call("POST", "/v1/projects/proj/secrets?secretId=api-key", `{"replication":{"automatic":{}},"labels":{"team":"a"}}`); code != 200 || out["name"] != "projects/proj/secrets/api-key" {
		t.Fatalf("create: %d %v", code, out)
	}
	data := base64.StdEncoding.EncodeToString([]byte("s3cr3t"))
	if code, out := call("POST", "/v1/projects/proj/secrets/api-key:addVersion", `{"payload":{"data":"`+data+`"}}`); code != 200 || out["state"] != "ENABLED" {
		t.Fatalf("add version: %d %v", code, out)
	}
	for _, path := range []string{"/v1/projects/proj/secrets/api-key/versions/latest:access", "/v1/projects/proj/locations/us/secrets/api-key/versions/1:access"} {
		_, out := call("GET", path, "")
		if p, _ := out["payload"].(map[string]any); p["data"] != data || p["dataCrc32c"] != "825573743" {
			t.Fatalf("%s: %v", path, out)
		}
	}
	if _, out := call("PATCH", "/v1/projects/proj/secrets/api-key?updateMask=labels", `{"labels":{"team":"b"}}`); out["labels"].(map[string]any)["team"] != "b" {
		t.Fatalf("patch: %v", out)
	}
	if _, out := call("POST", "/v1/projects/proj/secrets/api-key/versions/1:disable", `{}`); out["state"] != "DISABLED" {
		t.Fatalf("disable: %v", out)
	}
	if _, out := call("GET", "/v1/projects/proj/secrets", ""); out["totalSize"] != 1.0 {
		t.Fatalf("list: %v", out)
	}
	code, out := call("GET", "/v1/projects/proj/secrets/missing", "")
	if e, _ := out["error"].(map[string]any); code != 404 || e["status"] != "NOT_FOUND" || e["code"] != 404.0 {
		t.Fatalf("missing: %d %v", code, out)
	}
	if code, out = call("GET", "/v1/projects/proj/secrets/api-key/versions/1:access", ""); code != 400 || out["error"].(map[string]any)["status"] != "FAILED_PRECONDITION" {
		t.Fatalf("disabled access: %d %v", code, out)
	}
}

func TestPersistence(t *testing.T) {
	svc, c, _ := setup(t)
	sec := must(c.CreateSecret(ctx, &smpb.CreateSecretRequest{Parent: "projects/proj", SecretId: "kept", Secret: &smpb.Secret{Replication: automatic}}))
	must(c.AddSecretVersion(ctx, &smpb.AddSecretVersionRequest{Parent: sec.Name, Payload: &smpb.SecretPayload{Data: []byte("v1")}}))
	b := must(svc.Save())

	svc.Reset()
	_, err := c.GetSecret(ctx, &smpb.GetSecretRequest{Name: sec.Name})
	wantCode(t, err, codes.NotFound)
	if err := svc.Load(b); err != nil {
		t.Fatal(err)
	}
	res := must(c.AccessSecretVersion(ctx, &smpb.AccessSecretVersionRequest{Name: sec.Name + "/versions/latest"}))
	if string(res.Payload.Data) != "v1" {
		t.Fatalf("restored payload = %q", res.Payload.Data)
	}
	if rows := svc.Resources()[0].Rows; len(rows) != 1 || rows[0][0] != sec.Name || rows[0][2] != "1" || rows[0][3] != "ENABLED" {
		t.Fatalf("resources = %v", rows)
	}
}
