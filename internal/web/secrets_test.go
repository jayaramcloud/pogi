package web_test

import (
	"encoding/base64"
	"html"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/core"
)

// send makes a JSON request to the running pogi the way a client would.
func send(t *testing.T, srv *pogi.Running, method, path, body string, hdr ...string) string {
	t.Helper()
	req, _ := http.NewRequest(method, srv.URL+path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	b, _ := io.ReadAll(res.Body)
	if res.StatusCode >= 300 {
		t.Fatalf("%s %s: %s %s", method, path, res.Status, b)
	}
	return string(b)
}

var bearer = []string{"Authorization", "Bearer pogi"}

const keyvaultAPI = "?api-version=7.4"

var output = regexp.MustCompile(`(?s)<pre id="out">(.*?)</pre>`)

func TestCryptoPanels(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"kms", "cloudkms", "keyvault"}})
	call(t, srv, "TrentService", "CreateKey", map[string]any{"Description": "app"})
	ring := "projects/" + core.DefaultProject + "/locations/global/keyRings/ring"
	send(t, srv, "POST", "/v1/projects/"+core.DefaultProject+"/locations/global/keyRings?keyRingId=ring", `{}`)
	send(t, srv, "POST", "/v1/"+ring+"/cryptoKeys?cryptoKeyId=k1", `{"purpose":"ENCRYPT_DECRYPT"}`)
	send(t, srv, "POST", "/keys/rsa1/create"+keyvaultAPI, `{"kty":"RSA","key_size":2048}`, bearer...)
	send(t, srv, "POST", "/keys/aes1/create"+keyvaultAPI, `{"kty":"oct","key_size":256}`, bearer...)

	p := page(t, srv, "/_pogi/crypto")
	keys := regexp.MustCompile(`<option value="((?:aws|gcp|azure)\|[^"]+)"`).FindAllStringSubmatch(p, -1)
	if len(keys) != 4 {
		t.Fatalf("keys offered: %v\n%s", keys, p)
	}
	for _, k := range keys {
		key := html.UnescapeString(k[1])
		run := func(mode, in string) string {
			return form(t, srv, "/_pogi/crypto", url.Values{"key": {key}, "mode": {mode}, "input": {in}})
		}
		enc := run("encrypt", "hello pogi")
		m := output.FindStringSubmatch(enc)
		if m == nil {
			t.Fatalf("%s: encrypt gave no result:\n%s", key, results(enc))
		}
		ct := html.UnescapeString(m[1])
		if strings.Contains(ct, "hello pogi") || ct == "" {
			t.Fatalf("%s: ciphertext %q", key, ct)
		}
		dec := output.FindStringSubmatch(run("decrypt", ct))
		if dec == nil || html.UnescapeString(dec[1]) != "hello pogi" {
			t.Errorf("%s: decrypt = %v, want hello pogi", key, dec)
		}
		shows(t, run("decrypt", "bm90IGNpcGhlcnRleHQ"), `class="flash"`) // garbage is refused
	}
}

func TestSecretReveal(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"secretsmanager", "ssm", "secretmanager", "keyvault"}})
	call(t, srv, "secretsmanager", "CreateSecret", map[string]any{"Name": "prod/db", "SecretString": "aws-sm-s3cret"})
	call(t, srv, "AmazonSSM", "PutParameter", map[string]any{"Name": "/app/token", "Value": "ssm-t0ken", "Type": "SecureString"})
	send(t, srv, "POST", "/v1/projects/"+core.DefaultProject+"/secrets?secretId=gsec", `{"replication":{"automatic":{}}}`)
	send(t, srv, "POST", "/v1/projects/"+core.DefaultProject+"/secrets/gsec:addVersion", `{"payload":{"data":"`+base64.StdEncoding.EncodeToString([]byte("gcp-sm-v4lue"))+`"}}`)
	send(t, srv, "PUT", "/secrets/kvsec"+keyvaultAPI, `{"value":"kv-s3cret"}`, bearer...)

	// listed, and every value hidden
	p := page(t, srv, "/_pogi/secrets")
	has(t, p, "prod/db", "/app/token", "gsec", "kvsec", "SSM SecureString", "Nothing revealed yet")
	for _, v := range []string{"aws-sm-s3cret", "ssm-t0ken", "gcp-sm-v4lue", "kv-s3cret"} {
		if strings.Contains(p, v) {
			t.Fatalf("the list shows %q before any reveal", v)
		}
	}
	refs := regexp.MustCompile(`name="ref" value="([^"]+)"`).FindAllStringSubmatch(p, -1)
	if len(refs) != 4 {
		t.Fatalf("refs = %v", refs)
	}
	values := map[string]string{"awssm": "aws-sm-s3cret", "ssm": "ssm-t0ken", "gcp": "gcp-sm-v4lue", "azure": "kv-s3cret"}
	for _, r := range refs {
		ref := html.UnescapeString(r[1])
		got := form(t, srv, "/_pogi/secrets/reveal", url.Values{"ref": {ref}})
		if kind, _, _ := strings.Cut(ref, "|"); !strings.Contains(got, values[kind]) {
			t.Errorf("reveal %s = %s, want %s", ref, got, values[kind])
		}
	}
	if got := form(t, srv, "/_pogi/secrets/reveal", url.Values{"ref": {"awssm|" + core.DefaultAccount + "|" + core.DefaultRegion + "|nope"}}); !strings.Contains(got, "st bad") {
		t.Errorf("revealing a missing secret: %s", got)
	}

	// each reveal is logged, without the value
	p = page(t, srv, "/_pogi/secrets")
	has(t, p, "awssm "+core.DefaultAccount, "ssm ", "gcp projects/", "azure default kvsec")
	hides(t, p, "aws-sm-s3cret", "ssm-t0ken", "gcp-sm-v4lue", "kv-s3cret", "Nothing revealed yet")
	if reqs := page(t, srv, "/_pogi/requests?service=console"); !strings.Contains(reqs, "RevealSecret") {
		t.Errorf("reveals are not in the request log:\n%s", results(reqs))
	}
}
