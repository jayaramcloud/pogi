package oauth_test

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/gcp/gapi"
	"golang.org/x/oauth2"
)

var ctx = context.Background()

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

func start(t *testing.T) *pogi.Running {
	t.Helper()
	return pogi.Start(t, pogi.Config{Services: []string{"gcpoauth"}})
}

// call posts a form (or GETs) and decodes the JSON answer.
func call(t *testing.T, method, u string, form url.Values) (int, map[string]any) {
	t.Helper()
	var resp *http.Response
	if method == "GET" {
		resp = must(http.Get(u + "?" + form.Encode()))
	} else {
		resp = must(http.PostForm(u, form))
	}
	defer resp.Body.Close()
	var m map[string]any
	json.NewDecoder(resp.Body).Decode(&m)
	return resp.StatusCode, m
}

func TestUserCredentials(t *testing.T) {
	s := start(t)
	cfg := &oauth2.Config{ClientID: "c", ClientSecret: "s", Endpoint: oauth2.Endpoint{TokenURL: s.URL + "/token"}}

	// what gcloud's application-default credentials do: trade a refresh token for access tokens
	tok := must(cfg.TokenSource(ctx, &oauth2.Token{RefreshToken: "1//0gRealLookingToken"}).Token())
	if !strings.HasPrefix(tok.AccessToken, "ya29.pogi-") || tok.Expiry.IsZero() {
		t.Fatalf("token %+v", tok)
	}
	if code, info := call(t, "GET", s.URL+"/oauth2/v3/tokeninfo", url.Values{"access_token": {tok.AccessToken}}); code != 200 || info["email"] != "user@example.com" {
		t.Fatalf("tokeninfo %d %v", code, info)
	}

	// authorization code: an access token, a refresh token that names the user, and an ID token
	ex := must(cfg.Exchange(ctx, "some-code"))
	idt, _ := ex.Extra("id_token").(string)
	if ex.RefreshToken == "" || idt == "" || !must(gapi.ParseJWT(idt)).Verify(gapi.PublicKey()) {
		t.Fatalf("exchange %+v", ex)
	}
	if _, err := cfg.TokenSource(ctx, &oauth2.Token{RefreshToken: ex.RefreshToken}).Token(); err != nil {
		t.Fatal(err)
	}

	// the 4/ endpoint aliases
	if code, _ := call(t, "POST", s.URL+"/oauth2/v4/token", url.Values{"grant_type": {"refresh_token"}, "refresh_token": {"x"}}); code != 200 {
		t.Fatalf("v4 alias: %d", code)
	}
	for _, c := range []struct {
		form url.Values
		want string
	}{
		{url.Values{}, "invalid_request"},
		{url.Values{"grant_type": {"password"}}, "unsupported_grant_type"},
		{url.Values{"grant_type": {"refresh_token"}}, "invalid_request"},
		{url.Values{"grant_type": {"authorization_code"}}, "invalid_request"},
		{url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "assertion": {"junk"}}, "invalid_grant"},
	} {
		if code, m := call(t, "POST", s.URL+"/token", c.form); code != 400 || m["error"] != c.want {
			t.Fatalf("%v: %d %v", c.form, code, m)
		}
	}
}

func TestTokeninfoAndCerts(t *testing.T) {
	s := start(t)
	if code, m := call(t, "GET", s.URL+"/oauth2/v3/tokeninfo", url.Values{"access_token": {"ya29.nope"}}); code != 400 || m["error_description"] != "Invalid Value" {
		t.Fatalf("bad token: %d %v", code, m)
	}
	if code, _ := call(t, "GET", s.URL+"/oauth2/v1/tokeninfo", url.Values{"access_token": {gapi.AccessToken("a@b", "sc", -time.Hour)}}); code != 400 {
		t.Fatalf("expired token: %d", code)
	}
	id := gapi.OIDCToken("a@b.iam.gserviceaccount.com", "the-aud")
	if code, m := call(t, "GET", s.URL+"/oauth2/v3/tokeninfo", url.Values{"id_token": {id}}); code != 200 || m["aud"] != "the-aud" || m["email"] != "a@b.iam.gserviceaccount.com" || m["iss"] != "https://accounts.google.com" {
		t.Fatalf("id token info: %d %v", code, m)
	}
	if code, _ := call(t, "GET", s.URL+"/oauth2/v3/tokeninfo", url.Values{"id_token": {id[:len(id)-4] + "AAAA"}}); code != 400 {
		t.Fatalf("tampered id token: %d", code)
	}

	_, jwks := call(t, "GET", s.URL+"/oauth2/v3/certs", nil)
	keys, _ := jwks["keys"].([]any)
	if len(keys) != 1 || keys[0].(map[string]any)["kid"] != gapi.KeyID || keys[0].(map[string]any)["alg"] != "RS256" {
		t.Fatalf("jwks %v", jwks)
	}
	_, x509s := call(t, "GET", s.URL+"/oauth2/v1/certs", nil)
	blk, _ := pem.Decode([]byte(x509s[gapi.KeyID].(string)))
	if blk == nil || must(x509.ParseCertificate(blk.Bytes)) == nil {
		t.Fatalf("v1 certs %v", x509s)
	}
}

func TestSTS(t *testing.T) {
	s := start(t)
	full := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:token-exchange"}, "subject_token": {"jwt"},
		"subject_token_type": {"urn:ietf:params:oauth:token-type:jwt"}, "audience": {"//iam.googleapis.com/x"}, "scope": {"https://www.googleapis.com/auth/cloud-platform"}}
	code, m := call(t, "POST", s.URL+"/v1/token", full)
	if code != 200 || m["issued_token_type"] != "urn:ietf:params:oauth:token-type:access_token" || m["token_type"] != "Bearer" ||
		!strings.HasPrefix(m["access_token"].(string), "ya29.pogi-") || m["expires_in"] == nil {
		t.Fatalf("exchange %d %v", code, m)
	}
	for _, drop := range []string{"subject_token", "subject_token_type", "audience", "grant_type"} {
		f := url.Values{}
		for k, v := range full {
			if k != drop {
				f[k] = v
			}
		}
		if code, m := call(t, "POST", s.URL+"/v1/token", f); code != 400 || m["error"] == nil {
			t.Fatalf("without %s: %d %v", drop, code, m)
		}
	}
	// a JSON body is accepted too
	body := strings.NewReader(`{"grant_type":"urn:ietf:params:oauth:grant-type:token-exchange","subject_token":"x","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","audience":"a"}`)
	if resp := must(http.Post(s.URL+"/v1/token", "application/json", body)); resp.StatusCode != 200 {
		t.Fatalf("json body: %d", resp.StatusCode)
	}
}
