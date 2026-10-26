package core

import (
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

func TestQueryEmbeddedStructs(t *testing.T) {
	type base struct{ Name string }
	var in struct {
		base
		Tags []struct{ Key, Value string } `query:"Tags"`
	}
	form := url.Values{"Name": {"x"}, "Tags.member.1.Key": {"k"}, "Tags.member.1.Value": {"v"}}
	if err := DecodeQuery(form, &in); err != nil || in.Name != "x" || len(in.Tags) != 1 || in.Tags[0].Value != "v" {
		t.Fatalf("decode: %v %+v", err, in)
	}
	var b strings.Builder
	WriteXMLFields(&b, reflect.ValueOf(in))
	if !strings.Contains(b.String(), "<Name>x</Name>") {
		t.Fatalf("encode: %s", b.String())
	}
	var out struct {
		base  `query:"-"` // internal fields stay out of responses
		Shown string
	}
	out.Name, out.Shown = "secret", "ok"
	b.Reset()
	WriteXMLFields(&b, reflect.ValueOf(out))
	if b.String() != "<Shown>ok</Shown>" {
		t.Fatalf("omitted embedded struct leaked: %s", b.String())
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		`{"AuthParameters":{"USERNAME":"bob","PASSWORD":"hunter2"},"ClientId":"c"}`:                                                         `{"AuthParameters":{"USERNAME":"bob","PASSWORD":"[redacted]"},"ClientId":"c"}`,
		`{"SecretString":"a\"b","Name":"n"}`:                                                                                                `{"SecretString":"[redacted]","Name":"n"}`,
		`Action=CreateLoginProfile&UserName=bob&Password=hunter2&PasswordResetRequired=true`:                                                `Action=CreateLoginProfile&UserName=bob&Password=[redacted]&PasswordResetRequired=true`,
		`<Credentials><AccessKeyId>ASIA1</AccessKeyId><SecretAccessKey>xyz</SecretAccessKey><SessionToken>tok</SessionToken></Credentials>`: `<Credentials><AccessKeyId>ASIA1</AccessKeyId><SecretAccessKey>[redacted]</SecretAccessKey><SessionToken>[redacted]</SessionToken></Credentials>`,
	} {
		if got := redact(in, ""); got != want {
			t.Errorf("redact(%s)\n got %s\nwant %s", in, got, want)
		}
	}
}

func TestRedactKeepsKeysHidesValues(t *testing.T) {
	for _, c := range []struct{ in, path, want string }{
		{`{"Name":"/p","Value":"s3","Type":"SecureString"}`, "/", `{"Name":"/p","Value":"[redacted]","Type":"SecureString"}`},
		{`{"Name":"/p","Value":"plain","Type":"String"}`, "/", `{"Name":"/p","Value":"plain","Type":"String"}`},
		{`{"value":"kv"}`, "/secrets/x", `{"value":"[redacted]"}`},
		{`{"payload":{"data":"abc="}}`, "/v1/x", `{"payload":{"data":"[redacted]"}}`},
		{`{"Environment":{"Variables":{"A":"1","B":"2"}},"Handler":"h"}`, "/", `{"Environment":{"Variables":{"A":"[redacted]","B":"[redacted]"}},"Handler":"h"}`},
		{`{"AccessToken":"t","NextToken":"n","administratorLoginPassword":"p"}`, "/", `{"AccessToken":"[redacted]","NextToken":"n","administratorLoginPassword":"[redacted]"}`},
	} {
		if got := redact(c.in, c.path); got != c.want {
			t.Errorf("redact(%s)\n got %s\nwant %s", c.in, got, c.want)
		}
	}
	if got, want := redactPath("/b/k?X-Amz-Security-Token=t&key=g&prefix=a&key-marker=m&X-Amz-Signature=s"), "/b/k?X-Amz-Security-Token=[redacted]&key=[redacted]&prefix=a&key-marker=m&X-Amz-Signature=[redacted]"; got != want {
		t.Errorf("redactPath = %s", got)
	}
	h := flatHeaders(http.Header{"X-Amz-Security-Token": {"t"}, "X-Goog-Api-Key": {"k"}, "Content-Type": {"x"}})
	if h["X-Amz-Security-Token"] != "[redacted]" || h["X-Goog-Api-Key"] != "[redacted]" || h["Content-Type"] != "x" {
		t.Errorf("flatHeaders = %v", h)
	}
}

// TestXMLEmptyList: an empty list tagged query:",empty" is written as an empty element (AWS's <Parameters/>
// in GetTemplateSummary, which aws cloudformation deploy reads); other empty lists are left out.
func TestXMLEmptyList(t *testing.T) {
	var b strings.Builder
	WriteXMLFields(&b, reflect.ValueOf(struct {
		Parameters []string `query:",empty"`
		Outputs    []string
	}{Outputs: []string{}}))
	if b.String() != "<Parameters/>" {
		t.Fatalf("got %q", b.String())
	}
}
