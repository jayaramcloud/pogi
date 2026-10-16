package core

import (
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
		if got := redact(in); got != want {
			t.Errorf("redact(%s)\n got %s\nwant %s", in, got, want)
		}
	}
}
