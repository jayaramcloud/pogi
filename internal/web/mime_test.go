package web

import (
	"strings"
	"testing"
)

func TestSanitizeHTML(t *testing.T) {
	in := `<p onclick="x()">Hello <b>World</b></p><script>alert(1)</script><img src="https://evil.example/pixel.png" alt="px">` +
		`<img src="cid:logo" alt="logo"><a href="javascript:alert(2)">bad</a><a href="HTTPS://ok.example/a?b=1&c=2">good</a>` +
		`<iframe src="//evil.example"><p>inside</p></iframe><form action="/x"><input name="a"><button>go</button></form>` +
		`<style>p { color: red } @import url(//evil.example/x.css);</style><div style="background:url(http://e/x)">d</div>` +
		`<div style="color: blue">e</div><svg onload="x()"><script>y()</script></svg><object data="x"></object><title>T</title>`
	got := sanitizeHTML(in, map[string]string{"logo": "data:image/png;base64,AAAA"})
	for _, bad := range []string{"<script", "alert", "onclick", "onload", "javascript:", "evil.example", "<iframe", "inside", "<form", "<input", "<button",
		"@import", "url(", "<svg", "<object", "<title", "pixel.png"} {
		if strings.Contains(got, bad) {
			t.Errorf("sanitized HTML still has %q:\n%s", bad, got)
		}
	}
	for _, keep := range []string{"<b>World</b>", `src="data:image/png;base64,AAAA"`, `href="HTTPS://ok.example/a?b=1&amp;c=2"`, `rel="noreferrer noopener"`,
		`style="color: blue"`, "go", "bad"} {
		if !strings.Contains(got, keep) {
			t.Errorf("sanitized HTML lost %q:\n%s", keep, got)
		}
	}
	if !strings.Contains(sanitizeHTML(`<style>p { color: red }</style>`, nil), "color: red") {
		t.Error("harmless style elements are kept")
	}
	if got := sanitizeHTML(`a < b & c <b`, nil); strings.Contains(got, "< ") {
		t.Errorf("text is escaped: %s", got)
	}
}

func TestParseMIME(t *testing.T) {
	raw := "From: Ann <ann@example.com>\r\nTo: bob@example.com\r\nSubject: =?UTF-8?Q?Caf=C3=A9_menu?=\r\nMIME-Version: 1.0\r\n" +
		"Content-Type: multipart/mixed; boundary=outer\r\n\r\n" +
		"--outer\r\nContent-Type: multipart/alternative; boundary=inner\r\n\r\n" +
		"--inner\r\nContent-Type: text/plain; charset=utf-8\r\n\r\nplain text\r\n" +
		"--inner\r\nContent-Type: text/html; charset=utf-8\r\nContent-Transfer-Encoding: quoted-printable\r\n\r\n<p>caf=C3=A9 <img src=3D\"cid:logo\"></p>\r\n--inner--\r\n" +
		"--outer\r\nContent-Type: image/png\r\nContent-ID: <logo>\r\nContent-Transfer-Encoding: base64\r\n\r\niVBORw0K\r\nGgo=\r\n" +
		"--outer\r\nContent-Type: application/pdf; name=report.pdf\r\nContent-Disposition: attachment; filename=report.pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0x\r\n" +
		"--outer--\r\n"
	m, err := parseMIME(raw)
	if err != nil {
		t.Fatal(err)
	}
	if m.Subject != "Café menu" || m.Text != "plain text" || !strings.Contains(m.HTML, "café") {
		t.Errorf("subject %q text %q html %q", m.Subject, m.Text, m.HTML)
	}
	var kinds []string
	for _, p := range m.Parts {
		kinds = append(kinds, p.Path+" "+p.Type+" "+p.Name)
	}
	if got := strings.Join(kinds, "|"); got != "1.1.1 text/plain |1.1.2 text/html |1.2 image/png |1.3 application/pdf report.pdf" {
		t.Errorf("parts = %s", got)
	}
	doc := previewHTML(m)
	if !strings.Contains(doc, `src="data:image/png;base64,`) || !strings.Contains(doc, "Content-Security-Policy") {
		t.Errorf("preview: %s", doc)
	}
}

func TestParseSQLOut(t *testing.T) {
	r := parseSQLOut("postgres", "id,name\n1,\"Ann, the \"\"first\"\"\"\n2,\n")
	if len(r.Cols) != 2 || len(r.Rows) != 2 || r.Rows[0][1] != `Ann, the "first"` || r.Rows[1][1] != "" {
		t.Errorf("postgres csv: %+v", r)
	}
	if r := parseSQLOut("postgres", "INSERT 0 1\n"); r.Note != "INSERT 0 1" || r.Cols != nil {
		t.Errorf("command tag: %+v", r)
	}
	if r := parseSQLOut("mysql", "id\tnote\n1\ta\\tb\n"); len(r.Rows) != 1 || r.Rows[0][1] != "a\tb" {
		t.Errorf("mysql: %+v", r)
	}
	if r := parseSQLOut("mysql", ""); r.Note == "" {
		t.Errorf("empty output: %+v", r)
	}
}

func TestPartiQL(t *testing.T) {
	in, err := partiql(`select a, "b" from "T"."idx" where a = 1 and b <> 'it''s' and c >= ? limit 5;`, map[string]any{":p0": map[string]any{"N": "9"}})
	if err != nil {
		t.Fatal(err)
	}
	if in["TableName"] != "T" || in["IndexName"] != "idx" || in["Limit"] != 5 || in["FilterExpression"] != "#a0 = :v0 AND #a1 <> :v1 AND #a2 >= :v2" || in["ProjectionExpression"] != "#a0, #a1" {
		t.Errorf("translated: %v", in)
	}
	if v := in["ExpressionAttributeValues"].(map[string]any); v[":v1"].(map[string]any)["S"] != "it's" || v[":v2"].(map[string]any)["N"] != "9" {
		t.Errorf("values: %v", v)
	}
	for _, bad := range []string{`UPDATE "T" SET a = 1`, `SELECT * FROM "T" WHERE a LIKE 'x'`, `SELECT * FROM "T" WHERE a = ?`} {
		if _, err := partiql(bad, nil); err == nil {
			t.Errorf("%q should be refused", bad)
		}
	}
}
