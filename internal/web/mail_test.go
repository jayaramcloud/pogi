package web_test

import (
	"context"
	"html"
	"regexp"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sesv2"
	"github.com/aws/aws-sdk-go-v2/service/sesv2/types"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

var srcdoc = regexp.MustCompile(`(?s)<iframe class="mailframe" sandbox="([^"]*)"[^>]*srcdoc="(.*?)" title`)

func TestInboxPreview(t *testing.T) {
	srv := pogi.Start(t, pogi.Config{Services: []string{"ses"}})
	c := sesv2.NewFromConfig(awstest.Config(t, srv.URL))
	ctx := context.Background()
	to := &types.Destination{ToAddresses: []string{"bob@example.com"}}
	nasty := `<h1>Welcome</h1><p onmouseover="steal()">Hi <b>Bob</b></p><script>fetch('//evil.example')</script>` +
		`<img src="https://evil.example/pixel.gif"><a href="javascript:alert(1)">click</a><iframe src="//evil.example"></iframe>`
	if _, err := c.SendEmail(ctx, &sesv2.SendEmailInput{FromEmailAddress: aws.String("ann@example.com"), Destination: to,
		Content: &types.EmailContent{Simple: &types.Message{Subject: &types.Content{Data: aws.String("Welcome aboard")},
			Body: &types.Body{Html: &types.Content{Data: aws.String(nasty)}, Text: &types.Content{Data: aws.String("Welcome")}}}}}); err != nil {
		t.Fatal(err)
	}
	raw := "From: ann@example.com\r\nTo: bob@example.com\r\nSubject: Invoice\r\nMIME-Version: 1.0\r\nContent-Type: multipart/mixed; boundary=b\r\n\r\n" +
		"--b\r\nContent-Type: text/html\r\n\r\n<p>Your <i>invoice</i> is attached</p>\r\n" +
		"--b\r\nContent-Type: application/pdf; name=invoice.pdf\r\nContent-Disposition: attachment; filename=invoice.pdf\r\nContent-Transfer-Encoding: base64\r\n\r\nJVBERi0x\r\n--b--\r\n"
	if _, err := c.SendEmail(ctx, &sesv2.SendEmailInput{Content: &types.EmailContent{Raw: &types.RawMessage{Data: []byte(raw)}}}); err != nil {
		t.Fatal(err)
	}

	// the inbox links each message to its preview, newest first
	inbox := page(t, srv, "/_pogi/inbox")
	has(t, inbox, `href="/_pogi/inbox/0"`, `href="/_pogi/inbox/1"`, "Invoice", "Welcome aboard")

	// an HTML message: sanitized srcdoc in a sandboxed iframe
	p := page(t, srv, "/_pogi/inbox/1")
	m := srcdoc.FindStringSubmatch(p)
	if m == nil {
		t.Fatalf("no sandboxed preview:\n%s", p)
	}
	if strings.Contains(m[1], "allow-scripts") || strings.Contains(m[1], "allow-same-origin") {
		t.Errorf("sandbox = %q", m[1])
	}
	doc := html.UnescapeString(m[2])
	for _, bad := range []string{"<script", "steal", "javascript:", "evil.example", "<iframe", "onmouseover"} {
		if strings.Contains(doc, bad) {
			t.Errorf("preview has %q:\n%s", bad, doc)
		}
	}
	has(t, doc, "<h1>Welcome</h1>", "<b>Bob</b>", "Content-Security-Policy", "default-src 'none'")

	// a raw MIME message: parsed, with its attachment listed
	p = page(t, srv, "/_pogi/inbox/0")
	doc = html.UnescapeString(srcdoc.FindStringSubmatch(p)[2])
	has(t, doc, "<i>invoice</i>")
	has(t, p, "application/pdf", "invoice.pdf", "multipart/mixed", "Content-Type")

	// an unknown message goes back to the inbox with an error
	has(t, page(t, srv, "/_pogi/inbox/9"), "not in the inbox any more")
}
