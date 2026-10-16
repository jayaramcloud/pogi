package ses_test

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/smtp"
	"net/textproto"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ses"
	"github.com/aws/aws-sdk-go-v2/service/ses/types"
	sessvc "github.com/nrndev/pogi/internal/aws/ses"
	"github.com/nrndev/pogi/internal/core"
)

// serveSMTP serves the service's SMTP interface on a random port.
func serveSMTP(t *testing.T, svc *sessvc.Service) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go svc.ServeSMTP(ln)
	return ln.Addr().String()
}

// send delivers one message over SMTP with AUTH PLAIN.
func send(addr, user, pass, from string, to []string, msg string) error {
	c, err := smtp.Dial(addr)
	if err != nil {
		return err
	}
	defer c.Close()
	if err := c.Auth(smtp.PlainAuth("", user, pass, "127.0.0.1")); err != nil {
		return err
	}
	if err := c.Mail(from); err != nil {
		return err
	}
	for _, r := range to {
		if err := c.Rcpt(r); err != nil {
			return err
		}
	}
	w, err := c.Data()
	if err != nil {
		return err
	}
	fmt.Fprint(w, msg)
	if err := w.Close(); err != nil {
		return err
	}
	return c.Quit()
}

func smtpCode(err error) int {
	var te *textproto.Error
	if errors.As(err, &te) {
		return te.Code
	}
	return 0
}

func TestSMTP(t *testing.T) {
	svc, hub, url, _ := setup(t)
	addr := serveSMTP(t, svc)
	c := must(smtp.Dial(addr))
	if err := c.Hello("client.test"); err != nil {
		t.Fatal(err)
	}
	if ok, _ := c.Extension("STARTTLS"); !ok {
		t.Fatal("STARTTLS not offered")
	}
	ca, _, _ := hub.CertPaths()
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(must(os.ReadFile(ca)))
	if err := c.StartTLS(&tls.Config{RootCAs: pool, ServerName: "localhost"}); err != nil {
		t.Fatalf("STARTTLS with pogi's CA: %v", err)
	}
	if err := c.Auth(smtp.PlainAuth("", "test", "any password", "127.0.0.1")); err != nil {
		t.Fatal(err)
	}
	for _, step := range []error{c.Mail("sender@example.com"), c.Rcpt("to@example.com"), c.Rcpt("hidden@example.com")} {
		if step != nil {
			t.Fatal(step)
		}
	}
	w := must(c.Data())
	fmt.Fprint(w, "From: Sender <sender@example.com>\r\nTo: to@example.com\r\nSubject: Hello over SMTP\r\n\r\nFirst line\r\n.starts with a dot\r\n")
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := c.Quit(); err != nil {
		t.Fatal(err)
	}

	o := peek(t, url+"/_aws/ses")
	if len(o.Messages) != 1 {
		t.Fatalf("outbox = %+v", o)
	}
	m := o.Messages[0]
	if m.Source != "sender@example.com" || !slices.Equal(m.Destination.ToAddresses, []string{"to@example.com", "hidden@example.com"}) ||
		m.Subject != "Hello over SMTP" || !strings.Contains(m.RawData, "First line\n.starts with a dot\n") {
		t.Fatalf("message = %+v", m)
	}
}

func TestSMTPCredentials(t *testing.T) {
	hub := core.NewHub(core.Config{DataDir: t.TempDir(), ValidateSigs: true}, nil)
	svc := sessvc.New(hub)
	hub.Register(svc)
	t.Cleanup(hub.Close)
	hub.AddIdentity("AKIAPOGITEST00000001", core.Identity{Account: "111122223333", Secret: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"})
	addr := serveSMTP(t, svc)
	const password = "BLBM/9hSUELfq8Gw+rU1YcBjkOxGbhT2XG763xVLGWL9" // AWS's derivation for that secret in us-east-1
	msg := "From: app@example.com\r\nSubject: hi\r\n\r\nbody\r\n"

	if err := send(addr, "AKIAPOGITEST00000001", "wrong", "app@example.com", []string{"a@example.com"}, msg); smtpCode(err) != 535 {
		t.Fatalf("a wrong password: %v", err)
	}
	if err := send(addr, "AKIAPOGITEST00000001", password, "app@example.com", []string{"a@example.com"}, msg); err != nil {
		t.Fatalf("the derived password: %v", err)
	}
	if out := svc.Outbox(); len(out) != 1 || out[0].Account != "111122223333" {
		t.Fatalf("outbox = %+v", out)
	}
	c := must(smtp.Dial(addr))
	defer c.Close()
	if err := c.Mail("app@example.com"); smtpCode(err) != 530 {
		t.Fatalf("mail without logging in: %v", err)
	}
}

func TestSMTPProtocol(t *testing.T) {
	svc, _, _, _ := setup(t)
	conn := must(textproto.Dial("tcp", serveSMTP(t, svc)))
	defer conn.Close()
	say := func(cmd string, code int) string {
		t.Helper()
		if cmd != "" {
			conn.PrintfLine("%s", cmd)
		}
		_, msg, err := conn.ReadResponse(code)
		if err != nil {
			t.Fatalf("%.40q: %v", cmd, err)
		}
		return msg
	}
	b64 := func(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }
	say("", 220)
	say("MAIL FROM:<a@example.com>", 503) // EHLO first
	if ext := say("EHLO me", 250); !strings.Contains(ext, "AUTH PLAIN LOGIN") || !strings.Contains(ext, "SIZE 10485760") {
		t.Fatalf("EHLO = %q", ext)
	}
	say("AUTH CRAM-MD5", 504)
	say("AUTH LOGIN", 334)
	say(b64("test"), 334)
	say(b64("secret"), 235)
	say("RCPT TO:<b@example.com>", 503) // MAIL first
	say("MAIL FROM:<a@example.com> SIZE=99999999", 552)
	say("MAIL FROM:a@example.com", 501)
	say("MAIL FROM:<a@example.com> SIZE=100", 250)
	say("RSET", 250)
	say("DATA", 503)
	say("NOOP", 250)
	say("VRFY a", 502)
	say("MAIL FROM:<>", 250)
	say("RCPT TO:<b@example.com>", 250)
	say("DATA", 354)
	conn.PrintfLine("Subject: no sender\r\n\r\nbody\r\n.")
	say("", 554) // no envelope sender and no From
	say(strings.Repeat("x", 5000), 500)
	if out := svc.Outbox(); len(out) != 0 {
		t.Fatalf("outbox = %+v", out)
	}
}

// upstream is a tiny SMTP server standing in for Mailpit; it sends each
// message it gets, with its envelope, on the channel.
func upstream(t *testing.T) (string, chan []string) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	got := make(chan []string, 10)
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				c := textproto.NewConn(nc)
				defer c.Close()
				c.PrintfLine("220 upstream")
				var m []string // MAIL and RCPT arguments, then the data
				for {
					line, err := c.ReadLine()
					if err != nil {
						return
					}
					switch verb, arg, _ := strings.Cut(line, " "); verb {
					case "MAIL", "RCPT":
						m = append(m, arg)
					case "DATA":
						c.PrintfLine("354 go on")
						b, _ := c.ReadDotBytes()
						got <- append(m, string(b))
						m = nil
					case "QUIT":
						c.PrintfLine("221 bye")
						return
					}
					c.PrintfLine("250 ok")
				}
			}()
		}
	}()
	return ln.Addr().String(), got
}

func TestRelay(t *testing.T) {
	svc, hub, _, cfg := setup(t)
	relay, got := upstream(t)
	hub.Cfg.SMTPRelay = "smtp://" + relay
	next := func() []string {
		select {
		case m := <-got:
			return m
		case <-time.After(5 * time.Second):
			t.Fatal("nothing relayed")
			return nil
		}
	}

	must(ses.NewFromConfig(cfg).SendEmail(ctx, &ses.SendEmailInput{Source: aws.String("Shop <shop@example.com>"),
		Destination: &types.Destination{ToAddresses: []string{"a@example.com"}, BccAddresses: []string{"b@example.com"}},
		Message: &types.Message{Subject: &types.Content{Data: aws.String("Ünïcode order")},
			Body: &types.Body{Text: &types.Content{Data: aws.String("plain body")}, Html: &types.Content{Data: aws.String("<b>html body</b>")}}}}))
	m := next()
	if !slices.Equal(m[:3], []string{"FROM:<shop@example.com>", "TO:<a@example.com>", "TO:<b@example.com>"}) {
		t.Fatalf("envelope = %q", m[:3])
	}
	for _, want := range []string{"From: Shop <shop@example.com>", "To: a@example.com", "Subject: =?utf-8?q?", "multipart/alternative", "plain body", "<b>html body</b>"} {
		if !strings.Contains(m[3], want) {
			t.Errorf("relayed message lacks %q:\n%s", want, m[3])
		}
	}
	if strings.Contains(m[3], "b@example.com") {
		t.Errorf("Bcc in the headers:\n%s", m[3])
	}

	if err := send(serveSMTP(t, svc), "test", "x", "app@example.com", []string{"c@example.com"}, "From: app@example.com\r\nSubject: raw\r\n\r\nraw body\r\n"); err != nil {
		t.Fatal(err)
	}
	if m := next(); m[1] != "TO:<c@example.com>" || !strings.Contains(m[2], "Subject: raw") || !strings.Contains(m[2], "raw body") {
		t.Fatalf("relayed SMTP mail = %q", m)
	}
}
