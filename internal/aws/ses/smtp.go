package ses

import (
	"bufio"
	"bytes"
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"net/textproto"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const (
	maxMessage    = 10 << 20 // SES's limit on a message
	maxRecipients = 50       // and on its recipients
	smtpTimeout   = 5 * time.Minute
)

// ServeSMTP speaks SES's SMTP interface on ln until Close: mail sent to it
// lands in the outbox (and the relay) the way SendRawEmail does.
func (s *Service) ServeSMTP(ln net.Listener) error {
	s.mu.Lock()
	s.listeners = append(s.listeners, ln)
	s.mu.Unlock()
	var tc *tls.Config // STARTTLS with pogi's local CA or the configured certificate; nil: none
	if !s.hub.Cfg.NoTLS {
		tc, _ = s.hub.TLS()
	}
	for {
		conn, err := ln.Accept()
		if errors.Is(err, net.ErrClosed) {
			return nil
		}
		if err != nil {
			s.hub.Log.Warn("SMTP listener stopped", "err", err)
			return err
		}
		sc := &smtpConn{conn, bufio.NewReader(conn), bufio.NewWriter(conn)}
		core.Go("ses.smtp", func() { s.smtp(sc, tc) })
	}
}

// Close stops the SMTP listeners.
func (s *Service) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ln := range s.listeners {
		ln.Close()
	}
	s.listeners = nil
	return nil
}

type smtpConn struct {
	net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

func (c *smtpConn) reply(format string, a ...any) {
	fmt.Fprintf(c.w, format+"\r\n", a...)
	c.w.Flush()
}

// line reads a command line; lines over the reader's 4 KiB buffer are refused.
func (c *smtpConn) line() (string, error) {
	c.SetReadDeadline(time.Now().Add(smtpTimeout))
	b, err := c.r.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		c.reply("500 5.5.2 Line too long")
	}
	return strings.TrimRight(string(b), "\r\n"), err
}

// smtp serves one connection: EHLO/HELO, STARTTLS, AUTH PLAIN and LOGIN,
// MAIL, RCPT, DATA, RSET, NOOP and QUIT.
func (s *Service) smtp(c *smtpConn, tc *tls.Config) {
	defer func() { c.Close() }()
	host := s.hub.Cfg.Hostname
	var (
		helo, authed, mailing bool
		user, from            string // user is the access key that logged in
		to                    []string
	)
	c.reply("220 %s ESMTP pogi", host)
	for {
		line, err := c.line()
		if err != nil {
			return
		}
		verb, arg, _ := strings.Cut(line, " ")
		switch verb = strings.ToUpper(verb); verb {
		case "HELO", "EHLO":
			helo, mailing, from, to = true, false, "", nil
			if verb == "HELO" {
				c.reply("250 %s", host)
				continue
			}
			ext := []string{host, "8BITMIME", "SIZE " + strconv.Itoa(maxMessage), "AUTH PLAIN LOGIN"}
			if _, secure := c.Conn.(*tls.Conn); tc != nil && !secure {
				ext = append(ext, "STARTTLS")
			}
			for _, e := range ext[:len(ext)-1] {
				fmt.Fprintf(c.w, "250-%s\r\n", e)
			}
			c.reply("250 %s", ext[len(ext)-1])
		case "STARTTLS":
			if _, secure := c.Conn.(*tls.Conn); tc == nil || secure {
				c.reply("502 5.5.1 STARTTLS is not offered")
				continue
			}
			c.reply("220 2.0.0 Ready to start TLS")
			t := tls.Server(c.Conn, tc)
			if t.Handshake() != nil {
				return
			}
			*c = smtpConn{t, bufio.NewReader(t), bufio.NewWriter(t)} // RFC 3207: forget what came before
			helo, authed, mailing, user, from, to = false, false, false, "", "", nil
		case "AUTH":
			if !helo || authed {
				c.reply("503 5.5.1 Bad sequence of commands")
				continue
			}
			user, authed = s.auth(c, arg)
		case "MAIL":
			addr, params, ok := path(arg, "FROM:")
			switch {
			case !helo:
				c.reply("503 5.5.1 Send EHLO first")
			case s.hub.Cfg.ValidateSigs && !authed:
				c.reply("530 5.7.0 Authentication required")
			case mailing:
				c.reply("503 5.5.1 Nested MAIL command")
			case !ok:
				c.reply("501 5.5.4 Syntax: MAIL FROM:<address>")
			case slices.ContainsFunc(params, func(p string) bool {
				n, ok := strings.CutPrefix(strings.ToUpper(p), "SIZE=")
				size, _ := strconv.Atoi(n)
				return ok && size > maxMessage
			}):
				c.reply("552 5.3.4 Message size exceeds fixed maximum message size")
			default:
				mailing, from, to = true, addr, nil
				c.reply("250 2.1.0 Ok")
			}
		case "RCPT":
			addr, _, ok := path(arg, "TO:")
			switch {
			case !mailing:
				c.reply("503 5.5.1 Need MAIL command")
			case !ok || addr == "":
				c.reply("501 5.5.4 Syntax: RCPT TO:<address>")
			case len(to) >= maxRecipients:
				c.reply("452 4.5.3 Too many recipients")
			default:
				to = append(to, addr)
				c.reply("250 2.1.5 Ok")
			}
		case "DATA":
			if len(to) == 0 {
				c.reply("503 5.5.1 Need RCPT command")
				continue
			}
			c.reply("354 End data with <CR><LF>.<CR><LF>")
			c.SetReadDeadline(time.Now().Add(smtpTimeout))
			dot := textproto.NewReader(c.r).DotReader() // undoes the dot-stuffing
			data, err := io.ReadAll(io.LimitReader(dot, maxMessage+1))
			if err != nil {
				return
			}
			if len(data) > maxMessage {
				io.Copy(io.Discard, dot)
				c.reply("552 5.3.4 Message size exceeds fixed maximum message size")
			} else if id, err := s.receive(user, from, to, data); err == nil {
				c.reply("250 Ok %s", id)
			} else {
				c.reply("554 5.6.0 %s", err)
			}
			mailing, from, to = false, "", nil
		case "RSET":
			mailing, from, to = false, "", nil
			c.reply("250 2.0.0 Ok")
		case "NOOP":
			c.reply("250 2.0.0 Ok")
		case "QUIT":
			c.reply("221 2.0.0 Bye")
			return
		default:
			c.reply("502 5.5.2 Command not recognized")
		}
	}
}

// path parses "FROM:<address> PARAM=…" (or "TO:"); the brackets are required.
func path(arg, prefix string) (addr string, params []string, ok bool) {
	if len(arg) < len(prefix) || !strings.EqualFold(arg[:len(prefix)], prefix) {
		return "", nil, false
	}
	rest := strings.TrimSpace(arg[len(prefix):])
	end := strings.IndexByte(rest, '>')
	if !strings.HasPrefix(rest, "<") || end < 0 {
		return "", nil, false
	}
	return rest[1:end], strings.Fields(rest[end+1:]), true
}

// auth runs AUTH PLAIN or LOGIN. The username is an access key and the
// password the SES SMTP password derived from its secret; it is checked only
// when signatures are (Config.ValidateSigs), else anyone may log in.
func (s *Service) auth(c *smtpConn, arg string) (user string, ok bool) {
	mech, resp, _ := strings.Cut(arg, " ")
	var creds []string // user, password
	switch strings.ToUpper(mech) {
	case "PLAIN": // base64("authzid\0user\0password")
		if resp == "" {
			c.reply("334 ")
			resp, _ = c.line()
		}
		b, err := base64.StdEncoding.DecodeString(resp)
		if f := strings.Split(string(b), "\x00"); err == nil && len(f) == 3 {
			creds = f[1:]
		}
	case "LOGIN":
		if resp == "" {
			c.reply("334 VXNlcm5hbWU6") // Username:
			resp, _ = c.line()
		}
		c.reply("334 UGFzc3dvcmQ6") // Password:
		pass, _ := c.line()
		u, err1 := base64.StdEncoding.DecodeString(resp)
		p, err2 := base64.StdEncoding.DecodeString(pass)
		if err1 == nil && err2 == nil {
			creds = []string{string(u), string(p)}
		}
	default:
		c.reply("504 5.5.4 Unrecognized authentication type")
		return "", false
	}
	switch {
	case creds == nil:
		c.reply("501 5.5.2 Cannot decode the credentials")
		return "", false
	case s.hub.Cfg.ValidateSigs && !s.validLogin(creds[0], creds[1]):
		c.reply("535 5.7.8 Authentication credentials invalid")
		return "", false
	}
	c.reply("235 2.7.0 Authentication successful")
	return creds[0], true
}

func (s *Service) validLogin(user, pass string) bool {
	_, secret, ok := s.hub.Credentials(user)
	return ok && hmac.Equal([]byte(smtpPassword(secret, s.hub.Cfg.Region)), []byte(pass))
}

// smtpPassword derives the SES SMTP password of a secret access key the way
// AWS documents it: SigV4's HMAC chain over "SendRawEmail" on the date
// 11111111, after a version byte 4, in base64.
// ponytail: only the password for the default region is accepted.
func smtpPassword(secret, region string) string {
	k := []byte("AWS4" + secret)
	for _, part := range []string{"11111111", region, "ses", "aws4_request", "SendRawEmail"} {
		m := hmac.New(sha256.New, k)
		m.Write([]byte(part))
		k = m.Sum(nil)
	}
	return base64.StdEncoding.EncodeToString(append([]byte{4}, k...))
}

// receive stores mail that came over SMTP the way SendRawEmail does, in the
// account of the access key that logged in; an error without a sender.
func (s *Service) receive(user, from string, to []string, data []byte) (string, error) {
	account, _, _ := s.hub.Credentials(user)
	e := Email{Source: from, Destination: &Destination{ToAddresses: to}}
	if !parseRaw(data, &e) {
		return "", errors.New("Missing required header 'From'.")
	}
	return s.send(&core.Ctx{Hub: s.hub, Account: account, Region: s.hub.Cfg.Region, RequestID: core.NewID()}, e, "")
}

// relay also delivers a sent message to Config.SMTPRelay (Mailpit, MailHog).
// ponytail: one attempt; a failure is logged, not retried or bounced.
func (s *Service) relay(e Email) {
	u, err := url.Parse(s.hub.Cfg.SMTPRelay)
	if err != nil {
		s.hub.Log.Warn("SES relay", "err", err)
		return
	}
	var auth smtp.Auth
	if u.User != nil {
		pass, _ := u.User.Password()
		auth = smtp.PlainAuth("", u.User.Username(), pass, u.Hostname())
	}
	var to []string
	for _, a := range slices.Concat(e.Destination.ToAddresses, e.Destination.CcAddresses, e.Destination.BccAddresses) {
		to = append(to, bare(a))
	}
	msg := []byte(e.RawData)
	if e.RawData == "" {
		msg = e.mimeMessage()
	}
	if err := smtp.SendMail(net.JoinHostPort(u.Hostname(), cmp.Or(u.Port(), "25")), auth, bare(e.Source), to, msg); err != nil {
		s.hub.Log.Warn("SES relay failed", "relay", u.Redacted(), "id", e.Id, "err", err)
	}
}

// bare is the address in "Name <address>".
func bare(addr string) string {
	if a, err := mail.ParseAddress(addr); err == nil {
		return a.Address
	}
	return addr
}

// mimeMessage renders a message sent as a subject and body (SendEmail,
// templates) as MIME, for the relay.
func (e Email) mimeMessage() []byte {
	var b bytes.Buffer
	mw := multipart.NewWriter(&b)
	for _, h := range [][2]string{{"From", e.Source}, {"To", strings.Join(e.Destination.ToAddresses, ", ")},
		{"Cc", strings.Join(e.Destination.CcAddresses, ", ")}, {"Subject", mime.QEncoding.Encode("utf-8", e.Subject)},
		{"Date", e.Timestamp.Format(time.RFC1123Z)}, {"Message-ID", "<" + e.Id + "@email.amazonses.com>"},
		{"MIME-Version", "1.0"}, {"Content-Type", "multipart/alternative; boundary=" + mw.Boundary()}} {
		if h[1] != "" {
			fmt.Fprintf(&b, "%s: %s\r\n", h[0], h[1])
		}
	}
	b.WriteString("\r\n")
	body := cmp.Or(e.Body, &Body{})
	for _, part := range [][2]string{{"text/plain", body.TextPart}, {"text/html", body.HtmlPart}} {
		if part[1] == "" {
			continue
		}
		w, _ := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {part[0] + "; charset=UTF-8"}, "Content-Transfer-Encoding": {"quoted-printable"}})
		qp := quotedprintable.NewWriter(w)
		qp.Write([]byte(part[1]))
		qp.Close()
	}
	mw.Close()
	return b.Bytes()
}
