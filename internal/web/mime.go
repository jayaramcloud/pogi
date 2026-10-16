package web

// Email preview: the MIME structure of a captured message, and its HTML
// sanitized for an iframe with sandbox and srcdoc.

import (
	"encoding/base64"
	"fmt"
	"html"
	"io"
	"mime"
	"mime/multipart"
	"mime/quotedprintable"
	"net/http"
	netmail "net/mail"
	"strconv"
	"strings"

	xhtml "golang.org/x/net/html"

	"github.com/nrndev/pogi/internal/core"
)

type mimePart struct {
	Path, Type, Name string
	Size             int
	Data             []byte `json:"-"`
	cid              string
}

type parsedMail struct {
	Headers [][2]string // the message's headers, decoded, sorted by name
	Subject string
	Parts   []mimePart
	HTML    string // first text/html part
	Text    string // first text/plain part
}

// parseMIME reads a raw message. ponytail: charsets other than UTF-8 are shown
// as they are, and messages nested as message/rfc822 are one attachment.
func parseMIME(raw string) (*parsedMail, error) {
	msg, err := netmail.ReadMessage(strings.NewReader(raw))
	if err != nil {
		return nil, err
	}
	p := &parsedMail{}
	for _, k := range core.SortedKeys(msg.Header) {
		for _, v := range msg.Header[k] {
			v, _ = new(mime.WordDecoder).DecodeHeader(v)
			p.Headers = append(p.Headers, [2]string{k, v})
		}
	}
	p.Subject, _ = new(mime.WordDecoder).DecodeHeader(msg.Header.Get("Subject"))
	p.walk("1", hdrs(msg.Header), msg.Body)
	return p, nil
}

type hdrs map[string][]string

func (h hdrs) get(k string) string {
	if v := h[http.CanonicalHeaderKey(k)]; len(v) > 0 {
		return v[0]
	}
	return ""
}

// decode undoes the transfer encoding of a body.
func decode(encoding string, r io.Reader) []byte {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "base64":
		b, _ := io.ReadAll(base64.NewDecoder(base64.StdEncoding, &stripSpace{r}))
		return b
	case "quoted-printable":
		b, _ := io.ReadAll(quotedprintable.NewReader(r))
		return b
	}
	b, _ := io.ReadAll(r)
	return b
}

// stripSpace drops the line breaks of base64 text.
type stripSpace struct{ r io.Reader }

func (s *stripSpace) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	out := p[:0]
	for _, c := range p[:n] {
		if c != '\r' && c != '\n' && c != ' ' && c != '\t' {
			out = append(out, c)
		}
	}
	return len(out), err
}

func (p *parsedMail) walk(path string, h hdrs, body io.Reader) {
	ct, params, err := mime.ParseMediaType(h.get("Content-Type"))
	if err != nil {
		ct = "text/plain"
	}
	if strings.HasPrefix(ct, "multipart/") && params["boundary"] != "" {
		mr := multipart.NewReader(body, params["boundary"])
		for i := 1; ; i++ {
			part, err := mr.NextRawPart()
			if err != nil {
				return
			}
			p.walk(path+"."+strconv.Itoa(i), hdrs(part.Header), part)
		}
	}
	data := decode(h.get("Content-Transfer-Encoding"), body)
	name := params["name"]
	if _, dp, err := mime.ParseMediaType(h.get("Content-Disposition")); err == nil && dp["filename"] != "" {
		name = dp["filename"]
	}
	cid := strings.Trim(h.get("Content-ID"), "<>")
	switch {
	case ct == "text/html" && p.HTML == "" && name == "":
		p.HTML = string(data)
	case ct == "text/plain" && p.Text == "" && name == "":
		p.Text = string(data)
	}
	p.Parts = append(p.Parts, mimePart{Path: path, Type: ct, Name: name, Size: len(data), Data: data, cid: cid})
}

// ---- sanitizer ----

var (
	// dropped with everything inside them
	dropWhole = map[string]bool{"script": true, "iframe": true, "object": true, "embed": true, "noscript": true, "template": true,
		"svg": true, "math": true, "frameset": true, "applet": true, "title": true}
	// dropped, their content kept
	dropTag = map[string]bool{"form": true, "input": true, "button": true, "select": true, "textarea": true, "link": true,
		"meta": true, "base": true, "html": true, "body": true, "head": true, "frame": true}
	allowAttr = map[string]bool{"href": true, "src": true, "alt": true, "title": true, "width": true, "height": true, "colspan": true,
		"rowspan": true, "align": true, "valign": true, "bgcolor": true, "color": true, "border": true, "cellpadding": true,
		"cellspacing": true, "class": true, "style": true, "dir": true, "lang": true, "target": true}
)

// safeURL allows web, mail and page links in href and inline images only in
// src: remote images would load (and so track) when the message is opened.
func safeURL(attr, v string) bool {
	v = strings.ToLower(strings.TrimSpace(v))
	if attr == "src" {
		return strings.HasPrefix(v, "data:image/")
	}
	return strings.HasPrefix(v, "http://") || strings.HasPrefix(v, "https://") || strings.HasPrefix(v, "mailto:") || strings.HasPrefix(v, "#")
}

// safeCSS refuses styles that load or run something.
func safeCSS(v string) bool {
	l := strings.ToLower(v)
	return !strings.Contains(l, "url(") && !strings.Contains(l, "expression") && !strings.Contains(l, "javascript") &&
		!strings.Contains(l, "@import") && !strings.Contains(l, "behavior") && !strings.Contains(l, "<")
}

// sanitizeHTML keeps the formatting of a message and nothing that runs or
// loads: scripts, frames, forms and event handlers go, links keep only safe
// schemes, and cid: images become the inline parts. The iframe's sandbox and
// the CSP in the document are the second wall; this is the first.
func sanitizeHTML(src string, cids map[string]string) string {
	var out strings.Builder
	z := xhtml.NewTokenizer(strings.NewReader(src))
	skip, skipName := 0, ""
	inStyle := false
	for {
		tt := z.Next()
		if tt == xhtml.ErrorToken {
			break
		}
		t := z.Token()
		name := strings.ToLower(t.Data)
		switch tt {
		case xhtml.StartTagToken, xhtml.SelfClosingTagToken:
			if skip > 0 {
				if name == skipName && tt == xhtml.StartTagToken {
					skip++
				}
				continue
			}
			if dropWhole[name] {
				if tt == xhtml.StartTagToken {
					skip, skipName = 1, name
				}
				continue
			}
			if dropTag[name] {
				continue
			}
			inStyle = name == "style" && tt == xhtml.StartTagToken
			var attrs []string
			for _, a := range t.Attr {
				k, v := strings.ToLower(a.Key), a.Val
				switch {
				case !allowAttr[k]:
					continue
				case k == "src" || k == "href":
					if c, ok := strings.CutPrefix(v, "cid:"); ok && cids[c] != "" {
						v = cids[c]
					}
					if !safeURL(k, v) {
						continue
					}
				case k == "style" && !safeCSS(v):
					continue
				}
				attrs = append(attrs, fmt.Sprintf(` %s="%s"`, k, html.EscapeString(v)))
			}
			if name == "a" {
				attrs = append(attrs, ` target="_blank" rel="noreferrer noopener"`)
			}
			end := ""
			if tt == xhtml.SelfClosingTagToken {
				end = " /"
			}
			out.WriteString("<" + name + strings.Join(attrs, "") + end + ">")
		case xhtml.EndTagToken:
			if skip > 0 {
				if name == skipName {
					skip--
				}
				continue
			}
			if name == "style" {
				inStyle = false
			}
			if !dropTag[name] && !dropWhole[name] {
				out.WriteString("</" + name + ">")
			}
		case xhtml.TextToken:
			switch {
			case skip > 0:
			case inStyle:
				if safeCSS(t.Data) {
					out.WriteString(t.Data)
				}
			default:
				out.WriteString(html.EscapeString(t.Data))
			}
		}
	}
	return out.String()
}

// previewDoc is the document the preview iframe shows (srcdoc). The CSP
// forbids scripts, frames and every network load: remote images stay blocked.
func previewDoc(body string) string {
	return `<!doctype html><meta charset="utf-8"><meta http-equiv="Content-Security-Policy" content="default-src 'none'; img-src data:; style-src 'unsafe-inline'">` +
		`<base target="_blank"><body style="font:14px system-ui,sans-serif;margin:12px">` + body
}

// previewHTML makes the sanitized preview document of a message.
func previewHTML(m *parsedMail) string {
	cids := map[string]string{}
	for _, p := range m.Parts {
		if p.cid != "" && strings.HasPrefix(p.Type, "image/") && len(p.Data) < 2<<20 {
			cids[p.cid] = "data:" + p.Type + ";base64," + base64.StdEncoding.EncodeToString(p.Data)
		}
	}
	if m.HTML != "" {
		return previewDoc(sanitizeHTML(m.HTML, cids))
	}
	return previewDoc("<pre style=\"white-space:pre-wrap\">" + html.EscapeString(m.Text) + "</pre>")
}

// mailPage previews one captured message: sandboxed HTML, headers, parts and source.
func (ui *UI) mailPage(w http.ResponseWriter, r *http.Request) {
	list := ui.mailbox()
	n, _ := strconv.Atoi(r.PathValue("n"))
	if n < 0 || n >= len(list) {
		back(w, r, "/_pogi/inbox", fmt.Errorf("message %d is not in the inbox any more", n))
		return
	}
	m := list[n]
	data := map[string]any{"Mail": m, "List": list}
	if pm, err := parseMIME(m.Raw); m.Raw != "" && err == nil {
		data["Parsed"], data["Doc"] = pm, previewHTML(pm)
	} else if m.HTML != "" {
		data["Doc"] = previewDoc(sanitizeHTML(m.HTML, nil))
	} else {
		data["Doc"] = previewDoc("<pre style=\"white-space:pre-wrap\">" + html.EscapeString(m.Body) + "</pre>")
	}
	ui.render(w, r, "x-mail", m.Subject, "inbox", data)
}
