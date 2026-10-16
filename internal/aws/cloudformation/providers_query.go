package cloudformation

import (
	"bytes"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Providers for the services that speak the Query protocol (EC2, ELB v2, Auto
// Scaling, RDS) send a form and read the XML answer through the gateway,
// the same way the AWS SDK would.

// xnode is one element of an XML answer.
type xnode struct {
	Name string
	Text string
	Kids []*xnode
}

func parseXML(b []byte) *xnode {
	d := xml.NewDecoder(bytes.NewReader(b))
	root := &xnode{}
	stack := []*xnode{root}
	for {
		tok, err := d.Token()
		if err != nil {
			break
		}
		switch t := tok.(type) {
		case xml.StartElement:
			n := &xnode{Name: t.Name.Local}
			top := stack[len(stack)-1]
			top.Kids = append(top.Kids, n)
			stack = append(stack, n)
		case xml.EndElement:
			if len(stack) > 1 {
				stack = stack[:len(stack)-1]
			}
		case xml.CharData:
			stack[len(stack)-1].Text += string(t)
		}
	}
	if len(root.Kids) == 1 {
		return root.Kids[0]
	}
	return root
}

// get follows element names down from n; nil when one is missing.
func (n *xnode) get(path ...string) *xnode {
	for _, name := range path {
		if n == nil {
			return nil
		}
		var next *xnode
		for _, k := range n.Kids {
			if k.Name == name {
				next = k
				break
			}
		}
		n = next
	}
	return n
}

// find returns the first element called name anywhere below n.
func (n *xnode) find(name string) *xnode {
	if n == nil {
		return nil
	}
	for _, k := range n.Kids {
		if k.Name == name {
			return k
		}
		if f := k.find(name); f != nil {
			return f
		}
	}
	return nil
}

// text is the text of the element at path ("" when it is missing).
func (n *xnode) text(path ...string) string {
	if e := n.get(path...); e != nil {
		return strings.TrimSpace(e.Text)
	}
	return ""
}

// findText is the text of the first element called name anywhere below n.
func (n *xnode) findText(name string) string {
	if e := n.find(name); e != nil {
		return strings.TrimSpace(e.Text)
	}
	return ""
}

// items are the entries of the list at path (<item> or <member> children).
func (n *xnode) items(path ...string) []*xnode {
	var out []*xnode
	if l := n.get(path...); l != nil {
		for _, k := range l.Kids {
			if k.Name == "item" || k.Name == "member" {
				out = append(out, k)
			}
		}
	}
	return out
}

// form flattens a value into Query keys: maps as Name.Key, lists as Name.N.
func form(v url.Values, key string, x any) {
	switch t := x.(type) {
	case nil:
	case map[string]any:
		for _, k := range core.SortedKeys(t) {
			form(v, join(key, k), t[k])
		}
	case []any:
		for i, e := range t {
			form(v, key+"."+strconv.Itoa(i+1), e)
		}
	case []string:
		for i, e := range t {
			v.Set(key+"."+strconv.Itoa(i+1), e)
		}
	case []map[string]string:
		for i, e := range t {
			for _, k := range core.SortedKeys(e) {
				v.Set(key+"."+strconv.Itoa(i+1)+"."+k, e[k])
			}
		}
	case string:
		v.Set(key, t)
	case float64:
		v.Set(key, strconv.FormatFloat(t, 'f', -1, 64))
	case bool:
		v.Set(key, strconv.FormatBool(t))
	default:
		v.Set(key, fmt.Sprint(t))
	}
}

func join(a, b string) string {
	if a == "" {
		return b
	}
	return a + "." + b
}

// query calls a Query-protocol operation (signing is the service's signing name, like "ec2").
func (c *pctx) query(signing, action string, params map[string]any) (*xnode, error) {
	v := url.Values{"Action": {action}}
	form(v, "", params)
	status, _, resp := c.s.hub.Do(c.account, c.region, signing, http.MethodPost, "/",
		http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}, []byte(v.Encode()))
	root := parseXML(resp)
	if status >= 300 {
		code, msg := root.findText("Code"), root.findText("Message")
		if code == "" {
			code, msg = http.StatusText(status), string(resp)
		}
		return nil, core.Errorf(status, code, "%s", msg)
	}
	return root, nil
}

func (c *pctx) ec2(action string, params map[string]any) (*xnode, error) {
	return c.query("ec2", action, params)
}

// ec2Tags makes the TagSpecification list for a new resource of a type ("vpc"); nil when there are no tags.
func ec2Tags(resource string, tags any) any {
	t := tagList(tags)
	if len(t) == 0 {
		return nil
	}
	return []any{map[string]any{"ResourceType": resource, "Tag": t}}
}

// ec2Retag makes a resource's tags match the new ones: removed keys are deleted, new and changed ones are set.
func (c *pctx) ec2Retag(id string, old, now any) error {
	o, n := tagMap(old), tagMap(now)
	var gone, add []map[string]string
	for _, k := range core.SortedKeys(o) {
		if _, ok := n[k]; !ok {
			gone = append(gone, map[string]string{"Key": k})
		}
	}
	for _, k := range core.SortedKeys(n) {
		if v, ok := o[k]; !ok || v != n[k] {
			add = append(add, map[string]string{"Key": k, "Value": n[k]})
		}
	}
	if len(gone) > 0 {
		if _, err := c.ec2("DeleteTags", map[string]any{"ResourceId": []string{id}, "Tag": gone}); err != nil {
			return err
		}
	}
	if len(add) > 0 {
		if _, err := c.ec2("CreateTags", map[string]any{"ResourceId": []string{id}, "Tag": add}); err != nil {
			return err
		}
	}
	return nil
}

// differs says whether a property outside inPlace changed: those need a replacement.
func differs(old, now map[string]any, inPlace ...string) bool {
	for _, m := range []map[string]any{old, now} {
		for k := range m {
			if !slices.Contains(inPlace, k) && !equal(old[k], now[k]) {
				return true
			}
		}
	}
	return false
}

// restXML sends an XML request to a REST service (Route 53, CloudFront) and parses the XML answer.
func (c *pctx) restXML(signing, method, path string, body string) (*xnode, error) {
	n, _, err := c.restXMLh(signing, method, path, body, nil)
	return n, err
}

// restXMLh is restXML with request headers (If-Match) and the answer's headers (ETag).
func (c *pctx) restXMLh(signing, method, path string, body string, hdr http.Header) (*xnode, http.Header, error) {
	var b []byte
	if body != "" {
		b = []byte(xml.Header + body)
	}
	h := http.Header{"Content-Type": {"application/xml"}}
	for k, v := range hdr {
		h[k] = v
	}
	status, rh, resp := c.s.hub.Do(c.account, c.region, signing, method, path, h, b)
	root := parseXML(resp)
	if status >= 300 {
		code, msg := root.findText("Code"), root.findText("Message")
		if code == "" {
			code, msg = http.StatusText(status), string(resp)
		}
		return nil, nil, core.Errorf(status, code, "%s", msg)
	}
	return root, rh, nil
}

// xmlOf writes a value as XML elements: a map as child elements (in key order), a list as the element repeated, text escaped.
// item names the element of a list by the name of the list member ("ResourceRecords" -> "ResourceRecord").
func xmlOf(name string, v any, item map[string]string) string {
	var b strings.Builder
	switch x := v.(type) {
	case nil:
	case map[string]any:
		b.WriteString("<" + name + ">")
		for _, k := range core.SortedKeys(x) {
			b.WriteString(xmlOf(k, x[k], item))
		}
		b.WriteString("</" + name + ">")
	case []any:
		if it := item[name]; it != "" { // a wrapper with repeated children
			b.WriteString("<" + name + ">")
			for _, e := range x {
				b.WriteString(xmlOf(it, e, item))
			}
			b.WriteString("</" + name + ">")
			break
		}
		for _, e := range x {
			b.WriteString(xmlOf(name, e, item))
		}
	default:
		var esc bytes.Buffer
		xml.EscapeText(&esc, []byte(str(x)))
		b.WriteString("<" + name + ">" + esc.String() + "</" + name + ">")
	}
	return b.String()
}

// restRaw sends a request with its own headers and body (not JSON) and returns the answer's headers and body.
func (c *pctx) restRaw(signing, method, path string, hdr http.Header, body []byte) (http.Header, []byte, error) {
	status, h, resp := c.s.hub.Do(c.account, c.region, signing, method, path, hdr, body)
	if status >= 300 {
		var j struct{ Message, Code string }
		code, msg := http.StatusText(status), string(resp)
		if json.Unmarshal(resp, &j) == nil && (j.Message != "" || j.Code != "") {
			code, msg = j.Code, j.Message
		}
		return nil, nil, core.Errorf(status, code, "%s", msg)
	}
	return h, resp, nil
}
