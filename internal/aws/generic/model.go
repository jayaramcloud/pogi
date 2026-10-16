package generic

import (
	"bytes"
	"compress/gzip"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
)

//go:generate sh ../../../scripts/models.sh

//go:embed models
var modelFS embed.FS

// meta is what Info needs, read from models/index.json without parsing a model.
type meta struct {
	Name           string `json:"-"`
	Protocol       string `json:"protocol"` // json, query, ec2, rest-json, rest-xml
	JSONVersion    string `json:"jsonVersion"`
	TargetPrefix   string `json:"targetPrefix"`
	EndpointPrefix string `json:"endpointPrefix"`
	SigningName    string `json:"signingName"`
	APIVersion     string `json:"apiVersion"`
	ServiceID      string `json:"serviceId"`
	Title          string `json:"title"`
}

var (
	indexOnce sync.Once
	index     map[string]*meta
)

func loadIndex() map[string]*meta {
	indexOnce.Do(func() {
		b, err := modelFS.ReadFile("models/index.json")
		if err != nil {
			panic(err)
		}
		if err := json.Unmarshal(b, &index); err != nil {
			panic(err)
		}
		for n, m := range index {
			m.Name = n
			if m.Protocol == "smithy-rpc-v2-cbor" { // such services also speak awsJson (their targetPrefix and jsonVersion are in the index)
				m.Protocol = "json"
			}
		}
	})
	return index
}

// ref is a member of a structure, list or map, or the input/output of an operation.
type ref struct {
	Shape           string `json:"shape"`
	LocationName    string `json:"locationName"`
	Location        string `json:"location"`
	QueryName       string `json:"queryName"`
	Flattened       bool   `json:"flattened"`
	TimestampFormat string `json:"timestampFormat"`
	XMLAttribute    bool   `json:"xmlAttribute"`
	ResultWrapper   string `json:"resultWrapper"`
	XMLNamespace    *struct {
		URI string `json:"uri"`
	} `json:"xmlNamespace"`
	sh *shape
}

type shape struct {
	Name            string          `json:"-"`
	Type            string          `json:"type"`
	Members         map[string]*ref `json:"members"`
	Required        []string        `json:"required"`
	Member          *ref            `json:"member"`
	Key             *ref            `json:"key"`
	Value           *ref            `json:"value"`
	Enum            []string        `json:"enum"`
	Flattened       bool            `json:"flattened"`
	LocationName    string          `json:"locationName"`
	Min             *float64        `json:"min"`
	Max             *float64        `json:"max"`
	Pattern         string          `json:"pattern"`
	Payload         string          `json:"payload"`
	TimestampFormat string          `json:"timestampFormat"`
	Exception       bool            `json:"exception"`
	Document        bool            `json:"document"` // a free-form JSON value (members is empty)
	Error           *struct {
		Code           string `json:"code"`
		HTTPStatusCode int    `json:"httpStatusCode"`
	} `json:"error"`
	XMLNamespace *struct {
		URI string `json:"uri"`
	} `json:"xmlNamespace"`
	names []string // sorted member names
	req   map[string]bool
}

type operation struct {
	Name string `json:"name"`
	HTTP struct {
		Method       string `json:"method"`
		RequestURI   string `json:"requestUri"`
		ResponseCode int    `json:"responseCode"`
	} `json:"http"`
	Input  *ref   `json:"input"`
	Output *ref   `json:"output"`
	Errors []*ref `json:"errors"`

	in, out *shape
	kind    kind
	typ     *rtype
	pager   *pager
	route   *route
}

type pager struct {
	In, Out, Limit, Result string
}

// route is the HTTP binding of an operation (rest protocols).
type route struct {
	op       *operation
	segs     []string // literal, or "{Label}" / "{Label+}"
	query    map[string]string
	literals int
}

type model struct {
	meta   *meta
	ops    map[string]*operation
	shapes map[string]*shape
	routes map[string][]*route // by method, most specific first
	types  map[string]*rtype   // by normalized name
	errs   []*shape            // exception shapes
	xmlns  string
}

type rawPager struct {
	In     json.RawMessage `json:"input_token"`
	Out    json.RawMessage `json:"output_token"`
	Limit  string          `json:"limit_key"`
	Result json.RawMessage `json:"result_key"`
}

func first(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var l []string
	if json.Unmarshal(raw, &l) == nil && len(l) > 0 {
		return l[0]
	}
	return ""
}

func loadModel(md *meta) (*model, error) {
	f, err := modelFS.Open("models/" + md.Name + ".json.gz")
	if err != nil {
		return nil, err
	}
	defer f.Close()
	zr, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	b, err := io.ReadAll(zr)
	if err != nil {
		return nil, err
	}
	var doc struct {
		M struct {
			Operations map[string]*operation `json:"operations"`
			Shapes     map[string]*shape     `json:"shapes"`
		} `json:"m"`
		P map[string]rawPager `json:"p"`
	}
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&doc); err != nil {
		return nil, fmt.Errorf("%s: %w", md.Name, err)
	}
	m := link(md, doc.M.Operations, doc.M.Shapes)
	for n, op := range m.ops {
		if p, ok := doc.P[n]; ok {
			op.pager = &pager{In: first(p.In), Out: first(p.Out), Limit: p.Limit, Result: first(p.Result)}
		}
	}
	m.inferTypes()
	return m, nil
}

// link joins the references of a decoded model (shape names to shapes) and builds its routes.
func link(md *meta, ops map[string]*operation, shapes map[string]*shape) *model {
	m := &model{meta: md, ops: ops, shapes: shapes, routes: map[string][]*route{}}
	resolve := func(r *ref) {
		if r != nil {
			r.sh = m.shapes[r.Shape]
		}
	}
	for n, sh := range m.shapes {
		sh.Name = n
		resolve(sh.Member)
		resolve(sh.Key)
		resolve(sh.Value)
		sh.req = map[string]bool{}
		for _, r := range sh.Required {
			sh.req[r] = true
		}
		for mn, r := range sh.Members {
			resolve(r)
			sh.names = append(sh.names, mn)
		}
		sort.Strings(sh.names)
		if sh.Exception {
			m.errs = append(m.errs, sh)
		}
	}
	sort.Slice(m.errs, func(i, j int) bool { return m.errs[i].Name < m.errs[j].Name })
	for n, op := range m.ops {
		op.Name = n
		resolve(op.Input)
		resolve(op.Output)
		for _, e := range op.Errors {
			resolve(e)
		}
		if op.Input != nil {
			op.in = op.Input.sh
		}
		if op.Output != nil {
			op.out = op.Output.sh
		}
	}
	m.xmlns = "https://" + md.EndpointPrefix + ".amazonaws.com/doc/" + md.APIVersion + "/"
	m.buildRoutes()
	return m
}

func (m *model) buildRoutes() {
	if !strings.HasPrefix(m.meta.Protocol, "rest") {
		return
	}
	for _, op := range m.ops {
		path, q, _ := strings.Cut(op.HTTP.RequestURI, "?")
		rt := &route{op: op, query: map[string]string{}}
		for _, s := range strings.Split(strings.Trim(path, "/"), "/") {
			if s == "" {
				continue
			}
			rt.segs = append(rt.segs, s)
			if !strings.HasPrefix(s, "{") {
				rt.literals++
			}
		}
		for p := range strings.SplitSeq(q, "&") {
			if p != "" {
				k, v, _ := strings.Cut(p, "=")
				rt.query[k] = v
			}
		}
		op.route = rt
		m.routes[op.HTTP.Method] = append(m.routes[op.HTTP.Method], rt)
	}
	for _, l := range m.routes {
		sort.Slice(l, func(i, j int) bool {
			a, b := l[i], l[j]
			if a.literals != b.literals {
				return a.literals > b.literals
			}
			if len(a.query) != len(b.query) {
				return len(a.query) > len(b.query)
			}
			return a.op.Name < b.op.Name
		})
	}
}

// match finds the operation for a REST request and its URI labels.
func (m *model) match(r *http.Request) (*operation, map[string]string) {
	segs := strings.Split(strings.Trim(r.URL.EscapedPath(), "/"), "/")
	if len(segs) == 1 && segs[0] == "" {
		segs = nil
	}
	q := r.URL.Query()
next:
	for _, rt := range m.routes[r.Method] {
		for k, v := range rt.query {
			if !q.Has(k) || (v != "" && q.Get(k) != v) {
				continue next
			}
		}
		labels := map[string]string{}
		i := 0
		for j, s := range rt.segs {
			switch {
			case strings.HasPrefix(s, "{") && strings.HasSuffix(s, "+}"):
				if i >= len(segs) {
					continue next
				}
				rest := strings.Join(segs[i:], "/")
				if j != len(rt.segs)-1 { // greedy label before literals (rare): take what is left after them
					tail := len(rt.segs) - 1 - j
					if len(segs)-i <= tail {
						continue next
					}
					rest = strings.Join(segs[i:len(segs)-tail], "/")
					i = len(segs) - tail - 1
				} else {
					i = len(segs) - 1
				}
				labels[s[1:len(s)-2]], _ = url.PathUnescape(rest)
				i++
			case strings.HasPrefix(s, "{"):
				if i >= len(segs) {
					continue next
				}
				labels[s[1:len(s)-1]], _ = url.PathUnescape(segs[i])
				i++
			default:
				if i >= len(segs) || segs[i] != s {
					continue next
				}
				i++
			}
		}
		if i != len(segs) {
			continue
		}
		return rt.op, labels
	}
	return nil, nil
}

var nonAlnum = regexp.MustCompile(`[^a-z0-9]`)

// norm lower-cases a name and drops everything but letters and digits: the
// key under which members are compared.
func norm(s string) string { return nonAlnum.ReplaceAllString(strings.ToLower(s), "") }
