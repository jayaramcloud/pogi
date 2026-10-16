package generic

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// This file checks the limits of the AWS API models (min, max, enum) on the input of a request, the way
// AWS does. The generic engine calls validate itself. CheckInput runs it for the hand-written services,
// from models/limits (only the input members that carry limits, see scripts/models.sh).

// checker collects the failures of one input. skip names members (by model name, at any depth) that a
// service checks itself, with its own error.
type checker struct {
	required bool
	skip     map[string]bool
	errs     []string
}

// validate checks the limits of an input, and its required members when required is set. It returns
// AWS's message ("2 validation errors detected: Value ..."), or "" when the input is fine.
func validate(sh *shape, in map[string]any, required bool, skip map[string]bool) string {
	ck := &checker{required: required, skip: skip}
	ck.structure(sh, in, "")
	switch len(ck.errs) {
	case 0:
		return ""
	case 1:
		return "1 validation error detected: " + ck.errs[0]
	}
	return fmt.Sprint(len(ck.errs), " validation errors detected: ", strings.Join(ck.errs, "; "))
}

// structure checks the members of a structure. path is the member path so far, in AWS's
// "list.1.member.name" style.
func (ck *checker) structure(sh *shape, v map[string]any, path string) {
	if ck.required {
		for _, n := range sh.Required {
			if v[n] == nil {
				ck.errs = append(ck.errs, fmt.Sprintf("Value null at '%s' failed to satisfy constraint: Member must not be null", path+lowerFirst(n)))
			}
		}
	}
	for _, n := range sh.names {
		if x, ok := v[n]; ok && !ck.skip[n] {
			mr := sh.Members[n]
			ck.value(mr.sh, x, path+lowerFirst(locName(n, mr)))
		}
	}
}

// value checks one value against its shape. A value of another type than the shape says (a query
// parameter that is not a number) is left to the service.
func (ck *checker) value(sh *shape, x any, path string) {
	fail := func(what string) {
		ck.errs = append(ck.errs, fmt.Sprintf("Value '%s' at '%s' failed to satisfy constraint: Member must %s", show(x), path, what))
	}
	switch sh.Type {
	case "structure":
		if m, ok := x.(map[string]any); ok {
			ck.structure(sh, m, path+".")
		}
	case "list":
		l, ok := x.([]any)
		if !ok {
			return
		}
		checkSize(sh, float64(len(l)), fail)
		for i, e := range l {
			ck.value(sh.Member.sh, e, fmt.Sprintf("%s.%d.member", path, i+1))
		}
	case "map":
		m, ok := x.(map[string]any)
		if !ok {
			return
		}
		checkSize(sh, float64(len(m)), fail)
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			ck.value(sh.Value.sh, m[k], path+"."+k)
		}
	case "string":
		s, ok := x.(string)
		if !ok {
			return
		}
		checkSize(sh, float64(utf8.RuneCountInString(s)), fail)
		if len(sh.Enum) > 0 && !slices.Contains(sh.Enum, s) {
			fail("satisfy enum value set: [" + strings.Join(sh.Enum, ", ") + "]")
		}
	case "integer", "long", "float", "double":
		f, ok := number(x)
		if !ok {
			return
		}
		if sh.Min != nil && f < *sh.Min {
			fail("have value greater than or equal to " + num(*sh.Min))
		}
		if sh.Max != nil && f > *sh.Max {
			fail("have value less than or equal to " + num(*sh.Max))
		}
	}
}

// checkSize checks the length of a string or the size of a list or map.
func checkSize(sh *shape, n float64, fail func(string)) {
	if sh.Min != nil && n < *sh.Min {
		fail("have length greater than or equal to " + num(*sh.Min))
	}
	if sh.Max != nil && n > *sh.Max {
		fail("have length less than or equal to " + num(*sh.Max))
	}
}

func number(x any) (float64, bool) {
	switch n := x.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	}
	return 0, false
}

func num(f float64) string { return strconv.FormatFloat(f, 'f', -1, 64) }

// show prints a value the way AWS does in its message: a list as [a, b], a map as {k=v}.
func show(x any) string {
	switch v := x.(type) {
	case float64:
		return num(v)
	case []any:
		parts := make([]string, len(v))
		for i, e := range v {
			parts[i] = show(e)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case map[string]any:
		parts := make([]string, 0, len(v))
		for k, e := range v {
			parts = append(parts, k+"="+show(e))
		}
		sort.Strings(parts)
		return "{" + strings.Join(parts, ", ") + "}"
	}
	return fmt.Sprint(x)
}

// lowerFirst lower-cases the first letter of a member name: AWS prints 'maxResults' for MaxResults.
func lowerFirst(s string) string {
	if s == "" {
		return s
	}
	return strings.ToLower(s[:1]) + s[1:]
}

// ---- the check for hand-written services

var (
	limitMu    sync.Mutex
	limitIndex map[string][]string // service name -> botocore directories
	limitCache = map[string][]*model{}
)

// limitModels returns the models that check the input of a hand-written service (none for others).
func limitModels(name string) []*model {
	limitMu.Lock()
	defer limitMu.Unlock()
	if limitIndex == nil {
		b, err := modelFS.ReadFile("models/limits/index.json")
		if err != nil {
			panic(err)
		}
		if err := json.Unmarshal(b, &limitIndex); err != nil {
			panic(err)
		}
	}
	if ms, ok := limitCache[name]; ok {
		return ms
	}
	var ms []*model
	for _, dir := range limitIndex[name] {
		if md := loadIndex()[dir]; md != nil { // a generic service has its full model
			if m, err := loadModel(md); err == nil {
				ms = append(ms, m)
			}
		} else if m, err := loadLimits(dir); err == nil {
			ms = append(ms, m)
		}
	}
	limitCache[name] = ms
	return ms
}

func loadLimits(dir string) (*model, error) {
	f, err := modelFS.Open("models/limits/" + dir + ".json.gz")
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
			Metadata   meta                  `json:"metadata"`
			Operations map[string]*operation `json:"operations"`
			Shapes     map[string]*shape     `json:"shapes"`
		} `json:"m"`
	}
	if err := json.NewDecoder(bytes.NewReader(b)).Decode(&doc); err != nil {
		return nil, fmt.Errorf("%s: %w", dir, err)
	}
	md := &doc.M.Metadata
	md.Name = dir
	if md.Protocol == "smithy-rpc-v2-cbor" {
		md.Protocol = "json"
	}
	return link(md, doc.M.Operations, doc.M.Shapes), nil
}

// restXMLCode is the code a rest-xml service answers a bad input with.
var restXMLCode = map[string]string{"route53": "InvalidInput", "cloudfront": "InvalidArgument"}

// limitCode is the code of the validation error of the json and rest-json services that do not use
// ValidationException.
var limitCode = map[string]string{"cognito-idp": "InvalidParameterException", "lambda": "InvalidParameterValueException"}

// limitSkip are the members a hand-written service checks itself and answers with its own AWS error
// (IAM's LimitExceeded for tags, Cognito's NotAuthorizedException for an unknown session), and the
// enums AWS reads without case (Amazon MQ takes ActiveMQ and ACTIVEMQ).
var limitSkip = map[string]map[string]bool{
	"cognito-idp":              {"Session": true},
	"iam":                      {"Tags": true},
	"mq":                       {"EngineType": true},
	"resourcegroupstaggingapi": {"Tags": true},
}

// CheckInput is core.Hub.CheckInput: it checks the limits of the input of a request to a hand-written
// AWS service before the service sees it, and answers a bad one with AWS's validation error. It reports
// whether it answered. A request it cannot parse is left to the service.
func CheckInput(svc core.Service, c *core.Ctx, w http.ResponseWriter, r *http.Request) bool {
	if _, own := svc.(*Service); own || r.Header.Get("Content-Encoding") != "" {
		return false // the generic engine checks its own input
	}
	body := c.Payload()
	for _, m := range limitModels(svc.Info().Name) {
		op, in, wire, err := m.parse(r, body)
		if err != nil || op == nil || op.in == nil {
			continue
		}
		msg := validate(op.in, in, false, limitSkip[svc.Info().Name])
		if msg == "" {
			return false
		}
		code := "ValidationException"
		if c := limitCode[svc.Info().Name]; c != "" {
			code = c
		}
		switch wire {
		case wQuery:
			code = "ValidationError"
		case wRestXML:
			if code = restXMLCode[svc.Info().Name]; code == "" {
				code = "ValidationError"
			}
		}
		c.Op = op.Name
		m.writeErr(w, c, wire, core.Errorf(http.StatusBadRequest, code, "%s", msg))
		return true
	}
	return false
}
