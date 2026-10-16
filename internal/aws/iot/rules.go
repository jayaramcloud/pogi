package iot

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"

	mqtt "github.com/mochi-mqtt/server/v2"
	"github.com/nrndev/pogi/internal/core"
)

// ---- the topic rules API ----

var ruleNameRe = regexp.MustCompile(`^[a-zA-Z0-9_]{1,128}$`)

// rulePayload decodes and checks the TopicRulePayload of a request.
func rulePayload(r *http.Request) (*Rule, error) {
	var in struct {
		Sql              string          `json:"sql"`
		Description      string          `json:"description"`
		Actions          json.RawMessage `json:"actions"`
		RuleDisabled     bool            `json:"ruleDisabled"`
		AwsIotSqlVersion string          `json:"awsIotSqlVersion"`
		ErrorAction      json.RawMessage `json:"errorAction"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	name := r.PathValue("name")
	if !ruleNameRe.MatchString(name) {
		return nil, invalid("Rule name %s is not valid: it takes letters, digits and underscores.", name)
	}
	if _, err := parse(in.Sql); err != nil {
		return nil, core.Errorf(400, "SqlParseException", "%v", err)
	}
	var actions []map[string]json.RawMessage
	if len(in.Actions) == 0 {
		in.Actions = json.RawMessage("[]")
	}
	if json.Unmarshal(in.Actions, &actions) != nil {
		return nil, invalid("Actions must be a list of actions.")
	}
	return &Rule{RuleName: name, Sql: in.Sql, Description: in.Description, Actions: in.Actions, RuleDisabled: in.RuleDisabled,
		AwsIotSqlVersion: cmp.Or(in.AwsIotSqlVersion, "2015-10-08"), ErrorAction: in.ErrorAction}, nil
}

// rule finds a topic rule. A missing rule is UnauthorizedException, as in
// AWS (Terraform lists rules first because of it).
func (s *Service) rule(c *core.Ctx, name string) (*Rule, error) {
	if r := s.st.Rules[key(c, name)]; r != nil {
		return r, nil
	}
	return nil, core.Errorf(401, "UnauthorizedException", "Access to topic rule '%s' was denied", name)
}

func (s *Service) createRule(c *core.Ctx, r *http.Request) (any, error) {
	rule, err := rulePayload(r)
	if err != nil {
		return nil, err
	}
	tags, err := url.ParseQuery(r.Header.Get("X-Amz-Tagging"))
	if err != nil {
		return nil, invalid("Invalid tags: %v", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Rules[key(c, rule.RuleName)] != nil {
		return nil, exists("Rule %s already exists.", rule.RuleName)
	}
	rule.CreatedAt = core.Time(time.Now())
	s.st.Rules[key(c, rule.RuleName)] = rule
	var tt []tag
	for k := range tags {
		tt = append(tt, tag{k, tags.Get(k)})
	}
	s.tag(c.ARN("iot", "rule/"+rule.RuleName), tt)
	return nil, nil
}

func (s *Service) getRule(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rule, err := s.rule(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	return map[string]any{"ruleArn": c.ARN("iot", "rule/"+rule.RuleName), "rule": rule}, nil
}

func (s *Service) replaceRule(c *core.Ctx, r *http.Request) (any, error) {
	rule, err := rulePayload(r)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.rule(c, rule.RuleName)
	if err != nil {
		return nil, err
	}
	rule.CreatedAt = old.CreatedAt
	s.st.Rules[key(c, rule.RuleName)] = rule
	return nil, nil
}

func (s *Service) deleteRule(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rule, err := s.rule(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Rules, key(c, rule.RuleName))
	delete(s.st.Tags, c.ARN("iot", "rule/"+rule.RuleName))
	return nil, nil
}

// enableRule enables (EnableTopicRule) or disables (DisableTopicRule) a rule.
func (s *Service) enableRule(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rule, err := s.rule(c, r.PathValue("name"))
	if err != nil {
		return nil, err
	}
	v := *rule
	v.RuleDisabled = c.Op == "DisableTopicRule"
	s.st.Rules[key(c, rule.RuleName)] = &v
	return nil, nil
}

func (s *Service) listRules(c *core.Ctx, r *http.Request) (any, error) {
	type item struct {
		RuleArn      string    `json:"ruleArn"`
		RuleName     string    `json:"ruleName"`
		TopicPattern string    `json:"topicPattern"`
		CreatedAt    core.Time `json:"createdAt"`
		RuleDisabled bool      `json:"ruleDisabled"`
	}
	q := r.URL.Query()
	var all []item
	s.mu.Lock()
	for k, rule := range s.st.Rules {
		qr, _ := parse(rule.Sql)
		switch {
		case !strings.HasPrefix(k, key(c, "")), qr == nil:
		case q.Get("topic") != "" && qr.from != q.Get("topic"):
		case q.Get("ruleDisabled") != "" && strconv.FormatBool(rule.RuleDisabled) != q.Get("ruleDisabled"):
		default:
			all = append(all, item{c.ARN("iot", "rule/"+rule.RuleName), rule.RuleName, qr.from, rule.CreatedAt, rule.RuleDisabled})
		}
	}
	s.mu.Unlock()
	items, next := page(all, func(i item) string { return i.RuleName }, q.Get("nextToken"), q.Get("maxResults"))
	return map[string]any{"rules": items, "nextToken": next}, nil
}

// ---- running rules ----

// message is a published message as the rules see it.
type message struct {
	topic, client string
	payload       []byte
	doc           any // the payload as JSON, or undef
	at            time.Time
}

// rules runs the topic rules on a message: those enabled whose topic filter
// matches and whose WHERE clause holds run their actions in the background.
// ponytail: one broker for every account and region, so every account's rules see every message.
func (s *Service) rules(m *message) {
	type hit struct {
		account, region string
		rule            *Rule
		q               *query
	}
	var hits []hit
	s.mu.Lock()
	for k, rule := range s.st.Rules {
		q := s.queries[rule.Sql]
		if q == nil {
			q, _ = parse(rule.Sql) // checked when the rule was made
			s.queries[rule.Sql] = q
		}
		if !rule.RuleDisabled && q != nil && matches(q.from, m.topic) {
			p := strings.SplitN(k, "/", 3)
			hits = append(hits, hit{p[0], p[1], rule, q})
		}
	}
	s.mu.Unlock()
	for _, h := range hits {
		if out, ok := h.q.eval(m); ok {
			core.Go("iot.act", func() { s.act(h.account, h.region, h.rule, m, out) })
		}
	}
}

// matches reports whether an MQTT topic filter (with + and #) matches a topic.
func matches(filter, topic string) bool {
	f, t := strings.Split(filter, "/"), strings.Split(topic, "/")
	for i, p := range f {
		switch {
		case p == "#":
			return true
		case i >= len(t), p != "+" && p != t[i]:
			return false
		}
	}
	return len(f) == len(t)
}

// act runs a rule's actions with its output; a failed action runs the rule's errorAction.
func (s *Service) act(account, region string, rule *Rule, m *message, out []byte) {
	var actions []map[string]json.RawMessage
	json.Unmarshal(rule.Actions, &actions)
	for _, a := range actions {
		for kind, params := range a {
			err := s.do(account, region, kind, params, m, out)
			if err == nil {
				continue
			}
			s.hub.Log.Debug("iot: rule action failed", "rule", rule.RuleName, "action", kind, "err", err)
			var errAction map[string]json.RawMessage
			if json.Unmarshal(rule.ErrorAction, &errAction) != nil {
				continue
			}
			failure, _ := json.Marshal(map[string]any{"ruleName": rule.RuleName, "topic": m.topic, "clientId": m.client,
				"base64OriginalPayload": m.payload, "failures": []map[string]string{{"failedAction": kind + "Action", "errorMessage": err.Error()}}})
			for ek, ep := range errAction {
				if err := s.do(account, region, ek, ep, m, failure); err != nil {
					s.hub.Log.Debug("iot: rule error action failed", "rule", rule.RuleName, "action", ek, "err", err)
				}
			}
		}
	}
}

// do runs one rule action through the hub.
func (s *Service) do(account, region, kind string, raw json.RawMessage, m *message, out []byte) error {
	var p struct {
		FunctionArn, QueueUrl, TargetArn, MessageFormat, BucketName, Key, CannedAcl, TableName, StreamName, PartitionKey string
		DeliveryStreamName, Separator, Topic, StateMachineName, ExecutionNamePrefix, LogGroupName                        string
		HashKeyField, HashKeyValue, HashKeyType, RangeKeyField, RangeKeyValue, RangeKeyType, PayloadField, Operation     string
		UseBase64                                                                                                        bool
		Qos                                                                                                              int
		PutItem                                                                                                          struct{ TableName string }
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return err
	}
	sub := func(tmpl string) string { return substitute(tmpl, m, out) }
	h := s.hub
	status := func(st int, _ http.Header, b []byte) error {
		if st >= 300 {
			return fmt.Errorf("%s: %d %s", kind, st, b)
		}
		return nil
	}
	switch kind {
	case "lambda": // asynchronous, as IoT invokes functions
		return status(h.Do(account, region, "lambda", http.MethodPost, "/2015-03-31/functions/"+url.PathEscape(p.FunctionArn)+"/invocations",
			http.Header{"X-Amz-Invocation-Type": {"Event"}}, out))
	case "sqs":
		body := string(out)
		if p.UseBase64 {
			body = base64.StdEncoding.EncodeToString(out)
		}
		return h.Call(account, region, "AmazonSQS", "SendMessage", map[string]string{"QueueUrl": p.QueueUrl, "MessageBody": body}, nil)
	case "sns":
		in := map[string]string{"TargetArn": p.TargetArn, "Message": string(out)}
		if p.MessageFormat == "JSON" {
			in["MessageStructure"] = "json"
		}
		return h.Call(account, region, "AmazonSimpleNotificationService", "Publish", in, nil)
	case "s3":
		hd := http.Header{}
		if p.CannedAcl != "" {
			hd.Set("X-Amz-Acl", p.CannedAcl)
		}
		return status(h.Do(account, region, "s3", http.MethodPut, (&url.URL{Path: "/" + p.BucketName + "/" + sub(p.Key)}).EscapedPath(), hd, out))
	case "dynamoDBv2":
		var doc map[string]any
		if json.Unmarshal(out, &doc) != nil {
			return errors.New("dynamoDBv2: the rule's output is not a JSON object")
		}
		return h.Call(account, region, "DynamoDB_20120810", "PutItem", map[string]any{"TableName": sub(p.PutItem.TableName), "Item": item(doc)}, nil)
	case "dynamoDB":
		k := map[string]any{p.HashKeyField: keyAttr(p.HashKeyType, sub(p.HashKeyValue))}
		if p.RangeKeyField != "" {
			k[p.RangeKeyField] = keyAttr(p.RangeKeyType, sub(p.RangeKeyValue))
		}
		if strings.EqualFold(p.Operation, "DELETE") {
			return h.Call(account, region, "DynamoDB_20120810", "DeleteItem", map[string]any{"TableName": sub(p.TableName), "Key": k}, nil)
		}
		var doc any
		if json.Unmarshal(out, &doc) != nil {
			doc = string(out)
		}
		k[cmp.Or(p.PayloadField, "payload")] = attr(doc)
		return h.Call(account, region, "DynamoDB_20120810", "PutItem", map[string]any{"TableName": sub(p.TableName), "Item": k}, nil)
	case "kinesis":
		return h.Call(account, region, "Kinesis_20131202", "PutRecord", map[string]any{"StreamName": p.StreamName, "Data": out,
			"PartitionKey": cmp.Or(sub(p.PartitionKey), core.UUID())}, nil)
	case "firehose":
		return h.Call(account, region, "Firehose_20150804", "PutRecord", map[string]any{"DeliveryStreamName": p.DeliveryStreamName,
			"Record": map[string]any{"Data": slices.Concat(out, []byte(p.Separator))}}, nil)
	case "republish":
		return s.mq.Publish(sub(p.Topic), out, false, byte(min(p.Qos, 1)))
	case "stepFunctions":
		return h.Call(account, region, "AWSStepFunctions", "StartExecution", map[string]string{"input": string(out),
			"stateMachineArn": core.ARN("states", region, account, "stateMachine:"+p.StateMachineName), "name": p.ExecutionNamePrefix + core.UUID()}, nil)
	case "cloudwatchLogs":
		stream := "iot-rules"
		h.Call(account, region, "Logs_20140328", "CreateLogStream", map[string]string{"logGroupName": p.LogGroupName, "logStreamName": stream}, nil)
		return h.Call(account, region, "Logs_20140328", "PutLogEvents", map[string]any{"logGroupName": p.LogGroupName, "logStreamName": stream,
			"logEvents": []map[string]any{{"timestamp": m.at.UnixMilli(), "message": string(out)}}}, nil)
	}
	// ponytail: the other actions (http, kafka, timestream…) are kept but not run
	s.hub.Log.Debug("iot: rule action not run by pogi", "action", kind)
	return nil
}

// attr is a JSON value as a DynamoDB attribute value.
func attr(v any) map[string]any {
	switch v := v.(type) {
	case string:
		return map[string]any{"S": v}
	case float64:
		return map[string]any{"N": strconv.FormatFloat(v, 'f', -1, 64)}
	case bool:
		return map[string]any{"BOOL": v}
	case []any:
		l := make([]any, len(v))
		for i, x := range v {
			l[i] = attr(x)
		}
		return map[string]any{"L": l}
	case map[string]any:
		return map[string]any{"M": item(v)}
	}
	return map[string]any{"NULL": true}
}

func item(doc map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range doc {
		out[k] = attr(v)
	}
	return out
}

func keyAttr(typ, v string) map[string]any {
	if typ == "NUMBER" {
		return map[string]any{"N": v}
	}
	return map[string]any{"S": v}
}

var template = regexp.MustCompile(`\$\{([^}]*)\}`)

// substitute fills the ${expression} templates of an action parameter, from
// the rule's output and else the message itself.
func substitute(s string, m *message, out []byte) string {
	if !strings.Contains(s, "${") {
		return s
	}
	o := *m
	var doc any
	if json.Unmarshal(out, &doc) == nil {
		o.doc = doc
	}
	return template.ReplaceAllStringFunc(s, func(t string) string {
		e, err := parseExpr(t[2 : len(t)-1])
		if err != nil {
			return ""
		}
		v := e.eval(&o)
		if v == undef {
			v = e.eval(m)
		}
		if v == undef {
			return ""
		}
		return text(v)
	})
}

func text(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// ---- the rule SQL (version 2016-03-23, a subset) ----
//
//	SELECT * | expr [AS name], … FROM 'topic/filter/+/#' [WHERE expr]
//
// Expressions are fields and nested paths (a.b.c), numbers, 'strings', true,
// false, null, comparisons (= <> != < <= > >=), AND, OR, NOT, parentheses
// and the functions topic(n), timestamp(), clientid(), upper, lower and
// concat. Comparing values of different types, or a missing field, is
// undefined: the WHERE clause fails and SELECT leaves the field out.

// undefined is the value of what a message does not have.
type undefined struct{}

var undef any = undefined{}

type expr interface{ eval(m *message) any }

type (
	literal struct{ v any }
	path    []string
	call    struct {
		name string
		args []expr
	}
	binary struct {
		op   string
		l, r expr
	}
	negation struct{ x expr }
)

var funcs = []string{"topic", "timestamp", "clientid", "upper", "lower", "concat"}

type selected struct {
	e    expr
	name string
	star bool
}

type query struct {
	items []selected
	from  string
	where expr
}

type token struct {
	kind byte // w: word, n: number, s: string, p: punctuation
	text string
}

func lex(src string) ([]token, error) {
	var out []token
	word := func(c byte) bool { return c == '_' || c == '$' || c >= '0' && c <= '9' || unicode.IsLetter(rune(c)) }
	for i := 0; i < len(src); {
		c := src[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'' || c == '"':
			j := strings.IndexByte(src[i+1:], c)
			if j < 0 {
				return nil, errors.New("unterminated string")
			}
			out = append(out, token{'s', src[i+1 : i+1+j]})
			i += j + 2
		case c >= '0' && c <= '9':
			j := i
			for j < len(src) && (src[j] >= '0' && src[j] <= '9' || src[j] == '.') {
				j++
			}
			if j < len(src) && (src[j] == 'e' || src[j] == 'E') { // exponent, as in 1e5 or 2.5E-3
				k := j + 1
				if k < len(src) && (src[k] == '+' || src[k] == '-') {
					k++
				}
				if k < len(src) && src[k] >= '0' && src[k] <= '9' {
					for j = k; j < len(src) && src[j] >= '0' && src[j] <= '9'; j++ {
					}
				}
			}
			out = append(out, token{'n', src[i:j]})
			i = j
		case word(c):
			j := i
			for j < len(src) && word(src[j]) {
				j++
			}
			out = append(out, token{'w', src[i:j]})
			i = j
		default:
			op := src[i : i+1]
			if two := src[i:min(i+2, len(src))]; two == "<>" || two == "!=" || two == "<=" || two == ">=" {
				op = two
			} else if !strings.Contains("=<>,.()*-", op) {
				return nil, fmt.Errorf("unexpected character %q", op)
			}
			out = append(out, token{'p', op})
			i += len(op)
		}
	}
	return out, nil
}

type parser struct {
	toks []token
	i    int
}

func (p *parser) next() token {
	var t token
	if p.i < len(p.toks) {
		t = p.toks[p.i]
	}
	p.i++
	return t
}

func (p *parser) is(kind byte, text string) bool {
	if p.i < len(p.toks) && p.toks[p.i].kind == kind && strings.EqualFold(p.toks[p.i].text, text) {
		p.i++
		return true
	}
	return false
}

func parse(sql string) (*query, error) {
	toks, err := lex(sql)
	if err != nil {
		return nil, err
	}
	p, q := &parser{toks: toks}, &query{}
	if !p.is('w', "SELECT") {
		return nil, errors.New("the statement must start with SELECT")
	}
	for {
		if p.is('p', "*") {
			q.items = append(q.items, selected{star: true})
		} else {
			e, err := p.expr()
			if err != nil {
				return nil, err
			}
			it := selected{e: e, name: "_" + strconv.Itoa(len(q.items)+1)}
			switch e := e.(type) {
			case path:
				it.name = e[len(e)-1]
			case call:
				it.name = e.name
			}
			if p.is('w', "AS") {
				t := p.next()
				if t.kind != 'w' && t.kind != 's' {
					return nil, errors.New("AS needs a name")
				}
				it.name = t.text
			}
			q.items = append(q.items, it)
		}
		if !p.is('p', ",") {
			break
		}
	}
	if !p.is('w', "FROM") {
		return nil, errors.New("expected FROM after the SELECT list")
	}
	t := p.next()
	if t.kind != 's' || !mqtt.IsValidFilter(t.text, false) {
		return nil, errors.New("FROM takes a quoted topic filter")
	}
	q.from = t.text
	if p.is('w', "WHERE") {
		if q.where, err = p.expr(); err != nil {
			return nil, err
		}
	}
	if p.i < len(p.toks) {
		return nil, fmt.Errorf("unexpected %q", p.toks[p.i].text)
	}
	return q, nil
}

func parseExpr(src string) (expr, error) {
	toks, err := lex(src)
	if err != nil {
		return nil, err
	}
	p := &parser{toks: toks}
	e, err := p.expr()
	if err == nil && p.i < len(toks) {
		err = fmt.Errorf("unexpected %q", toks[p.i].text)
	}
	return e, err
}

func (p *parser) expr() (expr, error) {
	l, err := p.and()
	for err == nil && p.is('w', "OR") {
		var r expr
		r, err = p.and()
		l = binary{"OR", l, r}
	}
	return l, err
}

func (p *parser) and() (expr, error) {
	l, err := p.not()
	for err == nil && p.is('w', "AND") {
		var r expr
		r, err = p.not()
		l = binary{"AND", l, r}
	}
	return l, err
}

func (p *parser) not() (expr, error) {
	if p.is('w', "NOT") {
		x, err := p.not()
		return negation{x}, err
	}
	l, err := p.primary()
	if err != nil {
		return nil, err
	}
	for _, op := range []string{"=", "<>", "!=", "<=", ">=", "<", ">"} {
		if p.is('p', op) {
			r, err := p.primary()
			return binary{op, l, r}, err
		}
	}
	return l, nil
}

func (p *parser) primary() (expr, error) {
	t := p.next()
	switch t.kind {
	case 'n':
		f, err := strconv.ParseFloat(t.text, 64)
		return literal{f}, err
	case 's':
		return literal{t.text}, nil
	case 'p':
		switch t.text {
		case "(":
			e, err := p.expr()
			if err == nil && !p.is('p', ")") {
				err = errors.New("expected )")
			}
			return e, err
		case "-":
			if n := p.next(); n.kind == 'n' {
				f, err := strconv.ParseFloat(n.text, 64)
				return literal{-f}, err
			}
		}
	case 'w':
		switch w := strings.ToLower(t.text); w {
		case "true", "false":
			return literal{w == "true"}, nil
		case "null":
			return literal{nil}, nil
		case "select", "from", "where", "and", "or", "not", "as":
			return nil, fmt.Errorf("unexpected %s", t.text)
		}
		if p.is('p', "(") {
			c := call{name: strings.ToLower(t.text)}
			if !slices.Contains(funcs, c.name) {
				return nil, fmt.Errorf("unknown function %s", t.text)
			}
			for !p.is('p', ")") {
				if len(c.args) > 0 && !p.is('p', ",") {
					return nil, fmt.Errorf("expected , or ) in %s()", c.name)
				}
				a, err := p.expr()
				if err != nil {
					return nil, err
				}
				c.args = append(c.args, a)
			}
			return c, nil
		}
		pth := path{t.text}
		for p.is('p', ".") {
			n := p.next()
			if n.kind != 'w' {
				return nil, errors.New("expected a field name after .")
			}
			pth = append(pth, n.text)
		}
		return pth, nil
	}
	if t.text == "" {
		return nil, errors.New("unexpected end of the statement")
	}
	return nil, fmt.Errorf("unexpected %q", t.text)
}

// eval applies a query to a message: the JSON its actions get, and whether its WHERE clause holds.
func (q *query) eval(m *message) ([]byte, bool) {
	if q.where != nil && q.where.eval(m) != true {
		return nil, false
	}
	out := map[string]any{}
	for _, it := range q.items {
		if !it.star {
			if v := it.e.eval(m); v != undef {
				out[it.name] = v
			}
			continue
		}
		obj, ok := m.doc.(map[string]any)
		if !ok && len(q.items) == 1 {
			return m.payload, true // SELECT * of a payload that is not a JSON object passes it on as is
		}
		maps.Copy(out, obj)
	}
	b, _ := json.Marshal(out)
	return b, true
}

func (l literal) eval(*message) any { return l.v }

func (p path) eval(m *message) any {
	v := m.doc
	for _, f := range p {
		obj, ok := v.(map[string]any)
		if !ok {
			return undef
		}
		if v, ok = obj[f]; !ok {
			return undef
		}
	}
	return v
}

func (n negation) eval(m *message) any {
	if b, ok := n.x.eval(m).(bool); ok {
		return !b
	}
	return undef
}

func (b binary) eval(m *message) any {
	l := b.l.eval(m)
	switch b.op {
	case "AND", "OR":
		short := b.op == "OR" // the value that decides alone
		if l == short {
			return short
		}
		r := b.r.eval(m)
		if r == short {
			return short
		}
		if l == !short && r == !short {
			return !short
		}
		return undef
	}
	r := b.r.eval(m)
	var c int
	switch a := l.(type) {
	case float64:
		x, ok := r.(float64)
		if !ok {
			return undef
		}
		c = cmp.Compare(a, x)
	case string:
		x, ok := r.(string)
		if !ok {
			return undef
		}
		c = strings.Compare(a, x)
	default:
		if l == undef || r == undef || b.op != "=" && b.op != "<>" && b.op != "!=" {
			return undef
		}
		if !reflect.DeepEqual(l, r) {
			c = 1
		}
	}
	switch b.op {
	case "=":
		return c == 0
	case "<>", "!=":
		return c != 0
	case "<":
		return c < 0
	case "<=":
		return c <= 0
	case ">":
		return c > 0
	}
	return c >= 0
}

func (c call) eval(m *message) any {
	args := make([]any, len(c.args))
	for i, a := range c.args {
		if args[i] = a.eval(m); args[i] == undef {
			return undef
		}
	}
	switch c.name {
	case "topic":
		if len(args) == 0 {
			return m.topic
		}
		parts := strings.Split(m.topic, "/")
		if n, ok := args[0].(float64); ok && n >= 1 && int(n) <= len(parts) {
			return parts[int(n)-1]
		}
	case "timestamp":
		return float64(m.at.UnixMilli())
	case "clientid":
		return m.client
	case "upper", "lower":
		if len(args) == 1 {
			if s, ok := args[0].(string); ok {
				return map[string]func(string) string{"upper": strings.ToUpper, "lower": strings.ToLower}[c.name](s)
			}
		}
	case "concat":
		var list []any
		var b strings.Builder
		arrays := len(args) >= 2
		for _, a := range args {
			l, ok := a.([]any)
			arrays = arrays && ok
			list = append(list, l...)
			b.WriteString(text(a))
		}
		switch {
		case arrays:
			return list
		case len(args) >= 2:
			return b.String()
		}
	}
	return undef
}
