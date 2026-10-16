package appsync

import (
	"cmp"
	"encoding/json"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dop251/goja"
	"github.com/nrndev/pogi/internal/core"
)

// jsPrelude is what APPSYNC_JS code imports (__mods): util, runtime and
// extensions from @aws-appsync/utils, and the @aws-appsync/utils/dynamodb
// helpers; plus console, and __run, which calls an exported function with ctx.
// ponytail: util covers ids, errors, null checks, time and DynamoDB values;
// no rds, http or str helpers.
const jsPrelude = `
var __errors = [], __logs = [], __info = {};
function __Err(message, type, data, info) { this.message = message; this.type = type; this.data = data; this.info = info; }
function __Early(data) { this.data = data; }
function __log() {
  __logs.push(Array.prototype.map.call(arguments, function (v) { return typeof v === 'string' ? v : JSON.stringify(v); }).join(' '));
}
var console = { log: __log, info: __log, warn: __log, error: __log, debug: __log };
function __ddb(v) {
  if (v === null || v === undefined) return { NULL: true };
  switch (typeof v) {
    case 'string': return { S: v };
    case 'number': return { N: String(v) };
    case 'boolean': return { BOOL: v };
  }
  if (Array.isArray(v)) return { L: v.map(__ddb) };
  return { M: __ddbMap(v) };
}
function __ddbMap(o) {
  var m = {};
  Object.keys(o || {}).forEach(function (k) { if (o[k] !== undefined) m[k] = __ddb(o[k]); });
  return m;
}
function __cond(f) { return f ? JSON.parse(__expr(JSON.stringify(f))) : undefined; }
function __proj(p) {
  if (!p) return undefined;
  var names = {};
  return { expression: p.map(function (f, i) { names['#p' + i] = f; return '#p' + i; }).join(', '), expressionNames: names };
}
function __empty(v) { return v === null || v === undefined || v === ''; }
var __util = {
  autoId: __autoId,
  error: function (message, type, data, info) { throw new __Err(message, type, data, info); },
  appendError: function (message, type, data, info) { __errors.push({ message: message, errorType: type, data: data, errorInfo: info }); },
  unauthorized: function () { throw new __Err('Not Authorized to access ' + __info.fieldName + ' on type ' + __info.parentTypeName, 'Unauthorized'); },
  isNull: function (v) { return v === null || v === undefined; },
  isNullOrEmpty: __empty,
  isNullOrBlank: function (v) { return __empty(v) || String(v).trim() === ''; },
  defaultIfNull: function (v, d) { return v === null || v === undefined ? d : v; },
  defaultIfNullOrEmpty: function (v, d) { return __empty(v) ? d : v; },
  defaultIfNullOrBlank: function (v, d) { return __empty(v) || String(v).trim() === '' ? d : v; },
  time: {
    nowISO8601: function () { return new Date().toISOString(); },
    nowEpochSeconds: function () { return Math.floor(Date.now() / 1000); },
    nowEpochMilliSeconds: function () { return Date.now(); },
  },
  dynamodb: { toDynamoDB: __ddb, toMapValues: __ddbMap },
  transform: {
    toDynamoDBFilterExpression: function (f) { return __expr(JSON.stringify(f)); },
    toDynamoDBConditionExpression: function (f) { return __expr(JSON.stringify(f)); },
  },
};
function __update(u) {
  var set = [], add = [], rem = [], names = {}, values = {};
  Object.keys(u || {}).forEach(function (k, i) {
    var o = u[k], n = '#u' + i, v = ':u' + i;
    names[n] = k;
    if (!o || !o.__op) o = { __op: 'set', v: o };
    if (o.__op === 'remove') return rem.push(n);
    values[v] = __ddb(o.v);
    if (o.__op === 'add') add.push(n + ' ' + v);
    else if (o.__op === 'append') set.push(n + ' = list_append(' + n + ', ' + v + ')');
    else if (o.__op === 'prepend') set.push(n + ' = list_append(' + v + ', ' + n + ')');
    else if (o.__op === 'item') set.push(n + '[' + o.i + '] = ' + v);
    else set.push(n + ' = ' + v);
  });
  var e = [];
  if (set.length) e.push('SET ' + set.join(', '));
  if (add.length) e.push('ADD ' + add.join(', '));
  if (rem.length) e.push('REMOVE ' + rem.join(', '));
  return { expression: e.join(' '), expressionNames: names, expressionValues: values };
}
var __mods = {
  '@aws-appsync/utils': {
    util: __util,
    runtime: { earlyReturn: function (data) { throw new __Early(data); } },
    extensions: { setSubscriptionFilter: function () {}, setSubscriptionInvalidationFilter: function () {}, invalidateSubscriptions: function () {} },
  },
  '@aws-appsync/utils/dynamodb': {
    get: function (p) { return { operation: 'GetItem', key: __ddbMap(p.key), consistentRead: p.consistentRead, projection: __proj(p.projection) }; },
    put: function (p) { return { operation: 'PutItem', key: __ddbMap(p.key), attributeValues: __ddbMap(p.item), condition: __cond(p.condition) }; },
    remove: function (p) { return { operation: 'DeleteItem', key: __ddbMap(p.key), condition: __cond(p.condition) }; },
    update: function (p) { return { operation: 'UpdateItem', key: __ddbMap(p.key), update: __update(p.update), condition: __cond(p.condition) }; },
    query: function (p) {
      return { operation: 'Query', query: __cond(p.query), filter: __cond(p.filter), index: p.index, limit: p.limit, nextToken: p.nextToken,
        scanIndexForward: p.scanIndexForward, consistentRead: p.consistentRead, select: p.select, projection: __proj(p.projection) };
    },
    scan: function (p) {
      return { operation: 'Scan', filter: __cond(p.filter), index: p.index, limit: p.limit, nextToken: p.nextToken, consistentRead: p.consistentRead,
        segment: p.segment, totalSegments: p.totalSegments, select: p.select, projection: __proj(p.projection) };
    },
    operations: {
      add: function (v) { return { __op: 'set', v: v }; },
      replace: function (v) { return { __op: 'set', v: v }; },
      remove: function () { return { __op: 'remove' }; },
      increment: function (v) { return { __op: 'add', v: v }; },
      decrement: function (v) { return { __op: 'add', v: -v }; },
      append: function (v) { return { __op: 'append', v: v }; },
      prepend: function (v) { return { __op: 'prepend', v: v }; },
      updateListItem: function (v, i) { return { __op: 'item', v: v, i: i }; },
    },
  },
};
function __run(mod, fn) {
  var out = {}, ctx = JSON.parse(__ctx);
  ctx.args = ctx.arguments;
  __info = ctx.info || {};
  try {
    if (typeof mod[fn] !== 'function') throw new Error('The code does not export a ' + fn + ' function.');
    out.result = mod[fn](ctx);
  } catch (e) {
    if (e instanceof __Early) { out.early = true; out.result = e.data; }
    else if (e instanceof __Err) out.error = { message: e.message, errorType: e.type, data: e.data, errorInfo: e.info };
    else throw e;
  }
  out.stash = ctx.stash;
  out.errors = __errors;
  out.logs = __logs;
  return JSON.stringify(out);
}
`

var prelude = sync.OnceValue(func() *goja.Program { return goja.MustCompile("prelude.js", jsPrelude, false) })

var (
	importRe = regexp.MustCompile(`(?s)\bimport\s+(.+?)\s+from\s*['"]([^'"]+)['"]\s*;?`)
	asRe     = regexp.MustCompile(`(\w+)\s+as\s+(\w+)`)
	exportRe = regexp.MustCompile(`\bexport\s+(default\s+)?`)
)

// wrap turns APPSYNC_JS module code into a script whose value holds its
// request and response functions: imports read __mods, exports become plain
// declarations, and line numbers stay where they were.
func wrap(code string) string {
	code = importRe.ReplaceAllStringFunc(code, func(m string) string {
		sm := importRe.FindStringSubmatch(m)
		what, mod := strings.TrimSpace(sm[1]), "__mods["+strconv.Quote(sm[2])+"] || {}"
		decl := "const " + asRe.ReplaceAllString(what, "$1: $2") + " = " + mod + ";"
		if name, ok := strings.CutPrefix(what, "* as "); ok {
			decl = "const " + strings.TrimSpace(name) + " = " + mod + ";"
		}
		return decl + strings.Repeat("\n", strings.Count(m, "\n"))
	})
	return "(function () {" + exportRe.ReplaceAllString(code, "") +
		"\nreturn { request: typeof request === 'function' ? request : undefined, response: typeof response === 'function' ? response : undefined };\n})()"
}

func compileJS(code string) error {
	_, err := goja.Compile("code.js", wrap(code), false)
	return err
}

// runJS calls the request or response function of APPSYNC_JS code with rc
// as ctx; it returns the value, and whether runtime.earlyReturn gave it.
func runJS(code, fn string, rc *rctx) (any, bool, error) {
	vm := goja.New()
	timer := time.AfterFunc(10*time.Second, func() { vm.Interrupt("the code ran for more than 10 seconds") })
	defer timer.Stop()
	vm.Set("__autoId", core.UUID)
	vm.Set("__expr", func(f string) string {
		var m map[string]any
		json.Unmarshal([]byte(f), &m)
		return toJSON(condition(m))
	})
	vm.Set("__ctx", toJSON(rc.ctx()))
	if _, err := vm.RunProgram(prelude()); err != nil {
		return nil, false, err
	}
	mod, err := vm.RunString(wrap(code))
	if err != nil {
		return nil, false, &resolveErr{Message: err.Error(), Type: "Code"}
	}
	run, _ := goja.AssertFunction(vm.Get("__run"))
	out, err := run(goja.Undefined(), mod, vm.ToValue(fn))
	if err != nil {
		return nil, false, &resolveErr{Message: err.Error(), Type: "Code"}
	}
	var res struct {
		Result any            `json:"result"`
		Early  bool           `json:"early"`
		Error  *resolveErr    `json:"error"`
		Stash  map[string]any `json:"stash"`
		Errors []*resolveErr  `json:"errors"`
		Logs   []string       `json:"logs"`
	}
	json.Unmarshal([]byte(out.String()), &res)
	if res.Stash != nil {
		rc.stash = res.Stash
	}
	rc.errors, rc.logs = append(rc.errors, res.Errors...), append(rc.logs, res.Logs...)
	if res.Error != nil {
		return nil, false, res.Error
	}
	return res.Result, res.Early, nil
}

// evalContext reads the context of EvaluateCode and EvaluateMappingTemplate.
func evalContext(s string) (*rctx, error) {
	var in struct {
		Arguments map[string]any           `json:"arguments"`
		Source    any                      `json:"source"`
		Identity  any                      `json:"identity"`
		Info      map[string]any           `json:"info"`
		Stash     map[string]any           `json:"stash"`
		Result    any                      `json:"result"`
		Error     any                      `json:"error"`
		Prev      struct{ Result any }     `json:"prev"`
		Request   struct{ Headers header } `json:"request"`
	}
	if s != "" && json.Unmarshal([]byte(s), &in) != nil {
		return nil, badRequest("The context is not valid JSON.")
	}
	rc := &rctx{args: in.Arguments, source: in.Source, identity: in.Identity, info: in.Info, stash: in.Stash, result: in.Result,
		err: in.Error, prev: in.Prev.Result, headers: in.Request.Headers}
	if rc.args == nil {
		rc.args = map[string]any{}
	}
	if rc.stash == nil {
		rc.stash = map[string]any{}
	}
	return rc, nil
}

// evaluated is the output of EvaluateCode and EvaluateMappingTemplate.
func evaluated(rc *rctx, result string, err error) map[string]any {
	out := map[string]any{"stash": toJSON(rc.stash), "outErrors": "[]", "logs": []string{}}
	if rc.errors != nil {
		out["outErrors"] = toJSON(rc.errors)
	}
	if rc.logs != nil {
		out["logs"] = rc.logs
	}
	if err != nil {
		out["error"] = map[string]any{"message": err.Error()}
	} else {
		out["evaluationResult"] = result
	}
	return out
}

func (s *Service) evaluateCode(r *http.Request, c *core.Ctx) (any, error) {
	var in struct {
		Runtime  *runtime `json:"runtime"`
		Code     string   `json:"code"`
		Context  string   `json:"context"`
		Function string   `json:"function"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if !in.Runtime.js() {
		return nil, badRequest("The runtime must be APPSYNC_JS.")
	}
	rc, err := evalContext(in.Context)
	if err != nil {
		return nil, err
	}
	v, _, err := runJS(in.Code, cmp.Or(in.Function, "request"), rc)
	return evaluated(rc, toJSON(v), err), nil
}

func (s *Service) evaluateTemplate(r *http.Request, c *core.Ctx) (any, error) {
	var in struct {
		Template string `json:"template"`
		Context  string `json:"context"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	rc, err := evalContext(in.Context)
	if err != nil {
		return nil, err
	}
	text, ret, returned, err := renderVTL(in.Template, rc)
	if returned {
		text = toJSON(ret)
	}
	return evaluated(rc, text, err), nil
}
