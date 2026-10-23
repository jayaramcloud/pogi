package generic

import (
	"encoding/json"
	"math"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// X-Ray. PutTraceSegments checks each segment document and keeps good ones as
// one "Trace" resource per trace ID (so snapshots and reset include them);
// GetTraceSummaries and BatchGetTraces read them back. Sampling rules are
// "SamplingRule" resources, plus the built-in Default rule that real X-Ray
// always has. Not done: the UDP daemon, service graphs, insights and filter
// expressions.
const (
	xrayMaxDoc       = 64 * 1024
	xrayRetention    = 30 * 24 * time.Hour // X-Ray keeps traces 30 days
	xrayMaxTraces    = 1000                // ponytail: fixed cap, oldest traces go first; make it a flag if tests need more
	xrayMaxSegments  = 500                 // per trace, so one trace cannot grow without bound
	xrayPageSize     = 100
	xrayTargetTTL    = time.Hour // how long a reservoir quota in a sampling target is good for
	xrayTargetPeriod = 10        // seconds between a client's target requests
)

var (
	xrayMu      sync.Mutex // PutTraceSegments reads then rewrites traces
	xrayHexID   = regexp.MustCompile(`^[0-9a-f]{16}$`)
	xrayTraceID = regexp.MustCompile(`^1-[0-9a-f]{8}-[0-9a-f]{24}$`)
)

func init() {
	Override("xray", "PutTraceSegments", xrayPut)
	Override("xray", "GetTraceSummaries", xraySummaries)
	Override("xray", "BatchGetTraces", xrayBatchGet)
	Override("xray", "CreateSamplingRule", xrayCreateRule)
	Override("xray", "UpdateSamplingRule", xrayUpdateRule)
	Override("xray", "DeleteSamplingRule", xrayDeleteRule)
	Override("xray", "GetSamplingRules", xrayRules)
	Override("xray", "GetSamplingTargets", xrayTargets)
	Override("xray", "PutEncryptionConfig", xrayPutEncryption)
	Override("xray", "GetEncryptionConfig", xrayGetEncryption)
}

func xrayBad(id, msg string) map[string]any {
	return map[string]any{"Id": id, "ErrorCode": "ParseError", "Message": msg}
}

// xrayCheck returns the parsed segment, or what is wrong with it.
func xrayCheck(doc string) (seg map[string]any, id, problem string) {
	if len(doc) > xrayMaxDoc {
		return nil, "", "segment document is larger than 64 KB"
	}
	if err := json.Unmarshal([]byte(doc), &seg); err != nil || seg == nil {
		return nil, "", "segment document is not a JSON object"
	}
	id = str(seg["id"])
	_, hasEnd := seg["end_time"].(float64)
	_, hasStart := seg["start_time"].(float64)
	switch {
	case !xrayHexID.MatchString(id):
		return nil, id, "id must be 16 hexadecimal digits"
	case !xrayTraceID.MatchString(str(seg["trace_id"])):
		return nil, id, "trace_id must look like 1-<8 hex digits>-<24 hex digits>"
	case str(seg["name"]) == "":
		return nil, id, "name is required"
	case !hasStart:
		return nil, id, "start_time must be a number"
	case !hasEnd && seg["in_progress"] != true:
		return nil, id, "end_time must be a number, or in_progress must be true"
	}
	return seg, id, ""
}

func xrayPut(c *Call) (map[string]any, error) {
	xrayMu.Lock()
	defer xrayMu.Unlock()
	now := c.now()
	bad := []any{}
	traces := map[string]*Resource{}
	for _, d := range asList(c.In["TraceSegmentDocuments"]) {
		doc := str(d)
		seg, id, problem := xrayCheck(doc)
		if problem != "" {
			bad = append(bad, xrayBad(id, problem))
			continue
		}
		tid := str(seg["trace_id"])
		r := traces[tid]
		if r == nil {
			if r = c.Get("Trace", tid); r == nil {
				r = &Resource{Type: "Trace", Key: tid, Name: tid, Account: c.Account, Region: c.key(), Data: map[string]any{}, Created: now}
			}
			traces[tid] = r
		}
		segs := asList(r.Data["Segments"])
		entry := map[string]any{"Id": id, "Document": doc}
		if i := slices.IndexFunc(segs, func(s any) bool { return str(asMap(s)["Id"]) == id }); i >= 0 {
			segs[i] = entry // the same segment sent again (now finished) replaces the old one
		} else if len(segs) >= xrayMaxSegments {
			bad = append(bad, map[string]any{"Id": id, "ErrorCode": "LimitExceeded", "Message": "trace already has the most segments pogi keeps"})
			continue
		} else {
			segs = append(segs, entry)
		}
		r.Data["Segments"] = segs
	}
	for _, r := range traces {
		r.Updated = now
		c.Put(r)
	}
	xrayPrune(c, now)
	return map[string]any{"UnprocessedTraceSegments": bad}, nil
}

// xrayPrune drops traces past the retention time, then the oldest ones past the cap.
func xrayPrune(c *Call, now time.Time) {
	var keep []*Resource
	for _, r := range c.List("Trace") { // oldest first
		if now.Sub(r.Updated) > xrayRetention {
			c.Svc.Delete(c.Account, c.Region, "Trace", r.Key)
		} else {
			keep = append(keep, r)
		}
	}
	for len(keep) > xrayMaxTraces {
		c.Svc.Delete(c.Account, c.Region, "Trace", keep[0].Key)
		keep = keep[1:]
	}
}

// xrayTrace is a trace worked out from its stored segments.
type xrayTrace struct {
	id                   string
	start, end, response float64
	err, fault, throttle bool
	partial              bool
	http                 map[string]any
	segments             []any
	idTime               time.Time
	updated              time.Time
}

func xrayLoad(r *Resource) *xrayTrace {
	t := &xrayTrace{id: r.Key, start: math.Inf(1), end: math.Inf(-1), segments: asList(r.Data["Segments"]), updated: r.Updated}
	if len(r.Key) >= 10 {
		if n, err := strconv.ParseInt(r.Key[2:10], 16, 64); err == nil {
			t.idTime = time.Unix(n, 0).UTC()
		}
	}
	var root map[string]any
	var flags func(m map[string]any)
	flags = func(m map[string]any) {
		t.err = t.err || m["error"] == true
		t.fault = t.fault || m["fault"] == true
		t.throttle = t.throttle || m["throttle"] == true
		for _, s := range asList(m["subsegments"]) {
			flags(asMap(s))
		}
	}
	for _, s := range t.segments {
		var m map[string]any
		if json.Unmarshal([]byte(str(asMap(s)["Document"])), &m) != nil {
			continue
		}
		st, _ := m["start_time"].(float64)
		en, ok := m["end_time"].(float64)
		if !ok {
			en = st
			t.partial = true
		}
		t.start, t.end = math.Min(t.start, st), math.Max(t.end, en)
		flags(m)
		if m["parent_id"] == nil && m["type"] != "subsegment" {
			if rs, _ := root["start_time"].(float64); root == nil || st < rs {
				root = m
				t.response = en - st
			}
		}
	}
	if root == nil || math.IsInf(t.start, 1) {
		t.response = t.end - t.start
	}
	if h := asMap(root["http"]); h != nil {
		req, resp := asMap(h["request"]), asMap(h["response"])
		t.http = map[string]any{}
		for k, v := range map[string]any{"HttpURL": req["url"], "HttpMethod": req["method"], "UserAgent": req["user_agent"], "ClientIp": req["client_ip"], "HttpStatus": resp["status"]} {
			if v != nil {
				t.http[k] = v
			}
		}
	}
	return t
}

func xraySummaries(c *Call) (map[string]any, error) {
	start, e1 := time.Parse(time.RFC3339Nano, str(c.In["StartTime"]))
	end, e2 := time.Parse(time.RFC3339Nano, str(c.In["EndTime"]))
	if e1 != nil || e2 != nil {
		return nil, c.Invalid("StartTime and EndTime are required timestamps")
	}
	if start.After(end) {
		return nil, c.Invalid("StartTime must not be after EndTime")
	}
	if str(c.In["FilterExpression"]) != "" {
		return nil, c.Err("InvalidRequestException", "pogi does not evaluate filter expressions yet; remove FilterExpression to list every trace in the time range")
	}
	var found []*xrayTrace
	for _, r := range c.List("Trace") {
		t := xrayLoad(r)
		var at time.Time // TraceId (the default): when the trace began; Event: when it last changed; Service: when it ended
		switch str(c.In["TimeRangeType"]) {
		case "Event":
			at = t.updated
		case "Service":
			at = time.Unix(0, int64(t.end*1e9))
		default:
			at = t.idTime
		}
		if !at.Before(start) && !at.After(end) {
			found = append(found, t)
		}
	}
	slices.SortFunc(found, func(a, b *xrayTrace) int {
		if x := a.idTime.Compare(b.idTime); x != 0 {
			return x
		}
		return strings.Compare(a.id, b.id)
	})
	off := 0
	if tok := str(c.In["NextToken"]); tok != "" {
		var err error
		if off, err = strconv.Atoi(tok); err != nil || off < 0 {
			return nil, c.Invalid("Invalid NextToken: %s", tok)
		}
	}
	off = min(off, len(found))
	next := ""
	if off+xrayPageSize < len(found) {
		next = strconv.Itoa(off + xrayPageSize)
		found = found[:off+xrayPageSize]
	}
	sums := []any{}
	for _, t := range found[off:] {
		s := map[string]any{"Id": t.id, "Duration": t.end - t.start, "ResponseTime": t.response, "HasError": t.err, "HasFault": t.fault,
			"HasThrottle": t.throttle, "IsPartial": t.partial}
		if t.http != nil {
			s["Http"] = t.http
		}
		sums = append(sums, s)
	}
	out := map[string]any{"TraceSummaries": sums, "TracesProcessedCount": float64(len(sums)), "ApproximateTime": c.now().Format(time.RFC3339Nano)}
	if next != "" {
		out["NextToken"] = next
	}
	return out, nil
}

func xrayBatchGet(c *Call) (map[string]any, error) {
	ids := asList(c.In["TraceIds"])
	if len(ids) == 0 || len(ids) > 5 {
		return nil, c.Invalid("TraceIds must have between 1 and 5 trace IDs")
	}
	traces, missing := []any{}, []any{}
	for _, id := range ids {
		r := c.Get("Trace", str(id))
		if r == nil {
			missing = append(missing, str(id))
			continue
		}
		t := xrayLoad(r)
		traces = append(traces, map[string]any{"Id": t.id, "Duration": t.end - t.start, "Segments": t.segments})
	}
	return map[string]any{"Traces": traces, "UnprocessedTraceIds": missing}, nil
}

// ---- sampling

// xrayDefault is the Default rule real X-Ray always has.
func xrayDefault(c *Call) *Resource {
	if r := c.Get("SamplingRule", "Default"); r != nil {
		return r
	}
	now := time.Unix(0, 0).UTC() // ponytail: the built-in rule has no real creation time
	return &Resource{Type: "SamplingRule", Key: "Default", Name: "Default", ARN: c.arnOf("sampling-rule/Default", nil), Account: c.Account, Region: c.key(), Created: now, Updated: now,
		Data: map[string]any{"RuleName": "Default", "Priority": 10000.0, "FixedRate": 0.05, "ReservoirSize": 1.0, "ServiceName": "*", "ServiceType": "*",
			"Host": "*", "HTTPMethod": "*", "URLPath": "*", "ResourceARN": "*", "Version": 1.0}}
}

func xrayRecord(r *Resource) map[string]any {
	rule := map[string]any{}
	for k, v := range r.Data {
		rule[k] = v
	}
	rule["RuleName"], rule["RuleARN"] = r.Name, r.ARN
	return map[string]any{"SamplingRule": rule, "CreatedAt": r.Created.Format(time.RFC3339Nano), "ModifiedAt": r.Updated.Format(time.RFC3339Nano)}
}

// xrayRule finds a rule by RuleName or RuleARN (the Default rule always exists).
func xrayRule(c *Call, name, arn string) *Resource {
	if arn != "" {
		for _, r := range append([]*Resource{xrayDefault(c)}, c.List("SamplingRule")...) {
			if r.ARN == arn {
				return r
			}
		}
		return nil
	}
	if name == "Default" {
		return xrayDefault(c)
	}
	return c.Get("SamplingRule", name)
}

func xrayCheckRule(c *Call, m map[string]any) error {
	if f, ok := number(m["FixedRate"]); ok && (f < 0 || f > 1) {
		return c.Invalid("FixedRate must be between 0 and 1")
	}
	if p, ok := number(m["Priority"]); ok && (p < 1 || p > 9999) {
		return c.Invalid("Priority must be between 1 and 9999")
	}
	if n, ok := number(m["ReservoirSize"]); ok && n < 0 {
		return c.Invalid("ReservoirSize must not be negative")
	}
	return nil
}

func xrayCreateRule(c *Call) (map[string]any, error) {
	m := asMap(c.In["SamplingRule"])
	name := str(m["RuleName"])
	if name == "" {
		return nil, c.Invalid("SamplingRule.RuleName is required")
	}
	if err := xrayCheckRule(c, m); err != nil {
		return nil, err
	}
	if xrayRule(c, name, "") != nil {
		return nil, c.Err("InvalidRequestException", "Sampling rule %s already exists", name)
	}
	now := c.now()
	r := &Resource{Type: "SamplingRule", Key: name, Name: name, Account: c.Account, Region: c.key(), Data: m, Created: now, Updated: now}
	r.ARN = c.arnOf("sampling-rule/"+name, nil)
	_, add, _ := takeTags(c.op.in, c.In)
	applyTags(r, add, nil)
	c.Put(r)
	return map[string]any{"SamplingRuleRecord": xrayRecord(r)}, nil
}

func xrayUpdateRule(c *Call) (map[string]any, error) {
	u := asMap(c.In["SamplingRuleUpdate"])
	r := xrayRule(c, str(u["RuleName"]), str(u["RuleARN"]))
	if r == nil {
		return nil, c.Err("InvalidRequestException", "Sampling rule %s not found", str(u["RuleName"])+str(u["RuleARN"]))
	}
	if err := xrayCheckRule(c, u); err != nil {
		return nil, err
	}
	for k, v := range u {
		if k != "RuleName" && k != "RuleARN" {
			r.Data[k] = v
		}
	}
	r.Updated = c.now()
	c.Put(r)
	return map[string]any{"SamplingRuleRecord": xrayRecord(r)}, nil
}

func xrayDeleteRule(c *Call) (map[string]any, error) {
	r := xrayRule(c, str(c.In["RuleName"]), str(c.In["RuleARN"]))
	if r == nil {
		return nil, c.Err("InvalidRequestException", "Sampling rule %s not found", str(c.In["RuleName"])+str(c.In["RuleARN"]))
	}
	if r.Name == "Default" {
		return nil, c.Err("InvalidRequestException", "Cannot delete the Default sampling rule")
	}
	c.Svc.Delete(c.Account, c.Region, "SamplingRule", r.Key)
	return map[string]any{"SamplingRuleRecord": xrayRecord(r)}, nil
}

func xrayRules(c *Call) (map[string]any, error) {
	rules := []*Resource{xrayDefault(c)}
	for _, r := range c.List("SamplingRule") {
		if r.Name != "Default" {
			rules = append(rules, r)
		}
	}
	recs := make([]any, len(rules))
	for i, r := range rules {
		recs[i] = xrayRecord(r)
	}
	page, next, err := pageOf(c, recs, "NextToken", "Limit")
	if err != nil {
		return nil, err
	}
	out := map[string]any{"SamplingRuleRecords": page}
	if next != "" {
		out["NextToken"] = next
	}
	return out, nil
}

func xrayTargets(c *Call) (map[string]any, error) {
	docs, unproc := []any{}, []any{}
	var last time.Time
	for _, d := range asList(c.In["SamplingStatisticsDocuments"]) {
		name := str(asMap(d)["RuleName"])
		r := xrayRule(c, name, "")
		if r == nil {
			unproc = append(unproc, map[string]any{"RuleName": name, "ErrorCode": "400", "Message": "Sampling rule not found"})
			continue
		}
		rate, _ := number(r.Data["FixedRate"])
		size, _ := number(r.Data["ReservoirSize"])
		docs = append(docs, map[string]any{"RuleName": name, "FixedRate": rate, "ReservoirQuota": size,
			"ReservoirQuotaTTL": c.now().Add(xrayTargetTTL).Format(time.RFC3339Nano), "Interval": float64(xrayTargetPeriod)})
		if r.Updated.After(last) {
			last = r.Updated
		}
	}
	for _, r := range c.List("SamplingRule") {
		if r.Updated.After(last) {
			last = r.Updated
		}
	}
	return map[string]any{"SamplingTargetDocuments": docs, "UnprocessedStatistics": unproc, "LastRuleModification": last.Format(time.RFC3339Nano)}, nil
}

// ---- encryption: one setting per account and region; real X-Ray starts with NONE (its own keys).

func xrayGetEncryption(c *Call) (map[string]any, error) {
	cfg := map[string]any{"Type": "NONE", "Status": "ACTIVE"}
	if r := c.Get("EncryptionConfig", "config"); r != nil {
		cfg = r.Data
	}
	return map[string]any{"EncryptionConfig": cfg}, nil
}

func xrayPutEncryption(c *Call) (map[string]any, error) {
	typ := str(c.In["Type"])
	if typ == "KMS" && str(c.In["KeyId"]) == "" {
		return nil, c.Invalid("KeyId is required when Type is KMS")
	}
	cfg := map[string]any{"Type": typ, "Status": "ACTIVE"}
	if typ == "KMS" {
		cfg["KeyId"] = str(c.In["KeyId"])
	}
	now := c.now()
	c.Put(&Resource{Type: "EncryptionConfig", Key: "config", Account: c.Account, Region: c.key(), Data: cfg, Created: now, Updated: now})
	return map[string]any{"EncryptionConfig": cfg}, nil
}
