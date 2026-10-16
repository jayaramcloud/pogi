package generic

import (
	"encoding/json"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/pricelist"
	"github.com/nrndev/pogi/internal/core"
)

// The cost model behind Cost Explorer, Budgets, the CUR and the data exports.
//
// There is no bill, so pogi makes one up, deterministically, from what it has
// seen and what is running, priced with the embedded price list
// (internal/aws/pricelist):
//
//   - Every successful AWS call in the request log is one billable request,
//     priced per request: S3 PUT, COPY, POST and LIST calls at the Tier1 rate and
//     GET, HEAD and the rest at Tier2 (deletes are free, as on AWS), SQS calls at
//     the standard-queue rate, Lambda Invoke at the request rate, DynamoDB
//     writes and reads at the on-demand request-unit rates (one unit per call).
//     Services the price list does not have cost nothing.
//   - Every EC2 instance that is running and every RDS instance that is
//     available costs its instance-hour price for each hour it has been up.
//
// A call is billed in the region and the hour it was made, an instance hour in
// the hour it was lived, so a day's cost is the sum of its hours and re-asking
// for the same period gives the same answer (until more happens in it). The
// request log keeps the last 1000 calls, so a busy instance forgets the oldest
// requests, and instance hours are counted from launch to now (stopped
// instances are not remembered). Storage and Lambda duration are not billed.
type costLine struct {
	At        time.Time // the hour
	Account   string
	Service   string // as Cost Explorer names it
	Code      string // price-list service code
	Region    string
	UsageType string
	Operation string
	Instance  string // instance type, for EC2 and RDS
	Qty       float64
	Cost      float64
}

type costLineKey struct {
	at                                           time.Time
	code, region, usageType, operation, instance string
}

type costLineSet struct {
	account string
	m       map[costLineKey]*costLine
}

func (ls *costLineSet) add(at time.Time, svc *pricelist.Service, p pricelist.Product, op, instance string, qty float64) {
	k := costLineKey{at, svc.Code, p.Region, p.UsageType, op, instance}
	l := ls.m[k]
	if l == nil {
		l = &costLine{At: at, Account: ls.account, Service: svc.Name, Code: svc.Code, Region: p.Region, UsageType: p.UsageType, Operation: op, Instance: instance}
		ls.m[k] = l
	}
	l.Qty += qty
	l.Cost += qty * p.Price
}

// requestUsage is the usage type and unit count one API call is billed as.
func costRequestUsage(service, op string) (code, usageType string, ok bool) {
	switch service {
	case "s3":
		switch {
		case strings.HasPrefix(op, "Delete"):
			return "", "", false
		case strings.HasPrefix(op, "Put"), strings.HasPrefix(op, "Post"), strings.HasPrefix(op, "Copy"), strings.HasPrefix(op, "List"),
			strings.HasPrefix(op, "Create"), strings.HasPrefix(op, "Upload"), strings.HasPrefix(op, "Complete"):
			return "AmazonS3", "Requests-Tier1", true
		}
		return "AmazonS3", "Requests-Tier2", true
	case "sqs":
		return "AWSQueueService", "Requests", true
	case "lambda":
		return "AWSLambda", "Request", op == "Invoke"
	case "dynamodb":
		switch op {
		case "PutItem", "UpdateItem", "DeleteItem", "BatchWriteItem", "TransactWriteItems":
			return "AmazonDynamoDB", "WriteRequestUnits", true
		case "GetItem", "Query", "Scan", "BatchGetItem", "TransactGetItems":
			return "AmazonDynamoDB", "ReadRequestUnits", true
		}
	}
	return "", "", false
}

// costLines is the made-up bill of an account for [from, to), by hour.
func costLines(h *core.Hub, account string, from, to time.Time) []*costLine {
	ls := &costLineSet{account: account, m: map[costLineKey]*costLine{}}
	for _, e := range h.Requests.Query(0, 0, "", "", false) {
		if e.Cloud != core.AWS || e.Account != account || e.Status >= 400 || e.Time.Before(from) || !e.Time.Before(to) {
			continue
		}
		code, ut, ok := costRequestUsage(e.Service, e.Op)
		if !ok {
			continue
		}
		svc := pricelist.Lookup(code)
		if p, ok := svc.Price(e.Region, ut, ""); ok {
			ls.add(e.Time.UTC().Truncate(time.Hour), svc, p, e.Op, "", 1)
		}
	}
	now := time.Now().UTC()
	end := costMinTime(to, now)
	ec2, rds := pricelist.Lookup("AmazonEC2"), pricelist.Lookup("AmazonRDS")
	for _, r := range costBrowse(h, "ec2", "Instances") { // Instance, State, Type, Image, Private IP, Region, Account
		if len(r.row) < 7 || r.row[1] != "running" || r.row[6] != account {
			continue
		}
		if p, ok := ec2.Price(r.row[5], "BoxUsage:"+r.row[2], ""); ok {
			ls.hours(ec2, p, "RunInstances", r.row[2], r.time("LaunchTime"), from, end)
		}
	}
	for _, r := range costBrowse(h, "rds", "DB instances") { // Identifier, Engine, Status, Endpoint, Class, Region, Account
		if len(r.row) < 7 || r.row[2] != "available" || r.row[6] != account {
			continue
		}
		op := "CreateDBInstance:0002"
		if strings.HasPrefix(r.row[1], "postgres") {
			op = "CreateDBInstance:0014"
		}
		if p, ok := rds.Price(r.row[5], "InstanceUsage:"+r.row[4], op); ok {
			ls.hours(rds, p, op, r.row[4], r.time("InstanceCreateTime"), from, end)
		}
	}
	out := make([]*costLine, 0, len(ls.m))
	for _, l := range ls.m {
		out = append(out, l)
	}
	slices.SortFunc(out, func(a, b *costLine) int {
		if c := a.At.Compare(b.At); c != 0 {
			return c
		}
		return strings.Compare(a.Code+a.Region+a.UsageType+a.Operation, b.Code+b.Region+b.UsageType+b.Operation)
	})
	return out
}

// hours adds the hours an instance that came up at launch was up within [from, end).
func (ls *costLineSet) hours(svc *pricelist.Service, p pricelist.Product, op, instance string, launch, from, end time.Time) {
	start := costMaxTime(launch, from)
	for h := start.Truncate(time.Hour); h.Before(end); h = h.Add(time.Hour) {
		if d := costMinTime(h.Add(time.Hour), end).Sub(costMaxTime(h, start)); d > 0 {
			ls.add(h, svc, p, op, instance, d.Hours())
		}
	}
}

// costListed is one row of a service's console table, and a way to its details.
type costListed struct {
	row []string
	h   *core.Hub
	svc string
	tbl string
}

// browse lists the rows of a console table of another service through the hub.
func costBrowse(h *core.Hub, svc, table string) []costListed {
	b, ok := h.Service(svc).(core.Browsable)
	if !ok {
		return nil
	}
	var out []costListed
	for _, t := range b.Resources() {
		if t.Name == table {
			for _, r := range t.Rows {
				out = append(out, costListed{row: r, h: h, svc: svc, tbl: table})
			}
		}
	}
	return out
}

// time reads a timestamp of the row's details (the Inspect view), now when it has none.
func (l costListed) time(key string) time.Time {
	if in, ok := l.h.Service(l.svc).(core.Inspectable); ok && len(l.row) > 0 {
		b, _ := json.Marshal(in.Inspect(l.tbl, l.row[0]))
		var v any
		json.Unmarshal(b, &v)
		if s := costFindString(v, key); s != "" {
			for _, layout := range []string{time.RFC3339Nano, core.ISO, "2006-01-02T15:04:05.000Z"} {
				if t, err := time.Parse(layout, s); err == nil {
					return t.UTC()
				}
			}
		}
	}
	return time.Now().UTC()
}

func costFindString(v any, key string) string {
	switch x := v.(type) {
	case map[string]any:
		if s, ok := x[key].(string); ok {
			return s
		}
		for _, e := range x {
			if s := costFindString(e, key); s != "" {
				return s
			}
		}
	case []any:
		for _, e := range x {
			if s := costFindString(e, key); s != "" {
				return s
			}
		}
	}
	return ""
}

func costMinTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}

func costMaxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}

// ---- dimensions and filters

// dim is the value of a Cost Explorer dimension of a line.
func (l *costLine) dim(key string) string {
	switch key {
	case "SERVICE":
		return l.Service
	case "REGION":
		return l.Region
	case "USAGE_TYPE":
		return l.UsageType
	case "OPERATION":
		return l.Operation
	case "INSTANCE_TYPE":
		return l.Instance
	case "LINKED_ACCOUNT", "PAYER_ACCOUNT":
		return l.Account
	}
	return ""
}

// budgetFilterKeys maps the keys of a budget's CostFilters to dimensions.
var budgetFilterKeys = map[string]string{"Service": "SERVICE", "Region": "REGION", "UsageType": "USAGE_TYPE", "Operation": "OPERATION",
	"InstanceType": "INSTANCE_TYPE", "LinkedAccount": "LINKED_ACCOUNT"}

// matches applies an Expression (And, Or, Not, Dimensions; tags and cost
// categories match everything, as nothing here is tagged) to a line.
func costMatches(e map[string]any, l *costLine) bool {
	if e == nil {
		return true
	}
	if and, ok := e["And"].([]any); ok {
		for _, x := range and {
			if m, _ := x.(map[string]any); !costMatches(m, l) {
				return false
			}
		}
	}
	if or, ok := e["Or"].([]any); ok && len(or) > 0 {
		hit := false
		for _, x := range or {
			m, _ := x.(map[string]any)
			hit = hit || costMatches(m, l)
		}
		if !hit {
			return false
		}
	}
	if not, ok := e["Not"].(map[string]any); ok && costMatches(not, l) {
		return false
	}
	if d, ok := e["Dimensions"].(map[string]any); ok {
		vals, _ := d["Values"].([]any)
		if len(vals) == 0 {
			return true
		}
		have, opt := l.dim(str(d["Key"])), "EQUALS"
		if mo, _ := d["MatchOptions"].([]any); len(mo) > 0 {
			opt = str(mo[0])
		}
		hit := false
		for _, v := range vals {
			w := str(v)
			switch opt {
			case "STARTS_WITH":
				hit = hit || strings.HasPrefix(have, w)
			case "ENDS_WITH":
				hit = hit || strings.HasSuffix(have, w)
			case "CONTAINS":
				hit = hit || strings.Contains(have, w)
			case "ABSENT":
				hit = hit || have == ""
			default:
				hit = hit || have == w
			}
		}
		return hit
	}
	return true
}

// ---- time

var costDayFormats = []string{"2006-01-02", time.RFC3339, "2006-01-02T15:04:05Z", "2006-01-02T15:04:05"}

// parseTime reads a Cost Explorer or Budgets time: a date, or a date and hour.
func costParseTime(v any) (time.Time, bool) {
	switch x := v.(type) {
	case string:
		for _, f := range costDayFormats {
			if t, err := time.Parse(f, x); err == nil {
				return t.UTC(), true
			}
		}
		if s, ok := normTime(x); ok {
			t, err := time.Parse(time.RFC3339Nano, s)
			return t.UTC(), err == nil
		}
	case float64:
		return time.UnixMilli(int64(x * 1000)).UTC(), true
	}
	return time.Time{}, false
}

func costDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func costMonthStart(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
}

// periodStart is the first instant of the period of a granularity or time unit that holds t.
func costPeriodStart(t time.Time, unit string) time.Time {
	switch unit {
	case "HOURLY":
		return t.Truncate(time.Hour)
	case "MONTHLY":
		return costMonthStart(t)
	case "QUARTERLY":
		m := costMonthStart(t)
		return m.AddDate(0, -int(m.Month()-1)%3, 0)
	case "ANNUALLY":
		return time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
	}
	return costDay(t)
}

func costNextPeriod(t time.Time, unit string) time.Time {
	switch unit {
	case "HOURLY":
		return t.Add(time.Hour)
	case "MONTHLY":
		return t.AddDate(0, 1, 0)
	case "QUARTERLY":
		return t.AddDate(0, 3, 0)
	case "ANNUALLY":
		return t.AddDate(1, 0, 0)
	}
	return t.AddDate(0, 0, 1)
}

// ---- numbers

// money renders an amount the way Cost Explorer does.
func costMoney(f float64) string { return strconv.FormatFloat(math.Round(f*1e10)/1e10, 'f', -1, 64) }

func costSumCost(lines []*costLine, q bool) (s float64) {
	for _, l := range lines {
		if q {
			s += l.Qty
		} else {
			s += l.Cost
		}
	}
	return
}
