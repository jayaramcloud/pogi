package web

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/core"
)

// What a service can create from the console, by where its API comes from:
//   - AWS services on the generic engine: forms from the botocore model's create and update inputs;
//   - the other AWS services: a curated set of 15 common creates (their handlers take unexported input
//     types, so there is nothing to reflect on), sent as JSON through hub.Call;
//   - Google Cloud services: forms from the protobuf request of every Create RPC (forms_gcp.go);
//   - Azure: a JSON editor for any ARM resource (forms_arm.go).

// formOp is one entry of a service's "New" menu.
type formOp struct{ Op, Title string }

func (ui *UI) formOps(svc string) []formOp {
	var out []formOp
	switch s := ui.hub.Service(svc).(type) {
	case nil:
	case *generic.Service:
		for _, op := range s.FormOps() {
			out = append(out, formOp{op, core.Sentence(op)})
		}
	default:
		for _, c := range curated[svc] {
			out = append(out, formOp{c.Op, c.Title})
		}
		if len(out) == 0 {
			out = ui.gcpOps(svc)
		}
	}
	for _, sp := range specials[svc] { // a special replaces the generated form of the same title
		out = slices.DeleteFunc(out, func(o formOp) bool { return o.Title == sp.Title })
		out = append(out, formOp{sp.Op, sp.Title})
	}
	return out
}

func (ui *UI) form(svc, op string, sc scope) *core.Form {
	if sp := ui.special(svc, op); sp != nil && ui.hub.Service(svc) != nil {
		fs := slices.Clone(sp.Fields(ui)) // fixed() shares its slice
		for i := range fs {               // {account}, {location}, {group} and {azlocation} in a default are the scope's
			fs[i].Default = strings.NewReplacer("{account}", sc.acct(), "{location}", sc.location(), "{group}", sc.group(), "{azlocation}", sc.azLocation()).Replace(fs[i].Default)
		}
		return &core.Form{Op: sp.Op, Title: sp.Title, Fields: fs}
	}
	switch s := ui.hub.Service(svc).(type) {
	case nil:
		return nil
	case *generic.Service:
		return s.Form(op)
	}
	if c := curatedOp(svc, op); c != nil {
		return &core.Form{Op: c.Op, Title: c.Title, Fields: c.Fields}
	}
	return ui.gcpForm(svc, op, sc)
}

// submit sends a filled form to the service as the console's own identity: through the hub, as the
// account and region of the switcher.
func (ui *UI) submit(svc, op string, sc scope, in map[string]any) error {
	acct, region := sc.acct(), sc.region(ui.hub.Cfg.Region)
	if sp := ui.special(svc, op); sp != nil {
		return sp.Submit(ui, sc, in)
	}
	if g, ok := ui.hub.Service(svc).(*generic.Service); ok {
		b, _ := json.Marshal(in) // as a request carries it: numbers are float64
		var j map[string]any
		json.Unmarshal(b, &j)
		return ui.write(svc, op, "", func() error {
			_, err := g.Do(acct, region, op, j) // the engine's in-process call: not every generic service speaks JSON
			return err
		})
	}
	if c := curatedOp(svc, op); c != nil {
		return ui.hub.ConsoleCall(acct, region, c.Target, op, in, nil)
	}
	return ui.gcpSubmit(svc, op, in)
}

// tagOps is how each service lists a resource's tags: the table it covers, and a call that returns
// them as the API answers (a map, or a list of key and value structures; S3 answers XML).
var tagOps = map[string]struct {
	table string
	get   func(ui *UI, acct, region, name string) (any, error)
}{
	"sqs": {"Queues", func(ui *UI, a, r, n string) (any, error) {
		return ui.jsonCall(a, r, "AmazonSQS", "ListQueueTags", map[string]any{"QueueUrl": n})
	}},
	"sns": {"Topics", func(ui *UI, a, r, n string) (any, error) {
		return ui.jsonCall(a, r, "AmazonSimpleNotificationService", "ListTagsForResource", map[string]any{"ResourceArn": core.ARN("sns", r, a, n)})
	}},
	"dynamodb": {"Tables", func(ui *UI, a, r, n string) (any, error) {
		return ui.jsonCall(a, r, "DynamoDB_20120810", "ListTagsOfResource", map[string]any{"ResourceArn": core.ARN("dynamodb", r, a, "table/"+n)})
	}},
	"lambda": {"Functions", func(ui *UI, a, r, n string) (any, error) {
		req, _ := http.NewRequest(http.MethodGet, "http://localhost/2017-03-31/tags/"+url.PathEscape(core.ARN("lambda", r, a, "function:"+n)), nil)
		status, b := ui.hub.DoServiceAs("lambda", a, r, req)
		var out any
		if status >= 300 || json.Unmarshal(b, &out) != nil {
			return nil, fmt.Errorf("%d", status)
		}
		return out, nil
	}},
	"s3": {"Buckets", func(ui *UI, a, r, n string) (any, error) {
		req, _ := http.NewRequest(http.MethodGet, "http://localhost/"+url.PathEscape(n)+"?tagging", nil)
		status, b := ui.hub.DoServiceAs("s3", a, r, req)
		var x struct {
			Tag []struct{ Key, Value string } `xml:"TagSet>Tag"`
		}
		if status >= 300 || xml.Unmarshal(b, &x) != nil { // NoSuchTagSet: none
			return nil, nil
		}
		tags := map[string]any{}
		for _, t := range x.Tag {
			tags[t.Key] = t.Value
		}
		return map[string]any{"Tags": tags}, nil
	}},
}

func (ui *UI) jsonCall(acct, region, target, op string, in map[string]any) (any, error) {
	var out any
	return out, ui.hub.Call(acct, region, target, op, in, &out)
}

// resourceTags are the tags of one resource row of a table, and whether its service lists tags at all.
func (ui *UI) resourceTags(svc, table, id string) (map[string]string, bool) {
	op, ok := tagOps[svc]
	b, _ := ui.hub.Service(svc).(core.Browsable)
	if !ok || b == nil || op.table != table {
		return nil, false
	}
	for _, t := range b.Resources() {
		if t.Name != table {
			continue
		}
		for _, row := range t.Rows {
			if len(row) == 0 || row[0] != id {
				continue
			}
			var acct, region string
			for i, c := range t.Columns {
				switch c {
				case "Account":
					acct = row[i]
				case "Region":
					region = row[i]
				}
			}
			raw, _ := op.get(ui, acct, region, id)
			tags := map[string]string{}
			if m, _ := raw.(map[string]any); m != nil {
				switch v := m["Tags"].(type) {
				case map[string]any:
					for k, x := range v {
						tags[k] = fmt.Sprint(x)
					}
				case []any:
					for _, e := range v {
						if kv, _ := e.(map[string]any); kv != nil {
							tags[fmt.Sprint(kv["Key"])] = fmt.Sprint(kv["Value"])
						}
					}
				}
			}
			return tags, true
		}
	}
	return nil, false
}
