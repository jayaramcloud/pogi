package web

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/core"
)

// Bulk actions: tick rows of a resources table, then delete them or tag them. Each row goes through
// the service's own delete or tag call (the generic engine's, the AWS API through the hub, a Google
// Cloud Delete RPC's REST binding, ARM), as the console's identity.

// tbl is a table of the resources page with what can be done to its rows.
type tbl struct {
	core.Table
	Del, Tag bool
	Enc      []string // each row, encoded for its checkbox
}

func encRow(row []string) string {
	b, _ := json.Marshal(row)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decRow(s string) ([]string, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	var row []string
	if err == nil {
		err = json.Unmarshal(b, &row)
	}
	if err != nil || len(row) == 0 {
		return nil, fmt.Errorf("a ticked row is garbled")
	}
	return row, nil
}

// bulkSpec is how the rows of a hand-written AWS service's table are deleted and tagged.
type bulkSpec struct {
	Del, Tag string
	// DelIn and TagIn build the call's input from the row, keyed by column name.
	DelIn func(ui *UI, c bulkCtx) (map[string]any, error)
	TagIn func(ui *UI, c bulkCtx, tags map[string]string) (map[string]any, error)
}

type bulkCtx struct {
	svc, account, region string
	row                  map[string]string
}

// id builds an input with one member from a column.
func id(member, col string) func(*UI, bulkCtx) (map[string]any, error) {
	return func(_ *UI, c bulkCtx) (map[string]any, error) { return map[string]any{member: c.row[col]}, nil }
}

// tagged adds the tags to what id builds, as a map or as a list of key and value structures.
func tagged(idIn func(*UI, bulkCtx) (map[string]any, error), member, kind string) func(*UI, bulkCtx, map[string]string) (map[string]any, error) {
	return func(ui *UI, c bulkCtx, tags map[string]string) (map[string]any, error) {
		in, err := idIn(ui, c)
		if err != nil {
			return nil, err
		}
		if kind == "map" {
			in[member] = tags
			return in, nil
		}
		kn, vn := "Key", "Value"
		if kind == "lower" {
			kn, vn = "key", "value"
		}
		var l []any
		for _, k := range core.SortedKeys(tags) {
			l = append(l, map[string]any{kn: k, vn: tags[k]})
		}
		in[member] = l
		return in, nil
	}
}

func arnIn(member, service, resource string) func(*UI, bulkCtx) (map[string]any, error) {
	return func(_ *UI, c bulkCtx) (map[string]any, error) {
		return map[string]any{member: core.ARN(service, c.region, c.account, resource+c.row["Name"])}, nil
	}
}

// queueIn finds the queue's URL, which is what SQS calls take.
func queueIn(ui *UI, c bulkCtx) (map[string]any, error) {
	var out struct{ QueueUrl string }
	if err := ui.hub.Call(c.account, c.region, "AmazonSQS", "GetQueueUrl", map[string]any{"QueueName": c.row["Queue"]}, &out); err != nil {
		return nil, err
	}
	return map[string]any{"QueueUrl": out.QueueUrl}, nil
}

var bulkSpecs = map[string]bulkSpec{
	"sqs/Queues":      {"DeleteQueue", "TagQueue", queueIn, tagged(queueIn, "Tags", "map")},
	"sns/Topics":      {"DeleteTopic", "TagResource", arnIn("TopicArn", "sns", ""), tagged(arnIn("ResourceArn", "sns", ""), "Tags", "list")},
	"dynamodb/Tables": {"DeleteTable", "TagResource", id("TableName", "Name"), tagged(arnIn("ResourceArn", "dynamodb", "table/"), "Tags", "list")},
	"kinesis/Streams": {"DeleteStream", "AddTagsToStream", id("StreamName", "Name"), tagged(id("StreamName", "Name"), "Tags", "map")},
	"logs/Log groups": {"DeleteLogGroup", "TagLogGroup", id("logGroupName", "Name"), tagged(id("logGroupName", "Name"), "tags", "map")},
	"ssm/Parameters": {"DeleteParameter", "AddTagsToResource", id("Name", "Name"), func(ui *UI, c bulkCtx, t map[string]string) (map[string]any, error) {
		in, _ := tagged(id("ResourceId", "Name"), "Tags", "list")(ui, c, t)
		in["ResourceType"] = "Parameter"
		return in, nil
	}},
	"secretsmanager/Secrets": {"DeleteSecret", "TagResource", func(_ *UI, c bulkCtx) (map[string]any, error) {
		return map[string]any{"SecretId": c.row["Name"], "ForceDeleteWithoutRecovery": true}, nil
	}, tagged(id("SecretId", "Name"), "Tags", "list")},
	"events/Buses": {Del: "DeleteEventBus", DelIn: id("Name", "Name")},
	"ecr/Repositories": {Del: "DeleteRepository", DelIn: func(_ *UI, c bulkCtx) (map[string]any, error) {
		return map[string]any{"repositoryName": c.row["Repository"], "force": true}, nil
	}},
	"ecs/Clusters": {Del: "DeleteCluster", DelIn: id("cluster", "Cluster")},
	"iam/Users":    {Del: "DeleteUser", DelIn: id("UserName", "Name")},
	"iam/Roles":    {Del: "DeleteRole", DelIn: id("RoleName", "Name")},
}

// bulkable says what the rows of a table support.
func (ui *UI) bulkable(svc, table string) (del, tag bool) {
	switch s := ui.hub.Service(svc).(type) {
	case nil:
	case *generic.Service:
		return true, true
	default:
		if sp, ok := bulkSpecs[svc+"/"+table]; ok {
			return true, sp.Tag != ""
		}
		if svc == "arm" {
			return table == "Resource groups" || table == "Resources", table == "Resource groups" || table == "Resources"
		}
		if s.Info().Cloud == core.GCP {
			return ui.gcpDelete(svc, table) != nil, false
		}
	}
	return false, false
}

// bulk runs the action on every ticked row.
func (ui *UI) bulk(w http.ResponseWriter, r *http.Request) {
	svc, table := r.PathValue("service"), r.PathValue("table")
	to := "/_pogi/resources/" + url.PathEscape(svc)
	if b := r.FormValue("back"); strings.HasPrefix(b, to) { // the tab the rows were ticked on
		to = b
	}
	var cols []string
	if b, ok := ui.hub.Service(svc).(core.Browsable); ok {
		for _, t := range b.Resources() {
			if t.Name == table {
				cols = t.Columns
			}
		}
	}
	del, tag := ui.bulkable(svc, table)
	action := r.FormValue("action")
	tags := parseTags(r.FormValue("tags"))
	var errs []string
	switch {
	case action == "delete" && !del, action == "tag" && !tag:
		errs = append(errs, fmt.Sprintf("%s %s cannot be %sd in bulk", svc, table, action))
	case action == "tag" && len(tags) == 0:
		errs = append(errs, "give the tags as key=value, key=value")
	case action != "delete" && action != "tag":
		errs = append(errs, "unknown action "+action)
	default:
		for _, enc := range r.Form["row"] {
			row, err := decRow(enc)
			if err == nil {
				err = ui.bulkOne(svc, table, action, ui.scope(r), cols, row, tags)
			}
			if err != nil {
				errs = append(errs, errText(err))
			}
		}
	}
	if len(errs) > 0 {
		back(w, r, to, fmt.Errorf("%s", strings.Join(errs, "; ")))
		return
	}
	msg := map[string]string{"delete": "Deleted", "tag": "Tagged"}[action] + " " + plural(len(r.Form["row"]), "resource")
	if rows := r.Form["row"]; len(rows) == 1 { // name it: "Queue jobs deleted"
		if row, err := decRow(rows[0]); err == nil && len(row) > 0 {
			msg = strings.ToUpper(singular(table)[:1]) + singular(table)[1:] + " " + row[0] + " " + map[string]string{"delete": "deleted", "tag": "tagged"}[action]
		}
	}
	done(w, r, to, msg, nil)
}

func parseTags(s string) map[string]string {
	out := map[string]string{}
	for _, kv := range strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == '\n' }) {
		if k, v, _ := strings.Cut(kv, "="); strings.TrimSpace(k) != "" {
			out[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return out
}

func (ui *UI) bulkOne(svc, table, action string, sc scope, cols, row []string, tags map[string]string) error {
	switch s := ui.hub.Service(svc).(type) {
	case *generic.Service:
		if action == "delete" {
			return s.Remove(table, row[0])
		}
		return s.Tag(table, row[0], tags)
	}
	rec := map[string]string{}
	for i, c := range cols {
		if i < len(row) {
			rec[c] = row[i]
		}
	}
	if svc == "arm" {
		return ui.armBulk(action, sc, table, row, tags)
	}
	if sp, ok := bulkSpecs[svc+"/"+table]; ok {
		c := bulkCtx{svc, orElse(rec["Account"], sc.acct()), orElse(rec["Region"], sc.region(ui.hub.Cfg.Region)), rec}
		op, build := sp.Del, sp.DelIn
		var in map[string]any
		var err error
		if action == "tag" {
			op = sp.Tag
			in, err = sp.TagIn(ui, c, tags)
		} else {
			in, err = build(ui, c)
		}
		if err != nil {
			return err
		}
		return ui.hub.ConsoleCall(c.account, c.region, curated[svc][0].Target, op, in, nil)
	}
	return ui.gcpBulkDelete(svc, table, row[0])
}

// armBulk deletes or tags the resource group or resource a row shows.
func (ui *UI) armBulk(action string, sc scope, table string, row []string, tags map[string]string) error {
	path := row[0]
	if table == "Resource groups" {
		path = "/subscriptions/" + orElse(row[2], sc.sub()) + "/resourceGroups/" + row[0]
	}
	if action == "delete" {
		if status, resp := ui.armDo(http.MethodDelete, path, nil); status >= 300 {
			return armError(status, resp)
		}
		return nil
	}
	status, resp := ui.armDo(http.MethodGet, path, nil)
	if status >= 300 {
		return armError(status, resp)
	}
	var cur struct{ Tags map[string]string }
	json.Unmarshal(resp, &cur)
	if cur.Tags == nil {
		cur.Tags = map[string]string{}
	}
	for k, v := range tags {
		cur.Tags[k] = v
	}
	b, _ := json.Marshal(map[string]any{"tags": cur.Tags})
	if status, resp := ui.armDo(http.MethodPatch, path, b); status >= 300 {
		return armError(status, resp)
	}
	return nil
}

// singular is a table's name for one row: "Queues" -> "queue". It is for toasts, so a rough guess will do.
func singular(table string) string {
	t := strings.ToLower(table)
	switch {
	case strings.HasSuffix(t, "ies"):
		return t[:len(t)-3] + "y"
	case strings.HasSuffix(t, "ses"), strings.HasSuffix(t, "xes"):
		return t[:len(t)-2]
	}
	return strings.TrimSuffix(t, "s")
}
