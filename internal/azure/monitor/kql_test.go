package monitor

import (
	"slices"
	"strings"
	"testing"
	"time"
)

var kqlNow = time.Date(2024, 5, 6, 12, 0, 0, 0, time.UTC)

func logsTable() *rel {
	at := func(h, m int) time.Time { return time.Date(2024, 5, 6, h, m, 0, 0, time.UTC) }
	return &rel{
		cols: []col{{"TimeGenerated", "datetime"}, {"Level", "string"}, {"Message", "string"}, {"Duration", "long"},
			{"Value", "real"}, {"Ok", "bool"}, {"Tags", "dynamic"}},
		rows: [][]any{
			{at(11, 0), "info", "service started", int64(120), 1.5, true, map[string]any{"env": "prod", "n": 1.0}},
			{at(11, 10), "error", "disk full on node-1", int64(900), 9.5, false, map[string]any{"env": "prod"}},
			{at(11, 12), "warn", "slow request", int64(450), 4.0, true, map[string]any{"env": "dev"}},
			{at(11, 30), "info", "request ok", int64(80), 0.5, true, map[string]any{"env": "prod"}},
			{at(11, 31), "error", "timeout calling Billing", int64(3000), 30.0, false, map[string]any{"env": "prod", "n": 2.0}},
			{at(11, 59), "info", "heartbeat", int64(10), 0.1, true, map[string]any{}},
			{at(9, 0), "debug", "old row", nil, nil, nil, nil},
		},
	}
}

func runKQL(src string) (*rel, error) {
	q, err := parseKQL(src)
	if err != nil {
		return nil, err
	}
	logs := logsTable()
	return q.run(&env{now: kqlNow, tables: func(n string) (*rel, bool) { return logs, n == "Logs" }})
}

func render(r *rel) []string {
	var out []string
	for _, row := range r.rows {
		cells := make([]string, len(row))
		for i, v := range row {
			cells[i] = tostr(v)
		}
		out = append(out, strings.Join(cells, "|"))
	}
	return out
}

func TestKQL(t *testing.T) {
	tests := []struct {
		q    string
		want []string
		cols string // "name:type,..." when set
	}{
		{`Logs | count`, []string{"7"}, "Count:long"},
		{`Logs | where Level == "error" | count`, []string{"2"}, ""},
		{`Logs | where Level != "info" and Duration > 400 | project Message`, []string{"disk full on node-1", "slow request", "timeout calling Billing"}, "Message:string"},
		{`Logs | where Level =~ "ERROR" | count`, []string{"2"}, ""},
		{`Logs | where Message contains "REQUEST" | project Message`, []string{"slow request", "request ok"}, ""},
		{`Logs | where Message startswith "disk" | count`, []string{"1"}, ""},
		{`Logs | where Message has "request" | count`, []string{"2"}, ""},
		{`Logs | where Message has "req" | count`, []string{"0"}, ""},
		{`Logs | where Message !contains "request" and Level == "info" | count`, []string{"2"}, ""},
		{`Logs | where Level in ("warn", "error") | count`, []string{"3"}, ""},
		{`Logs | where Level !in ("info", "debug") | count`, []string{"3"}, ""},
		{`Logs | where Duration between (100 .. 500) | project Duration | order by Duration asc`, []string{"120", "450"}, ""},
		{`Logs | where isnull(Duration) | project Message`, []string{"old row"}, ""},
		{`Logs | where isnotempty(Message) and not(Ok) | count`, []string{"2"}, ""},
		{`Logs | where Level == "error" or Level == "warn" and Duration > 500 | count`, []string{"2"}, ""},
		{`Logs | where TimeGenerated > ago(1h) | count`, []string{"5"}, ""},
		{`Logs | where TimeGenerated >= datetime(2024-05-06T11:30:00Z) | count`, []string{"3"}, ""},
		{`Logs | where TimeGenerated between (datetime(2024-05-06 11:00:00) .. datetime(2024-05-06 11:15:00)) | count`, []string{"3"}, ""},
		{`Logs | where Level == "error" | project Level, Duration | order by Duration desc`, []string{"error|3000", "error|900"}, "Level:string,Duration:long"},
		{`Logs | limit 3 | count`, []string{"3"}, ""},
		{`Logs | take 1 | project-rename Msg = Message | project Msg`, []string{"service started"}, "Msg:string"},
		{`Logs | top 2 by Duration | project Duration`, []string{"3000", "900"}, ""},
		{`Logs | top 1 by Duration asc | project Message`, []string{"old row"}, ""},
		{`Logs | order by Duration desc | take 1 | project Message`, []string{"timeout calling Billing"}, ""},
		{`Logs | where Duration > 100 | sort by Level asc, Duration desc | project Level, Duration`, []string{"error|3000", "error|900", "info|120", "warn|450"}, ""},
		{`Logs | distinct Level | order by Level asc`, []string{"debug", "error", "info", "warn"}, ""},
		{`Logs | summarize count() by Level | order by Level asc`, []string{"debug|1", "error|2", "info|3", "warn|1"}, "Level:string,count_:long"},
		{`Logs | summarize n = count(), total = sum(Duration), avg_d = avg(Duration), mx = max(Duration), mn = min(Duration) by Level | where Level == "error"`,
			[]string{"error|2|3900|1950|3000|900"}, "Level:string,n:long,total:long,avg_d:real,mx:long,mn:long"},
		{`Logs | summarize dcount(Level)`, []string{"4"}, "dcount_Level:long"},
		{`Logs | where Ok == true | summarize make_list(Level) by Ok`, []string{`true|["info","warn","info","info"]`}, "Ok:bool,make_list_Level:dynamic"},
		{`Logs | summarize make_set(Level) | project n = array_length(make_set_Level)`, []string{"4"}, ""},
		{`Logs | summarize count() by bin(TimeGenerated, 1h) | order by TimeGenerated asc`,
			[]string{"2024-05-06T09:00:00.0000000Z|1", "2024-05-06T11:00:00.0000000Z|6"}, "TimeGenerated:datetime,count_:long"},
		{`Logs | where TimeGenerated > ago(2h) | summarize count() by bin(TimeGenerated, 30m) | order by TimeGenerated asc`,
			[]string{"2024-05-06T11:00:00.0000000Z|3", "2024-05-06T11:30:00.0000000Z|3"}, ""},
		{`Logs | summarize avg(Value) by Ok | order by Ok asc`, []string{"|", "false|19.75", "true|1.525"}, ""},
		{`Logs | summarize sum(Duration) by Level | where sum_Duration > 1000 | order by Level asc`, []string{"error|3900"}, ""},
		{`Logs | summarize countif(Level == "error"), sumif(Duration, Ok == true)`, []string{"2|660"}, "countif_:long,sumif_Duration:long"},
		{`Logs | where Level == "nothing" | summarize count()`, []string{"0"}, ""},
		{`Logs | where Level == "nothing" | summarize count() by Level`, nil, "Level:string,count_:long"},
		{`Logs | extend Slow = Duration > 400 | where Slow | count`, []string{"3"}, ""},
		{`Logs | extend Sec = Duration / 1000.0 | where Message == "heartbeat" | project Sec`, []string{"0.01"}, "Sec:real"},
		{`Logs | extend Label = strcat(Level, ": ", Message) | where Duration == 10 | project Label`, []string{"info: heartbeat"}, ""},
		{`Logs | where Duration == 10 | project u = toupper(Level), l = strlen(Message), s = substring(Message, 0, 4)`, []string{"INFO|9|hear"}, "u:string,l:long,s:string"},
		{`Logs | where Duration == 120 | extend Sev = iff(Level == "error", 3, 1) | project Sev`, []string{"1"}, "Sev:long"},
		{`Logs | where Duration == 900 | extend Sev = case(Level == "info", 1, Level == "warn", 2, Level == "error", 3, 0) | project Sev`, []string{"3"}, ""},
		{`Logs | project-away Tags, Ok, Value, TimeGenerated | where Duration == 80`, []string{"info|request ok|80"}, "Level:string,Message:string,Duration:long"},
		{`Logs | where Tags.env == "dev" | project Message`, []string{"slow request"}, ""},
		{`Logs | where tostring(Tags.env) == "prod" | count`, []string{"4"}, ""},
		{`Logs | where toint(Tags.n) == 2 | project Message, n = tolong(Tags["n"])`, []string{"timeout calling Billing|2"}, ""},
		{`let thr = 400; Logs | where Duration > thr | count`, []string{"3"}, ""},
		{`let errs = Logs | where Level == "error"; errs | count`, []string{"2"}, ""},
		{`Logs | where Message matches regex @"^disk.*node-\d$" | count`, []string{"1"}, ""},
		{`Logs | extend D = TimeGenerated - datetime(2024-05-06T10:00:00Z) | where Message == "service started" | project D`, []string{"01:00:00"}, "D:timespan"},
		{`Logs | extend B = bin(Duration, 100) | where Duration == 450 | project B`, []string{"400"}, ""},
		{`Logs | where Message == "heartbeat" | project t = tostring(TimeGenerated)`, []string{"2024-05-06T11:59:00.0000000Z"}, ""},
		{`Logs | where Duration * 2 > 1000 | count`, []string{"2"}, ""},
		{`Logs | where Level == "warn" | summarize count() | render barchart`, []string{"1"}, ""},
		{`Logs | where Level == "info" | project Level, Message = tolower(Message) // a comment` + "\n" + `| count`, []string{"3"}, ""},
		{`Logs | summarize n = count(), s = sum(Duration), a = avg(Duration), m = max(TimeGenerated) by Level | take 0`, nil, "Level:string,n:long,s:long,a:real,m:datetime"},
	}
	if len(tests) < 40 {
		t.Fatalf("only %d queries", len(tests))
	}
	for _, tc := range tests {
		t.Run(tc.q, func(t *testing.T) {
			r, err := runKQL(tc.q)
			if err != nil {
				t.Fatal(err)
			}
			if got := render(r); !slices.Equal(got, tc.want) {
				t.Errorf("rows\n got  %q\n want %q", got, tc.want)
			}
			if tc.cols != "" {
				var got []string
				for _, c := range r.cols {
					got = append(got, c.Name+":"+c.Type)
				}
				if g := strings.Join(got, ","); g != tc.cols {
					t.Errorf("columns\n got  %s\n want %s", g, tc.cols)
				}
			}
		})
	}
}

func TestKQLErrors(t *testing.T) {
	for _, tc := range []struct{ q, kind, msg string }{
		{`Logs | where Nope == 1`, "SemanticError", "Failed to resolve scalar expression named 'Nope'"},
		{`Missing | count`, "SemanticError", "Failed to resolve table"},
		{`Logs | where (Level == "x"`, "SyntaxError", "expected ')'"},
		{`Logs | where`, "SyntaxError", "expected an expression"},
		{`Logs | join Logs on Level`, "SyntaxError", "not supported"},
		{`Logs | where nosuchfn(Level) == 1`, "SemanticError", "nosuchfn"},
		{`Logs | where count() > 1`, "SemanticError", "summarize"},
		{`Logs | summarize Level`, "SemanticError", "aggregate"},
		{`Logs | where Level == "unterminated`, "SyntaxError", "unterminated"},
		{`let a = a; a`, "SemanticError", "Failed to resolve"},
		{`let a = b; let b = a; a`, "SemanticError", "Failed to resolve"},
		{`let x = x + 1; Logs | extend y = x`, "SemanticError", "Failed to resolve"},
		{`Logs | where ` + strings.Repeat("(", 100000) + `1==1`, "SyntaxError", "nested more than"},
		{`Logs | extend a = ` + strings.Repeat("-", 100000) + `1`, "SyntaxError", "nested more than"},
	} {
		_, err := runKQL(tc.q)
		ke, ok := err.(*kqlError)
		if !ok || ke.kind != tc.kind || !strings.Contains(ke.msg, tc.msg) {
			t.Errorf("%s: got %v, want %s containing %q", tc.q, err, tc.kind, tc.msg)
		}
	}
}
