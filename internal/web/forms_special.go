package web

import (
	"bytes"
	"cmp"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/aws/sns"
	"github.com/nrndev/pogi/internal/core"
)

// special is a form no API model describes: creating a bucket, publishing a message, sending an
// event. Its fields are the console's own, and it submits through the service's console calls.
type special struct {
	Op, Title string
	Fields    func(ui *UI) []core.Field
	Submit    func(ui *UI, sc scope, in map[string]any) error
	// CLI is the command the form stands for, when the generic mirror (from the field names) is wrong.
	CLI func(endpoint string, in map[string]any) string
}

func fixed(f ...core.Field) func(*UI) []core.Field { return func(*UI) []core.Field { return f } }

const bucketNameHelp = "Use 3–63 characters: lowercase letters, digits, dots and hyphens, starting and ending with a letter or digit."

// bucketNameField is the S3 bucket name with its naming rules as help text and a pattern, so the
// browser flags a bad name before the call does.
func bucketNameField() core.Field {
	f := req(str("Bucket"))
	f.Help, f.Pattern = bucketNameHelp, `[a-z0-9][a-z0-9.\-]{1,61}[a-z0-9]`
	return f
}

func str1(in map[string]any, k string) string { s, _ := in[k].(string); return s }

var specials = map[string][]special{
	"s3": {{Op: "CreateBucket", Title: "Create bucket", Fields: fixed(bucketNameField()),
		Submit: func(ui *UI, sc scope, in map[string]any) error {
			s, _ := ui.hub.Service("s3").(*s3.Service)
			name := strings.TrimSpace(str1(in, "Bucket"))
			err := ui.write("s3", "CreateBucket", name, func() error { return s.CreateBucket(sc.acct(), sc.region(ui.hub.Cfg.Region), name) })
			if err != nil && core.AsError(err).Code == "InvalidBucketName" {
				return fmt.Errorf("Bucket name is invalid: %q. %s", name, bucketNameHelp)
			}
			return err
		}}},
	"gcs": {{Op: "CreateBucket", Title: "Create bucket", Fields: fixed(req(str("name"))),
		Submit: func(ui *UI, _ scope, in map[string]any) error {
			return ui.store("gcs").MakeBucket(strings.TrimSpace(str1(in, "name")))
		},
		CLI: func(ep string, in map[string]any) string {
			return fmt.Sprintf("curl -X POST '%s/storage/v1/b?project=%s' -H 'Content-Type: application/json' -d '%s'", ep, core.DefaultProject, jsonOf(in))
		}}},
	"azblob": {{Op: "CreateContainer", Title: "Create container", Fields: fixed(req(str("name"))),
		Submit: func(ui *UI, _ scope, in map[string]any) error {
			return ui.store("azblob").MakeBucket(strings.TrimSpace(str1(in, "name")))
		},
		CLI: func(ep string, in map[string]any) string {
			return fmt.Sprintf("curl -X PUT '%s/%s?restype=container'", ep, str1(in, "name"))
		}}},
	"sns": {{Op: "Publish", Title: "Publish message", Fields: func(ui *UI) []core.Field {
		return []core.Field{req(enum("TopicArn", ui.choices("sns", "Topics")...)), str("Subject"), req(text("Message"))}
	}, Submit: func(ui *UI, _ scope, in map[string]any) error {
		s, _ := ui.hub.Service("sns").(*sns.Service)
		topic := str1(in, "TopicArn")
		_, region, account, _, _ := core.ParseARN(topic)
		return ui.write("sns", "Publish", topic, func() error {
			_, err := s.PublishConsole(account, region, topic, str1(in, "Subject"), str1(in, "Message"))
			return err
		})
	}}},
	"events": {{Op: "PutEvent", Title: "Send event", Fields: func(ui *UI) []core.Field {
		buses := slices.DeleteFunc(ui.choices("events", "Buses"), func(b string) bool { return b == "default" })
		bus := req(enum("EventBusName", append([]string{"default"}, buses...)...))
		bus.Default = "default"
		return []core.Field{bus, req(str("Source")), req(str("DetailType")), text("Detail")}
	}, Submit: func(ui *UI, sc scope, in map[string]any) error {
		e, _ := ui.hub.Service("events").(*events.Service)
		detail := strings.TrimSpace(str1(in, "Detail"))
		if detail == "" {
			detail = "{}"
		}
		if !json.Valid([]byte(detail)) {
			return fmt.Errorf("Detail is not valid JSON")
		}
		return ui.write("events", "PutEvents", str1(in, "EventBusName"), func() error {
			_, err := e.PutEvent(sc.acct(), sc.region(ui.hub.Cfg.Region), str1(in, "EventBusName"), str1(in, "Source"), str1(in, "DetailType"), detail)
			return err
		})
	}, CLI: func(_ string, in map[string]any) string {
		return "pogi aws events put-events --entries '[" + jsonOf(in) + "]'"
	}}},
}

func file(n string) core.Field { return fld(n, "file") }

// rest sends a JSON call to a service in-process and turns a failure into its message.
func (ui *UI) rest(sc scope, svc, verb, path string, body any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(verb, "http://localhost"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(core.ConsoleHeader, "1")
	if status, resp := ui.hub.DoServiceAs(svc, sc.acct(), sc.region(ui.hub.Cfg.Region), req); status >= 300 {
		return fmt.Errorf("%d: %s", status, apiMessage(resp))
	}
	return nil
}

func init() {
	specials["lambda"] = []special{{Op: "CreateFunction", Title: "Create function",
		Fields: fixed(req(str("FunctionName")),
			req(enum("Runtime", "python3.13", "python3.12", "nodejs22.x", "nodejs20.x", "java21", "dotnet8", "provided.al2023")),
			withDefault(req(str("Handler")), "app.handler"),
			withDefault(req(str("Role")), "arn:aws:iam::{account}:role/lambda"),
			req(file("ZipFile")),
			withDefault(num("MemorySize"), "128"), withDefault(num("Timeout"), "3"), str("Description"),
			obj("Environment", dict("Variables"))),
		Submit: func(ui *UI, sc scope, in map[string]any) error {
			in["Code"] = map[string]any{"ZipFile": in["ZipFile"]}
			delete(in, "ZipFile")
			return ui.rest(sc, "lambda", http.MethodPost, "/2015-03-31/functions", in)
		},
		CLI: func(_ string, in map[string]any) string {
			ph := func(k string) string { return shellArg(cmp.Or(str1(in, k), "<"+kebab(k)+">")) }
			c := "pogi aws lambda create-function --function-name " + ph("FunctionName") + " \\\n  --runtime " + ph("Runtime") +
				" --handler " + ph("Handler") + " \\\n  --role " + ph("Role") + " \\\n  --zip-file fileb://function.zip"
			for _, k := range []string{"MemorySize", "Timeout"} {
				if v, ok := in[k]; ok {
					c += " --" + kebab(k) + " " + fmt.Sprint(v)
				}
			}
			return c
		}}}
	specials["pubsub"] = []special{{Op: "Publish", Title: "Publish message",
		Fields: func(ui *UI) []core.Field {
			return []core.Field{req(enum("topic", ui.choices("pubsub", "Topics")...)), req(text("data"))}
		},
		Submit: func(ui *UI, sc scope, in map[string]any) error {
			msg := map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(str1(in, "data")))}
			return ui.rest(sc, "pubsub", http.MethodPost, "/v1/"+str1(in, "topic")+":publish", map[string]any{"messages": []any{msg}})
		},
		CLI: func(ep string, in map[string]any) string {
			b, _ := json.Marshal(map[string]any{"messages": []any{map[string]any{"data": base64.StdEncoding.EncodeToString([]byte(str1(in, "data")))}}})
			return curlCmd("POST", ep+"/v1/"+str1(in, "topic")+":publish", b)
		}}}
	specials["azblob"] = append(specials["azblob"], special{Op: "UploadBlob", Title: "Upload blob",
		Fields: func(ui *UI) []core.Field {
			return []core.Field{req(enum("container", ui.choices("azblob", "Containers")...)), req(str("name")), req(file("file"))}
		},
		Submit: func(ui *UI, _ scope, in map[string]any) error {
			data, err := base64.StdEncoding.DecodeString(str1(in, "file"))
			if err != nil {
				return err
			}
			return ui.store("azblob").Upload(str1(in, "container"), str1(in, "name"), "", data)
		},
		CLI: func(ep string, in map[string]any) string {
			return "curl -X PUT -H 'x-ms-blob-type: BlockBlob' --data-binary @file '" + ep + "/" + str1(in, "container") + "/" + str1(in, "name") + "'"
		}})
}

func withDefault(f core.Field, d string) core.Field { f.Default = d; return f }

// jsonOf is a value as JSON that sits inside single quotes in a shell command.
func jsonOf(v any) string {
	b, _ := json.Marshal(v)
	return strings.ReplaceAll(string(b), "'", `'\''`)
}

func (ui *UI) special(svc, op string) *special {
	for i, sp := range specials[svc] {
		if sp.Op == op {
			return &specials[svc][i]
		}
	}
	return nil
}

// choices are what a service's action forms pick from (SNS topic ARNs, event bus names).
func (ui *UI) choices(svc, table string) []string {
	b, ok := ui.hub.Service(svc).(core.Browsable)
	if !ok {
		return nil
	}
	var ts []core.Table
	for _, t := range b.Resources() {
		if t.Name == table {
			ts = append(ts, t)
		}
	}
	if svc == "sns" || svc == "events" {
		return extra(svc, ts)
	}
	var out []string
	for _, t := range ts {
		for _, row := range t.Rows {
			switch {
			case svc == "azblob" && len(row) > 1:
				out = append(out, row[1]+"/"+row[0]) // the store's bucket: account/container
			case len(row) > 0:
				out = append(out, row[0])
			}
		}
	}
	return out
}
