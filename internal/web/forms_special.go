package web

import (
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
	return ui.restJSON(sc, svc, verb, path, body, nil)
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

// eventSources are the queues, streams and DynamoDB streams a Lambda function can read from, as ARNs.
func (ui *UI) eventSources() []string {
	var out []string
	each := func(svc, table string, f func(row []string)) {
		if b, ok := ui.hub.Service(svc).(core.Browsable); ok {
			for _, t := range b.Resources() {
				for _, row := range t.Rows {
					if t.Name == table && len(row) > 2 {
						f(row)
					}
				}
			}
		}
	}
	each("sqs", "Queues", func(r []string) { out = append(out, core.ARN("sqs", r[1], r[2], r[0])) })
	each("kinesis", "Streams", func(r []string) { out = append(out, core.ARN("kinesis", r[1], r[2], "stream/"+r[0])) })
	each("dynamodb", "Tables", func(r []string) {
		if len(r) > 6 && r[6] != "" { // only a table with a stream on
			if arn := dig(ui.inspectMap("dynamodb", "Tables", r[0]), "Table", "LatestStreamArn"); arn != "" {
				out = append(out, arn)
			}
		}
	})
	return out
}

// jsonOrEmpty checks that text, when there is any, is a JSON value.
func jsonOrEmpty(label, text string) error {
	if text = strings.TrimSpace(text); text != "" && !json.Valid([]byte(text)) {
		return fmt.Errorf("%s is not valid JSON", label)
	}
	return nil
}

func init() {
	specials["lambda"] = append(specials["lambda"], special{Op: "CreateEventSourceMapping", Title: "Create event source mapping",
		Fields: func(ui *UI) []core.Field {
			start := enum("StartingPosition", "LATEST", "TRIM_HORIZON")
			start.Default, start.Help = "LATEST", "For Kinesis and DynamoDB streams. Queues ignore it."
			return []core.Field{req(enum("FunctionName", ui.choices("lambda", "Functions")...)), req(enum("EventSourceArn", ui.eventSources()...)),
				withDefault(num("BatchSize"), "10"), start}
		},
		Submit: func(ui *UI, sc scope, in map[string]any) error {
			if _, ok := in["BatchSize"]; ok && in["BatchSize"].(int64) < 1 {
				return fmt.Errorf("Batch size must be 1 or more")
			}
			return ui.rest(sc, "lambda", http.MethodPost, "/2015-03-31/event-source-mappings", in)
		},
		CLI: func(_ string, in map[string]any) string {
			c := "pogi aws lambda create-event-source-mapping --function-name " + shellArg(cmp.Or(str1(in, "FunctionName"), "<function-name>")) +
				" --event-source-arn " + shellArg(cmp.Or(str1(in, "EventSourceArn"), "<source-arn>"))
			if v, ok := in["BatchSize"]; ok {
				c += fmt.Sprintf(" --batch-size %d", v)
			}
			if p := str1(in, "StartingPosition"); p != "" {
				c += " --starting-position " + p
			}
			return c
		}})

	specials["sns"] = append(specials["sns"], special{Op: "CreateSubscription", Title: "Create subscription",
		Fields: func(ui *UI) []core.Field {
			raw := flag("RawMessageDelivery")
			raw.Help = "Send the message as it is, without the SNS JSON envelope."
			filter := text("FilterPolicy")
			filter.Help = `JSON, for example {"kind": ["order"]}. Only matching messages are delivered.`
			ep := req(str("Endpoint"))
			ep.Help = "A queue or function ARN, a URL, or an email address, to match the protocol."
			return []core.Field{req(enum("TopicArn", ui.choices("sns", "Topics")...)), req(enum("Protocol", "sqs", "lambda", "http", "https", "email")), ep, raw, filter}
		},
		Submit: func(ui *UI, _ scope, in map[string]any) error {
			_, region, account, _, ok := core.ParseARN(str1(in, "TopicArn"))
			if !ok {
				return fmt.Errorf("Pick a topic")
			}
			if err := jsonOrEmpty("Filter policy", str1(in, "FilterPolicy")); err != nil {
				return err
			}
			attrs := map[string]string{}
			if raw, ok := in["RawMessageDelivery"].(bool); ok {
				attrs["RawMessageDelivery"] = fmt.Sprint(raw)
			}
			if f := strings.TrimSpace(str1(in, "FilterPolicy")); f != "" {
				attrs["FilterPolicy"] = f
			}
			call := map[string]any{"TopicArn": in["TopicArn"], "Protocol": in["Protocol"], "Endpoint": strings.TrimSpace(str1(in, "Endpoint")), "ReturnSubscriptionArn": true}
			if len(attrs) > 0 {
				call["Attributes"] = attrs
			}
			return ui.hub.ConsoleCall(account, region, snsTarget, "Subscribe", call, nil)
		},
		CLI: func(_ string, in map[string]any) string {
			c := "pogi aws sns subscribe --topic-arn " + shellArg(cmp.Or(str1(in, "TopicArn"), "<topic-arn>")) +
				" --protocol " + shellArg(cmp.Or(str1(in, "Protocol"), "<protocol>")) + " --notification-endpoint " + shellArg(cmp.Or(str1(in, "Endpoint"), "<endpoint>"))
			var attrs []string
			if raw, ok := in["RawMessageDelivery"].(bool); ok {
				attrs = append(attrs, fmt.Sprintf("RawMessageDelivery=%t", raw))
			}
			if f := strings.TrimSpace(str1(in, "FilterPolicy")); f != "" {
				attrs = append(attrs, "FilterPolicy="+f)
			}
			if len(attrs) > 0 {
				c += " --attributes " + shellArg(strings.Join(attrs, ","))
			}
			return c
		}})

	specials["events"] = append(specials["events"], special{Op: "PutRule", Title: "Put rule",
		Fields: func(ui *UI) []core.Field {
			arn := req(str("Arn"))
			arn.Help = "What the rule calls: a queue, topic, function or state machine ARN."
			in := str("Input")
			in.Help = "JSON sent instead of the event. Leave empty to send the event."
			return []core.Field{req(str("Name")), str("EventBusName"), text("EventPattern"), str("ScheduleExpression"), enum("State", "ENABLED", "DISABLED"),
				str("Description"), list("Targets", obj("", req(str("Id")), arn, in)), tags("Tags")}
		},
		Submit: func(ui *UI, sc scope, in map[string]any) error {
			targets, _ := in["Targets"].([]any)
			delete(in, "Targets")
			for i, t := range targets { // check them all before anything is saved
				m, _ := t.(map[string]any)
				if str1(m, "Id") == "" || str1(m, "Arn") == "" {
					return fmt.Errorf("Target %d needs an Id and an ARN", i+1)
				}
				if err := jsonOrEmpty(fmt.Sprintf("Target %d input", i+1), str1(m, "Input")); err != nil {
					return err
				}
				if str1(m, "Input") == "" {
					delete(m, "Input")
				}
			}
			acct, region := sc.acct(), sc.region(ui.hub.Cfg.Region)
			if err := ui.hub.ConsoleCall(acct, region, "AWSEvents", "PutRule", in, nil); err != nil {
				return err
			}
			if len(targets) == 0 {
				return nil
			}
			err := ui.hub.ConsoleCall(acct, region, "AWSEvents", "PutTargets", map[string]any{"Rule": in["Name"], "EventBusName": in["EventBusName"], "Targets": targets}, nil)
			if err != nil {
				return fmt.Errorf("Rule %s was saved, but its targets were not: %s", in["Name"], errText(err))
			}
			return nil
		},
		CLI: func(_ string, in map[string]any) string {
			c := "pogi aws events put-rule --name " + shellArg(cmp.Or(str1(in, "Name"), "<name>"))
			if p := str1(in, "EventPattern"); p != "" {
				c += " --event-pattern " + shellArg(p)
			}
			if p := str1(in, "ScheduleExpression"); p != "" {
				c += " --schedule-expression " + shellArg(p)
			}
			if ts, _ := in["Targets"].([]any); len(ts) > 0 {
				b, _ := json.Marshal(ts)
				c += "\npogi aws events put-targets --rule " + shellArg(cmp.Or(str1(in, "Name"), "<name>")) + " --targets " + shellArg(string(b))
			}
			return c
		}})
}
