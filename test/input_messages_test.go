package test

// Guards for plain error messages (no Go parser text) and name checks on odd input. Each probe runs against a
// real pogi.

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

type inProbe struct {
	cloud, name, method, path string // path may start with "port:<service>" to use that service's own port
	ctype, sign, target       string // sign: AWS service to sign for; target: X-Amz-Target
	body                      string
}

var goJargon = []string{"invalid character", "json:", "cannot unmarshal", "unexpected end of JSON", "proto:", "EOF",
	"runtime error", "goroutine", "nil pointer", "strconv.", "interface conversion", "reflect.", "xml:", "unmarshal", "syntax error"}

// I2: malformed requests on a sample of APIs per cloud. Open = a 5xx, an empty answer, or an answer not in the
// cloud's error shape. Partly = a clear 4xx that shows a Go parser message.
func TestMalformedRequestsGetPlainErrors(t *testing.T) {
	srv := pogi.Start(t)
	const j, f, x = "application/json", "application/x-www-form-urlencoded", "application/xml"
	const aj = "application/x-amz-json-1.0"
	probes := []inProbe{
		{"aws", "SQS json: bad JSON", "POST", "/", aj, "sqs", "AmazonSQS.SendMessage", `{bad`},
		{"aws", "SQS json: wrong type", "POST", "/", aj, "sqs", "AmazonSQS.SendMessage", `{"QueueUrl":5,"MessageBody":[1]}`},
		{"aws", "SQS json: required missing", "POST", "/", aj, "sqs", "AmazonSQS.SendMessage", `{}`},
		{"aws", "SQS json: body is a JSON array", "POST", "/", aj, "sqs", "AmazonSQS.CreateQueue", `[1,2]`},
		{"aws", "SQS json: 9 MB of nested arrays", "POST", "/", aj, "sqs", "AmazonSQS.CreateQueue", strings.Repeat("[", 9<<20)},
		{"gcp", "Pub/Sub: 9 MB of nested objects", "PUT", "/v1/projects/p/topics/t", j, "", "", strings.Repeat(`{"a":`, 2<<20)},
		{"aws", "SQS json: body is null", "POST", "/", aj, "sqs", "AmazonSQS.CreateQueue", `null`},
		{"aws", "SQS query: bad action", "POST", "/", f, "sqs", "", `Action=Nope&Version=2012-11-05`},
		{"aws", "SQS query: required missing", "POST", "/", f, "sqs", "", `Action=SendMessage&Version=2012-11-05`},
		{"aws", "SQS query: bad escape", "POST", "/", f, "sqs", "", `Action=%zz&QueueName=%`},
		{"aws", "DynamoDB: bad JSON", "POST", "/", "application/x-amz-json-1.0", "dynamodb", "DynamoDB_20120810.PutItem", `{"TableName":`},
		{"aws", "DynamoDB: wrong types", "POST", "/", "application/x-amz-json-1.0", "dynamodb", "DynamoDB_20120810.PutItem", `{"TableName":7,"Item":"x"}`},
		{"aws", "DynamoDB: required missing", "POST", "/", "application/x-amz-json-1.0", "dynamodb", "DynamoDB_20120810.CreateTable", `{}`},
		{"aws", "DynamoDB: bad attribute value", "POST", "/", "application/x-amz-json-1.0", "dynamodb", "DynamoDB_20120810.PutItem", `{"TableName":"tbl","Item":{"a":{"Q":"1"}}}`},
		{"aws", "S3: bad XML bucket config", "PUT", "/badxml", x, "s3", "", `<CreateBucketConfiguration><`},
		{"aws", "S3: bad XML DeleteObjects", "POST", "/nobucket?delete", x, "s3", "", `not xml at all`},
		{"aws", "S3: bad range header", "GET", "/nobucket/k", "", "s3", "", ``},
		{"aws", "Lambda: bad JSON", "POST", "/2015-03-31/functions", j, "lambda", "", `{bad`},
		{"aws", "Lambda: required missing", "POST", "/2015-03-31/functions", j, "lambda", "", `{}`},
		{"aws", "Lambda: wrong types", "POST", "/2015-03-31/functions", j, "lambda", "", `{"FunctionName":5,"Code":"x","MemorySize":"big"}`},
		{"aws", "EC2: required missing", "POST", "/", f, "ec2", "", `Action=RunInstances&Version=2016-11-15`},
		{"aws", "EC2: non-number count", "POST", "/", f, "ec2", "", `Action=RunInstances&Version=2016-11-15&ImageId=ami-1&MinCount=abc&MaxCount=-1`},
		{"aws", "SNS: required missing", "POST", "/", f, "sns", "", `Action=Publish&Version=2010-03-31`},
		{"aws", "Secrets Manager: bad JSON", "POST", "/", "application/x-amz-json-1.1", "secretsmanager", "secretsmanager.CreateSecret", `}{`},
		{"aws", "Kinesis: wrong type", "POST", "/", "application/x-amz-json-1.1", "kinesis", "Kinesis_20131202.PutRecord", `{"StreamName":["a"],"Data":5}`},
		{"gcp", "Pub/Sub: bad JSON", "PUT", "/v1/projects/p/topics/t", j, "", "", `{bad`},
		{"gcp", "Pub/Sub: wrong type", "POST", "/v1/projects/p/topics/t:publish", j, "", "", `{"messages":"x"}`},
		{"gcp", "Pub/Sub: required missing", "PUT", "/v1/projects/p/subscriptions/s", j, "", "", `{}`},
		{"gcp", "Pub/Sub: empty body on create", "POST", "/v1/projects/p/topics/t:publish", j, "", "", ``},
		{"gcp", "GCS: bad JSON bucket", "POST", "/storage/v1/b?project=p", j, "", "", `{"name":`},
		{"gcp", "GCS: wrong type", "POST", "/storage/v1/b?project=p", j, "", "", `{"name":["x"]}`},
		{"gcp", "BigQuery: bad JSON", "POST", "/bigquery/v2/projects/p/datasets", j, "", "", `{`},
		{"gcp", "BigQuery: wrong type", "POST", "/bigquery/v2/projects/p/datasets", j, "", "", `{"datasetReference":5}`},
		{"gcp", "Secret Manager: bad JSON", "POST", "/v1/projects/p/secrets?secretId=s", j, "", "", `{"replication":`},
		{"gcp", "Compute: bad JSON", "POST", "/compute/v1/projects/p/zones/us-central1-a/instances", j, "", "", `{"name":`},
		{"gcp", "Compute: wrong type", "POST", "/compute/v1/projects/p/zones/us-central1-a/instances", j, "", "", `{"name":7,"disks":"x"}`},
		{"gcp", "Firestore/Datastore: bad JSON", "POST", "/v1/projects/p:beginTransaction", j, "", "", `x`},
		{"azure", "ARM: bad JSON", "PUT", "/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg?api-version=2021-04-01", j, "", "", `{bad`},
		{"azure", "ARM: wrong type", "PUT", "/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg?api-version=2021-04-01", j, "", "", `{"location":5,"tags":"x"}`},
		{"azure", "ARM: no api-version", "PUT", "/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/rg", j, "", "", `{"location":"eastus"}`},
		{"azure", "ARM: storage account bad types", "PUT", "/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.Storage/storageAccounts/acct1?api-version=2023-01-01", j, "", "", `{"sku":"x","kind":5}`},
		{"azure", "Blob: bad XML PutBlockList", "PUT", "port:azblob/devstoreaccount1/c/b?comp=blocklist", x, "", "", `<BlockList><`},
		{"azure", "Blob: bad blockid", "PUT", "port:azblob/devstoreaccount1/c/b?comp=block&blockid=!!", "", "", "", `x`},
		{"azure", "Queue: bad XML message", "POST", "port:azqueue/devstoreaccount1/q/messages", x, "", "", `<QueueMessage><`},
		{"azure", "Table: bad JSON", "POST", "port:aztable/devstoreaccount1/Tables", j, "", "", `{bad`},
	}
	for _, p := range probes {
		base, path := srv.URL, p.path
		if svc, rest, _ := strings.Cut(strings.TrimPrefix(p.path, "port:"), "/"); strings.HasPrefix(p.path, "port:") {
			port, have := srv.Ports[svc]
			if !have {
				t.Logf("%s %s: SKIP no own port for %q", p.cloud, p.name, svc)
				continue
			}
			base, path = port, "/"+rest
		}
		req, _ := http.NewRequest(p.method, base+path, strings.NewReader(p.body))
		if p.ctype != "" {
			req.Header.Set("Content-Type", p.ctype)
		}
		if p.target != "" {
			req.Header.Set("X-Amz-Target", p.target)
		}
		if p.sign != "" {
			awstest.Sign(t, req, p.sign)
		}
		res, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
		if err != nil {
			t.Errorf("[%s] %s: OPEN request failed: %v", p.cloud, p.name, err)
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(res.Body, 64<<10))
		res.Body.Close()
		body := string(b)
		short := strings.Join(strings.Fields(body), " ")
		if len(short) > 230 {
			short = short[:230]
		}
		var problems []string
		if res.StatusCode >= 500 {
			problems = append(problems, "5xx")
		}
		if res.StatusCode < 400 {
			problems = append(problems, "accepted bad input")
		}
		if strings.TrimSpace(body) == "" && res.Request.Method != "HEAD" {
			problems = append(problems, "empty body")
		}
		plainPart, _, _ := strings.Cut(body, " — ")                 // Go's text may follow after " — "
		plainPart = strings.ReplaceAll(plainPart, "into json:", "") // AWS's own Lambda wording
		for _, w := range goJargon {
			if strings.Contains(plainPart, w) {
				problems = append(problems, "go-jargon:"+w)
				break
			}
		}
		shape := true
		switch p.cloud {
		case "gcp":
			shape = strings.Contains(body, `"status"`) && strings.Contains(body, `"message"`) || strings.Contains(body, `"errors"`)
		case "azure":
			shape = strings.Contains(body, `"code"`) && strings.Contains(body, `"message"`) || strings.Contains(body, "<Code>") || strings.Contains(body, "<Message>")
		case "aws":
			shape = strings.Contains(body, "__type") || strings.Contains(body, "<Code>") || strings.Contains(body, `"Code"`) || strings.Contains(body, `"code"`) || strings.Contains(body, `"message"`)
		}
		if !shape {
			problems = append(problems, "not-"+p.cloud+"-shaped")
		}
		t.Logf("[%s] %s -> %d %s | %s", p.cloud, p.name, res.StatusCode, strings.Join(problems, ","), short)
		if len(problems) > 0 {
			t.Errorf("[%s] %s: %v", p.cloud, p.name, problems)
		}
	}
}

func TestOddNamesAreChecked(t *testing.T) {
	srv := pogi.Start(t)
	u := srv.URL
	rep := strings.Repeat
	const aj, aj11, f = "application/x-amz-json-1.0", "application/x-amz-json-1.1", "application/x-www-form-urlencoded"
	type nameCase struct {
		what   string
		accept bool // true: the real cloud accepts this name
		do     func() (int, string)
	}
	var cases []nameCase
	add := func(what string, accept bool, do func() (int, string)) {
		cases = append(cases, nameCase{what, accept, do})
	}
	// ok says whether the real cloud takes the name: its documented pattern and length.
	ok := func(re string, lo, hi int, name string) bool {
		return len(name) >= lo && len(name) <= hi && regexp.MustCompile("^"+re+"$").MatchString(name)
	}
	names := []struct{ label, name string }{
		{"empty", ""}, {"81 chars", rep("q", 81)}, {"10,000 chars", rep("q", 10000)},
		{"unicode", "队列🙂"}, {"space", "a b"}, {"slash", "a/b"}, {"newline", "a\nb"},
		{"html", "<img src=x onerror=alert(1)>"}, {"80 chars", rep("q", 80)}, {"letters", "good-name_1"},
	}
	for _, n := range names {
		n := n
		add("SQS CreateQueue "+n.label, ok(`[A-Za-z0-9_-]+`, 1, 80, n.name), func() (int, string) {
			b, _ := json.Marshal(map[string]string{"QueueName": n.name})
			return msgCall(t, "POST", u+"/", aj, "sqs", "AmazonSQS.CreateQueue", b)
		})
		add("SNS CreateTopic "+n.label, ok(`[A-Za-z0-9_-]+`, 1, 256, n.name), func() (int, string) {
			return msgCall(t, "POST", u+"/", f, "sns", "", []byte(url.Values{"Action": {"CreateTopic"}, "Name": {n.name}, "Version": {"2010-03-31"}}.Encode()))
		})
		add("DynamoDB CreateTable "+n.label, ok(`[A-Za-z0-9_.-]+`, 3, 255, n.name), func() (int, string) {
			b, _ := json.Marshal(map[string]any{"TableName": n.name, "BillingMode": "PAY_PER_REQUEST",
				"AttributeDefinitions": []map[string]string{{"AttributeName": "a", "AttributeType": "S"}},
				"KeySchema":            []map[string]string{{"AttributeName": "a", "KeyType": "HASH"}}})
			return msgCall(t, "POST", u+"/", aj, "dynamodb", "DynamoDB_20120810.CreateTable", b)
		})
		add("Lambda CreateFunction "+n.label, ok(`[A-Za-z0-9_-]+`, 1, 64, n.name), func() (int, string) {
			b, _ := json.Marshal(map[string]any{"FunctionName": n.name, "Runtime": "python3.12", "Handler": "i.h", "Role": "arn:aws:iam::000000000000:role/r", "Code": map[string]string{"ZipFile": "UEsFBgAAAAAAAAAAAAAAAAAAAAAAAA=="}})
			return msgCall(t, "POST", u+"/2015-03-31/functions", "application/json", "lambda", "", b)
		})
		add("S3 CreateBucket "+n.label, ok(`[a-z0-9][a-z0-9.-]*[a-z0-9]`, 3, 63, n.name), func() (int, string) {
			return msgCall(t, "PUT", u+"/"+url.PathEscape(n.name), "", "s3", "", nil)
		})
		add("Secrets Manager CreateSecret "+n.label, ok(`[A-Za-z0-9/_+=.@-]+`, 1, 512, n.name), func() (int, string) {
			b, _ := json.Marshal(map[string]string{"Name": n.name, "SecretString": "x"})
			return msgCall(t, "POST", u+"/", aj11, "secretsmanager", "secretsmanager.CreateSecret", b)
		})
		add("Pub/Sub create topic "+n.label, ok(`[A-Za-z][A-Za-z0-9._~+%-]*`, 3, 255, n.name), func() (int, string) {
			return msgCall(t, "PUT", u+"/v1/projects/p/topics/"+url.PathEscape(n.name), "application/json", "", "", []byte("{}"))
		})
		add("GCS create bucket "+n.label, ok(`[a-z0-9][a-z0-9._-]*[a-z0-9]`, 3, 63, n.name), func() (int, string) {
			b, _ := json.Marshal(map[string]string{"name": n.name})
			return msgCall(t, "POST", u+"/storage/v1/b?project=p", "application/json", "", "", b)
		})
		add("ARM resource group "+n.label, ok(`[\p{L}\p{N}_().-]*[\p{L}\p{N}_()-]`, 1, 90, n.name), func() (int, string) {
			return msgCall(t, "PUT", u+"/subscriptions/00000000-0000-0000-0000-000000000000/resourcegroups/"+url.PathEscape(n.name)+"?api-version=2021-04-01", "application/json", "", "", []byte(`{"location":"eastus"}`))
		})
		add("Blob create container "+n.label, ok(`[a-z0-9][a-z0-9-]*[a-z0-9]`, 3, 63, n.name), func() (int, string) {
			return msgCall(t, "PUT", srv.Ports["azblob"]+"/devstoreaccount1/"+url.PathEscape(n.name)+"?restype=container", "", "", "", nil)
		})
	}
	add("S3 object key of 1025 bytes", false, func() (int, string) {
		msgCall(t, "PUT", u+"/keys", "", "s3", "", nil)
		return msgCall(t, "PUT", u+"/keys/"+rep("k", 1025), "", "s3", "", []byte("x"))
	})
	add("S3 object key of 100 KB", false, func() (int, string) {
		return msgCall(t, "PUT", u+"/keys/"+rep("k", 100<<10), "", "s3", "", []byte("x"))
	})
	add("S3 object key with unicode and <script>", true, func() (int, string) {
		return msgCall(t, "PUT", u+"/keys/"+url.PathEscape("数据/<script>alert(1)</script>.txt"), "", "s3", "", []byte("x"))
	})
	add("S3 object key with a newline", true, func() (int, string) { return msgCall(t, "PUT", u+"/keys/a%0Ab", "", "s3", "", []byte("x")) })
	bad := 0
	for _, c := range cases {
		if strings.HasPrefix(c.what, "ARM resource group empty") || strings.HasPrefix(c.what, "S3 CreateBucket empty") {
			continue // an empty name is another route (a list, PUT /), not a name
		}
		code, body := c.do()
		short := strings.Join(strings.Fields(body), " ")
		if len(short) > 140 {
			short = short[:140]
		}
		verdict := "ok"
		switch {
		case code == 0 || code >= 500:
			verdict = "OPEN 5xx/dead"
		case c.accept && code >= 400:
			verdict = "refused a valid name"
		case !c.accept && code < 300:
			verdict = "accepted a name the cloud refuses"
		}
		if verdict != "ok" {
			bad++
			t.Errorf("%s -> %d %s | %s", c.what, code, verdict, short)
		} else {
			t.Logf("%s -> %d ok | %s", c.what, code, short)
		}
	}
	// the console still renders every list, and shows the odd names escaped
	for _, svc := range []string{"sqs", "sns", "dynamodb", "s3", "gcs", "azblob", "pubsub", "lambda"} {
		code, body := msgCall(t, "GET", u+"/_pogi/resources/"+svc, "", "", "", nil)
		if code != 200 || strings.Contains(body, "<img src=x onerror") || strings.Contains(body, "<script>alert(1)") {
			t.Errorf("console list %s: status %d, raw html in page: %v", svc, code, strings.Contains(body, "<img src=x onerror") || strings.Contains(body, "<script>alert(1)"))
		}
	}
	code, body := msgCall(t, "GET", u+"/_pogi/objects/s3/keys", "", "", "", nil)
	if code != 200 || strings.Contains(body, "<script>alert(1)") {
		t.Errorf("console object list: status %d or unescaped key", code)
	}
	// the same name in three clouds
	msgCall(t, "PUT", u+"/shared-name", "", "s3", "", nil)
	gc, _ := msgCall(t, "POST", u+"/storage/v1/b?project=p", "application/json", "", "", []byte(`{"name":"shared-name"}`))
	bc, _ := msgCall(t, "PUT", srv.Ports["azblob"]+"/devstoreaccount1/shared-name?restype=container", "", "", "", nil)
	sq, _ := msgCall(t, "POST", u+"/", aj, "sqs", "AmazonSQS.CreateQueue", []byte(`{"QueueName":"shared-name"}`))
	sn, _ := msgCall(t, "POST", u+"/", f, "sns", "", []byte("Action=CreateTopic&Name=shared-name&Version=2010-03-31"))
	t.Logf("the name 'shared-name' in S3, GCS=%d, Blob=%d, SQS=%d, SNS=%d", gc, bc, sq, sn)
	if gc >= 300 || bc >= 300 || sq >= 300 || sn >= 300 {
		t.Errorf("a name used in S3 clashed with another cloud: gcs=%d blob=%d sqs=%d sns=%d", gc, bc, sq, sn)
	}
	// console forms
	for _, nm := range []string{"", rep("q", 81), "队列", "<b>x</b>", rep("q", 200000)} {
		code, body := msgCall(t, "POST", u+"/_pogi/new/sqs/CreateQueue", f, "", "", []byte(url.Values{"QueueName": {nm}}.Encode()))
		short := strings.Join(strings.Fields(body), " ")
		if i := strings.Index(short, `role="alert"`); i >= 0 {
			short = short[i:min(len(short), i+140)]
		} else {
			short = short[:min(len(short), 60)]
		}
		t.Logf("console SQS form, name len %d -> %d | %s", len(nm), code, short)
		if code >= 500 || strings.Contains(body, "<b>x</b>") {
			t.Errorf("console SQS form, name %.20q: status %d or unescaped", nm, code)
		}
	}
	if !alive(u) {
		t.Error("pogi died")
	}
}

// msgCall is one raw request; it returns the status and the start of the body.
func msgCall(t *testing.T, method, u, ctype, sign, target string, body []byte) (int, string) {
	t.Helper()
	req, _ := http.NewRequest(method, u, strings.NewReader(string(body)))
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	if target != "" {
		req.Header.Set("X-Amz-Target", target)
	}
	if sign != "" {
		awstest.Sign(t, req, sign)
	}
	r := fmSend(t, req)
	return r.Code, r.Body
}
