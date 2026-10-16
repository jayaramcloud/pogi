package cloudformation

import (
	"archive/zip"
	"bytes"
	"cmp"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// provider deploys one resource type through pogi's services.
type provider struct {
	create func(c *pctx, props map[string]any) (string, map[string]any, error)
	update func(c *pctx, phys string, old, props map[string]any) (map[string]any, bool, error) // nil: replace on change
	del    func(c *pctx, phys string, props map[string]any) error
	note   string
}

// pctx is what a provider knows about the resource it handles.
type pctx struct {
	s                                             *Service
	account, region, stack, stackID, logical, typ string
}

func (s *Service) ctx(st *Stack, logical, typ string) *pctx {
	return &pctx{s: s, account: st.Account, region: st.Region, stack: st.Name, stackID: st.ID, logical: logical, typ: typ}
}

func (c *pctx) call(target, op string, in, out any) error {
	return c.s.hub.Call(c.account, c.region, target, op, in, out)
}

// rest sends a request to a REST service (S3, Lambda, API Gateway …) in-process.
func (c *pctx) rest(signing, method, path string, body any, out any) error {
	var b []byte
	ct := "application/json"
	switch x := body.(type) {
	case nil:
	case []byte:
		b, ct = x, "application/xml"
	case string:
		b, ct = []byte(x), "application/xml"
	default:
		b, _ = json.Marshal(x)
	}
	status, _, resp := c.s.hub.Do(c.account, c.region, signing, method, path, http.Header{"Content-Type": {ct}}, b)
	if status >= 300 {
		var j struct{ Message, MessageLC, Code, Type string }
		var x struct {
			Code    string `xml:"Code"`
			Message string `xml:"Message"`
		}
		code, msg := http.StatusText(status), string(resp)
		if json.Unmarshal(resp, &j) == nil && (j.Message != "" || j.Code != "") {
			code, msg = j.Code, j.Message
		} else if xml.Unmarshal(resp, &x) == nil && x.Code != "" {
			code, msg = x.Code, x.Message
		}
		if code == "" {
			code = "ServiceError"
		}
		return core.Errorf(status, code, "%s", msg)
	}
	if out != nil && len(resp) > 0 {
		return json.Unmarshal(resp, out)
	}
	return nil
}

// name generates a physical name the way CloudFormation does: stack-logical-RANDOM.
func (c *pctx) name(max int, lower bool) string {
	base := c.stack + "-" + c.logical
	if len(base) > max-13 {
		base = base[:max-13]
	}
	n := base + "-" + core.RandString(12, core.UpperNum)
	if c.stack == seedStack { // seeding runs again after resets: keep the name it finds
		n = base
	}
	if lower {
		n = strings.ToLower(n)
	}
	return n
}

func (c *pctx) arn(service, resource string) string {
	return core.ARN(service, c.region, c.account, resource)
}

// pick copies the listed properties (when set) into an API input, optionally renamed "Prop=ApiName".
func pick(props map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, k := range keys {
		from, to, ok := strings.Cut(k, "=")
		if !ok {
			to = from
		}
		if v, ok := props[from]; ok {
			out[to] = v
		}
	}
	return out
}

// ttlSpec reads a table's TimeToLiveSpecification: the attribute name and
// whether TTL is on. A value that is not an object fails as CloudFormation's
// model validation does; so does an enabled one with no attribute name.
func ttlSpec(p map[string]any) (name string, on bool, err error) {
	v := p["TimeToLiveSpecification"]
	if v == nil {
		return "", false, nil
	}
	m, ok := v.(map[string]any)
	if !ok {
		kind := "String" // CloudFormation types every scalar as a string
		if _, list := v.([]any); list {
			kind = "JSONArray"
		}
		return "", false, fmt.Errorf("Property [TimeToLiveSpecification] expected type: JSONObject, found: %s", kind)
	}
	name, on = str(m["AttributeName"]), str(m["Enabled"]) == "true"
	if on && name == "" {
		return "", false, errors.New("Invalid request provided: AttributeName property of TimeToLiveSpecification is required when TTL status is enabled or when enabling TTL.")
	}
	return name, on, nil
}

// tableTTL turns a table's time to live on or off.
func tableTTL(c *pctx, table, attr string, on bool) error {
	return c.call("DynamoDB_20120810", "UpdateTimeToLive", map[string]any{"TableName": table,
		"TimeToLiveSpecification": map[string]any{"AttributeName": attr, "Enabled": on}}, nil)
}

func tagList(v any) []map[string]string {
	var out []map[string]string
	switch x := v.(type) {
	case []any:
		for _, t := range x {
			m, _ := t.(map[string]any)
			out = append(out, map[string]string{"Key": str(m["Key"]), "Value": str(m["Value"])})
		}
	case map[string]any:
		for _, k := range core.SortedKeys(x) {
			out = append(out, map[string]string{"Key": k, "Value": str(x[k])})
		}
	}
	return out
}

func tagMap(v any) map[string]string {
	out := map[string]string{}
	for _, t := range tagList(v) {
		out[t["Key"]] = t["Value"]
	}
	return out
}

func jsonString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	b, _ := json.Marshal(v)
	return string(b)
}

func lastSegment(s string) string { return s[strings.LastIndexAny(s, "/:")+1:] }

// noop records a resource that needs no backing service.
func noop(note string) *provider {
	return &provider{
		create: func(c *pctx, props map[string]any) (string, map[string]any, error) {
			id := c.name(64, false)
			return id, map[string]any{"Id": id}, nil
		},
		update: func(c *pctx, phys string, old, props map[string]any) (map[string]any, bool, error) {
			return nil, false, nil
		},
		del:  func(c *pctx, phys string, props map[string]any) error { return nil },
		note: note,
	}
}

// supportedType says whether pogi creates resources of a type: through a provider here, or through the
// engine of an embedded service (see engineProvider).
func (s *Service) supportedType(typ string) bool {
	return providers[typ] != nil || aliasProvider(typ) != nil || strings.HasPrefix(typ, "Custom::") || typ == "AWS::CloudFormation::CustomResource" || s.engineHas(typ)
}

// provider returns the provider for a type. A type without one cannot be created (checkTypes refuses the
// template first); a stack saved by an older pogi may still hold such a resource, which deletes as nothing.
func (s *Service) provider(typ string) *provider {
	if p := providers[typ]; p != nil {
		return p
	}
	if strings.HasPrefix(typ, "Custom::") || typ == "AWS::CloudFormation::CustomResource" {
		return customProvider
	}
	if p := aliasProvider(typ); p != nil {
		return p
	}
	if s.engineHas(typ) {
		return engineProvider(typ)
	}
	return &provider{
		create: func(c *pctx, props map[string]any) (string, map[string]any, error) {
			return "", nil, validation("pogi does not support %s yet", typ)
		},
		del: func(c *pctx, phys string, props map[string]any) error { return nil },
	}
}

var customProvider = &provider{
	create: func(c *pctx, props map[string]any) (string, map[string]any, error) {
		return c.s.custom(c, "Create", "", props, nil)
	},
	update: func(c *pctx, phys string, old, props map[string]any) (map[string]any, bool, error) {
		newPhys, data, err := c.s.custom(c, "Update", phys, props, old)
		return data, newPhys != phys && err == nil, err
	},
	del: func(c *pctx, phys string, props map[string]any) error {
		_, _, err := c.s.custom(c, "Delete", phys, props, nil)
		return err
	},
}

var providers = map[string]*provider{}

func register(typ string, p *provider) { providers[typ] = p }

func init() {
	register("AWS::CDK::Metadata", noop(""))
	register("AWS::CloudFormation::WaitConditionHandle", noop(""))
	register("AWS::CloudFormation::WaitCondition", noop(""))
	register("AWS::CloudFormation::Stack", nestedStack)

	// ---- S3 ----
	register("AWS::S3::Bucket", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["BucketName"])
			if name == "" {
				name = c.name(63, true)
			}
			var body []byte
			if c.region != "us-east-1" {
				body = []byte(`<CreateBucketConfiguration><LocationConstraint>` + c.region + `</LocationConstraint></CreateBucketConfiguration>`)
			}
			if err := c.rest("s3", "PUT", "/"+name, body, nil); err != nil {
				return "", nil, err
			}
			return name, bucketAttrs(c, name), configureBucket(c, name, nil, p)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if n := str(p["BucketName"]); n != "" && n != phys {
				return nil, true, nil
			}
			return bucketAttrs(c, phys), false, configureBucket(c, phys, old, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error { return c.rest("s3", "DELETE", "/"+phys, nil, nil) },
	})
	register("AWS::S3::BucketPolicy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			b := str(p["Bucket"])
			return b, nil, c.rest("s3", "PUT", "/"+b+"?policy", []byte(jsonString(p["PolicyDocument"])), nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			return nil, false, c.rest("s3", "PUT", "/"+phys+"?policy", []byte(jsonString(p["PolicyDocument"])), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("s3", "DELETE", "/"+phys+"?policy", nil, nil)
		},
	})

	// ---- SQS ----
	register("AWS::SQS::Queue", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["QueueName"])
			fifo := str(p["FifoQueue"]) == "true"
			if name == "" {
				name = c.name(80-5, false)
				if fifo {
					name += ".fifo"
				}
			}
			var out struct{ QueueUrl string }
			err := c.call("AmazonSQS", "CreateQueue", map[string]any{"QueueName": name, "Attributes": queueAttrs(p), "tags": tagMap(p["Tags"])}, &out)
			return out.QueueUrl, map[string]any{"Arn": c.arn("sqs", name), "QueueName": name, "QueueUrl": out.QueueUrl}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["QueueName"]) != str(old["QueueName"]) || str(p["FifoQueue"]) != str(old["FifoQueue"]) {
				return nil, true, nil
			}
			name := lastSegment(phys)
			return map[string]any{"Arn": c.arn("sqs", name), "QueueName": name, "QueueUrl": phys}, false,
				c.call("AmazonSQS", "SetQueueAttributes", map[string]any{"QueueUrl": phys, "Attributes": queueAttrs(p)}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AmazonSQS", "DeleteQueue", map[string]any{"QueueUrl": phys}, nil)
		},
	})
	register("AWS::SQS::QueuePolicy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			for _, q := range list(p["Queues"]) {
				if err := c.call("AmazonSQS", "SetQueueAttributes", map[string]any{"QueueUrl": q, "Attributes": map[string]string{"Policy": jsonString(p["PolicyDocument"])}}, nil); err != nil {
					return "", nil, err
				}
			}
			return c.name(64, false), nil, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			for _, q := range list(p["Queues"]) {
				c.call("AmazonSQS", "SetQueueAttributes", map[string]any{"QueueUrl": q, "Attributes": map[string]string{"Policy": ""}}, nil)
			}
			return nil
		},
	})

	// ---- SNS ----
	register("AWS::SNS::Topic", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["TopicName"])
			if name == "" {
				name = c.name(256-5, false)
				if str(p["FifoTopic"]) == "true" {
					name += ".fifo"
				}
			}
			attrs := map[string]string{}
			for _, k := range []string{"DisplayName", "FifoTopic", "ContentBasedDeduplication", "KmsMasterKeyId", "SignatureVersion", "TracingConfig"} {
				if v, ok := p[k]; ok {
					attrs[k] = str(v)
				}
			}
			var out struct{ TopicArn string }
			if err := c.call("AmazonSimpleNotificationService", "CreateTopic", map[string]any{"Name": name, "Attributes": attrs, "Tags": tagList(p["Tags"])}, &out); err != nil {
				return "", nil, err
			}
			for _, sub := range listMaps(p["Subscription"]) {
				if err := c.call("AmazonSimpleNotificationService", "Subscribe", map[string]any{"TopicArn": out.TopicArn, "Protocol": sub["Protocol"], "Endpoint": sub["Endpoint"]}, nil); err != nil {
					return "", nil, err
				}
			}
			return out.TopicArn, map[string]any{"TopicArn": out.TopicArn, "TopicName": name}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["TopicName"]) != str(old["TopicName"]) {
				return nil, true, nil
			}
			if v, ok := p["DisplayName"]; ok {
				c.call("AmazonSimpleNotificationService", "SetTopicAttributes", map[string]any{"TopicArn": phys, "AttributeName": "DisplayName", "AttributeValue": str(v)}, nil)
			}
			return map[string]any{"TopicArn": phys, "TopicName": lastSegment(phys)}, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AmazonSimpleNotificationService", "DeleteTopic", map[string]any{"TopicArn": phys}, nil)
		},
	})
	register("AWS::SNS::Subscription", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			attrs := map[string]string{}
			for _, k := range []string{"FilterPolicy", "FilterPolicyScope", "RawMessageDelivery", "RedrivePolicy", "DeliveryPolicy", "SubscriptionRoleArn"} {
				if v, ok := p[k]; ok {
					attrs[k] = jsonString(v)
				}
			}
			var out struct{ SubscriptionArn string }
			err := c.call("AmazonSimpleNotificationService", "Subscribe", map[string]any{"TopicArn": p["TopicArn"], "Protocol": p["Protocol"],
				"Endpoint": p["Endpoint"], "Attributes": attrs, "ReturnSubscriptionArn": true}, &out)
			return out.SubscriptionArn, map[string]any{"Arn": out.SubscriptionArn}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AmazonSimpleNotificationService", "Unsubscribe", map[string]any{"SubscriptionArn": phys}, nil)
		},
	})
	register("AWS::SNS::TopicPolicy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			for _, t := range list(p["Topics"]) {
				if err := c.call("AmazonSimpleNotificationService", "SetTopicAttributes", map[string]any{"TopicArn": t, "AttributeName": "Policy", "AttributeValue": jsonString(p["PolicyDocument"])}, nil); err != nil {
					return "", nil, err
				}
			}
			return c.name(64, false), nil, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error { return nil },
	})

	// ---- DynamoDB ----
	register("AWS::DynamoDB::Table", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			ttlName, ttlOn, err := ttlSpec(p) // before the table exists, so a bad one leaves nothing behind
			if err != nil {
				return "", nil, err
			}
			name := str(p["TableName"])
			if name == "" {
				name = c.name(255, false)
			}
			in := pick(p, "KeySchema", "AttributeDefinitions", "BillingMode", "ProvisionedThroughput", "GlobalSecondaryIndexes",
				"LocalSecondaryIndexes", "SSESpecification", "TableClass", "DeletionProtectionEnabled")
			in["TableName"] = name
			if s, ok := p["StreamSpecification"].(map[string]any); ok {
				in["StreamSpecification"] = map[string]any{"StreamEnabled": true, "StreamViewType": s["StreamViewType"]}
			}
			if t := tagList(p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			if in["BillingMode"] == nil && in["ProvisionedThroughput"] == nil {
				in["BillingMode"] = "PROVISIONED"
				in["ProvisionedThroughput"] = map[string]any{"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
			}
			var out struct {
				TableDescription struct{ TableArn, LatestStreamArn string }
			}
			if err := c.call("DynamoDB_20120810", "CreateTable", in, &out); err != nil {
				return "", nil, err
			}
			if ttlOn {
				if err := tableTTL(c, name, ttlName, true); err != nil {
					c.call("DynamoDB_20120810", "DeleteTable", map[string]any{"TableName": name}, nil)
					return "", nil, err
				}
			}
			if err := tablePITR(c, name, p); err != nil {
				return "", nil, err
			}
			attrs := map[string]any{"Arn": out.TableDescription.TableArn}
			if out.TableDescription.LatestStreamArn != "" {
				attrs["StreamArn"] = out.TableDescription.LatestStreamArn
			}
			return name, attrs, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if !equal(p["KeySchema"], old["KeySchema"]) || str(p["TableName"]) != str(old["TableName"]) || !equal(p["LocalSecondaryIndexes"], old["LocalSecondaryIndexes"]) {
				return nil, true, nil
			}
			oldName, oldOn, _ := ttlSpec(old)
			ttlName, ttlOn, err := ttlSpec(p)
			if err == nil && oldOn && ttlOn && oldName != ttlName {
				err = errors.New("Invalid request provided: Cannot change time-to-live attribute name. To update this property, you must first disable TTL then enable TTL with the new attribute name.")
			}
			if err != nil {
				return nil, false, err
			}
			in := pick(p, "BillingMode", "ProvisionedThroughput", "DeletionProtectionEnabled")
			in["TableName"] = phys
			if !equal(p["StreamSpecification"], old["StreamSpecification"]) {
				if s, ok := p["StreamSpecification"].(map[string]any); ok {
					in["StreamSpecification"] = map[string]any{"StreamEnabled": true, "StreamViewType": s["StreamViewType"]}
				} else {
					in["StreamSpecification"] = map[string]any{"StreamEnabled": false}
				}
			}
			var out struct {
				TableDescription struct{ TableArn, LatestStreamArn string }
			}
			if err := c.call("DynamoDB_20120810", "UpdateTable", in, &out); err != nil {
				return nil, false, err
			}
			if oldOn != ttlOn {
				if err := tableTTL(c, phys, cmp.Or(ttlName, oldName), ttlOn); err != nil {
					return nil, false, err
				}
			}
			if !equal(p["PointInTimeRecoverySpecification"], old["PointInTimeRecoverySpecification"]) {
				if err := tablePITR(c, phys, p); err != nil {
					return nil, false, err
				}
			}
			attrs := map[string]any{"Arn": out.TableDescription.TableArn}
			if out.TableDescription.LatestStreamArn != "" {
				attrs["StreamArn"] = out.TableDescription.LatestStreamArn
			}
			return attrs, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("DynamoDB_20120810", "DeleteTable", map[string]any{"TableName": phys}, nil)
		},
	})

	// ---- Lambda ----
	register("AWS::Lambda::Function", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["FunctionName"])
			if name == "" {
				name = c.name(64, false)
			}
			in := functionInput(p)
			in["FunctionName"] = name
			code, err := functionCode(p)
			if err != nil {
				return "", nil, err
			}
			in["Code"] = code
			var out struct{ FunctionArn string }
			if err := c.rest("lambda", "POST", "/2015-03-31/functions", in, &out); err != nil {
				return "", nil, err
			}
			if v, ok := p["ReservedConcurrentExecutions"]; ok {
				c.rest("lambda", "PUT", "/2017-10-31/functions/"+name+"/concurrency", map[string]any{"ReservedConcurrentExecutions": v}, nil)
			}
			return name, map[string]any{"Arn": out.FunctionArn}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["FunctionName"]) != str(old["FunctionName"]) {
				return nil, true, nil
			}
			if !equal(p["Code"], old["Code"]) {
				code, err := functionCode(p)
				if err != nil {
					return nil, false, err
				}
				if err := c.rest("lambda", "PUT", "/2015-03-31/functions/"+phys+"/code", code, nil); err != nil {
					return nil, false, err
				}
			}
			var out struct{ FunctionArn string }
			err := c.rest("lambda", "PUT", "/2015-03-31/functions/"+phys+"/configuration", functionInput(p), &out)
			return map[string]any{"Arn": out.FunctionArn}, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("lambda", "DELETE", "/2015-03-31/functions/"+phys, nil, nil)
		},
	})
	register("AWS::Lambda::Permission", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			sid := c.name(100, false)
			in := pick(p, "Action", "Principal", "SourceArn", "SourceAccount", "PrincipalOrgID", "FunctionUrlAuthType", "EventSourceToken")
			in["StatementId"] = sid
			return sid, nil, c.rest("lambda", "POST", "/2015-03-31/functions/"+url.PathEscape(str(p["FunctionName"]))+"/policy", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("lambda", "DELETE", "/2015-03-31/functions/"+url.PathEscape(str(p["FunctionName"]))+"/policy/"+phys, nil, nil)
		},
	})
	register("AWS::Lambda::EventSourceMapping", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "FunctionName", "EventSourceArn", "BatchSize", "StartingPosition", "StartingPositionTimestamp", "Enabled", "FilterCriteria",
				"FunctionResponseTypes", "MaximumBatchingWindowInSeconds", "MaximumRetryAttempts", "BisectBatchOnFunctionError", "DestinationConfig", "ScalingConfig")
			var out struct{ UUID string }
			err := c.rest("lambda", "POST", "/2015-03-31/event-source-mappings/", in, &out)
			return out.UUID, map[string]any{"Id": out.UUID}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["EventSourceArn"]) != str(old["EventSourceArn"]) {
				return nil, true, nil
			}
			in := pick(p, "FunctionName", "BatchSize", "Enabled", "FilterCriteria", "FunctionResponseTypes", "MaximumBatchingWindowInSeconds", "MaximumRetryAttempts", "DestinationConfig", "ScalingConfig")
			return map[string]any{"Id": phys}, false, c.rest("lambda", "PUT", "/2015-03-31/event-source-mappings/"+phys, in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("lambda", "DELETE", "/2015-03-31/event-source-mappings/"+phys, nil, nil)
		},
	})
	register("AWS::Lambda::Version", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct{ FunctionArn, Version string }
			err := c.rest("lambda", "POST", "/2015-03-31/functions/"+url.PathEscape(str(p["FunctionName"]))+"/versions", pick(p, "Description", "CodeSha256"), &out)
			return out.FunctionArn, map[string]any{"Version": out.Version, "FunctionArn": out.FunctionArn}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error { return nil }, // AWS retains versions
	})
	register("AWS::Lambda::Alias", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct{ AliasArn string }
			err := c.rest("lambda", "POST", "/2015-03-31/functions/"+url.PathEscape(str(p["FunctionName"]))+"/aliases",
				pick(p, "Name", "FunctionVersion", "Description", "RoutingConfig"), &out)
			return out.AliasArn, map[string]any{"AliasArn": out.AliasArn}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["Name"]) != str(old["Name"]) {
				return nil, true, nil
			}
			return map[string]any{"AliasArn": phys}, false, c.rest("lambda", "PUT", "/2015-03-31/functions/"+url.PathEscape(str(p["FunctionName"]))+"/aliases/"+str(p["Name"]),
				pick(p, "FunctionVersion", "Description", "RoutingConfig"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("lambda", "DELETE", "/2015-03-31/functions/"+url.PathEscape(str(p["FunctionName"]))+"/aliases/"+str(p["Name"]), nil, nil)
		},
	})
	register("AWS::Lambda::LayerVersion", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["LayerName"])
			if name == "" {
				name = c.name(64, false)
			}
			var out struct{ LayerVersionArn string }
			err := c.rest("lambda", "POST", "/2018-10-31/layers/"+name+"/versions", pick(p, "Content", "Description", "CompatibleRuntimes", "LicenseInfo", "CompatibleArchitectures"), &out)
			return out.LayerVersionArn, map[string]any{"LayerVersionArn": out.LayerVersionArn}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error { return nil },
	})
	register("AWS::Lambda::Url", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			fn := str(p["TargetFunctionArn"])
			var out struct{ FunctionUrl, FunctionArn string }
			err := c.rest("lambda", "POST", "/2021-10-31/functions/"+url.PathEscape(fn)+"/url", pick(p, "AuthType", "Cors", "InvokeMode"), &out)
			return out.FunctionArn, map[string]any{"FunctionUrl": out.FunctionUrl, "FunctionArn": out.FunctionArn}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("lambda", "DELETE", "/2021-10-31/functions/"+url.PathEscape(str(p["TargetFunctionArn"]))+"/url", nil, nil)
		},
	})

	// ---- IAM (global) ----
	const iam = "AWSIdentityManagementV20100508"
	register("AWS::IAM::Role", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["RoleName"])
			if name == "" {
				name = c.name(64, false)
			}
			in := pick(p, "Path", "Description", "MaxSessionDuration", "PermissionsBoundary")
			in["RoleName"], in["AssumeRolePolicyDocument"], in["Tags"] = name, jsonString(p["AssumeRolePolicyDocument"]), tagList(p["Tags"])
			var out struct{ Role struct{ Arn, RoleId string } }
			if err := c.call(iam, "CreateRole", in, &out); err != nil {
				return "", nil, err
			}
			if err := rolePolicies(c, name, nil, p); err != nil {
				return "", nil, err
			}
			return name, map[string]any{"Arn": out.Role.Arn, "RoleId": out.Role.RoleId}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["RoleName"]) != str(old["RoleName"]) || str(p["Path"]) != str(old["Path"]) {
				return nil, true, nil
			}
			c.call(iam, "UpdateAssumeRolePolicy", map[string]any{"RoleName": phys, "PolicyDocument": jsonString(p["AssumeRolePolicyDocument"])}, nil)
			if err := setBoundary(c, "Role", phys, old, p); err != nil {
				return nil, false, err
			}
			var out struct{ Role struct{ Arn, RoleId string } }
			c.call(iam, "GetRole", map[string]any{"RoleName": phys}, &out)
			return map[string]any{"Arn": out.Role.Arn, "RoleId": out.Role.RoleId}, false, rolePolicies(c, phys, old, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			rolePolicies(c, phys, p, map[string]any{})
			return c.call(iam, "DeleteRole", map[string]any{"RoleName": phys}, nil)
		},
	})
	register("AWS::IAM::Policy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return c.name(128, false), nil, inlinePolicy(c, "Put", p)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			inlinePolicy(c, "Delete", old)
			return nil, false, inlinePolicy(c, "Put", p)
		},
		del: func(c *pctx, phys string, p map[string]any) error { return inlinePolicy(c, "Delete", p) },
	})
	register("AWS::IAM::ManagedPolicy", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["ManagedPolicyName"])
			if name == "" {
				name = c.name(128, false)
			}
			in := pick(p, "Path", "Description")
			in["PolicyName"], in["PolicyDocument"] = name, jsonString(p["PolicyDocument"])
			var out struct{ Policy struct{ Arn string } }
			if err := c.call(iam, "CreatePolicy", in, &out); err != nil {
				return "", nil, err
			}
			return out.Policy.Arn, map[string]any{"PolicyArn": out.Policy.Arn}, attachManaged(c, "Attach", out.Policy.Arn, p)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["ManagedPolicyName"]) != str(old["ManagedPolicyName"]) {
				return nil, true, nil
			}
			if !equal(p["PolicyDocument"], old["PolicyDocument"]) {
				c.call(iam, "CreatePolicyVersion", map[string]any{"PolicyArn": phys, "PolicyDocument": jsonString(p["PolicyDocument"]), "SetAsDefault": true}, nil)
			}
			attachManaged(c, "Detach", phys, old)
			return map[string]any{"PolicyArn": phys}, false, attachManaged(c, "Attach", phys, p)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			attachManaged(c, "Detach", phys, p)
			return c.call(iam, "DeletePolicy", map[string]any{"PolicyArn": phys}, nil)
		},
	})
	register("AWS::IAM::User", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["UserName"])
			if name == "" {
				name = c.name(64, false)
			}
			var out struct{ User struct{ Arn string } }
			if err := c.call(iam, "CreateUser", map[string]any{"UserName": name, "Path": p["Path"], "Tags": tagList(p["Tags"]), "PermissionsBoundary": p["PermissionsBoundary"]}, &out); err != nil {
				return "", nil, err
			}
			userLinks(c, true, name, p)
			return name, map[string]any{"Arn": out.User.Arn}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["UserName"]) != str(old["UserName"]) || str(p["Path"]) != str(old["Path"]) {
				return nil, true, nil
			}
			if err := setBoundary(c, "User", phys, old, p); err != nil {
				return nil, false, err
			}
			userLinks(c, false, phys, old)
			userLinks(c, true, phys, p)
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			userLinks(c, false, phys, p)
			return c.call(iam, "DeleteUser", map[string]any{"UserName": phys}, nil)
		},
	})
	register("AWS::IAM::Group", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["GroupName"])
			if name == "" {
				name = c.name(128, false)
			}
			var out struct{ Group struct{ Arn string } }
			if err := c.call(iam, "CreateGroup", map[string]any{"GroupName": name, "Path": p["Path"]}, &out); err != nil {
				return "", nil, err
			}
			for _, arn := range list(p["ManagedPolicyArns"]) {
				c.call(iam, "AttachGroupPolicy", map[string]any{"GroupName": name, "PolicyArn": arn}, nil)
			}
			return name, map[string]any{"Arn": out.Group.Arn}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			for _, arn := range list(p["ManagedPolicyArns"]) {
				c.call(iam, "DetachGroupPolicy", map[string]any{"GroupName": phys, "PolicyArn": arn}, nil)
			}
			return c.call(iam, "DeleteGroup", map[string]any{"GroupName": phys}, nil)
		},
	})
	register("AWS::IAM::AccessKey", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				AccessKey struct{ AccessKeyId, SecretAccessKey string }
			}
			err := c.call(iam, "CreateAccessKey", map[string]any{"UserName": p["UserName"]}, &out)
			return out.AccessKey.AccessKeyId, map[string]any{"SecretAccessKey": out.AccessKey.SecretAccessKey}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(iam, "DeleteAccessKey", map[string]any{"UserName": p["UserName"], "AccessKeyId": phys}, nil)
		},
	})
	register("AWS::IAM::InstanceProfile", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["InstanceProfileName"])
			if name == "" {
				name = c.name(128, false)
			}
			var out struct{ InstanceProfile struct{ Arn string } }
			if err := c.call(iam, "CreateInstanceProfile", map[string]any{"InstanceProfileName": name, "Path": p["Path"]}, &out); err != nil {
				return "", nil, err
			}
			for _, r := range list(p["Roles"]) {
				c.call(iam, "AddRoleToInstanceProfile", map[string]any{"InstanceProfileName": name, "RoleName": r}, nil)
			}
			return name, map[string]any{"Arn": out.InstanceProfile.Arn}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			for _, r := range list(p["Roles"]) {
				c.call(iam, "RemoveRoleFromInstanceProfile", map[string]any{"InstanceProfileName": phys, "RoleName": r}, nil)
			}
			return c.call(iam, "DeleteInstanceProfile", map[string]any{"InstanceProfileName": phys}, nil)
		},
	})

	// ---- SSM, Secrets Manager, KMS, Logs ----
	register("AWS::SSM::Parameter", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["Name"])
			if name == "" {
				name = "CFN-" + c.logical + "-" + core.RandString(12, core.AlphaNum)
			}
			in := pick(p, "Type", "Value", "Description", "AllowedPattern", "Tier", "DataType")
			in["Name"], in["Tags"] = name, tagList(p["Tags"])
			err := c.call("AmazonSSM", "PutParameter", in, nil)
			return name, map[string]any{"Type": p["Type"], "Value": p["Value"]}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["Name"]) != str(old["Name"]) {
				return nil, true, nil
			}
			in := pick(p, "Type", "Value", "Description", "AllowedPattern", "Tier", "DataType")
			in["Name"], in["Overwrite"] = phys, true
			return map[string]any{"Type": p["Type"], "Value": p["Value"]}, false, c.call("AmazonSSM", "PutParameter", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AmazonSSM", "DeleteParameter", map[string]any{"Name": phys}, nil)
		},
	})
	register("AWS::SecretsManager::Secret", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["Name"])
			if name == "" {
				name = c.logical + "-" + core.RandString(12, core.AlphaNum)
			}
			in := pick(p, "Description", "KmsKeyId")
			in["Name"], in["Tags"] = name, tagList(p["Tags"])
			value, err := secretValue(c, p)
			if err != nil {
				return "", nil, err
			}
			in["SecretString"] = value
			var out struct{ ARN string }
			err = c.call("secretsmanager", "CreateSecret", in, &out)
			return out.ARN, map[string]any{"Id": out.ARN}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["Name"]) != str(old["Name"]) {
				return nil, true, nil
			}
			in := pick(p, "Description", "KmsKeyId")
			in["SecretId"] = phys
			if !equal(p["SecretString"], old["SecretString"]) || !equal(p["GenerateSecretString"], old["GenerateSecretString"]) {
				value, err := secretValue(c, p)
				if err != nil {
					return nil, false, err
				}
				in["SecretString"] = value
			}
			return map[string]any{"Id": phys}, false, c.call("secretsmanager", "UpdateSecret", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("secretsmanager", "DeleteSecret", map[string]any{"SecretId": phys, "ForceDeleteWithoutRecovery": true}, nil)
		},
	})
	register("AWS::KMS::Key", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Description", "KeySpec", "KeyUsage", "MultiRegion", "Origin")
			if v, ok := p["KeyPolicy"]; ok {
				in["Policy"] = jsonString(v)
			}
			var tags []map[string]string
			for _, t := range tagList(p["Tags"]) {
				tags = append(tags, map[string]string{"TagKey": t["Key"], "TagValue": t["Value"]})
			}
			in["Tags"] = tags
			var out struct{ KeyMetadata struct{ KeyId, Arn string } }
			if err := c.call("TrentService", "CreateKey", in, &out); err != nil {
				return "", nil, err
			}
			if str(p["EnableKeyRotation"]) == "true" {
				c.call("TrentService", "EnableKeyRotation", map[string]any{"KeyId": out.KeyMetadata.KeyId}, nil)
			}
			return out.KeyMetadata.KeyId, map[string]any{"Arn": out.KeyMetadata.Arn, "KeyId": out.KeyMetadata.KeyId}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["KeySpec"]) != str(old["KeySpec"]) || str(p["KeyUsage"]) != str(old["KeyUsage"]) {
				return nil, true, nil
			}
			if d, ok := p["Description"]; ok {
				c.call("TrentService", "UpdateKeyDescription", map[string]any{"KeyId": phys, "Description": d}, nil)
			}
			return map[string]any{"Arn": c.arn("kms", "key/"+phys), "KeyId": phys}, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("TrentService", "ScheduleKeyDeletion", map[string]any{"KeyId": phys, "PendingWindowInDays": 7}, nil)
		},
	})
	register("AWS::KMS::Alias", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["AliasName"]), nil, c.call("TrentService", "CreateAlias", pick(p, "AliasName", "TargetKeyId"), nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["AliasName"]) != phys {
				return nil, true, nil
			}
			return nil, false, c.call("TrentService", "UpdateAlias", pick(p, "AliasName", "TargetKeyId"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("TrentService", "DeleteAlias", map[string]any{"AliasName": phys}, nil)
		},
	})
	register("AWS::Logs::LogGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["LogGroupName"])
			if name == "" {
				name = c.name(512, false)
			}
			in := map[string]any{"logGroupName": name, "tags": tagMap(p["Tags"])}
			if k, ok := p["KmsKeyId"]; ok {
				in["kmsKeyId"] = k
			}
			if err := c.call("Logs_20140328", "CreateLogGroup", in, nil); err != nil {
				return "", nil, err
			}
			if r, ok := p["RetentionInDays"]; ok {
				c.call("Logs_20140328", "PutRetentionPolicy", map[string]any{"logGroupName": name, "retentionInDays": r}, nil)
			}
			return name, map[string]any{"Arn": c.arn("logs", "log-group:"+name+":*")}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["LogGroupName"]) != str(old["LogGroupName"]) {
				return nil, true, nil
			}
			if r, ok := p["RetentionInDays"]; ok {
				c.call("Logs_20140328", "PutRetentionPolicy", map[string]any{"logGroupName": phys, "retentionInDays": r}, nil)
			} else {
				c.call("Logs_20140328", "DeleteRetentionPolicy", map[string]any{"logGroupName": phys}, nil)
			}
			return map[string]any{"Arn": c.arn("logs", "log-group:"+phys+":*")}, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("Logs_20140328", "DeleteLogGroup", map[string]any{"logGroupName": phys}, nil)
		},
	})

	// ---- EventBridge, Step Functions, Kinesis, Firehose ----
	register("AWS::Events::EventBus", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct{ EventBusArn string }
			err := c.call("AWSEvents", "CreateEventBus", map[string]any{"Name": p["Name"], "Tags": tagList(p["Tags"])}, &out)
			return str(p["Name"]), map[string]any{"Arn": out.EventBusArn, "Name": p["Name"]}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AWSEvents", "DeleteEventBus", map[string]any{"Name": phys}, nil)
		},
	})
	register("AWS::Events::Rule", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) { return putRule(c, "", p) },
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["Name"]) != str(old["Name"]) || str(p["EventBusName"]) != str(old["EventBusName"]) {
				return nil, true, nil
			}
			removeTargets(c, phys, old)
			_, attrs, err := putRule(c, phys, p)
			return attrs, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			removeTargets(c, phys, p)
			bus, name := ruleParts(phys)
			return c.call("AWSEvents", "DeleteRule", map[string]any{"Name": name, "EventBusName": bus}, nil)
		},
	})
	register("AWS::StepFunctions::StateMachine", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["StateMachineName"])
			if name == "" {
				name = c.logical + "-" + core.RandString(12, core.AlphaNum)
			}
			in := map[string]any{"name": name, "definition": definition(p), "roleArn": p["RoleArn"], "type": p["StateMachineType"]}
			if l, ok := p["LoggingConfiguration"]; ok {
				in["loggingConfiguration"] = l
			}
			var tags []map[string]string
			for _, t := range tagList(p["Tags"]) {
				tags = append(tags, map[string]string{"key": t["Key"], "value": t["Value"]})
			}
			in["tags"] = tags
			var out struct{ StateMachineArn string }
			err := c.call("AWSStepFunctions", "CreateStateMachine", in, &out)
			return out.StateMachineArn, map[string]any{"Arn": out.StateMachineArn, "Name": name, "StateMachineRevisionId": "1"}, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["StateMachineName"]) != str(old["StateMachineName"]) || str(p["StateMachineType"]) != str(old["StateMachineType"]) {
				return nil, true, nil
			}
			err := c.call("AWSStepFunctions", "UpdateStateMachine", map[string]any{"stateMachineArn": phys, "definition": definition(p), "roleArn": p["RoleArn"]}, nil)
			return map[string]any{"Arn": phys, "Name": lastSegment(phys)}, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AWSStepFunctions", "DeleteStateMachine", map[string]any{"stateMachineArn": phys}, nil)
		},
	})
	register("AWS::Kinesis::Stream", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["Name"])
			if name == "" {
				name = c.name(128, false)
			}
			in := pick(p, "ShardCount", "StreamModeDetails")
			in["StreamName"] = name
			if in["ShardCount"] == nil && in["StreamModeDetails"] == nil {
				in["ShardCount"] = 1
			}
			if err := c.call("Kinesis_20131202", "CreateStream", in, nil); err != nil {
				return "", nil, err
			}
			if t := tagMap(p["Tags"]); len(t) > 0 {
				c.call("Kinesis_20131202", "AddTagsToStream", map[string]any{"StreamName": name, "Tags": t}, nil)
			}
			return name, map[string]any{"Arn": c.arn("kinesis", "stream/"+name)}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("Kinesis_20131202", "DeleteStream", map[string]any{"StreamName": phys, "EnforceConsumerDeletion": true}, nil)
		},
	})
	register("AWS::KinesisFirehose::DeliveryStream", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["DeliveryStreamName"])
			if name == "" {
				name = c.name(64, false)
			}
			in := pick(p, "DeliveryStreamType", "S3DestinationConfiguration", "ExtendedS3DestinationConfiguration", "HttpEndpointDestinationConfiguration",
				"KinesisStreamSourceConfiguration", "Tags")
			in["DeliveryStreamName"] = name
			var out struct{ DeliveryStreamARN string }
			err := c.call("Firehose_20150804", "CreateDeliveryStream", in, &out)
			return name, map[string]any{"Arn": out.DeliveryStreamARN}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("Firehose_20150804", "DeleteDeliveryStream", map[string]any{"DeliveryStreamName": phys}, nil)
		},
	})

	// ---- thin extras ----
	register("AWS::ECR::Repository", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["RepositoryName"])
			if name == "" {
				name = strings.ToLower(c.name(256, true))
			}
			in := pick(p, "ImageScanningConfiguration", "ImageTagMutability", "EncryptionConfiguration")
			in["repositoryName"], in["tags"] = name, tagList(p["Tags"])
			var out struct {
				Repository struct{ RepositoryArn, RepositoryUri string }
			}
			if err := c.call("AmazonEC2ContainerRegistry_V20150921", "CreateRepository", in, &out); err != nil {
				return "", nil, err
			}
			if lp, ok := p["LifecyclePolicy"].(map[string]any); ok {
				c.call("AmazonEC2ContainerRegistry_V20150921", "PutLifecyclePolicy", map[string]any{"repositoryName": name, "lifecyclePolicyText": lp["LifecyclePolicyText"]}, nil)
			}
			if pol, ok := p["RepositoryPolicyText"]; ok {
				c.call("AmazonEC2ContainerRegistry_V20150921", "SetRepositoryPolicy", map[string]any{"repositoryName": name, "policyText": jsonString(pol)}, nil)
			}
			return name, map[string]any{"Arn": out.Repository.RepositoryArn, "RepositoryUri": out.Repository.RepositoryUri}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AmazonEC2ContainerRegistry_V20150921", "DeleteRepository", map[string]any{"repositoryName": phys, "force": true}, nil)
		},
	})
	register("AWS::CloudWatch::Alarm", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := str(p["AlarmName"])
			if name == "" {
				name = c.name(255, false)
			}
			in := pick(p, "AlarmDescription", "ActionsEnabled", "OKActions", "AlarmActions", "InsufficientDataActions", "MetricName", "Namespace",
				"Statistic", "ExtendedStatistic", "Dimensions", "Period", "Unit", "EvaluationPeriods", "DatapointsToAlarm", "Threshold",
				"ComparisonOperator", "TreatMissingData", "Metrics")
			in["AlarmName"] = name
			err := c.call("GraniteServiceVersion20100801", "PutMetricAlarm", in, nil)
			return name, map[string]any{"Arn": c.arn("cloudwatch", "alarm:"+name)}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("GraniteServiceVersion20100801", "DeleteAlarms", map[string]any{"AlarmNames": []string{phys}}, nil)
		},
	})
	register("AWS::CertificateManager::Certificate", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct{ CertificateArn string }
			err := c.call("CertificateManager", "RequestCertificate", pick(p, "DomainName", "SubjectAlternativeNames", "ValidationMethod"), &out)
			return out.CertificateArn, nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("CertificateManager", "DeleteCertificate", map[string]any{"CertificateArn": phys}, nil)
		},
	})
	register("AWS::Cognito::UserPool", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Policies", "LambdaConfig", "AutoVerifiedAttributes", "AliasAttributes", "UsernameAttributes", "MfaConfiguration",
				"Schema", "UsernameConfiguration", "AccountRecoverySetting", "AdminCreateUserConfig", "VerificationMessageTemplate", "UserPoolTags=UserPoolTags")
			in["PoolName"] = str(p["UserPoolName"])
			if in["PoolName"] == "" {
				in["PoolName"] = c.name(128, false)
			}
			var out struct {
				UserPool struct{ Id, Arn, ProviderName, ProviderURL string }
			}
			if err := c.call("AWSCognitoIdentityProviderService", "CreateUserPool", in, &out); err != nil {
				return "", nil, err
			}
			u := out.UserPool
			return u.Id, map[string]any{"Arn": u.Arn, "ProviderName": u.ProviderName, "ProviderURL": u.ProviderURL, "UserPoolId": u.Id}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AWSCognitoIdentityProviderService", "DeleteUserPool", map[string]any{"UserPoolId": phys}, nil)
		},
	})
	register("AWS::Cognito::UserPoolClient", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "UserPoolId", "ClientName", "GenerateSecret", "ExplicitAuthFlows", "AllowedOAuthFlows", "AllowedOAuthScopes",
				"AllowedOAuthFlowsUserPoolClient", "CallbackURLs", "LogoutURLs", "SupportedIdentityProviders", "AccessTokenValidity",
				"IdTokenValidity", "RefreshTokenValidity", "TokenValidityUnits", "PreventUserExistenceErrors")
			if in["ClientName"] == nil {
				in["ClientName"] = c.logical
			}
			var out struct {
				UserPoolClient struct{ ClientId, ClientSecret string }
			}
			err := c.call("AWSCognitoIdentityProviderService", "CreateUserPoolClient", in, &out)
			return out.UserPoolClient.ClientId, map[string]any{"ClientId": out.UserPoolClient.ClientId, "ClientSecret": out.UserPoolClient.ClientSecret, "Name": in["ClientName"]}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call("AWSCognitoIdentityProviderService", "DeleteUserPoolClient", map[string]any{"UserPoolId": p["UserPoolId"], "ClientId": phys}, nil)
		},
	})

	// ---- API Gateway (REST v1 and HTTP v2) ----
	register("AWS::ApiGateway::RestApi", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Name=name", "Description=description", "BinaryMediaTypes=binaryMediaTypes", "EndpointConfiguration=endpointConfiguration")
			if in["name"] == nil {
				in["name"] = c.logical
			}
			var out struct {
				ID string `json:"id"`
			}
			if err := c.rest("apigateway", "POST", "/restapis", in, &out); err != nil {
				return "", nil, err
			}
			if body, ok := p["Body"]; ok {
				if err := c.rest("apigateway", "PUT", "/restapis/"+out.ID+"?mode=overwrite", []byte(jsonString(body)), nil); err != nil {
					return "", nil, err
				}
			}
			return out.ID, restAPIAttrs(c, out.ID), nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if body, ok := p["Body"]; ok && !equal(body, old["Body"]) {
				if err := c.rest("apigateway", "PUT", "/restapis/"+phys+"?mode=overwrite", []byte(jsonString(body)), nil); err != nil {
					return nil, false, err
				}
			}
			return restAPIAttrs(c, phys), false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGateway::Resource", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				ID string `json:"id"`
			}
			err := c.rest("apigateway", "POST", "/restapis/"+str(p["RestApiId"])+"/resources/"+str(p["ParentId"]), map[string]any{"pathPart": p["PathPart"]}, &out)
			return out.ID, map[string]any{"ResourceId": out.ID}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/resources/"+phys, nil, nil)
		},
	})
	putMethod := func(c *pctx, p map[string]any) error {
		base := "/restapis/" + str(p["RestApiId"]) + "/resources/" + str(p["ResourceId"]) + "/methods/" + str(p["HttpMethod"])
		if err := c.rest("apigateway", "PUT", base, map[string]any{"authorizationType": or(str(p["AuthorizationType"]), "NONE"),
			"authorizerId": p["AuthorizerId"], "apiKeyRequired": p["ApiKeyRequired"], "requestParameters": p["RequestParameters"],
			"requestValidatorId": p["RequestValidatorId"], "requestModels": p["RequestModels"], "operationName": p["OperationName"],
			"authorizationScopes": p["AuthorizationScopes"]}, nil); err != nil {
			return err
		}
		if ig, ok := p["Integration"].(map[string]any); ok {
			in := map[string]any{"type": ig["Type"], "uri": ig["Uri"], "integrationHttpMethod": ig["IntegrationHttpMethod"],
				"requestTemplates": ig["RequestTemplates"], "passthroughBehavior": ig["PassthroughBehavior"], "requestParameters": ig["RequestParameters"]}
			if err := c.rest("apigateway", "PUT", base+"/integration", in, nil); err != nil {
				return err
			}
			for _, ir := range listMaps(ig["IntegrationResponses"]) {
				c.rest("apigateway", "PUT", base+"/integration/responses/"+or(str(ir["StatusCode"]), "200"), map[string]any{
					"selectionPattern": ir["SelectionPattern"], "responseTemplates": ir["ResponseTemplates"], "responseParameters": ir["ResponseParameters"]}, nil)
			}
		}
		for _, mr := range listMaps(p["MethodResponses"]) {
			c.rest("apigateway", "PUT", base+"/responses/"+or(str(mr["StatusCode"]), "200"), map[string]any{"responseModels": mr["ResponseModels"], "responseParameters": mr["ResponseParameters"]}, nil)
		}
		return nil
	}
	delMethod := func(c *pctx, p map[string]any) error {
		return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/resources/"+str(p["ResourceId"])+"/methods/"+str(p["HttpMethod"]), nil, nil)
	}
	register("AWS::ApiGateway::Method", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return c.name(64, false), nil, putMethod(c, p)
		},
		// The method id is the API, resource and verb, so a change to anything else
		// is in place: drop the method and put it again. A new id is a replacement.
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			for _, k := range []string{"RestApiId", "ResourceId", "HttpMethod"} {
				if str(p[k]) != str(old[k]) {
					return nil, true, nil
				}
			}
			if err := delMethod(c, old); err != nil {
				return nil, false, err
			}
			if err := putMethod(c, p); err != nil {
				putMethod(c, old) // keep the old method
				return nil, false, err
			}
			return nil, false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error { return delMethod(c, p) },
	})
	register("AWS::ApiGateway::Deployment", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				ID string `json:"id"`
			}
			err := c.rest("apigateway", "POST", "/restapis/"+str(p["RestApiId"])+"/deployments", map[string]any{"stageName": p["StageName"], "description": p["Description"]}, &out)
			return out.ID, map[string]any{"DeploymentId": out.ID}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/deployments/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGateway::Stage", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["StageName"]), c.logical)
			err := c.rest("apigateway", "POST", "/restapis/"+str(p["RestApiId"])+"/stages", map[string]any{"stageName": name, "deploymentId": p["DeploymentId"],
				"variables": p["Variables"], "description": p["Description"]}, nil)
			return name, nil, err
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(p["StageName"]) != str(old["StageName"]) {
				return nil, true, nil
			}
			ops := []map[string]any{{"op": "replace", "path": "/deploymentId", "value": p["DeploymentId"]}}
			return nil, false, c.rest("apigateway", "PATCH", "/restapis/"+str(p["RestApiId"])+"/stages/"+phys, map[string]any{"patchOperations": ops}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/restapis/"+str(p["RestApiId"])+"/stages/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGatewayV2::Api", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "Name=name", "ProtocolType=protocolType", "Target=target", "RouteKey=routeKey", "CorsConfiguration=corsConfiguration",
				"Description=description", "RouteSelectionExpression=routeSelectionExpression", "Tags=tags")
			if in["name"] == nil {
				in["name"] = c.logical
			}
			var out struct {
				APIID       string `json:"apiId"`
				APIEndpoint string `json:"apiEndpoint"`
			}
			err := c.rest("apigateway", "POST", "/v2/apis", in, &out)
			return out.APIID, map[string]any{"ApiId": out.APIID, "ApiEndpoint": out.APIEndpoint}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/apis/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGatewayV2::Integration", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "IntegrationType=integrationType", "IntegrationUri=integrationUri", "IntegrationMethod=integrationMethod",
				"PayloadFormatVersion=payloadFormatVersion", "TimeoutInMillis=timeoutInMillis")
			var out struct {
				ID string `json:"integrationId"`
			}
			err := c.rest("apigateway", "POST", "/v2/apis/"+str(p["ApiId"])+"/integrations", in, &out)
			return out.ID, map[string]any{"IntegrationId": out.ID}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/apis/"+str(p["ApiId"])+"/integrations/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGatewayV2::Route", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := pick(p, "RouteKey=routeKey", "Target=target", "AuthorizationType=authorizationType", "AuthorizerId=authorizerId")
			var out struct {
				ID string `json:"routeId"`
			}
			err := c.rest("apigateway", "POST", "/v2/apis/"+str(p["ApiId"])+"/routes", in, &out)
			return out.ID, map[string]any{"RouteId": out.ID}, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/apis/"+str(p["ApiId"])+"/routes/"+phys, nil, nil)
		},
	})
	register("AWS::ApiGatewayV2::Stage", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["StageName"]), "$default")
			err := c.rest("apigateway", "POST", "/v2/apis/"+str(p["ApiId"])+"/stages", map[string]any{"stageName": name, "autoDeploy": p["AutoDeploy"],
				"stageVariables": p["StageVariables"], "deploymentId": p["DeploymentId"]}, nil)
			return name, nil, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("apigateway", "DELETE", "/v2/apis/"+str(p["ApiId"])+"/stages/"+url.PathEscape(phys), nil, nil)
		},
	})
}

// ---- helpers ----

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// userLinks adds (or removes) a user's groups and managed policies.
func userLinks(c *pctx, add bool, user string, p map[string]any) {
	const iam = "AWSIdentityManagementV20100508"
	op := map[bool][2]string{true: {"AddUserToGroup", "AttachUserPolicy"}, false: {"RemoveUserFromGroup", "DetachUserPolicy"}}[add]
	for _, g := range list(p["Groups"]) {
		c.call(iam, op[0], map[string]any{"UserName": user, "GroupName": g}, nil)
	}
	for _, arn := range list(p["ManagedPolicyArns"]) {
		c.call(iam, op[1], map[string]any{"UserName": user, "PolicyArn": arn}, nil)
	}
}

// setBoundary makes a role's or user's permissions boundary match p (kind is "Role" or "User").
func setBoundary(c *pctx, kind, name string, old, p map[string]any) error {
	const iam = "AWSIdentityManagementV20100508"
	want := str(p["PermissionsBoundary"])
	if want == str(old["PermissionsBoundary"]) {
		return nil
	}
	if want == "" {
		return c.call(iam, "Delete"+kind+"PermissionsBoundary", map[string]any{kind + "Name": name}, nil)
	}
	return c.call(iam, "Put"+kind+"PermissionsBoundary", map[string]any{kind + "Name": name, "PermissionsBoundary": want}, nil)
}

func list(v any) []string {
	var out []string
	switch x := v.(type) {
	case []any:
		for _, i := range x {
			out = append(out, str(i))
		}
	case string:
		out = []string{x}
	}
	return out
}

func listMaps(v any) []map[string]any {
	var out []map[string]any
	if l, ok := v.([]any); ok {
		for _, i := range l {
			if m, ok := i.(map[string]any); ok {
				out = append(out, m)
			}
		}
	}
	return out
}

func bucketAttrs(c *pctx, name string) map[string]any {
	suffix := core.URLSuffix(c.region)
	return map[string]any{"Arn": core.GlobalARN("s3", c.region, "", name), "DomainName": name + ".s3." + suffix,
		"RegionalDomainName": name + ".s3." + c.region + "." + suffix, "DualStackDomainName": name + ".s3.dualstack." + c.region + "." + suffix,
		"WebsiteURL": "http://" + name + ".s3-website-" + c.region + "." + suffix}
}

// configureBucket applies the bucket sub-configurations CloudFormation exposes.
// configureBucket applies a bucket's settings; old (nil on create) lets it remove
// the ones an update dropped, for those with a delete call.
func configureBucket(c *pctx, name string, old, p map[string]any) error {
	put := func(sub, body string) error { return c.rest("s3", "PUT", "/"+name+"?"+sub, []byte(body), nil) }
	for _, sub := range [][2]string{{"OwnershipControls", "ownershipControls"}, {"LifecycleConfiguration", "lifecycle"}} {
		if _, was := old[sub[0]]; was && p[sub[0]] == nil {
			c.rest("s3", "DELETE", "/"+name+"?"+sub[1], nil, nil)
		}
	}
	if oc, ok := p["OwnershipControls"].(map[string]any); ok {
		var b strings.Builder
		b.WriteString("<OwnershipControls>")
		for _, r := range listMaps(oc["Rules"]) {
			b.WriteString("<Rule><ObjectOwnership>" + xmlEsc(str(r["ObjectOwnership"])) + "</ObjectOwnership></Rule>")
		}
		b.WriteString("</OwnershipControls>")
		if err := put("ownershipControls", b.String()); err != nil {
			return err
		}
	}
	if lc, ok := p["LifecycleConfiguration"].(map[string]any); ok {
		if err := put("lifecycle", lifecycleXML(lc)); err != nil {
			return err
		}
	}
	if v, ok := p["VersioningConfiguration"].(map[string]any); ok {
		if err := put("versioning", `<VersioningConfiguration><Status>`+str(v["Status"])+`</Status></VersioningConfiguration>`); err != nil {
			return err
		}
	}
	if t := tagList(p["Tags"]); len(t) > 0 {
		var b strings.Builder
		b.WriteString("<Tagging><TagSet>")
		for _, tag := range t {
			b.WriteString("<Tag><Key>" + xmlEsc(tag["Key"]) + "</Key><Value>" + xmlEsc(tag["Value"]) + "</Value></Tag>")
		}
		b.WriteString("</TagSet></Tagging>")
		put("tagging", b.String())
	}
	if cors, ok := p["CorsConfiguration"].(map[string]any); ok {
		var b strings.Builder
		b.WriteString("<CORSConfiguration>")
		for _, r := range listMaps(cors["CorsRules"]) {
			b.WriteString("<CORSRule>")
			for _, field := range [][2]string{{"AllowedOrigins", "AllowedOrigin"}, {"AllowedMethods", "AllowedMethod"}, {"AllowedHeaders", "AllowedHeader"}, {"ExposedHeaders", "ExposeHeader"}} {
				for _, v := range list(r[field[0]]) {
					b.WriteString("<" + field[1] + ">" + xmlEsc(v) + "</" + field[1] + ">")
				}
			}
			if m := str(r["MaxAge"]); m != "" {
				b.WriteString("<MaxAgeSeconds>" + m + "</MaxAgeSeconds>")
			}
			b.WriteString("</CORSRule>")
		}
		b.WriteString("</CORSConfiguration>")
		put("cors", b.String())
	}
	if n, ok := p["NotificationConfiguration"].(map[string]any); ok {
		var b strings.Builder
		b.WriteString("<NotificationConfiguration>")
		for _, kind := range [][3]string{{"QueueConfigurations", "QueueConfiguration", "Queue"}, {"TopicConfigurations", "TopicConfiguration", "Topic"}, {"LambdaConfigurations", "CloudFunctionConfiguration", "CloudFunction"}} {
			srcKey := map[string]string{"Queue": "Queue", "Topic": "Topic", "CloudFunction": "Function"}[kind[2]]
			for _, cfg := range listMaps(n[kind[0]]) {
				b.WriteString("<" + kind[1] + "><" + kind[2] + ">" + xmlEsc(str(cfg[srcKey])) + "</" + kind[2] + "><Event>" + xmlEsc(str(cfg["Event"])) + "</Event>")
				if f, ok := cfg["Filter"].(map[string]any); ok {
					if k, ok := f["S3Key"].(map[string]any); ok {
						b.WriteString("<Filter><S3Key>")
						for _, rule := range listMaps(k["Rules"]) {
							b.WriteString("<FilterRule><Name>" + xmlEsc(str(rule["Name"])) + "</Name><Value>" + xmlEsc(str(rule["Value"])) + "</Value></FilterRule>")
						}
						b.WriteString("</S3Key></Filter>")
					}
				}
				b.WriteString("</" + kind[1] + ">")
			}
		}
		if eb, ok := n["EventBridgeConfiguration"].(map[string]any); ok && str(eb["EventBridgeEnabled"]) == "true" {
			b.WriteString("<EventBridgeConfiguration></EventBridgeConfiguration>")
		}
		b.WriteString("</NotificationConfiguration>")
		if err := put("notification", b.String()); err != nil {
			return err
		}
	}
	if pab, ok := p["PublicAccessBlockConfiguration"].(map[string]any); ok {
		var b strings.Builder
		b.WriteString("<PublicAccessBlockConfiguration>")
		for _, k := range core.SortedKeys(pab) {
			b.WriteString("<" + k + ">" + str(pab[k]) + "</" + k + ">")
		}
		b.WriteString("</PublicAccessBlockConfiguration>")
		put("publicAccessBlock", b.String())
	}
	if raw, ok := p["BucketEncryption"]; ok {
		_ = raw // SSE is always on in pogi; the default configuration is returned
	}
	return nil
}

// lifecycleXML turns a template's LifecycleConfiguration into the S3 document.
func lifecycleXML(lc map[string]any) string {
	var b strings.Builder
	el := func(name, v string) {
		if v != "" {
			b.WriteString("<" + name + ">" + xmlEsc(v) + "</" + name + ">")
		}
	}
	tag := func(t map[string]any) string {
		return "<Tag><Key>" + xmlEsc(str(t["Key"])) + "</Key><Value>" + xmlEsc(str(t["Value"])) + "</Value></Tag>"
	}
	// transitions: the one-item form (key) and the list form (key+"s")
	each := func(r map[string]any, key string, fn func(t map[string]any)) {
		items := listMaps(r[key+"s"])
		if one, ok := r[key].(map[string]any); ok {
			items = append(items, one)
		}
		for _, t := range items {
			fn(t)
		}
	}
	b.WriteString("<LifecycleConfiguration>")
	for _, r := range listMaps(lc["Rules"]) {
		b.WriteString("<Rule>")
		el("ID", str(r["Id"]))
		el("Status", str(r["Status"]))
		tags, prefix := listMaps(r["TagFilters"]), str(r["Prefix"])
		minSize, maxSize := str(r["ObjectSizeGreaterThan"]), str(r["ObjectSizeLessThan"])
		b.WriteString("<Filter>")
		switch {
		case len(tags) == 0 && minSize == "" && maxSize == "":
			el("Prefix", prefix)
		case len(tags) == 1 && prefix == "" && minSize == "" && maxSize == "":
			b.WriteString(tag(tags[0]))
		default:
			b.WriteString("<And>")
			el("Prefix", prefix)
			for _, t := range tags {
				b.WriteString(tag(t))
			}
			el("ObjectSizeGreaterThan", minSize)
			el("ObjectSizeLessThan", maxSize)
			b.WriteString("</And>")
		}
		b.WriteString("</Filter>")
		if r["ExpirationInDays"] != nil || r["ExpirationDate"] != nil || r["ExpiredObjectDeleteMarker"] != nil {
			b.WriteString("<Expiration>")
			el("Days", str(r["ExpirationInDays"]))
			el("Date", str(r["ExpirationDate"]))
			el("ExpiredObjectDeleteMarker", str(r["ExpiredObjectDeleteMarker"]))
			b.WriteString("</Expiration>")
		}
		each(r, "Transition", func(t map[string]any) {
			b.WriteString("<Transition>")
			el("Days", str(t["TransitionInDays"]))
			el("Date", str(t["TransitionDate"]))
			el("StorageClass", str(t["StorageClass"]))
			b.WriteString("</Transition>")
		})
		each(r, "NoncurrentVersionTransition", func(t map[string]any) {
			b.WriteString("<NoncurrentVersionTransition>")
			el("NoncurrentDays", str(t["TransitionInDays"]))
			el("NewerNoncurrentVersions", str(t["NewerNoncurrentVersions"]))
			el("StorageClass", str(t["StorageClass"]))
			b.WriteString("</NoncurrentVersionTransition>")
		})
		nve, _ := r["NoncurrentVersionExpiration"].(map[string]any)
		if days := str(r["NoncurrentVersionExpirationInDays"]); days != "" || nve != nil {
			b.WriteString("<NoncurrentVersionExpiration>")
			el("NoncurrentDays", cmp.Or(str(nve["NoncurrentDays"]), days))
			el("NewerNoncurrentVersions", str(nve["NewerNoncurrentVersions"]))
			b.WriteString("</NoncurrentVersionExpiration>")
		}
		if ab, ok := r["AbortIncompleteMultipartUpload"].(map[string]any); ok {
			b.WriteString("<AbortIncompleteMultipartUpload>")
			el("DaysAfterInitiation", str(ab["DaysAfterInitiation"]))
			b.WriteString("</AbortIncompleteMultipartUpload>")
		}
		b.WriteString("</Rule>")
	}
	b.WriteString("</LifecycleConfiguration>")
	return b.String()
}

func xmlEsc(s string) string {
	var b bytes.Buffer
	xml.EscapeText(&b, []byte(s))
	return b.String()
}

func queueAttrs(p map[string]any) map[string]string {
	out := map[string]string{}
	for _, k := range []string{"DelaySeconds", "MaximumMessageSize", "MessageRetentionPeriod", "ReceiveMessageWaitTimeSeconds", "VisibilityTimeout",
		"FifoQueue", "ContentBasedDeduplication", "KmsMasterKeyId", "KmsDataKeyReusePeriodSeconds", "SqsManagedSseEnabled", "DeduplicationScope",
		"FifoThroughputLimit"} {
		if v, ok := p[k]; ok {
			out[k] = str(v)
		}
	}
	for _, k := range []string{"RedrivePolicy", "RedriveAllowPolicy"} {
		if v, ok := p[k]; ok {
			out[k] = jsonString(v)
		}
	}
	return out
}

func functionInput(p map[string]any) map[string]any {
	in := pick(p, "Runtime", "Handler", "Role", "Description", "Timeout", "MemorySize", "Environment", "Layers", "Architectures",
		"DeadLetterConfig", "TracingConfig", "EphemeralStorage", "VpcConfig", "PackageType", "ImageConfig", "KmsKeyArn", "LoggingConfig")
	if t := tagMap(p["Tags"]); len(t) > 0 {
		in["Tags"] = t
	}
	return in
}

// functionCode turns CloudFormation's Code (incl. inline ZipFile source) into Lambda's.
func functionCode(p map[string]any) (map[string]any, error) {
	code, _ := p["Code"].(map[string]any)
	if src, ok := code["ZipFile"].(string); ok {
		ext := ".js"
		if strings.HasPrefix(str(p["Runtime"]), "python") {
			ext = ".py"
		}
		var buf bytes.Buffer
		zw := zip.NewWriter(&buf)
		w, _ := zw.Create("index" + ext)
		w.Write([]byte(src))
		zw.Close()
		return map[string]any{"ZipFile": buf.Bytes()}, nil
	}
	out := pick(code, "S3Bucket", "S3Key", "S3ObjectVersion", "ImageUri")
	if len(out) == 0 {
		return nil, fmt.Errorf("Code needs ZipFile, S3Bucket/S3Key or ImageUri")
	}
	return out, nil
}

// rolePolicies syncs a role's managed and inline policies from old to new props.
func rolePolicies(c *pctx, role string, old, p map[string]any) error {
	const iam = "AWSIdentityManagementV20100508"
	for _, arn := range list(old["ManagedPolicyArns"]) {
		c.call(iam, "DetachRolePolicy", map[string]any{"RoleName": role, "PolicyArn": arn}, nil)
	}
	for _, pol := range listMaps(old["Policies"]) {
		c.call(iam, "DeleteRolePolicy", map[string]any{"RoleName": role, "PolicyName": pol["PolicyName"]}, nil)
	}
	for _, arn := range list(p["ManagedPolicyArns"]) {
		if err := c.call(iam, "AttachRolePolicy", map[string]any{"RoleName": role, "PolicyArn": arn}, nil); err != nil {
			return err
		}
	}
	for _, pol := range listMaps(p["Policies"]) {
		if err := c.call(iam, "PutRolePolicy", map[string]any{"RoleName": role, "PolicyName": pol["PolicyName"], "PolicyDocument": jsonString(pol["PolicyDocument"])}, nil); err != nil {
			return err
		}
	}
	return nil
}

func inlinePolicy(c *pctx, verb string, p map[string]any) error {
	const iam = "AWSIdentityManagementV20100508"
	doc := jsonString(p["PolicyDocument"])
	for _, kind := range [][2]string{{"Roles", "Role"}, {"Users", "User"}, {"Groups", "Group"}} {
		for _, name := range list(p[kind[0]]) {
			in := map[string]any{kind[1] + "Name": name, "PolicyName": p["PolicyName"]}
			if verb == "Put" {
				in["PolicyDocument"] = doc
			}
			if err := c.call(iam, verb+kind[1]+"Policy", in, nil); err != nil && verb == "Put" {
				return err
			}
		}
	}
	return nil
}

func attachManaged(c *pctx, verb, arn string, p map[string]any) error {
	const iam = "AWSIdentityManagementV20100508"
	for _, kind := range [][2]string{{"Roles", "Role"}, {"Users", "User"}, {"Groups", "Group"}} {
		for _, name := range list(p[kind[0]]) {
			if err := c.call(iam, verb+kind[1]+"Policy", map[string]any{kind[1] + "Name": name, "PolicyArn": arn}, nil); err != nil && verb == "Attach" {
				return err
			}
		}
	}
	return nil
}

// secretValue honours SecretString or GenerateSecretString.
func secretValue(c *pctx, p map[string]any) (string, error) {
	if v, ok := p["SecretString"]; ok {
		return str(v), nil
	}
	g, ok := p["GenerateSecretString"].(map[string]any)
	if !ok {
		return "", nil
	}
	in := pick(g, "PasswordLength", "ExcludeCharacters", "ExcludeNumbers", "ExcludePunctuation", "ExcludeUppercase", "ExcludeLowercase",
		"IncludeSpace", "RequireEachIncludedType")
	var out struct{ RandomPassword string }
	if err := c.call("secretsmanager", "GetRandomPassword", in, &out); err != nil {
		out.RandomPassword = core.RandString(32, core.AlphaNum)
	}
	if tmpl := str(g["SecretStringTemplate"]); tmpl != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(tmpl), &m); err != nil {
			return "", fmt.Errorf("SecretStringTemplate must be JSON")
		}
		m[or(str(g["GenerateStringKey"]), "password")] = out.RandomPassword
		b, _ := json.Marshal(m)
		return string(b), nil
	}
	return out.RandomPassword, nil
}

// definition renders a state machine definition with DefinitionSubstitutions.
func definition(p map[string]any) string {
	def := str(p["DefinitionString"])
	if d, ok := p["Definition"]; ok {
		def = jsonString(d)
	}
	subs, _ := p["DefinitionSubstitutions"].(map[string]any)
	keys := core.SortedKeys(subs)
	sort.Slice(keys, func(i, j int) bool { return len(keys[i]) > len(keys[j]) })
	for _, k := range keys {
		def = strings.ReplaceAll(def, "${"+k+"}", str(subs[k]))
	}
	return def
}

// rule ids are "bus|name" for non-default buses.
func ruleParts(phys string) (bus, name string) {
	if b, n, ok := strings.Cut(phys, "|"); ok {
		return b, n
	}
	return "default", phys
}

func putRule(c *pctx, phys string, p map[string]any) (string, map[string]any, error) {
	name := str(p["Name"])
	if name == "" {
		_, name = ruleParts(phys)
	}
	if name == "" {
		name = c.name(64, false)
	}
	bus := or(lastSegment(str(p["EventBusName"])), "default")
	in := pick(p, "Description", "ScheduleExpression", "State", "RoleArn")
	in["Name"], in["EventBusName"], in["Tags"] = name, bus, tagList(p["Tags"])
	if ep, ok := p["EventPattern"]; ok {
		in["EventPattern"] = jsonString(ep)
	}
	var out struct{ RuleArn string }
	if err := c.call("AWSEvents", "PutRule", in, &out); err != nil {
		return "", nil, err
	}
	if targets := listMaps(p["Targets"]); len(targets) > 0 {
		for _, t := range targets {
			if it, ok := t["InputTransformer"].(map[string]any); ok {
				if tmpl, ok := it["InputTemplate"]; ok {
					it["InputTemplate"] = str(tmpl)
				}
			}
		}
		if err := c.call("AWSEvents", "PutTargets", map[string]any{"Rule": name, "EventBusName": bus, "Targets": targets}, nil); err != nil {
			return "", nil, err
		}
	}
	id := name
	if bus != "default" {
		id = bus + "|" + name
	}
	return id, map[string]any{"Arn": out.RuleArn}, nil
}

func removeTargets(c *pctx, phys string, p map[string]any) {
	var ids []string
	for _, t := range listMaps(p["Targets"]) {
		ids = append(ids, str(t["Id"]))
	}
	if len(ids) > 0 {
		bus, name := ruleParts(phys)
		c.call("AWSEvents", "RemoveTargets", map[string]any{"Rule": name, "EventBusName": bus, "Ids": ids}, nil)
	}
}

func restAPIAttrs(c *pctx, id string) map[string]any {
	var out struct {
		Items []struct {
			ID   string `json:"id"`
			Path string `json:"path"`
		} `json:"item"` // API Gateway v1 lists under "item"
	}
	c.rest("apigateway", "GET", "/restapis/"+id+"/resources", nil, &out)
	root := ""
	for _, it := range out.Items {
		if it.Path == "/" {
			root = it.ID
		}
	}
	return map[string]any{"RestApiId": id, "RootResourceId": root}
}

// ---- nested stacks ----

var nestedStack = &provider{
	create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
		return c.s.nested(c, "", p)
	},
	update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
		_, attrs, err := c.s.nested(c, phys, p)
		return attrs, false, err
	},
	del: func(c *pctx, phys string, p map[string]any) error {
		c.s.mu.Lock()
		var child *Stack
		for _, st := range c.s.st.Stacks {
			if st.ID == phys {
				child = st
			}
		}
		c.s.mu.Unlock()
		if child == nil || !child.Deleted.IsZero() {
			return nil
		}
		c.s.runDelete(child, nil)
		if child.Status != "DELETE_COMPLETE" {
			return fmt.Errorf("nested stack %s: %s", child.Name, child.Reason)
		}
		return nil
	},
}

// nested deploys a child stack synchronously and exposes its outputs as Outputs.X.
func (s *Service) nested(c *pctx, phys string, p map[string]any) (string, map[string]any, error) {
	body, err := s.templateBody("", str(p["TemplateURL"]))
	if err != nil {
		return "", nil, err
	}
	tmpl, err := c.s.template(body)
	if err != nil {
		return "", nil, err
	}
	var inputs []paramInput
	if params, ok := p["Parameters"].(map[string]any); ok {
		for _, k := range core.SortedKeys(params) {
			inputs = append(inputs, paramInput{ParameterKey: k, ParameterValue: str(params[k])})
		}
	}
	ctx := &core.Ctx{Account: c.account, Region: c.region}
	s.mu.Lock()
	var child *Stack
	for _, st := range s.st.Stacks {
		if st.ID == phys {
			child = st
		}
	}
	if child == nil {
		child = s.newStack(ctx, c.stack+"-"+c.logical+"-"+core.RandString(12, core.UpperNum), body, tagList2(p["Tags"]), []string{"CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"})
		child.Parent = c.stackID
	}
	prev := child.Params
	s.mu.Unlock()
	values, resolved, err := s.params(ctx, tmpl, inputs, prev)
	if err != nil {
		return "", nil, err
	}
	if phys == "" {
		s.mu.Lock()
		child.Body, child.Params = body, resolved
		s.mu.Unlock()
		s.runCreate(child, tmpl, values)
	} else {
		s.runUpdate(child, body, resolved, tmpl, values)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !strings.HasSuffix(child.Status, "_COMPLETE") || strings.Contains(child.Status, "ROLLBACK") {
		return child.ID, nil, fmt.Errorf("nested stack %s failed: %s", child.Name, child.Reason)
	}
	attrs := map[string]any{}
	for _, o := range child.Outputs {
		attrs["Outputs."+o.Key] = o.Value
	}
	return child.ID, attrs, nil
}

func tagList2(v any) []Tag {
	var out []Tag
	for _, t := range tagList(v) {
		out = append(out, Tag{t["Key"], t["Value"]})
	}
	return out
}

var _ = time.Now
