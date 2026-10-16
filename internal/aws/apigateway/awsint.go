package apigateway

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// jsonServices are the AWS services that take their action in X-Amz-Target
// (the JSON protocols). Any other service gets Action in the query string, as
// the query protocols (SQS, SNS) read it.
var jsonServices = map[string]struct{ target, contentType string }{
	"dynamodb":       {"DynamoDB_20120810", "application/x-amz-json-1.0"},
	"states":         {"AWSStepFunctions", "application/x-amz-json-1.0"},
	"kinesis":        {"Kinesis_20131202", "application/x-amz-json-1.1"},
	"firehose":       {"Firehose_20150804", "application/x-amz-json-1.1"},
	"events":         {"AWSEvents", "application/x-amz-json-1.1"},
	"logs":           {"Logs_20140328", "application/x-amz-json-1.1"},
	"ssm":            {"AmazonSSM", "application/x-amz-json-1.1"},
	"secretsmanager": {"secretsmanager", "application/x-amz-json-1.1"},
}

// awsService runs an AWS integration to a service other than Lambda
// (arn:aws:apigateway:<region>:<service>:path/<path> or :action/<Action>).
// The request goes to the service inside pogi as the API's account; its
// answer is returned for the integration response to pick by status code.
// Like AWS, only the headers, query strings and paths that the integration's
// requestParameters name are sent, plus the rendered body.
// ponytail: the credentials role is not assumed and its permissions are not checked.
func (s *Service) awsService(x *call, in *Integration, body []byte) (out []byte, status int, header http.Header, err error) {
	parts := strings.SplitN(x.expand(in.URI), ":", 6)
	if len(parts) != 6 || parts[2] != "apigateway" {
		s.hub.Log.Warn("api gateway: not an AWS integration URI", "uri", in.URI)
		return nil, 0, nil, errInternal
	}
	region, service, rest := parts[3], parts[4], parts[5]
	service = service[strings.LastIndex(service, ".")+1:] // "bucket.s3" names the service s3
	q, h := url.Values{}, http.Header{}
	var path string
	switch kind, name, _ := strings.Cut(rest, "/"); kind {
	case "path":
		path, _, _ = strings.Cut(name, "?")
		path = "/" + path
		if _, query, ok := strings.Cut(name, "?"); ok {
			q, _ = url.ParseQuery(query)
		}
	case "action":
		path = "/"
		action, _, _ := strings.Cut(strings.SplitN(name, "&", 2)[0], "?")
		if js, ok := jsonServices[service]; ok {
			h.Set("X-Amz-Target", js.target+"."+action)
			h.Set("Content-Type", js.contentType)
		} else {
			q.Set("Action", action)
			h.Set("Content-Type", "application/x-www-form-urlencoded")
		}
	default:
		s.hub.Log.Warn("api gateway: not an AWS integration URI", "uri", in.URI)
		return nil, 0, nil, errInternal
	}
	for k, v := range in.RequestParameters {
		switch name, val := strings.Split(k, "."), x.mapped(v); {
		case len(name) < 4 || name[0] != "integration" || name[1] != "request":
		case name[2] == "header":
			h.Set(strings.Join(name[3:], "."), val)
		case name[2] == "querystring":
			q.Set(strings.Join(name[3:], "."), val)
		case name[2] == "path": // fills {name} in the URI path
			path = strings.ReplaceAll(path, "{"+strings.Join(name[3:], ".")+"}", url.PathEscape(val))
		}
	}
	if len(q) > 0 {
		path += "?" + q.Encode()
	}
	if x.c.Trace != "" {
		h.Set("X-Amzn-Trace-Id", core.TraceHeader(x.c.Trace))
	}
	status, rh, out := s.hub.Do(x.account, region, service, in.HTTPMethod, path, h, body)
	return out, status, rh, nil
}

// mapped reads a request parameter expression: 'literal', method.request.path.x,
// method.request.header.x, method.request.querystring.x, stageVariables.x or context.x.
func (x *call) mapped(v string) string {
	if len(v) >= 2 && v[0] == '\'' && v[len(v)-1] == '\'' {
		return v[1 : len(v)-1]
	}
	if p, ok := strings.CutPrefix(v, "method.request.path."); ok {
		return x.params[p]
	}
	return x.identity(v)
}
