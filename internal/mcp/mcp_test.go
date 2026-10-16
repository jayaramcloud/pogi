package mcp_test

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestTools(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t)
	// 2025-06-18 is what Claude Code and most agents speak today.
	cs, err := sdk.NewClient(&sdk.Implementation{Name: "test", Version: "1"}, nil).Connect(ctx,
		&sdk.StreamableClientTransport{Endpoint: srv.URL + "/_pogi/mcp"}, &sdk.ClientSessionOptions{ProtocolVersion: "2025-06-18"})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	call := func(name string, args any) string {
		t.Helper()
		res, err := cs.CallTool(ctx, &sdk.CallToolParams{Name: name, Arguments: args})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		text := res.Content[0].(*sdk.TextContent).Text
		if res.IsError {
			t.Fatalf("%s: %s", name, text)
		}
		return text
	}
	want := func(name, got, sub string) {
		t.Helper()
		if !strings.Contains(got, sub) {
			t.Fatalf("%s: want %s in %s", name, sub, got)
		}
	}

	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 22 {
		t.Fatalf("tools: %d, %v", len(tools.Tools), err)
	}
	want("status", call("status", nil), `"name":"sqs"`)

	q := sqs.NewFromConfig(awstest.Config(t, srv.URL))
	out, err := q.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("jobs")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: out.QueueUrl, MessageBody: aws.String("hello")}); err != nil {
		t.Fatal(err)
	}
	want("list_resources", call("list_resources", map[string]any{"service": "sqs"}), `["jobs",`)
	want("request_log", call("request_log", map[string]any{"service": "sqs", "limit": 1}), `"op":"SendMessage"`)
	want("request_log", call("request_log", map[string]any{"query": "createqueue"}), `"op":"CreateQueue"`)
	want("peek_queue", call("peek_queue", map[string]any{"queue": *out.QueueUrl}), `"Body":"hello"`)

	tmpl := `{"Resources": {"T": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "alerts"}}},
		"Outputs": {"Arn": {"Value": {"Ref": "T"}}}}`
	got := call("deploy_stack", map[string]any{"name": "demo", "template_body": tmpl})
	want("deploy_stack", got, `"status":"CREATE_COMPLETE"`)
	want("deploy_stack", got, `"Arn":"arn:aws:sns:us-east-1:000000000000:alerts"`)
	bad := `{"Resources": {"T": {"Type": "AWS::SNS::Topic", "Properties": {"TopicName": "alerts"}},
		"B": {"Type": "AWS::S3::Bucket", "Properties": {"BucketName": "Bad_Bucket"}}}}`
	got = call("deploy_stack", map[string]any{"name": "demo", "template_body": bad})
	want("deploy_stack", got, `"status":"UPDATE_ROLLBACK_COMPLETE"`)
	want("deploy_stack", got, `"firstFailure":{"LogicalResourceId":"B"`)

	call("reset", nil)
	if got := call("list_resources", map[string]any{"service": "sqs"}); strings.Contains(got, "jobs") {
		t.Fatalf("queue survived reset: %s", got)
	}

	// Browser pages from other origins and DNS-rebound hosts are refused.
	for _, c := range []struct {
		header, value string
		code          int
	}{
		{"Origin", "https://evil.example", 403},
		{"Host", "evil.example", 403},
		{"Origin", "http://localhost:3000", 200},
		{"Host", "aws.localhost", 200},
	} {
		req, _ := http.NewRequest("POST", srv.URL+"/_pogi/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		req.Header.Set(c.header, c.value)
		req.Host = req.Header.Get("Host")
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		if res.StatusCode != c.code {
			t.Errorf("%s: %s = %d, want %d", c.header, c.value, res.StatusCode, c.code)
		}
	}
}
