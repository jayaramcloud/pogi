package config

import (
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestItems(t *testing.T) {
	var r Resources
	src := `
aws:
  s3: {buckets: [uploads, {name: logs, versioning: true}]}
  secretsmanager: {secrets: {db-password: hunter2}}
  sqs: {queues: {jobs: {delaySeconds: 5}}}
typed:
  - {type: "AWS::SNS::Topic", properties: {TopicName: alerts}}
`
	if err := yaml.Unmarshal([]byte(src), &r); err != nil {
		t.Fatal(err)
	}
	b := r.AWS["s3"]["buckets"]
	if len(b) != 2 || b[0].Name != "uploads" || b[1].Name != "logs" || b[1].Props["versioning"] != true || b[1].Line != 3 {
		t.Fatalf("buckets: %+v", b)
	}
	if s := r.AWS["secretsmanager"]["secrets"]; s[0].Name != "db-password" || s[0].Props["value"] != "hunter2" {
		t.Fatalf("secrets: %+v", s)
	}
	if q := r.AWS["sqs"]["queues"]; q[0].Name != "jobs" || q[0].Props["delaySeconds"] != 5 {
		t.Fatalf("queues: %+v", q)
	}
	if ty := r.Typed[0]; ty.Type != "AWS::SNS::Topic" || ty.Line != 7 || ty.Properties["TopicName"] != "alerts" {
		t.Fatalf("typed: %+v", ty)
	}
	if err := yaml.Unmarshal([]byte("typed: [{type: X, propertes: {}}]"), &r); err == nil {
		t.Fatal("unknown field accepted")
	}
}
