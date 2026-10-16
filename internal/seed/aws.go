package seed

import (
	"archive/zip"
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"

	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
)

// typed provisions a resources.typed entry; the logical id defaults to the
// type's last part and position (Topic1), which keeps generated names stable.
func (s *seeder) typed(t config.Typed, i int) error {
	if !strings.HasPrefix(t.Type, "AWS::") {
		return errors.New("typed google/azure resources arrive with the generic engine; use the friendly form")
	}
	ts := *s
	ts.account, ts.region = cmp.Or(t.Account, s.account), cmp.Or(t.Region, s.region)
	return ts.cfn(t.Type, cmp.Or(t.Name, fmt.Sprintf("%s%d", t.Type[strings.LastIndex(t.Type, ":")+1:], i+1)), t.Properties)
}

// provision creates an AWS resource through CloudFormation's providers and
// returns its physical id; "already exists" means an earlier seeding made it.
func (s *seeder) provision(typ, logical string, props map[string]any) (string, error) {
	cfn, ok := s.h.Service("cloudformation").(interface {
		Provision(account, region, typ, logical string, props map[string]any) (string, map[string]any, error)
	})
	if !ok {
		return "", errors.New("seeding AWS resources needs the cloudformation service")
	}
	// CreateBucket succeeds again on your own bucket in us-east-1, and would reconfigure it
	if b := text(props["BucketName"]); typ == "AWS::S3::Bucket" && b != "" {
		if st, _, _ := s.h.Do(s.account, s.region, "s3", http.MethodHead, "/"+b, nil, nil); st == http.StatusOK {
			return b, nil
		}
	}
	phys, _, err := cfn.Provision(s.account, s.region, typ, logical, props)
	if exists(err) {
		err = nil
	}
	return phys, err
}

func (s *seeder) cfn(typ, logical string, props map[string]any) error {
	_, err := s.provision(typ, logical, props)
	return err
}

// plain is a kind whose properties map one to one onto its CloudFormation type's.
func plain(typ, nameProp string, pairs ...string) func(*seeder, config.Item) error {
	var keys []string
	for _, p := range pairs {
		k, _, _ := strings.Cut(p, "=")
		keys = append(keys, k)
	}
	return func(s *seeder, it config.Item) error {
		if err := allow(it.Props, keys...); err != nil {
			return err
		}
		props := map[string]any{nameProp: it.Name}
		rename(props, it.Props, pairs...)
		return s.cfn(typ, it.Name, props)
	}
}

// arn turns a name into an ARN of this account and region, unless it is one.
func (s *seeder) arn(name, service, prefix string) string {
	if strings.HasPrefix(name, "arn:") {
		return name
	}
	region := s.region
	if service == "iam" {
		region = ""
	}
	return core.ARN(service, region, s.account, prefix+name)
}

// bucket: versioning, tags.
func bucket(s *seeder, it config.Item) error {
	if err := allow(it.Props, "versioning", "tags"); err != nil {
		return err
	}
	props := map[string]any{"BucketName": it.Name, "Tags": it.Props["tags"]}
	if it.Props["versioning"] == true {
		props["VersioningConfiguration"] = map[string]any{"Status": "Enabled"}
	}
	return s.cfn("AWS::S3::Bucket", it.Name, props)
}

// queue: fifo (or a .fifo name), delaySeconds, visibilityTimeout,
// deadLetter {queue, maxReceiveCount}, tags.
func queue(s *seeder, it config.Item) error {
	p := it.Props
	if err := allow(p, "fifo", "delaySeconds", "visibilityTimeout", "deadLetter", "tags"); err != nil {
		return err
	}
	dl, err := nested(p["deadLetter"], "queue", "maxReceiveCount")
	if err != nil {
		return err
	}
	props := map[string]any{"QueueName": it.Name}
	rename(props, p, "delaySeconds=DelaySeconds", "visibilityTimeout=VisibilityTimeout", "tags=Tags")
	if p["fifo"] == true || strings.HasSuffix(it.Name, ".fifo") {
		props["FifoQueue"] = true
	}
	for _, d := range dl {
		props["RedrivePolicy"] = map[string]any{"deadLetterTargetArn": s.arn(text(d["queue"]), "sqs", ""), "maxReceiveCount": d["maxReceiveCount"]}
	}
	return s.cfn("AWS::SQS::Queue", it.Name, props)
}

// topic: fifo (or a .fifo name), subscriptions [{protocol, endpoint}]; sqs
// and lambda endpoints may be names.
func topic(s *seeder, it config.Item) error {
	if err := allow(it.Props, "fifo", "subscriptions"); err != nil {
		return err
	}
	subs, err := nested(it.Props["subscriptions"], "protocol", "endpoint")
	if err != nil {
		return err
	}
	var list []any
	for _, sub := range subs {
		proto, ep := text(sub["protocol"]), text(sub["endpoint"])
		switch proto {
		case "sqs":
			ep = s.arn(ep, "sqs", "")
		case "lambda":
			ep = s.arn(ep, "lambda", "function:")
		}
		list = append(list, map[string]any{"Protocol": proto, "Endpoint": ep})
	}
	props := map[string]any{"TopicName": it.Name, "Subscription": list}
	if it.Props["fifo"] == true || strings.HasSuffix(it.Name, ".fifo") {
		props["FifoTopic"] = true
	}
	return s.cfn("AWS::SNS::Topic", it.Name, props)
}

// table: key (pk or [pk, sk], each "name" or "name:TYPE"), indexes
// [{name, key}], ttl, stream; billing is on demand.
func table(s *seeder, it config.Item) error {
	p := it.Props
	if err := allow(p, "key", "indexes", "ttl", "stream"); err != nil {
		return err
	}
	types := map[string]string{}
	key, err := keySchema(p["key"], types)
	if err != nil {
		return err
	}
	indexes, err := nested(p["indexes"], "name", "key")
	if err != nil {
		return err
	}
	var gsis, defs []any
	for _, ix := range indexes {
		ks, err := keySchema(ix["key"], types)
		if err != nil {
			return fmt.Errorf("index %v: %w", ix["name"], err)
		}
		gsis = append(gsis, map[string]any{"IndexName": ix["name"], "KeySchema": ks, "Projection": map[string]any{"ProjectionType": "ALL"}})
	}
	for _, name := range core.SortedKeys(types) {
		defs = append(defs, map[string]any{"AttributeName": name, "AttributeType": types[name]})
	}
	props := map[string]any{"TableName": it.Name, "KeySchema": key, "AttributeDefinitions": defs, "GlobalSecondaryIndexes": gsis,
		"BillingMode": "PAY_PER_REQUEST"}
	if ttl, ok := p["ttl"]; ok {
		props["TimeToLiveSpecification"] = map[string]any{"AttributeName": ttl, "Enabled": true}
	}
	if p["stream"] == true {
		props["StreamSpecification"] = map[string]any{"StreamViewType": "NEW_AND_OLD_IMAGES"}
	}
	return s.cfn("AWS::DynamoDB::Table", it.Name, props)
}

// keySchema turns "pk" or [pk, "sk:N"] into a KeySchema and records the
// attribute types (S by default).
func keySchema(v any, types map[string]string) ([]any, error) {
	var names []any
	switch k := v.(type) {
	case string:
		names = []any{k}
	case []any:
		names = k
	}
	if len(names) == 0 || len(names) > 2 {
		return nil, fmt.Errorf("key must be a name or [hash, range], not %v", v)
	}
	var ks []any
	for i, n := range names {
		name, typ, _ := strings.Cut(text(n), ":")
		if typ = cmp.Or(typ, "S"); typ != "S" && typ != "N" && typ != "B" {
			return nil, fmt.Errorf("key %s: the type must be S, N or B", name)
		}
		if _, ok := types[name]; !ok {
			types[name] = typ
		}
		ks = append(ks, map[string]any{"AttributeName": name, "KeyType": [2]string{"HASH", "RANGE"}[i]})
	}
	return ks, nil
}

// parameter: value, type (String by default).
func parameter(s *seeder, it config.Item) error {
	if err := allow(it.Props, "value", "type"); err != nil {
		return err
	}
	return s.cfn("AWS::SSM::Parameter", it.Name, map[string]any{"Name": it.Name, "Value": text(it.Props["value"]),
		"Type": cmp.Or(text(it.Props["type"]), "String")})
}

// kmsKey: the name is the key's alias.
func kmsKey(s *seeder, it config.Item) error {
	if err := allow(it.Props); err != nil {
		return err
	}
	alias := it.Name
	if !strings.HasPrefix(alias, "alias/") {
		alias = "alias/" + alias
	}
	// a second CreateKey is never a duplicate, so look for the alias first
	if s.h.Call(s.account, s.region, "TrentService", "DescribeKey", map[string]any{"KeyId": alias}, nil) == nil {
		return nil
	}
	id, err := s.provision("AWS::KMS::Key", it.Name, map[string]any{"Description": alias})
	if err != nil {
		return err
	}
	return s.cfn("AWS::KMS::Alias", it.Name, map[string]any{"AliasName": alias, "TargetKeyId": id})
}

// function: runtime, handler, code (a directory or .zip, relative to the
// config file), role (default pogi-seed-lambda), environment, timeout, memory.
// CloudFormation has no local code, so this calls Lambda itself.
func function(s *seeder, it config.Item) error {
	p := it.Props
	if err := allow(p, "runtime", "handler", "code", "role", "environment", "timeout", "memory"); err != nil {
		return err
	}
	if p["code"] == nil {
		return errors.New("code is required")
	}
	code, err := zipCode(rel(s.file, text(p["code"])))
	if err != nil {
		return err
	}
	env, ok := p["environment"].(map[string]any)
	if !ok && p["environment"] != nil {
		return errors.New("environment must be a map of variables")
	}
	vars := map[string]string{}
	for k, v := range env {
		vars[k] = text(v)
	}
	role := text(p["role"])
	if role == "" {
		if role, err = s.lambdaRole(); err != nil {
			return err
		}
	}
	in := map[string]any{"FunctionName": it.Name, "Runtime": p["runtime"], "Handler": p["handler"], "Role": s.arn(role, "iam", "role/"),
		"Code": map[string]any{"ZipFile": code}, "Environment": map[string]any{"Variables": vars}}
	rename(in, p, "timeout=Timeout", "memory=MemorySize")
	b, _ := json.Marshal(in)
	status, _, resp := s.h.Do(s.account, s.region, "lambda", http.MethodPost, "/2015-03-31/functions", http.Header{"Content-Type": {"application/json"}}, b)
	if status < 300 {
		return nil
	}
	var e struct {
		Type    string `json:"__type"`
		Message string
	}
	json.Unmarshal(resp, &e)
	if err = core.Errorf(status, e.Type, "%s", e.Message); exists(err) {
		return nil
	}
	return err
}

// lambdaRole is the role seeded functions run as by default; it is created
// when IAM is enabled (Lambda does not check roles).
func (s *seeder) lambdaRole() (string, error) {
	const name = "pogi-seed-lambda"
	if s.h.Service("iam") == nil {
		return name, nil
	}
	err := s.h.Call(s.account, s.region, iamTarget, "CreateRole", map[string]any{"RoleName": name,
		"AssumeRolePolicyDocument": trust("lambda.amazonaws.com", s.account)}, nil)
	if err != nil && !exists(err) {
		return "", err
	}
	return name, nil
}

// zipCode zips a directory, or reads a .zip.
func zipCode(path string) ([]byte, error) {
	if fi, err := os.Stat(path); err != nil || !fi.IsDir() {
		return os.ReadFile(path)
	}
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	// ponytail: AddFS rejects symlinks (node_modules/.bin); point code at a .zip for those
	if err := zw.AddFS(os.DirFS(path)); err != nil {
		return nil, fmt.Errorf("zip %s: %w", path, err)
	}
	err := zw.Close()
	return buf.Bytes(), err
}

// stateMachine: definition (a JSON file relative to the config file, or an
// inline map), role.
func stateMachine(s *seeder, it config.Item) error {
	p := it.Props
	if err := allow(p, "definition", "role"); err != nil {
		return err
	}
	def := text(p["definition"])
	if _, inline := p["definition"].(map[string]any); !inline {
		b, err := os.ReadFile(rel(s.file, def))
		if err != nil {
			return err
		}
		def = string(b)
	}
	props := map[string]any{"StateMachineName": it.Name, "DefinitionString": def}
	if r := text(p["role"]); r != "" {
		props["RoleArn"] = s.arn(r, "iam", "role/")
	}
	return s.cfn("AWS::StepFunctions::StateMachine", it.Name, props)
}
