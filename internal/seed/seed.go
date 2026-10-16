// Package seed applies pogi.yaml: IAM identities, friendly and typed
// resources, and CloudFormation stacks. It runs before "Ready.", again after
// every reset, and for MCP's seed tool. Anything that already exists is
// skipped, never updated.
package seed

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
	"go.yaml.in/yaml/v3"
)

// Apply seeds f's identities, then its resources, then its stacks.
func Apply(h *core.Hub, f *config.File) error {
	file := cmp.Or(f.Path, "pogi.yaml")
	if err := identities(h, file, f.Identities.AWS); err != nil {
		return err
	}
	if err := apply(h, f.Resources, file, nil); err != nil {
		return err
	}
	return stacks(h, file, f.Stacks)
}

// ApplyResources seeds friendly and typed resources. file names their source
// in errors, and relative paths start from its directory.
func ApplyResources(h *core.Hub, r config.Resources, file string) error {
	return apply(h, r, file, nil)
}

// Hook sets hub.Seed (MCP's seed tool) and, with a pogi.yaml, seeds again
// after every reset: everything after a full reset; after a partial one only
// the resources of the reset services (and the identities, when iam is one).
func Hook(h *core.Hub, f *config.File) {
	h.Seed = func(fragment []byte) error {
		var r config.Resources
		d := yaml.NewDecoder(bytes.NewReader(fragment))
		d.KnownFields(true)
		if err := d.Decode(&r); err != nil && !errors.Is(err, io.EOF) {
			return fmt.Errorf("seed: %w", err)
		}
		return ApplyResources(h, r, "seed")
	}
	if f == nil {
		return
	}
	h.OnReset(func(services []string) {
		if err := reseed(h, f, services); err != nil {
			h.Log.Error("seeding after reset failed", "err", err)
		}
	})
}

func reseed(h *core.Hub, f *config.File, services []string) error {
	if len(services) == 0 {
		return Apply(h, f)
	}
	file := cmp.Or(f.Path, "pogi.yaml")
	if slices.Contains(services, "iam") {
		if err := identities(h, file, f.Identities.AWS); err != nil {
			return err
		}
	}
	return apply(h, f.Resources, file, services)
}

// kind is a friendly resource kind and the service a partial reset names it by.
type kind struct {
	path, service string
	seed          func(*seeder, config.Item) error
}

// kinds in seeding order (topics before their subscriptions).
var kinds = []kind{
	{"aws.kms.keys", "kms", kmsKey},
	{"aws.secretsmanager.secrets", "secretsmanager", plain("AWS::SecretsManager::Secret", "Name", "value=SecretString")},
	{"aws.ssm.parameters", "ssm", parameter},
	{"aws.s3.buckets", "s3", bucket},
	{"aws.sqs.queues", "sqs", queue},
	{"aws.dynamodb.tables", "dynamodb", table},
	{"aws.kinesis.streams", "kinesis", plain("AWS::Kinesis::Stream", "Name", "shards=ShardCount")},
	{"aws.logs.groups", "logs", plain("AWS::Logs::LogGroup", "LogGroupName", "retentionDays=RetentionInDays")},
	{"aws.events.buses", "events", plain("AWS::Events::EventBus", "Name")},
	{"aws.lambda.functions", "lambda", function},
	{"aws.sns.topics", "sns", topic},
	{"aws.stepfunctions.stateMachines", "stepfunctions", stateMachine},
	{"gcp.pubsub.topics", "pubsub", pubsubTopic},
	{"gcp.pubsub.subscriptions", "pubsub", subscription},
	{"gcp.storage.buckets", "gcs", gcsBucket},
	{"gcp.gcs.buckets", "gcs", gcsBucket},
	{"gcp.secretmanager.secrets", "secretmanager", gcpSecret},
	{"gcp.firestore.documents", "firestore", document},
	{"azure.blob.containers", "azblob", container},
	{"azure.queue.queues", "azqueue", azureQueue},
	{"azure.table.tables", "aztable", azureTable},
	{"azure.appconfig.stores", "azappconfig", appConfigStore},
	{"azure.appconfig.keyvalues", "azappconfig", appConfigKeyValue},
	{"azure.keyvault.secrets", "keyvault", vaultSecret},
	{"azure.cosmos.databases", "cosmos", cosmosDB},
	{"azure.servicebus.queues", "servicebus", serviceBus("queue")},
	{"azure.servicebus.topics", "servicebus", serviceBus("topic")},
	{"azure.eventgrid.topics", "eventgrid", eventGridTopic},
	{"azure.eventgrid.systemTopics", "eventgrid", eventGridSystemTopic},
	{"azure.eventgrid.subscriptions", "eventgrid", eventGridSubscription}, // after the topics they subscribe to
	{"azure.eventhubs.hubs", "eventhubs", eventHub},
	{"azure.eventhubs.consumerGroups", "eventhubs", consumerGroup},
	{"azure.functions.apps", "azfunctions", functionApp},
}

// seeder is what every resource needs.
type seeder struct {
	h               *core.Hub
	file            string
	account, region string
}

// apply seeds r, or only the resources of the given services when only is set.
func apply(h *core.Hub, r config.Resources, file string, only []string) error {
	clouds := map[string]map[string]map[string]config.Items{"aws": r.AWS, "gcp": r.GCP, "azure": r.Azure}
	for _, cloud := range []string{"aws", "gcp", "azure"} {
		for _, svc := range core.SortedKeys(clouds[cloud]) {
			for _, name := range core.SortedKeys(clouds[cloud][svc]) {
				path := cloud + "." + svc + "." + name
				items := clouds[cloud][svc][name]
				if len(items) > 0 && !slices.ContainsFunc(kinds, func(k kind) bool { return k.path == path }) {
					return fmt.Errorf("%s:%d: pogi cannot seed %s", file, items[0].Line, path)
				}
			}
		}
	}
	s := &seeder{h: h, file: file, account: core.DefaultAccount, region: h.Cfg.Region}
	for _, k := range kinds {
		if only != nil && !slices.Contains(only, k.service) {
			continue
		}
		p := strings.Split(k.path, ".")
		for _, it := range clouds[p[0]][p[1]][p[2]] {
			if err := k.seed(s, it); err != nil {
				return fmt.Errorf("%s:%d: %s %s: %w", file, it.Line, k.path, it.Name, err)
			}
		}
	}
	for i, t := range r.Typed {
		if only != nil && !slices.Contains(only, typedService(t.Type)) {
			continue
		}
		if err := s.typed(t, i); err != nil {
			return fmt.Errorf("%s:%d: %s: %w", file, t.Line, t.Type, err)
		}
	}
	return nil
}

// typedService is the service of a typed resource: AWS::S3::Bucket → s3.
func typedService(typ string) string {
	p := strings.Split(strings.ToLower(typ), "::")
	if len(p) < 2 {
		return ""
	}
	if alias, ok := map[string]string{"kinesisfirehose": "firehose", "certificatemanager": "acm", "cognito": "cognito-idp", "apigatewayv2": "apigateway"}[p[1]]; ok {
		return alias
	}
	return p[1]
}

// ---- identities ----

const iamTarget = "AWSIdentityManagementV20100508"

// identities creates IAM users (with fixed access keys) and roles.
func identities(h *core.Hub, file string, ids config.AWSIdentities) error {
	for _, u := range ids.Users {
		if err := user(h, u); err != nil {
			return fmt.Errorf("%s: identities.aws.users %s: %w", file, u.Name, err)
		}
	}
	for _, r := range ids.Roles {
		if err := role(h, r); err != nil {
			return fmt.Errorf("%s: identities.aws.roles %s: %w", file, r.Name, err)
		}
	}
	return nil
}

// iamCall calls IAM in-process as account.
func iamCall(h *core.Hub, account string) func(op string, in map[string]any) error {
	return func(op string, in map[string]any) error { return h.Call(account, h.Cfg.Region, iamTarget, op, in, nil) }
}

func user(h *core.Hub, u config.User) error {
	account := cmp.Or(u.Account, core.DefaultAccount)
	call := iamCall(h, account)
	switch err := call("CreateUser", map[string]any{"UserName": u.Name}); {
	case exists(err): // left as it is; its fixed key is still registered below
	case err != nil:
		return err
	default:
		if err := grant(call, "User", u.Name, u.Policies, u.Inline); err != nil {
			return err
		}
		for _, g := range u.Groups {
			if err := call("CreateGroup", map[string]any{"GroupName": g}); err != nil && !exists(err) {
				return err
			}
			if err := call("AddUserToGroup", map[string]any{"GroupName": g, "UserName": u.Name}); err != nil {
				return err
			}
		}
	}
	if u.AccessKey == "" {
		return nil
	}
	// CreateUser above needed the iam service, so it is there
	iam := h.Service("iam").(interface {
		ImportAccessKey(account, user, akid, secret string) error
	})
	return iam.ImportAccessKey(account, u.Name, u.AccessKey, u.SecretKey)
}

func role(h *core.Hub, r config.Role) error {
	account := cmp.Or(r.Account, core.DefaultAccount)
	call := iamCall(h, account)
	switch err := call("CreateRole", map[string]any{"RoleName": r.Name, "AssumeRolePolicyDocument": trust(r.Trust, account)}); {
	case exists(err):
		return nil
	case err != nil:
		return err
	}
	return grant(call, "Role", r.Name, r.Policies, r.Inline)
}

// trust is a role's trust policy for a service principal (lambda.amazonaws.com),
// an account id or ARN, or "*"; empty trusts the role's own account.
func trust(who, account string) string {
	principal := map[string]any{"AWS": cmp.Or(who, account)}
	if strings.Contains(who, ".") && !strings.HasPrefix(who, "arn:") {
		principal = map[string]any{"Service": who}
	}
	return text(map[string]any{"Version": "2012-10-17", "Statement": []any{
		map[string]any{"Effect": "Allow", "Principal": principal, "Action": "sts:AssumeRole"}}})
}

// grant attaches managed policies to a user or role and puts its inline ones.
func grant(call func(string, map[string]any) error, kind, name string, managed []string, inline map[string]any) error {
	for _, arn := range managed {
		if err := call("Attach"+kind+"Policy", map[string]any{kind + "Name": name, "PolicyArn": arn}); err != nil {
			return err
		}
	}
	for _, p := range core.SortedKeys(inline) {
		if err := call("Put"+kind+"Policy", map[string]any{kind + "Name": name, "PolicyName": p, "PolicyDocument": text(inline[p])}); err != nil {
			return err
		}
	}
	return nil
}

// ---- stacks ----

// stacks deploys CloudFormation stacks, each to the end.
func stacks(h *core.Hub, file string, list []config.Stack) error {
	for _, st := range list {
		if err := stack(h, file, st); err != nil {
			return fmt.Errorf("%s:%d: stack %s: %w", file, st.Line, st.Name, err)
		}
	}
	return nil
}

func stack(h *core.Hub, file string, st config.Stack) error {
	body, err := os.ReadFile(rel(file, st.Template))
	if err != nil {
		return err
	}
	account, region := cmp.Or(st.Account, core.DefaultAccount), cmp.Or(st.Region, h.Cfg.Region)
	call := func(op string, in, out any) error { return h.Call(account, region, "CloudFormation", op, in, out) }
	var params []map[string]string
	for _, k := range core.SortedKeys(st.Parameters) {
		params = append(params, map[string]string{"ParameterKey": k, "ParameterValue": st.Parameters[k]})
	}
	err = call("CreateStack", map[string]any{"StackName": st.Name, "TemplateBody": string(body), "Parameters": params,
		"Capabilities": []string{"CAPABILITY_IAM", "CAPABILITY_NAMED_IAM", "CAPABILITY_AUTO_EXPAND"}}, nil)
	if exists(err) {
		return nil
	}
	if err != nil {
		return err
	}
	in := map[string]any{"StackName": st.Name}
	for deadline := time.Now().Add(5 * time.Minute); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		var out struct {
			Stacks []struct{ StackStatus, StackStatusReason string }
		}
		if err := call("DescribeStacks", in, &out); err != nil {
			return err
		}
		switch s := out.Stacks[0]; {
		case s.StackStatus == "CREATE_COMPLETE":
			return nil
		case !strings.HasSuffix(s.StackStatus, "_IN_PROGRESS"):
			return fmt.Errorf("%s: %s", s.StackStatus, failure(call, in, s.StackStatusReason))
		}
	}
	return errors.New("not complete after 5 minutes")
}

// failure is the reason of the first resource that failed, else the stack's.
func failure(call func(op string, in, out any) error, in any, reason string) string {
	var out struct {
		StackEvents []struct{ LogicalResourceId, ResourceType, ResourceStatus, ResourceStatusReason string }
	}
	call("DescribeStackEvents", in, &out)
	for i := len(out.StackEvents) - 1; i >= 0; i-- { // newest first
		if e := out.StackEvents[i]; strings.HasSuffix(e.ResourceStatus, "_FAILED") && e.ResourceType != "AWS::CloudFormation::Stack" {
			return e.LogicalResourceId + ": " + e.ResourceStatusReason
		}
	}
	return reason
}

// ---- helpers ----

// alreadyExists matches the AWS error codes of creating something twice.
var alreadyExists = regexp.MustCompile(`AlreadyExists|AlreadyOwnedByYou|^Resource(InUse|Exists|Conflict)Exception$|^QueueNameExists$`)

func exists(err error) bool { return err != nil && alreadyExists.MatchString(core.AsError(err).Code) }

// allow rejects properties other than keys.
func allow(p map[string]any, keys ...string) error {
	for _, k := range core.SortedKeys(p) {
		if !slices.Contains(keys, k) {
			return fmt.Errorf("unknown property %q (allowed: %s)", k, cmp.Or(strings.Join(keys, ", "), "none"))
		}
	}
	return nil
}

// nested reads a list of property maps (or one map), rejecting unknown keys.
func nested(v any, keys ...string) ([]map[string]any, error) {
	list, ok := v.([]any)
	if !ok && v != nil {
		list = []any{v}
	}
	var out []map[string]any
	for _, x := range list {
		m, ok := x.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("expected {%s}, got %v", strings.Join(keys, ", "), x)
		}
		if err := allow(m, keys...); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, nil
}

// rename copies the friendly properties that are set into API ones: "from=To".
func rename(dst, src map[string]any, pairs ...string) {
	for _, pair := range pairs {
		from, to, _ := strings.Cut(pair, "=")
		if v, ok := src[from]; ok {
			dst[to] = v
		}
	}
}

// text renders a scalar as text and anything else as JSON.
func text(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case map[string]any, []any:
		b, _ := json.Marshal(x)
		return string(b)
	}
	return fmt.Sprint(v)
}

// rel resolves a path against the directory of the config file.
func rel(file, p string) string {
	if filepath.IsAbs(p) {
		return p
	}
	return filepath.Join(filepath.Dir(file), p)
}
