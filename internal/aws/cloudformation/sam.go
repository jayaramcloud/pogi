package cloudformation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// The SAM transform (AWS::Serverless-2016-10-31), expanded in Go before a
// template is deployed, as CloudFormation does. The logical ids it makes match
// the ones AWS SAM makes (MyFnRole, MyFnAliaslive, ServerlessRestApiProdStage,
// ...), so templates can !Ref and !GetAtt them.
//
// Every type this file makes is one pogi provisions: AWS::Lambda::Function,
// Version, Alias, Permission, Url, LayerVersion, EventSourceMapping;
// AWS::IAM::Role; AWS::ApiGateway::RestApi, Deployment, Stage;
// AWS::ApiGatewayV2::Api, Integration, Route, Stage, Authorizer;
// AWS::DynamoDB::Table; AWS::SNS::Subscription; AWS::Events::Rule;
// AWS::Scheduler::Schedule; AWS::StepFunctions::StateMachine.

const samName = "AWS::Serverless-2016-10-31"

// transform expands the SAM transform of tmpl in place (no-op without it).
func (s *Service) transform(tmpl map[string]any) error {
	var rest []any
	found := false
	switch t := tmpl["Transform"].(type) {
	case string:
		found = t == samName
	case []any:
		for _, it := range t {
			if it == samName {
				found = true
			} else {
				rest = append(rest, it)
			}
		}
	}
	if !found {
		return nil
	}
	x := &samExp{s: s, rename: map[string]string{}, rest: map[string]*samRest{}, http: map[string]*samHTTP{}}
	if err := x.run(tmpl); err != nil {
		return err
	}
	if len(rest) > 0 {
		tmpl["Transform"] = rest
	} else {
		delete(tmpl, "Transform")
	}
	return nil
}

// samSrc is the SAM resource being expanded. Condition and DependsOn go to
// everything it makes.
type samSrc struct {
	id    string
	attrs map[string]any
}

type samExp struct {
	s       *Service
	in, out map[string]any
	globals map[string]map[string]any
	rename  map[string]string // references to rewrite: "MyFn.Alias" -> "MyFnAliaslive"
	rest    map[string]*samRest
	http    map[string]*samHTTP
	err     error
}

// fail records the first error; the run stops at the end of the current resource.
func (x *samExp) fail(format string, a ...any) {
	if x.err == nil {
		x.err = validation(format, a...)
	}
}

// check fails on any property that is not in allowed (a space separated list).
func (x *samExp) check(what string, props map[string]any, allowed string) {
	ok := strings.Fields(allowed)
	for _, k := range core.SortedKeys(props) {
		if !slices.Contains(ok, k) {
			x.fail("pogi does not support the property %s of %s (or it is not valid)", k, what)
		}
	}
}

func (x *samExp) run(tmpl map[string]any) error {
	res, _ := tmpl["Resources"].(map[string]any)
	if res == nil {
		return validation("Template format error: At least one Resources member must be defined.")
	}
	x.in, x.out, x.globals = res, map[string]any{}, map[string]map[string]any{}
	if g, ok := tmpl["Globals"].(map[string]any); ok {
		for _, k := range core.SortedKeys(g) {
			if !slices.Contains([]string{"Function", "Api", "HttpApi", "SimpleTable", "StateMachine"}, k) {
				return validation("Invalid Globals section: %s is not supported", k)
			}
			x.globals[k], _ = g[k].(map[string]any)
		}
		delete(tmpl, "Globals")
	}
	ids := core.SortedKeys(res)
	type item struct {
		src   *samSrc
		typ   string
		props map[string]any
	}
	var items []item
	for _, id := range ids {
		r, _ := res[id].(map[string]any)
		typ := str(r["Type"])
		if !strings.HasPrefix(typ, "AWS::Serverless::") {
			x.out[id] = r
			continue
		}
		src := &samSrc{id: id, attrs: map[string]any{}}
		for _, k := range []string{"Condition", "DependsOn", "Metadata", "DeletionPolicy", "UpdateReplacePolicy"} {
			if v, ok := r[k]; ok {
				src.attrs[k] = v
			}
		}
		props, _ := r["Properties"].(map[string]any)
		if g := x.globals[strings.TrimPrefix(typ, "AWS::Serverless::")]; g != nil {
			props, _ = samMerge(g, props).(map[string]any)
		}
		if props == nil {
			props = map[string]any{}
		}
		items = append(items, item{src, typ, props})
	}
	// The APIs go first: events add their routes to them.
	for _, it := range items {
		switch it.typ {
		case "AWS::Serverless::Api":
			x.rest[it.src.id] = &samRest{id: it.src.id, src: it.src, props: it.props, explicit: true}
		case "AWS::Serverless::HttpApi":
			x.http[it.src.id] = &samHTTP{id: it.src.id, src: it.src, props: it.props, explicit: true}
		}
	}
	for _, it := range items {
		switch it.typ {
		case "AWS::Serverless::Function":
			x.function(it.src, it.props)
		case "AWS::Serverless::StateMachine":
			x.stateMachine(it.src, it.props)
		case "AWS::Serverless::SimpleTable":
			x.simpleTable(it.src, it.props)
		case "AWS::Serverless::LayerVersion":
			x.layer(it.src, it.props)
		case "AWS::Serverless::Api", "AWS::Serverless::HttpApi":
		default:
			x.fail("pogi does not support %s yet", it.typ)
		}
		if x.err != nil {
			return x.err
		}
	}
	for _, id := range core.SortedKeys(x.rest) {
		x.finishRest(x.rest[id])
	}
	for _, id := range core.SortedKeys(x.http) {
		x.finishHTTP(x.http[id])
	}
	if x.err != nil {
		return x.err
	}
	for _, id := range core.SortedKeys(x.out) {
		x.out[id] = samRewrite(x.out[id], x.rename)
	}
	tmpl["Resources"] = x.out
	if o, ok := tmpl["Outputs"]; ok {
		tmpl["Outputs"] = samRewrite(o, x.rename)
	}
	return nil
}

// add adds a resource that came out of src.
func (x *samExp) add(src *samSrc, id, typ string, props map[string]any) map[string]any {
	if x.out[id] != nil || id != src.id && x.in[id] != nil {
		x.fail("Resource with id [%s] is invalid. The logical id is already used by another resource, so SAM cannot make it for %s", id, src.id)
		return nil
	}
	r := map[string]any{"Type": typ, "Properties": props}
	for _, k := range []string{"Condition", "DependsOn"} {
		if v, ok := src.attrs[k]; ok {
			r[k] = v
		}
	}
	x.out[id] = r
	return r
}

// addMain adds the resource that stands for the SAM resource itself: it also
// keeps the attributes only the main resource has.
func (x *samExp) addMain(src *samSrc, id, typ string, props map[string]any) map[string]any {
	r := x.add(src, id, typ, props)
	if r != nil {
		for _, k := range []string{"Metadata", "DeletionPolicy", "UpdateReplacePolicy"} {
			if v, ok := src.attrs[k]; ok {
				r[k] = v
			}
		}
	}
	return r
}

// ---- small helpers ----

func samRef(id string) map[string]any { return map[string]any{"Ref": id} }

func samAtt(id, attr string) map[string]any {
	return map[string]any{"Fn::GetAtt": []any{id, attr}}
}

func samSub(s string, vars map[string]any) map[string]any {
	if len(vars) == 0 {
		return map[string]any{"Fn::Sub": s}
	}
	return map[string]any{"Fn::Sub": []any{s, vars}}
}

func samHash(v any) string {
	b, _ := json.Marshal(v)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])[:10]
}

func samAlnum(s string) string {
	var b strings.Builder
	for _, r := range s {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// samIntrinsic reports whether v is {"Ref": ...} or an {"Fn::...": ...} call.
func samIntrinsic(v any) bool {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return false
	}
	for k := range m {
		return k == "Ref" || strings.HasPrefix(k, "Fn::")
	}
	return false
}

// samClone copies a JSON-like value so edits do not leak into Globals.
func samClone(v any) any {
	b, _ := json.Marshal(v)
	var out any
	json.Unmarshal(b, &out)
	return out
}

// samMerge merges a Globals section g with the resource's own l: maps merge
// by key, lists add up (global entries first), anything else is the resource's.
func samMerge(g, l any) any {
	if l == nil {
		return samClone(g)
	}
	if samIntrinsic(g) || samIntrinsic(l) {
		return l
	}
	switch gv := g.(type) {
	case map[string]any:
		if lv, ok := l.(map[string]any); ok {
			out := map[string]any{}
			for k, v := range gv {
				out[k] = samClone(v)
			}
			for k, v := range lv {
				if gk, has := gv[k]; has {
					out[k] = samMerge(gk, v)
				} else {
					out[k] = v
				}
			}
			return out
		}
	case []any:
		if lv, ok := l.([]any); ok {
			return append(samClone(gv).([]any), lv...)
		}
	}
	return l
}

// samRewrite changes references to ids SAM renamed (!Ref MyLayer, ${MyFn.Alias}, DependsOn).
func samRewrite(v any, m map[string]string) any {
	if len(m) == 0 {
		return v
	}
	subst := func(s string) string {
		for k, n := range m {
			s = strings.ReplaceAll(s, "${"+k+"}", "${"+n+"}")
		}
		return s
	}
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			switch s := val.(type) {
			case string:
				if n, ok := m[s]; ok && k == "Ref" {
					t[k] = n
				}
				continue
			}
			switch k {
			case "Fn::Sub":
				switch s := val.(type) {
				case string:
					t[k] = subst(s)
				case []any:
					if len(s) == 2 {
						s[0] = subst(str(s[0]))
						s[1] = samRewrite(s[1], m)
					}
				}
			case "DependsOn":
				if l, ok := val.([]any); ok {
					for i, d := range l {
						if n, ok := m[str(d)]; ok {
							l[i] = n
						}
					}
				}
			default:
				t[k] = samRewrite(val, m)
			}
		}
	case []any:
		for i := range t {
			t[i] = samRewrite(t[i], m)
		}
	}
	return v
}

// samS3Loc reads an S3 location written as "s3://bucket/key?versionId=v" or
// {Bucket, Key, Version}. A local path fails: it has to be packaged first.
func (x *samExp) samS3Loc(what string, v any) (bucket, key, version any, ok bool) {
	switch t := v.(type) {
	case string:
		u, err := url.Parse(t)
		if err != nil || u.Scheme != "s3" {
			x.fail("%s must be an S3 location (s3://bucket/key) or {Bucket, Key}; %q is a local path. Package the template first (sam package or aws cloudformation package)", what, t)
			return nil, nil, nil, false
		}
		var ver any
		if v := u.Query().Get("versionId"); v != "" {
			ver = v
		}
		return u.Host, strings.TrimPrefix(u.Path, "/"), ver, true
	case map[string]any:
		if samIntrinsic(t) {
			x.fail("%s must be an S3 location or {Bucket, Key}", what)
			return nil, nil, nil, false
		}
		if t["Bucket"] == nil || t["Key"] == nil {
			x.fail("%s needs both Bucket and Key", what)
			return nil, nil, nil, false
		}
		return t["Bucket"], t["Key"], t["Version"], true
	}
	x.fail("%s must be an S3 location or {Bucket, Key}", what)
	return nil, nil, nil, false
}

// samS3Doc reads a text document (a state machine or API definition) from S3.
func (x *samExp) samS3Doc(what string, v any) (string, bool) {
	b, k, _, ok := x.samS3Loc(what, v)
	if !ok {
		return "", false
	}
	body, err := x.s.templateBody("", fmt.Sprintf("s3://%s/%s", str(b), str(k)))
	if err != nil {
		x.fail("%s: %v", what, err)
		return "", false
	}
	return body, true
}

// ---- AWS::Serverless::Function ----

const samFunctionProps = "FunctionName Handler Runtime CodeUri InlineCode ImageUri PackageType ImageConfig MemorySize Timeout Environment Description " +
	"Layers Architectures VpcConfig DeadLetterQueue KmsKeyArn Tracing ReservedConcurrentExecutions EphemeralStorage LoggingConfig Tags " +
	"Role Policies PermissionsBoundary RolePath Events AutoPublishAlias VersionDescription FunctionUrlConfig"

// samFn is how events point at the function: the Lambda function, or its alias
// when AutoPublishAlias is set.
type samFn struct {
	id  string
	ref any    // Ref: the function name, or the alias ARN
	arn any    // the ARN events call
	sub string // the same ARN inside Fn::Sub
}

func (x *samExp) function(src *samSrc, props map[string]any) {
	id := src.id
	what := "AWS::Serverless::Function " + id
	x.check(what, props, samFunctionProps)
	if x.err != nil {
		return
	}
	lp := map[string]any{}
	image := props["PackageType"] == "Image" || props["ImageUri"] != nil
	code := map[string]any{}
	switch {
	case props["InlineCode"] != nil:
		code["ZipFile"] = props["InlineCode"]
	case image:
		if props["ImageUri"] == nil {
			x.fail("%s: PackageType Image needs ImageUri", what)
			return
		}
		code["ImageUri"], lp["PackageType"] = props["ImageUri"], "Image"
	case props["CodeUri"] != nil:
		b, k, v, ok := x.samS3Loc(what+" CodeUri", props["CodeUri"])
		if !ok {
			return
		}
		code["S3Bucket"], code["S3Key"] = b, k
		if v != nil {
			code["S3ObjectVersion"] = v
		}
	default:
		x.fail("%s: Either CodeUri or InlineCode must be specified", what)
		return
	}
	lp["Code"] = code
	if !image && (props["Handler"] == nil || props["Runtime"] == nil) {
		x.fail("%s: Handler and Runtime are required", what)
		return
	}
	for _, k := range strings.Fields("FunctionName Handler Runtime MemorySize Timeout Environment Description Layers Architectures VpcConfig KmsKeyArn ReservedConcurrentExecutions EphemeralStorage ImageConfig LoggingConfig") {
		if v, ok := props[k]; ok {
			lp[k] = v
		}
	}
	if q, ok := props["DeadLetterQueue"].(map[string]any); ok {
		lp["DeadLetterConfig"] = map[string]any{"TargetArn": q["TargetArn"]}
	}
	if t, ok := props["Tracing"]; ok {
		lp["TracingConfig"] = map[string]any{"Mode": t}
	}
	lp["Tags"] = samTags(props["Tags"], "lambda")

	// events decide some role policies, so read them first
	events, _ := props["Events"].(map[string]any)
	if props["Events"] != nil && events == nil {
		x.fail("%s: Events must be a map of event names", what)
		return
	}
	roleDefaults := []string{"service-role/AWSLambdaBasicExecutionRole"}
	want := func(policy string) {
		if !slices.Contains(roleDefaults, policy) {
			roleDefaults = append(roleDefaults, policy)
		}
	}
	if props["VpcConfig"] != nil {
		want("service-role/AWSLambdaVPCAccessExecutionRole")
	}
	if props["Tracing"] == "Active" {
		want("AWSXrayWriteOnlyAccess")
	}
	for _, name := range core.SortedKeys(events) {
		ev, _ := events[name].(map[string]any)
		switch str(ev["Type"]) {
		case "SQS":
			want("service-role/AWSLambdaSQSQueueExecutionRole")
		case "DynamoDB":
			want("service-role/AWSLambdaDynamoDBExecutionRole")
		case "Kinesis":
			want("service-role/AWSLambdaKinesisExecutionRole")
		}
	}
	if props["Role"] != nil {
		lp["Role"] = props["Role"]
	} else {
		var extra []any
		if q, ok := props["DeadLetterQueue"].(map[string]any); ok {
			action := "sqs:SendMessage"
			if str(q["Type"]) == "SNS" {
				action = "sns:Publish"
			}
			extra = append(extra, map[string]any{"PolicyName": "DeadLetterQueuePolicy", "PolicyDocument": map[string]any{"Version": "2012-10-17",
				"Statement": []any{map[string]any{"Action": action, "Effect": "Allow", "Resource": q["TargetArn"]}}}})
		}
		lp["Role"] = x.samRole(src, id+"Role", "lambda.amazonaws.com", props, roleDefaults, extra)
	}
	if x.err != nil {
		return
	}
	x.addMain(src, id, "AWS::Lambda::Function", lp)

	fn := &samFn{id: id, ref: samRef(id), arn: samAtt(id, "Arn"), sub: id + ".Arn"}
	if a := props["AutoPublishAlias"]; a != nil {
		alias, ok := a.(string)
		if !ok || alias == "" {
			x.fail("%s: AutoPublishAlias must be a plain string", what)
			return
		}
		vid, aid := id+"Version"+samHash(lp), id+"Alias"+samAlnum(alias)
		vp := map[string]any{"FunctionName": samRef(id)}
		if d := props["VersionDescription"]; d != nil {
			vp["Description"] = d
		}
		if v := x.add(src, vid, "AWS::Lambda::Version", vp); v != nil {
			v["DeletionPolicy"] = "Retain"
		}
		x.add(src, aid, "AWS::Lambda::Alias", map[string]any{"Name": alias, "FunctionName": samRef(id), "FunctionVersion": samAtt(vid, "Version")})
		x.rename[id+".Alias"], x.rename[id+".Version"] = aid, vid
		fn.ref, fn.arn, fn.sub = samRef(aid), samRef(aid), aid
	}
	if u, ok := props["FunctionUrlConfig"].(map[string]any); ok {
		x.check(what+" FunctionUrlConfig", u, "AuthType Cors InvokeMode")
		if u["AuthType"] == nil {
			x.fail("%s: FunctionUrlConfig needs AuthType", what)
			return
		}
		up := map[string]any{"TargetFunctionArn": fn.ref, "AuthType": u["AuthType"]}
		for _, k := range []string{"Cors", "InvokeMode"} {
			if v, ok := u[k]; ok {
				up[k] = v
			}
		}
		x.add(src, id+"Url", "AWS::Lambda::Url", up)
		if u["AuthType"] == "NONE" {
			x.add(src, id+"UrlPublicPermissions", "AWS::Lambda::Permission", map[string]any{"Action": "lambda:InvokeFunctionUrl",
				"FunctionName": fn.ref, "Principal": "*", "FunctionUrlAuthType": "NONE"})
		}
	}
	for _, name := range core.SortedKeys(events) {
		ev, _ := events[name].(map[string]any)
		p, _ := ev["Properties"].(map[string]any)
		x.functionEvent(src, fn, name, str(ev["Type"]), p)
		if x.err != nil {
			return
		}
	}
}

// samPermission lets a service call a function.
func (x *samExp) samPermission(src *samSrc, id string, fn *samFn, principal string, extra map[string]any) {
	p := map[string]any{"Action": "lambda:InvokeFunction", "FunctionName": fn.ref, "Principal": principal}
	for k, v := range extra {
		p[k] = v
	}
	x.add(src, id, "AWS::Lambda::Permission", p)
}

func (x *samExp) functionEvent(src *samSrc, fn *samFn, name, typ string, p map[string]any) {
	what := fmt.Sprintf("event %s of %s", name, src.id)
	base := src.id + name
	if p == nil {
		p = map[string]any{}
	}
	need := func(keys ...string) bool {
		for _, k := range keys {
			if p[k] == nil {
				x.fail("%s: %s is required", what, k)
				return false
			}
		}
		return true
	}
	switch typ {
	case "SQS", "Kinesis", "DynamoDB":
		src2 := "Queue"
		allowed := "Queue BatchSize Enabled FunctionResponseTypes MaximumBatchingWindowInSeconds FilterCriteria"
		if typ != "SQS" {
			src2 = "Stream"
			allowed = "Stream StartingPosition BatchSize Enabled FunctionResponseTypes MaximumBatchingWindowInSeconds FilterCriteria MaximumRetryAttempts BisectBatchOnFunctionError DestinationConfig"
		}
		x.check(what, p, allowed)
		if !need(src2) || typ != "SQS" && !need("StartingPosition") {
			return
		}
		m := map[string]any{"FunctionName": fn.ref, "EventSourceArn": p[src2]}
		for _, k := range strings.Fields(allowed)[1:] {
			if k != "Stream" && p[k] != nil {
				m[k] = p[k]
			}
		}
		x.add(src, base, "AWS::Lambda::EventSourceMapping", m)
	case "SNS":
		x.check(what, p, "Topic Region FilterPolicy FilterPolicyScope")
		if !need("Topic") {
			return
		}
		sp := map[string]any{"Protocol": "lambda", "TopicArn": p["Topic"], "Endpoint": fn.arn}
		for _, k := range []string{"Region", "FilterPolicy", "FilterPolicyScope"} {
			if p[k] != nil {
				sp[k] = p[k]
			}
		}
		x.add(src, base, "AWS::SNS::Subscription", sp)
		x.samPermission(src, base+"Permission", fn, "sns.amazonaws.com", map[string]any{"SourceArn": p["Topic"]})
	case "S3":
		x.check(what, p, "Bucket Events Filter")
		if !need("Bucket", "Events") {
			return
		}
		ref, _ := p["Bucket"].(map[string]any)
		bucket, _ := x.in[str(ref["Ref"])].(map[string]any)
		if bucket == nil || str(bucket["Type"]) != "AWS::S3::Bucket" {
			x.fail("%s: Bucket must be a !Ref to an AWS::S3::Bucket in this template", what)
			return
		}
		x.samPermission(src, base+"Permission", fn, "s3.amazonaws.com", map[string]any{"SourceAccount": samRef("AWS::AccountId")})
		bp, _ := bucket["Properties"].(map[string]any)
		if bp == nil {
			bp = map[string]any{}
			bucket["Properties"] = bp
		}
		nc, _ := bp["NotificationConfiguration"].(map[string]any)
		if nc == nil {
			nc = map[string]any{}
			bp["NotificationConfiguration"] = nc
		}
		evs, ok := p["Events"].([]any)
		if s, isStr := p["Events"].(string); isStr {
			evs, ok = []any{s}, true
		}
		if !ok {
			x.fail("%s: Events must be a string or a list", what)
			return
		}
		lcs, _ := nc["LambdaConfigurations"].([]any)
		for _, e := range evs {
			c := map[string]any{"Event": e, "Function": fn.arn}
			if f := p["Filter"]; f != nil {
				c["Filter"] = f
			}
			lcs = append(lcs, c)
		}
		nc["LambdaConfigurations"] = lcs
		// the bucket may only call the function once the permission is there
		deps, _ := bucket["DependsOn"].([]any)
		if d, ok := bucket["DependsOn"].(string); ok {
			deps = []any{d}
		}
		bucket["DependsOn"] = append(deps, base+"Permission")
	case "Schedule":
		x.check(what, p, "Schedule Input Enabled Name Description State")
		if !need("Schedule") {
			return
		}
		x.samRule(src, base, base+"Permission", fn, name+"LambdaTarget", map[string]any{"ScheduleExpression": p["Schedule"]}, p, "Input")
	case "EventBridgeRule", "CloudWatchEvent":
		allowed := "Pattern Input InputPath Target Enabled State"
		if typ == "EventBridgeRule" {
			allowed += " EventBusName"
		}
		x.check(what, p, allowed)
		if !need("Pattern") {
			return
		}
		rp := map[string]any{"EventPattern": p["Pattern"]}
		if p["EventBusName"] != nil {
			rp["EventBusName"] = p["EventBusName"]
		}
		x.samRule(src, base, base+"Permission", fn, name+"LambdaTarget", rp, p, "Input", "InputPath")
	case "ScheduleV2":
		x.check(what, p, "ScheduleExpression ScheduleExpressionTimezone FlexibleTimeWindow Name GroupName Description State Input RoleArn")
		if !need("ScheduleExpression") {
			return
		}
		role := p["RoleArn"]
		if role == nil {
			rid := base + "ScheduleRole"
			x.add(src, rid, "AWS::IAM::Role", map[string]any{
				"AssumeRolePolicyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
					"Action": "sts:AssumeRole", "Effect": "Allow", "Principal": map[string]any{"Service": "scheduler.amazonaws.com"}}}},
				"Policies": []any{map[string]any{"PolicyName": "SchedulerRolePolicy", "PolicyDocument": map[string]any{"Version": "2012-10-17",
					"Statement": []any{map[string]any{"Action": "lambda:InvokeFunction", "Effect": "Allow", "Resource": fn.arn}}}}},
			})
			role = samAtt(rid, "Arn")
		}
		x.samSchedule(src, base, p, fn.arn, role)
	case "Api":
		x.check(what, p, "Path Method RestApiId Auth")
		if !need("Path", "Method") {
			return
		}
		r := x.restOf(what, p)
		if r == nil {
			return
		}
		method := strings.ToLower(str(p["Method"]))
		integ := map[string]any{"type": "aws_proxy", "httpMethod": "POST",
			"uri": samSub("arn:${AWS::Partition}:apigateway:${AWS::Region}:lambda:path/2015-03-31/functions/${"+fn.sub+"}/invocations", nil)}
		x.restRoute(what, r, str(p["Path"]), method, integ, p["Auth"])
		stage := "Prod"
		if s, ok := r.props["StageName"].(string); ok && r.explicit {
			stage = samAlnum(s)
		}
		x.samPermission(src, base+"Permission"+stage, fn, "apigateway.amazonaws.com", map[string]any{"SourceArn": samExecArn(r.id, method, str(p["Path"]))})
	case "HttpApi":
		x.check(what, p, "Path Method ApiId Auth PayloadFormatVersion TimeoutInMillis")
		h := x.httpOf(what, p)
		if h == nil {
			return
		}
		method := strings.ToUpper(str(p["Method"]))
		path := str(p["Path"])
		key := "$default"
		if path != "$default" {
			if !need("Path", "Method") {
				return
			}
			key = method + " " + path
		}
		ip := map[string]any{"IntegrationType": "AWS_PROXY", "IntegrationUri": fn.arn, "IntegrationMethod": "POST", "PayloadFormatVersion": "2.0"}
		if v := p["PayloadFormatVersion"]; v != nil {
			ip["PayloadFormatVersion"] = v
		}
		if v := p["TimeoutInMillis"]; v != nil {
			ip["TimeoutInMillis"] = v
		}
		x.httpRoute(src, what, h, base, key, ip, p["Auth"])
		arn := "arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/*/$default"
		if key != "$default" {
			arn = "arn:${AWS::Partition}:execute-api:${AWS::Region}:${AWS::AccountId}:${__ApiId__}/*/" + samMethodSeg(method) + samStar(path)
		}
		x.samPermission(src, base+"Permission", fn, "apigateway.amazonaws.com", map[string]any{"SourceArn": samSub(arn, map[string]any{"__ApiId__": samRef(h.id)})})
	default:
		x.fail("pogi does not support the SAM event type %s (event %s of %s) yet", typ, name, src.id)
	}
}

// samRule adds an EventBridge rule that calls fn (or a state machine) and
// the permission that lets it. extra is the rule's own properties; keys of p
// named in targetKeys go to the target.
func (x *samExp) samRule(src *samSrc, ruleID, permID string, fn *samFn, targetID string, rp, p map[string]any, targetKeys ...string) {
	rp = samRuleBase(rp, p)
	t := map[string]any{"Arn": fn.arn, "Id": targetID}
	for _, k := range targetKeys {
		if p[k] != nil {
			t[k] = p[k]
		}
	}
	rp["Targets"] = []any{t}
	x.add(src, ruleID, "AWS::Events::Rule", rp)
	x.samPermission(src, permID, fn, "events.amazonaws.com", map[string]any{"SourceArn": samAtt(ruleID, "Arn")})
}

// samRuleBase adds the state, name and description every SAM rule event has.
func samRuleBase(rp, p map[string]any) map[string]any {
	rp["State"] = "ENABLED"
	if s := p["State"]; s != nil {
		rp["State"] = s
	} else if p["Enabled"] == false {
		rp["State"] = "DISABLED"
	}
	for _, k := range []string{"Name", "Description"} {
		if p[k] != nil {
			rp[k] = p[k]
		}
	}
	return rp
}

// samSchedule adds an AWS::Scheduler::Schedule for a ScheduleV2 event.
func (x *samExp) samSchedule(src *samSrc, id string, p map[string]any, target, role any) {
	t := map[string]any{"Arn": target, "RoleArn": role}
	if p["Input"] != nil {
		t["Input"] = p["Input"]
	}
	sp := map[string]any{"ScheduleExpression": p["ScheduleExpression"], "Target": t, "FlexibleTimeWindow": map[string]any{"Mode": "OFF"}, "State": "ENABLED"}
	for _, k := range []string{"ScheduleExpressionTimezone", "FlexibleTimeWindow", "Name", "GroupName", "Description", "State"} {
		if p[k] != nil {
			sp[k] = p[k]
		}
	}
	x.add(src, id, "AWS::Scheduler::Schedule", sp)
}

// ---- AWS::Serverless::SimpleTable ----

func (x *samExp) simpleTable(src *samSrc, props map[string]any) {
	what := "AWS::Serverless::SimpleTable " + src.id
	x.check(what, props, "PrimaryKey ProvisionedThroughput TableName Tags SSESpecification PointInTimeRecoverySpecification")
	if x.err != nil {
		return
	}
	name, typ := "id", "S"
	if pk, ok := props["PrimaryKey"].(map[string]any); ok {
		name = str(pk["Name"])
		typ = map[string]string{"String": "S", "Number": "N", "Binary": "B"}[str(pk["Type"])]
		if name == "" || typ == "" {
			x.fail("%s: PrimaryKey needs a Name and a Type of String, Number or Binary", what)
			return
		}
	}
	tp := map[string]any{
		"AttributeDefinitions": []any{map[string]any{"AttributeName": name, "AttributeType": typ}},
		"KeySchema":            []any{map[string]any{"AttributeName": name, "KeyType": "HASH"}},
		"Tags":                 samTags(props["Tags"], "lambda"),
	}
	if t := props["ProvisionedThroughput"]; t != nil {
		tp["ProvisionedThroughput"] = t
	} else {
		tp["BillingMode"] = "PAY_PER_REQUEST"
	}
	for _, k := range []string{"TableName", "SSESpecification", "PointInTimeRecoverySpecification"} {
		if props[k] != nil {
			tp[k] = props[k]
		}
	}
	x.addMain(src, src.id, "AWS::DynamoDB::Table", tp)
}

// ---- AWS::Serverless::LayerVersion ----

func (x *samExp) layer(src *samSrc, props map[string]any) {
	what := "AWS::Serverless::LayerVersion " + src.id
	x.check(what, props, "LayerName Description ContentUri CompatibleRuntimes CompatibleArchitectures LicenseInfo RetentionPolicy")
	if x.err != nil {
		return
	}
	b, k, v, ok := x.samS3Loc(what+" ContentUri", props["ContentUri"])
	if !ok {
		return
	}
	content := map[string]any{"S3Bucket": b, "S3Key": k}
	if v != nil {
		content["S3ObjectVersion"] = v
	}
	lp := map[string]any{"Content": content}
	for _, f := range strings.Fields("Description CompatibleRuntimes CompatibleArchitectures LicenseInfo") {
		if props[f] != nil {
			lp[f] = props[f]
		}
	}
	if props["LayerName"] != nil {
		lp["LayerName"] = props["LayerName"]
	} else {
		lp["LayerName"] = src.id
	}
	// a new hash for new content: the layer is a new resource, the old version stays
	id := src.id + samHash(lp)
	r := x.addMain(src, id, "AWS::Lambda::LayerVersion", lp)
	if r != nil {
		r["DeletionPolicy"] = "Retain"
		if str(props["RetentionPolicy"]) == "Delete" {
			r["DeletionPolicy"] = "Delete"
		}
	}
	x.rename[src.id] = id
}

// ---- AWS::Serverless::StateMachine ----

func (x *samExp) stateMachine(src *samSrc, props map[string]any) {
	id := src.id
	what := "AWS::Serverless::StateMachine " + id
	x.check(what, props, "Definition DefinitionUri DefinitionSubstitutions Role Policies PermissionsBoundary RolePath Events Name Type Logging Tracing Tags")
	if x.err != nil {
		return
	}
	sp := map[string]any{"Tags": samTags(props["Tags"], "stateMachine")}
	switch {
	case props["Definition"] != nil:
		sp["Definition"] = props["Definition"]
	case props["DefinitionUri"] != nil:
		body, ok := x.samS3Doc(what+" DefinitionUri", props["DefinitionUri"])
		if !ok {
			return
		}
		sp["DefinitionString"] = body
	default:
		x.fail("%s: Either Definition or DefinitionUri must be specified", what)
		return
	}
	for from, to := range map[string]string{"DefinitionSubstitutions": "DefinitionSubstitutions", "Name": "StateMachineName", "Type": "StateMachineType", "Logging": "LoggingConfiguration", "Tracing": "TracingConfiguration"} {
		if props[from] != nil {
			sp[to] = props[from]
		}
	}
	if props["Role"] != nil {
		sp["RoleArn"] = props["Role"]
	} else {
		sp["RoleArn"] = x.samRole(src, id+"Role", "states.amazonaws.com", props, nil, nil)
	}
	if x.err != nil {
		return
	}
	x.addMain(src, id, "AWS::StepFunctions::StateMachine", sp)

	events, _ := props["Events"].(map[string]any)
	for _, name := range core.SortedKeys(events) {
		ev, _ := events[name].(map[string]any)
		p, _ := ev["Properties"].(map[string]any)
		if p == nil {
			p = map[string]any{}
		}
		typ := str(ev["Type"])
		ewhat := fmt.Sprintf("event %s of %s", name, id)
		base := id + name
		// the role that lets the event start an execution
		eventRole := func(service string) any {
			if p["RoleArn"] != nil {
				return p["RoleArn"]
			}
			rid := base + "Role"
			x.add(src, rid, "AWS::IAM::Role", map[string]any{
				"AssumeRolePolicyDocument": map[string]any{"Version": "2012-10-17", "Statement": []any{map[string]any{
					"Action": "sts:AssumeRole", "Effect": "Allow", "Principal": map[string]any{"Service": service}}}},
				"Policies": []any{map[string]any{"PolicyName": base + "RolePolicy", "PolicyDocument": map[string]any{"Version": "2012-10-17",
					"Statement": []any{map[string]any{"Action": "states:StartExecution", "Effect": "Allow", "Resource": samRef(id)}}}}},
			})
			return samAtt(rid, "Arn")
		}
		switch typ {
		case "Schedule", "EventBridgeRule", "CloudWatchEvent":
			rp := map[string]any{}
			switch typ {
			case "Schedule":
				x.check(ewhat, p, "Schedule Input Enabled Name Description State RoleArn")
				rp["ScheduleExpression"] = p["Schedule"]
			default:
				x.check(ewhat, p, "Pattern Input InputPath Target Enabled State EventBusName RoleArn")
				rp["EventPattern"] = p["Pattern"]
				if p["EventBusName"] != nil {
					rp["EventBusName"] = p["EventBusName"]
				}
			}
			if x.err != nil {
				return
			}
			rp = samRuleBase(rp, p)
			t := map[string]any{"Arn": samRef(id), "Id": name + "StepFunctionsTarget", "RoleArn": eventRole("events.amazonaws.com")}
			for _, k := range []string{"Input", "InputPath"} {
				if p[k] != nil {
					t[k] = p[k]
				}
			}
			rp["Targets"] = []any{t}
			x.add(src, base, "AWS::Events::Rule", rp)
		case "ScheduleV2":
			x.check(ewhat, p, "ScheduleExpression ScheduleExpressionTimezone FlexibleTimeWindow Name GroupName Description State Input RoleArn")
			if x.err != nil {
				return
			}
			x.samSchedule(src, base, p, samRef(id), eventRole("scheduler.amazonaws.com"))
		case "Api":
			x.check(ewhat, p, "Path Method RestApiId Auth")
			if p["Path"] == nil || p["Method"] == nil {
				x.fail("%s: Path and Method are required", ewhat)
				return
			}
			r := x.restOf(ewhat, p)
			if r == nil {
				return
			}
			role := eventRole("apigateway.amazonaws.com")
			integ := map[string]any{"type": "aws", "httpMethod": "POST", "credentials": role,
				"uri":              samSub("arn:${AWS::Partition}:apigateway:${AWS::Region}:states:action/StartExecution", nil),
				"requestTemplates": map[string]any{"application/json": samSub(`{"input": "$util.escapeJavaScript($input.json('$'))", "stateMachineArn": "${__SMArn__}"}`, map[string]any{"__SMArn__": samRef(id)})},
				"responses":        map[string]any{"default": map[string]any{"statusCode": "200"}}}
			x.restRoute(ewhat, r, str(p["Path"]), strings.ToLower(str(p["Method"])), integ, p["Auth"])
		default:
			x.fail("pogi does not support the SAM event type %s (event %s of %s) yet", typ, name, id)
		}
		if x.err != nil {
			return
		}
	}
}
