package appsync_test

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/appsync"
	"github.com/aws/aws-sdk-go-v2/service/appsync/types"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	lambdatypes "github.com/aws/aws-sdk-go-v2/service/lambda/types"
	"github.com/coder/websocket"
	appsyncsvc "github.com/nrndev/pogi/internal/aws/appsync"
	cognitosvc "github.com/nrndev/pogi/internal/aws/cognito"
	ddbsvc "github.com/nrndev/pogi/internal/aws/dynamodb"
	lambdasvc "github.com/nrndev/pogi/internal/aws/lambda"
	rdsdatasvc "github.com/nrndev/pogi/internal/aws/rdsdata"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

type M = map[string]any

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

type env struct {
	t   *testing.T
	url string
	as  *appsync.Client
	cfg aws.Config
}

func setup(t *testing.T) *env {
	h, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return appsyncsvc.New(h) },
		func(h *core.Hub) core.Service { return ddbsvc.New(h) },
		func(h *core.Hub) core.Service { return lambdasvc.New(h) },
		func(h *core.Hub) core.Service { return cognitosvc.New(h) })
	h.Cfg.CORSOrigins = []string{"https://app.example"} // the front door lets the browser origin in first
	cfg := awstest.Config(t, url)
	return &env{t, url, appsync.NewFromConfig(cfg), cfg}
}

// api creates an API with a schema and returns its id.
func (e *env) api(in *appsync.CreateGraphqlApiInput, sdl string) string {
	e.t.Helper()
	id := aws.ToString(must(e.as.CreateGraphqlApi(ctx, in)).GraphqlApi.ApiId)
	must(e.as.StartSchemaCreation(ctx, &appsync.StartSchemaCreationInput{ApiId: &id, Definition: []byte(sdl)}))
	if st := must(e.as.GetSchemaCreationStatus(ctx, &appsync.GetSchemaCreationStatusInput{ApiId: &id})); st.Status != types.SchemaStatusSuccess {
		e.t.Fatalf("schema: %s %s", st.Status, aws.ToString(st.Details))
	}
	return id
}

func (e *env) key(id string) string {
	e.t.Helper()
	return aws.ToString(must(e.as.CreateApiKey(ctx, &appsync.CreateApiKeyInput{ApiId: &id})).ApiKey.Id)
}

func (e *env) source(id, name string, typ types.DataSourceType, cfg func(*appsync.CreateDataSourceInput)) {
	e.t.Helper()
	in := &appsync.CreateDataSourceInput{ApiId: &id, Name: &name, Type: typ}
	if cfg != nil {
		cfg(in)
	}
	must(e.as.CreateDataSource(ctx, in))
}

var jsRuntime = &types.AppSyncRuntime{Name: types.RuntimeNameAppsyncJs, RuntimeVersion: aws.String("1.0.0")}

// resolve attaches a unit resolver: JS code, or VTL request and response templates.
func (e *env) resolve(id, typ, field, source, code, req, resp string) {
	e.t.Helper()
	in := &appsync.CreateResolverInput{ApiId: &id, TypeName: &typ, FieldName: &field, DataSourceName: &source}
	if code != "" {
		in.Runtime, in.Code = jsRuntime, &code
	}
	if req != "" {
		in.RequestMappingTemplate, in.ResponseMappingTemplate = &req, &resp
	}
	must(e.as.CreateResolver(ctx, in))
}

func port(u string) string { return u[strings.LastIndexByte(u, ':')+1:] }

// request builds a GraphQL request to the API's host (the <id>.appsync-api.localhost form).
func (e *env) request(id, query string, vars M) *http.Request {
	body, _ := json.Marshal(M{"query": query, "variables": vars})
	req, _ := http.NewRequest(http.MethodPost, e.url+"/graphql", bytes.NewReader(body))
	req.Host = id + ".appsync-api.localhost:" + port(e.url)
	req.Header.Set("Content-Type", "application/json")
	return req
}

func (e *env) send(req *http.Request) (int, M) {
	e.t.Helper()
	resp := must(http.DefaultClient.Do(req))
	defer resp.Body.Close()
	var out M
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		e.t.Fatalf("response: %v", err)
	}
	return resp.StatusCode, out
}

// gql runs a query with headers and fails on any error.
func (e *env) gql(id string, hdr map[string]string, query string, vars M) M {
	e.t.Helper()
	req := e.request(id, query, vars)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	status, out := e.send(req)
	if status != 200 || out["errors"] != nil {
		e.t.Fatalf("%s: %d %v", query, status, out)
	}
	return out["data"].(M)
}

func errorType(out M) string {
	errs, _ := out["errors"].([]any)
	if len(errs) == 0 {
		return ""
	}
	return errs[0].(M)["errorType"].(string)
}

func TestControlPlane(t *testing.T) {
	e := setup(t)
	c := e.as
	api := must(c.CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{Name: aws.String("cp"), AuthenticationType: types.AuthenticationTypeApiKey,
		Tags: map[string]string{"team": "core"}})).GraphqlApi
	id := aws.ToString(api.ApiId)
	if api.Uris["GRAPHQL"] != "http://"+id+".appsync-api.localhost:"+port(e.url)+"/graphql" ||
		!strings.HasPrefix(api.Uris["REALTIME"], "ws://"+id+".appsync-realtime-api.localhost:") ||
		aws.ToString(api.Arn) != "arn:aws:appsync:us-east-1:000000000000:apis/"+id || api.Visibility != types.GraphQLApiVisibilityGlobal {
		t.Fatalf("api = %+v", api)
	}
	upd := must(c.UpdateGraphqlApi(ctx, &appsync.UpdateGraphqlApiInput{ApiId: &id, Name: aws.String("cp2"), AuthenticationType: types.AuthenticationTypeApiKey,
		AdditionalAuthenticationProviders: []types.AdditionalAuthenticationProvider{{AuthenticationType: types.AuthenticationTypeAwsIam}}})).GraphqlApi
	got := must(c.GetGraphqlApi(ctx, &appsync.GetGraphqlApiInput{ApiId: &id})).GraphqlApi
	if aws.ToString(upd.Name) != "cp2" || len(got.AdditionalAuthenticationProviders) != 1 || got.Tags["team"] != "core" {
		t.Fatalf("updated = %+v", got)
	}
	if l := must(c.ListGraphqlApis(ctx, &appsync.ListGraphqlApisInput{})); len(l.GraphqlApis) != 1 {
		t.Fatalf("list = %+v", l.GraphqlApis)
	}
	var nf *types.NotFoundException
	if _, err := c.GetGraphqlApi(ctx, &appsync.GetGraphqlApiInput{ApiId: aws.String("nope")}); !errors.As(err, &nf) {
		t.Fatalf("get missing = %v", err)
	}
	var bad *types.BadRequestException
	if _, err := c.CreateGraphqlApi(ctx, &appsync.CreateGraphqlApiInput{Name: aws.String("x"), AuthenticationType: types.AuthenticationTypeAmazonCognitoUserPools}); !errors.As(err, &bad) {
		t.Fatalf("cognito without a pool = %v", err)
	}

	// Schema: a failure leaves the status FAILED with details.
	must(c.StartSchemaCreation(ctx, &appsync.StartSchemaCreationInput{ApiId: &id, Definition: []byte("type Query { broken: Nope }")}))
	if st := must(c.GetSchemaCreationStatus(ctx, &appsync.GetSchemaCreationStatusInput{ApiId: &id})); st.Status != types.SchemaStatusFailed ||
		!strings.Contains(aws.ToString(st.Details), "Nope") {
		t.Fatalf("bad schema status = %s %s", st.Status, aws.ToString(st.Details))
	}
	sdl := `type Post @aws_api_key { id: ID! title: String date: AWSDateTime }
type Query { post(id: ID!): Post @aws_api_key @aws_iam }
type Mutation { addPost(title: String!): Post }
type Subscription { onPost: Post @aws_subscribe(mutations: ["addPost"]) }`
	must(c.StartSchemaCreation(ctx, &appsync.StartSchemaCreationInput{ApiId: &id, Definition: []byte(sdl)}))
	if st := must(c.GetSchemaCreationStatus(ctx, &appsync.GetSchemaCreationStatusInput{ApiId: &id})); st.Status != types.SchemaStatusSuccess {
		t.Fatalf("schema status = %s %s", st.Status, aws.ToString(st.Details))
	}
	printed := must(c.GetIntrospectionSchema(ctx, &appsync.GetIntrospectionSchemaInput{ApiId: &id, Format: types.OutputTypeSdl})).Schema
	intro := must(c.GetIntrospectionSchema(ctx, &appsync.GetIntrospectionSchemaInput{ApiId: &id, Format: types.OutputTypeJson})).Schema
	if !strings.Contains(string(printed), "type Post") || strings.Contains(string(printed), "scalar AWSDateTime") ||
		!strings.Contains(string(intro), `"__schema"`) || !strings.Contains(string(intro), `"name":"addPost"`) {
		t.Fatalf("introspection:\n%s\n%.300s", printed, intro)
	}
	ts := must(c.ListTypes(ctx, &appsync.ListTypesInput{ApiId: &id, Format: types.TypeDefinitionFormatSdl})).Types
	pt := must(c.GetType(ctx, &appsync.GetTypeInput{ApiId: &id, TypeName: aws.String("Post"), Format: types.TypeDefinitionFormatSdl})).Type
	if len(ts) != 4 || !strings.Contains(aws.ToString(pt.Definition), "title: String") {
		t.Fatalf("types = %d, Post = %s", len(ts), aws.ToString(pt.Definition))
	}
	if _, err := c.GetType(ctx, &appsync.GetTypeInput{ApiId: &id, TypeName: aws.String("Nope"), Format: types.TypeDefinitionFormatSdl}); !errors.As(err, &nf) {
		t.Fatalf("missing type = %v", err)
	}

	// API keys: 1 to 365 days.
	exp := time.Now().Add(30 * 24 * time.Hour).Unix()
	key := must(c.CreateApiKey(ctx, &appsync.CreateApiKeyInput{ApiId: &id, Description: aws.String("web"), Expires: exp})).ApiKey
	var bounds *types.ApiKeyValidityOutOfBoundsException
	if _, err := c.CreateApiKey(ctx, &appsync.CreateApiKeyInput{ApiId: &id, Expires: time.Now().Add(2 * time.Hour).Unix()}); !errors.As(err, &bounds) {
		t.Fatalf("short key = %v", err)
	}
	must(c.UpdateApiKey(ctx, &appsync.UpdateApiKeyInput{ApiId: &id, Id: key.Id, Description: aws.String("mobile")}))
	keys := must(c.ListApiKeys(ctx, &appsync.ListApiKeysInput{ApiId: &id})).ApiKeys
	if !strings.HasPrefix(aws.ToString(key.Id), "da2-") || key.Expires != exp/3600*3600 || len(keys) != 1 || aws.ToString(keys[0].Description) != "mobile" {
		t.Fatalf("key = %+v, keys = %+v", key, keys)
	}
	must(c.DeleteApiKey(ctx, &appsync.DeleteApiKeyInput{ApiId: &id, Id: key.Id}))

	// Data sources.
	e.source(id, "none", types.DataSourceTypeNone, nil)
	e.source(id, "posts", types.DataSourceTypeAmazonDynamodb, func(in *appsync.CreateDataSourceInput) {
		in.DynamodbConfig = &types.DynamodbDataSourceConfig{TableName: aws.String("posts"), AwsRegion: aws.String("us-east-1")}
	})
	if _, err := c.CreateDataSource(ctx, &appsync.CreateDataSourceInput{ApiId: &id, Name: aws.String("none"), Type: types.DataSourceTypeNone}); !errors.As(err, &bad) {
		t.Fatalf("duplicate data source = %v", err)
	}
	if _, err := c.CreateDataSource(ctx, &appsync.CreateDataSourceInput{ApiId: &id, Name: aws.String("fn"), Type: types.DataSourceTypeAwsLambda}); !errors.As(err, &bad) {
		t.Fatalf("lambda without config = %v", err)
	}
	must(c.UpdateDataSource(ctx, &appsync.UpdateDataSourceInput{ApiId: &id, Name: aws.String("posts"), Type: types.DataSourceTypeAmazonDynamodb,
		Description: aws.String("all posts"), DynamodbConfig: &types.DynamodbDataSourceConfig{TableName: aws.String("posts2"), AwsRegion: aws.String("us-east-1")}}))
	ds := must(c.GetDataSource(ctx, &appsync.GetDataSourceInput{ApiId: &id, Name: aws.String("posts")})).DataSource
	if aws.ToString(ds.DynamodbConfig.TableName) != "posts2" || aws.ToString(ds.Description) != "all posts" ||
		aws.ToString(ds.DataSourceArn) != "arn:aws:appsync:us-east-1:000000000000:apis/"+id+"/datasources/posts" {
		t.Fatalf("data source = %+v", ds)
	}
	if l := must(c.ListDataSources(ctx, &appsync.ListDataSourcesInput{ApiId: &id})).DataSources; len(l) != 2 {
		t.Fatalf("data sources = %d", len(l))
	}

	// Functions and resolvers.
	fn := must(c.CreateFunction(ctx, &appsync.CreateFunctionInput{ApiId: &id, Name: aws.String("load"), DataSourceName: aws.String("none"),
		Runtime: jsRuntime, Code: aws.String("export function request(ctx) { return { payload: 1 } }\nexport function response(ctx) { return ctx.result }")})).FunctionConfiguration
	fid := fn.FunctionId
	if _, err := c.CreateFunction(ctx, &appsync.CreateFunctionInput{ApiId: &id, Name: aws.String("broken"), DataSourceName: aws.String("none"),
		Runtime: jsRuntime, Code: aws.String("export function request(ctx) { return {")}); !errors.As(err, &bad) {
		t.Fatalf("broken code = %v", err)
	}
	must(c.UpdateFunction(ctx, &appsync.UpdateFunctionInput{ApiId: &id, FunctionId: fid, Name: aws.String("load"), DataSourceName: aws.String("none"),
		Description: aws.String("loads"), FunctionVersion: aws.String("2018-05-29"), RequestMappingTemplate: aws.String(`{"version":"2018-05-29","payload":1}`),
		ResponseMappingTemplate: aws.String("$util.toJson($ctx.result)")}))
	if f := must(c.GetFunction(ctx, &appsync.GetFunctionInput{ApiId: &id, FunctionId: fid})).FunctionConfiguration; aws.ToString(f.Description) != "loads" {
		t.Fatalf("function = %+v", f)
	}
	if l := must(c.ListFunctions(ctx, &appsync.ListFunctionsInput{ApiId: &id})).Functions; len(l) != 1 {
		t.Fatalf("functions = %d", len(l))
	}
	e.resolve(id, "Query", "post", "none", "", `{"version":"2018-05-29","payload":{"id":"1"}}`, "$util.toJson($ctx.result)")
	must(c.CreateResolver(ctx, &appsync.CreateResolverInput{ApiId: &id, TypeName: aws.String("Mutation"), FieldName: aws.String("addPost"),
		Kind: types.ResolverKindPipeline, PipelineConfig: &types.PipelineConfig{Functions: []string{aws.ToString(fid)}},
		Runtime: jsRuntime, Code: aws.String("export function request(ctx) { return {} }\nexport function response(ctx) { return ctx.prev.result }")}))
	if _, err := c.CreateResolver(ctx, &appsync.CreateResolverInput{ApiId: &id, TypeName: aws.String("Query"), FieldName: aws.String("post"),
		DataSourceName: aws.String("none")}); !errors.As(err, &bad) {
		t.Fatalf("second resolver = %v", err)
	}
	if _, err := c.CreateResolver(ctx, &appsync.CreateResolverInput{ApiId: &id, TypeName: aws.String("Nope"), FieldName: aws.String("x"),
		DataSourceName: aws.String("none")}); !errors.As(err, &nf) {
		t.Fatalf("resolver on a missing type = %v", err)
	}
	must(c.UpdateResolver(ctx, &appsync.UpdateResolverInput{ApiId: &id, TypeName: aws.String("Query"), FieldName: aws.String("post"),
		DataSourceName: aws.String("posts"), RequestMappingTemplate: aws.String("{}"), ResponseMappingTemplate: aws.String("{}")}))
	r := must(c.GetResolver(ctx, &appsync.GetResolverInput{ApiId: &id, TypeName: aws.String("Query"), FieldName: aws.String("post")})).Resolver
	if aws.ToString(r.DataSourceName) != "posts" || r.Kind != types.ResolverKindUnit ||
		aws.ToString(r.ResolverArn) != "arn:aws:appsync:us-east-1:000000000000:apis/"+id+"/types/Query/resolvers/post" {
		t.Fatalf("resolver = %+v", r)
	}
	byFn := must(c.ListResolversByFunction(ctx, &appsync.ListResolversByFunctionInput{ApiId: &id, FunctionId: fid})).Resolvers
	if l := must(c.ListResolvers(ctx, &appsync.ListResolversInput{ApiId: &id, TypeName: aws.String("Query")})).Resolvers; len(l) != 1 || len(byFn) != 1 {
		t.Fatalf("resolvers = %d, by function = %d", len(l), len(byFn))
	}
	var busy *types.ConcurrentModificationException
	if _, err := c.DeleteFunction(ctx, &appsync.DeleteFunctionInput{ApiId: &id, FunctionId: fid}); !errors.As(err, &busy) {
		t.Fatalf("delete a function in use = %v", err)
	}
	must(c.DeleteResolver(ctx, &appsync.DeleteResolverInput{ApiId: &id, TypeName: aws.String("Mutation"), FieldName: aws.String("addPost")}))
	must(c.DeleteFunction(ctx, &appsync.DeleteFunctionInput{ApiId: &id, FunctionId: fid}))
	must(c.DeleteDataSource(ctx, &appsync.DeleteDataSourceInput{ApiId: &id, Name: aws.String("none")}))
	if _, err := c.GetResolver(ctx, &appsync.GetResolverInput{ApiId: &id, TypeName: aws.String("Mutation"), FieldName: aws.String("addPost")}); !errors.As(err, &nf) {
		t.Fatalf("deleted resolver = %v", err)
	}

	// Tags.
	must(c.TagResource(ctx, &appsync.TagResourceInput{ResourceArn: api.Arn, Tags: map[string]string{"env": "dev"}}))
	must(c.UntagResource(ctx, &appsync.UntagResourceInput{ResourceArn: api.Arn, TagKeys: []string{"team"}}))
	if tags := must(c.ListTagsForResource(ctx, &appsync.ListTagsForResourceInput{ResourceArn: api.Arn})).Tags; len(tags) != 1 || tags["env"] != "dev" {
		t.Fatalf("tags = %v", tags)
	}

	must(c.DeleteGraphqlApi(ctx, &appsync.DeleteGraphqlApiInput{ApiId: &id}))
	if _, err := c.GetGraphqlApi(ctx, &appsync.GetGraphqlApiInput{ApiId: &id}); !errors.As(err, &nf) {
		t.Fatalf("deleted api = %v", err)
	}
}

func TestEvaluate(t *testing.T) {
	c := setup(t).as
	tmpl := `## build the request
#set($items = [])
#foreach($i in [1..3])
  #if($i % 2 == 1)$util.qr($items.add($i * 10))#end
#end
#set($ctx.stash.count = $items.size())
#set($m = {"a": 1})
$util.qr($m.put("b", [true, $null]))
$util.appendError("note", "Warn")
{
  "id": $util.toJson($ctx.args.id),
  "items": $util.toJson($items),
  "key": $util.dynamodb.toDynamoDBJson($ctx.args.id),
  "name": "${ctx.args.name}!",
  "empty": $util.isNullOrEmpty($ctx.args.missing),
  "keys": $util.toJson($m.keySet()),
  #if($ctx.args.name == "bob" && !($items.size() > 5))"bob": true,#elseif($ctx.args.name)"other": true,#else"none": true,#end
  "upper": "$ctx.args.name.toUpperCase()",
  "m": $util.toJson($m),
  "unknown": "$ctx.args.missing$!ctx.args.missing"
}`
	out := must(c.EvaluateMappingTemplate(ctx, &appsync.EvaluateMappingTemplateInput{Template: &tmpl, Context: aws.String(`{"arguments":{"id":"x1","name":"bob"}}`)}))
	var res M
	if out.Error != nil || json.Unmarshal([]byte(aws.ToString(out.EvaluationResult)), &res) != nil {
		t.Fatalf("evaluation = %s %+v", aws.ToString(out.EvaluationResult), out.Error)
	}
	want := `{"bob":true,"empty":true,"id":"x1","items":[10,30],"key":{"S":"x1"},"keys":["a","b"],"m":{"a":1,"b":[true,null]},"name":"bob!","unknown":"$ctx.args.missing","upper":"BOB"}`
	if got, _ := json.Marshal(res); string(got) != want || !strings.Contains(aws.ToString(out.Stash), `"count":2`) ||
		!strings.Contains(aws.ToString(out.OutErrors), `"note"`) {
		t.Fatalf("got   %s\nwant  %s\nstash %s errors %s", got, want, aws.ToString(out.Stash), aws.ToString(out.OutErrors))
	}
	for tmpl, want := range map[string]string{
		`#return({"early": true})never`:                                              `{"early":true}`,
		`$util.toJson($util.rds.toJsonObject($ctx.result)[0][0])`:                    `{"id":1,"name":"a"}`,
		`$util.transform.toDynamoDBFilterExpression({"title": {"beginsWith": "a"}})`: `{"expression":"(begins_with(#title, :title_beginsWith0))","expressionNames":{"#title":"title"},"expressionValues":{":title_beginsWith0":{"S":"a"}}}`,
	} {
		rds := `{"sqlStatementResults":[{"records":[[{"longValue":1},{"stringValue":"a"}]],"columnMetadata":[{"label":"id"},{"label":"name"}]}]}`
		out := must(c.EvaluateMappingTemplate(ctx, &appsync.EvaluateMappingTemplateInput{Template: &tmpl, Context: aws.String(`{"result":` + strconvQuote(rds) + `}`)}))
		if aws.ToString(out.EvaluationResult) != want {
			t.Fatalf("%s = %s %+v", tmpl, aws.ToString(out.EvaluationResult), out.Error)
		}
	}
	for tmpl, want := range map[string]string{`$util.error("boom", "Custom")`: "boom", `#if($x`: "syntax error"} {
		out := must(c.EvaluateMappingTemplate(ctx, &appsync.EvaluateMappingTemplateInput{Template: aws.String(tmpl), Context: aws.String(`{}`)}))
		if out.Error == nil || !strings.Contains(aws.ToString(out.Error.Message), want) {
			t.Fatalf("%s = %+v", tmpl, out)
		}
	}

	code := `import { util } from '@aws-appsync/utils';
import * as ddb from '@aws-appsync/utils/dynamodb';
export function request(ctx) {
  ctx.stash.seen = true;
  console.log('query', ctx.args.id);
  return ddb.query({ query: { pk: { eq: ctx.args.id }, sk: { beginsWith: 'a' } }, limit: 5 });
}
export function response(ctx) {
  if (ctx.error) util.error(ctx.error.message, ctx.error.type);
  return ctx.result.items.map((it) => ({ ...it, seen: ctx.stash.seen ?? false }));
}`
	run := func(fn, context string) *appsync.EvaluateCodeOutput {
		return must(c.EvaluateCode(ctx, &appsync.EvaluateCodeInput{Runtime: jsRuntime, Code: &code, Function: &fn, Context: &context}))
	}
	req := run("request", `{"arguments":{"id":"p1"}}`)
	if r := aws.ToString(req.EvaluationResult); req.Error != nil || !strings.Contains(r, `"operation":"Query"`) || !strings.Contains(r, `begins_with(#sk`) ||
		!strings.Contains(r, `{"S":"p1"}`) || !strings.Contains(r, `"limit":5`) || !strings.Contains(aws.ToString(req.Stash), `"seen":true`) ||
		len(req.Logs) != 1 || req.Logs[0] != "query p1" {
		t.Fatalf("request = %s %+v %s %v", r, req.Error, aws.ToString(req.Stash), req.Logs)
	}
	if resp := run("response", `{"result":{"items":[{"pk":"p1"}]},"stash":{"seen":true}}`); aws.ToString(resp.EvaluationResult) != `[{"pk":"p1","seen":true}]` {
		t.Fatalf("response = %s %+v", aws.ToString(resp.EvaluationResult), resp.Error)
	}
	if failed := run("response", `{"error":{"message":"nope","type":"DynamoDB:X"}}`); failed.Error == nil || aws.ToString(failed.Error.Message) != "nope" {
		t.Fatalf("util.error = %+v", failed)
	}
}

func strconvQuote(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func zipFile(name, body string) []byte {
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, _ := zw.Create(name)
	io.WriteString(w, body)
	zw.Close()
	return buf.Bytes()
}

// pyFunction creates a native python3 Lambda function and returns its ARN.
func (e *env) pyFunction(name, code string) string {
	e.t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		e.t.Skip("python3 not installed")
	}
	fn := must(lambda.NewFromConfig(e.cfg).CreateFunction(ctx, &lambda.CreateFunctionInput{FunctionName: &name, Runtime: lambdatypes.RuntimePython312,
		Handler: aws.String("app.handler"), Role: aws.String("arn:aws:iam::000000000000:role/r"),
		Code: &lambdatypes.FunctionCode{ZipFile: zipFile("app.py", code)}}))
	return aws.ToString(fn.FunctionArn)
}

const itemSchema = `type Item { id: ID! name: String count: Int }
input ItemInput { id: ID name: String count: Int }
type Query {
  getItem(id: ID!): Item
  getItemJS(id: ID!): Item
  listItems: [Item!]
  hello(name: String!): String
  pipeline(n: Int!): AWSJSON
}
type Mutation {
  putItem(input: ItemInput!): Item
  putItemJS(input: ItemInput!): Item
}`

const (
	putVTL = `#set($id = $util.defaultIfNullOrEmpty($ctx.args.input.id, $util.autoId()))
#set($values = {})
#foreach($k in $ctx.args.input.keySet())
  #if($k != "id")$util.qr($values.put($k, $ctx.args.input.get($k)))#end
#end
{
  "version": "2018-05-29",
  "operation": "PutItem",
  "key": { "id": $util.dynamodb.toDynamoDBJson($id) },
  "attributeValues": $util.dynamodb.toMapValuesJson($values),
  "condition": { "expression": "attribute_not_exists(id)" }
}`
	getVTL = `{"version": "2018-05-29", "operation": "GetItem", "key": {"id": $util.dynamodb.toDynamoDBJson($ctx.args.id)}}`
	outVTL = `#if($ctx.error)$util.error($ctx.error.message, $ctx.error.type)#end
$util.toJson($ctx.result)`
	putJS = `import { util } from '@aws-appsync/utils';
import { put } from '@aws-appsync/utils/dynamodb';
export function request(ctx) {
  const { id, ...item } = ctx.args.input;
  return put({ key: { id: id ?? util.autoId() }, item });
}
export const response = (ctx) => {
  if (ctx.error) util.error(ctx.error.message, ctx.error.type);
  return ctx.result;
};`
	getJS = `import * as ddb from '@aws-appsync/utils/dynamodb';
export function request(ctx) { return ddb.get({ key: { id: ctx.args.id } }); }
export function response(ctx) { return ctx.result; }`
	scanJS = `import { scan } from '@aws-appsync/utils/dynamodb';
export function request(ctx) { return scan({ filter: { count: { gt: 0 } } }); }
export function response(ctx) { return ctx.result.items; }`
	helloPy = `def handler(event, context):
    name = event["arguments"]["name"]
    if name == "boom":
        raise ValueError("no booms")
    return "hello %s via %s.%s" % (name, event["info"]["parentTypeName"], event["info"]["fieldName"])
`
)

// TestResolvers is the done-when case: a query resolves through a python
// Lambda resolver and through DynamoDB resolvers in VTL and in JS.
func TestResolvers(t *testing.T) {
	e := setup(t)
	arn := e.pyFunction("hello", helloPy)
	must(dynamodb.NewFromConfig(e.cfg).CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("items"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("id"), AttributeType: ddbtypes.ScalarAttributeTypeS}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("id"), KeyType: ddbtypes.KeyTypeHash}},
		BillingMode:          ddbtypes.BillingModePayPerRequest}))
	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("items"), AuthenticationType: types.AuthenticationTypeApiKey}, itemSchema)
	hdr := map[string]string{"x-api-key": e.key(id)}
	e.source(id, "items", types.DataSourceTypeAmazonDynamodb, func(in *appsync.CreateDataSourceInput) {
		in.DynamodbConfig = &types.DynamodbDataSourceConfig{TableName: aws.String("items"), AwsRegion: aws.String("us-east-1")}
	})
	e.source(id, "fn", types.DataSourceTypeAwsLambda, func(in *appsync.CreateDataSourceInput) {
		in.LambdaConfig = &types.LambdaDataSourceConfig{LambdaFunctionArn: &arn}
	})
	e.source(id, "none", types.DataSourceTypeNone, nil)
	e.resolve(id, "Mutation", "putItem", "items", "", putVTL, outVTL)
	e.resolve(id, "Query", "getItem", "items", "", getVTL, outVTL)
	e.resolve(id, "Mutation", "putItemJS", "items", putJS, "", "")
	e.resolve(id, "Query", "getItemJS", "items", getJS, "", "")
	e.resolve(id, "Query", "listItems", "items", scanJS, "", "")
	e.resolve(id, "Query", "hello", "fn", "", "", "") // a direct Lambda resolver

	put := e.gql(id, hdr, `mutation($in: ItemInput!) { putItem(input: $in) { id name count } }`, M{"in": M{"name": "vtl", "count": 2}})["putItem"].(M)
	vid := put["id"].(string)
	if len(vid) != 36 || put["name"] != "vtl" || put["count"] != 2.0 {
		t.Fatalf("putItem = %v", put)
	}
	if got := e.gql(id, hdr, `query Get($id: ID!) { getItem(id: $id) { ...F } } fragment F on Item { id name __typename }`, M{"id": vid})["getItem"]; got.(M)["name"] != "vtl" || got.(M)["__typename"] != "Item" {
		t.Fatalf("getItem = %v", got)
	}
	jsPut := e.gql(id, hdr, `mutation { putItemJS(input: {id: "js-1", name: "js", count: 3}) { id name } }`, nil)["putItemJS"].(M)
	jsGet := e.gql(id, hdr, `{ a: getItemJS(id: "js-1") { name count } b: getItemJS(id: "missing") { name } }`, nil)
	if jsPut["id"] != "js-1" || jsGet["a"].(M)["count"] != 3.0 || jsGet["b"] != nil {
		t.Fatalf("putItemJS = %v, getItemJS = %v", jsPut, jsGet)
	}
	if items := e.gql(id, hdr, `{ listItems { id } }`, nil)["listItems"].([]any); len(items) != 2 {
		t.Fatalf("listItems = %v", items)
	}
	if got := e.gql(id, hdr, `{ hello(name: "bob") }`, nil)["hello"]; got != "hello bob via Query.hello" {
		t.Fatalf("hello = %v", got)
	}
	intro := e.gql(id, hdr, `{ __type(name: "Item") { name fields { name type { kind ofType { name } } } } __schema { queryType { name } } }`, nil)
	if got, _ := json.Marshal(intro); !strings.Contains(string(got), `{"name":"id","type":{"kind":"NON_NULL","ofType":{"name":"ID"}}}`) ||
		!strings.Contains(string(got), `"queryType":{"name":"Query"}`) {
		t.Fatalf("introspection = %s", got)
	}

	// Errors come back AppSync-style next to the partial data.
	_, out := e.send(withHeaders(e.request(id, `mutation { putItem(input: {id: "js-1"}) { id } } `, nil), hdr))
	if errorType(out) != "DynamoDB:ConditionalCheckFailedException" || out["data"].(M)["putItem"] != nil {
		t.Fatalf("conditional put = %v", out)
	}
	_, out = e.send(withHeaders(e.request(id, `{ hello(name: "boom") ok: hello(name: "ann") }`, nil), hdr))
	if errs := out["errors"].([]any); errorType(out) != "Lambda:Unhandled" || !strings.Contains(errs[0].(M)["message"].(string), "no booms") ||
		errs[0].(M)["path"].([]any)[0] != "hello" || out["data"].(M)["ok"] != "hello ann via Query.hello" {
		t.Fatalf("lambda error = %v", out)
	}
	_, out = e.send(withHeaders(e.request(id, `{ nope }`, nil), hdr))
	if errs, _ := out["errors"].([]any); len(errs) != 1 || !strings.Contains(errs[0].(M)["message"].(string), "nope") || out["data"] != nil {
		t.Fatalf("validation = %v", out)
	}
}

// TestDynamoDBOperations sends each AppSync DynamoDB request through a pass-through VTL resolver.
func TestDynamoDBOperations(t *testing.T) {
	e := setup(t)
	s, hash, rng := ddbtypes.ScalarAttributeTypeS, ddbtypes.KeyTypeHash, ddbtypes.KeyTypeRange
	must(dynamodb.NewFromConfig(e.cfg).CreateTable(ctx, &dynamodb.CreateTableInput{TableName: aws.String("orders"),
		AttributeDefinitions: []ddbtypes.AttributeDefinition{{AttributeName: aws.String("pk"), AttributeType: s}, {AttributeName: aws.String("sk"), AttributeType: s}},
		KeySchema:            []ddbtypes.KeySchemaElement{{AttributeName: aws.String("pk"), KeyType: hash}, {AttributeName: aws.String("sk"), KeyType: rng}},
		BillingMode:          ddbtypes.BillingModePayPerRequest}))
	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("orders"), AuthenticationType: types.AuthenticationTypeApiKey}, "type Query { op(req: AWSJSON!): AWSJSON }")
	hdr := map[string]string{"x-api-key": e.key(id)}
	e.source(id, "orders", types.DataSourceTypeAmazonDynamodb, func(in *appsync.CreateDataSourceInput) {
		in.DynamodbConfig = &types.DynamodbDataSourceConfig{TableName: aws.String("orders"), AwsRegion: aws.String("us-east-1")}
	})
	e.resolve(id, "Query", "op", "orders", "", "$ctx.args.req", outVTL)
	op := func(req string) string {
		return e.gql(id, hdr, `query($r: AWSJSON!) { op(req: $r) }`, M{"r": req})["op"].(string)
	}
	key := func(sk string) string { return `{"pk": {"S": "u1"}, "sk": {"S": "` + sk + `"}}` }
	query := `{"operation": "Query", "limit": 2, "query": {"expression": "pk = :pk AND begins_with(sk, :o)", "expressionValues": {":pk": {"S": "u1"}, ":o": {"S": "o"}}}`
	for _, c := range []struct{ req, want string }{
		{`{"operation": "PutItem", "key": ` + key("o1") + `, "attributeValues": {"n": {"N": "1"}}}`, `{"n":1,"pk":"u1","sk":"o1"}`},
		{`{"operation": "UpdateItem", "key": ` + key("o1") + `, "update": {"expression": "SET #n = #n + :one", "expressionNames": {"#n": "n"},
			"expressionValues": {":one": {"N": "2"}}}}`, `{"n":3,"pk":"u1","sk":"o1"}`},
		{`{"operation": "BatchPutItem", "tables": {"orders": [` + key("o2") + `, ` + key("o3") + `]}}`,
			`{"data":{"orders":[{"pk":"u1","sk":"o2"},{"pk":"u1","sk":"o3"}]},"unprocessedItems":{"orders":[]}}`},
		{`{"operation": "BatchGetItem", "tables": {"orders": {"keys": [` + key("o3") + `, ` + key("nope") + `]}}}`,
			`{"data":{"orders":[{"pk":"u1","sk":"o3"},null]},"unprocessedKeys":{"orders":[]}}`},
	} {
		if got := op(c.req); got != c.want {
			t.Fatalf("%s\n got %s\nwant %s", c.req, got, c.want)
		}
	}
	var page struct {
		Items     []M
		NextToken string
	}
	json.Unmarshal([]byte(op(query+"}")), &page)
	if len(page.Items) != 2 || page.Items[1]["sk"] != "o2" || page.NextToken == "" {
		t.Fatalf("query page 1 = %+v", page)
	}
	if got := op(query + `, "nextToken": "` + page.NextToken + `"}`); got != `{"items":[{"pk":"u1","sk":"o3"}],"nextToken":null,"scannedCount":1}` {
		t.Fatalf("query page 2 = %s", got)
	}
	for _, c := range []struct{ req, want string }{
		{`{"operation": "TransactWriteItems", "transactItems": [{"table": "orders", "operation": "PutItem", "key": ` + key("o4") + `},
			{"table": "orders", "operation": "DeleteItem", "key": ` + key("o2") + `}]}`, `{"cancellationReasons":null,"keys":[{"pk":"u1","sk":"o4"},{"pk":"u1","sk":"o2"}]}`},
		{`{"operation": "BatchDeleteItem", "tables": {"orders": [` + key("o3") + `]}}`, `{"data":{"orders":[{"pk":"u1","sk":"o3"}]},"unprocessedKeys":{"orders":[]}}`},
		{`{"operation": "DeleteItem", "key": ` + key("o1") + `}`, `{"n":3,"pk":"u1","sk":"o1"}`},
		{`{"operation": "Scan"}`, `{"items":[{"pk":"u1","sk":"o4"}],"nextToken":null,"scannedCount":1}`},
	} {
		if got := op(c.req); got != c.want {
			t.Fatalf("%s\n got %s\nwant %s", c.req, got, c.want)
		}
	}
}

// TestHTTPAndRDSSources calls an HTTP endpoint, and reaches the RDS Data API
// (which, with no cluster, answers an error the resolver passes on).
func TestHTTPAndRDSSources(t *testing.T) {
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(M{"path": r.URL.Path, "name": r.URL.Query().Get("name"), "h": r.Header.Get("x-test")})
	}))
	defer web.Close()
	_, url := core.ServeTest(t,
		func(h *core.Hub) core.Service { return appsyncsvc.New(h) },
		func(h *core.Hub) core.Service { return rdsdatasvc.New(h) })
	e := &env{t: t, url: url, as: appsync.NewFromConfig(awstest.Config(t, url))}
	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("sources"), AuthenticationType: types.AuthenticationTypeApiKey},
		"type Query { web(name: String): AWSJSON db(id: Int): AWSJSON }")
	hdr := map[string]string{"x-api-key": e.key(id)}
	e.source(id, "web", types.DataSourceTypeHttp, func(in *appsync.CreateDataSourceInput) {
		in.HttpConfig = &types.HttpDataSourceConfig{Endpoint: &web.URL}
	})
	e.source(id, "db", types.DataSourceTypeRelationalDatabase, func(in *appsync.CreateDataSourceInput) {
		in.RelationalDatabaseConfig = &types.RelationalDatabaseDataSourceConfig{RelationalDatabaseSourceType: types.RelationalDatabaseSourceTypeRdsHttpEndpoint,
			RdsHttpEndpointConfig: &types.RdsHttpEndpointConfig{DbClusterIdentifier: aws.String("arn:aws:rds:us-east-1:000000000000:cluster:db"),
				AwsSecretStoreArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:db"), DatabaseName: aws.String("app")}}
	})
	e.resolve(id, "Query", "web", "web", "", `{"version": "2018-05-29", "method": "GET", "resourcePath": "/hello",
  "params": {"query": {"name": "$ctx.args.name"}, "headers": {"x-test": "yes"}}}`, `$ctx.result.body`)
	e.resolve(id, "Query", "db", "db", "", `{"version": "2018-05-29", "statements": ["select * from t where id = :ID"], "variableMap": {":ID": $ctx.args.id}}`,
		`#if($ctx.error)$util.error($ctx.error.message, $ctx.error.type)#end$util.toJson($util.rds.toJsonObject($ctx.result))`)
	if got := e.gql(id, hdr, `{ web(name: "ann") }`, nil)["web"]; got != `{"h":"yes","name":"ann","path":"/hello"}` {
		t.Fatalf("http = %v", got)
	}
	if _, out := e.send(withHeaders(e.request(id, `{ db(id: 1) }`, nil), hdr)); errorType(out) != "RDSHttp:BadRequestException" {
		t.Fatalf("rds = %v", out)
	}
}

func withHeaders(req *http.Request, hdr map[string]string) *http.Request {
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	return req
}

// TestPipeline runs a pipeline resolver with a JS function and a VTL function
// around VTL before and after steps that share the stash.
func TestPipeline(t *testing.T) {
	e := setup(t)
	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("pipe"), AuthenticationType: types.AuthenticationTypeApiKey}, itemSchema)
	e.source(id, "none", types.DataSourceTypeNone, nil)
	a := must(e.as.CreateFunction(ctx, &appsync.CreateFunctionInput{ApiId: &id, Name: aws.String("a"), DataSourceName: aws.String("none"), Runtime: jsRuntime,
		Code: aws.String(`export function request(ctx) { ctx.stash.trail.push('a'); return { payload: ctx.prev.result.n + ctx.args.n }; }
export function response(ctx) { return ctx.result; }`)})).FunctionConfiguration.FunctionId
	b := must(e.as.CreateFunction(ctx, &appsync.CreateFunctionInput{ApiId: &id, Name: aws.String("b"), DataSourceName: aws.String("none"),
		FunctionVersion: aws.String("2018-05-29"), RequestMappingTemplate: aws.String(`{"version": "2018-05-29", "payload": $util.toJson($ctx.prev.result)}`),
		ResponseMappingTemplate: aws.String(`$util.qr($ctx.stash.trail.add("b"))$util.toJson($ctx.result * 2)`)})).FunctionConfiguration.FunctionId
	must(e.as.CreateResolver(ctx, &appsync.CreateResolverInput{ApiId: &id, TypeName: aws.String("Query"), FieldName: aws.String("pipeline"),
		Kind: types.ResolverKindPipeline, PipelineConfig: &types.PipelineConfig{Functions: []string{*a, *b}},
		RequestMappingTemplate:  aws.String(`$util.qr($ctx.stash.put("trail", ["before"])){"n": 10}`),
		ResponseMappingTemplate: aws.String(`$util.toJson({"n": $ctx.prev.result, "trail": $ctx.stash.trail})`)}))
	got := e.gql(id, map[string]string{"x-api-key": e.key(id)}, `{ pipeline(n: 1) }`, nil)["pipeline"]
	if got != `{"n":22,"trail":["before","a","b"]}` {
		t.Fatalf("pipeline = %v", got)
	}
}

// realtime dials the API's real-time endpoint with auth headers.
func (e *env) realtime(id string, auth M) *websocket.Conn {
	e.t.Helper()
	hdr := base64.StdEncoding.EncodeToString([]byte(must(json.Marshal(auth))))
	u := "ws://127.0.0.1:" + port(e.url) + "/graphql?header=" + url.QueryEscape(hdr) + "&payload=e30="
	ws, _, err := websocket.Dial(ctx, u, &websocket.DialOptions{Subprotocols: []string{"graphql-ws"}, Host: id + ".appsync-realtime-api.localhost:" + port(e.url)})
	if err != nil {
		e.t.Fatal(err)
	}
	e.t.Cleanup(func() { ws.CloseNow() })
	return ws
}

func send(t *testing.T, ws *websocket.Conn, v M) {
	t.Helper()
	if err := ws.Write(ctx, websocket.MessageText, must(json.Marshal(v))); err != nil {
		t.Fatal(err)
	}
}

// next reads the next message other than a keep-alive.
func next(t *testing.T, ws *websocket.Conn) M {
	t.Helper()
	for {
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, b, err := ws.Read(c)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		var m M
		json.Unmarshal(b, &m)
		if m["type"] != "ka" {
			return m
		}
	}
}

// TestSubscription is the done-when case: a subscription with an argument
// filter receives a matching mutation, shaped to its own selection set.
func TestSubscription(t *testing.T) {
	e := setup(t)
	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("chat"), AuthenticationType: types.AuthenticationTypeApiKey},
		`type Message { room: String! text: String author: String }
type Query { ping: String }
type Mutation { send(room: String!, text: String, author: String): Message }
type Subscription { onSend(room: String): Message @aws_subscribe(mutations: ["send"]) }`)
	key := e.key(id)
	e.source(id, "none", types.DataSourceTypeNone, nil)
	e.resolve(id, "Mutation", "send", "none", "", `{"version": "2018-05-29", "payload": $util.toJson($ctx.args)}`, "$util.toJson($ctx.result)")
	auth := M{"host": id + ".appsync-api.localhost", "x-api-key": key}

	ws := e.realtime(id, auth)
	send(t, ws, M{"type": "connection_init"})
	if m := next(t, ws); m["type"] != "connection_ack" || m["payload"].(M)["connectionTimeoutMs"] != 300000.0 {
		t.Fatalf("ack = %v", m)
	}
	data := `{"query":"subscription Room($r: String) { onSend(room: $r) { room text __typename } }","variables":{"r":"a"}}`
	send(t, ws, M{"id": "s1", "type": "start", "payload": M{"data": data, "extensions": M{"authorization": auth}}})
	if m := next(t, ws); m["type"] != "start_ack" || m["id"] != "s1" {
		t.Fatalf("start = %v", m)
	}
	hdr := map[string]string{"x-api-key": key}
	e.gql(id, hdr, `mutation { send(room: "b", text: "elsewhere", author: "zed") { room text author } }`, nil)
	e.gql(id, hdr, `mutation { send(room: "a", text: "hi", author: "zed") { room text author } }`, nil)
	m := next(t, ws)
	if got, _ := json.Marshal(m["payload"]); m["type"] != "data" || m["id"] != "s1" || string(got) != `{"data":{"onSend":{"__typename":"Message","room":"a","text":"hi"}}}` {
		t.Fatalf("data = %v %s", m, got)
	}
	send(t, ws, M{"id": "s1", "type": "stop"})
	if m := next(t, ws); m["type"] != "complete" || m["id"] != "s1" {
		t.Fatalf("stop = %v", m)
	}
	send(t, ws, M{"id": "s2", "type": "start", "payload": M{"data": data, "extensions": M{"authorization": M{"x-api-key": "da2-wrong"}}}})
	if m := next(t, ws); m["type"] != "error" || !strings.Contains(string(must(json.Marshal(m))), "UnauthorizedException") {
		t.Fatalf("start with a wrong key = %v", m)
	}

	bad := e.realtime(id, M{"x-api-key": "da2-wrong"})
	send(t, bad, M{"type": "connection_init"})
	if m := next(t, bad); m["type"] != "connection_error" {
		t.Fatalf("connection with a wrong key = %v", m)
	}
}

// cognito calls the Cognito API over HTTP, so tokens name this server as their issuer.
func (e *env) cognito(op string, in M) M {
	e.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, e.url+"/", bytes.NewReader(must(json.Marshal(in))))
	req.Header.Set("X-Amz-Target", "AWSCognitoIdentityProviderService."+op)
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	status, out := e.send(req)
	if status != 200 {
		e.t.Fatalf("%s: %d %v", op, status, out)
	}
	return out
}

const whoJS = `export function request(ctx) { return { payload: null }; }
export function response(ctx) {
  const id = ctx.identity || {};
  if (ctx.info.fieldName !== 'me') return 'ok';
  if (id.claims) return 'cognito:' + id.username + ':' + (id.groups || []).join(',');
  if (id.resolverContext) return 'lambda:' + id.resolverContext.user;
  return 'iam:' + id.accountId;
}`

func TestAuth(t *testing.T) {
	e := setup(t)
	authorizer := e.pyFunction("authorizer", `def handler(event, context):
    return {"isAuthorized": event["authorizationToken"] == "letmein", "resolverContext": {"user": "bob"}}
`)
	pool := e.cognito("CreateUserPool", M{"PoolName": "users"})["UserPool"].(M)["Id"].(string)
	client := e.cognito("CreateUserPoolClient", M{"UserPoolId": pool, "ClientName": "web", "ExplicitAuthFlows": []string{"ALLOW_USER_PASSWORD_AUTH"}})["UserPoolClient"].(M)["ClientId"].(string)
	e.cognito("AdminCreateUser", M{"UserPoolId": pool, "Username": "ann", "MessageAction": "SUPPRESS"})
	e.cognito("AdminSetUserPassword", M{"UserPoolId": pool, "Username": "ann", "Password": "Passw0rd!", "Permanent": true})
	token := func() string {
		return e.cognito("InitiateAuth", M{"AuthFlow": "USER_PASSWORD_AUTH", "ClientId": client,
			"AuthParameters": M{"USERNAME": "ann", "PASSWORD": "Passw0rd!"}})["AuthenticationResult"].(M)["IdToken"].(string)
	}

	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("auth"), AuthenticationType: types.AuthenticationTypeApiKey,
		AdditionalAuthenticationProviders: []types.AdditionalAuthenticationProvider{
			{AuthenticationType: types.AuthenticationTypeAwsIam},
			{AuthenticationType: types.AuthenticationTypeAmazonCognitoUserPools, UserPoolConfig: &types.CognitoUserPoolConfig{UserPoolId: &pool, AwsRegion: aws.String("us-east-1")}},
			{AuthenticationType: types.AuthenticationTypeAwsLambda, LambdaAuthorizerConfig: &types.LambdaAuthorizerConfig{AuthorizerUri: &authorizer}},
		}}, `type Query {
  open: String @aws_api_key @aws_iam @aws_cognito_user_pools @aws_lambda
  me: String @aws_cognito_user_pools @aws_iam @aws_lambda
  admins: String @aws_cognito_user_pools(cognito_groups: ["admins"])
}`)
	e.source(id, "none", types.DataSourceTypeNone, nil)
	for _, f := range []string{"open", "me", "admins"} {
		e.resolve(id, "Query", f, "none", whoJS, "", "")
	}
	key := e.key(id)

	// API key: the path form of the endpoint works too.
	body, _ := json.Marshal(M{"query": "{ open me }"})
	req, _ := http.NewRequest(http.MethodPost, e.url+"/appsync-api/"+id+"/graphql", bytes.NewReader(body))
	req.Header.Set("x-api-key", key)
	if _, out := e.send(req); out["data"].(M)["open"] != "ok" || out["data"].(M)["me"] != nil || errorType(out) != "Unauthorized" {
		t.Fatalf("api key = %v", out)
	}
	pre, _ := http.NewRequest(http.MethodOptions, e.url+"/appsync-api/"+id+"/graphql", nil)
	pre.Header.Set("Origin", "https://app.example")
	pre.Header.Set("Access-Control-Request-Method", "POST")
	pre.Header.Set("Access-Control-Request-Headers", "x-api-key,content-type")
	if resp := must(http.DefaultClient.Do(pre)); resp.StatusCode != 200 || resp.Header.Get("Access-Control-Allow-Origin") != "*" ||
		resp.Header.Get("Access-Control-Allow-Headers") != "x-api-key,content-type" {
		t.Fatalf("preflight = %d %v", resp.StatusCode, resp.Header)
	}
	for _, hdr := range []map[string]string{{"x-api-key": "da2-wrong"}, {}, {"Authorization": "nope"}} {
		if status, out := e.send(withHeaders(e.request(id, "{ open }", nil), hdr)); status != 401 || errorType(out) != "UnauthorizedException" {
			t.Fatalf("%v = %d %v", hdr, status, out)
		}
	}

	// Cognito: claims and groups reach $ctx.identity; cognito_groups narrow a field.
	_, out := e.send(withHeaders(e.request(id, "{ me admins }", nil), map[string]string{"Authorization": token()}))
	if out["data"].(M)["me"] != "cognito:ann:" || out["data"].(M)["admins"] != nil || errorType(out) != "Unauthorized" {
		t.Fatalf("cognito = %v", out)
	}
	e.cognito("CreateGroup", M{"UserPoolId": pool, "GroupName": "admins"})
	e.cognito("AdminAddUserToGroup", M{"UserPoolId": pool, "Username": "ann", "GroupName": "admins"})
	if got := e.gql(id, map[string]string{"Authorization": token()}, "{ me admins }", nil); got["me"] != "cognito:ann:admins" || got["admins"] != "ok" {
		t.Fatalf("cognito admin = %v", got)
	}

	// IAM: a SigV4-signed request.
	req = e.request(id, "{ me }", nil)
	awstest.Sign(t, req, "appsync")
	if _, out := e.send(req); out["data"].(M)["me"] != "iam:000000000000" {
		t.Fatalf("iam = %v", out)
	}

	// Lambda authorizer.
	if got := e.gql(id, map[string]string{"Authorization": "letmein"}, "{ me }", nil); got["me"] != "lambda:bob" {
		t.Fatalf("lambda = %v", got)
	}

	// OIDC: the pool's issuer, verified through its discovery document.
	oidc := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("oidc"), AuthenticationType: types.AuthenticationTypeOpenidConnect,
		OpenIDConnectConfig: &types.OpenIDConnectConfig{Issuer: aws.String(e.url + "/" + pool), ClientId: &client}}, "type Query { me: String }")
	e.source(oidc, "none", types.DataSourceTypeNone, nil)
	e.resolve(oidc, "Query", "me", "none", "export function request(ctx) { return {} }\nexport function response(ctx) { return ctx.identity.claims['cognito:username'] }", "", "")
	if got := e.gql(oidc, map[string]string{"Authorization": "Bearer " + token()}, "{ me }", nil); got["me"] != "ann" {
		t.Fatalf("oidc = %v", got)
	}
}

// TestPersistence: a saved API comes back with a working schema after Reset and Load.
func TestPersistence(t *testing.T) {
	var svc *appsyncsvc.Service
	_, url := core.ServeTest(t, func(h *core.Hub) core.Service { svc = appsyncsvc.New(h); return svc })
	e := &env{t: t, url: url, as: appsync.NewFromConfig(awstest.Config(t, url))}
	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("saved"), AuthenticationType: types.AuthenticationTypeApiKey}, "type Query { ping: String }")
	hdr := map[string]string{"x-api-key": e.key(id)}
	e.source(id, "none", types.DataSourceTypeNone, nil)
	e.resolve(id, "Query", "ping", "none", "", `{"version": "2018-05-29", "payload": "pong"}`, "$util.toJson($ctx.result)")
	state := must(svc.Save())
	svc.Reset()
	var nf *types.NotFoundException
	if _, err := e.as.GetGraphqlApi(ctx, &appsync.GetGraphqlApiInput{ApiId: &id}); !errors.As(err, &nf) {
		t.Fatalf("after reset = %v", err)
	}
	if err := svc.Load(state); err != nil {
		t.Fatal(err)
	}
	if got := e.gql(id, hdr, "{ ping }", nil)["ping"]; got != "pong" || len(svc.Resources()[0].Rows) != 1 || svc.Inspect("GraphQL APIs", id) == nil {
		t.Fatalf("after load: ping = %v, resources = %v", got, svc.Resources())
	}
}

// TestSignature: with signature checking on, the GraphQL endpoint rejects a bad SigV4 signature.
func TestSignature(t *testing.T) {
	h := core.NewHub(core.Config{Version: "test", DataDir: t.TempDir(), ValidateSigs: true}, nil)
	h.Register(appsyncsvc.New(h))
	h.URL = core.Listen(t, h)
	t.Cleanup(h.Close)
	e := &env{t: t, url: h.URL, as: appsync.NewFromConfig(awstest.Config(t, h.URL))}
	id := e.api(&appsync.CreateGraphqlApiInput{Name: aws.String("iam"), AuthenticationType: types.AuthenticationTypeAwsIam}, "type Query { ping: String }")
	req := e.request(id, "{ ping }", nil)
	awstest.Sign(t, req, "appsync")
	if status, out := e.send(req); status != 200 || out["errors"] != nil {
		t.Fatalf("signed = %d %v", status, out)
	}
	req = e.request(id, "{ ping }", nil)
	awstest.Sign(t, req, "appsync")
	req.Header.Set("Authorization", strings.Replace(req.Header.Get("Authorization"), "Signature=", "Signature=0", 1))
	if status, out := e.send(req); status != 403 || errorType(out) != "InvalidSignatureException" {
		t.Fatalf("bad signature = %d %v", status, out)
	}
}
