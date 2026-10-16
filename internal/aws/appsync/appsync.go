// Package appsync emulates AWS AppSync GraphQL APIs: the REST-JSON control
// plane (APIs, API keys, schemas, data sources, functions, resolvers), the
// GraphQL endpoint on <apiId>.appsync-api.localhost that runs VTL and
// APPSYNC_JS resolvers against Lambda, DynamoDB, HTTP, RDS and NONE data
// sources, and real-time subscriptions on <apiId>.appsync-realtime-api.localhost.
package appsync

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

// Service is the AppSync emulator.
type Service struct {
	hub   *core.Hub
	mux   *http.ServeMux
	mu    sync.Mutex
	st    state
	subMu sync.Mutex
	conns map[*conn]bool // open real-time connections and their subscriptions; under subMu
}

type state struct {
	APIs map[string]*API `json:"apis"` // by apiId
}

// API is a GraphQL API and everything attached to it. Its nested values are
// replaced, never changed in place, so a request can run on a snapshot.
type API struct {
	ID        string                 `json:"id"`
	Account   string                 `json:"account"`
	Region    string                 `json:"region"`
	Config    apiConfig              `json:"config"`
	Tags      map[string]string      `json:"tags,omitempty"`
	SDL       string                 `json:"sdl,omitempty"`
	Status    string                 `json:"status,omitempty"` // schema creation: SUCCESS or FAILED
	Details   string                 `json:"details,omitempty"`
	Keys      map[string]*APIKey     `json:"keys"`
	Sources   map[string]*DataSource `json:"sources"`
	Functions map[string]*Function   `json:"functions"`
	Resolvers map[string]*Resolver   `json:"resolvers"` // by Type.field
	parsed    *ast.Schema
}

// apiConfig holds the GraphqlApi fields a client sets.
// ponytail: logConfig, queryDepthLimit and resolverCountLimit are kept, not
// applied; resolvers' cachingConfig and syncConfig likewise.
type apiConfig struct {
	Name                              string          `json:"name"`
	AuthenticationType                string          `json:"authenticationType"`
	AdditionalAuthenticationProviders []provider      `json:"additionalAuthenticationProviders,omitempty"`
	UserPoolConfig                    *userPool       `json:"userPoolConfig,omitempty"`
	OpenIDConnectConfig               *oidcConfig     `json:"openIDConnectConfig,omitempty"`
	LambdaAuthorizerConfig            *lambdaAuth     `json:"lambdaAuthorizerConfig,omitempty"`
	LogConfig                         json.RawMessage `json:"logConfig,omitempty"`
	XrayEnabled                       bool            `json:"xrayEnabled"`
	Visibility                        string          `json:"visibility,omitempty"`
	ApiType                           string          `json:"apiType,omitempty"`
	MergedApiExecutionRoleArn         string          `json:"mergedApiExecutionRoleArn,omitempty"`
	OwnerContact                      string          `json:"ownerContact,omitempty"`
	IntrospectionConfig               string          `json:"introspectionConfig,omitempty"`
	QueryDepthLimit                   int             `json:"queryDepthLimit,omitempty"`
	ResolverCountLimit                int             `json:"resolverCountLimit,omitempty"`
	EnhancedMetricsConfig             json.RawMessage `json:"enhancedMetricsConfig,omitempty"`
}

// provider is an authorization mode: the default one or an additional one.
type provider struct {
	AuthenticationType     string      `json:"authenticationType"`
	OpenIDConnectConfig    *oidcConfig `json:"openIDConnectConfig,omitempty"`
	UserPoolConfig         *userPool   `json:"userPoolConfig,omitempty"`
	LambdaAuthorizerConfig *lambdaAuth `json:"lambdaAuthorizerConfig,omitempty"`
}

type userPool struct {
	UserPoolId       string `json:"userPoolId"`
	AwsRegion        string `json:"awsRegion,omitempty"`
	DefaultAction    string `json:"defaultAction,omitempty"` // ALLOW or DENY, default mode only
	AppIdClientRegex string `json:"appIdClientRegex,omitempty"`
}

type oidcConfig struct {
	Issuer   string `json:"issuer"`
	ClientId string `json:"clientId,omitempty"`
	IatTTL   int64  `json:"iatTTL,omitempty"`
	AuthTTL  int64  `json:"authTTL,omitempty"`
}

type lambdaAuth struct {
	AuthorizerResultTtlInSeconds int    `json:"authorizerResultTtlInSeconds"`
	AuthorizerUri                string `json:"authorizerUri"`
	IdentityValidationExpression string `json:"identityValidationExpression,omitempty"`
}

// APIKey is an API key; its id is the key.
type APIKey struct {
	ID          string `json:"id"`
	Description string `json:"description,omitempty"`
	Expires     int64  `json:"expires"`
	Deletes     int64  `json:"deletes"`
}

// DataSource is a data source; its configs are kept as sent.
type DataSource struct {
	DataSourceArn            string          `json:"dataSourceArn"`
	Name                     string          `json:"name"`
	Description              string          `json:"description,omitempty"`
	Type                     string          `json:"type"`
	ServiceRoleArn           string          `json:"serviceRoleArn,omitempty"`
	DynamodbConfig           json.RawMessage `json:"dynamodbConfig,omitempty"`
	LambdaConfig             json.RawMessage `json:"lambdaConfig,omitempty"`
	ElasticsearchConfig      json.RawMessage `json:"elasticsearchConfig,omitempty"`
	OpenSearchServiceConfig  json.RawMessage `json:"openSearchServiceConfig,omitempty"`
	HttpConfig               json.RawMessage `json:"httpConfig,omitempty"`
	RelationalDatabaseConfig json.RawMessage `json:"relationalDatabaseConfig,omitempty"`
	EventBridgeConfig        json.RawMessage `json:"eventBridgeConfig,omitempty"`
	MetricsConfig            string          `json:"metricsConfig,omitempty"`
}

// Resolver attaches a data source (UNIT) or functions (PIPELINE) to a field.
type Resolver struct {
	TypeName                string          `json:"typeName"`
	FieldName               string          `json:"fieldName"`
	DataSourceName          string          `json:"dataSourceName,omitempty"`
	ResolverArn             string          `json:"resolverArn"`
	RequestMappingTemplate  string          `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string          `json:"responseMappingTemplate,omitempty"`
	Kind                    string          `json:"kind"`
	PipelineConfig          *pipeline       `json:"pipelineConfig,omitempty"`
	SyncConfig              json.RawMessage `json:"syncConfig,omitempty"`
	CachingConfig           json.RawMessage `json:"cachingConfig,omitempty"`
	MaxBatchSize            int             `json:"maxBatchSize,omitempty"`
	Runtime                 *runtime        `json:"runtime,omitempty"`
	Code                    string          `json:"code,omitempty"`
	MetricsConfig           string          `json:"metricsConfig,omitempty"`
}

type pipeline struct {
	Functions []string `json:"functions"`
}

type runtime struct {
	Name           string `json:"name"`
	RuntimeVersion string `json:"runtimeVersion"`
}

// Function is a pipeline function.
type Function struct {
	FunctionId              string          `json:"functionId"`
	FunctionArn             string          `json:"functionArn"`
	Name                    string          `json:"name"`
	Description             string          `json:"description,omitempty"`
	DataSourceName          string          `json:"dataSourceName"`
	RequestMappingTemplate  string          `json:"requestMappingTemplate,omitempty"`
	ResponseMappingTemplate string          `json:"responseMappingTemplate,omitempty"`
	FunctionVersion         string          `json:"functionVersion,omitempty"`
	SyncConfig              json.RawMessage `json:"syncConfig,omitempty"`
	MaxBatchSize            int             `json:"maxBatchSize,omitempty"`
	Runtime                 *runtime        `json:"runtime,omitempty"`
	Code                    string          `json:"code,omitempty"`
}

func (r *runtime) js() bool { return r != nil && r.Name == "APPSYNC_JS" }

// New creates the AppSync service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), st: newState(), conns: map[*conn]bool{}}
	s.routes()
	return s
}

func newState() state { return state{APIs: map[string]*API{}} }

func (s *Service) Info() core.Info {
	return core.Info{Name: "appsync", Cloud: core.AWS, Title: "AppSync", Signing: []string{"appsync"}, Protocol: "rest-json", Match: dataPlane}
}

// dataPlane recognises GraphQL and real-time traffic (see target).
func dataPlane(r *http.Request) bool {
	_, _, ok := target(r)
	return ok
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if dataPlane(r) {
		s.serveGraphQL(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// Close ends the real-time connections.
func (s *Service) Close() error {
	s.subMu.Lock()
	defer s.subMu.Unlock()
	for c := range s.conns {
		c.ws.CloseNow()
	}
	return nil
}

type handler func(r *http.Request, c *core.Ctx) (any, error)

// raw is an output written as the HTTP payload (GetIntrospectionSchema).
type raw []byte

// route serves one operation; handlers run under the service lock unless
// free, and outputs are encoded under it too, since they alias the state.
func (s *Service) route(pattern, op string, h handler, free ...bool) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		if err := c.Intercept(); err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		core.Body(r) // read the body before taking the lock
		if len(free) == 0 {
			s.mu.Lock()
		}
		out, err := h(r, c)
		body, isRaw := out.(raw)
		if !isRaw && err == nil {
			if out == nil {
				out = struct{}{}
			}
			body, err = json.Marshal(out)
		}
		if len(free) == 0 {
			s.mu.Unlock()
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("x-amzn-RequestId", c.RequestID)
		w.Write(body)
	})
}

func (s *Service) routes() {
	const api = "/v1/apis/{apiId}"
	s.route("POST /v1/apis", "CreateGraphqlApi", s.createAPI)
	s.route("GET /v1/apis", "ListGraphqlApis", s.listAPIs)
	s.route("GET "+api, "GetGraphqlApi", s.getAPI)
	s.route("POST "+api, "UpdateGraphqlApi", s.updateAPI)
	s.route("DELETE "+api, "DeleteGraphqlApi", s.deleteAPI)
	s.route("POST "+api+"/apikeys", "CreateApiKey", s.createKey)
	s.route("GET "+api+"/apikeys", "ListApiKeys", s.listKeys)
	s.route("POST "+api+"/apikeys/{id}", "UpdateApiKey", s.updateKey)
	s.route("DELETE "+api+"/apikeys/{id}", "DeleteApiKey", s.deleteKey)
	s.route("POST "+api+"/schemacreation", "StartSchemaCreation", s.startSchema)
	s.route("GET "+api+"/schemacreation", "GetSchemaCreationStatus", s.schemaStatus)
	s.route("GET "+api+"/schema", "GetIntrospectionSchema", s.introspectionSchema)
	s.route("GET "+api+"/types", "ListTypes", s.listTypes)
	s.route("GET "+api+"/types/{typeName}", "GetType", s.getType)
	s.route("POST "+api+"/datasources", "CreateDataSource", s.putSource(true))
	s.route("POST "+api+"/datasources/{name}", "UpdateDataSource", s.putSource(false))
	s.route("GET "+api+"/datasources", "ListDataSources", s.listSources)
	s.route("GET "+api+"/datasources/{name}", "GetDataSource", s.getSource)
	s.route("DELETE "+api+"/datasources/{name}", "DeleteDataSource", s.deleteSource)
	s.route("POST "+api+"/functions", "CreateFunction", s.putFunction(true))
	s.route("POST "+api+"/functions/{functionId}", "UpdateFunction", s.putFunction(false))
	s.route("GET "+api+"/functions", "ListFunctions", s.listFunctions)
	s.route("GET "+api+"/functions/{functionId}", "GetFunction", s.getFunction)
	s.route("DELETE "+api+"/functions/{functionId}", "DeleteFunction", s.deleteFunction)
	s.route("GET "+api+"/functions/{functionId}/resolvers", "ListResolversByFunction", s.listResolvers)
	s.route("POST "+api+"/types/{typeName}/resolvers", "CreateResolver", s.putResolver(true))
	s.route("POST "+api+"/types/{typeName}/resolvers/{fieldName}", "UpdateResolver", s.putResolver(false))
	s.route("GET "+api+"/types/{typeName}/resolvers", "ListResolvers", s.listResolvers)
	s.route("GET "+api+"/types/{typeName}/resolvers/{fieldName}", "GetResolver", s.getResolver)
	s.route("DELETE "+api+"/types/{typeName}/resolvers/{fieldName}", "DeleteResolver", s.deleteResolver)
	s.route("GET /v1/tags/{arn...}", "ListTagsForResource", s.listTags)
	s.route("POST /v1/tags/{arn...}", "TagResource", s.tagResource)
	s.route("DELETE /v1/tags/{arn...}", "UntagResource", s.untagResource)
	s.route("POST /v1/dataplane-evaluatetemplate", "EvaluateMappingTemplate", s.evaluateTemplate, true)
	s.route("POST /v1/dataplane-evaluatecode", "EvaluateCode", s.evaluateCode, true)
	s.route("/", "Unknown", func(r *http.Request, c *core.Ctx) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement AppSync %s %s", r.Method, r.URL.Path)
	})
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 {
		if err := json.Unmarshal(b, v); err != nil {
			return badRequest("Invalid request body: %v", err)
		}
	}
	return nil
}

func badRequest(format string, a ...any) error {
	return core.Errorf(400, "BadRequestException", format, a...)
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "NotFoundException", format, a...)
}

// newID is an AppSync-style id: 26 lowercase letters and digits.
func newID() string { return strings.ToLower(core.RandString(26, core.UpperNum)) }

// api finds the API of the request's apiId in the caller's account and region; lock held.
func (s *Service) api(r *http.Request, c *core.Ctx) (*API, error) {
	id := r.PathValue("apiId")
	if a := s.st.APIs[id]; a != nil && a.Account == c.Account && a.Region == c.Region {
		return a, nil
	}
	return nil, notFound("GraphQL API %s not found.", id)
}

func (a *API) arn(resource string) string {
	return core.ARN("appsync", a.Region, a.Account, "apis/"+a.ID+resource)
}

// schema returns the parsed schema, parsing it after a restart; lock held.
func (a *API) schema() *ast.Schema {
	if a.parsed == nil && a.SDL != "" {
		a.parsed, _ = loadSchema(a.SDL)
	}
	return a.parsed
}

// page cuts sorted keys at nextToken (the last key returned) to maxResults.
func page(r *http.Request, keys []string) ([]string, string) {
	q := r.URL.Query()
	if t := q.Get("nextToken"); t != "" {
		keys = keys[after(keys, t):]
	}
	n, _ := strconv.Atoi(q.Get("maxResults"))
	if n <= 0 || n > 25 {
		n = 25
	}
	if len(keys) > n {
		return keys[:n], keys[n-1]
	}
	return keys, ""
}

// after returns the index of the first key after t in sorted keys.
func after(keys []string, t string) int {
	i, found := slices.BinarySearch(keys, t)
	if found {
		i++
	}
	return i
}

func listOut(key string, items []any, next string) map[string]any {
	out := map[string]any{key: items}
	if next != "" {
		out["nextToken"] = next
	}
	return out
}

// ---- GraphQL APIs ----

var authTypes = []string{"API_KEY", "AWS_IAM", "AMAZON_COGNITO_USER_POOLS", "OPENID_CONNECT", "AWS_LAMBDA"}

func (cfg *apiConfig) providers() []provider {
	return append([]provider{{cfg.AuthenticationType, cfg.OpenIDConnectConfig, cfg.UserPoolConfig, cfg.LambdaAuthorizerConfig}},
		cfg.AdditionalAuthenticationProviders...)
}

func (cfg *apiConfig) check() error {
	if cfg.Name == "" {
		return badRequest("name is required.")
	}
	for _, p := range cfg.providers() {
		switch {
		case !slices.Contains(authTypes, p.AuthenticationType):
			return badRequest("Invalid authenticationType: %q.", p.AuthenticationType)
		case p.AuthenticationType == "AMAZON_COGNITO_USER_POOLS" && p.UserPoolConfig == nil:
			return badRequest("userPoolConfig is required for AMAZON_COGNITO_USER_POOLS.")
		case p.AuthenticationType == "OPENID_CONNECT" && p.OpenIDConnectConfig == nil:
			return badRequest("openIDConnectConfig is required for OPENID_CONNECT.")
		case p.AuthenticationType == "AWS_LAMBDA" && p.LambdaAuthorizerConfig == nil:
			return badRequest("lambdaAuthorizerConfig is required for AWS_LAMBDA.")
		}
	}
	cfg.Visibility = cmp.Or(cfg.Visibility, "GLOBAL")
	cfg.ApiType = cmp.Or(cfg.ApiType, "GRAPHQL")
	cfg.IntrospectionConfig = cmp.Or(cfg.IntrospectionConfig, "ENABLED")
	return nil
}

type apiView struct {
	apiConfig
	ApiId string            `json:"apiId"`
	Arn   string            `json:"arn"`
	Uris  map[string]string `json:"uris"`
	Tags  map[string]string `json:"tags,omitempty"`
	Owner string            `json:"owner"`
}

func (a *API) view(host string) apiView {
	return apiView{apiConfig: a.Config, ApiId: a.ID, Arn: a.arn(""), Uris: uris(host, a.ID), Tags: a.Tags, Owner: a.Account}
}

// uris are the endpoints of an API for a client that reached pogi at host.
func uris(host, id string) map[string]string {
	port := ""
	if i := strings.LastIndex(host, ":"); i >= 0 && !strings.Contains(host[i:], "]") {
		port = host[i:]
	}
	return map[string]string{"GRAPHQL": "http://" + id + ".appsync-api.localhost" + port + "/graphql",
		"REALTIME": "ws://" + id + ".appsync-realtime-api.localhost" + port + "/graphql"}
}

func (s *Service) createAPI(r *http.Request, c *core.Ctx) (any, error) {
	var in struct {
		apiConfig
		Tags map[string]string `json:"tags"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	if err := in.check(); err != nil {
		return nil, err
	}
	a := &API{ID: newID(), Account: c.Account, Region: c.Region, Config: in.apiConfig, Tags: in.Tags, Keys: map[string]*APIKey{},
		Sources: map[string]*DataSource{}, Functions: map[string]*Function{}, Resolvers: map[string]*Resolver{}}
	s.st.APIs[a.ID] = a
	return map[string]any{"graphqlApi": a.view(c.Host)}, nil
}

func (s *Service) getAPI(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	return map[string]any{"graphqlApi": a.view(c.Host)}, nil
}

func (s *Service) listAPIs(r *http.Request, c *core.Ctx) (any, error) {
	var ids []string
	for id, a := range s.st.APIs {
		if a.Account == c.Account && a.Region == c.Region {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	ids, next := page(r, ids)
	out := []any{}
	for _, id := range ids {
		out = append(out, s.st.APIs[id].view(c.Host))
	}
	return listOut("graphqlApis", out, next), nil
}

// updateAPI replaces the settable fields; the type and visibility stay.
func (s *Service) updateAPI(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	var in apiConfig
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	in.ApiType, in.Visibility = a.Config.ApiType, a.Config.Visibility
	if err := in.check(); err != nil {
		return nil, err
	}
	a.Config = in
	return map[string]any{"graphqlApi": a.view(c.Host)}, nil
}

func (s *Service) deleteAPI(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	delete(s.st.APIs, a.ID)
	return nil, nil
}

// ---- API keys ----

// expiry checks an expiry (default: 7 days) against AppSync's 1 to 365 day
// window and rounds it down to the hour, as AppSync does.
func expiry(e int64) (int64, error) {
	now := time.Now().Unix()
	switch {
	case e == 0:
		e = now + 7*86400
	case e < now+86400-3600: // an hour of grace: clients round down to the hour too
		return 0, core.Errorf(400, "ApiKeyValidityOutOfBoundsException", "API key must be valid for a minimum of 1 day.")
	case e > now+365*86400:
		return 0, core.Errorf(400, "ApiKeyValidityOutOfBoundsException", "API key must be valid for a maximum of 365 days.")
	}
	return e / 3600 * 3600, nil
}

func (s *Service) createKey(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	var in struct {
		Description string `json:"description"`
		Expires     int64  `json:"expires"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	exp, err := expiry(in.Expires)
	if err != nil {
		return nil, err
	}
	k := &APIKey{ID: "da2-" + newID(), Description: in.Description, Expires: exp, Deletes: exp + 60*86400}
	a.Keys[k.ID] = k
	return map[string]any{"apiKey": k}, nil
}

func (s *Service) listKeys(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	now := time.Now().Unix()
	var ids []string
	for id, k := range a.Keys {
		if k.Deletes > now {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	ids, next := page(r, ids)
	out := []any{}
	for _, id := range ids {
		out = append(out, a.Keys[id])
	}
	return listOut("apiKeys", out, next), nil
}

func (s *Service) updateKey(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	old := a.Keys[r.PathValue("id")]
	if old == nil {
		return nil, notFound("API key %s not found.", r.PathValue("id"))
	}
	var in struct {
		Description *string `json:"description"`
		Expires     int64   `json:"expires"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	k := *old
	if in.Description != nil {
		k.Description = *in.Description
	}
	if in.Expires != 0 {
		if k.Expires, err = expiry(in.Expires); err != nil {
			return nil, err
		}
		k.Deletes = k.Expires + 60*86400
	}
	a.Keys[k.ID] = &k
	return map[string]any{"apiKey": &k}, nil
}

func (s *Service) deleteKey(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	if a.Keys[r.PathValue("id")] == nil {
		return nil, notFound("API key %s not found.", r.PathValue("id"))
	}
	delete(a.Keys, r.PathValue("id"))
	return nil, nil
}

// ---- schema ----

// startSchema parses the schema right away; a failure keeps the previous one,
// as AppSync does, and shows in GetSchemaCreationStatus.
func (s *Service) startSchema(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	var in struct {
		Definition []byte `json:"definition"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	sch, err := loadSchema(string(in.Definition))
	if err != nil {
		a.Status, a.Details = "FAILED", err.Error()
	} else {
		a.SDL, a.parsed, a.Status = string(in.Definition), sch, "SUCCESS"
		a.Details = fmt.Sprintf("Successfully created schema with %d types.", len(userTypes(sch)))
	}
	return map[string]any{"status": "PROCESSING"}, nil
}

func (s *Service) schemaStatus(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	return map[string]any{"status": cmp.Or(a.Status, "NOT_APPLICABLE"), "details": a.Details}, nil
}

// userTypes are the schema's own types, sorted.
func userTypes(sch *ast.Schema) []*ast.Definition {
	var out []*ast.Definition
	for _, name := range core.SortedKeys(sch.Types) {
		if d := sch.Types[name]; !d.BuiltIn {
			out = append(out, d)
		}
	}
	return out
}

func (s *Service) introspectionSchema(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	sch := a.schema()
	if sch == nil {
		return nil, notFound("No schema found for GraphQL API %s.", a.ID)
	}
	if r.URL.Query().Get("format") == "JSON" {
		return raw(toJSON(map[string]any{"data": map[string]any{"__schema": introspect(sch)}})), nil
	}
	var b strings.Builder
	formatter.NewFormatter(&b, formatter.WithIndent("  ")).FormatSchema(sch)
	return raw(b.String()), nil
}

// typeView renders one type as SDL or as its introspection JSON.
func (a *API) typeView(d *ast.Definition, format string) map[string]any {
	def := ""
	if format == "JSON" {
		def = toJSON(introspectType(a.parsed, d))
	} else {
		var b strings.Builder
		formatter.NewFormatter(&b, formatter.WithIndent("  ")).(interface{ FormatDefinition(*ast.Definition, bool) }).FormatDefinition(d, false)
		def = strings.TrimSpace(b.String())
	}
	return map[string]any{"name": d.Name, "description": d.Description, "arn": a.arn("/types/" + d.Name), "definition": def, "format": format}
}

func typeFormat(r *http.Request) (string, error) {
	f := r.URL.Query().Get("format")
	if f != "SDL" && f != "JSON" {
		return "", badRequest("format must be SDL or JSON.")
	}
	return f, nil
}

func (s *Service) listTypes(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	format, err := typeFormat(r)
	if err != nil {
		return nil, err
	}
	out := []any{}
	if sch := a.schema(); sch != nil {
		var names []string
		for _, d := range userTypes(sch) {
			names = append(names, d.Name)
		}
		names, next := page(r, names)
		for _, n := range names {
			out = append(out, a.typeView(sch.Types[n], format))
		}
		return listOut("types", out, next), nil
	}
	return listOut("types", out, ""), nil
}

func (s *Service) getType(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	format, err := typeFormat(r)
	if err != nil {
		return nil, err
	}
	var d *ast.Definition
	if sch := a.schema(); sch != nil {
		d = sch.Types[r.PathValue("typeName")]
	}
	if d == nil || d.BuiltIn {
		return nil, notFound("Type %s not found.", r.PathValue("typeName"))
	}
	return map[string]any{"type": a.typeView(d, format)}, nil
}

// ---- data sources ----

var (
	resourceName = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)
	sourceTypes  = map[string]func(*DataSource) json.RawMessage{
		"AWS_LAMBDA":                func(d *DataSource) json.RawMessage { return d.LambdaConfig },
		"AMAZON_DYNAMODB":           func(d *DataSource) json.RawMessage { return d.DynamodbConfig },
		"HTTP":                      func(d *DataSource) json.RawMessage { return d.HttpConfig },
		"RELATIONAL_DATABASE":       func(d *DataSource) json.RawMessage { return d.RelationalDatabaseConfig },
		"AMAZON_ELASTICSEARCH":      func(d *DataSource) json.RawMessage { return d.ElasticsearchConfig },
		"AMAZON_OPENSEARCH_SERVICE": func(d *DataSource) json.RawMessage { return d.OpenSearchServiceConfig },
		"AMAZON_EVENTBRIDGE":        func(d *DataSource) json.RawMessage { return d.EventBridgeConfig },
		"NONE":                      func(*DataSource) json.RawMessage { return json.RawMessage("{}") },
		"AMAZON_BEDROCK_RUNTIME":    func(*DataSource) json.RawMessage { return json.RawMessage("{}") },
	}
)

func (s *Service) putSource(create bool) handler {
	return func(r *http.Request, c *core.Ctx) (any, error) {
		a, err := s.api(r, c)
		if err != nil {
			return nil, err
		}
		var d DataSource
		if err := decode(r, &d); err != nil {
			return nil, err
		}
		if !create {
			d.Name = r.PathValue("name")
		}
		cfg := sourceTypes[d.Type]
		switch {
		case !resourceName.MatchString(d.Name):
			return nil, badRequest("Invalid data source name %q.", d.Name)
		case cfg == nil:
			return nil, badRequest("Invalid data source type %q.", d.Type)
		case len(cfg(&d)) == 0:
			return nil, badRequest("A data source of type %s needs its configuration.", d.Type)
		case create && a.Sources[d.Name] != nil:
			return nil, badRequest("Data source with name %s already exists.", d.Name)
		case !create && a.Sources[d.Name] == nil:
			return nil, notFound("Data source %s not found.", d.Name)
		}
		d.DataSourceArn = a.arn("/datasources/" + d.Name)
		a.Sources[d.Name] = &d
		return map[string]any{"dataSource": &d}, nil
	}
}

func (s *Service) getSource(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	d := a.Sources[r.PathValue("name")]
	if d == nil {
		return nil, notFound("Data source %s not found.", r.PathValue("name"))
	}
	return map[string]any{"dataSource": d}, nil
}

func (s *Service) listSources(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	names, next := page(r, core.SortedKeys(a.Sources))
	out := []any{}
	for _, n := range names {
		out = append(out, a.Sources[n])
	}
	return listOut("dataSources", out, next), nil
}

func (s *Service) deleteSource(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	if a.Sources[r.PathValue("name")] == nil {
		return nil, notFound("Data source %s not found.", r.PathValue("name"))
	}
	delete(a.Sources, r.PathValue("name"))
	return nil, nil
}

// ---- functions and resolvers ----

// checkCode compiles APPSYNC_JS code, as AppSync does on save.
func checkCode(rt *runtime, code string) error {
	if !rt.js() {
		if rt != nil {
			return badRequest("Unsupported runtime %q.", rt.Name)
		}
		return nil
	}
	if err := compileJS(code); err != nil {
		return badRequest("The code contains one or more errors: %v", err)
	}
	return nil
}

func (s *Service) putFunction(create bool) handler {
	return func(r *http.Request, c *core.Ctx) (any, error) {
		a, err := s.api(r, c)
		if err != nil {
			return nil, err
		}
		var f Function
		if err := decode(r, &f); err != nil {
			return nil, err
		}
		f.FunctionId = r.PathValue("functionId")
		switch {
		case create:
			f.FunctionId = newID()
		case a.Functions[f.FunctionId] == nil:
			return nil, notFound("Function %s not found.", f.FunctionId)
		}
		if !resourceName.MatchString(f.Name) {
			return nil, badRequest("Invalid function name %q.", f.Name)
		}
		if a.Sources[f.DataSourceName] == nil {
			return nil, notFound("Data source %s not found.", f.DataSourceName)
		}
		if err := checkCode(f.Runtime, f.Code); err != nil {
			return nil, err
		}
		if !f.Runtime.js() {
			f.FunctionVersion = cmp.Or(f.FunctionVersion, "2018-05-29")
		}
		f.FunctionArn = a.arn("/functions/" + f.FunctionId)
		a.Functions[f.FunctionId] = &f
		return map[string]any{"functionConfiguration": &f}, nil
	}
}

func (s *Service) getFunction(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	f := a.Functions[r.PathValue("functionId")]
	if f == nil {
		return nil, notFound("Function %s not found.", r.PathValue("functionId"))
	}
	return map[string]any{"functionConfiguration": f}, nil
}

func (s *Service) listFunctions(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	ids, next := page(r, core.SortedKeys(a.Functions))
	out := []any{}
	for _, id := range ids {
		out = append(out, a.Functions[id])
	}
	return listOut("functions", out, next), nil
}

// deleteFunction refuses while a pipeline resolver still runs the function.
func (s *Service) deleteFunction(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	id := r.PathValue("functionId")
	if a.Functions[id] == nil {
		return nil, notFound("Function %s not found.", id)
	}
	for _, k := range core.SortedKeys(a.Resolvers) {
		if p := a.Resolvers[k].PipelineConfig; p != nil && slices.Contains(p.Functions, id) {
			return nil, core.Errorf(409, "ConcurrentModificationException", "Function %s is used by the resolver of %s.", id, k)
		}
	}
	delete(a.Functions, id)
	return nil, nil
}

func (s *Service) putResolver(create bool) handler {
	return func(r *http.Request, c *core.Ctx) (any, error) {
		a, err := s.api(r, c)
		if err != nil {
			return nil, err
		}
		var res Resolver
		if err := decode(r, &res); err != nil {
			return nil, err
		}
		res.TypeName = r.PathValue("typeName")
		if !create {
			res.FieldName = r.PathValue("fieldName")
		}
		k := res.TypeName + "." + res.FieldName
		switch {
		case create && a.Resolvers[k] != nil:
			return nil, badRequest("Only one resolver is allowed per field.")
		case !create && a.Resolvers[k] == nil:
			return nil, notFound("No resolver found.")
		}
		if sch := a.schema(); sch != nil {
			if t := sch.Types[res.TypeName]; t == nil {
				return nil, notFound("Type %s not found.", res.TypeName)
			} else if t.Fields.ForName(res.FieldName) == nil {
				return nil, badRequest("No field named %s found on type %s.", res.FieldName, res.TypeName)
			}
		}
		res.Kind = cmp.Or(res.Kind, "UNIT")
		if res.Kind == "PIPELINE" {
			if res.PipelineConfig == nil {
				res.PipelineConfig = &pipeline{Functions: []string{}}
			}
			for _, id := range res.PipelineConfig.Functions {
				if a.Functions[id] == nil {
					return nil, notFound("Function %s not found.", id)
				}
			}
		} else if a.Sources[res.DataSourceName] == nil {
			return nil, notFound("Data source %s not found.", res.DataSourceName)
		}
		if err := checkCode(res.Runtime, res.Code); err != nil {
			return nil, err
		}
		res.ResolverArn = a.arn("/types/" + res.TypeName + "/resolvers/" + res.FieldName)
		a.Resolvers[k] = &res
		return map[string]any{"resolver": &res}, nil
	}
}

func (s *Service) getResolver(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	res := a.Resolvers[r.PathValue("typeName")+"."+r.PathValue("fieldName")]
	if res == nil {
		return nil, notFound("No resolver found.")
	}
	return map[string]any{"resolver": res}, nil
}

// listResolvers lists the resolvers of a type, or of a function.
func (s *Service) listResolvers(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	fn := r.PathValue("functionId")
	var keys []string
	for k, res := range a.Resolvers {
		if fn == "" && res.TypeName == r.PathValue("typeName") || fn != "" && res.PipelineConfig != nil && slices.Contains(res.PipelineConfig.Functions, fn) {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	keys, next := page(r, keys)
	out := []any{}
	for _, k := range keys {
		out = append(out, a.Resolvers[k])
	}
	return listOut("resolvers", out, next), nil
}

func (s *Service) deleteResolver(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.api(r, c)
	if err != nil {
		return nil, err
	}
	k := r.PathValue("typeName") + "." + r.PathValue("fieldName")
	if a.Resolvers[k] == nil {
		return nil, notFound("No resolver found.")
	}
	delete(a.Resolvers, k)
	return nil, nil
}

// ---- tags ----

func (s *Service) tagged(r *http.Request) (*API, error) {
	arn := r.PathValue("arn")
	for _, a := range s.st.APIs {
		if a.arn("") == arn {
			return a, nil
		}
	}
	return nil, notFound("Resource %s not found.", arn)
}

func (s *Service) listTags(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.tagged(r)
	if err != nil {
		return nil, err
	}
	tags := a.Tags
	if tags == nil {
		tags = map[string]string{}
	}
	return map[string]any{"tags": tags}, nil
}

func (s *Service) tagResource(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.tagged(r)
	if err != nil {
		return nil, err
	}
	var in struct {
		Tags map[string]string `json:"tags"`
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	tags := maps.Clone(a.Tags)
	if tags == nil {
		tags = map[string]string{}
	}
	maps.Copy(tags, in.Tags)
	a.Tags = tags
	return nil, nil
}

func (s *Service) untagResource(r *http.Request, c *core.Ctx) (any, error) {
	a, err := s.tagged(r)
	if err != nil {
		return nil, err
	}
	tags := maps.Clone(a.Tags)
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(tags, k)
	}
	a.Tags = tags
	return nil, nil
}

// ---- console ----

// Resources lists the GraphQL APIs.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "GraphQL APIs", Columns: []string{"Id", "Name", "Auth", "Region", "Account", "Data sources", "Resolvers", "URL"}}
	host := "localhost:7644"
	if s.hub != nil && s.hub.URL != "" {
		host = core.Hostport(s.hub.URL)
	}
	for _, id := range core.SortedKeys(s.st.APIs) {
		a := s.st.APIs[id]
		t.Rows = append(t.Rows, []string{id, a.Config.Name, a.Config.AuthenticationType, a.Region, a.Account,
			strconv.Itoa(len(a.Sources)), strconv.Itoa(len(a.Resolvers)), uris(host, id)["GRAPHQL"]})
	}
	return []core.Table{t}
}

// Inspect returns an API with its schema, keys, data sources, functions and resolvers.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.st.APIs[id]
	if a == nil {
		return nil
	}
	b, _ := json.Marshal(map[string]any{"graphqlApi": a.view("localhost:7644"), "schema": a.SDL, "schemaStatus": a.Status, "apiKeys": a.Keys,
		"dataSources": a.Sources, "functions": a.Functions, "resolvers": a.Resolvers}) // under the lock: it aliases the state
	return json.RawMessage(b)
}
