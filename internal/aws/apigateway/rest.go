package apigateway

import (
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The v1 wire shapes double as state: their JSON is what AWS returns.

// RestApi is a v1 REST API.
type RestApi struct {
	ID                        string                 `json:"id"`
	Name                      string                 `json:"name"`
	Description               string                 `json:"description,omitempty"`
	CreatedDate               core.Time              `json:"createdDate"`
	Version                   string                 `json:"version,omitempty"`
	BinaryMediaTypes          []string               `json:"binaryMediaTypes,omitempty"`
	MinimumCompressionSize    *int                   `json:"minimumCompressionSize,omitempty"`
	ApiKeySource              string                 `json:"apiKeySource,omitempty"`
	EndpointConfiguration     *EndpointConfiguration `json:"endpointConfiguration,omitempty"`
	Policy                    string                 `json:"policy,omitempty"`
	Tags                      map[string]string      `json:"tags,omitempty"`
	DisableExecuteApiEndpoint bool                   `json:"disableExecuteApiEndpoint"`
	RootResourceID            string                 `json:"rootResourceId"`
	ApiStatus                 string                 `json:"apiStatus,omitempty"`
	Warnings                  []string               `json:"warnings,omitempty"` // import answers only, never stored
}

type EndpointConfiguration struct {
	Types          []string `json:"types,omitempty"`
	VpcEndpointIds []string `json:"vpcEndpointIds,omitempty"`
	IpAddressType  string   `json:"ipAddressType,omitempty"`
}

// restAPI is a REST API with everything under it.
type restAPI struct {
	RestApi
	Account     string                 `json:"account"`
	Region      string                 `json:"region"`
	Resources   map[string]*Resource   `json:"resources"`
	Deployments map[string]*Deployment `json:"deployments"`
	Stages      map[string]*Stage      `json:"stages"`
	Authorizers map[string]*Authorizer `json:"authorizers"`
	// stored for templates and clients; requests are not checked against them
	Models       map[string]*Model            `json:"models,omitempty"`           // by name
	Validators   map[string]*RequestValidator `json:"validators,omitempty"`       // by id
	GatewayResps map[string]*GatewayResponse  `json:"gatewayResponses,omitempty"` // by response type
}

// Model is a JSON schema models of requests and responses refer to.
type Model struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
	Schema      string `json:"schema,omitempty"`
	ContentType string `json:"contentType,omitempty"`
}

type RequestValidator struct {
	ID                        string `json:"id"`
	Name                      string `json:"name"`
	ValidateRequestBody       bool   `json:"validateRequestBody"`
	ValidateRequestParameters bool   `json:"validateRequestParameters"`
}

type GatewayResponse struct {
	ResponseType       string            `json:"responseType"`
	StatusCode         string            `json:"statusCode,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	DefaultResponse    bool              `json:"defaultResponse"`
}

type Resource struct {
	ID              string             `json:"id"`
	ParentID        string             `json:"parentId,omitempty"`
	PathPart        string             `json:"pathPart,omitempty"`
	Path            string             `json:"path"`
	ResourceMethods map[string]*Method `json:"resourceMethods,omitempty"`
}

type Method struct {
	HTTPMethod          string                     `json:"httpMethod"`
	AuthorizationType   string                     `json:"authorizationType"`
	AuthorizerID        string                     `json:"authorizerId,omitempty"`
	ApiKeyRequired      bool                       `json:"apiKeyRequired"`
	RequestValidatorID  string                     `json:"requestValidatorId,omitempty"`
	OperationName       string                     `json:"operationName,omitempty"`
	RequestParameters   map[string]bool            `json:"requestParameters,omitempty"`
	RequestModels       map[string]string          `json:"requestModels,omitempty"`
	AuthorizationScopes []string                   `json:"authorizationScopes,omitempty"`
	MethodResponses     map[string]*MethodResponse `json:"methodResponses,omitempty"`
	MethodIntegration   *Integration               `json:"methodIntegration,omitempty"`
}

type MethodResponse struct {
	StatusCode         string            `json:"statusCode"`
	ResponseParameters map[string]bool   `json:"responseParameters,omitempty"`
	ResponseModels     map[string]string `json:"responseModels,omitempty"`
}

// Integration is a method's backend; HTTPMethod is the integration's own method.
type Integration struct {
	Type                 string                          `json:"type"`
	HTTPMethod           string                          `json:"httpMethod,omitempty"`
	URI                  string                          `json:"uri,omitempty"`
	ConnectionType       string                          `json:"connectionType,omitempty"`
	ConnectionID         string                          `json:"connectionId,omitempty"`
	Credentials          string                          `json:"credentials,omitempty"`
	RequestParameters    map[string]string               `json:"requestParameters,omitempty"`
	RequestTemplates     map[string]string               `json:"requestTemplates,omitempty"`
	PassthroughBehavior  string                          `json:"passthroughBehavior,omitempty"`
	ContentHandling      string                          `json:"contentHandling,omitempty"`
	TimeoutInMillis      int                             `json:"timeoutInMillis"`
	CacheNamespace       string                          `json:"cacheNamespace,omitempty"`
	CacheKeyParameters   []string                        `json:"cacheKeyParameters"`
	IntegrationResponses map[string]*IntegrationResponse `json:"integrationResponses,omitempty"`
	TlsConfig            any                             `json:"tlsConfig,omitempty"`
}

type IntegrationResponse struct {
	StatusCode         string            `json:"statusCode"`
	SelectionPattern   string            `json:"selectionPattern,omitempty"`
	ResponseParameters map[string]string `json:"responseParameters,omitempty"`
	ResponseTemplates  map[string]string `json:"responseTemplates,omitempty"`
	ContentHandling    string            `json:"contentHandling,omitempty"`
}

type Deployment struct {
	ID          string    `json:"id"`
	Description string    `json:"description,omitempty"`
	CreatedDate core.Time `json:"createdDate"`
}

type Stage struct {
	DeploymentID         string            `json:"deploymentId,omitempty"`
	StageName            string            `json:"stageName"`
	Description          string            `json:"description,omitempty"`
	CacheClusterEnabled  bool              `json:"cacheClusterEnabled"`
	CacheClusterSize     string            `json:"cacheClusterSize,omitempty"`
	CacheClusterStatus   string            `json:"cacheClusterStatus,omitempty"`
	MethodSettings       map[string]any    `json:"methodSettings,omitempty"`
	Variables            map[string]string `json:"variables,omitempty"`
	DocumentationVersion string            `json:"documentationVersion,omitempty"`
	AccessLogSettings    any               `json:"accessLogSettings,omitempty"`
	CanarySettings       any               `json:"canarySettings,omitempty"`
	TracingEnabled       bool              `json:"tracingEnabled"`
	WebAclArn            string            `json:"webAclArn,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
	CreatedDate          core.Time         `json:"createdDate"`
	LastUpdatedDate      core.Time         `json:"lastUpdatedDate"`
}

type Authorizer struct {
	ID                           string   `json:"id"`
	Name                         string   `json:"name"`
	Type                         string   `json:"type"`
	ProviderARNs                 []string `json:"providerARNs,omitempty"`
	AuthType                     string   `json:"authType,omitempty"`
	AuthorizerURI                string   `json:"authorizerUri,omitempty"`
	AuthorizerCredentials        string   `json:"authorizerCredentials,omitempty"`
	IdentitySource               string   `json:"identitySource,omitempty"`
	IdentityValidationExpression string   `json:"identityValidationExpression,omitempty"`
	AuthorizerResultTtlInSeconds *int     `json:"authorizerResultTtlInSeconds,omitempty"`
}

type ApiKey struct {
	ID              string            `json:"id"`
	Value           string            `json:"value,omitempty"`
	Name            string            `json:"name,omitempty"`
	CustomerID      string            `json:"customerId,omitempty"`
	Description     string            `json:"description,omitempty"`
	Enabled         bool              `json:"enabled"`
	CreatedDate     core.Time         `json:"createdDate"`
	LastUpdatedDate core.Time         `json:"lastUpdatedDate"`
	Tags            map[string]string `json:"tags,omitempty"`
}

type UsagePlan struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	ApiStages   []ApiStage        `json:"apiStages,omitempty"`
	Throttle    any               `json:"throttle,omitempty"`
	Quota       any               `json:"quota,omitempty"`
	ProductCode string            `json:"productCode,omitempty"`
	Tags        map[string]string `json:"tags,omitempty"`
}

type ApiStage struct {
	ApiID    string `json:"apiId"`
	Stage    string `json:"stage"`
	Throttle any    `json:"throttle,omitempty"`
}

// restHandler is a management operation on the REST API named in the path.
type restHandler func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error)

// v1 resolves the caller's REST API {api} for h; lock held.
func (s *Service) v1(h restHandler) handler {
	return func(r *http.Request, c *core.Ctx) (int, any, error) {
		a := s.st.Rest[r.PathValue("api")]
		if a == nil || a.Account != c.Account || a.Region != c.Region {
			return 0, nil, notFound("Invalid API identifier specified %s:%s", c.Account, r.PathValue("api"))
		}
		return h(a, r, c)
	}
}

func (a *restAPI) resource(id string) (*Resource, error) { return find(a.Resources, id, "Resource") }

// method returns the resource and method named in the path; m is nil when only the resource exists.
func (a *restAPI) method(r *http.Request) (res *Resource, m *Method, err error) {
	if res, err = a.resource(r.PathValue("res")); err != nil {
		return nil, nil, err
	}
	if m = res.ResourceMethods[strings.ToUpper(r.PathValue("verb"))]; m == nil {
		return res, nil, notFound("Invalid Method identifier specified")
	}
	return res, m, nil
}

// integration returns the integration of the method named in the path.
func (a *restAPI) integration(r *http.Request) (*Integration, error) {
	_, m, err := a.method(r)
	if err != nil {
		return nil, err
	}
	if m.MethodIntegration == nil {
		return nil, notFound("No integration defined for method")
	}
	return m.MethodIntegration, nil
}

func (s *Service) restRoutes() {
	const api = "/restapis/{api}"
	const res = api + "/resources/{res}"
	const verb = res + "/methods/{verb}"
	const integ = verb + "/integration"
	s.route("POST /restapis", "CreateRestApi", s.createRestApi)
	s.route("GET /restapis", "GetRestApis", func(r *http.Request, c *core.Ctx) (int, any, error) {
		list := []*RestApi{}
		for _, id := range core.SortedKeys(s.st.Rest) {
			if a := s.st.Rest[id]; a.Account == c.Account && a.Region == c.Region {
				list = append(list, &a.RestApi)
			}
		}
		return 200, map[string]any{"item": list}, nil
	})
	s.route("GET "+api, "GetRestApi", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		return 200, &a.RestApi, nil
	}))
	s.route("PATCH "+api, "UpdateRestApi", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		return 200, &a.RestApi, patch(r, &a.RestApi)
	}))
	s.route("PUT "+api, "PutRestApi", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		upd := clone(a) // a bad document leaves the API untouched
		warnings, err := upd.load(core.Body(r), r.URL.Query().Get("mode") == "overwrite")
		if err == nil {
			err = failOnWarnings(r, warnings)
		}
		if err != nil {
			return 0, nil, err
		}
		*a = *upd
		out := a.RestApi
		out.Warnings = warnings
		return 200, &out, nil
	}))
	s.route("DELETE "+api, "DeleteRestApi", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		delete(s.st.Rest, a.ID)
		return http.StatusAccepted, nil, nil
	}))

	// resources
	s.route("GET "+api+"/resources", "GetResources", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		list := slices.SortedFunc(maps.Values(a.Resources), func(x, y *Resource) int { return strings.Compare(x.Path, y.Path) })
		return 200, map[string]any{"item": list}, nil
	}))
	s.route("GET "+res, "GetResource", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		res, err := a.resource(r.PathValue("res"))
		return 200, res, err
	}))
	s.route("POST "+res, "CreateResource", s.v1(createResource))
	s.route("DELETE "+res, "DeleteResource", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		res, err := a.resource(r.PathValue("res"))
		if err != nil {
			return 0, nil, err
		}
		if res.ID == a.RootResourceID {
			return 0, nil, badRequest("Cannot delete the root resource")
		}
		for id, x := range a.Resources {
			if x.Path == res.Path || strings.HasPrefix(x.Path, res.Path+"/") {
				delete(a.Resources, id)
			}
		}
		return http.StatusAccepted, nil, nil
	}))

	// methods and method responses
	s.route("PUT "+verb, "PutMethod", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		res, m, err := a.method(r)
		if res == nil {
			return 0, nil, err
		}
		if m != nil {
			return 0, nil, conflict("Method already exists for this resource")
		}
		m = &Method{}
		if err := decode(r, m); err != nil {
			return 0, nil, err
		}
		m.HTTPMethod = strings.ToUpper(r.PathValue("verb"))
		if m.AuthorizationType == "" {
			m.AuthorizationType = "NONE"
		}
		if res.ResourceMethods == nil {
			res.ResourceMethods = map[string]*Method{}
		}
		res.ResourceMethods[m.HTTPMethod] = m
		return http.StatusCreated, m, nil
	}))
	s.route("GET "+verb, "GetMethod", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, m, err := a.method(r)
		return 200, m, err
	}))
	s.route("PATCH "+verb, "UpdateMethod", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, m, err := a.method(r)
		if err != nil {
			return 0, nil, err
		}
		return 200, m, patch(r, m)
	}))
	s.route("DELETE "+verb, "DeleteMethod", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		res, m, err := a.method(r)
		if err != nil {
			return 0, nil, err
		}
		delete(res.ResourceMethods, m.HTTPMethod)
		return http.StatusNoContent, nil, nil
	}))
	s.route("PUT "+verb+"/responses/{code}", "PutMethodResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, m, err := a.method(r)
		if err != nil {
			return 0, nil, err
		}
		mr := &MethodResponse{}
		if err := decode(r, mr); err != nil {
			return 0, nil, err
		}
		mr.StatusCode = r.PathValue("code")
		if m.MethodResponses == nil {
			m.MethodResponses = map[string]*MethodResponse{}
		}
		m.MethodResponses[mr.StatusCode] = mr
		return http.StatusCreated, mr, nil
	}))
	methodResponse := func(a *restAPI, r *http.Request) (*Method, *MethodResponse, error) {
		_, m, err := a.method(r)
		if err != nil {
			return nil, nil, err
		}
		if mr := m.MethodResponses[r.PathValue("code")]; mr != nil {
			return m, mr, nil
		}
		return nil, nil, notFound("Invalid Response status code specified")
	}
	s.route("GET "+verb+"/responses/{code}", "GetMethodResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, mr, err := methodResponse(a, r)
		return 200, mr, err
	}))
	s.route("PATCH "+verb+"/responses/{code}", "UpdateMethodResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, mr, err := methodResponse(a, r)
		if err != nil {
			return 0, nil, err
		}
		return http.StatusCreated, mr, patch(r, mr)
	}))
	s.route("DELETE "+verb+"/responses/{code}", "DeleteMethodResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		m, mr, err := methodResponse(a, r)
		if err != nil {
			return 0, nil, err
		}
		delete(m.MethodResponses, mr.StatusCode)
		return http.StatusNoContent, nil, nil
	}))

	// integrations and integration responses
	s.route("PUT "+integ, "PutIntegration", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		res, m, err := a.method(r)
		if err != nil {
			return 0, nil, err
		}
		in := &Integration{}
		if err := decode(r, in); err != nil {
			return 0, nil, err
		}
		if err := in.validate(); err != nil {
			return 0, nil, err
		}
		in.CacheNamespace = cmp.Or(in.CacheNamespace, res.ID)
		m.MethodIntegration = in
		return http.StatusCreated, in, nil
	}))
	s.route("GET "+integ, "GetIntegration", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		in, err := a.integration(r)
		return 200, in, err
	}))
	s.route("PATCH "+integ, "UpdateIntegration", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		in, err := a.integration(r)
		if err != nil {
			return 0, nil, err
		}
		return 200, in, patch(r, in)
	}))
	s.route("DELETE "+integ, "DeleteIntegration", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, m, err := a.method(r)
		if err != nil {
			return 0, nil, err
		}
		if m.MethodIntegration == nil {
			return 0, nil, notFound("No integration defined for method")
		}
		m.MethodIntegration = nil
		return http.StatusNoContent, nil, nil
	}))
	s.route("PUT "+integ+"/responses/{code}", "PutIntegrationResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		in, err := a.integration(r)
		if err != nil {
			return 0, nil, err
		}
		ir := &IntegrationResponse{}
		if err := decode(r, ir); err != nil {
			return 0, nil, err
		}
		ir.StatusCode = r.PathValue("code")
		if in.IntegrationResponses == nil {
			in.IntegrationResponses = map[string]*IntegrationResponse{}
		}
		in.IntegrationResponses[ir.StatusCode] = ir
		return http.StatusCreated, ir, nil
	}))
	integrationResponse := func(a *restAPI, r *http.Request) (*Integration, *IntegrationResponse, error) {
		in, err := a.integration(r)
		if err != nil {
			return nil, nil, err
		}
		if ir := in.IntegrationResponses[r.PathValue("code")]; ir != nil {
			return in, ir, nil
		}
		return nil, nil, notFound("Invalid Response status code specified")
	}
	s.route("GET "+integ+"/responses/{code}", "GetIntegrationResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, ir, err := integrationResponse(a, r)
		return 200, ir, err
	}))
	s.route("PATCH "+integ+"/responses/{code}", "UpdateIntegrationResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		_, ir, err := integrationResponse(a, r)
		if err != nil {
			return 0, nil, err
		}
		return 200, ir, patch(r, ir)
	}))
	s.route("DELETE "+integ+"/responses/{code}", "DeleteIntegrationResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		in, ir, err := integrationResponse(a, r)
		if err != nil {
			return 0, nil, err
		}
		delete(in.IntegrationResponses, ir.StatusCode)
		return http.StatusNoContent, nil, nil
	}))

	// deployments and stages
	s.route("POST "+api+"/deployments", "CreateDeployment", s.v1(createDeployment))
	crud1(s, api+"/deployments", "Deployment", func(a *restAPI) map[string]*Deployment { return a.Deployments }, func(a *restAPI, d *Deployment) error {
		for _, st := range a.Stages {
			if st.DeploymentID == d.ID {
				return badRequest("Active stages pointing to this deployment must be moved or deleted")
			}
		}
		return nil
	})
	s.route("POST "+api+"/stages", "CreateStage", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		st := &Stage{}
		if err := decode(r, st); err != nil {
			return 0, nil, err
		}
		if st.StageName == "" {
			return 0, nil, badRequest("Stage name must be non-empty")
		}
		if a.Stages[st.StageName] != nil {
			return 0, nil, conflict("Stage already exists")
		}
		if _, err := find(a.Deployments, st.DeploymentID, "Deployment"); err != nil {
			return 0, nil, err
		}
		st.CreatedDate = core.Time(time.Now())
		st.LastUpdatedDate = st.CreatedDate
		a.Stages[st.StageName] = st
		return http.StatusCreated, st, nil
	}))
	crud1(s, api+"/stages", "Stage", func(a *restAPI) map[string]*Stage { return a.Stages }, nil)

	// authorizers
	s.route("POST "+api+"/authorizers", "CreateAuthorizer", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		au := &Authorizer{}
		if err := decode(r, au); err != nil {
			return 0, nil, err
		}
		if !slices.Contains([]string{"TOKEN", "REQUEST", "COGNITO_USER_POOLS"}, au.Type) || au.Name == "" {
			return 0, nil, badRequest("Invalid authorizer: a name and a type of TOKEN, REQUEST or COGNITO_USER_POOLS are required")
		}
		if t := au.AuthorizerResultTtlInSeconds; t != nil && (*t < 0 || *t > 3600) {
			return 0, nil, badRequest("AuthorizerResultTtlInSeconds must be between 0 and 3600")
		}
		au.ID = newID()
		a.Authorizers[au.ID] = au
		return http.StatusCreated, au, nil
	}))
	crud1(s, api+"/authorizers", "Authorizer", func(a *restAPI) map[string]*Authorizer { return a.Authorizers }, nil)

	s.route("POST "+api+"/models", "CreateModel", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		m := &Model{}
		if err := decode(r, m); err != nil {
			return 0, nil, err
		}
		if m.Name == "" {
			return 0, nil, badRequest("A model needs a name")
		}
		if a.Models[m.Name] != nil {
			return 0, nil, conflict("Model name already exists for this REST API")
		}
		if a.Models == nil {
			a.Models = map[string]*Model{}
		}
		m.ID, m.ContentType = newID(), cmp.Or(m.ContentType, "application/json")
		a.Models[m.Name] = m
		return http.StatusCreated, m, nil
	}))
	crud1(s, api+"/models", "Model", func(a *restAPI) map[string]*Model { return a.Models }, nil)
	s.route("POST "+api+"/requestvalidators", "CreateRequestValidator", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		v := &RequestValidator{}
		if err := decode(r, v); err != nil {
			return 0, nil, err
		}
		if a.Validators == nil {
			a.Validators = map[string]*RequestValidator{}
		}
		v.ID = newID()
		a.Validators[v.ID] = v
		return http.StatusCreated, v, nil
	}))
	crud1(s, api+"/requestvalidators", "RequestValidator", func(a *restAPI) map[string]*RequestValidator { return a.Validators }, nil)
	s.route("PUT "+api+"/gatewayresponses/{id}", "PutGatewayResponse", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		g := &GatewayResponse{}
		if err := decode(r, g); err != nil {
			return 0, nil, err
		}
		if a.GatewayResps == nil {
			a.GatewayResps = map[string]*GatewayResponse{}
		}
		g.ResponseType = r.PathValue("id")
		a.GatewayResps[g.ResponseType] = g
		return http.StatusCreated, g, nil
	}))
	crud1(s, api+"/gatewayresponses", "GatewayResponse", func(a *restAPI) map[string]*GatewayResponse { return a.GatewayResps }, nil)

	s.keyRoutes()
	s.tagRoutes("/tags", "PUT", http.StatusNoContent)
}

// crud1 serves GetXs, GetX, UpdateX (patch operations) and DeleteX of one v1
// sub-resource kind; deletable can veto a delete.
func crud1[T any](s *Service, path, name string, of func(*restAPI) map[string]*T, deletable func(*restAPI, *T) error) {
	s.route("GET "+path, "Get"+name+"s", s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		return 200, items(of(a), "item"), nil
	}))
	s.route("GET "+path+"/{id}", "Get"+name, s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		v, err := find(of(a), r.PathValue("id"), name)
		return 200, v, err
	}))
	s.route("PATCH "+path+"/{id}", "Update"+name, s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		v, err := find(of(a), r.PathValue("id"), name)
		if err != nil {
			return 0, nil, err
		}
		return 200, v, patch(r, v)
	}))
	s.route("DELETE "+path+"/{id}", "Delete"+name, s.v1(func(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		v, err := find(of(a), r.PathValue("id"), name)
		if err == nil && deletable != nil {
			err = deletable(a, v)
		}
		if err != nil {
			return 0, nil, err
		}
		delete(of(a), r.PathValue("id"))
		return http.StatusAccepted, nil, nil
	}))
}

// keyRoutes serves API keys and usage plans; they are not scoped by account.
func (s *Service) keyRoutes() {
	s.route("POST /apikeys", "CreateApiKey", func(r *http.Request, c *core.Ctx) (int, any, error) {
		var in struct {
			Name, Description, Value, CustomerID string
			Enabled                              bool
			Tags                                 map[string]string
		}
		if err := decode(r, &in); err != nil {
			return 0, nil, err
		}
		now := core.Time(time.Now())
		k := &ApiKey{ID: newID(), Name: in.Name, Description: in.Description, CustomerID: in.CustomerID, Enabled: in.Enabled,
			Value: cmp.Or(in.Value, core.RandString(40, core.AlphaNum)), Tags: in.Tags, CreatedDate: now, LastUpdatedDate: now}
		s.st.Keys[k.ID] = k
		return http.StatusCreated, k, nil
	})
	// withValue hides the key value unless the caller asked for it.
	withValue := func(k *ApiKey, show bool) *ApiKey {
		out := *k
		if !show {
			out.Value = ""
		}
		return &out
	}
	s.route("GET /apikeys", "GetApiKeys", func(r *http.Request, c *core.Ctx) (int, any, error) {
		list := []*ApiKey{}
		for _, id := range core.SortedKeys(s.st.Keys) {
			list = append(list, withValue(s.st.Keys[id], r.URL.Query().Get("includeValues") == "true"))
		}
		return 200, map[string]any{"item": list}, nil
	})
	s.route("GET /apikeys/{id}", "GetApiKey", func(r *http.Request, c *core.Ctx) (int, any, error) {
		k, err := find(s.st.Keys, r.PathValue("id"), "API Key")
		if err != nil {
			return 0, nil, err
		}
		return 200, withValue(k, r.URL.Query().Get("includeValue") == "true"), nil
	})
	s.route("PATCH /apikeys/{id}", "UpdateApiKey", func(r *http.Request, c *core.Ctx) (int, any, error) {
		k, err := find(s.st.Keys, r.PathValue("id"), "API Key")
		if err != nil {
			return 0, nil, err
		}
		if err := patch(r, k); err != nil {
			return 0, nil, err
		}
		k.LastUpdatedDate = core.Time(time.Now())
		return 200, k, nil
	})
	s.route("DELETE /apikeys/{id}", "DeleteApiKey", func(r *http.Request, c *core.Ctx) (int, any, error) {
		id := r.PathValue("id")
		if _, err := find(s.st.Keys, id, "API Key"); err != nil {
			return 0, nil, err
		}
		delete(s.st.Keys, id)
		for p, keys := range s.st.PlanKeys {
			s.st.PlanKeys[p] = slices.DeleteFunc(keys, func(k string) bool { return k == id })
		}
		return http.StatusAccepted, nil, nil
	})

	s.route("POST /usageplans", "CreateUsagePlan", func(r *http.Request, c *core.Ctx) (int, any, error) {
		p := &UsagePlan{}
		if err := decode(r, p); err != nil {
			return 0, nil, err
		}
		p.ID = newID()
		s.st.Plans[p.ID] = p
		return http.StatusCreated, p, nil
	})
	s.route("GET /usageplans", "GetUsagePlans", func(r *http.Request, c *core.Ctx) (int, any, error) {
		return 200, items(s.st.Plans, "item"), nil
	})
	s.route("GET /usageplans/{id}", "GetUsagePlan", func(r *http.Request, c *core.Ctx) (int, any, error) {
		p, err := find(s.st.Plans, r.PathValue("id"), "Usage Plan")
		return 200, p, err
	})
	s.route("PATCH /usageplans/{id}", "UpdateUsagePlan", func(r *http.Request, c *core.Ctx) (int, any, error) {
		p, err := find(s.st.Plans, r.PathValue("id"), "Usage Plan")
		if err != nil {
			return 0, nil, err
		}
		return 200, p, patch(r, p)
	})
	s.route("DELETE /usageplans/{id}", "DeleteUsagePlan", func(r *http.Request, c *core.Ctx) (int, any, error) {
		if _, err := find(s.st.Plans, r.PathValue("id"), "Usage Plan"); err != nil {
			return 0, nil, err
		}
		delete(s.st.Plans, r.PathValue("id"))
		delete(s.st.PlanKeys, r.PathValue("id"))
		return http.StatusAccepted, nil, nil
	})
	planKey := func(k *ApiKey) map[string]string {
		return map[string]string{"id": k.ID, "type": "API_KEY", "value": k.Value, "name": k.Name}
	}
	s.route("POST /usageplans/{id}/keys", "CreateUsagePlanKey", func(r *http.Request, c *core.Ctx) (int, any, error) {
		var in struct{ KeyID, KeyType string }
		if err := decode(r, &in); err != nil {
			return 0, nil, err
		}
		p, err := find(s.st.Plans, r.PathValue("id"), "Usage Plan")
		if err != nil {
			return 0, nil, err
		}
		k, err := find(s.st.Keys, in.KeyID, "API Key")
		if err != nil {
			return 0, nil, err
		}
		if slices.Contains(s.st.PlanKeys[p.ID], k.ID) {
			return 0, nil, conflict("Usage Plan %s cannot be added because API Key %s cannot reference it", p.ID, k.ID)
		}
		s.st.PlanKeys[p.ID] = append(s.st.PlanKeys[p.ID], k.ID)
		return http.StatusCreated, planKey(k), nil
	})
	s.route("GET /usageplans/{id}/keys", "GetUsagePlanKeys", func(r *http.Request, c *core.Ctx) (int, any, error) {
		if _, err := find(s.st.Plans, r.PathValue("id"), "Usage Plan"); err != nil {
			return 0, nil, err
		}
		list := []any{}
		for _, id := range s.st.PlanKeys[r.PathValue("id")] {
			list = append(list, planKey(s.st.Keys[id]))
		}
		return 200, map[string]any{"item": list}, nil
	})
	s.route("GET /usageplans/{id}/keys/{key}", "GetUsagePlanKey", func(r *http.Request, c *core.Ctx) (int, any, error) {
		if !slices.Contains(s.st.PlanKeys[r.PathValue("id")], r.PathValue("key")) {
			return 0, nil, notFound("Invalid Usage Plan Key identifier specified")
		}
		return 200, planKey(s.st.Keys[r.PathValue("key")]), nil
	})
	s.route("DELETE /usageplans/{id}/keys/{key}", "DeleteUsagePlanKey", func(r *http.Request, c *core.Ctx) (int, any, error) {
		keys := s.st.PlanKeys[r.PathValue("id")]
		if !slices.Contains(keys, r.PathValue("key")) {
			return 0, nil, notFound("Invalid Usage Plan Key identifier specified")
		}
		s.st.PlanKeys[r.PathValue("id")] = slices.DeleteFunc(keys, func(k string) bool { return k == r.PathValue("key") })
		return http.StatusAccepted, nil, nil
	})
}

func (s *Service) createRestApi(r *http.Request, c *core.Ctx) (int, any, error) {
	var in RestApi
	importing := r.URL.Query().Get("mode") == "import"
	if importing {
		c.Op = "ImportRestApi"
	} else if err := decode(r, &in); err != nil {
		return 0, nil, err
	}
	in.ID = cmp.Or(in.Tags["_custom_id_"], newID()) // LocalStack's way to pick the id
	if s.st.Rest[in.ID] != nil || s.st.HTTP[in.ID] != nil {
		return 0, nil, conflict("An API with id %s already exists", in.ID)
	}
	root := &Resource{ID: newID(), Path: "/"}
	in.RootResourceID, in.CreatedDate, in.ApiKeySource, in.ApiStatus = root.ID, core.Time(time.Now()), cmp.Or(in.ApiKeySource, "HEADER"), "AVAILABLE"
	if in.EndpointConfiguration == nil {
		in.EndpointConfiguration = &EndpointConfiguration{Types: []string{"EDGE"}}
	}
	a := &restAPI{RestApi: in, Account: c.Account, Region: c.Region, Resources: map[string]*Resource{root.ID: root},
		Deployments: map[string]*Deployment{}, Stages: map[string]*Stage{}, Authorizers: map[string]*Authorizer{}}
	var warnings []string
	if importing {
		var err error
		if warnings, err = a.load(core.Body(r), true); err == nil {
			err = failOnWarnings(r, warnings)
		}
		if err != nil {
			return 0, nil, err
		}
	}
	if a.Name == "" {
		return 0, nil, badRequest("A REST API name is required")
	}
	s.st.Rest[a.ID] = a
	out := a.RestApi
	out.Warnings = warnings
	return http.StatusCreated, &out, nil
}

// failOnWarnings rejects an import that gave warnings when ?failonwarnings=true.
func failOnWarnings(r *http.Request, warnings []string) error {
	if len(warnings) > 0 && r.URL.Query().Get("failonwarnings") == "true" {
		return badRequest("Errors found during import:\n\t%s", strings.Join(warnings, "\n\t"))
	}
	return nil
}

func createResource(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
	parent, err := a.resource(r.PathValue("res"))
	if err != nil {
		return 0, nil, err
	}
	var in struct {
		PathPart string `json:"pathPart"`
	}
	if err := decode(r, &in); err != nil {
		return 0, nil, err
	}
	if in.PathPart == "" || strings.Contains(in.PathPart, "/") {
		return 0, nil, badRequest("Resource's path part only allow a-zA-Z0-9._- and curly braces at the beginning and the end.")
	}
	if strings.HasSuffix(parent.PathPart, "+}") {
		return 0, nil, badRequest("Cannot create a child of a resource with a greedy path variable: %s", parent.PathPart)
	}
	for _, sib := range a.Resources {
		switch {
		case sib.ParentID != parent.ID:
		case sib.PathPart == in.PathPart:
			return 0, nil, conflict("Another resource with the same parent already has this name: %s", in.PathPart)
		case strings.HasPrefix(sib.PathPart, "{") && strings.HasPrefix(in.PathPart, "{"):
			return 0, nil, badRequest("A sibling (%s) of this resource already has a variable path part -- only one is allowed", sib.PathPart)
		}
	}
	return http.StatusCreated, a.child(parent, in.PathPart), nil
}

// child adds a resource under parent.
func (a *restAPI) child(parent *Resource, part string) *Resource {
	res := &Resource{ID: newID(), ParentID: parent.ID, PathPart: part, Path: strings.TrimSuffix(parent.Path, "/") + "/" + part}
	a.Resources[res.ID] = res
	return res
}

func (in *Integration) validate() error {
	switch in.Type {
	case "MOCK":
	case "AWS", "AWS_PROXY", "HTTP", "HTTP_PROXY":
		if in.URI == "" || in.HTTPMethod == "" {
			return badRequest("Integrations of type %s require a uri and an httpMethod", in.Type)
		}
	default:
		return badRequest("Invalid integration type %q: use AWS, AWS_PROXY, HTTP, HTTP_PROXY or MOCK", in.Type)
	}
	if in.TimeoutInMillis == 0 {
		in.TimeoutInMillis = 29000
	}
	if in.CacheKeyParameters == nil {
		in.CacheKeyParameters = []string{} // AWS answers [], not a missing field
	}
	in.PassthroughBehavior = cmp.Or(in.PassthroughBehavior, "WHEN_NO_MATCH")
	in.ConnectionType = cmp.Or(in.ConnectionType, "INTERNET")
	return nil
}

func createDeployment(a *restAPI, r *http.Request, c *core.Ctx) (int, any, error) {
	var in struct {
		StageName        string            `json:"stageName"`
		StageDescription string            `json:"stageDescription"`
		Description      string            `json:"description"`
		Variables        map[string]string `json:"variables"`
	}
	if err := decode(r, &in); err != nil {
		return 0, nil, err
	}
	methods := 0
	for _, res := range a.Resources {
		for _, m := range res.ResourceMethods {
			if m.MethodIntegration == nil {
				return 0, nil, badRequest("No integration defined for method")
			}
			methods++
		}
	}
	if methods == 0 {
		return 0, nil, badRequest("The REST API doesn't contain any methods")
	}
	now := core.Time(time.Now())
	d := &Deployment{ID: newID(), Description: in.Description, CreatedDate: now}
	a.Deployments[d.ID] = d
	if in.StageName != "" {
		st := a.Stages[in.StageName]
		if st == nil {
			st = &Stage{StageName: in.StageName, Description: in.StageDescription, CreatedDate: now}
			a.Stages[in.StageName] = st
		}
		st.DeploymentID, st.LastUpdatedDate = d.ID, now
		if in.Variables != nil {
			st.Variables = in.Variables
		}
	}
	return http.StatusCreated, d, nil
}

// load imports an OpenAPI 3 or Swagger 2 JSON document with
// x-amazon-apigateway-integration extensions. overwrite replaces every
// resource and the API's name and description; otherwise methods are merged.
// It returns warnings for parts it could not read (they do not stop the import).
// Of the security schemes, the API key one sets apiKeyRequired, the
// x-amazon-apigateway-authorizer ones become authorizers (token, request or
// Cognito) and awsSigv4 means AWS_IAM; a method's security list picks one.
// ponytail: JSON only, no YAML or basePath handling; add when needed.
func (a *restAPI) load(body []byte, overwrite bool) (warnings []string, _ error) {
	var doc struct {
		Swagger string `json:"swagger"`
		OpenAPI string `json:"openapi"`
		Info    struct {
			Title       string `json:"title"`
			Description string `json:"description"`
			Version     string `json:"version"`
		} `json:"info"`
		Paths            map[string]map[string]json.RawMessage `json:"paths"`
		BinaryMediaTypes []string                              `json:"x-amazon-apigateway-binary-media-types"`
		ApiKeySource     string                                `json:"x-amazon-apigateway-api-key-source"`
		Security         json.RawMessage                       `json:"security"`
		Schemes          map[string]securityScheme             `json:"securityDefinitions"` // Swagger 2
		Components       struct {
			Schemes map[string]securityScheme `json:"securitySchemes"`
		} `json:"components"`
	}
	if json.Unmarshal(body, &doc) != nil || doc.Swagger == "" && doc.OpenAPI == "" {
		return nil, badRequest("Invalid OpenAPI input: pogi imports OpenAPI 3 and Swagger 2 documents in JSON")
	}
	schemes := doc.Components.Schemes
	if schemes == nil {
		schemes = doc.Schemes
	}
	// authIDs maps a scheme name to the authorizer made for it.
	authIDs := map[string]string{}
	for _, name := range core.SortedKeys(schemes) {
		if sc := schemes[name]; sc.Authorizer != nil {
			authIDs[name] = a.importAuthorizer(name, sc)
		}
	}
	// readSecurity reads a security list: whether the API key scheme is named,
	// and the authorization (type, authorizer id, scopes) of the first scheme that sets one.
	type auth struct {
		keyRequired bool
		typ, id     string
		scopes      []string
	}
	readSecurity := func(raw json.RawMessage) (auth, error) {
		var reqs []map[string][]string
		if err := json.Unmarshal(raw, &reqs); err != nil {
			return auth{}, err
		}
		out := auth{typ: "NONE"}
		for _, req := range reqs {
			for _, name := range core.SortedKeys(req) {
				sc := schemes[name]
				switch {
				case sc.Type == "apiKey" && sc.In == "header" && strings.EqualFold(sc.Name, "x-api-key"):
					out.keyRequired = true
				case authIDs[name] != "" && out.typ == "NONE":
					out.typ, out.id, out.scopes = "CUSTOM", authIDs[name], req[name]
					if sc.Authorizer.Type == "cognito_user_pools" {
						out.typ = "COGNITO_USER_POOLS"
					}
				case sc.AuthType == "awsSigv4" && out.typ == "NONE":
					out.typ = "AWS_IAM"
				}
			}
		}
		return out, nil
	}
	global := auth{typ: "NONE"}
	if doc.Security != nil {
		var err error
		if global, err = readSecurity(doc.Security); err != nil {
			warnings = append(warnings, "Invalid OpenAPI input: security must be an array of requirement objects")
		}
	}
	if doc.ApiKeySource != "" {
		a.ApiKeySource = doc.ApiKeySource
	}
	if overwrite {
		root := a.Resources[a.RootResourceID]
		root.ResourceMethods = nil
		a.Resources = map[string]*Resource{root.ID: root}
		a.Name, a.Description, a.Version = cmp.Or(doc.Info.Title, a.Name), doc.Info.Description, doc.Info.Version
	}
	if doc.BinaryMediaTypes != nil {
		a.BinaryMediaTypes = doc.BinaryMediaTypes
	}
	for _, p := range core.SortedKeys(doc.Paths) {
		res := a.Resources[a.RootResourceID]
		for _, part := range strings.FieldsFunc(p, func(r rune) bool { return r == '/' }) {
			var next *Resource
			for _, x := range a.Resources {
				if x.ParentID == res.ID && x.PathPart == part {
					next = x
				}
			}
			if next == nil {
				next = a.child(res, part)
			}
			res = next
		}
		for verb, raw := range doc.Paths[p] {
			verb = strings.ToUpper(verb)
			if verb == "X-AMAZON-APIGATEWAY-ANY-METHOD" {
				verb = "ANY"
			}
			if !slices.Contains([]string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "ANY"}, verb) {
				continue // parameters, summary, …
			}
			var op struct {
				OperationID string                     `json:"operationId"`
				Security    json.RawMessage            `json:"security"`
				Responses   map[string]json.RawMessage `json:"responses"`
				Integration *struct {
					Integration
					Responses map[string]IntegrationResponse `json:"responses"`
				} `json:"x-amazon-apigateway-integration"`
			}
			if err := json.Unmarshal(raw, &op); err != nil {
				return nil, badRequest("Invalid OpenAPI operation %s %s: %v", verb, p, err)
			}
			au := global
			if op.Security != nil { // an operation's own list replaces the API-wide one
				var err error
				if au, err = readSecurity(op.Security); err != nil {
					au = global
					warnings = append(warnings, "Invalid OpenAPI input: security of "+verb+" "+p+" must be an array of requirement objects")
				}
			}
			m := &Method{HTTPMethod: verb, AuthorizationType: au.typ, AuthorizerID: au.id, AuthorizationScopes: au.scopes, OperationName: op.OperationID,
				ApiKeyRequired: au.keyRequired, MethodResponses: map[string]*MethodResponse{}}
			for code := range op.Responses {
				if code != "default" {
					m.MethodResponses[code] = &MethodResponse{StatusCode: code}
				}
			}
			if x := op.Integration; x != nil {
				in := x.Integration
				in.Type, in.HTTPMethod = strings.ToUpper(in.Type), strings.ToUpper(in.HTTPMethod)
				in.URI, in.Credentials = cfnVars(in.URI, a), cfnVars(in.Credentials, a)
				if err := in.validate(); err != nil {
					return nil, err
				}
				for pattern, ir := range x.Responses {
					if pattern != "default" {
						ir.SelectionPattern = pattern
					}
					if in.IntegrationResponses == nil {
						in.IntegrationResponses = map[string]*IntegrationResponse{}
					}
					if ir.StatusCode != "" {
						in.IntegrationResponses[ir.StatusCode] = &ir
					}
				}
				m.MethodIntegration = &in
			}
			if res.ResourceMethods == nil {
				res.ResourceMethods = map[string]*Method{}
			}
			res.ResourceMethods[verb] = m
		}
	}
	return warnings, nil
}

// securityScheme is the part of an OpenAPI security scheme pogi reads.
type securityScheme struct {
	Type       string `json:"type"`
	Name       string `json:"name"`
	In         string `json:"in"`
	AuthType   string `json:"x-amazon-apigateway-authtype"`
	Authorizer *struct {
		Type                         string   `json:"type"`
		ProviderARNs                 []string `json:"providerARNs"`
		AuthorizerURI                string   `json:"authorizerUri"`
		AuthorizerCredentials        string   `json:"authorizerCredentials"`
		IdentitySource               string   `json:"identitySource"`
		IdentityValidationExpression string   `json:"identityValidationExpression"`
		AuthorizerResultTtlInSeconds *int     `json:"authorizerResultTtlInSeconds"`
	} `json:"x-amazon-apigateway-authorizer"`
}

// importAuthorizer creates (or, on a second import, updates) the authorizer a
// security scheme describes and returns its id.
func (a *restAPI) importAuthorizer(name string, sc securityScheme) string {
	x := sc.Authorizer
	typ := strings.ToUpper(x.Type)
	if typ == "TOKEN" || typ == "COGNITO_USER_POOLS" {
		// a token authorizer reads the header the scheme names
		if x.IdentitySource == "" && sc.In == "header" && sc.Name != "" {
			x.IdentitySource = "method.request.header." + sc.Name
		}
	}
	au := &Authorizer{Name: name, Type: typ, ProviderARNs: x.ProviderARNs, AuthType: sc.AuthType,
		AuthorizerURI: cfnVars(x.AuthorizerURI, a), AuthorizerCredentials: cfnVars(x.AuthorizerCredentials, a), IdentitySource: x.IdentitySource,
		IdentityValidationExpression: x.IdentityValidationExpression, AuthorizerResultTtlInSeconds: x.AuthorizerResultTtlInSeconds}
	for _, old := range a.Authorizers {
		if old.Name == name {
			au.ID = old.ID
		}
	}
	if au.ID == "" {
		au.ID = newID()
	}
	a.Authorizers[au.ID] = au
	return au.ID
}

// cfnVars fills in the CloudFormation-style variables that AWS resolves when
// it imports a spec: ${AWS::Partition}, ${AWS::Region} and ${AWS::AccountId}.
func cfnVars(s string, a *restAPI) string {
	return strings.NewReplacer("${AWS::Partition}", core.Partition(a.Region), "${AWS::Region}", a.Region, "${AWS::AccountId}", a.Account).Replace(s)
}
