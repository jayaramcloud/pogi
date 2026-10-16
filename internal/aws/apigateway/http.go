package apigateway

import (
	"cmp"
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The v2 wire shapes double as state; v2 timestamps are ISO 8601.

// Api is a v2 HTTP or WebSocket API.
type Api struct {
	ApiEndpoint               string            `json:"apiEndpoint,omitempty"`
	ApiID                     string            `json:"apiId"`
	ApiKeySelectionExpression string            `json:"apiKeySelectionExpression,omitempty"`
	CorsConfiguration         *Cors             `json:"corsConfiguration,omitempty"`
	CreatedDate               time.Time         `json:"createdDate"`
	Description               string            `json:"description,omitempty"`
	DisableSchemaValidation   bool              `json:"disableSchemaValidation,omitempty"`
	DisableExecuteApiEndpoint bool              `json:"disableExecuteApiEndpoint"`
	IpAddressType             string            `json:"ipAddressType,omitempty"`
	Name                      string            `json:"name"`
	ProtocolType              string            `json:"protocolType"`
	RouteSelectionExpression  string            `json:"routeSelectionExpression,omitempty"`
	Tags                      map[string]string `json:"tags,omitempty"`
	Version                   string            `json:"version,omitempty"`
}

type Cors struct {
	AllowCredentials bool     `json:"allowCredentials,omitempty"`
	AllowHeaders     []string `json:"allowHeaders,omitempty"`
	AllowMethods     []string `json:"allowMethods,omitempty"`
	AllowOrigins     []string `json:"allowOrigins,omitempty"`
	ExposeHeaders    []string `json:"exposeHeaders,omitempty"`
	MaxAge           *int     `json:"maxAge,omitempty"`
}

// httpAPI is a v2 API with everything under it.
type httpAPI struct {
	Api
	Account      string                      `json:"account"`
	Region       string                      `json:"region"`
	Routes       map[string]*Route           `json:"routes"`
	Integrations map[string]*HTTPIntegration `json:"integrations"`
	Stages       map[string]*HTTPStage       `json:"stages"`
	Deployments  map[string]*HTTPDeployment  `json:"deployments"`
	Authorizers  map[string]*HTTPAuthorizer  `json:"authorizers"`

	// WebSocket APIs: route id or integration id → response id → response
	RouteResponses       map[string]map[string]*RouteResponse           `json:"routeResponses,omitempty"`
	IntegrationResponses map[string]map[string]*HTTPIntegrationResponse `json:"integrationResponses,omitempty"`
}

// RouteResponse makes a WebSocket route two-way: its integration's answer
// goes back to the client.
type RouteResponse struct {
	ModelSelectionExpression string            `json:"modelSelectionExpression,omitempty"`
	ResponseModels           map[string]string `json:"responseModels,omitempty"`
	ResponseParameters       map[string]any    `json:"responseParameters,omitempty"`
	RouteResponseID          string            `json:"routeResponseId"`
	RouteResponseKey         string            `json:"routeResponseKey"`
}

// HTTPIntegrationResponse maps a WebSocket integration's answer; its key is
// $default or a status pattern such as /2\d\d/.
type HTTPIntegrationResponse struct {
	ContentHandlingStrategy     string            `json:"contentHandlingStrategy,omitempty"`
	IntegrationResponseID       string            `json:"integrationResponseId"`
	IntegrationResponseKey      string            `json:"integrationResponseKey"`
	ResponseParameters          map[string]string `json:"responseParameters,omitempty"`
	ResponseTemplates           map[string]string `json:"responseTemplates,omitempty"`
	TemplateSelectionExpression string            `json:"templateSelectionExpression,omitempty"`
}

type Route struct {
	ApiGatewayManaged                bool              `json:"apiGatewayManaged,omitempty"`
	ApiKeyRequired                   bool              `json:"apiKeyRequired"`
	AuthorizationScopes              []string          `json:"authorizationScopes,omitempty"`
	AuthorizationType                string            `json:"authorizationType"`
	AuthorizerID                     string            `json:"authorizerId,omitempty"`
	ModelSelectionExpression         string            `json:"modelSelectionExpression,omitempty"`
	OperationName                    string            `json:"operationName,omitempty"`
	RequestModels                    map[string]string `json:"requestModels,omitempty"`
	RequestParameters                map[string]any    `json:"requestParameters,omitempty"`
	RouteID                          string            `json:"routeId"`
	RouteKey                         string            `json:"routeKey"`
	RouteResponseSelectionExpression string            `json:"routeResponseSelectionExpression,omitempty"`
	Target                           string            `json:"target,omitempty"`
}

type HTTPIntegration struct {
	ApiGatewayManaged           bool                         `json:"apiGatewayManaged,omitempty"`
	ConnectionID                string                       `json:"connectionId,omitempty"`
	ConnectionType              string                       `json:"connectionType,omitempty"`
	ContentHandlingStrategy     string                       `json:"contentHandlingStrategy,omitempty"`
	CredentialsArn              string                       `json:"credentialsArn,omitempty"`
	Description                 string                       `json:"description,omitempty"`
	IntegrationID               string                       `json:"integrationId"`
	IntegrationMethod           string                       `json:"integrationMethod,omitempty"`
	IntegrationSubtype          string                       `json:"integrationSubtype,omitempty"`
	IntegrationType             string                       `json:"integrationType"`
	IntegrationURI              string                       `json:"integrationUri,omitempty"`
	PassthroughBehavior         string                       `json:"passthroughBehavior,omitempty"`
	PayloadFormatVersion        string                       `json:"payloadFormatVersion,omitempty"`
	RequestParameters           map[string]string            `json:"requestParameters,omitempty"`
	RequestTemplates            map[string]string            `json:"requestTemplates,omitempty"`
	ResponseParameters          map[string]map[string]string `json:"responseParameters,omitempty"`
	TemplateSelectionExpression string                       `json:"templateSelectionExpression,omitempty"`
	TimeoutInMillis             int                          `json:"timeoutInMillis,omitempty"`
	TlsConfig                   any                          `json:"tlsConfig,omitempty"`
}

type HTTPStage struct {
	AccessLogSettings    any               `json:"accessLogSettings,omitempty"`
	ApiGatewayManaged    bool              `json:"apiGatewayManaged,omitempty"`
	AutoDeploy           bool              `json:"autoDeploy"`
	ClientCertificateID  string            `json:"clientCertificateId,omitempty"`
	CreatedDate          time.Time         `json:"createdDate"`
	DefaultRouteSettings any               `json:"defaultRouteSettings,omitempty"`
	DeploymentID         string            `json:"deploymentId,omitempty"`
	Description          string            `json:"description,omitempty"`
	LastUpdatedDate      time.Time         `json:"lastUpdatedDate"`
	RouteSettings        map[string]any    `json:"routeSettings,omitempty"`
	StageName            string            `json:"stageName"`
	StageVariables       map[string]string `json:"stageVariables,omitempty"`
	Tags                 map[string]string `json:"tags,omitempty"`
}

type HTTPDeployment struct {
	AutoDeployed     bool      `json:"autoDeployed"`
	CreatedDate      time.Time `json:"createdDate"`
	DeploymentID     string    `json:"deploymentId"`
	DeploymentStatus string    `json:"deploymentStatus"`
	Description      string    `json:"description,omitempty"`
}

type HTTPAuthorizer struct {
	AuthorizerCredentialsArn       string            `json:"authorizerCredentialsArn,omitempty"`
	AuthorizerID                   string            `json:"authorizerId"`
	AuthorizerPayloadFormatVersion string            `json:"authorizerPayloadFormatVersion,omitempty"`
	AuthorizerResultTtlInSeconds   int               `json:"authorizerResultTtlInSeconds,omitempty"`
	AuthorizerType                 string            `json:"authorizerType"`
	AuthorizerURI                  string            `json:"authorizerUri,omitempty"`
	EnableSimpleResponses          bool              `json:"enableSimpleResponses,omitempty"`
	IdentitySource                 []string          `json:"identitySource,omitempty"`
	IdentityValidationExpression   string            `json:"identityValidationExpression,omitempty"`
	JwtConfiguration               *JWTConfiguration `json:"jwtConfiguration,omitempty"`
	Name                           string            `json:"name"`
}

type JWTConfiguration struct {
	Audience []string `json:"audience,omitempty"`
	Issuer   string   `json:"issuer,omitempty"`
}

// httpHandler is a management operation on the v2 API named in the path.
type httpHandler func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error)

// v2 resolves the caller's API {api} for h; lock held.
func (s *Service) v2(h httpHandler) handler {
	return func(r *http.Request, c *core.Ctx) (int, any, error) {
		a := s.st.HTTP[r.PathValue("api")]
		if a == nil || a.Account != c.Account || a.Region != c.Region {
			return 0, nil, notFound("Invalid API identifier specified %s:%s", c.Account, r.PathValue("api"))
		}
		return h(a, r, c)
	}
}

// out returns the API's wire shape with its endpoint as this caller reaches it.
func (a *httpAPI) out(c *core.Ctx) *Api {
	a.ApiEndpoint = a.url(c.Host)
	return &a.Api
}

// url is the API's endpoint for a client that reached pogi at host.
func (a *httpAPI) url(host string) string {
	u := endpoint(host, a.ApiID)
	if a.ProtocolType == "WEBSOCKET" {
		u = "ws" + strings.TrimPrefix(u, "http")
	}
	return u
}

func (s *Service) httpRoutes() {
	const api = "/v2/apis/{api}"
	s.route("POST /v2/apis", "CreateApi", s.createApi)
	s.route("GET /v2/apis", "GetApis", func(r *http.Request, c *core.Ctx) (int, any, error) {
		list := []*Api{}
		for _, id := range core.SortedKeys(s.st.HTTP) {
			if a := s.st.HTTP[id]; a.Account == c.Account && a.Region == c.Region {
				list = append(list, a.out(c))
			}
		}
		return 200, map[string]any{"items": list}, nil
	})
	s.route("GET "+api, "GetApi", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		return 200, a.out(c), nil
	}))
	s.route("PATCH "+api, "UpdateApi", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		upd := clone(&a.Api)
		if err := decode(r, upd); err != nil {
			return 0, nil, err
		}
		a.Api = *upd
		return 200, a.out(c), nil
	}))
	s.route("DELETE "+api, "DeleteApi", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		delete(s.st.HTTP, a.ApiID)
		return http.StatusNoContent, nil, nil
	}))
	s.route("DELETE "+api+"/cors", "DeleteCorsConfiguration", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		a.CorsConfiguration = nil
		return http.StatusNoContent, nil, nil
	}))

	s.route("POST "+api+"/routes", "CreateRoute", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		rt := &Route{RouteID: newID()}
		if err := decode(r, rt); err != nil {
			return 0, nil, err
		}
		if err := a.checkRoute(rt); err != nil {
			return 0, nil, err
		}
		a.Routes[rt.RouteID] = rt
		return http.StatusCreated, rt, nil
	}))
	crud(s, api+"/routes", "Route", func(a *httpAPI, _ *http.Request) map[string]*Route { return a.Routes }, (*httpAPI).checkRoute)

	s.route("POST "+api+"/routes/{route}/routeresponses", "CreateRouteResponse", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		rr := &RouteResponse{RouteResponseID: newID()}
		if err := decode(r, rr); err != nil {
			return 0, nil, err
		}
		if _, err := find(a.Routes, r.PathValue("route"), "Route"); err != nil {
			return 0, nil, err
		}
		if rr.RouteResponseKey == "" {
			return 0, nil, badRequest("RouteResponseKey is required")
		}
		a.RouteResponses = nest(a.RouteResponses, r.PathValue("route"), rr.RouteResponseID, rr)
		return http.StatusCreated, rr, nil
	}))
	crud(s, api+"/routes/{route}/routeresponses", "RouteResponse", func(a *httpAPI, r *http.Request) map[string]*RouteResponse {
		return a.RouteResponses[r.PathValue("route")]
	}, nil)

	s.route("POST "+api+"/integrations", "CreateIntegration", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		in := &HTTPIntegration{IntegrationID: newID()}
		if err := decode(r, in); err != nil {
			return 0, nil, err
		}
		if err := a.checkIntegration(in); err != nil {
			return 0, nil, err
		}
		a.Integrations[in.IntegrationID] = in
		return http.StatusCreated, in, nil
	}))
	crud(s, api+"/integrations", "Integration", func(a *httpAPI, _ *http.Request) map[string]*HTTPIntegration { return a.Integrations }, (*httpAPI).checkIntegration)

	s.route("POST "+api+"/integrations/{integ}/integrationresponses", "CreateIntegrationResponse", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		ir := &HTTPIntegrationResponse{IntegrationResponseID: newID()}
		if err := decode(r, ir); err != nil {
			return 0, nil, err
		}
		if _, err := find(a.Integrations, r.PathValue("integ"), "Integration"); err != nil {
			return 0, nil, err
		}
		if ir.IntegrationResponseKey == "" {
			return 0, nil, badRequest("IntegrationResponseKey is required")
		}
		a.IntegrationResponses = nest(a.IntegrationResponses, r.PathValue("integ"), ir.IntegrationResponseID, ir)
		return http.StatusCreated, ir, nil
	}))
	crud(s, api+"/integrations/{integ}/integrationresponses", "IntegrationResponse", func(a *httpAPI, r *http.Request) map[string]*HTTPIntegrationResponse {
		return a.IntegrationResponses[r.PathValue("integ")]
	}, nil)

	s.route("POST "+api+"/stages", "CreateStage", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		st := &HTTPStage{}
		if err := decode(r, st); err != nil {
			return 0, nil, err
		}
		if st.StageName == "" {
			return 0, nil, badRequest("StageName is required")
		}
		if a.Stages[st.StageName] != nil {
			return 0, nil, conflict("Stage %s already exists", st.StageName)
		}
		st.CreatedDate = time.Now().UTC()
		st.LastUpdatedDate = st.CreatedDate
		if st.AutoDeploy {
			st.DeploymentID = a.deploy("", true).DeploymentID
		}
		a.Stages[st.StageName] = st
		return http.StatusCreated, st, nil
	}))
	crud(s, api+"/stages", "Stage", func(a *httpAPI, _ *http.Request) map[string]*HTTPStage { return a.Stages }, func(_ *httpAPI, st *HTTPStage) error {
		st.LastUpdatedDate = time.Now().UTC()
		return nil
	})

	s.route("POST "+api+"/deployments", "CreateDeployment", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		var in struct{ Description, StageName string }
		if err := decode(r, &in); err != nil {
			return 0, nil, err
		}
		var st *HTTPStage
		if in.StageName != "" {
			var err error
			if st, err = find(a.Stages, in.StageName, "Stage"); err != nil {
				return 0, nil, err
			}
		}
		d := a.deploy(in.Description, false)
		if st != nil {
			st.DeploymentID, st.LastUpdatedDate = d.DeploymentID, d.CreatedDate
		}
		return http.StatusCreated, d, nil
	}))
	crud(s, api+"/deployments", "Deployment", func(a *httpAPI, _ *http.Request) map[string]*HTTPDeployment { return a.Deployments }, nil)

	s.route("POST "+api+"/authorizers", "CreateAuthorizer", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		au := &HTTPAuthorizer{AuthorizerID: newID()}
		if err := decode(r, au); err != nil {
			return 0, nil, err
		}
		if err := checkAuthorizer(a, au); err != nil {
			return 0, nil, err
		}
		a.Authorizers[au.AuthorizerID] = au
		return http.StatusCreated, au, nil
	}))
	crud(s, api+"/authorizers", "Authorizer", func(a *httpAPI, _ *http.Request) map[string]*HTTPAuthorizer { return a.Authorizers }, checkAuthorizer)

	s.tagRoutes("/v2/tags", "POST", http.StatusCreated)
}

// crud serves GetXs, GetX, UpdateX and DeleteX of one v2 sub-resource kind.
// Updates merge the PATCH body into a copy that check validates.
func crud[T any](s *Service, path, name string, of func(*httpAPI, *http.Request) map[string]*T, check func(*httpAPI, *T) error) {
	s.route("GET "+path, "Get"+name+"s", s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		return 200, items(of(a, r), "items"), nil
	}))
	s.route("GET "+path+"/{id}", "Get"+name, s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		v, err := find(of(a, r), r.PathValue("id"), name)
		return 200, v, err
	}))
	s.route("PATCH "+path+"/{id}", "Update"+name, s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		v, err := find(of(a, r), r.PathValue("id"), name)
		if err != nil {
			return 0, nil, err
		}
		upd := clone(v)
		if err := decode(r, upd); err != nil {
			return 0, nil, err
		}
		if check != nil {
			if err := check(a, upd); err != nil {
				return 0, nil, err
			}
		}
		*v = *upd
		return 200, v, nil
	}))
	s.route("DELETE "+path+"/{id}", "Delete"+name, s.v2(func(a *httpAPI, r *http.Request, c *core.Ctx) (int, any, error) {
		if _, err := find(of(a, r), r.PathValue("id"), name); err != nil {
			return 0, nil, err
		}
		delete(of(a, r), r.PathValue("id"))
		return http.StatusNoContent, nil, nil
	}))
}

func (s *Service) createApi(r *http.Request, c *core.Ctx) (int, any, error) {
	var in struct {
		Api
		Target         string `json:"target"`
		RouteKey       string `json:"routeKey"`
		CredentialsArn string `json:"credentialsArn"`
	}
	if err := decode(r, &in); err != nil {
		return 0, nil, err
	}
	switch {
	case in.Name == "":
		return 0, nil, badRequest("Name is required")
	case in.ProtocolType == "HTTP":
		in.RouteSelectionExpression = cmp.Or(in.RouteSelectionExpression, "$request.method $request.path")
	case in.ProtocolType != "WEBSOCKET":
		return 0, nil, badRequest("Invalid ProtocolType %q: use HTTP or WEBSOCKET", in.ProtocolType)
	case in.RouteSelectionExpression == "":
		return 0, nil, badRequest("RouteSelectionExpression is required for WEBSOCKET APIs")
	case in.Target != "":
		return 0, nil, badRequest("Quick create is only supported for HTTP APIs")
	}
	in.ApiID = cmp.Or(in.Tags["_custom_id_"], newID())
	if s.st.HTTP[in.ApiID] != nil || s.st.Rest[in.ApiID] != nil {
		return 0, nil, conflict("An API with id %s already exists", in.ApiID)
	}
	in.CreatedDate = time.Now().UTC()
	in.ApiKeySelectionExpression = cmp.Or(in.ApiKeySelectionExpression, "$request.header.x-api-key")
	a := &httpAPI{Api: in.Api, Account: c.Account, Region: c.Region, Routes: map[string]*Route{},
		Integrations: map[string]*HTTPIntegration{}, Stages: map[string]*HTTPStage{},
		Deployments: map[string]*HTTPDeployment{}, Authorizers: map[string]*HTTPAuthorizer{}}
	if in.Target != "" { // quick create: integration, route and an auto-deployed $default stage
		integ := &HTTPIntegration{IntegrationID: newID(), IntegrationType: "AWS_PROXY", IntegrationURI: in.Target,
			PayloadFormatVersion: "2.0", CredentialsArn: in.CredentialsArn, ApiGatewayManaged: true}
		if strings.HasPrefix(in.Target, "http://") || strings.HasPrefix(in.Target, "https://") {
			integ.IntegrationType, integ.IntegrationMethod, integ.PayloadFormatVersion = "HTTP_PROXY", "ANY", "1.0"
		}
		a.checkIntegration(integ)
		a.Integrations[integ.IntegrationID] = integ
		rt := &Route{RouteID: newID(), RouteKey: cmp.Or(in.RouteKey, "$default"), AuthorizationType: "NONE",
			Target: "integrations/" + integ.IntegrationID, ApiGatewayManaged: true}
		a.Routes[rt.RouteID] = rt
		a.Stages["$default"] = &HTTPStage{StageName: "$default", AutoDeploy: true, DeploymentID: a.deploy("", true).DeploymentID,
			CreatedDate: in.CreatedDate, LastUpdatedDate: in.CreatedDate, ApiGatewayManaged: true}
	}
	s.st.HTTP[a.ApiID] = a
	return http.StatusCreated, a.out(c), nil
}

func (a *httpAPI) deploy(desc string, auto bool) *HTTPDeployment {
	d := &HTTPDeployment{DeploymentID: newID(), DeploymentStatus: "DEPLOYED", Description: desc, AutoDeployed: auto, CreatedDate: time.Now().UTC()}
	a.Deployments[d.DeploymentID] = d
	return d
}

func (a *httpAPI) checkRoute(rt *Route) error {
	if rt.RouteKey == "" {
		return badRequest("RouteKey is required")
	}
	if method, path, ok := strings.Cut(rt.RouteKey, " "); a.ProtocolType == "HTTP" && rt.RouteKey != "$default" && (!ok || method == "" || !strings.HasPrefix(path, "/")) {
		return badRequest("Invalid route key %q: use \"METHOD /path\" or \"$default\"", rt.RouteKey)
	}
	for _, o := range a.Routes {
		if o.RouteKey == rt.RouteKey && o.RouteID != rt.RouteID {
			return conflict("Route with key %s already exists for this API", rt.RouteKey)
		}
	}
	rt.AuthorizationType = cmp.Or(rt.AuthorizationType, "NONE")
	return nil
}

func (a *httpAPI) checkIntegration(in *HTTPIntegration) error {
	switch in.IntegrationType {
	case "MOCK":
	case "AWS_PROXY", "HTTP_PROXY", "AWS", "HTTP":
		if in.IntegrationURI == "" {
			return badRequest("IntegrationUri is required for %s integrations", in.IntegrationType)
		}
	default:
		return badRequest("Invalid IntegrationType %q", in.IntegrationType)
	}
	in.PayloadFormatVersion = cmp.Or(in.PayloadFormatVersion, "1.0") // WebSocket integrations only have 1.0
	if in.IntegrationType == "AWS_PROXY" {
		in.IntegrationMethod = cmp.Or(in.IntegrationMethod, "POST")
	}
	in.ConnectionType = cmp.Or(in.ConnectionType, "INTERNET")
	in.TimeoutInMillis = cmp.Or(in.TimeoutInMillis, 30000)
	return nil
}

func checkAuthorizer(_ *httpAPI, au *HTTPAuthorizer) error {
	switch {
	case au.Name == "":
		return badRequest("Name is required")
	case au.AuthorizerResultTtlInSeconds < 0 || au.AuthorizerResultTtlInSeconds > 3600:
		return badRequest("AuthorizerResultTtlInSeconds must be between 0 and 3600")
	case au.AuthorizerType == "JWT" && au.JwtConfiguration == nil:
		return badRequest("JwtConfiguration is required for JWT authorizers")
	case au.AuthorizerType == "JWT" && len(au.IdentitySource) == 0:
		au.IdentitySource = []string{"$request.header.Authorization"}
	case au.AuthorizerType == "REQUEST" && au.AuthorizerURI == "":
		return badRequest("AuthorizerUri is required for REQUEST authorizers")
	case au.AuthorizerType != "JWT" && au.AuthorizerType != "REQUEST":
		return badRequest("Invalid AuthorizerType %q: use JWT or REQUEST", au.AuthorizerType)
	}
	return nil
}
