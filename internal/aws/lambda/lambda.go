// Package lambda emulates AWS Lambda (REST-JSON): functions, versions,
// aliases, layers, permissions, function URLs and event source mappings.
// Python and Node.js handlers run as warm local processes behind an embedded
// Runtime API client; other runtimes and container images run in Docker.
package lambda

import (
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io/fs"
	"maps"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Lambda emulator.
type Service struct {
	hub   *core.Hub
	mux   *http.ServeMux
	pool  *pool
	mu    sync.Mutex
	st    state
	busy  map[string]int // in-flight invocations per function, and per account/region of unreserved ones; under mu
	logMu sync.Mutex
	logs  map[string][]string // account/region/name → recent log lines
	esm   *pollers
	stop  func()
}

type state struct {
	Functions map[string]*Function `json:"functions"` // account/region/name
	Mappings  map[string]*Mapping  `json:"mappings"`  // UUID
	Layers    map[string]*Layer    `json:"layers"`    // account/region/name
}

// Function is a function with its $LATEST config, versions and aliases.
type Function struct {
	Name        string                  `json:"name"`
	Account     string                  `json:"account"`
	Region      string                  `json:"region"`
	Latest      *Config                 `json:"latest"`
	Versions    map[string]*Config      `json:"versions,omitempty"`
	NextVersion int                     `json:"nextVersion"`
	Aliases     map[string]*Alias       `json:"aliases,omitempty"`
	Policy      []map[string]any        `json:"policy,omitempty"`
	Tags        map[string]string       `json:"tags,omitempty"`
	Reserved    *int                    `json:"reserved,omitempty"`
	Provisioned map[string]*Provisioned `json:"provisioned,omitempty"` // version or alias → provisioned concurrency
	URL         *URLConfig              `json:"url,omitempty"`
	EventInvoke map[string]any          `json:"eventInvoke,omitempty"`
}

// Config is one immutable (or $LATEST) function configuration.
type Config struct {
	Version       string            `json:"version"`
	Runtime       string            `json:"runtime,omitempty"`
	Handler       string            `json:"handler,omitempty"`
	Role          string            `json:"role"`
	Description   string            `json:"description,omitempty"`
	Timeout       int               `json:"timeout"`
	MemorySize    int               `json:"memorySize"`
	Env           map[string]string `json:"env,omitempty"`
	PackageType   string            `json:"packageType"`
	ImageURI      string            `json:"imageUri,omitempty"`
	ImageConfig   *ImageConfig      `json:"imageConfig,omitempty"`
	Code          []byte            `json:"code,omitempty"`
	CodeDir       string            `json:"codeDir,omitempty"` // hot reload
	CodeSha256    string            `json:"codeSha256"`
	CodeSize      int64             `json:"codeSize"`
	Layers        []string          `json:"layers,omitempty"`
	Architectures []string          `json:"architectures,omitempty"`
	DeadLetterArn string            `json:"deadLetterArn,omitempty"`
	TracingMode   string            `json:"tracingMode,omitempty"`
	Ephemeral     int               `json:"ephemeral,omitempty"`
	Vpc           json.RawMessage   `json:"vpc,omitempty"`
	Modified      time.Time         `json:"modified"`
	RevisionID    string            `json:"revisionId"`
}

// ImageConfig overrides a container image's entrypoint, command and working directory.
type ImageConfig struct {
	EntryPoint       []string `json:",omitempty"`
	Command          []string `json:",omitempty"`
	WorkingDirectory string   `json:",omitempty"`
}

// Alias points at a version, optionally splitting traffic.
type Alias struct {
	Name        string             `json:"name"`
	Version     string             `json:"version"`
	Description string             `json:"description,omitempty"`
	Weights     map[string]float64 `json:"weights,omitempty"`
	RevisionID  string             `json:"revisionId"`
}

// URLConfig is a function URL.
type URLConfig struct {
	ID       string          `json:"id"`
	AuthType string          `json:"authType"`
	Cors     json.RawMessage `json:"cors,omitempty"`
	Mode     string          `json:"mode,omitempty"`
	Created  time.Time       `json:"created"`
}

// Layer holds the versions of one layer.
type Layer struct {
	Name     string                `json:"name"`
	Account  string                `json:"account"`
	Region   string                `json:"region"`
	Versions map[int]*LayerVersion `json:"versions"`
	Next     int                   `json:"next"`
}

// LayerVersion is one published layer version.
type LayerVersion struct {
	Version     int       `json:"version"`
	Zip         []byte    `json:"zip"`
	Sha         string    `json:"sha"`
	Description string    `json:"description,omitempty"`
	License     string    `json:"license,omitempty"`
	Runtimes    []string  `json:"runtimes,omitempty"`
	Archs       []string  `json:"archs,omitempty"`
	Created     time.Time `json:"created"`
}

// New creates the Lambda service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), logs: map[string][]string{}, busy: map[string]int{}}
	s.st = newState()
	s.pool = newPool(s)
	s.routes()
	if hub != nil {
		hub.OnInvoke(s.invokeSync)
		hub.OnInvokeFrom(s.invokeFrom)
		hub.OnDeliver("lambda", s.deliver)
		hub.Mount("/_aws/lambda/code/", http.HandlerFunc(s.serveCode))
	}
	s.esm = startPollers(s)
	s.stop = core.Every(5*time.Second, s.Tick)
	return s
}

func newState() state {
	return state{Functions: map[string]*Function{}, Mappings: map[string]*Mapping{}, Layers: map[string]*Layer{}}
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "lambda", Cloud: core.AWS, Title: "Lambda", Signing: []string{"lambda"}, Protocol: "rest-json", Match: isURL}
}

// isURL recognises function URL traffic: <url-id>.lambda-url[.<region>].localhost.
func isURL(r *http.Request) bool {
	_, base := core.SplitHost(r.Host, "")
	return strings.HasPrefix(base, "lambda-url.")
}

// BodyLimit raises the upload limit for function code: CreateFunction, UpdateFunctionCode and
// PublishLayerVersion carry a zip of up to 50 MB as base64 in JSON.
func (s *Service) BodyLimit(r *http.Request) int64 {
	p := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case r.Method == http.MethodPost && p == "/2015-03-31/functions",
		r.Method == http.MethodPut && strings.HasPrefix(p, "/2015-03-31/functions/") && strings.HasSuffix(p, "/code"),
		r.Method == http.MethodPost && strings.HasPrefix(p, "/2018-10-31/layers/") && strings.HasSuffix(p, "/versions"):
		return s.hub.CodeLimit(true)
	}
	return 0
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if isURL(r) {
		s.serveURL(w, r)
		return
	}
	s.mux.ServeHTTP(w, r)
}

// Close stops every runtime process and poller.
func (s *Service) Close() error {
	s.stop()
	s.esm.stop()
	s.pool.close()
	return nil
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
	s.pool.invalidate("")
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
	s.pool.invalidate("")
	return nil
}

type handler func(w http.ResponseWriter, r *http.Request, c *core.Ctx) error

func (s *Service) route(pattern, op string, h handler) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		if err := c.Intercept(); err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		if err := h(w, r, c); err != nil {
			var t *throttleError
			if errors.As(err, &t) {
				t.write(w, c)
				return
			}
			core.RESTJSONError(w, c, err)
		}
	})
}

func (s *Service) routes() {
	const fn = "/2015-03-31/functions/{name}"
	s.route("POST /2015-03-31/functions", "CreateFunction", s.createFunction)
	s.route("POST /2015-03-31/functions/{$}", "CreateFunction", s.createFunction)
	s.route("GET /2015-03-31/functions", "ListFunctions", s.listFunctions)
	s.route("GET /2015-03-31/functions/{$}", "ListFunctions", s.listFunctions)
	s.route("GET "+fn, "GetFunction", s.getFunction)
	s.route("DELETE "+fn, "DeleteFunction", s.deleteFunction)
	s.route("GET "+fn+"/configuration", "GetFunctionConfiguration", s.getConfiguration)
	s.route("PUT "+fn+"/configuration", "UpdateFunctionConfiguration", s.updateConfiguration)
	s.route("PUT "+fn+"/code", "UpdateFunctionCode", s.updateCode)
	s.route("POST "+fn+"/invocations", "Invoke", s.invoke)
	s.route("POST /2014-11-13/functions/{name}/invoke-async/", "InvokeAsync", s.invokeAsyncLegacy)
	s.route("POST "+fn+"/versions", "PublishVersion", s.publishVersion)
	s.route("GET "+fn+"/versions", "ListVersionsByFunction", s.listVersions)
	s.route("POST "+fn+"/aliases", "CreateAlias", s.createAlias)
	s.route("GET "+fn+"/aliases", "ListAliases", s.listAliases)
	s.route("GET "+fn+"/aliases/{alias}", "GetAlias", s.getAlias)
	s.route("PUT "+fn+"/aliases/{alias}", "UpdateAlias", s.updateAlias)
	s.route("DELETE "+fn+"/aliases/{alias}", "DeleteAlias", s.deleteAlias)
	s.route("POST "+fn+"/policy", "AddPermission", s.addPermission)
	s.route("GET "+fn+"/policy", "GetPolicy", s.getPolicy)
	s.route("DELETE "+fn+"/policy/{sid}", "RemovePermission", s.removePermission)
	s.route("PUT /2017-10-31/functions/{name}/concurrency", "PutFunctionConcurrency", s.putConcurrency)
	s.route("GET /2019-09-30/functions/{name}/concurrency", "GetFunctionConcurrency", s.getConcurrency)
	s.route("DELETE /2017-10-31/functions/{name}/concurrency", "DeleteFunctionConcurrency", s.deleteConcurrency)
	s.route("PUT /2019-09-30/functions/{name}/provisioned-concurrency", "PutProvisionedConcurrencyConfig", s.putProvisioned)
	s.route("GET /2019-09-30/functions/{name}/provisioned-concurrency", "GetProvisionedConcurrencyConfig", s.getProvisioned) // and List (?List=ALL)
	s.route("DELETE /2019-09-30/functions/{name}/provisioned-concurrency", "DeleteProvisionedConcurrencyConfig", s.deleteProvisioned)
	s.route("POST /2021-10-31/functions/{name}/url", "CreateFunctionUrlConfig", s.putURL(true))
	s.route("PUT /2021-10-31/functions/{name}/url", "UpdateFunctionUrlConfig", s.putURL(false))
	s.route("GET /2021-10-31/functions/{name}/url", "GetFunctionUrlConfig", s.getURL)
	s.route("DELETE /2021-10-31/functions/{name}/url", "DeleteFunctionUrlConfig", s.deleteURL)
	s.route("GET /2021-10-31/functions/{name}/urls", "ListFunctionUrlConfigs", s.listURLs)
	for _, m := range []string{"PUT", "POST"} {
		s.route(m+" /2019-09-25/functions/{name}/event-invoke-config", m+"FunctionEventInvokeConfig", s.putEventInvoke)
	}
	s.route("GET /2019-09-25/functions/{name}/event-invoke-config", "GetFunctionEventInvokeConfig", s.getEventInvoke)
	s.route("DELETE /2019-09-25/functions/{name}/event-invoke-config", "DeleteFunctionEventInvokeConfig", s.deleteEventInvoke)
	s.route("GET /2019-09-25/functions/{name}/event-invoke-config/list", "ListFunctionEventInvokeConfigs", s.listEventInvokes)
	s.route("GET /2020-06-30/functions/{name}/code-signing-config", "GetFunctionCodeSigningConfig", s.static(func(name string) any {
		return map[string]string{"CodeSigningConfigArn": "", "FunctionName": name}
	}))
	s.route("GET /2021-07-20/functions/{name}/runtime-management-config", "GetRuntimeManagementConfig", s.static(func(string) any {
		return map[string]string{"UpdateRuntimeOn": "Auto"}
	}))
	s.route("GET /2024-08-31/functions/{name}/recursion-config", "GetFunctionRecursionConfig", s.static(func(string) any {
		return map[string]string{"RecursiveLoop": "Terminate"}
	}))
	s.route("GET /2017-03-31/tags/{arn}", "ListTags", s.listTags)
	s.route("POST /2017-03-31/tags/{arn}", "TagResource", s.tagResource)
	s.route("DELETE /2017-03-31/tags/{arn}", "UntagResource", s.untagResource)
	s.route("GET /2016-08-19/account-settings/", "GetAccountSettings", s.accountSettings)
	s.route("GET /2016-08-19/account-settings", "GetAccountSettings", s.accountSettings)
	s.route("POST /2018-10-31/layers/{layer}/versions", "PublishLayerVersion", s.publishLayer)
	s.route("GET /2018-10-31/layers/{layer}/versions", "ListLayerVersions", s.listLayerVersions)
	s.route("GET /2018-10-31/layers/{layer}/versions/{n}", "GetLayerVersion", s.getLayerVersion)
	s.route("DELETE /2018-10-31/layers/{layer}/versions/{n}", "DeleteLayerVersion", s.deleteLayerVersion)
	s.route("POST /2018-10-31/layers/{layer}/versions/{n}/policy", "AddLayerVersionPermission", s.layerPermission)
	s.route("GET /2018-10-31/layers/{layer}/versions/{n}/policy", "GetLayerVersionPolicy", s.layerPolicy)
	s.route("GET /2018-10-31/layers", "ListLayers", s.listLayers)
	s.route("POST /2015-03-31/event-source-mappings/", "CreateEventSourceMapping", s.createMapping)
	s.route("POST /2015-03-31/event-source-mappings", "CreateEventSourceMapping", s.createMapping)
	s.route("GET /2015-03-31/event-source-mappings/", "ListEventSourceMappings", s.listMappings)
	s.route("GET /2015-03-31/event-source-mappings", "ListEventSourceMappings", s.listMappings)
	s.route("GET /2015-03-31/event-source-mappings/{uuid}", "GetEventSourceMapping", s.getMapping)
	s.route("PUT /2015-03-31/event-source-mappings/{uuid}", "UpdateEventSourceMapping", s.updateMapping)
	s.route("DELETE /2015-03-31/event-source-mappings/{uuid}", "DeleteEventSourceMapping", s.deleteMapping)
	s.route("/", "Unknown", func(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
		return core.Errorf(501, "NotImplemented", "pogi does not implement Lambda %s %s", r.Method, r.URL.Path)
	})
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 {
		if err := json.Unmarshal(b, v); err != nil {
			return core.Errorf(400, "InvalidRequestContentException", "Could not parse request body into json: %v", err)
		}
	}
	return nil
}

func errNoFunction(arn string) error {
	return core.Errorf(404, "ResourceNotFoundException", "Function not found: %s", arn)
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterValueException", format, a...)
}

// splitName accepts a name, partial ARN or ARN and returns the parts it names.
func splitName(ref string) (name, qualifier, region, account string) {
	p := strings.Split(ref, ":")
	switch {
	case strings.HasPrefix(ref, "arn:") && len(p) >= 7:
		name, region, account = p[6], p[3], p[4]
		if len(p) >= 8 {
			qualifier = p[7]
		}
	case len(p) == 2:
		name, qualifier = p[0], p[1]
	case len(p) >= 3: // 123456789012:function:name[:qualifier]
		name, account = p[2], p[0]
		if len(p) >= 4 {
			qualifier = p[3]
		}
	default:
		name = ref
	}
	return
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func (f *Function) arn() string { return core.ARN("lambda", f.Region, f.Account, "function:"+f.Name) }

// function finds a function by name or ARN; the lock must be held.
func (s *Service) function(c *core.Ctx, ref string) (*Function, string, error) {
	name, q, region, account := splitName(ref)
	if region == "" {
		region = c.Region
	}
	if account == "" {
		account = c.Account
	}
	if f := s.st.Functions[key(account, region, name)]; f != nil {
		return f, q, nil
	}
	return nil, q, errNoFunction(core.ARN("lambda", region, account, "function:"+name))
}

// qualify joins the qualifier named in a function reference ("f:1") with the Qualifier
// parameter. When both are given they must agree.
func qualify(fromName, param string) (string, error) {
	if fromName != "" && param != "" && fromName != param {
		return "", invalid("The derived qualifier from the function name does not match the specified qualifier.")
	}
	return cmp.Or(param, fromName), nil
}

// resolve picks the config a qualifier (version, alias or $LATEST) refers to.
func (f *Function) resolve(q string) (*Config, string, error) {
	switch {
	case q == "" || q == "$LATEST":
		return f.Latest, "$LATEST", nil
	case f.Versions[q] != nil:
		return f.Versions[q], q, nil
	case f.Aliases[q] != nil:
		a := f.Aliases[q]
		v, r := a.Version, rand.Float64()
		for ver, w := range a.Weights {
			if r < w {
				v = ver
				break
			}
			r -= w
		}
		if v == "$LATEST" {
			return f.Latest, v, nil
		}
		if cfg := f.Versions[v]; cfg != nil {
			return cfg, v, nil
		}
	}
	return nil, "", errNoFunction(f.arn() + ":" + q)
}

type codeInput struct {
	ZipFile         []byte
	S3Bucket        string
	S3Key           string
	S3ObjectVersion string
	ImageUri        string
}

// loadCode resolves function code from a zip, S3 or a hot-reload directory.
func (s *Service) loadCode(code codeInput) (cfg Config, err error) {
	marker := os.Getenv("BUCKET_MARKER_LOCAL")
	if marker == "" {
		marker = "hot-reload"
	}
	switch {
	case code.ImageUri != "":
		cfg.PackageType, cfg.ImageURI = "Image", code.ImageUri
		h := sha256.Sum256([]byte(code.ImageUri))
		cfg.CodeSha256 = base64.StdEncoding.EncodeToString(h[:])
		return cfg, nil
	case code.S3Bucket == marker:
		st, err := os.Stat(code.S3Key)
		if err != nil || !st.IsDir() || !filepath.IsAbs(code.S3Key) {
			return cfg, invalid("hot-reload S3Key must be an absolute local directory, got %q", code.S3Key)
		}
		cfg.CodeDir = code.S3Key
		h := sha256.Sum256([]byte(code.S3Key))
		cfg.CodeSha256 = base64.StdEncoding.EncodeToString(h[:])
	case len(code.ZipFile) > 0:
		cfg.Code = code.ZipFile
	case code.S3Bucket != "":
		s3svc, _ := s.hub.Service("s3").(*s3.Service)
		if s3svc == nil {
			return cfg, invalid("S3 is not enabled, cannot read code from s3://%s/%s", code.S3Bucket, code.S3Key)
		}
		o, err := s3svc.Get(code.S3Bucket, code.S3Key)
		if err != nil {
			return cfg, invalid("Error occurred while GetObject. S3 Error Code: NoSuchKey. S3 Error Message: %v", err)
		}
		cfg.Code = o.Data
	default:
		return cfg, invalid("Please provide a source for function code.")
	}
	cfg.PackageType = "Zip"
	if cfg.Code != nil {
		h := sha256.Sum256(cfg.Code)
		cfg.CodeSha256 = base64.StdEncoding.EncodeToString(h[:])
		cfg.CodeSize = int64(len(cfg.Code))
	}
	return cfg, nil
}

type configInput struct {
	Role             *string
	Handler          *string
	Description      *string
	Timeout          *int
	MemorySize       *int
	Runtime          *string
	Environment      *struct{ Variables map[string]string }
	Layers           *[]string
	DeadLetterConfig *struct{ TargetArn string }
	TracingConfig    *struct{ Mode string }
	EphemeralStorage *struct{ Size int }
	VpcConfig        json.RawMessage
	ImageConfig      *ImageConfig
	RevisionId       string
}

func (in *configInput) apply(cfg *Config) {
	set := func(dst *string, v *string) {
		if v != nil {
			*dst = *v
		}
	}
	set(&cfg.Role, in.Role)
	set(&cfg.Handler, in.Handler)
	set(&cfg.Description, in.Description)
	set(&cfg.Runtime, in.Runtime)
	if in.Timeout != nil {
		cfg.Timeout = *in.Timeout
	}
	if in.MemorySize != nil {
		cfg.MemorySize = *in.MemorySize
	}
	if in.Environment != nil {
		cfg.Env = in.Environment.Variables
	}
	if in.Layers != nil {
		cfg.Layers = *in.Layers
	}
	if in.DeadLetterConfig != nil {
		cfg.DeadLetterArn = in.DeadLetterConfig.TargetArn
	}
	if in.TracingConfig != nil {
		cfg.TracingMode = in.TracingConfig.Mode
	}
	if in.EphemeralStorage != nil {
		cfg.Ephemeral = in.EphemeralStorage.Size
	}
	if len(in.VpcConfig) > 0 {
		cfg.Vpc = in.VpcConfig
	}
	if in.ImageConfig != nil {
		cfg.ImageConfig = in.ImageConfig
	}
}

var fnName = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

func (s *Service) createFunction(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct {
		configInput
		FunctionName  string
		Code          codeInput
		PackageType   string
		Publish       bool
		Tags          map[string]string
		Architectures []string
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	if !fnName.MatchString(in.FunctionName) {
		return invalid("1 validation error detected: Value '%s' at 'functionName' failed to satisfy constraint", in.FunctionName)
	}
	cfg, err := s.loadCode(in.Code)
	if err != nil {
		return err
	}
	if in.PackageType != "" && in.PackageType != cfg.PackageType {
		return invalid("PackageType is %s but the code is a %s package.", in.PackageType, cfg.PackageType)
	}
	cfg.Version, cfg.Timeout, cfg.MemorySize, cfg.Architectures = "$LATEST", 3, 128, in.Architectures
	in.apply(&cfg)
	if cfg.PackageType == "Zip" && (cfg.Runtime == "" || cfg.Handler == "") {
		return invalid("Runtime and Handler are mandatory parameters for functions created with Zip packages.")
	}
	cfg.Modified, cfg.RevisionID = time.Now().UTC(), core.UUID()
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.missingLayer(c, cfg.Layers); err != nil {
		return err
	}
	k := key(c.Account, c.Region, in.FunctionName)
	if s.st.Functions[k] != nil {
		return core.Errorf(409, "ResourceConflictException", "Function already exist: %s", in.FunctionName)
	}
	f := &Function{Name: in.FunctionName, Account: c.Account, Region: c.Region, Latest: &cfg, Tags: in.Tags, NextVersion: 1}
	s.st.Functions[k] = f
	out := s.configJSON(f, &cfg)
	if in.Publish {
		out = s.configJSON(f, f.publish(""))
	}
	core.RESTJSON(w, c, 201, out)
	return nil
}

// publish snapshots $LATEST as a new version (or returns the last one if unchanged).
func (f *Function) publish(desc string) *Config {
	if f.Versions == nil {
		f.Versions = map[string]*Config{}
	}
	if last := f.Versions[strconv.Itoa(f.NextVersion-1)]; last != nil && last.CodeSha256 == f.Latest.CodeSha256 && last.RevisionID == f.Latest.RevisionID {
		return last
	}
	v := *f.Latest
	v.Version = strconv.Itoa(f.NextVersion)
	if desc != "" {
		v.Description = desc
	}
	f.Versions[v.Version] = &v
	f.NextVersion++
	return &v
}

func (s *Service) configJSON(f *Function, cfg *Config) map[string]any {
	arn := f.arn()
	if cfg.Version != "$LATEST" {
		arn += ":" + cfg.Version
	}
	out := map[string]any{
		"FunctionName": f.Name, "FunctionArn": arn, "Role": cfg.Role, "CodeSize": cfg.CodeSize, "Description": cfg.Description,
		"Timeout": cfg.Timeout, "MemorySize": cfg.MemorySize, "LastModified": cfg.Modified.Format("2006-01-02T15:04:05.000-0700"),
		"CodeSha256": cfg.CodeSha256, "Version": cfg.Version, "RevisionId": cfg.RevisionID, "State": "Active",
		"LastUpdateStatus": "Successful", "PackageType": cfg.PackageType, "TracingConfig": map[string]string{"Mode": or(cfg.TracingMode, "PassThrough")},
		"Architectures": orList(cfg.Architectures, []string{"x86_64"}), "EphemeralStorage": map[string]int{"Size": max(cfg.Ephemeral, 512)},
		"SnapStart":     map[string]string{"ApplyOn": "None", "OptimizationStatus": "Off"},
		"LoggingConfig": map[string]string{"LogFormat": "Text", "LogGroup": "/aws/lambda/" + f.Name},
	}
	if cfg.PackageType == "Zip" {
		out["Runtime"], out["Handler"] = cfg.Runtime, cfg.Handler
	}
	if ic := cfg.ImageConfig; ic != nil && (len(ic.EntryPoint) > 0 || len(ic.Command) > 0 || ic.WorkingDirectory != "") {
		out["ImageConfigResponse"] = map[string]any{"ImageConfig": ic}
	}
	if len(cfg.Env) > 0 {
		out["Environment"] = map[string]any{"Variables": cfg.Env}
	}
	if len(cfg.Layers) > 0 {
		var ls []map[string]any
		for _, l := range cfg.Layers {
			ls = append(ls, map[string]any{"Arn": l, "CodeSize": len(s.layerZip(l))})
		}
		out["Layers"] = ls
	}
	if cfg.DeadLetterArn != "" {
		out["DeadLetterConfig"] = map[string]string{"TargetArn": cfg.DeadLetterArn}
	}
	if len(cfg.Vpc) > 0 {
		out["VpcConfig"] = cfg.Vpc
	}
	return out
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

func orList(s, def []string) []string {
	if len(s) == 0 {
		return def
	}
	return s
}

func (s *Service) getFunction(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, q, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	if q, err = qualify(q, r.URL.Query().Get("Qualifier")); err != nil {
		return err
	}
	cfg, _, err := f.resolve(q)
	if err != nil {
		return err
	}
	code := map[string]string{"RepositoryType": "S3", "Location": "http://" + c.Host + "/_aws/lambda/code/" + f.Account + "/" + f.Region + "/" + f.Name + ".zip"}
	if cfg.PackageType == "Image" {
		code = map[string]string{"RepositoryType": "ECR", "ImageUri": cfg.ImageURI, "ResolvedImageUri": cfg.ImageURI}
	}
	out := map[string]any{"Configuration": s.configJSON(f, cfg), "Code": code, "Tags": orMap(f.Tags)}
	if f.Reserved != nil {
		out["Concurrency"] = map[string]int{"ReservedConcurrentExecutions": *f.Reserved}
	}
	core.RESTJSON(w, c, 200, out)
	return nil
}

func orMap(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func (s *Service) getConfiguration(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, q, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	if q, err = qualify(q, r.URL.Query().Get("Qualifier")); err != nil {
		return err
	}
	cfg, _, err := f.resolve(q)
	if err != nil {
		return err
	}
	core.RESTJSON(w, c, 200, s.configJSON(f, cfg))
	return nil
}

func (s *Service) listFunctions(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	q := r.URL.Query()
	all := q.Get("FunctionVersion") == "ALL"
	maxItems, _ := strconv.Atoi(q.Get("MaxItems"))
	if maxItems <= 0 || maxItems > 50 {
		maxItems = 50
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var names []string
	for _, f := range s.st.Functions {
		if f.Account == c.Account && f.Region == c.Region {
			names = append(names, f.Name)
		}
	}
	sort.Strings(names)
	out := map[string]any{}
	var list []any
	for _, n := range names {
		if m := q.Get("Marker"); m != "" && n <= m {
			continue
		}
		if len(list) >= maxItems {
			out["NextMarker"] = names[sort.SearchStrings(names, n)-1]
			break
		}
		f := s.st.Functions[key(c.Account, c.Region, n)]
		list = append(list, s.configJSON(f, f.Latest))
		if all {
			for _, v := range sortedVersions(f) {
				list = append(list, s.configJSON(f, f.Versions[v]))
			}
		}
	}
	if list == nil {
		list = []any{}
	}
	out["Functions"] = list
	core.RESTJSON(w, c, 200, out)
	return nil
}

func sortedVersions(f *Function) []string {
	var vs []string
	for v := range f.Versions {
		vs = append(vs, v)
	}
	sort.Slice(vs, func(i, j int) bool { a, _ := strconv.Atoi(vs[i]); b, _ := strconv.Atoi(vs[j]); return a < b })
	return vs
}

func (s *Service) deleteFunction(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	f, q, err := s.function(c, r.PathValue("name"))
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if q, err = qualify(q, r.URL.Query().Get("Qualifier")); err != nil {
		s.mu.Unlock()
		return err
	}
	k := key(f.Account, f.Region, f.Name)
	if q != "" && q != "$LATEST" {
		delete(f.Versions, q)
		delete(f.Provisioned, q)
	} else {
		delete(s.st.Functions, k)
		for id, m := range s.st.Mappings {
			if m.FunctionArn == f.arn() {
				delete(s.st.Mappings, id)
			}
		}
	}
	s.mu.Unlock()
	s.pool.invalidate(k + ":")
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) updateConfiguration(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in configInput
	if err := decode(r, &in); err != nil {
		return err
	}
	s.mu.Lock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if in.RevisionId != "" && in.RevisionId != f.Latest.RevisionID {
		s.mu.Unlock()
		return core.Errorf(412, "PreconditionFailedException", "The Revision Id provided does not match the latest Revision Id.")
	}
	cfg := *f.Latest
	in.apply(&cfg)
	if err := s.missingLayer(c, cfg.Layers); err != nil {
		s.mu.Unlock()
		return err
	}
	cfg.Modified, cfg.RevisionID = time.Now().UTC(), core.UUID()
	f.Latest = &cfg
	out := s.configJSON(f, &cfg)
	k := key(f.Account, f.Region, f.Name)
	s.mu.Unlock()
	s.pool.invalidate(k + ":$LATEST@") // published versions keep their (provisioned) environments
	core.RESTJSON(w, c, 200, out)
	return nil
}

func (s *Service) updateCode(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct {
		codeInput
		Publish       bool
		DryRun        bool
		RevisionId    string
		Architectures []string
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	code, err := s.loadCode(in.codeInput)
	if err != nil {
		return err
	}
	s.mu.Lock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		s.mu.Unlock()
		return err
	}
	if in.RevisionId != "" && in.RevisionId != f.Latest.RevisionID {
		s.mu.Unlock()
		return core.Errorf(412, "PreconditionFailedException", "The Revision Id provided does not match the latest Revision Id.")
	}
	if code.PackageType != f.Latest.PackageType {
		s.mu.Unlock()
		if f.Latest.PackageType == "Image" {
			return invalid("Please provide ImageUri when updating a function with packageType Image.")
		}
		return invalid("Please don't provide ImageUri when updating a function with packageType Zip.")
	}
	cfg := *f.Latest
	cfg.PackageType, cfg.ImageURI, cfg.Code, cfg.CodeDir, cfg.CodeSha256, cfg.CodeSize = code.PackageType, code.ImageURI, code.Code, code.CodeDir, code.CodeSha256, code.CodeSize
	if len(in.Architectures) > 0 {
		cfg.Architectures = in.Architectures
	}
	cfg.Modified, cfg.RevisionID = time.Now().UTC(), core.UUID()
	if !in.DryRun {
		f.Latest = &cfg
	}
	out := s.configJSON(f, &cfg)
	if in.Publish && !in.DryRun {
		out = s.configJSON(f, f.publish(""))
	}
	k := key(f.Account, f.Region, f.Name)
	s.mu.Unlock()
	s.pool.invalidate(k + ":$LATEST@")
	core.RESTJSON(w, c, 200, out)
	return nil
}

func (s *Service) publishVersion(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct {
		Description, CodeSha256, RevisionId string
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	if in.CodeSha256 != "" && in.CodeSha256 != f.Latest.CodeSha256 {
		return invalid("CodeSHA256 (%s) is different from current CodeSHA256 in $LATEST (%s). Please try again with the CodeSHA256 in $LATEST.", in.CodeSha256, f.Latest.CodeSha256)
	}
	core.RESTJSON(w, c, 201, s.configJSON(f, f.publish(in.Description)))
	return nil
}

func (s *Service) listVersions(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	list := []any{s.configJSON(f, f.Latest)}
	for _, v := range sortedVersions(f) {
		list = append(list, s.configJSON(f, f.Versions[v]))
	}
	core.RESTJSON(w, c, 200, map[string]any{"Versions": list})
	return nil
}

// ---- aliases ----

type aliasInput struct {
	Name            string
	FunctionVersion string
	Description     *string
	RoutingConfig   *struct{ AdditionalVersionWeights map[string]float64 }
	RevisionId      string
}

func aliasJSON(f *Function, a *Alias) map[string]any {
	out := map[string]any{"AliasArn": f.arn() + ":" + a.Name, "Name": a.Name, "FunctionVersion": a.Version, "Description": a.Description, "RevisionId": a.RevisionID}
	if len(a.Weights) > 0 {
		out["RoutingConfig"] = map[string]any{"AdditionalVersionWeights": a.Weights}
	}
	return out
}

func (s *Service) createAlias(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in aliasInput
	if err := decode(r, &in); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	if f.Aliases[in.Name] != nil {
		return core.Errorf(409, "ResourceConflictException", "Alias already exists: %s:%s", f.arn(), in.Name)
	}
	if in.FunctionVersion != "$LATEST" && f.Versions[in.FunctionVersion] == nil {
		return errNoFunction(f.arn() + ":" + in.FunctionVersion)
	}
	a := &Alias{Name: in.Name, Version: in.FunctionVersion, RevisionID: core.UUID()}
	if in.Description != nil {
		a.Description = *in.Description
	}
	if in.RoutingConfig != nil {
		a.Weights = in.RoutingConfig.AdditionalVersionWeights
	}
	if f.Aliases == nil {
		f.Aliases = map[string]*Alias{}
	}
	f.Aliases[a.Name] = a
	core.RESTJSON(w, c, 201, aliasJSON(f, a))
	return nil
}

func (s *Service) withAlias(w http.ResponseWriter, r *http.Request, c *core.Ctx, fn func(f *Function, a *Alias) (any, int, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	a := f.Aliases[r.PathValue("alias")]
	if a == nil {
		return core.Errorf(404, "ResourceNotFoundException", "Alias not found: %s:%s", f.arn(), r.PathValue("alias"))
	}
	out, status, err := fn(f, a)
	if err != nil {
		return err
	}
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return nil
	}
	core.RESTJSON(w, c, status, out)
	return nil
}

func (s *Service) getAlias(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withAlias(w, r, c, func(f *Function, a *Alias) (any, int, error) { return aliasJSON(f, a), 200, nil })
}

func (s *Service) updateAlias(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in aliasInput
	if err := decode(r, &in); err != nil {
		return err
	}
	return s.withAlias(w, r, c, func(f *Function, a *Alias) (any, int, error) {
		if in.FunctionVersion != "" {
			a.Version = in.FunctionVersion
		}
		if in.Description != nil {
			a.Description = *in.Description
		}
		if in.RoutingConfig != nil {
			a.Weights = in.RoutingConfig.AdditionalVersionWeights
		}
		a.RevisionID = core.UUID()
		return aliasJSON(f, a), 200, nil
	})
}

func (s *Service) deleteAlias(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withAlias(w, r, c, func(f *Function, a *Alias) (any, int, error) {
		delete(f.Aliases, a.Name)
		delete(f.Provisioned, a.Name)
		return nil, http.StatusNoContent, nil
	})
}

func (s *Service) listAliases(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	list := []any{}
	for _, n := range core.SortedKeys(f.Aliases) {
		if v := r.URL.Query().Get("FunctionVersion"); v == "" || v == f.Aliases[n].Version {
			list = append(list, aliasJSON(f, f.Aliases[n]))
		}
	}
	core.RESTJSON(w, c, 200, map[string]any{"Aliases": list})
	return nil
}

// ---- resource policy ----

// policyScope finds the function and the Resource its statements are stored under:
// the function, or a version or alias of it when a Qualifier is given.
func (s *Service) policyScope(r *http.Request, c *core.Ctx) (*Function, string, error) {
	f, q, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return nil, "", err
	}
	if q, err = qualify(q, r.URL.Query().Get("Qualifier")); err != nil {
		return nil, "", err
	}
	if q == "" {
		return f, f.arn(), nil
	}
	if _, _, err := f.resolve(q); err != nil {
		return nil, "", err
	}
	return f, f.arn() + ":" + q, nil
}

// scoped lists the statements stored under one Resource.
func scoped(f *Function, resource string) []map[string]any {
	var out []map[string]any
	for _, st := range f.Policy {
		if st["Resource"] == resource {
			out = append(out, st)
		}
	}
	return out
}

func (s *Service) addPermission(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct {
		StatementId, Action, Principal, SourceArn, SourceAccount, PrincipalOrgID, FunctionUrlAuthType string
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f, resource, err := s.policyScope(r, c)
	if err != nil {
		return err
	}
	for _, st := range scoped(f, resource) {
		if st["Sid"] == in.StatementId {
			return core.Errorf(409, "ResourceConflictException", "The statement id (%s) provided already exists. Please provide a new statement id, or remove the existing statement.", in.StatementId)
		}
	}
	principal := map[string]string{"Service": in.Principal}
	if !strings.Contains(in.Principal, ".amazonaws.com") {
		principal = map[string]string{"AWS": in.Principal}
		if len(in.Principal) == 12 && strings.Trim(in.Principal, "0123456789") == "" { // an account id is stored as its root ARN
			principal["AWS"] = "arn:aws:iam::" + in.Principal + ":root"
		}
		if in.Principal == "*" {
			principal = nil
		}
	}
	st := map[string]any{"Sid": in.StatementId, "Effect": "Allow", "Action": in.Action, "Resource": resource}
	if principal == nil {
		st["Principal"] = "*"
	} else {
		st["Principal"] = principal
	}
	cond := map[string]any{}
	if in.SourceArn != "" {
		cond["ArnLike"] = map[string]string{"AWS:SourceArn": in.SourceArn}
	}
	equals := map[string]string{}
	if in.SourceAccount != "" {
		equals["AWS:SourceAccount"] = in.SourceAccount
	}
	if in.PrincipalOrgID != "" {
		equals["aws:PrincipalOrgID"] = in.PrincipalOrgID
	}
	if len(equals) > 0 {
		cond["StringEquals"] = equals
	}
	if len(cond) > 0 {
		st["Condition"] = cond
	}
	f.Policy = append(f.Policy, st)
	b, _ := json.Marshal(st)
	core.RESTJSON(w, c, 201, map[string]string{"Statement": string(b)})
	return nil
}

func (s *Service) getPolicy(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, resource, err := s.policyScope(r, c)
	if err != nil {
		return err
	}
	list := scoped(f, resource)
	if len(list) == 0 {
		return core.Errorf(404, "ResourceNotFoundException", "The resource you requested does not exist.")
	}
	b, _ := json.Marshal(map[string]any{"Version": "2012-10-17", "Id": "default", "Statement": list})
	core.RESTJSON(w, c, 200, map[string]string{"Policy": string(b), "RevisionId": f.Latest.RevisionID})
	return nil
}

func (s *Service) removePermission(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, resource, err := s.policyScope(r, c)
	if err != nil {
		return err
	}
	for i, st := range f.Policy {
		if st["Resource"] == resource && st["Sid"] == r.PathValue("sid") {
			f.Policy = append(f.Policy[:i], f.Policy[i+1:]...)
			w.WriteHeader(http.StatusNoContent)
			return nil
		}
	}
	return core.Errorf(404, "ResourceNotFoundException", "Statement %s is not found in resource policy.", r.PathValue("sid"))
}

// ---- concurrency, URLs, event invoke config, tags, account ----

func (s *Service) withFunction(w http.ResponseWriter, r *http.Request, c *core.Ctx, status int, fn func(f *Function) (any, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	f, _, err := s.function(c, r.PathValue("name"))
	if err != nil {
		return err
	}
	out, err := fn(f)
	if err != nil {
		return err
	}
	if status == http.StatusNoContent {
		w.WriteHeader(status)
		return nil
	}
	core.RESTJSON(w, c, status, out)
	return nil
}

func (s *Service) static(fn func(name string) any) handler {
	return func(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
		return s.withFunction(w, r, c, 200, func(f *Function) (any, error) { return fn(f.Name), nil })
	}
}

func (s *Service) putConcurrency(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct{ ReservedConcurrentExecutions int }
	if err := decode(r, &in); err != nil {
		return err
	}
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		free := s.unreserved(f.Account, f.Region)
		if f.Reserved != nil {
			free += *f.Reserved
		}
		switch n := in.ReservedConcurrentExecutions; {
		case n < 0:
			return nil, invalid("1 validation error detected: Value '%d' at 'reservedConcurrentExecutions' failed to satisfy constraint: Member must have value greater than or equal to 0", n)
		case free-n < minUnreserved:
			return nil, invalid("Specified ReservedConcurrentExecutions for function decreases account's UnreservedConcurrentExecution below its minimum value of [%d].", minUnreserved)
		}
		f.Reserved = &in.ReservedConcurrentExecutions
		return in, nil
	})
}

func (s *Service) getConcurrency(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		if f.Reserved == nil {
			return map[string]any{}, nil
		}
		return map[string]int{"ReservedConcurrentExecutions": *f.Reserved}, nil
	})
}

func (s *Service) deleteConcurrency(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, http.StatusNoContent, func(f *Function) (any, error) { f.Reserved = nil; return nil, nil })
}

func urlJSON(f *Function, host string) map[string]any {
	port := "7644"
	if i := strings.LastIndex(host, ":"); i >= 0 {
		port = host[i+1:]
	}
	out := map[string]any{"FunctionUrl": "http://" + f.URL.ID + ".lambda-url." + f.Region + ".localhost:" + port + "/", "FunctionArn": f.arn(),
		"AuthType": f.URL.AuthType, "CreationTime": f.URL.Created.Format(time.RFC3339), "LastModifiedTime": f.URL.Created.Format(time.RFC3339),
		"InvokeMode": or(f.URL.Mode, "BUFFERED")}
	if len(f.URL.Cors) > 0 {
		out["Cors"] = f.URL.Cors
	}
	return out
}

func (s *Service) putURL(create bool) handler {
	return func(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
		var in struct {
			AuthType, InvokeMode string
			Cors                 json.RawMessage
		}
		if err := decode(r, &in); err != nil {
			return err
		}
		status := 200
		if create {
			status = 201
		}
		return s.withFunction(w, r, c, status, func(f *Function) (any, error) {
			if create && f.URL != nil {
				return nil, core.Errorf(409, "ResourceConflictException", "Failed to create function url config for [functionArn = %s]. Error message: FunctionUrlConfig exists for this Lambda function", f.arn())
			}
			if !create && f.URL == nil {
				return nil, core.Errorf(404, "ResourceNotFoundException", "The resource you requested does not exist.")
			}
			if create {
				f.URL = &URLConfig{ID: core.RandString(32, "abcdefghijklmnopqrstuvwxyz0123456789"), Created: time.Now().UTC()}
			}
			if in.AuthType != "" {
				f.URL.AuthType = in.AuthType
			}
			if in.InvokeMode != "" {
				f.URL.Mode = in.InvokeMode
			}
			if len(in.Cors) > 0 {
				f.URL.Cors = in.Cors
			}
			return urlJSON(f, c.Host), nil
		})
	}
}

func (s *Service) getURL(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		if f.URL == nil {
			return nil, core.Errorf(404, "ResourceNotFoundException", "The resource you requested does not exist.")
		}
		return urlJSON(f, c.Host), nil
	})
}

func (s *Service) deleteURL(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, http.StatusNoContent, func(f *Function) (any, error) { f.URL = nil; return nil, nil })
}

func (s *Service) listURLs(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		list := []any{}
		if f.URL != nil {
			list = append(list, urlJSON(f, c.Host))
		}
		return map[string]any{"FunctionUrlConfigs": list}, nil
	})
}

func (s *Service) putEventInvoke(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in map[string]any
	if err := decode(r, &in); err != nil {
		return err
	}
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		if f.EventInvoke == nil || r.Method == http.MethodPut {
			f.EventInvoke = map[string]any{}
		}
		for k, v := range in {
			f.EventInvoke[k] = v
		}
		f.EventInvoke["FunctionArn"] = f.arn()
		f.EventInvoke["LastModified"] = core.Unix(time.Now())
		return f.EventInvoke, nil
	})
}

func (s *Service) getEventInvoke(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		if f.EventInvoke == nil {
			return nil, core.Errorf(404, "ResourceNotFoundException", "The function %s doesn't have an EventInvokeConfig", f.arn())
		}
		return f.EventInvoke, nil
	})
}

// listEventInvokes lists the function's one config (pogi keeps one per function, not one per version or alias).
func (s *Service) listEventInvokes(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, 200, func(f *Function) (any, error) {
		list := []any{}
		if f.EventInvoke != nil {
			list = append(list, f.EventInvoke)
		}
		return map[string]any{"FunctionEventInvokeConfigs": list}, nil
	})
}

func (s *Service) deleteEventInvoke(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return s.withFunction(w, r, c, http.StatusNoContent, func(f *Function) (any, error) { f.EventInvoke = nil; return nil, nil })
}

// tagged finds the tags of a function or event source mapping ARN; lock held.
func (s *Service) tagged(arn string) (*map[string]string, error) {
	if _, _, _, res, ok := core.ParseARN(arn); ok && strings.HasPrefix(res, "event-source-mapping:") {
		if m := s.st.Mappings[strings.TrimPrefix(res, "event-source-mapping:")]; m != nil {
			return &m.Tags, nil
		}
	}
	name, _, region, account := splitName(arn)
	if f := s.st.Functions[key(account, region, name)]; f != nil {
		return &f.Tags, nil
	}
	return nil, errNoFunction(arn)
}

func (s *Service) listTags(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagged(r.PathValue("arn"))
	if err != nil {
		return err
	}
	core.RESTJSON(w, c, 200, map[string]any{"Tags": orMap(*tags)})
	return nil
}

func (s *Service) tagResource(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct{ Tags map[string]string }
	if err := decode(r, &in); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagged(r.PathValue("arn"))
	if err != nil {
		return err
	}
	if *tags == nil {
		*tags = map[string]string{}
	}
	for k, v := range in.Tags {
		(*tags)[k] = v
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) untagResource(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagged(r.PathValue("arn"))
	if err != nil {
		return err
	}
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(*tags, k)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) accountSettings(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	n, size := 0, 0
	for _, f := range s.st.Functions {
		if f.Account == c.Account && f.Region == c.Region {
			n++
			size += int(f.Latest.CodeSize)
		}
	}
	free := s.unreserved(c.Account, c.Region)
	s.mu.Unlock()
	core.RESTJSON(w, c, 200, map[string]any{
		"AccountLimit": map[string]any{"TotalCodeSize": 80530636800, "CodeSizeUnzipped": 262144000, "CodeSizeZipped": 52428800,
			"ConcurrentExecutions": accountLimit, "UnreservedConcurrentExecutions": free},
		"AccountUsage": map[string]any{"TotalCodeSize": size, "FunctionCount": n},
	})
	return nil
}

// serveCode lets GetFunction's Code.Location download the zip.
func (s *Service) serveCode(w http.ResponseWriter, r *http.Request) {
	p := strings.Split(strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/_aws/lambda/code/"), ".zip"), "/")
	if len(p) != 3 {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	f := s.st.Functions[key(p[0], p[1], p[2])]
	var code []byte
	if f != nil {
		code = f.Latest.Code
	}
	s.mu.Unlock()
	if code == nil {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/zip")
	w.Write(code)
}

// ---- layers ----

func (l *Layer) arn() string { return core.ARN("lambda", l.Region, l.Account, "layer:"+l.Name) }

func layerJSON(l *Layer, v *LayerVersion, host string) map[string]any {
	return map[string]any{
		"LayerArn": l.arn(), "LayerVersionArn": l.arn() + ":" + strconv.Itoa(v.Version), "Version": v.Version, "Description": v.Description,
		"CreatedDate": v.Created.Format("2006-01-02T15:04:05.000-0700"), "CompatibleRuntimes": v.Runtimes, "CompatibleArchitectures": v.Archs,
		"LicenseInfo": v.License, "Content": map[string]any{"Location": "http://" + host + "/_aws/lambda/layers/" + l.Name, "CodeSha256": v.Sha, "CodeSize": len(v.Zip)},
	}
}

func (s *Service) publishLayer(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct {
		Description, LicenseInfo                    string
		Content                                     codeInput
		CompatibleRuntimes, CompatibleArchitectures []string
	}
	if err := decode(r, &in); err != nil {
		return err
	}
	code, err := s.loadCode(in.Content)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, r.PathValue("layer"))
	l := s.st.Layers[k]
	if l == nil {
		l = &Layer{Name: r.PathValue("layer"), Account: c.Account, Region: c.Region, Versions: map[int]*LayerVersion{}, Next: 1}
		s.st.Layers[k] = l
	}
	v := &LayerVersion{Version: l.Next, Zip: code.Code, Sha: code.CodeSha256, Description: in.Description, License: in.LicenseInfo,
		Runtimes: in.CompatibleRuntimes, Archs: in.CompatibleArchitectures, Created: time.Now().UTC()}
	l.Versions[v.Version] = v
	l.Next++
	core.RESTJSON(w, c, 201, layerJSON(l, v, c.Host))
	return nil
}

// missingLayer refuses a layer version of the caller's own account and region that does not exist.
// Layers of other accounts (public ones such as Powertools) are taken on trust. The lock must be held.
func (s *Service) missingLayer(c *core.Ctx, arns []string) error {
	for _, a := range arns {
		p := strings.Split(a, ":") // arn:aws:lambda:<region>:<account>:layer:<name>:<version>
		if len(p) != 8 || p[3] != c.Region || p[4] != c.Account {
			continue
		}
		if _, _, err := s.layerVersion(c, p[6], p[7]); err != nil {
			return invalid("Layer version %s does not exist.", a)
		}
	}
	return nil
}

func (s *Service) layerVersion(c *core.Ctx, name, n string) (*Layer, *LayerVersion, error) {
	l := s.st.Layers[key(c.Account, c.Region, name)]
	num, _ := strconv.Atoi(n)
	if l == nil || l.Versions[num] == nil {
		return nil, nil, core.Errorf(404, "ResourceNotFoundException", "The resource you requested does not exist.")
	}
	return l, l.Versions[num], nil
}

func (s *Service) getLayerVersion(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	l, v, err := s.layerVersion(c, r.PathValue("layer"), r.PathValue("n"))
	if err != nil {
		return err
	}
	core.RESTJSON(w, c, 200, layerJSON(l, v, c.Host))
	return nil
}

func (s *Service) deleteLayerVersion(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if l, v, err := s.layerVersion(c, r.PathValue("layer"), r.PathValue("n")); err == nil {
		delete(l.Versions, v.Version)
	}
	w.WriteHeader(http.StatusNoContent)
	return nil
}

func (s *Service) listLayerVersions(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := []any{}
	if l := s.st.Layers[key(c.Account, c.Region, r.PathValue("layer"))]; l != nil {
		for n := l.Next - 1; n > 0; n-- {
			if v := l.Versions[n]; v != nil {
				list = append(list, layerJSON(l, v, c.Host))
			}
		}
	}
	core.RESTJSON(w, c, 200, map[string]any{"LayerVersions": list})
	return nil
}

func (s *Service) listLayers(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	q := r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	if q.Get("find") == "LayerVersion" { // GetLayerVersionByArn
		name, n, region, account := splitName(strings.Replace(q.Get("Arn"), ":layer:", ":function:", 1))
		l := s.st.Layers[key(account, region, name)]
		num, _ := strconv.Atoi(n)
		if l == nil || l.Versions[num] == nil {
			return core.Errorf(404, "ResourceNotFoundException", "The resource you requested does not exist.")
		}
		core.RESTJSON(w, c, 200, layerJSON(l, l.Versions[num], c.Host))
		return nil
	}
	runtime, arch := q.Get("CompatibleRuntime"), q.Get("CompatibleArchitecture")
	// ponytail: checks the runtime family, not the exact enum; list the runtimes if a typo like python3.99 must be refused.
	if runtime != "" && !slices.ContainsFunc([]string{"python", "nodejs", "java", "dotnet", "ruby", "go", "provided"}, func(f string) bool { return strings.HasPrefix(runtime, f) }) {
		return invalid("Value '%s' at 'compatibleRuntime' failed to satisfy constraint: Member must satisfy enum value set", runtime)
	}
	maxItems, _ := strconv.Atoi(q.Get("MaxItems"))
	if maxItems <= 0 || maxItems > 50 {
		maxItems = 50
	}
	out := map[string]any{}
	list := []any{}
	for _, k := range core.SortedKeys(s.st.Layers) {
		l := s.st.Layers[k]
		if l.Account != c.Account || l.Region != c.Region || l.Name <= q.Get("Marker") {
			continue
		}
		for n := l.Next - 1; n > 0; n-- { // the newest version that fits the filters
			if v := l.Versions[n]; v != nil && (runtime == "" || contains(v.Runtimes, runtime)) && (arch == "" || contains(v.Archs, arch)) {
				if len(list) == maxItems {
					out["NextMarker"] = list[len(list)-1].(map[string]any)["LayerName"]
					out["Layers"] = list
					core.RESTJSON(w, c, 200, out)
					return nil
				}
				list = append(list, map[string]any{"LayerName": l.Name, "LayerArn": l.arn(), "LatestMatchingVersion": layerJSON(l, v, c.Host)})
				break
			}
		}
	}
	out["Layers"] = list
	core.RESTJSON(w, c, 200, out)
	return nil
}

func (s *Service) layerPermission(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	var in struct{ StatementId, Action, Principal string }
	if err := decode(r, &in); err != nil {
		return err
	}
	b, _ := json.Marshal(map[string]any{"Sid": in.StatementId, "Effect": "Allow", "Principal": in.Principal, "Action": in.Action})
	core.RESTJSON(w, c, 201, map[string]string{"Statement": string(b), "RevisionId": core.UUID()})
	return nil
}

func (s *Service) layerPolicy(w http.ResponseWriter, r *http.Request, c *core.Ctx) error {
	return core.Errorf(404, "ResourceNotFoundException", "No policy is associated with the given resource.")
}

// layerZip finds a layer version by ARN in any account; the lock must be held.
func (s *Service) layerZip(arn string) []byte {
	name, n, region, account := splitName(strings.Replace(arn, ":layer:", ":function:", 1))
	l := s.st.Layers[key(account, region, name)]
	num, _ := strconv.Atoi(n)
	if l == nil || l.Versions[num] == nil {
		return nil
	}
	return l.Versions[num].Zip
}

// ---- console ----

// TaggedResources lists functions (ARN → tags) for the tagging API and Resource Explorer.
func (s *Service) TaggedResources(account, region string) map[string]map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]string{}
	for _, f := range s.st.Functions {
		if f.Account == account && f.Region == region {
			out[core.ARN("lambda", region, account, "function:"+f.Name)] = maps.Clone(f.Tags)
		}
	}
	return out
}

// Resources lists functions and event source mappings.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	fns := core.Table{Name: "Functions", Columns: []string{"Name", "Region", "Account", "Runtime", "Handler", "Memory", "Timeout", "Modified"}}
	for _, k := range core.SortedKeys(s.st.Functions) {
		f := s.st.Functions[k]
		rt := f.Latest.Runtime
		if f.Latest.PackageType == "Image" {
			rt = "image"
		}
		fns.Rows = append(fns.Rows, []string{f.Name, f.Region, f.Account, rt, f.Latest.Handler, strconv.Itoa(f.Latest.MemorySize) + " MB",
			strconv.Itoa(f.Latest.Timeout) + "s", f.Latest.Modified.Format(time.DateTime)})
	}
	esm := core.Table{Name: "Event source mappings", Columns: []string{"UUID", "Function", "Source", "State", "Batch", "Last result"}}
	for _, id := range core.SortedKeys(s.st.Mappings) {
		m := s.st.Mappings[id]
		esm.Rows = append(esm.Rows, []string{m.UUID, m.FunctionArn[strings.LastIndex(m.FunctionArn, ":")+1:], m.SourceArn, m.state(), strconv.Itoa(m.BatchSize), m.LastResult})
	}
	return []core.Table{fns, esm}
}

// Inspect returns a function's configuration, versions, aliases, policy and recent logs.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	if table == "Event source mappings" {
		if m := s.st.Mappings[id]; m != nil {
			return m.json()
		}
		return nil
	}
	for _, f := range s.st.Functions {
		if f.Name == id {
			out := map[string]any{"configuration": s.configJSON(f, f.Latest), "versions": sortedVersions(f), "aliases": f.Aliases,
				"policy": f.Policy, "tags": f.Tags, "recentLogs": s.RecentLogs(f.Account, f.Region, f.Name)}
			return out
		}
	}
	return nil
}

// RecentLogs returns the last log lines of a function.
func (s *Service) RecentLogs(account, region, name string) []string {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	return append([]string(nil), s.logs[key(account, region, name)]...)
}

// logLine keeps recent lines for the console and forwards them to CloudWatch Logs.
func (s *Service) logLine(sp *spec, stream, line string) {
	k := key(sp.account, sp.region, sp.name)
	s.logMu.Lock()
	l := append(s.logs[k], line)
	if len(l) > 300 {
		l = l[len(l)-300:]
	}
	s.logs[k] = l
	s.logMu.Unlock()
	if s.hub != nil {
		s.hub.Deliver(core.ARN("logs", sp.region, sp.account, "log-group:/aws/lambda/"+sp.name+":log-stream:"+stream), &core.Message{Body: line})
	}
}

// fingerprint changes when files in a hot-reload directory change.
// ponytail: walks the tree on every invoke (skipping node_modules/.git); cache it if trees get huge.
func fingerprint(dir string) string {
	var latest time.Time
	n := 0
	filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() && (d.Name() == "node_modules" || d.Name() == ".git" || d.Name() == "__pycache__") {
			return filepath.SkipDir
		}
		if info, err := d.Info(); err == nil && info.ModTime().After(latest) {
			latest = info.ModTime()
		}
		n++
		return nil
	})
	return strconv.FormatInt(latest.UnixNano(), 36) + "-" + strconv.Itoa(n)
}
