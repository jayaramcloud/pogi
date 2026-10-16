// Package ssm emulates AWS Systems Manager over JSON 1.1. Parameter Store:
// String, StringList and SecureString parameters with path hierarchies,
// version history, labels and tags, plus the public /aws/service parameters
// that CDK and SSM-typed CloudFormation parameters read, and
// /aws/reference/secretsmanager/<id>. Also documents and associations
// (documents.go), Run Command (commands.go) and service settings.
package ssm

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Parameter Store emulator.
type Service struct {
	hub    *core.Hub
	rpc    *core.RPC
	mu     sync.Mutex
	st     state
	ctx    context.Context // ends when the service closes; commands run under it
	cancel context.CancelFunc
}

type state struct {
	Params   map[string]*Param       `json:"params"`             // account/region/name
	Docs     map[string]*Document    `json:"docs,omitempty"`     // account/region/name
	Assocs   map[string]*Association `json:"assocs,omitempty"`   // account/region/id
	Commands map[string]*Command     `json:"commands,omitempty"` // account/region/id
	Settings map[string]*setting     `json:"settings,omitempty"` // account/region/id
}

// Param is a parameter with its versions, oldest first.
type Param struct {
	Name     string            `json:"name"`
	Account  string            `json:"account"`
	Region   string            `json:"region"`
	Tags     map[string]string `json:"tags,omitempty"`
	Versions []*Version        `json:"versions"`
}

// Version is one value of a parameter.
type Version struct {
	Version        int64     `json:"version"`
	Value          string    `json:"value"`
	Type           string    `json:"type"`
	KeyID          string    `json:"keyId,omitempty"`
	Description    string    `json:"description,omitempty"`
	AllowedPattern string    `json:"allowedPattern,omitempty"`
	Tier           string    `json:"tier"`
	Policies       []policy  `json:"policies,omitempty"`
	DataType       string    `json:"dataType"`
	Labels         []string  `json:"labels,omitempty"`
	Modified       time.Time `json:"modified"`
	User           string    `json:"user"`
}

// policy is a parameter policy as DescribeParameters shows it.
type policy struct{ PolicyText, PolicyType, PolicyStatus string }

// New creates the Parameter Store service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"PutParameter":            core.H(s.putParameter),
		"GetParameter":            core.H(s.getParameter),
		"GetParameters":           core.H(s.getParameters),
		"GetParametersByPath":     core.H(s.getParametersByPath),
		"DeleteParameter":         core.H(s.deleteParameter),
		"DeleteParameters":        core.H(s.deleteParameters),
		"DescribeParameters":      core.H(s.describeParameters),
		"GetParameterHistory":     core.H(s.getParameterHistory),
		"LabelParameterVersion":   core.H(s.labelParameterVersion),
		"UnlabelParameterVersion": core.H(s.unlabelParameterVersion),
		"AddTagsToResource":       core.H(s.addTagsToResource),
		"RemoveTagsFromResource":  core.H(s.removeTagsFromResource),
		"ListTagsForResource":     core.H(s.listTagsForResource),

		"CreateDocument":               core.H(s.createDocument),
		"UpdateDocument":               core.H(s.updateDocument),
		"UpdateDocumentDefaultVersion": core.H(s.updateDocumentDefaultVersion),
		"DescribeDocument":             core.H(s.describeDocument),
		"GetDocument":                  core.H(s.getDocument),
		"ListDocuments":                core.H(s.listDocuments),
		"ListDocumentVersions":         core.H(s.listDocumentVersions),
		"DeleteDocument":               core.H(s.deleteDocument),
		"DescribeDocumentPermission":   core.H(s.describeDocumentPermission),
		"ModifyDocumentPermission":     core.H(s.modifyDocumentPermission),
		"CreateAssociation":            core.H(s.createAssociation),
		"DescribeAssociation":          core.H(s.describeAssociation),
		"ListAssociations":             core.H(s.listAssociations),
		"UpdateAssociation":            core.H(s.updateAssociation),
		"DeleteAssociation":            core.H(s.deleteAssociation),
		"SendCommand":                  core.H(s.sendCommand),
		"ListCommands":                 core.H(s.listCommands),
		"ListCommandInvocations":       core.H(s.listCommandInvocations),
		"GetCommandInvocation":         core.H(s.getCommandInvocation),
		"CancelCommand":                core.H(s.cancelCommand),
		"GetServiceSetting":            core.H(s.getServiceSetting),
		"UpdateServiceSetting":         core.H(s.updateServiceSetting),
		"ResetServiceSetting":          core.H(s.resetServiceSetting),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "ssm", Cloud: core.AWS, Title: "Systems Manager", Targets: []string{"AmazonSSM"}, Signing: []string{"ssm"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

func newState() state {
	return state{Params: map[string]*Param{}, Docs: map[string]*Document{}, Assocs: map[string]*Association{},
		Commands: map[string]*Command{}, Settings: map[string]*setting{}}
}

// Close stops the commands that are still running.
func (s *Service) Close() error {
	s.cancel()
	return nil
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
	for _, c := range st.Commands { // a command that was running when pogi stopped will never finish
		c.abandon()
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- public parameters ----

var regions = []string{"af-south-1", "ap-east-1", "ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-south-1", "ap-south-2",
	"ap-southeast-1", "ap-southeast-2", "ap-southeast-3", "ap-southeast-4", "ca-central-1", "ca-west-1", "eu-central-1", "eu-central-2",
	"eu-north-1", "eu-south-1", "eu-south-2", "eu-west-1", "eu-west-2", "eu-west-3", "il-central-1", "me-central-1", "me-south-1",
	"sa-east-1", "us-east-1", "us-east-2", "us-west-1", "us-west-2"}

// public holds the read-only /aws/service parameters AWS publishes in every
// region; the AMI ids are plausible stand-ins.
var public = func() map[string]string {
	m := map[string]string{
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64":       "ami-0a7d80731ae1b2435",
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-arm64":        "ami-0b3e7dd7b2a99b08d",
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-x86_64":           "ami-0a7d80731ae1b2435",
		"/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-arm64":            "ami-0b3e7dd7b2a99b08d",
		"/aws/service/ami-amazon-linux-latest/amzn2-ami-hvm-x86_64-gp2":               "ami-0c101f26f147fa7fd",
		"/aws/service/ami-amazon-linux-latest/amzn2-ami-kernel-5.10-hvm-x86_64-gp2":   "ami-0c101f26f147fa7fd",
		"/aws/service/ecs/optimized-ami/amazon-linux-2023/recommended/image_id":       "ami-0e2d2a5e8f1c3b4a6",
		"/aws/service/ecs/optimized-ami/amazon-linux-2023/arm64/recommended/image_id": "ami-0f4c3b2a1d0e9f8c7",
		"/aws/service/ecs/optimized-ami/amazon-linux-2/recommended/image_id":          "ami-0d9e8f7a6b5c4d3e2",
	}
	for _, r := range regions {
		m["/aws/service/global-infrastructure/regions/"+r] = r
	}
	return m
}()

var publicDate = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func publicParam(name, region string) *Param {
	return &Param{Name: name, Region: region, Versions: []*Version{{Version: 1, Value: public[name], Type: "String", Tier: "Standard",
		DataType: "text", Modified: publicDate, User: core.GlobalARN("iam", region, "aws", "root")}}}
}

// ---- lookups ----

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func errNotFound(name string) error {
	return core.Errorf(400, "ParameterNotFound", "Parameter %s not found.", name)
}

func validation(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

// secretsPrefix is the path of the parameters that read Secrets Manager secrets.
const secretsPrefix = "/aws/reference/secretsmanager/"

// secretParam reads a secret as a SecureString parameter of version 0, the way
// /aws/reference/secretsmanager/<id> does. sel is a version stage or id.
// It calls Secrets Manager while the lock is held; Secrets Manager never calls back.
func (s *Service) secretParam(account, region, name, sel string) (*Param, error) {
	in := map[string]string{"SecretId": strings.TrimPrefix(name, secretsPrefix)}
	if len(sel) >= 32 { // AWS version ids are UUIDs, stages are short words
		in["VersionId"] = sel
	} else if sel != "" {
		in["VersionStage"] = sel
	}
	var out struct {
		SecretString string
		SecretBinary []byte
		CreatedDate  core.Time
	}
	if err := s.hub.Call(account, region, "secretsmanager", "GetSecretValue", in, &out); err != nil {
		return nil, errNotFound(name)
	}
	value := out.SecretString
	if value == "" {
		value = base64.StdEncoding.EncodeToString(out.SecretBinary)
	}
	return &Param{Name: name, Region: region, Versions: []*Version{{Value: value, Type: "SecureString", Tier: "Standard", DataType: "text",
		Modified: time.Time(out.CreatedDate), User: core.GlobalARN("iam", region, "aws", "root")}}}, nil
}

func (s *Service) lookup(account, region, name string) *Param {
	if _, ok := public[name]; ok {
		return publicParam(name, region)
	}
	return s.st.Params[key(account, region, name)]
}

// get resolves a name, "name:version", "name:label" or an ARN to a version
// and its selector. Lock held.
func (s *Service) get(c *core.Ctx, ref string) (*Param, *Version, string, error) {
	account, region, name, arn := c.Account, c.Region, ref, false
	if _, r, a, res, ok := core.ParseARN(ref); ok && strings.HasPrefix(res, "parameter/") {
		region, name, arn = r, strings.TrimPrefix(res, "parameter"), true
		if a != "" {
			account = a
		}
	}
	name, sel, _ := strings.Cut(name, ":")
	if strings.HasPrefix(name, secretsPrefix) {
		p, err := s.secretParam(account, region, name, sel)
		if err != nil {
			return nil, nil, "", err
		}
		return p, p.Versions[0], "", nil
	}
	p := s.lookup(account, region, name)
	if p == nil && arn { // ARNs add a slash to names that have none
		p = s.lookup(account, region, name[1:])
	}
	if p == nil {
		return nil, nil, "", errNotFound(name)
	}
	v := p.Versions[len(p.Versions)-1]
	if sel != "" {
		if v = p.version(sel); v == nil {
			if _, err := strconv.Atoi(sel); err == nil {
				return nil, nil, "", core.Errorf(400, "ParameterVersionNotFound", "Systems Manager could not find version %s of %s. Verify the version and try again.", sel, name)
			}
			return nil, nil, "", errNotFound(name + ":" + sel)
		}
		sel = ":" + sel
	}
	return p, v, sel, nil
}

// owned finds one of the caller's own (not public) parameters. Lock held.
func (s *Service) owned(c *core.Ctx, name string) (*Param, error) {
	if p := s.st.Params[key(c.Account, c.Region, name)]; p != nil {
		return p, nil
	}
	return nil, errNotFound(name)
}

// version finds a version by number or label.
func (p *Param) version(sel string) *Version {
	for _, v := range p.Versions {
		if strconv.FormatInt(v.Version, 10) == sel || slices.Contains(v.Labels, sel) {
			return v
		}
	}
	return nil
}

func (p *Param) arn() string {
	return core.ARN("ssm", p.Region, p.Account, "parameter/"+strings.TrimPrefix(p.Name, "/"))
}

// value is the plain value, or a stand-in for the KMS ciphertext AWS returns
// for a SecureString read without decryption.
func (v *Version) value(decrypt bool) string {
	if v.Type != "SecureString" || decrypt {
		return v.Value
	}
	h := sha256.Sum256([]byte(v.KeyID + "\x00" + v.Value))
	return "AQICAH" + base64.StdEncoding.EncodeToString(h[:])
}

// paramOut is the Parameter shape of the Get* operations.
type paramOut struct {
	Name             string
	Type             string
	Value            string
	Version          int64
	Selector         string `json:",omitempty"`
	LastModifiedDate core.Time
	ARN              string
	DataType         string
}

func (p *Param) out(v *Version, sel string, decrypt bool) paramOut {
	return paramOut{p.Name, v.Type, v.value(decrypt), v.Version, sel, core.Time(v.Modified), p.arn(), v.DataType}
}

// meta is ParameterMetadata, and ParameterHistory once Value and Labels are set.
type meta struct {
	Name             string
	ARN              string
	Type             string
	KeyId            string `json:",omitempty"`
	LastModifiedDate core.Time
	LastModifiedUser string
	Description      string `json:",omitempty"`
	Value            string `json:",omitempty"`
	AllowedPattern   string `json:",omitempty"`
	Version          int64
	Labels           []string `json:",omitempty"`
	Tier             string
	Policies         []policy `json:",omitempty"`
	DataType         string
}

func (p *Param) meta(v *Version) meta {
	return meta{Name: p.Name, ARN: p.arn(), Type: v.Type, KeyId: v.KeyID, LastModifiedDate: core.Time(v.Modified), LastModifiedUser: v.User,
		Description: v.Description, AllowedPattern: v.AllowedPattern, Version: v.Version, Tier: v.Tier, Policies: v.Policies, DataType: v.DataType}
}

// ---- put, get, delete ----

var (
	nameRe  = regexp.MustCompile(`^/?[a-zA-Z0-9_.-]+(/[a-zA-Z0-9_.-]+)*$`)
	keyIDRe = regexp.MustCompile(`^(alias/[a-zA-Z0-9/_-]+|arn:aws[a-z-]*:kms:.+|(mrk-)?[a-fA-F0-9-]{32,36})$`)
)

func checkName(name string) error {
	first := strings.ToLower(strings.TrimPrefix(name, "/"))
	if !nameRe.MatchString(name) || len(name) > 1011 || strings.Contains(name, "/") && name[0] != '/' ||
		strings.HasPrefix(first, "aws") || strings.HasPrefix(first, "ssm") {
		return validation(`Parameter name: can't be prefixed with "aws" or "ssm" (case-insensitive). If formed as a path, it can consist of sub-paths divided by slash symbol; each sub-path can be formed as a mix of letters, numbers and the following 3 symbols .-_`)
	}
	if strings.Count(name, "/") > 15 {
		return core.Errorf(400, "HierarchyLevelLimitExceededException", "A hierarchy can have a maximum of 15 levels. For more information, see https://docs.aws.amazon.com/systems-manager/latest/userguide/sysman-parameter-name-constraints.html")
	}
	return nil
}

type tag struct{ Key, Value string }

func (s *Service) putParameter(c *core.Ctx, in *struct {
	Name           string
	Value          *string
	Type           string
	KeyId          string
	Overwrite      bool
	Description    *string
	Tier           string
	AllowedPattern *string
	Policies       string
	DataType       string
	Tags           []tag
}) (any, error) {
	if err := checkName(in.Name); err != nil {
		return nil, err
	}
	switch {
	case in.Value == nil || *in.Value == "":
		return nil, validation("1 validation error detected: Value at 'value' failed to satisfy constraint: Member must have length greater than or equal to 1")
	case in.Overwrite && len(in.Tags) > 0:
		return nil, validation("Invalid request: tags and overwrite can't be used together. To create a parameter with tags, please remove overwrite flag. To update tags for an existing parameter, please use AddTagsToResource or RemoveTagsFromResource.")
	case !slices.Contains([]string{"", "String", "StringList", "SecureString"}, in.Type):
		return nil, validation("1 validation error detected: Value '%s' at 'type' failed to satisfy constraint: Member must satisfy enum value set: [SecureString, StringList, String]", in.Type)
	case !slices.Contains([]string{"", "Standard", "Advanced", "Intelligent-Tiering"}, in.Tier):
		return nil, validation("1 validation error detected: Value '%s' at 'tier' failed to satisfy constraint: Member must satisfy enum value set: [Advanced, Intelligent-Tiering, Standard]", in.Tier)
	case !slices.Contains([]string{"", "text", "aws:ec2:image", "aws:ssm:integration"}, in.DataType):
		return nil, validation("The following data type is not supported: %s (Data type names are all lowercase.)", in.DataType)
	case in.KeyId != "" && !keyIDRe.MatchString(in.KeyId):
		return nil, core.Errorf(400, "InvalidKeyId", "The KeyId %s is not valid.", in.KeyId)
	}
	var policies []policy
	if in.Policies != "" {
		var raw []json.RawMessage
		if err := json.Unmarshal([]byte(in.Policies), &raw); err != nil {
			return nil, core.Errorf(400, "InvalidPolicyTypeException", "The policies must be a JSON array of policy objects.")
		}
		for _, r := range raw {
			var t struct{ Type string }
			if err := json.Unmarshal(r, &t); err != nil || t.Type == "" {
				return nil, core.Errorf(400, "InvalidPolicyTypeException", "Each policy needs a Type.")
			}
			policies = append(policies, policy{string(r), t.Type, "Pending"})
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.Name)
	p := s.st.Params[k]
	user := c.Principal
	if user == "" {
		user = c.GlobalARN("iam", "root")
	}
	v := &Version{Version: 1, Value: *in.Value, Type: "String", Tier: "Standard", DataType: "text", Modified: time.Now().UTC(), User: user}
	if p != nil {
		if !in.Overwrite {
			return nil, core.Errorf(400, "ParameterAlreadyExists", "The parameter already exists. To overwrite this value, set the overwrite option in the request to true.")
		}
		prev := p.Versions[len(p.Versions)-1] // omitted settings carry over
		v.Version, v.Type, v.KeyID, v.Description, v.AllowedPattern, v.Tier, v.DataType, v.Policies =
			prev.Version+1, prev.Type, prev.KeyID, prev.Description, prev.AllowedPattern, prev.Tier, prev.DataType, prev.Policies
	}
	if policies != nil {
		v.Policies = policies
	}
	if in.Type != "" {
		v.Type = in.Type
	}
	if in.KeyId != "" {
		v.KeyID = in.KeyId
	}
	switch {
	case v.Type != "SecureString":
		v.KeyID = ""
	case v.KeyID == "":
		v.KeyID = "alias/aws/ssm"
	}
	if in.Description != nil {
		v.Description = *in.Description
	}
	if in.AllowedPattern != nil {
		v.AllowedPattern = *in.AllowedPattern
	}
	if in.DataType != "" {
		v.DataType = in.DataType
	}
	if in.Tier != "" {
		v.Tier = in.Tier
	}
	if v.Tier == "Intelligent-Tiering" {
		v.Tier = map[bool]string{true: "Advanced", false: "Standard"}[len(v.Value) > 4096]
	}
	if limit := map[string]int{"Standard": 4096, "Advanced": 8192}[v.Tier]; len(v.Value) > limit {
		return nil, validation("%s tier parameters support a maximum parameter value of %d characters.", v.Tier, limit)
	}
	if v.AllowedPattern != "" {
		re, err := regexp.Compile("^(?:" + v.AllowedPattern + ")$")
		if err != nil {
			return nil, core.Errorf(400, "InvalidAllowedPatternException", "The allowed pattern %s is not a valid regular expression.", v.AllowedPattern)
		}
		if !re.MatchString(v.Value) {
			return nil, core.Errorf(400, "ParameterPatternMismatchException", "Parameter value, cannot be validated against allowedPattern: %s", v.AllowedPattern)
		}
	}
	if p == nil {
		p = &Param{Name: in.Name, Account: c.Account, Region: c.Region, Tags: map[string]string{}}
		for _, t := range in.Tags {
			p.Tags[t.Key] = t.Value
		}
		s.st.Params[k] = p
	}
	// ponytail: the oldest of 100 versions is dropped even when labeled (AWS refuses then).
	if p.Versions = append(p.Versions, v); len(p.Versions) > 100 {
		p.Versions = p.Versions[len(p.Versions)-100:]
	}
	return map[string]any{"Version": v.Version, "Tier": v.Tier}, nil
}

func (s *Service) getParameter(c *core.Ctx, in *struct {
	Name           string
	WithDecryption bool
}) (any, error) {
	if strings.HasPrefix(in.Name, secretsPrefix) && !in.WithDecryption {
		return nil, validation("WithDecryption flag must be True for retrieving a Secret Manager secret.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, v, sel, err := s.get(c, in.Name)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Parameter": p.out(v, sel, in.WithDecryption)}, nil
}

func (s *Service) getParameters(c *core.Ctx, in *struct {
	Names          []string
	WithDecryption bool
}) (any, error) {
	if len(in.Names) > 10 {
		return nil, validation("1 validation error detected: Value at 'names' failed to satisfy constraint: Member must have length less than or equal to 10")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out := struct {
		Parameters        []paramOut
		InvalidParameters []string
	}{[]paramOut{}, []string{}}
	for _, n := range in.Names {
		p, v, sel, err := s.get(c, n)
		if err != nil || strings.HasPrefix(n, secretsPrefix) && !in.WithDecryption {
			out.InvalidParameters = append(out.InvalidParameters, n)
			continue
		}
		out.Parameters = append(out.Parameters, p.out(v, sel, in.WithDecryption))
	}
	return &out, nil
}

func (s *Service) deleteParameter(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.owned(c, in.Name); err != nil {
		return nil, err
	}
	delete(s.st.Params, key(c.Account, c.Region, in.Name))
	return nil, nil
}

func (s *Service) deleteParameters(c *core.Ctx, in *struct{ Names []string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := struct{ DeletedParameters, InvalidParameters []string }{[]string{}, []string{}}
	for _, n := range in.Names {
		if _, err := s.owned(c, n); err != nil {
			out.InvalidParameters = append(out.InvalidParameters, n)
			continue
		}
		delete(s.st.Params, key(c.Account, c.Region, n))
		out.DeletedParameters = append(out.DeletedParameters, n)
	}
	return &out, nil
}

// ---- listing ----

type stringFilter struct {
	Key    string
	Option string
	Values []string
}

// params lists the caller's parameters by name, with the public ones when asked. Lock held.
func (s *Service) params(c *core.Ctx, withPublic bool) []*Param {
	var out []*Param
	for _, p := range s.st.Params {
		if p.Account == c.Account && p.Region == c.Region {
			out = append(out, p)
		}
	}
	if withPublic {
		for name := range public {
			out = append(out, publicParam(name, c.Region))
		}
	}
	slices.SortFunc(out, func(a, b *Param) int { return strings.Compare(a.Name, b.Name) })
	return out
}

// under reports whether name lies below path: one level down, or any depth when recursive.
func under(name, path string, recursive bool) bool {
	rest, ok := strings.CutPrefix(name, strings.TrimSuffix(path, "/")+"/")
	return ok && rest != "" && (recursive || !strings.Contains(rest, "/"))
}

func checkFilters(filters []stringFilter, keys ...string) error {
	for _, f := range filters {
		if !slices.Contains(keys, f.Key) && !strings.HasPrefix(f.Key, "tag:") {
			return core.Errorf(400, "InvalidFilterKey", "The following filter key is not valid: %s. Valid filter keys include: [%s].", f.Key, strings.Join(keys, ", "))
		}
		if !(f.Option == "" || f.Option == "Equals" || f.Key == "Name" && (f.Option == "BeginsWith" || f.Option == "Contains") ||
			f.Key == "Path" && (f.Option == "Recursive" || f.Option == "OneLevel")) {
			return core.Errorf(400, "InvalidFilterOption", "The following filter option is not valid: %s. Valid options include: [Equals, BeginsWith, Contains] for Name and [Recursive, OneLevel] for Path.", f.Option)
		}
		if len(f.Values) == 0 && !strings.HasPrefix(f.Key, "tag:") {
			return core.Errorf(400, "InvalidFilterValue", "The following filter values are missing : null for filter key %s.", f.Key)
		}
	}
	return nil
}

// match applies ParameterStringFilters to one version of p.
func (p *Param) match(v *Version, filters []stringFilter) bool {
	for _, f := range filters {
		var field string
		switch f.Key {
		case "Name":
			field = p.Name
		case "Type":
			field = v.Type
		case "KeyId":
			field = v.KeyID
		case "Tier":
			field = v.Tier
		case "DataType":
			field = v.DataType
		case "Label":
			if !slices.ContainsFunc(f.Values, func(l string) bool { return slices.Contains(v.Labels, l) }) {
				return false
			}
			continue
		case "Path":
			if !slices.ContainsFunc(f.Values, func(path string) bool { return under(p.Name, path, f.Option == "Recursive") }) {
				return false
			}
			continue
		default: // tag:Key
			val, ok := p.Tags[strings.TrimPrefix(f.Key, "tag:")]
			if !ok || len(f.Values) > 0 && !slices.Contains(f.Values, val) {
				return false
			}
			continue
		}
		if !slices.ContainsFunc(f.Values, func(x string) bool {
			switch f.Option {
			case "BeginsWith":
				return strings.HasPrefix(field, x)
			case "Contains":
				return strings.Contains(field, x)
			}
			return field == x
		}) {
			return false
		}
	}
	return true
}

// page slices n items for an offset NextToken and a page size (def when unset).
func page(n int, token string, size, def int) (start, end int, next string, err error) {
	if token != "" {
		if start, err = strconv.Atoi(token); err != nil || start < 0 {
			return 0, 0, "", core.Errorf(400, "InvalidNextToken", "The specified token isn't valid.")
		}
	}
	if size <= 0 {
		size = def
	}
	start = min(start, n)
	end = min(start+size, n)
	if end < n {
		next = strconv.Itoa(end)
	}
	return start, end, next, nil
}

func (s *Service) getParametersByPath(c *core.Ctx, in *struct {
	Path             string
	Recursive        bool
	ParameterFilters []stringFilter
	WithDecryption   bool
	MaxResults       int
	NextToken        string
}) (any, error) {
	if !strings.HasPrefix(in.Path, "/") {
		return nil, validation(`The parameter doesn't meet the parameter name requirements. The parameter name must begin with a forward slash "/".`)
	}
	if err := checkFilters(in.ParameterFilters, "Type", "KeyId", "Label"); err != nil {
		return nil, err
	}
	var labels []string
	for _, f := range in.ParameterFilters {
		if f.Key == "Label" {
			labels = append(labels, f.Values...)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []paramOut
	for _, p := range s.params(c, strings.HasPrefix(in.Path, "/aws/")) {
		if !under(p.Name, in.Path, in.Recursive) {
			continue
		}
		v := p.Versions[len(p.Versions)-1]
		if len(labels) > 0 { // a Label filter selects the labeled version
			i := slices.IndexFunc(p.Versions, func(v *Version) bool {
				return slices.ContainsFunc(labels, func(l string) bool { return slices.Contains(v.Labels, l) })
			})
			if i < 0 {
				continue
			}
			v = p.Versions[i]
		}
		if p.match(v, in.ParameterFilters) {
			all = append(all, p.out(v, "", in.WithDecryption))
		}
	}
	start, end, next, err := page(len(all), in.NextToken, in.MaxResults, 10)
	if err != nil {
		return nil, err
	}
	return &struct {
		Parameters []paramOut
		NextToken  string `json:",omitempty"`
	}{append([]paramOut{}, all[start:end]...), next}, nil
}

func (s *Service) describeParameters(c *core.Ctx, in *struct {
	Filters          []stringFilter
	ParameterFilters []stringFilter
	MaxResults       int
	NextToken        string
}) (any, error) {
	if len(in.Filters) > 0 && len(in.ParameterFilters) > 0 {
		return nil, validation("You can use either Filters or ParameterFilters in a single request.")
	}
	filters := in.ParameterFilters
	for _, f := range in.Filters { // the legacy Name filter matches by prefix
		if f.Key == "Name" {
			f.Option = "BeginsWith"
		}
		filters = append(filters, f)
	}
	if err := checkFilters(filters, "Name", "Type", "KeyId", "Path", "Tier", "DataType", "Label"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []meta
	for _, p := range s.params(c, false) {
		if v := p.Versions[len(p.Versions)-1]; p.match(v, filters) {
			all = append(all, p.meta(v))
		}
	}
	start, end, next, err := page(len(all), in.NextToken, in.MaxResults, 10)
	if err != nil {
		return nil, err
	}
	return &struct {
		Parameters []meta
		NextToken  string `json:",omitempty"`
	}{append([]meta{}, all[start:end]...), next}, nil
}

func (s *Service) getParameterHistory(c *core.Ctx, in *struct {
	Name           string
	WithDecryption bool
	MaxResults     int
	NextToken      string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, _, _, err := s.get(c, in.Name)
	if err != nil {
		return nil, err
	}
	start, end, next, err := page(len(p.Versions), in.NextToken, in.MaxResults, 50)
	if err != nil {
		return nil, err
	}
	out := struct {
		Parameters []meta
		NextToken  string `json:",omitempty"`
	}{Parameters: []meta{}, NextToken: next}
	for _, v := range p.Versions[start:end] {
		m := p.meta(v)
		m.Value, m.Labels = v.value(in.WithDecryption), slices.Clone(v.Labels)
		out.Parameters = append(out.Parameters, m)
	}
	return &out, nil
}

// ---- labels ----

var labelRe = regexp.MustCompile(`^[a-zA-Z_.-][a-zA-Z0-9_.-]{0,99}$`)

func validLabel(l string) bool {
	low := strings.ToLower(l)
	return labelRe.MatchString(l) && !strings.HasPrefix(low, "aws") && !strings.HasPrefix(low, "ssm")
}

// labeled finds the version a label operation targets (the latest when n is nil). Lock held.
func (s *Service) labeled(c *core.Ctx, name string, n *int64) (*Param, *Version, error) {
	p, err := s.owned(c, name)
	if err != nil {
		return nil, nil, err
	}
	v := p.Versions[len(p.Versions)-1]
	if n != nil {
		if v = p.version(strconv.FormatInt(*n, 10)); v == nil {
			return nil, nil, core.Errorf(400, "ParameterVersionNotFound", "Systems Manager could not find version %d of %s. Verify the version and try again.", *n, name)
		}
	}
	return p, v, nil
}

func (s *Service) labelParameterVersion(c *core.Ctx, in *struct {
	Name             string
	ParameterVersion *int64
	Labels           []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, v, err := s.labeled(c, in.Name, in.ParameterVersion)
	if err != nil {
		return nil, err
	}
	invalid := []string{}
	for _, l := range in.Labels {
		switch {
		case !validLabel(l):
			invalid = append(invalid, l)
		case slices.Contains(v.Labels, l):
		case len(v.Labels) >= 10:
			return nil, core.Errorf(400, "ParameterVersionLabelLimitExceeded", "A parameter version can have a maximum of 10 labels.")
		default: // a label names one version at a time
			for _, o := range p.Versions {
				o.Labels = slices.DeleteFunc(o.Labels, func(x string) bool { return x == l })
			}
			v.Labels = append(v.Labels, l)
		}
	}
	return map[string]any{"InvalidLabels": invalid, "ParameterVersion": v.Version}, nil
}

func (s *Service) unlabelParameterVersion(c *core.Ctx, in *struct {
	Name             string
	ParameterVersion *int64
	Labels           []string
}) (any, error) {
	if in.ParameterVersion == nil {
		return nil, validation("1 validation error detected: Value null at 'parameterVersion' failed to satisfy constraint: Member must not be null")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, err := s.labeled(c, in.Name, in.ParameterVersion)
	if err != nil {
		return nil, err
	}
	out := struct{ RemovedLabels, InvalidLabels []string }{[]string{}, []string{}}
	for _, l := range in.Labels {
		if !slices.Contains(v.Labels, l) {
			out.InvalidLabels = append(out.InvalidLabels, l)
			continue
		}
		v.Labels = slices.DeleteFunc(v.Labels, func(x string) bool { return x == l })
		out.RemovedLabels = append(out.RemovedLabels, l)
	}
	return &out, nil
}

// ---- tags ----

// tagged finds the tags of a parameter, document or association for the tagging operations. Lock held.
func (s *Service) tagged(c *core.Ctx, typ, id string) (*map[string]string, error) {
	switch typ {
	case "Parameter":
		if p := s.st.Params[key(c.Account, c.Region, id)]; p != nil {
			return &p.Tags, nil
		}
	case "Document":
		if d := s.st.Docs[key(c.Account, c.Region, id)]; d != nil {
			return &d.Tags, nil
		}
	case "Association":
		if a := s.st.Assocs[key(c.Account, c.Region, id)]; a != nil {
			return &a.Tags, nil
		}
	default:
		return nil, core.Errorf(400, "InvalidResourceType", "The resource type %s isn't supported by pogi's SSM; Parameter, Document and Association are.", typ)
	}
	return nil, core.Errorf(400, "InvalidResourceId", "The resource ID \"%s\" isn't valid. Verify the ID and try again.", id)
}

func (s *Service) addTagsToResource(c *core.Ctx, in *struct {
	ResourceType, ResourceId string
	Tags                     []tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagged(c, in.ResourceType, in.ResourceId)
	if err != nil {
		return nil, err
	}
	if *tags == nil {
		*tags = map[string]string{}
	}
	for _, t := range in.Tags {
		(*tags)[t.Key] = t.Value
	}
	if len(*tags) > 50 {
		for _, t := range in.Tags {
			delete(*tags, t.Key)
		}
		return nil, core.Errorf(400, "TooManyTagsError", "The Targets parameter includes too many tags. Remove one or more tags and try the request again.")
	}
	return nil, nil
}

func (s *Service) removeTagsFromResource(c *core.Ctx, in *struct {
	ResourceType, ResourceId string
	TagKeys                  []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagged(c, in.ResourceType, in.ResourceId)
	if err != nil {
		return nil, err
	}
	for _, k := range in.TagKeys {
		delete(*tags, k)
	}
	return nil, nil
}

func (s *Service) listTagsForResource(c *core.Ctx, in *struct{ ResourceType, ResourceId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tags, err := s.tagged(c, in.ResourceType, in.ResourceId)
	if err != nil {
		return nil, err
	}
	list := []tag{}
	for _, k := range core.SortedKeys(*tags) {
		list = append(list, tag{k, (*tags)[k]})
	}
	return map[string]any{"TagList": list}, nil
}

// ---- Go API for other services and the console ----

// Value returns the current (decrypted) value of a parameter, a "name:version"
// or "name:label" selector, or a public /aws/service parameter. CloudFormation
// uses it for SSM parameter types.
func (s *Service) Value(account, region, name string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, v, _, err := s.get(&core.Ctx{Account: account, Region: region}, name)
	if err != nil {
		return "", err
	}
	return v.Value, nil
}

// Resources lists parameters for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Parameters", Columns: []string{"Name", "Region", "Account", "Type", "Version", "Modified"}}
	for _, k := range core.SortedKeys(s.st.Params) {
		p := s.st.Params[k]
		v := p.Versions[len(p.Versions)-1]
		t.Rows = append(t.Rows, []string{p.Name, p.Region, p.Account, v.Type, strconv.FormatInt(v.Version, 10), v.Modified.Format(time.DateTime)})
	}
	return []core.Table{t}
}

// Inspect returns a parameter's metadata, labels and tags, and its value unless it is a SecureString.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Params) {
		p := s.st.Params[k]
		if p.Name != id {
			continue
		}
		v := p.Versions[len(p.Versions)-1]
		out := map[string]any{"parameter": p.meta(v), "labels": slices.Clone(v.Labels), "tags": maps.Clone(p.Tags)}
		if v.Type != "SecureString" {
			out["value"] = v.Value
		}
		return out
	}
	return nil
}
