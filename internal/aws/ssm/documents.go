package ssm

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"go.yaml.in/yaml/v3"
)

// Document is a document with its versions, oldest first.
type Document struct {
	Name       string            `json:"name"`
	Account    string            `json:"account"`
	Region     string            `json:"region"`
	Type       string            `json:"type"`
	TargetType string            `json:"targetType,omitempty"`
	Default    int               `json:"default"`
	Versions   []*DocVersion     `json:"versions"`
	Tags       map[string]string `json:"tags,omitempty"`
	Shared     []string          `json:"shared,omitempty"` // account ids it is shared with
	Owner      string            `json:"owner,omitempty"`  // "Amazon" for the built-in ones
}

// DocVersion is one revision of a document.
type DocVersion struct {
	Version int       `json:"version"`
	Name    string    `json:"name,omitempty"` // VersionName
	Content string    `json:"content"`
	Format  string    `json:"format"`
	Created time.Time `json:"created"`
}

// docSpec is the part of a document's content that pogi reads.
type docSpec struct {
	SchemaVersion string `json:"schemaVersion"`
	Description   string `json:"description"`
	Parameters    map[string]struct {
		Type        string `json:"type"`
		Description string `json:"description"`
		Default     any    `json:"default"`
	} `json:"parameters"`
	MainSteps []struct {
		Action string         `json:"action"`
		Name   string         `json:"name"`
		Inputs map[string]any `json:"inputs"`
	} `json:"mainSteps"`
}

var docTypes = []string{"Command", "Policy", "Automation", "Session", "Package", "ApplicationConfiguration", "ApplicationConfigurationSchema",
	"DeploymentStrategy", "ChangeCalendar", "Automation.ChangeTemplate", "ProblemAnalysis", "ProblemAnalysisTemplate", "CloudFormation",
	"ConformancePackTemplate", "QuickSetup"}

var docNameRe = regexp.MustCompile(`^[a-zA-Z0-9_\-.:/]{3,128}$`)

// parseDoc reads the content of a JSON or YAML document.
func parseDoc(content, format string) (*docSpec, error) {
	var spec docSpec
	switch format {
	case "JSON":
		if err := json.Unmarshal([]byte(content), &spec); err != nil {
			return nil, core.Errorf(400, "InvalidDocumentContent", "The content for the document is not valid: %v", err)
		}
	case "YAML":
		var v any
		if err := yaml.Unmarshal([]byte(content), &v); err != nil {
			return nil, core.Errorf(400, "InvalidDocumentContent", "The content for the document is not valid: %v", err)
		}
		b, err := json.Marshal(v)
		if err == nil {
			err = json.Unmarshal(b, &spec)
		}
		if err != nil {
			return nil, core.Errorf(400, "InvalidDocumentContent", "The content for the document is not valid: %v", err)
		}
	}
	return &spec, nil
}

// ---- the built-in documents ----

const shellDoc = `{"schemaVersion":"2.2","description":"Run a shell script or specify the commands to run.",
"parameters":{"commands":{"type":"StringList","description":"(Required) Specify a shell script or a command to run."},
"workingDirectory":{"type":"String","default":"","description":"(Optional) The path to the working directory on your instance."},
"executionTimeout":{"type":"String","default":"3600","description":"(Optional) The time in seconds for a command to complete before it is considered to have failed."}},
"mainSteps":[{"action":"aws:runShellScript","name":"runShellScript","inputs":{"timeoutSeconds":"{{ executionTimeout }}","runCommand":"{{ commands }}"}}]}`

const powershellDoc = `{"schemaVersion":"2.2","description":"Run a PowerShell script or specify the commands to run.",
"parameters":{"commands":{"type":"StringList","description":"(Required) Specify a shell script or a command to run."},
"workingDirectory":{"type":"String","default":"","description":"(Optional) The path to the working directory on your instance."},
"executionTimeout":{"type":"String","default":"3600","description":"(Optional) The time in seconds for a command to complete before it is considered to have failed."}},
"mainSteps":[{"action":"aws:runPowerShellScript","name":"runPowerShellScript","inputs":{"timeoutSeconds":"{{ executionTimeout }}","runCommand":"{{ commands }}"}}]}`

// builtinDocs are the AWS-owned documents that SendCommand most often names.
// ponytail: only the two script documents run; the others exist to be listed and read.
var builtinDocs = func() map[string]*Document {
	m := map[string]*Document{}
	for name, content := range map[string]string{"AWS-RunShellScript": shellDoc, "AWS-RunPowerShellScript": powershellDoc,
		"AWS-UpdateSSMAgent":          `{"schemaVersion":"2.0","description":"Update the SSM Agent.","mainSteps":[{"action":"aws:updateSsmAgent","name":"update","inputs":{}}]}`,
		"AWS-ConfigureAWSPackage":     `{"schemaVersion":"2.0","description":"Install or uninstall a Distributor package.","mainSteps":[{"action":"aws:configurePackage","name":"configure","inputs":{}}]}`,
		"AWS-GatherSoftwareInventory": `{"schemaVersion":"1.2","description":"Gather software inventory.","runtimeConfig":{"aws:softwareInventory":{"properties":[{}]}}}`} {
		m[name] = &Document{Name: name, Owner: "Amazon", Type: "Command", Default: 1,
			Versions: []*DocVersion{{Version: 1, Content: content, Format: "JSON", Created: publicDate}}}
	}
	return m
}()

// document finds a document of the caller or a built-in one. Lock held.
func (s *Service) document(c *core.Ctx, name string) *Document {
	if d := builtinDocs[name]; d != nil {
		return d
	}
	if i := strings.LastIndex(name, "/"); i >= 0 && strings.HasPrefix(name, "arn:") { // an ARN: .../document/<name>
		name = name[i+1:]
	}
	return s.st.Docs[key(c.Account, c.Region, name)]
}

func errNoDocument(name string) error {
	return core.Errorf(400, "InvalidDocument", "Document with name %s does not exist.", name)
}

// version picks a version by number, "$LATEST", "$DEFAULT" or VersionName; empty means the default.
func (d *Document) version(v, name string) *DocVersion {
	switch {
	case v == "" && name == "", v == "$DEFAULT":
		return d.Versions[d.index(d.Default)]
	case v == "$LATEST":
		return d.Versions[len(d.Versions)-1]
	}
	for _, x := range d.Versions {
		if v != "" && strconv.Itoa(x.Version) == v || v == "" && name == x.Name {
			return x
		}
	}
	return nil
}

func (d *Document) index(version int) int {
	if i := slices.IndexFunc(d.Versions, func(x *DocVersion) bool { return x.Version == version }); i >= 0 {
		return i
	}
	return 0
}

func (d *Document) owner() string {
	if d.Owner != "" {
		return d.Owner
	}
	return d.Account
}

type docParam struct {
	Name         string
	Type         string
	Description  string `json:",omitempty"`
	DefaultValue string `json:",omitempty"`
}

// docDesc is DocumentDescription.
type docDesc struct {
	Hash            string
	HashType        string
	Name            string
	Owner           string
	VersionName     string `json:",omitempty"`
	CreatedDate     core.Time
	Status          string
	DocumentVersion string
	Description     string `json:",omitempty"`
	Parameters      []docParam
	PlatformTypes   []string
	DocumentType    string
	SchemaVersion   string
	LatestVersion   string
	DefaultVersion  string
	DocumentFormat  string
	TargetType      string `json:",omitempty"`
	Tags            []tag
}

func platforms(spec *docSpec) []string {
	linux, windows := false, false
	for _, st := range spec.MainSteps {
		switch {
		case strings.HasSuffix(st.Action, "PowerShellScript"):
			windows = true
		case strings.HasSuffix(st.Action, "runShellScript"):
			linux = true
		default:
			return []string{"Linux", "MacOS", "Windows"}
		}
	}
	switch {
	case linux && !windows:
		return []string{"Linux", "MacOS"}
	case windows && !linux:
		return []string{"Windows"}
	}
	return []string{"Linux", "MacOS", "Windows"}
}

func (d *Document) describe(v *DocVersion) docDesc {
	spec, _ := parseDoc(v.Content, v.Format)
	if spec == nil {
		spec = &docSpec{}
	}
	sum := sha256.Sum256([]byte(v.Content))
	out := docDesc{Hash: hex.EncodeToString(sum[:]), HashType: "Sha256", Name: d.Name, Owner: d.owner(), VersionName: v.Name,
		CreatedDate: core.Time(v.Created), Status: "Active", DocumentVersion: strconv.Itoa(v.Version), Description: spec.Description,
		Parameters: []docParam{}, PlatformTypes: platforms(spec), DocumentType: d.Type, SchemaVersion: spec.SchemaVersion,
		LatestVersion: strconv.Itoa(d.Versions[len(d.Versions)-1].Version), DefaultVersion: strconv.Itoa(d.Default),
		DocumentFormat: v.Format, TargetType: d.TargetType, Tags: []tag{}}
	for _, name := range core.SortedKeys(spec.Parameters) {
		p := spec.Parameters[name]
		def := ""
		switch x := p.Default.(type) {
		case nil:
		case string:
			def = x
		default:
			b, _ := json.Marshal(x)
			def = string(b)
		}
		out.Parameters = append(out.Parameters, docParam{name, p.Type, p.Description, def})
	}
	for _, k := range core.SortedKeys(d.Tags) {
		out.Tags = append(out.Tags, tag{k, d.Tags[k]})
	}
	return out
}

// checkContent validates the content of a document the way CreateDocument does.
func checkContent(typ, content, format string) error {
	if content == "" {
		return validation("1 validation error detected: Value at 'content' failed to satisfy constraint: Member must have length greater than or equal to 1")
	}
	spec, err := parseDoc(content, format)
	if err != nil {
		return err
	}
	if (typ == "Command" || typ == "Automation" || typ == "Session") && format != "TEXT" && spec.SchemaVersion == "" {
		return core.Errorf(400, "InvalidDocumentContent", "The content for the document is not valid: schemaVersion is required.")
	}
	return nil
}

func (s *Service) createDocument(c *core.Ctx, in *struct {
	Name, Content, DocumentType, DocumentFormat, TargetType, VersionName, DisplayName string
	Tags                                                                              []tag
}) (any, error) {
	typ, format := orDefault(in.DocumentType, "Command"), orDefault(in.DocumentFormat, "JSON")
	low := strings.ToLower(in.Name)
	switch {
	case !docNameRe.MatchString(in.Name) || strings.HasPrefix(low, "aws") || strings.HasPrefix(low, "amazon") || strings.HasPrefix(low, "amzn"):
		return nil, core.Errorf(400, "ValidationException", "Document name %s is invalid: use 3 to 128 letters, numbers and _ - . : /, and not a name that starts with aws, amazon or amzn.", in.Name)
	case !slices.Contains(docTypes, typ):
		return nil, validation("1 validation error detected: Value '%s' at 'documentType' failed to satisfy constraint: Member must satisfy enum value set: %v", typ, docTypes)
	case !slices.Contains([]string{"JSON", "YAML", "TEXT"}, format):
		return nil, validation("1 validation error detected: Value '%s' at 'documentFormat' failed to satisfy constraint: Member must satisfy enum value set: [YAML, TEXT, JSON]", format)
	}
	if err := checkContent(typ, in.Content, format); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.Name)
	if s.st.Docs[k] != nil {
		return nil, core.Errorf(400, "DocumentAlreadyExists", "The specified document already exists.")
	}
	d := &Document{Name: in.Name, Account: c.Account, Region: c.Region, Type: typ, TargetType: in.TargetType, Default: 1, Tags: map[string]string{},
		Versions: []*DocVersion{{Version: 1, Name: in.VersionName, Content: in.Content, Format: format, Created: time.Now().UTC()}}}
	for _, t := range in.Tags {
		d.Tags[t.Key] = t.Value
	}
	s.st.Docs[k] = d
	return map[string]any{"DocumentDescription": d.describe(d.Versions[0])}, nil
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}

// owned finds a document of the caller that they may change. Lock held.
func (s *Service) ownedDoc(c *core.Ctx, name string) (*Document, error) {
	d := s.document(c, name)
	switch {
	case d == nil:
		return nil, errNoDocument(name)
	case d.Owner == "Amazon":
		return nil, core.Errorf(400, "InvalidDocumentOperation", "You can't change the AWS-owned document %s.", name)
	}
	return d, nil
}

func (s *Service) updateDocument(c *core.Ctx, in *struct {
	Name, Content, DocumentVersion, DocumentFormat, TargetType, VersionName string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.ownedDoc(c, in.Name)
	if err != nil {
		return nil, err
	}
	latest := d.Versions[len(d.Versions)-1]
	format := orDefault(in.DocumentFormat, latest.Format)
	if !slices.Contains([]string{"JSON", "YAML", "TEXT"}, format) {
		return nil, validation("1 validation error detected: Value '%s' at 'documentFormat' failed to satisfy constraint: Member must satisfy enum value set: [YAML, TEXT, JSON]", format)
	}
	if err := checkContent(d.Type, in.Content, format); err != nil {
		return nil, err
	}
	switch {
	case in.DocumentVersion != "" && in.DocumentVersion != "$LATEST" && in.DocumentVersion != strconv.Itoa(latest.Version):
		return nil, core.Errorf(400, "InvalidDocumentVersion", "The document version is not valid or does not exist.")
	case in.Content == latest.Content && format == latest.Format:
		return nil, core.Errorf(400, "DuplicateDocumentContent", "The content of the association document matches another document. Change the content of the document and try again.")
	case in.VersionName != "" && slices.ContainsFunc(d.Versions, func(v *DocVersion) bool { return v.Name == in.VersionName }):
		return nil, core.Errorf(400, "DuplicateDocumentVersionName", "The specified version name is a duplicate.")
	case len(d.Versions) >= 1000:
		return nil, core.Errorf(400, "DocumentVersionLimitExceeded", "The document has too many versions. Delete one and try again.")
	}
	v := &DocVersion{Version: latest.Version + 1, Name: in.VersionName, Content: in.Content, Format: format, Created: time.Now().UTC()}
	d.Versions = append(d.Versions, v)
	if in.TargetType != "" {
		d.TargetType = in.TargetType
	}
	return map[string]any{"DocumentDescription": d.describe(v)}, nil
}

func (s *Service) updateDocumentDefaultVersion(c *core.Ctx, in *struct{ Name, DocumentVersion string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.ownedDoc(c, in.Name)
	if err != nil {
		return nil, err
	}
	v := d.version(in.DocumentVersion, "")
	if in.DocumentVersion == "" || v == nil {
		return nil, core.Errorf(400, "InvalidDocumentVersion", "The document version is not valid or does not exist.")
	}
	d.Default = v.Version
	return map[string]any{"Description": map[string]any{"Name": d.Name, "DefaultVersion": strconv.Itoa(v.Version), "DefaultVersionName": v.Name}}, nil
}

// pick finds a document and one of its versions for the read operations. Lock held.
func (s *Service) pick(c *core.Ctx, name, version, versionName string) (*Document, *DocVersion, error) {
	d := s.document(c, name)
	if d == nil {
		return nil, nil, errNoDocument(name)
	}
	v := d.version(version, versionName)
	if v == nil {
		return nil, nil, core.Errorf(400, "InvalidDocument", "Document with name %s and version %s does not exist.", name, orDefault(version, versionName))
	}
	return d, v, nil
}

func (s *Service) describeDocument(c *core.Ctx, in *struct{ Name, DocumentVersion, VersionName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, v, err := s.pick(c, in.Name, in.DocumentVersion, in.VersionName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Document": d.describe(v)}, nil
}

func (s *Service) getDocument(c *core.Ctx, in *struct{ Name, DocumentVersion, VersionName, DocumentFormat string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, v, err := s.pick(c, in.Name, in.DocumentVersion, in.VersionName)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"Name": d.Name, "CreatedDate": core.Time(v.Created), "DocumentVersion": strconv.Itoa(v.Version), "Status": "Active",
		"Content": v.Content, "DocumentType": d.Type, "DocumentFormat": v.Format}
	if v.Name != "" {
		out["VersionName"] = v.Name
	}
	return out, nil
}

type docFilter struct {
	Key    string
	Values []string
}

func (s *Service) listDocuments(c *core.Ctx, in *struct {
	DocumentFilterList []struct{ Key, Value string }
	Filters            []docFilter
	MaxResults         int
	NextToken          string
}) (any, error) {
	filters := slices.Clone(in.Filters)
	for _, f := range in.DocumentFilterList { // the older filter list, keys Name, Owner, PlatformTypes and DocumentType
		filters = append(filters, docFilter{f.Key, []string{f.Value}})
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*Document
	for _, d := range builtinDocs {
		all = append(all, d)
	}
	for _, d := range s.st.Docs {
		if d.Account == c.Account && d.Region == c.Region {
			all = append(all, d)
		}
	}
	for _, d := range s.st.Docs { // documents others shared with the caller
		if d.Account != c.Account && d.Region == c.Region && slices.Contains(d.Shared, c.Account) {
			all = append(all, d)
		}
	}
	slices.SortFunc(all, func(a, b *Document) int { return strings.Compare(a.Name, b.Name) })
	match := func(d *Document, f docFilter) bool {
		spec, _ := parseDoc(d.Versions[d.index(d.Default)].Content, d.Versions[d.index(d.Default)].Format)
		switch k := f.Key; {
		case k == "Name":
			return slices.Contains(f.Values, d.Name)
		case k == "Owner":
			return slices.ContainsFunc(f.Values, func(v string) bool {
				switch v {
				case "Self":
					return d.Owner == "" && d.Account == c.Account
				case "Amazon":
					return d.Owner == "Amazon"
				case "Private":
					return d.Owner == "" && slices.Contains(d.Shared, c.Account) || d.Owner == "" && d.Account == c.Account
				}
				return false
			})
		case k == "DocumentType":
			return slices.Contains(f.Values, d.Type)
		case k == "PlatformTypes" && spec != nil:
			return slices.ContainsFunc(platforms(spec), func(p string) bool { return slices.Contains(f.Values, p) })
		case k == "TagKey":
			return slices.ContainsFunc(f.Values, func(v string) bool { _, ok := d.Tags[v]; return ok })
		case strings.HasPrefix(k, "tag:"):
			v, ok := d.Tags[strings.TrimPrefix(k, "tag:")]
			return ok && slices.Contains(f.Values, v)
		}
		return false
	}
	var list []*Document
	for _, d := range all {
		if !slices.ContainsFunc(filters, func(f docFilter) bool { return !match(d, f) }) {
			list = append(list, d)
		}
	}
	start, end, next, err := page(len(list), in.NextToken, in.MaxResults, 10)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, d := range list[start:end] {
		v := d.Versions[d.index(d.Default)]
		desc := d.describe(v)
		m := map[string]any{"Name": d.Name, "CreatedDate": desc.CreatedDate, "Owner": desc.Owner, "PlatformTypes": desc.PlatformTypes,
			"DocumentVersion": desc.DefaultVersion, "DocumentType": d.Type, "SchemaVersion": desc.SchemaVersion, "DocumentFormat": v.Format, "Tags": desc.Tags}
		if v.Name != "" {
			m["VersionName"] = v.Name
		}
		if d.TargetType != "" {
			m["TargetType"] = d.TargetType
		}
		out = append(out, m)
	}
	return map[string]any{"DocumentIdentifiers": out, "NextToken": next}, nil
}

func (s *Service) listDocumentVersions(c *core.Ctx, in *struct {
	Name       string
	MaxResults int
	NextToken  string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.document(c, in.Name)
	if d == nil {
		return nil, errNoDocument(in.Name)
	}
	start, end, next, err := page(len(d.Versions), in.NextToken, in.MaxResults, 50)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, v := range d.Versions[start:end] {
		m := map[string]any{"Name": d.Name, "DocumentVersion": strconv.Itoa(v.Version), "CreatedDate": core.Time(v.Created),
			"IsDefaultVersion": v.Version == d.Default, "DocumentFormat": v.Format, "Status": "Active"}
		if v.Name != "" {
			m["VersionName"] = v.Name
		}
		out = append(out, m)
	}
	return map[string]any{"DocumentVersions": out, "NextToken": next}, nil
}

func (s *Service) deleteDocument(c *core.Ctx, in *struct {
	Name, DocumentVersion, VersionName string
	Force                              bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.ownedDoc(c, in.Name)
	if err != nil {
		return nil, err
	}
	if in.DocumentVersion == "" && in.VersionName == "" {
		for _, a := range s.st.Assocs {
			if a.Name == d.Name && a.Account == d.Account && a.Region == d.Region && !in.Force {
				return nil, core.Errorf(400, "AssociatedInstances", "You must disassociate the document from all instances before you delete it.")
			}
		}
		delete(s.st.Docs, key(d.Account, d.Region, d.Name))
		return map[string]any{}, nil
	}
	v := d.version(in.DocumentVersion, in.VersionName)
	switch {
	case v == nil:
		return nil, core.Errorf(400, "InvalidDocumentVersion", "The document version is not valid or does not exist.")
	case v.Version == d.Default:
		return nil, core.Errorf(400, "InvalidDocumentOperation", "You can't delete the default version of a document. Make another version the default first.")
	}
	d.Versions = slices.DeleteFunc(d.Versions, func(x *DocVersion) bool { return x == v })
	return map[string]any{}, nil
}

func (s *Service) describeDocumentPermission(c *core.Ctx, in *struct{ Name, PermissionType string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.ownedDoc(c, in.Name)
	if err != nil {
		return nil, err
	}
	if in.PermissionType != "Share" {
		return nil, validation("1 validation error detected: Value '%s' at 'permissionType' failed to satisfy constraint: Member must satisfy enum value set: [Share]", in.PermissionType)
	}
	ids, info := []string{}, []map[string]any{}
	for _, a := range d.Shared {
		ids = append(ids, a)
		info = append(info, map[string]any{"AccountId": a, "SharedDocumentVersion": "$DEFAULT"})
	}
	return map[string]any{"AccountIds": ids, "AccountSharingInfoList": info}, nil
}

var accountRe = regexp.MustCompile(`^(\d{12}|all)$`)

func (s *Service) modifyDocumentPermission(c *core.Ctx, in *struct {
	Name, PermissionType, SharedDocumentVersion string
	AccountIdsToAdd, AccountIdsToRemove         []string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.ownedDoc(c, in.Name)
	if err != nil {
		return nil, err
	}
	if in.PermissionType != "Share" {
		return nil, validation("1 validation error detected: Value '%s' at 'permissionType' failed to satisfy constraint: Member must satisfy enum value set: [Share]", in.PermissionType)
	}
	for _, a := range in.AccountIdsToAdd {
		if !accountRe.MatchString(a) {
			return nil, core.Errorf(400, "InvalidPermissionType", "The account id %s is not valid.", a)
		}
	}
	for _, a := range in.AccountIdsToAdd {
		if !slices.Contains(d.Shared, a) {
			d.Shared = append(d.Shared, a)
		}
	}
	d.Shared = slices.DeleteFunc(d.Shared, func(a string) bool { return slices.Contains(in.AccountIdsToRemove, a) })
	return map[string]any{}, nil
}

// ---- associations ----

type target struct {
	Key    string
	Values []string
}

// Association ties a document to targets. pogi keeps the latest version only.
type Association struct {
	ID                      string              `json:"id"`
	Account                 string              `json:"account"`
	Region                  string              `json:"region"`
	Name                    string              `json:"name"`
	DocumentVersion         string              `json:"documentVersion,omitempty"`
	InstanceID              string              `json:"instanceId,omitempty"`
	Targets                 []target            `json:"targets"`
	Parameters              map[string][]string `json:"parameters,omitempty"`
	ScheduleExpression      string              `json:"scheduleExpression,omitempty"`
	AssociationName         string              `json:"associationName,omitempty"`
	ApplyOnlyAtCronInterval bool                `json:"applyOnlyAtCronInterval,omitempty"`
	ComplianceSeverity      string              `json:"complianceSeverity,omitempty"`
	SyncCompliance          string              `json:"syncCompliance,omitempty"`
	MaxConcurrency          string              `json:"maxConcurrency,omitempty"`
	MaxErrors               string              `json:"maxErrors,omitempty"`
	OutputLocation          json.RawMessage     `json:"outputLocation,omitempty"`
	Version                 int                 `json:"version"`
	Created                 time.Time           `json:"created"`
	Updated                 time.Time           `json:"updated"`
	Status                  string              `json:"status"` // Pending, Success or Failed
	LastRun                 *time.Time          `json:"lastRun,omitempty"`
	Tags                    map[string]string   `json:"tags,omitempty"`
}

type assocIn struct {
	Name, DocumentVersion, InstanceId, ScheduleExpression, AssociationName, ComplianceSeverity, SyncCompliance, MaxConcurrency, MaxErrors string
	Targets                                                                                                                               []target
	Parameters                                                                                                                            map[string][]string
	ApplyOnlyAtCronInterval                                                                                                               bool
	OutputLocation                                                                                                                        json.RawMessage
	Tags                                                                                                                                  []tag
}

func (a *Association) out() map[string]any {
	status := map[string]any{"Date": core.Time(a.Updated), "Name": "Associated", "Message": "Associated with " + a.Name}
	counts := map[string]int{}
	if a.Status != "Pending" {
		counts[a.Status] = 1
	}
	m := map[string]any{"Name": a.Name, "AssociationVersion": strconv.Itoa(a.Version), "Date": core.Time(a.Created), "LastUpdateAssociationDate": core.Time(a.Updated),
		"Status": status, "Overview": map[string]any{"Status": a.Status, "DetailedStatus": a.Status, "AssociationStatusAggregatedCount": counts},
		"AssociationId": a.ID, "Targets": orEmptyTargets(a.Targets), "ApplyOnlyAtCronInterval": a.ApplyOnlyAtCronInterval}
	for k, v := range map[string]string{"DocumentVersion": a.DocumentVersion, "InstanceId": a.InstanceID, "ScheduleExpression": a.ScheduleExpression,
		"AssociationName": a.AssociationName, "ComplianceSeverity": a.ComplianceSeverity, "SyncCompliance": a.SyncCompliance,
		"MaxConcurrency": a.MaxConcurrency, "MaxErrors": a.MaxErrors} {
		if v != "" {
			m[k] = v
		}
	}
	if len(a.Parameters) > 0 {
		m["Parameters"] = a.Parameters
	}
	if len(a.OutputLocation) > 0 {
		m["OutputLocation"] = a.OutputLocation
	}
	if a.LastRun != nil {
		m["LastExecutionDate"] = core.Time(*a.LastRun)
		if a.Status == "Success" {
			m["LastSuccessfulExecutionDate"] = core.Time(*a.LastRun)
		}
	}
	return m
}

func orEmptyTargets(t []target) []target {
	if t == nil {
		return []target{}
	}
	return t
}

func (s *Service) createAssociation(c *core.Ctx, in *assocIn) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.document(c, in.Name)
	switch {
	case d == nil:
		return nil, errNoDocument(in.Name)
	case in.InstanceId == "" && len(in.Targets) == 0:
		return nil, core.Errorf(400, "InvalidTarget", "The association must name an InstanceId or Targets.")
	case in.InstanceId != "" && len(in.Targets) > 0:
		return nil, validation("Use either InstanceId or Targets, not both.")
	case in.DocumentVersion != "" && in.DocumentVersion != "$LATEST" && in.DocumentVersion != "$DEFAULT" && d.version(in.DocumentVersion, "") == nil:
		return nil, core.Errorf(400, "InvalidDocumentVersion", "The document version is not valid or does not exist.")
	}
	a := &Association{ID: core.UUID(), Account: c.Account, Region: c.Region, Name: d.Name, DocumentVersion: in.DocumentVersion, InstanceID: in.InstanceId,
		Targets: in.Targets, Parameters: in.Parameters, ScheduleExpression: in.ScheduleExpression, AssociationName: in.AssociationName,
		ApplyOnlyAtCronInterval: in.ApplyOnlyAtCronInterval, ComplianceSeverity: in.ComplianceSeverity, SyncCompliance: in.SyncCompliance,
		MaxConcurrency: in.MaxConcurrency, MaxErrors: in.MaxErrors, OutputLocation: in.OutputLocation, Version: 1,
		Created: time.Now().UTC(), Status: "Pending", Tags: map[string]string{}}
	a.Updated = a.Created
	for _, t := range in.Tags {
		a.Tags[t.Key] = t.Value
	}
	for _, o := range s.st.Assocs {
		if o.Account == c.Account && o.Region == c.Region && o.sameAs(a) {
			return nil, core.Errorf(400, "AssociationAlreadyExists", "The specified association already exists.")
		}
	}
	s.st.Assocs[key(c.Account, c.Region, a.ID)] = a
	if !a.ApplyOnlyAtCronInterval {
		s.applyAssociation(c, a, d)
	}
	return map[string]any{"AssociationDescription": a.out()}, nil
}

// sameAs: an association with the same document, targets, parameters, schedule and name is a duplicate.
func (a *Association) sameAs(b *Association) bool {
	return a.Name == b.Name && a.InstanceID == b.InstanceID && a.ScheduleExpression == b.ScheduleExpression && a.AssociationName == b.AssociationName &&
		fmt.Sprint(a.Targets) == fmt.Sprint(b.Targets) && maps.EqualFunc(a.Parameters, b.Parameters, slices.Equal)
}

func (s *Service) assocOf(c *core.Ctx, id, name, instance string) (*Association, error) {
	if id != "" {
		if a := s.st.Assocs[key(c.Account, c.Region, id)]; a != nil {
			return a, nil
		}
	} else {
		for _, k := range core.SortedKeys(s.st.Assocs) {
			if a := s.st.Assocs[k]; a.Account == c.Account && a.Region == c.Region && a.Name == name && a.InstanceID == instance && instance != "" {
				return a, nil
			}
		}
	}
	return nil, core.Errorf(400, "AssociationDoesNotExist", "The specified association does not exist.")
}

func (s *Service) describeAssociation(c *core.Ctx, in *struct{ AssociationId, Name, InstanceId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.assocOf(c, in.AssociationId, in.Name, in.InstanceId)
	if err != nil {
		return nil, err
	}
	return map[string]any{"AssociationDescription": a.out()}, nil
}

func (s *Service) listAssociations(c *core.Ctx, in *struct {
	AssociationFilterList []struct{ Key, Value string }
	MaxResults            int
	NextToken             string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Association
	for _, k := range core.SortedKeys(s.st.Assocs) {
		a := s.st.Assocs[k]
		if a.Account != c.Account || a.Region != c.Region {
			continue
		}
		ok := true
		for _, f := range in.AssociationFilterList {
			switch f.Key {
			case "Name":
				ok = ok && a.Name == f.Value
			case "InstanceId":
				ok = ok && (a.InstanceID == f.Value || slices.ContainsFunc(a.Targets, func(t target) bool { return t.Key == "InstanceIds" && slices.Contains(t.Values, f.Value) }))
			case "AssociationId":
				ok = ok && a.ID == f.Value
			case "AssociationStatusName":
				ok = ok && a.Status == f.Value
			case "AssociationName":
				ok = ok && a.AssociationName == f.Value
			}
		}
		if ok {
			list = append(list, a)
		}
	}
	start, end, next, err := page(len(list), in.NextToken, in.MaxResults, 50)
	if err != nil {
		return nil, err
	}
	out := []map[string]any{}
	for _, a := range list[start:end] {
		m := map[string]any{"Name": a.Name, "AssociationId": a.ID, "AssociationVersion": strconv.Itoa(a.Version), "Targets": orEmptyTargets(a.Targets),
			"LastExecutionDate": core.Time(a.Created), "Overview": a.out()["Overview"]}
		for k, v := range map[string]string{"DocumentVersion": a.DocumentVersion, "InstanceId": a.InstanceID, "ScheduleExpression": a.ScheduleExpression, "AssociationName": a.AssociationName} {
			if v != "" {
				m[k] = v
			}
		}
		out = append(out, m)
	}
	return map[string]any{"Associations": out, "NextToken": next}, nil
}

func (s *Service) updateAssociation(c *core.Ctx, in *struct {
	AssociationId string
	assocIn
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.assocOf(c, in.AssociationId, "", "")
	if err != nil {
		return nil, err
	}
	for _, f := range []struct {
		in string
		to *string
	}{{in.DocumentVersion, &a.DocumentVersion}, {in.ScheduleExpression, &a.ScheduleExpression}, {in.AssociationName, &a.AssociationName},
		{in.ComplianceSeverity, &a.ComplianceSeverity}, {in.SyncCompliance, &a.SyncCompliance}, {in.MaxConcurrency, &a.MaxConcurrency}, {in.MaxErrors, &a.MaxErrors}} {
		if f.in != "" {
			*f.to = f.in
		}
	}
	if in.Targets != nil {
		a.Targets = in.Targets
	}
	if in.Parameters != nil {
		a.Parameters = in.Parameters
	}
	if in.OutputLocation != nil {
		a.OutputLocation = in.OutputLocation
	}
	if in.Name != "" {
		if s.document(c, in.Name) == nil {
			return nil, errNoDocument(in.Name)
		}
		a.Name = in.Name
	}
	a.ApplyOnlyAtCronInterval = in.ApplyOnlyAtCronInterval
	a.Version++
	a.Updated = time.Now().UTC()
	return map[string]any{"AssociationDescription": a.out()}, nil
}

func (s *Service) deleteAssociation(c *core.Ctx, in *struct{ AssociationId, Name, InstanceId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.assocOf(c, in.AssociationId, in.Name, in.InstanceId)
	if err != nil {
		return nil, err
	}
	delete(s.st.Assocs, key(a.Account, a.Region, a.ID))
	return map[string]any{}, nil
}
