package appconfig

import (
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// tokenTTL is how long a poll token stays good, as on AWS.
const tokenTTL = 24 * time.Hour

// session is an AppConfigData configuration session.
type session struct {
	Account  string    `json:"account"`
	Region   string    `json:"region"`
	App      string    `json:"app"`
	Env      string    `json:"env"`
	Profile  string    `json:"profile"`
	Interval int       `json:"interval"` // seconds between polls
	Seen     string    `json:"seen"`     // hash of the content the client has
	Issued   time.Time `json:"issued"`   // of its current token
}

func (s *Service) startSession(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		ApplicationIdentifier, EnvironmentIdentifier, ConfigurationProfileIdentifier string
		RequiredMinimumPollIntervalInSeconds                                         *int
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	interval := 60
	if p := in.RequiredMinimumPollIntervalInSeconds; p != nil {
		if *p < 15 || *p > 86400 {
			return nil, badRequest("RequiredMinimumPollIntervalInSeconds %d is not between 15 and 86400.", *p)
		}
		interval = *p
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, e, err := s.env(c, in.ApplicationIdentifier, in.EnvironmentIdentifier)
	if err != nil {
		return nil, err
	}
	_, p, err := s.profile(c, a.Id, in.ConfigurationProfileIdentifier)
	if err != nil {
		return nil, err
	}
	tok := core.RandHex(32)
	s.st.Sessions[tok] = &session{c.Account, c.Region, a.Id, e.Id, p.Id, interval, "", time.Now().UTC()}
	return map[string]string{"InitialConfigurationToken": tok}, nil
}

// latest is GetLatestConfiguration: what is deployed, an empty body when the
// session has it already, and the next token. Tokens are single-use.
func (s *Service) latest(c *core.Ctx, r *http.Request) (any, error) {
	tok := r.URL.Query().Get("configuration_token")
	s.mu.Lock()
	defer s.mu.Unlock()
	ss := s.st.Sessions[tok]
	if ss == nil || time.Since(ss.Issued) > tokenTTL {
		return nil, badRequest("The configuration token is invalid, expired or already used.")
	}
	_, e, err := s.env(&core.Ctx{Account: ss.Account, Region: ss.Region}, ss.App, ss.Env)
	if err != nil {
		return nil, err
	}
	d := e.live(ss.Profile)
	if d == nil {
		return nil, notFound("Configuration profile %s has no deployment to environment %s.", ss.Profile, ss.Env)
	}
	next := core.RandHex(32)
	delete(s.st.Sessions, tok)
	s.st.Sessions[next], ss.Issued = ss, time.Now().UTC()
	h := http.Header{}
	h.Set("Next-Poll-Configuration-Token", next)
	h.Set("Next-Poll-Interval-In-Seconds", strconv.Itoa(ss.Interval))
	sum := sha256.Sum256(append([]byte(d.ContentType+"\n"), d.Content...))
	if seen := hex.EncodeToString(sum[:]); seen != ss.Seen {
		ss.Seen = seen
		h.Set("Content-Type", d.ContentType)
		if wantsIon(r.Header.Get("Accept")) && d.ContentType == "application/json" {
			// ponytail: Ion text, which reads JSON as it is, not Ion binary; any Ion reader accepts both.
			h.Set("Content-Type", "application/ion")
		}
		if d.VersionLabel != "" {
			h.Set("Version-Label", d.VersionLabel)
		}
		return raw{h, d.Content}, nil
	}
	return raw{h, nil}, nil
}

// wantsIon: the Accept header lists application/ion, with or without a type parameter, quoted or not.
func wantsIon(accept string) bool {
	for _, part := range strings.Split(accept, ",") {
		if mt, _, err := mime.ParseMediaType(strings.TrimSpace(part)); err == nil && mt == "application/ion" {
			return true
		}
	}
	return false
}

// getConfiguration is the deprecated GetConfiguration: what is deployed, or
// nothing when the client says it has that version.
func (s *Service) getConfiguration(c *core.Ctx, r *http.Request) (any, error) {
	q := r.URL.Query()
	if q.Get("client_id") == "" {
		return nil, badRequest("ClientId is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, e, err := s.envOf(c, r)
	if err != nil {
		return nil, err
	}
	_, p, err := s.profile(c, a.Id, r.PathValue("Configuration"))
	if err != nil {
		return nil, err
	}
	d := e.live(p.Id)
	if d == nil {
		return nil, notFound("Configuration profile %s has no deployment to environment %s.", p.Id, e.Id)
	}
	h := http.Header{}
	h.Set("Configuration-Version", d.ConfigurationVersion)
	h.Set("Content-Type", d.ContentType)
	if q.Get("client_configuration_version") == d.ConfigurationVersion {
		return raw{h, nil}, nil
	}
	return raw{h, d.Content}, nil
}

// validateConfiguration runs a profile's validators over one of its versions.
func (s *Service) validateConfiguration(c *core.Ctx, r *http.Request) (any, error) {
	version := r.URL.Query().Get("configuration_version")
	s.mu.Lock()
	_, p, err := s.profileOf(c, r)
	var prof ConfigurationProfile
	if err == nil {
		prof = p.ConfigurationProfile
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	got, err := s.fetch(c, prof, version)
	if err != nil {
		return nil, err
	}
	return nil, s.check(c, prof, version, got.content)
}

// fetched is a configuration version as read from its store.
type fetched struct {
	content            []byte
	contentType, label string
}

// fetch reads a configuration version from where its profile says it lives:
// the hosted store, SSM Parameter Store, Secrets Manager or S3. Call it
// without the lock.
// ponytail: SSM documents are not emulated; ssm-document:// profiles deploy empty content.
func (s *Service) fetch(c *core.Ctx, p ConfigurationProfile, version string) (fetched, error) {
	kind, name, _ := strings.Cut(p.LocationUri, "://")
	failed := func(err error) (fetched, error) {
		return fetched{}, badRequest("Could not read version %s of %s: %v", version, p.LocationUri, err)
	}
	switch kind {
	case "hosted":
		s.mu.Lock()
		defer s.mu.Unlock()
		_, hp, err := s.profile(c, p.ApplicationId, p.Id)
		if err != nil {
			return fetched{}, err
		}
		v, err := hp.version(version)
		if err != nil {
			return fetched{}, err
		}
		return fetched{v.Content, v.ContentType, v.VersionLabel}, nil
	case "ssm-parameter":
		if version != "" {
			name += ":" + version
		}
		var out struct{ Parameter struct{ Value string } }
		if err := s.hub.Call(c.Account, c.Region, "AmazonSSM", "GetParameter", map[string]any{"Name": name, "WithDecryption": true}, &out); err != nil {
			return failed(err)
		}
		return fetched{[]byte(out.Parameter.Value), "text/plain", ""}, nil
	case "secretsmanager":
		in := map[string]string{"SecretId": name}
		if version != "" {
			in["VersionId"] = version
		}
		var out struct {
			SecretString string
			SecretBinary []byte
		}
		if err := s.hub.Call(c.Account, c.Region, "secretsmanager", "GetSecretValue", in, &out); err != nil {
			return failed(err)
		}
		if out.SecretBinary != nil {
			return fetched{out.SecretBinary, "application/octet-stream", ""}, nil
		}
		return fetched{[]byte(out.SecretString), "text/plain", ""}, nil
	case "s3":
		path := (&url.URL{Path: "/" + name}).EscapedPath()
		if version != "" && version != "null" {
			path += "?versionId=" + url.QueryEscape(version)
		}
		status, h, body := s.hub.Do(c.Account, c.Region, "s3", http.MethodGet, path, nil, nil)
		if status != http.StatusOK {
			return failed(fmt.Errorf("S3 answered %d: %s", status, body))
		}
		return fetched{body, cmp.Or(h.Get("Content-Type"), "application/octet-stream"), ""}, nil
	}
	return fetched{nil, "text/plain", ""}, nil
}

// check runs a profile's validators over content; call it without the lock.
func (s *Service) check(c *core.Ctx, p ConfigurationProfile, version string, content []byte) error {
	for _, v := range p.Validators {
		var problems []string
		switch v.Type {
		case "JSON_SCHEMA":
			var schema, doc any
			json.Unmarshal([]byte(v.Content), &schema) // checked when the profile was saved
			if err := json.Unmarshal(content, &doc); err != nil {
				problems = []string{"the content is not JSON: " + err.Error()}
			} else {
				problems = conform(schema, doc, "$")
			}
		case "LAMBDA":
			event, _ := json.Marshal(map[string]string{"applicationId": p.ApplicationId, "configurationProfileId": p.Id,
				"configurationVersion": version, "content": base64.StdEncoding.EncodeToString(content), "uri": p.LocationUri})
			if _, err := s.hub.Invoke(v.Content, event); err != nil {
				problems = []string{err.Error()}
			}
		}
		if len(problems) > 0 {
			return badRequest("Version %s failed %s validation: %s", version, v.Type, strings.Join(problems, "; "))
		}
	}
	return nil
}

// flagValues turns a feature flag document into what clients get: each flag
// with "enabled" and its attribute values, without the "_" metadata.
func flagValues(doc []byte) ([]byte, error) {
	var in struct {
		Flags  map[string]json.RawMessage
		Values map[string]map[string]any
	}
	if err := json.Unmarshal(doc, &in); err != nil {
		return nil, err
	}
	if in.Flags == nil {
		return nil, errors.New(`"flags" is missing`)
	}
	out := map[string]map[string]any{}
	for name := range in.Flags {
		v := map[string]any{"enabled": false}
		for k, x := range in.Values[name] {
			if !strings.HasPrefix(k, "_") {
				v[k] = x
			}
		}
		out[name] = v
	}
	return json.Marshal(out)
}

// conform checks doc against a JSON schema and lists what does not match.
// ponytail: a subset: type, enum, required, properties, items and the
// minimum/maximum, minLength/maxLength and minItems/maxItems bounds; $ref,
// combinators, additionalProperties, patterns and formats pass unchecked.
func conform(schema, doc any, at string) []string {
	sc, _ := schema.(map[string]any)
	if t, ok := sc["type"]; ok && !typed(t, doc) {
		return []string{fmt.Sprintf("%s is not of type %v", at, t)}
	}
	var out []string
	if e, ok := sc["enum"].([]any); ok && !slices.ContainsFunc(e, func(x any) bool { return reflect.DeepEqual(x, doc) }) {
		out = append(out, fmt.Sprintf("%s is not one of %v", at, e))
	}
	bound := func(n float64, lo, hi string) {
		if m, ok := sc[lo].(float64); ok && n < m {
			out = append(out, fmt.Sprintf("%s is under its %s, %v", at, lo, m))
		}
		if m, ok := sc[hi].(float64); ok && n > m {
			out = append(out, fmt.Sprintf("%s is over its %s, %v", at, hi, m))
		}
	}
	switch v := doc.(type) {
	case float64:
		bound(v, "minimum", "maximum")
	case string:
		bound(float64(utf8.RuneCountInString(v)), "minLength", "maxLength")
	case []any:
		bound(float64(len(v)), "minItems", "maxItems")
		for i, x := range v {
			out = append(out, conform(sc["items"], x, at+"["+strconv.Itoa(i)+"]")...)
		}
	case map[string]any:
		req, _ := sc["required"].([]any)
		for _, k := range req {
			if name, _ := k.(string); !hasKey(v, name) {
				out = append(out, fmt.Sprintf("%s.%s is required", at, name))
			}
		}
		props, _ := sc["properties"].(map[string]any)
		for _, k := range core.SortedKeys(props) {
			if x, ok := v[k]; ok {
				out = append(out, conform(props[k], x, at+"."+k)...)
			}
		}
	}
	return out
}

func hasKey(m map[string]any, k string) bool {
	_, ok := m[k]
	return ok
}

// typed reports whether v has the JSON schema type t, a name or a list of names.
func typed(t, v any) bool {
	names, _ := t.([]any)
	if n, ok := t.(string); ok {
		names = []any{n}
	}
	for _, n := range names {
		switch v := v.(type) {
		case nil:
			if n == "null" {
				return true
			}
		case bool:
			if n == "boolean" {
				return true
			}
		case float64:
			if n == "number" || n == "integer" && v == math.Trunc(v) {
				return true
			}
		case string:
			if n == "string" {
				return true
			}
		case []any:
			if n == "array" {
				return true
			}
		case map[string]any:
			if n == "object" {
				return true
			}
		}
	}
	return false
}
