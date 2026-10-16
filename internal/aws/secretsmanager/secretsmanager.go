// Package secretsmanager emulates AWS Secrets Manager over JSON 1.1: secrets
// with versions and staging labels, rotation through a Lambda function,
// random passwords, resource policies, tags, replica metadata and scheduled
// deletion.
package secretsmanager

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"math/big"
	"net/http"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the Secrets Manager emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
	mu  sync.Mutex
	st  state
}

type state struct {
	Secrets map[string]*Secret `json:"secrets"` // account/region/name
}

// Secret is one secret and its versions.
type Secret struct {
	Name           string              `json:"name"`
	Account        string              `json:"account"`
	Region         string              `json:"region"`
	ARN            string              `json:"arn"`
	Description    string              `json:"description,omitempty"`
	KmsKeyID       string              `json:"kmsKeyId,omitempty"`
	Tags           []Tag               `json:"tags,omitempty"`
	Versions       map[string]*Version `json:"versions"`
	Policy         string              `json:"policy,omitempty"`
	Replicas       []Replica           `json:"replicas,omitempty"`
	Rotation       bool                `json:"rotation,omitempty"`
	RotationLambda string              `json:"rotationLambda,omitempty"`
	RotationRules  *RotationRules      `json:"rotationRules,omitempty"`
	Created        time.Time           `json:"created"`
	Changed        time.Time           `json:"changed"`
	Accessed       time.Time           `json:"accessed,omitzero"`
	Rotated        time.Time           `json:"rotated,omitzero"`
	Deleted        time.Time           `json:"deleted,omitzero"` // when DeleteSecret was called
	PurgeAt        time.Time           `json:"purgeAt,omitzero"` // end of the recovery window
}

// Version is one secret value and its staging labels. A version without a
// value is the placeholder RotateSecret creates for the rotation function.
type Version struct {
	ID       string    `json:"id"`
	String   *string   `json:"string,omitempty"`
	Binary   []byte    `json:"binary,omitempty"`
	Stages   []string  `json:"stages"`
	Created  time.Time `json:"created"`
	Accessed time.Time `json:"accessed,omitzero"`
}

// Tag is a secret tag (API shape).
type Tag struct {
	Key   string
	Value string
}

// RotationRules is the rotation schedule (API shape).
type RotationRules struct {
	AutomaticallyAfterDays int64  `json:",omitempty"`
	Duration               string `json:",omitempty"`
	ScheduleExpression     string `json:",omitempty"`
}

// Replica is the replication status of one region (API shape).
type Replica struct {
	Region        string
	KmsKeyId      string `json:",omitempty"`
	Status        string `json:",omitempty"`
	StatusMessage string `json:",omitempty"`
}

// New creates the Secrets Manager service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.Reset()
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"CreateSecret":                 core.H(s.createSecret),
		"GetSecretValue":               core.H(s.getSecretValue),
		"BatchGetSecretValue":          core.H(s.batchGetSecretValue),
		"PutSecretValue":               core.H(s.putSecretValue),
		"UpdateSecret":                 core.H(s.updateSecret),
		"UpdateSecretVersionStage":     core.H(s.updateSecretVersionStage),
		"DescribeSecret":               core.H(s.describeSecret),
		"ListSecrets":                  core.H(s.listSecrets),
		"ListSecretVersionIds":         core.H(s.listSecretVersionIds),
		"DeleteSecret":                 core.H(s.deleteSecret),
		"RestoreSecret":                core.H(s.restoreSecret),
		"TagResource":                  core.H(s.tagResource),
		"UntagResource":                core.H(s.untagResource),
		"GetRandomPassword":            core.H(s.getRandomPassword),
		"GetResourcePolicy":            core.H(s.getResourcePolicy),
		"PutResourcePolicy":            core.H(s.putResourcePolicy),
		"DeleteResourcePolicy":         core.H(s.deleteResourcePolicy),
		"ValidateResourcePolicy":       core.H(s.validateResourcePolicy),
		"RotateSecret":                 core.H(s.rotateSecret),
		"CancelRotateSecret":           core.H(s.cancelRotateSecret),
		"ReplicateSecretToRegions":     core.H(s.replicateSecretToRegions),
		"RemoveRegionsFromReplication": core.H(s.removeRegionsFromReplication),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "secretsmanager", Cloud: core.AWS, Title: "Secrets Manager", Targets: []string{"secretsmanager"},
		Signing: []string{"secretsmanager"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = state{Secrets: map[string]*Secret{}}
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Secrets == nil {
		st.Secrets = map[string]*Secret{}
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func key(account, region, name string) string { return account + "/" + region + "/" + name }

func invalidParam(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

func invalidReq(format string, a ...any) error {
	return core.Errorf(400, "InvalidRequestException", format, a...)
}

// find resolves a SecretId (name, ARN, or ARN without the random suffix);
// live refuses secrets scheduled for deletion. Lock held.
func (s *Service) find(c *core.Ctx, id string, live bool) (*Secret, error) {
	account, region, name := c.Account, c.Region, id
	if _, r, a, res, ok := core.ParseARN(id); ok {
		account, region, name = a, r, strings.TrimPrefix(res, "secret:")
		if i := strings.LastIndex(name, "-"); i > 0 {
			if sec := s.st.Secrets[key(a, r, name[:i])]; sec != nil && sec.ARN == id {
				name = sec.Name
			}
		}
	}
	k := key(account, region, name)
	sec := s.st.Secrets[k]
	if sec != nil && !sec.PurgeAt.IsZero() && time.Now().After(sec.PurgeAt) {
		delete(s.st.Secrets, k)
		sec = nil
	}
	if sec == nil {
		return nil, core.Errorf(400, "ResourceNotFoundException", "Secrets Manager can't find the specified secret.")
	}
	if live && !sec.Deleted.IsZero() {
		return nil, invalidReq("You can't perform this operation on the secret because it was marked for deletion.")
	}
	return sec, nil
}

// result is the small output most operations share.
type result struct {
	ARN               string
	Name              string    `json:",omitempty"`
	VersionId         string    `json:",omitempty"`
	VersionStages     []string  `json:",omitempty"`
	DeletionDate      core.Time `json:",omitzero"`
	ReplicationStatus []Replica `json:",omitempty"`
}

// update runs fn on a secret under the lock and answers with its ARN and name.
func (s *Service) update(c *core.Ctx, id string, live bool, fn func(*Secret) error) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, id, live)
	if err != nil {
		return nil, err
	}
	if err := fn(sec); err != nil {
		return nil, err
	}
	return &result{ARN: sec.ARN, Name: sec.Name}, nil
}

// ---- versions and staging labels ----

func (sec *Secret) byStage(label string) *Version {
	for _, v := range sec.Versions {
		if slices.Contains(v.Stages, label) {
			return v
		}
	}
	return nil
}

func drop(list []string, label string) []string {
	return slices.DeleteFunc(list, func(l string) bool { return l == label })
}

// stage moves a staging label to v; moving AWSCURRENT leaves AWSPREVIOUS on
// the version that had it. Lock held.
func (sec *Secret) stage(label string, v *Version) {
	for _, o := range sec.Versions {
		if o != v && slices.Contains(o.Stages, label) {
			o.Stages = drop(o.Stages, label)
			if label == "AWSCURRENT" {
				sec.stage("AWSPREVIOUS", o)
			}
		}
	}
	if !slices.Contains(v.Stages, label) {
		v.Stages = append(v.Stages, label)
	}
	sec.Changed = time.Now().UTC()
}

// put stores a value as version token (idempotent for the same value) and
// applies its staging labels, AWSCURRENT by default. Lock held.
func (sec *Secret) put(token string, str *string, bin []byte, stages []string) (*Version, error) {
	if token == "" {
		token = core.UUID()
	}
	if len(stages) == 0 {
		stages = []string{"AWSCURRENT"}
	}
	v := sec.Versions[token]
	switch {
	case v == nil:
		v = &Version{ID: token, Stages: []string{}, Created: time.Now().UTC()}
		sec.Versions[token] = v
	case v.String == nil && v.Binary == nil: // the rotation placeholder gets its value
	case (v.String == nil) == (str == nil) && (str == nil || *v.String == *str) && bytes.Equal(v.Binary, bin):
		return v, nil
	default:
		return nil, core.Errorf(400, "ResourceExistsException", "You can't modify an existing version, you can only create a new version.")
	}
	v.String, v.Binary = str, bin
	for _, l := range stages {
		sec.stage(l, v)
	}
	// ponytail: unlabeled versions beyond 100 are dropped right away; AWS does it eventually.
	for len(sec.Versions) > 100 {
		var old *Version
		for _, o := range sec.Versions {
			if len(o.Stages) == 0 && (old == nil || o.Created.Before(old.Created)) {
				old = o
			}
		}
		if old == nil {
			break
		}
		delete(sec.Versions, old.ID)
	}
	return v, nil
}

// version picks a version by id and/or staging label (AWSCURRENT by default). Lock held.
func (sec *Secret) version(id, stage string) (*Version, error) {
	if id == "" && stage == "" {
		stage = "AWSCURRENT"
	}
	v := sec.byStage(stage)
	if id != "" {
		v = sec.Versions[id]
	}
	if v == nil || stage != "" && !slices.Contains(v.Stages, stage) || v.String == nil && v.Binary == nil {
		what := "VersionId: " + id
		if id == "" {
			what = "staging label: " + stage
		}
		return nil, core.Errorf(400, "ResourceNotFoundException", "Secrets Manager can't find the specified secret value for %s", what)
	}
	return v, nil
}

// valueEntry is GetSecretValue's output and a BatchGetSecretValue entry.
type valueEntry struct {
	ARN           string
	Name          string
	VersionId     string
	SecretString  *string `json:",omitempty"`
	SecretBinary  []byte  `json:",omitempty"`
	VersionStages []string
	CreatedDate   core.Time
}

// value renders a version and records the access day, as AWS does. Lock held.
func (sec *Secret) value(v *Version) valueEntry {
	day := time.Now().UTC().Truncate(24 * time.Hour)
	sec.Accessed, v.Accessed = day, day
	return valueEntry{sec.ARN, sec.Name, v.ID, v.String, v.Binary, slices.Clone(v.Stages), core.Time(v.Created)}
}

func checkValue(str *string, bin []byte) error {
	switch {
	case str != nil && len(bin) > 0:
		return invalidParam("You can't specify both a binary secret value and a string secret value in the same secret.")
	case str != nil && len(*str) > 65536 || len(bin) > 65536:
		return invalidParam("The secret value can't be larger than 65536 bytes.")
	}
	return nil
}

// ---- secrets ----

var nameRe = regexp.MustCompile(`^[A-Za-z0-9/_+=.@!-]{1,512}$`)

func (s *Service) createSecret(c *core.Ctx, in *struct {
	Name               string
	ClientRequestToken string
	Description        string
	KmsKeyId           string
	SecretString       *string
	SecretBinary       []byte
	Tags               []Tag
	AddReplicaRegions  []Replica
}) (any, error) {
	if !nameRe.MatchString(in.Name) {
		return nil, invalidParam("Invalid name. Must be a valid name containing alphanumeric characters, or any of the following: -/_+=.@!")
	}
	if err := checkValue(in.SecretString, in.SecretBinary); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, err := s.find(c, in.Name, false); err == nil {
		if !old.Deleted.IsZero() {
			return nil, invalidReq("You can't create this secret because a secret with this name is already scheduled for deletion.")
		}
		return nil, core.Errorf(400, "ResourceExistsException", "The operation failed because the secret %s already exists.", in.Name)
	}
	now := time.Now().UTC()
	sec := &Secret{Name: in.Name, Account: c.Account, Region: c.Region, Description: in.Description, KmsKeyID: in.KmsKeyId,
		Tags: in.Tags, Versions: map[string]*Version{}, Created: now, Changed: now,
		ARN: c.ARN("secretsmanager", "secret:"+in.Name+"-"+core.RandString(6, core.AlphaNum))}
	out := &result{ARN: sec.ARN, Name: sec.Name}
	if in.SecretString != nil || len(in.SecretBinary) > 0 {
		v, err := sec.put(in.ClientRequestToken, in.SecretString, in.SecretBinary, nil)
		if err != nil {
			return nil, err
		}
		out.VersionId = v.ID
	}
	sec.replicate(in.AddReplicaRegions)
	out.ReplicationStatus = slices.Clone(sec.Replicas)
	s.st.Secrets[key(c.Account, c.Region, in.Name)] = sec
	return out, nil
}

func (s *Service) getSecretValue(c *core.Ctx, in *struct{ SecretId, VersionId, VersionStage string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, true)
	if err != nil {
		return nil, err
	}
	v, err := sec.version(in.VersionId, in.VersionStage)
	if err != nil {
		return nil, err
	}
	return sec.value(v), nil
}

type filter struct {
	Key    string
	Values []string
}

func (s *Service) batchGetSecretValue(c *core.Ctx, in *struct {
	SecretIdList []string
	Filters      []filter
	MaxResults   int
	NextToken    string
}) (any, error) {
	if (len(in.SecretIdList) > 0) == (len(in.Filters) > 0) {
		return nil, invalidParam("Either 'SecretIdList' or 'Filters' must be provided, but not both.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	type apiError struct{ SecretId, ErrorCode, Message string }
	out := struct {
		SecretValues []valueEntry
		Errors       []apiError `json:",omitempty"`
		NextToken    string     `json:",omitempty"`
	}{SecretValues: []valueEntry{}}
	ids := in.SecretIdList
	if len(in.Filters) > 0 {
		list, next, err := s.list(c, in.Filters, false, "", "", in.NextToken, in.MaxResults)
		if err != nil {
			return nil, err
		}
		ids, out.NextToken = nil, next
		for _, sec := range list {
			ids = append(ids, sec.ARN)
		}
	}
	for _, id := range ids {
		sec, err := s.find(c, id, true)
		var v *Version
		if err == nil {
			v, err = sec.version("", "")
		}
		if err != nil {
			e := core.AsError(err)
			out.Errors = append(out.Errors, apiError{id, e.Code, e.Msg})
			continue
		}
		out.SecretValues = append(out.SecretValues, sec.value(v))
	}
	return &out, nil
}

func (s *Service) putSecretValue(c *core.Ctx, in *struct {
	SecretId           string
	ClientRequestToken string
	SecretString       *string
	SecretBinary       []byte
	VersionStages      []string
}) (any, error) {
	if err := checkValue(in.SecretString, in.SecretBinary); err != nil {
		return nil, err
	}
	if in.SecretString == nil && len(in.SecretBinary) == 0 {
		return nil, invalidParam("You must provide either SecretString or SecretBinary.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, true)
	if err != nil {
		return nil, err
	}
	v, err := sec.put(in.ClientRequestToken, in.SecretString, in.SecretBinary, in.VersionStages)
	if err != nil {
		return nil, err
	}
	return &result{ARN: sec.ARN, Name: sec.Name, VersionId: v.ID, VersionStages: slices.Clone(v.Stages)}, nil
}

func (s *Service) updateSecret(c *core.Ctx, in *struct {
	SecretId           string
	ClientRequestToken string
	Description        *string
	KmsKeyId           *string
	SecretString       *string
	SecretBinary       []byte
}) (any, error) {
	if err := checkValue(in.SecretString, in.SecretBinary); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, true)
	if err != nil {
		return nil, err
	}
	out := &result{ARN: sec.ARN, Name: sec.Name}
	if in.SecretString != nil || len(in.SecretBinary) > 0 {
		v, err := sec.put(in.ClientRequestToken, in.SecretString, in.SecretBinary, nil)
		if err != nil {
			return nil, err
		}
		out.VersionId = v.ID
	}
	if in.Description != nil {
		sec.Description = *in.Description
	}
	if in.KmsKeyId != nil {
		sec.KmsKeyID = *in.KmsKeyId
	}
	sec.Changed = time.Now().UTC()
	return out, nil
}

func (s *Service) updateSecretVersionStage(c *core.Ctx, in *struct{ SecretId, VersionStage, RemoveFromVersionId, MoveToVersionId string }) (any, error) {
	return s.update(c, in.SecretId, true, func(sec *Secret) error {
		label := in.VersionStage
		if label == "" {
			return invalidParam("You must specify a VersionStage.")
		}
		holder := sec.byStage(label)
		switch {
		case in.MoveToVersionId == "" && in.RemoveFromVersionId == "":
			return invalidParam("You must specify MoveToVersionId or RemoveFromVersionId.")
		case in.RemoveFromVersionId != "" && (holder == nil || holder.ID != in.RemoveFromVersionId):
			return invalidParam("The staging label %s isn't attached to the version %s.", label, in.RemoveFromVersionId)
		case in.RemoveFromVersionId == "" && holder != nil && holder.ID != in.MoveToVersionId:
			return invalidParam("The staging label %s is currently attached to version %s, so you must explicitly reference that version in RemoveFromVersionId.", label, holder.ID)
		case in.MoveToVersionId == "" && label == "AWSCURRENT":
			return invalidParam("You can't remove the staging label AWSCURRENT, you can only move it to another version.")
		case in.MoveToVersionId == "":
			holder.Stages = drop(holder.Stages, label)
			sec.Changed = time.Now().UTC()
			return nil
		}
		v := sec.Versions[in.MoveToVersionId]
		if v == nil {
			return core.Errorf(400, "ResourceNotFoundException", "Secrets Manager can't find the specified secret value for VersionId: %s", in.MoveToVersionId)
		}
		sec.stage(label, v)
		return nil
	})
}

// describeOutput is DescribeSecret's output and a ListSecrets entry.
type describeOutput struct {
	ARN                    string
	Name                   string
	Description            string              `json:",omitempty"`
	KmsKeyId               string              `json:",omitempty"`
	RotationEnabled        *bool               `json:",omitempty"`
	RotationLambdaARN      string              `json:",omitempty"`
	RotationRules          *RotationRules      `json:",omitempty"`
	LastRotatedDate        core.Time           `json:",omitzero"`
	LastChangedDate        core.Time           `json:",omitzero"`
	LastAccessedDate       core.Time           `json:",omitzero"`
	DeletedDate            core.Time           `json:",omitzero"`
	NextRotationDate       core.Time           `json:",omitzero"`
	CreatedDate            core.Time           `json:",omitzero"`
	Tags                   []Tag               `json:",omitempty"`
	VersionIdsToStages     map[string][]string `json:",omitempty"`
	SecretVersionsToStages map[string][]string `json:",omitempty"`
	PrimaryRegion          string              `json:",omitempty"`
	ReplicationStatus      []Replica           `json:",omitempty"`
}

// describe renders a secret without its value (copies, so it can be encoded unlocked). Lock held.
func (sec *Secret) describe(list bool) *describeOutput {
	o := &describeOutput{ARN: sec.ARN, Name: sec.Name, Description: sec.Description, KmsKeyId: sec.KmsKeyID,
		RotationLambdaARN: sec.RotationLambda, LastRotatedDate: core.Time(sec.Rotated), LastChangedDate: core.Time(sec.Changed),
		LastAccessedDate: core.Time(sec.Accessed), DeletedDate: core.Time(sec.Deleted), CreatedDate: core.Time(sec.Created),
		Tags: slices.Clone(sec.Tags), ReplicationStatus: slices.Clone(sec.Replicas)}
	if sec.Rotation || sec.RotationLambda != "" {
		on := sec.Rotation
		o.RotationEnabled = &on
	}
	if r := sec.RotationRules; r != nil {
		rules := *r
		o.RotationRules = &rules
		if sec.Rotation && r.AutomaticallyAfterDays > 0 {
			base := sec.Rotated
			if base.IsZero() {
				base = sec.Created
			}
			o.NextRotationDate = core.Time(base.AddDate(0, 0, int(r.AutomaticallyAfterDays)))
		}
	}
	if len(sec.Replicas) > 0 {
		o.PrimaryRegion = sec.Region
	}
	stages := map[string][]string{} // versions without labels are deprecated and not shown
	for id, v := range sec.Versions {
		if len(v.Stages) > 0 {
			stages[id] = slices.Clone(v.Stages)
		}
	}
	if list {
		o.SecretVersionsToStages = stages
	} else {
		o.VersionIdsToStages = stages
	}
	return o
}

func (s *Service) describeSecret(c *core.Ctx, in *struct{ SecretId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, false)
	if err != nil {
		return nil, err
	}
	return sec.describe(false), nil
}

// page slices n items for an offset NextToken and a page size (def when unset).
func page(n int, token string, size, def int) (start, end int, next string, err error) {
	if token != "" {
		if start, err = strconv.Atoi(token); err != nil || start < 0 {
			return 0, 0, "", core.Errorf(400, "InvalidNextTokenException", "The NextToken value is invalid.")
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

// list returns the caller's matching secrets, sorted and paged. Lock held.
func (s *Service) list(c *core.Ctx, filters []filter, deleted bool, sortBy, order, token string, max int) ([]*Secret, string, error) {
	var all []*Secret
	for k, sec := range s.st.Secrets {
		switch {
		case sec.Account != c.Account || sec.Region != c.Region:
		case !sec.PurgeAt.IsZero() && time.Now().After(sec.PurgeAt):
			delete(s.st.Secrets, k)
		case (deleted || sec.Deleted.IsZero()) && sec.matches(filters):
			all = append(all, sec)
		}
	}
	sort.Slice(all, func(i, j int) bool {
		a, b := all[i], all[j]
		switch sortBy {
		case "name":
			return a.Name < b.Name
		case "last-changed-date":
			return a.Changed.Before(b.Changed)
		case "last-accessed-date":
			return a.Accessed.Before(b.Accessed)
		}
		return a.Created.Before(b.Created) || a.Created.Equal(b.Created) && a.Name < b.Name
	})
	if order == "desc" {
		slices.Reverse(all)
	}
	start, end, next, err := page(len(all), token, max, 100)
	if err != nil {
		return nil, "", err
	}
	return all[start:end], next, nil
}

// matches applies ListSecrets filters: filters are ANDed, the values of one
// filter ORed, "!" negates. A value matches the start of an attribute or of
// any word in it; description and all ignore case.
func (sec *Secret) matches(filters []filter) bool {
	for _, f := range filters {
		var fields []string
		fold := false
		switch f.Key {
		case "name":
			fields = []string{sec.Name}
		case "description":
			fields, fold = []string{sec.Description}, true
		case "primary-region":
			fields = []string{sec.Region}
		case "tag-key", "tag-value", "all":
			for _, t := range sec.Tags {
				if f.Key != "tag-value" {
					fields = append(fields, t.Key)
				}
				if f.Key != "tag-key" {
					fields = append(fields, t.Value)
				}
			}
			if f.Key == "all" {
				fields, fold = append(fields, sec.Name, sec.Description), true
			}
		}
		ok := false
		for _, v := range f.Values {
			neg := strings.HasPrefix(v, "!")
			if prefixMatch(fields, strings.TrimPrefix(v, "!"), fold) != neg {
				ok = true
			}
		}
		if !ok {
			return false
		}
	}
	return true
}

func prefixMatch(fields []string, v string, fold bool) bool {
	if fold {
		v = strings.ToLower(v)
	}
	for _, f := range fields {
		if fold {
			f = strings.ToLower(f)
		}
		words := strings.FieldsFunc(f, func(r rune) bool { return strings.ContainsRune("/_+=.@- ", r) })
		if strings.HasPrefix(f, v) || slices.ContainsFunc(words, func(w string) bool { return strings.HasPrefix(w, v) }) {
			return true
		}
	}
	return false
}

func (s *Service) listSecrets(c *core.Ctx, in *struct {
	Filters                []filter
	IncludePlannedDeletion bool
	MaxResults             int
	NextToken              string
	SortBy                 string
	SortOrder              string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	list, next, err := s.list(c, in.Filters, in.IncludePlannedDeletion, in.SortBy, in.SortOrder, in.NextToken, in.MaxResults)
	if err != nil {
		return nil, err
	}
	out := struct {
		SecretList []*describeOutput
		NextToken  string `json:",omitempty"`
	}{SecretList: []*describeOutput{}, NextToken: next}
	for _, sec := range list {
		out.SecretList = append(out.SecretList, sec.describe(true))
	}
	return &out, nil
}

func (s *Service) listSecretVersionIds(c *core.Ctx, in *struct {
	SecretId          string
	IncludeDeprecated bool
	MaxResults        int
	NextToken         string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, false)
	if err != nil {
		return nil, err
	}
	var vs []*Version
	for _, v := range sec.Versions {
		if len(v.Stages) > 0 || in.IncludeDeprecated {
			vs = append(vs, v)
		}
	}
	sort.Slice(vs, func(i, j int) bool {
		return vs[i].Created.After(vs[j].Created) || vs[i].Created.Equal(vs[j].Created) && vs[i].ID < vs[j].ID
	})
	start, end, next, err := page(len(vs), in.NextToken, in.MaxResults, 100)
	if err != nil {
		return nil, err
	}
	type entry struct {
		VersionId        string
		VersionStages    []string  `json:",omitempty"`
		CreatedDate      core.Time `json:",omitzero"`
		LastAccessedDate core.Time `json:",omitzero"`
	}
	out := struct {
		ARN, Name string
		Versions  []entry
		NextToken string `json:",omitempty"`
	}{ARN: sec.ARN, Name: sec.Name, Versions: []entry{}, NextToken: next}
	for _, v := range vs[start:end] {
		out.Versions = append(out.Versions, entry{v.ID, slices.Clone(v.Stages), core.Time(v.Created), core.Time(v.Accessed)})
	}
	return &out, nil
}

func (s *Service) deleteSecret(c *core.Ctx, in *struct {
	SecretId                   string
	RecoveryWindowInDays       *int
	ForceDeleteWithoutRecovery bool
}) (any, error) {
	days := 30
	switch {
	case in.ForceDeleteWithoutRecovery && in.RecoveryWindowInDays != nil:
		return nil, invalidParam("You can't use ForceDeleteWithoutRecovery in conjunction with RecoveryWindowInDays.")
	case in.RecoveryWindowInDays != nil:
		days = *in.RecoveryWindowInDays
	}
	if days < 7 || days > 30 {
		return nil, invalidParam("The RecoveryWindowInDays value must be between 7 and 30 days (inclusive).")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, false)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if in.ForceDeleteWithoutRecovery {
		delete(s.st.Secrets, key(sec.Account, sec.Region, sec.Name))
		return &result{ARN: sec.ARN, Name: sec.Name, DeletionDate: core.Time(now)}, nil
	}
	if !sec.Deleted.IsZero() {
		return nil, invalidReq("You can't perform this operation on the secret because it was already scheduled for deletion.")
	}
	sec.Deleted, sec.PurgeAt = now, now.AddDate(0, 0, days)
	return &result{ARN: sec.ARN, Name: sec.Name, DeletionDate: core.Time(sec.PurgeAt)}, nil
}

func (s *Service) restoreSecret(c *core.Ctx, in *struct{ SecretId string }) (any, error) {
	return s.update(c, in.SecretId, false, func(sec *Secret) error {
		sec.Deleted, sec.PurgeAt, sec.Changed = time.Time{}, time.Time{}, time.Now().UTC()
		return nil
	})
}

func (s *Service) tagResource(c *core.Ctx, in *struct {
	SecretId string
	Tags     []Tag
}) (any, error) {
	_, err := s.update(c, in.SecretId, true, func(sec *Secret) error {
		for _, t := range in.Tags {
			if i := slices.IndexFunc(sec.Tags, func(x Tag) bool { return x.Key == t.Key }); i >= 0 {
				sec.Tags[i] = t
			} else {
				sec.Tags = append(sec.Tags, t)
			}
		}
		return nil
	})
	return nil, err
}

func (s *Service) untagResource(c *core.Ctx, in *struct {
	SecretId string
	TagKeys  []string
}) (any, error) {
	_, err := s.update(c, in.SecretId, true, func(sec *Secret) error {
		sec.Tags = slices.DeleteFunc(sec.Tags, func(t Tag) bool { return slices.Contains(in.TagKeys, t.Key) })
		return nil
	})
	return nil, err
}

// ---- random passwords ----

func (s *Service) getRandomPassword(c *core.Ctx, in *struct {
	PasswordLength          *int
	ExcludeCharacters       string
	ExcludeNumbers          bool
	ExcludePunctuation      bool
	ExcludeUppercase        bool
	ExcludeLowercase        bool
	IncludeSpace            bool
	RequireEachIncludedType *bool
}) (any, error) {
	n := 32
	if in.PasswordLength != nil {
		n = *in.PasswordLength
	}
	if n < 1 || n > 4096 {
		return nil, invalidParam("PasswordLength must be between 1 and 4096.")
	}
	keep := func(set string) string {
		return strings.Map(func(r rune) rune {
			if strings.ContainsRune(in.ExcludeCharacters, r) {
				return -1
			}
			return r
		}, set)
	}
	var classes []string // each must appear once when RequireEachIncludedType
	for _, cl := range []struct {
		skip  bool
		chars string
	}{
		{in.ExcludeLowercase, "abcdefghijklmnopqrstuvwxyz"},
		{in.ExcludeUppercase, "ABCDEFGHIJKLMNOPQRSTUVWXYZ"},
		{in.ExcludeNumbers, "0123456789"},
		{in.ExcludePunctuation, "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~"},
	} {
		if chars := keep(cl.chars); !cl.skip && chars != "" {
			classes = append(classes, chars)
		}
	}
	all := strings.Join(classes, "")
	if in.IncludeSpace {
		all += keep(" ")
	}
	if all == "" {
		return nil, invalidParam("The password can't be generated because every character type is excluded.")
	}
	pw := make([]byte, n)
	for i := range pw {
		pw[i] = all[randN(len(all))]
	}
	if in.RequireEachIncludedType == nil || *in.RequireEachIncludedType {
		if n < len(classes) {
			return nil, invalidParam("PasswordLength is too short to include every required character type.")
		}
		pos := make([]int, n) // a partial shuffle picks distinct positions for one character of each class
		for i := range pos {
			pos[i] = i
		}
		for i, cl := range classes {
			j := i + randN(n-i)
			pos[i], pos[j] = pos[j], pos[i]
			pw[pos[i]] = cl[randN(len(cl))]
		}
	}
	return map[string]string{"RandomPassword": string(pw)}, nil
}

func randN(n int) int {
	v, _ := rand.Int(rand.Reader, big.NewInt(int64(n)))
	return int(v.Int64())
}

// ---- resource policies ----

func validPolicy(p string) error {
	var doc map[string]any
	if json.Unmarshal([]byte(p), &doc) != nil {
		return core.Errorf(400, "MalformedPolicyDocumentException", "This resource policy contains an unsupported or malformed syntax.")
	}
	return nil
}

func (s *Service) getResourcePolicy(c *core.Ctx, in *struct{ SecretId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, false)
	if err != nil {
		return nil, err
	}
	return &struct {
		ARN, Name      string
		ResourcePolicy string `json:",omitempty"`
	}{sec.ARN, sec.Name, sec.Policy}, nil
}

func (s *Service) putResourcePolicy(c *core.Ctx, in *struct{ SecretId, ResourcePolicy string }) (any, error) {
	if err := validPolicy(in.ResourcePolicy); err != nil {
		return nil, err
	}
	return s.update(c, in.SecretId, true, func(sec *Secret) error {
		sec.Policy = in.ResourcePolicy
		return nil
	})
}

func (s *Service) deleteResourcePolicy(c *core.Ctx, in *struct{ SecretId string }) (any, error) {
	return s.update(c, in.SecretId, false, func(sec *Secret) error {
		sec.Policy = ""
		return nil
	})
}

func (s *Service) validateResourcePolicy(c *core.Ctx, in *struct{ SecretId, ResourcePolicy string }) (any, error) {
	if err := validPolicy(in.ResourcePolicy); err != nil {
		return nil, err
	}
	return map[string]any{"PolicyValidationPassed": true, "ValidationErrors": []any{}}, nil
}

// ---- rotation ----

func (s *Service) rotateSecret(c *core.Ctx, in *struct {
	SecretId           string
	ClientRequestToken string
	RotationLambdaARN  string
	RotationRules      *RotationRules
	RotateImmediately  *bool
}) (any, error) {
	s.mu.Lock()
	sec, err := s.find(c, in.SecretId, true)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if in.RotationLambdaARN != "" {
		sec.RotationLambda = in.RotationLambdaARN
	}
	if in.RotationRules != nil {
		sec.RotationRules = in.RotationRules
	}
	sec.Rotation = true
	out := &result{ARN: sec.ARN, Name: sec.Name}
	if in.RotateImmediately != nil && !*in.RotateImmediately {
		s.mu.Unlock() // ponytail: scheduled rotations don't run; RotateSecret rotates on demand
		return out, nil
	}
	token := in.ClientRequestToken
	if token == "" {
		token = core.UUID()
	}
	out.VersionId = token
	if sec.RotationLambda == "" { // no function: rotate by copying the current value
		defer s.mu.Unlock()
		cur, err := sec.version("", "")
		if err == nil {
			_, err = sec.put(token, cur.String, cur.Binary, nil)
		}
		if err != nil {
			return nil, err
		}
		sec.Rotated = time.Now().UTC()
		return out, nil
	}
	if sec.Versions[token] == nil {
		sec.Versions[token] = &Version{ID: token, Stages: []string{}, Created: time.Now().UTC()}
	}
	sec.stage("AWSPENDING", sec.Versions[token])
	fn, arn := sec.RotationLambda, sec.ARN
	s.mu.Unlock()

	// The function calls back into pogi to read and write the pending version.
	for _, step := range []string{"createSecret", "setSecret", "testSecret", "finishSecret"} {
		payload, _ := json.Marshal(map[string]string{"Step": step, "SecretId": arn, "ClientRequestToken": token})
		if _, err := s.hub.Invoke(fn, payload); err != nil {
			return nil, invalidReq("Rotation function %s failed during %s: %v", fn, step, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if sec, err = s.find(c, arn, true); err != nil {
		return nil, err
	}
	v := sec.Versions[token]
	if v == nil || v.String == nil && v.Binary == nil {
		return nil, invalidReq("Rotation function %s did not store a value for version %s.", fn, token)
	}
	sec.stage("AWSCURRENT", v) // a no-op when finishSecret already moved it
	v.Stages = drop(v.Stages, "AWSPENDING")
	sec.Rotated = time.Now().UTC()
	return out, nil
}

func (s *Service) cancelRotateSecret(c *core.Ctx, in *struct{ SecretId string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, in.SecretId, true)
	if err != nil {
		return nil, err
	}
	sec.Rotation = false
	out := &result{ARN: sec.ARN, Name: sec.Name}
	if v := sec.byStage("AWSPENDING"); v != nil && !slices.Contains(v.Stages, "AWSCURRENT") {
		out.VersionId = v.ID // the unfinished version, for the caller to clean up
	}
	return out, nil
}

// ---- replication (metadata only) ----

func (sec *Secret) replicate(add []Replica) {
	for _, r := range add {
		sec.Replicas = slices.DeleteFunc(sec.Replicas, func(x Replica) bool { return x.Region == r.Region })
		sec.Replicas = append(sec.Replicas, Replica{Region: r.Region, KmsKeyId: r.KmsKeyId, Status: "InSync", StatusMessage: "Replication succeeded"})
	}
}

func (s *Service) replicateSecretToRegions(c *core.Ctx, in *struct {
	SecretId          string
	AddReplicaRegions []Replica
}) (any, error) {
	return s.replication(c, in.SecretId, func(sec *Secret) { sec.replicate(in.AddReplicaRegions) })
}

func (s *Service) removeRegionsFromReplication(c *core.Ctx, in *struct {
	SecretId             string
	RemoveReplicaRegions []string
}) (any, error) {
	return s.replication(c, in.SecretId, func(sec *Secret) {
		sec.Replicas = slices.DeleteFunc(sec.Replicas, func(r Replica) bool { return slices.Contains(in.RemoveReplicaRegions, r.Region) })
	})
}

func (s *Service) replication(c *core.Ctx, id string, fn func(*Secret)) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sec, err := s.find(c, id, true)
	if err != nil {
		return nil, err
	}
	fn(sec)
	return &result{ARN: sec.ARN, ReplicationStatus: append([]Replica{}, sec.Replicas...)}, nil
}

// ---- console ----

// Resources lists secrets for the console (never their values).
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Secrets", Columns: []string{"Name", "Region", "Account", "Versions", "Rotation", "Last changed"}}
	for _, k := range core.SortedKeys(s.st.Secrets) {
		sec := s.st.Secrets[k]
		rotation := map[bool]string{true: "enabled", false: "disabled"}[sec.Rotation]
		t.Rows = append(t.Rows, []string{sec.Name, sec.Region, sec.Account, strconv.Itoa(len(sec.Versions)), rotation, sec.Changed.Format(time.DateTime)})
	}
	return []core.Table{t}
}

// Inspect returns a secret's DescribeSecret view; the value is never shown.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Secrets) {
		if sec := s.st.Secrets[k]; sec.Name == id {
			return sec.describe(false)
		}
	}
	return nil
}
