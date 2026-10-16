package stepfunctions

// State machine versions and aliases. A version is a numbered, unchangeable copy
// of a state machine; an alias points at one version, or splits traffic between two.
// Their ARNs are the machine ARN plus ":number" or ":aliasName".

import (
	"cmp"
	"encoding/json"
	"math/rand/v2"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// maxVersions is the AWS quota of published versions per state machine.
const maxVersions = 1000

// Version is a published copy of a state machine.
type Version struct {
	ARN         string          `json:"arn"`
	MachineARN  string          `json:"machineArn"`
	Number      int             `json:"number"`
	Description string          `json:"description,omitempty"`
	Type        string          `json:"type"`
	Definition  string          `json:"definition"`
	RoleArn     string          `json:"roleArn"`
	Logging     json.RawMessage `json:"logging,omitempty"`
	Tracing     json.RawMessage `json:"tracing,omitempty"`
	Revision    string          `json:"revision"`
	Created     time.Time       `json:"created"`
}

// Alias points at one or two versions of a state machine.
type Alias struct {
	ARN         string    `json:"arn"`
	Name        string    `json:"name"`
	MachineARN  string    `json:"machineArn"`
	Description string    `json:"description,omitempty"`
	Routes      []route   `json:"routes"`
	Created     time.Time `json:"created"`
	Updated     time.Time `json:"updated"`
}

type route struct {
	Version string `json:"stateMachineVersionArn"`
	Weight  int    `json:"weight"`
}

func (a *Alias) routing() []map[string]any {
	out := make([]map[string]any, len(a.Routes))
	for i, r := range a.Routes {
		out[i] = map[string]any{"stateMachineVersionArn": r.Version, "weight": r.Weight}
	}
	return out
}

// pick chooses the version an execution started through the alias runs, by weight.
func (a *Alias) pick() string {
	if len(a.Routes) == 2 && rand.IntN(100) >= a.Routes[0].Weight {
		return a.Routes[1].Version
	}
	return a.Routes[0].Version
}

// target is what a state machine ARN names: the machine itself, one of its versions or an alias.
type target struct {
	m *Machine
	v *Version
	a *Alias
}

// resolve finds what an ARN names; lock held.
func (s *Service) resolve(arn string) (target, error) {
	if err := checkARN(arn, "stateMachine"); err != nil {
		return target{}, err
	}
	switch {
	case s.st.Machines[arn] != nil:
		return target{m: s.st.Machines[arn]}, nil
	case s.st.Versions[arn] != nil:
		v := s.st.Versions[arn]
		return target{m: s.st.Machines[v.MachineARN], v: v}, nil
	case s.st.Aliases[arn] != nil:
		a := s.st.Aliases[arn]
		return target{m: s.st.Machines[a.MachineARN], a: a}, nil
	}
	return target{}, core.Errorf(400, "StateMachineDoesNotExist", "State Machine Does Not Exist: '%s'", arn)
}

// machineFor finds a state machine by its own ARN; lock held.
func (s *Service) machineFor(arn string) (*Machine, error) {
	t, err := s.resolve(arn)
	if err != nil {
		return nil, err
	}
	if t.v != nil || t.a != nil {
		return nil, core.Errorf(400, "InvalidArn", "Invalid Arn: 'Resource type not valid in this context: %s'", arn)
	}
	return t.m, nil
}

// versionsOf lists a machine's versions, newest first; lock held.
func (s *Service) versionsOf(m *Machine) []*Version {
	var out []*Version
	for _, v := range s.st.Versions {
		if v.MachineARN == m.ARN {
			out = append(out, v)
		}
	}
	slices.SortFunc(out, func(a, b *Version) int { return b.Number - a.Number })
	return out
}

// describeVersion is DescribeStateMachine for a version.
func (v *Version) describe(name string) map[string]any {
	out := map[string]any{"stateMachineArn": v.ARN, "name": name, "status": "ACTIVE", "definition": v.Definition,
		"roleArn": v.RoleArn, "type": v.Type, "creationDate": core.Time(v.Created), "revisionId": v.Revision,
		"loggingConfiguration": rawOr(v.Logging, `{"level":"OFF","includeExecutionData":false}`),
		"tracingConfiguration": rawOr(v.Tracing, `{"enabled":false}`)}
	if v.Description != "" {
		out["description"] = v.Description
	}
	return out
}

// publish makes a version of the machine's current revision, or returns the one already made for it; lock held.
func (s *Service) publish(m *Machine, description string) (*Version, error) {
	if utf8.RuneCountInString(description) > 256 {
		return nil, core.Errorf(400, "ValidationException", "Value at 'versionDescription' failed to satisfy constraint: Member must have length less than or equal to 256")
	}
	existing := s.versionsOf(m)
	for _, v := range existing {
		if v.Revision == m.Revision {
			return v, nil
		}
	}
	if len(existing) >= maxVersions {
		return nil, core.Errorf(400, "ServiceQuotaExceededException", "The maximum number of published versions (%d) for this state machine has been reached. Delete a version to publish a new one.", maxVersions)
	}
	m.LastVersion++
	v := &Version{ARN: m.ARN + ":" + strconv.Itoa(m.LastVersion), MachineARN: m.ARN, Number: m.LastVersion, Description: description, Type: m.Type,
		Definition: m.Definition, RoleArn: m.RoleArn, Logging: m.Logging, Tracing: m.Tracing, Revision: m.Revision, Created: time.Now().UTC()}
	s.st.Versions[v.ARN] = v
	return v, nil
}

func (s *Service) publishStateMachineVersion(c *core.Ctx, in *struct{ StateMachineArn, RevisionId, Description string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.machineFor(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	if in.RevisionId != "" && in.RevisionId != m.Revision {
		return nil, core.Errorf(400, "ConflictException", "Failed to publish the version: the revision id %s is not the current revision of the state machine", in.RevisionId)
	}
	v, err := s.publish(m, in.Description)
	if err != nil {
		return nil, err
	}
	return map[string]any{"stateMachineVersionArn": v.ARN, "creationDate": core.Time(v.Created)}, nil
}

func (s *Service) listStateMachineVersions(c *core.Ctx, in *struct {
	StateMachineArn, NextToken string
	MaxResults                 int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	m, err := s.machineFor(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	list := []map[string]any{}
	for _, v := range s.versionsOf(m) {
		list = append(list, map[string]any{"stateMachineVersionArn": v.ARN, "creationDate": core.Time(v.Created)})
	}
	return page("stateMachineVersions", list, in.MaxResults, in.NextToken), nil
}

func (s *Service) deleteStateMachineVersion(c *core.Ctx, in *struct{ StateMachineVersionArn string }) (any, error) {
	if err := checkARN(in.StateMachineVersionArn, "stateMachine"); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Versions[in.StateMachineVersionArn] == nil {
		return map[string]any{}, nil
	}
	for _, a := range s.st.Aliases {
		for _, r := range a.Routes {
			if r.Version == in.StateMachineVersionArn {
				return nil, core.Errorf(400, "ConflictException", "The version %s is used by the alias %s", in.StateMachineVersionArn, a.ARN)
			}
		}
	}
	delete(s.st.Versions, in.StateMachineVersionArn)
	return map[string]any{}, nil
}

// ---- aliases ----

var aliasName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{1,80}$`)

// routes checks a routing configuration and returns it with the machine it belongs to; lock held.
func (s *Service) routes(cfg []route) (*Machine, []route, error) {
	if len(cfg) < 1 || len(cfg) > 2 {
		return nil, nil, core.Errorf(400, "ValidationException", "The routing configuration must have one or two versions")
	}
	total, seen := 0, map[string]bool{}
	var m *Machine
	for _, r := range cfg {
		if r.Weight < 0 || r.Weight > 100 || seen[r.Version] {
			return nil, nil, core.Errorf(400, "ValidationException", "The routing configuration needs different versions with weights from 0 to 100")
		}
		seen[r.Version] = true
		total += r.Weight
		if err := checkARN(r.Version, "stateMachine"); err != nil {
			return nil, nil, err
		}
		v := s.st.Versions[r.Version]
		if v == nil {
			return nil, nil, core.Errorf(400, "ResourceNotFound", "Resource not found: '%s'", r.Version)
		}
		if m != nil && m.ARN != v.MachineARN {
			return nil, nil, core.Errorf(400, "ValidationException", "All versions of an alias must belong to the same state machine")
		}
		m = s.st.Machines[v.MachineARN]
	}
	if total != 100 {
		return nil, nil, core.Errorf(400, "ValidationException", "The weights of the routing configuration must add up to 100")
	}
	return m, cfg, nil
}

type routeIn struct {
	StateMachineVersionArn string
	Weight                 int
}

func routesOf(in []routeIn) []route {
	out := make([]route, len(in))
	for i, r := range in {
		out[i] = route{r.StateMachineVersionArn, r.Weight}
	}
	return out
}

func (s *Service) createStateMachineAlias(c *core.Ctx, in *struct {
	Name, Description    string
	RoutingConfiguration []routeIn
}) (any, error) {
	if !aliasName.MatchString(in.Name) || strings.Trim(in.Name, "0123456789") == "" {
		return nil, core.Errorf(400, "ValidationException", "Invalid alias name '%s': use 1 to 80 letters, digits, '-', '_' or '.', and not only digits", in.Name)
	}
	if utf8.RuneCountInString(in.Description) > 256 {
		return nil, core.Errorf(400, "ValidationException", "Value at 'description' failed to satisfy constraint: Member must have length less than or equal to 256")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	m, routes, err := s.routes(routesOf(in.RoutingConfiguration))
	if err != nil {
		return nil, err
	}
	arn := m.ARN + ":" + in.Name
	if s.st.Aliases[arn] != nil {
		return nil, core.Errorf(400, "ConflictException", "The alias %s already exists", arn)
	}
	now := time.Now().UTC()
	s.st.Aliases[arn] = &Alias{ARN: arn, Name: in.Name, MachineARN: m.ARN, Description: in.Description, Routes: routes, Created: now, Updated: now}
	return map[string]any{"stateMachineAliasArn": arn, "creationDate": core.Time(now)}, nil
}

// alias finds an alias by ARN; lock held.
func (s *Service) alias(arn string) (*Alias, error) {
	if err := checkARN(arn, "stateMachine"); err != nil {
		return nil, err
	}
	if a := s.st.Aliases[arn]; a != nil {
		return a, nil
	}
	return nil, core.Errorf(400, "ResourceNotFound", "Resource not found: '%s'", arn)
}

func (s *Service) describeStateMachineAlias(c *core.Ctx, in *struct{ StateMachineAliasArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.alias(in.StateMachineAliasArn)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"stateMachineAliasArn": a.ARN, "name": a.Name, "routingConfiguration": a.routing(),
		"creationDate": core.Time(a.Created), "updateDate": core.Time(a.Updated)}
	if a.Description != "" {
		out["description"] = a.Description
	}
	return out, nil
}

func (s *Service) updateStateMachineAlias(c *core.Ctx, in *struct {
	StateMachineAliasArn, Description string
	RoutingConfiguration              []routeIn
}) (any, error) {
	if in.Description == "" && in.RoutingConfiguration == nil {
		return nil, core.Errorf(400, "MissingRequiredParameter", "Either the description or the routingConfiguration must be specified")
	}
	if utf8.RuneCountInString(in.Description) > 256 {
		return nil, core.Errorf(400, "ValidationException", "Value at 'description' failed to satisfy constraint: Member must have length less than or equal to 256")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.alias(in.StateMachineAliasArn)
	if err != nil {
		return nil, err
	}
	if in.RoutingConfiguration != nil {
		m, routes, err := s.routes(routesOf(in.RoutingConfiguration))
		if err != nil {
			return nil, err
		}
		if m.ARN != a.MachineARN {
			return nil, core.Errorf(400, "ValidationException", "All versions of an alias must belong to the same state machine")
		}
		a.Routes = routes
	}
	a.Description = cmp.Or(in.Description, a.Description)
	a.Updated = time.Now().UTC()
	return map[string]any{"updateDate": core.Time(a.Updated)}, nil
}

func (s *Service) deleteStateMachineAlias(c *core.Ctx, in *struct{ StateMachineAliasArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.alias(in.StateMachineAliasArn)
	if err != nil {
		return nil, err
	}
	delete(s.st.Aliases, a.ARN)
	return map[string]any{}, nil
}

// listStateMachineAliases lists the aliases of a machine, or those that point at a version.
func (s *Service) listStateMachineAliases(c *core.Ctx, in *struct {
	StateMachineArn, NextToken string
	MaxResults                 int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.resolve(in.StateMachineArn)
	if err != nil {
		return nil, err
	}
	if t.a != nil {
		return nil, core.Errorf(400, "InvalidArn", "Invalid Arn: 'Resource type not valid in this context: %s'", in.StateMachineArn)
	}
	list := []map[string]any{}
	for _, k := range core.SortedKeys(s.st.Aliases) {
		a := s.st.Aliases[k]
		if a.MachineARN == t.m.ARN && (t.v == nil || slices.ContainsFunc(a.Routes, func(r route) bool { return r.Version == t.v.ARN })) {
			list = append(list, map[string]any{"stateMachineAliasArn": a.ARN, "creationDate": core.Time(a.Created)})
		}
	}
	return page("stateMachineAliases", list, in.MaxResults, in.NextToken), nil
}

// dropVersions removes the versions and aliases of a deleted machine; lock held.
func (s *Service) dropVersions(m *Machine) {
	for k, v := range s.st.Versions {
		if v.MachineARN == m.ARN {
			delete(s.st.Versions, k)
		}
	}
	for k, a := range s.st.Aliases {
		if a.MachineARN == m.ARN {
			delete(s.st.Aliases, k)
		}
	}
}
