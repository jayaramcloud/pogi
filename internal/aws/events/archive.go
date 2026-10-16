package events

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/pattern"
	"github.com/nrndev/pogi/internal/core"
)

// ---- archives ----

// Archive keeps the events that reach a bus (and match its pattern) for replay.
type Archive struct {
	ArchiveName    string
	ArchiveArn     string
	EventSourceArn string
	Description    string `json:",omitempty"`
	EventPattern   string `json:",omitempty"`
	RetentionDays  int
	State          string
	CreationTime   float64
	EventCount     int
	SizeBytes      int
	Events         []archived `json:",omitempty"` // ponytail: kept forever, prune by RetentionDays if archives grow
	pat            *pattern.Pattern
}

type archived struct {
	At    time.Time
	Event string
}

// out is the API view: counts instead of the events.
func (a *Archive) out() Archive {
	o := *a
	o.Events, o.pat, o.EventCount, o.SizeBytes = nil, nil, len(a.Events), 0
	for _, e := range a.Events {
		o.SizeBytes += len(e.Event)
	}
	return o
}

func (s *Service) archive(c *core.Ctx, name string) (*Archive, error) {
	if a := s.st.Archives[key(c.Account, c.Region, name)]; a != nil {
		return a, nil
	}
	return nil, notFound("Archive %s does not exist.", name)
}

func (s *Service) createArchive(c *core.Ctx, in *Archive) (any, error) {
	if !nameRe.MatchString(in.ArchiveName) {
		return nil, invalid("Archive name %s is not valid.", in.ArchiveName)
	}
	if in.EventPattern != "" {
		if _, err := compilePattern(in.EventPattern); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.ArchiveName)
	if s.st.Archives[k] != nil {
		return nil, exists("Archive %s already exists.", in.ArchiveName)
	}
	b, err := s.findBus(c, in.EventSourceArn)
	if err != nil {
		return nil, err
	}
	a := &Archive{ArchiveName: in.ArchiveName, ArchiveArn: c.ARN("events", "archive/"+in.ArchiveName), EventSourceArn: b.arn(),
		Description: in.Description, EventPattern: in.EventPattern, RetentionDays: in.RetentionDays, State: "ENABLED", CreationTime: core.Unix(time.Now())}
	s.st.Archives[k] = a
	return map[string]any{"ArchiveArn": a.ArchiveArn, "State": a.State, "CreationTime": a.CreationTime}, nil
}

func (s *Service) describeArchive(c *core.Ctx, in *struct{ ArchiveName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.archive(c, in.ArchiveName)
	if err != nil {
		return nil, err
	}
	return a.out(), nil
}

func (s *Service) listArchives(c *core.Ctx, in *struct {
	listInput
	EventSourceArn, State string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Archive
	for k, a := range s.st.Archives {
		if strings.HasPrefix(k, key(c.Account, c.Region, in.NamePrefix)) && (in.EventSourceArn == "" || a.EventSourceArn == in.EventSourceArn) && (in.State == "" || a.State == in.State) {
			out = append(out, a.out())
		}
	}
	items, next := page(out, func(a Archive) string { return a.ArchiveName }, in.NextToken, in.Limit)
	return list("Archives", items, next), nil
}

func (s *Service) updateArchive(c *core.Ctx, in *struct {
	ArchiveName               string
	Description, EventPattern *string
	RetentionDays             *int
}) (any, error) {
	if in.EventPattern != nil && *in.EventPattern != "" {
		if _, err := compilePattern(*in.EventPattern); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.archive(c, in.ArchiveName)
	if err != nil {
		return nil, err
	}
	if in.Description != nil {
		a.Description = *in.Description
	}
	if in.EventPattern != nil {
		a.EventPattern, a.pat = *in.EventPattern, nil
	}
	if in.RetentionDays != nil {
		a.RetentionDays = *in.RetentionDays
	}
	return map[string]any{"ArchiveArn": a.ArchiveArn, "State": a.State, "CreationTime": a.CreationTime}, nil
}

func (s *Service) deleteArchive(c *core.Ctx, in *struct{ ArchiveName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.archive(c, in.ArchiveName); err != nil {
		return nil, err
	}
	delete(s.st.Archives, key(c.Account, c.Region, in.ArchiveName))
	return nil, nil
}

// ---- replays ----

// Replay re-sends archived events to the archive's bus. pogi replays
// synchronously, so a replay is COMPLETED by the time StartReplay returns.
type Replay struct {
	ReplayName     string
	ReplayArn      string
	Description    string `json:",omitempty"`
	EventSourceArn string
	Destination    struct {
		Arn        string
		FilterArns []string `json:",omitempty"`
	}
	State                                         string
	EventStartTime, EventEndTime, ReplayStartTime float64
	ReplayEndTime, EventLastReplayedTime          float64 `json:",omitempty"`
}

func (s *Service) startReplay(c *core.Ctx, r *Replay) (any, error) {
	if !nameRe.MatchString(r.ReplayName) {
		return nil, invalid("Replay name %s is not valid.", r.ReplayName)
	}
	if r.EventStartTime >= r.EventEndTime {
		return nil, invalid("Parameter EventEndTime is not valid. Reason: EventStartTime must be before EventEndTime.")
	}
	evs, err := s.addReplay(c, r)
	if err != nil {
		return nil, err
	}
	account, region, bus := busRef(c.Account, c.Region, r.Destination.Arn)
	for _, a := range evs {
		var ev event
		json.Unmarshal([]byte(a.Event), &ev)
		ev.ReplayName = r.ReplayName
		if err := s.publish(account, region, bus, &ev, r.Destination.FilterArns, 0); err != nil {
			s.hub.Log.Warn("events: replay failed", "replay", r.ReplayName, "err", err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r.State, r.ReplayEndTime = "COMPLETED", core.Unix(time.Now())
	if len(evs) > 0 {
		r.EventLastReplayedTime = core.Unix(evs[len(evs)-1].At)
	}
	return map[string]any{"ReplayArn": r.ReplayArn, "ReplayStartTime": r.ReplayStartTime, "State": "STARTING"}, nil
}

// addReplay registers a replay and returns the archived events it covers.
func (s *Service) addReplay(c *core.Ctx, r *Replay) ([]archived, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, r.ReplayName)
	if s.st.Replays[k] != nil {
		return nil, exists("Replay %s already exists.", r.ReplayName)
	}
	_, region, account, res, _ := core.ParseARN(r.EventSourceArn)
	a := s.st.Archives[key(account, region, strings.TrimPrefix(res, "archive/"))]
	if a == nil {
		return nil, notFound("Archive %s does not exist.", r.EventSourceArn)
	}
	if b, err := s.findBus(c, r.Destination.Arn); err != nil || b.arn() != a.EventSourceArn {
		return nil, invalid("Parameter Destination.Arn is not valid. Reason: Cross event bus replay is not permitted.")
	}
	start, end := fromUnix(r.EventStartTime), fromUnix(r.EventEndTime)
	var evs []archived
	for _, e := range a.Events {
		if !e.At.Before(start) && e.At.Before(end) {
			evs = append(evs, e)
		}
	}
	r.ReplayArn, r.State, r.ReplayStartTime = c.ARN("events", "replay/"+r.ReplayName), "RUNNING", core.Unix(time.Now())
	s.st.Replays[k] = r
	return evs, nil
}

func (s *Service) replay(c *core.Ctx, name string) (*Replay, error) {
	if r := s.st.Replays[key(c.Account, c.Region, name)]; r != nil {
		return r, nil
	}
	return nil, notFound("Replay %s does not exist.", name)
}

func (s *Service) describeReplay(c *core.Ctx, in *struct{ ReplayName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.replay(c, in.ReplayName)
	if err != nil {
		return nil, err
	}
	return *r, nil
}

func (s *Service) listReplays(c *core.Ctx, in *struct {
	listInput
	EventSourceArn, State string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Replay
	for k, r := range s.st.Replays {
		if strings.HasPrefix(k, key(c.Account, c.Region, in.NamePrefix)) && (in.EventSourceArn == "" || r.EventSourceArn == in.EventSourceArn) && (in.State == "" || r.State == in.State) {
			out = append(out, *r)
		}
	}
	items, next := page(out, func(r Replay) string { return r.ReplayName }, in.NextToken, in.Limit)
	return list("Replays", items, next), nil
}

// cancelReplay always finds the replay finished: replays run synchronously.
func (s *Service) cancelReplay(c *core.Ctx, in *struct{ ReplayName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, err := s.replay(c, in.ReplayName)
	if err != nil {
		return nil, err
	}
	return nil, core.Errorf(400, "IllegalStatusException", "Replay %s is not in a valid state for this operation (%s).", r.ReplayName, r.State)
}

// ---- connections ----

// Connection holds the authorization that API destination calls carry.
type Connection struct {
	Name              string
	ConnectionArn     string
	ConnectionState   string
	AuthorizationType string
	Description       string `json:",omitempty"`
	AuthParameters    struct {
		ApiKeyAuthParameters     *apiKeyAuth     `json:",omitempty"`
		BasicAuthParameters      *basicAuth      `json:",omitempty"`
		OAuthParameters          json.RawMessage `json:",omitempty"` // stored only: pogi fetches no tokens
		InvocationHttpParameters *struct {
			HeaderParameters, QueryStringParameters, BodyParameters []struct {
				Key, Value    string
				IsValueSecret bool
			} `json:",omitempty"`
		} `json:",omitempty"`
	}
	SecretArn                                          string
	CreationTime, LastModifiedTime, LastAuthorizedTime float64
}

type apiKeyAuth struct {
	ApiKeyName  string
	ApiKeyValue string `json:",omitempty"`
}

type basicAuth struct {
	Username string
	Password string `json:",omitempty"`
}

// public hides the secrets, as DescribeConnection does.
func (c Connection) public() Connection {
	if k := c.AuthParameters.ApiKeyAuthParameters; k != nil {
		c.AuthParameters.ApiKeyAuthParameters = &apiKeyAuth{ApiKeyName: k.ApiKeyName}
	}
	if b := c.AuthParameters.BasicAuthParameters; b != nil {
		c.AuthParameters.BasicAuthParameters = &basicAuth{Username: b.Username}
	}
	return c
}

func (s *Service) connection(c *core.Ctx, name string) (*Connection, error) {
	if conn := s.st.Connections[key(c.Account, c.Region, name)]; conn != nil {
		return conn, nil
	}
	return nil, notFound("Connection '%s' does not exist.", name)
}

func (s *Service) createConnection(c *core.Ctx, in *Connection) (any, error) {
	switch {
	case !nameRe.MatchString(in.Name):
		return nil, invalid("Connection name %s is not valid.", in.Name)
	case in.AuthorizationType != "API_KEY" && in.AuthorizationType != "BASIC" && in.AuthorizationType != "OAUTH_CLIENT_CREDENTIALS":
		return nil, invalid("AuthorizationType %s is not valid.", in.AuthorizationType)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.Name)
	if s.st.Connections[k] != nil {
		return nil, exists("Connection %s already exists.", in.Name)
	}
	id, now := core.UUID(), core.Unix(time.Now())
	in.ConnectionArn = c.ARN("events", "connection/"+in.Name+"/"+id)
	in.SecretArn = c.ARN("secretsmanager", "secret:events!connection/"+in.Name+"/"+id)
	in.ConnectionState, in.CreationTime, in.LastModifiedTime, in.LastAuthorizedTime = "AUTHORIZED", now, now, now
	s.st.Connections[k] = in
	return in.public(), nil
}

func (s *Service) describeConnection(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conn, err := s.connection(c, in.Name)
	if err != nil {
		return nil, err
	}
	return conn.public(), nil
}

func (s *Service) listConnections(c *core.Ctx, in *struct {
	listInput
	ConnectionState string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Connection
	for k, conn := range s.st.Connections {
		if strings.HasPrefix(k, key(c.Account, c.Region, in.NamePrefix)) && (in.ConnectionState == "" || conn.ConnectionState == in.ConnectionState) {
			out = append(out, conn.public())
		}
	}
	items, next := page(out, func(c Connection) string { return c.Name }, in.NextToken, in.Limit)
	return list("Connections", items, next), nil
}

func (s *Service) deleteConnection(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conn, err := s.connection(c, in.Name)
	if err != nil {
		return nil, err
	}
	delete(s.st.Connections, key(c.Account, c.Region, in.Name))
	out := conn.public()
	out.ConnectionState = "DELETING"
	return out, nil
}

// ---- API destinations ----

// APIDestination is an HTTP endpoint that rules can target.
type APIDestination struct {
	Name                           string
	ApiDestinationArn              string
	ApiDestinationState            string
	ConnectionArn                  string
	Description                    string `json:",omitempty"`
	HttpMethod                     string
	InvocationEndpoint             string
	InvocationRateLimitPerSecond   int `json:",omitempty"`
	CreationTime, LastModifiedTime float64
}

func (s *Service) destination(c *core.Ctx, name string) (*APIDestination, error) {
	if d := s.st.Destinations[key(c.Account, c.Region, name)]; d != nil {
		return d, nil
	}
	return nil, notFound("Failed to describe the api-destination(s). An api-destination '%s' does not exist.", name)
}

func (s *Service) createAPIDestination(c *core.Ctx, in *APIDestination) (any, error) {
	switch {
	case !nameRe.MatchString(in.Name):
		return nil, invalid("Api destination name %s is not valid.", in.Name)
	case !slices.Contains([]string{"POST", "GET", "HEAD", "OPTIONS", "PUT", "PATCH", "DELETE"}, in.HttpMethod):
		return nil, invalid("HttpMethod %s is not valid.", in.HttpMethod)
	case in.InvocationEndpoint == "":
		return nil, invalid("InvocationEndpoint is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c.Account, c.Region, in.Name)
	if s.st.Destinations[k] != nil {
		return nil, exists("An api-destination '%s' already exists.", in.Name)
	}
	if s.connByARN(in.ConnectionArn) == nil {
		return nil, notFound("Connection '%s' does not exist.", in.ConnectionArn)
	}
	now := core.Unix(time.Now())
	in.ApiDestinationArn = c.ARN("events", "api-destination/"+in.Name+"/"+core.UUID())
	in.ApiDestinationState, in.CreationTime, in.LastModifiedTime = "ACTIVE", now, now
	s.st.Destinations[k] = in
	return *in, nil
}

func (s *Service) describeAPIDestination(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.destination(c, in.Name)
	if err != nil {
		return nil, err
	}
	return *d, nil
}

func (s *Service) listAPIDestinations(c *core.Ctx, in *struct {
	listInput
	ConnectionArn string
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []APIDestination
	for k, d := range s.st.Destinations {
		if strings.HasPrefix(k, key(c.Account, c.Region, in.NamePrefix)) && (in.ConnectionArn == "" || d.ConnectionArn == in.ConnectionArn) {
			out = append(out, *d)
		}
	}
	items, next := page(out, func(d APIDestination) string { return d.Name }, in.NextToken, in.Limit)
	return list("ApiDestinations", items, next), nil
}

func (s *Service) deleteAPIDestination(c *core.Ctx, in *struct{ Name string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.destination(c, in.Name); err != nil {
		return nil, err
	}
	delete(s.st.Destinations, key(c.Account, c.Region, in.Name))
	return nil, nil
}

func (s *Service) connByARN(arn string) *Connection {
	for _, c := range s.st.Connections {
		if c.ConnectionArn == arn {
			return c
		}
	}
	return nil
}

var httpClient = &http.Client{Timeout: 10 * time.Second}

// Invoke sends an event to an API destination with its connection's
// authorization and returns the response body (Pipes enrich through it).
// Stored destinations and connections are never modified in place, so they
// are read safely after unlocking.
func (s *Service) Invoke(arn, body string) ([]byte, error) {
	var d *APIDestination
	s.mu.Lock()
	for _, x := range s.st.Destinations {
		if x.ApiDestinationArn == arn {
			d = x
		}
	}
	var conn *Connection
	if d != nil {
		conn = s.connByARN(d.ConnectionArn)
	}
	s.mu.Unlock()
	if d == nil {
		return nil, fmt.Errorf("api destination %s does not exist", arn)
	}
	req, err := http.NewRequest(d.HttpMethod, d.InvocationEndpoint, strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	if conn != nil {
		auth := &conn.AuthParameters
		if k := auth.ApiKeyAuthParameters; k != nil {
			req.Header.Set(k.ApiKeyName, k.ApiKeyValue)
		}
		if b := auth.BasicAuthParameters; b != nil {
			req.SetBasicAuth(b.Username, b.Password)
		}
		if p := auth.InvocationHttpParameters; p != nil {
			for _, h := range p.HeaderParameters {
				req.Header.Set(h.Key, h.Value)
			}
			if len(p.QueryStringParameters) > 0 {
				q := req.URL.Query()
				for _, kv := range p.QueryStringParameters {
					q.Set(kv.Key, kv.Value)
				}
				req.URL.RawQuery = q.Encode()
			}
		}
	}
	res, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	out, err := io.ReadAll(res.Body)
	res.Body.Close()
	if res.StatusCode >= 400 {
		return nil, fmt.Errorf("%s %s returned %s", d.HttpMethod, d.InvocationEndpoint, res.Status)
	}
	return out, err
}
