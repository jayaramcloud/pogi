// Package sts emulates AWS STS (Query protocol). Credentials returned by
// AssumeRole map to the role's account, so multi-account setups work.
package sts

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Service is the STS emulator.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
}

// New creates the STS service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.rpc = &core.RPC{XMLNS: "https://sts.amazonaws.com/doc/2011-06-15/", Ops: map[string]core.Handler{
		"GetCallerIdentity":          core.H(s.getCallerIdentity),
		"AssumeRole":                 core.H(s.assumeRole),
		"AssumeRoleWithWebIdentity":  core.H(s.assumeRole),
		"AssumeRoleWithSAML":         core.H(s.assumeRole),
		"GetSessionToken":            core.H(s.getSessionToken),
		"GetFederationToken":         core.H(s.getFederationToken),
		"GetAccessKeyInfo":           core.H(s.getAccessKeyInfo),
		"DecodeAuthorizationMessage": core.H(s.decode),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "sts", Cloud: core.AWS, Title: "STS", Signing: []string{"sts"}, Targets: []string{"AWSSecurityTokenServiceV20110615"},
		Match: anonymous, Protocol: "query"}
}

// anonymous takes the Query calls SDKs send unsigned, AssumeRoleWithWebIdentity
// and AssumeRoleWithSAML, which would otherwise fall to S3.
func anonymous(r *http.Request) bool {
	return r.Method == http.MethodPost && r.URL.Path == "/" && r.Header.Get("Authorization") == "" && !r.URL.Query().Has("X-Amz-Credential") &&
		strings.HasPrefix(r.Header.Get("Content-Type"), "application/x-www-form-urlencoded")
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if anonymous(r) {
		s.hub.CORS(w, r) // the gateway leaves CORS to the services it routes by Match
	}
	s.rpc.ServeHTTP(w, r)
}

func (s *Service) getCallerIdentity(c *core.Ctx, _ *struct{}) (any, error) {
	userID := c.AccessKey
	if id, ok := s.hub.Identity(c.AccessKey); ok {
		userID = id.UserID
	} else if strings.HasSuffix(c.Principal, ":root") {
		userID = c.Account
	}
	return map[string]string{"UserId": userID, "Account": c.Account, "Arn": c.Principal}, nil
}

type credentials struct {
	AccessKeyId     string
	SecretAccessKey string
	SessionToken    string
	Expiration      time.Time
}

// issue mints temporary credentials bound to an identity. DurationSeconds
// below 900 is refused, as AWS does; 0 means the default hour.
func (s *Service) issue(id core.Identity, seconds int) (credentials, error) {
	if seconds == 0 {
		seconds = 3600
	}
	if seconds < 900 {
		return credentials{}, core.Errorf(400, "ValidationError", "1 validation error detected: Value '%d' at 'durationSeconds' failed to satisfy constraint: Member must have value greater than or equal to 900", seconds)
	}
	cr := credentials{
		AccessKeyId:     "ASIA" + core.RandString(16, core.UpperNum),
		SecretAccessKey: core.RandString(40, core.AlphaNum),
		SessionToken:    "FwoGZXIvYXdzE" + core.RandString(180, core.AlphaNum),
		Expiration:      time.Now().UTC().Add(time.Duration(seconds) * time.Second),
	}
	id.Secret, id.Expires = cr.SecretAccessKey, cr.Expiration
	s.hub.AddIdentity(cr.AccessKeyId, id)
	return cr, nil
}

// roleSource is the iam service: whether a role exists in an account.
type roleSource interface {
	RoleExists(account, name string) bool
}

// tokenExpired reads the exp claim of a web identity token (a JWT). pogi does
// not check the signature; a token it cannot read has no expiry to check.
func tokenExpired(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return false
	}
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(parts[1], "="))
	if err != nil {
		return false
	}
	var claims struct {
		Exp float64 `json:"exp"`
	}
	return json.Unmarshal(b, &claims) == nil && claims.Exp > 0 && time.Now().Unix() >= int64(claims.Exp)
}

type assumeRoleInput struct {
	RoleArn          string
	RoleSessionName  string
	DurationSeconds  int
	ProviderId       string
	WebIdentityToken string
}

func (s *Service) assumeRole(c *core.Ctx, in *assumeRoleInput) (any, error) {
	_, _, account, res, ok := core.ParseARN(in.RoleArn)
	if !ok || !strings.HasPrefix(res, "role/") || len(account) != 12 {
		return nil, core.Errorf(400, "ValidationError", "%q is not a valid role ARN", in.RoleArn)
	}
	role := res[strings.LastIndex(res, "/")+1:]
	if iam, ok := s.hub.Service("iam").(roleSource); ok && !iam.RoleExists(account, role) {
		return nil, core.Errorf(403, "AccessDenied", "User: %s is not authorized to perform: sts:AssumeRole on resource: %s", c.Principal, in.RoleArn)
	}
	if c.Op == "AssumeRoleWithWebIdentity" && tokenExpired(in.WebIdentityToken) {
		return nil, core.Errorf(400, "ExpiredTokenException", "Token expired: the web identity token is past its exp time.")
	}
	session := in.RoleSessionName
	if session == "" {
		session = "pogi-session"
	}
	roleID := "AROA" + core.RandString(17, core.UpperNum)
	arn := core.GlobalARN("sts", c.Region, account, "assumed-role/"+role+"/"+session)
	cr, err := s.issue(core.Identity{Account: account, ARN: arn, UserID: roleID + ":" + session}, in.DurationSeconds)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"Credentials":      cr,
		"AssumedRoleUser":  struct{ AssumedRoleId, Arn string }{roleID + ":" + session, arn}, // a struct: Query XML renders maps as entries
		"PackedPolicySize": 6,
	}
	if c.Op == "AssumeRoleWithWebIdentity" {
		out["SubjectFromWebIdentityToken"] = session
		out["Provider"] = in.ProviderId
	}
	return out, nil
}

func (s *Service) getSessionToken(c *core.Ctx, in *struct{ DurationSeconds int }) (any, error) {
	cr, err := s.issue(core.Identity{Account: c.Account, ARN: c.Principal, UserID: c.AccessKey}, in.DurationSeconds)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Credentials": cr}, nil
}

func (s *Service) getFederationToken(c *core.Ctx, in *struct {
	Name            string
	DurationSeconds int
}) (any, error) {
	arn := c.GlobalARN("sts", "federated-user/"+in.Name)
	id := c.Account + ":" + in.Name
	cr, err := s.issue(core.Identity{Account: c.Account, ARN: arn, UserID: id}, in.DurationSeconds)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Credentials": cr, "FederatedUser": struct{ FederatedUserId, Arn string }{id, arn}, "PackedPolicySize": 6}, nil
}

func (s *Service) getAccessKeyInfo(c *core.Ctx, in *struct{ AccessKeyId string }) (any, error) {
	if id, ok := s.hub.Identity(in.AccessKeyId); ok {
		return map[string]string{"Account": id.Account}, nil
	}
	if len(in.AccessKeyId) == 12 && strings.Trim(in.AccessKeyId, "0123456789") == "" {
		return map[string]string{"Account": in.AccessKeyId}, nil
	}
	return map[string]string{"Account": core.DefaultAccount}, nil
}

func (s *Service) decode(c *core.Ctx, in *struct{ EncodedMessage string }) (any, error) {
	return map[string]string{"DecodedMessage": `{"allowed":true,"explicitDeny":false,"matchedStatements":{"items":[]},"failures":{"items":[]},"context":{}}`}, nil
}
