package cognito

import (
	"encoding/json"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// GetTokensFromRefreshToken is REFRESH_TOKEN_AUTH with refresh token rotation:
// when the client turns RefreshTokenRotation on, the call also returns a new
// refresh token and the old one stops working after RetryGracePeriodSeconds.
// Access and id tokens made earlier stay valid until they expire.
// ponytail: InitiateAuth REFRESH_TOKEN_AUTH never rotates.

// rotation reads the client's RefreshTokenRotation setting (API shape).
func (cl *Client) rotation() (on bool, grace int) {
	var r struct {
		Feature                 string
		RetryGracePeriodSeconds int
	}
	b, _ := json.Marshal(cl.RefreshTokenRotation)
	json.Unmarshal(b, &r)
	return r.Feature == "ENABLED", r.RetryGracePeriodSeconds
}

// checkRotation validates RefreshTokenRotation when a client is saved.
func (cl *Client) checkRotation() error {
	if cl.RefreshTokenRotation == nil {
		return nil
	}
	var r struct {
		Feature                 string
		RetryGracePeriodSeconds int
	}
	b, _ := json.Marshal(cl.RefreshTokenRotation)
	if json.Unmarshal(b, &r) != nil || r.Feature != "" && r.Feature != "ENABLED" && r.Feature != "DISABLED" {
		return invalid("RefreshTokenRotation.Feature must be ENABLED or DISABLED.")
	}
	if r.RetryGracePeriodSeconds < 0 || r.RetryGracePeriodSeconds > 60 {
		return invalid("RefreshTokenRotation.RetryGracePeriodSeconds must be between 0 and 60.")
	}
	return nil
}

func (s *Service) getTokensFromRefreshToken(c *core.Ctx, in *struct {
	RefreshToken, ClientId, ClientSecret string
	ClientMetadata                       map[string]string
}) (any, error) {
	s.mu.Lock()
	p, cl, err := s.client(in.ClientId)
	var x *Session
	var jti string
	switch {
	case err != nil:
	case cl.ClientSecret != in.ClientSecret:
		err = core.Errorf(400, "NotAuthorizedException", "Client authentication failed.")
	case !cl.allows("REFRESH_TOKEN_AUTH"):
		err = invalid("REFRESH_TOKEN_AUTH flow not enabled for this client")
	default:
		x, jti, err = p.session(cl, in.RefreshToken)
	}
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	var next string
	oldJTI, was := jti, *x
	rotate, grace := cl.rotation()
	if rotate { // the new refresh token gets its own session, the old one retires
		next = core.RandHex(48)
		fresh := *x
		fresh.Rotated, fresh.Grace = time.Time{}, 0
		p.Sessions[originJTI(next)] = &fresh
		old := *x
		if old.Rotated.IsZero() {
			old.Rotated, old.Grace = time.Now(), grace
		}
		p.Sessions[jti] = &old
		jti = originJTI(next)
	}
	s.mu.Unlock()
	t, err := s.issue(c, grant{pool: p.ID, user: x.Username, client: cl.ClientId, source: "TokenGeneration_RefreshTokens",
		scope: x.Scope, meta: in.ClientMetadata, jti: jti})
	if err != nil {
		if next != "" { // the refresh did not happen: the old token stays as it was
			s.mu.Lock()
			delete(p.Sessions, jti)
			p.Sessions[oldJTI] = &was
			s.mu.Unlock()
		}
		return nil, err
	}
	t.RefreshToken = next
	return map[string]any{"AuthenticationResult": t}, nil
}
