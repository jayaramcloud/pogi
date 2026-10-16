package cognito

import (
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Software token MFA: AssociateSoftwareToken gives the secret, VerifySoftwareToken
// proves the authenticator has it. Both work for a signed-in user (AccessToken)
// and in the middle of a sign-in that asked for MFA_SETUP (Session).

func (p *Pool) totpEnabled() bool {
	m, _ := p.Mfa.SoftwareTokenMfaConfiguration.(map[string]any)
	on, _ := m["Enabled"].(bool)
	return on
}

// mfaCodeOK checks an SMS_MFA or SOFTWARE_TOKEN_MFA answer. A user with a
// verified software token needs a real TOTP code.
// ponytail: a user whose method was only switched on with SetUserMFAPreference
// has no secret, so 123456 passes (SMS codes accept it too).
func (s *Service) mfaCodeOK(ch *challenge, code string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, u, _, err := s.grantee(ch.grant)
	switch {
	case err != nil:
		return false
	case ch.name == "SOFTWARE_TOKEN_MFA" && u.TOTP != "":
		return totpOK(u.TOTP, code, time.Now())
	}
	return code == "123456" || ch.code != "" && code == ch.code
}

// setupSession finds the MFA_SETUP sign-in a Session belongs to; lock held.
func (s *Service) setupSession(key string) (*challenge, error) {
	ch := s.pending[key]
	if ch == nil || time.Now().After(ch.exp) || ch.name != "MFA_SETUP" {
		return nil, core.Errorf(400, "NotAuthorizedException", "Invalid session for the user, session is expired.")
	}
	return ch, nil
}

// rekey gives a sign-in in progress a fresh Session, like AWS does after each call; lock held.
func (s *Service) rekey(ch *challenge) string {
	delete(s.pending, ch.key)
	b := make([]byte, 48)
	rand.Read(b)
	ch.key, ch.exp = base64.StdEncoding.EncodeToString(b), time.Now().Add(3*time.Minute)
	s.pending[ch.key] = ch
	return ch.key
}

func (s *Service) associateSoftwareToken(c *core.Ctx, in *struct{ AccessToken, Session string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	secret := newTOTPSecret()
	if in.AccessToken != "" {
		_, u, _, err := s.authed(in.AccessToken, adminScope)
		if err != nil {
			return nil, err
		}
		u.TOTPNew = secret
		return map[string]any{"SecretCode": secret}, nil
	}
	ch, err := s.setupSession(in.Session)
	if err != nil {
		return nil, err
	}
	ch.secret = secret
	return map[string]any{"SecretCode": secret, "Session": s.rekey(ch)}, nil
}

func (s *Service) verifySoftwareToken(c *core.Ctx, in *struct{ AccessToken, Session, UserCode, FriendlyDeviceName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	mismatch := core.Errorf(400, "EnableSoftwareTokenMFAException", "Code mismatch and fail enable Software Token MFA")
	if in.AccessToken != "" {
		_, u, _, err := s.authed(in.AccessToken, adminScope)
		switch {
		case err != nil:
			return nil, err
		case u.TOTPNew == "":
			return nil, core.Errorf(400, "SoftwareTokenMFANotFoundException", "Software token MFA has not been associated with this user.")
		case !totpOK(u.TOTPNew, in.UserCode, time.Now()):
			return nil, mismatch
		}
		u.TOTP, u.TOTPNew = u.TOTPNew, ""
		return map[string]any{"Status": "SUCCESS"}, nil
	}
	ch, err := s.setupSession(in.Session)
	switch {
	case err != nil:
		return nil, err
	case ch.secret == "":
		return nil, core.Errorf(400, "SoftwareTokenMFANotFoundException", "Software token MFA has not been associated with this user.")
	case !totpOK(ch.secret, in.UserCode, time.Now()):
		return nil, mismatch
	}
	ch.verified = true
	return map[string]any{"Status": "SUCCESS", "Session": s.rekey(ch)}, nil
}

// finishSetup answers MFA_SETUP: the verified token becomes the user's MFA method and the sign-in completes.
func (s *Service) finishSetup(c *core.Ctx, ch *challenge) (any, error) {
	s.mu.Lock()
	_, u, _, err := s.grantee(ch.grant)
	if err == nil && !ch.verified {
		err = invalid("Software token MFA was not set up: call AssociateSoftwareToken and VerifySoftwareToken first.")
	}
	if err == nil {
		u.TOTP = ch.secret
		u.setMFA(&userInput{SoftwareTokenMfaSettings: &mfaSetting{Enabled: true, PreferredMfa: u.Preferred == ""}})
	}
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return s.finish(c, ch)
}
