package cognito

import (
	"slices"

	"github.com/nrndev/pogi/internal/core"
)

// USER_AUTH is choice-based sign-in: the client names a first factor
// (PREFERRED_CHALLENGE) or picks one from SELECT_CHALLENGE's AvailableChallenges.
// Supported first factors: PASSWORD, PASSWORD_SRP, EMAIL_OTP and SMS_OTP.
// ponytail: WEB_AUTHN (passkeys) is not supported, and a one-time-password
// sign-in skips MFA.

// available lists the first factors the pool allows and the user (nil for a
// user that does not exist) can use; lock held.
func (p *Pool) available(u *User) []string {
	factors := []any{"PASSWORD"}
	if pol := p.Settings.Policies; pol != nil {
		if sp, ok := pol.SignInPolicy.(map[string]any); ok {
			if l, ok := sp["AllowedFirstAuthFactors"].([]any); ok && len(l) > 0 {
				factors = l
			}
		}
	}
	var out []string
	for _, f := range factors {
		switch f {
		case "PASSWORD":
			out = append(out, "PASSWORD", "PASSWORD_SRP")
		case "EMAIL_OTP":
			if u != nil && u.Status == "CONFIRMED" && u.Attrs["email"] != "" && u.Attrs["email_verified"] == "true" {
				out = append(out, "EMAIL_OTP")
			}
		case "SMS_OTP":
			if u != nil && u.Status == "CONFIRMED" && u.Attrs["phone_number"] != "" && u.Attrs["phone_number_verified"] == "true" {
				out = append(out, "SMS_OTP")
			}
		}
	}
	return out
}

// choices is available for the user of a sign-in.
func (s *Service) choices(ch *challenge) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(ch.pool)
	if err != nil {
		return nil, err
	}
	if ch.ghost {
		return p.available(nil), nil
	}
	_, u, _, err := s.grantee(ch.grant)
	if err != nil {
		return nil, err
	}
	return p.available(u), nil
}

// userAuth starts USER_AUTH: straight to the preferred first factor when it
// is available, else SELECT_CHALLENGE.
func (s *Service) userAuth(c *core.Ctx, ch *challenge, params map[string]string) (any, error) {
	avail, err := s.choices(ch)
	if err != nil {
		return nil, err
	}
	if pref := params["PREFERRED_CHALLENGE"]; slices.Contains(avail, pref) {
		return s.runFactor(c, ch, pref, params["SRP_A"])
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	res := s.ask(ch, "SELECT_CHALLENGE", map[string]string{})
	res.AvailableChallenges = avail
	return res, nil
}

// runFactor starts one first factor: PASSWORD asks for the password, PASSWORD_SRP
// continues with PASSWORD_VERIFIER, and the OTP factors send a code.
func (s *Service) runFactor(c *core.Ctx, ch *challenge, factor, srpA string) (any, error) {
	switch factor {
	case "PASSWORD":
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.ask(ch, "PASSWORD", map[string]string{}), nil
	case "PASSWORD_SRP":
		ch.srpA = srpA
		if ch.ghost { // a verifier no password can answer
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.ask(ch, "PASSWORD_VERIFIER", map[string]string{"SALT": core.RandHex(16), "SRP_B": core.RandHex(384),
				"USERNAME": ch.user, "USER_ID_FOR_SRP": ch.user}), nil
		}
		return s.srpChallenge(ch)
	}
	attr, medium := "email", "EMAIL"
	if factor == "SMS_OTP" {
		attr, medium = "phone_number", "SMS"
	}
	s.mu.Lock()
	p, u, _, err := s.grantee(ch.grant)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	ch.code = core.RandString(6, digits)
	msg := p.sendTo(u, attr, ch.code, "Authentication", ch.client, ch.meta)
	res := s.ask(ch, factor, map[string]string{"CODE_DELIVERY_DELIVERY_MEDIUM": medium, "CODE_DELIVERY_DESTINATION": mask(u.Attrs[attr])})
	s.mu.Unlock()
	return res, s.deliver(msg)
}

// firstFactor answers SELECT_CHALLENGE, PASSWORD, EMAIL_OTP and SMS_OTP.
func (s *Service) firstFactor(c *core.Ctx, ch *challenge, r map[string]string) (any, error) {
	switch ch.name {
	case "SELECT_CHALLENGE":
		avail, err := s.choices(ch)
		if err != nil {
			return nil, err
		}
		if !slices.Contains(avail, r["ANSWER"]) {
			return nil, invalid("Invalid ANSWER %q: choose one of %v.", r["ANSWER"], avail)
		}
		return s.runFactor(c, ch, r["ANSWER"], r["SRP_A"])
	case "PASSWORD":
		if err := s.password(ch, r["PASSWORD"]); err != nil {
			return nil, err
		}
		return s.passed(c, ch)
	}
	if code := r[ch.name+"_CODE"]; ch.code == "" || code != ch.code {
		return nil, core.Errorf(400, "CodeMismatchException", "Invalid code or auth state for the user.")
	}
	return s.finish(c, ch)
}
