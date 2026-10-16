package cognito

import (
	"regexp"
	"slices"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

// Identity providers (Google, Facebook, Apple, OIDC, SAML) are stored and
// returned like AWS does, so Terraform and CDK can manage them and the app
// client and the login page can name them.
// ponytail: pogi does not sign anyone in through an external provider; the
// login page lists the providers and /oauth2/authorize?identity_provider=X
// answers 501.

// IdentityProvider is the API shape.
type IdentityProvider struct {
	UserPoolId, ProviderName, ProviderType string
	ProviderDetails                        map[string]string
	AttributeMapping                       map[string]string `json:",omitempty"`
	IdpIdentifiers                         []string          `json:",omitempty"`
	LastModifiedDate, CreationDate         core.Time
}

var (
	providerName = regexp.MustCompile(`^[^_\p{Z}][\p{L}\p{M}\p{S}\p{N}\p{P}][^_\p{Z}]+$`)
	// required keys of ProviderDetails by type
	providerKeys = map[string][]string{
		"Google":          {"client_id", "client_secret", "authorize_scopes"},
		"Facebook":        {"client_id", "client_secret", "authorize_scopes"},
		"LoginWithAmazon": {"client_id", "client_secret", "authorize_scopes"},
		"SignInWithApple": {"client_id", "team_id", "key_id", "private_key", "authorize_scopes"},
		"OIDC":            {"client_id", "oidc_issuer", "attributes_request_method", "authorize_scopes"},
		"SAML":            nil, // MetadataURL or MetadataFile, checked below
	}
)

type idpInput struct {
	UserPoolId, ProviderName, ProviderType, IdpIdentifier, NextToken string
	ProviderDetails, AttributeMapping                                map[string]string
	IdpIdentifiers                                                   []string
	MaxResults                                                       int
}

// checkDetails validates a provider's type and details.
func checkDetails(typ string, d map[string]string) error {
	keys, ok := providerKeys[typ]
	if !ok {
		return invalid("ProviderType %q is not valid: use SAML, Facebook, Google, LoginWithAmazon, SignInWithApple or OIDC.", typ)
	}
	for _, k := range keys {
		if d[k] == "" {
			return invalid("Missing required ProviderDetails key %s for a %s provider.", k, typ)
		}
	}
	if typ == "SAML" && d["MetadataURL"] == "" && d["MetadataFile"] == "" {
		return invalid("A SAML provider needs ProviderDetails MetadataURL or MetadataFile.")
	}
	if m := d["attributes_request_method"]; typ == "OIDC" && m != "GET" && m != "POST" {
		return invalid("attributes_request_method must be GET or POST.")
	}
	return nil
}

func errNoProvider(name string) error {
	return core.Errorf(400, "ResourceNotFoundException", "Identity provider %s does not exist.", name)
}

// identifierTaken reports whether another provider of the pool already uses an identifier; lock held.
func (p *Pool) identifierTaken(ids []string, except string) bool {
	for n, o := range p.Providers {
		if n != except && slices.ContainsFunc(ids, func(id string) bool { return slices.Contains(o.IdpIdentifiers, id) }) {
			return true
		}
	}
	return false
}

func (s *Service) createIdentityProvider(c *core.Ctx, in *idpInput) (any, error) {
	if n := utf8.RuneCountInString(in.ProviderName); n > 32 || !providerName.MatchString(in.ProviderName) || in.ProviderName == "COGNITO" {
		return nil, invalid("ProviderName %q is not valid: 3 to 32 characters, not starting or ending with _ or a space, and not COGNITO.", in.ProviderName)
	}
	if err := checkDetails(in.ProviderType, in.ProviderDetails); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	switch {
	case err != nil:
		return nil, err
	case p.Providers[in.ProviderName] != nil:
		return nil, core.Errorf(400, "DuplicateProviderException", "A provider with the name %s already exists in this user pool.", in.ProviderName)
	case p.identifierTaken(in.IdpIdentifiers, ""):
		return nil, core.Errorf(400, "DuplicateProviderException", "An IdP identifier is already used by another provider in this user pool.")
	}
	now := core.Time(time.Now())
	idp := &IdentityProvider{UserPoolId: p.ID, ProviderName: in.ProviderName, ProviderType: in.ProviderType, ProviderDetails: in.ProviderDetails,
		AttributeMapping: in.AttributeMapping, IdpIdentifiers: in.IdpIdentifiers, CreationDate: now, LastModifiedDate: now}
	p.Providers[idp.ProviderName] = idp
	return map[string]any{"IdentityProvider": idp}, nil
}

// provider finds a provider; lock held.
func (s *Service) provider(poolID, name string) (*Pool, *IdentityProvider, error) {
	p, err := s.pool(poolID)
	if err != nil {
		return nil, nil, err
	}
	idp := p.Providers[name]
	if idp == nil {
		return nil, nil, errNoProvider(name)
	}
	return p, idp, nil
}

func (s *Service) describeIdentityProvider(c *core.Ctx, in *idpInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, idp, err := s.provider(in.UserPoolId, in.ProviderName)
	if err != nil {
		return nil, err
	}
	return map[string]any{"IdentityProvider": idp}, nil
}

func (s *Service) getIdentityProviderByIdentifier(c *core.Ctx, in *idpInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	for _, idp := range p.Providers {
		if slices.Contains(idp.IdpIdentifiers, in.IdpIdentifier) {
			return map[string]any{"IdentityProvider": idp}, nil
		}
	}
	return nil, errNoProvider(in.IdpIdentifier)
}

func (s *Service) updateIdentityProvider(c *core.Ctx, in *idpInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, idp, err := s.provider(in.UserPoolId, in.ProviderName)
	if err != nil {
		return nil, err
	}
	next := *idp
	if in.ProviderDetails != nil {
		next.ProviderDetails = in.ProviderDetails
	}
	if in.AttributeMapping != nil {
		next.AttributeMapping = in.AttributeMapping
	}
	if in.IdpIdentifiers != nil {
		next.IdpIdentifiers = in.IdpIdentifiers
	}
	if err := checkDetails(next.ProviderType, next.ProviderDetails); err != nil {
		return nil, err
	}
	if p.identifierTaken(next.IdpIdentifiers, idp.ProviderName) {
		return nil, core.Errorf(400, "DuplicateProviderException", "An IdP identifier is already used by another provider in this user pool.")
	}
	next.LastModifiedDate = core.Time(time.Now())
	p.Providers[idp.ProviderName] = &next
	return map[string]any{"IdentityProvider": &next}, nil
}

func (s *Service) deleteIdentityProvider(c *core.Ctx, in *idpInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, _, err := s.provider(in.UserPoolId, in.ProviderName)
	if err != nil {
		return nil, err
	}
	delete(p.Providers, in.ProviderName)
	for _, cl := range p.Clients { // a client cannot name a provider that is gone
		if slices.Contains(cl.SupportedIdentityProviders, in.ProviderName) {
			next := *cl
			next.SupportedIdentityProviders = slices.DeleteFunc(slices.Clone(cl.SupportedIdentityProviders), func(x string) bool { return x == in.ProviderName })
			p.Clients[cl.ClientId] = &next
		}
	}
	return nil, nil
}

func (s *Service) listIdentityProviders(c *core.Ctx, in *idpInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, err := s.pool(in.UserPoolId)
	if err != nil {
		return nil, err
	}
	names, next := page(core.SortedKeys(p.Providers), in.NextToken, in.MaxResults)
	out := []map[string]any{}
	for _, n := range names {
		idp := p.Providers[n]
		out = append(out, map[string]any{"ProviderName": n, "ProviderType": idp.ProviderType,
			"LastModifiedDate": idp.LastModifiedDate, "CreationDate": idp.CreationDate})
	}
	return withNext(map[string]any{"Providers": out}, "NextToken", next), nil
}

// checkProviders makes sure a client names only COGNITO and providers of its pool.
func (p *Pool) checkProviders(cl *Client) error {
	for _, n := range cl.SupportedIdentityProviders {
		if n != "COGNITO" && p.Providers[n] == nil {
			return invalid("The provider %s does not exist for User Pool %s.", n, p.ID)
		}
	}
	return nil
}
