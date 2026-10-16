package apim

import (
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// validate-jwt: the token comes from a header (Authorization by default), a query parameter or token-value; its signature
// is checked against the keys of its <openid-config> documents and <issuer-signing-keys>, then exp, nbf, audiences,
// issuers and required claims.
// ponytail: RS256/384/512 and HS256/384/512 only (no certificate-id keys, token decryption or output-token-variable-name),
// and the openid-config document and its keys are fetched on every request. Upgrade path: a cache with the document's TTL.
func (e *exec) validateJWT(n *node) (*reply, error) {
	status := http.StatusUnauthorized
	if c, err := strconv.Atoi(n.attr["failed-validation-httpcode"]); err == nil {
		status = c
	}
	msg := n.attr["failed-validation-error-message"]
	if msg == "" {
		msg = "Unauthorized. Access token is missing or invalid."
	}
	fail := func(why string) (*reply, error) {
		rep := errReply(status, msg)
		if status == http.StatusUnauthorized {
			rep.hdr.Set("WWW-Authenticate", `Bearer error="invalid_token", error_description="`+why+`"`)
		}
		return rep, nil
	}

	var tok string
	switch {
	case n.attr["token-value"] != "":
		v, err := e.attr(n, "token-value")
		if err != nil {
			return nil, err
		}
		tok = v
	case n.attr["query-parameter-name"] != "":
		tok = e.query.Get(n.attr["query-parameter-name"])
	default:
		name := n.attr["header-name"]
		if name == "" {
			name = "Authorization"
		}
		tok = e.hdr.Get(name)
		scheme := n.attr["require-scheme"]
		if scheme == "" && strings.EqualFold(name, "Authorization") {
			scheme = "Bearer"
		}
		if scheme != "" {
			rest, ok := strings.CutPrefix(tok, scheme+" ")
			if !ok && tok != "" {
				return fail("the scheme is not " + scheme)
			}
			tok = rest
		}
	}
	if tok == "" {
		return fail("the token is missing")
	}
	p := strings.Split(tok, ".")
	if len(p) != 3 {
		return fail("the token is not a JWT")
	}
	var head struct{ Alg, Kid string }
	claims := map[string]any{}
	if json.Unmarshal(b64(p[0]), &head) != nil || json.Unmarshal(b64(p[1]), &claims) != nil {
		return fail("the token is malformed")
	}
	sig := b64(p[2])

	keys, err := e.signingKeys(n)
	if err != nil {
		return fail(err.Error())
	}
	if !slices.ContainsFunc(keys, func(k signingKey) bool { return k.verify(head.Alg, head.Kid, p[0]+"."+p[1], sig) }) {
		return fail("the signature is invalid")
	}

	skew := 0.0
	if v, err := strconv.ParseFloat(n.attr["clock-skew"], 64); err == nil {
		skew = v
	}
	now := float64(time.Now().Unix())
	if exp, ok := claims["exp"].(float64); !ok || now > exp+skew {
		return fail("the token is expired")
	}
	if nbf, ok := claims["nbf"].(float64); ok && now < nbf-skew {
		return fail("the token is not valid yet")
	}
	if auds := n.child("audiences").kidTexts("audience"); len(auds) > 0 && !hasAny(claims["aud"], auds) {
		return fail("the audience is not accepted")
	}
	if iss := n.child("issuers").kidTexts("issuer"); len(iss) > 0 && !hasAny(claims["iss"], iss) {
		return fail("the issuer is not accepted")
	}
	// required-claims: every claim with match="all" (the default) must hold, and one with match="any" if there are some
	all, anyOK, anySeen := true, false, false
	for _, c := range n.child("required-claims").kidNodes() {
		want := c.kidTexts("value")
		have, present := claims[c.attr["name"]]
		ok := c.name == "claim" && present && (len(want) == 0 || hasAny(have, want))
		if c.attr["match"] == "any" {
			anySeen, anyOK = true, anyOK || ok
		} else {
			all = all && ok
		}
	}
	if !all || anySeen && !anyOK {
		return fail("a required claim is missing")
	}
	return nil, nil
}

// hasAny reports whether a claim (a string, a space-separated string such as scp, or an array) has one of want.
func hasAny(claim any, want []string) bool {
	var have []string
	switch c := claim.(type) {
	case string:
		have = append(strings.Fields(c), c)
	case []any:
		for _, x := range c {
			have = append(have, fmt.Sprint(x))
		}
	}
	return slices.ContainsFunc(have, func(h string) bool { return slices.Contains(want, h) })
}

func b64(s string) []byte {
	b, _ := base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
	return b
}

// signingKey is a key a token may be signed with: an RSA public key (kid from a JWKS) or a symmetric one.
type signingKey struct {
	kid string
	rsa *rsa.PublicKey
	hs  []byte
}

var algs = map[string]crypto.Hash{"RS256": crypto.SHA256, "RS384": crypto.SHA384, "RS512": crypto.SHA512,
	"HS256": crypto.SHA256, "HS384": crypto.SHA384, "HS512": crypto.SHA512}

func (k signingKey) verify(alg, kid, msg string, sig []byte) bool {
	h, ok := algs[alg]
	if !ok || !h.Available() {
		return false
	}
	if k.rsa != nil && strings.HasPrefix(alg, "RS") && (kid == "" || k.kid == "" || kid == k.kid) {
		hh := h.New()
		hh.Write([]byte(msg))
		return rsa.VerifyPKCS1v15(k.rsa, h, hh.Sum(nil), sig) == nil
	}
	if k.hs != nil && strings.HasPrefix(alg, "HS") {
		m := hmac.New(h.New, k.hs)
		m.Write([]byte(msg))
		return hmac.Equal(m.Sum(nil), sig)
	}
	return false
}

// signingKeys gathers the keys of a validate-jwt policy.
func (e *exec) signingKeys(n *node) ([]signingKey, error) {
	var keys []signingKey
	for _, k := range n.child("issuer-signing-keys").kidNodes() {
		if k.name != "key" {
			continue
		}
		b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(k.text))
		if err != nil {
			return nil, errors.New("an issuer signing key is not base64")
		}
		keys = append(keys, signingKey{hs: b})
	}
	for _, o := range n.kids {
		if o.name != "openid-config" {
			continue
		}
		var doc struct {
			JWKSURI string `json:"jwks_uri"`
		}
		if err := e.s.fetch(o.attr["url"], &doc); err != nil {
			return nil, fmt.Errorf("the openid-config %s could not be read", o.attr["url"])
		}
		var set struct {
			Keys []struct{ Kty, Kid, N, E string }
		}
		if err := e.s.fetch(doc.JWKSURI, &set); err != nil {
			return nil, fmt.Errorf("the keys %s could not be read", doc.JWKSURI)
		}
		for _, k := range set.Keys {
			if k.Kty == "RSA" {
				keys = append(keys, signingKey{kid: k.Kid, rsa: &rsa.PublicKey{N: new(big.Int).SetBytes(b64(k.N)), E: int(new(big.Int).SetBytes(b64(k.E)).Int64())}})
			}
		}
	}
	if len(keys) == 0 {
		return nil, errors.New("the policy has no signing keys")
	}
	return keys, nil
}

func (n *node) kidNodes() []*node {
	if n == nil {
		return nil
	}
	return n.kids
}

// entraHosts are the hosts of Entra ID: pogi answers for them in-process, so that a policy written for Azure works.
var entraHosts = []string{"login.microsoftonline.com", "login.windows.net", "sts.windows.net", "login.microsoft.com"}

// fetch GETs a JSON document: from pogi's Entra when the URL is an Entra one, else over the network.
func (s *Service) fetch(u string, out any) error {
	pu, err := url.Parse(u)
	if err != nil || pu.Host == "" {
		return errors.New("bad URL")
	}
	var status int
	var body []byte
	if slices.Contains(entraHosts, pu.Hostname()) {
		req, _ := http.NewRequest(http.MethodGet, "http://localhost"+pu.RequestURI(), nil)
		req.Host = pu.Host
		status, body = s.h.DoService("entra", req)
	} else {
		c := http.Client{Timeout: 5 * time.Second}
		res, err := c.Get(u)
		if err != nil {
			return err
		}
		defer res.Body.Close()
		status = res.StatusCode
		body, _ = io.ReadAll(res.Body)
	}
	if status != http.StatusOK {
		return fmt.Errorf("status %d", status)
	}
	return json.Unmarshal(body, out)
}
