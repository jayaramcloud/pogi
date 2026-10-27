package core

import (
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

const (
	sigAlgo     = "AWS4-HMAC-SHA256"
	maxSkew     = 15 * time.Minute
	sigMismatch = "The request signature we calculated does not match the signature you provided. Check your AWS Secret Access Key and signing method."
)

// secretFor returns the secret of an access key: registered identities (IAM
// keys, STS sessions, Lambda roles) carry their own; the default key "test"
// and 12-digit account ids used as keys (the LocalStack convention) use "test".
// ponytail: session tokens are not checked, the secret alone proves the key (expiry is, in check).
func (h *Hub) secretFor(akid string) (string, bool) {
	if id, ok := h.Identity(akid); ok {
		return id.Secret, true
	}
	if akid == "test" || len(akid) == 12 && strings.Trim(akid, "0123456789") == "" {
		return "test", true
	}
	return "", false
}

// Credentials returns the account an access key acts in and its secret (ok
// is false for an unknown key), for logins outside SigV4 such as SES SMTP.
func (h *Hub) Credentials(akid string) (account, secret string, ok bool) {
	account, _ = h.accountFor(akid, h.Cfg.Region)
	secret, ok = h.secretFor(akid)
	return account, secret, ok
}

// unsignedOK are x-amz-* headers a presigned S3 request may carry without signing them (browsers add the first).
var unsignedOK = map[string]bool{"x-amz-user-agent": true, "x-amz-content-sha256": true, "x-amz-date": true, "x-amz-security-token": true}

// verify checks the SigV4 signature of an AWS request when Config.ValidateSigs
// is on: header-signed and presigned requests, the 15-minute clock skew and
// presigned expiry. The payload hash is x-amz-content-sha256 when it is
// UNSIGNED-PAYLOAD or STREAMING-*, else the SHA-256 of the body, so a body
// that does not match its signed hash fails too.
// ponytail: per-chunk signatures not verified, aws-chunked bodies check the seed signature only.
// ponytail: anonymous requests, SigV2 ("AWS akid:sig") and SigV4a are not checked; S3 POST uploads use VerifyPolicy.
func (h *Hub) verify(c *Ctx) *Error {
	r := c.req
	if !h.Cfg.ValidateSigs || c.Cloud != AWS || r == nil {
		return nil
	}
	q := r.URL.Query()
	presigned := q.Has("X-Amz-Signature")
	var cred, signed, sig, date string
	switch auth := r.Header.Get("Authorization"); {
	case strings.HasPrefix(auth, sigAlgo+" "):
		f := map[string]string{}
		for _, kv := range strings.Split(auth[len(sigAlgo)+1:], ",") {
			k, v, _ := strings.Cut(strings.TrimSpace(kv), "=")
			f[k] = v
		}
		cred, signed, sig, date, presigned = f["Credential"], f["SignedHeaders"], f["Signature"], r.Header.Get("X-Amz-Date"), false
	case presigned:
		cred, signed, sig, date = q.Get("X-Amz-Credential"), q.Get("X-Amz-SignedHeaders"), q.Get("X-Amz-Signature"), q.Get("X-Amz-Date")
	default:
		return nil
	}
	s3 := c.Service == "s3"
	code := "SignatureDoesNotMatch" // Query, REST-XML and S3
	if svc := h.byName[c.Service]; r.Header.Get("X-Amz-Target") != "" || r.Header.Get("Smithy-Protocol") != "" || svc != nil && svc.Info().Protocol == "rest-json" {
		code = "InvalidSignatureException"
	}

	if s3 && presigned { // S3 refuses a presigned request carrying an x-amz-* header that the URL does not sign (floci #4150)
		for name := range r.Header {
			name = strings.ToLower(name)
			if strings.HasPrefix(name, "x-amz-") && !slices.Contains(strings.Split(signed, ";"), name) && !unsignedOK[name] {
				return &Error{403, "AccessDenied", "There were headers present in the request which were not signed: " + name}
			}
		}
	}
	t, err := time.Parse("20060102T150405Z", date)
	if err != nil {
		return &Error{403, code, "The request must carry a valid X-Amz-Date, got " + strconv.Quote(date) + "."}
	}
	ttl := maxSkew
	if n, err := strconv.Atoi(q.Get("X-Amz-Expires")); presigned && err == nil {
		ttl = time.Duration(n) * time.Second
	}
	if late := time.Since(t); late > ttl || !presigned && -late > maxSkew {
		switch {
		case s3 && presigned:
			return &Error{403, "AccessDenied", "Request has expired"}
		case s3:
			return &Error{403, "RequestTimeTooSkewed", "The difference between the request time and the current time is too large."}
		}
		return &Error{403, code, "Signature expired: " + date + " is outside the allowed time window."}
	}

	hash := r.Header.Get("X-Amz-Content-Sha256")
	if hash == "" && presigned && s3 {
		hash = cmp.Or(q.Get("X-Amz-Content-Sha256"), "UNSIGNED-PAYLOAD")
	}
	if hash != "UNSIGNED-PAYLOAD" && !strings.HasPrefix(hash, "STREAMING-") {
		sum := sha256.Sum256(Body(r))
		hash = hex.EncodeToString(sum[:])
	}

	uri := r.URL.EscapedPath()
	if !s3 { // S3 signs the path as sent; the others normalize it and encode it again
		clean := path.Clean("/" + uri)
		if strings.HasSuffix(uri, "/") && clean != "/" {
			clean += "/"
		}
		uri = strings.ReplaceAll(escape(clean), "%2F", "/")
	}
	var query []string
	for k, vs := range q {
		for _, v := range vs {
			if !presigned || k != "X-Amz-Signature" {
				query = append(query, escape(k)+"="+escape(v))
			}
		}
	}
	slices.SortFunc(query, func(a, b string) int { // by name, then value
		ak, av, _ := strings.Cut(a, "=")
		bk, bv, _ := strings.Cut(b, "=")
		return cmp.Or(strings.Compare(ak, bk), strings.Compare(av, bv))
	})
	var headers strings.Builder
	for _, name := range strings.Split(signed, ";") {
		vs := r.Header.Values(name)
		switch {
		case name == "host":
			vs = []string{r.Host}
		case name == "content-length" && len(vs) == 0:
			vs = []string{strconv.FormatInt(r.ContentLength, 10)}
		}
		clean := make([]string, len(vs))
		for i, v := range vs {
			clean[i] = strings.Join(strings.Fields(v), " ")
		}
		headers.WriteString(name + ":" + strings.Join(clean, ",") + "\n")
	}
	canonical := strings.Join([]string{r.Method, cmp.Or(uri, "/"), strings.Join(query, "&"), headers.String(), signed, hash}, "\n")
	sum := sha256.Sum256([]byte(canonical))
	_, scope, _ := strings.Cut(cred, "/")
	return h.check(cred, sigAlgo+"\n"+date+"\n"+scope+"\n"+hex.EncodeToString(sum[:]), sig, s3, code)
}

// VerifyPresigned checks a presigned URL that never arrives as an HTTP
// request, like an RDS IAM auth token, the way verify checks requests;
// service is a registered one, whose error codes apply.
func (h *Hub) VerifyPresigned(service, rawURL string) error {
	r, err := http.NewRequest(http.MethodGet, rawURL, http.NoBody)
	if err != nil {
		return err
	}
	if e := h.verify(&Ctx{Hub: h, Cloud: AWS, Service: service, req: r}); e != nil {
		return e
	}
	return nil
}

// VerifyPolicy checks an S3 browser POST upload when Config.ValidateSigs is
// on: the x-amz-signature form field over the base64 policy, then the
// policy's expiration. Uploads without a SigV4 signature pass.
// ponytail: policy conditions are not enforced.
func (h *Hub) VerifyPolicy(credential, policy, signature string) *Error {
	if !h.Cfg.ValidateSigs || signature == "" {
		return nil
	}
	if e := h.check(credential, policy, signature, true, "SignatureDoesNotMatch"); e != nil {
		return e
	}
	var doc struct {
		Expiration time.Time `json:"expiration"`
	}
	raw, _ := base64.StdEncoding.DecodeString(policy)
	if json.Unmarshal(raw, &doc) != nil || time.Now().After(doc.Expiration) {
		return &Error{403, "AccessDenied", "Invalid according to Policy: Policy expired."}
	}
	return nil
}

// check verifies sig over toSign with the signing key of credential
// ("akid/date/region/service/aws4_request").
func (h *Hub) check(credential, toSign, sig string, s3 bool, code string) *Error {
	p := strings.Split(credential, "/")
	secret, ok := h.secretFor(p[0])
	switch {
	case !ok && s3:
		return &Error{403, "InvalidAccessKeyId", "The AWS Access Key Id you provided does not exist in our records."}
	case !ok:
		return &Error{403, "InvalidClientTokenId", "The security token included in the request is invalid."}
	case len(p) != 5 || !hmac.Equal([]byte(sign(secret, p[1], p[2], p[3], toSign)), []byte(sig)):
		return &Error{403, code, sigMismatch}
	}
	if id, _ := h.Identity(p[0]); !id.Expires.IsZero() && time.Now().After(id.Expires) { // expired STS credentials
		expired := "ExpiredToken"
		if code == "InvalidSignatureException" { // JSON protocols add "Exception"
			expired += "Exception"
		}
		return &Error{403, expired, "The security token included in the request is expired"}
	}
	return nil
}

// sign derives the SigV4 signing key and returns the hex signature of s.
func sign(secret, date, region, service, s string) string {
	k := []byte("AWS4" + secret)
	for _, part := range []string{date, region, service, "aws4_request", s} {
		m := hmac.New(sha256.New, k)
		m.Write([]byte(part))
		k = m.Sum(nil)
	}
	return hex.EncodeToString(k)
}

// escape is SigV4's URI encoding: everything but A-Z a-z 0-9 - _ . ~ as %XX.
func escape(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
