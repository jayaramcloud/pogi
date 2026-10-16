package eks

import (
	"cmp"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// authn is the TokenReview webhook of the clusters' API servers, at
// /_aws/eks/authn/<account>/<region>/<cluster>.
func (s *Service) authn(w http.ResponseWriter, r *http.Request) {
	var tr struct {
		APIVersion string `json:"apiVersion"`
		Spec       struct{ Token string }
	}
	json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&tr)
	key := strings.TrimPrefix(r.URL.Path, "/_aws/eks/authn/")
	status := doc{"authenticated": false}
	if user, err := s.review(key, tr.Spec.Token); err != nil {
		status["error"] = err.Error()
		s.hub.Log.Info("eks token rejected", "cluster", key, "reason", err)
	} else {
		status = doc{"authenticated": true, "user": user}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(doc{"apiVersion": cmp.Or(tr.APIVersion, "authentication.k8s.io/v1"), "kind": "TokenReview", "status": status})
}

// review checks a token of `aws eks get-token` the way EKS does: it is
// "k8s-aws-v1." and a base64url presigned STS GetCallerIdentity URL whose
// signature covers the header x-k8s-aws-id: <cluster>. It returns the
// Kubernetes user of the caller.
func (s *Service) review(key, token string) (doc, error) {
	b64, ok := strings.CutPrefix(token, "k8s-aws-v1.")
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(b64, "="))
	if !ok || err != nil {
		return nil, errors.New("not an EKS token")
	}
	u, err := url.Parse(string(raw))
	if err != nil {
		return nil, errors.New("not an EKS token")
	}
	q := u.Query()
	cred := strings.Split(q.Get("X-Amz-Credential"), "/") // akid/date/region/sts/aws4_request
	if q.Get("Action") != "GetCallerIdentity" || len(cred) != 5 || cred[3] != "sts" ||
		!slices.Contains(strings.Split(q.Get("X-Amz-SignedHeaders"), ";"), "x-k8s-aws-id") {
		return nil, errors.New("not a presigned STS GetCallerIdentity URL for a cluster")
	}
	// EKS honours a token for 15 minutes whatever its X-Amz-Expires says (kubectl reuses it for 14)
	if t, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date")); err != nil || time.Since(t).Abs() > 15*time.Minute {
		return nil, errors.New("the token has expired")
	}
	account, rest, _ := strings.Cut(key, "/")
	region, name, _ := strings.Cut(rest, "/")
	id, ok := s.principal(cred[0], region)
	if !ok {
		return nil, fmt.Errorf("unknown access key %s", cred[0])
	}
	if !hmac.Equal([]byte(signature(u, name, id.Secret)), []byte(q.Get("X-Amz-Signature"))) {
		return nil, errors.New("the signature does not match: the token is for another cluster, or the secret key is wrong")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cl := s.st.Clusters[key]
	if cl == nil {
		return nil, fmt.Errorf("no cluster %s", key)
	}
	groups, ok := cl.groups(id.ARN, core.GlobalARN("iam", region, account, "root"))
	if !ok {
		return nil, fmt.Errorf("%s has no access entry in cluster %s", id.ARN, name)
	}
	return doc{"username": id.ARN, "uid": "aws-iam-authenticator:" + id.Account + ":" + id.UserID, "groups": groups,
		"extra": doc{"arn": []string{id.ARN}, "accessKeyId": []string{cred[0]}}}, nil
}

// principal is who an access key belongs to, with its secret: a registered
// key (IAM user, STS session), else "test" or a 12-digit account id meaning
// the root of the default or that account.
func (s *Service) principal(akid, region string) (core.Identity, bool) {
	if id, ok := s.hub.Identity(akid); ok {
		return id, true
	}
	account := core.DefaultAccount
	if len(akid) == 12 && strings.Trim(akid, "0123456789") == "" {
		account = akid
	} else if akid != "test" {
		return core.Identity{}, false
	}
	return core.Identity{Account: account, ARN: core.GlobalARN("iam", region, account, "root"), UserID: account, Secret: "test"}, true
}

// groups are the Kubernetes groups of a caller of the cluster: system:masters
// for the account's root and, unless accessConfig turns it off, its creator;
// else its access entry's groups. ok is false for a caller without one. Lock held.
func (cl *cluster) groups(arn, root string) (groups []string, ok bool) {
	arn = canonical(arn)
	if arn == root || arn == canonical(cl.Creator) && object(cl.Doc["accessConfig"])["bootstrapClusterCreatorAdminPermissions"] != false {
		return []string{"system:masters"}, true
	}
	e := cl.Parts["access-entries"][arn]
	if e == nil {
		return nil, false
	}
	for _, g := range list(e["kubernetesGroups"]) {
		groups = append(groups, str(g))
	}
	// ponytail: only the cluster admin policy maps to a group; bind the groups of the others (view, edit…) with RBAC
	for _, p := range cl.Policies[arn] {
		if strings.HasSuffix(str(p["policyArn"]), "/AmazonEKSClusterAdminPolicy") {
			groups = append(groups, "system:masters")
		}
	}
	return groups, true
}

// canonical turns an assumed-role session ARN into its role's ARN, the way
// access entries name roles.
func canonical(arn string) string {
	head, res, ok := strings.Cut(arn, ":assumed-role/")
	if !ok {
		return arn
	}
	role, _, _ := strings.Cut(res, "/")
	return strings.Replace(head, ":sts:", ":iam:", 1) + ":role/" + role
}

// signature is the SigV4 signature of u, a presigned GET whose signed
// headers are host and x-k8s-aws-id: cluster.
func signature(u *url.URL, cluster, secret string) string {
	q := u.Query()
	var query []string
	for k, vs := range q {
		for _, v := range vs {
			if k != "X-Amz-Signature" {
				query = append(query, esc(k)+"="+esc(v))
			}
		}
	}
	slices.SortFunc(query, func(a, b string) int { // by name, then value
		ak, av, _ := strings.Cut(a, "=")
		bk, bv, _ := strings.Cut(b, "=")
		return cmp.Or(strings.Compare(ak, bk), strings.Compare(av, bv))
	})
	host := u.Host
	if p := u.Port(); u.Scheme == "http" && p == "80" || u.Scheme == "https" && p == "443" {
		host = u.Hostname()
	}
	signed := q.Get("X-Amz-SignedHeaders")
	var headers strings.Builder
	for _, h := range strings.Split(signed, ";") {
		headers.WriteString(h + ":" + map[string]string{"host": host, "x-k8s-aws-id": cluster}[h] + "\n")
	}
	const empty = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855" // SHA-256 of no payload
	sum := sha256.Sum256([]byte(strings.Join([]string{"GET", cmp.Or(u.EscapedPath(), "/"), strings.Join(query, "&"), headers.String(), signed, empty}, "\n")))
	cred := strings.Split(q.Get("X-Amz-Credential"), "/")
	key := []byte("AWS4" + secret)
	for _, s := range append(cred[1:], "AWS4-HMAC-SHA256\n"+q.Get("X-Amz-Date")+"\n"+strings.Join(cred[1:], "/")+"\n"+hex.EncodeToString(sum[:])) {
		m := hmac.New(sha256.New, key)
		m.Write([]byte(s))
		key = m.Sum(nil)
	}
	return hex.EncodeToString(key)
}

// esc is SigV4's URI encoding.
func esc(s string) string { return strings.ReplaceAll(url.QueryEscape(s), "+", "%20") }
