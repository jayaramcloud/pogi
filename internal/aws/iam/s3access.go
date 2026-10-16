package iam

import (
	"encoding/json"
	"encoding/xml"
	"maps"
	"net/url"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// access is one permission check: an action on a resource, with its condition keys.
type access struct {
	action, resource string
	keys             map[string]string
}

// objectTagger is the S3 service: the tags of one object version (nil when it has none or is absent).
type objectTagger interface {
	ObjectTags(bucket, key, version string) map[string]string
}

// bucketPolicies is the S3 service: a bucket's policy document and owner account.
type bucketPolicies interface {
	BucketPolicy(bucket string) (policy, account string, ok bool)
}

// versionAction is the IAM action S3 asks for when a request names a version.
var versionAction = map[string]string{
	"s3:getobject": "s3:GetObjectVersion", "s3:deleteobject": "s3:DeleteObjectVersion",
	"s3:getobjecttagging": "s3:GetObjectVersionTagging", "s3:putobjecttagging": "s3:PutObjectVersionTagging",
	"s3:deleteobjecttagging": "s3:DeleteObjectVersionTagging", "s3:getobjectacl": "s3:GetObjectVersionAcl",
	"s3:putobjectacl": "s3:PutObjectVersionAcl",
}

func s3ARN(c *core.Ctx, bucket, key string) string {
	return core.GlobalARN("s3", c.Region, "", bucket+"/"+key)
}

// withExistingTags adds the s3:ExistingObjectTag/<key> condition keys of an object.
func withExistingTags(c *core.Ctx, bucket, key, version string, keys map[string]string) map[string]string {
	keys = maps.Clone(keys)
	if t, ok := c.Hub.Service("s3").(objectTagger); ok {
		for k, v := range t.ObjectTags(bucket, key, version) {
			keys["s3:existingobjecttag/"+strings.ToLower(k)] = v
		}
	}
	return keys
}

// s3Accesses lists every check an S3 request needs. Beyond the main action it adds:
// the version-specific action name, s3:PutObjectTagging when the request sets tags,
// s3:BypassGovernanceRetention when the request asks for it, s3:GetObject on a copy source, and the s3:RequestObjectTag/<key> and
// s3:ExistingObjectTag/<key> condition keys.
func s3Accesses(c *core.Ctx, a access) []access {
	q, _ := url.ParseQuery(c.RawQuery)
	bucket, key := s3Target(c.Host, c.Hub.Cfg.Hostname, c.Path)
	version := q.Get("versionId")
	plain := a.keys
	a.keys = maps.Clone(plain)
	if version != "" {
		a.keys["s3:versionid"] = version
		if v, ok := versionAction[strings.ToLower(a.action)]; ok {
			a.action = v
		}
	}
	tags := requestTags(c)
	for k, v := range tags {
		a.keys["s3:requestobjecttag/"+strings.ToLower(k)] = v
	}
	if key != "" && a.action != "s3:PutObject" { // S3 does not offer the existing tags to a write of a new object
		a.keys = withExistingTags(c, bucket, key, version, a.keys)
	}
	out := []access{a}
	if len(tags) > 0 && c.Op != "PutObjectTagging" {
		out = append(out, access{"s3:PutObjectTagging", a.resource, a.keys})
	}
	if strings.EqualFold(c.Header.Get("X-Amz-Bypass-Governance-Retention"), "true") {
		out = append(out, access{"s3:BypassGovernanceRetention", a.resource, a.keys})
	}
	if c.Op == "CopyObject" || c.Op == "UploadPartCopy" {
		src, v, _ := strings.Cut(c.Header.Get("X-Amz-Copy-Source"), "?versionId=")
		src, _ = url.PathUnescape(strings.TrimPrefix(src, "/"))
		if sb, sk, ok := strings.Cut(src, "/"); ok && sk != "" {
			action := "s3:GetObject"
			if v != "" {
				action = "s3:GetObjectVersion"
			}
			out = append(out, access{action, s3ARN(c, sb, sk), withExistingTags(c, sb, sk, v, plain)})
		}
	}
	return out
}

// requestTags are the tags a request sets: the x-amz-tagging header (S3 copies the
// query parameter of a presigned URL into it) or the body of PutObjectTagging.
func requestTags(c *core.Ctx) map[string]string {
	tags := map[string]string{}
	if c.Op == "PutObjectTagging" {
		var in struct {
			Tags []struct{ Key, Value string } `xml:"TagSet>Tag"`
		}
		if xml.Unmarshal(c.Payload(), &in) == nil {
			for _, t := range in.Tags {
				tags[t.Key] = t.Value
			}
		}
		return tags
	}
	vals, _ := url.ParseQuery(c.Header.Get("X-Amz-Tagging"))
	for k := range vals {
		tags[k] = vals.Get(k)
	}
	return tags
}

// authorizeDeleteObjects checks every key of a DeleteObjects call on its own, as S3
// does: a refused key is not deleted and is reported as AccessDenied, the call itself
// still answers 200. In strict mode the refused keys go to c.Denied for the handler.
func (s *Service) authorizeDeleteObjects(c *core.Ctx, keys map[string]string) {
	var in struct {
		Objects []struct{ Key, VersionId string } `xml:"Object"`
	}
	xml.Unmarshal(c.Payload(), &in) // a bad body is the handler's to refuse
	bucket, _ := s3Target(c.Host, c.Hub.Cfg.Hostname, c.Path)
	for _, o := range in.Objects {
		a := access{"s3:DeleteObject", s3ARN(c, bucket, o.Key), withExistingTags(c, bucket, o.Key, o.VersionId, keys)}
		if o.VersionId != "" {
			a.action = "s3:DeleteObjectVersion"
		}
		decision, reason := s.evaluate(c, a)
		s.record(principalOf(c), a.action, a.resource, decision, reason)
		if decision != "allowed" && c.Hub.IAMMode() == "strict" {
			if c.Denied == nil {
				c.Denied = map[string]bool{}
			}
			c.Denied[o.Key] = true
			c.IAM = "deny " + a.action + ": " + reason
		}
	}
}

// AnonymousAllowed reports whether a bucket policy lets a request without credentials
// through: every check the request needs must be allowed by statements whose Principal
// is "*", and none may be denied there. An empty policy allows nothing.
func AnonymousAllowed(c *core.Ctx, policy string) bool {
	public := []map[string]any{}
	for _, st := range statements(policy) {
		p := st["Principal"]
		if m, ok := p.(map[string]any); ok {
			p = m["AWS"]
		}
		if slices.Contains(asStrings(p), "*") {
			public = append(public, st)
		}
	}
	doc, _ := json.Marshal(map[string]any{"Statement": public})
	resource, keys := resourceOf(c), contextKeys(c)
	for _, a := range s3Accesses(c, access{actionOf("s3", c.Op), resource, keys}) {
		if d, _ := decide([]string{string(doc)}, a.action, a.resource, a.keys); d != "allowed" {
			return false
		}
	}
	return true
}

// withBucketPolicy folds an S3 bucket policy into the identity decision, as AWS does.
// An explicit Deny in the policy wins. In the bucket's own account an Allow that names
// the caller (or "*") grants access even when the identity policies do not. From another
// account both sides must allow, so the policy must also Allow the caller or its account.
func withBucketPolicy(c *core.Ctx, a access, decision, reason string) (string, string) {
	svc, _, _, res, ok := core.ParseARN(a.resource)
	bucket, _, _ := strings.Cut(res, "/")
	bp, isS3 := c.Hub.Service("s3").(bucketPolicies)
	if !ok || svc != "s3" || bucket == "" || !isS3 || !strings.HasPrefix(a.action, "s3:") {
		return decision, reason
	}
	policy, owner, found := bp.BucketPolicy(bucket)
	if !found {
		return decision, reason
	}
	crossAccount, allowed := owner != c.Account, false
	for _, st := range statements(policy) {
		direct, viaAccount := policyPrincipal(st["Principal"], c, a.keys)
		deny := st["Effect"] == "Deny"
		if !(direct || viaAccount) || !applies(st, a.action, a.resource, a.keys, deny) {
			continue
		}
		if deny {
			return "explicitDeny", "with an explicit deny in a resource-based policy"
		}
		allowed = allowed || direct || crossAccount // naming the account only delegates inside it
	}
	switch {
	case crossAccount && decision == "allowed" && !allowed:
		return "implicitDeny", "no resource-based policy allows the " + a.action + " action"
	case !crossAccount && allowed:
		return "allowed", ""
	}
	return decision, reason
}

// policyPrincipal tells how a statement's Principal covers the caller: direct (it names
// the caller, its role or "*") or viaAccount (it names the caller's account).
func policyPrincipal(p any, c *core.Ctx, keys map[string]string) (direct, viaAccount bool) {
	if m, ok := p.(map[string]any); ok {
		p = m["AWS"]
	}
	for _, x := range asStrings(p) {
		switch x {
		case "*", c.Principal, keys["aws:principalarn"]:
			direct = true
		case c.Account, c.GlobalARN("iam", "root"):
			viaAccount = true
		}
	}
	return
}
