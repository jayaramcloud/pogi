// Package tagging emulates the Resource Groups Tagging API (JSON 1.1). It
// keeps no state: it reads tags from services that implement Tagged and
// changes them through each service's own JSON tag operations.
package tagging

import (
	"cmp"
	"net/http"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Tagged is implemented by services that can enumerate their resources: every
// resource of the account in the region, ARN → tags (empty or nil when
// untagged). The tagging API lists the tagged ones, Resource Explorer all.
type Tagged interface {
	TaggedResources(account, region string) map[string]map[string]string // ARN → tags
}

// Service is the tagging API.
type Service struct {
	hub *core.Hub
	rpc *core.RPC
}

// tagger is a service's ARN-based tag operations in its JSON protocol.
type tagger struct {
	target, tag, untag, arnField string
	tagMap                       bool // tags as a map rather than a [{Key, Value}] list
}

// taggers maps an ARN's service to its tag operations. Others (SQS, S3,
// SNS, IAM…) tag through non-JSON or non-ARN APIs and are reported as failed.
var taggers = map[string]tagger{
	"acm":            {"CertificateManager", "AddTagsToCertificate", "RemoveTagsFromCertificate", "CertificateArn", false},
	"ecr":            {"AmazonEC2ContainerRegistry_V20150921", "TagResource", "UntagResource", "resourceArn", false},
	"cloudwatch":     {"GraniteServiceVersion20100801", "TagResource", "UntagResource", "ResourceARN", false},
	"dynamodb":       {"DynamoDB_20120810", "TagResource", "UntagResource", "ResourceArn", false},
	"events":         {"AWSEvents", "TagResource", "UntagResource", "ResourceARN", false},
	"states":         {"AWSStepFunctions", "TagResource", "UntagResource", "resourceArn", false},
	"kms":            {"TrentService", "TagResource", "UntagResource", "KeyId", false},
	"secretsmanager": {"secretsmanager", "TagResource", "UntagResource", "SecretId", false},
	"logs":           {"Logs_20140328", "TagResource", "UntagResource", "resourceArn", true},
	"kinesis":        {"Kinesis_20131202", "TagResource", "UntagResource", "ResourceARN", true},
}

// New creates the tagging service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub}
	s.rpc = &core.RPC{JSONVersion: "1.1", Ops: map[string]core.Handler{
		"GetResources":   core.H(s.getResources),
		"GetTagKeys":     core.H(s.getTagKeys),
		"GetTagValues":   core.H(s.getTagValues),
		"TagResources":   core.H(s.tagResources),
		"UntagResources": core.H(s.untagResources),
	}}
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "resourcegroupstaggingapi", Cloud: core.AWS, Title: "Resource Groups Tagging API",
		Targets: []string{"ResourceGroupsTaggingAPI_20170126"}, Signing: []string{"tagging"}, Protocol: "json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.rpc.ServeHTTP(w, r) }

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

// all collects ARN → tags of the tagged resources of every service that exposes them.
func (s *Service) all(c *core.Ctx) map[string]map[string]string {
	out := map[string]map[string]string{}
	for _, svc := range s.hub.Services() {
		if t, ok := svc.(Tagged); ok {
			for arn, tags := range t.TaggedResources(c.Account, c.Region) {
				if len(tags) > 0 {
					out[arn] = tags
				}
			}
		}
	}
	return out
}

// implicitType is the resource type of services whose ARNs hold only a name, so "sqs:queue" can match.
var implicitType = map[string]string{"sqs": "queue", "sns": "topic"}

// typeMatches applies ResourceTypeFilters such as "ec2" or "ec2:vpc".
func typeMatches(arn string, filters []string) bool {
	svc, _, _, res, _ := core.ParseARN(arn)
	res = strings.TrimPrefix(res, "/") // API Gateway: "/apikeys/id"
	if t, ok := implicitType[svc]; ok && !strings.ContainsAny(res, ":/") {
		res = t + ":" + res // the ARN holds only the name
	}
	return len(filters) == 0 || slices.ContainsFunc(filters, func(f string) bool {
		fs, ft, _ := strings.Cut(f, ":")
		return fs == svc && (ft == "" || res == ft || strings.HasPrefix(res, ft+"/") || strings.HasPrefix(res, ft+":"))
	})
}

type tag struct{ Key, Value string }

type mapping struct {
	ResourceARN string
	Tags        []tag
}

// getResources returns every match in one page.
func (s *Service) getResources(c *core.Ctx, in *struct {
	TagFilters []struct {
		Key    string
		Values []string
	}
	ResourceTypeFilters []string
	ResourceARNList     []string
}) (any, error) {
	out := &struct {
		ResourceTagMappingList []mapping
		PaginationToken        string
	}{ResourceTagMappingList: []mapping{}}
	all := s.all(c)
	for _, arn := range core.SortedKeys(all) {
		tags := all[arn]
		ok := typeMatches(arn, in.ResourceTypeFilters) && (len(in.ResourceARNList) == 0 || slices.Contains(in.ResourceARNList, arn))
		for _, f := range in.TagFilters {
			v, has := tags[f.Key]
			ok = ok && has && (len(f.Values) == 0 || slices.Contains(f.Values, v))
		}
		if ok {
			m := mapping{ResourceARN: arn}
			for _, k := range core.SortedKeys(tags) {
				m.Tags = append(m.Tags, tag{k, tags[k]})
			}
			out.ResourceTagMappingList = append(out.ResourceTagMappingList, m)
		}
	}
	return out, nil
}

func (s *Service) getTagKeys(c *core.Ctx, _ *struct{}) (any, error) {
	keys := map[string]bool{}
	for _, tags := range s.all(c) {
		for k := range tags {
			keys[k] = true
		}
	}
	return map[string]any{"TagKeys": core.SortedKeys(keys), "PaginationToken": ""}, nil
}

func (s *Service) getTagValues(c *core.Ctx, in *struct{ Key string }) (any, error) {
	values := map[string]bool{}
	for _, tags := range s.all(c) {
		if v, ok := tags[in.Key]; ok {
			values[v] = true
		}
	}
	return map[string]any{"TagValues": core.SortedKeys(values), "PaginationToken": ""}, nil
}

type failure struct {
	StatusCode   int
	ErrorCode    string
	ErrorMessage string
}

// change calls each ARN's service with body(t) and collects the failures.
func (s *Service) change(c *core.Ctx, arns []string, untag bool, body func(t tagger, arn string) map[string]any) (any, error) {
	if len(arns) == 0 || len(arns) > 20 {
		return nil, invalid("ResourceARNList must contain between 1 and 20 ARNs.")
	}
	failed := map[string]failure{}
	for _, arn := range arns {
		svc, region, account, _, _ := core.ParseARN(arn)
		t, known := taggers[svc]
		if !known {
			failed[arn] = failure{400, "InvalidParameterException", "pogi cannot tag " + svc + " resources through the tagging API"}
			continue
		}
		op := t.tag
		if untag {
			op = t.untag
		}
		if err := s.hub.Call(cmp.Or(account, c.Account), cmp.Or(region, c.Region), t.target, op, body(t, arn), nil); err != nil {
			e, code := core.AsError(err), "InvalidParameterException"
			if e.Status >= 500 {
				code = "InternalServiceException"
			}
			failed[arn] = failure{e.Status, code, e.Code + ": " + e.Msg}
		}
	}
	return map[string]any{"FailedResourcesMap": failed}, nil
}

func (s *Service) tagResources(c *core.Ctx, in *struct {
	ResourceARNList []string
	Tags            map[string]string
}) (any, error) {
	if len(in.Tags) == 0 {
		return nil, invalid("Tags must contain at least one tag.")
	}
	return s.change(c, in.ResourceARNList, false, func(t tagger, arn string) map[string]any {
		if t.tagMap {
			return map[string]any{t.arnField: arn, "Tags": in.Tags}
		}
		var list []map[string]string
		for _, k := range core.SortedKeys(in.Tags) { // KMS spells them TagKey/TagValue
			list = append(list, map[string]string{"Key": k, "Value": in.Tags[k], "TagKey": k, "TagValue": in.Tags[k]})
		}
		return map[string]any{t.arnField: arn, "Tags": list}
	})
}

func (s *Service) untagResources(c *core.Ctx, in *struct {
	ResourceARNList []string
	TagKeys         []string
}) (any, error) {
	if len(in.TagKeys) == 0 {
		return nil, invalid("TagKeys must contain at least one key.")
	}
	return s.change(c, in.ResourceARNList, true, func(t tagger, arn string) map[string]any {
		var list []map[string]string // ACM removes by Tags, the others by TagKeys
		for _, k := range in.TagKeys {
			list = append(list, map[string]string{"Key": k})
		}
		return map[string]any{t.arnField: arn, "TagKeys": in.TagKeys, "Tags": list}
	})
}
