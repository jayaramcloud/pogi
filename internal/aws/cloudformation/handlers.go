package cloudformation

import (
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// The resource handlers CloudFormation deploys with, for Cloud Control. A
// resource made here belongs to no stack; its generated names carry "ccapi".

func (s *Service) handlerCtx(account, region, typ string) *pctx {
	logical := typ[strings.LastIndex(typ, ":")+1:]
	return &pctx{s: s, account: account, region: region, stack: "ccapi", logical: logical, typ: typ,
		stackID: core.ARN("cloudformation", region, account, "stack/ccapi/"+account)}
}

// HasHandler says whether pogi deploys the resource type for real (not a placeholder).
func (s *Service) HasHandler(typ string) bool { return providers[typ] != nil }

// CreateResource creates a resource of a type from its properties and returns its physical id and attributes.
func (s *Service) CreateResource(account, region, typ string, props map[string]any) (string, map[string]any, error) {
	p := providers[typ]
	if p == nil {
		return "", nil, validation("pogi has no handler for %s", typ)
	}
	return p.create(s.handlerCtx(account, region, typ), props)
}

// UpdateResource applies new properties to a resource. replace says it cannot be updated in place: the caller
// creates a new one and deletes this one.
func (s *Service) UpdateResource(account, region, typ, physical string, old, props map[string]any) (attrs map[string]any, replace bool, err error) {
	p := providers[typ]
	if p == nil {
		return nil, false, validation("pogi has no handler for %s", typ)
	}
	if p.update == nil {
		return nil, true, nil
	}
	return p.update(s.handlerCtx(account, region, typ), physical, old, props)
}

// DeleteResource deletes a resource by its physical id.
func (s *Service) DeleteResource(account, region, typ, physical string, props map[string]any) error {
	p := providers[typ]
	if p == nil {
		return validation("pogi has no handler for %s", typ)
	}
	return p.del(s.handlerCtx(account, region, typ), physical, props)
}

// listers find the resources of a type however they were made, for Cloud Control's ListResources:
// physical id -> the properties known from the owning service. A type without one lists only what Cloud Control made.
var listers = map[string]func(c *pctx) map[string]map[string]any{
	"AWS::SQS::Queue": func(c *pctx) map[string]map[string]any {
		var out struct{ QueueUrls []string }
		res := map[string]map[string]any{}
		if c.call("AmazonSQS", "ListQueues", map[string]any{}, &out) == nil {
			for _, u := range out.QueueUrls {
				name := lastSegment(u)
				res[u] = map[string]any{"QueueUrl": u, "QueueName": name, "Arn": c.arn("sqs", name)}
			}
		}
		return res
	},
	"AWS::Logs::LogGroup": func(c *pctx) map[string]map[string]any {
		res := map[string]map[string]any{}
		next := ""
		for {
			var out struct {
				LogGroups []struct{ LogGroupName, Arn string }
				NextToken string
			}
			in := map[string]any{}
			if next != "" {
				in["nextToken"] = next
			}
			if c.call("Logs_20140328", "DescribeLogGroups", in, &out) != nil {
				return res
			}
			for _, g := range out.LogGroups {
				res[g.LogGroupName] = map[string]any{"LogGroupName": g.LogGroupName, "Arn": g.Arn}
			}
			if next = out.NextToken; next == "" {
				return res
			}
		}
	},
}

// ListResources lists the resources of a type found in their own service.
// ok is false when pogi cannot list the type that way.
func (s *Service) ListResources(account, region, typ string) (res map[string]map[string]any, ok bool) {
	l := listers[typ]
	if l == nil {
		return nil, false
	}
	return l(s.handlerCtx(account, region, typ)), true
}
