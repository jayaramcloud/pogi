package cloudformation

import "github.com/nrndev/pogi/internal/core"

// seedStack is the pseudo stack that pogi.yaml resources are provisioned in.
const seedStack = "pogi-seed"

// Provision creates one resource through its provider, outside any real stack
// (pogi.yaml seeding). Generated names are stable (see pctx.name), so seeding
// again hits "already exists" instead of making a copy.
func (s *Service) Provision(account, region, typ, logical string, props map[string]any) (physical string, attrs map[string]any, err error) {
	p := providers[typ]
	if p == nil {
		return "", nil, validation("pogi cannot provision %s outside a stack", typ)
	}
	return p.create(&pctx{s: s, account: account, region: region, stack: seedStack, logical: logical, typ: typ,
		stackID: core.ARN("cloudformation", region, account, "stack/"+seedStack+"/"+account)}, props)
}
