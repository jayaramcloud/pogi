package cloudformation

import (
	"encoding/json"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// A resource type with no provider of its own that a service with a generated engine (EFS, WAF, Config,
// SageMaker …) manages is deployed through Cloud Control, which creates, patches and deletes it there.

// engine is the Cloud Control service, which knows what the generated engines manage.
type engine interface{ SupportsType(typeName string) bool }

func (s *Service) engineHas(typ string) bool {
	if s.hub == nil {
		return false
	}
	e, ok := s.hub.Service("cloudcontrol").(engine)
	return ok && e.SupportsType(typ)
}

const ccTarget = "CloudApiService"

// ccCall runs a Cloud Control request and returns its progress event; a failed one is an error.
func ccCall(c *pctx, op string, in map[string]any) (map[string]any, error) {
	var out struct{ ProgressEvent map[string]any }
	if err := c.call(ccTarget, op, in, &out); err != nil {
		return nil, err
	}
	ev := out.ProgressEvent
	if str(ev["OperationStatus"]) == "FAILED" {
		return nil, core.Errorf(400, or(str(ev["ErrorCode"]), "GeneralServiceException"), "%s", str(ev["StatusMessage"]))
	}
	return ev, nil
}

func engineProvider(typ string) *provider {
	model := func(ev map[string]any) map[string]any {
		m := map[string]any{}
		if s := str(ev["ResourceModel"]); s != "" {
			json.Unmarshal([]byte(s), &m)
		}
		return m
	}
	return &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			ev, err := ccCall(c, "CreateResource", map[string]any{"TypeName": typ, "DesiredState": jsonString(p)})
			if err != nil {
				return "", nil, err
			}
			return str(ev["Identifier"]), model(ev), nil
		},
		// the changed properties go as a JSON Patch
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			var ops []any
			for k, v := range p {
				if !equal(old[k], v) {
					ops = append(ops, map[string]any{"op": "add", "path": "/" + jsonPointer(k), "value": v})
				}
			}
			for k := range old {
				if _, ok := p[k]; !ok {
					ops = append(ops, map[string]any{"op": "remove", "path": "/" + jsonPointer(k)})
				}
			}
			ev, err := ccCall(c, "UpdateResource", map[string]any{"TypeName": typ, "Identifier": phys, "PatchDocument": jsonString(ops)})
			if err != nil {
				if strings.Contains(err.Error(), "cannot be updated in place") || core.AsError(err).Code == "NotUpdatable" {
					return nil, true, nil
				}
				return nil, false, err
			}
			return model(ev), false, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			_, err := ccCall(c, "DeleteResource", map[string]any{"TypeName": typ, "Identifier": phys})
			return err
		},
	}
}

func jsonPointer(k string) string { return strings.NewReplacer("~", "~0", "/", "~1").Replace(k) }
