package generic

import (
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Control Tower: a landing zone and enabled controls. Their operations finish
// at once and stay readable by id. Nothing is deployed (ponytail: no accounts,
// OUs or guardrails are created).

// ctOp records a finished operation of a landing zone or a control.
func ctOp(c *Call, kind, typ string, f map[string]any) string {
	id, now := core.UUID(), c.now()
	f["operationIdentifier"], f["operationType"], f["status"] = id, typ, "SUCCEEDED"
	f["startTime"], f["endTime"] = now.Format(time.RFC3339Nano), now.Format(time.RFC3339Nano)
	c.Put(&Resource{Type: "Operation", Key: id, IDs: map[string]string{"operationIdentifier": id}, Name: id, Account: c.Account, Region: c.key(), Created: now, Updated: now, Data: map[string]any{"kind": kind, "detail": f}})
	return id
}

func ctOpOut(c *Call, kind, out string) (map[string]any, error) {
	r := c.Get("Operation", str(c.In["operationIdentifier"]))
	if r == nil || r.Data["kind"] != kind {
		return nil, c.NotFound("Operation %s not found", str(c.In["operationIdentifier"]))
	}
	return map[string]any{out: r.Data["detail"]}, nil
}

func ctLandingZone(c *Call) (*Resource, error) {
	id := str(c.In["landingZoneIdentifier"])
	for _, r := range c.List("LandingZone") {
		if r.ARN == id {
			return r, nil
		}
	}
	return nil, c.NotFound("Landing zone %s not found", id)
}

// ctControl finds an enabled control by its ARN, or by control and target.
func ctControl(c *Call) *Resource {
	for _, r := range c.List("EnabledControl") {
		if arn := str(c.In["enabledControlIdentifier"]); arn != "" && r.ARN == arn ||
			arn == "" && r.Data["controlIdentifier"] == c.In["controlIdentifier"] && r.Data["targetIdentifier"] == c.In["targetIdentifier"] {
			return r
		}
	}
	return nil
}

func ctControlDetail(r *Resource) map[string]any {
	op, _ := r.Data["operation"].(string)
	return map[string]any{"arn": r.ARN, "controlIdentifier": r.Data["controlIdentifier"], "targetIdentifier": r.Data["targetIdentifier"], "parameters": r.Data["parameters"],
		"statusSummary": map[string]any{"status": "SUCCEEDED", "lastOperationIdentifier": op}, "driftStatusSummary": map[string]any{"driftStatus": "IN_SYNC"}}
}

func init() {
	Override("controltower", "CreateLandingZone", func(c *Call) (map[string]any, error) {
		if len(c.List("LandingZone")) > 0 {
			return nil, c.Exists("A landing zone already exists in this account")
		}
		arn := core.ARN("controltower", c.Region, c.Account, "landingzone/"+strings.ToUpper(core.RandString(17, core.UpperNum)))
		c.In["landingZoneIdentifier"] = arn
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		r.ARN = arn
		c.Put(r)
		return map[string]any{"arn": arn, "operationIdentifier": ctOp(c, "landingzone", "CREATE", map[string]any{})}, nil
	})
	Override("controltower", "GetLandingZone", func(c *Call) (map[string]any, error) {
		r, err := ctLandingZone(c)
		if err != nil {
			return nil, err
		}
		return map[string]any{"landingZone": map[string]any{"arn": r.ARN, "version": r.Data["version"], "manifest": r.Data["manifest"], "remediationTypes": r.Data["remediationTypes"],
			"status": "ACTIVE", "latestAvailableVersion": "3.3", "driftStatus": map[string]any{"status": "IN_SYNC"}}}, nil
	})
	Override("controltower", "ListLandingZones", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("LandingZone") {
			out = append(out, map[string]any{"arn": r.ARN})
		}
		return map[string]any{"landingZones": out}, nil
	})
	Override("controltower", "UpdateLandingZone", func(c *Call) (map[string]any, error) {
		r, err := ctLandingZone(c)
		if err != nil {
			return nil, err
		}
		for _, k := range []string{"version", "manifest", "remediationTypes"} {
			if v, ok := c.In[k]; ok {
				r.Data[k] = v
			}
		}
		r.Updated = c.now()
		c.Put(r)
		return map[string]any{"operationIdentifier": ctOp(c, "landingzone", "UPDATE", map[string]any{})}, nil
	})
	Override("controltower", "DeleteLandingZone", func(c *Call) (map[string]any, error) {
		r, err := ctLandingZone(c)
		if err != nil {
			return nil, err
		}
		c.Svc.Delete(c.Account, c.Region, "LandingZone", r.Key)
		return map[string]any{"operationIdentifier": ctOp(c, "landingzone", "DELETE", map[string]any{})}, nil
	})
	Override("controltower", "ResetLandingZone", func(c *Call) (map[string]any, error) {
		if _, err := ctLandingZone(c); err != nil {
			return nil, err
		}
		return map[string]any{"operationIdentifier": ctOp(c, "landingzone", "RESET", map[string]any{})}, nil
	})
	Override("controltower", "GetLandingZoneOperation", func(c *Call) (map[string]any, error) {
		return ctOpOut(c, "landingzone", "operationDetails")
	})
	Override("controltower", "ListLandingZoneOperations", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, r := range c.List("Operation") {
			if r.Data["kind"] == "landingzone" {
				d := r.Data["detail"].(map[string]any)
				out = append(out, map[string]any{"operationIdentifier": d["operationIdentifier"], "operationType": d["operationType"], "status": d["status"]})
			}
		}
		return map[string]any{"landingZoneOperations": out}, nil
	})

	Override("controltower", "EnableControl", func(c *Call) (map[string]any, error) {
		if ctControl(c) != nil {
			return nil, c.Exists("Control %s is already enabled for %s", str(c.In["controlIdentifier"]), str(c.In["targetIdentifier"]))
		}
		arn := core.ARN("controltower", c.Region, c.Account, "enabledcontrol/"+strings.ToLower(core.RandString(17, core.UpperNum)))
		now := c.now()
		r := &Resource{Type: "EnabledControl", Key: arn, IDs: map[string]string{"enabledControlIdentifier": arn}, ARN: arn, Name: arn, Account: c.Account, Region: c.key(), Created: now, Updated: now,
			Data: map[string]any{"controlIdentifier": c.In["controlIdentifier"], "targetIdentifier": c.In["targetIdentifier"], "parameters": c.In["parameters"]}}
		_, add, _ := takeTags(c.op.in, c.In)
		applyTags(r, add, nil)
		op := ctOp(c, "control", "ENABLE_CONTROL", map[string]any{"controlIdentifier": c.In["controlIdentifier"], "targetIdentifier": c.In["targetIdentifier"], "enabledControlIdentifier": arn})
		r.Data["operation"] = op
		c.Put(r)
		return map[string]any{"arn": arn, "operationIdentifier": op}, nil
	})
	Override("controltower", "GetEnabledControl", func(c *Call) (map[string]any, error) {
		r := ctControl(c)
		if r == nil {
			return nil, c.NotFound("Enabled control %s not found", str(c.In["enabledControlIdentifier"]))
		}
		return map[string]any{"enabledControlDetails": ctControlDetail(r)}, nil
	})
	Override("controltower", "ListEnabledControls", func(c *Call) (map[string]any, error) {
		f, _ := c.In["filter"].(map[string]any)
		out := []any{}
		for _, r := range c.List("EnabledControl") {
			if t := str(c.In["targetIdentifier"]); t != "" && t != str(r.Data["targetIdentifier"]) {
				continue
			}
			if ids := asList(f["controlIdentifiers"]); len(ids) > 0 && !slices.ContainsFunc(ids, func(i any) bool { return i == r.Data["controlIdentifier"] }) {
				continue
			}
			d := ctControlDetail(r)
			out = append(out, map[string]any{"arn": d["arn"], "controlIdentifier": d["controlIdentifier"], "targetIdentifier": d["targetIdentifier"], "statusSummary": d["statusSummary"], "driftStatusSummary": d["driftStatusSummary"]})
		}
		return map[string]any{"enabledControls": out}, nil
	})
	Override("controltower", "DisableControl", func(c *Call) (map[string]any, error) {
		r := ctControl(c)
		if r == nil {
			return nil, c.NotFound("Enabled control not found")
		}
		c.Svc.Delete(c.Account, c.Region, "EnabledControl", r.Key)
		return map[string]any{"operationIdentifier": ctOp(c, "control", "DISABLE_CONTROL", map[string]any{"controlIdentifier": r.Data["controlIdentifier"], "targetIdentifier": r.Data["targetIdentifier"], "enabledControlIdentifier": r.ARN})}, nil
	})
	Override("controltower", "GetControlOperation", func(c *Call) (map[string]any, error) {
		return ctOpOut(c, "control", "controlOperation")
	})

	Override("controltower", "ListBaselines", func(c *Call) (map[string]any, error) {
		out := []any{}
		for _, b := range []string{"AWSControlTowerBaseline", "IdentityCenterBaseline", "AuditBaseline", "LogArchiveBaseline"} {
			out = append(out, map[string]any{"arn": core.ARN("controltower", c.Region, "", "baseline/"+b), "name": b, "description": "The " + b + " baseline"})
		}
		return map[string]any{"baselines": out}, nil
	})
}
