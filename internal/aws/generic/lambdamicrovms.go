package generic

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Lambda microVMs: images are the engine's CRUD (CREATING, then CREATED; UPDATING,
// then UPDATED after an update, with the image version counting up); a microVM
// is PENDING, RUNNING, can be suspended and resumed, and ends TERMINATED. Nothing
// runs.
func init() {
	// Lambda signs its calls as "lambda" too: microVM paths, and tags on microVM ARNs, are ours.
	matchers["lambda-microvms"] = func(r *http.Request) bool {
		p := r.URL.Path
		return strings.HasPrefix(p, "/2025-09-09/") || strings.HasPrefix(p, "/2017-03-31/tags/") && strings.Contains(p, "microvm")
	}
	Derive("lambda-microvms", "MicrovmImage", func(r *Resource) map[string]any {
		ver, _ := orDefault(r.Data["_ver"], 1.0).(float64)
		s := Step(r, "img", "CREATING", "CREATED")
		if r.Data["_phase"] == "update" {
			s = Step(r, "img", "UPDATING", "UPDATED")
		}
		out := map[string]any{"state": s, "imageVersion": strconv.Itoa(int(ver))}
		if s == "CREATED" || s == "UPDATED" {
			out["latestActiveImageVersion"] = strconv.Itoa(int(ver))
		}
		return out
	})
	Override("lambda-microvms", "UpdateMicrovmImage", func(c *Call) (map[string]any, error) {
		old := c.Get("MicrovmImage", str(c.In["imageIdentifier"]))
		if old == nil {
			return nil, c.NotFound("Image %s not found.", str(c.In["imageIdentifier"]))
		}
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		ver, _ := orDefault(old.Data["_ver"], 1.0).(float64)
		r.Data["_ver"], r.Data["_phase"] = ver+1, "update"
		Begin(r, "img")
		c.Put(r)
		return c.Fill(r), nil
	})

	Derive("lambda-microvms", "Microvm", func(r *Resource) map[string]any {
		var s string
		switch r.Data["_phase"] {
		case "suspend":
			s = Step(r, "vm", "SUSPENDING", "SUSPENDED")
		case "terminate":
			s = Step(r, "vm", "TERMINATING", "TERMINATED")
		default: // started, or resumed
			s = Step(r, "vm", "PENDING", "RUNNING")
		}
		return map[string]any{"state": s}
	})
	Override("lambda-microvms", "RunMicrovm", func(c *Call) (map[string]any, error) {
		img := c.Get("MicrovmImage", str(c.In["imageIdentifier"]))
		if img == nil {
			return nil, c.NotFound("Image %s not found.", str(c.In["imageIdentifier"]))
		}
		now := c.now()
		id := "mvm-" + randHex(16)
		data := map[string]any{}
		for k, v := range c.In {
			if k != "imageIdentifier" && k != "clientToken" && k != "runHookPayload" {
				data[k] = v
			}
		}
		data["imageArn"], data["microvmId"], data["endpoint"] = img.ARN, id, id+".microvm."+c.Region+".lambda.localhost"
		data["imageVersion"] = orDefault(c.In["imageVersion"], derived("lambda-microvms", img)["imageVersion"])
		data["startedAt"] = now.Format(timeFmt)
		r := &Resource{Type: "Microvm", Key: id, IDs: map[string]string{"microvmIdentifier": id}, ID: id, ARN: c.arnOf("microvm/"+id, nil), Account: c.Account, Region: c.Region, Data: data, Created: now, Updated: now}
		Begin(r, "vm")
		c.Put(r)
		return vmOut(r), nil
	})
	Override("lambda-microvms", "GetMicrovm", func(c *Call) (map[string]any, error) {
		r := c.Get("Microvm", str(c.In["microvmIdentifier"]))
		if r == nil {
			return nil, c.NotFound("Microvm %s not found.", str(c.In["microvmIdentifier"]))
		}
		return vmOut(r), nil
	})
	Override("lambda-microvms", "ListMicrovms", func(c *Call) (map[string]any, error) {
		var items []any
		for _, r := range c.List("Microvm") {
			if i := str(c.In["imageIdentifier"]); i != "" && str(r.Data["imageArn"]) != i && !endsWithName(str(r.Data["imageArn"]), i) {
				continue
			}
			o := vmOut(r)
			items = append(items, map[string]any{"imageArn": o["imageArn"], "imageVersion": o["imageVersion"], "microvmId": o["microvmId"], "startedAt": o["startedAt"], "state": o["state"]})
		}
		page, next, err := pageOf(c, items, "nextToken", "maxResults")
		if err != nil {
			return nil, err
		}
		out := map[string]any{"items": page}
		if next != "" {
			out["nextToken"] = next
		}
		return out, nil
	})
	for op, phase := range map[string]string{"SuspendMicrovm": "suspend", "ResumeMicrovm": "resume", "TerminateMicrovm": "terminate"} {
		Override("lambda-microvms", op, func(c *Call) (map[string]any, error) {
			r := c.Get("Microvm", str(c.In["microvmIdentifier"]))
			if r == nil {
				return nil, c.NotFound("Microvm %s not found.", str(c.In["microvmIdentifier"]))
			}
			st := str(derived("lambda-microvms", r)["state"])
			if st == "TERMINATED" || st == "TERMINATING" {
				return nil, core.Errorf(409, "ConflictException", "Microvm %s is %s.", r.ID, st)
			}
			r.Data["_phase"] = phase
			Begin(r, "vm")
			if phase == "terminate" {
				r.Data["terminatedAt"] = c.now().Format(timeFmt)
			}
			c.Put(r)
			return map[string]any{}, nil
		})
	}
}

func endsWithName(arn, name string) bool {
	return len(arn) > len(name) && arn[len(arn)-len(name)-1:] == "/"+name
}

// vmOut is a microVM as GetMicrovm shows it.
func vmOut(r *Resource) map[string]any {
	out := map[string]any{}
	for k, v := range r.Data {
		if k[0] != '_' {
			out[k] = v
		}
	}
	out["state"] = derived("lambda-microvms", r)["state"]
	return out
}
