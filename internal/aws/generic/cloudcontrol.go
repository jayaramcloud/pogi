package generic

import (
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

// Cloud Control API. A resource type that CloudFormation deploys for real
// (AWS::S3::Bucket, AWS::SQS::Queue ...) runs through those same handlers; a
// type that maps onto an embedded model (AWS::EFS::FileSystem) runs through
// that service's engine. Requests run at once and keep a progress event that
// reads IN_PROGRESS, then SUCCESS, so that the waiters see a lifecycle.

// ccHandlers is the CloudFormation service, found through the hub.
type ccHandlers interface {
	HasHandler(typ string) bool
	CreateResource(account, region, typ string, props map[string]any) (string, map[string]any, error)
	UpdateResource(account, region, typ, physical string, old, props map[string]any) (map[string]any, bool, error)
	DeleteResource(account, region, typ, physical string, props map[string]any) error
	ListResources(account, region, typ string) (res map[string]map[string]any, ok bool)
}

func init() {
	Override("cloudcontrol", "CreateResource", ccCreate)
	Override("cloudcontrol", "GetResource", ccGet)
	Override("cloudcontrol", "ListResources", ccList)
	Override("cloudcontrol", "UpdateResource", ccUpdate)
	Override("cloudcontrol", "DeleteResource", ccDelete)
	Override("cloudcontrol", "GetResourceRequestStatus", func(c *Call) (map[string]any, error) {
		r, err := ccRequest(c, str(c.In["RequestToken"]))
		if err != nil {
			return nil, err
		}
		return map[string]any{"ProgressEvent": ccEvent(r)}, nil
	})
	Override("cloudcontrol", "CancelResourceRequest", func(c *Call) (map[string]any, error) {
		r, err := ccRequest(c, str(c.In["RequestToken"]))
		if err != nil {
			return nil, err
		}
		if ccStatus(r) != "IN_PROGRESS" {
			return nil, c.Invalid("Request %s is already %s and cannot be canceled", r.Key, ccStatus(r))
		}
		r.Data["canceled"] = true
		c.Put(r)
		return map[string]any{"ProgressEvent": ccEvent(r)}, nil
	})
	Override("cloudcontrol", "ListResourceRequests", ccListRequests)
}

// ---- backends

// ccTarget is where a type's resources live: CloudFormation's handlers, or the engine of another service.
type ccTarget struct {
	typeName string
	cfn      ccHandlers
	svc      *Service
	rt       *rtype
}

// SupportsType says whether Cloud Control manages a type through the engine of an embedded service
// (not through CloudFormation's own handlers). CloudFormation deploys such types the same way.
func (s *Service) SupportsType(typeName string) bool {
	parts := strings.Split(typeName, "::")
	if len(parts) != 3 || parts[0] != "AWS" || s.h == nil {
		return false
	}
	for _, o := range s.h.Services() {
		if g, ok := o.(*Service); ok && g != s && norm(g.meta.Name) == norm(parts[1]) {
			m, err := g.model()
			if err != nil {
				return false
			}
			rt := m.types[norm(parts[2])]
			return rt != nil && len(rt.ops[kCreate]) > 0 && len(rt.ops[kDelete]) > 0
		}
	}
	return false
}

func ccFind(c *Call, typeName string) (*ccTarget, error) {
	parts := strings.Split(typeName, "::")
	if len(parts) != 3 || parts[0] != "AWS" {
		return nil, c.Invalid("Invalid TypeName '%s': expected AWS::<Service>::<Resource>", typeName)
	}
	t := &ccTarget{typeName: typeName}
	if cf, ok := c.Hub().Service("cloudformation").(ccHandlers); ok && cf.HasHandler(typeName) {
		t.cfn = cf
		return t, nil
	}
	for _, o := range c.Hub().Services() {
		if g, ok := o.(*Service); ok && g != c.Svc && norm(g.meta.Name) == norm(parts[1]) {
			m, err := g.model()
			if err != nil {
				return nil, err
			}
			if t.rt = m.types[norm(parts[2])]; t.rt != nil {
				t.svc = g
				return t, nil
			}
		}
	}
	return nil, core.Errorf(400, "UnsupportedActionException", "Resource type %s is not supported by pogi's Cloud Control API", typeName)
}

// call runs an operation of the target service in-process and returns the call, for its resource.
func (t *ccTarget) call(c *Call, k kind, in map[string]any) (*Call, map[string]any, error) {
	ops := t.rt.ops[k]
	if len(ops) == 0 {
		return nil, nil, core.Errorf(400, "UnsupportedActionException", "Resource type %s does not support this action", t.typeName)
	}
	op := shortestOp(ops)
	m, _ := t.svc.model()
	if k == kCreate && op.in != nil { // a resource handler makes up the required members (an idempotency token) a template leaves out
		for _, n := range op.in.Required {
			if sh := op.in.Members[n].sh; in[n] == nil && sh.Type == "string" {
				in[n] = mintID(sh, t.rt.name)
			}
		}
	}
	tc := &Call{Svc: t.svc, Op: op.Name, In: in, Account: c.Account, Region: c.Region, op: op, m: m}
	out, err := t.svc.run(tc)
	return tc, out, err
}

// shortestOp is the plain operation (DeleteX before DeleteXBatch). It leaves the shared model's
// slice alone, since concurrent requests read it.
func shortestOp(ops []*operation) *operation {
	return slices.MinFunc(ops, func(a, b *operation) int { return len(a.Name) - len(b.Name) })
}

// identifier is what Cloud Control calls a resource: its identifiers joined with "|".
func ccIdentifier(r *Resource) string { return strings.ReplaceAll(r.Key, "/", "|") }

// properties is a stored engine resource as Cloud Control shows it.
func ccProperties(r *Resource) map[string]any {
	out := map[string]any{}
	for k, v := range r.Data {
		if !strings.HasPrefix(k, "_") {
			out[k] = v
		}
	}
	for k, v := range r.IDs {
		out[k] = v
	}
	if r.ARN != "" {
		out["Arn"] = r.ARN
	}
	if len(r.Tags) > 0 {
		var tags []any
		for _, k := range core.SortedKeys(r.Tags) {
			tags = append(tags, map[string]any{"Key": k, "Value": r.Tags[k]})
		}
		out["Tags"] = tags
	}
	return out
}

// ---- stored records

// A "Resource" record is a CloudFormation-backed resource Cloud Control made:
// Data.Identifier, Data.Properties (what was asked), Data.Attributes (what the handler returned).
func ccRecord(c *Call, typeName, id string) *Resource { return c.Get("Resource", typeName, id) }

func ccKey(typeName, id string) string { return typeName + "/" + id }

func ccStore(c *Call, typeName, id string, props, attrs map[string]any) {
	now := c.now()
	r := &Resource{Type: "Resource", Key: ccKey(typeName, id), IDs: map[string]string{"TypeName": typeName, "Identifier": id},
		ID: id, Account: c.Account, Region: c.Region, Created: now, Updated: now,
		Data: map[string]any{"TypeName": typeName, "Identifier": id, "Properties": props, "Attributes": attrs}}
	if old := ccRecord(c, typeName, id); old != nil {
		r.Created = old.Created
	}
	c.Put(r)
}

func ccShown(r *Resource) map[string]any {
	out := map[string]any{}
	for _, k := range []string{"Attributes", "Properties"} {
		if m, ok := r.Data[k].(map[string]any); ok {
			for n, v := range m {
				out[n] = v
			}
		}
	}
	return out
}

// ---- requests

func ccRequest(c *Call, token string) (*Resource, error) {
	r := c.Get("Request", token)
	if r == nil {
		return nil, c.m.modelErr(c.op, eNotFound, "RequestToken", fmt.Sprintf("Request with token %s was not found.", token))
	}
	return r, nil
}

// ccDone records a finished operation and returns the output of the call that made it.
func ccDone(c *Call, op, typeName, id string, model map[string]any, fail error) (map[string]any, error) {
	now := c.now()
	r := &Resource{Type: "Request", Key: core.UUID(), Account: c.Account, Region: c.Region, Created: now, Updated: now,
		Data: map[string]any{"Operation": op, "TypeName": typeName, "Identifier": id}}
	r.ID = r.Key
	if model != nil {
		b, _ := json.Marshal(model)
		r.Data["ResourceModel"] = string(b)
	}
	if fail != nil {
		e := core.AsError(fail)
		code := "GeneralServiceException"
		switch {
		case strings.Contains(e.Code, "NotFound") || e.Status == 404:
			code = "NotFound"
		case strings.Contains(e.Code, "Exists") || strings.Contains(e.Code, "Conflict") || e.Status == 409:
			code = "AlreadyExists"
		case e.Status == 400:
			code = "InvalidRequest"
		}
		r.Data["failed"], r.Data["ErrorCode"], r.Data["StatusMessage"] = true, code, e.Msg
	}
	c.Put(r)
	return map[string]any{"ProgressEvent": ccEvent(r)}, nil
}

func ccStatus(r *Resource) string {
	switch {
	case r.Data["failed"] == true:
		return "FAILED"
	case r.Data["canceled"] == true:
		return "CANCEL_COMPLETE"
	}
	return Step(r, "req", "IN_PROGRESS", "SUCCESS")
}

func ccEvent(r *Resource) map[string]any {
	ev := map[string]any{"RequestToken": r.Key, "Operation": r.Data["Operation"], "TypeName": r.Data["TypeName"],
		"OperationStatus": ccStatus(r), "EventTime": r.Updated.Format(timeFmt)}
	for _, k := range []string{"Identifier", "ResourceModel", "ErrorCode", "StatusMessage"} {
		if v := r.Data[k]; v != nil && v != "" {
			ev[k] = v
		}
	}
	if ev["OperationStatus"] == "FAILED" {
		delete(ev, "ResourceModel")
	}
	return ev
}

func ccListRequests(c *Call) (map[string]any, error) {
	f, _ := c.In["ResourceRequestStatusFilter"].(map[string]any)
	ops, sts := asList(f["Operations"]), asList(f["OperationStatuses"])
	var evs []any
	for _, r := range c.List("Request") {
		ev := ccEvent(r)
		if len(ops) > 0 && !slices.Contains(ops, any(ev["Operation"])) || len(sts) > 0 && !slices.Contains(sts, any(ev["OperationStatus"])) {
			continue
		}
		evs = append(evs, ev)
	}
	page, next, err := ccPage(c, evs)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"ResourceRequestStatusSummaries": page}
	if next != "" {
		out["NextToken"] = next
	}
	return out, nil
}

// ccPage cuts a list by MaxResults and NextToken (an offset).
func ccPage(c *Call, all []any) ([]any, string, error) {
	return pageOf(c, all, "NextToken", "MaxResults")
}

// ---- operations

func ccState(c *Call) (map[string]any, error) {
	var props map[string]any
	if err := json.Unmarshal([]byte(str(c.In["DesiredState"])), &props); err != nil || props == nil {
		return nil, c.Invalid("DesiredState must be a JSON object")
	}
	return props, nil
}

func ccCreate(c *Call) (map[string]any, error) {
	typeName := str(c.In["TypeName"])
	t, err := ccFind(c, typeName)
	if err != nil {
		return nil, err
	}
	props, err := ccState(c)
	if err != nil {
		return nil, err
	}
	if t.cfn != nil {
		id, attrs, err := t.cfn.CreateResource(c.Account, c.Region, typeName, props)
		if err != nil {
			return ccDone(c, "CREATE", typeName, "", nil, err)
		}
		ccStore(c, typeName, id, props, attrs)
		return ccDone(c, "CREATE", typeName, id, ccShown(ccRecord(c, typeName, id)), nil)
	}
	tc, _, err := t.call(c, kCreate, props)
	if err != nil {
		if ce := core.AsError(err); ce.Code == "UnsupportedActionException" {
			return nil, err
		}
		return ccDone(c, "CREATE", typeName, "", nil, err)
	}
	return ccDone(c, "CREATE", typeName, ccIdentifier(tc.Res), ccProperties(tc.Res), nil)
}

func ccGet(c *Call) (map[string]any, error) {
	typeName, id := str(c.In["TypeName"]), str(c.In["Identifier"])
	t, err := ccFind(c, typeName)
	if err != nil {
		return nil, err
	}
	var props map[string]any
	if t.cfn != nil {
		if r := ccRecord(c, typeName, id); r != nil {
			props = ccShown(r)
		}
	} else if r := t.svc.Get(c.Account, c.Region, t.rt.name, strings.ReplaceAll(id, "|", "/")); r != nil {
		props = ccProperties(r)
	}
	if props == nil {
		return nil, c.NotFound("Resource of type '%s' with identifier '%s' was not found.", typeName, id)
	}
	b, _ := json.Marshal(props)
	return map[string]any{"TypeName": typeName, "ResourceDescription": map[string]any{"Identifier": id, "Properties": string(b)}}, nil
}

// ponytail: of the CloudFormation-backed types only some list what was made outside Cloud Control (see cloudformation's listers); the rest list only what Cloud Control made.
func ccList(c *Call) (map[string]any, error) {
	typeName := str(c.In["TypeName"])
	t, err := ccFind(c, typeName)
	if err != nil {
		return nil, err
	}
	var descs []any
	add := func(id string, props map[string]any) {
		b, _ := json.Marshal(props)
		descs = append(descs, map[string]any{"Identifier": id, "Properties": string(b)})
	}
	if live, ok := listed(c, t); ok {
		for _, id := range core.SortedKeys(live) {
			props := live[id]
			if r := ccRecord(c, typeName, id); r != nil {
				props = ccShown(r)
			}
			add(id, props)
		}
	} else if t.cfn != nil {
		for _, r := range c.List("Resource") {
			if r.IDs["TypeName"] == typeName {
				add(r.ID, ccShown(r))
			}
		}
	} else {
		for _, r := range t.svc.List(c.Account, c.Region, t.rt.name) {
			add(ccIdentifier(r), ccProperties(r))
		}
	}
	page, next, err := ccPage(c, descs)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"TypeName": typeName, "ResourceDescriptions": page}
	if next != "" {
		out["NextToken"] = next
	}
	return out, nil
}

// listed asks the owning service for a CloudFormation-backed type's resources, when pogi can.
func listed(c *Call, t *ccTarget) (map[string]map[string]any, bool) {
	if t.cfn == nil {
		return nil, false
	}
	return t.cfn.ListResources(c.Account, c.Region, t.typeName)
}

func ccUpdate(c *Call) (map[string]any, error) {
	typeName, id := str(c.In["TypeName"]), str(c.In["Identifier"])
	t, err := ccFind(c, typeName)
	if err != nil {
		return nil, err
	}
	var patch []map[string]any
	if err := json.Unmarshal([]byte(str(c.In["PatchDocument"])), &patch); err != nil {
		return nil, c.Invalid("PatchDocument must be a JSON Patch array")
	}
	if t.cfn != nil {
		r := ccRecord(c, typeName, id)
		if r == nil {
			return nil, c.NotFound("Resource of type '%s' with identifier '%s' was not found.", typeName, id)
		}
		old, _ := r.Data["Properties"].(map[string]any)
		oldAttrs, _ := r.Data["Attributes"].(map[string]any)
		doc, err := jsonPatch(ccShown(r), patch)
		if err != nil {
			return nil, c.Invalid("%v", err)
		}
		props := doc.(map[string]any)
		for k := range oldAttrs { // what the handler made up is not a property
			if _, asked := old[k]; !asked {
				delete(props, k)
			}
		}
		attrs, replace, err := t.cfn.UpdateResource(c.Account, c.Region, typeName, id, old, props)
		if err == nil && replace {
			err = core.Errorf(400, "NotUpdatable", "The patch changes a property of %s that cannot be updated in place", typeName)
		}
		if err != nil {
			return ccDone(c, "UPDATE", typeName, id, nil, err)
		}
		if attrs == nil {
			attrs = oldAttrs
		}
		ccStore(c, typeName, id, props, attrs)
		return ccDone(c, "UPDATE", typeName, id, ccShown(ccRecord(c, typeName, id)), nil)
	}
	r := t.svc.Get(c.Account, c.Region, t.rt.name, strings.ReplaceAll(id, "|", "/"))
	if r == nil {
		return nil, c.NotFound("Resource of type '%s' with identifier '%s' was not found.", typeName, id)
	}
	doc, err := jsonPatch(ccProperties(r), patch)
	if err != nil {
		return nil, c.Invalid("%v", err)
	}
	in := doc.(map[string]any)
	delete(in, "Arn")
	tc, _, err := t.call(c, kUpdate, in)
	if err != nil {
		if ce := core.AsError(err); ce.Code == "UnsupportedActionException" {
			return ccDone(c, "UPDATE", typeName, id, nil, core.Errorf(400, "NotUpdatable", "%s cannot be updated", typeName))
		}
		return ccDone(c, "UPDATE", typeName, id, nil, err)
	}
	return ccDone(c, "UPDATE", typeName, id, ccProperties(tc.Res), nil)
}

func ccDelete(c *Call) (map[string]any, error) {
	typeName, id := str(c.In["TypeName"]), str(c.In["Identifier"])
	t, err := ccFind(c, typeName)
	if err != nil {
		return nil, err
	}
	if t.cfn != nil {
		r := ccRecord(c, typeName, id)
		if r == nil {
			return nil, c.NotFound("Resource of type '%s' with identifier '%s' was not found.", typeName, id)
		}
		old, _ := r.Data["Properties"].(map[string]any)
		if err := t.cfn.DeleteResource(c.Account, c.Region, typeName, id, old); err != nil {
			return ccDone(c, "DELETE", typeName, id, nil, err)
		}
		c.Svc.Delete(c.Account, c.Region, "Resource", ccKey(typeName, id))
		return ccDone(c, "DELETE", typeName, id, nil, nil)
	}
	r := t.svc.Get(c.Account, c.Region, t.rt.name, strings.ReplaceAll(id, "|", "/"))
	if r == nil {
		return nil, c.NotFound("Resource of type '%s' with identifier '%s' was not found.", typeName, id)
	}
	in := map[string]any{}
	for k, v := range r.IDs {
		in[k] = v
	}
	// a delete call may also need what the resource stored (WAF's LockToken)
	if ops := t.rt.ops[kDelete]; len(ops) > 0 && shortestOp(ops).in != nil {
		for _, n := range shortestOp(ops).in.Required {
			if v, ok := r.Data[n]; ok && in[n] == nil {
				in[n] = v
			}
		}
	}
	if _, _, err := t.call(c, kDelete, in); err != nil {
		return ccDone(c, "DELETE", typeName, id, nil, err)
	}
	return ccDone(c, "DELETE", typeName, id, nil, nil)
}

// ---- JSON Patch (RFC 6902)

// jsonPatch applies a patch document to a JSON value.
func jsonPatch(doc any, ops []map[string]any) (any, error) {
	b, _ := json.Marshal(doc) // work on a copy
	var cur any
	json.Unmarshal(b, &cur)
	var err error
	for _, op := range ops {
		path, from := str(op["path"]), str(op["from"])
		switch name := str(op["op"]); name {
		case "add":
			cur, err = ptrSet(cur, ptrParts(path), op["value"], true)
		case "replace":
			if _, e := ptrGet(cur, ptrParts(path)); e != nil {
				return nil, fmt.Errorf("patch: replace %s: %v", path, e)
			}
			cur, err = ptrSet(cur, ptrParts(path), op["value"], false)
		case "remove":
			cur, err = ptrRemove(cur, ptrParts(path))
		case "move", "copy":
			var v any
			if v, err = ptrGet(cur, ptrParts(from)); err == nil {
				if name == "move" {
					cur, err = ptrRemove(cur, ptrParts(from))
				}
				if err == nil {
					cur, err = ptrSet(cur, ptrParts(path), v, true)
				}
			}
		case "test":
			var v any
			if v, err = ptrGet(cur, ptrParts(path)); err == nil {
				a, _ := json.Marshal(v)
				e, _ := json.Marshal(op["value"])
				if string(a) != string(e) {
					err = fmt.Errorf("test failed")
				}
			}
		default:
			err = fmt.Errorf("unknown operation %q", name)
		}
		if err != nil {
			return nil, fmt.Errorf("patch: %s %s: %v", str(op["op"]), path, err)
		}
	}
	return cur, nil
}

func ptrParts(p string) []string {
	if p == "" {
		return nil
	}
	parts := strings.Split(strings.TrimPrefix(p, "/"), "/")
	for i, s := range parts {
		parts[i] = strings.NewReplacer("~1", "/", "~0", "~").Replace(s)
	}
	return parts
}

func ptrGet(doc any, parts []string) (any, error) {
	for _, p := range parts {
		switch x := doc.(type) {
		case map[string]any:
			v, ok := x[p]
			if !ok {
				return nil, fmt.Errorf("no member %q", p)
			}
			doc = v
		case []any:
			i, err := strconv.Atoi(p)
			if err != nil || i < 0 || i >= len(x) {
				return nil, fmt.Errorf("no index %q", p)
			}
			doc = x[i]
		default:
			return nil, fmt.Errorf("cannot descend into %q", p)
		}
	}
	return doc, nil
}

// ptrSet sets (or, with insert, inserts into an array) the value at a path and returns the new document.
func ptrSet(doc any, parts []string, v any, insert bool) (any, error) {
	if len(parts) == 0 {
		return v, nil
	}
	switch x := doc.(type) {
	case map[string]any:
		if len(parts) == 1 {
			x[parts[0]] = v
			return x, nil
		}
		sub, ok := x[parts[0]]
		if !ok {
			return nil, fmt.Errorf("no member %q", parts[0])
		}
		nv, err := ptrSet(sub, parts[1:], v, insert)
		x[parts[0]] = nv
		return x, err
	case []any:
		if len(parts) == 1 {
			if parts[0] == "-" {
				return append(x, v), nil
			}
			i, err := strconv.Atoi(parts[0])
			if err != nil || i < 0 || i > len(x) || !insert && i == len(x) {
				return nil, fmt.Errorf("no index %q", parts[0])
			}
			if insert {
				return slices.Insert(x, i, v), nil
			}
			x[i] = v
			return x, nil
		}
		i, err := strconv.Atoi(parts[0])
		if err != nil || i < 0 || i >= len(x) {
			return nil, fmt.Errorf("no index %q", parts[0])
		}
		nv, err := ptrSet(x[i], parts[1:], v, insert)
		x[i] = nv
		return x, err
	}
	return nil, fmt.Errorf("cannot descend into %q", parts[0])
}

func ptrRemove(doc any, parts []string) (any, error) {
	if len(parts) == 0 {
		return nil, fmt.Errorf("cannot remove the document")
	}
	switch x := doc.(type) {
	case map[string]any:
		if len(parts) == 1 {
			if _, ok := x[parts[0]]; !ok {
				return nil, fmt.Errorf("no member %q", parts[0])
			}
			delete(x, parts[0])
			return x, nil
		}
		sub, ok := x[parts[0]]
		if !ok {
			return nil, fmt.Errorf("no member %q", parts[0])
		}
		nv, err := ptrRemove(sub, parts[1:])
		x[parts[0]] = nv
		return x, err
	case []any:
		i, err := strconv.Atoi(parts[0])
		if err != nil || i < 0 || i >= len(x) {
			return nil, fmt.Errorf("no index %q", parts[0])
		}
		if len(parts) == 1 {
			return slices.Delete(x, i, i+1), nil
		}
		nv, err := ptrRemove(x[i], parts[1:])
		x[i] = nv
		return x, err
	}
	return nil, fmt.Errorf("cannot descend into %q", parts[0])
}
