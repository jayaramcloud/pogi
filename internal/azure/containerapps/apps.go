package containerapps

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

func nowStamp() string { return time.Now().UTC().Format(time.RFC3339) }

// computed are the properties of an app that the API fills in, and a PATCH sends back.
var computed = []string{"provisioningState", "latestRevisionName", "latestReadyRevisionName", "latestRevisionFqdn", "outboundIpAddresses",
	"eventStreamEndpoint", "customDomainVerificationId"}

func hashOf(v any) string {
	b, _ := json.Marshal(v)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:8])
}

// validateTemplate checks the containers of a template and says it has one.
func validateTemplate(p map[string]any) error {
	tpl, _ := p["template"].(map[string]any)
	cs := objs(tpl, "containers")
	if len(cs) == 0 {
		return invalid("InvalidRequestContent", "properties.template.containers must have at least one container.")
	}
	for _, c := range cs {
		if armres.Str(c, "image") == "" {
			return invalid("InvalidRequestContent", "Every container of the template needs an image.")
		}
	}
	return nil
}

// putApp checks an app and makes its revision, when its template changed.
func (s *Service) putApp(c *core.Ctx, id arm.ID, r map[string]any) error {
	p := armres.Props(r)
	if _, err := s.environment(r); err != nil {
		return err
	}
	for _, k := range computed {
		delete(p, k)
	}
	p["environmentId"] = envOf(r)
	delete(p, "managedEnvironmentId")
	if err := validateTemplate(p); err != nil {
		return err
	}
	k := key(id)
	s.mu.Lock()
	old, _ := s.st.ARM.Get(id)
	s.mu.Unlock()
	cfg := obj(p, "configuration")
	if oc, ok := armres.Props(old)["configuration"].(map[string]any); ok {
		restoreSecrets(oc, cfg)
	}
	mode := cmp.Or(armres.Str(cfg, "activeRevisionsMode"), "Single")
	if mode != "Single" && mode != "Multiple" {
		return invalid("InvalidRequestContent", "configuration.activeRevisionsMode must be Single or Multiple, not '%s'.", mode)
	}
	cfg["activeRevisionsMode"] = mode
	if ing, ok := cfg["ingress"].(map[string]any); ok {
		delete(ing, "fqdn")
		armres.Defaults(ing, map[string]any{"external": false, "transport": "Auto", "allowInsecure": false})
		if num(ing["targetPort"]) <= 0 || num(ing["targetPort"]) > 65535 {
			return invalid("InvalidRequestContent", "configuration.ingress.targetPort must be a port, 1 to 65535.")
		}
		if err := checkTraffic(ing); err != nil {
			return err
		}
	}
	if st, _ := armres.Props(old)["runningStatus"].(string); st != "" {
		p["runningStatus"] = st
	}
	tpl := obj(p, "template")

	s.mu.Lock()
	revName := s.st.Latest[k]
	if h := hashOf(tpl); revName == "" || s.st.Hash[strings.ToLower(id.Child("revisions", revName).String())] != h {
		suffix := cmp.Or(armres.Str(tpl, "revisionSuffix"), core.RandHex(4)[:7])
		revName = id.Name() + "--" + suffix
		rid := id.Child("revisions", revName)
		if _, exists := s.st.ARM.Get(rid); exists {
			s.mu.Unlock()
			return invalid("InvalidRequestContent", "The revision '%s' already exists with another template: use another template.revisionSuffix.", revName)
		}
		rev := armres.Envelope(rid, map[string]any{"properties": map[string]any{"template": armres.Clone(tpl), "active": true,
			"createdTime": nowStamp(), "lastActiveTime": nowStamp()}})
		s.st.ARM.Put(rid, rev)
		s.st.Latest[k], s.st.Hash[strings.ToLower(rid.String())] = revName, h
		var stop []*inst
		if mode == "Single" { // the new revision takes over
			for _, o := range s.st.ARM.List(id, typRev) {
				if oid, _ := arm.ParseID(armres.Str(o, "id")); !strings.EqualFold(oid.Name(), revName) {
					armres.Props(o)["active"] = false
					s.st.ARM.Put(oid, o)
					stop = append(stop, s.take(strings.ToLower(oid.String())))
				}
			}
		}
		defer func() {
			for _, x := range stop {
				x.stop()
			}
		}()
		minReplicas := num(obj(tpl, "scale")["minReplicas"])
		_, hasIngress := cfg["ingress"].(map[string]any)
		if (minReplicas > 0 || !hasIngress) && p["runningStatus"] != "Stopped" {
			app := armres.Clone(r)
			core.Go("containerapps.warm", func() { s.warm(app, rev) })
		}
	}
	cfg["activeRevisionsMode"] = mode
	if ing, ok := cfg["ingress"].(map[string]any); ok && (mode == "Single" || len(objs(ing, "traffic")) == 0) {
		ing["traffic"] = []any{map[string]any{"latestRevision": true, "weight": 100}}
	}
	armres.Defaults(p, map[string]any{"runningStatus": "Running"})
	s.mu.Unlock()
	return nil
}

// checkTraffic wants weights that add up to 100, when there are any.
func checkTraffic(ing map[string]any) error {
	ts := objs(ing, "traffic")
	if len(ts) == 0 {
		return nil
	}
	total := 0.0
	for _, t := range ts {
		total += num(t["weight"])
		if t["latestRevision"] != true && armres.Str(t, "revisionName") == "" {
			return invalid("InvalidRequestContent", "Every traffic weight needs a revisionName or latestRevision.")
		}
	}
	if total != 100 {
		return invalid("InvalidRequestContent", "The traffic weights add up to %v, not 100.", total)
	}
	return nil
}

// viewApp adds what the API computes to a stored app.
func (s *Service) viewApp(c *core.Ctx, r map[string]any) map[string]any {
	id, _ := arm.ParseID(armres.Str(r, "id"))
	p := armres.Props(r)
	cfg := obj(p, "configuration")
	hideSecrets(cfg)
	s.mu.Lock()
	latest := s.st.Latest[key(id)]
	s.mu.Unlock()
	env := strings.ToLower(lastSeg(envOf(r)))
	host := ""
	if ing, ok := cfg["ingress"].(map[string]any); ok {
		ing["fqdn"] = strings.ToLower(id.Name()) + "." + env + hostSuffix + ":" + s.port(c)
		host = strings.ToLower(latest) + "." + env + hostSuffix + ":" + s.port(c)
	}
	p["managedEnvironmentId"] = p["environmentId"]
	p["latestRevisionName"], p["latestReadyRevisionName"], p["latestRevisionFqdn"] = latest, latest, host
	p["provisioningState"], p["outboundIpAddresses"], p["eventStreamEndpoint"] = "Succeeded", []any{"127.0.0.1"}, ""
	p["customDomainVerificationId"] = strings.ToUpper(armres.Key("containerapp/" + id.String()))[:64]
	armres.Defaults(p, map[string]any{"runningStatus": "Running", "workloadProfileName": "Consumption"})
	return r
}

func (s *Service) deleteApp(c *core.Ctx, id arm.ID) {
	k := key(id)
	s.mu.Lock()
	delete(s.st.Latest, k)
	var stop []*inst
	for name := range s.st.Hash {
		if strings.HasPrefix(name, k+"/revisions/") {
			delete(s.st.Hash, name)
			stop = append(stop, s.take(name))
		}
	}
	s.mu.Unlock()
	for _, x := range stop {
		x.stop()
	}
}

// ---- revisions ----

// weights is how an app splits its traffic: revision name → weight, latestRevision resolved; lock held.
func (s *Service) weights(app map[string]any) map[string]float64 {
	id, _ := arm.ParseID(armres.Str(app, "id"))
	cfg, _ := armres.Props(app)["configuration"].(map[string]any)
	ing, _ := cfg["ingress"].(map[string]any)
	out := map[string]float64{}
	for _, t := range objs(ing, "traffic") {
		name := armres.Str(t, "revisionName")
		if t["latestRevision"] == true {
			name = s.st.Latest[key(id)]
		}
		out[strings.ToLower(name)] += num(t["weight"])
	}
	return out
}

func (s *Service) viewRev(c *core.Ctx, r map[string]any) map[string]any {
	id, _ := arm.ParseID(armres.Str(r, "id"))
	p := armres.Props(r)
	s.mu.Lock()
	defer s.mu.Unlock()
	app, _ := s.st.ARM.Get(id.Parent())
	env := strings.ToLower(lastSeg(envOf(app)))
	active, _ := p["active"].(bool)
	p["fqdn"] = strings.ToLower(id.Name()) + "." + env + hostSuffix + ":" + s.port(c)
	p["trafficWeight"] = int(s.weights(app)[strings.ToLower(id.Name())])
	p["provisioningState"], p["healthState"], p["replicas"] = "Provisioned", "Healthy", 0
	if s.insts[key(id)] != nil {
		p["replicas"] = 1
	}
	p["runningState"] = map[bool]string{true: "Running", false: "Stopped"}[active]
	return r
}

// setActive activates or deactivates a revision, which a deactivated one stops serving.
func (s *Service) setActive(id arm.ID, on bool) error {
	s.mu.Lock()
	r, ok := s.st.ARM.Get(id)
	if !ok {
		s.mu.Unlock()
		return arm.NotFound(id)
	}
	armres.Props(r)["active"] = on
	s.st.ARM.Put(id, r)
	var x *inst
	if !on {
		x = s.take(key(id))
	}
	s.mu.Unlock()
	x.stop()
	return nil
}

func (s *Service) activate(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	return nil, s.setActive(id, true)
}

func (s *Service) deactivate(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	return nil, s.setActive(id, false)
}

// restartRev drops the revision's container; the next request starts a new one.
func (s *Service) restartRev(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	s.mu.Lock()
	_, ok := s.st.ARM.Get(id)
	x := s.take(key(id))
	s.mu.Unlock()
	if !ok {
		return nil, arm.NotFound(id)
	}
	x.stop()
	return nil, nil
}

// setRunning is the start and stop actions of an app.
func (s *Service) setRunning(on bool) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		s.mu.Lock()
		r, ok := s.st.ARM.Get(id)
		if !ok {
			s.mu.Unlock()
			return nil, arm.NotFound(id)
		}
		armres.Props(r)["runningStatus"] = map[bool]string{true: "Running", false: "Stopped"}[on]
		s.st.ARM.Put(id, r)
		var stop []*inst
		if !on {
			for _, o := range s.st.ARM.List(id, typRev) {
				stop = append(stop, s.take(strings.ToLower(armres.Str(o, "id"))))
			}
		}
		s.mu.Unlock()
		for _, x := range stop {
			x.stop()
		}
		return nil, nil
	}
}

// ---- helpers shared with jobs ----

// secretsOf is the values of an app's or job's secrets.
func secretsOf(p map[string]any) map[string]string {
	out := map[string]string{}
	cfg, _ := p["configuration"].(map[string]any)
	for _, sec := range objs(cfg, "secrets") {
		out[armres.Str(sec, "name")] = armres.Str(sec, "value")
	}
	return out
}

// envList is the environment variables of a container, with secret references resolved.
func envList(c map[string]any, secrets map[string]string) []string {
	var env []string
	for _, e := range objs(c, "env") {
		v := armres.Str(e, "value")
		if ref := armres.Str(e, "secretRef"); ref != "" {
			v = secrets[ref]
		}
		env = append(env, armres.Str(e, "name")+"="+v)
	}
	return env
}

// strs is a JSON list of strings.
func strs(v any) []string {
	var out []string
	l, _ := v.([]any)
	for _, x := range l {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return slices.Clip(out)
}

var memUnits = []struct {
	suffix string
	mult   float64
}{{"Gi", 1 << 30}, {"Mi", 1 << 20}, {"Gb", 1e9}, {"Mb", 1e6}, {"G", 1e9}, {"M", 1e6}}

// limits are the memory (bytes) and CPUs of a container's resources, 0 when it has none.
func limits(c map[string]any) (mem int64, cpu float64) {
	res, _ := c["resources"].(map[string]any)
	cpu = num(res["cpu"])
	m := armres.Str(res, "memory")
	for _, u := range memUnits {
		if n, ok := strings.CutSuffix(m, u.suffix); ok {
			var f float64
			if json.Unmarshal([]byte(n), &f) == nil {
				mem = int64(f * u.mult)
			}
			break
		}
	}
	return mem, cpu
}
