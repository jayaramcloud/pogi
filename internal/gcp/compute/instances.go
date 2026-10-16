package compute

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

func instanceKind() *kind {
	return &kind{kind: "compute#instance", fill: fillInstance, after: func(q *req, path string) error { return q.s.boot(path, "run") }, del: delInstance,
		acts: map[string]func(*req, string) error{"start": startInstance, "stop": stopInstance, "reset": resetInstance,
			"setMetadata": setMetadata, "setLabels": setLabels},
		reads: map[string]func(*req, string) (any, error){"getGuestAttributes": getGuestAttributes}}
}

// fillInstance validates a new instance and creates the disks it asks for; mu is held. It changes nothing
// until everything is valid.
func fillInstance(q *req, path string, doc map[string]any) error {
	s := q.s
	p := strings.Split(path, "/") // projects, p, zones, z, instances, n
	zp, name := strings.Join(p[:4], "/"), p[5]
	mt := ref(str(doc["machineType"]), zp+"/machineTypes")
	if i := slices.IndexFunc(machines, func(m machine) bool { return m.name == mt[strings.LastIndexByte(mt, '/')+1:] }); i < 0 {
		return invalid("Invalid value for field 'resource.machineType': '%s'. Machine type with name '%s' does not exist in zone '%s'.", str(doc["machineType"]), str(doc["machineType"]), p[3])
	}
	nics := arr(doc, "networkInterfaces")
	if len(nics) == 0 {
		return invalid("At least one network interface is required.")
	}
	ips := s.st.IP // allocations are given back when a later check fails
	rollback := func(err error) error { s.st.IP = ips; return err }
	for i, n := range nics {
		m, _ := n.(map[string]any)
		if m == nil {
			return rollback(invalid("Invalid value for field 'resource.networkInterfaces[%d]'.", i))
		}
		net, subnet := str(m["network"]), ""
		if sub := str(m["subnetwork"]); sub != "" {
			subnet = ref(sub, "projects/"+q.project+"/regions/"+regionOf(p[3])+"/subnetworks")
			sd := s.st.Res[subnet]
			if sd == nil {
				return rollback(notFound(subnet))
			}
			net = str(sd["network"])
			m["subnetwork"] = api + subnet
		}
		if net == "" {
			return rollback(invalid("Required field 'networkInterfaces[%d].network' or 'subnetwork' not specified", i))
		}
		np, err := s.network(q.project, net)
		if err != nil {
			return rollback(err)
		}
		m["network"], m["name"], m["fingerprint"] = api+np, "nic"+strconv.Itoa(i), fingerprint()
		if str(m["networkIP"]) == "" {
			m["networkIP"] = s.intIP(subnet)
		}
		m["stackType"] = "IPV4_ONLY"
		for _, a := range arr(m, "accessConfigs") {
			if ac, _ := a.(map[string]any); ac != nil {
				ac["type"], ac["name"], ac["networkTier"] = firstOf(str(ac["type"]), "ONE_TO_ONE_NAT"), firstOf(str(ac["name"]), "External NAT"), "PREMIUM"
				if str(ac["natIP"]) == "" {
					ac["natIP"] = s.extIP()
				}
			}
		}
	}

	self := api + path
	disks := arr(doc, "disks")
	if len(disks) == 0 {
		return rollback(invalid("Invalid value for field 'resource.disks': ''. At least one disk, the boot disk, is required."))
	}
	made := map[string]map[string]any{} // disks to create, by path
	attach := []string{}                // existing disks to attach
	for i, d := range disks {
		m, _ := d.(map[string]any)
		if m == nil {
			return rollback(invalid("Invalid value for field 'resource.disks[%d]'.", i))
		}
		var dp string
		if ip := obj(m, "initializeParams"); ip != nil || str(m["source"]) == "" {
			dn := firstOf(str(ip["diskName"]), name)
			if i > 0 && str(ip["diskName"]) == "" {
				dn += "-" + strconv.Itoa(i)
			}
			dp = zp + "/disks/" + dn
			dd := map[string]any{"name": dn, "sourceImage": ip["sourceImage"], "sizeGb": ip["diskSizeGb"], "type": ip["diskType"], "labels": ip["labels"]}
			if dd["sourceImage"] == nil && i == 0 {
				return rollback(invalid("Invalid value for field 'resource.disks[0].initializeParams.sourceImage': ''. Boot disk must have a source image."))
			}
			for k, v := range dd {
				if v == nil {
					delete(dd, k)
				}
			}
			if s.st.Res[dp] != nil || made[dp] != nil {
				return rollback(core.Errorf(http.StatusConflict, "alreadyExists", "The resource '%s' already exists", dp))
			}
			dd["kind"], dd["id"], dd["creationTimestamp"], dd["selfLink"], dd["zone"] = "compute#disk", newID(), now(), api+dp, api+zp
			saved := q.r.URL.RawQuery
			q.r.URL.RawQuery = "" // a sourceImage query parameter belongs to disks.insert
			err := fillDisk(q, dp, dd)
			q.r.URL.RawQuery = saved
			if err != nil {
				return rollback(err)
			}
			dd["users"] = []any{self}
			made[dp] = dd
			m["diskSizeGb"] = dd["sizeGb"]
		} else {
			dp = ref(str(m["source"]), zp+"/disks")
			sd := s.st.Res[dp]
			if sd == nil {
				return rollback(notFound(dp))
			}
			if u := arr(sd, "users"); len(u) > 0 {
				return rollback(invalid("The disk resource '%s' is already being used by '%s'", dp, strings.TrimPrefix(str(u[0]), api)))
			}
			attach = append(attach, dp)
			m["diskSizeGb"] = sd["sizeGb"]
		}
		delete(m, "initializeParams")
		m["source"], m["type"], m["mode"], m["index"] = api+dp, "PERSISTENT", firstOf(str(m["mode"]), "READ_WRITE"), i
		m["interface"], m["deviceName"] = firstOf(str(m["interface"]), "SCSI"), firstOf(str(m["deviceName"]), "persistent-disk-"+strconv.Itoa(i))
		m["kind"], m["boot"] = "compute#attachedDisk", i == 0 || m["boot"] == true
		if m["autoDelete"] == nil {
			m["autoDelete"] = false
		}
	}
	for dp, dd := range made {
		s.st.Res[dp] = dd
	}
	for _, dp := range attach {
		s.st.Res[dp]["users"] = []any{self}
	}

	doc["machineType"], doc["status"] = api+mt, "STAGING"
	doc["metadata"] = metadataDoc(obj(doc, "metadata"))
	doc["tags"] = map[string]any{"items": arr(obj(doc, "tags"), "items"), "fingerprint": fingerprint()}
	if arr(obj(doc, "tags"), "items") == nil {
		doc["tags"].(map[string]any)["items"] = []any{}
	}
	if doc["labels"] != nil {
		doc["labelFingerprint"] = fingerprint()
	}
	if obj(doc, "scheduling") == nil {
		doc["scheduling"] = map[string]any{"automaticRestart": true, "onHostMaintenance": "MIGRATE", "preemptible": false, "provisioningModel": "STANDARD"}
	}
	for _, f := range []string{"canIpForward", "deletionProtection", "startRestricted"} {
		if doc[f] == nil {
			doc[f] = false
		}
	}
	doc["cpuPlatform"], doc["fingerprint"] = "Intel Broadwell", fingerprint()
	return nil
}

// ---- lifecycle ----

// setStatus sets an instance's status, and returns the name of its container.
func (s *Service) setStatus(path, status string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if d := s.st.Res[path]; d != nil {
		d["status"] = status
	}
	return s.st.Ctr[path]
}

// boot runs the instance's container: "run" makes it, "start" starts the stopped one and "restart" restarts
// it. Without an engine it only sets the status. The container's own failure is the error.
func (s *Service) boot(path, verb string) error {
	d := s.hub.Docker()
	engine := d.Available()
	s.mu.Lock()
	doc := s.st.Res[path]
	if doc == nil {
		s.mu.Unlock()
		return nil
	}
	if verb == "run" && engine && s.st.Ctr[path] == "" {
		s.st.Ctr[path] = docker.Name("compute", str(doc["id"]))
	}
	name := s.st.Ctr[path]
	if name == "" {
		doc["status"] = "RUNNING"
		s.mu.Unlock()
		return nil
	}
	doc["status"] = "STAGING"
	ct := s.container(path, doc, name)
	s.mu.Unlock()

	s.eng.Lock()
	defer s.eng.Unlock()
	ctx := context.Background()
	var err error
	switch {
	case verb == "start" && d.Start(ctx, name) == nil, verb == "restart" && d.Restart(ctx, name) == nil:
	default:
		d.Remove(ctx, name) // a leftover
		_, err = d.Run(ctx, ct)
	}
	if err != nil {
		s.hub.Log.Warn("compute instance container failed", "instance", path, "err", err)
		s.setStatus(path, "TERMINATED")
		return core.Errorf(http.StatusInternalServerError, "internalError", "The instance container failed: %v", err)
	}
	s.setStatus(path, "RUNNING")
	return nil
}

func startInstance(q *req, path string) error {
	if q.s.status(path) == "RUNNING" {
		return nil
	}
	return q.s.boot(path, "start")
}

func resetInstance(q *req, path string) error { return q.s.boot(path, "restart") }

func (s *Service) status(path string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return str(s.st.Res[path]["status"])
}

func stopInstance(q *req, path string) error {
	s := q.s
	if name := s.setStatus(path, "STOPPING"); name != "" {
		s.eng.Lock()
		s.hub.Docker().Stop(context.Background(), name)
		s.eng.Unlock()
	}
	s.setStatus(path, "TERMINATED")
	return nil
}

func delInstance(q *req, path string, doc map[string]any) error {
	s := q.s
	name := s.setStatus(path, "STOPPING")
	s.mu.Lock()
	l := s.md[path]
	delete(s.md, path)
	s.mu.Unlock()
	l.close()
	if name != "" {
		s.eng.Lock()
		s.hub.Docker().Remove(context.Background(), name)
		s.eng.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, d := range arr(doc, "disks") {
		m, _ := d.(map[string]any)
		dp := ref(str(m["source"]), "")
		if m["autoDelete"] == true {
			delete(s.st.Res, dp)
		} else if sd := s.st.Res[dp]; sd != nil {
			sd["users"] = []any{}
		}
	}
	delete(s.st.Res, path)
	delete(s.st.Ctr, path)
	delete(s.st.Guest, path)
	return nil
}

func setMetadata(q *req, path string) error {
	s := q.s
	s.mu.Lock()
	defer s.mu.Unlock()
	doc := s.st.Res[path]
	if err := checkFingerprint(q.body["fingerprint"], obj(doc, "metadata"), "fingerprint", "metadata"); err != nil {
		return err
	}
	doc["metadata"] = metadataDoc(q.body)
	return nil
}

func getGuestAttributes(q *req, path string) (any, error) {
	s := q.s
	query := q.r.URL.Query()
	s.mu.Lock()
	defer s.mu.Unlock()
	attrs := s.st.Guest[path]
	self := api + path + "/getGuestAttributes"
	if key := query.Get("variableKey"); key != "" {
		v, ok := attrs[key]
		if !ok {
			return nil, notFound(path + "/guestAttributes/" + key)
		}
		return map[string]any{"kind": "compute#guestAttributes", "variableKey": key, "variableValue": v, "selfLink": self}, nil
	}
	prefix := query.Get("queryPath")
	items := []any{}
	for _, k := range core.SortedKeys(attrs) {
		if ns, key, _ := strings.Cut(k, "/"); strings.HasPrefix(k, prefix) {
			items = append(items, map[string]any{"namespace": ns, "key": key, "value": attrs[k]})
		}
	}
	if len(items) == 0 {
		return nil, notFound(path + "/guestAttributes/" + prefix)
	}
	return map[string]any{"kind": "compute#guestAttributes", "queryPath": prefix, "queryValue": map[string]any{"items": items}, "selfLink": self}, nil
}

// ---- the container ----

// bootScript is the entrypoint of instance containers: it runs the startup-script metadata at every boot, in the
// background as the guest agent does, and stays up. Its output goes to the container's log, the serial console.
// ponytail: the script is read when the container is made, so a changed startup-script reaches a stopped
// instance only when a new one is made; startup-script-url and shutdown scripts are not run.
const bootScript = `trap 'exit 0' TERM INT
if [ -n "${POGI_STARTUP:-}" ]; then
  printf '%s\n' "$POGI_STARTUP" > /var/run/google.startup.script
  chmod +x /var/run/google.startup.script
  env -u POGI_STARTUP /var/run/google.startup.script &
fi
while :; do sleep 3600 & wait $!; done`

// container describes an instance's container; mu is held.
func (s *Service) container(path string, doc map[string]any, name string) *docker.Container {
	p := strings.Split(path, "/")
	img := "debian:12"
	for _, d := range arr(doc, "disks") {
		m, _ := d.(map[string]any)
		if m["boot"] != true {
			continue
		}
		if im := findImage(str(s.st.Res[ref(str(m["source"]), "")]["sourceImage"])); im != nil {
			img = im.docker
		}
	}
	host := s.hub.Docker().HostAddr()
	hostname := p[5] + "." + p[3] + ".c." + p[1] + ".internal"
	ct := &docker.Container{Name: name, Image: img, Service: "compute", Resource: str(doc["id"]), Entrypoint: []string{"/bin/sh", "-c", bootScript},
		Aliases: []string{hostname, p[5]},
		Env:     []string{"GCE_METADATA_HOST=" + host + ":" + s.listener(path), "GOOGLE_CLOUD_PROJECT=" + p[1], "GCLOUD_PROJECT=" + p[1]}}
	if host == "127.0.0.1" { // a Linux engine without a VM reaches pogi's loopback from the host network only
		// ponytail: there instances share the host's ports; give them their own network namespace if that bites
		ct.Network, ct.Aliases = "host", nil
	}
	for _, it := range arr(obj(doc, "metadata"), "items") {
		if m, _ := it.(map[string]any); m != nil && m["key"] == "startup-script" && str(m["value"]) != "" {
			sh := str(m["value"])
			if !strings.HasPrefix(sh, "#!") {
				sh = "#!/bin/sh\n" + sh
			}
			ct.Env = append(ct.Env, "POGI_STARTUP="+sh)
		}
	}
	return ct
}
