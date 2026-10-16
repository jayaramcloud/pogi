package compute

import (
	"cmp"
	"context"
	"encoding/base64"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

func (s *Service) vmProvider() arm.Provider {
	act := func(f func(*core.Ctx, arm.ID) error) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
		return func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) { return nil, f(c, id) }
	}
	return arm.Provider{Type: ns + "virtualMachines", Async: true, Put: s.putVM, Delete: s.deleteVM,
		Get: func(_ *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if r := s.st.Res[lower(id)]; r != nil {
				return s.vmView(lower(id), clone(r), true), nil
			}
			return nil, arm.NotFound(id)
		},
		List: func(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			return s.listOf(ns+"virtualMachines", parent, same)
		},
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"start":      act(func(_ *core.Ctx, id arm.ID) error { return s.start(lower(id)) }),
			"restart":    act(func(_ *core.Ctx, id arm.ID) error { return s.boot(lower(id), "restart") }),
			"powerOff":   act(func(_ *core.Ctx, id arm.ID) error { s.stop(lower(id), "stopped"); return nil }),
			"deallocate": act(func(_ *core.Ctx, id arm.ID) error { s.stop(lower(id), "deallocated"); return nil }),
		},
		LongActions: []string{"start", "restart", "powerOff", "deallocate"},
	}
}

// imageInfo says what a VM's image reference is: the OS and the container image its VM runs. Windows
// and images that are not Ubuntu or Debian have no container image.
func imageInfo(props map[string]any) (osType, img string) {
	ir := obj(obj(props, "storageProfile"), "imageReference")
	pub, offer, sku := strings.ToLower(str(ir["publisher"])), strings.ToLower(str(ir["offer"])), strings.ToLower(str(ir["sku"]))
	both := offer + " " + sku
	switch {
	case obj(obj(props, "osProfile"), "windowsConfiguration") != nil || strings.Contains(pub, "windows") || strings.Contains(offer, "windows"):
		return "Windows", ""
	case pub == "canonical" || strings.Contains(offer, "ubuntu"):
		v := "22.04"
		for _, w := range [][2]string{{"24_04", "24.04"}, {"2404", "24.04"}, {"noble", "24.04"}, {"20_04", "20.04"}, {"2004", "20.04"}, {"focal", "20.04"}, {"18_04", "18.04"}, {"bionic", "18.04"}} {
			if strings.Contains(both, w[0]) {
				v = w[1]
				break
			}
		}
		return "Linux", "ubuntu:" + v
	case pub == "debian" || strings.Contains(offer, "debian"):
		v := "12"
		for _, w := range []string{"13", "11", "10"} {
			if strings.Contains(both, w) {
				v = w
				break
			}
		}
		return "Linux", "debian:" + v
	}
	return "Linux", ""
}

func (s *Service) putVM(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	key := lower(id)
	props := clone(obj(body, "properties"))
	nicIDs := []string{}
	for _, n := range list(obj(props, "networkProfile"), "networkInterfaces") {
		m, _ := n.(map[string]any)
		nicIDs = append(nicIDs, str(m["id"]))
		if err := s.checkRef(c, str(m["id"]), "Microsoft.Network/networkInterfaces"); err != nil {
			return nil, err
		}
	}
	if len(nicIDs) == 0 {
		return nil, bad("InvalidParameter", "Required parameter 'networkProfile.networkInterfaces' is missing (null).")
	}
	ident, err := s.identity(c, body["identity"])
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	old := s.st.Res[key]
	oldProps := obj(old, "properties")
	loc := normLoc(cmp.Or(str(body["location"]), str(old["location"])))
	if loc == "" {
		s.mu.Unlock()
		return nil, bad("LocationRequired", "The location property is required for this definition.")
	}
	size := cmp.Or(str(obj(props, "hardwareProfile")["vmSize"]), str(obj(oldProps, "hardwareProfile")["vmSize"]))
	if size == "" {
		s.mu.Unlock()
		return nil, bad("InvalidParameter", "Required parameter 'hardwareProfile.vmSize' is missing (null).")
	}
	if as := str(obj(props, "availabilitySet")["id"]); as != "" && s.st.Res[strings.ToLower(as)] == nil {
		s.mu.Unlock()
		return nil, bad("InvalidResourceReference", "Resource %s referenced by resource %s was not found.", as, id.String())
	}
	st := sub(props, "storageProfile")
	if old != nil { // the image and the OS disk of a VM stay
		st["imageReference"] = obj(oldProps, "storageProfile")["imageReference"]
	}
	osType, _ := imageInfo(props)
	osd := sub(st, "osDisk")
	osProf := obj(props, "osProfile")
	custom := str(osProf["customData"])
	if _, err := base64.StdEncoding.DecodeString(custom); err != nil {
		s.mu.Unlock()
		return nil, bad("InvalidParameter", "Custom data in OSProfile must be in Base64 encoding.")
	}
	if old == nil && cmp.Or(str(osd["createOption"]), "FromImage") == "FromImage" {
		if err := checkOSProfile(osProf, osType); err != nil {
			s.mu.Unlock()
			return nil, err
		}
	}

	// the disks: nothing changes until all of them are right
	var add []map[string]any // disks to make
	attach := map[string]bool{}
	vmID := id.String()
	mkDisk := func(name, createOption string, size int, sku string, os string) map[string]any {
		cd := map[string]any{"createOption": createOption}
		if createOption == "FromImage" {
			cd["imageReference"] = map[string]any{"id": "/Subscriptions/" + id.Subscription + "/Providers/Microsoft.Compute/Locations/" + loc + "/Publishers/" + str(obj(st, "imageReference")["publisher"]) +
				"/ArtifactTypes/VMImage/Offers/" + str(obj(st, "imageReference")["offer"]) + "/Skus/" + str(obj(st, "imageReference")["sku"]) + "/Versions/" + cmp.Or(str(obj(st, "imageReference")["version"]), "latest")}
		}
		return diskDoc(idOf(id, "disks", name), loc, nil, sku, size, os, cd, vmID, nil)
	}
	attachExisting := func(diskID, what string) (map[string]any, error) {
		d := s.st.Res[strings.ToLower(diskID)]
		if d == nil {
			return nil, bad("InvalidResourceReference", "Resource %s referenced by %s was not found.", diskID, what)
		}
		if by := str(d["managedBy"]); by != "" && !strings.EqualFold(by, vmID) {
			return nil, core.Errorf(http.StatusConflict, "OperationNotAllowed", "Disk %s is already attached to VM %s.", str(d["name"]), by)
		}
		attach[strings.ToLower(diskID)] = true
		return d, nil
	}
	if old != nil {
		st["osDisk"] = mergeKeep(obj(obj(oldProps, "storageProfile"), "osDisk"), osd, "caching", "deleteOption", "writeAcceleratorEnabled")
		osd = obj(st, "osDisk")
		attach[strings.ToLower(str(obj(osd, "managedDisk")["id"]))] = true
	} else {
		sku := cmp.Or(str(obj(osd, "managedDisk")["storageAccountType"]), "Premium_LRS")
		switch create := cmp.Or(str(osd["createOption"]), "FromImage"); create {
		case "FromImage":
			name := cmp.Or(str(osd["name"]), id.Name()+"_OsDisk_1_"+core.RandHex(16))
			if s.st.Res[strings.ToLower(idOf(id, "disks", name).String())] != nil {
				s.mu.Unlock()
				return nil, core.Errorf(http.StatusConflict, "Conflict", "Disk %s already exists.", name)
			}
			size := intOf(osd["diskSizeGB"])
			if size == 0 {
				size = map[string]int{"Linux": 30, "Windows": 127}[osType]
			}
			d := mkDisk(name, "FromImage", size, sku, osType)
			add = append(add, d)
			st["osDisk"] = map[string]any{"osType": osType, "name": name, "createOption": "FromImage", "caching": cmp.Or(str(osd["caching"]), "ReadWrite"),
				"managedDisk": map[string]any{"storageAccountType": sku, "id": d["id"]}, "diskSizeGB": size, "deleteOption": cmp.Or(str(osd["deleteOption"]), "Detach")}
		case "Attach":
			d, err := attachExisting(str(obj(osd, "managedDisk")["id"]), "storageProfile.osDisk")
			if err != nil {
				s.mu.Unlock()
				return nil, err
			}
			osType = cmp.Or(str(osd["osType"]), str(obj(d, "properties")["osType"]), "Linux")
			st["osDisk"] = map[string]any{"osType": osType, "name": d["name"], "createOption": "Attach", "caching": cmp.Or(str(osd["caching"]), "ReadWrite"),
				"managedDisk": map[string]any{"id": d["id"], "storageAccountType": str(obj(d, "sku")["name"])}, "deleteOption": cmp.Or(str(osd["deleteOption"]), "Detach")}
		default:
			s.mu.Unlock()
			return nil, bad("InvalidParameter", "Invalid value '%s' for storageProfile.osDisk.createOption.", create)
		}
	}
	data := []any{}
	for i, d := range list(st, "dataDisks") {
		m, _ := d.(map[string]any)
		if m == nil {
			continue
		}
		m = clone(m)
		lun := intOf(m["lun"])
		name := str(m["name"])
		switch create := str(m["createOption"]); create {
		case "Empty":
			size := intOf(m["diskSizeGB"])
			if name == "" || size == 0 {
				s.mu.Unlock()
				return nil, bad("InvalidParameter", "storageProfile.dataDisks[%d] needs a name and diskSizeGB for createOption Empty.", i)
			}
			did := idOf(id, "disks", name).String()
			if ex := s.st.Res[strings.ToLower(did)]; ex != nil { // an earlier PUT made it
				if _, err := attachExisting(did, "storageProfile.dataDisks"); err != nil {
					s.mu.Unlock()
					return nil, err
				}
			} else {
				dd := mkDisk(name, "Empty", size, cmp.Or(str(obj(m, "managedDisk")["storageAccountType"]), "Premium_LRS"), "")
				add = append(add, dd)
			}
			m["managedDisk"] = map[string]any{"id": did, "storageAccountType": cmp.Or(str(obj(m, "managedDisk")["storageAccountType"]), "Premium_LRS")}
		case "Attach":
			d, err := attachExisting(str(obj(m, "managedDisk")["id"]), "storageProfile.dataDisks")
			if err != nil {
				s.mu.Unlock()
				return nil, err
			}
			m["name"], m["diskSizeGB"] = d["name"], obj(d, "properties")["diskSizeGB"]
			m["managedDisk"] = map[string]any{"id": d["id"], "storageAccountType": str(obj(d, "sku")["name"])}
		default:
			s.mu.Unlock()
			return nil, bad("InvalidParameter", "Invalid value '%s' for storageProfile.dataDisks[%d].createOption.", create, i)
		}
		m["lun"], m["caching"], m["toBeDetached"] = lun, cmp.Or(str(m["caching"]), "None"), false
		m["deleteOption"], m["writeAcceleratorEnabled"] = cmp.Or(str(m["deleteOption"]), "Detach"), false
		data = append(data, m)
	}
	st["dataDisks"] = data
	if ir := obj(st, "imageReference"); ir != nil {
		ir["exactVersion"] = cmp.Or(str(ir["exactVersion"]), exactVersion(str(ir["version"])))
	}

	// commit
	for _, d := range add {
		s.st.Res[strings.ToLower(str(d["id"]))] = d
		attach[strings.ToLower(str(d["id"]))] = true
	}
	for k, d := range s.st.Res {
		switch {
		case attach[k]:
			d["managedBy"] = vmID
			sub(d, "properties")["diskState"] = "Attached"
		case strings.EqualFold(str(d["managedBy"]), vmID):
			delete(d, "managedBy") // detached by this PUT
			sub(d, "properties")["diskState"] = "Unattached"
		}
	}
	props["provisioningState"] = "Succeeded"
	props["vmId"] = cmp.Or(str(oldProps["vmId"]), core.UUID())
	props["timeCreated"] = cmp.Or(str(oldProps["timeCreated"]), time.Now().UTC().Format(time.RFC3339Nano))
	props["hardwareProfile"] = map[string]any{"vmSize": size}
	delete(props, "osProfile")
	if osProf != nil {
		op := clone(osProf)
		delete(op, "customData")
		delete(op, "adminPassword")
		op["computerName"] = cmp.Or(str(op["computerName"]), id.Name())
		if osType == "Linux" {
			lc := sub(op, "linuxConfiguration")
			if lc["disablePasswordAuthentication"] == nil {
				lc["disablePasswordAuthentication"] = false
			}
			lc["provisionVMAgent"] = true
		}
		op["secrets"], op["allowExtensionOperations"], op["requireGuestProvisionSignal"] = []any{}, true, true
		props["osProfile"] = op
	} else if old != nil {
		props["osProfile"] = oldProps["osProfile"]
	}
	if custom == "" && osProf == nil {
		custom = s.st.CustomData[key]
	}
	r := arm.Resource(id, loc, body["tags"], props)
	if old != nil {
		if _, set := body["tags"]; !set {
			r["tags"] = old["tags"]
		}
	}
	if ident != nil {
		r["identity"] = ident
	}
	for _, f := range []string{"zones", "plan", "extendedLocation"} {
		if body[f] != nil {
			r[f] = body[f]
		}
	}
	s.st.Res[key] = r
	if custom != "" {
		s.st.CustomData[key] = custom
	}
	if old == nil {
		s.st.Power[key] = "running"
	}
	out := clone(r)
	s.mu.Unlock()

	if old == nil {
		if err := s.boot(key, "run"); err != nil {
			s.mu.Lock()
			sub(s.st.Res[key], "properties")["provisioningState"] = "Failed"
			s.st.Power[key] = "stopped"
			s.mu.Unlock()
			return nil, err
		}
	}
	return out, nil
}

// mergeKeep is the stored map with the given keys of the request's map applied.
func mergeKeep(stored, req map[string]any, keys ...string) map[string]any {
	out := clone(stored)
	for _, k := range keys {
		if v := req[k]; v != nil {
			out[k] = v
		}
	}
	return out
}

func exactVersion(v string) string {
	if v == "" || v == "latest" {
		return "22.04.202601120"
	}
	return v
}

// checkOSProfile is what a VM made from an image must say about its administrator.
func checkOSProfile(p map[string]any, osType string) error {
	if p == nil {
		return bad("InvalidParameter", "Required parameter 'osProfile' is missing (null).")
	}
	if str(p["adminUsername"]) == "" {
		return bad("InvalidParameter", "Required parameter 'adminUsername' is missing (null).")
	}
	keys := list(obj(obj(p, "linuxConfiguration"), "ssh"), "publicKeys")
	if str(p["adminPassword"]) == "" && (osType == "Windows" || len(keys) == 0) {
		return bad("InvalidParameter", "Required parameter 'adminPassword' is missing (null).")
	}
	return nil
}

// checkRef fails when the resource at ref, of a type another service provides, does not exist. The type's
// service may be off, then nothing can be said.
func (s *Service) checkRef(c *core.Ctx, ref, typ string) error {
	p := arm.Lookup(s.h, typ)
	rid, ok := arm.ParseID(ref)
	if !ok || !strings.EqualFold(rid.Type(), typ) {
		return bad("InvalidParameter", "The resource id '%s' is not a valid %s id.", ref, typ)
	}
	if p == nil || p.Get == nil {
		return nil
	}
	if _, err := p.Get(c, rid); err != nil {
		return bad("InvalidResourceReference", "Resource %s referenced by resource was not found. Please make sure that the referenced resource exists, and that both resources are in the same region.", ref)
	}
	return nil
}

// identity is the identity block of a VM: a system-assigned principal and the user-assigned identities, which
// the Entra service describes.
func (s *Service) identity(c *core.Ctx, in any) (map[string]any, error) {
	m, _ := in.(map[string]any)
	typ := str(m["type"])
	if typ == "" || strings.EqualFold(typ, "None") {
		return nil, nil
	}
	out := map[string]any{"type": typ}
	if strings.Contains(typ, "SystemAssigned") {
		out["principalId"], out["tenantId"] = core.UUID(), arm.TenantID
	}
	if ua := obj(m, "userAssignedIdentities"); len(ua) > 0 {
		ids := map[string]any{}
		for uid := range ua {
			e := map[string]any{"principalId": core.UUID(), "clientId": core.UUID()}
			if p := arm.Lookup(s.h, "Microsoft.ManagedIdentity/userAssignedIdentities"); p != nil && p.Get != nil {
				rid, _ := arm.ParseID(uid)
				d, err := p.Get(c, rid)
				if err != nil {
					return nil, bad("InvalidResourceReference", "Resource %s referenced by resource was not found.", uid)
				}
				pr := obj(d, "properties")
				e["principalId"], e["clientId"] = pr["principalId"], pr["clientId"]
			}
			ids[uid] = e
		}
		out["userAssignedIdentities"] = ids
	}
	return out, nil
}

// vmView adds the instance view; mu is held.
func (s *Service) vmView(key string, r map[string]any, withView bool) map[string]any {
	if !withView {
		return r
	}
	props := sub(r, "properties")
	power := cmp.Or(s.st.Power[key], "running")
	t := time.Now().UTC().Format(time.RFC3339Nano)
	label := map[string]string{"running": "VM running", "stopped": "VM stopped", "deallocated": "VM deallocated"}[power]
	agent := "Ready"
	if power != "running" {
		agent = "Not Ready"
	}
	osType := str(obj(obj(props, "storageProfile"), "osDisk")["osType"])
	disks := []any{}
	for _, d := range append([]any{obj(obj(props, "storageProfile"), "osDisk")}, list(obj(props, "storageProfile"), "dataDisks")...) {
		m, _ := d.(map[string]any)
		disks = append(disks, map[string]any{"name": m["name"], "statuses": []any{map[string]any{"code": "ProvisioningState/succeeded", "level": "Info", "displayStatus": "Provisioning succeeded", "time": t}}})
	}
	props["instanceView"] = map[string]any{"computerName": obj(props, "osProfile")["computerName"], "osName": strings.ToLower(osType), "osVersion": "pogi",
		"vmAgent": map[string]any{"vmAgentVersion": "2.9.1.1", "statuses": []any{map[string]any{"code": "ProvisioningState/succeeded", "level": "Info", "displayStatus": agent, "message": "Guest Agent is running", "time": t}}},
		"disks":   disks, "hyperVGeneration": "V1", "platformUpdateDomain": 0, "platformFaultDomain": 0,
		"statuses": []any{map[string]any{"code": "ProvisioningState/" + strings.ToLower(str(props["provisioningState"])), "level": "Info", "displayStatus": "Provisioning " + strings.ToLower(str(props["provisioningState"])), "time": t},
			map[string]any{"code": "PowerState/" + power, "level": "Info", "displayStatus": label}}}
	return r
}

func (s *Service) deleteVM(_ *core.Ctx, id arm.ID) error {
	key := lower(id)
	s.mu.Lock()
	r := s.st.Res[key]
	name := s.st.Ctr[key]
	s.mu.Unlock()
	if r == nil {
		return nil
	}
	if name != "" {
		s.eng.Lock()
		s.h.Docker().Remove(context.Background(), name)
		s.eng.Unlock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	st := obj(obj(r, "properties"), "storageProfile")
	for _, d := range append([]any{st["osDisk"]}, list(st, "dataDisks")...) {
		m, _ := d.(map[string]any)
		dk := strings.ToLower(str(obj(m, "managedDisk")["id"]))
		if m["deleteOption"] == "Delete" {
			delete(s.st.Res, dk)
		} else if dd := s.st.Res[dk]; dd != nil {
			delete(dd, "managedBy")
			sub(dd, "properties")["diskState"] = "Unattached"
		}
	}
	delete(s.st.Res, key)
	delete(s.st.CustomData, key)
	delete(s.st.Power, key)
	delete(s.st.Ctr, key)
	return nil
}

// ---- power ----

func (s *Service) start(key string) error {
	s.mu.Lock()
	running := s.st.Power[key] == "running"
	s.mu.Unlock()
	if running {
		return nil
	}
	return s.boot(key, "start")
}

// stop stops the container of a VM; power is stopped or deallocated.
func (s *Service) stop(key, power string) {
	s.mu.Lock()
	name := s.st.Ctr[key]
	s.st.Power[key] = power
	s.mu.Unlock()
	if name != "" {
		s.eng.Lock()
		s.h.Docker().Stop(context.Background(), name)
		s.eng.Unlock()
	}
}

// boot runs the VM's container: "run" makes it, "start" starts the stopped one and "restart" restarts it. Without
// an engine, or for an image that has no container, it only sets the power state.
func (s *Service) boot(key, verb string) error {
	d := s.h.Docker()
	engine := d.Available()
	s.mu.Lock()
	r := s.st.Res[key]
	if r == nil {
		s.mu.Unlock()
		return nil
	}
	props := obj(r, "properties")
	if _, img := imageInfo(props); verb == "run" && engine && img != "" && s.st.Ctr[key] == "" {
		s.st.Ctr[key] = docker.Name("azcompute", str(props["vmId"]))
	}
	name := s.st.Ctr[key]
	if name == "" {
		s.st.Power[key] = "running"
		s.mu.Unlock()
		return nil
	}
	ct := s.container(key, r, name)
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
		s.h.Log.Warn("azure vm container failed", "vm", key, "err", err)
		return core.Errorf(http.StatusInternalServerError, "InternalExecutionError", "The virtual machine's container failed: %v", err)
	}
	s.mu.Lock()
	s.st.Power[key] = "running"
	s.mu.Unlock()
	return nil
}

// bootScript is the entrypoint of VM containers: it runs customData once, as cloud-init does (logged to
// cloud-init-output.log), and stays up.
const bootScript = `trap 'exit 0' TERM INT
mkdir -p /var/lib/cloud/instance /var/log
if [ -n "${POGI_CUSTOM_DATA:-}" ] && [ ! -e /var/lib/cloud/instance/boot-finished ]; then
  printf '%s' "$POGI_CUSTOM_DATA" > /var/lib/cloud/instance/user-data.txt
  chmod +x /var/lib/cloud/instance/user-data.txt
  env -u POGI_CUSTOM_DATA /var/lib/cloud/instance/user-data.txt >> /var/log/cloud-init-output.log 2>&1
fi
touch /var/lib/cloud/instance/boot-finished
while :; do sleep 3600 & wait $!; done`

// container describes a VM's container; mu is held.
func (s *Service) container(key string, r map[string]any, name string) *docker.Container {
	props := obj(r, "properties")
	_, img := imageInfo(props)
	host := s.h.Docker().HostAddr()
	port := strconv.Itoa(cmp.Or(s.h.Cfg.Port, 7644))
	if u, err := url.Parse(s.h.URL); err == nil && u.Port() != "" {
		port = u.Port()
	}
	imds := "http://" + host + ":" + port + "/_azure/imds/" + str(props["vmId"])
	ct := &docker.Container{Name: name, Image: img, Service: "azcompute", Resource: str(props["vmId"]),
		Entrypoint: []string{"/bin/sh", "-c", bootScript}, Aliases: []string{str(obj(props, "osProfile")["computerName"])},
		Env: []string{"IDENTITY_ENDPOINT=" + imds + "/msi/token", "IDENTITY_HEADER=pogi", "AZURE_POD_IDENTITY_AUTHORITY_HOST=" + imds,
			"AZURE_SUBSCRIPTION_ID=" + strings.Split(key, "/")[2]}}
	if host == "127.0.0.1" { // a Linux engine without a VM reaches pogi's loopback from the host network only
		// ponytail: there VMs share the host's ports; give them their own network namespace if that bites
		ct.Network, ct.Aliases = "host", nil
	}
	if sh := script(s.st.CustomData[key]); sh != "" {
		ct.Env = append(ct.Env, "POGI_CUSTOM_DATA="+sh)
	}
	return ct
}

// script is the shell script customData runs: a #! script as is, or the runcmd list of a #cloud-config.
// ponytail: a runcmd subset (one-line items, plain or [list]); other cloud-config modules, MIME multipart and
// gzip are ignored.
func script(customData string) string {
	b, _ := base64.StdEncoding.DecodeString(customData)
	text := string(b)
	switch {
	case strings.HasPrefix(text, "#!"):
		return text
	case !strings.HasPrefix(text, "#cloud-config"):
		return ""
	}
	var cmds []string
	in := false
	for _, line := range strings.Split(text, "\n") {
		t := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "runcmd:"):
			in = true
		case in && strings.HasPrefix(t, "- "):
			cmds = append(cmds, shellLine(strings.TrimSpace(t[2:])))
		case in && t != "" && line[0] != ' ' && line[0] != '\t':
			in = false
		}
	}
	if len(cmds) == 0 {
		return ""
	}
	return "#!/bin/sh\n" + strings.Join(cmds, "\n") + "\n"
}

// shellLine turns a runcmd item into a shell line: a string as is, a list word by word, quoted.
func shellLine(item string) string {
	if !strings.HasPrefix(item, "[") {
		return unquote(item)
	}
	var words []string
	for _, w := range strings.Split(strings.Trim(item, "[]"), ",") {
		words = append(words, "'"+strings.ReplaceAll(unquote(strings.TrimSpace(w)), "'", `'\''`)+"'")
	}
	return strings.Join(words, " ")
}

func unquote(s string) string {
	if len(s) >= 2 && (s[0] == '"' || s[0] == '\'') && s[len(s)-1] == s[0] {
		if u, err := strconv.Unquote(s); err == nil && s[0] == '"' {
			return u
		}
		return s[1 : len(s)-1]
	}
	return s
}
