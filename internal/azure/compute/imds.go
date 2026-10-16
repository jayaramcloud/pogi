package compute

// The instance metadata service. A VM's container reaches it under /_azure/imds/<vmId>/ on pogi's port
// (IDENTITY_ENDPOINT, AZURE_POD_IDENTITY_AUTHORITY_HOST), which is how pogi knows who asks; the token
// endpoints under that prefix are the Entra service's, called with the prefix removed. The plain
// /metadata/instance answers for the only VM there is.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"sort"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

var imdsVersions = []string{"2021-02-01", "2021-12-13", "2023-07-01", "2023-11-15", "2025-04-07"}

func isIdentity(p string) bool { return p == "/metadata/identity/oauth2/token" || p == "/msi/token" }

func matchIMDS(r *http.Request) bool {
	p := r.URL.Path
	return strings.HasPrefix(p, "/_azure/imds/") || strings.HasPrefix(p, "/metadata/instance") || strings.HasPrefix(p, "/metadata/attested") ||
		p == "/metadata/versions"
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	p, vmID := r.URL.Path, ""
	if rest, ok := strings.CutPrefix(p, "/_azure/imds/"); ok {
		vmID, p, _ = strings.Cut(rest, "/")
		p = "/" + p
	}
	if isIdentity(p) { // the token endpoints are Entra's: it checks the headers and the identity itself
		entra := s.h.Service("entra")
		if entra == nil {
			http.NotFound(w, r)
			return
		}
		r2 := r.Clone(r.Context())
		r2.URL.Path = p
		entra.ServeHTTP(w, r2)
		return
	}
	c.Op = "GetInstanceMetadata"
	if err := c.Intercept(); err != nil {
		writeJSON(w, core.AsError(err).Status, map[string]string{"error": err.Error()})
		return
	}
	if r.Header.Get("Metadata") != "true" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Bad request. Required metadata header not specified"})
		return
	}
	if p == "/metadata/versions" {
		writeJSON(w, http.StatusOK, map[string]any{"apiVersions": imdsVersions})
		return
	}
	q := r.URL.Query()
	if v := q.Get("api-version"); v == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Bad request. api-version was not specified in the request. For more information refer to aka.ms/azureimds", "newest-versions": imdsVersions[len(imdsVersions)-3:]})
		return
	} else if !isVersion(v) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Bad request. Api version " + v + " is not supported. For more information refer to aka.ms/azureimds", "newest-versions": imdsVersions[len(imdsVersions)-3:]})
		return
	}
	if strings.HasPrefix(p, "/metadata/attested") {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Attested data is not available"})
		return
	}
	tree, err := s.imdsTree(c, vmID)
	if err != nil {
		writeJSON(w, core.AsError(err).Status, map[string]string{"error": err.Error()})
		return
	}
	var node any = tree
	for _, part := range strings.Split(strings.Trim(strings.TrimPrefix(p, "/metadata/instance"), "/"), "/") {
		if part == "" {
			continue
		}
		var next any
		switch n := node.(type) {
		case map[string]any:
			next = n[part]
		case []any:
			if i := atoi(part); i >= 0 && i < len(n) {
				next = n[i]
			}
		}
		if next == nil {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": "Not found"})
			return
		}
		node = next
	}
	if q.Get("format") == "text" {
		w.Header().Set("Content-Type", "text/plain")
		switch n := node.(type) {
		case map[string]any:
			names := make([]string, 0, len(n))
			for k, v := range n {
				if _, dir := v.(map[string]any); dir {
					k += "/"
				}
				names = append(names, k)
			}
			sort.Strings(names)
			fmt.Fprint(w, strings.Join(names, "\n"))
		case []any:
			for i := range n {
				fmt.Fprintln(w, i)
			}
		default:
			fmt.Fprint(w, n)
		}
		return
	}
	writeJSON(w, http.StatusOK, node)
}

func isVersion(v string) bool {
	for _, x := range imdsVersions {
		if x == v {
			return true
		}
	}
	return v >= "2017-03-01" && v <= imdsVersions[len(imdsVersions)-1] && len(v) == 10
}

func atoi(s string) int {
	n := 0
	for _, c := range s {
		if c < '0' || c > '9' {
			return -1
		}
		n = n*10 + int(c-'0')
	}
	return n
}

// imdsTree is the metadata of a VM: the one with this vmId, or the only one.
func (s *Service) imdsTree(c *core.Ctx, vmID string) (map[string]any, error) {
	s.mu.Lock()
	var vm map[string]any
	for _, k := range core.SortedKeys(s.st.Res) {
		r := s.st.Res[k]
		if !strings.EqualFold(str(r["type"]), ns+"virtualMachines") {
			continue
		}
		if id := str(obj(r, "properties")["vmId"]); vmID != "" && id == vmID {
			vm = clone(r)
			break
		} else if vmID == "" {
			if vm != nil {
				s.mu.Unlock()
				return nil, core.Errorf(http.StatusNotFound, "NotFound", "Several virtual machines exist: ask /_azure/imds/<vmId>/metadata/instance")
			}
			vm = clone(r)
		}
	}
	disks := map[string]map[string]any{}
	for k, d := range s.st.Res {
		if strings.EqualFold(str(d["type"]), ns+"disks") {
			disks[k] = clone(d)
		}
	}
	s.mu.Unlock()
	if vm == nil {
		return nil, core.Errorf(http.StatusNotFound, "NotFound", "No such virtual machine")
	}
	id, _ := arm.ParseID(str(vm["id"]))
	props := obj(vm, "properties")
	st, osProf := obj(props, "storageProfile"), obj(props, "osProfile")
	ir := obj(st, "imageReference")
	osd := obj(st, "osDisk")
	disk := func(d map[string]any) map[string]any {
		md := obj(d, "managedDisk")
		dd := disks[strings.ToLower(str(md["id"]))]
		return map[string]any{"name": str(d["name"]), "caching": str(d["caching"]), "createOption": str(d["createOption"]), "diskSizeGB": fmt.Sprint(obj(dd, "properties")["diskSizeGB"]),
			"managedDisk": map[string]any{"id": str(md["id"]), "storageAccountType": str(md["storageAccountType"])}, "writeAcceleratorEnabled": "false",
			"encryptionSettings": map[string]any{"enabled": "false"}, "image": map[string]any{"uri": ""}, "vhd": map[string]any{"uri": ""}, "diffDiskSettings": map[string]any{"option": ""}}
	}
	os := disk(osd)
	os["osType"] = str(osd["osType"])
	dd := []any{}
	for _, d := range list(st, "dataDisks") {
		m, _ := d.(map[string]any)
		x := disk(m)
		x["lun"] = fmt.Sprint(intOf(m["lun"]))
		dd = append(dd, x)
	}
	var tagList []any
	var tags []string
	tm, _ := vm["tags"].(map[string]any)
	for _, k := range core.SortedKeys(tm) {
		tagList = append(tagList, map[string]any{"name": k, "value": str(tm[k])})
		tags = append(tags, k+":"+str(tm[k]))
	}
	keys := []any{}
	for _, k := range list(obj(obj(osProf, "linuxConfiguration"), "ssh"), "publicKeys") {
		m, _ := k.(map[string]any)
		keys = append(keys, map[string]any{"keyData": str(m["keyData"]), "path": str(m["path"])})
	}
	zone := ""
	if z := list(vm, "zones"); len(z) > 0 {
		zone = str(z[0])
	}
	compute := map[string]any{"azEnvironment": "AzurePublicCloud", "customData": "", "evictionPolicy": "", "isHostCompatibilityLayerVm": "false", "licenseType": "",
		"location": str(vm["location"]), "name": str(vm["name"]), "offer": str(ir["offer"]), "osType": str(osd["osType"]), "placementGroupId": "",
		"plan": map[string]any{"name": "", "product": "", "publisher": ""}, "platformFaultDomain": "0", "platformUpdateDomain": "0", "priority": "",
		"provider": "Microsoft.Compute", "publicKeys": keys, "publisher": str(ir["publisher"]), "resourceGroupName": id.ResourceGroup, "resourceId": str(vm["id"]),
		"securityProfile": map[string]any{"secureBootEnabled": "false", "virtualTpmEnabled": "false"}, "sku": str(ir["sku"]),
		"storageProfile": map[string]any{"dataDisks": dd, "imageReference": map[string]any{"id": "", "offer": str(ir["offer"]), "publisher": str(ir["publisher"]), "sku": str(ir["sku"]), "version": str(ir["exactVersion"])},
			"osDisk": os},
		"subscriptionId": id.Subscription, "tags": strings.Join(tags, ";"), "tagsList": tagList, "userData": "", "version": str(ir["exactVersion"]), "vmId": str(props["vmId"]),
		"vmScaleSetName": "", "vmSize": str(obj(props, "hardwareProfile")["vmSize"]), "zone": zone,
		"osProfile": map[string]any{"adminUsername": str(osProf["adminUsername"]), "computerName": str(osProf["computerName"]),
			"disablePasswordAuthentication": fmt.Sprint(obj(osProf, "linuxConfiguration")["disablePasswordAuthentication"] == true)}}

	var nics []any
	for _, n := range list(obj(props, "networkProfile"), "networkInterfaces") {
		m, _ := n.(map[string]any)
		nics = append(nics, s.imdsNIC(c, str(m["id"])))
	}
	return map[string]any{"compute": compute, "network": map[string]any{"interface": nics}}, nil
}

// imdsNIC is a network interface of a VM as IMDS tells it, from what the network service knows of it.
func (s *Service) imdsNIC(c *core.Ctx, nicID string) map[string]any {
	get := func(id, typ string) map[string]any {
		p := arm.Lookup(s.h, typ)
		rid, ok := arm.ParseID(id)
		if p == nil || p.Get == nil || !ok {
			return nil
		}
		d, _ := p.Get(c, rid)
		return d
	}
	var addrs, subnets []any
	mac := ""
	if nic := get(nicID, "Microsoft.Network/networkInterfaces"); nic != nil {
		props := obj(nic, "properties")
		mac = strings.ToUpper(strings.ReplaceAll(str(props["macAddress"]), "-", ""))
		for _, ic := range list(props, "ipConfigurations") {
			ip := obj(ic.(map[string]any), "properties")
			a := map[string]any{"privateIpAddress": str(ip["privateIPAddress"]), "publicIpAddress": ""}
			if pip := get(str(obj(ip, "publicIPAddress")["id"]), "Microsoft.Network/publicIPAddresses"); pip != nil {
				a["publicIpAddress"] = str(obj(pip, "properties")["ipAddress"])
			}
			addrs = append(addrs, a)
			if sn := get(str(obj(ip, "subnet")["id"]), "Microsoft.Network/virtualNetworks/subnets"); sn != nil {
				if pf, err := netip.ParsePrefix(str(obj(sn, "properties")["addressPrefix"])); err == nil {
					subnets = append(subnets, map[string]any{"address": pf.Masked().Addr().String(), "prefix": fmt.Sprint(pf.Bits())})
				}
			}
		}
	}
	if mac == "" {
		mac = "000D3A" + strings.ToUpper(strings.ReplaceAll(core.UUID(), "-", ""))[:6]
	}
	return map[string]any{"ipv4": map[string]any{"ipAddress": orEmpty(addrs), "subnet": orEmpty(subnets)}, "ipv6": map[string]any{"ipAddress": []any{}}, "macAddress": mac}
}

func orEmpty(l []any) []any {
	if l == nil {
		return []any{}
	}
	return l
}
