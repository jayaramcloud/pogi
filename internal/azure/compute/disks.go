package compute

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
	"golang.org/x/crypto/ssh"
)

// intOf reads a number: float64 from a request or a loaded state, int from a document made here.
func intOf(v any) int {
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	}
	return 0
}

// diskDoc is a managed disk as ARM returns it; managedBy is the ID of the VM it is attached to.
func diskDoc(id arm.ID, loc string, tags any, sku string, size int, osType string, cd map[string]any, managedBy string, zones any) map[string]any {
	tier := "Standard"
	if strings.HasPrefix(sku, "Premium") {
		tier = "Premium"
	}
	state := "Unattached"
	if managedBy != "" {
		state = "Attached"
	}
	props := map[string]any{"creationData": cd, "diskSizeGB": size, "diskSizeBytes": int64(size) << 30, "diskState": state, "provisioningState": "Succeeded",
		"timeCreated": time.Now().UTC().Format(time.RFC3339Nano), "uniqueId": core.UUID(), "encryption": map[string]any{"type": "EncryptionAtRestWithPlatformKey"},
		"networkAccessPolicy": "AllowAll", "publicNetworkAccess": "Enabled", "diskIOPSReadWrite": 500, "diskMBpsReadWrite": 60}
	if osType != "" {
		props["osType"] = osType
	}
	d := arm.Resource(id, loc, tags, props)
	d["sku"] = map[string]any{"name": sku, "tier": tier}
	if managedBy != "" {
		d["managedBy"] = managedBy
	}
	if zones != nil {
		d["zones"] = zones
	}
	return d
}

func (s *Service) diskProvider() arm.Provider {
	return arm.Provider{Type: ns + "disks", Async: true, Created: http.StatusAccepted,
		Put: func(_ *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			key, props := lower(id), obj(body, "properties")
			old := s.st.Res[key]
			loc := normLoc(cmpOr(str(body["location"]), str(old["location"])))
			if loc == "" {
				return nil, bad("LocationRequired", "The location property is required for this definition.")
			}
			cd := obj(props, "creationData")
			if cd == nil && old != nil {
				cd = obj(obj(old, "properties"), "creationData")
			}
			size := intOf(props["diskSizeGB"])
			switch create := str(cd["createOption"]); create {
			case "":
				return nil, bad("InvalidParameter", "Required parameter 'creationData.createOption' is missing (null).")
			case "Empty":
				if size == 0 {
					return nil, bad("InvalidParameter", "diskSizeGB is required when creationData.createOption is Empty.")
				}
			case "Copy":
				src := s.st.Res[strings.ToLower(str(cd["sourceResourceId"]))]
				if src == nil {
					return nil, core.Errorf(http.StatusNotFound, "ResourceNotFound", "The source disk '%s' was not found.", str(cd["sourceResourceId"]))
				}
				size = max(size, intOf(obj(src, "properties")["diskSizeGB"]))
			case "FromImage":
				size = max(size, 30)
			}
			managedBy := str(old["managedBy"])
			if old != nil {
				if cur := intOf(obj(old, "properties")["diskSizeGB"]); size == 0 {
					size = cur
				} else if size < cur {
					return nil, bad("OperationNotAllowed", "Disk size reduction is not supported. Current size is %d GB, requested %d GB.", cur, size)
				}
			}
			sku := cmpOr(str(obj(body, "sku")["name"]), str(obj(old, "sku")["name"]), "Standard_LRS")
			d := diskDoc(id, loc, body["tags"], sku, size, str(props["osType"]), cd, managedBy, body["zones"])
			if old != nil {
				if _, set := body["tags"]; !set {
					d["tags"] = old["tags"]
				}
				obj(d, "properties")["uniqueId"], obj(d, "properties")["timeCreated"] = obj(old, "properties")["uniqueId"], obj(old, "properties")["timeCreated"]
			}
			s.st.Res[key] = d
			return clone(d), nil
		},
		Get: func(_ *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if d := s.st.Res[lower(id)]; d != nil {
				return clone(d), nil
			}
			return nil, arm.NotFound(id)
		},
		Delete: func(_ *core.Ctx, id arm.ID) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			key := lower(id)
			if by := str(s.st.Res[key]["managedBy"]); by != "" {
				return core.Errorf(http.StatusConflict, "OperationNotAllowed", "Disk %s is attached to VM %s.", id.Name(), by)
			}
			delete(s.st.Res, key)
			return nil
		},
		List: func(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) { return s.listOf(ns+"disks", parent, same) },
	}
}

func (s *Service) availabilitySetProvider() arm.Provider {
	// the VMs of a set are computed on read
	view := func(key string, r map[string]any) map[string]any {
		vms := []any{}
		for _, k := range core.SortedKeys(s.st.Res) {
			if v := s.st.Res[k]; strings.EqualFold(str(v["type"]), ns+"virtualMachines") &&
				strings.EqualFold(str(obj(obj(v, "properties"), "availabilitySet")["id"]), str(r["id"])) {
				vms = append(vms, map[string]any{"id": v["id"]})
			}
		}
		obj(r, "properties")["virtualMachines"] = vms
		return r
	}
	return arm.Provider{Type: ns + "availabilitySets", Created: http.StatusOK,
		Put: func(_ *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			key, props := lower(id), obj(body, "properties")
			old := s.st.Res[key]
			loc := normLoc(cmpOr(str(body["location"]), str(old["location"])))
			if loc == "" {
				return nil, bad("LocationRequired", "The location property is required for this definition.")
			}
			sku := cmpOr(str(obj(body, "sku")["name"]), str(obj(old, "sku")["name"]), "Classic")
			if sku != "Classic" && sku != "Aligned" {
				return nil, bad("InvalidParameter", "The specified sku name '%s' is not valid. Allowed values are Classic and Aligned.", sku)
			}
			fd, ud := intOf(props["platformFaultDomainCount"]), intOf(props["platformUpdateDomainCount"])
			if fd == 0 {
				fd = 3
			}
			if ud == 0 {
				ud = 5
			}
			if fd > 3 || ud > 20 {
				return nil, bad("InvalidParameter", "The fault domain count must be between 1 and 3 and the update domain count between 1 and 20.")
			}
			p := map[string]any{"platformFaultDomainCount": fd, "platformUpdateDomainCount": ud}
			if v := props["proximityPlacementGroup"]; v != nil {
				p["proximityPlacementGroup"] = v
			}
			r := arm.Resource(id, loc, body["tags"], p)
			r["sku"] = map[string]any{"name": sku}
			s.st.Res[key] = r
			return view(key, clone(r)), nil
		},
		Get: func(_ *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if r := s.st.Res[lower(id)]; r != nil {
				return view(lower(id), clone(r)), nil
			}
			return nil, arm.NotFound(id)
		},
		Delete: func(_ *core.Ctx, id arm.ID) error {
			s.mu.Lock()
			defer s.mu.Unlock()
			key := lower(id)
			if r := s.st.Res[key]; r != nil && len(list(obj(view(key, clone(r)), "properties"), "virtualMachines")) > 0 {
				return core.Errorf(http.StatusConflict, "OperationNotAllowed", "The availability set '%s' still has virtual machines.", id.Name())
			}
			delete(s.st.Res, key)
			return nil
		},
		List: func(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			var out []map[string]any
			pre := strings.ToLower(parent.String()) + "/"
			for _, k := range core.SortedKeys(s.st.Res) {
				if r := s.st.Res[k]; strings.HasPrefix(k, pre) && strings.EqualFold(str(r["type"]), ns+"availabilitySets") {
					out = append(out, view(k, clone(r)))
				}
			}
			return out, nil
		},
	}
}

func (s *Service) sshKeyProvider() arm.Provider {
	return arm.Provider{Type: ns + "sshPublicKeys",
		Put: func(_ *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			key := lower(id)
			old := s.st.Res[key]
			loc := normLoc(cmpOr(str(body["location"]), str(old["location"])))
			if loc == "" {
				return nil, bad("LocationRequired", "The location property is required for this definition.")
			}
			pub := cmpOr(str(obj(body, "properties")["publicKey"]), str(obj(old, "properties")["publicKey"]))
			if pub != "" {
				if _, _, _, _, err := ssh.ParseAuthorizedKey([]byte(pub)); err != nil {
					return nil, bad("InvalidParameter", "Public key is not a valid SSH public key: %v", err)
				}
			}
			props := map[string]any{}
			if pub != "" {
				props["publicKey"] = pub
			}
			r := arm.Resource(id, loc, body["tags"], props)
			s.st.Res[key] = r
			return clone(r), nil
		},
		Get: func(_ *core.Ctx, id arm.ID) (map[string]any, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			if r := s.st.Res[lower(id)]; r != nil {
				return clone(r), nil
			}
			return nil, arm.NotFound(id)
		},
		Delete: func(_ *core.Ctx, id arm.ID) error {
			s.mu.Lock()
			delete(s.st.Res, lower(id))
			s.mu.Unlock()
			return nil
		},
		List: func(_ *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			return s.listOf(ns+"sshPublicKeys", parent, same)
		},
		// generateKeyPair makes a key pair, keeps the public key and returns both.
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"generateKeyPair": func(_ *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
			k, err := rsa.GenerateKey(rand.Reader, 2048)
			if err != nil {
				return nil, err
			}
			pk, err := ssh.NewPublicKey(&k.PublicKey)
			if err != nil {
				return nil, err
			}
			pub := strings.TrimSpace(string(ssh.MarshalAuthorizedKey(pk)))
			s.mu.Lock()
			if r := s.st.Res[lower(id)]; r != nil {
				sub(r, "properties")["publicKey"] = pub
			}
			s.mu.Unlock()
			priv := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
			return map[string]any{"privateKey": string(priv), "publicKey": pub, "id": id.String()}, nil
		}},
	}
}
