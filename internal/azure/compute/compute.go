// Package compute emulates the Azure Compute management plane: virtual machines, managed disks,
// availability sets and SSH public keys, all ARM Providers, and the instance metadata service (IMDS) of
// virtual machines.
//
// With a container engine a virtual machine runs as a container of the base image its image reference names
// (Ubuntu and Debian; Windows and any other image are metadata only): customData runs as a script at the first
// boot, as cloud-init does. The container reaches the VM's IMDS through IDENTITY_ENDPOINT and
// AZURE_POD_IDENTITY_AUTHORITY_HOST, and gets tokens from the Entra service. Without an engine the VM is
// running at once, as metadata only.
//
// ponytail: no extensions, scale sets, images, snapshots or run commands; no ssh into the container; the
// instance view is part of every GET, and GET .../instanceView is not served (the Resource Manager has no
// route for a single document at a collection path).
package compute

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

const ns = "Microsoft.Compute/"

type state struct {
	Res        map[string]map[string]any `json:"res"`        // lower-cased ID -> resource, of the four types
	CustomData map[string]string         `json:"customData"` // VM ID -> base64 customData, which a GET does not return
	Power      map[string]string         `json:"power"`      // VM ID -> running, stopped or deallocated
	Ctr        map[string]string         `json:"ctr"`        // VM ID -> container name; absent: metadata only
}

// Service is the Azure Compute emulator.
type Service struct {
	h   *core.Hub
	mu  sync.Mutex
	st  state
	eng sync.Mutex // one container-engine call at a time, taken without mu
}

func New(h *core.Hub) *Service {
	s := &Service{h: h}
	s.Reset()
	arm.Register(h, s.vmProvider())
	arm.Register(h, s.diskProvider())
	arm.Register(h, s.availabilitySetProvider())
	arm.Register(h, s.sshKeyProvider())
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "azcompute", Cloud: core.Azure, Title: "Azure Virtual Machines", Protocol: "azure-json", Match: matchIMDS}
}

// ---- helpers ----

func lower(id arm.ID) string { return strings.ToLower(id.String()) }

func str(v any) string { s, _ := v.(string); return s }

func obj(m map[string]any, k string) map[string]any { o, _ := m[k].(map[string]any); return o }

func list(m map[string]any, k string) []any { l, _ := m[k].([]any); return l }

func clone(m map[string]any) map[string]any {
	b, _ := json.Marshal(m)
	out := map[string]any{}
	json.Unmarshal(b, &out)
	return out
}

// sub returns m[k] as an object, making it when it is not one.
func sub(m map[string]any, k string) map[string]any {
	o := obj(m, k)
	if o == nil {
		o = map[string]any{}
		m[k] = o
	}
	return o
}

func cmpOr(l ...string) string {
	for _, s := range l {
		if s != "" {
			return s
		}
	}
	return ""
}

func bad(code, format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, code, format, a...)
}

// normLoc is a location as ARM stores it: "West Europe" is "westeurope".
func normLoc(l string) string { return strings.ToLower(strings.ReplaceAll(l, " ", "")) }

// idOf is the ID of a resource of the group of id.
func idOf(id arm.ID, typ, name string) arm.ID {
	id.Namespace, id.Types, id.Names = "Microsoft.Compute", []string{typ}, []string{name}
	return id
}

// ---- persistence and console ----

func (s *Service) Reset() { s.Load([]byte("{}")) }

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load replaces the state. VMs that ran in containers boot again (persisted state keeps them), and the
// containers of the old state are removed first.
func (s *Service) Load(b []byte) error {
	var st state
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Res == nil {
		st.Res = map[string]map[string]any{}
	}
	if st.CustomData == nil {
		st.CustomData = map[string]string{}
	}
	if st.Power == nil {
		st.Power = map[string]string{}
	}
	if st.Ctr == nil {
		st.Ctr = map[string]string{}
	}
	s.mu.Lock()
	old := s.st
	s.st = st
	var boot []string
	for key := range st.Ctr {
		if st.Power[key] == "running" {
			boot = append(boot, key)
		}
	}
	s.mu.Unlock()
	if len(old.Ctr) > 0 && s.h.Docker().Available() {
		s.eng.Lock()
		for _, name := range old.Ctr {
			s.h.Docker().Remove(context.Background(), name)
		}
		s.eng.Unlock()
	}
	for _, key := range boot {
		core.Go("compute.boot", func() { s.boot(key, "run") })
	}
	return nil
}

func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.Table
	for _, t := range []struct{ name, typ string }{{"Virtual machines", "virtualmachines"}, {"Disks", "disks"}, {"Availability sets", "availabilitysets"},
		{"SSH public keys", "sshpublickeys"}} {
		tb := core.Table{Name: t.name, Columns: []string{"ID", "Name", "Location", "State"}}
		for _, key := range core.SortedKeys(s.st.Res) {
			if r := s.st.Res[key]; strings.EqualFold(str(r["type"]), ns+t.typ) {
				state := str(obj(r, "properties")["provisioningState"])
				if t.typ == "virtualmachines" {
					state = s.st.Power[key]
				}
				tb.Rows = append(tb.Rows, []string{str(r["id"]), str(r["name"]), str(r["location"]), state})
			}
		}
		out = append(out, tb)
	}
	return out
}

func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.st.Res[strings.ToLower(id)]
}

// listOf lists the resources of a type under a group or subscription, or all of them for the type.
func (s *Service) listOf(typ string, parent arm.ID, view func(map[string]any) map[string]any) (out []map[string]any, _ error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pre := strings.ToLower(parent.String()) + "/"
	for _, key := range core.SortedKeys(s.st.Res) {
		if r := s.st.Res[key]; strings.HasPrefix(key, pre) && strings.EqualFold(str(r["type"]), typ) {
			out = append(out, view(clone(r)))
		}
	}
	return out, nil
}

func same(r map[string]any) map[string]any { return r }
