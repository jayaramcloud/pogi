package armres

import (
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// Stored describes a resource type that the resource manager keeps in a Store and a service
// mirrors: Put and Delete make the service do its part, View adds what the service computes
// (endpoints, keys, state) to a copy of the stored body on every read.
type Stored struct {
	Type    string
	Mu      sync.Locker // the service's lock, which guards St
	St      *Store
	Put     func(c *core.Ctx, id arm.ID, r map[string]any) error // before the body is stored; may be nil
	Delete  func(c *core.Ctx, id arm.ID)                         // after it is gone; may be nil
	View    func(c *core.Ctx, r map[string]any) map[string]any   // nil: the stored body
	Actions map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error)
	Async   bool // PUT and DELETE are long-running operations, as the SDKs expect of this type
	// Tombstone, when set, leaves a record of a deleted resource under Namespace/locations/<Tombstone>/<name>
	// (see Tombstone), with the old resource's ID as configurationStoreId.
	Tombstone string
}

// Tombstone records that a soft-deletable resource (a Key Vault, an App Configuration store) was
// deleted, as <Namespace>/locations/<location>/<typ>/<name>: azurerm looks there before it creates one
// (a soft-deleted twin must be recovered or purged first) and purges after it deletes one. props
// are what the record shows, typically the old resource's ID under the key the API calls it by.
func Tombstone(st *Store, id arm.ID, typ, location string, props map[string]any) {
	t := arm.ID{Subscription: id.Subscription, Namespace: id.Namespace, Types: []string{"locations", typ}, Names: []string{location, id.Name()}}
	now := time.Now().UTC()
	Defaults(props, map[string]any{"location": location, "deletionDate": now.Format(time.RFC3339), "scheduledPurgeDate": now.Add(7 * 24 * time.Hour).Format(time.RFC3339)})
	st.Put(t, map[string]any{"id": t.String(), "name": id.Name(), "type": t.Type(), "properties": props})
}

// TombstoneProvider serves the tombstones of one type (Microsoft.KeyVault/locations/deletedVaults): they
// are read, listed and purged, which forgets them. ns/locations/<typ> is the type.
func TombstoneProvider(ns, typ string, mu sync.Locker, st *Store) arm.Provider {
	full := ns + "/locations/" + typ
	return arm.Provider{Type: full, LongActions: []string{"purge"},
		Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			r, ok := st.Get(id)
			if !ok {
				return nil, arm.NotFound(id)
			}
			return r, nil
		},
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			mu.Lock()
			defer mu.Unlock()
			return st.List(parent, full), nil
		},
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"purge": func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
				mu.Lock()
				st.Delete(id)
				mu.Unlock()
				return nil, nil
			},
		},
	}
}

// Provider makes the arm.Provider.
func (d Stored) Provider() arm.Provider {
	view := func(c *core.Ctx, r map[string]any) map[string]any {
		if d.View != nil {
			return d.View(c, r)
		}
		return r
	}
	return arm.Provider{Type: d.Type, Actions: d.Actions, Async: d.Async,
		Put: func(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
			r := Envelope(id, body)
			if d.Put != nil {
				if err := d.Put(c, id, r); err != nil {
					return nil, err
				}
			}
			d.Mu.Lock()
			d.St.Put(id, r)
			d.Mu.Unlock()
			return view(c, Clone(r)), nil
		},
		Get: func(c *core.Ctx, id arm.ID) (map[string]any, error) {
			d.Mu.Lock()
			r, ok := d.St.Get(id)
			d.Mu.Unlock()
			if !ok {
				return nil, arm.NotFound(id)
			}
			return view(c, r), nil
		},
		Delete: func(c *core.Ctx, id arm.ID) error {
			d.Mu.Lock()
			if r, ok := d.St.Get(id); ok && d.Tombstone != "" {
				Tombstone(d.St, id, d.Tombstone, Str(r, "location"), map[string]any{"configurationStoreId": id.String(), "tags": r["tags"]})
			}
			d.St.Delete(id)
			d.Mu.Unlock()
			if d.Delete != nil {
				d.Delete(c, id)
			}
			return nil
		},
		List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
			d.Mu.Lock()
			stored := d.St.List(parent, d.Type)
			d.Mu.Unlock()
			var out []map[string]any
			for _, r := range stored {
				out = append(out, view(c, r))
			}
			return out, nil
		},
	}
}
