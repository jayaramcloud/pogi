package queue

import (
	"maps"
	"net/http"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: the account's queue service and its queues. The
// storage account itself belongs to the blob package.

func (s *Service) registerARM() {
	arm.Register(s.hub, armres.ServiceProvider("Microsoft.Storage/storageAccounts/queueServices", func(p map[string]any) map[string]any {
		return armres.Defaults(p, map[string]any{"cors": map[string]any{"corsRules": []any{}}})
	}, &s.mu, &s.st.ARM))
	arm.Register(s.hub, arm.Provider{Type: "Microsoft.Storage/storageAccounts/queueServices/queues", Created: 200, Deleted: 204,
		Put: s.armPut, Get: s.armGet, Delete: s.armDelete, List: s.armList})
}

// DropAccount forgets a storage account's queues; the blob package calls it when the account is deleted.
func (s *Service) DropAccount(name string) {
	s.mu.Lock()
	delete(s.st.Accounts, name)
	s.mu.Unlock()
}

func armView(id arm.ID, q *Queue) map[string]any {
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(),
		"properties": map[string]any{"metadata": maps.Clone(q.Meta), "approximateMessageCount": len(q.Messages)}}
}

// accountOf is the storage account a queue's ID is under.
func accountOf(id arm.ID) string { return id.Parent().Parent().Name() }

func (s *Service) armPut(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	name := id.Name()
	if len(name) < 3 || len(name) > 63 {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidResourceName", "The specified resource name contains invalid characters.")
	}
	meta := map[string]string{}
	if m, ok := armres.Props(body)["metadata"].(map[string]any); ok {
		for k, v := range m {
			meta[k], _ = v.(string)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(accountOf(id))
	q := a.Queues[name]
	if q == nil {
		q = &Queue{Name: name}
		a.Queues[name] = q
	}
	q.Meta = meta
	return armView(id, q), nil
}

func (s *Service) armGet(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Accounts[accountOf(id)]; a != nil && a.Queues[id.Name()] != nil {
		return armView(id, a.Queues[id.Name()]), nil
	}
	return nil, arm.NotFound(id)
}

func (s *Service) armDelete(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Accounts[accountOf(id)]; a != nil {
		delete(a.Queues, id.Name())
	}
	return nil
}

func (s *Service) armList(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	if a := s.st.Accounts[parent.Parent().Name()]; a != nil {
		for _, n := range core.SortedKeys(a.Queues) {
			out = append(out, armView(parent.Child("queues", n), a.Queues[n]))
		}
	}
	return out, nil
}
