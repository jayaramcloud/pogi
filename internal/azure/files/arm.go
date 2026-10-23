package files

import (
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// The resource-manager side of Azure Files: Microsoft.Storage/storageAccounts/fileServices/shares.
// (The "default" file service is the Blob package's, next to the account.)

func (s *Service) registerARM() {
	arm.Register(s.hub, arm.Provider{Type: "Microsoft.Storage/storageAccounts/fileServices/shares", Created: 201, Deleted: 204,
		Put: s.armPut, Get: s.armGet, Delete: s.armDelete, List: s.armList})
}

// accountOf is the storage account a share's ID is under.
func accountOf(id arm.ID) string { return id.Parent().Parent().Name() }

func armView(id arm.ID, sh *Share) map[string]any {
	meta := map[string]string{}
	for k, v := range sh.Meta {
		meta[k] = v
	}
	p := map[string]any{"shareQuota": sh.Quota, "enabledProtocols": "SMB", "accessTier": or(sh.Tier, "TransactionOptimized"),
		"metadata": meta, "etag": sh.ETag, "lastModifiedTime": sh.Modified.UTC().Format("2006-01-02T15:04:05.0000000Z"),
		"leaseStatus": "Unlocked", "leaseState": "Available", "signedIdentifiers": []any{}, "deleted": false, "remainingRetentionDays": 0}
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "etag": sh.ETag, "properties": p}
}

func (s *Service) armPut(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	name, p := id.Name(), armres.Props(body)
	if !validShare(name) {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidResourceName", "The specified resource name contains invalid characters.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(accountOf(id))
	sh := a.Shares[name]
	if sh == nil {
		sh = &Share{Name: name, Quota: defaultQuota, Entries: map[string]*Entry{}}
		a.Shares[name] = sh
	}
	if q, ok := p["shareQuota"].(float64); ok {
		if q < 1 || q > 102400 {
			return nil, core.Errorf(http.StatusBadRequest, "InvalidInput", "The share quota must be between 1 and 102400 GiB.")
		}
		sh.Quota = int(q)
	}
	if t := armres.Str(p, "accessTier"); t != "" {
		sh.Tier = t
	}
	if m, ok := p["metadata"].(map[string]any); ok {
		sh.Meta = map[string]string{}
		for k, v := range m {
			sh.Meta[strings.ToLower(k)], _ = v.(string)
		}
	}
	sh.ETag, sh.Modified = s.etag(), time.Now().UTC()
	return armView(id, sh), nil
}

func (s *Service) armGet(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Accounts[accountOf(id)]; a != nil && a.Shares[id.Name()] != nil {
		return armView(id, a.Shares[id.Name()]), nil
	}
	return nil, arm.NotFound(id)
}

func (s *Service) armDelete(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Accounts[accountOf(id)]; a != nil {
		delete(a.Shares, id.Name())
	}
	return nil
}

func (s *Service) armList(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	if a := s.st.Accounts[parent.Parent().Name()]; a != nil {
		for _, n := range core.SortedKeys(a.Shares) {
			out = append(out, armView(parent.Child("shares", n), a.Shares[n]))
		}
	}
	return out, nil
}
