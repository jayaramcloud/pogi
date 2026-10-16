package blob

import (
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// The resource-manager side of Storage: the account itself (here, since the
// blob service is the one every account has), its blob and file services and
// the containers. Queue and table services register their own types.

const accounts = "Microsoft.Storage/storageAccounts"

func (s *Service) registerARM() {
	h := s.hub
	arm.Register(h, arm.Provider{Type: accounts, Async: true, Created: 202, Deleted: 200, Put: s.putAccount, Get: s.getAccount, Delete: s.deleteAccount, List: s.listAccounts,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){"listKeys": s.listKeys}})
	for _, svc := range []string{"blobServices", "fileServices"} {
		arm.Register(h, armres.ServiceProvider(accounts+"/"+svc, func(p map[string]any) map[string]any {
			if svc == "fileServices" {
				return armres.Defaults(p, map[string]any{"cors": map[string]any{"corsRules": []any{}},
					"shareDeleteRetentionPolicy": map[string]any{"enabled": true, "days": 7}, "protocolSettings": map[string]any{"smb": map[string]any{}}})
			}
			return armres.Defaults(p, map[string]any{"cors": map[string]any{"corsRules": []any{}}, "isVersioningEnabled": false,
				"deleteRetentionPolicy": map[string]any{"enabled": false}, "containerDeleteRetentionPolicy": map[string]any{"enabled": false},
				"changeFeed": map[string]any{"enabled": false}, "restorePolicy": map[string]any{"enabled": false}})
		}, &s.mu, &s.st.ARM))
	}
	// an account that was never migrated reads as one whose (last) migration is complete
	arm.Register(h, armres.ServiceProvider(accounts+"/accountMigrations", func(p map[string]any) map[string]any {
		return armres.Defaults(p, map[string]any{"targetSkuName": "Standard_LRS", "migrationStatus": "Complete"})
	}, &s.mu, &s.st.ARM))
	arm.Register(h, arm.Provider{Type: accounts + "/blobServices/containers", Created: 200, Put: s.armPutContainer, Get: s.armGetContainer,
		Delete: s.armDeleteContainer, List: s.armListContainers})
}

// ---- accounts ----

func (s *Service) accountView(c *core.Ctx, stored map[string]any) map[string]any {
	r := stored
	p := armres.Props(r)
	id, _ := arm.ParseID(armres.Str(r, "id"))
	name := id.Name()
	// the data planes answer on the main port to <account>.<service>.localhost (armres.HostMatch); azurerm
	// reads the account's name off that host
	ep := func(service string) string { return armres.Labeled(c, "https", name+"."+service) + "/" }
	armres.Defaults(r, map[string]any{"kind": "StorageV2", "sku": map[string]any{"name": "Standard_LRS"}})
	sku := r["sku"].(map[string]any)
	sku["tier"] = "Standard"
	if strings.HasPrefix(armres.Str(sku, "name"), "Premium") {
		sku["tier"] = "Premium"
	}
	armres.Defaults(p, map[string]any{
		"accessTier": "Hot", "supportsHttpsTrafficOnly": true, "minimumTlsVersion": "TLS1_2", "allowBlobPublicAccess": false,
		"allowSharedKeyAccess": true, "isHnsEnabled": false, "isSftpEnabled": false, "isLocalUserEnabled": false, "isNfsV3Enabled": false,
		"defaultToOAuthAuthentication": false, "publicNetworkAccess": "Enabled", "allowCrossTenantReplication": false, "dnsEndpointType": "Standard",
		"networkAcls": map[string]any{"bypass": "AzureServices", "defaultAction": "Allow", "ipRules": []any{}, "virtualNetworkRules": []any{}},
		"encryption": map[string]any{"keySource": "Microsoft.Storage", "requireInfrastructureEncryption": false, "services": map[string]any{
			"blob": map[string]any{"enabled": true, "keyType": "Account"}, "file": map[string]any{"enabled": true, "keyType": "Account"}}},
	})
	p["provisioningState"], p["statusOfPrimary"], p["primaryLocation"] = "Succeeded", "available", r["location"]
	p["primaryEndpoints"] = map[string]any{
		"blob":  ep("blob"),
		"queue": ep("queue"),
		"table": ep("table"),
		"file":  "https://" + name + ".file.core.windows.net/", "web": "https://" + name + ".z13.web.core.windows.net/", "dfs": "https://" + name + ".dfs.core.windows.net/",
	}
	return r
}

func (s *Service) putAccount(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	r := armres.Envelope(id, body)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.st.ARM.Get(id); ok {
		armres.Props(r)["creationTime"] = armres.Props(old)["creationTime"]
	} else {
		armres.Props(r)["creationTime"] = time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z")
	}
	s.account(id.Name()) // the data plane has it from now on
	s.st.ARM.Put(id, r)
	return s.accountView(c, armres.Clone(r)), nil
}

func (s *Service) getAccount(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.st.ARM.Get(id)
	if !ok {
		return nil, arm.NotFound(id)
	}
	return s.accountView(c, r), nil
}

func (s *Service) listAccounts(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, r := range s.st.ARM.List(parent, accounts) {
		out = append(out, s.accountView(c, r))
	}
	return out, nil
}

// deleteAccount drops the account's blobs and, through their optional DropAccount, its queues and tables.
func (s *Service) deleteAccount(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	delete(s.st.Accounts, id.Name())
	s.st.ARM.Delete(id)
	s.mu.Unlock()
	for _, svc := range []string{"azqueue", "aztable"} {
		if d, ok := c.Hub.Service(svc).(interface{ DropAccount(string) }); ok {
			d.DropAccount(id.Name())
		}
	}
	return nil
}

func (s *Service) listKeys(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	s.mu.Lock()
	_, ok := s.st.ARM.Get(id)
	s.mu.Unlock()
	if !ok {
		return nil, arm.NotFound(id)
	}
	created := time.Now().UTC().Format("2006-01-02T15:04:05.0000000Z")
	key := func(n string) map[string]any {
		return map[string]any{"keyName": n, "value": armres.Key("storage/" + n + "/" + id.Name()), "permissions": "FULL", "creationTime": created}
	}
	return map[string]any{"keys": []any{key("key1"), key("key2")}}, nil
}

// ---- containers ----

func containerView(id arm.ID, ct *Container) map[string]any {
	p := map[string]any{"publicAccess": or(ct.Public, "None"), "metadata": map[string]string{}, "etag": ct.ETag,
		"lastModifiedTime": ct.Modified.UTC().Format("2006-01-02T15:04:05.0000000Z"), "leaseStatus": "Unlocked", "leaseState": ct.Lease.state(),
		"hasLegalHold": false, "hasImmutabilityPolicy": false, "deleted": false, "remainingRetentionDays": 0,
		"defaultEncryptionScope": "$account-encryption-key", "denyEncryptionScopeOverride": false,
		"immutableStorageWithVersioning": map[string]any{"enabled": false}}
	if ct.Meta != nil {
		p["metadata"] = ct.Meta
	}
	if ct.Lease.active() {
		p["leaseStatus"] = "Locked"
	}
	if p["leaseState"] == "available" {
		p["leaseState"] = "Available"
	}
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "etag": ct.ETag, "properties": p}
}

func (s *Service) armPutContainer(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	name, p := id.Name(), armres.Props(body)
	if len(name) < 3 || len(name) > 63 || strings.ToLower(name) != name {
		return nil, core.Errorf(http.StatusBadRequest, "InvalidResourceName", "The specified resource name contains invalid characters.")
	}
	meta := map[string]string{}
	if m, ok := p["metadata"].(map[string]any); ok {
		for k, v := range m {
			meta[k], _ = v.(string)
		}
	}
	public := armres.Str(p, "publicAccess")
	if public == "None" {
		public = ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	a := s.account(id.Parent().Parent().Name())
	ct := a.Containers[name]
	if ct == nil {
		now := time.Now().UTC()
		ct = &Container{Name: name, Created: now, Modified: now, ETag: s.etag(), Blobs: map[string]*Blob{}}
		a.Containers[name] = ct
	}
	ct.Meta, ct.Public = meta, public
	return containerView(id, ct), nil
}

func (s *Service) armGetContainer(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Accounts[id.Parent().Parent().Name()]; a != nil {
		if ct := a.Containers[id.Name()]; ct != nil {
			return containerView(id, ct), nil
		}
	}
	return nil, arm.NotFound(id)
}

func (s *Service) armDeleteContainer(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if a := s.st.Accounts[id.Parent().Parent().Name()]; a != nil {
		delete(a.Containers, id.Name())
	}
	return nil
}

func (s *Service) armListContainers(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	if a := s.st.Accounts[parent.Parent().Name()]; a != nil {
		for _, n := range core.SortedKeys(a.Containers) {
			out = append(out, containerView(parent.Child("containers", n), a.Containers[n]))
		}
	}
	return out, nil
}
