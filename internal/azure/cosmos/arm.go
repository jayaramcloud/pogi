package cosmos

import (
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Resource-manager side: database accounts, their SQL databases and
// containers, and throughput settings. The data plane has no accounts:
// databases are global, so two accounts share them.
// ponytail: databases are not scoped to an account; add the account to the
// database key when two accounts need the same names.

const (
	accounts   = "Microsoft.DocumentDB/databaseAccounts"
	databases  = accounts + "/sqlDatabases"
	containers = databases + "/containers"
)

func (s *Service) registerARM() {
	h := s.hub
	arm.Register(h, arm.Provider{Type: accounts, Async: true, Created: 200, Put: s.putAccount, Get: s.getAccount, Delete: s.deleteAccount, List: s.listAccounts,
		Actions: map[string]func(*core.Ctx, arm.ID, map[string]any) (any, error){
			"listKeys": s.keys(true), "readonlykeys": s.keys(false), "listReadOnlyKeys": s.keys(false), "listConnectionStrings": s.connectionStrings,
		}})
	arm.Register(h, arm.Provider{Type: databases, Async: true, Created: 200, Put: s.putResource, Get: s.getResource, Delete: s.deleteResource, List: s.listResources})
	arm.Register(h, arm.Provider{Type: containers, Async: true, Created: 200, Put: s.putResource, Get: s.getResource, Delete: s.deleteResource, List: s.listResources})
	for _, t := range []string{databases, containers} {
		arm.Register(h, arm.Provider{Type: t + "/throughputSettings", Get: s.getThroughput, Put: s.putThroughput})
	}
}

// ---- accounts ----

func (s *Service) accountView(c *core.Ctx, r map[string]any) map[string]any {
	p, loc, name := armres.Props(r), armres.Str(r, "location"), armres.Str(r, "name")
	ep := armres.Base(c) + "/"
	armres.Defaults(r, map[string]any{"kind": "GlobalDocumentDB"})
	armres.Defaults(p, map[string]any{
		"databaseAccountOfferType": "Standard", "enableAutomaticFailover": false, "enableMultipleWriteLocations": false,
		"isVirtualNetworkFilterEnabled": false, "virtualNetworkRules": []any{}, "ipRules": []any{}, "publicNetworkAccess": "Enabled",
		"disableKeyBasedMetadataWriteAccess": false, "disableLocalAuth": false, "enableFreeTier": false, "enableAnalyticalStorage": false,
		"enableBurstCapacity": false, "enablePartitionMerge": false, "minimalTlsVersion": "Tls12", "networkAclBypass": "None",
		"networkAclBypassResourceIds": []any{}, "capabilities": []any{}, "cors": []any{}, "capacity": map[string]any{"totalThroughputLimit": -1},
		"consistencyPolicy": map[string]any{"defaultConsistencyLevel": "Session", "maxIntervalInSeconds": 5, "maxStalenessPrefix": 100},
		"backupPolicy": map[string]any{"type": "Periodic", "periodicModeProperties": map[string]any{
			"backupIntervalInMinutes": 240, "backupRetentionIntervalInHours": 8, "backupStorageRedundancy": "Geo"}},
		"analyticalStorageConfiguration": map[string]any{"schemaType": "WellDefined"},
		"defaultIdentity":                "FirstPartyIdentity",
		"createMode":                     "Default",
	})
	// the locations the request lists (geoLocations in some API versions), as the service returns them
	var locs []any
	req, _ := p["locations"].([]any)
	if req == nil {
		req, _ = p["geoLocations"].([]any)
	}
	for i, g := range req {
		gm, _ := g.(map[string]any)
		locs = append(locs, location(name, armres.Str(gm, "locationName"), ep, i, gm["isZoneRedundant"] == true))
	}
	if len(locs) == 0 {
		locs = []any{location(name, loc, ep, 0, false)}
	}
	p["locations"], p["readLocations"], p["writeLocations"] = locs, locs, locs[:1]
	var failover []any
	for i, l := range locs {
		lm := l.(map[string]any)
		failover = append(failover, map[string]any{"id": lm["id"], "locationName": lm["locationName"], "failoverPriority": i})
	}
	p["failoverPolicies"] = failover
	delete(p, "geoLocations")
	p["provisioningState"], p["documentEndpoint"] = "Succeeded", ep
	p["instanceId"] = armres.Key("cosmos/instance/" + name)[:32]
	p["keysMetadata"] = map[string]any{"primaryMasterKey": map[string]any{"generationTime": "2020-01-01T00:00:00Z"},
		"secondaryMasterKey":         map[string]any{"generationTime": "2020-01-01T00:00:00Z"},
		"primaryReadonlyMasterKey":   map[string]any{"generationTime": "2020-01-01T00:00:00Z"},
		"secondaryReadonlyMasterKey": map[string]any{"generationTime": "2020-01-01T00:00:00Z"}}
	return r
}

func location(account, name, ep string, priority int, zones bool) map[string]any {
	n := strings.ToLower(strings.ReplaceAll(name, " ", ""))
	return map[string]any{"id": account + "-" + n, "locationName": name, "documentEndpoint": strings.TrimSuffix(ep, "/") + "/",
		"provisioningState": "Succeeded", "failoverPriority": priority, "isZoneRedundant": zones}
}

func (s *Service) putAccount(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	r := armres.Envelope(id, body)
	s.mu.Lock()
	defer s.mu.Unlock()
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

// deleteAccount removes the account's own record; the databases stay (they are not its).
func (s *Service) deleteAccount(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.st.ARM.Delete(id)
	return nil
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

func (s *Service) known(id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ARM.Get(id); !ok {
		return arm.NotFound(id)
	}
	return nil
}

func (s *Service) keys(all bool) func(*core.Ctx, arm.ID, map[string]any) (any, error) {
	return func(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
		if err := s.known(id); err != nil {
			return nil, err
		}
		k := func(n string) string { return armres.Key("cosmos/" + n + "/" + id.Name()) }
		out := map[string]any{"primaryReadonlyMasterKey": k("primaryReadonly"), "secondaryReadonlyMasterKey": k("secondaryReadonly")}
		if all {
			out["primaryMasterKey"], out["secondaryMasterKey"] = k("primary"), k("secondary")
		}
		return out, nil
	}
}

func (s *Service) connectionStrings(c *core.Ctx, id arm.ID, _ map[string]any) (any, error) {
	if err := s.known(id); err != nil {
		return nil, err
	}
	conn := func(desc, key string) map[string]any {
		return map[string]any{"description": desc, "connectionString": fmt.Sprintf("AccountEndpoint=%s/;AccountKey=%s;", armres.Base(c), armres.Key("cosmos/"+key+"/"+id.Name()))}
	}
	return map[string]any{"connectionStrings": []any{
		conn("Primary SQL Connection String", "primary"), conn("Secondary SQL Connection String", "secondary"),
		conn("Primary Read-Only SQL Connection String", "primaryReadonly"), conn("Secondary Read-Only SQL Connection String", "secondaryReadonly")}}, nil
}

// ---- databases and containers ----

func (s *Service) putResource(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	p := armres.Props(body)
	res, _ := p["resource"].(map[string]any)
	opts, _ := p["options"].(map[string]any)
	in := armres.Clone(res)
	if in == nil {
		in = map[string]any{}
	}
	in["id"] = id.Name()
	path, isContainer := "/dbs", len(id.Types) == 3
	if isContainer {
		path += "/" + id.Parent().Name() + "/colls"
		if in["partitionKey"] == nil {
			return nil, core.Errorf(http.StatusBadRequest, "BadRequest", "The input container resource must have a partitionKey.")
		}
	}
	var hdr []string
	if t, ok := opts["throughput"].(float64); ok {
		hdr = []string{"x-ms-offer-throughput", strconv.Itoa(int(t))}
	} else if a, ok := opts["autoscaleSettings"].(map[string]any); ok {
		b, _ := json.Marshal(a)
		hdr = []string{"x-ms-cosmos-offer-autopilot-settings", string(b)}
	}
	if status, resp := armres.Send(c.Hub, "cosmos", http.MethodPost, "", path, in, hdr...); status >= 300 && status != http.StatusConflict {
		return nil, core.Errorf(status, "BadRequest", "%s", resp)
	}
	return s.getResource(c, id)
}

// resource is the data-plane properties of a database or container.
func (s *Service) resource(id arm.ID) map[string]any {
	if len(id.Types) == 3 {
		if db := s.st.DBs[id.Parent().Name()]; db != nil && db.Colls[id.Name()] != nil {
			return db.Colls[id.Name()].Props
		}
		return nil
	}
	if db := s.st.DBs[id.Name()]; db != nil {
		return db.Props
	}
	return nil
}

func (s *Service) getResource(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	props := s.resource(id)
	if props == nil {
		return nil, arm.NotFound(id)
	}
	res := map[string]any{}
	keep := []string{"id", "_rid", "_ts", "_etag"}
	if len(id.Types) == 3 {
		keep = append(keep, "indexingPolicy", "partitionKey", "defaultTtl", "uniqueKeyPolicy", "conflictResolutionPolicy", "analyticalStorageTtl")
	}
	for _, k := range keep {
		if v, ok := props[k]; ok {
			res[k] = v
		}
	}
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": "eastus", "tags": map[string]any{},
		"properties": map[string]any{"resource": res}}, nil
}

func (s *Service) deleteResource(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(id.Types) == 3 {
		if db := s.st.DBs[id.Parent().Name()]; db != nil && db.Colls[id.Name()] != nil {
			s.dropOffers(db.Colls[id.Name()].Props["_rid"])
			delete(db.Colls, id.Name())
		}
		return nil
	}
	if db := s.st.DBs[id.Name()]; db != nil {
		s.dropOffers(db.Props["_rid"])
		for _, ct := range db.Colls {
			s.dropOffers(ct.Props["_rid"])
		}
		delete(s.st.DBs, id.Name())
	}
	return nil
}

func (s *Service) listResources(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	s.mu.Lock()
	var ids []arm.ID
	if len(parent.Types) == 2 { // a database: its containers
		if db := s.st.DBs[parent.Name()]; db != nil {
			for _, n := range core.SortedKeys(db.Colls) {
				ids = append(ids, parent.Child("containers", n))
			}
		}
	} else {
		for _, n := range core.SortedKeys(s.st.DBs) {
			ids = append(ids, parent.Child("sqlDatabases", n))
		}
	}
	s.mu.Unlock()
	var out []map[string]any
	for _, id := range ids {
		if r, err := s.getResource(c, id); err == nil {
			out = append(out, r)
		}
	}
	return out, nil
}

// ---- throughput ----

// offer finds the offer of the database or container at id; the lock is held.
func (s *Service) offer(id arm.ID) (map[string]any, map[string]any) {
	res := s.resource(id)
	if res == nil {
		return nil, nil
	}
	for _, o := range s.st.Offers {
		if o["offerResourceId"] == res["_rid"] {
			return o, res
		}
	}
	return nil, res
}

func (s *Service) getThroughput(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, _ := s.offer(id.Parent())
	if o == nil {
		return nil, core.Errorf(http.StatusNotFound, "NotFound", "Throughput of the resource is not set; it may be shared, or on a serverless account.")
	}
	content, _ := o["content"].(map[string]any)
	res := map[string]any{"throughput": content["offerThroughput"], "minimumThroughput": "400", "offerReplacePending": "false"}
	if a, ok := content["offerAutopilotSettings"].(map[string]any); ok {
		res["autoscaleSettings"] = map[string]any{"maxThroughput": a["maxThroughput"]}
		delete(res, "throughput")
	}
	return map[string]any{"id": id.String(), "name": "default", "type": id.Type(), "properties": map[string]any{"resource": res}}, nil
}

func (s *Service) putThroughput(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	res, _ := armres.Props(body)["resource"].(map[string]any)
	s.mu.Lock()
	o, target := s.offer(id.Parent())
	if target == nil {
		s.mu.Unlock()
		return nil, arm.NotFound(id.Parent())
	}
	content := map[string]any{"offerThroughput": res["throughput"], "offerIsRUPerMinuteThroughputEnabled": false}
	if a, ok := res["autoscaleSettings"].(map[string]any); ok {
		top, _ := a["maxThroughput"].(float64)
		content["offerThroughput"], content["offerAutopilotSettings"] = int(top)/10, a
	}
	if o == nil {
		oid := core.RandString(4, core.AlphaNum)
		o = map[string]any{"id": oid, "offerVersion": "V2", "offerType": "Invalid", "resource": target["_self"], "offerResourceId": target["_rid"]}
		stamp(o, oid, "offers/"+oid+"/")
	} else {
		o = maps.Clone(o) // stored offers are replaced, never changed in place
	}
	o["content"], o["_ts"] = content, float64(time.Now().Unix())
	s.st.Offers[o["id"].(string)] = o
	s.mu.Unlock()
	return s.getThroughput(c, id)
}
