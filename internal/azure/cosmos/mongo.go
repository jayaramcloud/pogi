package cosmos

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// Cosmos DB for MongoDB: an account of kind MongoDB runs the MongoDB
// container DocumentDB uses (rds.MongoSpec). Its ARM databases and
// collections are stored like the SQL ones and also created in that MongoDB,
// so a driver sees the collections. The account name is the user and the
// primary key the password.
// ponytail: only the primary key logs in (a MongoDB user has one password),
// an empty database shows in the ARM API only (MongoDB makes a database when
// it gets a collection), and a shard key is stored, not used.

const (
	mongoDBs   = accounts + "/mongodbDatabases"
	mongoColls = mongoDBs + "/collections"
	mongoPort  = 27017
)

// mongoServer is the MongoDB container of one account.
type mongoServer struct {
	Name string     `json:"name"` // the account name
	Pogi rds.Server `json:"pogi"`
}

func (m *mongoServer) password() string { return armres.Key("cosmos/primary/" + m.Name) }

func (m *mongoServer) spec() rds.Spec { return rds.MongoSpec(m.Name, m.password(), m.Pogi.HostPort) }

func (s *Service) owns(srv *rds.Server) bool {
	for _, m := range s.st.Mongo {
		if &m.Pogi == srv {
			return true
		}
	}
	return false
}

func (s *Service) containers() map[string]bool {
	keep := map[string]bool{}
	for _, m := range s.st.Mongo {
		if m.Pogi.Container != "" {
			keep[m.Pogi.Container] = true
		}
	}
	return keep
}

func isMongo(account map[string]any) bool {
	return strings.EqualFold(armres.Str(account, "kind"), "MongoDB")
}

// startMongo gives a new MongoDB account its server. The lock is held.
func (s *Service) startMongo(id arm.ID, docker bool) {
	k := strings.ToLower(id.String())
	if s.st.Mongo[k] != nil {
		return
	}
	if s.st.Mongo == nil {
		s.st.Mongo = map[string]*mongoServer{}
	}
	m := &mongoServer{Name: id.Name()}
	s.st.Mongo[k] = m
	n := id.Name()
	if !strings.EqualFold(id.ResourceGroup, core.AzureResourceGroup) {
		n += "-" + id.ResourceGroup
	}
	s.box.Serve(&m.Pogi, core.DefaultAccount, s.hub.Cfg.Region, n, 0, mongoPort, m.spec, docker)
}

// stopMongo drops the server of a deleted account. The lock is held.
func (s *Service) stopMongo(id arm.ID) {
	k := strings.ToLower(id.String())
	if m := s.st.Mongo[k]; m != nil {
		if m.Pogi.Container != "" {
			s.box.Remove(m.Pogi.Container)
		}
		delete(s.st.Mongo, k)
	}
}

// mongosh runs a script in the account's MongoDB in the background. It retries
// for two minutes, because the server may still be starting. The script is
// an argument of the shell, never part of its text. The lock is held.
func (s *Service) mongosh(account arm.ID, script string) {
	m := s.st.Mongo[strings.ToLower(account.String())]
	if m == nil || m.Pogi.Container == "" {
		return
	}
	s.box.Exec(&m.Pogi, "sh", "-c", `for i in $(seq 120); do mongosh --quiet -u "$2" -p "$3" --authenticationDatabase admin --eval "$1" && exit; sleep 1; done; exit 1`,
		"sh", script, m.Name, m.password())
}

func js(v any) string { b, _ := json.Marshal(v); return string(b) }

// connectionStrings of a MongoDB account, once its server is up.
func (s *Service) mongoConnectionStrings(id arm.ID, m *mongoServer) (any, error) {
	if m.Pogi.Container == "" {
		return nil, core.Errorf(http.StatusBadRequest, "BadRequest", "Docker is needed: a Cosmos DB for MongoDB account runs a MongoDB container, and Docker is not available.")
	}
	deadline := time.Now().Add(2 * time.Minute)
	for {
		s.mu.Lock()
		addr, state, note := m.Pogi.Addr(), m.Pogi.State, m.Pogi.Note
		s.mu.Unlock()
		if addr != nil {
			cs := fmt.Sprintf("mongodb://%s:%s@%s:%d/?authSource=admin", url.PathEscape(m.Name), url.QueryEscape(m.password()), addr.Address, addr.Port)
			return map[string]any{"connectionStrings": []any{map[string]any{"description": "Primary MongoDB Connection String",
				"connectionString": cs, "keyKind": "Primary", "type": "Mongo"}}}, nil
		}
		if state == "failed" || time.Now().After(deadline) {
			return nil, core.Errorf(http.StatusBadRequest, "BadRequest", "The MongoDB container is not running: %s", note)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// ---- databases and collections ----

func (s *Service) registerMongo() {
	h := s.hub
	arm.Register(h, arm.Provider{Type: mongoDBs, Async: true, Created: 200, Put: s.putMongo, Get: s.getMongo, Delete: s.deleteMongo, List: s.listMongo})
	arm.Register(h, arm.Provider{Type: mongoColls, Async: true, Created: 200, Put: s.putMongo, Get: s.getMongo, Delete: s.deleteMongo, List: s.listMongo})
	for _, t := range []string{mongoDBs, mongoColls} {
		arm.Register(h, arm.Provider{Type: t + "/throughputSettings", Async: true, Created: 200, Get: s.getMongoThroughput, Put: s.putMongoThroughput})
	}
}

func accountOf(id arm.ID) arm.ID {
	for len(id.Types) > 1 {
		id = id.Parent()
	}
	return id
}

func bad(format string, a ...any) error {
	return core.Errorf(http.StatusBadRequest, "BadRequest", format, a...)
}

// checkMongoName applies MongoDB's own rules for a database or collection name.
func checkMongoName(id arm.ID) error {
	n := id.Name()
	if n == "" || strings.ContainsRune(n, 0) {
		return bad("The name of a MongoDB resource cannot be empty or hold a NUL character.")
	}
	if len(id.Types) == 2 {
		if len(n) > 63 || strings.ContainsAny(n, "/\\. \"$*<>:|?") {
			return bad("Invalid database name %q: at most 63 characters, none of /\\. \"$*<>:|?", n)
		}
	} else if strings.Contains(n, "$") || strings.HasPrefix(n, "system.") {
		return bad("Invalid collection name %q: it cannot hold $ or start with system.", n)
	}
	return nil
}

// indexes checks the indexes of a collection and returns the createIndex calls
// for the ones MongoDB lacks (not _id).
func indexes(list []any) (string, error) {
	var calls []map[string]any
	for _, x := range list {
		ix, _ := x.(map[string]any)
		key, _ := ix["key"].(map[string]any)
		fields, _ := key["keys"].([]any)
		if len(fields) == 0 {
			return "", bad("Each index needs key.keys, a list of field names.")
		}
		spec, names := map[string]any{}, []string{}
		for _, f := range fields {
			n, ok := f.(string)
			if !ok || n == "" {
				return "", bad("The field names of an index must be non-empty strings.")
			}
			spec[n], names = 1, append(names, n)
		}
		if len(names) == 1 && names[0] == "_id" {
			continue
		}
		opts, _ := ix["options"].(map[string]any)
		o := map[string]any{}
		if u, ok := opts["unique"].(bool); ok && u {
			o["unique"] = true
		}
		if t, ok := opts["expireAfterSeconds"].(float64); ok && t >= 0 {
			o["expireAfterSeconds"] = int64(t)
		}
		calls = append(calls, map[string]any{"key": spec, "opts": o})
	}
	return js(calls), nil
}

func (s *Service) putMongo(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	if err := checkMongoName(id); err != nil {
		return nil, err
	}
	p := armres.Props(body)
	in, _ := p["resource"].(map[string]any)
	coll := len(id.Types) == 3
	res := map[string]any{"id": id.Name()}
	script := ""
	if coll {
		for _, k := range []string{"shardKey", "analyticalStorageTtl"} {
			if v, ok := in[k]; ok {
				res[k] = v
			}
		}
		list, _ := in["indexes"].([]any)
		if list != nil {
			res["indexes"] = list
		}
		ix, err := indexes(list)
		if err != nil {
			return nil, err
		}
		script = fmt.Sprintf(`const d=db.getSiblingDB(%s);if(!d.getCollectionNames().includes(%[2]s))d.createCollection(%[2]s);for(const i of %[3]s)d.getCollection(%[2]s).createIndex(i.key,i.opts)`,
			js(id.Parent().Name()), js(id.Name()), ix)
	}
	opts, _ := p["options"].(map[string]any)
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.st.ARM.Get(id); ok { // keep what the service made the first time
		for _, k := range []string{"_rid", "_ts", "_etag"} {
			res[k] = armres.Props(old)["resource"].(map[string]any)[k]
		}
	} else {
		res["_rid"], res["_ts"] = armres.Key("cosmos/mongo/" + id.String())[:8]+"==", float64(time.Now().Unix())
	}
	res["_etag"] = `"` + core.UUID() + `"`
	r := armres.Envelope(id, nil)
	r["properties"] = map[string]any{"resource": res, "options": opts}
	s.st.ARM.Put(id, r)
	if script != "" {
		s.mongosh(accountOf(id), script)
	}
	s.hub.MarkDirty()
	return s.mongoView(id, r), nil
}

func (s *Service) mongoView(id arm.ID, r map[string]any) map[string]any {
	acc, _ := s.st.ARM.Get(accountOf(id))
	loc := armres.Str(acc, "location")
	if loc == "" {
		loc = "eastus"
	}
	return map[string]any{"id": id.String(), "name": id.Name(), "type": id.Type(), "location": loc, "tags": map[string]any{},
		"properties": map[string]any{"resource": armres.Props(r)["resource"]}}
}

func (s *Service) getMongo(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.st.ARM.Get(id)
	if !ok {
		return nil, arm.NotFound(id)
	}
	return s.mongoView(id, r), nil
}

func (s *Service) deleteMongo(c *core.Ctx, id arm.ID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ARM.Get(id); !ok {
		return nil
	}
	if len(id.Types) == 3 {
		s.mongosh(accountOf(id), fmt.Sprintf(`db.getSiblingDB(%s).getCollection(%s).drop()`, js(id.Parent().Name()), js(id.Name())))
	} else {
		s.mongosh(accountOf(id), fmt.Sprintf(`db.getSiblingDB(%s).dropDatabase()`, js(id.Name())))
	}
	s.st.ARM.Delete(id)
	s.hub.MarkDirty()
	return nil
}

func (s *Service) listMongo(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
	typ := mongoDBs
	if len(parent.Types) == 2 {
		typ = mongoColls
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, r := range s.st.ARM.List(parent, typ) {
		id, _ := arm.ParseID(armres.Str(r, "id"))
		out = append(out, s.mongoView(id, r))
	}
	return out, nil
}

// ---- throughput: stored with the database or collection ----

func (s *Service) getMongoThroughput(c *core.Ctx, id arm.ID) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.mongoThroughput(id)
}

func (s *Service) mongoThroughput(id arm.ID) (map[string]any, error) {
	r, ok := s.st.ARM.Get(id.Parent())
	if !ok {
		return nil, arm.NotFound(id.Parent())
	}
	opts, _ := armres.Props(r)["options"].(map[string]any)
	res := map[string]any{"minimumThroughput": "400", "offerReplacePending": "false"}
	switch a, _ := opts["autoscaleSettings"].(map[string]any); {
	case a != nil:
		res["autoscaleSettings"] = map[string]any{"maxThroughput": a["maxThroughput"]}
	case opts["throughput"] != nil:
		res["throughput"] = opts["throughput"]
	default:
		return nil, core.Errorf(http.StatusNotFound, "NotFound", "Throughput of the resource is not set; it may be shared, or on a serverless account.")
	}
	return map[string]any{"id": id.String(), "name": "default", "type": id.Type(), "properties": map[string]any{"resource": res}}, nil
}

func (s *Service) putMongoThroughput(c *core.Ctx, id arm.ID, body map[string]any) (map[string]any, error) {
	res, _ := armres.Props(body)["resource"].(map[string]any)
	opts := map[string]any{}
	if a, ok := res["autoscaleSettings"].(map[string]any); ok {
		opts["autoscaleSettings"] = a
	} else if t, ok := res["throughput"]; ok {
		if _, isNum := t.(float64); !isNum {
			return nil, bad("throughput must be a number.")
		}
		opts["throughput"] = t
	} else {
		return nil, bad("The request needs throughput or autoscaleSettings.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	r, ok := s.st.ARM.Get(id.Parent())
	if !ok {
		return nil, arm.NotFound(id.Parent())
	}
	armres.Props(r)["options"] = opts
	s.st.ARM.Put(id.Parent(), r)
	s.hub.MarkDirty()
	return s.mongoThroughput(id)
}
