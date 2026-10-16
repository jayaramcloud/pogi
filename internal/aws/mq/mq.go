// Package mq emulates Amazon MQ (REST-JSON): brokers, their users and
// configurations. With a container engine running, a RabbitMQ broker runs on
// rabbitmq:<version>-management-alpine and an ActiveMQ broker on
// apache/activemq-classic, published on 127.0.0.1; without one the API still
// answers and brokers are RUNNING as metadata only.
package mq

import (
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// doc is an API shape as JSON decodes it; brokers and configurations are kept
// in this form, member names and all, and returned as they are.
type doc = map[string]any

// Service is the Amazon MQ emulator.
type Service struct {
	hub  *core.Hub
	mux  *http.ServeMux
	ctx  context.Context // container work; ends at Close
	stop context.CancelFunc
	mu   sync.Mutex
	st   state
}

type state struct {
	Brokers map[string]*Broker        `json:"brokers"`        // account/region/broker id
	Configs map[string]*Configuration `json:"configurations"` // account/region/configuration id
}

func newState() state {
	return state{Brokers: map[string]*Broker{}, Configs: map[string]*Configuration{}}
}

// Broker is a broker as DescribeBroker shows it, less what is computed
// (state, instances, users), with its users and pogi's side.
type Broker struct {
	Doc       doc                `json:"doc"`
	Users     map[string]*User   `json:"users"`
	State     string             `json:"state"`
	Image     string             `json:"image,omitempty"`     // what its container runs
	Container string             `json:"container,omitempty"` // empty: metadata only
	Ports     map[int]int        `json:"ports,omitempty"`     // container port → host port on 127.0.0.1
	Note      string             `json:"pogiNote,omitempty"`
	cancel    context.CancelFunc // stops its container start
}

// User is a broker user. Live is what the broker runs with (nil until a
// CREATE applies); Next and Change are a change waiting for a reboot, as
// changes to ActiveMQ users do on AWS.
type User struct {
	Live   *Login `json:"live,omitempty"`
	Next   *Login `json:"next,omitempty"`
	Change string `json:"change,omitempty"` // CREATE, UPDATE or DELETE
}

// Login is how a user signs in and what it may do.
type Login struct {
	Password        string   `json:"password"`
	ConsoleAccess   bool     `json:"consoleAccess"`
	Groups          []string `json:"groups,omitempty"`
	ReplicationUser bool     `json:"replicationUser"`
}

// Configuration is a configuration as DescribeConfiguration shows it, less
// its latest revision, with every revision.
type Configuration struct {
	Doc       doc         `json:"doc"`
	Revisions []*Revision `json:"revisions"` // revision n is Revisions[n-1]
}

// Revision is one revision of a configuration.
type Revision struct {
	Created     string `json:"created"`
	Description string `json:"description,omitempty"`
	Data        string `json:"data"` // base64 of XML (ActiveMQ) or cuttlefish (RabbitMQ)
}

// New creates the Amazon MQ service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), st: newState()}
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.routes()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "mq", Cloud: core.AWS, Title: "Amazon MQ", Signing: []string{"mq"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// handler serves one operation with the lock held; in is the JSON body. A
// nil answer is 204 No Content.
type handler func(c *core.Ctx, r *http.Request, in doc) (any, error)

func (s *Service) route(pattern, op string, h handler) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		out, err := s.serve(c, r, h)
		switch {
		case err != nil:
			core.RESTJSONError(w, c, err)
		case out == nil:
			core.RESTJSON(w, c, http.StatusNoContent, nil)
		default:
			core.RESTJSON(w, c, http.StatusOK, json.RawMessage(out))
		}
	})
}

func (s *Service) serve(c *core.Ctx, r *http.Request, h handler) ([]byte, error) {
	if err := c.Intercept(); err != nil {
		return nil, err
	}
	in := doc{}
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, &in) != nil {
		return nil, bad("The request body is not valid JSON.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	out, err := h(c, r, in)
	if err != nil || out == nil {
		return nil, err
	}
	return json.Marshal(out) // under the lock: out shares the state's maps
}

func (s *Service) routes() {
	const b, cf = "/v1/brokers/{id}", "/v1/configurations/{id}"
	s.route("POST /v1/brokers", "CreateBroker", s.createBroker)
	s.route("GET /v1/brokers", "ListBrokers", s.listBrokers)
	s.route("GET "+b, "DescribeBroker", s.describeBroker)
	s.route("PUT "+b, "UpdateBroker", s.updateBroker)
	s.route("DELETE "+b, "DeleteBroker", s.deleteBroker)
	s.route("POST "+b+"/reboot", "RebootBroker", s.rebootBroker)
	s.route("GET "+b+"/shared-resources", "DescribeSharedResources", s.sharedResources)
	s.route("GET "+b+"/users", "ListUsers", s.listUsers)
	s.route("POST "+b+"/users/{user}", "CreateUser", s.createUser)
	s.route("GET "+b+"/users/{user}", "DescribeUser", s.describeUser)
	s.route("PUT "+b+"/users/{user}", "UpdateUser", s.updateUser)
	s.route("DELETE "+b+"/users/{user}", "DeleteUser", s.deleteUser)
	s.route("POST /v1/configurations", "CreateConfiguration", s.createConfiguration)
	s.route("GET /v1/configurations", "ListConfigurations", s.listConfigurations)
	s.route("GET "+cf, "DescribeConfiguration", s.describeConfiguration)
	s.route("PUT "+cf, "UpdateConfiguration", s.updateConfiguration)
	s.route("DELETE "+cf, "DeleteConfiguration", s.deleteConfiguration)
	s.route("GET "+cf+"/revisions", "ListConfigurationRevisions", s.listRevisions)
	s.route("GET "+cf+"/revisions/{rev}", "DescribeConfigurationRevision", s.describeRevision)
	s.route("GET /v1/broker-engine-types", "DescribeBrokerEngineTypes", engineTypes)
	s.route("GET /v1/broker-instance-options", "DescribeBrokerInstanceOptions", instanceOptions)
	s.route("POST /v1/tags/{arn}", "CreateTags", s.createTags)
	s.route("GET /v1/tags/{arn}", "ListTags", s.listTags)
	s.route("DELETE /v1/tags/{arn}", "DeleteTags", s.deleteTags)
	s.route("/", "Unknown", func(_ *core.Ctx, r *http.Request, _ doc) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement Amazon MQ %s %s", r.Method, r.URL.Path)
	})
}

// ---- brokers ----

var (
	nameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,50}$`)
	userRe = regexp.MustCompile(`^[A-Za-z0-9._~-]{2,100}$`)
)

func (s *Service) createBroker(c *core.Ctx, _ *http.Request, in doc) (any, error) {
	name, eng := str(in["brokerName"]), engineName(str(in["engineType"]))
	e := engines[eng]
	switch {
	case !nameRe.MatchString(name):
		return nil, bad("Broker name %q must be 1-50 letters, digits, dashes or underscores.", name)
	case e == nil:
		return nil, bad("Engine type %q is not supported; use ACTIVEMQ or RABBITMQ.", str(in["engineType"]))
	case str(in["hostInstanceType"]) == "":
		return nil, bad("The host instance type of the broker is required.")
	case !slices.Contains(e.modes, str(in["deploymentMode"])):
		return nil, bad("Deployment mode %q is not supported for %s; use one of %s.", str(in["deploymentMode"]), eng, strings.Join(e.modes, ", "))
	}
	version := cmp.Or(str(in["engineVersion"]), e.versions[0][0])
	if e.image(version) == "" {
		return nil, badVersion(eng, version)
	}
	auth, err := strategy(in["authenticationStrategy"], "simple")
	if err != nil {
		return nil, err
	}
	for k, b := range s.st.Brokers {
		if strings.HasPrefix(k, prefix(c)) && b.Doc["brokerName"] == name {
			return nil, core.Errorf(409, "ConflictException", "A broker named %s already exists.", name)
		}
	}
	users := map[string]*User{}
	for _, v := range list(in["users"]) {
		n, l := str(object(v)["username"]), login(object(v), nil)
		if users[n] != nil {
			return nil, bad("User %s is given twice.", n)
		}
		if err := checkUser(eng, n, l); err != nil {
			return nil, err
		}
		users[n] = &User{Live: l}
	}
	switch {
	case auth == "simple" && len(users) == 0:
		return nil, bad("A broker with simple authentication needs a user.")
	case eng == "RabbitMQ" && len(users) > 1:
		return nil, bad("A RabbitMQ broker takes one user; add others in the RabbitMQ web console.")
	}
	ref := object(in["configuration"])
	if ref != nil {
		if ref, err = s.configRef(c, eng, ref); err != nil {
			return nil, err
		}
	} else { // Amazon MQ makes a default configuration
		cf := s.newConfig(c, name+"-configuration", eng, version, auth, nil)
		ref = doc{"id": cf.Doc["id"], "revision": 1}
	}

	id, d := "b-"+core.UUID(), in
	for _, k := range []string{"users", "configuration", "creatorRequestId", "dataReplicationPrimaryBrokerArn"} {
		delete(d, k)
	}
	d["brokerId"], d["brokerArn"], d["brokerName"], d["created"] = id, c.ARN("mq", "broker:"+name+":"+id), name, now()
	d["engineType"], d["engineVersion"], d["authenticationStrategy"] = eng, version, auth
	d["storageType"] = strings.ToLower(cmp.Or(str(d["storageType"]), e.storage))
	d["configurations"] = doc{"current": ref, "history": []any{}}
	def(d, "autoMinorVersionUpgrade", true)
	def(d, "publiclyAccessible", false)
	def(d, "encryptionOptions", doc{"useAwsOwnedKey": true})
	def(d, "maintenanceWindowStartTime", doc{"dayOfWeek": "SUNDAY", "timeOfDay": "03:00", "timeZone": "UTC"})
	def(d, "dataReplicationMode", "NONE")
	def(d, "securityGroups", []any{})
	def(d, "subnetIds", []any{})
	def(d, "tags", doc{})
	logs := child(d, "logs")
	def(logs, "general", false)
	logs["generalLogGroup"] = "/aws/amazonmq/broker/" + id + "/general"
	if eng == "ActiveMQ" {
		def(logs, "audit", false)
		logs["auditLogGroup"] = "/aws/amazonmq/broker/" + id + "/audit"
	}
	if l := object(d["ldapServerMetadata"]); l != nil {
		delete(l, "serviceAccountPassword")
	}
	b, k := &Broker{Doc: d, Users: users, State: "CREATION_IN_PROGRESS"}, key(c, id)
	s.st.Brokers[k] = b
	if s.hub.Docker().Available() {
		b.Container = docker.Name("mq", id)
		s.boot(k, b, "run")
	} else {
		b.State, b.Note = "RUNNING", noDocker
		s.hub.Log.Warn(noDocker, "service", "mq", "broker", name)
	}
	return doc{"brokerArn": d["brokerArn"], "brokerId": id}, nil
}

// broker finds the broker the path names; lock held.
func (s *Service) broker(c *core.Ctx, r *http.Request) (*Broker, string, error) {
	k := key(c, r.PathValue("id"))
	if b := s.st.Brokers[k]; b != nil {
		return b, k, nil
	}
	return nil, "", notFound("broker", r.PathValue("id"))
}

// view is DescribeBroker's answer; lock held.
func (b *Broker) view() doc {
	out := maps.Clone(b.Doc)
	out["brokerState"], out["users"] = b.State, b.summaries()
	if b.State == "RUNNING" || b.State == "REBOOT_IN_PROGRESS" {
		out["brokerInstances"] = []doc{b.instance()}
	}
	return out
}

// instance is where the broker listens.
// ponytail: plain URLs on 127.0.0.1 (amqp://, tcp://, stomp://…) where AWS gives
// TLS ones (amqps://, ssl://…) on its host names; one instance in every deployment mode.
func (b *Broker) instance() doc {
	out, urls := doc{"ipAddress": "127.0.0.1"}, []string{}
	for _, l := range engines[str(b.Doc["engineType"])].listeners {
		u := l.scheme + "://127.0.0.1:" + strconv.Itoa(cmp.Or(b.Ports[l.port], l.port))
		if l.scheme == "http" {
			out["consoleURL"] = u
		} else {
			urls = append(urls, u)
		}
	}
	out["endpoints"] = urls
	return out
}

// summaries are the users as DescribeBroker and ListUsers show them.
func (b *Broker) summaries() []doc {
	out := []doc{}
	for _, n := range core.SortedKeys(b.Users) {
		u := doc{"username": n}
		if ch := b.Users[n].Change; ch != "" {
			u["pendingChange"] = ch
		}
		out = append(out, u)
	}
	return out
}

func (s *Service) describeBroker(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	b, _, err := s.broker(c, r)
	if err != nil {
		return nil, err
	}
	return b.view(), nil
}

// ponytail: lists return everything in one page; add nextToken when someone has thousands.
func (s *Service) listBrokers(c *core.Ctx, _ *http.Request, _ doc) (any, error) {
	out := []doc{}
	for _, k := range core.SortedKeys(s.st.Brokers) {
		if b := s.st.Brokers[k]; strings.HasPrefix(k, prefix(c)) {
			d := b.Doc
			out = append(out, doc{"brokerArn": d["brokerArn"], "brokerId": d["brokerId"], "brokerName": d["brokerName"], "brokerState": b.State,
				"created": d["created"], "deploymentMode": d["deploymentMode"], "engineType": d["engineType"], "hostInstanceType": d["hostInstanceType"]})
		}
	}
	return doc{"brokerSummaries": out}, nil
}

// pendable are the settings UpdateBroker changes at the next reboot;
// DescribeBroker shows them as pending<Setting> until then.
var pendable = []string{"engineVersion", "hostInstanceType", "authenticationStrategy", "ldapServerMetadata", "dataReplicationMode"}

func (s *Service) updateBroker(c *core.Ctx, r *http.Request, in doc) (any, error) {
	b, _, err := s.broker(c, r)
	if err != nil {
		return nil, err
	}
	d, eng := b.Doc, str(b.Doc["engineType"])
	if v, ok := in["engineVersion"]; ok && engines[eng].image(str(v)) == "" {
		return nil, badVersion(eng, str(v))
	}
	if v, ok := in["authenticationStrategy"]; ok {
		if in["authenticationStrategy"], err = strategy(v, ""); err != nil {
			return nil, err
		}
	}
	if v := object(in["configuration"]); v != nil {
		ref, err := s.configRef(c, eng, v)
		if err != nil {
			return nil, err
		}
		cfs := object(d["configurations"])
		pend(cfs, "pending", cfs["current"], ref)
	}
	if l := object(in["ldapServerMetadata"]); l != nil {
		delete(l, "serviceAccountPassword")
	}
	for _, k := range []string{"autoMinorVersionUpgrade", "maintenanceWindowStartTime", "securityGroups", "storageSize"} {
		if v, ok := in[k]; ok {
			d[k] = v
		}
	}
	for _, k := range pendable {
		if v, ok := in[k]; ok {
			pend(d, "pending"+strings.ToUpper(k[:1])+k[1:], d[k], v)
		}
	}
	if v := object(in["logs"]); v != nil {
		logs, cur := object(d["logs"]), doc{}
		for k := range v {
			cur[k] = logs[k]
		}
		pend(logs, "pending", cur, v)
	}
	out := maps.Clone(in)
	out["brokerId"] = d["brokerId"]
	return out, nil
}

// pend records v as the pending value d[p], or drops it when v is what runs (cur).
func pend(d doc, p string, cur, v any) {
	a, _ := json.Marshal(cur)
	b, _ := json.Marshal(v)
	if bytes.Equal(a, b) {
		delete(d, p)
	} else {
		d[p] = v
	}
}

// apply makes the changes waiting for a reboot current; lock held.
func (b *Broker) apply() {
	d := b.Doc
	for _, k := range pendable {
		p := "pending" + strings.ToUpper(k[:1]) + k[1:]
		if v, ok := d[p]; ok {
			d[k] = v
			delete(d, p)
		}
	}
	if cfs := object(d["configurations"]); cfs["pending"] != nil {
		cfs["history"] = append(list(cfs["history"]), cfs["current"])
		cfs["current"] = cfs["pending"]
		delete(cfs, "pending")
	}
	if logs := object(d["logs"]); logs["pending"] != nil {
		maps.Copy(logs, object(logs["pending"]))
		delete(logs, "pending")
	}
	for n, u := range b.Users {
		switch u.Change {
		case "DELETE":
			delete(b.Users, n)
		case "CREATE", "UPDATE":
			u.Live, u.Next, u.Change = u.Next, nil, ""
		}
	}
}

func (s *Service) rebootBroker(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	b, k, err := s.broker(c, r)
	if err != nil {
		return nil, err
	}
	if b.State != "RUNNING" && b.State != "CRITICAL_ACTION_REQUIRED" {
		return nil, bad("Broker %s is %s; only a running broker can reboot.", b.Doc["brokerId"], b.State)
	}
	b.apply()
	if b.Container != "" {
		verb := "restart"
		if s.image(b) != b.Image {
			verb = "run" // another engine version: a fresh container
		}
		b.State = "REBOOT_IN_PROGRESS"
		s.boot(k, b, verb)
	}
	return doc{}, nil
}

func (s *Service) deleteBroker(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	b, k, err := s.broker(c, r)
	if err != nil {
		return nil, err
	}
	delete(s.st.Brokers, k)
	s.drop(b)
	return doc{"brokerId": b.Doc["brokerId"]}, nil
}

// sharedResources answers DescribeSharedResources: pogi has no RAM shares.
func (s *Service) sharedResources(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	_, _, err := s.broker(c, r)
	return doc{"sharedResources": []any{}}, err
}

// ---- users ----

// login applies the members of a user in a request to a copy of l.
func login(in doc, l *Login) *Login {
	n := &Login{}
	if l != nil {
		*n = *l
	}
	if v, ok := in["password"].(string); ok {
		n.Password = v
	}
	if v, ok := in["consoleAccess"].(bool); ok {
		n.ConsoleAccess = v
	}
	if v, ok := in["replicationUser"].(bool); ok {
		n.ReplicationUser = v
	}
	if v, ok := in["groups"]; ok {
		n.Groups = strs(v)
	}
	return n
}

// checkUser applies Amazon MQ's rules for users. They also keep logins
// parseable in ActiveMQ's properties files (no commas, colons or equal signs).
func checkUser(eng, name string, l *Login) error {
	seen := map[rune]bool{}
	for _, r := range l.Password {
		seen[r] = true
	}
	switch {
	case !userRe.MatchString(name) || eng == "RabbitMQ" && (strings.Contains(name, "~") || name == "guest"):
		return bad("Username %q must be 2-100 letters, digits, dashes, periods, underscores or tildes (RabbitMQ: no tildes, and not guest).", name)
	case len(l.Password) < 12 || len(l.Password) > 250 || len(seen) < 4 || strings.ContainsAny(l.Password, ",:="):
		return bad("Password must be 12-250 characters long, contain at least 4 unique characters, and must not contain commas, colons, or equal signs.")
	}
	for _, g := range l.Groups {
		if !userRe.MatchString(g) {
			return bad("Group %q must be 2-100 letters, digits, dashes, periods, underscores or tildes.", g)
		}
	}
	return nil
}

// manage finds the broker whose users the path names: Amazon MQ manages the
// users of ActiveMQ brokers only (RabbitMQ keeps its own). Lock held.
func (s *Service) manage(c *core.Ctx, r *http.Request) (*Broker, error) {
	b, _, err := s.broker(c, r)
	if err == nil && b.Doc["engineType"] == "RabbitMQ" {
		err = bad("Amazon MQ does not manage the users of RabbitMQ brokers; use the RabbitMQ web console or management API.")
	}
	return b, err
}

func (s *Service) createUser(c *core.Ctx, r *http.Request, in doc) (any, error) {
	b, err := s.manage(c, r)
	if err != nil {
		return nil, err
	}
	n, l := r.PathValue("user"), login(in, nil)
	if b.Users[n] != nil {
		return nil, core.Errorf(409, "ConflictException", "User %s already exists.", n)
	}
	if err := checkUser("ActiveMQ", n, l); err != nil {
		return nil, err
	}
	b.Users[n] = &User{Next: l, Change: "CREATE"}
	return doc{}, nil
}

func (s *Service) updateUser(c *core.Ctx, r *http.Request, in doc) (any, error) {
	b, err := s.manage(c, r)
	if err != nil {
		return nil, err
	}
	n := r.PathValue("user")
	u := b.Users[n]
	if u == nil || u.Change == "DELETE" {
		return nil, notFound("user", n)
	}
	l := login(in, cmp.Or(u.Next, u.Live))
	if err := checkUser("ActiveMQ", n, l); err != nil {
		return nil, err
	}
	u.Next, u.Change = l, cmp.Or(u.Change, "UPDATE")
	return doc{}, nil
}

func (s *Service) deleteUser(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	b, err := s.manage(c, r)
	if err != nil {
		return nil, err
	}
	n := r.PathValue("user")
	switch u := b.Users[n]; {
	case u == nil:
		return nil, notFound("user", n)
	case u.Live == nil: // a CREATE that never applied
		delete(b.Users, n)
	default:
		u.Next, u.Change = nil, "DELETE"
	}
	return doc{}, nil
}

// user is DescribeUser's answer, never with the password; lock held.
func (b *Broker) user(n string) doc {
	u := b.Users[n]
	l := cmp.Or(u.Live, u.Next)
	out := doc{"brokerId": b.Doc["brokerId"], "username": n, "consoleAccess": l.ConsoleAccess, "groups": l.Groups, "replicationUser": l.ReplicationUser}
	if u.Change != "" {
		p := doc{"pendingChange": u.Change}
		if u.Next != nil {
			p["consoleAccess"], p["groups"] = u.Next.ConsoleAccess, u.Next.Groups
		}
		out["pending"] = p
	}
	return out
}

func (s *Service) describeUser(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	b, _, err := s.broker(c, r)
	if err != nil {
		return nil, err
	}
	if b.Users[r.PathValue("user")] == nil {
		return nil, notFound("user", r.PathValue("user"))
	}
	return b.user(r.PathValue("user")), nil
}

func (s *Service) listUsers(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	b, _, err := s.broker(c, r)
	if err != nil {
		return nil, err
	}
	return doc{"brokerId": b.Doc["brokerId"], "users": b.summaries()}, nil
}

// ---- configurations ----

// defaults are the data of a configuration's first revision, as Amazon MQ writes it.
var defaults = map[string]string{
	"ActiveMQ": `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<broker xmlns="http://activemq.apache.org/schema/core">
  <plugins>
    <forcePersistencyModeBrokerPlugin persistenceFlag="true"/>
    <statisticsBrokerPlugin/>
    <timeStampingBrokerPlugin ttlCeiling="86400000" zeroExpirationOverride="86400000"/>
  </plugins>
</broker>
`,
	"RabbitMQ": "# Default RabbitMQ delivery acknowledgement timeout is 30 minutes in milliseconds\nconsumer_timeout = 1800000\n",
}

// newConfig stores a configuration with its first revision; lock held.
func (s *Service) newConfig(c *core.Ctx, name, eng, version, auth string, tags doc) *Configuration {
	if tags == nil {
		tags = doc{}
	}
	id, t := "c-"+core.UUID(), now()
	cf := &Configuration{Doc: doc{"arn": c.ARN("mq", "configuration:"+id), "authenticationStrategy": auth, "created": t,
		"engineType": eng, "engineVersion": version, "id": id, "name": name, "tags": tags},
		Revisions: []*Revision{{Created: t, Description: "Auto-generated default for " + name + " on " + eng + " " + version,
			Data: base64.StdEncoding.EncodeToString([]byte(defaults[eng]))}}}
	s.st.Configs[key(c, id)] = cf
	return cf
}

// view is DescribeConfiguration's answer.
func (cf *Configuration) view() doc {
	out, n := maps.Clone(cf.Doc), len(cf.Revisions)
	out["latestRevision"] = cf.Revisions[n-1].summary(n)
	if d := cf.Revisions[n-1].Description; d != "" {
		out["description"] = d
	}
	return out
}

func (rv *Revision) summary(n int) doc {
	out := doc{"created": rv.Created, "revision": n}
	if rv.Description != "" {
		out["description"] = rv.Description
	}
	return out
}

// configRef checks the configuration a broker is to run, {id, revision}
// (the latest when it names none); lock held.
func (s *Service) configRef(c *core.Ctx, eng string, ref doc) (doc, error) {
	id := str(ref["id"])
	cf := s.st.Configs[key(c, id)]
	if cf == nil {
		return nil, bad("Configuration %s does not exist.", id)
	}
	n := len(cf.Revisions)
	if v, ok := ref["revision"].(float64); ok {
		n = int(v)
	}
	switch {
	case cf.Doc["engineType"] != eng:
		return nil, bad("Configuration %s is for %s, not %s.", id, cf.Doc["engineType"], eng)
	case n < 1 || n > len(cf.Revisions):
		return nil, bad("Configuration %s has no revision %d.", id, n)
	}
	return doc{"id": id, "revision": n}, nil
}

// config finds the configuration the path names; lock held.
func (s *Service) config(c *core.Ctx, r *http.Request) (*Configuration, error) {
	if cf := s.st.Configs[key(c, r.PathValue("id"))]; cf != nil {
		return cf, nil
	}
	return nil, notFound("configuration", r.PathValue("id"))
}

func (s *Service) createConfiguration(c *core.Ctx, _ *http.Request, in doc) (any, error) {
	name, eng := str(in["name"]), engineName(str(in["engineType"]))
	e := engines[eng]
	switch {
	case name == "":
		return nil, bad("The configuration name is required.")
	case e == nil:
		return nil, bad("Engine type %q is not supported; use ACTIVEMQ or RABBITMQ.", str(in["engineType"]))
	}
	version := cmp.Or(str(in["engineVersion"]), e.versions[0][0])
	if e.image(version) == "" {
		return nil, badVersion(eng, version)
	}
	auth, err := strategy(in["authenticationStrategy"], "simple")
	if err != nil {
		return nil, err
	}
	return s.newConfig(c, name, eng, version, auth, object(in["tags"])).view(), nil
}

func (s *Service) describeConfiguration(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	cf, err := s.config(c, r)
	if err != nil {
		return nil, err
	}
	return cf.view(), nil
}

// ponytail: the data is kept as sent, neither sanitized (AWS strips what
// ActiveMQ brokers may not set, with warnings) nor applied to the containers.
func (s *Service) updateConfiguration(c *core.Ctx, r *http.Request, in doc) (any, error) {
	cf, err := s.config(c, r)
	if err != nil {
		return nil, err
	}
	data := str(in["data"])
	if _, err := base64.StdEncoding.DecodeString(data); err != nil || data == "" {
		return nil, bad("The configuration data must be base64 encoded.")
	}
	cf.Revisions = append(cf.Revisions, &Revision{Created: now(), Description: str(in["description"]), Data: data})
	out := cf.view()
	out["warnings"] = []any{}
	return out, nil
}

func (s *Service) deleteConfiguration(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	cf, err := s.config(c, r)
	if err != nil {
		return nil, err
	}
	id := cf.Doc["id"]
	for _, b := range s.st.Brokers {
		if cfs := object(b.Doc["configurations"]); object(cfs["current"])["id"] == id || object(cfs["pending"])["id"] == id {
			return nil, core.Errorf(409, "ConflictException", "Configuration %s is in use by broker %s.", id, b.Doc["brokerId"])
		}
	}
	delete(s.st.Configs, key(c, str(id)))
	return doc{"configurationId": id}, nil
}

func (s *Service) listConfigurations(c *core.Ctx, _ *http.Request, _ doc) (any, error) {
	out := []doc{}
	for _, k := range core.SortedKeys(s.st.Configs) {
		if strings.HasPrefix(k, prefix(c)) {
			out = append(out, s.st.Configs[k].view())
		}
	}
	return doc{"configurations": out}, nil
}

func (s *Service) listRevisions(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	cf, err := s.config(c, r)
	if err != nil {
		return nil, err
	}
	out := []doc{}
	for i, rv := range cf.Revisions {
		out = append(out, rv.summary(i+1))
	}
	return doc{"configurationId": cf.Doc["id"], "revisions": out}, nil
}

func (s *Service) describeRevision(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	cf, err := s.config(c, r)
	if err != nil {
		return nil, err
	}
	n, _ := strconv.Atoi(r.PathValue("rev"))
	if n < 1 || n > len(cf.Revisions) {
		return nil, notFound("configuration revision", r.PathValue("rev"))
	}
	rv := cf.Revisions[n-1]
	out := doc{"configurationId": cf.Doc["id"], "created": rv.Created, "data": rv.Data}
	if rv.Description != "" {
		out["description"] = rv.Description
	}
	return out, nil
}

// ---- engine types, instance options, tags ----

func engineTypes(_ *core.Ctx, r *http.Request, _ doc) (any, error) {
	out := []doc{}
	for _, n := range core.SortedKeys(engines) {
		if !match(r.URL.Query().Get("engineType"), n) {
			continue
		}
		vs := []doc{}
		for _, v := range engines[n].versions {
			vs = append(vs, doc{"name": v[0]})
		}
		out = append(out, doc{"engineType": strings.ToUpper(n), "engineVersions": vs})
	}
	return doc{"brokerEngineTypes": out}, nil
}

// instanceTypes are the host instance types on offer, with their storage and
// deployment modes (DescribeBrokerInstanceOptions).
var instanceTypes = []struct {
	engine, instance, storage string
	modes                     []string
}{
	{"ActiveMQ", "mq.t3.micro", "EBS", []string{"SINGLE_INSTANCE"}},
	{"ActiveMQ", "mq.m5.large", "EFS", []string{"SINGLE_INSTANCE", "ACTIVE_STANDBY_MULTI_AZ"}},
	{"RabbitMQ", "mq.t3.micro", "EBS", []string{"SINGLE_INSTANCE"}},
	{"RabbitMQ", "mq.m7g.large", "EBS", []string{"SINGLE_INSTANCE", "CLUSTER_MULTI_AZ"}},
}

func instanceOptions(c *core.Ctx, r *http.Request, _ doc) (any, error) {
	q, out := r.URL.Query(), []doc{}
	for _, t := range instanceTypes {
		if !match(q.Get("engineType"), t.engine) || !match(q.Get("hostInstanceType"), t.instance) || !match(q.Get("storageType"), t.storage) {
			continue
		}
		var versions []string
		for _, v := range engines[t.engine].versions {
			versions = append(versions, v[0])
		}
		out = append(out, doc{"availabilityZones": []doc{{"name": c.Region + "a"}, {"name": c.Region + "b"}, {"name": c.Region + "c"}},
			"engineType": strings.ToUpper(t.engine), "hostInstanceType": t.instance, "storageType": t.storage,
			"supportedDeploymentModes": t.modes, "supportedEngineVersions": versions})
	}
	return doc{"brokerInstanceOptions": out}, nil
}

// tags finds the tags of a broker or configuration by ARN; lock held.
func (s *Service) tags(arn string) (doc, error) {
	_, region, account, res, _ := core.ParseARN(arn)
	k := account + "/" + region + "/" + res[strings.LastIndex(res, ":")+1:]
	switch kind, _, _ := strings.Cut(res, ":"); {
	case kind == "broker" && s.st.Brokers[k] != nil:
		return child(s.st.Brokers[k].Doc, "tags"), nil
	case kind == "configuration" && s.st.Configs[k] != nil:
		return child(s.st.Configs[k].Doc, "tags"), nil
	}
	return nil, notFound("resource", arn)
}

func (s *Service) createTags(_ *core.Ctx, r *http.Request, in doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	if err == nil {
		maps.Copy(t, object(in["tags"]))
	}
	return nil, err
}

func (s *Service) deleteTags(_ *core.Ctx, r *http.Request, _ doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(t, k)
	}
	return nil, err
}

func (s *Service) listTags(_ *core.Ctx, r *http.Request, _ doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	return doc{"tags": t}, nil
}

// ---- persistence, console ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state. Brokers that ran in containers start them again
// (persisted state leaves them running), or run as metadata when the
// engine is gone.
func (s *Service) Load(data []byte) error {
	st := newState()
	if err := json.Unmarshal(data, &st); err != nil {
		return err
	}
	engine := false
	for _, b := range st.Brokers {
		if b.Container != "" {
			engine = s.hub.Docker().Available()
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.st
	s.st = st
	for k, b := range st.Brokers {
		switch {
		case b.Container == "":
		case engine:
			s.boot(k, b, "start")
		default:
			b.State, b.Note = "RUNNING", noDocker
		}
	}
	for _, b := range old.Brokers {
		s.drop(b)
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Resources lists brokers and configurations for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	bt := core.Table{Name: "Brokers", Columns: []string{"Name", "ID", "Engine", "State", "Endpoint", "Console", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Brokers) {
		b, p := s.st.Brokers[k], strings.SplitN(k, "/", 3)
		in := b.instance()
		bt.Rows = append(bt.Rows, []string{str(b.Doc["brokerName"]), str(b.Doc["brokerId"]), str(b.Doc["engineType"]) + " " + str(b.Doc["engineVersion"]),
			b.State, in["endpoints"].([]string)[0], str(in["consoleURL"]), p[1], p[0]})
	}
	ct := core.Table{Name: "Configurations", Columns: []string{"ID", "Name", "Engine", "Revision", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Configs) {
		cf, p := s.st.Configs[k], strings.SplitN(k, "/", 3)
		ct.Rows = append(ct.Rows, []string{str(cf.Doc["id"]), str(cf.Doc["name"]), str(cf.Doc["engineType"]) + " " + str(cf.Doc["engineVersion"]),
			strconv.Itoa(len(cf.Revisions)), p[1], p[0]})
	}
	return []core.Table{bt, ct}
}

// Inspect returns a broker (by name or id) with its users, never their
// passwords, and pogi's side: container, image and pogiNote; or a
// configuration with its revisions.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out doc
	if table == "Configurations" {
		for _, cf := range s.st.Configs {
			if cf.Doc["id"] == id {
				out = doc{"configuration": cf.view(), "revisions": cf.Revisions}
			}
		}
	} else {
		for _, b := range s.st.Brokers {
			if b.Doc["brokerName"] == id || b.Doc["brokerId"] == id {
				users := doc{}
				for n := range b.Users {
					users[n] = b.user(n)
				}
				out = doc{"broker": b.view(), "users": users, "container": b.Container, "image": b.Image, "pogiNote": b.Note}
			}
		}
	}
	if out == nil {
		return nil
	}
	j, _ := json.Marshal(out) // under the lock: out shares the state's maps
	return json.RawMessage(j)
}

// ---- helpers ----

func key(c *core.Ctx, id string) string { return prefix(c) + id }
func prefix(c *core.Ctx) string         { return c.Account + "/" + c.Region + "/" }
func now() string                       { return time.Now().UTC().Format(core.ISO) }

// strategy reads an authentication strategy in any case; AWS answers in lower case.
func strategy(v any, def string) (string, error) {
	a := strings.ToLower(cmp.Or(str(v), def))
	if !slices.Contains([]string{"simple", "ldap", "config_managed"}, a) {
		return "", bad("Authentication strategy %q is not supported; use SIMPLE, LDAP or CONFIG_MANAGED.", str(v))
	}
	return a, nil
}

// match is an optional filter that ignores case.
func match(want, v string) bool { return want == "" || strings.EqualFold(want, v) }

func bad(format string, a ...any) error {
	return core.Errorf(400, "BadRequestException", format, a...)
}

func badVersion(eng, v string) error {
	return bad("Broker engine version %s is not supported for %s.", v, eng)
}

func notFound(kind, id string) error {
	return core.Errorf(404, "NotFoundException", "Can't find requested %s [%s]. Make sure your %s exists.", kind, id, kind)
}

func str(v any) string { s, _ := v.(string); return s }
func list(v any) []any { l, _ := v.([]any); return l }
func object(v any) doc { m, _ := v.(doc); return m }
func def(d doc, k string, v any) {
	if _, ok := d[k]; !ok {
		d[k] = v
	}
}

func strs(v any) []string {
	var out []string
	for _, x := range list(v) {
		out = append(out, str(x))
	}
	return out
}

// child is d[k] as an object, made when missing.
func child(d doc, k string) doc {
	m := object(d[k])
	if m == nil {
		m = doc{}
		d[k] = m
	}
	return m
}
