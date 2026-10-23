// Package kafka emulates Amazon MSK (REST-JSON). With a container engine
// every cluster, provisioned or serverless, is a real Kafka API: one Redpanda
// broker in a container, published on 127.0.0.1. Without one the API still
// answers, as metadata. Configurations, cluster operations, VPC connections
// and SCRAM secret associations are metadata.
package kafka

import (
	"context"
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

// noDocker is the note of a cluster created while Docker was not available.
const noDocker = "Docker is not available: the API works, but no Kafka broker runs behind the bootstrap brokers"

// versions are the Kafka versions on offer, oldest first.
// ponytail: MSK's current versions, all ACTIVE; a cluster may still name any other.
var versions = []string{"2.8.1", "3.3.2", "3.4.0", "3.5.1", "3.6.0", "3.7.x", "3.7.x.kraft", "3.8.x", "3.9.x", "3.9.x.kraft", "4.0.x.kraft", "4.1.x.kraft"}

// doc is an API shape as JSON decodes it; resources are kept in this form,
// member names and all, and returned as they are.
type doc = map[string]any

// Service is the MSK emulator.
type Service struct {
	hub  *core.Hub
	mux  *http.ServeMux
	ctx  context.Context // broker starts; ends at Close
	stop context.CancelFunc
	mu   sync.Mutex
	st   state
}

type state struct {
	Clusters map[string]*cluster `json:"clusters"`       // ARN
	Configs  map[string]*config  `json:"configurations"` // ARN
	VPCs     map[string]doc      `json:"vpcConnections"` // ARN → DescribeVpcConnection's shape
}

func newState() state {
	return state{Clusters: map[string]*cluster{}, Configs: map[string]*config{}, VPCs: map[string]doc{}}
}

// cluster is a cluster as DescribeCluster shows it, plus its clusterType (and
// for a serverless one the members of Serverless instead), its operations and
// pogi's side.
type cluster struct {
	Doc       doc                `json:"doc"`
	Ops       []doc              `json:"operations,omitempty"` // ClusterOperationInfo, oldest first
	Secrets   []string           `json:"scramSecrets,omitempty"`
	Container string             `json:"container,omitempty"` // Redpanda; empty: metadata only
	Bootstrap string             `json:"bootstrap,omitempty"` // 127.0.0.1:<port> of its Kafka API
	Note      string             `json:"pogiNote,omitempty"`
	cancel    context.CancelFunc // stops its broker start
}

// config is a configuration as DescribeConfiguration shows it, but for
// latestRevision, and its revisions (DescribeConfigurationRevision's shape
// without arn), oldest first.
type config struct {
	Doc       doc   `json:"doc"`
	Revisions []doc `json:"revisions"`
}

// New creates the MSK service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), st: newState()}
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.routes()
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "kafka", Cloud: core.AWS, Title: "MSK", Signing: []string{"kafka"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// handler serves one operation with the lock held: cl is the cluster the path
// names (nil when it names none) and in the JSON body. A nil result is 204.
type handler func(c *core.Ctx, r *http.Request, cl *cluster, in doc) (any, error)

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
	if b := core.Body(r); len(b) > 0 && (json.Unmarshal(b, &in) != nil || in == nil) {
		return nil, invalid("The request body is not a JSON object")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var cl *cluster
	if arn := r.PathValue("cluster"); arn != "" {
		if cl = s.st.Clusters[arn]; cl == nil {
			return nil, notFound("Cluster %s does not exist.", arn)
		}
	}
	out, err := h(c, r, cl, in)
	if err != nil || out == nil {
		return nil, err
	}
	return json.Marshal(out) // under the lock: out shares the state's maps
}

func (s *Service) routes() {
	const cl = "/v1/clusters/{cluster}"
	s.route("POST /v1/clusters", "CreateCluster", s.createCluster)
	s.route("POST /api/v2/clusters", "CreateClusterV2", s.createCluster)
	s.route("GET /v1/clusters", "ListClusters", s.listClusters)
	s.route("GET /api/v2/clusters", "ListClustersV2", s.listClusters)
	s.route("GET "+cl, "DescribeCluster", provisioned(describeCluster))
	s.route("GET /api/v2/clusters/{cluster}", "DescribeClusterV2", describeClusterV2)
	s.route("DELETE "+cl, "DeleteCluster", s.deleteCluster)
	s.route("GET "+cl+"/bootstrap-brokers", "GetBootstrapBrokers", bootstrapBrokers)
	s.route("GET "+cl+"/nodes", "ListNodes", provisioned(listNodes))
	s.route("PUT "+cl+"/nodes/count", "UpdateBrokerCount", provisioned(updateBrokerCount))
	s.route("PUT "+cl+"/nodes/storage", "UpdateBrokerStorage", provisioned(updateBrokerStorage))
	s.route("PUT "+cl+"/nodes/type", "UpdateBrokerType", provisioned(updateBrokerType))
	s.route("PUT "+cl+"/configuration", "UpdateClusterConfiguration", provisioned(s.updateClusterConfiguration))
	s.route("PUT "+cl+"/version", "UpdateClusterKafkaVersion", provisioned(s.updateClusterKafkaVersion))
	s.route("GET "+cl+"/operations", "ListClusterOperations", listOperations)
	s.route("GET /v1/operations/{op}", "DescribeClusterOperation", s.describeOperation)
	s.route("POST "+cl+"/scram-secrets", "BatchAssociateScramSecret", scramSecrets(true))
	s.route("PATCH "+cl+"/scram-secrets", "BatchDisassociateScramSecret", scramSecrets(false))
	s.route("GET "+cl+"/scram-secrets", "ListScramSecrets", listScramSecrets)
	s.route("POST /v1/configurations", "CreateConfiguration", s.createConfiguration)
	s.route("GET /v1/configurations", "ListConfigurations", s.listConfigurations)
	s.route("GET /v1/configurations/{arn}", "DescribeConfiguration", s.describeConfiguration)
	s.route("PUT /v1/configurations/{arn}", "UpdateConfiguration", s.updateConfiguration)
	s.route("DELETE /v1/configurations/{arn}", "DeleteConfiguration", s.deleteConfiguration)
	s.route("GET /v1/configurations/{arn}/revisions", "ListConfigurationRevisions", s.listRevisions)
	s.route("GET /v1/configurations/{arn}/revisions/{rev}", "DescribeConfigurationRevision", s.describeRevision)
	s.route("POST /v1/vpc-connection", "CreateVpcConnection", s.createVpcConnection)
	s.route("GET /v1/vpc-connection/{arn}", "DescribeVpcConnection", s.describeVpcConnection)
	s.route("DELETE /v1/vpc-connection/{arn}", "DeleteVpcConnection", s.deleteVpcConnection)
	s.route("GET /v1/vpc-connections", "ListVpcConnections", s.listVpcConnections)
	s.route("GET /v1/kafka-versions", "ListKafkaVersions", listVersions)
	s.route("GET /v1/compatible-kafka-versions", "GetCompatibleKafkaVersions", s.compatibleVersions)
	s.route("GET /v1/tags/{arn}", "ListTagsForResource", s.listTags)
	s.route("POST /v1/tags/{arn}", "TagResource", s.tagResource)
	s.route("DELETE /v1/tags/{arn}", "UntagResource", s.untagResource)
	s.route("/", "Unknown", func(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement MSK %s %s", r.Method, r.URL.Path)
	})
}

// ---- clusters ----

var nameRe = regexp.MustCompile(`^[0-9A-Za-z][0-9A-Za-z-]{0,63}$`)

// createCluster serves CreateCluster, whose body holds the members of a
// provisioned cluster, and CreateClusterV2, which nests them under
// provisioned or serverless.
func (s *Service) createCluster(c *core.Ctx, r *http.Request, _ *cluster, in doc) (any, error) {
	name, typ, d, v2 := str(in["clusterName"]), "PROVISIONED", in, strings.HasPrefix(r.URL.Path, "/api/v2/")
	if v2 {
		if d = object(in["provisioned"]); in["serverless"] != nil {
			typ, d = "SERVERLESS", object(in["serverless"])
		}
	}
	bn := object(d["brokerNodeGroupInfo"])
	switch subnets := len(list(bn["clientSubnets"])); {
	case !nameRe.MatchString(name):
		return nil, invalid("The cluster name %q must be 1 to 64 letters, digits and hyphens, starting with a letter or digit.", name)
	case d == nil:
		return nil, invalid("Specify provisioned or serverless.")
	case typ == "SERVERLESS":
		if len(list(d["vpcConfigs"])) == 0 {
			return nil, invalid("A serverless cluster needs vpcConfigs.")
		}
	case str(d["kafkaVersion"]) == "" || str(bn["instanceType"]) == "" || subnets == 0:
		return nil, invalid("kafkaVersion and brokerNodeGroupInfo with instanceType and clientSubnets are required.")
	case num(d["numberOfBrokerNodes"]) < 1 || num(d["numberOfBrokerNodes"])%subnets != 0:
		return nil, invalid("The number of broker nodes must be a multiple of Availability Zones in the Client Subnets parameter.")
	}
	ci := object(d["configurationInfo"])
	if ci != nil {
		if err := s.revision(ci); err != nil {
			return nil, err
		}
	}
	prefix := c.ARN("kafka", "cluster/"+name+"/")
	for arn := range s.st.Clusters {
		if strings.HasPrefix(arn, prefix) {
			return nil, core.Errorf(409, "ConflictException", "A cluster named %s already exists.", name)
		}
	}
	id := core.UUID()
	arn := prefix + id
	d["clusterArn"], d["clusterName"], d["clusterType"], d["creationTime"], d["currentVersion"], d["tags"] = arn, name, typ, now(), version(), in["tags"]
	child(d, "tags")
	if typ == "PROVISIONED" {
		v := str(d["kafkaVersion"])
		sw := doc{"kafkaVersion": v}
		if ci != nil {
			sw["configurationArn"], sw["configurationRevision"] = ci["arn"], ci["revision"]
		}
		d["currentBrokerSoftwareInfo"] = sw
		delete(d, "kafkaVersion")
		delete(d, "configurationInfo")
		def(bn, "brokerAZDistribution", "DEFAULT")
		def(bn, "storageInfo", doc{"ebsStorageInfo": doc{"volumeSize": 1000}})
		enc := child(d, "encryptionInfo")
		def(child(enc, "encryptionAtRest"), "dataVolumeKMSKeyId", c.ARN("kms", "key/"+core.UUID()))
		def(child(enc, "encryptionInTransit"), "clientBroker", "TLS")
		def(child(enc, "encryptionInTransit"), "inCluster", true)
		def(d, "enhancedMonitoring", "DEFAULT")
		def(d, "storageMode", "LOCAL")
		def(d, "customerActionStatus", "NONE")
		if !strings.HasSuffix(v, ".kraft") {
			// ponytail: Redpanda has no ZooKeeper; these name a port where nothing listens
			d["zookeeperConnectString"], d["zookeeperConnectStringTls"] = "127.0.0.1:2181", "127.0.0.1:2182"
		}
	} else {
		def(d, "clientAuthentication", doc{"sasl": doc{"iam": doc{"enabled": true}}})
	}
	cl := &cluster{Doc: d}
	s.st.Clusters[arn] = cl
	if s.hub.Docker().Available() {
		cl.Container = docker.Name("kafka", name[:min(len(name), 40)]+"-"+id[:8]) // unique, and short enough for a DNS name
		s.boot(cl)
	} else {
		d["state"], cl.Bootstrap, cl.Note = "ACTIVE", "127.0.0.1:"+strconv.Itoa(freePort(0)), noDocker
		s.hub.Log.Warn(noDocker, "service", "kafka", "cluster", name)
	}
	out := doc{"clusterArn": arn, "clusterName": name, "state": "CREATING"}
	if v2 {
		out["clusterType"] = typ
	}
	return out, nil
}

// boot starts the broker of a cluster in the background, the container it
// has or a fresh one, and makes the cluster ACTIVE once the broker answers.
// Lock held.
func (s *Service) boot(cl *cluster) {
	ctx, cancel := context.WithCancel(s.ctx)
	cl.cancel = cancel
	cl.Doc["state"] = "CREATING"
	arn := str(cl.Doc["clusterArn"])
	port, _ := strconv.Atoi(cl.Bootstrap[strings.LastIndexByte(cl.Bootstrap, ':')+1:]) // kept when the container must be made again
	rp := &Redpanda{Name: cl.Container, Service: "kafka", Resource: str(cl.Doc["clusterName"]), Port: port}
	core.Go("kafka.boot", func() {
		defer cancel()
		addr, err := rp.Run(ctx, s.hub.Docker())
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case s.st.Clusters[arn] != cl: // deleted meanwhile
			s.drop(cl)
		case err != nil:
			cl.Doc["state"], cl.Doc["stateInfo"], cl.Note = "FAILED", doc{"code": "InternalFailure", "message": err.Error()}, docker.Plain(err)
			s.hub.Log.Warn("kafka cluster failed", "cluster", arn, "err", err)
		default:
			cl.Doc["state"], cl.Bootstrap, cl.Note = "ACTIVE", addr, ""
			delete(cl.Doc, "stateInfo")
			s.hub.SetEndpoint("kafka:"+arn, rp.Name+":9092") // for containers on pogi's network
			s.hub.SetEndpoint("kafka-host:"+arn, addr)       // for pogi itself (Lambda event source mappings)
		}
	})
}

// drop stops the broker start of a cluster that is gone and removes its
// container, unless a current cluster runs in it (the same state, loaded
// again). Lock held.
func (s *Service) drop(cl *cluster) {
	if cl.cancel != nil {
		cl.cancel()
	}
	for _, x := range s.st.Clusters {
		if x.Container == cl.Container {
			return
		}
	}
	if cl.Container != "" {
		s.hub.SetEndpoint("kafka:"+str(cl.Doc["clusterArn"]), "")
		s.hub.SetEndpoint("kafka-host:"+str(cl.Doc["clusterArn"]), "")
		go s.hub.Docker().Remove(context.Background(), cl.Container)
	}
}

// provisioned refuses serverless clusters, as the operations of MSK's first API do.
func provisioned(h handler) handler {
	return func(c *core.Ctx, r *http.Request, cl *cluster, in doc) (any, error) {
		if cl.Doc["clusterType"] == "SERVERLESS" {
			return nil, invalid("This operation cannot be performed on serverless clusters.")
		}
		return h(c, r, cl, in)
	}
}

func describeCluster(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	return doc{"clusterInfo": cl.Doc}, nil
}

func describeClusterV2(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	return doc{"clusterInfo": v2(cl.Doc)}, nil
}

// common are the members a v2 cluster has outside provisioned or serverless.
var common = []string{"activeOperationArn", "clusterArn", "clusterName", "clusterType", "creationTime", "currentVersion", "state", "stateInfo", "tags"}

// v2 is a cluster as the v2 API shows it: the members of its type under
// provisioned or serverless.
func v2(d doc) doc {
	out, typed := doc{}, doc{}
	for k, v := range d {
		if slices.Contains(common, k) {
			out[k] = v
		} else {
			typed[k] = v
		}
	}
	out[strings.ToLower(str(d["clusterType"]))] = typed
	return out
}

// ponytail: lists return everything in one page; add nextToken when someone has thousands.
func (s *Service) listClusters(c *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	q, isV2 := r.URL.Query(), strings.HasPrefix(r.URL.Path, "/api/v2/")
	out := []any{}
	for _, arn := range core.SortedKeys(s.st.Clusters) {
		d := s.st.Clusters[arn].Doc
		switch typ := str(d["clusterType"]); {
		case !strings.HasPrefix(arn, c.ARN("kafka", "cluster/"+q.Get("clusterNameFilter"))):
		case isV2 && (q.Get("clusterTypeFilter") == "" || q.Get("clusterTypeFilter") == typ):
			out = append(out, v2(d))
		case !isV2 && typ == "PROVISIONED":
			out = append(out, d)
		}
	}
	return doc{"clusterInfoList": out}, nil
}

func (s *Service) deleteCluster(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
	if v := r.URL.Query().Get("currentVersion"); v != "" && v != cl.Doc["currentVersion"] {
		return nil, invalid("The provided current version %s does not match the actual current version %s.", v, cl.Doc["currentVersion"])
	}
	arn := str(cl.Doc["clusterArn"])
	delete(s.st.Clusters, arn)
	s.drop(cl)
	return doc{"clusterArn": arn, "state": "DELETING"}, nil
}

// ponytail: the broker has one plaintext listener; the TLS, SASL/SCRAM and IAM
// strings name it too, so clients must connect without TLS or SASL.
func bootstrapBrokers(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	out := doc{}
	if cl.Bootstrap != "" {
		for _, k := range []string{"bootstrapBrokerString", "bootstrapBrokerStringTls", "bootstrapBrokerStringSaslScram", "bootstrapBrokerStringSaslIam"} {
			out[k] = cl.Bootstrap
		}
	}
	return out, nil
}

// ponytail: every node is the cluster's one broker.
func listNodes(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	d, nodes := cl.Doc, []any{}
	bn := object(d["brokerNodeGroupInfo"])
	subnets := list(bn["clientSubnets"])
	for i := 1; i <= num(d["numberOfBrokerNodes"]) && len(subnets) > 0; i++ {
		nodes = append(nodes, doc{"nodeType": "BROKER", "nodeARN": arnOf(d, "broker") + "/" + strconv.Itoa(i), "instanceType": bn["instanceType"],
			"addedToClusterTime": d["creationTime"], "brokerNodeInfo": doc{"brokerId": i, "clientSubnet": subnets[(i-1)%len(subnets)],
				"clientVpcIpAddress": "127.0.0.1", "currentBrokerSoftwareInfo": d["currentBrokerSoftwareInfo"], "endpoints": []string{"127.0.0.1"}}})
	}
	return doc{"nodeInfoList": nodes}, nil
}

// arnOf is the ARN of a kind of part of the cluster d: arn:…:<kind>/<name>/<uuid>.
func arnOf(d doc, kind string) string {
	return strings.Replace(str(d["clusterArn"]), ":cluster/", ":"+kind+"/", 1)
}

// ---- updates: metadata at once; the broker keeps running as it is ----

// changed checks that a cluster can take an update and records it as a
// completed operation, from source to target (MutableClusterInfo); the
// caller applies it on success. Lock held.
func changed(cl *cluster, in doc, typ string, source, target doc) (doc, error) {
	d := cl.Doc
	switch {
	case d["state"] != "ACTIVE":
		return nil, invalid("The cluster is %v; only an ACTIVE cluster can be updated.", d["state"])
	case in["currentVersion"] != d["currentVersion"]:
		return nil, invalid("The provided current version %v does not match the actual current version %v.", in["currentVersion"], d["currentVersion"])
	}
	at := now()
	op := doc{"clusterArn": d["clusterArn"], "operationArn": arnOf(d, "cluster-operation") + "/" + core.UUID(), "operationType": typ,
		"operationState": "UPDATE_COMPLETE", "creationTime": at, "endTime": at, "sourceClusterInfo": source, "targetClusterInfo": target}
	cl.Ops = append(cl.Ops, op)
	d["currentVersion"] = version()
	return doc{"clusterArn": d["clusterArn"], "clusterOperationArn": op["operationArn"]}, nil
}

func updateBrokerCount(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	n := in["targetNumberOfBrokerNodes"]
	if subnets := len(list(object(cl.Doc["brokerNodeGroupInfo"])["clientSubnets"])); num(n) < 1 || subnets > 0 && num(n)%subnets != 0 {
		return nil, invalid("The number of broker nodes must be a multiple of Availability Zones in the Client Subnets parameter.")
	}
	out, err := changed(cl, in, "UPDATE_BROKER_COUNT", doc{"numberOfBrokerNodes": cl.Doc["numberOfBrokerNodes"]}, doc{"numberOfBrokerNodes": n})
	if err == nil {
		cl.Doc["numberOfBrokerNodes"] = n
	}
	return out, err
}

// ponytail: one volume size for every broker, whatever kafkaBrokerNodeId says.
func updateBrokerStorage(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	vols := list(in["targetBrokerEBSVolumeInfo"])
	if len(vols) == 0 {
		return nil, invalid("targetBrokerEBSVolumeInfo is required.")
	}
	ebs := child(child(object(cl.Doc["brokerNodeGroupInfo"]), "storageInfo"), "ebsStorageInfo")
	out, err := changed(cl, in, "UPDATE_BROKER_STORAGE",
		doc{"brokerEBSVolumeInfo": []any{doc{"kafkaBrokerNodeId": "All", "volumeSizeGB": ebs["volumeSize"]}}}, doc{"brokerEBSVolumeInfo": vols})
	if err == nil {
		v := object(vols[0])
		ebs["volumeSize"] = v["volumeSizeGB"]
		if pt := v["provisionedThroughput"]; pt != nil {
			ebs["provisionedThroughput"] = pt
		}
	}
	return out, err
}

func updateBrokerType(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	bn := object(cl.Doc["brokerNodeGroupInfo"])
	out, err := changed(cl, in, "UPDATE_BROKER_TYPE", doc{"instanceType": bn["instanceType"]}, doc{"instanceType": in["targetInstanceType"]})
	if err == nil {
		bn["instanceType"] = in["targetInstanceType"]
	}
	return out, err
}

// ponytail: the broker does not read the configuration's server properties.
func (s *Service) updateClusterConfiguration(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	ci := object(in["configurationInfo"])
	if err := s.revision(ci); err != nil {
		return nil, err
	}
	sw := object(cl.Doc["currentBrokerSoftwareInfo"])
	out, err := changed(cl, in, "UPDATE_CLUSTER_CONFIGURATION",
		doc{"configurationInfo": doc{"arn": sw["configurationArn"], "revision": sw["configurationRevision"]}}, doc{"configurationInfo": ci})
	if err == nil {
		sw["configurationArn"], sw["configurationRevision"] = ci["arn"], ci["revision"]
	}
	return out, err
}

// ponytail: Redpanda speaks the Kafka API of every version; the version is metadata.
func (s *Service) updateClusterKafkaVersion(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	v, ci := str(in["targetKafkaVersion"]), object(in["configurationInfo"])
	if v == "" {
		return nil, invalid("targetKafkaVersion is required.")
	}
	target := doc{"kafkaVersion": v}
	if ci != nil {
		if err := s.revision(ci); err != nil {
			return nil, err
		}
		target["configurationInfo"] = ci
	}
	sw := object(cl.Doc["currentBrokerSoftwareInfo"])
	out, err := changed(cl, in, "UPDATE_CLUSTER_KAFKA_VERSION", doc{"kafkaVersion": sw["kafkaVersion"]}, target)
	if err == nil {
		sw["kafkaVersion"] = v
		if ci != nil {
			sw["configurationArn"], sw["configurationRevision"] = ci["arn"], ci["revision"]
		}
	}
	return out, err
}

func listOperations(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	ops := []any{}
	for _, op := range slices.Backward(cl.Ops) {
		ops = append(ops, op)
	}
	return doc{"clusterOperationInfoList": ops}, nil
}

func (s *Service) describeOperation(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	arn := r.PathValue("op")
	for _, cl := range s.st.Clusters {
		for _, op := range cl.Ops {
			if op["operationArn"] == arn {
				return doc{"clusterOperationInfo": op}, nil
			}
		}
	}
	return nil, notFound("Cluster operation %s does not exist.", arn)
}

// ---- SCRAM secrets ----

func scramSecrets(associate bool) handler {
	return func(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
		for _, a := range list(in["secretArnList"]) {
			switch i := slices.Index(cl.Secrets, str(a)); {
			case associate && i < 0:
				cl.Secrets = append(cl.Secrets, str(a))
			case !associate && i >= 0:
				cl.Secrets = slices.Delete(cl.Secrets, i, i+1)
			}
		}
		return doc{"clusterArn": cl.Doc["clusterArn"], "unprocessedScramSecrets": []any{}}, nil
	}
}

func listScramSecrets(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	return doc{"secretArnList": cl.Secrets}, nil
}

// ---- configurations ----

func (s *Service) createConfiguration(c *core.Ctx, _ *http.Request, _ *cluster, in doc) (any, error) {
	name := str(in["name"])
	switch {
	case !nameRe.MatchString(name):
		return nil, invalid("The configuration name %q must be 1 to 64 letters, digits and hyphens, starting with a letter or digit.", name)
	case str(in["serverProperties"]) == "":
		return nil, invalid("serverProperties is required.")
	}
	prefix := c.ARN("kafka", "configuration/"+name+"/")
	for arn := range s.st.Configs {
		if strings.HasPrefix(arn, prefix) {
			return nil, core.Errorf(409, "ConflictException", "A configuration named %s already exists.", name)
		}
	}
	at, props := now(), in["serverProperties"]
	delete(in, "serverProperties")
	in["arn"], in["creationTime"], in["state"] = prefix+core.UUID(), at, "ACTIVE"
	def(in, "kafkaVersions", []any{})
	cf := &config{Doc: in, Revisions: []doc{{"revision": 1, "creationTime": at, "description": in["description"], "serverProperties": props}}}
	s.st.Configs[str(in["arn"])] = cf
	return cf.view(), nil
}

// config finds a configuration by ARN; lock held.
func (s *Service) config(arn string) (*config, error) {
	if cf := s.st.Configs[arn]; cf != nil {
		return cf, nil
	}
	return nil, invalid("Configuration ARN does not exist: %s", arn) // MSK's answer, which Terraform looks for
}

// revision checks that a configurationInfo names a revision that exists; lock held.
func (s *Service) revision(ci doc) error {
	if cf := s.st.Configs[str(ci["arn"])]; cf == nil || num(ci["revision"]) < 1 || num(ci["revision"]) > len(cf.Revisions) {
		return invalid("Configuration %v revision %v does not exist.", ci["arn"], ci["revision"])
	}
	return nil
}

// view is a configuration as DescribeConfiguration shows it.
func (cf *config) view() doc {
	out := maps.Clone(cf.Doc)
	out["latestRevision"] = summary(cf.Revisions[len(cf.Revisions)-1])
	return out
}

// summary is a revision as lists show it, without its server properties.
func summary(rev doc) doc {
	out := maps.Clone(rev)
	delete(out, "serverProperties")
	return out
}

func (s *Service) describeConfiguration(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	cf, err := s.config(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	return cf.view(), nil
}

func (s *Service) listConfigurations(c *core.Ctx, _ *http.Request, _ *cluster, _ doc) (any, error) {
	out := []any{}
	for _, arn := range core.SortedKeys(s.st.Configs) {
		if strings.HasPrefix(arn, c.ARN("kafka", "configuration/")) {
			out = append(out, s.st.Configs[arn].view())
		}
	}
	return doc{"configurations": out}, nil
}

func (s *Service) updateConfiguration(_ *core.Ctx, r *http.Request, _ *cluster, in doc) (any, error) {
	cf, err := s.config(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	if str(in["serverProperties"]) == "" {
		return nil, invalid("serverProperties is required.")
	}
	rev := doc{"revision": len(cf.Revisions) + 1, "creationTime": now(), "description": in["description"], "serverProperties": in["serverProperties"]}
	cf.Revisions = append(cf.Revisions, rev)
	return doc{"arn": cf.Doc["arn"], "latestRevision": summary(rev)}, nil
}

func (s *Service) deleteConfiguration(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	cf, err := s.config(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	delete(s.st.Configs, str(cf.Doc["arn"]))
	return doc{"arn": cf.Doc["arn"], "state": "DELETING"}, nil
}

func (s *Service) listRevisions(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	cf, err := s.config(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	revs := []any{}
	for _, rev := range cf.Revisions {
		revs = append(revs, summary(rev))
	}
	return doc{"revisions": revs}, nil
}

func (s *Service) describeRevision(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	cf, err := s.config(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	n, _ := strconv.Atoi(r.PathValue("rev"))
	if n < 1 || n > len(cf.Revisions) {
		return nil, invalid("Configuration revision %s does not exist.", r.PathValue("rev"))
	}
	out := maps.Clone(cf.Revisions[n-1])
	out["arn"] = cf.Doc["arn"]
	return out, nil
}

// ---- VPC connections ----

func (s *Service) createVpcConnection(c *core.Ctx, _ *http.Request, _ *cluster, in doc) (any, error) {
	target := s.st.Clusters[str(in["targetClusterArn"])]
	if target == nil {
		return nil, notFound("Cluster %v does not exist.", in["targetClusterArn"])
	}
	arn := c.ARN("kafka", "vpc-connection/"+c.Account+"/"+str(target.Doc["clusterName"])+"/"+core.UUID())
	in["vpcConnectionArn"], in["creationTime"], in["state"], in["subnets"] = arn, now(), "AVAILABLE", in["clientSubnets"]
	delete(in, "clientSubnets")
	child(in, "tags")
	s.st.VPCs[arn] = in
	out := maps.Clone(in)
	out["state"], out["clientSubnets"] = "CREATING", in["subnets"]
	return out, nil
}

// vpcConnection finds a VPC connection by ARN; lock held.
func (s *Service) vpcConnection(arn string) (doc, error) {
	if v := s.st.VPCs[arn]; v != nil {
		return v, nil
	}
	return nil, notFound("VPC connection %s does not exist.", arn)
}

func (s *Service) describeVpcConnection(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	return s.vpcConnection(r.PathValue("arn"))
}

func (s *Service) deleteVpcConnection(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	v, err := s.vpcConnection(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	delete(s.st.VPCs, str(v["vpcConnectionArn"]))
	return doc{"vpcConnectionArn": v["vpcConnectionArn"], "state": "DELETING"}, nil
}

func (s *Service) listVpcConnections(c *core.Ctx, _ *http.Request, _ *cluster, _ doc) (any, error) {
	out := []any{}
	for _, arn := range core.SortedKeys(s.st.VPCs) {
		if strings.HasPrefix(arn, c.ARN("kafka", "vpc-connection/")) {
			out = append(out, s.st.VPCs[arn])
		}
	}
	return doc{"vpcConnections": out}, nil
}

// ---- Kafka versions ----

func listVersions(_ *core.Ctx, _ *http.Request, _ *cluster, _ doc) (any, error) {
	out := []any{}
	for _, v := range versions {
		out = append(out, doc{"version": v, "status": "ACTIVE"})
	}
	return doc{"kafkaVersions": out}, nil
}

// compatibleVersions offers the later versions of the same metadata mode
// (ZooKeeper or KRaft) as upgrades.
func (s *Service) compatibleVersions(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	sources := versions
	if arn := r.URL.Query().Get("clusterArn"); arn != "" {
		cl := s.st.Clusters[arn]
		if cl == nil {
			return nil, notFound("Cluster %s does not exist.", arn)
		}
		sources = []string{str(object(cl.Doc["currentBrokerSoftwareInfo"])["kafkaVersion"])}
	}
	out := []any{}
	for _, src := range sources {
		targets := []string{}
		for _, v := range versions[slices.Index(versions, src)+1:] {
			if strings.HasSuffix(v, ".kraft") == strings.HasSuffix(src, ".kraft") {
				targets = append(targets, v)
			}
		}
		out = append(out, doc{"sourceVersion": src, "targetVersions": targets})
	}
	return doc{"compatibleKafkaVersions": out}, nil
}

// ---- tags ----

// tags finds the tags of a cluster or VPC connection by ARN; lock held.
func (s *Service) tags(arn string) (doc, error) {
	if cl := s.st.Clusters[arn]; cl != nil {
		return child(cl.Doc, "tags"), nil
	}
	if v := s.st.VPCs[arn]; v != nil {
		return child(v, "tags"), nil
	}
	return nil, notFound("Resource %s does not exist.", arn)
}

func (s *Service) tagResource(_ *core.Ctx, r *http.Request, _ *cluster, in doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	maps.Copy(t, object(in["tags"]))
	return nil, nil
}

func (s *Service) untagResource(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(t, k)
	}
	return nil, nil
}

func (s *Service) listTags(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	return doc{"tags": t}, nil
}

// ---- persistence, console, shutdown ----

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

// Load restores state. Clusters that ran a broker start their containers
// again (persisted state leaves them behind on shutdown), or fresh ones when
// they are gone; without an engine they stay ACTIVE as metadata.
func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	engine := false
	for _, cl := range st.Clusters {
		if cl.Container != "" {
			engine = s.hub.Docker().Available()
			break
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.st
	s.st = st
	for _, cl := range st.Clusters {
		switch {
		case cl.Container == "":
		case engine:
			s.boot(cl)
		default:
			cl.Doc["state"], cl.Note = "ACTIVE", noDocker
		}
	}
	for _, cl := range old.Clusters {
		s.drop(cl)
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close stops broker starts. The hub's container engine client then removes
// the containers, unless state is persisted: the next run starts them again.
func (s *Service) Close() error {
	s.stop()
	return nil
}

// Resources lists clusters and configurations for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	cls := core.Table{Name: "Clusters", Columns: []string{"Name", "State", "Type", "Kafka version", "Bootstrap brokers", "Region", "Account"}}
	for _, arn := range core.SortedKeys(s.st.Clusters) {
		cl := s.st.Clusters[arn]
		_, region, account, _, _ := core.ParseARN(arn)
		cls.Rows = append(cls.Rows, []string{str(cl.Doc["clusterName"]), str(cl.Doc["state"]), str(cl.Doc["clusterType"]),
			str(object(cl.Doc["currentBrokerSoftwareInfo"])["kafkaVersion"]), cl.Bootstrap, region, account})
	}
	cfs := core.Table{Name: "Configurations", Columns: []string{"Name", "Latest revision", "Region", "Account"}}
	for _, arn := range core.SortedKeys(s.st.Configs) {
		cf := s.st.Configs[arn]
		_, region, account, _, _ := core.ParseARN(arn)
		cfs.Rows = append(cfs.Rows, []string{str(cf.Doc["name"]), strconv.Itoa(len(cf.Revisions)), region, account})
	}
	return []core.Table{cls, cfs}
}

// Inspect returns a cluster with its operations and pogi's side (container,
// bootstrap brokers, pogiNote), or a configuration with its revisions.
func (s *Service) Inspect(table, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	var v doc
	if table == "Configurations" {
		for _, arn := range core.SortedKeys(s.st.Configs) {
			if cf := s.st.Configs[arn]; v == nil && cf.Doc["name"] == id {
				v = doc{"configuration": cf.view(), "revisions": cf.Revisions}
			}
		}
	} else {
		for _, arn := range core.SortedKeys(s.st.Clusters) {
			if cl := s.st.Clusters[arn]; v == nil && cl.Doc["clusterName"] == id {
				v = doc{"cluster": v2(cl.Doc), "operations": cl.Ops, "scramSecrets": cl.Secrets, "container": cl.Container,
					"bootstrapBrokers": cl.Bootstrap, "pogiNote": cl.Note}
			}
		}
	}
	if v == nil {
		return nil
	}
	b, _ := json.Marshal(v) // a copy: the state may change once the lock is released
	return json.RawMessage(b)
}

// ---- helpers ----

func invalid(format string, a ...any) error {
	return core.Errorf(400, "BadRequestException", format, a...)
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "NotFoundException", format, a...)
}

func str(v any) string { s, _ := v.(string); return s }
func num(v any) int    { f, _ := v.(float64); return int(f) }
func list(v any) []any { l, _ := v.([]any); return l }
func object(v any) doc { d, _ := v.(doc); return d }
func def(d doc, k string, v any) {
	if _, ok := d[k]; !ok {
		d[k] = v
	}
}

// child returns the object member k of d, adding an empty one when missing.
func child(d doc, k string) doc {
	if c, ok := d[k].(doc); ok {
		return c
	}
	c := doc{}
	d[k] = c
	return c
}

func now() string { return time.Now().UTC().Format(core.ISO) }

// version is a cluster version (currentVersion), which every update changes.
func version() string { return "K" + core.RandString(13, core.UpperNum) }
