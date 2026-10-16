// Package eks emulates Amazon EKS (REST-JSON). With a container engine every
// cluster is a real Kubernetes API server, a k3s container that checks bearer
// tokens the way EKS does, so `aws eks update-kubeconfig` and kubectl work
// unchanged. Node groups, Fargate profiles, add-ons, access entries, identity
// provider configs and pod identity associations are metadata.
package eks

import (
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"path"
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
const noDocker = "Docker is not available: the API works, but no Kubernetes API server runs behind this endpoint"

// k3sTags maps the Kubernetes versions on offer to k3s images.
var k3sTags = map[string]string{"1.30": "v1.30.14-k3s2", "1.31": "v1.31.14-k3s1", "1.32": "v1.32.13-k3s1",
	"1.33": "v1.33.13-k3s2", "1.34": "v1.34.12-k3s1", "1.35": "v1.35.5-k3s1", "1.36": "v1.36.5-k3s1"}

const defaultVersion = "1.35"

// addons are the add-ons on offer and the version each installs by default.
// ponytail: one version per add-on, compatible with every Kubernetes version.
var addons = map[string]string{"vpc-cni": "v1.20.4-eksbuild.2", "coredns": "v1.12.4-eksbuild.1", "kube-proxy": "v1.35.0-eksbuild.2",
	"aws-ebs-csi-driver": "v1.51.0-eksbuild.1", "eks-pod-identity-agent": "v1.3.9-eksbuild.3", "metrics-server": "v0.8.0-eksbuild.3"}

// doc is an API shape as JSON decodes it; resources are kept in this form,
// member names and all, and returned as they are.
type doc = map[string]any

// Service is the EKS emulator.
type Service struct {
	hub  *core.Hub
	mux  *http.ServeMux
	ctx  context.Context // k3s work; ends at Close
	stop context.CancelFunc
	mu   sync.Mutex
	st   state
}

type state struct {
	Clusters map[string]*cluster `json:"clusters"` // account/region/name
}

// cluster is a cluster as DescribeCluster shows it, its parts and pogi's side.
type cluster struct {
	Doc       doc                       `json:"doc"`
	Parts     map[string]map[string]doc `json:"parts,omitempty"`    // path segment (node-groups…) → id → API shape
	Policies  map[string][]doc          `json:"policies,omitempty"` // principal ARN → associated access policies
	Updates   map[string]*update        `json:"updates,omitempty"`
	Creator   string                    `json:"creator"`             // ARN of the caller that created it
	Container string                    `json:"container,omitempty"` // k3s; empty: metadata only
	Token     string                    `json:"token,omitempty"`     // k3s server token, never shown
	Note      string                    `json:"pogiNote,omitempty"`
	cancel    context.CancelFunc        // stops its k3s start
}

// update is a change made by an Update* call. It succeeds at once; the
// caller sees it InProgress.
type update struct {
	Doc doc    `json:"doc"`
	Of  string `json:"of,omitempty"` // nodegroup:<name> or addon:<name>; empty: the cluster
}

// kind is a part of a cluster served at /clusters/{name}/<path segment>.
type kind struct {
	op, ops string // Create<op>…, List<ops>
	id      string // the member naming one
	one     string // the response member of one; its ARN is <one>Arn
	list    string // the response member of List
	arn     string // ARN resource type
	status  bool   // goes CREATING → ACTIVE
}

var kinds = map[string]kind{
	"node-groups":               {"Nodegroup", "Nodegroups", "nodegroupName", "nodegroup", "nodegroups", "nodegroup", true},
	"fargate-profiles":          {"FargateProfile", "FargateProfiles", "fargateProfileName", "fargateProfile", "fargateProfileNames", "fargateprofile", true},
	"addons":                    {"Addon", "Addons", "addonName", "addon", "addons", "addon", true},
	"access-entries":            {"AccessEntry", "AccessEntries", "principalArn", "accessEntry", "accessEntries", "access-entry", false},
	"pod-identity-associations": {"PodIdentityAssociation", "PodIdentityAssociations", "associationId", "association", "associations", "podidentityassociation", false},
}

// idps is the part holding identity provider configs (by name), served at
// /clusters/{name}/identity-provider-configs/<verb>.
const idps = "identity-provider-configs"

// New creates the EKS service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux(), st: state{Clusters: map[string]*cluster{}}}
	s.ctx, s.stop = context.WithCancel(context.Background())
	s.routes()
	hub.Mount("/_aws/eks/authn/", http.HandlerFunc(s.authn))
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "eks", Cloud: core.AWS, Title: "EKS", Signing: []string{"eks"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// handler serves one operation with the lock held: cl is the cluster the
// path names (nil when it names none) and in the JSON body.
type handler func(c *core.Ctx, r *http.Request, cl *cluster, in doc) (any, error)

func (s *Service) route(pattern, op string, h handler) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		out, err := s.serve(c, r, h)
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		core.RESTJSON(w, c, http.StatusOK, json.RawMessage(out))
	})
}

func (s *Service) serve(c *core.Ctx, r *http.Request, h handler) ([]byte, error) {
	if err := c.Intercept(); err != nil {
		return nil, err
	}
	in := doc{}
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, &in) != nil {
		return nil, core.Errorf(400, "BadRequestException", "The request body is not valid JSON")
	}
	delete(in, "clientRequestToken")
	s.mu.Lock()
	defer s.mu.Unlock()
	var cl *cluster
	if name := r.PathValue("name"); name != "" {
		if cl = s.st.Clusters[c.Account+"/"+c.Region+"/"+name]; cl == nil {
			return nil, core.Errorf(404, "ResourceNotFoundException", "No cluster found for name: %s.", name)
		}
	}
	out, err := h(c, r, cl, in)
	if err != nil {
		return nil, err
	}
	return json.Marshal(out) // under the lock: out shares the state's maps
}

func (s *Service) routes() {
	s.route("POST /clusters", "CreateCluster", s.createCluster)
	s.route("GET /clusters", "ListClusters", s.listClusters)
	s.route("GET /clusters/{name}", "DescribeCluster", describeCluster)
	s.route("DELETE /clusters/{name}", "DeleteCluster", s.deleteCluster)
	s.route("POST /clusters/{name}/update-config", "UpdateClusterConfig", updateClusterConfig)
	s.route("POST /clusters/{name}/updates", "UpdateClusterVersion", updateClusterVersion)
	s.route("GET /clusters/{name}/updates", "ListUpdates", listUpdates)
	s.route("GET /clusters/{name}/updates/{id}", "DescribeUpdate", describeUpdate)
	for seg, k := range kinds {
		p := "/clusters/{name}/" + seg
		s.route("POST "+p, "Create"+k.op, createPart(seg))
		s.route("GET "+p, "List"+k.ops, listParts(seg))
		s.route("GET "+p+"/{id}", "Describe"+k.op, describePart(seg))
		s.route("DELETE "+p+"/{id}", "Delete"+k.op, deletePart(seg))
	}
	s.route("POST /clusters/{name}/node-groups/{id}/update-config", "UpdateNodegroupConfig", updatePart("node-groups", "ConfigUpdate"))
	s.route("POST /clusters/{name}/node-groups/{id}/update-version", "UpdateNodegroupVersion", updatePart("node-groups", "VersionUpdate"))
	s.route("POST /clusters/{name}/addons/{id}/update", "UpdateAddon", updatePart("addons", "AddonUpdate"))
	s.route("POST /clusters/{name}/access-entries/{id}", "UpdateAccessEntry", updatePart("access-entries", ""))
	s.route("POST /clusters/{name}/pod-identity-associations/{id}", "UpdatePodIdentityAssociation", updatePart("pod-identity-associations", ""))
	s.route("POST /clusters/{name}/access-entries/{id}/access-policies", "AssociateAccessPolicy", associatePolicy)
	s.route("GET /clusters/{name}/access-entries/{id}/access-policies", "ListAssociatedAccessPolicies", listPolicies)
	s.route("DELETE /clusters/{name}/access-entries/{id}/access-policies/{policy}", "DisassociateAccessPolicy", disassociatePolicy)
	s.route("POST /clusters/{name}/"+idps+"/associate", "AssociateIdentityProviderConfig", associateIdp)
	s.route("POST /clusters/{name}/"+idps+"/describe", "DescribeIdentityProviderConfig", idp(false))
	s.route("POST /clusters/{name}/"+idps+"/disassociate", "DisassociateIdentityProviderConfig", idp(true))
	s.route("GET /clusters/{name}/"+idps, "ListIdentityProviderConfigs", listIdps)
	s.route("GET /addons/supported-versions", "DescribeAddonVersions", addonVersions)
	s.route("GET /access-policies", "ListAccessPolicies", accessPolicies)
	s.route("POST /tags/{arn...}", "TagResource", s.tagResource)
	s.route("GET /tags/{arn...}", "ListTagsForResource", s.listTags)
	s.route("DELETE /tags/{arn...}", "UntagResource", s.untagResource)
	s.route("/", "Unknown", func(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
		return nil, core.Errorf(501, "NotImplemented", "pogi does not implement EKS %s %s", r.Method, r.URL.Path)
	})
}

// ---- clusters ----

var nameRe = regexp.MustCompile(`^[0-9A-Za-z][A-Za-z0-9_-]{0,99}$`)

func (s *Service) createCluster(c *core.Ctx, _ *http.Request, _ *cluster, d doc) (any, error) {
	name := str(d["name"])
	if !nameRe.MatchString(name) {
		return nil, invalid("The cluster name %q must start with a letter or digit and have at most 100 letters, digits, hyphens and underscores.", name)
	}
	version := cmp.Or(str(d["version"]), defaultVersion)
	if k3sTags[version] == "" {
		return nil, invalid("unsupported Kubernetes version %s", version)
	}
	key := c.Account + "/" + c.Region + "/" + name
	if s.st.Clusters[key] != nil {
		return nil, core.Errorf(409, "ResourceInUseException", "Cluster already exists with name: %s", name)
	}
	delete(d, "bootstrapSelfManagedAddons")
	d["name"], d["arn"], d["version"], d["platformVersion"] = name, c.ARN("eks", "cluster/"+name), version, "eks.1"
	d["createdAt"] = core.Unix(time.Now())
	vpc := child(d, "resourcesVpcConfig")
	if id, err := s.subnetsVpc(c, list(vpc["subnetIds"])); err != nil {
		return nil, err
	} else if id != "" {
		vpc["vpcId"] = id
	}
	def(vpc, "endpointPublicAccess", true)
	def(vpc, "endpointPrivateAccess", false)
	def(vpc, "publicAccessCidrs", []any{"0.0.0.0/0"})
	def(vpc, "clusterSecurityGroupId", "sg-"+core.RandHex(9)[:17])
	def(vpc, "vpcId", "vpc-"+core.RandHex(9)[:17])
	knc := child(d, "kubernetesNetworkConfig")
	def(knc, "serviceIpv4Cidr", "10.100.0.0/16")
	def(knc, "ipFamily", "ipv4")
	access := child(d, "accessConfig")
	def(access, "authenticationMode", "CONFIG_MAP")
	def(access, "bootstrapClusterCreatorAdminPermissions", true)
	def(d, "upgradePolicy", doc{"supportType": "EXTENDED"})
	def(d, "logging", doc{"clusterLogging": []any{doc{"types": []any{"api", "audit", "authenticator", "controllerManager", "scheduler"}, "enabled": false}}})
	d["identity"] = doc{"oidc": doc{"issuer": "https://oidc.eks." + c.Region + "." + core.URLSuffix(c.Region) + "/id/" + strings.ToUpper(core.RandHex(16))}}
	cl := &cluster{Doc: d, Creator: c.Principal}
	s.st.Clusters[key] = cl
	if s.hub.Docker().Available() {
		cl.Container, cl.Token = docker.Name("eks", name), core.RandHex(16)
		if c.Account != core.DefaultAccount || c.Region != s.hub.Cfg.Region {
			cl.Container += "-" + c.Account + "-" + c.Region
		}
		s.boot(key, cl, false)
	} else {
		d["status"], cl.Note = "ACTIVE", noDocker
		d["endpoint"] = "https://" + strings.ToUpper(core.RandHex(16)) + ".gr7." + c.Region + ".eks." + core.URLSuffix(c.Region)
		s.hub.Log.Warn(noDocker, "service", "eks", "cluster", name)
	}
	out := created(d)
	delete(out, "endpoint")
	return doc{"cluster": out}, nil
}

// subnetsVpc asks pogi's EC2 for the VPC of the subnets of a cluster, and
// refuses subnets that do not exist or lie in different VPCs. It returns ""
// when there is nothing to check against: no subnets, or EC2 is not enabled.
func (s *Service) subnetsVpc(c *core.Ctx, ids []any) (string, error) {
	if len(ids) == 0 || s.hub.Service("ec2") == nil {
		return "", nil
	}
	form := url.Values{"Action": {"DescribeSubnets"}, "Version": {"2016-11-15"}}
	for i, id := range ids {
		form.Set("SubnetId."+strconv.Itoa(i+1), str(id))
	}
	code, _, body := s.hub.Do(c.Account, c.Region, "ec2", http.MethodPost, "/",
		http.Header{"Content-Type": {"application/x-www-form-urlencoded"}}, []byte(form.Encode()))
	var known struct {
		Subnets []struct {
			VPC string `xml:"vpcId"`
		} `xml:"subnetSet>item"`
	}
	if code != http.StatusOK || xml.Unmarshal(body, &known) != nil || len(known.Subnets) != len(ids) {
		return "", invalid("One or more of the subnet IDs %v does not exist.", ids)
	}
	for _, sn := range known.Subnets {
		if sn.VPC != known.Subnets[0].VPC {
			return "", invalid("The subnets %v must all be in the same VPC.", ids)
		}
	}
	return known.Subnets[0].VPC, nil
}

// boot runs the k3s container of a cluster in the background (restart: the
// one it has) and makes the cluster ACTIVE once its node is Ready. Lock held.
func (s *Service) boot(key string, cl *cluster, restart bool) {
	ctx, cancel := context.WithCancel(s.ctx)
	cl.cancel = cancel
	cl.Doc["status"] = "CREATING"
	d := s.hub.Docker()
	port := strconv.Itoa(s.hub.Cfg.Port) // before pogi listens (Load at startup) its URL is unset
	if u, err := url.Parse(s.hub.URL); err == nil && u.Port() != "" {
		port = u.Port()
	}
	// ponytail: an engine without a VM (Linux) cannot reach pogi on 127.0.0.1; kubectl
	// then gets Unauthorized until pogi listens where the container can reach it.
	k := &K3s{Name: cl.Container, Image: "rancher/k3s:" + k3sTags[str(cl.Doc["version"])], Service: "eks", Resource: str(cl.Doc["name"]),
		Token: cl.Token, Webhook: "http://" + d.HostAddr() + ":" + port + "/_aws/eks/authn/" + key,
		Args: []string{"--service-cidr=" + str(object(cl.Doc["kubernetesNetworkConfig"])["serviceIpv4Cidr"])}}
	core.Go("eks.boot", func() {
		defer cancel()
		endpoint, ca, err := k.Run(ctx, d, restart)
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case s.st.Clusters[key] != cl: // deleted meanwhile
			s.drop(cl)
		case err != nil:
			if restart {
				err = fmt.Errorf("its k3s container did not start again: %w", err)
			}
			cl.Doc["status"], cl.Note = "FAILED", err.Error()
			s.hub.Log.Warn("eks cluster failed", "cluster", key, "err", err)
		default:
			cl.Doc["status"], cl.Doc["endpoint"], cl.Note = "ACTIVE", endpoint, ""
			cl.Doc["certificateAuthority"] = doc{"data": base64.StdEncoding.EncodeToString(ca)}
		}
	})
}

// drop stops the k3s work of a cluster that is gone and removes its
// container, unless a current cluster runs in it. Lock held.
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
		go s.hub.Docker().Remove(context.Background(), cl.Container)
	}
}

func describeCluster(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	return doc{"cluster": cl.Doc}, nil
}

// ponytail: lists return everything in one page; add nextToken when someone has thousands.
func (s *Service) listClusters(c *core.Ctx, _ *http.Request, _ *cluster, _ doc) (any, error) {
	names := []string{}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		if n, ok := strings.CutPrefix(k, c.Account+"/"+c.Region+"/"); ok {
			names = append(names, n)
		}
	}
	return doc{"clusters": names}, nil
}

func (s *Service) deleteCluster(c *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
	if len(cl.Parts["node-groups"]) > 0 {
		return nil, core.Errorf(409, "ResourceInUseException", "Cluster has nodegroups attached")
	}
	if len(cl.Parts["fargate-profiles"]) > 0 {
		return nil, core.Errorf(409, "ResourceInUseException", "Cluster has fargate profiles attached")
	}
	delete(s.st.Clusters, c.Account+"/"+c.Region+"/"+r.PathValue("name"))
	s.drop(cl)
	cl.Doc["status"] = "DELETING"
	return doc{"cluster": cl.Doc}, nil
}

// updateTypes names the update a member of UpdateClusterConfig makes.
var updateTypes = map[string]string{"resourcesVpcConfig": "EndpointAccessUpdate", "logging": "LoggingUpdate",
	"accessConfig": "AccessConfigUpdate", "upgradePolicy": "UpgradePolicyUpdate", "zonalShiftConfig": "ZonalShiftConfigUpdate"}

func updateClusterConfig(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	typ := "ConfigUpdate"
	for k := range in {
		typ = cmp.Or(updateTypes[k], typ)
	}
	merge(cl.Doc, in)
	return doc{"update": cl.change("", typ, in)}, nil
}

func updateClusterVersion(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	v := str(in["version"])
	if k3sTags[v] == "" {
		return nil, invalid("unsupported Kubernetes version %s", v)
	}
	// ponytail: a running k3s keeps its version; create the cluster again to run another
	cl.Doc["version"] = v
	return doc{"update": cl.change("", "VersionUpdate", doc{"version": v, "platformVersion": "eks.1"})}, nil
}

// change records an update made with the members of in; lock held.
func (cl *cluster) change(of, typ string, in doc) doc {
	params := []any{}
	for _, k := range slices.Sorted(maps.Keys(in)) {
		if k == "" {
			continue
		}
		v, ok := in[k].(string)
		if !ok {
			b, _ := json.Marshal(in[k])
			v = string(b)
		}
		params = append(params, doc{"type": strings.ToUpper(k[:1]) + k[1:], "value": v})
	}
	id := core.UUID()
	u := doc{"id": id, "status": "Successful", "type": typ, "params": params, "createdAt": core.Unix(time.Now()), "errors": []any{}}
	if cl.Updates == nil {
		cl.Updates = map[string]*update{}
	}
	cl.Updates[id] = &update{Doc: u, Of: of}
	out := maps.Clone(u)
	out["status"] = "InProgress"
	return out
}

// of is what the nodegroupName or addonName parameter scopes an update to.
func of(r *http.Request) string {
	q := r.URL.Query()
	switch {
	case q.Get("nodegroupName") != "":
		return "nodegroup:" + q.Get("nodegroupName")
	case q.Get("addonName") != "":
		return "addon:" + q.Get("addonName")
	}
	return ""
}

func listUpdates(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
	ids := []string{}
	for _, id := range core.SortedKeys(cl.Updates) {
		if cl.Updates[id].Of == of(r) {
			ids = append(ids, id)
		}
	}
	return doc{"updateIds": ids}, nil
}

func describeUpdate(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
	id := r.PathValue("id")
	if u := cl.Updates[id]; u != nil && u.Of == of(r) {
		return doc{"update": u.Doc}, nil
	}
	return nil, core.Errorf(404, "ResourceNotFoundException", "No update found for ID: %s", id)
}

// ---- node groups, Fargate profiles, add-ons, access entries, pod identity associations ----

// part finds the part of cl the path names; lock held.
func part(r *http.Request, cl *cluster, seg string) (doc, error) {
	id := r.PathValue("id")
	if d := cl.Parts[seg][id]; d != nil {
		return d, nil
	}
	k := kinds[seg]
	return nil, core.Errorf(404, "ResourceNotFoundException", "No %s found for %s: %s.", k.op, k.id, id)
}

func createPart(seg string) handler {
	k := kinds[seg]
	return func(c *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
		name, id := str(cl.Doc["name"]), str(in[k.id])
		res := k.arn + "/" + name + "/" + id + "/" + core.UUID()
		switch seg {
		case "node-groups":
			def(in, "version", cl.Doc["version"])
			def(in, "releaseVersion", str(in["version"])+".0-20260901")
			def(in, "capacityType", "ON_DEMAND")
			def(in, "amiType", "AL2023_x86_64_STANDARD")
			if in["launchTemplate"] == nil {
				def(in, "diskSize", 20)
				def(in, "instanceTypes", []any{"t3.medium"})
			}
			def(in, "updateConfig", doc{"maxUnavailable": 1})
			in["resources"] = doc{"autoScalingGroups": []any{doc{"name": "eks-" + id + "-" + core.UUID()}}}
		case "addons":
			def(in, "addonVersion", cmp.Or(addons[id], "v1.0.0-eksbuild.1"))
			delete(in, "resolveConflicts")
			delete(in, "podIdentityAssociations") // ponytail: add-ons make no pod identity associations
		case "access-entries":
			_, _, account, p, ok := core.ParseARN(id)
			typ, rest, _ := strings.Cut(p, "/")
			if !ok || typ != "role" && typ != "user" {
				return nil, invalid("The specified principalArn is invalid: %s", id)
			}
			if object(cl.Doc["accessConfig"])["authenticationMode"] == "CONFIG_MAP" {
				return nil, core.Errorf(400, "InvalidRequestException", "The cluster's authentication mode must be set to one of [API, API_AND_CONFIG_MAP] to perform this operation.")
			}
			res = k.arn + "/" + name + "/" + typ + "/" + account + "/" + path.Base(rest) + "/" + core.UUID()
			def(in, "type", "STANDARD")
			def(in, "username", id)
			def(in, "kubernetesGroups", []any{})
		case "pod-identity-associations":
			id = "a-" + core.RandString(17, "abcdefghijklmnopqrstuvwxyz0123456789")
			res = k.arn + "/" + name + "/" + id
		}
		if cl.Parts[seg][id] != nil {
			return nil, core.Errorf(409, "ResourceInUseException", "%s %s already exists in cluster %s.", k.op, id, name)
		}
		now := core.Unix(time.Now())
		in[k.id], in["clusterName"], in[k.one+"Arn"], in["createdAt"], in["modifiedAt"] = id, name, c.ARN("eks", res), now, now
		if k.status {
			in["status"], in["health"] = "ACTIVE", doc{"issues": []any{}}
		}
		cl.add(seg, id, in)
		return doc{k.one: created(in)}, nil
	}
}

// add stores a part; lock held.
func (cl *cluster) add(seg, id string, d doc) {
	if cl.Parts == nil {
		cl.Parts = map[string]map[string]doc{}
	}
	if cl.Parts[seg] == nil {
		cl.Parts[seg] = map[string]doc{}
	}
	cl.Parts[seg][id] = d
}

func listParts(seg string) handler {
	return func(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
		q, out := r.URL.Query(), []any{}
		ns, sa, policy := q.Get("namespace"), q.Get("serviceAccount"), q.Get("associatedPolicyArn")
		for _, id := range core.SortedKeys(cl.Parts[seg]) {
			d := cl.Parts[seg][id]
			switch {
			case seg == "pod-identity-associations": // summaries, not ids
				if (ns == "" || d["namespace"] == ns) && (sa == "" || d["serviceAccount"] == sa) {
					out = append(out, d)
				}
			case policy == "" || slices.ContainsFunc(cl.Policies[id], func(p doc) bool { return p["policyArn"] == policy }):
				out = append(out, id)
			}
		}
		return doc{kinds[seg].list: out}, nil
	}
}

func describePart(seg string) handler {
	return func(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
		d, err := part(r, cl, seg)
		if err != nil {
			return nil, err
		}
		return doc{kinds[seg].one: d}, nil
	}
}

func deletePart(seg string) handler {
	return func(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
		d, err := part(r, cl, seg)
		if err != nil {
			return nil, err
		}
		delete(cl.Parts[seg], r.PathValue("id"))
		if seg == "access-entries" {
			delete(cl.Policies, r.PathValue("id"))
		}
		if d["status"] != nil {
			d["status"] = "DELETING"
		}
		return doc{kinds[seg].one: d}, nil
	}
}

// updatePart changes a part: node groups and add-ons through an update of
// type typ, access entries and pod identity associations in place.
func updatePart(seg, typ string) handler {
	k := kinds[seg]
	return func(_ *core.Ctx, r *http.Request, cl *cluster, in doc) (any, error) {
		d, err := part(r, cl, seg)
		if err != nil {
			return nil, err
		}
		params := maps.Clone(in)
		if l := object(in["labels"]); l != nil && seg == "node-groups" { // {addOrUpdateLabels, removeLabels}
			labels := child(d, "labels")
			maps.Copy(labels, object(l["addOrUpdateLabels"]))
			for _, key := range list(l["removeLabels"]) {
				delete(labels, str(key))
			}
			delete(in, "labels")
		}
		if t := object(in["taints"]); t != nil && seg == "node-groups" { // {addOrUpdateTaints, removeTaints}, by key and effect
			taints := list(d["taints"])
			for _, x := range append(list(t["removeTaints"]), list(t["addOrUpdateTaints"])...) {
				taints = slices.DeleteFunc(taints, func(y any) bool {
					return object(x)["key"] == object(y)["key"] && object(x)["effect"] == object(y)["effect"]
				})
			}
			d["taints"] = append(taints, list(t["addOrUpdateTaints"])...)
			delete(in, "taints")
		}
		if in["updateConfig"] != nil { // maxUnavailable or maxUnavailablePercentage: one replaces the other
			delete(d, "updateConfig")
		}
		if v := str(in["version"]); v != "" && in["releaseVersion"] == nil {
			in["releaseVersion"] = v + ".0-20260901"
		}
		for _, m := range []string{"force", "resolveConflicts", "podIdentityAssociations"} {
			delete(in, m)
		}
		merge(d, in)
		d["modifiedAt"] = core.Unix(time.Now())
		if typ == "" {
			return doc{k.one: d}, nil
		}
		return doc{"update": cl.change(k.one+":"+r.PathValue("id"), typ, params)}, nil
	}
}

func associatePolicy(_ *core.Ctx, r *http.Request, cl *cluster, in doc) (any, error) {
	if _, err := part(r, cl, "access-entries"); err != nil {
		return nil, err
	}
	id, now := r.PathValue("id"), core.Unix(time.Now())
	p := doc{"policyArn": in["policyArn"], "accessScope": in["accessScope"], "associatedAt": now, "modifiedAt": now}
	if cl.Policies == nil {
		cl.Policies = map[string][]doc{}
	}
	cl.Policies[id] = append(slices.DeleteFunc(cl.Policies[id], func(x doc) bool { return x["policyArn"] == p["policyArn"] }), p)
	return doc{"clusterName": cl.Doc["name"], "principalArn": id, "associatedAccessPolicy": p}, nil
}

func listPolicies(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
	if _, err := part(r, cl, "access-entries"); err != nil {
		return nil, err
	}
	id := r.PathValue("id")
	return doc{"clusterName": cl.Doc["name"], "principalArn": id, "associatedAccessPolicies": cl.Policies[id]}, nil
}

func disassociatePolicy(_ *core.Ctx, r *http.Request, cl *cluster, _ doc) (any, error) {
	if _, err := part(r, cl, "access-entries"); err != nil {
		return nil, err
	}
	id, policy := r.PathValue("id"), r.PathValue("policy")
	n := len(cl.Policies[id])
	if cl.Policies[id] = slices.DeleteFunc(cl.Policies[id], func(x doc) bool { return x["policyArn"] == policy }); len(cl.Policies[id]) == n {
		return nil, core.Errorf(404, "ResourceNotFoundException", "The policy %s is not associated with %s.", policy, id)
	}
	return doc{}, nil
}

func accessPolicies(c *core.Ctx, _ *http.Request, _ *cluster, _ doc) (any, error) {
	out := []any{}
	for _, n := range []string{"AmazonEKSAdminPolicy", "AmazonEKSClusterAdminPolicy", "AmazonEKSEditPolicy", "AmazonEKSViewPolicy"} {
		out = append(out, doc{"name": n, "arn": "arn:" + core.Partition(c.Region) + ":eks::aws:cluster-access-policy/" + n})
	}
	return doc{"accessPolicies": out}, nil
}

func addonVersions(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	q, out := r.URL.Query(), []any{}
	for _, name := range core.SortedKeys(addons) {
		if cmp.Or(q.Get("addonName"), name) != name {
			continue
		}
		out = append(out, doc{"addonName": name, "owner": "aws", "publisher": "eks", "addonVersions": []any{doc{
			"addonVersion": addons[name], "architecture": []any{"amd64", "arm64"}, "requiresConfiguration": false,
			"compatibilities": []any{doc{"clusterVersion": cmp.Or(q.Get("kubernetesVersion"), defaultVersion), "platformVersions": []any{"*"}, "defaultVersion": true}}}}})
	}
	return doc{"addons": out}, nil
}

// ---- identity provider configs ----

func associateIdp(c *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
	d, cluster := object(in["oidc"]), str(cl.Doc["name"])
	name := str(d["identityProviderConfigName"])
	if name == "" {
		return nil, invalid("oidc.identityProviderConfigName is required")
	}
	if cl.Parts[idps][name] != nil {
		return nil, core.Errorf(409, "ResourceInUseException", "Identity provider config %s is already associated with cluster %s.", name, cluster)
	}
	d["identityProviderConfigArn"] = c.ARN("eks", "identityproviderconfig/"+cluster+"/oidc/"+name+"/"+core.UUID())
	d["clusterName"], d["status"], d["tags"] = cluster, "ACTIVE", in["tags"]
	cl.add(idps, name, d)
	return doc{"update": cl.change("", "AssociateIdentityProviderConfig", doc{"identityProviderConfig": []any{doc{"type": "oidc", "name": name}}}),
		"tags": in["tags"]}, nil
}

// idp serves DescribeIdentityProviderConfig, or with disassociate
// DisassociateIdentityProviderConfig.
func idp(disassociate bool) handler {
	return func(_ *core.Ctx, _ *http.Request, cl *cluster, in doc) (any, error) {
		name := str(object(in["identityProviderConfig"])["name"])
		d := cl.Parts[idps][name]
		switch {
		case d == nil:
			return nil, core.Errorf(404, "ResourceNotFoundException", "No identity provider config found for name: %s.", name)
		case !disassociate:
			return doc{"identityProviderConfig": doc{"oidc": d}}, nil
		}
		delete(cl.Parts[idps], name)
		return doc{"update": cl.change("", "DisassociateIdentityProviderConfig", doc{"identityProviderConfig": []any{doc{"type": "oidc", "name": name}}})}, nil
	}
}

func listIdps(_ *core.Ctx, _ *http.Request, cl *cluster, _ doc) (any, error) {
	out := []any{}
	for _, n := range core.SortedKeys(cl.Parts[idps]) {
		out = append(out, doc{"type": "oidc", "name": n})
	}
	return doc{"identityProviderConfigs": out}, nil
}

// ---- tags ----

// tags finds the tags of a cluster or one of its parts by ARN; lock held.
func (s *Service) tags(arn string) (doc, error) {
	_, region, account, res, ok := core.ParseARN(arn)
	p := strings.Split(res, "/")
	if !ok || len(p) < 2 {
		return nil, core.Errorf(400, "BadRequestException", "%s is not a valid ARN.", arn)
	}
	if cl := s.st.Clusters[account+"/"+region+"/"+p[1]]; cl != nil {
		if cl.Doc["arn"] == arn {
			return child(cl.Doc, "tags"), nil
		}
		for seg, ids := range cl.Parts {
			field := kinds[seg].one + "Arn"
			if seg == idps {
				field = "identityProviderConfigArn"
			}
			for _, d := range ids {
				if d[field] == arn {
					return child(d, "tags"), nil
				}
			}
		}
	}
	return nil, core.Errorf(404, "NotFoundException", "Resource %s was not found.", arn)
}

func (s *Service) tagResource(_ *core.Ctx, r *http.Request, _ *cluster, in doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	maps.Copy(t, object(in["tags"]))
	return doc{}, nil
}

func (s *Service) untagResource(_ *core.Ctx, r *http.Request, _ *cluster, _ doc) (any, error) {
	t, err := s.tags(r.PathValue("arn"))
	if err != nil {
		return nil, err
	}
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(t, k)
	}
	return doc{}, nil
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

// Load restores state. Clusters that ran on k3s start their containers again
// (persisted state leaves them behind on shutdown), or turn FAILED when the
// engine or the container is gone.
func (s *Service) Load(b []byte) error {
	st := state{}
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	if st.Clusters == nil {
		st.Clusters = map[string]*cluster{}
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
	for k, cl := range st.Clusters {
		switch {
		case cl.Container == "":
		case engine:
			s.boot(k, cl, true)
		default:
			cl.Doc["status"], cl.Note = "FAILED", "Docker is not available, so the k3s container of this cluster cannot start"
		}
	}
	for _, cl := range old.Clusters {
		s.drop(cl)
	}
	return nil
}

func (s *Service) Reset() { s.Load([]byte("{}")) }

// Close stops k3s work and removes the clusters' containers, unless state is
// persisted: the next run starts them again.
func (s *Service) Close() error {
	s.stop()
	if cfg := s.hub.Cfg; cfg.Persist || cfg.Storage != "" && cfg.Storage != "memory" {
		return nil
	}
	s.mu.Lock()
	var names []string
	for _, cl := range s.st.Clusters {
		if cl.Container != "" {
			names = append(names, cl.Container)
		}
	}
	s.mu.Unlock()
	for _, n := range names {
		s.hub.Docker().Remove(context.Background(), n)
	}
	return nil
}

// Resources lists the clusters for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	t := core.Table{Name: "Clusters", Columns: []string{"Name", "Status", "Version", "Endpoint", "Node groups", "Region", "Account"}}
	for _, k := range core.SortedKeys(s.st.Clusters) {
		cl, p := s.st.Clusters[k], strings.SplitN(k, "/", 3)
		t.Rows = append(t.Rows, []string{p[2], str(cl.Doc["status"]), str(cl.Doc["version"]), str(cl.Doc["endpoint"]),
			strconv.Itoa(len(cl.Parts["node-groups"])), p[1], p[0]})
	}
	return []core.Table{t}
}

// Inspect returns a cluster with its parts and pogi's side: its container
// and pogiNote (never its k3s token).
func (s *Service) Inspect(_, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Clusters) {
		if cl := s.st.Clusters[k]; cl.Doc["name"] == id {
			b, _ := json.Marshal(doc{"cluster": cl.Doc, "parts": cl.Parts, "accessPolicies": cl.Policies, "container": cl.Container, "pogiNote": cl.Note})
			return json.RawMessage(b)
		}
	}
	return nil
}

// ---- helpers ----

func invalid(format string, a ...any) error {
	return core.Errorf(400, "InvalidParameterException", format, a...)
}

func str(v any) string { s, _ := v.(string); return s }
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

// merge sets the members of in on d; objects merge one level deep.
func merge(d, in doc) {
	for k, v := range in {
		if m, ok := v.(doc); ok {
			if old, ok := d[k].(doc); ok {
				maps.Copy(old, m)
				continue
			}
		}
		d[k] = v
	}
}

// created is a new resource as its caller sees it: still CREATING.
func created(d doc) doc {
	out := maps.Clone(d)
	if out["status"] != nil {
		out["status"] = "CREATING"
	}
	return out
}
