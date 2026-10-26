package web

// Kubernetes in the console: short "Create cluster" forms for EKS, GKE and AKS, and the
// "Connect with kubectl" part of a cluster's drawer, with the kubeconfig download.

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/aws/eks"
	"github.com/nrndev/pogi/internal/core"
)

// kubeSvcs are the services with clusters, and the CLI command that writes a kubeconfig for each.
var kubeSvcs = map[string]bool{"eks": true, "container": true, "azaks": true}

var (
	locationRE = regexp.MustCompile(`^[a-z][a-z0-9-]{1,40}$`)
	groupRE    = regexp.MustCompile(`^[A-Za-z0-9._()-]{1,90}$`)
)

func init() {
	specials["eks"] = append(specials["eks"], special{Op: "CreateCluster", Title: "Create cluster",
		Fields: func(ui *UI) []core.Field {
			versions, def := eks.Versions()
			ver := withDefault(req(enum("version", versions...)), def)
			subnets := list("subnetIds", enum("", ui.choices("ec2", "Subnets")...))
			subnets.Label, subnets.Help = "Subnets", "Optional. With none picked, pogi makes a VPC and two subnets first."
			return []core.Field{req(str("name")), ver, subnets, withDefault(req(str("roleArn")), "arn:aws:iam::{account}:role/eks")}
		},
		Submit: func(ui *UI, sc scope, in map[string]any) error { return ui.eksCreate(sc, in) },
		CLI: func(_ string, in map[string]any) string {
			c := "pogi aws eks create-cluster --name " + shellArg(str1(in, "name")) + " \\\n  --kubernetes-version " + shellArg(str1(in, "version")) +
				" \\\n  --role-arn " + shellArg(str1(in, "roleArn"))
			if ids, _ := in["subnetIds"].([]any); len(ids) > 0 {
				var s []string
				for _, id := range ids {
					s = append(s, fmt.Sprint(id))
				}
				c += " \\\n  --resources-vpc-config subnetIds=" + strings.Join(s, ",")
			}
			return c
		}})
	specials["container"] = append(specials["container"], special{Op: "CreateCluster", Title: "Create cluster",
		Fields: fixed(req(str("name")), withDefault(req(str("location")), "{location}"), withDefault(req(num("nodeCount")), "1")),
		Submit: func(ui *UI, sc scope, in map[string]any) error {
			name, loc, n := strings.TrimSpace(str1(in, "name")), strings.TrimSpace(str1(in, "location")), nodeCount(in)
			if !locationRE.MatchString(loc) {
				return fmt.Errorf("Location %q is not valid. Use a region or zone such as us-central1.", loc)
			}
			if err := checkNodes(n); err != nil {
				return err
			}
			body := map[string]any{"cluster": map[string]any{"name": name, "initialNodeCount": n}}
			return ui.rest(sc, "container", http.MethodPost, "/v1/projects/"+url.PathEscape(sc.project())+"/locations/"+loc+"/clusters", body)
		},
		CLI: func(ep string, in map[string]any) string {
			return curlCmd("POST", ep+"/v1/projects/"+core.DefaultProject+"/locations/"+str1(in, "location")+"/clusters",
				[]byte(`{"cluster":{"name":`+strconv.Quote(str1(in, "name"))+`,"initialNodeCount":`+strconv.FormatInt(nodeCount(in), 10)+`}}`))
		}})
	specials["azaks"] = append(specials["azaks"], special{Op: "CreateCluster", Title: "Create cluster",
		Fields: fixed(req(str("name")), withDefault(req(str("resourceGroup")), "{group}"), withDefault(req(str("location")), "{azlocation}"),
			withDefault(req(num("nodeCount")), "1")),
		Submit: func(ui *UI, sc scope, in map[string]any) error { return ui.aksCreate(sc, in) },
		CLI: func(_ string, in map[string]any) string {
			return fmt.Sprintf("az aks create --resource-group %s --name %s --location %s --node-count %d",
				shellArg(str1(in, "resourceGroup")), shellArg(str1(in, "name")), shellArg(str1(in, "location")), nodeCount(in))
		}})
}

func nodeCount(in map[string]any) int64 { n, _ := in["nodeCount"].(int64); return n }

func checkNodes(n int64) error {
	if n < 1 || n > 100 {
		return fmt.Errorf("Node count must be from 1 to 100.")
	}
	return nil
}

// ---- EKS ----

// ec2Query sends a Query-protocol call to pogi's EC2 as the console and decodes the XML answer.
func (ui *UI) ec2Query(sc scope, params url.Values, out any) error {
	params.Set("Version", "2016-11-15")
	req, _ := http.NewRequest(http.MethodPost, "http://localhost/", strings.NewReader(params.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set(core.ConsoleHeader, "1")
	status, resp := ui.hub.DoServiceAs("ec2", sc.acct(), sc.region(ui.hub.Cfg.Region), req)
	if status >= 300 {
		var e struct {
			Message string `xml:"Errors>Error>Message"`
		}
		xml.Unmarshal(resp, &e)
		return fmt.Errorf("EC2 %s failed (%d): %s", params.Get("Action"), status, e.Message)
	}
	if out == nil {
		return nil
	}
	return xml.Unmarshal(resp, out)
}

// eksCreate makes the cluster. With no subnets picked it makes a VPC and two subnets first, and
// removes them again when a later step fails.
func (ui *UI) eksCreate(sc scope, in map[string]any) error {
	ids := []any{}
	if l, ok := in["subnetIds"].([]any); ok {
		ids = l
	}
	made := ""
	if len(ids) == 0 && ui.hub.Service("ec2") != nil {
		var undo []url.Values
		defer func() {
			for i := len(undo) - 1; i >= 0 && made == ""; i-- { // made is set only on success
				ui.ec2Query(sc, undo[i], nil)
			}
		}()
		var vpc struct {
			ID string `xml:"vpc>vpcId"`
		}
		if err := ui.ec2Query(sc, url.Values{"Action": {"CreateVpc"}, "CidrBlock": {"10.0.0.0/16"}}, &vpc); err != nil {
			return err
		}
		undo = append(undo, url.Values{"Action": {"DeleteVpc"}, "VpcId": {vpc.ID}})
		for i, az := range []string{"a", "b"} {
			var sn struct {
				ID string `xml:"subnet>subnetId"`
			}
			err := ui.ec2Query(sc, url.Values{"Action": {"CreateSubnet"}, "VpcId": {vpc.ID}, "CidrBlock": {fmt.Sprintf("10.0.%d.0/24", i+1)},
				"AvailabilityZone": {sc.region(ui.hub.Cfg.Region) + az}}, &sn)
			if err != nil {
				return err
			}
			undo = append(undo, url.Values{"Action": {"DeleteSubnet"}, "SubnetId": {sn.ID}})
			ids = append(ids, sn.ID)
		}
		made = vpc.ID
	}
	body := map[string]any{"name": strings.TrimSpace(str1(in, "name")), "version": in["version"], "roleArn": strings.TrimSpace(str1(in, "roleArn")),
		"resourcesVpcConfig": map[string]any{"subnetIds": ids}}
	if err := ui.rest(sc, "eks", http.MethodPost, "/clusters", body); err != nil {
		made = "" // undo the VPC
		return err
	}
	in["_msg"] = "Cluster " + str1(body, "name") + " created."
	if made != "" {
		in["_msg"] = fmt.Sprintf("Cluster %s created. No subnets were picked, so pogi first made VPC %s with two subnets.", str1(body, "name"), made)
	}
	return nil
}

// ---- AKS ----

// aksCreate makes the cluster, and its resource group first when the group does not exist.
func (ui *UI) aksCreate(sc scope, in map[string]any) error {
	name, group, loc, n := strings.TrimSpace(str1(in, "name")), strings.TrimSpace(str1(in, "resourceGroup")), strings.TrimSpace(str1(in, "location")), nodeCount(in)
	if !groupRE.MatchString(group) {
		return fmt.Errorf("Resource group %q is not valid. Use letters, digits, dots, hyphens, underscores and brackets, up to 90 characters.", group)
	}
	if !locationRE.MatchString(loc) {
		return fmt.Errorf("Location %q is not valid. Use a region such as eastus.", loc)
	}
	if err := checkNodes(n); err != nil {
		return err
	}
	gpath := "/subscriptions/" + sc.sub() + "/resourceGroups/" + group
	madeGroup := false
	if status, _ := ui.armDo(http.MethodGet, gpath, nil); status == http.StatusNotFound {
		status, resp := ui.armDo(http.MethodPut, gpath, []byte(`{"location":`+strconv.Quote(loc)+`}`))
		if status >= 300 {
			return armError(status, resp)
		}
		madeGroup = true
	}
	body := fmt.Sprintf(`{"location":%q,"properties":{"dnsPrefix":%q,"agentPoolProfiles":[{"name":"nodepool1","count":%d,"vmSize":"Standard_DS2_v2","mode":"System"}]}}`, loc, name, n)
	if status, resp := ui.armDo(http.MethodPut, gpath+"/providers/Microsoft.ContainerService/managedClusters/"+url.PathEscape(name), []byte(body)); status >= 300 {
		if madeGroup {
			ui.armDo(http.MethodDelete, gpath, nil) // do not leave an empty group behind
		}
		return armError(status, resp)
	}
	in["_msg"] = "Cluster " + name + " created."
	if madeGroup {
		in["_msg"] = "Cluster " + name + " created. Resource group " + group + " did not exist, so pogi made it first."
	}
	return nil
}

// ---- connect ----

// kubeConnect is the "Connect with kubectl" part of a cluster's drawer.
type kubeConnect struct {
	Ready          bool
	Download, File string
	Fetch, Kubectl string
}

// kubeConnectFor builds that part for a cluster of the Clusters table, or nil for any other resource.
func (ui *UI) kubeConnectFor(r *http.Request, svc, table, id string) *kubeConnect {
	if !kubeSvcs[svc] || table != "Clusters" {
		return nil
	}
	file := id + ".kubeconfig"
	c := &kubeConnect{File: file, Download: "/_pogi/kubeconfig/" + svc + "/" + url.PathEscape(id),
		Kubectl: "kubectl --kubeconfig " + shellArg(file) + " get nodes"}
	_, err := ui.kubeconfig(svc, id)
	c.Ready = err == nil
	row := []string{}
	for _, x := range ui.rows(r, svc, "Clusters") {
		if x[0] == id {
			row = x
		}
	}
	at := func(i int) string {
		if i < len(row) {
			return row[i]
		}
		return ""
	}
	ep := "http://" + r.Host
	switch svc { // columns: EKS has region at 5, GKE location at 5 and project at 6, AKS location at 5 and group at 6
	case "eks":
		c.Fetch = "aws eks update-kubeconfig --name " + shellArg(id) + " --region " + shellArg(at(5)) + " --endpoint-url " + shellArg(ep) + " --kubeconfig " + shellArg(file)
	case "container":
		c.Fetch = "pogi kubeconfig gke " + shellArg(id) + " --project " + shellArg(at(6)) + " --location " + shellArg(at(5)) + " --endpoint " + shellArg(ep) + " > " + shellArg(file)
	default:
		c.Fetch = "az aks get-credentials --resource-group " + shellArg(at(6)) + " --name " + shellArg(id) + " --file " + shellArg(file)
	}
	return c
}

func (ui *UI) kubeconfig(svc, id string) (string, error) {
	k, ok := ui.hub.Service(svc).(interface{ Kubeconfig(string) (string, error) })
	if !kubeSvcs[svc] || !ok {
		return "", core.Errorf(http.StatusNotFound, "NotFound", "%s has no clusters.", svc)
	}
	return k.Kubeconfig(id)
}

// kubeconfigFile serves the download: GET /_pogi/kubeconfig/<service>/<cluster>.
func (ui *UI) kubeconfigFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	cfg, err := ui.kubeconfig(r.PathValue("service"), id)
	if err != nil {
		e := core.AsError(err)
		http.Error(w, e.Msg, e.Status)
		return
	}
	w.Header().Set("Content-Type", "application/yaml")
	w.Header().Set("Content-Disposition", "attachment; filename=\""+strings.Map(func(c rune) rune {
		if strings.ContainsRune(`"\/;`, c) || c < ' ' {
			return '_'
		}
		return c
	}, id)+".kubeconfig\"")
	w.Header().Set("Cache-Control", "no-store")
	fmt.Fprint(w, cfg)
}
