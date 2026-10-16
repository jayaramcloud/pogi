package web_test

import (
	"regexp"
	"strings"
	"testing"

	"github.com/nrndev/pogi"
)

func TestNetworkView(t *testing.T) {
	// no container engine: instances are metadata, which is all the view reads
	srv := pogi.Start(t, pogi.Config{Services: []string{"ec2"}})
	vpc := call(t, srv, "AmazonEC2", "CreateVpc", map[string]any{"CidrBlock": "10.9.0.0/16"})["Vpc"].(map[string]any)["VpcId"].(string)
	subnet := call(t, srv, "AmazonEC2", "CreateSubnet", map[string]any{"VpcId": vpc, "CidrBlock": "10.9.1.0/24"})["Subnet"].(map[string]any)["SubnetId"].(string)
	group := call(t, srv, "AmazonEC2", "CreateSecurityGroup", map[string]any{"GroupName": "web", "Description": "web tier", "VpcId": vpc})["GroupId"].(string)
	run := call(t, srv, "AmazonEC2", "RunInstances", map[string]any{"ImageId": "ami-0a1b2c3d4e5f60006", "InstanceType": "t3.micro", "MinCount": 1, "MaxCount": 1, "SubnetId": subnet})
	id := run["Instances"].([]any)[0].(map[string]any)["InstanceId"].(string)

	p := page(t, srv, "/_pogi/network")
	// the instance appears in the table and under its subnet, under its VPC, with its state
	if n := strings.Count(p, id); n < 4 { // two hrefs and two labels
		t.Errorf("instance %s shown %d times, want it in the table and the tree:\n%s", id, n, results(p))
	}
	tree := p[strings.Index(p, `<ul class="tree top">`):]
	order := regexp.MustCompile(vpc + `[\s\S]*?` + subnet + `[\s\S]*?` + id).FindString(tree)
	if order == "" {
		t.Errorf("the tree does not nest vpc > subnet > instance:\n%s", tree)
	}
	has(t, p, group, "web", "10.9.1.0/24", "10.9.0.0/16", `class="state `, "t3.micro")

	// stopping the instance shows in the view
	call(t, srv, "AmazonEC2", "StopInstances", map[string]any{"InstanceIds": []string{id}})
	if p := page(t, srv, "/_pogi/network"); !regexp.MustCompile(`class="state stop(ped|ping)"`).MatchString(p) {
		t.Errorf("the stopped instance still shows as running:\n%s", results(p))
	}
}
