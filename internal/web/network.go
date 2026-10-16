package web

// EC2 and VPC views: the instances with their state, and the tree of VPCs
// with their subnets, instances and security groups.

import "net/http"

type instNode struct {
	ID, State, Type, IP, Image string
}

type subnetNode struct {
	ID, CIDR, Zone string
	Instances      []instNode
}

type vpcNode struct {
	ID, CIDR, Region, Account string
	Default                   bool
	Subnets                   []subnetNode
	Groups                    [][2]string // id, name
	Loose                     []instNode  // instances with no known subnet
}

func (ui *UI) networkPage(w http.ResponseWriter, r *http.Request) {
	insts := map[string][]instNode{} // by subnet id
	loose := map[string][]instNode{} // by vpc id
	var all []instNode
	for _, i := range ui.rows(r, "ec2", "Instances") { // id, state, type, image, ip
		n := instNode{i[0], i[1], i[2], i[4], i[3]}
		all = append(all, n)
		m := ui.inspectMap("ec2", "Instances", i[0])
		if s := dig(m, "SubnetId"); s != "" {
			insts[s] = append(insts[s], n)
		} else {
			loose[dig(m, "VpcId")] = append(loose[dig(m, "VpcId")], n)
		}
	}
	var vpcs []vpcNode
	for _, v := range ui.rows(r, "ec2", "VPCs") { // id, cidr, default, region, account
		n := vpcNode{ID: v[0], CIDR: v[1], Default: v[2] == "true", Region: v[3], Account: v[4], Loose: loose[v[0]]}
		for _, s := range ui.rows(r, "ec2", "Subnets") { // id, vpc, cidr, zone
			if s[1] == v[0] {
				n.Subnets = append(n.Subnets, subnetNode{s[0], s[2], s[3], insts[s[0]]})
			}
		}
		for _, g := range ui.rows(r, "ec2", "Security groups") { // id, name, vpc
			if g[2] == v[0] {
				n.Groups = append(n.Groups, [2]string{g[0], g[1]})
			}
		}
		vpcs = append(vpcs, n)
	}
	ui.xrender(w, r, "x-network", "EC2 and VPC", "ec2", map[string]any{"VPCs": vpcs, "Instances": all})
}
