package network

import (
	"net/netip"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/core"
)

// ---- shared checks ----

func oldProps(old map[string]any) map[string]any {
	p, _ := old["properties"].(map[string]any)
	return p
}

// enum fails unless props[key] is one of the values; when absent it takes the first one.
func enum(props map[string]any, key string, vals ...string) error {
	v, ok := props[key]
	if !ok {
		props[key] = vals[0]
		return nil
	}
	if !slices.ContainsFunc(vals, func(x string) bool { return strings.EqualFold(x, str(v)) }) {
		return bad("InvalidRequestFormat", "Invalid value '%v' for %s. Valid values are: %s.", v, key, strings.Join(vals, ", "))
	}
	return nil
}

// cidr parses a network prefix in CIDR notation, host bits clear.
func cidr(v any, where string) (netip.Prefix, error) {
	p, err := netip.ParsePrefix(str(v))
	if err != nil || p != p.Masked() {
		return p, bad("InvalidCIDRNotation", "The address prefix %v in resource %s is not formatted correctly. It should follow CIDR notation, for example 10.0.0.0/24 for IPv4.", v, where)
	}
	return p, nil
}

// keep takes a server-assigned property from the stored resource, or makes one.
func keep(props, old map[string]any, key string, mk func() any) {
	if v, ok := old[key]; ok {
		props[key] = v
	} else {
		props[key] = mk()
	}
}

func newGUID() any { return guid() }

// ---- virtual networks ----

func fillVNet(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	pre, _ := sub(p, "addressSpace")["addressPrefixes"].([]any)
	if len(pre) == 0 {
		return bad("InvalidRequestFormat", "Virtual network %s must have at least one address prefix.", id.Name())
	}
	for _, v := range pre {
		if _, err := cidr(v, id.Name()); err != nil {
			return err
		}
	}
	def(p, "enableDdosProtection", false)
	def(p, "virtualNetworkPeerings", []any{})
	keep(p, oldProps(old), "resourceGuid", newGUID)
	return nil
}

func fillSubnet(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	vkey := lower(id.Parent())
	vnet := s.st.Res[vkey]
	space := prefixes(sub(vnet, "properties"), "addressSpace")
	var own []netip.Prefix
	for _, v := range append([]any{p["addressPrefix"]}, asList(p["addressPrefixes"])...) {
		if v == nil {
			continue
		}
		pf, err := cidr(v, id.Name())
		if err != nil {
			return err
		}
		if !within(space, pf) {
			return bad("NetcfgInvalidSubnet", "Subnet '%s' is not valid in virtual network '%s'.", id.Name(), id.Parent().Name())
		}
		own = append(own, pf)
	}
	if len(own) == 0 {
		return bad("InvalidRequestFormat", "Subnet %s must have an address prefix.", id.Name())
	}
	for _, ck := range s.childKeys(vkey, ns+"virtualNetworks/subnets") {
		if ck == lower(id) {
			continue
		}
		for _, other := range subnetPrefixes(sub(s.st.Res[ck], "properties")) {
			for _, pf := range own {
				if pf.Overlaps(other) {
					return bad("NetcfgSubnetRangesOverlap", "Subnet '%s' is not valid because its IP address range overlaps with that of an existing subnet in virtual network '%s'.", id.Name(), id.Parent().Name())
				}
			}
		}
	}
	def(p, "privateEndpointNetworkPolicies", "Disabled")
	def(p, "privateLinkServiceNetworkPolicies", "Enabled")
	def(p, "delegations", []any{})
	return nil
}

// ---- network security groups ----

func fillRule(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	for key, vals := range map[string][]string{"protocol": {"Tcp", "Udp", "Icmp", "Esp", "*", "Ah"}, "access": {"Allow", "Deny"}, "direction": {"Inbound", "Outbound"}} {
		if _, ok := p[key]; !ok {
			return bad("InvalidRequestFormat", "Security rule %s must have the property %s.", id.Name(), key)
		}
		if err := enum(p, key, vals...); err != nil {
			return err
		}
	}
	prio, ok := p["priority"].(float64)
	if !ok || prio < 100 || prio > 4096 {
		return bad("SecurityRuleInvalidPriority", "Security rule %s has an invalid priority. The priority must be between 100 and 4096.", id.Name())
	}
	pkey := lower(id.Parent())
	for _, ck := range s.childKeys(pkey, ns+"networkSecurityGroups/securityRules") {
		o := s.st.Res[ck]
		op := sub(o, "properties")
		if ck != lower(id) && op["priority"] == p["priority"] && strings.EqualFold(str(op["direction"]), str(p["direction"])) {
			return bad("SecurityRuleConflict", "Security rule %s conflicts with rule %s. Rules cannot have the same Priority and Direction.", id.Name(), str(o["name"]))
		}
	}
	return nil
}

func viewNSG(s *Service, res map[string]any) {
	id := str(res["id"])
	rule := func(name string, prio int, dir, access, src, dst string) any {
		return map[string]any{"id": id + "/defaultSecurityRules/" + name, "name": name, "type": ns + "networkSecurityGroups/defaultSecurityRules",
			"properties": map[string]any{"provisioningState": "Succeeded", "protocol": "*", "access": access, "direction": dir, "priority": prio,
				"description": name, "sourcePortRange": "*", "destinationPortRange": "*", "sourceAddressPrefix": src, "destinationAddressPrefix": dst,
				"sourceAddressPrefixes": []any{}, "destinationAddressPrefixes": []any{}}}
	}
	sub(res, "properties")["defaultSecurityRules"] = []any{
		rule("AllowVnetInBound", 65000, "Inbound", "Allow", "VirtualNetwork", "VirtualNetwork"),
		rule("AllowAzureLoadBalancerInBound", 65001, "Inbound", "Allow", "AzureLoadBalancer", "*"),
		rule("DenyAllInBound", 65500, "Inbound", "Deny", "*", "*"),
		rule("AllowVnetOutBound", 65000, "Outbound", "Allow", "VirtualNetwork", "VirtualNetwork"),
		rule("AllowInternetOutBound", 65001, "Outbound", "Allow", "*", "Internet"),
		rule("DenyAllOutBound", 65500, "Outbound", "Deny", "*", "*"),
	}
}

// ---- public IP addresses ----

// fakeIP hands out the next address of a range that is not routable: 198.18.0.0/15, or 2001:db8::/32.
// ponytail: the counter wraps after 131000 addresses, and freed ones are not reused.
func (s *Service) fakeIP(v6 bool) string {
	s.st.NextIP++
	n := s.st.NextIP%131000 + 1
	if v6 {
		b := netip.MustParseAddr("2001:db8::").As16()
		b[13], b[14], b[15] = byte(n>>16), byte(n>>8), byte(n)
		return netip.AddrFrom16(b).String()
	}
	v := uint32(0xC6120000) + uint32(n)
	return netip.AddrFrom4([4]byte{byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}).String()
}

func fillPublicIP(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	if err := enum(p, "publicIPAddressVersion", "IPv4", "IPv6"); err != nil {
		return err
	}
	if err := enum(p, "publicIPAllocationMethod", "Dynamic", "Static"); err != nil {
		return err
	}
	def(p, "idleTimeoutInMinutes", float64(4))
	def(p, "ipTags", []any{})
	def(res, "sku", map[string]any{"name": "Basic", "tier": "Regional"})
	keep(p, oldProps(old), "resourceGuid", newGUID)
	// ponytail: a Dynamic address is allocated at once; Azure waits until it is attached.
	if ip := str(oldProps(old)["ipAddress"]); ip != "" && strings.EqualFold(str(oldProps(old)["publicIPAddressVersion"]), str(p["publicIPAddressVersion"])) {
		p["ipAddress"] = ip
	} else {
		p["ipAddress"] = s.fakeIP(str(p["publicIPAddressVersion"]) == "IPv6")
	}
	if ds, ok := p["dnsSettings"].(map[string]any); ok && str(ds["domainNameLabel"]) != "" {
		label, loc := strings.ToLower(str(ds["domainNameLabel"])), str(res["location"])
		for key, o := range s.st.Res {
			if d, _ := oldProps(o)["dnsSettings"].(map[string]any); key != lower(id) && strings.EqualFold(str(o["location"]), loc) && strings.EqualFold(str(d["domainNameLabel"]), label) {
				return bad("DnsRecordInUse", "DNS record %s.%s.cloudapp.azure.com is already used by another public IP or Traffic Manager profile.", label, loc)
			}
		}
		ds["fqdn"] = label + "." + strings.ToLower(loc) + ".cloudapp.azure.com"
	}
	return nil
}

// ---- network interfaces ----

func fillNIC(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	cfgs, _ := p["ipConfigurations"].([]any)
	if len(cfgs) == 0 {
		return bad("InvalidRequestFormat", "Network interface %s must have at least one IP configuration.", id.Name())
	}
	// addresses in use, per subnet, by the other interfaces
	used := map[string]map[netip.Addr]bool{}
	mark := func(subnet, addr string) {
		if a, err := netip.ParseAddr(addr); err == nil {
			if used[subnet] == nil {
				used[subnet] = map[netip.Addr]bool{}
			}
			used[subnet][a] = true
		}
	}
	for key, o := range s.st.Res {
		if key != lower(id) && strings.EqualFold(str(o["type"]), ns+"networkInterfaces") {
			for _, c := range asList(oldProps(o)["ipConfigurations"]) {
				cp := oldProps(c.(map[string]any))
				mark(strings.ToLower(str(refID(cp["subnet"]))), str(cp["privateIPAddress"]))
			}
		}
	}
	before := map[string]map[string]any{} // this NIC's configurations as they were, by name
	for _, c := range asList(oldProps(old)["ipConfigurations"]) {
		if m, ok := c.(map[string]any); ok {
			before[str(m["name"])] = oldProps(m)
		}
	}
	for i, c := range cfgs {
		m, _ := c.(map[string]any)
		name := str(m["name"])
		if name == "" {
			return bad("InvalidRequestFormat", "An IP configuration of network interface %s has no name.", id.Name())
		}
		cp := sub(m, "properties")
		m["id"], m["type"], m["etag"] = id.String()+"/ipConfigurations/"+name, ns+"networkInterfaces/ipConfigurations", `W/"`+guid()+`"`
		cp["provisioningState"] = "Succeeded"
		def(cp, "primary", i == 0)
		if err := enum(cp, "privateIPAllocationMethod", "Dynamic", "Static"); err != nil {
			return err
		}
		subnet := str(refID(cp["subnet"]))
		if subnet == "" {
			return bad("InvalidRequestFormat", "IP configuration %s must reference a subnet.", name)
		}
		skey := strings.ToLower(subnet)
		addr, err := s.nicAddress(cp, before[name], subnetPrefixes(sub(s.st.Res[skey], "properties")), used[skey], subnet)
		if err != nil {
			return err
		}
		cp["privateIPAddress"], cp["privateIPAddressVersion"] = addr.String(), map[bool]string{true: "IPv4", false: "IPv6"}[addr.Is4()]
		mark(skey, addr.String())
	}
	def(p, "enableIPForwarding", false)
	def(p, "enableAcceleratedNetworking", false)
	def(p, "nicType", "Standard")
	def(p, "tapConfigurations", []any{})
	def(p, "dnsSettings", map[string]any{"dnsServers": []any{}, "appliedDnsServers": []any{}, "internalDomainNameSuffix": "pogi.internal.cloudapp.net"})
	old0 := oldProps(old)
	keep(p, old0, "macAddress", func() any {
		h := strings.ToUpper(core.NewID()[:6])
		return "00-0D-3A-" + h[:2] + "-" + h[2:4] + "-" + h[4:]
	})
	keep(p, old0, "resourceGuid", newGUID)
	return nil
}

func refID(v any) any { m, _ := v.(map[string]any); return m["id"] }

// nicAddress picks the private address of one IP configuration: the static one, if it is valid, or the one
// it had if still free, or the first free one of the subnet (Azure keeps the first four and the last for itself).
func (s *Service) nicAddress(cp, before map[string]any, pfx []netip.Prefix, used map[netip.Addr]bool, subnet string) (netip.Addr, error) {
	if str(cp["privateIPAllocationMethod"]) == "Static" {
		a, err := netip.ParseAddr(str(cp["privateIPAddress"]))
		if err != nil {
			return a, bad("InvalidRequestFormat", "Static IP configuration needs a valid privateIPAddress, got '%v'.", cp["privateIPAddress"])
		}
		for _, q := range pfx {
			if q.Contains(a) {
				if reserved(q, a) {
					return a, bad("PrivateIPAddressIsReserved", "Private IP address %s is reserved by Azure in subnet %s.", a, subnet)
				}
				if used[a] {
					return a, bad("PrivateIPAddressIsAllocated", "IP configuration is using the private IP address %s which is already allocated in subnet %s.", a, subnet)
				}
				return a, nil
			}
		}
		return a, bad("PrivateIPAddressNotInSubnet", "Private static IP address %s does not belong to the range of subnet prefix %s.", a, subnet)
	}
	v6 := strings.EqualFold(str(cp["privateIPAddressVersion"]), "IPv6")
	if a, err := netip.ParseAddr(str(before["privateIPAddress"])); err == nil && strings.EqualFold(str(refID(before["subnet"])), subnet) && !used[a] && a.Is6() == v6 {
		return a, nil
	}
	for _, q := range pfx {
		if q.Addr().Is6() != v6 {
			continue
		}
		a := q.Masked().Addr().Next().Next().Next().Next()
		for ; q.Contains(a); a = a.Next() {
			if !used[a] && !reserved(q, a) {
				return a, nil
			}
		}
	}
	return netip.Addr{}, bad("SubnetIsFull", "Subnet %s does not have enough free addresses.", subnet)
}

// reserved: Azure keeps the first four addresses of a subnet, and the last of an IPv4 one.
func reserved(p netip.Prefix, a netip.Addr) bool {
	first := p.Masked().Addr().Next().Next().Next()
	return !first.Less(a) || (a.Is4() && !p.Contains(a.Next()))
}

// ---- route tables ----

func fillRouteTable(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	def(p, "disableBgpRoutePropagation", false)
	keep(p, oldProps(old), "resourceGuid", newGUID)
	return nil
}

func fillRoute(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	if _, err := cidr(p["addressPrefix"], id.Name()); err != nil {
		return err
	}
	if _, ok := p["nextHopType"]; !ok {
		return bad("InvalidRequestFormat", "Route %s must have a nextHopType.", id.Name())
	}
	if err := enum(p, "nextHopType", "VirtualNetworkGateway", "VnetLocal", "Internet", "VirtualAppliance", "None"); err != nil {
		return err
	}
	if strings.EqualFold(str(p["nextHopType"]), "VirtualAppliance") {
		if _, err := netip.ParseAddr(str(p["nextHopIpAddress"])); err != nil {
			return bad("InvalidRequestFormat", "Route %s needs a valid nextHopIpAddress when the next hop is a VirtualAppliance.", id.Name())
		}
	}
	def(p, "hasBgpOverride", false)
	return nil
}

// ---- NAT gateways ----

func fillNAT(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	def(res, "sku", map[string]any{"name": "Standard"})
	def(p, "idleTimeoutInMinutes", float64(4))
	if t, _ := p["idleTimeoutInMinutes"].(float64); t < 4 || t > 120 {
		return bad("InvalidRequestFormat", "NAT gateway %s: idleTimeoutInMinutes must be between 4 and 120.", id.Name())
	}
	keep(p, oldProps(old), "resourceGuid", newGUID)
	return nil
}

// ---- Private DNS ----

func fillZone(s *Service, id arm.ID, old, res map[string]any) error {
	if n := id.Name(); n != strings.Trim(n, ".") || !strings.Contains(n, ".") {
		return bad("InvalidZoneName", "The zone name '%s' is not valid. A private zone name has at least two labels, like contoso.com.", n)
	}
	keep(sub(res, "properties"), oldProps(old), "internalId", func() any { return core.NewID() })
	return nil
}

func viewZone(s *Service, res map[string]any) {
	key := strings.ToLower(str(res["id"]))
	p := sub(res, "properties")
	records, links, reg := 0, 0, 0
	for _, r := range recordTypes {
		records += len(s.childKeys(key, ns+"privateDnsZones/"+r))
	}
	for _, lk := range s.childKeys(key, ns+"privateDnsZones/virtualNetworkLinks") {
		links++
		if b, _ := oldProps(s.st.Res[lk])["registrationEnabled"].(bool); b {
			reg++
		}
	}
	p["maxNumberOfRecordSets"], p["numberOfRecordSets"] = 25000, records
	p["maxNumberOfVirtualNetworkLinks"], p["numberOfVirtualNetworkLinks"] = 1000, links
	p["maxNumberOfVirtualNetworkLinksWithRegistration"], p["numberOfVirtualNetworkLinksWithRegistration"] = 100, reg
}

// soa puts the SOA record set a new zone starts with.
func (s *Service) soa(zone arm.ID) error {
	return s.put(s.kinds[strings.ToLower(ns+"privateDnsZones/SOA")], zone.Child("SOA", "@"), map[string]any{"properties": map[string]any{"ttl": 3600,
		"soaRecord": map[string]any{"host": "azureprivatedns.net", "email": "azureprivatedns-host.microsoft.com", "serialNumber": 1,
			"refreshTime": 3600, "retryTime": 300, "expireTime": 2419200, "minimumTtl": 10}}})
}

func fillLink(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	if refID(p["virtualNetwork"]) == nil {
		return bad("InvalidRequestFormat", "Virtual network link %s must reference a virtualNetwork.", id.Name())
	}
	def(p, "registrationEnabled", false)
	p["virtualNetworkLinkState"] = "Completed"
	return nil
}

func fillRecord(s *Service, id arm.ID, old, res map[string]any) error {
	p := sub(res, "properties")
	zone := id.Parent().Name()
	p["fqdn"], p["isAutoRegistered"] = strings.ToLower(id.Name())+"."+zone+".", false
	if id.Name() == "@" {
		p["fqdn"] = zone + "."
	}
	check := func(key, field string, v6 bool) error {
		for _, r := range asList(p[key]) {
			m, _ := r.(map[string]any)
			if a, err := netip.ParseAddr(str(m[field])); r != nil && (err != nil || a.Is6() != v6) {
				return bad("InvalidRequestFormat", "The %s record '%v' of %s is not a valid IPv%d address.", id.Type()[strings.LastIndexByte(id.Type(), '/')+1:], m[field], id.Name(), map[bool]int{false: 4, true: 6}[v6])
			}
		}
		return nil
	}
	switch id.Types[len(id.Types)-1] {
	case "A":
		return check("aRecords", "ipv4Address", false)
	case "AAAA":
		return check("aaaaRecords", "ipv6Address", true)
	case "CNAME":
		if str(sub(p, "cnameRecord")["cname"]) == "" {
			return bad("InvalidRequestFormat", "CNAME record set %s needs a cnameRecord.cname.", id.Name())
		}
	}
	return nil
}
