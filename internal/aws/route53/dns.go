package route53

// Private hosted zones answer DNS for the containers pogi starts in a VPC.
//
// Each VPC with a running container gets a DNS server of its own on the host
// (UDP and TCP, one port). Docker's HostConfig.Dns takes port 53 only, so an
// alpine/socat container on the pogi network forwards port 53 to that server
// and containers use its address as their DNS server. Names inside a private
// zone of the VPC are answered from the zone's records, read live; every other
// name goes to the host's own resolver (the nameserver lines of /etc/resolv.conf).
//
// Routing policies (weighted, failover, latency…) are not evaluated: of several
// record sets with one name and type, the first one in list order answers.

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

const (
	maxChain   = 8               // longest CNAME or alias chain followed
	udpMax     = 512             // largest UDP answer unless the client says EDNS
	tcpMax     = 65535           // largest DNS message over TCP
	upstreamTO = 3 * time.Second // time for the host's resolver to answer
	defaultTTL = 300
)

// vpcDNS is one VPC's DNS server and the forwarder that gives it port 53.
type vpcDNS struct {
	udp  net.PacketConn
	tcp  net.Listener
	fwd  string // forwarder container id
	addr string // forwarder address on the pogi network
}

func (v *vpcDNS) close() {
	v.udp.Close()
	v.tcp.Close()
}

// ContainerDNS is the Dns setting for a container in the VPC: the forwarder's
// address, or nil (the container keeps Docker's resolver) when there is no
// engine, the container shares the host's network, or the forwarder cannot
// start. Callers pass the result to docker.Container.Dns.
func ContainerDNS(hub *core.Hub, account, region, vpcID string) []string {
	s, _ := hub.Service("route53").(*Service)
	d := hub.Docker()
	if s == nil || vpcID == "" || !d.Available() || d.HostAddr() == "127.0.0.1" {
		return nil // a Linux engine without a VM: containers use the host's network, which has no Dns setting
	}
	addr, err := s.dnsFor(d, account, region, vpcID)
	if err != nil {
		hub.Log.Warn("route53 private zones will not resolve in "+vpcID, "err", err)
		return nil
	}
	return []string{addr}
}

type subnetVPC interface {
	VPCOfSubnet(account, region, subnet string) string
}

// SubnetDNS is ContainerDNS for a container placed in the first of these subnets.
func SubnetDNS(hub *core.Hub, account, region string, subnets []string) []string {
	if ec, ok := hub.Service("ec2").(subnetVPC); ok && len(subnets) > 0 {
		return ContainerDNS(hub, account, region, ec.VPCOfSubnet(account, region, subnets[0]))
	}
	return nil
}

// dnsFor starts the VPC's server and forwarder once and returns the address
// containers use.
func (s *Service) dnsFor(d *docker.Client, account, region, vpcID string) (string, error) {
	key := account + "/" + region + "/" + vpcID
	s.dmu.Lock()
	defer s.dmu.Unlock()
	if v := s.dns[key]; v != nil {
		return v.addr, nil
	}
	host := d.ListenAddr() // the forwarder calls it
	udp, err := net.ListenPacket("udp", net.JoinHostPort(host, "0"))
	if err != nil {
		return "", err
	}
	port := strconv.Itoa(udp.LocalAddr().(*net.UDPAddr).Port)
	tcp, err := net.Listen("tcp", net.JoinHostPort(host, port)) // same number, so one forwarder target serves both
	if err != nil {
		udp.Close()
		return "", err
	}
	v := &vpcDNS{udp: udp, tcp: tcp}
	core.Go("route53.dns.udp", func() { s.serveUDP(udp, account, region, vpcID) })
	core.Go("route53.dns.tcp", func() { s.serveTCP(tcp, account, region, vpcID) })
	ctx, name := context.Background(), docker.Name("route53", vpcID+"-dns")
	target := net.JoinHostPort(d.HostAddr(), port)
	err = d.RemoveLeftover(ctx, name) // this VPC's forwarder of an earlier run
	if err == nil {
		v.fwd, err = d.Run(ctx, &docker.Container{Name: name, Image: "alpine/socat", Service: "route53", Resource: vpcID + "-dns",
			Entrypoint: []string{"/bin/sh", "-c", "socat udp-listen:53,fork,reuseaddr udp:" + target + " & exec socat tcp-listen:53,fork,reuseaddr tcp:" + target}})
	}
	if err == nil {
		var in *docker.Info
		if in, err = d.Inspect(ctx, v.fwd); err == nil {
			for _, n := range in.NetworkSettings.Networks {
				v.addr = n.IPAddress
			}
			if v.addr == "" {
				err = errors.New("the DNS forwarder has no address")
			}
		}
	}
	if err != nil {
		v.close()
		if v.fwd != "" {
			d.Remove(ctx, v.fwd)
		}
		return "", err
	}
	s.dns[key] = v
	return v.addr, nil
}

// Close stops every DNS server and forwarder.
func (s *Service) Close() error {
	s.dmu.Lock()
	defer s.dmu.Unlock()
	for k, v := range s.dns {
		v.close()
		s.hub.Docker().Remove(context.Background(), v.fwd)
		delete(s.dns, k)
	}
	return nil
}

func (s *Service) serveUDP(pc net.PacketConn, account, region, vpcID string) {
	buf := make([]byte, 4096)
	for {
		n, from, err := pc.ReadFrom(buf)
		if err != nil {
			return
		}
		q := append([]byte(nil), buf[:n]...)
		core.Go("route53.dns.query", func() {
			if out := s.respond(q, account, region, vpcID, true); out != nil {
				pc.WriteTo(out, from)
			}
		})
	}
}

func (s *Service) serveTCP(ln net.Listener, account, region, vpcID string) {
	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		core.Go("route53.dns.conn", func() {
			defer c.Close()
			for {
				c.SetDeadline(time.Now().Add(30 * time.Second))
				var h [2]byte
				if _, err := io.ReadFull(c, h[:]); err != nil {
					return
				}
				q := make([]byte, int(h[0])<<8|int(h[1]))
				if _, err := io.ReadFull(c, q); err != nil {
					return
				}
				out := s.respond(q, account, region, vpcID, false)
				if out == nil || len(out) > tcpMax {
					return
				}
				if _, err := c.Write(append([]byte{byte(len(out) >> 8), byte(len(out))}, out...)); err != nil {
					return
				}
			}
		})
	}
}

// respond answers one query message, or returns nil for garbage that has no
// id to answer to.
func (s *Service) respond(q []byte, account, region, vpcID string, udp bool) []byte {
	var m dnsmessage.Message
	if err := m.Unpack(q); err != nil || m.Response {
		return nil
	}
	reply := dnsmessage.Message{Header: dnsmessage.Header{ID: m.ID, Response: true, OpCode: m.OpCode, RecursionDesired: m.RecursionDesired, RecursionAvailable: true},
		Questions: m.Questions}
	switch {
	case m.OpCode != 0:
		reply.RCode = dnsmessage.RCodeNotImplemented
	case len(m.Questions) != 1: // pogi answers one question at a time, as every resolver asks
		reply.RCode = dnsmessage.RCodeFormatError
	default:
		qn := m.Questions[0]
		name := strings.ToLower(qn.Name.String())
		s.mu.Lock()
		z := s.privateZone(account, region, vpcID, name)
		var out *result
		if z != nil {
			out = s.lookup(account, region, vpcID, z, name, qn.Type)
		}
		s.mu.Unlock()
		if z == nil {
			return s.forward(q, udp)
		}
		reply.Authoritative, reply.RCode = true, out.rcode
		reply.Answers, reply.Authorities = out.answers, out.authority
	}
	limit := tcpMax
	if udp {
		limit = udpMax
		for _, a := range m.Additionals {
			if a.Header.Type == dnsmessage.TypeOPT && int(a.Header.Class) > limit {
				limit = min(int(a.Header.Class), 1232)
			}
		}
	}
	b, err := reply.Pack()
	if err == nil && len(b) > limit {
		reply.Truncated, reply.Answers, reply.Authorities = true, nil, nil
		b, err = reply.Pack()
	}
	if err != nil {
		reply.RCode, reply.Answers, reply.Authorities = dnsmessage.RCodeServerFailure, nil, nil
		b, _ = reply.Pack()
	}
	return b
}

// privateZone is the longest private zone of the VPC that holds name; lock held.
func (s *Service) privateZone(account, region, vpcID, name string) *Zone {
	var best *Zone
	for _, z := range s.st.Zones {
		if z.Account == account && inZone(z, name) && (best == nil || len(z.Name) > len(best.Name)) {
			for _, v := range z.VPCs {
				if v.VPCId == vpcID && (v.VPCRegion == "" || v.VPCRegion == region) {
					best = z
				}
			}
		}
	}
	return best
}

// inZone reports whether name is the zone's apex or below it.
func inZone(z *Zone, name string) bool { return name == z.Name || strings.HasSuffix(name, "."+z.Name) }

type result struct {
	rcode              dnsmessage.RCode
	answers, authority []dnsmessage.Resource
}

// lookup answers name and typ from zone z and, for a CNAME or alias that leaves
// it, from the other zones of the VPC; lock held.
func (s *Service) lookup(account, region, vpcID string, z *Zone, name string, typ dnsmessage.Type) *result {
	out := &result{}
	for range maxChain {
		sets, exists := z.match(name)
		if !exists {
			out.rcode = dnsmessage.RCodeNameError
			out.authority = soaOf(z)
			return out
		}
		set, cname := pick(sets, typ)
		if set == nil && cname != nil && typ != dnsmessage.TypeCNAME {
			out.answers = append(out.answers, records(name, *cname, dnsmessage.TypeCNAME)...)
			next, ok := firstTarget(cname)
			if !ok {
				return out
			}
			name = strings.ToLower(next)
			if z = s.privateZone(account, region, vpcID, name); z == nil {
				return out // it leaves the private zones: the client asks on
			}
			continue
		}
		if set == nil {
			out.authority = soaOf(z) // the name exists, the type does not
			return out
		}
		if a := set.AliasTarget; a != nil {
			// an alias answers as the record it points to, under this name
			target := fqdn(a.DNSName)
			tz := s.privateZone(account, region, vpcID, target)
			if tz == nil {
				return out
			}
			sub := s.lookup(account, region, vpcID, tz, target, typ)
			for _, r := range sub.answers {
				r.Header.Name = mustName(name)
				out.answers = append(out.answers, r)
			}
			return out
		}
		out.answers = append(out.answers, records(name, *set, typ)...)
		return out
	}
	return out
}

// match returns the record sets for name: its own, or when it has none, those
// of the closest wildcard. exists is false when neither is there.
func (z *Zone) match(name string) (sets []RRSet, exists bool) {
	for _, rs := range z.Records {
		if rs.Name == name {
			sets = append(sets, rs)
		}
	}
	if len(sets) > 0 {
		return sets, true
	}
	if z.hasBelow(name) { // an empty non-terminal: the name exists, it has no records
		return nil, true
	}
	for n := name; n != z.Name; {
		_, n, _ = strings.Cut(n, ".")
		for _, rs := range z.Records {
			if rs.Name == "\\052."+n || rs.Name == "*."+n {
				sets = append(sets, rs)
			}
		}
		if len(sets) > 0 {
			return sets, true
		}
	}
	return nil, false
}

func (z *Zone) hasBelow(name string) bool {
	for _, rs := range z.Records {
		if strings.HasSuffix(rs.Name, "."+name) {
			return true
		}
	}
	return false
}

// pick chooses the set of the asked type, else the name's CNAME. The first of
// several (a routing policy) wins.
func pick(sets []RRSet, typ dnsmessage.Type) (set, cname *RRSet) {
	for i, rs := range sets {
		switch {
		case rs.Type == "CNAME":
			if cname == nil {
				cname = &sets[i]
			}
			if typ == dnsmessage.TypeCNAME && set == nil {
				set = &sets[i]
			}
		case set == nil && typeOf(rs.Type) == typ:
			set = &sets[i]
		}
	}
	return
}

func firstTarget(rs *RRSet) (string, bool) {
	if len(rs.ResourceRecords) == 0 {
		return "", false
	}
	return fqdn(rs.ResourceRecords[0].Value), true
}

func typeOf(t string) dnsmessage.Type {
	return map[string]dnsmessage.Type{"A": dnsmessage.TypeA, "AAAA": dnsmessage.TypeAAAA, "CNAME": dnsmessage.TypeCNAME, "TXT": dnsmessage.TypeTXT,
		"MX": dnsmessage.TypeMX, "SRV": dnsmessage.TypeSRV, "PTR": dnsmessage.TypePTR, "NS": dnsmessage.TypeNS, "SOA": dnsmessage.TypeSOA}[t]
}

func mustName(n string) dnsmessage.Name {
	nm, _ := dnsmessage.NewName(n)
	return nm
}

func soaOf(z *Zone) []dnsmessage.Resource {
	for _, rs := range z.Records {
		if rs.Name == z.Name && rs.Type == "SOA" {
			return records(z.Name, rs, dnsmessage.TypeSOA)
		}
	}
	return nil
}

// records turns the values of a set into DNS records owned by name. A value it
// cannot read is left out.
func records(name string, rs RRSet, _ dnsmessage.Type) []dnsmessage.Resource {
	ttl := uint32(defaultTTL)
	if rs.TTL != nil && *rs.TTL >= 0 && *rs.TTL < 1<<31 {
		ttl = uint32(*rs.TTL)
	}
	h := dnsmessage.ResourceHeader{Name: mustName(name), Type: typeOf(rs.Type), Class: dnsmessage.ClassINET, TTL: ttl}
	var out []dnsmessage.Resource
	for _, v := range rs.ResourceRecords {
		if b := body(rs.Type, v.Value); b != nil {
			out = append(out, dnsmessage.Resource{Header: h, Body: b})
		}
	}
	return out
}

func body(typ, v string) dnsmessage.ResourceBody {
	f := strings.Fields(v)
	num := func(i int) uint16 { n, _ := strconv.ParseUint(f[i], 10, 16); return uint16(n) }
	switch typ {
	case "A", "AAAA":
		a, err := netip.ParseAddr(strings.TrimSpace(v))
		switch {
		case err != nil:
		case typ == "A" && a.Is4():
			return &dnsmessage.AResource{A: a.As4()}
		case typ == "AAAA" && a.Is6():
			return &dnsmessage.AAAAResource{AAAA: a.As16()}
		}
	case "CNAME":
		if n, err := dnsmessage.NewName(fqdn(v)); err == nil {
			return &dnsmessage.CNAMEResource{CNAME: n}
		}
	case "NS":
		if n, err := dnsmessage.NewName(fqdn(v)); err == nil {
			return &dnsmessage.NSResource{NS: n}
		}
	case "PTR":
		if n, err := dnsmessage.NewName(fqdn(v)); err == nil {
			return &dnsmessage.PTRResource{PTR: n}
		}
	case "TXT":
		return &dnsmessage.TXTResource{TXT: txtStrings(v)}
	case "MX":
		if n, err := dnsmessage.NewName(fqdn(last(f, 2))); err == nil && len(f) == 2 {
			return &dnsmessage.MXResource{Pref: num(0), MX: n}
		}
	case "SRV":
		if n, err := dnsmessage.NewName(fqdn(last(f, 4))); err == nil && len(f) == 4 {
			return &dnsmessage.SRVResource{Priority: num(0), Weight: num(1), Port: num(2), Target: n}
		}
	case "SOA":
		if len(f) == 7 {
			ns, e1 := dnsmessage.NewName(fqdn(f[0]))
			mb, e2 := dnsmessage.NewName(fqdn(f[1]))
			u := func(i int) uint32 { n, _ := strconv.ParseUint(f[i], 10, 32); return uint32(n) }
			if e1 == nil && e2 == nil {
				return &dnsmessage.SOAResource{NS: ns, MBox: mb, Serial: u(2), Refresh: u(3), Retry: u(4), Expire: u(5), MinTTL: u(6)}
			}
		}
	}
	return nil
}

func last(f []string, n int) string {
	if len(f) != n {
		return ""
	}
	return f[n-1]
}

// txtStrings splits a TXT value of quoted strings ("a" "b") into its strings,
// each at most 255 bytes; an unquoted value is one string.
func txtStrings(v string) []string {
	var out []string
	for rest := strings.TrimSpace(v); strings.HasPrefix(rest, `"`); {
		var sb strings.Builder
		i := 1
		for ; i < len(rest) && rest[i] != '"'; i++ {
			if rest[i] == '\\' && i+1 < len(rest) {
				i++
			}
			sb.WriteByte(rest[i])
		}
		out = append(out, sb.String())
		rest = strings.TrimSpace(rest[min(i+1, len(rest)):])
	}
	if len(out) == 0 {
		out = []string{v}
	}
	for i, s := range out {
		out[i] = s[:min(len(s), 255)]
	}
	return out
}

// forward asks the host's resolver and returns its answer untouched; when none
// can be reached the client gets SERVFAIL.
func (s *Service) forward(q []byte, udp bool) []byte {
	ns := s.resolvers
	if ns == nil {
		ns = systemResolvers()
	}
	for _, addr := range ns {
		if out, err := ask(addr, q, udp); err == nil {
			return out
		}
	}
	var m dnsmessage.Message
	if m.Unpack(q) != nil {
		return nil
	}
	m.Response, m.RCode, m.Answers, m.Authorities, m.Additionals = true, dnsmessage.RCodeServerFailure, nil, nil, nil
	b, _ := m.Pack()
	return b
}

// ask sends q to one resolver over UDP, or over TCP when the client came by TCP.
func ask(addr string, q []byte, udp bool) ([]byte, error) {
	deadline := time.Now().Add(upstreamTO)
	if udp {
		c, err := net.DialTimeout("udp", addr, upstreamTO)
		if err != nil {
			return nil, err
		}
		c.SetDeadline(deadline)
		_, err = c.Write(q)
		buf := make([]byte, 4096)
		n := 0
		if err == nil {
			n, err = c.Read(buf)
		}
		c.Close()
		if err != nil {
			return nil, err
		}
		return buf[:n], nil // a cut-short answer goes on as it is: the client asks again by TCP
	}
	c, err := net.DialTimeout("tcp", addr, upstreamTO)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	c.SetDeadline(deadline)
	if _, err := c.Write(append([]byte{byte(len(q) >> 8), byte(len(q))}, q...)); err != nil {
		return nil, err
	}
	var h [2]byte
	if _, err := io.ReadFull(c, h[:]); err != nil {
		return nil, err
	}
	out := make([]byte, int(h[0])<<8|int(h[1]))
	_, err = io.ReadFull(c, out)
	return out, err
}

// systemResolvers are the nameserver lines of /etc/resolv.conf as host:port.
// There are none where the file does not exist (Windows).
func systemResolvers() []string {
	b, err := os.ReadFile("/etc/resolv.conf")
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(string(b), "\n") {
		if f := strings.Fields(l); len(f) >= 2 && f[0] == "nameserver" {
			out = append(out, net.JoinHostPort(strings.SplitN(f[1], "%", 2)[0], "53"))
		}
	}
	return out
}
