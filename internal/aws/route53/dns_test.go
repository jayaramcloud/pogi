package route53

import (
	"net"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

func ttl(n int64) *int64 { return &n }

// dnsEnv is a service with one private zone for vpc-1 and a DNS listener on UDP and TCP for it.
func dnsEnv(t *testing.T, noUpstream ...bool) (s *Service, udp, tcp string) {
	t.Helper()
	s = New(nil)
	zone := func(id, name, vpcID string, recs ...RRSet) {
		s.st.Zones[id] = &Zone{Id: id, Account: "111111111111", Name: name, VPCs: []vpc{{VPCId: vpcID, VPCRegion: "us-east-1"}},
			Records: append([]RRSet{{Name: name, Type: "SOA", TTL: ttl(900), ResourceRecords: []rr{{"ns.x. host.x. 1 7200 900 1209600 86400"}}}}, recs...)}
	}
	zone("Z1", "corp.example.", "vpc-1",
		RRSet{Name: "db.corp.example.", Type: "A", TTL: ttl(30), ResourceRecords: []rr{{"10.0.0.5"}}},
		RRSet{Name: "db.corp.example.", Type: "TXT", ResourceRecords: []rr{{`"v=1" "two words"`}}},
		RRSet{Name: "web.corp.example.", Type: "CNAME", ResourceRecords: []rr{{"app.corp.example"}}},
		RRSet{Name: "app.corp.example.", Type: "CNAME", ResourceRecords: []rr{{"db.corp.example."}}},
		RRSet{Name: "\\052.wild.corp.example.", Type: "A", ResourceRecords: []rr{{"10.0.0.9"}}},
		RRSet{Name: "alias.corp.example.", Type: "A", AliasTarget: &aliasTarget{DNSName: "db.corp.example."}},
		RRSet{Name: "mail.corp.example.", Type: "MX", ResourceRecords: []rr{{"10 smtp.corp.example."}}},
		RRSet{Name: "_sip._tcp.corp.example.", Type: "SRV", ResourceRecords: []rr{{"1 2 5060 db.corp.example."}}},
		RRSet{Name: "loop1.corp.example.", Type: "CNAME", ResourceRecords: []rr{{"loop2.corp.example."}}},
		RRSet{Name: "loop2.corp.example.", Type: "CNAME", ResourceRecords: []rr{{"loop1.corp.example."}}},
		RRSet{Name: "big.corp.example.", Type: "TXT", ResourceRecords: []rr{{`"` + strings.Repeat("a", 250) + `"`}, {`"` + strings.Repeat("b", 250) + `"`}, {`"` + strings.Repeat("c", 250) + `"`}}})
	zone("Z2", "other.example.", "vpc-2", RRSet{Name: "x.other.example.", Type: "A", ResourceRecords: []rr{{"10.9.9.9"}}})

	up, err := net.ListenPacket("udp", "127.0.0.1:0") // a fake upstream resolver: answers every A query with 192.0.2.1
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { up.Close() })
	go func() {
		buf := make([]byte, 512)
		for {
			n, from, err := up.ReadFrom(buf)
			if err != nil {
				return
			}
			var m dnsmessage.Message
			if m.Unpack(buf[:n]) == nil {
				m.Response = true
				m.Answers = []dnsmessage.Resource{{Header: dnsmessage.ResourceHeader{Name: m.Questions[0].Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 5},
					Body: &dnsmessage.AResource{A: [4]byte{192, 0, 2, 1}}}}
				b, _ := m.Pack()
				up.WriteTo(b, from)
			}
		}
	}()
	s.resolvers = []string{up.LocalAddr().String()}
	if len(noUpstream) > 0 {
		s.resolvers = []string{} // none: not nil, so the host's are not read
	}

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { pc.Close(); ln.Close() })
	go s.serveUDP(pc, "111111111111", "us-east-1", "vpc-1")
	go s.serveTCP(ln, "111111111111", "us-east-1", "vpc-1")
	return s, pc.LocalAddr().String(), ln.Addr().String()
}

func query(t *testing.T, addr, name string, typ dnsmessage.Type) dnsmessage.Message {
	t.Helper()
	q, _ := (&dnsmessage.Message{Header: dnsmessage.Header{ID: 7, RecursionDesired: true},
		Questions: []dnsmessage.Question{{Name: mustName(name), Type: typ, Class: dnsmessage.ClassINET}}}).Pack()
	c, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	c.Write(q)
	buf := make([]byte, 4096)
	n, err := c.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	var m dnsmessage.Message
	if err := m.Unpack(buf[:n]); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDNSAnswers(t *testing.T) {
	_, udp, _ := dnsEnv(t)

	m := query(t, udp, "db.corp.example.", dnsmessage.TypeA)
	if m.RCode != 0 || len(m.Answers) != 1 || m.Answers[0].Header.TTL != 30 || m.Answers[0].Body.(*dnsmessage.AResource).A != [4]byte{10, 0, 0, 5} || !m.Authoritative {
		t.Fatalf("A: %+v", m)
	}
	// a CNAME chain ends at the A record, every link in the answer
	m = query(t, udp, "WEB.corp.example.", dnsmessage.TypeA)
	if len(m.Answers) != 3 || m.Answers[0].Header.Type != dnsmessage.TypeCNAME || m.Answers[2].Header.Type != dnsmessage.TypeA {
		t.Fatalf("chain: %+v", m.Answers)
	}
	m = query(t, udp, "anything.wild.corp.example.", dnsmessage.TypeA) // wildcard answers under the asked name
	if len(m.Answers) != 1 || m.Answers[0].Header.Name.String() != "anything.wild.corp.example." {
		t.Fatalf("wildcard: %+v", m)
	}
	m = query(t, udp, "alias.corp.example.", dnsmessage.TypeA)
	if len(m.Answers) != 1 || m.Answers[0].Header.Name.String() != "alias.corp.example." || m.Answers[0].Body.(*dnsmessage.AResource).A != [4]byte{10, 0, 0, 5} {
		t.Fatalf("alias: %+v", m.Answers)
	}
	m = query(t, udp, "db.corp.example.", dnsmessage.TypeTXT)
	if txt := m.Answers[0].Body.(*dnsmessage.TXTResource).TXT; len(txt) != 2 || txt[1] != "two words" {
		t.Fatalf("txt: %v", txt)
	}
	if m = query(t, udp, "mail.corp.example.", dnsmessage.TypeMX); m.Answers[0].Body.(*dnsmessage.MXResource).Pref != 10 {
		t.Fatalf("mx: %+v", m.Answers)
	}
	if m = query(t, udp, "_sip._tcp.corp.example.", dnsmessage.TypeSRV); m.Answers[0].Body.(*dnsmessage.SRVResource).Port != 5060 {
		t.Fatalf("srv: %+v", m.Answers)
	}
	if m = query(t, udp, "corp.example.", dnsmessage.TypeSOA); len(m.Answers) != 1 {
		t.Fatalf("soa: %+v", m)
	}
	// the name exists but has no AAAA: NOERROR, no answer, the SOA says so
	if m = query(t, udp, "db.corp.example.", dnsmessage.TypeAAAA); m.RCode != 0 || len(m.Answers) != 0 || len(m.Authorities) != 1 {
		t.Fatalf("nodata: %+v", m)
	}
	// a name in the zone without records hides the public one
	if m = query(t, udp, "nothing.corp.example.", dnsmessage.TypeA); m.RCode != dnsmessage.RCodeNameError || len(m.Authorities) != 1 {
		t.Fatalf("nxdomain: %+v", m)
	}
	// a loop stops after a few links instead of hanging
	if m = query(t, udp, "loop1.corp.example.", dnsmessage.TypeA); len(m.Answers) != maxChain {
		t.Fatalf("loop: %d answers", len(m.Answers))
	}
}

func TestDNSForwardsAndKeepsZonesApart(t *testing.T) {
	_, udp, _ := dnsEnv(t)
	// a zone of another VPC does not answer here: the name goes to the upstream
	m := query(t, udp, "x.other.example.", dnsmessage.TypeA)
	if len(m.Answers) != 1 || m.Answers[0].Body.(*dnsmessage.AResource).A != [4]byte{192, 0, 2, 1} || m.Authoritative {
		t.Fatalf("other VPC: %+v", m)
	}
	if m = query(t, udp, "example.org.", dnsmessage.TypeA); m.Answers[0].Body.(*dnsmessage.AResource).A != [4]byte{192, 0, 2, 1} {
		t.Fatalf("public: %+v", m)
	}
}

func TestDNSNoUpstreamIsServfail(t *testing.T) {
	_, udp, _ := dnsEnv(t, true)
	if m := query(t, udp, "example.org.", dnsmessage.TypeA); m.RCode != dnsmessage.RCodeServerFailure {
		t.Fatalf("want SERVFAIL, got %+v", m)
	}
}

func TestDNSTruncatesAndTCP(t *testing.T) {
	_, udp, tcp := dnsEnv(t)
	if m := query(t, udp, "big.corp.example.", dnsmessage.TypeTXT); !m.Truncated || len(m.Answers) != 0 {
		t.Fatalf("UDP answer over 512 bytes should be cut short: %+v", m)
	}
	c, err := net.Dial("tcp", tcp)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	q, _ := (&dnsmessage.Message{Header: dnsmessage.Header{ID: 9}, Questions: []dnsmessage.Question{{Name: mustName("big.corp.example."), Type: dnsmessage.TypeTXT, Class: dnsmessage.ClassINET}}}).Pack()
	c.Write(append([]byte{byte(len(q) >> 8), byte(len(q))}, q...))
	var h [2]byte
	if _, err := c.Read(h[:]); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, int(h[0])<<8|int(h[1]))
	for got := 0; got < len(buf); {
		n, err := c.Read(buf[got:])
		if err != nil {
			t.Fatal(err)
		}
		got += n
	}
	var m dnsmessage.Message
	if err := m.Unpack(buf); err != nil || m.Truncated || len(m.Answers) != 3 {
		t.Fatalf("TCP: %v %+v", err, m)
	}
}

func TestDNSRefusesOddQueries(t *testing.T) {
	s, _, _ := dnsEnv(t)
	two, _ := (&dnsmessage.Message{Header: dnsmessage.Header{ID: 1}, Questions: []dnsmessage.Question{
		{Name: mustName("a.corp.example."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}, {Name: mustName("b.corp.example."), Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}}}).Pack()
	var m dnsmessage.Message
	if m.Unpack(s.respond(two, "111111111111", "us-east-1", "vpc-1", true)); m.RCode != dnsmessage.RCodeFormatError {
		t.Fatalf("two questions: %+v", m)
	}
	if s.respond([]byte("garbage"), "111111111111", "us-east-1", "vpc-1", true) != nil {
		t.Fatal("garbage should get no answer")
	}
}

// records changed after the server started are seen at once
func TestDNSSeesNewRecords(t *testing.T) {
	s, udp, _ := dnsEnv(t)
	s.mu.Lock()
	z := s.st.Zones["Z1"]
	z.Records = append(z.Records, RRSet{Name: "new.corp.example.", Type: "A", ResourceRecords: []rr{{"10.1.1.1"}}})
	s.mu.Unlock()
	if m := query(t, udp, "new.corp.example.", dnsmessage.TypeA); len(m.Answers) != 1 {
		t.Fatalf("%+v", m)
	}
}

// a VPC associated later gets the zone's answers, and loses them when disassociated
func TestDNSFollowsAssociation(t *testing.T) {
	s, udp, _ := dnsEnv(t) // the listener serves vpc-1
	ask := func() dnsmessage.Message { return query(t, udp, "x.other.example.", dnsmessage.TypeA) }
	if m := ask(); m.Authoritative {
		t.Fatalf("vpc-1 is not associated yet: %+v", m)
	}
	s.mu.Lock()
	s.st.Zones["Z2"].VPCs = append(s.st.Zones["Z2"].VPCs, vpc{VPCId: "vpc-1", VPCRegion: "us-east-1"}) // what associate does
	s.mu.Unlock()
	if m := ask(); !m.Authoritative || m.Answers[0].Body.(*dnsmessage.AResource).A != [4]byte{10, 9, 9, 9} {
		t.Fatalf("associated: %+v", m)
	}
	s.mu.Lock()
	s.st.Zones["Z2"].VPCs = s.st.Zones["Z2"].VPCs[:1]
	s.mu.Unlock()
	if m := ask(); m.Authoritative {
		t.Fatalf("disassociated: %+v", m)
	}
}
