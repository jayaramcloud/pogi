package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nrndev/pogi/internal/cost"
)

func TestAWSRuleMatchesRegionPrefixes(t *testing.T) {
	r := rate("aws/natgw/hour", "hour", 1, "NAT Gateway", "", `NatGateway-Hours`)
	for ut, want := range map[string]bool{
		"NatGateway-Hours": true, "USE1-NatGateway-Hours": true, "EU-NatGateway-Hours": true, "APS1-NatGateway-Hours": true,
		"TS-NatGateway-Hours": false, "Outposts-NatGateway-Hours": false, "NatGateway-Hours-Extra": false,
	} {
		if got := r.matches(&awsRow{fam: "NAT Gateway", ut: ut}); got != want {
			t.Errorf("%s: matches = %v, want %v", ut, got, want)
		}
	}
	if r.matches(&awsRow{fam: "Storage", ut: "NatGateway-Hours"}) {
		t.Error("a row of another product family matched")
	}
}

// scanText runs one offer over a small CSV in the shape of the price files.
func scanText(t *testing.T, of awsOffer, region, body string) (*awsGen, error) {
	t.Helper()
	head := "\"FormatVersion\",\"v1.0\"\n\"Disclaimer\",\"x\"\n\"Publication Date\",\"2026-10-15\"\n\"Version\",\"1\"\n\"OfferCode\",\"X\"\n"
	path := filepath.Join(t.TempDir(), "x.csv")
	if err := os.WriteFile(path, []byte(head+body), 0o644); err != nil {
		t.Fatal(err)
	}
	g := &awsGen{rates: map[string]map[string]float64{}, units: map[string]string{}, global: map[string]float64{},
		sizes: map[string]map[string]cost.Size{}, colTbl: map[string]string{}, cols: map[string]map[string]map[string]float64{}, regions: map[string]bool{}}
	return g, g.scan(&of, region, path)
}

const csvHead = `"TermType","StartingRange","EndingRange","Unit","PricePerUnit","Product Family","usageType","operation","PriceDescription","Database Engine","Deployment Option"` + "\n"

func TestAWSScanKeepsFirstPaidTier(t *testing.T) {
	of := awsOffer{code: "T", rates: []awsRule{rate("aws/x/storage", "GB-month", 1, "Storage", "", `TimedStorage`), rate("aws/x/req", "1M requests", 1e6, "Req", "", `Requests`)}}
	g, err := scanText(t, of, "eu-west-1", csvHead+
		`"OnDemand","0","25","GB-Mo","0.0000000000","Storage","EU-TimedStorage","","free",,`+"\n"+
		`"OnDemand","25","Inf","GB-Mo","0.2500000000","Storage","EU-TimedStorage","","paid",,`+"\n"+
		`"OnDemand","25","Inf","GB-Mo","0.2500000000","Storage","EU-TimedStorage","","the same row again",,`+"\n"+
		`"OnDemand","1000","Inf","GB-Mo","0.1000000000","Storage","EU-TimedStorage","","a later, cheaper tier",,`+"\n"+
		`"Reserved","0","Inf","GB-Mo","9.0000000000","Storage","EU-TimedStorage","","not on demand",,`+"\n"+
		`"OnDemand","0","Inf","Requests","0.0000004000","Req","Requests","","per request",,`+"\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := g.rates["aws/x/storage"]["eu-west-1"]; got != 0.25 {
		t.Errorf("storage: %v, want the first paid tier 0.25", got)
	}
	if got := g.rates["aws/x/req"]["eu-west-1"]; got != 0.4 {
		t.Errorf("requests: %v, want 0.4 per million", got)
	}
}

func TestAWSScanPrefersTheEnginesOwnPrice(t *testing.T) {
	rds := rate("aws/rds/postgres/storage/standard", "GB-month", 1, "Database Storage", "", `RDS:StorageUsage`)
	rds.rank = func(r *awsRow) int {
		switch r.get("Database Engine") {
		case "PostgreSQL":
			return 2
		case "Any":
			return 1
		}
		return -1
	}
	of := awsOffer{code: "T", rates: []awsRule{rds}}
	g, err := scanText(t, of, "us-east-1", csvHead+
		`"OnDemand","0","Inf","GB-Mo","0.1000000000","Database Storage","RDS:StorageUsage","","",Any,Single-AZ`+"\n"+
		`"OnDemand","0","Inf","GB-Mo","0.1010000000","Database Storage","RDS:StorageUsage","","",PostgreSQL,Single-AZ`+"\n"+
		`"OnDemand","0","Inf","GB-Mo","0.2000000000","Database Storage","RDS:StorageUsage","","","SQL Server",Single-AZ`+"\n")
	if err != nil {
		t.Fatal(err)
	}
	if got := g.rates["aws/rds/postgres/storage/standard"]["us-east-1"]; got != 0.101 {
		t.Errorf("got %v, want the PostgreSQL price 0.101", got)
	}
}

func TestAWSScanErrors(t *testing.T) {
	of := awsOffer{code: "T", rates: []awsRule{rate("aws/x/a", "hour", 1, "Fam", "", `A`)}}
	for name, c := range map[string]struct{ body, want string }{
		"two prices at one tier": {csvHead + `"OnDemand","0","Inf","Hrs","0.1","Fam","A","","one",,` + "\n" + `"OnDemand","0","Inf","Hrs","0.2","Fam","A","","two",,` + "\n", "two prices"},
		"a bad price":            {csvHead + `"OnDemand","0","Inf","Hrs","free","Fam","A","","",,` + "\n", "price"},
		"a missing column":       {`"TermType","Unit"` + "\n" + `"OnDemand","Hrs"` + "\n", "column"},
		"a cut-off file":         {"", "column names"},
	} {
		if _, err := scanText(t, of, "us-east-1", c.body); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %v, want one that says %q", name, err, c.want)
		}
	}
}

func TestAWSBuildFailsWhenARuleFindsNothing(t *testing.T) {
	g := &awsGen{rates: map[string]map[string]float64{}, global: map[string]float64{}}
	_, err := g.build([]awsOffer{{code: "T", rates: []awsRule{rate("aws/x/a", "hour", 1, "Fam", "", `A`)}}})
	if err == nil || !strings.Contains(err.Error(), "aws/x/a") {
		t.Errorf("error %v, want one that names the missing rate", err)
	}
}

func TestAWSRulesAreUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, of := range awsOffers() {
		for _, r := range append(append([]awsRule{}, of.rates...), of.global...) {
			if seen[r.key] {
				t.Errorf("the rate %s is defined twice", r.key)
			}
			seen[r.key] = true
		}
	}
}

func TestAWSSizeHelpers(t *testing.T) {
	for in, want := range map[string]string{"m5.large": "m5", "db.r6g.large": "r6g", "cache.m5.large": "m5", "r6g.large.search": "r6g", "kafka.m7g.large": "m7g", "p6-b200.48xlarge": "p6-b200"} {
		if got := awsFamily(in); got != want {
			t.Errorf("awsFamily(%s) = %s, want %s", in, got, want)
		}
	}
	for name, arm := range map[string]bool{"m6g.large": true, "db.r6gd.large": true, "im4gn.2xlarge": true, "cache.c7gn.large": true, "m5.large": false, "g5.xlarge": false, "g4dn.xlarge": false, "t3.micro": false, "g5g.xlarge": true} {
		if got := awsArmName.MatchString(name); got != arm {
			t.Errorf("awsArmName(%s) = %v, want %v", name, got, arm)
		}
	}
	if awsNum("16 GiB") != 16 || awsNum("0.5 GiB") != 0.5 || awsNum("NA") != 0 {
		t.Error("awsNum")
	}
}
