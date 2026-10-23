package generic

import (
	"fmt"
	"hash/fnv"
	"strings"

	"github.com/nrndev/pogi/internal/azure/arm"
	"github.com/nrndev/pogi/internal/azure/armres"
	"github.com/nrndev/pogi/internal/core"
)

// DNS: a zone has name servers and starts with its NS and SOA record sets, as Azure's does; azurerm reads the
// SOA record of every zone. Nothing answers DNS queries.

const dnsZones = "Microsoft.Network/dnsZones"

func init() {
	Override(dnsZones, Hook{
		Put: func(s *Service, c *core.Ctx, id arm.ID, r map[string]any) error {
			armres.Defaults(armres.Props(r), map[string]any{"zoneType": "Public"})
			if s.Has(id) {
				return nil
			}
			ns := nameServers(id)
			var nsRecords []any
			for _, n := range ns {
				nsRecords = append(nsRecords, map[string]any{"nsdname": n})
			}
			if err := s.Create(c, id.Child("NS", "@"), map[string]any{"properties": map[string]any{"TTL": 172800, "NSRecords": nsRecords}}); err != nil {
				return err
			}
			return s.Create(c, id.Child("SOA", "@"), map[string]any{"properties": map[string]any{"TTL": 3600, "SOARecord": map[string]any{
				"host": ns[0], "email": "azuredns-hostmaster.microsoft.com", "serialNumber": 1, "refreshTime": 3600,
				"retryTime": 300, "expireTime": 2419200, "minimumTTL": 300}}})
		},
		// GET .../dnsZones/<zone>/recordsets and .../all list the record sets of every type.
		Extra: func(s *Service) []arm.Provider {
			var out []arm.Provider
			for _, t := range []string{"recordsets", "all"} {
				out = append(out, arm.Provider{Type: dnsZones + "/" + t, List: func(c *core.Ctx, parent arm.ID) ([]map[string]any, error) {
					return s.Children(parent), nil
				}})
			}
			return out
		},
		View: func(s *Service, id arm.ID, r map[string]any) {
			p := armres.Props(r)
			p["nameServers"] = nameServers(id)
			p["maxNumberOfRecordSets"], p["maxNumberOfRecordsPerRecordSet"] = 10000, 20
			p["numberOfRecordSets"] = s.Count(id)
			r["properties"] = p
		},
	})
	for _, t := range []string{"A", "AAAA", "CAA", "CNAME", "MX", "NS", "PTR", "SOA", "SRV", "TXT"} {
		Override(dnsZones+"/"+t, Hook{View: func(s *Service, id arm.ID, r map[string]any) {
			fqdn := id.Name() + "." + id.Parent().Name() + "."
			if id.Name() == "@" {
				fqdn = id.Parent().Name() + "."
			}
			armres.Props(r)["fqdn"] = fqdn
		}})
	}
}

// nameServers are the four Azure gives a zone, such as ns1-07.azure-dns.com., ns2-07.azure-dns.net.,
// ns3-07.azure-dns.org. and ns4-07.azure-dns.info.; the number comes from the zone.
func nameServers(id arm.ID) []string {
	h := fnv.New32a()
	h.Write([]byte(strings.ToLower(id.String())))
	n := 1 + h.Sum32()%9
	var out []string
	for i, tld := range []string{"com", "net", "org", "info"} {
		out = append(out, fmt.Sprintf("ns%d-%02d.azure-dns.%s.", i+1, n, tld))
	}
	return out
}
