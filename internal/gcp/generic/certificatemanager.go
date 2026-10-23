package generic

import "fmt"

func init() {
	apiHooks["certificatemanager"] = hooks{
		required: map[string][]string{"DnsAuthorization": {"domain"}},
		created: map[string]func(*call, map[string]any){
			// the CNAME a user adds to prove the domain; Terraform reads it back
			"DnsAuthorization": func(_ *call, d map[string]any) {
				if d["type"] == nil {
					d["type"] = "FIXED_RECORD"
				}
				token := newToken()
				d["dnsResourceRecord"] = map[string]any{"type": "CNAME",
					"name": fmt.Sprintf("_acme-challenge_%s.%s.", token[:16], d["domain"]),
					"data": fmt.Sprintf("%s.%s.authorize.certificatemanager.goog.", token[16:], token[:8])}
			},
			"Certificate": func(_ *call, cert map[string]any) {
				if cert["scope"] == nil {
					cert["scope"] = "DEFAULT"
				}
				if m, ok := cert["managed"].(map[string]any); ok {
					m["state"] = "ACTIVE"
				}
			},
		},
	}
}
