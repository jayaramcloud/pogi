package generic

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Marketplace Metering: MeterUsage and BatchMeterUsage record usage as
// MeteringRecord resources (the console lists them). A record is for one
// product, customer, dimension and hour: sending it again with the same quantity
// (or the same ClientToken) returns the first record's id, with another quantity
// it is a duplicate; a timestamp more than 6 hours old, or in the future, is out of
// bounds. RegisterUsage returns a made-up, unsigned token. ResolveCustomer reads a
// token of the form pogi:<product>:<customer>:<account> (any other token resolves,
// deterministically, to a customer made up from its hash); a token that starts
// with "expired" has expired.
func init() {
	const svc = "meteringmarketplace"
	Override(svc, "MeterUsage", func(c *Call) (map[string]any, error) {
		ts, err := costMeterTime(c, c.In["Timestamp"])
		if err != nil {
			return nil, err
		}
		qty := costToFloat(c.In["UsageQuantity"])
		var total float64
		for _, a := range asList(c.In["UsageAllocations"]) {
			total += costToFloat(costCmap(a)["AllocatedUsageQuantity"])
		}
		if len(asList(c.In["UsageAllocations"])) > 0 && total != qty {
			return nil, c.costShapeErr("InvalidUsageAllocationsException", fmt.Sprintf("The allocated usage (%v) must add up to the usage quantity (%v)", total, qty))
		}
		if c.In["DryRun"] == true {
			return nil, core.Errorf(412, "DryRunOperation", "Request would have succeeded, but DryRun flag is set.")
		}
		id, dup := costMeterRecord(c, map[string]any{"ProductCode": c.In["ProductCode"], "Dimension": c.In["UsageDimension"], "Quantity": qty, "ClientToken": c.In["ClientToken"],
			"Allocations": c.In["UsageAllocations"]}, str(c.In["ProductCode"]), "", ts)
		if dup {
			return nil, c.costShapeErr("DuplicateRequestException", "A metering record already exists for this product, dimension and hour with a different usage quantity")
		}
		return map[string]any{"MeteringRecordId": id}, nil
	})
	Override(svc, "BatchMeterUsage", func(c *Call) (map[string]any, error) {
		var results, unprocessed []any
		for _, x := range asList(c.In["UsageRecords"]) {
			rec := costCmap(x)
			customer := str(rec["CustomerIdentifier"])
			if customer == "" {
				customer = str(rec["CustomerAWSAccountId"])
			}
			ts, err := costMeterTime(c, rec["Timestamp"])
			switch {
			case err != nil:
				unprocessed = append(unprocessed, rec)
			case customer == "" && str(rec["LicenseArn"]) == "":
				results = append(results, map[string]any{"UsageRecord": rec, "Status": "CustomerNotSubscribed"})
			default:
				id, dup := costMeterRecord(c, map[string]any{"ProductCode": c.In["ProductCode"], "Dimension": rec["Dimension"], "Quantity": costToFloat(rec["Quantity"]),
					"CustomerIdentifier": rec["CustomerIdentifier"], "CustomerAWSAccountId": rec["CustomerAWSAccountId"], "LicenseArn": rec["LicenseArn"], "Allocations": rec["UsageAllocations"]},
					str(c.In["ProductCode"]), customer+str(rec["LicenseArn"]), ts)
				status := "Success"
				if dup {
					status = "DuplicateRecord"
				}
				results = append(results, map[string]any{"UsageRecord": rec, "MeteringRecordId": id, "Status": status})
			}
		}
		return map[string]any{"Results": costOrEmpty(results), "UnprocessedRecords": costOrEmpty(unprocessed)}, nil
	})
	Override(svc, "RegisterUsage", func(c *Call) (map[string]any, error) {
		seg := func(v any) string { b, _ := json.Marshal(v); return base64.RawURLEncoding.EncodeToString(b) }
		sig := make([]byte, 32)
		rand.Read(sig)
		now := time.Now().UTC()
		token := seg(map[string]any{"alg": "none"}) + "." + seg(map[string]any{"productCode": c.In["ProductCode"], "publicKeyVersion": c.In["PublicKeyVersion"], "nonce": c.In["Nonce"], "iat": now.Unix()}) +
			"." + base64.RawURLEncoding.EncodeToString(sig)
		return map[string]any{"Signature": token, "PublicKeyRotationTimestamp": now.AddDate(1, 0, 0).Format(time.RFC3339)}, nil
	})
	Override(svc, "ResolveCustomer", func(c *Call) (map[string]any, error) {
		token := str(c.In["RegistrationToken"])
		if strings.HasPrefix(token, "expired") {
			return nil, c.costShapeErr("ExpiredTokenException", "The registration token has expired")
		}
		if p := strings.Split(token, ":"); len(p) == 4 && p[0] == "pogi" {
			return map[string]any{"ProductCode": p[1], "CustomerIdentifier": p[2], "CustomerAWSAccountId": p[3]}, nil
		}
		h := sha256.Sum256([]byte(token))
		n, _ := strconv.ParseUint(hex.EncodeToString(h[:6]), 16, 64)
		return map[string]any{"ProductCode": "pogi-product", "CustomerIdentifier": "cust-" + hex.EncodeToString(h[:6]), "CustomerAWSAccountId": fmt.Sprintf("%012d", n%1e12)}, nil
	})
}

// meterTime checks that a timestamp is one a record can have: at most 6 hours old, and not in the future.
func costMeterTime(c *Call, v any) (time.Time, error) {
	ts, ok := costParseTime(v)
	if now := time.Now().UTC(); !ok || ts.Before(now.Add(-6*time.Hour)) || ts.After(now.Add(time.Minute)) {
		return ts, c.costShapeErr("TimestampOutOfBoundsException", "The timestamp is more than 6 hours old or in the future")
	}
	return ts, nil
}

// meterRecord stores a metering record unless the hour already has one for its product, customer and dimension; dup
// says the one that is there has another quantity (and then nothing is stored), the id is the first record's.
func costMeterRecord(c *Call, data map[string]any, product, customer string, ts time.Time) (id string, dup bool) {
	hour := ts.Truncate(time.Hour)
	key := strings.Join([]string{product, customer, str(data["Dimension"]), hour.Format(time.RFC3339)}, "|")
	for _, r := range c.List("MeteringRecord") {
		if r.IDs["Key"] == key {
			same := costToFloat(r.Data["Quantity"]) == costToFloat(data["Quantity"]) || str(data["ClientToken"]) != "" && str(data["ClientToken"]) == str(r.Data["ClientToken"])
			return r.ID, !same
		}
	}
	id = core.UUID()
	data["Timestamp"], data["Hour"] = ts.Format(time.RFC3339), hour.Format(time.RFC3339)
	c.Put(&Resource{Type: "MeteringRecord", Key: id, IDs: map[string]string{"Key": key}, ID: id, Account: c.Account, Region: c.key(), Data: data, Created: time.Now().UTC(), Updated: time.Now().UTC()})
	return id, false
}
