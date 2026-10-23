package generic

// Oracle Database@AWS: an ODB network is AVAILABLE at once. Its managed services (S3, STS, KMS and
// zero-ETL access) are ENABLED or DISABLED as the create call asked; nothing is built behind them.

func init() {
	Derive("odb", "OdbNetwork", func(r *Resource) map[string]any {
		on := func(k string) string {
			if str(r.Data[k]) == "ENABLED" {
				return "ENABLED"
			}
			return "DISABLED"
		}
		access := func(k, policy string) map[string]any {
			a := map[string]any{"status": on(k)}
			if p := str(r.Data[policy]); p != "" {
				a[policy] = p
			}
			return a
		}
		return map[string]any{"percentProgress": 100.0, "managedServices": map[string]any{
			"s3Access":                          access("s3Access", "s3PolicyDocument"),
			"stsAccess":                         access("stsAccess", "stsPolicyDocument"),
			"kmsAccess":                         access("kmsAccess", "kmsPolicyDocument"),
			"zeroEtlAccess":                     map[string]any{"status": on("zeroEtlAccess")},
			"managedS3BackupAccess":             map[string]any{"status": "DISABLED"},
			"crossRegionS3RestoreSourcesAccess": []any{},
			"managedServicesIpv4Cidrs":          []any{},
		}}
	})
}
