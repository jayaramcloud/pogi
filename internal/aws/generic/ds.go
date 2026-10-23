package generic

// Directory Service: a directory made with CreateDirectory is a Simple AD that is Active at once. Nothing
// runs: no domain controller answers, so the DNS addresses stay empty.
func init() {
	Derive("ds", "Directory", func(r *Resource) map[string]any {
		return map[string]any{"Stage": "Active", "Type": "SimpleAD", "ShareStatus": nil, "OwnerDirectoryDescription": nil}
	})
}
