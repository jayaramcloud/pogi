// Package terraform reads a Terraform folder or a plan JSON file into a list of resources, with
// their attribute values and the resources they reference. It only reads files: it never runs
// terraform, providers, scripts or external data sources.
package terraform

// Limits on what Load and Parse read, so a wrong folder can't exhaust memory.
const (
	MaxFiles = 2000
	MaxBytes = 50 << 20
	// MaxDepth is how deeply brackets, calls and templates may nest in one file. Real files stay far below it;
	// the parser and the evaluator recurse once per level, so a deeper file could overflow the stack.
	MaxDepth = 200
)

// Unknown is the value of an attribute pogi could not work out from the files: a computed id, a
// data source's result, or a function the parser does not know.
type Unknown struct{}

// Resource is one resource instance (one element of a count or for_each).
type Resource struct {
	Address string // module.net.aws_subnet.a["x"]
	Block   string // the address without instance keys: module.net.aws_subnet.a
	Module  string // module.net; "" for the root module
	Type    string // aws_subnet
	Name    string // a
	Data    bool   // a data source
	Cloud   string // "aws", "gcp" or "azure" from the type's provider; "" for other providers
	Region  string // the region or location (us-east-1, us-central1, eastus); "" when unknown

	// Attrs holds the attribute values in the shape of Terraform's JSON: string, float64, bool,
	// nil, []any and map[string]any. Nested blocks are lists of maps, as in a plan
	// (root_block_device → []any{map[string]any{"volume_size": 50.0}}). An attribute that is
	// set but could not be worked out holds Unknown{}.
	Attrs map[string]any

	Deps []string // the Blocks this resource references, directly or through locals, variables and module outputs; sorted

	File string // where the block is (folders only): a slash path relative to the folder
	Line int

	// From a plan only.
	Action string         // create, update, replace, delete, read or no-op
	Before map[string]any // the attributes before the change (update, replace and delete)
}

// Config is what Load, Parse or ReadPlan found.
type Config struct {
	Source    string // "folder" or "plan"
	Version   string // the Terraform version that wrote the plan
	Resources []*Resource
	Warnings  []string // what could not be read or resolved, in plain words
}
