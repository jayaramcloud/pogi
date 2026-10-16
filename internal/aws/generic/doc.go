// Package generic emulates AWS services from their botocore models.
//
// A service here is a model file plus, where it matters, a few overrides. The
// engine reads the model's shapes and HTTP bindings and speaks the json 1.0/1.1,
// query, ec2, rest-json, rest-xml and Smithy RPCv2 CBOR protocols (document
// shapes pass through as free-form JSON). It infers resources from
// operation names and stores them, so create, describe, list, update, delete
// and tagging work without any per-service code.
//
// # Adding a service
//
//  1. Add the botocore directory name (the one under botocore/data) to
//     services.txt.
//  2. Run `go generate ./internal/aws/generic`. It fetches the pinned botocore
//     wheel (scripts/models.sh), writes models/<name>.json.gz and refreshes
//     models/index.json.
//  3. That is all: Services() returns a factory for every embedded model and
//     pogi.go registers each one that has no hand-written service of the same
//     name. The service is named after the botocore directory.
//  4. Add a test with the official SDK client (see codeartifact_test.go). If
//     something comes back wrong, fix it in the engine when other services
//     would benefit, else with an override.
//
// A service's name, signing name and target prefix come from the model, so
// routing needs nothing more. When a hand-written service already owns the
// signing name, a Query service is routed by its Version parameter instead
// (Classic ELB beside ELB v2), and a REST service by whether the request is one
// of its operations and is signed for it (the Bedrock services all sign as "bedrock").
//
// # What the engine infers
//
//   - Create<X>, Put<X> and Register<X> create a resource of type X (a prefix
//     such as In, For or From ends the name: PutQuotaIntoTemplate is a Quota).
//     Describe<X>/Get<X> read one, List<X>s and Describe<X>s list them,
//     Update<X>/Modify<X> change one, Delete<X>/Deregister<X> remove one.
//     TagResource, UntagResource, ListTagsForResource, AddTags and RemoveTags
//     work on any resource by ARN or identifier; Tags and AddTags/RemoveTags
//     members of create and update calls are kept as tags.
//   - A type's identifiers are the required string members of its Get,
//     Describe or Delete call (parents included: a filter is keyed by its
//     detector and its name). The member named after the type (<X>Name, <X>Id,
//     <X>Arn, Name, Id, Arn) is the one the service makes up when a create call
//     leaves it out, from the member's pattern or as <prefix>-<hex>.
//   - A resource keeps the merged input of its create and update calls.
//     Outputs are filled by member name from it, from the request, and from
//     the identifiers, ARN, tags, timestamps and the account. A status or state
//     member with an enum gets a ready value (ACTIVE, AVAILABLE and so on);
//     required numbers, booleans and lists get zero values so that clients
//     never read a missing member.
//   - Lists page with the model's paginators (or by member names), filter by
//     identifiers and by members that match stored members, and a Describe with
//     identifiers that match nothing is a not-found error.
//   - Errors use the model's shapes: the declared not-found, already-exists and
//     validation errors, else ResourceNotFoundException, ConflictException and
//     ValidationException.
//
// # Overrides and hooks
//
// Override replaces one operation; Hook runs after one succeeds. Both are
// registered by service and operation name, usually from an init function in
// <service>.go in this package (see elb.go):
//
//	generic.Override("efs", "DescribeLifecycleConfiguration", func(c *generic.Call) (map[string]any, error) {
//		out, err := c.Default() // the engine's own result
//		...
//		return out, nil // keyed by the model's member names
//	})
//	generic.Hook("guardduty", "CreateDetector", func(c *generic.Call) { /* c.Res is the new detector */ })
//
// An override that must write a whole operation's response itself (an event stream)
// sets Call.Stream; see ai.go. Raw lets a service answer requests that are not operations of its model (the Prometheus API of Managed
// Prometheus), and OnClose runs a function when the hub shuts the service down (a listener, a store).
// A service with work of its own (budget alerts, report delivery) starts it from
// a function added to starters, with Service.Every; Service.Tick runs that work
// once, for tests, and Service.Close stops it.
//
// A Call carries the parsed input (c.In, keyed by member names, as
// map[string]any), the account and region, and the hub. c.Get, c.List and
// c.Put read and write resources; c.NotFound, c.Exists and c.Invalid make the
// model's own errors. Service.Do runs an operation in-process, and
// Service.Types, List, Get and Delete expose the stored resources by type for
// Cloud Control (cloudcontrol.go runs on them).
//
// # Statuses that move
//
// A status that goes CREATING, then ACTIVE is worked out when a resource is
// read, so nothing runs in the background and a create never blocks. Derive
// registers a function that returns members of a resource type at read time;
// Step picks the state of a timed sequence (Begin starts or restarts one):
//
//	generic.Derive("mwaa", "Environment", func(r *generic.Resource) map[string]any {
//		return map[string]any{"Status": generic.Step(r, "env", "CREATING", "AVAILABLE")}
//	})
//
// Each state lasts SetLifecycleDelay long (1.5 seconds by default); tests set 0
// to settle every resource at once, or an hour to freeze the first state.
//
// A service whose signing name a hand-written service owns, and that is not
// Query, claims its requests with an entry in matchers (lambdamicrovms.go).
package generic
