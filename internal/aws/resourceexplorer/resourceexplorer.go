// Package resourceexplorer emulates AWS Resource Explorer (REST-JSON):
// indexes, views and search, in the Resource Explorer query language, over
// the resources that every service lists through tagging.Tagged.
package resourceexplorer

import (
	"cmp"
	"encoding/json"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/nrndev/pogi/internal/aws/tagging"
	"github.com/nrndev/pogi/internal/core"
)

// Service is the Resource Explorer emulator.
type Service struct {
	hub *core.Hub
	mux *http.ServeMux
	mu  sync.Mutex
	st  state
}

type state struct {
	Indexes  map[string]*index            `json:"indexes"`  // account/region
	Views    map[string]*view             `json:"views"`    // ARN
	Defaults map[string]string            `json:"defaults"` // account/region → default view ARN
	Tags     map[string]map[string]string `json:"tags"`     // index or view ARN → tags
}

func newState() state {
	return state{Indexes: map[string]*index{}, Views: map[string]*view{}, Defaults: map[string]string{}, Tags: map[string]map[string]string{}}
}

// index is a Region's index; it is ACTIVE for as long as it exists.
type index struct {
	Arn, Region, Type        string
	CreatedAt, LastUpdatedAt time.Time
}

// view is a view in the API's shape.
type view struct {
	ViewArn, ViewName, Owner, Scope string
	IncludedProperties              []prop
	Filters                         filter
	LastUpdatedAt                   time.Time
}

type prop struct{ Name string }

type filter struct{ FilterString string }

// resource is a search result; id and tags are what queries match on.
type resource struct {
	Arn, OwningAccountId, Region, ResourceType, Service string
	LastReportedAt                                      time.Time
	Properties                                          []map[string]any
	id                                                  string
	tags                                                map[string]string
}

type pageIn struct {
	MaxResults int
	NextToken  string
}

// New creates the Resource Explorer service.
func New(hub *core.Hub) *Service {
	s := &Service{hub: hub, mux: http.NewServeMux()}
	s.Reset()
	for op, fn := range map[string]func(*core.Ctx, *http.Request) (any, error){
		"CreateIndex":                         typed(s.createIndex),
		"GetIndex":                            typed(s.getIndex),
		"UpdateIndexType":                     typed(s.updateIndexType),
		"DeleteIndex":                         typed(s.deleteIndex),
		"ListIndexes":                         typed(s.listIndexes),
		"CreateView":                          typed(s.createView),
		"GetView":                             typed(s.getView),
		"ListViews":                           typed(s.listViews),
		"UpdateView":                          typed(s.updateView),
		"DeleteView":                          typed(s.deleteView),
		"BatchGetView":                        typed(s.batchGetView),
		"AssociateDefaultView":                typed(s.associateDefaultView),
		"GetDefaultView":                      typed(s.getDefaultView),
		"DisassociateDefaultView":             typed(s.disassociateDefaultView),
		"Search":                              typed(s.search),
		"ListResources":                       typed(s.listResources),
		"ListSupportedResourceTypes":          listSupportedResourceTypes,
		"GetAccountLevelServiceConfiguration": getAccountLevelServiceConfiguration,
	} {
		s.mux.HandleFunc("POST /"+op, s.handle(op, fn))
	}
	s.mux.HandleFunc("POST /tags/{resourceArn...}", s.handle("TagResource", s.tagResource))
	s.mux.HandleFunc("GET /tags/{resourceArn...}", s.handle("ListTagsForResource", s.listTags))
	s.mux.HandleFunc("DELETE /tags/{resourceArn...}", s.handle("UntagResource", s.untagResource))
	s.mux.HandleFunc("/", s.handle("Unknown", s.unknown))
	return s
}

func (s *Service) Info() core.Info {
	return core.Info{Name: "resource-explorer-2", Cloud: core.AWS, Title: "Resource Explorer", Signing: []string{"resource-explorer-2"}, Protocol: "rest-json"}
}

func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) { s.mux.ServeHTTP(w, r) }

// handle runs one operation and writes its REST-JSON result or error.
func (s *Service) handle(op string, fn func(*core.Ctx, *http.Request) (any, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := core.C(r)
		c.Op = op
		var out any
		err := c.Intercept()
		if err == nil {
			out, err = fn(c, r)
		}
		if err != nil {
			core.RESTJSONError(w, c, err)
			return
		}
		if out == nil {
			out = struct{}{}
		}
		core.RESTJSON(w, c, http.StatusOK, out)
	}
}

// typed decodes the JSON body into fn's input.
func typed[In any](fn func(*core.Ctx, *In) (any, error)) func(*core.Ctx, *http.Request) (any, error) {
	return func(c *core.Ctx, r *http.Request) (any, error) {
		in := new(In)
		if err := decode(r, in); err != nil {
			return nil, err
		}
		return fn(c, in)
	}
}

func decode(r *http.Request, v any) error {
	if b := core.Body(r); len(b) > 0 && json.Unmarshal(b, v) != nil {
		return invalid("The request body is not valid JSON.")
	}
	return nil
}

func (s *Service) unknown(c *core.Ctx, r *http.Request) (any, error) {
	return nil, core.Errorf(404, "UnknownOperationException", "pogi does not implement this Resource Explorer request (%s %s)", r.Method, r.URL.Path)
}

func (s *Service) Reset() {
	s.mu.Lock()
	s.st = newState()
	s.mu.Unlock()
}

func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.st)
}

func (s *Service) Load(b []byte) error {
	st := newState()
	if err := json.Unmarshal(b, &st); err != nil {
		return err
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

func notFound(format string, a ...any) error {
	return core.Errorf(404, "ResourceNotFoundException", format, a...)
}

func conflict(format string, a ...any) error {
	return core.Errorf(409, "ConflictException", format, a...)
}

func invalid(format string, a ...any) error {
	return core.Errorf(400, "ValidationException", format, a...)
}

// key is the caller's account/region.
func key(c *core.Ctx) string { return c.Account + "/" + c.Region }

// viewPrefix starts the ARNs of the caller's views.
func viewPrefix(c *core.Ctx) string { return c.ARN("resource-explorer-2", "view/") }

// ---- indexes ----

// index returns the index of the caller's Region, which must be arn when set; lock held.
func (s *Service) index(c *core.Ctx, arn string) (*index, error) {
	ix := s.st.Indexes[key(c)]
	switch {
	case ix == nil:
		return nil, notFound("Resource Explorer has no index in %s.", c.Region)
	case arn != "" && arn != ix.Arn:
		return nil, notFound("The index %s does not exist in %s.", arn, c.Region)
	}
	return ix, nil
}

// ponytail: ClientToken is ignored, so a retried CreateIndex or CreateView
// conflicts instead of repeating its result; keep the token if retries matter.
func (s *Service) createIndex(c *core.Ctx, in *struct{ Tags map[string]string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Indexes[key(c)] != nil {
		return nil, conflict("Resource Explorer already has an index in %s.", c.Region)
	}
	now := time.Now().UTC()
	ix := &index{Arn: c.ARN("resource-explorer-2", "index/"+core.UUID()), Region: c.Region, Type: "LOCAL", CreatedAt: now, LastUpdatedAt: now}
	s.st.Indexes[key(c)] = ix
	s.tag(ix.Arn, in.Tags)
	return map[string]any{"Arn": ix.Arn, "State": "ACTIVE", "CreatedAt": now}, nil
}

func (s *Service) getIndex(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ix, err := s.index(c, "")
	if err != nil {
		return nil, err
	}
	return map[string]any{"Arn": ix.Arn, "Type": ix.Type, "State": "ACTIVE", "CreatedAt": ix.CreatedAt,
		"LastUpdatedAt": ix.LastUpdatedAt, "Tags": s.tags(ix.Arn)}, nil
}

// updateIndexType switches at once; an account has at most one aggregator.
func (s *Service) updateIndexType(c *core.Ctx, in *struct{ Arn, Type string }) (any, error) {
	if in.Type != "LOCAL" && in.Type != "AGGREGATOR" {
		return nil, invalid("Type %q is not valid: use LOCAL or AGGREGATOR.", in.Type)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ix, err := s.index(c, in.Arn)
	if err != nil {
		return nil, err
	}
	for k, o := range s.st.Indexes {
		if in.Type == "AGGREGATOR" && o != ix && o.Type == "AGGREGATOR" && strings.HasPrefix(k, c.Account+"/") {
			return nil, core.Errorf(402, "ServiceQuotaExceededException", "The account already has an aggregator index in %s.", o.Region)
		}
	}
	ix.Type, ix.LastUpdatedAt = in.Type, time.Now().UTC()
	return map[string]any{"Arn": ix.Arn, "Type": ix.Type, "State": "ACTIVE", "LastUpdatedAt": ix.LastUpdatedAt}, nil
}

// deleteIndex removes the index at once, and with it the Region's views as AWS does.
func (s *Service) deleteIndex(c *core.Ctx, in *struct{ Arn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ix, err := s.index(c, in.Arn)
	if err != nil {
		return nil, err
	}
	for arn := range s.st.Views {
		if strings.HasPrefix(arn, viewPrefix(c)) {
			delete(s.st.Views, arn)
			delete(s.st.Tags, arn)
		}
	}
	delete(s.st.Indexes, key(c))
	delete(s.st.Defaults, key(c))
	delete(s.st.Tags, ix.Arn)
	return map[string]any{"Arn": ix.Arn, "State": "DELETING", "LastUpdatedAt": time.Now().UTC()}, nil
}

func (s *Service) listIndexes(c *core.Ctx, in *struct {
	pageIn
	Type    string
	Regions []string
}) (any, error) {
	out := []map[string]string{}
	s.mu.Lock()
	for _, k := range core.SortedKeys(s.st.Indexes) {
		ix := s.st.Indexes[k]
		if strings.HasPrefix(k, c.Account+"/") && (in.Type == "" || in.Type == ix.Type) && (len(in.Regions) == 0 || slices.Contains(in.Regions, ix.Region)) {
			out = append(out, map[string]string{"Region": ix.Region, "Arn": ix.Arn, "Type": ix.Type})
		}
	}
	s.mu.Unlock()
	items, next, err := page(out, in.pageIn)
	if err != nil {
		return nil, err
	}
	return list(map[string]any{"Indexes": items}, next), nil
}

// ---- views ----

var viewName = regexp.MustCompile(`^[a-zA-Z0-9\-]{1,64}$`)

// view returns one of the caller's views in its Region; lock held.
func (s *Service) view(c *core.Ctx, arn string) (*view, error) {
	if v := s.st.Views[arn]; v != nil && strings.HasPrefix(arn, viewPrefix(c)) {
		return v, nil
	}
	return nil, notFound("The view %s does not exist.", arn)
}

// set validates and applies a view's included properties and filter; unset means none.
func (v *view) set(props []prop, f filter) error {
	if _, err := parse(f.FilterString); err != nil {
		return err
	}
	v.IncludedProperties, v.Filters, v.LastUpdatedAt = append([]prop{}, props...), f, time.Now().UTC()
	return nil
}

func (s *Service) createView(c *core.Ctx, in *struct {
	ViewName, Scope    string
	IncludedProperties []prop
	Filters            filter
	Tags               map[string]string
}) (any, error) {
	if !viewName.MatchString(in.ViewName) {
		return nil, invalid("The view name %q is not valid: use 1 to 64 letters, digits and hyphens.", in.ViewName)
	}
	v := &view{ViewArn: c.ARN("resource-explorer-2", "view/"+in.ViewName+"/"+core.UUID()), ViewName: in.ViewName,
		Owner: c.Account, Scope: cmp.Or(in.Scope, c.GlobalARN("iam", "root"))}
	if err := v.set(in.IncludedProperties, in.Filters); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for arn, o := range s.st.Views {
		if o.ViewName == in.ViewName && strings.HasPrefix(arn, viewPrefix(c)) {
			return nil, conflict("A view named %s already exists in %s.", in.ViewName, c.Region)
		}
	}
	s.st.Views[v.ViewArn] = v
	s.tag(v.ViewArn, in.Tags)
	return map[string]any{"View": *v}, nil
}

func (s *Service) getView(c *core.Ctx, in *struct{ ViewArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.view(c, in.ViewArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"View": *v, "Tags": s.tags(v.ViewArn)}, nil
}

func (s *Service) listViews(c *core.Ctx, in *pageIn) (any, error) {
	out := []string{}
	s.mu.Lock()
	for _, arn := range core.SortedKeys(s.st.Views) {
		if strings.HasPrefix(arn, viewPrefix(c)) {
			out = append(out, arn)
		}
	}
	s.mu.Unlock()
	items, next, err := page(out, *in)
	if err != nil {
		return nil, err
	}
	return list(map[string]any{"Views": items}, next), nil
}

// updateView replaces the included properties and the filter.
func (s *Service) updateView(c *core.Ctx, in *struct {
	ViewArn            string
	IncludedProperties []prop
	Filters            filter
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, err := s.view(c, in.ViewArn)
	if err != nil {
		return nil, err
	}
	if err := v.set(in.IncludedProperties, in.Filters); err != nil {
		return nil, err
	}
	return map[string]any{"View": *v}, nil
}

func (s *Service) deleteView(c *core.Ctx, in *struct{ ViewArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.view(c, in.ViewArn); err != nil {
		return nil, err
	}
	delete(s.st.Views, in.ViewArn)
	delete(s.st.Tags, in.ViewArn)
	if s.st.Defaults[key(c)] == in.ViewArn {
		delete(s.st.Defaults, key(c))
	}
	return map[string]string{"ViewArn": in.ViewArn}, nil
}

func (s *Service) batchGetView(c *core.Ctx, in *struct{ ViewArns []string }) (any, error) {
	views, errs := []view{}, []map[string]string{}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, arn := range in.ViewArns {
		if v, err := s.view(c, arn); err != nil {
			errs = append(errs, map[string]string{"ViewArn": arn, "ErrorMessage": core.AsError(err).Msg})
		} else {
			views = append(views, *v)
		}
	}
	return map[string]any{"Views": views, "Errors": errs}, nil
}

func (s *Service) associateDefaultView(c *core.Ctx, in *struct{ ViewArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.view(c, in.ViewArn); err != nil {
		return nil, err
	}
	s.st.Defaults[key(c)] = in.ViewArn
	return map[string]string{"ViewArn": in.ViewArn}, nil
}

func (s *Service) getDefaultView(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if arn := s.st.Defaults[key(c)]; arn != "" {
		return map[string]string{"ViewArn": arn}, nil
	}
	return nil, nil
}

func (s *Service) disassociateDefaultView(c *core.Ctx, _ *struct{}) (any, error) {
	s.mu.Lock()
	delete(s.st.Defaults, key(c))
	s.mu.Unlock()
	return nil, nil
}

// ---- tags (indexes and views carry them) ----

// tag adds tags to an index or view; lock held.
func (s *Service) tag(arn string, tags map[string]string) {
	if s.st.Tags[arn] == nil {
		s.st.Tags[arn] = map[string]string{}
	}
	maps.Copy(s.st.Tags[arn], tags)
}

// tags copies the tags of an index or view; lock held.
func (s *Service) tags(arn string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, s.st.Tags[arn])
	return out
}

// tagged returns the path's ARN when it is the caller's index or one of its views; lock held.
func (s *Service) tagged(c *core.Ctx, r *http.Request) (string, error) {
	arn := r.PathValue("resourceArn")
	if ix := s.st.Indexes[key(c)]; ix != nil && ix.Arn == arn {
		return arn, nil
	}
	_, err := s.view(c, arn)
	return arn, err
}

func (s *Service) tagResource(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ Tags map[string]string }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, r)
	if err != nil {
		return nil, err
	}
	s.tag(arn, in.Tags)
	return nil, nil
}

func (s *Service) untagResource(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, r)
	if err != nil {
		return nil, err
	}
	for _, k := range r.URL.Query()["tagKeys"] {
		delete(s.st.Tags[arn], k)
	}
	return nil, nil
}

func (s *Service) listTags(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	arn, err := s.tagged(c, r)
	if err != nil {
		return nil, err
	}
	return map[string]any{"Tags": s.tags(arn)}, nil
}

// ---- search ----

func (s *Service) search(c *core.Ctx, in *struct {
	pageIn
	QueryString, ViewArn string
}) (any, error) {
	arn, found, err := s.find(c, in.ViewArn, in.QueryString)
	if err != nil {
		return nil, err
	}
	items, next, err := page(found, in.pageIn)
	if err != nil {
		return nil, err
	}
	return list(map[string]any{"Resources": items, "ViewArn": arn, "Count": map[string]any{"TotalResources": len(found), "Complete": true}}, next), nil
}

func (s *Service) listResources(c *core.Ctx, in *struct {
	pageIn
	ViewArn string
	Filters filter
}) (any, error) {
	arn, found, err := s.find(c, in.ViewArn, in.Filters.FilterString)
	if err != nil {
		return nil, err
	}
	items, next, err := page(found, in.pageIn)
	if err != nil {
		return nil, err
	}
	return list(map[string]any{"Resources": items, "ViewArn": arn}, next), nil
}

// scope returns the view a query runs through (default: the Region's default
// view) and the Regions it covers: every indexed Region behind an aggregator.
func (s *Service) scope(c *core.Ctx, viewArn string) (view, []string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ix, err := s.index(c, "")
	if err != nil {
		return view{}, nil, err
	}
	if viewArn = cmp.Or(viewArn, s.st.Defaults[key(c)]); viewArn == "" {
		return view{}, nil, notFound("There is no default view in %s; pass a ViewArn.", c.Region)
	}
	v, err := s.view(c, viewArn)
	if err != nil {
		return view{}, nil, err
	}
	regions := []string{c.Region}
	for k, o := range s.st.Indexes {
		if ix.Type == "AGGREGATOR" && o != ix && strings.HasPrefix(k, c.Account+"/") {
			regions = append(regions, o.Region)
		}
	}
	return *v, regions, nil
}

// find returns the view's ARN and, sorted by ARN, the resources that both
// its filter and the query match.
func (s *Service) find(c *core.Ctx, viewArn, query string) (string, []resource, error) {
	v, regions, err := s.scope(c, viewArn)
	if err != nil {
		return "", nil, err
	}
	vf, _ := parse(v.Filters.FilterString) // checked when the view was saved
	terms, err := parse(query)
	if err != nil {
		return "", nil, err
	}
	now, all := time.Now().UTC(), map[string]resource{}
	for _, region := range regions { // services lock themselves: never call them under s.mu
		for _, svc := range s.hub.Services() {
			if t, ok := svc.(tagging.Tagged); ok {
				for arn, tags := range t.TaggedResources(c.Account, region) {
					all[arn] = describe(arn, c.Account, region, tags, now)
				}
			}
		}
	}
	out := []resource{}
	for _, arn := range core.SortedKeys(all) {
		r := all[arn]
		if !matches(vf, &r) || !matches(terms, &r) {
			continue
		}
		if slices.Contains(v.IncludedProperties, prop{"tags"}) && len(r.tags) > 0 {
			data := []map[string]string{}
			for _, k := range core.SortedKeys(r.tags) {
				data = append(data, map[string]string{"Key": k, "Value": r.tags[k]})
			}
			r.Properties = []map[string]any{{"Name": "tags", "LastReportedAt": now, "Data": data}}
		}
		out = append(out, r)
	}
	return v.ViewArn, out, nil
}

// bareTypes are the resource types of services whose ARNs name only the resource.
var bareTypes = map[string]string{"s3": "bucket", "sqs": "queue", "sns": "topic"}

// describe turns an ARN into a result: the type is the resource part up to
// the first "/" or ":". account and region fill in what the ARN leaves out (S3).
func describe(arn, account, region string, tags map[string]string, now time.Time) resource {
	svc, reg, acct, res, _ := core.ParseARN(arn)
	typ, id := bareTypes[svc], res
	if i := strings.IndexAny(res, "/:"); i >= 0 {
		typ, id = res[:i], res[i+1:]
	}
	return resource{Arn: arn, OwningAccountId: cmp.Or(acct, account), Region: cmp.Or(reg, region), ResourceType: svc + ":" + typ,
		Service: svc, LastReportedAt: now, Properties: []map[string]any{}, id: id, tags: tags}
}

// supported are the resource types pogi's services report.
var supported = []string{"acm:certificate", "cloudwatch:alarm", "dynamodb:table", "ec2:image", "ec2:instance",
	"ec2:internet-gateway", "ec2:key-pair", "ec2:launch-template", "ec2:network-acl", "ec2:route-table",
	"ec2:security-group", "ec2:subnet", "ec2:volume", "ec2:vpc", "ecr:repository", "lambda:function",
	"route53:healthcheck", "route53:hostedzone", "s3:bucket", "sns:topic", "sqs:queue"}

func listSupportedResourceTypes(*core.Ctx, *http.Request) (any, error) {
	out := []map[string]string{}
	for _, t := range supported {
		svc, _, _ := strings.Cut(t, ":")
		out = append(out, map[string]string{"Service": svc, "ResourceType": t})
	}
	return map[string]any{"ResourceTypes": out}, nil
}

func getAccountLevelServiceConfiguration(*core.Ctx, *http.Request) (any, error) {
	return map[string]any{"OrgConfiguration": map[string]string{"AWSServiceAccessStatus": "DISABLED"}}, nil
}

// ---- query language ----

// term is a keyword, or a filter when prefix is set; value is lowercase.
type term struct {
	neg           bool
	prefix, value string
}

var prefixes = []string{"service", "resourcetype", "region", "accountid", "id", "tag", "tag.key", "tag.value"}

// parse splits a query or filter string into terms: whitespace separates
// them unless double-quoted, a leading "-" negates one, and "prefix:value"
// with a known prefix is a filter. Matching ignores case.
func parse(q string) ([]term, error) {
	var words []string
	var w strings.Builder
	quoted, inWord := false, false
	for _, r := range q + " " {
		switch {
		case r == '"':
			quoted, inWord = !quoted, true
		case quoted || !unicode.IsSpace(r):
			w.WriteRune(r)
			inWord = true
		case inWord:
			words, inWord = append(words, w.String()), false
			w.Reset()
		}
	}
	if quoted {
		return nil, invalid("The query %q has an unmatched double quote.", q)
	}
	var out []term
	for _, w := range words {
		var t term
		if w, t.neg = strings.CutPrefix(w, "-"); w == "" {
			continue
		}
		if p, v, ok := strings.Cut(w, ":"); ok && slices.Contains(prefixes, strings.ToLower(p)) {
			if v == "" {
				return nil, invalid("The filter %s: in %q has no value.", p, q)
			}
			t.prefix, w = strings.ToLower(p), v
		}
		t.value = strings.ToLower(w)
		out = append(out, t)
	}
	return out, nil
}

// matches combines terms as AWS does: positive terms with the same prefix
// are OR'ed (keywords are one group), groups are AND'ed, and no negated term
// may match.
func matches(terms []term, r *resource) bool {
	groups := map[string]bool{}
	for _, t := range terms {
		ok := t.match(r)
		if t.neg && ok {
			return false
		}
		if !t.neg {
			groups[t.prefix] = groups[t.prefix] || ok
		}
	}
	for _, ok := range groups {
		if !ok {
			return false
		}
	}
	return true
}

func (t term) match(r *resource) bool {
	switch t.prefix {
	case "service":
		return like(t.value, r.Service)
	case "resourcetype":
		return like(t.value, r.ResourceType)
	case "region":
		return like(t.value, r.Region)
	case "accountid":
		return like(t.value, r.OwningAccountId)
	case "id":
		return like(t.value, r.id)
	case "tag":
		if t.value == "none" || t.value == "all" {
			return (len(r.tags) > 0) == (t.value == "all")
		}
		k, v, kv := strings.Cut(t.value, "=")
		for tk, tv := range r.tags {
			if like(k, tk) && (!kv || like(v, tv)) {
				return true
			}
		}
		return false
	case "tag.key", "tag.value":
		for tk, tv := range r.tags {
			if t.prefix == "tag.value" {
				tk = tv
			}
			if like(t.value, tk) {
				return true
			}
		}
		return false
	}
	kw := strings.TrimSuffix(t.value, "*") // keywords already match substrings
	if strings.Contains(strings.ToLower(r.Arn), kw) {
		return true
	}
	for tk, tv := range r.tags {
		if strings.Contains(strings.ToLower(tk), kw) || strings.Contains(strings.ToLower(tv), kw) {
			return true
		}
	}
	return false
}

// like matches s against a lowercase value: exactly, or by prefix when the value ends in "*".
func like(v, s string) bool {
	s = strings.ToLower(s)
	if p, ok := strings.CutSuffix(v, "*"); ok {
		return strings.HasPrefix(s, p)
	}
	return s == v
}

// ---- helpers, console ----

// page returns the items after the offset token: at most MaxResults, 50 by default.
func page[T any](items []T, in pageIn) ([]T, string, error) {
	off, err := strconv.Atoi(cmp.Or(in.NextToken, "0"))
	switch n := cmp.Or(in.MaxResults, 50); {
	case err != nil || off < 0 || off > len(items):
		return nil, "", invalid("The NextToken %q is not valid.", in.NextToken)
	case n < 1 || n > 1000:
		return nil, "", invalid("MaxResults must be between 1 and 1000.")
	case len(items)-off > n:
		return items[off : off+n], strconv.Itoa(off + n), nil
	}
	return items[off:], "", nil
}

func list(out map[string]any, next string) map[string]any {
	if next != "" {
		out["NextToken"] = next
	}
	return out
}

// Resources lists indexes and views for the console.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	it := core.Table{Name: "Indexes", Columns: []string{"Index", "Region", "Type", "Created"}}
	for _, k := range core.SortedKeys(s.st.Indexes) {
		ix := s.st.Indexes[k]
		it.Rows = append(it.Rows, []string{ix.Arn, ix.Region, ix.Type, ix.CreatedAt.Format(time.DateTime)})
	}
	vt := core.Table{Name: "Views", Columns: []string{"View", "Name", "Filter", "Default"}}
	for _, arn := range core.SortedKeys(s.st.Views) {
		v := s.st.Views[arn]
		_, region, account, _, _ := core.ParseARN(arn)
		vt.Rows = append(vt.Rows, []string{arn, v.ViewName, v.Filters.FilterString, strconv.FormatBool(s.st.Defaults[account+"/"+region] == arn)})
	}
	return []core.Table{it, vt}
}
