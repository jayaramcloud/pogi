package dynamodb

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"hash/fnv"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// writeInput is the input of PutItem, UpdateItem and DeleteItem, and of each
// TransactWriteItems action.
type writeInput struct {
	TableName                           string
	Key                                 Item
	Item                                Item
	ConditionExpression                 string
	UpdateExpression                    string
	ExpressionAttributeNames            map[string]string
	ExpressionAttributeValues           map[string]*Value
	ReturnValues                        string
	ReturnValuesOnConditionCheckFailure string
	ReturnConsumedCapacity              string
	Expected                            map[string]expected   // legacy
	AttributeUpdates                    map[string]attrUpdate // legacy
	ConditionalOperator                 string                // legacy
}

// change is a planned write: the item before and after (nil when absent).
type change struct {
	t        *Table
	k        string
	old, new Item
	paths    []path // UpdateItem targets, for UPDATED_* return values
	fail     error  // errCondFailed, or an error found while applying the update
}

var errCondFailed = errors.New("conditional check failed")

// plan works out one write without applying it; lock held. op is Put,
// Update, Delete or ConditionCheck. Request errors are returned; a failed
// condition or update is left in change.fail for the caller to report.
func (s *Service) plan(c *core.Ctx, op string, in *writeInput) (*change, error) {
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	x, err := newExprs(in.ExpressionAttributeNames, in.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}
	key := in.Key
	if op == "Put" {
		if err := checkItem(in.Item); err != nil {
			return nil, err
		}
		if err := t.fits(in.Item); err != nil {
			return nil, err
		}
		key = pick(in.Item, keyNames(t.Keys)...)
	} else if err := t.checkKey(key); err != nil {
		return nil, err
	}
	cond, err := x.cond(in.ConditionExpression, "ConditionExpression")
	if err != nil {
		return nil, err
	}
	if cond == nil {
		if cond, err = expectedCond(in.Expected, in.ConditionalOperator); err != nil {
			return nil, err
		}
	}
	var acts []action
	if op == "Update" {
		if acts, err = x.update(in.UpdateExpression); err != nil {
			return nil, err
		}
		if acts == nil {
			if acts, err = legacyUpdates(in.AttributeUpdates); err != nil {
				return nil, err
			}
		}
		for _, a := range acts {
			if slices.Contains(keyNames(t.Keys), a.path[0].(string)) {
				return nil, errVal("One or more parameter values were invalid: Cannot update attribute %s. This attribute is part of the key", a.path[0])
			}
		}
	}
	if err := x.unused(); err != nil {
		return nil, err
	}
	ch := &change{t: t, k: t.key(key)}
	ch.old = t.Items[ch.k]
	if cond != nil && !cond.test(ch.old) {
		ch.fail = errCondFailed
		return ch, nil
	}
	switch op {
	case "Put":
		ch.new = in.Item
	case "Update":
		base := ch.old
		if base == nil {
			base = key
		}
		if ch.new, ch.fail = apply(base, acts); ch.fail == nil {
			ch.fail = t.fits(ch.new)
		}
		for _, a := range acts {
			ch.paths = append(ch.paths, a.path)
		}
	case "ConditionCheck":
		ch.new = ch.old
	}
	return ch, nil
}

// failure renders change.fail for a single-item write.
func (ch *change) failure(returnValues string) error {
	if ch.fail != errCondFailed {
		return ch.fail
	}
	e := &apiError{err: core.Errorf(400, "ConditionalCheckFailedException", "The conditional request failed")}
	if returnValues == "ALL_OLD" && ch.old != nil {
		e.extra = map[string]any{"Item": ch.old}
	}
	return e
}

// commit applies a planned change and records it in the stream; lock held.
func (s *Service) commit(ch *change) {
	if ch.new == nil {
		delete(ch.t.Items, ch.k)
		s.j.Delete(ikey(ch.t, ch.k))
	} else {
		ch.t.Items[ch.k] = ch.new
		s.j.Put(ikey(ch.t, ch.k), ch.new)
	}
	s.record(ch.t, ch.old, ch.new, nil)
	s.replicate(ch.t, ch.k, ch.old, ch.new)
}

func consumed(mode, table string, units float64) map[string]any {
	switch mode {
	case "TOTAL":
		return map[string]any{"TableName": table, "CapacityUnits": units}
	case "INDEXES":
		return map[string]any{"TableName": table, "CapacityUnits": units, "Table": map[string]any{"CapacityUnits": units}}
	}
	return nil
}

// capacities renders per-table units as a ConsumedCapacity list (nil when not asked for).
func capacities(mode string, units map[string]float64) []map[string]any {
	var out []map[string]any
	for _, name := range core.SortedKeys(units) {
		if cc := consumed(mode, name, units[name]); cc != nil {
			out = append(out, cc)
		}
	}
	return out
}

func writeUnits(ch *change) float64 {
	return math.Ceil(float64(max(itemSize(ch.old), itemSize(ch.new), 1)) / 1024)
}

func readUnits(size int, consistent bool) float64 {
	u := math.Ceil(float64(max(size, 1)) / 4096)
	if !consistent {
		u /= 2
	}
	return u
}

func (s *Service) putItem(c *core.Ctx, in *writeInput) (any, error) {
	return s.write(c, "Put", in, "NONE", "ALL_OLD")
}

func (s *Service) updateItem(c *core.Ctx, in *writeInput) (any, error) {
	return s.write(c, "Update", in, "NONE", "ALL_OLD", "UPDATED_OLD", "ALL_NEW", "UPDATED_NEW")
}

func (s *Service) deleteItem(c *core.Ctx, in *writeInput) (any, error) {
	return s.write(c, "Delete", in, "NONE", "ALL_OLD")
}

func (s *Service) write(c *core.Ctx, op string, in *writeInput, returns ...string) (any, error) {
	if in.ReturnValues != "" && !slices.Contains(returns, in.ReturnValues) {
		return nil, errVal("Return values set to invalid value")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ch, err := s.plan(c, op, in)
	if err != nil {
		return nil, err
	}
	if ch.fail != nil {
		return nil, ch.failure(in.ReturnValuesOnConditionCheckFailure)
	}
	s.commit(ch)
	var attrs Item
	switch in.ReturnValues {
	case "ALL_OLD":
		attrs = ch.old
	case "ALL_NEW":
		attrs = ch.new
	case "UPDATED_OLD":
		attrs = project(ch.old, ch.paths)
	case "UPDATED_NEW":
		attrs = project(ch.new, ch.paths)
	}
	out := map[string]any{}
	if len(attrs) > 0 {
		out["Attributes"] = attrs
	}
	if cc := consumed(in.ReturnConsumedCapacity, ch.t.Name, writeUnits(ch)); cc != nil {
		out["ConsumedCapacity"] = cc
	}
	return out, nil
}

// readInput is the input of GetItem, Query and Scan (and their batch and
// transaction forms).
type readInput struct {
	TableName                 string
	IndexName                 string
	Key                       Item
	Keys                      []Item // BatchGetItem
	Select                    string
	Limit                     int
	ConsistentRead            bool
	ScanIndexForward          *bool
	KeyConditionExpression    string
	FilterExpression          string
	ProjectionExpression      string
	ExpressionAttributeNames  map[string]string
	ExpressionAttributeValues map[string]*Value
	ExclusiveStartKey         Item
	Segment                   *int
	TotalSegments             *int
	ReturnConsumedCapacity    string
	AttributesToGet           []string             // legacy
	KeyConditions             map[string]condition // legacy
	QueryFilter               map[string]condition // legacy
	ScanFilter                map[string]condition // legacy
	ConditionalOperator       string               // legacy
}

func (s *Service) getItem(c *core.Ctx, in *readInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.get(c, in)
}

// get serves GetItem; lock held.
func (s *Service) get(c *core.Ctx, in *readInput) (map[string]any, error) {
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	x, err := newExprs(in.ExpressionAttributeNames, in.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}
	if err := in.mixedParams(); err != nil {
		return nil, err
	}
	if err := t.checkKey(in.Key); err != nil {
		return nil, err
	}
	paths, err := x.projection(in.ProjectionExpression)
	if err != nil {
		return nil, err
	}
	paths = attrPaths(paths, in.AttributesToGet)
	if err := x.unused(); err != nil {
		return nil, err
	}
	out := map[string]any{}
	it := t.Items[t.key(in.Key)]
	if it != nil {
		if paths != nil {
			it = project(it, paths)
		}
		out["Item"] = it
	}
	if cc := consumed(in.ReturnConsumedCapacity, t.Name, readUnits(itemSize(it), in.ConsistentRead)); cc != nil {
		out["ConsumedCapacity"] = cc
	}
	return out, nil
}

// mixedParams refuses a request that uses both legacy and expression parameters.
func (in *readInput) mixedParams() error {
	var legacy, expr []string
	for name, set := range map[string]bool{"AttributesToGet": in.AttributesToGet != nil, "KeyConditions": in.KeyConditions != nil,
		"QueryFilter": in.QueryFilter != nil, "ScanFilter": in.ScanFilter != nil, "ConditionalOperator": in.ConditionalOperator != ""} {
		if set {
			legacy = append(legacy, name)
		}
	}
	for name, set := range map[string]bool{"ProjectionExpression": in.ProjectionExpression != "", "FilterExpression": in.FilterExpression != "",
		"KeyConditionExpression": in.KeyConditionExpression != ""} {
		if set {
			expr = append(expr, name)
		}
	}
	if len(legacy) == 0 || len(expr) == 0 {
		return nil
	}
	slices.Sort(legacy)
	slices.Sort(expr)
	return errVal("Can not use both expression and non-expression parameters in the same request: Non-expression parameters: {%s} Expression parameters: {%s}",
		strings.Join(legacy, ", "), strings.Join(expr, ", "))
}

// attrPaths turns the legacy AttributesToGet into projection paths.
func attrPaths(paths []path, attrs []string) []path {
	if paths != nil {
		return paths
	}
	for _, a := range attrs {
		paths = append(paths, path{a})
	}
	return paths
}

func (s *Service) query(c *core.Ctx, in *readInput) (any, error) { return s.read(c, in, true) }
func (s *Service) scan(c *core.Ctx, in *readInput) (any, error)  { return s.read(c, in, false) }

// read serves Query and Scan.
func (s *Service) read(c *core.Ctx, in *readInput, query bool) (any, error) {
	if (in.Segment == nil) != (in.TotalSegments == nil) || in.TotalSegments != nil && (*in.TotalSegments < 1 || *in.Segment < 0 || *in.Segment >= *in.TotalSegments) {
		return nil, errVal("The Segment parameter must be at least 0 and less than TotalSegments, and both must be given together")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, err
	}
	keys, global := t.Keys, false
	var ix *index
	if in.IndexName != "" {
		if ix, global = t.index(in.IndexName); ix == nil {
			return nil, errVal("The table does not have the specified index: %s", in.IndexName)
		}
		if keys = ix.KeySchema; global && in.ConsistentRead {
			return nil, errVal("Consistent reads are not supported on global secondary indexes")
		}
	}
	if err := in.mixedParams(); err != nil {
		return nil, err
	}
	x, err := newExprs(in.ExpressionAttributeNames, in.ExpressionAttributeValues)
	if err != nil {
		return nil, err
	}
	nh := hashCount(keys)
	hashes := keyNames(keys)[:nh]
	var pks []*Value
	var sks []*node
	if query {
		kc, err := x.cond(in.KeyConditionExpression, "KeyConditionExpression")
		if err == nil && kc == nil {
			kc, err = legacyConds(in.KeyConditions, "")
		}
		if err != nil {
			return nil, err
		}
		if kc == nil {
			return nil, errVal("Either the KeyConditions or KeyConditionExpression parameter must be specified in the request.")
		}
		if pks, sks, err = keyCond(kc, keys, t); err != nil {
			return nil, err
		}
	}
	filter, err := x.cond(in.FilterExpression, "FilterExpression")
	if legacy := in.ScanFilter; err == nil && filter == nil {
		if query {
			legacy = in.QueryFilter
		}
		filter, err = legacyConds(legacy, in.ConditionalOperator)
	}
	if err != nil {
		return nil, err
	}
	if query && in.FilterExpression != "" {
		for _, k := range keyNames(keys) {
			if filter.uses(k) {
				return nil, errVal("Filter Expression can only contain non-primary key attributes: Primary key attribute: %s", k)
			}
		}
	}
	paths, err := x.projection(in.ProjectionExpression)
	if err != nil {
		return nil, err
	}
	paths = attrPaths(paths, in.AttributesToGet)
	if err := x.unused(); err != nil {
		return nil, err
	}
	sel := in.Select
	switch {
	case sel == "" && paths != nil:
		sel = "SPECIFIC_ATTRIBUTES"
	case sel == "" && ix != nil:
		sel = "ALL_PROJECTED_ATTRIBUTES"
	case sel == "":
		sel = "ALL_ATTRIBUTES"
	case sel == "ALL_PROJECTED_ATTRIBUTES" && ix == nil:
		return nil, errVal("ALL_PROJECTED_ATTRIBUTES can be used only when Querying using an IndexName")
	case sel == "ALL_ATTRIBUTES" && global && ix.Projection.ProjectionType != "ALL":
		return nil, errVal("One or more parameter values were invalid: Select type ALL_ATTRIBUTES is not supported for global secondary index %s because its projection type is not ALL", ix.IndexName)
	}

	// Items come in key order: the index key, then the table key to break ties.
	order := keyNames(t.Keys)
	if ix != nil {
		order = uniq(slices.Concat(keyNames(ix.KeySchema), order))
	}
	lastKey := order
	if query {
		order = order[nh:] // one partition
	}
	byOrder := byKeys(order, query && in.ScanIndexForward != nil && !*in.ScanIndexForward)
	var items []Item
	for _, it := range t.Items {
		switch {
		case ix != nil && !inIndex(ix, it):
		case query && !inPartition(it, hashes, pks, sks):
		case in.TotalSegments != nil && segment(it[hashes[0]], *in.TotalSegments) != *in.Segment:
		default:
			items = append(items, it)
		}
	}
	slices.SortFunc(items, byOrder)
	if start := in.ExclusiveStartKey; start != nil {
		if err := checkItem(start); err != nil {
			return nil, err
		}
		for _, n := range lastKey {
			if start[n] == nil || start[n].kind() != t.attrType(n) {
				return nil, errVal("The provided starting key is invalid: The provided key element does not match the schema")
			}
		}
		for i, h := range hashes {
			if query && !equal(start[h], pks[i]) {
				return nil, errVal("The provided starting key is outside query boundaries based on provided conditions")
			}
		}
		for len(items) > 0 && byOrder(items[0], start) <= 0 {
			items = items[1:]
		}
	}

	found, scanned, size := []Item{}, 0, 0
	out := map[string]any{}
	for _, it := range items {
		if in.Limit > 0 && scanned == in.Limit || size > 1<<20 {
			break
		}
		scanned++
		size += itemSize(it)
		if in.Limit > 0 && scanned == in.Limit || size > 1<<20 {
			out["LastEvaluatedKey"] = pick(it, lastKey...)
		}
		view := it
		if global { // a GSI holds only its projection; an LSI can fetch from the table
			view = t.indexView(ix, it)
		}
		if filter != nil && !filter.test(view) {
			continue
		}
		switch sel {
		case "SPECIFIC_ATTRIBUTES":
			view = project(view, paths)
		case "ALL_PROJECTED_ATTRIBUTES":
			view = t.indexView(ix, it)
		}
		found = append(found, view)
	}
	out["Count"], out["ScannedCount"] = len(found), scanned
	if sel != "COUNT" {
		out["Items"] = found
	}
	if cc := consumed(in.ReturnConsumedCapacity, t.Name, readUnits(size, in.ConsistentRead)); cc != nil {
		out["ConsumedCapacity"] = cc
	}
	return out, nil
}

func uniq(xs []string) []string {
	var out []string
	for _, x := range xs {
		if !slices.Contains(out, x) {
			out = append(out, x)
		}
	}
	return out
}

// segment assigns a partition key to one of n parallel scan segments.
func segment(pk *Value, n int) int {
	h := fnv.New32a()
	h.Write([]byte(keyOf(Item{"k": pk}, "k")))
	return int(h.Sum32() % uint32(n))
}

// hashCount is the number of partition key attributes of a key schema:
// one, except in a global secondary index with multi-attribute keys.
func hashCount(ks []keyElem) int {
	n := 0
	for _, k := range ks {
		if k.KeyType == "HASH" {
			n++
		}
	}
	return n
}

// inPartition reports whether an item has the queried partition key values
// and meets every sort key condition.
func inPartition(it Item, hashes []string, pks []*Value, sks []*node) bool {
	for i, h := range hashes {
		if !equal(it[h], pks[i]) {
			return false
		}
	}
	for _, sk := range sks {
		if !sk.test(it) {
			return false
		}
	}
	return true
}

// keyCond splits a key condition into the partition key values (one per
// HASH attribute) and the sort key conditions. Sort key conditions go left
// to right without gaps and only the last one may be a range, as in AWS.
func keyCond(n *node, keys []keyElem, t *Table) (pks []*Value, sks []*node, err error) {
	names := keyNames(keys)
	nh := hashCount(keys)
	var conds []*node
	var walk func(n *node)
	walk = func(n *node) {
		if n.op == "and" {
			walk(n.args[0])
			walk(n.args[1])
		} else {
			conds = append(conds, n)
		}
	}
	walk(n)
	unsupported := errVal("Query key condition not supported")
	pks = make([]*Value, nh)
	rngs := make([]*node, len(names)-nh)
	for _, c := range conds {
		if !slices.Contains([]string{"=", "<", "<=", ">", ">=", "between", "begins_with"}, c.op) ||
			c.args[0].op != "path" || len(c.args[0].path) != 1 || slices.ContainsFunc(c.args[1:], func(a *node) bool { return a.op != "value" }) {
			return nil, nil, unsupported
		}
		name := c.args[0].path[0].(string)
		i := slices.Index(names, name)
		switch {
		case i < 0:
			return nil, nil, errVal("Query condition missed key schema element: %s", names[min(nh, len(names)-1)])
		case i < nh && c.op != "=":
			return nil, nil, unsupported
		case i < nh && pks[i] != nil || i >= nh && rngs[i-nh] != nil:
			return nil, nil, errVal("KeyConditionExpressions must only contain one condition per key")
		case c.op == "begins_with" && t.attrType(name) == "N":
			return nil, nil, errVal("Invalid KeyConditionExpression: Incorrect operand type for operator or function; operator or function: begins_with, operand type: N")
		}
		for _, a := range c.args[1:] {
			if a.val.kind() != t.attrType(name) {
				return nil, nil, errVal("One or more parameter values were invalid: Condition parameter type does not match schema type")
			}
		}
		if i < nh {
			pks[i] = c.args[1].val
		} else {
			rngs[i-nh] = c
		}
	}
	for i, pk := range pks {
		if pk == nil {
			return nil, nil, errVal("Query condition missed key schema element: %s", names[i])
		}
	}
	for i, c := range rngs {
		switch {
		case c == nil:
			if slices.ContainsFunc(rngs[i:], func(c *node) bool { return c != nil }) {
				return nil, nil, errVal("Query condition missed key schema element: %s", names[nh+i])
			}
		case c.op != "=" && slices.ContainsFunc(rngs[i+1:], func(c *node) bool { return c != nil }):
			return nil, nil, errVal("Query key condition not supported: only the last sort key attribute can use a range condition")
		default:
			sks = append(sks, c)
		}
	}
	return pks, sks, nil
}

func (s *Service) batchGet(c *core.Ctx, in *struct {
	RequestItems           map[string]*readInput
	ReturnConsumedCapacity string
}) (any, error) {
	n := 0
	for _, r := range in.RequestItems {
		n += len(r.Keys)
	}
	switch {
	case n == 0:
		return nil, errVal("1 validation error detected: Value at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1")
	case n > 100:
		return nil, errVal("Too many items requested for the BatchGetItem call")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	resp, units := map[string][]Item{}, map[string]float64{}
	for name, r := range in.RequestItems {
		t, err := s.table(c, name)
		if err != nil {
			return nil, err
		}
		resp[name] = []Item{}
		seen, size := map[string]bool{}, 0
		for _, k := range r.Keys {
			got, err := s.get(c, &readInput{TableName: name, Key: k, ProjectionExpression: r.ProjectionExpression,
				ExpressionAttributeNames: r.ExpressionAttributeNames, AttributesToGet: r.AttributesToGet})
			if err != nil {
				return nil, err
			}
			if seen[t.key(k)] {
				return nil, errVal("Provided list of item keys contains duplicates")
			}
			seen[t.key(k)] = true
			if it, ok := got["Item"].(Item); ok {
				resp[name] = append(resp[name], it)
				size += itemSize(it)
			}
		}
		units[name] = readUnits(size, r.ConsistentRead)
	}
	out := map[string]any{"Responses": resp, "UnprocessedKeys": map[string]any{}}
	if caps := capacities(in.ReturnConsumedCapacity, units); caps != nil {
		out["ConsumedCapacity"] = caps
	}
	return out, nil
}

func (s *Service) batchWrite(c *core.Ctx, in *struct {
	RequestItems map[string][]struct {
		PutRequest    *writeInput
		DeleteRequest *writeInput
	}
	ReturnConsumedCapacity string
}) (any, error) {
	n := 0
	for _, reqs := range in.RequestItems {
		n += len(reqs)
	}
	switch {
	case n == 0:
		return nil, errVal("1 validation error detected: Value at 'requestItems' failed to satisfy constraint: Member must have length greater than or equal to 1")
	case n > 25:
		return nil, errVal("Too many items requested for the BatchWriteItem call")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var chs []*change
	seen := map[*Table]map[string]bool{}
	for name, reqs := range in.RequestItems {
		for _, r := range reqs {
			op, w := "Put", r.PutRequest
			if w == nil {
				op, w = "Delete", r.DeleteRequest
			}
			if w == nil || r.PutRequest != nil && r.DeleteRequest != nil {
				return nil, errVal("1 validation error detected: a WriteRequest must contain exactly one of PutRequest or DeleteRequest")
			}
			ch, err := s.plan(c, op, &writeInput{TableName: name, Item: w.Item, Key: w.Key})
			if err != nil {
				return nil, err
			}
			if seen[ch.t] == nil {
				seen[ch.t] = map[string]bool{}
			}
			if seen[ch.t][ch.k] {
				return nil, errVal("Provided list of item keys contains duplicates")
			}
			seen[ch.t][ch.k] = true
			chs = append(chs, ch)
		}
	}
	units := map[string]float64{}
	for _, ch := range chs {
		s.commit(ch)
		units[ch.t.Name] += writeUnits(ch)
	}
	out := map[string]any{"UnprocessedItems": map[string]any{}}
	if caps := capacities(in.ReturnConsumedCapacity, units); caps != nil {
		out["ConsumedCapacity"] = caps
	}
	return out, nil
}

func (s *Service) transactWrite(c *core.Ctx, in *struct {
	TransactItems []struct {
		ConditionCheck, Put, Delete, Update *writeInput
	}
	ClientRequestToken     string
	ReturnConsumedCapacity string
}) (any, error) {
	if n := len(in.TransactItems); n == 0 || n > 100 {
		return nil, errVal("1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100 and greater than or equal to 1")
	}
	b, _ := json.Marshal(in.TransactItems)
	sum := sha256.Sum256(b)
	hash, tk := hex.EncodeToString(sum[:]), c.Account+"/"+in.ClientRequestToken
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.st.Tokens[tk]; ok && in.ClientRequestToken != "" && time.Since(old.At) < 10*time.Minute {
		if old.Hash != hash {
			return nil, core.Errorf(400, "IdempotentParameterMismatchException", "Request with the same client token was made with different parameters")
		}
		return nil, nil // a retry of a transaction that already succeeded
	}
	chs := make([]*change, len(in.TransactItems))
	reasons := make([]map[string]any, len(in.TransactItems))
	var codes []string
	seen := map[string]bool{}
	failed := false
	for i, ti := range in.TransactItems {
		op, w := "ConditionCheck", ti.ConditionCheck
		switch {
		case ti.Put != nil:
			op, w = "Put", ti.Put
		case ti.Delete != nil:
			op, w = "Delete", ti.Delete
		case ti.Update != nil:
			op, w = "Update", ti.Update
		}
		if w == nil {
			return nil, errVal("TransactItems can only contain one of Check, Put, Update or Delete")
		}
		ch, err := s.plan(c, op, w)
		if err != nil {
			return nil, err
		}
		id := ch.t.arn() + "\x00" + ch.k
		if seen[id] {
			return nil, errVal("Transaction request cannot include multiple operations on one item")
		}
		seen[id] = true
		chs[i], reasons[i] = ch, map[string]any{"Code": "None"}
		switch {
		case ch.fail == errCondFailed:
			reasons[i] = map[string]any{"Code": "ConditionalCheckFailed", "Message": "The conditional request failed"}
			if w.ReturnValuesOnConditionCheckFailure == "ALL_OLD" && ch.old != nil {
				reasons[i]["Item"] = ch.old
			}
		case ch.fail != nil:
			reasons[i] = map[string]any{"Code": "ValidationError", "Message": core.AsError(ch.fail).Msg}
		}
		failed = failed || ch.fail != nil
		codes = append(codes, reasons[i]["Code"].(string))
	}
	if failed {
		msg := "Transaction cancelled, please refer cancellation reasons for specific reasons [" + strings.Join(codes, ", ") + "]"
		return nil, &apiError{core.Errorf(400, "TransactionCanceledException", "%s", msg), map[string]any{"CancellationReasons": reasons, "Message": msg}}
	}
	units := map[string]float64{}
	for i, ch := range chs {
		if in.TransactItems[i].ConditionCheck == nil {
			s.commit(ch)
		}
		units[ch.t.Name] += 2 * writeUnits(ch) // transactions cost double
	}
	if in.ClientRequestToken != "" {
		for k, v := range s.st.Tokens {
			if time.Since(v.At) > 10*time.Minute {
				delete(s.st.Tokens, k)
				s.j.Delete("x/" + k)
			}
		}
		s.st.Tokens[tk] = token{hash, time.Now()}
		s.j.Put("x/"+tk, s.st.Tokens[tk])
	}
	if caps := capacities(in.ReturnConsumedCapacity, units); caps != nil {
		return map[string]any{"ConsumedCapacity": caps}, nil
	}
	return nil, nil
}

func (s *Service) transactGet(c *core.Ctx, in *struct {
	TransactItems []struct{ Get readInput }
}) (any, error) {
	if n := len(in.TransactItems); n == 0 || n > 100 {
		return nil, errVal("1 validation error detected: Value at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100 and greater than or equal to 1")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := make([]map[string]any, len(in.TransactItems))
	for i, ti := range in.TransactItems {
		var err error
		if resp[i], err = s.get(c, &ti.Get); err != nil {
			return nil, err
		}
	}
	return map[string]any{"Responses": resp}, nil
}

// ---- legacy (pre-expression) parameters, translated to expression trees ----

type condition struct {
	ComparisonOperator string
	AttributeValueList []*Value
}

type expected struct {
	Value              *Value
	Exists             *bool
	ComparisonOperator string
	AttributeValueList []*Value
}

type attrUpdate struct {
	Action string // PUT, ADD, DELETE
	Value  *Value
}

var legacyOps = map[string]string{"EQ": "=", "NE": "<>", "LE": "<=", "LT": "<", "GE": ">=", "GT": ">", "NOT_NULL": "attribute_exists",
	"NULL": "attribute_not_exists", "CONTAINS": "contains", "NOT_CONTAINS": "contains", "BEGINS_WITH": "begins_with", "BETWEEN": "between", "IN": "in"}

var legacyArgs = map[string]int{"NOT_NULL": 0, "NULL": 0, "BETWEEN": 2, "IN": -1}

func legacyNode(name string, c condition) (*node, error) {
	op, ok := legacyOps[c.ComparisonOperator]
	want, fixed := legacyArgs[c.ComparisonOperator]
	if !fixed {
		want = 1
	}
	if !ok || want >= 0 && len(c.AttributeValueList) != want || want < 0 && len(c.AttributeValueList) == 0 {
		return nil, errVal("One or more parameter values were invalid: Invalid number of argument(s) for the %s ComparisonOperator", c.ComparisonOperator)
	}
	n := &node{op: op, args: []*node{{op: "path", path: path{name}}}}
	for _, v := range c.AttributeValueList {
		if err := v.check(); err != nil {
			return nil, err
		}
		n.args = append(n.args, &node{op: "value", val: v})
	}
	if c.ComparisonOperator == "NOT_CONTAINS" {
		n = &node{op: "not", args: []*node{n}}
	}
	return n, nil
}

// legacyConds joins KeyConditions, QueryFilter or ScanFilter with AND (or OR).
func legacyConds(conds map[string]condition, operator string) (*node, error) {
	var out *node
	for _, name := range core.SortedKeys(conds) {
		n, err := legacyNode(name, conds[name])
		if err != nil {
			return nil, err
		}
		out = join(out, n, operator)
	}
	return out, nil
}

func join(a, b *node, operator string) *node {
	if a == nil {
		return b
	}
	op := "and"
	if operator == "OR" {
		op = "or"
	}
	return &node{op: op, args: []*node{a, b}}
}

// expectedCond translates Expected: {Value}, {Exists: false} or a comparison.
func expectedCond(exp map[string]expected, operator string) (*node, error) {
	var out *node
	for _, name := range core.SortedKeys(exp) {
		e := exp[name]
		c := condition{e.ComparisonOperator, e.AttributeValueList}
		switch {
		case c.ComparisonOperator != "":
		case e.Exists != nil && !*e.Exists:
			c.ComparisonOperator = "NULL"
		case e.Value != nil:
			c = condition{"EQ", []*Value{e.Value}}
		default:
			return nil, errVal("One or more parameter values were invalid: Value must be provided when Exists is true for Attribute: %s", name)
		}
		n, err := legacyNode(name, c)
		if err != nil {
			return nil, err
		}
		out = join(out, n, operator)
	}
	return out, nil
}

// legacyUpdates translates AttributeUpdates into update actions.
func legacyUpdates(updates map[string]attrUpdate) ([]action, error) {
	var acts []action
	for _, name := range core.SortedKeys(updates) {
		u := updates[name]
		a := action{op: map[string]string{"": "SET", "PUT": "SET", "ADD": "ADD", "DELETE": "DELETE"}[u.Action], path: path{name}}
		if u.Value != nil {
			if err := u.Value.check(); err != nil {
				return nil, err
			}
			a.val = &node{op: "value", val: u.Value}
		}
		switch {
		case a.op == "":
			return nil, errVal("1 validation error detected: Value '%s' at 'attributeUpdates.%s.member.action' failed to satisfy constraint: Member must satisfy enum value set: [ADD, PUT, DELETE]", u.Action, name)
		case a.op == "DELETE" && u.Value == nil:
			a.op = "REMOVE"
		case u.Value == nil:
			return nil, errVal("One or more parameter values were invalid: Only DELETE action is allowed when no attribute value is specified")
		}
		acts = append(acts, a)
	}
	return acts, nil
}
