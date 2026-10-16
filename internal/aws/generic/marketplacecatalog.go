package generic

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Marketplace Catalog: entities (products and offers) and the change sets that
// create and change them. A change set applies as it is started (so one is
// never preparing and CancelChangeSet always fails with ResourceInUse): a Create*
// change makes an entity of the change's type, any other change updates the
// entity it names, merging its Details into the entity's, and the whole change
// set fails, applying nothing, when one change does not fit (ENTITY_NOT_FOUND,
// ENTITY_ALREADY_EXISTS, INVALID_DETAILS). Intent VALIDATE checks without
// applying. Details are not checked against the real product schemas
// (ponytail: any JSON object is accepted). Resource policies and tags are the
// engine's, on the ARNs of entities and change sets.
func init() {
	const svc = "marketplace-catalog"
	Override(svc, "StartChangeSet", costStartChangeSet)
	Override(svc, "DescribeChangeSet", func(c *Call) (map[string]any, error) {
		cs, err := costChangeSetOf(c)
		if err != nil {
			return nil, err
		}
		d := costChangeSetFields(cs)
		d["ChangeSet"] = cs.Data["Changes"]
		d["Intent"] = cs.Data["Intent"]
		d["FailureDescription"] = cs.Data["FailureDescription"]
		return d, nil
	})
	Override(svc, "ListChangeSets", func(c *Call) (map[string]any, error) {
		if err := costCatalogOf(c); err != nil {
			return nil, err
		}
		sets := c.List("ChangeSet")
		if sort := costCmap(c.In["Sort"]); str(sort["SortOrder"]) == "DESCENDING" {
			slices.Reverse(sets)
		}
		var items []any
	next:
		for _, cs := range sets {
			for _, f := range asList(c.In["FilterList"]) {
				fm := costCmap(f)
				have := map[string]string{"ChangeSetName": cs.Name, "Status": str(cs.Data["Status"]), "EntityId": strings.Join(costStrsOf(asList(cs.Data["EntityIdList"])), ",")}[str(fm["Name"])]
				if !slices.ContainsFunc(asList(fm["ValueList"]), func(v any) bool { return strings.Contains(","+have+",", ","+str(v)+",") }) {
					continue next
				}
			}
			s := costChangeSetFields(cs)
			s["EntityIdList"] = cs.Data["EntityIdList"]
			items = append(items, s)
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 20)
		return map[string]any{"ChangeSetSummaryList": page, "NextToken": next}, nil
	})
	Override(svc, "CancelChangeSet", func(c *Call) (map[string]any, error) {
		cs, err := costChangeSetOf(c)
		if err != nil {
			return nil, err
		}
		return nil, c.costShapeErr("ResourceInUseException", "Change set "+cs.ID+" is "+strings.ToLower(str(cs.Data["Status"]))+" and cannot be cancelled")
	})
	Override(svc, "DescribeEntity", func(c *Call) (map[string]any, error) {
		if err := costCatalogOf(c); err != nil {
			return nil, err
		}
		e := c.Get("Entity", costBaseID(str(c.In["EntityId"])))
		if e == nil {
			return nil, c.NotFound("Entity %s was not found", str(c.In["EntityId"]))
		}
		return costEntityView(e), nil
	})
	Override(svc, "BatchDescribeEntities", func(c *Call) (map[string]any, error) {
		found, failed := map[string]any{}, map[string]any{}
		for _, r := range asList(c.In["EntityRequestList"]) {
			rm := costCmap(r)
			id := str(rm["EntityId"])
			if str(rm["Catalog"]) != "AWSMarketplace" {
				failed[id] = map[string]any{"ErrorCode": "ENTITY_NOT_SUPPORTED", "ErrorMessage": "Catalog " + str(rm["Catalog"]) + " is not supported"}
			} else if e := c.Get("Entity", costBaseID(id)); e == nil {
				failed[id] = map[string]any{"ErrorCode": "ENTITY_NOT_FOUND", "ErrorMessage": "Entity " + id + " was not found"}
			} else {
				v := costEntityView(e)
				delete(v, "Details")
				found[id] = v
			}
		}
		return map[string]any{"EntityDetails": found, "Errors": failed}, nil
	})
	Override(svc, "ListEntities", func(c *Call) (map[string]any, error) {
		if err := costCatalogOf(c); err != nil {
			return nil, err
		}
		var items []any
		for _, e := range c.List("Entity") {
			if costEntityBase(str(e.Data["EntityType"])) != str(c.In["EntityType"]) {
				continue
			}
			title := str(costCmap(e.Data["DetailsDocument"])["ProductTitle"])
			if title == "" {
				title = str(costCmap(e.Data["DetailsDocument"])["Name"])
			}
			if title == "" {
				title = e.ID
			}
			s := map[string]any{"Name": title, "EntityType": e.Data["EntityType"], "EntityId": e.ID, "EntityArn": e.ARN, "LastModifiedDate": e.Data["LastModifiedDate"], "Visibility": "Draft"}
			if strings.HasSuffix(costEntityBase(str(e.Data["EntityType"])), "Product") {
				s[costEntityBase(str(e.Data["EntityType"]))+"Summary"] = map[string]any{"ProductTitle": title, "Visibility": "Draft"}
			}
			if !costFilterEntity(c.In["FilterList"], e, title) {
				continue
			}
			items = append(items, s)
		}
		page, next := costPageOf(items, c.In["NextToken"], c.In["MaxResults"], 50)
		return map[string]any{"EntitySummaryList": page, "NextToken": next}, nil
	})
	// Entities and change sets are not types the engine knows, so it does not fill their tags.
	Override(svc, "ListTagsForResource", func(c *Call) (map[string]any, error) {
		c.Svc.mu.Lock()
		r := c.Svc.byARNLocked(c.Account, str(c.In["ResourceArn"]))
		var tags map[string]string
		if r != nil {
			tags = maps.Clone(r.Tags)
		}
		c.Svc.mu.Unlock()
		if r == nil {
			return nil, c.NotFound("Resource %s was not found", str(c.In["ResourceArn"]))
		}
		return map[string]any{"ResourceArn": c.In["ResourceArn"], "Tags": tagsAs(c.op.out.Members["Tags"].sh, tags)}, nil
	})
	// A policy belongs to an entity or a change set.
	Override(svc, "PutResourcePolicy", func(c *Call) (map[string]any, error) {
		c.Svc.mu.Lock()
		found := c.Svc.byARNLocked(c.Account, str(c.In["ResourceArn"])) != nil
		c.Svc.mu.Unlock()
		if !found {
			return nil, c.NotFound("Resource %s was not found", str(c.In["ResourceArn"]))
		}
		return c.Default()
	})
}

func costCatalogOf(c *Call) error {
	if cat := str(c.In["Catalog"]); cat != "AWSMarketplace" {
		return c.Invalid("Catalog %s is not supported; use AWSMarketplace", cat)
	}
	return nil
}

func costChangeSetOf(c *Call) (*Resource, error) {
	if err := costCatalogOf(c); err != nil {
		return nil, err
	}
	cs := c.Get("ChangeSet", str(c.In["ChangeSetId"]))
	if cs == nil {
		return nil, c.NotFound("Change set %s was not found", str(c.In["ChangeSetId"]))
	}
	return cs, nil
}

// shapeError is the error of a shape of the model, by name.
func (c *Call) costShapeErr(name, msg string) error {
	if sh := c.m.shapes[name]; sh != nil {
		return c.m.shapeErr(sh, msg)
	}
	return core.Errorf(400, name, "%s", msg)
}

// changeSetFields are the members that Describe and List share.
func costChangeSetFields(cs *Resource) map[string]any {
	return map[string]any{"ChangeSetId": cs.ID, "ChangeSetArn": cs.ARN, "ChangeSetName": cs.Name, "StartTime": cs.Data["StartTime"], "EndTime": cs.Data["EndTime"],
		"Status": cs.Data["Status"], "FailureCode": cs.Data["FailureCode"]}
}

func costEntityBase(t string) string { b, _, _ := strings.Cut(t, "@"); return b }
func costBaseID(id string) string    { b, _, _ := strings.Cut(id, "@"); return b }

func costEntityView(e *Resource) map[string]any {
	return map[string]any{"EntityType": e.Data["EntityType"], "EntityIdentifier": e.ID, "EntityArn": e.ARN, "LastModifiedDate": e.Data["LastModifiedDate"],
		"Details": e.Data["Details"], "DetailsDocument": e.Data["DetailsDocument"]}
}

func costFilterEntity(filters any, e *Resource, title string) bool {
	for _, f := range asList(filters) {
		fm := costCmap(f)
		have := map[string]string{"EntityId": e.ID, "Name": title, "ProductTitle": title}[str(fm["Name"])]
		if have != "" && !slices.ContainsFunc(asList(fm["ValueList"]), func(v any) bool { return str(v) == have }) {
			return false
		}
	}
	return true
}

// entityPrefix is how an entity id starts, by type.
func costEntityPrefix(t string) string {
	switch b := costEntityBase(t); {
	case strings.HasSuffix(b, "Product"):
		return "prod-"
	case b == "Offer":
		return "offer-"
	case b == "OfferSet":
		return "offerset-"
	case b == "ResaleAuthorization":
		return "resaleauthz-"
	}
	return "ent-"
}

func costStartChangeSet(c *Call) (map[string]any, error) {
	if err := costCatalogOf(c); err != nil {
		return nil, err
	}
	changes := asList(c.In["ChangeSet"])
	if len(changes) == 0 {
		return nil, c.Invalid("ChangeSet must have at least one change")
	}
	token := str(c.In["ClientRequestToken"])
	if token != "" {
		for _, cs := range c.List("ChangeSet") { // the same token is the same change set
			if str(cs.Data["Token"]) == token {
				return map[string]any{"ChangeSetId": cs.ID, "ChangeSetArn": cs.ARN}, nil
			}
		}
	}
	now := time.Now().UTC().Format("2006-01-02T15:04:05Z")
	id := strings.ToLower(core.RandHex(7))[:13]
	apply := str(c.In["Intent"]) != "VALIDATE"
	var summaries []any
	var failure string
	entities := map[string]*Resource{} // as the change set leaves them
	var order []string
	for _, x := range changes {
		ch := costCmap(x)
		ent := costCmap(ch["Entity"])
		etype, eid := str(ent["Type"]), costBaseID(str(ent["Identifier"]))
		sum := map[string]any{"ChangeType": ch["ChangeType"], "ChangeName": ch["ChangeName"], "Details": ch["Details"], "DetailsDocument": ch["DetailsDocument"],
			"Entity": map[string]any{"Type": etype, "Identifier": ent["Identifier"]}}
		fail := func(code, msg string) {
			sum["ErrorDetailList"] = []any{map[string]any{"ErrorCode": code, "ErrorMessage": msg}}
			if failure == "" {
				failure = msg
			}
		}
		var details map[string]any
		if d := str(ch["Details"]); d != "" && json.Unmarshal([]byte(d), &details) != nil {
			fail("INVALID_DETAILS", "Details is not a JSON object")
		}
		for k, v := range costCmap(ch["DetailsDocument"]) {
			if details == nil {
				details = map[string]any{}
			}
			details[k] = v
		}
		e := entities[eid]
		if e == nil && eid != "" {
			e = c.Get("Entity", eid)
		}
		switch isCreate := strings.HasPrefix(str(ch["ChangeType"]), "Create"); {
		case sum["ErrorDetailList"] != nil:
		case isCreate && e != nil:
			fail("ENTITY_ALREADY_EXISTS", "Entity "+eid+" already exists")
		case isCreate:
			if eid == "" {
				eid = costEntityPrefix(etype) + strings.ToLower(core.RandHex(7))[:13]
			}
			e = &Resource{Type: "Entity", Key: eid, IDs: map[string]string{"EntityId": eid}, ID: eid, Account: c.Account, Region: c.key(), Created: time.Now().UTC(),
				ARN: core.ARN("aws-marketplace", c.Region, c.Account, "AWSMarketplace/"+costEntityBase(etype)+"/"+eid), Data: map[string]any{"EntityType": etype, "DetailsDocument": map[string]any{}}}
		case e == nil:
			fail("ENTITY_NOT_FOUND", "Entity "+eid+" was not found")
		}
		if e != nil && sum["ErrorDetailList"] == nil {
			if etype != "" && costEntityBase(etype) != costEntityBase(str(e.Data["EntityType"])) {
				fail("INVALID_ENTITY_TYPE", "Entity "+eid+" is a "+str(e.Data["EntityType"]))
			}
			doc := costCmap(e.Data["DetailsDocument"])
			for k, v := range details {
				doc[k] = v
			}
			b, _ := json.Marshal(doc)
			e.Data["DetailsDocument"], e.Data["Details"], e.Data["LastModifiedDate"] = doc, string(b), now
			if tags := tagMap(ch["EntityTags"]); len(tags) > 0 {
				applyTags(e, tags, nil)
			}
			sum["Entity"] = map[string]any{"Type": etype, "Identifier": eid}
			if entities[eid] == nil {
				order = append(order, eid)
			}
			entities[eid] = e
		}
		summaries = append(summaries, sum)
	}
	status, code := "SUCCEEDED", ""
	if failure != "" {
		status, code = "FAILED", "CLIENT_ERROR"
	} else if apply {
		for _, eid := range order {
			c.Put(entities[eid])
		}
	}
	arn := core.ARN("aws-marketplace", c.Region, c.Account, "AWSMarketplace/ChangeSet/"+id)
	cs := &Resource{Type: "ChangeSet", Key: id, IDs: map[string]string{"ChangeSetId": id}, Name: str(c.In["ChangeSetName"]), ID: id, ARN: arn, Account: c.Account, Region: c.key(),
		Created: time.Now().UTC(), Data: map[string]any{"Changes": summaries, "Status": status, "FailureCode": code, "FailureDescription": failure, "Intent": c.In["Intent"],
			"StartTime": now, "EndTime": now, "EntityIdList": costStrs(order), "Token": token}}
	if cs.Data["Intent"] == nil {
		cs.Data["Intent"] = "APPLY"
	}
	applyTags(cs, tagMap(c.In["ChangeSetTags"]), nil)
	c.Put(cs)
	return map[string]any{"ChangeSetId": id, "ChangeSetArn": arn}, nil
}
