package generic

import (
	"regexp"
	"slices"
)

var geoName = regexp.MustCompile(`^[-._\w]{1,100}$`)

// Amazon Location: resources keep the ARNs AWS gives them (service "geo"). Maps, searches and routes return
// nothing: only the resources themselves are kept.
func init() {
	arnTemplates["location/Map"] = "map/{MapName}"
	arnTemplates["location/PlaceIndex"] = "place-index/{IndexName}"
	arnTemplates["location/RouteCalculator"] = "route-calculator/{CalculatorName}"
	arnTemplates["location/GeofenceCollection"] = "geofence-collection/{CollectionName}"
	arnTemplates["location/Tracker"] = "tracker/{TrackerName}"

	// Names are letters, digits, hyphen, period and underscore; the real service says so with a ValidationException.
	for op, member := range map[string]string{"CreateMap": "MapName", "CreatePlaceIndex": "IndexName", "CreateRouteCalculator": "CalculatorName",
		"CreateGeofenceCollection": "CollectionName", "CreateTracker": "TrackerName"} {
		Override("location", op, func(c *Call) (map[string]any, error) {
			if !geoName.MatchString(str(c.In[member])) {
				return nil, c.Invalid("%s %q is not valid: use letters, digits, hyphen, period and underscore.", member, str(c.In[member]))
			}
			return c.Default()
		})
	}
	// A tracker's consumers are the ARNs of geofence collections it is linked to.
	Override("location", "AssociateTrackerConsumer", func(c *Call) (map[string]any, error) {
		t, err := locTracker(c)
		if err != nil {
			return nil, err
		}
		arn := str(c.In["ConsumerArn"])
		if slices.Contains(locConsumers(t), arn) {
			return nil, c.Exists("%s is already a consumer of the tracker.", arn)
		}
		t.Data["_consumers"] = append(asList(t.Data["_consumers"]), arn)
		c.Put(t)
		return map[string]any{}, nil
	})
	Override("location", "DisassociateTrackerConsumer", func(c *Call) (map[string]any, error) {
		t, err := locTracker(c)
		if err != nil {
			return nil, err
		}
		var keep []any
		for _, a := range locConsumers(t) {
			if a != str(c.In["ConsumerArn"]) {
				keep = append(keep, a)
			}
		}
		t.Data["_consumers"] = keep
		c.Put(t)
		return map[string]any{}, nil
	})
	Override("location", "ListTrackerConsumers", func(c *Call) (map[string]any, error) {
		t, err := locTracker(c)
		if err != nil {
			return nil, err
		}
		page, next := s2Page(asList(t.Data["_consumers"]), c.In["MaxResults"], str(c.In["NextToken"]))
		out := map[string]any{"ConsumerArns": page}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	})
}

func locTracker(c *Call) (*Resource, error) {
	t := c.Get("Tracker", str(c.In["TrackerName"]))
	if t == nil {
		return nil, c.NotFound("Tracker %s not found.", str(c.In["TrackerName"]))
	}
	if t.Data == nil {
		t.Data = map[string]any{}
	}
	return t, nil
}

func locConsumers(t *Resource) []string { return s2Names(t.Data["_consumers"]) }
