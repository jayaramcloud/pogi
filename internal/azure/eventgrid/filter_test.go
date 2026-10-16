package eventgrid

import (
	"encoding/json"
	"testing"
)

func TestFilters(t *testing.T) {
	e := Event{ID: "1", Subject: "/blobServices/default/containers/Photos/blobs/cat.PNG", EventType: "Microsoft.Storage.BlobCreated",
		DataVersion: "1.0", Data: json.RawMessage(`{"api":"PutBlob","size":42,"ok":true,"nested":{"color":"Red"},"nothing":null}`)}
	adv := func(op, key string, value any, values ...any) Filter {
		return Filter{Advanced: []AdvancedFilter{{Operator: op, Key: key, Value: value, Values: values}}}
	}
	for name, c := range map[string]struct {
		f    Filter
		want bool
	}{
		"empty":                   {Filter{}, true},
		"type in":                 {Filter{IncludedEventTypes: []string{"x", "microsoft.storage.blobcreated"}}, true},
		"type out":                {Filter{IncludedEventTypes: []string{"Microsoft.Storage.BlobDeleted"}}, false},
		"begins, any case":        {Filter{SubjectBeginsWith: "/BLOBSERVICES/default/containers/photos/"}, true},
		"begins, case sensitive":  {Filter{SubjectBeginsWith: "/BLOBSERVICES/", IsSubjectCaseSensitive: true}, false},
		"ends":                    {Filter{SubjectEndsWith: ".png"}, true},
		"ends, case sensitive":    {Filter{SubjectEndsWith: ".png", IsSubjectCaseSensitive: true}, false},
		"begins and ends":         {Filter{SubjectBeginsWith: "/blobServices", SubjectEndsWith: ".jpg"}, false},
		"StringIn":                {adv("StringIn", "data.api", nil, "PutBlob", "x"), true},
		"StringIn, case":          {adv("StringIn", "data.api", nil, "putblob"), true},
		"StringNotIn":             {adv("StringNotIn", "data.api", nil, "PutBlob"), false},
		"StringBeginsWith":        {adv("StringBeginsWith", "subject", nil, "/blobservices/"), true},
		"StringEndsWith":          {adv("StringEndsWith", "data.nested.color", nil, "ed"), true},
		"StringContains":          {adv("StringContains", "eventType", nil, "BlobCreated"), true},
		"StringNotContains":       {adv("StringNotContains", "eventType", nil, "BlobCreated"), false},
		"NumberGreaterThan":       {adv("NumberGreaterThan", "data.size", 41.0), true},
		"NumberGreaterThan, =":    {adv("NumberGreaterThan", "data.size", 42.0), false},
		"NumberGreaterThanOrEq":   {adv("NumberGreaterThanOrEquals", "data.size", 42.0), true},
		"NumberLessThan":          {adv("NumberLessThan", "data.size", 10.0), false},
		"NumberIn":                {adv("NumberIn", "data.size", nil, 1.0, 42.0), true},
		"NumberNotIn":             {adv("NumberNotIn", "data.size", nil, 42.0), false},
		"NumberInRange":           {adv("NumberInRange", "data.size", nil, []any{0.0, 10.0}, []any{40.0, 50.0}), true},
		"NumberNotInRange":        {adv("NumberNotInRange", "data.size", nil, []any{0.0, 10.0}), true},
		"BoolEquals":              {adv("BoolEquals", "data.ok", true), true},
		"BoolEquals, false":       {adv("BoolEquals", "data.ok", false), false},
		"IsNullOrUndefined, null": {adv("IsNullOrUndefined", "data.nothing", nil), true},
		"IsNullOrUndefined, none": {adv("IsNullOrUndefined", "data.missing", nil), true},
		"IsNotNull":               {adv("IsNotNull", "data.api", nil), true},
		"missing key":             {adv("StringIn", "data.missing", nil, "x"), false},
		"missing key, NotIn":      {adv("StringNotIn", "data.missing", nil, "x"), false},
		"wrong type":              {adv("NumberIn", "data.api", nil, 1.0), false},
		"unknown operator":        {adv("Frobnicate", "data.api", "x"), false},
		"all must hold": {Filter{Advanced: []AdvancedFilter{
			{Operator: "StringIn", Key: "data.api", Values: []any{"PutBlob"}}, {Operator: "NumberLessThan", Key: "data.size", Value: 1.0}}}, false},
	} {
		if got := c.f.match(e); got != c.want {
			t.Errorf("%s: match = %v, want %v", name, got, c.want)
		}
	}
}
