package iot

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRuleSQL(t *testing.T) {
	msg := func(topic, payload string) *message {
		m := &message{topic: topic, client: "dev-1", payload: []byte(payload), doc: undef, at: time.UnixMilli(1700000000123)}
		var doc any
		if json.Unmarshal(m.payload, &doc) == nil {
			m.doc = doc
		}
		return m
	}
	for _, c := range []struct{ sql, topic, payload, want string }{
		{"SELECT * FROM 'a/#'", "a/b", `{"x":1}`, `{"x":1}`},
		{"SELECT * FROM 'a/#'", "a", `not json`, `not json`},
		{"SELECT * FROM 'a/+'", "a/b/c", `{}`, "-"}, // the filter does not match
		{"SELECT *, clientid() AS cid, timestamp() AS ts FROM 'a/b'", "a/b", `{"x":1}`, `{"cid":"dev-1","ts":1700000000123,"x":1}`},
		{"SELECT s.n.v AS deep, upper(name) AS up, lower('AbC') AS low, concat(name, '-', topic(2)) AS c FROM '+/+'", "room/7",
			`{"s":{"n":{"v":[1,2]}},"name":"Lamp"}`, `{"c":"Lamp-7","deep":[1,2],"low":"abc","up":"LAMP"}`},
		{"SELECT x FROM 'a' WHERE NOT (x < 0) AND (s = 'q' OR x >= 10)", "a", `{"x":12,"s":"z"}`, `{"x":12}`},
		{"SELECT x FROM 'a' WHERE NOT (x < 0) AND (s = 'q' OR x >= 10)", "a", `{"x":2,"s":"z"}`, "-"},
		{"SELECT x FROM 'a' WHERE missing > 1 OR x <> 2", "a", `{"x":2}`, "-"}, // undefined OR false
		{"SELECT x, nope FROM 'a' WHERE x = -1.5 AND ok = true", "a", `{"x":-1.5,"ok":true}`, `{"x":-1.5}`},
		// floci #4440: exponent literals such as 1e5 and 2.5E-1 parse.
		{"SELECT temp FROM 'a' WHERE temp > 1e5", "a", `{"temp":200000}`, `{"temp":200000}`},
		{"SELECT temp FROM 'a' WHERE temp > 1e5", "a", `{"temp":5}`, "-"},
		{"SELECT temp FROM 'a' WHERE temp < 2.5E-1", "a", `{"temp":0.2}`, `{"temp":0.2}`},
		{"select topic(3) as t from 'a/#' where topic(1) = 'a'", "a/b/c", `{}`, `{"t":"c"}`},
	} {
		q, err := parse(c.sql)
		if err != nil {
			t.Fatalf("%s: %v", c.sql, err)
		}
		got := "-"
		if out, ok := q.eval(msg(c.topic, c.payload)); ok && matches(q.from, c.topic) {
			got = string(out)
		}
		if got != c.want {
			t.Errorf("%s on %s %s = %s, want %s", c.sql, c.topic, c.payload, got, c.want)
		}
	}
	for _, bad := range []string{"SELECT FROM 'a'", "SELECT x FROM a", "SELECT x FROM 'a' WHERE", "SELECT nope(1) FROM 'a'",
		"SELECT x FROM 'a/#/b'", "SELECT x FROM 'a' WHERE x = 'open", "UPDATE x"} {
		if _, err := parse(bad); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
	if s := substitute("logs/${topic(2)}/${id}-${missing}.json", msg("a/b", `{"id":7}`), []byte(`{}`)); s != "logs/b/7-.json" {
		t.Errorf("substitute = %s", s)
	}
}
