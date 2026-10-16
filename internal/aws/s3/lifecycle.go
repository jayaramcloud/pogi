package s3

import (
	"encoding/xml"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Lifecycle rules. A sweep every second applies them: it expires current versions,
// removes noncurrent versions and expired delete markers, and aborts old multipart
// uploads. S3 does this once a day; pogi does it at once so a test does not wait.
//
// ponytail: the sweep walks every key of every bucket that has rules, under the write
// lock; index by expiry time if a bucket with rules holds hundreds of thousands of keys.
// Transition rules are stored and ignored (there is only one storage class here).

type lifecycleRule struct {
	ID     string `xml:"ID"`
	Status string `xml:"Status"`
	Prefix string `xml:"Prefix"` // the older form, outside Filter
	Filter struct {
		Prefix  string   `xml:"Prefix"`
		Tag     *tagXML  `xml:"Tag"`
		Greater int64    `xml:"ObjectSizeGreaterThan"`
		Less    int64    `xml:"ObjectSizeLessThan"`
		And     struct { // several conditions together
			Prefix  string   `xml:"Prefix"`
			Tags    []tagXML `xml:"Tag"`
			Greater int64    `xml:"ObjectSizeGreaterThan"`
			Less    int64    `xml:"ObjectSizeLessThan"`
		} `xml:"And"`
	} `xml:"Filter"`
	Expiration struct {
		Days         int    `xml:"Days"`
		Date         string `xml:"Date"`
		DeleteMarker bool   `xml:"ExpiredObjectDeleteMarker"`
	} `xml:"Expiration"`
	Noncurrent struct {
		Days  int `xml:"NoncurrentDays"`
		Newer int `xml:"NewerNoncurrentVersions"`
	} `xml:"NoncurrentVersionExpiration"`
	Abort struct {
		Days int `xml:"DaysAfterInitiation"`
	} `xml:"AbortIncompleteMultipartUpload"`
}

// lifecycleRules parses a bucket's enabled rules (none when unset or invalid).
func lifecycleRules(b *Bucket) []lifecycleRule {
	var cfg struct {
		Rules []lifecycleRule `xml:"Rule"`
	}
	if b.Config["lifecycle"] == "" || xml.Unmarshal([]byte(b.Config["lifecycle"]), &cfg) != nil {
		return nil
	}
	return slices.DeleteFunc(cfg.Rules, func(r lifecycleRule) bool { return r.Status != "Enabled" })
}

// matches applies the rule's filter to an object.
func (r *lifecycleRule) matches(o *Object) bool {
	f := r.Filter
	prefix := r.Prefix + f.Prefix + f.And.Prefix
	tags := f.And.Tags
	if f.Tag != nil {
		tags = append(tags, *f.Tag)
	}
	size := int64(len(o.Data))
	if gt := max(f.Greater, f.And.Greater); gt > 0 && size <= gt {
		return false
	}
	if lt := max(f.Less, f.And.Less); lt > 0 && size >= lt {
		return false
	}
	for _, t := range tags {
		if v, ok := o.Tags[t.Key]; !ok || v != t.Value {
			return false
		}
	}
	return strings.HasPrefix(o.Key, prefix)
}

// expiryTime is when the rule expires the current version: its Date, or Days after
// creation rounded up to the next midnight UTC, as S3 does.
func (r *lifecycleRule) expiryTime(o *Object) (time.Time, bool) {
	if r.Expiration.Date != "" {
		t, err := time.Parse(time.RFC3339, r.Expiration.Date)
		return t, err == nil
	}
	if r.Expiration.Days > 0 {
		t := o.Modified.UTC().AddDate(0, 0, r.Expiration.Days)
		if d := t.Truncate(24 * time.Hour); !d.Equal(t) {
			t = d.Add(24 * time.Hour)
		}
		return t, true
	}
	return time.Time{}, false
}

// expirationHeader is the x-amz-expiration value of a current version ("" when no
// rule expires it).
func (b *Bucket) expirationHeader(o *Object) string {
	var best time.Time
	id := ""
	for _, r := range lifecycleRules(b) {
		if t, ok := r.expiryTime(o); ok && r.matches(o) && (best.IsZero() || t.Before(best)) {
			best, id = t, r.ID
		}
	}
	if best.IsZero() {
		return ""
	}
	return fmt.Sprintf("expiry-date=%q, rule-id=%q", best.UTC().Format(http.TimeFormat), id)
}

// Sweep applies every bucket's lifecycle rules as of now.
func (s *Service) Sweep(now time.Time) {
	var ev []pending
	s.mu.Lock()
	for _, b := range s.st.Buckets {
		if b.Config["lifecycle"] == "" {
			continue
		}
		rules := lifecycleRules(b)
		for _, key := range slices.Sorted(maps.Keys(b.Objects)) {
			changed := false
			for _, r := range rules {
				e, c := s.sweepKey(b, key, &r, now)
				ev, changed = append(ev, e...), changed || c
			}
			if changed {
				s.journal(b, key)
			}
		}
		for id, u := range b.Uploads {
			for _, r := range rules {
				if d := r.Abort.Days; d > 0 && r.matches(&Object{Key: u.Key}) && now.Sub(u.Initiated) >= time.Duration(d)*24*time.Hour {
					delete(b.Uploads, id)
					s.j.Delete("u/" + b.Name + "/" + id)
					break
				}
			}
		}
	}
	s.mu.Unlock()
	s.notify(core.NewTrace(), ev)
}

// sweepKey applies one rule to the versions of one key and says whether it changed
// any; the bucket lock is held.
func (s *Service) sweepKey(b *Bucket, key string, r *lifecycleRule, now time.Time) (ev []pending, changed bool) {
	expire := func(version string) {
		changed = true
		res := b.remove(key, version)
		if res.Event != "" {
			ev = append(ev, s.events(b, strings.Replace(res.Event, "ObjectRemoved", "LifecycleExpiration", 1), res.Obj)...)
		}
	}
	vs := b.Objects[key]
	if len(vs) == 0 {
		return
	}
	cur := vs[len(vs)-1]
	switch {
	case cur.DeleteMarker && len(vs) == 1 && r.Expiration.DeleteMarker && r.matches(cur):
		expire(cur.VersionID)
		return
	case !cur.DeleteMarker && r.matches(cur) && lockedBy(cur, now, false) == "":
		if t, ok := r.expiryTime(cur); ok && !now.Before(t) {
			expire("")
		}
	}
	if r.Noncurrent.Days > 0 {
		vs = b.Objects[key]
		newer := 0 // noncurrent versions newer than the one being looked at
		var old []string
		for i := len(vs) - 2; i >= 0; i-- {
			v := vs[i]
			if now.Sub(vs[i+1].Modified) >= time.Duration(r.Noncurrent.Days)*24*time.Hour && newer >= r.Noncurrent.Newer &&
				r.matches(v) && lockedBy(v, now, false) == "" {
				old = append(old, v.VersionID)
			}
			newer++
		}
		for _, id := range old {
			expire(id)
		}
	}
	return
}
