package web_test

import (
	"net/http"
	"testing"
)

func TestBellShowsLoudErrorsOnly(t *testing.T) {
	c := start(t, "s3")
	for _, p := range []string{"/nobucket/missing.txt", "/nobucket?acl"} { // a read of nothing is a quiet 404
		if res, err := c.c.Get(c.base + p); err == nil {
			res.Body.Close()
		}
	}
	req, _ := http.NewRequest("PUT", c.base+"/Bad_Bucket", nil) // an invalid name is a loud 400
	if res, err := c.c.Do(req); err == nil {
		res.Body.Close()
	}
	bell := c.get("/_pogi/notes")
	want(t, bell, `aria-pressed="true"`, `<button type="button" class="chip`, "See all errors in Activity", "/_pogi/requests?errors=1", "S3 CreateBucket failed")
	wantNot(t, bell, "GetObject failed", "<span class=\"chip")
}
