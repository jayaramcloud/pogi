// Package s3io reads and writes objects in pogi's own S3 for the services that
// store files there (Transfer Family, DataSync, S3 Tables). It goes through the
// hub, as any service must, and never imports the s3 package.
package s3io

import (
	"encoding/xml"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// ErrNotFound is a missing bucket or object.
var ErrNotFound = errors.New("not found")

// Client is S3 as one account in one region.
type Client struct {
	Hub             *core.Hub
	Account, Region string
}

// Object is one listed key.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

func (c Client) do(method, path string, h http.Header, body []byte) (int, http.Header, []byte) {
	return c.Hub.Do(c.Account, c.Region, "s3", method, path, h, body)
}

func objPath(bucket, key string) string {
	return (&url.URL{Path: "/" + bucket + "/" + key}).EscapedPath()
}

// fail turns an S3 error response into an error.
func fail(op string, status int, body []byte) error {
	if status == http.StatusNotFound {
		return fmt.Errorf("%s: %w", op, ErrNotFound)
	}
	var e struct{ Code, Message string }
	xml.Unmarshal(body, &e)
	return fmt.Errorf("%s: %d %s %s", op, status, e.Code, e.Message)
}

// MakeBucket creates a bucket; one that exists already is not an error.
func (c Client) MakeBucket(bucket string) error {
	var body []byte
	if c.Region != "us-east-1" {
		body = []byte("<CreateBucketConfiguration><LocationConstraint>" + c.Region + "</LocationConstraint></CreateBucketConfiguration>")
	}
	if st, _, b := c.do(http.MethodPut, "/"+bucket, nil, body); st != http.StatusOK && !strings.Contains(string(b), "BucketAlreadyOwnedByYou") {
		return fail("create bucket "+bucket, st, b)
	}
	return nil
}

// DeleteBucket removes a bucket; a missing one is fine.
func (c Client) DeleteBucket(bucket string) {
	c.do(http.MethodDelete, "/"+bucket, nil, nil)
}

// BucketExists says whether the bucket is there.
func (c Client) BucketExists(bucket string) bool {
	st, _, _ := c.do(http.MethodHead, "/"+bucket, nil, nil)
	return st == http.StatusOK
}

// Put writes an object.
func (c Client) Put(bucket, key string, data []byte) error {
	if st, _, b := c.do(http.MethodPut, objPath(bucket, key), nil, data); st != http.StatusOK {
		return fail("put s3://"+bucket+"/"+key, st, b)
	}
	return nil
}

// Get reads an object, or n bytes of it from off (n < 0: to the end).
func (c Client) Get(bucket, key string, off, n int64) ([]byte, error) {
	var h http.Header
	if off > 0 || n >= 0 {
		rng := "bytes=" + strconv.FormatInt(off, 10) + "-"
		if n >= 0 {
			rng += strconv.FormatInt(off+n-1, 10)
		}
		if n == 0 {
			return nil, nil
		}
		h = http.Header{"Range": {rng}}
	}
	st, _, b := c.do(http.MethodGet, objPath(bucket, key), h, nil)
	switch {
	case st == http.StatusRequestedRangeNotSatisfiable:
		return nil, nil // past the end
	case st != http.StatusOK && st != http.StatusPartialContent:
		return nil, fail("get s3://"+bucket+"/"+key, st, b)
	}
	return b, nil
}

// Head returns the object's size and time.
func (c Client) Head(bucket, key string) (Object, error) {
	st, h, b := c.do(http.MethodHead, objPath(bucket, key), nil, nil)
	if st != http.StatusOK {
		return Object{}, fail("head s3://"+bucket+"/"+key, st, b)
	}
	size, _ := strconv.ParseInt(h.Get("Content-Length"), 10, 64)
	mod, _ := http.ParseTime(h.Get("Last-Modified"))
	return Object{Key: key, Size: size, Modified: mod}, nil
}

// Delete removes an object; a missing one is fine.
func (c Client) Delete(bucket, key string) error {
	if st, _, b := c.do(http.MethodDelete, objPath(bucket, key), nil, nil); st >= 300 && st != http.StatusNotFound {
		return fail("delete s3://"+bucket+"/"+key, st, b)
	}
	return nil
}

// List returns the objects under prefix. With a delimiter it stops at the first one
// after the prefix and returns the common prefixes (the "directories") too.
func (c Client) List(bucket, prefix, delimiter string) (objs []Object, prefixes []string, err error) {
	for token := ""; ; {
		q := url.Values{"list-type": {"2"}, "prefix": {prefix}}
		if delimiter != "" {
			q.Set("delimiter", delimiter)
		}
		if token != "" {
			q.Set("continuation-token", token)
		}
		st, _, body := c.do(http.MethodGet, "/"+bucket+"?"+q.Encode(), nil, nil)
		if st != http.StatusOK {
			return nil, nil, fail("list s3://"+bucket+"/"+prefix, st, body)
		}
		var out struct {
			Contents []struct {
				Key          string
				Size         int64
				LastModified time.Time
			}
			CommonPrefixes        []struct{ Prefix string }
			IsTruncated           bool
			NextContinuationToken string
		}
		if err := xml.Unmarshal(body, &out); err != nil {
			return nil, nil, err
		}
		for _, o := range out.Contents {
			objs = append(objs, Object{o.Key, o.Size, o.LastModified})
		}
		for _, p := range out.CommonPrefixes {
			prefixes = append(prefixes, p.Prefix)
		}
		if !out.IsTruncated {
			return objs, prefixes, nil
		}
		token = out.NextContinuationToken
	}
}

// Split splits s3://bucket/key into its parts; the key may be empty.
func Split(loc string) (bucket, key string, ok bool) {
	rest, ok := strings.CutPrefix(loc, "s3://")
	if !ok {
		return "", "", false
	}
	bucket, key, _ = strings.Cut(rest, "/")
	return bucket, key, bucket != ""
}
