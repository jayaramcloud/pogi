package s3

import (
	"encoding/xml"
	"net/http"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Object Lock. The lock of an object version lives in its stored headers (the same
// ones a PutObject sets), so retention and legal hold survive every storage mode.
const (
	lockMode  = "X-Amz-Object-Lock-Mode"
	lockUntil = "X-Amz-Object-Lock-Retain-Until-Date"
	lockHold  = "X-Amz-Object-Lock-Legal-Hold"
)

func errLocked() error {
	return core.Errorf(403, "AccessDenied", "Access Denied because object protected by object lock.")
}

func errNoLockBucket() error {
	return core.Errorf(400, "InvalidRequest", "Bucket is missing Object Lock Configuration")
}

func parseLockTime(s string) (time.Time, bool) {
	t, err := time.Parse(time.RFC3339, s)
	return t, err == nil
}

// retention returns the mode and end of an object's retention (zero when it has none).
func retention(o *Object) (mode string, until time.Time) {
	until, _ = parseLockTime(o.Headers[lockUntil])
	return o.Headers[lockMode], until
}

// lockedBy tells why a version cannot be deleted now ("" when it can): a legal hold,
// COMPLIANCE retention, or GOVERNANCE retention without the bypass header.
func lockedBy(o *Object, now time.Time, bypass bool) string {
	if o == nil || o.DeleteMarker {
		return ""
	}
	if strings.EqualFold(o.Headers[lockHold], "ON") {
		return "legal hold"
	}
	mode, until := retention(o)
	switch {
	case !until.After(now):
		return ""
	case mode == "COMPLIANCE":
		return "compliance retention"
	case mode == "GOVERNANCE" && !bypass:
		return "governance retention"
	}
	return ""
}

func bypassGovernance(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("X-Amz-Bypass-Governance-Retention"), "true")
}

func (b *Bucket) lockEnabled() bool {
	return strings.Contains(b.Config["object-lock"], "<ObjectLockEnabled>Enabled</ObjectLockEnabled>")
}

// applyDefaultRetention gives a new version the bucket's default retention when the
// request set none.
func (b *Bucket) applyDefaultRetention(o *Object) {
	if o.DeleteMarker || o.Headers[lockMode] != "" || !strings.Contains(b.Config["object-lock"], "<DefaultRetention>") {
		return
	}
	var cfg struct {
		Mode  string `xml:"Rule>DefaultRetention>Mode"`
		Days  int    `xml:"Rule>DefaultRetention>Days"`
		Years int    `xml:"Rule>DefaultRetention>Years"`
	}
	if xml.Unmarshal([]byte(b.Config["object-lock"]), &cfg) != nil || cfg.Mode == "" {
		return
	}
	if o.Headers == nil {
		o.Headers = map[string]string{}
	}
	o.Headers[lockMode] = cfg.Mode
	o.Headers[lockUntil] = time.Now().UTC().AddDate(cfg.Years, 0, cfg.Days).Format(core.ISO)
}

// checkLockHeaders refuses lock headers on a bucket without Object Lock, and half a
// retention (a mode needs a date and the other way round), as S3 does.
func (b *Bucket) checkLockHeaders(h http.Header) error {
	mode, until, hold := h.Get(lockMode), h.Get(lockUntil), h.Get(lockHold)
	if mode == "" && until == "" && hold == "" {
		return nil
	}
	if !b.lockEnabled() {
		return errNoLockBucket()
	}
	if (mode == "") != (until == "") {
		return core.Errorf(400, "InvalidArgument", "x-amz-object-lock-retain-until-date and x-amz-object-lock-mode must both be supplied")
	}
	if _, ok := parseLockTime(until); until != "" && !ok {
		return core.Errorf(400, "InvalidArgument", "The retain until date must be provided in ISO 8601 format")
	}
	return nil
}

// lockTarget finds the version a lock request names (the latest when none) in a bucket with Object Lock.
func lockTarget(w http.ResponseWriter, r *http.Request, b *Bucket, key string) (*Object, error) {
	if !b.lockEnabled() {
		return nil, errNoLockBucket()
	}
	return lookup(w, b, key, r.URL.Query().Get("versionId"))
}

func errNoLock() error {
	return core.Errorf(404, "NoSuchObjectLockConfiguration", "The specified object does not have a ObjectLock configuration")
}

func (s *Service) putObjectRetention(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	body, _, err := readBody(r)
	if err != nil {
		return err
	}
	var in struct {
		Mode  string `xml:"Mode"`
		Until string `xml:"RetainUntilDate"`
	}
	bad := core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
	if xml.Unmarshal(body, &in) != nil || in.Mode != "" && in.Mode != "GOVERNANCE" && in.Mode != "COMPLIANCE" {
		return bad
	}
	until, ok := parseLockTime(in.Until)
	if (in.Mode == "") != (in.Until == "") || in.Until != "" && !ok {
		return bad
	}
	if in.Mode != "" && !until.After(time.Now()) {
		return core.Errorf(400, "InvalidArgument", "The retain until date must be in the future!")
	}
	return s.withBucket(bucket, true, func(b *Bucket) error {
		o, err := lockTarget(w, r, b, key)
		if err != nil {
			return err
		}
		// Changing a retention that is still running: COMPLIANCE can only grow; GOVERNANCE
		// can grow or become COMPLIANCE, anything else needs the bypass header.
		if mode, cur := retention(o); cur.After(time.Now()) {
			grows := in.Mode == mode && !until.Before(cur) || mode == "GOVERNANCE" && in.Mode == "COMPLIANCE"
			if !grows && !(mode == "GOVERNANCE" && bypassGovernance(r)) {
				return errLocked()
			}
		}
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		delete(o.Headers, lockMode)
		delete(o.Headers, lockUntil)
		if in.Mode != "" {
			o.Headers[lockMode], o.Headers[lockUntil] = in.Mode, until.UTC().Format(core.ISO)
		}
		s.journal(b, key)
		w.WriteHeader(http.StatusOK)
		return nil
	})
}

func (s *Service) getObjectRetention(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	return s.withBucket(bucket, false, func(b *Bucket) error {
		o, err := lockTarget(w, r, b, key)
		if err != nil {
			return err
		}
		mode, until := retention(o)
		if mode == "" {
			return errNoLock()
		}
		return writeXML(w, 200, struct {
			XMLName xml.Name `xml:"Retention"`
			Xmlns   string   `xml:"xmlns,attr"`
			Mode    string
			Until   string `xml:"RetainUntilDate"`
		}{Xmlns: xmlns, Mode: mode, Until: until.UTC().Format(core.ISO)})
	})
}

func (s *Service) putObjectLegalHold(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	body, _, err := readBody(r)
	if err != nil {
		return err
	}
	var in struct {
		Status string `xml:"Status"`
	}
	if xml.Unmarshal(body, &in) != nil || in.Status != "ON" && in.Status != "OFF" {
		return core.Errorf(400, "MalformedXML", "The XML you provided was not well-formed or did not validate against our published schema")
	}
	return s.withBucket(bucket, true, func(b *Bucket) error {
		o, err := lockTarget(w, r, b, key)
		if err != nil {
			return err
		}
		if o.Headers == nil {
			o.Headers = map[string]string{}
		}
		o.Headers[lockHold] = in.Status
		s.journal(b, key)
		w.WriteHeader(http.StatusOK)
		return nil
	})
}

func (s *Service) getObjectLegalHold(w http.ResponseWriter, r *http.Request, c *core.Ctx, bucket, key string) error {
	return s.withBucket(bucket, false, func(b *Bucket) error {
		o, err := lockTarget(w, r, b, key)
		if err != nil {
			return err
		}
		if o.Headers[lockHold] == "" {
			return errNoLock()
		}
		return writeXML(w, 200, struct {
			XMLName xml.Name `xml:"LegalHold"`
			Xmlns   string   `xml:"xmlns,attr"`
			Status  string
		}{Xmlns: xmlns, Status: o.Headers[lockHold]})
	})
}
