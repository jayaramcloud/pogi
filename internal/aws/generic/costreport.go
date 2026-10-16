package generic

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// The CSV reports of the Cost and Usage Report and of BCM Data Exports: the
// cost model's bill (costmodel.go) for the billing month so far, one row per
// hour, usage type and operation. The columns are a small subset of the real
// ones; Parquet and ZIP output are written as CSV (ponytail: one format).
var costReportHeader = []string{"identity/LineItemId", "bill/BillingPeriodStartDate", "lineItem/UsageAccountId", "lineItem/LineItemType", "lineItem/UsageStartDate",
	"lineItem/UsageEndDate", "lineItem/ProductCode", "lineItem/UsageType", "lineItem/Operation", "lineItem/UsageAmount", "lineItem/UnblendedCost",
	"lineItem/CurrencyCode", "product/region"}

// reportCSV renders the month-to-date bill of an account, as gzip when asked.
func costReportCSV(h *core.Hub, account string, now time.Time, gz bool) []byte {
	month := costMonthStart(now)
	var buf bytes.Buffer
	var w interface {
		Write([]byte) (int, error)
	} = &buf
	var zw *gzip.Writer
	if gz {
		zw = gzip.NewWriter(&buf)
		w = zw
	}
	cw := csv.NewWriter(w)
	cw.Write(costReportHeader)
	for _, l := range costLines(h, account, month, now.Add(time.Hour)) {
		id := sha256.Sum256([]byte(strings.Join([]string{l.At.String(), l.Code, l.Region, l.UsageType, l.Operation, l.Instance}, "|")))
		cw.Write([]string{hex.EncodeToString(id[:12]), month.Format(time.RFC3339), account, "Usage", l.At.Format(time.RFC3339), l.At.Add(time.Hour).Format(time.RFC3339),
			l.Code, l.UsageType, l.Operation, strconv.FormatFloat(l.Qty, 'f', -1, 64), costMoney(l.Cost), "USD", l.Region})
	}
	cw.Flush()
	if zw != nil {
		zw.Close()
	}
	return buf.Bytes()
}

// putObject writes an object into a bucket through the hub; the status is S3's.
func costPutObject(h *core.Hub, account, region, bucket, key string, body []byte) int {
	status, _, _ := h.Do(account, region, "s3", http.MethodPut, (&url.URL{Path: "/" + bucket + "/" + key}).EscapedPath(), nil, body)
	return status
}

// bucketExists asks S3 (when it is enabled) whether the bucket is there.
func costBucketExists(h *core.Hub, account, region, bucket string) bool {
	if h.Service("s3") == nil {
		return true
	}
	status, _, _ := h.Do(account, region, "s3", http.MethodHead, "/"+bucket, nil, nil)
	return status < 300
}

// allOf returns copies of the resources of a type in every account and region.
func (s *Service) costAllOf(typ string) []*Resource {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Resource
	for _, k := range core.SortedKeys(s.st.Res) {
		if r := s.st.Res[k]; r.Type == typ {
			out = append(out, r.clone())
		}
	}
	return out
}

// mutate applies f to the stored resource r was copied from, unless it is gone.
func (s *Service) costMutate(r *Resource, f func(*Resource)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cur := s.st.Res[r.skey()]; cur != nil {
		f(cur)
	}
}

// billingPeriod is the folder name of the billing month of t (20261001-20261101).
func costBillingPeriod(t time.Time) string {
	m := costMonthStart(t)
	return m.Format("20060102") + "-" + m.AddDate(0, 1, 0).Format("20060102")
}
