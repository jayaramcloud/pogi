// Package core is pogi's shared plumbing: the gateway that routes every request
// to a service, the wire-protocol codecs, the request log, chaos rules,
// persistence and the admin API.
package core

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Clouds.
const (
	AWS   = "aws"
	GCP   = "gcp"
	Azure = "azure"
)

// Defaults used when a request does not say otherwise.
const (
	DefaultAccount = "000000000000"
	DefaultRegion  = "us-east-1"

	// Azure resource IDs need a subscription and a resource group; pogi's resources live in these.
	AzureSubscription  = "00000000-0000-0000-0000-000000000000"
	AzureResourceGroup = "pogi"
)

// Info describes a service and how the gateway finds it.
type Info struct {
	Name     string                   // unique id, e.g. "s3", "sqs", "gcs"
	Cloud    string                   // AWS, GCP or Azure
	Title    string                   // human name
	Targets  []string                 // X-Amz-Target prefixes and smithy rpcv2 service names
	Signing  []string                 // SigV4 signing names
	Match    func(*http.Request) bool // routing on path or host, checked before Signing
	Protocol string                   // error format for gateway-made errors: json, query, rest-xml, rest-json
}

// Service is one emulated cloud service.
type Service interface {
	Info() Info
	ServeHTTP(http.ResponseWriter, *http.Request)
}

// Stateful services can be persisted, snapshotted and reset.
type Stateful interface {
	Save() ([]byte, error)
	Load([]byte) error
	Reset()
}

// Porter services also listen on their own port (Azure Storage on Azurite's ports).
type Porter interface {
	Port() int
}

// Browsable services list their resources for the console, TUI and CLI.
type Browsable interface {
	Resources() []Table
}

// Inspectable services return the full view of one resource (first column of a row).
type Inspectable interface {
	Inspect(table, id string) any
}

// Table is a generic resource listing; the first column identifies a row.
type Table struct {
	Name    string     `json:"name"`
	Columns []string   `json:"columns"`
	Rows    [][]string `json:"rows"`
	// Log marks a record of activity, such as CloudTrail events or log entries, not resources the user made.
	// Status does not count its rows, so a service is not "in use" just because other calls were logged.
	Log bool `json:"log,omitempty"`
	// Global marks a list that spans every region, like S3's bucket list. The console's region switcher
	// does not hide its rows (the account still does).
	Global bool `json:"global,omitempty"`
}

// ObjectInfo is one row of the console's object browser (S3, GCS, Blob).
type ObjectInfo struct {
	Key         string
	Size        int
	Modified    time.Time
	ContentType string
}

// Ctx carries what the gateway learned about a request.
type Ctx struct {
	Hub       *Hub
	Cloud     string
	Service   string
	Op        string
	Account   string
	Region    string
	AccessKey string
	Principal string
	Bearer    string // the Authorization bearer token (HTTP) or authorization metadata (gRPC), for the cloud's IAM authorizer
	Resource  string // the resource name a gRPC request names, for IAM (HTTP requests carry theirs in Path)
	RequestID string
	Host      string
	Path      string
	RawQuery  string          // the URL query as received (HTTP), for IAM
	Header    http.Header     // the request headers (HTTP), for IAM
	Denied    map[string]bool // S3 DeleteObjects: keys IAM refused; the handler answers each as AccessDenied
	ErrCode   string          // set by error writers, shown in the request log
	IAM       string          // the IAM decision when enforcement is on, shown in the request log
	Trace     string          // the flow this request belongs to, shown in the request log
	Chaos     bool            // the response was injected by a chaos rule
	Internal  bool            // an in-process call from another service (Hub.Call)
	ctx       context.Context // the request's context: it ends when the client leaves
	body      []byte
	read      bool
	req       *http.Request // the request as received, for signature checks
	bodyErr   *Error        // the body was refused (too large, stalled); Intercept answers it
}

type ctxKey struct{}

// C returns the request's Ctx. Outside the gateway it returns defaults.
func C(r *http.Request) *Ctx {
	if c, ok := r.Context().Value(ctxKey{}).(*Ctx); ok {
		return c
	}
	return &Ctx{Account: DefaultAccount, Region: DefaultRegion, RequestID: NewID(), Host: r.Host}
}

// Body returns the request body; the gateway buffers it once for everybody.
func Body(r *http.Request) []byte {
	c, ok := r.Context().Value(ctxKey{}).(*Ctx)
	if !ok {
		b, _ := io.ReadAll(r.Body)
		return b
	}
	if !c.read {
		c.body, _ = io.ReadAll(r.Body)
		c.read = true
	}
	return c.body
}

// BodyErr is the answer for a request body that was too large or stalled, or nil. A service that looks up
// the operation before Intercept must answer it first, so the caller sees 413 and not "not implemented".
func (c *Ctx) BodyErr() error {
	if c.bodyErr == nil {
		return nil // not c.bodyErr: that would be a non-nil error holding a nil pointer
	}
	return c.bodyErr
}

// Intercept checks the signature (Config.ValidateSigs), then runs chaos rules
// and IAM enforcement once the operation is known. A non-nil error must be
// returned to the caller in the service's protocol.
func (c *Ctx) Intercept() error {
	if c.bodyErr != nil {
		return c.bodyErr
	}
	if c.Hub == nil || c.Internal {
		return nil
	}
	if e := c.Hub.verify(c); e != nil {
		return e
	}
	if e := c.Hub.Chaos.Apply(c); e != nil {
		c.Chaos = true
		return e
	}
	if e := c.Hub.Check(c); e != nil {
		return e // a nil *Error must not become a non-nil error
	}
	return nil
}

// Check runs the request's cloud's IAM authorizer. It also runs in off mode, where it only
// records for the console's IAM page; only strict mode denies. Paths that bypass the HTTP
// gateway (gRPC, AMQP) call it themselves.
func (h *Hub) Check(c *Ctx) *Error {
	if f := h.Authorize[c.Cloud]; f != nil {
		if e := f(c); e != nil && h.IAMMode() == "strict" {
			return e
		}
	}
	return nil
}

// Payload is the request body, once the gateway or a service has read it.
func (c *Ctx) Payload() []byte { return c.body }

// ARN builds an AWS ARN for this request's account and region.
func (c *Ctx) ARN(service, resource string) string {
	return ARN(service, c.Region, c.Account, resource)
}

// GlobalARN builds a region-less ARN (IAM, STS) for this request's account,
// in the partition of its region.
func (c *Ctx) GlobalARN(service, resource string) string {
	return GlobalARN(service, c.Region, c.Account, resource)
}

// Error is an API error, rendered in the caller's wire protocol.
type Error struct {
	Status int
	Code   string
	Msg    string
}

func (e *Error) Error() string { return e.Code + ": " + e.Msg }

// Errorf builds an *Error.
func Errorf(status int, code, format string, a ...any) *Error {
	return &Error{status, code, fmt.Sprintf(format, a...)}
}

// AsError converts any error into an *Error; unknown errors become 500s.
func AsError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return &Error{http.StatusInternalServerError, "InternalFailure", err.Error()}
}

// NewID returns 32 random hex characters.
func NewID() string { return RandHex(16) }

// RandHex returns n random bytes as hex.
func RandHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// UUID returns a random v4 UUID.
func UUID() string {
	b := make([]byte, 16)
	rand.Read(b)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	h := hex.EncodeToString(b)
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// RandString returns n random characters from alphabet.
func RandString(n int, alphabet string) string {
	b := make([]byte, n)
	rand.Read(b)
	for i := range b {
		b[i] = alphabet[int(b[i])%len(alphabet)]
	}
	return string(b)
}

// Alphabets for RandString.
const (
	Upper    = "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
	UpperNum = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	AlphaNum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

// Partition is the AWS partition of a region: aws-cn for China, aws-us-gov
// for GovCloud, otherwise aws.
func Partition(region string) string {
	switch {
	case strings.HasPrefix(region, "cn-"):
		return "aws-cn"
	case strings.HasPrefix(region, "us-gov-"):
		return "aws-us-gov"
	}
	return "aws"
}

// URLSuffix is the domain of a region's endpoints (CloudFormation's AWS::URLSuffix).
func URLSuffix(region string) string {
	if strings.HasPrefix(region, "cn-") {
		return "amazonaws.com.cn"
	}
	return "amazonaws.com"
}

// ARN builds "arn:partition:service:region:account:resource"; the partition
// follows the region.
func ARN(service, region, account, resource string) string {
	return "arn:" + Partition(region) + ":" + service + ":" + region + ":" + account + ":" + resource
}

// GlobalARN builds the ARN of a resource without a region (IAM, STS, S3
// buckets, Route 53), in the partition of region:
// "arn:partition:service::account:resource".
func GlobalARN(service, region, account, resource string) string {
	return "arn:" + Partition(region) + ":" + service + "::" + account + ":" + resource
}

// ParseARN splits an ARN into its parts.
func ParseARN(arn string) (service, region, account, resource string, ok bool) {
	p := strings.SplitN(arn, ":", 6)
	if len(p) != 6 || p[0] != "arn" {
		return "", "", "", "", false
	}
	return p[2], p[3], p[4], p[5], true
}

// Unix renders t the way AWS JSON protocols do: epoch seconds with millis.
func Unix(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UnixMilli()) / 1000
}

// ISO is the timestamp layout used by XML protocols.
const ISO = "2006-01-02T15:04:05.000Z"

// Context is the request's context; it ends when the client leaves. Calls made in-process have none, so it is then Background.
func (c *Ctx) Context() context.Context {
	if c.ctx == nil {
		return context.Background()
	}
	return c.ctx
}
