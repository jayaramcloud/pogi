package core

import (
	"encoding/json"
	"net"
	"net/http"
	"regexp"
	"strings"
)

// googleHosts names the googleapis.com host of each Google service that routes
// by path (service name -> the label in front of ".googleapis.com"). A request
// for such a path on another *.googleapis.com host is not that API's. Hosts
// that are localhost, an IP, pogi's own names or not under googleapis.com are
// never checked. A service missing here is not checked either.
var googleHosts = map[string][]string{
	"artifactregistry": {"artifactregistry"}, "bigquery": {"bigquery"}, "cloudfunctions": {"cloudfunctions"},
	"cloudkms": {"cloudkms"}, "cloudscheduler": {"cloudscheduler"}, "cloudtasks": {"cloudtasks"},
	"compute": {"compute"}, "container": {"container"}, "datastore": {"datastore"}, "eventarc": {"eventarc"},
	"firebaseauth": {"identitytoolkit", "securetoken"}, "gcpiam": {"iam"}, "gcs": {"storage"},
	"iamcredentials": {"iamcredentials"}, "logging": {"logging"}, "managedkafka": {"managedkafka"},
	"monitoring": {"monitoring"}, "pubsub": {"pubsub"}, "resourcemanager": {"cloudresourcemanager"},
	"run": {"run"}, "secretmanager": {"secretmanager"}, "serviceusage": {"serviceusage"}, "sqladmin": {"sqladmin"},
	"redis":     {"redis"},
	"workflows": {"workflows", "workflowexecutions"},
	"spanner":   {"spanner"}, "bigtable": {"bigtableadmin"},
	"gcplb": {"compute"}, "cloudarmor": {"compute"},
}

// googlePrefixed are the Google APIs (host labels) that pogi also serves under /<label>/ on any host.
// Two APIs can share their paths (Memorystore and Filestore both have
// /v1/projects/p/locations/l/instances), and on localhost only the prefix tells them apart. pogi tf
// writes it into the google provider's custom endpoints; an SDK takes it as part of its endpoint.
var googlePrefixed = map[string]bool{
	"bigtableadmin": true, "redis": true, "spanner": true, "workflowexecutions": true, "workflows": true,
}

// googlePrefix turns a request for /<label>/v1/... into one for /v1/... on <label>.googleapis.com,
// for the APIs in googlePrefixed, so that the services' path matchers and googleHostOK pick the right
// one. The client's own host stays in Ctx.Host.
func googlePrefix(r *http.Request) {
	label, rest, ok := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	if !ok || !googlePrefixed[label] || !APIVersionPath("/"+rest) {
		return
	}
	r.URL.Path = "/" + rest
	if r.URL.RawPath != "" {
		r.URL.RawPath = strings.TrimPrefix(r.URL.RawPath, "/"+label)
	}
	r.Host = label + ".googleapis.com"
}

// googleHostOK says whether host may carry the Google service's API: the
// API's own host, a regional one ("us-central1-run.googleapis.com"), a
// bucket host ("b.storage.googleapis.com"), the mTLS form, or the shared
// www.googleapis.com.
func googleHostOK(service, host string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	host, google := strings.CutSuffix(host, ".googleapis.com")
	apis := googleHosts[service]
	if !google || len(apis) == 0 || host == "www" {
		return true
	}
	host = strings.Replace(host, ".mtls", "", 1)
	for _, api := range apis {
		if host == api || strings.HasSuffix(host, "."+api) || strings.HasSuffix(host, "-"+api) {
			return true
		}
	}
	return false
}

// apiVersion matches the first path segment of a Google API: v1, v2beta1, v3alpha, v1b3 (Dataflow).
var apiVersion = regexp.MustCompile(`^v[0-9]+((alpha|beta)[0-9]*|b[0-9]+)?$`)

// APIVersionPath says whether a request path starts with a Google API version
// ("/v1/projects/..."). No S3 bucket has such a name: two characters are too short.
func APIVersionPath(path string) bool {
	seg, _, _ := strings.Cut(strings.TrimPrefix(path, "/"), "/")
	return apiVersion.MatchString(seg)
}

func hostSuffix(r *http.Request, suffix ...string) bool {
	host := r.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	for _, s := range suffix {
		if strings.HasSuffix(host, s) {
			return true
		}
	}
	return false
}

// looksGoogle says whether an unclaimed request is aimed at a Google API.
func looksGoogle(r *http.Request) bool {
	if hostSuffix(r, ".googleapis.com") || APIVersionPath(r.URL.Path) {
		return true
	}
	for k := range r.Header {
		if strings.HasPrefix(strings.ToLower(k), "x-goog-") {
			return true
		}
	}
	ua := strings.ToLower(r.UserAgent())
	return strings.Contains(ua, "google") || strings.Contains(ua, "gcloud")
}

var (
	graphVersion = regexp.MustCompile(`^v[0-9]+\.[0-9]+$`)
	guid         = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
)

// azureAuth says whether the Authorization header has a scheme that is neither
// AWS's nor Bearer: Basic, SharedKey, SharedAccessSignature, garbage.
func azureAuth(r *http.Request) bool {
	a := r.Header.Get("Authorization")
	return a != "" && !strings.HasPrefix(a, "AWS") && !strings.HasPrefix(strings.ToLower(a), "bearer")
}

// azureKind says what an unclaimed request is aimed at: "graph" (a /v1.0 path), "entra" (a tenant id first, or an Entra host), "arm" (any other
// Azure request) or "" (no sign of Azure). Azure signs of its own: an
// api-version query, a /_azure/ path, a login host, an Authorization scheme other
// than Bearer (a Bearer request may be for S3-compatible or Google clients).
// The caller has ruled out AWS credentials and Google signs.
func azureKind(r *http.Request) string {
	first, _, _ := strings.Cut(strings.TrimPrefix(r.URL.Path, "/"), "/")
	switch {
	case graphVersion.MatchString(first):
		return "graph"
	case guid.MatchString(first) || hostSuffix(r, "login.microsoftonline.com", "login.windows.net", "sts.windows.net"):
		return "entra"
	case first == "_azure" || r.URL.Query().Has("api-version") || azureAuth(r) || hostSuffix(r, ".azure.net", ".windows.net", ".azure.com", ".microsoft.com"):
		return "arm"
	}
	return ""
}

// looksAWS says whether an unclaimed request is aimed at an AWS service that
// is not S3: an unknown target or signing name, or an amazonaws.com host
// without an s3 label. A request S3 would serve (anonymous, presigned,
// signed for "s3") has none of these.
func looksAWS(r *http.Request, signing string) bool {
	if r.Header.Get("X-Amz-Target") != "" || strings.HasPrefix(r.Header.Get("Smithy-Protocol"), "rpc-v2") {
		return true
	}
	if signing != "" {
		return !strings.HasPrefix(signing, "s3")
	}
	if !hostSuffix(r, ".amazonaws.com", ".amazonaws.com.cn") {
		return false
	}
	for _, label := range strings.Split(strings.ToLower(r.Host), ".") {
		if strings.HasPrefix(label, "s3") {
			return false
		}
	}
	return true
}

// unknownAPI answers a request that no service claims, in the error shape of
// the cloud it was aimed at, so that no cloud's SDK is handed another's (S3's)
// XML. It is not a registered service.
type unknownAPI struct {
	cloud    string
	notFound bool   // Google: the host does not serve this path (404), instead of not implemented (501)
	kind     string // Azure: what azureKind found
}

func (u unknownAPI) Info() Info {
	return Info{Name: "unknown", Cloud: u.cloud, Title: "Unknown API"}
}

func (u unknownAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c := C(r)
	if u.cloud == GCP {
		if u.notFound {
			WriteGoogleError(w, c, http.StatusNotFound, "NOT_FOUND", "pogi does not serve "+r.URL.Path+" on "+r.Host)
			return
		}
		WriteGoogleError(w, c, http.StatusNotImplemented, "UNIMPLEMENTED", "pogi does not implement "+r.Method+" "+r.URL.Path)
		return
	}
	if u.cloud == Azure {
		c.ErrCode = "NotFound"
		w.Header().Set("Content-Type", "application/json")
		msg := "pogi does not implement " + r.Method + " " + r.URL.Path
		body := map[string]any{"error": map[string]any{"code": "NotFound", "message": msg}}
		code := http.StatusNotFound
		switch u.kind {
		case "graph":
			c.ErrCode, code = "Request_BadRequest", http.StatusBadRequest
			body = map[string]any{"error": map[string]any{"code": c.ErrCode, "message": msg}}
		case "entra":
			c.ErrCode, code = "invalid_request", http.StatusBadRequest
			body = map[string]any{"error": c.ErrCode, "error_description": msg}
		}
		w.WriteHeader(code)
		json.NewEncoder(w).Encode(body)
		return
	}
	_, _, signing := sigv4(r)
	name, _, _ := strings.Cut(r.Header.Get("X-Amz-Target"), ".")
	if name == "" {
		name, _, _ = strings.Cut(r.Host, ".")
	}
	if signing != "" {
		name = signing
	}
	msg := "pogi does not implement the AWS service " + name
	ct := r.Header.Get("Content-Type")
	switch {
	case r.Header.Get("X-Amz-Target") != "" || strings.HasPrefix(r.Header.Get("Smithy-Protocol"), "rpc-v2"):
		_, version, _ := strings.Cut(ct, "application/x-amz-json-")
		WriteJSONError(w, c, version, Errorf(http.StatusBadRequest, "UnknownOperationException", "%s", msg))
	case strings.HasPrefix(ct, "application/x-www-form-urlencoded") || r.URL.Query().Has("Action"):
		WriteQueryError(w, c, "", http.StatusBadRequest, "InvalidAction", msg)
	default:
		RESTJSONError(w, c, Errorf(http.StatusNotFound, "UnknownOperationException", "%s", msg))
	}
}

// RegisterGoogleAPI adds a Google API that a generic engine serves (internal/gcp/generic): the
// service's host label goes into googleHosts, and /<label>/v1/... reaches it on any host, like the
// APIs in googlePrefixed. Call it from an init function, before the server starts.
func RegisterGoogleAPI(service, label string) {
	googleHosts[service] = []string{label}
	googlePrefixed[label] = true
}
