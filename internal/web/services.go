package web

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/events"
	"github.com/nrndev/pogi/internal/aws/lambda"
	"github.com/nrndev/pogi/internal/aws/s3"
	"github.com/nrndev/pogi/internal/aws/ses"
	"github.com/nrndev/pogi/internal/aws/sns"
	"github.com/nrndev/pogi/internal/aws/sqs"
	"github.com/nrndev/pogi/internal/azure/email"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/gcp/firebaseauth"
)

// Service pages beyond the generic resource tables.
func (ui *UI) serviceRoutes(m *http.ServeMux) {
	layout := shell()
	for _, p := range []string{"catalog", "tag"} {
		ui.pages[p] = template.Must(template.Must(layout.Clone()).ParseFS(files, "templates/"+p+".html"))
	}
	m.HandleFunc("GET /_pogi/services", ui.catalog)
	m.HandleFunc("GET /_pogi/bulk/{service}/{table}", ui.bulkTag)
	m.HandleFunc("POST /_pogi/cli/{service}/{op}", ui.cliMirror)
	m.HandleFunc("GET /_pogi/objects/{svc}/{bucket...}", ui.objects)
	m.HandleFunc("POST /_pogi/objects/{svc}/{bucket...}", ui.objectAction)
	m.HandleFunc("POST /_pogi/objects/{svc}", ui.makeBucket)
	m.HandleFunc("GET /_pogi/sqs/{account}/{region}/{name}", ui.sqsQueue)
	m.HandleFunc("POST /_pogi/sqs", ui.sqsCreate)
	m.HandleFunc("POST /_pogi/sqs/{account}/{region}/{name}/send", ui.sqsSend)
	m.HandleFunc("POST /_pogi/sqs/{account}/{region}/{name}/cli", ui.sqsCLI)
	m.HandleFunc("POST /_pogi/sqs/{account}/{region}/{name}/purge", ui.sqsPurge)
	m.HandleFunc("GET /_pogi/lambda/{account}/{region}/{name}", ui.lambdaPage)
	m.HandleFunc("POST /_pogi/lambda/{account}/{region}/{name}/invoke", ui.lambdaInvoke)
	m.HandleFunc("POST /_pogi/sns/publish", ui.snsPublish)
	m.HandleFunc("POST /_pogi/events/put", ui.eventsPut)
	m.HandleFunc("GET /_pogi/inbox", ui.inbox)
	m.HandleFunc("POST /_pogi/inbox/clear", ui.inboxClear)
}

// extra lists choices a service section needs for its action forms.
func extra(svc string, tables []core.Table) []string {
	var out []string
	for _, t := range tables {
		for _, r := range t.Rows {
			switch {
			case svc == "sns" && t.Name == "Topics" && len(r) > 2:
				out = append(out, core.ARN("sns", r[1], r[2], r[0]))
			case svc == "events" && t.Name == "Buses":
				out = append(out, r[0])
			}
		}
	}
	return out
}

// stay ends a post made inside the drawer: a toast, and the drawer shows to again. A post from
// anywhere else ends as done does, and an error is a toast in both.
func stay(w http.ResponseWriter, r *http.Request, to, msg string, err error) {
	if !inDrawer(r) || err != nil {
		done(w, r, to, msg, err)
		return
	}
	trigger(w, "pogi-toast", map[string]string{"msg": msg})
	loc, _ := json.Marshal(map[string]string{"path": to, "target": "#drawer", "swap": "innerHTML"})
	w.Header().Set("HX-Location", string(loc))
}

// ---- object storage (S3, Cloud Storage, Blob Storage) ----

// objectStore is what the browser needs; every storage service provides it.
type objectStore interface {
	Browse(bucket, prefix string) ([]string, []core.ObjectInfo, error)
	Download(bucket, key string) ([]byte, string, error)
	Upload(bucket, key, contentType string, data []byte) error
	Remove(bucket, key string) error
	MakeBucket(name string) error
	DropBucket(name string) error
}

// s3Store adapts S3's console API (accounts and regions are the defaults).
type s3Store struct {
	s      *s3.Service
	region string
}

func (a s3Store) Browse(b, p string) ([]string, []core.ObjectInfo, error) { return a.s.Browse(b, p) }
func (a s3Store) Download(b, k string) ([]byte, string, error) {
	o, err := a.s.Get(b, k)
	return o.Data, o.ContentType, err
}
func (a s3Store) Upload(b, k, ct string, d []byte) error { return a.s.Put(b, k, ct, d) }
func (a s3Store) Remove(b, k string) error               { return a.s.Delete(b, k) }
func (a s3Store) MakeBucket(n string) error {
	return a.s.CreateBucket(core.DefaultAccount, a.region, n)
}
func (a s3Store) DropBucket(n string) error { return a.s.DeleteBucket(n) }

// write runs a write the console makes straight on a service and logs it in Activity, marked "console".
// Writes that go through the hub are logged there; the rest come through here.
func (ui *UI) write(svc, op, target string, f func() error) error {
	start := time.Now()
	err := f()
	ui.hub.LogConsole(svc, op, target, time.Since(start), err)
	return err
}

// loggedStore logs the writes made on an object store (buckets, containers, objects).
type loggedStore struct {
	objectStore
	ui  *UI
	svc string
}

func (l loggedStore) Upload(b, k, ct string, d []byte) error {
	return l.ui.write(l.svc, "PutObject", b+"/"+k, func() error { return l.objectStore.Upload(b, k, ct, d) })
}
func (l loggedStore) Remove(b, k string) error {
	return l.ui.write(l.svc, "DeleteObject", b+"/"+k, func() error { return l.objectStore.Remove(b, k) })
}
func (l loggedStore) MakeBucket(n string) error {
	return l.ui.write(l.svc, "CreateBucket", n, func() error { return l.objectStore.MakeBucket(n) })
}
func (l loggedStore) DropBucket(n string) error {
	return l.ui.write(l.svc, "DeleteBucket", n, func() error { return l.objectStore.DropBucket(n) })
}

func (ui *UI) store(svc string) objectStore {
	switch s := ui.hub.Service(svc).(type) {
	case *s3.Service:
		return loggedStore{s3Store{s, ui.hub.Cfg.Region}, ui, svc}
	case objectStore:
		return loggedStore{s, ui, svc}
	}
	return nil
}

var storeTitles = map[string]string{"s3": "S3", "gcs": "Cloud Storage", "azblob": "Blob Storage"}
var storeNouns = map[string]string{"s3": "bucket", "gcs": "bucket", "azblob": "container"}

func objectsURL(svc, bucket, prefix string) string {
	u := "/_pogi/objects/" + svc + "/" + bucket
	if prefix != "" {
		u += "?prefix=" + url.QueryEscape(prefix)
	}
	return u
}

func (ui *UI) objects(w http.ResponseWriter, r *http.Request) {
	svc, bucket, q := r.PathValue("svc"), r.PathValue("bucket"), r.URL.Query()
	st := ui.store(svc)
	if st == nil {
		http.NotFound(w, r)
		return
	}
	if key := q.Get("download"); key != "" {
		data, ct, err := st.Download(bucket, key)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", ct)
		if q.Get("inline") != "" {
			// stored HTML runs in an opaque origin: it cannot reach the console's cookies, storage or API
			w.Header().Set("Content-Security-Policy", "sandbox")
			w.Header().Del("Cross-Origin-Embedder-Policy") // the page may load images from other sites
		} else {
			w.Header().Set("Content-Disposition", `attachment; filename="`+strings.ReplaceAll(key[strings.LastIndex(key, "/")+1:], `"`, "")+`"`)
		}
		w.Write(data)
		return
	}
	dirs, objs, err := st.Browse(bucket, q.Get("prefix"))
	if err != nil {
		back(w, r, "/_pogi/resources/"+svc, err)
		return
	}
	objs = slices.DeleteFunc(objs, func(o core.ObjectInfo) bool { return o.Key == q.Get("prefix") }) // a folder's own marker
	ui.renderX(w, r, "objects", bucket, svc, storeTitles[svc]+" "+storeNouns[svc], map[string]any{"Svc": svc, "Title": storeTitles[svc], "Bucket": bucket,
		"Prefix": q.Get("prefix"), "Dirs": dirs, "Objects": objs})
}

func (ui *UI) objectAction(w http.ResponseWriter, r *http.Request) {
	svc, bucket := r.PathValue("svc"), r.PathValue("bucket")
	st := ui.store(svc)
	if st == nil {
		http.NotFound(w, r)
		return
	}
	// Parse first, under the upload limit: FormValue would swallow a "too large" error.
	if err := ui.parseUpload(w, r, false); err != nil {
		back(w, r, objectsURL(svc, bucket, ""), err)
		return
	}
	prefix := r.FormValue("prefix")
	to, msg := objectsURL(svc, bucket, prefix), "Done"
	var err error
	switch r.FormValue("action") {
	case "upload":
		for _, fh := range r.MultipartForm.File["file"] {
			if fh.Size > ui.hub.MaxUpload() {
				err = errors.New(ui.tooBig(bucket+"/"+prefix+fh.Filename, fh.Size, ui.hub.MaxUpload(), false))
				break
			}
			f, ferr := fh.Open()
			if ferr != nil {
				err = ferr
				break
			}
			data, _ := io.ReadAll(f)
			f.Close()
			if err = st.Upload(bucket, prefix+fh.Filename, fh.Header.Get("Content-Type"), data); err != nil {
				break
			}
		}
		msg = "Uploaded"
	case "mkdir":
		name := strings.Trim(strings.TrimSpace(r.FormValue("folder")), "/")
		if name == "" {
			err = fmt.Errorf("give the folder a name")
			break
		}
		err, msg = st.Upload(bucket, prefix+name+"/", "application/x-directory", nil), "Folder "+name+" created"
	case "delete":
		err, msg = st.Remove(bucket, r.FormValue("key")), r.FormValue("key")+" deleted"
	case "drop":
		err, to, msg = st.DropBucket(bucket), "/_pogi/resources/"+svc, "Bucket "+bucket+" deleted"
		if err == nil {
			done(w, r, to, msg, nil) // the drawer's subject is gone: close it
			return
		}
	default:
		err = fmt.Errorf("unknown action %q", r.FormValue("action"))
	}
	stay(w, r, to, msg, err)
}

// parseUpload parses a form post that may carry files. The body stops at the upload limit (plus a
// little for the form's own framing); a post over it fails with the caution message. Function
// code (code is true) has the code limit instead: its zip may be up to 50 MB.
func (ui *UI) parseUpload(w http.ResponseWriter, r *http.Request, code bool) error {
	limit := ui.hub.MaxUpload()
	if code {
		limit = ui.hub.CodeLimit(false)
	}
	err := r.ParseMultipartForm(64 << 20) // also parses a plain form; the body is already limited (bodyLimit)
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		size := max(r.ContentLength, tooBig.Limit+1)
		if raw, ok := r.Context().Value(rawBodyKey{}).(io.Reader); ok {
			n, _ := io.Copy(io.Discard, raw) // the rest of the body, so the size shown is the real one
			size = max(size, tooBig.Limit+1+n)
		}
		return errors.New(ui.tooBig(r.URL.Path, size, limit, code))
	}
	if errors.Is(err, http.ErrNotMultipart) {
		return nil
	}
	return err
}

// tooBig is the caution for a console upload: a zip of function code, or a plain upload.
func (ui *UI) tooBig(resource string, size, limit int64, code bool) string {
	if code {
		return ui.hub.TooBigCode("console", resource, "Lambda code", size, limit)
	}
	return ui.hub.TooBig("console", resource, size, limit)
}

func (ui *UI) makeBucket(w http.ResponseWriter, r *http.Request) {
	svc := r.PathValue("svc")
	st := ui.store(svc)
	if st == nil {
		http.NotFound(w, r)
		return
	}
	done(w, r, "/_pogi/resources/"+svc, storeNouns[svc]+" "+strings.TrimSpace(r.FormValue("name"))+" created", st.MakeBucket(strings.TrimSpace(r.FormValue("name"))))
}

// ---- SQS ----

func (ui *UI) sqs() *sqs.Service {
	s, _ := ui.hub.Service("sqs").(*sqs.Service)
	return s
}

func (ui *UI) sqsQueue(w http.ResponseWriter, r *http.Request) {
	acct, region, name := r.PathValue("account"), r.PathValue("region"), r.PathValue("name")
	msgs, err := ui.sqs().Peek(acct, region, name)
	if err != nil {
		back(w, r, "/_pogi/resources/sqs", err)
		return
	}
	n := map[string]int{}
	for _, m := range msgs {
		n[m.State]++
	}
	ui.renderX(w, r, "sqs", name, "sqs", "SQS queue · "+region, map[string]any{"Account": acct, "Region": region, "Name": name,
		"Messages": msgs, "Count": n, "Queue": ui.sqs().Inspect("Queues", name), "FIFO": strings.HasSuffix(name, ".fifo"), "Rows": []int{0, 1, 2, 3, 4}})
}

func (ui *UI) sqsCreate(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimSpace(r.FormValue("name"))
	err := ui.write("sqs", "CreateQueue", name, func() error { return ui.sqs().Create(core.DefaultAccount, ui.hub.Cfg.Region, name) })
	done(w, r, "/_pogi/resources/sqs", "Queue "+name+" created", err)
}

// sqsSendIn is the SendMessage input a queue drawer's form stands for: up to five String attributes,
// a delay, and for FIFO queues the group and deduplication IDs.
func sqsSendIn(r *http.Request, name string) (map[string]any, error) {
	in := map[string]any{"QueueUrl": name, "MessageBody": r.FormValue("body")}
	if d := strings.TrimSpace(r.FormValue("delay")); d != "" {
		n, err := strconv.Atoi(d)
		if err != nil || n < 0 || n > 900 {
			return nil, fmt.Errorf("Delay must be 0 to 900 seconds")
		}
		in["DelaySeconds"] = n
	}
	attrs := map[string]any{}
	for i := 0; i < 5; i++ {
		if k := strings.TrimSpace(r.FormValue("ak" + strconv.Itoa(i))); k != "" {
			attrs[k] = map[string]string{"DataType": "String", "StringValue": r.FormValue("av" + strconv.Itoa(i))}
		}
	}
	if len(attrs) > 0 {
		in["MessageAttributes"] = attrs
	}
	if strings.HasSuffix(name, ".fifo") {
		in["MessageGroupId"] = cmp.Or(strings.TrimSpace(r.FormValue("group")), "console")
		in["MessageDeduplicationId"] = cmp.Or(strings.TrimSpace(r.FormValue("dedup")), core.NewID())
	}
	return in, nil
}

func (ui *UI) sqsSend(w http.ResponseWriter, r *http.Request) {
	acct, region, name := r.PathValue("account"), r.PathValue("region"), r.PathValue("name")
	in, err := sqsSendIn(r, name)
	if err == nil {
		err = ui.hub.ConsoleCall(acct, region, "AmazonSQS", "SendMessage", in, nil)
	}
	stay(w, r, "/_pogi/sqs/"+acct+"/"+region+"/"+url.PathEscape(name), "Message sent", err)
}

// sqsCLI answers the drawer's CLI mirror for the send form.
func (ui *UI) sqsCLI(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	in, err := sqsSendIn(r, name)
	if err != nil {
		fmt.Fprint(w, "# "+html.EscapeString(err.Error()))
		return
	}
	c := "pogi aws sqs send-message --queue-url " + shellArg("http://"+r.Host+"/"+r.PathValue("account")+"/"+name) +
		" --message-body " + shellArg(fmt.Sprint(in["MessageBody"]))
	if v, ok := in["DelaySeconds"]; ok {
		c += fmt.Sprintf(" --delay-seconds %d", v)
	}
	if v, ok := in["MessageAttributes"]; ok {
		b, _ := json.Marshal(v)
		c += " --message-attributes " + shellArg(string(b))
	}
	if g, ok := in["MessageGroupId"]; ok {
		c += " --message-group-id " + shellArg(fmt.Sprint(g)) + " --message-deduplication-id " + shellArg(fmt.Sprint(in["MessageDeduplicationId"]))
	}
	fmt.Fprint(w, html.EscapeString(c))
}

func (ui *UI) sqsPurge(w http.ResponseWriter, r *http.Request) {
	acct, region, name := r.PathValue("account"), r.PathValue("region"), r.PathValue("name")
	err := ui.write("sqs", "PurgeQueue", name, func() error { return ui.sqs().Purge(acct, region, name) })
	stay(w, r, "/_pogi/sqs/"+acct+"/"+region+"/"+url.PathEscape(name), "Queue purged", err)
}

// ---- Lambda ----

func (ui *UI) lambdaPage(w http.ResponseWriter, r *http.Request) {
	l, _ := ui.hub.Service("lambda").(*lambda.Service)
	acct, region, name := r.PathValue("account"), r.PathValue("region"), r.PathValue("name")
	if l == nil {
		http.NotFound(w, r)
		return
	}
	info := l.Inspect("Functions", name)
	if info == nil {
		back(w, r, "/_pogi/resources/lambda", fmt.Errorf("function %s not found", name))
		return
	}
	ui.renderX(w, r, "lambda", name, "lambda", "Lambda function · "+region, map[string]any{"Account": acct, "Region": region, "Name": name,
		"Info": info, "Logs": l.RecentLogs(acct, region, name)})
}

func (ui *UI) lambdaInvoke(w http.ResponseWriter, r *http.Request) {
	l, _ := ui.hub.Service("lambda").(*lambda.Service)
	acct, region, name := r.PathValue("account"), r.PathValue("region"), r.PathValue("name")
	payload := strings.TrimSpace(r.FormValue("payload"))
	if payload == "" {
		payload = "{}"
	}
	data := map[string]any{}
	if !json.Valid([]byte(payload)) {
		data["Error"] = "payload is not valid JSON"
	} else if res, err := l.Invoke(acct, region, name, []byte(payload)); err != nil {
		data["Error"] = err.Error()
	} else {
		data["Result"] = res
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	ui.pages["lambda"].ExecuteTemplate(w, "result", data)
}

// ---- SNS, EventBridge, inbox ----

func (ui *UI) snsPublish(w http.ResponseWriter, r *http.Request) {
	s, _ := ui.hub.Service("sns").(*sns.Service)
	topic := r.FormValue("topic")
	_, region, account, _, _ := core.ParseARN(topic)
	err := ui.write("sns", "Publish", topic, func() error {
		_, err := s.PublishConsole(account, region, topic, r.FormValue("subject"), r.FormValue("message"))
		return err
	})
	done(w, r, "/_pogi/resources/sns", "Message published", err)
}

func (ui *UI) eventsPut(w http.ResponseWriter, r *http.Request) {
	e, _ := ui.hub.Service("events").(*events.Service)
	detail := strings.TrimSpace(r.FormValue("detail"))
	if detail == "" {
		detail = "{}"
	}
	err := ui.write("events", "PutEvents", r.FormValue("bus"), func() error {
		_, err := e.PutEvent(core.DefaultAccount, ui.hub.Cfg.Region, r.FormValue("bus"), r.FormValue("source"), r.FormValue("detailType"), detail)
		return err
	})
	done(w, r, "/_pogi/resources/events", "Event sent", err)
}

// mail is one captured message for the inbox page.
type mail struct {
	Time                       time.Time
	Service, Kind, To, Subject string
	Body                       string
	HTML, Raw                  string // an HTML part, and the raw MIME message, for the preview page
	N                          int    // position in the inbox, newest first
}

// inbox gathers what pogi "sent" instead of reaching the outside world.
func (ui *UI) inbox(w http.ResponseWriter, r *http.Request) {
	list := ui.mailbox()
	if len(list) > 0 {
		r.SetPathValue("n", "0") // the newest message is open, as in a mail client
		ui.mailPage(w, r)
		return
	}
	ui.render(w, r, "inbox", "Inbox", "inbox", map[string]any{"Mail": list})
}

// inboxClear empties every service's captured mail.
func (ui *UI) inboxClear(w http.ResponseWriter, r *http.Request) {
	for _, n := range []string{"sns", "ses", "azemail", "firebaseauth"} {
		if c, ok := ui.hub.Service(n).(interface{ ClearOutbox() }); ok {
			c.ClearOutbox()
		}
	}
	done(w, r, "/_pogi/inbox", "Inbox cleared", nil)
}

// mailbox is the captured messages, newest first.
func (ui *UI) mailbox() []mail {
	var list []mail
	if s, ok := ui.hub.Service("sns").(*sns.Service); ok {
		for _, m := range s.Outbox() {
			list = append(list, mail{Time: m.Timestamp, Service: "sns", Kind: m.Protocol, To: m.Destination, Subject: m.Subject, Body: m.Message})
		}
	}
	if s, ok := ui.hub.Service("ses").(*ses.Service); ok {
		for _, e := range s.Outbox() {
			var to []string
			if d := e.Destination; d != nil {
				to = append(append(append(to, d.ToAddresses...), d.CcAddresses...), d.BccAddresses...)
			}
			body, html := e.RawData, ""
			if b := e.Body; b != nil {
				body, html = cmp.Or(b.TextPart, b.HtmlPart), b.HtmlPart
			}
			if e.Template != "" {
				body = "template " + e.Template + " " + e.TemplateData
			}
			list = append(list, mail{Time: e.Timestamp, Service: "ses", Kind: "email", To: strings.Join(to, ", "), Subject: e.Subject, Body: body, HTML: html, Raw: e.RawData})
		}
	}
	if s, ok := ui.hub.Service("azemail").(*email.Service); ok {
		for _, m := range s.Outbox() {
			var to []string
			for _, a := range slices.Concat(m.To, m.Cc, m.Bcc) {
				to = append(to, a.Address)
			}
			list = append(list, mail{Time: m.Time, Service: "azemail", Kind: "email", To: strings.Join(to, ", "), Subject: m.Subject, Body: cmp.Or(m.Text, m.HTML), HTML: m.HTML})
		}
	}
	if s, ok := ui.hub.Service("firebaseauth").(*firebaseauth.Service); ok {
		for _, m := range s.Outbox() {
			list = append(list, mail{Time: m.Time, Service: "firebaseauth", Kind: m.Kind, To: m.To, Subject: m.Subject, Body: m.Link})
		}
	}
	slices.SortFunc(list, func(a, b mail) int { return b.Time.Compare(a.Time) })
	for i := range list {
		list[i].N = i
	}
	return list
}
