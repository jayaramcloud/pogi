package web

// Pub/Sub "Messages" tab: pull messages from a subscription and ack the ones you choose.
// The calls go through the Pub/Sub service as the console, so they show in Activity.

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

func init() {
	explorerTabs["pubsub"] = struct{ Key, Label, URL string }{"messages", "Messages", "/_pogi/pubsub/messages"}
	explorerTabs["kinesis"] = struct{ Key, Label, URL string }{"records", "Records", "/_pogi/kinesis/records"}
}

// dataRoutes registers the pages of this file, kinesis_recs.go and ddb_items.go.
func (ui *UI) dataRoutes(m *http.ServeMux) {
	for _, p := range []string{"pubsub", "kinesis"} {
		ui.pages["x-"+p] = template.Must(template.Must(shell().Clone()).ParseFS(files, "templates/x-common.html", "templates/x-"+p+".html"))
	}
	m.HandleFunc("POST /_pogi/query/dynamodb/{act}", ui.ddbItem)
	m.HandleFunc("GET /_pogi/pubsub/messages", ui.pubsubMsgs)
	m.HandleFunc("POST /_pogi/pubsub/messages", ui.pubsubMsgs)
	m.HandleFunc("GET /_pogi/kinesis/records", ui.kinesisRecs)
	m.HandleFunc("POST /_pogi/kinesis/records", ui.kinesisRecs)
}

// restJSON calls a REST service as the console and decodes the JSON answer into out (when not nil).
func (ui *UI) restJSON(sc scope, svc, verb, path string, body, out any) error {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(verb, "http://localhost"+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(core.ConsoleHeader, "1")
	status, resp := ui.hub.DoServiceAs(svc, sc.acct(), sc.region(ui.hub.Cfg.Region), req)
	if status >= 300 {
		return fmt.Errorf("%d: %s", status, apiMessage(resp))
	}
	if out != nil && len(resp) > 0 {
		return json.Unmarshal(resp, out)
	}
	return nil
}

type psMsg struct {
	AckID, Data, Published string
	Binary                 bool
	Attrs                  map[string]string
}

type psData struct {
	Subs   []choice
	Sub    string
	N      int
	Msgs   []psMsg
	Pulled bool
	Err    string
	Msg    string
}

var subRE = regexp.MustCompile(`^projects/[A-Za-z0-9_.-]+/subscriptions/[A-Za-z0-9_.%~+-]+$`)

// showData is a message body as text, or as base64 when it is not text.
func showData(b []byte) (string, bool) {
	if utf8.Valid(b) && !strings.ContainsFunc(string(b), func(r rune) bool { return r < ' ' && r != '\n' && r != '\t' && r != '\r' }) {
		return string(b), false
	}
	return base64.StdEncoding.EncodeToString(b), true
}

func (ui *UI) pubsubMsgs(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "The form is too large or unreadable.", http.StatusBadRequest)
		return
	}
	d := &psData{Sub: r.Form.Get("sub"), N: 10}
	for _, s := range ui.rows(r, "pubsub", "Subscriptions") {
		if s[4] == "push" {
			continue // a push subscription has no pull
		}
		d.Subs = append(d.Subs, choice{s[0], s[0][strings.LastIndexByte(s[0], '/')+1:] + " · " + s[1][strings.LastIndexByte(s[1], '/')+1:]})
	}
	if r.Method == http.MethodPost {
		if err := ui.pubsubDo(r, d); err != nil {
			d.Err = err.Error()
		}
	}
	ui.xrender(w, r, "x-pubsub", "Pub/Sub", "pubsub", d)
}

// pubsubDo runs the form's action: pull, ack (the ticked ones) or pull and ack.
func (ui *UI) pubsubDo(r *http.Request, d *psData) error {
	act := r.PostForm.Get("act")
	if !subRE.MatchString(d.Sub) {
		return errors.New("Choose a subscription.")
	}
	if n, err := strconv.Atoi(r.PostForm.Get("n")); err != nil || n < 1 || n > 100 {
		return errors.New("Number of messages must be from 1 to 100.")
	} else {
		d.N = n
	}
	sc := ui.scope(r)
	if act == "ack" {
		ids := r.PostForm["ack"]
		if len(ids) == 0 || len(ids) > 100 {
			return errors.New("Tick the messages to ack first.")
		}
		if err := ui.restJSON(sc, "pubsub", http.MethodPost, "/v1/"+d.Sub+":acknowledge", map[string]any{"ackIds": ids}, nil); err != nil {
			return err
		}
		d.Msg = plural(len(ids), "message") + " acked"
		return nil
	}
	if act != "pull" && act != "pullack" {
		return errors.New("Unknown action.")
	}
	var out struct {
		ReceivedMessages []struct {
			AckID   string
			Message struct {
				Data        string
				Attributes  map[string]string
				PublishTime string
			}
		}
	}
	if err := ui.restJSON(sc, "pubsub", http.MethodPost, "/v1/"+d.Sub+":pull", map[string]any{"maxMessages": d.N, "returnImmediately": true}, &out); err != nil {
		return err
	}
	d.Pulled = true
	var ids []string
	for _, m := range out.ReceivedMessages {
		raw, err := base64.StdEncoding.DecodeString(m.Message.Data)
		if err != nil {
			raw = []byte(m.Message.Data)
		}
		pm := psMsg{AckID: m.AckID, Attrs: m.Message.Attributes, Published: m.Message.PublishTime}
		pm.Data, pm.Binary = showData(raw)
		d.Msgs = append(d.Msgs, pm)
		ids = append(ids, m.AckID)
	}
	if act == "pullack" && len(ids) > 0 {
		if err := ui.restJSON(sc, "pubsub", http.MethodPost, "/v1/"+d.Sub+":acknowledge", map[string]any{"ackIds": ids}, nil); err != nil {
			return err
		}
		d.Msg = plural(len(ids), "message") + " pulled and acked"
	}
	return nil
}
