package web

// Kinesis "Records" tab: put a record on a stream and read records from a shard.
// Writes go through the hub as the console, so they show in Activity.

import (
	"encoding/base64"
	"errors"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type kinRec struct {
	Seq, Key, Data, Arrived string
	Binary                  bool
}

type kinData struct {
	Streams  []choice
	V        map[string]string
	Recs     []kinRec
	Got      bool
	Err, Msg string
}

var shardRE = regexp.MustCompile(`^shardId-[0-9]{12}$`)

func (ui *UI) kinesisRecs(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 2<<20)
	if err := r.ParseForm(); err != nil {
		http.Error(w, "The form is too large or unreadable.", http.StatusBadRequest)
		return
	}
	d := &kinData{V: map[string]string{"shard": "shardId-000000000000", "limit": "10", "from": "oldest"}}
	for k := range r.Form {
		if k != "act" {
			d.V[k] = r.Form.Get(k)
		}
	}
	for _, s := range ui.rows(r, "kinesis", "Streams") { // name, region, account
		d.Streams = append(d.Streams, choice{s[2] + "/" + s[1] + "/" + s[0], s[0]})
	}
	if r.Method == http.MethodPost {
		if err := ui.kinesisDo(r.PostForm.Get("act"), d); err != nil {
			d.Err = err.Error()
		}
	}
	ui.xrender(w, r, "x-kinesis", "Kinesis", "kinesis", d)
}

func (ui *UI) kinesisDo(act string, d *kinData) error {
	t := strings.SplitN(d.V["stream"], "/", 3) // account/region/stream
	if len(t) != 3 {
		return errors.New("Choose a stream.")
	}
	account, region, stream := t[0], t[1], t[2]
	const target = "Kinesis_20131202"
	switch act {
	case "put":
		key := d.V["key"]
		if key == "" || len(key) > 256 {
			return errors.New("Partition key is required, up to 256 characters.")
		}
		in := map[string]any{"StreamName": stream, "PartitionKey": key, "Data": base64.StdEncoding.EncodeToString([]byte(d.V["data"]))}
		var out struct{ SequenceNumber, ShardId string }
		if err := ui.hub.ConsoleCall(account, region, target, "PutRecord", in, &out); err != nil {
			return err
		}
		d.Msg = "Record put on " + stream + ", shard " + out.ShardId + ", sequence " + out.SequenceNumber
		return nil
	case "get":
		if !shardRE.MatchString(d.V["shard"]) {
			return errors.New("Shard must look like shardId-000000000000.")
		}
		limit, err := strconv.Atoi(d.V["limit"])
		if err != nil || limit < 1 || limit > 1000 {
			return errors.New("Limit must be from 1 to 1000.")
		}
		typ := map[string]string{"oldest": "TRIM_HORIZON", "latest": "LATEST"}[d.V["from"]]
		if typ == "" {
			return errors.New("From must be oldest or latest.")
		}
		var it struct{ ShardIterator string }
		if err := ui.hub.Call(account, region, target, "GetShardIterator", map[string]any{"StreamName": stream, "ShardId": d.V["shard"], "ShardIteratorType": typ}, &it); err != nil {
			return err
		}
		var out struct {
			Records []struct {
				SequenceNumber, PartitionKey, Data string
				ApproximateArrivalTimestamp        float64
			}
		}
		if err := ui.hub.Call(account, region, target, "GetRecords", map[string]any{"ShardIterator": it.ShardIterator, "Limit": limit}, &out); err != nil {
			return err
		}
		d.Got = true
		for _, rec := range out.Records {
			raw, err := base64.StdEncoding.DecodeString(rec.Data)
			if err != nil {
				raw = []byte(rec.Data)
			}
			kr := kinRec{Seq: rec.SequenceNumber, Key: rec.PartitionKey, Arrived: time.Unix(int64(rec.ApproximateArrivalTimestamp), 0).UTC().Format(time.RFC3339)}
			kr.Data, kr.Binary = showData(raw)
			d.Recs = append(d.Recs, kr)
		}
		return nil
	}
	return errors.New("Unknown action.")
}
