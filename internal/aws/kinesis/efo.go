package kinesis

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"net/http"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// SubscribeToShard (enhanced fan-out): one long response of event-stream
// frames. Each SubscribeToShardEvent carries the records after the last one
// sent. Like AWS, the subscription ends after five minutes, or when a closed
// shard has been read to its end (that last event names the child shards).
const (
	subscriptionLimit = 5 * time.Minute
	heartbeat         = 5 * time.Second
	eventRecords      = 1000
)

// pollEvery is how often a subscription looks for new records.
// ponytail: polling; a notify channel per shard if the delay matters.
var pollEvery = 100 * time.Millisecond

// eventFrame is one event-stream message: prelude, headers, JSON payload, CRCs.
func eventFrame(kind string, payload []byte) []byte {
	var hdr []byte
	for _, h := range [][2]string{{":event-type", kind}, {":content-type", "application/json"}, {":message-type", "event"}} {
		hdr = append(hdr, byte(len(h[0])))
		hdr = append(hdr, h[0]...)
		hdr = append(hdr, 7) // string
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(h[1])))
		hdr = append(hdr, h[1]...)
	}
	b := binary.BigEndian.AppendUint32(nil, uint32(12+len(hdr)+len(payload)+4))
	b = binary.BigEndian.AppendUint32(b, uint32(len(hdr)))
	b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	b = append(b, hdr...)
	b = append(b, payload...)
	return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
}

type subscribeInput struct {
	ConsumerARN      string
	ShardId          string
	StartingPosition struct {
		Type           string
		SequenceNumber string
		Timestamp      *float64
	}
}

// subscribe checks the request like any operation (errors are plain JSON),
// then keeps the response open and writes events.
func (s *Service) subscribe(w http.ResponseWriter, r *http.Request) {
	c := core.C(r)
	c.Op = "SubscribeToShard"
	var in subscribeInput
	var it iterator
	err := json.Unmarshal(core.Body(r), &in)
	if err != nil {
		err = core.Errorf(400, "SerializationException", "%v", err)
	}
	if err == nil {
		err = c.Intercept()
	}
	if err == nil {
		s.mu.Lock()
		err = s.begin(c, &in, &it)
		s.mu.Unlock()
	}
	if err != nil {
		e := core.AsError(err)
		c.ErrCode = e.Code
		core.WriteJSONError(w, c, "1.1", e)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/vnd.amazon.eventstream")
	h.Set("x-amzn-RequestId", c.RequestID)
	w.WriteHeader(http.StatusOK)
	flush, _ := w.(http.Flusher)
	write := func(kind string, ev any) bool {
		b, _ := json.Marshal(ev)
		if _, err := w.Write(eventFrame(kind, b)); err != nil {
			return false
		}
		if flush != nil {
			flush.Flush()
		}
		return true
	}
	if !write("initial-response", struct{}{}) { // the SDK reads this first; it also flushes the headers
		return
	}
	send := func(ev map[string]any) bool { return write("SubscribeToShardEvent", ev) }

	end := time.Now().Add(subscriptionLimit)
	last := time.Time{} // the last event sent; zero sends one at once
	tick := time.NewTicker(pollEvery)
	defer tick.Stop()
	for first := true; ; first = false {
		if !first {
			select {
			case <-tick.C:
			case <-r.Context().Done():
				return
			}
		}
		s.mu.Lock()
		st, sh, err := s.shardOf(c, &in)
		if err != nil { // the stream or shard was deleted
			s.mu.Unlock()
			return
		}
		st.trim(sh)
		unread := sh.after(it)
		batch := unread[:min(eventRecords, len(unread))]
		finished := len(batch) == len(unread) && sh.EndSeq != ""
		var children []map[string]any
		if finished {
			children = st.children(sh)
		}
		s.mu.Unlock()
		if len(batch) == 0 && !finished && time.Since(last) < heartbeat {
			if time.Now().After(end) {
				return
			}
			continue
		}
		recs := []outRecord{}
		for _, rec := range batch {
			recs = append(recs, outRecord{rec.Seq, core.Unix(rec.At), rec.Data, rec.Key, ""})
		}
		behind := 0
		if n := len(unread) - len(batch); n > 0 {
			behind = int(time.Since(unread[len(batch)].At).Milliseconds())
		}
		if len(batch) > 0 {
			it.After, it.At = batch[len(batch)-1].Seq, false
		}
		cont := it.After
		if cont == "" {
			cont = sh.StartSeq
		}
		ev := map[string]any{"Records": recs, "ContinuationSequenceNumber": cont, "MillisBehindLatest": behind}
		if finished {
			ev["ChildShards"] = children
		}
		if !send(ev) || finished {
			return
		}
		last = time.Now()
		if time.Now().After(end) {
			return
		}
	}
}

// shardOf finds the stream and shard of a subscription; lock held.
func (s *Service) shardOf(c *core.Ctx, in *subscribeInput) (*Stream, *Shard, error) {
	st, _, err := s.consumer(c, "", "", in.ConsumerARN)
	if err != nil {
		return nil, nil, err
	}
	sh, err := st.shard(in.ShardId)
	return st, sh, err
}

// begin checks a subscription and sets its starting position; lock held.
func (s *Service) begin(c *core.Ctx, in *subscribeInput, it *iterator) error {
	if in.ConsumerARN == "" || in.ShardId == "" || in.StartingPosition.Type == "" {
		return validation("ConsumerARN, ShardId and StartingPosition are required.")
	}
	st, sh, err := s.shardOf(c, in)
	if err != nil {
		return err
	}
	p := in.StartingPosition
	*it, err = st.position(sh, p.Type, p.SequenceNumber, p.Timestamp)
	return err
}
