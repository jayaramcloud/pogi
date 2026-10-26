package eventgrid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// retrySchedule is Event Grid's wait before each retry; the last step repeats.
var retrySchedule = []time.Duration{10 * time.Second, 30 * time.Second, time.Minute, 5 * time.Minute, 10 * time.Minute,
	30 * time.Minute, time.Hour, 3 * time.Hour, 6 * time.Hour, 12 * time.Hour}

func backoff(attempt int) time.Duration {
	return retrySchedule[min(max(attempt, 1), len(retrySchedule))-1]
}

// Tick makes every delivery that is due at now: the first attempt of a published event, or a retry. It
// runs every second, and when events are published; tests call it with a later now to skip the backoff.
func (s *Service) Tick(now time.Time) {
	s.mu.Lock()
	var due []*Delivery
	rest := s.st.Pending[:0:0]
	for _, d := range s.st.Pending {
		if d.Due.After(now) {
			rest = append(rest, d)
		} else {
			due = append(due, d)
		}
	}
	s.st.Pending = rest
	subs := make([]Subscription, len(due)) // a copy: attempts run without the lock
	for i, d := range due {
		if sub := s.st.Subs[d.Sub]; sub != nil {
			subs[i] = *sub
		}
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for i, d := range due {
		if subs[i].Name == "" {
			continue // the subscription is gone
		}
		wg.Add(1)
		core.Go("eventgrid.Tick", func() {
			defer wg.Done()
			s.attempt(d, &subs[i], now)
		})
	}
	wg.Wait()
}

// attempt delivers once; a failure is retried after the backoff, until the attempts or the time to live
// run out and the event is dead-lettered (when the subscription has a destination for that) or dropped.
func (s *Service) attempt(d *Delivery, sub *Subscription, now time.Time) {
	d.Attempts++
	status, err := s.send(sub, d)
	if err == nil {
		return
	}
	d.Reason, d.Status = err.Error(), status
	wait := s.Backoff(d.Attempts)
	switch {
	case d.Attempts >= sub.Retry.MaxAttempts:
		s.deadLetter(sub, d, "MaxDeliveryAttemptsExceeded")
	case now.Add(wait).After(d.Expires):
		s.deadLetter(sub, d, "TimeToLiveExceeded")
	default:
		d.Due = now.Add(wait)
		s.mu.Lock()
		if s.st.Subs[d.Sub] != nil { // not deleted or reset meanwhile
			s.st.Pending = append(s.st.Pending, d)
		}
		s.mu.Unlock()
	}
}

// send makes one delivery attempt and returns the HTTP status of a webhook.
func (s *Service) send(sub *Subscription, d *Delivery) (int, error) {
	dest := sub.Destination
	if dest.Type == "WebHook" {
		start := time.Now()
		code, err := s.post(sub, d)
		s.hub.Span(core.SpanNotify, "eventgrid", "deliver", d.Event.Topic, dest.Endpoint, d.Trace, start, err)
		return code, err
	}
	body, _ := render(sub.Schema, d.Event)
	if dest.Type == "EventHub" {
		body = append(append([]byte{'['}, body...), ']') // Event Hubs get an array
	}
	return 0, s.hub.Deliver(dest.target(), &core.Message{Body: string(body), Subject: d.Event.Subject, DetailType: d.Event.EventType, Trace: d.Trace, From: d.Event.Topic})
}

func (s *Service) post(sub *Subscription, d *Delivery) (int, error) {
	body, ct := render(sub.Schema, d.Event)
	if sub.Schema != "CloudEventSchemaV1_0" {
		body = append(append([]byte{'['}, body...), ']')
	}
	req, _ := http.NewRequest(http.MethodPost, sub.Destination.Endpoint, bytes.NewReader(body))
	req.Header.Set("Content-Type", ct)
	req.Header.Set("aeg-event-type", "Notification")
	req.Header.Set("aeg-subscription-name", strings.ToUpper(sub.Name))
	req.Header.Set("aeg-delivery-count", itoa(d.Attempts-1))
	req.Header.Set("aeg-data-version", d.Event.DataVersion)
	req.Header.Set("aeg-metadata-version", "1")
	if t := core.Traceparent(d.Trace); t != "" {
		req.Header.Set("traceparent", t)
	}
	return s.do(req)
}

func (s *Service) do(req *http.Request) (int, error) {
	res, err := s.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	io.Copy(io.Discard, io.LimitReader(res.Body, 1<<16))
	if res.StatusCode/100 != 2 {
		return res.StatusCode, fmt.Errorf("the endpoint answered %s", res.Status)
	}
	return res.StatusCode, nil
}

// render writes an event in a delivery schema.
func render(schema string, e Event) (body []byte, contentType string) {
	data := e.Data
	if len(data) == 0 {
		data = json.RawMessage("{}")
	}
	if schema == "CloudEventSchemaV1_0" {
		ce := struct {
			SpecVersion     string          `json:"specversion"`
			ID              string          `json:"id"`
			Source          string          `json:"source"`
			Type            string          `json:"type"`
			Subject         string          `json:"subject,omitempty"`
			Time            string          `json:"time"`
			DataContentType string          `json:"datacontenttype"`
			Data            json.RawMessage `json:"data"`
		}{"1.0", e.ID, orStr(e.Source, e.Topic), e.EventType, e.Subject, e.EventTime, orStr(e.DataContentType, "application/json"), data}
		body, _ = json.Marshal(ce)
		return body, "application/cloudevents+json; charset=utf-8"
	}
	body, _ = json.Marshal(struct {
		ID              string          `json:"id"`
		Topic           string          `json:"topic"`
		Subject         string          `json:"subject"`
		EventType       string          `json:"eventType"`
		EventTime       string          `json:"eventTime"`
		Data            json.RawMessage `json:"data"`
		DataVersion     string          `json:"dataVersion"`
		MetadataVersion string          `json:"metadataVersion"`
	}{e.ID, e.Topic, e.Subject, e.EventType, e.EventTime, data, e.DataVersion, "1"})
	return body, "application/json; charset=utf-8"
}

// handshake proves a webhook wants the events: an Event Grid schema subscription is sent a
// SubscriptionValidationEvent and must answer with its validationCode; a CloudEvents one gets the
// OPTIONS abuse-protection request and must allow the origin eventgrid.azure.net.
func (s *Service) handshake(sub *Subscription) error {
	fail := func(why string) error {
		return core.Errorf(http.StatusBadRequest, "Url validation", "The attempt to validate the provided endpoint %s failed (%s). "+
			"For more details, visit https://aka.ms/esvalidation.", sub.Destination.Endpoint, why)
	}
	if sub.Schema == "CloudEventSchemaV1_0" {
		req, _ := http.NewRequest(http.MethodOptions, sub.Destination.Endpoint, nil)
		req.Header.Set("WebHook-Request-Origin", "eventgrid.azure.net")
		req.Header.Set("WebHook-Request-Callback", "https://eventgrid.azure.net/validate")
		req.Header.Set("WebHook-Request-Rate", "120")
		res, err := s.http.Do(req)
		if err != nil {
			return fail(err.Error())
		}
		res.Body.Close()
		if o := res.Header.Get("WebHook-Allowed-Origin"); res.StatusCode/100 != 2 || (o != "*" && o != "eventgrid.azure.net") {
			return fail("the OPTIONS request did not allow the origin eventgrid.azure.net")
		}
		return nil
	}
	code := core.UUID()
	s.mu.Lock()
	topic := s.topicID(sub.Scope)
	s.mu.Unlock()
	body, _ := json.Marshal([]map[string]any{{
		"id": core.UUID(), "topic": topic, "subject": "", "eventType": "Microsoft.EventGrid.SubscriptionValidationEvent",
		"eventTime": time.Now().UTC().Format(time.RFC3339Nano), "metadataVersion": "1", "dataVersion": "2",
		"data": map[string]string{"validationCode": code,
			"validationUrl": "https://rp-eastus.eventgrid.azure.net:553/eventsubscriptions/" + sub.Name + "/validate?id=" + code},
	}})
	req, _ := http.NewRequest(http.MethodPost, sub.Destination.Endpoint, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("aeg-event-type", "SubscriptionValidation")
	res, err := s.http.Do(req)
	if err != nil {
		return fail(err.Error())
	}
	defer res.Body.Close()
	var out struct {
		ValidationResponse string `json:"validationResponse"`
	}
	json.NewDecoder(io.LimitReader(res.Body, 1<<16)).Decode(&out)
	if res.StatusCode/100 != 2 || out.ValidationResponse != code {
		return fail("the endpoint answered " + res.Status + " without the validationResponse")
	}
	return nil
}

// deadLetter writes a failed event to the subscription's blob container, as
// <topic>/<subscription>/<yyyy>/<MM>/<dd>/<HH>/<id>.json; without a destination it is dropped.
func (s *Service) deadLetter(sub *Subscription, d *Delivery, reason string) {
	dl := sub.DeadLetter
	if dl == nil || dl.ResourceID == "" {
		return
	}
	_, account, _ := strings.Cut(strings.ToLower(dl.ResourceID), "/storageaccounts/")
	account, _, _ = strings.Cut(account, "/")
	now := time.Now().UTC()
	// the event as it was published, plus why it ended up here
	var doc map[string]any
	b, _ := json.Marshal(d.Event)
	json.Unmarshal(b, &doc)
	doc["deadLetterReason"], doc["deliveryAttempts"], doc["lastDeliveryOutcome"] = reason, d.Attempts, "Failed"
	doc["lastDeliveryAttemptTime"], doc["publishTime"] = now.Format(time.RFC3339Nano), d.Event.EventTime
	if d.Status != 0 {
		doc["lastHttpStatusCode"] = d.Status
	}
	body, _ := json.Marshal([]any{doc})
	topic := d.Event.Topic[strings.LastIndex(d.Event.Topic, "/")+1:]
	name := fmt.Sprintf("%s/%s/%s/%s.json", topic, sub.Name, now.Format("2006/01/02/15"), d.Event.ID)
	put := func(path string, h map[string]string, body []byte) int {
		req, _ := http.NewRequest(http.MethodPut, "http://localhost"+path, bytes.NewReader(body))
		for k, v := range h {
			req.Header.Set(k, v)
		}
		status, _ := s.hub.DoService("azblob", req)
		return status
	}
	put("/"+account+"/"+dl.Container+"?restype=container", nil, nil) // 409 when it is there
	if status := put("/"+account+"/"+dl.Container+"/"+name, map[string]string{"x-ms-blob-type": "BlockBlob", "Content-Type": "application/json"}, body); status != http.StatusCreated {
		s.hub.Log.Warn("eventgrid: could not dead-letter an event", "subscription", d.Sub, "status", status)
	}
}
