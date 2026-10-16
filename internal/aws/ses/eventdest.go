package ses

import (
	"encoding/json"
	"net/http"
	"net/mail"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// Configuration sets: event destinations (SNS, EventBridge, CloudWatch; Firehose and
// Pinpoint are stored and echoed), and the options a v2 client sets. Both API versions
// share one record. Event types are kept in the v2 spelling (RENDERING_FAILURE).

// ConfigSet is what a configuration set holds besides its name.
type ConfigSet struct {
	Dests   []EventDest                `json:"dests,omitempty"`
	Options map[string]json.RawMessage `json:"options,omitempty"` // v2 options as sent: DeliveryOptions, Tags, ...
}

// EventDest is one event destination.
type EventDest struct {
	Name           string      `json:"name"`
	Enabled        bool        `json:"enabled"`
	Types          []string    `json:"types"`
	SNSTopic       string      `json:"snsTopic,omitempty"`
	EventBus       string      `json:"eventBus,omitempty"`
	Dimensions     []Dimension `json:"dimensions,omitempty"` // set when the destination is CloudWatch
	CloudWatch     bool        `json:"cloudWatch,omitempty"`
	FirehoseRole   string      `json:"firehoseRole,omitempty"`
	FirehoseStream string      `json:"firehoseStream,omitempty"`
	Pinpoint       string      `json:"pinpoint,omitempty"`
}

// Dimension is a CloudWatch dimension configuration.
type Dimension struct{ Name, Source, Default string }

var eventTypes = []string{"SEND", "REJECT", "BOUNCE", "COMPLAINT", "DELIVERY", "OPEN", "CLICK", "RENDERING_FAILURE", "DELIVERY_DELAY", "SUBSCRIPTION"}

// v2Type turns a v1 event type (renderingFailure) into the v2 spelling.
func v2Type(t string) string {
	var b strings.Builder
	for i, r := range t {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToUpper(b.String())
}

// v1Type is the reverse: RENDERING_FAILURE becomes renderingFailure.
func v1Type(t string) string {
	parts := strings.Split(strings.ToLower(t), "_")
	for i := 1; i < len(parts); i++ {
		parts[i] = strings.ToUpper(parts[i][:1]) + parts[i][1:]
	}
	return strings.Join(parts, "")
}

// check validates a destination as both API versions require.
func (d *EventDest) check() string {
	if d.Name == "" {
		return "EventDestinationName is required."
	}
	if len(d.Types) == 0 {
		return "MatchingEventTypes must have at least one event type."
	}
	for _, t := range d.Types {
		if !slices.Contains(eventTypes, t) {
			return "Invalid event type " + t + "."
		}
	}
	n := 0
	for _, set := range []bool{d.SNSTopic != "", d.EventBus != "", d.CloudWatch, d.FirehoseStream != "", d.Pinpoint != ""} {
		if set {
			n++
		}
	}
	if n != 1 {
		return "An event destination needs exactly one destination."
	}
	if d.EventBus != "" && !strings.HasSuffix(d.EventBus, ":event-bus/default") {
		return "EventBusArn: only the default event bus is supported."
	}
	return ""
}

// details returns the record of a set, creating it when missing; lock held.
func (s *Service) details(c *core.Ctx, name string) *ConfigSet {
	if s.st.ConfigDetails == nil {
		s.st.ConfigDetails = map[string]*ConfigSet{}
	}
	d := s.st.ConfigDetails[scope(c)+name]
	if d == nil {
		d = &ConfigSet{}
		s.st.ConfigDetails[scope(c)+name] = d
	}
	return d
}

// changeDests runs f on the destinations of a set that exists.
func (s *Service) changeDests(c *core.Ctx, set string, f func(cs *ConfigSet) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ConfigSets[scope(c)+set]; !ok {
		return noConfigSet(set)
	}
	return f(s.details(c, set))
}

func addDest(cs *ConfigSet, d EventDest, bad func(string) error, exists error) error {
	if msg := d.check(); msg != "" {
		return bad(msg)
	}
	if slices.ContainsFunc(cs.Dests, func(x EventDest) bool { return x.Name == d.Name }) {
		return exists
	}
	cs.Dests = append(cs.Dests, d)
	return nil
}

func replaceDest(cs *ConfigSet, d EventDest, bad func(string) error, missing error) error {
	if msg := d.check(); msg != "" {
		return bad(msg)
	}
	i := slices.IndexFunc(cs.Dests, func(x EventDest) bool { return x.Name == d.Name })
	if i < 0 {
		return missing
	}
	cs.Dests[i] = d
	return nil
}

func removeDest(cs *ConfigSet, name string, missing error) error {
	i := slices.IndexFunc(cs.Dests, func(x EventDest) bool { return x.Name == name })
	if i < 0 {
		return missing
	}
	cs.Dests = slices.Delete(cs.Dests, i, i+1)
	return nil
}

// ---- v1 ----

type destV1 struct {
	Name                       string
	Enabled                    bool
	MatchingEventTypes         []string
	KinesisFirehoseDestination *struct{ IAMRoleARN, DeliveryStreamARN string } `json:",omitempty"`
	CloudWatchDestination      *struct {
		DimensionConfigurations []struct{ DimensionName, DimensionValueSource, DefaultDimensionValue string }
	} `json:",omitempty"`
	SNSDestination *struct{ TopicARN string } `json:",omitempty"`
}

func (v destV1) toDest() EventDest {
	d := EventDest{Name: v.Name, Enabled: v.Enabled}
	for _, t := range v.MatchingEventTypes {
		d.Types = append(d.Types, v2Type(t))
	}
	if f := v.KinesisFirehoseDestination; f != nil {
		d.FirehoseRole, d.FirehoseStream = f.IAMRoleARN, f.DeliveryStreamARN
	}
	if cw := v.CloudWatchDestination; cw != nil {
		d.CloudWatch = true
		for _, x := range cw.DimensionConfigurations {
			d.Dimensions = append(d.Dimensions, Dimension{x.DimensionName, strings.ToUpper(v2Type(x.DimensionValueSource)), x.DefaultDimensionValue})
		}
	}
	if sn := v.SNSDestination; sn != nil {
		d.SNSTopic = sn.TopicARN
	}
	return d
}

func (d EventDest) toV1() destV1 {
	v := destV1{Name: d.Name, Enabled: d.Enabled, MatchingEventTypes: []string{}}
	for _, t := range d.Types {
		v.MatchingEventTypes = append(v.MatchingEventTypes, v1Type(t))
	}
	if d.FirehoseStream != "" {
		v.KinesisFirehoseDestination = &struct{ IAMRoleARN, DeliveryStreamARN string }{d.FirehoseRole, d.FirehoseStream}
	}
	if d.CloudWatch {
		cw := &struct {
			DimensionConfigurations []struct{ DimensionName, DimensionValueSource, DefaultDimensionValue string }
		}{}
		for _, x := range d.Dimensions {
			cw.DimensionConfigurations = append(cw.DimensionConfigurations, struct{ DimensionName, DimensionValueSource, DefaultDimensionValue string }{x.Name, v1Type(x.Source), x.Default})
		}
		v.CloudWatchDestination = cw
	}
	if d.SNSTopic != "" {
		v.SNSDestination = &struct{ TopicARN string }{d.SNSTopic}
	}
	return v
}

func (s *Service) createEventDestination(c *core.Ctx, in *struct {
	ConfigurationSetName string
	EventDestination     destV1
}) (any, error) {
	return empty, s.changeDests(c, in.ConfigurationSetName, func(cs *ConfigSet) error {
		return addDest(cs, in.EventDestination.toDest(), badV1,
			core.Errorf(400, "EventDestinationAlreadyExists", "Event destination <%s> already exists.", in.EventDestination.Name))
	})
}

func (s *Service) updateEventDestination(c *core.Ctx, in *struct {
	ConfigurationSetName string
	EventDestination     destV1
}) (any, error) {
	return empty, s.changeDests(c, in.ConfigurationSetName, func(cs *ConfigSet) error {
		return replaceDest(cs, in.EventDestination.toDest(), badV1,
			core.Errorf(400, "EventDestinationDoesNotExist", "Event destination <%s> does not exist.", in.EventDestination.Name))
	})
}

func (s *Service) deleteEventDestination(c *core.Ctx, in *struct{ ConfigurationSetName, EventDestinationName string }) (any, error) {
	return empty, s.changeDests(c, in.ConfigurationSetName, func(cs *ConfigSet) error {
		return removeDest(cs, in.EventDestinationName,
			core.Errorf(400, "EventDestinationDoesNotExist", "Event destination <%s> does not exist.", in.EventDestinationName))
	})
}

// tlsV1 is the v1 spelling of a TLS policy (Require, Optional).
func tlsV1(p string) string {
	if p == "" {
		return ""
	}
	return strings.ToUpper(p[:1]) + strings.ToLower(p[1:])
}

func (s *Service) putDeliveryOptionsV1(c *core.Ctx, in *struct {
	ConfigurationSetName string
	DeliveryOptions      struct{ TlsPolicy string }
}) (any, error) {
	p := strings.ToUpper(in.DeliveryOptions.TlsPolicy)
	if p != "" && p != "REQUIRE" && p != "OPTIONAL" {
		return nil, invalid("TlsPolicy must be Require or Optional.")
	}
	return empty, s.changeDests(c, in.ConfigurationSetName, func(cs *ConfigSet) error {
		if cs.Options == nil {
			cs.Options = map[string]json.RawMessage{}
		}
		cs.Options["DeliveryOptions"], _ = json.Marshal(map[string]string{"TlsPolicy": p})
		return nil
	})
}

func (s *Service) describeConfigurationSet(c *core.Ctx, in *struct{ ConfigurationSetName string }) (any, error) {
	if in.ConfigurationSetName == "" || !s.hasConfigSet(c, in.ConfigurationSetName) {
		return nil, noConfigSet(in.ConfigurationSetName)
	}
	out := map[string]any{"ConfigurationSet": configSet{in.ConfigurationSetName}}
	s.mu.Lock()
	defer s.mu.Unlock()
	if cs := s.st.ConfigDetails[scope(c)+in.ConfigurationSetName]; cs != nil {
		dests := []destV1{}
		for _, d := range cs.Dests {
			dests = append(dests, d.toV1())
		}
		out["EventDestinations"] = dests
		var do struct{ TlsPolicy string }
		if json.Unmarshal(cs.Options["DeliveryOptions"], &do) == nil && do.TlsPolicy != "" {
			out["DeliveryOptions"] = struct{ TlsPolicy string }{tlsV1(do.TlsPolicy)}
		}
	}
	return out, nil
}

// ---- v2 ----

type destV2 struct {
	Name                       string `json:"Name,omitempty"`
	Enabled                    bool
	MatchingEventTypes         []string
	KinesisFirehoseDestination *struct{ IamRoleArn, DeliveryStreamArn string } `json:",omitempty"`
	CloudWatchDestination      *struct {
		DimensionConfigurations []struct{ DimensionName, DimensionValueSource, DefaultDimensionValue string }
	} `json:",omitempty"`
	SnsDestination         *struct{ TopicArn string }       `json:",omitempty"`
	EventBridgeDestination *struct{ EventBusArn string }    `json:",omitempty"`
	PinpointDestination    *struct{ ApplicationArn string } `json:",omitempty"`
}

func (v destV2) toDest(name string) EventDest {
	d := EventDest{Name: name, Enabled: v.Enabled, Types: v.MatchingEventTypes}
	if f := v.KinesisFirehoseDestination; f != nil {
		d.FirehoseRole, d.FirehoseStream = f.IamRoleArn, f.DeliveryStreamArn
	}
	if cw := v.CloudWatchDestination; cw != nil {
		d.CloudWatch = true
		for _, x := range cw.DimensionConfigurations {
			d.Dimensions = append(d.Dimensions, Dimension{x.DimensionName, x.DimensionValueSource, x.DefaultDimensionValue})
		}
	}
	if sn := v.SnsDestination; sn != nil {
		d.SNSTopic = sn.TopicArn
	}
	if eb := v.EventBridgeDestination; eb != nil {
		d.EventBus = eb.EventBusArn
	}
	if p := v.PinpointDestination; p != nil {
		d.Pinpoint = p.ApplicationArn
	}
	return d
}

func (d EventDest) toV2() destV2 {
	v := destV2{Name: d.Name, Enabled: d.Enabled, MatchingEventTypes: d.Types}
	if d.FirehoseStream != "" {
		v.KinesisFirehoseDestination = &struct{ IamRoleArn, DeliveryStreamArn string }{d.FirehoseRole, d.FirehoseStream}
	}
	if d.CloudWatch {
		cw := &struct {
			DimensionConfigurations []struct{ DimensionName, DimensionValueSource, DefaultDimensionValue string }
		}{}
		for _, x := range d.Dimensions {
			cw.DimensionConfigurations = append(cw.DimensionConfigurations, struct{ DimensionName, DimensionValueSource, DefaultDimensionValue string }{x.Name, x.Source, x.Default})
		}
		v.CloudWatchDestination = cw
	}
	if d.SNSTopic != "" {
		v.SnsDestination = &struct{ TopicArn string }{d.SNSTopic}
	}
	if d.EventBus != "" {
		v.EventBridgeDestination = &struct{ EventBusArn string }{d.EventBus}
	}
	if d.Pinpoint != "" {
		v.PinpointDestination = &struct{ ApplicationArn string }{d.Pinpoint}
	}
	return v
}

func badV1(msg string) error { return invalid("%s", msg) }
func badV2(msg string) error { return badRequest("%s", msg) }

func noConfigSetV2(name string) error {
	return notFound("Configuration set <%s> does not exist.", name)
}

// changeDestsV2 is changeDests with the v2 error for a missing set.
func (s *Service) changeDestsV2(c *core.Ctx, set string, f func(cs *ConfigSet) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ConfigSets[scope(c)+set]; !ok {
		return noConfigSetV2(set)
	}
	return f(s.details(c, set))
}

// options are the settings a v2 client can put on a configuration set, with the path of
// the call that replaces each one.
var options = map[string]string{"delivery-options": "DeliveryOptions", "sending": "SendingOptions", "reputation-options": "ReputationOptions",
	"tracking-options": "TrackingOptions", "suppression-options": "SuppressionOptions", "vdm-options": "VdmOptions", "archiving-options": "ArchivingOptions"}

func (s *Service) createConfigurationSetV2(c *core.Ctx, r *http.Request) (any, error) {
	var in map[string]json.RawMessage
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	var name string
	json.Unmarshal(in["ConfigurationSetName"], &name)
	if name == "" {
		return nil, badRequest("ConfigurationSetName is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ConfigSets[scope(c)+name]; ok {
		return nil, core.Errorf(400, "AlreadyExistsException", "Configuration set <%s> already exists.", name)
	}
	s.st.ConfigSets[scope(c)+name] = time.Now().UTC()
	cs := s.details(c, name)
	cs.Options = map[string]json.RawMessage{}
	for _, k := range append([]string{"Tags"}, core.SortedKeys(map[string]string{"DeliveryOptions": "", "SendingOptions": "", "ReputationOptions": "",
		"TrackingOptions": "", "SuppressionOptions": "", "VdmOptions": "", "ArchivingOptions": ""})...) {
		if v, ok := in[k]; ok && string(v) != "null" {
			cs.Options[k] = v
		}
	}
	return map[string]any{}, nil
}

func (s *Service) getConfigurationSetV2(c *core.Ctx, r *http.Request) (any, error) {
	name := r.PathValue("name")
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ConfigSets[scope(c)+name]; !ok {
		return nil, noConfigSetV2(name)
	}
	out := map[string]any{"ConfigurationSetName": name, "SendingOptions": map[string]bool{"SendingEnabled": true},
		"ReputationOptions": map[string]bool{"ReputationMetricsEnabled": false}, "Tags": []tagV2{}}
	if cs := s.st.ConfigDetails[scope(c)+name]; cs != nil {
		for k, v := range cs.Options {
			out[k] = v
		}
	}
	return out, nil
}

func (s *Service) listConfigurationSetsV2(c *core.Ctx, r *http.Request) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := []string{}
	for _, k := range core.SortedKeys(s.st.ConfigSets) {
		if n, ok := strings.CutPrefix(k, scope(c)); ok {
			names = append(names, n)
		}
	}
	return map[string]any{"ConfigurationSets": names}, nil
}

func (s *Service) deleteConfigurationSetV2(c *core.Ctx, r *http.Request) (any, error) {
	name := r.PathValue("name")
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.st.ConfigSets[scope(c)+name]; !ok {
		return nil, noConfigSetV2(name)
	}
	delete(s.st.ConfigSets, scope(c)+name)
	delete(s.st.ConfigDetails, scope(c)+name)
	return map[string]any{}, nil
}

// putOptionV2 replaces one option of a configuration set with the request body.
func (s *Service) putOptionV2(key string) func(*core.Ctx, *http.Request) (any, error) {
	return func(c *core.Ctx, r *http.Request) (any, error) {
		return map[string]any{}, s.changeDestsV2(c, r.PathValue("name"), func(cs *ConfigSet) error {
			if cs.Options == nil {
				cs.Options = map[string]json.RawMessage{}
			}
			cs.Options[key] = json.RawMessage(core.Body(r))
			return nil
		})
	}
}

func (s *Service) createEventDestinationV2(c *core.Ctx, r *http.Request) (any, error) {
	var in struct {
		EventDestinationName string
		EventDestination     destV2
	}
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	return map[string]any{}, s.changeDestsV2(c, r.PathValue("name"), func(cs *ConfigSet) error {
		return addDest(cs, in.EventDestination.toDest(in.EventDestinationName), badV2,
			core.Errorf(400, "AlreadyExistsException", "Event destination <%s> already exists.", in.EventDestinationName))
	})
}

func (s *Service) updateEventDestinationV2(c *core.Ctx, r *http.Request) (any, error) {
	var in struct{ EventDestination destV2 }
	if err := decode(r, &in); err != nil {
		return nil, err
	}
	name := r.PathValue("dest")
	return map[string]any{}, s.changeDestsV2(c, r.PathValue("name"), func(cs *ConfigSet) error {
		return replaceDest(cs, in.EventDestination.toDest(name), badV2, notFound("Event destination <%s> does not exist.", name))
	})
}

func (s *Service) deleteEventDestinationV2(c *core.Ctx, r *http.Request) (any, error) {
	name := r.PathValue("dest")
	return map[string]any{}, s.changeDestsV2(c, r.PathValue("name"), func(cs *ConfigSet) error {
		return removeDest(cs, name, notFound("Event destination <%s> does not exist.", name))
	})
}

func (s *Service) getEventDestinationsV2(c *core.Ctx, r *http.Request) (any, error) {
	out := []destV2{}
	err := s.changeDestsV2(c, r.PathValue("name"), func(cs *ConfigSet) error {
		for _, d := range cs.Dests {
			out = append(out, d.toV2())
		}
		return nil
	})
	return map[string]any{"EventDestinations": out}, err
}

// ---- publishing ----

// simulated tells what the SES mailbox simulator does with a recipient: bounce@, ooto@,
// complaint@, suppressionlist@ and success@ (also with a +label). Everyone else is delivered.
func simulated(addr string) (outcome string) {
	if a, err := mail.ParseAddress(addr); err == nil {
		addr = a.Address
	}
	local, domain, _ := strings.Cut(strings.ToLower(addr), "@")
	if domain != "simulator.amazonses.com" {
		return "Delivery"
	}
	local, _, _ = strings.Cut(local, "+")
	switch local {
	case "bounce", "suppressionlist":
		return "Bounce"
	case "complaint":
		return "Complaint"
	}
	return "Delivery"
}

// sesEvent is one event of the event publishing format, without the mail part.
type sesEvent struct {
	kind string
	body map[string]any
}

// publish sends the events of one message to the destinations of its configuration set:
// Send, then per recipient Delivery, Bounce or Complaint (a complaint follows a delivery).
func (s *Service) publish(c *core.Ctx, e Email, set string) {
	s.mu.Lock()
	var dests []EventDest
	if cs := s.st.ConfigDetails[scope(c)+set]; cs != nil {
		dests = slices.Clone(cs.Dests)
	}
	s.mu.Unlock()
	if len(dests) == 0 || s.hub == nil {
		return
	}
	to := slices.Concat(e.Destination.ToAddresses, e.Destination.CcAddresses, e.Destination.BccAddresses)
	ts := e.Timestamp.Format(core.ISO)
	mailPart := map[string]any{"timestamp": ts, "source": e.Source, "sendingAccountId": c.Account, "messageId": e.Id, "destination": to,
		"headersTruncated": false, "tags": map[string][]string{"ses:configuration-set": {set}, "ses:from-domain": {e.Source[strings.LastIndex(e.Source, "@")+1:]}},
		"commonHeaders": map[string]any{"from": []string{e.Source}, "to": e.Destination.ToAddresses, "messageId": e.Id, "subject": e.Subject}}
	events := []sesEvent{{"Send", map[string]any{"send": map[string]any{}}}}
	for _, r := range to {
		switch simulated(r) {
		case "Bounce":
			events = append(events, sesEvent{"Bounce", map[string]any{"bounce": map[string]any{"bounceType": "Permanent", "bounceSubType": map[bool]string{true: "Suppressed", false: "General"}[strings.HasPrefix(strings.ToLower(r), "suppressionlist")],
				"bouncedRecipients": []map[string]string{{"emailAddress": r, "action": "failed", "status": "5.1.1", "diagnosticCode": "smtp; 550 5.1.1 user unknown"}},
				"timestamp":         ts, "feedbackId": core.NewID(), "reportingMTA": "pogi"}}})
			continue
		case "Complaint":
			events = append(events, sesEvent{"Complaint", map[string]any{"complaint": map[string]any{"complainedRecipients": []map[string]string{{"emailAddress": r}},
				"timestamp": ts, "feedbackId": core.NewID(), "complaintFeedbackType": "abuse"}}})
		}
		events = append(events, sesEvent{"Delivery", map[string]any{"delivery": map[string]any{"timestamp": ts, "processingTimeMillis": 0, "recipients": []string{r},
			"smtpResponse": "250 2.0.0 OK", "reportingMTA": "pogi"}}})
	}
	detailType := map[string]string{"Send": "Email Sent", "Delivery": "Email Delivered", "Bounce": "Email Bounced", "Complaint": "Email Complaint Received"}
	for _, ev := range events {
		ev.body["eventType"], ev.body["mail"] = ev.kind, mailPart
		body, _ := json.Marshal(ev.body)
		for _, d := range dests {
			if !d.Enabled || !slices.Contains(d.Types, strings.ToUpper(ev.kind)) {
				continue
			}
			var err error
			switch {
			case d.SNSTopic != "":
				err = s.hub.Deliver(d.SNSTopic, &core.Message{Body: string(body), Subject: "Amazon SES Email Event Notification"})
			case d.EventBus != "":
				err = s.hub.Deliver(d.EventBus, &core.Message{Body: string(body), Source: "aws.ses", DetailType: detailType[ev.kind]})
			case d.CloudWatch:
				dims := []map[string]string{}
				for _, x := range d.Dimensions {
					dims = append(dims, map[string]string{"Name": x.Name, "Value": x.Default})
				}
				err = s.hub.Call(c.Account, c.Region, "GraniteServiceVersion20100801", "PutMetricData", map[string]any{"Namespace": "AWS/SES",
					"MetricData": []map[string]any{{"MetricName": ev.kind, "Dimensions": dims, "Value": 1, "Unit": "Count"}}}, nil)
			}
			if err != nil {
				s.hub.Log.Warn("ses event failed", "destination", d.Name, "err", err)
			}
		}
	}
}
