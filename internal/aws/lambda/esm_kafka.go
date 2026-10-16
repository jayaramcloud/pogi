package lambda

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// Kafka event source mappings, self-managed or Amazon MSK: pogi joins a consumer group on the brokers (pogi's MSK
// Redpanda, or any reachable broker) and invokes the function with batches of records, as Lambda does.
// A failed batch is tried again until it succeeds, which holds the partitions back, as on AWS.
//
// An MSK mapping (EventSourceArn is a cluster ARN) reads from the broker pogi's MSK service runs for that
// cluster; the event then says eventSource "aws:kafka" and carries the cluster ARN.
//
// ponytail: plain connections or SASL (BASIC_AUTH, SASL_SCRAM_256_AUTH, SASL_SCRAM_512_AUTH); the TLS
// settings of SourceAccessConfigurations are not used.

// accessConfig is one SourceAccessConfiguration.
type accessConfig struct {
	Type string `json:"type"`
	URI  string `json:"uri"`
}

// kafka reports whether the mapping reads from Kafka (self-managed or MSK).
func (m *Mapping) kafka() bool { return len(m.Topics) > 0 }

// brokers returns the bootstrap servers of a Kafka mapping. For MSK, pogi's MSK service
// records where the cluster's broker listens once it is running.
func (p *pollers) brokers(m *Mapping) ([]string, error) {
	if m.SourceArn == "" {
		return m.Endpoints, nil
	}
	addr, ok := p.s.hub.Endpoint("kafka-host:" + m.SourceArn)
	if !ok {
		return nil, fmt.Errorf("the MSK cluster %s has no running broker (is it ACTIVE, and is Docker available?)", m.SourceArn)
	}
	return []string{addr}, nil
}

// kafkaConsumer is the consumer of one mapping and the batch it works on.
type kafkaConsumer struct {
	cl      *kgo.Client
	pending []*kgo.Record
	first   time.Time // when the first record of pending arrived
}

// saslFor builds the SASL login from the Secrets Manager secret a SourceAccessConfiguration names.
func (p *pollers) saslFor(m *Mapping) (sasl.Mechanism, error) {
	for _, a := range m.Access {
		if a.Type != "BASIC_AUTH" && a.Type != "SASL_SCRAM_256_AUTH" && a.Type != "SASL_SCRAM_512_AUTH" {
			continue
		}
		_, region, account, _, _ := core.ParseARN(a.URI)
		var out struct{ SecretString string }
		if err := p.s.hub.Call(account, region, "secretsmanager", "GetSecretValue", map[string]string{"SecretId": a.URI}, &out); err != nil {
			return nil, fmt.Errorf("the secret %s: %w", a.URI, err)
		}
		var cred struct{ Username, Password string }
		if err := json.Unmarshal([]byte(out.SecretString), &cred); err != nil || cred.Username == "" {
			return nil, fmt.Errorf("the secret %s needs username and password", a.URI)
		}
		switch a.Type {
		case "BASIC_AUTH":
			return plain.Auth{User: cred.Username, Pass: cred.Password}.AsMechanism(), nil
		case "SASL_SCRAM_256_AUTH":
			return scram.Auth{User: cred.Username, Pass: cred.Password}.AsSha256Mechanism(), nil
		}
		return scram.Auth{User: cred.Username, Pass: cred.Password}.AsSha512Mechanism(), nil
	}
	return nil, nil
}

// consumer returns the mapping's consumer, made on first use. It joins the group when it first polls.
func (p *pollers) consumer(m *Mapping) (*kafkaConsumer, error) {
	p.mu.Lock()
	k := p.kafka[m.UUID]
	p.mu.Unlock()
	if k != nil {
		return k, nil
	}
	start := kgo.NewOffset().AtEnd() // LATEST
	switch m.StartingPosition {
	case "TRIM_HORIZON":
		start = kgo.NewOffset().AtStart()
	case "AT_TIMESTAMP":
		start = kgo.NewOffset().AfterMilli(int64(m.StartingTimestamp * 1000))
	}
	brokers, err := p.brokers(m)
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...), kgo.ConsumerGroup(m.ConsumerGroup), kgo.ConsumeTopics(m.Topics...), kgo.ConsumeResetOffset(start),
		kgo.DisableAutoCommit(), kgo.ClientID("pogi-lambda-" + m.UUID)}
	mech, err := p.saslFor(m)
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}
	k = &kafkaConsumer{cl: cl}
	p.mu.Lock()
	p.kafka[m.UUID] = k
	p.mu.Unlock()
	return k, nil
}

// closeKafka leaves the consumer group of a mapping that is deleted or disabled.
func (p *pollers) closeKafka(uuid string) {
	p.mu.Lock()
	k := p.kafka[uuid]
	delete(p.kafka, uuid)
	p.mu.Unlock()
	if k != nil {
		k.cl.Close()
	}
}

// kafkaRecord is a record as Lambda shows it to a self-managed Kafka function.
func kafkaRecord(r *kgo.Record) map[string]any {
	headers := []map[string][]int{}
	for _, h := range r.Headers {
		bytes := make([]int, len(h.Value))
		for i, b := range h.Value {
			bytes[i] = int(b)
		}
		headers = append(headers, map[string][]int{h.Key: bytes})
	}
	typ := "CREATE_TIME"
	if r.Attrs.TimestampType() == 1 {
		typ = "LOG_APPEND_TIME"
	}
	rec := map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "timestamp": r.Timestamp.UnixMilli(), "timestampType": typ,
		"value": base64.StdEncoding.EncodeToString(r.Value), "headers": headers}
	if r.Key != nil {
		rec["key"] = base64.StdEncoding.EncodeToString(r.Key)
	}
	return rec
}

// pollKafka runs one cycle for a Kafka mapping and returns its LastProcessingResult ("" if idle).
func (p *pollers) pollKafka(m *Mapping) string {
	k, err := p.consumer(m)
	if err != nil {
		return "PROBLEM: " + err.Error()
	}
	size := min(max(m.BatchSize, 1), 10000)
	if len(k.pending) == 0 || len(k.pending) < size && time.Since(k.first) < time.Duration(m.Window)*time.Second {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		fs := k.cl.PollRecords(ctx, size-len(k.pending))
		cancel()
		for _, e := range fs.Errors() {
			if !errors.Is(e.Err, context.DeadlineExceeded) && !errors.Is(e.Err, context.Canceled) {
				return "PROBLEM: " + e.Err.Error()
			}
		}
		if len(k.pending) == 0 {
			k.first = time.Now()
		}
		k.pending = append(k.pending, fs.Records()...)
	}
	if len(k.pending) == 0 || len(k.pending) < size && time.Since(k.first) < time.Duration(m.Window)*time.Second {
		return ""
	}
	batch := k.pending
	byPartition := map[string][]map[string]any{}
	for _, r := range batch {
		rec := kafkaRecord(r)
		view := map[string]any{"topic": r.Topic, "partition": r.Partition, "offset": r.Offset, "timestamp": r.Timestamp.UnixMilli(), "value": string(r.Value)}
		if r.Key != nil {
			view["key"] = string(r.Key)
		}
		if keep(m, view, "value") { // records the filter drops are committed with the batch
			id := r.Topic + "-" + strconv.Itoa(int(r.Partition))
			byPartition[id] = append(byPartition[id], rec)
		}
	}
	if len(byPartition) > 0 {
		res, err := p.invokeKafka(m, byPartition)
		if err != nil || res.errType != "" {
			return "PROBLEM: Function call failed" // the same batch runs again on the next tick
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := k.cl.CommitRecords(ctx, batch...); err != nil {
		return "PROBLEM: " + err.Error()
	}
	k.pending = nil
	return "OK"
}

func (p *pollers) invokeKafka(m *Mapping, records map[string][]map[string]any) (outcome, error) {
	t, err := p.s.target(ctxFor(m.FunctionArn), m.FunctionArn, "")
	if err != nil {
		return outcome{}, err
	}
	servers, _ := p.brokers(m)
	event := map[string]any{"eventSource": "SelfManagedKafka", "bootstrapServers": strings.Join(servers, ","), "records": records}
	if m.SourceArn != "" {
		event["eventSource"], event["eventSourceArn"] = "aws:kafka", m.SourceArn
	}
	payload, _ := json.Marshal(event)
	return p.s.runQueued(t, payload, origin{from: "kafka:" + m.Topics[0], span: true}, func() bool {
		p.s.mu.Lock()
		defer p.s.mu.Unlock()
		cur := p.s.st.Mappings[m.UUID]
		return cur != nil && cur.Enabled
	})
}
