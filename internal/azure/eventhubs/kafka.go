package eventhubs

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	rp "github.com/nrndev/pogi/internal/aws/kafka"
	"github.com/nrndev/pogi/internal/core"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaBootstrap returns the bootstrap address of a namespace's Kafka
// endpoint, starting its Redpanda broker on first use (this takes a while the
// first time: the image is pulled). Topics are named after the event hubs and
// have their partition count and retention; hubs created later get theirs too.
//
// ponytail: the AMQP and Kafka sides are separate logs, with no bridging:
// events sent over AMQP are not in the topic, and the other way round. Plain
// Kafka, no SASL; bridge by consuming one side and sending to the other if needed.
func (s *Service) KafkaBootstrap(ctx context.Context, ns string) (string, error) {
	s.kmu.Lock()
	defer s.kmu.Unlock()
	s.mu.Lock()
	n, ok := s.st.Namespaces[k(ns)]
	if !ok {
		s.mu.Unlock()
		return "", fmt.Errorf("namespace %q: %w", ns, ErrNotFound)
	}
	if addr := s.kafka[k(ns)]; addr != "" {
		s.mu.Unlock()
		return addr, nil
	}
	if n.Container == "" {
		n.Container = "pogi-eh-" + k(ns) + "-" + core.RandHex(4) // kept in the state: a loaded one finds its container again
	}
	container := n.Container
	s.mu.Unlock()

	d := s.h.Docker()
	if !d.Available() {
		return "", errors.New("the Kafka endpoint needs Docker, and no container engine is running")
	}
	addr, err := (&rp.Redpanda{Name: container, Service: "eventhubs", Resource: ns}).Run(ctx, d)
	if err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.st.Namespaces[k(ns)] != n { // deleted or reset meanwhile
		s.mu.Unlock()
		go d.Remove(context.Background(), container)
		return "", fmt.Errorf("namespace %q: %w", ns, ErrNotFound)
	}
	s.kafka[k(ns)] = addr // from now on CreateHub makes its own topic
	var hubs []HubInfo
	for _, h := range n.Hubs {
		hubs = append(hubs, h.info())
	}
	s.mu.Unlock()
	for _, h := range hubs {
		if err := topic(ctx, addr, h.Name, len(h.PartitionIDs), h.Retention, false); err != nil {
			s.h.Log.Warn("eventhubs: kafka topic", "hub", h.Name, "err", err)
		}
	}
	return addr, nil
}

// kafkaTopic creates (or deletes) the topic of a hub when the namespace's broker runs.
func (s *Service) kafkaTopic(ns, hub string, partitions int, retention time.Duration, del bool) {
	s.mu.Lock()
	addr := s.kafka[k(ns)]
	s.mu.Unlock()
	if addr == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := topic(ctx, addr, hub, partitions, retention, del); err != nil {
		s.h.Log.Warn("eventhubs: kafka topic", "hub", hub, "err", err)
	}
}

func topic(ctx context.Context, addr, name string, partitions int, retention time.Duration, del bool) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		return err
	}
	defer cl.Close()
	if del {
		req := kmsg.NewPtrDeleteTopicsRequest()
		t := kmsg.NewDeleteTopicsRequestTopic()
		t.Topic = kmsg.StringPtr(name)
		req.Topics, req.TopicNames = append(req.Topics, t), append(req.TopicNames, name)
		req.TimeoutMillis = 10000
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			return err
		}
		for _, r := range resp.Topics {
			if e := kerr.ErrorForCode(r.ErrorCode); e != nil && e != kerr.UnknownTopicOrPartition {
				return e
			}
		}
		return nil
	}
	req := kmsg.NewPtrCreateTopicsRequest()
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = name, int32(partitions), 1
	c := kmsg.NewCreateTopicsRequestTopicConfig()
	c.Name, c.Value = "retention.ms", kmsg.StringPtr(strconv.FormatInt(retention.Milliseconds(), 10))
	t.Configs = append(t.Configs, c)
	req.Topics = append(req.Topics, t)
	req.TimeoutMillis = 10000
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, r := range resp.Topics {
		if e := kerr.ErrorForCode(r.ErrorCode); e != nil && e != kerr.TopicAlreadyExists {
			return e
		}
	}
	return nil
}

// removeContainers removes Redpanda containers in the background.
func (s *Service) removeContainers(names []string) {
	for _, n := range names {
		if n != "" {
			go s.h.Docker().Remove(context.Background(), n)
		}
	}
}
