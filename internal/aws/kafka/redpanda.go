package kafka

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"

	"github.com/nrndev/pogi/internal/docker"
)

// Redpanda is a Kafka broker in one Redpanda container (dev mode) with two
// listeners: containers on pogi's network reach it at <Name>:9092, host
// clients at the 127.0.0.1 address Run returns. MSK runs one per cluster;
// GCP Managed Kafka and Event Hubs' Kafka endpoint can run theirs the same way.
type Redpanda struct {
	Name     string // container name, unique to its cluster: Run starts a container of that name as it is
	Service  string // pogi.service label; docker.images["<Service>.redpanda"] overrides the image
	Resource string // pogi.resource label
	Port     int    // host port of a fresh container, kept when free; 0: any
}

// Run starts the broker, the container of that name when there is one, else
// a fresh one, and waits until it answers a metadata request. It returns the
// bootstrap address of host clients, 127.0.0.1:<port>.
func (r *Redpanda) Run(ctx context.Context, d *docker.Client) (string, error) {
	if d.Start(ctx, r.Name) != nil { // none yet, or it cannot start again (its port is taken)
		port := freePort(r.Port) // advertised, so picked before the container starts
		d.Remove(ctx, r.Name)
		ct := &docker.Container{Name: r.Name, Image: "redpandadata/redpanda", Service: r.Service, Resource: r.Resource,
			Ports: map[int]int{19092: port},
			Cmd: []string{"redpanda", "start", "--mode", "dev-container", "--smp", "1", "--memory", "1G",
				"--kafka-addr", "internal://0.0.0.0:9092,external://0.0.0.0:19092",
				"--advertise-kafka-addr", "internal://" + r.Name + ":9092,external://127.0.0.1:" + strconv.Itoa(port)}}
		if _, err := d.Run(ctx, ct); err != nil {
			return "", err
		}
	}
	info, err := d.Inspect(ctx, r.Name)
	if err != nil {
		return "", err
	}
	hostPort := info.HostPort(19092)
	addr := "127.0.0.1:" + strconv.Itoa(hostPort) // what clients on the host use
	for end := time.Now().Add(5 * time.Minute); !answers(d.Addr(hostPort)); {
		if info, err := d.Inspect(ctx, r.Name); err != nil || !info.State.Running || time.Now().After(end) {
			return "", fmt.Errorf("redpanda did not answer on %s (docker logs %s tells why)", addr, r.Name)
		}
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	return addr, nil
}

// answers sends the broker at addr a Kafka Metadata request (v4, no topics)
// and reports whether the reply names a broker. Docker Desktop accepts
// connections before the broker listens, so only a reply counts.
func answers(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return false
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	// size, api key 3, version 4, correlation id 1, client id "pogi", topics [], allow_auto_topic_creation false
	c.Write([]byte{0, 0, 0, 19, 0, 3, 0, 4, 0, 0, 0, 1, 0, 4, 'p', 'o', 'g', 'i', 0, 0, 0, 0, 0})
	var r [16]byte // size, correlation id, throttle time, broker count
	_, err = io.ReadFull(c, r[:])
	return err == nil && binary.BigEndian.Uint32(r[4:]) == 1 && int32(binary.BigEndian.Uint32(r[12:])) > 0
}

// freePort returns want when it is free on 127.0.0.1, else any free port.
func freePort(want int) int {
	ln, err := net.Listen("tcp", "127.0.0.1:"+strconv.Itoa(want))
	if err != nil {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		return 0
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}
