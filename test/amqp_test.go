package test

import (
	"context"
	"strings"
	"testing"
	"time"

	goamqp "github.com/Azure/go-amqp"
	"github.com/nrndev/pogi"
	"github.com/nrndev/pogi/internal/amqp"
)

// pogi.Start opens an AMQP port that serves $cbs; its Conn sees the claim.
func TestStartServesAMQP(t *testing.T) {
	s := pogi.Start(t)
	addr := s.Ports["amqp"]
	if !strings.HasPrefix(addr, "amqp://127.0.0.1:") {
		t.Fatalf("Ports[amqp] = %q", addr)
	}
	conns := make(chan *amqp.Conn, 4)
	s.Hub.AMQP().Add(func(l *amqp.Link) (any, error) {
		if l.Address != "probe" {
			return nil, nil
		}
		return probe(func() { conns <- l.Conn }), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	c, err := goamqp.Dial(ctx, addr, &goamqp.ConnOptions{SASLType: goamqp.SASLTypeAnonymous()})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	sess, err := c.NewSession(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	snd, err := sess.NewSender(ctx, "$cbs", nil)
	if err != nil {
		t.Fatal(err)
	}
	rcv, err := sess.NewReceiver(ctx, "$cbs", &goamqp.ReceiverOptions{TargetAddress: "cbs-reply", Credit: 10,
		SettlementMode: goamqp.ReceiverSettleModeFirst.Ptr(), RequestedSenderSettleMode: goamqp.SenderSettleModeSettled.Ptr()})
	if err != nil {
		t.Fatal(err)
	}
	reply := "cbs-reply"
	err = snd.Send(ctx, &goamqp.Message{Value: "SharedAccessSignature sr=x&sig=y&se=4102444800&skn=k",
		Properties:            &goamqp.MessageProperties{MessageID: "1", ReplyTo: &reply},
		ApplicationProperties: map[string]any{"operation": "put-token", "type": "servicebus.windows.net:sastoken", "name": "sb://127.0.0.1/q"}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	m, err := rcv.Receive(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.ApplicationProperties["status-code"] != int32(202) {
		t.Fatalf("put-token: %v", m.ApplicationProperties)
	}
	p, err := sess.NewSender(ctx, "probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Send(ctx, goamqp.NewMessage([]byte("x")), nil); err != nil {
		t.Fatal(err)
	}
	if conn := <-conns; !conn.Authorized("sb://127.0.0.1/q") {
		t.Fatal("claim from $cbs not recorded on the connection")
	}
}

type probe func()

func (p probe) Accept(ctx context.Context, l *amqp.Link, m *amqp.Message) error { p(); return nil }
