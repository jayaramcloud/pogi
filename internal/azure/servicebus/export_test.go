package servicebus

import (
	"time"

	"github.com/nrndev/pogi/internal/amqp"
)

// Manage answers a management request as the broker would hand it to the node of <path>/$management.
func (s *Service) Manage(ns, path string, req *amqp.Message) *amqp.Message {
	return s.manage(ns, path, req)
}

// Eval runs a SQL filter against a message.
func Eval(sql string, m *amqp.Message, params map[string]any) (bool, error) {
	f, err := compileFilter(sql)
	if err != nil {
		return false, err
	}
	return f(&view{m: m, enqueued: time.Now()}, params) == true, nil
}

// Act runs a SQL action on a message.
func Act(sql string, m *amqp.Message, params map[string]any) error {
	a, err := compileAction(sql)
	if err != nil {
		return err
	}
	a.apply(&view{m: m}, params)
	return nil
}

// Encode and Decode run the message codec.
func Encode(m *amqp.Message) ([]byte, error) { return encode(m) }
func Decode(b []byte) (*amqp.Message, error) { return decode(b) }
