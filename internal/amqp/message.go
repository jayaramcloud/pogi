package amqp

import (
	"errors"
	"fmt"
	"time"
)

// Message section descriptors.
const (
	secHeader      = 0x70
	secDeliveryAnn = 0x71
	secMessageAnn  = 0x72
	secProperties  = 0x73
	secAppProps    = 0x74
	secData        = 0x75
	secSequence    = 0x76
	secValue       = 0x77
	secFooter      = 0x78
)

// BatchFormat is the message-format of a Service Bus / Event Hubs batch: one
// data section per inner message, each an encoded message.
const BatchFormat = 0x80013700

func nilStr(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func nilSym(s string) any {
	if s == "" {
		return nil
	}
	return Symbol(s)
}

func nilTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

func nilBytes(b []byte) any {
	if b == nil {
		return nil
	}
	return b
}

// trim drops trailing nil fields from a performative or section list.
func trim(l []any) []any {
	for len(l) > 0 && l[len(l)-1] == nil {
		l = l[:len(l)-1]
	}
	return l
}

// Marshal encodes the bare message (its sections, no transfer framing).
func (m *Message) Marshal() ([]byte, error) {
	var b []byte
	var err error
	add := func(code uint64, v any) {
		if err == nil {
			b, err = appendValue(b, Described{code, v})
		}
	}
	if h := m.Header; h != nil {
		var ttl any
		if h.TTL > 0 {
			ttl = uint32(h.TTL / time.Millisecond)
		}
		var dc any
		if h.DeliveryCount > 0 {
			dc = h.DeliveryCount
		}
		add(secHeader, trim([]any{h.Durable, h.Priority, ttl, h.FirstAcquirer, dc}))
	}
	if len(m.DeliveryAnnotations) > 0 {
		add(secDeliveryAnn, annotations(m.DeliveryAnnotations))
	}
	if len(m.MessageAnnotations) > 0 {
		add(secMessageAnn, annotations(m.MessageAnnotations))
	}
	if p := m.Properties; p != nil {
		var gs any
		if p.GroupSequence > 0 {
			gs = p.GroupSequence
		}
		add(secProperties, trim([]any{p.MessageID, nilBytes(p.UserID), nilStr(p.To), nilStr(p.Subject), nilStr(p.ReplyTo),
			p.CorrelationID, nilSym(p.ContentType), nilSym(p.ContentEncoding), nilTime(p.AbsoluteExpiryTime),
			nilTime(p.CreationTime), nilStr(p.GroupID), gs, nilStr(p.ReplyToGroupID)}))
	}
	if len(m.ApplicationProperties) > 0 {
		add(secAppProps, m.ApplicationProperties)
	}
	switch {
	case len(m.Data) > 0:
		for _, d := range m.Data {
			add(secData, nilBytes(d))
		}
	case len(m.Sequence) > 0:
		for _, s := range m.Sequence {
			add(secSequence, s)
		}
	default: // a message needs a body
		add(secValue, m.Value)
	}
	if len(m.Footer) > 0 {
		add(secFooter, annotations(m.Footer))
	}
	return b, err
}

// Unmarshal decodes a bare message.
func Unmarshal(b []byte) (*Message, error) {
	m := &Message{}
	r := reader{b}
	for len(r.b) > 0 {
		v, err := r.value()
		if err != nil {
			return nil, err
		}
		d, ok := v.(Described)
		if !ok {
			return nil, errors.New("amqp: message section is not a described type")
		}
		code, _ := asUint(d.Descriptor)
		switch code {
		case secHeader:
			l, _ := d.Value.([]any)
			h := &Header{Priority: 4}
			h.Durable, _ = at(l, 0).(bool)
			if p, ok := asUint(at(l, 1)); ok {
				h.Priority = uint8(p)
			}
			if t, ok := asUint(at(l, 2)); ok {
				h.TTL = time.Duration(t) * time.Millisecond
			}
			h.FirstAcquirer, _ = at(l, 3).(bool)
			if c, ok := asUint(at(l, 4)); ok {
				h.DeliveryCount = uint32(c)
			}
			m.Header = h
		case secDeliveryAnn:
			m.DeliveryAnnotations = toMap(d.Value)
		case secMessageAnn:
			m.MessageAnnotations = toMap(d.Value)
		case secProperties:
			l, _ := d.Value.([]any)
			p := &Properties{MessageID: at(l, 0), CorrelationID: at(l, 5)}
			p.UserID, _ = at(l, 1).([]byte)
			p.To = str(at(l, 2))
			p.Subject = str(at(l, 3))
			p.ReplyTo = str(at(l, 4))
			p.ContentType = str(at(l, 6))
			p.ContentEncoding = str(at(l, 7))
			p.AbsoluteExpiryTime, _ = at(l, 8).(time.Time)
			p.CreationTime, _ = at(l, 9).(time.Time)
			p.GroupID = str(at(l, 10))
			if g, ok := asUint(at(l, 11)); ok {
				p.GroupSequence = uint32(g)
			}
			p.ReplyToGroupID = str(at(l, 12))
			m.Properties = p
		case secAppProps:
			m.ApplicationProperties = toMap(d.Value)
		case secData:
			bs, _ := d.Value.([]byte)
			m.Data = append(m.Data, bs)
		case secSequence:
			l, _ := d.Value.([]any)
			m.Sequence = append(m.Sequence, l)
		case secValue:
			m.Value = d.Value
		case secFooter:
			m.Footer = toMap(d.Value)
		default:
			return nil, fmt.Errorf("amqp: unknown message section %v", d.Descriptor)
		}
	}
	return m, nil
}

// toMap turns a decoded map into map[string]any (keys of other types are
// printed); anything else is nil.
func toMap(v any) map[string]any {
	switch x := v.(type) {
	case map[string]any:
		return x
	case map[any]any:
		m := make(map[string]any, len(x))
		for k, v := range x {
			m[keyString(k)] = v
		}
		return m
	}
	return nil
}

func at(l []any, i int) any {
	if i < len(l) {
		return l[i]
	}
	return nil
}

func str(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case Symbol:
		return string(x)
	}
	return ""
}

func asUint(v any) (uint64, bool) {
	switch x := v.(type) {
	case uint8:
		return uint64(x), true
	case uint16:
		return uint64(x), true
	case uint32:
		return uint64(x), true
	case uint64:
		return x, true
	case int8:
		return uint64(x), x >= 0
	case int16:
		return uint64(x), x >= 0
	case int32:
		return uint64(x), x >= 0
	case int64:
		return uint64(x), x >= 0
	}
	return 0, false
}

func u32(v any) uint32 { n, _ := asUint(v); return uint32(n) }

func boolean(v any) bool { b, _ := v.(bool); return b }
