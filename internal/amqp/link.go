package amqp

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// inDelivery is a delivery the client is sending, being assembled from frames.
type inDelivery struct {
	id      uint32
	tag     []byte
	format  uint32
	settled bool
	payload []byte
}

// lnk is the broker's end of an attached link.
type lnk struct {
	c      *conn
	s      *session
	pub    *Link
	handle uint32
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{} // closed when the link's goroutine has stopped

	mu         sync.Mutex
	cond       *sync.Cond
	node       any  // what the resolver returned
	closed     bool // torn down
	detachSent bool

	// the client receives: the broker sends
	src           Source
	reply         chan *Message // set on the response link of a request-response node
	credit        uint32
	deliveryCount uint32
	draining      bool
	cancelNext    context.CancelFunc

	// the client sends: the broker receives
	sink      Sink
	mgmt      Handler // set on the request link of a request-response node
	in        chan *inDelivery
	partial   *inDelivery
	rcvCount  uint32 // deliveries received
	processed uint32
	lastFlow  uint32
}

var dynamicNodes atomic.Uint64

func (s *session) onAttach(f []any) *Error {
	c := s.c
	name, handle := str(at(f, 0)), u32(at(f, 1))
	if s.links[handle] != nil {
		return Errorf("amqp:session:handle-in-use", "handle %d is in use", handle)
	}
	clientReceives := boolean(at(f, 2))
	srcL, tgtL := terminusFields(at(f, 5)), terminusFields(at(f, 6))
	pub := &Link{Conn: c.pub, Name: name, Receiving: clientReceives, Properties: toMap(at(f, 13))}
	srcAddr, tgtAddr := str(at(srcL, 0)), str(at(tgtL, 0))
	if srcAddr == "" && boolean(at(srcL, 4)) {
		srcAddr = fmt.Sprintf("$dynamic/%d", dynamicNodes.Add(1))
	}
	if tgtAddr == "" && boolean(at(tgtL, 4)) {
		tgtAddr = fmt.Sprintf("$dynamic/%d", dynamicNodes.Add(1))
	}
	if clientReceives {
		pub.Address, pub.target = srcAddr, tgtAddr
		pub.Filter = unwrapFilter(at(srcL, 7))
		pub.Settled = u32(at(f, 3)) == 1 && at(f, 3) != nil
	} else {
		pub.Address, pub.target = tgtAddr, srcAddr
	}

	node, err := c.resolve(pub)
	var reqHandler management
	var isMgmt bool
	if err == nil {
		if m, ok := node.(management); ok {
			reqHandler, isMgmt = m, true
		} else if clientReceives {
			if _, ok := node.(Source); !ok {
				err = Errorf(NotAllowed, "%s does not give out messages", pub.Address)
			}
		} else if _, ok := node.(Sink); !ok {
			err = Errorf(NotAllowed, "%s does not take messages", pub.Address)
		}
	}
	if err != nil {
		e := asError(err)
		c.log.Debug("amqp attach refused", "link", name, "address", pub.Address, "condition", e.Condition)
		c.send(s.ch, perf(pAttach, name, handle, !clientReceives, at(f, 3), at(f, 4), nil, nil), nil)
		c.send(s.ch, perf(pDetach, handle, true, errorDescribed(e)), nil)
		return nil
	}

	l := &lnk{c: c, s: s, pub: pub, handle: handle, done: make(chan struct{})}
	l.cond = sync.NewCond(&l.mu)
	l.ctx, l.cancel = context.WithCancel(c.ctx)
	l.node = node
	// Close may be called from any goroutine, even while the broker is inside
	// a Resolver: detach in the background so it never waits on a connection.
	pub.close = func(e *Error) { go l.closeWith(e) }
	switch {
	case isMgmt && clientReceives:
		l.reply = make(chan *Message, 64)
		l.src = replySource(l.reply)
	case isMgmt:
		l.mgmt = Handler(reqHandler)
	case clientReceives:
		l.src = node.(Source)
	default:
		l.sink = node.(Sink)
	}

	// the attach reply
	var src, tgt any
	if clientReceives {
		var filter any
		if len(pub.ReplySourceFilter) > 0 {
			filter = wrapFilter(pub.ReplySourceFilter)
		}
		src = perf(dSource, nilStr(srcAddr), nil, nil, nil, nil, nil, nil, filter)
		tgt = perf(dTarget, nilStr(tgtAddr))
	} else {
		src = perf(dSource, nilStr(srcAddr))
		tgt = perf(dTarget, nilStr(tgtAddr))
	}
	var idc, props any
	if clientReceives {
		idc = uint32(0)
	} else {
		l.rcvCount = u32(at(f, 9))
		l.processed, l.lastFlow = l.rcvCount, l.rcvCount
	}
	if len(pub.ReplyProperties) > 0 {
		props = annotations(pub.ReplyProperties)
	}
	if e := c.send(s.ch, perf(pAttach, name, handle, !clientReceives, at(f, 3), at(f, 4), src, tgt, nil, nil, idc, nilZero(pub.ReplyMaxMessageSize), nil, nil, props), nil); e != nil {
		return asErr(e)
	}
	s.links[handle] = l
	if l.reply != nil {
		c.mu.Lock()
		for _, a := range []string{srcAddr, tgtAddr} {
			if a != "" {
				c.replies[a] = l
			}
		}
		c.mu.Unlock()
	}
	c.wg.Add(1)
	if clientReceives {
		c.goSafe("pump", func() { defer c.wg.Done(); l.pump() })
	} else {
		l.in = make(chan *inDelivery, 2*creditWindow)
		l.sendCredit(creditWindow)
		c.goSafe("work", func() { defer c.wg.Done(); l.work() })
	}
	return nil
}

// resolve finds the node of a link: $cbs is built in, the rest is the resolvers'.
func (c *conn) resolve(l *Link) (any, error) {
	if l.Address == "$cbs" {
		return management(c.putToken), nil
	}
	n, err := c.s.Resolve(l)
	if err != nil && l.Receiving && isManagement(l.Address) {
		// the response link of a request-response node: a resolver that only
		// knows the request side still gets it
		l.Receiving = false
		n2, err2 := c.s.Resolve(l)
		l.Receiving = true
		if _, ok := n2.(management); err2 == nil && ok {
			return n2, nil
		}
	}
	return n, err
}

func isManagement(addr string) bool {
	return addr == "$management" || (len(addr) > 12 && addr[len(addr)-12:] == "/$management")
}

func terminusFields(v any) []any {
	d, _ := v.(Described)
	l, _ := d.Value.([]any)
	return l
}

func unwrapFilter(v any) map[string]any {
	m := toMap(v)
	for k, x := range m {
		if d, ok := x.(Described); ok {
			m[k] = d.Value
		}
	}
	return m
}

func wrapFilter(m map[string]any) annotations {
	out := make(annotations, len(m))
	for k, v := range m {
		if _, ok := v.(Described); !ok {
			v = Described{Symbol(k), v}
		}
		out[k] = v
	}
	return out
}

// replySource is the Source of a response link: the replies the broker queued.
type replySource chan *Message

func (r replySource) Next(ctx context.Context, l *Link) (*Message, error) {
	select {
	case m := <-r:
		return m, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (replySource) Settle(*Link, *Message, Outcome) {}

// teardown detaches nothing on the wire; it stops the link's goroutine,
// cancels the ctx its Source and Sink see, and releases what is unsettled.
func (l *lnk) teardown() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	l.cancel()
	l.cond.Broadcast()
	l.c.wg.Add(1) // under l.mu: the connection's shutdown can't miss it
	l.mu.Unlock()
	if l.reply != nil {
		c := l.c
		c.mu.Lock()
		for a, r := range c.replies {
			if r == l {
				delete(c.replies, a)
			}
		}
		c.mu.Unlock()
	}
	l.c.goSafe("detach", func() {
		defer l.c.wg.Done()
		<-l.done
		if l.src != nil {
			s := l.s
			s.mu.Lock()
			type rel struct {
				id uint32
				d  *outDelivery
			}
			var rs []rel
			for id, d := range s.out {
				if d.l == l {
					rs = append(rs, rel{id, d})
					delete(s.out, id)
				}
			}
			s.mu.Unlock()
			sort.Slice(rs, func(i, j int) bool { return rs[i].id < rs[j].id })
			for _, r := range rs {
				l.src.Settle(l.pub, r.d.m, Outcome{State: "released"})
			}
		}
		if d, ok := l.node.(Detacher); ok {
			d.Detached(l.pub)
		}
	})
}

// closeWith detaches the link from the broker side.
func (l *lnk) closeWith(e *Error) {
	l.mu.Lock()
	if l.detachSent || l.closed {
		l.mu.Unlock()
		return
	}
	l.detachSent = true
	l.mu.Unlock()
	var ev any
	if e != nil {
		ev = errorDescribed(e)
	}
	l.c.send(l.s.ch, perf(pDetach, l.handle, true, ev), nil)
	l.teardown()
}

// --- the client receives: the broker sends

func (l *lnk) onFlow(f []any) {
	if l.src == nil { // the sender's flow on a link the broker receives
		return
	}
	l.mu.Lock()
	credit := u32(at(f, 6))
	if dc := at(f, 5); dc != nil {
		credit += u32(dc) - l.deliveryCount
		if int32(credit) < 0 {
			credit = 0 // the receiver's count lags ours
		}
	}
	l.credit = credit
	l.draining = boolean(at(f, 8))
	if l.draining && l.cancelNext != nil {
		l.cancelNext() // wake a Next that is waiting for a message
	}
	echo := boolean(at(f, 9))
	l.cond.Broadcast()
	l.mu.Unlock()
	if echo && !l.draining {
		l.sendFlow(false)
	}
}

// sendFlow tells the client where the broker's sending side stands.
func (l *lnk) sendFlow(drain bool) {
	l.mu.Lock()
	dc, credit := l.deliveryCount, l.credit
	l.mu.Unlock()
	var d any
	if drain {
		d = true
	}
	l.c.send(l.s.ch, perf(pFlow, append(l.s.flowHead(), l.handle, dc, credit, nil, d)...), nil)
}

func (l *lnk) pump() {
	defer close(l.done)
	for {
		l.mu.Lock()
		for !l.closed && l.credit == 0 && !l.draining {
			l.cond.Wait()
		}
		if l.closed {
			l.mu.Unlock()
			return
		}
		if l.credit == 0 { // drain with nothing to send
			l.draining = false
			l.mu.Unlock()
			l.sendFlow(true)
			continue
		}
		var nctx context.Context
		var ncancel context.CancelFunc
		if l.draining {
			nctx, ncancel = context.WithTimeout(l.ctx, drainWait)
		} else {
			nctx, ncancel = context.WithCancel(l.ctx)
			l.cancelNext = ncancel
		}
		l.mu.Unlock()
		m, err := l.src.Next(nctx, l.pub)
		ncancel()
		l.mu.Lock()
		l.cancelNext = nil
		draining := l.draining
		l.mu.Unlock()
		if l.ctx.Err() != nil {
			if m != nil && !l.pub.Settled { // pulled but never sent
				l.src.Settle(l.pub, m, Outcome{State: "released"})
			}
			return
		}
		if m == nil || err != nil {
			switch {
			case draining && errors.Is(err, context.Canceled):
				// the drain request woke Next; ask again with a short timeout
			case draining:
				l.mu.Lock()
				l.deliveryCount += l.credit
				l.credit = 0
				l.draining = false
				l.mu.Unlock()
				l.sendFlow(true)
			case err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded):
				l.closeWith(asError(err))
				return
			case err == nil: // nothing and no error; don't spin
				select {
				case <-time.After(10 * time.Millisecond):
				case <-l.ctx.Done():
				}
			}
			continue
		}
		if !l.deliver(m) {
			return
		}
	}
}

// deliver sends m; false when the connection is gone.
func (l *lnk) deliver(m *Message) bool {
	s := l.s
	payload, err := m.Marshal()
	if err != nil {
		l.c.log.Warn("amqp message not sent", "address", l.pub.Address, "err", err)
		if !l.pub.Settled {
			l.src.Settle(l.pub, m, Outcome{State: "rejected", Error: Errorf(InternalError, "%v", err)})
		}
		return true
	}
	s.sendMu.Lock()
	defer s.sendMu.Unlock()
	s.mu.Lock()
	id := s.nextDelivery
	s.nextDelivery++
	tag := binary.BigEndian.AppendUint32(nil, id)
	m.DeliveryTag = tag
	if !l.pub.Settled {
		s.out[id] = &outDelivery{l, m}
	}
	s.mu.Unlock()
	l.mu.Lock()
	if l.credit > 0 { // ponytail: a flow that took credit away while Next waited doesn't hold the message back
		l.credit--
	}
	l.deliveryCount++
	l.mu.Unlock()
	if err := s.sendTransfer(l, id, tag, m.Format, l.pub.Settled, payload); err != nil {
		return false // the connection is closing; unsettled deliveries are released
	}
	return true
}

// --- the client sends: the broker receives

func (s *session) onTransfer(f []any, payload []byte) *Error {
	s.mu.Lock()
	s.nextIn++
	s.mu.Unlock()
	l := s.links[u32(at(f, 0))]
	if l == nil {
		return Errorf("amqp:session:unattached-handle", "transfer for unknown handle %d", u32(at(f, 0)))
	}
	l.mu.Lock()
	skip := l.closed || l.detachSent // detaching: the client's in-flight transfers are dropped
	l.mu.Unlock()
	if skip {
		return nil
	}
	if l.sink == nil && l.mgmt == nil {
		l.closeWith(Errorf(NotAllowed, "transfer on a link the client receives on"))
		return nil
	}
	d := l.partial
	if d == nil {
		d = &inDelivery{id: u32(at(f, 1)), format: u32(at(f, 3))}
		d.tag, _ = at(f, 2).([]byte)
		l.partial = d
	}
	d.settled = d.settled || boolean(at(f, 4))
	if len(d.payload)+len(payload) > maxMessage {
		l.partial = nil
		l.closeWith(Errorf(MessageSizeExceed, "message larger than %d bytes", maxMessage))
		return nil
	}
	d.payload = append(d.payload, payload...)
	if boolean(at(f, 5)) && !boolean(at(f, 7)) { // more frames follow
		return nil
	}
	l.partial = nil
	l.mu.Lock()
	l.rcvCount++
	l.mu.Unlock()
	if boolean(at(f, 7)) { // aborted
		l.mu.Lock()
		l.processed++
		l.mu.Unlock()
		return nil
	}
	select {
	case l.in <- d:
	default:
		l.closeWith(Errorf("amqp:link:transfer-limit-exceeded", "more than %d deliveries beyond the credit", 2*creditWindow))
	}
	return nil
}

// sendCredit gives the client credit for n more deliveries; 0 just reports the state.
func (l *lnk) sendCredit(n uint32) {
	l.mu.Lock()
	dc := l.rcvCount
	l.mu.Unlock()
	l.c.send(l.s.ch, perf(pFlow, append(l.s.flowHead(), l.handle, dc, n)...), nil)
}

func (l *lnk) work() {
	defer close(l.done)
	for {
		select {
		case <-l.ctx.Done():
			return
		case d := <-l.in:
			l.process(d)
		}
	}
}

func (l *lnk) process(d *inDelivery) {
	var err error
	if l.mgmt != nil {
		err = l.serveRequest(d)
	} else {
		err = l.accept(d)
	}
	if l.ctx.Err() == nil && !d.settled {
		st := perf(dAccepted)
		if err != nil {
			st = stateFor(Outcome{State: "rejected", Error: asError(err)})
		}
		l.c.send(l.s.ch, perf(pDisposition, true, d.id, nil, true, st), nil)
	}
	l.mu.Lock()
	l.processed++
	var credit uint32
	renew := l.processed-l.lastFlow >= creditWindow/2
	if renew {
		l.lastFlow = l.processed
		credit = creditWindow - (l.rcvCount - l.processed)
	}
	l.mu.Unlock()
	if renew {
		l.sendCredit(credit)
	}
}

func (l *lnk) accept(d *inDelivery) error {
	m, err := Unmarshal(d.payload)
	if err != nil {
		return Errorf("amqp:decode-error", "%v", err)
	}
	m.Format, m.DeliveryTag = d.format, d.tag
	if d.format != BatchFormat || len(m.Data) == 0 {
		return l.sink.Accept(l.ctx, l.pub, m)
	}
	// A batch: every data section is an encoded message. All of them or none
	// are accepted from the client's side: one rejection rejects the delivery.
	// ponytail: messages accepted before a rejected one stay accepted
	inner := make([]*Message, len(m.Data))
	for i, data := range m.Data {
		im, err := Unmarshal(data)
		if err != nil {
			return Errorf("amqp:decode-error", "batch message %d: %v", i, err)
		}
		im.DeliveryTag = d.tag
		for k, v := range m.MessageAnnotations { // the envelope's annotations (x-opt-partition-key) apply to all
			if im.MessageAnnotations == nil {
				im.MessageAnnotations = map[string]any{}
			}
			if _, ok := im.MessageAnnotations[k]; !ok {
				im.MessageAnnotations[k] = v
			}
		}
		inner[i] = im
	}
	for _, im := range inner {
		if err := l.sink.Accept(l.ctx, l.pub, im); err != nil {
			return err
		}
	}
	return nil
}

// nilZero leaves an optional ulong field out of the frame when it is 0.
func nilZero(n uint64) any {
	if n == 0 {
		return nil
	}
	return n
}
