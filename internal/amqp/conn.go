package amqp

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	frameAMQP = 0
	frameSASL = 1

	ourMaxFrame   = 65536 // advertised max-frame-size, like Service Bus
	minMaxFrame   = 512
	windowMax     = 0x7fffffff // session windows are never the limit; ponytail: no session-level flow control
	creditWindow  = 256        // credit the broker gives a client sending on a link
	maxMessage    = 256 << 20  // largest message the broker assembles; ponytail: fixed cap
	drainWait     = 50 * time.Millisecond
	defaultIdle   = 4 * time.Minute
	handshakeWait = 30 * time.Second
)

// Performative descriptors.
const (
	pOpen        = 0x10
	pBegin       = 0x11
	pAttach      = 0x12
	pFlow        = 0x13
	pTransfer    = 0x14
	pDisposition = 0x15
	pDetach      = 0x16
	pEnd         = 0x17
	pClose       = 0x18
	pError       = 0x1d

	pSASLMechanisms = 0x40
	pSASLInit       = 0x41
	pSASLOutcome    = 0x44

	dReceived = 0x23
	dAccepted = 0x24
	dRejected = 0x25
	dReleased = 0x26
	dModified = 0x27

	dSource = 0x28
	dTarget = 0x29
)

var perfNames = map[uint64]string{pOpen: "open", pBegin: "begin", pAttach: "attach", pFlow: "flow", pTransfer: "transfer",
	pDisposition: "disposition", pDetach: "detach", pEnd: "end", pClose: "close",
	pSASLMechanisms: "sasl-mechanisms", pSASLInit: "sasl-init", pSASLOutcome: "sasl-outcome"}

func perf(code uint64, fields ...any) Described { return Described{code, trim(fields)} }

func errorDescribed(e *Error) Described {
	var info any
	if len(e.Info) > 0 {
		info = annotations(e.Info)
	}
	return perf(pError, Symbol(e.Condition), nilStr(e.Description), info)
}

func parseError(v any) *Error {
	d, ok := v.(Described)
	if !ok {
		return nil
	}
	l, _ := d.Value.([]any)
	return &Error{Condition: str(at(l, 0)), Description: str(at(l, 1)), Info: toMap(at(l, 2))}
}

func asError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		return e
	}
	return Errorf(InternalError, "%v", err)
}

// Serve accepts AMQP connections on ln until it is closed.
func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return net.ErrClosed
	}
	if s.lns == nil {
		s.lns = map[net.Listener]struct{}{}
		s.conns = map[*conn]struct{}{}
	}
	s.lns[ln] = struct{}{}
	s.mu.Unlock()
	for {
		nc, err := ln.Accept()
		if err != nil {
			s.mu.Lock()
			delete(s.lns, ln)
			s.mu.Unlock()
			return err
		}
		go s.serveConn(nc)
	}
}

// Close stops the listeners given to Serve and drops every connection.
func (s *Server) Close() {
	s.mu.Lock()
	s.closed = true
	for ln := range s.lns {
		ln.Close()
	}
	for c := range s.conns {
		c.nc.Close()
	}
	s.mu.Unlock()
}

type conn struct {
	s   *Server
	nc  net.Conn
	br  *bufio.Reader
	pub *Conn
	log *slog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup // per-link cleanups

	wmu       sync.Mutex
	lastWrite atomic.Int64 // unix nanos

	maxFrame uint32        // largest frame either side takes
	idle     time.Duration // our idle-time-out: close a peer silent for this long
	peerIdle time.Duration // the peer's: send it a frame at least every half

	sessions map[uint16]*session // read loop only

	mu      sync.Mutex
	replies map[string]*lnk // response links of request-response nodes, by source and target address
}

// recoverLog is deferred at the top of a goroutine: it logs a panic with the
// stack and swallows it, so one bad connection cannot take pogi down.
// (This package cannot use core.Go: core imports amqp.)
func recoverLog(log *slog.Logger, name string) {
	if r := recover(); r != nil {
		log.Error("panic in amqp goroutine", "goroutine", name, "panic", r, "stack", string(debug.Stack()))
	}
}

// goSafe runs f in a new goroutine with panics logged.
func (c *conn) goSafe(name string, f func()) {
	go func() {
		defer recoverLog(c.log, name)
		f()
	}()
}

func (s *Server) serveConn(nc net.Conn) {
	defer recoverLog(s.opt.Log, "serveConn")
	ctx, cancel := context.WithCancel(context.Background())
	c := &conn{s: s, nc: nc, br: bufio.NewReaderSize(nc, 64<<10), pub: &Conn{}, ctx: ctx, cancel: cancel,
		log: s.opt.Log.With("remote", nc.RemoteAddr().String()), sessions: map[uint16]*session{}, replies: map[string]*lnk{},
		idle: cmpOr(s.opt.IdleTimeout, defaultIdle)}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		nc.Close()
		return
	}
	s.conns[c] = struct{}{}
	s.mu.Unlock()
	defer func() {
		cancel()
		nc.Close()
		for _, ss := range c.sessions {
			ss.shutdown()
		}
		c.wg.Wait()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
	}()
	if err := c.handshake(); err != nil {
		c.log.Debug("amqp handshake failed", "err", err)
		return
	}
	c.nc.SetDeadline(time.Time{})
	c.goSafe("heartbeat", c.heartbeat)
	c.loop()
}

func cmpOr(a, b time.Duration) time.Duration {
	if a != 0 {
		return a
	}
	return b
}

func (c *conn) debug(dir, typ string, ch uint16) {
	if c.log.Enabled(context.Background(), slog.LevelDebug) {
		c.log.Debug("amqp frame", "dir", dir, "type", typ, "channel", ch)
	}
}

// --- frames

func header(proto byte) []byte { return []byte{'A', 'M', 'Q', 'P', proto, 1, 0, 0} }

func (c *conn) write(typ byte, ch uint16, body []byte) error {
	buf := make([]byte, 8, 8+len(body))
	binary.BigEndian.PutUint32(buf, uint32(8+len(body)))
	buf[4], buf[5] = 2, typ
	binary.BigEndian.PutUint16(buf[6:], ch)
	buf = append(buf, body...)
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.nc.Write(buf)
	c.lastWrite.Store(time.Now().UnixNano())
	return err
}

func (c *conn) send(ch uint16, p Described, payload []byte) error {
	body, err := appendValue(nil, p)
	if err != nil {
		return err
	}
	code, _ := asUint(p.Descriptor)
	c.debug("->", perfNames[code], ch)
	return c.write(frameAMQP, ch, append(body, payload...))
}

func (c *conn) readFrame() (typ byte, ch uint16, body []byte, err error) {
	var h [8]byte
	if _, err = io.ReadFull(c.br, h[:]); err != nil {
		return
	}
	size, doff := binary.BigEndian.Uint32(h[:]), int(h[4])*4
	limit := uint32(ourMaxFrame)
	if c.maxFrame == 0 { // before open: the minimum every peer must take
		limit = minMaxFrame * 8
	}
	if size < 8 || size > limit || doff < 8 || doff > int(size) {
		return 0, 0, nil, fmt.Errorf("bad frame size %d doff %d", size, doff)
	}
	buf := make([]byte, size-8)
	if _, err = io.ReadFull(c.br, buf); err != nil {
		return
	}
	return h[5], binary.BigEndian.Uint16(h[6:]), buf[doff-8:], nil
}

func decodePerf(body []byte) (Described, []byte, error) {
	v, rest, err := Decode(body)
	if err != nil {
		return Described{}, nil, err
	}
	d, ok := v.(Described)
	if !ok {
		return Described{}, nil, errors.New("frame body is not a performative")
	}
	return d, rest, nil
}

// --- connection setup

func (c *conn) handshake() error {
	c.nc.SetDeadline(time.Now().Add(handshakeWait))
	var h [8]byte
	if _, err := io.ReadFull(c.br, h[:]); err != nil {
		return err
	}
	if string(h[:4]) != "AMQP" || h[5] != 1 || h[6] != 0 || h[7] != 0 || (h[4] != 0 && h[4] != 3) {
		c.nc.Write(header(0))
		return fmt.Errorf("unsupported protocol header % x", h)
	}
	if h[4] == 3 {
		if err := c.sasl(); err != nil {
			return err
		}
		if _, err := io.ReadFull(c.br, h[:]); err != nil {
			return err
		}
		if string(h[:4]) != "AMQP" || h[4] != 0 {
			return fmt.Errorf("expected AMQP header after SASL, got % x", h)
		}
	} else if c.s.opt.Login != nil { // credentials are required
		c.nc.Write(header(3))
		return errors.New("SASL required")
	}
	if _, err := c.nc.Write(header(0)); err != nil {
		return err
	}
	// open
	for {
		typ, ch, body, err := c.readFrame()
		if err != nil {
			return err
		}
		if len(body) == 0 {
			continue
		}
		d, _, err := decodePerf(body)
		if err != nil {
			return err
		}
		if code, _ := asUint(d.Descriptor); typ != frameAMQP || ch != 0 || code != pOpen {
			return fmt.Errorf("expected open, got frame type %d performative %v", typ, d.Descriptor)
		}
		c.debug("<-", "open", ch)
		f, _ := d.Value.([]any)
		c.pub.ContainerID, c.pub.Host = str(at(f, 0)), str(at(f, 1))
		peerMax := uint32(ourMaxFrame)
		if m, ok := asUint(at(f, 2)); ok && m < uint64(peerMax) {
			peerMax = max(uint32(m), minMaxFrame)
		}
		c.maxFrame = peerMax
		if t, ok := asUint(at(f, 4)); ok {
			c.peerIdle = time.Duration(t) * time.Millisecond
		}
		return c.send(0, perf(pOpen, "pogi", nil, uint32(ourMaxFrame), uint16(65535), uint32(c.idle/time.Millisecond)), nil)
	}
}

func (c *conn) sasl() error {
	if _, err := c.nc.Write(header(3)); err != nil {
		return err
	}
	body, _ := appendValue(nil, perf(pSASLMechanisms, []Symbol{"ANONYMOUS", "PLAIN"}))
	if err := c.write(frameSASL, 0, body); err != nil {
		return err
	}
	typ, _, fb, err := c.readFrame()
	if err != nil {
		return err
	}
	d, _, err := decodePerf(fb)
	if err != nil {
		return err
	}
	if code, _ := asUint(d.Descriptor); typ != frameSASL || code != pSASLInit {
		return errors.New("expected sasl-init")
	}
	f, _ := d.Value.([]any)
	resp, _ := at(f, 1).([]byte)
	ok := false
	switch str(at(f, 0)) {
	case "ANONYMOUS":
		ok = true
	case "PLAIN": // [authzid] NUL authcid NUL passwd
		parts := splitNUL(resp)
		if len(parts) == 3 {
			ok = c.s.opt.Login == nil || c.s.opt.Login(parts[1], parts[2])
			if ok {
				c.pub.User = parts[1]
			}
		}
	}
	code := byte(0)
	if !ok {
		code = 1
	}
	body, _ = appendValue(nil, perf(pSASLOutcome, code))
	if err := c.write(frameSASL, 0, body); err != nil {
		return err
	}
	if !ok {
		return errors.New("SASL authentication failed")
	}
	return nil
}

func splitNUL(b []byte) []string {
	var out []string
	start := 0
	for i, x := range b {
		if x == 0 {
			out = append(out, string(b[start:i]))
			start = i + 1
		}
	}
	return append(out, string(b[start:]))
}

// heartbeat sends an empty frame whenever nothing was written for half the
// peer's idle timeout.
func (c *conn) heartbeat() {
	if c.peerIdle <= 0 {
		return
	}
	every := max(c.peerIdle/4, 5*time.Millisecond)
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-t.C:
			if time.Since(time.Unix(0, c.lastWrite.Load())) >= c.peerIdle/2 {
				c.debug("->", "empty", 0)
				if c.write(frameAMQP, 0, nil) != nil {
					return
				}
			}
		}
	}
}

// --- main loop

func (c *conn) loop() {
	for {
		c.nc.SetReadDeadline(time.Now().Add(c.idle))
		typ, ch, body, err := c.readFrame()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				c.closeWith(Errorf("amqp:connection:forced", "idle timeout"))
			} else if err != io.EOF && c.ctx.Err() == nil {
				c.log.Debug("amqp read", "err", err)
			}
			return
		}
		if len(body) == 0 {
			c.debug("<-", "empty", ch)
			continue
		}
		d, payload, err := decodePerf(body)
		if err != nil {
			c.closeWith(Errorf("amqp:connection:framing-error", "%v", err))
			return
		}
		if typ != frameAMQP {
			c.closeWith(Errorf(NotAllowed, "unexpected SASL frame"))
			return
		}
		done, e := c.dispatch(ch, d, payload)
		if e != nil {
			c.closeWith(e)
			return
		}
		if done {
			return
		}
	}
}

func (c *conn) closeWith(e *Error) {
	c.log.Debug("amqp closing connection", "condition", e.Condition, "description", e.Description)
	var ev any
	if e != nil {
		ev = errorDescribed(e)
	}
	c.send(0, perf(pClose, ev), nil)
}

func (c *conn) dispatch(ch uint16, d Described, payload []byte) (bool, *Error) {
	code, _ := asUint(d.Descriptor)
	f, _ := d.Value.([]any)
	c.debug("<-", perfNames[code], ch)
	switch code {
	case pClose:
		if e := parseError(at(f, 0)); e != nil {
			c.log.Debug("amqp peer closed with error", "condition", e.Condition, "description", e.Description)
		}
		c.send(0, perf(pClose), nil)
		return true, nil
	case pBegin:
		if c.sessions[ch] != nil {
			return false, Errorf(NotAllowed, "channel %d already has a session", ch)
		}
		if at(f, 0) != nil {
			return false, Errorf(NotAllowed, "begin replies to a session the broker never began")
		}
		s := newSession(c, ch, u32(at(f, 1)), u32(at(f, 2)))
		c.sessions[ch] = s
		return false, asErr(c.send(ch, perf(pBegin, ch, uint32(0), uint32(windowMax), uint32(windowMax), uint32(65535)), nil))
	}
	s := c.sessions[ch]
	if s == nil {
		return false, Errorf("amqp:connection:framing-error", "frame on channel %d without a session", ch)
	}
	switch code {
	case pAttach:
		return false, s.onAttach(f)
	case pFlow:
		return false, s.onFlow(f)
	case pTransfer:
		return false, s.onTransfer(f, payload)
	case pDisposition:
		s.onDisposition(f)
	case pDetach:
		s.onDetach(f)
	case pEnd:
		s.shutdown()
		delete(c.sessions, ch)
		c.send(ch, perf(pEnd), nil)
	default:
		c.log.Debug("amqp unexpected performative", "descriptor", d.Descriptor)
	}
	return false, nil
}

func asErr(err error) *Error {
	if err == nil {
		return nil
	}
	return Errorf(InternalError, "%v", err)
}

// --- sessions

type outDelivery struct {
	l *lnk
	m *Message
}

type session struct {
	c  *conn
	ch uint16

	mu           sync.Mutex
	cond         *sync.Cond
	nextIn       uint32 // next-incoming-id: transfer frames received
	nextOut      uint32 // next-outgoing-id: transfer frames sent
	remoteWin    uint32 // frames the peer still accepts
	nextDelivery uint32
	out          map[uint32]*outDelivery // unsettled deliveries sent, by delivery-id
	dead         bool

	sendMu sync.Mutex // one delivery at a time on the wire
	links  map[uint32]*lnk
}

func newSession(c *conn, ch uint16, nextOut, inWindow uint32) *session {
	s := &session{c: c, ch: ch, nextIn: nextOut, remoteWin: inWindow, out: map[uint32]*outDelivery{}, links: map[uint32]*lnk{}}
	s.cond = sync.NewCond(&s.mu)
	return s
}

func (s *session) shutdown() {
	s.mu.Lock()
	s.dead = true
	s.cond.Broadcast()
	s.mu.Unlock()
	for _, l := range s.links {
		l.teardown()
	}
	s.links = map[uint32]*lnk{}
}

func (s *session) flowHead() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	return []any{s.nextIn, uint32(windowMax), s.nextOut, uint32(windowMax)}
}

func (s *session) onFlow(f []any) *Error {
	s.mu.Lock()
	var niid uint32
	if v := at(f, 0); v != nil {
		niid = u32(v)
	}
	s.remoteWin = niid + u32(at(f, 1)) - s.nextOut
	s.cond.Broadcast()
	s.mu.Unlock()
	if h := at(f, 4); h != nil {
		l := s.links[u32(h)]
		if l == nil {
			return Errorf("amqp:session:unattached-handle", "flow for unknown handle %d", u32(h))
		}
		l.onFlow(f)
	} else if boolean(at(f, 9)) { // echo
		s.c.send(s.ch, perf(pFlow, s.flowHead()...), nil)
	}
	return nil
}

func (s *session) onDisposition(f []any) {
	if !boolean(at(f, 0)) { // from a sender: settles deliveries the broker received, which it already settled
		return
	}
	first := u32(at(f, 1))
	last := first
	if v := at(f, 2); v != nil {
		last = u32(v)
	}
	settled := boolean(at(f, 3))
	o, terminal := parseOutcome(at(f, 4), settled)
	if !terminal {
		return
	}
	s.mu.Lock()
	var ids []uint32
	for id := range s.out {
		if id-first <= last-first {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i]-first < ids[j]-first })
	ds := make([]*outDelivery, len(ids))
	for i, id := range ids {
		ds[i] = s.out[id]
		delete(s.out, id)
	}
	s.mu.Unlock()
	confirm := at(f, 4)
	if o.State == "rejected" { // go-amqp reads a rejected confirmation as the broker refusing the settle
		confirm = perf(dAccepted)
	}
	for i, d := range ds {
		d.l.src.Settle(d.l.pub, d.m, o)
		if !settled { // receive settle mode second: confirm
			s.c.send(s.ch, perf(pDisposition, false, ids[i], nil, true, confirm), nil)
		}
	}
}

// parseOutcome reads a delivery state; terminal is false for received/absent
// states that don't settle anything yet.
func parseOutcome(state any, settled bool) (Outcome, bool) {
	d, ok := state.(Described)
	if !ok {
		return Outcome{State: "released"}, settled // settled without an outcome: let the message go back
	}
	f, _ := d.Value.([]any)
	code, _ := asUint(d.Descriptor)
	switch code {
	case dAccepted:
		return Outcome{State: "accepted"}, true
	case dRejected:
		return Outcome{State: "rejected", Error: parseError(at(f, 0))}, true
	case dReleased:
		return Outcome{State: "released"}, true
	case dModified:
		return Outcome{State: "modified", DeliveryFailed: boolean(at(f, 0)), UndeliverableHere: boolean(at(f, 1)), Annotations: toMap(at(f, 2))}, true
	}
	return Outcome{}, false
}

func stateFor(o Outcome) Described {
	switch o.State {
	case "rejected":
		if o.Error != nil {
			return perf(dRejected, errorDescribed(o.Error))
		}
		return perf(dRejected)
	case "released":
		return perf(dReleased)
	}
	return perf(dAccepted)
}

func (s *session) onDetach(f []any) {
	h := u32(at(f, 0))
	l := s.links[h]
	if l == nil {
		return
	}
	if e := parseError(at(f, 2)); e != nil {
		s.c.log.Debug("amqp link detached with error", "link", l.pub.Name, "condition", e.Condition, "description", e.Description)
	}
	l.mu.Lock()
	ours := l.detachSent
	l.detachSent = true
	l.mu.Unlock()
	if !ours { // the peer detached first: reply
		s.c.send(s.ch, perf(pDetach, h, boolean(at(f, 1))), nil)
	}
	delete(s.links, h)
	l.teardown()
}

// sendTransfer writes one delivery, split over as many frames as the peer's
// max-frame-size needs, waiting for session window.
func (s *session) sendTransfer(l *lnk, id uint32, tag []byte, format uint32, settled bool, payload []byte) error {
	c := s.c
	room := int(c.maxFrame) - 8
	for first := true; ; first = false {
		mk := func(more bool) Described {
			var mv any
			if more {
				mv = true
			}
			if first {
				return perf(pTransfer, l.handle, id, tag, format, settled, mv)
			}
			return perf(pTransfer, l.handle, nil, nil, nil, nil, mv)
		}
		pb, err := appendValue(nil, mk(false))
		if err != nil {
			return err
		}
		chunk := payload
		more := len(pb)+len(payload) > room
		if more {
			if pb, err = appendValue(nil, mk(true)); err != nil {
				return err
			}
			chunk = payload[:room-len(pb)]
		}
		s.mu.Lock()
		for s.remoteWin == 0 && !s.dead {
			s.cond.Wait()
		}
		if s.dead {
			s.mu.Unlock()
			return net.ErrClosed
		}
		s.remoteWin--
		s.nextOut++
		s.mu.Unlock()
		c.debug("->", "transfer", s.ch)
		if err := c.write(frameAMQP, s.ch, append(pb, chunk...)); err != nil {
			return err
		}
		if !more {
			return nil
		}
		payload = payload[len(chunk):]
	}
}
