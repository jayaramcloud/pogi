// Package amqp is pogi's AMQP 1.0 broker, the transport under Azure Service
// Bus and Event Hubs. Services plug in through Resolvers: the broker owns
// connections, SASL, $cbs tokens, sessions, links, flow control and
// settlement; a service only sees messages arriving at its Sinks and pulls
// messages out of its Sources.
//
// This file is the contract services build against. The wire protocol lives
// in the rest of the package.
package amqp

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

// Message is an AMQP 1.0 message. Map keys of the annotation sections are
// symbols on the wire; values use Go types (string, int64, uint32, bool,
// []byte, time.Time, UUID, Symbol, nil, []any, map[string]any).
type Message struct {
	Header                *Header
	DeliveryAnnotations   map[string]any
	MessageAnnotations    map[string]any // x-opt-sequence-number, x-opt-enqueued-time, x-opt-locked-until, x-opt-partition-key, x-opt-offset, ...
	Properties            *Properties
	ApplicationProperties map[string]any
	Data                  [][]byte // data sections (the usual body)
	Value                 any      // amqp-value body
	Sequence              [][]any  // amqp-sequence body
	Footer                map[string]any
	Format                uint32 // message-format of the transfer; 0x80013700 marks Event Hubs/Service Bus batches

	// Set by the broker on deliveries it sends; read-only for services.
	DeliveryTag []byte
}

// Header is the message header section.
type Header struct {
	Durable       bool
	Priority      uint8
	TTL           time.Duration
	FirstAcquirer bool
	DeliveryCount uint32
}

// Properties is the immutable properties section. Message, correlation ids
// may be string, UUID, []byte or uint64.
type Properties struct {
	MessageID          any
	UserID             []byte
	To                 string
	Subject            string
	ReplyTo            string
	CorrelationID      any
	ContentType        string
	ContentEncoding    string
	AbsoluteExpiryTime time.Time
	CreationTime       time.Time
	GroupID            string
	GroupSequence      uint32
	ReplyToGroupID     string
}

// UUID is the AMQP uuid type.
type UUID [16]byte

func (u UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:16])
}

// Symbol is the AMQP symbol type, for values that must be encoded as one.
type Symbol string

// Error is an AMQP error condition. Return one from a Sink, Source, Handler
// or Resolver to send that condition ("amqp:not-found",
// "com.microsoft:message-lock-lost", ...) instead of amqp:internal-error.
type Error struct {
	Condition   string
	Description string
	Info        map[string]any
}

func (e *Error) Error() string { return e.Condition + ": " + e.Description }

// Errorf builds an *Error.
func Errorf(condition, format string, a ...any) *Error {
	return &Error{Condition: condition, Description: fmt.Sprintf(format, a...)}
}

// Common conditions.
const (
	NotFound          = "amqp:not-found"
	Unauthorized      = "amqp:unauthorized-access"
	InternalError     = "amqp:internal-error"
	ResourceLimit     = "amqp:resource-limit-exceeded"
	NotAllowed        = "amqp:not-allowed"
	InvalidField      = "amqp:invalid-field"
	Stolen            = "amqp:link:stolen"
	MessageSizeExceed = "amqp:link:message-size-exceeded"
)

// Outcome is the disposition a client gives a delivery from a Source.
type Outcome struct {
	State             string // "accepted", "released", "rejected" or "modified"
	Error             *Error // rejected
	DeliveryFailed    bool   // modified
	UndeliverableHere bool   // modified
	Annotations       map[string]any
}

// Conn is a client connection.
type Conn struct {
	Host        string // hostname from the open frame, e.g. "ns.servicebus.windows.net" or "localhost"
	ContainerID string
	User        string // SASL PLAIN user; "" for ANONYMOUS

	mu     sync.Mutex
	claims map[string]time.Time // $cbs audiences authorized, with expiry
	jwts   map[string]string    // the JWT put for an audience, for the authorizer
}

// JWT is the JWT a client put with $cbs for the entity at addr (its audience ends with it), "" when
// none; with no match it is the connection's latest one, as a client uses one token for everything.
func (c *Conn) JWT(addr string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	last := ""
	for a, t := range c.jwts {
		if strings.HasSuffix(strings.TrimSuffix(a, "/"), "/"+strings.Trim(addr, "/")) {
			return t
		}
		last = t
	}
	return last
}

// Authorized reports whether a $cbs token covering audience (a prefix of
// it, as Azure scopes tokens) is in force on this connection.
func (c *Conn) Authorized(audience string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for a, exp := range c.claims {
		if (exp.IsZero() || time.Now().Before(exp)) && len(audience) >= len(a) && audience[:len(a)] == a {
			return true
		}
	}
	return false
}

func (c *Conn) addJWT(audience, token string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.jwts == nil {
		c.jwts = map[string]string{}
	}
	c.jwts[audience] = token
}

func (c *Conn) addClaim(audience string, exp time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.claims == nil {
		c.claims = map[string]time.Time{}
	}
	c.claims[audience] = exp
}

// Link is an attached link, seen from the client: Receiving links take
// messages from a Source, sending links put them into a Sink.
type Link struct {
	Conn      *Conn
	Name      string
	Address   string         // target address when the client sends, source address when it receives
	Receiving bool           // the client receives on this link
	Filter    map[string]any // source filter: "apache.org:selector-filter:string", "com.microsoft:session-filter", ...
	// Properties are the attach properties: "com.microsoft:epoch",
	// "com.microsoft:receiver-name", "com.microsoft:timeout", ...
	Properties map[string]any
	// Settled is true when the client receives pre-settled (Event Hubs, and
	// Service Bus receive-and-delete): the broker never calls Settle.
	Settled bool

	// A Resolver may set these to fill the attach reply, for example the
	// session filter naming the session a session receiver locked.
	ReplySourceFilter map[string]any
	ReplyProperties   map[string]any
	// ReplyMaxMessageSize, when set, is the max-message-size of the attach
	// reply; azeventhubs sizes its batches by it.
	ReplyMaxMessageSize uint64

	target string // the client's own address on a receiving link (a request-response reply-to)
	close  func(*Error)
}

// Close detaches the link from the broker side with err (nil: no error),
// for example amqp:link:stolen when a higher Event Hubs epoch attaches. It
// may be called from any goroutine, including a Resolver or Sink; the detach
// happens in the background.
func (l *Link) Close(err *Error) {
	if l.close != nil {
		l.close(err)
	}
}

// Sink takes messages a client sends on a link. Return nil to accept; an
// error rejects the delivery with that condition.
type Sink interface {
	Accept(ctx context.Context, l *Link, m *Message) error
}

// Source gives messages to a client receiving on a link. The broker calls
// Next only while the link has credit; Next blocks until a message is
// available or ctx ends (the link detached or the connection closed). For
// unsettled links the broker reports the client's disposition through
// Settle; a link that detaches with deliveries unsettled gets Settle with
// "released" for each of them.
type Source interface {
	Next(ctx context.Context, l *Link) (*Message, error)
	Settle(l *Link, m *Message, o Outcome)
}

// Detacher is an optional interface of a Sink or Source node: Detached is
// called once when a link on the node ends for any reason (client detach,
// connection close, Link.Close), after Next's ctx is cancelled and the link's
// unsettled deliveries are released.
type Detacher interface {
	Detached(l *Link)
}

// Resolver finds the node for a link's address: a Sink when l.Receiving is
// false, a Source when it is true. It returns (nil, nil) when the address
// isn't its own, so the next resolver is tried, and an *Error to refuse the
// attach. Resolvers see "$management" and "<entity>/$management" addresses
// too; return Management(handler) for them.
type Resolver func(l *Link) (any, error)

// Handler answers a request on a request-response node such as
// $management. The broker sets the response's correlation-id from the
// request's message-id and sends it on the client's link whose source
// address is the request's reply-to.
type Handler func(ctx context.Context, l *Link, req *Message) (*Message, error)

// Management makes a request-response node from h.
func Management(h Handler) Sink { return management(h) }

type management Handler

func (m management) Accept(ctx context.Context, l *Link, req *Message) error {
	return errors.New("amqp: management nodes are served by the broker")
}

// Options configures a Server.
type Options struct {
	Log *slog.Logger
	// Login checks SASL PLAIN credentials; nil accepts any. ANONYMOUS is always offered.
	Login func(user, pass string) bool
	// Authorize checks a $cbs put-token ("servicebus.windows.net:sastoken" or
	// "jwt") for an audience; nil accepts any token.
	Authorize func(tokenType, audience, token string) error
	// IdleTimeout is the idle-time-out the broker asks of clients: a client
	// silent for this long is closed. 0: 4 minutes.
	IdleTimeout time.Duration
}

// Server is the broker. One Server serves every AMQP service on the hub.
type Server struct {
	opt       Options
	mu        sync.Mutex
	resolvers []Resolver
	lns       map[net.Listener]struct{}
	conns     map[*conn]struct{}
	closed    bool
}

// NewServer creates a broker.
func NewServer(o Options) *Server {
	if o.Log == nil {
		o.Log = slog.New(slog.DiscardHandler)
	}
	return &Server{opt: o}
}

// Add registers a service's resolver; resolvers are tried in order.
func (s *Server) Add(r Resolver) {
	s.mu.Lock()
	s.resolvers = append(s.resolvers, r)
	s.mu.Unlock()
}

// Resolve finds the node for a link, or an amqp:not-found error.
func (s *Server) Resolve(l *Link) (any, error) {
	s.mu.Lock()
	rs := s.resolvers
	s.mu.Unlock()
	for _, r := range rs {
		n, err := r(l)
		if err != nil || n != nil {
			return n, err
		}
	}
	return nil, Errorf(NotFound, "The messaging entity '%s' could not be found.", l.Address)
}
