package rds

// How clients reach a database. With IAM database authentication on, pogi's
// proxy owns the endpoint port and the container publishes a free port behind
// it. The proxy speaks just enough of the wire protocol to log the client in
// with its password, or with an IAM auth token (a presigned rds-db URL, as
// `aws rds generate-db-auth-token` makes), then splices bytes. Once a proxy
// fronts a server it stays, splicing from the start when IAM auth is off, so
// the endpoint port never moves. The RDS Data API reaches clusters through
// DataTarget, and Enable/DisableHttpEndpoint switch it.

import (
	"bytes"
	"cmp"
	"context"
	"crypto/tls"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"github.com/nrndev/pogi/internal/core"
)

// wire is the protocol of an engine: postgres, mysql or "" (others).
func wire(engine string) string {
	switch {
	case strings.Contains(engine, "postgres"):
		return "postgres"
	case strings.Contains(engine, "mysql"), engine == "mariadb":
		return "mysql"
	}
	return ""
}

// target is what the proxy needs of the resource behind srv; lock held.
type target struct {
	engine, version, user, password string
	port                            int
	iam                             bool
}

func (s *Service) target(srv *Server) (target, bool) {
	for _, i := range s.st.Instances {
		if &i.Pogi == srv {
			return target{i.Engine, i.EngineVersion, i.MasterUsername, srv.Password, srv.HostPort, i.IAMDatabaseAuthenticationEnabled}, true
		}
	}
	for _, cl := range s.st.Clusters {
		if &cl.Pogi == srv {
			return target{cl.Engine, cl.EngineVersion, cl.MasterUsername, srv.Password, srv.HostPort, cl.IAMDatabaseAuthenticationEnabled}, true
		}
	}
	return target{}, false
}

// front puts the proxy on the endpoint port (want, or any free one) of a new
// resource with IAM auth and returns the port its container should publish
// instead (0: any). Lock held.
func (s *Service) front(srv *Server, engine string, want int, iam, docker bool) int {
	if !iam || !docker || wire(engine) == "" {
		return want
	}
	s.listen(srv, want)
	return 0
}

// iamAuth follows IAM auth turned on by a modify. A container that publishes
// the endpoint port itself is made again behind the proxy: its databases are
// dumped, the new container on a free port loads them, the proxy takes the
// port. Lock held.
func (s *Service) iamAuth(srv *Server, engine string, on bool, spec func() Spec) {
	if !on || srv.Proxy != 0 || srv.Container == "" || wire(engine) == "" {
		return
	}
	if srv.State != "available" { // ponytail: takes effect only on a running server; else on the next modify
		return
	}
	name, port, pw, file := srv.Container, srv.HostPort, srv.Password, filepath.Join(s.files(), "restores", srv.Container+".sql")
	srv.State, srv.Proxy = "modifying", port // the endpoint stays
	s.box.serial(name, func() {
		err := s.dump(name, engine, pw, file)
		if err == nil {
			err = s.box.d.Remove(s.box.ctx, name) // frees the port
		}
		if err == nil {
			waitFree(port, 15*time.Second)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.owns(srv) {
			return
		}
		if err != nil {
			srv.State, srv.Proxy, srv.Note = "available", 0, err.Error()
			return
		}
		s.listen(srv, port)
		sp := spec()
		sp.Host, srv.Seed = 0, file
		s.box.Up(srv, sp, "run")
	})
}

// waitFree waits until nothing holds port on 127.0.0.1: the engine releases the
// published port of a removed container a moment after the removal returns, and
// the proxy that takes over must get that port, not another, to keep the endpoint.
func waitFree(port int, d time.Duration) {
	for end := time.Now().Add(d); time.Now().Before(end); time.Sleep(50 * time.Millisecond) {
		if ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port))); err == nil {
			ln.Close()
			return
		}
	}
}

// listen serves the proxy of srv on port, or on any free one when it is
// taken. Lock held.
func (s *Service) listen(srv *Server, port int) {
	ln, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		ln, err = net.Listen("tcp", "127.0.0.1:0")
	}
	if err != nil {
		srv.Note = err.Error()
		return
	}
	if s.fronts == nil {
		s.fronts = map[*Server]net.Listener{}
	}
	s.fronts[srv], srv.Proxy = ln, ln.Addr().(*net.TCPAddr).Port
	core.Go("rds.listen", func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			core.Go("rds.proxy", func() { s.proxy(srv, c) })
		}
	})
}

// gone drops what pogi keeps for a deleted resource's server: its proxy and
// the dump it was made from. Lock held.
func (s *Service) gone(srv *Server) {
	if ln := s.fronts[srv]; ln != nil {
		ln.Close()
		delete(s.fronts, srv)
	}
	if srv.Container != "" && s.dir != "" {
		os.Remove(filepath.Join(s.dir, "restores", srv.Container+".sql"))
	}
}

// reload follows a Load: proxies go with the servers they fronted and come
// up for the loaded ones, and the dumps of dropped snapshots go. Lock held.
func (s *Service) reload(old state) {
	for srv := range s.fronts {
		if !s.owns(srv) {
			s.gone(srv)
		}
	}
	for _, srv := range s.servers() {
		if srv.Proxy != 0 && s.fronts[srv] == nil {
			s.listen(srv, srv.Proxy)
		}
	}
	for k := range old.Snapshots {
		if s.st.Snapshots[k] == nil {
			os.Remove(s.snapFile(k, false))
		}
	}
	for k := range old.ClusterSnapshots {
		if s.st.ClusterSnapshots[k] == nil {
			os.Remove(s.snapFile(k, true))
		}
	}
}

// servers lists the servers of every instance and cluster; lock held.
func (s *Service) servers() []*Server {
	var out []*Server
	for _, i := range s.st.Instances {
		out = append(out, &i.Pogi)
	}
	for _, cl := range s.st.Clusters {
		out = append(out, &cl.Pogi)
	}
	return out
}

// closeFronts stops every proxy and drops a temporary dump directory.
func (s *Service) closeFronts() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for srv, ln := range s.fronts {
		ln.Close()
		delete(s.fronts, srv)
	}
	if s.temp {
		os.RemoveAll(s.dir)
	}
}

// proxy serves one client connection.
func (s *Service) proxy(srv *Server, c net.Conn) {
	defer c.Close()
	s.mu.Lock()
	t, ok := s.target(srv)
	s.mu.Unlock()
	if !ok {
		return
	}
	addr := s.hub.Docker().Addr(t.port)
	var b net.Conn
	var err error
	switch {
	case !t.iam:
		b, err = net.Dial("tcp", addr)
	case wire(t.engine) == "postgres":
		b, err = s.pgLogin(&c, pgAuth{cancel: addr, pick: func(user, pw string) (target, bool, error) {
			if !isToken(pw) {
				return t, false, nil
			}
			return t, true, s.checkToken(pw, user)
		}})
	default:
		b, err = s.myLogin(c, t, addr)
	}
	if err != nil {
		s.hub.Log.Debug("rds proxy", "addr", addr, "err", err)
		return
	}
	splice(c, b)
}

// splice copies bytes both ways until either side ends, then closes both.
func splice(c, b net.Conn) {
	defer b.Close()
	done := make(chan struct{})
	go func() { io.Copy(b, c); b.Close(); close(done) }()
	io.Copy(c, b)
	c.Close()
	<-done
}

// isToken tells an IAM auth token from a password.
func isToken(pw string) bool { return strings.Contains(pw, "X-Amz-Signature=") }

// checkToken checks an IAM auth token for user: a presigned rds-db connect
// URL, at most 15 minutes old, signed by a key pogi knows when signatures are
// checked (Config.ValidateSigs).
func (s *Service) checkToken(token, user string) error {
	u, err := url.Parse("http://" + token)
	if err != nil {
		return err
	}
	q := u.Query()
	cred := strings.Split(q.Get("X-Amz-Credential"), "/")
	signed, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	expires, _ := strconv.Atoi(q.Get("X-Amz-Expires"))
	switch {
	case err != nil || q.Get("Action") != "connect" || len(cred) != 5 || cred[3] != "rds-db":
		return errors.New("not an IAM auth token")
	case q.Get("DBUser") != user:
		return fmt.Errorf("the token is for %q, not %q", q.Get("DBUser"), user)
	case time.Since(signed) > min(time.Duration(expires)*time.Second, 15*time.Minute):
		return errors.New("the token expired")
	}
	return s.hub.VerifyPresigned("rds", "http://"+token)
}

// serverTLS is the TLS the proxy offers: pogi's leaf certificate (CA:FALSE, for
// localhost) signed by the local CA, which clients load as sslrootcert so that
// verify-full works (`pogi cert` shows where it is). Nil when pogi has nowhere
// to keep a certificate; then SSL is declined.
func (s *Service) serverTLS() *tls.Config {
	if s.hub.Cfg.DataDir == "" && s.hub.Cfg.TLSCert == "" {
		return nil
	}
	cfg, err := s.hub.TLS()
	if err != nil {
		s.hub.Log.Warn("rds proxy tls", "err", err)
		return nil
	}
	cfg = cfg.Clone()
	cfg.NextProtos = nil // the HTTP protocols are not Postgres's
	return cfg
}

// pgAuth says where a Postgres login goes and how it is checked.
type pgAuth struct {
	cancel     string // where a cancel request is relayed; "" drops it
	requireTLS bool   // refuse a client that did not start TLS (Aurora DSQL)
	// pick runs once the password is known. It returns the server to log in to and whether the password is an
	// IAM auth token (pogi checked it: the login is as the server's user, then switched to the token's user).
	// An error is told to the client: a *pgconn.PgError as it is, any other as a failed password.
	pick func(user, password string) (t target, token bool, err error)
}

// pgLogin logs a Postgres client in: SSL is offered (see serverTLS), the
// password comes in clear text and the server checks it, unless it is an IAM auth token: pogi
// checks that and logs in as the master user, switching to the token's user
// (the login is refused when that role does not exist). The server's greeting is relayed.
// After TLS starts, *cp is the TLS connection, for the bytes that follow.
func (s *Service) pgLogin(cp *net.Conn, a pgAuth) (net.Conn, error) {
	c := *cp
	fe := pgproto3.NewBackend(c, c) // pogi is the client's backend
	var start *pgproto3.StartupMessage
	for start == nil {
		m, err := fe.ReceiveStartupMessage()
		if err != nil {
			return nil, err
		}
		switch m := m.(type) {
		case *pgproto3.StartupMessage:
			start = m
		case *pgproto3.CancelRequest: // the server knows the key it handed out
			if b, err := net.Dial("tcp", a.cancel); a.cancel != "" && err == nil {
				buf, _ := m.Encode(nil)
				b.Write(buf)
				b.Close()
			}
			return nil, errors.New("cancel request relayed")
		case *pgproto3.SSLRequest:
			cfg := s.serverTLS()
			if cfg == nil {
				if _, err := c.Write([]byte("N")); err != nil {
					return nil, err
				}
				break
			}
			if _, err := c.Write([]byte("S")); err != nil {
				return nil, err
			}
			tc := tls.Server(c, cfg)
			if err := tc.Handshake(); err != nil {
				return nil, err
			}
			c, *cp = tc, tc
			fe = pgproto3.NewBackend(c, c)
		default: // GSSEncRequest
			if _, err := c.Write([]byte("N")); err != nil {
				return nil, err
			}
		}
	}
	if _, secure := c.(*tls.Conn); a.requireTLS && !secure {
		return nil, pgFail(fe, &pgconn.PgError{Severity: "FATAL", Code: "28000", Message: "SSL/TLS connection is required"}, errors.New("no TLS"))
	}
	user := start.Parameters["user"]
	fe.Send(&pgproto3.AuthenticationCleartextPassword{})
	if err := fe.Flush(); err != nil {
		return nil, err
	}
	fe.SetAuthType(pgproto3.AuthTypeCleartextPassword)
	m, err := fe.Receive()
	if err != nil {
		return nil, err
	}
	pw, ok := m.(*pgproto3.PasswordMessage)
	if !ok {
		return nil, fmt.Errorf("expected a password, got %T", m)
	}
	t, token, err := a.pick(user, pw.Password)
	if err != nil {
		pe := &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: fmt.Sprintf("PAM authentication failed for user %q", user)}
		errors.As(err, &pe)
		return nil, pgFail(fe, pe, err)
	}
	cfg, err := pgconn.ParseConfig("sslmode=disable")
	if err != nil {
		return nil, err
	}
	cfg.Host, cfg.Port, cfg.Fallbacks, cfg.ValidateConnect = "127.0.0.1", uint16(t.port), nil, nil
	cfg.User, cfg.Password, cfg.Database = user, pw.Password, cmp.Or(start.Parameters["database"], user)
	cfg.RuntimeParams = map[string]string{}
	for k, v := range start.Parameters {
		if k != "user" && k != "database" {
			cfg.RuntimeParams[k] = v
		}
	}
	if token {
		cfg.User, cfg.Password = t.user, t.password
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pc, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, pgFail(fe, err, err)
	}
	if token && user != t.user { // a role the database lacks must not stay the master user
		if _, err := pc.Exec(ctx, `SET SESSION AUTHORIZATION "`+strings.ReplaceAll(user, `"`, `""`)+`"`).ReadAll(); err != nil {
			pc.Close(ctx)
			return nil, pgFail(fe, &pgconn.PgError{Severity: "FATAL", Code: "28P01", Message: fmt.Sprintf("PAM authentication failed for user %q", user)}, err)
		}
	}
	hj, err := pc.Hijack()
	if err != nil {
		pc.Close(ctx)
		return nil, err
	}
	fe.Send(&pgproto3.AuthenticationOk{})
	for k, v := range hj.ParameterStatuses {
		fe.Send(&pgproto3.ParameterStatus{Name: k, Value: v})
	}
	fe.Send(&pgproto3.BackendKeyData{ProcessID: hj.PID, SecretKey: hj.SecretKey})
	fe.Send(&pgproto3.ReadyForQuery{TxStatus: hj.TxStatus})
	if err := fe.Flush(); err != nil {
		hj.Conn.Close()
		return nil, err
	}
	return hj.Conn, nil
}

// pgFail tells a Postgres client why its login failed (the server's error
// when there is one) and returns err.
func pgFail(fe *pgproto3.Backend, why, err error) error {
	e := &pgconn.PgError{Severity: "FATAL", Code: "08006", Message: why.Error()}
	errors.As(why, &e)
	fe.Send(&pgproto3.ErrorResponse{Severity: e.Severity, SeverityUnlocalized: e.Severity, Code: e.Code, Message: e.Message})
	fe.Flush()
	return err
}

// MySQL capability flags the proxy offers clients: those the driver that
// logs in to the server uses, minus TLS and compression.
const (
	myLongPassword   = 1
	myFoundRows      = 2
	myConnectWithDB  = 8
	myProtocol41     = 512
	mySecureConn     = 1 << 15
	myMultiStatement = 1 << 16
	myPluginAuth     = 1 << 19
	myLenencAuth     = 1 << 21
	myCaps           = myLongPassword | myFoundRows | 4 /* long flag */ | myConnectWithDB | 128 /* local files */ | myProtocol41 |
		8192 /* transactions */ | mySecureConn | myMultiStatement | 1<<17 /* multi results */ | myPluginAuth | 1<<20 /* attrs */ |
		myLenencAuth | 1<<24 /* deprecate EOF */
)

// myLogin logs a MySQL client in: the handshake switches it to the
// mysql_clear_password plugin (clients allow it, as for RDS IAM auth), the
// password or token is checked as for Postgres, and the server login is the
// driver's, offered only the capabilities the client chose so that both
// sides speak the same protocol.
// ponytail: a token logs in as the master user; MySQL cannot switch users.
func (s *Service) myLogin(c net.Conn, t target, addr string) (net.Conn, error) {
	version := t.version
	if t.engine == "mariadb" {
		version = "5.5.5-" + version + "-MariaDB"
	}
	nonce := []byte(core.RandString(20, core.AlphaNum))
	hello := append(append([]byte{10}, version...), 0, 1, 0, 0, 0) // protocol 10, version, thread id 1
	hello = binary.LittleEndian.AppendUint16(append(append(hello, nonce[:8]...), 0), myCaps&0xffff)
	hello = binary.LittleEndian.AppendUint16(append(hello, 45, 2, 0), myCaps>>16) // utf8mb4, autocommit
	hello = append(append(append(hello, 21), make([]byte, 10)...), nonce[8:]...)
	hello = append(hello, "\x00mysql_native_password\x00"...)
	if err := myWrite(c, 0, hello); err != nil {
		return nil, err
	}
	seq, resp, err := myRead(c)
	if err == nil && len(resp) < 33 {
		err = errors.New("short handshake response")
	}
	if err != nil {
		return nil, err
	}
	flags := binary.LittleEndian.Uint32(resp)
	user, rest := cstr(resp[32:])
	switch { // skip the auth response
	case flags&myLenencAuth != 0 && len(rest) > 0:
		n, w := int(rest[0]), 1
		if n == 0xfc && len(rest) > 2 {
			n, w = int(binary.LittleEndian.Uint16(rest[1:])), 3
		}
		rest = rest[min(len(rest), w+n):]
	case flags&mySecureConn != 0 && len(rest) > 0:
		rest = rest[min(len(rest), 1+int(rest[0])):]
	default:
		_, rest = cstr(rest)
	}
	var db string
	if flags&myConnectWithDB != 0 {
		db, _ = cstr(rest)
	}
	if err := myWrite(c, seq+1, []byte("\xfemysql_clear_password\x00")); err != nil {
		return nil, err
	}
	seq, pw, err := myRead(c)
	if err != nil {
		return nil, err
	}
	password := strings.TrimSuffix(string(pw), "\x00")
	cfg := mysql.NewConfig()
	cfg.Net, cfg.Addr, cfg.User, cfg.Passwd, cfg.DBName, cfg.MaxAllowedPacket = "tcp", addr, user, password, db, 64<<20
	cfg.ClientFoundRows, cfg.MultiStatements = flags&myFoundRows != 0, flags&myMultiStatement != 0
	if isToken(password) {
		if err := s.checkToken(password, user); err != nil {
			return nil, myFail(c, seq+1, &mysql.MySQLError{Number: 1045, SQLState: [5]byte([]byte("28000")),
				Message: fmt.Sprintf("Access denied for user '%s'@'localhost' (using password: YES)", user)}, err)
		}
		cfg.User, cfg.Passwd = t.user, t.password
	}
	var raw net.Conn
	cfg.DialFunc = func(ctx context.Context, network, addr string) (net.Conn, error) {
		var err error
		raw, err = (&net.Dialer{}).DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		return &masked{Conn: raw, keep: flags | myLongPassword | myConnectWithDB | myProtocol41 | mySecureConn | myPluginAuth | myLenencAuth | 1<<20}, nil
	}
	conn, _ := mysql.NewConnector(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dc, err := conn.Connect(ctx)
	if err != nil {
		return nil, myFail(c, seq+1, err, err)
	}
	if err := myWrite(c, seq+1, []byte{0, 0, 0, 2, 0, 0, 0}); err != nil { // OK, autocommit
		dc.Close()
		return nil, err
	}
	return &driverConn{raw, dc}, nil
}

// driverConn is the server connection of the driver that logged in; closing
// it closes the driver's side too.
type driverConn struct {
	net.Conn
	dc driver.Conn
}

func (d *driverConn) Close() error { return d.dc.Close() }

// masked hides from the driver the server capabilities the client did not
// choose, so the server session speaks the client's protocol (EOF packets,
// found rows, multi statements), and MariaDB's extended ones.
type masked struct {
	net.Conn
	keep uint32
	buf  []byte
	read bool
}

func (m *masked) Read(p []byte) (int, error) {
	if !m.read {
		m.read = true
		seq, hello, err := myRead(m.Conn)
		if err != nil {
			return 0, err
		}
		if i := bytes.IndexByte(hello, 0); i > 0 && len(hello) >= i+32 && hello[0] == 10 {
			lo, hi := i+14, i+19 // after version, thread id, scramble, filler; after charset and status
			caps := (uint32(binary.LittleEndian.Uint16(hello[lo:])) | uint32(binary.LittleEndian.Uint16(hello[hi:]))<<16) & m.keep
			binary.LittleEndian.PutUint16(hello[lo:], uint16(caps))
			binary.LittleEndian.PutUint16(hello[hi:], uint16(caps>>16))
			clear(hello[hi+3 : hi+13])
		}
		m.buf = append([]byte{byte(len(hello)), byte(len(hello) >> 8), byte(len(hello) >> 16), seq}, hello...)
	}
	if len(m.buf) > 0 {
		n := copy(p, m.buf)
		m.buf = m.buf[n:]
		return n, nil
	}
	return m.Conn.Read(p)
}

// myFail sends a MySQL client the server's error (or why's message as an
// access error) and returns err.
func myFail(c net.Conn, seq byte, why, err error) error {
	e := &mysql.MySQLError{Number: 1045, SQLState: [5]byte([]byte("28000")), Message: why.Error()}
	errors.As(why, &e)
	myWrite(c, seq, append(binary.LittleEndian.AppendUint16([]byte{0xff}, e.Number), "#"+string(e.SQLState[:])+e.Message...))
	return err
}

func myRead(r io.Reader) (byte, []byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	p := make([]byte, int(h[0])|int(h[1])<<8|int(h[2])<<16)
	_, err := io.ReadFull(r, p)
	return h[3], p, err
}

func myWrite(w io.Writer, seq byte, p []byte) error {
	_, err := w.Write(append([]byte{byte(len(p)), byte(len(p) >> 8), byte(len(p) >> 16), seq}, p...))
	return err
}

// cstr splits a NUL-terminated string off b.
func cstr(b []byte) (string, []byte) {
	if i := bytes.IndexByte(b, 0); i >= 0 {
		return string(b[:i]), b[i+1:]
	}
	return string(b), nil
}

// ---- the Data API's side ----

// DataTarget is a DB cluster as the RDS Data API sees it.
type DataTarget struct {
	ID, Engine, Database, Status string
	Port                         int // of its server on 127.0.0.1, not of a proxy; 0 when nothing runs
	HTTPEndpoint                 bool
}

// DataTarget resolves the DB cluster ARN of a Data API call; ok is false
// when there is no such cluster.
func (s *Service) DataTarget(arn string) (DataTarget, bool) {
	_, region, account, res, _ := core.ParseARN(arn)
	kind, id, _ := strings.Cut(res, ":")
	s.mu.Lock()
	defer s.mu.Unlock()
	cl := s.st.Clusters[key(&core.Ctx{Account: account, Region: region}, id)]
	if kind != "cluster" || cl == nil {
		return DataTarget{}, false
	}
	t := DataTarget{ID: cl.DBClusterIdentifier, Engine: cl.Engine, Database: cl.DatabaseName, Status: cl.Pogi.State, HTTPEndpoint: cl.HttpEndpointEnabled}
	if cl.Pogi.Container != "" {
		t.Port = cl.Pogi.HostPort
	}
	return t, true
}

type httpEndpointInput struct{ ResourceArn string }

// httpEndpoint serves EnableHttpEndpoint and DisableHttpEndpoint.
func (s *Service) httpEndpoint(c *core.Ctx, in *httpEndpointInput) (any, error) {
	_, region, account, res, _ := core.ParseARN(in.ResourceArn)
	kind, id, _ := strings.Cut(res, ":")
	s.mu.Lock()
	defer s.mu.Unlock()
	cl := s.st.Clusters[key(&core.Ctx{Account: account, Region: region}, id)]
	if kind != "cluster" || cl == nil {
		return nil, core.Errorf(404, "ResourceNotFoundFault", "The specified resource ARN %s was not found.", in.ResourceArn)
	}
	cl.HttpEndpointEnabled = c.Op == "EnableHttpEndpoint"
	return map[string]any{"ResourceArn": in.ResourceArn, "HttpEndpointEnabled": cl.HttpEndpointEnabled}, nil
}
