package rds

// The SQL endpoint of Aurora DSQL. The DSQL API lives in the generic service, which asks this one for a
// PostgreSQL server per cluster (AddSQL, DropSQL). Clients reach all of them on pogi's main port: the port
// hands a connection that opens like a PostgreSQL client's to servePG. A DSQL auth token (a presigned URL that
// starts with the cluster's host name) both logs the client in and says which cluster it wants.

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"github.com/nrndev/pogi/internal/core"
)

const dsqlAdmin = "admin"

// AddSQL gives the DSQL cluster id its server: PostgreSQL 16 with the admin role and the postgres database.
// Without Docker only a note is kept, which the client reads when it connects.
func (s *Service) AddSQL(account, region, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	srv := &Server{Password: core.RandString(24, core.AlphaNum)}
	k := account + "/" + region + "/" + id
	if old := s.st.SQL[k]; old != nil && old.Container != "" {
		s.box.Remove(old.Container)
	}
	s.st.SQL[k] = srv
	s.box.Serve(srv, account, region, id, 0, 0, func() Spec { return sqlSpec(srv) }, s.box.Available())
}

// DropSQL removes the server of a deleted DSQL cluster, with its container and data.
func (s *Service) DropSQL(account, region, id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := account + "/" + region + "/" + id
	if srv := s.st.SQL[k]; srv != nil {
		delete(s.st.SQL, k)
		if srv.Container != "" {
			s.box.Remove(srv.Container)
		}
	}
}

func sqlSpec(srv *Server) Spec {
	return dbSpec("postgres", "16", dsqlAdmin, srv.Password, "postgres", srv.HostPort)
}

// servePG serves one PostgreSQL client that reached pogi's main port.
func (s *Service) servePG(c net.Conn) {
	defer c.Close()
	b, err := s.pgLogin(&c, pgAuth{requireTLS: true, pick: s.dsqlPick})
	if err != nil {
		s.hub.Log.Debug("dsql login", "err", err)
		return
	}
	splice(c, b)
}

// dsqlPick checks a DSQL auth token for user and finds the cluster's server.
func (s *Service) dsqlPick(user, token string) (target, bool, error) {
	id, region, err := s.dsqlToken(token, user)
	if err != nil {
		return target{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, srv := range s.st.SQL {
		if !strings.HasSuffix(k, "/"+region+"/"+id) {
			continue
		}
		switch {
		case srv.Container == "":
			return target{}, false, &pgconn.PgError{Severity: "FATAL", Code: "58000", Message: "the SQL endpoint of this Aurora DSQL cluster needs Docker, which was not running when the cluster was made"}
		case srv.State != "available":
			return target{}, false, &pgconn.PgError{Severity: "FATAL", Code: "57P03", Message: "the SQL endpoint of this Aurora DSQL cluster is not ready (" + srv.State + ")"}
		}
		return target{user: dsqlAdmin, password: srv.Password, port: srv.HostPort}, true, nil
	}
	return target{}, false, fmt.Errorf("no cluster %s in %s", id, region)
}

// dsqlToken checks a DSQL auth token: a presigned URL for <id>.dsql.<region>.on.aws, action DbConnectAdmin for
// the admin user and DbConnect for any other, service dsql, valid for the X-Amz-Expires it was made with (at
// most a week), signed by a key pogi knows when signatures are checked. It returns the cluster id and region.
func (s *Service) dsqlToken(token, user string) (id, region string, err error) {
	u, err := url.Parse("http://" + token)
	if err != nil {
		return "", "", err
	}
	q := u.Query()
	cred := strings.Split(q.Get("X-Amz-Credential"), "/")
	signed, err := time.Parse("20060102T150405Z", q.Get("X-Amz-Date"))
	expires, _ := strconv.Atoi(q.Get("X-Amz-Expires"))
	id, rest, _ := strings.Cut(u.Hostname(), ".dsql.")
	region, ok := strings.CutSuffix(rest, ".on.aws")
	want := "DbConnect"
	if user == dsqlAdmin {
		want = "DbConnectAdmin"
	}
	switch {
	case err != nil || len(cred) != 5 || cred[3] != "dsql" || !ok || id == "" || region == "":
		return "", "", errors.New("not a DSQL auth token")
	case q.Get("Action") != want:
		return "", "", fmt.Errorf("the token is for action %q, not %q", q.Get("Action"), want)
	case expires < 1 || expires > 604800 || time.Since(signed) > time.Duration(expires)*time.Second || signed.After(time.Now().Add(5*time.Minute)):
		return "", "", errors.New("the token expired")
	}
	return id, region, s.hub.VerifyPresigned("dsql", "http://"+token)
}
