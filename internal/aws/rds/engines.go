package rds

// The engines RDS's API serves beyond PostgreSQL, MySQL and MariaDB:
// DocumentDB (on MongoDB), Neptune (on Gremlin Server) and SQL Server.

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

var sqlVersions = []string{"16.00.4185.3.v1", "15.00.4415.2.v1"}

// isMaster is a legacy OP_QUERY {isMaster: 1} on admin.$cmd, which every MongoDB answers.
const isMaster = "\x3a\x00\x00\x00" + // length 58
	"\x01\x00\x00\x00\x00\x00\x00\x00\xd4\x07\x00\x00" + // request 1, response to 0, OP_QUERY
	"\x00\x00\x00\x00admin.$cmd\x00\x00\x00\x00\x00\xff\xff\xff\xff" + // flags, collection, skip, return
	"\x13\x00\x00\x00\x10isMaster\x00\x01\x00\x00\x00\x00" // {isMaster: 1}

// prelogin is a TDS PRELOGIN packet (a version, no encryption), which SQL Server answers.
const prelogin = "\x12\x01\x00\x1a\x00\x00\x01\x00" + // PRELOGIN, last packet, length 26
	"\x00\x00\x0b\x00\x06\x01\x00\x11\x00\x01\xff" + // VERSION at 11 (6 bytes), ENCRYPTION at 17 (1)
	"\x0f\x00\x07\xd0\x00\x00\x02" // 15.0.2000, ENCRYPT_NOT_SUP

// otherSpec is the container of a DocumentDB, Neptune or SQL Server server.
// ponytail: one image per engine whatever the version: mongo:7 speaks the
// DocumentDB 4.0 and 5.0 APIs, Gremlin Server 3.7 what Neptune 1.3 and 1.4 do.
func otherSpec(engine, user, pw string, host int) (Spec, bool) {
	switch {
	case engine == "docdb":
		return Spec{Image: "mongo:7", Port: 27017, Host: host, Hello: isMaster,
			Env: []string{"MONGO_INITDB_ROOT_USERNAME=" + user, "MONGO_INITDB_ROOT_PASSWORD=" + pw}}, true
	case engine == "neptune":
		// Gremlin over WebSocket and HTTP POST /gremlin, like Neptune's port; no auth.
		// ponytail: no openCypher or SPARQL endpoints, and IAM auth is not enforced.
		return Spec{Image: "tinkerpop/gremlin-server:3.7", Port: 8182, Host: host, Hello: "GET / HTTP/1.0\r\n\r\n",
			Entry: []string{"/bin/sh", "-c", "sed -i s/WebSocketChannelizer/WsAndHttpChannelizer/ conf/gremlin-server.yaml && " +
				"exec /docker-entrypoint.sh conf/gremlin-server.yaml"}}, true
	case strings.HasPrefix(engine, "sqlserver"):
		// The image is amd64 only; Docker Desktop runs it under emulation on Apple Silicon.
		return Spec{Image: "mcr.microsoft.com/mssql/server:2022-latest", Port: 1433, Host: host, Hello: prelogin,
			Env: []string{"ACCEPT_EULA=Y", "MSSQL_SA_PASSWORD=" + saPassword(pw)},
			Init: tsql(saPassword(pw), fmt.Sprintf("IF SUSER_ID(N'%[1]s') IS NULL CREATE LOGIN [%[1]s] WITH PASSWORD = N'%[2]s', CHECK_POLICY = OFF; "+
				"ALTER SERVER ROLE sysadmin ADD MEMBER [%[1]s]", user, sqlQuote(pw)))}, true
	}
	return Spec{}, false
}

// otherAlter is the command that changes the master password in a server of
// otherSpec; nil when there is none (Neptune).
func otherAlter(engine, user, old, pw string) ([]string, bool) {
	switch {
	case engine == "docdb":
		u, _ := json.Marshal(user)
		p, _ := json.Marshal(pw)
		return []string{"mongosh", "admin", "--quiet", "-u", user, "-p", old, "--eval", "db.changeUserPassword(" + string(u) + "," + string(p) + ")"}, true
	case engine == "neptune":
		return nil, true
	case strings.HasPrefix(engine, "sqlserver"):
		return tsql(saPassword(old), fmt.Sprintf("ALTER LOGIN [%s] WITH PASSWORD = N'%s', CHECK_POLICY = OFF; ALTER LOGIN [sa] WITH PASSWORD = N'%s'",
			user, sqlQuote(pw), sqlQuote(saPassword(pw)))), true
	}
	return nil, false
}

// saPassword is the sa password of a SQL Server whose master user has pw: the
// master user gets its own login, as on RDS, and sa a password that always
// meets SQL Server's policy (RDS master passwords need not).
func saPassword(pw string) string { return "Pogi-" + pw }

func sqlQuote(s string) string { return strings.ReplaceAll(s, "'", "''") }

// tsql runs batch as sa; SQL Server listens a little before it takes logins, so it retries for a minute.
func tsql(sa, batch string) []string {
	return []string{"sh", "-c", `for i in $(seq 60); do /opt/mssql-tools18/bin/sqlcmd -C -b -S localhost -U sa -P "$1" -Q "$2" && exit; sleep 1; done; exit 1`,
		"sh", sa, batch}
}

// describeGlobalClusters: pogi has no global clusters, but the DocumentDB and
// Neptune Terraform resources look for the one a cluster is in.
func (s *Service) describeGlobalClusters(c *core.Ctx, in *struct{ GlobalClusterIdentifier, Marker string }) (any, error) {
	if in.GlobalClusterIdentifier != "" {
		return nil, core.Errorf(404, "GlobalClusterNotFoundFault", "Global cluster %s not found.", in.GlobalClusterIdentifier)
	}
	return map[string]any{"GlobalClusters": []struct{}{}}, nil
}
