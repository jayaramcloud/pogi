package generic

import (
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/transfer"
	"github.com/nrndev/pogi/internal/core"
	"golang.org/x/crypto/ssh"
)

// Transfer Family: the servers and users of the control plane are the engine's; an SFTP server
// (package transfer) listens on Config.TransferPort, started on first use, and serves the S3 buckets
// to users that log in with a key they imported. See package transfer for what it does and does not do.
// Log in as <user>, or <serverId>@<user> when several servers have a user of that name; the address
// is in the hub's endpoint "sftp:transfer" and in the _sftp member of a server in the console.
//
// ponytail: after a restart the SFTP port opens again when the API is first used, not before.
func init() {
	arnTemplates["transfer/User"] = "user/{ServerId}/{UserName}"
	OnClose("transfer", func(s *Service) { // the SFTP server stops when the hub shuts down
		if v, ok := sftpServers.LoadAndDelete(s); ok {
			v.(*transfer.Server).Close()
			if addr, ok := s.h.Endpoint("sftp:transfer"); ok {
				_, p, _ := net.SplitHostPort(addr)
				port, _ := strconv.Atoi(p)
				s.h.ClosedPort("transfer", "SFTP", port)
			}
		}
	})

	Override("transfer", "CreateServer", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		def := map[string]any{"State": "ONLINE", "Protocols": []any{"SFTP"}, "IdentityProviderType": "SERVICE_MANAGED",
			"EndpointType": "PUBLIC", "Domain": "S3", "SecurityPolicyName": "TransferSecurityPolicy-2020-06"}
		for k, v := range def {
			if c.Res.Data[k] == nil {
				s2Set(c, nil, map[string]any{k: v})
			}
		}
		if srv := trServer(c); srv != nil {
			addr, _ := c.Hub().Endpoint("sftp:transfer")
			s2Set(c, nil, map[string]any{"HostKeyFingerprint": srv.Fingerprint(), "_sftp": addr})
		}
		return out, nil
	})
	s2After("transfer", "DescribeServer", func(c *Call, out map[string]any) error {
		if srv := s2Map(out["Server"]); srv != nil {
			srv["UserCount"] = float64(len(trUsers(c, str(c.In["ServerId"]))))
		}
		return nil
	})
	s2After("transfer", "ListServers", func(c *Call, out map[string]any) error {
		for _, it := range asList(out["Servers"]) {
			m := it.(map[string]any)
			m["UserCount"] = float64(len(trUsers(c, str(m["ServerId"]))))
		}
		return nil
	})
	for op, state := range map[string]string{"StartServer": "ONLINE", "StopServer": "OFFLINE"} {
		Override("transfer", op, func(c *Call) (map[string]any, error) {
			r := c.Get("Server", str(c.In["ServerId"]))
			if r == nil {
				return nil, c.NotFound("Unknown server")
			}
			r.Data["State"] = state
			if addr, ok := c.Hub().Endpoint("sftp:transfer"); ok {
				r.Data["_sftp"] = addr
			}
			c.Put(r)
			c.Res = r
			return map[string]any{}, nil
		})
	}
	Hook("transfer", "DeleteServer", func(c *Call) {
		for _, u := range trUsers(c, c.Res.ID) {
			c.Svc.Delete(c.Account, c.Region, "User", u.Key)
		}
	})
	for _, op := range []string{"CreateServer", "StartServer", "DescribeServer", "ListServers", "CreateUser"} {
		Hook("transfer", op, func(c *Call) { trServer(c) })
	}

	Override("transfer", "CreateUser", func(c *Call) (map[string]any, error) {
		if c.Get("Server", str(c.In["ServerId"])) == nil {
			return nil, c.NotFound("Unknown server")
		}
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		keys := []any{}
		if body := str(c.In["SshPublicKeyBody"]); body != "" {
			if _, _, _, _, err := ssh.ParseAuthorizedKey([]byte(body)); err != nil {
				c.Svc.Delete(c.Account, c.Region, "User", c.Res.Key)
				return nil, c.Invalid("Unsupported or invalid SSH public key format")
			}
			keys = append(keys, trKey(body))
		}
		s2Set(c, nil, map[string]any{"SshPublicKeys": keys})
		if c.Res.Data["HomeDirectoryType"] == nil {
			s2Set(c, nil, map[string]any{"HomeDirectoryType": "PATH"})
		}
		return out, nil
	})
	s2After("transfer", "ListUsers", func(c *Call, out map[string]any) error {
		for _, it := range asList(out["Users"]) {
			m := it.(map[string]any)
			if u := c.Get("User", str(c.In["ServerId"]), str(m["UserName"])); u != nil {
				m["SshPublicKeyCount"] = float64(len(asList(u.Data["SshPublicKeys"])))
			}
		}
		return nil
	})
	Override("transfer", "ImportSshPublicKey", func(c *Call) (map[string]any, error) {
		u := c.Get("User", str(c.In["ServerId"]), str(c.In["UserName"]))
		if u == nil {
			return nil, c.NotFound("Unknown user")
		}
		body := strings.TrimSpace(str(c.In["SshPublicKeyBody"]))
		if _, _, _, _, err := ssh.ParseAuthorizedKey([]byte(body)); err != nil {
			return nil, c.Invalid("Unsupported or invalid SSH public key format")
		}
		keys := asList(u.Data["SshPublicKeys"])
		for _, k := range keys {
			if str(s2Map(k)["SshPublicKeyBody"]) == body {
				return nil, c.Exists("The key is already imported for user %s", u.Name)
			}
		}
		k := trKey(body)
		u.Data["SshPublicKeys"] = append(keys, k)
		c.Put(u)
		return map[string]any{"ServerId": str(c.In["ServerId"]), "UserName": str(c.In["UserName"]), "SshPublicKeyId": k["SshPublicKeyId"]}, nil
	})
	Override("transfer", "DeleteSshPublicKey", func(c *Call) (map[string]any, error) {
		u := c.Get("User", str(c.In["ServerId"]), str(c.In["UserName"]))
		if u == nil {
			return nil, c.NotFound("Unknown user")
		}
		keys := asList(u.Data["SshPublicKeys"])
		i := slices.IndexFunc(keys, func(k any) bool { return str(s2Map(k)["SshPublicKeyId"]) == str(c.In["SshPublicKeyId"]) })
		if i < 0 {
			return nil, c.NotFound("Unknown key %s", str(c.In["SshPublicKeyId"]))
		}
		u.Data["SshPublicKeys"] = slices.Delete(keys, i, i+1)
		c.Put(u)
		return map[string]any{}, nil
	})
}

func trKey(body string) map[string]any {
	return map[string]any{"SshPublicKeyBody": strings.TrimSpace(body), "SshPublicKeyId": "key-" + randHex(17), "DateImported": s2Time(time.Now())}
}

// trUsers lists the users of a server.
func trUsers(c *Call, server string) []*Resource {
	var out []*Resource
	for _, u := range c.List("User") {
		if u.IDs["ServerId"] == server {
			out = append(out, u)
		}
	}
	return out
}

var sftpServers sync.Map // *Service → *transfer.Server

// trServer returns the SFTP server of this instance, starting it when a server of the service exists.
func trServer(c *Call) *transfer.Server {
	s := c.Svc
	if v, ok := sftpServers.Load(s); ok {
		return v.(*transfer.Server)
	}
	h := c.Hub()
	if h == nil {
		return nil
	}
	srv, err := transfer.New(h, s.sftpAccount)
	if err != nil {
		return nil
	}
	if v, loaded := sftpServers.LoadOrStore(s, srv); loaded {
		return v.(*transfer.Server)
	}
	host := h.Cfg.Host
	if host == "" {
		host = "127.0.0.1"
	}
	addr, err := srv.Listen(host + ":" + str(max(h.Cfg.TransferPort, 0)))
	if err != nil {
		h.Log.Warn("Transfer Family SFTP port unavailable, using a free one", "port", h.Cfg.TransferPort, "err", err)
		addr, err = srv.Listen(host + ":0")
	}
	if err != nil {
		h.Log.Warn("Transfer Family SFTP server did not start", "err", err)
		sftpServers.Delete(s)
		return nil
	}
	h.SetEndpoint("sftp:transfer", addr)
	h.Log.Info("Transfer Family SFTP", "addr", addr)
	return srv
}

// sftpAccount finds the user that is logging in: the first online SFTP server that has the name.
func (s *Service) sftpAccount(login string) *transfer.Account {
	server, user := "", login
	if id, name, ok := strings.Cut(login, "@"); ok && strings.HasPrefix(id, "s-") {
		server, user = id, name
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range core.SortedKeys(s.st.Res) {
		r := s.st.Res[k]
		if r.Type != "User" || r.Name != user || server != "" && r.IDs["ServerId"] != server {
			continue
		}
		srv := s.st.Res[skey(r.Account, r.Region, "Server", r.IDs["ServerId"])]
		if srv == nil || srv.Data["State"] != "ONLINE" || !slices.Contains(s2Names(srv.Data["Protocols"]), "SFTP") ||
			str(srv.Data["IdentityProviderType"]) != "SERVICE_MANAGED" {
			continue
		}
		a := &transfer.Account{Server: srv.ID, User: user, Account: r.Account, Region: r.Region, Home: str(r.Data["HomeDirectory"])}
		for _, k := range asList(r.Data["SshPublicKeys"]) {
			if pk, _, _, _, err := ssh.ParseAuthorizedKey([]byte(str(s2Map(k)["SshPublicKeyBody"]))); err == nil {
				a.Keys = append(a.Keys, pk)
			}
		}
		if str(r.Data["HomeDirectoryType"]) == "LOGICAL" {
			a.Mappings = []transfer.Mapping{}
			for _, m := range asList(r.Data["HomeDirectoryMappings"]) {
				a.Mappings = append(a.Mappings, transfer.Mapping{Entry: str(s2Map(m)["Entry"]), Target: str(s2Map(m)["Target"])})
			}
		}
		return a
	}
	return nil
}
