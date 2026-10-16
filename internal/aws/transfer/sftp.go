// Package transfer is the SFTP server of AWS Transfer Family: users log in with an SSH key and see
// S3 objects as files. The control plane (servers, users, keys) lives in the generic engine, which
// hands this package a lookup of the user that is logging in; files are read and written through
// pogi's S3 with the hub, so nothing here imports the s3 package.
package transfer

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3io"
	"github.com/nrndev/pogi/internal/core"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// Mapping is one entry of a LOGICAL home directory: the path the user sees and the S3 path behind it.
type Mapping struct{ Entry, Target string }

// Account is a user of a server, as the control plane stores it.
type Account struct {
	Server, User    string
	Account, Region string
	Keys            []ssh.PublicKey
	Home            string    // PATH home directory: /bucket/prefix
	Mappings        []Mapping // LOGICAL home directory
}

// Server accepts SSH connections and serves SFTP to the users the lookup finds.
//
// ponytail: one listener for every server of the instance (a user name is taken from the first online server
// that has it, or <serverId>@<name> picks one); a file is held in memory while it is written, and read
// from S3 in ranges; an IAM role or policy of the user is not enforced; password logins do not exist.
type Server struct {
	hub    *core.Hub
	lookup func(user string) *Account
	cfg    *ssh.ServerConfig
	key    ssh.Signer

	mu sync.Mutex
	ln net.Listener
}

// New makes a server; Listen starts it.
func New(h *core.Hub, lookup func(user string) *Account) (*Server, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	key, err := ssh.NewSignerFromKey(priv)
	if err != nil {
		return nil, err
	}
	s := &Server{hub: h, lookup: lookup, key: key}
	s.cfg = &ssh.ServerConfig{PublicKeyCallback: s.auth}
	s.cfg.AddHostKey(key)
	return s, nil
}

// Fingerprint is the host key fingerprint, as DescribeServer shows it.
func (s *Server) Fingerprint() string { return ssh.FingerprintSHA256(s.key.PublicKey()) }

// Listen serves on addr (a port of 0 takes a free one) and returns the address.
func (s *Server) Listen(addr string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return s.ln.Addr().String(), nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	s.ln = ln
	core.Go("transfer.Listen", func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			core.Go("transfer.serve", func() { s.serve(c) })
		}
	})
	return ln.Addr().String(), nil
}

// Close stops listening; connections that are open finish on their own.
func (s *Server) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		s.ln.Close()
		s.ln = nil
	}
}

func (s *Server) auth(conn ssh.ConnMetadata, key ssh.PublicKey) (*ssh.Permissions, error) {
	a := s.lookup(conn.User())
	if a != nil {
		for _, k := range a.Keys {
			if bytes.Equal(k.Marshal(), key.Marshal()) {
				return &ssh.Permissions{Extensions: map[string]string{"user": conn.User()}}, nil
			}
		}
	}
	return nil, errors.New("permission denied")
}

func (s *Server) serve(nc net.Conn) {
	defer nc.Close()
	nc.SetDeadline(time.Now().Add(30 * time.Second)) // the handshake
	conn, chans, reqs, err := ssh.NewServerConn(nc, s.cfg)
	if err != nil {
		return
	}
	nc.SetDeadline(time.Time{})
	defer conn.Close()
	go ssh.DiscardRequests(reqs)
	a := s.lookup(conn.Permissions.Extensions["user"])
	for nch := range chans {
		if nch.ChannelType() != "session" || a == nil {
			nch.Reject(ssh.UnknownChannelType, "only sftp sessions")
			continue
		}
		ch, creqs, err := nch.Accept()
		if err != nil {
			return
		}
		core.Go("transfer.serve", func() {
			for r := range creqs {
				ok := r.Type == "subsystem" && len(r.Payload) > 4 && string(r.Payload[4:]) == "sftp"
				r.Reply(ok, nil)
				if ok {
					h := &fs{s3: s3io.Client{Hub: s.hub, Account: a.Account, Region: a.Region}, a: a}
					srv := sftp.NewRequestServer(ch, sftp.Handlers{FileGet: h, FilePut: h, FileCmd: h, FileList: h})
					srv.Serve()
					srv.Close()
					return
				}
			}
		})
	}
}

// fs maps the paths of one user's session onto S3.
type fs struct {
	s3 s3io.Client
	a  *Account
}

// target maps what the user sees to an S3 path, as bucket and key.
func (f *fs) target(p string) (bucket, key string, err error) {
	p = path.Clean("/" + p)
	switch {
	case f.a.Mappings != nil:
		best := -1
		for i, m := range f.a.Mappings {
			e := path.Clean("/" + m.Entry)
			if (p == e || e == "/" || strings.HasPrefix(p, e+"/")) && (best < 0 || len(e) > len(path.Clean("/"+f.a.Mappings[best].Entry))) {
				best = i
			}
		}
		if best < 0 {
			return "", "", os.ErrNotExist
		}
		e := path.Clean("/" + f.a.Mappings[best].Entry)
		p = path.Join(f.a.Mappings[best].Target, strings.TrimPrefix(p, e))
	case f.a.Home != "":
		if h := path.Clean("/" + f.a.Home); p != h && !strings.HasPrefix(p, h+"/") { // relative paths are the home directory's
			p = path.Join(h, p)
		}
	}
	bucket, key, _ = strings.Cut(strings.TrimPrefix(path.Clean("/"+p), "/"), "/")
	return bucket, key, nil
}

// virtualDirs are the folders that exist only in the user's view: the first names of the logical entries.
func (f *fs) virtualDirs(p string) ([]string, bool) {
	if f.a.Mappings == nil || p != "/" {
		return nil, false
	}
	var out []string
	for _, m := range f.a.Mappings {
		if e := path.Clean("/" + m.Entry); e == "/" {
			return nil, false // the root is mapped to a real place
		} else if first, _, _ := strings.Cut(e[1:], "/"); !slices.Contains(out, first) {
			out = append(out, first)
		}
	}
	return out, true
}

// isRoot says whether a path is the top of the user's tree: it exists even when empty.
func (f *fs) isRoot(p string) bool {
	p = path.Clean("/" + p)
	if p == "/" {
		return true
	}
	if f.a.Home != "" && f.a.Mappings == nil {
		return p == path.Clean("/"+f.a.Home)
	}
	for _, m := range f.a.Mappings {
		if path.Clean("/"+m.Entry) == p {
			return true
		}
	}
	return false
}

func (f *fs) Fileread(r *sftp.Request) (io.ReaderAt, error) {
	b, k, err := f.target(r.Filepath)
	if err != nil || k == "" {
		return nil, os.ErrNotExist
	}
	o, err := f.s3.Head(b, k)
	if err != nil {
		return nil, notExist(err)
	}
	return &reader{f.s3, b, k, o.Size}, nil
}

type reader struct {
	s3     s3io.Client
	bucket string
	key    string
	size   int64
}

func (r *reader) ReadAt(p []byte, off int64) (int, error) {
	if off >= r.size {
		return 0, io.EOF
	}
	b, err := r.s3.Get(r.bucket, r.key, off, int64(len(p)))
	if err != nil {
		return 0, err
	}
	n := copy(p, b)
	if off+int64(n) >= r.size {
		return n, io.EOF
	}
	return n, nil
}

func (f *fs) Filewrite(r *sftp.Request) (io.WriterAt, error) {
	b, k, err := f.target(r.Filepath)
	if err != nil || k == "" || strings.HasSuffix(k, "/") {
		return nil, os.ErrPermission
	}
	return &writer{s3: f.s3, bucket: b, key: k, max: f.s3.Hub.MaxUpload(), hub: f.s3.Hub}, nil
}

// writer collects a file and puts it in S3 when the client closes it.
type writer struct {
	s3     s3io.Client
	bucket string
	key    string
	max    int64 // the largest file: the hub's upload limit
	hub    *core.Hub

	mu  sync.Mutex
	buf []byte
}

func (w *writer) WriteAt(p []byte, off int64) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	// A client picks the offset, so check it before it sizes a buffer.
	if off < 0 {
		return 0, fmt.Errorf("write at offset %d is outside the file", off)
	}
	if off > w.max || int64(len(p)) > w.max-off {
		return 0, errors.New(w.hub.TooBig("transfer", w.bucket+"/"+w.key, off+int64(len(p)), w.max))
	}
	if end := int(off) + len(p); end > len(w.buf) {
		w.buf = append(w.buf, make([]byte, end-len(w.buf))...)
	}
	return copy(w.buf[off:], p), nil
}

func (w *writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.s3.Put(w.bucket, w.key, w.buf)
}

func (f *fs) Filecmd(r *sftp.Request) error {
	b, k, err := f.target(r.Filepath)
	if err != nil {
		return err
	}
	switch r.Method {
	case "Setstat": // times and modes mean nothing in S3
		return nil
	case "Mkdir":
		if k == "" {
			return os.ErrPermission
		}
		return f.s3.Put(b, k+"/", nil)
	case "Remove":
		if _, err := f.s3.Head(b, k); err != nil {
			return notExist(err)
		}
		return f.s3.Delete(b, k)
	case "Rmdir":
		objs, dirs, err := f.s3.List(b, k+"/", "/")
		if err != nil {
			return err
		}
		if len(objs)+len(dirs) == 0 {
			return os.ErrNotExist
		}
		if len(dirs) > 0 || len(objs) > 1 || objs[0].Key != k+"/" {
			return errors.New("directory not empty")
		}
		return f.s3.Delete(b, k+"/")
	case "Rename", "PosixRename":
		tb, tk, err := f.target(r.Target)
		if err != nil {
			return err
		}
		return f.rename(b, k, tb, tk)
	}
	return sftp.ErrSSHFxOpUnsupported
}

// rename moves an object, or every object under a folder.
func (f *fs) rename(b, k, tb, tk string) error {
	if _, err := f.s3.Head(b, k); err == nil {
		data, err := f.s3.Get(b, k, 0, -1)
		if err != nil {
			return err
		}
		if err := f.s3.Put(tb, tk, data); err != nil {
			return err
		}
		return f.s3.Delete(b, k)
	}
	objs, _, err := f.s3.List(b, k+"/", "")
	if err != nil || len(objs) == 0 {
		return os.ErrNotExist
	}
	for _, o := range objs {
		data, err := f.s3.Get(b, o.Key, 0, -1)
		if err != nil {
			return err
		}
		if err := f.s3.Put(tb, tk+"/"+strings.TrimPrefix(o.Key, k+"/"), data); err != nil {
			return err
		}
		if err := f.s3.Delete(b, o.Key); err != nil {
			return err
		}
	}
	return nil
}

func (f *fs) Filelist(r *sftp.Request) (sftp.ListerAt, error) {
	p := path.Clean("/" + r.Filepath)
	switch r.Method {
	case "Stat":
		fi, err := f.stat(p)
		if err != nil {
			return nil, err
		}
		return lister{fi}, nil
	case "List":
		var out []os.FileInfo
		if names, ok := f.virtualDirs(p); ok {
			for _, n := range names {
				out = append(out, info{name: n, dir: true})
			}
			return lister(out), nil
		}
		b, k, err := f.target(p)
		if err != nil {
			return nil, err
		}
		if k != "" {
			k += "/"
		}
		if b == "" {
			return lister(nil), nil // no home directory: no buckets to show
		}
		objs, dirs, err := f.s3.List(b, k, "/")
		if err != nil {
			return nil, notExist(err)
		}
		for _, o := range objs {
			if o.Key != k {
				out = append(out, info{name: path.Base(o.Key), size: o.Size, mod: o.Modified})
			}
		}
		for _, d := range dirs {
			out = append(out, info{name: path.Base(d), dir: true})
		}
		if len(out) == 0 && k != "" && !f.isRoot(p) {
			return nil, os.ErrNotExist
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Name() < out[j].Name() })
		return lister(out), nil
	}
	return nil, sftp.ErrSSHFxOpUnsupported
}

func (f *fs) stat(p string) (os.FileInfo, error) {
	if _, ok := f.virtualDirs(p); ok || f.isRoot(p) {
		return info{name: path.Base(p), dir: true}, nil
	}
	b, k, err := f.target(p)
	if err != nil || b == "" {
		return nil, os.ErrNotExist
	}
	if k == "" {
		if f.s3.BucketExists(b) {
			return info{name: b, dir: true}, nil
		}
		return nil, os.ErrNotExist
	}
	if o, err := f.s3.Head(b, k); err == nil {
		return info{name: path.Base(k), size: o.Size, mod: o.Modified}, nil
	}
	if objs, dirs, err := f.s3.List(b, k+"/", "/"); err == nil && len(objs)+len(dirs) > 0 {
		return info{name: path.Base(k), dir: true}, nil
	}
	return nil, os.ErrNotExist
}

func notExist(err error) error {
	if errors.Is(err, s3io.ErrNotFound) {
		return os.ErrNotExist
	}
	return err
}

type lister []os.FileInfo

func (l lister) ListAt(ls []os.FileInfo, off int64) (int, error) {
	if off >= int64(len(l)) {
		return 0, io.EOF
	}
	n := copy(ls, l[off:])
	if int(off)+n >= len(l) {
		return n, io.EOF
	}
	return n, nil
}

type info struct {
	name string
	size int64
	dir  bool
	mod  time.Time
}

func (i info) Name() string { return i.name }
func (i info) Size() int64  { return i.size }
func (i info) Mode() os.FileMode {
	if i.dir {
		return os.ModeDir | 0o755
	}
	return 0o644
}
func (i info) ModTime() time.Time {
	if i.mod.IsZero() {
		return time.Now()
	}
	return i.mod
}
func (i info) IsDir() bool { return i.dir }
func (i info) Sys() any    { return nil }
