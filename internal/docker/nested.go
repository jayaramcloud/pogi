package docker

// Support for a pogi that runs inside a container and starts its containers
// through the engine's socket ("nested"). Those containers are siblings, not
// children: the engine sees the host's folders and networks, not pogi's. So
// when nested, pogi:
//   - joins the pogi network, and tells containers to reach it at its address there (HostAddr);
//   - reaches its containers at their address on that network, not at a host port (Addr);
//   - rewrites binds whose source is a folder of its own container, so the engine finds the files.
// Not nested, none of this applies.

import (
	"archive/tar"
	"context"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Where a container (not the host) shows itself; vars so a test can point them elsewhere.
var (
	containerMarks = []string{"/.dockerenv", "/run/.containerenv"} // Docker; Podman
	mountInfoFile  = "/proc/self/mountinfo"
)

// Mount is a mount of a container, as Inspect reports it.
type Mount struct {
	Type        string // bind, volume, tmpfs
	Name        string // of a volume
	Source      string // on the engine's host
	Destination string // in the container
}

// self is pogi's own container, when pogi runs in one.
type self struct {
	ip     string // on the pogi network
	mounts []Mount
}

// nest returns pogi's own container, or nil when pogi does not run in a
// container of the engine it uses (or no engine answers yet). It joins the
// pogi network once.
func (c *Client) nest() *self {
	if c.find() == nil {
		return nil
	}
	c.nestMu.Lock()
	defer c.nestMu.Unlock()
	if !c.nestDone {
		c.self, c.nestDone = c.detect(), true
	}
	return c.self
}

// detect finds pogi's own container and joins it to the pogi network.
func (c *Client) detect() *self {
	inside := false
	for _, f := range containerMarks {
		if _, err := os.Stat(f); err == nil {
			inside = true
		}
	}
	if !inside {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	host, _ := os.Hostname()
	mi, _ := os.ReadFile(mountInfoFile)
	for _, id := range selfIDs(string(mi), host) {
		info, err := c.Inspect(ctx, id)
		if err != nil {
			continue // not a container of this engine
		}
		if err := c.ensureNetwork(ctx); err != nil {
			c.opt.Log.Warn("pogi runs in a container but cannot make its network; containers it starts cannot reach it", "err", err)
			return nil
		}
		err = c.do(ctx, "POST", "/networks/"+c.opt.Network+"/connect", map[string]string{"Container": info.ID}, nil)
		if err != nil && status(err) != http.StatusForbidden { // 403: already connected
			c.opt.Log.Warn("cannot join the pogi network", "err", err)
			return nil
		}
		if info, err = c.Inspect(ctx, info.ID); err != nil {
			return nil
		}
		c.opt.Log.Info("pogi runs in a container; containers it starts are siblings on the "+c.opt.Network+" network", "container", info.Name)
		return &self{ip: info.NetworkSettings.Networks[c.opt.Network].IPAddress, mounts: info.Mounts}
	}
	return nil
}

var containerDir = regexp.MustCompile(`/containers/([0-9a-f]{64})/`)

// selfIDs are the names this process's container may go by in the engine: the
// id in its mount table (/var/lib/docker/containers/<id>/hostname), then the hostname
// (the short id, unless --hostname changed it).
func selfIDs(mountinfo, hostname string) []string {
	var out []string
	if m := containerDir.FindStringSubmatch(mountinfo); m != nil {
		out = append(out, m[1])
	}
	if hostname != "" {
		out = append(out, hostname)
	}
	return out
}

// ListenAddr is the address to listen on for something a container calls:
// pogi's address on the pogi network when nested, else loopback.
func (c *Client) ListenAddr() string {
	if s := c.nest(); s != nil && s.ip != "" {
		return s.ip
	}
	return "127.0.0.1"
}

// Addr is host:port for a container's tcp port that was published on hostPort
// of the host. Nested, that port is not reachable from pogi's container, so it
// is the container's address on the pogi network and its own port.
func (c *Client) Addr(hostPort int) string {
	if c.nest() != nil {
		if a := c.childAddr(hostPort); a != "" {
			return a
		}
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(hostPort))
}

// childAddr looks up (and remembers, until a container changes) where the container that publishes hostPort is.
func (c *Client) childAddr(hostPort int) string {
	c.mu.Lock()
	a, ok := c.addrs[hostPort]
	c.mu.Unlock()
	if ok {
		return a
	}
	list, err := c.List(context.Background(), LabelManaged+"=true")
	if err != nil {
		return ""
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addrs = map[int]string{}
	for _, s := range list {
		ip := s.NetworkSettings.Networks[c.opt.Network].IPAddress
		for _, p := range s.Ports {
			if ip != "" && p.PublicPort != 0 && p.Type == "tcp" {
				c.addrs[p.PublicPort] = net.JoinHostPort(ip, strconv.Itoa(p.PrivatePort))
			}
		}
	}
	return c.addrs[hostPort]
}

// forget drops the remembered container addresses; they may have changed.
func (c *Client) forget() {
	c.mu.Lock()
	c.addrs = nil
	c.mu.Unlock()
}

// plan says how to give a container the folder that a bind names.
type plan struct {
	bind  string         // the bind to use instead
	mount map[string]any // or a mount (a folder of a volume)
	copy  bool           // or neither: copy the files in
}

// rebind plans a bind whose source (src, with dst and opts the rest) is a path in pogi's container:
// where pogi's mounts put it on the host. subpath: the engine can mount a folder of a volume.
func rebind(src, dst, opts string, mounts []Mount, subpath bool) plan {
	var m *Mount
	for i := range mounts {
		d := mounts[i].Destination
		if (src == d || strings.HasPrefix(src, strings.TrimSuffix(d, "/")+"/")) && d != "/" && (m == nil || len(d) > len(m.Destination)) {
			m = &mounts[i]
		}
	}
	if m == nil {
		return plan{copy: true}
	}
	rel := strings.TrimPrefix(strings.TrimPrefix(src, m.Destination), "/")
	withOpts := func(b string) string {
		if opts != "" {
			b += ":" + opts
		}
		return b
	}
	switch {
	case m.Type == "bind":
		return plan{bind: withOpts(filepath.Join(m.Source, rel) + ":" + dst)}
	case m.Type == "volume" && rel == "":
		return plan{bind: withOpts(m.Name + ":" + dst)}
	case m.Type == "volume" && subpath:
		return plan{mount: map[string]any{"Type": "volume", "Source": m.Name, "Target": dst, "ReadOnly": strings.Contains(opts, "ro"),
			"VolumeOptions": map[string]any{"Subpath": rel}}}
	}
	return plan{copy: true}
}

// rebinds rewrites the binds of a container that name pogi's own folders. It
// returns the binds and mounts for the engine, and the binds to copy in once
// the container exists. Binds without a source in pogi's container (a named
// volume, a path of the host) stay as they are.
func (c *Client) rebinds(cn *conn, binds []string) (keep []string, mounts []map[string]any, copies [][2]string) {
	s := c.nest()
	if s == nil {
		return binds, nil, nil
	}
	_, minor, _ := strings.Cut(cn.API, ".")
	n, _ := strconv.Atoi(minor)
	for _, b := range binds {
		p := strings.SplitN(b, ":", 3)
		if len(p) < 2 || !strings.HasPrefix(p[0], "/") {
			keep = append(keep, b)
			continue
		}
		if _, err := os.Stat(p[0]); err != nil {
			keep = append(keep, b)
			continue
		}
		var opts string
		if len(p) == 3 {
			opts = p[2]
		}
		switch pl := rebind(filepath.Clean(p[0]), p[1], opts, s.mounts, n >= 45); {
		case pl.bind != "":
			keep = append(keep, pl.bind)
		case pl.mount != nil:
			mounts = append(mounts, pl.mount)
		default:
			if !strings.Contains(opts, "ro") {
				c.opt.Log.Warn("a writable folder of pogi's container is outside its mounts, so it is copied into the container; what the container writes there does not come back", "folder", p[0])
			}
			copies = append(copies, [2]string{p[0], p[1]})
		}
	}
	return keep, mounts, copies
}

// copyIn puts the files of src (a file or a folder of pogi's container) at dst in a created container.
// ponytail: a copy is taken once at create; later changes in src do not reach the container,
// and what the container writes does not come back. Put the folder under the data dir (a mount) if that matters.
func (c *Client) copyIn(ctx context.Context, id, src, dst string) error {
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(tarFiles(pw, src, dst)) }()
	defer pr.Close()
	return c.CopyTo(ctx, id, "/", pr)
}

// tarFiles writes src (a file or a folder) as a tar archive of paths under dst, as root.
func tarFiles(w io.Writer, src, dst string) error {
	tw := tar.NewWriter(w)
	dst = strings.TrimPrefix(filepath.Clean(dst), "/")
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		fi, err := d.Info()
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		name := filepath.ToSlash(filepath.Join(dst, rel))
		var link string
		if fi.Mode()&fs.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		}
		h, err := tar.FileInfoHeader(fi, link)
		if err != nil {
			return err
		}
		h.Name, h.Uid, h.Gid, h.Uname, h.Gname = name, 0, 0, "", ""
		if fi.IsDir() {
			h.Name += "/"
		}
		if !fi.Mode().IsRegular() && !fi.IsDir() && link == "" {
			return nil // sockets and the like
		}
		if err := tw.WriteHeader(h); err != nil {
			return err
		}
		if !fi.Mode().IsRegular() {
			return nil
		}
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
	if err != nil {
		return fmt.Errorf("copy %s: %w", src, err)
	}
	return tw.Close()
}
