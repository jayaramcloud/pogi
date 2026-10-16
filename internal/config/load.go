package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Find returns the config file to use: explicit, else $POGI_CONFIG, else
// ./pogi.yaml or ./pogi.yml; "" when there is none.
func Find(explicit string) string {
	if explicit != "" {
		return explicit
	}
	if p := os.Getenv("POGI_CONFIG"); p != "" {
		return p
	}
	for _, p := range []string{"pogi.yaml", "pogi.yml"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return ""
}

// Load reads a pogi.yaml: it expands ${VAR} and ${VAR:-default}, rejects
// unknown keys and checks values. Errors read "path:line: message", one per line.
func Load(path string) (*File, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b = []byte(expand(string(b)))
	f := &File{Path: path}
	dec := yaml.NewDecoder(bytes.NewReader(b))
	dec.KnownFields(true)
	if err := dec.Decode(f); err != nil && err != io.EOF { // EOF: empty file
		return nil, yamlErrors(path, err)
	}
	var root yaml.Node
	yaml.Unmarshal(b, &root) // for line numbers; it parsed above
	if err := f.check(&root); err != nil {
		return nil, err
	}
	if f.Version == 0 {
		f.Version = 1
	}
	return f, nil
}

var envRef = regexp.MustCompile(`\$\{(\w+)(?::-([^}]*))?\}`)

// expand replaces ${VAR} and ${VAR:-default} (used when VAR is unset or
// empty); any other $ stays as it is.
func expand(s string) string {
	return envRef.ReplaceAllStringFunc(s, func(m string) string {
		p := envRef.FindStringSubmatch(m)
		if v := os.Getenv(p[1]); v != "" {
			return v
		}
		return p[2]
	})
}

var yamlLine = regexp.MustCompile(`^(?:yaml: )?line (\d+): `)

// yamlErrors rewrites yaml.v3's "line N: message" errors as "path:N: message".
func yamlErrors(path string, err error) error {
	msgs := []string{err.Error()}
	if te, ok := err.(*yaml.TypeError); ok {
		msgs = te.Errors
	}
	for i, m := range msgs {
		if s := yamlLine.FindStringSubmatch(m); s != nil {
			msgs[i] = path + ":" + s[1] + ": " + m[len(s[0]):]
		} else {
			msgs[i] = path + ": " + strings.TrimPrefix(m, "yaml: ")
		}
	}
	return errors.New(strings.Join(msgs, "\n"))
}

var accessKey = regexp.MustCompile(`^[A-Z0-9]{16,128}$`)

// check validates values, citing the line of each bad one.
func (f *File) check(root *yaml.Node) error {
	var errs []string
	bad := func(at, format string, a ...any) {
		errs = append(errs, fmt.Sprintf("%s:%d: %s: %s", f.Path, line(root, at), at, fmt.Sprintf(format, a...)))
	}
	oneOf := func(at, v string, allowed ...string) {
		if v != "" && !slices.Contains(allowed, v) {
			bad(at, "%q is not one of %s", v, strings.Join(allowed, ", "))
		}
	}
	if f.Version != 0 && f.Version != 1 {
		bad("version", "%d is not supported (use 1)", f.Version)
	}
	if p := f.Server.Port; p < 0 || p > 65535 {
		bad("server.port", "%d is not a port", p)
	}
	modes := []string{"memory", "hybrid", "persistent", "wal"}
	oneOf("storage.mode", f.Storage.Mode, modes...)
	for _, k := range slices.Sorted(maps.Keys(f.Storage.Services)) {
		oneOf("storage.services."+k, f.Storage.Services[k], modes...)
	}
	oneOf("iam.mode", f.IAM.Mode, "off", "soft", "strict")
	oneOf("lambda.executor", f.Lambda.Executor, "auto", "native", "docker")
	if p := f.Email.SMTPPort; p < -1 || p > 65535 {
		bad("email.smtpPort", "%d is not a port (-1 turns SMTP off)", p)
	}
	if err := CheckRelay(f.Email.Relay); err != nil {
		bad("email.relay", "%v", err)
	}
	for i, c := range f.Chaos {
		at := "chaos." + strconv.Itoa(i)
		if c.Rate < 0 || c.Rate > 1 {
			bad(at+".rate", "%v is not between 0 and 1", c.Rate)
		}
		if c.Status != 0 && (c.Status < 400 || c.Status > 599) {
			bad(at+".status", "%d is not an error status (400-599)", c.Status)
		}
	}
	for i, t := range f.Resources.Typed {
		if t.Type == "" {
			bad("resources.typed."+strconv.Itoa(i), "missing type")
		}
	}
	for i, s := range f.Stacks {
		at := "stacks." + strconv.Itoa(i)
		if s.Name == "" {
			bad(at, "missing name")
		}
		tpl := s.Template
		if !filepath.IsAbs(tpl) {
			tpl = filepath.Join(filepath.Dir(f.Path), tpl) // relative to this file
		}
		if s.Template == "" {
			bad(at, "missing template")
		} else if _, err := os.Stat(tpl); err != nil {
			bad(at+".template", "%v", err)
		}
	}
	users := map[string]bool{}
	for i, u := range f.Identities.AWS.Users {
		at := "identities.aws.users." + strconv.Itoa(i)
		if u.Name == "" {
			bad(at, "missing name")
		} else if users[u.Name] {
			bad(at+".name", "user %q is defined twice", u.Name)
		}
		users[u.Name] = true
		if u.AccessKey != "" && !accessKey.MatchString(u.AccessKey) {
			bad(at+".accessKey", "%q is not 16-128 upper-case letters and digits", u.AccessKey)
		}
		if u.AccessKey != "" && u.SecretKey == "" {
			bad(at, "accessKey needs a secretKey")
		}
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}

// CheckRelay checks an email relay URL, smtp://[user:pass@]host[:port]; empty is none.
func CheckRelay(relay string) error {
	if u, err := url.Parse(relay); relay != "" && (err != nil || u.Scheme != "smtp" || u.Hostname() == "") {
		return fmt.Errorf("%q is not smtp://[user:pass@]host[:port]", relay)
	}
	return nil
}

// line is the line of the value at a dotted path ("chaos.0.rate"), or of its
// closest parent when the rest is missing.
func line(n *yaml.Node, path string) int {
	if n.Kind == yaml.DocumentNode && len(n.Content) > 0 {
		n = n.Content[0]
	}
	for k := range strings.SplitSeq(path, ".") {
		if n.Kind == yaml.AliasNode {
			n = n.Alias
		}
		var next *yaml.Node
		switch i, err := strconv.Atoi(k); {
		case n.Kind == yaml.MappingNode:
			for j := 0; j+1 < len(n.Content); j += 2 {
				if n.Content[j].Value == k {
					next = n.Content[j+1]
				}
			}
		case n.Kind == yaml.SequenceNode && err == nil && i < len(n.Content):
			next = n.Content[i]
		}
		if next == nil {
			break
		}
		n = next
	}
	return n.Line
}

// MarshalYAML writes items in list form, for `pogi config print`.
func (l Items) MarshalYAML() (any, error) {
	out := make([]any, len(l))
	for i, it := range l {
		out[i] = it.Name
		if len(it.Props) > 0 {
			m := maps.Clone(it.Props)
			m["name"] = it.Name
			out[i] = m
		}
	}
	return out, nil
}
