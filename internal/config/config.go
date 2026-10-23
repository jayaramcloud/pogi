// Package config is the schema of pogi.yaml: server settings, seed
// resources, CloudFormation stacks, chaos rules and IAM identities.
// It imports nothing from pogi so every package can use it.
package config

import (
	"fmt"
	"slices"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// File is a parsed pogi.yaml. Zero values mean "not set": defaults,
// environment variables and flags fill them in (flags > env > file > defaults).
type File struct {
	Path       string     `yaml:"-"` // where it was loaded from, for error messages
	Version    int        `yaml:"version"`
	Server     Server     `yaml:"server"`
	Services   Services   `yaml:"services"`
	Storage    Storage    `yaml:"storage"`
	IAM        IAM        `yaml:"iam"`
	Lambda     Lambda     `yaml:"lambda"`
	Docker     Docker     `yaml:"docker"`
	AI         AI         `yaml:"ai"`
	Email      Email      `yaml:"email"`
	Chaos      []Chaos    `yaml:"chaos"`
	Identities Identities `yaml:"identities"`
	Resources  Resources  `yaml:"resources"`
	Stacks     []Stack    `yaml:"stacks"`
}

// Server is where and how pogi listens.
type Server struct {
	Port          int    `yaml:"port"`
	Host          string `yaml:"host"`          // anything but loopback needs AllowExternal
	Region        string `yaml:"region"`        // the partition follows the region (cn-*, us-gov-*)
	Hostname      string `yaml:"hostname"`      // localhost; aws.localhost enables virtual-host style
	TLS           TLS    `yaml:"tls"`           // empty: pogi's local CA
	CORS          CORS   `yaml:"cors"`          // extra origins allowed to call the APIs from a browser
	AllowExternal bool   `yaml:"allowExternal"` // allow a non-loopback Host
	MaxUploadMB   int    `yaml:"maxUploadMB"`   // largest upload in MiB; 0 means 10
}

// TLS is a custom certificate for the HTTPS side of the main port.
type TLS struct {
	Cert string `yaml:"cert"`
	Key  string `yaml:"key"`
}

// CORS lists browser origins and headers allowed on every API.
type CORS struct {
	Origins []string `yaml:"origins"`
	Headers []string `yaml:"headers"`
}

// Services enables clouds or services ("all", "aws", "s3", …).
type Services struct {
	Enable  []string `yaml:"enable"`
	Disable []string `yaml:"disable"`
}

// Storage is how state is kept: memory | hybrid | persistent | wal.
type Storage struct {
	Mode          string            `yaml:"mode"`
	Dir           string            `yaml:"dir"`
	FlushInterval time.Duration     `yaml:"flushInterval"` // hybrid
	Services      map[string]string `yaml:"services"`      // per-service mode
}

// IAM is policy enforcement (off | soft | strict) and signature checking.
type IAM struct {
	Mode               string `yaml:"mode"`
	ValidateSignatures bool   `yaml:"validateSignatures"`
}

// Lambda is the function executor (auto | native | docker).
type Lambda struct {
	Executor    string        `yaml:"executor"`
	WarmPool    int           `yaml:"warmPool"`
	InitTimeout time.Duration `yaml:"initTimeout"` // longest a new environment may take to start; 0 means 10s, as on AWS
}

// Docker is the container engine pogi drives when one is running.
type Docker struct {
	Host    string            `yaml:"host"`    // empty: auto-detect Docker, Podman, Colima, OrbStack
	Network string            `yaml:"network"` // default "pogi"
	Images  map[string]string `yaml:"images"`  // e.g. rds.postgres: postgres:16-alpine
}

// AI is an optional OpenAI-compatible endpoint (Ollama, LM Studio) that
// Bedrock and the language services answer with.
type AI struct {
	Endpoint string            `yaml:"endpoint"`
	APIKey   string            `yaml:"apiKey"`
	Models   map[string]string `yaml:"models"`  // service model id glob → local model
	Timeout  time.Duration     `yaml:"timeout"` // longest one model call; 0 means 5m
}

// Email is SES's SMTP side: the port mail can be sent to, and an SMTP server
// (Mailpit, MailHog) every message sent through SES also goes to.
type Email struct {
	SMTPPort int    `yaml:"smtpPort"` // 2525 by default; -1 turns it off
	Relay    string `yaml:"relay"`    // smtp://[user:pass@]host:port
}

// Chaos is a fault-injection rule (see core.Rule).
type Chaos struct {
	Name    string        `yaml:"name"`
	Cloud   string        `yaml:"cloud"`
	Service string        `yaml:"service"`
	Op      string        `yaml:"op"`
	Region  string        `yaml:"region"`
	Rate    float64       `yaml:"rate"` // 0..1; 0 means always
	Latency time.Duration `yaml:"latency"`
	Status  int           `yaml:"status"`
	Code    string        `yaml:"code"`
	Message string        `yaml:"message"`
}

// Identities are IAM users and roles created at startup (and after a reset).
type Identities struct {
	AWS AWSIdentities `yaml:"aws"`
}

// AWSIdentities are IAM users (with fixed access keys) and roles.
type AWSIdentities struct {
	Users []User `yaml:"users"`
	Roles []Role `yaml:"roles"`
}

// User is an IAM user; AccessKey and SecretKey make its credentials fixed.
type User struct {
	Name      string         `yaml:"name"`
	Account   string         `yaml:"account"`
	AccessKey string         `yaml:"accessKey"`
	SecretKey string         `yaml:"secretKey"`
	Policies  []string       `yaml:"policies"` // managed policy ARNs
	Inline    map[string]any `yaml:"inline"`   // policy name → policy document
	Groups    []string       `yaml:"groups"`
}

// Role is an IAM role; Trust is a service principal (lambda.amazonaws.com),
// an account id or ARN, or "*".
type Role struct {
	Name     string         `yaml:"name"`
	Account  string         `yaml:"account"`
	Trust    string         `yaml:"trust"`
	Policies []string       `yaml:"policies"`
	Inline   map[string]any `yaml:"inline"`
}

// Resources are seeded at startup and after every reset; existing ones are skipped.
// Friendly entries are keyed cloud → service → kind, e.g. aws.s3.buckets.
type Resources struct {
	AWS   map[string]map[string]Items `yaml:"aws"`
	GCP   map[string]map[string]Items `yaml:"gcp"`
	Azure map[string]map[string]Items `yaml:"azure"`
	Typed []Typed                     `yaml:"typed"`
}

// Typed is a resource in CloudFormation form ("AWS::SNS::Topic"); GCP and
// Azure types follow once the generic engine exists.
type Typed struct {
	Type       string         `yaml:"type"`
	Name       string         `yaml:"name"` // optional logical id
	Account    string         `yaml:"account"`
	Region     string         `yaml:"region"`
	Properties map[string]any `yaml:"properties"`
	Line       int            `yaml:"-"`
}

// Stack is a CloudFormation stack deployed at startup.
type Stack struct {
	Name       string            `yaml:"name"`
	Template   string            `yaml:"template"` // path, relative to the config file
	Parameters map[string]string `yaml:"parameters"`
	Account    string            `yaml:"account"`
	Region     string            `yaml:"region"`
	Line       int               `yaml:"-"`
}

// Item is one friendly resource: a bare name, or a map with a name and properties.
type Item struct {
	Name  string
	Props map[string]any // everything except name
	Line  int
}

// Items accepts three spellings:
//
//	buckets: [uploads, {name: logs, versioning: true}]   # names or maps
//	secrets: {db-password: hunter2}                       # name → value
//	queues:  {jobs: {delaySeconds: 5}}                    # name → properties
type Items []Item

// UnmarshalYAML implements the spellings above.
func (l *Items) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.SequenceNode:
		for _, e := range n.Content {
			it := Item{Line: e.Line}
			switch e.Kind {
			case yaml.ScalarNode:
				it.Name = e.Value
			case yaml.MappingNode:
				if err := e.Decode(&it.Props); err != nil {
					return err
				}
				name, _ := it.Props["name"].(string)
				if name == "" {
					return fmt.Errorf("line %d: missing name", e.Line)
				}
				it.Name = name
				delete(it.Props, "name")
			default:
				return fmt.Errorf("line %d: expected a name or a map", e.Line)
			}
			*l = append(*l, it)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			it := Item{Name: k.Value, Line: k.Line}
			switch v.Kind {
			case yaml.ScalarNode:
				it.Props = map[string]any{"value": v.Value}
			case yaml.MappingNode:
				if err := v.Decode(&it.Props); err != nil {
					return err
				}
			default:
				return fmt.Errorf("line %d: expected a value or a map", v.Line)
			}
			*l = append(*l, it)
		}
	default:
		return fmt.Errorf("line %d: expected a list or a map", n.Line)
	}
	return nil
}

// UnmarshalYAML records the line, for seeding errors.
func (t *Typed) UnmarshalYAML(n *yaml.Node) error {
	type plain Typed
	if err := strict(n, "type", "name", "account", "region", "properties"); err != nil {
		return err
	}
	t.Line = n.Line
	return n.Decode((*plain)(t))
}

// UnmarshalYAML records the line, for deployment errors.
func (s *Stack) UnmarshalYAML(n *yaml.Node) error {
	type plain Stack
	if err := strict(n, "name", "template", "parameters", "account", "region"); err != nil {
		return err
	}
	s.Line = n.Line
	return n.Decode((*plain)(s))
}

// strict rejects unknown keys (Node.Decode does not inherit KnownFields).
func strict(n *yaml.Node, keys ...string) error {
	if n.Kind != yaml.MappingNode {
		return fmt.Errorf("line %d: expected a map", n.Line)
	}
	for i := 0; i < len(n.Content); i += 2 {
		if k := n.Content[i]; !slices.Contains(keys, k.Value) {
			return fmt.Errorf("line %d: unknown field %q (expected one of %s)", k.Line, k.Value, strings.Join(keys, ", "))
		}
	}
	return nil
}
