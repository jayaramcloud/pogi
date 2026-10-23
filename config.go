package pogi

import (
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/config"
	"github.com/nrndev/pogi/internal/core"
)

// LoadConfig reads a pogi.yaml for New or Start:
//
//	cfg, err := pogi.LoadConfig("testdata/pogi.yaml")
//	srv := pogi.Start(t, cfg)
func LoadConfig(path string) (Config, error) {
	f, err := config.Load(path)
	if err != nil {
		return Config{}, err
	}
	return ConfigFromFile(f), nil
}

// ConfigFromFile maps a pogi.yaml onto a Config. What the file leaves unset
// stays zero, so environment variables, flags and New's defaults fill it in.
func ConfigFromFile(f *config.File) Config {
	s := f.Server
	cfg := Config{
		Port: s.Port, Host: s.Host, Region: s.Region, Hostname: s.Hostname, TLSCert: s.TLS.Cert, TLSKey: s.TLS.Key,
		CORSOrigins: s.CORS.Origins, CORSHeaders: s.CORS.Headers, AllowExternal: s.AllowExternal, MaxUploadMB: s.MaxUploadMB,
		Storage: f.Storage.Mode, DataDir: f.Storage.Dir, FlushInterval: f.Storage.FlushInterval, StorageModes: f.Storage.Services,
		IAM: f.IAM.Mode, ValidateSigs: f.IAM.ValidateSignatures, Lambda: f.Lambda.Executor, WarmPool: f.Lambda.WarmPool, LambdaInit: f.Lambda.InitTimeout,
		DockerHost: f.Docker.Host, DockerNetwork: f.Docker.Network, DockerImages: f.Docker.Images,
		AIEndpoint: f.AI.Endpoint, AIKey: f.AI.APIKey, AIModels: f.AI.Models, AITimeout: f.AI.Timeout, SMTPPort: f.Email.SMTPPort, SMTPRelay: f.Email.Relay, File: f,
	}
	if d := cfg.DataDir; d == "~" || strings.HasPrefix(d, "~/") {
		home, _ := os.UserHomeDir()
		cfg.DataDir = filepath.Join(home, d[1:])
	}
	if len(f.Services.Enable)+len(f.Services.Disable) > 0 {
		cfg.Services = slices.Clone(f.Services.Enable)
		for _, name := range f.Services.Disable {
			cfg.Services = append(cfg.Services, "-"+name)
		}
	}
	return cfg
}

// ServiceInfo is a service pogi can run.
type ServiceInfo struct {
	Name, Cloud string
	Enabled     bool // by the Config given to Services
}

// Services lists every service pogi can run, in routing order.
func Services(cfg Config) []ServiceInfo {
	out := make([]ServiceInfo, len(services))
	for i, f := range services {
		out[i] = ServiceInfo{f.name, f.cloud, enabled(cfg.Services, f.name, f.cloud)}
	}
	return out
}

// addChaos loads the chaos rules of a pogi.yaml; a rule's name is its id,
// and a repeated name gets a random one.
func addChaos(h *core.Hub, f *config.File) {
	if f == nil {
		return
	}
	seen := map[string]bool{}
	for _, c := range f.Chaos {
		id := c.Name
		if seen[id] {
			id = ""
		}
		seen[id] = true
		h.Chaos.Add(core.Rule{ID: id, Cloud: c.Cloud, Service: c.Service, Op: c.Op, Region: c.Region, Probability: c.Rate,
			LatencyMs: int(c.Latency.Milliseconds()), Status: c.Status, Code: c.Code, Message: c.Message})
	}
}
