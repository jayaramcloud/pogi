package docker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// auth is the X-Registry-Auth header that pulls image with the credentials
// the docker CLI would use, from $DOCKER_CONFIG or ~/.docker/config.json: the
// credential helper it names for the registry (credHelpers, else credsStore),
// else its auths entry. "" pulls anonymously.
func auth(image string) string {
	dir := os.Getenv("DOCKER_CONFIG")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".docker")
	}
	b, err := os.ReadFile(filepath.Join(dir, "config.json"))
	if err != nil {
		return ""
	}
	var cfg struct {
		Auths map[string]struct {
			Auth          string `json:"auth"`
			IdentityToken string `json:"identitytoken"`
		} `json:"auths"`
		CredsStore  string            `json:"credsStore"`
		CredHelpers map[string]string `json:"credHelpers"`
	}
	if json.Unmarshal(b, &cfg) != nil {
		return ""
	}
	server := registry(image)
	var a struct {
		Username      string `json:"username,omitempty"`
		Password      string `json:"password,omitempty"`
		IdentityToken string `json:"identitytoken,omitempty"`
		ServerAddress string `json:"serveraddress"`
	}
	helper := cfg.CredsStore
	for k, h := range cfg.CredHelpers {
		if hostOf(k) == hostOf(server) {
			helper = h
		}
	}
	if helper != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "docker-credential-"+helper, "get")
		cmd.Stdin = strings.NewReader(server)
		var c struct{ Username, Secret string }
		if out, err := cmd.Output(); err == nil && json.Unmarshal(out, &c) == nil {
			if c.Username == "<token>" {
				a.IdentityToken = c.Secret
			} else {
				a.Username, a.Password = c.Username, c.Secret
			}
		}
	}
	for k, e := range cfg.Auths {
		if a.Username == "" && a.IdentityToken == "" && hostOf(k) == hostOf(server) {
			raw, _ := base64.StdEncoding.DecodeString(e.Auth)
			a.Username, a.Password, _ = strings.Cut(string(raw), ":")
			a.IdentityToken = e.IdentityToken
		}
	}
	if a.Username == "" && a.IdentityToken == "" {
		return ""
	}
	a.ServerAddress = server
	j, _ := json.Marshal(a)
	return base64.URLEncoding.EncodeToString(j)
}

// registry is where an image comes from, as config.json names it: its first
// path part when that is a host, else Docker Hub.
func registry(image string) string {
	first, _, ok := strings.Cut(image, "/")
	if !ok || !strings.ContainsAny(first, ".:") && first != "localhost" || hostOf(first) == "index.docker.io" {
		return "https://index.docker.io/v1/"
	}
	return first
}

// hostOf strips a registry key to its host: https://index.docker.io/v1/ is
// index.docker.io, and so are Docker Hub's other names.
func hostOf(key string) string {
	key = strings.TrimPrefix(strings.TrimPrefix(key, "https://"), "http://")
	key, _, _ = strings.Cut(key, "/")
	if key == "docker.io" || key == "registry-1.docker.io" {
		return "index.docker.io"
	}
	return key
}
