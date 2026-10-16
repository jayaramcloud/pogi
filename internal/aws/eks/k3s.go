package eks

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/docker"
)

// K3s is a Kubernetes cluster in one privileged k3s container, its API
// server published on a random 127.0.0.1 port. EKS runs one per cluster;
// GKE and AKS can run theirs the same way.
type K3s struct {
	Name     string   // container name
	Image    string   // rancher/k3s:<tag>; docker.images["<Service>.k3s"] overrides it
	Service  string   // pogi.service label
	Resource string   // pogi.resource label
	Token    string   // k3s server token
	Webhook  string   // URL of a TokenReview webhook that authenticates bearer tokens; empty: none
	Args     []string // more k3s server flags
}

// DefaultImage is the k3s image of EKS's default Kubernetes version, which GKE and AKS run too.
func DefaultImage() string { return "rancher/k3s:" + k3sTags[defaultVersion] }

// Run starts the cluster, a fresh container or (restart) the one it already
// has, and waits until its node is Ready and its API server answers on the
// published port. It returns the API server's URL and the PEM of the CA its
// certificate chains to.
func (k *K3s) Run(ctx context.Context, d *docker.Client, restart bool) (endpoint string, ca []byte, err error) {
	if !restart {
		d.Remove(ctx, k.Name) // a leftover of a crashed run
		cmd := append([]string{"server", "--disable=traefik", "--disable=metrics-server", "--disable-network-policy",
			"--tls-san=127.0.0.1", "--tls-san=localhost", "--tls-san=host.docker.internal", "--token=" + k.Token}, k.Args...)
		if k.Webhook != "" {
			cmd = append(cmd, "--kube-apiserver-arg=authentication-token-webhook-config-file=/etc/pogi/authn.yaml")
		}
		// tmpfs on /run like k3d: a restart must not trip over the last run's sockets and pid files
		ct := &docker.Container{Name: k.Name, Image: k.Image, Service: k.Service, Resource: k.Resource, Cmd: cmd,
			Ports: map[int]int{6443: 0}, Privileged: true, Tmpfs: map[string]string{"/run": "", "/var/run": ""}}
		if _, err := d.Create(ctx, ct); err != nil {
			return "", nil, err
		}
	}
	if k.Webhook != "" { // on every start: pogi's port may have changed since
		if err := d.CopyTo(ctx, k.Name, "/etc", webhookConfig(k.Webhook)); err != nil {
			return "", nil, err
		}
	}
	start := d.Start
	if restart {
		start = d.Restart // a running API server reads its webhook config only at start
	}
	if err := start(ctx, k.Name); err != nil {
		return "", nil, err
	}
	end := time.Now().Add(5 * time.Minute)
	err = k.wait(ctx, end, func() (bool, error) {
		out, code, err := d.Exec(ctx, k.Name, "kubectl", "get", "nodes", "-o", `jsonpath={.items[*].status.conditions[?(@.type=="Ready")].status}`)
		return err == nil && code == 0 && strings.TrimSpace(string(out)) == "True", err // err: the container stopped
	})
	if err != nil {
		return "", nil, err
	}
	ca, code, err := d.Exec(ctx, k.Name, "cat", "/var/lib/rancher/k3s/server/tls/server-ca.crt")
	if err == nil && code != 0 {
		err = fmt.Errorf("k3s: reading its CA: %s", ca)
	}
	if err != nil {
		return "", nil, err
	}
	info, err := d.Inspect(ctx, k.Name)
	if err != nil {
		return "", nil, err
	}
	endpoint = "https://127.0.0.1:" + strconv.Itoa(info.HostPort(6443)) // what clients on the host use
	probe := "https://" + d.Addr(info.HostPort(6443))                   // where pogi reaches it (differs when pogi runs in a container)
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(ca)
	hc := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "127.0.0.1"}}}
	defer hc.CloseIdleConnections()
	return endpoint, ca, k.wait(ctx, end, func() (bool, error) { // Docker Desktop forwards the port a moment after a restart
		resp, err := hc.Get(probe + "/readyz")
		if err == nil {
			resp.Body.Close()
		}
		return err == nil, nil
	})
}

// wait polls ready until it is true or fails, ctx ends or end passes.
func (k *K3s) wait(ctx context.Context, end time.Time, ready func() (bool, error)) error {
	for {
		ok, err := ready()
		switch {
		case err != nil:
			return fmt.Errorf("k3s: %w (docker logs %s tells why)", err, k.Name)
		case ok:
			return nil
		case time.Now().After(end):
			return fmt.Errorf("k3s: not ready within 5 minutes (docker logs %s tells why)", k.Name)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// webhookConfig is a tar of pogi/authn.yaml: the kubeconfig that points the
// API server's token webhook at url.
func webhookConfig(url string) *bytes.Buffer {
	cfg := "apiVersion: v1\nkind: Config\nclusters:\n- name: pogi\n  cluster:\n    server: " + url +
		"\nusers:\n- name: kube-apiserver\ncontexts:\n- name: webhook\n  context:\n    cluster: pogi\n    user: kube-apiserver\ncurrent-context: webhook\n"
	var b bytes.Buffer
	tw := tar.NewWriter(&b)
	tw.WriteHeader(&tar.Header{Name: "pogi/authn.yaml", Mode: 0o644, Size: int64(len(cfg))})
	tw.Write([]byte(cfg))
	tw.Close()
	return &b
}
