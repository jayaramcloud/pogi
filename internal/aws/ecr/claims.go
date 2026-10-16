package ecr

// The registry as the other clouds' container registries use it (core.ImageRegistry):
// Azure Container Registry <name>.azurecr.localhost:<port> is the names "<name>/...",
// Artifact Registry <region>-docker.pkg.dev/<project>/<repo> the names "<project>/<repo>/...".

import (
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

var _ core.ImageRegistry = (*Service)(nil)

// acrHost is a Container Registry's own host name, whose name prefixes every repository.
var acrHost = regexp.MustCompile(`^([a-z0-9]+)\.azurecr\.localhost(:\d+)?$`)

// hostPrefix is "<registry>/" for a request to a Container Registry's host, else "".
func hostPrefix(host string) string {
	if m := acrHost.FindStringSubmatch(strings.ToLower(host)); m != nil {
		return m[1] + "/"
	}
	return ""
}

func (s *Service) Claim(prefix string) {
	s.mu.Lock()
	s.claims[strings.ToLower(prefix)] = true
	s.mu.Unlock()
	s.expose()
}

func (s *Service) Release(prefix string) {
	prefix = strings.ToLower(prefix)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.claims, prefix)
	for n := range s.st.Ext {
		if strings.HasPrefix(n, prefix+"/") {
			delete(s.st.Ext, n)
		}
	}
}

func (s *Service) Images(prefix string) []core.RegistryImage {
	prefix = strings.ToLower(prefix) + "/"
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []core.RegistryImage
	for _, n := range core.SortedKeys(s.st.Ext) {
		name, ok := strings.CutPrefix(n, prefix)
		if !ok {
			continue
		}
		r := s.st.Ext[n]
		for _, d := range core.SortedKeys(r.Images) {
			img := r.Images[d]
			out = append(out, core.RegistryImage{Name: name, Digest: d, MediaType: img.MediaType, Tags: slices.Clone(img.Tags),
				Size: img.Size, Pushed: time.Time(img.Pushed)})
		}
	}
	return out
}

// findExt finds the repository of a name under a claimed prefix, making it when create is set; lock held.
func (s *Service) findExt(name string, create bool) *Repo {
	claimed := false
	for p := range s.claims {
		if len(name) > len(p)+1 && strings.HasPrefix(name, p+"/") {
			claimed = true
			break
		}
	}
	if !claimed {
		return nil
	}
	r := s.st.Ext[name]
	if r == nil && create {
		r = &Repo{repository: repository{RepositoryName: name, ImageTagMutability: "MUTABLE", CreatedAt: core.Time(time.Now().UTC())}}
		s.st.Ext[name] = r
	}
	return r
}
