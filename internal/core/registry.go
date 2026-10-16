package core

import "time"

// ImageRegistry is what the ECR service offers the other clouds' container
// registries (Azure Container Registry, Artifact Registry): its OCI endpoint
// for docker push and pull, under a name prefix of their own such as
// "myregistry" or "project/repository". A push under a claimed prefix makes
// its repository, and ECR's own API never lists them.
type ImageRegistry interface {
	// Claim serves the names under prefix/; it is idempotent and survives ECR's Reset.
	Claim(prefix string)
	// Release stops serving prefix/ and deletes everything pushed under it.
	Release(prefix string)
	// Images lists what was pushed under prefix/, by repository name and digest.
	Images(prefix string) []RegistryImage
}

// RegistryImage is one pushed image of a claimed prefix.
type RegistryImage struct {
	Name      string // the repository below the prefix, "app/web"
	Digest    string
	MediaType string
	Tags      []string
	Size      int64
	Pushed    time.Time
}

// Registry is the shared OCI registry, or nil when ECR is not enabled.
func Registry(h *Hub) ImageRegistry {
	r, _ := h.Service("ecr").(ImageRegistry)
	return r
}
