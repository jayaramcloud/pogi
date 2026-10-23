package main

import "github.com/nrndev/pogi/internal/gcp/generic"

// The generic Google APIs add their own custom endpoints to what "pogi tf --cloud gcp" writes.
func init() {
	for k, v := range generic.Endpoints() {
		googleEndpoints[k] = v
	}
}
