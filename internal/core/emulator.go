package core

import "net/http"

type emulatorRoute struct {
	match func(*http.Request) bool
	h     http.Handler
}

// Emulator registers a handler for the vendor emulators' admin paths under
// /emulator/v1/ (Firestore's DELETE .../documents, Firebase Auth's accounts,
// oobCodes and config). The first handler whose match accepts the request
// serves it, so each emulator claims its own sub-paths and none swallows another's.
func (h *Hub) Emulator(match func(*http.Request) bool, handler http.Handler) {
	h.emulators = append(h.emulators, emulatorRoute{match, handler})
}

func (h *Hub) serveEmulator(w http.ResponseWriter, r *http.Request) {
	for _, e := range h.emulators {
		if e.match(r) {
			e.h.ServeHTTP(w, r)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
}
