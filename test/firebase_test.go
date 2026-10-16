package test

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/auth"
	"github.com/nrndev/pogi"
	"google.golang.org/api/option"
)

// TestEmulatorRoutes: Firestore and Firebase Auth both serve under /emulator/v1/ without swallowing each
// other's paths, Cloud KMS is reachable among the other Google services, and the Admin SDK works
// against the whole stack with the variables `pogi env` sets.
func TestEmulatorRoutes(t *testing.T) {
	ctx := context.Background()
	srv := pogi.Start(t, pogi.Config{Services: []string{"gcp"}})
	for _, kv := range srv.Env() {
		if k, v, _ := strings.Cut(kv, "="); k == "FIRESTORE_EMULATOR_HOST" || k == "FIREBASE_AUTH_EMULATOR_HOST" {
			t.Setenv(k, v)
		}
	}
	fs, err := firestore.NewClient(ctx, "proj")
	if err != nil {
		t.Fatal(err)
	}
	defer fs.Close()
	app, err := firebase.NewApp(ctx, &firebase.Config{ProjectID: "proj"}, option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	ac, err := app.Auth(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.Doc("users/ada").Set(ctx, map[string]any{"n": 1}); err != nil {
		t.Fatal(err)
	}
	u, err := ac.CreateUser(ctx, (&auth.UserToCreate{}).Email("ada@example.com").Password("secret1"))
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path string) (int, string) {
		t.Helper()
		req, _ := http.NewRequest(method, srv.URL+path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, _ := call("DELETE", "/emulator/v1/projects/proj/databases/(default)/documents"); code != 200 {
		t.Fatalf("clearing Firestore = %d", code)
	}
	if _, err := ac.GetUser(ctx, u.UID); err != nil {
		t.Fatalf("clearing Firestore must not clear Auth accounts: %v", err)
	}
	if snap, _ := fs.Doc("users/ada").Get(ctx); snap != nil && snap.Exists() {
		t.Fatal("the Firestore document survived its clear")
	}
	if code, _ := call("DELETE", "/emulator/v1/projects/proj/accounts"); code != 200 {
		t.Fatalf("clearing Auth = %d", code)
	}
	if _, err := ac.GetUser(ctx, u.UID); !auth.IsUserNotFound(err) {
		t.Fatalf("Auth accounts survived their clear: %v", err)
	}
	if code, _ := call("GET", "/emulator/v1/projects/proj/unknown"); code != 404 {
		t.Fatalf("unknown emulator path = %d", code)
	}

	// A password reset shows up in the console inbox.
	if _, err := ac.CreateUser(ctx, (&auth.UserToCreate{}).Email("grace@example.com")); err != nil {
		t.Fatal(err)
	}
	if _, err := ac.PasswordResetLink(ctx, "grace@example.com"); err != nil {
		t.Fatal(err)
	}
	if code, page := call("GET", "/_pogi/inbox"); code != 200 || !strings.Contains(page, "grace@example.com") || !strings.Contains(page, "firebaseauth") {
		t.Fatalf("inbox = %d %.200s", code, page)
	}

	// Cloud KMS is not swallowed by Cloud Storage's matcher.
	req, _ := http.NewRequest("POST", srv.URL+"/v1/projects/proj/locations/global/keyRings?keyRingId=ring", strings.NewReader("{}"))
	req.Header.Set("X-Goog-Api-Client", "gl-go/1.26 gccl/1.0")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !strings.Contains(string(b), "keyRings/ring") {
		t.Fatalf("create key ring = %d %s", resp.StatusCode, b)
	}
}
