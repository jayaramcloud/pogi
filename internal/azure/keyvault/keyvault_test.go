package keyvault_test

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azkeys"
	"github.com/Azure/azure-sdk-for-go/sdk/security/keyvault/azsecrets"
	"github.com/nrndev/pogi/internal/azure/keyvault"
	"github.com/nrndev/pogi/internal/core"
)

var ctx = context.Background()

// token is a fake credential: pogi accepts any bearer token.
type token struct{}

func (token) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "pogi", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// wantErr checks the HTTP status and Key Vault error code of an SDK error.
func wantErr(t *testing.T, err error, status int, code string) {
	t.Helper()
	var re *azcore.ResponseError
	if !errors.As(err, &re) || re.StatusCode != status || re.ErrorCode != code {
		t.Fatalf("want %d %s, got %v", status, code, err)
	}
}

// serve runs Key Vault behind the whole gateway over HTTPS. The client trusts
// pogi's CA and reaches it for any host name, so <vault>.vault.localhost works.
func serve(t *testing.T) (*core.Hub, string, azcore.ClientOptions) {
	h, _ := core.ServeTest(t, func(h *core.Hub) core.Service { return keyvault.New(h) })
	url, pool := core.ListenTLS(t, h, h)
	addr := "127.0.0.1" + url[strings.LastIndex(url, ":"):]
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: pool, ServerName: "localhost"},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}
	t.Cleanup(tr.CloseIdleConnections)
	return h, url, azcore.ClientOptions{Transport: &http.Client{Transport: tr}}
}

func secretsClient(url string, opts azcore.ClientOptions) *azsecrets.Client {
	return must(azsecrets.NewClient(url, token{}, &azsecrets.ClientOptions{ClientOptions: opts, DisableChallengeResourceVerification: true}))
}

func keysClient(url string, opts azcore.ClientOptions) *azkeys.Client {
	return must(azkeys.NewClient(url, token{}, &azkeys.ClientOptions{ClientOptions: opts, DisableChallengeResourceVerification: true}))
}

func TestSecrets(t *testing.T) {
	h, url, opts := serve(t)
	c := secretsClient(url, opts)

	v1 := must(c.SetSecret(ctx, "db-pass", azsecrets.SetSecretParameters{Value: to.Ptr("s3cret"), ContentType: to.Ptr("text/plain"),
		Tags: map[string]*string{"env": to.Ptr("dev")}}, nil))
	// The SDK first went anonymous, got the challenge, then retried with its token.
	if log := h.Requests.Query(0, 0, "keyvault", "", false); len(log) != 2 || log[0].Status != 401 || log[0].Op != "SetSecret" || log[1].Status != 200 {
		t.Fatalf("challenge flow = %+v", log)
	}
	if *v1.Value != "s3cret" || v1.ID.Name() != "db-pass" || len(v1.ID.Version()) != 32 || !strings.HasPrefix(string(*v1.ID), url+"/secrets/db-pass/") ||
		*v1.Attributes.RecoveryLevel != "Recoverable+Purgeable" || *v1.Attributes.RecoverableDays != 90 || v1.Attributes.Created == nil {
		t.Fatalf("set = %+v", v1)
	}
	v2 := must(c.SetSecret(ctx, "db-pass", azsecrets.SetSecretParameters{Value: to.Ptr("rotated")}, nil))
	if got := must(c.GetSecret(ctx, "DB-Pass", "", nil)); *got.Value != "rotated" { // names are case-insensitive; "" is the latest
		t.Fatalf("latest = %q", *got.Value)
	}
	if got := must(c.GetSecret(ctx, "db-pass", v1.ID.Version(), nil)); *got.Value != "s3cret" || *got.ContentType != "text/plain" || *got.Tags["env"] != "dev" {
		t.Fatalf("v1 = %+v", got)
	}

	// A disabled version cannot be read.
	upd := must(c.UpdateSecretProperties(ctx, "db-pass", v2.ID.Version(), azsecrets.UpdateSecretPropertiesParameters{
		SecretAttributes: &azsecrets.SecretAttributes{Enabled: to.Ptr(false)}, Tags: map[string]*string{"owner": to.Ptr("ops")}}, nil))
	if *upd.Attributes.Enabled || *upd.Tags["owner"] != "ops" || upd.Value != nil {
		t.Fatalf("update = %+v", upd)
	}
	_, err := c.GetSecret(ctx, "db-pass", "", nil)
	wantErr(t, err, 403, "Forbidden")
	_, err = c.GetSecret(ctx, "nope", "", nil)
	wantErr(t, err, 404, "SecretNotFound")

	// Lists page 25 at a time.
	for i := range 30 {
		must(c.SetSecret(ctx, fmt.Sprintf("s%02d", i), azsecrets.SetSecretParameters{Value: to.Ptr("x")}, nil))
	}
	names, pages := 0, 0
	for p := c.NewListSecretPropertiesPager(nil); p.More(); pages++ {
		names += len(must(p.NextPage(ctx)).Value)
	}
	versions := must(c.NewListSecretPropertiesVersionsPager("db-pass", nil).NextPage(ctx)).Value
	if names != 31 || pages != 2 || len(versions) != 2 || versions[0].ID.Version() != v1.ID.Version() {
		t.Fatalf("listed %d secrets in %d pages, %d versions", names, pages, len(versions))
	}

	// Soft delete reserves the name until recovered or purged.
	del := must(c.DeleteSecret(ctx, "db-pass", nil))
	if *del.RecoveryID != url+"/deletedsecrets/db-pass" || del.DeletedDate == nil || del.ScheduledPurgeDate == nil || del.Value != nil {
		t.Fatalf("delete = %+v", del)
	}
	_, err = c.SetSecret(ctx, "db-pass", azsecrets.SetSecretParameters{Value: to.Ptr("again")}, nil)
	wantErr(t, err, 409, "Conflict")
	_, err = c.GetSecret(ctx, "db-pass", v1.ID.Version(), nil)
	wantErr(t, err, 404, "SecretNotFound")
	if got := must(c.GetDeletedSecret(ctx, "db-pass", nil)); got.ID.Version() != v2.ID.Version() {
		t.Fatalf("deleted = %+v", got)
	}
	if deleted := must(c.NewListDeletedSecretPropertiesPager(nil).NextPage(ctx)).Value; len(deleted) != 1 || deleted[0].ID.Name() != "db-pass" {
		t.Fatalf("deleted list = %v", deleted)
	}
	must(c.RecoverDeletedSecret(ctx, "db-pass", nil))
	if got := must(c.GetSecret(ctx, "db-pass", v1.ID.Version(), nil)); *got.Value != "s3cret" {
		t.Fatalf("recovered = %q", *got.Value)
	}

	// Backup, delete and purge, restore.
	backup := must(c.BackupSecret(ctx, "db-pass", nil))
	must(c.DeleteSecret(ctx, "db-pass", nil))
	must(c.PurgeDeletedSecret(ctx, "db-pass", nil))
	_, err = c.GetDeletedSecret(ctx, "db-pass", nil)
	wantErr(t, err, 404, "SecretNotFound")
	must(c.RestoreSecret(ctx, azsecrets.RestoreSecretParameters{SecretBackup: backup.Value}, nil))
	if got := must(c.GetSecret(ctx, "db-pass", v1.ID.Version(), nil)); *got.Value != "s3cret" {
		t.Fatalf("restored = %q", *got.Value)
	}
	_, err = c.RestoreSecret(ctx, azsecrets.RestoreSecretParameters{SecretBackup: backup.Value}, nil)
	wantErr(t, err, 409, "Conflict")

	// Each <vault>.vault.<host> is its own vault.
	other := secretsClient(strings.Replace(url, "localhost", "other.vault.localhost", 1), opts)
	_, err = other.GetSecret(ctx, "db-pass", "", nil)
	wantErr(t, err, 404, "SecretNotFound")
	if got := must(other.SetSecret(ctx, "db-pass", azsecrets.SetSecretParameters{Value: to.Ptr("o")}, nil)); !strings.HasPrefix(string(*got.ID), "https://other.vault.localhost:") {
		t.Fatalf("id = %s", *got.ID)
	}

	// The console sees metadata, never values.
	svc := h.Service("keyvault")
	rows := svc.(core.Browsable).Resources()[1].Rows
	info := must(json.Marshal(svc.(core.Inspectable).Inspect("Secrets", "default/db-pass")))
	if len(rows) != 32 || rows[0][0] != "default" || rows[0][1] != "db-pass" || rows[31][0] != "other" ||
		!strings.Contains(string(info), v1.ID.Version()) || strings.Contains(string(info), "s3cret") {
		t.Fatalf("rows %v, inspect %s", rows[:1], info)
	}
}

func TestKeys(t *testing.T) {
	h, url, opts := serve(t)
	c := keysClient(url, opts)

	rk := must(c.CreateKey(ctx, "rsa", azkeys.CreateKeyParameters{Kty: to.Ptr(azkeys.KeyTypeRSA), KeySize: to.Ptr(int32(2048))}, nil))
	ver := rk.Key.KID.Version()
	pub := &rsa.PublicKey{N: new(big.Int).SetBytes(rk.Key.N), E: int(new(big.Int).SetBytes(rk.Key.E).Int64())}
	if pub.N.BitLen() != 2048 || pub.E != 65537 || rk.Key.D != nil || len(rk.Key.KeyOps) != 6 || !strings.HasPrefix(string(*rk.Key.KID), url+"/keys/rsa/") {
		t.Fatalf("rsa key = %+v", rk.Key)
	}

	// RSA-OAEP-256 with a version, decrypting with the latest ("").
	oaep := to.Ptr(azkeys.EncryptionAlgorithmRSAOAEP256)
	enc := must(c.Encrypt(ctx, "rsa", ver, azkeys.KeyOperationParameters{Algorithm: oaep, Value: []byte("hello")}, nil))
	if dec := must(c.Decrypt(ctx, "rsa", "", azkeys.KeyOperationParameters{Algorithm: oaep, Value: enc.Result}, nil)); string(dec.Result) != "hello" || *enc.KID != *rk.Key.KID {
		t.Fatalf("decrypt = %q, kid %s", dec.Result, *enc.KID)
	}

	// RS256, verified by the service and locally with the returned public key.
	digest := sha256.Sum256([]byte("message"))
	rs256 := to.Ptr(azkeys.SignatureAlgorithmRS256)
	sig := must(c.Sign(ctx, "rsa", ver, azkeys.SignParameters{Algorithm: rs256, Value: digest[:]}, nil))
	ok := must(c.Verify(ctx, "rsa", ver, azkeys.VerifyParameters{Algorithm: rs256, Digest: digest[:], Signature: sig.Result}, nil))
	if !*ok.Value || rsa.VerifyPKCS1v15(pub, crypto.SHA256, digest[:], sig.Result) != nil {
		t.Fatal("RS256 signature does not verify")
	}
	bad := must(c.Verify(ctx, "rsa", ver, azkeys.VerifyParameters{Algorithm: rs256, Digest: digest[:], Signature: append([]byte{^sig.Result[0]}, sig.Result[1:]...)}, nil))
	if *bad.Value {
		t.Fatal("a tampered signature verified")
	}
	_, err := c.Sign(ctx, "rsa", ver, azkeys.SignParameters{Algorithm: rs256, Value: []byte("not a digest")}, nil)
	wantErr(t, err, 400, "BadParameter")

	// Wrap and unwrap.
	wrapped := must(c.WrapKey(ctx, "rsa", ver, azkeys.KeyOperationParameters{Algorithm: to.Ptr(azkeys.EncryptionAlgorithmRSAOAEP), Value: []byte("0123456789abcdef")}, nil))
	if got := must(c.UnwrapKey(ctx, "rsa", ver, azkeys.KeyOperationParameters{Algorithm: to.Ptr(azkeys.EncryptionAlgorithmRSAOAEP), Value: wrapped.Result}, nil)); string(got.Result) != "0123456789abcdef" {
		t.Fatalf("unwrap = %q", got.Result)
	}

	// EC P-256 and ES256: a JWS r||s signature, checked locally.
	ek := must(c.CreateKey(ctx, "ec", azkeys.CreateKeyParameters{Kty: to.Ptr(azkeys.KeyTypeEC), Curve: to.Ptr(azkeys.CurveNameP256)}, nil))
	esig := must(c.Sign(ctx, "ec", "", azkeys.SignParameters{Algorithm: to.Ptr(azkeys.SignatureAlgorithmES256), Value: digest[:]}, nil)).Result
	epub := must(ecdsa.ParseUncompressedPublicKey(elliptic.P256(), append(append([]byte{4}, ek.Key.X...), ek.Key.Y...)))
	if *ek.Key.Crv != azkeys.CurveNameP256 || len(esig) != 64 || !ecdsa.Verify(epub, digest[:], new(big.Int).SetBytes(esig[:32]), new(big.Int).SetBytes(esig[32:])) {
		t.Fatal("ES256 signature does not verify")
	}
	_, err = c.Encrypt(ctx, "ec", "", azkeys.KeyOperationParameters{Algorithm: oaep, Value: []byte("x")}, nil)
	wantErr(t, err, 403, "Forbidden") // EC keys only sign and verify
	_, err = c.CreateKey(ctx, "k1", azkeys.CreateKeyParameters{Kty: to.Ptr(azkeys.KeyTypeEC), Curve: to.Ptr(azkeys.CurveNameP256K)}, nil)
	wantErr(t, err, 400, "BadParameter")

	// Rotation adds a version; the old one still decrypts.
	rot := must(c.RotateKey(ctx, "rsa", nil))
	n := 0
	for p := c.NewListKeyPropertiesVersionsPager("rsa", nil); p.More(); {
		n += len(must(p.NextPage(ctx)).Value)
	}
	if rot.Key.KID.Version() == ver || n != 2 {
		t.Fatalf("rotate: version %s, %d versions", rot.Key.KID.Version(), n)
	}
	if dec := must(c.Decrypt(ctx, "rsa", ver, azkeys.KeyOperationParameters{Algorithm: oaep, Value: enc.Result}, nil)); string(dec.Result) != "hello" {
		t.Fatalf("old version decrypt = %q", dec.Result)
	}
	must(c.UpdateKeyRotationPolicy(ctx, "rsa", azkeys.KeyRotationPolicy{
		LifetimeActions: []*azkeys.LifetimeAction{{Action: &azkeys.LifetimeActionType{Type: to.Ptr(azkeys.KeyRotationPolicyActionRotate)},
			Trigger: &azkeys.LifetimeActionTrigger{TimeAfterCreate: to.Ptr("P90D")}}},
		Attributes: &azkeys.KeyRotationPolicyAttributes{ExpiryTime: to.Ptr("P1Y")}}, nil))
	if p := must(c.GetKeyRotationPolicy(ctx, "rsa", nil)); *p.Attributes.ExpiryTime != "P1Y" || *p.LifetimeActions[0].Action.Type != azkeys.KeyRotationPolicyActionRotate {
		t.Fatalf("rotation policy = %+v", p)
	}

	// An imported oct key wraps the RFC 3394 test vector and seals with AES-GCM.
	unhex := func(s string) []byte { return must(hex.DecodeString(s)) }
	must(c.ImportKey(ctx, "kek", azkeys.ImportKeyParameters{Key: &azkeys.JSONWebKey{Kty: to.Ptr(azkeys.KeyTypeOct), K: unhex("000102030405060708090A0B0C0D0E0F")}}, nil))
	a128kw := to.Ptr(azkeys.EncryptionAlgorithmA128KW)
	kw := must(c.WrapKey(ctx, "kek", "", azkeys.KeyOperationParameters{Algorithm: a128kw, Value: unhex("00112233445566778899AABBCCDDEEFF")}, nil))
	if hex.EncodeToString(kw.Result) != "1fa68b0a8112b447aef34bd8fb5a7b829d3e862371d2cfe5" {
		t.Fatalf("A128KW = %x", kw.Result)
	}
	if uw := must(c.UnwrapKey(ctx, "kek", "", azkeys.KeyOperationParameters{Algorithm: a128kw, Value: kw.Result}, nil)); hex.EncodeToString(uw.Result) != "00112233445566778899aabbccddeeff" {
		t.Fatalf("A128KW unwrap = %x", uw.Result)
	}
	gcm := to.Ptr(azkeys.EncryptionAlgorithmA128GCM)
	sealed := must(c.Encrypt(ctx, "kek", "", azkeys.KeyOperationParameters{Algorithm: gcm, Value: []byte("hi"), AdditionalAuthenticatedData: []byte("aad")}, nil))
	opened := must(c.Decrypt(ctx, "kek", "", azkeys.KeyOperationParameters{Algorithm: gcm, Value: sealed.Result, IV: sealed.IV,
		AuthenticationTag: sealed.AuthenticationTag, AdditionalAuthenticatedData: []byte("aad")}, nil))
	if string(opened.Result) != "hi" || len(sealed.IV) != 12 || len(sealed.AuthenticationTag) != 16 {
		t.Fatalf("A128GCM = %q", opened.Result)
	}

	// Listing, errors, random bytes.
	keys := 0
	for p := c.NewListKeyPropertiesPager(nil); p.More(); {
		keys += len(must(p.NextPage(ctx)).Value)
	}
	if keys != 3 {
		t.Fatalf("listed %d keys", keys)
	}
	_, err = c.GetKey(ctx, "nope", "", nil)
	wantErr(t, err, 404, "KeyNotFound")
	if rb := must(c.GetRandomBytes(ctx, azkeys.GetRandomBytesParameters{Count: to.Ptr(int32(16))}, nil)); len(rb.Value) != 16 {
		t.Fatalf("random bytes = %d", len(rb.Value))
	}

	// Backup, delete, purge and restore; a snapshot round trip keeps the private key.
	backup := must(c.BackupKey(ctx, "rsa", nil))
	if del := must(c.DeleteKey(ctx, "rsa", nil)); *del.RecoveryID != url+"/deletedkeys/rsa" || del.Key.KID.Version() != rot.Key.KID.Version() {
		t.Fatalf("delete = %+v", del)
	}
	must(c.PurgeDeletedKey(ctx, "rsa", nil))
	_, err = c.GetKey(ctx, "rsa", "", nil)
	wantErr(t, err, 404, "KeyNotFound")
	must(c.RestoreKey(ctx, azkeys.RestoreKeyParameters{KeyBackup: backup.Value}, nil))
	snap := must(h.Snapshot())
	h.Reset()
	_, err = c.GetKey(ctx, "rsa", "", nil)
	wantErr(t, err, 404, "KeyNotFound")
	if err := h.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if dec := must(c.Decrypt(ctx, "rsa", ver, azkeys.KeyOperationParameters{Algorithm: oaep, Value: enc.Result}, nil)); string(dec.Result) != "hello" {
		t.Fatalf("decrypt after restore = %q", dec.Result)
	}
}

func TestCertificates(t *testing.T) {
	_, url, opts := serve(t)
	hc := opts.Transport.(*http.Client)
	do := func(method, path string, body any) (int, map[string]any) {
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest(method, url+path+"?api-version=7.5", bytes.NewReader(b))
		req.Header.Set("Authorization", "Bearer pogi")
		res := must(hc.Do(req))
		defer res.Body.Close()
		var out map[string]any
		json.NewDecoder(res.Body).Decode(&out)
		return res.StatusCode, out
	}

	// Anonymous requests get Key Vault's challenge.
	res := must(hc.Get(url + "/certificates?api-version=7.5"))
	res.Body.Close()
	if res.StatusCode != 401 || !strings.Contains(res.Header.Get("WWW-Authenticate"), `resource="https://vault.azure.net"`) {
		t.Fatalf("anonymous = %d %q", res.StatusCode, res.Header.Get("WWW-Authenticate"))
	}

	for _, months := range []int{-1, 1201, 1 << 31} {
		if st, _ := do("POST", "/certificates/long/create", map[string]any{"policy": map[string]any{"x509_props": map[string]any{"validity_months": months}}}); st != 400 {
			t.Errorf("validity_months %d = %d, want 400", months, st)
		}
	}

	status, op := do("POST", "/certificates/web/create", map[string]any{"policy": map[string]any{
		"key_props": map[string]any{"kty": "EC", "crv": "P-256"}, "secret_props": map[string]any{"contentType": "application/x-pem-file"},
		"x509_props": map[string]any{"subject": "CN=web.local, O=pogi", "sans": map[string]any{"dns_names": []string{"web.local"}}, "validity_months": 6},
		"issuer":     map[string]any{"name": "Self"}}})
	if status != 202 || op["status"] != "completed" || op["target"] != url+"/certificates/web" {
		t.Fatalf("create = %d %v", status, op)
	}
	if status, pending := do("GET", "/certificates/web/pending", nil); status != 200 || pending["status"] != "completed" {
		t.Fatalf("pending = %d %v", status, pending)
	}
	_, cert := do("GET", "/certificates/web", nil)
	x := must(x509.ParseCertificate(must(base64.StdEncoding.DecodeString(cert["cer"].(string)))))
	if x.Subject.CommonName != "web.local" || strings.Join(x.Subject.Organization, ",") != "pogi" || strings.Join(x.DNSNames, ",") != "web.local" || cert["x5t"] == nil ||
		cert["policy"].(map[string]any)["id"] != url+"/certificates/web/policy" {
		t.Fatalf("certificate = %v", cert)
	}

	// Its secret holds the PEM key and certificate, its key the public half; all share one version.
	s := must(secretsClient(url, opts).GetSecret(ctx, "web", "", nil))
	k := must(keysClient(url, opts).GetKey(ctx, "web", "", nil))
	block, rest := pem.Decode([]byte(*s.Value))
	priv := must(x509.ParsePKCS8PrivateKey(block.Bytes)).(*ecdsa.PrivateKey)
	point := must(priv.PublicKey.Bytes()) // 0x04 | x | y
	if block, _ = pem.Decode(rest); block == nil || !bytes.Equal(block.Bytes, x.Raw) || !priv.PublicKey.Equal(x.PublicKey) ||
		!*s.Managed || !*k.Managed || s.ID.Version() != k.Key.KID.Version() || !strings.HasSuffix(cert["id"].(string), "/"+s.ID.Version()) ||
		!bytes.Equal(k.Key.X, point[1:33]) {
		t.Fatalf("secret %+v / key %+v do not match the certificate", s, k.Key)
	}

	// The default content type is PKCS#12.
	do("POST", "/certificates/api/create", map[string]any{"policy": map[string]any{"x509_props": map[string]any{"subject": "CN=api"}}})
	if p := must(secretsClient(url, opts).GetSecret(ctx, "api", "", nil)); *p.ContentType != "application/x-pkcs12" || len(must(base64.StdEncoding.DecodeString(*p.Value))) < 1000 {
		t.Fatalf("pkcs12 secret = %+v", p)
	}

	// Deleting the certificate soft-deletes its key and secret; recovery brings them back.
	if status, del := do("DELETE", "/certificates/web", nil); status != 200 || del["recoveryId"] != url+"/deletedcertificates/web" {
		t.Fatalf("delete = %d %v", status, del)
	}
	_, err := secretsClient(url, opts).GetSecret(ctx, "web", "", nil)
	wantErr(t, err, 404, "SecretNotFound")
	if status, _ := do("POST", "/deletedcertificates/web/recover", nil); status != 200 {
		t.Fatalf("recover = %d", status)
	}
	must(keysClient(url, opts).GetKey(ctx, "web", "", nil))
	if status, body := do("GET", "/certificates/nope", nil); status != 404 || body["error"].(map[string]any)["code"] != "CertificateNotFound" {
		t.Fatalf("missing certificate = %d %v", status, body)
	}
}
