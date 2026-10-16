package web

// KMS encrypt/decrypt panels (AWS KMS, Cloud KMS, Key Vault keys) and secret
// reveal (Secrets Manager, SSM, Secret Manager, Key Vault).

import (
	"cmp"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/nrndev/pogi/internal/core"
)

type cryptoData struct {
	Keys          []choice
	Key, Mode, In string
	Out, Err      string
	Alg           string
}

// cryptoKeys lists keys that can encrypt: value is "aws|account|region|keyId", "gcp|name" or "azure|vault|name".
func (ui *UI) cryptoKeys(r *http.Request) []choice {
	var out []choice
	for _, k := range ui.rows(r, "kms", "Keys") {
		if k[5] == "ENCRYPT_DECRYPT" && k[6] == "Enabled" {
			out = append(out, choice{"aws|" + k[3] + "|" + k[2] + "|" + k[0], "AWS KMS · " + cmp.Or(k[1], k[0])})
		}
	}
	for _, k := range ui.rows(r, "cloudkms", "Keys") {
		if k[1] == "ENCRYPT_DECRYPT" {
			out = append(out, choice{"gcp|" + k[0], "Cloud KMS · " + k[0][strings.Index(k[0], "/keyRings/")+1:]})
		}
	}
	for _, k := range ui.rows(r, "keyvault", "Keys") {
		out = append(out, choice{"azure|" + k[0] + "|" + k[1] + "|" + k[2], "Key Vault · " + k[0] + "/" + k[1] + " (" + k[2] + ")"})
	}
	return out
}

func (ui *UI) cryptoPage(w http.ResponseWriter, r *http.Request) {
	ui.xrender(w, r, "x-crypto", "Encrypt and decrypt", "kms", &cryptoData{Keys: ui.cryptoKeys(r), Key: r.URL.Query().Get("key"), Mode: "encrypt"})
}

func (ui *UI) cryptoRun(w http.ResponseWriter, r *http.Request) {
	d := &cryptoData{Keys: ui.cryptoKeys(r), Key: r.FormValue("key"), Mode: r.FormValue("mode"), In: r.FormValue("input"), Alg: strings.TrimSpace(r.FormValue("alg"))}
	var err error
	d.Out, err = ui.crypt(d.Key, d.Mode, d.In, d.Alg)
	if err != nil {
		d.Err = err.Error()
	}
	ui.xrender(w, r, "x-crypto", "Encrypt and decrypt", "kms", d)
}

// plainText shows decrypted bytes as text, or as base64 when they are not UTF-8.
func plainText(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return "base64: " + base64.StdEncoding.EncodeToString(b)
}

func (ui *UI) crypt(key, mode, in, alg string) (string, error) {
	p := strings.Split(key, "|")
	enc := mode == "encrypt"
	if mode != "encrypt" && mode != "decrypt" || len(p) < 2 {
		return "", errors.New("choose a key and a mode")
	}
	std := base64.StdEncoding
	switch p[0] {
	case "aws":
		var out struct{ CiphertextBlob, Plaintext string }
		req, op := map[string]any{"KeyId": p[3]}, "Decrypt"
		if enc {
			req["Plaintext"], op = std.EncodeToString([]byte(in)), "Encrypt"
		} else {
			req["CiphertextBlob"] = strings.TrimSpace(in)
		}
		if err := ui.hub.Call(p[1], p[2], "TrentService", op, req, &out); err != nil {
			return "", err
		}
		if enc {
			return out.CiphertextBlob, nil
		}
		b, _ := std.DecodeString(out.Plaintext)
		return plainText(b), nil
	case "gcp":
		var out struct{ Ciphertext, Plaintext string }
		req, op := map[string]any{}, ":decrypt"
		if enc {
			req["plaintext"], op = std.EncodeToString([]byte(in)), ":encrypt"
		} else {
			req["ciphertext"] = strings.TrimSpace(in)
		}
		if err := ui.gwJSON("POST", "", "/v1/"+p[1]+op, nil, req, &out); err != nil {
			return "", err
		}
		if enc {
			return out.Ciphertext, nil
		}
		b, _ := std.DecodeString(out.Plaintext)
		return plainText(b), nil
	case "azure": // vault, name, key type
		if len(p) < 4 {
			return "", errors.New("choose a key")
		}
		if alg == "" {
			alg = map[string]string{"oct": "A256GCM"}[strings.ToLower(p[3])]
		}
		if alg == "" {
			alg = "RSA-OAEP-256"
		}
		b64 := base64.RawURLEncoding
		req := map[string]any{"alg": alg}
		if enc {
			req["value"] = b64.EncodeToString([]byte(in))
		} else if in = strings.TrimSpace(in); strings.HasPrefix(in, "{") { // what encrypt returned: value, iv and tag
			if err := json.Unmarshal([]byte(in), &req); err != nil {
				return "", err
			}
		} else {
			req["value"] = in
		}
		var out map[string]any
		op := map[bool]string{true: "/encrypt", false: "/decrypt"}[enc]
		if err := ui.gwJSON("POST", ui.vaultHost(p[1]), "/keys/"+url.PathEscape(p[2])+op+"?api-version=7.4", map[string]string{"Authorization": "Bearer pogi"}, req, &out); err != nil {
			return "", err
		}
		if enc {
			delete(out, "kid")
			out["alg"] = alg
			b, _ := json.Marshal(out)
			return string(b), nil
		}
		b, _ := b64.DecodeString(dig(out, "value"))
		return plainText(b), nil
	}
	return "", errors.New("unknown key")
}

// vaultHost is the Host that selects a vault: "<vault>.vault.localhost:port".
func (ui *UI) vaultHost(vault string) string {
	if vault == "" || vault == "default" {
		return ""
	}
	return vault + ".vault.localhost:" + strconv.Itoa(ui.hub.Cfg.Port)
}

// ---- secrets ----

type secretRow struct {
	Cloud, Kind, Name, Detail, Ref string
}

// secretRows lists every secret: Ref is what reveal needs.
func (ui *UI) secretRows(r *http.Request) []secretRow {
	var out []secretRow
	for _, s := range ui.rows(r, "secretsmanager", "Secrets") {
		out = append(out, secretRow{core.AWS, "Secrets Manager", s[0], s[1], "awssm|" + s[2] + "|" + s[1] + "|" + s[0]})
	}
	for _, s := range ui.rows(r, "ssm", "Parameters") {
		out = append(out, secretRow{core.AWS, "SSM " + s[3], s[0], s[1], "ssm|" + s[2] + "|" + s[1] + "|" + s[0]})
	}
	for _, s := range ui.rows(r, "secretmanager", "Secrets") {
		out = append(out, secretRow{core.GCP, "Secret Manager", s[0], s[1], "gcp|" + s[0]})
	}
	for _, s := range ui.rows(r, "keyvault", "Secrets") {
		out = append(out, secretRow{core.Azure, "Key Vault", s[1], s[0], "azure|" + s[0] + "|" + s[1]})
	}
	return out
}

func (ui *UI) secretsPage(w http.ResponseWriter, r *http.Request) {
	ui.render(w, r, "x-secrets", "Secrets", "explore", map[string]any{"Secrets": ui.secretRows(r), "Log": ui.hub.Requests.Query(0, 20, "console", "", false)})
}

// reveal answers the htmx POST of one secret's reveal button with the value.
// Every reveal is logged: in the request log (service "console") and the
// server log.
func (ui *UI) reveal(w http.ResponseWriter, r *http.Request) {
	ref := r.FormValue("ref")
	v, err := ui.secretValue(ref)
	e := core.Entry{Time: time.Now(), Cloud: strings.SplitN(ref, "|", 2)[0], Service: "console", Op: "RevealSecret", Method: "POST", Path: strings.ReplaceAll(ref, "|", " "), Status: 200}
	if err != nil {
		e.Status, e.Error = 404, err.Error()
	}
	ui.hub.Requests.Add(e)
	ui.hub.Log.Info("secret revealed in the console", "secret", e.Path, "ok", err == nil)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	ui.pages["x-secrets"].ExecuteTemplate(w, "revealed", map[string]any{"Value": v, "Err": err})
}

func (ui *UI) secretValue(ref string) (string, error) {
	p := strings.Split(ref, "|")
	if len(p) < 2 {
		return "", errors.New("unknown secret")
	}
	switch p[0] {
	case "awssm": // account, region, name
		var out struct {
			SecretString string
			SecretBinary string
		}
		if err := ui.hub.Call(p[1], p[2], "secretsmanager", "GetSecretValue", map[string]any{"SecretId": p[3]}, &out); err != nil {
			return "", err
		}
		if out.SecretString == "" && out.SecretBinary != "" {
			b, _ := base64.StdEncoding.DecodeString(out.SecretBinary)
			return plainText(b), nil
		}
		return out.SecretString, nil
	case "ssm":
		var out struct{ Parameter struct{ Value string } }
		if err := ui.hub.Call(p[1], p[2], "AmazonSSM", "GetParameter", map[string]any{"Name": p[3], "WithDecryption": true}, &out); err != nil {
			return "", err
		}
		return out.Parameter.Value, nil
	case "gcp":
		var out struct{ Payload struct{ Data string } }
		if err := ui.gwJSON("GET", "", "/v1/"+p[1]+"/versions/latest:access", nil, nil, &out); err != nil {
			return "", err
		}
		b, _ := base64.StdEncoding.DecodeString(out.Payload.Data)
		return plainText(b), nil
	case "azure": // vault, name
		if len(p) < 3 {
			return "", errors.New("unknown secret")
		}
		var out struct{ Value string }
		if err := ui.gwJSON("GET", ui.vaultHost(p[1]), "/secrets/"+url.PathEscape(p[2])+"?api-version=7.4", map[string]string{"Authorization": "Bearer pogi"}, nil, &out); err != nil {
			return "", err
		}
		return out.Value, nil
	}
	return "", fmt.Errorf("unknown secret kind %q", p[0])
}
