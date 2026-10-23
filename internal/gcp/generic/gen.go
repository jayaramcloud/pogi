//go:build ignore

// gen copies the discovery documents that apis.txt lists from the google.golang.org/api module
// that go.mod pins (or from extra/ when the module has none), keeps what the engine needs and writes them gzipped to docs/. Run it with
// `go generate ./internal/gcp/generic`. It reads the local module cache: no network.
package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "google.golang.org/api").Output()
	check(err)
	dir := strings.TrimSpace(string(out))
	list, err := os.ReadFile("apis.txt")
	check(err)
	for _, line := range strings.Split(string(list), "\n") {
		f := strings.Fields(line)
		if len(f) == 0 || strings.HasPrefix(f[0], "#") {
			continue
		}
		if len(f) < 4 {
			check(fmt.Errorf("apis.txt: %q wants: api version terraform-argument title", line))
		}
		api, version := f[0], f[1]
		raw, err := os.ReadFile(filepath.Join(dir, api, version, api+"-api.json"))
		if os.IsNotExist(err) { // an API the module has no document for: one committed under extra/
			raw, err = os.ReadFile(filepath.Join("extra", api+"-"+version+"-api.json"))
		}
		check(err)
		var d map[string]any
		check(json.Unmarshal(raw, &d))
		keep := map[string]any{"schemas": map[string]any{}, "resources": prune(d["resources"], resource)}
		for _, k := range []string{"name", "version", "rootUrl"} {
			keep[k] = d[k]
		}
		for n, s := range d["schemas"].(map[string]any) {
			keep["schemas"].(map[string]any)[n] = schema(s)
		}
		b, err := json.Marshal(keep) // map keys come out sorted: the output is the same every run
		check(err)
		var buf bytes.Buffer
		zw, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
		zw.Write(b)
		check(zw.Close())
		check(os.WriteFile(filepath.Join("docs", api+"-"+version+".json.gz"), buf.Bytes(), 0o644))
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}

// prune applies f to every value of a map.
func prune(v any, f func(any) any) any {
	out := map[string]any{}
	m, _ := v.(map[string]any)
	for k, x := range m {
		out[k] = f(x)
	}
	return out
}

func pick(m map[string]any, keys ...string) map[string]any {
	out := map[string]any{}
	for _, k := range keys {
		if v, ok := m[k]; ok {
			out[k] = v
		}
	}
	return out
}

// resource keeps a resource's methods (paths, parameters, request and response) and its sub-resources.
func resource(v any) any {
	m, _ := v.(map[string]any)
	out := map[string]any{}
	if x, ok := m["resources"]; ok {
		out["resources"] = prune(x, resource)
	}
	out["methods"] = prune(m["methods"], func(v any) any {
		mm, _ := v.(map[string]any)
		o := pick(mm, "id", "path", "flatPath", "httpMethod", "request", "response")
		o["parameters"] = prune(mm["parameters"], func(p any) any {
			return pick(p.(map[string]any), "location", "required", "type")
		})
		return o
	})
	return out
}

// schema keeps what decides a field's type and who may set it, without the descriptions.
func schema(v any) any {
	m, _ := v.(map[string]any)
	out := pick(m, "type", "format", "$ref", "enum", "default", "readOnly")
	if x, ok := m["properties"]; ok {
		out["properties"] = prune(x, schema)
	}
	for _, k := range []string{"items", "additionalProperties"} {
		if x, ok := m[k]; ok {
			out[k] = schema(x)
		}
	}
	return out
}
