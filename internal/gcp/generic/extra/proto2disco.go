//go:build ignore

// proto2disco writes a discovery document for an API whose host serves none, or none without an API key
// (Distributed Cloud, GKE Multi-Cloud, Security Operations and Compliance Manager), from the protobuf definitions that the cloud.google.com/go client compiles in:
// the messages, and the REST binding (google.api.http) of every method. It writes the same shape the
// documents in google.golang.org/api have, with only what internal/gcp/generic/gen.go keeps.
// Run it in internal/gcp/generic: `go run extra/proto2disco.go`.
package main

import (
	"encoding/json"
	"os"
	"regexp"
	"strings"

	_ "cloud.google.com/go/chronicle/apiv1/chroniclepb"
	_ "cloud.google.com/go/cloudsecuritycompliance/apiv1/cloudsecuritycompliancepb"
	_ "cloud.google.com/go/edgecontainer/apiv1/edgecontainerpb"
	_ "cloud.google.com/go/gkemulticloud/apiv1/gkemulticloudpb"
	"google.golang.org/genproto/googleapis/api/annotations"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func main() {
	write("edgecontainer", "v1", "google.cloud.edgecontainer.v1")
	write("gkemulticloud", "v1", "google.cloud.gkemulticloud.v1")
	write("chronicle", "v1", "google.cloud.chronicle.v1")
	write("cloudsecuritycompliance", "v1", "google.cloud.cloudsecuritycompliance.v1")
}

type obj = map[string]any

func write(api, version, pkg string) {
	schemas := obj{
		"Empty":     obj{"type": "object"},
		"Operation": obj{"type": "object", "properties": obj{"name": obj{"type": "string"}, "done": obj{"type": "boolean"}, "metadata": obj{"type": "object"}, "response": obj{"type": "object"}, "error": obj{"type": "object"}}},
	}
	methods := obj{}
	protoregistry.GlobalFiles.RangeFilesByPackage(protoreflect.FullName(pkg), func(f protoreflect.FileDescriptor) bool {
		var msgs func(ms protoreflect.MessageDescriptors)
		msgs = func(ms protoreflect.MessageDescriptors) {
			for i := 0; i < ms.Len(); i++ {
				m := ms.Get(i)
				if !m.IsMapEntry() {
					schemas[name(m, pkg)] = message(m, pkg)
				}
				msgs(m.Messages())
			}
		}
		msgs(f.Messages())
		for i := 0; i < f.Services().Len(); i++ {
			ms := f.Services().Get(i).Methods()
			for j := 0; j < ms.Len(); j++ {
				if id, m := method(api, ms.Get(j), pkg); m != nil {
					methods[id] = m
				}
			}
		}
		return true
	})
	doc := obj{"name": api, "version": version, "rootUrl": "https://" + api + ".googleapis.com/",
		"schemas": schemas, "resources": obj{"all": obj{"methods": methods}}}
	b, err := json.MarshalIndent(doc, "", " ")
	if err != nil {
		panic(err)
	}
	if err := os.WriteFile("extra/"+api+"-"+version+"-api.json", b, 0o644); err != nil {
		panic(err)
	}
}

// name is a message's schema name: its name, with the names it is nested in in front.
func name(m protoreflect.MessageDescriptor, pkg string) string {
	return strings.ReplaceAll(strings.TrimPrefix(string(m.FullName()), pkg+"."), ".", "")
}

func message(m protoreflect.MessageDescriptor, pkg string) obj {
	props := obj{}
	for i := 0; i < m.Fields().Len(); i++ {
		f := m.Fields().Get(i)
		s := field(f, pkg)
		if behaviors, ok := proto.GetExtension(f.Options(), annotations.E_FieldBehavior).([]annotations.FieldBehavior); ok {
			for _, b := range behaviors {
				if b == annotations.FieldBehavior_OUTPUT_ONLY {
					s["readOnly"] = true
				}
			}
		}
		props[f.JSONName()] = s
	}
	return obj{"type": "object", "properties": props}
}

func field(f protoreflect.FieldDescriptor, pkg string) obj {
	if f.IsMap() {
		return obj{"type": "object", "additionalProperties": scalar(f.MapValue(), pkg)}
	}
	s := scalar(f, pkg)
	if f.IsList() {
		return obj{"type": "array", "items": s}
	}
	return s
}

func scalar(f protoreflect.FieldDescriptor, pkg string) obj {
	switch f.Kind() {
	case protoreflect.StringKind:
		return obj{"type": "string"}
	case protoreflect.BoolKind:
		return obj{"type": "boolean"}
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind, protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return obj{"type": "integer", "format": "int32"}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return obj{"type": "string", "format": "int64"}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return obj{"type": "string", "format": "uint64"}
	case protoreflect.FloatKind, protoreflect.DoubleKind:
		return obj{"type": "number"}
	case protoreflect.BytesKind:
		return obj{"type": "string", "format": "byte"}
	case protoreflect.EnumKind:
		var vals []string
		for i := 0; i < f.Enum().Values().Len(); i++ {
			vals = append(vals, string(f.Enum().Values().Get(i).Name()))
		}
		return obj{"type": "string", "enum": vals}
	}
	m := f.Message()
	switch m.FullName() {
	case "google.protobuf.Timestamp":
		return obj{"type": "string", "format": "google-datetime"}
	case "google.protobuf.Duration":
		return obj{"type": "string", "format": "google-duration"}
	case "google.protobuf.FieldMask":
		return obj{"type": "string", "format": "google-fieldmask"}
	}
	if strings.HasPrefix(string(m.FullName()), pkg+".") {
		return obj{"$ref": name(m, pkg)}
	}
	return obj{"type": "object"} // Struct, Any, Status, and the like
}

var param = regexp.MustCompile(`\{([^}=]+)(=([^}]*))?\}`)

// method turns an rpc with an HTTP binding into a discovery method.
func method(api string, m protoreflect.MethodDescriptor, pkg string) (string, obj) {
	rule, _ := proto.GetExtension(m.Options(), annotations.E_Http).(*annotations.HttpRule)
	if rule == nil {
		return "", nil
	}
	var verb, path string
	switch p := rule.Pattern.(type) {
	case *annotations.HttpRule_Get:
		verb, path = "GET", p.Get
	case *annotations.HttpRule_Post:
		verb, path = "POST", p.Post
	case *annotations.HttpRule_Put:
		verb, path = "PUT", p.Put
	case *annotations.HttpRule_Patch:
		verb, path = "PATCH", p.Patch
	case *annotations.HttpRule_Delete:
		verb, path = "DELETE", p.Delete
	default:
		return "", nil
	}
	// the path with the variables written out: {name=projects/*/locations/*} -> projects/{projectsId}/locations/{locationsId}
	inPath := map[string]bool{}
	flat := param.ReplaceAllStringFunc(path, func(s string) string {
		g := param.FindStringSubmatch(s)
		inPath[strings.Split(g[1], ".")[0]] = true
		if g[3] == "" {
			return "{" + g[1] + "}"
		}
		parts := strings.Split(g[3], "/")
		for i := 1; i < len(parts); i += 2 {
			parts[i] = "{" + parts[i-1] + "Id}"
		}
		return strings.Join(parts, "/")
	})
	flat = strings.TrimPrefix(flat, "/")
	d := obj{"httpMethod": verb, "path": strings.TrimPrefix(param.ReplaceAllString(path, "{+$1}"), "/"), "flatPath": flat}
	params := obj{}
	in := m.Input()
	for i := 0; i < in.Fields().Len(); i++ {
		f := in.Fields().Get(i)
		switch {
		case inPath[string(f.Name())]:
			params[f.JSONName()] = obj{"location": "path"}
		case rule.Body == "*" || rule.Body == string(f.Name()):
		default:
			params[f.JSONName()] = obj{"location": "query"}
		}
	}
	d["parameters"] = params
	switch {
	case rule.Body == "*":
		d["request"] = obj{"$ref": name(in, pkg)}
	case rule.Body != "":
		d["request"] = obj{"$ref": name(in.Fields().ByName(protoreflect.Name(rule.Body)).Message(), pkg)}
	}
	switch out := m.Output().FullName(); out {
	case "google.longrunning.Operation":
		d["response"] = obj{"$ref": "Operation"}
	case "google.protobuf.Empty":
		d["response"] = obj{"$ref": "Empty"}
	default:
		d["response"] = obj{"$ref": name(m.Output(), pkg)}
	}
	// the id the standard methods have in discovery documents: edgecontainer.projects.locations.clusters.create
	var lits []string
	for _, s := range strings.Split(strings.SplitN(flat, ":", 2)[0], "/") {
		if !strings.HasPrefix(s, "{") && !(len(lits) == 0 && s[0] == 'v') {
			lits = append(lits, s)
		}
	}
	rpc := string(m.Name())
	last := strings.ToLower(rpc[:1]) + rpc[1:]
	if !strings.Contains(flat, ":") {
		for _, p := range []struct{ prefix, id string }{{"Create", "create"}, {"Get", "get"}, {"List", "list"}, {"Update", "patch"}, {"Delete", "delete"}} {
			if strings.HasPrefix(rpc, p.prefix) {
				last = p.id
			}
		}
	}
	id := api + "." + strings.Join(lits, ".") + "." + last
	d["id"] = id
	return id, d
}
