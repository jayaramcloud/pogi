package glue

// The Glue Schema Registry: registries, schemas with versions, and version
// metadata. A registry is the object Registry/<account>/<region>/<name>; a
// schema is Schema/<account>/<region>/<registry>/<name> and holds its versions.
// ponytail: compatibility rules are not checked, except DISABLED (no new versions).

import (
	"cmp"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

const defaultRegistry = "default-registry"

// iso is a time as the registry API writes it: a string.
func iso() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z") }

var compatModes = []string{"NONE", "DISABLED", "BACKWARD", "BACKWARD_ALL", "FORWARD", "FORWARD_ALL", "FULL", "FULL_ALL"}

type registryID struct{ RegistryName, RegistryArn string }

type schemaID struct{ SchemaArn, SchemaName, RegistryName string }

// versionRef names a version by number: the latest, or a number.
type versionRef struct {
	LatestVersion bool
	VersionNumber int64
}

func registryArn(c *core.Ctx, name string) string { return c.ARN("glue", "registry/"+name) }
func schemaArn(c *core.Ctx, reg, name string) string {
	return c.ARN("glue", "schema/"+reg+"/"+name)
}

// registryName reads a RegistryId; none means the default registry.
func registryName(id *registryID) (string, error) {
	if id == nil || id.RegistryName == "" && id.RegistryArn == "" {
		return defaultRegistry, nil
	}
	if id.RegistryName != "" {
		return id.RegistryName, nil
	}
	_, _, _, res, ok := core.ParseARN(id.RegistryArn)
	if n, found := strings.CutPrefix(res, "registry/"); ok && found {
		return n, nil
	}
	return "", invalid("Invalid registry ARN: %s", id.RegistryArn)
}

// schemaName reads a SchemaId into registry and schema names.
func schemaName(id *schemaID) (reg, name string, err error) {
	switch {
	case id == nil:
		return "", "", invalid("SchemaId is required.")
	case id.SchemaArn != "":
		_, _, _, res, ok := core.ParseARN(id.SchemaArn)
		parts := strings.SplitN(strings.TrimPrefix(res, "schema/"), "/", 2)
		if !ok || !strings.HasPrefix(res, "schema/") || len(parts) != 2 {
			return "", "", invalid("Invalid schema ARN: %s", id.SchemaArn)
		}
		return parts[0], parts[1], nil
	case id.SchemaName == "":
		return "", "", invalid("SchemaId needs a SchemaArn or a SchemaName.")
	}
	return cmp.Or(id.RegistryName, defaultRegistry), id.SchemaName, nil
}

// canonical is a schema definition reduced to what makes versions differ:
// whitespace, key order and (for Avro) custom attributes do not.
func canonical(format, def string) (string, error) {
	switch format {
	case "AVRO", "JSON":
		var v any
		if err := json.Unmarshal([]byte(def), &v); err != nil {
			return "", invalid("The %s schema definition is not valid JSON: %v", format, err)
		}
		if format == "AVRO" {
			var err error
			if v, err = avroCanon(v); err != nil {
				return "", err
			}
		}
		b, _ := json.Marshal(v)
		return string(b), nil
	case "PROTOBUF":
		if strings.TrimSpace(def) == "" {
			return "", invalid("The PROTOBUF schema definition is empty.")
		}
		return strings.Join(strings.Fields(def), " "), nil
	}
	return "", invalid("DataFormat must be AVRO, JSON or PROTOBUF.")
}

// avroKeys are the attributes the Avro specification defines; others are custom.
var avroKeys = []string{"type", "name", "namespace", "doc", "aliases", "fields", "symbols", "items", "values", "size", "default", "order", "logicalType", "precision", "scale"}

func avroCanon(v any) (any, error) {
	switch x := v.(type) {
	case string:
		return x, nil
	case []any:
		out := make([]any, len(x))
		for i, e := range x {
			var err error
			if out[i], err = avroCanon(e); err != nil {
				return nil, err
			}
		}
		return out, nil
	case map[string]any:
		if x["type"] == nil {
			return nil, invalid("The Avro schema has an object without a type.")
		}
		out := map[string]any{}
		for _, k := range avroKeys {
			if e, ok := x[k]; ok {
				if k == "default" || k == "doc" || k == "name" || k == "namespace" {
					out[k] = e
				} else if c, err := avroCanon(e); err == nil {
					out[k] = c
				} else {
					return nil, err
				}
			}
		}
		return out, nil
	}
	return nil, invalid("The Avro schema is not a type, an object or a union.")
}

// version is one entry of a schema's Versions.
func newVersion(n int64, def string) obj {
	return obj{"SchemaVersionId": core.UUID(), "VersionNumber": n, "SchemaDefinition": def, "Status": "AVAILABLE", "CreatedTime": iso()}
}

func versions(sc obj) []obj { return list(sc["Versions"]) }

// latest is the highest version that is not deleted, or nil.
func latest(sc obj) obj {
	var last obj
	for _, v := range versions(sc) {
		if v["Status"] != "DELETING" {
			last = v
		}
	}
	return last
}

func n64(v any) int64 {
	switch n := v.(type) {
	case int64:
		return n
	case float64:
		return int64(n)
	}
	return 0
}

func schemaOut(sc obj) obj {
	out := obj{}
	for _, k := range []string{"RegistryName", "RegistryArn", "SchemaName", "SchemaArn", "Description", "DataFormat", "Compatibility", "SchemaCheckpoint", "SchemaStatus", "CreatedTime", "UpdatedTime"} {
		out[k] = sc[k]
	}
	if l := latest(sc); l != nil {
		out["LatestSchemaVersion"] = l["VersionNumber"]
	}
	vs := versions(sc)
	out["NextSchemaVersion"] = n64(vs[len(vs)-1]["VersionNumber"]) + 1
	return out
}

// ---- registries ----

func (s *Service) createRegistry(c *core.Ctx, in *struct {
	RegistryName, Description string
	Tags                      map[string]string
}) (any, error) {
	if err := checkRegistryName(in.RegistryName); err != nil {
		return nil, err
	}
	at := iso()
	r := obj{"RegistryName": in.RegistryName, "RegistryArn": registryArn(c, in.RegistryName), "Description": in.Description, "Status": "AVAILABLE", "CreatedTime": at, "UpdatedTime": at}
	if err := s.put(c, "Registry", in.RegistryName, r, in.Tags); err != nil {
		return nil, err
	}
	out := obj{"RegistryName": in.RegistryName, "RegistryArn": r["RegistryArn"], "Description": in.Description}
	if len(in.Tags) > 0 {
		out["Tags"] = in.Tags
	}
	return out, nil
}

func checkRegistryName(n string) error {
	if n == "" || len(n) > 255 || strings.Trim(n, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_$#.") != "" {
		return invalid("The name %q is not valid: use 1 to 255 letters, digits and -_$#.", n)
	}
	return nil
}

func (s *Service) getRegistry(c *core.Ctx, in *struct{ RegistryId *registryID }) (any, error) {
	name, err := registryName(in.RegistryId)
	if err != nil {
		return nil, err
	}
	r, err := s.get(c, "Registry", name)
	if err != nil {
		return nil, notFound("Registry %s not found.", name)
	}
	return r, nil
}

func (s *Service) listRegistries(c *core.Ctx, in *pageInput) (any, error) {
	items, next := page(s.all(c, "Registry", ""), in.NextToken, in.MaxResults)
	return listOut("Registries", items, next), nil
}

func (s *Service) updateRegistry(c *core.Ctx, in *struct {
	RegistryId  *registryID
	Description string
}) (any, error) {
	name, err := registryName(in.RegistryId)
	if err != nil {
		return nil, err
	}
	r, err := s.change(c, "Registry", name, func(r obj) error { r["Description"], r["UpdatedTime"] = in.Description, iso(); return nil })
	if err != nil {
		return nil, notFound("Registry %s not found.", name)
	}
	return obj{"RegistryName": name, "RegistryArn": r["RegistryArn"]}, nil
}

// deleteRegistry deletes a registry with its schemas.
func (s *Service) deleteRegistry(c *core.Ctx, in *struct{ RegistryId *registryID }) (any, error) {
	name, err := registryName(in.RegistryId)
	if err != nil {
		return nil, err
	}
	r, err := s.get(c, "Registry", name)
	if err != nil {
		return nil, notFound("Registry %s not found.", name)
	}
	s.mu.Lock()
	for k := range s.st.Objects {
		if strings.HasPrefix(k, "Schema/"+key(c, name, "")) {
			delete(s.st.Objects, k)
		}
	}
	for k := range s.st.Tags {
		if strings.HasPrefix(k, key(c, "schema", name, "")) {
			delete(s.st.Tags, k)
		}
	}
	s.mu.Unlock()
	if err := s.drop(c, "Registry", name); err != nil {
		return nil, err
	}
	return obj{"RegistryName": name, "RegistryArn": r["RegistryArn"], "Status": "DELETING"}, nil
}

// ---- schemas ----

func (s *Service) createSchema(c *core.Ctx, in *struct {
	RegistryId                                                           *registryID
	SchemaName, DataFormat, Compatibility, Description, SchemaDefinition string
	Tags                                                                 map[string]string
}) (any, error) {
	reg, err := registryName(in.RegistryId)
	if err != nil {
		return nil, err
	}
	comp := cmp.Or(in.Compatibility, "BACKWARD")
	switch {
	case checkRegistryName(in.SchemaName) != nil:
		return nil, checkRegistryName(in.SchemaName)
	case !slices.Contains(compatModes, comp):
		return nil, invalid("Compatibility must be one of %s.", strings.Join(compatModes, ", "))
	}
	if _, err := canonical(in.DataFormat, in.SchemaDefinition); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.st.Objects["Registry/"+key(c, reg)] == nil {
		if reg != defaultRegistry {
			s.mu.Unlock()
			return nil, notFound("Registry %s not found.", reg)
		}
		at := iso()
		s.st.Objects["Registry/"+key(c, reg)] = obj{"RegistryName": reg, "RegistryArn": registryArn(c, reg), "Description": "", "Status": "AVAILABLE", "CreatedTime": at, "UpdatedTime": at}
	}
	s.mu.Unlock()
	at, v := iso(), newVersion(1, in.SchemaDefinition)
	sc := obj{"RegistryName": reg, "RegistryArn": registryArn(c, reg), "SchemaName": in.SchemaName, "SchemaArn": schemaArn(c, reg, in.SchemaName),
		"Description": in.Description, "DataFormat": in.DataFormat, "Compatibility": comp, "SchemaCheckpoint": int64(1), "SchemaStatus": "AVAILABLE",
		"CreatedTime": at, "UpdatedTime": at, "Versions": []any{v}}
	if err := s.put(c, "Schema", reg+"/"+in.SchemaName, sc, in.Tags); err != nil {
		return nil, err
	}
	out := schemaOut(sc)
	out["SchemaVersionId"], out["SchemaVersionStatus"] = v["SchemaVersionId"], "AVAILABLE"
	delete(out, "CreatedTime")
	delete(out, "UpdatedTime")
	if len(in.Tags) > 0 {
		out["Tags"] = in.Tags
	}
	return out, nil
}

// schema finds a schema from a SchemaId.
func (s *Service) schema(c *core.Ctx, id *schemaID) (obj, error) {
	reg, name, err := schemaName(id)
	if err != nil {
		return nil, err
	}
	sc, err := s.get(c, "Schema", reg+"/"+name)
	if err != nil {
		return nil, notFound("Schema %s not found in registry %s.", name, reg)
	}
	return sc, nil
}

func (s *Service) getSchema(c *core.Ctx, in *struct{ SchemaId *schemaID }) (any, error) {
	sc, err := s.schema(c, in.SchemaId)
	if err != nil {
		return nil, err
	}
	return schemaOut(sc), nil
}

func (s *Service) listSchemas(c *core.Ctx, in *struct {
	RegistryId *registryID
	pageInput
}) (any, error) {
	prefix := ""
	if in.RegistryId != nil {
		reg, err := registryName(in.RegistryId)
		if err != nil {
			return nil, err
		}
		prefix = reg + "/"
	}
	var out []obj
	for _, sc := range s.all(c, "Schema", prefix) {
		o := obj{}
		for _, k := range []string{"RegistryName", "SchemaName", "SchemaArn", "Description", "SchemaStatus", "CreatedTime", "UpdatedTime"} {
			o[k] = sc[k]
		}
		out = append(out, o)
	}
	items, next := page(out, in.NextToken, in.MaxResults)
	return listOut("Schemas", items, next), nil
}

func (s *Service) updateSchema(c *core.Ctx, in *struct {
	SchemaId            *schemaID
	SchemaVersionNumber *versionRef
	Compatibility       string
	Description         *string
}) (any, error) {
	reg, name, err := schemaName(in.SchemaId)
	if err != nil {
		return nil, err
	}
	if in.Compatibility != "" && !slices.Contains(compatModes, in.Compatibility) {
		return nil, invalid("Compatibility must be one of %s.", strings.Join(compatModes, ", "))
	}
	sc, err := s.change(c, "Schema", reg+"/"+name, func(sc obj) error {
		if in.Compatibility != "" {
			sc["Compatibility"] = in.Compatibility
		}
		if in.Description != nil {
			sc["Description"] = *in.Description
		}
		if v := in.SchemaVersionNumber; v != nil {
			n := v.VersionNumber
			if v.LatestVersion {
				n = n64(latest(sc)["VersionNumber"])
			}
			if !slices.ContainsFunc(versions(sc), func(x obj) bool { return n64(x["VersionNumber"]) == n && x["Status"] != "DELETING" }) {
				return notFound("Schema version %d not found.", n)
			}
			sc["SchemaCheckpoint"] = n
		}
		sc["UpdatedTime"] = iso()
		return nil
	})
	if err != nil {
		return nil, cmpErr(err, notFound("Schema %s not found in registry %s.", name, reg))
	}
	return obj{"SchemaArn": sc["SchemaArn"], "SchemaName": name, "RegistryName": reg}, nil
}

// cmpErr is err, or alt when err says the schema is missing (the generic message names a kind).
func cmpErr(err, alt error) error {
	if strings.HasPrefix(err.Error(), "Schema ") && strings.Contains(err.Error(), " not found.") {
		return alt
	}
	return err
}

func (s *Service) deleteSchema(c *core.Ctx, in *struct{ SchemaId *schemaID }) (any, error) {
	sc, err := s.schema(c, in.SchemaId)
	if err != nil {
		return nil, err
	}
	if err := s.drop(c, "Schema", str(sc["RegistryName"])+"/"+str(sc["SchemaName"])); err != nil {
		return nil, err
	}
	return obj{"SchemaArn": sc["SchemaArn"], "SchemaName": sc["SchemaName"], "Status": "DELETING"}, nil
}

// ---- versions ----

func (s *Service) registerVersion(c *core.Ctx, in *struct {
	SchemaId         *schemaID
	SchemaDefinition string
}) (any, error) {
	reg, name, err := schemaName(in.SchemaId)
	if err != nil {
		return nil, err
	}
	var got obj
	_, err = s.change(c, "Schema", reg+"/"+name, func(sc obj) error {
		want, err := canonical(str(sc["DataFormat"]), in.SchemaDefinition)
		if err != nil {
			return err
		}
		for _, v := range versions(sc) {
			if have, _ := canonical(str(sc["DataFormat"]), str(v["SchemaDefinition"])); have == want && v["Status"] != "DELETING" {
				got = v // the same schema: the same version
				return nil
			}
		}
		if sc["Compatibility"] == "DISABLED" {
			return invalid("Schema %s has compatibility DISABLED: no new versions can be registered.", name)
		}
		vs := versions(sc)
		got = newVersion(n64(vs[len(vs)-1]["VersionNumber"])+1, in.SchemaDefinition)
		sc["Versions"] = append(slices.Clone(asAny(vs)), got)
		sc["UpdatedTime"] = iso()
		return nil
	})
	if err != nil {
		return nil, cmpErr(err, notFound("Schema %s not found in registry %s.", name, reg))
	}
	return obj{"SchemaVersionId": got["SchemaVersionId"], "VersionNumber": got["VersionNumber"], "Status": got["Status"]}, nil
}

func asAny(vs []obj) []any {
	out := make([]any, len(vs))
	for i, v := range vs {
		out[i] = v
	}
	return out
}

// findVersion locates a version by SchemaVersionId, or by SchemaId and number; lock not held.
func (s *Service) findVersion(c *core.Ctx, id *schemaID, versionID string, ref *versionRef) (sc, v obj, err error) {
	if versionID != "" {
		for _, o := range s.all(c, "Schema", "") {
			for _, x := range versions(o) {
				if x["SchemaVersionId"] == versionID {
					return o, x, nil
				}
			}
		}
		return nil, nil, notFound("Schema version %s not found.", versionID)
	}
	if sc, err = s.schema(c, id); err != nil {
		return nil, nil, err
	}
	if ref == nil {
		return nil, nil, invalid("SchemaVersionId, or SchemaId with SchemaVersionNumber, is required.")
	}
	if ref.LatestVersion {
		if v = latest(sc); v != nil {
			return sc, v, nil
		}
	}
	for _, x := range versions(sc) {
		if n64(x["VersionNumber"]) == ref.VersionNumber && !ref.LatestVersion {
			return sc, x, nil
		}
	}
	return nil, nil, notFound("Schema version not found.")
}

func (s *Service) getVersion(c *core.Ctx, in *struct {
	SchemaId            *schemaID
	SchemaVersionId     string
	SchemaVersionNumber *versionRef
}) (any, error) {
	sc, v, err := s.findVersion(c, in.SchemaId, in.SchemaVersionId, in.SchemaVersionNumber)
	if err != nil {
		return nil, err
	}
	return obj{"SchemaVersionId": v["SchemaVersionId"], "SchemaDefinition": v["SchemaDefinition"], "DataFormat": sc["DataFormat"], "SchemaArn": sc["SchemaArn"],
		"VersionNumber": v["VersionNumber"], "Status": v["Status"], "CreatedTime": v["CreatedTime"]}, nil
}

func (s *Service) listVersions(c *core.Ctx, in *struct {
	SchemaId *schemaID
	pageInput
}) (any, error) {
	sc, err := s.schema(c, in.SchemaId)
	if err != nil {
		return nil, err
	}
	var out []obj
	for _, v := range versions(sc) {
		out = append(out, obj{"SchemaArn": sc["SchemaArn"], "SchemaVersionId": v["SchemaVersionId"], "VersionNumber": v["VersionNumber"], "Status": v["Status"], "CreatedTime": v["CreatedTime"]})
	}
	items, next := page(out, in.NextToken, in.MaxResults)
	return listOut("Schemas", items, next), nil
}

func (s *Service) schemaByDefinition(c *core.Ctx, in *struct {
	SchemaId         *schemaID
	SchemaDefinition string
}) (any, error) {
	sc, err := s.schema(c, in.SchemaId)
	if err != nil {
		return nil, err
	}
	want, err := canonical(str(sc["DataFormat"]), in.SchemaDefinition)
	if err != nil {
		return nil, err
	}
	for _, v := range versions(sc) {
		if have, _ := canonical(str(sc["DataFormat"]), str(v["SchemaDefinition"])); have == want && v["Status"] != "DELETING" {
			return obj{"SchemaVersionId": v["SchemaVersionId"], "SchemaArn": sc["SchemaArn"], "DataFormat": sc["DataFormat"], "Status": v["Status"], "CreatedTime": v["CreatedTime"]}, nil
		}
	}
	return nil, notFound("The schema definition is not a version of schema %s.", sc["SchemaName"])
}

func (s *Service) checkValidity(c *core.Ctx, in *struct{ DataFormat, SchemaDefinition string }) (any, error) {
	if _, err := canonical(in.DataFormat, in.SchemaDefinition); err != nil {
		if code(err) == "InvalidInputException" && slices.Contains([]string{"AVRO", "JSON", "PROTOBUF"}, in.DataFormat) {
			return obj{"Valid": false, "Error": err.Error()}, nil
		}
		return nil, err
	}
	return obj{"Valid": true}, nil
}

// deleteVersions marks versions DELETING (the checkpoint and the latest one cannot go, as in AWS).
func (s *Service) deleteVersions(c *core.Ctx, in *struct {
	SchemaId *schemaID
	Versions string
}) (any, error) {
	reg, name, err := schemaName(in.SchemaId)
	if err != nil {
		return nil, err
	}
	nums, err := parseRange(in.Versions)
	if err != nil {
		return nil, err
	}
	var errs []obj
	_, err = s.change(c, "Schema", reg+"/"+name, func(sc obj) error {
		var vs []any
		for _, v := range versions(sc) {
			v = maps.Clone(v)
			n := n64(v["VersionNumber"])
			if slices.Contains(nums, n) && v["Status"] != "DELETING" {
				if n == n64(sc["SchemaCheckpoint"]) {
					errs = append(errs, obj{"VersionNumber": n, "ErrorDetails": obj{"ErrorCode": "InvalidInputException", "ErrorMessage": "The checkpoint version cannot be deleted."}})
				} else {
					v["Status"] = "DELETING"
				}
			}
			vs = append(vs, v)
		}
		if latest(obj{"Versions": vs}) == nil {
			return invalid("A schema must keep a version.")
		}
		sc["Versions"] = vs
		return nil
	})
	if err != nil {
		return nil, cmpErr(err, notFound("Schema %s not found in registry %s.", name, reg))
	}
	return obj{"SchemaVersionErrors": errs}, nil
}

// parseRange reads "1-3,5" into [1 2 3 5].
func parseRange(s string) ([]int64, error) {
	var out []int64
	for _, part := range strings.Split(s, ",") {
		lo, hi, isRange := strings.Cut(strings.TrimSpace(part), "-")
		a, err1 := strconv.ParseInt(lo, 10, 64)
		b, err2 := a, error(nil)
		if isRange {
			b, err2 = strconv.ParseInt(hi, 10, 64)
		}
		if err1 != nil || err2 != nil || b < a || b-a > 1000 {
			return nil, invalid("Versions %q is not a list of numbers and ranges such as 1-3,5.", s)
		}
		for n := a; n <= b; n++ {
			out = append(out, n)
		}
	}
	return out, nil
}

// ---- version metadata ----

type metaInput struct {
	SchemaId            *schemaID
	SchemaVersionNumber *versionRef
	SchemaVersionId     string
	MetadataKeyValue    struct{ MetadataKey, MetadataValue string }
}

// editMeta changes the metadata of a version and stores the schema.
func (s *Service) editMeta(c *core.Ctx, in *metaInput, f func(m obj) error) (any, error) {
	sc, v, err := s.findVersion(c, in.SchemaId, in.SchemaVersionId, in.SchemaVersionNumber)
	if err != nil {
		return nil, err
	}
	reg, name := str(sc["RegistryName"]), str(sc["SchemaName"])
	_, err = s.change(c, "Schema", reg+"/"+name, func(sc obj) error {
		var vs []any
		for _, x := range versions(sc) {
			if x["SchemaVersionId"] == v["SchemaVersionId"] {
				x = maps.Clone(x)
				m := maps.Clone(obj(nil))
				if old, ok := x["Metadata"].(obj); ok {
					m = maps.Clone(old)
				}
				if m == nil {
					m = obj{}
				}
				if err := f(m); err != nil {
					return err
				}
				x["Metadata"] = m
			}
			vs = append(vs, x)
		}
		sc["Versions"] = vs
		return nil
	})
	if err != nil {
		return nil, err
	}
	kv := in.MetadataKeyValue
	return obj{"SchemaArn": sc["SchemaArn"], "SchemaName": name, "RegistryName": reg, "LatestVersion": latest(sc)["SchemaVersionId"] == v["SchemaVersionId"],
		"VersionNumber": v["VersionNumber"], "SchemaVersionId": v["SchemaVersionId"], "MetadataKey": kv.MetadataKey, "MetadataValue": kv.MetadataValue}, nil
}

func (s *Service) putMeta(c *core.Ctx, in *metaInput) (any, error) {
	kv := in.MetadataKeyValue
	if kv.MetadataKey == "" || kv.MetadataValue == "" {
		return nil, invalid("MetadataKeyValue needs a MetadataKey and a MetadataValue.")
	}
	return s.editMeta(c, in, func(m obj) error {
		if cur, ok := m[kv.MetadataKey].(obj); ok { // a key keeps its earlier values
			if cur["MetadataValue"] == kv.MetadataValue {
				return exists("Metadata %s=%s already exists.", kv.MetadataKey, kv.MetadataValue)
			}
			others := append(slices.Clone(asAnyList(cur["OtherMetadataValueList"])), obj{"MetadataValue": cur["MetadataValue"], "CreatedTime": cur["CreatedTime"]})
			m[kv.MetadataKey] = obj{"MetadataValue": kv.MetadataValue, "CreatedTime": iso(), "OtherMetadataValueList": others}
			return nil
		}
		if len(m) >= 10 {
			return core.Errorf(400, "ResourceNumberLimitExceededException", "A schema version can have at most 10 metadata keys.")
		}
		m[kv.MetadataKey] = obj{"MetadataValue": kv.MetadataValue, "CreatedTime": iso()}
		return nil
	})
}

func asAnyList(v any) []any { l, _ := v.([]any); return l }

func (s *Service) removeMeta(c *core.Ctx, in *metaInput) (any, error) {
	kv := in.MetadataKeyValue
	return s.editMeta(c, in, func(m obj) error {
		cur, _ := m[kv.MetadataKey].(obj)
		if cur == nil || kv.MetadataValue != "" && cur["MetadataValue"] != kv.MetadataValue {
			return notFound("Metadata %s=%s not found.", kv.MetadataKey, kv.MetadataValue)
		}
		delete(m, kv.MetadataKey)
		return nil
	})
}

func (s *Service) queryMeta(c *core.Ctx, in *struct {
	SchemaId            *schemaID
	SchemaVersionNumber *versionRef
	SchemaVersionId     string
	MetadataList        []struct{ MetadataKey, MetadataValue string }
	pageInput
}) (any, error) {
	_, v, err := s.findVersion(c, in.SchemaId, in.SchemaVersionId, in.SchemaVersionNumber)
	if err != nil {
		return nil, err
	}
	out := obj{}
	m, _ := v["Metadata"].(obj)
	for _, k := range core.SortedKeys(m) {
		e, _ := m[k].(obj)
		if e == nil {
			continue
		}
		keep := len(in.MetadataList) == 0
		for _, f := range in.MetadataList {
			keep = keep || f.MetadataKey == k && (f.MetadataValue == "" || f.MetadataValue == fmt.Sprint(e["MetadataValue"]))
		}
		if keep {
			out[k] = e
		}
	}
	return obj{"MetadataInfoMap": out, "SchemaVersionId": v["SchemaVersionId"]}, nil
}
