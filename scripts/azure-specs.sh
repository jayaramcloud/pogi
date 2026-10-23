#!/bin/sh
# Refreshes internal/azure/generic/specs.json.gz from Microsoft's azure-rest-api-specs (MIT, see NOTICE) at a pinned commit.
# Run by `go generate ./internal/azure/generic`. Needs curl and python3.
# It fetches only the spec files that services.txt names and keeps, per listed resource type, what the engine
# needs: API versions, the operations, which of them are long-running, read-only and required properties,
# whether the type is tracked (has location and tags) and the constraints on its name.
set -eu
commit=1355fe2fc33e3e3d4b69b026392f887239c87fe9
root=$(cd "$(dirname "$0")/.." && pwd)
dir=$root/internal/azure/generic
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

grep -v '^#' "$dir/services.txt" | grep . | cut -d'|' -f3 | tr ',' '\n' | tr -d ' ' | sort -u | while read -r f; do
  mkdir -p "$work/$(dirname "$f")"
  curl -fsSL -o "$work/$f" "https://raw.githubusercontent.com/Azure/azure-rest-api-specs/$commit/specification/$f"
done
python3 -I - "$work" "$dir/services.txt" "$dir/specs.json.gz" <<'PY'
import gzip, json, os, re, sys

work, txt, out = sys.argv[1:]
METHODS = ("get", "put", "patch", "delete")
TRACKED = ("TrackedResource",)  # common-types definitions that carry location and tags


def doc_at(doc, ref):
    node = doc
    for part in ref[2:].split("/"):
        node = node[part]
    return node


def flatten(doc, schema):
    """The properties and required names of a schema, with its allOf parents folded in."""
    schema = resolve_ref(doc, schema)
    props, req = {}, []
    if schema.get("x-common") in TRACKED:
        props = {"location": {}, "tags": {}}
    for parent in schema.get("allOf", []):
        p, r = flatten(doc, parent)
        props.update(p)
        req += r
    props.update(schema.get("properties", {}))
    req += schema.get("required", [])
    return props, req


def resolve_ref(doc, node):
    while "$ref" in node:
        if not node["$ref"].startswith("#/"):
            name = node["$ref"].rsplit("/", 1)[-1]
            if node["$ref"].endswith("network.json#/definitions/Resource"):
                name = "TrackedResource"  # Microsoft.Network's own Resource has location and tags
            return {"x-common": name}
        node = doc_at(doc, node["$ref"])
    return node


def param(doc, p):
    if "$ref" in p:
        return doc_at(doc, p["$ref"]) if p["$ref"].startswith("#/") else {}
    return p


def type_chain(doc, path, params):
    """The resource types a path can stand for: [(namespace/type..., [type slot values]), ...] or None."""
    i = path.rfind("/providers/")
    if i < 0:
        return None
    seg = path[i + len("/providers/"):].split("/")
    if len(seg) < 3 or len(seg) % 2 == 0:  # a collection or an action is not an item
        return None
    ns, rest = seg[0], seg[1:]
    chains = [ns]
    for j in range(0, len(rest), 2):
        t = rest[j]
        if t.startswith("{"):
            vals = params.get(t[1:-1], {}).get("enum")
            if not vals:
                return None
        else:
            vals = [t]
        chains = [c + "/" + v for c in chains for v in vals]
    last = rest[-1]
    name = params.get(last[1:-1], {}) if last.startswith("{") else {}
    return chains, name


def body_info(doc, op):
    for p in op.get("parameters", []):
        p = param(doc, p)
        if p.get("in") == "body":
            props, req = flatten(doc, p["schema"])
            sub, subreq = {}, []
            if "properties" in props:
                sub, subreq = flatten(doc, props["properties"])
            ro = sorted(k for k, v in sub.items() if resolve_ref(doc, v).get("readOnly") or v.get("readOnly"))
            required = [r for r in req if r not in ("id", "name", "type")] + ["properties." + r for r in subreq]
            return {"ro": ro, "req": sorted(set(required)), "tracked": "location" in props and "tags" in props}
    return {"ro": [], "req": [], "tracked": False}


def merge(dst, src):
    dst["v"] = sorted(set(dst.get("v", []) + src["v"]))
    dst["ops"] = sorted(set(dst.get("ops", []) + src["ops"]))
    dst["lro"] = sorted(set(dst.get("lro", []) + src["lro"]))
    for k in ("ro", "req", "tracked", "name", "created"):
        if src.get(k) not in (None, [], {}, False) or k not in dst:
            dst[k] = src.get(k, dst.get(k))


result = {}
for line in open(txt):
    if line.startswith("#") or not line.strip():
        continue
    svc, title, files, types = [f.strip() for f in line.split("|")]
    want = types.split()
    found = {}
    for f in files.split(","):
        f = f.strip()
        doc = json.load(open(os.path.join(work, f)))
        ver = doc["info"]["version"]
        for path, item in doc["paths"].items():
            common = {p["name"]: p for p in (param(doc, x) for x in item.get("parameters", [])) if "name" in p}
            ops = {m: item[m] for m in METHODS if m in item}
            if "put" not in ops:
                continue
            allp = dict(common)
            for o in ops.values():
                allp.update({p["name"]: p for p in (param(doc, x) for x in o.get("parameters", [])) if "name" in p})
            chain = type_chain(doc, path, allp)
            if chain is None:
                continue
            for typ in chain[0]:
                if typ not in want:
                    continue
                nm = {k: v for k, v in (("pattern", chain[1].get("pattern")), ("min", chain[1].get("minLength")), ("max", chain[1].get("maxLength"))) if v is not None}
                # Go's regexp has no lookahead: a leading length check becomes min and max, any other lookahead is dropped
                m = re.match(r"\^\(\?=\.\{(\d+),(\d+)\}\$\)", nm.get("pattern", ""))
                if m:
                    nm["pattern"], nm["min"], nm["max"] = "^" + nm["pattern"][m.end():], int(m.group(1)), int(m.group(2))
                if "(?" in nm.get("pattern", "").replace("(?:", ""):
                    del nm["pattern"]
                rec = {"v": [ver], "ops": sorted(ops), "lro": sorted(m for m, o in ops.items() if o.get("x-ms-long-running-operation")),
                       "name": nm, **body_info(doc, ops["put"])}
                # the SDKs accept only the statuses of the spec: 201 for a create unless the PUT has no 201
                codes = ops["put"]["responses"]
                if "201" not in codes:
                    rec["created"] = 202 if "202" in codes else 200
                if typ in found:
                    merge(found[typ], rec)
                else:
                    found[typ] = rec
    missing = [t for t in want if t not in found]
    if missing:
        sys.exit("%s: not found in %s: %s" % (svc, files, " ".join(missing)))
    result[svc] = {"title": title, "types": {t: found[t] for t in want}}
    print(svc, len(found), "types")
with gzip.GzipFile(out, "wb", 9, mtime=0) as f:
    f.write(json.dumps(result, separators=(",", ":"), sort_keys=True).encode())
PY
