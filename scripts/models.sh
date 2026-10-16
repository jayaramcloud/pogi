#!/bin/sh
# Refreshes internal/aws/generic/models/<svc>.json.gz from a pinned botocore wheel (Apache-2.0, see NOTICE).
# Run by `go generate ./internal/aws/generic`. Needs curl, unzip and python3.
# Each file holds {"m": service-2.json, "p": paginators-1.json} minified, without the documentation strings.
# It also writes models/limits/<dir>.json.gz for the hand-written services in limits.txt: only the input
# members that carry limits (min, max, enum), which is all the shared limit check reads.
set -eu
version=1.43.111
sha256=f1f4c28cb2a096bf246d0bb24cbb1a01c5cb696ef499fa71b155adda7b94c90b
url=https://files.pythonhosted.org/packages/ad/5b/c3ce1b227954eb0313e76e6e7c0b5b24d4c553f0e8a03e5828ff5a5918dc/botocore-$version-py3-none-any.whl
root=$(cd "$(dirname "$0")/.." && pwd)
out=$root/internal/aws/generic/models
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

curl -fsSL -o "$work/botocore.whl" "$url"
echo "$sha256  $work/botocore.whl" | shasum -a 256 -c - >/dev/null
svcs=$(grep -v '^#' "$root/internal/aws/generic/services.txt" | tr -d ' \r' | grep .)
mkdir -p "$work/x" "$out"
lims=$(grep -v '^#' "$root/internal/aws/generic/limits.txt" | grep . | cut -d' ' -f2- | tr ' ' '\n' | sort -u | grep -vxF "$(echo "$svcs" | tr ' ' '\n')" || true)
for s in $svcs $lims; do unzip -q -o "$work/botocore.whl" "botocore/data/$s/*" -d "$work/x"; done
rm -rf "$out"/*.json.gz "$out"/index.json "$out"/limits
mkdir -p "$out/limits"
python3 -I - "$work/x/botocore/data" "$out" $svcs <<'PY'
import gzip, json, os, sys

def strip(v):
    if isinstance(v, dict):
        return {k: strip(x) for k, x in v.items() if k not in ("documentation", "documentationUrl", "examples")}
    if isinstance(v, list):
        return [strip(x) for x in v]
    return v

data, out, svcs = sys.argv[1], sys.argv[2], sys.argv[3:]
index = {}
for s in svcs:
    ver = sorted(d for d in os.listdir(os.path.join(data, s)) if os.path.isdir(os.path.join(data, s, d)))[-1]
    d = os.path.join(data, s, ver)
    doc = {"m": strip(json.load(gzip.open(os.path.join(d, "service-2.json.gz"))))}
    md = doc["m"]["metadata"]
    index[s] = {k: v for k, v in {
        "protocol": md["protocol"], "jsonVersion": md.get("jsonVersion"), "targetPrefix": md.get("targetPrefix"),
        "endpointPrefix": md["endpointPrefix"], "signingName": md.get("signingName"), "apiVersion": md["apiVersion"],
        "serviceId": md.get("serviceId"), "title": md.get("serviceFullName"),
    }.items() if v}
    pg = os.path.join(d, "paginators-1.json")
    if os.path.exists(pg):
        doc["p"] = json.load(open(pg)).get("pagination", {})
    with gzip.GzipFile(os.path.join(out, s + ".json.gz"), "wb", 9, mtime=0) as f:
        f.write(json.dumps(doc, separators=(",", ":"), sort_keys=True).encode())
    print(s, ver)
with open(os.path.join(out, "index.json"), "w") as f:
    json.dump(index, f, indent=1, sort_keys=True)
PY
python3 -I - "$work/x/botocore/data" "$out/limits" "$root/internal/aws/generic/limits.txt" $lims <<'PY'
import gzip, json, os, sys

data, out, txt, dirs = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4:]
SCALARS = ("integer", "long", "float", "double")

def lean(doc):
    shapes = doc["shapes"]

    def direct(sh):  # does the shape carry a limit itself
        t, lo, hi = sh["type"], sh.get("min"), sh.get("max")
        if t in SCALARS:
            return lo is not None or hi is not None
        if t == "string":
            return bool(sh.get("enum")) or bool(lo) or hi is not None
        if t in ("list", "map"):
            return bool(lo) or hi is not None
        return False

    def kids(sh):
        refs = list(sh.get("members", {}).values())
        refs += [sh[k] for k in ("member", "key", "value") if k in sh]
        return [r["shape"] for r in refs]

    held = {n for n, sh in shapes.items() if direct(sh)}
    while True:  # a shape holds limits when any shape inside it does
        more = {n for n, sh in shapes.items() if n not in held and any(k in held for k in kids(sh))}
        if not more:
            break
        held |= more

    def ref(r):
        return {k: r[k] for k in ("shape", "location", "locationName", "queryName", "flattened", "timestampFormat") if k in r}

    kept = {}

    def emit(n):
        if n in kept:
            return
        sh = shapes[n]
        o = kept[n] = {"type": sh["type"]}
        if direct(sh):
            for k in ("min", "max", "enum"):
                if sh.get(k) is not None and not (k == "min" and sh["type"] not in SCALARS and sh[k] == 0):
                    o[k] = sh[k]
        for k in ("flattened", "locationName", "timestampFormat"):
            if k in sh:
                o[k] = sh[k]
        if sh["type"] == "structure":
            ms = {m: ref(r) for m, r in sh.get("members", {}).items() if r["shape"] in held}
            if ms:
                o["members"] = ms
            if sh.get("payload") in ms:
                o["payload"] = sh["payload"]
        for k in ("member", "key", "value"):
            if k in sh:
                o[k] = ref(sh[k])
        for r in list(o.get("members", {}).values()) + [o[k] for k in ("member", "key", "value") if k in o]:
            emit(r["shape"])

    ops = {}
    for n, op in doc["operations"].items():
        o = {"name": n, "http": {k: v for k, v in op["http"].items() if k in ("method", "requestUri")}}
        if "input" in op and op["input"]["shape"] in held:
            o["input"] = {"shape": op["input"]["shape"]}
            emit(op["input"]["shape"])
        ops[n] = o
    md = doc["metadata"]
    keep = ("protocol", "jsonVersion", "targetPrefix", "endpointPrefix", "signingName", "apiVersion", "serviceId")
    return {"m": {"metadata": {k: md[k] for k in keep if k in md}, "operations": ops, "shapes": kept}}

for s in dirs:
    ver = sorted(d for d in os.listdir(os.path.join(data, s)) if os.path.isdir(os.path.join(data, s, d)))[-1]
    doc = lean(json.load(gzip.open(os.path.join(data, s, ver, "service-2.json.gz"))))
    with gzip.GzipFile(os.path.join(out, s + ".json.gz"), "wb", 9, mtime=0) as f:
        f.write(json.dumps(doc, separators=(",", ":"), sort_keys=True).encode())
    print("limits", s, ver)
index = {}
for line in open(txt):
    p = line.split()
    if p and not p[0].startswith("#"):
        index[p[0]] = p[1:]
with open(os.path.join(out, "index.json"), "w") as f:
    json.dump(index, f, indent=1, sort_keys=True)
PY
