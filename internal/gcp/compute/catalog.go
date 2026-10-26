package compute

import (
	"slices"
	"strconv"
	"strings"

	"github.com/nrndev/pogi/internal/gcp/gapi"
)

// regions and their zones: <region>-a, -b and -c.
var regionNames = gapi.Regions

func zonesOf(region string) []string { return []string{region + "-a", region + "-b", region + "-c"} }

func regionOf(zone string) string { return zone[:max(strings.LastIndexByte(zone, '-'), 0)] }

func validRegion(r string) bool { return slices.Contains(regionNames, r) }

func validZone(z string) bool {
	return validRegion(regionOf(z)) && slices.Contains(zonesOf(regionOf(z)), z)
}

type machine struct {
	name      string
	cpus, mem int // memory in MB
}

var machines = []machine{{"e2-micro", 2, 1024}, {"e2-small", 2, 2048}, {"e2-medium", 2, 4096}, {"e2-standard-2", 2, 8192},
	{"e2-standard-4", 4, 16384}, {"e2-standard-8", 8, 32768}, {"f1-micro", 1, 614}, {"g1-small", 1, 1740}, {"n1-standard-1", 1, 3840},
	{"n1-standard-2", 2, 7680}, {"n1-standard-4", 4, 15360}, {"n2-standard-2", 2, 8192}, {"n2-standard-4", 4, 16384}, {"c2-standard-4", 4, 16384}}

var diskTypes = []string{"pd-standard", "pd-balanced", "pd-ssd"}

// image is a public image; its instances run the container image docker.
type image struct{ project, family, name, prefix, docker string }

var images = []image{
	{"debian-cloud", "debian-12", "debian-12-bookworm-v20260112", "debian-12", "debian:12"},
	{"debian-cloud", "debian-11", "debian-11-bullseye-v20260112", "debian-11", "debian:11"},
	{"debian-cloud", "debian-13", "debian-13-trixie-v20260112", "debian-13", "debian:13"},
	{"ubuntu-os-cloud", "ubuntu-2204-lts", "ubuntu-2204-jammy-v20260112", "ubuntu-2204", "ubuntu:22.04"},
	{"ubuntu-os-cloud", "ubuntu-2404-lts-amd64", "ubuntu-2404-noble-amd64-v20260112", "ubuntu-2404", "ubuntu:24.04"},
	{"cos-cloud", "cos-stable", "cos-stable-121-18867-90-4", "cos-", "debian:12"},
	{"rocky-linux-cloud", "rocky-linux-9", "rocky-linux-9-v20260112", "rocky-linux-9", "rockylinux:9"},
}

// findImage resolves a source image reference: a URL, projects/p/global/images/[family/]x, p/x or a bare
// family or name. A version of a family that is not listed (...-v20240101) matches by prefix.
func findImage(ref string) *image {
	last := ref[strings.LastIndexByte(strings.TrimRight(ref, "/"), '/')+1:]
	last = strings.TrimRight(last, "/")
	for i := range images {
		if last == images[i].family || last == images[i].name {
			return &images[i]
		}
	}
	for i := range images {
		if strings.HasPrefix(last, images[i].prefix) {
			return &images[i]
		}
	}
	return nil
}

func (im *image) doc() map[string]any {
	return map[string]any{"kind": "compute#image", "id": strconv.Itoa(1000000 + len(im.name)), "creationTimestamp": "2026-01-12T00:00:00.000-08:00",
		"name": im.name, "family": im.family, "status": "READY", "archiveSizeBytes": "2147483648", "diskSizeGb": "10", "sourceType": "RAW",
		"selfLink": api + "projects/" + im.project + "/global/images/" + im.name, "labelFingerprint": "42WmSpB8rSM=", "architecture": "X86_64"}
}

func zoneDoc(z string) map[string]any {
	return map[string]any{"kind": "compute#zone", "id": strconv.Itoa(2000 + int(z[len(z)-1]) + len(z)), "creationTimestamp": "1969-12-31T16:00:00.000-08:00", "name": z,
		"status": "UP", "region": api + "projects/-/regions/" + regionOf(z), "selfLink": api + "projects/-/zones/" + z}
}

func regionDoc(r string) map[string]any {
	zs := []string{}
	for _, z := range zonesOf(r) {
		zs = append(zs, api+"projects/-/zones/"+z)
	}
	return map[string]any{"kind": "compute#region", "id": strconv.Itoa(1000 + len(r)*7), "creationTimestamp": "1969-12-31T16:00:00.000-08:00", "name": r,
		"status": "UP", "zones": zs, "selfLink": api + "projects/-/regions/" + r}
}

func (m machine) doc(project, zone string) map[string]any {
	return map[string]any{"kind": "compute#machineType", "id": strconv.Itoa(3000 + m.cpus*100 + m.mem/1024), "creationTimestamp": "1969-12-31T16:00:00.000-08:00",
		"name": m.name, "description": strconv.Itoa(m.cpus) + " vCPUs, " + strconv.Itoa(m.mem) + " MB RAM", "guestCpus": m.cpus, "memoryMb": m.mem,
		"imageSpaceGb": 0, "maximumPersistentDisks": 128, "maximumPersistentDisksSizeGb": "263168", "zone": zone, "isSharedCpu": m.cpus < 2,
		"selfLink": api + "projects/" + project + "/zones/" + zone + "/machineTypes/" + m.name}
}
