package generic

import (
	"encoding/json"
	"fmt"

	"github.com/nrndev/pogi/internal/core"
)

// obj returns m[k] as an object, making it when it is missing.
func obj(m map[string]any, k string) map[string]any {
	o, _ := m[k].(map[string]any)
	if o == nil {
		o = map[string]any{}
		m[k] = o
	}
	return o
}

// setDefault sets m[k] when the client did not.
func setDefault(m map[string]any, k string, v any) {
	if m[k] == nil {
		m[k] = v
	}
}

func init() {
	apiHooks["dataproc"] = hooks{
		// the id of a cluster is its clusterName, in the body; the region is in the path
		required: map[string][]string{"Cluster": {"clusterName"}},
		created: map[string]func(*call, map[string]any){"Cluster": func(c *call, cl map[string]any) {
			project, region, name := c.segs[1], c.segs[3], cl["clusterName"].(string)
			cl["projectId"] = project
			cl["clusterUuid"] = core.UUID()
			cl["status"] = map[string]any{"state": "RUNNING", "stateStartTime": now()}
			cfg := obj(cl, "config")
			setDefault(cfg, "configBucket", "dataproc-staging-"+region+"-"+name)
			setDefault(cfg, "tempBucket", "dataproc-temp-"+region+"-"+name)
			gce := obj(cfg, "gceClusterConfig")
			setDefault(gce, "zoneUri", fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/zones/%s-a", project, region))
			setDefault(gce, "networkUri", fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/global/networks/default", project))
			sw := obj(cfg, "softwareConfig")
			setDefault(sw, "imageVersion", "2.2-debian12")
			setDefault(sw, "properties", map[string]any{})
			obj(cfg, "endpointConfig")
			// the machines: names a user can read back; nothing runs on them
			for _, g := range []string{"masterConfig", "workerConfig"} {
				ig := obj(cfg, g)
				setDefault(ig, "numInstances", json.Number("0"))
				setDefault(ig, "diskConfig", map[string]any{"bootDiskSizeGb": 1000, "bootDiskType": "pd-standard"})
				n := 0
				if v, ok := ig["numInstances"].(json.Number); ok {
					n64, _ := v.Int64()
					n = int(n64)
				}
				var names []any
				for i := 0; i < n; i++ {
					suffix := "m"
					if g == "workerConfig" {
						suffix = "w"
					}
					if g == "workerConfig" || i > 0 {
						suffix = fmt.Sprintf("%s-%d", suffix, i)
					}
					names = append(names, name+"-"+suffix)
				}
				ig["instanceNames"] = names
			}
		}},
	}
}
