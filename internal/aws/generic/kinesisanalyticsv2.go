package generic

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3io"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// Managed Service for Apache Flink: an application is READY, goes STARTING then
// RUNNING when started and STOPPING then READY when stopped. What the caller
// sets in ApplicationConfiguration (or ApplicationConfigurationUpdate) is kept
// as the description that DescribeApplication shows. With Docker, StartApplication
// runs the application's JAR from S3 on a real Flink cluster (see flinkUp) and the
// status follows it, and a job that ends by itself makes the application READY. After a pogi restart
// the applications that were running start again. Without Docker nothing runs and the statuses are on a timer.
func init() {
	OnClose("kinesisanalyticsv2", func(s *Service) { s.closeBoxes() })
	resumers["kinesisanalyticsv2"] = func(s *Service) { s.resume("Application", flinkWasUp, flinkStart) }
	Derive("kinesisanalyticsv2", "Application", func(r *Resource) map[string]any {
		out := map[string]any{"ApplicationVersionId": orDefault(r.Data["_ver"], 1.0), "ApplicationConfigurationDescription": r.Data["_desc"]}
		if id := str(r.Data["_box"]); id != "" { // a real cluster: anything but starting, running or stopping is READY
			out["ApplicationStatus"] = cmp.Or(map[string]string{"starting": "STARTING", "running": "RUNNING", "stopping": "STOPPING"}[boxOf(id).state], "READY")
			return out
		}
		switch r.Data["_phase"] {
		case "start":
			out["ApplicationStatus"] = Step(r, "app", "STARTING", "RUNNING")
		case "stop":
			out["ApplicationStatus"] = Step(r, "app", "STOPPING", "READY")
		}
		return out
	})
	Derive("kinesisanalyticsv2", "ApplicationSnapshot", func(r *Resource) map[string]any { // CREATING, then READY
		return map[string]any{"SnapshotStatus": Step(r, "snap", "CREATING", "READY")}
	})
	Override("kinesisanalyticsv2", "CreateApplication", func(c *Call) (map[string]any, error) {
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		desc := kaDefaults(str(r.Data["RuntimeEnvironment"]))
		kaMerge(desc, kaDescribe(r.Data["ApplicationConfiguration"]))
		r.Data["_desc"], r.Data["_ver"] = desc, 1.0
		if lo := asList(r.Data["CloudWatchLoggingOptions"]); len(lo) > 0 {
			var ds []any
			for i, l := range lo {
				ds = append(ds, map[string]any{"CloudWatchLoggingOptionId": "1." + string(rune('1'+i)), "LogStreamARN": l.(map[string]any)["LogStreamARN"]})
			}
			r.Data["CloudWatchLoggingOptionDescriptions"] = ds
		}
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("kinesisanalyticsv2", "UpdateApplication", func(c *Call) (map[string]any, error) {
		cur := c.Get("Application", str(c.In["ApplicationName"]))
		if cur == nil {
			return nil, c.NotFound("Application %s was not found.", str(c.In["ApplicationName"]))
		}
		if v, ok := c.In["CurrentApplicationVersionId"].(float64); ok && v != orDefault(cur.Data["_ver"], 1.0) {
			return nil, core.Errorf(400, "ConcurrentModificationException", "The application version %v is not the current one.", v)
		}
		upd := c.In["ApplicationConfigurationUpdate"]
		delete(c.In, "ApplicationConfigurationUpdate")
		delete(c.In, "CurrentApplicationVersionId")
		if _, err := c.Default(); err != nil {
			return nil, err
		}
		r := c.Res
		desc, _ := r.Data["_desc"].(map[string]any)
		kaMerge(desc, kaDescribe(upd))
		r.Data["_desc"] = desc
		ver, _ := orDefault(cur.Data["_ver"], 1.0).(float64)
		r.Data["_ver"] = ver + 1
		c.Put(r)
		return c.Fill(r), nil
	})
	Override("kinesisanalyticsv2", "DeleteApplication", func(c *Call) (map[string]any, error) {
		if r := c.Get("Application", str(c.In["ApplicationName"])); r != nil {
			if st := str(derived("kinesisanalyticsv2", r)["ApplicationStatus"]); st != "" && st != "READY" {
				return nil, core.Errorf(400, "ResourceInUseException", "Application %s is %s.", r.Name, st)
			}
		}
		return c.Default()
	})
	for _, op := range []string{"StartApplication", "StopApplication"} {
		Override("kinesisanalyticsv2", op, func(c *Call) (map[string]any, error) {
			r := c.Get("Application", str(c.In["ApplicationName"]))
			if r == nil {
				return nil, c.NotFound("Application %s was not found.", str(c.In["ApplicationName"]))
			}
			st := str(derived("kinesisanalyticsv2", r)["ApplicationStatus"])
			switch {
			case c.Op == "StartApplication" && st != "" && st != "READY":
				return nil, core.Errorf(400, "ResourceInUseException", "Application %s is %s.", r.Name, st)
			case c.Op == "StopApplication" && st != "STARTING" && st != "RUNNING":
				return nil, core.Errorf(400, "InvalidApplicationConfigurationException", "Application %s is not running.", r.Name)
			}
			r.Data["_phase"] = map[string]string{"StartApplication": "start", "StopApplication": "stop"}[c.Op]
			Begin(r, "app")
			if c.Op == "StopApplication" {
				stopBox(str(r.Data["_box"]))
			} else if err := flinkStart(c, r); err != nil {
				return nil, err
			}
			c.Put(r)
			return map[string]any{"OperationId": core.UUID()}, nil
		})
	}
}

// containers of ApplicationConfiguration whose description has another name.
var kaNames = map[string]string{
	"ApplicationConfiguration": "ApplicationConfigurationDescription", "ApplicationCodeConfiguration": "ApplicationCodeConfigurationDescription",
	"CodeContent": "CodeContentDescription", "S3ContentLocation": "S3ApplicationCodeLocationDescription",
	"FlinkApplicationConfiguration": "FlinkApplicationConfigurationDescription", "CheckpointConfiguration": "CheckpointConfigurationDescription",
	"MonitoringConfiguration": "MonitoringConfigurationDescription", "ParallelismConfiguration": "ParallelismConfigurationDescription",
	"ApplicationSnapshotConfiguration": "ApplicationSnapshotConfigurationDescription", "ApplicationEncryptionConfiguration": "ApplicationEncryptionConfigurationDescription",
	"ApplicationSystemRollbackConfiguration": "ApplicationSystemRollbackConfigurationDescription", "EnvironmentProperties": "EnvironmentPropertyDescriptions",
	"EnvironmentProperty": "EnvironmentPropertyDescriptions", "PropertyGroups": "PropertyGroupDescriptions", "VpcConfigurations": "VpcConfigurationDescriptions",
	"VpcConfiguration": "VpcConfigurationDescriptions", "ZeppelinApplicationConfiguration": "ZeppelinApplicationConfigurationDescription",
}

// kaDescribe renames an ApplicationConfiguration, or its update, into the shape of its description.
func kaDescribe(v any) map[string]any {
	m, _ := v.(map[string]any)
	out := map[string]any{}
	for k, x := range m {
		n := strings.TrimSuffix(strings.TrimSuffix(k, "Updates"), "Update")
		if k == "EnvironmentPropertyUpdates" {
			n = "EnvironmentProperty"
		}
		if d, ok := kaNames[n]; ok {
			n = d
		}
		switch t := x.(type) {
		case map[string]any:
			out[n] = kaDescribe(t)
		case []any:
			var l []any
			for i, e := range t {
				if em, ok := e.(map[string]any); ok {
					d := kaDescribe(em)
					if n == "VpcConfigurationDescriptions" && d["VpcConfigurationId"] == nil {
						d["VpcConfigurationId"] = "1." + string(rune('1'+i))
						d["VpcId"] = "vpc-" + randHex(8)
					}
					l = append(l, d)
				} else {
					l = append(l, e)
				}
			}
			out[n] = l
		default:
			out[n] = x
		}
	}
	return out
}

// kaMerge puts src over dst: structures merge, anything else replaces.
func kaMerge(dst, src map[string]any) {
	for k, v := range src {
		sm, sok := v.(map[string]any)
		dm, dok := dst[k].(map[string]any)
		if sok && dok {
			kaMerge(dm, sm)
		} else {
			dst[k] = v
		}
	}
}

// kaDefaults is what a Flink application shows before anything is set.
func kaDefaults(runtime string) map[string]any {
	if !strings.HasPrefix(runtime, "FLINK") {
		return map[string]any{}
	}
	return map[string]any{
		"FlinkApplicationConfigurationDescription": map[string]any{
			"CheckpointConfigurationDescription": map[string]any{"ConfigurationType": "DEFAULT", "CheckpointingEnabled": true, "CheckpointInterval": 60000.0, "MinPauseBetweenCheckpoints": 5000.0},
			"MonitoringConfigurationDescription": map[string]any{"ConfigurationType": "DEFAULT", "MetricsLevel": "APPLICATION", "LogLevel": "INFO"},
			"ParallelismConfigurationDescription": map[string]any{"ConfigurationType": "DEFAULT", "Parallelism": 1.0, "ParallelismPerKPU": 1.0,
				"CurrentParallelism": 1.0, "AutoScalingEnabled": false},
		},
		"ApplicationSnapshotConfigurationDescription": map[string]any{"SnapshotsEnabled": true},
	}
}

// flinkImages are the apache/flink tags of the runtime environments (FLINK-1_20 ...); others run the newest.
// The images of these tags are on Docker Hub; only 1.20.3 has an arm64 build, the rest run emulated there.
var flinkImages = map[string]string{"FLINK-1_20": "1.20.3", "FLINK-1_19": "1.19.3", "FLINK-1_18": "1.18.1", "FLINK-1_15": "1.15.4",
	"FLINK-1_13": "1.13.6", "FLINK-1_11": "1.11.6"}

// flinkSpec is what flinkUp runs: the application's code and settings at the time of StartApplication.
type flinkSpec struct {
	app, image, bucket, key string
	props                   []byte   // the runtime properties, as a Flink application reads them
	slots                   int      // parallelism
	env                     []string // credentials and endpoint of pogi
	host                    bool     // share the host network to reach pogi
	s3                      s3io.Client
	ended                   func() // called when the job ended by itself
}

// flinkStart starts a cluster for the application when a container engine runs; r.Data["_box"] names it.
// Without an engine the status stays on its timer, and the log says why nothing runs.
func flinkStart(c *Call, r *Resource) error {
	delete(r.Data, "_box")
	d := c.Hub().Docker()
	if !d.Available() {
		c.Hub().Log.Info("no container engine: application "+r.Name+" gets statuses only and runs no code", "service", "kinesisanalyticsv2")
		return nil
	}
	desc := r.Data["_desc"]
	_, _, _, bucket, _ := core.ParseARN(str(dig(desc, "ApplicationCodeConfigurationDescription", "CodeContentDescription", "S3ApplicationCodeLocationDescription", "BucketARN")))
	key := str(dig(desc, "ApplicationCodeConfigurationDescription", "CodeContentDescription", "S3ApplicationCodeLocationDescription", "FileKey"))
	if bucket == "" || key == "" {
		return core.Errorf(400, "InvalidApplicationConfigurationException", "Application %s has no code in S3 to run.", r.Name)
	}
	props, _ := json.Marshal(orDefault(dig(desc, "EnvironmentPropertyDescriptions", "PropertyGroupDescriptions"), []any{}))
	slots, _ := dig(desc, "FlinkApplicationConfigurationDescription", "ParallelismConfigurationDescription", "Parallelism").(float64)
	sp := flinkSpec{app: r.Name, image: "apache/flink:" + cmp.Or(flinkImages[str(r.Data["RuntimeEnvironment"])], "1.20.3"), bucket: bucket, key: key,
		props: props, slots: max(1, int(slots)), s3: s3io.Client{Hub: c.Hub(), Account: c.Account, Region: c.Region}}
	sp.env, sp.host = c.podEnv(d)
	appKey := r.skey()
	sp.ended = func() { // the application is not running any more, so it must not start again after a pogi restart
		c.Svc.mu.Lock()
		defer c.Svc.mu.Unlock()
		if cur := c.Svc.st.Res[appKey]; cur != nil && cur.Data["_phase"] == "start" {
			cur.Data["_phase"] = "stop"
		}
	}
	name := docker.Name("kinesisanalyticsv2", r.Name)
	r.Data["_box"] = c.Svc.runBox(d, []string{name}, flinkUp(d, name, sp))
	return nil
}

// flinkWasUp is true for an application that was running when pogi stopped (it has a cluster's id
// and was not stopped or ended). ponytail: one that failed to start is tried again too.
func flinkWasUp(r *Resource) bool { return str(r.Data["_box"]) != "" && r.Data["_phase"] == "start" }

// jobsEnded says whether a /jobs/overview reply shows a job that ended by itself: no job is active
// (RUNNING, CREATED, RESTARTING) and one finished, failed or was canceled. An unreadable reply is no.
func jobsEnded(overview []byte) bool {
	var o struct{ Jobs []struct{ State string } }
	if json.Unmarshal(overview, &o) != nil {
		return false
	}
	ended := false
	for _, j := range o.Jobs {
		switch j.State {
		case "RUNNING", "CREATED", "RESTARTING":
			return false
		case "FINISHED", "FAILED", "CANCELED":
			ended = true
		}
	}
	return ended
}

// flinkUp is the life of a Flink application: one container runs the JobManager and a TaskManager
// (a local cluster), the JAR is submitted to it, and the application is RUNNING once the job is.
// The container has the runtime properties where Managed Flink puts them
// (/etc/flink/application_properties.json) and AWS credentials and endpoint for pogi.
// While it runs, /jobs/overview is read every few seconds: when the job ended by itself the
// application is READY again and the container is removed. A REST error changes nothing.
func flinkUp(d *docker.Client, name string, sp flinkSpec) func(context.Context, func(string)) error {
	return func(ctx context.Context, ready func(string)) error {
		jar, err := sp.s3.Get(sp.bucket, sp.key, 0, -1)
		if err != nil {
			return err
		}
		d.Remove(ctx, name)
		slots := strconv.Itoa(sp.slots)
		ct := &docker.Container{Name: name, Image: sp.image, Service: "kinesisanalyticsv2", Resource: sp.app,
			Env: append(sp.env, "FLINK_PROPERTIES=taskmanager.numberOfTaskSlots: "+slots+"\nparallelism.default: "+slots+"\nrest.bind-address: 0.0.0.0"),
			Cmd: []string{"sh", "-c", "start-cluster.sh && exec sleep infinity"}}
		if sp.host {
			ct.Network = "host"
		}
		if _, err := d.Create(ctx, ct); err != nil {
			return err
		}
		if err := d.CopyTo(ctx, name, "/", tarFiles(map[string][]byte{"tmp/app.jar": jar, "etc/flink/application_properties.json": sp.props})); err != nil {
			return err
		}
		if err := d.Start(ctx, name); err != nil {
			return err
		}
		curl := func(path string) []byte { // Flink's REST API, from inside the container (no port to publish)
			out, code, _ := d.Exec(ctx, name, "curl", "-sf", "http://localhost:8081/"+path)
			if code != 0 {
				return nil
			}
			return out
		}
		if !until(ctx, 3*time.Minute, func() bool { return curl("overview") != nil }) {
			return fmt.Errorf("the Flink cluster did not start: %s", logTail(d, name))
		}
		if out, code, err := d.Exec(ctx, name, "flink", "run", "-d", "-p", slots, "/tmp/app.jar"); err != nil || code != 0 {
			return fmt.Errorf("flink run failed: %v %s", err, out)
		}
		failed := ""
		started := until(ctx, 3*time.Minute, func() bool {
			var jobs struct {
				Jobs []struct{ Name, State string }
			}
			json.Unmarshal(curl("jobs/overview"), &jobs)
			for _, j := range jobs.Jobs {
				if j.State == "FAILED" {
					failed = j.Name
				}
				if j.State == "RUNNING" || j.State == "FAILED" {
					return true
				}
			}
			return false
		})
		if failed != "" {
			return fmt.Errorf("job %s failed: %s", failed, logTail(d, name))
		}
		switch {
		case ctx.Err() != nil:
			return nil // stopped meanwhile
		case !started:
			return fmt.Errorf("the job did not start: %s", logTail(d, name))
		}
		ready("")
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(3 * time.Second):
			}
			if err := gone(ctx, d, name); err != nil {
				return err
			}
			if jobsEnded(curl("jobs/overview")) && ctx.Err() == nil {
				sp.ended()
				return nil // the box is stopped, so the application is READY
			}
		}
	}
}
