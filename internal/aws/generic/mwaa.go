package generic

import (
	"archive/zip"
	"bytes"
	"cmp"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/aws/s3io"
	"github.com/nrndev/pogi/internal/core"
	"github.com/nrndev/pogi/internal/docker"
)

// mwaaCLIPath is the Airflow CLI endpoint of an environment's web server; its
// callers send a CLI token as a bearer token and no signature, so the path routes them.
const mwaaCLIPath = "/aws_mwaa/cli"

// mwaaCLITTL is how long a CLI token works, as in AWS.
const mwaaCLITTL = time.Minute

// mwaaHostSuffix ends the web server host of every environment; requests for those hosts go to Airflow.
const mwaaHostSuffix = ".airflow.localhost"

// MWAA: an environment goes CREATING, AVAILABLE (UPDATING, AVAILABLE after an
// update) and has a web server host; the CLI and web login tokens are made up.
// With Docker, CreateEnvironment runs the official apache/airflow image
// (standalone mode, DAGs synced from the environment's S3 DagS3Path, plus its
// requirements.txt, plugins.zip and startup script), the status follows it, and
// the web server host and the CLI endpoint go to it. UpdateEnvironment runs it
// again with the new settings. After a pogi restart the environments that were up
// start again. Without Docker nothing runs and the statuses are on a timer.
func init() {
	OnClose("mwaa", func(s *Service) { s.closeBoxes() })
	resumers["mwaa"] = func(s *Service) { s.resume("Environment", mwaaWasUp, airflowStart) }
	matchers["mwaa"] = func(r *http.Request) bool { return r.URL.Path == mwaaCLIPath || mwaaHost(r) != "" }
	Raw("mwaa", mwaaServe)
	Derive("mwaa", "Environment", func(r *Resource) map[string]any {
		if id := str(r.Data["_box"]); id != "" { // a real Airflow
			b, upd := boxOf(id), r.Data["_phase"] == "update"
			switch {
			case b.state == "starting" && upd:
				return map[string]any{"Status": "UPDATING"}
			case b.state == "starting":
				return map[string]any{"Status": "CREATING"}
			case b.state == "running":
				return map[string]any{"Status": "AVAILABLE"}
			case b.state == "failed" && !b.ready:
				st, src := "CREATE_FAILED", "CreateEnvironment"
				if upd {
					st, src = "UPDATE_FAILED", "UpdateEnvironment"
				}
				return map[string]any{"Status": st, "LastUpdate": map[string]any{"Status": "FAILED", "Source": src,
					"Error": map[string]any{"ErrorCode": "INTERNAL_ERROR", "ErrorMessage": b.err.Error()}}}
			}
			return map[string]any{"Status": "UNAVAILABLE"} // it died, is stopping, or pogi restarted
		}
		s := Step(r, "env", "CREATING", "AVAILABLE")
		if r.Data["_phase"] == "update" {
			s = Step(r, "env", "UPDATING", "AVAILABLE")
		}
		return map[string]any{"Status": s}
	})
	Override("mwaa", "CreateEnvironment", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["WebserverUrl"] = randHex(8) + "-vpce." + c.Region + ".airflow.localhost"
		for k, v := range map[string]any{"AirflowVersion": "2.10.1", "EnvironmentClass": "mw1.small", "MaxWorkers": 10.0, "MinWorkers": 1.0, "Schedulers": 2.0, "WebserverAccessMode": "PRIVATE_ONLY"} {
			if r.Data[k] == nil {
				r.Data[k] = v
			}
		}
		Begin(r, "env")
		if err := airflowStart(c, r); err != nil {
			return nil, err
		}
		c.Put(r)
		return out, nil
	})
	Override("mwaa", "DeleteEnvironment", func(c *Call) (map[string]any, error) {
		if r := c.Get("Environment", str(c.In["Name"])); r != nil {
			stopBox(str(r.Data["_box"])) // the container goes in the background
		}
		return c.Default()
	})
	Override("mwaa", "UpdateEnvironment", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		r := c.Res
		r.Data["_phase"] = "update"
		Begin(r, "env")
		if err := airflowStart(c, r); err != nil { // the container runs again with the new settings
			return nil, err
		}
		c.Put(r)
		return out, nil
	})
	token := func(key string, extra func(r *Resource) map[string]any) func(c *Call) (map[string]any, error) {
		return func(c *Call) (map[string]any, error) {
			r := c.Get("Environment", str(c.In["Name"]))
			if r == nil {
				return nil, c.NotFound("Environment %s not found", str(c.In["Name"]))
			}
			if s := str(derived("mwaa", r)["Status"]); s != "AVAILABLE" {
				return nil, c.Invalid("Environment %s is %s, not AVAILABLE", r.Name, s)
			}
			tok := core.RandString(40, core.UpperNum+"abcdefghijklmnopqrstuvwxyz")
			out := map[string]any{key: tok, "WebServerHostname": r.Data["WebserverUrl"]}
			if key == "CliToken" { // the CLI endpoint knows the tokens it handed out
				tokens := map[string]any{tok: float64(time.Now().Add(mwaaCLITTL).Unix())}
				for t, exp := range asMap(r.Data["_cliTokens"]) {
					if n, _ := exp.(float64); n > float64(time.Now().Unix()) {
						tokens[t] = exp
					}
				}
				r.Data["_cliTokens"] = tokens
				c.Put(r)
			}
			for k, v := range extra(r) {
				out[k] = v
			}
			return out, nil
		}
	}
	Override("mwaa", "CreateCliToken", token("CliToken", func(*Resource) map[string]any { return nil }))
	Override("mwaa", "CreateWebLoginToken", token("WebToken", func(r *Resource) map[string]any {
		return map[string]any{"IamIdentity": core.ARN("sts", "", r.Account, "assumed-role/pogi/pogi"), "AirflowIdentity": "pogi"}
	}))
}

func asMap(v any) map[string]any { m, _ := v.(map[string]any); return m }

// mwaaHost is the host of the request when it is an environment's web server host, else "".
func mwaaHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.Host)
	if err != nil {
		host = r.Host
	}
	if strings.HasSuffix(host, mwaaHostSuffix) {
		return host
	}
	return ""
}

// airflowSpec is what airflowUp runs: the environment at the time of CreateEnvironment.
type airflowSpec struct {
	env, image, bucket, prefix     string
	requirements, plugins, startup string         // S3 keys in the bucket of the requirements.txt, the plugins zip and the startup script, or ""
	options                        map[string]any // AirflowConfigurationOptions
	podEnv                         []string       // credentials and endpoint of pogi
	host                           bool           // share the host network to reach pogi
	s3                             s3io.Client
	log                            *slog.Logger
}

// airflowStart starts Airflow for the environment when a container engine runs; r.Data["_box"] names it.
// An Airflow that runs already is stopped first (an update): the new one waits until its container is gone.
func airflowStart(c *Call, r *Resource) error {
	prev := str(r.Data["_box"])
	delete(r.Data, "_box")
	stopBox(prev)
	d := c.Hub().Docker()
	if !d.Available() {
		c.Hub().Log.Info("no container engine: environment "+r.Name+" gets statuses only and runs no Airflow", "service", "mwaa")
		return nil
	}
	_, _, _, bucket, ok := core.ParseARN(str(r.Data["SourceBucketArn"]))
	if !ok || bucket == "" {
		return c.Invalid("SourceBucketArn %q is not an S3 bucket ARN.", str(r.Data["SourceBucketArn"]))
	}
	sp := airflowSpec{env: r.Name, image: "apache/airflow:" + str(r.Data["AirflowVersion"]), bucket: bucket,
		prefix: strings.TrimSuffix(str(r.Data["DagS3Path"]), "/") + "/", requirements: str(r.Data["RequirementsS3Path"]),
		plugins: str(r.Data["PluginsS3Path"]), startup: str(r.Data["StartupScriptS3Path"]), options: asMap(r.Data["AirflowConfigurationOptions"]),
		s3: s3io.Client{Hub: c.Hub(), Account: c.Account, Region: c.Region}, log: c.Hub().Log}
	sp.podEnv, sp.host = c.podEnv(d)
	name := docker.Name("mwaa", r.Name)
	r.Data["_box"] = c.Svc.runBox(d, []string{name}, after(prev, airflowUp(d, name, sp)))
	return nil
}

// mwaaWasUp is true for an environment that had an Airflow when pogi stopped (the container ids are
// kept with the state). ponytail: one that failed to start is tried again too.
func mwaaWasUp(r *Resource) bool { return str(r.Data["_box"]) != "" }

// webserverConfig lets everyone in as Admin: pogi has no sign-in for the web server host.
const webserverConfig = `from flask_appbuilder.security.manager import AUTH_DB
AUTH_TYPE = AUTH_DB
AUTH_ROLE_PUBLIC = "Admin"
`

// airflowUp is the life of an environment: one Airflow container in standalone mode (web server,
// scheduler and a SQLite database) with the DAGs of the S3 path, which are synced again every
// 15 seconds. AirflowConfigurationOptions become AIRFLOW__<SECTION>__<KEY> variables. The environment
// is AVAILABLE once the scheduler's heartbeat shows in /health; ready gets the address of the web server.
// The requirements.txt goes to the image's _PIP_ADDITIONAL_REQUIREMENTS (it installs them at start, so
// the container needs internet access), the plugins zip is unzipped into the plugins folder, and a
// startup script runs before Airflow starts.
func airflowUp(d *docker.Client, name string, sp airflowSpec) func(context.Context, func(string)) error {
	return func(ctx context.Context, ready func(string)) error {
		d.Remove(ctx, name)
		env := append(slices.Clone(sp.podEnv), "AIRFLOW__CORE__LOAD_EXAMPLES=False", "AIRFLOW__DATABASE__LOAD_DEFAULT_CONNECTIONS=False",
			"AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.default", "AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True")
		for _, k := range core.SortedKeys(sp.options) {
			if sect, key, ok := strings.Cut(k, "."); ok {
				env = append(env, "AIRFLOW__"+strings.ToUpper(sect)+"__"+strings.ToUpper(key)+"="+str(sp.options[k]))
			}
		}
		files, pip, err := sp.fromSettings()
		if err != nil {
			return err
		}
		if pip != "" {
			env = append(env, "_PIP_ADDITIONAL_REQUIREMENTS="+pip)
		}
		cmd := []string{"standalone"}
		if sp.startup != "" {
			cmd = []string{"bash", "-c", "source /opt/airflow/startup.sh && exec airflow standalone"}
		}
		ct := &docker.Container{Name: name, Image: sp.image, Service: "mwaa", Resource: sp.env, Cmd: cmd, Ports: map[int]int{8080: 0}}
		port := 0
		if sp.host {
			port = freePort()
			ct.Network, ct.Ports = "host", nil
			env = append(env, fmt.Sprintf("AIRFLOW__WEBSERVER__WEB_SERVER_PORT=%d", port))
		}
		ct.Env = env
		if _, err := d.Create(ctx, ct); err != nil {
			return err
		}
		seen := map[string]string{}
		if err := sp.syncDags(ctx, d, name, seen, files); err != nil {
			return err
		}
		if err := d.Start(ctx, name); err != nil {
			return err
		}
		if info, err := d.Inspect(ctx, name); err != nil {
			return err
		} else if !sp.host {
			port = info.HostPort(8080)
		}
		addr, dead := fmt.Sprintf("127.0.0.1:%d", port), false
		if !sp.host {
			addr = d.Addr(port)
		}
		if !until(ctx, 10*time.Minute, func() bool {
			if i, err := d.Inspect(ctx, name); err == nil && !i.State.Running {
				dead = true
				return true
			}
			return airflowHealthy(ctx, addr)
		}) || dead {
			if ctx.Err() != nil {
				return nil // stopped meanwhile
			}
			return fmt.Errorf("Airflow did not start: %s", logTail(d, name))
		}
		ready(addr)
		for { // keep the DAGs in step with S3, until stopped or the container dies
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Second):
			}
			if err := gone(ctx, d, name); err != nil {
				return err
			}
			if err := sp.syncDags(ctx, d, name, seen, nil); err != nil && ctx.Err() == nil {
				sp.log.Warn("DAG sync failed, trying again", "environment", sp.env, "err", err)
			}
		}
	}
}

// fromSettings reads from S3 what the environment's settings name: the files for the container
// (the web server config, the plugins, the startup script; path without a leading slash → content)
// and the requirements as the value of _PIP_ADDITIONAL_REQUIREMENTS.
func (a airflowSpec) fromSettings() (files map[string][]byte, pip string, err error) {
	files = map[string][]byte{"opt/airflow/webserver_config.py": []byte(webserverConfig)}
	read := func(key string) ([]byte, error) {
		b, err := a.s3.Get(a.bucket, key, 0, -1)
		if err != nil {
			err = fmt.Errorf("reading s3://%s/%s: %w", a.bucket, key, err)
		}
		return b, err
	}
	if a.requirements != "" {
		b, err := read(a.requirements)
		if err != nil {
			return nil, "", err
		}
		pip = pipRequirements(b)
	}
	if a.plugins != "" {
		b, err := read(a.plugins)
		if err != nil {
			return nil, "", err
		}
		if err := unzipPlugins(b, files); err != nil {
			return nil, "", fmt.Errorf("plugins zip %s: %w", a.plugins, err)
		}
	}
	if a.startup != "" {
		b, err := read(a.startup)
		if err != nil {
			return nil, "", err
		}
		files["opt/airflow/startup.sh"] = b
	}
	return files, pip, nil
}

// pipRequirements turns the text of a requirements.txt into the space-separated list that the image
// installs: one entry per line, without blank lines and comments.
func pipRequirements(text []byte) string {
	var out []string
	for _, l := range strings.Split(string(text), "\n") {
		if l = strings.TrimSpace(l); l != "" && !strings.HasPrefix(l, "#") {
			out = append(out, l)
		}
	}
	return strings.Join(out, " ")
}

// unzipPlugins adds the files of a plugins zip to files, under the plugins folder of the container.
// A name that leaves the folder (../x, /x) is an error.
func unzipPlugins(zipped []byte, files map[string][]byte) error {
	zr, err := zip.NewReader(bytes.NewReader(zipped), int64(len(zipped)))
	if err != nil {
		return err
	}
	for _, f := range zr.File {
		if f.FileInfo().IsDir() {
			continue
		}
		if !fs.ValidPath(f.Name) {
			return fmt.Errorf("file name %q is not inside the plugins folder", f.Name)
		}
		rc, err := f.Open()
		if err != nil {
			return err
		}
		b, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return err
		}
		files["opt/airflow/plugins/"+f.Name] = b
	}
	return nil
}

// syncDags copies into the container the objects under the DAG path that changed since the last call
// (seen: relative key → size and time), removes the files whose objects are gone, and adds extra files
// (path without a leading slash → content).
func (a airflowSpec) syncDags(ctx context.Context, d *docker.Client, name string, seen map[string]string, extra map[string][]byte) error {
	objs, _, err := a.s3.List(a.bucket, a.prefix, "")
	if err != nil {
		return err
	}
	files, now := map[string][]byte{}, map[string]string{}
	for _, o := range objs {
		rel := strings.TrimPrefix(o.Key, a.prefix)
		if rel == "" || strings.HasSuffix(rel, "/") || path.Clean(rel) != rel || strings.HasPrefix(rel, "../") {
			continue
		}
		now[rel] = fmt.Sprint(o.Size, "/", o.Modified.UnixNano())
		if seen[rel] != now[rel] {
			if files["opt/airflow/dags/"+rel], err = a.s3.Get(a.bucket, o.Key, 0, -1); err != nil {
				return err
			}
		}
	}
	var removed []string
	for rel := range seen {
		if _, ok := now[rel]; !ok {
			removed = append(removed, "/opt/airflow/dags/"+rel)
		}
	}
	if len(removed) > 0 {
		d.Exec(ctx, name, append([]string{"rm", "-f"}, removed...)...)
	}
	maps.Copy(files, extra)
	if len(files) > 0 {
		if err := d.CopyTo(ctx, name, "/", tarFiles(files)); err != nil {
			return err
		}
	}
	clear(seen)
	maps.Copy(seen, now)
	return nil
}

// airflowHealthy is true when the web server at addr answers /health (or Airflow 3's path) with a healthy scheduler.
func airflowHealthy(ctx context.Context, addr string) bool {
	for _, p := range []string{"/health", "/api/v2/monitor/health"} {
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+p, nil)
		res, err := (&http.Client{Timeout: 3 * time.Second}).Do(req)
		if err != nil {
			continue
		}
		var h struct{ Scheduler struct{ Status string } }
		err = json.NewDecoder(res.Body).Decode(&h)
		res.Body.Close()
		if err == nil && h.Scheduler.Status == "healthy" {
			return true
		}
	}
	return false
}

// mwaaCLICommands are the first words of the Airflow CLI commands MWAA runs (it also allows `db check`).
var mwaaCLICommands = []string{"backfill", "cheat-sheet", "config", "connections", "dags", "info", "jobs", "plugins", "pools", "providers",
	"roles", "sync-perm", "tasks", "users", "variables", "version"}

// mwaaServe answers what Airflow answers. A request for an environment's web server host goes to its
// Airflow. POST /aws_mwaa/cli takes an Airflow CLI command as the body and a token from CreateCliToken as
// the bearer; the command runs in the environment's container, and the reply is {"stdout","stderr"} in
// base64, as in AWS (stderr is mixed into stdout here). Without a container engine a command that is
// allowed gets a clear error, not made-up output.
func mwaaServe(s *Service, w http.ResponseWriter, r *http.Request) bool {
	reply := func(status int, msg string) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		json.NewEncoder(w).Encode(map[string]string{"message": msg})
	}
	find := func(match func(*Resource) bool) *Resource {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, res := range s.st.Res {
			if res.Type == "Environment" && match(res) {
				return res.clone()
			}
		}
		return nil
	}
	if host := mwaaHost(r); host != "" {
		env := find(func(res *Resource) bool { return res.Data["WebserverUrl"] == host })
		if env == nil {
			reply(http.StatusNotFound, "No environment has the web server host "+host+".")
			return true
		}
		b := boxOf(str(env.Data["_box"]))
		if b.state != "running" {
			reply(http.StatusServiceUnavailable, "Environment "+env.Name+" has no running Airflow (it is "+cmp.Or(str(derived("mwaa", env)["Status"]), "not available")+").")
			return true
		}
		(&httputil.ReverseProxy{Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(&url.URL{Scheme: "http", Host: b.addr})
			pr.Out.Host = r.Host
		}}).ServeHTTP(w, r)
		return true
	}
	if r.URL.Path != mwaaCLIPath {
		return false
	}
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	var env *Resource
	if token != "" {
		env = find(func(res *Resource) bool {
			exp, ok := asMap(res.Data["_cliTokens"])[token].(float64)
			return ok && exp > float64(time.Now().Unix())
		})
	}
	cmd, _ := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	args := strings.Fields(string(cmd))
	switch {
	case r.Method != http.MethodPost:
		reply(http.StatusMethodNotAllowed, "The Airflow CLI endpoint takes POST.")
	case env == nil:
		reply(http.StatusForbidden, "The CLI token is missing, expired or not from CreateCliToken.")
	case len(args) == 0:
		reply(http.StatusBadRequest, "The body must be an Airflow CLI command, such as: dags list")
	case !slices.Contains(mwaaCLICommands, args[0]) && !(args[0] == "db" && len(args) > 1 && args[1] == "check"):
		reply(http.StatusBadRequest, "The Airflow CLI command "+args[0]+" is not supported by MWAA.")
	case str(env.Data["_box"]) == "":
		reply(http.StatusNotImplemented, "pogi does not run Apache Airflow without a container engine, so environment "+env.Name+" cannot run: "+strings.Join(args, " "))
	default:
		if boxOf(str(env.Data["_box"])).state != "running" {
			reply(http.StatusServiceUnavailable, "Environment "+env.Name+" has no running Airflow.")
			return true
		}
		ctx, stop := context.WithTimeout(r.Context(), 2*time.Minute)
		defer stop()
		out, _, err := s.h.Docker().Exec(ctx, docker.Name("mwaa", env.Name), append([]string{"airflow"}, args...)...)
		if err != nil {
			reply(http.StatusBadGateway, "The command could not run: "+err.Error())
			return true
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"stdout": base64.StdEncoding.EncodeToString(out), "stderr": ""})
	}
	return true
}
