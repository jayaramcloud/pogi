package web

// The CLI mirror: every create drawer shows the command that does the same thing, updated as the
// form changes. POST /_pogi/cli/{service}/{op} takes the form's values and answers with the text.

import (
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"regexp"
	"strings"

	"github.com/nrndev/pogi/internal/core"
)

var (
	kebabLead = regexp.MustCompile(`([A-Z]+)([A-Z][a-z])`)
	kebabCase = regexp.MustCompile(`([a-z0-9])([A-Z])`)
	plainArg  = regexp.MustCompile(`^[A-Za-z0-9_./:=@%+,-]+$`)
)

// kebab is how the AWS CLI spells an operation or member: CreateQueue is create-queue.
func kebab(s string) string {
	s = kebabLead.ReplaceAllString(s, "$1-$2")
	return strings.ToLower(kebabCase.ReplaceAllString(s, "$1-$2"))
}

// shellArg quotes a value for a shell when it needs it.
func shellArg(s string) string {
	if plainArg.MatchString(s) {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// cliValue is a form value as a CLI argument: plain values inline, structures and lists as JSON.
func cliValue(v any) string {
	switch v := v.(type) {
	case string:
		return shellArg(v)
	case float64, int64, int, bool:
		return fmt.Sprint(v)
	}
	return "'" + jsonOf(v) + "'"
}

// cliNames are the services whose `aws` command is not the service's name.
var cliNames = map[string]string{"s3": "s3api"}

// awsCLI is `pogi aws <service> <op> --member value …` for a filled form. The first member shares the
// command's line; the others each get one.
func awsCLI(svc, op string, fs []core.Field, in map[string]any, region string) string {
	name := svc
	if n, ok := cliNames[svc]; ok {
		name = n
	}
	cmd := "pogi aws " + name + " " + kebab(op)
	var args []string
	for _, f := range fs {
		v, ok := in[f.Name]
		if !ok {
			continue
		}
		if b, isBool := v.(bool); isBool {
			args = append(args, map[bool]string{true: "--", false: "--no-"}[b]+kebab(f.Name))
			continue
		}
		args = append(args, "--"+kebab(f.Name)+" "+cliValue(v))
	}
	if region != "" {
		args = append(args, "--region "+region)
	}
	if len(args) == 0 {
		return cmd
	}
	return cmd + " " + strings.Join(args, " \\\n  ")
}

// curlCmd is a REST call as a curl command.
func curlCmd(verb, url string, body []byte) string {
	c := "curl -X " + verb + " '" + url + "'"
	if len(body) > 0 {
		c += " \\\n  -H 'Content-Type: application/json' \\\n  -d '" + strings.ReplaceAll(string(body), "'", `'\''`) + "'"
	}
	return c
}

// cliCommand is the command a posted form stands for, or a "#" line saying why there is none yet.
func (ui *UI) cliCommand(r *http.Request, svc, op string) string {
	endpoint := "http://" + r.Host
	if svc == "arm" && op == "new" { // the JSON editor: a PUT of the body to the resource ID
		path, body := strings.TrimSpace(r.PostFormValue("path")), strings.TrimSpace(r.PostFormValue("body"))
		var buf json.RawMessage
		if json.Unmarshal([]byte(body), &buf) == nil {
			b, _ := json.Marshal(buf)
			body = string(b)
		}
		return curlCmd("PUT", endpoint+path+"?api-version="+armVersion, []byte(body))
	}
	sc := ui.scope(r)
	f := ui.form(svc, op, sc)
	if f == nil {
		return "# no such form"
	}
	in, err := input(f.Fields, r.PostForm)
	if err != nil {
		return "# " + err.Error()
	}
	if sp := ui.special(svc, op); sp != nil && sp.CLI != nil {
		return sp.CLI(endpoint, in)
	}
	if s := ui.hub.Service(svc); s != nil && s.Info().Cloud == core.GCP {
		verb, uri, body, err := ui.gcpCall(svc, op, in)
		if err != nil {
			return "# " + err.Error()
		}
		return curlCmd(verb, endpoint+uri, body)
	}
	region := ""
	if sc.Region != all && sc.Region != ui.hub.Cfg.Region {
		region = sc.Region
	}
	return awsCLI(svc, op, f.Fields, in, region)
}

func (ui *UI) cliMirror(w http.ResponseWriter, r *http.Request) {
	r.ParseMultipartForm(32 << 20)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, html.EscapeString(ui.cliCommand(r, r.PathValue("service"), r.PathValue("op"))))
}
