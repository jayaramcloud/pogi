package web

// The console's background work: image downloads and container starts (see docker.Tasks), served
// as JSON for the live rows and the top-bar indicator, and told to the bell when they finish.

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/nrndev/pogi/internal/docker"
)

func init() { noteFuncs = append(noteFuncs, taskNotes, problemNotes) }

// problemNotes tells the bell about pogi-wide problems: saved state that can't be read or written,
// and ports pogi could not open.
func problemNotes(ui *UI) []note {
	var out []note
	for _, p := range ui.hub.Problems() {
		n := note{Time: p.Time, Kind: "bad", Icon: "x", Title: "Saved state", Text: p.Text, Href: "/_pogi/snapshots"}
		if p.Kind == "port" {
			n.Title, n.Href = "Port in use", "/_pogi/"
		}
		out = append(out, n)
	}
	return out
}

// tasks is GET /_pogi/tasks: what is running, and what finished in the last minute.
func (ui *UI) tasks(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string][]docker.Task{"tasks": ui.hub.Docker().Tasks().List()})
}

// taskNotes tells the bell about resources that became ready or failed, and downloads that failed,
// in the last half hour. A download that fails for a resource is that resource's failure: it gets one
// notice, not one for the download and one for the start that could not follow.
func taskNotes(ui *UI) []note {
	tasks := ui.hub.Docker().Tasks().Recent(30 * time.Minute)
	type key struct{ service, resource string }
	dlFailed := map[key]time.Time{} // when the download of a resource failed
	for _, t := range tasks {
		if t.Kind == "download" && t.State == "failed" && t.Resource != "" {
			dlFailed[key{t.Service, t.Resource}] = t.Finished()
		}
	}
	var out []note
	for _, t := range tasks {
		if t.State == "running" {
			continue
		}
		name, href := t.Resource, "/_pogi/resources"
		if t.Service != "" {
			href += "/" + t.Service
			if svc := ui.hub.Service(t.Service); svc != nil && name != "" {
				name += " (" + svc.Info().Title + ")"
			}
		}
		n := note{Time: t.Finished(), Icon: "box", Text: t.What, Href: href}
		switch {
		case t.Kind == "start" && t.State == "done":
			n.Kind, n.Title = "ok", name+" is ready"
		case t.Kind == "start":
			if at, ok := dlFailed[key{t.Service, t.Resource}]; ok && t.Finished().Sub(at) < time.Minute {
				continue // the download's notice says it
			}
			n.Kind, n.Title = "bad", name+" failed: "+t.Error
		case t.State == "failed" && t.Resource != "":
			n.Kind, n.Title = "bad", name+" failed: couldn't download "+t.What+": "+t.Error
		case t.State == "failed":
			n.Kind, n.Title = "bad", "Download of "+t.What+" failed: "+t.Error
		default:
			continue
		}
		out = append(out, n)
	}
	return out
}
