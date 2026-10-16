package generic

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"io"
	"slices"
	"testing"
)

func TestPipRequirements(t *testing.T) {
	got := pipRequirements([]byte("# for the DAGs\r\ncowsay==6.1\n\n  apache-airflow-providers-http  \n#end\n"))
	if got != "cowsay==6.1 apache-airflow-providers-http" {
		t.Fatalf("got %q", got)
	}
}

func zipOf(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var b bytes.Buffer
	zw := zip.NewWriter(&b)
	for n, c := range files {
		w, _ := zw.Create(n)
		w.Write([]byte(c))
	}
	zw.Close()
	return b.Bytes()
}

func TestUnzipPlugins(t *testing.T) {
	files := map[string][]byte{}
	if err := unzipPlugins(zipOf(t, map[string]string{"my_plugin.py": "x = 1", "sub/other.py": "y = 2", "sub/": ""}), files); err != nil {
		t.Fatal(err)
	}
	var names []string
	rd := tar.NewReader(tarFiles(files)) // what docker.CopyTo gets
	for h, err := rd.Next(); err == nil; h, err = rd.Next() {
		b, _ := io.ReadAll(rd)
		names = append(names, h.Name+"="+string(b))
	}
	slices.Sort(names)
	if !slices.Equal(names, []string{"opt/airflow/plugins/my_plugin.py=x = 1", "opt/airflow/plugins/sub/other.py=y = 2"}) {
		t.Fatalf("got %v", names)
	}
	for _, bad := range []string{"../evil.py", "/etc/evil.py"} {
		if unzipPlugins(zipOf(t, map[string]string{bad: "x"}), map[string][]byte{}) == nil {
			t.Fatalf("%s was accepted", bad)
		}
	}
	if unzipPlugins([]byte("not a zip"), map[string][]byte{}) == nil {
		t.Fatal("garbage was accepted")
	}
}

func TestJobsEnded(t *testing.T) {
	for _, c := range []struct {
		overview string
		want     bool
	}{
		{`{"jobs":[{"state":"RUNNING"}]}`, false},
		{`{"jobs":[{"state":"FINISHED"}]}`, true},
		{`{"jobs":[{"state":"FAILED"}]}`, true},
		{`{"jobs":[{"state":"CANCELED"}]}`, true},
		{`{"jobs":[{"state":"FAILED"},{"state":"RESTARTING"}]}`, false},
		{`{"jobs":[{"state":"CANCELED"},{"state":"CREATED"}]}`, false},
		{`{"jobs":[]}`, false},
		{``, false}, // the REST call failed: keep the status
		{`<html>`, false},
	} {
		if got := jobsEnded([]byte(c.overview)); got != c.want {
			t.Errorf("%s: got %v", c.overview, got)
		}
	}
}

func TestWasUp(t *testing.T) {
	res := func(data map[string]any) *Resource { return &Resource{Data: data} }
	for _, c := range []struct {
		name string
		r    *Resource
		flnk bool
		mwaa bool
	}{
		{"running app, up env", res(map[string]any{"_box": "b1", "_phase": "start"}), true, true},
		{"stopped app", res(map[string]any{"_box": "b1", "_phase": "stop"}), false, true},
		{"no containers (no Docker)", res(map[string]any{"_phase": "start"}), false, false},
		{"never started", res(map[string]any{}), false, false},
	} {
		if flinkWasUp(c.r) != c.flnk || mwaaWasUp(c.r) != c.mwaa {
			t.Errorf("%s: flink %v mwaa %v", c.name, flinkWasUp(c.r), mwaaWasUp(c.r))
		}
	}
}
