package tui

import (
	"strings"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/nrndev/pogi/internal/core"
)

func TestRenderEveryTab(t *testing.T) {
	m := &model{endpoint: "http://localhost:7644"}
	m.Update(tea.WindowSizeMsg{Width: 120, Height: 30})
	m.Update(dataMsg{
		status: core.Status{Version: "test", Services: []core.ServiceStatus{{Name: "s3", Cloud: core.AWS, Title: "S3", Resources: 1, Requests: 2}}},
		reqs:   []core.Entry{{ID: 1, Cloud: core.AWS, Service: "s3", Op: "PutObject", Status: 200}, {ID: 2, Cloud: core.AWS, Service: "s3", Op: "GetObject", Status: 404, Error: "NoSuchKey"}},
		res:    map[string][]core.Table{"s3": {{Name: "Buckets", Columns: []string{"Name"}, Rows: [][]string{{"photos"}}}}},
		rules:  []core.Rule{{ID: "r1", Service: "sqs", Status: 500, Code: "InternalError"}},
		snaps:  []core.SnapshotInfo{{Name: "before-demo"}},
		logs:   []logLine{{T: time.Now(), Source: "aws /app", Text: "hello logs"}},
	})
	want := []string{"Recent", "NoSuchKey", "photos", "InternalError", "before-demo", "hello logs"}
	for tab := range tabs {
		m.tab = tab
		if out := m.render(); !strings.Contains(out, want[tab]) {
			t.Errorf("tab %s does not show %q:\n%s", tabs[tab], want[tab], out)
		}
	}
	m.tab = 1
	m.key(tea.KeyPressMsg{Code: 'e', Text: "e"})
	if out := m.render(); strings.Contains(out, "PutObject") || !strings.Contains(out, "GetObject") {
		t.Errorf("errors-only filter failed:\n%s", out)
	}
}
