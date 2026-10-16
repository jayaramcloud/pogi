package web_test

import (
	"regexp"
	"testing"
)

// The shell's overlays carry dialog, combobox and live-region semantics, and every page that opens a drawer from a row
// offers a real link for the keyboard.
func TestA11yShell(t *testing.T) {
	c := start(t, "s3", "sqs")
	c.traffic()
	page := c.get("/_pogi/requests")
	want(t, page,
		`<a class="skip" href="#main">Skip to content</a>`, `<main id="main" tabindex="-1">`, `<th class="col-path">Request</th>`,
		`id="drawer" role="dialog" aria-modal="true" aria-labelledby="dtitle"`,
		`id="keys" role="dialog" aria-modal="true" aria-labelledby="keys-title"`,
		`id="pq" role="combobox"`, `aria-controls="pres"`, `aria-activedescendant=""`,
		`id="pres" role="listbox"`,
		`id="toast" role="status" aria-live="polite"`,
		`id="scopeBtn"`, `aria-haspopup="dialog" aria-expanded="false"`,
		`id="menu" role="menu"`,
		`<label for="sc-account">Account</label>`, `id="sc-account"`,
		`role="tab" aria-selected=`,
		`aria-label="Filter requests"`)
	// the row's primary cell is a real link to the same drawer the row opens
	m := regexp.MustCompile(`<tr data-drawer="(/_pogi/requests/[^"]+)"`).FindStringSubmatch(page)
	if m == nil {
		t.Fatal("no request row:\n" + page)
	}
	want(t, page, `<a href="`+m[1]+`" data-drawer>`)
	// polls go through pogi.js, which pauses them while the tab is hidden; htmx's own timers would not
	want(t, page, `data-poll="2000" data-poll-live`)
	want(t, c.get("/_pogi/static/pogi.js"), "if (document.hidden) return;")
	for _, p := range []string{"/_pogi/requests", "/_pogi/flows", "/_pogi/map", "/_pogi/"} {
		wantNot(t, c.get(p), `hx-trigger="every`)
	}
}

func TestA11yDrawerAndMenus(t *testing.T) {
	c := start(t, "s3")
	c.traffic()
	id := regexp.MustCompile(`/_pogi/requests/([^"]+)"`).FindStringSubmatch(c.get("/_pogi/requests"))[1]
	d := c.drawer("/_pogi/requests/" + id)
	want(t, d, `<h2 id="dtitle">`, `role="tablist"`, `<button type="button" role="tab" aria-selected="true" class="on" data-tab="overview">`, `aria-label="Close"`)
	wantNot(t, d, `<a class="on" data-tab`)

	page := c.get("/_pogi/resources/s3")
	want(t, page, `aria-haspopup="menu" aria-expanded="false"`, `<button type="button" role="menuitem" data-drawer=`, `role="menuitem" class="danger" hx-post=`)
	wantNot(t, page, `<a class="danger" hx-post`)
	want(t, c.get("/_pogi/services"), `role="button" tabindex="0" aria-pressed="`, `aria-label="Pin `)
}

func TestA11yMapNodesAreLinks(t *testing.T) {
	c := start(t, "sns", "sqs")
	c.wire()
	want(t, c.get("/_pogi/map"), `<a class="node`, `href="/_pogi/map/node?id=`)
}
