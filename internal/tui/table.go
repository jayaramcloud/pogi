package tui

// One table helper for every tab: columns with a title, alignment, a width cap
// and a keep priority. When the screen is too narrow the least important
// columns are dropped, never wrapped.

import (
	"strings"
	"unicode"

	"github.com/charmbracelet/x/ansi"
)

type col struct {
	title string
	right bool // numbers: aligned to the right
	max   int  // widest the column may get (0 = no cap)
	keep  int  // higher stays longer when the screen is narrow
}

type table struct {
	cols     []col
	rows     [][]string
	sel      int  // selected row (index in rows), -1 for none
	blur     bool // the selection is shown with ▸ and bold only, not filled (pane without focus)
	from, to int  // rows shown: [from, to); to <= 0 means all
	cell     func(row, col int, padded string) string
}

// lastMin is the least room the last column needs before another column is dropped.
const lastMin = 20

func pad(s string, w int, right bool) string {
	gap := w - ansi.StringWidth(s)
	if gap <= 0 {
		return s
	}
	if right {
		return strings.Repeat(" ", gap) + s
	}
	return s + strings.Repeat(" ", gap)
}

// clean makes text from the server safe to draw on one line: escape codes are removed, and line
// breaks and other control characters become spaces. Log messages and request paths are user data.
func clean(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, ansi.Strip(s))
}

// layout picks the columns that fit in w and their widths.
func (t table) layout(w int) (idx, widths []int) {
	nat := make([]int, len(t.cols))
	for i, c := range t.cols {
		nat[i] = ansi.StringWidth(c.title)
		for _, r := range t.rows {
			if i < len(r) {
				nat[i] = max(nat[i], ansi.StringWidth(clean(r[i])))
			}
		}
		if c.max > 0 {
			nat[i] = min(nat[i], c.max)
		}
	}
	for i := range t.cols {
		idx = append(idx, i)
	}
	need := func() int {
		n := 2
		for k, i := range idx {
			if k > 0 {
				n += 2
			}
			if k == len(idx)-1 {
				n += min(nat[i], lastMin)
			} else {
				n += nat[i]
			}
		}
		return n
	}
	for len(idx) > 1 && need() > w {
		drop := 0
		for k, i := range idx {
			if t.cols[i].keep <= t.cols[idx[drop]].keep {
				drop = k // ties: the rightmost
			}
		}
		idx = append(idx[:drop:drop], idx[drop+1:]...)
	}
	widths = make([]int, len(idx))
	used := 2
	for k, i := range idx {
		widths[k] = nat[i]
		if k < len(idx)-1 {
			used += nat[i] + 2
		}
	}
	last := len(idx) - 1
	widths[last] = max(1, min(nat[idx[last]], w-used))
	return idx, widths
}

// render returns the header line and the shown rows, each no wider than w.
func (t table) render(w int) []string {
	idx, widths := t.layout(w)
	build := func(cells func(i int) string, plain bool, r int) string {
		var b strings.Builder
		for k, i := range idx {
			if k > 0 {
				b.WriteString("  ")
			}
			s := ansi.Truncate(clean(cells(i)), widths[k], "…")
			if k < len(idx)-1 || t.cols[i].right {
				s = pad(s, widths[k], t.cols[i].right)
			}
			if !plain && t.cell != nil && r >= 0 {
				s = t.cell(r, i, s)
			}
			b.WriteString(s)
		}
		return b.String()
	}
	out := []string{dim.Render(ansi.Truncate("  "+build(func(i int) string { return t.cols[i].title }, true, -1), w, "…"))}
	to := t.to
	if to <= 0 || to > len(t.rows) {
		to = len(t.rows)
	}
	for r := max(0, t.from); r < to; r++ {
		row := t.rows[r]
		get := func(i int) string {
			if i < len(row) {
				return row[i]
			}
			return ""
		}
		if r == t.sel {
			line := ansi.Truncate("▸ "+build(get, true, r), w, "…")
			if t.blur {
				out = append(out, bold.Render(line))
			} else {
				out = append(out, selStyle.Render(pad(line, w, false)))
			}
			continue
		}
		out = append(out, ansi.Truncate("  "+build(get, false, r), w, "…"))
	}
	return out
}

// window returns the range of n rows, room at a time, that keeps sel in view.
func window(n, sel, room int) (from, to int) {
	room = max(1, room)
	if n <= room {
		return 0, n
	}
	from = min(max(0, sel-room/2), n-room)
	return from, from + room
}
