package tui

import (
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/charmbracelet/x/ansi"
)

func testTable() table {
	return table{
		cols: []col{{"ID", true, 0, 5}, {"NAME", false, 8, 9}, {"NOTE", false, 0, 1}, {"MS", true, 0, 7}, {"LAST", false, 0, 3}},
		rows: [][]string{{"1", "alpha", "a fairly long note here", "5.0", "the last column is long too"}, {"22", "a-very-long-name", "x", "10.5", "z"}},
		sel:  1,
	}
}

func TestTableFitsEveryWidth(t *testing.T) {
	for _, w := range []int{120, 80, 60, 30, 12} {
		for _, l := range testTable().render(w) {
			if ansi.StringWidth(l) > w {
				t.Errorf("w=%d: line is %d wide: %q", w, ansi.StringWidth(l), ansi.Strip(l))
			}
		}
	}
}

func TestTableDropsLowestKeepFirst(t *testing.T) {
	tb := testTable()
	// natural widths: gutter 2 + ID 2 + NAME 8 + NOTE 23 + MS 4 + LAST (10 reserved) + 4 gaps of 2
	idx, _ := tb.layout(200)
	if len(idx) != 5 {
		t.Fatalf("at 200 all columns stay, got %v", idx)
	}
	idx, _ = tb.layout(50)
	if len(idx) != 4 || idx[0] != 0 || idx[1] != 1 || idx[2] != 3 || idx[3] != 4 { // NOTE (keep 1) goes first
		t.Errorf("columns at 50 = %v, want NOTE dropped first", idx)
	}
	idx, _ = tb.layout(30) // then LAST (keep 3) goes; the last column left is MS
	if len(idx) != 3 || idx[0] != 0 || idx[1] != 1 || idx[2] != 3 {
		t.Errorf("columns at 30 = %v, want ID NAME MS", idx)
	}
}

func TestTableAlignGutterAndCut(t *testing.T) {
	lines := testTable().render(200)
	if len(lines) != 3 {
		t.Fatalf("header + 2 rows, got %d lines", len(lines))
	}
	h, r0, r1 := ansi.Strip(lines[0]), ansi.Strip(lines[1]), ansi.Strip(lines[2])
	if !strings.HasPrefix(r0, "  ") || !strings.HasPrefix(r1, "▸ ") || !strings.HasPrefix(h, "  ID") {
		t.Errorf("gutter wrong:\n%s\n%s\n%s", h, r0, r1)
	}
	if !strings.Contains(r1, "a-very-…") { // NAME is capped at 8
		t.Errorf("long cell not cut with …: %q", r1)
	}
	if !strings.HasPrefix(r0, "   1  alpha") || !strings.HasPrefix(r1, "▸ 22  a-very-…") { // ID is right-aligned
		t.Errorf("numbers are not right-aligned:\n%s\n%s", r0, r1)
	}
	at := func(s, sub string) int { return utf8.RuneCountInString(s[:strings.Index(s, sub)]) }
	if at(h, "MS")+2 != at(r1, "10.5")+4 { // MS ends where its widest cell ends
		t.Errorf("MS column not right-aligned:\n%s\n%s", h, r1)
	}
}

func TestTableLastColumnTakesTheRoom(t *testing.T) {
	tb := table{cols: []col{{"A", false, 0, 2}, {"MESSAGE", false, 0, 1}}, rows: [][]string{{"x", strings.Repeat("m", 100)}}, sel: -1}
	l := ansi.Strip(tb.render(40)[1])
	if ansi.StringWidth(l) != 40 || !strings.HasSuffix(l, "…") {
		t.Errorf("last column should fill and end in …: %q (%d)", l, ansi.StringWidth(l))
	}
}

func TestWindowKeepsSelection(t *testing.T) {
	for _, c := range [][4]int{{100, 0, 10, 0}, {100, 50, 10, 45}, {100, 99, 10, 90}, {5, 3, 10, 0}} {
		from, to := window(c[0], c[1], c[2])
		if from != c[3] || c[1] < from || c[1] >= to {
			t.Errorf("window(%d,%d,%d) = %d..%d", c[0], c[1], c[2], from, to)
		}
	}
}
