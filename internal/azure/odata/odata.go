// Package odata reads an OData $filter and applies it to a JSON object. Microsoft Graph and Azure Resource
// Manager both use it, so a filter means the same in each.
package odata

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"
)

var (
	fnRe    = regexp.MustCompile(`(?is)^(startswith|endswith|contains)\(\s*([\w/]+)\s*,\s*'((?:[^']|'')*)'\s*\)$`)
	anyRe   = regexp.MustCompile(`(?is)^([\w/]+)/any\(\s*(\w+)\s*:\s*(.+)\)$`)
	inRe    = regexp.MustCompile(`(?is)^([\w/]+)\s+in\s+\((.*)\)$`)
	cmpRe   = regexp.MustCompile(`(?is)^([\w/]+)\s+(eq|ne)\s+(.+)$`)
	valueRe = regexp.MustCompile(`'((?:[^']|'')*)'`)
)

// Parse reads a $filter: eq and ne, startswith, endswith, contains, in, any(), and, or and parentheses. It
// returns the test the filter describes, or an error for an expression it cannot read.
func Parse(f string) (func(map[string]any) bool, error) {
	f = strings.TrimSpace(f)
	if f == "" {
		return func(map[string]any) bool { return true }, nil
	}
	if parts := splitTop(f, "or"); len(parts) > 1 {
		return combine(parts, false)
	}
	if parts := splitTop(f, "and"); len(parts) > 1 {
		return combine(parts, true)
	}
	if strings.HasPrefix(f, "(") && strings.HasSuffix(f, ")") && splitParen(f) {
		return Parse(f[1 : len(f)-1])
	}
	if m := fnRe.FindStringSubmatch(f); m != nil {
		want, fn := unquote(m[3]), map[string]func(a, b string) bool{"startswith": strings.HasPrefix, "endswith": strings.HasSuffix, "contains": strings.Contains}[strings.ToLower(m[1])]
		return func(o map[string]any) bool {
			v, ok := Lookup(o, m[2]).(string)
			return ok && fn(strings.ToLower(v), strings.ToLower(want))
		}, nil
	}
	if m := anyRe.FindStringSubmatch(f); m != nil {
		inner, err := Parse(strings.ReplaceAll(m[3], m[2]+" ", "_ ")) // "c eq 'x'" -> "_ eq 'x'"
		if err != nil {
			return nil, err
		}
		return func(o map[string]any) bool {
			items, _ := Lookup(o, m[1]).([]any)
			return slices.ContainsFunc(items, func(it any) bool { return inner(map[string]any{"_": it}) })
		}, nil
	}
	if m := inRe.FindStringSubmatch(f); m != nil {
		var set []string
		for _, v := range valueRe.FindAllStringSubmatch(m[2], -1) {
			set = append(set, unquote(v[1]))
		}
		return func(o map[string]any) bool {
			v, _ := Lookup(o, m[1]).(string)
			return slices.ContainsFunc(set, func(s string) bool { return strings.EqualFold(s, v) })
		}, nil
	}
	if m := cmpRe.FindStringSubmatch(f); m != nil {
		lit := strings.TrimSpace(m[3])
		eq := strings.EqualFold(m[2], "eq")
		return func(o map[string]any) bool { return equal(Lookup(o, m[1]), lit) == eq }, nil
	}
	return nil, fmt.Errorf("unsupported expression %q", f)
}

func unquote(s string) string { return strings.ReplaceAll(s, "''", "'") }

// equal compares a property to a filter literal: 'text' (case-insensitive), true, false, null or a number.
func equal(v any, lit string) bool {
	switch {
	case strings.HasPrefix(lit, "'") && strings.HasSuffix(lit, "'") && len(lit) >= 2:
		s, ok := v.(string)
		return ok && strings.EqualFold(s, unquote(lit[1:len(lit)-1]))
	case lit == "null":
		return v == nil
	case lit == "true" || lit == "false":
		b, ok := v.(bool)
		return ok && b == (lit == "true")
	}
	n, err := strconv.ParseFloat(lit, 64)
	f, ok := v.(float64)
	return err == nil && ok && f == n
}

// Lookup reads a property by a "/"- or "."-separated path, ignoring case.
func Lookup(o map[string]any, path string) any {
	var cur any = o
	for _, p := range strings.FieldsFunc(path, func(r rune) bool { return r == '/' }) {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = nil
		for k, v := range m {
			if strings.EqualFold(k, p) {
				cur = v
				break
			}
		}
	}
	return cur
}

func combine(parts []string, and bool) (func(map[string]any) bool, error) {
	var fs []func(map[string]any) bool
	for _, p := range parts {
		f, err := Parse(p)
		if err != nil {
			return nil, err
		}
		fs = append(fs, f)
	}
	return func(o map[string]any) bool {
		for _, f := range fs {
			if f(o) != and {
				return !and
			}
		}
		return and
	}, nil
}

// splitTop splits at the word (and, or) where it stands alone outside quotes and parentheses.
func splitTop(f, word string) []string {
	var parts []string
	depth, quote, start := 0, false, 0
	for i := 0; i < len(f); i++ {
		switch c := f[i]; {
		case c == '\'':
			quote = !quote
		case quote:
		case c == '(':
			depth++
		case c == ')':
			depth--
		case depth == 0 && c == ' ' && len(f) >= i+len(word)+2 && strings.EqualFold(f[i+1:i+1+len(word)], word) && f[i+1+len(word)] == ' ':
			parts = append(parts, f[start:i])
			start = i + len(word) + 2
			i += len(word) + 1
		}
	}
	if len(parts) == 0 {
		return nil
	}
	return append(parts, f[start:])
}

// splitParen reports whether the opening parenthesis of f closes at its end.
func splitParen(f string) bool {
	depth, quote := 0, false
	for i := 0; i < len(f); i++ {
		switch c := f[i]; {
		case c == '\'':
			quote = !quote
		case quote:
		case c == '(':
			depth++
		case c == ')':
			depth--
			if depth == 0 {
				return i == len(f)-1
			}
		}
	}
	return false
}
