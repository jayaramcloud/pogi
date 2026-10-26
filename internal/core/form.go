package core

import (
	"regexp"
	"strings"
)

var camelBreak = regexp.MustCompile(`([a-z0-9])([A-Z])|([A-Z]+)([A-Z][a-z])`)

// Label turns a member name into a label: DBInstanceIdentifier is "DB Instance Identifier".
func Label(name string) string {
	l := camelBreak.ReplaceAllString(name, "$1$3 $2$4")
	if l != "" {
		l = strings.ToUpper(l[:1]) + l[1:]
	}
	return l
}

// Sentence is Label in sentence case, for titles: CreateNodePool is "Create node pool". Acronyms
// (DB, IAM) keep their capitals.
func Sentence(name string) string {
	words := strings.Split(Label(name), " ")
	for i, w := range words {
		if i > 0 && w != strings.ToUpper(w) {
			words[i] = strings.ToLower(w)
		}
	}
	return strings.Join(words, " ")
}

// Field is one input of a console form, built from an API model (botocore, protobuf)
// or written by hand. Values are read back into JSON by member name.
type Field struct {
	Name     string // member name, as the API spells it
	Label    string // "DB Instance Identifier": shown instead of the name
	Kind     string // string, integer, number, bool, enum, json, struct, list, map
	Required bool
	Enum     []string
	Fields   []Field // the members of a struct
	Elem     *Field  // the element of a list, the value of a map
	Default  string  // prefilled value
	Help     string  // a hint under the input, for rules the API only reports after the call
	Pattern  string  // a regular expression the browser checks a string input against
}

// Form is a create or update call the console can render and submit.
type Form struct {
	Op, Title string
	Fields    []Field
}
