package graph

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/azure/odata"
)

func decodeB64(s string) ([]byte, error) {
	return base64.RawURLEncoding.DecodeString(strings.TrimRight(s, "="))
}

// sortBy orders ids by $orderby ("field", "field desc"); the default is creation order.
func sortBy(ids []string, objs map[string]map[string]any, orderby string) {
	field, desc := "createdDateTime", false
	if f := strings.Fields(orderby); len(f) > 0 {
		field, desc = f[0], len(f) > 1 && strings.EqualFold(f[1], "desc")
	}
	slices.SortFunc(ids, func(a, b string) int {
		c := strings.Compare(strings.ToLower(fmt.Sprint(odata.Lookup(objs[a], field))), strings.ToLower(fmt.Sprint(odata.Lookup(objs[b], field))))
		if c == 0 {
			c = strings.Compare(a, b)
		}
		if desc {
			return -c
		}
		return c
	})
}
