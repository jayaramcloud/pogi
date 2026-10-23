package web

import (
	"encoding/json"
	"net/http"
	"slices"
)

// costAPI is GET /_pogi/cost.json: the running estimate the Cost page shows, as JSON for `pogi cost`.
func (ui *UI) costAPI(w http.ResponseWriter, r *http.Request) {
	region := r.URL.Query().Get("region")
	if region == "" {
		region = ui.defaultRegion()
	} else if !slices.Contains(costRegions(), region) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]string{"error": "unknown region " + region})
		return
	}
	est := ui.estimate(region, scope{Account: all, Region: all}, "")
	type row struct {
		Cloud   string  `json:"cloud"`
		Service string  `json:"service"`
		Name    string  `json:"name"`
		Detail  string  `json:"detail"`
		Monthly float64 `json:"monthly"`
		Priced  bool    `json:"priced"`
	}
	out := struct {
		Region  string  `json:"region"`
		Monthly float64 `json:"monthly"`
		Basis   string  `json:"basis"`
		Rows    []row   `json:"rows"`
	}{region, est.Total, est.Basis, []row{}}
	for _, c := range est.Rows {
		out.Rows = append(out.Rows, row{c.Cloud, c.Service, c.Name, c.Detail, c.Monthly, c.Priced})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(out)
}
