package bigquery

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// ---- statements that change metadata only ----

// native runs CREATE VIEW, CREATE SCHEMA and the DROPs, which need no engine.
func (s *Service) native(st *stmt) (*result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := &result{verb: st.verb}
	t := st.target
	switch st.verb {
	case "CREATE_SCHEMA":
		key := t.project + "/" + t.dataset
		if _, ok := s.st.datasets[key]; ok {
			if st.ifExists {
				return res, nil
			}
			return nil, errf(409, "duplicate", "Already Exists: Dataset %s:%s", t.project, t.dataset)
		}
		if !datasetIDRE.MatchString(t.dataset) {
			return nil, invalid("Invalid dataset ID %q", t.dataset)
		}
		loc := strings.ToUpper(st.location)
		if loc == "" {
			loc = "US"
		}
		now := ms(time.Now())
		s.st.datasets[key] = &dataset{Ref: datasetRef{t.project, t.dataset}, Location: loc, Description: st.description, Created: now, Modified: now, tables: map[string]*table{}}
	case "DROP_SCHEMA":
		d := s.st.datasets[t.project+"/"+t.dataset]
		switch {
		case d == nil && st.ifExists:
		case d == nil:
			return nil, notFound("Not found: Dataset %s:%s", t.project, t.dataset)
		case len(d.tables) > 0 && !st.cascade:
			return nil, errf(400, "resourceInUse", "Dataset %s:%s is still in use", t.project, t.dataset)
		default:
			delete(s.st.datasets, t.project+"/"+t.dataset)
		}
	case "DROP_TABLE", "DROP_VIEW":
		d, err := s.getDS(t.project, t.dataset)
		if err != nil {
			if st.ifExists {
				return res, nil
			}
			return nil, err
		}
		cur := d.tables[t.table]
		want := map[string]string{"DROP_TABLE": "TABLE", "DROP_VIEW": "VIEW"}[st.verb]
		switch {
		case cur == nil && st.ifExists:
		case cur == nil:
			return nil, notFound("Not found: %s %s:%s.%s", strings.ToUpper(want[:1])+strings.ToLower(want[1:]), t.project, t.dataset, t.table)
		case cur.Type != want:
			return nil, invalid("%s is a %s: use DROP %s", cur.Ref, strings.ToLower(cur.Type), cur.Type)
		default:
			delete(d.tables, t.table)
		}
	case "CREATE_VIEW":
		d, err := s.getDS(t.project, t.dataset)
		if err != nil {
			return nil, err
		}
		if cur := d.tables[t.table]; cur != nil && !st.orReplace {
			if st.ifExists {
				return res, nil
			}
			return nil, errf(409, "duplicate", "Already Exists: Table %s", cur.Ref)
		}
		now := ms(time.Now())
		d.tables[t.table] = &table{Ref: t.ref(), Type: "VIEW", View: &view{Query: st.viewSQL}, Created: now, Modified: now}
	}
	return res, nil
}

// ---- persistence ----

type savedDataset struct {
	Dataset *dataset `json:"dataset"`
	Tables  []*table `json:"tables"`
}

type saved struct {
	Datasets []savedDataset `json:"datasets"`
	Jobs     []*job         `json:"jobs"`
}

// ponytail: a query job's rows are not saved: its getQueryResults answers empty after a restart.
func (s *Service) Save() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := saved{}
	for _, k := range core.SortedKeys(s.st.datasets) {
		d := s.st.datasets[k]
		sd := savedDataset{Dataset: d}
		for _, id := range core.SortedKeys(d.tables) {
			sd.Tables = append(sd.Tables, d.tables[id])
		}
		out.Datasets = append(out.Datasets, sd)
	}
	for _, k := range s.st.jobOrder {
		if j := s.st.jobs[k]; j != nil {
			out.Jobs = append(out.Jobs, j)
		}
	}
	return json.Marshal(out)
}

func (s *Service) Load(b []byte) error {
	var in saved
	if err := json.Unmarshal(b, &in); err != nil {
		return err
	}
	st := newState()
	for _, sd := range in.Datasets {
		d := sd.Dataset
		d.tables = map[string]*table{}
		for _, t := range sd.Tables {
			d.tables[t.Ref.TableID] = t
		}
		st.datasets[d.Ref.ProjectID+"/"+d.Ref.DatasetID] = d
	}
	for _, j := range in.Jobs {
		k := j.Ref.ProjectID + "/" + j.Ref.JobID
		st.jobs[k] = j
		st.jobOrder = append(st.jobOrder, k)
	}
	s.mu.Lock()
	s.st = st
	s.mu.Unlock()
	return nil
}

// ---- console ----

// Resources lists datasets, tables and jobs.
func (s *Service) Resources() []core.Table {
	s.mu.Lock()
	defer s.mu.Unlock()
	ds := core.Table{Name: "Datasets", Columns: []string{"Dataset", "Location", "Tables", "Created"}}
	ts := core.Table{Name: "Tables", Columns: []string{"Table", "Type", "Rows", "Columns"}}
	for _, k := range core.SortedKeys(s.st.datasets) {
		d := s.st.datasets[k]
		ds.Rows = append(ds.Rows, []string{d.Ref.ProjectID + ":" + d.Ref.DatasetID, d.Location, strconv.Itoa(len(d.tables)), time.UnixMilli(d.Created).Format(time.DateTime)})
		for _, id := range core.SortedKeys(d.tables) {
			t := d.tables[id]
			ts.Rows = append(ts.Rows, []string{t.Ref.String(), t.Type, strconv.Itoa(len(t.Rows)), strconv.Itoa(len(t.fields()))})
		}
	}
	js := core.Table{Name: "Jobs", Columns: []string{"Job", "Type", "State", "Created"}}
	for _, j := range s.sortedJobs() {
		state := "DONE"
		if j.Err != nil {
			state = "FAILED: " + j.Err.Message
		}
		js.Rows = append(js.Rows, []string{j.Ref.ProjectID + ":" + j.Ref.Location + "." + j.Ref.JobID, j.Type, state, time.UnixMilli(j.Created).Format(time.DateTime)})
	}
	return []core.Table{ds, ts, js}
}

// Inspect returns a dataset or table (with its first rows) as JSON.
func (s *Service) Inspect(tbl, id string) any {
	s.mu.Lock()
	defer s.mu.Unlock()
	project, rest, _ := strings.Cut(id, ":")
	switch tbl {
	case "Datasets":
		if d := s.st.datasets[project+"/"+rest]; d != nil {
			return d.api()
		}
	case "Tables":
		dsID, tID, _ := strings.Cut(rest, ".")
		if d := s.st.datasets[project+"/"+dsID]; d != nil && d.tables[tID] != nil {
			t := d.tables[tID]
			out := t.api()
			n := min(len(t.Rows), 20)
			out["sampleRows"] = wireRows(t.fields(), t.Rows[:n], false)
			return out
		}
	case "Jobs":
		_, jid, _ := strings.Cut(rest, ".")
		if j := s.st.jobs[project+"/"+jid]; j != nil {
			return j.api()
		}
	}
	return nil
}
