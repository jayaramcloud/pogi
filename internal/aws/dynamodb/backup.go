package dynamodb

import (
	"cmp"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// This file has on-demand backups, restore (from a backup or "to a point in
// time") and global table replicas.

// Backup is an on-demand backup: the table as it was, items included.
type Backup struct {
	Arn     string    `json:"arn"`
	Name    string    `json:"name"`
	Account string    `json:"account"`
	Region  string    `json:"region"`
	Created time.Time `json:"created"`
	Deleted bool      `json:"deleted,omitempty"`
	Table   *Table    `json:"table"`
}

// restoreSummary is what DescribeTable reports for a restored table.
type restoreSummary struct {
	SourceArn string    `json:"sourceArn"`
	BackupArn string    `json:"backupArn,omitempty"`
	At        time.Time `json:"at"`
}

func errNoTableBackup(name string) error {
	return core.Errorf(400, "TableNotFoundException", "Table not found: %s", name)
}

func errNoBackup(arn string) error {
	return core.Errorf(400, "BackupNotFoundException", "Backup not found: %s", arn)
}

// clone copies a table with its items; nothing is shared with t.
func (t *Table) clone() *Table {
	b, _ := json.Marshal(t)
	n := &Table{}
	json.Unmarshal(b, n)
	if n.Items == nil {
		n.Items = map[string]Item{}
	}
	return n
}

func (t *Table) size() int {
	n := 0
	for _, it := range t.Items {
		n += itemSize(it)
	}
	return n
}

func (b *Backup) details() map[string]any {
	status := "AVAILABLE"
	if b.Deleted {
		status = "DELETED"
	}
	return map[string]any{"BackupArn": b.Arn, "BackupName": b.Name, "BackupSizeBytes": b.Table.size(), "BackupStatus": status,
		"BackupType": "USER", "BackupCreationDateTime": core.Unix(b.Created)}
}

func (b *Backup) description() map[string]any {
	t := b.Table
	src := map[string]any{"TableName": t.Name, "TableId": t.ID, "TableArn": t.arn(), "TableSizeBytes": t.size(), "KeySchema": t.Keys,
		"TableCreationDateTime": core.Unix(t.Created), "ProvisionedThroughput": throughputDesc(t.Throughput), "ItemCount": len(t.Items), "BillingMode": t.Billing}
	feat := map[string]any{}
	if len(t.GSIs) > 0 {
		var l []map[string]any
		for _, ix := range t.GSIs {
			l = append(l, map[string]any{"IndexName": ix.IndexName, "KeySchema": ix.KeySchema, "Projection": ix.Projection, "ProvisionedThroughput": throughputDesc(ix.ProvisionedThroughput)})
		}
		feat["GlobalSecondaryIndexes"] = l
	}
	if len(t.LSIs) > 0 {
		var l []map[string]any
		for _, ix := range t.LSIs {
			l = append(l, map[string]any{"IndexName": ix.IndexName, "KeySchema": ix.KeySchema, "Projection": ix.Projection})
		}
		feat["LocalSecondaryIndexes"] = l
	}
	if t.TTL != "" {
		feat["TimeToLiveDescription"] = map[string]string{"TimeToLiveStatus": "ENABLED", "AttributeName": t.TTL}
	} else {
		feat["TimeToLiveDescription"] = map[string]string{"TimeToLiveStatus": "DISABLED"}
	}
	return map[string]any{"BackupDetails": b.details(), "SourceTableDetails": src, "SourceTableFeatureDetails": feat}
}

var backupName = regexp.MustCompile(`^[a-zA-Z0-9_.-]{3,255}$`)

func (s *Service) createBackup(c *core.Ctx, in *struct{ TableName, BackupName string }) (any, error) {
	if !backupName.MatchString(in.BackupName) {
		return nil, errVal("1 validation error detected: Value '%s' at 'backupName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+ and have length between 3 and 255", in.BackupName)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, err := s.table(c, in.TableName)
	if err != nil {
		return nil, errNoTableBackup(in.TableName)
	}
	now := time.Now().UTC()
	b := &Backup{Arn: fmt.Sprintf("%s/backup/%014d-%s", t.arn(), now.UnixMilli(), core.RandHex(4)), Name: in.BackupName,
		Account: t.Account, Region: t.Region, Created: now, Table: t.clone()}
	s.st.Backups[b.Arn] = b
	s.j.Put("b/"+b.Arn, b)
	return map[string]any{"BackupDetails": b.details()}, nil
}

func (s *Service) listBackups(c *core.Ctx, in *struct {
	TableName               string
	Limit                   int
	TimeRangeLowerBound     *float64
	TimeRangeUpperBound     *float64
	ExclusiveStartBackupArn string
	BackupType              string
}) (any, error) {
	limit := in.Limit
	if limit <= 0 || limit > 100 {
		limit = 100
	}
	name := in.TableName
	if _, _, _, res, ok := core.ParseARN(name); ok {
		name = strings.TrimPrefix(res, "table/")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var all []*Backup
	for _, b := range s.st.Backups {
		at := float64(b.Created.UnixMilli()) / 1000
		switch {
		case b.Account != c.Account || b.Region != c.Region || b.Deleted:
		case name != "" && b.Table.Name != name:
		case in.BackupType != "" && in.BackupType != "USER" && in.BackupType != "ALL":
		case in.TimeRangeLowerBound != nil && at < *in.TimeRangeLowerBound, in.TimeRangeUpperBound != nil && at >= *in.TimeRangeUpperBound:
		case b.Arn <= in.ExclusiveStartBackupArn:
		default:
			all = append(all, b)
		}
	}
	slices.SortFunc(all, func(a, b *Backup) int { return cmp.Compare(a.Arn, b.Arn) })
	out := map[string]any{}
	if len(all) > limit {
		all = all[:limit]
		out["LastEvaluatedBackupArn"] = all[limit-1].Arn
	}
	sums := []map[string]any{}
	for _, b := range all {
		d := b.details()
		d["TableName"], d["TableId"], d["TableArn"] = b.Table.Name, b.Table.ID, b.Table.arn()
		sums = append(sums, d)
	}
	out["BackupSummaries"] = sums
	return out, nil
}

// backup finds a live backup of the caller's account; lock held.
func (s *Service) backup(c *core.Ctx, arn string) (*Backup, error) {
	if b := s.st.Backups[arn]; b != nil && !b.Deleted && b.Account == c.Account {
		return b, nil
	}
	return nil, errNoBackup(arn)
}

func (s *Service) describeBackup(c *core.Ctx, in *struct{ BackupArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.backup(c, in.BackupArn)
	if err != nil {
		return nil, err
	}
	return map[string]any{"BackupDescription": b.description()}, nil
}

func (s *Service) deleteBackup(c *core.Ctx, in *struct{ BackupArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.backup(c, in.BackupArn)
	if err != nil {
		return nil, err
	}
	d := *b
	d.Deleted = true
	delete(s.st.Backups, b.Arn)
	s.j.Delete("b/" + b.Arn)
	return map[string]any{"BackupDescription": d.description()}, nil
}

// restore makes a new table in the caller's region from a saved copy; lock held.
func (s *Service) restore(c *core.Ctx, src *Table, target string, rs *restoreSummary) (any, error) {
	if !tableName.MatchString(target) {
		return nil, errVal("1 validation error detected: Value '%s' at 'targetTableName' failed to satisfy constraint: Member must satisfy regular expression pattern: [a-zA-Z0-9_.-]+ and have length between 3 and 255", target)
	}
	k := tkey(c.Account, c.Region, target)
	if s.st.Tables[k] != nil {
		return nil, core.Errorf(400, "TableAlreadyExistsException", "Table already exists: %s", target)
	}
	// Like AWS, the copy keeps keys, indexes, billing and items, but not
	// the stream, TTL, point-in-time recovery, tags or replicas.
	t := src.clone()
	t.Name, t.Account, t.Region, t.ID, t.Created = target, c.Account, c.Region, core.UUID(), time.Now().UTC()
	t.StreamArn, t.TTL, t.PITR, t.Tags, t.Replicas, t.Protected, t.Restore = "", "", false, nil, nil, false, rs
	s.st.Tables[k] = t
	s.journalTable(t)
	for ik, it := range t.Items {
		s.j.Put(ikey(t, ik), it)
	}
	return map[string]any{"TableDescription": s.describe(t)}, nil
}

func (s *Service) restoreFromBackup(c *core.Ctx, in *struct{ TargetTableName, BackupArn string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := s.backup(c, in.BackupArn)
	if err != nil {
		return nil, err
	}
	return s.restore(c, b.Table, in.TargetTableName, &restoreSummary{SourceArn: b.Table.arn(), BackupArn: b.Arn, At: time.Now().UTC()})
}

// restoreToPointInTime copies the table as it is now: pogi keeps no item
// history, so a time in the past restores the current items.
// ponytail: no history; keep one if tests need a true past state.
func (s *Service) restoreToPointInTime(c *core.Ctx, in *struct {
	SourceTableArn, SourceTableName, TargetTableName string
	UseLatestRestorableTime                          bool
	RestoreDateTime                                  *float64
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := cmp.Or(in.SourceTableArn, in.SourceTableName)
	src, err := s.table(c, name)
	if err != nil {
		return nil, errNoTableBackup(name)
	}
	if !src.PITR {
		return nil, core.Errorf(400, "PointInTimeRecoveryUnavailableException", "Point in time recovery is not enabled for table '%s'", src.Name)
	}
	if in.UseLatestRestorableTime == (in.RestoreDateTime != nil) {
		return nil, errVal("Exactly one of UseLatestRestorableTime and RestoreDateTime must be given")
	}
	if at := in.RestoreDateTime; at != nil {
		if *at < float64(src.Created.Unix()) || *at > float64(time.Now().Unix()) {
			return nil, core.Errorf(400, "InvalidRestoreTimeException", "An error occurred (InvalidRestoreTimeException) when calling the RestoreTableToPointInTime operation: Time is out of range of the table's restorable window")
		}
	}
	return s.restore(c, src, in.TargetTableName, &restoreSummary{SourceArn: src.arn(), At: time.Now().UTC()})
}

// ---- global table replicas ----

var regionName = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]$`)

type replicaUpdate struct {
	Create *struct{ RegionName string }
	Update *struct{ RegionName string }
	Delete *struct{ RegionName string }
}

// replicaTable is the same table in another region of the same account.
func (s *Service) replicaTable(t *Table, region string) *Table {
	return s.st.Tables[tkey(t.Account, region, t.Name)]
}

// checkReplicas validates ReplicaUpdates against t before anything changes.
// stream is the view of the stream t will have after the same UpdateTable.
func (s *Service) checkReplicas(t *Table, ups []replicaUpdate, view string) error {
	have := slices.Clone(t.Replicas)
	for _, u := range ups {
		switch {
		case u.Create != nil:
			r := u.Create.RegionName
			switch {
			case !regionName.MatchString(r):
				return errVal("Invalid region name: %s", r)
			case r == t.Region || slices.Contains(have, r):
				return errVal("One or more parameter values were invalid: Replica %s already exists", r)
			case view != "NEW_AND_OLD_IMAGES":
				return errVal("Replica creation requires DynamoDB Streams enabled with StreamViewType NEW_AND_OLD_IMAGES on table %s", t.Name)
			case s.replicaTable(t, r) != nil:
				return core.Errorf(400, "ReplicaAlreadyExistsException", "Table %s already exists in %s", t.Name, r)
			}
			have = append(have, r)
		case u.Delete != nil:
			r := u.Delete.RegionName
			if !slices.Contains(have, r) {
				return core.Errorf(400, "ReplicaNotFoundException", "Replica not found in %s", r)
			}
			have = slices.DeleteFunc(have, func(x string) bool { return x == r })
		case u.Update != nil:
			if r := u.Update.RegionName; !slices.Contains(have, r) && r != t.Region {
				return core.Errorf(400, "ReplicaNotFoundException", "Replica not found in %s", r)
			}
		default:
			return errVal("ReplicaUpdates entries need Create, Update or Delete")
		}
	}
	return nil
}

// regionsOf is every region that holds the table, t's own first.
func regionsOf(t *Table) []string { return append([]string{t.Region}, t.Replicas...) }

// applyReplicas makes the changes checkReplicas accepted; lock held.
func (s *Service) applyReplicas(t *Table, ups []replicaUpdate) {
	for _, u := range ups {
		switch {
		case u.Create != nil:
			r := u.Create.RegionName
			n := t.clone()
			n.Region, n.ID, n.Created, n.Restore, n.StreamArn = r, core.UUID(), time.Now().UTC(), nil, ""
			s.enableStream(n, "NEW_AND_OLD_IMAGES")
			n.Replicas = slices.Clone(regionsOf(t)) // everyone but itself
			for _, reg := range t.Replicas {
				if o := s.replicaTable(t, reg); o != nil {
					o.Replicas = append(slices.Clone(o.Replicas), r)
					s.journalTable(o)
				}
			}
			t.Replicas = append(slices.Clone(t.Replicas), r)
			s.st.Tables[tkey(n.Account, r, n.Name)] = n
			s.journalTable(n)
			for ik, it := range n.Items {
				s.j.Put(ikey(n, ik), it)
			}
		case u.Delete != nil:
			s.dropReplica(t, u.Delete.RegionName)
		}
	}
	s.journalTable(t)
}

// dropReplica deletes the copy in region and removes it from the others' lists; lock held.
func (s *Service) dropReplica(t *Table, region string) {
	if r := s.replicaTable(t, region); r != nil {
		if st := s.st.Streams[r.StreamArn]; st != nil {
			st.Enabled = false
			s.j.Put("s/"+st.Arn, st)
		}
		delete(s.st.Tables, tkey(r.Account, region, r.Name))
		s.j.Delete("t/" + tkey(r.Account, region, r.Name))
	}
	for _, reg := range regionsOf(t) {
		if o := s.replicaTable(t, reg); o != nil {
			o.Replicas = slices.DeleteFunc(slices.Clone(o.Replicas), func(x string) bool { return x == region })
			s.journalTable(o)
		}
	}
}

// replicate copies one committed item change to the table's other regions
// and records it in their streams; lock held. It writes directly, so a
// change never travels back.
func (s *Service) replicate(t *Table, k string, old, new Item) {
	for _, reg := range t.Replicas {
		r := s.replicaTable(t, reg)
		if r == nil {
			continue
		}
		prev := r.Items[k]
		if new == nil {
			delete(r.Items, k)
			s.j.Delete(ikey(r, k))
		} else {
			r.Items[k] = new
			s.j.Put(ikey(r, k), new)
		}
		s.record(r, prev, new, nil)
	}
}

// replicaDescriptions is DescribeTable's Replicas: every region of the global table.
func replicaDescriptions(t *Table) []map[string]any {
	var out []map[string]any
	for _, r := range regionsOf(t) {
		out = append(out, map[string]any{"RegionName": r, "ReplicaStatus": "ACTIVE"})
	}
	return out
}
