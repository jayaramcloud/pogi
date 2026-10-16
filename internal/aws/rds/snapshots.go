package rds

// Snapshots keep data: a SQL dump of the server's databases, made with
// pg_dump or mysqldump inside the container and kept in the rds directory
// (DataDir/rds when the state is kept on disk, else a temporary one).
// Restores make a fresh container that loads a dump at init (Server.Seed):
// the snapshot's, or for point-in-time restores one of the source made then.

import (
	"cmp"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// snapPogi is pogi's side of a DB or cluster snapshot: what its restores
// take that the API shape lacks.
type snapPogi struct {
	Password string `json:"masterPassword,omitempty"`
	DBName   string `json:"dbName,omitempty"`
	Class    string `json:"instanceClass,omitempty"`
}

// DBClusterSnapshot is a manual snapshot of a DB cluster.
type DBClusterSnapshot struct {
	AvailabilityZones                []string
	DBClusterSnapshotIdentifier      string
	DBClusterIdentifier              string
	SnapshotCreateTime               time.Time
	Engine                           string
	EngineMode                       string
	AllocatedStorage                 int
	Status                           string
	Port                             int
	VpcId                            string
	ClusterCreateTime                time.Time
	MasterUsername                   string
	EngineVersion                    string
	LicenseModel                     string
	SnapshotType                     string
	PercentProgress                  int
	StorageEncrypted                 bool
	KmsKeyId                         string `json:",omitempty"`
	DBClusterSnapshotArn             string
	IAMDatabaseAuthenticationEnabled bool
	TagList                          []Tag
	DbClusterResourceId              string
	StorageType                      string
	Pogi                             snapPogi `json:"pogi" query:"-"`
}

// files is the directory of dumps; lock held.
func (s *Service) files() string {
	if s.dir == "" {
		cfg := s.hub.Cfg
		s.dir = filepath.Join(cfg.DataDir, "rds")
		if cmp.Or(cfg.StorageModes["rds"], cfg.Storage, map[bool]string{true: "hybrid"}[cfg.Persist], "memory") == "memory" {
			if d, err := os.MkdirTemp("", "pogi-rds-"); err == nil {
				s.dir, s.temp = d, true
			}
		}
	}
	return s.dir
}

// snapFile is the dump of the snapshot with state key k; lock held.
func (s *Service) snapFile(k string, cluster bool) string {
	return filepath.Join(s.files(), map[bool]string{false: "snapshots", true: "cluster-snapshots"}[cluster], filepath.FromSlash(k)+".sql")
}

// keep dumps srv into file for a new snapshot, in turn with the container's
// other work (a delete waits); the snapshot is "creating" until then. live
// says, lock held, whether the snapshot still exists. Lock held.
func (s *Service) keep(srv *Server, engine, file string, status *string, pct *int, live func() bool) {
	if srv.Container == "" || srv.State != "available" || wire(engine) == "" {
		return // ponytail: nothing runs (or an engine pogi cannot dump): the snapshot is metadata, its restores start empty
	}
	*status, *pct = "creating", 0
	name, pw := srv.Container, srv.Password
	s.box.serial(name, func() {
		err := s.dump(name, engine, pw, file)
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case !live():
			os.Remove(file)
		case err != nil:
			*status = "failed"
			s.hub.Log.Warn("rds snapshot failed", "container", name, "err", err)
		default:
			*status, *pct = "available", 100
		}
	})
}

// dumpScripts write a dump to stdout, or an error and a non-zero exit.
// Postgres: roles (errors ignored: the master user exists), then each
// database, made when missing. MySQL and MariaDB: the user schemas.
// ponytail: names with spaces break the loops; MySQL users other than the master are not kept.
const (
	pgDump = `(
dbs=$(psql -U "$POSTGRES_USER" -d postgres -Atc 'select datname from pg_database where not datistemplate') || exit 1
printf '%s\n' '\set ON_ERROR_STOP off'
pg_dumpall -U "$POSTGRES_USER" --roles-only || exit 1
printf '%s\n' '\set ON_ERROR_STOP on'
for d in $dbs; do
  printf '%s\n' "select 'create database \"$d\"' where not exists (select from pg_database where datname = '$d')\\gexec" "\\connect \"$d\""
  pg_dump -U "$POSTGRES_USER" "$d" || exit 1
done
) >/tmp/pogi-dump.sql 2>/tmp/pogi-dump.err || { cat /tmp/pogi-dump.err; exit 1; }
cat /tmp/pogi-dump.sql; rm -f /tmp/pogi-dump.sql /tmp/pogi-dump.err`
	myDump = `(
dbs=$(CLI -uroot -N -e "select schema_name from information_schema.schemata where schema_name not in ('mysql','information_schema','performance_schema','sys')") || exit 1
[ -z "$dbs" ] || DUMP -uroot --single-transaction --routines --events --triggers --databases $dbs || exit 1
) >/tmp/pogi-dump.sql 2>/tmp/pogi-dump.err || { cat /tmp/pogi-dump.err; exit 1; }
cat /tmp/pogi-dump.sql; rm -f /tmp/pogi-dump.sql /tmp/pogi-dump.err`
)

// Dump returns the databases of a running container as SQL its image loads
// at init. pw is the master (MySQL root) password.
// ponytail: the dump passes through memory; stream it if databases grow big.
func (b *Box) Dump(name, engine, pw string) ([]byte, error) {
	script := pgDump
	if wire(engine) == "mysql" {
		cli, dump := "mysql", "mysqldump"
		if engine == "mariadb" {
			cli, dump = "mariadb", "mariadb-dump"
		}
		script = strings.NewReplacer("CLI", cli, "DUMP", dump).Replace(myDump)
	}
	return b.Out(name, "env", "MYSQL_PWD="+pw, "sh", "-c", script)
}

// dump writes the databases of a running container to file.
func (s *Service) dump(name, engine, pw, file string) error {
	out, err := s.box.Dump(name, engine, pw)
	if err == nil {
		err = os.MkdirAll(filepath.Dir(file), 0o700)
	}
	if err == nil {
		err = os.WriteFile(file, out, 0o600)
	}
	return err
}

type snapshotAttribute struct {
	AttributeName   string
	AttributeValues []string
}

// describeDBSnapshotAttributes: snapshots are not shared, so "restore" names no accounts.
func (s *Service) describeDBSnapshotAttributes(c *core.Ctx, in *snapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snapshot(c, in.DBSnapshotIdentifier)
	if err != nil {
		return nil, err
	}
	return map[string]any{"DBSnapshotAttributesResult": struct {
		DBSnapshotIdentifier string
		DBSnapshotAttributes []snapshotAttribute
	}{sn.DBSnapshotIdentifier, []snapshotAttribute{{"restore", nil}}}}, nil
}

// ---- DB cluster snapshots ----

type clusterSnapshotInput struct {
	DBClusterSnapshotIdentifier, DBClusterIdentifier, SnapshotType, Marker string
	MaxRecords                                                             int
	Filters                                                                []Filter
	Tags                                                                   []Tag
}

func (s *Service) createDBClusterSnapshot(c *core.Ctx, in *clusterSnapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.DBClusterIdentifier)
	if err != nil {
		return nil, err
	}
	if cl.Pogi.State != "available" {
		return nil, core.Errorf(400, "InvalidDBClusterStateFault", "DbCluster %s is not in available state.", cl.DBClusterIdentifier)
	}
	sn, err := s.addClusterSnapshot(c, in.DBClusterSnapshotIdentifier, cl, in.Tags)
	if err != nil {
		return nil, err
	}
	return map[string]any{"DBClusterSnapshot": Clone(sn)}, nil
}

// addClusterSnapshot snapshots a cluster; lock held.
func (s *Service) addClusterSnapshot(c *core.Ctx, id string, cl *DBCluster, tags []Tag) (*DBClusterSnapshot, error) {
	id = strings.ToLower(id)
	if err := ValidID("DBClusterSnapshotIdentifier", id, 255); err != nil {
		return nil, err
	}
	k := key(c, id)
	if s.st.ClusterSnapshots[k] != nil {
		return nil, core.Errorf(400, "DBClusterSnapshotAlreadyExistsFault", "Cannot create the cluster snapshot because one with the identifier %s already exists.", id)
	}
	if len(tags) == 0 && cl.CopyTagsToSnapshot {
		tags = cl.TagList
	}
	v := s.clusterView(c, cl)
	sn := &DBClusterSnapshot{AvailabilityZones: cl.AvailabilityZones, DBClusterSnapshotIdentifier: id, DBClusterIdentifier: cl.DBClusterIdentifier,
		SnapshotCreateTime: time.Now().UTC(), Engine: cl.Engine, EngineMode: cl.EngineMode, AllocatedStorage: cl.AllocatedStorage, Status: "available",
		Port: v.Port, VpcId: DefaultVPC, ClusterCreateTime: cl.ClusterCreateTime, MasterUsername: cl.MasterUsername, EngineVersion: cl.EngineVersion,
		LicenseModel: engines[cl.Engine].license, SnapshotType: "manual", PercentProgress: 100, StorageEncrypted: cl.StorageEncrypted, KmsKeyId: cl.KmsKeyId,
		DBClusterSnapshotArn: c.ARN("rds", "cluster-snapshot:"+id), IAMDatabaseAuthenticationEnabled: cl.IAMDatabaseAuthenticationEnabled,
		TagList: AddTags(nil, tags), DbClusterResourceId: cl.DbClusterResourceId, StorageType: cl.StorageType,
		Pogi: snapPogi{Password: cl.Pogi.Password, DBName: cl.DatabaseName, Class: cl.DBClusterInstanceClass}}
	s.st.ClusterSnapshots[k] = sn
	s.keep(&cl.Pogi, cl.Engine, s.snapFile(k, true), &sn.Status, &sn.PercentProgress, func() bool { return s.st.ClusterSnapshots[k] == sn })
	return sn, nil
}

func (s *Service) clusterSnapshot(c *core.Ctx, id string) (*DBClusterSnapshot, error) {
	if sn := s.st.ClusterSnapshots[key(c, ident(id))]; sn != nil {
		return sn, nil
	}
	return nil, core.Errorf(404, "DBClusterSnapshotNotFoundFault", "DBClusterSnapshot %s not found.", id)
}

func (s *Service) describeDBClusterSnapshots(c *core.Ctx, in *clusterSnapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if in.DBClusterSnapshotIdentifier != "" {
		if _, err := s.clusterSnapshot(c, in.DBClusterSnapshotIdentifier); err != nil {
			return nil, err
		}
	}
	var list []*DBClusterSnapshot
	for k, sn := range s.st.ClusterSnapshots {
		switch {
		case !mine(c, k),
			in.DBClusterSnapshotIdentifier != "" && sn.DBClusterSnapshotIdentifier != ident(in.DBClusterSnapshotIdentifier),
			in.DBClusterIdentifier != "" && sn.DBClusterIdentifier != ident(in.DBClusterIdentifier),
			in.SnapshotType != "" && sn.SnapshotType != in.SnapshotType:
			continue
		}
		ok, err := filter(in.Filters, map[string][]string{"db-cluster-id": {sn.DBClusterIdentifier}, "db-cluster-snapshot-id": {sn.DBClusterSnapshotIdentifier, sn.DBClusterSnapshotArn},
			"snapshot-type": {sn.SnapshotType}, "engine": {sn.Engine}})
		if err != nil {
			return nil, err
		}
		if ok {
			list = append(list, sn)
		}
	}
	list, marker := Page(list, func(sn *DBClusterSnapshot) string { return sn.DBClusterSnapshotIdentifier }, in.Marker, in.MaxRecords)
	return map[string]any{"DBClusterSnapshots": each(list, Clone[DBClusterSnapshot]), "Marker": marker}, nil
}

func (s *Service) deleteDBClusterSnapshot(c *core.Ctx, in *clusterSnapshotInput) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.clusterSnapshot(c, in.DBClusterSnapshotIdentifier)
	if err != nil {
		return nil, err
	}
	k := key(c, sn.DBClusterSnapshotIdentifier)
	delete(s.st.ClusterSnapshots, k)
	os.Remove(s.snapFile(k, true))
	return map[string]any{"DBClusterSnapshot": Clone(sn)}, nil
}

// ---- restores ----

type restoreInstanceInput struct {
	createInstanceInput
	DBSnapshotIdentifier, SourceDBInstanceIdentifier, TargetDBInstanceIdentifier string
}

// restoreDBInstance serves RestoreDBInstanceFromDBSnapshot and
// RestoreDBInstanceToPointInTime: a new instance like the source, with the
// source's master password, whose fresh container loads the snapshot's dump
// or one of the source made now.
func (s *Service) restoreDBInstance(c *core.Ctx, in *restoreInstanceInput) (any, error) {
	ci := &in.createInstanceInput
	ci.DBInstanceIdentifier = strings.ToLower(cmp.Or(in.TargetDBInstanceIdentifier, ci.DBInstanceIdentifier))
	if err := ValidID("DBInstanceIdentifier", ci.DBInstanceIdentifier, 63); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.st.Instances[key(c, ci.DBInstanceIdentifier)] != nil {
		s.mu.Unlock()
		return nil, core.Errorf(400, "DBInstanceAlreadyExists", "DB instance already exists")
	}
	var pw, seed, from string
	if in.DBSnapshotIdentifier != "" {
		sn, err := s.snapshot(c, in.DBSnapshotIdentifier)
		if err == nil && sn.Status != "available" {
			err = core.Errorf(400, "InvalidDBSnapshotState", "DB snapshot %s is not available.", sn.DBSnapshotIdentifier)
		}
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		ci.Engine, ci.EngineVersion, ci.MasterUsername, ci.DBName = sn.Engine, sn.EngineVersion, sn.MasterUsername, sn.Pogi.DBName
		ci.DBInstanceClass, ci.AllocatedStorage = cmp.Or(ci.DBInstanceClass, sn.Pogi.Class), cmp.Or(ci.AllocatedStorage, sn.AllocatedStorage)
		pw, seed = sn.Pogi.Password, s.snapFile(key(c, sn.DBSnapshotIdentifier), false)
	} else {
		i, err := s.instance(c, in.SourceDBInstanceIdentifier)
		if err == nil && i.DBClusterIdentifier != "" {
			err = combo("DB instance %s belongs to a DB cluster; restore the cluster instead.", i.DBInstanceIdentifier)
		}
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		ci.Engine, ci.EngineVersion, ci.MasterUsername, ci.DBName = i.Engine, i.EngineVersion, i.MasterUsername, i.DBName
		ci.DBInstanceClass, ci.AllocatedStorage = cmp.Or(ci.DBInstanceClass, i.DBInstanceClass), cmp.Or(ci.AllocatedStorage, i.AllocatedStorage)
		pw = i.Pogi.Password
		if i.Pogi.Container != "" && i.Pogi.State == "available" {
			from, seed = i.Pogi.Container, filepath.Join(s.files(), "restores", s.box.Name(c.Account, c.Region, ci.DBInstanceIdentifier)+".sql")
		}
	}
	s.mu.Unlock()
	if err := s.pitr(from, ci.Engine, pw, seed); err != nil {
		return nil, err
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	i, err := s.addInstance(c, ci.DBInstanceIdentifier, ci, pw, docker)
	if err != nil {
		return nil, err
	}
	if i.Pogi.Container != "" {
		i.Pogi.Seed = seed // Box reads it when it makes the container, after this lock
	}
	return map[string]any{"DBInstance": s.view(c, i)}, nil
}

// pitr dumps the source of a point-in-time restore, when one runs.
// ponytail: the data is the source's now, not at RestoreTime; there is no log to replay.
func (s *Service) pitr(container, engine, pw, file string) error {
	if container == "" {
		return nil
	}
	if err := s.dump(container, engine, pw, file); err != nil {
		return core.Errorf(400, "InvalidDBInstanceState", "pogi could not read the source's data: %v", err)
	}
	return nil
}

type restoreClusterInput struct {
	createClusterInput
	SnapshotIdentifier, SourceDBClusterIdentifier string
}

// restoreDBCluster serves RestoreDBClusterFromSnapshot and
// RestoreDBClusterToPointInTime, as restoreDBInstance does for instances.
func (s *Service) restoreDBCluster(c *core.Ctx, in *restoreClusterInput) (any, error) {
	ci := &in.createClusterInput
	ci.DBClusterIdentifier = strings.ToLower(ci.DBClusterIdentifier)
	if err := ValidID("DBClusterIdentifier", ci.DBClusterIdentifier, 63); err != nil {
		return nil, err
	}
	s.mu.Lock()
	if s.st.Clusters[key(c, ci.DBClusterIdentifier)] != nil {
		s.mu.Unlock()
		return nil, core.Errorf(400, "DBClusterAlreadyExistsFault", "DB Cluster already exists")
	}
	var pw, seed, from string
	if in.SnapshotIdentifier != "" {
		sn, err := s.clusterSnapshot(c, in.SnapshotIdentifier)
		if err == nil && sn.Status != "available" {
			err = core.Errorf(400, "InvalidDBClusterSnapshotStateFault", "DB cluster snapshot %s is not available.", sn.DBClusterSnapshotIdentifier)
		}
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		ci.Engine, ci.EngineVersion, ci.MasterUsername = sn.Engine, sn.EngineVersion, sn.MasterUsername
		ci.DatabaseName, ci.EngineMode = cmp.Or(ci.DatabaseName, sn.Pogi.DBName), cmp.Or(ci.EngineMode, sn.EngineMode)
		ci.DBClusterInstanceClass = cmp.Or(ci.DBClusterInstanceClass, sn.Pogi.Class)
		pw, seed = sn.Pogi.Password, s.snapFile(key(c, sn.DBClusterSnapshotIdentifier), true)
	} else {
		cl, err := s.cluster(c, in.SourceDBClusterIdentifier)
		if err != nil {
			s.mu.Unlock()
			return nil, err
		}
		ci.Engine, ci.EngineVersion, ci.MasterUsername = cl.Engine, cl.EngineVersion, cl.MasterUsername
		ci.DatabaseName, ci.EngineMode = cmp.Or(ci.DatabaseName, cl.DatabaseName), cmp.Or(ci.EngineMode, cl.EngineMode)
		ci.DBClusterInstanceClass = cmp.Or(ci.DBClusterInstanceClass, cl.DBClusterInstanceClass)
		pw = cl.Pogi.Password
		if cl.Pogi.Container != "" && cl.Pogi.State == "available" {
			from, seed = cl.Pogi.Container, filepath.Join(s.files(), "restores", s.box.Name(c.Account, c.Region, ci.DBClusterIdentifier)+".sql")
		}
	}
	s.mu.Unlock()
	if err := s.pitr(from, ci.Engine, pw, seed); err != nil {
		return nil, err
	}
	docker := s.box.Available()
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.addCluster(c, ci.DBClusterIdentifier, ci, pw, docker)
	if err != nil {
		return nil, err
	}
	if cl.Pogi.Container != "" {
		cl.Pogi.Seed = seed
	}
	return map[string]any{"DBCluster": s.clusterView(c, cl)}, nil
}

// ---- copy and modify ----

func (s *Service) copyDBSnapshot(c *core.Ctx, in *struct {
	SourceDBSnapshotIdentifier, TargetDBSnapshotIdentifier, KmsKeyId, SourceRegion, PreSignedUrl string
	CopyTags                                                                                     bool
	Tags                                                                                         []Tag
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The source is in this region, or an ARN of another region. The SDKs turn SourceRegion into a PreSignedUrl.
	from := &core.Ctx{Account: c.Account, Region: c.Region}
	if _, region, account, _, ok := core.ParseARN(in.SourceDBSnapshotIdentifier); ok {
		from.Account, from.Region = account, region
	}
	if from.Region != c.Region && in.SourceRegion == "" && in.PreSignedUrl == "" {
		return nil, invalid("SourceRegion must be given, naming the region of the source snapshot (%s), to copy a snapshot from another region.", from.Region)
	}
	if in.SourceRegion != "" && in.SourceRegion != from.Region {
		return nil, invalid("SourceRegion %s is not the region of the source snapshot (%s).", in.SourceRegion, from.Region)
	}
	src, err := s.snapshot(from, in.SourceDBSnapshotIdentifier)
	if err != nil {
		return nil, err
	}
	if src.Status != "available" {
		return nil, core.Errorf(400, "InvalidDBSnapshotState", "Cannot copy the snapshot %s because it is in state %s.", src.DBSnapshotIdentifier, src.Status)
	}
	id := strings.ToLower(in.TargetDBSnapshotIdentifier)
	if err := ValidID("TargetDBSnapshotIdentifier", id, 255); err != nil {
		return nil, err
	}
	if s.st.Snapshots[key(c, id)] != nil {
		return nil, core.Errorf(400, "DBSnapshotAlreadyExists", "Cannot create the snapshot because a snapshot with the identifier %s already exists.", id)
	}
	sn := Clone(src)
	sn.DBSnapshotIdentifier, sn.DBSnapshotArn, sn.SnapshotType = id, c.ARN("rds", "snapshot:"+id), "manual"
	sn.SnapshotCreateTime = time.Now().UTC()
	sn.SourceDBSnapshotIdentifier, sn.SourceRegion = src.DBSnapshotArn, from.Region
	if in.KmsKeyId != "" {
		sn.Encrypted, sn.KmsKeyId = true, in.KmsKeyId
	}
	switch {
	case len(in.Tags) > 0:
		sn.TagList = AddTags(nil, in.Tags)
	case !in.CopyTags:
		sn.TagList = nil
	}
	k := key(c, id)
	if b, err := os.ReadFile(s.snapFile(key(from, ident(in.SourceDBSnapshotIdentifier)), false)); err == nil { // the dump, if there is one
		dst := s.snapFile(k, false)
		if os.MkdirAll(filepath.Dir(dst), 0o755) == nil {
			os.WriteFile(dst, b, 0o644)
		}
	}
	s.st.Snapshots[k] = sn
	return map[string]any{"DBSnapshot": Clone(sn)}, nil
}

// modifyDBSnapshot changes the engine version of a snapshot (the only thing the API lets it change here).
func (s *Service) modifyDBSnapshot(c *core.Ctx, in *struct{ DBSnapshotIdentifier, EngineVersion, OptionGroupName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sn, err := s.snapshot(c, in.DBSnapshotIdentifier)
	if err != nil {
		return nil, err
	}
	if in.EngineVersion != "" {
		sn.EngineVersion = in.EngineVersion
	}
	return map[string]any{"DBSnapshot": Clone(sn)}, nil
}
