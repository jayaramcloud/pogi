package redshift

// Snapshot copy to another region, snapshot copy grants and snapshot
// schedules. They are settings: pogi copies no snapshot and takes none on a
// schedule, but Terraform and the CLI can create, read, change and delete them.

import (
	"cmp"
	"encoding/base64"
	"regexp"
	"slices"
	"strings"

	"github.com/nrndev/pogi/internal/aws/rds"
	"github.com/nrndev/pogi/internal/core"
)

// page is rds.Page with an opaque marker (the key, base64): a marker that
// is not one is refused, as AWS refuses a made-up one.
func page[T any](items []T, key func(T) string, marker string, max int) ([]T, string, error) {
	if marker != "" {
		b, err := base64.RawURLEncoding.DecodeString(marker)
		if err != nil {
			return nil, "", invalid("The Marker parameter is not valid: %s", marker)
		}
		marker = string(b)
	}
	items, next := rds.Page(items, key, marker, max)
	if next != "" {
		next = base64.RawURLEncoding.EncodeToString([]byte(next))
	}
	return items, next, nil
}

// CopyStatus is a cluster's snapshot copy setup.
type CopyStatus struct {
	DestinationRegion             string
	RetentionPeriod               int
	ManualSnapshotRetentionPeriod int
	SnapshotCopyGrantName         string `json:",omitempty"`
}

// CopyGrant lets Redshift use a KMS key to encrypt copied snapshots.
type CopyGrant struct {
	SnapshotCopyGrantName string
	KmsKeyId              string
	Tags                  []rds.Tag
}

// Schedule is a snapshot schedule; ScheduleDefinitions are rate() or cron() expressions.
type Schedule struct {
	ScheduleDefinitions    []string
	ScheduleIdentifier     string
	ScheduleDescription    string
	Tags                   []rds.Tag
	AssociatedClusterCount int
	AssociatedClusters     []ClusterAssociation
}

// ClusterAssociation is a cluster a schedule is set on.
type ClusterAssociation struct{ ClusterIdentifier, ScheduleAssociationState string }

var regionRe = regexp.MustCompile(`^[a-z]{2}(-[a-z]+)+-[0-9]$`)

func fault(code string, status int, format string, a ...any) error {
	return core.Errorf(status, code, format, a...)
}

// ---- snapshot copy ----

func (s *Service) enableSnapshotCopy(c *core.Ctx, in *struct {
	ClusterIdentifier, DestinationRegion, SnapshotCopyGrantName string
	RetentionPeriod, ManualSnapshotRetentionPeriod              *int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	days, manual := 7, cmp.Or(cl.ManualSnapshotRetentionPeriod, -1)
	if in.RetentionPeriod != nil {
		days = *in.RetentionPeriod
	}
	if in.ManualSnapshotRetentionPeriod != nil {
		manual = *in.ManualSnapshotRetentionPeriod
	}
	switch {
	case cl.ClusterSnapshotCopyStatus != nil:
		return nil, fault("SnapshotCopyAlreadyEnabledFault", 400, "Snapshot copy is already enabled on cluster %s.", cl.ClusterIdentifier)
	case !regionRe.MatchString(in.DestinationRegion):
		return nil, fault("UnknownSnapshotCopyRegionFault", 404, "Unknown region: %q", in.DestinationRegion)
	case in.DestinationRegion == c.Region:
		return nil, invalid("The destination region must differ from the cluster's region (%s).", c.Region)
	case days < 1 || days > 35:
		return nil, invalid("RetentionPeriod must be between 1 and 35 days.")
	case manual < -1 || manual > 3653:
		return nil, invalid("ManualSnapshotRetentionPeriod must be -1 or between 1 and 3653 days.")
	case in.SnapshotCopyGrantName != "" && s.st.Grants[key(c, in.SnapshotCopyGrantName)] == nil:
		return nil, fault("SnapshotCopyGrantNotFoundFault", 404, "Snapshot copy grant %s not found.", in.SnapshotCopyGrantName)
	}
	cl.ClusterSnapshotCopyStatus = &CopyStatus{in.DestinationRegion, days, manual, in.SnapshotCopyGrantName}
	return map[string]any{"Cluster": view(cl)}, nil
}

func (s *Service) disableSnapshotCopy(c *core.Ctx, in *struct{ ClusterIdentifier string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	if cl.ClusterSnapshotCopyStatus == nil {
		return nil, fault("SnapshotCopyAlreadyDisabledFault", 400, "Snapshot copy is already disabled on cluster %s.", cl.ClusterIdentifier)
	}
	cl.ClusterSnapshotCopyStatus = nil
	return map[string]any{"Cluster": view(cl)}, nil
}

func (s *Service) modifyCopyRetention(c *core.Ctx, in *struct {
	ClusterIdentifier string
	RetentionPeriod   int
	Manual            bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	st := cl.ClusterSnapshotCopyStatus
	switch {
	case st == nil:
		return nil, fault("SnapshotCopyDisabledFault", 400, "Snapshot copy is disabled on cluster %s.", cl.ClusterIdentifier)
	case in.Manual && (in.RetentionPeriod < -1 || in.RetentionPeriod == 0 || in.RetentionPeriod > 3653):
		return nil, invalid("RetentionPeriod must be -1 or between 1 and 3653 days for manual snapshots.")
	case !in.Manual && (in.RetentionPeriod < 1 || in.RetentionPeriod > 35):
		return nil, invalid("RetentionPeriod must be between 1 and 35 days.")
	case in.Manual:
		st.ManualSnapshotRetentionPeriod = in.RetentionPeriod
	default:
		st.RetentionPeriod = in.RetentionPeriod
	}
	return map[string]any{"Cluster": view(cl)}, nil
}

// ---- snapshot copy grants ----

func (s *Service) createCopyGrant(c *core.Ctx, in *struct {
	SnapshotCopyGrantName, KmsKeyId string
	Tags                            []rds.Tag
}) (any, error) {
	name := strings.ToLower(in.SnapshotCopyGrantName)
	if err := rds.ValidID("SnapshotCopyGrantName", name, 63); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Grants[key(c, name)] != nil {
		return nil, fault("SnapshotCopyGrantAlreadyExistsFault", 400, "Snapshot copy grant %s already exists.", name)
	}
	g := &CopyGrant{SnapshotCopyGrantName: name, KmsKeyId: cmp.Or(in.KmsKeyId, core.ARN("kms", c.Region, c.Account, "alias/aws/redshift")), Tags: in.Tags}
	s.st.Grants[key(c, name)] = g
	return map[string]any{"SnapshotCopyGrant": rds.Clone(g)}, nil
}

func (s *Service) describeCopyGrants(c *core.Ctx, in *struct {
	SnapshotCopyGrantName, Marker string
	MaxRecords                    int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*CopyGrant
	for k, g := range s.st.Grants {
		if mine(c, k) && (in.SnapshotCopyGrantName == "" || g.SnapshotCopyGrantName == strings.ToLower(in.SnapshotCopyGrantName)) {
			list = append(list, g)
		}
	}
	if in.SnapshotCopyGrantName != "" && len(list) == 0 {
		return nil, fault("SnapshotCopyGrantNotFoundFault", 404, "Snapshot copy grant %s not found.", in.SnapshotCopyGrantName)
	}
	list, marker, err := page(list, func(g *CopyGrant) string { return g.SnapshotCopyGrantName }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"SnapshotCopyGrants": each(list, rds.Clone[CopyGrant]), "Marker": marker}, nil
}

func (s *Service) deleteCopyGrant(c *core.Ctx, in *struct{ SnapshotCopyGrantName string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	k := key(c, in.SnapshotCopyGrantName)
	if s.st.Grants[k] == nil {
		return nil, fault("SnapshotCopyGrantNotFoundFault", 404, "Snapshot copy grant %s not found.", in.SnapshotCopyGrantName)
	}
	for _, cl := range s.st.Clusters {
		if st := cl.ClusterSnapshotCopyStatus; st != nil && strings.EqualFold(st.SnapshotCopyGrantName, in.SnapshotCopyGrantName) {
			return nil, fault("InvalidSnapshotCopyGrantStateFault", 400, "Snapshot copy grant %s is in use by cluster %s.", in.SnapshotCopyGrantName, cl.ClusterIdentifier)
		}
	}
	delete(s.st.Grants, k)
	return nil, nil
}

// ---- snapshot schedules ----

var scheduleDef = regexp.MustCompile(`^(rate\(\d+ (minute|minutes|hour|hours|day|days)\)|cron\(.+\))$`)

func checkDefinitions(defs []string) error {
	if len(defs) == 0 {
		return fault("InvalidScheduleFault", 400, "At least one schedule definition is required.")
	}
	for _, d := range defs {
		if !scheduleDef.MatchString(d) {
			return fault("InvalidScheduleFault", 400, "The schedule %q is not a rate() or cron() expression.", d)
		}
	}
	return nil
}

// schedule finds a schedule; lock held.
func (s *Service) schedule(c *core.Ctx, id string) (*Schedule, error) {
	if sc := s.st.Schedules[key(c, id)]; sc != nil {
		return sc, nil
	}
	return nil, fault("SnapshotScheduleNotFoundFault", 404, "Snapshot schedule %s not found.", id)
}

// scheduleView is a schedule with its clusters, worked out from the clusters; lock held.
func (s *Service) scheduleView(c *core.Ctx, sc *Schedule) *Schedule {
	v := rds.Clone(sc)
	v.AssociatedClusters = nil
	for k, cl := range s.st.Clusters {
		if mine(c, k) && strings.EqualFold(cl.ScheduleIdentifier, sc.ScheduleIdentifier) {
			v.AssociatedClusters = append(v.AssociatedClusters, ClusterAssociation{cl.ClusterIdentifier, "ACTIVE"})
		}
	}
	slices.SortFunc(v.AssociatedClusters, func(a, b ClusterAssociation) int { return strings.Compare(a.ClusterIdentifier, b.ClusterIdentifier) })
	v.AssociatedClusterCount = len(v.AssociatedClusters)
	return v
}

func (s *Service) createSchedule(c *core.Ctx, in *struct {
	ScheduleIdentifier, ScheduleDescription string
	ScheduleDefinitions                     []string
	Tags                                    []rds.Tag
}) (any, error) {
	if err := checkDefinitions(in.ScheduleDefinitions); err != nil {
		return nil, err
	}
	if in.ScheduleIdentifier == "" {
		return nil, invalid("ScheduleIdentifier is required.")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.st.Schedules[key(c, in.ScheduleIdentifier)] != nil {
		return nil, fault("SnapshotScheduleAlreadyExistsFault", 400, "Snapshot schedule %s already exists.", in.ScheduleIdentifier)
	}
	sc := &Schedule{ScheduleDefinitions: in.ScheduleDefinitions, ScheduleIdentifier: in.ScheduleIdentifier, ScheduleDescription: in.ScheduleDescription, Tags: in.Tags}
	s.st.Schedules[key(c, in.ScheduleIdentifier)] = sc
	return s.scheduleView(c, sc), nil
}

func (s *Service) modifySchedule(c *core.Ctx, in *struct {
	ScheduleIdentifier  string
	ScheduleDefinitions []string
}) (any, error) {
	if err := checkDefinitions(in.ScheduleDefinitions); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.schedule(c, in.ScheduleIdentifier)
	if err != nil {
		return nil, err
	}
	sc.ScheduleDefinitions = in.ScheduleDefinitions
	return s.scheduleView(c, sc), nil
}

func (s *Service) describeSchedules(c *core.Ctx, in *struct {
	ClusterIdentifier, ScheduleIdentifier, Marker string
	MaxRecords                                    int
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var list []*Schedule
	for k, sc := range s.st.Schedules {
		if !mine(c, k) || in.ScheduleIdentifier != "" && !strings.EqualFold(sc.ScheduleIdentifier, in.ScheduleIdentifier) {
			continue
		}
		v := s.scheduleView(c, sc)
		if in.ClusterIdentifier != "" && !slices.ContainsFunc(v.AssociatedClusters, func(a ClusterAssociation) bool { return strings.EqualFold(a.ClusterIdentifier, in.ClusterIdentifier) }) {
			continue
		}
		list = append(list, v)
	}
	if in.ScheduleIdentifier != "" && len(list) == 0 {
		return nil, fault("SnapshotScheduleNotFoundFault", 404, "Snapshot schedule %s not found.", in.ScheduleIdentifier)
	}
	list, marker, err := page(list, func(sc *Schedule) string { return sc.ScheduleIdentifier }, in.Marker, in.MaxRecords)
	if err != nil {
		return nil, err
	}
	return map[string]any{"SnapshotSchedules": list, "Marker": marker}, nil
}

func (s *Service) deleteSchedule(c *core.Ctx, in *struct{ ScheduleIdentifier string }) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sc, err := s.schedule(c, in.ScheduleIdentifier)
	if err != nil {
		return nil, err
	}
	if s.scheduleView(c, sc).AssociatedClusterCount > 0 {
		return nil, fault("SnapshotScheduleActiveFault", 400, "Snapshot schedule %s is set on a cluster.", in.ScheduleIdentifier)
	}
	delete(s.st.Schedules, key(c, in.ScheduleIdentifier))
	return nil, nil
}

func (s *Service) modifyClusterSchedule(c *core.Ctx, in *struct {
	ClusterIdentifier, ScheduleIdentifier string
	DisassociateSchedule                  bool
}) (any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cl, err := s.cluster(c, in.ClusterIdentifier)
	if err != nil {
		return nil, err
	}
	if in.DisassociateSchedule {
		cl.ScheduleIdentifier = ""
		return nil, nil
	}
	sc, err := s.schedule(c, in.ScheduleIdentifier)
	if err != nil {
		return nil, err
	}
	cl.ScheduleIdentifier = sc.ScheduleIdentifier
	return nil, nil
}
