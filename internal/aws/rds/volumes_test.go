package rds_test

import "testing"

// The data of a database lives in a named volume next to its container: a new resource starts from an
// empty volume (the old one goes first), and deleting the resource removes it.
func TestDataVolumeFollowsTheResource(t *testing.T) {
	f, _, base := setup(t, false)
	create(t, base, "vol1")
	await(t, base, "vol1", "available")
	const v = "pogi-rds-vol1-data"
	if !f.HasVolume(v) {
		t.Fatalf("no data volume after create: %v", f.Called("volume"))
	}
	if got := f.Called("volume"); len(got) < 2 || got[0] != "volume rm "+v || got[1] != "volume create "+v {
		t.Fatalf("a new database must start from an empty volume (remove the old, then create): %v", got)
	}
	if len(f.Called("bind pogi-rds-vol1 "+v+":/var/lib/postgresql/data")) != 1 {
		t.Fatalf("the container does not mount the volume: %v", f.Called("bind"))
	}
	ok(t, base, nil, "Action", "DeleteDBInstance", "DBInstanceIdentifier", "vol1", "SkipFinalSnapshot", "true")
	eventually(t, "the volume to go with the container", func() bool { return !f.HasVolume(v) })
}
