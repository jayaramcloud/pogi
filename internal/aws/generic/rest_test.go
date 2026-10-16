package generic_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/backup"
	backuptypes "github.com/aws/aws-sdk-go-v2/service/backup/types"
	"github.com/aws/aws-sdk-go-v2/service/dlm"
	dlmtypes "github.com/aws/aws-sdk-go-v2/service/dlm/types"
	"github.com/aws/aws-sdk-go-v2/service/efs"
	efstypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/aws/aws-sdk-go-v2/service/guardduty"
	gdtypes "github.com/aws/aws-sdk-go-v2/service/guardduty/types"
	"github.com/nrndev/pogi/internal/aws/generic"
	"github.com/nrndev/pogi/internal/awstest"
	"github.com/nrndev/pogi/internal/core"
)

// REST-JSON with query-string lists, a nested resource (access points belong to a file system) and tags in the path
func TestEFS(t *testing.T) {
	_, url := start(t, "efs")
	ctx := context.Background()
	c := efs.NewFromConfig(awstest.Config(t, url))
	var ids []string
	for i := range 3 {
		out, err := c.CreateFileSystem(ctx, &efs.CreateFileSystemInput{CreationToken: aws.String(fmt.Sprintf("tok%d", i)),
			Tags: []efstypes.Tag{{Key: aws.String("Name"), Value: aws.String(fmt.Sprintf("fs%d", i))}}, Encrypted: aws.Bool(true)})
		if err != nil {
			t.Fatal(err)
		}
		if out.LifeCycleState != efstypes.LifeCycleStateAvailable || aws.ToString(out.FileSystemId) == "" || aws.ToString(out.FileSystemArn) == "" ||
			out.SizeInBytes == nil || aws.ToString(out.CreationToken) != fmt.Sprintf("tok%d", i) || aws.ToString(out.OwnerId) != "000000000000" {
			t.Fatalf("%+v", out)
		}
		ids = append(ids, aws.ToString(out.FileSystemId))
	}
	d, err := c.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{FileSystemId: aws.String(ids[1])})
	if err != nil || len(d.FileSystems) != 1 || aws.ToString(d.FileSystems[0].FileSystemId) != ids[1] || !aws.ToBool(d.FileSystems[0].Encrypted) ||
		len(d.FileSystems[0].Tags) != 1 {
		t.Fatalf("%v %+v", err, d)
	}
	pages, n := 0, 0
	p := efs.NewDescribeFileSystemsPaginator(c, &efs.DescribeFileSystemsInput{MaxItems: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.FileSystems)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	// nested: an access point belongs to its file system
	ap, err := c.CreateAccessPoint(ctx, &efs.CreateAccessPointInput{FileSystemId: aws.String(ids[0]), ClientToken: aws.String("ap1")})
	if err != nil || aws.ToString(ap.FileSystemId) != ids[0] {
		t.Fatalf("%v %+v", err, ap)
	}
	if _, err := c.CreateAccessPoint(ctx, &efs.CreateAccessPointInput{FileSystemId: aws.String(ids[1]), ClientToken: aws.String("ap2")}); err != nil {
		t.Fatal(err)
	}
	aps, err := c.DescribeAccessPoints(ctx, &efs.DescribeAccessPointsInput{FileSystemId: aws.String(ids[0])})
	if err != nil || len(aps.AccessPoints) != 1 {
		t.Fatalf("%v %+v", err, aps)
	}
	if _, err := c.UntagResource(ctx, &efs.UntagResourceInput{ResourceId: aws.String(ids[1]), TagKeys: []string{"Name"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.TagResource(ctx, &efs.TagResourceInput{ResourceId: aws.String(ids[1]), Tags: []efstypes.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &efs.ListTagsForResourceInput{ResourceId: aws.String(ids[1])})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "env" {
		t.Fatalf("%v %+v", err, tags)
	}
	if _, err := c.DeleteFileSystem(ctx, &efs.DeleteFileSystemInput{FileSystemId: aws.String(ids[1])}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeFileSystems(ctx, &efs.DescribeFileSystemsInput{FileSystemId: aws.String(ids[1])})
	var nf *efstypes.FileSystemNotFound
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

func TestBackup(t *testing.T) {
	_, url := start(t, "backup")
	ctx := context.Background()
	c := backup.NewFromConfig(awstest.Config(t, url))
	for i := range 3 {
		out, err := c.CreateBackupVault(ctx, &backup.CreateBackupVaultInput{BackupVaultName: aws.String(fmt.Sprintf("v%d", i)), BackupVaultTags: map[string]string{"env": "dev"}})
		if err != nil {
			t.Fatal(err)
		}
		if aws.ToString(out.BackupVaultArn) == "" || out.CreationDate == nil {
			t.Fatalf("%+v", out)
		}
	}
	d, err := c.DescribeBackupVault(ctx, &backup.DescribeBackupVaultInput{BackupVaultName: aws.String("v1")})
	if err != nil || aws.ToString(d.BackupVaultName) != "v1" {
		t.Fatalf("%v %+v", err, d)
	}
	arn := aws.ToString(d.BackupVaultArn)
	pages, n := 0, 0
	p := backup.NewListBackupVaultsPaginator(c, &backup.ListBackupVaultsInput{MaxResults: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.BackupVaultList)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	plan, err := c.CreateBackupPlan(ctx, &backup.CreateBackupPlanInput{BackupPlan: &backuptypes.BackupPlanInput{BackupPlanName: aws.String("daily"),
		Rules: []backuptypes.BackupRuleInput{{RuleName: aws.String("r"), TargetBackupVaultName: aws.String("v1"), ScheduleExpression: aws.String("cron(0 12 * * ? *)")}}}})
	if err != nil || aws.ToString(plan.BackupPlanId) == "" {
		t.Fatalf("%v %+v", err, plan)
	}
	gp, err := c.GetBackupPlan(ctx, &backup.GetBackupPlanInput{BackupPlanId: plan.BackupPlanId})
	if err != nil || aws.ToString(gp.BackupPlan.BackupPlanName) != "daily" || len(gp.BackupPlan.Rules) != 1 || aws.ToString(gp.BackupPlan.Rules[0].TargetBackupVaultName) != "v1" {
		t.Fatalf("%v %+v", err, gp)
	}
	if _, err := c.TagResource(ctx, &backup.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"a": "b"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &backup.UntagResourceInput{ResourceArn: aws.String(arn), TagKeyList: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTags(ctx, &backup.ListTagsInput{ResourceArn: aws.String(arn)})
	if err != nil || len(tags.Tags) != 1 || tags.Tags["a"] != "b" {
		t.Fatalf("%v %+v", err, tags)
	}
	if _, err := c.DeleteBackupVault(ctx, &backup.DeleteBackupVaultInput{BackupVaultName: aws.String("v1")}); err != nil {
		t.Fatal(err)
	}
	_, err = c.DescribeBackupVault(ctx, &backup.DescribeBackupVaultInput{BackupVaultName: aws.String("v1")})
	var nf *backuptypes.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

func TestDLM(t *testing.T) {
	_, url := start(t, "dlm")
	ctx := context.Background()
	c := dlm.NewFromConfig(awstest.Config(t, url))
	var ids []string
	for i := range 3 {
		out, err := c.CreateLifecyclePolicy(ctx, &dlm.CreateLifecyclePolicyInput{Description: aws.String(fmt.Sprintf("p%d", i)), ExecutionRoleArn: aws.String("arn:aws:iam::000000000000:role/dlm"),
			State: dlmtypes.SettablePolicyStateValuesEnabled, PolicyDetails: &dlmtypes.PolicyDetails{ResourceTypes: []dlmtypes.ResourceTypeValues{dlmtypes.ResourceTypeValuesVolume}},
			Tags: map[string]string{"env": "dev"}})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, aws.ToString(out.PolicyId))
	}
	g, err := c.GetLifecyclePolicy(ctx, &dlm.GetLifecyclePolicyInput{PolicyId: aws.String(ids[1])})
	if err != nil || aws.ToString(g.Policy.Description) != "p1" || len(g.Policy.PolicyDetails.ResourceTypes) != 1 || g.Policy.State != dlmtypes.GettablePolicyStateValuesEnabled {
		t.Fatalf("%v %+v", err, g.Policy)
	}
	arn := aws.ToString(g.Policy.PolicyArn)
	if arn == "" {
		t.Fatal("no policy arn")
	}
	l, err := c.GetLifecyclePolicies(ctx, &dlm.GetLifecyclePoliciesInput{})
	if err != nil || len(l.Policies) != 3 {
		t.Fatalf("%v %+v", err, l)
	}
	if _, err := c.UpdateLifecyclePolicy(ctx, &dlm.UpdateLifecyclePolicyInput{PolicyId: aws.String(ids[1]), Description: aws.String("new")}); err != nil {
		t.Fatal(err)
	}
	g, _ = c.GetLifecyclePolicy(ctx, &dlm.GetLifecyclePolicyInput{PolicyId: aws.String(ids[1])})
	if aws.ToString(g.Policy.Description) != "new" {
		t.Fatalf("%+v", g.Policy)
	}
	c.TagResource(ctx, &dlm.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"k": "v"}})
	c.UntagResource(ctx, &dlm.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"env"}})
	tags, err := c.ListTagsForResource(ctx, &dlm.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil || len(tags.Tags) != 1 || tags.Tags["k"] != "v" {
		t.Fatalf("%v %+v", err, tags)
	}
	if _, err := c.DeleteLifecyclePolicy(ctx, &dlm.DeleteLifecyclePolicyInput{PolicyId: aws.String(ids[1])}); err != nil {
		t.Fatal(err)
	}
	_, err = c.GetLifecyclePolicy(ctx, &dlm.GetLifecyclePolicyInput{PolicyId: aws.String(ids[1])})
	var nf *dlmtypes.ResourceNotFoundException
	if !errors.As(err, &nf) {
		t.Fatalf("%T %v", err, err)
	}
}

// nested: a filter belongs to a detector
func TestGuardDuty(t *testing.T) {
	_, url := start(t, "guardduty")
	ctx := context.Background()
	c := guardduty.NewFromConfig(awstest.Config(t, url))
	var dets []string
	for range 3 {
		out, err := c.CreateDetector(ctx, &guardduty.CreateDetectorInput{Enable: aws.Bool(true), FindingPublishingFrequency: gdtypes.FindingPublishingFrequencyFifteenMinutes, Tags: map[string]string{"env": "dev"}})
		if err != nil {
			t.Fatal(err)
		}
		dets = append(dets, aws.ToString(out.DetectorId))
	}
	g, err := c.GetDetector(ctx, &guardduty.GetDetectorInput{DetectorId: aws.String(dets[0])})
	if err != nil || g.Status != gdtypes.DetectorStatusEnabled || g.FindingPublishingFrequency != gdtypes.FindingPublishingFrequencyFifteenMinutes || g.Tags["env"] != "dev" {
		t.Fatalf("%v %+v", err, g)
	}
	pages, n := 0, 0
	p := guardduty.NewListDetectorsPaginator(c, &guardduty.ListDetectorsInput{MaxResults: aws.Int32(2)})
	for p.HasMorePages() {
		out, err := p.NextPage(ctx)
		if err != nil {
			t.Fatal(err)
		}
		pages++
		n += len(out.DetectorIds)
	}
	if pages != 2 || n != 3 {
		t.Fatalf("pages %d n %d", pages, n)
	}
	for _, d := range dets[:2] {
		if _, err := c.CreateFilter(ctx, &guardduty.CreateFilterInput{DetectorId: aws.String(d), Name: aws.String("noisy"), Action: gdtypes.FilterActionArchive, Rank: aws.Int32(1),
			FindingCriteria: &gdtypes.FindingCriteria{Criterion: map[string]gdtypes.Condition{"severity": {Gte: aws.Int32(4)}}}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.CreateFilter(ctx, &guardduty.CreateFilterInput{DetectorId: aws.String(dets[0]), Name: aws.String("noisy"), Action: gdtypes.FilterActionArchive,
		FindingCriteria: &gdtypes.FindingCriteria{}}); err == nil {
		t.Fatal("duplicate filter")
	}
	f, err := c.GetFilter(ctx, &guardduty.GetFilterInput{DetectorId: aws.String(dets[1]), FilterName: aws.String("noisy")})
	if err != nil || aws.ToString(f.Name) != "noisy" || f.Action != gdtypes.FilterActionArchive || aws.ToInt32(f.FindingCriteria.Criterion["severity"].Gte) != 4 {
		t.Fatalf("%v %+v", err, f)
	}
	l, err := c.ListFilters(ctx, &guardduty.ListFiltersInput{DetectorId: aws.String(dets[0])})
	if err != nil || len(l.FilterNames) != 1 || l.FilterNames[0] != "noisy" {
		t.Fatalf("%v %+v", err, l)
	}
	arn := fmt.Sprintf("arn:aws:guardduty:us-east-1:000000000000:detector/%s/filter/noisy", dets[0])
	if _, err := c.TagResource(ctx, &guardduty.TagResourceInput{ResourceArn: aws.String(arn), Tags: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &guardduty.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	if err != nil || tags.Tags["k"] != "v" {
		t.Fatalf("%v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &guardduty.UntagResourceInput{ResourceArn: aws.String(arn), TagKeys: []string{"k"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateDetector(ctx, &guardduty.UpdateDetectorInput{DetectorId: aws.String(dets[0]), Enable: aws.Bool(false)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteFilter(ctx, &guardduty.DeleteFilterInput{DetectorId: aws.String(dets[1]), FilterName: aws.String("noisy")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteDetector(ctx, &guardduty.DeleteDetectorInput{DetectorId: aws.String(dets[1])}); err != nil {
		t.Fatal(err)
	}
	// this model has no not-found error: an API error with a code
	_, err = c.GetDetector(ctx, &guardduty.GetDetectorInput{DetectorId: aws.String(dets[1])})
	var ae interface{ ErrorCode() string }
	if !errors.As(err, &ae) {
		t.Fatalf("%T %v", err, err)
	}
	t.Log(err)
}

// floci #4986: a logically air-gapped vault can be described, with its vault type
func TestBackupAirGappedVault(t *testing.T) {
	_, url := start(t, "backup")
	ctx := context.Background()
	c := backup.NewFromConfig(awstest.Config(t, url))
	out, err := c.CreateLogicallyAirGappedBackupVault(ctx, &backup.CreateLogicallyAirGappedBackupVaultInput{
		BackupVaultName: aws.String("av1"), MinRetentionDays: aws.Int64(7), MaxRetentionDays: aws.Int64(30)})
	if err != nil || out.VaultState == "" {
		t.Fatalf("%v %+v", err, out)
	}
	d, err := c.DescribeBackupVault(ctx, &backup.DescribeBackupVaultInput{BackupVaultName: aws.String("av1")})
	if err != nil || d.VaultType != backuptypes.VaultTypeLogicallyAirGappedBackupVault || !strings.HasSuffix(aws.ToString(d.BackupVaultArn), ":backup-vault:av1") {
		t.Fatalf("%v %+v", err, d)
	}
}

// floci #5458, #4817, #5250: a backup job completes into a recovery point, tells the vault's topic,
// works in any account, and one that was not finished when the service stopped finishes after a restart.
func TestBackupJobs(t *testing.T) {
	const topic = "arn:aws:sns:us-east-1:210987654321:notify"
	var told []string
	var svc *generic.Service
	build := func(h *core.Hub) core.Service {
		h.OnDeliver("sns", func(arn string, m *core.Message) error { told = append(told, arn+" "+m.Body); return nil })
		svc = generic.New(h, "backup")
		return svc
	}
	_, url := core.ServeTest(t, build)
	cfg := awstest.ConfigAs(t, url, "210987654321", "test", "")
	c := backup.NewFromConfig(cfg)
	ctx := context.Background()
	if _, err := c.StartBackupJob(ctx, &backup.StartBackupJobInput{BackupVaultName: aws.String("nope"), ResourceArn: aws.String("arn:aws:dynamodb:us-east-1:210987654321:table/t"), IamRoleArn: aws.String("arn:aws:iam::210987654321:role/r")}); !errors.As(err, new(*backuptypes.ResourceNotFoundException)) {
		t.Fatalf("job for a missing vault: %v", err)
	}
	if _, err := c.CreateBackupVault(ctx, &backup.CreateBackupVaultInput{BackupVaultName: aws.String("v1")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PutBackupVaultNotifications(ctx, &backup.PutBackupVaultNotificationsInput{BackupVaultName: aws.String("v1"), SNSTopicArn: aws.String(topic),
		BackupVaultEvents: []backuptypes.BackupVaultEvent{"BACKUP_JOB_COMPLETED"}}); err != nil {
		t.Fatal(err)
	}
	start := func() string {
		j, err := c.StartBackupJob(ctx, &backup.StartBackupJobInput{BackupVaultName: aws.String("v1"), ResourceArn: aws.String("arn:aws:dynamodb:us-east-1:210987654321:table/t"), IamRoleArn: aws.String("arn:aws:iam::210987654321:role/r")})
		if err != nil {
			t.Fatal(err)
		}
		return aws.ToString(j.BackupJobId)
	}
	state := func(id string) string {
		d, err := c.DescribeBackupJob(ctx, &backup.DescribeBackupJobInput{BackupJobId: aws.String(id)})
		if err != nil {
			t.Fatal(err)
		}
		return string(d.State)
	}

	defer generic.SetLifecycleDelay(generic.SetLifecycleDelay(time.Hour)) // jobs wait for the first tick
	id := start()
	svc.Tick(time.Now())
	if s := state(id); s != "RUNNING" {
		t.Fatalf("state before the delay = %s", s)
	}

	// Restart: save, make a new service, load.
	snap, _ := svc.Save()
	_, url = core.ServeTest(t, build)
	if err := svc.Load(snap); err != nil {
		t.Fatal(err)
	}
	c = backup.NewFromConfig(awstest.ConfigAs(t, url, "210987654321", "test", ""))
	generic.SetLifecycleDelay(0)
	svc.Tick(time.Now())
	if s := state(id); s != "COMPLETED" {
		t.Fatalf("state after the restart = %s", s)
	}
	d, _ := c.DescribeBackupJob(ctx, &backup.DescribeBackupJobInput{BackupJobId: aws.String(id)})
	rp, err := c.DescribeRecoveryPoint(ctx, &backup.DescribeRecoveryPointInput{BackupVaultName: aws.String("v1"), RecoveryPointArn: d.RecoveryPointArn})
	if err != nil || rp.Status != backuptypes.RecoveryPointStatusCompleted || aws.ToString(rp.ResourceType) != "DynamoDB" {
		t.Fatalf("recovery point: %v %+v", err, rp)
	}
	v, _ := c.DescribeBackupVault(ctx, &backup.DescribeBackupVaultInput{BackupVaultName: aws.String("v1")})
	if v.NumberOfRecoveryPoints != 1 {
		t.Fatalf("NumberOfRecoveryPoints = %d", v.NumberOfRecoveryPoints)
	}
	if l, err := c.ListRecoveryPointsByBackupVault(ctx, &backup.ListRecoveryPointsByBackupVaultInput{BackupVaultName: aws.String("v1")}); err != nil || len(l.RecoveryPoints) != 1 {
		t.Fatalf("list: %v %+v", err, l)
	}
	if l, err := c.ListBackupJobs(ctx, &backup.ListBackupJobsInput{ByState: "COMPLETED"}); err != nil || len(l.BackupJobs) != 1 {
		t.Fatalf("ListBackupJobs: %v %+v", err, l)
	}
	if len(told) != 1 || !strings.HasPrefix(told[0], topic) || !strings.Contains(told[0], id) {
		t.Fatalf("topic got %q", told)
	}
	if _, err := c.DeleteBackupVault(ctx, &backup.DeleteBackupVaultInput{BackupVaultName: aws.String("v1")}); err == nil {
		t.Fatal("deleted a vault that holds a recovery point")
	}
	if _, err := c.DeleteRecoveryPoint(ctx, &backup.DeleteRecoveryPointInput{BackupVaultName: aws.String("v1"), RecoveryPointArn: d.RecoveryPointArn}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteBackupVault(ctx, &backup.DeleteBackupVaultInput{BackupVaultName: aws.String("v1")}); err != nil {
		t.Fatalf("empty vault: %v", err)
	}
}
