package generic

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/nrndev/pogi/internal/core"
)

// A logically air-gapped vault is a backup vault with a vault type, so it is stored as one:
// DescribeBackupVault, ListBackupVaults and DeleteBackupVault then find it.
func init() {
	Override("backup", "CreateLogicallyAirGappedBackupVault", func(c *Call) (map[string]any, error) {
		op := *c.op // a copy, so that the shared operation keeps its own type
		op.typ = c.m.types[norm("BackupVault")]
		c.op = &op
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		s2Set(c, out, map[string]any{"VaultType": "LOGICALLY_AIR_GAPPED_BACKUP_VAULT", "VaultState": "AVAILABLE"})
		return out, nil
	})
}

// Backup jobs. StartBackupJob stores a job; a background tick moves it to
// RUNNING and then COMPLETED, makes the recovery point, and tells the vault's
// SNS topic. Jobs and recovery points are resources of the service, so they
// are saved with it and a job that was not done when pogi stopped finishes
// after the restart.

const bkJob, bkPoint = "BackupJob", "RecoveryPoint"

var bkTerminal = []string{"COMPLETED", "FAILED", "ABORTED", "EXPIRED"}

// bkResourceTypes maps the service part of a resource ARN to AWS Backup's resource type name.
var bkResourceTypes = map[string]string{"dynamodb": "DynamoDB", "s3": "S3", "rds": "RDS", "ec2": "EC2", "elasticfilesystem": "EFS",
	"fsx": "FSx", "storagegateway": "Storage Gateway", "docdb": "DocumentDB", "neptune": "Neptune", "redshift": "Redshift"}

func bkVault(s *Service, account, region, name string) *Resource {
	return s.st.Res[skey(account, region, "BackupVault", name)]
}

// bkPoints counts the recovery points of a vault; lock not held.
func bkPoints(s *Service, account, region, vault string) int {
	n := 0
	for _, r := range s.List(account, region, bkPoint) {
		if r.Data["BackupVaultName"] == vault {
			n++
		}
	}
	return n
}

func bkJobOut(r *Resource) map[string]any {
	out := map[string]any{}
	for k, v := range r.Data {
		if k[0] != '_' {
			out[k] = v
		}
	}
	return out
}

func bkFail(c *Call, format string, a ...any) error {
	return c.Err("ResourceNotFoundException", format, a...)
}

func init() {
	Override("backup", "StartBackupJob", func(c *Call) (map[string]any, error) {
		vault, arn := str(c.In["BackupVaultName"]), str(c.In["ResourceArn"])
		if c.Get("BackupVault", vault) == nil {
			return nil, bkFail(c, "Backup vault with name %s not found.", vault)
		}
		_, _, _, res, ok := core.ParseARN(arn)
		if !ok || res == "" {
			return nil, c.Err("InvalidParameterValueException", "Invalid resource ARN: %s", arn)
		}
		svc, _, _, _, _ := core.ParseARN(arn)
		id := strings.ToUpper(strings.ReplaceAll(core.UUID(), "-", ""))
		now := time.Now().UTC().Format(time.RFC3339Nano)
		v := c.Get("BackupVault", vault)
		d := map[string]any{"BackupJobId": id, "BackupVaultName": vault, "BackupVaultArn": v.ARN, "ResourceArn": arn,
			"ResourceType": cmp.Or(bkResourceTypes[svc], svc), "ResourceName": res[strings.LastIndexAny(res, "/:")+1:],
			"IamRoleArn": c.In["IamRoleArn"], "AccountId": c.Account, "State": "CREATED", "CreationDate": now, "PercentDone": "0.0",
			"BackupType": "FULL", "IsParent": false}
		if l, ok := c.In["Lifecycle"]; ok {
			d["_lifecycle"] = l
		}
		if t, ok := c.In["RecoveryPointTags"]; ok {
			d["_tags"] = t
		}
		s2Store(c, bkJob, id, id, "", d, "BackupJobId")
		return map[string]any{"BackupJobId": id, "CreationDate": now, "IsParent": false}, nil
	})
	Override("backup", "DescribeBackupJob", func(c *Call) (map[string]any, error) {
		r := c.Get(bkJob, str(c.In["BackupJobId"]))
		if r == nil {
			return nil, bkFail(c, "Backup job with ID %s not found.", str(c.In["BackupJobId"]))
		}
		return bkJobOut(r), nil
	})
	Override("backup", "StopBackupJob", func(c *Call) (map[string]any, error) {
		r := c.Get(bkJob, str(c.In["BackupJobId"]))
		if r == nil {
			return nil, bkFail(c, "Backup job with ID %s not found.", str(c.In["BackupJobId"]))
		}
		if st := str(r.Data["State"]); slices.Contains(bkTerminal, st) {
			return nil, c.Err("InvalidRequestException", "Cannot stop a backup job in state %s", st)
		}
		r.Data["State"], r.Data["CompletionDate"] = "ABORTED", time.Now().UTC().Format(time.RFC3339Nano)
		c.Put(r)
		return map[string]any{}, nil
	})
	Override("backup", "ListBackupJobs", func(c *Call) (map[string]any, error) {
		var rows []any
		for _, r := range c.List(bkJob) {
			ok := true
			for in, member := range map[string]string{"ByBackupVaultName": "BackupVaultName", "ByState": "State", "ByResourceArn": "ResourceArn", "ByResourceType": "ResourceType", "ByAccountId": "AccountId"} {
				ok = ok && (str(c.In[in]) == "" || str(c.In[in]) == str(r.Data[member]))
			}
			if ok {
				rows = append(rows, bkJobOut(r))
			}
		}
		page, next := costPageOf(rows, c.In["NextToken"], c.In["MaxResults"], 1000)
		return map[string]any{"BackupJobs": page, "NextToken": next}, nil
	})

	Override("backup", "DescribeRecoveryPoint", func(c *Call) (map[string]any, error) {
		r := bkFindPoint(c)
		if r == nil {
			return nil, bkFail(c, "Recovery point with ARN %s not found.", str(c.In["RecoveryPointArn"]))
		}
		return bkJobOut(r), nil
	})
	Override("backup", "ListRecoveryPointsByBackupVault", func(c *Call) (map[string]any, error) {
		var rows []any
		for _, r := range c.List(bkPoint) {
			if r.Data["BackupVaultName"] == c.In["BackupVaultName"] && (str(c.In["ByResourceArn"]) == "" || c.In["ByResourceArn"] == r.Data["ResourceArn"]) {
				rows = append(rows, bkJobOut(r))
			}
		}
		page, next := costPageOf(rows, c.In["NextToken"], c.In["MaxResults"], 1000)
		return map[string]any{"RecoveryPoints": page, "NextToken": next}, nil
	})
	Override("backup", "DeleteRecoveryPoint", func(c *Call) (map[string]any, error) {
		r := bkFindPoint(c)
		if r == nil {
			return nil, bkFail(c, "Recovery point with ARN %s not found.", str(c.In["RecoveryPointArn"]))
		}
		c.Svc.Delete(c.Account, c.Region, bkPoint, r.Key)
		return map[string]any{}, nil
	})

	// A vault shows its recovery points and cannot be deleted while it has any.
	s2After("backup", "DescribeBackupVault", func(c *Call, out map[string]any) error {
		out["NumberOfRecoveryPoints"] = float64(bkPoints(c.Svc, c.Account, c.Region, str(c.In["BackupVaultName"])))
		return nil
	})
	s2After("backup", "ListBackupVaults", func(c *Call, out map[string]any) error {
		for _, v := range asList(out["BackupVaultList"]) {
			if m, ok := v.(map[string]any); ok {
				m["NumberOfRecoveryPoints"] = float64(bkPoints(c.Svc, c.Account, c.Region, str(m["BackupVaultName"])))
			}
		}
		return nil
	})
	Override("backup", "DeleteBackupVault", func(c *Call) (map[string]any, error) {
		if bkPoints(c.Svc, c.Account, c.Region, str(c.In["BackupVaultName"])) > 0 {
			return nil, c.Err("InvalidRequestException", "Backup vault cannot be deleted because it contains recovery points.")
		}
		return c.Default()
	})

	starters["backup"] = append(starters["backup"], func(s *Service) { s.Every("jobs", 500*time.Millisecond, s.backupJobs) })
}

func bkFindPoint(c *Call) *Resource {
	for _, r := range c.List(bkPoint) {
		if r.Data["RecoveryPointArn"] == c.In["RecoveryPointArn"] && r.Data["BackupVaultName"] == c.In["BackupVaultName"] {
			return r
		}
	}
	return nil
}

// backupJobs moves the jobs of every account and region along. A job is
// RUNNING from the first tick and COMPLETED once the lifecycle delay has passed.
func (s *Service) backupJobs(now time.Time) {
	type note struct{ topic, subject, body string }
	var notes []note
	s.mu.Lock()
	for _, k := range core.SortedKeys(s.st.Res) {
		r := s.st.Res[k]
		if r.Type != bkJob || slices.Contains(bkTerminal, str(r.Data["State"])) {
			continue
		}
		if str(r.Data["State"]) == "CREATED" {
			r.Data["State"], r.Data["PercentDone"] = "RUNNING", "50.0"
		}
		if d := time.Duration(lifecycleDelay.Load()); now.Sub(r.Created) < d {
			continue
		}
		vault := bkVault(s, r.Account, r.Region, str(r.Data["BackupVaultName"]))
		if vault == nil { // the vault went while the job ran
			r.Data["State"], r.Data["StatusMessage"], r.Data["CompletionDate"] = "FAILED", "Backup vault not found", now.Format(time.RFC3339Nano)
			continue
		}
		id := strings.ToLower(core.UUID())
		arn := core.ARN("backup", r.Region, r.Account, "recovery-point:"+id)
		done := now.Format(time.RFC3339Nano)
		rp := map[string]any{"RecoveryPointArn": arn, "BackupVaultName": r.Data["BackupVaultName"], "BackupVaultArn": r.Data["BackupVaultArn"],
			"ResourceArn": r.Data["ResourceArn"], "ResourceType": r.Data["ResourceType"], "ResourceName": r.Data["ResourceName"],
			"IamRoleArn": r.Data["IamRoleArn"], "Status": "COMPLETED", "CreationDate": r.Data["CreationDate"], "CompletionDate": done,
			"BackupSizeInBytes": float64(0), "IsEncrypted": false}
		if l := r.Data["_lifecycle"]; l != nil {
			rp["Lifecycle"] = l
		}
		s.st.Res[skey(r.Account, r.Region, bkPoint, id)] = &Resource{Type: bkPoint, Key: id, IDs: map[string]string{"RecoveryPointArn": id}, ARN: arn,
			Account: r.Account, Region: r.Region, Data: rp, Created: now, Updated: now}
		r.Data["State"], r.Data["PercentDone"], r.Data["RecoveryPointArn"], r.Data["CompletionDate"] = "COMPLETED", "100.0", arn, done
		r.Data["BackupSizeInBytes"], r.Data["BytesTransferred"] = float64(0), float64(0)
		r.Updated = now
		if n := s.st.Res[skey(r.Account, r.Region, "BackupVaultNotification", str(r.Data["BackupVaultName"]))]; n != nil && n.Data["SNSTopicArn"] != nil {
			events := asList(n.Data["BackupVaultEvents"])
			if slices.Contains(events, any("BACKUP_JOB_COMPLETED")) || slices.Contains(events, any("BACKUP_JOB_SUCCESSFUL")) {
				notes = append(notes, note{str(n.Data["SNSTopicArn"]), "Notification from AWS Backup",
					fmt.Sprintf("An AWS Backup job was completed successfully. Recovery point ARN: %s. Resource ARN : %s. BackupJob ID : %s", arn, r.Data["ResourceArn"], r.Key)})
			}
		}
	}
	s.mu.Unlock()
	if s.h == nil {
		return
	}
	for _, n := range notes {
		s.h.Deliver(n.topic, &core.Message{Subject: n.subject, Body: n.body, From: "backup"})
	}
}
