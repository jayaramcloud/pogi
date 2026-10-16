package cloudformation

import (
	"net/url"
	"strings"
)

// Glue, Athena, Systems Manager documents and associations, Backup and Managed Flink applications.

const (
	glueT   = "AWSGlue"
	athenaT = "AmazonAthena"
	ssmT    = "AmazonSSM"
	kaT     = "KinesisAnalytics_20180523"
)

// nameOf reads a member of a nested template object ("DatabaseInput", "Name").
func nameOf(p map[string]any, obj, member string) string {
	m, _ := p[obj].(map[string]any)
	return str(m[member])
}

func init() {
	// ---- Glue ----
	input := func(c *pctx, p map[string]any, obj string) string {
		return or(nameOf(p, obj, "Name"), c.name(255, true))
	}
	register("AWS::Glue::Database", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"DatabaseInput": map[string]any{}}
			for k, v := range p["DatabaseInput"].(map[string]any) {
				in["DatabaseInput"].(map[string]any)[k] = v
			}
			name := input(c, p, "DatabaseInput")
			in["DatabaseInput"].(map[string]any)["Name"] = name
			if p["DatabaseName"] != nil {
				in["DatabaseName"] = p["DatabaseName"]
			}
			if t := tagMap(p["Tags"]); len(t) > 0 {
				in["Tags"] = t
			}
			return strings.ToLower(name), nil, c.call(glueT, "CreateDatabase", in, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if !strings.EqualFold(nameOf(old, "DatabaseInput", "Name"), nameOf(p, "DatabaseInput", "Name")) {
				return nil, true, nil
			}
			return nil, false, c.call(glueT, "UpdateDatabase", map[string]any{"Name": phys, "DatabaseInput": p["DatabaseInput"]}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(glueT, "DeleteDatabase", map[string]any{"Name": phys}, nil)
		},
	})
	register("AWS::Glue::Table", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := input(c, p, "TableInput")
			ti := map[string]any{}
			for k, v := range p["TableInput"].(map[string]any) {
				ti[k] = v
			}
			ti["Name"] = name
			return strings.ToLower(name), nil, c.call(glueT, "CreateTable", map[string]any{"DatabaseName": p["DatabaseName"], "TableInput": ti}, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if !strings.EqualFold(str(old["DatabaseName"]), str(p["DatabaseName"])) || !strings.EqualFold(nameOf(old, "TableInput", "Name"), nameOf(p, "TableInput", "Name")) {
				return nil, true, nil
			}
			return nil, false, c.call(glueT, "UpdateTable", map[string]any{"DatabaseName": p["DatabaseName"], "TableInput": p["TableInput"]}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(glueT, "DeleteTable", map[string]any{"DatabaseName": p["DatabaseName"], "Name": phys}, nil)
		},
	})
	// the crawler's Schedule is an object in a template and an expression in the API
	crawler := func(c *pctx, p map[string]any, name string) map[string]any {
		in := map[string]any{}
		for k, v := range p {
			in[k] = v
		}
		if s, ok := p["Schedule"].(map[string]any); ok {
			in["Schedule"] = s["ScheduleExpression"]
		}
		in["Name"] = name
		return in
	}
	register("AWS::Glue::Crawler", simple(spec{target: glueT, create: "CreateCrawler", update: "UpdateCrawler", del: "DeleteCrawler", name: "Name", max: 255,
		inPlace: []string{"Role", "DatabaseName", "Targets", "Schedule", "SchemaChangePolicy", "Configuration", "TablePrefix", "Description", "Classifiers", "RecrawlPolicy", "Tags"},
		build:   func(c *pctx, p map[string]any, name string) map[string]any { return crawler(c, p, name) }}))
	register("AWS::Glue::Job", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(255, false))
			in := map[string]any{}
			for k, v := range p {
				in[k] = v
			}
			in["Name"] = name
			return name, nil, c.call(glueT, "CreateJob", in, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) {
				return nil, true, nil
			}
			up := map[string]any{}
			for k, v := range p {
				if k != "Name" && k != "Tags" {
					up[k] = v
				}
			}
			return nil, false, c.call(glueT, "UpdateJob", map[string]any{"JobName": phys, "JobUpdate": up}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(glueT, "DeleteJob", map[string]any{"JobName": phys}, nil)
		},
	})
	register("AWS::Glue::Connection", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := input(c, p, "ConnectionInput")
			ci := map[string]any{}
			for k, v := range p["ConnectionInput"].(map[string]any) {
				ci[k] = v
			}
			ci["Name"] = name
			return name, nil, c.call(glueT, "CreateConnection", map[string]any{"ConnectionInput": ci}, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if nameOf(old, "ConnectionInput", "Name") != nameOf(p, "ConnectionInput", "Name") {
				return nil, true, nil
			}
			ci := map[string]any{}
			for k, v := range p["ConnectionInput"].(map[string]any) {
				ci[k] = v
			}
			ci["Name"] = phys
			return nil, false, c.call(glueT, "UpdateConnection", map[string]any{"Name": phys, "ConnectionInput": ci}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(glueT, "DeleteConnection", map[string]any{"ConnectionName": phys}, nil)
		},
	})
	register("AWS::Glue::Trigger", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(255, false))
			in := map[string]any{}
			for k, v := range p {
				in[k] = v
			}
			in["Name"] = name
			return name, nil, c.call(glueT, "CreateTrigger", in, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) || differs(pick(old, "Type", "WorkflowName"), pick(p, "Type", "WorkflowName")) {
				return nil, true, nil
			}
			up := pick(p, "Description", "Schedule", "Actions", "Predicate", "EventBatchingCondition")
			return nil, false, c.call(glueT, "UpdateTrigger", map[string]any{"Name": phys, "TriggerUpdate": up}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(glueT, "DeleteTrigger", map[string]any{"Name": phys}, nil)
		},
	})
	register("AWS::Glue::Registry", simple(spec{target: glueT, create: "CreateRegistry", del: "DeleteRegistry", name: "Name", max: 255, key: "RegistryName",
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"Description": p["Description"], "Tags": tagMap(p["Tags"])}
		},
		id: func(_ string, out map[string]any) string { return dig(out, "RegistryArn") },
		delIn: func(phys string, _ map[string]any) map[string]any {
			return map[string]any{"RegistryId": map[string]any{"RegistryArn": phys}}
		},
		attrs: func(c *pctx, phys string, _, _ map[string]any) map[string]any { return map[string]any{"Arn": phys} }}))
	register("AWS::Glue::Schema", simple(spec{target: glueT, create: "CreateSchema", del: "DeleteSchema", name: "Name", max: 255, key: "SchemaName",
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			in := pick(p, "DataFormat", "Compatibility", "Description")
			if r, ok := p["Registry"].(map[string]any); ok {
				in["RegistryId"] = pick(r, "Name=RegistryName", "Arn=RegistryArn")
			}
			if s, ok := p["SchemaDefinition"]; ok {
				in["SchemaDefinition"] = s
			}
			in["Tags"] = tagMap(p["Tags"])
			return in
		},
		id: func(_ string, out map[string]any) string { return dig(out, "SchemaArn") },
		delIn: func(phys string, _ map[string]any) map[string]any {
			return map[string]any{"SchemaId": map[string]any{"SchemaArn": phys}}
		},
		attrs: func(c *pctx, phys string, _, out map[string]any) map[string]any {
			return map[string]any{"Arn": phys, "InitialSchemaVersionId": dig(out, "SchemaVersionId")}
		}}))

	// ---- Athena ----
	register("AWS::Athena::WorkGroup", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(128, false))
			in := map[string]any{"Name": name, "Description": p["Description"], "Configuration": p["WorkGroupConfiguration"], "Tags": tagList(p["Tags"])}
			return name, map[string]any{"WorkGroupConfiguration.EngineVersion.EffectiveEngineVersion": "Athena engine version 3"}, c.call(athenaT, "CreateWorkGroup", in, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if str(old["Name"]) != str(p["Name"]) {
				return nil, true, nil
			}
			in := map[string]any{"WorkGroup": phys, "Description": p["Description"], "State": p["State"]}
			if cfg, ok := p["WorkGroupConfiguration"].(map[string]any); ok {
				up := map[string]any{}
				for k, v := range cfg {
					if k == "ResultConfiguration" {
						rc, _ := v.(map[string]any)
						up["ResultConfigurationUpdates"] = pick(rc, "OutputLocation", "ExpectedBucketOwner", "EncryptionConfiguration")
						continue
					}
					up[k] = v
				}
				in["ConfigurationUpdates"] = up
			}
			return nil, false, c.call(athenaT, "UpdateWorkGroup", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(athenaT, "DeleteWorkGroup", map[string]any{"WorkGroup": phys, "RecursiveDeleteOption": true}, nil)
		},
	})
	register("AWS::Athena::NamedQuery", simple(spec{target: athenaT, create: "CreateNamedQuery", del: "DeleteNamedQuery", name: "Name", max: 128,
		id:    func(_ string, out map[string]any) string { return dig(out, "NamedQueryId") },
		delIn: func(phys string, _ map[string]any) map[string]any { return map[string]any{"NamedQueryId": phys} },
		attrs: func(c *pctx, phys string, _, _ map[string]any) map[string]any {
			return map[string]any{"NamedQueryId": phys}
		}}))
	register("AWS::Athena::DataCatalog", simple(spec{target: athenaT, create: "CreateDataCatalog", update: "UpdateDataCatalog", del: "DeleteDataCatalog", name: "Name", max: 127,
		inPlace: []string{"Type", "Description", "Parameters", "Tags"},
		build: func(c *pctx, p map[string]any, _ string) map[string]any {
			return map[string]any{"Type": p["Type"], "Description": p["Description"], "Parameters": p["Parameters"], "Tags": tagList(p["Tags"])}
		}}))
	register("AWS::Athena::PreparedStatement", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			return str(p["StatementName"]), nil, c.call(athenaT, "CreatePreparedStatement", pick(p, "StatementName", "WorkGroup", "QueryStatement", "Description"), nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(pick(old, "StatementName", "WorkGroup"), pick(p, "StatementName", "WorkGroup")) {
				return nil, true, nil
			}
			return nil, false, c.call(athenaT, "UpdatePreparedStatement", pick(p, "StatementName", "WorkGroup", "QueryStatement", "Description"), nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(athenaT, "DeletePreparedStatement", pick(p, "StatementName", "WorkGroup"), nil)
		},
	})

	// ---- Systems Manager ----
	register("AWS::SSM::Document", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["Name"]), c.name(128, false))
			in := pick(p, "DocumentType", "DocumentFormat", "TargetType", "VersionName", "Requires", "Attachments")
			in["Name"], in["Content"], in["Tags"] = name, jsonText(p["Content"]), tagList(p["Tags"])
			if s, ok := p["Content"].(string); ok {
				in["Content"] = s
			}
			return name, nil, c.call(ssmT, "CreateDocument", in, nil)
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			// the default update replaces the document; NewVersion adds a version of it
			if str(p["UpdateMethod"]) != "NewVersion" || str(old["Name"]) != str(p["Name"]) || differs(pick(old, "DocumentType"), pick(p, "DocumentType")) {
				return nil, true, nil
			}
			in := pick(p, "DocumentFormat", "TargetType", "VersionName")
			in["Name"], in["DocumentVersion"] = phys, "$LATEST"
			if s, ok := p["Content"].(string); ok {
				in["Content"] = s
			} else {
				in["Content"] = jsonText(p["Content"])
			}
			var out struct {
				DocumentDescription struct{ DocumentVersion string }
			}
			if err := c.call(ssmT, "UpdateDocument", in, &out); err != nil {
				return nil, false, err
			}
			return nil, false, c.call(ssmT, "UpdateDocumentDefaultVersion", map[string]any{"Name": phys, "DocumentVersion": out.DocumentDescription.DocumentVersion}, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(ssmT, "DeleteDocument", map[string]any{"Name": phys, "Force": true}, nil)
		},
	})
	assocKeys := []string{"Name", "AssociationName", "InstanceId", "Targets", "ScheduleExpression", "Parameters", "DocumentVersion", "OutputLocation", "MaxErrors", "MaxConcurrency",
		"ComplianceSeverity", "ApplyOnlyAtCronInterval", "WaitForSuccessTimeoutSeconds", "CalendarNames", "AutomationTargetParameterName", "SyncCompliance"}
	register("AWS::SSM::Association", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct {
				AssociationDescription struct{ AssociationId string }
			}
			if err := c.call(ssmT, "CreateAssociation", pick(p, assocKeys...), &out); err != nil {
				return "", nil, err
			}
			id := out.AssociationDescription.AssociationId
			return id, map[string]any{"AssociationId": id}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			if differs(pick(old, "Name", "InstanceId"), pick(p, "Name", "InstanceId")) {
				return nil, true, nil
			}
			in := pick(p, assocKeys...)
			delete(in, "InstanceId")
			in["AssociationId"] = phys
			return nil, false, c.call(ssmT, "UpdateAssociation", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.call(ssmT, "DeleteAssociation", map[string]any{"AssociationId": phys}, nil)
		},
	})

	// ---- Backup (REST) ----
	register("AWS::Backup::BackupVault", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["BackupVaultName"]), c.name(50, false))
			in := pick(p, "EncryptionKeyArn=EncryptionKeyArn")
			if t := tagMap(p["BackupVaultTags"]); len(t) > 0 {
				in["BackupVaultTags"] = t
			}
			var out struct{ BackupVaultArn string }
			path := "/backup-vaults/" + url.PathEscape(name)
			if err := c.rest("backup", "PUT", path, in, &out); err != nil {
				return "", nil, err
			}
			if pol := p["AccessPolicy"]; pol != nil {
				c.rest("backup", "PUT", path+"/access-policy", map[string]any{"Policy": jsonString(pol)}, nil)
			}
			if n, ok := p["Notifications"].(map[string]any); ok {
				c.rest("backup", "PUT", path+"/notification-configuration", map[string]any{"SNSTopicArn": n["SNSTopicArn"], "BackupVaultEvents": n["BackupVaultEvents"]}, nil)
			}
			return name, map[string]any{"BackupVaultArn": out.BackupVaultArn, "BackupVaultName": name}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("backup", "DELETE", "/backup-vaults/"+url.PathEscape(phys), nil, nil)
		},
	})
	plan := func(p map[string]any) map[string]any {
		bp, _ := p["BackupPlan"].(map[string]any)
		out := map[string]any{"BackupPlanName": bp["BackupPlanName"]}
		var rules []any
		for _, r := range listMaps(bp["BackupPlanRule"]) {
			rule := map[string]any{}
			for k, v := range r {
				if k == "TargetBackupVault" {
					k = "TargetBackupVaultName"
				}
				rule[k] = v
			}
			rules = append(rules, rule)
		}
		out["Rules"] = rules
		return out
	}
	register("AWS::Backup::BackupPlan", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			in := map[string]any{"BackupPlan": plan(p)}
			if t := tagMap(p["BackupPlanTags"]); len(t) > 0 {
				in["BackupPlanTags"] = t
			}
			var out struct{ BackupPlanId, BackupPlanArn, VersionId string }
			if err := c.rest("backup", "PUT", "/backup/plans/", in, &out); err != nil {
				return "", nil, err
			}
			return out.BackupPlanId, map[string]any{"BackupPlanId": out.BackupPlanId, "BackupPlanArn": out.BackupPlanArn, "VersionId": out.VersionId}, nil
		},
		update: func(c *pctx, phys string, old, p map[string]any) (map[string]any, bool, error) {
			var out struct{ BackupPlanId, BackupPlanArn, VersionId string }
			err := c.rest("backup", "POST", "/backup/plans/"+phys, map[string]any{"BackupPlan": plan(p)}, &out)
			return map[string]any{"BackupPlanId": phys, "BackupPlanArn": out.BackupPlanArn, "VersionId": out.VersionId}, false, err
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("backup", "DELETE", "/backup/plans/"+phys, nil, nil)
		},
	})
	register("AWS::Backup::BackupSelection", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			var out struct{ SelectionId string }
			if err := c.rest("backup", "PUT", "/backup/plans/"+str(p["BackupPlanId"])+"/selections/", map[string]any{"BackupSelection": p["BackupSelection"]}, &out); err != nil {
				return "", nil, err
			}
			return out.SelectionId, map[string]any{"SelectionId": out.SelectionId, "BackupPlanId": p["BackupPlanId"]}, nil
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			return c.rest("backup", "DELETE", "/backup/plans/"+str(p["BackupPlanId"])+"/selections/"+phys, nil, nil)
		},
	})

	// ---- Managed Service for Apache Flink ----
	register("AWS::KinesisAnalyticsV2::Application", &provider{
		create: func(c *pctx, p map[string]any) (string, map[string]any, error) {
			name := or(str(p["ApplicationName"]), c.name(128, false))
			in := pick(p, "RuntimeEnvironment", "ServiceExecutionRole", "ApplicationConfiguration", "ApplicationDescription", "ApplicationMode")
			in["ApplicationName"], in["Tags"] = name, tagList(p["Tags"])
			return name, nil, c.call(kaT, "CreateApplication", in, nil)
		},
		del: func(c *pctx, phys string, p map[string]any) error {
			var out struct{ ApplicationDetail struct{ CreateTimestamp any } }
			if err := c.call(kaT, "DescribeApplication", map[string]any{"ApplicationName": phys}, &out); err != nil {
				return err
			}
			return c.call(kaT, "DeleteApplication", map[string]any{"ApplicationName": phys, "CreateTimestamp": out.ApplicationDetail.CreateTimestamp}, nil)
		},
	})
}
