package generic

import (
	"slices"
	"strings"
	"time"
)

// AWS Config: recorders, delivery channels, rules and evaluations keep the behaviour clients rely on.
// The recorder and the channel are structures inside the Put call (so the engine cannot name them),
// recording has a status, and evaluations round-trip from PutEvaluations to GetComplianceDetailsByConfigRule.
//
// ponytail: a conformance pack is stored, but its template is not expanded into rules;
// a custom rule is never invoked, evaluations come from PutEvaluations/PutExternalEvaluation
// (the result token is the rule's name or ARN).
func init() {
	Override("config", "PutConfigurationRecorder", func(c *Call) (map[string]any, error) {
		rec := s2Clone(s2Map(c.In["ConfigurationRecorder"]))
		name := str(rec["name"])
		if name == "" {
			name = "default"
			rec["name"] = name
		}
		if others := c.List("ConfigurationRecorder"); len(others) > 0 && others[0].Name != name {
			return nil, s2Err("MaxNumberOfConfigurationRecordersExceededException", "Failed to put configuration recorder '%s' because the maximum number of configuration recorders: 1 is reached.", name)
		}
		if rec["recordingGroup"] == nil {
			rec["recordingGroup"] = map[string]any{"allSupported": true, "includeGlobalResourceTypes": false, "resourceTypes": []any{}}
		}
		if rec["recordingMode"] == nil {
			rec["recordingMode"] = map[string]any{"recordingFrequency": "CONTINUOUS", "recordingModeOverrides": []any{}}
		}
		arn := str(rec["arn"])
		if old := c.Get("ConfigurationRecorder", name); old != nil {
			arn = old.ARN
		} else if arn == "" {
			arn = c.arnOf("configuration-recorder/"+name+"/"+randHex(10), nil)
		}
		rec["arn"] = arn
		c.Res = s2Store(c, "ConfigurationRecorder", name, name, arn, rec)
		s2Tags(c, c.Res)
		return map[string]any{}, nil
	})
	Override("config", "DescribeConfigurationRecorders", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "ConfigurationRecorder", "ConfigurationRecorderNames", "NoSuchConfigurationRecorderException", "Cannot find configuration recorder with the specified name '%s'.")
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rs {
			out = append(out, r.Data)
		}
		return map[string]any{"ConfigurationRecorders": orEmpty(out)}, nil
	})
	Override("config", "ListConfigurationRecorders", func(c *Call) (map[string]any, error) {
		var out []any
		for _, r := range c.List("ConfigurationRecorder") {
			out = append(out, map[string]any{"arn": r.ARN, "name": r.Name, "recordingScope": "INTERNAL"})
		}
		return map[string]any{"ConfigurationRecorderSummaries": orEmpty(out)}, nil
	})
	Override("config", "DescribeConfigurationRecorderStatus", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "ConfigurationRecorder", "ConfigurationRecorderNames", "NoSuchConfigurationRecorderException", "Cannot find configuration recorder with the specified name '%s'.")
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rs {
			st := map[string]any{"arn": r.ARN, "name": r.Name, "recording": r.Data["_recording"] == true, "lastStatus": "Pending"}
			for k, from := range map[string]string{"lastStartTime": "_start", "lastStopTime": "_stop", "lastStatusChangeTime": "_change"} {
				if v := r.Data[from]; v != nil {
					st[k] = v
				}
			}
			if st["recording"] == true {
				st["lastStatus"] = "Success"
			}
			out = append(out, st)
		}
		return map[string]any{"ConfigurationRecordersStatus": orEmpty(out)}, nil
	})
	for op, on := range map[string]bool{"StartConfigurationRecorder": true, "StopConfigurationRecorder": false} {
		Override("config", op, func(c *Call) (map[string]any, error) {
			name := str(c.In["ConfigurationRecorderName"])
			r := c.Get("ConfigurationRecorder", name)
			if r == nil {
				return nil, s2Err("NoSuchConfigurationRecorderException", "Cannot find configuration recorder with the specified name '%s'.", name)
			}
			if on && len(c.List("DeliveryChannel")) == 0 {
				return nil, s2Err("NoAvailableDeliveryChannelException", "Delivery channel is not available to start configuration recorder.")
			}
			now := s2Time(time.Now())
			r.Data["_recording"], r.Data["_change"] = on, now
			if on {
				r.Data["_start"] = now
			} else {
				r.Data["_stop"] = now
			}
			c.Put(r)
			return map[string]any{}, nil
		})
	}
	Override("config", "DeleteConfigurationRecorder", func(c *Call) (map[string]any, error) {
		name := str(c.In["ConfigurationRecorderName"])
		if !c.Svc.Delete(c.Account, c.Region, "ConfigurationRecorder", name) {
			return nil, s2Err("NoSuchConfigurationRecorderException", "Cannot find configuration recorder with the specified name '%s'.", name)
		}
		return map[string]any{}, nil
	})

	Override("config", "PutDeliveryChannel", func(c *Call) (map[string]any, error) {
		ch := s2Clone(s2Map(c.In["DeliveryChannel"]))
		name := str(ch["name"])
		if name == "" {
			name = "default"
			ch["name"] = name
		}
		if others := c.List("DeliveryChannel"); len(others) > 0 && others[0].Name != name {
			return nil, s2Err("MaxNumberOfDeliveryChannelsExceededException", "Failed to put delivery channel '%s' because the maximum number of delivery channels: 1 is reached.", name)
		}
		if len(c.List("ConfigurationRecorder")) == 0 {
			return nil, s2Err("NoAvailableConfigurationRecorderException", "Configuration recorder is not available to put delivery channel.")
		}
		if str(ch["s3BucketName"]) == "" {
			return nil, s2Err("InvalidDeliveryChannelNameException", "The s3BucketName is required.")
		}
		s2Store(c, "DeliveryChannel", name, name, "", ch)
		return map[string]any{}, nil
	})
	Override("config", "DescribeDeliveryChannels", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "DeliveryChannel", "DeliveryChannelNames", "NoSuchDeliveryChannelException", "Cannot find delivery channel with specified name '%s'.")
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rs {
			out = append(out, r.Data)
		}
		return map[string]any{"DeliveryChannels": orEmpty(out)}, nil
	})
	Override("config", "DescribeDeliveryChannelStatus", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "DeliveryChannel", "DeliveryChannelNames", "NoSuchDeliveryChannelException", "Cannot find delivery channel with specified name '%s'.")
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rs {
			out = append(out, map[string]any{"name": r.Name,
				"configHistoryDeliveryInfo":  map[string]any{"lastStatus": "SUCCESS"},
				"configSnapshotDeliveryInfo": map[string]any{"lastStatus": "SUCCESS"},
				"configStreamDeliveryInfo":   map[string]any{"lastStatus": "Not_Applicable"}})
		}
		return map[string]any{"DeliveryChannelsStatus": orEmpty(out)}, nil
	})
	Override("config", "DeleteDeliveryChannel", func(c *Call) (map[string]any, error) {
		name := str(c.In["DeliveryChannelName"])
		if c.Get("DeliveryChannel", name) == nil {
			return nil, s2Err("NoSuchDeliveryChannelException", "Cannot find delivery channel with specified name '%s'.", name)
		}
		for _, r := range c.List("ConfigurationRecorder") {
			if r.Data["_recording"] == true {
				return nil, s2Err("LastDeliveryChannelDeleteFailedException", "Failed to delete last specified delivery channel with name '%s', because there,"+
					" is a running configuration recorder.", name)
			}
		}
		c.Svc.Delete(c.Account, c.Region, "DeliveryChannel", name)
		return map[string]any{}, nil
	})

	Override("config", "PutConfigRule", func(c *Call) (map[string]any, error) {
		rule := s2Clone(s2Map(c.In["ConfigRule"]))
		name := str(rule["ConfigRuleName"])
		if name == "" {
			return nil, s2Err("InvalidParameterValueException", "ConfigRuleName is required.")
		}
		if owner := str(s2Map(rule["Source"])["Owner"]); owner == "AWS" && str(s2Map(rule["Source"])["SourceIdentifier"]) == "" {
			return nil, s2Err("InvalidParameterValueException", "SourceIdentifier is required for an AWS managed rule.")
		}
		id, arn := "config-rule-"+randHex(6), ""
		if old := c.Get("ConfigRule", name); old != nil {
			id, arn = str(old.Data["ConfigRuleId"]), old.ARN
		} else {
			arn = c.arnOf("config-rule/"+id, nil)
		}
		rule["ConfigRuleId"], rule["ConfigRuleArn"], rule["ConfigRuleState"] = id, arn, "ACTIVE"
		c.Res = s2Store(c, "ConfigRule", name, name, arn, rule)
		s2Tags(c, c.Res)
		return map[string]any{}, nil
	})
	Override("config", "DescribeConfigRules", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "ConfigRule", "ConfigRuleNames", "NoSuchConfigRuleException", "The ConfigRule '%s' provided in the request is invalid. Please check the configRule name")
		if err != nil {
			return nil, err
		}
		var items []any
		for _, r := range rs {
			items = append(items, r.Data)
		}
		page, next := s2Page(items, nil, str(c.In["NextToken"]))
		out := map[string]any{"ConfigRules": orEmpty(page)}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	})
	Override("config", "DeleteConfigRule", func(c *Call) (map[string]any, error) {
		name := str(c.In["ConfigRuleName"])
		if !c.Svc.Delete(c.Account, c.Region, "ConfigRule", name) {
			return nil, s2Err("NoSuchConfigRuleException", "The ConfigRule '%s' provided in the request is invalid. Please check the configRule name", name)
		}
		for _, e := range c.List("Evaluation") { // its results go with it
			if str(e.Data["ConfigRuleName"]) == name {
				c.Svc.Delete(c.Account, c.Region, "Evaluation", e.Key)
			}
		}
		return map[string]any{}, nil
	})
	Override("config", "StartConfigRulesEvaluation", func(c *Call) (map[string]any, error) {
		for _, n := range s2Names(c.In["ConfigRuleNames"]) {
			if c.Get("ConfigRule", n) == nil {
				return nil, s2Err("NoSuchConfigRuleException", "The ConfigRule '%s' provided in the request is invalid. Please check the configRule name", n)
			}
		}
		return map[string]any{}, nil
	})
	Override("config", "DescribeConfigRuleEvaluationStatus", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "ConfigRule", "ConfigRuleNames", "NoSuchConfigRuleException", "The ConfigRule '%s' provided in the request is invalid. Please check the configRule name")
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rs {
			out = append(out, map[string]any{"ConfigRuleName": r.Name, "ConfigRuleArn": r.ARN, "ConfigRuleId": r.Data["ConfigRuleId"],
				"FirstActivatedTime": s2Time(r.Created), "FirstEvaluationStarted": true, "LastSuccessfulEvaluationTime": s2Time(r.Updated)})
		}
		return map[string]any{"ConfigRulesEvaluationStatus": orEmpty(out)}, nil
	})

	Override("config", "PutEvaluations", func(c *Call) (map[string]any, error) {
		token := str(c.In["ResultToken"])
		var rule *Resource
		for _, r := range c.List("ConfigRule") {
			if r.Name == token || r.ARN == token {
				rule = r
			}
		}
		if rule == nil {
			return nil, s2Err("InvalidResultTokenException", "The resultToken provided is invalid")
		}
		failed := []any{}
		for _, e := range asList(c.In["Evaluations"]) {
			ev := s2Map(e)
			if ct := str(ev["ComplianceType"]); !slices.Contains([]string{"COMPLIANT", "NON_COMPLIANT", "NOT_APPLICABLE"}, ct) {
				return nil, s2Err("InvalidParameterValueException", "Invalid compliance type '%s' in an evaluation: use COMPLIANT, NON_COMPLIANT or NOT_APPLICABLE.", ct)
			}
			if c.In["TestMode"] != true {
				cfgEvaluate(c, rule.Name, ev, token)
			}
		}
		return map[string]any{"FailedEvaluations": failed}, nil
	})
	Override("config", "PutExternalEvaluation", func(c *Call) (map[string]any, error) {
		name := str(c.In["ConfigRuleName"])
		if c.Get("ConfigRule", name) == nil {
			return nil, s2Err("NoSuchConfigRuleException", "The ConfigRule '%s' provided in the request is invalid. Please check the configRule name", name)
		}
		cfgEvaluate(c, name, s2Map(c.In["ExternalEvaluation"]), "")
		return map[string]any{}, nil
	})
	Override("config", "GetComplianceDetailsByConfigRule", func(c *Call) (map[string]any, error) {
		name := str(c.In["ConfigRuleName"])
		if c.Get("ConfigRule", name) == nil {
			return nil, s2Err("NoSuchConfigRuleException", "The ConfigRule '%s' provided in the request is invalid. Please check the configRule name", name)
		}
		want := s2Names(c.In["ComplianceTypes"])
		var items []any
		for _, e := range c.List("Evaluation") {
			if str(e.Data["ConfigRuleName"]) == name && (len(want) == 0 || slices.Contains(want, str(e.Data["ComplianceType"]))) {
				items = append(items, map[string]any{
					"EvaluationResultIdentifier": map[string]any{
						"EvaluationResultQualifier": map[string]any{"ConfigRuleName": name, "ResourceType": e.Data["ComplianceResourceType"], "ResourceId": e.Data["ComplianceResourceId"]},
						"OrderingTimestamp":         e.Data["OrderingTimestamp"]},
					"ComplianceType": e.Data["ComplianceType"], "Annotation": e.Data["Annotation"], "ResultToken": e.Data["ResultToken"],
					"ResultRecordedTime": s2Time(e.Updated), "ConfigRuleInvokedTime": s2Time(e.Updated)})
			}
		}
		page, next := s2Page(items, c.In["Limit"], str(c.In["NextToken"]))
		out := map[string]any{"EvaluationResults": orEmpty(page)}
		if next != "" {
			out["NextToken"] = next
		}
		return out, nil
	})
	Override("config", "DescribeComplianceByConfigRule", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "ConfigRule", "ConfigRuleNames", "NoSuchConfigRuleException", "The ConfigRule '%s' provided in the request is invalid. Please check the configRule name")
		if err != nil {
			return nil, err
		}
		want := s2Names(c.In["ComplianceTypes"])
		evals := c.List("Evaluation")
		var out []any
		for _, r := range rs {
			ct, n := "INSUFFICIENT_DATA", 0.0
			for _, e := range evals {
				if str(e.Data["ConfigRuleName"]) != r.Name {
					continue
				}
				switch str(e.Data["ComplianceType"]) {
				case "NON_COMPLIANT":
					ct, n = "NON_COMPLIANT", n+1
				case "COMPLIANT":
					if ct != "NON_COMPLIANT" {
						ct = "COMPLIANT"
					}
				}
			}
			if len(want) == 0 || slices.Contains(want, ct) {
				out = append(out, map[string]any{"ConfigRuleName": r.Name, "Compliance": map[string]any{"ComplianceType": ct,
					"ComplianceContributorCount": map[string]any{"CappedCount": n, "CapExceeded": false}}})
			}
		}
		return map[string]any{"ComplianceByConfigRules": orEmpty(out)}, nil
	})

	// A conformance pack has an id, and an ARN that ends in it.
	Override("config", "PutConformancePack", func(c *Call) (map[string]any, error) {
		out, err := c.Default()
		if err != nil {
			return nil, err
		}
		r := c.Res
		id := str(r.Data["ConformancePackId"])
		if id == "" {
			id = "conformance-pack-" + randHex(9)
			r.ARN = c.arnOf("conformance-pack/"+r.Name+"/"+id, nil)
		}
		s2Set(c, out, map[string]any{"ConformancePackId": id, "ConformancePackArn": r.ARN})
		return out, nil
	})
	Override("config", "DescribeConformancePackStatus", func(c *Call) (map[string]any, error) {
		rs, err := cfgPick(c, "ConformancePack", "ConformancePackNames", "NoSuchConformancePackException", "Conformance pack %s not found")
		if err != nil {
			return nil, err
		}
		var out []any
		for _, r := range rs {
			out = append(out, map[string]any{"ConformancePackName": r.Name, "ConformancePackId": r.Data["ConformancePackId"], "ConformancePackArn": r.ARN,
				"ConformancePackState": "CREATE_COMPLETE", "StackArn": c.arnOf("stack/awsconfigconforms-"+r.Name+"/"+randHex(8), nil),
				"LastUpdateRequestedTime": s2Time(r.Updated), "LastUpdateCompletedTime": s2Time(r.Updated)})
		}
		return map[string]any{"ConformancePackStatusDetails": orEmpty(out)}, nil
	})
}

// cfgPick returns the resources of a type, or those named by a list member of the request;
// a name that does not exist is the type's NoSuch exception.
func cfgPick(c *Call, typ, member, code, msg string) ([]*Resource, error) {
	names := s2Names(c.In[member])
	if len(names) == 0 {
		return c.List(typ), nil
	}
	var out []*Resource
	for _, n := range names {
		r := c.Get(typ, n)
		if r == nil {
			return nil, s2Err(code, "%s", strings.ReplaceAll(msg, "%s", n))
		}
		out = append(out, r)
	}
	return out, nil
}

// cfgEvaluate records the latest evaluation of a resource by a rule.
func cfgEvaluate(c *Call, rule string, ev map[string]any, token string) {
	ev = s2Clone(ev)
	if ts, ok := normTime(ev["OrderingTimestamp"]); ok {
		ev["OrderingTimestamp"] = ts
	}
	ev["ConfigRuleName"], ev["ResultToken"] = rule, token
	key := rule + "/" + str(ev["ComplianceResourceType"]) + "/" + str(ev["ComplianceResourceId"])
	s2Store(c, "Evaluation", key, key, "", ev)
}

// orEmpty is a list that encodes as [] rather than being left out.
func orEmpty(l []any) []any {
	if l == nil {
		return []any{}
	}
	return l
}
