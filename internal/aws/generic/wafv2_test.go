package generic_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/wafv2"
	"github.com/aws/aws-sdk-go-v2/service/wafv2/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestWAFv2(t *testing.T) {
	_, base := start(t, "wafv2")
	ctx := context.Background()
	c := wafv2.NewFromConfig(awstest.Config(t, base))
	vis := &types.VisibilityConfig{CloudWatchMetricsEnabled: true, MetricName: aws.String("m"), SampledRequestsEnabled: true}

	ip, err := c.CreateIPSet(ctx, &wafv2.CreateIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, IPAddressVersion: types.IPAddressVersionIpv4, Addresses: []string{"10.0.0.0/8"},
		Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	ipArn := aws.ToString(ip.Summary.ARN)
	if ipArn == "" || aws.ToString(ip.Summary.Id) == "" || aws.ToString(ip.Summary.LockToken) == "" {
		t.Fatalf("ip set: %+v", ip.Summary)
	}
	if _, err := c.CreateIPSet(ctx, &wafv2.CreateIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, IPAddressVersion: types.IPAddressVersionIpv4, Addresses: []string{}}); !errors.As(err, new(*types.WAFDuplicateItemException)) {
		t.Fatalf("duplicate ip set: %T %v", err, err)
	}
	g, err := c.GetIPSet(ctx, &wafv2.GetIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, Id: ip.Summary.Id})
	if err != nil || len(g.IPSet.Addresses) != 1 || aws.ToString(g.LockToken) != aws.ToString(ip.Summary.LockToken) {
		t.Fatalf("get ip set: %v %+v", err, g)
	}
	// Lock tokens guard updates and change with each one.
	if _, err := c.UpdateIPSet(ctx, &wafv2.UpdateIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, Id: ip.Summary.Id, Addresses: []string{"10.0.0.0/8", "192.168.0.0/16"}, LockToken: aws.String("stale")}); !errors.As(err, new(*types.WAFOptimisticLockException)) {
		t.Fatalf("stale lock token: %T %v", err, err)
	}
	up, err := c.UpdateIPSet(ctx, &wafv2.UpdateIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, Id: ip.Summary.Id, Addresses: []string{"10.0.0.0/8", "192.168.0.0/16"}, LockToken: g.LockToken})
	if err != nil || aws.ToString(up.NextLockToken) == aws.ToString(g.LockToken) {
		t.Fatalf("update ip set: %v %+v", err, up)
	}
	g, _ = c.GetIPSet(ctx, &wafv2.GetIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, Id: ip.Summary.Id})
	if len(g.IPSet.Addresses) != 2 || aws.ToString(g.LockToken) != aws.ToString(up.NextLockToken) {
		t.Fatalf("after update: %+v", g)
	}

	// floci #3328: addresses must be CIDR blocks of the set's IP version, on create and update.
	bare := []string{"203.0.113.10"}
	if _, err := c.CreateIPSet(ctx, &wafv2.CreateIPSetInput{Name: aws.String("bare"), Scope: types.ScopeRegional, IPAddressVersion: types.IPAddressVersionIpv4, Addresses: bare}); !errors.As(err, new(*types.WAFInvalidParameterException)) {
		t.Fatalf("bare IP on create: %T %v", err, err)
	}
	if _, err := c.CreateIPSet(ctx, &wafv2.CreateIPSetInput{Name: aws.String("v6"), Scope: types.ScopeRegional, IPAddressVersion: types.IPAddressVersionIpv4, Addresses: []string{"2001:db8::/32"}}); !errors.As(err, new(*types.WAFInvalidParameterException)) {
		t.Fatalf("IPv6 block in an IPv4 set: %T %v", err, err)
	}
	if _, err := c.UpdateIPSet(ctx, &wafv2.UpdateIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, Id: ip.Summary.Id, Addresses: bare, LockToken: g.LockToken}); !errors.As(err, new(*types.WAFInvalidParameterException)) {
		t.Fatalf("bare IP on update: %T %v", err, err)
	}

	rx, err := c.CreateRegexPatternSet(ctx, &wafv2.CreateRegexPatternSetInput{Name: aws.String("bots"), Scope: types.ScopeRegional, RegularExpressionList: []types.Regex{{RegexString: aws.String("^bot")}}})
	if err != nil {
		t.Fatal(err)
	}

	// A rule group and a web ACL that use them; capacity is computed.
	rules := []types.Rule{{Name: aws.String("ip"), Priority: 1, Action: &types.RuleAction{Block: &types.BlockAction{}}, VisibilityConfig: vis,
		Statement: &types.Statement{IPSetReferenceStatement: &types.IPSetReferenceStatement{ARN: aws.String(ipArn)}}},
		{Name: aws.String("rx"), Priority: 2, Action: &types.RuleAction{Count: &types.CountAction{}}, VisibilityConfig: vis,
			Statement: &types.Statement{RegexPatternSetReferenceStatement: &types.RegexPatternSetReferenceStatement{ARN: rx.Summary.ARN, FieldToMatch: &types.FieldToMatch{UriPath: &types.UriPath{}},
				TextTransformations: []types.TextTransformation{{Priority: 0, Type: types.TextTransformationTypeNone}}}}}}
	cc, err := c.CheckCapacity(ctx, &wafv2.CheckCapacityInput{Scope: types.ScopeRegional, Rules: rules})
	if err != nil || cc.Capacity != 26 {
		t.Fatalf("capacity: %v %+v", err, cc)
	}
	rg, err := c.CreateRuleGroup(ctx, &wafv2.CreateRuleGroupInput{Name: aws.String("rg"), Scope: types.ScopeRegional, Capacity: aws.Int64(30), Rules: rules, VisibilityConfig: vis})
	if err != nil {
		t.Fatal(err)
	}
	acl, err := c.CreateWebACL(ctx, &wafv2.CreateWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, DefaultAction: &types.DefaultAction{Allow: &types.AllowAction{}}, VisibilityConfig: vis,
		Description: aws.String("front door"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
		Rules: []types.Rule{{Name: aws.String("grp"), Priority: 0, OverrideAction: &types.OverrideAction{None: &types.NoneAction{}}, VisibilityConfig: vis,
			Statement: &types.Statement{RuleGroupReferenceStatement: &types.RuleGroupReferenceStatement{ARN: rg.Summary.ARN}}}}})
	if err != nil {
		t.Fatal(err)
	}
	ga, err := c.GetWebACL(ctx, &wafv2.GetWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, Id: acl.Summary.Id})
	if err != nil || ga.WebACL.Capacity != 30 || len(ga.WebACL.Rules) != 1 || ga.WebACL.DefaultAction.Allow == nil || aws.ToString(ga.WebACL.Description) != "front door" || aws.ToString(ga.WebACL.ARN) != aws.ToString(acl.Summary.ARN) {
		t.Fatalf("get web acl: %v %+v", err, ga)
	}
	if byArn, err := c.GetRuleGroup(ctx, &wafv2.GetRuleGroupInput{ARN: rg.Summary.ARN}); err != nil || aws.ToString(byArn.RuleGroup.Name) != "rg" {
		t.Fatalf("rule group by arn: %v %+v", err, byArn)
	}
	list, err := c.ListWebACLs(ctx, &wafv2.ListWebACLsInput{Scope: types.ScopeRegional})
	if err != nil || len(list.WebACLs) != 1 || aws.ToString(list.WebACLs[0].Name) != "acl" {
		t.Fatalf("list web acls: %v %+v", err, list)
	}
	if l, err := c.ListIPSets(ctx, &wafv2.ListIPSetsInput{Scope: types.ScopeRegional}); err != nil || len(l.IPSets) != 1 {
		t.Fatalf("list ip sets: %v %+v", err, l)
	}
	if l, _ := c.ListIPSets(ctx, &wafv2.ListIPSetsInput{Scope: types.ScopeCloudfront}); len(l.IPSets) != 0 {
		t.Fatalf("cloudfront scope is separate: %+v", l)
	}
	if l, err := c.ListRuleGroups(ctx, &wafv2.ListRuleGroupsInput{Scope: types.ScopeRegional}); err != nil || len(l.RuleGroups) != 1 {
		t.Fatalf("list rule groups: %v %+v", err, l)
	}
	if l, err := c.ListRegexPatternSets(ctx, &wafv2.ListRegexPatternSetsInput{Scope: types.ScopeRegional}); err != nil || len(l.RegexPatternSets) != 1 {
		t.Fatalf("list regex sets: %v %+v", err, l)
	}

	// Update a web ACL, and tag it.
	if _, err := c.UpdateWebACL(ctx, &wafv2.UpdateWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, Id: acl.Summary.Id, LockToken: ga.LockToken, DefaultAction: &types.DefaultAction{Block: &types.BlockAction{}},
		VisibilityConfig: vis, Description: aws.String("locked down")}); err != nil {
		t.Fatal(err)
	}
	ga, _ = c.GetWebACL(ctx, &wafv2.GetWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, Id: acl.Summary.Id})
	if ga.WebACL.DefaultAction.Block == nil || aws.ToString(ga.WebACL.Description) != "locked down" || len(ga.WebACL.Rules) != 0 || ga.WebACL.Capacity != 0 {
		t.Fatalf("after update: %+v", ga.WebACL)
	}
	if _, err := c.TagResource(ctx, &wafv2.TagResourceInput{ResourceARN: acl.Summary.ARN, Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("sec")}}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &wafv2.ListTagsForResourceInput{ResourceARN: acl.Summary.ARN})
	if err != nil || len(tags.TagInfoForResource.TagList) != 2 {
		t.Fatalf("tags: %v %+v", err, tags)
	}
	if _, err := c.UntagResource(ctx, &wafv2.UntagResourceInput{ResourceARN: acl.Summary.ARN, TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, _ = c.ListTagsForResource(ctx, &wafv2.ListTagsForResourceInput{ResourceARN: acl.Summary.ARN})
	if len(tags.TagInfoForResource.TagList) != 1 || aws.ToString(tags.TagInfoForResource.TagList[0].Key) != "team" {
		t.Fatalf("tags after: %+v", tags.TagInfoForResource.TagList)
	}

	// Association is stored, and blocks deletion.
	const lb = "arn:aws:elasticloadbalancing:us-east-1:000000000000:loadbalancer/app/x/1"
	if _, err := c.AssociateWebACL(ctx, &wafv2.AssociateWebACLInput{WebACLArn: acl.Summary.ARN, ResourceArn: aws.String(lb)}); err != nil {
		t.Fatal(err)
	}
	if w, err := c.GetWebACLForResource(ctx, &wafv2.GetWebACLForResourceInput{ResourceArn: aws.String(lb)}); err != nil || aws.ToString(w.WebACL.Name) != "acl" {
		t.Fatalf("web acl for resource: %v %+v", err, w)
	}
	if r, err := c.ListResourcesForWebACL(ctx, &wafv2.ListResourcesForWebACLInput{WebACLArn: acl.Summary.ARN}); err != nil || len(r.ResourceArns) != 1 || r.ResourceArns[0] != lb {
		t.Fatalf("resources for web acl: %v %+v", err, r)
	}
	if _, err := c.DeleteWebACL(ctx, &wafv2.DeleteWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, Id: acl.Summary.Id, LockToken: ga.LockToken}); !errors.As(err, new(*types.WAFAssociatedItemException)) {
		t.Fatalf("delete associated web acl: %T %v", err, err)
	}
	if _, err := c.DisassociateWebACL(ctx, &wafv2.DisassociateWebACLInput{ResourceArn: aws.String(lb)}); err != nil {
		t.Fatal(err)
	}

	// Logging.
	if _, err := c.PutLoggingConfiguration(ctx, &wafv2.PutLoggingConfigurationInput{LoggingConfiguration: &types.LoggingConfiguration{ResourceArn: acl.Summary.ARN,
		LogDestinationConfigs: []string{"arn:aws:logs:us-east-1:000000000000:log-group:aws-waf-logs-x"}}}); err != nil {
		t.Fatal(err)
	}
	if lc, err := c.GetLoggingConfiguration(ctx, &wafv2.GetLoggingConfigurationInput{ResourceArn: acl.Summary.ARN}); err != nil || len(lc.LoggingConfiguration.LogDestinationConfigs) != 1 {
		t.Fatalf("logging: %v %+v", err, lc)
	}
	if _, err := c.DeleteLoggingConfiguration(ctx, &wafv2.DeleteLoggingConfigurationInput{ResourceArn: acl.Summary.ARN}); err != nil {
		t.Fatal(err)
	}

	// Deletion: in-use sets are protected, then everything goes.
	del := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	rgg, _ := c.GetRuleGroup(ctx, &wafv2.GetRuleGroupInput{Name: aws.String("rg"), Scope: types.ScopeRegional, Id: rg.Summary.Id})
	ga, _ = c.GetWebACL(ctx, &wafv2.GetWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, Id: acl.Summary.Id})
	if _, err := c.DeleteIPSet(ctx, &wafv2.DeleteIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, Id: ip.Summary.Id, LockToken: g.LockToken}); !errors.As(err, new(*types.WAFAssociatedItemException)) {
		t.Fatalf("delete ip set in use: %T %v", err, err)
	}
	_, err = c.DeleteWebACL(ctx, &wafv2.DeleteWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, Id: acl.Summary.Id, LockToken: ga.LockToken})
	del(err)
	_, err = c.DeleteRuleGroup(ctx, &wafv2.DeleteRuleGroupInput{Name: aws.String("rg"), Scope: types.ScopeRegional, Id: rg.Summary.Id, LockToken: rgg.LockToken})
	del(err)
	_, err = c.DeleteIPSet(ctx, &wafv2.DeleteIPSetInput{Name: aws.String("block"), Scope: types.ScopeRegional, Id: ip.Summary.Id, LockToken: g.LockToken})
	del(err)
	rxg, _ := c.GetRegexPatternSet(ctx, &wafv2.GetRegexPatternSetInput{Name: aws.String("bots"), Scope: types.ScopeRegional, Id: rx.Summary.Id})
	_, err = c.DeleteRegexPatternSet(ctx, &wafv2.DeleteRegexPatternSetInput{Name: aws.String("bots"), Scope: types.ScopeRegional, Id: rx.Summary.Id, LockToken: rxg.LockToken})
	del(err)
	_, err = c.GetWebACL(ctx, &wafv2.GetWebACLInput{Name: aws.String("acl"), Scope: types.ScopeRegional, Id: acl.Summary.Id})
	if !errors.As(err, new(*types.WAFNonexistentItemException)) {
		t.Fatalf("get after delete: %T %v", err, err)
	}
}

// floci #4035: a resource holds at most 50 tags, in one call or built up over several.
func TestWAFv2TagLimit(t *testing.T) {
	_, base := start(t, "wafv2")
	ctx := context.Background()
	c := wafv2.NewFromConfig(awstest.Config(t, base))
	ip, err := c.CreateIPSet(ctx, &wafv2.CreateIPSetInput{Name: aws.String("tagged"), Scope: types.ScopeRegional, IPAddressVersion: types.IPAddressVersionIpv4, Addresses: []string{"10.0.0.0/8"}})
	if err != nil {
		t.Fatal(err)
	}
	tags := func(from, to int) (out []types.Tag) {
		for i := from; i < to; i++ {
			out = append(out, types.Tag{Key: aws.String(fmt.Sprintf("k%02d", i)), Value: aws.String("v")})
		}
		return out
	}
	if _, err := c.TagResource(ctx, &wafv2.TagResourceInput{ResourceARN: ip.Summary.ARN, Tags: tags(0, 51)}); !errors.As(err, new(*types.WAFLimitsExceededException)) {
		t.Fatalf("51 tags in one call: %T %v", err, err)
	}
	if _, err := c.TagResource(ctx, &wafv2.TagResourceInput{ResourceARN: ip.Summary.ARN, Tags: tags(0, 50)}); err != nil {
		t.Fatalf("50 tags: %v", err)
	}
	if _, err := c.TagResource(ctx, &wafv2.TagResourceInput{ResourceARN: ip.Summary.ARN, Tags: tags(0, 50)}); err != nil {
		t.Fatalf("re-sending the same 50 tags: %v", err)
	}
	if _, err := c.TagResource(ctx, &wafv2.TagResourceInput{ResourceARN: ip.Summary.ARN, Tags: tags(50, 51)}); !errors.As(err, new(*types.WAFLimitsExceededException)) {
		t.Fatalf("the 51st tag: %T %v", err, err)
	}
}
