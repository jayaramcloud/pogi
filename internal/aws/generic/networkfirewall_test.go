package generic_test

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall"
	"github.com/aws/aws-sdk-go-v2/service/networkfirewall/types"
	"github.com/nrndev/pogi/internal/awstest"
)

func TestNetworkFirewall(t *testing.T) {
	_, base := start(t, "network-firewall")
	ctx := context.Background()
	c := networkfirewall.NewFromConfig(awstest.Config(t, base))

	// A stateful rule group.
	rg, err := c.CreateRuleGroup(ctx, &networkfirewall.CreateRuleGroupInput{RuleGroupName: aws.String("allow-web"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(100),
		Description: aws.String("web"), Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
		RuleGroup: &types.RuleGroup{RulesSource: &types.RulesSource{StatefulRules: []types.StatefulRule{{Action: types.StatefulActionPass,
			Header:      &types.Header{Protocol: types.StatefulRuleProtocolTcp, Source: aws.String("ANY"), SourcePort: aws.String("ANY"), Destination: aws.String("ANY"), DestinationPort: aws.String("443"), Direction: types.StatefulRuleDirectionForward},
			RuleOptions: []types.RuleOption{{Keyword: aws.String("sid:1")}}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	rgArn := aws.ToString(rg.RuleGroupResponse.RuleGroupArn)
	if rgArn != "arn:aws:network-firewall:us-east-1:000000000000:stateful-rulegroup/allow-web" || rg.RuleGroupResponse.RuleGroupStatus != types.ResourceStatusActive || aws.ToString(rg.UpdateToken) == "" ||
		aws.ToInt32(rg.RuleGroupResponse.Capacity) != 100 || len(rg.RuleGroupResponse.Tags) != 1 {
		t.Fatalf("create rule group: %+v %+v", rg.RuleGroupResponse, rg)
	}
	if _, err := c.CreateRuleGroup(ctx, &networkfirewall.CreateRuleGroupInput{RuleGroupName: aws.String("allow-web"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(1)}); err == nil {
		t.Fatal("duplicate rule group accepted")
	}
	d, err := c.DescribeRuleGroup(ctx, &networkfirewall.DescribeRuleGroupInput{RuleGroupName: aws.String("allow-web"), Type: types.RuleGroupTypeStateful})
	if err != nil || d.RuleGroup == nil || len(d.RuleGroup.RulesSource.StatefulRules) != 1 || aws.ToString(d.RuleGroupResponse.Description) != "web" {
		t.Fatalf("describe rule group: %v %+v", err, d)
	}
	if byArn, err := c.DescribeRuleGroup(ctx, &networkfirewall.DescribeRuleGroupInput{RuleGroupArn: aws.String(rgArn)}); err != nil || aws.ToString(byArn.RuleGroupResponse.RuleGroupName) != "allow-web" {
		t.Fatalf("describe by arn: %v %+v", err, byArn)
	}
	// Update tokens guard changes and rotate.
	if _, err := c.UpdateRuleGroup(ctx, &networkfirewall.UpdateRuleGroupInput{RuleGroupArn: aws.String(rgArn), UpdateToken: aws.String("stale"), Description: aws.String("x"), RuleGroup: d.RuleGroup}); !errors.As(err, new(*types.InvalidTokenException)) {
		t.Fatalf("stale token: %T %v", err, err)
	}
	up, err := c.UpdateRuleGroup(ctx, &networkfirewall.UpdateRuleGroupInput{RuleGroupArn: aws.String(rgArn), UpdateToken: d.UpdateToken, Description: aws.String("web traffic"), RuleGroup: d.RuleGroup})
	if err != nil || aws.ToString(up.UpdateToken) == aws.ToString(d.UpdateToken) || aws.ToString(up.RuleGroupResponse.Description) != "web traffic" {
		t.Fatalf("update rule group: %v %+v", err, up)
	}
	if l, err := c.ListRuleGroups(ctx, &networkfirewall.ListRuleGroupsInput{}); err != nil || len(l.RuleGroups) != 1 || aws.ToString(l.RuleGroups[0].Name) != "allow-web" {
		t.Fatalf("list rule groups: %v %+v", err, l)
	}
	if l, _ := c.ListRuleGroups(ctx, &networkfirewall.ListRuleGroupsInput{Type: types.RuleGroupTypeStateless}); len(l.RuleGroups) != 0 {
		t.Fatalf("stateless rule groups: %+v", l.RuleGroups)
	}
	if _, err := c.TagResource(ctx, &networkfirewall.TagResourceInput{ResourceArn: aws.String(rgArn), Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("net")}}}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.UntagResource(ctx, &networkfirewall.UntagResourceInput{ResourceArn: aws.String(rgArn), TagKeys: []string{"env"}}); err != nil {
		t.Fatal(err)
	}
	tags, err := c.ListTagsForResource(ctx, &networkfirewall.ListTagsForResourceInput{ResourceArn: aws.String(rgArn)})
	if err != nil || len(tags.Tags) != 1 || aws.ToString(tags.Tags[0].Key) != "team" {
		t.Fatalf("tags: %v %+v", err, tags)
	}

	// A policy that uses it, and a firewall that uses the policy.
	fp, err := c.CreateFirewallPolicy(ctx, &networkfirewall.CreateFirewallPolicyInput{FirewallPolicyName: aws.String("pol"), FirewallPolicy: &types.FirewallPolicy{
		StatelessDefaultActions: []string{"aws:forward_to_sfe"}, StatelessFragmentDefaultActions: []string{"aws:forward_to_sfe"},
		StatefulRuleGroupReferences: []types.StatefulRuleGroupReference{{ResourceArn: aws.String(rgArn)}}}})
	if err != nil {
		t.Fatal(err)
	}
	polArn := aws.ToString(fp.FirewallPolicyResponse.FirewallPolicyArn)
	if polArn != "arn:aws:network-firewall:us-east-1:000000000000:firewall-policy/pol" {
		t.Fatalf("policy arn %s", polArn)
	}
	if g, err := c.DescribeRuleGroup(ctx, &networkfirewall.DescribeRuleGroupInput{RuleGroupArn: aws.String(rgArn)}); err != nil || aws.ToInt32(g.RuleGroupResponse.NumberOfAssociations) != 1 {
		t.Fatalf("rule group associations: %v %+v", err, g)
	}
	if _, err := c.DeleteRuleGroup(ctx, &networkfirewall.DeleteRuleGroupInput{RuleGroupArn: aws.String(rgArn)}); !errors.As(err, new(*types.InvalidOperationException)) {
		t.Fatalf("delete rule group in use: %T %v", err, err)
	}
	if _, err := c.CreateFirewall(ctx, &networkfirewall.CreateFirewallInput{FirewallName: aws.String("fw"), FirewallPolicyArn: aws.String(polArn + "-missing"), VpcId: aws.String("vpc-1"),
		SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-1")}}}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("firewall with a missing policy: %T %v", err, err)
	}
	fw, err := c.CreateFirewall(ctx, &networkfirewall.CreateFirewallInput{FirewallName: aws.String("fw"), FirewallPolicyArn: aws.String(polArn), VpcId: aws.String("vpc-1"), DeleteProtection: true,
		SubnetMappings: []types.SubnetMapping{{SubnetId: aws.String("subnet-1")}, {SubnetId: aws.String("subnet-2")}}, Tags: []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}}})
	if err != nil {
		t.Fatal(err)
	}
	fwArn := aws.ToString(fw.Firewall.FirewallArn)
	if fw.FirewallStatus.Status != types.FirewallStatusValueReady || fw.FirewallStatus.ConfigurationSyncStateSummary != types.ConfigurationSyncStateInSync || len(fw.FirewallStatus.SyncStates) != 2 ||
		aws.ToString(fw.Firewall.VpcId) != "vpc-1" || !fw.Firewall.DeleteProtection {
		t.Fatalf("create firewall: %+v %+v", fw.Firewall, fw.FirewallStatus)
	}
	df, err := c.DescribeFirewall(ctx, &networkfirewall.DescribeFirewallInput{FirewallName: aws.String("fw")})
	if err != nil || aws.ToString(df.Firewall.FirewallPolicyArn) != polArn || len(df.Firewall.SubnetMappings) != 2 || len(df.Firewall.Tags) != 1 || aws.ToString(df.UpdateToken) == "" {
		t.Fatalf("describe firewall: %v %+v", err, df)
	}
	if l, err := c.ListFirewalls(ctx, &networkfirewall.ListFirewallsInput{VpcIds: []string{"vpc-1"}}); err != nil || len(l.Firewalls) != 1 || aws.ToString(l.Firewalls[0].FirewallName) != "fw" {
		t.Fatalf("list firewalls: %v %+v", err, l)
	}
	if l, _ := c.ListFirewalls(ctx, &networkfirewall.ListFirewallsInput{VpcIds: []string{"vpc-other"}}); len(l.Firewalls) != 0 {
		t.Fatalf("firewalls of another vpc: %+v", l.Firewalls)
	}
	dp, err := c.UpdateFirewallDeleteProtection(ctx, &networkfirewall.UpdateFirewallDeleteProtectionInput{FirewallName: aws.String("fw"), DeleteProtection: false, UpdateToken: df.UpdateToken})
	if err != nil || dp.DeleteProtection {
		t.Fatalf("delete protection: %v %+v", err, dp)
	}
	if _, err := c.UpdateFirewallDescription(ctx, &networkfirewall.UpdateFirewallDescriptionInput{FirewallName: aws.String("fw"), Description: aws.String("edge"), UpdateToken: df.UpdateToken}); !errors.As(err, new(*types.InvalidTokenException)) {
		t.Fatalf("old token: %T %v", err, err)
	}
	if _, err := c.UpdateFirewallDescription(ctx, &networkfirewall.UpdateFirewallDescriptionInput{FirewallName: aws.String("fw"), Description: aws.String("edge"), UpdateToken: dp.UpdateToken}); err != nil {
		t.Fatal(err)
	}
	if df, _ = c.DescribeFirewall(ctx, &networkfirewall.DescribeFirewallInput{FirewallArn: aws.String(fwArn)}); aws.ToString(df.Firewall.Description) != "edge" {
		t.Fatalf("description: %+v", df.Firewall)
	}
	if _, err := c.DeleteFirewallPolicy(ctx, &networkfirewall.DeleteFirewallPolicyInput{FirewallPolicyName: aws.String("pol")}); !errors.As(err, new(*types.InvalidOperationException)) {
		t.Fatalf("delete policy in use: %T %v", err, err)
	}

	// Policy update, then everything is deleted from the top.
	dpol, _ := c.DescribeFirewallPolicy(ctx, &networkfirewall.DescribeFirewallPolicyInput{FirewallPolicyArn: aws.String(polArn)})
	if _, err := c.UpdateFirewallPolicy(ctx, &networkfirewall.UpdateFirewallPolicyInput{FirewallPolicyArn: aws.String(polArn), UpdateToken: dpol.UpdateToken, FirewallPolicy: dpol.FirewallPolicy, Description: aws.String("v2")}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListFirewallPolicies(ctx, &networkfirewall.ListFirewallPoliciesInput{}); err != nil || len(l.FirewallPolicies) != 1 {
		t.Fatalf("list policies: %v %+v", err, l)
	}
	if _, err := c.DeleteFirewall(ctx, &networkfirewall.DeleteFirewallInput{FirewallName: aws.String("fw")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeFirewall(ctx, &networkfirewall.DescribeFirewallInput{FirewallName: aws.String("fw")}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("describe deleted firewall: %T %v", err, err)
	}
	if _, err := c.DeleteFirewallPolicy(ctx, &networkfirewall.DeleteFirewallPolicyInput{FirewallPolicyName: aws.String("pol")}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DeleteRuleGroup(ctx, &networkfirewall.DeleteRuleGroupInput{RuleGroupArn: aws.String(rgArn)}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.DescribeRuleGroup(ctx, &networkfirewall.DescribeRuleGroupInput{RuleGroupArn: aws.String(rgArn)}); !errors.As(err, new(*types.ResourceNotFoundException)) {
		t.Fatalf("describe deleted rule group: %T %v", err, err)
	}
}

// floci #4763: a dry run checks the request and changes nothing.
func TestNetworkFirewallDryRun(t *testing.T) {
	_, base := start(t, "network-firewall")
	ctx := context.Background()
	c := networkfirewall.NewFromConfig(awstest.Config(t, base))
	rg := &types.RuleGroup{RulesSource: &types.RulesSource{RulesString: aws.String(`pass tcp any any -> any any (sid:1;)`)}}
	if _, err := c.CreateRuleGroup(ctx, &networkfirewall.CreateRuleGroupInput{RuleGroupName: aws.String("rg"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(10), RuleGroup: rg, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	if l, err := c.ListRuleGroups(ctx, &networkfirewall.ListRuleGroupsInput{}); err != nil || len(l.RuleGroups) != 0 {
		t.Fatalf("a dry-run create made rule groups: %v %+v", err, l)
	}
	made, err := c.CreateRuleGroup(ctx, &networkfirewall.CreateRuleGroupInput{RuleGroupName: aws.String("rg"), Type: types.RuleGroupTypeStateful, Capacity: aws.Int32(10), RuleGroup: rg, Description: aws.String("one")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.UpdateRuleGroup(ctx, &networkfirewall.UpdateRuleGroupInput{RuleGroupName: aws.String("rg"), Type: types.RuleGroupTypeStateful, UpdateToken: made.UpdateToken, Description: aws.String("two"), RuleGroup: rg, DryRun: true}); err != nil {
		t.Fatal(err)
	}
	d, err := c.DescribeRuleGroup(ctx, &networkfirewall.DescribeRuleGroupInput{RuleGroupName: aws.String("rg"), Type: types.RuleGroupTypeStateful})
	if err != nil || aws.ToString(d.RuleGroupResponse.Description) != "one" || aws.ToString(d.UpdateToken) != aws.ToString(made.UpdateToken) {
		t.Fatalf("a dry-run update changed the rule group: %v %+v", err, d)
	}
}
