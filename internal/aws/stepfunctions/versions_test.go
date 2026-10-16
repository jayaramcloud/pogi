package stepfunctions_test

import (
	"errors"
	"strconv"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sfn"
	"github.com/aws/aws-sdk-go-v2/service/sfn/types"
)

func passDef(result int, comment string) string {
	return `{"Comment":"` + comment + `","StartAt":"A","States":{"A":{"Type":"Pass","Result":` + strconv.Itoa(result) + `,"End":true}}}`
}

func isErr[T error](t *testing.T, what string, err error) {
	t.Helper()
	var want T
	if !errors.As(err, &want) {
		t.Errorf("%s: got %v", what, err)
	}
}

// floci #3614, #3516: versions, aliases, and version and alias ARNs wherever a state machine ARN is taken.
func TestVersionsAndAliases(t *testing.T) {
	e := setup(t)
	v1, v2 := passDef(1, "one"), passDef(2, "two")
	sm := e.machine(t, "ver", v1)
	publish := func(rev, desc string) (*sfn.PublishStateMachineVersionOutput, error) {
		in := &sfn.PublishStateMachineVersionInput{StateMachineArn: aws.String(sm)}
		if rev != "" {
			in.RevisionId = aws.String(rev)
		}
		if desc != "" {
			in.Description = aws.String(desc)
		}
		return e.sfn.PublishStateMachineVersion(ctx, in)
	}

	first := must(publish("", "first"))
	if aws.ToString(first.StateMachineVersionArn) != sm+":1" || first.CreationDate == nil {
		t.Fatalf("publish = %+v", first)
	}
	if again := must(publish("", "ignored")); aws.ToString(again.StateMachineVersionArn) != sm+":1" {
		t.Fatalf("publishing an unchanged revision made %s", aws.ToString(again.StateMachineVersionArn))
	}
	_, err := publish("not-the-revision", "")
	isErr[*types.ConflictException](t, "stale revision id", err)

	up := must(e.sfn.UpdateStateMachine(ctx, &sfn.UpdateStateMachineInput{StateMachineArn: aws.String(sm), Definition: aws.String(v2), Publish: true, VersionDescription: aws.String("second")}))
	if aws.ToString(up.StateMachineVersionArn) != sm+":2" {
		t.Fatalf("update with publish = %+v", up)
	}
	list := must(e.sfn.ListStateMachineVersions(ctx, &sfn.ListStateMachineVersionsInput{StateMachineArn: aws.String(sm)}))
	if len(list.StateMachineVersions) != 2 || aws.ToString(list.StateMachineVersions[0].StateMachineVersionArn) != sm+":2" {
		t.Fatalf("versions = %+v", list.StateMachineVersions)
	}
	page := must(e.sfn.ListStateMachineVersions(ctx, &sfn.ListStateMachineVersionsInput{StateMachineArn: aws.String(sm), MaxResults: 1}))
	if len(page.StateMachineVersions) != 1 || page.NextToken == nil {
		t.Fatalf("first page = %+v", page)
	}

	d1 := must(e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm + ":1")}))
	if aws.ToString(d1.Definition) != v1 || aws.ToString(d1.Description) != "first" || aws.ToString(d1.StateMachineArn) != sm+":1" {
		t.Fatalf("describe version 1 = %s %s", aws.ToString(d1.Definition), aws.ToString(d1.Description))
	}
	if d := must(e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm)})); aws.ToString(d.Definition) != v2 {
		t.Fatalf("describe the machine = %s", aws.ToString(d.Definition))
	}
	_, err = e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm + ":9")})
	isErr[*types.StateMachineDoesNotExist](t, "unknown version", err)

	// An execution through a version runs that version's definition.
	byVersion := e.start(t, sm+":1", `{}`)
	d := e.wait(t, byVersion)
	if aws.ToString(d.Output) != "1" || aws.ToString(d.StateMachineVersionArn) != sm+":1" || d.StateMachineAliasArn != nil || aws.ToString(d.StateMachineArn) != sm {
		t.Fatalf("execution of version 1 = %s %v %v", aws.ToString(d.Output), aws.ToString(d.StateMachineVersionArn), aws.ToString(d.StateMachineArn))
	}
	if h := must(e.sfn.DescribeStateMachineForExecution(ctx, &sfn.DescribeStateMachineForExecutionInput{ExecutionArn: aws.String(byVersion)})); aws.ToString(h.Definition) != v1 {
		t.Fatalf("definition for the execution = %s", aws.ToString(h.Definition))
	}
	plain := e.wait(t, e.start(t, sm, `{}`))
	if aws.ToString(plain.Output) != "2" || plain.StateMachineVersionArn != nil {
		t.Fatalf("unqualified execution = %s %v", aws.ToString(plain.Output), plain.StateMachineVersionArn)
	}

	// Aliases.
	route := func(arn string, w int32) types.RoutingConfigurationListItem {
		return types.RoutingConfigurationListItem{StateMachineVersionArn: aws.String(arn), Weight: w}
	}
	prod := must(e.sfn.CreateStateMachineAlias(ctx, &sfn.CreateStateMachineAliasInput{Name: aws.String("PROD"), Description: aws.String("live"),
		RoutingConfiguration: []types.RoutingConfigurationListItem{route(sm+":1", 100)}}))
	if aws.ToString(prod.StateMachineAliasArn) != sm+":PROD" {
		t.Fatalf("alias = %s", aws.ToString(prod.StateMachineAliasArn))
	}
	if a := must(e.sfn.DescribeStateMachineAlias(ctx, &sfn.DescribeStateMachineAliasInput{StateMachineAliasArn: aws.String(sm + ":PROD")})); aws.ToString(a.Name) != "PROD" ||
		aws.ToString(a.Description) != "live" || len(a.RoutingConfiguration) != 1 || a.CreationDate == nil || a.UpdateDate == nil {
		t.Fatalf("describe alias = %+v", a)
	}
	if d := must(e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm + ":PROD")})); aws.ToString(d.Definition) != v1 {
		t.Fatalf("describe through the alias = %s", aws.ToString(d.Definition))
	}
	byAlias := e.wait(t, e.start(t, sm+":PROD", `{}`))
	if aws.ToString(byAlias.Output) != "1" || aws.ToString(byAlias.StateMachineAliasArn) != sm+":PROD" || aws.ToString(byAlias.StateMachineVersionArn) != sm+":1" {
		t.Fatalf("execution through the alias = %s %v %v", aws.ToString(byAlias.Output), aws.ToString(byAlias.StateMachineAliasArn), aws.ToString(byAlias.StateMachineVersionArn))
	}
	must(e.sfn.UpdateStateMachineAlias(ctx, &sfn.UpdateStateMachineAliasInput{StateMachineAliasArn: aws.String(sm + ":PROD"),
		RoutingConfiguration: []types.RoutingConfigurationListItem{route(sm+":2", 100)}}))
	if d := e.wait(t, e.start(t, sm+":PROD", `{}`)); aws.ToString(d.Output) != "2" {
		t.Fatalf("after the alias moved: %s", aws.ToString(d.Output))
	}
	// A split with weight 0 on one side always takes the other.
	must(e.sfn.UpdateStateMachineAlias(ctx, &sfn.UpdateStateMachineAliasInput{StateMachineAliasArn: aws.String(sm + ":PROD"),
		RoutingConfiguration: []types.RoutingConfigurationListItem{route(sm+":1", 0), route(sm+":2", 100)}}))
	for range 5 {
		if d := e.wait(t, e.start(t, sm+":PROD", `{}`)); aws.ToString(d.Output) != "2" {
			t.Fatalf("weighted alias ran %s", aws.ToString(d.Output))
		}
	}
	for arn, want := range map[string]int{sm: 9, sm + ":1": 2, sm + ":2": 6, sm + ":PROD": 7} {
		if l := must(e.sfn.ListExecutions(ctx, &sfn.ListExecutionsInput{StateMachineArn: aws.String(arn)})); len(l.Executions) != want {
			t.Errorf("ListExecutions(%s) = %d, want %d", arn, len(l.Executions), want)
		}
	}
	al := must(e.sfn.ListStateMachineAliases(ctx, &sfn.ListStateMachineAliasesInput{StateMachineArn: aws.String(sm)}))
	if len(al.StateMachineAliases) != 1 || aws.ToString(al.StateMachineAliases[0].StateMachineAliasArn) != sm+":PROD" {
		t.Fatalf("aliases = %+v", al.StateMachineAliases)
	}
	if l := must(e.sfn.ListStateMachineAliases(ctx, &sfn.ListStateMachineAliasesInput{StateMachineArn: aws.String(sm + ":1")})); len(l.StateMachineAliases) != 1 {
		t.Fatalf("aliases of version 1 = %+v", l.StateMachineAliases)
	}

	// Bad requests.
	other := e.machine(t, "other", v1)
	otherV := *must(e.sfn.PublishStateMachineVersion(ctx, &sfn.PublishStateMachineVersionInput{StateMachineArn: aws.String(other)})).StateMachineVersionArn
	create := func(name string, rc ...types.RoutingConfigurationListItem) error {
		_, err := e.sfn.CreateStateMachineAlias(ctx, &sfn.CreateStateMachineAliasInput{Name: aws.String(name), RoutingConfiguration: rc})
		return err
	}
	isErr[*types.ConflictException](t, "duplicate alias", create("PROD", route(sm+":1", 100)))
	isErr[*types.ValidationException](t, "numeric alias name", create("123", route(sm+":1", 100)))
	isErr[*types.ValidationException](t, "weights below 100", create("A1", route(sm+":1", 50)))
	isErr[*types.ValidationException](t, "versions of two machines", create("A2", route(sm+":1", 50), route(otherV, 50)))
	isErr[*types.ResourceNotFound](t, "unknown version", create("A4", route(sm+":7", 100)))
	_, err = e.sfn.DeleteStateMachineVersion(ctx, &sfn.DeleteStateMachineVersionInput{StateMachineVersionArn: aws.String(sm + ":2")})
	isErr[*types.ConflictException](t, "deleting a version an alias uses", err)
	_, err = e.sfn.DescribeStateMachineAlias(ctx, &sfn.DescribeStateMachineAliasInput{StateMachineAliasArn: aws.String(sm + ":NOPE")})
	isErr[*types.ResourceNotFound](t, "unknown alias", err)
	_, err = e.sfn.UpdateStateMachine(ctx, &sfn.UpdateStateMachineInput{StateMachineArn: aws.String(sm + ":1"), Definition: aws.String(v1)})
	isErr[*types.InvalidArn](t, "updating a version", err)

	// Everything survives a save and load, and goes away with the machine.
	saved := must(e.svc.Save())
	must(0, e.svc.Load(saved))
	if d := must(e.sfn.DescribeStateMachineAlias(ctx, &sfn.DescribeStateMachineAliasInput{StateMachineAliasArn: aws.String(sm + ":PROD")})); len(d.RoutingConfiguration) != 2 {
		t.Fatalf("alias after load = %+v", d)
	}
	must(e.sfn.DeleteStateMachineAlias(ctx, &sfn.DeleteStateMachineAliasInput{StateMachineAliasArn: aws.String(sm + ":PROD")}))
	must(e.sfn.DeleteStateMachineVersion(ctx, &sfn.DeleteStateMachineVersionInput{StateMachineVersionArn: aws.String(sm + ":2")}))
	if l := must(e.sfn.ListStateMachineVersions(ctx, &sfn.ListStateMachineVersionsInput{StateMachineArn: aws.String(sm)})); len(l.StateMachineVersions) != 1 {
		t.Fatalf("versions after delete = %+v", l.StateMachineVersions)
	}
	// Version numbers are not used again: the machine is still at the revision of the deleted version 2.
	if v := must(e.sfn.PublishStateMachineVersion(ctx, &sfn.PublishStateMachineVersionInput{StateMachineArn: aws.String(sm)})); aws.ToString(v.StateMachineVersionArn) != sm+":3" {
		t.Fatalf("publish after delete = %s", aws.ToString(v.StateMachineVersionArn))
	}
	must(e.sfn.DeleteStateMachine(ctx, &sfn.DeleteStateMachineInput{StateMachineArn: aws.String(sm)}))
	_, err = e.sfn.DescribeStateMachine(ctx, &sfn.DescribeStateMachineInput{StateMachineArn: aws.String(sm + ":1")})
	isErr[*types.StateMachineDoesNotExist](t, "version of a deleted machine", err)

	// CreateStateMachine can publish the first version.
	c := must(e.sfn.CreateStateMachine(ctx, &sfn.CreateStateMachineInput{Name: aws.String("pub"), Definition: aws.String(v1), RoleArn: aws.String(role), Publish: true}))
	if aws.ToString(c.StateMachineVersionArn) != aws.ToString(c.StateMachineArn)+":1" {
		t.Fatalf("create with publish = %+v", c)
	}
}

// floci #3614: at most 1000 versions are kept per state machine.
func TestVersionQuota(t *testing.T) {
	e := setup(t)
	sm := e.machine(t, "many", passDef(0, "0"))
	for i := range 1000 {
		out, err := e.sfn.UpdateStateMachine(ctx, &sfn.UpdateStateMachineInput{StateMachineArn: aws.String(sm), Definition: aws.String(passDef(i, strconv.Itoa(i))), Publish: true})
		if err != nil || aws.ToString(out.StateMachineVersionArn) != sm+":"+strconv.Itoa(i+1) {
			t.Fatalf("version %d: %v", i+1, err)
		}
	}
	_, err := e.sfn.UpdateStateMachine(ctx, &sfn.UpdateStateMachineInput{StateMachineArn: aws.String(sm), Definition: aws.String(passDef(5, "x")), Publish: true})
	isErr[*types.ServiceQuotaExceededException](t, "the 1001st version", err)
	must(e.sfn.DeleteStateMachineVersion(ctx, &sfn.DeleteStateMachineVersionInput{StateMachineVersionArn: aws.String(sm + ":1")}))
	if _, err := e.sfn.PublishStateMachineVersion(ctx, &sfn.PublishStateMachineVersionInput{StateMachineArn: aws.String(sm)}); err != nil {
		t.Fatalf("publish after freeing a slot: %v", err)
	}
}
