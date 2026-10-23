package terraform

import (
	"os"
	"reflect"
	"strings"
	"testing"
)

func readPlan(t *testing.T, name string) *Config {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := ReadPlan(data)
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return cfg
}

func byAddr(cfg *Config) map[string]*Resource {
	m := map[string]*Resource{}
	for _, r := range cfg.Resources {
		m[r.Address] = r
	}
	return m
}

func TestRealPlanCreate(t *testing.T) {
	cfg := readPlan(t, "plan-create.json")
	if cfg.Source != "plan" || cfg.Version == "" {
		t.Errorf("source %q version %q", cfg.Source, cfg.Version)
	}
	m := byAddr(cfg)
	lt := must(t, m, "aws_launch_template.worker")
	if lt.Action != "create" || lt.Attrs["instance_type"] != "t3.micro" || lt.Region != "us-east-1" || lt.Cloud != "aws" {
		t.Errorf("launch template: %+v", lt)
	}
	if _, ok := lt.Attrs["id"].(Unknown); !ok {
		t.Errorf("a value known after apply should be Unknown: %#v", lt.Attrs["id"])
	}
	svc := must(t, m, "aws_ecs_service.web")
	if !reflect.DeepEqual(svc.Deps, []string{"aws_ecs_cluster.main", "aws_ecs_task_definition.web", "aws_lb_listener.http", "aws_lb_target_group.web", "data.aws_subnets.default"}) {
		t.Errorf("ecs service deps: %v", svc.Deps)
	}
	// Terraform reads these during the plan, so they are not in resource_changes; they come from the configuration.
	if d := must(t, m, "data.aws_vpc.default"); !d.Data || d.Action != "read" {
		t.Errorf("data source: %+v", d)
	}
}

func TestRealPlanChanges(t *testing.T) {
	m := byAddr(readPlan(t, "plan-change.json"))
	for addr, want := range map[string]string{
		"aws_launch_template.worker":    "update",
		"aws_ecr_repository.app":        "replace",
		"aws_autoscaling_group.workers": "delete",
		"aws_ecs_cluster.main":          "no-op",
	} {
		if got := must(t, m, addr).Action; got != want {
			t.Errorf("%s: action %q, want %q", addr, got, want)
		}
	}
	up := must(t, m, "aws_launch_template.worker")
	if up.Attrs["instance_type"] != "t3.small" || up.Before["instance_type"] != "t3.micro" {
		t.Errorf("update after %v before %v", up.Attrs["instance_type"], up.Before["instance_type"])
	}
	del := must(t, m, "aws_autoscaling_group.workers")
	if del.Attrs["max_size"] != 2.0 || del.Region != "us-east-1" {
		t.Errorf("a deleted resource takes its attributes from before: %+v", del.Attrs["max_size"])
	}
}

// A small plan with a module, a provider region from a variable, a sensitive value and an
// unknown value inside a nested block.
const modulePlan = `{
 "format_version": "1.2", "terraform_version": "1.9.0",
 "variables": {"region": {"value": "eu-west-1"}},
 "planned_values": {"root_module": {"resources": [
   {"address": "aws_instance.app", "mode": "managed", "type": "aws_instance", "name": "app",
    "values": {"instance_type": "t3.micro", "password": "hunter2", "root_block_device": [{"volume_size": 30}]}}
  ], "child_modules": [{"address": "module.net", "resources": [
   {"address": "module.net.aws_subnet.s[\"a.b\"]", "mode": "managed", "type": "aws_subnet", "name": "s", "index": "a.b", "values": {"cidr_block": "10.0.0.0/24"}}
  ]}]}},
 "resource_changes": [
  {"address": "aws_instance.app", "mode": "managed", "type": "aws_instance", "name": "app",
   "change": {"actions": ["create"], "before": null,
    "after": {"instance_type": "t3.micro", "password": "hunter2", "root_block_device": [{"volume_size": 30}]},
    "after_unknown": {"id": true, "root_block_device": [{"iops": true}]},
    "after_sensitive": {"password": true, "root_block_device": [{}]}}},
  {"address": "module.net.aws_subnet.s[\"a.b\"]", "module_address": "module.net", "mode": "managed", "type": "aws_subnet", "name": "s",
   "change": {"actions": ["create"], "after": {"cidr_block": "10.0.0.0/24"}, "after_unknown": {"id": true}}}
 ],
 "configuration": {
  "provider_config": {"aws": {"name": "aws", "expressions": {"region": {"references": ["var.region"]}}}},
  "root_module": {
   "resources": [{"address": "aws_instance.app", "mode": "managed", "type": "aws_instance", "name": "app", "provider_config_key": "aws",
     "expressions": {"subnet_id": {"references": ["module.net.subnet_id", "module.net"]}, "tags": {"n": {"references": ["local.x"]}}}}],
   "module_calls": {"net": {"expressions": {"vpc": {"references": ["aws_vpc.main.id", "aws_vpc.main"]}},
     "module": {"resources": [{"address": "aws_subnet.s", "mode": "managed", "type": "aws_subnet", "name": "s", "provider_config_key": "aws",
       "expressions": {"vpc_id": {"references": ["var.vpc"]}}}],
      "outputs": {"subnet_id": {"expression": {"references": ["aws_subnet.s.id", "aws_subnet.s"]}}}}}}}
 }}`

func TestPlanModulesMasksAndRegion(t *testing.T) {
	cfg, err := ReadPlan([]byte(modulePlan))
	if err != nil {
		t.Fatal(err)
	}
	m := byAddr(cfg)
	app := must(t, m, "aws_instance.app")
	if app.Region != "eu-west-1" {
		t.Errorf("region through a variable: %q", app.Region)
	}
	if _, ok := app.Attrs["password"]; ok {
		t.Error("a sensitive value should be dropped")
	}
	if _, ok := app.Attrs["id"].(Unknown); !ok {
		t.Errorf("id: %#v", app.Attrs["id"])
	}
	rbd := app.Attrs["root_block_device"].([]any)[0].(map[string]any)
	if rbd["volume_size"] != 30.0 {
		t.Errorf("nested block: %#v", rbd)
	}
	if _, ok := rbd["iops"].(Unknown); !ok {
		t.Errorf("an unknown nested value: %#v", rbd)
	}
	s := must(t, m, `module.net.aws_subnet.s["a.b"]`)
	if s.Block != "module.net.aws_subnet.s" || s.Module != "module.net" {
		t.Errorf("module resource: %+v", s)
	}
}

func TestPlanDepsThroughModules(t *testing.T) {
	// aws_vpc.main is not a resource in the plan, so only known blocks are kept.
	cfg, _ := ReadPlan([]byte(strings.Replace(modulePlan, `"resource_changes": [`,
		`"resource_changes": [{"address": "aws_vpc.main", "mode": "managed", "type": "aws_vpc", "name": "main", "change": {"actions": ["no-op"], "after": {}}},`, 1)))
	m := byAddr(cfg)
	if got := must(t, m, "aws_instance.app").Deps; !reflect.DeepEqual(got, []string{"module.net.aws_subnet.s"}) {
		t.Errorf("deps through a module's output: %v", got)
	}
	if got := must(t, m, `module.net.aws_subnet.s["a.b"]`).Deps; !reflect.DeepEqual(got, []string{"aws_vpc.main"}) {
		t.Errorf("deps through a module's input: %v", got)
	}
}

func TestPlanErrors(t *testing.T) {
	for name, in := range map[string]string{
		"not json":         `hello`,
		"not a plan":       `{"resource": {"aws_instance": {}}}`,
		"old format":       `{"format_version": "0.2", "planned_values": {}}`,
		"state not a plan": `{"format_version": "1.0", "values": {}}`,
	} {
		_, err := ReadPlan([]byte(in))
		if err == nil || !strings.Contains(err.Error(), "terraform show -json plan.out > plan.json") {
			t.Errorf("%s: want an error with the hint, got %v", name, err)
		}
	}
	if IsPlan([]byte(`[]`)) || IsPlan([]byte(`{"format_version":"1.0"}`)) || !IsPlan([]byte(modulePlan)) {
		t.Error("IsPlan")
	}
}

func TestLoadPlanFile(t *testing.T) {
	cfg, err := Load("testdata/plan-create.json", nil)
	if err != nil || cfg.Source != "plan" {
		t.Errorf("Load of a plan: %v", err)
	}
	if _, err := Load("testdata/err_empty/readme.txt", nil); err == nil || !strings.Contains(err.Error(), "plan JSON") {
		t.Errorf("a file that is not a plan: %v", err)
	}
}

func TestStripKeys(t *testing.T) {
	if got := stripKeys(`module.a["x]y"].aws_x.y[0]`); got != "module.a.aws_x.y" {
		t.Errorf("stripKeys: %q", got)
	}
	typ, name, mode, mod := splitAddress(`module.a["k"].module.b.data.aws_x.y["z"]`)
	if typ != "aws_x" || name != "y" || mode != "data" || mod != `module.a["k"].module.b` {
		t.Errorf("splitAddress: %q %q %q %q", typ, name, mode, mod)
	}
}

// A real plan that deletes a group: the group is gone from the configuration, so its references
// come from prior_state's depends_on.
func TestRealPlanDeleteKeepsDeps(t *testing.T) {
	m := byAddr(readPlan(t, "plan-delete.json"))
	del := must(t, m, "aws_autoscaling_group.workers")
	if del.Action != "delete" || !reflect.DeepEqual(del.Deps, []string{"aws_launch_template.worker", "data.aws_subnets.default", "data.aws_vpc.default"}) {
		t.Errorf("deleted group: action %q deps %v", del.Action, del.Deps)
	}
}
