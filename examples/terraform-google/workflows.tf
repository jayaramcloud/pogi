# Cloud Workflows: a workflow whose YAML pogi runs when an execution starts.
resource "google_workflows_workflow" "workflows_demo" {
  name                = "workflows-demo"
  region              = local.region
  description         = "assign, a switch and a return"
  deletion_protection = false
  source_contents     = <<-YAML
    main:
      params: [args]
      steps:
        - init:
            assign:
              - name: $${default(map.get(args, "name"), "world")}
        - pick:
            switch:
              - condition: $${name == "world"}
                return: "hello, world"
        - greet:
            return: $${"hello, " + name}
  YAML
}
