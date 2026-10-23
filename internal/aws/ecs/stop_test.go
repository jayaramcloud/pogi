package ecs

import "testing"

// A failed task counts against its deployment the moment it stops, not once its containers are
// gone: a reconcile in between must not see the task gone with its failure uncounted, or it
// starts one task too many and the circuit breaker trips late.
func TestStopTaskCountsAFailureAtOnce(t *testing.T) {
	s := &Service{st: newState()}
	cluster := "arn:aws:ecs:us-east-1:000000000000:cluster/c"
	dep := &Deployment{Id: "ecs-svc/1"}
	s.st.Services[key("000000000000", "us-east-1", "c", "web")] = &Svc{ServiceName: "web", ClusterArn: cluster, Deployments: []*Deployment{dep}}
	task := func(id string) *Task {
		return &Task{TaskArn: "arn:aws:ecs:us-east-1:000000000000:task/c/" + id, ClusterArn: cluster, Group: "service:web",
			StartedBy: dep.Id, DesiredStatus: "RUNNING", LastStatus: "PROVISIONING"}
	}

	failed := task("a")
	s.stopTask(failed, "TaskFailedToStart", "CannotPullContainerError")
	if dep.FailedTasks != 1 || dep.failedAt.IsZero() {
		t.Fatalf("a task that failed to start: FailedTasks %d, want 1 at once", dep.FailedTasks)
	}
	s.stopTask(failed, "TaskFailedToStart", "again")
	s.stopTask(task("b"), "UserInitiated", "stopped by the user")
	s.stopTask(task("c"), "EssentialContainerExited", "Essential container in task exited")
	if dep.FailedTasks != 2 {
		t.Errorf("FailedTasks %d, want 2: a second stop and a stop the user asked for don't count", dep.FailedTasks)
	}
}
