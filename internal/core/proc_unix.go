//go:build !windows

package core

import (
	"os/exec"
	"syscall"
)

// OwnGroup makes cmd start in its own process group, so StopGroup reaches
// the children it starts too (a Functions host's workers, for one). Call it
// before cmd.Start.
func OwnGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// InterruptGroup asks the whole group of cmd to stop (SIGINT).
func InterruptGroup(cmd *exec.Cmd) error {
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
}

// KillGroup kills the whole group of cmd (SIGKILL); it is safe to call again.
func KillGroup(cmd *exec.Cmd) {
	if syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) != nil {
		cmd.Process.Kill()
	}
}
