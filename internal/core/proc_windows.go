package core

import (
	"os"
	"os/exec"
)

// OwnGroup does nothing on Windows: only the process itself is stopped.
func OwnGroup(cmd *exec.Cmd) {}

// InterruptGroup asks the process to stop.
func InterruptGroup(cmd *exec.Cmd) error { return cmd.Process.Signal(os.Interrupt) }

// KillGroup kills the process.
func KillGroup(cmd *exec.Cmd) { cmd.Process.Kill() }
