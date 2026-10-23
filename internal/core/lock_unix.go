//go:build !windows

package core

import (
	"os"
	"syscall"
)

// lockFile takes an exclusive lock on f without waiting. The system drops it
// when the process ends, so a crashed pogi leaves no stale lock.
func lockFile(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) }
