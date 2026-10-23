package core

import (
	"os"

	"golang.org/x/sys/windows"
)

// lockFile takes an exclusive lock on f without waiting. The system drops it
// when the process ends, so a crashed pogi leaves no stale lock.
func lockFile(f *os.File) error {
	return windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, new(windows.Overlapped))
}
