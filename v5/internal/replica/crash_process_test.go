package replica

import (
	"os"
	"syscall"
	"time"
)

// SIGKILL delivery is asynchronous. After requesting it, the crash seam must
// perform no more work or cleanup before OS death. Timer sleeps prevent a Go
// deadlock exit; the parent subprocess deadline remains the failure bound.
func killSelfForCrashTest() error {
	if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
		return err
	}
	for {
		time.Sleep(time.Hour)
	}
}
