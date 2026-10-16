package replica

import (
	"os"

	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/testutil"
)

func TestApplicationReceiverNativePersistFailureCannotReturnPackets(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL seam")
	}
	if phase := os.Getenv("RHO_RECEIVER_PHASE"); phase != "" {
		_, original, barrier, cut := receiverNativeAppendFixture(t)
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if phase == "write" && op.Kind == errorfs.OpFileWrite || phase == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				return errorfs.ErrInjected
			}
			return nil
		})}
		s, _ := receiverTestStoreAt(t, errorfs.Wrap(vfs.Default, toggle), os.Getenv("RHO_RECEIVER_DIR"))
		r := receiverTestOpen(t, receiverTestConfig(s, cut.Term))
		if barrier.Index == 0 || cut.Index < barrier.Index {
			t.Fatal("actual source barrier/cut absent")
		}
		if phase == "write" || phase == "sync" {
			toggle.On()
		}
		out, err := r.StepReceiving(original)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != 1 {
			t.Fatal("actual response packet absent")
		}
		if _, err := os.Stdout.WriteString("RECEIVER_RETURNED_PACKETS_AFTER_LOCAL_SYNC\n"); err != nil {
			t.Fatal(err)
		}
		if err := syscall.Kill(os.Getpid(), syscall.SIGKILL); err != nil {
			t.Fatal(err)
		}
		t.Fatal("after-return kill returned")
	}
	for _, phase := range []string{"write", "sync", "after-return"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "receiver")
			r := testutil.RunChild(t, "TestApplicationReceiverNativePersistFailureCannotReturnPackets", []string{"RHO_RECEIVER_PHASE=" + phase, "RHO_RECEIVER_DIR=" + dir}, 30*time.Second, "-test.count=1")
			if r.Exit == nil || r.TimedOut || !r.Reaped {
				t.Fatalf("receiver process not terminal/reaped: %v %s", r.Err, r.Output)
			}
			returned := r.Contains("RECEIVER_RETURNED_PACKETS_AFTER_LOCAL_SYNC")
			if phase == "after-return" {
				if !r.Killed() || !returned {
					t.Fatalf("actual post-sync kill absent: %v %s", r.Err, r.Output)
				}
			} else if r.ExitCode != 1 || !r.Contains("fatal commit error") || returned {
				t.Fatalf("failed term persistence returned packets: %v %s", r.Err, r.Output)
			}
			// Reopen the real target volume; an uncertain write/sync may recover
			// old OR new whole term. It must never install vote/commit/application.
			_, cfg := receiverTestStore(t, vfs.NewMem())
			cfg.Dir, cfg.Create, cfg.FS = dir, false, vfs.Default
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			state, err := s.ApplicationReceivingState()
			if err != nil || state.Term != 0 && state.Term != 2 || phase == "after-return" && state.Term != 2 {
				t.Fatal("receiving term not old/new whole state", phase, state, err)
			}
			hard, conf, err := s.InitialState()
			if err != nil || hard.GetTerm() != state.Term || hard.GetVote() != 0 || hard.GetCommit() != 0 || len(conf.GetVoters()) != 0 {
				t.Fatal("failed receiving step installed authority", hard, conf, err)
			}
			t.Logf("receiving child phase=%s PID=%d reaped=true exit=%d waitStatus=%d signaled=%t signal=%d localPacketsReturned=%t recoveredTerm=%d; no network delivery/quorum/activation", phase, r.PID, r.Exit.ExitCode(), int(r.Status), r.Status.Signaled(), int(r.Status.Signal()), returned, state.Term)
		})
	}
}
