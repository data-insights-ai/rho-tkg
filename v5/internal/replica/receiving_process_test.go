package replica

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
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
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestApplicationReceiverNativePersistFailureCannotReturnPackets$", "-test.count=1")
			child.Env = append(os.Environ(), "RHO_RECEIVER_PHASE="+phase, "RHO_RECEIVER_DIR="+dir)
			output, err := child.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || ctx.Err() != nil || child.ProcessState == nil {
				t.Fatalf("receiver process not terminal/reaped: %v %s", err, output)
			}
			wait, ok := exit.Sys().(syscall.WaitStatus)
			if !ok {
				t.Fatal("actual receiver WaitStatus absent")
			}
			returned := bytes.Contains(output, []byte("RECEIVER_RETURNED_PACKETS_AFTER_LOCAL_SYNC"))
			if phase == "after-return" {
				if !wait.Signaled() || wait.Signal() != syscall.SIGKILL || !returned {
					t.Fatalf("actual post-sync kill absent: %v %s", err, output)
				}
			} else if exit.ExitCode() != 1 || !bytes.Contains(output, []byte("fatal commit error")) || returned {
				t.Fatalf("failed term persistence returned packets: %v %s", err, output)
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
			t.Logf("receiving child phase=%s PID=%d reaped=true exit=%d waitStatus=%d signaled=%t signal=%d localPacketsReturned=%t recoveredTerm=%d; no network delivery/quorum/activation", phase, child.Process.Pid, exit.ExitCode(), int(wait), wait.Signaled(), int(wait.Signal()), returned, state.Term)
		})
	}
}
