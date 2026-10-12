package raftlog

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
)

func TestApplicationReceivingProcessCrashAndUncertainSync(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL process seam")
	}
	if phase := os.Getenv("RHO_RECEIVING_PHASE"); phase != "" {
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if phase == "write" && op.Kind == errorfs.OpFileWrite || phase == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				return errorfs.ErrInjected
			}
			return nil
		})}
		cfg := receivingTestConfig(t, errorfs.Wrap(vfs.Default, toggle), false)
		cfg.Dir = os.Getenv("RHO_RECEIVING_DIR")
		s := receivingTestOpen(t, cfg)
		if err := s.PersistApplicationReceiving(receivingTerm(3)); err != nil {
			t.Fatal(err)
		}
		if phase == "before-write" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
			t.Fatal("before-write kill returned")
		}
		if phase == "write" || phase == "sync" {
			toggle.On()
		}
		if err := s.PersistApplicationReceiving(receivingTerm(7)); err != nil {
			t.Fatal(err)
		}
		// This records only a completed local sync, never a network ACK, quorum,
		// graph operation or replacement admission. Failure must not reach it.
		if _, err := os.Stdout.WriteString("RECEIVING_TERM_SYNC_RETURNED\n"); err != nil {
			t.Fatal(err)
		}
		if err := snapshotKill(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("after-return kill returned")
	}
	for _, phase := range []string{"before-write", "after-return", "write", "sync"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestApplicationReceivingProcessCrashAndUncertainSync$", "-test.count=1")
			child.Env = append(os.Environ(), "RHO_RECEIVING_PHASE="+phase, "RHO_RECEIVING_DIR="+dir)
			output, err := child.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || ctx.Err() != nil || child.ProcessState == nil {
				t.Fatalf("receiving process seam not terminal/reaped: %v %s", err, output)
			}
			returned := bytes.Contains(output, []byte("RECEIVING_TERM_SYNC_RETURNED"))
			if phase == "write" || phase == "sync" {
				if exit.ExitCode() != 1 || !bytes.Contains(output, []byte("fatal commit error")) || returned {
					t.Fatalf("uncertain durable failure returned success: %v %s", err, output)
				}
			} else {
				wait, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !wait.Signaled() || wait.Signal() != syscall.SIGKILL || returned != (phase == "after-return") {
					t.Fatalf("actual receiving kill/sync boundary absent: %v %s", err, output)
				}
			}
			cfg := receivingTestConfig(t, vfs.Default, false)
			cfg.Dir, cfg.Create = dir, false
			s := receivingTestOpen(t, cfg)
			state, err := s.ApplicationReceivingState()
			if err != nil || state.Term != 3 && state.Term != 7 || phase == "before-write" && state.Term != 3 || phase == "after-return" && state.Term != 7 {
				t.Fatal("receiving term did not recover whole allowed state", phase, state, err)
			}
			hard, conf, err := s.InitialState()
			if err != nil || hard.GetTerm() != state.Term || hard.GetVote() != 0 || hard.GetCommit() != 0 || !receivingConfEmpty(conf) {
				t.Fatal("uncertain term persisted vote/commit/membership", hard, conf, err)
			}
			if index, image, err := s.Checkpoint(); err != nil || index != 0 || len(image) != 0 {
				t.Fatal("uncertain term persisted application", index, image, err)
			}
			if err := s.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, ErrInvalid) {
				t.Fatal("crashed receiving identity bootstrapped", err)
			}
		})
	}
}
