package replica

import (
	"errors"
	"os"

	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/testutil"
)

// Pebble's documented default fatal logger exits the process on WAL commit
// failures. Test this boundary in a child, never replace Fatalf with a no-op.
// OS-cache survival makes the interrupted outcome UNKNOWN, not certainly absent.
func TestFatalStorageReplicaStopsBeforeOutput(t *testing.T) {
	if fault := os.Getenv("RHO_RAFT_FATAL_FAULT"); fault != "" {
		dir := os.Getenv("RHO_RAFT_FATAL_DIR")
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if fault == "write" && op.Kind == errorfs.OpFileWrite || fault == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				return errorfs.ErrInjected
			}
			return nil
		})}
		s, err := raftlog.Open(raftlog.Config{Dir: dir, FS: errorfs.Wrap(vfs.Default, toggle), Create: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1}, nil); err != nil {
			t.Fatal(err)
		}
		d, err := Open(Config{ID: 1, Store: s, Machine: &scalar{}})
		if err != nil {
			t.Fatal(err)
		}
		packets(t, d.Campaign)
		packets(t, func() (Output, error) { return d.Propose(command(17)) })
		// The prior acknowledged/applied scalar effect must survive every fault.
		if err := d.SaveCheckpoint(); err != nil {
			t.Fatal(err)
		}
		toggle.On()
		out, err := d.Propose(command(31))
		// Reaching this marker would mean a fatal error was improperly contained.
		_, _ = os.Stdout.WriteString("FAILED_READY_RETURNED_OUTPUT\n")
		t.Fatalf("fatal call returned: %v %v", out, err)
	}
	for _, fault := range []string{"write", "sync"} {
		t.Run(fault, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			r := testutil.RunChild(t, "TestFatalStorageReplicaStopsBeforeOutput", []string{"RHO_RAFT_FATAL_FAULT=" + fault, "RHO_RAFT_FATAL_DIR=" + dir}, 10*time.Second)
			if r.Exit == nil || r.ExitCode != 1 || !r.Contains("fatal commit error") || r.Contains("FAILED_READY_RETURNED_OUTPUT") {
				t.Fatalf("fail-stop contract: %v %s", r.Err, r.Output)
			}
			s, err := raftlog.Open(raftlog.Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Scrub(); err != nil {
				t.Fatal(err)
			}
			m := &scalar{}
			d, err := Open(Config{ID: 1, Store: s, Machine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			packets(t, d.Tick)
			// The failed proposal has no receipt; WAL bytes may or may not survive.
			if m.value != 17 && m.value != 48 {
				t.Fatalf("lost prior acknowledged effect or invented state: %d", m.value)
			}
		})
	}
}

func TestOpenVFSFaultsPreserveAcknowledgedPrefix(t *testing.T) {
	if kind := os.Getenv("RHO_RAFT_OPEN_FAULT"); kind != "" {
		dir := os.Getenv("RHO_RAFT_FATAL_DIR")
		s, err := raftlog.Open(raftlog.Config{Dir: dir, Create: true})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1}, nil); err != nil {
			t.Fatal(err)
		}
		d, err := Open(Config{ID: 1, Store: s, Machine: &scalar{}})
		if err != nil {
			t.Fatal(err)
		}
		packets(t, d.Campaign)
		packets(t, func() (Output, error) { return d.Propose(command(17)) })
		if err := d.SaveCheckpoint(); err != nil {
			t.Fatal(err)
		}
		if err := d.Close(); err != nil {
			t.Fatal(err)
		}
		var injected sync.Once
		fault := errorfs.InjectorFunc(func(op errorfs.Op) error {
			match := kind == "create" && op.Kind == errorfs.OpCreate || kind == "rename" && op.Kind == errorfs.OpRename || kind == "opendir" && op.Kind == errorfs.OpOpenDir || kind == "enospc" && op.Kind == errorfs.OpCreate
			if match {
				injected.Do(func() { _, _ = os.Stdout.WriteString("INJECTED_OPEN_FAULT_" + kind + "\n") })
				if kind == "enospc" {
					return syscall.ENOSPC
				}
				return errorfs.ErrInjected
			}
			return nil
		})
		opened, err := raftlog.Open(raftlog.Config{Dir: dir, FS: errorfs.Wrap(vfs.Default, fault)})
		if err != nil {
			if kind == "enospc" && !errors.Is(err, syscall.ENOSPC) || kind != "enospc" && !errors.Is(err, errorfs.ErrInjected) {
				t.Fatal("lost sentinel", err)
			}
			_, _ = os.Stdout.WriteString("EXPECTED_OPEN_ERROR\n")
			os.Exit(2)
		}
		if opened != nil {
			opened.Close()
		}
		t.Fatal("scheduled open fault did not fire")
	}
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, kind := range []string{"create", "rename", "opendir", "enospc"} {
		t.Run(kind, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			r := testutil.RunChild(t, "TestOpenVFSFaultsPreserveAcknowledgedPrefix", []string{"RHO_RAFT_OPEN_FAULT=" + kind, "RHO_RAFT_FATAL_DIR=" + dir}, 5*time.Second)
			if !r.Contains("INJECTED_OPEN_FAULT_" + kind + "\n") {
				t.Fatalf("child never reached scheduled fault: %v %s", r.Err, r.Output)
			}
			if r.TimedOut {
				t.Logf("%s fault: pinned Pebble open required supervisor kill after deadline; no success/packet returned", kind)
			} else if r.Exit == nil || r.ExitCode != 2 || !r.Contains("EXPECTED_OPEN_ERROR") {
				t.Fatalf("open fault did not prevent success: %v %s", r.Err, r.Output)
			}
			s, err := raftlog.Open(raftlog.Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			m := &scalar{}
			d, err := Open(Config{ID: 1, Store: s, Machine: m})
			if err != nil {
				t.Fatal(err)
			}
			defer d.Close()
			packets(t, d.Tick)
			if m.value != 17 {
				t.Fatal("acknowledged prefix lost", m.value)
			}
			if err := s.Scrub(); err != nil {
				t.Fatal(err)
			}
		})
	}
}
