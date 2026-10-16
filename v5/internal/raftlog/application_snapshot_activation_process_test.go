package raftlog

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"

	"github.com/data-insights-ai/rho-tkg/v5/internal/testutil"
)

func TestApplicationSnapshotActivationProcessCrashAndSyncFault(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL seam")
	}
	if phase := os.Getenv("RHO_ACTIVATION_PHASE"); phase != "" {
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if phase == "write" && op.Kind == errorfs.OpFileWrite || phase == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				return errorfs.ErrInjected
			}
			return nil
		})}
		s := receiverStoreAt(t, os.Getenv("RHO_ACTIVATION_DIR"), errorfs.Wrap(vfs.Default, toggle), 1)
		_, p, snap := preparedFixture(t, s)
		if phase == "prepared" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
		}
		claim, err := p.Claim(snap)
		if err != nil {
			t.Fatal(err)
		}
		if phase == "claimed" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
		}
		s.activationCommitHook = func() {
			if phase == "before-sync" {
				if err := snapshotKill(); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "write" || phase == "sync" {
				toggle.On()
			}
		}
		s.activationAfterSyncHook = func() {
			if phase == "after-sync" {
				if err := snapshotKill(); err != nil {
					t.Fatal(err)
				}
			}
		}
		root, err := s.PersistApplicationReady(raft.Ready{Snapshot: snap, HardState: &pb.HardState{Term: new(uint64(5)), Vote: new(uint64(2)), Commit: new(uint64(2))}}, claim)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = os.Stdout.WriteString("ACTIVATION_ROOT_RETURNED\n")
		if root.Index != 2 || string(root.Image) != "incoming" {
			t.Fatal(root)
		}
		if err := snapshotKill(); err != nil {
			t.Fatal(err)
		}
		t.Fatal("process seam returned")
	}
	for _, phase := range []string{"prepared", "claimed", "before-sync", "after-sync", "root", "write", "sync"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			r := testutil.RunChild(t, "TestApplicationSnapshotActivationProcessCrashAndSyncFault", []string{"RHO_ACTIVATION_PHASE=" + phase, "RHO_ACTIVATION_DIR=" + dir}, 30*time.Second)
			if r.Exit == nil || r.TimedOut {
				t.Fatalf("seam incomplete: %v %s", r.Err, r.Output)
			}
			if phase == "write" || phase == "sync" {
				if r.ExitCode != 1 || !r.Contains("fatal commit error") || r.Contains("ACTIVATION_ROOT_RETURNED") {
					t.Fatalf("uncertain activation returned root: %v %s", r.Err, r.Output)
				}
			} else if !r.Killed() {
				t.Fatalf("not SIGKILL: %v %s", r.Err, r.Output)
			}
			p, tc := transferConfig(1)
			s, err := Open(Config{Dir: dir, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{2 << 30, 2000000}, PublishedCuts: ApplicationPublishedCutLimits{1000000}, Replication: ApplicationReplicationConfig{[3]uint64{1, 2, 3}}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			index, image, err := s.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if index != 1 && index != 2 || phase == "after-sync" && index != 2 || phase == "root" && index != 2 || (phase == "prepared" || phase == "claimed" || phase == "before-sync") && index != 1 {
				t.Fatal("mixed activation boundary", index, phase)
			}
			if index == 1 {
				if string(image) != "initial" || s.activeBank() != 0 || s.meta.Hard.GetVote() != 0 || s.meta.Rep.LastActivatedManifestID != [32]byte{} {
					t.Fatal("old state mixed", s.meta, string(image))
				}
			} else {
				if string(image) != "incoming" || s.activeBank() != 1 || s.meta.Hard.GetVote() != 2 || s.meta.App.Policy.LocalVoter != 1 || s.meta.Rep.LastActivatedManifestID == [32]byte{} {
					t.Fatal("new state mixed", s.meta, string(image))
				}
			}
			assertActivationKV(t, s, index, index == 2)
			cut, err := s.PublishedApplicationCut()
			if err != nil || cut.Index != index || cut.ImageHash != s.meta.ImageHash {
				t.Fatal("cut/image mixed", cut, err)
			}
			if len(s.applicationSnapshotClaims) != 0 || s.applicationImport != nil || s.pinnedApplicationBytes != 0 {
				t.Fatal("reopen resurrected volatile authority")
			}
			if err := s.Scrub(); err != nil {
				t.Fatal(err)
			}
			if err := s.ScrubApplication(t.Context()); err != nil {
				t.Fatal(err)
			}
		})
	}
}
