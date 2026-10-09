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
	"go.etcd.io/raft/v3"
	pb "go.etcd.io/raft/v3/raftpb"
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
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestApplicationSnapshotActivationProcessCrashAndSyncFault$")
			child.Env = append(os.Environ(), "RHO_ACTIVATION_PHASE="+phase, "RHO_ACTIVATION_DIR="+dir)
			output, err := child.CombinedOutput()
			exit, ok := errors.AsType[*exec.ExitError](err)
			if !ok || ctx.Err() != nil {
				t.Fatalf("seam incomplete: %v %s", err, output)
			}
			if phase == "write" || phase == "sync" {
				if exit.ExitCode() != 1 || !bytes.Contains(output, []byte("fatal commit error")) || bytes.Contains(output, []byte("ACTIVATION_ROOT_RETURNED")) {
					t.Fatalf("uncertain activation returned root: %v %s", err, output)
				}
			} else {
				status, ok := exit.Sys().(syscall.WaitStatus)
				if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
					t.Fatalf("not SIGKILL: %v %s", err, output)
				}
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
