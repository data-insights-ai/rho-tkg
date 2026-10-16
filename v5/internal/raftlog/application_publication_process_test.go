package raftlog

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2/vfs"
	"github.com/cockroachdb/pebble/v2/vfs/errorfs"

	"github.com/data-insights-ai/rho-tkg/v5/internal/testutil"
)

func TestApplicationPublicationCrashAndFaultNeverExposeMixedBase(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("SIGKILL seam")
	}
	if phase := os.Getenv("RHO_PUBLISHED_CUT_CRASH"); phase != "" {
		p, tc := transferConfig(1)
		p.ReclaimEntries = 1
		toggle := &errorfs.Toggle{Injector: errorfs.InjectorFunc(func(op errorfs.Op) error {
			if phase == "write" && op.Kind == errorfs.OpFileWrite || phase == "sync" && (op.Kind == errorfs.OpFileSync || op.Kind == errorfs.OpFileSyncData) {
				return errorfs.ErrInjected
			}
			return nil
		})}
		s, err := Open(Config{Dir: os.Getenv("RHO_PUBLISHED_CUT_DIR"), FS: errorfs.Wrap(vfs.Default, toggle), Create: true, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 1_000_000}})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Initialize([]uint64{1}, []byte("initial")); err != nil {
			t.Fatal(err)
		}
		generationApply(t, s, "old", KV{Key: []byte("a"), Value: []byte("old")})
		if err := s.PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		generationApply(t, s, "new", KV{Key: []byte("a"), Value: []byte("new")})
		s.publicationCaptureHook = func() {
			if phase == "captured" {
				if err := snapshotKill(); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "write" || phase == "sync" {
				toggle.On()
			}
		}
		if err := s.PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		if phase == "synced" {
			if err := snapshotKill(); err != nil {
				t.Fatal(err)
			}
		}
		_, _ = os.Stdout.WriteString("PUBLICATION_FAULT_RETURNED\n")
		t.Fatal("crash seam returned")
	}
	for _, phase := range []string{"captured", "synced", "write", "sync"} {
		t.Run(phase, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "db")
			r := testutil.RunChild(t, "TestApplicationPublicationCrashAndFaultNeverExposeMixedBase", []string{"RHO_PUBLISHED_CUT_CRASH=" + phase, "RHO_PUBLISHED_CUT_DIR=" + dir}, 30*time.Second)
			if r.Exit == nil || r.TimedOut {
				t.Fatalf("crash seam incomplete: %v %s", r.Err, r.Output)
			}
			if phase == "write" || phase == "sync" {
				if r.ExitCode != 1 || !r.Contains("fatal commit error") || r.Contains("PUBLICATION_FAULT_RETURNED") {
					t.Fatalf("uncertain publication returned: %v %s", r.Err, r.Output)
				}
			} else if !r.Killed() {
				t.Fatalf("not SIGKILL: %v %s", r.Err, r.Output)
			}
			p, tc := transferConfig(1)
			p.ReclaimEntries = 1
			s, err := Open(Config{Dir: dir, Application: p, Transfer: tc, Generations: ApplicationGenerationLimits{MaxBytes: 2 << 30, MaxRecords: 2_000_000}, PublishedCuts: ApplicationPublishedCutLimits{MaxTransferChunks: 1_000_000}})
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			cut, err := s.PublishedApplicationCut()
			if err != nil {
				t.Fatal(err)
			}
			if cut.Index != 2 && cut.Index != 3 || phase == "captured" && cut.Index != 2 || phase == "synced" && cut.Index != 3 {
				t.Fatal("unexpected publication boundary", cut.Index)
			}
			want := "old"
			if cut.Index == 3 {
				want = "new"
			}
			if cut.ImageHash != sha256.Sum256([]byte(want)) {
				t.Fatal("published cut/image mixed", cut)
			}
			snapshot, err := s.Snapshot()
			if err != nil || snapshot.GetMetadata().GetIndex() != cut.Index || string(snapshot.Data) != want {
				t.Fatal("snapshot/ref mixed", snapshot, err)
			}
			first, err := s.FirstIndex()
			if err != nil || first != cut.Index+1 {
				t.Fatal("prefix/ref mixed", first, err)
			}
			index, image, err := s.Checkpoint()
			if err != nil || index != 3 || string(image) != "new" {
				t.Fatal("publication rolled checkpoint back", index, string(image), err)
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
