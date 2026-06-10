package tiered

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Full lifecycle of the sticky background error: poison → fail closed →
// recovery refused while persistence is still broken → recovery succeeds
// once healed → store fully usable WITHOUT a close/re-open cycle.
func TestRecoverBackgroundErrorLifecycle(t *testing.T) {
	// No t.Parallel(): the test chmods a directory; keep failure modes local.

	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1, // disable periodic flush
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()
	caseTok, _, _ := installDefaultTestLabelRegistry(t, ts)
	gen := newTestGen(t, 0)

	// Healthy store: recovery is a no-op.
	if err := ts.RecoverBackgroundError(); err != nil {
		t.Fatalf("RecoverBackgroundError on healthy store = %v, want nil", err)
	}

	// Baseline write proves the store works before poisoning.
	before := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(before); err != nil {
		t.Fatalf("baseline PutNode: %v", err)
	}

	// Poison: simulate an idle-shard close failure (the real producer of
	// background errors) and assert the store fails closed on reads AND
	// writes with the recorded cause.
	injected := errors.New("injected idle-close failure")
	ts.recordBackgroundError(injected)

	poisonedNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(poisonedNode); !errors.Is(err, injected) {
		t.Fatalf("PutNode on poisoned store = %v, want the injected error", err)
	}
	if _, err := ts.GetNode(before.ID()); !errors.Is(err, injected) {
		t.Fatalf("GetNode on poisoned store = %v, want the injected error", err)
	}

	// Recovery while persistence is STILL broken must fail and keep the
	// gate: make the catalog directory unwritable so the probe save fails.
	metaDir := filepath.Join(dir, "meta")
	if err := os.Chmod(metaDir, 0o555); err != nil {
		t.Fatalf("chmod meta dir: %v", err)
	}
	restored := false
	restore := func() {
		if !restored {
			restored = true
			if err := os.Chmod(metaDir, 0o755); err != nil {
				t.Fatalf("restore meta dir perms: %v", err)
			}
		}
	}
	defer restore()

	if err := ts.RecoverBackgroundError(); err == nil {
		t.Fatalf("RecoverBackgroundError succeeded while the catalog dir is unwritable; the probe is not probing")
	} else if !errors.Is(err, injected) {
		t.Fatalf("failed recovery lost the original cause: %v", err)
	}
	if err := ts.PutNode(poisonedNode); !errors.Is(err, injected) {
		t.Fatalf("store recovered despite failed probe: PutNode = %v", err)
	}

	// Heal the filesystem; recovery must clear the gate.
	restore()
	if err := ts.RecoverBackgroundError(); err != nil {
		t.Fatalf("RecoverBackgroundError after heal = %v, want nil", err)
	}

	// Fully usable without reopen: writes land, prior data still readable.
	after := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(after); err != nil {
		t.Fatalf("PutNode after recovery: %v", err)
	}
	got, err := ts.GetNode(before.ID())
	if err != nil {
		t.Fatalf("GetNode(before) after recovery: %v", err)
	}
	if got.ID() != before.ID() || got.PrimaryLabelToken() != before.PrimaryLabelToken() {
		t.Fatalf("pre-poison node mutated across recovery: %v", got.ID())
	}
	if _, err := ts.GetNode(after.ID()); err != nil {
		t.Fatalf("GetNode(after) post recovery: %v", err)
	}

	// Idempotent once healthy.
	if err := ts.RecoverBackgroundError(); err != nil {
		t.Fatalf("second RecoverBackgroundError = %v, want nil", err)
	}
}
