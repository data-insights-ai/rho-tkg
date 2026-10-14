package badger

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Final review of backlog 56.

// crashReopen flushes, closes without an open image (a crash) and reopens.
func crashReopen(t *testing.T, bs *Store, cfg Config) *Store {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	bs.openImageSuppressed.Store(true)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

func labelIDsAt(t *testing.T, bs *Store, at types.Instant) []types.NodeID {
	t.Helper()
	got, err := bs.NodesByLabel(7, QueryOpts{ValidAt: at})
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]types.NodeID, len(got))
	for i, n := range got {
		ids[i] = n.ID()
	}
	return ids
}

// Finding 1: a PutNodeVersion during Phase 2 marks the entry Mutated without
// writing its current 0x0B row; Phase 3 skipped the disk Set, so a crash
// reopen rebuilt the index without the node.
func TestNodeTemporalEnvelope_PutNodeVersionDuringBuildOnDisk(t *testing.T) {
	cfg := Config{Dir: t.TempDir(), TemporalIndexOnDisk: true, FlushInterval: time.Hour}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nid := types.NodeID(snowflake.ID(8050))
	if err := bs.PutNode(envNode(nid, 5, 5000, 6000, 7)); err != nil {
		t.Fatal(err)
	}
	temporalCreatePhase2Hook = func() {
		if err := bs.PutNodeVersion(nid, 1, envNode(nid, 1, 100, 200, 7)); err != nil {
			t.Error(err)
		}
	}
	defer func() { temporalCreatePhase2Hook = nil }()
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	temporalCreatePhase2Hook = nil
	bs = crashReopen(t, bs, cfg)
	if ids := labelIDsAt(t, bs, 5500); len(ids) != 1 {
		t.Errorf("NodesByLabel(7, ValidAt 5500) = %v, want [%d]", ids, nid)
	}
	nodeMustKeep(t, bs, nid, 5500)
}

// Finding 3: a member that loses the label during Phase 2 must not get a 0x0B
// row; a re-add with another from would leave two rows and a crash reopen
// returned the node twice.
func TestNodeTemporalEnvelope_LabelLostDuringBuildOnDisk(t *testing.T) {
	cfg := Config{Dir: t.TempDir(), TemporalIndexOnDisk: true, FlushInterval: time.Hour}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nid := types.NodeID(snowflake.ID(8051))
	v0 := envNode(nid, 0, 5000, 6000, 8, 7)
	if err := bs.PutNode(v0); err != nil {
		t.Fatal(err)
	}
	temporalCreatePhase2Hook = func() {
		if err := bs.RemoveNodeLabelTokenWithHistory(nid, 7, envNode(nid, 1, 5000, 6000, 8), 0, v0); err != nil {
			t.Error(err)
		}
	}
	defer func() { temporalCreatePhase2Hook = nil }()
	if err := bs.CreateTemporalIndex(7); err != nil {
		t.Fatal(err)
	}
	temporalCreatePhase2Hook = nil
	if err := bs.AddNodeLabelToken(nid, 7, envNode(nid, 2, 5200, 0, 8, 7)); err != nil {
		t.Fatal(err)
	}
	bs = crashReopen(t, bs, cfg)
	if ids := labelIDsAt(t, bs, 5800); len(ids) != 1 {
		t.Errorf("NodesByLabel(7, ValidAt 5800) = %v, want [%d] once", ids, nid)
	}
}
