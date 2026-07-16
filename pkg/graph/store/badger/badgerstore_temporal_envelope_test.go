package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// B4 Stage 2 — the temporal index is a per-node valid-time ENVELOPE across all
// versions. These white-box tests inspect bs.temporalIndexes directly (the RAM
// authority) to prove a past version's interval stays covered after the current
// version moves off it — both via forward maintenance and after a restart rebuild.

func envelopeCoversPast(t *testing.T, bs *Store, label uint16, id snowflake.ID, from, to types.Instant) {
	t.Helper()
	bs.idxMu.RLock()
	ti := bs.temporalIndexes[label]
	bs.idxMu.RUnlock()
	if ti == nil {
		t.Fatalf("no temporal index for label %d", label)
	}
	got := ti.QueryOverlap(from, to)
	for _, g := range got {
		if g == id {
			return
		}
	}
	t.Fatalf("temporal envelope for label %d does not cover past window [%d,%d) for id %d (got %v)", label, from, to, id, got)
}

func TestTemporalEnvelope_ForwardMaintenance(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bs.Close() })
	const label = uint16(1)

	n := types.NewNode(types.NodeID(100), label, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 10, ValidTo: 20})
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(label); err != nil {
		t.Fatal(err)
	}
	// Update: current version moves to [30,40). Envelope must retain [10,20).
	updated := types.NewNode(types.NodeID(100), label, nil)
	updated.SetTemporal(&types.TemporalMetadata{ValidFrom: 30, ValidTo: 40})
	updated.SetVersion(1)
	if err := bs.ReplaceNodeWithHistory(updated, 0, n); err != nil {
		t.Fatal(err)
	}
	envelopeCoversPast(t, bs, label, snowflake.ID(100), 10, 20) // past version
	envelopeCoversPast(t, bs, label, snowflake.ID(100), 30, 40) // current version
}

// The envelope must be reconstructed from history on restart (rebuild sees only
// the current version; the history-fold pass restores the past coverage).
func TestTemporalEnvelope_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	const label = uint16(2)

	n := types.NewNode(types.NodeID(200), label, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 10, ValidTo: 20})
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(label); err != nil {
		t.Fatal(err)
	}
	updated := types.NewNode(types.NodeID(200), label, nil)
	updated.SetTemporal(&types.TemporalMetadata{ValidFrom: 30, ValidTo: 40})
	updated.SetVersion(1)
	if err := bs.ReplaceNodeWithHistory(updated, 0, n); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the temporal-index definition persists; rebuild + history-fold must
	// restore the past-version envelope coverage.
	bs2, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { bs2.Close() })
	envelopeCoversPast(t, bs2, label, snowflake.ID(200), 10, 20) // past version, post-restart
	envelopeCoversPast(t, bs2, label, snowflake.ID(200), 30, 40) // current version
}
