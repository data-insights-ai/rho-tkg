package sharded_test

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardedTemporalIndexDDLSentinels checks the store-level temporal-index
// DDL contract: double-create -> ErrTemporalIndexExists, drop-missing ->
// ErrTemporalIndexNotFound, both errors.Is-able through the coalesce.
func TestShardedTemporalIndexDDLSentinels(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	defer func() { _ = st.Close() }()

	const label = uint16(1)
	if err := st.CreateTemporalIndex(label); err != nil {
		t.Fatalf("first CreateTemporalIndex: %v", err)
	}
	if err := st.CreateTemporalIndex(label); !errors.Is(err, storepkg.ErrTemporalIndexExists) {
		t.Fatalf("double create = %v, want ErrTemporalIndexExists", err)
	}
	if err := st.DropTemporalIndex(label); err != nil {
		t.Fatalf("DropTemporalIndex: %v", err)
	}
	if err := st.DropTemporalIndex(label); !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
		t.Fatalf("drop missing = %v, want ErrTemporalIndexNotFound", err)
	}
}

// TestShardedTemporalIndexQueryCorrect verifies that a temporal interval query
// returns the correct CROSS-SHARD set with a temporal index present — the index
// accelerates each shard's local scan and must not change results. Nodes carry
// explicit tkg_valid_from across distinct intervals, distributed across shards.
func TestShardedTemporalIndexQueryCorrect(t *testing.T) {
	g := newLanedShardedGraph(t, 4)

	// Three cohorts with distinct valid-from times, spread across shards.
	// vf(t): 1000, 2000, 3000 (ms).
	cohorts := []types.Instant{1000, 2000, 3000}
	byCohort := map[types.Instant][]types.NodeID{}
	slots := map[int64]struct{}{}
	const perCohort = 3
	seq := 0
	for r := 0; r < perCohort; r++ {
		for _, vf := range cohorts {
			sess, err := g.Ingest().NewSession(ingest.IngestOptions{Concurrent: true})
			if err != nil {
				t.Fatalf("NewSession: %v", err)
			}
			n, err := sess.AddNode([]string{"Event"}, map[string]any{
				"tkg_valid_from": vf,
				"seq":            int64(seq),
			})
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			if _, err := sess.Submit(); err != nil {
				t.Fatalf("Submit: %v", err)
			}
			_ = sess.Close()
			byCohort[vf] = append(byCohort[vf], n.ID())
			slots[g.Admin().DecomposeNodeID(n.ID()).NodeID] = struct{}{}
			seq++
		}
	}
	if len(slots) < 2 {
		t.Fatalf("nodes not spread across shards (slots=%d)", len(slots))
	}

	if err := g.Index().CreateTemporal("Event"); err != nil {
		t.Fatalf("CreateTemporal: %v", err)
	}

	// NodesAt(2500): valid_from <= 2500 and still open -> cohorts 1000 + 2000.
	got, err := g.Temporal().NodesAt(2500)
	if err != nil {
		t.Fatalf("NodesAt: %v", err)
	}
	want := append([]types.NodeID{}, byCohort[1000]...)
	want = append(want, byCohort[2000]...)
	assertNodeIDSet(t, got, want)
}
