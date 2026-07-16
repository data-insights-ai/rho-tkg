package sharded_test

import (
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestShardedHighFrequencyIndexDDLSentinels checks the store-level high-freq
// DDL contract, including the shared-namespace exclusion with the temporal
// index and bucket-size validation.
func TestShardedHighFrequencyIndexDDLSentinels(t *testing.T) {
	st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 4})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	defer func() { _ = st.Close() }()

	const label = uint16(1)
	if err := st.CreateHighFrequencyIndex(label, time.Second); err != nil {
		t.Fatalf("first CreateHighFrequencyIndex: %v", err)
	}
	if err := st.CreateHighFrequencyIndex(label, time.Second); !errors.Is(err, storepkg.ErrTemporalIndexExists) {
		t.Fatalf("double create = %v, want ErrTemporalIndexExists", err)
	}
	// A temporal index on the same label is excluded (shared namespace).
	if err := st.CreateTemporalIndex(label); !errors.Is(err, storepkg.ErrTemporalIndexExists) {
		t.Fatalf("temporal create over high-freq = %v, want ErrTemporalIndexExists", err)
	}
	// Invalid (non-positive) bucket size rejected.
	if err := st.CreateHighFrequencyIndex(uint16(2), 0); !errors.Is(err, storepkg.ErrInvalidTemporalIndexConfig) {
		t.Fatalf("bucket=0 = %v, want ErrInvalidTemporalIndexConfig", err)
	}
	if err := st.DropHighFrequencyIndex(label); err != nil {
		t.Fatalf("DropHighFrequencyIndex: %v", err)
	}
	if err := st.DropHighFrequencyIndex(label); !errors.Is(err, storepkg.ErrTemporalIndexNotFound) {
		t.Fatalf("drop missing = %v, want ErrTemporalIndexNotFound", err)
	}
}

// TestShardedHighFrequencyIndexQueryCorrect verifies a temporal interval query
// returns the correct CROSS-SHARD set with a high-frequency index present — the
// index accelerates each shard's local scan and must not change results.
func TestShardedHighFrequencyIndexQueryCorrect(t *testing.T) {
	g := newLanedShardedGraph(t, 4)

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
			n, err := sess.AddNode([]string{"Tick"}, map[string]any{
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

	if err := g.Index().CreateHighFrequency("Tick", 100*time.Millisecond); err != nil {
		t.Fatalf("CreateHighFrequency: %v", err)
	}

	// NodesAt(2500): cohorts 1000 + 2000 valid, 3000 not yet.
	got, err := g.Temporal().NodesAt(2500)
	if err != nil {
		t.Fatalf("NodesAt: %v", err)
	}
	want := append([]types.NodeID{}, byCohort[1000]...)
	want = append(want, byCohort[2000]...)
	assertNodeIDSet(t, got, want)
}
