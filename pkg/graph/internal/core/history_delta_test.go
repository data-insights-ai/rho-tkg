package core

import (
	"context"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestHistoryDeltaCoreDifferentialAndIntegrity drives an identical history-heavy
// workload through a badger graph with HistoryDeltaEncoding ON and a memory graph
// (full-snapshot oracle), asserting end-to-end that:
//   - version history reconstructs identically across the two backends,
//   - the large unchanging blob is present in EVERY reconstructed version (the
//     delta path must re-materialize it),
//   - the hash chain verifies over the delta-reconstructed chain (integrity is
//     preserved through anchor+delta storage),
//   - point-in-time reads agree between the two backends (two-phase temporal).
func TestHistoryDeltaCoreDifferentialAndIntegrity(t *testing.T) {
	ctx := context.Background()

	deltaG, err := New(Config{BadgerInMemory: true, HistoryDeltaEncoding: true})
	if err != nil {
		t.Fatalf("New(delta): %v", err)
	}
	t.Cleanup(func() { _ = deltaG.Close() })
	memG, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New(mem): %v", err)
	}
	t.Cleanup(func() { _ = memG.Close() })

	const blob = "a large unchanging free-text blob " +
		"that a full snapshot would re-serialize on every single version bump"
	blobBig := strings.Repeat(blob+" | ", 6)

	// run applies the same workload and returns the entity id (ids differ per
	// backend, so all comparisons are by content).
	run := func(g *Core) types.NodeID {
		n, err := g.Nodes.Add(ctx, []string{"Doc"}, map[string]any{
			"blob":           blobBig,
			"counter":        int64(0),
			"region":         "eu-west",
			"tkg_valid_from": types.Instant(1000),
		})
		if err != nil {
			t.Fatalf("Add: %v", err)
		}
		for v := 1; v <= 20; v++ { // crosses the anchor boundary at 16
			if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{
				"counter":        int64(v),
				"status":         []string{"active", "pending", "held", "closed"}[v%4],
				"tkg_valid_from": types.Instant(1000 + int64(v)*100),
			}); err != nil {
				t.Fatalf("Update v%d: %v", v, err)
			}
		}
		return n.ID()
	}
	idDelta := run(deltaG)
	idMem := run(memG)

	// History parity + blob-in-every-version.
	hd, err := deltaG.Nodes.History(idDelta)
	if err != nil {
		t.Fatalf("delta History: %v", err)
	}
	hm, err := memG.Nodes.History(idMem)
	if err != nil {
		t.Fatalf("mem History: %v", err)
	}
	if len(hd) != len(hm) {
		t.Fatalf("history length delta=%d mem=%d", len(hd), len(hm))
	}
	for i := range hd {
		cd, _ := hd[i].GetProperty("counter")
		cm, _ := hm[i].GetProperty("counter")
		if cd != cm {
			t.Fatalf("history[%d] counter delta=%v mem=%v", i, cd, cm)
		}
		if b, ok := hd[i].GetProperty("blob"); !ok || b != blobBig {
			t.Fatalf("history[%d] blob not reconstructed in delta backend", i)
		}
	}

	// Integrity: the hash chain must verify over the delta-reconstructed chain.
	ok, err := deltaG.Hash.VerifyNodeChain(idDelta)
	if err != nil {
		t.Fatalf("delta VerifyNodeChain: %v", err)
	}
	if !ok {
		t.Fatalf("delta hash chain failed to verify — reconstruction corrupts integrity")
	}

	// Two-phase point-in-time: both backends must agree at every probed time,
	// including times BEFORE the latest version (reflecting past state).
	for _, at := range []int64{1050, 1150, 1650, 2050, 3000} {
		nd, errd := deltaG.Temporal.NodeAt(idDelta, types.Instant(at))
		nm, errm := memG.Temporal.NodeAt(idMem, types.Instant(at))
		if (errd == nil) != (errm == nil) {
			t.Fatalf("NodeAt(%d) error mismatch delta=%v mem=%v", at, errd, errm)
		}
		if errd != nil {
			continue
		}
		cd, _ := nd.GetProperty("counter")
		cm, _ := nm.GetProperty("counter")
		if cd != cm {
			t.Fatalf("NodeAt(%d) counter delta=%v mem=%v", at, cd, cm)
		}
	}
}
