package tiered

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredColdShardFastDrop_ConcurrentWriters is the BACKLOG 19d regression:
// no genuine concurrent-WRITER (not just concurrent readers —
// TestTieredColdShardFastDrop_ConcurrentReads already covers that — and not
// just a Close() race — TestTieredColdShardFastDrop_CloseDoesNotRaceEventShardsMap
// covers that) test exercised the cold-shard-drop drain protocol. This test
// runs the fast-drop purge concurrently with several REAL writer goroutines —
// through the public API, with real timing, not a deterministic simulation —
// performing legitimate cross-shard relationship creates and foreign-label
// additions that touch the SAME candidate shard the drop targets, under
// -race.
//
// The writers deliberately touch SURVIVOR nodes only (an "Other"-labeled set
// co-located on the same candidate shard as the purge-target Signal nodes,
// via the same rotation window, but never eligible for THIS purge) — not the
// purge-target nodes themselves. Racing writes against nodes the purge may
// concurrently delete would conflate this test with the separate, already-
// documented MEDIUM-severity gap (BACKLOG 19g: "putRelationshipLocked's
// write ordering has no residue reconciliation on crash... undetectable
// except via manual VerifyShard/RunRepair") — a node legitimately vanishing
// out from under an in-flight cross-shard write is a known, accepted race
// with its own backlog entry; this test targets the DISTINCT property 19d
// asks for for a stable set of nodes.
//
// Rather than asserting one specific interleaving outcome (which would be
// flaky by construction against real goroutine scheduling), it asserts the
// property that must hold under EVERY interleaving: no crash/race, and
// RunRepair reports a fully consistent end state (no orphaned or missing in/
// entries) no matter how the race played out — a residue left by ANY
// interleaving would surface here.
func TestTieredColdShardFastDrop_ConcurrentWriters(t *testing.T) {
	ts, err := New(Config{
		DataDir:       t.TempDir(),
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	for _, l := range []string{"Case", "User", "Signal"} {
		if _, err := reg.GetOrCreate(l); err != nil {
			t.Fatalf("registry: %v", err)
		}
	}
	otherTok, err := reg.GetOrCreate("Other")
	if err != nil {
		t.Fatalf("registry: %v", err)
	}
	const caseTok, signalTok, relType = uint16(1), uint16(3), uint16(5)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	// Purge-target nodes: rotate into the candidate shard, will be deleted.
	for i := 0; i < 20; i++ {
		id := types.NodeID(nodeGen.Generate())
		if err := ts.PutNode(types.NewNode(id, signalTok, nil)); err != nil {
			t.Fatalf("put signal node: %v", err)
		}
	}
	// Survivor nodes: SAME rotation window (co-located on the candidate
	// shard) but a different label — never targeted by this purge, so
	// writers touching them race the drain protocol without racing their
	// own deletion.
	var survivorIDs []types.NodeID
	for i := 0; i < 12; i++ {
		id := types.NodeID(nodeGen.Generate())
		if err := ts.PutNode(types.NewNode(id, otherTok, nil)); err != nil {
			t.Fatalf("put survivor node: %v", err)
		}
		survivorIDs = append(survivorIDs, id)
	}
	var refIDs []types.NodeID
	for i := 0; i < 8; i++ {
		id := types.NodeID(nodeGen.Generate())
		if err := ts.PutNode(types.NewNode(id, caseTok, nil)); err != nil {
			t.Fatalf("put ref node: %v", err)
		}
		refIDs = append(refIDs, id)
	}
	if err := ts.ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}

	var relCounter atomic.Int64
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writer group 1: cross-shard relationship creates connecting survivor
	// (candidate-shard) nodes to reference-shard nodes — exercises
	// PutRelEntityAndOut/PutRelIncoming racing the drain's unlink+drain,
	// without ever racing a purge-target node's own deletion.
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				sv := survivorIDs[int(relCounter.Add(1))%len(survivorIDs)]
				ref := refIDs[w%len(refIDs)]
				r := types.NewRelationship(types.RelID(relGen.Generate()), relType, sv, ref)
				if err := ts.PutRelationship(r); err != nil {
					t.Errorf("PutRelationship(survivor->ref): %v", err)
					return
				}
			}
		}(w)
	}

	// Writer group 2: label additions on survivor nodes — exercises the
	// BACKLOG 19e window (a label change landing on the candidate shard
	// mid-drain) via real concurrent goroutines rather than a deterministic
	// simulation.
	wg.Add(1)
	go func() {
		defer wg.Done()
		idx := 0
		for {
			select {
			case <-stop:
				return
			default:
			}
			id := survivorIDs[idx%len(survivorIDs)]
			idx++
			n, err := ts.GetNode(id)
			if err != nil {
				t.Errorf("GetNode(survivor): %v", err)
				return
			}
			updated := n.DeepCopy()
			updated.AddLabelTokenRaw(caseTok)
			if err := ts.AddNodeLabelToken(id, caseTok, updated); err != nil {
				t.Errorf("AddNodeLabelToken(survivor): %v", err)
				return
			}
			// Idempotent re-add on the next pass is harmless (AddLabelTokenRaw
			// is a no-op if already present) — no need to alternate add/remove.
			time.Sleep(50 * time.Microsecond)
		}
	}()

	purgeDone := make(chan struct{})
	go func() {
		defer close(purgeDone)
		_, _ = ts.PurgeNodesByLabelBefore(signalTok, types.Instant(1<<50), 8)
	}()

	<-purgeDone
	close(stop)
	wg.Wait()

	// Every survivor node and its cross-shard edges must still be there —
	// they were never eligible for this purge.
	for _, id := range survivorIDs {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("survivor node %v lost: %v", id, err)
		}
	}

	// Whatever interleaving occurred, the store must be internally
	// consistent afterward — no orphaned or missing in/ entries anywhere.
	result, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if result.OrphanedInEntries != 0 || result.MissingInEntries != 0 {
		t.Fatalf("RunRepair after concurrent-writer drop found residue: orphaned=%d missing=%d — BACKLOG 19d regression",
			result.OrphanedInEntries, result.MissingInEntries)
	}
}
