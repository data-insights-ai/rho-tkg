package tiered

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredPurge_CrossShardEdgeSweep (ADR-0008 R4) is the correctness crux of the
// tiered purge under its SPLIT-WRITE adjacency layout. Two cross-shard edge
// classes between a reference node (ref shard) and a purged event (event shard):
//   - event->ref: the rel entity+out-leg live on the EVENT shard (removed by the
//     event's own purge), leaving an ORPHAN in-leg on the ref shard.
//   - ref->event: the rel entity+out-leg live on the REF shard (a full-local
//     residue there), only the in-leg lived on the event shard.
//
// Phase 2 must clean BOTH residues on the surviving ref shard, or they dangle as
// phantoms in the reference node's adjacency fold.
func TestTieredPurge_CrossShardEdgeSweep(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	const refTok = uint16(1) // "Case" (reference)
	const relType = uint16(5)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	ref := types.NewNode(types.NodeID(nodeGen.Generate()), refTok, nil)
	e1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	e2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{ref, e1, e2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put node: %v", err)
		}
	}

	// event->ref: entity on the EVENT shard, orphan in-leg on the ref shard.
	evToRef := types.NewRelationship(types.RelID(relGen.Generate()), relType, e1.ID(), ref.ID())
	if err := ts.PutRelationship(evToRef); err != nil {
		t.Fatalf("put event->ref: %v", err)
	}
	// ref->event: entity on the REF shard, in-leg on the event shard.
	refToEv := types.NewRelationship(types.RelID(relGen.Generate()), relType, ref.ID(), e1.ID())
	if err := ts.PutRelationship(refToEv); err != nil {
		t.Fatalf("put ref->event: %v", err)
	}

	if out, _ := ts.OutgoingRelationships(ref.ID(), 0); len(out) != 1 {
		t.Fatalf("pre-purge ref outgoing = %d, want 1 (ref->e1)", len(out))
	}
	if in, _ := ts.IncomingRelationships(ref.ID(), 0); len(in) != 1 {
		t.Fatalf("pre-purge ref incoming = %d, want 1 (e1->ref)", len(in))
	}

	before := types.Instant(1 << 50) // a far-future ms boundary — every node is older

	total := 0
	var relsSwept int
	for {
		res, err := ts.PurgeNodesByLabelBefore(signalTok, before, 8)
		if err != nil {
			t.Fatalf("purge: %v", err)
		}
		total += res.NodesPurged
		relsSwept += res.RelsPurged
		if !res.More {
			break
		}
	}
	if total != 2 {
		t.Fatalf("purged %d events, want 2", total)
	}

	// Events gone; reference survives.
	if _, err := ts.GetNode(e1.ID()); !errors.Is(err, storecontract.ErrNodeNotFound) {
		t.Fatalf("E1 survived: %v", err)
	}
	if _, err := ts.GetNode(e2.ID()); !errors.Is(err, storecontract.ErrNodeNotFound) {
		t.Fatalf("E2 survived: %v", err)
	}
	if _, err := ts.GetNode(ref.ID()); err != nil {
		t.Fatalf("reference wrongly purged: %v", err)
	}

	// BOTH cross-shard edges gone — no dangling residue on the ref shard.
	if _, err := ts.GetRelationship(evToRef.ID()); !errors.Is(err, storecontract.ErrRelNotFound) {
		t.Fatalf("event->ref edge survived: %v", err)
	}
	if _, err := ts.GetRelationship(refToEv.ID()); !errors.Is(err, storecontract.ErrRelNotFound) {
		t.Fatalf("ref->event edge survived (dangling entity on ref shard): %v", err)
	}
	if out, _ := ts.OutgoingRelationships(ref.ID(), 0); len(out) != 0 {
		t.Fatalf("ref outgoing = %d after purge, want 0 (no dangling ref->e1 entity)", len(out))
	}
	if in, _ := ts.IncomingRelationships(ref.ID(), 0); len(in) != 0 {
		t.Fatalf("ref incoming = %d after purge, want 0 (no orphan e1->ref in-leg)", len(in))
	}
}
