package tiered

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredPurge_ByValidTo_CrossShardEdgeSweep is the ByValidTo (ADR-0008 R5)
// counterpart of TestTieredPurge_CrossShardEdgeSweep: it exercises the identical
// split-write cross-shard residue sweep, but selects victims by world-time validity
// (ValidTo < before) instead of mint-time. It also proves selectivity — an event
// with an OPEN interval (ValidTo == 0) is NOT purged even though it is older than the
// boundary in mint-time, so the reference node retains exactly that one edge.
func TestTieredPurge_ByValidTo_CrossShardEdgeSweep(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	const refTok = uint16(1) // "Case" (reference)
	const relType = uint16(5)
	const boundary = types.Instant(5000)

	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	ref := types.NewNode(types.NodeID(nodeGen.Generate()), refTok, nil)
	// closed: validity ended at 1000 < boundary → purged.
	closed := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	closed.SetTemporal(&types.TemporalMetadata{ValidFrom: 100, ValidTo: 1000})
	// open: ValidTo == 0 → kept, despite being older than boundary in mint-time.
	open := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	open.SetTemporal(&types.TemporalMetadata{ValidFrom: 100})
	for _, n := range []*types.Node{ref, closed, open} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("put node: %v", err)
		}
	}

	// Cross-shard edges from the CLOSED (to-be-purged) event to/from the reference.
	// event->ref: entity+out-leg on the event shard, orphan in-leg on the ref shard.
	evToRef := types.NewRelationship(types.RelID(relGen.Generate()), relType, closed.ID(), ref.ID())
	if err := ts.PutRelationship(evToRef); err != nil {
		t.Fatalf("put event->ref: %v", err)
	}
	// ref->event: entity+out-leg on the ref shard, in-leg on the event shard.
	refToEv := types.NewRelationship(types.RelID(relGen.Generate()), relType, ref.ID(), closed.ID())
	if err := ts.PutRelationship(refToEv); err != nil {
		t.Fatalf("put ref->event: %v", err)
	}
	// A surviving edge ref->open must remain after the purge.
	refToOpen := types.NewRelationship(types.RelID(relGen.Generate()), relType, ref.ID(), open.ID())
	if err := ts.PutRelationship(refToOpen); err != nil {
		t.Fatalf("put ref->open: %v", err)
	}

	total, relsSwept := 0, 0
	for {
		res, err := ts.PurgeNodesByLabelValidToBefore(signalTok, boundary, 8)
		if err != nil {
			t.Fatalf("purge by valid-to: %v", err)
		}
		total += res.NodesPurged
		relsSwept += res.RelsPurged
		if !res.More {
			break
		}
	}
	if total != 1 {
		t.Fatalf("purged %d events, want 1 (only the early-closed event)", total)
	}

	// closed gone; open + reference survive.
	if _, err := ts.GetNode(closed.ID()); !errors.Is(err, storecontract.ErrNodeNotFound) {
		t.Fatalf("closed event survived: %v", err)
	}
	if _, err := ts.GetNode(open.ID()); err != nil {
		t.Fatalf("open event wrongly purged: %v", err)
	}
	if _, err := ts.GetNode(ref.ID()); err != nil {
		t.Fatalf("reference wrongly purged: %v", err)
	}

	// Both cross-shard edges to the purged event are gone; the ref->open edge remains.
	if _, err := ts.GetRelationship(evToRef.ID()); !errors.Is(err, storecontract.ErrRelNotFound) {
		t.Fatalf("event->ref edge survived: %v", err)
	}
	if _, err := ts.GetRelationship(refToEv.ID()); !errors.Is(err, storecontract.ErrRelNotFound) {
		t.Fatalf("ref->event edge survived (dangling entity on ref shard): %v", err)
	}
	if _, err := ts.GetRelationship(refToOpen.ID()); err != nil {
		t.Fatalf("ref->open edge wrongly removed: %v", err)
	}
	if out, _ := ts.OutgoingRelationships(ref.ID(), 0); len(out) != 1 {
		t.Fatalf("ref outgoing = %d after purge, want 1 (ref->open only)", len(out))
	}
	if in, _ := ts.IncomingRelationships(ref.ID(), 0); len(in) != 0 {
		t.Fatalf("ref incoming = %d after purge, want 0 (no orphan in-leg)", len(in))
	}
}
