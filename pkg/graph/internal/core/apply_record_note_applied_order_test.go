package core

import (
	"errors"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestApplyChange_RejectedNodePutDoesNotBumpAsOfCacheEpoch is the BACKLOG 12f
// proof: noteAppliedTxFrom must run AFTER property/hash verification
// succeeds, never before. Previously all apply handlers called it right
// after decoding the wire — so a record that would ultimately be REJECTED
// (a wrong hash, here) still fed its TxFrom to the as-of cache's past-dated
// detector before the rejection was discovered. Since the detector bumps a
// GLOBAL epoch whenever an applied TxFrom is lower than the running max, an
// out-of-order but otherwise-CORRUPT record could unnecessarily discard
// every cached as-of column for an entity that never actually landed in the
// store — over-invalidation with no compensating benefit (the record is
// rejected either way; the only question is whether the cache pays for it).
//
// This test establishes a high applied-TxFrom watermark via a valid apply,
// then sends a node put with an OLDER TxFrom (the out-of-order shape) AND a
// wrong hash (so it must be rejected), and asserts the as-of cache epoch is
// unchanged. Confirmed load-bearing: reverting the BACKLOG 12f reordering
// (restoring the noteAppliedTxFrom call to its original pre-verification
// position) turns this RED (the epoch bumps despite the rejection).
func TestApplyChange_RejectedNodePutDoesNotBumpAsOfCacheEpoch(t *testing.T) {
	t.Parallel()
	g := newTestGraph(t)
	personTok, err := g.labels.GetOrCreate("Person")
	if err != nil {
		t.Fatalf("labels.GetOrCreate: %v", err)
	}

	// Establish a high applied-TxFrom watermark via a valid ChangeNodePut apply.
	validWire := mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           100,
		PrimaryLabel: int(personTok),
		Version:      1,
		HasTemporal:  true,
		TxFrom:       1_000_000,
	}, []string{"Person"})
	rec1 := storepkg.ChangeRecord{
		LSN:     1,
		Tag:     storepkg.ChangeNodePut,
		Payload: mustMarshalChangePayload(t, storeutil.NodePutBody{Wire: validWire}),
	}
	if err := g.Repl.ApplyChange(rec1); err != nil {
		t.Fatalf("ApplyChange(watermark node put): %v", err)
	}

	epochBefore := g.asOfColumns.currentEpoch()

	// An out-of-order (older TxFrom) node put that carries a WRONG hash — must
	// be rejected, and rejection must mean the as-of cache is untouched.
	badWire := storeutil.NodeWire{
		ID:           101,
		PrimaryLabel: int(personTok),
		Version:      1,
		HasTemporal:  true,
		TxFrom:       500_000, // < 1_000_000 — the out-of-order shape
		Hash:         "not-the-real-hash",
	}
	rec2 := storepkg.ChangeRecord{
		LSN:     2,
		Tag:     storepkg.ChangeNodePut,
		Payload: mustMarshalChangePayload(t, storeutil.NodePutBody{Wire: badWire}),
	}
	err = g.Repl.ApplyChange(rec2)
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("ApplyChange(bad hash, out-of-order TxFrom) = %v, want ErrCorruptExport", err)
	}

	epochAfter := g.asOfColumns.currentEpoch()
	if epochAfter != epochBefore {
		t.Fatalf("a REJECTED node put still bumped the as-of cache epoch (BACKLOG 12f): before=%d after=%d", epochBefore, epochAfter)
	}
	if _, err := g.store.GetNode(types.NodeID(101)); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode(101) after rejected apply: got %v, want ErrNodeNotFound", err)
	}
}
