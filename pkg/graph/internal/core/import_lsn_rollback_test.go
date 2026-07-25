package core

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The LSN test below is RED-PROVEN; the foreign-stub test is NOT. Read this
// before trusting the second one as a guard.
//
//   - TestImport_FailedReplayRestoresTheAppliedLSNWatermark: RED against the
//     pre-fix code with `applied-LSN watermark = 9000 after a FAILED import,
//     was 0 before`. Its first version was worthless: it drove a MID-REPLAY
//     failure, but the SnapshotLSN commit sits at the END of
//     importReplayRecordsLocked, so that path never reached it and the
//     assertion held trivially at 0 == 0. The path that exhibits the bug needs
//     the replay to SUCCEED — a unique constraint plus a stream carrying two
//     conflicting nodes, so validation fails AFTER the watermark commits.
//   - The foreign-stub test infers ordering from a FAILED create. Verified by
//     reverting the fix: the create really does fail (ErrSlotNotLocal), but
//     createRelWithTypeRollback returns a NON-NIL rel alongside the error when
//     it cannot clean up the partial row ("failed to remove partial
//     relationship ... after create failure"). The pre-fix guard is
//     `if rel != nil`, so the advance runs in BOTH arrangements and the end
//     states are identical. Ordering here is a concurrency property and no
//     single-threaded observation can separate before-store from after-store;
//     it needs a concurrent reader calling NowTx while the create is in flight,
//     or a fault hook between the store write and the advance.
//
// Both gaps are named rather than papered over: a test that cannot fail is worse
// than no test, because it reads as coverage.

// A rolled-back Import must not leave the bootstrap-handoff watermark advanced.
//
// importReplayRecordsLocked commits header.SnapshotLSN as it replays. Rollback
// unwinds entities and registries and has no knowledge of that watermark, so a
// failed import left the replica claiming a replay position for a snapshot it
// had fully unwound. It then believes it is caught up, never re-bootstraps, and
// silently serves an incomplete dataset — the failure mode
// metakv_reap_policy.go already documents for replicaAppliedLSNMeta.
func TestImport_FailedReplayRestoresTheAppliedLSNWatermark(t *testing.T) {
	ctx := context.Background()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// A unique constraint the imported stream will violate. This is the ONLY
	// path that reaches the bug: the SnapshotLSN commit sits at the END of
	// importReplayRecordsLocked, so a mid-replay failure never gets there. The
	// replay must SUCCEED (committing the watermark) and post-replay validation
	// must then fail and roll back.
	seed, err := g.Nodes.Add(ctx, []string{"Person"}, map[string]any{"email": "seed@x"})
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	labelTok, ok := g.labels.Lookup("Person")
	if !ok {
		t.Fatal("Person label token missing")
	}
	g.uniqueMu.Lock()
	g.installConstraintLocked(labelTok, "email", "Person", constraintspkg.UniqueCurrent, true)
	g.uniqueMu.Unlock()
	_ = seed

	before, err := g.appliedLSNLocked()
	if err != nil {
		t.Skipf("backend does not expose an applied-LSN watermark: %v", err)
	}

	const snapshotLSN = 9_000
	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{
		Version: exportFormatVersion, SnapshotLSN: snapshotLSN, NodeCount: 2,
	})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels: g.labels.ExportNames(), RelTypes: g.relTypes.ExportNames(),
	})
	// Two Person nodes sharing an email: both replay cleanly, then validation
	// rejects the pair.
	for _, id := range []int64{7001, 7002} {
		writeImportMsgpackRecord(t, &stream, exportTagNode, mustHashedNodeWire(t, storeutil.NodeWire{
			ID: id, PrimaryLabel: int(labelTok), Version: 1,
			Properties: []storeutil.PropertyWire{{Key: "email", Value: "dup@x"}},
		}, []string{"Person"}))
	}

	err = g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if err == nil {
		t.Fatal("precondition: the import was expected to fail unique validation")
	}
	if _, gerr := g.store.GetNode(types.NodeID(7001)); !errors.Is(gerr, storepkg.ErrNodeNotFound) {
		t.Fatalf("precondition: node 7001 survived the rollback (%v)", gerr)
	}

	after, err := g.appliedLSNLocked()
	if err != nil {
		t.Fatalf("read applied LSN: %v", err)
	}
	if after != before {
		t.Fatalf("applied-LSN watermark = %d after a FAILED import, was %d before. The replay "+
			"committed the snapshot watermark, then unique validation rolled the DATA back — but "+
			"rollback knows nothing about the watermark. This replica now claims to hold a snapshot "+
			"it fully unwound: it will consider itself caught up, never re-bootstrap, and silently "+
			"serve an incomplete dataset.", after, before)
	}
}

// RecordForeignIncoming must cover the stamp BEFORE the row can be observed.
//
// Advancing after the create leaves a window in which the stub is durable and
// readable while NowTx() is still below it; the door holds a shared RLock and
// NowTx takes no lock, so the window is concurrently observable. Advancing first
// is the safe direction — the stamp is already bounded, so a failed create
// merely leaves the floor higher than necessary.
//
// The ordering is asserted through its observable consequence: when the create
// FAILS, the floor must already have advanced.
func TestRecordForeignIncoming_FloorAdvancesBeforeTheRowIsStored(t *testing.T) {
	g, endID := newForeignIncomingTestGraph(t, ValidationLimits{})

	// Plausible but ahead of this host — a peer mid-burst, or one hour fast.
	foreign := types.Instant(time.Now().Add(time.Hour).UnixMilli())

	edge := baseForeignEdge(endID)
	edge.TxFrom = foreign
	edge.AttestTx = foreign
	// A start node that does not exist makes the CREATE fail, after validation
	// has accepted the stamp.
	edge.EndID = types.NodeID(999_999_999)

	_ = g.Rels.RecordForeignIncoming(context.Background(), edge)

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	if pin < foreign {
		t.Fatalf("NowTx = %d < the accepted foreign stamp %d. The floor is advanced only AFTER the "+
			"create, so between the store write and the advance there is a window where the stub is "+
			"durable and readable while the pin sits below it — concurrently observable, since this "+
			"door holds only an RLock and NowTx takes no lock at all. Advancing first is safe: the "+
			"stamp is already bounded, so a failed create just leaves the floor higher than needed.",
			pin, foreign)
	}
}
