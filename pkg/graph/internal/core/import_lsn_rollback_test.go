package core

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// NOT RED-PROVEN — read this before trusting either test below as a guard.
//
// Both assert the right property and both pass, but removing the production fix
// does NOT make either fail, so neither has demonstrated it can detect the
// defect it describes. They are documentation with a runnable assertion, not
// verified regression guards.
//
//   - The LSN test drives a MID-REPLAY failure (an unresolvable relationship
//     endpoint). The SnapshotLSN commit sits at the END of
//     importReplayRecordsLocked, so that path never reaches it: the watermark is
//     0 before and after, and the assertion holds trivially. The path that
//     actually exhibits the bug is the OTHER one — replay SUCCEEDS, the
//     watermark commits, and post-replay unique-constraint validation then fails
//     and rolls back. Reproducing it needs a unique constraint installed against
//     the imported label (installConstraintLocked) plus a stream carrying two
//     conflicting nodes.
//   - The foreign-stub test infers ordering from a FAILED create, but the create
//     fails during validation, before either arrangement reaches its advance.
//     Ordering is a concurrency property; a single-threaded observation of the
//     end state cannot separate before-store from after-store. It needs a
//     concurrent reader calling NowTx while the create is in flight.
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
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	before, err := g.appliedLSNLocked()
	if err != nil {
		t.Skipf("backend does not expose an applied-LSN watermark: %v", err)
	}

	// A snapshot claiming a high LSN whose LAST record is unresolvable, so the
	// replay fails AFTER the watermark has been committed.
	const snapshotLSN = 9_000
	var stream bytes.Buffer
	// NodeCount/RelCount must match the stream, or the header check rejects it
	// before the replay ever reaches the watermark commit — which made the first
	// version of this test vacuous.
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{
		Version: exportFormatVersion, SnapshotLSN: snapshotLSN, NodeCount: 1, RelCount: 1,
	})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels: []string{"", "Person"}, RelTypes: []string{"", "KNOWS"},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNode, mustHashedNodeWire(t, storeutil.NodeWire{
		ID: 100, PrimaryLabel: 1, Version: 1,
	}, []string{"Person"}))
	// Endpoint 200 does not exist: PutRelationship fails after the node replay.
	writeImportMsgpackRecord(t, &stream, exportTagRel, mustHashedRelWire(t, storeutil.RelWire{
		ID: 300, RelType: 1, StartID: 100, EndID: 200,
	}, "KNOWS"))

	if err := g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{}); err == nil {
		t.Fatal("precondition: the import was expected to fail")
	}

	// The data really was unwound.
	if _, err := g.store.GetNode(types.NodeID(100)); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("precondition: node 100 survived the rollback (%v)", err)
	}

	after, err := g.appliedLSNLocked()
	if err != nil {
		t.Fatalf("read applied LSN: %v", err)
	}
	if after != before {
		t.Fatalf("applied-LSN watermark = %d after a FAILED import, was %d before. The data was "+
			"rolled back but the replay position was not, so this replica now claims to hold a "+
			"snapshot it fully unwound — it will consider itself caught up, never re-bootstrap, and "+
			"silently serve an incomplete dataset.", after, before)
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
