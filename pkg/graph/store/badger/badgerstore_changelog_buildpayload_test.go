package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 18a: every single-item Put/Replace/label/history-put door used to
// mutate in-memory state and enqueue write ops BEFORE marshaling its
// change-log payload (logNodePut/logRelPut/logRelPutTagged ran LAST, right
// before idxMu.Unlock()). A change-log marshal failure at that point would
// leave already-mutated state and already-queued ops with no corresponding
// record — a silent replica/CDC divergence risk (lesson 58). The established
// safe pattern (already used by DeleteNode) builds the record BEFORE any
// mutation, so a marshal error aborts the door with nothing touched.
//
// buildNodePutPayload / buildRelPutPayload are the new "build" phase every
// affected door now calls before bs.idxMu.Lock() and before any mutation;
// bs.logChangeRaw(tag, payload) is the "emit" phase, still called under the
// lock atomically with the entity ops. These tests exercise the build phase
// directly — the same code path every door (PutNode, ReplaceNode,
// RemoveNodeLabelToken, AddNodeLabelToken, putRelationship,
// ReplaceRelationship, and their four WithHistory siblings) now runs first.
//
// A genuine marshal failure is not reachable through the public Store API on
// a validated node/relationship (NodeToWireChecked/RelToWireChecked run
// identically — and successfully — during the door's own entity-wire marshal
// moments earlier), so these tests exercise the build phase directly with a
// nil entity, which IS a real, reachable error (NodeToWireChecked/
// RelToWireChecked reject nil) and proves the build phase surfaces it instead
// of panicking or silently dropping the record.

func TestBuildNodePutPayload_DisabledLogIsNoOp(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t) // ChangeLog not set
	n := types.NewNode(types.NodeID(snowflake.ID(1)), 10, nil)
	payload, err := bs.buildNodePutPayload(n, false)
	if err != nil {
		t.Fatalf("buildNodePutPayload with log disabled: %v", err)
	}
	if payload != nil {
		t.Fatalf("buildNodePutPayload with log disabled = %v, want nil payload", payload)
	}
}

func TestBuildNodePutPayload_NilNodeReturnsErrorBeforeAnyLockOrMutation(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	before := recs(t, bs)

	payload, err := bs.buildNodePutPayload(nil, false)
	if err == nil {
		t.Fatal("buildNodePutPayload(nil) = nil error, want an error")
	}
	if payload != nil {
		t.Fatalf("buildNodePutPayload(nil) payload = %v, want nil on error", payload)
	}

	// The build phase is pure (no lock taken, no state touched) — proving a
	// door that aborts here (as every fixed door now does, by checking the
	// error before bs.idxMu.Lock()) leaves the change-log and store state
	// completely untouched, unlike the pre-fix ordering where the mutation
	// and appendOps had ALREADY happened by the time the (equivalent) old
	// logNodePut call could fail.
	after := recs(t, bs)
	if len(after) != len(before) {
		t.Fatalf("buildNodePutPayload(nil) error path emitted %d change-log records, want 0 new records", len(after)-len(before))
	}
}

func TestBuildRelPutPayload_DisabledLogIsNoOp(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStore(t) // ChangeLog not set
	r := types.NewRelationship(types.RelID(snowflake.ID(100)), 5, types.NodeID(snowflake.ID(1)), types.NodeID(snowflake.ID(2)))
	payload, err := bs.buildRelPutPayload(r, false)
	if err != nil {
		t.Fatalf("buildRelPutPayload with log disabled: %v", err)
	}
	if payload != nil {
		t.Fatalf("buildRelPutPayload with log disabled = %v, want nil payload", payload)
	}
}

func TestBuildRelPutPayload_NilRelReturnsErrorBeforeAnyLockOrMutation(t *testing.T) {
	t.Parallel()
	bs := newChangeLogStore(t, false)

	before := recs(t, bs)

	payload, err := bs.buildRelPutPayload(nil, false)
	if err == nil {
		t.Fatal("buildRelPutPayload(nil) = nil error, want an error")
	}
	if payload != nil {
		t.Fatalf("buildRelPutPayload(nil) payload = %v, want nil on error", payload)
	}

	after := recs(t, bs)
	if len(after) != len(before) {
		t.Fatalf("buildRelPutPayload(nil) error path emitted %d change-log records, want 0 new records", len(after)-len(before))
	}
}

// relPutTag is a pure lookup — proven exhaustively for both inputs so a future
// edit cannot silently swap the ChangeRelPut/ChangeForeignIncoming branches
// (which would misroute a replica's foreign-incoming-stub apply, ADR-0010).
func TestRelPutTag(t *testing.T) {
	t.Parallel()
	if got := relPutTag(false); got != storecontract.ChangeRelPut {
		t.Fatalf("relPutTag(false) = %v, want ChangeRelPut", got)
	}
	if got := relPutTag(true); got != storecontract.ChangeForeignIncoming {
		t.Fatalf("relPutTag(true) = %v, want ChangeForeignIncoming", got)
	}
}

func recs(t *testing.T, bs *Store) []storecontract.ChangeRecord {
	t.Helper()
	out, err := bs.ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	return out
}
