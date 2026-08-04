package memory

import (
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Append-delta bookkeeping (R3), memory-store variant.
//
// THE DEFAULT IS POISON, and that inversion is the whole safety argument. Unlike
// badger, this store has no single add/remove seam — there are sixteen scattered
// bumpNodeEpoch() call sites, and classifying each as append-or-not is exactly the
// audit whose failure mode is a silently stale answer. The store's own field
// comment already rejects that trade ("a single missed mutation path would silently
// serve a stale aggregate, so correctness picks the impossible-to-under-fire global
// counter").
//
// So bumpNodeEpoch POISONS. Only a site that explicitly calls bumpNodeEpochAppend
// with the inserted node opts into the fast path. A write path left untouched —
// today's or one added later — keeps exactly today's rebuild behaviour. There is no
// way to opt in by omission.
//
// The epoch here is GLOBAL, not per-label, so a write to any label invalidates every
// label's columns. That is unchanged: the fast path only helps a label that itself
// received appends, and a cross-label write still forces the rebuild it forces
// today.

// maxAppendDeltaIDs bounds a label's pending appends; past it, extending is no
// longer clearly cheaper than rebuilding.
const maxAppendDeltaIDs = 50_000

// appendDeltaState is the store-wide record of pure inserts since the last
// non-append write. Guarded by ms.mu (every writer already holds it).
type appendDeltaState struct {
	byLabel  map[uint16][]types.NodeID
	epoch    uint64 // nodeEpoch immediately after the last recorded append
	poisoned bool   // some non-append write happened; rebuild until it is cleared
}

// bumpNodeEpochAppend records a PURE INSERT and bumps the epoch. Pass the inserted
// node; passing nil poisons instead, so an error path that never inserted anything
// cannot leave a phantom append behind.
func (ms *Store) bumpNodeEpochAppend(n *types.Node) {
	ms.bumpNodeEpochRaw()
	if n == nil {
		ms.appendDelta.poisoned = true
		ms.appendDelta.byLabel = nil
		return
	}
	count := n.LabelTokenCount()
	if count == 0 {
		ms.appendDelta.poisoned = true
		ms.appendDelta.byLabel = nil
		return
	}
	if ms.appendDelta.poisoned {
		return // still void; recording more would not make it usable
	}
	if ms.appendDelta.byLabel == nil {
		ms.appendDelta.byLabel = make(map[uint16][]types.NodeID)
	}
	id := n.ID()
	for i := 0; i < count; i++ {
		tok := n.LabelTokenRawAt(i)
		if tok == 0 {
			continue
		}
		if len(ms.appendDelta.byLabel[tok]) >= maxAppendDeltaIDs {
			ms.appendDelta.poisoned, ms.appendDelta.byLabel = true, nil
			return
		}
		ms.appendDelta.byLabel[tok] = append(ms.appendDelta.byLabel[tok], id)
	}
	ms.appendDelta.epoch = ms.nodeEpoch.Load()
}

// appendDeltaFor returns the IDs appended to a label since its snapshot was built,
// or ok=false if a rebuild is required. cur is the epoch the caller observed; a
// mismatch proves some write was not a recorded append. Caller must hold ms.mu.
func (ms *Store) appendDeltaFor(token uint16, cur, snapshotEpoch uint64) ([]types.NodeID, bool) {
	d := &ms.appendDelta
	if d.poisoned || d.byLabel == nil || d.epoch != cur {
		return nil, false
	}
	ids := d.byLabel[token]
	if len(ids) == 0 {
		return nil, false
	}
	// Accounting identity, as on the badger path. The epoch here is GLOBAL, so it
	// advances once per node write across ALL labels; the recorded appends for every
	// label must therefore account for the whole delta, not just this label's share.
	var recorded int
	for _, l := range d.byLabel {
		recorded += len(l)
	}
	if cur < snapshotEpoch || cur-snapshotEpoch != uint64(recorded) {
		return nil, false
	}
	out := make([]types.NodeID, len(ids))
	copy(out, ids)
	return out, true
}

// clearAppendDeltaFor drops a label's pending record once a snapshot covering those
// appends exists. Caller must hold ms.mu.
func (ms *Store) clearAppendDeltaFor(token uint16) {
	if ms.appendDelta.byLabel != nil {
		delete(ms.appendDelta.byLabel, token)
	}
}

// ColumnExtendCount reports how many columnar snapshots were refreshed by APPEND-
// EXTEND. Both refresh paths return identical data by construction, so this is what
// lets a test prove the append path actually fired rather than never firing.
func (ms *Store) ColumnExtendCount() uint64 { return ms.columnExtends.Load() }

// ColumnRebuildCount reports how many columnar snapshots were refreshed by a FULL
// REBUILD. Paired with ColumnExtendCount it is the builds-per-read telemetry the
// cache-versus-native-columnar decision needs.
func (ms *Store) ColumnRebuildCount() uint64 { return ms.columnRebuilds.Load() }
