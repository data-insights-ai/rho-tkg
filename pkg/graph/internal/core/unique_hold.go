package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// The claim hold shared by every unique-claiming door (tasks/backlog.md items
// 12 and 29).
//
// A passing unique check hands its door a uniqueHold: the value stripes it
// holds and the UniqueForever claims THIS call made (registry misses — a value
// the entity owned before the call is never recorded, so it is never
// withdrawn). The door keeps the hold until its last store write returns and
// answers a failed write with writeFailed BEFORE it releases the stripes, so a
// concurrent writer of the value serializes behind the withdrawal.
//
// Doors: the SetNodeVersionInterval cascade (enforceUniqueForCascade, which
// passes the rows it already wrote), and every door of enforceUniqueForNodeHeld
// — Add/AddWithTx, Import/AddByIDIfAbsent, GetOrCreateByKey, Update/
// UpdateWithTx, UpdateInPlace, CompareAndSetProperty, AddLabel, their GraphTx
// twins, BatchBuilder UpdateNode and the concurrent ingest creates — plus the
// batch create pre-check (partitionBatchNodesByUnique), which holds no stripes
// (the batch's exclusive c.mu.Lock fences writers). The single-row doors
// answer a failed write with storeWriteFailed, judging against the node's
// stored row.
//
// Limit: the claim is persisted before the row write, in a separate MetaKV
// write. A crash between the two leaves the claim without the row (barred,
// correctable via ReleaseOwnership); closing that needs one atomic MetaKV +
// store commit (v5 PLAN §5.2).
// =============================================================================

// uniqueHold is what a passing unique check hands its door. The zero value (no
// constraint bound, or a door that holds no stripes) is a no-op hold.
type uniqueHold struct {
	c      *Core
	id     types.NodeID
	held   []uint8
	claims []uniqueCheckTuple // claims THIS call made (registry misses)
}

// release unlocks the value stripes. Doors defer it, so the stripes are held
// across every store write and every withdrawal.
func (h *uniqueHold) release() {
	if h.c != nil && len(h.held) > 0 {
		h.c.valueLocks.UnlockStripes(h.held)
		h.held = nil
	}
}

// claim claims tp's UniqueForever value for h.id (claimForever) and records the
// claim when this call made it. The caller holds tp's value stripe (or the
// batch's exclusive c.mu.Lock).
func (h *uniqueHold) claim(tp uniqueCheckTuple) error {
	claimed, err := h.c.claimForever(tp.labelTok, tp.key, tp.valueKey, h.id)
	if err != nil {
		return err
	}
	if claimed {
		h.claims = append(h.claims, tp)
	}
	return nil
}

// writeFailed withdraws every claim this call made whose value no row in
// written carries (under the still-held stripes) and returns writeErr, joined
// with a withdrawal failure if there is one.
func (h *uniqueHold) writeFailed(written []*types.Node, writeErr error) error {
	if h.c == nil || len(h.claims) == 0 {
		return writeErr
	}
	var keys []string
	for _, tp := range h.claims {
		if !rowsCarryValue(written, tp) {
			keys = append(keys, foreverOwnerKey(tp.labelTok, tp.key, tp.valueKey))
		}
	}
	if err := h.c.withdrawForeverClaims(keys, h.id); err != nil {
		return errors.Join(writeErr, err)
	}
	return writeErr
}

// storeWriteFailed is writeFailed for a door that writes one node row: the
// rows that count as written are the node's stored current row, re-read under
// the still-held stripes (and the door's entity lock). A store that reports a
// failure after installing the row, or a partial create its cleanup could not
// remove, therefore keeps the claim; a write that stored nothing withdraws it.
// When the stored row cannot be read, every claim is kept (never admits a
// duplicate) and the read failure is joined to writeErr.
func (h *uniqueHold) storeWriteFailed(writeErr error) error {
	if h.c == nil || len(h.claims) == 0 {
		return writeErr
	}
	stored, err := h.c.getCurrentNode(h.id)
	switch {
	case err == nil:
		return h.writeFailed([]*types.Node{stored}, writeErr)
	case errors.Is(err, storepkg.ErrNodeNotFound):
		return h.writeFailed(nil, writeErr)
	default:
		return errors.Join(writeErr, fmt.Errorf("graph: unique-forever withdraw: read stored node %d: %w", h.id, err))
	}
}

// rowsCarryValue reports whether any row carries tp's label and value.
func rowsCarryValue(rows []*types.Node, tp uniqueCheckTuple) bool {
	for _, r := range rows {
		hasLabel := false
		for i := 0; i < r.LabelTokenCount(); i++ {
			if r.LabelTokenRawAt(i) == tp.labelTok {
				hasLabel = true
				break
			}
		}
		if !hasLabel {
			continue
		}
		if vk, ok := r.IndexablePropertyValueKey(tp.key); ok && vk == tp.valueKey {
			return true
		}
	}
	return false
}
