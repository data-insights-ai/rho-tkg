package core

import (
	"errors"

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
