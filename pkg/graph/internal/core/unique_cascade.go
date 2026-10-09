package core

import (
	"errors"
	"fmt"
	"sort"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Unique constraints on the cascade doors (tasks/backlog.md item 12).
//
// SetNodeVersionInterval appends rows built from a props PATCH over the state
// valid at each instant of [validFrom, validTo) (nodeCorrectionSegments). The
// kernel judges its BUILT rows the way the update doors judge a finalized
// write:
//
//   - UniqueCurrent binds the CURRENT row. When an appended row takes the
//     current slot (the open tail of an open-ended cascade), every constrained
//     value on it that differs from the replaced current row is refused when
//     ANOTHER current node holds it; moving off a value frees it. A bounded
//     cascade leaves the current row's value unchanged (the resumption
//     re-asserts it), so its correction rows are not checked against
//     UniqueCurrent: history duplicates are legal.
//   - UniqueForever binds every value ever written. Every constrained value the
//     patch writes on a correction row (any piece) is refused when another
//     entity owns it (or another current node holds it) and is claimed for the
//     node when every check passes. Values a row carries from its base row
//     were written before and are not re-judged.
//
// A patch that names no constrained key, nil props, and a key delete introduce
// nothing. A float on a constrained key is refused with
// ErrUniqueUnsupportedType, as on the update doors.
//
// Placement: the kernel calls this after every row is built, versioned, hashed
// and the current-slot replacement validated, right before its first store
// write — so every kernel refusal (deleted entity, version overflow,
// ErrTooManyProperties, hash, replacement) happens before any UniqueForever
// claim. When a store write then fails, the kernel withdraws every claim this
// call made whose value no already-written row carries (writeFailed): a value
// that reached a stored row was written and stays owned. Locking (entity -> value -> idxMu): the stripes of every
// checked value, plus the replaced current value's, are held until the kernel
// returns, across every store write, so concurrent writers of one value
// serialize to exactly one winner. All four doors (Temporal, GraphTx,
// BatchBuilder, ingest Session in strong and concurrent mode) run the kernel.
// =============================================================================

// cascadeUniqueHold is what a passing check hands the kernel: the value
// stripes it holds and the UniqueForever claims it made. The zero value (no
// constraint bound) is a no-op hold.
type cascadeUniqueHold struct {
	c      *Core
	id     types.NodeID
	held   []uint8
	claims []cascadeUniqueTuple // claims THIS call made (registry misses)
}

// release unlocks the value stripes. The kernel defers it, so the stripes are
// held across every store write.
func (h *cascadeUniqueHold) release() {
	if h.c != nil && len(h.held) > 0 {
		h.c.valueLocks.UnlockStripes(h.held)
		h.held = nil
	}
}

// writeFailed withdraws every claim this call made whose value no row in
// written carries (under the still-held stripes) and returns writeErr, joined
// with a withdrawal failure if there is one.
func (h *cascadeUniqueHold) writeFailed(written []*types.Node, writeErr error) error {
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
func rowsCarryValue(rows []*types.Node, tp cascadeUniqueTuple) bool {
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

type cascadeUniqueTuple struct {
	labelTok uint16
	key      string
	raw      any
	valueKey string
	scope    constraintspkg.UniqueScope
}

// enforceUniqueForCascade checks the rows a node cascade is about to write.
// appended are the built rows; newCurrent is the row that takes the current
// slot when curIsNew (current is the row it replaces, nil if none). A row is a
// correction piece when its ValidFrom lies in [newVF, newVT) (newVT == 0:
// open), else it is the resumption. On success the caller defers the
// hold's release and calls hold.writeFailed when a store write fails; on
// error nothing is held and nothing is claimed.
func (c *Core) enforceUniqueForCascade(id types.NodeID, current *types.Node, appended []*types.Node, newCurrent *types.Node, curIsNew bool, newVT types.Instant, props map[string]any) (*cascadeUniqueHold, error) {
	noop := &cascadeUniqueHold{}
	if len(appended) == 0 || !c.hasUniqueConstraints.Load() {
		return noop, nil
	}
	constrained := c.cascadeConstraintsFor(appended)
	if len(constrained) == 0 {
		return noop, nil
	}

	tuples := make(map[string]cascadeUniqueTuple)
	var stripes []uint8
	add := func(row *types.Node, labelTok uint16, key, valueKey string, scope constraintspkg.UniqueScope) {
		sk := uniqueSeenKey(labelTok, key, valueKey)
		if _, dup := tuples[sk]; dup {
			return
		}
		raw, _ := row.GetProperty(key)
		tuples[sk] = cascadeUniqueTuple{labelTok: labelTok, key: key, raw: raw, valueKey: valueKey, scope: scope}
		stripes = append(stripes, uniqueValueStripe(labelTok, key, valueKey))
	}
	for _, row := range appended {
		isCurrent := curIsNew && row == newCurrent
		isCorrection := newVT == 0 || (row.Temporal() != nil && row.Temporal().ValidFrom < newVT)
		for li := 0; li < row.LabelTokenCount(); li++ {
			labelTok := row.LabelTokenRawAt(li)
			for key, scope := range constrained[labelTok] {
				valueKey, found := row.IndexablePropertyValueKey(key)
				if !found || valueKey == "" {
					continue
				}
				oldKey := ""
				if current != nil {
					oldKey, _ = current.IndexablePropertyValueKey(key)
				}
				patched := false
				if v, ok := props[key]; ok && v != nil && isCorrection {
					patched = true
				}
				changesCurrent := isCurrent && valueKey != oldKey
				if !changesCurrent && !patched {
					continue // carried from a row written before: not re-judged
				}
				if isFloatValueKey(valueKey) {
					return noop, fmt.Errorf("%w: label %q key %q holds a float value", ErrUniqueUnsupportedType, c.labels.Resolve(labelTok), key)
				}
				if changesCurrent {
					add(row, labelTok, key, valueKey, scope)
					if oldKey != "" {
						stripes = append(stripes, uniqueValueStripe(labelTok, key, oldKey))
					}
					continue
				}
				if scope == constraintspkg.UniqueForever {
					add(row, labelTok, key, valueKey, scope)
				}
				// UniqueCurrent: a past slice (or the unchanged current value) is
				// a legal history duplicate.
			}
		}
	}
	if len(tuples) == 0 {
		return noop, nil
	}
	ordered := make([]cascadeUniqueTuple, 0, len(tuples))
	for _, tp := range tuples {
		ordered = append(ordered, tp)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return uniqueSeenKey(ordered[i].labelTok, ordered[i].key, ordered[i].valueKey) <
			uniqueSeenKey(ordered[j].labelTok, ordered[j].key, ordered[j].valueKey)
	})

	hold := &cascadeUniqueHold{c: c, id: id, held: c.valueLocks.LockStripes(stripes)}
	keep := false
	defer func() {
		if !keep {
			hold.release()
		}
	}()

	// Pass 1: check every value read-only, so a refusal claims nothing.
	for _, tp := range ordered {
		matches, err := c.nodesByLabelAndProperty(tp.labelTok, tp.key, tp.raw, storepkg.QueryOpts{})
		if err != nil {
			return noop, fmt.Errorf("graph: unique constraint lookup: %w", err)
		}
		for _, m := range matches {
			if m.ID() != id {
				return noop, fmt.Errorf("%w: label %q key %q already held by node %d",
					ErrUniqueViolation, c.labels.Resolve(tp.labelTok), tp.key, m.ID())
			}
		}
		if tp.scope == constraintspkg.UniqueForever {
			if err := c.checkForeverOwnership(tp.labelTok, tp.key, tp.valueKey, id); err != nil {
				return noop, err
			}
		}
	}
	// Pass 2: claim every UniqueForever value; a failed claim withdraws the
	// ones this pass already made (nothing was written).
	for _, tp := range ordered {
		if tp.scope != constraintspkg.UniqueForever {
			continue
		}
		claimed, err := c.claimForever(tp.labelTok, tp.key, tp.valueKey, id)
		if err != nil {
			return noop, hold.writeFailed(nil, err)
		}
		if claimed {
			hold.claims = append(hold.claims, tp)
		}
	}
	keep = true
	return hold, nil
}

// cascadeConstraintsFor returns, per label token the rows carry, the
// constrained keys with their scope (a snapshot under uniqueMu).
func (c *Core) cascadeConstraintsFor(rows []*types.Node) map[uint16]map[string]constraintspkg.UniqueScope {
	c.uniqueMu.RLock()
	defer c.uniqueMu.RUnlock()
	var out map[uint16]map[string]constraintspkg.UniqueScope
	for _, row := range rows {
		for i := 0; i < row.LabelTokenCount(); i++ {
			labelTok := row.LabelTokenRawAt(i)
			if _, seen := out[labelTok]; seen {
				continue
			}
			byKey := c.uniqueConstraints[labelTok]
			if len(byKey) == 0 {
				continue
			}
			if out == nil {
				out = make(map[uint16]map[string]constraintspkg.UniqueScope)
			}
			m := make(map[string]constraintspkg.UniqueScope, len(byKey))
			for key, st := range byKey {
				m[key] = st.scope
			}
			out[labelTok] = m
		}
	}
	return out
}
