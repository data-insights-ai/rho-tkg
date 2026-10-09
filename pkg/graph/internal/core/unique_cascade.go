package core

import (
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
// patch is judged the way the update doors judge a finalized write:
//
//   - UniqueCurrent binds the CURRENT row. Only an open-ended cascade
//     (validTo == 0) replaces it: its open tail (the last correction piece,
//     base + patch) takes the current slot. A constrained value the patch
//     writes there is refused when ANOTHER current node holds it, and moving
//     off a value frees it. A bounded cascade leaves the current row's value
//     unchanged (the resumption re-asserts it), so its patch values are not
//     checked against UniqueCurrent: history duplicates are legal.
//   - UniqueForever binds every value ever written. Every constrained value
//     the patch writes, on any piece, is refused when another entity owns it
//     (or another current node holds it) and is claimed for the node when the
//     whole patch passes.
//
// Values the patch does not name, nil props, and a nil value (key delete)
// introduce nothing and are not checked. A float on a constrained key is
// refused with ErrUniqueUnsupportedType, as on the update doors.
//
// Locking (entity -> value -> idxMu): the cascade kernel calls this under the
// node's entity lock, before any row is built; the value stripes of every
// checked value (plus, for the open tail, the stripe of the current value it
// replaces) are held until the kernel returns, i.e. across every store write,
// so concurrent writers of one value serialize to exactly one winner. All four
// doors (Temporal, GraphTx, BatchBuilder, ingest Session in strong and
// concurrent mode) run the kernel, so all four enforce.
// =============================================================================

type cascadeUniqueTuple struct {
	labelTok uint16
	key      string
	raw      any
	valueKey string
	scope    constraintspkg.UniqueScope
}

// enforceUniqueForCascade checks the props patch of a node cascade against the
// node's unique constraints. current may be nil; history is the node's
// history in store order (the same chain the kernel builds its rows from). On
// success the caller defers the returned release; on error nothing is held
// and nothing is claimed.
func (c *Core) enforceUniqueForCascade(id types.NodeID, current *types.Node, history []*types.Node, newVF, newVT types.Instant, props map[string]any) (func(), error) {
	noop := func() {}
	if len(props) == 0 || !c.hasUniqueConstraints.Load() {
		return noop, nil
	}
	preChain := make([]*types.Node, 0, len(history)+1)
	preChain = append(preChain, history...)
	if current != nil {
		preChain = append(preChain, current)
	}
	if len(preChain) == 0 {
		return noop, nil
	}
	constrained := c.cascadeConstraintsFor(preChain, props)
	if len(constrained) == 0 {
		return noop, nil
	}

	template := current
	if template == nil {
		template = history[len(history)-1]
	}
	segs, err := c.nodeCorrectionSegments(preChain, template, newVF, newVT)
	if err != nil {
		// The kernel computes the same segments and surfaces this error.
		return noop, nil
	}

	tuples := make(map[string]cascadeUniqueTuple)
	var stripes []uint8
	for i, seg := range segs {
		// The open tail of an open-ended cascade becomes the current row.
		isCurrent := newVT == 0 && i == len(segs)-1
		row := seg.base.DeepCopy()
		if !applyCascadePatch(row, props) {
			return noop, nil // the kernel refuses the same patch with its own error
		}
		for li := 0; li < row.LabelTokenCount(); li++ {
			labelTok := row.LabelTokenRawAt(li)
			for key, scope := range constrained[labelTok] {
				valueKey, found := row.IndexablePropertyValueKey(key)
				if !found || valueKey == "" {
					continue
				}
				if isFloatValueKey(valueKey) {
					return noop, fmt.Errorf("%w: label %q key %q holds a float value", ErrUniqueUnsupportedType, c.labels.Resolve(labelTok), key)
				}
				if !isCurrent && scope != constraintspkg.UniqueForever {
					continue // a past slice is a legal history duplicate under UniqueCurrent
				}
				sk := uniqueSeenKey(labelTok, key, valueKey)
				if _, dup := tuples[sk]; !dup {
					raw, _ := row.GetProperty(key)
					tuples[sk] = cascadeUniqueTuple{labelTok: labelTok, key: key, raw: raw, valueKey: valueKey, scope: scope}
					stripes = append(stripes, uniqueValueStripe(labelTok, key, valueKey))
				}
				if isCurrent && current != nil {
					if oldKey, ok := current.IndexablePropertyValueKey(key); ok && oldKey != "" && oldKey != valueKey {
						stripes = append(stripes, uniqueValueStripe(labelTok, key, oldKey))
					}
				}
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

	held := c.valueLocks.LockStripes(stripes)
	release := func() { c.valueLocks.UnlockStripes(held) }

	// Pass 1: check every value read-only, so a refusal claims nothing.
	for _, tp := range ordered {
		matches, err := c.nodesByLabelAndProperty(tp.labelTok, tp.key, tp.raw, storepkg.QueryOpts{})
		if err != nil {
			release()
			return noop, fmt.Errorf("graph: unique constraint lookup: %w", err)
		}
		for _, m := range matches {
			if m.ID() == id {
				continue
			}
			release()
			return noop, fmt.Errorf("%w: label %q key %q already held by node %d",
				ErrUniqueViolation, c.labels.Resolve(tp.labelTok), tp.key, m.ID())
		}
		if tp.scope == constraintspkg.UniqueForever {
			if err := c.checkForeverOwnership(tp.labelTok, tp.key, tp.valueKey, id); err != nil {
				release()
				return noop, err
			}
		}
	}
	// Pass 2: claim every UniqueForever value.
	for _, tp := range ordered {
		if tp.scope != constraintspkg.UniqueForever {
			continue
		}
		if err := c.checkAndClaimForever(tp.labelTok, tp.key, tp.valueKey, id); err != nil {
			release()
			return noop, err
		}
	}
	return release, nil
}

// cascadeConstraintsFor returns, per label token any row of the chain carries,
// the constrained keys the patch WRITES (a non-nil value) with their scope.
// Empty when the patch cannot introduce a constrained value.
func (c *Core) cascadeConstraintsFor(chain []*types.Node, props map[string]any) map[uint16]map[string]constraintspkg.UniqueScope {
	c.uniqueMu.RLock()
	defer c.uniqueMu.RUnlock()
	var out map[uint16]map[string]constraintspkg.UniqueScope
	for _, row := range chain {
		for i := 0; i < row.LabelTokenCount(); i++ {
			labelTok := row.LabelTokenRawAt(i)
			if _, seen := out[labelTok]; seen {
				continue
			}
			for key, st := range c.uniqueConstraints[labelTok] {
				if v, ok := props[key]; !ok || v == nil {
					continue
				}
				if out == nil {
					out = make(map[uint16]map[string]constraintspkg.UniqueScope)
				}
				if out[labelTok] == nil {
					out[labelTok] = make(map[string]constraintspkg.UniqueScope)
				}
				out[labelTok][key] = st.scope
			}
		}
	}
	return out
}

// applyCascadePatch applies props to row the way buildNodeCorrectionRow does
// (nil deletes a key). False when the patch does not apply.
func applyCascadePatch(row *types.Node, props map[string]any) bool {
	for key, val := range props {
		if val == nil {
			if _, err := row.DeleteProperty(key); err != nil {
				return false
			}
		} else if err := row.SetProperty(key, val); err != nil {
			return false
		}
	}
	return true
}
