package core

import (
	"fmt"
	"sort"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// enforceUniqueForNodes is enforceUniqueForNodeHeld for GraphTx.AddNodes'
// fresh nodes, which all carry the same labels: all or nothing (BATCHADD).
// When those labels bind no constrained key it looks nothing up. Otherwise it
// locks the value stripes of every constrained value of every node as one
// sorted set (a transaction may hold only the shared lock, so a concurrent
// writer of a value serializes on its stripe), then checks every value
// read-only: a twin earlier in the call, a current holder or a UniqueForever
// owner refuses the whole call before any claim is made (BACKLOG 9e). Only
// then does it claim owner-index values and UniqueForever values; a failed
// claim withdraws every claim of the call.
//
// On success the caller writes the nodes, answers a failed write with
// storeWriteFailed on every hold, and calls release after the write (and the
// withdrawals). On an error nothing stays claimed or locked.
func (c *Core) enforceUniqueForNodes(nodes []*types.Node) (holds []*uniqueHold, release func(), err error) {
	release = func() {}
	if len(nodes) == 0 || !c.hasUniqueConstraints.Load() {
		return nil, release, nil
	}
	type constrainedKey struct {
		labelTok uint16
		key      string
		scope    constraintspkg.UniqueScope
	}
	first := nodes[0] // every node carries the same label tokens
	var keys []constrainedKey
	c.uniqueMu.RLock()
	for i := 0; i < first.LabelTokenCount(); i++ {
		labelTok := first.LabelTokenRawAt(i)
		for key, st := range c.uniqueConstraints[labelTok] {
			keys = append(keys, constrainedKey{labelTok: labelTok, key: key, scope: st.scope})
		}
	}
	c.uniqueMu.RUnlock()
	if len(keys) == 0 {
		return nil, release, nil
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].labelTok != keys[j].labelTok {
			return keys[i].labelTok < keys[j].labelTok
		}
		return keys[i].key < keys[j].key
	})

	// The constrained values of every node, in the nodes' order.
	checks := make([][]uniqueCheckTuple, len(nodes))
	var stripes []uint8
	for i, n := range nodes {
		for _, k := range keys {
			valueKey, found := uniqueValueKey(n, k.key)
			if !found || valueKey == "" {
				continue // unconstrained (missing key or unindexable value)
			}
			if isFloatValueKey(valueKey) {
				return nil, release, fmt.Errorf("graph: node %d of %d: %w: label %q key %q holds a float value",
					i, len(nodes), ErrUniqueUnsupportedType, c.labels.Resolve(k.labelTok), k.key)
			}
			raw, _ := n.GetProperty(k.key)
			checks[i] = append(checks[i], uniqueCheckTuple{labelTok: k.labelTok, key: k.key, raw: raw, valueKey: valueKey, scope: k.scope})
			stripes = append(stripes, uniqueValueStripe(k.labelTok, k.key, valueKey))
		}
	}
	if len(stripes) == 0 {
		return nil, release, nil
	}
	held := c.valueLocks.LockStripes(stripes)
	release = func() { c.valueLocks.UnlockStripes(held) }
	fail := func(err error) ([]*uniqueHold, func(), error) {
		release()
		return nil, func() {}, err
	}

	// Pass 1: every value of every node, read-only.
	seen := make(map[string]int, len(stripes)) // seen-key -> first node of the call
	for i, n := range nodes {
		for _, ck := range checks[i] {
			sk := uniqueSeenKey(ck.labelTok, ck.key, ck.valueKey)
			if j, ok := seen[sk]; ok {
				return fail(fmt.Errorf("graph: node %d of %d: %w: label %q key %q repeats node %d of the call",
					i, len(nodes), ErrUniqueViolation, c.labels.Resolve(ck.labelTok), ck.key, j))
			}
			seen[sk] = i
			if s := c.nodePutSeam; s != nil {
				s.lookups.Add(1)
			}
			matches, err := c.nodesByLabelAndProperty(ck.labelTok, ck.key, ck.raw, storepkg.QueryOpts{})
			if err != nil {
				return fail(fmt.Errorf("graph: unique constraint lookup: %w", err))
			}
			for _, m := range matches {
				if m.ID() == n.ID() {
					continue
				}
				return fail(fmt.Errorf("graph: node %d of %d: %w: label %q key %q already held by node %d",
					i, len(nodes), ErrUniqueViolation, c.labels.Resolve(ck.labelTok), ck.key, m.ID()))
			}
			if ck.scope == constraintspkg.UniqueForever {
				if err := c.checkForeverOwnership(ck.labelTok, ck.key, ck.valueKey, n.ID()); err != nil {
					return fail(fmt.Errorf("graph: node %d of %d: %w", i, len(nodes), err))
				}
			}
		}
	}

	// Owner index (UNIQIDX): claim each value before its row is written; a
	// claim no row backs is ignored by readers.
	for i, n := range nodes {
		for _, ck := range checks[i] {
			if oi, ok := c.ownerIndexFor(ck.labelTok, ck.key); ok {
				if err := oi.PutUniqueOwner(ck.labelTok, ck.key, ck.valueKey, n.ID()); err != nil {
					return fail(fmt.Errorf("graph: unique owner claim: %w", err))
				}
			}
		}
	}

	// Pass 2: claim each UniqueForever value. A failed claim withdraws every
	// claim of the call (nothing is written yet) under the held stripes.
	anyForever := false
	for _, k := range keys {
		anyForever = anyForever || k.scope == constraintspkg.UniqueForever
	}
	if !anyForever {
		return nil, release, nil
	}
	holds = make([]*uniqueHold, 0, len(nodes))
	for i, n := range nodes {
		hold := &uniqueHold{c: c, id: n.ID()}
		holds = append(holds, hold)
		for _, ck := range checks[i] {
			if ck.scope != constraintspkg.UniqueForever {
				continue
			}
			if err := hold.claim(ck); err != nil {
				for _, h := range holds {
					err = h.writeFailed(nil, err)
				}
				return fail(err)
			}
		}
	}
	return holds, release, nil
}
