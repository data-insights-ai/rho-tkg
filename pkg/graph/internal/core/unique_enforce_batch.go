package core

import (
	"fmt"
	"sort"

	constraintspkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/constraints"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Unique constraint enforcement — batch create path (ADR-0002 Stage D).
//
// BatchBuilder node creates land through store.PutNodesBatch, NOT the standalone
// addNodeInternal door, so they bypass enforceUniqueForNode. Execute holds
// c.mu.Lock EXCLUSIVELY for its whole duration, so committed state is stable and
// no concurrent standalone writer can interleave — the batch pre-check needs no
// value locks; a batch-local seen-map is enough to catch two same-value creates
// inside one batch (the SECOND fails at op time, per the ADR). Violating nodes
// are removed from the create set and surfaced as failed ops.
// =============================================================================

// batchNodeUniqueViolation records one pending node rejected by a unique
// constraint during batch pre-check.
type batchNodeUniqueViolation struct {
	pn  pendingNode
	id  types.NodeID
	err error
}

// nodeUniqueValueKeys collects the constrained (labelToken, key, canonical
// value-key) tuples a node binds, using the CURRENT (in-memory registry) view of
// active constraints. Returns a float-unsupported error if any constrained key
// on the node holds a float value. Caller resolves label strings to tokens (a
// brand-new label absent from the registry cannot carry an active constraint, so
// it is simply skipped).
func (c *Core) nodeUniqueValueKeys(node *types.Node, labels []string) (map[string]uniqueCheckTuple, error) {
	if node == nil || !c.hasUniqueConstraints.Load() {
		return nil, nil
	}
	out := make(map[string]uniqueCheckTuple)
	c.uniqueMu.RLock()
	defer c.uniqueMu.RUnlock()
	if len(c.uniqueConstraints) == 0 {
		return nil, nil
	}
	for _, label := range labels {
		labelTok, ok := c.labels.Lookup(label)
		if !ok {
			continue // absent label cannot carry an active constraint
		}
		byKey, ok := c.uniqueConstraints[labelTok]
		if !ok {
			continue
		}
		for key, st := range byKey {
			valueKey, found := node.IndexablePropertyValueKey(key)
			if !found || valueKey == "" {
				continue
			}
			if isFloatValueKey(valueKey) {
				return nil, fmt.Errorf("%w: label %q key %q holds a float value", ErrUniqueUnsupportedType, label, key)
			}
			raw, _ := node.GetProperty(key)
			out[uniqueSeenKey(labelTok, key, valueKey)] = uniqueCheckTuple{
				labelTok: labelTok,
				key:      key,
				raw:      raw,
				valueKey: valueKey,
				scope:    st.scope,
			}
		}
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

type uniqueCheckTuple struct {
	labelTok uint16
	key      string
	raw      any
	valueKey string
	scope    constraintspkg.UniqueScope
}

func uniqueSeenKey(labelTok uint16, key, valueKey string) string {
	return fmt.Sprintf("%d\x00%s\x00%s", labelTok, key, valueKey)
}

// partitionBatchNodesByUnique splits pending node creates into those that pass
// unique-constraint enforcement (survivors, preserving order) and those that do
// not (violators). Runs under the batch's exclusive c.mu.Lock. A value is a
// violation if it is already held by a committed CURRENT node OR by an EARLIER
// pending node in the same batch.
//
// holds[i] is the claim hold of survivors[i] (no stripes: the exclusive lock
// fences writers). When the batch then fails to store the survivors, Execute
// answers with holds[i].storeWriteFailed so a node that was never stored owns
// nothing (item 29).
func (c *Core) partitionBatchNodesByUnique(nodes []pendingNode) (survivors []pendingNode, holds []*uniqueHold, violators []batchNodeUniqueViolation) {
	if !c.hasUniqueConstraints.Load() {
		return nodes, nil, nil
	}
	survivors = make([]pendingNode, 0, len(nodes))
	holds = make([]*uniqueHold, 0, len(nodes))
	seen := make(map[string]types.NodeID) // seen-key -> first claiming node in this batch

	for _, pn := range nodes {
		tuples, ferr := c.nodeUniqueValueKeys(pn.node, pn.labels)
		if ferr != nil {
			violators = append(violators, batchNodeUniqueViolation{pn: pn, id: pn.node.ID(), err: ferr})
			continue
		}
		// Pass 1: check EVERY tuple this node binds, read-only, before claiming
		// anything. A node with more than one constrained tuple (e.g. two
		// UniqueForever keys) that claimed each tuple as it was checked could
		// fail on a LATER tuple and be rejected as a violator while an
		// EARLIER tuple's claim stayed durably persisted — a permanent
		// orphaned ownership claim on a node that never actually landed
		// (BACKLOG 9e, batch mirror of the standalone-door fix). The batch's
		// exclusive c.mu.Lock already fences out any concurrent standalone
		// writer for the whole partition pass, so no TOCTOU window exists
		// between this read-only pass and the claim pass below.
		var vErr error
		for seenKey, tuple := range tuples {
			// (a) earlier pending node in THIS batch already claimed it.
			if first, ok := seen[seenKey]; ok {
				vErr = fmt.Errorf("%w: label token %d key %q already claimed by node %d earlier in this batch",
					ErrUniqueViolation, tuple.labelTok, tuple.key, first)
				break
			}
			// (b) a committed current node already holds it.
			matches, err := c.nodesByLabelAndProperty(tuple.labelTok, tuple.key, tuple.raw, storepkg.QueryOpts{})
			if err != nil {
				vErr = fmt.Errorf("graph: batch unique lookup: %w", err)
				break
			}
			for _, m := range matches {
				if m.ID() == pn.node.ID() {
					continue
				}
				vErr = fmt.Errorf("%w: label token %d key %q already held by node %d",
					ErrUniqueViolation, tuple.labelTok, tuple.key, m.ID())
				break
			}
			if vErr != nil {
				break
			}
			// (c) UniqueForever: the current-state check passed; consult the
			// durable ownership registry READ-ONLY (a value can be owned
			// forever by an entity that no longer holds it — supersession,
			// hard delete — so (b) alone misses it). Claiming happens in the
			// second pass below, only after every tuple has passed.
			if tuple.scope == constraintspkg.UniqueForever {
				if err := c.checkForeverOwnership(tuple.labelTok, tuple.key, tuple.valueKey, pn.node.ID()); err != nil {
					vErr = err
					break
				}
			}
		}
		if vErr != nil {
			violators = append(violators, batchNodeUniqueViolation{pn: pn, id: pn.node.ID(), err: vErr})
			continue
		}
		// Pass 2: every tuple passed — now durably claim each UniqueForever
		// value through the shared claim hold (the batch's exclusive c.mu.Lock
		// already fences out any concurrent standalone writer, so no value
		// stripe is needed here). A claim that fails withdraws the claims this
		// node already made (item 29). Deterministic order, as the kernel.
		ordered := make([]uniqueCheckTuple, 0, len(tuples))
		for _, tuple := range tuples {
			ordered = append(ordered, tuple)
		}
		sort.Slice(ordered, func(i, j int) bool {
			return uniqueSeenKey(ordered[i].labelTok, ordered[i].key, ordered[i].valueKey) <
				uniqueSeenKey(ordered[j].labelTok, ordered[j].key, ordered[j].valueKey)
		})
		hold := &uniqueHold{c: c, id: pn.node.ID()}
		for _, tuple := range ordered {
			if tuple.scope != constraintspkg.UniqueForever {
				continue
			}
			if err := hold.claim(tuple); err != nil {
				vErr = hold.storeWriteFailed(err)
				break
			}
		}
		if vErr != nil {
			violators = append(violators, batchNodeUniqueViolation{pn: pn, id: pn.node.ID(), err: vErr})
			continue
		}
		// Record this node's claims so a later same-value node in the batch fails.
		for seenKey := range tuples {
			seen[seenKey] = pn.node.ID()
		}
		survivors = append(survivors, pn)
		holds = append(holds, hold)
	}
	return survivors, holds, violators
}
