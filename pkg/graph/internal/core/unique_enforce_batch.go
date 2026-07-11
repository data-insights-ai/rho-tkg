package core

import (
	"fmt"

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
func (c *Core) partitionBatchNodesByUnique(nodes []pendingNode) ([]pendingNode, []batchNodeUniqueViolation) {
	if !c.hasUniqueConstraints.Load() {
		return nodes, nil
	}
	survivors := make([]pendingNode, 0, len(nodes))
	var violators []batchNodeUniqueViolation
	seen := make(map[string]types.NodeID) // seen-key -> first claiming node in this batch

	for _, pn := range nodes {
		tuples, ferr := c.nodeUniqueValueKeys(pn.node, pn.labels)
		if ferr != nil {
			violators = append(violators, batchNodeUniqueViolation{pn: pn, id: pn.node.ID(), err: ferr})
			continue
		}
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
			// durable ownership registry (a value can be owned forever by an
			// entity that no longer holds it — supersession, hard delete — so
			// (b) alone misses it). Registry hit + different entity => violation;
			// same entity => pass; miss => claim + persist. This reuses the same
			// registry seam the standalone kernel uses (checkAndClaimForever takes
			// c.uniqueMu; the batch's exclusive c.mu.Lock already fences out any
			// concurrent standalone writer, so no value stripe is needed here).
			if tuple.scope == constraintspkg.UniqueForever {
				if err := c.checkAndClaimForever(tuple.labelTok, tuple.key, tuple.valueKey, pn.node.ID()); err != nil {
					vErr = err
					break
				}
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
	}
	return survivors, violators
}
