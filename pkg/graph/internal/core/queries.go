package core

import (
	"errors"
	"slices"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func (c *Core) validateRelTypeQueryName(typeName string) error {
	if _, ok := c.cachedRelType(typeName); ok {
		return nil
	}
	return c.validateIndexName(typeName)
}

func (c *Core) lookupRelTypeQueryToken(typeName string) (uint16, bool) {
	if tok, ok := c.cachedRelType(typeName); ok {
		return tok, true
	}
	return c.relTypes.Lookup(typeName)
}

// --- Store passthrough queries ---
//
// Get lives in node_delete.go / relationship_delete.go. Every public read
// has a paired (*Core).*Locked helper that runs the body without c.mu; the
// public method wraps it in c.mu.RLock, the tx mirror (tx_consistent_reads.go)
// calls it under tx.mu and its own brief c.mu.RLock (lockActiveCore), because
// RWMutex is not reentrant (lesson 31). Input validation stays at the public
// layer so both paths check alike.

// ByLabel returns nodes with the label (resolved from string), with optional
// pagination; nil if the label is not registered. With a temporal filter
// (ValidAt or ValidStart/ValidEnd) it is history-aware: every node (current +
// history) is scanned and a version whose label set held the label at that
// time matches. Otherwise it uses the store label index.
func (n *NodeOps) ByLabel(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateIndexLabel(label); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByLabelLocked(label, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// nodeRangeCardinalityScanner is the OPTIONAL store capability behind
// NodeOps.RangeCardinality (bit-sliced index count, no node scan).
type nodeRangeCardinalityScanner interface {
	NodeRangeCardinality(labelToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error)
}

// RangeCardinality counts the label's nodes whose numeric propKey value lies
// in [min,max] (inclusivity per flags) from the property index, no node scan:
// prefix sums when the index has PropertyIndexOptions.RangeCounts, else
// summed per-value buckets. exact=false declines (caller scans and counts)
// when the store lacks the capability, the index is absent or poisoned (an
// integer past 2^53), or a temporal filter is set (the index is valid-time
// agnostic). The bounds MUST capture the whole predicate; the caller
// enforces that.
func (n *NodeOps) RangeCardinality(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if opts.HasTimeCoordinate() {
		return 0, false, nil // temporal — the BSI cannot answer; caller scans
	}
	scanner, native := c.store.(nodeRangeCardinalityScanner)
	if !native {
		return 0, false, nil
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupLabelLocked(label)
		return nil
	}); err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil // unknown label — caller scans (finds zero)
	}
	return scanner.NodeRangeCardinality(tok, propKey, min, max, inclMin, inclMax)
}

// relRangeCardinalityScanner is the OPTIONAL store capability behind
// RelOps.RangeCardinality — the relationship mirror of nodeRangeCardinalityScanner
// (memory, badger, sharded and, since v4.59.0, tiered).
type relRangeCardinalityScanner interface {
	RelRangeCardinality(relTypeToken uint16, propKey string, min, max float64, inclMin, inclMax bool) (int64, bool, error)
}

// RangeCardinality is the relationship mirror of NodeOps.RangeCardinality,
// summed from the rel property index's buckets, no rel scan. exact=false
// declines when the store lacks the capability, the index is absent
// or poisoned, or a temporal filter is set. Bounds MUST capture the whole
// predicate. Backs the ORDER BY r.prop LIMIT k push-down.
func (r *RelOps) RangeCardinality(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts) (int64, bool, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, false, err
	}
	if opts.HasTimeCoordinate() {
		return 0, false, nil // temporal — the index cannot answer; caller scans
	}
	scanner, native := c.store.(relRangeCardinalityScanner)
	if !native {
		return 0, false, nil
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return 0, false, err
	}
	if !ok {
		return 0, false, nil // unknown rel type — caller scans (finds zero)
	}
	return scanner.RelRangeCardinality(tok, propKey, min, max, inclMin, inclMax)
}

// nodeLabelScanner is the OPTIONAL store capability behind
// NodeOps.ForEachByLabel: label scans with O(1) peak memory. Stores without
// it use the materializing path.
type nodeLabelScanner interface {
	ForEachNodeByLabel(token uint16, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
}

// ForEachByLabel streams nodes carrying the label to fn in snowflake-ID
// order; fn returning false stops early. The streaming sibling of ByLabel
// for scan consumers that must not materialize the result.
//
// Isolation is RELAXED: the IDs are snapshotted, then rows are fetched and fn
// called without graph locks. fn may call back into the graph; concurrent
// writers are neither blocked nor seen atomically. Rows are shared frozen
// pointers; fn must not mutate them. Deleted rows are skipped, new rows are not seen.
//
// A temporal-filter query streams too: ByLabel's answers in ascending ID
// order, resolved labelScanAtChunk at a time under the read lock and handed
// to fn with no lock held (label_scan_at.go). A non-native store routes a
// current-state scan through ByLabel.
func (n *NodeOps) ForEachByLabel(label string, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return err
	}

	if hasTemporalFilter(opts) {
		// Ascending ID order; fn runs with no lock held (label_scan_at.go).
		return c.forEachNodeByLabelAt(label, opts, fn)
	}
	scanner, native := c.store.(nodeLabelScanner)
	if !native || !c.storeRowsTrust {
		nodes, err := n.ByLabel(label, opts)
		if err != nil {
			return err
		}
		for _, nd := range nodes {
			if !fn(nd) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see the isolation note above.
	return scanner.ForEachNodeByLabel(tok, opts, fn)
}

// nodeRangeScanner is the OPTIONAL store capability behind
// NodeOps.ForEachByLabelPropertyRange — streaming numeric range scans over
// a property index's ordered view (ordered range view). Implemented by the badger store.
type nodeRangeScanner interface {
	ForEachNodeByLabelPropertyRange(token uint16, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error
}

// ForEachByLabelPropertyRange streams nodes carrying the label whose
// NUMERIC propKey value lies in [min, max], in snowflake-ID order. Candidates
// come from the index's ordered numeric view, which OVER-SELECTS (float64
// sort keys, ulp-widened bounds): fn must re-check with exact comparison.
// Returns storepkg.ErrIndexNotFound when no usable index exists or the store
// lacks the capability. Same isolation and frozen-row contract as
// ForEachByLabel.
//
// A TEMPORAL QueryOpts is served by a full fold (forEachNodeInRangeTemporal):
// the version ByLabel resolves, kept when its value AT THE PIN is in range.
// No index needed, never ErrIndexNotFound. Under ValidStart+ValidEnd the
// range tests the ONE resolved version, unlike ByLabelAndProperty, which
// matches a value held anywhere in the interval (backlog 16).
func (n *NodeOps) ForEachByLabelPropertyRange(label, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return err
	}
	if hasTemporalFilter(opts) {
		return forEachNodeInRangeTemporal(c, label, propKey, min, max, opts, fn)
	}
	scanner, native := c.store.(nodeRangeScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachNodeByLabelPropertyRange(tok, propKey, min, max, inclMin, inclMax, opts, fn)
}

// ForEachByLabelPropertyRangeOrdered streams nodes carrying the label whose
// NUMERIC propKey value lies in [min, max] to fn in CONTRACTUAL VALUE ORDER
// (ascending, or descending when desc), ties by node ID ASCENDING in both
// directions. A query layer compiling ORDER BY n.prop LIMIT k returns false
// from fn after k rows, so the scan stops at O(k + log n) index work.
//
// Candidates come from the ordered numeric view, which OVER-SELECTS: fn MUST
// re-check the exact bounds (int64 magnitudes past 2^53 collapse onto
// neighbouring sort keys, so the door never skips a boundary bucket).
//
// Non-temporal opts use the index (ErrIndexNotFound when no usable ordered
// view exists or the store lacks the capability). A TEMPORAL QueryOpts is
// served by a sound full fold (Stage B): every member resolved to its version
// at the pin, filtered and sorted on the value AT t, O(N log N), no index.
// Same isolation and frozen-row contract as ForEachByLabel.
func (n *NodeOps) ForEachByLabelPropertyRangeOrdered(label, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	// Temporal ordered scan (Stage B): value-at-t is not indexed, so fold every
	// label member's version at the pin, keep in-range values, sort by value.
	if opts.HasTimeCoordinate() {
		if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
			return err
		}
		return forEachNodeValueOrderedTemporal(c, label, opts, desc,
			func(nd *types.Node) (float64, bool) {
				v, ok := nd.GetProperty(propKey)
				if !ok {
					return 0, false
				}
				f, ok := types.NumericFloat64(v)
				if !ok || !numericInRange(f, min, max, true, true) { // over-select; fn re-checks inclusivity
					return 0, false
				}
				return f, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner := c.nodeOrderedRange
	if scanner == nil {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	visit, rowErr := c.trustedNodeVisit(fn)
	if err := scanner.ForEachNodeByLabelPropertyRangeOrdered(tok, propKey, min, max, inclMin, inclMax, desc, visit); err != nil {
		return err
	}
	return *rowErr
}

// ForEachByLabelPropertyPrefix streams nodes carrying the label whose STRING
// propKey value begins with prefix to fn in CONTRACTUAL VALUE ORDER
// (lexicographic ascending, or descending when desc), ties by node ID
// ASCENDING in both directions. A LIMIT k is pushed into the index by
// returning false from fn after k rows. An empty prefix matches every string
// value.
//
// Candidates come from the ordered STRING view, which is EXACT (string sort
// keys never collide), so fn receives rows already matching the prefix.
//
// Non-temporal opts use the index (ErrIndexNotFound when no usable ordered
// view exists or the store lacks the capability). A TEMPORAL QueryOpts is
// served by a sound full fold (Stage B), O(N log N), no index. Same
// isolation and frozen-row contract as ForEachByLabel.
func (n *NodeOps) ForEachByLabelPropertyPrefix(label, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateIndexLabel(label); err != nil {
		return err
	}
	// Temporal prefix scan (Stage B): value-at-t is not indexed, so fold every
	// label member's version at the pin, keep matching strings, sort.
	if opts.HasTimeCoordinate() {
		if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
			return err
		}
		return forEachNodeValueOrderedTemporal(c, label, opts, desc,
			func(nd *types.Node) (string, bool) {
				v, ok := nd.GetProperty(propKey)
				if !ok {
					return "", false
				}
				s, ok := v.(string)
				if !ok || !strings.HasPrefix(s, prefix) {
					return "", false
				}
				return s, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner := c.nodePrefixScan
	if scanner == nil {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.labels.Lookup(label)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	visit, rowErr := c.trustedNodeVisit(fn)
	if err := scanner.ForEachNodeByLabelPropertyPrefix(tok, propKey, prefix, desc, visit); err != nil {
		return err
	}
	return *rowErr
}

// nodeLabelCandidatesAt gathers the candidate IDs a label read at a temporal
// coordinate resolves (nodesByLabelLocked, countNodesByLabelLocked), with the
// label's current rows the gather reads. Callers hold c.mu (R or W).
func (c *Core) nodeLabelCandidatesAt(tok uint16, opts storepkg.QueryOpts) ([]*types.Node, []types.NodeID, error) {
	current, err := c.store.NodesByLabel(tok, storepkg.QueryOpts{Depth: opts.Depth})
	if err != nil {
		return nil, nil, err
	}
	currentIDs, err := c.nodeIDsFromLabelRows(tok, current)
	if err != nil {
		return nil, nil, err
	}
	candIDs, err := c.nodeLabelCandidatesFrom(tok, currentIDs, opts)
	if err != nil {
		return nil, nil, err
	}
	return current, candIDs, nil
}

// nodeLabelCandidateIDsAt is nodeLabelCandidatesAt for callers that resolve
// every candidate themselves and need no current row: current members come
// from store.MemberIDsCapability without reading rows (backlog 33), else
// from the rows. Those stores keep no tiers, so opts.Depth selects nothing.
// Callers hold c.mu (R or W).
func (c *Core) nodeLabelCandidateIDsAt(tok uint16, opts storepkg.QueryOpts) ([]types.NodeID, error) {
	if c.memberIDs == nil {
		_, ids, err := c.nodeLabelCandidatesAt(tok, opts)
		return ids, err
	}
	currentIDs, err := c.memberIDs.NodeIDsByLabel(tok)
	if err != nil {
		return nil, err
	}
	return c.nodeLabelCandidatesFrom(tok, currentIDs, opts)
}

// nodeLabelCandidatesFrom widens the label's current members to the candidate
// set of a read at opts (the ever-members or the history fold, then the
// valid-time envelope prune). Callers hold c.mu (R or W).
func (c *Core) nodeLabelCandidatesFrom(tok uint16, currentIDs []types.NodeID, opts storepkg.QueryOpts) ([]types.NodeID, error) {

	// Gather candidate IDs. K1: with a transaction-time label-membership
	// sidecar, scope to the label's ever-members (O(matches)) instead of folding
	// all node history; otherwise fold by depth. The chain resolver
	// (findNodeVersionForOpts) stays the authority: the sidecar and the B4
	// envelope prune are sound supersets. Wrapped stores decline the sidecar; a
	// depth-scoped read on tiered keeps the depth fold.
	//
	// Candidates come in ascending ID order (current members merged with the
	// gathered IDs), so a read walks forward once and never sorts
	// (label_scan_at.go).
	candIDs, err := c.nodeLabelCandidatesSorted(tok, currentIDs, opts)
	if err != nil {
		return nil, err
	}

	return c.pruneNodeLabelCandidates(tok, candIDs, opts), nil
}

// pruneNodeLabelCandidates drops the ids of label tok whose valid-time
// ENVELOPE (CreateTemporal) cannot overlap opts' valid-time filter, without
// loading their chains; ids are unchanged when opts has no valid-time filter
// or no envelope covers the label. The envelope is a sound superset over every
// row of the entity (current and history, any transaction time): pruning is
// positive-evidence-only. Ascending input stays ascending. The one seam of the
// node envelope prune: the ByLabel doors and the paged effective door
// (backlog 75). Callers hold c.mu (R or W).
func (c *Core) pruneNodeLabelCandidates(tok uint16, ids []types.NodeID, opts storepkg.QueryOpts) []types.NodeID {
	if c.temporalCandidates == nil {
		return ids
	}
	sorted := slices.IsSorted(ids)
	kept, ok := c.temporalCandidates.PruneTemporalCandidates(tok, ids, opts)
	if !ok {
		return ids
	}
	if sorted && !slices.IsSorted(kept) {
		slices.Sort(kept)
	}
	return kept
}

// nodeLabelCandidatesSorted is the gather of nodeLabelCandidatesFrom in
// ascending ID order: K1 ever-members (pruned by the tx-time pin) or the
// history fold by depth, united with currentIDs.
func (c *Core) nodeLabelCandidatesSorted(tok uint16, currentIDs []types.NodeID, opts storepkg.QueryOpts) ([]types.NodeID, error) {
	if !slices.IsSorted(currentIDs) {
		currentIDs = slices.Sorted(slices.Values(currentIDs))
	}
	var extra []types.NodeID
	add := func(id types.NodeID) {
		if !inSorted(currentIDs, id) {
			extra = append(extra, id)
		}
	}
	if c.labelTxMembers != nil && c.membershipSidecarUsable(opts.Depth) {
		pinTx := effectiveTxPin(opts)
		if err := c.labelTxMembers.ForEachLabelTxMember(tok, func(id types.NodeID, firstTx types.Instant) bool {
			if pinTx == 0 || firstTx == 0 || firstTx <= pinTx {
				add(id)
			}
			return true
		}); err != nil {
			return nil, err
		}
	} else if err := c.forEachNodeHistoryIDByDepth(opts.Depth, func(id types.NodeID) bool {
		add(id)
		return true
	}); err != nil {
		return nil, err
	}
	return sortedCandidates(currentIDs, extra), nil
}

// nodesByLabelLocked is the lock-free body of NodeOps.ByLabel. Callers must
// hold c.mu (R or W). Used by the public method (under RLock) and by
// (*GraphTx).NodesByLabel (under tx-inherited Lock).
func (c *Core) nodesByLabelLocked(label string, opts storepkg.QueryOpts) ([]*types.Node, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		nodes, err := c.store.NodesByLabel(tok, opts)
		if err == nil && !c.storeRowsTrust {
			if err = c.validateNodesByLabelPage(tok, opts, nodes); err == nil {
				nodes = copyNodeRows(nodes)
			}
		}
		return nodes, err
	}
	return c.nodesByLabelTemporalLocked(tok, opts)
}

// ByType returns relationships with the type (resolved from string), with
// optional pagination; nil if the type is not registered. With a temporal
// filter every relationship (current + history) is scanned; the type token
// is immutable, so this only adds deleted/closed-out relationships the type
// index no longer references. Otherwise it uses the store type index.
//
// DECLARED view under a temporal filter: each relationship's own asserted
// validity, NOT masked by endpoint validity. Temporal().Snapshot /
// NeighborsAt / OutgoingRelsAt give the EFFECTIVE view.
func (r *RelOps) ByType(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.relsByTypeLocked(typeName, opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// relTypeCandidatesAt is nodeLabelCandidatesAt for a relationship type
// (relsByTypeLocked, countRelsByTypeLocked).
func (c *Core) relTypeCandidatesAt(tok uint16, opts storepkg.QueryOpts) ([]*types.Relationship, []types.RelID, error) {
	current, err := c.store.RelationshipsByType(tok, storepkg.QueryOpts{Depth: opts.Depth})
	if err != nil {
		return nil, nil, err
	}
	currentIDs, err := c.relIDsFromTypeRows(tok, current)
	if err != nil {
		return nil, nil, err
	}
	candIDs, err := c.relTypeCandidatesFrom(tok, currentIDs, opts)
	if err != nil {
		return nil, nil, err
	}
	return current, candIDs, nil
}

// relTypeCandidateIDsAt is nodeLabelCandidateIDsAt for a relationship type.
func (c *Core) relTypeCandidateIDsAt(tok uint16, opts storepkg.QueryOpts) ([]types.RelID, error) {
	if c.memberIDs == nil {
		_, ids, err := c.relTypeCandidatesAt(tok, opts)
		return ids, err
	}
	currentIDs, err := c.memberIDs.RelIDsByType(tok)
	if err != nil {
		return nil, err
	}
	return c.relTypeCandidatesFrom(tok, currentIDs, opts)
}

// relTypeCandidatesFrom is nodeLabelCandidatesFrom for a relationship type.
func (c *Core) relTypeCandidatesFrom(tok uint16, currentIDs []types.RelID, opts storepkg.QueryOpts) ([]types.RelID, error) {

	// scope the candidate set to the type's ever-members when the store owns
	// the transaction-time rel-type-membership sidecar (see nodesByLabelLocked).
	// Ascending ID order, as nodeLabelCandidatesFrom.
	candIDs, err := c.relTypeCandidatesSorted(tok, currentIDs, opts)
	if err != nil {
		return nil, err
	}

	return c.pruneRelTypeCandidates(tok, candIDs, opts), nil
}

// pruneRelTypeCandidates is pruneNodeLabelCandidates for relationship type tok
// (backlog 21c, CreateRelTemporal).
func (c *Core) pruneRelTypeCandidates(tok uint16, ids []types.RelID, opts storepkg.QueryOpts) []types.RelID {
	if c.relTypeTemporalCandidates == nil {
		return ids
	}
	sorted := slices.IsSorted(ids)
	kept, ok := c.relTypeTemporalCandidates.PruneRelTypeTemporalCandidates(tok, ids, opts)
	if !ok {
		return ids
	}
	if sorted && !slices.IsSorted(kept) {
		slices.Sort(kept)
	}
	return kept
}

// relTypeCandidatesSorted is nodeLabelCandidatesSorted for a relationship type.
func (c *Core) relTypeCandidatesSorted(tok uint16, currentIDs []types.RelID, opts storepkg.QueryOpts) ([]types.RelID, error) {
	if !slices.IsSorted(currentIDs) {
		currentIDs = slices.Sorted(slices.Values(currentIDs))
	}
	var extra []types.RelID
	add := func(id types.RelID) {
		if !inSorted(currentIDs, id) {
			extra = append(extra, id)
		}
	}
	if c.relTypeTxMembers != nil && c.membershipSidecarUsable(opts.Depth) {
		pinTx := effectiveTxPin(opts)
		if err := c.relTypeTxMembers.ForEachRelTypeTxMember(tok, func(id types.RelID, firstTx types.Instant) bool {
			if pinTx == 0 || firstTx == 0 || firstTx <= pinTx {
				add(id)
			}
			return true
		}); err != nil {
			return nil, err
		}
	} else if err := c.forEachRelHistoryIDByDepth(opts.Depth, func(id types.RelID) bool {
		add(id)
		return true
	}); err != nil {
		return nil, err
	}
	return sortedCandidates(currentIDs, extra), nil
}

// relsByTypeLocked is the lock-free body of RelOps.ByType.
func (c *Core) relsByTypeLocked(typeName string, opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return nil, nil
	}
	if !hasTemporalFilter(opts) {
		rels, err := c.store.RelationshipsByType(tok, opts)
		if err == nil && !c.storeRowsTrust {
			if err = c.validateRelationshipsByTypePage(tok, opts, rels); err == nil {
				rels = copyRelationshipRows(rels)
			}
		}
		return rels, err
	}
	return c.relsByTypeTemporalLocked(tok, opts)
}

// relTypeScanner is the OPTIONAL store capability behind RelOps.ForEachByType:
// type scans with O(1) peak memory. Stores without it use the materializing
// path.
type relTypeScanner interface {
	ForEachRelByType(token uint16, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
}

// ForEachByType streams relationships carrying the type to fn in
// snowflake-ID order; fn returning false stops early. The streaming sibling
// of ByType.
//
// Isolation is RELAXED: the IDs are snapshotted, then rows are fetched and fn
// called without graph locks. fn may call back into the graph; concurrent
// writers are neither blocked nor seen atomically. Rows are shared frozen
// pointers; fn must not mutate them. Deleted rows are skipped, new rows are not seen.
//
// A temporal-filter query streams too: ByType's answers in ascending ID
// order, resolved labelScanAtChunk at a time under the read lock and handed
// to fn with no lock held (label_scan_at.go). A non-native store routes a
// current-state scan through ByType.
//
// DECLARED view under a temporal filter, as ByType (not masked by endpoint validity).
func (r *RelOps) ForEachByType(typeName string, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
		return err
	}

	if hasTemporalFilter(opts) {
		return c.forEachRelByTypeAt(typeName, opts, fn)
	}
	scanner, native := c.store.(relTypeScanner)
	if !native || !c.storeRowsTrust {
		rels, err := r.ByType(typeName, opts)
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see the isolation note above.
	return scanner.ForEachRelByType(tok, opts, fn)
}

// relAdjacencyScanner is the OPTIONAL store capability behind
// RelOps.ForEachOutgoing / ForEachIncoming: streaming adjacency scans for
// hub-degree consumers that must not materialize a hub's adjacency.
type relAdjacencyScanner interface {
	ForEachOutgoingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error
	ForEachIncomingRel(nid types.NodeID, typeToken uint16, fn func(*types.Relationship) bool) error
}

// ForEachOutgoing streams the node's outgoing relationships (optionally
// type-filtered; empty typeName means all types) to fn in snowflake-ID order;
// fn returning false stops early. Same isolation and frozen-row contract as
// ForEachByType. An unregistered typeName streams nothing (after verifying
// the node exists, as Outgoing does).
func (r *RelOps) ForEachOutgoing(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	return r.forEachAdjacent(nodeID, typeName, false, fn)
}

// ForEachIncoming is ForEachOutgoing for the incoming direction.
func (r *RelOps) ForEachIncoming(nodeID types.NodeID, typeName string, fn func(*types.Relationship) bool) error {
	return r.forEachAdjacent(nodeID, typeName, true, fn)
}

func (r *RelOps) forEachAdjacent(nodeID types.NodeID, typeName string, incoming bool, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}

	scanner, native := c.store.(relAdjacencyScanner)
	if !native || !c.storeRowsTrust {
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			// Unregistered type: mirror Outgoing/Incoming — the node-exists
			// check still applies, then nothing matches.
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	// Deliberately outside c.mu — see ForEachByType's isolation note.
	if incoming {
		return scanner.ForEachIncomingRel(nodeID, tok, fn)
	}
	return scanner.ForEachOutgoingRel(nodeID, tok, fn)
}

// relEndpointScanner is the OPTIONAL store capability behind
// RelOps.ForEachAdjacentEndpoint — streaming adjacency that yields the OTHER
// endpoint (and the relationship's id) WITHOUT decoding the relationship row.
type relEndpointScanner interface {
	ForEachAdjacentEndpoint(nid types.NodeID, typeToken uint16, incoming bool, fn func(rel types.RelID, other types.NodeID) bool) error
}

// ForEachAdjacentEndpoint streams (relID, otherEndpoint) for the node's
// adjacency in the given direction WITHOUT decoding relationship rows, for
// traversals that need only the neighbour. Stores without the native
// capability decode via Outgoing/Incoming and project the endpoint (same
// results). Same relaxed isolation as ForEachOutgoing; fn returning false
// stops the scan.
func (r *RelOps) ForEachAdjacentEndpoint(nodeID types.NodeID, typeName string, incoming bool, fn func(rel types.RelID, other types.NodeID) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}

	scanner, native := c.store.(relEndpointScanner)
	if !native {
		// Fallback: decode the rels and project the OTHER endpoint.
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			other := rel.EndNodeID()
			if incoming {
				other = rel.StartNodeID()
			}
			if !fn(rel.InternalID(), other) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	return scanner.ForEachAdjacentEndpoint(nodeID, tok, incoming, fn)
}

// relEndpointScannerAt is the OPTIONAL store capability behind
// RelOps.ForEachAdjacentEndpointAt: yields the OTHER endpoint of edges
// passing the opts temporal filter, rejecting expired edges from inline
// valid-time stamps without decoding the row.
type relEndpointScannerAt interface {
	ForEachAdjacentEndpointAt(nid types.NodeID, typeToken uint16, incoming bool, opts storepkg.QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error
}

// ForEachAdjacentEndpointAt streams (relID, otherEndpoint) for the node's
// adjacency in the given direction under the opts temporal filter (ValidAt /
// ValidStart+ValidEnd / TxAt / TxPin). With a filter the door is
// VERSION-AWARE: each adjacent relationship resolves to the version valid
// under opts, as the node doors and Temporal().OutgoingRelsAt do, and a
// since-deleted edge stays visible inside its window
// (forEachAdjacentRelVersionLocked). Under a pure valid-time filter a live
// relationship without history rows (store.HistoryPresenceCapability) is
// answered from the live row (singleVersionAdjacency). Without a temporal
// filter this is ForEachAdjacentEndpoint. fn returning false stops the scan.
//
// The stores' inline-stamp scanners (relEndpointScannerAt) are not consulted
// under a temporal filter: a stamp only proves the LIVE row out-of-window.
//
// DECLARED view under a temporal filter, as ByType (not masked by endpoint validity).
func (r *RelOps) ForEachAdjacentEndpointAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(rel types.RelID, other types.NodeID) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
		return err
	}
	if hasTemporalFilter(opts) {
		return c.readUnderRLock(func() error {
			return c.forEachAdjacentRelVersionLocked(nodeID, typeName, incoming, opts, func(rel *types.Relationship) bool {
				other := rel.EndNodeID()
				if incoming {
					other = rel.StartNodeID()
				}
				return fn(rel.InternalID(), other)
			})
		})
	}

	scanner, native := c.store.(relEndpointScannerAt)
	if !native {
		// Fallback: decode the rels, apply the canonical temporal predicate, and
		// project the OTHER endpoint.
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !storeutil.MatchesTemporalFilter(rel.InternalID().SnowflakeID(), rel.Temporal(), opts) {
				continue
			}
			other := rel.EndNodeID()
			if incoming {
				other = rel.StartNodeID()
			}
			if !fn(rel.InternalID(), other) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	return scanner.ForEachAdjacentEndpointAt(nodeID, tok, incoming, opts, fn)
}

// relRelScannerAt is the OPTIONAL store capability behind
// RelOps.ForEachAdjacentRelAt: the decode-arm sibling of relEndpointScannerAt,
// skipping the decode of inline-stamp-rejected edges.
type relRelScannerAt interface {
	ForEachAdjacentRelAt(nid types.NodeID, typeToken uint16, incoming bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
}

// ForEachAdjacentRelAt streams the DECODED relationships for the node's
// adjacency under the opts temporal filter. With a filter it is
// VERSION-AWARE: per adjacent relationship, the version valid under opts (see
// ForEachAdjacentEndpointAt). Without one it is ForEachOutgoing/Incoming. fn
// returning false stops the scan.
//
// DECLARED view under a temporal filter, as ByType (not masked by endpoint validity).
func (r *RelOps) ForEachAdjacentRelAt(nodeID types.NodeID, typeName string, incoming bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
		return err
	}
	if hasTemporalFilter(opts) {
		return c.readUnderRLock(func() error {
			return c.forEachAdjacentRelVersionLocked(nodeID, typeName, incoming, opts, fn)
		})
	}

	scanner, native := c.store.(relRelScannerAt)
	if !native {
		// Fallback: decode via Outgoing/Incoming, apply the canonical filter.
		var rels []*types.Relationship
		var err error
		if incoming {
			rels, err = r.Incoming(nodeID, typeName)
		} else {
			rels, err = r.Outgoing(nodeID, typeName)
		}
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !storeutil.MatchesTemporalFilter(rel.InternalID().SnowflakeID(), rel.Temporal(), opts) {
				continue
			}
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var tok uint16
	if typeName != "" {
		var ok bool
		if err := c.readUnderRLock(func() error {
			tok, ok = c.lookupRelTypeQueryToken(typeName)
			return nil
		}); err != nil {
			return err
		}
		if !ok {
			return c.readUnderRLock(func() error {
				return c.validateRequestedNodesExist([]types.NodeID{nodeID})
			})
		}
	}
	return scanner.ForEachAdjacentRelAt(nodeID, tok, incoming, opts, fn)
}

// Outgoing returns all outgoing relationships from the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (r *RelOps) Outgoing(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.outgoingRelsLocked(nodeID, typeName)
		return err
	})
	return result, err
}

// outgoingRelsLocked is the lock-free body of RelOps.Outgoing.
func (c *Core) outgoingRelsLocked(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist([]types.NodeID{nodeID})
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.OutgoingRelationships(nodeID, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateOutgoingRelationshipRows(nodeID, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRows(rels)
	}
	return rels, nil
}

// OutgoingForNodes returns outgoing relationships for many nodes in one
// batch: a map nodeID -> rels sorted by ID; nodes without any are absent. A
// non-empty typeName filters by type; an unregistered one, or nil/empty
// nodeIDs, returns nil, nil.
func (r *RelOps) OutgoingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.outgoingRelsForNodesLocked(nodeIDs, typeName)
		return err
	})
	return result, err
}

// outgoingRelsForNodesLocked is the lock-free body of RelOps.OutgoingForNodes.
func (c *Core) outgoingRelsForNodesLocked(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist(nodeIDs)
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateOutgoingRelationshipMap(nodeIDs, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRowMap(rels)
	}
	return rels, nil
}

// IncomingForNodes returns incoming relationships for many nodes in one
// batch: a map nodeID -> rels sorted by ID; nodes without any are absent. A
// non-empty typeName filters by type; an unregistered one, or nil/empty
// nodeIDs, returns nil, nil.
func (r *RelOps) IncomingForNodes(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.incomingRelsForNodesLocked(nodeIDs, typeName)
		return err
	})
	return result, err
}

// incomingRelsForNodesLocked is the lock-free body of RelOps.IncomingForNodes.
func (c *Core) incomingRelsForNodesLocked(nodeIDs []types.NodeID, typeName string) (map[types.NodeID][]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist(nodeIDs)
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.IncomingRelationshipsForNodes(nodeIDs, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateIncomingRelationshipMap(nodeIDs, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRowMap(rels)
	}
	return rels, nil
}

// OutgoingForNodesAtTx is the bitemporal (tx-time-pinned) counterpart of
// OutgoingForNodes: each candidate relationship's version is resolved through
// the chain seam the TxAt door uses (findRelVersionForOpts ->
// filterRelChainByTxAt).
//
// SEMANTICS: agrees with the TxAt-pinned BITEMPORAL door (QueryOpts{TxAt})
// filtered by endpoint, NOT with a belief-state pin. The TxAt arm applies a
// POINT valid-time probe at c.readNow() when no valid-time opts are set (see
// the QueryOpts.TxAt warning), so an edge whose valid interval lies wholly in
// the past (CloseVersion-ed, or width-1 point-event) is silently dropped even
// though it was believed at txAt. For pure belief-state semantics use
// OutgoingForNodesAtPin / IncomingForNodesAtPin.
//
// Candidates: the live adjacency UNIONED with each seed's ex-adjacency
// (forEachRelAdjacencyCandidateID; AGENTS.md "Adjacency-at-t fold uses the
// seed's ex-adjacency"). A relationship deleted after txAt is still visible
// (delete is a tx-time tombstone, lesson 60); one created after, or deleted
// before, txAt is not; a backfilled one (AddWithTx) is visible from its
// backfilled TxFrom.
//
// txAt == 0 delegates to OutgoingForNodes. An unregistered typeName still
// validates that every node exists and returns (nil, nil). Results are
// sorted by ID within each node's slice.
//
// DECLARED view under a temporal filter, as ByType (not masked by endpoint validity).
func (r *RelOps) OutgoingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtTxLocked(nodeIDs, typeName, txAt, true)
		return err
	})
	return result, err
}

// IncomingForNodesAtTx is the bitemporal counterpart of IncomingForNodes; see
// OutgoingForNodesAtTx (mirrored). DECLARED view, not masked by endpoint
// validity.
func (r *RelOps) IncomingForNodesAtTx(nodeIDs []types.NodeID, typeName string, txAt types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtTxLocked(nodeIDs, typeName, txAt, false)
		return err
	})
	return result, err
}

// directionalRelsForNodesAtTxLocked is the lock-free shared body of
// Outgoing/IncomingForNodesAtTx; outgoing=true selects the start-endpoint
// direction.
func (c *Core) directionalRelsForNodesAtTxLocked(nodeIDs []types.NodeID, typeName string, txAt types.Instant, outgoing bool) (map[types.NodeID][]*types.Relationship, error) {
	// Relationship-purge watermarks apply (backlog 21).
	if err := c.checkRelsOnlyRetention(typeName, txAt); err != nil {
		return nil, err
	}
	if txAt == 0 {
		if outgoing {
			return c.outgoingRelsForNodesLocked(nodeIDs, typeName)
		}
		return c.incomingRelsForNodesLocked(nodeIDs, typeName)
	}

	var tok uint16
	hasType := typeName != ""
	if hasType {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist(nodeIDs)
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist(nodeIDs); err != nil {
			return nil, err
		}
	}

	var (
		liveRows map[types.NodeID][]*types.Relationship
		err      error
	)
	if outgoing {
		liveRows, err = c.store.OutgoingRelationshipsForNodes(nodeIDs, tok)
	} else {
		liveRows, err = c.store.IncomingRelationshipsForNodes(nodeIDs, tok)
	}
	if err != nil {
		return nil, err
	}

	seedIDs, err := c.relIDsFromNodeMapRows(nodeIDs, tok, liveRows, outgoing)
	if err != nil {
		return nil, err
	}

	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	for _, id := range nodeIDs {
		requested[id] = struct{}{}
	}

	opts := c.normalizeTxAtOnlyOpts(storepkg.QueryOpts{TxAt: txAt})
	var pred func(*types.Relationship) bool
	if hasType {
		pred = func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	}

	result := make(map[types.NodeID][]*types.Relationship)
	if err := c.forEachRelAdjacencyCandidateID(nodeIDs, adjDirsOf(outgoing), seedIDs, func(id types.RelID) error {
		r, err := c.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		var endpoint types.NodeID
		if outgoing {
			endpoint = r.StartNodeID()
		} else {
			endpoint = r.EndNodeID()
		}
		if _, ok := requested[endpoint]; !ok {
			return nil
		}
		result[endpoint] = append(result[endpoint], r)
		return nil
	}); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	for id := range result {
		storeutil.SortRelsByID(result[id])
	}
	return result, nil
}

// OutgoingForNodesAtPin is the pure belief-state counterpart of
// OutgoingForNodes / OutgoingForNodesAtTx: every outgoing relationship
// BELIEVED at the transaction-time pin, with NO valid-time filtering. It
// agrees with ByType(QueryOpts{TxPin: pin}) filtered by endpoint BY
// CONSTRUCTION: candidates go through the same as-of resolution as the TxPin
// door (findRelVersionForOpts -> relAsOfLocked -> storeutil.SelectAsOf), so
// the doors cannot drift (rule 17).
//
// Use this for AS-OF-SYSTEM-TIME semantics. Unlike OutgoingForNodesAtTx it
// returns EVERY edge believed at the pin: past-valid facts, point events and
// unset-valid_from edges alike. An edge hard-deleted after the pin is still
// visible; one created after, or deleted before, the pin is not; a
// backfilled edge (AddWithTx) is visible from its backfilled TxFrom.
//
// Candidates: the live adjacency of the CURRENTLY-EXISTING seeds UNIONED with
// the ex-adjacency of EVERY seed, deleted ones included
// (forEachRelAdjacencyCandidateID; AGENTS.md "Adjacency-at-t fold uses the
// seed's ex-adjacency").
//
// SEED TOLERANCE: unlike OutgoingForNodes/AtTx, a seed absent from CURRENT
// state does NOT error with ErrNodeNotFound. A seed hard-deleted after the
// pin is accepted: its cascade-deleted pre-delete edges still name it as
// endpoint and are recovered. A seed created after the pin, or that never
// existed, contributes nothing and is skipped silently (as ByType{TxPin}
// filtered by endpoint). Seed IDs are still format-validated
// (ValidateNodeID).
//
// pin == 0 delegates to OutgoingForNodes (including its ErrNodeNotFound seed
// validation). Results are sorted by ID within each node's slice.
//
// DECLARED view under a temporal filter, as ByType (not masked by endpoint validity).
func (r *RelOps) OutgoingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtPinLocked(nodeIDs, typeName, pin, true)
		return err
	})
	return result, err
}

// IncomingForNodesAtPin is the belief-state counterpart of IncomingForNodes;
// see OutgoingForNodesAtPin (mirrored). DECLARED view, not masked by
// endpoint validity.
func (r *RelOps) IncomingForNodesAtPin(nodeIDs []types.NodeID, typeName string, pin types.Instant) (map[types.NodeID][]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if len(nodeIDs) == 0 {
		return nil, nil
	}
	for _, id := range nodeIDs {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result map[types.NodeID][]*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.directionalRelsForNodesAtPinLocked(nodeIDs, typeName, pin, false)
		return err
	})
	return result, err
}

// directionalRelsForNodesAtPinLocked is the lock-free shared body of
// Outgoing/IncomingForNodesAtPin; outgoing=true selects the start-endpoint
// direction. See OutgoingForNodesAtPin for the seed-tolerance contract.
func (c *Core) directionalRelsForNodesAtPinLocked(nodeIDs []types.NodeID, typeName string, pin types.Instant, outgoing bool) (map[types.NodeID][]*types.Relationship, error) {
	// Relationship-purge watermarks apply (backlog 21).
	if err := c.checkRelsOnlyRetention(typeName, pin); err != nil {
		return nil, err
	}
	if pin == 0 {
		if outgoing {
			return c.outgoingRelsForNodesLocked(nodeIDs, typeName)
		}
		return c.incomingRelsForNodesLocked(nodeIDs, typeName)
	}

	var tok uint16
	hasType := typeName != ""
	if hasType {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			// Unregistered type: no edges at any pin; tolerant of seeds, unlike AtTx.
			return nil, nil
		}
		tok = t
	}

	// Partition seeds by CURRENT existence for the live-adjacency probe. A seed
	// hard-deleted after the pin is not current (the cascade purged its live
	// adjacency), and OutgoingRelationshipsForNodes would ErrNodeNotFound on it;
	// its edges are recovered via the ex-adjacency below. `requested` keeps ALL
	// deduped seeds so the endpoint filter still admits them.
	requested := make(map[types.NodeID]struct{}, len(nodeIDs))
	liveSeedIDs := make([]types.NodeID, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		if _, dup := requested[id]; dup {
			continue
		}
		requested[id] = struct{}{}
		if _, err := c.getCurrentNode(id); err != nil {
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				continue
			}
			return nil, err
		}
		liveSeedIDs = append(liveSeedIDs, id)
	}

	var (
		liveRows map[types.NodeID][]*types.Relationship
		err      error
	)
	if len(liveSeedIDs) > 0 {
		if outgoing {
			liveRows, err = c.store.OutgoingRelationshipsForNodes(liveSeedIDs, tok)
		} else {
			liveRows, err = c.store.IncomingRelationshipsForNodes(liveSeedIDs, tok)
		}
		if err != nil {
			return nil, err
		}
	}

	seedIDs, err := c.relIDsFromNodeMapRows(liveSeedIDs, tok, liveRows, outgoing)
	if err != nil {
		return nil, err
	}

	opts := storepkg.QueryOpts{TxPin: pin}
	var pred func(*types.Relationship) bool
	if hasType {
		pred = func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
	}

	result := make(map[types.NodeID][]*types.Relationship)
	if err := c.forEachRelAdjacencyCandidateID(nodeIDs, adjDirsOf(outgoing), seedIDs, func(id types.RelID) error {
		r, err := c.findRelVersionForOpts(id, opts, pred)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		var endpoint types.NodeID
		if outgoing {
			endpoint = r.StartNodeID()
		} else {
			endpoint = r.EndNodeID()
		}
		if _, ok := requested[endpoint]; !ok {
			return nil
		}
		result[endpoint] = append(result[endpoint], r)
		return nil
	}); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return nil, nil
	}
	for id := range result {
		storeutil.SortRelsByID(result[id])
	}
	return result, nil
}

// OutgoingDegree returns the number of outgoing relationships from nodeID,
// optionally filtered by type, using the store's DegreeCapability (adjacency
// count, no materialization) or else len(Outgoing(...)).
func (r *RelOps) OutgoingDegree(nodeID types.NodeID, typeName string) (int, error) {
	return r.degree(nodeID, typeName, true)
}

// IncomingDegree returns the number of incoming relationships to nodeID,
// optionally filtered by type. See OutgoingDegree for semantics.
func (r *RelOps) IncomingDegree(nodeID types.NodeID, typeName string) (int, error) {
	return r.degree(nodeID, typeName, false)
}

func (r *RelOps) degree(nodeID types.NodeID, typeName string, outgoing bool) (int, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return 0, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return 0, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return 0, err
	}
	var result int
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.degreeLocked(nodeID, typeName, outgoing)
		return err
	})
	return result, err
}

// degreeLocked is the lock-free body of OutgoingDegree/IncomingDegree.
func (c *Core) degreeLocked(nodeID types.NodeID, typeName string, outgoing bool) (int, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			// Unknown type → zero, but still surface a missing-node error to
			// match the adjacency methods' contract.
			return 0, c.validateRequestedNodesExist([]types.NodeID{nodeID})
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
			return 0, err
		}
	}
	if dc, ok := c.store.(storepkg.DegreeCapability); ok {
		if outgoing {
			return dc.OutgoingDegree(nodeID, tok)
		}
		return dc.IncomingDegree(nodeID, tok)
	}
	// Fallback: materialize and count (validated rows).
	var rels []*types.Relationship
	var err error
	if outgoing {
		rels, err = c.store.OutgoingRelationships(nodeID, tok)
	} else {
		rels, err = c.store.IncomingRelationships(nodeID, tok)
	}
	if err != nil {
		return 0, err
	}
	return len(rels), nil
}

// Incoming returns all incoming relationships to the given node.
// If typeName is empty, all types are returned. If typeName is non-empty, only
// relationships of that type are returned (nil if the type is not registered).
func (r *RelOps) Incoming(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if typeName != "" {
		if err := c.validateRelTypeQueryName(typeName); err != nil {
			return nil, err
		}
	}
	if err := storepkg.ValidateNodeID(nodeID); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.incomingRelsLocked(nodeID, typeName)
		return err
	})
	return result, err
}

// incomingRelsLocked is the lock-free body of RelOps.Incoming.
func (c *Core) incomingRelsLocked(nodeID types.NodeID, typeName string) ([]*types.Relationship, error) {
	var tok uint16
	if typeName != "" {
		t, ok := c.lookupRelTypeQueryToken(typeName)
		if !ok {
			return nil, c.validateRequestedNodesExist([]types.NodeID{nodeID})
		}
		tok = t
	}
	if !c.storeRowsTrust {
		if err := c.validateRequestedNodesExist([]types.NodeID{nodeID}); err != nil {
			return nil, err
		}
	}
	rels, err := c.store.IncomingRelationships(nodeID, tok)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := c.validateIncomingRelationshipRows(nodeID, tok, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRows(rels)
	}
	return rels, nil
}

// Count returns the number of nodes in the store.
func (n *NodeOps) Count() (int, error) {
	c := n.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, ErrGraphClosed
	}
	return c.nodeCount()
}

// Count returns the number of relationships in the store.
func (r *RelOps) Count() (int, error) {
	c := r.c
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.closed.Load() {
		return 0, ErrGraphClosed
	}
	return c.relCount()
}

// All returns all nodes, with optional pagination. With a temporal filter
// (ValidAt or ValidStart/ValidEnd) it is history-aware: current and history
// IDs are merged and resolved via the version chain, surfacing deleted
// entities valid at that time. Otherwise the store pushdown path is used.
func (n *NodeOps) All(opts storepkg.QueryOpts) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return nil, err
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.allNodesLocked(opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// allNodesLocked is the lock-free body of NodeOps.All.
func (c *Core) allNodesLocked(opts storepkg.QueryOpts) ([]*types.Node, error) {
	if !hasTemporalFilter(opts) {
		nodes, err := c.store.AllNodes(opts)
		if err != nil {
			return nil, err
		}
		if !c.storeRowsTrust {
			if err := validateAllNodePage(opts, nodes); err != nil {
				return nil, err
			}
			nodes = copyNodeRows(nodes)
		}
		return nodes, nil
	}
	var result []*types.Node
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	err := c.forEachKnownNodeIDByDepth(opts.Depth, func(id types.NodeID) error {
		n, err := c.findNodeVersionForOpts(id, resolveOpts, nil)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				return nil
			}
			return err
		}
		result = append(result, n)
		return nil
	})
	if err != nil {
		return nil, err
	}
	storeutil.SortNodesByID(result)
	result = storeutil.PaginateNodes(result, opts.After, opts.Limit)
	return result, nil
}

// ForEach streams all nodes matching opts to fn. Current-state unpaginated
// scans walk the store's node-ID iterator, one row at a time (O(1) memory);
// temporal and paginated scans fall back to All. Isolation is relaxed as in
// ForEachByLabel: IDs are snapshotted, rows fetched and fn called without
// graph locks; concurrently deleted rows may be skipped, created ones may be
// missed.
func (n *NodeOps) ForEach(opts storepkg.QueryOpts, fn func(*types.Node) bool) error {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeNodes); err != nil {
		return err
	}

	if !c.storeRowsTrust || hasTemporalFilter(opts) || opts.After != 0 || opts.Limit != 0 {
		nodes, err := n.All(opts)
		if err != nil {
			return err
		}
		for _, nd := range nodes {
			if !fn(nd) {
				return nil
			}
		}
		return nil
	}

	var iterErr error
	err := c.forEachNodeID(func(id types.NodeID) bool {
		nd, getErr := c.store.GetNode(id)
		if getErr != nil {
			if errors.Is(getErr, storepkg.ErrNodeNotFound) {
				return true
			}
			iterErr = getErr
			return false
		}
		return fn(nd)
	})
	if err != nil {
		return err
	}
	return iterErr
}

// All returns all relationships, with optional pagination. With a temporal
// filter (ValidAt or ValidStart/ValidEnd) it is history-aware: current and
// history IDs are merged and resolved via the version chain, surfacing
// deleted relationships valid at that time. Otherwise the store pushdown
// path is used.
//
// DECLARED view under a temporal filter, as ByType (not masked by endpoint validity).
func (r *RelOps) All(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeAllRels); err != nil {
		return nil, err
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.allRelsLocked(opts)
		return err
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// allRelsLocked is the lock-free body of RelOps.All.
func (c *Core) allRelsLocked(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	if !hasTemporalFilter(opts) {
		rels, err := c.store.AllRelationships(opts)
		if err != nil {
			return nil, err
		}
		if !c.storeRowsTrust {
			if err := validateAllRelationshipPage(opts, rels); err != nil {
				return nil, err
			}
			rels = copyRelationshipRows(rels)
		}
		return rels, nil
	}
	var result []*types.Relationship
	resolveOpts := c.normalizeTxAtOnlyOpts(opts)
	err := c.forEachKnownRelIDByDepth(opts.Depth, func(id types.RelID) error {
		r, err := c.findRelVersionForOpts(id, resolveOpts, nil)
		if err != nil {
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
				return nil
			}
			return err
		}
		result = append(result, r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	storeutil.SortRelsByID(result)
	result = storeutil.PaginateRels(result, opts.After, opts.Limit)
	return result, nil
}

// ForEach streams all relationships matching opts to fn; mirrors
// NodeOps.ForEach (Testing Rule 2). Current-state unpaginated scans walk the
// rel-ID iterator one row at a time (O(1) memory); temporal and paginated
// scans fall back to All. Isolation is relaxed as in ForEachByType: IDs are
// snapshotted, rows fetched and fn called without graph locks.
//
// DECLARED view under a temporal filter, as ByType (not masked by endpoint validity).
func (r *RelOps) ForEach(opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateTemporalQueryOptsScan(opts, scopeAllRels); err != nil {
		return err
	}

	if !c.storeRowsTrust || hasTemporalFilter(opts) || opts.After != 0 || opts.Limit != 0 {
		rels, err := r.All(opts)
		if err != nil {
			return err
		}
		for _, rel := range rels {
			if !fn(rel) {
				return nil
			}
		}
		return nil
	}

	var iterErr error
	err := c.forEachRelID(func(id types.RelID) bool {
		rel, getErr := c.store.GetRelationship(id)
		if getErr != nil {
			if errors.Is(getErr, storepkg.ErrRelNotFound) {
				return true
			}
			iterErr = getErr
			return false
		}
		return fn(rel)
	})
	if err != nil {
		return err
	}
	return iterErr
}

// GetByIDs returns nodes for every requested ID sorted by ascending ID.
// Nil or empty ids returns nil, nil after the graph lifecycle check.
// Missing IDs return store.ErrNodeNotFound.
func (n *NodeOps) GetByIDs(ids []types.NodeID) ([]*types.Node, error) {
	c := n.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Node
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.nodesByIDsLocked(ids)
		return err
	})
	return result, err
}

// nodesByIDsLocked is the lock-free body of NodeOps.GetByIDs.
func (c *Core) nodesByIDsLocked(ids []types.NodeID) ([]*types.Node, error) {
	nodes, err := c.store.GetNodesByIDs(ids)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateNodesByIDRows(ids, nodes); err != nil {
			return nil, err
		}
		nodes = copyNodeRows(nodes)
	}
	c.opNodeReads.Add(int64(len(nodes)))
	return nodes, nil
}

// GetByIDs returns relationships for every requested ID sorted by ascending ID.
// Nil or empty ids returns nil, nil after the graph lifecycle check.
// Missing IDs return store.ErrRelNotFound.
func (r *RelOps) GetByIDs(ids []types.RelID) ([]*types.Relationship, error) {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	if len(ids) == 0 {
		return nil, nil
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	var result []*types.Relationship
	err := c.readUnderRLock(func() error {
		var err error
		result, err = c.relsByIDsLocked(ids)
		return err
	})
	return result, err
}

// relsByIDsLocked is the lock-free body of RelOps.GetByIDs.
func (c *Core) relsByIDsLocked(ids []types.RelID) ([]*types.Relationship, error) {
	rels, err := c.store.GetRelationshipsByIDs(ids)
	if err != nil {
		return nil, err
	}
	if !c.storeRowsTrust {
		if err := validateRelationshipsByIDRows(ids, rels); err != nil {
			return nil, err
		}
		rels = copyRelationshipRows(rels)
	}
	c.opRelReads.Add(int64(len(rels)))
	return rels, nil
}

// --- Per-label / per-type statistics ---

// CountByLabel returns the number of nodes with the label, O(1); 0 if the
// label was never registered. Hot-path inlined (see NodeOps.Get, B4).
func (n *NodeOps) CountByLabel(label string) (int, error) {
	c := n.c
	if err := c.validateIndexLabel(label); err != nil {
		return 0, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return 0, ErrGraphClosed
	}
	tok, ok := c.labels.Lookup(label)
	if !ok {
		c.mu.RUnlock()
		return 0, nil
	}
	count, err := c.nodeCountByLabel(tok)
	c.mu.RUnlock()
	return count, err
}

// CountByType returns the number of relationships with the type, O(1); 0 if
// the type was never registered. Hot-path inlined (see NodeOps.Get, B4).
func (r *RelOps) CountByType(typeName string) (int, error) {
	c := r.c
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return 0, err
	}
	c.mu.RLock()
	if c.closed.Load() {
		c.mu.RUnlock()
		return 0, ErrGraphClosed
	}
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		c.mu.RUnlock()
		return 0, nil
	}
	count, err := c.relCountByType(tok)
	c.mu.RUnlock()
	return count, err
}

// countByLabelLocked is the lock-free body of NodeOps.CountByLabel.
func (c *Core) countByLabelLocked(label string) (int, error) {
	tok, ok := c.labels.Lookup(label)
	if !ok {
		return 0, nil
	}
	return c.nodeCountByLabel(tok)
}

// countByTypeLocked is the lock-free body of RelOps.CountByType.
func (c *Core) countByTypeLocked(typeName string) (int, error) {
	tok, ok := c.lookupRelTypeQueryToken(typeName)
	if !ok {
		return 0, nil
	}
	return c.relCountByType(tok)
}

// --- Relationship property-index scans ---

// relRangeScanner is the OPTIONAL store capability behind
// RelOps.ForEachByTypePropertyRange — the relationship mirror of
// nodeRangeScanner. Implemented by the memory and badger stores.
type relRangeScanner interface {
	ForEachRelByTypePropertyRange(typeToken uint16, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error
}

// ForEachByTypePropertyRange streams relationships carrying the type whose
// NUMERIC propKey value lies in [min, max], in snowflake-ID order; the
// relationship mirror of NodeOps.ForEachByLabelPropertyRange. Candidates
// OVER-SELECT (float64 sort keys, ulp-widened bounds): fn must re-check with
// exact comparison. Returns storepkg.ErrIndexNotFound when no usable rel
// index exists or the store lacks the capability. Same isolation and
// frozen-row contract as ForEachByType. A TEMPORAL QueryOpts is served by a
// full fold over ByType's resolved versions (forEachRelInRangeTemporal):
// value at the pin, ID order, no index; under ValidStart+ValidEnd the range
// tests the ONE resolved version, unlike ByTypeAndProperty (backlog 16).
func (r *RelOps) ForEachByTypePropertyRange(typeName, propKey string, min, max float64, inclMin, inclMax bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
		return err
	}
	if hasTemporalFilter(opts) {
		return forEachRelInRangeTemporal(c, typeName, propKey, min, max, opts, fn)
	}
	scanner, native := c.store.(relRangeScanner)
	if !native || !c.storeRowsTrust {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	return scanner.ForEachRelByTypePropertyRange(tok, propKey, min, max, inclMin, inclMax, opts, fn)
}

// ForEachByTypePropertyRangeOrdered streams relationships carrying the type
// whose NUMERIC propKey value lies in [min, max] to fn in CONTRACTUAL VALUE
// ORDER (ascending, or descending when desc), ties by rel ID ASCENDING in
// both directions. Mirror of NodeOps.ForEachByLabelPropertyRangeOrdered: a
// LIMIT k is pushed into the index by returning false from fn after k rows.
//
// Candidates OVER-SELECT: fn MUST re-check the exact bounds.
//
// Non-temporal opts use the index (ErrIndexNotFound when no usable ordered
// view exists or the store lacks the capability). A TEMPORAL QueryOpts is
// served by a sound full fold (Stage B): every rel resolved to its version at
// the pin, filtered and sorted on the value AT t, O(N log N), no index. Same
// isolation and frozen-row contract as ForEachByType.
func (r *RelOps) ForEachByTypePropertyRangeOrdered(typeName, propKey string, min, max float64, inclMin, inclMax, desc bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	// Temporal ordered scan (Stage B): value-at-t is not indexed, so fold every
	// rel's version at the pin, keep in-range values, sort by value.
	if opts.HasTimeCoordinate() {
		if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
			return err
		}
		return forEachRelValueOrderedTemporal(c, typeName, opts, desc,
			func(rl *types.Relationship) (float64, bool) {
				v, ok := rl.GetProperty(propKey)
				if !ok {
					return 0, false
				}
				f, ok := types.NumericFloat64(v)
				if !ok || !numericInRange(f, min, max, true, true) { // over-select; fn re-checks inclusivity
					return 0, false
				}
				return f, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner := c.relOrderedRange
	if scanner == nil {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	visit, rowErr := c.trustedRelVisit(fn)
	if err := scanner.ForEachRelByTypePropertyRangeOrdered(tok, propKey, min, max, inclMin, inclMax, desc, visit); err != nil {
		return err
	}
	return *rowErr
}

// ForEachByTypePropertyPrefix streams relationships carrying the type whose
// STRING propKey value begins with prefix to fn in CONTRACTUAL VALUE ORDER
// (lexicographic ascending, or descending when desc), ties by rel ID
// ASCENDING in both directions. Mirror of
// NodeOps.ForEachByLabelPropertyPrefix; fn returning false stops (LIMIT
// pushdown); an empty prefix matches every string value.
//
// Non-temporal opts use the index (ErrIndexNotFound when no usable rel index
// exists or the store lacks the capability). A TEMPORAL QueryOpts is served
// by a sound full fold (Stage B), O(N log N), no index. Same isolation and
// frozen-row contract as ForEachByType.
func (r *RelOps) ForEachByTypePropertyPrefix(typeName, propKey, prefix string, desc bool, opts storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	c := r.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if fn == nil {
		return grapherr.ErrNilCallback
	}
	if err := c.validateRelTypeQueryName(typeName); err != nil {
		return err
	}
	// Temporal prefix scan (Stage B): value-at-t is not indexed, so fold every
	// rel's version at the pin, keep matching strings, sort.
	if opts.HasTimeCoordinate() {
		if err := c.validateTemporalQueryOptsScan(opts, c.relTypeRetentionScope(typeName)); err != nil {
			return err
		}
		return forEachRelValueOrderedTemporal(c, typeName, opts, desc,
			func(rl *types.Relationship) (string, bool) {
				v, ok := rl.GetProperty(propKey)
				if !ok {
					return "", false
				}
				s, ok := v.(string)
				if !ok || !strings.HasPrefix(s, prefix) {
					return "", false
				}
				return s, true
			}, fn)
	}
	if err := storepkg.ValidateQueryOpts(opts); err != nil {
		return err
	}
	scanner := c.relPrefixScan
	if scanner == nil {
		return storepkg.ErrIndexNotFound
	}
	var tok uint16
	var ok bool
	if err := c.readUnderRLock(func() error {
		tok, ok = c.lookupRelTypeQueryToken(typeName)
		return nil
	}); err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Deliberately outside c.mu — see ForEachByLabel's isolation note.
	visit, rowErr := c.trustedRelVisit(fn)
	if err := scanner.ForEachRelByTypePropertyPrefix(tok, propKey, prefix, desc, visit); err != nil {
		return err
	}
	return *rowErr
}
