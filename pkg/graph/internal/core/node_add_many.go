package core

import (
	"context"
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// AddNodes creates one node per element of props, all carrying labels, inside
// the transaction, and returns them in props' order. It is AddNode called once
// per element, with the work that does not depend on the element done once: the
// labels are resolved once, the transaction's locks are taken once, and the
// nodes reach the store in one batch write (Store.PutNodesBatch) instead of one
// write each. Every node gets its own ID, hash, transaction time (strictly
// increasing in props' order) and create event; each is tracked for rollback, so
// Rollback removes all of them.
//
// All or nothing: a validation error in any element (labels, provenance or
// temporal shadow keys, property limits) returns it before an ID is minted, and
// a failed store write removes the nodes it may have written; the returned slice
// is then nil. Where a node needs a per-node check the batch cannot make (a
// unique-property constraint exists, or the transaction runs under a scoped
// write token), the call runs AddNode's path per element instead: same results,
// none of the saving, and an error stops at the failing element with the
// earlier nodes kept (the transaction's rollback removes them).
//
// An empty props creates nothing and returns (nil, nil).
func (tx *GraphTx) AddNodes(labels []string, props []map[string]any) ([]*types.Node, error) {
	if err := tx.lockActiveCoreWrite(); err != nil {
		return nil, err
	}
	defer tx.unlockActiveCoreWrite()

	nodes, err := tx.g.addNodesInternal(tx.doorCtx(), labels, props)
	for _, n := range nodes {
		tx.noteNodeCreateResultLocked(n)
	}
	return nodes, err
}

// addNodesInternal is the lock-free body of GraphTx.AddNodes. Callers hold the
// transaction's core write section.
func (c *Core) addNodesInternal(ctx context.Context, labels []string, props []map[string]any) ([]*types.Node, error) {
	if err := checkCtx(ctx); err != nil {
		return nil, err
	}
	if len(props) == 0 {
		return nil, nil
	}
	if !c.canBatchNodeCreates(ctx) {
		return c.addNodesOneByOne(ctx, labels, props)
	}
	if err := c.validateNodeCreateLabels(labels); err != nil {
		return nil, err
	}

	prepared := make([]preparedNodeCreate, len(props))
	for i, p := range props {
		if err := c.prepareNodeCreate(p, &prepared[i]); err != nil {
			return nil, fmt.Errorf("graph: node %d of %d: %w", i, len(props), err)
		}
	}

	primaryToken, extraTokens, labelSnapshot, allocatedLabels, labelsLocked, err := c.getOrCreateLabelsWithSnapshot(labels)
	if err != nil {
		return nil, err
	}
	nodes := make([]*types.Node, 0, len(props))
	finished := false
	finish := func(err error) error {
		finished = true
		if err != nil {
			err = c.removePartialNodes(nodes, err)
		}
		if !labelsLocked {
			return err
		}
		return c.restoreNewLabelsOnError(labelSnapshot, allocatedLabels, err)
	}
	defer func() {
		if !finished {
			_ = finish(fmt.Errorf("panic during node batch create"))
		}
	}()

	var canonicalLabels []string
	for i := range prepared {
		pn := &prepared[i]
		n := types.NewNode(c.nextNodeID(), primaryToken, extraTokens)
		if err := n.SetOwnedProperties(pn.props); err != nil {
			return nil, finish(fmt.Errorf("graph: node properties: %w", err))
		}
		if canonicalLabels == nil {
			canonicalLabels = labels
			if len(labels) != 1 {
				canonicalLabels = c.nodeLabelsUnlocked(n)
			}
		}
		hash, err := integrity.ComputeNodeHashChecked(n, canonicalLabels)
		if err != nil {
			return nil, finish(fmt.Errorf("graph: compute node hash: %w", err))
		}
		n.SetIntegrity(&types.NodeIntegrity{
			Hash:               hash,
			AuthorID:           pn.authorID,
			Signature:          pn.signature,
			AuthorizedBy:       pn.authorizedBy,
			AuthorizationLevel: pn.authLevel,
		})
		pn.stampTemporal(n, c.now())
		nodes = append(nodes, n)
	}
	if err := checkCtx(ctx); err != nil {
		nodes = nil
		return nil, finish(err)
	}
	if err := c.putGeneratedNodesBatch(nodes); err != nil {
		return nil, finish(err)
	}
	if err := finish(nil); err != nil {
		c.opNodeAdds.Add(int64(len(nodes)))
		return nodes, err
	}
	c.opNodeAdds.Add(int64(len(nodes)))
	return nodes, nil
}

// canBatchNodeCreates reports whether a node create may skip the per-node
// store write: no unique-property constraint needs its stripe held across the
// write, and no scoped write token routes the write.
func (c *Core) canBatchNodeCreates(ctx context.Context) bool {
	if c.hasUniqueConstraints.Load() {
		return false
	}
	if token, ok := scopeTokenFrom(ctx); ok && token != 0 {
		return false
	}
	return true
}

// addNodesOneByOne is AddNodes' per-node path (see AddNodes).
func (c *Core) addNodesOneByOne(ctx context.Context, labels []string, props []map[string]any) ([]*types.Node, error) {
	nodes := make([]*types.Node, 0, len(props))
	for _, p := range props {
		n, err := c.addNodeInternal(ctx, labels, p)
		if n != nil {
			nodes = append(nodes, n)
		}
		if err != nil {
			return nodes, err
		}
	}
	return nodes, nil
}

// removePartialNodes deletes the nodes a failed batch write may have stored
// and adds any cleanup failure to err.
func (c *Core) removePartialNodes(nodes []*types.Node, err error) error {
	for _, n := range nodes {
		if cleanupErr := runRollbackCleanup(func() error {
			return c.deletePartialNodeForRollback(n)
		}); cleanupErr != nil && !errors.Is(cleanupErr, storepkg.ErrNodeNotFound) {
			err = fmt.Errorf("%w; additionally failed to remove partial node %d after create failure: %v", err, n.ID().SnowflakeID(), cleanupErr)
		}
	}
	return err
}

// preparedNodeCreate is one element of a node batch create after the checks
// addNodeInternal makes before it mints an ID.
type preparedNodeCreate struct {
	authorID, authorizedBy        string
	signature                     []byte
	authLevel                     uint8
	validFrom, validTo, createdAt types.Instant
	txFromOverride                types.Instant
	props                         types.OwnedPropertySlice
}

// prepareNodeCreate makes addNodeInternal's checks of one element's
// properties: provenance and temporal shadow keys, the backfill gate, the
// property limits and the property slice.
func (c *Core) prepareNodeCreate(props map[string]any, pn *preparedNodeCreate) error {
	authorID, sig, authorizedBy, authLevel, props, err := extractProvenance(props)
	if err != nil {
		return err
	}
	validFrom, validTo, createdAt, txFromOverride, props, err := extractTemporal(props)
	if err != nil {
		return err
	}
	txFromOverride, err = c.resolveBackfillTxFrom(txFromOverride)
	if err != nil {
		return err
	}
	if err := c.validateProperties(props); err != nil {
		return err
	}
	ps, err := types.NewOwnedPropertySlice(props)
	if err != nil {
		return fmt.Errorf("graph: node properties: %w", err)
	}
	*pn = preparedNodeCreate{
		authorID: authorID, signature: sig, authorizedBy: authorizedBy, authLevel: authLevel,
		validFrom: validFrom, validTo: validTo, createdAt: createdAt,
		txFromOverride: txFromOverride, props: ps,
	}
	return nil
}

// stampTemporal sets the node's transaction time (now, or the backfilled
// override) and the caller's temporal metadata, as addNodeInternal does after
// the hash.
func (pn *preparedNodeCreate) stampTemporal(n *types.Node, now types.Instant) {
	if pn.txFromOverride != 0 {
		now = pn.txFromOverride
	}
	tm := n.Temporal()
	if tm == nil {
		tm = &types.TemporalMetadata{}
		n.SetTemporal(tm)
	}
	tm.TxFrom = now
	if pn.validFrom != 0 {
		tm.ValidFrom = pn.validFrom
	}
	if pn.validTo != 0 {
		tm.ValidTo = pn.validTo
	}
	if pn.createdAt != 0 {
		tm.CreatedAt = pn.createdAt
	}
}
