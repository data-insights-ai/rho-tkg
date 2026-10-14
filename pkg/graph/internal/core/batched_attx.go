package core

import (
	"errors"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// =============================================================================
// Batched bitemporal point read (backlog 57, sigma-tkgd R1)
//
// NodesAtTxByIDs / RelsAtTxByIDs answer NodeAtTx / RelAtTx for many IDs at one
// (validAt, txAt): per ID the per-ID door's gates and body (nodeAtLockedTx's
// gates, then nodeAtFromCurrent), so every element is that door's answer.
// What the batch shares: the open / ID / pin checks run once, the graph read
// lock is held once per effScanChunk IDs, the reads go through one
// store.ReadSnapshot where the store offers one (badger: one transaction per
// chunk instead of one per read; see effective_scan_reads.go), and a repeated
// ID is resolved once. An ID whose current row answers (the backlog-10c
// shortcut) reads no history.
// =============================================================================

// NodesAtTxByIDs returns, for each element of ids, the row NodeAtTx(id,
// validAt, txAt) returns, aligned to ids (same length and order; a repeated
// id is answered at each position, the rows may alias). An element is nil
// where NodeAtTx answers ErrNodeNotFound or ErrNoVersionValidAt. Call-level
// errors: ErrGraphClosed, ErrInvalidStoreMutation (an invalid id, checked
// before anything is read), ErrInvalidTimeRange (txAt <= 0), ErrTxPinTooNew
// (txAt above the commit clock), and a member's ErrHistoryCompacted /
// ErrRetentionExpired (the per-ID gates; the call fails, no rows). Empty ids
// returns an empty slice.
func (t *TempOps) NodesAtTxByIDs(ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateNodeID(id); err != nil {
			return nil, err
		}
	}
	if err := c.validateEffectivePin(txAt); err != nil {
		return nil, err
	}
	return atTxByIDs(c, ids, func(rd entityReads, id types.NodeID) (*types.Node, error) {
		if err := c.checkNodePointCompaction(id, txAt); err != nil {
			return nil, err
		}
		if err := c.checkNodePointRetention(id, txAt); err != nil {
			return nil, err
		}
		current, err := rd.currentNode(id)
		if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
			return nil, err
		}
		n, err := c.nodeAtFromCurrent(rd, id, current, validAt, txAt)
		if errors.Is(err, storepkg.ErrNodeNotFound) || errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, nil
		}
		return n, err
	})
}

// RelsAtTxByIDs is NodesAtTxByIDs for relationships: each element is the
// row RelAtTx returns (nil for ErrRelNotFound / ErrNoVersionValidAt), with
// RelAtTx's gates (the relationship-family retention gate included).
// DECLARED view: not masked by endpoint validity.
func (t *TempOps) RelsAtTxByIDs(ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error) {
	c := t.c
	if err := c.checkOpen(); err != nil {
		return nil, err
	}
	for _, id := range ids {
		if err := storepkg.ValidateRelID(id); err != nil {
			return nil, err
		}
	}
	if err := c.validateEffectivePin(txAt); err != nil {
		return nil, err
	}
	return atTxByIDs(c, ids, func(rd entityReads, id types.RelID) (*types.Relationship, error) {
		if err := c.checkRelTypePointRetention(id, txAt); err != nil {
			return nil, err
		}
		if err := c.checkRelPointCompaction(id, txAt); err != nil {
			return nil, err
		}
		if err := c.checkRelPointRetention(txAt); err != nil {
			return nil, err
		}
		current, err := rd.currentRel(id)
		if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
			return nil, err
		}
		r, err := c.relAtFromCurrent(rd, id, current, validAt, txAt)
		if errors.Is(err, storepkg.ErrRelNotFound) || errors.Is(err, storepkg.ErrNoVersionValidAt) {
			return nil, nil
		}
		return r, err
	})
}

// atTxByIDs runs read once per distinct id, effScanChunk ids per hold of the
// graph read lock, through one store read snapshot where the store offers
// one, and lays the answers out aligned to ids. The first read error fails
// the call.
func atTxByIDs[ID comparable, T any](c *Core, ids []ID, read func(entityReads, ID) (T, error)) ([]T, error) {
	if len(ids) == 0 {
		return []T{}, nil
	}
	unique := firstOccurrences(ids)
	answers := make([]T, len(unique)) // aligned to unique
	rd := entityReads{c: c}
	if c.readSnapshots != nil {
		rd.snap = c.readSnapshots.NewReadSnapshot()
		defer rd.snap.Close()
	}
	for start := 0; start < len(unique); start += effScanChunk {
		chunk := unique[start:min(start+effScanChunk, len(unique))]
		if err := c.readUnderRLock(func() error {
			if rd.snap != nil {
				defer rd.snap.Release() // no transaction held between chunks
			}
			for i, id := range chunk {
				v, err := read(rd, id)
				if err != nil {
					return err
				}
				answers[start+i] = v
			}
			return nil
		}); err != nil {
			return nil, err
		}
	}
	if len(unique) == len(ids) {
		return answers, nil
	}
	at := make(map[ID]int, len(unique))
	for i, id := range unique {
		at[id] = i
	}
	out := make([]T, len(ids))
	for i, id := range ids {
		out[i] = answers[at[id]]
	}
	return out, nil
}
