package core

import (
	"errors"
	"fmt"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Batched bitemporal point read (backlog 57).
//
// NodesAtTxByIDs / RelsAtTxByIDs answer NodeAtTx / RelAtTx for many IDs at one
// (validAt, txAt), with the per-ID door's gates and body. The batch shares the
// open/ID/pin checks, holds the graph read lock once per effScanChunk IDs, reads
// through one store.ReadSnapshot where offered (effective_scan_reads.go) and
// resolves a repeated ID once.

// NodesAtTxByIDs returns, aligned to ids, the row NodeAtTx(id, validAt, txAt)
// returns (nil for ErrNodeNotFound / ErrNoVersionValidAt; repeated ids answered
// at each position, rows may alias). Call-level errors: ErrGraphClosed,
// ErrInvalidStoreMutation (invalid id, checked first), ErrInvalidTimeRange
// (txAt == 0; a negative pin recorded nothing, all nil, round 11),
// ErrTxPinTooNew, and a member's ErrHistoryCompacted /
// ErrRetentionExpired (fails the call). Empty ids returns an empty slice.
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
	if txAt == 0 {
		// 0 is NodeAtTx's "no pin"; the batch needs a pin. A pin at the
		// instant 0 is NodesAtOptsByIDs with TxAtSet (round 11).
		return nil, fmt.Errorf("%w: transaction-time pin 0 (a pin is required; a pin at the instant 0: NodesAtOptsByIDs with TxAtSet)", ErrInvalidTimeRange)
	}
	if err := c.validateEffectivePin(txAt); err != nil { // a negative pin: nothing recorded yet (round 11)
		return nil, err
	}
	return c.nodesAtTxByIDs(ids, validAt, txAt)
}

// nodesAtTxByIDs is NodesAtTxByIDs after its input checks (NodesAtOptsByIDs
// shares it): per ID NodeAtTx's gates and body, txAt 0 no pin.
func (c *Core) nodesAtTxByIDs(ids []types.NodeID, validAt, txAt types.Instant) ([]*types.Node, error) {
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

// RelsAtTxByIDs is NodesAtTxByIDs for relationships, with RelAtTx's gates
// (including the relationship-family retention gate). DECLARED view: not masked
// by endpoint validity.
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
	if txAt == 0 {
		// 0 is NodeAtTx's "no pin"; the batch needs a pin. A pin at the
		// instant 0 is RelsAtOptsByIDs with TxAtSet (round 11).
		return nil, fmt.Errorf("%w: transaction-time pin 0 (a pin is required; a pin at the instant 0: RelsAtOptsByIDs with TxAtSet)", ErrInvalidTimeRange)
	}
	if err := c.validateEffectivePin(txAt); err != nil { // a negative pin: nothing recorded yet (round 11)
		return nil, err
	}
	return c.relsAtTxByIDs(ids, validAt, txAt)
}

// relsAtTxByIDs is RelsAtTxByIDs after its input checks (RelsAtOptsByIDs
// shares it).
func (c *Core) relsAtTxByIDs(ids []types.RelID, validAt, txAt types.Instant) ([]*types.Relationship, error) {
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

// atTxByIDs runs read once per distinct id, effScanChunk ids per read-lock hold,
// and lays answers out aligned to ids. The first read error fails the call.
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
