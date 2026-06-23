package core

import (
	"errors"
	"fmt"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// applyChangeRecordLocked applies one change-log record from a primary's feed to
// this (replica) store, reproducing the primary's row EXACTLY: every write goes
// through a foreign-ID store door that persists the supplied wire VERBATIM (the
// integrity hash, version, and full temporal metadata are reconstructed from the
// record, never recomputed or re-stamped), so the replica's bytes equal the
// primary's. The caller holds c.mu.Lock; this path deliberately does NOT call
// checkWritable, so it works on a read-only replica.
//
// Records are total-ordered by LSN, so when a record references an entity, all
// prior records establishing it have already applied — the replica's local
// current row therefore equals the primary's pre-mutation state, which is what
// lets a regenerated history row (ReplaceNodeWithHistory) be byte-exact.
//
// Every handler is idempotent under at-least-once redelivery (a crash between a
// door commit and the watermark advance replays the last record): an identical
// row is a no-op, and a delete tolerates a missing entity as already-applied.
func (c *Core) applyChangeRecordLocked(rec storepkg.ChangeRecord) error {
	switch rec.Tag {
	case storepkg.ChangeNodePut:
		body, err := storeutil.DecodeNodePut(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyNodePutLocked(body)
	case storepkg.ChangeRelPut:
		body, err := storeutil.DecodeRelPut(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyRelPutLocked(body)
	case storepkg.ChangeNodeDelete:
		body, err := storeutil.DecodeNodeDelete(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyNodeDeleteLocked(body)
	case storepkg.ChangeRelDelete:
		body, err := storeutil.DecodeRelDelete(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyRelDeleteLocked(body)
	case storepkg.ChangeNodeHistoryVersion:
		body, err := storeutil.DecodeHistoryVersionNode(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyNodeHistoryVersionLocked(body)
	case storepkg.ChangeRelHistoryVersion:
		body, err := storeutil.DecodeHistoryVersionRel(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyRelHistoryVersionLocked(body)
	case storepkg.ChangeNodeHistoryTruncate:
		body, err := storeutil.DecodeHistoryTruncate(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyHistoryTruncateLocked(true, body)
	case storepkg.ChangeRelHistoryTruncate:
		body, err := storeutil.DecodeHistoryTruncate(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyHistoryTruncateLocked(false, body)
	case storepkg.ChangeClear:
		return c.store.Clear()
	default:
		return fmt.Errorf("graph: apply: unknown change tag %d", byte(rec.Tag))
	}
}

// applyTokenSyncErr annotates a token-not-in-registry failure with its likely
// operational cause on a replica: the primary registered a label / rel-type
// AFTER this replica's bootstrap snapshot, and lazy registry refetch is a
// deferred Phase-1 feature — so until it lands, the primary must not register
// new tokens while a replica is catching up. (The wrapped ErrCorruptExport is
// preserved for errors.Is.)
func applyTokenSyncErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w — replica token registry is behind the primary (a label/rel-type was registered after this replica's bootstrap snapshot; lazy registry refetch is a deferred Phase-1 feature)", err)
}

func (c *Core) applyNodePutLocked(body storeutil.NodePutBody) error {
	if err := validateNodeTokensInRegistry(&body.Wire, c.labels); err != nil {
		return applyTokenSyncErr(err)
	}
	n, err := storeutil.WireToNodeChecked(body.Wire)
	if err != nil {
		return err
	}
	if err := c.validatePropertySliceLimits(n.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedNodeHash(n, body.Wire.ID, "node"); err != nil {
		return err
	}
	id := n.InternalID()
	local, gerr := c.store.GetNode(id)
	if errors.Is(gerr, storepkg.ErrNodeNotFound) {
		return c.store.PutNode(n)
	}
	if gerr != nil {
		return gerr
	}
	if nodeWireMatches(local, &body.Wire) {
		return nil // already applied (idempotent redelivery)
	}
	if added, removed, changed := labelTokenDiff(local, n); changed {
		return c.applyNodeLabelChangeLocked(local, n, added, removed, body.WithHistory)
	}
	if body.WithHistory {
		return c.store.ReplaceNodeWithHistory(n, local.Version(), local)
	}
	return c.store.ReplaceNode(n)
}

// applyNodeLabelChangeLocked routes a NodePut whose label set differs from the
// local row to the matching label-token door (a label mutation on the primary
// went through Add/RemoveNodeLabelToken{,WithHistory}, which ReplaceNode rejects).
func (c *Core) applyNodeLabelChangeLocked(local, n *types.Node, added, removed uint16, withHistory bool) error {
	id := n.InternalID()
	// Every label-token door mutates exactly one token, so a label-mutation
	// NodePut differs from the local row by exactly one added OR one removed
	// token. A simultaneous add+remove is not representable as a single
	// label-token door op — fail closed rather than silently apply only one.
	if added != 0 && removed != 0 {
		return fmt.Errorf("graph: apply: node %d label change adds token %d AND removes token %d (not a single-token mutation)", body64(id), added, removed)
	}
	switch {
	case added != 0:
		if withHistory {
			return c.store.AddNodeLabelTokenWithHistory(id, added, n, local.Version(), local)
		}
		return c.store.AddNodeLabelToken(id, added, n)
	case removed != 0:
		if withHistory {
			return c.store.RemoveNodeLabelTokenWithHistory(id, removed, n, local.Version(), local)
		}
		return c.store.RemoveNodeLabelToken(id, removed, n)
	default:
		return fmt.Errorf("graph: apply: node %d label change with no token diff", body64(n.InternalID()))
	}
}

func (c *Core) applyRelPutLocked(body storeutil.RelPutBody) error {
	if err := validateRelTokensInRegistry(&body.Wire, c.relTypes); err != nil {
		return applyTokenSyncErr(err)
	}
	r, err := storeutil.WireToRelChecked(body.Wire)
	if err != nil {
		return err
	}
	if err := c.validatePropertySliceLimits(r.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedRelHash(r, body.Wire.ID, "relationship"); err != nil {
		return err
	}
	id := r.InternalID()
	local, gerr := c.store.GetRelationship(id)
	if errors.Is(gerr, storepkg.ErrRelNotFound) {
		return c.store.PutRelationship(r)
	}
	if gerr != nil {
		return gerr
	}
	if relWireMatches(local, &body.Wire) {
		return nil
	}
	if body.WithHistory {
		return c.store.ReplaceRelWithHistory(r, local.Version(), local)
	}
	return c.store.ReplaceRelationship(r)
}

func (c *Core) applyNodeDeleteLocked(body storeutil.NodeDeleteBody) error {
	id := types.NodeID(body.ID)
	if !body.WithHistory {
		// Hard delete (unconnected or cascade): DeleteNodeCascade removes the
		// node and whatever adjacency it has, matching the primary (the replica's
		// adjacency equals the primary's by LSN ordering).
		err := c.store.DeleteNodeCascade(id)
		if errors.Is(err, storepkg.ErrNodeNotFound) {
			return nil // already applied
		}
		return err
	}
	if body.Tombstone == nil {
		return fmt.Errorf("graph: apply: with-history node delete %d missing tombstone", body.ID)
	}
	local, gerr := c.store.GetNode(id)
	if errors.Is(gerr, storepkg.ErrNodeNotFound) {
		return nil // already applied
	}
	if gerr != nil {
		return gerr
	}
	nodeTomb, err := storeutil.WireToNodeChecked(*body.Tombstone)
	if err != nil {
		return err
	}
	relTombs := make([]storepkg.RelTombstone, 0, len(body.RelTombstones))
	for i := range body.RelTombstones {
		rw := body.RelTombstones[i]
		rt, err := storeutil.WireToRelChecked(rw)
		if err != nil {
			return err
		}
		localRel, rerr := c.store.GetRelationship(types.RelID(rw.ID))
		if rerr != nil {
			return fmt.Errorf("graph: apply: cascade tombstone rel %d: %w", rw.ID, rerr)
		}
		relTombs = append(relTombs, storepkg.RelTombstone{
			ID:          types.RelID(rw.ID),
			PrevVersion: localRel.Version(),
			Tombstone:   rt,
		})
	}
	return c.store.DeleteNodeWithHistory(id, local.Version(), nodeTomb, relTombs)
}

func (c *Core) applyRelDeleteLocked(body storeutil.RelDeleteBody) error {
	id := types.RelID(body.ID)
	if !body.WithHistory {
		err := c.store.DeleteRelationship(id)
		if errors.Is(err, storepkg.ErrRelNotFound) {
			return nil
		}
		return err
	}
	if body.Tombstone == nil {
		return fmt.Errorf("graph: apply: with-history rel delete %d missing tombstone", body.ID)
	}
	local, gerr := c.store.GetRelationship(id)
	if errors.Is(gerr, storepkg.ErrRelNotFound) {
		return nil
	}
	if gerr != nil {
		return gerr
	}
	tomb, err := storeutil.WireToRelChecked(*body.Tombstone)
	if err != nil {
		return err
	}
	return c.store.DeleteRelWithHistory(id, local.Version(), tomb)
}

func (c *Core) applyNodeHistoryVersionLocked(body storeutil.HistoryVersionNodeBody) error {
	if err := validateNodeTokensInRegistry(&body.Wire, c.labels); err != nil {
		return applyTokenSyncErr(err)
	}
	n, err := storeutil.WireToNodeChecked(body.Wire)
	if err != nil {
		return err
	}
	if err := c.validatePropertySliceLimits(n.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedNodeHash(n, body.Wire.ID, "node version"); err != nil {
		return err
	}
	return c.store.PutNodeVersion(n.InternalID(), uint32(body.Version), n) // #nosec G115 — version from our own wire
}

func (c *Core) applyRelHistoryVersionLocked(body storeutil.HistoryVersionRelBody) error {
	if err := validateRelTokensInRegistry(&body.Wire, c.relTypes); err != nil {
		return applyTokenSyncErr(err)
	}
	r, err := storeutil.WireToRelChecked(body.Wire)
	if err != nil {
		return err
	}
	if err := c.validatePropertySliceLimits(r.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedRelHash(r, body.Wire.ID, "relationship version"); err != nil {
		return err
	}
	return c.store.PutRelVersion(r.InternalID(), uint32(body.Version), r) // #nosec G115 — version from our own wire
}

func (c *Core) applyHistoryTruncateLocked(isNode bool, body storeutil.HistoryTruncateBody) error {
	if body.IsTrim {
		// trim-from is the optional HistoryRollbackTrimCapability; a primary only
		// emits this record on a backend that has it, so a same-backend replica
		// has it too. Fail closed if not.
		if c.historyTrim == nil {
			return fmt.Errorf("graph: apply: trim-from record but backend lacks %w", storepkg.ErrCapabilityNotSupported)
		}
		if isNode {
			return c.historyTrim.TrimNodeHistoryFrom(types.NodeID(body.ID), uint32(body.Bound)) // #nosec G115 — bound from our own record
		}
		return c.historyTrim.TrimRelHistoryFrom(types.RelID(body.ID), uint32(body.Bound)) // #nosec G115 — bound from our own record
	}
	if isNode {
		return c.store.TruncateNodeHistory(types.NodeID(body.ID), int(body.Bound))
	}
	return c.store.TruncateRelHistory(types.RelID(body.ID), int(body.Bound))
}

// labelTokenDiff reports the single label token added or removed between the
// local row and the incoming node (a label-mutation NodePut differs by exactly
// one token; a non-label update has changed == false).
func labelTokenDiff(local, n *types.Node) (added, removed uint16, changed bool) {
	lset := make(map[uint16]struct{}, local.LabelTokenCount())
	for i := 0; i < local.LabelTokenCount(); i++ {
		lset[local.LabelTokenRawAt(i)] = struct{}{}
	}
	nset := make(map[uint16]struct{}, n.LabelTokenCount())
	for i := 0; i < n.LabelTokenCount(); i++ {
		nset[n.LabelTokenRawAt(i)] = struct{}{}
	}
	for t := range nset {
		if _, ok := lset[t]; !ok {
			added, changed = t, true
		}
	}
	for t := range lset {
		if _, ok := nset[t]; !ok {
			removed, changed = t, true
		}
	}
	return added, removed, changed
}

func body64(id types.NodeID) int64 { return int64(id.SnowflakeID()) }
