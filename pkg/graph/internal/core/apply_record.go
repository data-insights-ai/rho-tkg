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
		return c.applyNodePutLocked(body, rec)
	case storepkg.ChangeRelPut:
		body, err := storeutil.DecodeRelPut(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyRelPutLocked(body, rec)
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
		return c.applyNodeHistoryVersionLocked(body, rec)
	case storepkg.ChangeRelHistoryVersion:
		body, err := storeutil.DecodeHistoryVersionRel(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyRelHistoryVersionLocked(body, rec)
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
// AFTER this replica's bootstrap snapshot, and the lazy registry refetch could
// not resolve it. This is reached when NO ReplicationSource is configured (the
// replica has no door to refetch the primary's registry), or when a refetch ran
// but the token was still absent afterward. (The wrapped ErrCorruptExport is
// preserved for errors.Is.)
func applyTokenSyncErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w — replica token registry is behind the primary (a label/rel-type was registered after this replica's bootstrap snapshot; configure a ReplicationSource via Config.ReplicationSource / g.SetReplicationSource so the replica can refetch the primary's registry)", err)
}

// appendNamer is the append-only grow surface the refetch hook drives. Both
// LabelRegistry and RelTypeRegistry satisfy it (PropertyKeyRegistry is tokenized
// locally from untokenized record wires and is never synced from the primary).
type appendNamer interface {
	ExportNames() []string
	AppendNames(prefix, suffix []string) (bool, error)
}

// appendRegistrySuffix extends reg with the tail of target beyond reg's current
// length. No-op when the primary is not ahead on this registry. Returns an error
// if the primary's names do not append-only-extend the replica's (divergence).
func appendRegistrySuffix(reg appendNamer, target []string, kind string) error {
	current := reg.ExportNames()
	// The primary must append-only-EXTEND us: our current names must be a prefix
	// of target. Verify the overlap in EVERY case (not just the grow branch) — a
	// corrupt/foreign/misconfigured source that diverges within the prefix region
	// must fail closed, not silently graft a mismatched suffix onto our names.
	if len(target) < len(current) {
		return fmt.Errorf("graph: apply: %s %w (primary registry shorter: %d < %d)", kind, storepkg.ErrRegistryDiverged, len(target), len(current))
	}
	for i := range current {
		if target[i] != current[i] {
			return fmt.Errorf("graph: apply: %s %w (mismatch at token %d)", kind, storepkg.ErrRegistryDiverged, i)
		}
	}
	if len(target) == len(current) {
		return nil // prefix verified equal; nothing to append
	}
	suffix := target[len(current):]
	ok, err := reg.AppendNames(current, suffix)
	if err != nil {
		return fmt.Errorf("graph: apply: append %s registry: %w", kind, err)
	}
	if !ok {
		// AppendNames re-verifies current==reg under its own lock; (false) here
		// means the registry changed between our ExportNames and the append (we
		// hold c.registryMu, so this should not occur) — fail closed.
		return fmt.Errorf("graph: apply: %s %w (registry changed during append)", kind, storepkg.ErrRegistryDiverged)
	}
	return nil
}

// refetchRegistriesLocked pulls the primary's registry snapshot and
// append-only-extends the replica's label + rel-type registries from it, then
// persists. Caller holds c.mu.Lock; the source is called OUTSIDE c.registryMu
// (it is the remote/primary Core, taking only its OWN locks), and the local grow
// + persist happen under c.registryMu (the established registry lock order:
// c.mu -> c.registryMu). On a persist failure the in-memory grow is rolled back
// so the in-memory registry never runs ahead of durable storage (which would
// leave an applied entity referencing an unpersisted token after a crash).
func (c *Core) refetchRegistriesLocked(src storepkg.ReplicationSource, rec storepkg.ChangeRecord) error {
	snap, err := src.RegistrySnapshot()
	if err != nil {
		return fmt.Errorf("graph: apply change LSN %d: registry refetch: %w", rec.LSN, err)
	}
	if snap == nil {
		return fmt.Errorf("graph: apply change LSN %d: registry refetch returned nil snapshot", rec.LSN)
	}
	// The primary's snapshot must already contain the missing token: its capture
	// LSN must be at or beyond the record we are applying.
	if snap.CapturedAtLSN < rec.LSN {
		return fmt.Errorf("graph: apply change LSN %d: %w (captured at LSN %d); retry after the primary commits", rec.LSN, storepkg.ErrPrimaryRegistryStale, snap.CapturedAtLSN)
	}

	c.registryMu.Lock()
	defer c.registryMu.Unlock()

	labelSnap := c.labels.ExportNames()
	relSnap := c.relTypes.ExportNames()
	restore := func() {
		if alloc := newlyAllocatedNames(labelSnap, c.labels.ExportNames()); len(alloc) > 0 {
			_, _ = c.labels.RollbackNames(labelSnap, alloc...)
		}
		if alloc := newlyAllocatedNames(relSnap, c.relTypes.ExportNames()); len(alloc) > 0 {
			_, _ = c.relTypes.RollbackNames(relSnap, alloc...)
		}
	}
	if err := appendRegistrySuffix(c.labels, snap.Labels, "label"); err != nil {
		restore()
		return err
	}
	if err := appendRegistrySuffix(c.relTypes, snap.RelTypes, "rel-type"); err != nil {
		restore()
		return err
	}
	// persistRegistries writes label+reltype (SaveRegistries, one atomic txn) and
	// property keys (SavePropertyKeyRegistry). On the badger backend under default
	// !SyncWrites this commits to the LSM without an fsync, so a crash can lose
	// the grow — but that is recovered, not corrupting: tokens are stored
	// numerically in rows (the row stays decodable), and the watermark advances
	// only AFTER the entity flush, so a lost grow re-fetches and re-applies
	// idempotently from the unadvanced watermark. After a successful refetch in a
	// batch the durable registry may legitimately LEAD the watermark (a later
	// record in the same batch can fail); that is the safe direction for an
	// append-only registry (AppendNames is a no-op once the tokens are present).
	// On persist failure we roll the in-memory grow back so in-memory == disk.
	if err := c.persistRegistries(); err != nil {
		restore()
		return fmt.Errorf("graph: apply change LSN %d: persist grown registry: %w", rec.LSN, err)
	}
	return nil
}

// validateNodeTokensWithRefetch validates a node wire's label tokens against the
// local registry; on an unresolved token, if a ReplicationSource is configured,
// it refetches the primary's registry, append-only-extends, and re-validates.
// Without a source (or if still unresolved after a refetch) it fails closed via
// applyTokenSyncErr, exactly as before this capability existed.
func (c *Core) validateNodeTokensWithRefetch(w *storeutil.NodeWire, rec storepkg.ChangeRecord) error {
	origErr := validateNodeTokensInRegistry(w, c.labels)
	if origErr == nil {
		return nil
	}
	src := c.replicationSource()
	if src == nil {
		return applyTokenSyncErr(origErr)
	}
	if err := c.refetchRegistriesLocked(src, rec); err != nil {
		return err
	}
	if err := validateNodeTokensInRegistry(w, c.labels); err != nil {
		return applyTokenSyncErr(err)
	}
	return nil
}

// validateRelTokensWithRefetch is the relationship counterpart.
func (c *Core) validateRelTokensWithRefetch(w *storeutil.RelWire, rec storepkg.ChangeRecord) error {
	origErr := validateRelTokensInRegistry(w, c.relTypes)
	if origErr == nil {
		return nil
	}
	src := c.replicationSource()
	if src == nil {
		return applyTokenSyncErr(origErr)
	}
	if err := c.refetchRegistriesLocked(src, rec); err != nil {
		return err
	}
	if err := validateRelTokensInRegistry(w, c.relTypes); err != nil {
		return applyTokenSyncErr(err)
	}
	return nil
}

func (c *Core) applyNodePutLocked(body storeutil.NodePutBody, rec storepkg.ChangeRecord) error {
	if err := c.validateNodeTokensWithRefetch(&body.Wire, rec); err != nil {
		return err
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
	if added, removed, addedN, removedN := labelTokenDiff(local, n); addedN+removedN > 0 {
		return c.applyNodeLabelChangeLocked(local, n, added, removed, addedN, removedN, body.WithHistory)
	}
	if body.WithHistory {
		return c.store.ReplaceNodeWithHistory(n, local.Version(), local)
	}
	return c.store.ReplaceNode(n)
}

// applyNodeLabelChangeLocked routes a NodePut whose label set differs from the
// local row to the matching label-token door (a label mutation on the primary
// went through Add/RemoveNodeLabelToken{,WithHistory}, which ReplaceNode rejects).
func (c *Core) applyNodeLabelChangeLocked(local, n *types.Node, added, removed uint16, addedN, removedN int, withHistory bool) error {
	id := n.InternalID()
	// Every label-token door mutates exactly one token, so a well-formed
	// label-mutation NodePut differs from the local row by exactly one added OR
	// one removed token. Anything else — multiple added, multiple removed, or a
	// simultaneous add+remove — is not representable as a single label-token door
	// op. Such a record cannot come from a correct contiguous feed (it implies a
	// gap or a hand-crafted record); fail closed rather than silently apply one
	// token and leave the label index diverged from the row.
	if addedN+removedN != 1 {
		return fmt.Errorf("graph: apply: node %d label change is not a single-token mutation (%d added, %d removed) — a well-formed feed emits one label-token door op per record", body64(id), addedN, removedN)
	}
	if addedN == 1 {
		if withHistory {
			return c.store.AddNodeLabelTokenWithHistory(id, added, n, local.Version(), local)
		}
		return c.store.AddNodeLabelToken(id, added, n)
	}
	if withHistory {
		return c.store.RemoveNodeLabelTokenWithHistory(id, removed, n, local.Version(), local)
	}
	return c.store.RemoveNodeLabelToken(id, removed, n)
}

func (c *Core) applyRelPutLocked(body storeutil.RelPutBody, rec storepkg.ChangeRecord) error {
	if err := c.validateRelTokensWithRefetch(&body.Wire, rec); err != nil {
		return err
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

func (c *Core) applyNodeHistoryVersionLocked(body storeutil.HistoryVersionNodeBody, rec storepkg.ChangeRecord) error {
	if err := c.validateNodeTokensWithRefetch(&body.Wire, rec); err != nil {
		return err
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

func (c *Core) applyRelHistoryVersionLocked(body storeutil.HistoryVersionRelBody, rec storepkg.ChangeRecord) error {
	if err := c.validateRelTokensWithRefetch(&body.Wire, rec); err != nil {
		return err
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

// labelTokenDiff reports the label tokens added and removed between the local
// row and the incoming node, with their counts. A well-formed label-mutation
// NodePut differs by exactly one token (each label-token door mutates one token);
// the counts let the caller fail closed on a multi-token diff instead of silently
// applying only one. A non-label update has addedN == removedN == 0. When a count
// exceeds 1, the returned added/removed token is an arbitrary one of them — the
// caller must treat addedN+removedN != 1 as an error, not act on the token.
func labelTokenDiff(local, n *types.Node) (added, removed uint16, addedN, removedN int) {
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
			added, addedN = t, addedN+1
		}
	}
	for t := range lset {
		if _, ok := nset[t]; !ok {
			removed, removedN = t, removedN+1
		}
	}
	return added, removed, addedN, removedN
}

func body64(id types.NodeID) int64 { return int64(id.SnowflakeID()) }
