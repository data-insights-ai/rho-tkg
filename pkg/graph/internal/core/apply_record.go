package core

import (
	"errors"
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
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
	case storepkg.ChangeForeignIncoming:
		body, err := storeutil.DecodeRelPut(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyForeignIncomingLocked(body, rec)
	case storepkg.ChangeForeignIncomingDelete:
		body, err := storeutil.DecodeForeignIncomingDelete(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyForeignIncomingDeleteLocked(body)
	case storepkg.ChangeRangePurge:
		body, err := storeutil.DecodeRangePurge(rec.Payload)
		if err != nil {
			return err
		}
		return c.applyRangePurgeLocked(body)
	case storepkg.ChangeClear:
		// Captured BEFORE Clear (BACKLOG 13l), mirroring Admin.Reset(): a
		// replica's id_slot_lease is data-independent orchestrator state that
		// must survive a ChangeClear apply too, not just a primary-side Reset.
		leaseBytes := c.captureIDSlotLeaseForReset()
		if err := c.store.Clear(); err != nil {
			return err
		}
		// A replica applying ChangeClear must reproduce the exact state a
		// primary's Reset() ends up in, not just wipe the Store — Reset() also
		// clears Core-level in-memory state (as-of DocValues cache, unique
		// constraints/owners, compaction/retention watermarks, op counters)
		// that store.Clear() cannot reach (BACKLOG 12a).
		return c.reapCoreStateForClear(leaseBytes)
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
		return fmt.Errorf("graph: apply: node %d: %w: %v", body.Wire.ID, ErrCorruptExport, err)
	}
	if err := c.validatePropertySliceLimits(n.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedNodeHash(n, body.Wire.ID, "node"); err != nil {
		return err
	}
	c.noteAppliedTxFrom(n.Temporal())
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
		reproduceSupersededNodeTxTo(local, n)
		return c.store.ReplaceNodeWithHistory(n, local.Version(), local)
	}
	return c.store.ReplaceNode(n)
}

// reproduceSupersededNodeTxTo restores the ONE field a WithHistory put record
// does not carry: the superseded prior version's TxTo. Every WithHistory emitter
// (Update, label-token mutation) stamps prev.TxTo = the new version's TxFrom (a
// single monotonic `now`), but the record carries only the NEW version, so a
// replica / delta-merge regenerating the prior history row from its in-sync local
// current must restore that field for a BYTE-exact (not merely hash-exact)
// reproduction. TxTo is not part of the integrity hash, so this never alters the
// verified chain — which is exactly why a hash-only convergence oracle cannot see
// the gap. No-op when the new version carries no TxFrom (defensive).
func reproduceSupersededNodeTxTo(prev, next *types.Node) {
	nt := next.Temporal()
	if nt == nil || nt.TxFrom == 0 {
		return
	}
	pt := prev.Temporal()
	if pt == nil {
		pt = &types.TemporalMetadata{}
		prev.SetTemporal(pt)
	}
	pt.TxTo = nt.TxFrom
}

// reproduceSupersededRelTxTo is the relationship counterpart.
func reproduceSupersededRelTxTo(prev, next *types.Relationship) {
	nt := next.Temporal()
	if nt == nil || nt.TxFrom == 0 {
		return
	}
	pt := prev.Temporal()
	if pt == nil {
		pt = &types.TemporalMetadata{}
		prev.SetTemporal(pt)
	}
	pt.TxTo = nt.TxFrom
}

// applyNodeLabelChangeLocked routes a NodePut whose label set differs from the
// local row to the matching label-token door (a label mutation on the primary
// went through Add/RemoveNodeLabelToken{,WithHistory}, which ReplaceNode rejects).
//
// BACKLOG 12j: NodeWire carries only the FINAL label set, never an explicit
// added/removed diff — the wire format has no "this was a label mutation"
// marker at all. Mutation intent is entirely INFERRED here by diffing the
// incoming row against this replica's own local current row
// (labelTokenDiff), which is only a valid inference under two conditions the
// wire itself does not encode or enforce: (1) records apply in strict,
// gap-free LSN order, so "local" really is the primary's pre-mutation state
// (documented at the package level in applyChangeRecordLocked); (2) every
// label-token-mutating door changes exactly ONE token per call (enforced
// below by rejecting addedN+removedN != 1). Both hold today — this is
// correct behavior, not a bug — but a future door that could mutate more
// than one label token per primary-side call, or a replica that applies
// records out of order or with gaps, would silently break this inference
// without any wire-level signal to catch it.
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
			reproduceSupersededNodeTxTo(local, n)
			return c.store.AddNodeLabelTokenWithHistory(id, added, n, local.Version(), local)
		}
		return c.store.AddNodeLabelToken(id, added, n)
	}
	if withHistory {
		reproduceSupersededNodeTxTo(local, n)
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
		return fmt.Errorf("graph: apply: relationship %d: %w: %v", body.Wire.ID, ErrCorruptExport, err)
	}
	if err := c.validatePropertySliceLimits(r.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedRelHash(r, body.Wire.ID, "relationship"); err != nil {
		return err
	}
	c.noteAppliedTxFrom(r.Temporal())
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
		reproduceSupersededRelTxTo(local, r)
		return c.store.ReplaceRelWithHistory(r, local.Version(), local)
	}
	return c.store.ReplaceRelationship(r)
}

// applyForeignIncomingLocked reproduces a cross-machine incoming half-edge stub
// (ADR-0010 Model A) on a replica. The stub's rel-ID belongs to a FOREIGN slot,
// so a plain rel-put apply would route by rel-slot and fail ErrSlotNotLocal;
// instead it routes to the END node's shard via the ForeignIncomingRelCapability,
// idempotently (an already-present stub is a no-op — the watermark advances via a
// separate MetaSet, so re-apply must be a no-op). The row is reconstructed
// verbatim through the same import-trust pipeline as an ordinary rel put.
//
// BACKLOG 12e: calls noteAppliedTxFrom, matching every sibling put/version
// handler (applyNodePutLocked, applyRelPutLocked, applyNodeHistoryVersionLocked,
// applyRelHistoryVersionLocked). The as-of DocValues cache's past-dated
// detector treats ANY out-of-order applied entity TxFrom (node or rel) as
// evidence that this replica's apply stream is not strictly TxFrom-monotonic
// and conservatively invalidates every cached as-of column — a foreign-
// incoming stub is exactly as capable of arriving out of order as an ordinary
// rel put (its origin machine's write time is unrelated to this shard's local
// apply order), so omitting it here left a real gap in that detector, not
// just a stylistic asymmetry.
func (c *Core) applyForeignIncomingLocked(body storeutil.RelPutBody, rec storepkg.ChangeRecord) error {
	if c.foreignIncomingRel == nil {
		return fmt.Errorf("graph: apply: ChangeForeignIncoming requires a partitioned store")
	}
	if err := c.validateRelTokensWithRefetch(&body.Wire, rec); err != nil {
		return err
	}
	r, err := storeutil.WireToRelChecked(body.Wire)
	if err != nil {
		return fmt.Errorf("graph: apply: foreign-incoming relationship %d: %w: %v", body.Wire.ID, ErrCorruptExport, err)
	}
	if err := c.validatePropertySliceLimits(r.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedRelHash(r, body.Wire.ID, "relationship"); err != nil {
		return err
	}
	c.noteAppliedTxFrom(r.Temporal())
	if err := c.foreignIncomingRel.RecordForeignIncoming(r, generatedcreate.FreshGraphID()); err != nil && !errors.Is(err, storepkg.ErrRelExists) {
		return err
	}
	return nil
}

// applyForeignIncomingDeleteLocked removes a Model-A incoming half-edge stub on a
// replica (ADR-0010 §3.3 cascade), routing by the END-node slot carried in the
// record — the rel-ID's own slot is foreign, so a plain rel-delete apply would
// fail ErrSlotNotLocal. Idempotent: an already-absent stub is a no-op (the
// watermark advances via a separate MetaSet, so re-apply must not error).
func (c *Core) applyForeignIncomingDeleteLocked(body storeutil.ForeignIncomingDeleteBody) error {
	if c.foreignIncomingRel == nil {
		return fmt.Errorf("graph: apply: ChangeForeignIncomingDelete requires a partitioned store")
	}
	return c.foreignIncomingRel.DeleteForeignIncoming(types.RelID(body.RelID), types.NodeID(body.EndID))
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
		return fmt.Errorf("graph: apply: node delete tombstone %d: %w: %v", body.ID, ErrCorruptExport, err)
	}
	relTombs := make([]storepkg.RelTombstone, 0, len(body.RelTombstones))
	for i := range body.RelTombstones {
		rw := body.RelTombstones[i]
		rt, err := storeutil.WireToRelChecked(rw)
		if err != nil {
			return fmt.Errorf("graph: apply: cascade tombstone rel %d: %w: %v", rw.ID, ErrCorruptExport, err)
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
		return fmt.Errorf("graph: apply: rel delete tombstone %d: %w: %v", body.ID, ErrCorruptExport, err)
	}
	return c.store.DeleteRelWithHistory(id, local.Version(), tomb)
}

func (c *Core) applyNodeHistoryVersionLocked(body storeutil.HistoryVersionNodeBody, rec storepkg.ChangeRecord) error {
	if err := c.validateNodeTokensWithRefetch(&body.Wire, rec); err != nil {
		return err
	}
	n, err := storeutil.WireToNodeChecked(body.Wire)
	if err != nil {
		return fmt.Errorf("graph: apply: node version %d: %w: %v", body.Wire.ID, ErrCorruptExport, err)
	}
	if err := c.validatePropertySliceLimits(n.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedNodeHash(n, body.Wire.ID, "node version"); err != nil {
		return err
	}
	c.noteAppliedTxFrom(n.Temporal())
	return c.store.PutNodeVersion(n.InternalID(), uint32(body.Version), n) // #nosec G115 — version from our own wire
}

func (c *Core) applyRelHistoryVersionLocked(body storeutil.HistoryVersionRelBody, rec storepkg.ChangeRecord) error {
	if err := c.validateRelTokensWithRefetch(&body.Wire, rec); err != nil {
		return err
	}
	r, err := storeutil.WireToRelChecked(body.Wire)
	if err != nil {
		return fmt.Errorf("graph: apply: relationship version %d: %w: %v", body.Wire.ID, ErrCorruptExport, err)
	}
	if err := c.validatePropertySliceLimits(r.Properties()); err != nil {
		return err
	}
	if err := c.verifyImportedRelHash(r, body.Wire.ID, "relationship version"); err != nil {
		return err
	}
	c.noteAppliedTxFrom(r.Temporal())
	return c.store.PutRelVersion(r.InternalID(), uint32(body.Version), r) // #nosec G115 — version from our own wire
}

func (c *Core) applyHistoryTruncateLocked(isNode bool, body storeutil.HistoryTruncateBody) error {
	// Truncation/trim removes history rows, which can change an as-of belief within
	// the removed range — invalidate the as-of column cache.
	c.asOfColumns.bump()
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
