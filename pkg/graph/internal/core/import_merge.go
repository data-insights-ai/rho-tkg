package core

import (
	"bufio"
	"errors"
	"fmt"
	"io"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ImportMerge applies a delta stream (ExportSince) onto this graph. See
// io.API.ImportMerge for the public contract.
//
// It is the consumer of the delta wire: each exportTagChange record carries a
// change-log record verbatim, replayed through the SAME foreign-ID apply doors
// the replica-apply path uses (applyChangeRecordLocked), so the merged rows are
// byte-exact reproductions of the source — including the integrity hash, version,
// and temporal metadata, which are reconstructed from the record and VERIFIED
// (recompute-and-compare), never re-stamped. The registry record is
// append-only-merged onto the base so the records' label/rel-type tokens resolve
// with no live primary, and the final pass re-verifies every touched entity's
// hash chain. Any failure rolls the touched subgraph back to its pre-merge state.
//
// Two-phase, mirroring Import: Phase 1 streams the stream into a bounded staging
// file with no lock held; Phase 2 replays it under c.mu.Lock.
func (o *IOOps) ImportMerge(r io.Reader, opts tkgio.MergeOptions) error {
	c := o.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	// A merge replays arbitrary (possibly past-dated) history verbatim — discard
	// cached as-of columns (a rolled-back merge just forces a harmless rebuild).
	defer c.asOfColumns.bump()
	if isNilInterfaceValue(r) {
		return ErrNilReader
	}
	if opts.MaxStagedBytes < 0 {
		return fmt.Errorf("%w: negative cap %d", ErrImportSizeLimit, opts.MaxStagedBytes)
	}

	// --- Phase 1: stream all records into a bounded stage (no lock) ---
	stage := newImportStage(opts.StagingDir, importMemoryStageLimit)
	defer stage.close()

	var staged int64
	for {
		tag, data, recordSize, rerr := readImportStageRecord(r, staged, opts.MaxStagedBytes)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return fmt.Errorf("import merge: read record: %w", rerr)
		}
		if werr := stage.writeRecord(tag, data, recordSize); werr != nil {
			return fmt.Errorf("import merge: stage record: %w", werr)
		}
		staged += recordSize
	}
	var (
		br  *bufio.Reader
		err error
	)
	if stage.file != nil {
		br, err = stage.reader()
		if err != nil {
			return err
		}
	}

	// --- Phase 2: replay staged records under write lock ---
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	mrb := newMergeRollback(c)
	if err := c.importMergeReplayStageLocked(stage, br, mrb, opts); err != nil {
		if rbErr := mrb.rollback(); rbErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

func (c *Core) importMergeReplayStageLocked(stage *importStage, br *bufio.Reader, mrb *mergeRollback, opts tkgio.MergeOptions) error {
	if stage.file != nil {
		return c.importMergeReplayRecordsLocked(func() (byte, []byte, error) {
			return readExportRecord(br)
		}, mrb, opts)
	}
	data := stage.mem.Bytes()
	offset := 0
	return c.importMergeReplayRecordsLocked(func() (byte, []byte, error) {
		return readExportRecordBytes(data, &offset)
	}, mrb, opts)
}

// importMergeReplayRecordsLocked replays a delta stream. Caller holds c.mu.Lock.
func (c *Core) importMergeReplayRecordsLocked(readRecord func() (byte, []byte, error), mrb *mergeRollback, opts tkgio.MergeOptions) error {
	var (
		seenHeader   bool
		seenRegistry bool
		touchedNodes = make(map[types.NodeID]struct{})
		touchedRels  = make(map[types.RelID]struct{})
	)

	for {
		tag, data, rerr := readRecord()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return fmt.Errorf("import merge: replay staging: %w", rerr)
		}
		switch tag {
		case exportTagHeader:
			if seenHeader {
				return fmt.Errorf("%w: duplicate header record", ErrCorruptExport)
			}
			var hdr exportHeader
			if err := storeutil.SafeUnmarshal(data, &hdr); err != nil {
				return fmt.Errorf("%w: unmarshal header: %v", ErrCorruptExport, err)
			}
			if hdr.Version < exportFormatVersionMin || hdr.Version > exportFormatVersionMax {
				return fmt.Errorf("%w: got %d, want %d..%d", ErrIncompatibleExport, hdr.Version, exportFormatVersionMin, exportFormatVersionMax)
			}
			if !hdr.IsDelta {
				return fmt.Errorf("%w: not a delta stream — use Import for a full export", ErrIncompatibleExport)
			}
			if !opts.ExpectBase.IsZero() {
				if hdr.FromLSN != opts.ExpectBase.LSN || hdr.FromEpoch != opts.ExpectBase.Epoch {
					return fmt.Errorf("%w: delta base {LSN:%d,Epoch:%d} != ExpectBase {LSN:%d,Epoch:%d}",
						ErrDeltaBaseMismatch, hdr.FromLSN, hdr.FromEpoch, opts.ExpectBase.LSN, opts.ExpectBase.Epoch)
				}
			}
			seenHeader = true

		case exportTagRegistry:
			if !seenHeader {
				return fmt.Errorf("%w: registry record before header", ErrCorruptExport)
			}
			if seenRegistry {
				return fmt.Errorf("%w: duplicate registry record", ErrCorruptExport)
			}
			var reg tiered.RegistryFileData
			if err := storeutil.SafeUnmarshal(data, &reg); err != nil {
				return fmt.Errorf("%w: unmarshal registry: %v", ErrCorruptExport, err)
			}
			if err := c.validateRegistryNames("label", reg.Labels); err != nil {
				return fmt.Errorf("%w: label registry: %w", ErrCorruptExport, err)
			}
			if err := c.validateRegistryNames("reltype", reg.RelTypes); err != nil {
				return fmt.Errorf("%w: reltype registry: %w", ErrCorruptExport, err)
			}
			// Reconcile the delta's registry with the base's. The two must agree on
			// their common prefix (registries are append-only, so one is a prefix
			// of the other); the base extends if the delta is longer, and a delta
			// whose registry is SHORTER (an older delta re-applied after a newer
			// one already grew the base) is a no-op — not a divergence.
			if err := c.mergeRegistryNamesLocked(c.labels, reg.Labels, "label"); err != nil {
				return err
			}
			if err := c.mergeRegistryNamesLocked(c.relTypes, reg.RelTypes, "rel-type"); err != nil {
				return err
			}
			if err := c.persistRegistries(); err != nil {
				return fmt.Errorf("import merge: persist registries: %w", err)
			}
			seenRegistry = true

		case exportTagChange:
			if !seenHeader {
				return fmt.Errorf("%w: change record before header", ErrCorruptExport)
			}
			if !seenRegistry {
				return fmt.Errorf("%w: change record before registry", ErrCorruptExport)
			}
			var crw changeRecordWire
			if err := storeutil.SafeUnmarshal(data, &crw); err != nil {
				return fmt.Errorf("%w: unmarshal change record: %v", ErrCorruptExport, err)
			}
			ctag := storepkg.ChangeTag(crw.Tag)
			if !ctag.Valid() {
				return fmt.Errorf("%w: invalid change tag %d", ErrCorruptExport, crw.Tag)
			}
			rec := storepkg.ChangeRecord{LSN: crw.LSN, Tag: ctag, Payload: crw.Payload}
			if err := c.applyMergeChangeRecord(mrb, rec, touchedNodes, touchedRels, opts); err != nil {
				return err
			}

		default:
			return fmt.Errorf("%w: unknown record tag 0x%02x", ErrCorruptExport, tag)
		}
	}

	if !seenHeader {
		return fmt.Errorf("%w: missing header record", ErrCorruptExport)
	}
	if !seenRegistry {
		return fmt.Errorf("%w: missing registry record", ErrCorruptExport)
	}

	// Final trust-boundary pass: every touched entity's hash chain must verify.
	// A hard-deleted entity (fully absent) has no chain — that is the correct
	// merged outcome, not corruption.
	for id := range touchedNodes {
		valid, err := c.verifyNodeChainLocked(id)
		if err != nil {
			if errors.Is(err, storepkg.ErrNodeNotFound) {
				continue
			}
			return fmt.Errorf("import merge: verify node chain %d: %w", id.SnowflakeID(), err)
		}
		if !valid {
			return fmt.Errorf("import merge: node %d: %w: merged hash chain does not verify", id.SnowflakeID(), ErrCorruptExport)
		}
	}
	for id := range touchedRels {
		valid, err := c.verifyRelChainLocked(id)
		if err != nil {
			if errors.Is(err, storepkg.ErrRelNotFound) {
				continue
			}
			return fmt.Errorf("import merge: verify rel chain %d: %w", id.SnowflakeID(), err)
		}
		if !valid {
			return fmt.Errorf("import merge: rel %d: %w: merged hash chain does not verify", id.SnowflakeID(), ErrCorruptExport)
		}
	}
	return nil
}

// applyMergeChangeRecord captures the touched entities for rollback, runs the
// optional Strict base check, then applies the record verbatim.
func (c *Core) applyMergeChangeRecord(mrb *mergeRollback, rec storepkg.ChangeRecord, tn map[types.NodeID]struct{}, tr map[types.RelID]struct{}, opts tkgio.MergeOptions) error {
	if err := c.captureMergeRecord(mrb, rec, tn, tr); err != nil {
		return err
	}
	if opts.Strict {
		if err := c.strictCheckMergeRecord(rec); err != nil {
			return err
		}
	}
	// The delta stream is untrusted; applyChangeRecordLocked is the trusted
	// replica-apply door and advances the commit-clock floor unconditionally.
	// Bound the stamp here, before it reaches the clock.
	if err := c.checkImportedRecordStamp(rec); err != nil {
		return err
	}
	if err := c.applyChangeRecordLocked(rec); err != nil {
		return fmt.Errorf("import merge: apply change LSN %d: %w", rec.LSN, err)
	}
	return nil
}

// captureMergeRecord decodes a change record enough to identify the entities it
// touches, snapshots their pre-merge state for rollback, and records the IDs for
// the final chain-verification pass. Decode failures classify as ErrCorruptExport
// (the stream is the untrusted trust boundary).
func (c *Core) captureMergeRecord(mrb *mergeRollback, rec storepkg.ChangeRecord, tn map[types.NodeID]struct{}, tr map[types.RelID]struct{}) error {
	switch rec.Tag {
	case storepkg.ChangeNodePut:
		body, err := storeutil.DecodeNodePut(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node put", err)
		}
		id := types.NodeID(body.Wire.ID)
		tn[id] = struct{}{}
		if err := mrb.captureNode(id); err != nil {
			return err
		}
		// A label-changing put rebuilds the node's label index on apply; capture
		// its adjacency so the purge-and-recreate rollback keeps unchanged edges.
		return mrb.captureNodeAdjacency(id)

	case storepkg.ChangeNodeHistoryVersion:
		body, err := storeutil.DecodeHistoryVersionNode(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node history version", err)
		}
		id := types.NodeID(body.Wire.ID)
		tn[id] = struct{}{}
		if err := mrb.captureNode(id); err != nil {
			return err
		}
		// Rollback purges the node with DeleteNodeCascade (dropping ALL its
		// edges); capture its adjacency so unchanged edges this record did not
		// touch are restored too — otherwise a rolled-back merge silently
		// destroys them. Same invariant as ChangeNodePut/ChangeNodeDelete.
		return mrb.captureNodeAdjacency(id)

	case storepkg.ChangeRelPut:
		body, err := storeutil.DecodeRelPut(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel put", err)
		}
		id := types.RelID(body.Wire.ID)
		tr[id] = struct{}{}
		return mrb.captureRel(id)

	case storepkg.ChangeRelHistoryVersion:
		body, err := storeutil.DecodeHistoryVersionRel(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel history version", err)
		}
		id := types.RelID(body.Wire.ID)
		tr[id] = struct{}{}
		return mrb.captureRel(id)

	case storepkg.ChangeNodeDelete:
		body, err := storeutil.DecodeNodeDelete(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node delete", err)
		}
		id := types.NodeID(body.ID)
		tn[id] = struct{}{}
		if err := mrb.captureNode(id); err != nil {
			return err
		}
		if err := mrb.captureNodeAdjacency(id); err != nil {
			return err
		}
		for _, rid := range body.CascadedRelIDs {
			rl := types.RelID(rid)
			tr[rl] = struct{}{}
			if err := mrb.captureRel(rl); err != nil {
				return err
			}
		}
		return nil

	case storepkg.ChangeRelDelete:
		body, err := storeutil.DecodeRelDelete(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel delete", err)
		}
		id := types.RelID(body.ID)
		tr[id] = struct{}{}
		return mrb.captureRel(id)

	case storepkg.ChangeNodeHistoryTruncate:
		body, err := storeutil.DecodeHistoryTruncate(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node history truncate", err)
		}
		id := types.NodeID(body.ID)
		tn[id] = struct{}{}
		if err := mrb.captureNode(id); err != nil {
			return err
		}
		// Same as ChangeNodeHistoryVersion: capture adjacency so the cascade
		// purge in rollback does not drop edges this truncate did not touch.
		return mrb.captureNodeAdjacency(id)

	case storepkg.ChangeRelHistoryTruncate:
		body, err := storeutil.DecodeHistoryTruncate(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel history truncate", err)
		}
		id := types.RelID(body.ID)
		tr[id] = struct{}{}
		return mrb.captureRel(id)

	case storepkg.ChangeForeignIncoming:
		// A cross-machine incoming half-edge stub (ADR-0010 Model A). Its rel-ID
		// deliberately belongs to a FOREIGN slot — the whole point of the stub is
		// that it is written co-located on the END node's shard, "reachable via
		// that shard's adjacency fold, never a slot-routed point read"
		// (ForeignIncomingRelCapability's own doc). mrb.captureRel /
		// verifyRelChainLocked, like every other case in this switch, both do a
		// plain SLOT-ROUTED getCurrentRelationship — which fails closed with
		// ErrSlotNotLocal for exactly this rel-ID, not "not found". So, unlike
		// ChangeRelPut, this case cannot reuse the generic single-entity
		// capture/verify machinery without a bespoke foreign-slot-aware
		// read/restore path this pass does not add. Validate the payload decodes
		// (the untrusted-stream check every case performs) and otherwise capture
		// NOTHING — safe because RecordForeignIncoming is documented idempotent
		// ("an already-present stub is a no-op"), so a merge retried after an
		// unrelated later-record failure simply re-applies this put again.
		if _, err := storeutil.DecodeRelPut(rec.Payload); err != nil {
			return corruptDelta(rec, "foreign-incoming put", err)
		}
		return nil

	case storepkg.ChangeForeignIncomingDelete:
		// Same foreign-slot addressing constraint as ChangeForeignIncoming above.
		// DeleteForeignIncoming is documented idempotent ("an already-absent stub
		// is not an error"), so capturing nothing and letting a merge retry
		// re-apply the delete is safe.
		if _, err := storeutil.DecodeForeignIncomingDelete(rec.Payload); err != nil {
			return corruptDelta(rec, "foreign-incoming delete", err)
		}
		return nil

	case storepkg.ChangeRangePurge:
		// A ChangeRangePurge record carries no per-entity rows — it names a
		// PREDICATE (label + before-instant) the replica re-executes, deriving
		// whatever entity set currently matches (ADR-0008 R3;
		// applyRangePurgeLocked). Unlike every other tag, "capture the touched
		// entities for merge rollback" is neither meaningful (there is no fixed
		// ID set to snapshot ahead of applying) NOR desirable: retention purge
		// exists to durably remove data, so snapshotting the about-to-be-purged
		// rows into mergeRollback's in-memory buffers — even temporarily, even
		// if a later record's failure never actually triggers a restore — would
		// undermine the exact retention/compliance guarantee the primary's
		// purge made. So this case deliberately captures NOTHING: only
		// validate the payload decodes cleanly (the untrusted-stream check
		// every other case performs), and let the purge's effect stand
		// uncaptured. applyRangePurgeLocked is documented idempotent, so a
		// merge retry after an unrelated later-record failure simply
		// re-executes the same predicate — never a correctness problem, and
		// never a reason to hold purged data in memory "just in case".
		if _, err := storeutil.DecodeRangePurge(rec.Payload); err != nil {
			return corruptDelta(rec, "range purge", err)
		}
		return nil

	case storepkg.ChangeClear, storepkg.ChangeMeta:
		return fmt.Errorf("%w: change tag %s is not valid in a delta merge", ErrCorruptExport, rec.Tag)

	default:
		return fmt.Errorf("%w: unknown change tag %d", ErrCorruptExport, byte(rec.Tag))
	}
}

func corruptDelta(rec storepkg.ChangeRecord, what string, err error) error {
	return fmt.Errorf("import merge: decode %s (change LSN %d): %w: %v", what, rec.LSN, ErrCorruptExport, err)
}

// mergeRegistryNamesLocked reconciles a delta's registry names with the base's.
// The two are append-only, so one must be a prefix of the other: their common
// prefix must match, the base extends when the delta is longer, and a delta whose
// registry is shorter (already covered by a newer delta) is a no-op. A prefix
// mismatch is ErrIncompatibleRegistry (the delta is from an incompatible lineage).
func (c *Core) mergeRegistryNamesLocked(reg appendNamer, want []string, kind string) error {
	current := reg.ExportNames()
	n := len(current)
	if len(want) < n {
		n = len(want)
	}
	for i := 0; i < n; i++ {
		if current[i] != want[i] {
			return fmt.Errorf("import merge: %s registry diverged at token %d: %w", kind, i, ErrIncompatibleRegistry)
		}
	}
	if len(want) <= len(current) {
		return nil // base already holds every name in the delta's registry
	}
	suffix := want[len(current):]
	ok, err := reg.AppendNames(current, suffix)
	if err != nil {
		return fmt.Errorf("import merge: append %s registry: %w", kind, err)
	}
	if !ok {
		return fmt.Errorf("import merge: %s registry changed during append: %w", kind, ErrIncompatibleRegistry)
	}
	return nil
}

// strictCheckMergeRecord rejects an update or tombstone for an entity absent from
// the base — the signal that a delta is being applied onto the wrong base.
//
// ChangeForeignIncoming / ChangeForeignIncomingDelete (ADR-0010 Model A stubs) are
// deliberately left unchecked: their rel-IDs belong to a FOREIGN slot, so a plain
// slot-routed GetRelationship would fail with ErrSlotNotLocal (not ErrRelNotFound),
// making an absence probe meaningless without the bespoke END-node-slot-aware read
// path — and both are structurally a create-or-idempotent-delete with no "a prior
// version must already exist" signal to check in the first place (see
// captureMergeRecord's own doc comment on this record shape). ChangeRangePurge
// carries a PREDICATE, not a fixed entity set, so there is no per-entity absence
// signal to probe at all.
func (c *Core) strictCheckMergeRecord(rec storepkg.ChangeRecord) error {
	switch rec.Tag {
	case storepkg.ChangeNodePut:
		body, err := storeutil.DecodeNodePut(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node put", err)
		}
		if body.WithHistory {
			if _, gerr := c.store.GetNode(types.NodeID(body.Wire.ID)); errors.Is(gerr, storepkg.ErrNodeNotFound) {
				return fmt.Errorf("%w: with-history node put %d but node absent in base (Strict)", ErrDeltaBaseMismatch, body.Wire.ID)
			}
		}
	case storepkg.ChangeNodeDelete:
		body, err := storeutil.DecodeNodeDelete(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node delete", err)
		}
		if _, gerr := c.store.GetNode(types.NodeID(body.ID)); errors.Is(gerr, storepkg.ErrNodeNotFound) {
			return fmt.Errorf("%w: node delete %d but node absent in base (Strict)", ErrDeltaBaseMismatch, body.ID)
		}
	case storepkg.ChangeRelPut:
		body, err := storeutil.DecodeRelPut(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel put", err)
		}
		if body.WithHistory {
			if _, gerr := c.store.GetRelationship(types.RelID(body.Wire.ID)); errors.Is(gerr, storepkg.ErrRelNotFound) {
				return fmt.Errorf("%w: with-history rel put %d but rel absent in base (Strict)", ErrDeltaBaseMismatch, body.Wire.ID)
			}
		}
	case storepkg.ChangeRelDelete:
		body, err := storeutil.DecodeRelDelete(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel delete", err)
		}
		if _, gerr := c.store.GetRelationship(types.RelID(body.ID)); errors.Is(gerr, storepkg.ErrRelNotFound) {
			return fmt.Errorf("%w: rel delete %d but rel absent in base (Strict)", ErrDeltaBaseMismatch, body.ID)
		}

	case storepkg.ChangeNodeHistoryVersion:
		body, err := storeutil.DecodeHistoryVersionNode(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node history version", err)
		}
		// Gated on Version > 0 (conservative): a genesis (Version==0) history-version
		// record is structurally unexpected on any door in this codebase, so this
		// gate never suppresses a real check in practice — it only avoids a
		// hypothetical false positive if such a record ever appeared.
		if body.Version > 0 {
			id := types.NodeID(body.Wire.ID)
			known, kerr := c.baseKnowsNode(id)
			if kerr != nil {
				return kerr
			}
			if !known {
				return fmt.Errorf("%w: node history version %d but node absent in base (Strict)", ErrDeltaBaseMismatch, body.Wire.ID)
			}
		}
	case storepkg.ChangeRelHistoryVersion:
		body, err := storeutil.DecodeHistoryVersionRel(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel history version", err)
		}
		if body.Version > 0 {
			id := types.RelID(body.Wire.ID)
			known, kerr := c.baseKnowsRel(id)
			if kerr != nil {
				return kerr
			}
			if !known {
				return fmt.Errorf("%w: rel history version %d but rel absent in base (Strict)", ErrDeltaBaseMismatch, body.Wire.ID)
			}
		}
	case storepkg.ChangeNodeHistoryTruncate:
		body, err := storeutil.DecodeHistoryTruncate(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "node history truncate", err)
		}
		id := types.NodeID(body.ID)
		known, kerr := c.baseKnowsNode(id)
		if kerr != nil {
			return kerr
		}
		if !known {
			return fmt.Errorf("%w: node history truncate %d but node absent in base (Strict)", ErrDeltaBaseMismatch, body.ID)
		}
	case storepkg.ChangeRelHistoryTruncate:
		body, err := storeutil.DecodeHistoryTruncate(rec.Payload)
		if err != nil {
			return corruptDelta(rec, "rel history truncate", err)
		}
		id := types.RelID(body.ID)
		known, kerr := c.baseKnowsRel(id)
		if kerr != nil {
			return kerr
		}
		if !known {
			return fmt.Errorf("%w: rel history truncate %d but rel absent in base (Strict)", ErrDeltaBaseMismatch, body.ID)
		}
	}
	return nil
}

// baseKnowsNode reports whether the merge base has ANY record of id — a current
// row, any history version, or a lingering compaction stub. Total ignorance of an
// id a delta record references is the wrong-base signal (records replay in strict
// LSN order onto a bootstrap that includes history-only entities, so a correct
// base can never be ignorant of a legitimately-referenced id — see BACKLOG 12i
// design writeup for the full proof). This is a Strict-mode-only probe.
func (c *Core) baseKnowsNode(id types.NodeID) (bool, error) {
	if _, err := c.store.GetNode(id); err == nil {
		return true, nil
	} else if !errors.Is(err, storepkg.ErrNodeNotFound) {
		return false, err
	}
	hist, err := c.store.GetNodeHistory(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return false, err
	}
	if len(hist) > 0 {
		return true, nil
	}
	_, ok, err := c.loadNodeCompactionStub(id)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// baseKnowsRel is the relationship mirror of baseKnowsNode.
func (c *Core) baseKnowsRel(id types.RelID) (bool, error) {
	if _, err := c.store.GetRelationship(id); err == nil {
		return true, nil
	} else if !errors.Is(err, storepkg.ErrRelNotFound) {
		return false, err
	}
	hist, err := c.store.GetRelHistory(id)
	if err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
		return false, err
	}
	if len(hist) > 0 {
		return true, nil
	}
	_, ok, err := c.loadRelCompactionStub(id)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// =============================================================================
// mergeRollback — purge-and-recreate the touched subgraph from pre-merge snapshots
// =============================================================================
//
// A delta merge applies an arbitrary mix of put / replace / replace-with-history
// / label-token / history-version / cascade-delete doors. Rather than invert each
// surgically (label-index changes and cascades make that error-prone), rollback
// PURGES every touched entity (DeleteRelationship then DeleteNodeCascade, which
// clears all indexes and adjacency) and RE-CREATES it from its captured snapshot
// through the CREATE doors (PutNode / PutNodeVersion / PutRelationship), which
// rebuild every index correctly. Nodes are recreated before relationships so a
// restored relationship always finds its endpoints. Because purging a node
// removes its adjacency, capture records every touched node's full adjacency —
// even edges the delta did not change — so they are restored too.

type mergeNodeSnap struct {
	current *types.Node
	history []*types.Node
	existed bool
}

type mergeRelSnap struct {
	current *types.Relationship
	history []*types.Relationship
	existed bool
}

type mergeRollback struct {
	c         *Core
	labels    []string
	relTypes  []string
	nodes     map[types.NodeID]mergeNodeSnap
	nodeOrder []types.NodeID
	rels      map[types.RelID]mergeRelSnap
	relOrder  []types.RelID
}

func newMergeRollback(c *Core) *mergeRollback {
	return &mergeRollback{
		c:        c,
		labels:   c.labels.ExportNames(),
		relTypes: c.relTypes.ExportNames(),
		nodes:    make(map[types.NodeID]mergeNodeSnap),
		rels:     make(map[types.RelID]mergeRelSnap),
	}
}

func (mrb *mergeRollback) captureNode(id types.NodeID) error {
	if _, ok := mrb.nodes[id]; ok {
		return nil
	}
	c := mrb.c
	var snap mergeNodeSnap
	cur, err := c.getCurrentNode(id)
	switch {
	case err == nil:
		snap.current = cur.DeepCopy()
		snap.existed = true
	case errors.Is(err, storepkg.ErrNodeNotFound):
	default:
		return err
	}
	hist, herr := c.copyNodeHistory(id)
	if herr != nil && !errors.Is(herr, storepkg.ErrNodeNotFound) {
		return herr
	}
	if len(hist) > 0 {
		snap.history = hist
		snap.existed = true
	}
	mrb.nodes[id] = snap
	mrb.nodeOrder = append(mrb.nodeOrder, id)
	return nil
}

func (mrb *mergeRollback) captureRel(id types.RelID) error {
	if _, ok := mrb.rels[id]; ok {
		return nil
	}
	c := mrb.c
	var snap mergeRelSnap
	cur, err := c.getCurrentRelationship(id)
	switch {
	case err == nil:
		snap.current = cur.DeepCopy()
		snap.existed = true
	case errors.Is(err, storepkg.ErrRelNotFound):
	default:
		return err
	}
	hist, herr := c.copyRelHistory(id)
	if herr != nil && !errors.Is(herr, storepkg.ErrRelNotFound) {
		return herr
	}
	if len(hist) > 0 {
		snap.history = hist
		snap.existed = true
	}
	mrb.rels[id] = snap
	mrb.relOrder = append(mrb.relOrder, id)
	return nil
}

// captureNodeAdjacency captures every relationship currently connected to id
// (any type, both directions) so a purge-and-recreate rollback of id does not
// silently drop an edge the delta itself did not touch.
func (mrb *mergeRollback) captureNodeAdjacency(id types.NodeID) error {
	c := mrb.c
	out, err := c.store.OutgoingRelationships(id, 0)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return err
	}
	for _, r := range out {
		if err := mrb.captureRel(r.ID()); err != nil {
			return err
		}
	}
	in, err := c.store.IncomingRelationships(id, 0)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return err
	}
	for _, r := range in {
		if err := mrb.captureRel(r.ID()); err != nil {
			return err
		}
	}
	return nil
}

func (mrb *mergeRollback) rollback() error {
	c := mrb.c
	var firstErr error
	grab := func(e error) {
		if e != nil && firstErr == nil {
			firstErr = e
		}
	}

	// 1. Delete every touched relationship — current row AND version history
	//    (DeleteRelationship clears the current row but not the 0x08 history
	//    keyspace; an interrupted with-history merge may have written rel history).
	for _, id := range mrb.relOrder {
		if err := c.store.DeleteRelationship(id); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
			grab(err)
		}
		if err := c.store.TruncateRelHistory(id, 0); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
			grab(err)
		}
	}
	// 2. Purge every touched node — current + indexes + adjacency (cascade) AND
	//    version history. DeleteNodeCascade does NOT clear the 0x07 history
	//    keyspace, so a rolled-back update/label/delete would otherwise leave a
	//    stale history row behind; truncate it so the re-create from snapshot is
	//    exact. Cascade is safe: the node's edges were deleted in step 1.
	for _, id := range mrb.nodeOrder {
		if err := c.store.DeleteNodeCascade(id); err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
			grab(err)
		}
		if err := c.store.TruncateNodeHistory(id, 0); err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
			grab(err)
		}
	}
	// 3. Re-create pre-existing nodes from snapshot (CREATE doors rebuild indexes).
	for _, id := range mrb.nodeOrder {
		snap := mrb.nodes[id]
		if !snap.existed {
			continue
		}
		grab(restoreMergeNode(c, id, snap))
	}
	// 4. Re-create pre-existing relationships (endpoints exist from step 3).
	for _, id := range mrb.relOrder {
		snap := mrb.rels[id]
		if !snap.existed {
			continue
		}
		grab(restoreMergeRel(c, id, snap))
	}
	grab(mrb.restoreRegistries())
	return firstErr
}

func restoreMergeNode(c *Core, id types.NodeID, snap mergeNodeSnap) error {
	for _, h := range snap.history {
		if err := c.store.PutNodeVersion(id, h.Version(), h); err != nil {
			return err
		}
	}
	if snap.current != nil {
		if err := c.store.PutNode(snap.current); err != nil {
			return err
		}
	}
	return nil
}

func restoreMergeRel(c *Core, id types.RelID, snap mergeRelSnap) error {
	for _, h := range snap.history {
		if err := c.store.PutRelVersion(id, h.Version(), h); err != nil {
			return err
		}
	}
	if snap.current != nil {
		if err := c.store.PutRelationship(snap.current); err != nil {
			return err
		}
	}
	return nil
}

func (mrb *mergeRollback) restoreRegistries() error {
	labels := registrypkg.NewLabelRegistry()
	if err := labels.ImportNames(mrb.labels); err != nil {
		return err
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if err := relTypes.ImportNames(mrb.relTypes); err != nil {
		return err
	}
	// Swap + persist under registryMu in addition to the exclusive c.mu the
	// import path holds — synchronizes with registryMu-only readers (Close's
	// persist, declare-on-prepare); see GraphTx.restoreRegistries.
	mrb.c.registryMu.Lock()
	defer mrb.c.registryMu.Unlock()
	mrb.c.labels = labels
	mrb.c.relTypes = relTypes
	mrb.c.clearRelTypeCache()
	setTieredLabelRegistryIfSupported(mrb.c.store, mrb.c.labels)
	return mrb.c.persistRegistries()
}
