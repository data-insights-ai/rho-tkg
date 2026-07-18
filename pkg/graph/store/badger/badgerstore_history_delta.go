package badger

import (
	"errors"
	"fmt"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// ADR-0009 — badger anchor+delta version-history storage.
//
// WRITE: historyNodeValue / historyRelValue turn a version's full state into the
// on-disk history value — a full anchor when delta encoding is off, the version
// is an anchor (V%HistoryAnchorInterval==0), or the anchor is unavailable / a
// delta would not be smaller; otherwise a delta against the interval anchor.
//
// READ: reconstructNodeHistoryWire / reconstructRelHistoryWire turn a raw history
// value back into a full wire, point-reading the interval anchor when the value
// is a delta. Reads accept BOTH forms regardless of the flag, so the flag is
// safe to toggle on an existing store.

// ---- raw single-row reads (pending-overlay + badger aware) ----

// readHistoryNodeRaw returns the raw stored bytes for one node history version,
// consulting the write buffer (pending + flushing) before badger. A tombstoned
// or absent version returns ErrVersionNotFound.
func (bs *Store) readHistoryNodeRaw(id snowflake.ID, version uint64) ([]byte, error) {
	return bs.readHistoryRaw(storepkg.HistNodeKey(id, version))
}

func (bs *Store) readHistoryRelRaw(id snowflake.ID, version uint64) ([]byte, error) {
	return bs.readHistoryRaw(storepkg.HistRelKey(id, version))
}

func (bs *Store) readHistoryRaw(key []byte) ([]byte, error) {
	if op, found := bs.lookupPending(string(key)); found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		out := make([]byte, len(op.value))
		copy(out, op.value)
		return out, nil
	}
	var out []byte
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err == badgerv4.ErrKeyNotFound {
			return ErrVersionNotFound
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			out = make([]byte, len(val))
			copy(out, val)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---- write-side: full-or-delta value construction ----

// historyNodeValue builds the on-disk history value for (version, state).
func (bs *Store) historyNodeValue(id snowflake.ID, version uint64, state *types.Node) ([]byte, error) {
	reg := bs.propKeyReg.Load()
	targetWire, err := storepkg.NodeToWireCheckedWithKeys(state, reg)
	if err != nil {
		return nil, err
	}
	fullBytes, err := storepkg.MarshalNodeWireStruct(targetWire)
	if err != nil {
		return nil, err
	}
	if !bs.historyDelta || storepkg.IsAnchorVersion(version, bs.historyAnchorInterval) {
		return fullBytes, nil
	}
	anchorRaw, err := bs.readHistoryNodeRaw(id, storepkg.AnchorVersionFor(version, bs.historyAnchorInterval))
	if err != nil || storepkg.HistoryValueKindOf(anchorRaw) != storepkg.HistoryFull {
		return fullBytes, nil // anchor missing or itself a delta → safe full fallback
	}
	var anchorWire storepkg.NodeWire
	if err := storepkg.SafeUnmarshal(anchorRaw, &anchorWire); err != nil {
		return fullBytes, nil
	}
	deltaBytes, err := storepkg.EncodeNodeHistoryDelta(storepkg.DiffNodeHistory(anchorWire, targetWire))
	if err != nil || len(deltaBytes) >= len(fullBytes) {
		return fullBytes, nil
	}
	return deltaBytes, nil
}

// historyRelValue mirrors historyNodeValue for relationships.
func (bs *Store) historyRelValue(id snowflake.ID, version uint64, state *types.Relationship) ([]byte, error) {
	reg := bs.propKeyReg.Load()
	targetWire, err := storepkg.RelToWireCheckedWithKeys(state, reg)
	if err != nil {
		return nil, err
	}
	fullBytes, err := storepkg.MarshalRelWireStruct(targetWire)
	if err != nil {
		return nil, err
	}
	if !bs.historyDelta || storepkg.IsAnchorVersion(version, bs.historyAnchorInterval) {
		return fullBytes, nil
	}
	anchorRaw, err := bs.readHistoryRelRaw(id, storepkg.AnchorVersionFor(version, bs.historyAnchorInterval))
	if err != nil || storepkg.HistoryValueKindOf(anchorRaw) != storepkg.HistoryFull {
		return fullBytes, nil
	}
	var anchorWire storepkg.RelWire
	if err := storepkg.SafeUnmarshal(anchorRaw, &anchorWire); err != nil {
		return fullBytes, nil
	}
	deltaBytes, err := storepkg.EncodeRelHistoryDelta(storepkg.DiffRelHistory(anchorWire, targetWire))
	if err != nil || len(deltaBytes) >= len(fullBytes) {
		return fullBytes, nil
	}
	return deltaBytes, nil
}

// ---- read-side: reconstruction to a full wire ----

// historyRawRow is a collected (version, raw-bytes) pair, used by streaming read
// paths that gather rows inside a badger View and reconstruct after it closes.
type historyRawRow struct {
	version uint64
	raw     []byte
}

// localAnchorFunc optionally supplies an interval anchor's raw bytes from an
// in-scan cache (e.g. a whole-chain scan that already holds every row), avoiding
// a point read. It returns (bytes, true) on a hit.
type localAnchorFunc func(anchorVersion uint64) ([]byte, bool)

// reconstructNodeHistoryWire turns a raw history value into a full NodeWire,
// applying the interval anchor when raw is a delta.
func (bs *Store) reconstructNodeHistoryWire(id snowflake.ID, version uint64, raw []byte, local localAnchorFunc) (storepkg.NodeWire, error) {
	if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryFull {
		var w storepkg.NodeWire
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return storepkg.NodeWire{}, err
		}
		return w, nil
	}
	d, err := storepkg.DecodeNodeHistoryDelta(raw)
	if err != nil {
		return storepkg.NodeWire{}, err
	}
	anchorVer := storepkg.AnchorVersionFor(version, bs.historyAnchorInterval)
	anchorRaw, err := bs.fetchAnchorRaw(anchorVer, local, func() ([]byte, error) { return bs.readHistoryNodeRaw(id, anchorVer) })
	if err != nil {
		return storepkg.NodeWire{}, fmt.Errorf("graph: read node history anchor v%d: %w", anchorVer, err)
	}
	if storepkg.HistoryValueKindOf(anchorRaw) != storepkg.HistoryFull {
		return storepkg.NodeWire{}, fmt.Errorf("%w: node history anchor v%d is not a full snapshot", storecontract.ErrCorruptWire, anchorVer)
	}
	var anchorWire storepkg.NodeWire
	if err := storepkg.SafeUnmarshal(anchorRaw, &anchorWire); err != nil {
		return storepkg.NodeWire{}, err
	}
	w := storepkg.ApplyNodeHistory(anchorWire, d)
	// ApplyNodeHistory merges properties in token-identity order; the decoder
	// requires key-string order. Resolve keys (registry available) and re-sort.
	if err := bs.resolveNodeWireKeys(&w); err != nil {
		return storepkg.NodeWire{}, fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	storepkg.SortWirePropertiesByKey(w.Properties)
	return w, nil
}

// reconstructRelHistoryWire mirrors reconstructNodeHistoryWire.
func (bs *Store) reconstructRelHistoryWire(id snowflake.ID, version uint64, raw []byte, local localAnchorFunc) (storepkg.RelWire, error) {
	if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryFull {
		var w storepkg.RelWire
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return storepkg.RelWire{}, err
		}
		return w, nil
	}
	d, err := storepkg.DecodeRelHistoryDelta(raw)
	if err != nil {
		return storepkg.RelWire{}, err
	}
	anchorVer := storepkg.AnchorVersionFor(version, bs.historyAnchorInterval)
	anchorRaw, err := bs.fetchAnchorRaw(anchorVer, local, func() ([]byte, error) { return bs.readHistoryRelRaw(id, anchorVer) })
	if err != nil {
		return storepkg.RelWire{}, fmt.Errorf("graph: read rel history anchor v%d: %w", anchorVer, err)
	}
	if storepkg.HistoryValueKindOf(anchorRaw) != storepkg.HistoryFull {
		return storepkg.RelWire{}, fmt.Errorf("%w: rel history anchor v%d is not a full snapshot", storecontract.ErrCorruptWire, anchorVer)
	}
	var anchorWire storepkg.RelWire
	if err := storepkg.SafeUnmarshal(anchorRaw, &anchorWire); err != nil {
		return storepkg.RelWire{}, err
	}
	w := storepkg.ApplyRelHistory(anchorWire, d)
	if err := bs.resolveRelWireKeys(&w); err != nil {
		return storepkg.RelWire{}, fmt.Errorf("%w: %w", ErrInvalidStoreMutation, err)
	}
	storepkg.SortWirePropertiesByKey(w.Properties)
	return w, nil
}

// historyNodeTemporal decodes only enough of a node history value to read its
// temporal metadata (for as-of classification): a delta carries the full
// temporal block verbatim in its Meta, so no anchor read is needed. Used by the
// reverse as-of scan, which then fully reconstructs only the winning version.
func (bs *Store) historyNodeTemporal(id snowflake.ID, version uint64, raw []byte) (*types.Node, error) {
	var w storepkg.NodeWire
	if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryFull {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
	} else {
		d, err := storepkg.DecodeNodeHistoryDelta(raw)
		if err != nil {
			return nil, err
		}
		w = d.Meta
	}
	return bs.decodeNodeHistoryWireForKey(w, id, version)
}

// historyRelTemporal mirrors historyNodeTemporal for relationships.
func (bs *Store) historyRelTemporal(id snowflake.ID, version uint64, raw []byte) (*types.Relationship, error) {
	var w storepkg.RelWire
	if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryFull {
		if err := storepkg.SafeUnmarshal(raw, &w); err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
	} else {
		d, err := storepkg.DecodeRelHistoryDelta(raw)
		if err != nil {
			return nil, err
		}
		w = d.Meta
	}
	return bs.decodeRelHistoryWireForKey(w, id, version)
}

// fetchAnchorRaw prefers the in-scan cache, else the point-read.
func (bs *Store) fetchAnchorRaw(anchorVer uint64, local localAnchorFunc, point func() ([]byte, error)) ([]byte, error) {
	if local != nil {
		if b, ok := local(anchorVer); ok {
			return b, nil
		}
	}
	return point()
}

// rematerializeOrphanedDeltas returns Set writeOps that rewrite, as full
// snapshots, any KEPT history delta whose interval anchor is NOT itself kept — a
// truncation that keeps the newest N versions can drop an anchor while keeping
// deltas in its interval, orphaning them. A delta whose anchor survives keeps its
// (compact) form. Dispatches node/rel on the prefix. Reconstruction reads the
// (still-present) anchor before the deletes commit in the same batch. No-op when
// delta encoding is off (no deltas exist) or nothing is orphaned.
func (bs *Store) rematerializeOrphanedDeltas(prefix []byte, keptKeys []string) ([]writeOp, error) {
	if !bs.historyDelta || len(keptKeys) == 0 {
		return nil, nil
	}
	isNode := prefix[0] == storepkg.KeyHistNode
	id := storepkg.ParseIDFromKey(prefix, 1)
	keptVersions := make(map[uint64]struct{}, len(keptKeys))
	for _, k := range keptKeys {
		keptVersions[historyVersionFromKey([]byte(k))] = struct{}{}
	}
	var ops []writeOp
	for _, k := range keptKeys {
		raw, err := bs.readHistoryRaw([]byte(k))
		if err != nil {
			if errors.Is(err, ErrVersionNotFound) {
				continue
			}
			return nil, err
		}
		if storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
			continue // already a full snapshot
		}
		version := historyVersionFromKey([]byte(k))
		if _, anchorKept := keptVersions[storepkg.AnchorVersionFor(version, bs.historyAnchorInterval)]; anchorKept {
			continue // anchor survives — the delta stays reconstructable
		}
		full, err := bs.materializeFullHistoryValue(isNode, id, version, raw)
		if err != nil {
			return nil, err
		}
		ops = append(ops, writeOp{opType: writeOpSet, key: []byte(k), value: full})
	}
	return ops, nil
}

// materializeFullHistoryValue reconstructs a delta value and re-marshals it as a
// canonical full snapshot (property keys re-tokenized), for node or rel.
// reconstruct* returns resolved (Key-set) properties in key order; ApplyProperty
// KeyTokens converts them back to the tokenized wire form the store stores.
func (bs *Store) materializeFullHistoryValue(isNode bool, id snowflake.ID, version uint64, raw []byte) ([]byte, error) {
	reg := bs.propKeyReg.Load()
	if isNode {
		w, err := bs.reconstructNodeHistoryWire(id, version, raw, nil)
		if err != nil {
			return nil, err
		}
		storepkg.ApplyPropertyKeyTokens(w.Properties, reg)
		return storepkg.MarshalNodeWireStruct(w)
	}
	w, err := bs.reconstructRelHistoryWire(id, version, raw, nil)
	if err != nil {
		return nil, err
	}
	storepkg.ApplyPropertyKeyTokens(w.Properties, reg)
	return storepkg.MarshalRelWireStruct(w)
}

// resolveHistoryAnchorInterval maps the config value (0 = default) to the effective
// anchor interval. Validation of the range happens at New via
// validateHistoryAnchorInterval; this resolve is total (a validated 0 → default).
func resolveHistoryAnchorInterval(configured int) uint64 {
	if configured <= 0 {
		return storepkg.DefaultHistoryAnchorInterval
	}
	return uint64(configured) // #nosec G115 — validated in [2,4096] at New
}
