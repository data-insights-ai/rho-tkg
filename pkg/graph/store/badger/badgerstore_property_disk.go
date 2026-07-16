// Package badgerstore provides Store — the persistent Store
// implementation backed by Badger v4. Used as a backend by pkg/graph
// directly and as a shard implementation inside internal/tieredstore.
package badger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Disk-resident property-value index (opt-in via Config.PropertyIndexOnDisk).
//
// Mirrors the LabelIndexOnDisk / AdjacencyIndexOnDisk pattern (see
// badgerstore_label_disk.go): persisted keyspace + prefix/range iteration +
// a pending-write overlay that resolves set-vs-delete PER KEY (lesson 57),
// not a running aggregate. The one structural difference: the label and
// adjacency keyspaces have ALWAYS been written transactionally (since the
// format's inception), so enabling their on-disk mode needs no migration —
// but the 0x0A property-index keyspace is NEW as of this feature, so an
// existing directory that already has property-index DEFINITIONS (from
// CreatePropertyIndex) needs an explicit one-time backfill the first time
// PropertyIndexOnDisk is turned on. See commitPropertyIndexOnDiskBackfill
// (called from loadIndexesScan in badgerstore.go), guarded by
// storeutil.PropertyIndexOnDiskBuiltKey so it runs exactly once.
//
// On-disk key format and value-byte encoding: see
// internal/storeutil/property_index_key.go.
//
// Key design point the WRITE-PATH maintenance functions below all rely on:
// the on-disk key does NOT include the label token (unlike PropertyIndexKey,
// which is scoped to (LabelToken, PropertyKey) in RAM). A property key
// shared by two active index definitions on DIFFERENT labels therefore
// shares ONE physical row per (node, value) — written once, read by both
// definitions. Every reader (equality and range) already re-fetches the
// candidate node and re-checks HasLabelTokenRaw before trusting a match
// (see badgerstore_node_query.go / badgerstore_node_range_scan.go — this is
// the SAME "over-select then recheck" contract those callers already use
// against the RAM-mode indexed path, not a new relaxation introduced here),
// so omitting the label from the physical key is safe: a false-positive
// candidate (row exists because some OTHER label's definition needed it) is
// filtered out by the recheck exactly like an orphaned/stale index entry
// would be. DropPropertyIndex reference-counts by PropertyKey (not the full
// (label,key) pair) before physically purging rows, so dropping one
// definition never corrupts a sibling definition still using the same
// property key under a different label.

// propKeyTokenFor resolves propertyKey to its registry token for the on-disk
// key. ok=false when no property-key registry is wired (badger.Config.
// PropertyKeyRegistry unset and no meta-persisted registry found) or the
// registry is full (GetOrCreate returns the reserved token 0) — callers
// degrade to a no-op in that case, the same graceful-fallback shape
// storeutil.ApplyPropertyKeyTokens already uses for wire encoding. A graph
// opened via pkg/graph always wires a registry before any door reachable by
// a caller runs, so this only bites a direct badger.Store user who enabled
// PropertyIndexOnDisk without ever supplying/growing a registry.
func (bs *Store) propKeyTokenFor(propertyKey string) (uint16, bool) {
	reg := bs.propKeyReg.Load()
	if reg == nil {
		return 0, false
	}
	tok, err := reg.GetOrCreate(propertyKey)
	if err != nil || tok == 0 {
		return 0, false
	}
	return tok, true
}

// propertyIndexDiskOp builds the writeOp for one (propertyKey, valueKey,
// nodeID) entry. ok=false when the value isn't indexable or no token could
// be resolved — callers simply skip the entry, mirroring AddKey/removeKey's
// own `if vk == "" { return }` no-op guard.
func (bs *Store) propertyIndexDiskOp(propertyKey, valueKey string, id snowflake.ID, opType writeOpType) (writeOp, bool) {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return writeOp{}, false
	}
	payload, ok := storepkg.PropertyIndexValueBytes(valueKey)
	if !ok {
		return writeOp{}, false
	}
	key := storepkg.PropertyIndexEntryKey(tok, payload, id)
	if opType == writeOpDelete {
		return writeOp{opType: writeOpDelete, key: key}, true
	}
	return writeOp{opType: writeOpSet, key: key, value: []byte{}}, true
}

// maintainPropertyIndexesAdd is the write-path maintenance entry point every
// node-mutation door calls in place of indexpkg.AddNodeToPropertyIndexes.
// When disk mode is off it delegates unchanged (RAM Entries/numBuckets
// maintenance, zero behavior change). When on, it iterates n's labels exactly
// like the RAM helper, but for each (label,propertyKey) definition that
// exists, computes a disk writeOp instead of mutating Entries — and marks
// the definition's Mutated set exactly like AddKey does (3-phase
// index-creation safety, see CreatePropertyIndex). Caller must already hold
// idxMu (every call site does) and is responsible for merging the returned
// ops into the SAME appendOps call as the entity row (crash consistency —
// see the call sites in badgerstore_node.go / badgerstore_history_node.go /
// badgerstore_node_batch.go).
//
// Composite property indexes (v1) are ALWAYS RAM-only (no on-disk mode, see
// badgerstore_composite_index.go), so their maintenance runs unconditionally
// here — the ONE seam every node-mutation door already funnels through —
// rather than at each of those door call sites individually.
func (bs *Store) maintainPropertyIndexesAdd(n *types.Node, id snowflake.ID) []writeOp {
	indexpkg.AddNodeToCompositeIndexes(bs.compositeIndexes, bs.compositeIndexesByLabel, n, id)
	if !bs.propIdxOnDisk {
		indexpkg.AddNodeToPropertyIndexes(bs.propertyIndexes, n, id)
		return nil
	}
	return bs.propertyIndexDiskMaintain(n, id, writeOpSet)
}

// maintainPropertyIndexesRemove is the Remove counterpart of
// maintainPropertyIndexesAdd — see its doc comment.
func (bs *Store) maintainPropertyIndexesRemove(n *types.Node, id snowflake.ID) []writeOp {
	indexpkg.RemoveNodeFromCompositeIndexes(bs.compositeIndexes, bs.compositeIndexesByLabel, n, id)
	if !bs.propIdxOnDisk {
		indexpkg.RemoveNodeFromPropertyIndexes(bs.propertyIndexes, n, id)
		return nil
	}
	return bs.propertyIndexDiskMaintain(n, id, writeOpDelete)
}

func (bs *Store) propertyIndexDiskMaintain(n *types.Node, id snowflake.ID, opType writeOpType) []writeOp {
	if len(bs.propertyIndexes) == 0 {
		return nil
	}
	var ops []writeOp
	labelCount := n.LabelTokenCount()
	for i := 0; i < labelCount; i++ {
		labelToken := n.LabelTokenRawAt(i)
		n.ForEachIndexablePropertyValueKey(func(propertyKey, valueKey string) bool {
			key := indexpkg.PropertyIndexKey{LabelToken: labelToken, PropertyKey: propertyKey}
			idx, exists := bs.propertyIndexes[key]
			if !exists {
				return true
			}
			if op, ok := bs.propertyIndexDiskOp(propertyKey, valueKey, id, opType); ok {
				ops = append(ops, op)
			}
			if idx.Mutated != nil {
				idx.Mutated[id] = struct{}{}
			}
			return true
		})
	}
	return ops
}

// maintainPropertyIndexesPurge is the corruption-path brute-force fallback
// (node data unavailable, so the value can't be computed) — mirrors
// indexpkg.PurgeNodeFromAllPropertyIndexes. For every DISTINCT property key
// currently referenced by any definition, scans its entire on-disk
// sub-keyspace for a row whose trailing node ID matches id and deletes it.
// O(index size) per distinct key — corruption-only path, same complexity
// class as the RAM-mode brute-force sweep it mirrors. Caller holds idxMu.
//
// Composite indexes are always RAM-only (v1), so their purge runs
// unconditionally here too.
func (bs *Store) maintainPropertyIndexesPurge(id snowflake.ID) []writeOp {
	indexpkg.PurgeNodeFromAllCompositeIndexes(bs.compositeIndexes, id)
	if !bs.propIdxOnDisk {
		indexpkg.PurgeNodeFromAllPropertyIndexes(bs.propertyIndexes, id)
		return nil
	}
	tokens := bs.distinctPropertyKeyTokensLocked()
	if len(tokens) == 0 {
		return nil
	}
	var ops []writeOp
	// Purge any unflushed SET for this ID under a purged token FIRST — before the
	// Badger scan. A concurrent flush() commits a parked index SET to Badger and
	// THEN clears `flushing`, so a scan-first purge that read Badger before the
	// commit and the overlay after `flushing` was cleared would emit NO delete for
	// that key and ORPHAN the just-committed index entry for the deleted node.
	// Capturing the overlay first closes the window; a key seen by both passes
	// yields a duplicate delete op, which coalesces harmlessly (idempotent delete,
	// last-write-wins in the pending map). See lesson 64.
	bs.rangePending(func(k string, op writeOp) {
		kb := []byte(k)
		if len(kb) < 3 || kb[0] != storepkg.KeyPropertyIndex {
			return
		}
		tok := uint16(kb[1])<<8 | uint16(kb[2])
		if _, ok := tokens[tok]; !ok {
			return
		}
		if len(kb) < 8 || op.opType != writeOpSet {
			return
		}
		if storepkg.PropertyIndexNodeIDFromKey(kb) == id {
			ops = append(ops, writeOp{opType: writeOpDelete, key: kb})
		}
	})
	_ = bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		for tok := range tokens {
			prefix := storepkg.PropertyIndexTokenPrefix(tok)
			it := txn.NewIterator(opts)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				key := it.Item().KeyCopy(nil)
				if len(key) >= 8 && storepkg.PropertyIndexNodeIDFromKey(key) == id {
					ops = append(ops, writeOp{opType: writeOpDelete, key: key})
				}
			}
			it.Close()
		}
		return nil
	})
	return ops
}

// distinctPropertyKeyTokensLocked returns the set of propertyKey tokens
// referenced by any CURRENT property-index definition. Caller holds idxMu.
func (bs *Store) distinctPropertyKeyTokensLocked() map[uint16]struct{} {
	seenKeys := make(map[string]struct{})
	tokens := make(map[uint16]struct{})
	for k := range bs.propertyIndexes {
		if _, dup := seenKeys[k.PropertyKey]; dup {
			continue
		}
		seenKeys[k.PropertyKey] = struct{}{}
		if tok, ok := bs.propKeyTokenFor(k.PropertyKey); ok {
			tokens[tok] = struct{}{}
		}
	}
	return tokens
}

// propertyIndexDiskEqualityCandidatesLocked returns the (unsorted) node IDs
// whose on-disk entry for propKeyToken exactly matches payload, merging the
// pending-write overlay (set/delete resolved PER KEY over the persisted
// keyspace — lesson 57). Caller holds idxMu (R or W).
func (bs *Store) propertyIndexDiskEqualityCandidatesLocked(propKeyToken uint16, payload []byte) ([]types.NodeID, error) {
	prefix := storepkg.PropertyIndexValuePrefix(propKeyToken, payload)
	prefixStr := string(prefix)
	wantLen := len(prefixStr) + 8

	var adds, dels map[types.NodeID]struct{}
	bs.rangePending(func(k string, op writeOp) {
		if len(k) != wantLen || !strings.HasPrefix(k, prefixStr) {
			return
		}
		nid := types.NodeID(storepkg.PropertyIndexNodeIDFromKey([]byte(k)))
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[types.NodeID]struct{})
			}
			adds[nid] = struct{}{}
			delete(dels, nid)
		} else {
			if dels == nil {
				dels = make(map[types.NodeID]struct{})
			}
			dels[nid] = struct{}{}
			delete(adds, nid)
		}
	})

	var nids []types.NodeID
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().Key()
			nid := types.NodeID(storepkg.PropertyIndexNodeIDFromKey(key))
			if _, del := dels[nid]; del {
				continue
			}
			delete(adds, nid)
			nids = append(nids, nid)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: property index equality scan: %w", err)
	}
	for nid := range adds {
		nids = append(nids, nid)
	}
	return nids, nil
}

// propertyIndexDiskLookupLocked resolves propertyKey+valueKey to disk
// candidate node IDs. ok=false (nil, nil error) when the property key has no
// token (registry not wired) or the value isn't indexable — matching the
// RAM path's silent-empty contract for a non-indexable/unresolvable value.
func (bs *Store) propertyIndexDiskLookupLocked(propertyKey, valueKey string) ([]types.NodeID, error) {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil, nil
	}
	payload, ok := storepkg.PropertyIndexValueBytes(valueKey)
	if !ok {
		return nil, nil
	}
	return bs.propertyIndexDiskEqualityCandidatesLocked(tok, payload)
}

// propertyIndexDiskRangeCandidatesLocked returns the (unsorted) node IDs
// whose numeric on-disk sort-key lies within the widened [lo,hi] byte
// bounds, merging the pending-write overlay. Over-selects exactly like the
// in-memory ordered view (property_index_range.go) — callers MUST
// post-filter with an exact comparison against the caller's own inclusivity
// flags. Caller holds idxMu.
func (bs *Store) propertyIndexDiskRangeCandidatesLocked(propKeyToken uint16, lo, hi []byte) ([]types.NodeID, error) {
	domainPrefix := storepkg.PropertyIndexNumericDomainPrefix(propKeyToken)
	boundLen := len(lo)

	var adds, dels map[types.NodeID]struct{}
	bs.rangePending(func(k string, op writeOp) {
		kb := []byte(k)
		if len(kb) != storepkg.PropIdxNumericEntryKeyLen || !bytes.HasPrefix(kb, domainPrefix) {
			return
		}
		if bytes.Compare(kb[:boundLen], lo) < 0 || bytes.Compare(kb[:boundLen], hi) > 0 {
			return
		}
		nid := types.NodeID(storepkg.PropertyIndexNodeIDFromKey(kb))
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[types.NodeID]struct{})
			}
			adds[nid] = struct{}{}
			delete(dels, nid)
		} else {
			if dels == nil {
				dels = make(map[types.NodeID]struct{})
			}
			dels[nid] = struct{}{}
			delete(adds, nid)
		}
	})

	var nids []types.NodeID
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(lo); it.ValidForPrefix(domainPrefix); it.Next() {
			key := it.Item().Key()
			if bytes.Compare(key[:boundLen], hi) > 0 {
				break
			}
			nid := types.NodeID(storepkg.PropertyIndexNodeIDFromKey(key))
			if _, del := dels[nid]; del {
				continue
			}
			delete(adds, nid)
			nids = append(nids, nid)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: property index range scan: %w", err)
	}
	for nid := range adds {
		nids = append(nids, nid)
	}
	return nids, nil
}

// diskOrderedEntry pairs a numeric entry's order-preserving sort-key bits with
// its node ID for value-ordered sorting. sortBits is the same
// orderPreservingFloat64Bits encoding stored in the key, so sorting sortBits
// ascending is exactly value-ascending — and same-magnitude entries of
// different numeric subtypes share sortBits, so they interleave purely by node
// ID, matching the RAM ordered view's conflated per-magnitude bucket.
type diskOrderedEntry struct {
	sortBits uint64
	id       snowflake.ID
}

// propertyIndexDiskRangeOrderedLocked resolves propertyKey+[min,max] to disk
// range candidate node IDs in CONTRACTUAL VALUE ORDER (value asc, or desc when
// desc; ties by node ID ASCENDING in both directions), merging the
// pending-write overlay. Over-selects exactly like the in-memory ordered view
// (callers post-filter with exact comparison). supported=false when the
// property key has no token — matching the RAM ordered view's "index exists
// but nothing usable" contract. Caller holds idxMu.
func (bs *Store) propertyIndexDiskRangeOrderedLocked(propertyKey string, min, max float64, desc bool) ([]snowflake.ID, bool, error) {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil, false, nil
	}
	lo, hi := storepkg.PropertyIndexNumericRangeBounds(tok, min, max)
	entries, err := bs.propertyIndexDiskRangeOrderedEntriesLocked(tok, lo, hi)
	if err != nil {
		return nil, false, err
	}
	sortDiskOrderedEntries(entries, desc)
	ids := make([]snowflake.ID, len(entries))
	for i, e := range entries {
		ids[i] = e.id
	}
	return ids, true, nil
}

// sortDiskOrderedEntries orders by (sortBits, nodeID) — ascending, or by
// (sortBits DESC, nodeID ASC) when desc so ties stay node-ID-ascending in both
// scan directions.
func sortDiskOrderedEntries(entries []diskOrderedEntry, desc bool) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].sortBits != entries[j].sortBits {
			if desc {
				return entries[i].sortBits > entries[j].sortBits
			}
			return entries[i].sortBits < entries[j].sortBits
		}
		return entries[i].id < entries[j].id
	})
}

// propertyIndexDiskRangeOrderedEntriesLocked returns the (sortBits, nodeID)
// entries whose numeric on-disk sort-key lies within the widened [lo,hi] byte
// bounds, merging the pending-write overlay (set/delete resolved PER KEY over
// the persisted keyspace — lesson 57). Caller holds idxMu.
func (bs *Store) propertyIndexDiskRangeOrderedEntriesLocked(propKeyToken uint16, lo, hi []byte) ([]diskOrderedEntry, error) {
	domainPrefix := storepkg.PropertyIndexNumericDomainPrefix(propKeyToken)
	boundLen := len(lo)

	// sortBitsFromKey extracts the 8-byte order-preserving sort key that
	// immediately follows the numeric domain prefix.
	sortBitsFromKey := func(kb []byte) uint64 {
		return binary.BigEndian.Uint64(kb[len(domainPrefix) : len(domainPrefix)+8])
	}

	var adds map[snowflake.ID]uint64
	var dels map[snowflake.ID]struct{}
	bs.rangePending(func(k string, op writeOp) {
		kb := []byte(k)
		if len(kb) != storepkg.PropIdxNumericEntryKeyLen || !bytes.HasPrefix(kb, domainPrefix) {
			return
		}
		if bytes.Compare(kb[:boundLen], lo) < 0 || bytes.Compare(kb[:boundLen], hi) > 0 {
			return
		}
		nid := storepkg.PropertyIndexNodeIDFromKey(kb)
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[snowflake.ID]uint64)
			}
			adds[nid] = sortBitsFromKey(kb)
			delete(dels, nid)
		} else {
			if dels == nil {
				dels = make(map[snowflake.ID]struct{})
			}
			dels[nid] = struct{}{}
			delete(adds, nid)
		}
	})

	var entries []diskOrderedEntry
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(lo); it.ValidForPrefix(domainPrefix); it.Next() {
			key := it.Item().Key()
			if bytes.Compare(key[:boundLen], hi) > 0 {
				break
			}
			nid := storepkg.PropertyIndexNodeIDFromKey(key)
			if _, del := dels[nid]; del {
				continue
			}
			delete(adds, nid)
			entries = append(entries, diskOrderedEntry{sortBits: sortBitsFromKey(key), id: nid})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: property index ordered range scan: %w", err)
	}
	for nid, sb := range adds {
		entries = append(entries, diskOrderedEntry{sortBits: sb, id: nid})
	}
	return entries, nil
}

// propertyIndexDiskRangeLocked resolves propertyKey+[min,max] to disk range
// candidates. supported=false (nil, false, nil error) when the property key
// has no token — matching the RAM ordered view's "index exists but nothing
// usable" contract. Caller holds idxMu.
func (bs *Store) propertyIndexDiskRangeLocked(propertyKey string, min, max float64) ([]types.NodeID, bool, error) {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil, false, nil
	}
	lo, hi := storepkg.PropertyIndexNumericRangeBounds(tok, min, max)
	nids, err := bs.propertyIndexDiskRangeCandidatesLocked(tok, lo, hi)
	if err != nil {
		return nil, false, err
	}
	return nids, true, nil
}

// diskStrOrderedEntry pairs a string entry's order-preserving value bytes (the
// domain-tagged payload minus the trailing node ID) with its node ID. Byte
// ordering of val IS lexicographic value ordering: every string entry shares the
// constant [PropIdxDomainRaw]"s:" payload prefix, so comparing the payload bytes
// compares the underlying strings.
type diskStrOrderedEntry struct {
	val string
	id  snowflake.ID
}

func sortDiskStrOrderedEntries(entries []diskStrOrderedEntry, desc bool) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].val != entries[j].val {
			if desc {
				return entries[i].val > entries[j].val
			}
			return entries[i].val < entries[j].val
		}
		return entries[i].id < entries[j].id
	})
}

// propertyIndexDiskPrefixOrderedLocked resolves propertyKey + string prefix to
// disk candidate node IDs in CONTRACTUAL VALUE ORDER (lex value asc, or desc when
// desc; ties by node ID ASCENDING in both directions), merging the pending-write
// overlay. The string view is EXACT (no over-selection). supported=false when the
// property key has no token. An empty prefix matches every string value. Caller
// holds idxMu.
func (bs *Store) propertyIndexDiskPrefixOrderedLocked(propertyKey, prefix string, desc bool) ([]snowflake.ID, bool, error) {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil, false, nil
	}
	// "s:"+prefix -> raw-domain payload [PropIdxDomainRaw]"s:"+prefix; the full key
	// prefix bounds exactly the string entries whose value begins with prefix (the
	// "s:" tag excludes bool/temporal raw-domain entries).
	payload, ok := storepkg.PropertyIndexValueBytes("s:" + prefix)
	if !ok {
		return nil, false, nil
	}
	keyPrefix := storepkg.PropertyIndexValuePrefix(tok, payload)
	entries, err := bs.propertyIndexDiskPrefixEntriesLocked(keyPrefix)
	if err != nil {
		return nil, false, err
	}
	sortDiskStrOrderedEntries(entries, desc)
	ids := make([]snowflake.ID, len(entries))
	for i, e := range entries {
		ids[i] = e.id
	}
	return ids, true, nil
}

// propertyIndexDiskPrefixEntriesLocked returns the (value, nodeID) entries whose
// on-disk key begins with keyPrefix, merging the pending-write overlay (set/delete
// resolved PER KEY over the persisted keyspace — lesson 57). The value is the
// order-preserving payload bytes between the KeyPropertyIndex(1)+token(2) header
// and the trailing 8-byte node ID. Caller holds idxMu.
func (bs *Store) propertyIndexDiskPrefixEntriesLocked(keyPrefix []byte) ([]diskStrOrderedEntry, error) {
	prefixStr := string(keyPrefix)
	minLen := len(keyPrefix) + 8
	valOf := func(kb []byte) string { return string(kb[3 : len(kb)-8]) }

	var adds map[snowflake.ID]string
	var dels map[snowflake.ID]struct{}
	bs.rangePending(func(k string, op writeOp) {
		if len(k) < minLen || !strings.HasPrefix(k, prefixStr) {
			return
		}
		kb := []byte(k)
		nid := storepkg.PropertyIndexNodeIDFromKey(kb)
		if op.opType == writeOpSet {
			if adds == nil {
				adds = make(map[snowflake.ID]string)
			}
			adds[nid] = valOf(kb)
			delete(dels, nid)
		} else {
			if dels == nil {
				dels = make(map[snowflake.ID]struct{})
			}
			dels[nid] = struct{}{}
			delete(adds, nid)
		}
	})

	var entries []diskStrOrderedEntry
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(keyPrefix); it.ValidForPrefix(keyPrefix); it.Next() {
			key := it.Item().Key()
			if len(key) < minLen {
				continue
			}
			nid := storepkg.PropertyIndexNodeIDFromKey(key)
			if _, del := dels[nid]; del {
				continue
			}
			delete(adds, nid)
			entries = append(entries, diskStrOrderedEntry{val: valOf(key), id: nid})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: property index prefix scan: %w", err)
	}
	for nid, v := range adds {
		entries = append(entries, diskStrOrderedEntry{val: v, id: nid})
	}
	return entries, nil
}

// purgePropertyKeyDiskEntriesLocked deletes every on-disk row for
// propertyKey's token — both persisted (via a Badger scan) and any unflushed
// SET still sitting in the write buffer. Called from DropPropertyIndex only
// after confirming no OTHER active definition still references the same
// PropertyKey (rows are physically shared across labels — see the file doc
// comment). Caller holds idxMu.
func (bs *Store) purgePropertyKeyDiskEntriesLocked(propertyKey string) ([]writeOp, error) {
	tok, ok := bs.propKeyTokenFor(propertyKey)
	if !ok {
		return nil, nil
	}
	prefix := storepkg.PropertyIndexTokenPrefix(tok)
	prefixStr := string(prefix)
	var ops []writeOp
	// Snapshot parked SETs for this token BEFORE the Badger scan so a flush that
	// commits a parked index entry and clears `flushing` in the scan->overlay
	// window cannot leave the entry un-deleted (orphan). Duplicate delete ops for
	// a key seen by both passes coalesce harmlessly. See lesson 64.
	bs.rangePending(func(k string, op writeOp) {
		if !strings.HasPrefix(k, prefixStr) {
			return
		}
		if op.opType == writeOpSet {
			keyCopy := make([]byte, len(k))
			copy(keyCopy, k)
			ops = append(ops, writeOp{opType: writeOpDelete, key: keyCopy})
		}
	})
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			ops = append(ops, writeOp{opType: writeOpDelete, key: it.Item().KeyCopy(nil)})
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("graph: drop property index: purge disk entries: %w", err)
	}
	return ops, nil
}

// commitPropertyIndexOnDiskBackfill writes the one-time rebuild-on-enable
// backfill (item (d)) plus the built marker in a single WriteBatch, so a
// crash mid-backfill leaves either NO rows and no marker (retried on the
// next open) or ALL rows and the marker (never a half-built keyspace that a
// later open trusts as complete). ops are always writeOpSet: the only
// caller (loadIndexesScan) only ever backfills additions from current node
// state, never deletions.
func (bs *Store) commitPropertyIndexOnDiskBackfill(ops []writeOp) error {
	wb := bs.db.NewWriteBatch()
	defer wb.Cancel()
	for _, op := range ops {
		if err := wb.SetEntry(badgerv4.NewEntry(op.key, op.value)); err != nil {
			return fmt.Errorf("graph: property-index-on-disk backfill: %w", err)
		}
	}
	if err := wb.SetEntry(badgerv4.NewEntry(storepkg.PropertyIndexOnDiskBuiltKey, []byte{1})); err != nil {
		return fmt.Errorf("graph: property-index-on-disk backfill: mark built: %w", err)
	}
	if err := wb.Flush(); err != nil {
		return fmt.Errorf("graph: property-index-on-disk backfill: %w", err)
	}
	return nil
}
