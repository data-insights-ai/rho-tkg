package badger

import (
	"bytes"
	"fmt"
	"sort"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// store.ReadSnapshotCapability (backlog 55): an effective scan reads a run of
// entities (one chunk, one hold of the graph read lock) through ONE read
// transaction and forward-walking iterators, instead of a View per read
// (three per entity with history: the row, the temporal skeletons, the
// winning version).
//
// THE RULE. Each read answers what the store's method of the same name answers
// at that moment. The cache and the write buffer are consulted live, exactly
// as the method does; only the Badger part comes from the shared transaction.
// That part equals a fresh View's as long as nothing reached Badger since the
// transaction opened, and every commit that moves rows out of the write
// buffer bumps commitGen (flush, Clear) BEFORE the rows leave the overlay and
// before a flush turns cache entries clean (markCacheFlushed follows the
// bump, so commitGen also covers the caches' flush epochs: a cache miss after
// an unchanged commitGen proves the row's latest version is in the
// transaction). So the transaction is re-pinned whenever commitGen moved since
// it opened, checked after the cache miss or the overlay capture the read
// depends on (scanSnapshot's rule, extended to history rows): one transaction
// per run plus one per flush that lands during it, never one per entity.
//
// Release ends the run: it closes the iterators (they pin table files) and
// discards the transaction (its read mark would keep every version it can see
// from compaction and value-log GC while the caller runs fn).

var _ storecontract.ReadSnapshotCapability = (*Store)(nil)

var _ storecontract.ReadSnapshotLendCapability = (*readSnapshot)(nil)

// NewReadSnapshot returns a store.ReadSnapshot; it opens its transaction on
// the first read that reaches Badger.
func (bs *Store) NewReadSnapshot() storecontract.ReadSnapshot { return &readSnapshot{bs: bs} }

// The keyspaces a snapshot walks, one iterator each.
const (
	snapNodeRows = iota
	snapRelRows
	snapNodeHist
	snapRelHist
	snapNodeVer // point reads of history rows (winners), ascending across entities
	snapRelVer
	snapIters
)

// snapWalk is how many Next steps a forward walk tries before it seeks: an
// ascending scan of one type finds its next row within a few keys.
const snapWalk = 8

type readSnapshot struct {
	bs        *Store
	txn       *badgerv4.Txn
	gen       uint64 // commitGen read before txn opened
	nodeEpoch uint64 // the caches' flush epochs read before txn opened
	relEpoch  uint64
	its       [snapIters]snapIter
	memo      histMemo
	metas     metaMemo
}

// snapIter is one keyspace's iterator and the key it last sought, the
// iterator standing at the first key at or after it (nil: unknown): a later
// key walks forward from there, an earlier one seeks.
type snapIter struct {
	it   *badgerv4.Iterator
	last []byte
}

// histMemo is the committed history rows (key → raw value) of the entity read
// last in the current transaction (Close drops it with the transaction): the
// skeleton read, the winner hydration and a full-history read of one entity
// read Badger once.
type histMemo struct {
	prefix string
	rows   map[string][]byte
}

// metaMemo is the committed history skeleton (key → temporal meta) of the
// entity whose skeleton was read last in the current transaction: the
// presence probe and the skeleton read of one entity walk Badger once and copy
// no row (row 121); a winning row is then read on its own (version).
type metaMemo struct {
	prefix string
	metas  map[string]storecontract.VersionTemporalMeta
}

// pinned returns the transaction, re-opened when a commit happened since it
// opened. Call it only after the cache miss or the overlay capture the read
// depends on. The cache epochs are read before it opens, for LoadCleanAt.
func (s *readSnapshot) pinned() *badgerv4.Txn {
	bs := s.bs
	gen := bs.commitGen.Load()
	if s.txn == nil || gen != s.gen {
		s.Release()
		s.gen, s.nodeEpoch, s.relEpoch = gen, bs.nodeCache.FlushEpoch(), bs.relCache.FlushEpoch()
		s.txn = newReadTxn(bs.db)
	}
	return s.txn
}

// Release closes the iterators and discards the transaction (with it the
// history memo); the next read opens a new one. After the store closed the
// transaction is dropped without Discard (its read mark died with the
// database).
func (s *readSnapshot) Release() {
	for i := range s.its {
		if s.its[i].it != nil {
			s.its[i].it.Close()
		}
		s.its[i] = snapIter{}
	}
	if s.txn != nil {
		endReadTxn(s.txn, !s.bs.dbClosed.Load() && !s.bs.closing.Load())
	}
	s.txn = nil
	s.memo = histMemo{}
	s.metas = metaMemo{}
}

// Close is Release: a snapshot holds nothing between runs.
func (s *readSnapshot) Close() { s.Release() }

// pastPrefix records a walk that read every key under prefix: the iterator
// stands at the first key at or after the prefix's end, so the next
// ascending entity walks forward from it instead of seeking (a badger Seek
// drops the iterator's current item and copies the next value afresh).
func (si *snapIter) pastPrefix(prefix []byte) {
	si.last = append(si.last[:0], prefix...)
	for i := len(si.last) - 1; i >= 0; i-- {
		if si.last[i]++; si.last[i] != 0 {
			return
		}
	}
	si.last = nil // the keyspace's end: the next read seeks
}

// seek positions keyspace i's iterator at the first key at or after key.
func (s *readSnapshot) seek(i int, key []byte) *badgerv4.Iterator {
	txn := s.pinned()
	si := &s.its[i]
	if si.it == nil {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false // one entity per step: no value window to fill
		si.it = txn.NewIterator(opts)
		si.last = nil
	}
	walked := false
	if si.last != nil && bytes.Compare(key, si.last) >= 0 {
		for n := 0; n < snapWalk; n++ {
			if !si.it.Valid() || bytes.Compare(si.it.Item().Key(), key) >= 0 {
				walked = true
				break
			}
			si.it.Next()
		}
	}
	if !walked {
		si.it.Seek(key)
	}
	si.last = append(si.last[:0], key...)
	return si.it
}

// row hands the value stored at key (keyspace i) to decode, or reports
// false. The value is the iterator's buffer, valid only during decode: the
// iterator reuses it across rows, so a run of reads copies no row (row 121).
func (s *readSnapshot) row(i int, key []byte, decode func(val []byte) error) (bool, error) {
	it := s.seek(i, key)
	if !it.Valid() || !bytes.Equal(it.Item().Key(), key) {
		return false, nil
	}
	return true, it.Item().Value(decode)
}

// committedHistory returns the committed history rows under prefix (keyspace
// i) in the current transaction, from the memo when this entity was read last
// on it. Call it after the overlay capture.
func (s *readSnapshot) committedHistory(i int, prefix []byte) (map[string][]byte, error) {
	s.pinned()
	if s.memo.rows != nil && s.memo.prefix == string(prefix) {
		return s.memo.rows, nil
	}
	rows := map[string][]byte{}
	for it := s.seek(i, prefix); it.ValidForPrefix(prefix); it.Next() {
		item := it.Item()
		if len(item.Key()) != storepkg.SizeHistKey {
			continue
		}
		val, err := item.ValueCopy(nil)
		if err != nil {
			return nil, err
		}
		rows[string(item.KeyCopy(nil))] = val
	}
	s.its[i].pastPrefix(prefix)
	s.memo = histMemo{prefix: string(prefix), rows: rows}
	return rows, nil
}

// committedMetas returns the temporal skeleton of the committed history rows
// under prefix (keyspace i) in the current transaction, decoded in place from
// the iterator's buffers, from the memo when this entity was read last. Call
// it after the overlay capture.
func (s *readSnapshot) committedMetas(i int, prefix []byte) (map[string]storecontract.VersionTemporalMeta, error) {
	s.pinned()
	if s.metas.metas != nil && s.metas.prefix == string(prefix) {
		return s.metas.metas, nil
	}
	node := i == snapNodeHist
	metas := map[string]storecontract.VersionTemporalMeta{}
	if s.memo.rows != nil && s.memo.prefix == string(prefix) {
		for k, raw := range s.memo.rows {
			m, err := historyTemporalMetaOf(raw, node)
			if err != nil {
				return nil, err
			}
			metas[k] = m
		}
	} else {
		for it := s.seek(i, prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			if len(item.Key()) != storepkg.SizeHistKey {
				continue
			}
			var m storecontract.VersionTemporalMeta
			if err := item.Value(func(val []byte) error {
				var err error
				m, err = historyTemporalMetaOf(val, node)
				return err
			}); err != nil {
				return nil, err
			}
			metas[string(item.Key())] = m
		}
		s.its[i].pastPrefix(prefix)
	}
	s.metas = metaMemo{prefix: string(prefix), metas: metas}
	return metas, nil
}

// historyMetas merges the committed skeleton under prefix with the write
// buffer as history merges rows: key → temporal meta.
func (s *readSnapshot) historyMetas(i int, prefix []byte) (map[string]storecontract.VersionTemporalMeta, error) {
	overlay, overlayDeletes := s.bs.pendingHistoryVersionOverlay(prefix, 0)
	if h := s.bs.historyScanTestHook; h != nil {
		h() // tests only: the commit window between the overlay capture and the read
	}
	committed, err := s.committedMetas(i, prefix)
	if err != nil {
		return nil, err
	}
	entries := make(map[string]storecontract.VersionTemporalMeta, len(committed)+len(overlay))
	for k, m := range committed {
		if _, deleted := overlayDeletes[k]; !deleted {
			entries[k] = m
		}
	}
	for k, raw := range overlay {
		m, err := historyTemporalMetaOf(raw, i == snapNodeHist)
		if err != nil {
			return nil, fmt.Errorf("graph: decode pending history temporal meta: %w", err)
		}
		entries[k] = m
	}
	return entries, nil
}

// history merges the committed rows under prefix with the write buffer as
// getNodeHistoryByPrefix does (overlay captured first; a buffered delete masks
// a committed row, a buffered set replaces it): key → raw value.
func (s *readSnapshot) history(i int, prefix []byte) (map[string][]byte, error) {
	overlay, overlayDeletes := s.bs.pendingHistoryVersionOverlay(prefix, 0)
	if h := s.bs.historyScanTestHook; h != nil {
		h() // tests only: the commit window between the overlay capture and the read
	}
	committed, err := s.committedHistory(i, prefix)
	if err != nil {
		return nil, err
	}
	entries := make(map[string][]byte, len(committed)+len(overlay))
	for k, v := range committed {
		if _, deleted := overlayDeletes[k]; !deleted {
			entries[k] = v
		}
	}
	for k, v := range overlay {
		entries[k] = v
	}
	return entries, nil
}

// version returns history row key's raw value as GetNodeVersion /
// GetRelVersion read it: the write buffer first, then the snapshot.
func (s *readSnapshot) version(prefix, key []byte) ([]byte, error) {
	if op, found := s.bs.lookupPending(string(key)); found {
		if op.opType == writeOpDelete {
			return nil, ErrVersionNotFound
		}
		return op.value, nil
	}
	s.pinned()
	if s.memo.rows != nil && s.memo.prefix == string(prefix) {
		if raw, ok := s.memo.rows[string(key)]; ok {
			return raw, nil
		}
		return nil, ErrVersionNotFound
	}
	item, err := s.txn.Get(key)
	if err == badgerv4.ErrKeyNotFound {
		return nil, ErrVersionNotFound
	}
	if err != nil {
		return nil, err
	}
	return item.ValueCopy(nil)
}

// versionRow hands history row key's raw value to decode, read as version
// reads it. A full committed row is decoded inside the read, from the
// iterator's buffer (valid only during decode), so a winning history row
// costs one decode and no copy of its own (row 121); a delta
// row is copied out first, as its anchor is read through the same
// transaction.
func (s *readSnapshot) versionRow(i int, prefix, key []byte, decode func(raw []byte) error) error {
	if op, found := s.bs.lookupPending(string(key)); found {
		if op.opType == writeOpDelete {
			return ErrVersionNotFound
		}
		return decode(op.value)
	}
	s.pinned()
	if s.memo.rows != nil && s.memo.prefix == string(prefix) {
		raw, ok := s.memo.rows[string(key)]
		if !ok {
			return ErrVersionNotFound
		}
		return decode(raw)
	}
	// Its own iterator, walked forward: winners of ascending entities ascend,
	// and a walked iterator reuses its items' buffers where a Get copies the
	// row twice afresh.
	it := s.seek(i, key)
	if !it.Valid() || !bytes.Equal(it.Item().Key(), key) {
		return ErrVersionNotFound
	}
	var delta []byte
	if err := it.Item().Value(func(val []byte) error {
		if storepkg.HistoryValueKindOf(val) == storepkg.HistoryFull {
			return decode(val)
		}
		delta = append([]byte(nil), val...)
		return nil
	}); err != nil || delta == nil {
		return err
	}
	return decode(delta)
}

// anchor is the localAnchorFunc of a delta row read through the snapshot.
func (s *readSnapshot) anchor(prefix []byte, key func(uint64) []byte) localAnchorFunc {
	return func(v uint64) ([]byte, bool) {
		raw, err := s.version(prefix, key(v))
		return raw, err == nil
	}
}

// historyMeta is historyTemporalMetaByPrefix through the snapshot.
func (s *readSnapshot) historyMeta(i int, prefix []byte) ([]storecontract.VersionTemporalMeta, error) {
	entries, err := s.historyMetas(i, prefix)
	if err != nil {
		return nil, fmt.Errorf("graph: scan history temporal meta: %w", err)
	}
	if len(entries) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]storecontract.VersionTemporalMeta, 0, len(keys))
	for _, k := range keys {
		out = append(out, entries[k])
	}
	return out, nil
}

// sortedKeys returns the keys of a history row map ascending (by version:
// the keys share one entity prefix and end in the big-endian version).
func sortedKeys(m map[string][]byte) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// GetNode is Store.GetNode through the snapshot.
func (s *readSnapshot) GetNode(nid types.NodeID) (*types.Node, error) {
	n, err := s.LendNode(nid)
	if err != nil {
		return nil, err
	}
	return n.DeepCopy(), nil
}

// LendNode is Store.LendNode through the snapshot
// (store.ReadSnapshotLendCapability): the cached frozen row, or the committed
// row decoded in place, frozen, cached and lent.
func (s *readSnapshot) LendNode(nid types.NodeID) (*types.Node, error) {
	bs := s.bs
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	v, status := bs.nodeCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v, nil
	case indexpkg.CacheDeleted:
		return nil, ErrNodeNotFound
	}
	bs.idxMu.RLock()
	_, exists := bs.nodeIDs[nid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrNodeNotFound
	}
	var n *types.Node
	ok, err := s.row(snapNodeRows, storepkg.NodeKey(id), func(val []byte) error {
		w := getPooledNodeWire()
		defer putPooledNodeWire(w)
		if err := storepkg.SafeUnmarshal(val, w); err != nil {
			return fmt.Errorf("graph: unmarshal node: %w", err)
		}
		decoded, err := bs.decodeNodeWireForKey(*w, id)
		if err != nil {
			return fmt.Errorf("graph: decode node: %w", err)
		}
		n = decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrNodeNotFound
	}
	n.Freeze() // shared between cache and caller
	bs.nodeCache.LoadCleanAt(id, n, s.nodeEpoch)
	return n, nil
}

// GetRelationship is Store.GetRelationship through the snapshot.
func (s *readSnapshot) GetRelationship(rid types.RelID) (*types.Relationship, error) {
	r, err := s.LendRelationship(rid)
	if err != nil {
		return nil, err
	}
	return r.DeepCopy(), nil
}

// LendRelationship is LendNode for relationships.
func (s *readSnapshot) LendRelationship(rid types.RelID) (*types.Relationship, error) {
	bs := s.bs
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	id := rid.SnowflakeID()
	v, status := bs.relCache.Get(id)
	switch status {
	case indexpkg.CacheHit:
		return v, nil
	case indexpkg.CacheDeleted:
		return nil, ErrRelNotFound
	}
	bs.idxMu.RLock()
	_, exists := bs.relIDs[rid]
	bs.idxMu.RUnlock()
	if !exists {
		return nil, ErrRelNotFound
	}
	var r *types.Relationship
	ok, err := s.row(snapRelRows, storepkg.RelKey(id), func(val []byte) error {
		var w storepkg.RelWire
		if err := storepkg.SafeUnmarshal(val, &w); err != nil {
			return fmt.Errorf("graph: unmarshal relationship: %w", err)
		}
		decoded, err := bs.decodeRelWireForKey(w, id)
		if err != nil {
			return fmt.Errorf("graph: decode relationship: %w", err)
		}
		r = decoded
		return nil
	})
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrRelNotFound
	}
	r.Freeze() // shared between cache and caller
	bs.relCache.LoadCleanAt(id, r, s.relEpoch)
	return r, nil
}

func (s *readSnapshot) HasNodeHistory(nid types.NodeID) (bool, error) {
	bs := s.bs
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return false, err
	}
	return bs.hasHistory(&bs.histNodePresence, storepkg.KeyHistNode, nid.SnowflakeID(), s.probe)
}

// HasRelHistory is HasNodeHistory for relationships.
func (s *readSnapshot) HasRelHistory(rid types.RelID) (bool, error) {
	bs := s.bs
	if err := bs.checkOpen(); err != nil {
		return false, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return false, err
	}
	return bs.hasHistory(&bs.histRelPresence, storepkg.KeyHistRel, rid.SnowflakeID(), s.probe)
}

// probe is probeHistoryPresence through the snapshot: whether id has a
// history row (write buffer first, a buffered delete masking its committed
// key) and the highest version, clamped.
func (s *readSnapshot) probe(kind byte, id snowflake.ID, _ bool) (bool, uint32, error) {
	i, prefix := snapRelHist, storepkg.HistRelPrefix(id)
	if kind == storepkg.KeyHistNode {
		i, prefix = snapNodeHist, storepkg.HistNodePrefix(id)
	}
	entries, err := s.historyMetas(i, prefix)
	if err != nil {
		return false, 0, fmt.Errorf("graph: probe history presence: %w", err)
	}
	var top uint64
	for k := range entries {
		top = max(top, historyVersionFromKey([]byte(k)))
	}
	return len(entries) > 0, clampTop(top), nil
}

// nodeHistoryRaw returns nid's history rows (key → raw) through the snapshot.
func (s *readSnapshot) nodeHistoryRaw(nid types.NodeID) (map[string][]byte, error) {
	if err := s.bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	return s.history(snapNodeHist, storepkg.HistNodePrefix(nid.SnowflakeID()))
}

func (s *readSnapshot) relHistoryRaw(rid types.RelID) (map[string][]byte, error) {
	if err := s.bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	return s.history(snapRelHist, storepkg.HistRelPrefix(rid.SnowflakeID()))
}

// NodeHistoryTemporalMeta is Store.NodeHistoryTemporalMeta through the snapshot.
func (s *readSnapshot) NodeHistoryTemporalMeta(nid types.NodeID) ([]storecontract.VersionTemporalMeta, error) {
	if err := s.bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	return s.historyMeta(snapNodeHist, storepkg.HistNodePrefix(nid.SnowflakeID()))
}

// RelHistoryTemporalMeta is Store.RelHistoryTemporalMeta through the snapshot.
func (s *readSnapshot) RelHistoryTemporalMeta(rid types.RelID) ([]storecontract.VersionTemporalMeta, error) {
	if err := s.bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	return s.historyMeta(snapRelHist, storepkg.HistRelPrefix(rid.SnowflakeID()))
}

// GetNodeVersion is Store.GetNodeVersion through the snapshot.
func (s *readSnapshot) GetNodeVersion(nid types.NodeID, version uint32) (*types.Node, error) {
	bs := s.bs
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateNodeID(nid); err != nil {
		return nil, err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	var n *types.Node
	err := s.versionRow(snapNodeVer, prefix, storepkg.HistNodeKey(id, uint64(version)), func(raw []byte) error {
		var err error
		n, err = s.decodeNodeVersion(id, uint64(version), raw, prefix)
		return err
	})
	if err != nil {
		return nil, err
	}
	return n, nil // a fresh decode: the caller's own
}

// GetRelVersion is Store.GetRelVersion through the snapshot.
func (s *readSnapshot) GetRelVersion(rid types.RelID, version uint32) (*types.Relationship, error) {
	bs := s.bs
	if err := bs.checkOpen(); err != nil {
		return nil, err
	}
	if err := storecontract.ValidateRelID(rid); err != nil {
		return nil, err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	var r *types.Relationship
	err := s.versionRow(snapRelVer, prefix, storepkg.HistRelKey(id, uint64(version)), func(raw []byte) error {
		var err error
		r, err = s.decodeRelVersion(id, uint64(version), raw, prefix)
		return err
	})
	if err != nil {
		return nil, err
	}
	return r, nil // a fresh decode: the caller's own
}

func (s *readSnapshot) decodeNodeVersion(id snowflake.ID, version uint64, raw, prefix []byte) (*types.Node, error) {
	if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryFull {
		return s.bs.decodeHistoryNodeValue(id, version, raw) // no anchor: the pooled full-row decode
	}
	anchor := s.anchor(prefix, func(v uint64) []byte { return storepkg.HistNodeKey(id, v) })
	w, err := s.bs.reconstructNodeHistoryWire(id, version, raw, anchor)
	if err != nil {
		return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
	}
	n, err := s.bs.decodeNodeHistoryWireForKey(w, id, version)
	if err != nil {
		return nil, fmt.Errorf("graph: decode node version: %w", err)
	}
	return n, nil
}

func (s *readSnapshot) decodeRelVersion(id snowflake.ID, version uint64, raw, prefix []byte) (*types.Relationship, error) {
	anchor := s.anchor(prefix, func(v uint64) []byte { return storepkg.HistRelKey(id, v) })
	w, err := s.bs.reconstructRelHistoryWire(id, version, raw, anchor)
	if err != nil {
		return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
	}
	r, err := s.bs.decodeRelHistoryWireForKey(w, id, version)
	if err != nil {
		return nil, fmt.Errorf("graph: decode rel version: %w", err)
	}
	return r, nil
}

// GetNodeHistory is Store.GetNodeHistory through the snapshot.
func (s *readSnapshot) GetNodeHistory(nid types.NodeID) ([]*types.Node, error) {
	entries, err := s.nodeHistoryRaw(nid)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	id := nid.SnowflakeID()
	prefix := storepkg.HistNodePrefix(id)
	out := make([]*types.Node, 0, len(entries))
	for _, k := range sortedKeys(entries) {
		n, err := s.decodeNodeVersion(id, historyVersionFromKey([]byte(k)), entries[k], prefix)
		if err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, nil
}

// GetRelHistory is Store.GetRelHistory through the snapshot.
func (s *readSnapshot) GetRelHistory(rid types.RelID) ([]*types.Relationship, error) {
	entries, err := s.relHistoryRaw(rid)
	if err != nil || len(entries) == 0 {
		return nil, err
	}
	id := rid.SnowflakeID()
	prefix := storepkg.HistRelPrefix(id)
	out := make([]*types.Relationship, 0, len(entries))
	for _, k := range sortedKeys(entries) {
		r, err := s.decodeRelVersion(id, historyVersionFromKey([]byte(k)), entries[k], prefix)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, nil
}

// historyTemporalMetaOf decodes one history value's selection-scope temporal
// skeleton; node selects the delta decoder (NodeHistoryDelta vs
// RelHistoryDelta), the partial FULL-row decode is entity-agnostic.
func historyTemporalMetaOf(raw []byte, node bool) (storecontract.VersionTemporalMeta, error) {
	if storepkg.HistoryValueKindOf(raw) == storepkg.HistoryDelta {
		if node {
			d, err := storepkg.DecodeNodeHistoryDelta(raw)
			if err != nil {
				return storecontract.VersionTemporalMeta{}, err
			}
			return storecontract.VersionTemporalMeta{
				Version:  uint32(d.Meta.Version), // #nosec G115 — version from our own serialization
				Temporal: storepkg.SelectionTemporalMetaOfNodeWire(d.Meta),
			}, nil
		}
		d, err := storepkg.DecodeRelHistoryDelta(raw)
		if err != nil {
			return storecontract.VersionTemporalMeta{}, err
		}
		return storecontract.VersionTemporalMeta{
			Version:  uint32(d.Meta.Version), // #nosec G115 — version from our own serialization
			Temporal: storepkg.SelectionTemporalMetaOfRelWire(d.Meta),
		}, nil
	}
	version, tm, err := storepkg.DecodeWireTemporalMeta(raw)
	if err != nil {
		return storecontract.VersionTemporalMeta{}, err
	}
	return storecontract.VersionTemporalMeta{Version: version, Temporal: tm}, nil
}
