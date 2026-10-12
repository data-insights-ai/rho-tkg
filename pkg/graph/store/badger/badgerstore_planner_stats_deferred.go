package badger

import (
	"encoding/binary"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"

	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Planner statistics across a clean reopen (backlog 54).
//
// Planner statistics (presence counts, type-class counts, NDV sketches and
// min/max) are O(distinct (label or type, key) pairs) since backlog 53, so the
// open image carries them whole (imgSecPlannerStats): a clean reopen restores
// exactly the state the closing process held — the NDV sketch with the values
// deleted during earlier sessions still counted (NDV never decreases, see
// docs/query-planners.md), min/max with its dirty flag. Only an image without
// that section (written while the statistics were still deferred, or with
// DisablePlannerStats) defers them: the first statistics read then builds
// them from the current rows (ensurePlannerStats), as a full open would.
// While deferred, every write skips the statistics (plannerStatsOff): the
// build reads the rows those writes left.

const plannerStatsImageVersion byte = 1

// statsBuildFaultForTest, when set, fails the deferred build after its node
// scan (tests only: a failed build must leave the statistics retryable).
var statsBuildFaultForTest func() error

// plannerStatsOff reports whether a write must skip planner-statistics
// maintenance. Caller holds idxMu.Lock (the build flips statsDeferred under it).
func (bs *Store) plannerStatsOff() bool {
	return bs.disablePlannerStats || bs.statsDeferred.Load()
}

// ensurePlannerStats builds deferred planner statistics. Every statistics read
// calls it before taking any lock. The build holds flushMu and idxMu.Lock
// (the Clear order), so no write and no flush commit runs beside it: a row is
// either still dirty in the entity cache or committed in badger, and one
// sequential scan per entity kind in ID order (as a full open reads them)
// sees every current row exactly once. A failed scan leaves the statistics
// empty and deferred, so the next read retries.
func (bs *Store) ensurePlannerStats() error {
	if !bs.statsDeferred.Load() {
		return nil
	}
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.idxMu.Lock()
	defer bs.idxMu.Unlock()
	if !bs.statsDeferred.Load() {
		return nil
	}
	bs.statsDeferred.Store(false) // the maintenance doors below must run
	err := bs.db.View(func(txn *badgerv4.Txn) error {
		err := mergeScanCurrent(txn, storepkg.KeyNode, storepkg.SizeNodeKey, sortedIDs(bs.nodeIDs),
			func(id types.NodeID, raw []byte) {
				var n *types.Node
				if raw == nil {
					n, _ = bs.getNodeLocked(id) // only in the write buffer
				} else if v, st := bs.nodeCache.Peek(id.SnowflakeID()); st == indexpkg.CacheHit {
					n = v // dirty or cached: the cache is never older than badger
				} else if st != indexpkg.CacheDeleted {
					var w storepkg.NodeWire
					if storepkg.SafeUnmarshal(raw, &w) == nil {
						n, _ = bs.decodeNodeWireForKey(w, id.SnowflakeID())
					}
				}
				if n != nil { // an unreadable row is skipped, as a full open skips it
					bs.adjustNodePropertyKeyCounts(n, 1)
				}
			})
		if err == nil && statsBuildFaultForTest != nil {
			err = statsBuildFaultForTest()
		}
		if err != nil {
			return err
		}
		return mergeScanCurrent(txn, storepkg.KeyRel, storepkg.SizeRelKey, sortedIDs(bs.relIDs),
			func(id types.RelID, raw []byte) {
				var r *types.Relationship
				if raw == nil {
					r, _ = bs.getRelLocked(id)
				} else if v, st := bs.relCache.Peek(id.SnowflakeID()); st == indexpkg.CacheHit {
					r = v
				} else if st != indexpkg.CacheDeleted {
					var w storepkg.RelWire
					if storepkg.SafeUnmarshal(raw, &w) == nil {
						r, _ = bs.decodeRelWireForKey(w, id.SnowflakeID())
					}
				}
				if r != nil {
					bs.addRelPropertyTypeClassCounts(r)
					bs.addRelPropertyStatsCounts(r)
				}
			})
	})
	if err != nil {
		bs.resetPlannerStatsLocked()
		bs.statsDeferred.Store(true)
		return fmt.Errorf("graph: build planner statistics: %w", err)
	}
	return nil
}

// resetPlannerStatsLocked empties every planner statistic. Caller holds
// idxMu.Lock.
func (bs *Store) resetPlannerStatsLocked() {
	for _, m := range []*sync.Map{&bs.propertyKeyCounts, &bs.propertyTypeClassCounts,
		&bs.relPropertyKeyCounts, &bs.relPropertyTypeClassCounts} {
		m.Range(func(k, _ any) bool { m.Delete(k); return true })
	}
	bs.propertyStats = make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator)
	bs.relPropertyStats = make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator)
}

func sortedIDs[ID ~int64](set map[ID]struct{}) []ID {
	out := make([]ID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	slices.Sort(out)
	return out
}

// mergeScanCurrent walks the entity keyspace (prefix, fixed key size) and the
// sorted live IDs together in ID order, calling fn once per live ID with the
// stored value, or with nil for a live ID not in badger yet (write buffer).
func mergeScanCurrent[ID ~int64](txn *badgerv4.Txn, prefix byte, size int, ids []ID, fn func(id ID, raw []byte)) error {
	it := txn.NewIterator(badgerv4.DefaultIteratorOptions)
	defer it.Close()
	p := []byte{prefix}
	i := 0
	for it.Seek(p); it.ValidForPrefix(p) && i < len(ids); it.Next() {
		key := it.Item().Key()
		if len(key) != size {
			continue
		}
		id := ID(storepkg.ParseIDFromKey(key, 1))
		for i < len(ids) && ids[i] < id {
			fn(ids[i], nil)
			i++
		}
		if i == len(ids) || ids[i] != id {
			continue // a stored row the RAM state does not count as live
		}
		if err := it.Item().Value(func(val []byte) error {
			fn(id, val)
			return nil
		}); err != nil {
			return err
		}
		i++
	}
	for ; i < len(ids); i++ {
		fn(ids[i], nil)
	}
	return nil
}

// ---- the image section ----

// imgPlannerStats is a decoded planner-statistics section.
type imgPlannerStats struct {
	nodeCounts  map[indexpkg.PropertyIndexKey]int64
	nodeClasses map[indexpkg.PropertyIndexKey][types.NumPropertyTypeClasses]int64
	nodeAccs    map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator
	relCounts   map[indexpkg.RelPropertyIndexKey]int64
	relClasses  map[indexpkg.RelPropertyIndexKey][types.NumPropertyTypeClasses]int64
	relAccs     map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator
}

// encodePlannerStatsLocked renders the statistics section payload, or nil when
// there is nothing exact to persist (statistics disabled or still deferred).
// Caller holds idxMu (read): the accumulators are guarded by it.
func (bs *Store) encodePlannerStatsLocked() []byte {
	if bs.plannerStatsOff() {
		return nil
	}
	b := []byte{plannerStatsImageVersion}
	keyed := func(b []byte, tok uint16, key string) []byte {
		return appendImgString(binary.BigEndian.AppendUint16(b, tok), key)
	}
	appendSyncMap := func(m *sync.Map, entry func(b []byte, k, v any) []byte) {
		var n uint64
		var body []byte
		m.Range(func(k, v any) bool {
			n++
			body = entry(body, k, v)
			return true
		})
		b = append(binary.AppendUvarint(b, n), body...)
	}
	count := func(b []byte, v any) []byte { return binary.AppendVarint(b, v.(*atomic.Int64).Load()) }
	classes := func(b []byte, v any) []byte {
		for i := range v.(*typeClassCounters).classes {
			b = binary.AppendVarint(b, v.(*typeClassCounters).classes[i].Load())
		}
		return b
	}
	nodeKeyed := func(b []byte, k any) []byte {
		key := k.(indexpkg.PropertyIndexKey)
		return keyed(b, key.LabelToken, key.PropertyKey)
	}
	relKeyed := func(b []byte, k any) []byte {
		key := k.(indexpkg.RelPropertyIndexKey)
		return keyed(b, key.RelTypeToken, key.PropertyKey)
	}
	appendSyncMap(&bs.propertyKeyCounts, func(b []byte, k, v any) []byte { return count(nodeKeyed(b, k), v) })
	appendSyncMap(&bs.propertyTypeClassCounts, func(b []byte, k, v any) []byte { return classes(nodeKeyed(b, k), v) })
	b = binary.AppendUvarint(b, uint64(len(bs.propertyStats)))
	for key, acc := range bs.propertyStats {
		b = acc.AppendBinary(nodeKeyed(b, key))
	}
	appendSyncMap(&bs.relPropertyKeyCounts, func(b []byte, k, v any) []byte { return count(relKeyed(b, k), v) })
	appendSyncMap(&bs.relPropertyTypeClassCounts, func(b []byte, k, v any) []byte { return classes(relKeyed(b, k), v) })
	b = binary.AppendUvarint(b, uint64(len(bs.relPropertyStats)))
	for key, acc := range bs.relPropertyStats {
		b = acc.AppendBinary(relKeyed(b, key))
	}
	return b
}

func decodePlannerStats(r *imgReader) *imgPlannerStats {
	if v := r.bytes(1); r.err != nil || v[0] != plannerStatsImageVersion {
		r.fail("planner statistics version")
		return nil
	}
	st := &imgPlannerStats{
		nodeCounts:  make(map[indexpkg.PropertyIndexKey]int64),
		nodeClasses: make(map[indexpkg.PropertyIndexKey][types.NumPropertyTypeClasses]int64),
		nodeAccs:    make(map[indexpkg.PropertyIndexKey]*indexpkg.PropertyStatsAccumulator),
		relCounts:   make(map[indexpkg.RelPropertyIndexKey]int64),
		relClasses:  make(map[indexpkg.RelPropertyIndexKey][types.NumPropertyTypeClasses]int64),
		relAccs:     make(map[indexpkg.RelPropertyIndexKey]*indexpkg.PropertyStatsAccumulator),
	}
	classesOf := func() (c [types.NumPropertyTypeClasses]int64) {
		for i := range c {
			c[i] = r.varint()
		}
		return c
	}
	acc := func() *indexpkg.PropertyStatsAccumulator {
		if r.err != nil {
			return nil
		}
		a, n, err := indexpkg.DecodePropertyStatsAccumulator(r.b)
		if err != nil {
			r.fail("planner statistics accumulator")
			return nil
		}
		r.b = r.b[n:]
		return a
	}
	nodeKey := func() indexpkg.PropertyIndexKey {
		return indexpkg.PropertyIndexKey{LabelToken: r.u16(), PropertyKey: r.str()}
	}
	relKey := func() indexpkg.RelPropertyIndexKey {
		return indexpkg.RelPropertyIndexKey{RelTypeToken: r.u16(), PropertyKey: r.str()}
	}
	for n := r.count(len(r.b)); n > 0 && r.err == nil; n-- {
		k := nodeKey()
		st.nodeCounts[k] = r.varint()
	}
	for n := r.count(len(r.b)); n > 0 && r.err == nil; n-- {
		k := nodeKey()
		st.nodeClasses[k] = classesOf()
	}
	for n := r.count(len(r.b)); n > 0 && r.err == nil; n-- {
		k := nodeKey()
		st.nodeAccs[k] = acc()
	}
	for n := r.count(len(r.b)); n > 0 && r.err == nil; n-- {
		k := relKey()
		st.relCounts[k] = r.varint()
	}
	for n := r.count(len(r.b)); n > 0 && r.err == nil; n-- {
		k := relKey()
		st.relClasses[k] = classesOf()
	}
	for n := r.count(len(r.b)); n > 0 && r.err == nil; n-- {
		k := relKey()
		st.relAccs[k] = acc()
	}
	return st
}

// installPlannerStatsLocked restores the closing process's statistics. Open
// path, single goroutine.
func (bs *Store) installPlannerStatsLocked(st *imgPlannerStats) {
	for k, c := range st.nodeCounts {
		bs.getOrCreatePropertyKeyCounter(k.LabelToken, k.PropertyKey).Store(c)
	}
	for k, cs := range st.nodeClasses {
		tc := bs.getOrCreateTypeClassCounters(k.LabelToken, k.PropertyKey)
		for i, c := range cs {
			tc.classes[i].Store(c)
		}
	}
	for k, a := range st.nodeAccs {
		bs.propertyStats[k] = a
	}
	for k, c := range st.relCounts {
		bs.getOrCreateRelPropertyKeyCounter(k.RelTypeToken, k.PropertyKey).Store(c)
	}
	for k, cs := range st.relClasses {
		tc := bs.getOrCreateRelTypeClassCounters(k.RelTypeToken, k.PropertyKey)
		for i, c := range cs {
			tc.classes[i].Store(c)
		}
	}
	for k, a := range st.relAccs {
		bs.relPropertyStats[k] = a
	}
}
