package badger

import (
	"fmt"
	"iter"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// SetRowDecodeHookForTest installs fn to run once per entity row any Store in
// the process decodes (current rows and history rows) and returns a restore
// func. Open-cost tests count the rows a reopen decodes with it; the caller
// must not run in parallel with other decoding tests. Not for production use.
func SetRowDecodeHookForTest(fn func()) (restore func()) {
	prev := rowDecodeHook.Load()
	if fn == nil {
		rowDecodeHook.Store(nil)
	} else {
		rowDecodeHook.Store(&fn)
	}
	return func() { rowDecodeHook.Store(prev) }
}

// CountRowDecodesForTest runs fn and returns how many entity rows were decoded
// while it ran. Not for production use.
func CountRowDecodesForTest(fn func()) int64 {
	var n atomic.Int64
	restore := SetRowDecodeHookForTest(func() { n.Add(1) })
	defer restore()
	fn()
	return n.Load()
}

// IndexStateDigestForTest renders every RAM structure an open derives from the
// persisted rows — liveness sets, ordinals, label / type / adjacency indexes,
// counters, property / relationship-property / composite / temporal /
// high-frequency / vector index contents, planner statistics and node
// integrity hashes — as one canonical, sorted text. Two stores over the same
// data must render the same digest whatever path built their indexes; tests
// compare a reopened store against a full-rebuild oracle with it. Not for
// production use.
func (bs *Store) IndexStateDigestForTest() string {
	bs.idxMu.RLock()
	nodes := digestNodeIDSet(&bs.nodeIDs)
	rels := digestRelIDSet(&bs.relIDs)
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	for _, id := range nodes {
		add("node %d ord=%d", id, bs.ords.node(id, false))
	}
	for _, id := range rels {
		add("rel %d ord=%d", id, bs.ords.rel(id, false))
	}
	for tok, set := range bs.labelIdx {
		add("label %d %v", tok, digestNodeIDs(set))
	}
	for tok, set := range bs.typeIdx {
		add("type %d %v", tok, digestRelIDs(set))
	}
	for start, set := range bs.outIdx {
		for rid, end := range set {
			add("out %d %d %d", start, rid, end)
		}
	}
	for end, set := range bs.inIdx {
		for rid, e := range set {
			add("in %d %d %d %d", end, rid, e.start, e.typ)
		}
	}
	add("counts nodes=%d rels=%d", bs.nodeCount.Load(), bs.relCount.Load())
	add("epochs node=%d rel=%d salt=%d coarse=%d", bs.nodeEpoch.Load(), bs.relEpoch.Load(), bs.nodeEpochSalt.Load(), bs.relEpochCoarse.Load())
	for i := range bs.nodeLabelEpochs {
		if v := bs.nodeLabelEpochs[i].Load(); v != 0 {
			add("labelepoch %d %d", i, v)
		}
	}
	for i := range bs.relTypeEpochs {
		if v := bs.relTypeEpochs[i].Load(); v != 0 {
			add("typeepoch %d %d", i, v)
		}
	}
	bs.labelCounts.Range(func(k, v any) bool {
		if c := v.(*atomic.Int64).Load(); c != 0 {
			add("labelcount %v %d", k, c)
		}
		return true
	})
	bs.typeCounts.Range(func(k, v any) bool {
		if c := v.(*atomic.Int64).Load(); c != 0 {
			add("typecount %v %d", k, c)
		}
		return true
	})
	for key, idx := range bs.propertyIndexes {
		for vk, ids := range idx.Entries {
			add("propidx %d %s %q %v", key.LabelToken, key.PropertyKey, vk, ids.AppendAfter(nil, 0, 0))
		}
	}
	for key, idx := range bs.relPropertyIndexes {
		for vk, ids := range idx.Entries {
			add("relpropidx %d %s %q %v", key.RelTypeToken, key.PropertyKey, vk, ids.AppendAfter(nil, 0, 0))
		}
	}
	for key, idx := range bs.compositeIndexes {
		idx.Range(func(vk string, _ int, ids iter.Seq[snowflake.ID]) bool {
			add("composite %d %q %q %v", key.LabelToken, key.Keys, vk, slices.Collect(ids))
			return true
		})
	}
	for tok, ti := range bs.temporalIndexes {
		for _, e := range ti.Entries {
			add("temporal %d %d %d %d", tok, e.ID, e.From, e.To)
		}
	}
	for tok, ti := range bs.relTypeTemporalIndexes {
		for _, e := range ti.Entries {
			add("reltemporal %d %d %d %d", tok, e.ID, e.From, e.To)
		}
	}
	for tok, hfi := range bs.hfIndexes {
		ids := hfi.CandidatesUpTo(types.Instant(math.MaxInt64))
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		add("hf %d %v", tok, ids)
	}
	for key, vi := range bs.vectorIndexes {
		ids := vi.IDs()
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
		add("vector %d %s %v", key.LabelToken, key.PropertyKey, ids)
	}
	bs.idxMu.RUnlock()

	if !bs.disablePlannerStats {
		lines = append(lines, bs.plannerStatsDigestLines()...)
	}
	for _, id := range nodes {
		h, err := bs.NodeIntegrityHash(id)
		add("hash %d %s %v", id, h, err)
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

func (bs *Store) plannerStatsDigestLines() []string {
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	if err := bs.ensurePlannerStats(); err != nil {
		add("statistics build failed: %v", err)
	}
	bs.propertyKeyCounts.Range(func(k, v any) bool {
		key := k.(indexpkg.PropertyIndexKey)
		if c := v.(*atomic.Int64).Load(); c != 0 {
			add("nodepresence %d %s %d", key.LabelToken, key.PropertyKey, c)
		}
		tc, _ := bs.NodePropertyTypeClassCounts(key.LabelToken, key.PropertyKey)
		st, _ := bs.NodePropertyStats(key.LabelToken, key.PropertyKey)
		add("nodestats %d %s %+v %+v", key.LabelToken, key.PropertyKey, tc, st)
		return true
	})
	bs.relPropertyKeyCounts.Range(func(k, v any) bool {
		key := k.(indexpkg.RelPropertyIndexKey)
		if c := v.(*atomic.Int64).Load(); c != 0 {
			add("relpresence %d %s %d", key.RelTypeToken, key.PropertyKey, c)
		}
		tc, _ := bs.RelPropertyTypeClassCounts(key.RelTypeToken, key.PropertyKey)
		st, _ := bs.RelPropertyStats(key.RelTypeToken, key.PropertyKey)
		add("relstats %d %s %+v %+v", key.RelTypeToken, key.PropertyKey, tc, st)
		return true
	})
	return lines
}

func digestNodeIDSet(set *indexpkg.IDSet) []types.NodeID {
	out := make([]types.NodeID, 0, set.Len())
	set.Ascend(func(id int64) bool { out = append(out, types.NodeID(id)); return true })
	return out
}

func digestNodeIDs(set map[types.NodeID]struct{}) []types.NodeID {
	out := make([]types.NodeID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func digestRelIDSet(set *indexpkg.IDSet) []types.RelID {
	out := make([]types.RelID, 0, set.Len())
	set.Ascend(func(id int64) bool { out = append(out, types.RelID(id)); return true })
	return out
}

func digestRelIDs(set map[types.RelID]struct{}) []types.RelID {
	out := make([]types.RelID, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// DeleteOpenStateForTest deletes the persisted open state in txn (a raw badger
// transaction on a closed store's directory), leaving the directory as a
// release without it would have closed it. Not for production use.
func DeleteOpenStateForTest(txn *badgerv4.Txn) error {
	return forEachOpenStateKey(txn, func(key []byte) error { return txn.Delete(key) })
}

// CorruptOpenStateForTest flips one byte in every persisted open-state chunk
// in txn. Not for production use.
func CorruptOpenStateForTest(txn *badgerv4.Txn) error {
	// The header is rewritten unchanged in the same commit, so it stays the
	// store's newest version: only the checksum can tell the damage.
	return forEachOpenStateKey(txn, func(key []byte) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.ValueCopy(nil)
		if err != nil || len(val) == 0 {
			return err
		}
		if string(key) != string(openImageHeaderKey) {
			val[len(val)/2] ^= 0xff
		}
		return txn.Set(key, val)
	})
}

func forEachOpenStateKey(txn *badgerv4.Txn, fn func(key []byte) error) error {
	opts := badgerv4.DefaultIteratorOptions
	opts.PrefetchValues = false
	opts.Prefix = openImageHeaderKey
	it := txn.NewIterator(opts)
	var keys [][]byte
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, it.Item().KeyCopy(nil))
	}
	it.Close()
	for _, k := range keys {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

// SetOpenImageChunkBytesForTest sets the open-image chunk size (default 512
// KiB) so a small store spans several chunks; returns a restore func. Not for
// production use.
func SetOpenImageChunkBytesForTest(n int) (restore func()) {
	prev := openImageChunkBytes
	openImageChunkBytes = n
	return func() { openImageChunkBytes = prev }
}

// PlannerStatsStateForTest renders the raw planner-statistics state — every
// counter, and per accumulator its NDV estimate, typed min/max and dirty flag —
// without building deferred statistics and without the dirty rescan a
// statistics read runs, so it shows exactly what the store holds. Not for
// production use.
func (bs *Store) PlannerStatsStateForTest() string {
	bs.idxMu.RLock()
	defer bs.idxMu.RUnlock()
	var lines []string
	add := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }
	for _, m := range []*sync.Map{&bs.propertyKeyCounts, &bs.relPropertyKeyCounts} {
		m.Range(func(k, v any) bool { add("count %T %v %d", k, k, v.(*atomic.Int64).Load()); return true })
	}
	for _, m := range []*sync.Map{&bs.propertyTypeClassCounts, &bs.relPropertyTypeClassCounts} {
		m.Range(func(k, v any) bool {
			var c []int64
			for i := range v.(*typeClassCounters).classes {
				c = append(c, v.(*typeClassCounters).classes[i].Load())
			}
			add("classes %T %v %v", k, k, c)
			return true
		})
	}
	for k, a := range bs.propertyStats {
		ndv, mn, mx := a.Snapshot()
		add("acc node %v ndv=%d min=%#v max=%#v dirty=%v", k, ndv, mn, mx, a.Dirty())
	}
	for k, a := range bs.relPropertyStats {
		ndv, mn, mx := a.Snapshot()
		add("acc rel %v ndv=%d min=%#v max=%#v dirty=%v", k, ndv, mn, mx, a.Dirty())
	}
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}

// StatsDeferredForTest reports whether planner statistics wait for their
// first read. Not for production use.
func (bs *Store) StatsDeferredForTest() bool { return bs.statsDeferred.Load() }

// PlannerStatsDigestForTest renders the planner statistics alone (building
// deferred ones first), canonical and sorted. Not for production use.
func (bs *Store) PlannerStatsDigestForTest() string {
	lines := bs.plannerStatsDigestLines()
	sort.Strings(lines)
	return strings.Join(lines, "\n")
}
