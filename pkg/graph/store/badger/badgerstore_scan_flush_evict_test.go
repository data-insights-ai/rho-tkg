package badger

import (
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// These tests pin the FLUSH-EVICT window in the bulk scans (forEachNodeBulk,
// collectNodesBulkParallel, forEachRelBulk). Each opens ONE badger read
// transaction up front and consults the entity cache per row inside it. A row
// that is dirty in the cache when that snapshot opens is in NEITHER view once a
// flush lands mid-scan: flush() commits it at a timestamp after the snapshot, so
// the pinned iterator cannot see it, and MarkFlushed then turns it clean, which
// makes it evictable — so the cache lookup misses too. The scan used to read that
// as an orphaned index entry and drop the row silently.
//
// Distinct from the `flushing` commit-window race
// (badgerstore_flushing_commit_window_test.go): there the flush has not committed
// and the overlay is the repair. Here the flush HAS committed — the row is
// durable, just not in this transaction's past.
//
// newFlushParkStore keeps the background flush loop out of the way (interval 1h);
// CacheCapacity 1 makes the post-flush eviction certain rather than likely.

const scanFlushEvictLabel = uint16(7)
const scanFlushEvictType = uint16(9)

func TestForEachNodeByLabel_NoDropWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })

	const n = 64
	for i := 0; i < n; i++ {
		putTestNode(t, bs, int64(1000+i), scanFlushEvictLabel, nil)
	}
	// An id in the label index with no row anywhere: the scan must still skip
	// it. A repair that re-reads without the liveness check would either fail
	// the scan or emit a phantom.
	orphan := types.NodeID(snowflake.ID(999999))
	bs.idxMu.Lock()
	bs.labelIdx[scanFlushEvictLabel][orphan] = struct{}{}
	bs.idxMu.Unlock()

	var seen []types.NodeID
	flushed := false
	var flushErr error
	err := bs.ForEachNodeByLabel(scanFlushEvictLabel, QueryOpts{}, func(node *types.Node) bool {
		seen = append(seen, node.ID())
		if !flushed {
			flushed = true
			flushErr = bs.Flush()
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeByLabel: %v", err)
	}
	if flushErr != nil {
		t.Fatalf("Flush mid-scan: %v", flushErr)
	}
	if !flushed {
		t.Fatal("scan emitted no row at all — fixture invalid")
	}
	for _, id := range seen {
		if id == orphan {
			t.Fatal("scan emitted the orphaned label-index entry")
		}
	}
	if len(seen) != n {
		t.Fatalf("scan emitted %d rows, want %d (flush-evict mid-scan dropped rows)", len(seen), n)
	}
}

// flushOnNthGet fires once, immediately before the cache lookup of the nth
// scanned row, so a commit+evict lands strictly inside the scan's pinned
// snapshot window. Used where the scan takes no caller callback.
type flushOnNthGet struct {
	indexpkg.EntityCache[*types.Node]
	nth  int
	seen int
	fire func()
}

func (c *flushOnNthGet) GetNoPromote(id snowflake.ID) (*types.Node, indexpkg.CacheStatus) {
	c.seen++
	if c.seen == c.nth {
		c.fire()
	}
	return c.EntityCache.GetNoPromote(id)
}

func TestCollectNodesBulkParallel_NoDropWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })

	const n = 64
	ids := make([]types.NodeID, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, putTestNode(t, bs, int64(2000+i), scanFlushEvictLabel, nil).ID())
	}

	var flushErr error
	fired := false
	bs.nodeCache = &flushOnNthGet{
		EntityCache: bs.nodeCache,
		nth:         2,
		fire: func() {
			fired = true
			flushErr = bs.Flush()
		},
	}

	got, err := bs.collectNodesBulkParallel(ids)
	if err != nil {
		t.Fatalf("collectNodesBulkParallel: %v", err)
	}
	if flushErr != nil {
		t.Fatalf("Flush mid-scan: %v", flushErr)
	}
	if !fired {
		t.Fatal("flush hook never fired — fixture invalid")
	}
	if len(got) != n {
		t.Fatalf("parallel bulk returned %d nodes, want %d (flush-evict mid-scan dropped rows)", len(got), n)
	}
}

func TestForEachRelBulk_NoDropWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })

	putTestNode(t, bs, 1, scanFlushEvictLabel, nil)
	putTestNode(t, bs, 2, scanFlushEvictLabel, nil)

	const n = 64
	ids := make([]types.RelID, 0, n)
	for i := 0; i < n; i++ {
		ids = append(ids, putTestRel(t, bs, int64(3000+i), scanFlushEvictType, 1, 2).ID())
	}

	seen := 0
	flushed := false
	var flushErr error
	err := bs.forEachRelBulk(ids, func(*types.Relationship) bool {
		seen++
		if !flushed {
			flushed = true
			flushErr = bs.Flush()
		}
		return true
	})
	if err != nil {
		t.Fatalf("forEachRelBulk: %v", err)
	}
	if flushErr != nil {
		t.Fatalf("Flush mid-scan: %v", flushErr)
	}
	if !flushed {
		t.Fatal("scan emitted no row at all — fixture invalid")
	}
	if seen != n {
		t.Fatalf("rel bulk emitted %d rows, want %d (flush-evict mid-scan dropped rows)", seen, n)
	}
}

// --- Stale versions: the same window, for a row UPDATED before the scan. ---
//
// The row's previous version is already in badger (flushed earlier); the new
// version is dirty in the cache when the scan's snapshot opens. A mid-scan flush
// commits the new version after the snapshot and evicts it, so the cache misses
// and the pinned snapshot answers with the PREVIOUS version: the scan returns a
// row the store had already replaced before the scan began.

const scanVersionKey = "v"

// putFlushedThenUpdatedNodes stores n nodes at version 1, flushes them, then
// replaces each with version 2 WITHOUT flushing (dirty in the cache).
func putFlushedThenUpdatedNodes(t *testing.T, bs *Store, base int64, n int) []types.NodeID {
	t.Helper()
	ids := make([]types.NodeID, 0, n)
	for i := 0; i < n; i++ {
		node := types.NewNode(types.NodeID(snowflake.ID(base+int64(i))), scanFlushEvictLabel, nil)
		if err := node.SetProperty(scanVersionKey, int64(1)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, node.ID())
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush v1: %v", err)
	}
	for _, id := range ids {
		node := types.NewNode(id, scanFlushEvictLabel, nil)
		if err := node.SetProperty(scanVersionKey, int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceNode(node); err != nil {
			t.Fatalf("ReplaceNode: %v", err)
		}
	}
	return ids
}

func nodeScanVersion(t *testing.T, n *types.Node) int64 {
	t.Helper()
	v, ok := n.GetProperty(scanVersionKey)
	if !ok {
		t.Fatalf("node %d has no %q property", n.ID(), scanVersionKey)
	}
	return v.(int64)
}

func relScanVersion(t *testing.T, r *types.Relationship) int64 {
	t.Helper()
	v, ok := r.GetProperty(scanVersionKey)
	if !ok {
		t.Fatalf("rel %d has no %q property", r.ID(), scanVersionKey)
	}
	return v.(int64)
}

func TestForEachNodeByLabel_NoStaleVersionWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	const n = 64
	putFlushedThenUpdatedNodes(t, bs, 4000, n)

	var stale, seen int
	flushed := false
	err := bs.ForEachNodeByLabel(scanFlushEvictLabel, QueryOpts{}, func(node *types.Node) bool {
		seen++
		if nodeScanVersion(t, node) != 2 {
			stale++
		}
		if !flushed {
			flushed = true
			if err := bs.Flush(); err != nil {
				t.Errorf("Flush mid-scan: %v", err)
			}
		}
		return true
	})
	if err != nil {
		t.Fatalf("ForEachNodeByLabel: %v", err)
	}
	if seen != n || stale != 0 {
		t.Fatalf("scan emitted %d rows, %d of them the replaced version 1; want %d rows, 0 stale", seen, stale, n)
	}
}

func TestCollectNodesBulkParallel_NoStaleVersionWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	const n = 64
	ids := putFlushedThenUpdatedNodes(t, bs, 5000, n)

	fired := false
	bs.nodeCache = &flushOnNthGet{
		EntityCache: bs.nodeCache,
		nth:         2,
		fire: func() {
			fired = true
			if err := bs.Flush(); err != nil {
				t.Errorf("Flush mid-scan: %v", err)
			}
		},
	}
	got, err := bs.collectNodesBulkParallel(ids)
	if err != nil {
		t.Fatalf("collectNodesBulkParallel: %v", err)
	}
	if !fired {
		t.Fatal("flush hook never fired — fixture invalid")
	}
	stale := 0
	for _, node := range got {
		if nodeScanVersion(t, node) != 2 {
			stale++
		}
	}
	if len(got) != n || stale != 0 {
		t.Fatalf("parallel bulk returned %d nodes, %d of them the replaced version 1; want %d, 0 stale", len(got), stale, n)
	}
}

func TestForEachRelBulk_NoStaleVersionWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	putTestNode(t, bs, 1, scanFlushEvictLabel, nil)
	putTestNode(t, bs, 2, scanFlushEvictLabel, nil)

	const n = 64
	ids := make([]types.RelID, 0, n)
	for i := 0; i < n; i++ {
		r := types.NewRelationship(types.RelID(snowflake.ID(6000+i)), scanFlushEvictType, 1, 2)
		if err := r.SetProperty(scanVersionKey, int64(1)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		ids = append(ids, r.ID())
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush v1: %v", err)
	}
	for _, id := range ids {
		r := types.NewRelationship(id, scanFlushEvictType, 1, 2)
		if err := r.SetProperty(scanVersionKey, int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceRelationship(r); err != nil {
			t.Fatalf("ReplaceRelationship: %v", err)
		}
	}

	var seen, stale int
	flushed := false
	err := bs.forEachRelBulk(ids, func(r *types.Relationship) bool {
		seen++
		if relScanVersion(t, r) != 2 {
			stale++
		}
		if !flushed {
			flushed = true
			if err := bs.Flush(); err != nil {
				t.Errorf("Flush mid-scan: %v", err)
			}
		}
		return true
	})
	if err != nil {
		t.Fatalf("forEachRelBulk: %v", err)
	}
	if seen != n || stale != 0 {
		t.Fatalf("rel bulk emitted %d rows, %d of them the replaced version 1; want %d rows, 0 stale", seen, stale, n)
	}
}

// --- NodesAsOf / RelsAsOf: the current-row arm reads the same pinned snapshot. ---
//
// Half the rows were flushed at version 1 then replaced (version 2, dirty); the
// other half were created and never flushed. A flush at candidate 1 commits and
// evicts all of them. The current-row arm then misses the cache and reads the
// scan's snapshot: the replaced rows answer with version 1, the new rows are
// absent. The arm also fills the cache from that snapshot, leaving version 1
// cached for every later point read after the scan has ended.

func TestNodesAsOf_NoDropOrStaleWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	const half = 16
	pin := types.Instant(1000)
	mk := func(id int64, v int64, txFrom types.Instant) *types.Node {
		node := types.NewNode(types.NodeID(snowflake.ID(id)), scanFlushEvictLabel, nil)
		node.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom})
		if err := node.SetProperty(scanVersionKey, v); err != nil {
			t.Fatal(err)
		}
		return node
	}
	var ids []types.NodeID
	for i := 0; i < half; i++ {
		if err := bs.PutNode(mk(7000+int64(i), 1, 100)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, types.NodeID(snowflake.ID(7000+int64(i))))
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < half; i++ {
		if err := bs.ReplaceNode(mk(7000+int64(i), 2, 200)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < half; i++ {
		if err := bs.PutNode(mk(8000+int64(i), 2, 200)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, types.NodeID(snowflake.ID(8000+int64(i))))
	}

	fired := false
	bs.bulkAsOfScanTestHook = func(idx int) {
		if idx == 1 && !fired {
			fired = true
			if err := bs.Flush(); err != nil {
				t.Errorf("Flush mid-scan: %v", err)
			}
		}
	}
	got, err := bs.NodesAsOf(pin)
	bs.bulkAsOfScanTestHook = nil
	if err != nil {
		t.Fatalf("NodesAsOf: %v", err)
	}
	if !fired {
		t.Fatal("scan hook never fired — fixture invalid")
	}
	stale := 0
	for _, node := range got {
		if nodeScanVersion(t, node) != 2 {
			stale++
		}
	}
	if len(got) != 2*half || stale != 0 {
		t.Fatalf("NodesAsOf returned %d nodes, %d stale; want %d, 0 stale", len(got), stale, 2*half)
	}
	for _, id := range ids { // Peek: a GetNode miss would refill and hide it
		if node, st := bs.nodeCache.Peek(id.SnowflakeID()); st == indexpkg.CacheHit && nodeScanVersion(t, node) != 2 {
			t.Fatalf("node %d cached at version %d after the scan, want 2 (scan filled the cache from a stale snapshot)", id, nodeScanVersion(t, node))
		}
	}
}

func TestRelsAsOf_NoDropOrStaleWhenFlushEvictsMidScan(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	for _, id := range []int64{1, 2} {
		node := types.NewNode(types.NodeID(snowflake.ID(id)), scanFlushEvictLabel, nil)
		node.SetTemporal(&types.TemporalMetadata{TxFrom: 10})
		if err := bs.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	const half = 16
	pin := types.Instant(1000)
	mk := func(id int64, v int64, txFrom types.Instant) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), scanFlushEvictType, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom})
		if err := r.SetProperty(scanVersionKey, v); err != nil {
			t.Fatal(err)
		}
		return r
	}
	var ids []types.RelID
	for i := 0; i < half; i++ {
		if err := bs.PutRelationship(mk(7000+int64(i), 1, 100)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, types.RelID(snowflake.ID(7000+int64(i))))
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < half; i++ {
		if err := bs.ReplaceRelationship(mk(7000+int64(i), 2, 200)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < half; i++ {
		if err := bs.PutRelationship(mk(8000+int64(i), 2, 200)); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, types.RelID(snowflake.ID(8000+int64(i))))
	}

	fired := false
	bs.bulkAsOfScanTestHook = func(idx int) {
		if idx == 1 && !fired {
			fired = true
			if err := bs.Flush(); err != nil {
				t.Errorf("Flush mid-scan: %v", err)
			}
		}
	}
	got, err := bs.RelsAsOf(pin)
	bs.bulkAsOfScanTestHook = nil
	if err != nil {
		t.Fatalf("RelsAsOf: %v", err)
	}
	if !fired {
		t.Fatal("scan hook never fired — fixture invalid")
	}
	stale := 0
	for _, r := range got {
		if relScanVersion(t, r) != 2 {
			stale++
		}
	}
	if len(got) != 2*half || stale != 0 {
		t.Fatalf("RelsAsOf returned %d rels, %d stale; want %d, 0 stale", len(got), stale, 2*half)
	}
	for _, id := range ids { // Peek: a GetRelationship miss would refill and hide it
		if r, st := bs.relCache.Peek(id.SnowflakeID()); st == indexpkg.CacheHit && relScanVersion(t, r) != 2 {
			t.Fatalf("rel %d cached at version %d after the scan, want 2 (scan filled the cache from a stale snapshot)", id, relScanVersion(t, r))
		}
	}
}

// --- Point reads: a cache fill from a snapshot older than the last flush. ---
//
// GetNode misses the cache, reads version 1 from badger, and then fills the
// cache with it. If, between that read and the fill, a writer replaces the row
// (version 2), a flush commits it and the cache evicts it, the fill inserts
// version 1 as a clean entry: every later GetNode returns the replaced version
// until that entry happens to be evicted.

// hookBeforeFill fires once, immediately before the first cache fill.
type hookBeforeFill[V any] struct {
	indexpkg.EntityCache[V]
	fired bool
	fire  func()
}

func (c *hookBeforeFill[V]) before() {
	if !c.fired {
		c.fired = true
		c.fire()
	}
}

func (c *hookBeforeFill[V]) LoadCleanAt(id snowflake.ID, v V, epoch uint64) bool {
	c.before()
	return c.EntityCache.LoadCleanAt(id, v, epoch)
}

func TestGetNode_NoStaleCacheFillAfterConcurrentFlushEvict(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	mk := func(id int64, v int64) *types.Node {
		node := types.NewNode(types.NodeID(snowflake.ID(id)), scanFlushEvictLabel, nil)
		if err := node.SetProperty(scanVersionKey, v); err != nil {
			t.Fatal(err)
		}
		return node
	}
	target := mk(9001, 1)
	if err := bs.PutNode(target); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(mk(9002, 1)); err != nil { // evicts target once flushed
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, st := bs.nodeCache.Peek(target.ID().SnowflakeID()); st != indexpkg.CacheMiss {
		t.Fatal("fixture: target still cached")
	}

	hook := &hookBeforeFill[*types.Node]{EntityCache: bs.nodeCache}
	hook.fire = func() {
		if err := bs.ReplaceNode(mk(9001, 2)); err != nil {
			t.Errorf("ReplaceNode: %v", err)
		}
		if err := bs.PutNode(mk(9003, 1)); err != nil { // evicts version 2 once flushed
			t.Errorf("PutNode: %v", err)
		}
		if err := bs.Flush(); err != nil {
			t.Errorf("Flush: %v", err)
		}
	}
	bs.nodeCache = hook

	if _, err := bs.GetNode(target.ID()); err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if !hook.fired {
		t.Fatal("fill hook never fired — fixture invalid")
	}
	got, err := bs.GetNode(target.ID())
	if err != nil {
		t.Fatalf("GetNode after: %v", err)
	}
	if v := nodeScanVersion(t, got); v != 2 {
		t.Fatalf("GetNode after the concurrent replace = version %d, want 2 (cache filled from a stale read)", v)
	}
}

func TestGetRelationship_NoStaleCacheFillAfterConcurrentFlushEvict(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 1 })
	putTestNode(t, bs, 1, scanFlushEvictLabel, nil)
	putTestNode(t, bs, 2, scanFlushEvictLabel, nil)
	mk := func(id int64, v int64) *types.Relationship {
		r := types.NewRelationship(types.RelID(snowflake.ID(id)), scanFlushEvictType, 1, 2)
		if err := r.SetProperty(scanVersionKey, v); err != nil {
			t.Fatal(err)
		}
		return r
	}
	target := mk(9001, 1)
	if err := bs.PutRelationship(target); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelationship(mk(9002, 1)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, st := bs.relCache.Peek(target.ID().SnowflakeID()); st != indexpkg.CacheMiss {
		t.Fatal("fixture: target still cached")
	}

	hook := &hookBeforeFill[*types.Relationship]{EntityCache: bs.relCache}
	hook.fire = func() {
		if err := bs.ReplaceRelationship(mk(9001, 2)); err != nil {
			t.Errorf("ReplaceRelationship: %v", err)
		}
		if err := bs.PutRelationship(mk(9003, 1)); err != nil {
			t.Errorf("PutRelationship: %v", err)
		}
		if err := bs.Flush(); err != nil {
			t.Errorf("Flush: %v", err)
		}
	}
	bs.relCache = hook

	if _, err := bs.GetRelationship(target.ID()); err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if !hook.fired {
		t.Fatal("fill hook never fired — fixture invalid")
	}
	got, err := bs.GetRelationship(target.ID())
	if err != nil {
		t.Fatalf("GetRelationship after: %v", err)
	}
	if v := relScanVersion(t, got); v != 2 {
		t.Fatalf("GetRelationship after the concurrent replace = version %d, want 2 (cache filled from a stale read)", v)
	}
}

// TestForEachNodeByLabel_FlushEvictMidScanOnDiskAndAfterRestart runs the
// mid-scan flush on a disk-backed store (the tests above are in-memory), then
// reopens it: the scan must be complete and current before the restart, and the
// flush it raced must have persisted every row at its latest version.
func TestForEachNodeByLabel_FlushEvictMidScanOnDiskAndAfterRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, FlushInterval: time.Hour, CacheCapacity: 1}
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const updated, created = 32, 32
	putFlushedThenUpdatedNodes(t, bs, 9200, updated)
	for i := 0; i < created; i++ { // never flushed before the scan
		node := types.NewNode(types.NodeID(snowflake.ID(9300+i)), scanFlushEvictLabel, nil)
		if err := node.SetProperty(scanVersionKey, int64(2)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(node); err != nil {
			t.Fatal(err)
		}
	}
	scan := func(bs *Store, flushAtFirst bool) (seen, stale int) {
		t.Helper()
		flushed := !flushAtFirst
		err := bs.ForEachNodeByLabel(scanFlushEvictLabel, QueryOpts{}, func(node *types.Node) bool {
			seen++
			if nodeScanVersion(t, node) != 2 {
				stale++
			}
			if !flushed {
				flushed = true
				if err := bs.Flush(); err != nil {
					t.Errorf("Flush mid-scan: %v", err)
				}
			}
			return true
		})
		if err != nil {
			t.Fatalf("ForEachNodeByLabel: %v", err)
		}
		return seen, stale
	}
	if seen, stale := scan(bs, true); seen != updated+created || stale != 0 {
		t.Fatalf("before restart: scan emitted %d rows, %d stale; want %d, 0 stale", seen, stale, updated+created)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if seen, stale := scan(bs, false); seen != updated+created || stale != 0 {
		t.Fatalf("after restart: scan emitted %d rows, %d stale; want %d, 0 stale", seen, stale, updated+created)
	}
}
