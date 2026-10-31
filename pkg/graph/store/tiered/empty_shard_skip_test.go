package tiered

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// catalogEntryNoRows reads the mark from the saved catalog, as a restart would.
func (ts *Store) catalogEntryNoRows(t *testing.T, cfg Config, name string) bool {
	t.Helper()
	raw, err := os.ReadFile(ts.catalog.path)
	if err != nil {
		t.Fatal(err)
	}
	var saved ShardCatalog
	if err := json.Unmarshal(raw, &saved); err != nil {
		t.Fatal(err)
	}
	for _, e := range saved.Shards {
		if e.Name == name {
			return e.NoRows
		}
	}
	t.Fatalf("no catalog entry %s under %s", name, filepath.Base(cfg.DataDir))
	return false
}

// emptyColdShard makes a store whose first hot shard, empty, was rotated out,
// demoted and closed, with one reference edge to read beside it.
func emptyColdShard(t *testing.T, cfg Config) (*Store, *EventShard, types.NodeID) {
	t.Helper()
	ts := openMintStore(t, cfg)
	ng := tieredNodeGen(t)
	name := hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	return ts, es, putMintNode(t, ts, types.NodeID(ng.Generate()), mintCaseTok)
}

// TestEmptyColdShard_FanOutSkipsIt: a closed cold shard that holds no row is
// neither opened nor counted by a depth-all read.
func TestEmptyColdShard_FanOutSkipsIt(t *testing.T) {
	t.Parallel()
	ts, es, a := emptyColdShard(t, mintCfg(t.TempDir(), true))
	opens, checkouts := es.coldOpens.Load(), es.accesses[2].Load()
	for range 3 {
		if _, err := ts.OutgoingRelationships(a, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := ts.AllNodes(QueryOpts{Depth: DepthAll}); err != nil {
			t.Fatal(err)
		}
	}
	if got := es.coldOpens.Load() - opens; got != 0 {
		t.Errorf("empty cold shard opened %d times", got)
	}
	if got := es.accesses[2].Load() - checkouts; got != 0 {
		t.Errorf("empty cold shard checked out %d times", got)
	}
}

// TestEmptyColdShard_WriteComesBack: a write to a skipped shard's window wakes
// it, persistently before the write: the row reads back, and the catalog a crash
// would reopen no longer marks it.
func TestEmptyColdShard_WriteComesBack(t *testing.T) {
	t.Parallel()
	cfg := mintCfg(t.TempDir(), true)
	ts := openMintStore(t, cfg)
	ng := tieredNodeGen(t)
	late := types.NodeID(ng.Generate())
	name := hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	if !es.noRows.Load() {
		t.Fatal("setup: the shard is not marked empty")
	}
	if !ts.catalogEntryNoRows(t, cfg, name) {
		t.Fatal("setup: the saved catalog does not mark it")
	}
	putMintNode(t, ts, late, mintSignalTok)
	if ts.catalogEntryNoRows(t, cfg, name) {
		t.Error("the saved catalog still marks the written shard: a crash would hide the row")
	}
	nodes, err := ts.AllNodes(QueryOpts{Depth: DepthAll})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].ID() != late {
		t.Fatalf("AllNodes = %d rows, want the late node", len(nodes))
	}
	closeColdStores(t, ts)
	if es.noRows.Load() {
		t.Error("a shard holding a row is marked empty after its close")
	}
}

// TestEmptyColdShard_OnlyEmptyIsSkipped: a shard whose only row is history (its
// node deleted) stays in the fan-out, and the mark of a truly empty shard
// survives a reopen.
func TestEmptyColdShard_OnlyEmptyIsSkipped(t *testing.T) {
	t.Parallel()
	cfg := mintCfg(t.TempDir(), true)
	ts := openMintStore(t, cfg)
	ng := tieredNodeGen(t)
	emptyName := hotShardName(ts)
	rotate(t, ts)
	histName := hotShardName(ts)
	id := putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	if err := ts.PutNodeVersion(id, 0, types.NewNode(id, mintSignalTok, nil)); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteNode(id); err != nil {
		t.Fatal(err)
	}
	rotate(t, ts)
	demoteToCold(ts, emptyName)
	demoteToCold(ts, histName)
	closeColdStores(t, ts)
	ts.mu.RLock()
	empty, hist := ts.eventShards[emptyName], ts.eventShards[histName]
	ts.mu.RUnlock()
	if !empty.noRows.Load() {
		t.Error("the empty shard is not marked")
	}
	if hist.noRows.Load() {
		t.Error("a shard holding history is marked empty")
	}
	if h, err := ts.GetNodeHistory(id); err != nil || len(h) != 1 {
		t.Errorf("GetNodeHistory = %d rows, %v; want 1", len(h), err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts = openMintStore(t, cfg)
	ts.mu.RLock()
	empty = ts.eventShards[emptyName]
	ts.mu.RUnlock()
	if !empty.noRows.Load() {
		t.Error("the mark did not survive the reopen")
	}
}

// TestEmptyColdShard_SweepFreesItsSlot: an empty cold shard left open, its last
// access fresh, is closed by the sweep, so fan-out reads stop holding a cap slot
// for it. A shard holding a row, as fresh, stays open.
func TestEmptyColdShard_SweepFreesItsSlot(t *testing.T) {
	t.Parallel()
	ts := openMintStore(t, mintCfg(t.TempDir(), true))
	ng := tieredNodeGen(t)
	emptyName := hotShardName(ts)
	rotate(t, ts)
	fullName := hotShardName(ts)
	putMintNode(t, ts, types.NodeID(ng.Generate()), mintSignalTok)
	rotate(t, ts)
	demoteToCold(ts, emptyName)
	demoteToCold(ts, fullName)
	ts.mu.RLock()
	empty, full := ts.eventShards[emptyName], ts.eventShards[fullName]
	ts.mu.RUnlock()
	for _, es := range []*EventShard{empty, full} {
		es.lastAccess.Store(time.Now().UnixMilli())
	}
	ts.closeIdleShards()
	open := func(es *EventShard) bool {
		es.shardMu.Lock()
		defer es.shardMu.Unlock()
		return es.store != nil
	}
	if !empty.noRows.Load() || open(empty) {
		t.Errorf("empty shard: marked %v, open %v; want marked and closed", empty.noRows.Load(), open(empty))
	}
	if !open(full) {
		t.Error("a fresh shard holding a row was closed")
	}
}

// TestEmptyColdShard_WakeSaveFailureRefusesTheWrite: if the catalog cannot record
// the wake, the write fails instead of landing in a shard a restart would skip.
func TestEmptyColdShard_WakeSaveFailureRefusesTheWrite(t *testing.T) {
	t.Parallel()
	ts := openMintStore(t, mintCfg(t.TempDir(), true))
	ng := tieredNodeGen(t)
	late := types.NodeID(ng.Generate())
	name := hotShardName(ts)
	rotate(t, ts)
	demoteToCold(ts, name)
	closeColdStores(t, ts)
	saved := ts.catalog.path
	ts.catalog.path = filepath.Join(t.TempDir(), "missing", "catalog.json")
	err := ts.PutNode(types.NewNode(late, mintSignalTok, nil))
	ts.catalog.path = saved
	if err == nil {
		t.Fatal("the write succeeded with the wake unsaved")
	}
	ts.mu.RLock()
	es := ts.eventShards[name]
	ts.mu.RUnlock()
	if !es.noRows.Load() {
		t.Error("the mark was dropped although the save failed")
	}
	if err := ts.catalog.Save(); err != nil {
		t.Fatal(err)
	}
	if !ts.catalogEntryNoRows(t, Config{}, name) {
		t.Error("the catalog dropped the mark although the save failed")
	}
}
