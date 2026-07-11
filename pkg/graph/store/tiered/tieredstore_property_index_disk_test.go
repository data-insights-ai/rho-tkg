package tiered

import (
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// ADR-0005 §3.3 — PropertyIndexOnDisk pass-through to tiered.Config.
//
// Scope reminder (see tieredstore.go Config.PropertyIndexOnDisk doc): the flag
// does NOT change WHERE property indexes live (reference-shard-only, unchanged
// — CreatePropertyIndex still rejects event labels). It only changes HOW the
// reference shard's badger.Store answers property-index reads: RAM maps
// (default) vs the persisted 0x0A keyspace. It is passed through badgerCfg to
// EVERY shard for uniformity (event shards never build a property index, so
// it is a harmless no-op there).

// tieredPropIdxDiskCfg returns a disk-backed Config with manual flushing (for
// deterministic before/after-flush assertions) and PropertyIndexOnDisk set.
func tieredPropIdxDiskCfg(dir string) Config {
	return Config{
		DataDir:             dir,
		RefLabels:           []string{"Case"},
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1, // manual flush
		PropertyIndexOnDisk: true,
	}
}

// nodeIDsSortedInt64 sorts a node-ID slice and renders it as int64s for
// exact-set string comparison, mirroring the convention already used in this
// package's other tuning/parity tests.
func nodeIDsSortedInt64(nodes []*types.Node) []int64 {
	ids := make([]int64, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, int64(n.ID().SnowflakeID()))
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	return ids
}

// --- (a) config pass-through reaches every shard ---

// TestTieredPropertyIndexOnDisk_ReachesEveryShard proves Config.
// PropertyIndexOnDisk reaches the reference, hot, and (post-rotation) warm
// shard's badger.Config the same way the other footprint knobs do
// (TestTieredFootprintKnobsReachBadgerOptions), and that the zero-value
// Config (flag unset) leaves every shard in the default RAM-backed mode —
// the backward-compatible default this pass-through must not disturb.
func TestTieredPropertyIndexOnDisk_ReachesEveryShard(t *testing.T) {
	t.Parallel()

	t.Run("enabled reaches reference, hot, and warm shards", func(t *testing.T) {
		t.Parallel()
		ts, err := New(Config{
			InMemory:            true,
			RefLabels:           []string{"Case"},
			ShardWindow:         7 * 24 * time.Hour,
			FlushInterval:       1<<63 - 1,
			PropertyIndexOnDisk: true,
		})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		defer ts.Close()

		if !ts.RefShardForTest().PropertyIndexOnDiskForTest() {
			t.Error("reference shard did not inherit PropertyIndexOnDisk=true")
		}
		ts.MuForTest().RLock()
		hot := ts.HotShardForTest().Store()
		ts.MuForTest().RUnlock()
		if !hot.PropertyIndexOnDiskForTest() {
			t.Error("hot shard did not inherit PropertyIndexOnDisk=true")
		}

		time.Sleep(2 * time.Millisecond)
		if err := ts.RotateHotShard(); err != nil {
			t.Fatalf("rotate: %v", err)
		}
		var warmChecked bool
		ts.MuForTest().RLock()
		for _, es := range ts.EventShardsForTest() {
			if es.Tier() == TierWarm && es.Store() != nil {
				if !es.Store().PropertyIndexOnDiskForTest() {
					t.Error("warm shard did not inherit PropertyIndexOnDisk=true")
				}
				warmChecked = true
			}
		}
		ts.MuForTest().RUnlock()
		if !warmChecked {
			t.Fatal("no open warm shard found to verify PropertyIndexOnDisk passthrough")
		}
	})

	t.Run("zero-value Config defaults every shard to RAM mode", func(t *testing.T) {
		t.Parallel()
		ts := newTestTieredStore(t) // Config{} equivalent — PropertyIndexOnDisk unset
		if ts.RefShardForTest().PropertyIndexOnDiskForTest() {
			t.Error("reference shard defaulted to disk mode with PropertyIndexOnDisk unset")
		}
		ts.MuForTest().RLock()
		hot := ts.HotShardForTest().Store()
		ts.MuForTest().RUnlock()
		if hot.PropertyIndexOnDiskForTest() {
			t.Error("hot shard defaulted to disk mode with PropertyIndexOnDisk unset")
		}
	})
}

// --- (b) reopen persistence through the reference shard ---

// TestTieredPropertyIndexOnDisk_ReopenPersistence mirrors the badger-level
// TestPropertyIndexOnDisk_ReopenPersistence: create a reference property
// index and reference nodes under PropertyIndexOnDisk, close, reopen with the
// SAME Config, and assert NodesByLabelAndProperty answers correctly WITHOUT
// any in-memory index rebuild pass — the reference shard's own badger.Store
// reloads its persisted property-key registry and 0x0A rows on its own.
func TestTieredPropertyIndexOnDisk_ReopenPersistence(t *testing.T) {
	dir := t.TempDir()
	cfg := tieredPropIdxDiskCfg(dir)

	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	labelReg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(labelReg)
	caseTok, err := labelReg.GetOrCreate("Case")
	if err != nil {
		t.Fatalf("GetOrCreate label: %v", err)
	}

	// CreatePropertyIndex in disk mode requires a wired property-key registry
	// (CLAUDE.md / badgerstore.go PropertyIndexOnDisk doc) — the tiered store
	// has no Config.PropertyKeyRegistry field (unlike badger.Config), so tests
	// wire it post-construction via SetPropertyKeyRegistry, exactly like a
	// graph-layer Open would (core.go wires it the same way for every backend).
	pkReg := registrypkg.NewPropertyKeyRegistry()
	ts.SetPropertyKeyRegistry(pkReg)

	if err := ts.CreatePropertyIndex(caseTok, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	gen := tieredNodeGen(t)
	var ids []types.NodeID
	for i := int64(1); i <= 5; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		if err := n.SetProperty("age", 30+i); err != nil {
			t.Fatalf("SetProperty(%d): %v", i, err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		ids = append(ids, n.ID())
	}

	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen with the SAME Config — no explicit registry wiring. The
	// reference shard's own badger.Store rehydrates its persisted
	// property-key registry from meta (mirroring the badger-level restart
	// test) and answers from the persisted 0x0A keyspace, not a rebuilt map.
	ts2, err := New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer ts2.Close()

	for i, id := range ids {
		age := int64(30 + i + 1)
		got, err := ts2.NodesByLabelAndProperty(caseTok, "age", age, QueryOpts{})
		if err != nil {
			t.Fatalf("NodesByLabelAndProperty(age=%d): %v", age, err)
		}
		gotIDs := nodeIDsSortedInt64(got)
		want := fmt.Sprintf("[%d]", id.SnowflakeID())
		if fmt.Sprint(gotIDs) != want {
			t.Fatalf("reopen: age=%d got=%v want=%s", age, gotIDs, want)
		}
	}

	// Negative: a value nobody wrote returns empty, not a phantom.
	got, err := ts2.NodesByLabelAndProperty(caseTok, "age", int64(999), QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabelAndProperty(phantom): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("phantom age value returned %d matches, want 0", len(got))
	}
}

// --- (c) the reference shard's persisted keyspace, inspected directly ---

// TestTieredPropertyIndexOnDisk_PersistedKeyspace bypasses the query door and
// inspects the reference shard's raw 0x0A keyspace directly (mirroring the
// badger-level crash-recovery test's raw-scan technique), proving the entries
// physically live on disk rather than merely being answerable through some
// RAM structure that happens to agree after a flush.
func TestTieredPropertyIndexOnDisk_PersistedKeyspace(t *testing.T) {
	t.Parallel()
	ts, err := New(Config{
		InMemory:            true,
		RefLabels:           []string{"Case"},
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1,
		PropertyIndexOnDisk: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()

	labelReg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(labelReg)
	caseTok, _ := labelReg.GetOrCreate("Case")

	pkReg := registrypkg.NewPropertyKeyRegistry()
	ageTok, err := pkReg.GetOrCreate("age")
	if err != nil {
		t.Fatalf("GetOrCreate propkey: %v", err)
	}
	ts.SetPropertyKeyRegistry(pkReg)

	if err := ts.CreatePropertyIndex(caseTok, "age"); err != nil {
		t.Fatalf("CreatePropertyIndex: %v", err)
	}

	gen := tieredNodeGen(t)
	want := make(map[int64]struct{})
	for i := int64(1); i <= 5; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		if err := n.SetProperty("age", 30+i); err != nil {
			t.Fatalf("SetProperty(%d): %v", i, err)
		}
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", i, err)
		}
		want[int64(n.ID().SnowflakeID())] = struct{}{}
	}
	if err := ts.RefShardForTest().Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	got := make(map[int64]struct{})
	if err := ts.RefShardForTest().DBForTest().View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		prefix := storeutil.PropertyIndexTokenPrefix(ageTok)
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			got[int64(storeutil.PropertyIndexNodeIDFromKey(key))] = struct{}{}
		}
		return nil
	}); err != nil {
		t.Fatalf("raw index scan: %v", err)
	}

	if len(got) != len(want) {
		t.Fatalf("raw 0x0A row count = %d, want %d (got=%v want=%v)", len(got), len(want), got, want)
	}
	for id := range want {
		if _, ok := got[id]; !ok {
			t.Errorf("missing persisted 0x0A row for node %d", id)
		}
	}
}

// --- (d) scope boundary: event labels still reject ---

// TestTieredPropertyIndexOnDisk_EventLabelStillRejects proves PropertyIndexOnDisk
// does not widen the reference-shard-only scope: CreatePropertyIndex and
// DropPropertyIndex on an event label still fail closed with
// ErrEventPropertyIndex, identical to RAM mode.
func TestTieredPropertyIndexOnDisk_EventLabelStillRejects(t *testing.T) {
	t.Parallel()
	ts, err := New(Config{
		InMemory:            true,
		RefLabels:           []string{"Case"}, // "Signal" below is NOT listed -> event class
		ShardWindow:         7 * 24 * time.Hour,
		FlushInterval:       1<<63 - 1,
		PropertyIndexOnDisk: true,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts.Close()

	labelReg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(labelReg)
	_, _ = labelReg.GetOrCreate("Case")
	signalTok, _ := labelReg.GetOrCreate("Signal")

	pkReg := registrypkg.NewPropertyKeyRegistry()
	ts.SetPropertyKeyRegistry(pkReg)

	if err := ts.CreatePropertyIndex(signalTok, "severity"); !errors.Is(err, ErrEventPropertyIndex) {
		t.Fatalf("CreatePropertyIndex(event label) = %v, want ErrEventPropertyIndex", err)
	}
	if err := ts.DropPropertyIndex(signalTok, "severity"); !errors.Is(err, ErrEventPropertyIndex) {
		t.Fatalf("DropPropertyIndex(event label) = %v, want ErrEventPropertyIndex", err)
	}
}

// --- (e) flag on/off equivalence ---

// TestTieredPropertyIndexOnDisk_EqualityParityWithRAMMode drives two tiered
// stores — one RAM-backed (default), one disk-backed — through the IDENTICAL
// mutation sequence (puts, a property update via ReplaceNode, a delete) and
// asserts NodesByLabelAndProperty agrees after every step. The two arms are
// parallel implementations of one contract (mirrors the badger-level
// TestPropertyIndexOnDisk_EqualityParityWithMemoryMode).
func TestTieredPropertyIndexOnDisk_EqualityParityWithRAMMode(t *testing.T) {
	t.Parallel()

	build := func(t *testing.T, onDisk bool) (*Store, uint16) {
		t.Helper()
		ts, err := New(Config{
			InMemory:            true,
			RefLabels:           []string{"Case"},
			ShardWindow:         7 * 24 * time.Hour,
			FlushInterval:       1<<63 - 1,
			PropertyIndexOnDisk: onDisk,
		})
		if err != nil {
			t.Fatalf("New(onDisk=%v): %v", onDisk, err)
		}
		t.Cleanup(func() { _ = ts.Close() })
		labelReg := registrypkg.NewLabelRegistry()
		ts.SetLabelRegistry(labelReg)
		caseTok, _ := labelReg.GetOrCreate("Case")
		pkReg := registrypkg.NewPropertyKeyRegistry()
		ts.SetPropertyKeyRegistry(pkReg)
		if err := ts.CreatePropertyIndex(caseTok, "status"); err != nil {
			t.Fatalf("CreatePropertyIndex(onDisk=%v): %v", onDisk, err)
		}
		return ts, caseTok
	}

	ram, ramTok := build(t, false)
	disk, diskTok := build(t, true)

	genRAM := tieredNodeGen(t)
	genDisk := newTestGen(t, 2) // distinct snowflake node id -> independent ID stream

	type node struct{ ram, disk *types.Node }
	var nodes []node
	for i := 0; i < 4; i++ {
		nr := types.NewNode(types.NodeID(genRAM.Generate()), ramTok, nil)
		nd := types.NewNode(types.NodeID(genDisk.Generate()), diskTok, nil)
		status := "open"
		if i%2 == 1 {
			status = "closed"
		}
		if err := nr.SetProperty("status", status); err != nil {
			t.Fatal(err)
		}
		if err := nd.SetProperty("status", status); err != nil {
			t.Fatal(err)
		}
		if err := ram.PutNode(nr); err != nil {
			t.Fatalf("ram PutNode: %v", err)
		}
		if err := disk.PutNode(nd); err != nil {
			t.Fatalf("disk PutNode: %v", err)
		}
		nodes = append(nodes, node{nr, nd})
	}

	assertParity := func(t *testing.T, step string, value any) {
		t.Helper()
		ramNodes, err := ram.NodesByLabelAndProperty(ramTok, "status", value, QueryOpts{})
		if err != nil {
			t.Fatalf("%s: ram query: %v", step, err)
		}
		diskNodes, err := disk.NodesByLabelAndProperty(diskTok, "status", value, QueryOpts{})
		if err != nil {
			t.Fatalf("%s: disk query: %v", step, err)
		}
		if len(ramNodes) != len(diskNodes) {
			t.Fatalf("%s: value=%v ram count=%d disk count=%d (ram=%v disk=%v)",
				step, value, len(ramNodes), len(diskNodes), nodeIDsSortedInt64(ramNodes), nodeIDsSortedInt64(diskNodes))
		}
	}

	assertParity(t, "after initial puts", "open")
	assertParity(t, "after initial puts", "closed")

	// Mutate one pair's property value on both arms (ReplaceNode path).
	if err := nodes[0].ram.SetProperty("status", "archived"); err != nil {
		t.Fatalf("ram SetProperty: %v", err)
	}
	if err := nodes[0].disk.SetProperty("status", "archived"); err != nil {
		t.Fatalf("disk SetProperty: %v", err)
	}
	if err := ram.ReplaceNode(nodes[0].ram); err != nil {
		t.Fatalf("ram ReplaceNode: %v", err)
	}
	if err := disk.ReplaceNode(nodes[0].disk); err != nil {
		t.Fatalf("disk ReplaceNode: %v", err)
	}
	assertParity(t, "after property update", "open")
	assertParity(t, "after property update", "archived")

	// Delete one pair.
	if err := ram.DeleteNode(nodes[1].ram.ID()); err != nil {
		t.Fatalf("ram DeleteNode: %v", err)
	}
	if err := disk.DeleteNode(nodes[1].disk.ID()); err != nil {
		t.Fatalf("disk DeleteNode: %v", err)
	}
	assertParity(t, "after delete", "closed")
}
