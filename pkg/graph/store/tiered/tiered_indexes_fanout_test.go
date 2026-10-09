package tiered

// Backlog 10 (ai-soc request 3): the tiered store builds relationship-type
// temporal indexes and composite node indexes on every shard, as sharded does.
// These store-level tests pin the DDL surface (break cases with errors.Is),
// that every shard — reference, archive, every event shard, the hot shard a
// later rotation opens, and a shard reopened from disk — carries the
// definition, and the capability flags against sharded's. The answers
// (prune parity with badger, composite exact sets across rotation and cold
// checkout) are pinned at the graph level in
// internal/core/tiered_indexes_parity_test.go.

import (
	"errors"
	"slices"
	"testing"
	"time"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func relTemporalDDL(t *testing.T, ts *Store) storecontract.RelTypeTemporalIndexCapability {
	t.Helper()
	cap, ok := any(ts).(storecontract.RelTypeTemporalIndexCapability)
	if !ok {
		t.Fatal("tiered.Store does not implement RelTypeTemporalIndexCapability")
	}
	return cap
}

func relTemporalPrune(t *testing.T, ts *Store) storecontract.RelTypeTemporalCandidateCapability {
	t.Helper()
	cap, ok := any(ts).(storecontract.RelTypeTemporalCandidateCapability)
	if !ok {
		t.Fatal("tiered.Store does not implement RelTypeTemporalCandidateCapability")
	}
	return cap
}

func compositeDDL(t *testing.T, ts *Store) (storecontract.CompositePropertyIndexCapability, storecontract.CompositeIndexIntrospectionCapability) {
	t.Helper()
	cap, ok := any(ts).(storecontract.CompositePropertyIndexCapability)
	if !ok {
		t.Fatal("tiered.Store does not implement CompositePropertyIndexCapability")
	}
	intro, ok := any(ts).(storecontract.CompositeIndexIntrospectionCapability)
	if !ok {
		t.Fatal("tiered.Store does not implement CompositeIndexIntrospectionCapability")
	}
	return cap, intro
}

// allShardStoresForTest returns every shard's badger store, opening closed
// cold shards, keyed by shard name. The shards stay pinned until the test ends.
func allShardStoresForTest(t *testing.T, ts *Store) map[string]*BadgerStore {
	t.Helper()
	out := map[string]*BadgerStore{"reference": ts.refShard}
	if archive := ts.refArchive.Load(); archive != nil {
		out["archive"] = archive
	}
	ts.mu.RLock()
	shards := make([]*EventShard, 0, len(ts.eventShards))
	for _, es := range ts.eventShards {
		shards = append(shards, es)
	}
	ts.mu.RUnlock()
	for _, es := range shards {
		s, release, err := es.checkoutStoreForRead(ts)
		if err != nil {
			t.Fatalf("checkout %s: %v", es.name, err)
		}
		out[es.name] = s
		t.Cleanup(release)
	}
	return out
}

func eventShardCount(ts *Store) int {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return len(ts.eventShards)
}

func TestTieredIndexes_CapabilityFlagsMatchSharded(t *testing.T) {
	ts := newTestTieredStore(t)
	ss, err := shardedpkg.New(shardedpkg.Config{InMemory: true, SlotCount: 2})
	if err != nil {
		t.Fatalf("sharded.New: %v", err)
	}
	t.Cleanup(func() { _ = ss.Close() })

	check := func(name string, has func(any) bool) {
		t.Helper()
		if got, want := has(ts), has(ss); got != want {
			t.Errorf("%s: tiered=%v sharded=%v", name, got, want)
		}
	}
	check("RelTypeTemporalIndexCapability", func(s any) bool { _, ok := s.(storecontract.RelTypeTemporalIndexCapability); return ok })
	check("RelTypeTemporalCandidateCapability", func(s any) bool { _, ok := s.(storecontract.RelTypeTemporalCandidateCapability); return ok })
	check("CompositePropertyIndexCapability", func(s any) bool { _, ok := s.(storecontract.CompositePropertyIndexCapability); return ok })
	check("CompositeIndexIntrospectionCapability", func(s any) bool { _, ok := s.(storecontract.CompositeIndexIntrospectionCapability); return ok })
	check("TemporalIndexListingCapability", func(s any) bool { _, ok := s.(storecontract.TemporalIndexListingCapability); return ok })

	tr, sr := storecontract.CapabilitiesOf(ts).IndexAcceleration, storecontract.CapabilitiesOf(ss).IndexAcceleration
	if tr.CompositePropertyIndex != sr.CompositePropertyIndex || tr.TemporalIndex != sr.TemporalIndex {
		t.Errorf("CapabilitiesOf index flags: tiered %+v, sharded %+v", tr, sr)
	}
	if !tr.CompositePropertyIndex {
		t.Error("CapabilitiesOf(tiered).IndexAcceleration.CompositePropertyIndex = false, want true")
	}
}

func TestTieredRelTemporalIndex_DDLBreakCases(t *testing.T) {
	ts := newTestTieredStore(t)
	ddl := relTemporalDDL(t, ts)
	const typ uint16 = 7

	if err := ddl.CreateRelTemporalIndex(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Errorf("create token 0: err = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ddl.DropRelTemporalIndex(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Errorf("drop token 0: err = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ddl.DropRelTemporalIndex(typ); !errors.Is(err, storecontract.ErrTemporalIndexNotFound) {
		t.Errorf("drop before create: err = %v, want ErrTemporalIndexNotFound", err)
	}
	if err := ddl.CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ddl.CreateRelTemporalIndex(typ); !errors.Is(err, storecontract.ErrTemporalIndexExists) {
		t.Errorf("duplicate create: err = %v, want ErrTemporalIndexExists", err)
	}
	listed, err := ts.RelTemporalIndexTypes()
	if err != nil || !slices.Equal(listed, []uint16{typ}) {
		t.Errorf("RelTemporalIndexTypes = %v, %v; want [%d]", listed, err, typ)
	}
	if err := ddl.DropRelTemporalIndex(typ); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := ddl.DropRelTemporalIndex(typ); !errors.Is(err, storecontract.ErrTemporalIndexNotFound) {
		t.Errorf("drop twice: err = %v, want ErrTemporalIndexNotFound", err)
	}
	if listed, err := ts.RelTemporalIndexTypes(); err != nil || len(listed) != 0 {
		t.Errorf("RelTemporalIndexTypes after drop = %v, %v; want empty", listed, err)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := ddl.CreateRelTemporalIndex(typ); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("create after close: err = %v, want ErrStoreClosed", err)
	}
	if err := ddl.DropRelTemporalIndex(typ); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("drop after close: err = %v, want ErrStoreClosed", err)
	}
	ids := []types.RelID{1, 2}
	kept, ok := relTemporalPrune(t, ts).PruneRelTypeTemporalCandidates(typ, ids, storecontract.QueryOpts{ValidAt: 5})
	if ok || !slices.Equal(kept, ids) {
		t.Errorf("prune after close = %v, %v; want ids unchanged, ok=false", kept, ok)
	}
}

func TestTieredRelTemporalIndex_PruneDeclinesWithoutFilterOrIndex(t *testing.T) {
	ts := newTestTieredStore(t)
	const typ uint16 = 7
	prune := relTemporalPrune(t, ts)
	ids := []types.RelID{11, 12, 13}

	if kept, ok := prune.PruneRelTypeTemporalCandidates(typ, ids, storecontract.QueryOpts{ValidAt: 5}); ok || !slices.Equal(kept, ids) {
		t.Errorf("no index: prune = %v, %v; want ids unchanged, ok=false", kept, ok)
	}
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	for name, opts := range map[string]storecontract.QueryOpts{
		"no filter":         {},
		"open interval":     {ValidStart: 5},
		"half interval end": {ValidEnd: 5},
	} {
		if kept, ok := prune.PruneRelTypeTemporalCandidates(typ, ids, opts); ok || !slices.Equal(kept, ids) {
			t.Errorf("%s: prune = %v, %v; want ids unchanged, ok=false", name, kept, ok)
		}
	}
	if kept, ok := prune.PruneRelTypeTemporalCandidates(typ+1, ids, storecontract.QueryOpts{ValidAt: 5}); ok || !slices.Equal(kept, ids) {
		t.Errorf("other type: prune = %v, %v; want ids unchanged, ok=false", kept, ok)
	}
	// Unknown ids are never pruned: no shard covers them.
	if kept, _ := prune.PruneRelTypeTemporalCandidates(typ, ids, storecontract.QueryOpts{ValidAt: 5}); !slices.Equal(kept, ids) {
		t.Errorf("uncovered ids: kept = %v, want %v", kept, ids)
	}
}

func TestTieredRelTemporalIndex_EveryShardAndNewHotShard(t *testing.T) {
	ts := newTestTieredStore(t)
	forceRotation(t, ts) // two event shards before the create
	const typ uint16 = 9
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	forceRotation(t, ts) // a hot shard opened after the create
	if got := eventShardCount(ts); got != 3 {
		t.Fatalf("event shards = %d, want 3", got)
	}
	shards := allShardStoresForTest(t, ts)
	for name, s := range shards {
		got, err := s.RelTemporalIndexTypes()
		if err != nil || !slices.Equal(got, []uint16{typ}) {
			t.Errorf("shard %s RelTemporalIndexTypes = %v, %v; want [%d]", name, got, err, typ)
		}
	}
	if err := relTemporalDDL(t, ts).DropRelTemporalIndex(typ); err != nil {
		t.Fatalf("drop: %v", err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		if got, err := s.RelTemporalIndexTypes(); err != nil || len(got) != 0 {
			t.Errorf("after drop shard %s RelTemporalIndexTypes = %v, %v; want empty", name, got, err)
		}
	}
}

func TestTieredRelTemporalIndex_SurvivesRestartAndColdReopen(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts.mu.RLock()
	first := ts.hotShard.name
	ts.mu.RUnlock()
	forceRotation(t, ts)
	const typ uint16 = 4
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	if got, err := ts.RelTemporalIndexTypes(); err != nil || !slices.Equal(got, []uint16{typ}) {
		t.Fatalf("after restart RelTemporalIndexTypes = %v, %v; want [%d]", got, err, typ)
	}
	// Close the first shard as an idle cold shard would be; the lazy reopen
	// must bring its definition back.
	demoteToCold(ts, first)
	ts.mu.RLock()
	es := ts.eventShards[first]
	ts.mu.RUnlock()
	es.shardMu.Lock()
	if es.store != nil {
		if err := es.store.Close(); err != nil {
			es.shardMu.Unlock()
			t.Fatalf("close cold shard: %v", err)
		}
		es.store = nil
	}
	es.shardMu.Unlock()
	forceRotation(t, ts)
	for name, s := range allShardStoresForTest(t, ts) {
		if got, err := s.RelTemporalIndexTypes(); err != nil || !slices.Equal(got, []uint16{typ}) {
			t.Errorf("shard %s RelTemporalIndexTypes = %v, %v; want [%d]", name, got, err, typ)
		}
	}
}

func TestTieredComposite_DDLBreakCases(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	ddl, intro := compositeDDL(t, ts)
	keys := []string{"device", "pid", "ctime"}

	for name, bad := range map[string][]string{
		"one key":       {"device"},
		"five keys":     {"a", "b", "c", "d", "e"},
		"duplicate key": {"device", "device"},
	} {
		if err := ddl.CreateCompositePropertyIndex(signalTok, bad); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
			t.Errorf("create %s: err = %v, want ErrInvalidStoreMutation", name, err)
		}
		if err := ddl.DropCompositePropertyIndex(signalTok, bad); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
			t.Errorf("drop %s: err = %v, want ErrInvalidStoreMutation", name, err)
		}
	}
	if err := ddl.CreateCompositePropertyIndex(signalTok, []string{"device", "tkg_valid_from"}); !errors.Is(err, types.ErrReservedPrefix) {
		t.Errorf("create reserved key: err = %v, want ErrReservedPrefix", err)
	}
	if err := ddl.CreateCompositePropertyIndex(0, keys); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Errorf("create token 0: err = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := intro.ListCompositePropertyIndexes(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Errorf("list token 0: err = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ddl.DropCompositePropertyIndex(signalTok, keys); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("drop before create: err = %v, want ErrIndexNotFound", err)
	}
	if err := ddl.CreateCompositePropertyIndex(signalTok, keys); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ddl.CreateCompositePropertyIndex(signalTok, keys); !errors.Is(err, storecontract.ErrIndexExists) {
		t.Errorf("duplicate create: err = %v, want ErrIndexExists", err)
	}
	// A different ORDER of the same key set is a distinct definition.
	reordered := []string{"pid", "device", "ctime"}
	if err := ddl.CreateCompositePropertyIndex(signalTok, reordered); err != nil {
		t.Fatalf("create reordered: %v", err)
	}
	got, err := intro.ListCompositePropertyIndexes(signalTok)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 || !slices.Equal(got[0], keys) || !slices.Equal(got[1], reordered) {
		t.Errorf("list = %v, want [%v %v]", got, keys, reordered)
	}
	if other, err := intro.ListCompositePropertyIndexes(signalTok + 1); err != nil || len(other) != 0 {
		t.Errorf("list other label = %v, %v; want empty", other, err)
	}
	if err := ddl.DropCompositePropertyIndex(signalTok, keys); err != nil {
		t.Fatalf("drop: %v", err)
	}
	if err := ddl.DropCompositePropertyIndex(signalTok, keys); !errors.Is(err, storecontract.ErrIndexNotFound) {
		t.Errorf("drop twice: err = %v, want ErrIndexNotFound", err)
	}
	if got, _ := intro.ListCompositePropertyIndexes(signalTok); len(got) != 1 || !slices.Equal(got[0], reordered) {
		t.Errorf("list after drop = %v, want [%v]", got, reordered)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := ddl.CreateCompositePropertyIndex(signalTok, keys); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("create after close: err = %v, want ErrStoreClosed", err)
	}
	if err := ddl.DropCompositePropertyIndex(signalTok, reordered); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("drop after close: err = %v, want ErrStoreClosed", err)
	}
	if _, err := intro.ListCompositePropertyIndexes(signalTok); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("list after close: err = %v, want ErrStoreClosed", err)
	}
	if _, err := ddl.NodesByLabelAndProperties(signalTok, map[string]any{"device": "d", "pid": int64(1)}, storecontract.QueryOpts{}); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Errorf("lookup after close: err = %v, want ErrStoreClosed", err)
	}
}

func TestTieredComposite_EveryShardAndNewHotShard(t *testing.T) {
	ts := newTestTieredStore(t)
	_, _, signalTok := installDefaultTestLabelRegistry(t, ts)
	forceRotation(t, ts)
	ddl, _ := compositeDDL(t, ts)
	keys := []string{"device", "pid"}
	if err := ddl.CreateCompositePropertyIndex(signalTok, keys); err != nil {
		t.Fatalf("create: %v", err)
	}
	forceRotation(t, ts)
	for name, s := range allShardStoresForTest(t, ts) {
		got, err := s.ListCompositePropertyIndexes(signalTok)
		if err != nil || len(got) != 1 || !slices.Equal(got[0], keys) {
			t.Errorf("shard %s composites = %v, %v; want [%v]", name, got, err, keys)
		}
	}
	if err := ddl.DropCompositePropertyIndex(signalTok, keys); err != nil {
		t.Fatalf("drop: %v", err)
	}
	for name, s := range allShardStoresForTest(t, ts) {
		if got, err := s.ListCompositePropertyIndexes(signalTok); err != nil || len(got) != 0 {
			t.Errorf("after drop shard %s composites = %v, %v; want empty", name, got, err)
		}
	}
}

func TestTieredComposite_SurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const label uint16 = 3
	forceRotation(t, ts)
	ddl, _ := compositeDDL(t, ts)
	keys := []string{"device", "pid", "ctime"}
	if err := ddl.CreateCompositePropertyIndex(label, keys); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	_, intro := compositeDDL(t, ts)
	if got, err := intro.ListCompositePropertyIndexes(label); err != nil || len(got) != 1 || !slices.Equal(got[0], keys) {
		t.Fatalf("after restart list = %v, %v; want [%v]", got, err, keys)
	}
	forceRotation(t, ts)
	for name, s := range allShardStoresForTest(t, ts) {
		if got, err := s.ListCompositePropertyIndexes(label); err != nil || len(got) != 1 {
			t.Errorf("shard %s composites = %v, %v; want one definition", name, got, err)
		}
	}
}

// A DDL interrupted between shards leaves copies on some non-reference shards
// that disagree with the anchoring reference shard: an extra definition (a
// create that never reached the reference shard) or a missing one (a drop
// that removed it from some shards first). Store open repairs every open
// shard to the anchor; the extra definition must not survive, the missing
// one must be rebuilt.
func TestTieredShardIndexes_InterruptedDDLRepairedAtOpen(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const anchoredType, orphanType, label = uint16(4), uint16(5), uint16(3)
	keys := []string{"device", "pid"}
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(anchoredType); err != nil {
		t.Fatalf("create: %v", err)
	}
	ddl, _ := compositeDDL(t, ts)
	if err := ddl.CreateCompositePropertyIndex(label, keys); err != nil {
		t.Fatalf("create composite: %v", err)
	}
	ts.mu.RLock()
	hot := ts.hotShard.store
	ts.mu.RUnlock()
	// Interrupted create: only the hot shard got the orphan definitions.
	if err := hot.CreateRelTemporalIndex(orphanType); err != nil {
		t.Fatal(err)
	}
	if err := hot.CreateCompositePropertyIndex(label, []string{"pid", "device"}); err != nil {
		t.Fatal(err)
	}
	// Interrupted drop: the hot shard already lost the anchored definitions.
	if err := hot.DropRelTemporalIndex(anchoredType); err != nil {
		t.Fatal(err)
	}
	if err := hot.DropCompositePropertyIndex(label, keys); err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	ts, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	for name, s := range allShardStoresForTest(t, ts) {
		if got, err := s.RelTemporalIndexTypes(); err != nil || !slices.Equal(got, []uint16{anchoredType}) {
			t.Errorf("shard %s rel temporal types = %v, %v; want [%d]", name, got, err, anchoredType)
		}
		if got, err := s.ListCompositePropertyIndexes(label); err != nil || len(got) != 1 || !slices.Equal(got[0], keys) {
			t.Errorf("shard %s composites = %v, %v; want [%v]", name, got, err, keys)
		}
	}
	// Re-running the DDL after an interrupted create reaches the shards
	// already carrying it without failing.
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(orphanType); err != nil {
		t.Fatalf("create orphan type: %v", err)
	}
	if got, _ := ts.RelTemporalIndexTypes(); !slices.Equal(got, []uint16{anchoredType, orphanType}) {
		t.Errorf("RelTemporalIndexTypes = %v, want [%d %d]", got, anchoredType, orphanType)
	}
}

// A shard that fails mid fan-out: every shard the DDL already changed is
// undone, and the anchor (reference shard, changed last) never records the
// definition — the index does not half-exist.
func TestTieredShardIndexes_FanOutRollsBackOnShardFailure(t *testing.T) {
	ts := newTestTieredStore(t)
	forceRotation(t, ts)
	forceRotation(t, ts) // three event shards
	const typ, label = uint16(8), uint16(3)
	keys := []string{"device", "pid"}

	ts.mu.RLock()
	var broken *EventShard
	for _, es := range ts.eventShards {
		if es != ts.hotShard {
			broken = es
			break
		}
	}
	ts.mu.RUnlock()
	if err := broken.store.Close(); err != nil {
		t.Fatal(err)
	}

	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("create with a failing shard: err = %v, want ErrStoreClosed", err)
	}
	ddl, intro := compositeDDL(t, ts)
	if err := ddl.CreateCompositePropertyIndex(label, keys); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("create composite with a failing shard: err = %v, want ErrStoreClosed", err)
	}
	if got, err := ts.RelTemporalIndexTypes(); err != nil || len(got) != 0 {
		t.Errorf("anchor lists %v, %v after a failed create; want none", got, err)
	}
	if got, err := intro.ListCompositePropertyIndexes(label); err != nil || len(got) != 0 {
		t.Errorf("anchor lists composites %v, %v after a failed create; want none", got, err)
	}
	ts.mu.RLock()
	shards := []*BadgerStore{ts.refShard}
	for _, es := range ts.eventShards {
		if es != broken {
			shards = append(shards, es.store)
		}
	}
	ts.mu.RUnlock()
	for i, s := range shards {
		if got, _ := s.RelTemporalIndexTypes(); len(got) != 0 {
			t.Errorf("shard %d kept rel temporal types %v after rollback", i, got)
		}
		if got, _ := s.ListCompositePropertyIndexes(label); len(got) != 0 {
			t.Errorf("shard %d kept composites %v after rollback", i, got)
		}
	}
}

// Drop with a failing shard: the shards already dropped get the index back
// and the anchor still lists it.
func TestTieredShardIndexes_DropRollsBackOnShardFailure(t *testing.T) {
	ts := newTestTieredStore(t)
	forceRotation(t, ts)
	forceRotation(t, ts)
	const typ uint16 = 8
	if err := relTemporalDDL(t, ts).CreateRelTemporalIndex(typ); err != nil {
		t.Fatalf("create: %v", err)
	}
	ts.mu.RLock()
	var broken *EventShard
	for _, es := range ts.eventShards {
		if es != ts.hotShard {
			broken = es
			break
		}
	}
	ts.mu.RUnlock()
	if err := broken.store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := relTemporalDDL(t, ts).DropRelTemporalIndex(typ); !errors.Is(err, storecontract.ErrStoreClosed) {
		t.Fatalf("drop with a failing shard: err = %v, want ErrStoreClosed", err)
	}
	if got, err := ts.RelTemporalIndexTypes(); err != nil || !slices.Equal(got, []uint16{typ}) {
		t.Errorf("anchor lists %v, %v after a failed drop; want [%d]", got, err, typ)
	}
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	for _, es := range ts.eventShards {
		if es == broken {
			continue
		}
		if got, _ := es.store.RelTemporalIndexTypes(); !slices.Equal(got, []uint16{typ}) {
			t.Errorf("shard %s lists %v after a failed drop; want [%d]", es.name, got, typ)
		}
	}
}
