package tiered

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestColdOpen_OverCapHandleOpensTransient (R3 qualify, measure.txt section
// H): with MaxOpenColdShards > 0 a read of a closed cold shard that finds the
// cap full closes its handle again at check-in, every read, as a cap-0 handle
// does. It must open as cheaply: the small block cache and the lazy label maps
// and ID sets (coldReadCfg's transient form), not the full config a retained
// handle gets. The cap is filled by counting one more open cold shard, which is
// what another retained shard does to the count. A handle that fits under the
// cap keeps the full config, and the over-cap handle answers the same and
// closes at check-in.
func TestColdOpen_OverCapHandleOpensTransient(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name      string
		occupied  int64 // other cold shards already open
		wantCache int64
		wantLazy  bool
		wantKept  bool
	}{
		{"under the cap: retained, full config", 0, 64 << 20, false, true},
		{"cap full: transient config, closed at check-in", 1, transientCache, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			ts, name, n, _ := coldOpenFixture(t, func(cfg *Config) {
				cfg.MaxOpenColdShards = 1
				cfg.BlockCacheSize = 64 << 20
			})
			ts.openColdShards.Add(c.occupied)
			t.Cleanup(func() { ts.openColdShards.Add(-c.occupied) })
			store, release := readOnlyColdHandle(t, ts, name)
			cache := store.DBForTest().Opts().BlockCacheSize
			lazy, _ := store.LabelMapStateForTest()
			ids, err := store.NodeIDsByLabel(mintSignalTok)
			release()
			if cache != c.wantCache || lazy != c.wantLazy {
				t.Fatalf("read-only cold handle: block cache %d, lazy label maps %v; want %d, %v", cache, lazy, c.wantCache, c.wantLazy)
			}
			if err != nil || !slices.Equal(ids, []types.NodeID{n}) {
				t.Fatalf("NodeIDsByLabel = %v, %v; want [%d]", ids, err, n)
			}
			if kept, _ := ts.EventShardsForTest()[name].StoreStateForTest(); (kept != nil) != c.wantKept {
				t.Fatalf("after check-in the handle is open %v, want %v", kept != nil, c.wantKept)
			}
		})
	}
}

// overCapFixture is a store with closed cold day shards, one signal node on
// each, MaxOpenColdShards cap and a 64 MiB block cache, so a handle's form is
// told by its block cache (64 MiB full, transientCache transient) and its label
// maps (built at the open, or lazy). late[i] is an ID minted in day i's window:
// its PutNode is a late write that opens day i writable. The idle tick never
// runs on its own (IdleTimeout an hour): the tests drive it (idleTick).
type overCapFixture struct {
	ts    *Store
	names []string
	nodes []types.NodeID
	late  []types.NodeID
}

func newOverCapFixture(t *testing.T, days, cap int, whileHot ...func(f *overCapFixture, day int)) *overCapFixture {
	t.Helper()
	cfg := mintCfg(t.TempDir(), true)
	cfg.MaxOpenColdShards = cap
	cfg.BlockCacheSize = 64 << 20
	cfg.MemTableSize = 8 << 20
	cfg.IdleTimeout = time.Hour
	f := &overCapFixture{ts: openMintStore(t, cfg)}
	holdHandleSweep(f.ts) // the test closes the demoted handles itself
	gen := tieredNodeGen(t)
	for i := 0; i < days; i++ {
		n, late := types.NodeID(gen.Generate()), types.NodeID(gen.Generate())
		putMintNode(t, f.ts, n, mintSignalTok)
		f.names = append(f.names, hotShardName(f.ts))
		f.nodes = append(f.nodes, n)
		f.late = append(f.late, late)
		for _, hook := range whileHot {
			hook(f, i)
		}
		rotate(t, f.ts)
	}
	for _, name := range f.names {
		demoteToCold(f.ts, name)
	}
	if closed := closeColdStores(t, f.ts); closed != days {
		t.Fatalf("setup: closed %d cold shards, want %d", closed, days)
	}
	for i, name := range f.names {
		if f.ts.timestampToEventShardEntry(f.late[i].SnowflakeID()) != f.ts.EventShardsForTest()[name] {
			t.Fatalf("setup: late ID %d was not minted in day %d's window", i, i)
		}
	}
	f.check(t, "setup", f.all("closed")...)
	return f
}

func (f *overCapFixture) all(form string) []string {
	out := make([]string, len(f.names))
	for i := range out {
		out[i] = form
	}
	return out
}

// form reports day i's handle: closed, full (read-only, the configured 64 MiB
// block cache, label maps built at the open), transient (read-only, 16 MiB,
// lazy label maps) or writable. A read-only handle that is neither fails.
func (f *overCapFixture) form(t *testing.T, i int) string {
	t.Helper()
	store, ro := f.ts.EventShardsForTest()[f.names[i]].StoreStateForTest()
	if store == nil {
		return "closed"
	}
	if !ro {
		return "writable"
	}
	cache := store.DBForTest().Opts().BlockCacheSize
	lazy, _ := store.LabelMapStateForTest()
	switch {
	case cache == 64<<20 && !lazy:
		return "full"
	case cache == transientCache && lazy:
		return "transient"
	}
	t.Fatalf("day %d: read-only handle with block cache %d and lazy label maps %v is neither form", i, cache, lazy)
	return ""
}

// check asserts each day's form (want, when given) and the count invariant
// (T6): the open cold count is the handles the cap governs, the full read-only
// and the writable ones; a transient handle is never in it.
func (f *overCapFixture) check(t *testing.T, step string, want ...string) {
	t.Helper()
	governed := 0
	got := make([]string, len(f.names))
	for i := range f.names {
		got[i] = f.form(t, i)
		if got[i] == "full" || got[i] == "writable" {
			governed++
		}
	}
	if want != nil && !slices.Equal(got, want) {
		t.Fatalf("%s: handles %v, want %v", step, got, want)
	}
	if n := f.ts.OpenColdShardsForTest(); n != int64(governed) {
		t.Fatalf("%s: handles %v; the open cold count is %d, want %d (the full and writable handles)", step, got, n, governed)
	}
}

// pin opens day i the way a read does and checks the handle answers the day's
// node through it; release is the read's check-in.
func (f *overCapFixture) pin(t *testing.T, i int) (release func()) {
	t.Helper()
	store, release := readOnlyColdHandle(t, f.ts, f.names[i])
	ids, err := store.NodeIDsByLabel(mintSignalTok)
	if err != nil || !slices.Equal(ids, []types.NodeID{f.nodes[i]}) {
		release()
		t.Fatalf("day %d: NodeIDsByLabel = %v, %v; want [%d]", i, ids, err, f.nodes[i])
	}
	return release
}

func (f *overCapFixture) read(t *testing.T, i int) { f.pin(t, i)() }

func (f *overCapFixture) coldOpens() []int64 {
	out := make([]int64, len(f.names))
	for i, name := range f.names {
		out[i] = f.ts.EventShardsForTest()[name].coldOpens.Load()
	}
	return out
}

// idleTick runs the idle sweeper's tick (closeIdleShards, then
// enforceColdShardCap) with every unpinned handle idle past the timeout.
func (f *overCapFixture) idleTick() {
	f.ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(3 * time.Millisecond)
	f.ts.closeIdleShards()
	f.ts.enforceColdShardCap()
	f.ts.SetIdleTimeoutForTest(time.Hour)
}

// closeAll is the idle tick with nothing pinned: every handle closes and the
// open cold count returns to 0 (an uncounted handle must not decrement it).
func (f *overCapFixture) closeAll(t *testing.T) {
	t.Helper()
	f.idleTick()
	f.check(t, "idle tick closes every handle", f.all("closed")...)
}

// TestColdCap_FullHandleOutlivesAPastCapReadInFlight (R3-1 OVERCAP T1,
// design-overcap.txt W1): two reads overlap at cap 2 with one day kept. The
// first opens the free slot full, the second finds the cap full and opens
// transient. The first checks in first: the full handle is the one kept and
// the transient one closes. The cap never keeps a transient handle, and a full
// open is not wasted for it.
func TestColdCap_FullHandleOutlivesAPastCapReadInFlight(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 3, 2)
	f.read(t, 0)
	f.check(t, "A read and kept", "full", "closed", "closed")
	releaseC := f.pin(t, 1)
	f.check(t, "C pinned", "full", "full", "closed")
	releaseD := f.pin(t, 2)
	f.check(t, "D pinned", "full", "full", "transient")
	releaseC()
	f.check(t, "C checked in first", "full", "full", "transient")
	releaseD()
	f.check(t, "D checked in", "full", "full", "closed")
	if got := f.coldOpens(); !slices.Equal(got, []int64{1, 1, 1}) {
		t.Fatalf("ColdOpens = %v, want [1 1 1]", got)
	}
	f.closeAll(t)
}

// TestColdCap_TickLeavesRetainedHandlesForAReadInFlight (T2, W2): the idle
// tick's cap pass runs while a past-cap read is in flight. The in-flight
// transient handle is not one the cap governs, so the pass evicts no kept day
// for it, and the read's check-in closes it.
func TestColdCap_TickLeavesRetainedHandlesForAReadInFlight(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 3, 2)
	f.read(t, 0)
	f.read(t, 1)
	f.check(t, "A and B kept", "full", "full", "closed")
	releaseC := f.pin(t, 2)
	f.check(t, "C pinned", "full", "full", "transient")
	f.ts.enforceColdShardCap()
	f.check(t, "the tick's cap pass with C in flight", "full", "full", "transient")
	releaseC()
	f.check(t, "C checked in", "full", "full", "closed")
	f.closeAll(t)
}

// TestColdCap_TransientHandleIsNotKeptWhenTheCapFrees (T3): the cap frees while
// a transient read is in flight (the kept day idles out). The transient handle
// still closes at check-in; the next read of that day finds room and opens it
// full, and that handle is kept.
func TestColdCap_TransientHandleIsNotKeptWhenTheCapFrees(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 2, 1)
	f.read(t, 0)
	f.check(t, "A kept", "full", "closed")
	releaseC := f.pin(t, 1)
	f.check(t, "C pinned", "full", "transient")
	f.idleTick() // A idles out; C is pinned
	f.check(t, "A idled out with C in flight", "closed", "transient")
	releaseC()
	f.check(t, "C checked in", "closed", "closed")
	f.read(t, 1)
	f.check(t, "C read again", "closed", "full")
	if got := f.coldOpens(); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("ColdOpens = %v, want [1 2]", got)
	}
	f.closeAll(t)
}

// TestColdCap_ThreeDaysTwoSlots (T4, measure.txt section H in the short tier):
// ten reads of each of three cold days, in turn and interleaved. At cap 2 the
// first two days open once each, full, and stay; the third opens transient on
// every read and closes. The exact unlimited control, cap 3, opens each day
// once, full. Both answer the same (pin checks each read's answer).
func TestColdCap_ThreeDaysTwoSlots(t *testing.T) {
	t.Parallel()
	const reads = 10
	orders := map[string][]int{}
	for i := 0; i < 3; i++ {
		for r := 0; r < reads; r++ {
			orders["in turn"] = append(orders["in turn"], i)
			orders["interleaved"] = append(orders["interleaved"], (i*reads+r)%3)
		}
	}
	for _, c := range []struct {
		cap       int
		wantOpens []int64
		wantForm  []string // each day's form at every read
		wantAfter []string
	}{
		{3, []int64{1, 1, 1}, []string{"full", "full", "full"}, []string{"full", "full", "full"}},
		{2, []int64{1, 1, reads}, []string{"full", "full", "transient"}, []string{"full", "full", "closed"}},
	} {
		for _, order := range []string{"in turn", "interleaved"} {
			days := orders[order]
			t.Run(fmt.Sprintf("cap %d, %s", c.cap, order), func(t *testing.T) {
				t.Parallel()
				f := newOverCapFixture(t, 3, c.cap)
				for step, i := range days {
					release := f.pin(t, i) // checks the answer
					if got := f.form(t, i); got != c.wantForm[i] {
						release()
						t.Fatalf("read %d of day %d: handle %s, want %s", step, i, got, c.wantForm[i])
					}
					f.check(t, fmt.Sprintf("read %d of day %d in flight", step, i))
					release()
					f.check(t, fmt.Sprintf("read %d of day %d checked in", step, i))
				}
				f.check(t, "after the reads", c.wantAfter...)
				if got := f.coldOpens(); !slices.Equal(got, c.wantOpens) {
					t.Fatalf("ColdOpens = %v, want %v", got, c.wantOpens)
				}
				f.closeAll(t)
			})
		}
	}
}

// TestColdCap_LabelCandidatesByHandleForm (R3-1 OVERCAP, the Fable review of
// b2b1e137): a budgeted label listing of the same closed cold shard counts
// what the handle it gets does at read time, and the form now depends on the
// cap's room. A full handle walked the label keys at its open (charged as the
// open, ShardOpens) and lists the live members: 1 candidate, no key walked. A
// transient handle walks the label's keys at the read, one candidate per key
// before it is looked at, live or stale (badgerstore_label_lazy.go): the member
// and the stale key, 2 candidates, 2 keys walked. Both answer the same. The
// counts are not made to agree: either would charge a full handle for a walk
// it does not do or leave the walk a transient handle does uncharged (AGENTS.md
// resource limits: the helper that does the work charges it; examined and
// rejected entries count).
func TestColdCap_LabelCandidatesByHandleForm(t *testing.T) {
	t.Parallel()
	staleKey := func(f *overCapFixture, day int) {
		if day != 1 {
			return
		}
		// A label key whose node has no row (a crash window, a historical orphan).
		err := eventShardStore(t, f.ts, f.names[day]).DBForTest().Update(func(txn *badgerv4.Txn) error {
			return txn.Set(storeutil.LabelIndexKey(mintSignalTok, snowflake.ID(9999)), nil)
		})
		if err != nil {
			t.Fatalf("stale label key: %v", err)
		}
	}
	f := newOverCapFixture(t, 2, 1, staleKey)
	count := func(step, wantForm string) (candidates uint64, walked int64) {
		t.Helper()
		store, release := readOnlyColdHandle(t, f.ts, f.names[1])
		defer release()
		if got := f.form(t, 1); got != wantForm {
			t.Fatalf("%s: handle %s, want %s", step, got, wantForm)
		}
		_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
		nodes, err := store.NodesByLabelMetered(mintSignalTok, QueryOpts{}.WithReadMeter(m, "probe"))
		if err != nil || len(nodes) != 1 || nodes[0].ID() != f.nodes[1] {
			t.Fatalf("%s: NodesByLabelMetered = %d nodes, %v; want [%d]", step, len(nodes), err, f.nodes[1])
		}
		return m.Work().Candidates, store.LabelKeysWalkedForTest()
	}
	if c, w := count("with room", "full"); c != 1 || w != 0 {
		t.Fatalf("full handle: %d candidates, %d label keys walked at the read; want 1 and 0", c, w)
	}
	f.closeAll(t)
	f.read(t, 0) // day 0 kept: the cap is full
	if c, w := count("cap full", "transient"); c != 2 || w != 2 {
		t.Fatalf("transient handle: %d candidates, %d label keys walked; want 2 and 2 (the member and the stale key)", c, w)
	}
	f.check(t, "after the transient read", "full", "closed")
}

// TestColdCap_WritableHandlesTakeSlots (T5, design D5): a late write's writable
// handle is one the cap governs. At cap 1 with day A open writable, a read of
// day B finds no room, opens transient and closes. A full read handle in flight
// when a late write opens another day finds the count past the cap at check-in
// and closes: what stays is at most the cap.
func TestColdCap_WritableHandlesTakeSlots(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 3, 1)
	putMintNode(t, f.ts, f.late[0], mintSignalTok)
	f.check(t, "late write on A", "writable", "closed", "closed")
	releaseB := f.pin(t, 1)
	f.check(t, "B pinned", "writable", "transient", "closed")
	releaseB()
	f.check(t, "B checked in", "writable", "closed", "closed")
	f.closeAll(t)

	releaseB = f.pin(t, 1)
	f.check(t, "B pinned with room", "closed", "full", "closed")
	putMintNode(t, f.ts, f.late[2], mintSignalTok)
	f.check(t, "late write on C with B in flight", "closed", "full", "writable")
	releaseB()
	f.check(t, "B checked in past the cap", "closed", "closed", "writable")
	f.closeAll(t)
}
