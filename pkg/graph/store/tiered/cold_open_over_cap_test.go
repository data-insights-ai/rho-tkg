package tiered

import (
	"context"
	"errors"
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
// handle gets. The cap (1) is filled by a read of another real cold day, which
// that day's check-in keeps. A handle that fits under the cap keeps the full
// config, and the over-cap handle answers the same and closes at check-in.
func TestColdOpen_OverCapHandleOpensTransient(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		name      string
		occupied  bool // another cold day is kept first
		wantCache int64
		wantLazy  bool
		wantKept  bool
	}{
		{"under the cap: retained, full config", false, 64 << 20, false, true},
		{"cap full: transient config, closed at check-in", true, transientCache, true, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			f := newOverCapFixture(t, 2, 1)
			ts, name, n := f.ts, f.names[1], f.nodes[1]
			if c.occupied {
				f.read(t, 0)
				f.check(t, "the other day kept", "full", "closed")
			}
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
			f.check(t, "after check-in")
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
	cfg.ColdAfter = time.Millisecond // a rotation demotes a warm shard (TestColdCap_RotationDoesNotCountATransientHandle)
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

// form reports day i's handle: warm (the shard is not cold, outside the cap),
// closed, full (read-only, the configured 64 MiB block cache, label maps built
// at the open), transient (read-only, 16 MiB, lazy label maps) or writable. A
// read-only handle that is neither fails.
func (f *overCapFixture) form(t *testing.T, i int) string {
	t.Helper()
	es := f.ts.EventShardsForTest()[f.names[i]]
	if es.currentTier() != TierCold {
		return "warm"
	}
	store, ro := es.StoreStateForTest()
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

// TestColdCap_RotationDoesNotCountATransientHandle (R3-1 OVERCAP review,
// finding 1): a rotation demotes warm shard H and counts its handle under H's
// lock. Between the tier flip and that lock a read can open H: the handle
// sweep's swap closed H's warm handle and, finding H cold at its install, left
// it closed (installSweptLocked), and the read takes H's lock first. With the
// cap full that read opens H transient, uncounted. The rotation must not count
// it: a counted transient handle makes the tick evict a kept day and the
// check-in keep the transient one. The demote hook runs the read between the
// flip and the count; the swap's close is done there by hand.
func TestColdCap_RotationDoesNotCountATransientHandle(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 1, 1)
	h := types.NodeID(tieredNodeGen(t).Generate())
	putMintNode(t, f.ts, h, mintSignalTok)
	hName := hotShardName(f.ts)
	f.names, f.nodes, f.late = append(f.names, hName), append(f.nodes, h), append(f.late, 0)
	rotate(t, f.ts) // H warm, on its hot-size handle (the sweep is held)
	f.read(t, 0)
	f.check(t, "A kept, H warm", "full", "warm")

	var release func()
	f.ts.SetDemoteHookForTest(func(es *EventShard) {
		if es.name != hName || release != nil {
			return
		}
		es.shardMu.Lock()
		err := es.store.Close()
		es.clearStoreLocked(f.ts)
		es.shardMu.Unlock()
		if err != nil {
			t.Errorf("close H's warm handle: %v", err)
		}
		release = f.pin(t, 1)
	})
	rotate(t, f.ts) // demotes H
	f.ts.SetDemoteHookForTest(nil)
	if release == nil {
		t.Fatal("the rotation did not demote H")
	}
	f.check(t, "the rotation demoted H with a transient read in flight", "full", "transient")
	f.ts.enforceColdShardCap()
	f.check(t, "the tick's cap pass", "full", "transient")
	release()
	f.check(t, "H checked in", "full", "closed")
	f.closeAll(t)
}

// TestColdCap_FailedUpgradeCloseDetachesAndFences (review finding 2): a writer
// replaces a kept read-only handle with a writable one, and the read-only
// handle's Close fails. The handle may be closed whatever the error says, so it
// is detached and the store fenced, as the idle close and the check-in close
// do: the write fails with the fault and no later read pins the closed handle.
// MoveShard's, the retention drop's and clearEventShard's closes do not yet.
func TestColdCap_FailedUpgradeCloseDetachesAndFences(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 1, 1)
	f.read(t, 0)
	f.check(t, "A kept", "full")
	es := f.ts.EventShardsForTest()[f.names[0]]
	store, _ := es.StoreStateForTest()
	fault := errors.New("injected close fault")
	store.FailNextCloseForTest(fault)
	if err := f.ts.PutNode(types.NewNode(f.late[0], mintSignalTok, nil)); !errors.Is(err, fault) {
		t.Fatalf("late write over a failing read-only close = %v, want the close fault", err)
	}
	f.check(t, "after the failed close", "closed")
	if err := f.ts.backgroundError(); !errors.Is(err, fault) {
		t.Fatalf("background error = %v, want the close fault (the store fenced)", err)
	}
	if _, release, err := es.checkoutStoreReadOnly(f.ts); err == nil {
		release()
		t.Fatal("a read pinned the shard after its close failed")
	}
}

// TestColdCap_FailedCheckinCloseDetaches (review finding 2): the check-in that
// closes a transient handle meets a Close fault. It detaches the handle and
// fences the store, as the idle close does; the count keeps the kept day only.
func TestColdCap_FailedCheckinCloseDetaches(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 2, 1)
	f.read(t, 0)
	release := f.pin(t, 1)
	f.check(t, "B pinned", "full", "transient")
	es := f.ts.EventShardsForTest()[f.names[1]]
	store, _ := es.StoreStateForTest()
	fault := errors.New("injected close fault")
	store.FailNextCloseForTest(fault)
	release()
	f.check(t, "B's check-in close failed", "full", "closed")
	if err := f.ts.backgroundError(); !errors.Is(err, fault) {
		t.Fatalf("background error = %v, want the close fault (the store fenced)", err)
	}
	if _, release, err := es.checkoutStoreReadOnly(f.ts); err == nil {
		release()
		t.Fatal("a read pinned the shard after its close failed")
	}
}

// TestColdCap_WriterUpgradesATransientHandle: a late write reaches a day a
// past-cap read holds on a transient, uncounted handle. The write waits for the
// read; the read's check-in closes the transient handle and the write opens the
// day writable, counted. The count is checked at every step.
func TestColdCap_WriterUpgradesATransientHandle(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 2, 1)
	f.read(t, 0)
	release := f.pin(t, 1)
	f.check(t, "B pinned", "full", "transient")
	done := make(chan error, 1)
	go func() { done <- f.ts.PutNode(types.NewNode(f.late[1], mintSignalTok, nil)) }()
	time.Sleep(20 * time.Millisecond)
	f.check(t, "the write waits for the read", "full", "transient")
	release()
	if err := <-done; err != nil {
		t.Fatalf("late write: %v", err)
	}
	f.check(t, "the write opened B writable", "full", "writable")
	if got := f.coldOpens(); !slices.Equal(got, []int64{1, 2}) {
		t.Fatalf("ColdOpens = %v, want [1 2]", got)
	}
	f.closeAll(t)
}

// TestColdCap_ReadersShareATransientHandle: two past-cap reads of one day share
// its one transient handle; the first check-in leaves it to the second, and
// the last check-in closes it.
func TestColdCap_ReadersShareATransientHandle(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 2, 1)
	f.read(t, 0)
	r1 := f.pin(t, 1)
	r2 := f.pin(t, 1)
	f.check(t, "two reads of B", "full", "transient")
	if got := f.coldOpens(); !slices.Equal(got, []int64{1, 1}) {
		t.Fatalf("ColdOpens = %v, want [1 1] (one open shared)", got)
	}
	r1()
	f.check(t, "the first check-in", "full", "transient")
	r2()
	f.check(t, "the last check-in", "full", "closed")
	f.closeAll(t)
}

// TestColdCap_WritableFallbackWhenTheCapIsFull: a past-cap read whose
// read-only open fails (a WAL a read-only open cannot replay) opens the day
// writable instead. That handle is counted as every writable cold handle is,
// so the read holds the count past the cap, and its check-in closes it.
func TestColdCap_WritableFallbackWhenTheCapIsFull(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 2, 1)
	f.read(t, 0)
	es := f.ts.EventShardsForTest()[f.names[1]]
	injectCorruptMemFile(t, f.ts.shardDir(es.path))
	store, release, err := es.checkoutStoreReadOnly(f.ts)
	if err != nil {
		t.Fatalf("read of B: %v", err)
	}
	ids, err := store.NodeIDsByLabel(mintSignalTok)
	f.check(t, "the read on the writable fallback", "full", "writable")
	release()
	if err != nil || !slices.Equal(ids, []types.NodeID{f.nodes[1]}) {
		t.Fatalf("NodeIDsByLabel = %v, %v; want [%d]", ids, err, f.nodes[1])
	}
	f.check(t, "checked in past the cap", "full", "closed")
	f.closeAll(t)
}

// TestColdCap_CloseWithATransientHandleOpen: the idle tick and the store's
// Close meet a transient handle in flight. Neither decrements a count the
// handle never added: the count stays at the kept handles, never below 0, and
// ends at 0.
func TestColdCap_CloseWithATransientHandleOpen(t *testing.T) {
	t.Parallel()
	f := newOverCapFixture(t, 2, 1)
	f.read(t, 0)
	release := f.pin(t, 1)
	f.idleTick()
	f.check(t, "the idle tick with B in flight", "closed", "transient")
	done := make(chan error, 1)
	go func() { done <- f.ts.Close() }()
	time.Sleep(20 * time.Millisecond)
	if n := f.ts.OpenColdShardsForTest(); n != 0 {
		t.Fatalf("open cold count %d while Close waits for B's read, want 0", n)
	}
	release()
	if err := <-done; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := f.ts.OpenColdShardsForTest(); n != 0 {
		t.Fatalf("open cold count %d after Close, want 0", n)
	}
	for i, name := range f.names {
		if s, _ := f.ts.EventShardsForTest()[name].StoreStateForTest(); s != nil {
			t.Fatalf("day %d still holds a handle after Close", i)
		}
	}
}
