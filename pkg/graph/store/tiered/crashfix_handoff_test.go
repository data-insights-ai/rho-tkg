package tiered

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The run-time hand-off of CRASHFIX H4 (sweepOweRepairLocked), on ai-soc's v6
// configuration (cfCfg): which refusals hand a closed cold shard to the handle
// sweep, what the sweep's settle step leaves, and what a retention drop leaves.

// cfColdDay20 is a store on day 23 whose day 20 holds three logons and is
// cold and closed, cleanly (its demoted handle closed by the sweep).
func cfColdDay20(t *testing.T) (*Store, uint16, []types.NodeID) {
	t.Helper()
	clock := &tierClock{}
	clock.set(julDay(20, 12))
	ts, tok := cfOpen(t, t.TempDir(), clock)
	t.Cleanup(func() { _ = ts.Close() })
	var ids []types.NodeID
	for h := 1; h <= 3; h++ {
		id, err := cfPut(t, ts, tok, julDay(20, h))
		if err != nil {
			t.Fatalf("PutNode: %v", err)
		}
		ids = append(ids, id)
	}
	clock.set(julDay(23, 12))
	if err := ts.CheckRotationForTest(); err != nil {
		t.Fatalf("rotation: %v", err)
	}
	ts.SweepHandlesForTest()
	if s, _ := cfDay20(t, ts).StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the sweep; want closed")
	}
	return ts, tok, ids
}

func cfDay20(t *testing.T, ts *Store) *EventShard {
	t.Helper()
	es := ts.EventShardsForTest()["2026-07-20"]
	if es == nil || es.Tier() != TierCold {
		t.Fatalf("day 20: %+v; want cold", es)
	}
	return es
}

// cfMeteredGetBudget is cfMeteredGet under budget b.
func cfMeteredGetBudget(t *testing.T, ts *Store, id types.NodeID, b storecontract.ReadBudget) (*types.Node, error) {
	t.Helper()
	_, meter := storecontract.WithReadBudget(t.Context(), b)
	snap := ts.NewReadSnapshot()
	defer snap.Close()
	snap.(storecontract.ReadSnapshotMeterCapability).MeterReads(meter.Tally("cut"))
	return snap.GetNode(id)
}

// cfCountOpens runs fn and returns badger's own count of read-only and
// writable opens of es's directory during it.
func cfCountOpens(ts *Store, es *EventShard, fn func()) (ro, rw int) {
	dir := ts.shardDir(es.path)
	var mu sync.Mutex
	restore := badger.SetOpenHookForTest(func(d string, readOnly bool) {
		if d != dir {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if readOnly {
			ro++
		} else {
			rw++
		}
	})
	fn()
	restore()
	mu.Lock() // an open that loaded the hook before the restore may still be counting
	defer mu.Unlock()
	return ro, rw
}

// cfIdleClose closes every idle cold handle, as the idle tick does.
func cfIdleClose(ts *Store) {
	ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(time.Hour)
}

// TestCrashFix_BudgetRefusalOfCleanColdOpenHandsNothingOff: a cut's numeric
// limit reached inside the read-only open of a clean cold shard is that
// read's refusal, not a recovery the shard owes. The shard is not handed to
// the sweep, so a pass opens nothing (no writable handle held to the idle
// close, the catalog's seal kept).
func TestCrashFix_BudgetRefusalOfCleanColdOpenHandsNothingOff(t *testing.T) {
	testtier.Slow(t)
	ts, _, ids := cfColdDay20(t)
	es := cfDay20(t, ts)
	var refused *storecontract.ReadBudgetError
	if got, err := cfMeteredGetBudget(t, ts, ids[0], storecontract.ReadBudget{Candidates: 1}); !errors.As(err, &refused) || refused.Limit == "unsupported" {
		t.Fatalf("metered read under Candidates 1 = %v, %v; want a numeric refusal", got, err)
	}
	ro, rw := cfCountOpens(ts, es, ts.SweepHandlesForTest)
	if s, _ := es.StoreStateForTest(); s != nil || ro+rw != 0 {
		t.Fatalf("after the refusal and a sweep pass: open %v, %d read-only and %d writable opens; want closed, none", s != nil, ro, rw)
	}
	if e, _ := ts.CatalogForTest().GetShard(es.Name()); !e.CountsSealed || e.Recover {
		t.Fatalf("day 20's catalog entry: sealed %v, recover %v; want sealed, unmarked", e.CountsSealed, e.Recover)
	}
	if got, err := cfMeteredGet(t, ts, ids[0]); err != nil || got == nil || got.ID() != ids[0] {
		t.Fatalf("unlimited metered read = %v, %v; want the logon", got, err)
	}
}

// TestCrashFix_IndexDefinitionHandOffRecoversThenReadsReadOnly: an index
// definition the reference shard gained while day 20 was closed refuses the
// metered read (only a writable open syncs it) and hands day 20 to the sweep,
// marked; the pass's settle step syncs it on a writable open and closes it
// again, and the next metered read opens day 20 read-only and answers.
func TestCrashFix_IndexDefinitionHandOffRecoversThenReadsReadOnly(t *testing.T) {
	testtier.Slow(t)
	ts, tok, ids := cfColdDay20(t)
	es := cfDay20(t, ts)
	ts.HoldSettleStepsForTest() // the refusal's kick waits for the test
	if err := ts.refShard.CreateCompositePropertyIndex(tok, []string{"user", "host"}); err != nil {
		t.Fatal(err)
	}
	var refused *storecontract.ReadBudgetError
	if got, err := cfMeteredGet(t, ts, ids[0]); !errors.As(err, &refused) || refused.Limit != "unsupported" {
		t.Fatalf("metered read with a definition to sync = %v, %v; want the unsupported refusal", got, err)
	}
	if e, _ := ts.CatalogForTest().GetShard(es.Name()); !e.Recover {
		t.Fatal("day 20's entry after the refusal carries no recovery mark")
	}
	coldOpens := coldOpensOf(t, ts, es.Name())
	es.LockShardMuForTest()
	es.growNext = true // a writer's large handle is owed (the sweep closed its small one)
	es.UnlockShardMuForTest()
	_, rw := cfCountOpens(ts, es, func() { cfRelease(ts) })
	if !es.GrowNextForTest() {
		t.Fatal("the repair's own open took the large handle owed to the shard's next writer")
	}
	if s, _ := es.StoreStateForTest(); s != nil || rw != 1 {
		t.Fatalf("after the sweep pass: open %v, %d writable opens; want the shard repaired on one writable open and closed", s != nil, rw)
	}
	if e, _ := ts.CatalogForTest().GetShard(es.Name()); !cfEntrySettled(*e) {
		t.Fatalf("day 20's entry after the sweep pass: %+v; want settled", *e)
	}
	if got := coldOpensOf(t, ts, es.Name()); got != coldOpens {
		t.Fatalf("ColdOpens moved by the sweep's step: %d to %d; no access opened the shard", coldOpens, got)
	}
	var got *types.Node
	var err error
	ro, rw := cfCountOpens(ts, es, func() { got, err = cfMeteredGet(t, ts, ids[0]) })
	if err != nil || got == nil || got.ID() != ids[0] || ro != 1 || rw != 0 {
		t.Fatalf("metered read after the sweep pass = %v, %v, %d read-only and %d writable opens; want the logon from one read-only open", got, err, ro, rw)
	}
}

// TestCrashFix_HandOffRecoversAndClosesThenReadsReadOnly: the WAL case of the
// run-time hand-off (a catalog without the recovery mark; the settle step the
// sweep owes the unsealed shard anyway is held, so the read comes first). The
// refused read marks the shard; the sweep's step recovers it and closes it,
// and the next metered read opens day 20 read-only.
func TestCrashFix_HandOffRecoversAndClosesThenReadsReadOnly(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "cold")
	late := cfLateIDs(t, dir)[0]
	cfDropRecoverMarks(t, dir)
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, _ := cfOpenHeld(t, dir, clock)
	defer func() { _ = ts.Close() }()
	es := cfDay20(t, ts)
	if _, err := cfMeteredGet(t, ts, late); err == nil {
		t.Fatal("first metered read answered; want the refusal of the WAL replay")
	}
	cfRelease(ts)
	if s, _ := es.StoreStateForTest(); s != nil {
		t.Fatal("day 20 open after the sweep pass; want closed")
	}
	var got *types.Node
	var err error
	ro, rw := cfCountOpens(ts, es, func() { got, err = cfMeteredGet(t, ts, late) })
	if err != nil || got == nil || got.ID() != late || ro != 1 || rw != 0 {
		t.Fatalf("metered read after the sweep pass = %v, %v, %d read-only and %d writable opens; want the late logon from one read-only open", got, err, ro, rw)
	}
}

// TestCrashFix_CloseOfAMarkedShardHandsNothingOff: a close hands a shard to
// the sweep for an incomplete seal only. A recovery mark is the business of
// New and of a metered read's refusal, the two that know the directory needs
// a writable open: a read of a marked shard that opens and closes cleanly
// starts no repair (the sweep would open a clean shard writable).
func TestCrashFix_CloseOfAMarkedShardHandsNothingOff(t *testing.T) {
	testtier.Slow(t)
	ts, _, ids := cfColdDay20(t)
	es := cfDay20(t, ts)
	ts.CatalogForTest().SetShardRecover(es.Name(), true) // a mark no refusal stands behind
	steps := cfSettleSteps(ts)
	var got *types.Node
	var err error
	ro, rw := cfCountOpens(ts, es, func() {
		got, err = ts.GetNode(ids[0])
		ts.SweepHandlesForTest()
	})
	if err != nil || got == nil || ro != 1 || rw != 0 {
		t.Fatalf("read of the marked shard and a sweep = %v, %v, %d read-only and %d writable opens; want the logon from one read-only open", got, err, ro, rw)
	}
	if queued, ran := cfQueued(ts), cfSettleSteps(ts)-steps; queued != 0 || ran != 0 {
		t.Fatalf("after the read's check-in close: %d queued, %d settle steps; want none", queued, ran)
	}
}

// TestCrashFix_DropOfHandedOffShardOpensNothing: a retention drop of a shard
// queued for the sweep (a metered read's refusal) takes it off the sweep's
// hands. A pass that runs before the drop removes the directory opens nothing
// on it, so no handle is left on a shard the store no longer holds.
func TestCrashFix_DropOfHandedOffShardOpensNothing(t *testing.T) {
	testtier.Slow(t)
	dir := t.TempDir()
	cfRunChild(t, dir, "cold")
	late := cfLateIDs(t, dir)[0]
	cfDropRecoverMarks(t, dir)
	clock := &tierClock{}
	clock.set(julDay(23, 13))
	ts, tok := cfOpenHeld(t, dir, clock)
	defer func() { _ = ts.Close() }()
	es := cfDay20(t, ts)
	if _, err := cfMeteredGet(t, ts, late); err == nil {
		t.Fatal("first metered read answered; want the refusal of the WAL replay")
	}
	if _, queued := ts.owedQueued(es); !queued {
		t.Fatal("day 20 not queued for the sweep after the refusal")
	}
	var ro, rw int
	ts.dropRemoveHook = func() { ro, rw = cfCountOpens(ts, es, func() { cfRelease(ts) }) }
	res, err := ts.PurgeNodesByLabelBefore(tok, ms(julDay(21, 0)), 8)
	ts.dropRemoveHook = nil
	if err != nil || res.NodesPurged != 4 {
		t.Fatalf("purge of day 20: %+v, %v; want its 4 logons dropped", res, err)
	}
	if _, ok := ts.CatalogForTest().GetShard(es.Name()); ok {
		t.Fatal("day 20 still cataloged after the purge; want it dropped")
	}
	if s, _ := es.StoreStateForTest(); s != nil || ro+rw != 0 || ts.OpenColdShardsForTest() != 0 {
		t.Fatalf("after the drop: open %v, %d read-only and %d writable opens, %d open cold shards; want none", s != nil, ro, rw, ts.OpenColdShardsForTest())
	}
}
