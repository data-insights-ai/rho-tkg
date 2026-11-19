package tiered

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// holdFixture is a store with n cold, closed, sealed event shards, one node
// each, and MaxOpenColdShards 2: the shard names oldest first and their nodes.
func holdFixture(t *testing.T, n int) (*Store, []*EventShard, []*types.Node) {
	t.Helper()
	cfg := volumeCfg(t.TempDir(), t.TempDir(), "")
	cfg.MaxOpenColdShards = 2
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	var shards []*EventShard
	var nodes []*types.Node
	for range n {
		node := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)
		if err := ts.PutNode(node); err != nil {
			t.Fatal(err)
		}
		name := ts.HotShardForTest().Name()
		forceRotation(t, ts)
		demoteToCold(ts, name)
		es := ts.EventShardsForTest()[name]
		es.SetLastAccessForTest(0)
		shards, nodes = append(shards, es), append(nodes, node)
	}
	ts.SetIdleTimeoutForTest(0)
	ts.CloseIdleShardsForTest()
	for _, es := range shards {
		if es.Store() != nil || es.cachedCounts() == nil {
			t.Fatalf("fixture: cold shard %s is open (%v) or unsealed", es.Name(), es.Store() != nil)
		}
	}
	return ts, shards, nodes
}

func holdOpenCold(shards []*EventShard) int {
	n := 0
	for _, es := range shards {
		if es.Store() != nil {
			n++
		}
	}
	return n
}

// TestShardHold_OneOpenPerShardUntilRelease (CUTCOST N1): under a meter that
// holds strictly, twenty reads and a write of one cold shard open it once,
// writable, by the meter's count and by the store's, whatever else is open;
// a second shard likewise; a third is refused by name with nothing opened;
// Release closes them all, sealed. The same reads on a meter that does not
// hold open the shard again and again once two others are open.
func TestShardHold_OneOpenPerShardUntilRelease(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, shards, nodes := holdFixture(t, 5)
	coldOpens := func() uint64 { return ts.ShardAccessStats().ColdOpens }
	read := func(m *storecontract.ReadMeter, i int) error {
		got, err := ts.GetNodesByIDsMetered([]types.NodeID{nodes[i].ID()}, m.Tally("Door"))
		if err == nil && (len(got) != 1 || got[0] == nil) {
			t.Fatalf("read of node %d: %d rows", i, len(got))
		}
		return err
	}

	day := func(i int) types.Instant { return types.Instant(shards[i].timeStart.UnixMilli()) }
	settles := func() int64 { // the sweep's settle steps so far, every close before this call paid for
		ts.SweepHandlesForTest()
		return cfSettleSteps(ts)
	}
	strict := func() *storecontract.ReadMeter {
		_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{ShardOpens: 8})
		m.LimitShardOpens()
		m.HoldShardOpens(storecontract.ShardHoldStrict)
		return m
	}

	// Reads only: each shard once, read-only, so its seal stays and the sweep owes it nothing (review 4 F2).
	m := strict()
	before, steps := coldOpens(), settles()
	for range 20 {
		if err := read(m, 0); err != nil {
			t.Fatal(err)
		}
	}
	if err := read(m, 1); err != nil {
		t.Fatal(err)
	}
	if w, native := m.Work().ShardOpens, coldOpens()-before; w != 2 || native != 2 || holdOpenCold(shards) != 2 || !shards[0].storeRO || !shards[1].storeRO {
		t.Fatalf("20 reads of one cold shard and a read of a second: %d opens on the meter, %d by the store, %d open, read-only %v and %v; want 2, 2, 2, both read-only", w, native, holdOpenCold(shards), shards[0].storeRO, shards[1].storeRO)
	}
	err := read(m, 2)
	var be *storecontract.ReadBudgetError
	if !errors.As(err, &be) || be.Limit != "shards held" || be.ShardsHeldBound != 2 || coldOpens()-before != 2 || holdOpenCold(shards) != 2 {
		t.Fatalf("a third cold shard for a strict meter: %v, %d opened by the store, %d open; want it refused at shards held with nothing more opened", err, coldOpens()-before, holdOpenCold(shards))
	}
	m.Release()
	m.Release()
	if holdOpenCold(shards) != 0 || shards[0].cachedCounts() == nil || shards[1].cachedCounts() == nil {
		t.Fatalf("after Release %d cold shards are open, sealed %v and %v; want none open, both sealed", holdOpenCold(shards), shards[0].cachedCounts() != nil, shards[1].cachedCounts() != nil)
	}
	if after := settles(); after != steps {
		t.Fatalf("the sweep ran %d settle steps for two shards that were only read, want none", after-steps)
	}
	// After Release the meter holds no more: an open for it is the cap's, not kept for a Release that never comes.
	if m.Tally("Door").HoldMode() != storecontract.ShardHoldNone {
		t.Fatal("a released meter still holds what is opened for it")
	}

	// A day read and then named for a write: the handle held read-only is upgraded on the meter, and the write
	// opens nothing. Its close leaves one settle step, the written shard's; the shard that was only read, none.
	m = strict()
	before, steps = coldOpens(), settles()
	if err := read(m, 0); err != nil {
		t.Fatal(err)
	}
	if err := ts.HoldShardsAt([]types.Instant{day(0)}, m.Tally("HoldShardsAt")); err != nil {
		t.Fatal(err)
	}
	if w, native := m.Work().ShardOpens, coldOpens()-before; w != 2 || native != 2 || shards[0].storeRO {
		t.Fatalf("a read and the write hold of one cold shard: %d opens on the meter, %d by the store, read-only %v; want 2 and 2 (the open, the upgrade), writable", w, native, shards[0].storeRO)
	}
	if err := ts.ReplaceNode(nodes[0].DeepCopy()); err != nil { // the caller's own write of the held shard
		t.Fatal(err)
	}
	if err := read(m, 1); err != nil {
		t.Fatal(err)
	}
	if w, native := m.Work().ShardOpens, coldOpens()-before; w != 3 || native != 3 || holdOpenCold(shards) != 2 || !shards[1].storeRO {
		t.Fatalf("then the write and a read of a second shard: %d opens on the meter, %d by the store, %d open; want 3, 3 and 2: the write opens nothing", w, native, holdOpenCold(shards))
	}
	m.Release()
	if after := settles(); holdOpenCold(shards) != 0 || after-steps != 1 {
		t.Fatalf("after Release %d cold shards are open and the sweep ran %d settle steps; want none open and one step, the written shard's", holdOpenCold(shards), after-steps)
	}

	// A write of a day the meter holds read-only that was not named (another part's write under the caller's
	// meter): the upgrade is counted on the meter all the same.
	m = strict()
	before = coldOpens()
	if err := errors.Join(read(m, 2), ts.ReplaceNode(nodes[2].DeepCopy())); err != nil {
		t.Fatal(err)
	}
	if w, native := m.Work().ShardOpens, coldOpens()-before; w != 2 || native != 2 || shards[2].storeRO {
		t.Fatalf("a read and an unnamed write of one held cold shard: %d opens on the meter, %d by the store; want 2 and 2", w, native)
	}
	m.Release()
	settles()

	// A day named for a write before any read of it: one writable open.
	m = strict()
	before = coldOpens()
	if err := ts.HoldShardsAt([]types.Instant{day(1)}, m.Tally("HoldShardsAt")); err != nil {
		t.Fatal(err)
	}
	if err := errors.Join(ts.ReplaceNode(nodes[1].DeepCopy()), read(m, 1)); err != nil {
		t.Fatal(err)
	}
	if w, native := m.Work().ShardOpens, coldOpens()-before; w != 1 || native != 1 || shards[1].storeRO {
		t.Fatalf("the write hold, the write and a read of one cold shard: %d opens on the meter, %d by the store; want 1 and 1, writable", w, native)
	}
	m.Release()
	settles()

	// Without a hold: two shards stay open, the third opens once a read.
	_, plain := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	for _, i := range []int{0, 1} {
		if err := read(plain, i); err != nil {
			t.Fatal(err)
		}
	}
	before = coldOpens()
	for range 5 {
		if err := read(plain, 2); err != nil {
			t.Fatal(err)
		}
	}
	if native := coldOpens() - before; native != 5 {
		t.Fatalf("5 reads of a third cold shard on a meter that does not hold: %d opens by the store; this fixture expects one a read", native)
	}
}

// TestShardHold_LRUKeepsTheBound (CUTCOST N2): a meter that holds by LRU
// reads five cold shards round after round: never more than the store's two
// are open, each open is on the meter and equals the store's count, and after
// Release none is open. HoldShardsAt opens the shard of an instant for the
// meter before any read or write of it.
func TestShardHold_LRUKeepsTheBound(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ts, shards, nodes := holdFixture(t, 5)
	_, m := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	m.HoldShardOpens(storecontract.ShardHoldLRU)
	before := ts.ShardAccessStats().ColdOpens
	for round := range 3 {
		for i := range nodes {
			if _, err := ts.GetNodesByIDsMetered([]types.NodeID{nodes[i].ID()}, m.Tally("Door")); err != nil {
				t.Fatal(err)
			}
			if open := holdOpenCold(shards); open > 2 {
				t.Fatalf("round %d, shard %d: %d cold shards open, want at most the store's 2", round, i, open)
			}
		}
	}
	if w, native := m.Work().ShardOpens, ts.ShardAccessStats().ColdOpens-before; w != 15 || native != 15 {
		t.Fatalf("three rounds over five cold shards with two kept: %d opens on the meter, %d by the store; want 15 and 15", w, native)
	}
	// A write of a shard held read-only upgrades the handle; the hold's own pin does not make it wait.
	wrote := make(chan error, 1)
	go func() { wrote <- ts.ReplaceNode(nodes[4].DeepCopy()) }()
	select {
	case err := <-wrote:
		if err != nil {
			t.Fatalf("a write of a shard held read-only: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a write of a shard held read-only waits on the hold's own pin")
	}
	m.Release()
	if open := holdOpenCold(shards); open != 0 {
		t.Fatalf("after Release %d cold shards are open, want none", open)
	}

	_, strict := storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	strict.LimitShardOpens()
	strict.HoldShardOpens(storecontract.ShardHoldStrict)
	at := types.Instant(shards[3].timeStart.UnixMilli())
	if err := ts.HoldShardsAt([]types.Instant{at}, strict.Tally("HoldShardsAt")); !errors.Is(err, storecontract.ErrReadBudget) || holdOpenCold(shards) != 0 {
		t.Fatalf("HoldShardsAt of a cold shard outside the meter's windows and limit: %v, %d open; want a refusal, none opened", err, holdOpenCold(shards))
	}
	_, strict = storecontract.WithReadBudget(context.Background(), storecontract.ReadBudget{})
	strict.LimitShardOpens()
	strict.HoldShardOpens(storecontract.ShardHoldStrict)
	strict.AllowShardWindows([2]types.Instant{at, at})
	before = ts.ShardAccessStats().ColdOpens
	if err := ts.HoldShardsAt([]types.Instant{at, at + 1}, strict.Tally("HoldShardsAt")); err != nil || strict.Work().ShardOpens != 1 || shards[3].Store() == nil {
		t.Fatalf("HoldShardsAt of a cold shard the meter's window names: %v, %d opens; want it open after one", err, strict.Work().ShardOpens)
	}
	if err := ts.ReplaceNode(nodes[3].DeepCopy()); err != nil {
		t.Fatal(err)
	}
	if native := ts.ShardAccessStats().ColdOpens - before; native != 1 {
		t.Fatalf("the held shard and a write of it: %d opens by the store, want the one on the meter", native)
	}
	strict.Release()
	if open := holdOpenCold(shards); open != 0 {
		t.Fatalf("after Release %d cold shards are open, want none", open)
	}
}

// TestShardHold_ReadsVisitShardsInWindowOrder (CUTCOST review 3 P3): a read
// that fans out visits the event shards by window start, every time. What a
// holding meter opens and closes under the bound then follows from the store,
// not from a map's iteration.
func TestShardHold_ReadsVisitShardsInWindowOrder(t *testing.T) {
	ts, _, _ := holdFixture(t, 12)
	for range 20 {
		ts.mu.RLock()
		shards := ts.eventShardSnapshot(DepthAll)
		ts.mu.RUnlock()
		if len(shards) < 12 {
			t.Fatalf("fixture: %d event shards", len(shards))
		}
		for i := 1; i < len(shards); i++ {
			if shards[i].timeStart.Before(shards[i-1].timeStart) {
				t.Fatalf("shard %d (%s, from %v) is visited after %s (from %v); want window order", i, shards[i].name, shards[i].timeStart, shards[i-1].name, shards[i-1].timeStart)
			}
		}
	}
}
