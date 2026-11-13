package tiered

import (
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelIDRange_DeletedRelOpensOnlyItsShard (Sigma RH-S7 retraction): the
// history reads of a deleted relationship find no current row anywhere, so
// they used to open every closed shard. A closed shard's sealed counts carry
// the range of relationship IDs it holds (rows and history), and the reads
// skip a shard whose range misses the ID: only the deleted relationship's own
// shard opens.
func TestRelIDRange_DeletedRelOpensOnlyItsShard(t *testing.T) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	cfg := tierClockCfg(dir, clock)
	cfg.RelsByMint = true
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	labels, relTypes := registrypkg.NewLabelRegistry(), registrypkg.NewRelTypeRegistry()
	ts.SetLabelRegistry(labels)
	ts.SetNameRegistries(labels, relTypes)
	refTok, _ := labels.GetOrCreate("Case")
	fact, _ := relTypes.GetOrCreate("FACT")
	a := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	b := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	var ids []types.RelID
	for d := 17; d <= 22; d++ {
		id, ok, err := ts.MintRelIDAt(a.ID(), ms(julDay(d, 3)), true)
		if err != nil || !ok {
			t.Fatalf("MintRelIDAt day %d: %v, %v", d, ok, err)
		}
		if err := ts.PutRelationship(types.NewRelationship(id, fact, a.ID(), b.ID())); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	deleted := ids[3] // day 20
	v1 := types.NewRelationship(deleted, fact, a.ID(), b.ID())
	v2 := types.NewRelationship(deleted, fact, a.ID(), b.ID())
	v2.SetVersion(1)
	_ = v2.SetProperty("fact", "retracted")
	if err := ts.ReplaceRelWithHistory(v2, 0, v1); err != nil {
		t.Fatal(err)
	}
	if err := ts.DeleteRelationship(deleted); err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if ts, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	ts.SetLabelRegistry(labels)
	ts.SetNameRegistries(labels, relTypes)

	opens := func(what string, max uint64, read func() error) {
		t.Helper()
		before := ts.ShardAccessStats().ColdOpens
		if err := read(); err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		ts.CloseIdleShardsForTest()
		if got := ts.ShardAccessStats().ColdOpens - before; got > max {
			t.Fatalf("%s opened %d cold shards, want at most %d", what, got, max)
		}
	}
	ts.SetIdleTimeoutForTest(time.Nanosecond)
	opens("HasRelHistory", 1, func() error {
		has, err := ts.HasRelHistory(deleted)
		if err == nil && !has {
			t.Fatal("HasRelHistory = false for a deleted relationship")
		}
		return err
	})
	opens("RelHistoryStamps", 1, func() error {
		_, _, ok, err := ts.RelHistoryStamps(deleted)
		if err == nil && !ok {
			t.Fatal("RelHistoryStamps: no stamps for a deleted relationship")
		}
		return err
	})
	opens("GetRelHistory", 1, func() error {
		rows, err := ts.GetRelHistory(deleted)
		if err == nil && len(rows) == 0 {
			t.Fatal("GetRelHistory: no rows for a deleted relationship")
		}
		return err
	})
	opens("GetRelationship of a live one", 1, func() error {
		_, err := ts.GetRelationship(ids[1])
		return err
	})

	// A write into a sealed shard drops its range first: a new relationship
	// minted on day 19 outside the sealed range is found, also after a reopen
	// reseals the shard with the wider range.
	late, ok, err := ts.MintRelIDAt(a.ID(), ms(julDay(19, 7)), true)
	if err != nil || !ok {
		t.Fatalf("MintRelIDAt day 19: %v, %v", ok, err)
	}
	if err := ts.PutRelationship(types.NewRelationship(late, fact, a.ID(), b.ID())); err != nil {
		t.Fatal(err)
	}
	if _, err := ts.GetRelationship(late); err != nil {
		t.Fatalf("GetRelationship of the late write: %v", err)
	}
	ts.CloseIdleShardsForTest()
	if _, err := ts.GetRelationship(late); err != nil {
		t.Fatalf("GetRelationship of the late write after the idle close: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if ts, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	ts.SetLabelRegistry(labels)
	ts.SetNameRegistries(labels, relTypes)
	if _, err := ts.GetRelationship(late); err != nil {
		t.Fatalf("GetRelationship of the late write after a reopen: %v", err)
	}
	if has, err := ts.HasRelHistory(deleted); err != nil || !has {
		t.Fatalf("HasRelHistory of the deleted one after a reopen: %v, %v", has, err)
	}
}
