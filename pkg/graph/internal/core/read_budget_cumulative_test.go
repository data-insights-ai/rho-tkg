package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_MemberLimitHoldsAcrossKeysAndShards (R2 cumulative, ai-soc
// review 14 Nov 3.2, the rule: a limit is one allowance for the whole
// read): a temporal property read (TxPin) over a membership kept on disk, at
// the graph entry point, whose value keys, or whose tiered shards, each fit the
// Candidates limit but exceed it together. The store is the one owner of the
// count (the graph counts none of the members of a store that counts), so:
//
//   - the unlimited read answers the exact, non-empty set, and the count at
//     which a budget answers it exactly is one more than the count below which
//     it refuses (exact or refused);
//   - a limit above one key's or shard's share and below the total refuses
//     with the typed error and no rows;
//   - the native counter, the iterator steps of every on-disk index walk of the
//     process, which the meter does not keep, shows the refused read stopped
//     early: fewer entries visited than the control.
//
// badger: three values of three nodes each, one call with the three values;
// tiered: one value of three nodes on each of three event shards.
func TestReadBudget_MemberLimitHoldsAcrossKeysAndShards(t *testing.T) {
	testtier.Slow(t)
	const perGroup, groups = 20, 3
	const total = perGroup * groups
	openBadger := func(t *testing.T) (*Core, func(), func()) {
		g, err := New(Config{BadgerDir: t.TempDir(), UntimedValidStart: UntimedFromTxFrom, PropertyIndexOnDisk: true, badgerFlushInterval: txbNoAutoFlush})
		if err != nil {
			t.Fatal(err)
		}
		return g, func() { _ = g.Close() }, func() {}
	}
	openTiered := func(t *testing.T) (*Core, func(), func()) {
		ts, err := tiered.New(tiered.Config{
			DataDir:             t.TempDir(),
			RefLabels:           []string{"Ref"},
			ShardWindow:         7 * 24 * time.Hour,
			FlushInterval:       1<<63 - 1,
			PropertyIndexOnDisk: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		g, err := New(Config{Store: ts, UntimedValidStart: UntimedFromTxFrom})
		if err != nil {
			t.Fatal(err)
		}
		rotate := func() {
			ts.MuForTest().Lock()
			ts.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
			ts.MuForTest().Unlock()
			if err := ts.CheckRotationForTest(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(2 * time.Millisecond)
		}
		return g, func() { _ = g.Close(); _ = ts.Close() }, rotate
	}
	for _, be := range []struct {
		name   string
		open   func(t *testing.T) (*Core, func(), func())
		spread bool // the groups are tiered shards; else value keys
	}{{"badger/keys", openBadger, false}, {"tiered/shards", openTiered, true}} {
		t.Run(be.name, func(t *testing.T) {
			g, shut, rotate := be.open(t)
			defer shut()
			ctx := context.Background()
			if err := g.Index.CreateProperty("Sig", "k"); err != nil {
				t.Fatal(err)
			}
			var want []types.NodeID
			values := make([]any, 0, groups)
			for grp := 0; grp < groups; grp++ {
				if be.spread && grp > 0 {
					rotate() // the next batch opens a new event shard
				}
				v := int64(1)
				if !be.spread {
					v = int64(grp + 1)
				}
				values = append(values, v)
				for i := 0; i < perGroup; i++ {
					n, err := g.Nodes.Add(ctx, []string{"Sig"}, map[string]any{"k": v})
					if err != nil {
						t.Fatal(err)
					}
					want = append(want, n.ID())
				}
			}
			if be.spread {
				values = values[:1] // the one value, on three shards
				if n := len(g.store.(*tiered.Store).EventShardsForTest()); n != groups {
					t.Fatalf("fixture: %d event shards, want %d", n, groups)
				}
			}
			if err := g.store.(interface{ Flush() error }).Flush(); err != nil { // every walk steps an iterator
				t.Fatal(err)
			}
			pin, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			opts := storepkg.QueryOpts{TxPin: pin}
			read := func(ctx context.Context) ([]*types.Node, error) {
				return g.Nodes.ByLabelAndPropertyValuesCtx(ctx, "Sig", "k", values, opts)
			}
			visited, stop := badger.CountIndexEntriesVisitedForTest()
			defer stop()

			before := visited()
			uctx, all := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{})
			rows, err := read(uctx)
			if err != nil || len(rows) != total || fmt.Sprint(idsSorted(rows)) != fmt.Sprint(slices.Sorted(slices.Values(want))) {
				t.Fatalf("unlimited = %d rows, %v; want the %d nodes", len(rows), err, total)
			}
			control, count := visited()-before, all.Work().Candidates
			if control < total || count < total {
				t.Fatalf("unlimited: %d index entries visited, %d candidates; want at least the %d members", control, count, total)
			}

			t.Logf("unlimited: %d candidates, %d index entries visited", count, control)
			for _, limit := range []uint64{perGroup + 5, total - 1} { // above one group's share, below the total
				before = visited()
				bctx, m := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: limit})
				rows, err := read(bctx)
				be := rbdBudgetErr(t, err, "candidates")
				if rows != nil || be.Work.Candidates <= limit {
					t.Fatalf("Candidates %d: %d rows, %+v; want no rows and the refusal past the limit", limit, len(rows), m.Work())
				}
				steps := visited() - before
				t.Logf("Candidates %d: refused at %d counted, %d index entries visited", limit, be.Work.Candidates, steps)
				// Below the total the walks stop before they have visited every member entry (a
				// worker per shard may count one past the limit), whatever else the control reads.
				if (limit < total-1 && steps >= total) || steps > int64(limit)+groups {
					t.Fatalf("Candidates %d: %d index entries visited (the control %d, %d member entries); want the walks stopped at the refusal", limit, steps, control, total)
				}
			}

			// Exact or refused: at the unlimited count the read answers as before, one less refuses.
			bctx, m := storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count})
			if rows, err := read(bctx); err != nil || len(rows) != total || m.Work().Candidates != count {
				t.Fatalf("Candidates %d (the unlimited count): %d rows, %v, %+v; want the %d rows and the same count", count, len(rows), err, m.Work(), total)
			}
			bctx, _ = storepkg.WithReadBudget(ctx, storepkg.ReadBudget{Candidates: count - 1})
			if rows, err := read(bctx); rows != nil || !errors.Is(err, storepkg.ErrReadBudget) {
				t.Fatalf("Candidates %d: %d rows, %v; want the refusal", count-1, len(rows), err)
			}
		})
	}
}

// idsSorted returns the IDs of nodes in ascending order.
func idsSorted(nodes []*types.Node) []types.NodeID {
	return slices.Sorted(slices.Values(nodeIDsOf(nodes)))
}
