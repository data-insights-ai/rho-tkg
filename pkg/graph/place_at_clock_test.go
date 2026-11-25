package graph_test

import (
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAddRelationshipByIDAtClockSkipsTheColdValidDay: a relationship placed at
// the clock whose valid day's shard is cold and closed is minted on the
// clock's day, opens and writes no cold shard, and is found by valid time and
// by node beside the row on the cold day.
func TestAddRelationshipByIDAtClockSkipsTheColdValidDay(t *testing.T) {
	t.Parallel()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
		ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1, MintAtValidFrom: true, RelsByMint: true,
		Clock: func() time.Time { return tierClockDay(25, 9) }})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	ctx := t.Context()
	a, err := g.Nodes().Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	props := func(to int) map[string]any {
		p := tierClockAt(22, 12)
		p["tkg_valid_to"] = types.Instant(tierClockDay(22, to).UnixMilli())
		return p
	}
	var v1, v2 *types.Relationship
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		v1, err = tx.AddRelationshipByID("T", a.ID(), b.ID(), props(14))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cold := ts.EventShardsForTest()["2026-07-22"]
	if cold == nil || cold.Tier() != tieredpkg.TierCold {
		t.Fatalf("day 22's shard %+v, want cold", cold)
	}
	ts.SetIdleTimeoutForTest(0)
	ts.CloseIdleShardsForTest()
	ts.SetIdleTimeoutForTest(time.Hour)
	if s, _ := cold.StoreStateForTest(); s != nil {
		t.Fatal("day 22 still open")
	}

	before := ts.ShardAccessStats()
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		v2, err = tx.AddRelationshipByIDAtClock("T", a.ID(), b.ID(), props(13))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	after := ts.ShardAccessStats()
	if d := after.ColdOpens - before.ColdOpens; d != 0 {
		t.Errorf("cold opens %d, want 0", d)
	}
	if d := after.ColdWrites - before.ColdWrites; d != 0 {
		t.Errorf("cold writes %d, want 0", d)
	}
	if at := time.UnixMilli(int64(v2.ID().MintInstant())).UTC(); at.Before(tierClockDay(25, 0)) {
		t.Errorf("v2 minted at %v, want the clock's day", at)
	}

	var valid []types.RelID
	if err := g.Rels().ForEachAdjacentRelAt(a.ID(), "T", false, storepkg.QueryOpts{ValidAt: types.Instant(tierClockDay(22, 12).UnixMilli() + 1)}, func(r *types.Relationship) bool {
		valid = append(valid, r.ID())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	slices.Sort(valid)
	if want := []types.RelID{v1.ID(), v2.ID()}; !slices.Equal(valid, want) {
		t.Errorf("valid on day 22: %v, want %v", valid, want)
	}
	in, err := g.Rels().Incoming(b.ID(), "T")
	if err != nil || len(in) != 2 {
		t.Errorf("incoming by node: %d rows, %v, want 2", len(in), err)
	}
}
