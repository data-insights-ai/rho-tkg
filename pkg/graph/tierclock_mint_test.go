package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// tierClockStore opens a tiered store that mints at valid time (ADR-0013),
// its clock fixed at 2026-07-25 09:00 UTC, day windows, Case a reference label.
func tierClockStore(t *testing.T) *tieredpkg.Store {
	t.Helper()
	ts, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour,
		ShardWindowLocation: time.UTC, ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1, MintAtValidFrom: true,
		Clock: func() time.Time { return tierClockDay(25, 9) }})
	if err != nil {
		t.Fatal(err)
	}
	return ts
}

func tierClockDay(day, hour int) time.Time { return time.Date(2026, 7, day, hour, 0, 0, 0, time.UTC) }

func tierClockAt(day, hour int) map[string]any {
	return map[string]any{"tkg_valid_from": types.Instant(tierClockDay(day, hour).UnixMilli())}
}

// TestTierClockGraph_GeneratedIDsCarryValidTime (TIERCLOCK step 2): on a
// tiered store with MintAtValidFrom every generated-ID create door takes its
// event IDs from the store: a node's at its ValidFrom (untimed: the store's
// clock), a relationship's inside its start node's day (a reference start
// node: at its ValidFrom), all on the reserved node field 31; a reference
// node keeps the generator; a ValidFrom outside the ID range is refused; and
// graph.New refuses generators that would draw field 31.
func TestTierClockGraph_GeneratedIDsCarryValidTime(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{Store: tierClockStore(t)})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	field := func(id int64) int64 { return id >> 10 & 31 }
	when := func(id interface{ MintInstant() types.Instant }) time.Time {
		return time.UnixMilli(int64(id.MintInstant())).UTC()
	}
	inDay := func(what string, id int64, at time.Time, day int) {
		t.Helper()
		if field(id) != 31 || at.Before(tierClockDay(day, 0)) || !at.Before(tierClockDay(day+1, 0)) {
			t.Fatalf("%s: ID %d (field %d, time %v), want field 31 on 2026-07-%d", what, id, field(id), at, day)
		}
	}
	exact := func(what string, id int64, at, want time.Time) {
		t.Helper()
		if field(id) != 31 || !at.Equal(want) {
			t.Fatalf("%s: ID %d (field %d, time %v), want field 31 at %v", what, id, field(id), at, want)
		}
	}

	sig, err := g.Nodes().Add(ctx, []string{"Signal"}, tierClockAt(22, 12))
	if err != nil {
		t.Fatal(err)
	}
	exact("Nodes().Add", int64(sig.ID()), when(sig.ID()), tierClockDay(22, 12))
	untimed, err := g.Nodes().Add(ctx, []string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	exact("untimed node", int64(untimed.ID()), when(untimed.ID()), tierClockDay(25, 9))
	kase, err := g.Nodes().Add(ctx, []string{"Case"}, tierClockAt(22, 12))
	if err != nil {
		t.Fatal(err)
	}
	if field(int64(kase.ID())) == 31 {
		t.Fatalf("reference node %d minted by the store", kase.ID())
	}
	r, err := g.Rels().Add(ctx, "SEEN", sig, kase, tierClockAt(23, 5))
	if err != nil {
		t.Fatal(err)
	}
	inDay("Rels().Add, event start", int64(r.ID()), when(r.ID()), 22)
	r, err = g.Rels().Add(ctx, "OF", kase, sig, tierClockAt(24, 7))
	if err != nil {
		t.Fatal(err)
	}
	exact("Rels().Add, reference start", int64(r.ID()), when(r.ID()), tierClockDay(24, 7))
	before := map[string]any{"tkg_valid_from": types.Instant(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC).UnixMilli())}
	if _, err := g.Nodes().Add(ctx, []string{"Signal"}, before); !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("node before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}
	if _, err := g.Rels().Add(ctx, "OF", kase, sig, before); !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("rel before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}

	var txNodes []*types.Node
	var txRels []*types.Relationship
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		var err error
		if txNodes, err = tx.AddNodes([]string{"Signal"}, []map[string]any{tierClockAt(20, 1), tierClockAt(21, 2)}); err != nil {
			return err
		}
		txRels, err = tx.AddRelationships("SEEN", []graphpkg.RelCreate{
			{StartID: txNodes[0].ID(), EndID: kase.ID(), Props: tierClockAt(20, 3)},
			{StartID: kase.ID(), EndID: txNodes[1].ID(), Props: tierClockAt(21, 4)},
		})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	exact("tx.AddNodes", int64(txNodes[0].ID()), when(txNodes[0].ID()), tierClockDay(20, 1))
	exact("tx.AddNodes", int64(txNodes[1].ID()), when(txNodes[1].ID()), tierClockDay(21, 2))
	exact("tx.AddRelationships, event start", int64(txRels[0].ID()), when(txRels[0].ID()), tierClockDay(20, 3))
	exact("tx.AddRelationships, reference start", int64(txRels[1].ID()), when(txRels[1].ID()), tierClockDay(21, 4))
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		_, err := tx.AddRelationships("OF", []graphpkg.RelCreate{{StartID: kase.ID(), EndID: sig.ID(), Props: before}})
		return err
	})
	if !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("tx rel before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}
	err = g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		_, err := tx.AddNodes([]string{"Signal"}, []map[string]any{before})
		return err
	})
	if !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("tx node before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}

	var bNode *types.Node
	var bRel *types.Relationship
	if _, err := g.Batch().Run(func(b *graphpkg.BatchBuilder) error {
		var err error
		if bNode, err = b.AddNode([]string{"Signal"}, tierClockAt(19, 6)); err != nil {
			return err
		}
		if err := b.AddNodes([]string{"Signal"}, tierClockAt(18, 6), 2); err != nil {
			return err
		}
		bRel, err = b.AddRelationship("SEEN", bNode, kase, tierClockAt(19, 8))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	exact("batch AddNode", int64(bNode.ID()), when(bNode.ID()), tierClockDay(19, 6))
	exact("batch AddRelationship", int64(bRel.ID()), when(bRel.ID()), tierClockDay(19, 8))
	n18 := 0
	if err := g.Nodes().ForEachByLabel("Signal", storepkg.QueryOpts{}, func(n *types.Node) bool {
		if when(n.ID()).Equal(tierClockDay(18, 6)) && field(int64(n.ID())) == 31 {
			n18++
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if n18 != 2 {
		t.Fatalf("batch AddNodes: %d nodes minted at their ValidFrom, want 2", n18)
	}
	for _, add := range []func(b *graphpkg.BatchBuilder) error{
		func(b *graphpkg.BatchBuilder) error { _, err := b.AddNode([]string{"Signal"}, before); return err },
		func(b *graphpkg.BatchBuilder) error { _, err := b.AddRelationship("OF", kase, sig, before); return err },
	} {
		if _, err := g.Batch().Run(add); !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
			t.Fatalf("batch create before the epoch: %v, want ErrValidFromOutsideIDRange", err)
		}
	}

	// The constraint branch of the relationship kernel mints too.
	if err := g.Constraints().Add(temporalpkg.TemporalConstraint{Kind: temporalpkg.ConstraintRelWithinEndpoints}); err != nil {
		t.Fatal(err)
	}
	r, err = g.Rels().Add(ctx, "SEEN", sig, kase, tierClockAt(25, 10))
	if err != nil {
		t.Fatal(err)
	}
	inDay("Rels().Add under a temporal constraint", int64(r.ID()), when(r.ID()), 22)
	if _, err := g.Rels().Add(ctx, "OF", kase, sig, before); !errors.Is(err, graphpkg.ErrValidFromOutsideIDRange) {
		t.Fatalf("rel before the epoch under a temporal constraint: %v, want ErrValidFromOutsideIDRange", err)
	}
}

// TestTierClockGraph_NewRefusesField31: graph.New on a minting store refuses
// a generator on node field 31 and accepts the largest that stay clear of it;
// a tiered store without the option keeps every generator.
func TestTierClockGraph_NewRefusesField31(t *testing.T) {
	plain, err := tieredpkg.New(tieredpkg.Config{DataDir: t.TempDir(), ShardWindow: 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: plain, SnowflakeNodeID: 15, IngestLanes: 30})
	if err != nil {
		t.Fatalf("SnowflakeNodeID 15, IngestLanes 30 without MintAtValidFrom: %v", err)
	}
	_ = g.Close()

	for _, c := range []struct {
		node  int64
		lanes uint8
		ok    bool
	}{{15, 0, false}, {14, 0, true}, {0, 30, false}, {0, 29, true}} {
		ts := tierClockStore(t)
		g, err := graphpkg.New(graphpkg.Config{Store: ts, SnowflakeNodeID: c.node, IngestLanes: c.lanes})
		if (err == nil) != c.ok {
			t.Fatalf("SnowflakeNodeID %d, IngestLanes %d: %v, want ok %v", c.node, c.lanes, err, c.ok)
		}
		if g != nil {
			_ = g.Close()
		} else {
			_ = ts.Close()
		}
	}
}
