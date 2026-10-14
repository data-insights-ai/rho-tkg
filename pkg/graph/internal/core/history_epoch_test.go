package core

import (
	"context"
	"errors"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func mustHistory(t *testing.T, g *Core, id types.NodeID) []*types.Node {
	t.Helper()
	h, err := g.Nodes.History(id)
	if err != nil {
		t.Fatal(err)
	}
	return h
}

// store.HistoryEpochCapability's contract on every backend (backlog 68): the
// token stays while nothing touches the entity's history, and changes on
// every history write (corrections, also inside a GraphTx), rollback trim and compaction — removal-only changes
// included — and on a write to another entity it stays. A token is never
// handed out twice.
func TestHistoryEpoch_Contract(t *testing.T) {
	ctx := context.Background()
	for _, be := range r10AllBackends(t) {
		t.Run(be.name, func(t *testing.T) {
			g, err := New(be.cfg())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			if g.historyEpochs == nil {
				t.Fatalf("%s: HistoryEpochCapability not admitted", be.name)
			}
			id := r10Node(t, g, 6)
			other := r10Node(t, g, 2)
			rel := r10Rel(t, g, 3)
			seen := map[uint64]string{}
			token := func(what string) uint64 {
				t.Helper()
				e, err := g.historyEpochs.NodeHistoryEpoch(id)
				if err != nil || e == 0 {
					t.Fatalf("%s: token %d, %v", what, e, err)
				}
				return e
			}
			fresh := func(prev uint64, what string) uint64 {
				t.Helper()
				e := token(what)
				if e == prev {
					t.Fatalf("%s: token unchanged (%d)", what, e)
				}
				if was, dup := seen[e]; dup {
					t.Fatalf("%s: token %d reused (first after %s)", what, e, was)
				}
				seen[e] = what
				return e
			}
			same := func(prev uint64, what string) {
				t.Helper()
				if e := token(what); e != prev {
					t.Fatalf("%s: token moved %d -> %d without a history change", what, prev, e)
				}
			}
			e := fresh(0, "first")
			same(e, "second read")
			if _, err := g.Temporal.NodeEffectiveTimeline(id, g.peekNow()); err != nil {
				t.Fatal(err)
			}
			same(e, "after a read")
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, other, 1000, 2000, map[string]any{"x": int64(1)}); err != nil {
				t.Fatal(err)
			}
			same(e, "after a write to another node")
			if _, err := g.Temporal.SetRelVersionInterval(ctx, rel, 1000, 2000, map[string]any{"x": int64(1)}); err != nil {
				t.Fatal(err)
			}
			same(e, "after a write to a relationship")
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 1000, 2000, map[string]any{"x": int64(2)}); err != nil {
				t.Fatal(err)
			}
			e = fresh(e, "after a correction")
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 3000, 0, map[string]any{"x": int64(3)}); err != nil {
				t.Fatal(err)
			}
			e = fresh(e, "after an open correction (new current row)")
			// Written and rolled back between two reads: the row count is the
			// same again, the token must not be.
			tx0, err := g.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx0.SetNodeVersionInterval(id, 1000, 2000, map[string]any{"x": int64(-1)}); err != nil {
				t.Fatal(err)
			}
			if err := tx0.Rollback(); err != nil {
				t.Fatal(err)
			}
			e = fresh(e, "after a write and its rollback, unread in between")
			tx, err := g.BeginTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.SetNodeVersionInterval(id, 1000, 2000, map[string]any{"x": int64(4)}); err != nil {
				t.Fatal(err)
			}
			e = fresh(e, "inside a GraphTx after its correction")
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			fresh(e, "after the rollback")
			// Compaction removes rows only (an Update chain: every old row is
			// trimmable).
			plain, err := g.Nodes.Add(ctx, []string{"P"}, map[string]any{"k": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			for i := range 4 {
				if _, err := g.Nodes.Update(ctx, plain.ID(), map[string]any{"k": int64(i + 1)}); err != nil {
					t.Fatal(err)
				}
			}
			id = plain.ID()
			e = fresh(0, "plain node")
			before := len(mustHistory(t, g, id))
			_, err = g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1})
			switch {
			case errors.Is(err, storepkg.ErrCapabilityNotSupported): // sharded declines compaction
			case err != nil:
				t.Fatal(err)
			default:
				if after := len(mustHistory(t, g, id)); after >= before {
					t.Fatalf("compaction trimmed nothing of the node (%d -> %d rows)", before, after)
				}
				e = fresh(e, "after a compaction (rows removed only)")
				same(e, "after the compaction, read again")
			}
			re, err := g.historyEpochs.RelHistoryEpoch(rel)
			if err != nil || re == 0 {
				t.Fatalf("rel token %d, %v", re, err)
			}
			if _, err := g.Temporal.SetRelVersionInterval(ctx, rel, 1500, 2500, map[string]any{"y": int64(1)}); err != nil {
				t.Fatal(err)
			}
			if re2, _ := g.historyEpochs.RelHistoryEpoch(rel); re2 == re {
				t.Fatalf("rel token unchanged after a correction")
			}
		})
	}
}
