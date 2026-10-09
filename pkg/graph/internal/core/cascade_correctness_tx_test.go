package core

import (
	"errors"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestAsOfLabelCountAfterCascadeBelowLabelChange: a node gains label X; a
// bounded cascade then corrects an interval where the older version (without
// X) was valid, so its rows carry no X and sit above the current row, which
// keeps its slot. At a pin after the cascade the as-of row is the newest row
// recorded — a correction row without X — so ByLabel(X){TxPin}, NodesAsOf
// filtered by X and CountByLabelAt(X){TxPin} all exclude the node, before and
// after a later write. Catches a count arm that decides from the current row
// while a row above it was recorded (the rows match ByLabel(C) either way, so
// only a label that differs between the rows tells). Every backend; the
// count arm is the core one on sharded and tiered.
func TestAsOfLabelCountAfterCascadeBelowLabelChange(t *testing.T) {
	t.Parallel()
	for _, be := range txbBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t, false)
			useTestClock(t, g)
			e := newCCEnt(t, g, false)
			b := e.add("B", 1000, nil)
			id := e.add("T", 1000, nil)
			if err := g.Nodes.AddLabel(e.ctx, types.NodeID(b), "X"); err != nil {
				t.Fatalf("AddLabel(B): %v", err)
			}
			if err := g.Nodes.AddLabel(e.ctx, types.NodeID(id), "X"); err != nil {
				t.Fatalf("AddLabel(T): %v", err)
			}
			e.mustCascade(id, 1500, 1600, map[string]any{"x": int64(1)})
			pin := e.pin()
			if rows := e.chain(id); rows[len(rows)-1].current {
				t.Fatalf("fixture: the current row must stay below the cascade rows:%s", e.chainString(id))
			}
			check := func(phase string) {
				t.Helper()
				opts := storepkg.QueryOpts{TxPin: pin}
				byLabel := e.nodes(g.Nodes.ByLabel("X", opts))
				n, err := g.Nodes.CountByLabelAt("X", opts)
				if err != nil {
					t.Fatalf("CountByLabelAt: %v", err)
				}
				asOf, err := g.Temporal.NodesAsOf(pin)
				if err != nil {
					t.Fatalf("NodesAsOf: %v", err)
				}
				var withX []*types.Node
				for _, x := range asOf {
					if slices.Contains(g.Nodes.Labels(x), "X") {
						withX = append(withX, x)
					}
				}
				want := ccVer("B", 1)
				if got := e.nodes(withX, nil); byLabel != want || got != want || n != 1 {
					t.Fatalf("[%s] ByLabel(X){TxPin}=[%s] NodesAsOf∩X=[%s] CountByLabelAt(X){TxPin}=%d; want [%s] and 1:%s",
						phase, byLabel, got, n, want, e.chainString(id))
				}
			}
			check("after the cascade")
			e.mustUpdate(id, map[string]any{"x": int64(9)})
			check("after a later update")
		})
	}
}

// TestTxRefusedUpdateWithTxTakesNoSnapshot: a GraphTx caller-instant update
// that is refused (t not after the chain's stamps, an update that changes
// nothing, a closed entity) is refused BEFORE the transaction snapshots the
// entity, so a refusal never schedules a history rewrite for Rollback
// (backlog 19). White-box: the snapshot set stays empty. Counterpart: an
// accepted update does snapshot.
func TestTxRefusedUpdateWithTxTakesNoSnapshot(t *testing.T) {
	t.Parallel()
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("T", 1000, nil)
		closed := e.add("C", 1000, nil)
		if err := e.closeAt(closed, 2000); err != nil {
			t.Fatalf("CloseVersion: %v", err)
		}
		update := func(tx *GraphTx, id int64, m map[string]any, at types.Instant) error {
			if e.rel {
				_, err := tx.UpdateRelationshipWithTx(types.RelID(id), m, at)
				return err
			}
			_, err := tx.UpdateNodeWithTx(types.NodeID(id), m, at)
			return err
		}
		cases := []struct {
			name string
			id   int64
			m    map[string]any
			at   func() types.Instant
			want error
		}{
			{"t below the recorded stamps", id, map[string]any{"x": int64(1)}, func() types.Instant { return 1 }, ErrTxOrder},
			{"no change", id, map[string]any{"x": int64(0)}, e.pin, ErrTxOrder},
			{"closed entity", closed, map[string]any{"x": int64(1)}, e.pin, ErrAlreadyClosed},
		}
		for _, c := range cases {
			at := c.at()
			tx, err := e.g.BeginTx()
			if err != nil {
				t.Fatalf("BeginTx: %v", err)
			}
			err = update(tx, c.id, c.m, at)
			snapshots := len(tx.snapshotSet)
			_ = tx.Rollback()
			if !errors.Is(err, c.want) {
				t.Fatalf("%s: err = %v; want %v", c.name, err, c.want)
			}
			if snapshots != 0 {
				t.Fatalf("%s: a refused update left %d snapshots", c.name, snapshots)
			}
		}
		at := e.pin()
		tx, err := e.g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		if err := update(tx, id, map[string]any{"x": int64(1)}, at); err != nil {
			_ = tx.Rollback()
			t.Fatalf("accepted update: %v", err)
		}
		if len(tx.snapshotSet) != 1 {
			t.Fatalf("an accepted update left %d snapshots; want 1", len(tx.snapshotSet))
		}
		_ = tx.Rollback()
	})
}
