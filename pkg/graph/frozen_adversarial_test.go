package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round-two adversarial tests for frozen rows: concurrency aliasing, every
// remaining reference escape, fail-fast pins, and a bitemporal TX-axis
// cross-check. None of these confirm a feature works — each one tries to
// corrupt shared state or catch a door behaving differently from its twin.

// Pre-frozen-rows, scan readers never shared memory with writers (every row
// was a private deep copy). Now they hold pointers into the store's
// canonical state, so ANY internal path that mutates a published row in
// place is an unsynchronized write racing these readers. Run with -race:
// this test is the detector for the whole class.
func TestFrozenScanReadersRaceAgainstWriters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			const entities = 20
			nodes := make([]*types.Node, 0, entities)
			for i := 0; i < entities; i++ {
				n, err := g.Nodes().Add(ctx, []string{"Hot"}, map[string]any{"k": i, "tags": []string{"a", "b"}})
				if err != nil {
					t.Fatalf("seed add: %v", err)
				}
				nodes = append(nodes, n)
			}
			for i := 0; i < entities-1; i++ {
				if _, err := g.Rels().Add(ctx, "LINK", nodes[i], nodes[i+1], map[string]any{"w": i}); err != nil {
					t.Fatalf("seed rel: %v", err)
				}
			}

			const (
				writers   = 3
				readers   = 3
				perWorker = 150
			)
			var wg sync.WaitGroup
			errCh := make(chan error, writers+readers)

			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						n := nodes[(w*perWorker+i)%entities]
						switch i % 4 {
						case 0:
							if err := g.Nodes().SetProperty(ctx, n.ID(), "k", i); err != nil {
								errCh <- fmt.Errorf("writer %d SetProperty: %w", w, err)
								return
							}
						case 1:
							if err := g.Nodes().AddLabel(ctx, n.ID(), "Extra"); err != nil {
								errCh <- fmt.Errorf("writer %d AddLabel: %w", w, err)
								return
							}
						case 2:
							err := g.Nodes().RemoveLabel(ctx, n.ID(), "Extra")
							if err != nil && !errors.Is(err, graphpkg.ErrLabelNotFound) {
								errCh <- fmt.Errorf("writer %d RemoveLabel: %w", w, err)
								return
							}
						case 3:
							if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"u": i}); err != nil {
								errCh <- fmt.Errorf("writer %d Update: %w", w, err)
								return
							}
						}
					}
				}(w)
			}

			for r := 0; r < readers; r++ {
				wg.Add(1)
				go func(r int) {
					defer wg.Done()
					for i := 0; i < perWorker; i++ {
						rows, err := g.Nodes().ByLabel("Hot", storepkg.QueryOpts{})
						if err != nil {
							errCh <- fmt.Errorf("reader %d ByLabel: %w", r, err)
							return
						}
						for _, row := range rows {
							// Touch every read surface a consumer would.
							if tm := row.Temporal(); tm != nil {
								_ = tm.ValidFrom
								tm.ValidTo = 99 // own copy on frozen rows — must be harmless
							}
							if ig := row.Integrity(); ig != nil {
								_ = ig.Hash
							}
							_, _ = row.GetProperty("k")
							_ = g.Nodes().Labels(row)
						}
						rels, err := g.Rels().Outgoing(nodes[i%entities].ID(), "LINK")
						if err != nil {
							errCh <- fmt.Errorf("reader %d Outgoing: %w", r, err)
							return
						}
						for _, rel := range rels {
							_, _ = rel.GetProperty("w")
							if tm := rel.Temporal(); tm != nil {
								_ = tm.TxFrom
							}
						}
					}
				}(r)
			}

			wg.Wait()
			close(errCh)
			for err := range errCh {
				t.Fatal(err)
			}
		})
	}
}

// Reference-typed property values ([]string, map, []float32) on a FROZEN
// scan row: the returned values must be independent copies — appending or
// writing through them must never reach the canonical row.
func TestFrozenScanRowPropertyReferenceValuesAreIndependent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			_, err := g.Nodes().Add(ctx, []string{"Victim"}, map[string]any{
				"tags": []string{"clean-0", "clean-1"},
				"meta": map[string]any{"level": int64(1)},
				"vec":  []float32{1.0, 2.0},
			})
			if err != nil {
				t.Fatalf("add: %v", err)
			}

			rows, err := g.Nodes().ByLabel("Victim", storepkg.QueryOpts{})
			if err != nil || len(rows) != 1 {
				t.Fatalf("ByLabel: %v (%d)", err, len(rows))
			}
			row := rows[0]
			if !row.IsFrozen() {
				t.Fatalf("scan row not frozen")
			}

			if v, ok := row.GetProperty("tags"); ok {
				if tags, ok := v.([]string); ok && len(tags) > 0 {
					tags[0] = "POISON"
				} else {
					t.Fatalf("tags not []string: %T", v)
				}
			}
			if v, ok := row.GetProperty("meta"); ok {
				if m, ok := v.(map[string]any); ok {
					m["level"] = int64(666)
					m["injected"] = "yes"
				} else {
					t.Fatalf("meta not map: %T", v)
				}
			}
			if v, ok := row.GetProperty("vec"); ok {
				if vec, ok := v.([]float32); ok && len(vec) > 0 {
					vec[0] = -1
				}
			}

			rows2, err := g.Nodes().ByLabel("Victim", storepkg.QueryOpts{})
			if err != nil || len(rows2) != 1 {
				t.Fatalf("re-scan: %v", err)
			}
			fresh := rows2[0]
			if v, _ := fresh.GetProperty("tags"); fmt.Sprint(v) != "[clean-0 clean-1]" {
				t.Fatalf("tags poisoned through GetProperty backing: %v", v)
			}
			if v, _ := fresh.GetProperty("meta"); fmt.Sprint(v) != "map[level:1]" {
				t.Fatalf("map poisoned through GetProperty backing: %v", v)
			}
			if v, _ := fresh.GetProperty("vec"); fmt.Sprint(v) != "[1 2]" {
				t.Fatalf("vector poisoned through GetProperty backing: %v", v)
			}
		})
	}
}

// Pin the fail-fast contract on frozen adjacency rows: a consumer that
// mutates a scan result must get a loud, identifiable failure — never a
// silent no-op (which would hide bugs) and never silent success (which would
// corrupt the cache).
func TestFrozenAdjacencyRowsFailFastOnMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			a, _ := g.Nodes().Add(ctx, []string{"N"}, nil)
			b, _ := g.Nodes().Add(ctx, []string{"N"}, nil)
			if _, err := g.Rels().Add(ctx, "LINK", a, b, nil); err != nil {
				t.Fatalf("rel add: %v", err)
			}

			rels, err := g.Rels().Outgoing(a.ID(), "LINK")
			if err != nil || len(rels) != 1 {
				t.Fatalf("Outgoing: %v (%d)", err, len(rels))
			}
			row := rels[0]
			if !row.IsFrozen() {
				t.Fatalf("Outgoing returned an unfrozen adjacency row; scan doors must publish frozen rows")
			}

			if err := row.SetProperty("x", 1); !errors.Is(err, types.ErrFrozenRelationship) {
				t.Fatalf("SetProperty on frozen row = %v, want ErrFrozenRelationship", err)
			}
			func() {
				defer func() {
					if recover() == nil {
						t.Errorf("SetTemporal on frozen row did not panic — silent corruption path")
					}
				}()
				row.SetTemporal(&types.TemporalMetadata{})
			}()

			// And the thaw escape works: DeepCopy is mutable and detached.
			thawed := row.DeepCopy()
			if thawed.IsFrozen() {
				t.Fatalf("DeepCopy of frozen row is still frozen")
			}
			if err := thawed.SetProperty("x", 1); err != nil {
				t.Fatalf("thawed copy rejects mutation: %v", err)
			}
			again, err := g.Rels().Outgoing(a.ID(), "LINK")
			if err != nil || len(again) != 1 {
				t.Fatalf("re-read: %v", err)
			}
			if _, ok := again[0].GetProperty("x"); ok {
				t.Fatalf("thawed-copy mutation leaked into the store")
			}
		})
	}
}

// Bitemporal TX axis through the label door: the TxAt filter must
// reconstruct "as known then" for label mutations the same way it does for
// Update. The node is created valid from 1000 with {Thing, Keep}; RemoveLabel
// writes a version without a valid-time claim, so under UntimedFromTxFrom it
// starts at its own transaction instant. Each read asserts the exact label
// set, so a blend of the two versions fails.
func TestNodeAtTxSeesPreMutationLabelState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			n, err := g.Nodes().Add(ctx, []string{"Thing", "Keep"}, map[string]any{
				"tkg_valid_from": types.Instant(1000),
			})
			if err != nil {
				t.Fatalf("add: %v", err)
			}
			// NowTx reserves an instant strictly between the two writes.
			txBetween, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}
			if err := g.Nodes().RemoveLabel(ctx, n.ID(), "Thing"); err != nil {
				t.Fatalf("remove label: %v", err)
			}
			txAfter, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatalf("NowTx: %v", err)
			}

			labelsAt := func(vt, tx types.Instant) []string {
				t.Helper()
				row, err := g.Temporal().NodeAtTx(n.ID(), vt, tx)
				if err != nil {
					t.Fatalf("NodeAtTx(vt=%d, tx=%d): %v", vt, tx, err)
				}
				got := append([]string(nil), g.Nodes().Labels(row)...)
				slices.Sort(got)
				return got
			}
			pre := []string{"Keep", "Thing"}
			for _, c := range []struct {
				name   string
				vt, tx types.Instant
				want   []string
			}{
				// As known before the removal: the label is there.
				{"before removal", 1500, txBetween, pre},
				// As known after the removal, valid time 1500 is still
				// covered only by the first version: its full label set.
				{"after removal, old valid time", 1500, txAfter, pre},
				// The removal's own version answers from its start on.
				{"after removal, current", txAfter, txAfter, []string{"Keep"}},
			} {
				if got := labelsAt(c.vt, c.tx); !slices.Equal(got, c.want) {
					t.Errorf("%s: labels %v, want %v", c.name, got, c.want)
				}
			}
		})
	}
}
