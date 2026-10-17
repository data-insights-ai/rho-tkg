package core

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 57: NodesAtTxByIDs / RelsAtTxByIDs answer, per element, exactly what
// NodeAtTx / RelAtTx answers for that ID (nil on not-found / no version valid).
// The per-ID door is the oracle; every backend.

// batKit is one entity kind's doors for the oracle test; rows render to a
// comparable string ("<nil>" for no row).
type batKit struct {
	add   func(t *testing.T, props map[string]any) int64
	upd   func(t *testing.T, id int64, props map[string]any)
	del   func(t *testing.T, id int64)
	imp   func(t *testing.T, id int64, props map[string]any)
	one   func(id int64, validAt, txAt types.Instant) (string, error)
	batch func(ids []int64, validAt, txAt types.Instant) ([]string, error)
}

const batNil = "<nil>"

func batRenderNode(n *types.Node) string {
	if n == nil {
		return batNil
	}
	return fmt.Sprintf("%d v%d tx%d %v", n.ID(), n.Version(), beliefTx(n.Temporal()), n.PropertiesMap())
}

func batRenderRel(r *types.Relationship) string {
	if r == nil {
		return batNil
	}
	return fmt.Sprintf("%d v%d tx%d %d->%d %v", r.ID(), r.Version(), beliefTx(r.Temporal()), r.StartNodeID(), r.EndNodeID(), r.PropertiesMap())
}

func batNodeKit(g *Core) batKit {
	ctx := context.Background()
	return batKit{
		add: func(t *testing.T, props map[string]any) int64 {
			n, err := g.Nodes.Add(ctx, []string{"Ev"}, props)
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			return int64(n.ID())
		},
		upd: func(t *testing.T, id int64, props map[string]any) {
			if _, err := g.Nodes.Update(ctx, types.NodeID(id), props); err != nil {
				t.Fatalf("Update: %v", err)
			}
		},
		del: func(t *testing.T, id int64) {
			if err := g.Nodes.Delete(ctx, types.NodeID(id)); err != nil {
				t.Fatalf("Delete: %v", err)
			}
		},
		imp: func(t *testing.T, id int64, props map[string]any) {
			if _, err := g.Nodes.Import(ctx, types.NodeID(id), []string{"Ev"}, props); err != nil {
				t.Fatalf("Import: %v", err)
			}
		},
		one: func(id int64, validAt, txAt types.Instant) (string, error) {
			n, err := g.Temporal.NodeAtTx(types.NodeID(id), validAt, txAt)
			if errors.Is(err, storepkg.ErrNodeNotFound) || errors.Is(err, storepkg.ErrNoVersionValidAt) {
				return batNil, nil
			}
			return batRenderNode(n), err
		},
		batch: func(ids []int64, validAt, txAt types.Instant) ([]string, error) {
			var nids []types.NodeID
			if ids != nil {
				nids = make([]types.NodeID, len(ids))
			}
			for i, id := range ids {
				nids[i] = types.NodeID(id)
			}
			ns, err := g.Temporal.NodesAtTxByIDs(nids, validAt, txAt)
			if err != nil {
				return nil, err
			}
			if ns == nil {
				return nil, errors.New("nil result slice")
			}
			out := make([]string, len(ns))
			for i, n := range ns {
				out[i] = batRenderNode(n)
			}
			return out, nil
		},
	}
}

func batRelKit(t *testing.T, g *Core) batKit {
	ctx := context.Background()
	s, e := txbEndpoints(t, g)
	sn, err := g.Nodes.Get(ctx, s)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	en, err := g.Nodes.Get(ctx, e)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	return batKit{
		add: func(t *testing.T, props map[string]any) int64 {
			r, err := g.Rels.AddByID(ctx, "R", s, e, props)
			if err != nil {
				t.Fatalf("AddByID: %v", err)
			}
			return int64(r.ID())
		},
		upd: func(t *testing.T, id int64, props map[string]any) {
			if _, err := g.Rels.Update(ctx, types.RelID(id), props); err != nil {
				t.Fatalf("Update: %v", err)
			}
		},
		del: func(t *testing.T, id int64) {
			if err := g.Rels.Delete(ctx, types.RelID(id)); err != nil {
				t.Fatalf("Delete: %v", err)
			}
		},
		imp: func(t *testing.T, id int64, props map[string]any) {
			if _, err := g.Rels.Import(ctx, types.RelID(id), "R", sn, en, props); err != nil {
				t.Fatalf("Import: %v", err)
			}
		},
		one: func(id int64, validAt, txAt types.Instant) (string, error) {
			r, err := g.Temporal.RelAtTx(types.RelID(id), validAt, txAt)
			if errors.Is(err, storepkg.ErrRelNotFound) || errors.Is(err, storepkg.ErrNoVersionValidAt) {
				return batNil, nil
			}
			return batRenderRel(r), err
		},
		batch: func(ids []int64, validAt, txAt types.Instant) ([]string, error) {
			var rids []types.RelID
			if ids != nil {
				rids = make([]types.RelID, len(ids))
			}
			for i, id := range ids {
				rids[i] = types.RelID(id)
			}
			rs, err := g.Temporal.RelsAtTxByIDs(rids, validAt, txAt)
			if err != nil {
				return nil, err
			}
			if rs == nil {
				return nil, errors.New("nil result slice")
			}
			out := make([]string, len(rs))
			for i, r := range rs {
				out[i] = batRenderRel(r)
			}
			return out, nil
		},
	}
}

// TestNodesAtTxByIDsMatchesNodeAtTx: element-wise equality with NodeAtTx / RelAtTx
// over a (validAt, txAt) grid (updated, deleted, re-imported, created-after-pin,
// unknown, duplicate IDs), plus empty input and the call-level errors.
func TestNodesAtTxByIDsMatchesNodeAtTx(t *testing.T) {
	t.Parallel()
	for _, b := range txbBackends() {
		for _, rel := range []bool{false, true} {
			name := b.name + "/nodes"
			if rel {
				name = b.name + "/rels"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				g := b.open(t, false)
				clk := useTestClock(t, g)
				k := batNodeKit(g)
				if rel {
					k = batRelKit(t, g)
				}
				batOracleGrid(t, g, clk, k)
			})
		}
	}
}

// TestRelsAtTxByIDsMatchesRelAtTx names the relationship half of the oracle (the
// shared grid runs it in the "/rels" subtests); this one runs memory alone.
func TestRelsAtTxByIDsMatchesRelAtTx(t *testing.T) {
	t.Parallel()
	g := txbBackends()[0].open(t, false)
	clk := useTestClock(t, g)
	batOracleGrid(t, g, clk, batRelKit(t, g))
}

func batOracleGrid(t *testing.T, g *Core, clk *testClock, k batKit) {
	t.Helper()
	te := g.Temporal
	pin := func() types.Instant {
		p, err := te.NowTx()
		if err != nil {
			t.Fatalf("NowTx: %v", err)
		}
		return p
	}
	step := func() { clk.Advance(10 * time.Millisecond) }

	updated := k.add(t, map[string]any{"tkg_valid_from": types.Instant(1000), "v": int64(1)})
	deleted := k.add(t, map[string]any{"tkg_valid_from": types.Instant(1000), "v": int64(1)})
	reimp := k.add(t, map[string]any{"tkg_valid_from": types.Instant(1000), "life": int64(1)})
	step()
	p1 := pin()
	step()
	k.upd(t, updated, map[string]any{"v": int64(2)})
	late := k.add(t, map[string]any{"v": int64(9)})
	step()
	p2 := pin()
	step()
	k.del(t, deleted)
	k.del(t, reimp)
	step()
	p3 := pin()
	step()
	k.imp(t, reimp, map[string]any{"life": int64(2)})
	step()
	p4 := pin()
	unknown := updated + 1<<22

	ids := []int64{updated, deleted, reimp, late, unknown, updated, reimp, deleted}
	validAts := []types.Instant{500, 1000, 1500, p1, p2, p3, p4}
	answered, nils := 0, 0
	for _, txAt := range []types.Instant{p1, p2, p3, p4} {
		for _, validAt := range validAts {
			got, err := k.batch(ids, validAt, txAt)
			if err != nil {
				t.Fatalf("batch(validAt %d, txAt %d): %v", validAt, txAt, err)
			}
			if len(got) != len(ids) {
				t.Fatalf("batch(validAt %d, txAt %d): %d results for %d ids", validAt, txAt, len(got), len(ids))
			}
			for i, id := range ids {
				want := batOne(t, k, id, validAt, txAt)
				if got[i] != want {
					t.Errorf("validAt %d txAt %d ids[%d]=%d: batch %q, per-ID %q", validAt, txAt, i, id, got[i], want)
				}
				if want == batNil {
					nils++
				} else {
					answered++
				}
			}
		}
	}
	if answered == 0 || nils == 0 {
		t.Fatalf("grid degenerate: answered %d, nil %d", answered, nils)
	}
	// At p1 the update was not yet recorded: a history row answers.
	hist, err := k.batch([]int64{updated}, p4, p1)
	if err != nil {
		t.Fatalf("history probe: %v", err)
	}
	if cur := batOne(t, k, updated, p4, p4); hist[0] == batNil || hist[0] == cur {
		t.Fatalf("history version at p1 = %q, current %q", hist[0], cur)
	}
	// Both lives of the re-imported ID answer at their pins.
	l1, _ := k.batch([]int64{reimp}, 1500, p2)
	l2, _ := k.batch([]int64{reimp}, p4, p4)
	if len(l1) != 1 || len(l2) != 1 || l1[0] == batNil || l2[0] == batNil || l1[0] == l2[0] {
		t.Fatalf("re-imported lives: %q / %q", l1, l2)
	}

	// Empty input: empty slice, nil error.
	for _, empty := range [][]int64{nil, {}} {
		got, err := k.batch(empty, p4, p4)
		if err != nil || len(got) != 0 {
			t.Fatalf("empty ids %v: %v, %v", empty, got, err)
		}
	}

	// Call-level errors.
	peek, err := te.PeekTx()
	if err != nil {
		t.Fatalf("PeekTx: %v", err)
	}
	for _, c := range []struct {
		name string
		ids  []int64
		pin  types.Instant
		want error
	}{
		{"pin 0", ids, 0, ErrInvalidTimeRange}, // NodeAtTx's "no pin"; the batch needs a pin
		{"pin -1", ids, -1, nil},               // ordinary: nothing recorded yet, every answer nil (round 11)
		{"pin above clock", ids, peek + 3_600_000, ErrTxPinTooNew},
		{"pin above clock, empty ids", nil, peek + 3_600_000, ErrTxPinTooNew},
		{"id 0", []int64{updated, 0}, p4, storepkg.ErrInvalidStoreMutation},
		{"id -5", []int64{-5}, p4, storepkg.ErrInvalidStoreMutation},
	} {
		if _, err := k.batch(c.ids, p4, c.pin); !errors.Is(err, c.want) || (c.want == nil && err != nil) {
			t.Fatalf("%s: err = %v, want %v", c.name, err, c.want)
		}
	}
	if _, err := k.batch(ids, p4, peek); err != nil {
		t.Fatalf("pin == PeekTx: %v", err)
	}
	if err := g.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := k.batch(ids, p4, p4); !errors.Is(err, ErrGraphClosed) {
		t.Fatalf("closed: %v, want ErrGraphClosed", err)
	}
}

func batOne(t *testing.T, k batKit, id int64, validAt, txAt types.Instant) string {
	t.Helper()
	s, err := k.one(id, validAt, txAt)
	if err != nil {
		t.Fatalf("oracle(%d, %d, %d): %v", id, validAt, txAt, err)
	}
	return s
}

// TestAtTxByIDsGatesAsPerIDDoor: a member the per-ID door refuses
// (ErrHistoryCompacted, ErrRetentionExpired) fails the call with the same sentinel;
// a pin every member answers at returns rows. Memory (core-level gates).
func TestAtTxByIDsGatesAsPerIDDoor(t *testing.T) {
	t.Parallel()
	g := txbBackends()[0].open(t, false)
	useTestClock(t, g)
	ctx := context.Background()
	te := g.Temporal
	vf := map[string]any{"tkg_valid_from": types.Instant(1000)}
	a, err := g.Nodes.Add(ctx, []string{"A"}, vf)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	b, err := g.Nodes.Add(ctx, []string{"A"}, vf)
	if err != nil {
		t.Fatalf("add: %v", err)
	}
	q, err := g.Nodes.Add(ctx, []string{"Q"}, vf)
	if err != nil {
		t.Fatalf("add Q: %v", err)
	}
	r, err := g.Rels.AddByID(ctx, "R", a.ID(), b.ID(), vf)
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	r2, err := g.Rels.AddByID(ctx, "R", b.ID(), q.ID(), vf)
	if err != nil {
		t.Fatalf("add rel: %v", err)
	}
	p0, _ := te.NowTx()
	for i := 0; i < 2; i++ {
		if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"x": int64(i)}); err != nil {
			t.Fatalf("update: %v", err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"x": int64(i)}); err != nil {
			t.Fatalf("update rel: %v", err)
		}
	}
	if _, err := g.Admin.CompactHistoryNodes(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
		t.Fatalf("compact: %v", err)
	}
	if _, err := g.Admin.CompactHistoryRels(ctx, RetentionPolicy{KeepVersions: 1}); err != nil {
		t.Fatalf("compact rels: %v", err)
	}
	pin, _ := te.NowTx()
	if err := g.advanceRetentionWatermark(nodeLabelToken(t, g, "Q"), pin); err != nil {
		t.Fatalf("advanceRetentionWatermark: %v", err)
	}
	sentinels := []error{ErrHistoryCompacted, ErrRetentionExpired}
	sentinelOf := func(err error) error {
		for _, s := range sentinels {
			if errors.Is(err, s) {
				return s
			}
		}
		return err
	}
	refused := 0
	for _, p := range []types.Instant{p0, pin - 1, pin} {
		nodeSets := [][]types.NodeID{{q.ID(), a.ID()}, {b.ID()}, {q.ID()}, {a.ID()}}
		for _, ids := range nodeSets {
			var want error
			for _, id := range ids {
				if _, err := te.NodeAtTx(id, 2000, p); err != nil && !batAbsent(err) && want == nil {
					want = sentinelOf(err)
				}
			}
			got, err := te.NodesAtTxByIDs(ids, 2000, p)
			if want == nil && (err != nil || len(got) != len(ids)) {
				t.Fatalf("NodesAtTxByIDs(%v, pin %d) = %v, %v; want rows", ids, p, got, err)
			}
			if want != nil {
				refused++
				if !errors.Is(err, want) || got != nil {
					t.Fatalf("NodesAtTxByIDs(%v, pin %d) = %v, %v; want %v", ids, p, got, err, want)
				}
			}
		}
		relSets := [][]types.RelID{{r2.ID(), r.ID()}, {r2.ID()}}
		for _, ids := range relSets {
			var want error
			for _, id := range ids {
				if _, err := te.RelAtTx(id, 2000, p); err != nil && !batAbsent(err) && want == nil {
					want = sentinelOf(err)
				}
			}
			got, err := te.RelsAtTxByIDs(ids, 2000, p)
			if want == nil && (err != nil || len(got) != len(ids)) {
				t.Fatalf("RelsAtTxByIDs(%v, pin %d) = %v, %v; want rows", ids, p, got, err)
			}
			if want != nil {
				refused++
				if !errors.Is(err, want) || got != nil {
					t.Fatalf("RelsAtTxByIDs(%v, pin %d) = %v, %v; want %v", ids, p, got, err, want)
				}
			}
		}
	}
	if refused == 0 {
		t.Fatal("no gate refused a member: the fixture does not exercise the gates")
	}
}

// batAbsent reports a per-ID answer the batch doors turn into a nil element.
func batAbsent(err error) bool {
	return errors.Is(err, storepkg.ErrNodeNotFound) || errors.Is(err, storepkg.ErrRelNotFound) || errors.Is(err, storepkg.ErrNoVersionValidAt)
}
