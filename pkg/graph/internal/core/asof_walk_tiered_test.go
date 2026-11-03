package core

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// asOfChainAnswer is the whole-chain resolve the walk must agree with: the
// history and current row read in full, the as-of rule applied.
func asOfChainAnswer(g *Core, id types.NodeID, pin types.Instant) (*types.Node, error) {
	current, err := g.getCurrentNode(id)
	if err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
		return nil, err
	}
	hist, err := g.getNodeHistory(id)
	if err != nil {
		return nil, err
	}
	return g.resolveNodeChain(chainOf(hist, current), chainProbe{kind: probeAsOf, tx: pin, asOfCurrent: current != nil}, nil)
}

// asOfRender renders the whole as-of answer: the error class (absent by which
// sentinel, or the error), else the node's ID, labels, every property with its
// type, version and every temporal field. A walk that answers the wrong row,
// labels, properties or stamps renders differently from the chain.
func asOfRender(n *types.Node, err error) string {
	switch {
	case errors.Is(err, ErrNoVersionAsOf):
		return "absent: ErrNoVersionAsOf"
	case errors.Is(err, storepkg.ErrVersionNotFound):
		return "absent: ErrVersionNotFound"
	case err != nil:
		return "error: " + err.Error()
	case n == nil:
		return "nil node, nil error"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "id=%d v%d labels=[", n.ID(), n.Version())
	for i, tok := range n.AllLabelTokens() {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%d", tok.Value())
	}
	b.WriteString("] props={")
	props := n.Properties()
	sort.Slice(props, func(i, j int) bool { return props[i].Key < props[j].Key })
	for i, p := range props {
		if i > 0 {
			b.WriteByte(' ')
		}
		fmt.Fprintf(&b, "%s=%T(%v)", p.Key, p.Value, p.Value)
	}
	b.WriteString("}")
	if tm := n.Temporal(); tm != nil {
		fmt.Fprintf(&b, " valid=[%d,%d) tx=[%d,%d) created=%d updated=%d deleted=%d by=%q/%q",
			tm.ValidFrom, tm.ValidTo, tm.TxFrom, tm.TxTo, tm.CreatedAt, tm.UpdatedAt, tm.DeletedAt, tm.CreatedBy, tm.UpdatedBy)
	} else {
		b.WriteString(" temporal=nil")
	}
	txFrom, txTo, deletedAt := n.TxStamps()
	fmt.Fprintf(&b, " stamps=%d/%d/%d", txFrom, txTo, deletedAt)
	return b.String()
}

func asOfAbsent(r string) bool { return strings.HasPrefix(r, "absent: ") }

// TestTieredNodeAsOf_NoChainLoad: a TxPin read of a node whose history is all
// on its owner shard walks that shard and loads no chain, at any pin, on an
// event and a reference node, and answers what the whole chain answers.
func TestTieredNodeAsOf_NoChainLoad(t *testing.T) {
	t.Parallel()
	const versions = 200
	ts := newTestTieredStore(t)
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	for _, label := range []string{"Event", "Case"} {
		t.Run(label, func(t *testing.T) {
			n, err := g.Nodes.Add(t.Context(), []string{label}, map[string]any{"x": int64(0)})
			if err != nil {
				t.Fatal(err)
			}
			pins := make([]types.Instant, 0, versions)
			for i := 1; i < versions; i++ {
				if _, err := g.Nodes.Update(t.Context(), n.ID(), map[string]any{"x": int64(i)}); err != nil {
					t.Fatal(err)
				}
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatal(err)
				}
				pins = append(pins, pin)
			}
			for _, pin := range []types.Instant{pins[0], pins[versions/2], pins[len(pins)-1]} {
				want := asOfRender(asOfChainAnswer(g, n.ID(), pin))
				loaded := 0
				g.historyRead = func(rows int) { loaded += rows }
				got := asOfRender(g.Temporal.NodeAsOf(n.ID(), pin))
				g.historyRead = nil
				if got != want {
					t.Errorf("pin %d: NodeAsOf %s, chain %s", pin, got, want)
				}
				if loaded != 0 {
					t.Errorf("pin %d: NodeAsOf loaded %d history rows, want no chain load", pin, loaded)
				}
			}
		})
	}
}

// TestTieredNodeAsOf_WholeNodeAgreesWithChain: versions that differ in labels,
// the property set, property types and stamps, across a delete and a
// re-create under the same ID, answer the whole node the chain answers at
// every pin, and every pin walks the live node's owner shard.
func TestTieredNodeAsOf_WholeNodeAgreesWithChain(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := t.Context()
	for _, label := range []string{"Event", "Case"} {
		t.Run(label, func(t *testing.T) {
			type step struct {
				name string
				do   func(id types.NodeID) error
			}
			n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"x": int64(0), "s": "a"})
			if err != nil {
				t.Fatal(err)
			}
			id := n.ID()
			steps := []step{
				{"add props", func(id types.NodeID) error {
					_, err := g.Nodes.Update(ctx, id, map[string]any{"y": 1.5, "b": true})
					return err
				}},
				{"add label", func(id types.NodeID) error { return g.Nodes.AddLabel(ctx, id, "Hot") }},
				{"retype x", func(id types.NodeID) error { return g.Nodes.SetProperty(ctx, id, "x", "zero") }},
				{"drop y", func(id types.NodeID) error { return g.Nodes.DeleteProperty(ctx, id, "y") }},
				{"remove label", func(id types.NodeID) error { return g.Nodes.RemoveLabel(ctx, id, "Hot") }},
				{"add label again", func(id types.NodeID) error { return g.Nodes.AddLabel(ctx, id, "Cold") }},
			}
			pins := []types.Instant{n.Temporal().TxFrom - 1}
			now := func() {
				pin, err := g.Temporal.NowTx()
				if err != nil {
					t.Fatal(err)
				}
				pins = append(pins, pin)
			}
			now()
			for _, s := range steps {
				if err := s.do(id); err != nil {
					t.Fatalf("%s: %v", s.name, err)
				}
				now()
			}
			if err := g.Nodes.Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
			now()
			if _, created, err := g.Nodes.AddByIDIfAbsent(ctx, id, []string{label, "Reborn"}, map[string]any{"z": int64(9)}); err != nil || !created {
				t.Fatalf("re-create: created=%v err=%v", created, err)
			}
			now()
			if err := g.Nodes.SetProperty(ctx, id, "z", int64(10)); err != nil {
				t.Fatal(err)
			}
			now()

			seen := map[string]int{}
			for i, pin := range pins {
				want := asOfRender(asOfChainAnswer(g, id, pin))
				loaded := 0
				g.historyRead = func(rows int) { loaded += rows }
				got := asOfRender(g.Temporal.NodeAsOf(id, pin))
				g.historyRead = nil
				if got != want {
					t.Errorf("pin %d (%d): NodeAsOf\n  %s\nchain\n  %s", i, pin, got, want)
				}
				if prev, dup := seen[want]; dup && !asOfAbsent(want) {
					t.Errorf("pins %d and %d answer the same node: the steps do not differ", prev, i)
				}
				seen[want] = i
				if loaded != 0 {
					t.Errorf("pin %d: NodeAsOf loaded %d history rows, want the owner-shard walk", i, loaded)
				}
			}
			if !asOfAbsent(asOfRender(g.Temporal.NodeAsOf(id, pins[0]))) {
				t.Error("pin before create answered a node")
			}
		})
	}
}

// TestTieredNodeAsOf_SplitHistoryAgreesWithChain: where a node's rows do not
// all sit on its owner shard or it is live nowhere, the answer stays the whole
// chain's: restored from the archive (its old rows stay there), archived, and
// deleted.
func TestTieredNodeAsOf_SplitHistoryAgreesWithChain(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := t.Context()
	now := func() types.Instant {
		pin, err := g.Temporal.NowTx()
		if err != nil {
			t.Fatal(err)
		}
		return pin
	}
	update := func(id types.NodeID, x int64) types.Instant {
		if _, err := g.Nodes.Update(ctx, id, map[string]any{"x": x}); err != nil {
			t.Fatal(err)
		}
		return now()
	}
	check := func(t *testing.T, id types.NodeID, pins []types.Instant, wantAnswers bool) {
		t.Helper()
		answered := false
		for _, pin := range pins {
			want := asOfRender(asOfChainAnswer(g, id, pin))
			got := asOfRender(g.Temporal.NodeAsOf(id, pin))
			if got != want {
				t.Errorf("pin %d: NodeAsOf %s, chain %s", pin, got, want)
			}
			answered = answered || !asOfAbsent(got)
		}
		if wantAnswers && !answered {
			t.Error("no pin answered: the case tests nothing")
		}
	}

	restored, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"x": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	pins := []types.Instant{now(), update(restored.ID(), 1), update(restored.ID(), 2)}
	if err := ts.ArchiveNode(restored.ID()); err != nil {
		t.Fatal(err)
	}
	t.Run("archived", func(t *testing.T) { check(t, restored.ID(), pins, true) })
	if err := ts.RestoreNode(restored.ID()); err != nil {
		t.Fatal(err)
	}
	pins = append(pins, update(restored.ID(), 3))
	t.Run("restored", func(t *testing.T) {
		check(t, restored.ID(), pins, true)
		for i, pin := range pins[:3] {
			if got, want := asOfRender(g.Temporal.NodeAsOf(restored.ID(), pin)), fmt.Sprintf("x=int64(%d)", i); !strings.Contains(got, want) {
				t.Errorf("pin %d: %s, want the row with %s", i, got, want)
			}
		}
	})

	for _, label := range []string{"Event", "Case"} {
		gone, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"x": int64(0)})
		if err != nil {
			t.Fatal(err)
		}
		pins := []types.Instant{now(), update(gone.ID(), 1)}
		if err := g.Nodes.Delete(ctx, gone.ID()); err != nil {
			t.Fatal(err)
		}
		pins = append(pins, now())
		t.Run("deleted/"+label, func(t *testing.T) { check(t, gone.ID(), pins, true) })
	}
}

// tieredAsOfOldRow is an old row of id, recorded at txFrom, as a re-import or an
// earlier archive leaves in another part of the store.
func tieredAsOfOldRow(id types.NodeID, like *types.Node, txFrom types.Instant) *types.Node {
	old := types.NewNode(id, like.PrimaryLabelToken().Value(), nil)
	old.SetTemporal(&types.TemporalMetadata{TxFrom: txFrom, TxTo: txFrom + 1})
	return old
}

// TestTieredNodeAsOf_RowsOffTheOwnerShard: rows of a live node that sit off its
// owner shard still answer: in the archive under a live reference node, and in
// the archive under an event node created over them (an earlier life). The
// owner shard alone holds no row by the pin.
func TestTieredNodeAsOf_RowsOffTheOwnerShard(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := t.Context()
	seed, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.ArchiveNode(seed.ID()); err != nil {
		t.Fatal(err)
	}
	archive := ts.RefArchiveForTest().Load()

	check := func(t *testing.T, id types.NodeID, pin types.Instant) {
		t.Helper()
		want := asOfRender(asOfChainAnswer(g, id, pin))
		if asOfAbsent(want) {
			t.Fatal("the chain does not answer: the case tests nothing")
		}
		if got := asOfRender(g.Temporal.NodeAsOf(id, pin)); got != want {
			t.Errorf("NodeAsOf %s, chain %s", got, want)
		}
	}
	t.Run("live reference node over archive rows", func(t *testing.T) {
		n, err := g.Nodes.Add(ctx, []string{"Case"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		pin := n.Temporal().TxFrom - 500
		if err := archive.PutNodeVersion(n.ID(), 0, tieredAsOfOldRow(n.ID(), n, pin-500)); err != nil {
			t.Fatal(err)
		}
		check(t, n.ID(), pin-500)
	})
	t.Run("event node created over archive rows", func(t *testing.T) {
		like, err := g.Nodes.Add(ctx, []string{"Event"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		id := like.ID() + 1
		pin := like.Temporal().TxFrom - 500
		if err := archive.PutNodeVersion(id, 0, tieredAsOfOldRow(id, like, pin-500)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := g.Nodes.AddByIDIfAbsent(ctx, id, []string{"Event"}, nil); err != nil {
			t.Fatal(err)
		}
		check(t, id, pin-500)
	})
}

// TestTieredNodeAsOf_ArchivedDuringWalk: the current row leaves the reference
// shard after the walk saw it live and before it reads. Pinned at the current
// row, the shard's history alone would answer an older row.
func TestTieredNodeAsOf_ArchivedDuringWalk(t *testing.T) {
	t.Parallel()
	ts := newTestTieredStore(t)
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := t.Context()
	n, err := g.Nodes.Add(ctx, []string{"Case"}, map[string]any{"x": int64(0)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes.Update(ctx, n.ID(), map[string]any{"x": int64(1)}); err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatal(err)
	}
	fired := false
	ts.SetAsOfWalkGapHookForTest(func() {
		if !fired {
			fired = true
			if err := ts.ArchiveNode(n.ID()); err != nil {
				t.Error(err)
			}
		}
	})
	got, err := g.Temporal.NodeAsOf(n.ID(), pin)
	ts.SetAsOfWalkGapHookForTest(nil)
	if !fired {
		t.Fatal("the walk never ran")
	}
	if err != nil {
		t.Fatal(err)
	}
	if x, _ := got.GetProperty("x"); x != int64(1) {
		t.Errorf("x = %v, want the current row's 1", x)
	}
}
