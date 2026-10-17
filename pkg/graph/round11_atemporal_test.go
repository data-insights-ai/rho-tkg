package graph_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/ingest"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11, atemporal mode (René and Markus, 2026-10-17: "atemporal is just a
// flag"). Under Config.UntimedValidStart = UntimedAtemporal a write without a
// valid time makes no valid-time claim. An untimed write applies to EVERY
// valid-time segment of the entity and keeps the segment boundaries, as
// SQL:2011's UPDATE without FOR PORTION OF: each segment gets a new version in
// the new belief with the named properties changed and the others kept per
// segment; the old versions end in transaction time only; existence does not
// change. A pure atemporal entity has one whole-axis segment. An untimed
// delete ends the entity in transaction time only. A timed write (an explicit
// valid-from / valid-to) splits segments as in every mode.

const r11Far = types.Instant(1) << 52 // a valid instant far after every write

func r11Atemporal(c *graphpkg.Config) { c.UntimedValidStart = graphpkg.UntimedAtemporal }

// r11NowAfter is the commit clock after a short sleep, so the next write is
// stamped after it (a pin between two writes).
func r11NowAfter(t *testing.T, g *graphpkg.Graph) types.Instant {
	t.Helper()
	time.Sleep(3 * time.Millisecond)
	now, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(3 * time.Millisecond)
	return now
}

// r11KAt is property key of node id at valid instant v believed at pin (0:
// no pin): its value, or "none" when no version is valid there.
func r11KAt(t *testing.T, g *graphpkg.Graph, id types.NodeID, key string, v, pin types.Instant) string {
	t.Helper()
	var n *types.Node
	var err error
	if pin == 0 {
		n, err = g.Temporal().NodeAt(id, v)
	} else {
		n, err = g.Temporal().NodeAtTx(id, v, pin)
	}
	if errors.Is(err, graphpkg.ErrNoVersionValidAt) || errors.Is(err, graphpkg.ErrNodeNotFound) {
		return "none"
	}
	if err != nil {
		t.Fatalf("node %v at %d pin %d: %v", id, v, pin, err)
	}
	val, ok := n.GetProperty(key)
	if !ok {
		return "unset"
	}
	return fmt.Sprint(val)
}

// r11RelKAt is r11KAt for a relationship.
func r11RelKAt(t *testing.T, g *graphpkg.Graph, id types.RelID, key string, v, pin types.Instant) string {
	t.Helper()
	var r *types.Relationship
	var err error
	if pin == 0 {
		r, err = g.Temporal().RelAt(id, v)
	} else {
		r, err = g.Temporal().RelAtTx(id, v, pin)
	}
	if errors.Is(err, graphpkg.ErrNoVersionValidAt) || errors.Is(err, graphpkg.ErrRelNotFound) {
		return "none"
	}
	if err != nil {
		t.Fatalf("rel %v at %d pin %d: %v", id, v, pin, err)
	}
	val, ok := r.GetProperty(key)
	if !ok {
		return "unset"
	}
	return fmt.Sprint(val)
}

func r11Want(t *testing.T, what, got, want string) {
	t.Helper()
	if got != want {
		t.Errorf("%s: %s, want %s", what, got, want)
	}
}

// TestRound11AtemporalPure: an untimed create holds over the whole valid axis
// (EffectiveValidFrom -inf, the door reports UntimedAtemporal); an untimed
// update is a new whole-axis version in the new belief, so a read AT any
// valid time sees the version believed at its transaction coordinate (AS OF
// before the update: the old value everywhere; after: the new one), and
// the effective timeline is one whole-axis segment at either pin. Nodes and
// relationships, every backend.
func TestRound11AtemporalPure(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11Atemporal) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			if mode, complete, err := g.Admin().UntimedValidStart(); err != nil || mode != graphpkg.UntimedAtemporal || !complete {
				t.Fatalf("UntimedValidStart = %d, %v, %v; want UntimedAtemporal", mode, complete, err)
			}
			n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), "j": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			m, err := g.Nodes().Add(ctx, []string{"A"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "E", n, m, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if got := n.EffectiveValidFrom(); got != types.UnboundedValidFrom {
				t.Errorf("an untimed create starts at %d, want -inf", got)
			}
			before := r11NowAfter(t, g)
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			after := r11NowAfter(t, g)
			for _, v := range []types.Instant{types.UnboundedValidFrom + 1, -1_000_000, 0, before, after, r11Far} {
				r11Want(t, fmt.Sprintf("node AT %d now", v), r11KAt(t, g, n.ID(), "k", v, 0), "2")
				r11Want(t, fmt.Sprintf("node AT %d AS OF before", v), r11KAt(t, g, n.ID(), "k", v, before), "1")
				r11Want(t, fmt.Sprintf("node AT %d AS OF after", v), r11KAt(t, g, n.ID(), "k", v, after), "2")
				r11Want(t, fmt.Sprintf("node j AT %d now", v), r11KAt(t, g, n.ID(), "j", v, 0), "1")
				r11Want(t, fmt.Sprintf("rel AT %d now", v), r11RelKAt(t, g, r.ID(), "k", v, 0), "2")
				r11Want(t, fmt.Sprintf("rel AT %d AS OF before", v), r11RelKAt(t, g, r.ID(), "k", v, before), "1")
			}
			cur, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			if got := cur.EffectiveValidFrom(); got != types.UnboundedValidFrom {
				t.Errorf("the updated version starts at %d, want -inf (no valid-time claim)", got)
			}
			for _, pin := range []types.Instant{before, after} {
				segs, err := g.Temporal().NodeEffectiveTimeline(n.ID(), pin)
				if err != nil || len(segs) != 1 || segs[0].ValidFrom != types.UnboundedValidFrom || segs[0].ValidTo != 0 {
					t.Errorf("timeline at pin %d: %+v, %v; want one whole-axis segment", pin, segs, err)
				}
			}
			// A second update, a label change: still one whole-axis segment.
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(3)}); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().AddLabel(ctx, n.ID(), "B"); err != nil {
				t.Fatal(err)
			}
			for _, v := range []types.Instant{-1_000_000, r11Far} {
				got, err := g.Temporal().NodeAt(n.ID(), v)
				if err != nil {
					t.Fatalf("AT %d after a label change: %v", v, err)
				}
				if !g.Nodes().HasLabel(got, "B") {
					t.Errorf("AT %d: label B missing after an untimed label change", v)
				}
				r11Want(t, fmt.Sprintf("node AT %d after the label change", v), r11KAt(t, g, n.ID(), "k", v, 0), "3")
			}
			segs, err := g.Temporal().NodeEffectiveTimeline(n.ID(), r11NowAfter(t, g))
			if err != nil || len(segs) != 1 || segs[0].ValidFrom != types.UnboundedValidFrom {
				t.Errorf("timeline after a label change: %+v, %v; want one whole-axis segment", segs, err)
			}
		})
	}
}

// TestRound11AtemporalMixed: an entity with two explicit valid-time segments
// (k=1 on [100,200), k=2 on [200,open), j=1 on both) takes an untimed partial
// update j=9: each segment keeps its k and its boundaries and gets j=9; no
// segment appears before 100 (existence unchanged); AS OF before the update
// sees j=1 in both. Nodes and relationships, every backend.
func TestRound11AtemporalMixed(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11Atemporal) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), "j": int64(1), types.ShadowValidFrom: types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			m, err := g.Nodes().Add(ctx, []string{"A"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "E", m, n, map[string]any{"k": int64(1), "j": int64(1), types.ShadowValidFrom: types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), 200, 0, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetRelVersionInterval(ctx, r.ID(), 200, 0, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			before := r11NowAfter(t, g)
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"j": int64(9)}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(ctx, r.ID(), map[string]any{"j": int64(9)}); err != nil {
				t.Fatal(err)
			}
			after := r11NowAfter(t, g)
			for _, c := range []struct {
				v            types.Instant
				k, jNow, jAt string
			}{
				{50, "none", "none", "none"},
				{150, "1", "9", "1"},
				{250, "2", "9", "1"},
				{r11Far, "2", "9", "1"},
			} {
				r11Want(t, fmt.Sprintf("node k AT %d", c.v), r11KAt(t, g, n.ID(), "k", c.v, 0), c.k)
				r11Want(t, fmt.Sprintf("node j AT %d", c.v), r11KAt(t, g, n.ID(), "j", c.v, 0), c.jNow)
				r11Want(t, fmt.Sprintf("node j AT %d AS OF after", c.v), r11KAt(t, g, n.ID(), "j", c.v, after), c.jNow)
				r11Want(t, fmt.Sprintf("node j AT %d AS OF before", c.v), r11KAt(t, g, n.ID(), "j", c.v, before), c.jAt)
				r11Want(t, fmt.Sprintf("rel k AT %d", c.v), r11RelKAt(t, g, r.ID(), "k", c.v, 0), c.k)
				r11Want(t, fmt.Sprintf("rel j AT %d", c.v), r11RelKAt(t, g, r.ID(), "j", c.v, 0), c.jNow)
				r11Want(t, fmt.Sprintf("rel j AT %d AS OF before", c.v), r11RelKAt(t, g, r.ID(), "j", c.v, before), c.jAt)
			}
			segs, err := g.Temporal().NodeEffectiveTimeline(n.ID(), after)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, s := range segs {
				j, _ := s.Node.GetProperty("j")
				got = append(got, fmt.Sprintf("[%d,%d) j=%v", s.ValidFrom, s.ValidTo, j))
			}
			if want := "[[100,200) j=9 [200,0) j=9]"; fmt.Sprint(got) != want {
				t.Errorf("timeline after the untimed update: %v, want %s (boundaries kept)", got, want)
			}
			// A segment that already holds the change gets no new version: k=2
			// changes [100,200) only; an update that changes nothing writes nothing.
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			segs2, err := g.Temporal().NodeEffectiveTimeline(n.ID(), r11NowAfter(t, g))
			if err != nil || len(segs2) != 2 {
				t.Fatalf("timeline after k=2: %+v, %v", segs2, err)
			}
			if segs2[1].Node.Version() != segs[1].Node.Version() {
				t.Errorf("the unchanged segment [200,open) got a new version %d (was %d)", segs2[1].Node.Version(), segs[1].Node.Version())
			}
			if segs2[0].Node.Version() == segs[0].Node.Version() {
				t.Errorf("the changed segment [100,200) kept version %d", segs2[0].Node.Version())
			}
			cur, err := g.Nodes().Get(ctx, n.ID())
			if err != nil {
				t.Fatal(err)
			}
			again, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2), "j": int64(9)})
			if err != nil || again.Version() != cur.Version() {
				t.Errorf("a no-op untimed update: version %d, %v; want the current %d unchanged", again.Version(), err, cur.Version())
			}
		})
	}
}

// TestRound11AtemporalTimedSplitAndDelete: a timed write onto a pure atemporal
// entity splits its whole-axis segment (FOR PORTION OF); an untimed delete
// ends the entity in transaction time only: at the head it is valid nowhere,
// AS OF before the delete it is valid everywhere. Nodes and relationships,
// every backend.
func TestRound11AtemporalTimedSplitAndDelete(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11Atemporal) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			m, err := g.Nodes().Add(ctx, []string{"A"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(ctx, "E", m, n, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), 100, 200, map[string]any{"k": int64(5)}); err != nil {
				t.Fatal(err)
			}
			for v, want := range map[types.Instant]string{50: "1", 150: "5", 250: "1", r11Far: "1"} {
				r11Want(t, fmt.Sprintf("split AT %d", v), r11KAt(t, g, n.ID(), "k", v, 0), want)
			}
			before := r11NowAfter(t, g)
			if err := g.Rels().Delete(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().Delete(ctx, m.ID()); err != nil {
				t.Fatal(err)
			}
			for _, v := range []types.Instant{-1_000_000, r11Far} {
				r11Want(t, fmt.Sprintf("deleted node AT %d now", v), r11KAt(t, g, m.ID(), "k", v, 0), "none")
				r11Want(t, fmt.Sprintf("deleted node AT %d AS OF before", v), r11KAt(t, g, m.ID(), "k", v, before), "unset")
				r11Want(t, fmt.Sprintf("deleted rel AT %d now", v), r11RelKAt(t, g, r.ID(), "k", v, 0), "none")
				r11Want(t, fmt.Sprintf("deleted rel AT %d AS OF before", v), r11RelKAt(t, g, r.ID(), "k", v, before), "1")
			}
			after := r11NowAfter(t, g)
			if segs, err := g.Temporal().NodeEffectiveTimeline(m.ID(), after); err != nil || len(segs) != 0 {
				t.Errorf("timeline of the deleted node at the head: %+v, %v; want none", segs, err)
			}
			if segs, err := g.Temporal().NodeEffectiveTimeline(m.ID(), before); err != nil || len(segs) != 1 || segs[0].ValidFrom != types.UnboundedValidFrom || segs[0].ValidTo != 0 {
				t.Errorf("timeline of the deleted node AS OF before: %+v, %v; want one whole-axis segment", segs, err)
			}
			var at []string
			if err := g.Temporal().ForEachNodeEffectiveByLabel("A", after, func(s temporal.NodeSegment) bool {
				at = append(at, fmt.Sprint(s.Node.ID()))
				return true
			}); err != nil {
				t.Fatal(err)
			}
			for _, id := range at {
				if id == fmt.Sprint(m.ID()) {
					t.Errorf("the deleted node is in the label scan at the head")
				}
			}
		})
	}
}

// TestRound11AtemporalReopenAndMismatch: the mode is fixed at store creation:
// a durable store created atemporal reopens under UntimedDefault as atemporal
// (its updates stay whole-axis) and refuses another mode; a store with rows
// written under another mode refuses UntimedAtemporal; SetUntimedValidStart
// cannot name it (ErrInvalidUntimedValidStart) nor leave it.
func TestRound11AtemporalReopenAndMismatch(t *testing.T) {
	ctx := context.Background()
	for _, b := range uvsDurable {
		t.Run(b.name, func(t *testing.T) {
			dir := t.TempDir()
			g, _ := uvsMustOpen(t, b, dir, graphpkg.UntimedAtemporal)
			n, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedUnbounded); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Errorf("converting an atemporal store: %v, want ErrUntimedModeMismatch", err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			for _, other := range []graphpkg.UntimedValidStart{graphpkg.UntimedUnbounded, graphpkg.UntimedFromTxFrom} {
				if g, _, err := uvsOpen(t, b, dir, other); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
					if g != nil {
						_ = g.Close()
					}
					t.Errorf("atemporal store opened under %d: %v, want ErrUntimedModeMismatch", other, err)
				}
			}
			g, _ = uvsMustOpen(t, b, dir, graphpkg.UntimedDefault)
			if mode, _, err := g.Admin().UntimedValidStart(); err != nil || mode != graphpkg.UntimedAtemporal {
				t.Errorf("reopened under UntimedDefault: %d, %v; want UntimedAtemporal", mode, err)
			}
			if _, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			r11Want(t, "after the reopen, AT -1e6", r11KAt(t, g, n.ID(), "k", -1_000_000, 0), "2")
			if err := g.Admin().SetUntimedValidStart(ctx, graphpkg.UntimedAtemporal); !errors.Is(err, graphpkg.ErrInvalidUntimedValidStart) {
				t.Errorf("SetUntimedValidStart(UntimedAtemporal): %v, want ErrInvalidUntimedValidStart", err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}

			other := t.TempDir()
			g, _ = uvsMustOpen(t, b, other, graphpkg.UntimedUnbounded)
			if _, err := g.Nodes().Add(ctx, []string{"Ref"}, nil); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if g, _, err := uvsOpen(t, b, other, graphpkg.UntimedAtemporal); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				if g != nil {
					_ = g.Close()
				}
				t.Errorf("an UntimedUnbounded store with rows opened atemporal: %v, want ErrUntimedModeMismatch", err)
			}
		})
	}
}

// TestRound11AtemporalOtherModesUnchanged: under UntimedUnbounded an untimed
// update still starts at its TxFrom (the rule of the other modes is not
// changed by the flag).
func TestRound11AtemporalOtherModesUnchanged(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedUnbounded})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	u, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	tx, _, _ := u.TxStamps()
	if got := u.EffectiveValidFrom(); got != tx {
		t.Errorf("UntimedUnbounded update starts at %d, want its TxFrom %d", got, tx)
	}
	r11Want(t, "UntimedUnbounded AT -1e6 after an update", r11KAt(t, g, n.ID(), "k", -1_000_000, 0), "1")
	if _, err := g.Nodes().ByLabel("A", storepkg.QueryOpts{}); err != nil {
		t.Fatal(err)
	}
}

// TestRound11AtemporalDoors: the batch and ingest update doors and the
// GraphTx delete follow the mode as the plain doors do (one seam): an untimed
// batch or ingest update reaches every segment of a timed node; a GraphTx
// delete ends a node in transaction time only. A label change of a node with
// explicit valid-time segments is refused with ErrUntimedModeMismatch (the
// label index follows the current row; per-segment labels are an open item).
func TestRound11AtemporalDoors(t *testing.T) {
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(r11Atemporal) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			n, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1), types.ShadowValidFrom: types.Instant(100)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetNodeVersionInterval(ctx, n.ID(), 200, 0, map[string]any{"k": int64(2)}); err != nil {
				t.Fatal(err)
			}
			bb, err := g.Batch().New()
			if err != nil {
				t.Fatal(err)
			}
			if err := bb.UpdateNode(n.ID(), map[string]any{"j": int64(7)}); err != nil {
				t.Fatal(err)
			}
			if _, err := bb.Execute(); err != nil {
				t.Fatal(err)
			}
			r11Want(t, "batch: j AT 150", r11KAt(t, g, n.ID(), "j", 150, 0), "7")
			r11Want(t, "batch: j AT 250", r11KAt(t, g, n.ID(), "j", 250, 0), "7")
			r11Want(t, "batch: k AT 150", r11KAt(t, g, n.ID(), "k", 150, 0), "1")
			r11Want(t, "batch: AT 50", r11KAt(t, g, n.ID(), "j", 50, 0), "none")
			s, err := g.Ingest().NewSession(ingest.IngestOptions{Sync: true})
			if err != nil {
				t.Fatal(err)
			}
			if err := s.UpdateNode(n.ID(), map[string]any{"j": int64(8)}); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			r11Want(t, "ingest: j AT 150", r11KAt(t, g, n.ID(), "j", 150, 0), "8")
			r11Want(t, "ingest: j AT 250", r11KAt(t, g, n.ID(), "j", 250, 0), "8")
			if err := g.Nodes().AddLabel(ctx, n.ID(), "B"); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
				t.Errorf("label change of a node with valid-time segments: %v, want ErrUntimedModeMismatch", err)
			}
			m, err := g.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			before := r11NowAfter(t, g)
			if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error { return tx.DeleteNode(m.ID()) }); err != nil {
				t.Fatal(err)
			}
			r11Want(t, "GraphTx delete: AT -1e6 now", r11KAt(t, g, m.ID(), "k", -1_000_000, 0), "none")
			r11Want(t, "GraphTx delete: AT -1e6 AS OF before", r11KAt(t, g, m.ID(), "k", -1_000_000, before), "1")
		})
	}
}

// TestRound11AtemporalExportImport: an export of an UntimedAtemporal store
// carries the mode (header "ua"); an empty default store that imports it
// adopts it; a store of another mode with rows refuses it.
func TestRound11AtemporalExportImport(t *testing.T) {
	ctx := context.Background()
	src, err := graphpkg.New(graphpkg.Config{UntimedValidStart: graphpkg.UntimedAtemporal})
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	n, err := src.Nodes().Add(ctx, []string{"A"}, map[string]any{"k": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	var full bytes.Buffer
	if err := src.IO().Export(&full); err != nil {
		t.Fatal(err)
	}
	dst, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 2})
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	if err := dst.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatal(err)
	}
	if mode, _, err := dst.Admin().UntimedValidStart(); err != nil || mode != graphpkg.UntimedAtemporal {
		t.Errorf("after importing an atemporal export: %d, %v; want UntimedAtemporal", mode, err)
	}
	if _, err := dst.Nodes().Update(ctx, n.ID(), map[string]any{"k": int64(2)}); err != nil {
		t.Fatal(err)
	}
	r11Want(t, "imported store's update AT -1e6", r11KAt(t, dst, n.ID(), "k", -1_000_000, 0), "2")
	other, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 3, UntimedValidStart: graphpkg.UntimedUnbounded})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	if _, err := other.Nodes().Add(ctx, []string{"A"}, nil); err != nil {
		t.Fatal(err)
	}
	if err := other.IO().Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); !errors.Is(err, graphpkg.ErrUntimedModeMismatch) {
		t.Errorf("an UntimedUnbounded store with rows imports an atemporal export: %v, want ErrUntimedModeMismatch", err)
	}
}
