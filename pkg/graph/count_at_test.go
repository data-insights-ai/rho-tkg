package graph_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// countAtCoordinates lists the read coordinates a count is checked at: points
// and windows of valid time around every instant the history touched, every
// transaction-time pin taken, TxAt pins combined with valid time, and pages.
func countAtCoordinates(valid, pins []types.Instant) []graphpkg.QueryOpts {
	var out []graphpkg.QueryOpts
	for i, v := range valid {
		out = append(out, graphpkg.QueryOpts{ValidAt: v})
		if i+1 < len(valid) && valid[i+1] > v {
			out = append(out, graphpkg.QueryOpts{ValidStart: v, ValidEnd: valid[i+1]})
		}
	}
	for i, p := range pins {
		out = append(out, graphpkg.QueryOpts{TxPin: p}, graphpkg.QueryOpts{TxAt: p})
		if i < len(valid) {
			out = append(out, graphpkg.QueryOpts{TxAt: p, ValidAt: valid[i]})
		}
	}
	if len(valid) > 2 {
		out = append(out,
			graphpkg.QueryOpts{ValidAt: valid[len(valid)/2], Limit: 2},
			graphpkg.QueryOpts{ValidStart: valid[0], ValidEnd: valid[len(valid)-1], Limit: 3})
	}
	return out
}

// checkCountsAt asserts CountByLabelAt / CountByTypeAt equal the length of
// ByLabel / ByType at every coordinate, and that a page's count equals the
// page's length (After set to the first member of the unpaged answer).
func checkCountsAt(t *testing.T, g *graphpkg.Graph, labels, relTypes []string, coords []graphpkg.QueryOpts) {
	t.Helper()
	for _, opts := range coords {
		for _, label := range labels {
			want, err := g.Nodes().ByLabel(label, opts)
			if err != nil {
				t.Fatalf("ByLabel(%s, %+v): %v", label, opts, err)
			}
			got, err := g.Nodes().CountByLabelAt(label, opts)
			if err != nil {
				t.Fatalf("CountByLabelAt(%s, %+v): %v", label, opts, err)
			}
			if got != len(want) {
				t.Fatalf("CountByLabelAt(%s, %+v) = %d, ByLabel has %d", label, opts, got, len(want))
			}
			if len(want) > 0 && opts.Limit == 0 {
				paged := opts
				paged.After = types.EntityID(want[0].ID())
				page, err := g.Nodes().ByLabel(label, paged)
				if err != nil {
					t.Fatal(err)
				}
				if got, err := g.Nodes().CountByLabelAt(label, paged); err != nil || got != len(page) {
					t.Fatalf("CountByLabelAt(%s, %+v) = %d, %v; ByLabel has %d", label, paged, got, err, len(page))
				}
			}
		}
		for _, typ := range relTypes {
			want, err := g.Rels().ByType(typ, opts)
			if err != nil {
				t.Fatalf("ByType(%s, %+v): %v", typ, opts, err)
			}
			got, err := g.Rels().CountByTypeAt(typ, opts)
			if err != nil {
				t.Fatalf("CountByTypeAt(%s, %+v): %v", typ, opts, err)
			}
			if got != len(want) {
				t.Fatalf("CountByTypeAt(%s, %+v) = %d, ByType has %d", typ, opts, got, len(want))
			}
		}
	}
}

// CountByLabelAt and CountByTypeAt equal len(ByLabel) and len(ByType) at
// every coordinate over random histories on every backend: creates with
// explicit valid times, updates, closed versions, label removals and
// additions, deletes of nodes and relationships, checked at valid-time
// points and windows, transaction-time pins (TxPin, TxAt alone and with a
// valid time) and pages.
func TestCountAtAgreesWithTheReads(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		rng := rand.New(rand.NewPCG(7, uint64(len(b.name))))
		base := types.InstantFromTime(time.Now().Add(-time.Hour))
		valid := []types.Instant{base - 1}
		var pins []types.Instant
		pin := func() {
			p, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			pins = append(pins, p)
		}
		var people, others []types.NodeID
		for i := range 24 {
			from := base + types.Instant(i*1000)
			props := map[string]any{"i": int64(i), types.ShadowValidFrom: from}
			if i%5 == 0 {
				props[types.ShadowValidTo] = from + 2500
				valid = append(valid, from+2500)
			}
			label := "Person"
			if i%3 == 0 {
				label = "Other"
			}
			n, err := g.Nodes().Add(ctx, []string{label}, props)
			if err != nil {
				t.Fatal(err)
			}
			valid = append(valid, from, from+500)
			if label == "Person" {
				people = append(people, n.ID())
			} else {
				others = append(others, n.ID())
			}
		}
		pin()
		var rels []types.RelID
		for i := range 20 {
			s := people[rng.IntN(len(people))]
			e := others[rng.IntN(len(others))]
			props := map[string]any{"w": int64(i)}
			if i%4 == 0 {
				props[types.ShadowValidFrom] = base + types.Instant(i*700)
				valid = append(valid, base+types.Instant(i*700))
			}
			r, err := g.Rels().AddByID(ctx, "KNOWS", s, e, props)
			if err != nil {
				t.Fatal(err)
			}
			rels = append(rels, r.ID())
		}
		pin()
		alive := map[types.NodeID]bool{}
		for _, id := range append(append([]types.NodeID{}, people...), others...) {
			alive[id] = true
		}
		for step := range 40 {
			switch rng.IntN(6) {
			case 0, 1:
				id := people[rng.IntN(len(people))]
				if alive[id] {
					if _, err := g.Nodes().Update(ctx, id, map[string]any{"i": int64(-step)}); err != nil && !errors.Is(err, graphpkg.ErrAlreadyClosed) {
						t.Fatal(err)
					}
				}
			case 2:
				id := people[rng.IntN(len(people))]
				if alive[id] {
					// Adding first keeps a label when Person is the only one.
					_ = g.Nodes().AddLabel(ctx, id, "Other")
					_ = g.Nodes().RemoveLabel(ctx, id, "Person")
				}
			case 3:
				id := others[rng.IntN(len(others))]
				if alive[id] {
					_ = g.Nodes().AddLabel(ctx, id, "Person")
				}
			case 4:
				if len(rels) > 0 {
					k := rng.IntN(len(rels))
					if err := g.Rels().Delete(ctx, rels[k]); err != nil && !errors.Is(err, graphpkg.ErrRelNotFound) {
						t.Fatal(err)
					}
					rels = append(rels[:k], rels[k+1:]...)
				}
			case 5:
				id := people[rng.IntN(len(people))]
				if alive[id] && step%2 == 0 {
					if err := g.Nodes().Delete(ctx, id); err == nil {
						alive[id] = false
					}
				} else if len(rels) > 0 {
					k := rng.IntN(len(rels))
					if _, err := g.Rels().Update(ctx, rels[k], map[string]any{"w": int64(step)}); err != nil && !errors.Is(err, graphpkg.ErrRelNotFound) && !errors.Is(err, graphpkg.ErrAlreadyClosed) {
						t.Fatal(err)
					}
				}
			}
			if step%8 == 7 {
				pin()
			}
		}
		now := types.InstantFromTime(time.Now())
		valid = append(valid, now, now+3_600_000)
		checkCountsAt(t, g, []string{"Person", "Other"}, []string{"KNOWS"}, countAtCoordinates(valid, pins))
	})
}

// Two-phase: a count at a coordinate before a mutation keeps the old answer
// after it. Five people valid from base, one closed at base+5000; then two
// are deleted, one loses the label and three are added. At the pin taken
// before the mutations, and at the valid instants inside the history, the
// counts are the old ones; now they are the new ones; a phantom label counts
// 0 at every coordinate.
func TestCountAtRemembersThePast(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		base := types.InstantFromTime(time.Now().Add(-time.Hour))
		var ids []types.NodeID
		for i := range 5 {
			n, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(i), types.ShadowValidFrom: base})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, n.ID())
		}
		if err := g.Nodes().CloseVersion(ctx, ids[4], base+5000); err != nil {
			t.Fatal(err)
		}
		before, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		for _, id := range ids[:2] {
			if err := g.Nodes().Delete(ctx, id); err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Nodes().AddLabel(ctx, ids[2], "Other"); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().RemoveLabel(ctx, ids[2], "Person"); err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			if _, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(10 + i)}); err != nil {
				t.Fatal(err)
			}
		}
		for _, tc := range []struct {
			opts graphpkg.QueryOpts
			want int
		}{
			{graphpkg.QueryOpts{TxPin: before}, 5},
			{graphpkg.QueryOpts{TxAt: before, ValidAt: base + 1000}, 5},
			{graphpkg.QueryOpts{TxAt: before, ValidAt: base + 6000}, 4},
			{graphpkg.QueryOpts{ValidStart: base, ValidEnd: base + 1000, TxAt: before}, 5},
		} {
			got, err := g.Nodes().CountByLabelAt("Person", tc.opts)
			if err != nil || got != tc.want {
				t.Fatalf("CountByLabelAt(%+v) = %d, %v; want %d", tc.opts, got, err, tc.want)
			}
			if got, err := g.Nodes().CountByLabelAt("Phantom", tc.opts); err != nil || got != 0 {
				t.Fatalf("CountByLabelAt(Phantom, %+v) = %d, %v", tc.opts, got, err)
			}
		}
		now, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		// Now: ids[3] and the three new ones; ids[4] closed in valid time.
		if got, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{TxPin: now}); err != nil || got != 5 {
			t.Fatalf("CountByLabelAt(TxPin now) = %d, %v; want 5", got, err)
		}
		if got, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{}); err != nil || got != 5 {
			t.Fatalf("CountByLabelAt({}) = %d, %v; want 5 (the counter)", got, err)
		}
	})
}

// The as-of column set a DocValuesSnapshotAsOf builds serves the count at its
// pin, and the snapshot's Len is that count (the label's count at a pin without
// a pass once the set is cached).
func TestCountAtPinMatchesTheAsOfColumnSet(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		var ids []types.NodeID
		for i := range 6 {
			n, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"i": int64(i)})
			if err != nil {
				t.Fatal(err)
			}
			ids = append(ids, n.ID())
		}
		pin, err := g.Temporal().NowTx()
		if err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().Delete(ctx, ids[0]); err != nil {
			t.Fatal(err)
		}
		snap, _, ok, err := g.Nodes().DocValuesSnapshotAsOf("Person", nil, pin)
		if err != nil || !ok {
			t.Fatalf("DocValuesSnapshotAsOf: %v %v", ok, err)
		}
		rr, isRows := snap.(types.NodeColumnRowReader)
		if !isRows {
			t.Fatalf("as-of snapshot %T reads no positions", snap)
		}
		got, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{TxPin: pin})
		if err != nil || got != 6 || rr.Len() != 6 {
			t.Fatalf("count at pin = %d, %v; Len = %d; want 6", got, err, rr.Len())
		}
		if got, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{}); err != nil || got != 5 {
			t.Fatalf("count now = %d, %v; want 5", got, err)
		}
	})
}

// The count doors validate like the reads they count.
func TestCountAtValidates(t *testing.T) {
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().CountByLabelAt("", graphpkg.QueryOpts{}); err == nil {
		t.Fatal("empty label accepted")
	}
	if _, err := g.Rels().CountByTypeAt(" ", graphpkg.QueryOpts{}); err == nil {
		t.Fatal("blank type accepted")
	}
	conflict := graphpkg.QueryOpts{TxPin: 5, ValidAt: 5}
	if _, err := g.Nodes().CountByLabelAt("Person", conflict); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
		t.Fatalf("CountByLabelAt(conflict) = %v", err)
	}
	if _, err := g.Rels().CountByTypeAt("KNOWS", conflict); !errors.Is(err, graphpkg.ErrConflictingTemporalOpts) {
		t.Fatalf("CountByTypeAt(conflict) = %v", err)
	}
	if _, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{ValidStart: 9, ValidEnd: 3}); !errors.Is(err, graphpkg.ErrInvalidTimeRange) {
		t.Fatalf("CountByLabelAt(inverted window) = %v", err)
	}
	if n, err := g.Rels().CountByTypeAt("Unknown", graphpkg.QueryOpts{ValidAt: 3}); err != nil || n != 0 {
		t.Fatalf("CountByTypeAt(unknown) = %d, %v", n, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().CountByLabelAt("Person", graphpkg.QueryOpts{}); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed CountByLabelAt = %v", err)
	}
	if _, err := g.Rels().CountByTypeAt("KNOWS", graphpkg.QueryOpts{}); !errors.Is(err, graphpkg.ErrGraphClosed) {
		t.Fatalf("closed CountByTypeAt = %v", err)
	}
}

var _ = fmt.Sprintf
