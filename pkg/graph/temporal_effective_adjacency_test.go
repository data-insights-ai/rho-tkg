package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerstore "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Two visibility classes for relationships at a valid instant t:
//
//   - EFFECTIVE (graph view: Snapshot, Diff, NeighborsAt, OutgoingRelsAt,
//     IncomingRelsAt): an edge is visible at t only if its own row is valid at
//     t AND both endpoints resolve as valid at t.
//   - DECLARED (row doors: RelsAt, RelsAtTx, RelsDuring, ByType{ValidAt},
//     ForEachAdjacentRelAt, ForEachAdjacentEndpointAt, ...): the
//     relationship's own asserted validity only, NOT masked by endpoints.
//
// Before the fix OutgoingRelsAt/IncomingRelsAt checked only the anchor, so an
// edge A->B with A closed at 2022 was returned by IncomingRelsAt(B, 2023) while
// Snapshot(2023) and NeighborsAt(B, 2023) hid it.

func effYear(y int) types.Instant {
	return types.Instant(time.Date(y, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli())
}

func effBackends() map[string]func(t *testing.T) *graphpkg.Graph {
	validation := graphpkg.ValidationLimits{AllowSelfLoops: true}
	return map[string]func(t *testing.T) *graphpkg.Graph{
		"memory": func(t *testing.T) *graphpkg.Graph {
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 4, Validation: validation})
			if err != nil {
				t.Fatalf("graph.New(memory): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		},
		"badger": func(t *testing.T) *graphpkg.Graph {
			bs, err := badgerstore.New(badgerstore.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 5, Store: bs, Validation: validation})
			if err != nil {
				t.Fatalf("graph.New(badger): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		},
		"tiered": func(t *testing.T) *graphpkg.Graph {
			ts, err := tieredpkg.New(tieredpkg.Config{
				InMemory:      true,
				RefLabels:     []string{"Case"},
				ShardWindow:   7 * 24 * time.Hour,
				FlushInterval: 1<<63 - 1,
			})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 13, Store: ts, Validation: validation})
			if err != nil {
				t.Fatalf("graph.New(tiered): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		},
	}
}

// effFixture is the adversarial dataset shared by every test in this file.
//
// Nodes (valid-time, after phase-2 mutations):
//
//	H [2020, ∞)   hub
//	B [2020, ∞)
//	A [2020, 2022)  closed via Nodes().CloseVersion (the probe)
//	C [2024, ∞)     valid_from LATER than every edge touching it
//	D [2020, ∞)
//	E [2020, del)   hard-deleted now (history-only, valid through 2025)
//	F [2024, ∞)     like C, but reached only through a DELETED edge
//
// Relationships (all tkg_valid_from 2020, type LINK):
//
//	rAB A->B   rHB H->B   rHC H->C   rHA H->A
//	rBH B->H   rAH A->H   rCH C->H
//	rHH H->H (self-loop, valid)   rAA A->A (self-loop, A closed)
//	rHD H->D   rel row itself closed at 2022
//	rHE H->E   cascade-deleted with E
//	rHF H->F   rel hard-deleted now (history-only, deleted-rel fold);
//	           its row is valid through 2025 but F only from 2024
type effFixture struct {
	g                               *graphpkg.Graph
	H, B, A, C, D, E, F             types.NodeID
	rAB, rHB, rHC, rHA, rBH, rAH    types.RelID
	rCH, rHH, rAA, rHD, rHE, rHF    types.RelID
	relName                         map[types.RelID]string
	nodeName                        map[types.NodeID]string
	allNodes                        []types.NodeID
	y2021, y2022, y2023, y2024, y25 types.Instant
}

func buildEffFixture(t *testing.T, g *graphpkg.Graph) *effFixture {
	t.Helper()
	ctx := context.Background()
	f := &effFixture{
		g:        g,
		relName:  map[types.RelID]string{},
		nodeName: map[types.NodeID]string{},
		y2021:    effYear(2021),
		y2022:    effYear(2022),
		y2023:    effYear(2023),
		y2024:    effYear(2024),
		y25:      effYear(2025),
	}
	addNode := func(name string, from types.Instant) types.NodeID {
		n, err := g.Nodes().Add(ctx, []string{"N"}, map[string]any{"tkg_valid_from": from, "name": name})
		if err != nil {
			t.Fatalf("add node %s: %v", name, err)
		}
		f.nodeName[n.ID()] = name
		f.allNodes = append(f.allNodes, n.ID())
		return n.ID()
	}
	y2020 := effYear(2020)
	f.H = addNode("H", y2020)
	f.B = addNode("B", y2020)
	f.A = addNode("A", y2020)
	f.C = addNode("C", f.y2024)
	f.D = addNode("D", y2020)
	f.E = addNode("E", y2020)
	f.F = addNode("F", f.y2024)

	addRel := func(name string, from, to types.NodeID) types.RelID {
		r, err := g.Rels().AddByID(ctx, "LINK", from, to, map[string]any{"tkg_valid_from": y2020, "name": name})
		if err != nil {
			t.Fatalf("add rel %s: %v", name, err)
		}
		f.relName[r.ID()] = name
		return r.ID()
	}
	f.rAB = addRel("rAB", f.A, f.B)
	f.rHB = addRel("rHB", f.H, f.B)
	f.rHC = addRel("rHC", f.H, f.C)
	f.rHA = addRel("rHA", f.H, f.A)
	f.rBH = addRel("rBH", f.B, f.H)
	f.rAH = addRel("rAH", f.A, f.H)
	f.rCH = addRel("rCH", f.C, f.H)
	f.rHH = addRel("rHH", f.H, f.H)
	f.rAA = addRel("rAA", f.A, f.A)
	f.rHD = addRel("rHD", f.H, f.D)
	f.rHE = addRel("rHE", f.H, f.E)
	f.rHF = addRel("rHF", f.H, f.F)
	return f
}

// mutate applies the phase-2 lifecycle changes (after t0 = 2021 was observed).
func (f *effFixture) mutate(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	g := f.g
	if err := g.Nodes().CloseVersion(ctx, f.A, f.y2022); err != nil {
		t.Fatalf("CloseVersion(A): %v", err)
	}
	if err := g.Rels().CloseVersion(ctx, f.rHD, f.y2022); err != nil {
		t.Fatalf("Rels.CloseVersion(rHD): %v", err)
	}
	if err := g.Nodes().Delete(ctx, f.E); err != nil {
		t.Fatalf("Delete(E): %v", err)
	}
	if err := g.Rels().Delete(ctx, f.rHF); err != nil {
		t.Fatalf("Rels.Delete(rHF): %v", err)
	}
}

func (f *effFixture) names(ids []types.RelID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if n, ok := f.relName[id]; ok {
			out = append(out, n)
		} else {
			out = append(out, fmt.Sprintf("rel#%d", id))
		}
	}
	sort.Strings(out)
	return out
}

func (f *effFixture) nodeNames(ids []types.NodeID) []string {
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if n, ok := f.nodeName[id]; ok {
			out = append(out, n)
		} else {
			out = append(out, fmt.Sprintf("node#%d", id))
		}
	}
	sort.Strings(out)
	return out
}

func effRelIDs(rels []*types.Relationship) []types.RelID {
	out := make([]types.RelID, 0, len(rels))
	for _, r := range rels {
		out = append(out, r.ID())
	}
	return out
}

func effNodeIDs(nodes []*types.Node) []types.NodeID {
	out := make([]types.NodeID, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID())
	}
	return out
}

// assertRels is an exact-set assertion (catches over-reporting AND omission).
func (f *effFixture) assertRels(t *testing.T, label string, got []types.RelID, want ...types.RelID) {
	t.Helper()
	g, w := f.names(got), f.names(want)
	if fmt.Sprint(g) != fmt.Sprint(w) {
		t.Fatalf("%s = %v, want exactly %v", label, g, w)
	}
	seen := map[types.RelID]bool{}
	for _, id := range got {
		if seen[id] {
			t.Fatalf("%s: duplicate rel %s in %v", label, f.relName[id], g)
		}
		seen[id] = true
	}
}

func (f *effFixture) outAt(t *testing.T, n types.NodeID, at types.Instant) []types.RelID {
	t.Helper()
	rels, err := f.g.Temporal().OutgoingRelsAt(n, at)
	if err != nil {
		t.Fatalf("OutgoingRelsAt(%s, %d): %v", f.nodeName[n], at, err)
	}
	for _, r := range rels {
		if r.StartNodeID() != n {
			t.Fatalf("OutgoingRelsAt(%s): rel %s start=%d", f.nodeName[n], f.relName[r.ID()], r.StartNodeID())
		}
	}
	return effRelIDs(rels)
}

func (f *effFixture) inAt(t *testing.T, n types.NodeID, at types.Instant) []types.RelID {
	t.Helper()
	rels, err := f.g.Temporal().IncomingRelsAt(n, at)
	if err != nil {
		t.Fatalf("IncomingRelsAt(%s, %d): %v", f.nodeName[n], at, err)
	}
	for _, r := range rels {
		if r.EndNodeID() != n {
			t.Fatalf("IncomingRelsAt(%s): rel %s end=%d", f.nodeName[n], f.relName[r.ID()], r.EndNodeID())
		}
	}
	return effRelIDs(rels)
}

// TestEffectiveAdjacency_OtherEndpointMasked is the probe regression plus the
// mixed fan-out: OutgoingRelsAt/IncomingRelsAt mask an edge whose OTHER
// endpoint is not valid at t (closed, not-yet-valid, reached via a deleted
// edge), while an edge to a hard-deleted endpoint stays visible inside its
// window,
// while still returning it at an instant where both endpoints are valid
// (two-phase: 2021 is observed before and after the mutations).
func TestEffectiveAdjacency_OtherEndpointMasked(t *testing.T) {
	t.Parallel()
	for name, open := range effBackends() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := buildEffFixture(t, open(t))

			// Phase 1 — before any close/delete.
			f.assertRels(t, "phase1 In(B)@2023", f.inAt(t, f.B, f.y2023), f.rAB, f.rHB)
			f.assertRels(t, "phase1 Out(H)@2023", f.outAt(t, f.H, f.y2023),
				f.rHB, f.rHA, f.rHH, f.rHD, f.rHE) // rHC/rHF masked: C, F valid from 2024
			f.assertRels(t, "phase1 In(H)@2021", f.inAt(t, f.H, f.y2021),
				f.rBH, f.rAH, f.rHH) // rCH masked: C valid from 2024

			f.mutate(t)

			// Phase 2 at t0 = 2021: pre-mutation state must be remembered.
			f.assertRels(t, "In(B)@2021", f.inAt(t, f.B, f.y2021), f.rAB, f.rHB)
			f.assertRels(t, "Out(A)@2021", f.outAt(t, f.A, f.y2021), f.rAB, f.rAH, f.rAA)
			f.assertRels(t, "In(A)@2021", f.inAt(t, f.A, f.y2021), f.rHA, f.rAA)
			f.assertRels(t, "Out(H)@2021", f.outAt(t, f.H, f.y2021),
				f.rHB, f.rHA, f.rHH, f.rHD, f.rHE) // rHF masked: F valid from 2024
			f.assertRels(t, "In(H)@2021", f.inAt(t, f.H, f.y2021), f.rBH, f.rAH, f.rHH)

			// Phase 2 at 2023: A closed, rHD's row closed, C and F not yet valid.
			// The probe: A->B must NOT be visible from B's side.
			f.assertRels(t, "In(B)@2023", f.inAt(t, f.B, f.y2023), f.rHB)
			f.assertRels(t, "Out(B)@2023", f.outAt(t, f.B, f.y2023), f.rBH)
			f.assertRels(t, "Out(H)@2023", f.outAt(t, f.H, f.y2023), f.rHB, f.rHH, f.rHE)
			f.assertRels(t, "In(H)@2023", f.inAt(t, f.H, f.y2023), f.rBH, f.rHH)
			f.assertRels(t, "In(D)@2023", f.inAt(t, f.D, f.y2023)) // rel row closed
			f.assertRels(t, "In(E)@2023", f.inAt(t, f.E, f.y2023), f.rHE)

			// The anchor error is unchanged: A, C, F are not valid at 2023.
			for _, dir := range []struct {
				name string
				call func(types.NodeID, types.Instant) ([]*types.Relationship, error)
			}{
				{"OutgoingRelsAt", f.g.Temporal().OutgoingRelsAt},
				{"IncomingRelsAt", f.g.Temporal().IncomingRelsAt},
			} {
				for _, anchor := range []types.NodeID{f.A, f.F, f.C} {
					got, err := dir.call(anchor, f.y2023)
					if !errors.Is(err, storepkg.ErrNoVersionValidAt) {
						t.Fatalf("%s(%s)@2023 = (%v, %v), want ErrNoVersionValidAt",
							dir.name, f.nodeName[anchor], f.names(effRelIDs(got)), err)
					}
				}
			}

			// Phase 2 at 2025: C and F became valid, so their edges surface —
			// rHF only through the deleted-rel fold.
			f.assertRels(t, "Out(H)@2025", f.outAt(t, f.H, f.y25), f.rHB, f.rHC, f.rHH, f.rHE, f.rHF)
			f.assertRels(t, "In(F)@2025", f.inAt(t, f.F, f.y25), f.rHF)
			f.assertRels(t, "In(H)@2025", f.inAt(t, f.H, f.y25), f.rBH, f.rCH, f.rHH)
			f.assertRels(t, "In(C)@2025", f.inAt(t, f.C, f.y25), f.rHC)
			f.assertRels(t, "Out(C)@2025", f.outAt(t, f.C, f.y25), f.rCH)
		})
	}
}

// TestEffectiveAdjacency_SelfLoop pins the self-loop arm: the other endpoint
// IS the anchor, so the edge is visible exactly when the anchor is valid, and
// is listed once in each direction.
func TestEffectiveAdjacency_SelfLoop(t *testing.T) {
	t.Parallel()
	for name, open := range effBackends() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := buildEffFixture(t, open(t))
			f.mutate(t)

			f.assertRels(t, "Out(A)@2021 self-loop", f.outAt(t, f.A, f.y2021), f.rAB, f.rAH, f.rAA)
			f.assertRels(t, "In(A)@2021 self-loop", f.inAt(t, f.A, f.y2021), f.rHA, f.rAA)
			f.assertRels(t, "Out(H)@2023 self-loop", f.outAt(t, f.H, f.y2023), f.rHB, f.rHH, f.rHE)
			f.assertRels(t, "In(H)@2023 self-loop", f.inAt(t, f.H, f.y2023), f.rBH, f.rHH)

			// Invalid self-loop at 2023: A is closed, so the anchor check fails.
			if _, err := f.g.Temporal().OutgoingRelsAt(f.A, f.y2023); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
				t.Fatalf("OutgoingRelsAt(A)@2023 err = %v, want ErrNoVersionValidAt", err)
			}
			// Snapshot agrees: rAA present at 2021, absent at 2023; rHH present at both.
			for _, tc := range []struct {
				at   types.Instant
				want map[types.RelID]bool
			}{
				{f.y2021, map[types.RelID]bool{f.rAA: true, f.rHH: true}},
				{f.y2023, map[types.RelID]bool{f.rAA: false, f.rHH: true}},
			} {
				snap, err := f.g.Temporal().Snapshot(tc.at)
				if err != nil {
					t.Fatalf("Snapshot: %v", err)
				}
				in := map[types.RelID]bool{}
				for _, r := range snap.Relationships {
					in[r.ID()] = true
				}
				for id, want := range tc.want {
					if in[id] != want {
						t.Fatalf("Snapshot(%d) contains %s = %v, want %v", tc.at, f.relName[id], in[id], want)
					}
				}
			}
		})
	}
}

// TestDeclaredRelDoors_NotMaskedByEndpoints pins the DECLARED class: the row
// doors report the relationship's own asserted validity, unmasked by endpoint
// validity. A bulk consumer relies on this; the classification is deliberate.
func TestDeclaredRelDoors_NotMaskedByEndpoints(t *testing.T) {
	t.Parallel()
	for name, open := range effBackends() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := buildEffFixture(t, open(t))
			f.mutate(t)
			g := f.g

			// Every rel whose own row is valid at 2023 (all but rHD), including
			// rAB/rHA/rAH/rAA (A closed), rHC/rCH (C not yet valid), rHF (deleted
			// rel, F not yet valid).
			declared2023 := []types.RelID{f.rAB, f.rHB, f.rHC, f.rHA, f.rBH, f.rAH, f.rCH, f.rHH, f.rAA, f.rHE, f.rHF}

			rs, err := g.Temporal().RelsAt(f.y2023)
			if err != nil {
				t.Fatalf("RelsAt: %v", err)
			}
			f.assertRels(t, "RelsAt(2023)", effRelIDs(rs), declared2023...)

			rs, err = g.Temporal().RelsAtTx(f.y2023, 0)
			if err != nil {
				t.Fatalf("RelsAtTx: %v", err)
			}
			f.assertRels(t, "RelsAtTx(2023,0)", effRelIDs(rs), declared2023...)

			rs, err = g.Temporal().RelsDuring(f.y2023, f.y2024)
			if err != nil {
				t.Fatalf("RelsDuring: %v", err)
			}
			f.assertRels(t, "RelsDuring(2023,2024)", effRelIDs(rs), declared2023...)

			rs, err = g.Rels().ByType("LINK", storepkg.QueryOpts{ValidAt: f.y2023})
			if err != nil {
				t.Fatalf("ByType: %v", err)
			}
			f.assertRels(t, "ByType{ValidAt:2023}", effRelIDs(rs), declared2023...)

			var adj []types.RelID
			if err := g.Rels().ForEachAdjacentRelAt(f.H, "", false, storepkg.QueryOpts{ValidAt: f.y2023}, func(r *types.Relationship) bool {
				adj = append(adj, r.ID())
				return true
			}); err != nil {
				t.Fatalf("ForEachAdjacentRelAt: %v", err)
			}
			f.assertRels(t, "ForEachAdjacentRelAt(H,out,2023)", adj, f.rHB, f.rHC, f.rHA, f.rHH, f.rHE, f.rHF)

			adj = nil
			if err := g.Rels().ForEachAdjacentEndpointAt(f.H, "", true, storepkg.QueryOpts{ValidAt: f.y2023}, func(rel types.RelID, _ types.NodeID) bool {
				adj = append(adj, rel)
				return true
			}); err != nil {
				t.Fatalf("ForEachAdjacentEndpointAt: %v", err)
			}
			f.assertRels(t, "ForEachAdjacentEndpointAt(H,in,2023)", adj, f.rBH, f.rAH, f.rCH, f.rHH)

			// And the probe: RelsAt(2023) still returns A->B while the
			// effective view does not.
			eff, err := g.Temporal().IncomingRelsAt(f.B, f.y2023)
			if err != nil {
				t.Fatalf("IncomingRelsAt(B): %v", err)
			}
			for _, r := range eff {
				if r.ID() == f.rAB {
					t.Fatalf("IncomingRelsAt(B)@2023 contains A->B although A is closed at 2022")
				}
			}
		})
	}
}

// TestEffectiveDoorsAgree is the cross-door agreement check: Snapshot, Diff,
// NeighborsAt, OutgoingRelsAt, IncomingRelsAt (standalone and inside a tx) all
// answer the same effective graph at every probed instant.
func TestEffectiveDoorsAgree(t *testing.T) {
	t.Parallel()
	for name, open := range effBackends() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f := buildEffFixture(t, open(t))
			f.mutate(t)
			g := f.g

			wantSnap := map[types.Instant][]types.RelID{
				f.y2021: {f.rAB, f.rHB, f.rHA, f.rBH, f.rAH, f.rHH, f.rAA, f.rHD, f.rHE},
				f.y2023: {f.rHB, f.rBH, f.rHH, f.rHE},
				f.y25:   {f.rHB, f.rHC, f.rBH, f.rCH, f.rHH, f.rHE, f.rHF},
			}
			snapRels := map[types.Instant]map[types.RelID]bool{}

			for _, at := range []types.Instant{f.y2021, f.y2023, f.y25} {
				snap, err := g.Temporal().Snapshot(at)
				if err != nil {
					t.Fatalf("Snapshot(%d): %v", at, err)
				}
				label := fmt.Sprintf("@%d", time.UnixMilli(int64(at)).UTC().Year())
				f.assertRels(t, "Snapshot"+label, effRelIDs(snap.Relationships), wantSnap[at]...)

				set := map[types.RelID]bool{}
				for _, r := range snap.Relationships {
					set[r.ID()] = true
				}
				snapRels[at] = set

				valid := map[types.NodeID]bool{}
				for _, n := range snap.Nodes {
					valid[n.ID()] = true
				}

				var outUnion, inUnion []types.RelID
				for _, id := range f.allNodes {
					if !valid[id] {
						if _, err := g.Temporal().OutgoingRelsAt(id, at); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
							t.Fatalf("OutgoingRelsAt(%s)%s err = %v, want ErrNoVersionValidAt (not in snapshot)", f.nodeName[id], label, err)
						}
						if _, err := g.Temporal().NeighborsAt(id, at); !errors.Is(err, storepkg.ErrNoVersionValidAt) {
							t.Fatalf("NeighborsAt(%s)%s err = %v, want ErrNoVersionValidAt (not in snapshot)", f.nodeName[id], label, err)
						}
						continue
					}
					out := f.outAt(t, id, at)
					in := f.inAt(t, id, at)
					outUnion = append(outUnion, out...)
					inUnion = append(inUnion, in...)

					// NeighborsAt == other endpoints of Out ∪ In.
					wantNb := map[types.NodeID]bool{}
					for _, rid := range out {
						wantNb[relEndpoint(t, g, rid, at, false)] = true
					}
					for _, rid := range in {
						wantNb[relEndpoint(t, g, rid, at, true)] = true
					}
					var wantNbIDs []types.NodeID
					for nid := range wantNb {
						wantNbIDs = append(wantNbIDs, nid)
					}
					nb, err := g.Temporal().NeighborsAt(id, at)
					if err != nil {
						t.Fatalf("NeighborsAt(%s)%s: %v", f.nodeName[id], label, err)
					}
					if a, b := f.nodeNames(effNodeIDs(nb)), f.nodeNames(wantNbIDs); fmt.Sprint(a) != fmt.Sprint(b) {
						t.Fatalf("NeighborsAt(%s)%s = %v, Out∪In endpoints = %v", f.nodeName[id], label, a, b)
					}
				}
				f.assertRels(t, "∪OutgoingRelsAt"+label, outUnion, wantSnap[at]...)
				f.assertRels(t, "∪IncomingRelsAt"+label, inUnion, wantSnap[at]...)

				// Tx mirror answers the same effective view. Standalone reads
				// happen outside the tx (the tx holds the graph write lock).
				wantOut, wantIn := f.outAt(t, f.H, at), f.inAt(t, f.H, at)
				var txOut, txIn []*types.Relationship
				if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
					var err error
					if txOut, err = tx.OutgoingRelsAt(f.H, at); err != nil {
						return err
					}
					txIn, err = tx.IncomingRelsAt(f.H, at)
					return err
				}); err != nil {
					t.Fatalf("tx: %v", err)
				}
				f.assertRels(t, "tx.OutgoingRelsAt(H)"+label, effRelIDs(txOut), wantOut...)
				f.assertRels(t, "tx.IncomingRelsAt(H)"+label, effRelIDs(txIn), wantIn...)
			}

			// Diff classifies rel presence with the same effective rule.
			for _, pair := range [][2]types.Instant{{f.y2021, f.y2023}, {f.y2023, f.y25}} {
				d, err := g.Temporal().Diff(pair[0], pair[1])
				if err != nil {
					t.Fatalf("Diff: %v", err)
				}
				var wantCreated, wantDeleted []types.RelID
				for id := range snapRels[pair[1]] {
					if !snapRels[pair[0]][id] {
						wantCreated = append(wantCreated, id)
					}
				}
				for id := range snapRels[pair[0]] {
					if !snapRels[pair[1]][id] {
						wantDeleted = append(wantDeleted, id)
					}
				}
				label := fmt.Sprintf("Diff(%d,%d)", time.UnixMilli(int64(pair[0])).UTC().Year(), time.UnixMilli(int64(pair[1])).UTC().Year())
				f.assertRels(t, label+".RelsCreated", effRelIDs(d.RelsCreated), wantCreated...)
				f.assertRels(t, label+".RelsDeleted", effRelIDs(d.RelsDeleted), wantDeleted...)
			}
		})
	}
}

func relEndpoint(t *testing.T, g *graphpkg.Graph, id types.RelID, at types.Instant, start bool) types.NodeID {
	t.Helper()
	r, err := g.Temporal().RelAt(id, at)
	if err != nil {
		t.Fatalf("RelAt(%d): %v", id, err)
	}
	if start {
		return r.StartNodeID()
	}
	return r.EndNodeID()
}
