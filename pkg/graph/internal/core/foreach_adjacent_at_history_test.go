package core

// ForEachAdjacentRelAt / ForEachAdjacentEndpointAt under a valid-time filter
// must resolve the VERSION valid at the query time — like every node door
// (nodesByLabelLocked → findNodeVersionForOpts) and like
// Temporal().OutgoingRelsAt — not filter the current row. Before v4.35.0 both
// doors (native badger inline-stamp arm AND the memory decode fallback) tested
// the live row only, so a relationship whose latest version carries a later
// tkg_valid_from vanished from every adjacency query at an earlier instant
// while the store still held the older version. The consumer symptom was a
// Cypher `MATCH ()-[r]->() AT TIME t` returning nothing after
// `SET r.w = 2 VALID FROM 2000`.
//
// Break-first: every probe's pass condition is "did not drop", "did not
// duplicate", "did not return the wrong version", or "errored".

import (
	"context"
	"errors"
	"sort"
	"testing"

	grapherr "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/grapherr"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type adjBackend struct {
	name  string
	build func(t *testing.T) *Core
}

func adjBackends() []adjBackend {
	return []adjBackend{
		{name: "memory", build: func(t *testing.T) *Core {
			g, err := New(Config{Store: memory.New()})
			if err != nil {
				t.Fatalf("New(memory): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
		{name: "badger", build: func(t *testing.T) *Core {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			g, err := New(Config{Store: bs})
			if err != nil {
				t.Fatalf("New(badger): %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })
			return g
		}},
	}
}

// adjRelsAt collects the rows ForEachAdjacentRelAt yields, keyed by rel id
// with the version's "w" property, so duplicates and wrong versions both fail.
func adjRelsAt(t *testing.T, g *Core, node types.NodeID, typ string, incoming bool, opts storepkg.QueryOpts) map[types.RelID]any {
	t.Helper()
	got := map[types.RelID]any{}
	err := g.Rels.ForEachAdjacentRelAt(node, typ, incoming, opts, func(r *types.Relationship) bool {
		if _, dup := got[r.ID()]; dup {
			t.Errorf("rel %d yielded twice under %+v", r.ID(), opts)
		}
		got[r.ID()] = r.PropertiesMap()["w"]
		return true
	})
	if err != nil {
		t.Fatalf("ForEachAdjacentRelAt %+v: %v", opts, err)
	}
	return got
}

func adjEndpointsAt(t *testing.T, g *Core, node types.NodeID, typ string, incoming bool, opts storepkg.QueryOpts) map[types.RelID]types.NodeID {
	t.Helper()
	got := map[types.RelID]types.NodeID{}
	err := g.Rels.ForEachAdjacentEndpointAt(node, typ, incoming, opts, func(rel types.RelID, other types.NodeID) bool {
		if _, dup := got[rel]; dup {
			t.Errorf("rel %d yielded twice under %+v", rel, opts)
		}
		got[rel] = other
		return true
	})
	if err != nil {
		t.Fatalf("ForEachAdjacentEndpointAt %+v: %v", opts, err)
	}
	return got
}

// An updated relationship (later explicit valid_from) must resolve to the
// OLD version inside the old window, the NEW one inside the new window, and
// nothing before either — through both doors, both directions, both backends.
func TestForEachAdjacentAt_Break_UpdatedRelHistoryNotDropped(t *testing.T) {
	t.Parallel()
	for _, be := range adjBackends() {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)
			hub, _ := g.Nodes.Add(ctx, []string{"Hub"}, nil)
			tgt, _ := g.Nodes.Add(ctx, []string{"T"}, nil)
			r, err := g.Rels.Add(ctx, "LINK", hub, tgt, map[string]any{"w": int64(1), "tkg_valid_from": int64(1000)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(2), "tkg_valid_from": int64(2000)}); err != nil {
				t.Fatal(err)
			}

			cases := []struct {
				at   int64
				want any // nil = must be absent
			}{
				{500, nil}, {999, nil}, {1000, int64(1)}, {1500, int64(1)}, {1999, int64(1)},
				{2000, int64(2)}, {2500, int64(2)}, {1 << 50, int64(2)},
			}
			for _, tc := range cases {
				opts := storepkg.QueryOpts{ValidAt: types.Instant(tc.at)}
				for _, dir := range []struct {
					node     types.NodeID
					incoming bool
				}{{hub.ID(), false}, {tgt.ID(), true}} {
					got := adjRelsAt(t, g, dir.node, "LINK", dir.incoming, opts)
					if tc.want == nil {
						if len(got) != 0 {
							t.Errorf("at=%d incoming=%v: got %v, want nothing", tc.at, dir.incoming, got)
						}
					} else if len(got) != 1 || got[r.ID()] != tc.want {
						t.Errorf("at=%d incoming=%v: got %v, want {%d: w=%v}", tc.at, dir.incoming, got, r.ID(), tc.want)
					}
					eps := adjEndpointsAt(t, g, dir.node, "LINK", dir.incoming, opts)
					if (tc.want == nil) != (len(eps) == 0) {
						t.Errorf("at=%d incoming=%v: endpoint door disagrees with rel door: %v", tc.at, dir.incoming, eps)
					}
					if tc.want != nil {
						other := tgt.ID()
						if dir.incoming {
							other = hub.ID()
						}
						if eps[r.ID()] != other {
							t.Errorf("at=%d incoming=%v: endpoint = %d, want %d", tc.at, dir.incoming, eps[r.ID()], other)
						}
					}
				}
			}
		})
	}
}

// Interval filter: exactly ONE row per relationship (the most recent
// overlapping version, as ByType / nodesByLabel resolve it), never two.
func TestForEachAdjacentAt_Break_IntervalYieldsOneVersionPerRel(t *testing.T) {
	t.Parallel()
	for _, be := range adjBackends() {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)
			hub, _ := g.Nodes.Add(ctx, []string{"Hub"}, nil)
			tgt, _ := g.Nodes.Add(ctx, []string{"T"}, nil)
			r, _ := g.Rels.Add(ctx, "LINK", hub, tgt, map[string]any{"w": int64(1), "tkg_valid_from": int64(1000)})
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(2), "tkg_valid_from": int64(2000)}); err != nil {
				t.Fatal(err)
			}
			cases := []struct {
				from, to int64
				want     any
			}{
				{100, 900, nil},          // before both
				{1200, 1800, int64(1)},   // inside old only
				{2200, 2800, int64(2)},   // inside new only
				{1500, 2500, int64(2)},   // spans both → one row, most recent
				{100, 1 << 50, int64(2)}, // spans everything → one row
				{900, 1000, nil},         // [900,1000) touches nothing (half-open)
				{1999, 2000, int64(1)},   // last ms of the old window
			}
			for _, tc := range cases {
				opts := storepkg.QueryOpts{ValidStart: types.Instant(tc.from), ValidEnd: types.Instant(tc.to)}
				got := adjRelsAt(t, g, hub.ID(), "LINK", false, opts)
				if tc.want == nil {
					if len(got) != 0 {
						t.Errorf("[%d,%d): got %v, want nothing", tc.from, tc.to, got)
					}
					continue
				}
				if len(got) != 1 || got[r.ID()] != tc.want {
					t.Errorf("[%d,%d): got %v, want exactly {%d: w=%v}", tc.from, tc.to, got, r.ID(), tc.want)
				}
			}
		})
	}
}

// Lesson-42 shape: a plain Update CLEARS the explicit valid_from on the new
// version (effective valid-from becomes the mint time). The old explicit
// window must still resolve to the old version.
func TestForEachAdjacentAt_Break_ClearedValidFromKeepsOldWindow(t *testing.T) {
	t.Parallel()
	for _, be := range adjBackends() {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)
			hub, _ := g.Nodes.Add(ctx, []string{"Hub"}, nil)
			tgt, _ := g.Nodes.Add(ctx, []string{"T"}, nil)
			r, _ := g.Rels.Add(ctx, "LINK", hub, tgt, map[string]any{"w": int64(1), "tkg_valid_from": int64(1000)})
			r2, err := g.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(2)})
			if err != nil {
				t.Fatal(err)
			}
			if r2.Temporal().ValidFrom != 0 {
				t.Fatalf("precondition: plain Update must clear ValidFrom, got %d", r2.Temporal().ValidFrom)
			}
			got := adjRelsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{ValidAt: 1500})
			if len(got) != 1 || got[r.ID()] != int64(1) {
				t.Errorf("at=1500: got %v, want old version w=1", got)
			}
			got = adjRelsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{ValidAt: 1 << 50})
			if len(got) != 1 || got[r.ID()] != int64(2) {
				t.Errorf("far future: got %v, want current w=2", got)
			}
		})
	}
}

// Type and direction filters apply to the RESOLVED version, and a sibling
// relationship of another type on the same hub never leaks in.
func TestForEachAdjacentAt_Break_TypeAndDirectionStillFilter(t *testing.T) {
	t.Parallel()
	for _, be := range adjBackends() {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)
			hub, _ := g.Nodes.Add(ctx, []string{"Hub"}, nil)
			tgt, _ := g.Nodes.Add(ctx, []string{"T"}, nil)
			link, _ := g.Rels.Add(ctx, "LINK", hub, tgt, map[string]any{"w": int64(1), "tkg_valid_from": int64(1000)})
			other, _ := g.Rels.Add(ctx, "OTHER", hub, tgt, map[string]any{"w": int64(9), "tkg_valid_from": int64(1000)})
			back, _ := g.Rels.Add(ctx, "LINK", tgt, hub, map[string]any{"w": int64(7), "tkg_valid_from": int64(1000)})
			for _, id := range []types.RelID{link.ID(), other.ID(), back.ID()} {
				if _, err := g.Rels.Update(ctx, id, map[string]any{"w": int64(100), "tkg_valid_from": int64(2000)}); err != nil {
					t.Fatal(err)
				}
			}
			at := storepkg.QueryOpts{ValidAt: 1500}
			if got := adjRelsAt(t, g, hub.ID(), "LINK", false, at); len(got) != 1 || got[link.ID()] != int64(1) {
				t.Errorf("hub outgoing LINK: %v, want only link w=1", got)
			}
			if got := adjRelsAt(t, g, hub.ID(), "LINK", true, at); len(got) != 1 || got[back.ID()] != int64(7) {
				t.Errorf("hub incoming LINK: %v, want only back w=7", got)
			}
			if got := adjRelsAt(t, g, hub.ID(), "OTHER", false, at); len(got) != 1 || got[other.ID()] != int64(9) {
				t.Errorf("hub outgoing OTHER: %v, want only other w=9", got)
			}
			if got := adjRelsAt(t, g, hub.ID(), "", false, at); len(got) != 2 {
				t.Errorf("hub outgoing any type: %v, want link+other", got)
			}
			if got := adjRelsAt(t, g, hub.ID(), "NOPE", false, at); len(got) != 0 {
				t.Errorf("unregistered type: %v, want nothing", got)
			}
		})
	}
}

// Parity with the node doors and Temporal().OutgoingRelsAt: a DELETED
// relationship that was valid at t is still part of the adjacency at t
// (endpoints are immutable; the node path folds deleted candidates the same
// way). Far-future queries must not resurrect it.
func TestForEachAdjacentAt_Break_DeletedRelStillVisibleInItsWindow(t *testing.T) {
	t.Parallel()
	for _, be := range adjBackends() {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)
			// The named-door oracle (OutgoingRelsAt) also requires the HUB to be
			// valid at the probe instant; the scan door leaves node validity to
			// its caller (the node was already resolved at t), so stamp the hub.
			hub, _ := g.Nodes.Add(ctx, []string{"Hub"}, map[string]any{"tkg_valid_from": int64(1)})
			tgt, _ := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": int64(1)})
			r, _ := g.Rels.Add(ctx, "LINK", hub, tgt, map[string]any{"w": int64(1), "tkg_valid_from": int64(1000)})
			if err := g.Rels.Delete(ctx, r.ID()); err != nil {
				t.Fatal(err)
			}
			oracle, err := g.Temporal.OutgoingRelsAt(hub.ID(), 1500)
			if err != nil {
				t.Fatal(err)
			}
			got := adjRelsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{ValidAt: 1500})
			if len(got) != len(oracle) {
				t.Errorf("at=1500: door=%v oracle(OutgoingRelsAt)=%d rels — the scan door disagrees with the named door", got, len(oracle))
			}
			eps := adjEndpointsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{ValidAt: 1500})
			if len(eps) != len(oracle) {
				t.Errorf("endpoint door at=1500: %v, oracle %d", eps, len(oracle))
			}
			if got := adjRelsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{ValidAt: 1 << 50}); len(got) != 0 {
				t.Errorf("deleted rel resurrected in the far future: %v", got)
			}
		})
	}
}

// Value-level oracle referencing a DIFFERENT door (Pattern 57): for a hub
// with several multi-version and closed edges, the scan door's row set at
// every probe instant must equal Temporal().OutgoingRelsAt restricted to the
// type, and the endpoint door must agree with the rel door.
func TestForEachAdjacentAt_Break_ParityWithOutgoingRelsAt(t *testing.T) {
	t.Parallel()
	for _, be := range adjBackends() {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)
			hub, _ := g.Nodes.Add(ctx, []string{"Hub"}, map[string]any{"tkg_valid_from": int64(1)})
			var targets []*types.Node
			for i := 0; i < 4; i++ {
				n, _ := g.Nodes.Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": int64(1)})
				targets = append(targets, n)
			}
			mk := func(typ string, tgt int, vf int64, vt int64) types.RelID {
				props := map[string]any{"w": vf, "tkg_valid_from": vf}
				if vt != 0 {
					props["tkg_valid_to"] = vt
				}
				r, err := g.Rels.Add(ctx, typ, hub, targets[tgt], props)
				if err != nil {
					t.Fatal(err)
				}
				return r.ID()
			}
			a := mk("LINK", 0, 100, 0)
			mk("LINK", 1, 100, 300)
			c := mk("LINK", 2, 500, 0)
			mk("OTHER", 3, 100, 0)
			// two more versions on a, one on c
			for _, up := range []struct {
				id types.RelID
				vf int64
			}{{a, 400}, {a, 800}, {c, 900}} {
				if _, err := g.Rels.Update(ctx, up.id, map[string]any{"w": up.vf, "tkg_valid_from": up.vf}); err != nil {
					t.Fatal(err)
				}
			}
			for _, at := range []int64{50, 100, 200, 300, 350, 450, 550, 850, 950, 1 << 50} {
				oracleRows, err := g.Temporal.OutgoingRelsAt(hub.ID(), types.Instant(at))
				if err != nil {
					t.Fatal(err)
				}
				oracle := map[types.RelID]any{}
				for _, r := range oracleRows {
					if g.Rels.Type(r) == "LINK" {
						oracle[r.ID()] = r.PropertiesMap()["w"]
					}
				}
				got := adjRelsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{ValidAt: types.Instant(at)})
				if len(got) != len(oracle) {
					t.Errorf("at=%d: door=%v oracle=%v", at, got, oracle)
					continue
				}
				for id, w := range oracle {
					if got[id] != w {
						t.Errorf("at=%d rel %d: door w=%v oracle w=%v", at, id, got[id], w)
					}
				}
				eps := adjEndpointsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{ValidAt: types.Instant(at)})
				if len(eps) != len(got) {
					t.Errorf("at=%d: endpoint door %v disagrees with rel door %v", at, eps, got)
				}
			}
		})
	}
}

// Contract edges survive the version-aware path: early stop, nil callback,
// missing node, and the no-filter path still being exactly Outgoing().
func TestForEachAdjacentAt_Break_ContractEdges(t *testing.T) {
	t.Parallel()
	for _, be := range adjBackends() {
		t.Run(be.name, func(t *testing.T) {
			ctx := context.Background()
			g := be.build(t)
			hub, _ := g.Nodes.Add(ctx, []string{"Hub"}, nil)
			var ids []types.RelID
			for i := 0; i < 3; i++ {
				tgt, _ := g.Nodes.Add(ctx, []string{"T"}, nil)
				r, _ := g.Rels.Add(ctx, "LINK", hub, tgt, map[string]any{"w": int64(i), "tkg_valid_from": int64(1000)})
				if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(10 + i), "tkg_valid_from": int64(2000)}); err != nil {
					t.Fatal(err)
				}
				ids = append(ids, r.ID())
			}
			at := storepkg.QueryOpts{ValidAt: 1500}

			calls := 0
			if err := g.Rels.ForEachAdjacentRelAt(hub.ID(), "LINK", false, at, func(*types.Relationship) bool { calls++; return false }); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Errorf("early stop: fn called %d times, want 1", calls)
			}
			calls = 0
			if err := g.Rels.ForEachAdjacentEndpointAt(hub.ID(), "LINK", false, at, func(types.RelID, types.NodeID) bool { calls++; return false }); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Errorf("endpoint early stop: fn called %d times, want 1", calls)
			}

			if err := g.Rels.ForEachAdjacentRelAt(hub.ID(), "LINK", false, at, nil); !errors.Is(err, grapherr.ErrNilCallback) {
				t.Errorf("nil fn: %v, want ErrNilCallback", err)
			}
			missing := types.NodeID(hub.ID().SnowflakeID() + 12345)
			if err := g.Rels.ForEachAdjacentRelAt(missing, "LINK", false, at, func(*types.Relationship) bool { return true }); !errors.Is(err, storepkg.ErrNodeNotFound) {
				t.Errorf("missing node: %v, want ErrNodeNotFound", err)
			}

			// Deterministic order: ascending rel id, like Outgoing().
			var order []types.RelID
			if err := g.Rels.ForEachAdjacentRelAt(hub.ID(), "LINK", false, at, func(r *types.Relationship) bool { order = append(order, r.ID()); return true }); err != nil {
				t.Fatal(err)
			}
			if !sort.SliceIsSorted(order, func(i, j int) bool { return order[i] < order[j] }) || len(order) != 3 {
				t.Errorf("order = %v, want ascending ids", order)
			}

			// No temporal filter: current rows only, exactly Outgoing().
			cur := adjRelsAt(t, g, hub.ID(), "LINK", false, storepkg.QueryOpts{})
			for i, id := range ids {
				if cur[id] != int64(10+i) {
					t.Errorf("no-filter: rel %d w=%v, want current %d", id, cur[id], 10+i)
				}
			}
		})
	}
}
