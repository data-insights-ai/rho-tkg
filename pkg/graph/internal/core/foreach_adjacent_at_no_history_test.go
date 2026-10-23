package core

// Round 5 request 6 (sigma-tkgd C4s): the valid-time adjacency doors
// (ForEachAdjacentEndpointAt / ForEachAdjacentRelAt under ValidAt or
// ValidStart+ValidEnd) resolved EVERY adjacent relationship through its
// version chain (a point read and a history read each), although a
// relationship without history rows has exactly one version, the live row
// the door already holds. A window search was 11 times the static one on a
// graph where no searched relationship had history.
//
// Faulty implementations caught:
//   - the chain load per history-less relationship (the count test);
//   - a shortcut that skips the history check (an updated or deleted
//     relationship answers from its live row: the oracle test's old windows);
//   - a shortcut that ignores the transaction-time pin (TxAt before the row
//     was recorded must not see it) or the type/direction predicate.

import (
	"context"
	"fmt"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type noHistAdjFixture struct {
	hub, other   types.NodeID
	plain        []types.RelID // never updated
	updated      types.RelID   // valid_from 1000 -> 2000
	deleted      types.RelID   // valid_from 1000, deleted
	closed       types.RelID   // valid [1000, 2000) by CloseVersion
	otherType    types.RelID   // same hub, type OTHER
	incomingOnly types.RelID   // other -> hub
	all          []types.RelID
}

func buildNoHistAdjFixture(t *testing.T, g *Core) noHistAdjFixture {
	t.Helper()
	ctx := context.Background()
	hub, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := g.Nodes.Add(ctx, []string{"P"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	add := func(typ string, from, to *types.Node, props map[string]any) types.RelID {
		t.Helper()
		r, err := g.Rels.Add(ctx, typ, from, to, props)
		if err != nil {
			t.Fatal(err)
		}
		return r.ID()
	}
	var f noHistAdjFixture
	f.hub, f.other = hub.ID(), other.ID()
	// Plain relationships with distinct intervals: open from 1000, [1500,
	// 2500) set at creation, open from 3000, no explicit valid_from (the
	// mint instant, far after the probes' small instants).
	f.plain = []types.RelID{
		add("LINK", hub, other, map[string]any{"w": int64(1), "tkg_valid_from": int64(1000)}),
		add("LINK", hub, other, map[string]any{"w": int64(2), "tkg_valid_from": int64(1500), "tkg_valid_to": int64(2500)}),
		add("LINK", hub, other, map[string]any{"w": int64(3), "tkg_valid_from": int64(3000)}),
		add("LINK", hub, other, map[string]any{"w": int64(4)}),
	}
	f.updated = add("LINK", hub, other, map[string]any{"w": int64(5), "tkg_valid_from": int64(1000)})
	if _, err := g.Rels.Update(ctx, f.updated, map[string]any{"w": int64(6), "tkg_valid_from": int64(2000)}); err != nil {
		t.Fatal(err)
	}
	f.deleted = add("LINK", hub, other, map[string]any{"w": int64(7), "tkg_valid_from": int64(1000)})
	if err := g.Rels.Delete(ctx, f.deleted); err != nil {
		t.Fatal(err)
	}
	f.closed = add("LINK", hub, other, map[string]any{"w": int64(8), "tkg_valid_from": int64(1000)})
	if err := g.Rels.CloseVersion(ctx, f.closed, 2000); err != nil {
		t.Fatal(err)
	}
	f.otherType = add("OTHER", hub, other, map[string]any{"w": int64(9), "tkg_valid_from": int64(1000)})
	f.incomingOnly = add("LINK", other, hub, map[string]any{"w": int64(10), "tkg_valid_from": int64(1000)})
	f.all = append(append([]types.RelID(nil), f.plain...), f.updated, f.deleted, f.closed, f.otherType, f.incomingOnly)
	return f
}

// adjOracle is the version-aware answer computed relationship by
// relationship through findRelVersionForOpts (the resolver the door uses for
// a relationship with history): {rel: "w"} of the version valid under opts.
func adjOracle(t *testing.T, g *Core, f noHistAdjFixture, node types.NodeID, typ string, incoming bool, opts storepkg.QueryOpts) map[types.RelID]any {
	t.Helper()
	var tok uint16
	if typ != "" {
		var ok bool
		if tok, ok = g.lookupRelTypeQueryToken(typ); !ok {
			t.Fatalf("type %q", typ)
		}
	}
	pred := func(r *types.Relationship) bool {
		if tok != 0 && !r.HasTypeTokenRaw(tok) {
			return false
		}
		if incoming {
			return r.EndNodeID() == node
		}
		return r.StartNodeID() == node
	}
	want := map[types.RelID]any{}
	if err := g.readUnderRLock(func() error {
		for _, id := range f.all {
			r, err := g.findRelVersionForOpts(id, g.normalizeTxAtOnlyOpts(opts), pred)
			if err != nil {
				continue
			}
			want[id] = r.PropertiesMap()["w"]
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return want
}

func noHistProbeOpts(f noHistAdjFixture, g *Core) []storepkg.QueryOpts {
	var out []storepkg.QueryOpts
	for _, at := range []int64{500, 999, 1000, 1499, 1500, 1999, 2000, 2499, 2500, 2999, 3000, 1 << 50} {
		out = append(out, storepkg.QueryOpts{ValidAt: types.Instant(at)})
	}
	for _, w := range [][2]int64{{100, 1000}, {999, 1000}, {1000, 1001}, {1200, 1800}, {2000, 2500}, {2499, 2500}, {2500, 3000}, {2999, 3000}, {100, 1 << 50}} {
		out = append(out, storepkg.QueryOpts{ValidStart: types.Instant(w[0]), ValidEnd: types.Instant(w[1])})
	}
	// Transaction-time pins: before anything was recorded (nothing visible)
	// and now (everything recorded so far), with and without a valid-time
	// filter.
	now := g.readNow()
	out = append(out,
		storepkg.QueryOpts{TxAt: 1},
		storepkg.QueryOpts{TxAt: now},
		storepkg.QueryOpts{ValidAt: 1500, TxAt: 1},
		storepkg.QueryOpts{ValidAt: 1500, TxAt: now},
		storepkg.QueryOpts{ValidStart: 100, ValidEnd: 1 << 50, TxAt: 1},
		storepkg.QueryOpts{TxPin: now},
	)
	_ = f
	return out
}

// TestForEachAdjacentAt_NoHistoryRelsAgreeWithChainResolution: both doors,
// both directions, with and without a type, on every backend, against the
// per-relationship version resolution, for instants, windows (boundaries
// included) and transaction-time pins.
func TestForEachAdjacentAt_NoHistoryRelsAgreeWithChainResolution(t *testing.T) {
	t.Parallel()
	for _, be := range pinnedPropBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			f := buildNoHistAdjFixture(t, g)
			for _, opts := range noHistProbeOpts(f, g) {
				for _, dir := range []struct {
					node     types.NodeID
					incoming bool
				}{{f.hub, false}, {f.hub, true}, {f.other, true}, {f.other, false}} {
					for _, typ := range []string{"LINK", ""} {
						name := fmt.Sprintf("%+v node=%d incoming=%v type=%q", opts, dir.node, dir.incoming, typ)
						want := adjOracle(t, g, f, dir.node, typ, dir.incoming, opts)
						got := adjRelsAt(t, g, dir.node, typ, dir.incoming, opts)
						if !sameAnyMap(got, want) {
							t.Errorf("%s: rel door %v, want %v", name, got, want)
						}
						eps := adjEndpointsAt(t, g, dir.node, typ, dir.incoming, opts)
						if len(eps) != len(want) {
							t.Errorf("%s: endpoint door %v, want rels %v", name, eps, want)
						}
						for id := range want {
							if _, ok := eps[id]; !ok {
								t.Errorf("%s: endpoint door misses rel %d", name, id)
							}
						}
					}
				}
			}
		})
	}
}

func sameAnyMap(a, b map[types.RelID]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

// TestForEachAdjacentAt_NoHistoryRelsLoadNoChain: under a pure valid-time
// filter the doors resolve a version chain only for the relationships with
// history rows (here the updated, the deleted and the closed one), on the
// stores that state history presence; the plain relationships answer from
// the live row the door already read.
func TestForEachAdjacentAt_NoHistoryRelsLoadNoChain(t *testing.T) {
	t.Parallel()
	for _, be := range pinnedPropBackends() {
		if be.name == "wrapper" {
			continue // states history presence through the embedded store too, but keep the count to the in-tree stores
		}
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			f := buildNoHistAdjFixture(t, g)
			var withHistory []types.RelID
			for _, id := range f.all {
				has, err := g.Rels.HasHistory(id)
				if err != nil {
					t.Fatal(err)
				}
				if has {
					withHistory = append(withHistory, id)
				}
			}
			sort.Slice(withHistory, func(i, j int) bool { return withHistory[i] < withHistory[j] })
			if want := []types.RelID{f.updated, f.deleted, f.closed}; fmt.Sprint(withHistory) != fmt.Sprint(sortedRelIDs(want)) {
				t.Fatalf("fixture: rels with history %v, want %v", withHistory, sortedRelIDs(want))
			}
			for _, opts := range []storepkg.QueryOpts{
				{ValidStart: 1200, ValidEnd: 1800},
				{ValidStart: 100, ValidEnd: 1 << 50},
				{ValidAt: 1600},
			} {
				// LINK out of hub: 4 plain + updated + deleted + closed.
				loads := countChainLoads(g, func() { adjEndpointsAt(t, g, f.hub, "LINK", false, opts) })
				if loads > len(withHistory) {
					t.Errorf("%+v: %d chain loads, want at most %d (the rels with history)", opts, loads, len(withHistory))
				}
				loads = countChainLoads(g, func() { adjRelsAt(t, g, f.hub, "LINK", false, opts) })
				if loads > len(withHistory) {
					t.Errorf("%+v rel door: %d chain loads, want at most %d", opts, loads, len(withHistory))
				}
			}
		})
	}
}

func sortedRelIDs(ids []types.RelID) []types.RelID {
	out := append([]types.RelID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestSingleVersionAdjacencyMatchesResolver holds firstVersionCovers equal
// to the chain resolver over the same one-row chain, for first versions with
// and without valid-from and valid-to, at instants and windows around every
// bound. Faulty implementation caught: a bound off by one (half-open window,
// inclusive start), the mint-instant fallback missing, a valid-to ignored.
func TestSingleVersionAdjacencyMatchesResolver(t *testing.T) {
	t.Parallel()
	g := pinnedPropBackends()[0].open(t)
	ctx := context.Background()
	a, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
	b, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
	shapes := []map[string]any{
		nil,
		{"tkg_valid_from": int64(1000)},
		{"tkg_valid_from": int64(1000), "tkg_valid_to": int64(2000)},
		{"tkg_valid_from": int64(1000), "tkg_valid_to": int64(1001)},
		{"tkg_valid_to": int64(1 << 50)},
	}
	for _, props := range shapes {
		if _, err := g.Rels.Add(ctx, "LINK", a, b, props); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := g.store.OutgoingRelationships(a.ID(), 0)
	if err != nil || len(rows) != len(shapes) {
		t.Fatalf("rows %d %v", len(rows), err)
	}
	var instants []types.Instant
	for _, r := range rows {
		vf, vt, _ := r.ValidRange()
		if vf == 0 {
			vf = r.ID().MintInstant()
		}
		for _, x := range []types.Instant{vf, vt} {
			if x > 1 {
				instants = append(instants, x-1, x, x+1)
			}
		}
	}
	instants = append(instants, 1, 1<<51)
	for _, r := range rows {
		for _, at := range instants {
			for _, probe := range []chainProbe{
				{kind: probePoint, validAt: at},
				{kind: probeInterval, validStart: at, validEnd: at + 1},
				{kind: probeInterval, validStart: 1, validEnd: at},
				{kind: probeInterval, validStart: at, validEnd: 1 << 52},
			} {
				if probe.kind == probeInterval && probe.validStart >= probe.validEnd {
					continue
				}
				s := singleVersionAdjacencyResolver{c: g, probe: probe}
				covered, ok := s.firstVersionCovers(r)
				if !ok {
					t.Fatalf("rel %d version %d: not decided", r.ID(), r.Version())
				}
				_, rerr := g.resolveRelChain([]*types.Relationship{r}, probe, nil)
				if covered != (rerr == nil) {
					t.Errorf("rel %d probe %+v: firstVersionCovers %v, resolver err %v", r.ID(), probe, covered, rerr)
				}
			}
		}
	}
}
