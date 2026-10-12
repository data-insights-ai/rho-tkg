package core

// Round 5 request 8 (sigma-tkgd C4s): a temporal seek by property value on a
// (label, key) without a declared index — Nodes().ByLabelAndProperty /
// ByLabelAndProperties under a temporal filter — resolved the version chain
// of EVERY node with history, of any label: the cold harness's touched nodes
// (another label, hundreds of versions each) cost about 1 ms per seek on
// badger.
//
// Faulty implementations caught:
//   - the full-history fold (the count test: loads track the other label's
//     churn);
//   - a filter by the CURRENT label (a node that carried the label and value
//     in the past and lost the label since is dropped: the oracle test);
//   - a filter that ignores the transaction-time pin's lower bound on the
//     label's acquisition the other way round (a node that acquired the
//     label after the pin must not answer at the pin).

import (
	"context"
	"fmt"
	"sort"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type seekFixture struct {
	plain      types.NodeID // P {id: 1}, never changed
	updated    types.NodeID // P {id: 1} -> {id: 2} at valid 2000
	relabelled types.NodeID // P {id: 1} valid from 1000; label P removed later
	gained     types.NodeID // Q {id: 1}; label P added later
	other      types.NodeID // P {id: 5}
	churn      []types.NodeID
	pinBefore  types.Instant // after the first writes, before gained got P
}

func buildSeekFixture(t *testing.T, g *Core, churn, versions int) seekFixture {
	t.Helper()
	ctx := context.Background()
	add := func(labels []string, props map[string]any) types.NodeID {
		t.Helper()
		n, err := g.Nodes.Add(ctx, labels, props)
		if err != nil {
			t.Fatal(err)
		}
		return n.ID()
	}
	var f seekFixture
	f.plain = add([]string{"P"}, map[string]any{"id": int64(1), "tkg_valid_from": int64(1000)})
	f.updated = add([]string{"P"}, map[string]any{"id": int64(1), "tkg_valid_from": int64(1000)})
	if _, err := g.Nodes.Update(ctx, f.updated, map[string]any{"id": int64(2), "tkg_valid_from": int64(2000)}); err != nil {
		t.Fatal(err)
	}
	f.relabelled = add([]string{"P", "R"}, map[string]any{"id": int64(1), "tkg_valid_from": int64(1000)})
	f.gained = add([]string{"Q"}, map[string]any{"id": int64(1), "tkg_valid_from": int64(1000)})
	f.other = add([]string{"P"}, map[string]any{"id": int64(5), "tkg_valid_from": int64(1000)})
	f.pinBefore = g.readNow()
	if err := g.Nodes.RemoveLabel(ctx, f.relabelled, "P"); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes.AddLabel(ctx, f.gained, "P"); err != nil {
		t.Fatal(err)
	}
	// Churn: nodes of another label with many versions each, carrying the
	// sought key and value too (only the label tells them apart).
	for i := range churn {
		id := add([]string{"Tick"}, map[string]any{"id": int64(1), "t": int64(0)})
		for v := range versions {
			if err := g.Nodes.SetProperty(ctx, id, "t", int64(v+1)); err != nil {
				t.Fatal(err)
			}
		}
		f.churn = append(f.churn, id)
		_ = i
	}
	return f
}

func seekIDSet(nodes []*types.Node) []types.NodeID {
	out := make([]types.NodeID, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID())
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

func sortedNodeIDs(ids ...types.NodeID) []types.NodeID {
	out := append([]types.NodeID(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// TestTemporalPropertySeekAnswersEverLabelMembers: exact sets for windows,
// instants and pins, both doors, every backend.
func TestTemporalPropertySeekAnswersEverLabelMembers(t *testing.T) {
	for _, be := range pinnedPropBackends() {
		if be.name == "tiered" {
			continue // a label change across its reference/event classes is refused there
		}
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			f := buildSeekFixture(t, g, 3, 4)
			now := g.readNow()
			cases := []struct {
				opts storepkg.QueryOpts
				want []types.NodeID
			}{
				// The relabelled node held P and id 1 from 1000 until its
				// label was removed (a version valid from that write's
				// instant, long after these windows); the gained node held Q
				// only until then.
				{storepkg.QueryOpts{ValidStart: 1000, ValidEnd: 1500}, sortedNodeIDs(f.plain, f.updated, f.relabelled)},
				{storepkg.QueryOpts{ValidStart: 2000, ValidEnd: 2500}, sortedNodeIDs(f.plain, f.relabelled)},
				{storepkg.QueryOpts{ValidStart: 1000, ValidEnd: now + 1<<40}, sortedNodeIDs(f.plain, f.updated, f.relabelled, f.gained)},
				{storepkg.QueryOpts{ValidStart: 100, ValidEnd: 1000}, nil},
				{storepkg.QueryOpts{ValidAt: 1500}, sortedNodeIDs(f.plain, f.updated, f.relabelled)},
				{storepkg.QueryOpts{ValidAt: 999}, nil},
				// At the pin before the label changes: the relabelled node
				// still had P, the gained one did not yet.
				{storepkg.QueryOpts{TxPin: f.pinBefore}, sortedNodeIDs(f.plain, f.relabelled)},
				{storepkg.QueryOpts{TxPin: now}, sortedNodeIDs(f.plain, f.gained)},
				{storepkg.QueryOpts{ValidAt: 1500, TxAt: f.pinBefore}, sortedNodeIDs(f.plain, f.updated, f.relabelled)},
			}
			for _, tc := range cases {
				got, err := g.Nodes.ByLabelAndProperty("P", "id", int64(1), tc.opts)
				if err != nil {
					t.Fatalf("%+v: %v", tc.opts, err)
				}
				if fmt.Sprint(seekIDSet(got)) != fmt.Sprint(tc.want) {
					t.Errorf("ByLabelAndProperty %+v = %v, want %v", tc.opts, seekIDSet(got), tc.want)
				}
			}
		})
	}
}

// TestTemporalPropertySeekLoadsNoOtherLabelChains: a seek on (P, id) loads
// the chains of the current matches and of the nodes that ever carried P,
// not those of the churned :Tick nodes, however many there are.
func TestTemporalPropertySeekLoadsNoOtherLabelChains(t *testing.T) {
	for _, be := range pinnedLoadBackends() {
		if be.name == "sharded" {
			continue // states no label membership: the full-history fold stays its contract
		}
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			f := buildSeekFixture(t, g, 20, 3)
			// Ever-members of P: plain, updated, relabelled, gained, other.
			const everP = 5
			for _, opts := range []storepkg.QueryOpts{
				{ValidStart: 1000, ValidEnd: 1500},
				{ValidAt: 1500},
				{TxPin: f.pinBefore},
			} {
				loads := countChainLoads(g, func() {
					if _, err := g.Nodes.ByLabelAndProperty("P", "id", int64(1), opts); err != nil {
						t.Fatal(err)
					}
				})
				if loads > everP {
					t.Errorf("ByLabelAndProperty %+v: %d chain loads, want at most %d (the ever-members of P)", opts, loads, everP)
				}
				loads = countChainLoads(g, func() {
					if _, err := g.Nodes.ByLabelAndProperties("P", map[string]any{"id": int64(1), "t": int64(3)}, opts); err != nil {
						t.Fatal(err)
					}
				})
				if loads > everP {
					t.Errorf("ByLabelAndProperties %+v: %d chain loads, want at most %d", opts, loads, everP)
				}
			}
		})
	}
}

// TestTemporalRelPropertySeekScopedToType is the relationship twin: a
// temporal ByTypeAndProperty on (T, seat) without an index answers the T
// relationships that held the value (a deleted one included) and loads no
// chain of the churned relationships of another type.
func TestTemporalRelPropertySeekScopedToType(t *testing.T) {
	for _, be := range pinnedPropBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			ctx := context.Background()
			a, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
			b, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
			add := func(typ string, props map[string]any) types.RelID {
				t.Helper()
				r, err := g.Rels.Add(ctx, typ, a, b, props)
				if err != nil {
					t.Fatal(err)
				}
				return r.ID()
			}
			plain := add("T", map[string]any{"seat": int64(1), "tkg_valid_from": int64(1000)})
			updated := add("T", map[string]any{"seat": int64(1), "tkg_valid_from": int64(1000)})
			if _, err := g.Rels.Update(ctx, updated, map[string]any{"seat": int64(2), "tkg_valid_from": int64(2000)}); err != nil {
				t.Fatal(err)
			}
			deleted := add("T", map[string]any{"seat": int64(1), "tkg_valid_from": int64(1000)})
			if err := g.Rels.Delete(ctx, deleted); err != nil {
				t.Fatal(err)
			}
			for range 15 {
				id := add("U", map[string]any{"seat": int64(1), "t": int64(0)})
				for v := range 3 {
					if _, err := g.Rels.Update(ctx, id, map[string]any{"seat": int64(1), "t": int64(v + 1)}); err != nil {
						t.Fatal(err)
					}
				}
			}
			for _, tc := range []struct {
				opts storepkg.QueryOpts
				want []types.RelID
			}{
				{storepkg.QueryOpts{ValidStart: 1000, ValidEnd: 1500}, sortedRelIDs([]types.RelID{plain, updated, deleted})},
				{storepkg.QueryOpts{ValidStart: 2000, ValidEnd: 2500}, sortedRelIDs([]types.RelID{plain, deleted})},
				{storepkg.QueryOpts{ValidAt: 999}, nil},
			} {
				var got []*types.Relationship
				loads := countChainLoads(g, func() {
					var err error
					if got, err = g.Rels.ByTypeAndProperty("T", "seat", int64(1), tc.opts); err != nil {
						t.Fatal(err)
					}
				})
				ids := make([]types.RelID, 0, len(got))
				for _, r := range got {
					ids = append(ids, r.ID())
				}
				if fmt.Sprint(sortedRelIDs(ids)) != fmt.Sprint(tc.want) {
					t.Errorf("%+v: %v, want %v", tc.opts, sortedRelIDs(ids), tc.want)
				}
				switch be.name {
				case "memory", "memory-segments", "badger", "badger-delta":
					if loads > 3 {
						t.Errorf("%+v: %d chain loads, want at most 3 (the T relationships)", tc.opts, loads)
					}
				}
			}
		})
	}
}
