package core

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 6 request 12 (sigma-tkgd, EnvelopePruneLarge: 5.02 of 5.02 s in
// ByLabel at a valid-time coordinate, every candidate read a second time
// through GetNode and its version resolved). A label read at a temporal
// coordinate answers a node without history rows from the current row the
// label scan already read; the doors stream in ascending ID order and resolve
// only as far as the caller reads.

// labelAtFixture builds :E nodes whose valid intervals are mixed (closed
// before the probe instants, open, closed around them, starting after, no
// valid-from), some of them updated after creation (a history row each), one
// deleted, one that lost the label and one :Other node. It returns every node
// ID it made and the IDs with history rows (updated, deleted, relabelled).
func labelAtFixture(t *testing.T, g *Core, n int) (all, withHistory []types.NodeID) {
	t.Helper()
	ctx := context.Background()
	for i := range n {
		props := map[string]any{"k": int64(i)}
		switch i % 5 {
		case 0:
			props["tkg_valid_from"], props["tkg_valid_to"] = types.Instant(1000), types.Instant(2000)
		case 1:
			props["tkg_valid_from"] = types.Instant(1000 + int64(i))
		case 2:
			props["tkg_valid_from"], props["tkg_valid_to"] = types.Instant(4000), types.Instant(4001+int64(i))
		case 3:
			props["tkg_valid_from"] = types.Instant(6000)
		}
		nd, err := g.Nodes.Add(ctx, []string{"E"}, props)
		if err != nil {
			t.Fatal(err)
		}
		all = append(all, nd.ID())
	}
	other, err := g.Nodes.Add(ctx, []string{"Other"}, map[string]any{"tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	all = append(all, other.ID())
	// Two-phase members: a later version replaces the one valid at the probes.
	for i := 1; i < n; i += 7 {
		if i%5 != 1 && i%5 != 3 {
			continue // a closed version cannot be updated; a mint-time one not moved back
		}
		if _, err := g.Nodes.Update(ctx, all[i], map[string]any{"k": int64(-i), "tkg_valid_from": types.Instant(7500)}); err != nil {
			t.Fatal(err)
		}
		withHistory = append(withHistory, all[i])
	}
	if err := g.Nodes.Delete(ctx, all[2]); err != nil {
		t.Fatal(err)
	}
	withHistory = append(withHistory, all[2])
	if err := g.Nodes.AddLabel(ctx, all[4], "Other"); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes.RemoveLabel(ctx, all[4], "E"); err != nil {
		t.Fatal(err)
	}
	withHistory = append(withHistory, all[4])
	return all, withHistory
}

// labelAtOracle resolves every node the fixture made through the chain
// resolver's own door, findNodeVersionForOpts: the IDs and versions a label
// read at opts must answer, ascending by ID.
func labelAtOracle(t *testing.T, g *Core, ids []types.NodeID, label string, opts storepkg.QueryOpts) []string {
	t.Helper()
	tok, ok := g.labels.Lookup(label)
	if !ok {
		t.Fatalf("label %s unknown", label)
	}
	pred := func(n *types.Node) bool { return n.HasLabelTokenRaw(tok) }
	var out []string
	if err := g.readUnderRLock(func() error {
		resolve := g.normalizeTxAtOnlyOpts(opts)
		for _, id := range slices.Sorted(slices.Values(ids)) {
			n, err := g.findNodeVersionForOpts(id, resolve, pred)
			if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrNodeNotFound) {
				continue
			}
			if err != nil {
				return err
			}
			out = append(out, nodeAtKey(n))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

func nodeAtKey(n *types.Node) string {
	k, _ := n.GetProperty("k")
	return fmt.Sprintf("%d:v%d:k%v", n.ID().SnowflakeID(), n.Version(), k)
}

func nodeAtKeys(nodes []*types.Node) []string {
	out := make([]string, len(nodes))
	for i, n := range nodes {
		out[i] = nodeAtKey(n)
	}
	return out
}

var labelAtProbes = []storepkg.QueryOpts{
	{ValidAt: 500},
	{ValidAt: 1500},
	{ValidAt: 4500},
	{ValidAt: 5000},
	{ValidAt: 7000},
	{ValidAt: 8000},
	{ValidStart: 7000, ValidEnd: 7600},
	{ValidStart: 1500, ValidEnd: 4500},
	{ValidStart: 100, ValidEnd: 900},
	{ValidStart: 5000, ValidEnd: 1 << 50},
}

// TestLabelReadAtAgreesWithTheResolver: ByLabel, ForEachByLabel (with and
// without NoSort) and CountByLabelAt at a point and over an interval answer
// exactly the oracle's set, in ascending ID order, on every backend — the
// updated nodes with their older version where it held, the deleted node in
// its window, the node that left the label only where it held it.
func TestLabelReadAtAgreesWithTheResolver(t *testing.T) {
	for _, be := range pinnedPropBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			all, _ := labelAtFixture(t, g, 60)
			for _, opts := range labelAtProbes {
				want := labelAtOracle(t, g, all, "E", opts)
				got, err := g.Nodes.ByLabel("E", opts)
				if err != nil {
					t.Fatal(err)
				}
				if keys := nodeAtKeys(got); !slices.Equal(keys, want) {
					t.Fatalf("ByLabel %+v:\n got %v\nwant %v", opts, keys, want)
				}
				for _, noSort := range []bool{false, true} {
					o := opts
					o.NoSort = noSort
					var streamed []*types.Node
					if err := g.Nodes.ForEachByLabel("E", o, func(n *types.Node) bool {
						streamed = append(streamed, n)
						return true
					}); err != nil {
						t.Fatal(err)
					}
					keys := nodeAtKeys(streamed)
					if noSort {
						slices.Sort(keys)
						w := slices.Clone(want)
						slices.Sort(w)
						if !slices.Equal(keys, w) {
							t.Fatalf("ForEachByLabel NoSort %+v:\n got %v\nwant %v", opts, keys, w)
						}
						continue
					}
					if !slices.Equal(keys, want) {
						t.Fatalf("ForEachByLabel %+v:\n got %v\nwant %v", opts, keys, want)
					}
				}
				count, err := g.Nodes.CountByLabelAt("E", opts)
				if err != nil || count != len(want) {
					t.Fatalf("CountByLabelAt %+v = %d %v, want %d", opts, count, err, len(want))
				}
				if len(want) > 3 {
					// Keyset pagination over the same order.
					o := opts
					o.After, o.Limit = types.EntityID(got[1].ID()), 2
					page, err := g.Nodes.ByLabel("E", o)
					if err != nil {
						t.Fatal(err)
					}
					var streamed []*types.Node
					if err := g.Nodes.ForEachByLabel("E", o, func(n *types.Node) bool {
						streamed = append(streamed, n)
						return true
					}); err != nil {
						t.Fatal(err)
					}
					if w := want[2:4]; !slices.Equal(nodeAtKeys(page), w) || !slices.Equal(nodeAtKeys(streamed), w) {
						t.Fatalf("page %+v: ByLabel %v, ForEachByLabel %v, want %v", o, nodeAtKeys(page), nodeAtKeys(streamed), w)
					}
				}
			}
		})
	}
}

// TestLabelReadAtReadsNoHistoryNodeTwice: on the stores that state history
// presence (memory, badger), a label read at a valid-time point or interval
// resolves through the chain resolver's door only the nodes with history rows;
// every other node is answered from the row the label scan read. Faulty
// implementation it catches: the per-candidate GetNode and chain load.
func TestLabelReadAtReadsNoHistoryNodeTwice(t *testing.T) {
	for _, be := range pinnedPropBackends() {
		if be.name != "memory" && be.name != "badger" {
			continue
		}
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			_, withHistory := labelAtFixture(t, g, 200)
			for _, opts := range labelAtProbes {
				loads := countChainLoads(g, func() {
					if _, err := g.Nodes.ByLabel("E", opts); err != nil {
						t.Fatal(err)
					}
				})
				if loads > len(withHistory) {
					t.Fatalf("ByLabel %+v resolved %d chains, the fixture has %d nodes with history", opts, loads, len(withHistory))
				}
				loads = countChainLoads(g, func() {
					if err := g.Nodes.ForEachByLabel("E", opts, func(*types.Node) bool { return true }); err != nil {
						t.Fatal(err)
					}
				})
				if loads > len(withHistory) {
					t.Fatalf("ForEachByLabel %+v resolved %d chains, want at most %d", opts, loads, len(withHistory))
				}
				loads = countChainLoads(g, func() {
					if _, err := g.Nodes.CountByLabelAt("E", opts); err != nil {
						t.Fatal(err)
					}
				})
				if loads > len(withHistory) {
					t.Fatalf("CountByLabelAt %+v resolved %d chains, want at most %d", opts, loads, len(withHistory))
				}
			}
		})
	}
}

// TestForEachByLabelAtStreams: a label read at a valid-time coordinate
// resolves only as far as the caller reads — a callback that stops after the
// first row leaves the rest unresolved (at most one chunk). Faulty
// implementation it catches: resolving and materializing every candidate
// before the first callback.
func TestForEachByLabelAtStreams(t *testing.T) {
	g := pinnedPropBackends()[0].open(t)
	ctx := context.Background()
	const n = 3 * labelScanAtChunk
	for i := range n {
		nd, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"k": int64(i), "tkg_valid_from": types.Instant(1000)})
		if err != nil {
			t.Fatal(err)
		}
		// A history row each: every candidate takes the resolver's door.
		if _, err := g.Nodes.Update(ctx, nd.ID(), map[string]any{"k": int64(-i)}); err != nil {
			t.Fatal(err)
		}
	}
	rows := 0
	loads := countChainLoads(g, func() {
		if err := g.Nodes.ForEachByLabel("E", storepkg.QueryOpts{ValidAt: 1 << 50}, func(*types.Node) bool {
			rows++
			return false
		}); err != nil {
			t.Fatal(err)
		}
	})
	if rows != 1 || loads > labelScanAtChunk {
		t.Fatalf("a stop after the first row: %d rows, %d chains resolved (at most %d)", rows, loads, labelScanAtChunk)
	}
	// The whole read still answers every node, in ID order.
	var ids []types.NodeID
	if err := g.Nodes.ForEachByLabel("E", storepkg.QueryOpts{ValidAt: 1 << 50}, func(nd *types.Node) bool {
		ids = append(ids, nd.ID())
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != n || !slices.IsSorted(ids) {
		t.Fatalf("%d rows (want %d), sorted %v", len(ids), n, slices.IsSorted(ids))
	}
}

// TestSingleVersionNodeMatchesResolver holds firstVersionCovers equal to the
// chain resolver over the one-row chain for first versions of every interval
// shape, at instants on and around their bounds.
func TestSingleVersionNodeMatchesResolver(t *testing.T) {
	g := pinnedPropBackends()[0].open(t)
	ctx := context.Background()
	shapes := []map[string]any{
		nil,
		{"tkg_valid_from": int64(1000)},
		{"tkg_valid_from": int64(1000), "tkg_valid_to": int64(2000)},
		{"tkg_valid_from": int64(1000), "tkg_valid_to": int64(1001)},
		{"tkg_valid_to": int64(1 << 50)},
	}
	var rows []*types.Node
	for _, props := range shapes {
		n, err := g.Nodes.Add(ctx, []string{"P"}, props)
		if err != nil {
			t.Fatal(err)
		}
		rows = append(rows, n)
	}
	var instants []types.Instant
	for _, n := range rows {
		vf, vt, _ := n.ValidRange()
		if vf == 0 {
			vf = n.ID().MintInstant()
		}
		for _, x := range []types.Instant{vf, vt} {
			if x > 1 {
				instants = append(instants, x-1, x, x+1)
			}
		}
	}
	instants = append(instants, 1, 1<<51)
	for _, n := range rows {
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
				s := singleVersionNodeResolver{c: g, probe: probe}
				covered, ok := s.firstVersionCovers(n)
				if !ok {
					t.Fatalf("node %d version %d: not decided", n.ID(), n.Version())
				}
				_, rerr := g.resolveNodeChain([]*types.Node{n}, probe, nil)
				if covered != (rerr == nil) {
					t.Errorf("node %d probe %+v: firstVersionCovers %v, resolver err %v", n.ID(), probe, covered, rerr)
				}
			}
		}
	}
}

// TestTypeReadAtAgreesWithTheResolver is TestLabelReadAtAgreesWithTheResolver
// for relationship types (ByType, ForEachByType, CountByTypeAt), and on memory
// and badger holds the chain loads to the relationships with history.
func TestTypeReadAtAgreesWithTheResolver(t *testing.T) {
	for _, be := range pinnedPropBackends() {
		t.Run(be.name, func(t *testing.T) {
			g := be.open(t)
			ctx := context.Background()
			a, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
			b, _ := g.Nodes.Add(ctx, []string{"P"}, nil)
			var all, withHistory []types.RelID
			for i := range 60 {
				props := map[string]any{"k": int64(i)}
				switch i % 5 {
				case 0:
					props["tkg_valid_from"], props["tkg_valid_to"] = types.Instant(1000), types.Instant(2000)
				case 1:
					props["tkg_valid_from"] = types.Instant(1000 + int64(i))
				case 2:
					props["tkg_valid_from"], props["tkg_valid_to"] = types.Instant(4000), types.Instant(4001+int64(i))
				case 3:
					props["tkg_valid_from"] = types.Instant(6000)
				}
				typ := "R"
				if i == 59 {
					typ = "Other"
				}
				r, err := g.Rels.Add(ctx, typ, a, b, props)
				if err != nil {
					t.Fatal(err)
				}
				all = append(all, r.ID())
			}
			for i := 1; i < 59; i += 7 {
				if i%5 != 1 && i%5 != 3 {
					continue
				}
				if _, err := g.Rels.Update(ctx, all[i], map[string]any{"k": int64(-i), "tkg_valid_from": types.Instant(7500)}); err != nil {
					t.Fatal(err)
				}
				withHistory = append(withHistory, all[i])
			}
			if err := g.Rels.Delete(ctx, all[3]); err != nil {
				t.Fatal(err)
			}
			withHistory = append(withHistory, all[3])
			tok, _ := g.relTypes.Lookup("R")
			pred := func(r *types.Relationship) bool { return r.HasTypeTokenRaw(tok) }
			key := func(r *types.Relationship) string {
				k, _ := r.GetProperty("k")
				return fmt.Sprintf("%d:v%d:k%v", r.ID().SnowflakeID(), r.Version(), k)
			}
			for _, opts := range labelAtProbes {
				var want []string
				if err := g.readUnderRLock(func() error {
					resolve := g.normalizeTxAtOnlyOpts(opts)
					for _, id := range slices.Sorted(slices.Values(all)) {
						r, err := g.findRelVersionForOpts(id, resolve, pred)
						if errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, storepkg.ErrRelNotFound) {
							continue
						}
						if err != nil {
							return err
						}
						want = append(want, key(r))
					}
					return nil
				}); err != nil {
					t.Fatal(err)
				}
				var got, streamed []string
				loads := countChainLoads(g, func() {
					rels, err := g.Rels.ByType("R", opts)
					if err != nil {
						t.Fatal(err)
					}
					for _, r := range rels {
						got = append(got, key(r))
					}
				})
				if !slices.Equal(got, want) {
					t.Fatalf("ByType %+v:\n got %v\nwant %v", opts, got, want)
				}
				if (be.name == "memory" || be.name == "badger") && loads > len(withHistory) {
					t.Fatalf("ByType %+v resolved %d chains, %d relationships have history", opts, loads, len(withHistory))
				}
				if err := g.Rels.ForEachByType("R", opts, func(r *types.Relationship) bool {
					streamed = append(streamed, key(r))
					return true
				}); err != nil {
					t.Fatal(err)
				}
				if !slices.Equal(streamed, want) {
					t.Fatalf("ForEachByType %+v:\n got %v\nwant %v", opts, streamed, want)
				}
				if n, err := g.Rels.CountByTypeAt("R", opts); err != nil || n != len(want) {
					t.Fatalf("CountByTypeAt %+v = %d %v, want %d", opts, n, err, len(want))
				}
			}
		})
	}
}
