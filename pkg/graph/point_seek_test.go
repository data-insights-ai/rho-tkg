package graph_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Round 11 item 1 (sigma-tkgd, 2026-10-17): an equality seek of a number is
// one index walk, not one per numeric form. Since v4.57.1 (5c) a seek
// {k: 1000} asked the store once per form of types.EqualIndexValues (ten for
// 1000), and on tiered and sharded every ask walked every shard.

// r11SeekWorld writes nodes "Ev" and relationships "E" with an index on k
// (numbers) and on s (strings): rows 0..n-1 hold k = int64(1000+i) and
// s = "v<i>", and the rows of the probe value 1000 are stored in three
// widths (int64, float64, int16), as are three rows of "v0" (so both probes
// have three matches).
func r11SeekWorld(t *testing.T, g *graphpkg.Graph, n int) {
	t.Helper()
	ctx := context.Background()
	for _, k := range []string{"k", "s"} {
		if err := g.Index().CreateProperty("Ev", k); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreateRelProperty("E", k); err != nil {
			t.Fatal(err)
		}
	}
	hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := range n {
		var k any = int64(1000 + i)
		s := fmt.Sprintf("v%d", i)
		switch i {
		case 1:
			k, s = 1000.0, "v0"
		case 2:
			k, s = int16(1000), "v0"
		}
		props := map[string]any{"k": k, "s": s}
		node, err := g.Nodes().Add(ctx, []string{"Ev"}, props)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Add(ctx, "E", hub, node, map[string]any{"k": k, "s": s}); err != nil {
			t.Fatal(err)
		}
	}
}

// TestRound11FormsSeekMatchesOracle: the one-walk seek keeps 5c's equality
// (every stored width of the value) and the door's paging and time filter:
// for every probe value and page size, the pages concatenate to the oracle's
// rows in ID order, each once, on every backend, nodes and relationships.
func TestRound11FormsSeekMatchesOracle(t *testing.T) {
	stored := []any{
		int64(3), 3.0, float32(3), int8(3), uint64(3), uint16(3), int32(3), "3", true,
		int64(-7), -7.0, float32(-7), int64(1<<53 + 1), float64(1 << 53), math.NaN(), uint64(math.MaxUint64),
		math.Inf(1), float32(math.Inf(1)), math.Copysign(0, -1), int64(0), float32(0.1), 0.1,
	}
	probes := []any{
		int64(3), 3.0, float32(3), uint8(3), int64(-7), float32(-7), int64(1<<53 + 1), float64(1 << 53),
		uint64(math.MaxUint64), math.Inf(1), 0.0, int64(0), float32(0.1), 0.1, "3", true, math.NaN(), nil,
	}
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreateProperty("Ev", "k"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("E", "k"); err != nil {
				t.Fatal(err)
			}
			hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			type row struct {
				node types.NodeID
				rel  types.RelID
				v    any
				past bool // closed in valid time before the probe instant 0
			}
			var rows []row
			for rep := range 2 {
				for i, v := range stored {
					props := map[string]any{"k": v}
					past := (rep+i)%3 == 0
					if past {
						props["tkg_valid_from"] = types.Instant(-500)
						props["tkg_valid_to"] = types.Instant(-100)
					}
					n, err := g.Nodes().Add(ctx, []string{"Ev"}, props)
					if err != nil {
						t.Fatal(err)
					}
					r, err := g.Rels().Add(ctx, "E", hub, n, props)
					if err != nil {
						t.Fatal(err)
					}
					rows = append(rows, row{n.ID(), r.ID(), v, past})
				}
			}
			for _, p := range probes {
				for _, timed := range []bool{false, true} {
					opts := storepkg.QueryOpts{}
					if timed {
						opts = storepkg.QueryOpts{ValidAt: 0, ValidAtSet: true} // the closed rows end at -100; untimed rows hold from -inf
					}
					var wantN []types.NodeID
					var wantR []types.RelID
					for _, r := range rows {
						if (!timed || !r.past) && types.PropertyEqual(r.v, p) {
							wantN = append(wantN, r.node)
							wantR = append(wantR, r.rel)
						}
					}
					slices.Sort(wantN)
					slices.Sort(wantR)
					for _, limit := range []int{0, 1, 2, 5} {
						var gotN []types.NodeID
						var gotR []types.RelID
						page := opts
						page.Limit = limit
						for {
							ns, err := g.Nodes().ByLabelAndProperty("Ev", "k", p, page)
							if err != nil {
								t.Fatalf("nodes %T(%v) limit %d: %v", p, p, limit, err)
							}
							for _, n := range ns {
								gotN = append(gotN, n.ID())
							}
							if limit == 0 || len(ns) < limit {
								break
							}
							page.After = types.EntityID(ns[len(ns)-1].ID())
						}
						page = opts
						page.Limit = limit
						for {
							rs, err := g.Rels().ByTypeAndProperty("E", "k", p, page)
							if err != nil {
								t.Fatalf("rels %T(%v) limit %d: %v", p, p, limit, err)
							}
							for _, r := range rs {
								gotR = append(gotR, r.ID())
							}
							if limit == 0 || len(rs) < limit {
								break
							}
							page.After = types.EntityID(rs[len(rs)-1].ID())
						}
						if !slices.Equal(gotN, wantN) {
							t.Errorf("nodes %T(%v) timed=%v limit %d: got %v, want %v", p, p, timed, limit, gotN, wantN)
						}
						if !slices.Equal(gotR, wantR) {
							t.Errorf("rels %T(%v) timed=%v limit %d: got %v, want %v", p, p, timed, limit, gotR, wantR)
						}
					}
				}
			}
		})
	}
}

// TestRound11FormsSeekMoreDoors: the one-walk seek and the several-key
// sidecar enumeration on every backend, against an oracle: an Elements index
// whose lists hold several forms of one number (each row once), more matches
// than one union batch (150 rows over two forms, whole and paged by 70), and
// the pinned doors that read the membership sidecar (NodesByLabelPropertyAt /
// RelsByTypePropertyAt at now, ByLabelAndProperty / ByTypeAndProperty at a
// TxPin), which must find every stored width.
func TestRound11FormsSeekMoreDoors(t *testing.T) {
	for _, b := range allStoreBackends() {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			ctx := context.Background()
			if err := g.Index().CreatePropertyWithOptions("Ev", "tags", storepkg.PropertyIndexOptions{Elements: true}); err != nil {
				t.Fatal(err)
			}
			for _, k := range []string{"k"} {
				if err := g.Index().CreateProperty("Ev", k); err != nil {
					t.Fatal(err)
				}
				if err := g.Index().CreateRelProperty("E", k); err != nil {
					t.Fatal(err)
				}
			}
			hub, err := g.Nodes().Add(ctx, []string{"Hub"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			var wantTags, wantK []types.NodeID
			var wantR []types.RelID
			for i := range 150 {
				props := map[string]any{}
				switch i % 3 {
				case 0:
					props["k"], props["tags"] = int64(5), []any{int64(5), 5.0, "x"}
				case 1:
					props["k"], props["tags"] = 5.0, []any{float32(5)}
				case 2:
					props["k"], props["tags"] = int64(6), []any{int64(6), "5"}
				}
				n, err := g.Nodes().Add(ctx, []string{"Ev"}, props)
				if err != nil {
					t.Fatal(err)
				}
				r, err := g.Rels().Add(ctx, "E", hub, n, map[string]any{"k": props["k"]})
				if err != nil {
					t.Fatal(err)
				}
				if i%3 != 2 {
					wantTags = append(wantTags, n.ID())
					wantK = append(wantK, n.ID())
					wantR = append(wantR, r.ID())
				}
			}
			now, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			pinned := storepkg.QueryOpts{TxPin: now, TxPinSet: true}
			nodeIDs := func(ns []*types.Node) []types.NodeID {
				out := make([]types.NodeID, 0, len(ns))
				for _, n := range ns {
					out = append(out, n.ID())
				}
				slices.Sort(out)
				return out
			}
			relIDs := func(rs []*types.Relationship) []types.RelID {
				out := make([]types.RelID, 0, len(rs))
				for _, r := range rs {
					out = append(out, r.ID())
				}
				slices.Sort(out)
				return out
			}
			paged := func(key string, limit int) []types.NodeID {
				var out []types.NodeID
				opts := storepkg.QueryOpts{Limit: limit}
				for {
					ns, err := g.Nodes().ByLabelAndProperty("Ev", key, uint8(5), opts)
					if err != nil {
						t.Fatal(err)
					}
					for _, n := range ns {
						out = append(out, n.ID())
					}
					if limit == 0 || len(ns) < limit {
						return out
					}
					opts.After = types.EntityID(ns[len(ns)-1].ID())
				}
			}
			for _, limit := range []int{0, 70} {
				if got := paged("tags", limit); !slices.Equal(got, wantTags) {
					t.Errorf("Elements tags=5 limit %d: %d rows %v, want %d", limit, len(got), got, len(wantTags))
				}
				if got := paged("k", limit); !slices.Equal(got, wantK) {
					t.Errorf("k=5 limit %d: %d rows, want %d", limit, len(got), len(wantK))
				}
			}
			ns, err := g.Temporal().NodesByLabelPropertyAt("Ev", "k", float32(5), now)
			if err != nil {
				t.Fatal(err)
			}
			if got := nodeIDs(ns); !slices.Equal(got, wantK) {
				t.Errorf("NodesByLabelPropertyAt k=5: %d rows, want %d", len(got), len(wantK))
			}
			rs, err := g.Temporal().RelsByTypePropertyAt("E", "k", float32(5), now)
			if err != nil {
				t.Fatal(err)
			}
			if got := relIDs(rs); !slices.Equal(got, wantR) {
				t.Errorf("RelsByTypePropertyAt k=5: %d rows, want %d", len(got), len(wantR))
			}
			ns, err = g.Nodes().ByLabelAndProperty("Ev", "k", uint16(5), pinned)
			if err != nil {
				t.Fatal(err)
			}
			if got := nodeIDs(ns); !slices.Equal(got, wantK) {
				t.Errorf("ByLabelAndProperty k=5 at a TxPin: %d rows, want %d", len(got), len(wantK))
			}
			rs, err = g.Rels().ByTypeAndProperty("E", "k", uint16(5), pinned)
			if err != nil {
				t.Fatal(err)
			}
			if got := relIDs(rs); !slices.Equal(got, wantR) {
				t.Errorf("ByTypeAndProperty k=5 at a TxPin: %d rows, want %d", len(got), len(wantR))
			}
		})
	}
}
