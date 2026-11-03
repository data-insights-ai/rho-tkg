package graph_test

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ELEMPROBE: IDsByLabelAndPropertyValues returns the IDs ByLabelAndPropertyValues
// returns, and on badger and tiered it reads no row for them when the index
// answers: an Elements or a scalar definition, on disk or in RAM, with the
// label index in RAM or on disk, after a reopen (no row cached). Covered:
// another label's scalar key on the same value, a node that leaves the label
// while another label keeps its key, a deleted node, an update that drops the
// value, a write still pending, an event-shard and an archived
// node on tiered, paging over the shards, and the fallbacks (a TxPin, which
// the rows' history answers; an on-disk scalar definition sharing its key
// with an on-disk Elements definition, where a list value must not match;
// two scalar definitions sharing a key answer from the index).
func TestIDsByLabelAndPropertyValuesReadsNoRow(t *testing.T) {
	t.Parallel()
	type opened = probeGraph
	for _, b := range []struct {
		name   string
		onDisk bool // the property indexes live on disk
		open   func(t *testing.T, dir string) opened
	}{
		{"badger index on disk", true, func(t *testing.T, dir string) opened {
			return openBadgerProbe(t, badger.Config{Dir: dir, PropertyIndexOnDisk: true})
		}},
		{"badger index and labels on disk", true, func(t *testing.T, dir string) opened {
			return openBadgerProbe(t, badger.Config{Dir: dir, PropertyIndexOnDisk: true, LabelIndexOnDisk: true})
		}},
		{"badger index in RAM", false, func(t *testing.T, dir string) opened {
			return openBadgerProbe(t, badger.Config{Dir: dir})
		}},
		{"tiered index on disk", true, func(t *testing.T, dir string) opened {
			return openTieredProbe(t, tiered.Config{DataDir: dir, RefLabels: []string{"Block", "Other", "Plain", "Mixed", "Solo", "Solo2"}, ShardWindow: 7 * 24 * time.Hour, PropertyIndexOnDisk: true})
		}},
		{"tiered index and labels on disk", true, func(t *testing.T, dir string) opened {
			return openTieredProbe(t, tiered.Config{DataDir: dir, RefLabels: []string{"Block", "Other", "Plain", "Mixed", "Solo", "Solo2"}, ShardWindow: 7 * 24 * time.Hour, PropertyIndexOnDisk: true, LabelIndexOnDisk: true})
		}},
	} {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dir := t.TempDir()
			o := b.open(t, dir)
			g := o.g
			elements := storepkg.PropertyIndexOptions{Elements: true}
			for _, def := range []struct {
				label, key string
				opts       storepkg.PropertyIndexOptions
			}{{"Block", "bk", elements}, {"Other", "bk", storepkg.PropertyIndexOptions{}}, {"Plain", "pk", storepkg.PropertyIndexOptions{}}, {"Mixed", "pk", elements},
				{"Solo", "sk", storepkg.PropertyIndexOptions{}}, {"Solo2", "sk", storepkg.PropertyIndexOptions{}}} {
				if err := g.Index().CreatePropertyWithOptions(def.label, def.key, def.opts); err != nil {
					t.Fatal(err)
				}
			}
			blob := bytes.Repeat([]byte{1}, 64<<10)
			add := func(labels []string, props map[string]any) types.NodeID {
				t.Helper()
				props["blob"] = blob
				n, err := g.Nodes().Add(ctx, labels, props)
				if err != nil {
					t.Fatal(err)
				}
				return n.ID()
			}
			b1 := add([]string{"Block"}, map[string]any{"bk": []int64{1, 2}})
			b2 := add([]string{"Block"}, map[string]any{"bk": []int64{2, 3}})
			b3 := add([]string{"Block"}, map[string]any{"bk": int64(3)}) // a one-element list
			b4 := add([]string{"Block"}, map[string]any{"bk": []int64{2, 4}})
			b6 := add([]string{"Block"}, map[string]any{"bk": []int64{2}})
			e1 := add([]string{"Ev", "Block"}, map[string]any{"bk": []int64{3}}) // an event shard on tiered
			s1 := add([]string{"Solo"}, map[string]any{"sk": int64(7)})
			add([]string{"Solo2"}, map[string]any{"sk": int64(7)})
			pin := types.Instant(0)
			if n, err := g.Nodes().Get(ctx, e1); err != nil {
				t.Fatal(err)
			} else {
				pin = n.Temporal().TxFrom
			}
			time.Sleep(3 * time.Millisecond)
			o1 := add([]string{"Other"}, map[string]any{"bk": int64(2)})         // the same key under another label
			m := add([]string{"Block", "Other"}, map[string]any{"bk": int64(2)}) // leaves Block below; Other keeps its key
			p1 := add([]string{"Plain"}, map[string]any{"pk": int64(5)})
			add([]string{"Plain", "Mixed"}, map[string]any{"pk": []int64{5, 6}}) // Mixed's element key 5
			if err := g.Nodes().Delete(ctx, b4); err != nil {
				t.Fatal(err)
			}
			if o.archive != nil {
				if err := o.archive(b3); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.Nodes().Update(ctx, b6, map[string]any{"bk": []int64{9}}); err != nil {
				t.Fatal(err)
			}
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			o = b.open(t, dir)
			g = o.g
			defer func() { _ = g.Close() }()
			b5 := add([]string{"Block"}, map[string]any{"bk": []int64{2}}) // pending, cached
			if err := g.Nodes().RemoveLabel(ctx, m, "Block"); err != nil { // pending
				t.Fatal(err)
			}
			// The counter counts: a row lookup of the cold rows reads them (TxPin's
			// point reads below fill the cache).
			before0 := o.decodes()
			if _, err := g.Nodes().ByLabelAndPropertyValues("Block", "bk", []any{int64(3)}, storepkg.QueryOpts{}); err != nil {
				t.Fatal(err)
			}
			if o.decodes() == before0 {
				t.Error("ByLabelAndPropertyValues read no row: the counter does not count")
			}
			now := types.Instant(time.Now().UnixMilli())

			for _, c := range []struct {
				name      string
				label, k  string
				values    []any
				opts      storepkg.QueryOpts
				want      []types.NodeID
				fromIndex bool // answered without a row read
			}{
				{"one element", "Block", "bk", []any{int64(2)}, storepkg.QueryOpts{}, []types.NodeID{b1, b2, b5}, true},
				{"two values, a scalar row", "Block", "bk", []any{int64(3), 4.0}, storepkg.QueryOpts{}, []types.NodeID{b2, b3, e1}, true},
				{"paged", "Block", "bk", []any{int64(2), int64(3)}, storepkg.QueryOpts{After: types.EntityID(b1), Limit: 2}, []types.NodeID{b2, b3}, true},
				{"two scalar definitions", "Solo", "sk", []any{int64(7)}, storepkg.QueryOpts{}, []types.NodeID{s1}, true},
				{"TxPin", "Block", "bk", []any{int64(2)}, storepkg.QueryOpts{TxPin: pin}, []types.NodeID{b1, b2, b4, b6}, false},
				{"no value", "Block", "bk", nil, storepkg.QueryOpts{}, nil, true},
				{"another label's key", "Other", "bk", []any{int64(2)}, storepkg.QueryOpts{}, []types.NodeID{o1, m}, !b.onDisk}, // scalar beside Block's Elements
				{"time coordinate", "Block", "bk", []any{int64(2)}, storepkg.QueryOpts{ValidAt: now, ValidAtSet: true}, []types.NodeID{b1, b2, b5}, false},
				{"scalar beside Elements", "Plain", "pk", []any{int64(5)}, storepkg.QueryOpts{}, []types.NodeID{p1}, !b.onDisk},
			} {
				oracle, err := g.Nodes().ByLabelAndPropertyValues(c.label, c.k, c.values, c.opts)
				if err != nil {
					t.Fatalf("%s: rows: %v", c.name, err)
				}
				before := o.decodes()
				got, err := g.Nodes().IDsByLabelAndPropertyValues(c.label, c.k, c.values, c.opts)
				if err != nil {
					t.Fatalf("%s: %v", c.name, err)
				}
				read := o.decodes() - before
				if !slices.Equal(got, c.want) || !slices.Equal(got, nodeIDsOf(oracle)) {
					t.Errorf("%s: IDs %v, rows %v, want %v", c.name, got, nodeIDsOf(oracle), c.want)
				}
				if c.fromIndex && read != 0 {
					t.Errorf("%s: %d rows read for %d IDs, want 0", c.name, read, len(got))
				}
			}
		})
	}
}

// ELEMPROBE: dropping an on-disk definition while another one indexes the same
// property key leaves the dropped one's entries in the shared keyspace (a
// lookup rechecks rows), so the ID door must not answer from it: an Elements
// definition's element keys under a surviving scalar one, and a scalar key
// left behind when the node's value changed while no definition covered it.
// The IDs equal the rows' after the drop and after a reopen.
func TestIDsByLabelAndPropertyValuesAfterADropOnASharedKey(t *testing.T) {
	t.Parallel()
	for _, b := range []struct {
		name string
		open func(t *testing.T, dir string) probeGraph
	}{
		{"badger index on disk", func(t *testing.T, dir string) probeGraph {
			return openBadgerProbe(t, badger.Config{Dir: dir, PropertyIndexOnDisk: true})
		}},
		{"tiered index on disk", func(t *testing.T, dir string) probeGraph {
			return openTieredProbe(t, tiered.Config{DataDir: dir, RefLabels: []string{"Block", "Other", "Solo", "Solo2"}, ShardWindow: 7 * 24 * time.Hour, PropertyIndexOnDisk: true})
		}},
	} {
		t.Run(b.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dir := t.TempDir()
			g := b.open(t, dir).g
			for _, def := range []struct {
				label, key string
				elements   bool
			}{{"Other", "bk", false}, {"Block", "bk", true}, {"Solo", "sk", false}, {"Solo2", "sk", false}} {
				if err := g.Index().CreatePropertyWithOptions(def.label, def.key, storepkg.PropertyIndexOptions{Elements: def.elements}); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := g.Nodes().Add(ctx, []string{"Block", "Other"}, map[string]any{"bk": []int64{1, 2}}); err != nil {
				t.Fatal(err)
			}
			sn, err := g.Nodes().Add(ctx, []string{"Solo"}, map[string]any{"sk": int64(7)})
			if err != nil {
				t.Fatal(err)
			}
			s := sn.ID()
			if err := g.Index().DeleteProperty("Block", "bk"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().DeleteProperty("Solo", "sk"); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(ctx, s, map[string]any{"sk": int64(8)}); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().AddLabel(ctx, s, "Solo2"); err != nil {
				t.Fatal(err)
			}
			check := func(when string) {
				t.Helper()
				for _, c := range []struct {
					label, key string
					v          any
				}{{"Other", "bk", int64(2)}, {"Other", "bk", []int64{1, 2}}, {"Solo2", "sk", int64(7)}, {"Solo2", "sk", int64(8)}} {
					rows, err := g.Nodes().ByLabelAndPropertyValues(c.label, c.key, []any{c.v}, storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					got, err := g.Nodes().IDsByLabelAndPropertyValues(c.label, c.key, []any{c.v}, storepkg.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					if !slices.Equal(got, nodeIDsOf(rows)) {
						t.Errorf("%s, %s %s=%v: IDs %v, rows %v", when, c.label, c.key, c.v, got, nodeIDsOf(rows))
					}
				}
			}
			check("after the drops")
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = b.open(t, dir).g
			defer func() { _ = g.Close() }()
			check("after a reopen")
		})
	}
}

// probeGraph is a graph with its row-read counter and, on tiered, the
// archive move.
type probeGraph struct {
	g       *graphpkg.Graph
	decodes func() int64
	archive func(types.NodeID) error
}

func openBadgerProbe(t *testing.T, cfg badger.Config) probeGraph {
	t.Helper()
	bs, err := badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	return probeGraph{g: g, decodes: bs.NodeRowDecodesForTest}
}

func openTieredProbe(t *testing.T, cfg tiered.Config) probeGraph {
	t.Helper()
	ts, err := tiered.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	return probeGraph{g: g, decodes: ts.RefShardForTest().NodeRowDecodesForTest, archive: ts.ArchiveNode}
}

// lyingIDStore is a store that is not one of rho's: it answers the ID lookup
// with an ID no node has.
type lyingIDStore struct{ *memory.Store }

func (lyingIDStore) NodeIDsByLabelAndPropertyKeys(uint16, string, []string, storepkg.QueryOpts) ([]types.NodeID, bool, error) {
	return []types.NodeID{999}, true, nil
}

// ELEMPROBE: the graph takes the ID lookup only from rho's own stores, whose
// index keys are exact; another store's answer could not be checked without
// the rows, so the rows are read (a label scan: the wrapper declares no
// property index capability of its own).
func TestIDsByLabelAndPropertyValuesTrustsOnlyRhoStores(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: lyingIDStore{memory.New()}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = g.Close() }()
	n, err := g.Nodes().Add(context.Background(), []string{"Block"}, map[string]any{"bk": int64(2)})
	if err != nil {
		t.Fatal(err)
	}
	got, err := g.Nodes().IDsByLabelAndPropertyValues("Block", "bk", []any{int64(2)}, storepkg.QueryOpts{})
	if err != nil || !slices.Equal(got, []types.NodeID{n.ID()}) {
		t.Fatalf("IDs %v, %v; want [%d] from the rows", got, err, n.ID())
	}
}
