package core

// WP-31 (sigma row 59): relationship property indexes and event-label node
// property indexes on the tiered store answer exactly as on every other
// backend. One scenario through the graph doors on memory, badger, sharded and
// a disk tiered store whose rows span the reference shard, a warm event shard
// that is then demoted cold and closed, and a later hot shard: literal expected
// sets (so all backends cannot be wrong together) for the current lookup, the
// TxPin read, TxAt + valid window, numeric widths (int 1 == float 1.0;
// 2^53+1 != float 2^53), nil and NaN. On tiered a keyed TxPin read must take
// its candidates from the indexes and sidecars: no type or label scan.

import (
	"context"
	"math"
	"slices"
	"sort"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type propParityWorld struct {
	c     *Core
	ts    *tiered.Store // nil unless tiered
	rels  map[string]types.RelID
	nodes map[string]*types.Node
	names map[int64]string
	pin   types.Instant
}

func (w *propParityWorld) node(t *testing.T, name, label string, sev any) {
	t.Helper()
	props := map[string]any{}
	if sev != nil {
		props["sev"] = sev
	}
	n, err := w.c.Nodes.Add(context.Background(), []string{label}, props)
	if err != nil {
		t.Fatalf("add node %s: %v", name, err)
	}
	w.nodes[name] = n
	w.names[int64(n.ID().SnowflakeID())] = name
	w.pin = max(w.pin, n.Temporal().TxFrom)
}

func (w *propParityWorld) rel(t *testing.T, name, from, to string, v any, validFrom, validTo types.Instant) {
	t.Helper()
	props := map[string]any{"w": v}
	if validFrom != 0 {
		props[types.ShadowValidFrom] = validFrom
	}
	if validTo != 0 {
		props[types.ShadowValidTo] = validTo
	}
	r, err := w.c.Rels.Add(context.Background(), "HOP", w.nodes[from], w.nodes[to], props)
	if err != nil {
		t.Fatalf("add rel %s: %v", name, err)
	}
	w.rels[name] = r.ID()
	w.names[int64(r.ID().SnowflakeID())] = name
	w.pin = max(w.pin, r.Temporal().TxFrom)
}

func (w *propParityWorld) relQuery(t *testing.T, v any, opts storepkg.QueryOpts) []string {
	t.Helper()
	rs, err := w.c.Rels.ByTypeAndProperty("HOP", "w", v, opts)
	if err != nil {
		t.Fatalf("ByTypeAndProperty(%v, %+v): %v", v, opts, err)
	}
	out := []string{}
	for _, r := range rs {
		out = append(out, w.names[int64(r.ID().SnowflakeID())])
	}
	sort.Strings(out)
	return out
}

func (w *propParityWorld) nodeQuery(t *testing.T, v any, opts storepkg.QueryOpts) []string {
	t.Helper()
	ns, err := w.c.Nodes.ByLabelAndProperty("Signal", "sev", v, opts)
	if err != nil {
		t.Fatalf("ByLabelAndProperty(%v, %+v): %v", v, opts, err)
	}
	out := []string{}
	for _, n := range ns {
		out = append(out, w.names[int64(n.ID().SnowflakeID())])
	}
	sort.Strings(out)
	return out
}

func newPropParityBackend(t *testing.T, name string) (*Core, *tiered.Store) {
	t.Helper()
	var s storepkg.Store
	var ts *tiered.Store
	switch name {
	case "memory":
		s = memory.New()
	case "badger":
		bs, err := badger.New(badger.Config{InMemory: true})
		if err != nil {
			t.Fatal(err)
		}
		s = bs
	case "sharded":
		ss, err := sharded.New(sharded.Config{InMemory: true, SlotCount: 2})
		if err != nil {
			t.Fatal(err)
		}
		s = ss
	case "tiered":
		var err error
		ts, err = tiered.New(tiered.Config{
			DataDir:           t.TempDir(),
			RefLabels:         []string{"Case"},
			ShardWindow:       7 * 24 * time.Hour,
			FlushInterval:     1<<63 - 1,
			MaxOpenColdShards: 4, // a cold shard a read reopens stays open
		})
		if err != nil {
			t.Fatal(err)
		}
		s = ts
	}
	c, err := New(Config{Store: s})
	if err != nil {
		t.Fatalf("New(%s): %v", name, err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, ts
}

func TestTieredPropertyIndex_ParityAcrossBackends(t *testing.T) {
	big := int64(1<<53 + 1)
	type q struct {
		name  string
		rel   bool
		value any
		opts  func(w *propParityWorld) storepkg.QueryOpts
		want  []string
		keyed bool // a keyed TxPin read: no type / label scan on tiered
	}
	cur := func(*propParityWorld) storepkg.QueryOpts { return storepkg.QueryOpts{} }
	pinned := func(w *propParityWorld) storepkg.QueryOpts { return storepkg.QueryOpts{TxPin: w.pin} }
	window := func(w *propParityWorld) storepkg.QueryOpts {
		return storepkg.QueryOpts{TxAt: w.pin, ValidStart: 2000, ValidEnd: 2500}
	}
	queries := []q{
		{"rel current 1", true, int64(1), cur, []string{"early", "late", "refR"}, false},
		{"rel current 1.0", true, float64(1), cur, []string{"early", "late", "refR"}, false},
		{"rel current int 1", true, 1, cur, []string{"early", "late", "refR"}, false},
		{"rel current 5", true, int64(5), cur, []string{"a"}, false},
		{"rel current 2^53+1", true, big, cur, []string{"big"}, false},
		{"rel current float 2^53", true, float64(1 << 53), cur, []string{"bigF"}, false},
		{"rel current nil", true, nil, cur, []string{}, false},
		{"rel current NaN", true, math.NaN(), cur, []string{}, false},
		{"rel TxPin 1", true, int64(1), pinned, []string{"a", "b", "early", "refR"}, true},
		{"rel TxPin 1.0", true, float64(1), pinned, []string{"a", "b", "early", "refR"}, true},
		{"rel TxPin 5", true, int64(5), pinned, []string{}, true},
		{"rel TxPin 2^53+1", true, big, pinned, []string{"big"}, true},
		{"rel TxAt + valid window 1", true, int64(1), window, []string{"a", "b"}, true},
		{"node current 1", false, int64(1), cur, []string{"s2", "s4"}, false},
		{"node TxPin 1", false, int64(1), pinned, []string{"s1", "s2"}, true},
		{"node TxPin 1.0", false, float64(1), pinned, []string{"s1", "s2"}, true},
		{"node TxPin 7", false, int64(7), pinned, []string{}, true},
		{"node current 7", false, int64(7), cur, []string{"s1"}, false},
	}
	for _, backend := range []string{"memory", "badger", "sharded", "tiered"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			c, ts := newPropParityBackend(t, backend)
			w := &propParityWorld{c: c, ts: ts, rels: map[string]types.RelID{}, nodes: map[string]*types.Node{}, names: map[int64]string{}}
			var first string
			if ts != nil {
				ts.MuForTest().RLock()
				first = ts.HotShardForTest().Name()
				ts.MuForTest().RUnlock()
			}
			// Phase 1: reference shard (Case) and the first event shard (Signal).
			w.node(t, "c1", "Case", nil)
			w.node(t, "c2", "Case", nil)
			w.node(t, "s1", "Signal", int64(1))
			w.node(t, "s2", "Signal", float64(1))
			w.node(t, "s3", "Signal", int64(2))
			w.rel(t, "refR", "c1", "c2", int64(1), 5000, 0)
			w.rel(t, "a", "s1", "s2", int64(1), 1000, 0)
			w.rel(t, "b", "s1", "s2", float64(1), 1000, 0)
			w.rel(t, "early", "s2", "s3", int64(1), 100, 200)
			w.rel(t, "big", "s2", "s3", big, 0, 0)
			w.rel(t, "bigF", "s2", "s3", float64(1<<53), 0, 0)
			if err := c.Index.CreateRelProperty("HOP", "w"); err != nil {
				t.Fatalf("CreateRelProperty: %v", err)
			}
			if err := c.Index.CreateProperty("Signal", "sev"); err != nil {
				t.Fatalf("CreateProperty(event label): %v", err)
			}
			// After the pin: a moves 1 -> 5, b is deleted, s1 moves 1 -> 7.
			if _, err := c.Rels.Update(ctx, w.rels["a"], map[string]any{"w": int64(5)}); err != nil {
				t.Fatal(err)
			}
			if err := c.Rels.Delete(ctx, w.rels["b"]); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Nodes.Update(ctx, w.nodes["s1"].ID(), map[string]any{"sev": int64(7)}); err != nil {
				t.Fatal(err)
			}
			if ts != nil {
				// hot -> warm, then warm -> cold with the shard closed.
				if err := ts.ForceRotate(); err != nil {
					t.Fatal(err)
				}
				time.Sleep(2 * time.Millisecond)
			}
			pin := w.pin
			w.node(t, "s4", "Signal", int64(1))
			w.rel(t, "late", "s4", "s3", int64(1), 0, 0)
			w.pin = pin
			if ts != nil {
				demoteToCold(ts, first)
				closeEventShardStore(t, ts.EventShardsForTest()[first])
			}
			var scans int
			if ts != nil {
				ts.SetTypeScanHookForTest(func(string) { scans++ })
			}
			for _, qq := range queries {
				scans = 0
				var got []string
				if qq.rel {
					got = w.relQuery(t, qq.value, qq.opts(w))
				} else {
					got = w.nodeQuery(t, qq.value, qq.opts(w))
				}
				if !slices.Equal(got, qq.want) {
					t.Errorf("%s = %v, want %v", qq.name, got, qq.want)
				}
				if ts != nil && qq.keyed && scans != 0 {
					t.Errorf("%s: %d type/label scans on tiered, want 0 (keyed TxPin read)", qq.name, scans)
				}
			}
			if ts == nil {
				return
			}
			// The hook sees the fallback: a shard lacking its copy (an
			// interrupted drop) makes the sidecar decline, the history fold
			// scans, and the answer stays the same.
			ts.MuForTest().RLock()
			hot := ts.HotShardForTest().Store()
			ts.MuForTest().RUnlock()
			tok, ok := c.relTypes.Lookup("HOP")
			if !ok {
				t.Fatal("HOP not registered")
			}
			if err := hot.DropRelPropertyIndex(tok, "w"); err != nil {
				t.Fatal(err)
			}
			scans = 0
			if got, want := w.relQuery(t, int64(1), pinned(w)), []string{"a", "b", "early", "refR"}; !slices.Equal(got, want) {
				t.Errorf("rel TxPin 1 with a shard lacking the index = %v, want %v", got, want)
			}
			if scans == 0 {
				t.Error("rel TxPin 1 with a shard lacking the index: 0 scans, want the history fold's scans (the hook must see the fallback)")
			}
		})
	}
}
