package graph_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var elementsOpts = graphpkg.PropertyIndexOptions{Elements: true}

func elemNodeIDs(ns []*types.Node) []types.NodeID {
	out := make([]types.NodeID, 0, len(ns))
	for _, n := range ns {
		out = append(out, n.ID())
	}
	slices.Sort(out)
	return out
}

func elemRelIDs(rs []*types.Relationship) []types.RelID {
	out := make([]types.RelID, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.ID())
	}
	slices.Sort(out)
	return out
}

func elemWant(ns ...*types.Node) []types.NodeID { return elemNodeIDs(ns) }

// elementsFixture writes the sigma shape under label (d1 list with duplicate,
// d2 scalar, d3 empty list, d4 mixed list, d5 nil). pin sits after those writes
// and before d1's update (drops 101, adds 103). d6 gains 501 after a temporal
// read built the membership sidecar and loses it again: pin501 sees it only
// through the sidecar.
type elementsFixture struct {
	d1, d2, d3, d4, d5, d6 *types.Node
	pin, pin501, upd1      types.Instant
}

func writeElementsFixture(t *testing.T, g *graphpkg.Graph, label string) elementsFixture {
	t.Helper()
	ctx := context.Background()
	add := func(v any) *types.Node {
		t.Helper()
		n, err := g.Nodes().Add(ctx, []string{label}, map[string]any{"ov_support": v})
		if err != nil {
			t.Fatal(err)
		}
		return n
	}
	f := elementsFixture{
		d1: add([]int64{101, 102, 101}),
		d2: add(int64(101)),
		d3: add([]int64{}),
		d4: add([]any{int64(102), "x", nil, []any{int64(101)}}),
		d5: add(nil),
		d6: add([]int64{500}),
	}
	f.pin = f.d6.Temporal().TxFrom
	if _, err := g.Nodes().ByLabelAndProperty(label, "ov_support", int64(500), storepkg.QueryOpts{TxPin: f.pin}); err != nil {
		t.Fatal(err) // builds the sidecar before the updates below
	}
	update := func(n *types.Node, v any) types.Instant {
		t.Helper()
		time.Sleep(3 * time.Millisecond)
		u, err := g.Nodes().Update(ctx, n.ID(), map[string]any{"ov_support": v})
		if err != nil {
			t.Fatal(err)
		}
		return u.Temporal().TxFrom
	}
	f.upd1 = update(f.d1, []int64{102, 103})
	f.pin501 = update(f.d6, []int64{500, 501})
	update(f.d6, []int64{500})
	return f
}

func checkElementsFixture(t *testing.T, g *graphpkg.Graph, label string, f elementsFixture) {
	t.Helper()
	now := types.Instant(time.Now().UnixMilli())
	for _, c := range []struct {
		name  string
		value any
		opts  storepkg.QueryOpts
		want  []types.NodeID
	}{
		{"current 101", int64(101), storepkg.QueryOpts{}, elemWant(f.d2)},
		{"current 102", int64(102), storepkg.QueryOpts{}, elemWant(f.d1, f.d4)},
		{"current 103", int64(103), storepkg.QueryOpts{}, elemWant(f.d1)},
		{"current x", "x", storepkg.QueryOpts{}, elemWant(f.d4)},
		{"current 999", int64(999), storepkg.QueryOpts{}, nil},
		{"current int 101 equals int64 101", 101, storepkg.QueryOpts{}, elemWant(f.d2)},
		{"current list value", []int64{102, 103}, storepkg.QueryOpts{}, nil},
		{"TxPin 101", int64(101), storepkg.QueryOpts{TxPin: f.pin}, elemWant(f.d1, f.d2)},
		{"TxPin 103", int64(103), storepkg.QueryOpts{TxPin: f.pin}, nil},
		{"TxPin 102", int64(102), storepkg.QueryOpts{TxPin: f.pin}, elemWant(f.d1, f.d4)},
		{"TxAt+ValidAt 101", int64(101), storepkg.QueryOpts{TxAt: f.pin, ValidAt: now}, elemWant(f.d1, f.d2)},
		{"TxAt+ValidAt 103", int64(103), storepkg.QueryOpts{TxAt: f.pin, ValidAt: now}, nil},
		{"TxAt+interval 101", int64(101), storepkg.QueryOpts{TxAt: f.pin, ValidStart: 1, ValidEnd: now + 3_600_000}, elemWant(f.d1, f.d2)},
		{"ValidAt update 101", int64(101), storepkg.QueryOpts{ValidAt: f.upd1}, elemWant(f.d2)},
		{"ValidAt before update 101", int64(101), storepkg.QueryOpts{ValidAt: f.upd1 - 1}, elemWant(f.d1, f.d2)},
		{"ValidAt update 103", int64(103), storepkg.QueryOpts{ValidAt: f.upd1}, elemWant(f.d1)},
		{"current 501", int64(501), storepkg.QueryOpts{}, nil},
		{"TxPin 501 (sidecar record)", int64(501), storepkg.QueryOpts{TxPin: f.pin501}, elemWant(f.d6)},
		{"TxPin 501 before it", int64(501), storepkg.QueryOpts{TxPin: f.pin}, nil},
		{"TxAt+interval 103", int64(103), storepkg.QueryOpts{TxAt: f.pin, ValidStart: 1, ValidEnd: now + 3_600_000}, nil},
	} {
		got, err := g.Nodes().ByLabelAndProperty(label, "ov_support", c.value, c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if ids := elemNodeIDs(got); !slices.Equal(ids, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, ids, c.want)
		}
	}
	if o, ok, err := g.Index().PropertyOptions(label, "ov_support"); err != nil || !ok || !o.Elements {
		t.Errorf("PropertyOptions = %+v, %v, %v; want Elements", o, ok, err)
	}
}

// Backlog 63 (sigma R6): an Elements index makes ByLabelAndProperty mean "the
// list contains the value" at current, TxPin, TxAt+ValidAt and interval reads.
// Pins duplicates once, empty/nil nothing, scalar as one-element list, mixed
// lists (exact type, no nesting), rel parity, refused combinations, reopen, on-disk.
func TestPropertyIndexElementsContains(t *testing.T) {
	if testing.Short() && !isRaceEnabled() {
		t.Skip("long tier: release gate")
	}
	ctx := context.Background()
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		label := "Derived"
		if b.tiered {
			label = "Ref"
		}
		if err := g.Index().CreatePropertyWithOptions(label, "ov_support", elementsOpts); err != nil {
			t.Fatal(err)
		}
		f := writeElementsFixture(t, g, label)
		checkElementsFixture(t, g, label, f)

		// The refused combinations.
		if err := g.Index().CreatePropertyWithOptions(label, "other", graphpkg.PropertyIndexOptions{Elements: true, RangeCounts: true}); !errors.Is(err, graphpkg.ErrElementsIndexUnsupported) {
			t.Errorf("Elements+RangeCounts: %v, want ErrElementsIndexUnsupported", err)
		}
		if err := g.Constraints().CreateUnique(ctx, label, "ov_support"); !errors.Is(err, graphpkg.ErrElementsIndexUnsupported) {
			t.Errorf("CreateUnique on an Elements index: %v, want ErrElementsIndexUnsupported", err)
		}
		if _, _, err := g.Nodes().GetOrCreateByKey(ctx, label, "ov_support", int64(101), nil); !errors.Is(err, graphpkg.ErrElementsIndexUnsupported) {
			t.Errorf("GetOrCreateByKey on an Elements index: %v, want ErrElementsIndexUnsupported", err)
		}
		if _, err := g.Nodes().ByLabelAndProperties(label, map[string]any{"ov_support": int64(101), "k": "v"}, storepkg.QueryOpts{}); !errors.Is(err, graphpkg.ErrElementsIndexUnsupported) {
			t.Errorf("ByLabelAndProperties over an Elements key: %v, want ErrElementsIndexUnsupported", err)
		}
		if err := g.Index().CreateComposite(label, []string{"k", "ov_support"}); !errors.Is(err, graphpkg.ErrElementsIndexUnsupported) {
			t.Errorf("composite over an Elements key: %v, want ErrElementsIndexUnsupported", err)
		}
		if err := g.Constraints().CreateUnique(ctx, label, "u"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().DeleteProperty(label, "u"); err != nil {
			t.Fatal(err)
		}
		if err := g.Index().CreatePropertyWithOptions(label, "u", elementsOpts); !errors.Is(err, graphpkg.ErrElementsIndexUnsupported) {
			t.Errorf("Elements index on a unique key: %v, want ErrElementsIndexUnsupported", err)
		}
		// Range doors decline: the index keeps no ordered view.
		err := g.Nodes().ForEachByLabelPropertyRange(label, "ov_support", 100, 200, true, true, storepkg.QueryOpts{}, func(*types.Node) bool { return true })
		if !errors.Is(err, graphpkg.ErrIndexNotFound) && !errors.Is(err, graphpkg.ErrCapabilityNotSupported) {
			t.Errorf("range scan over an Elements index: %v, want the decline", err)
		}

		// Relationship parity.
		// (tiered since v4.58.0: built on every shard, same answers.)
		err = g.Index().CreateRelPropertyWithOptions("SUPPORTS", "ov", elementsOpts)
		if err != nil {
			t.Fatal(err)
		}
		r1, err := g.Rels().Add(ctx, "SUPPORTS", f.d2, f.d3, map[string]any{"ov": []string{"a", "b", "a"}})
		if err != nil {
			t.Fatal(err)
		}
		r2, err := g.Rels().Add(ctx, "SUPPORTS", f.d3, f.d4, map[string]any{"ov": "a"})
		if err != nil {
			t.Fatal(err)
		}
		rpin := r2.Temporal().TxFrom
		time.Sleep(3 * time.Millisecond)
		if _, err := g.Rels().Update(ctx, r1.ID(), map[string]any{"ov": []string{"b"}}); err != nil {
			t.Fatal(err)
		}
		for _, c := range []struct {
			name  string
			value any
			opts  storepkg.QueryOpts
			want  []types.RelID
		}{
			{"rel current a", "a", storepkg.QueryOpts{}, []types.RelID{r2.ID()}},
			{"rel current b", "b", storepkg.QueryOpts{}, []types.RelID{r1.ID()}},
			{"rel TxPin a", "a", storepkg.QueryOpts{TxPin: rpin}, elemRelIDs([]*types.Relationship{r1, r2})},
		} {
			got, err := g.Rels().ByTypeAndProperty("SUPPORTS", "ov", c.value, c.opts)
			if err != nil {
				t.Fatalf("%s: %v", c.name, err)
			}
			if ids := elemRelIDs(got); !slices.Equal(ids, c.want) {
				t.Errorf("%s: got %v, want %v", c.name, ids, c.want)
			}
		}
		if o, ok, err := g.Index().RelPropertyOptions("SUPPORTS", "ov"); err != nil || !ok || !o.Elements {
			t.Errorf("RelPropertyOptions = %+v, %v, %v; want Elements", o, ok, err)
		}
	})

	reopen := []struct {
		name  string
		label string
		open  func(t *testing.T, dir string) *graphpkg.Graph
	}{
		{"badger", "Derived", func(t *testing.T, dir string) *graphpkg.Graph {
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
		{"tiered", "Ref", func(t *testing.T, dir string) *graphpkg.Graph {
			ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, Store: ts})
			if err != nil {
				t.Fatal(err)
			}
			return g
		}},
	}
	for _, b := range reopen {
		t.Run("reopen/"+b.name, func(t *testing.T) {
			dir := t.TempDir()
			g := b.open(t, dir)
			if err := g.Index().CreatePropertyWithOptions(b.label, "ov_support", elementsOpts); err != nil {
				t.Fatal(err)
			}
			f := writeElementsFixture(t, g, b.label)
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			g = b.open(t, dir)
			defer g.Close()
			checkElementsFixture(t, g, b.label, f)
			// A write after the reopen is indexed per element too.
			d6, err := g.Nodes().Add(ctx, []string{b.label}, map[string]any{"ov_support": []int64{777}})
			if err != nil {
				t.Fatal(err)
			}
			got, err := g.Nodes().ByLabelAndProperty(b.label, "ov_support", int64(777), storepkg.QueryOpts{})
			if err != nil || !slices.Equal(elemNodeIDs(got), elemWant(d6)) {
				t.Errorf("after reopen, 777: %v, %v; want [%d]", elemNodeIDs(got), err, d6.ID())
			}
		})
	}

	t.Run("on-disk refused", func(t *testing.T) {
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1, BadgerDir: t.TempDir(), PropertyIndexOnDisk: true})
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		if err := g.Index().CreatePropertyWithOptions("Derived", "ov_support", elementsOpts); !errors.Is(err, graphpkg.ErrCapabilityNotSupported) {
			t.Fatalf("Elements on PropertyIndexOnDisk: %v, want ErrCapabilityNotSupported", err)
		}
		if has, _ := g.Index().HasProperty("Derived", "ov_support"); has {
			t.Fatal("an index was left behind")
		}
	})
}

// Tiered keeps an Elements index on the reference shard only; event-shard and
// archived Ref nodes answer with the same "contains" match, current and pinned.
func TestPropertyIndexElementsTieredEventShardAndArchive(t *testing.T) {
	ctx := context.Background()
	ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	g, err := graphpkg.New(graphpkg.Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	if err := g.Index().CreatePropertyWithOptions("Ref", "ov", elementsOpts); err != nil {
		t.Fatal(err)
	}
	ev, err := g.Nodes().Add(ctx, []string{"Ev", "Ref"}, map[string]any{"ov": []int64{101, 102}})
	if err != nil {
		t.Fatal(err)
	}
	ar, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"ov": []int64{101, 103}})
	if err != nil {
		t.Fatal(err)
	}
	ref, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"ov": int64(101)})
	if err != nil {
		t.Fatal(err)
	}
	pin := ref.Temporal().TxFrom
	time.Sleep(3 * time.Millisecond)
	if err := ts.ArchiveNode(ar.ID()); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name  string
		value any
		opts  storepkg.QueryOpts
		want  []types.NodeID
	}{
		{"current 101", int64(101), storepkg.QueryOpts{}, elemWant(ev, ar, ref)},
		{"current 102 (event shard)", int64(102), storepkg.QueryOpts{}, elemWant(ev)},
		{"current 103 (archive)", int64(103), storepkg.QueryOpts{}, elemWant(ar)},
		{"hot depth 103", int64(103), storepkg.QueryOpts{Depth: storepkg.DepthHot}, nil},
		{"TxPin 102", int64(102), storepkg.QueryOpts{TxPin: pin}, elemWant(ev)},
		{"TxPin 103", int64(103), storepkg.QueryOpts{TxPin: pin}, elemWant(ar)},
		{"TxPin 101", int64(101), storepkg.QueryOpts{TxPin: pin}, elemWant(ev, ar, ref)},
	} {
		got, err := g.Nodes().ByLabelAndProperty("Ref", "ov", c.value, c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if ids := elemNodeIDs(got); !slices.Equal(ids, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, ids, c.want)
		}
	}
}
