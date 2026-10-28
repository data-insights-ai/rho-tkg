package core

import (
	"context"
	"fmt"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestElementsIndexPinnedReads: an Elements index (node and relationship)
// read at past transaction-time pins, whose answers come from the property
// membership sidecars on badger, answers as the memory store does while the
// list values change between the pins, an element gained after the first
// pinned read and lost again included (IDXKEYLAZY: the sidecars key each
// element of a list).
func TestElementsIndexPinnedReads(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	open := map[string]func() (*Core, error){
		"memory": func() (*Core, error) { return New(Config{Store: memory.New()}) },
		"badger": func() (*Core, error) { return New(Config{BadgerInMemory: true}) },
	}
	answers := map[string][]string{}
	for _, name := range []string{"memory", "badger"} {
		g, err := open[name]()
		if err != nil {
			t.Fatal(err)
		}
		elements := storepkg.PropertyIndexOptions{Elements: true}
		if err := g.Index.CreatePropertyWithOptions("E", "tags", elements); err != nil {
			t.Fatal(err)
		}
		if err := g.Index.CreateRelPropertyWithOptions("R", "tags", elements); err != nil {
			t.Fatal(err)
		}
		a, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"tags": []any{"x", "y"}})
		if err != nil {
			t.Fatal(err)
		}
		b, err := g.Nodes.Add(ctx, []string{"E"}, map[string]any{"tags": []any{"y"}})
		if err != nil {
			t.Fatal(err)
		}
		r, err := g.Rels.Add(ctx, "R", a, b, map[string]any{"tags": []any{"x", "z"}})
		if err != nil {
			t.Fatal(err)
		}
		names := map[int64]string{int64(a.ID()): "a", int64(b.ID()): "b", int64(r.ID()): "r"}
		read := func(pin types.Instant) string {
			var out []string
			for _, v := range []string{"x", "y", "z"} {
				ns, err := g.Nodes.ByLabelAndProperty("E", "tags", v, storepkg.QueryOpts{TxPin: pin})
				if err != nil {
					t.Fatal(err)
				}
				rs, err := g.Rels.ByTypeAndProperty("R", "tags", v, storepkg.QueryOpts{TxPin: pin})
				if err != nil {
					t.Fatal(err)
				}
				var got []string
				for _, n := range ns {
					got = append(got, names[int64(n.ID())])
				}
				for _, rel := range rs {
					got = append(got, names[int64(rel.ID())])
				}
				out = append(out, fmt.Sprintf("%s=%v", v, got))
			}
			return fmt.Sprint(out)
		}
		var pins []types.Instant
		pin := func() {
			p, err := g.Temporal.NowTx()
			if err != nil {
				t.Fatal(err)
			}
			pins = append(pins, p)
			read(p) // builds the sidecars on the first pinned read
		}
		pin()
		if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"tags": []any{"z"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"tags": []any{"y"}}); err != nil {
			t.Fatal(err)
		}
		pin()
		if _, err := g.Nodes.Update(ctx, b.ID(), map[string]any{"tags": []any{"x", "z"}}); err != nil {
			t.Fatal(err)
		}
		pin()
		// a and r lose the elements they gained after the build: only the
		// sidecars still place them under z and y at the earlier pins.
		if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"tags": []any{"q"}}); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"tags": []any{"w"}}); err != nil {
			t.Fatal(err)
		}
		pin()
		for _, p := range pins {
			answers[name] = append(answers[name], read(p))
		}
		_ = g.Close()
	}
	for i, want := range answers["memory"] {
		if got := answers["badger"][i]; got != want {
			t.Errorf("pin %d: badger %s, memory %s", i, got, want)
		}
	}
	if len(answers["memory"]) != 4 {
		t.Fatalf("%d pins", len(answers["memory"]))
	}
}
