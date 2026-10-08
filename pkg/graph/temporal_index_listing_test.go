package graph_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// The listings name exactly the labels and relationship types that carry a
// temporal interval index, sorted, on every backend: a high-frequency index
// is not listed, a dropped index leaves the list, a type without one is not
// listed, HasRelTemporal agrees. Tiered lists no relationship types (it has
// none); the lists survive a badger reopen.
func TestTemporalIndexListing(t *testing.T) {
	forAllStoreBackends(t, func(t *testing.T, b storeBackend, g *graphpkg.Graph) {
		ctx := context.Background()
		a, _ := g.Nodes().Add(ctx, []string{"Zeta"}, nil)
		c, _ := g.Nodes().Add(ctx, []string{"Alpha"}, nil)
		if _, err := g.Nodes().Add(ctx, []string{"Plain", "Busy"}, nil); err != nil {
			t.Fatal(err)
		}
		for _, l := range []string{"Zeta", "Alpha"} {
			if err := g.Index().CreateTemporal(l); err != nil {
				t.Fatal(err)
			}
		}
		if err := g.Index().CreateHighFrequency("Busy", time.Hour); err != nil && !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			t.Fatal(err)
		}
		if _, err := g.Rels().Add(ctx, "T", a, c, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := g.Rels().Add(ctx, "U", a, c, nil); err != nil {
			t.Fatal(err)
		}
		relIndexes := true
		if err := g.Index().CreateRelTemporal("T"); errors.Is(err, storepkg.ErrCapabilityNotSupported) {
			relIndexes = false
		} else if err != nil {
			t.Fatal(err)
		}
		expect := func(step string, labels, relTypes []string) {
			t.Helper()
			gotL, err := g.Index().ListTemporal()
			if err != nil || !slices.Equal(gotL, labels) {
				t.Fatalf("%s: ListTemporal = %v, %v; want %v", step, gotL, err, labels)
			}
			gotT, err := g.Index().ListRelTemporal()
			if err != nil || !slices.Equal(gotT, relTypes) {
				t.Fatalf("%s: ListRelTemporal = %v, %v; want %v", step, gotT, err, relTypes)
			}
			for _, typ := range []string{"T", "U", "Never"} {
				has, err := g.Index().HasRelTemporal(typ)
				if err != nil || has != slices.Contains(relTypes, typ) {
					t.Fatalf("%s: HasRelTemporal(%s) = %v, %v", step, typ, has, err)
				}
			}
		}
		withT := []string{"T"}
		if !relIndexes {
			withT = nil
		}
		expect("created", []string{"Alpha", "Zeta"}, withT)
		if err := g.Index().DeleteTemporal("Zeta"); err != nil {
			t.Fatal(err)
		}
		expect("a label dropped", []string{"Alpha"}, withT)
		if relIndexes {
			if err := g.Index().DeleteRelTemporal("T"); err != nil {
				t.Fatal(err)
			}
		}
		expect("the type dropped", []string{"Alpha"}, nil)
		if _, err := g.Index().HasRelTemporal(""); err == nil {
			t.Fatal("an empty type passed")
		}
	})
}

// A badger directory keeps its label and relationship-type temporal index
// definitions across a reopen, and the listings say so.
func TestTemporalIndexListingSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() *graphpkg.Graph {
		g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0, BadgerDir: dir})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	g := open()
	ctx := context.Background()
	a, _ := g.Nodes().Add(ctx, []string{"A"}, nil)
	b, _ := g.Nodes().Add(ctx, []string{"B"}, nil)
	if _, err := g.Rels().Add(ctx, "T", a, b, nil); err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateTemporal("B"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateRelTemporal("T"); err != nil {
		t.Fatal(err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	g = open()
	defer g.Close()
	labels, err := g.Index().ListTemporal()
	if err != nil {
		t.Fatal(err)
	}
	types, err := g.Index().ListRelTemporal()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(labels, types) != "[B] [T]" {
		t.Fatalf("after reopen: %v %v", labels, types)
	}
}

// BenchmarkTemporalIndexListing: finding the temporal indexes among 1,000
// labels, by the listing against probing every label with HasTemporal
// (sigma-tkgd C3r: SHOW INDEXES would need a listing).
func BenchmarkTemporalIndexListing(b *testing.B) {
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer g.Close()
	ctx := context.Background()
	labels := make([]string, 1000)
	for i := range labels {
		labels[i] = fmt.Sprintf("L%04d", i)
		if _, err := g.Nodes().Add(ctx, []string{labels[i]}, nil); err != nil {
			b.Fatal(err)
		}
		if i%100 == 0 {
			if err := g.Index().CreateTemporal(labels[i]); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.Run("ListTemporal", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if got, err := g.Index().ListTemporal(); err != nil || len(got) != 10 {
				b.Fatal(len(got), err)
			}
		}
	})
	b.Run("HasTemporal-each", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			n := 0
			for _, l := range labels {
				if has, err := g.Index().HasTemporal(l); err != nil {
					b.Fatal(err)
				} else if has {
					n++
				}
			}
			if n != 10 {
				b.Fatal(n)
			}
		}
	})
}
