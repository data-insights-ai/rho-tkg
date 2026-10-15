package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// sigma R10 (backlog 68): a node whose versions keep superseding ONE validity
// (sigma's Derived writer: each cut writes SetNodeVersionInterval over
// [1000, 2000) with a new value). The read sigma pays every cut is the
// head-pin timeline; budget 1 ms per 10 facts at 300 versions.

type r10Backend struct {
	name string
	cfg  func() Config
}

func r10Backends() []r10Backend {
	return []r10Backend{
		{"memory", func() Config { return Config{} }},
		{"badger", func() Config { return Config{BadgerInMemory: true} }},
	}
}

func r10Props(i int) map[string]any {
	return map[string]any{"ov_namespace": "b", "ov_rel": "r", "ov_key": `["k"]`, "ov_program": "p",
		"ov_session": "s", "ov_derived_at": int64(i), "ov_c1": int64(i)}
}

// r10Node builds one node with versions rows of validity [1000, 2000).
func r10Node(tb testing.TB, g *Core, versions int) types.NodeID {
	tb.Helper()
	ctx := context.Background()
	p := r10Props(0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	n, err := g.Nodes.Add(ctx, []string{"Derived"}, p)
	if err != nil {
		tb.Fatalf("Add: %v", err)
	}
	for i := 1; i < versions; i++ {
		if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 1000, 2000, r10Props(i)); err != nil {
			tb.Fatalf("SetNodeVersionInterval: %v", err)
		}
	}
	return n.ID()
}

func BenchmarkR10NodeEffectiveTimelineHead(b *testing.B) {
	for _, be := range r10Backends() {
		for _, versions := range []int{1, 100, 300} {
			b.Run(fmt.Sprintf("%s/versions=%d", be.name, versions), func(b *testing.B) {
				g, err := New(be.cfg())
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = g.Close() })
				id := r10Node(b, g, versions)
				pin := g.peekNow()
				segs, err := g.Temporal.NodeEffectiveTimeline(id, pin)
				if err != nil || len(segs) != 1 {
					b.Fatalf("timeline: %v %d segments", err, len(segs))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := g.Temporal.NodeEffectiveTimeline(id, pin); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// r10Rel builds one relationship with versions rows of validity [1000, 2000).
func r10Rel(tb testing.TB, g *Core, versions int) types.RelID {
	tb.Helper()
	ctx := context.Background()
	s, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		tb.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		tb.Fatal(err)
	}
	p := r10Props(0)
	p["tkg_valid_from"], p["tkg_valid_to"] = int64(1000), int64(2000)
	r, err := g.Rels.AddByID(ctx, "LINK", s.ID(), e.ID(), p)
	if err != nil {
		tb.Fatalf("AddByID: %v", err)
	}
	for i := 1; i < versions; i++ {
		if _, err := g.Temporal.SetRelVersionInterval(ctx, r.ID(), 1000, 2000, r10Props(i)); err != nil {
			tb.Fatalf("SetRelVersionInterval: %v", err)
		}
	}
	return r.ID()
}

func BenchmarkR10RelEffectiveTimelineHead(b *testing.B) {
	for _, be := range r10Backends() {
		for _, versions := range []int{1, 100, 300} {
			b.Run(fmt.Sprintf("%s/versions=%d", be.name, versions), func(b *testing.B) {
				g, err := New(be.cfg())
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = g.Close() })
				id := r10Rel(b, g, versions)
				pin := g.peekNow()
				segs, err := g.Temporal.RelEffectiveTimeline(id, pin)
				if err != nil || len(segs) != 1 {
					b.Fatalf("timeline: %v %d segments", err, len(segs))
				}
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					if _, err := g.Temporal.RelEffectiveTimeline(id, pin); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkR10ReadThenWrite: sigma's Derived writer per fact and cut — read
// the head-pin timeline, then append one more version of the same validity
// in a GraphTx and commit. The node starts at N versions and gains b.N.
func BenchmarkR10ReadThenWrite(b *testing.B) {
	for _, be := range r10Backends() {
		for _, versions := range []int{1, 100, 300} {
			b.Run(fmt.Sprintf("%s/versions=%d", be.name, versions), func(b *testing.B) {
				g, err := New(be.cfg())
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = g.Close() })
				id := r10Node(b, g, versions)
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if _, err := g.Temporal.NodeEffectiveTimeline(id, g.peekNow()); err != nil {
						b.Fatal(err)
					}
					tx, err := g.BeginTx()
					if err != nil {
						b.Fatal(err)
					}
					if _, err := tx.SetNodeVersionInterval(id, 1000, 2000, map[string]any{"ov_c1": int64(-i)}); err != nil {
						b.Fatal(err)
					}
					if err := tx.Commit(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

// BenchmarkR10SetNodeVersionInterval: the write half of sigma's cut (one
// more version of the same validity), for context.
func BenchmarkR10SetNodeVersionInterval(b *testing.B) {
	for _, be := range r10Backends() {
		for _, versions := range []int{1, 100, 300} {
			b.Run(fmt.Sprintf("%s/versions=%d", be.name, versions), func(b *testing.B) {
				g, err := New(be.cfg())
				if err != nil {
					b.Fatal(err)
				}
				b.Cleanup(func() { _ = g.Close() })
				id := r10Node(b, g, versions)
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := range b.N {
					if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 1000, 2000, map[string]any{"ov_c1": int64(-i)}); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
