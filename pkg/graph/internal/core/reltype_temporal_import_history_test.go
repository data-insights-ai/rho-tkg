package core

// Backlog 17 / item-10 follow-up: a relationship version written through the
// store's PutRelVersion door (g.IO().Import replays every history version that
// way) must stay findable through the relationship-type temporal index. The
// envelope index prunes a candidate whose envelope cannot overlap the query; if
// PutRelVersion leaves the envelope at the current version's interval, the
// imported past version disappears from valid-time queries on a store that
// carries the index, while the same store without it still answers.

import (
	"bytes"
	"context"
	"slices"
	"testing"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestRelTemporalIndex_ImportedHistoryVersionStaysFindable(t *testing.T) {
	ctx := context.Background()
	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = src.Close() })
	a, err := src.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	b, err := src.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	moved, err := src.Rels.Add(ctx, "HOP", a, b, map[string]any{"name": "moved", types.ShadowValidFrom: types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src.Rels.Add(ctx, "HOP", a, b, map[string]any{"name": "late", types.ShadowValidFrom: types.Instant(6000)}); err != nil {
		t.Fatal(err)
	}
	// Two-phase: the row held [1000, ...) and then moved to [5000, ...).
	if _, err := src.Temporal.SetRelVersionInterval(ctx, moved.ID(), 5000, 0, nil); err != nil {
		t.Fatal(err)
	}
	var dump bytes.Buffer
	if err := src.IO.Export(&dump); err != nil {
		t.Fatalf("Export: %v", err)
	}
	names := func(rels []*types.Relationship) []string {
		out := make([]string, 0, len(rels))
		for _, r := range rels {
			name, _ := r.GetProperty("name")
			s, _ := name.(string)
			out = append(out, s)
		}
		slices.Sort(out)
		return out
	}
	want, err := src.Temporal.RelsByTypeAt("HOP", 1500)
	if err != nil {
		t.Fatal(err)
	}
	if got := names(want); !slices.Equal(got, []string{"moved"}) {
		t.Fatalf("oracle: source RelsByTypeAt(1500) = %v, want [moved]", got)
	}

	for _, backend := range []struct {
		name string
		open func(t *testing.T) storepkg.Store
	}{
		{"memory", func(*testing.T) storepkg.Store { return memory.New() }},
		{"badger", func(t *testing.T) storepkg.Store {
			bs, err := badger.New(badger.Config{InMemory: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs
		}},
	} {
		t.Run(backend.name, func(t *testing.T) {
			dst, err := New(Config{Store: backend.open(t)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = dst.Close() })
			if err := dst.Index.CreateRelTemporal("HOP"); err != nil {
				t.Fatalf("CreateRelTemporal: %v", err)
			}
			if err := dst.IO.Import(bytes.NewReader(dump.Bytes()), tkgio.ImportOptions{}); err != nil {
				t.Fatalf("Import: %v", err)
			}
			at, err := dst.Temporal.RelsByTypeAt("HOP", 1500)
			if err != nil {
				t.Fatal(err)
			}
			if got := names(at); !slices.Equal(got, []string{"moved"}) {
				t.Errorf("RelsByTypeAt(1500) after import = %v, want [moved]", got)
			}
			byType, err := dst.Rels.ByType("HOP", storepkg.QueryOpts{ValidAt: 1500})
			if err != nil {
				t.Fatal(err)
			}
			if got := names(byType); !slices.Equal(got, []string{"moved"}) {
				t.Errorf("ByType{ValidAt:1500} after import = %v, want [moved]", got)
			}
			during, err := dst.Rels.ByType("HOP", storepkg.QueryOpts{ValidStart: 1200, ValidEnd: 1300})
			if err != nil {
				t.Fatal(err)
			}
			if got := names(during); !slices.Equal(got, []string{"moved"}) {
				t.Errorf("ByType{1200-1300} after import = %v, want [moved]", got)
			}
			// The current answer is unchanged by the fix (no over-reporting).
			now, err := dst.Temporal.RelsByTypeAt("HOP", 6500)
			if err != nil {
				t.Fatal(err)
			}
			if got := names(now); !slices.Equal(got, []string{"late", "moved"}) {
				t.Errorf("RelsByTypeAt(6500) after import = %v, want [late moved]", got)
			}
		})
	}
}
