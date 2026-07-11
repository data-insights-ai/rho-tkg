package bench

import (
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// K3c — COMPOSITE node property indexes.
//
// compositeLookupFixtureSize matches the WP scenario name: 100k nodes where
// the FIRST key ("status") is deliberately UNSELECTIVE — only
// len(compositeLookupStatuses) distinct values, so a single-key index on
// "status" alone still leaves ~compositeLookupFixtureSize/len(statuses)
// candidates (20k at this fixture size) for a caller-side post-filter on the
// second key. The SECOND key ("region") is selective
// (compositeLookupRegionCount distinct values), so the FULL two-key
// predicate matches far fewer rows — the shape a composite index exists for.
// See docs/query-planners.md "Composite property indexes — When a composite
// index beats a single-key index + post-filter".
const compositeLookupFixtureSize = 100_000

const compositeLookupLabel = "CompositeBenchNode"

// compositeLookupStatuses is the unselective first key's value domain.
var compositeLookupStatuses = []string{"active", "pending", "archived", "suspended", "draft"}

// compositeLookupRegionCount is the selective second key's cardinality.
const compositeLookupRegionCount = 50

// buildCompositeLookupFixture assigns status and region so the two keys are
// DECORRELATED: status cycles on i directly (i % len(statuses)), region
// cycles on i/len(statuses) (% regionCount) — so for any FIXED status value,
// the region assignment still sweeps uniformly through every region value
// (both cycle lengths divide compositeLookupFixtureSize evenly), giving an
// exact, fixture-size-independent expected match count for the full
// (status, region) predicate: fixtureSize / len(statuses) / regionCount.
func buildCompositeLookupFixture(tb testing.TB, bc backendCase) *graph.Graph {
	tb.Helper()
	ctx := benchCtx()
	g := newBenchGraph(tb, bc)
	for i := 0; i < compositeLookupFixtureSize; i++ {
		status := compositeLookupStatuses[i%len(compositeLookupStatuses)]
		region := fmt.Sprintf("region-%d", (i/len(compositeLookupStatuses))%compositeLookupRegionCount)
		if _, err := g.Nodes().Add(ctx, []string{compositeLookupLabel}, map[string]any{
			"status": status,
			"region": region,
		}); err != nil {
			tb.Fatalf("add node %d: %v", i, err)
		}
	}
	return g
}

// BenchmarkCompositeLookupVsSingleIndexPlusFilter is K3c's required
// benchmark: CompositeLookup vs single-index+post-filter, on the fixture
// above, comparing:
//
//	single_index_plus_filter — g.Index().CreateProperty(label, "status") +
//	  g.Nodes().ByLabelAndProperty(label, "status", want, opts), then a
//	  caller-side loop filtering the candidate set by "region" — the
//	  pattern a caller reaches for BEFORE a composite index exists.
//	composite_lookup — g.Index().CreateComposite(label, []string{"status",
//	  "region"}) + g.Nodes().ByLabelAndProperties(label, values, opts) — O(matches)
//	  instead of O(label-size-for-that-status).
func BenchmarkCompositeLookupVsSingleIndexPlusFilter(b *testing.B) {
	const wantStatus = "active"
	const wantRegion = "region-7"
	expected := compositeLookupFixtureSize / len(compositeLookupStatuses) / compositeLookupRegionCount

	for _, bc := range backendCases {
		b.Run(bc.name, func(b *testing.B) {
			g := buildCompositeLookupFixture(b, bc)

			b.Run("single_index_plus_filter", func(b *testing.B) {
				if err := g.Index().CreateProperty(compositeLookupLabel, "status"); err != nil {
					b.Fatalf("CreateProperty: %v", err)
				}
				b.ReportAllocs()
				for b.Loop() {
					candidates, err := g.Nodes().ByLabelAndProperty(compositeLookupLabel, "status", wantStatus, storepkg.QueryOpts{})
					if err != nil {
						b.Fatalf("ByLabelAndProperty: %v", err)
					}
					matched := 0
					for _, n := range candidates {
						if v, ok := n.GetProperty("region"); ok && v == wantRegion {
							matched++
						}
					}
					if matched != expected {
						b.Fatalf("got %d matches, want %d", matched, expected)
					}
				}
			})

			b.Run("composite_lookup", func(b *testing.B) {
				if err := g.Index().CreateComposite(compositeLookupLabel, []string{"status", "region"}); err != nil {
					b.Fatalf("CreateComposite: %v", err)
				}
				b.ReportAllocs()
				for b.Loop() {
					nodes, err := g.Nodes().ByLabelAndProperties(compositeLookupLabel,
						map[string]any{"status": wantStatus, "region": wantRegion}, storepkg.QueryOpts{})
					if err != nil {
						b.Fatalf("ByLabelAndProperties: %v", err)
					}
					if len(nodes) != expected {
						b.Fatalf("got %d nodes, want %d", len(nodes), expected)
					}
				}
			})
		})
	}
}
