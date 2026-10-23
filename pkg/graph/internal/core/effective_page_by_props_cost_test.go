package core

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestForEachNodeEffectiveByLabelAndPropertiesColdBytes pins row 121's
// listing bound on ai-soc's shape: tiered on disk with the label a reference
// label, a composite index on (label, ns, rel), facts of 13 columns (442 bytes
// of values), the store closed and reopened so every row is cold. One page
// door call over the label allocates per fact at most 6x the fact's values
// for one version (2.3 kB measured: one decode of the current row, read
// through the reference shard's snapshot) and 12x for a fact rewritten in
// valid time (4.7 kB: the skeleton decoded in place, the current row, no
// history row copied), and 16x listed at a pin before the rewrite, where
// every winner is a history row (6.6 kB: the winners read through the
// snapshot's own forward iterator and decoded in place; a badger Get per
// winner cost 10.7 kB). Before the door, ByLabelAndProperties plus
// NodeEffectiveTimelines per fact cost 11.4 kB and 16.2 kB. Mutation: a
// candidate list read through rows, a point read per row instead of the
// snapshot, a history memo that copies every row, a Seek per entity or a
// Get per winner breaks a bound. Serial: it reads the process-wide allocation counter.
func TestForEachNodeEffectiveByLabelAndPropertiesColdBytes(t *testing.T) {
	const facts, columns = 1024, 13
	open := func(dir string) *Core {
		ts, err := tiered.New(tiered.Config{DataDir: dir, RefLabels: []string{"D"}, ShardWindow: time.Hour})
		if err != nil {
			t.Fatal(err)
		}
		g, err := New(Config{Store: ts})
		if err != nil {
			t.Fatal(err)
		}
		return g
	}
	props := map[string]any{"ns": "n", "rel": "r"}
	for _, c := range []struct {
		versions int
		early    bool // list at a pin before the rewrite: every winner is a history row
		bound    int  // times the fact's values
	}{{1, false, 6}, {2, false, 12}, {2, true, 16}} {
		dir := t.TempDir()
		g := open(dir)
		ctx := context.Background()
		if err := g.Index.CreateComposite("D", []string{"ns", "rel"}); err != nil {
			t.Fatal(err)
		}
		values := 0
		ids := make([]types.NodeID, facts)
		for i := range facts {
			p := map[string]any{"ns": "n", "rel": "r", "tkg_valid_from": types.Instant(1000)}
			values = 0
			for col := range columns {
				v := fmt.Sprintf("%034x", i*columns+col)
				p[fmt.Sprintf("c%d", col)] = v
				values += len(v)
			}
			n, err := g.Nodes.Add(ctx, []string{"D"}, p)
			if err != nil {
				t.Fatal(err)
			}
			ids[i] = n.ID()
		}
		early := g.readNow()
		for i, id := range ids {
			for v := 1; v < c.versions; v++ {
				if _, err := g.Temporal.SetNodeVersionInterval(ctx, id, 1000, 0, map[string]any{"c0": fmt.Sprintf("%034x", -i-v)}); err != nil {
					t.Fatal(err)
				}
			}
		}
		pin := g.readNow()
		if c.early {
			pin = early
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		g = open(dir)
		seen := 0
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		_, err := g.Temporal.ForEachNodeEffectiveByLabelAndProperties(ctx, "D", props, pin, temporal.EffectivePageOpts{Depth: storepkg.DepthHot}, func(_ types.NodeID, segs []temporal.NodeSegment) bool {
			if len(segs) == 1 {
				seen++
			}
			return true
		})
		runtime.ReadMemStats(&after)
		if cerr := g.Close(); err != nil || cerr != nil {
			t.Fatal(err, cerr)
		}
		if seen != facts {
			t.Fatalf("%d versions: %d facts with one segment, want %d", c.versions, seen, facts)
		}
		perFact := int(after.TotalAlloc-before.TotalAlloc) / facts
		t.Logf("%d version(s), early pin %v: %d B per fact cold, %.1fx its %d B of values", c.versions, c.early, perFact, float64(perFact)/float64(values), values)
		// -race drops sync.Pool entries on purpose: the bound is for real builds.
		if !isRaceEnabled() && perFact > c.bound*values {
			t.Fatalf("%d version(s): %d B per fact, want <= %dx the %d B of values", c.versions, perFact, c.bound, values)
		}
	}
}
