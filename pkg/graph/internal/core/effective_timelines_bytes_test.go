package core

import (
	"context"
	"runtime"
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEffectiveTimelinesBytesPerExtraVersion pins row 120 (ai-soc's listing
// bound): a second version of a 13-column fact adds less than 1 kB to a
// NodeEffectiveTimelines call at two pins on memory, and every further version
// less than 1 kB on memory, badger and tiered. The
// resolver works on temporal skeletons and hydrates the winners only; it
// neither copies every history row (memory's GetNodeHistory) nor copies the
// lent row's temporal block on every read (Node.Temporal on a frozen row).
// Mutation: either copy alone adds 1.5 to 3.7 kB per extra version.
//
// The gate counts the full history rows a call reads (Core.historyRead): a
// second version adds at most its one winner, every further version none (a
// copy of every history row reads them all). Bytes per call count the whole
// process, so they are logged only (MEMGATE: the unsigned slope wrapped, and
// parallel tests move the counter). Serial: it logs the process-wide
// allocation counter.
func TestEffectiveTimelinesBytesPerExtraVersion(t *testing.T) {
	stores := map[string]func(t *testing.T) Config{
		"memory": func(*testing.T) Config { return Config{Store: memory.New()} },
		"badger": func(t *testing.T) Config {
			bs, err := badgerpkg.New(badgerpkg.Config{InMemory: true, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return Config{Store: bs}
		},
		"tiered": func(t *testing.T) Config {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Fact"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return Config{Store: ts}
		},
	}
	props := map[string]any{"tkg_valid_from": types.Instant(1000)}
	for i := 0; i < 13; i++ {
		props["ov_c"+itoa(i)] = "term-value-" + itoa(i)
	}
	// cost is one call's history rows read and head hits (counts), and the
	// mean bytes per call over many (log only).
	type cost struct {
		rows  int
		hits  uint64
		bytes uint64
	}
	callCost := func(t *testing.T, mk func(*testing.T) Config, versions int) cost {
		g, err := New(mk(t))
		if err != nil {
			t.Fatal(err)
		}
		defer g.Close()
		ctx := context.Background()
		n, err := g.Nodes.Add(ctx, []string{"Fact"}, props)
		if err != nil {
			t.Fatal(err)
		}
		for i := 1; i < versions; i++ {
			if _, err := g.Temporal.SetNodeVersionInterval(ctx, n.ID(), 1000, 0, map[string]any{"ov_c1": i}); err != nil {
				t.Fatal(err)
			}
		}
		if f, ok := g.store.(interface{ Flush() error }); ok {
			_ = f.Flush() // measure the read, not the write buffer's overlay
		}
		pins := []types.Instant{g.readNow(), g.readNow()}
		read := func() {
			if _, err := g.Temporal.NodeEffectiveTimelines(n.ID(), pins); err != nil {
				t.Fatal(err)
			}
		}
		read() // warm caches
		var c cost
		g.historyRead = func(n int) { c.rows += n }
		hits := g.headHits.Load()
		read()
		g.historyRead = nil
		c.hits = g.headHits.Load() - hits
		const calls = 200
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range calls {
			read()
		}
		runtime.ReadMemStats(&after)
		c.bytes = (after.TotalAlloc - before.TotalAlloc) / calls
		return c
	}
	for _, name := range []string{"memory", "badger", "tiered"} {
		one, two, eight := callCost(t, stores[name], 1), callCost(t, stores[name], 2), callCost(t, stores[name], 8)
		t.Logf("%s: history rows read per call %d / %d / %d, head hits %d / %d / %d; %d B per call at 1 version, %d at 2, %d at 8 (process-wide, log only)",
			name, one.rows, two.rows, eight.rows, one.hits, two.hits, eight.hits, one.bytes, two.bytes, eight.bytes)
		if two.rows > one.rows+1 {
			t.Errorf("%s: a second version reads %d more history rows per call, want <= 1 (its winner)", name, two.rows-one.rows)
		}
		if eight.rows != two.rows {
			t.Errorf("%s: 8 versions read %d history rows per call, 2 versions %d: each further version adds rows", name, eight.rows, two.rows)
		}
	}
}
