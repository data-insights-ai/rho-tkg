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
// Serial: it reads the process-wide allocation counter.
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
	bytesPerCall := func(t *testing.T, mk func(*testing.T) Config, versions int) uint64 {
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
		const calls = 200
		var before, after runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&before)
		for range calls {
			read()
		}
		runtime.ReadMemStats(&after)
		return (after.TotalAlloc - before.TotalAlloc) / calls
	}
	for _, name := range []string{"memory", "badger", "tiered"} {
		one, two, eight := bytesPerCall(t, stores[name], 1), bytesPerCall(t, stores[name], 2), bytesPerCall(t, stores[name], 8)
		t.Logf("%s: %d B per call at 1 version, %d at 2, %d at 8", name, one, two, eight)
		if name == "memory" && two > one+1024 {
			t.Errorf("%s: a second version adds %d B per call, want < 1 kB", name, two-one)
		}
		// badger and tiered pay a fixed iterator cost once any history
		// exists (todo 120 follow-up); every further version stays cheap.
		if perVersion := (eight - two) / 6; perVersion > 1024 {
			t.Errorf("%s: each further version adds %d B per call, want < 1 kB", name, perVersion)
		}
	}
}
