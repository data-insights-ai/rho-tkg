package core

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestBitemporalOracle_BadgerCommitWindow reproduces the field configuration in
// which the badger-only, load-dependent read-consistency defect (lesson 64) was
// observed: a graph whose writes have JUST completed while the async flush loop
// is still draining `pending`->`flushing`->Badger, probed with the SAME
// cross-door oracle checks as TestBitemporalOracleHarness.
//
// Unlike the main harness (BadgerInMemory with the stock 100ms flush interval and
// an implicit pre-probe drain), this variant:
//   - drives a REAL badger store with a sub-millisecond FlushInterval, so the
//     background flush goroutine is continuously mid-commit during probing, and
//   - deliberately does NOT flush before probing, so the version chains a probe
//     reads are exactly the ones a concurrent flush is moving from `flushing`
//     into Badger — the getNodeHistoryByPrefix / getRelHistoryByPrefix scan->merge
//     window that dropped in-flight versions before the fix, and
//   - runs with a tiny cache so reads miss the resident cache and hit the store's
//     overlay paths.
//
// Every probe cross-checks NodesAtTx vs Nodes.All(point) vs ByLabel and
// RelsAtTx vs Rels.All(point) plus per-entity NodeAtTx/RelAtTx against the oracle
// (via w.runProbe). A dropped history version makes a per-node chain resolve to
// the wrong version or ErrNodeNotFound, which those exact-set assertions catch.
//
// This is a -race INTEGRATION guard over the fixed read path, NOT the
// deterministic reproduction: the scan->merge window is sub-microsecond, so —
// exactly as the field sighting was 2/30 only under heavy load — hitting it
// through the full public-door stack in a short run is unreliable. The
// deterministic reproduction lives in the badger package
// (TestFlushingCommitWindow_* with historyScanTestHook), which lands a
// concurrent flush inside the window on demand.
func TestBitemporalOracle_BadgerCommitWindow(t *testing.T) {
	const iterations = 10
	const base uint64 = 0xB17E_C0DE_64 // "bit-code-64"

	for it := 0; it < iterations; it++ {
		seed := base + uint64(it)
		t.Run(fmt.Sprintf("iter=%d/seed=%d", it, seed), func(t *testing.T) {
			bs, err := badger.New(badger.Config{
				InMemory:      true,
				FlushInterval: 150 * time.Microsecond, // keep a flush continuously mid-commit
				CacheCapacity: 8,                      // force cache misses so reads exercise the store overlay
			})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			g, err := New(Config{Store: bs, AllowTxBackfill: true})
			if err != nil {
				bs.Close()
				t.Fatalf("New: %v", err)
			}
			defer g.Close()
			if !g.bitemporalMigrated {
				t.Fatal("expected bitemporalMigrated=true; oracle assumptions invalid")
			}

			rng := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))
			w := newWorld(t, g, rng)
			w.setup()
			for i := 0; i < 18; i++ {
				w.step()
			}

			// NOTE: no Flush() here — probe while the last writes are still
			// draining through the flush loop, which is the window the defect fired
			// in. capture() reads the full chain back (History ++ current) so the
			// oracle records the real engine stamps.
			snap := w.capture()

			var maxStampV types.Instant
			for _, v := range snap.interestingInstants() {
				if v > maxStampV {
					maxStampV = v
				}
			}
			farFuture := maxStampV + 1_000_000

			probeRng := rand.New(rand.NewPCG(seed^0xD1B54A32D192ED03, seed))
			probes := snap.buildProbes(probeRng, 40, farFuture)
			if len(probes) == 0 {
				t.Fatal("buildProbes returned 0 probes")
			}

			// Two full probe passes so the narrow flush window is sampled at
			// different points of the drain.
			for pass := 0; pass < 2; pass++ {
				seenTxAt := map[types.Instant]struct{}{}
				for _, p := range probes {
					w.runProbe("badger-commit-window", seed, snap, p)
					if _, done := seenTxAt[p.txAt]; !done {
						seenTxAt[p.txAt] = struct{}{}
						w.runAsOfProbe("badger-commit-window", seed, snap, p.txAt)
					}
				}
			}
		})
	}
}
