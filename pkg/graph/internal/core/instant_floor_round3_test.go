package core

import (
	"math"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// A COMMITTED row's stamp must always raise the floor, however far ahead it is.
//
// v4.24.2 bounded advanceInstantFloor against the local wall clock to stop a
// corrupt watermark poisoning the clock. That bound was applied to ALL callers,
// which was too broad: the seed door reads untrusted bytes off disk, but the
// apply and import doors carry stamps for rows being committed RIGHT NOW. For
// those, silently declining to raise the floor stores the row durably while
// leaving NowTx() below it — reintroducing the exact lesson-71 anachronism the
// feature exists to close, and doing it silently.
//
// It is not exotic: a replica whose own clock is years behind the primary (a
// stale VM image, a dead RTC, a container with no NTP) sees EVERY legitimate
// primary stamp as out-of-bound and stops advancing entirely.
func TestInstantFloor_CommittedStampAlwaysRaisesTheFloor(t *testing.T) {

	g, err := New(Config{Store: memory.New(), SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// A stamp far beyond any plausibility bound anchored on this host's wall —
	// exactly what a replica with a badly-lagging clock sees from a healthy peer.
	far := types.Instant(time.Now().UnixMilli()) + types.Instant(maxClockAdvanceSkewMillis) + 1_000_000
	g.advanceInstantFloor(far)

	pin, err := g.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	if pin < far {
		t.Fatalf("NowTx = %d < a COMMITTED stamp of %d — the floor silently refused to advance, so "+
			"a durably stored row sits above the pin and every AS-OF read at NowTx() drops it. A "+
			"plausibility bound belongs on the untrusted SEED door, not on stamps for rows already "+
			"being committed.", pin, far)
	}
}

// Defence in depth: whatever installs a huge floor, the clock must SATURATE
// rather than wrap. v4.24.1's catastrophic symptom was not the large floor
// itself but `next = max(wall, last+1)` overflowing MaxInt64 into MinInt64 and
// turning every subsequent TxFrom negative.
func TestCommitClock_NeverWrapsPastMaxInt64(t *testing.T) {
	g, err := New(Config{Store: memory.New(), SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	g.lastInstant.Store(math.MaxInt64)
	for i := 0; i < 3; i++ {
		if got := g.now(); got <= 0 {
			t.Fatalf("now() = %d at a saturated clock — `last+1` wrapped past MaxInt64 into "+
				"negative transaction time", got)
		}
	}
}

// The untrusted door keeps its bound: a corrupt watermark must still be refused.
// This is the control that stops the fix above from simply removing the guard.
func TestInstantFloor_SeedStillRefusesAnImplausibleWatermark(t *testing.T) {
	g, err := New(Config{Store: memory.New(), SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	if got := g.plausibleSeedFloor(types.Instant(math.MaxInt64)); got {
		t.Fatal("the seed door accepted a MaxInt64 watermark — untrusted disk bytes must still be " +
			"bounded, or a corrupt value poisons the clock again")
	}
	sane := types.Instant(time.Now().UnixMilli())
	if got := g.plausibleSeedFloor(sane); !got {
		t.Fatalf("the seed door refused a sane watermark %d", sane)
	}
}
