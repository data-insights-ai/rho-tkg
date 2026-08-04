package core

import (
	"context"
	"encoding/binary"
	"math"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// The durable commit-clock watermark is UNTRUSTED INPUT on reopen.
//
// seedInstantFloor reads 8 raw bytes and feeds binary.BigEndian.Uint64 straight
// into advanceInstantFloor, which — unlike its sibling advanceClockFloor — has
// NO plausibility bound. advanceClockFloor rejects a target beyond
// maxClockAdvanceSkewMillis with ErrInvalidClockAdvance for exactly this reason
// (lesson 59: a bad value permanently poisons the graph's transaction clock).
//
// Found by a break-round, not by design review. An unbounded watermark installs
// a floor near MaxInt64; c.now()'s `next = max(wall, last+1)` then OVERFLOWS to
// MinInt64, so NowTx() and every subsequent TxFrom go NEGATIVE, and Close
// persists the poisoned floor — the corruption is durable, not process-scoped.
// Silent, too: TxFrom is not in the integrity hash, so Verify*Chain still passes.
//
// Note the first version of this test PASSED against the buggy code twice: once
// because Close re-persisted a sane floor over the corruption, and once because
// the assertion looked for a far-FUTURE clock when the real symptom is a
// wrapped-NEGATIVE one. Both are why it asserts the property (a plausible,
// positive instant) rather than a predicted failure mode.
func TestInstantFloor_CorruptWatermarkCannotPoisonTheCommitClock(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, err := g1.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": 1}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Corrupt the watermark the way reality does: an external write / bit rot
	// while the graph is CLOSED. Going through a live Core would be pointless —
	// its own Close re-persists the sane in-memory floor over the corruption.
	//
	// Simulated at the seam seedInstantFloor actually reads: install the corrupt
	// bytes and invoke the seed directly, which is exactly what New() does on the
	// next open.
	g3, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen g3: %v", err)
	}
	defer g3.Close()

	mk, ok := g3.store.(storepkg.MetaKVCapability)
	if !ok {
		t.Skip("backend without MetaKV")
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(math.MaxInt64))
	if err := mk.MetaSet(instantFloorMeta, buf); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	g3.seedInstantFloor() // what New() does on open, against untrusted bytes

	pin, err := g3.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	wall := nowInstant()
	t.Logf("after corrupt watermark: NowTx=%d wall=%d (MaxInt64=%d)", pin, wall, int64(math.MaxInt64))

	// The property: after consuming an untrusted watermark the commit clock must
	// still be a PLAUSIBLE, POSITIVE instant. Two ways it can fail, and the
	// second is the one that actually happens: the floor is installed verbatim
	// (absurdly far future), or last+1 OVERFLOWS and the clock wraps NEGATIVE.
	if pin <= 0 {
		t.Fatalf("OVERFLOW: NowTx = %d — a corrupt watermark installed a floor near MaxInt64, "+
			"and c.now()'s last+1 wrapped past MaxInt64 into negative time. Every subsequent write "+
			"is stamped with a negative TxFrom, ordering is destroyed, and Close persists it — the "+
			"corruption is DURABLE. advanceClockFloor rejects implausible values with "+
			"ErrInvalidClockAdvance for exactly this reason; advanceInstantFloor does not bound "+
			"its input at all.", pin)
	}
	if int64(pin) > int64(wall)+maxClockAdvanceSkewMillis {
		t.Fatalf("POISONED: a corrupt watermark installed a commit-clock floor of %d (wall %d, "+
			"skew bound %d) — the graph's transaction clock never recovers.",
			pin, wall, maxClockAdvanceSkewMillis)
	}
}
