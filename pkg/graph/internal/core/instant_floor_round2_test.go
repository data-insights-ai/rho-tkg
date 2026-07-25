package core

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// floorFailStore fails MetaGet for the commit-clock watermark ONLY, and no-ops
// Close so the shared memory store survives Core.Close() across sessions.
type floorFailStore struct {
	*memory.Store
	failGet error
}

func (s *floorFailStore) MetaGet(key string) ([]byte, error) {
	if s.failGet != nil && key == instantFloorMeta {
		return nil, s.failGet
	}
	return s.Store.MetaGet(key)
}
func (s *floorFailStore) Close() error { return nil }

// The watermark is a monotone HIGH-WATER MARK. A session that could not READ it
// must never LOWER it.
//
// seedInstantFloor collapses two different failures into one silent return:
// "the key is unreadable right now" (an IO/checksum/vlog fault, with the value
// still intact on disk) and "the value is malformed". Only the second justifies
// overwriting. Close then writes this session's lastInstant unconditionally, so
// one transient read fault permanently DOWNGRADES the durable floor — and since
// the seed is the only door that raises the floor on a plain reopen, every later
// open reseeds the regressed value and NowTx() under-covers burst rows that are
// still in the store. That is the lesson-71 anachronism, reintroduced durably.
func TestInstantFloor_FailedSeedMustNotDowngradeTheWatermark(t *testing.T) {
	ctx := context.Background()
	base := memory.New()

	// Session 1: a >1 write/ms burst under a frozen wall inflates the floor above
	// the wall, and Close persists it.
	g1, err := New(Config{Store: &floorFailStore{Store: base}})
	if err != nil {
		t.Fatalf("New g1: %v", err)
	}
	frozen := time.Now().Add(time.Second)
	g1.SetClockForTest(t, func() time.Time { return frozen })
	var burstMax types.Instant
	for i := 0; i < 300; i++ {
		n, err := g1.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": i})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		if tx := n.Temporal().TxFrom; tx > burstMax {
			burstMax = tx
		}
	}
	if burstMax <= types.Instant(frozen.UnixMilli()) {
		t.Fatalf("precondition: burst did not outrun the wall (max=%d wall=%d)", burstMax, frozen.UnixMilli())
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("close g1: %v", err)
	}
	v, err := base.MetaGet(instantFloorMeta)
	if err != nil || len(v) != 8 {
		t.Fatalf("precondition: watermark not persisted (%v, %d bytes)", err, len(v))
	}

	// Session 2: ONE transient read fault on the watermark key.
	g2, err := New(Config{Store: &floorFailStore{Store: base, failGet: errors.New("transient meta read fault")}})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}
	if _, err := g2.Nodes.Add(ctx, []string{"T"}, map[string]any{"post": true}); err != nil {
		t.Fatalf("g2 add: %v", err)
	}
	if err := g2.Close(); err != nil {
		t.Fatalf("close g2: %v", err)
	}

	v2, err := base.MetaGet(instantFloorMeta)
	if err != nil || len(v2) != 8 {
		t.Fatalf("watermark unreadable after g2: %v, %d bytes", err, len(v2))
	}
	if got := types.Instant(binary.BigEndian.Uint64(v2)); got < burstMax {
		t.Fatalf("WATERMARK DOWNGRADED: %d < %d — a session whose seed swallowed a MetaGet ERROR "+
			"destroyed a floor it never read. The watermark is a monotone high-water mark; an "+
			"unreadable-but-intact value must not be overwritten by this session's lower floor.",
			got, burstMax)
	}

	// Live consequence: a fresh pin must still cover the burst rows still stored.
	g3, err := New(Config{Store: &floorFailStore{Store: base}})
	if err != nil {
		t.Fatalf("New g3: %v", err)
	}
	defer g3.Close()
	pin, err := g3.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx: %v", err)
	}
	if pin < burstMax {
		t.Fatalf("NowTx pin %d < persisted max TxFrom %d — the AS-OF pin under-covers every burst "+
			"row still in the store", pin, burstMax)
	}
}

// NOTE — the sibling finding (a REJECTED import advancing the clock) is FIXED in
// import.go but is NOT covered here: constructing an export stream that reaches
// the per-wire advance site and is then rejected by token/limit/hash validation
// needs disproportionate plumbing. The fix moves each advance to AFTER that
// record's validation, matching applyChangeRecordLocked, whose comment states
// the rule: advance only after a SUCCESSFUL apply so a rejected or corrupt
// record cannot push the floor.
