package tiered

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// tierClock is a host clock a test moves (ADR-0013 Config.Clock).
type tierClock struct{ ms atomic.Int64 }

func (c *tierClock) now() time.Time  { return time.UnixMilli(c.ms.Load()).UTC() }
func (c *tierClock) set(t time.Time) { c.ms.Store(t.UnixMilli()) }
func julDay(d, h int) time.Time      { return time.Date(2026, 7, d, h, 0, 0, 0, time.UTC) }
func tierClockCfg(dir string, c *tierClock) Config {
	return Config{DataDir: dir, RefLabels: []string{"Case"}, ShardWindow: 24 * time.Hour, ShardWindowLocation: time.UTC,
		ColdAfter: 48 * time.Hour, FlushInterval: 1<<63 - 1, Clock: c.now, MintAtValidFrom: true}
}

func tierClockPut(t *testing.T, ts *Store, tok uint16) {
	t.Helper()
	if err := ts.PutNode(types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), tok, nil)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
}

// TestTierClock_RotationFollowsClockInCalendarWindows (TIERCLOCK step 1): the
// hot shard is the calendar day holding the host clock, also for a fresh
// store; a clock jump of three days ends the old hot shard at its own day's
// end (not at the rotation, which would stretch it over the skipped days) and
// makes no shard for the skipped days; ColdAfter demotes by the clock.
func TestTierClock_RotationFollowsClockInCalendarWindows(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, err := New(tierClockCfg(t.TempDir(), clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	if got := ts.HotShardForTest().Name(); got != "2026-07-22" {
		t.Fatalf("hot shard %q at open, want the clock's day 2026-07-22", got)
	}
	tierClockPut(t, ts, tok)

	clock.set(julDay(25, 8))
	tierClockPut(t, ts, tok)
	hot := ts.HotShardForTest()
	if hot.Name() != "2026-07-25" || !hot.timeStart.Equal(julDay(25, 0)) || !hot.timeEnd.Equal(julDay(26, 0)) {
		t.Fatalf("hot shard %q [%v, %v), want 2026-07-25 [%v, %v)", hot.Name(), hot.timeStart, hot.timeEnd, julDay(25, 0), julDay(26, 0))
	}
	old, ok := ts.CatalogForTest().GetShard("2026-07-22")
	if !ok || !old.TimeEnd.Equal(julDay(23, 0)) {
		t.Fatalf("old shard %+v, want it ending at its day's end %v", old, julDay(23, 0))
	}
	if old.Tier != TierCold {
		t.Fatalf("old shard tier %v, want cold: it ended 2026-07-23, before the clock minus ColdAfter", old.Tier)
	}
	for _, d := range []string{"2026-07-23", "2026-07-24"} {
		if _, ok := ts.CatalogForTest().GetShard(d); ok {
			t.Fatalf("shard %s made for a skipped day no data reached", d)
		}
	}
}

// TestTierClock_ManualRotationRefused: with MintAtValidFrom a manual rotation
// inside a window would cut it under rows minted past the cut.
func TestTierClock_ManualRotationRefused(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, err := New(tierClockCfg(t.TempDir(), clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	if err := ts.RotateHotShard(); !errors.Is(err, ErrManualRotation) {
		t.Fatalf("RotateHotShard = %v, want ErrManualRotation", err)
	}
	if got := ts.HotShardForTest().Name(); got != "2026-07-22" {
		t.Fatalf("hot shard %q after the refused rotation", got)
	}
}

// TestTierClock_OpenDemotesByClock: ColdAfter at open reads the clock too, so a
// replay that restarts demotes the days its clock has left behind.
func TestTierClock_OpenDemotesByClock(t *testing.T) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(22, 12))
	ts, err := New(tierClockCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	tok, _ := reg.GetOrCreate("Signal")
	tierClockPut(t, ts, tok)
	clock.set(julDay(23, 8))
	tierClockPut(t, ts, tok)
	if e, _ := ts.CatalogForTest().GetShard("2026-07-22"); e.Tier != TierWarm {
		t.Fatalf("2026-07-22 tier %v after the rotation, want warm (within ColdAfter)", e.Tier)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	clock.set(julDay(23, 20)) // 2026-07-22 ended within ColdAfter of the clock, months before the wall clock
	if ts, err = New(tierClockCfg(dir, clock)); err != nil {
		t.Fatal(err)
	}
	if e, _ := ts.CatalogForTest().GetShard("2026-07-22"); e.Tier != TierWarm {
		t.Fatalf("2026-07-22 tier %v after a reopen on 2026-07-23, want warm", e.Tier)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	clock.set(julDay(26, 12))
	ts, err = New(tierClockCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	if e, _ := ts.CatalogForTest().GetShard("2026-07-22"); e.Tier != TierCold {
		t.Fatalf("2026-07-22 tier %v after a reopen on 2026-07-26, want cold", e.Tier)
	}
}
