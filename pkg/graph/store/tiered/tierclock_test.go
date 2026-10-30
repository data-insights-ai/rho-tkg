package tiered

import (
	"errors"
	"path/filepath"
	"strconv"
	"sync"
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

func ms(t time.Time) types.Instant { return types.Instant(t.UnixMilli()) }

// mintField is the node field of a snowflake ID (bits 10-14).
func mintField(id int64) int64 { return id >> 10 & 31 }

// TestTierClock_MintsNodesAtValidFrom (TIERCLOCK step 2): an event node's ID
// carries its ValidFrom on the reserved field 31, repeats of an instant stay
// unique and in their day, an untimed node takes the clock, a reference label
// and a store without the option leave the mint to the caller, a ValidFrom
// before the epoch is refused, and a reopened store hands out no ID twice.
func TestTierClock_MintsNodesAtValidFrom(t *testing.T) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, err := New(tierClockCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	ref, _ := reg.GetOrCreate("Case")
	at := ms(julDay(22, 12))
	seen := map[types.NodeID]bool{}
	mint := func(s *Store, at types.Instant, set bool) types.NodeID {
		t.Helper()
		id, ok, err := s.MintNodeIDAt(ev, at, set)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt(%d) = %d, %v, %v", at, id, ok, err)
		}
		if seen[id] {
			t.Fatalf("ID %d minted twice", id)
		}
		seen[id] = true
		if f := mintField(int64(id)); f != 31 {
			t.Fatalf("ID %d on node field %d, want the reserved 31", id, f)
		}
		return id
	}
	first := mint(ts, at, true)
	if got := first.MintInstant(); got != at {
		t.Fatalf("ID time %d, want the ValidFrom %d", got, at)
	}
	if second := mint(ts, at, true); second <= first { // a repeat takes the next ID up
		t.Fatalf("repeat of the instant minted %d, not above %d", second, first)
	}
	if got := mint(ts, ms(julDay(22, 8)), true).MintInstant(); got != ms(julDay(22, 8)) { // below every ID minted
		t.Fatalf("earlier instant's ID time %d, want its ValidFrom %d", got, ms(julDay(22, 8)))
	}
	for range 3000 { // repeats of one instant: unique, and in the instant's day
		id := mint(ts, at, true)
		if d := time.UnixMilli(int64(id.MintInstant())).UTC(); d.Before(julDay(22, 0)) || !d.Before(julDay(23, 0)) {
			t.Fatalf("ID time %v outside 2026-07-22", d)
		}
	}
	for _, u := range []struct {
		at  types.Instant
		set bool
	}{{0, false}, {at, false}, {0, true}} { // no ValidFrom, or the untimed axis's literal 0
		if got := mint(ts, u.at, u.set).MintInstant(); got != ms(julDay(25, 9)) {
			t.Fatalf("untimed node (%d, %v): ID time %d, want the clock %d", u.at, u.set, got, ms(julDay(25, 9)))
		}
	}
	if _, ok, err := ts.MintNodeIDAt(ref, at, true); ok || err != nil {
		t.Fatalf("reference label minted by the store: ok %v, %v", ok, err)
	}
	if _, _, err := ts.MintNodeIDAt(ev, ms(time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC)), true); !errors.Is(err, ErrValidFromOutsideIDRange) {
		t.Fatalf("ValidFrom before the epoch: %v, want ErrValidFromOutsideIDRange", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	ts, err = New(tierClockCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	ts.SetLabelRegistry(reg)
	for range 3000 { // after a reopen: still no ID twice
		mint(ts, at, true)
	}
	_ = ts.Close()
	off := tierClockCfg(t.TempDir(), clock)
	off.MintAtValidFrom = false
	plain, err := New(off)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = plain.Close() }()
	plain.SetLabelRegistry(reg)
	past := types.NodeID(vtID(vtCounter(julDay(22, 12))))
	if err := plain.PutNode(types.NewNode(past, ev, nil)); err != nil {
		t.Fatal(err)
	}
	if got := tierClockShards(t, plain); len(got) != 1 {
		t.Fatalf("store without MintAtValidFrom made a shard for a past ID: %v", got)
	}
	if _, err := plain.GetNode(past); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := plain.MintNodeIDAt(ev, at, true); ok || err != nil {
		t.Fatalf("store without MintAtValidFrom minted: ok %v, %v", ok, err)
	}
	if _, ok, err := plain.MintRelIDAt(first, at, true); ok || err != nil {
		t.Fatalf("store without MintAtValidFrom minted a relationship: ok %v, %v", ok, err)
	}
	if plain.MintsAtValidTime() {
		t.Fatal("store without MintAtValidFrom reports it mints")
	}
}

// TestTierClock_MintRefusesOutsideIDRange: a window that straddles an end of
// the ID's time range (local windows around the epoch, the 48-bit end in
// 2034) mints nothing outside it.
func TestTierClock_MintRefusesOutsideIDRange(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	cfg := tierClockCfg(t.TempDir(), clock)
	cfg.ShardWindowLocation = time.FixedZone("UTC+1", 3600) // the day 2025-12-31 23:00Z holds the epoch
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	if !ts.MintsAtValidTime() {
		t.Fatal("store with MintAtValidFrom reports it does not mint")
	}
	end := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(1<<48) * time.Microsecond)
	for _, at := range []time.Time{time.Date(2025, 12, 31, 23, 30, 0, 0, time.UTC), end.Add(time.Millisecond)} {
		if id, _, err := ts.MintNodeIDAt(ev, ms(at), true); !errors.Is(err, ErrValidFromOutsideIDRange) {
			t.Fatalf("ValidFrom %v: ID %d, %v; want ErrValidFromOutsideIDRange", at, id, err)
		}
	}
	for _, at := range []time.Time{time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), end.Add(-time.Millisecond)} {
		id, ok, err := ts.MintNodeIDAt(ev, ms(at), true)
		if err != nil || !ok || id.MintInstant() != ms(at) {
			t.Fatalf("ValidFrom %v: ID time %d, %v, %v; want it minted at its ValidFrom", at, id.MintInstant(), ok, err)
		}
	}
}

// TestTierClock_MintsRelsInStartNodeWindow: a relationship's ID lies in its
// start node's window: at its ValidFrom when that falls there, else inside
// the window; a reference start node's relationship is minted at its ValidFrom.
func TestTierClock_MintsRelsInStartNodeWindow(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, err := New(tierClockCfg(t.TempDir(), clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	start, _, err := ts.MintNodeIDAt(ev, ms(julDay(22, 12)), true)
	if err != nil {
		t.Fatal(err)
	}
	day := func(id types.RelID) time.Time { return time.UnixMilli(int64(id.MintInstant())).UTC() }
	in, ok, err := ts.MintRelIDAt(start, ms(julDay(22, 15)), true)
	if err != nil || !ok || day(in) != julDay(22, 15) || mintField(int64(in)) != 31 {
		t.Fatalf("rel at its ValidFrom inside the start node's day: %d (%v), %v, %v", in, day(in), ok, err)
	}
	for _, at := range []time.Time{julDay(25, 10), julDay(20, 10)} { // after and before the start node's day
		id, ok, err := ts.MintRelIDAt(start, ms(at), true)
		if err != nil || !ok || day(id).Before(julDay(22, 0)) || !day(id).Before(julDay(23, 0)) {
			t.Fatalf("rel valid from %v: ID time %v, %v, %v; want it inside 2026-07-22", at, day(id), ok, err)
		}
	}
	refTok, _ := reg.GetOrCreate("Case")
	refNode := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	if err := ts.PutNode(refNode); err != nil {
		t.Fatal(err)
	}
	id, ok, err := ts.MintRelIDAt(refNode.ID(), ms(julDay(24, 7)), true)
	if err != nil || !ok || day(id) != julDay(24, 7) {
		t.Fatalf("rel of a reference start node: ID time %v, %v, %v; want its ValidFrom", day(id), ok, err)
	}
	for _, at := range []time.Time{time.UnixMilli(1), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)} {
		id, ok, err := ts.MintRelIDAt(refNode.ID(), ms(at), true)
		if err != nil || !ok || !day(id).Equal(ts.clock().UTC()) {
			t.Fatalf("rel of a reference start valid from %v: ID time %v, %v, %v; want the clock", at, day(id), ok, err)
		}
	}
	if err := ts.ArchiveNode(refNode.ID()); err != nil {
		t.Fatal(err)
	}
	id, ok, err = ts.MintRelIDAt(refNode.ID(), ms(julDay(24, 8)), true)
	if err != nil || !ok || day(id) != julDay(24, 8) {
		t.Fatalf("rel of an archived start node: ID time %v, %v, %v; want its ValidFrom", day(id), ok, err)
	}
	// Outside the ID range (PREEPOCHREL), an archived start mints at the clock.
	for _, at := range []time.Time{time.UnixMilli(1), time.Date(2040, 1, 1, 0, 0, 0, 0, time.UTC)} {
		id, ok, err = ts.MintRelIDAt(refNode.ID(), ms(at), true)
		if err != nil || !ok || !day(id).Equal(ts.clock().UTC()) {
			t.Fatalf("rel of an archived start valid from %v: ID time %v, %v, %v; want the clock", at, day(id), ok, err)
		}
	}
}

// TestTierClock_WindowEndUsesLowCursor: when the high cursor would leave the
// window, the mint walks down from the window's lowest ID instead.
func TestTierClock_WindowEndUsesLowCursor(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, err := New(tierClockCfg(t.TempDir(), clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	last := ms(julDay(23, 0)) - 1 // the day's last millisecond
	var prev types.NodeID
	for i := range 1024*1000 + 10 { // more than the last millisecond holds
		id, ok, err := ts.MintNodeIDAt(ev, last, true)
		if err != nil || !ok {
			t.Fatalf("mint %d: %v, %v", i, ok, err)
		}
		if d := time.UnixMilli(int64(id.MintInstant())).UTC(); d.Before(julDay(22, 0)) || !d.Before(julDay(23, 0)) {
			t.Fatalf("mint %d: ID time %v outside 2026-07-22", i, d)
		}
		if i > 1024*1000 && id >= prev {
			t.Fatalf("mint %d: %d not below %d: the low cursor did not take over", i, id, prev)
		}
		prev = id
	}
}

// tierClockShards maps each event shard's name to its listing.
func tierClockShards(t *testing.T, ts *Store) map[string]ShardInfo {
	t.Helper()
	list, err := ts.ListShards()
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]ShardInfo{}
	for _, s := range list {
		if s.Kind == ShardEvent {
			out[s.Name] = s
		}
	}
	return out
}

// TestTierClock_WindowShardMadeOnDemand (TIERCLOCK step 3): a node minted in
// a window without a shard makes that window's shard, its tier by the clock
// (cold past ColdAfter, else warm), kept in the catalog across a reopen; a
// rotation into a window data reached first makes that shard hot.
func TestTierClock_WindowShardMadeOnDemand(t *testing.T) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, err := New(tierClockCfg(dir, clock))
	if err != nil {
		t.Fatal(err)
	}
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	put := func(at time.Time) types.NodeID {
		t.Helper()
		id, ok, err := ts.MintNodeIDAt(ev, ms(at), true)
		if err != nil || !ok {
			t.Fatalf("mint at %v: %v, %v", at, ok, err)
		}
		if err := ts.PutNode(types.NewNode(id, ev, nil)); err != nil {
			t.Fatalf("PutNode at %v: %v", at, err)
		}
		return id
	}
	old, recent, ahead := put(julDay(22, 12)), put(julDay(24, 12)), put(julDay(26, 3))
	// Reference nodes, and a relationship between them without RelsByMint,
	// live on the reference shard: they make no shard.
	refTok, _ := reg.GetOrCreate("Case")
	a, b := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil), types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	rid, _, err := ts.MintRelIDAt(a.ID(), ms(julDay(20, 1)), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationship(types.NewRelationship(rid, 1, a.ID(), b.ID())); err != nil {
		t.Fatal(err)
	}
	want := map[string]ShardTier{"2026-07-22": TierCold, "2026-07-24": TierWarm, "2026-07-25": TierHot, "2026-07-26": TierWarm}
	check := func(when string) {
		t.Helper()
		got := tierClockShards(t, ts)
		for name, tier := range want {
			if s, ok := got[name]; !ok || s.Tier != tier {
				t.Fatalf("%s: shard %s = %+v (listed %v), want tier %v", when, name, s, ok, tier)
			}
		}
		if len(got) != len(want) {
			t.Fatalf("%s: %d event shards %v, want %d", when, len(got), got, len(want))
		}
		for _, id := range []types.NodeID{old, recent, ahead} {
			if _, err := ts.GetNode(id); err != nil {
				t.Fatalf("%s: GetNode(%d): %v", when, id, err)
			}
		}
	}
	check("after the writes")
	saved := NewShardCatalog(filepath.Join(dir, "meta", "shard_catalog.json"))
	if err := saved.Load(); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"2026-07-22", "2026-07-24", "2026-07-26"} {
		if _, ok := saved.GetShard(name); !ok {
			t.Fatalf("shard %s not in the saved catalog before Close: made shards are saved first", name)
		}
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if ts, err = New(tierClockCfg(dir, clock)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	ts.SetLabelRegistry(reg)
	check("after a reopen")
	clock.set(julDay(26, 9))
	put(julDay(26, 10)) // the rotation runs first and must promote 2026-07-26
	want["2026-07-25"], want["2026-07-26"] = TierWarm, TierHot
	check("after the rotation into a shard data made first")
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	if ts, err = New(tierClockCfg(dir, clock)); err != nil {
		t.Fatalf("reopen after the promotion: %v", err)
	}
	ts.SetLabelRegistry(reg)
	check("after a reopen after the promotion")
}

// TestTierClock_RelsByMintMakesAndReopensItsDay: a relationship between two
// reference nodes goes to its own day's shard (RelsByMint), made on demand,
// and to that shard also when it is cold; nothing falls back to the
// reference shard.
func TestTierClock_RelsByMintMakesAndReopensItsDay(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	cfg := tierClockCfg(t.TempDir(), clock)
	cfg.RelsByMint = true
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	refTok, _ := reg.GetOrCreate("Case")
	a := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	b := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), refTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for _, at := range []time.Time{julDay(21, 4), julDay(21, 5), julDay(24, 6), julDay(20, 7)} {
		id, ok, err := ts.MintRelIDAt(a.ID(), ms(at), true)
		if err != nil || !ok {
			t.Fatalf("MintRelIDAt(%v): %v, %v", at, ok, err)
		}
		if err := ts.PutRelationship(types.NewRelationship(id, 1, a.ID(), b.ID())); err != nil {
			t.Fatalf("PutRelationship at %v: %v", at, err)
		}
		if _, err := ts.GetRelationship(id); err != nil {
			t.Fatalf("GetRelationship at %v: %v", at, err)
		}
		if ts.RefShardForTest().HasRelID(id.SnowflakeID()) {
			t.Fatalf("relationship at %v on the reference shard, want its day's shard", at)
		}
	}
	got := tierClockShards(t, ts)
	if s, ok := got["2026-07-21"]; !ok || s.Tier != TierCold {
		t.Fatalf("shard 2026-07-21 = %+v (listed %v), want cold", s, ok)
	}
	if s := got["2026-07-24"]; s.Tier != TierWarm || s.Rels != 1 {
		t.Fatalf("shard 2026-07-24 = %+v, want warm with its relationship", s)
	}
	if n := ts.RelsByMintFallbacks(); n != 0 {
		t.Fatalf("RelsByMintFallbacks = %d, want 0", n)
	}
	open := 0
	for _, s := range got {
		if s.Tier == TierCold && s.Open {
			open++
		}
	}
	if n := ts.OpenColdShardsForTest(); open != 2 || n != 2 {
		t.Fatalf("%d cold shards open, %d counted against MaxOpenColdShards; want both made cold shards, counted", open, n)
	}
	// A relationship from a reference node to an event node lives with its
	// start, on the reference shard: no shard for its day.
	ev, _ := reg.GetOrCreate("Signal")
	sigID, _, err := ts.MintNodeIDAt(ev, ms(julDay(24, 1)), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(types.NewNode(sigID, ev, nil)); err != nil {
		t.Fatal(err)
	}
	id, _, err := ts.MintRelIDAt(a.ID(), ms(julDay(19, 2)), true)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationship(types.NewRelationship(id, 1, a.ID(), sigID)); err != nil {
		t.Fatal(err)
	}
	got = tierClockShards(t, ts)
	if _, ok := got["2026-07-19"]; ok || len(got) != 4 {
		t.Fatalf("event shards %v, want 2026-07-20, -21, -24 and the hot -25 only", got)
	}
}

// TestTierClock_WindowShardFillsAGap: windows that do not line up with the
// stored shards (a reopen under another ShardWindowLocation) are cut to the
// gap between their neighbours, and a name already taken gets a suffix.
func TestTierClock_WindowShardFillsAGap(t *testing.T) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, err := New(tierClockCfg(dir, clock)) // hot 2026-07-25 [25 00:00Z, 26 00:00Z)
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := tierClockCfg(dir, clock)
	cfg.ShardWindowLocation = time.FixedZone("UTC+1", 3600) // days [d-1 23:00Z, d 23:00Z)
	if ts, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	for _, at := range []time.Time{julDay(24, 23).Add(30 * time.Minute), julDay(26, 0).Add(30 * time.Minute), julDay(28, 12), julDay(22, 12)} {
		id, _, err := ts.MintNodeIDAt(ev, ms(at), true)
		if err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(types.NewNode(id, ev, nil)); err != nil {
			t.Fatalf("PutNode at %v: %v", at, err)
		}
		if _, err := ts.GetNode(id); err != nil {
			t.Fatalf("GetNode at %v: %v", at, err)
		}
	}
	want := map[string][2]time.Time{
		"2026-07-25": {julDay(25, 0), julDay(26, 0)},
		"2026-07-25-" + itoa(julDay(24, 23).UnixMilli()): {julDay(24, 23), julDay(25, 0)}, // local 25 Jul, before the hot shard
		"2026-07-26": {julDay(26, 0), julDay(26, 23)},  // local 26 Jul, after it
		"2026-07-28": {julDay(27, 23), julDay(28, 23)}, // no neighbour inside: the whole window
		"2026-07-22": {julDay(21, 23), julDay(22, 23)},
	}
	got := tierClockShards(t, ts)
	if len(got) != len(want) {
		t.Fatalf("event shards %v, want %v", got, want)
	}
	for name, w := range want {
		s, ok := got[name]
		if !ok || !s.TimeStart.Equal(w[0]) || !s.TimeEnd.Equal(w[1]) {
			t.Fatalf("shard %s = %+v (listed %v), want [%v, %v)", name, s, ok, w[0], w[1])
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

// TestTierClock_RacingCreatesMakeOneShard: creates racing into one window
// without a shard make one shard between them.
func TestTierClock_RacingCreatesMakeOneShard(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, err := New(tierClockCfg(t.TempDir(), clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	const writers = 16
	ids := make([]types.NodeID, writers)
	for i := range ids {
		if ids[i], _, err = ts.MintNodeIDAt(ev, ms(julDay(23, 12)), true); err != nil {
			t.Fatal(err)
		}
	}
	var start, done sync.WaitGroup
	start.Add(1)
	errs := make([]error, writers)
	for i := range ids {
		done.Add(1)
		go func() {
			defer done.Done()
			start.Wait()
			errs[i] = ts.PutNode(types.NewNode(ids[i], ev, nil))
		}()
	}
	start.Done()
	done.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("PutNode %d: %v", i, err)
		}
	}
	if got := tierClockShards(t, ts); len(got) != 2 {
		t.Fatalf("event shards %v, want the hot one and one for 2026-07-23", got)
	}
}

// TestTierClock_LateWriteIntoADroppingDay: a late create into a day whose
// shard a retention drop is removing makes a shard under another name and
// directory, so the drop's removal of its directory cannot take the new row.
func TestTierClock_LateWriteIntoADroppingDay(t *testing.T) {
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	ts, err := New(tierClockCfg(t.TempDir(), clock))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	ev, _ := reg.GetOrCreate("Signal")
	alert, _ := reg.GetOrCreate("Alert") // not purged: the late row outlives the drop
	put := func(tok uint16, at time.Time) types.NodeID {
		t.Helper()
		id, _, err := ts.MintNodeIDAt(tok, ms(at), true)
		if err != nil {
			t.Fatal(err)
		}
		if err := ts.PutNode(types.NewNode(id, tok, nil)); err != nil {
			t.Fatalf("PutNode at %v: %v", at, err)
		}
		return id
	}
	old := put(ev, julDay(22, 12))
	var late types.NodeID
	ts.dropRemoveHook = func() { late = put(alert, julDay(22, 13)) }
	res, err := ts.PurgeNodesByLabelBefore(ev, ms(julDay(23, 0)), 8)
	ts.dropRemoveHook = nil
	if err != nil || res.NodesPurged != 1 {
		t.Fatalf("purge: %+v, %v; want the day's one node dropped", res, err)
	}
	if _, err := ts.GetNode(old); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("purged node: %v, want ErrNodeNotFound", err)
	}
	if late == 0 {
		t.Fatal("the drop did not reach its directory removal")
	}
	if _, err := ts.GetNode(late); err != nil {
		t.Fatalf("late node written during the drop: %v", err)
	}
	got := tierClockShards(t, ts)
	if _, reused := got["2026-07-22"]; reused {
		t.Fatalf("event shards %v: the dropped day's name was reused", got)
	}
	if _, ok := got["2026-07-22-"+itoa(julDay(22, 0).UnixMilli())]; !ok {
		t.Fatalf("event shards %v: no shard for the late row under a new name", got)
	}
	// A finished drop frees its name again.
	if _, err := ts.PurgeNodesByLabelBefore(alert, ms(julDay(23, 0)), 8); err != nil {
		t.Fatal(err)
	}
	put(ev, julDay(22, 14))
	if got := tierClockShards(t, ts); len(got) != 2 || got["2026-07-22"].Name == "" {
		t.Fatalf("event shards %v after both drops, want the hot one and 2026-07-22 again", got)
	}
}
