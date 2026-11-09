package tiered

import (
	"encoding/binary"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// vtLeaseOf reads the window's persisted lease: what a restart after a crash
// starts from.
func vtLeaseOf(t *testing.T, ts *Store, ws time.Time) (lo, hi int64) {
	t.Helper()
	v, err := ts.refShard.MetaGet(vtLeaseKey(ws))
	if err != nil || len(v) != 16 {
		t.Fatalf("lease of %v: %d bytes, %v", ws, len(v), err)
	}
	return int64(binary.BigEndian.Uint64(v[:8])), int64(binary.BigEndian.Uint64(v[8:])) // #nosec G115 -- test counters
}

// vtCounterOf is an ID's mint counter (vtID's inverse).
func vtCounterOf(id int64) int64 { return (id>>vtTimeShift)<<vtStepBits | id&(1<<vtStepBits-1) }

// TestVTLease_TimeOrderedStreamSavesFewLeases (VTLEASE): a time-ordered
// stream over a day saves a bounded number of leases, not one per new
// millisecond; the persisted lease covers every ID handed out (a crash
// restart reuses none and forfeits at most vtLeaseCap past the last); a clean
// Close saves the exact cursors, so a reopen mints at the instant itself.
func TestVTLease_TimeOrderedStreamSavesFewLeases(t *testing.T) {
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
	day := julDay(22, 0)
	seen := map[types.NodeID]bool{}
	minC, maxC := int64(1<<62), int64(0)
	mint := func(s *Store, at time.Time) types.NodeID {
		t.Helper()
		id, ok, err := s.MintNodeIDAt(ev, ms(at), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt(%v) = %d, %v, %v", at, id, ok, err)
		}
		if seen[id] {
			t.Fatalf("ID %d minted twice", id)
		}
		seen[id] = true
		c := vtCounterOf(int64(id))
		minC, maxC = min(minC, c), max(maxC, c)
		return id
	}

	const n = 20000 // one new millisecond per record: 20000 lease saves before VTLEASE
	before := ts.vtLeaseSaves.Load()
	var last time.Time
	for i := range n {
		last = day.Add(time.Duration(i) * 4 * time.Second) // 22.2 h of the day
		c := vtCounterOf(int64(mint(ts, last)))
		if lo, hi := vtLeaseOf(t, ts, day); c < lo || c > hi {
			t.Fatalf("mint %d: counter %d outside the persisted lease [%d, %d]", i, c, lo, hi)
		}
	}
	saves := ts.vtLeaseSaves.Load() - before
	t.Logf("%d lease saves for %d records", saves, n)
	if saves > 100 {
		t.Fatalf("%d lease saves for a time-ordered stream of %d, want at most 100", saves, n)
	}

	// A crash: the cursors restart from the persisted lease.
	ts.vtMu.Lock()
	ts.vtWindows = nil
	ts.vtMu.Unlock()
	next := last.Add(time.Second)
	got := time.UnixMilli(int64(mint(ts, next).MintInstant())).UTC()
	if got.Before(next) || got.After(next.Add(time.Hour+time.Millisecond)) {
		t.Fatalf("after a crash the next ID's time %v, want within an hour past %v", got, next)
	}
	for i := range 200 { // instants already used: no ID twice
		mint(ts, day.Add(time.Duration(i)*time.Minute))
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// A clean Close: the lease is the minted bounds, so nothing is forfeited.
	if ts, err = New(tierClockCfg(dir, clock)); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()
	ts.SetLabelRegistry(reg)
	if lo, hi := vtLeaseOf(t, ts, day); lo != minC || hi != maxC {
		t.Fatalf("after a clean Close the lease [%d, %d], want the minted bounds [%d, %d]", lo, hi, minC, maxC)
	}
	at := got.Add(time.Second)
	if id := mint(ts, at); id.MintInstant() != ms(at) {
		t.Fatalf("after a clean Close the ID's time %d, want the instant %d", id.MintInstant(), ms(at))
	}
}
