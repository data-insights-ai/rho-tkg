package tiered

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// R3-3 LATEWRITE, the grow (tasks/evidence/R3/design-latewrite-fix.txt
// section 1, option A). With WarmMemTableSize 0 a writable open of a cold
// shard takes the small cold memtable (8 MiB); a handle that has committed
// coldGrowBytes (32 MiB) on it is closed by the handle sweep (sweepClose),
// and the shard's next writable open takes MemTableSize. The native counter
// is the .mem file's apparent size, 2 x the open's memtable (badger
// memtable.go:117); the sweep's closes and the shard's .sst count beside it.

const (
	cgHot     = 16 << 20
	cgRowSize = 256 << 10
)

type coldGrowFix struct {
	ts    *Store
	dir   string
	tok   uint16
	clock *tierClock
	next  int // the next row's i
}

// newColdGrow opens a store at MemTableSize 16 MiB and WarmMemTableSize warm,
// makes the cold day 10 with one row and leaves it closed (its first handle
// closed by the sweep, as a cold window shard's is).
func newColdGrow(t *testing.T, warm int64, mutate func(*Config)) *coldGrowFix {
	t.Helper()
	f := &coldGrowFix{dir: t.TempDir(), clock: &tierClock{}}
	f.clock.set(julDay(22, 12))
	cfg := tierClockCfg(f.dir, f.clock)
	cfg.MemTableSize = cgHot
	cfg.WarmMemTableSize = warm
	cfg.ValueLogFileSize = 1 << 20
	cfg.CacheCapacity = 100
	if mutate != nil {
		mutate(&cfg)
	}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	f.ts = ts
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	f.tok, _ = reg.GetOrCreate("Signal")
	f.put(t, 1)
	if es := f.shard(); es == nil || es.Tier() != TierCold {
		t.Fatalf("day 10 is %+v, want a cold shard", es)
	}
	ts.SweepHandlesForTest() // the window shard's first handle closes
	f.idleClose(t)
	return f
}

func (f *coldGrowFix) shard() *EventShard { return f.ts.EventShardsForTest()["2026-07-10"] }

func (f *coldGrowFix) wal(t *testing.T) int64 {
	return maxMemFileSizeTiered(t, filepath.Join(f.dir, "events", "2026-07-10"))
}

func (f *coldGrowFix) tables(t *testing.T) int {
	t.Helper()
	ssts, err := filepath.Glob(filepath.Join(f.dir, "events", "2026-07-10", "*.sst"))
	if err != nil {
		t.Fatal(err)
	}
	return len(ssts)
}

func (f *coldGrowFix) closes() int64 {
	_, closes := f.ts.HandleSweepCountsForTest()
	return closes
}

// put writes n rows of 256 KiB into day 10, flushing every 4 (1 MiB, under
// the 1.2 MiB transaction limit of an 8 MiB memtable) and at the end.
func (f *coldGrowFix) put(t *testing.T, n int) {
	t.Helper()
	if err := f.putErr(t, n); err != nil {
		t.Fatal(err)
	}
}

// putErr is put for a goroutine other than the test's: it reports instead of
// failing the test.
func (f *coldGrowFix) putErr(t *testing.T, n int) error {
	for k := 0; k < n; k++ {
		i := f.next
		f.next++
		id, ok, err := f.ts.MintNodeIDAt(f.tok, ms(julDay(10, 1+i%7)), true)
		if err != nil || !ok {
			return fmt.Errorf("MintNodeIDAt: %v, %v", ok, err)
		}
		node := types.NewNode(id, f.tok, nil)
		if err := node.SetProperties(mustTieredProps(t, map[string]any{"i": i, "pad": cgPad(i)})); err != nil {
			return err
		}
		if err := f.ts.PutNode(node); err != nil {
			return fmt.Errorf("PutNode %d: %w", i, err)
		}
		if k%4 == 3 || k == n-1 {
			if err := f.ts.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func cgPad(i int) string { return fmt.Sprintf("%05d", i) + strings.Repeat("p", cgRowSize) }

// idleClose closes every idle cold handle, then sets the idle timeout back
// so the idle loop leaves the next handle alone.
func (f *coldGrowFix) idleClose(t *testing.T) {
	t.Helper()
	f.ts.SetIdleTimeoutForTest(time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	f.ts.CloseIdleShardsForTest()
	f.ts.SetIdleTimeoutForTest(time.Hour)
	if s, _ := f.shard().StoreStateForTest(); s != nil {
		t.Fatal("day 10 still open after the idle close")
	}
}

// rows is day 10's rows by i, each pad checked against what was written.
func (f *coldGrowFix) rows(t *testing.T) int {
	t.Helper()
	ns, err := f.ts.NodesByLabel(f.tok, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	for _, n := range ns {
		iv, _ := n.GetProperty("i")
		i, err := toInt(iv)
		if err != nil {
			t.Fatalf("row i %v: %v", iv, err)
		}
		if pad, _ := n.GetProperty("pad"); fmt.Sprint(pad) != cgPad(i) {
			t.Fatalf("row %d reads a different pad", i)
		}
		seen[i] = true
	}
	for i := 0; i < f.next; i++ {
		if !seen[i] {
			t.Fatalf("row %d missing (%d rows read, %d written)", i, len(seen), f.next)
		}
	}
	return len(seen)
}

func toInt(v any) (int, error) {
	switch x := v.(type) {
	case int:
		return x, nil
	case int64:
		return int(x), nil
	case float64:
		return int(x), nil
	}
	return 0, fmt.Errorf("type %T", v)
}

type coldGrowSteps struct {
	walA, walB, walC int64 // WAL after (a), after (b)'s extra row, after (c)
	closesA, closesB int64 // sweep closes added by (a) and (b)
	tablesB          int
	growNextB        bool
	rows             int
}

// coldGrowRun is T1's steps (a) to (c) on a store at WarmMemTableSize warm.
func coldGrowRun(t *testing.T, warm int64) coldGrowSteps {
	t.Helper()
	f := newColdGrow(t, warm, nil)
	var s coldGrowSteps
	base := f.closes()
	// (a) 2 MiB: under the threshold.
	f.put(t, 8)
	f.ts.SweepHandlesForTest()
	s.walA, s.closesA = f.wal(t), f.closes()-base
	// (b) up to 40 MiB, a pass, one more row.
	f.put(t, 160-f.next)
	f.ts.SweepHandlesForTest()
	f.put(t, 1)
	s.walB, s.closesB, s.tablesB, s.growNextB = f.wal(t), f.closes()-base, f.tables(t), f.shard().GrowNextForTest()
	// (c) after the idle close the next late write is small again.
	f.idleClose(t)
	f.put(t, 1)
	s.walC = f.wal(t)
	s.rows = f.rows(t)
	return s
}

// TestColdGrow_SustainedWritesTakeTheHotSize (T1): a cold handle that has
// committed 32 MiB on the small memtable is closed by one sweep pass and the
// next write opens the shard at MemTableSize; after its idle close the next
// late write opens small again. An explicit WarmMemTableSize never grows.
// Control: WarmMemTableSize = MemTableSize, every open at 16 MiB, the same
// rows and pads.
func TestColdGrow_SustainedWritesTakeTheHotSize(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 40 MiB into a cold shard: not in -short")
	}
	got := coldGrowRun(t, 0)
	ctl := coldGrowRun(t, cgHot)
	t.Logf("WarmMemTableSize 0: WAL %d / %d / %d, sweep closes +%d / +%d, day 10 tables %d, rows %d", got.walA, got.walB, got.walC, got.closesA, got.closesB, got.tablesB, got.rows)
	t.Logf("control (16 MiB):   WAL %d / %d / %d, sweep closes +%d / +%d, day 10 tables %d, rows %d", ctl.walA, ctl.walB, ctl.walC, ctl.closesA, ctl.closesB, ctl.tablesB, ctl.rows)
	if ctl.walA != 2*cgHot || ctl.walB != 2*cgHot || ctl.walC != 2*cgHot || ctl.closesB != 0 || ctl.rows == 0 {
		t.Fatalf("control: %+v; want every WAL 2 x %d, no sweep close, rows", ctl, cgHot)
	}
	if got.walA != 2*coldMemTableSize || got.closesA != 0 {
		t.Errorf("(a) 2 MiB: WAL %d, closes +%d; want 2 x %d, +0", got.walA, got.closesA, coldMemTableSize)
	}
	if got.walB != 2*cgHot || got.closesB != 1 || got.growNextB {
		t.Errorf("(b) 40 MiB: WAL %d, closes +%d, growNext %v; want 2 x %d, +1, false", got.walB, got.closesB, got.growNextB, cgHot)
	}
	if got.walC != 2*coldMemTableSize {
		t.Errorf("(c) after the idle close: WAL %d, want 2 x %d", got.walC, coldMemTableSize)
	}
	if got.rows != ctl.rows {
		t.Errorf("rows %d, control %d", got.rows, ctl.rows)
	}

	// (d) An explicit WarmMemTableSize below MemTableSize is kept as set.
	f := newColdGrow(t, coldMemTableSize, nil)
	base := f.closes()
	f.put(t, 160-f.next)
	f.ts.SweepHandlesForTest()
	f.put(t, 1)
	if w, c := f.wal(t), f.closes()-base; w != 2*coldMemTableSize || c != 0 {
		t.Errorf("(d) WarmMemTableSize 8 MiB, 40 MiB: WAL %d, closes +%d; want 2 x %d, +0", w, c, coldMemTableSize)
	}
	if n := f.rows(t); n != f.next {
		t.Errorf("(d) rows %d, written %d", n, f.next)
	}
}

// coldGrowOwed is a store whose day 10 is on a small writable handle that has
// committed 40 MiB, the sweep held (fault at "gated"): each case sets the
// hook it needs.
func coldGrowOwed(t *testing.T, mutate func(*Config)) *coldGrowFix {
	t.Helper()
	f := newColdGrow(t, 0, mutate)
	holdHandleSweep(f.ts)
	f.put(t, 160-f.next)
	f.ts.SweepHandlesForTest() // waits for a pass the writes kicked; the hold stays
	es := f.shard()
	es.shardMu.Lock()
	owed := f.ts.coldGrowOwedLocked(es)
	es.shardMu.Unlock()
	if !owed || f.wal(t) != 2*coldMemTableSize {
		t.Fatalf("fixture: grow owed %v, WAL %d; want owed on a 2 x %d WAL", owed, f.wal(t), coldMemTableSize)
	}
	return f
}

// TestColdGrow_ScopePinAndFault (T2): the grow's close is sweepClose with
// its contract: a change-log scope defers it and loses no record, a pin
// defers it until released, a fault at the gate leaves the shard pending and
// writable, a writer blocked on the gate lands on the hot-size handle, and a
// close fault is the store's background error.
func TestColdGrow_ScopePinAndFault(t *testing.T) {
	if testing.Short() {
		t.Skip("writes 40 MiB into a cold shard per case: not in -short")
	}
	t.Run("scope", func(t *testing.T) {
		f := coldGrowOwed(t, func(c *Config) { c.ChangeLog = true })
		before, err := f.ts.ChangeFeed(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		base := f.closes()
		if err := f.ts.BeginLogScope(); err != nil {
			t.Fatal(err)
		}
		f.ts.SetSweepFaultForTest(nil)
		f.put(t, 1) // the checkout pends the grow
		f.ts.SweepHandlesForTest()
		if c := f.closes() - base; c != 0 || f.wal(t) != 2*coldMemTableSize {
			t.Fatalf("inside the scope: closes +%d, WAL %d; want +0 on the small handle", c, f.wal(t))
		}
		if _, err := f.ts.CommitLogScope(); err != nil {
			t.Fatal(err)
		}
		f.ts.SweepHandlesForTest()
		if c := f.closes() - base; c != 1 {
			t.Fatalf("after the commit: closes +%d, want +1", c)
		}
		after, err := f.ts.ChangeFeed(0, 0)
		if err != nil {
			t.Fatal(err)
		}
		if len(after) <= len(before) {
			t.Fatalf("change feed %d records after the scope, %d before: the scope's records are missing", len(after), len(before))
		}
		f.put(t, 1)
		if w := f.wal(t); w != 2*cgHot {
			t.Fatalf("after the grow: WAL %d, want 2 x %d", w, cgHot)
		}
		if again, _ := f.ts.ChangeFeed(0, 0); len(again) < len(after) {
			t.Fatalf("change feed shrank across the close: %d -> %d", len(after), len(again))
		}
		f.rows(t)
	})
	t.Run("pin", func(t *testing.T) {
		f := coldGrowOwed(t, nil)
		f.ts.SetSweepDrainTimeoutForTest(10 * time.Millisecond)
		base := f.closes()
		es := f.shard()
		pinned, err := es.CheckoutStoreForTest(f.ts)
		if err != nil {
			t.Fatal(err)
		}
		f.ts.SetSweepFaultForTest(nil)
		f.ts.SweepHandlesForTest()
		if s, _ := es.StoreStateForTest(); s != pinned || f.closes() != base {
			t.Fatalf("under a pin: handle replaced %v, closes +%d", s != pinned, f.closes()-base)
		}
		es.CheckinStoreForTest()
		f.ts.SweepHandlesForTest()
		if c := f.closes() - base; c != 1 {
			t.Fatalf("after the release: closes +%d, want +1", c)
		}
		f.put(t, 1)
		if w := f.wal(t); w != 2*cgHot {
			t.Fatalf("after the grow: WAL %d, want 2 x %d", w, cgHot)
		}
		f.rows(t)
	})
	t.Run("fault at the gate", func(t *testing.T) {
		f := coldGrowOwed(t, nil)
		base := f.closes()
		f.ts.SetSweepFaultForTest(func(step string) error {
			if step == "gated" {
				return errors.New("held")
			}
			return nil
		})
		f.ts.SweepHandlesForTest()
		f.ts.sweep.mu.Lock()
		_, pending := f.ts.sweep.pending[f.shard()]
		f.ts.sweep.mu.Unlock()
		if s, ro := f.shard().StoreStateForTest(); s == nil || ro || !pending || f.closes() != base {
			t.Fatalf("after a fault at the gate: open %v read-only %v pending %v closes +%d; want writable, pending, +0", s != nil, ro, pending, f.closes()-base)
		}
		f.put(t, 1)
		if w := f.wal(t); w != 2*coldMemTableSize {
			t.Fatalf("write under the held grow: WAL %d, want the small handle's", w)
		}
		f.rows(t)
	})
	t.Run("writer blocked on the gate", func(t *testing.T) {
		f := coldGrowOwed(t, nil)
		base := f.closes()
		reached, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		f.ts.SetSweepFaultForTest(func(step string) error {
			if step == "gated" {
				once.Do(func() { close(reached) })
				<-release
			}
			return nil
		})
		passDone := make(chan struct{})
		go func() { f.ts.SweepHandlesForTest(); close(passDone) }()
		awaitSweepStep(t, reached, "gated")
		wrote := make(chan error, 1)
		go func() { wrote <- f.putErr(t, 1) }()
		select {
		case <-wrote:
			t.Fatal("the write did not wait for the gate")
		case <-time.After(20 * time.Millisecond):
		}
		close(release)
		awaitSweepStep(t, passDone, "pass end")
		select {
		case err := <-wrote:
			if err != nil {
				t.Fatalf("the blocked write: %v", err)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("the blocked write never finished")
		}
		f.ts.SetSweepFaultForTest(nil)
		if c := f.closes() - base; c != 1 {
			t.Fatalf("closes +%d, want +1", c)
		}
		if w := f.wal(t); w != 2*cgHot {
			t.Fatalf("the blocked writer's handle: WAL %d, want 2 x %d", w, cgHot)
		}
		f.rows(t)
	})
	t.Run("close fault", func(t *testing.T) {
		// No flush loop: the pending row waits for the close's final flush.
		f := coldGrowOwed(t, func(c *Config) { c.FlushInterval = time.Hour })
		es := f.shard()
		s, _ := es.StoreStateForTest()
		id, _, err := f.ts.MintNodeIDAt(f.tok, ms(julDay(10, 2)), true)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.ts.PutNode(types.NewNode(id, f.tok, nil)); err != nil { // pending: the close's flush fails
			t.Fatal(err)
		}
		// At the gate (the pins drained, the close next) the DB refuses
		// commits: the count seal and the close's final flush fail.
		f.ts.SetSweepFaultForTest(func(step string) error {
			if step == "gated" {
				s.SetDBClosedForTest(true)
			}
			return nil
		})
		f.ts.SweepHandlesForTest()
		if err := f.ts.backgroundError(); err == nil || !strings.Contains(err.Error(), "handle sweep: close shard") {
			t.Fatalf("background error after a close fault %v, want the sweep's close error", err)
		}
		if c := f.closes(); c == 0 {
			t.Fatal("no sweep close")
		}
		if err := f.putErr(t, 1); err == nil {
			t.Fatal("a write was accepted past the close fault's background error")
		}
		if s2, _ := es.StoreStateForTest(); s2 != nil {
			t.Fatal("the faulted shard has a handle again")
		}
	})
}

// TestHandleSweep_NoSwapAtEqualWarmSize (T9, S1): WarmMemTableSize equal to
// MemTableSize owes the rotated-out shard no swap: the reopen would take the
// same arena. Control: WarmMemTableSize 8 MiB swaps once.
func TestHandleSweep_NoSwapAtEqualWarmSize(t *testing.T) {
	for _, tc := range []struct {
		warm      int64
		wantSwaps int64
	}{{cgHot, 0}, {8 << 20, 1}} {
		t.Run(fmt.Sprintf("WarmMemTableSize %d MiB", tc.warm>>20), func(t *testing.T) {
			ts, n, _ := handleSweepFixture(t, func(c *Config) { c.MemTableSize, c.WarmMemTableSize = cgHot, tc.warm })
			old := ts.HotShardForTest()
			before, _ := old.StoreStateForTest()
			forceRotation(t, ts)
			ts.SweepHandlesForTest()
			swaps, _ := ts.HandleSweepCountsForTest()
			after, _ := old.StoreStateForTest()
			if swaps != tc.wantSwaps || (tc.wantSwaps == 0) != (after == before) {
				t.Fatalf("swaps %d (handle kept %v), want %d", swaps, after == before, tc.wantSwaps)
			}
			lateWrite(t, ts, n, "after-rotation")
		})
	}
}
