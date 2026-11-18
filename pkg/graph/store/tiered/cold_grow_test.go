package tiered

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
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
	cgGrow    = 1 << 20 // the grow limit these tests set (SetColdGrowBytesForTest): four rows
	cgRows    = 9       // f.next once a handle is past cgGrow: two checkouts after the crossing flush
)

type coldGrowFix struct {
	ts    *Store
	dir   string
	tok   uint16
	reg   *registrypkg.LabelRegistry
	clock *tierClock
	next  int // the next row's i
}

// newColdGrow opens a store at MemTableSize 16 MiB and WarmMemTableSize warm,
// the grow limit lowered to cgGrow, makes the cold day 10 with one row and
// leaves it closed (its first handle closed by the sweep, as a cold window
// shard's is).
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
	ts.SetColdGrowBytesForTest(cgGrow)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	f.reg = reg
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
	// (a) 512 KiB: under the limit.
	f.put(t, 2)
	f.ts.SweepHandlesForTest()
	s.walA, s.closesA = f.wal(t), f.closes()-base
	// (b) past the limit, a pass, one more row.
	f.put(t, cgRows-f.next)
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
// committed the grow limit (lowered to 1 MiB; the 32 MiB constant is
// TestLateWriteBackfillGrowsTheColdHandle's, pkg/graph) on the small memtable
// is closed by one sweep pass and the next write opens the shard at
// MemTableSize; after its idle close the next late write opens small again.
// An explicit WarmMemTableSize never grows. Control: WarmMemTableSize =
// MemTableSize, every open at 16 MiB, the same rows and pads.
func TestColdGrow_SustainedWritesTakeTheHotSize(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	got := coldGrowRun(t, 0)
	ctl := coldGrowRun(t, cgHot)
	t.Logf("WarmMemTableSize 0: WAL %d / %d / %d, sweep closes +%d / +%d, day 10 tables %d, rows %d", got.walA, got.walB, got.walC, got.closesA, got.closesB, got.tablesB, got.rows)
	t.Logf("control (16 MiB):   WAL %d / %d / %d, sweep closes +%d / +%d, day 10 tables %d, rows %d", ctl.walA, ctl.walB, ctl.walC, ctl.closesA, ctl.closesB, ctl.tablesB, ctl.rows)
	if ctl.walA != 2*cgHot || ctl.walB != 2*cgHot || ctl.walC != 2*cgHot || ctl.closesB != 0 || ctl.rows == 0 {
		t.Fatalf("control: %+v; want every WAL 2 x %d, no sweep close, rows", ctl, cgHot)
	}
	if got.walA != 2*coldMemTableSize || got.closesA != 0 {
		t.Errorf("(a) 512 KiB: WAL %d, closes +%d; want 2 x %d, +0", got.walA, got.closesA, coldMemTableSize)
	}
	if got.walB != 2*cgHot || got.closesB != 1 || got.growNextB {
		t.Errorf("(b) past the limit: WAL %d, closes +%d, growNext %v; want 2 x %d, +1, false", got.walB, got.closesB, got.growNextB, cgHot)
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
	f.put(t, cgRows-f.next)
	f.ts.SweepHandlesForTest()
	f.put(t, 1)
	if w, c := f.wal(t), f.closes()-base; w != 2*coldMemTableSize || c != 0 {
		t.Errorf("(d) WarmMemTableSize 8 MiB, past the limit: WAL %d, closes +%d; want 2 x %d, +0", w, c, coldMemTableSize)
	}
	if n := f.rows(t); n != f.next {
		t.Errorf("(d) rows %d, written %d", n, f.next)
	}
}

// coldGrowOwed is a store whose day 10 is on a small writable handle that has
// committed past the grow limit, the sweep held (fault at "gated"): each case
// sets the hook it needs.
func coldGrowOwed(t *testing.T, mutate func(*Config)) *coldGrowFix {
	t.Helper()
	f := newColdGrow(t, 0, mutate)
	holdHandleSweep(f.ts)
	f.put(t, cgRows-f.next)
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
	testtier.Slow(t)
	t.Parallel()
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

// doorFix is newColdGrow at MaxOpenColdShards 1 with day 10 seeded with six
// small nodes of the label door (nodes[2] and nodes[3] also tag) and three
// relationships placed with their start node (nodes[0] -> nodes[1],
// nodes[0] -> nodes[2], nodes[1] -> nodes[2]), then pad rows past the grow
// limit on the same small handle, the sweep held. made collects what a create
// door made.
type doorFix struct {
	*coldGrowFix
	door, tag uint16
	nodes     []*types.Node
	rels      []*types.Relationship
	made      []types.RelID
}

func newDoorFix(t *testing.T, warm int64, mutate func(*Config)) *doorFix {
	t.Helper()
	f := &doorFix{coldGrowFix: newColdGrow(t, warm, func(c *Config) {
		c.MaxOpenColdShards = 1
		if mutate != nil {
			mutate(c)
		}
	})}
	f.door, _ = f.reg.GetOrCreate("Door")
	f.tag, _ = f.reg.GetOrCreate("Tag")
	holdHandleSweep(f.ts)
	for i := 0; i < 6; i++ {
		id, ok, err := f.ts.MintNodeIDAt(f.door, ms(julDay(10, 2)), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt: %v, %v", ok, err)
		}
		n := types.NewNode(id, f.door, nil)
		if i == 2 || i == 3 {
			n.AddLabelTokenRaw(f.tag)
		}
		if err := f.ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		f.nodes = append(f.nodes, n)
	}
	for _, e := range [][2]int{{0, 1}, {0, 2}, {1, 2}} {
		f.rels = append(f.rels, f.newRel(t, e[0], e[1]))
		if err := f.ts.PutRelationship(f.rels[len(f.rels)-1]); err != nil {
			t.Fatal(err)
		}
	}
	f.put(t, cgRows-f.next)
	f.ts.SweepHandlesForTest() // held: the shard stays pending
	return f
}

// newRel is a relationship nodes[a] -> nodes[b] minted in day 10, not stored.
func (f *doorFix) newRel(t *testing.T, a, b int) *types.Relationship {
	t.Helper()
	id, ok, err := f.ts.MintRelIDAt(f.nodes[a].ID(), ms(julDay(10, 3)), true)
	if err != nil || !ok {
		t.Fatalf("MintRelIDAt: %v, %v", ok, err)
	}
	return types.NewRelationship(id, 1, f.nodes[a].ID(), f.nodes[b].ID())
}

// pending reports whether day 10 is in the handle sweep's pending set.
func (f *doorFix) pending() bool {
	f.ts.sweep.mu.Lock()
	defer f.ts.sweep.mu.Unlock()
	_, ok := f.ts.sweep.pending[f.shard()]
	return ok
}

// owed reports whether day 10's handle owes the grow now.
func (f *doorFix) owed() bool {
	es := f.shard()
	es.shardMu.Lock()
	defer es.shardMu.Unlock()
	return f.ts.coldGrowOwedLocked(es)
}

func doorProp(t *testing.T, got any, err error, want int) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("door property %v, want %d", got, want)
	}
}

// coldDoor is one write door of the store by ID: call runs it in round 0
// (on the owed small handle) and round 1 (the shard closed by the grow);
// check reads what both rounds wrote.
type coldDoor struct {
	name  string
	call  func(t *testing.T, f *doorFix, round int) error
	check func(t *testing.T, f *doorFix)
}

func coldDoors() []coldDoor {
	nodeWith := func(f *doorFix, i, round int) *types.Node {
		n := f.nodes[i].DeepCopy()
		_ = n.SetProperty("door", round)
		return n
	}
	relWith := func(f *doorFix, i, round int) *types.Relationship {
		r := f.rels[i].DeepCopy()
		_ = r.SetProperty("door", round)
		return r
	}
	nodesGone := func(t *testing.T, f *doorFix) {
		for _, i := range []int{4, 5} {
			if _, err := f.ts.GetNode(f.nodes[i].ID()); !errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("node %d after its delete: %v, want ErrNodeNotFound", i, err)
			}
		}
		if _, err := f.ts.GetNode(f.nodes[0].ID()); err != nil {
			t.Fatalf("node 0 beside the deletes: %v", err)
		}
	}
	relsGone := func(t *testing.T, f *doorFix) {
		for _, i := range []int{1, 2} {
			if _, err := f.ts.GetRelationship(f.rels[i].ID()); !errors.Is(err, ErrRelNotFound) {
				t.Fatalf("relationship %d after its delete: %v, want ErrRelNotFound", i, err)
			}
		}
		if _, err := f.ts.GetRelationship(f.rels[0].ID()); err != nil {
			t.Fatalf("relationship 0 beside the deletes: %v", err)
		}
	}
	relsMade := func(t *testing.T, f *doorFix) {
		if len(f.made) != 2 {
			t.Fatalf("%d relationships made, want 2", len(f.made))
		}
		for _, id := range f.made {
			r, err := f.ts.GetRelationship(id)
			if err != nil || r.StartNodeID() != f.nodes[3].ID() {
				t.Fatalf("made relationship %d: %v, %v", id, r, err)
			}
		}
	}
	nodeVersions := func(t *testing.T, f *doorFix, want ...uint32) {
		hist, err := f.ts.GetNodeHistory(f.nodes[0].ID())
		if err != nil {
			t.Fatal(err)
		}
		var got []uint32
		for _, v := range hist {
			got = append(got, v.Version())
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("node 0 history versions %v, want %v", got, want)
		}
	}
	relVersions := func(t *testing.T, f *doorFix, want ...uint32) {
		hist, err := f.ts.GetRelHistory(f.rels[0].ID())
		if err != nil {
			t.Fatal(err)
		}
		var got []uint32
		for _, v := range hist {
			got = append(got, v.Version())
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("relationship 0 history versions %v, want %v", got, want)
		}
	}
	return []coldDoor{
		{"ReplaceNode", func(t *testing.T, f *doorFix, r int) error { return f.ts.ReplaceNode(nodeWith(f, 0, r)) },
			func(t *testing.T, f *doorFix) {
				n, err := f.ts.GetNode(f.nodes[0].ID())
				var v any
				if err == nil {
					v, _ = n.GetProperty("door")
				}
				doorProp(t, v, err, 1)
			}},
		{"DeleteNode", func(t *testing.T, f *doorFix, r int) error { return f.ts.DeleteNode(f.nodes[4+r].ID()) }, nodesGone},
		{"AddNodeLabelToken", func(t *testing.T, f *doorFix, r int) error {
			n := f.nodes[r].DeepCopy()
			n.AddLabelTokenRaw(f.tag)
			return f.ts.AddNodeLabelToken(n.ID(), f.tag, n)
		}, func(t *testing.T, f *doorFix) {
			ns, err := f.ts.NodesByLabel(f.tag, QueryOpts{})
			if err != nil || len(ns) != 4 {
				t.Fatalf("tag rows %d (%v), want 4 (two seeded, two added)", len(ns), err)
			}
		}},
		{"RemoveNodeLabelToken", func(t *testing.T, f *doorFix, r int) error {
			n := f.nodes[2+r].DeepCopy()
			n.RemoveLabelTokenRaw(f.tag)
			return f.ts.RemoveNodeLabelToken(n.ID(), f.tag, n)
		}, func(t *testing.T, f *doorFix) {
			if ns, err := f.ts.NodesByLabel(f.tag, QueryOpts{}); err != nil || len(ns) != 0 {
				t.Fatalf("tag rows %d (%v), want 0", len(ns), err)
			}
			if ns, err := f.ts.NodesByLabel(f.door, QueryOpts{}); err != nil || len(ns) != 6 {
				t.Fatalf("door rows %d (%v), want 6", len(ns), err)
			}
		}},
		{"DeleteNodesBatch", func(t *testing.T, f *doorFix, r int) error {
			return f.ts.DeleteNodesBatch([]types.NodeID{f.nodes[4+r].ID()})
		}, nodesGone},
		{"PutRelationship", func(t *testing.T, f *doorFix, r int) error {
			rel := f.newRel(t, 3, 4+r)
			f.made = append(f.made, rel.ID())
			return f.ts.PutRelationship(rel)
		}, relsMade},
		{"PutRelationshipsBatch", func(t *testing.T, f *doorFix, r int) error {
			rel := f.newRel(t, 3, 4+r)
			f.made = append(f.made, rel.ID())
			return f.ts.PutRelationshipsBatch([]*types.Relationship{rel})
		}, relsMade},
		{"ReplaceRelationship", func(t *testing.T, f *doorFix, r int) error { return f.ts.ReplaceRelationship(relWith(f, 0, r)) },
			func(t *testing.T, f *doorFix) {
				rel, err := f.ts.GetRelationship(f.rels[0].ID())
				var v any
				if err == nil {
					v, _ = rel.GetProperty("door")
				}
				doorProp(t, v, err, 1)
			}},
		{"DeleteRelationship", func(t *testing.T, f *doorFix, r int) error { return f.ts.DeleteRelationship(f.rels[1+r].ID()) }, relsGone},
		{"DeleteRelationshipsBatch", func(t *testing.T, f *doorFix, r int) error {
			return f.ts.DeleteRelationshipsBatch([]types.RelID{f.rels[1+r].ID()})
		}, relsGone},
		// One history door per routing function.
		{"ReplaceNodeWithHistory", func(t *testing.T, f *doorFix, r int) error {
			cur, err := f.ts.GetNode(f.nodes[0].ID())
			if err != nil {
				return err
			}
			next := cur.DeepCopy()
			next.SetVersion(cur.Version() + 1)
			_ = next.SetProperty("door", r)
			return f.ts.ReplaceNodeWithHistory(next, cur.Version(), cur)
		}, func(t *testing.T, f *doorFix) {
			nodeVersions(t, f, 0, 1)
			n, err := f.ts.GetNode(f.nodes[0].ID())
			var v any
			if err == nil {
				v, _ = n.GetProperty("door")
				if n.Version() != 2 {
					t.Fatalf("node 0 version %d, want 2", n.Version())
				}
			}
			doorProp(t, v, err, 1)
		}},
		{"ReplaceRelWithHistory", func(t *testing.T, f *doorFix, r int) error {
			cur, err := f.ts.GetRelationship(f.rels[0].ID())
			if err != nil {
				return err
			}
			next := cur.DeepCopy()
			next.SetVersion(cur.Version() + 1)
			_ = next.SetProperty("door", r)
			return f.ts.ReplaceRelWithHistory(next, cur.Version(), cur)
		}, func(t *testing.T, f *doorFix) {
			relVersions(t, f, 0, 1)
			rel, err := f.ts.GetRelationship(f.rels[0].ID())
			var v any
			if err == nil {
				v, _ = rel.GetProperty("door")
				if rel.Version() != 2 {
					t.Fatalf("relationship 0 version %d, want 2", rel.Version())
				}
			}
			doorProp(t, v, err, 1)
		}},
		{"PutNodeVersion", func(t *testing.T, f *doorFix, r int) error {
			v := nodeWith(f, 0, r)
			v.SetVersion(uint32(10 + r))
			return f.ts.PutNodeVersion(v.ID(), v.Version(), v)
		}, func(t *testing.T, f *doorFix) { nodeVersions(t, f, 10, 11) }},
		{"PutRelVersion", func(t *testing.T, f *doorFix, r int) error {
			v := relWith(f, 0, r)
			v.SetVersion(uint32(10 + r))
			return f.ts.PutRelVersion(v.ID(), v.Version(), v)
		}, func(t *testing.T, f *doorFix) { relVersions(t, f, 10, 11) }},
	}
}

// TestColdGrow_EveryWriteDoorPends (M1): every write door of the store that
// routes by ID checks the cold shard out writable through the one checkout
// that owes the grow (writableColdLocked), as PutNode does. On a small handle
// that has committed past the limit one call puts the shard in the sweep's
// pending set (the sweep's pass held off meanwhile, so the set is read before
// a pass takes it), a pass closes it (sweep closes +1), and the same door
// again opens the shard at MemTableSize (WAL 2 x 16 MiB); what both calls
// wrote reads back. PutNode is the row that was green before. Control:
// WarmMemTableSize = MemTableSize, not pending, no close, WAL 2 x 16 MiB
// throughout, the same answers.
func TestColdGrow_EveryWriteDoorPends(t *testing.T) {
	t.Parallel()
	doors := append([]coldDoor{{"PutNode", func(t *testing.T, f *doorFix, r int) error {
		return f.putErr(t, 1)
	}, func(t *testing.T, f *doorFix) { f.rows(t) }}}, coldDoors()...)
	for _, d := range doors {
		for _, warm := range []int64{0, cgHot} {
			t.Run(fmt.Sprintf("%s WarmMemTableSize %d MiB", d.name, warm>>20), func(t *testing.T) {
				t.Parallel()
				f := newDoorFix(t, warm, nil)
				if want := warm == 0; f.owed() != want {
					t.Fatalf("fixture: grow owed %v, want %v", f.owed(), want)
				}
				wantWAL := int64(2 * coldMemTableSize)
				if warm != 0 {
					wantWAL = 2 * cgHot
				}
				if w := f.wal(t); w != wantWAL {
					t.Fatalf("fixture: WAL %d, want %d", w, wantWAL)
				}
				base := f.closes()
				f.ts.sweep.passMu.Lock() // a kicked pass waits until the set is read
				f.ts.takeSweepPending()
				f.ts.SetSweepFaultForTest(nil)
				err := d.call(t, f, 0)
				pending := f.pending()
				f.ts.sweep.passMu.Unlock()
				if err != nil {
					t.Fatalf("%s: %v", d.name, err)
				}
				f.ts.SweepHandlesForTest()
				closes := f.closes() - base
				if err := d.call(t, f, 1); err != nil {
					t.Fatalf("%s again: %v", d.name, err)
				}
				wal := f.wal(t)
				t.Logf("pending %v, closes +%d, WAL after the second call %d", pending, closes, wal)
				if warm == 0 && (!pending || closes != 1 || wal != 2*cgHot) {
					t.Errorf("pending %v, closes +%d, WAL %d; want pending, +1, 2 x %d", pending, closes, wal, cgHot)
				}
				if warm != 0 && (pending || closes != 0 || wal != 2*cgHot) {
					t.Errorf("control: pending %v, closes +%d, WAL %d; want not pending, +0, 2 x %d", pending, closes, wal, cgHot)
				}
				d.check(t, f)
				f.rows(t)
			})
		}
	}
}

// TestColdGrow_CapZeroByIDWriteNeverGrows (M1): at MaxOpenColdShards 0 a
// by-ID write's handle closes at its check-in, so it never commits the grow
// limit: each write opens the closed shard again at the small size (ColdOpens
// +1 a write, every writable open 8 MiB), and the sweep closes nothing.
func TestColdGrow_CapZeroByIDWriteNeverGrows(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	f := newColdGrow(t, 0, nil)
	f.put(t, 1) // a row to replace, on the small handle a create keeps
	f.idleClose(t)
	ids, err := f.ts.NodesByLabel(f.tok, QueryOpts{})
	if err != nil || len(ids) != 2 {
		t.Fatalf("rows %d (%v), want 2", len(ids), err)
	}
	n := ids[0]
	es := f.shard()
	opens0, base := es.coldOpens.Load(), f.closes()
	writable := f.writableOpens()
	const writes = 6 // 1.5 MiB in all, past the limit
	for w := 0; w < writes; w++ {
		next := n.DeepCopy()
		iv, _ := n.GetProperty("i")
		i, _ := toInt(iv)
		if err := next.SetProperties(mustTieredProps(t, map[string]any{"i": i, "pad": cgPad(i), "w": w})); err != nil {
			t.Fatal(err)
		}
		if err := f.ts.ReplaceNode(next); err != nil {
			t.Fatalf("ReplaceNode %d: %v", w, err)
		}
		if s, _ := es.StoreStateForTest(); s != nil {
			t.Fatalf("write %d: the handle stayed open past its check-in", w)
		}
	}
	f.ts.SweepHandlesForTest()
	opens := writable()
	t.Logf("cold opens +%d, writable opens %v, sweep closes +%d", es.coldOpens.Load()-opens0, opens, f.closes()-base)
	if got := es.coldOpens.Load() - opens0; got != writes {
		t.Errorf("cold opens +%d, want +%d", got, writes)
	}
	if len(opens) != writes {
		t.Errorf("writable opens %v, want %d", opens, writes)
	}
	for _, m := range opens {
		if m != coldMemTableSize {
			t.Errorf("writable opens %v, want every one at %d", opens, coldMemTableSize)
			break
		}
	}
	if c := f.closes() - base; c != 0 {
		t.Errorf("sweep closes +%d, want +0", c)
	}
	got, err := f.ts.GetNode(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := got.GetProperty("w"); fmt.Sprint(v) != fmt.Sprint(writes-1) {
		t.Fatalf("w %v, want %d", v, writes-1)
	}
	f.rows(t)
}

// TestWritableCold_WakesNoRowsOnOpenHandle (M1 sibling): a write routed by
// ID (routeWrite) onto a cold shard whose writable handle is open but still
// carries the no-rows mark (the retention drop's rollback installs one) wakes
// the mark, in memory and in the saved catalog, before it writes: the label
// door and the history read find the rows. Control: the mark not set, the
// same rows.
func TestWritableCold_WakesNoRowsOnOpenHandle(t *testing.T) {
	t.Parallel()
	for _, marked := range []bool{true, false} {
		t.Run(fmt.Sprintf("marked %v", marked), func(t *testing.T) {
			t.Parallel()
			f := newColdGrow(t, 0, nil)
			f.put(t, 1) // the writable handle stays open
			es := f.shard()
			if s, ro := es.StoreStateForTest(); s == nil || ro {
				t.Fatal("fixture: day 10 not open writable")
			}
			ns, err := f.ts.NodesByLabel(f.tok, QueryOpts{})
			if err != nil || len(ns) != 2 {
				t.Fatalf("fixture: rows %d (%v), want 2", len(ns), err)
			}
			if marked {
				es.shardMu.Lock()
				es.noRows.Store(true)
				f.ts.catalog.SetShardNoRows(es.name, true)
				err := f.ts.catalog.Save()
				es.shardMu.Unlock()
				if err != nil || !f.ts.catalogEntryNoRows(t, Config{DataDir: f.dir}, es.name) {
					t.Fatalf("fixture: mark not saved (%v)", err)
				}
			}
			// A row put through the routeWrite seam, then a history version
			// through its door (PutNodeVersion, routeWrite).
			id, _, err := f.ts.MintNodeIDAt(f.tok, ms(julDay(10, 5)), true)
			if err != nil {
				t.Fatal(err)
			}
			row := types.NewNode(id, f.tok, nil)
			if err := row.SetProperties(mustTieredProps(t, map[string]any{"i": f.next, "pad": cgPad(f.next)})); err != nil {
				t.Fatal(err)
			}
			f.next++
			store, checkin, err := f.ts.shardForNodeIDChecked(id)
			if err != nil {
				t.Fatal(err)
			}
			err = store.PutNode(row)
			checkin()
			if err != nil {
				t.Fatal(err)
			}
			v := ns[0].DeepCopy()
			v.SetVersion(7)
			if err := f.ts.PutNodeVersion(v.ID(), 7, v); err != nil {
				t.Fatal(err)
			}
			if err := f.ts.Flush(); err != nil {
				t.Fatal(err)
			}
			if es.noRows.Load() || f.ts.catalogEntryNoRows(t, Config{DataDir: f.dir}, es.name) {
				t.Errorf("after the write: mark in memory %v, in the saved catalog %v; want both cleared", es.noRows.Load(), f.ts.catalogEntryNoRows(t, Config{DataDir: f.dir}, es.name))
			}
			if n := f.rows(t); n != 3 {
				t.Errorf("label rows %d, want 3", n)
			}
			hist, err := f.ts.GetNodeHistory(v.ID())
			if err != nil {
				t.Fatal(err)
			}
			if len(hist) == 0 || hist[len(hist)-1].Version() != 7 {
				t.Errorf("history of the versioned node %d entries, want version 7 last", len(hist))
			}
		})
	}
}

// TestHandleSweep_NoSwapAtEqualWarmSize (T9, S1): WarmMemTableSize equal to
// MemTableSize owes the rotated-out shard no swap: the reopen would take the
// same arena. Control: WarmMemTableSize 8 MiB swaps once.
func TestHandleSweep_NoSwapAtEqualWarmSize(t *testing.T) {
	testtier.Slow(t)
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
