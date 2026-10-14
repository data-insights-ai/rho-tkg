package core

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 28 (sigma-tkgd 2026-10-09): the state form {ValidAt, TxAt} of the
// count and column doors (CountByLabelAt / CountByTypeAt, ScanNodeColumns /
// ScanRelColumns) answers from a cached effective cut of the label or type at
// the pin, built once, instead of folding every candidate's rows per call.
// The oracle is the row fold itself: ByLabel / ByType under the same options,
// and the row column driver over its rows, with no ordinals (a read
// coordinate numbers no row: the winning row's stored ordinal changed when a
// later write moved or deleted it, so the answer at a pin changed).

// stOracle renders what the count and column doors must answer at opts: the
// fold's length and its rows through the row column driver ("ok=false" for
// a store without the column scan).
func stOracle(t *testing.T, e *ccEnt, opts storepkg.QueryOpts, props []string) (int, string) {
	t.Helper()
	var b strings.Builder
	if e.rel {
		rels, err := e.g.Rels.ByType(ccType, opts)
		if err != nil {
			t.Fatalf("ByType%+v: %v", opts, err)
		}
		if _, has := e.g.store.(storepkg.RelColumnScanCapability); !has {
			return len(rels), "ok=false"
		}
		err = storepkg.ScanColumnsFromRels(rels, props, func(rb *storepkg.RelColumnBatch) bool {
			rb.RelType = ccType
			clear(rb.Ordinals)
			clear(rb.StartOrdinals)
			clear(rb.EndOrdinals)
			fmt.Fprintf(&b, "%+v\n", *rb)
			return true
		})
		fmt.Fprintf(&b, "err=%v", err)
		return len(rels), b.String()
	}
	nodes, err := e.g.Nodes.ByLabel(ccLabel, opts)
	if err != nil {
		t.Fatalf("ByLabel%+v: %v", opts, err)
	}
	if _, has := e.g.store.(storepkg.NodeColumnScanCapability); !has {
		return len(nodes), "ok=false"
	}
	err = storepkg.ScanColumnsFromNodes(nodes, props, func(nb *storepkg.ColumnBatch) bool {
		clear(nb.Ordinals)
		fmt.Fprintf(&b, "%+v\n", *nb)
		return true
	})
	fmt.Fprintf(&b, "err=%v", err)
	return len(nodes), b.String()
}

// stDoors renders the count and the column scan the doors answer at opts
// (the error of either in place of the scan).
func stDoors(e *ccEnt, opts storepkg.QueryOpts, props []string) (int, string) {
	var b strings.Builder
	var n int
	var err error
	ok := true
	if e.rel {
		if n, err = e.g.Rels.CountByTypeAt(ccType, opts); err == nil {
			ok, err = e.g.ScanRelColumns(ccType, props, opts, func(rb *storepkg.RelColumnBatch) bool {
				fmt.Fprintf(&b, "%+v\n", *rb)
				return true
			})
		}
	} else if n, err = e.g.Nodes.CountByLabelAt(ccLabel, opts); err == nil {
		ok, err = e.g.ScanNodeColumns(ccLabel, props, opts, func(nb *storepkg.ColumnBatch) bool {
			fmt.Fprintf(&b, "%+v\n", *nb)
			return true
		})
	}
	if !ok && err == nil {
		return n, "ok=false"
	}
	fmt.Fprintf(&b, "err=%v", err)
	return n, b.String()
}

var stProps = []string{"x", "absent"}

// stCheck asserts the doors equal the fold at every valid instant of valids
// and every pin of pins (the state form, paged, and two forms the fold
// serves).
func stCheck(t *testing.T, e *ccEnt, phase string, pins, valids []types.Instant) {
	t.Helper()
	for _, pin := range pins {
		for _, v := range valids {
			for _, opts := range []storepkg.QueryOpts{
				{ValidAt: v, TxAt: pin},
				{ValidAt: v, TxAt: pin, After: types.EntityID(e.tracked()[1]), Limit: 2},
				{TxPin: pin}, // the fold: no ordinals either
				{ValidStart: v, ValidEnd: v + 1000, TxAt: pin}, // the fold
			} {
				wn, ws := stOracle(t, e, opts, stProps)
				gn, gs := stDoors(e, opts, stProps)
				if gn != wn || gs != ws {
					t.Fatalf("[%s %s] %+v: doors differ from the row fold:\n count %d, want %d\n scan %s\n want %s",
						e.kind(), phase, opts, gn, wn, gs, ws)
				}
			}
		}
	}
}

// stDecodes counts the badger rows decoded by one state count and one state
// column scan of the tracked label or type at (v, pin).
func stDecodes(t *testing.T, e *ccEnt, v, pin types.Instant) (count, scan int64) {
	t.Helper()
	opts := storepkg.QueryOpts{ValidAt: v, TxAt: pin}
	count = badgerpkg.CountRowDecodesForTest(func() {
		var err error
		if e.rel {
			_, err = e.g.Rels.CountByTypeAt(ccType, opts)
		} else {
			_, err = e.g.Nodes.CountByLabelAt(ccLabel, opts)
		}
		if err != nil {
			t.Fatalf("count: %v", err)
		}
	})
	scan = badgerpkg.CountRowDecodesForTest(func() {
		var err error
		if e.rel {
			_, err = e.g.ScanRelColumns(ccType, stProps, opts, func(*storepkg.RelColumnBatch) bool { return true })
		} else {
			_, err = e.g.ScanNodeColumns(ccLabel, stProps, opts, func(*storepkg.ColumnBatch) bool { return true })
		}
		if err != nil {
			t.Fatalf("scan: %v", err)
		}
	})
	return count, scan
}

// stBulk adds n tracked entities in batches, pins, then updates every tenth
// one after the pin (sigma's measured shape) and flushes.
func stBulk(t *testing.T, e *ccEnt, n int) types.Instant {
	t.Helper()
	const chunk = 10_000
	var ids []int64
	for at := 0; at < n; at += chunk {
		b, err := NewBatchBuilder(e.g)
		if err != nil {
			t.Fatalf("NewBatchBuilder: %v", err)
		}
		var start, end *types.Node
		if e.rel {
			if start, err = e.g.Nodes.Get(e.ctx, e.start); err == nil {
				end, err = e.g.Nodes.Get(e.ctx, e.end)
			}
			if err != nil {
				t.Fatalf("endpoints: %v", err)
			}
		}
		for i := at; i < min(at+chunk, n); i++ {
			props := map[string]any{"tkg_valid_from": types.Instant(1000), "x": int64(i)}
			if e.rel {
				r, err := b.AddRelationship(ccType, start, end, props)
				if err != nil {
					t.Fatalf("AddRelationship: %v", err)
				}
				ids = append(ids, int64(r.ID()))
			} else {
				nd, err := b.AddNode([]string{ccLabel}, props)
				if err != nil {
					t.Fatalf("AddNode: %v", err)
				}
				ids = append(ids, int64(nd.ID()))
			}
		}
		if res, err := b.Execute(); err != nil || res.Failed != 0 {
			t.Fatalf("Execute: %+v %v", res, err)
		}
	}
	pin := e.pin()
	b, err := NewBatchBuilder(e.g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	for i := 0; i < len(ids); i += 10 {
		if e.rel {
			err = b.UpdateRelationship(types.RelID(ids[i]), map[string]any{"x": int64(-i)})
		} else {
			err = b.UpdateNode(types.NodeID(ids[i]), map[string]any{"x": int64(-i)})
		}
		if err != nil {
			t.Fatalf("queue update: %v", err)
		}
	}
	if res, err := b.Execute(); err != nil || res.Failed != 0 {
		t.Fatalf("Execute updates: %+v %v", res, err)
	}
	estFlush(t, e.g)
	return pin
}

// TestCountByLabelAtStateColumnPath: on badger, node and relationship, the
// rows a state count and a state column scan decode after the first build are
// the same for 10 K and 100 K entities (1 K and 10 K in -short), and the
// answers equal the row fold. Break cases: TestStateColumnPath_BreakCases,
// TestStateColumnPath_Eviction, TestStateColumnPath_ConcurrentWrites.
func TestCountByLabelAtStateColumnPath(t *testing.T) {
	sizes := []int{10_000, 100_000}
	if testing.Short() {
		sizes = []int{1_000, 10_000}
	}
	for _, rel := range []bool{false, true} {
		t.Run(map[bool]string{false: "node", true: "rel"}[rel], func(t *testing.T) {
			var (
				decodes [][2]int64
				differ  []string
			)
			for _, n := range sizes {
				st, err := badgerpkg.New(badgerpkg.Config{InMemory: true, CacheCapacity: 1, FlushInterval: 1<<63 - 1})
				if err != nil {
					t.Fatalf("badger.New: %v", err)
				}
				g, err := New(Config{Store: st})
				if err != nil {
					t.Fatalf("New: %v", err)
				}
				useTestClock(t, g)
				e := newCCEnt(t, g, rel)
				pin := stBulk(t, e, n)
				v := g.readNow()
				stDecodes(t, e, v, pin) // the first build
				c, s := stDecodes(t, e, v+1, pin)
				decodes = append(decodes, [2]int64{c, s})
				opts := storepkg.QueryOpts{ValidAt: v, TxAt: pin}
				wn, ws := stOracle(t, e, opts, stProps)
				gn, gs := stDoors(e, opts, stProps)
				if gn != wn || gs != ws || wn != n {
					differ = append(differ, fmt.Sprintf("n=%d: count %d, fold %d, scan equal to the fold: %v", n, gn, wn, gs == ws))
				}
				_ = g.Close()
			}
			t.Logf("rows decoded per state count / scan after the first build: %d entities %v, %d entities %v",
				sizes[0], decodes[0], sizes[1], decodes[1])
			if decodes[0] != decodes[1] {
				t.Errorf("rows decoded per state count / scan grow with the label: %v at %d, %v at %d; want constant",
					decodes[0], sizes[0], decodes[1], sizes[1])
			}
			if differ != nil {
				t.Errorf("doors differ from the row fold: %s", strings.Join(differ, "; "))
			}
		})
	}
}

// stScenario builds the break-case entities: live (A), updated before the pin
// (B), corrected later (C), deleted (D), retracted (E), deleted and
// re-imported (F), scheduled to close then deleted (G, backlog 25), added
// after the pin (H), and backfilled below the pin (I). The doors are read at
// pinA before the later writes, so the later reads at pinA meet the cut that
// first read built.
func stScenario(t *testing.T, e *ccEnt, valids []types.Instant) (pinA, pinB types.Instant) {
	t.Helper()
	ids := map[string]int64{}
	for _, name := range []string{"A", "B", "C", "D", "E", "F", "G", "I"} {
		ids[name] = e.add(name, 1000, nil)
	}
	e.mustUpdate(ids["B"], map[string]any{"x": int64(1)})
	if err := e.closeAt(ids["G"], 1<<42); err != nil {
		t.Fatalf("closeAt: %v", err)
	}
	pinA = e.pin()
	stCheck(t, e, "before the later writes", []types.Instant{pinA}, valids)

	e.mustCascade(ids["C"], 2000, 3000, map[string]any{"x": int64(2)})
	e.mustDel(ids["D"])
	var err error
	if e.rel {
		err = e.g.Rels.Retract(e.ctx, types.RelID(ids["E"]))
	} else {
		err = e.g.Nodes.Retract(e.ctx, types.NodeID(ids["E"]))
	}
	if err != nil {
		t.Fatalf("Retract: %v", err)
	}
	e.mustDel(ids["F"])
	e.mustReimport(ids["F"], map[string]any{"tkg_valid_from": types.Instant(4000), "x": int64(3)})
	e.mustDel(ids["G"])
	e.add("H", 1000, nil)
	pinB = e.pin()
	stCheck(t, e, "after the later writes", []types.Instant{pinA, pinB}, valids)

	// A past-dated write below pinA changes the state at pinA.
	at := pinA - 1
	if e.rel {
		_, err = e.g.Rels.UpdateWithTx(e.ctx, types.RelID(ids["I"]), map[string]any{"x": int64(4)}, at)
	} else {
		_, err = e.g.Nodes.UpdateWithTx(e.ctx, types.NodeID(ids["I"]), map[string]any{"x": int64(4)}, at)
	}
	if err != nil {
		t.Fatalf("UpdateWithTx: %v", err)
	}
	stCheck(t, e, "after a backfill below the pin", []types.Instant{pinA, pinB}, valids)
	return pinA, pinB
}

func stValids(e *ccEnt) []types.Instant {
	return []types.Instant{500, 1000, 1999, 2000, 2500, 3000, 4000, e.g.readNow(), 1<<42 - 1, 1 << 42}
}

// TestStateColumnPath_BreakCases: on memory, badger, sharded and tiered, node
// and relationship, the state count and column scan equal the row fold at
// every valid instant before and after a correction at a later tx, a delete,
// a retraction, a re-import, a scheduled close then delete, an add and a
// backfill below the pin; on badger a repeated read decodes no row.
func TestStateColumnPath_BreakCases(t *testing.T) {
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		pinA, pinB := stScenario(t, e, stValids(e))
		if _, isBadger := e.g.store.(*badgerpkg.Store); !isBadger {
			return
		}
		estFlush(t, e.g)
		for _, pin := range []types.Instant{pinA, pinB} {
			stDecodes(t, e, e.g.readNow(), pin)
			if c, s := stDecodes(t, e, e.g.readNow(), pin); c != 0 || s != 0 {
				t.Fatalf("pin %d: a repeated state count decoded %d rows, a scan %d; want 0", pin, c, s)
			}
		}
	})
}

// TestStateColumnPath_ConcurrentWrites: readers at a fixed pin while writers
// add, update and delete above it answer, every time, what the row fold
// answers at that pin once the writers stopped (a pinned state is stable
// under forward writes), on every backend, node and relationship.
func TestStateColumnPath_ConcurrentWrites(t *testing.T) {
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		var ids []int64
		for i := 0; i < 40; i++ {
			ids = append(ids, e.add(fmt.Sprintf("E%02d", i), 1000, nil))
		}
		pin := e.pin()
		v := e.g.readNow()
		var (
			mu  sync.Mutex
			got []string
			wg  sync.WaitGroup
		)
		for r := 0; r < 3; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < 15; i++ {
					n, s := stDoors(e, storepkg.QueryOpts{ValidAt: v, TxAt: pin}, stProps)
					mu.Lock()
					got = append(got, fmt.Sprintf("%d\n%s", n, s))
					mu.Unlock()
				}
			}()
		}
		for i, id := range ids {
			switch i % 3 {
			case 0:
				e.mustUpdate(id, map[string]any{"x": int64(i)})
			case 1:
				e.mustDel(id)
			default:
				e.add(fmt.Sprintf("N%02d", i), 1000, nil)
			}
		}
		wg.Wait()
		wn, ws := stOracle(t, e, storepkg.QueryOpts{ValidAt: v, TxAt: pin}, stProps)
		want := fmt.Sprintf("%d\n%s", wn, ws)
		for _, g := range got {
			if g != want {
				t.Fatalf("[%s] a read during the writes differs from the fold at the pin:\n got  %s\n want %s", e.kind(), g, want)
			}
		}
	})
}

// setStateCutBudgetForTest sets the cut budget for the test's duration.
func (a *asOfColumnCache) setStateCutBudgetForTest(t testing.TB, budget int64) {
	t.Helper()
	a.mu.Lock()
	prev := a.cutBudget
	a.cutBudget = budget
	a.evictCutsLocked(budget)
	a.mu.Unlock()
	t.Cleanup(func() {
		a.mu.Lock()
		a.cutBudget = prev
		a.mu.Unlock()
	})
}

// TestStateColumnPath_Eviction: with a budget that holds one cut, reads
// alternating between two pins evict each other's cut and keep equal to the
// row fold (an evicted cut is rebuilt, never served stale); the held bytes
// stay within the budget; a budget below one cut keeps none and still
// answers; Reset of the cache by exact erasure (clear) drops every cut.
func TestStateColumnPath_Eviction(t *testing.T) {
	ccRun(t, true, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		pinA, pinB := stScenario(t, e, stValids(e))
		held := e.g.asOfColumns.stateCutBytes()
		if held <= 0 {
			t.Fatalf("no cut held after the scenario (%d bytes)", held)
		}
		one := e.g.asOfColumns.cuts[stateCutKey{rel: e.rel, tok: stTok(t, e), pin: pinA}].bytes()
		e.g.asOfColumns.setStateCutBudgetForTest(t, one)
		valids := []types.Instant{1000, 2500, e.g.readNow()}
		for round := 0; round < 3; round++ {
			stCheck(t, e, fmt.Sprintf("eviction round %d", round), []types.Instant{pinA, pinB, pinA}, valids)
			if held := e.g.asOfColumns.stateCutBytes(); held > one {
				t.Fatalf("cuts hold %d bytes over a budget of %d", held, one)
			}
		}
		e.g.asOfColumns.setStateCutBudgetForTest(t, 1)
		stCheck(t, e, "budget below one cut", []types.Instant{pinA, pinB}, valids)
		if held := e.g.asOfColumns.stateCutBytes(); held != 0 {
			t.Fatalf("a budget below one cut holds %d bytes", held)
		}
		e.g.asOfColumns.setStateCutBudgetForTest(t, stateCutBudget)
		stCheck(t, e, "budget restored", []types.Instant{pinA}, valids)
		e.g.asOfColumns.clear()
		if held, n := e.g.asOfColumns.stateCutBytes(), len(e.g.asOfColumns.cuts); held != 0 || n != 0 {
			t.Fatalf("clear left %d cuts, %d bytes", n, held)
		}
	})
}

// stTok is the tracked label's or type's token.
func stTok(t *testing.T, e *ccEnt) uint16 {
	t.Helper()
	tok, ok := e.g.stateCutToken(e.rel, map[bool]string{false: ccLabel, true: ccType}[e.rel])
	if !ok {
		t.Fatalf("no token for the tracked %s", e.kind())
	}
	return tok
}

// TestStateColumnPath_InPlaceUpdate: UpdateInPlace rewrites the row a cached
// cut was built from (no history row); the next state read equals the fold.
func TestStateColumnPath_InPlaceUpdate(t *testing.T) {
	ccRun(t, false, func(t *testing.T, mk func(t *testing.T) *ccEnt) {
		e := mk(t)
		id := e.add("A", 1000, nil)
		e.add("B", 1000, nil)
		pin := e.pin()
		valids := []types.Instant{2000}
		stCheck(t, e, "before", []types.Instant{pin}, valids) // builds the cut
		var err error
		if e.rel {
			_, err = e.g.Rels.UpdateInPlace(e.ctx, types.RelID(id), map[string]any{"x": int64(9)})
		} else {
			_, err = e.g.Nodes.UpdateInPlace(e.ctx, types.NodeID(id), map[string]any{"x": int64(9)})
		}
		if err != nil {
			t.Fatalf("UpdateInPlace: %v", err)
		}
		stCheck(t, e, "after UpdateInPlace", []types.Instant{pin}, valids)
	})
}

// TestStateColumnPath_CountInsideScanCallback: a state count called from a
// fold-path scan callback (which runs under the graph read lock) answers; the
// cut build never waits for the write lock.
func TestStateColumnPath_CountInsideScanCallback(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	useTestClock(t, g)
	e := newCCEnt(t, g, false)
	e.add("A", 1000, nil)
	pin := e.pin()
	done := make(chan int, 1)
	go func() {
		n := 0
		_, _ = g.ScanNodeColumns(ccLabel, nil, storepkg.QueryOpts{TxPin: pin}, func(*storepkg.ColumnBatch) bool {
			for range 2 { // the second ask builds the cut
				n, _ = g.Nodes.CountByLabelAt(ccLabel, storepkg.QueryOpts{ValidAt: 2000, TxAt: pin})
			}
			return true
		})
		done <- n
	}()
	select {
	case n := <-done:
		if n != 1 {
			t.Fatalf("count inside the callback: %d, want 1", n)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a state count inside a scan callback deadlocked")
	}
}

// TestStateColumnPath_OversizedBuildsOnce: a cut over the budget is built
// once per key and epoch (then the fold answers), not on every ask.
func TestStateColumnPath_OversizedBuildsOnce(t *testing.T) {
	g, err := New(Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	useTestClock(t, g)
	e := newCCEnt(t, g, false)
	e.add("A", 1000, nil)
	e.add("B", 1000, nil)
	pin := e.pin()
	g.asOfColumns.setStateCutBudgetForTest(t, 1)
	var reads int
	g.scanReadTestHook = func() { reads++ }
	for range 5 {
		if n, err := g.Nodes.CountByLabelAt(ccLabel, storepkg.QueryOpts{ValidAt: 2000, TxAt: pin}); err != nil || n != 2 {
			t.Fatalf("count %d, %v; want 2", n, err)
		}
	}
	if reads != 2 {
		t.Fatalf("5 asks of an oversized cut read %d entity timelines; want 2 (one build)", reads)
	}
}

// TestStateColumnPath_RelPurge: PurgeExpiredRels removes relationships a
// cached cut holds; the next state read at a pin above the watermark equals
// the fold.
func TestStateColumnPath_RelPurge(t *testing.T) {
	g, err := New(Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	useTestClock(t, g)
	e := newCCEnt(t, g, true)
	e.add("A", 1000, nil)
	e.add("B", 1000, nil)
	pin := e.pin()
	stCheck(t, e, "before the purge", []types.Instant{pin}, []types.Instant{2000}) // builds the cut
	if _, err := g.Admin.PurgeExpiredRels(e.ctx, RelPurgePolicy{Type: ccType, Before: pin}); err != nil {
		t.Fatalf("PurgeExpiredRels: %v", err)
	}
	stCheck(t, e, "after the purge", []types.Instant{pin}, []types.Instant{2000})
}
