package badger

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// The read side of the as-of index: an indexed read pins the store's
// committed view in one wbMu section and opens no Badger transaction. The
// native counters are process-wide: these tests do not run in parallel.
// Chains: version v covers the pins [(v+1)*100, (v+2)*100).

const asOfViewPayload = 8192

type asOfViewShape struct {
	name string
	cfg  func(t *testing.T) Config
}

func asOfViewShapes() []asOfViewShape {
	return []asOfViewShape{
		{"async", func(*testing.T) Config { return Config{InMemory: true, FlushInterval: time.Hour} }},
		{"sync writes", func(*testing.T) Config { return Config{InMemory: true, SyncWrites: true} }},
		{"sync writes on disk", func(t *testing.T) Config { return Config{Dir: t.TempDir(), SyncWrites: true} }},
	}
}

// asOfViewStore: chain 100 with versions 0..3 and chain 200 with versions 0
// and 1, all flushed.
func asOfViewStore(t *testing.T, k asOfBudgetKind, cfg Config) *Store {
	t.Helper()
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	asOfViewPut(t, bs, k, 100, 0, 4)
	asOfViewPut(t, bs, k, 200, 0, 2)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return bs
}

func asOfViewPut(t *testing.T, bs *Store, k asOfBudgetKind, id int64, from, to uint32) {
	t.Helper()
	for v := from; v < to; v++ {
		if err := k.put(bs, id, v, strings.Repeat("x", asOfViewPayload)); err != nil {
			t.Fatal(err)
		}
	}
}

type asOfViewSeams struct{ before, pinned int }

// asOfViewWork is a read's native work plus the as-of seek keys it visited:
// one per answer the index gave, none when the scan answered (visits counts
// the scan's walks of other indexes too).
type asOfViewWork struct {
	asOfIndexNativeWork
	seeks int64
}

func observeAsOfViewRead(bs *Store) func() asOfViewWork {
	seeks, stop := CountAsOfIndexSeekEntriesForTest()
	finish := observeAsOfIndexRead(bs)
	return func() asOfViewWork {
		defer stop()
		return asOfViewWork{finish(), seeks()}
	}
}

// othersFlushAtAsOfSeams makes every indexed read of bs meet, before its lock
// section and after it pinned the view, one more row of chain other appended
// and flushed and one direct commit (a MetaSet, as tiered's commit floor).
// The returned function removes the hook.
func othersFlushAtAsOfSeams(t *testing.T, bs *Store, k asOfBudgetKind, other int64, next uint32) (*asOfViewSeams, func()) {
	t.Helper()
	seen := &asOfViewSeams{}
	bs.asOfViewPinTestHook = func(pinned bool) {
		if pinned {
			seen.pinned++
		} else {
			seen.before++
		}
		if err := k.put(bs, other, next, strings.Repeat("x", asOfViewPayload)); err != nil {
			t.Error(err)
			return
		}
		next++
		if err := bs.Flush(); err != nil {
			t.Error(err)
		}
		if err := bs.MetaSet("floor", []byte{byte(next)}); err != nil {
			t.Error(err)
		}
	}
	return seen, func() { bs.asOfViewPinTestHook = nil }
}

func asOfViewBudget(w storecontract.ReadWork) storecontract.ReadBudget {
	return storecontract.ReadBudget{Candidates: w.Candidates, HistoryExamined: w.HistoryExamined, DecodedBytes: w.DecodedBytes, BufferBytes: w.BufferBytes}
}

// asOfViewOneLess returns, per dimension the control charged more than one
// unit of, the control's exact budget with that dimension one unit lower.
func asOfViewOneLess(w storecontract.ReadWork) map[string]storecontract.ReadBudget {
	out := map[string]storecontract.ReadBudget{}
	for name, lower := range map[string]func(*storecontract.ReadBudget) *uint64{
		"candidates":       func(b *storecontract.ReadBudget) *uint64 { return &b.Candidates },
		"history examined": func(b *storecontract.ReadBudget) *uint64 { return &b.HistoryExamined },
		"decoded bytes":    func(b *storecontract.ReadBudget) *uint64 { return &b.DecodedBytes },
		"buffer bytes":     func(b *storecontract.ReadBudget) *uint64 { return &b.BufferBytes },
	} {
		b := asOfViewBudget(w)
		if p := lower(&b); *p > 1 {
			*p--
			out[name] = b
		}
	}
	return out
}

func asOfViewAbsent(err error) bool {
	return errors.Is(err, ErrVersionNotFound) || errors.Is(err, ErrNodeNotFound) || errors.Is(err, ErrRelNotFound)
}

// TestAsOfQuietChainReadsIndexBesideOtherFlushes: a read of a chain with
// nothing buffered answers through the index whatever other chains flush and
// whatever commits directly around it: at both of its seams another chain is
// appended and flushed and a meta key committed. Its work, on the meter and
// on the native counters, is the quiet control's, so a budget equal to that
// control admits it and one unit less in any dimension refuses it. Before,
// the reader checked its own transaction against commitGen, which every
// flush moves: it declined, took the scan and was refused at this budget.
// The history-bounds door (the whole-axis write check's read) likewise.
func TestAsOfQuietChainReadsIndexBesideOtherFlushes(t *testing.T) {
	for _, shape := range asOfViewShapes() {
		for _, k := range asOfBudgetKinds() {
			t.Run(shape.name+"/"+k.name+"/point", func(t *testing.T) {
				bs := asOfViewStore(t, k, shape.cfg(t))
				read := func(b storecontract.ReadBudget) (storecontract.ReadWork, asOfViewWork, error) {
					t.Helper()
					_, m := storecontract.WithReadBudget(t.Context(), b)
					finish := observeAsOfViewRead(bs)
					v, ok, err := k.read(bs, 100, 250, m.Tally("quiet chain"))
					native := finish()
					if err == nil && (!ok || v != 1) {
						t.Fatalf("pin 250: v%d exact payload %v, want v1", v, ok)
					}
					return m.Work(), native, err
				}
				control, native, err := read(storecontract.ReadBudget{})
				if err != nil || native.seeks != 1 || native.rows != 1 || control.Candidates == 0 || control.HistoryExamined == 0 || control.DecodedBytes == 0 || control.BufferBytes == 0 {
					t.Fatalf("control: work %+v native %+v err %v, want one seek key and work in every dimension", control, native, err)
				}
				seen, stop := othersFlushAtAsOfSeams(t, bs, k, 200, 2)
				defer stop()
				w, got, err := read(asOfViewBudget(control))
				if err != nil || w != control || got != native || *seen != (asOfViewSeams{1, 1}) {
					t.Fatalf("beside other chains' flushes, under the control's budget: work %+v native %+v seams %+v err %v; control %+v native %+v, one pass through both seams", w, got, *seen, err, control, native)
				}
				for name, b := range asOfViewOneLess(control) {
					if _, _, err := read(b); !errors.Is(err, storecontract.ErrReadBudget) {
						t.Fatalf("one %s less than the control: %v, want ErrReadBudget", name, err)
					}
				}
			})
			t.Run(shape.name+"/"+k.name+"/bounds", func(t *testing.T) {
				bs := asOfViewStore(t, k, shape.cfg(t))
				read := func(b storecontract.ReadBudget) (historyBoundsAnswer, storecontract.ReadWork, error) {
					t.Helper()
					_, m := storecontract.WithReadBudget(t.Context(), b)
					a, err := historyBoundsRead(t, bs, k, 100, m.Tally("quiet chain's bounds"))
					return a, m.Work(), err
				}
				want := historyBoundsAnswer{version: 3, newest: 500, known: true}
				a, control, err := read(storecontract.ReadBudget{})
				if err != nil || a != want || control.Candidates != 2 {
					t.Fatalf("control: %+v work %+v err %v, want %+v from two probes", a, control, err, want)
				}
				seen, stop := othersFlushAtAsOfSeams(t, bs, k, 200, 2)
				defer stop()
				a, w, err := read(asOfViewBudget(control))
				if err != nil || a != want || w != control || *seen != (asOfViewSeams{1, 1}) {
					t.Fatalf("beside other chains' flushes: %+v work %+v seams %+v err %v; want %+v, work %+v", a, w, *seen, err, want, control)
				}
				for name, b := range asOfViewOneLess(control) {
					if _, _, err := read(b); !errors.Is(err, storecontract.ErrReadBudget) {
						t.Fatalf("one %s less than the control: %v, want ErrReadBudget", name, err)
					}
				}
			})
		}
	}
}

// TestAsOfIndexedReadOpensNoTransaction: an indexed as-of read and a
// history-bounds read open no Badger read transaction of their own, and at
// both seams their goroutine holds neither wbMu nor idxMu. (The point read's
// current-row lookup, before the index, opens one through view on a cache
// miss; it is not on the index's stack.)
func TestAsOfIndexedReadOpensNoTransaction(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			views, stopViews := countAsOfHookWork()
			defer stopViews()
			bs := asOfViewStore(t, k, Config{InMemory: true, FlushInterval: time.Hour})
			var opens, seams, locked int
			bs.readTxnOpenTestHook = func() {
				if onStackOf("indexedAsOf") || onStackOf("historyBounds") {
					opens++
				}
			}
			bs.asOfViewPinTestHook = func(bool) {
				seams++
				for _, mu := range []interface {
					TryLock() bool
					Unlock()
				}{&bs.wbMu, &bs.idxMu} {
					if !mu.TryLock() {
						locked++
						continue
					}
					mu.Unlock()
				}
			}
			defer func() { bs.readTxnOpenTestHook, bs.asOfViewPinTestHook = nil, nil }()
			finish := observeAsOfViewRead(bs)
			v, ok, err := k.read(bs, 100, 250, nil)
			if work := finish(); err != nil || !ok || v != 1 || work.seeks != 1 {
				t.Fatalf("pin 250: v%d ok %v work %+v err %v, want v1 through one seek key", v, ok, work, err)
			}
			if a, err := historyBoundsRead(t, bs, k, 100, nil); err != nil || !a.known {
				t.Fatalf("bounds: %+v err %v, want known", a, err)
			}
			if opens != 0 || seams != 4 || locked != 0 {
				t.Fatalf("two indexed reads: %d read transactions opened on the index's stack (want 0), %d seams (want 4), %d with wbMu or idxMu held (want 0)", opens, seams, locked)
			}
			// Both reads gave back their value-log slot and their view
			// reference: after a flush replaced the view, only the installed
			// one is alive.
			if a, b := bs.vlogReaders.active[0].Load(), bs.vlogReaders.active[1].Load(); a != 0 || b != 0 {
				t.Fatalf("value-log readers entered after the reads ended: %d and %d, want 0", a, b)
			}
			asOfViewPut(t, bs, k, 200, 2, 3)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if alive := views.snaps.Load() - views.ended.Load(); alive != 1 {
				t.Fatalf("%d views alive after the reads and a flush, want 1 (the installed one)", alive)
			}
		})
	}
}

type asOfViewResult struct {
	v    uint32
	ok   bool
	err  error
	work asOfViewWork
}

// holdAsOfRead starts one as-of read of id at pin and holds it at the given
// seam (before its lock section, or after it pinned the view) until release
// is closed. Reads that reach the seam later pass.
func holdAsOfRead(t *testing.T, bs *Store, k asOfBudgetKind, id int64, pin types.Instant, pinned bool) (release chan struct{}, out chan asOfViewResult) {
	t.Helper()
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	bs.asOfViewPinTestHook = func(p bool) {
		if p == pinned {
			once.Do(func() {
				close(held)
				<-release
			})
		}
	}
	t.Cleanup(func() { bs.asOfViewPinTestHook = nil })
	out = make(chan asOfViewResult, 1)
	go func() {
		finish := observeAsOfViewRead(bs)
		v, ok, err := k.read(bs, id, pin, nil)
		out <- asOfViewResult{v, ok, err, finish()}
	}()
	select {
	case <-held:
	case r := <-out:
		t.Fatalf("the as-of read did not reach the seam (pinned %v): %+v", pinned, r)
	case <-time.After(10 * time.Second):
		t.Fatal("the as-of read did not reach the seam")
	}
	return release, out
}

func asOfViewWithin(t *testing.T, release chan struct{}, what string, fn func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- fn() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		close(release)
		<-done
		t.Fatalf("%s waited for a held as-of read", what)
	}
}

// TestAsOfHeldReadAndItsNeighbours holds one indexed read at a seam and
// moves the store under it.
func TestAsOfHeldReadAndItsNeighbours(t *testing.T) {
	payload := strings.Repeat("x", asOfViewPayload)
	park := Config{InMemory: true, FlushInterval: time.Hour}
	for _, k := range asOfBudgetKinds() {
		for _, pinned := range []bool{false, true} {
			at := map[bool]string{false: "before the section", true: "after the pin"}[pinned]
			t.Run(k.name+"/writers pass a read held "+at, func(t *testing.T) {
				bs := asOfViewStore(t, k, park)
				release, out := holdAsOfRead(t, bs, k, 100, 250, pinned)
				asOfViewWithin(t, release, "a creator, an appender of another chain and a flush", func() error {
					n := types.NewNode(300, 1, nil)
					n.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 1})
					if err := bs.PutNode(n); err != nil {
						return err
					}
					if err := k.put(bs, 200, 2, payload); err != nil {
						return err
					}
					return bs.Flush()
				})
				close(release)
				if r := <-out; r.err != nil || !r.ok || r.v != 1 || r.work.seeks != 1 {
					t.Fatalf("held reader: %+v, want v1 through one seek key", r)
				}
			})
		}
		t.Run(k.name+"/own chain buffered", func(t *testing.T) {
			// Version 4 is buffered, in pending and then in flushing: the view
			// does not hold it, so the scan answers its pin, with no proof read.
			bs := asOfViewStore(t, k, park)
			if err := k.put(bs, 100, 4, payload); err != nil {
				t.Fatal(err)
			}
			read := func(b storecontract.ReadBudget) (storecontract.ReadWork, asOfViewWork, error) {
				t.Helper()
				_, m := storecontract.WithReadBudget(t.Context(), b)
				finish := observeAsOfViewRead(bs)
				v, ok, err := k.read(bs, 100, 550, m.Tally("buffered chain"))
				native := finish()
				if err == nil && (!ok || v != 4) {
					t.Fatalf("pin 550: v%d exact payload %v, want the buffered v4", v, ok)
				}
				return m.Work(), native, err
			}
			for _, where := range []string{"pending", "flushing"} {
				if where == "flushing" {
					parkPendingIntoFlushing(t, bs)
				}
				control, native, err := read(storecontract.ReadBudget{})
				if err != nil || native.probes != 0 || native.seeks != 0 || control.HistoryExamined == 0 {
					t.Fatalf("%s: work %+v native %+v err %v, want the scan's answer and no proof read", where, control, native, err)
				}
				if w, _, err := read(asOfViewBudget(control)); err != nil || w != control {
					t.Fatalf("%s, under the scan control's budget: work %+v err %v, want %+v", where, w, err, control)
				}
				for name, b := range asOfViewOneLess(control) {
					if _, _, err := read(b); !errors.Is(err, storecontract.ErrReadBudget) {
						t.Fatalf("%s, one %s less than the scan control: %v, want ErrReadBudget", where, name, err)
					}
				}
			}
		})
		t.Run(k.name+"/own chain appended before the section", func(t *testing.T) {
			bs := asOfViewStore(t, k, park)
			release, out := holdAsOfRead(t, bs, k, 100, 550, false)
			asOfViewWithin(t, release, "an appender to the read's own chain", func() error { return k.put(bs, 100, 4, payload) })
			close(release)
			if r := <-out; r.err != nil || !r.ok || r.v != 4 || r.work.probes != 0 || r.work.seeks != 0 {
				t.Fatalf("reader of a chain appended before its section: %+v, want the buffered v4 from the scan, no proof read", r)
			}
		})
		t.Run(k.name+"/own chain appended and flushed after the pin", func(t *testing.T) {
			// The read pinned the view when the chain ended at version 3: it
			// answers that state through the index while a flush replaces the
			// view under it. The next read sees version 4 through the index.
			bs := asOfViewStore(t, k, park)
			for _, round := range []struct {
				pin    types.Instant
				append uint32
				want   asOfViewResult
			}{
				{250, 4, asOfViewResult{v: 1, ok: true}},
				{650, 5, asOfViewResult{err: ErrVersionNotFound}}, // version 5 covers 600..699
			} {
				release, out := holdAsOfRead(t, bs, k, 100, round.pin, true)
				asOfViewWithin(t, release, "an appender to the read's own chain and a flush", func() error {
					if err := k.put(bs, 100, round.append, payload); err != nil {
						return err
					}
					return bs.Flush()
				})
				close(release)
				r := <-out
				if r.ok != round.want.ok || r.v != round.want.v || !errors.Is(r.err, round.want.err) || r.work.probes != 2 || r.work.seeks != 1 {
					t.Fatalf("pin %d, held across its own chain's flush: %+v, want %+v through one seek key", round.pin, r, round.want)
				}
				bs.asOfViewPinTestHook = nil
			}
			for pin, want := range map[types.Instant]uint32{550: 4, 650: 5} {
				assertAsOfIndexUsed(t, bs, k, 100, pin, want)
			}
		})
		t.Run(k.name+"/unknown chain", func(t *testing.T) {
			bs := asOfViewStore(t, k, park)
			seen, stop := othersFlushAtAsOfSeams(t, bs, k, 200, 2)
			defer stop()
			finish := observeAsOfViewRead(bs)
			_, ok, err := k.read(bs, 999, 250, nil)
			if work := finish(); ok || (err != nil && !asOfViewAbsent(err)) || work.probes != 1 || work.seeks != 0 || *seen != (asOfViewSeams{1, 1}) {
				t.Fatalf("unknown chain: ok %v err %v work %+v seams %+v, want absent after one proof probe", ok, err, work, *seen)
			}
		})
	}
}

// TestAsOfTwoReadersShareAView: two reads pin the same view and read it at
// the same time while a flush of another chain replaces it and a third chain
// is appended (the write hook reads the new view under wbMu). Both answer
// through the index; the replaced view lives until its last reader and is
// then ended: one view stays alive beside the reads, none after Close.
func TestAsOfTwoReadersShareAView(t *testing.T) {
	payload := strings.Repeat("x", asOfViewPayload)
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			views, stopViews := countAsOfHookWork()
			defer stopViews()
			bs, err := New(Config{InMemory: true, FlushInterval: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			closed := false
			defer func() {
				if !closed {
					_ = bs.Close()
				}
			}()
			asOfViewPut(t, bs, k, 100, 0, 4)
			asOfViewPut(t, bs, k, 200, 0, 2)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			const readers = 2
			var held sync.WaitGroup
			held.Add(readers)
			release := make(chan struct{})
			bs.asOfViewPinTestHook = func(pinned bool) {
				if pinned {
					held.Done()
					<-release
				}
			}
			seeks, stopSeeks := CountAsOfIndexSeekEntriesForTest()
			defer stopSeeks()
			out := make(chan asOfViewResult, readers)
			pins := []types.Instant{150, 450}
			for _, pin := range pins {
				go func() {
					var r asOfViewResult
					for range 200 { // the same view, read again and again beside the other reader
						if r.v, r.ok, r.err = k.read(bs, 100, pin, nil); r.err != nil || !r.ok {
							break
						}
					}
					out <- r
				}()
			}
			pinned := make(chan struct{})
			go func() { held.Wait(); close(pinned) }()
			select {
			case <-pinned:
			case <-time.After(10 * time.Second):
				close(release)
				t.Fatal("the two readers did not both pin the view")
			}
			pinnedView := asOfViewReadTS(bs)
			if err := k.put(bs, 200, 2, payload); err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if asOfViewReadTS(bs) == pinnedView {
				t.Fatal("control: the flush installed no new view")
			}
			if alive := views.snaps.Load() - views.ended.Load(); alive != 2 {
				t.Fatalf("%d views alive while two readers hold the replaced one, want 2 (it and the installed one)", alive)
			}
			bs.asOfViewPinTestHook = nil // the readers' later rounds pin the new view unheld
			close(release)
			appended := make(chan error, 1)
			go func() {
				var err error
				for v := uint32(0); v < 8 && err == nil; v++ {
					err = k.put(bs, 300, v, payload)
				}
				appended <- err
			}()
			want := map[uint32]bool{0: true, 3: true}
			for range readers {
				r := <-out
				if r.err != nil || !r.ok || !want[r.v] {
					t.Fatalf("reader: %+v, want v0 and v3", r)
				}
				delete(want, r.v)
			}
			if err := <-appended; err != nil {
				t.Fatal(err)
			}
			if got := seeks(); got != readers*200 {
				t.Fatalf("%d as-of seek keys visited by %d reads, want one each", got, readers*200)
			}
			if alive := views.snaps.Load() - views.ended.Load(); alive != 1 {
				t.Fatalf("%d views alive after the readers ended, want 1 (the installed one)", alive)
			}
			closed = true
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if alive := views.snaps.Load() - views.ended.Load(); alive != 0 {
				t.Fatalf("%d views alive after Close, want 0", alive)
			}
		})
	}
}

// TestAsOfReadersPinBesideFlushes: two readers of flushed quiet chains, back
// to back, beside an appender of another chain on a 1 ms flush tick and a
// MetaSet loop (each commit replaces the view). Every read answers its
// version through the index, without an error. A read that took its
// reference after it left the wbMu section that saw the view installed would
// meet a view that a flush in between had already discarded.
func TestAsOfReadersPinBesideFlushes(t *testing.T) {
	testtier.Slow(t)
	const quiet, versions, readers = 8, 4, 2
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs, err := New(Config{InMemory: true, FlushInterval: time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = bs.Close() }()
			for c := range int64(quiet) {
				asOfViewPut(t, bs, k, 1000+c, 0, versions)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			seeks, stopSeeks := CountAsOfIndexSeekEntriesForTest()
			defer stopSeeks()
			var reads, appends, metas atomic.Int64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			loop := func(n *atomic.Int64, step func(i int64) error) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := int64(0); ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						if err := step(i); err != nil {
							t.Error(err)
							return
						}
						n.Add(1)
					}
				}()
			}
			for r := range int64(readers) {
				loop(&reads, func(i int64) error {
					id := 1000 + (i+r*quiet/readers)%quiet
					v, ok, err := k.read(bs, id, 350, nil)
					if err == nil && (!ok || v != 2) {
						err = fmt.Errorf("chain %d at pin 350: v%d exact payload %v, want v2", id, v, ok)
					}
					return err
				})
			}
			loop(&appends, func(i int64) error { return k.put(bs, 1<<32+i/versions, uint32(i%versions), "x") })
			loop(&metas, func(i int64) error { return bs.MetaSet("floor", []byte{byte(i)}) })
			deadline := time.After(250 * time.Millisecond)
			for done := false; !done; {
				select {
				case <-deadline:
					done = true
				case <-time.After(time.Millisecond):
					done = t.Failed()
				}
			}
			close(stop)
			wg.Wait()
			if t.Failed() {
				return
			}
			if r, x := reads.Load(), seeks(); r == 0 || x != r || appends.Load() == 0 || metas.Load() == 0 {
				t.Fatalf("%d reads, %d through the index (want all), beside %d appends and %d MetaSets (want some of each)", r, x, appends.Load(), metas.Load())
			}
		})
	}
}

// TestAsOfReadBesideClose: no indexed read opens a transaction, so Close
// waits for none. A read that reaches its lock section after Close fails as a
// closed store and pins nothing; a read that pinned the view before Close
// does not hold Close up, and ends.
func TestAsOfReadBesideClose(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name+"/held before the section", func(t *testing.T) {
			views, stopViews := countAsOfHookWork()
			defer stopViews()
			bs := asOfViewStore(t, k, Config{InMemory: true, FlushInterval: time.Hour})
			release, out := holdAsOfRead(t, bs, k, 100, 250, false)
			asOfViewWithin(t, release, "Close", bs.Close)
			if alive := views.snaps.Load() - views.ended.Load(); alive != 0 {
				t.Fatalf("%d views alive after Close, want 0", alive)
			}
			close(release)
			if r := <-out; !errors.Is(r.err, ErrStoreClosed) || r.ok || r.work.probes != 0 {
				t.Fatalf("read that reached its section after Close: %+v, want ErrStoreClosed and no proof read", r)
			}
		})
		t.Run(k.name+"/held after the pin", func(t *testing.T) {
			views, stopViews := countAsOfHookWork()
			defer stopViews()
			bs := asOfViewStore(t, k, Config{InMemory: true, FlushInterval: time.Hour})
			release, out := holdAsOfRead(t, bs, k, 100, 250, true)
			asOfViewWithin(t, release, "Close", bs.Close)
			if alive := views.snaps.Load() - views.ended.Load(); alive != 1 {
				t.Fatalf("%d views alive while a read holds the dropped one, want 1", alive)
			}
			close(release)
			select {
			case r := <-out:
				// The database closed under the read: an answer or an error, never a hang.
				if r.err == nil && (!r.ok || r.v != 1) {
					t.Fatalf("read that pinned before Close answered v%d ok %v, want v1 or an error", r.v, r.ok)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("a read that pinned the view before Close did not end")
			}
			if alive := views.snaps.Load() - views.ended.Load(); alive != 0 {
				t.Fatalf("%d views alive after the read ended, want 0", alive)
			}
			if _, _, err := k.read(bs, 100, 250, nil); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("read after Close: %v, want ErrStoreClosed", err)
			}
		})
	}
}

// TestAsOfPinDeclinesInsideClear: between Clear's commitGen bump and the view
// it installs after the wipe, an indexed read pins nothing: the installed
// view is of the store before the wipe. Before the bump and after Clear a
// read of an unbuffered chain pins.
func TestAsOfPinDeclinesInsideClear(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := asOfViewStore(t, k, Config{InMemory: true, FlushInterval: time.Hour})
			prefix := storepkg.HistNodePrefix(100)
			prefix[0] = asOfIndexKind(k)
			pinned := func() bool {
				t.Helper()
				view, pin, err := bs.pinAsOfView(prefix)
				if err != nil {
					t.Fatal(err)
				}
				if view != nil {
					bs.unpinAsOfView(view, pin)
				}
				return view != nil
			}
			if !pinned() {
				t.Fatal("control: a flushed chain's read pinned no view before Clear")
			}
			inside, declined := 0, 0
			bs.readTxnOpenTestHook = func() {
				if onStackOf("refreshAsOfSnap") { // Clear opening its new view, after the wipe
					inside++
					if !pinned() {
						declined++
					}
				}
			}
			err := bs.Clear()
			bs.readTxnOpenTestHook = nil
			if err != nil || inside != 1 || declined != 1 {
				t.Fatalf("Clear %v: %d pins tried inside its window (want 1), %d declined (want 1)", err, inside, declined)
			}
			if !pinned() {
				t.Fatal("after Clear a read pinned no view")
			}
			if _, ok, err := k.read(bs, 100, 250, nil); ok || (err != nil && !asOfViewAbsent(err)) {
				t.Fatalf("pin 250 after Clear: ok %v err %v, want absent", ok, err)
			}
		})
	}
}

// TestAsOfReadAfterUncleanReopenBesideOtherFlushes: after an unclean close a
// writable open moves the recovery generation. A read of a chain written
// before it reads proof and generation from the view (two probes), finds
// them unequal and takes the scan, with other chains flushing at its seams as
// without; a chain written after the reopen reads through the index.
func TestAsOfReadAfterUncleanReopenBesideOtherFlushes(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), FlushInterval: time.Hour}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			asOfViewPut(t, bs, k, 100, 0, 4)
			bs.openImageSuppressed.Store(true) // no close header: an unclean close
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if bs, err = New(cfg); err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			asOfViewPut(t, bs, k, 200, 0, 2)
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			read := func() asOfViewResult {
				finish := observeAsOfViewRead(bs)
				v, ok, err := k.read(bs, 100, 250, nil)
				return asOfViewResult{v, ok, err, finish()}
			}
			quiet := read()
			if quiet.err != nil || !quiet.ok || quiet.v != 1 || quiet.work.probes != 2 || quiet.work.seeks != 0 {
				t.Fatalf("chain written before the unclean close: %+v, want v1 from the scan after two probes", quiet)
			}
			seen, stop := othersFlushAtAsOfSeams(t, bs, k, 200, 2)
			busy := read()
			stop()
			if busy != quiet || *seen != (asOfViewSeams{1, 1}) {
				t.Fatalf("the same read beside other chains' flushes: %+v seams %+v, want %+v", busy, *seen, quiet)
			}
			assertAsOfIndexUsed(t, bs, k, 200, 250, 1)
		})
	}
}

// TestAsOfViewRenewedByIdleFlush: a flush with nothing to commit (the idle
// tick of an async store) leaves the view at the store's newest version, so
// it does not hold back what a writer outside the write buffer and outside
// update committed (an open-image chunk batch, an index backfill).
func TestAsOfViewRenewedByIdleFlush(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	k := asOfBudgetKinds()[0]
	asOfViewPut(t, bs, k, 100, 0, 2)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(storepkg.MetaKey("chunk"), []byte{1}) }); err != nil {
		t.Fatal(err)
	}
	newest := bs.db.MaxVersion()
	if got := asOfViewReadTS(bs); got >= newest {
		t.Fatalf("control: view read ts %d, the outside commit's version %d: nothing to renew", got, newest)
	}
	if err := bs.Flush(); err != nil { // nothing pending: what the idle tick runs
		t.Fatal(err)
	}
	if got := asOfViewReadTS(bs); got != newest {
		t.Fatalf("after an idle flush: view read ts %d, store's newest version %d", got, newest)
	}
	assertAsOfIndexUsed(t, bs, k, 100, 250, 1)
}

// TestUpdateReleasesItsValueLogPinOnPanic: a panic out of update's
// transaction (badger panics on a damaged table; the read doors recover it)
// leaves no value-log reader entered: the next GC pass would wait for it
// until Close.
func TestUpdateReleasesItsValueLogPinOnPanic(t *testing.T) {
	bs := newFlushParkStore(t, nil)
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("control: update did not panic")
			}
		}()
		_ = bs.update(func(*badgerv4.Txn) error { panic("damaged table") })
	}()
	if a, b := bs.vlogReaders.active[0].Load(), bs.vlogReaders.active[1].Load(); a != 0 || b != 0 {
		t.Fatalf("value-log readers entered after a panicking update: %d and %d, want 0", a, b)
	}
}
