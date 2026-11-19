package badger

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// asOfModesPut writes version v of entity id with tm and the payload the
// readers of asOfBudgetKinds check.
func asOfModesPut(t *testing.T, bs *Store, k asOfBudgetKind, id int64, v uint32, tm *types.TemporalMetadata) {
	t.Helper()
	payload := strings.Repeat("x", 8192)
	var err error
	if k.name == "node" {
		n := types.NewNode(types.NodeID(id), 1, nil)
		n.SetVersion(v)
		n.SetTemporal(tm)
		_ = n.SetProperty("payload", payload)
		err = bs.PutNodeVersion(n.ID(), v, n)
	} else {
		r := types.NewRelationship(types.RelID(id), 5, 1, 2)
		r.SetVersion(v)
		r.SetTemporal(tm)
		_ = r.SetProperty("payload", payload)
		err = bs.PutRelVersion(r.ID(), v, r)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func asOfModesTM(v uint32) *types.TemporalMetadata {
	return &types.TemporalMetadata{TxFrom: types.Instant(v+1) * 100, TxTo: types.Instant(v+2) * 100, ValidFrom: 1}
}

type asOfModesAnswer struct {
	version      uint32
	ok, notFound bool
	otherErr     string
}

func asOfModesRead(t *testing.T, bs *Store, k asOfBudgetKind, ids []int64) map[[2]int64]asOfModesAnswer {
	t.Helper()
	out := map[[2]int64]asOfModesAnswer{}
	for _, id := range ids {
		for pin := types.Instant(0); pin <= 1500; pin += 25 {
			v, ok, err := k.read(bs, id, pin, nil)
			a := asOfModesAnswer{version: v, ok: ok, notFound: errors.Is(err, ErrVersionNotFound)}
			if err != nil && !a.notFound {
				a.otherErr = err.Error()
			}
			out[[2]int64{id, int64(pin)}] = a
		}
	}
	return out
}

// TestAsOfAnswersEqualOnOffInvalidated: the as-of answers of every pin over
// certified, rewritten, legacy, truncated and partly buffered chains are the
// same with the index used, declined (every read takes the admitted scan)
// and invalidated (a moved recovery generation), and match the history oracle
// with the index used. Appends after the invalidation answer the same too.
func TestAsOfAnswersEqualOnOffInvalidated(t *testing.T) {
	ids := []int64{100, 101, 102, 103, 104}
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding = true; c.HistoryAnchorInterval = 4 })
			put := func(id int64, from, to uint32) {
				for v := from; v < to; v++ {
					asOfModesPut(t, bs, k, id, v, asOfModesTM(v))
				}
			}
			put(100, 0, 4) // certified across two flushes
			put(101, 0, 4) // rewritten below: invalid
			asOfModesPut(t, bs, k, 102, 0, &types.TemporalMetadata{TxFrom: 100, ValidFrom: 150, ValidTo: 160, Void: true})
			put(103, 0, 10) // truncated below
			put(104, 0, 3)  // last versions stay buffered
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			put(100, 4, 8)
			asOfModesPut(t, bs, k, 101, 1, asOfModesTM(1))
			put(102, 1, 5) // legacy: never certified
			var err error
			if k.name == "node" {
				err = bs.TruncateNodeHistory(103, 3)
			} else {
				err = bs.TruncateRelHistory(103, 3)
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			put(104, 3, 6)
			compare := func(stage string) {
				t.Helper()
				on := asOfModesRead(t, bs, k, ids)
				bs.SetAsOfIndexDeclinedForTest(true)
				off := asOfModesRead(t, bs, k, ids)
				bs.SetAsOfIndexDeclinedForTest(false)
				for key, a := range on {
					if off[key] != a {
						t.Fatalf("%s id %d pin %d: index used %+v, declined %+v", stage, key[0], key[1], a, off[key])
					}
				}
				assertAsOfIndexOracle(t, bs, k) // entity 100 against SelectAsOf
			}
			compare("buffered")
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			compare("flushed")
			before := asOfModesRead(t, bs, k, ids)
			// The control is not empty: entity 100 reads through the index.
			assertAsOfIndexUsed(t, bs, k, 100, 450, 3)
			if err := bs.InvalidateAsOfGenerationForTest(); err != nil {
				t.Fatal(err)
			}
			invalidated := asOfModesRead(t, bs, k, ids)
			for key, a := range before {
				if invalidated[key] != a {
					t.Fatalf("id %d pin %d: index used %+v, invalidated %+v", key[0], key[1], a, invalidated[key])
				}
			}
			put(100, 8, 10)
			put(104, 6, 8)
			compare("appended after invalidation, buffered")
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			compare("appended after invalidation, flushed")
			// Entity 100 answered every version at its own pins before the
			// invalidation.
			for v := uint32(0); v < 8; v++ {
				if a := before[[2]int64{100, int64(v+1)*100 + 50}]; !a.ok || a.version != v {
					t.Fatalf("control: entity 100 at pin %d answered %+v, want v%d", (v+1)*100+50, a, v)
				}
			}
		})
	}
}

// assertAsOfIndexUsed: an as-of read of id at pin answers version want through
// the index (one seek-key visit, one row decoded).
func assertAsOfIndexUsed(t *testing.T, bs *Store, k asOfBudgetKind, id int64, pin types.Instant, want uint32) {
	t.Helper()
	finish := observeAsOfIndexRead(bs)
	v, ok, err := k.read(bs, id, pin, nil)
	work := finish()
	if err != nil || !ok || v != want || work.visits != 1 || work.rows != 1 {
		t.Fatalf("id %d pin %d: v%d ok %v work %+v err %v; want v%d through one seek key", id, pin, v, ok, work, err, want)
	}
}

// TestAsOfIndexAfterClear: after Clear the write hook reads the wiped store,
// not its view from before the wipe: a chain written again from version 0
// is certified under the wiped (zero) recovery generation and reads through
// the index; the old proof and the old generation are gone.
func TestAsOfIndexAfterClear(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			put := func(to uint32) {
				for v := range to {
					asOfModesPut(t, bs, k, 100, v, asOfModesTM(v))
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.InvalidateAsOfGenerationForTest(); err != nil {
				t.Fatal(err)
			}
			put(8)
			assertAsOfIndexUsed(t, bs, k, 100, 450, 3)
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			put(4)
			assertAsOfIndexUsed(t, bs, k, 100, 250, 1)
			assertAsOfIndexOracle(t, bs, k)
		})
	}
}

// TestAsOfConcurrentAppendsKeepProofs: appenders on their own chains, a
// creator and the background flush (1 ms) run together, so the hook's view is
// replaced under the appends. No chain loses its proof to a stale view, and
// every chain answers each version at its own pins through the index.
func TestAsOfConcurrentAppendsKeepProofs(t *testing.T) {
	const chains, versions = 4, 40
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.FlushInterval = time.Millisecond })
			var wg sync.WaitGroup
			errs := make(chan error, chains+1)
			for c := range int64(chains) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for v := range uint32(versions) {
						if err := k.put(bs, 100+c, v, strings.Repeat("x", 8192)); err != nil {
							errs <- err
							return
						}
					}
				}()
			}
			stop := make(chan struct{})
			go func() {
				for i := int64(0); ; i++ {
					select {
					case <-stop:
						errs <- nil
						return
					default:
					}
					n := types.NewNode(types.NodeID(1000+i), 1, nil)
					n.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 1})
					if err := bs.PutNode(n); err != nil {
						errs <- err
						return
					}
				}
			}()
			wg.Wait()
			close(stop)
			if err := <-errs; err != nil { // the creator's end, or an appender's error
				t.Fatal(err)
			}
			select {
			case err := <-errs:
				t.Fatal(err)
			default:
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			for c := range int64(chains) {
				for _, v := range []uint32{0, versions / 2, versions - 1} {
					assertAsOfIndexUsed(t, bs, k, 100+c, types.Instant(v+1)*100+50, v)
				}
			}
		})
	}
}

// TestAsOfStaleViewNeverRead: once commitGen moved past the hook's view (a
// commit of the write buffer whose flush has not installed a new view), an
// append does not trust what the view says: the chain is marked invalid
// rather than extended, and answers every pin as its history does.
func TestAsOfStaleViewNeverRead(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(8) {
				asOfModesPut(t, bs, k, 100, v, asOfModesTM(v))
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexUsed(t, bs, k, 100, 450, 3)
			bs.commitGen.Add(1)
			asOfModesPut(t, bs, k, 100, 8, asOfModesTM(8))
			bs.wbMu.Lock()
			raw := bs.pending[string(storepkg.HistoryAsOfProofKey(asOfIndexKind(k), 100))].value
			bs.wbMu.Unlock()
			if len(raw) != 1 || raw[0] != 0 {
				t.Fatalf("append over a stale view queued proof %x, want the invalid marker 00", raw)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexOracle(t, bs, k)
		})
	}
}

// TestAsOfAppendWhileFlushInstallsView: an append that runs while a flush
// opens the hook's next view (after its commit, before the view is
// installed) still reads a current view: the chain stays certified.
func TestAsOfAppendWhileFlushInstallsView(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(4) {
				asOfModesPut(t, bs, k, 100, v, asOfModesTM(v))
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			asOfModesPut(t, bs, k, 101, 0, asOfModesTM(0)) // the next flush commits something
			ran := false
			bs.readTxnOpenTestHook = func() {
				if !ran {
					ran = true
					asOfModesPut(t, bs, k, 100, 4, asOfModesTM(4))
				}
			}
			err := bs.Flush()
			bs.readTxnOpenTestHook = nil
			if err != nil || !ran {
				t.Fatalf("flush %v, append inside its view open %v", err, ran)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexUsed(t, bs, k, 100, 550, 4)
			assertAsOfIndexOracle(t, bs, k)
		})
	}
}

// TestAsOfGenerationAfterUncleanReopen: after an unclean close the open
// moves the recovery generation; the hook certifies chains written after
// it under the new generation, so they read through the index.
func TestAsOfGenerationAfterUncleanReopen(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			cfg := Config{Dir: t.TempDir(), FlushInterval: time.Hour}
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			asOfModesPut(t, bs, k, 101, 0, asOfModesTM(0))
			bs.openImageSuppressed.Store(true) // no close header: an unclean close
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			bs, err = New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			for v := range uint32(4) {
				asOfModesPut(t, bs, k, 100, v, asOfModesTM(v))
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexUsed(t, bs, k, 100, 250, 1)
		})
	}
}
