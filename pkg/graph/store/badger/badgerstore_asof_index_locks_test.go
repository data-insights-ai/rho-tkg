package badger

import (
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// The as-of write hook's native work and where it happens. The counters are
// process-wide (countAsOfHookWork): these tests do not run in parallel.

type asOfLockSeen struct {
	mu                 sync.Mutex
	opens, wbMu, idxMu int
}

// watchReadTxnLocks records, for every Badger read transaction the store
// opens, whether the opening goroutine holds wbMu or idxMu. The store's
// background flush is off (newFlushParkStore), so no other goroutine holds
// either lock while the test's door runs.
func watchReadTxnLocks(bs *Store) (*asOfLockSeen, func()) {
	seen := &asOfLockSeen{}
	bs.readTxnOpenTestHook = func() {
		seen.mu.Lock()
		defer seen.mu.Unlock()
		seen.opens++
		if bs.wbMu.TryLock() {
			bs.wbMu.Unlock()
		} else {
			seen.wbMu++
		}
		if bs.idxMu.TryLock() {
			bs.idxMu.Unlock()
		} else {
			seen.idxMu++
		}
	}
	return seen, func() { bs.readTxnOpenTestHook = nil }
}

func (s *asOfLockSeen) get() (opens, wb, idx int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opens, s.wbMu, s.idxMu
}

// TestAsOfAppendBadgerReads: an append whose proof is buffered reads only its
// alias key from Badger (the recovery generation stays in RAM), and no append
// opens a read transaction: the hook reads the snapshot the last flush left.
func TestAsOfAppendBadgerReads(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.MaxPendingWrites = -1 })
			payload := strings.Repeat("x", 8192)
			put := func(v uint32) asOfHookCount {
				t.Helper()
				w, stop := countAsOfHookWork()
				defer stop()
				seen, stopSeen := watchReadTxnLocks(bs)
				defer stopSeen()
				if err := k.put(bs, 100, v, payload); err != nil {
					t.Fatal(err)
				}
				opens, _, _ := seen.get()
				return asOfHookCount{int64(opens), w.gets.Load(), w.seeks.Load()}
			}
			if w := put(0); w.txns != 0 || w.seeks != 1 {
				t.Fatalf("version 0: %+v, want 0 txns and 1 seek", w)
			}
			for v := uint32(1); v < 8; v++ {
				if w := put(v); w != (asOfHookCount{0, 1, 0}) {
					t.Fatalf("version %d (proof buffered): %+v, want 0 txns, 1 get (alias), 0 seeks", v, w)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			if w := put(8); w != (asOfHookCount{0, 2, 0}) {
				t.Fatalf("version 8 (proof flushed): %+v, want 0 txns, 2 gets (alias, proof), 0 seeks", w)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			// Control, exact and non-empty: the chain is still certified and an
			// old pin reads through the index (one seek-key visit, one row).
			finish := observeAsOfIndexRead(bs)
			v, ok, err := k.read(bs, 100, 450, nil)
			work := finish()
			if err != nil || !ok || v != 3 || work.visits != 1 || work.rows != 1 {
				t.Fatalf("indexed control: v%d ok %v work %+v err %v, want v3 through one seek key", v, ok, work, err)
			}
			assertAsOfIndexOracle(t, bs, k)
		})
	}
}

type asOfHookCount struct{ txns, gets, seeks int64 }

// reloadAsOfView does for the as-of write hook what open and Clear do: a new
// committed view, and the recovery generation read again. A test that writes
// the generation straight into Badger (in production only open's recovery
// and a staged discard write it, before a handle exists) calls it before the
// next history append.
func reloadAsOfView(bs *Store) {
	bs.flushMu.Lock()
	defer bs.flushMu.Unlock()
	bs.refreshAsOfSnap(true)
}

// TestAsOfLegacyChainMarkedOnce: a chain the index cannot certify (here its
// first history row is void, so it never had a proof) is marked invalid on its
// next ordinary append; later appends stop at that marker instead of seeking
// the history prefix and reading the generation again, for good.
func TestAsOfLegacyChainMarkedOnce(t *testing.T) {
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			asOfIndexPut(t, bs, k, 0, &types.TemporalMetadata{TxFrom: 100, ValidFrom: 150, ValidTo: 160, Void: true})
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			for v := uint32(1); v < 5; v++ {
				w, stop := countAsOfHookWork()
				asOfIndexPut(t, bs, k, v, &types.TemporalMetadata{TxFrom: types.Instant(v+1) * 100, TxTo: types.Instant(v+2) * 100, ValidFrom: 1})
				stop()
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				if v > 1 && w.seeks.Load() != 0 {
					t.Fatalf("append %d to an uncertified chain seeked its history %d times, want 0", v, w.seeks.Load())
				}
			}
			var raw []byte
			if err := bs.view(func(txn *badgerv4.Txn) (err error) {
				raw, err = asOfStoredValue(txn, storepkg.HistoryAsOfProofKey(asOfIndexKind(k), 100))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if len(raw) != 1 || raw[0] != 0 {
				t.Fatalf("uncertified chain's proof %x, want the invalid marker 00", raw)
			}
			if entries, _ := asOfIndexDiskSize(t, bs); entries != 1 {
				t.Fatalf("uncertified chain has %d index entries, want 1 (the marker; no seek key or alias)", entries)
			}
			assertAsOfIndexOracle(t, bs, k)
		})
	}
}

// TestAsOfHookOpensNoReadTxnUnderLocks: no door that writes history opens a
// Badger read transaction while its goroutine holds wbMu or idxMu: the plain
// append (wbMu), the update door (idxMu across its append) and truncation
// (one append of many history deletes under idxMu).
func TestAsOfHookOpensNoReadTxnUnderLocks(t *testing.T) {
	payload := strings.Repeat("x", 8192)
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name+"/append", func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(3) {
				if err := k.put(bs, 100, v, payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			seen, stop := watchReadTxnLocks(bs)
			if err := k.put(bs, 100, 3, payload); err != nil {
				t.Fatal(err)
			}
			stop()
			if opens, wb, idx := seen.get(); opens != 0 || wb != 0 || idx != 0 {
				t.Fatalf("append: %d read txns opened (want 0), %d under wbMu, %d under idxMu", opens, wb, idx)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexOracle(t, bs, k)
		})
		t.Run(k.name+"/truncate", func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(20) {
				if err := k.put(bs, 100, v, payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			seen, stop := watchReadTxnLocks(bs)
			w, stopWork := countAsOfHookWork()
			var err error
			if k.name == "node" {
				err = bs.TruncateNodeHistory(100, 2)
			} else {
				err = bs.TruncateRelHistory(100, 2)
			}
			stopWork()
			stop()
			if err != nil {
				t.Fatal(err)
			}
			// 18 deletes, each an alias and a proof lookup (the first delete
			// invalidates, so later proofs come from the buffer): reads of
			// one view, no transaction per delete.
			if opens, wb, idx := seen.get(); wb != 0 || idx != 0 || w.gets.Load() != 19 {
				t.Fatalf("truncate of 18 versions: %d read txns opened, %d under wbMu, %d under idxMu; hook gets %d (want 19)", opens, wb, idx, w.gets.Load())
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			assertAsOfIndexOracle(t, bs, k)
		})
	}
	t.Run("node/update door", func(t *testing.T) {
		bs := newFlushParkStore(t, nil)
		node := func(v uint32, from, to types.Instant) *types.Node {
			n := types.NewNode(100, 1, nil)
			n.SetVersion(v)
			n.SetTemporal(&types.TemporalMetadata{TxFrom: from, TxTo: to, ValidFrom: 1})
			if err := n.SetProperty("payload", payload); err != nil {
				t.Fatal(err)
			}
			return n
		}
		if err := bs.PutNode(node(0, 100, 0)); err != nil {
			t.Fatal(err)
		}
		for v := uint32(1); v < 4; v++ {
			if err := bs.ReplaceNodeWithHistory(node(v, types.Instant(v+1)*100, 0), v-1, node(v-1, types.Instant(v)*100, types.Instant(v+1)*100)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		seen, stop := watchReadTxnLocks(bs)
		err := bs.ReplaceNodeWithHistory(node(4, 500, 0), 3, node(3, 400, 500))
		stop()
		if err != nil {
			t.Fatal(err)
		}
		if opens, wb, idx := seen.get(); opens != 0 || wb != 0 || idx != 0 {
			t.Fatalf("update door: %d read txns opened (want 0), %d under wbMu, %d under idxMu", opens, wb, idx)
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		for pin, want := range map[types.Instant]uint32{150: 0, 250: 1, 350: 2, 450: 3} {
			n, err := bs.NodeAsOf(100, pin)
			if err != nil || n == nil || n.Version() != want {
				t.Fatalf("pin %d: %v err %v, want v%d", pin, n, err, want)
			}
		}
	})
}

// TestAsOfSlowReadTxnBlocksNoWriter: while one goroutine's Badger read
// transaction is slow to open (here: held at the open), a creator and a
// history appender on other entities finish. With the read transaction
// opened under wbMu, both waited for it.
func TestAsOfSlowReadTxnBlocksNoWriter(t *testing.T) {
	payload := strings.Repeat("x", 8192)
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for _, id := range []int64{100, 200} {
				for v := range uint32(2) {
					if err := k.put(bs, id, v, payload); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			held, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			bs.readTxnOpenTestHook = func() {
				once.Do(func() {
					close(held)
					<-release
				})
			}
			defer func() { bs.readTxnOpenTestHook = nil }()
			slow := make(chan error, 1)
			go func() {
				// An append and a flush: whichever opens a read transaction
				// first is held there.
				err := k.put(bs, 100, 2, payload)
				if err == nil {
					err = bs.Flush()
				}
				slow <- err
			}()
			select {
			case <-held:
			case err := <-slow:
				t.Fatalf("the slow goroutine opened no read transaction: %v", err)
			case <-time.After(10 * time.Second):
				t.Fatal("no read transaction opened")
			}
			others := make(chan error, 1)
			go func() {
				n := types.NewNode(300, 1, nil)
				n.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 1})
				err := bs.PutNode(n)
				if err == nil {
					err = k.put(bs, 200, 2, payload)
				}
				others <- err
			}()
			select {
			case err := <-others:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				close(release)
				<-slow
				t.Fatal("a creator and an appender waited for another goroutine's read transaction to open")
			}
			close(release)
			if err := <-slow; err != nil {
				t.Fatal(err)
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			for _, id := range []int64{100, 200} {
				for pin, want := range map[types.Instant]uint32{150: 0, 250: 1, 350: 2} {
					v, ok, err := k.read(bs, id, pin, nil)
					if err != nil || !ok || v != want {
						t.Fatalf("id %d pin %d: v%d ok %v err %v, want v%d", id, pin, v, ok, err, want)
					}
				}
			}
		})
	}
}

// onStackOf reports whether the calling goroutine runs inside fn (a method
// name such as "refreshAsOfSnap"): a read-transaction hook uses it to tell
// one opener from the others.
func onStackOf(fn string) bool {
	pcs := make([]uintptr, 64)
	frames := runtime.CallersFrames(pcs[:runtime.Callers(2, pcs)])
	for {
		f, more := frames.Next()
		if strings.HasSuffix(f.Function, "."+fn) {
			return true
		}
		if !more {
			return false
		}
	}
}

// TestAsOfClearInstallsViewBeforeWriters: Clear installs the as-of hook's new
// view before it releases idxMu, so a history append that waited for Clear
// reads the wiped store and certifies its chain. Installed after the unlock,
// an append in between found the old view stale and marked its chain invalid
// for good.
func TestAsOfClearInstallsViewBeforeWriters(t *testing.T) {
	payload := strings.Repeat("x", 8192)
	for _, k := range asOfBudgetKinds() {
		t.Run(k.name, func(t *testing.T) {
			bs := newFlushParkStore(t, nil)
			for v := range uint32(3) {
				if err := k.put(bs, 100, v, payload); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			wrote := make(chan error, 1)
			var once sync.Once
			bs.readTxnOpenTestHook = func() {
				if !onStackOf("refreshAsOfSnap") {
					return
				}
				once.Do(func() {
					// A new chain written while Clear installs its view: it
					// either runs now (the view not yet installed) or waits
					// for Clear.
					go func() {
						err := k.put(bs, 100, 0, payload)
						if err == nil {
							err = k.put(bs, 100, 1, payload)
						}
						wrote <- err
					}()
					select {
					case err := <-wrote:
						wrote <- err
					case <-time.After(300 * time.Millisecond):
					}
				})
			}
			if err := bs.Clear(); err != nil {
				t.Fatal(err)
			}
			if err := <-wrote; err != nil {
				t.Fatal(err)
			}
			bs.readTxnOpenTestHook = nil
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if err := bs.view(func(txn *badgerv4.Txn) (err error) {
				raw, err = asOfStoredValue(txn, storepkg.HistoryAsOfProofKey(asOfIndexKind(k), 100))
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if _, valid, err := decodeAsOfProof(raw); err != nil || !valid {
				t.Fatalf("chain written beside Clear: proof %x valid %v err %v, want a valid proof", raw, valid, err)
			}
			finish := observeAsOfIndexRead(bs)
			v, ok, err := k.read(bs, 100, 150, nil)
			if work := finish(); err != nil || !ok || v != 0 || work.visits != 1 {
				t.Fatalf("pin 150 after Clear: v%d ok %v work %+v err %v, want v0 through one seek key", v, ok, work, err)
			}
			assertAsOfIndexOracle(t, bs, k)
		})
	}
}

// asOfViewReadTS is the read timestamp of the installed as-of view (0: none).
func asOfViewReadTS(bs *Store) uint64 {
	bs.wbMu.Lock()
	defer bs.wbMu.Unlock()
	if bs.asOfSnap.view == nil {
		return 0
	}
	return bs.asOfSnap.view.txn.ReadTs()
}

// TestAsOfViewRenewedAfterDirectCommit: a commit outside the write buffer (a
// MetaSet, as tiered's commit floor) leaves the hook's view at the store's
// newest version, so the view holds back no version a later flush did not
// write. Under SyncWrites no flush loop runs; without the renewal the view
// stayed below every such commit until the next flush. A chain appended
// after it still certifies and reads through the index.
func TestAsOfViewRenewedAfterDirectCommit(t *testing.T) {
	payload := strings.Repeat("x", 8192)
	for _, shape := range []struct {
		name string
		cfg  func(*Config)
	}{
		{"sync writes", func(c *Config) { c.SyncWrites = true }},
		{"async", nil},
	} {
		for _, k := range asOfBudgetKinds() {
			t.Run(shape.name+"/"+k.name, func(t *testing.T) {
				bs := newFlushParkStore(t, shape.cfg)
				for v := range uint32(2) {
					if err := k.put(bs, 100, v, payload); err != nil {
						t.Fatal(err)
					}
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				for i := range 3 {
					if err := bs.MetaSet("floor", []byte{byte(i)}); err != nil {
						t.Fatal(err)
					}
					if got, want := asOfViewReadTS(bs), bs.db.MaxVersion(); got != want {
						t.Fatalf("after direct commit %d: view read ts %d, store's newest version %d", i, got, want)
					}
				}
				if err := k.put(bs, 100, 2, payload); err != nil {
					t.Fatal(err)
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				finish := observeAsOfIndexRead(bs)
				v, ok, err := k.read(bs, 100, 250, nil)
				if work := finish(); err != nil || !ok || v != 1 || work.visits != 1 {
					t.Fatalf("pin 250: v%d ok %v work %+v err %v, want v1 through one seek key", v, ok, work, err)
				}
				assertAsOfIndexOracle(t, bs, k)
			})
		}
	}
}

// TestAsOfCloseDropsView: Close ends the as-of view before it stops the
// database (its read mark must not outlive the oracle).
func TestAsOfCloseDropsView(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.SyncWrites = true })
	k := asOfBudgetKinds()[0]
	if err := k.put(bs, 100, 0, strings.Repeat("x", 8192)); err != nil {
		t.Fatal(err)
	}
	if asOfViewReadTS(bs) == 0 {
		t.Fatal("control: no view installed on an open store")
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if bs.asOfSnap.view != nil {
		t.Fatal("Close left the as-of view installed")
	}
}
