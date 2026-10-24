package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// FLUSHPART, adversarial: each test names the defect it is built to catch.
// The happy-path and crash-point tests are in flush_split_test.go.

// stagedHistory is a staged-flush workload over two sessions: session 1
// writes base nodes (label 7) and closes cleanly, so an open image exists;
// session 2 writes nodes of label 8 with an indexed property, replaces a
// part of the base nodes and deletes another part, all in one flush large
// enough to be staged.
type stagedHistory struct {
	base, added []*types.Node
}

func newStagedHistory() stagedHistory {
	h := stagedHistory{base: make([]*types.Node, 2_000), added: make([]*types.Node, splitFlushNodeCount)}
	for i := range h.base {
		h.base[i] = types.NewNode(types.NodeID(snowflake.ID(500_000+i)), 7, nil)
		_ = h.base[i].SetProperty("k", int64(i))
	}
	for i := range h.added {
		h.added[i] = types.NewNode(types.NodeID(snowflake.ID(1_000_000+i)), 8, nil)
		_ = h.added[i].SetProperty("k", int64(i%97))
		h.added[i].SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(10_000 + i)})
	}
	return h
}

func (h stagedHistory) session1(t *testing.T, cfg Config) {
	t.Helper()
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(8, "k"); err != nil {
		t.Fatal(err)
	}
	putNodes(t, bs, h.base)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
}

// session2Writes are the writes of the staged flush.
func (h stagedHistory) session2Writes(t *testing.T, bs *Store) {
	t.Helper()
	putNodes(t, bs, h.added)
	for i := 0; i < 500; i++ {
		n := types.NewNode(h.base[i].ID(), 7, nil)
		_ = n.SetProperty("k", int64(-i))
		if err := bs.ReplaceNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for i := 500; i < 1_000; i++ {
		if err := bs.DeleteNode(h.base[i].ID()); err != nil {
			t.Fatal(err)
		}
	}
}

// digestOf opens dir read-only and returns its index digest.
func digestOf(t *testing.T, cfg Config) string {
	t.Helper()
	cfg.ReadOnly = true
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	// NDV only grows within a session and is exact after a rebuild (docs/
	// query-planners.md "Deletion semantics"), so an image and a rebuild of the
	// same rows may differ in it alone.
	return ndvPattern.ReplaceAllString(bs.IndexStateDigestForTest(), "NDV:_")
}

var ndvPattern = regexp.MustCompile(`NDV:\d+`)

type lsnTag struct {
	lsn uint64
	tag storecontract.ChangeTag
}

func changesOf(t *testing.T, bs *Store) []lsnTag {
	t.Helper()
	var out []lsnTag
	if err := bs.ForEachChange(0, func(r storecontract.ChangeRecord) bool {
		out = append(out, lsnTag{r.LSN, r.Tag})
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestStagedFlushRecoveryMatchesUncrashedStore: a store crashed at each point
// where recovery applies the flush (marked, apply written, applied) and then
// reopened has exactly the state of the same writes without a crash: the
// same index digest (rows, labels, adjacency, counters, the property index),
// the same change-log records (LSNs and tags), a stored commit floor at or
// above every row's stamp, an LSN allocator above every record, and nothing
// from the session-1 open image. Catches: recovery after loadIndexes or the
// counter reconciliation (digest, refused open); the commit floor or
// LastLSNKey left out of the staging (floor, reused LSN); the image trusted
// after recovery (digest).
func TestStagedFlushRecoveryMatchesUncrashedStore(t *testing.T) {
	h := newStagedHistory()
	oracle := splitFlushConfig(t.TempDir())
	h.session1(t, oracle)
	bs, err := New(oracle)
	if err != nil {
		t.Fatal(err)
	}
	h.session2Writes(t, bs)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	wantDigest := digestOf(t, oracle)
	bs, err = New(oracle)
	if err != nil {
		t.Fatal(err)
	}
	wantChanges := changesOf(t, bs)
	_ = bs.Close()

	for _, step := range []stagedFlushStep{stagedStepMarked, stagedStepApplyWritten, stagedStepApplied} {
		for _, sync := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/sync=%v", step, sync), func(t *testing.T) {
				t.Parallel()
				cfg := splitFlushConfig(t.TempDir())
				cfg.SyncWrites = sync
				h.session1(t, cfg)
				bs, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				injected := failAt(bs, step)
				if err := inOneFlush(bs, sync, func() { h.session2Writes(t, bs) }); !errors.Is(err, injected) {
					t.Fatalf("the flush = %v, want the injected failure", err)
				}
				crashStop(bs)

				bs, err = New(cfg)
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				if got := changesOf(t, bs); !slices.Equal(got, wantChanges) {
					t.Fatalf("%d change-log records after recovery, want the uncrashed %d (or they differ)", len(got), len(wantChanges))
				}
				floor, err := bs.StoredCommitFloor()
				if err != nil {
					t.Fatal(err)
				}
				if top := types.Instant(10_000 + len(h.added) - 1); floor < top {
					t.Fatalf("stored commit floor %d below the newest recovered row's TxFrom %d", floor, top)
				}
				extra := types.NewNode(types.NodeID(snowflake.ID(9_000_000)), 7, nil)
				if err := bs.PutNode(extra); err != nil {
					t.Fatal(err)
				}
				if err := bs.Flush(); err != nil {
					t.Fatal(err)
				}
				after := changesOf(t, bs)
				if len(after) != len(wantChanges)+1 || after[len(after)-1].lsn <= wantChanges[len(wantChanges)-1].lsn {
					t.Fatalf("the first write after recovery got LSN %d, last recovered LSN %d", after[len(after)-1].lsn, wantChanges[len(wantChanges)-1].lsn)
				}
				if err := bs.DeleteNode(extra.ID()); err != nil {
					t.Fatal(err)
				}
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				// The extra node is gone again; the rows and indexes are the
				// uncrashed store's.
				if got := digestOf(t, cfg); got != wantDigest {
					t.Fatalf("index digest after recovery differs from the uncrashed store:\n%s", digestDiff(got, wantDigest))
				}
			})
		}
	}
}

// TestStagedRecoveryIsRedoable: the open's recovery itself crashes (fails)
// after each of its steps; a later open finishes it. Catches a recovery that
// unmarks before it applies (the next open finds no marker and the flush is
// lost) or that deletes the staging before the unmark (the next open fails
// the count check).
func TestStagedRecoveryIsRedoable(t *testing.T) {
	for _, step := range []stagedFlushStep{stagedStepApplyWritten, stagedStepApplied, stagedStepUnmarked, stagedStepCleanWritten} {
		t.Run(step.String(), func(t *testing.T) {
			cfg := splitFlushConfig(t.TempDir())
			bs, err := New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			nodes := splitFlushNodes(splitFlushNodeCount)
			putNodes(t, bs, nodes)
			injected := failAt(bs, stagedStepMarked)
			if err := bs.Flush(); !errors.Is(err, injected) {
				t.Fatalf("Flush = %v", err)
			}
			crashStop(bs)

			recoveryCrash := fmt.Errorf("recovery crash after %v", step)
			recoverStagedFlushFault = func(s stagedFlushStep) error {
				if s == step {
					return recoveryCrash
				}
				return nil
			}
			_, err = New(cfg)
			recoverStagedFlushFault = nil
			if !errors.Is(err, recoveryCrash) {
				t.Fatalf("New = %v, want the injected recovery failure", err)
			}
			bs, err = New(cfg)
			if err != nil {
				t.Fatalf("second recovery: %v", err)
			}
			t.Cleanup(func() { _ = bs.Close() })
			if got := liveNodes(t, bs, nodes); got != len(nodes) {
				t.Fatalf("%d of %d nodes live after the redone recovery", got, len(nodes))
			}
			if got := len(changesOf(t, bs)); got != len(nodes) {
				t.Fatalf("%d change-log records, want %d", got, len(nodes))
			}
			if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
				t.Fatalf("%d staging keys left", n)
			}
		})
	}
}

// TestStagedFlushReadersSeeEveryRowThroughout: while a staged flush runs,
// every row of it stays readable through the paths that read badger behind
// the RAM caches (history, label membership, node count), at every step, and
// concurrently with readers. Catches the overlay (bs.flushing) dropped before
// the apply has written the rows.
func TestStagedFlushReadersSeeEveryRowThroughout(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	nodes := splitFlushNodes(splitFlushNodeCount)
	for i, n := range nodes {
		n.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(10_000 + i)})
	}
	putNodes(t, bs, nodes)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	// The staged flush replaces every node: its history rows (the archived
	// first versions) live only in the flush until it is applied.
	for i, n := range nodes {
		next := types.NewNode(n.ID(), 7, nil)
		_ = next.SetProperty("k", int64(i+1))
		next.SetTemporal(&types.TemporalMetadata{TxFrom: types.Instant(50_000 + i)})
		if err := bs.ReplaceNodeWithHistory(next, 0, n); err != nil {
			t.Fatal(err)
		}
	}
	check := func() error {
		for i := 0; i < len(nodes); i += 97 {
			h, err := bs.GetNodeHistory(nodes[i].ID())
			if err != nil {
				return fmt.Errorf("history of node %d: %w", i, err)
			}
			if len(h) == 0 {
				return fmt.Errorf("node %d has no history", i)
			}
		}
		ids, err := bs.NodeIDsByLabel(7)
		if err != nil {
			return err
		}
		if len(ids) != len(nodes) {
			return fmt.Errorf("label lists %d of %d nodes", len(ids), len(nodes))
		}
		if c, err := bs.NodeCount(); err != nil || c != len(nodes) {
			return fmt.Errorf("node count %d (%v), want %d", c, err, len(nodes))
		}
		return nil
	}
	var (
		mu     sync.Mutex
		failed []string
	)
	bs.stagedFlushFault = func(s stagedFlushStep) error {
		if err := check(); err != nil {
			mu.Lock()
			failed = append(failed, fmt.Sprintf("after %v: %v", s, err))
			mu.Unlock()
		}
		return nil
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := check(); err != nil {
					mu.Lock()
					failed = append(failed, "concurrent: "+err.Error())
					mu.Unlock()
					return
				}
			}
		}()
	}
	err = bs.Flush()
	close(stop)
	wg.Wait()
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) > 0 {
		t.Fatalf("rows not visible during the staged flush: %v", failed)
	}
	if err := check(); err != nil {
		t.Fatalf("after the flush: %v", err)
	}
}

// TestStagedFailClosedEveryDoor: after a staged flush commits its marker and
// the apply (or the unmark) fails, every door answers ErrStoreClosed, a write
// racing the failure is refused or lost (never committed after the marker),
// Close writes no open image, and the reopen applies the flush. Catches a
// door that skips the check (a write that the reopen's re-apply then
// overwrites) and a requeue after the marker.
func TestStagedFailClosedEveryDoor(t *testing.T) {
	for _, step := range []stagedFlushStep{stagedStepApplyWritten, stagedStepApplied} {
		for _, sync := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/sync=%v", step, sync), func(t *testing.T) {
				cfg := splitFlushConfig(t.TempDir())
				cfg.SyncWrites = sync
				h := newStagedHistory()
				h.session1(t, cfg) // an image exists: the failed session must not vouch for it
				bs, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				racer := types.NewNode(types.NodeID(snowflake.ID(8_000_000)), 7, nil)
				racerErr := make(chan error, 1)
				injected := fmt.Errorf("injected failure after %v", step)
				raced, failed := false, false // each fires once: a requeued flush must not race or fail again
				bs.stagedFlushFault = func(s stagedFlushStep) error {
					switch {
					case s == stagedStepMarked && !raced:
						raced = true
						// A writer racing the failing flush: with SyncWrites
						// its own flush waits for this one, then must fail;
						// without, it only buffers.
						if sync {
							go func() { racerErr <- bs.PutNode(racer) }()
						} else {
							racerErr <- bs.PutNode(racer)
						}
					case s == step && !failed:
						failed = true
						return injected
					}
					return nil
				}
				if err := inOneFlush(bs, sync, func() { h.session2Writes(t, bs) }); !errors.Is(err, injected) {
					t.Fatalf("the flush = %v, want the injected failure", err)
				}
				if err := <-racerErr; sync && !errors.Is(err, ErrStoreClosed) {
					t.Fatalf("a SyncWrites write racing the failure = %v, want ErrStoreClosed", err)
				}
				doors := map[string]func() error{
					"PutNode": func() error { return bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(8_000_001)), 7, nil)) },
					"ReplaceNode": func() error {
						n := types.NewNode(h.added[0].ID(), 8, nil)
						_ = n.SetProperty("k", int64(-5))
						return bs.ReplaceNode(n)
					},
					"DeleteNode":   func() error { return bs.DeleteNode(h.added[1].ID()) },
					"GetNode":      func() error { _, err := bs.GetNode(h.added[2].ID()); return err },
					"Flush":        bs.Flush,
					"DurableFlush": bs.DurableFlush,
					"GroupCommit": func() error {
						bs.BeginGroupCommit()
						return bs.EndGroupCommit()
					},
					"ForEachChange": func() error {
						return bs.ForEachChange(0, func(storecontract.ChangeRecord) bool { return true })
					},
				}
				for name, door := range doors {
					if err := door(); !errors.Is(err, ErrStoreClosed) {
						t.Errorf("%s after the failure = %v, want ErrStoreClosed", name, err)
					}
				}
				if err := bs.Close(); err == nil {
					t.Fatal("Close of a failed-closed store reported success")
				}

				bs, err = New(cfg)
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				t.Cleanup(func() { _ = bs.Close() })
				if got := liveNodes(t, bs, h.added); got != len(h.added) {
					t.Fatalf("%d of %d staged nodes live after the reopen", got, len(h.added))
				}
				if got := liveNodes(t, bs, []*types.Node{racer}); got != 0 {
					t.Fatal("the write racing the failure was committed after the marker")
				}
				n, err := bs.GetNode(h.added[0].ID())
				if err != nil {
					t.Fatal(err)
				}
				if v, _ := n.GetProperty("k"); v != int64(0) {
					t.Fatalf("node k = %v, want the staged 0", v)
				}
				count, err := bs.NodeCount()
				if err != nil {
					t.Fatal(err)
				}
				if want := len(h.added) + len(h.base) - 500; count != want {
					t.Fatalf("node count %d, want %d", count, want)
				}
				ids, err := bs.NodeIDsByLabel(8)
				if err != nil {
					t.Fatal(err)
				}
				if len(ids) != len(h.added) {
					t.Fatalf("label 8 lists %d nodes, want %d (a stale image?)", len(ids), len(h.added))
				}
			})
		}
	}
}

// TestStagedLeftoverWithoutMarkerReadOnly: staging without a marker (a crash
// before the commit point) is invisible to a read-only open, which succeeds
// and answers the state before the flush; the next writable open deletes it.
// Catches a read-only open that refuses (or applies) uncommitted staging.
func TestStagedLeftoverWithoutMarkerReadOnly(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nodes := splitFlushNodes(splitFlushNodeCount)
	putNodes(t, bs, nodes)
	injected := failAt(bs, stagedStepStaged)
	if err := bs.Flush(); !errors.Is(err, injected) {
		t.Fatalf("Flush = %v", err)
	}
	crashStop(bs)
	ro := cfg
	ro.ReadOnly = true
	bs, err = New(ro)
	if err != nil {
		t.Fatalf("read-only open with uncommitted staging: %v", err)
	}
	if got := liveNodes(t, bs, nodes); got != 0 {
		t.Fatalf("read-only open shows %d nodes of an uncommitted flush", got)
	}
	if keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}) == 0 {
		t.Fatal("fixture: no staging left")
	}
	_ = bs.Close()
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
		t.Fatalf("writable open left %d staging keys", n)
	}
	if got := liveNodes(t, bs, nodes); got != 0 {
		t.Fatalf("%d nodes of an uncommitted flush", got)
	}
}

// crashedAtMarker leaves dir with a committed, unapplied staged flush of nodes.
func crashedAtMarker(t *testing.T, cfg Config, nodes []*types.Node) {
	t.Helper()
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	putNodes(t, bs, nodes)
	injected := failAt(bs, stagedStepMarked)
	if err := bs.Flush(); !errors.Is(err, injected) {
		t.Fatalf("Flush = %v", err)
	}
	crashStop(bs)
}

// TestStagedRecoveryRefusesDamage: a damaged marker or staged entry (a short
// marker, a synced marker whose staging is missing, an empty staged value, a
// staged entry more than the marker counts: none of them a power loss) fails
// the open with ErrInvalidStoreMutation instead of applying a guess, and
// DiscardStagedFlush clears it. Catches a recovery that applies whatever it
// finds, or discards damage it cannot explain.
func TestStagedRecoveryRefusesDamage(t *testing.T) {
	damage := map[string]func(txn *badgerv4.Txn) error{
		"short marker": func(txn *badgerv4.Txn) error {
			return txn.Set(storepkg.FlushStageMarkerKey, make([]byte, 15))
		},
		"synced marker names no staging": func(txn *badgerv4.Txn) error {
			v := make([]byte, stageMarkerLen)
			binary.BigEndian.PutUint64(v, 1)
			binary.BigEndian.PutUint64(v[8:], 3)
			v[16] = stageMarkerSynced
			return txn.Set(storepkg.FlushStageMarkerKey, v)
		},
		"extra staged entry": func(txn *badgerv4.Txn) error {
			item, err := txn.Get(storepkg.FlushStageMarkerKey)
			if err != nil {
				return err
			}
			m, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			key := append(stagePrefix(binary.BigEndian.Uint64(m)), storepkg.MetaKey("flushpart_extra")...)
			return txn.Set(key, []byte{byte(writeOpSet), 1})
		},
		"empty staged value": func(txn *badgerv4.Txn) error {
			it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyFlushStage}})
			defer it.Close()
			it.Rewind()
			if !it.Valid() {
				return errors.New("no staging")
			}
			return txn.Set(it.Item().KeyCopy(nil), nil)
		},
	}
	for name, fn := range damage {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := splitFlushConfig(dir)
			crashedAtMarker(t, cfg, splitFlushNodes(splitFlushNodeCount))
			updateRawBadgerDir(t, dir, fn)
			bs, err := New(cfg)
			if err == nil {
				_ = bs.Close()
				t.Fatal("New applied a damaged staged flush")
			}
			if !errors.Is(err, ErrInvalidStoreMutation) || !strings.Contains(err.Error(), "DiscardStagedFlush") {
				t.Fatalf("New = %v, want ErrInvalidStoreMutation naming DiscardStagedFlush", err)
			}
			if dropped, err := DiscardStagedFlush(cfg); err != nil || !dropped {
				t.Fatalf("DiscardStagedFlush = %v, %v", dropped, err)
			}
			bs, err = New(cfg)
			if err != nil {
				t.Fatalf("open after DiscardStagedFlush: %v", err)
			}
			_ = bs.Close()
		})
	}
}

// TestStagedRecoveryAppliesOnlyTheMarkedSequence: staging of another
// sequence (an aborted attempt) next to the marked one is deleted, never
// applied. Catches a recovery that applies every 0x0E key.
func TestStagedRecoveryAppliesOnlyTheMarkedSequence(t *testing.T) {
	dir := t.TempDir()
	cfg := splitFlushConfig(dir)
	nodes := splitFlushNodes(splitFlushNodeCount)
	crashedAtMarker(t, cfg, nodes)
	probe := storepkg.MetaKey("flushpart_foreign_probe")
	updateRawBadgerDir(t, dir, func(txn *badgerv4.Txn) error {
		return txn.Set(append(stagePrefix(7), probe...), append([]byte{byte(writeOpSet)}, 1))
	})
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if got := liveNodes(t, bs, nodes); got != len(nodes) {
		t.Fatalf("%d of %d nodes", got, len(nodes))
	}
	if n := keysWithPrefix(t, bs, probe); n != 0 {
		t.Fatal("recovery applied staging of another sequence")
	}
	if n := keysWithPrefix(t, bs, []byte{storepkg.KeyFlushStage}); n != 0 {
		t.Fatalf("%d staging keys left", n)
	}
}

// TestStagedFlushCleanReopenTrustsImage: a staged flush followed by a clean
// Close leaves nothing for recovery to do, so the reopen trusts the open
// image and decodes no row. Catches a recovery that writes on every open
// (each open would fall back to the full rebuild).
func TestStagedFlushCleanReopenTrustsImage(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	putNodes(t, bs, splitFlushNodes(splitFlushNodeCount))
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if storedFormatMarker(t, bs) != storepkg.StagedFlushStoreFormatVersion {
		t.Fatal("fixture: the flush was not staged")
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	var reopened *Store
	decodes := CountRowDecodesForTest(func() { reopened, err = New(cfg) })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if decodes != 0 {
		t.Fatalf("the reopen decoded %d rows: the image was not trusted", decodes)
	}
}

// TestStageSeqNeverRepeats: staging sequences of one store are unique under
// concurrency and increase. Catches a sequence reused by two attempts (their
// staging would mix under one marker).
func TestStageSeqNeverRepeats(t *testing.T) {
	bs := &Store{}
	const goroutines, each = 8, 1_000
	seen := make(chan uint64, goroutines*each)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				seen <- bs.nextStageSeq()
			}
		}()
	}
	wg.Wait()
	close(seen)
	set := make(map[uint64]bool)
	for s := range seen {
		if set[s] {
			t.Fatalf("sequence %d issued twice", s)
		}
		set[s] = true
	}
	a, b := bs.nextStageSeq(), bs.nextStageSeq()
	if b <= a {
		t.Fatalf("sequence %d after %d", b, a)
	}
}

// digestDiff lists the lines only one digest has (at most 20).
func digestDiff(got, want string) string {
	g, w := map[string]bool{}, map[string]bool{}
	for _, l := range strings.Split(got, "\n") {
		g[l] = true
	}
	for _, l := range strings.Split(want, "\n") {
		w[l] = true
	}
	var out []string
	for l := range g {
		if !w[l] {
			out = append(out, "+ "+l)
		}
	}
	for l := range w {
		if !g[l] {
			out = append(out, "- "+l)
		}
	}
	slices.Sort(out)
	if len(out) > 20 {
		out = append(out[:20], fmt.Sprintf("... %d more", len(out)-20))
	}
	return strings.Join(out, "\n")
}

// TestStagedFlushDuplicateKeysKeepTheLast: a staged flush whose entries name a
// key twice (no entry source does today) stages and counts it once, so a
// crash after the marker reopens with the last value instead of a count
// refusal. Catches a marker that counts entries rather than staged keys.
func TestStagedFlushDuplicateKeysKeepTheLast(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	entries := make([]flushEntry, 0, 20_001)
	for i := 0; i < 20_000; i++ {
		entries = append(entries, flushEntry{key: storepkg.MetaKey(fmt.Sprintf("flushpart_dup_%05d", i)), value: []byte{0}})
	}
	dup := storepkg.MetaKey("flushpart_dup_00007")
	entries = append(entries, flushEntry{key: dup, value: []byte{9}})
	injected := failAt(bs, stagedStepMarked)
	if _, err := bs.commitFlushEntries(entries); !errors.Is(err, injected) {
		t.Fatalf("commit = %v, want the injected failure (a staged flush)", err)
	}
	crashStop(bs)
	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.DBForTest().View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(dup)
		if err != nil {
			return err
		}
		return item.Value(func(v []byte) error {
			if len(v) != 1 || v[0] != 9 {
				t.Fatalf("duplicated key holds %x, want the last value 09", v)
			}
			return nil
		})
	}); err != nil {
		t.Fatal(err)
	}
}

// TestStagedDurableFlushReportsTheFailure: a DurableFlush whose staged flush
// fails after the marker reports the failure (matching both ErrStoreClosed
// and the cause), and a retry is refused: the commit is not durable now, and
// the next open applies it. Catches a DurableFlush that reports success
// before the apply, or a retry that commits after the marker.
func TestStagedDurableFlushReportsTheFailure(t *testing.T) {
	cfg := splitFlushConfig(t.TempDir())
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	nodes := splitFlushNodes(splitFlushNodeCount)
	putNodes(t, bs, nodes)
	injected := failAt(bs, stagedStepApplyWritten)
	err = bs.DurableFlush()
	if !errors.Is(err, injected) || !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DurableFlush = %v, want the failure matching ErrStoreClosed and its cause", err)
	}
	if err := bs.DurableFlush(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("retried DurableFlush = %v, want ErrStoreClosed", err)
	}
	_ = bs.Close()
	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if got := liveNodes(t, bs, nodes); got != len(nodes) {
		t.Fatalf("%d of %d nodes after the reopen applied the flush", got, len(nodes))
	}
}

// TestStagedFlushEntryAtBadgerLimits: staging adds 9 bytes to a key and 1 to a
// value, so the write door's limits (maxEntryKey, maxEntryValue) leave that
// room: an entry exactly at them stages and commits; one byte above, the door
// refuses it (TestEntryTooLargeRefusedAtEveryDoor). Mutation: limits without
// the staging overhead (the staged flush then fails and is requeued forever).
func TestStagedFlushEntryAtBadgerLimits(t *testing.T) {
	small := func(n int) []flushEntry {
		e := make([]flushEntry, 0, n+1)
		for i := 0; i < n; i++ {
			e = append(e, flushEntry{key: storepkg.MetaKey(fmt.Sprintf("zz_%06d", i)), value: []byte{1}})
		}
		return e
	}
	t.Run("key at the door limit", func(t *testing.T) {
		bs, err := New(Config{Dir: t.TempDir(), MemTableSize: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		long := make([]byte, bs.maxEntryKey)
		long[0] = 0x0C // an unused keyspace
		entries := append(small(20_000), flushEntry{key: long, value: []byte{2}})
		if state, err := bs.commitFlushEntries(entries); err != nil {
			t.Fatalf("a staged flush with a %d-byte key (the door limit): state %d, %v", len(long), state, err)
		}
	})
	t.Run("value at the door limit", func(t *testing.T) {
		bs, err := New(Config{Dir: t.TempDir(), MemTableSize: 8 << 20, ValueLogFileSize: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		defer bs.Close()
		entries := append(small(20_000), flushEntry{key: []byte{0x0C, 1}, value: make([]byte, bs.maxEntryValue)})
		if state, err := bs.commitFlushEntries(entries); err != nil {
			t.Fatalf("a staged flush with a %d-byte value (the door limit): state %d, %v", bs.maxEntryValue, state, err)
		}
	})
}
