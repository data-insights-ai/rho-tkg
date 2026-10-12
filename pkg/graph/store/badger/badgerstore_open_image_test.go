package badger

import (
	"encoding/binary"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// After a clean reopen of an image without statistics (closed with
// DisablePlannerStats), statistics and integrity hashes are built on first use
// while writes run: the deferred statistics build and the lazy hash fill must
// end where a full rebuild of the same rows does.
func TestCleanReopenDeferredStateUnderConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, DisablePlannerStats: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(adLabel, "qty"); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 400; id++ {
		if err := bs.PutNode(adNode(id, id%11)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	// Writes stay in the buffer until Close: the deferred build must read them there.
	if bs, err = New(Config{Dir: dir, FlushInterval: time.Hour}); err != nil {
		t.Fatal(err)
	}
	if !bs.statsDeferred.Load() {
		t.Fatal("clean reopen did not defer planner statistics")
	}
	// Before the first statistics read: a replaced row (newer in the cache than
	// in badger) and a new row (only in the write buffer).
	for id := int64(401); id <= 420; id++ {
		replaced := adNode(id-400, 5000+id)
		_ = replaced.SetProperty("tag", "replaced") // a key only the cached row has
		if err := bs.ReplaceNode(replaced); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(adNode(id+5000, id)); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	for w := int64(0); w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := 1 + w; id <= 400; id += 4 {
				var err error
				switch id % 3 {
				case 0:
					err = bs.ReplaceNode(adNode(id, 1000+id))
				case 1:
					err = bs.DeleteNode(types.NodeID(id))
				default:
					err = bs.PutNode(adNode(1000+id, id))
				}
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for id := int64(1); id <= 400; id++ {
				_, _ = bs.NodePropertyStats(adLabel, "qty")
				_, _ = bs.NodeIntegrityHash(types.NodeID(id))
			}
		}()
	}
	wg.Wait()
	for id := range bs.nodeIDs {
		n, err := bs.GetNode(id)
		if err != nil {
			t.Fatal(err)
		}
		if h, err := bs.NodeIntegrityHash(id); err != nil || h != badgerNodeIntegrityHash(n) {
			t.Fatalf("node %d: integrity hash %q (%v), row says %q", id, h, err, badgerNodeIntegrityHash(n))
		}
	}
	live := bs.PlannerStatsDigestForTest()
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	full, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer full.Close()
	if full.statsDeferred.Load() {
		t.Fatal("oracle open trusted the image")
	}
	// NDV never decreases (docs/query-planners.md "Deletion semantics"): the
	// writes after the build keep their removed values in the live sketch,
	// a rebuild drops them. Every exact statistic must agree.
	ndv := regexp.MustCompile(`NDV:\d+`)
	if want := full.PlannerStatsDigestForTest(); ndv.ReplaceAllString(live, "") != ndv.ReplaceAllString(want, "") {
		t.Fatalf("statistics after concurrent writes:\n got: %s\nwant: %s", live, want)
	}
}

// statsStore writes nodes and relationships whose planner statistics carry
// every persisted part: counters, a type-class mix, NDV inflated by deletes
// (it never decreases) and, with dirty, a dirty min/max (its holder deleted).
// It returns the raw statistics the closing process held.
func statsStore(t *testing.T, cfg Config, dirty bool) string {
	t.Helper()
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 60; id++ {
		n := adNode(id, id%13)
		if id%5 == 0 {
			_ = n.SetProperty("tag", "s") // a second key and class
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for id := int64(1); id <= 40; id++ {
		r := types.NewRelationship(types.RelID(1000+id), 3, types.NodeID(id), types.NodeID(id+1))
		_ = r.SetProperty("w", float64(id)/4)
		if id%7 == 0 {
			_ = r.SetProperty("w", "text")
		}
		if err := bs.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	gone := types.NodeID(14) // qty 1, w 3.25 and 3.5: no minimum or maximum
	if dirty {
		gone = 12 // holds the max qty: the node accumulator turns dirty
	}
	for _, id := range []types.RelID{1025, types.RelID(999 + gone), types.RelID(1000 + gone)} {
		if err := bs.DeleteRelationship(id); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.DeleteNode(gone); err != nil {
		t.Fatal(err)
	}
	held := bs.PlannerStatsStateForTest()
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	return held
}

// statsReaders are the public statistics doors, each as a first read.
var statsReaders = map[string]func(bs *Store) string{
	"NodeCountByLabelAndPropertyKey": func(bs *Store) string {
		return fmt.Sprint(bs.NodeCountByLabelAndPropertyKey(adLabel, "qty"))
	},
	"NodePropertyTypeClassCounts": func(bs *Store) string {
		return fmt.Sprint(bs.NodePropertyTypeClassCounts(adLabel, "tag"))
	},
	"NodePropertyStats": func(bs *Store) string { return fmt.Sprint(bs.NodePropertyStats(adLabel, "qty")) },
	"NodePropertyStatsSketch": func(bs *Store) string {
		h, mn, mx, n, err := bs.NodePropertyStatsSketch(adLabel, "qty")
		return fmt.Sprint(h.Estimate(), mn, mx, n, err)
	},
	"NodePropertyNDV": func(bs *Store) string { return fmt.Sprint(bs.NodePropertyNDV(adLabel, "qty")) },
	"NodePropertyNDVSketch": func(bs *Store) string {
		h, n, err := bs.NodePropertyNDVSketch(adLabel, "qty")
		return fmt.Sprint(h.Estimate(), n, err)
	},
	"RelPropertyStats":           func(bs *Store) string { return fmt.Sprint(bs.RelPropertyStats(3, "w")) },
	"RelPropertyNDV":             func(bs *Store) string { return fmt.Sprint(bs.RelPropertyNDV(3, "w")) },
	"RelPropertyTypeClassCounts": func(bs *Store) string { return fmt.Sprint(bs.RelPropertyTypeClassCounts(3, "w")) },
}

// A clean reopen restores the closing process's planner statistics: nothing
// deferred, the raw state (counters, NDV, typed min/max, dirty flag) equal,
// and the first read through any door decodes no row.
func TestCleanReopenRestoresPlannerStats(t *testing.T) {
	dir := t.TempDir()
	held := statsStore(t, Config{Dir: dir}, true)
	if !strings.Contains(held, "dirty=true") {
		t.Fatalf("fixture lacks a dirty accumulator:\n%s", held)
	}
	bs, err := New(Config{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if bs.StatsDeferredForTest() {
		t.Error("clean reopen deferred planner statistics its image holds")
	}
	if got := bs.PlannerStatsStateForTest(); got != held {
		t.Errorf("restored statistics differ from the closing process's:\n got: %s\nwant: %s", got, held)
	}
	_ = bs.Close()

	// The first read through each door decodes no row (an accumulator whose
	// min/max is dirty rescans by design, so this fixture keeps them exact).
	clean := t.TempDir()
	statsStore(t, Config{Dir: clean}, false)
	for name, read := range statsReaders {
		t.Run(name, func(t *testing.T) {
			bs, err := New(Config{Dir: clean, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			if n := CountRowDecodesForTest(func() { _ = read(bs) }); n != 0 {
				t.Fatalf("first %s after a clean reopen decoded %d rows", name, n)
			}
		})
	}
}

// Every statistics door answers its first read after a reopen of an image
// without statistics (closed with DisablePlannerStats) with the full rebuild's
// value, not the empty deferred state.
func TestFirstStatsReadThroughEachDoorAfterDeferredOpen(t *testing.T) {
	dir := t.TempDir()
	statsStore(t, Config{Dir: dir, DisablePlannerStats: true}, true)
	oracle := t.TempDir()
	copyDirFiles(t, dir, oracle)
	db, err := badgerv4.Open(badgerv4.DefaultOptions(oracle).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	for name, read := range statsReaders {
		t.Run(name, func(t *testing.T) {
			full, err := New(Config{Dir: oracle, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			want := read(full)
			_ = full.Close()
			bs, err := New(Config{Dir: dir, ReadOnly: true})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			if !bs.StatsDeferredForTest() {
				t.Fatal("image without statistics did not defer them")
			}
			if got := read(bs); got != want {
				t.Fatalf("first %s: %s, full rebuild says %s", name, got, want)
			}
		})
	}
}

// A deferred build that fails half way returns the error, keeps nothing it
// half-built and stays deferred: the next read builds the full statistics.
func TestPlannerStatsBuildFailureRetries(t *testing.T) {
	dir := t.TempDir()
	statsStore(t, Config{Dir: dir, DisablePlannerStats: true}, false)
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	injected := errors.New("injected scan failure")
	statsBuildFaultForTest = func() error { statsBuildFaultForTest = nil; return injected }
	t.Cleanup(func() { statsBuildFaultForTest = nil })
	if _, err := bs.NodeCountByLabelAndPropertyKey(adLabel, "qty"); !errors.Is(err, injected) {
		t.Fatalf("first read after a failed build: err %v, want the scan failure", err)
	}
	if !bs.StatsDeferredForTest() || bs.PlannerStatsStateForTest() != "" {
		t.Fatalf("failed build left deferred=%v and state:\n%s", bs.StatsDeferredForTest(), bs.PlannerStatsStateForTest())
	}
	if n, err := bs.NodeCountByLabelAndPropertyKey(adLabel, "qty"); err != nil || n != 59 {
		t.Fatalf("retry: %d, %v; want 59 nodes with qty", n, err)
	}
}

// A write that passed checkWritable before Close set closing can still change
// RAM after the image was encoded. Its flush must not commit before the
// image header: a header that is the newest commit vouches for an image
// without that write.
func TestCloseImageExcludesWriteRacingClose(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(adLabel, "qty"); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 20; id++ {
		if err := bs.PutNode(adNode(id, id)); err != nil {
			t.Fatal(err)
		}
	}
	flushDone := make(chan struct{})
	openImageAfterEncodeHook = func() {
		openImageAfterEncodeHook = nil
		bs.closing.Store(false) // the writer checked before Close began
		err := bs.PutNode(adNode(99, 7))
		bs.closing.Store(true)
		if err != nil {
			t.Error(err)
		}
		go func() {
			defer close(flushDone)
			_ = bs.flush() // its flush (SyncWrites / backpressure / DurableFlush path)
		}()
		select {
		case <-flushDone:
		case <-time.After(300 * time.Millisecond): // blocked behind Close: the fix
		}
	}
	t.Cleanup(func() { openImageAfterEncodeHook = nil })
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	<-flushDone

	oracle := t.TempDir()
	copyDirFiles(t, dir, oracle)
	db, err := badgerv4.Open(badgerv4.DefaultOptions(oracle).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	digest := func(d string) string {
		s, err := New(Config{Dir: d, ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		return s.IndexStateDigestForTest()
	}
	if got, want := digest(dir), digest(oracle); got != want {
		t.Fatalf("reopen trusted an image that misses the write racing Close:\n got: %.600s\nwant: %.600s", got, want)
	}
}

// The image header guards a decodable blob with its length and checksum: a
// header rewritten so it is still the newest commit, but naming a different
// length or checksum, must send the open to the full rebuild (the checks the
// graph-level oracle test cannot isolate, because a damaged chunk usually
// also fails to decode).
func TestOpenImageHeaderGuards(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(hdr []byte) []byte
	}{
		{"checksum", func(h []byte) []byte { h[len(h)-1] ^= 1; return h }},
		{"length", func(h []byte) []byte {
			r := imgReader{b: h[len(openImageMagic):]}
			chunks, length := r.uvarint(), r.uvarint()
			out := binary.AppendUvarint([]byte(openImageMagic), chunks)
			out = binary.AppendUvarint(out, length+1)
			return append(out, r.b...)
		}},
		{"magic", func(h []byte) []byte { h[0] ^= 1; return h }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.CreatePropertyIndex(adLabel, "qty"); err != nil {
				t.Fatal(err)
			}
			for id := int64(1); id <= 50; id++ {
				if err := bs.PutNode(adNode(id, id%7)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			reopen := func() int64 {
				return CountRowDecodesForTest(func() {
					bs, err := New(Config{Dir: dir})
					if err != nil {
						t.Fatal(err)
					}
					if err := bs.Close(); err != nil {
						t.Fatal(err)
					}
				})
			}
			if n := reopen(); n != 0 {
				t.Fatalf("intact image: clean reopen decoded %d rows", n)
			}
			db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(txn *badgerv4.Txn) error {
				item, err := txn.Get(openImageHeaderKey)
				if err != nil {
					return err
				}
				hdr, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				return txn.Set(openImageHeaderKey, tc.damage(hdr))
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if n := reopen(); n < 50 {
				t.Fatalf("damaged header (%s) trusted: reopen decoded %d rows, want a full rebuild", tc.name, n)
			}
		})
	}
}
