package badger

import (
	"encoding/binary"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// After a clean reopen, statistics and integrity hashes are built on first use
// while writes run: the deferred statistics build and the lazy hash fill must
// end where a full rebuild of the same rows does.
func TestCleanReopenDeferredStateUnderConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir})
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
