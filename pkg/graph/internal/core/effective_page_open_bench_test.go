package core

import (
	"context"
	"fmt"
	"runtime"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// openImageBytes sums the open image's header and chunk values in dir.
func openImageBytes(b *testing.B, dir string) int64 {
	b.Helper()
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	var n int64
	prefix := storeutil.MetaKey("tkg_open_image")
	if err := db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n += it.Item().ValueSize()
		}
		return nil
	}); err != nil {
		b.Fatal(err)
	}
	return n
}

// BenchmarkEffectivePageFirstAfterOpen (backlog 74, ai-soc: "after every
// open, the first page waits about 3 minutes"): a disk store with 29 indexed
// relationship types and B74_BENCH_RELS relationships (default 200,000; every
// fourth updated once, so a quarter carry a history row). Each iteration
// opens the store and asks one page of 500; open-ms and first-page-ms are the
// two halves of "open to first answer". clean reopens after a clean close
// (the open image is trusted); dirty deletes the persisted open state first,
// as after a crash or an older release (the open rebuilds from the rows).
func BenchmarkEffectivePageFirstAfterOpen(b *testing.B) {
	const nTypes, limit = 29, 500
	n := benchEnvInt(b, "B74_BENCH_RELS", 200_000, 1_000, 10_000_000)
	ctx := context.Background()
	dir := b.TempDir()
	g, err := New(Config{BadgerDir: dir})
	if err != nil {
		b.Fatal(err)
	}
	s, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	e, err := g.Nodes.Add(ctx, []string{"Host"}, nil)
	if err != nil {
		b.Fatal(err)
	}
	relTypes := make([]string, nTypes)
	for i := range relTypes {
		relTypes[i] = fmt.Sprintf("T%02d", i)
		if err := g.Index.CreateRelProperty(relTypes[i], epProp); err != nil {
			b.Fatal(err)
		}
	}
	for i := 0; i < n; i++ {
		r, err := g.Rels.AddByID(ctx, relTypes[i%nTypes], s.ID(), e.ID(), map[string]any{epProp: fmt.Sprintf("e%d", i%10), "seq": int64(i)})
		if err != nil {
			b.Fatal(err)
		}
		if i%4 == 0 {
			if _, err := g.Rels.Update(ctx, r.ID(), map[string]any{"seq": int64(-i)}); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := g.Close(); err != nil {
		b.Fatal(err)
	}
	// size: the open image without and with the 29 sidecars, and their
	// resident bytes: the heap (after GC) of a clean open whose image holds
	// them minus that of one whose image does not. Both opens read keys and
	// the image only, so the badger caches match; a heap delta across the
	// build scan would count the block cache the scan fills.
	b.Run("size", func(b *testing.B) {
		heap := func() uint64 {
			var ms runtime.MemStats
			runtime.GC()
			runtime.GC()
			runtime.ReadMemStats(&ms)
			return ms.HeapAlloc
		}
		openHeap := func() (*Core, uint64) {
			g, err := New(Config{BadgerDir: dir})
			if err != nil {
				b.Fatal(err)
			}
			return g, heap()
		}
		without := openImageBytes(b, dir)
		g, before := openHeap()
		tok, _ := g.relTypes.Lookup(relTypes[0])
		if err := g.store.(storepkg.RelPropertyTxMembershipCapability).ForEachRelPropertyTxMember(tok, epProp, "phantom", func(types.RelID, types.Instant) bool { return true }); err != nil {
			b.Fatal(err)
		}
		st, err := g.store.(storepkg.PropertyTxMembershipStatsCapability).PropertyTxMembershipStats()
		if err != nil || st.RelSidecars != nTypes {
			b.Fatalf("stats %+v, %v", st, err)
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
		with := openImageBytes(b, dir)
		g, after := openHeap()
		if st, _ := g.store.(storepkg.PropertyTxMembershipStatsCapability).PropertyTxMembershipStats(); st.RelSidecars != nTypes || st.Builds != 0 {
			b.Fatalf("restored stats %+v", st)
		}
		if err := g.Close(); err != nil {
			b.Fatal(err)
		}
		b.ReportMetric(float64(st.RelPostings), "postings")
		b.ReportMetric(float64(after-before)/float64(st.RelPostings), "sidecar-B/posting")
		b.ReportMetric(float64(after-before)/(1<<20), "sidecar-MiB")
		b.ReportMetric(float64(without)/(1<<20), "image-MiB-without")
		b.ReportMetric(float64(with)/(1<<20), "image-MiB-with")
	})
	for _, mode := range []string{"clean", "dirty"} {
		b.Run(mode, func(b *testing.B) {
			var openD, pageD time.Duration
			for i := 0; i < b.N; i++ {
				if mode == "dirty" {
					db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
					if err != nil {
						b.Fatal(err)
					}
					if err := db.Update(badger.DeleteOpenStateForTest); err != nil {
						b.Fatal(err)
					}
					if err := db.Close(); err != nil {
						b.Fatal(err)
					}
				}
				t0 := time.Now()
				g, err := New(Config{BadgerDir: dir})
				if err != nil {
					b.Fatal(err)
				}
				t1 := time.Now()
				pin, _ := g.Temporal.NowTx()
				segs, _, err := g.Temporal.RelsEffectivePage(ctx, temporal.Pin{Tx: pin}, temporal.Filter{Prop: epProp, Eq: "e3"}, 0, limit)
				if err != nil {
					b.Fatal(err)
				}
				t2 := time.Now()
				if len(segs) < limit {
					b.Fatalf("first page holds %d segments, want >= %d", len(segs), limit)
				}
				openD += t1.Sub(t0)
				pageD += t2.Sub(t1)
				if err := g.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(openD.Milliseconds())/float64(b.N), "open-ms")
			b.ReportMetric(float64(pageD.Milliseconds())/float64(b.N), "first-page-ms")
		})
	}
}
