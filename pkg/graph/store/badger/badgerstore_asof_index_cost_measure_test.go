//go:build measure

// The as-of index's write cost on disk, index on against off in one binary
// (asOfIndexOff, badgerstore_asof_index_switch_measure.go). Wall time and
// runtime.MemStats are evidence, never a pass/fail, so this runs by hand:
//
//	go test -tags measure -count=1 -timeout 120m -v -run '^TestAsOfAppendCostOnDisk$' ./pkg/graph/store/badger
//	go test -tags measure -count=1 -v -run '^TestAsOfProofsAfterUncleanClose$' ./pkg/graph/store/badger
//	go test -tags measure -count=1 -v -run '^TestAsOfQuietReadersBesideFlushes$' ./pkg/graph/store/badger
//
// ASOFCOST_HOT (M1 appends a phase, default 200,000) and ASOFCOST_ENTITIES
// (M2 entities, default 1,000,000) scale the run down; ASOFCOST_PROBE_MS
// (default 1,500) is the length of one TestAsOfQuietReadersBesideFlushes cell.

package badger

import (
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

const (
	asOfCostPayload      = 512 // bytes of the row's one property
	asOfCostDurableEvery = 177 // records between DurableFlush calls in the durable mode
	asOfCostCreatorPause = 50 * time.Microsecond
)

func asOfCostEnv(name string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(name)); err == nil && v > 0 {
		return v
	}
	return def
}

// asOfCostCfg: ai-soc's badger settings as asocCfg carries them for a shard
// (NoCompression, a 64 MiB block cache, delta history), flush interval and
// pending-write cap at the defaults.
func asOfCostCfg(dir string) Config {
	return Config{Dir: dir, NoCompression: true, BlockCacheSize: 64 << 20, HistoryDeltaEncoding: true}
}

type asOfCostLat []time.Duration

func (l asOfCostLat) String() string {
	if len(l) == 0 {
		return "n 0"
	}
	s := slices.Clone(l)
	slices.Sort(s)
	var sum time.Duration
	for _, d := range s {
		sum += d
	}
	us := func(d time.Duration) string { return fmt.Sprintf("%.2f", float64(d)/1e3) }
	return fmt.Sprintf("n %d mean %s med %s p99 %s max %s us", len(s), us(sum/time.Duration(len(s))),
		us(s[len(s)/2]), us(s[len(s)*99/100]), us(s[len(s)-1]))
}

type asOfCostPhase struct {
	lat                asOfCostLat
	creator            asOfCostLat
	reader             asOfCostLat // as-of reads beside the appends, back to back
	indexed            int64       // as-of seek keys those reads visited: the answers the index gave
	wall, flush        time.Duration
	txns, gets, seeks  int64 // txns: every read transaction the store opened (ReadTxnsTotalForTest)
	snaps              int64 // the as-of views the flushes opened
	mallocs, allocByte uint64
}

func (p asOfCostPhase) String() string {
	n := float64(len(p.lat))
	s := fmt.Sprintf("append %v; wall/record %.2f us (durable flushes %.0f ms); per append: store read txns %.3f, hook gets %.3f seeks %.3f, hook views opened by flushes %.3f; allocs/append %.1f (%.0f B, process-wide)",
		p.lat, float64(p.wall)/1e3/n, float64(p.flush)/1e6, float64(p.txns)/n, float64(p.gets)/n, float64(p.seeks)/n, float64(p.snaps)/n,
		float64(p.mallocs)/n, float64(p.allocByte)/n)
	if len(p.creator) > 0 {
		s += fmt.Sprintf("\n      creator PutNode beside it: %v", p.creator)
	}
	if len(p.reader) > 0 {
		reads := float64(len(p.reader))
		s += fmt.Sprintf("\n      as-of reader beside it: %v; %.0f reads/s; index answers %d of %d reads (%.1f %%), not through the index %d",
			p.reader, reads/p.wall.Seconds(), p.indexed, len(p.reader), 100*float64(p.indexed)/reads, int64(len(p.reader))-p.indexed)
	}
	return s
}

// asOfCostRun times n appends (fn), with a DurableFlush every
// asOfCostDurableEvery records when durable, with a paced node creator (no
// history row) recording each PutNode's wall time when creator is set, and
// with one as-of reader reading back to back, recording each read's wall
// time, when reader is set (reader(i) reads an entity written before the
// phase: a chain with nothing buffered). The reads' index answers are the
// as-of seek keys visited meanwhile; the rest declined (index on) or scanned
// (index off).
func asOfCostRun(t *testing.T, bs *Store, n int, durable, creator bool, creatorBase int64, reader func(i int) error, fn func(i int) error) (p asOfCostPhase) {
	t.Helper()
	p.lat = make(asOfCostLat, 0, n)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	if reader != nil {
		seeks, stopSeeks := CountAsOfIndexSeekEntriesForTest()
		defer func() { p.indexed = seeks(); stopSeeks() }()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				if err := reader(i); err != nil {
					t.Error(err)
					return
				}
				p.reader = append(p.reader, time.Since(start))
			}
		}()
	}
	if creator {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := int64(0); ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				node := types.NewNode(types.NodeID(creatorBase+i), 1, nil)
				node.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: 1})
				if err := node.SetProperty("payload", strings.Repeat("c", asOfCostPayload)); err != nil {
					t.Error(err)
					return
				}
				start := time.Now()
				if err := bs.PutNode(node); err != nil {
					t.Error(err)
					return
				}
				p.creator = append(p.creator, time.Since(start))
				time.Sleep(asOfCostCreatorPause)
			}
		}()
	}
	hook, stopHook := countAsOfHookWork()
	txns := ReadTxnsTotalForTest()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	begin := time.Now()
	for i := range n {
		start := time.Now()
		if err := fn(i); err != nil {
			t.Fatal(err)
		}
		p.lat = append(p.lat, time.Since(start))
		if durable && (i+1)%asOfCostDurableEvery == 0 {
			fs := time.Now()
			if err := bs.DurableFlush(); err != nil {
				t.Fatal(err)
			}
			p.flush += time.Since(fs)
		}
	}
	p.wall = time.Since(begin)
	runtime.ReadMemStats(&after)
	close(stop)
	wg.Wait()
	stopHook()
	p.txns, p.gets, p.seeks, p.snaps = ReadTxnsTotalForTest()-txns, hook.gets.Load(), hook.seeks.Load(), hook.snaps.Load()
	p.mallocs, p.allocByte = after.Mallocs-before.Mallocs, after.TotalAlloc-before.TotalAlloc
	return p
}

// asOfCostDisk sums the directory's files by allocated blocks: the WAL (.mem)
// and the value log (.vlog) are preallocated sparse files while open.
func asOfCostDisk(t *testing.T, dir string) string {
	t.Helper()
	var sst, vlog, wal, other int64
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		info, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) {
			continue // a table compaction deleted after ReadDir
		}
		if err != nil {
			t.Fatal(err)
		}
		var used int64
		if st, ok := info.Sys().(*syscall.Stat_t); ok {
			used = st.Blocks * 512
		}
		switch filepath.Ext(e.Name()) {
		case ".sst":
			sst += used
		case ".vlog":
			vlog += used
		case ".mem":
			wal += used
		default:
			other += used
		}
	}
	mib := func(b int64) string { return fmt.Sprintf("%.1f", float64(b)/(1<<20)) }
	return fmt.Sprintf("sst %s vlog %s wal(alloc) %s other %s MiB", mib(sst), mib(vlog), mib(wal), mib(other))
}

// asOfCostBytes reports bytes after Flush (store open), after Close (memtable
// in L0) and after a full compaction (Flatten on the raw database).
func asOfCostBytes(t *testing.T, bs *Store, cfg Config) string {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	flushed := asOfCostDisk(t, cfg.Dir)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	closed := asOfCostDisk(t, cfg.Dir)
	db, err := badgerv4.Open(buildBadgerOptions(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Flatten(4); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("after Flush: %s\n      after Close: %s\n      after Flatten: %s", flushed, closed, asOfCostDisk(t, cfg.Dir))
}

func TestAsOfAppendCostOnDisk(t *testing.T) {
	defer func() { asOfIndexOff = false }()
	hot := asOfCostEnv("ASOFCOST_HOT", 200_000)
	cold := asOfCostEnv("ASOFCOST_ENTITIES", 1_000_000)
	payload := strings.Repeat("x", asOfCostPayload)
	t.Logf("GOMAXPROCS %d; payload %d B; M1 %d appends a phase (8 versions an entity); M2 %d entities", runtime.GOMAXPROCS(0), asOfCostPayload, hot, cold)
	for _, k := range asOfBudgetKinds() {
		for _, durable := range []bool{false, true} {
			mode := map[bool]string{false: "plain", true: fmt.Sprintf("durable/%d", asOfCostDurableEvery)}[durable]
			for _, off := range []bool{true, false} {
				asOfIndexOff = off
				name := fmt.Sprintf("%s %s index %s", k.name, mode, map[bool]string{true: "off", false: "on"}[off])

				// M1 hot: 8 versions an entity back to back.
				cfg := asOfCostCfg(t.TempDir())
				bs, err := New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				m1 := func(base int64) func(i int) error {
					return func(i int) error { return k.put(bs, base+int64(i/8), uint32(i%8), payload) }
				}
				// The reader reads phase a's chains (flushed by phase b's start)
				// at an old pin, through the index when it is on.
				r1 := func(i int) error {
					_, _, err := k.read(bs, 1+int64(i%(hot/8)), 450, nil)
					return err
				}
				a := asOfCostRun(t, bs, hot, durable, false, 0, nil, m1(1))
				b := asOfCostRun(t, bs, hot, durable, true, 1<<40, nil, m1(1+int64(hot)))
				c := asOfCostRun(t, bs, hot, durable, true, 1<<41, r1, m1(1+2*int64(hot)))
				t.Logf("%s M1 hot\n   alone: %v\n   with creator: %v\n   with creator and as-of reader: %v\n      %s", name, a, b, c, asOfCostBytes(t, bs, cfg))

				// M2 cold proof: one flushed row an entity, reopen, one shuffled
				// append an entity (every proof read from a table).
				cfg = asOfCostCfg(t.TempDir())
				bs, err = New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				const base = 1 << 30
				setup := time.Now()
				for i := range cold {
					if err := k.put(bs, base+int64(i), 0, payload); err != nil {
						t.Fatal(err)
					}
				}
				if err := bs.Close(); err != nil {
					t.Fatal(err)
				}
				setupWall := time.Since(setup)
				bs, err = New(cfg)
				if err != nil {
					t.Fatal(err)
				}
				order := rand.New(rand.NewPCG(1, 2)).Perm(cold)
				third := cold / 3
				m2 := func(off int) func(i int) error {
					return func(i int) error { return k.put(bs, base+int64(order[off+i]), 1, payload) }
				}
				// The reader reads phase a's entities at pin 150 (version 0).
				r2 := func(i int) error {
					_, _, err := k.read(bs, base+int64(order[i%third]), 150, nil)
					return err
				}
				a = asOfCostRun(t, bs, third, durable, false, 0, nil, m2(0))
				b = asOfCostRun(t, bs, third, durable, true, 1<<40, nil, m2(third))
				c = asOfCostRun(t, bs, cold-2*third, durable, true, 1<<41, r2, m2(2*third))
				t.Logf("%s M2 cold (setup %d rows %.1f s)\n   alone: %v\n   with creator: %v\n   with creator and as-of reader: %v\n      %s", name, cold, setupWall.Seconds(), a, b, c, asOfCostBytes(t, bs, cfg))
			}
		}
	}
}

// TestAsOfProofsAfterUncleanClose (M4, exact counts): of the entities written
// before one unclean close, how many proofs still validate, and the history
// rows one as-of read at an old pin examines on an 8- and a 64-version chain
// before and after.
func TestAsOfProofsAfterUncleanClose(t *testing.T) {
	const singles = 1000
	payload := strings.Repeat("x", 8192) // asOfBudgetKinds' read checks this payload
	for _, k := range asOfBudgetKinds() {
		cfg := asOfCostCfg(t.TempDir())
		bs, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		for i := range int64(singles) {
			if err := k.put(bs, 1000+i, 0, payload); err != nil {
				t.Fatal(err)
			}
		}
		chains := map[int64]uint32{10: 8, 11: 64}
		for id, versions := range chains {
			for v := range versions {
				if err := k.put(bs, id, v, payload); err != nil {
					t.Fatal(err)
				}
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		examined := func(bs *Store, id int64) uint64 {
			_, m := storecontract.WithReadBudget(t.Context(), storecontract.ReadBudget{})
			v, ok, err := k.read(bs, id, 150, m.Tally("m4"))
			if err != nil || !ok || v != 0 {
				t.Fatalf("id %d: v%d ok %v err %v, want v0", id, v, ok, err)
			}
			return m.Work().HistoryExamined
		}
		valid := func(bs *Store) (n int) {
			if err := bs.view(func(txn *badgerv4.Txn) error {
				genRaw, err := asOfStoredValue(txn, storepkg.HistoryAsOfGenerationKey)
				if err != nil {
					return err
				}
				gen, err := asOfGeneration(genRaw)
				if err != nil {
					return err
				}
				ids := []int64{10, 11}
				for i := range int64(singles) {
					ids = append(ids, 1000+i)
				}
				for _, id := range ids {
					raw, err := asOfStoredValue(txn, storepkg.HistoryAsOfProofKey(asOfIndexKind(k), types.NodeID(id).SnowflakeID()))
					if err != nil {
						return err
					}
					proof, ok, err := decodeAsOfProof(raw)
					if err != nil {
						return err
					}
					if ok && proof.generation == gen {
						n++
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			return n
		}
		before := fmt.Sprintf("valid proofs %d of %d; HistoryExamined at pin 150: 8 versions %d, 64 versions %d",
			valid(bs), singles+2, examined(bs, 10), examined(bs, 11))
		bs.openImageSuppressed.Store(true) // no close header: an unclean close
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		bs, err = New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		after := fmt.Sprintf("valid proofs %d of %d; HistoryExamined at pin 150: 8 versions %d, 64 versions %d",
			valid(bs), singles+2, examined(bs, 10), examined(bs, 11))
		// The next append of an entity whose proof failed the generation
		// check writes the invalid marker: the chain stays on the scan.
		if err := k.put(bs, 10, 8, payload); err != nil {
			t.Fatal(err)
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		var marker []byte
		if err := bs.view(func(txn *badgerv4.Txn) error {
			marker, err = asOfStoredValue(txn, storepkg.HistoryAsOfProofKey(asOfIndexKind(k), types.NodeID(10).SnowflakeID()))
			return err
		}); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s M4\n   before unclean close: %s\n   after reopen:         %s\n   proof of the 8-version chain after one more append: %x; HistoryExamined %d",
			k.name, before, after, marker, examined(bs, 10))
		_ = bs.Close()
	}
}

// TestAsOfQuietReadersBesideFlushes: how many as-of reads of chains with
// nothing buffered the index answers, and how many reads finish, while other
// chains are appended and flushed and a meta key is committed in a loop (as
// tiered's commit floor). 32 flushed 8-version chains read at an old pin by 2
// readers back to back, 3 appenders of other chains, one MetaSet loop; one
// cell per store shape and kind. Counts are exact; their rates are evidence.
func TestAsOfQuietReadersBesideFlushes(t *testing.T) {
	const quiet, versions, readers, appenders = 32, 8, 2, 3
	cell := time.Duration(asOfCostEnv("ASOFCOST_PROBE_MS", 1500)) * time.Millisecond
	payload := strings.Repeat("x", 8192) // asOfBudgetKinds' read checks this payload
	for _, shape := range []struct {
		name string
		cfg  func() Config
	}{
		{"SyncWrites, disk", func() Config { c := asOfCostCfg(t.TempDir()); c.SyncWrites = true; return c }},
		{"async 1 ms, disk", func() Config { c := asOfCostCfg(t.TempDir()); c.FlushInterval = time.Millisecond; return c }},
		{"SyncWrites, in memory", func() Config { return Config{InMemory: true, SyncWrites: true, HistoryDeltaEncoding: true} }},
	} {
		for _, k := range asOfBudgetKinds() {
			bs, err := New(shape.cfg())
			if err != nil {
				t.Fatal(err)
			}
			for c := range int64(quiet) {
				for v := range uint32(versions) {
					if err := k.put(bs, 1000+c, v, payload); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			seeks, stopSeeks := CountAsOfIndexSeekEntriesForTest()
			var reads, appends, metas atomic.Int64
			stop := make(chan struct{})
			var wg sync.WaitGroup
			loop := func(step func(i int64) error, n *atomic.Int64) {
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
				loop(func(i int64) error {
					v, ok, err := k.read(bs, 1000+(i+r*quiet/readers)%quiet, 450, nil)
					if err == nil && (!ok || v != 3) {
						err = fmt.Errorf("quiet chain at pin 450: v%d exact payload %v, want v3", v, ok)
					}
					return err
				}, &reads)
			}
			for a := range int64(appenders) {
				loop(func(i int64) error { return k.put(bs, (a+1)<<32+i/versions, uint32(i%versions), payload) }, &appends)
			}
			loop(func(i int64) error { return bs.MetaSet("floor", []byte{byte(i)}) }, &metas)
			time.Sleep(cell)
			close(stop)
			wg.Wait()
			r, x := reads.Load(), seeks()
			stopSeeks()
			t.Logf("%s, %s, %.1f s: %d reads of quiet chains (%.0f /s), index answers %d (%.1f %%), not through the index %d; beside %d appends and %d MetaSets",
				shape.name, k.name, cell.Seconds(), r, float64(r)/cell.Seconds(), x, 100*float64(x)/float64(max(r, 1)), r-x, appends.Load(), metas.Load())
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
}
