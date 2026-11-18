//go:build measure

// The as-of index's write cost on disk, index on against off in one binary
// (asOfIndexOff, badgerstore_asof_index_switch_measure.go). Wall time and
// runtime.MemStats are evidence, never a pass/fail, so this runs by hand:
//
//	go test -tags measure -count=1 -timeout 120m -v -run '^TestAsOfAppendCostOnDisk$' ./pkg/graph/store/badger
//	go test -tags measure -count=1 -v -run '^TestAsOfProofsAfterUncleanClose$' ./pkg/graph/store/badger
//
// ASOFCOST_HOT (M1 appends a phase, default 200,000) and ASOFCOST_ENTITIES
// (M2 entities, default 1,000,000) scale the run down.

package badger

import (
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
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
	wall, flush        time.Duration
	txns, gets, seeks  int64
	mallocs, allocByte uint64
}

func (p asOfCostPhase) String() string {
	n := float64(len(p.lat))
	s := fmt.Sprintf("append %v; wall/record %.2f us (durable flushes %.0f ms); hook per append: txns %.3f gets %.3f seeks %.3f; allocs/append %.1f (%.0f B, process-wide)",
		p.lat, float64(p.wall)/1e3/n, float64(p.flush)/1e6, float64(p.txns)/n, float64(p.gets)/n, float64(p.seeks)/n,
		float64(p.mallocs)/n, float64(p.allocByte)/n)
	if len(p.creator) > 0 {
		s += fmt.Sprintf("\n      creator PutNode beside it: %v", p.creator)
	}
	return s
}

// asOfCostRun times n appends (fn), with a DurableFlush every
// asOfCostDurableEvery records when durable, and with a paced node creator
// (no history row) recording each PutNode's wall time when creator is set.
func asOfCostRun(t *testing.T, bs *Store, n int, durable, creator bool, creatorBase int64, fn func(i int) error) asOfCostPhase {
	t.Helper()
	var p asOfCostPhase
	p.lat = make(asOfCostLat, 0, n)
	stop := make(chan struct{})
	var wg sync.WaitGroup
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
	asOfHookTxns.Store(0)
	asOfHookGets.Store(0)
	asOfHookSeeks.Store(0)
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
	p.txns, p.gets, p.seeks = asOfHookTxns.Load(), asOfHookGets.Load(), asOfHookSeeks.Load()
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
				a := asOfCostRun(t, bs, hot, durable, false, 0, m1(1))
				b := asOfCostRun(t, bs, hot, durable, true, 1<<40, m1(1+int64(hot)))
				t.Logf("%s M1 hot\n   alone: %v\n   with creator: %v\n      %s", name, a, b, asOfCostBytes(t, bs, cfg))

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
				half := cold / 2
				m2 := func(off int) func(i int) error {
					return func(i int) error { return k.put(bs, base+int64(order[off+i]), 1, payload) }
				}
				a = asOfCostRun(t, bs, half, durable, false, 0, m2(0))
				b = asOfCostRun(t, bs, cold-half, durable, true, 1<<40, m2(half))
				t.Logf("%s M2 cold (setup %d rows %.1f s)\n   alone: %v\n   with creator: %v\n      %s", name, cold, setupWall.Seconds(), a, b, asOfCostBytes(t, bs, cfg))
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
