package core

import (
	"fmt"
	"testing"
	"time"

	badgerpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	shardedpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 55 (ai-soc restart replay, sigma restore): a full effective scan
// reads through one badger read snapshot per store, not one transaction (or
// three) per entity. TestEffectiveScanTransactionsPerType counts the read
// transactions every scan door opens (badger.ReadTxnsTotalForTest) and checks
// each answer against the per-ID path (the entity's own Node/RelEffectiveTimeline).
//
// The rule under concurrent writes: each entity is still read at one moment
// under its entity lock, and the shared snapshot is re-pinned whenever a flush
// committed rows since it opened, so every read equals the store's own read at
// that moment; a pinned answer therefore equals the per-ID path's, and a scan
// opens one transaction per store plus one per flush that lands during it.

// estBackend opens a graph whose badger stores keep cacheCap rows each;
// shards is the number of badger stores a scan may read (0: none), perID marks
// a backend that keeps the per-ID path (tiered: every read checks a shard out
// and in, so no snapshot outlives a read).
type estBackend struct {
	name   string
	shards int64
	perID  bool
	open   func(t *testing.T, cacheCap int) *Core
}

func estBackends() []estBackend {
	newCore := func(t *testing.T, cfg Config) *Core {
		t.Helper()
		g, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = g.Close() })
		useTestClock(t, g)
		return g
	}
	const never = time.Duration(1<<63 - 1)
	return []estBackend{
		{"memory", 0, false, func(t *testing.T, _ int) *Core { return newCore(t, Config{}) }},
		{"badger", 1, false, func(t *testing.T, cacheCap int) *Core {
			st, err := badgerpkg.New(badgerpkg.Config{InMemory: true, CacheCapacity: cacheCap, FlushInterval: never})
			if err != nil {
				t.Fatalf("badger.New: %v", err)
			}
			return newCore(t, Config{Store: st})
		}},
		{"sharded", 2, false, func(t *testing.T, cacheCap int) *Core {
			st, err := shardedpkg.New(shardedpkg.Config{InMemory: true, BaseSlot: 0, SlotCount: 2, CacheCapacity: cacheCap})
			if err != nil {
				t.Fatalf("sharded.New: %v", err)
			}
			return newCore(t, Config{Store: st})
		}},
		{"tiered", 0, true, func(t *testing.T, cacheCap int) *Core {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour,
				FlushInterval: never, CacheCapacity: cacheCap})
			if err != nil {
				t.Fatalf("tiered.New: %v", err)
			}
			t.Cleanup(func() { _ = ts.Close() })
			return newCore(t, Config{Store: ts})
		}},
	}
}

// estFixture is n tracked entities in five shapes (i%5): live without
// history, updated, corrected by a cascade between pinBefore and pinAfter,
// deleted (history rows only), deleted and re-imported.
type estFixture struct {
	e                   *ccEnt
	ids                 []int64
	pinBefore, pinAfter types.Instant
}

func estBuild(t *testing.T, g *Core, rel bool, n int) *estFixture {
	t.Helper()
	e := newCCEnt(t, g, rel)
	f := &estFixture{e: e}
	for i := 0; i < n; i++ {
		f.ids = append(f.ids, e.add(fmt.Sprintf("E%03d", i), 1000, nil))
	}
	for i, id := range f.ids {
		switch i % 5 {
		case 1, 2:
			e.mustUpdate(id, map[string]any{"x": int64(i)})
		case 3:
			e.mustDel(id)
		case 4:
			e.mustDel(id)
			e.mustReimport(id, map[string]any{"tkg_valid_from": types.Instant(4000), "x": int64(-i)})
		}
	}
	f.pinBefore = e.pin()
	for i, id := range f.ids {
		if i%5 == 2 {
			e.mustCascade(id, 2000, 3000, map[string]any{"x": int64(100 + i)})
		}
	}
	f.pinAfter = e.pin()
	estFlush(t, g)
	return f
}

func estFlush(t *testing.T, g *Core) {
	t.Helper()
	if f, ok := g.store.(interface{ Flush() error }); ok {
		if err := f.Flush(); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	}
}

// estOracle is the per-ID path: each tracked entity's own timeline at pin.
func (f *estFixture) estOracle(pin types.Instant) map[string]string {
	want := map[string]string{}
	for _, id := range f.ids {
		line, err := f.e.timeline(id, pin)
		if err != nil {
			if f.e.isAbsent(err) {
				continue
			}
			f.e.t.Fatalf("timeline(%d, %d): %v", id, pin, err)
		}
		if line != "" {
			want[f.e.names[id]] = line
		}
	}
	return want
}

type estDoor struct {
	name string
	// gather, when set, runs the door's candidate gather alone: its reads are
	// the gather's, not the timelines' (ForEachRelEffectiveAtNodes reads the
	// seeds' adjacent rows to list them), and estScan counts the rest.
	gather func(pin types.Instant) error
	run    func(pin types.Instant, nfn func(temporal.NodeSegment) bool, rfn func(temporal.RelSegment) bool) error
}

// estDoors are every door that streams per-entity timelines; each lists every
// tracked entity of the fixture.
func (f *estFixture) estDoors() []estDoor {
	te, e := f.e.g.Temporal, f.e
	nodeIDs, relIDs := make([]types.NodeID, len(f.ids)), make([]types.RelID, len(f.ids))
	for i, id := range f.ids {
		nodeIDs[i], relIDs[i] = types.NodeID(id), types.RelID(id)
	}
	return []estDoor{
		{"by name", nil, func(pin types.Instant, nfn func(temporal.NodeSegment) bool, rfn func(temporal.RelSegment) bool) error {
			if e.rel {
				return te.ForEachRelEffectiveByType(ccType, pin, rfn)
			}
			return te.ForEachNodeEffectiveByLabel(ccLabel, pin, nfn)
		}},
		{"all", nil, func(pin types.Instant, nfn func(temporal.NodeSegment) bool, rfn func(temporal.RelSegment) bool) error {
			if e.rel {
				return te.ForEachRelEffective(pin, rfn)
			}
			return te.ForEachNodeEffective(pin, nfn)
		}},
		{"by ids", nil, func(pin types.Instant, nfn func(temporal.NodeSegment) bool, rfn func(temporal.RelSegment) bool) error {
			if e.rel {
				return te.RelsEffectiveByIDs(relIDs, pin, rfn)
			}
			return te.NodesEffectiveByIDs(nodeIDs, pin, nfn)
		}},
		{"from", nil, func(pin types.Instant, nfn func(temporal.NodeSegment) bool, rfn func(temporal.RelSegment) bool) error {
			if e.rel {
				return te.ForEachRelEffectiveByTypeFrom(ccType, pin, 1, rfn)
			}
			return te.ForEachNodeEffectiveByLabelsFrom([]string{ccLabel}, pin, 1, nfn)
		}},
		{"at nodes / by labels", func(types.Instant) error {
			if !e.rel {
				return nil
			}
			return f.e.g.readUnderRLock(func() error {
				_, err := f.e.g.relEffectiveAdjacencyCandidates([]types.NodeID{e.start}, nil, true)
				return err
			})
		}, func(pin types.Instant, nfn func(temporal.NodeSegment) bool, rfn func(temporal.RelSegment) bool) error {
			if e.rel {
				return te.ForEachRelEffectiveAtNodes([]types.NodeID{e.start}, temporal.Outgoing, nil, pin, rfn)
			}
			return te.ForEachNodeEffectiveByLabels([]string{ccLabel, "NoSuchLabel"}, pin, nfn)
		}},
	}
}

// estScan runs a door and returns its rendered answer and the read
// transactions it opened.
func (f *estFixture) estScan(d estDoor, pin types.Instant) (map[string]string, int64) {
	var gathered int64
	if d.gather != nil {
		before := badgerpkg.ReadTxnsTotalForTest()
		if err := d.gather(pin); err != nil {
			f.e.t.Fatalf("%s gather: %v", d.name, err)
		}
		gathered = badgerpkg.ReadTxnsTotalForTest() - before
	}
	before := badgerpkg.ReadTxnsTotalForTest()
	got := f.e.scanSegsWith(d.name, func(nfn func(temporal.NodeSegment) bool, rfn func(temporal.RelSegment) bool) error {
		return d.run(pin, nfn, rfn)
	})
	return got, badgerpkg.ReadTxnsTotalForTest() - before - gathered
}

// TestEffectiveScanTransactionsPerType: on memory, badger, sharded and tiered,
// node and relationship, every effective scan door at the pin before and after
// a correction answers the per-ID path's answer (live, updated, corrected,
// deleted and re-imported entities alike), and on badger and sharded opens
// the same number of read transactions for 10 and 60 entities, at most three
// per store (the gather's reads and the scan's snapshot). The gather of
// ForEachRelEffectiveAtNodes reads the seeds' adjacent rows; it is counted
// apart (a separate run of it, whose cache misses may differ by one), so
// that door is held to the bound only. Break cases:
// TestEffectiveScanTransactionsPerType_ConcurrentWrites.
func TestEffectiveScanTransactionsPerType(t *testing.T) {
	for _, be := range estBackends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				txns := map[string][]int64{}
				apart := map[string]bool{}
				for _, n := range []int{10, 60} {
					f := estBuild(t, be.open(t, 1), rel, n)
					for p, pin := range []types.Instant{f.pinBefore, f.pinAfter} {
						want := f.estOracle(pin)
						for _, d := range f.estDoors() {
							got, k := f.estScan(d, pin)
							if renderScan(got) != renderScan(want) {
								t.Fatalf("n=%d %s at pin %d differs from the per-ID path:\n got  %s\n want %s", n, d.name, pin, renderScan(got), renderScan(want))
							}
							key := fmt.Sprintf("%s, pin %s", d.name, []string{"before", "after"}[p])
							txns[key] = append(txns[key], k)
							apart[key] = d.gather != nil && rel
						}
					}
				}
				if be.shards == 0 {
					return // memory reads no badger; tiered keeps the per-ID path
				}
				for key, ks := range txns {
					t.Logf("%s: %d read transactions for 10 entities, %d for 60", key, ks[0], ks[1])
					if (!apart[key] && ks[0] != ks[1]) || ks[1] > 3*be.shards {
						t.Errorf("%s: %d read transactions for 10 entities, %d for 60; want equal and <= %d (one snapshot per store, not one per entity)", key, ks[0], ks[1], 3*be.shards)
					}
				}
			})
		}
	}
}

// TestEffectiveScanTransactionsPerType_ConcurrentWrites: while a scan runs, a
// writer (1) updates every live tracked entity and deletes some, then flushes:
// the rows the scan's snapshot predates land in badger while the store's
// caches keep every row, so a current row is read from the cache and its
// history from the snapshot; then (2) truncates the history of later entities
// and flushes: a history-only commit that turns no cache entry clean. The
// scan answers what the per-ID path answers after the writes (writes (1) are
// stamped above the pin and change nothing at it; (2) removes rows), and
// re-pins once per flush, never once per entity.
func TestEffectiveScanTransactionsPerType_ConcurrentWrites(t *testing.T) {
	for _, be := range estBackends() {
		for _, rel := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/rel=%v", be.name, rel), func(t *testing.T) {
				for di := range estDoorCount {
					g := be.open(t, 10_000)
					f := estBuild(t, g, rel, 40)
					d := f.estDoors()[di]
					t.Run(d.name, func(t *testing.T) {
						pin := f.pinAfter
						before := f.estOracle(pin)
						calls, stages, writerTxns := 0, 0, int64(0)
						g.scanReadTestHook = func() {
							calls++
							var write func(*estFixture) error
							switch calls {
							case 3:
								write = estWriteAll
							case 6:
								write = estTrimLater
							default:
								return
							}
							done := make(chan error, 1)
							start := badgerpkg.ReadTxnsTotalForTest()
							go func() { done <- write(f) }()
							select {
							case err := <-done:
								if err != nil {
									t.Errorf("writer: %v", err)
								}
								stages++
								writerTxns += badgerpkg.ReadTxnsTotalForTest() - start
							case <-time.After(20 * time.Second):
								t.Errorf("writer blocked by the scan")
							}
						}
						got, k := f.estScan(d, pin)
						k -= writerTxns // the writer's own reads
						g.scanReadTestHook = nil
						if stages != 2 {
							t.Fatalf("%s: %d of 2 writer stages ran during the scan (%d entity reads)", d.name, stages, calls)
						}
						want := f.estOracle(pin)
						if renderScan(want) == renderScan(before) {
							t.Fatalf("%s: the history trim changed nothing at the pin; the case proves nothing", d.name)
						}
						if renderScan(got) != renderScan(want) {
							t.Fatalf("%s under concurrent writes differs from the per-ID path:\n got  %s\n want %s", d.name, renderScan(got), renderScan(want))
						}
						if be.shards > 0 && k > 5*be.shards {
							t.Errorf("%s: %d read transactions with two flushes during the scan; want <= %d", d.name, k, 5*be.shards)
						}
					})
				}
			})
		}
	}
}

// estDoorCount is len(estDoors()).
const estDoorCount = 5

// estWriteAll updates every live tracked entity, deletes every 7th, and
// flushes the store.
func estWriteAll(f *estFixture) error {
	e := f.e
	for i, id := range f.ids {
		if i%5 == 3 {
			continue // deleted
		}
		var err error
		if i%7 == 0 {
			err = e.del(id)
		} else {
			err = e.update(id, map[string]any{"x": int64(1000 + i)})
		}
		if err != nil {
			return fmt.Errorf("write %d: %w", id, err)
		}
	}
	return estFlushErr(f.e.g)
}

// estTrimLater drops, through the store, the history of the updated
// entities from the 20th on (read after the 6th read of any door), and
// flushes: history keys only, no current row.
func estTrimLater(f *estFixture) error {
	type truncater interface {
		TruncateNodeHistory(types.NodeID, int) error
		TruncateRelHistory(types.RelID, int) error
	}
	tr, ok := f.e.g.store.(truncater)
	if !ok {
		return fmt.Errorf("store %T cannot truncate history", f.e.g.store)
	}
	for i, id := range f.ids {
		if i < 20 || i%5 != 1 {
			continue
		}
		var err error
		if f.e.rel {
			err = tr.TruncateRelHistory(types.RelID(id), 0)
		} else {
			err = tr.TruncateNodeHistory(types.NodeID(id), 0)
		}
		if err != nil {
			return fmt.Errorf("truncate %d: %w", id, err)
		}
	}
	return estFlushErr(f.e.g)
}

func estFlushErr(g *Core) error {
	if fl, ok := g.store.(interface{ Flush() error }); ok {
		return fl.Flush()
	}
	return nil
}
