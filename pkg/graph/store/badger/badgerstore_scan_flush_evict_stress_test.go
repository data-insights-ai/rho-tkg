package badger

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// The deterministic tests in badgerstore_scan_flush_evict_test.go place one
// flush at one point of one scan. This one runs the real interleaving: a writer
// replacing rows, a goroutine flushing in a loop, a cache far smaller than the
// data, and every multi-row read racing them. Each read must return every row,
// and no row older than the version that was committed before the read began.

const stressScanRows = parallelDecodeMinIDs // NodesByLabel takes the parallel path
const stressScanRounds = 12

type stressVersions struct{ latest []atomic.Int64 }

func (v *stressVersions) floor() []int64 {
	out := make([]int64, len(v.latest))
	for i := range v.latest {
		out[i] = v.latest[i].Load()
	}
	return out
}

// runScanStress starts the writer and flusher, calls scan stressScanRounds
// times and checks each result against the versions committed before it began.
// replace(i, v) must store row i at version v; scan returns version by row index.
func runScanStress(t *testing.T, bs *Store, rows int, replace func(i int, v int64) error, scan func() (map[int]int64, error)) {
	t.Helper()
	vers := &stressVersions{latest: make([]atomic.Int64, rows)}
	for i := range vers.latest {
		vers.latest[i].Store(1)
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var bgErr atomic.Value
	wg.Add(2)
	go func() { // writer
		defer wg.Done()
		for k := 0; ; k++ {
			select {
			case <-stop:
				return
			default:
			}
			i := (k * 7919) % rows
			v := vers.latest[i].Load() + 1
			if err := replace(i, v); err != nil {
				bgErr.Store(err)
				return
			}
			vers.latest[i].Store(v) // only after the store accepted it
		}
	}()
	go func() { // flusher
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := bs.Flush(); err != nil {
				bgErr.Store(err)
				return
			}
			runtime.Gosched()
		}
	}()
	defer func() {
		close(stop)
		wg.Wait()
		if err, _ := bgErr.Load().(error); err != nil {
			t.Fatalf("writer/flusher: %v", err)
		}
	}()

	for round := 0; round < stressScanRounds; round++ {
		floor := vers.floor()
		got, err := scan()
		if err != nil {
			t.Fatalf("round %d: scan: %v", round, err)
		}
		stale := 0
		for i, v := range got {
			if v < floor[i] {
				stale++
			}
		}
		if len(got) != rows || stale != 0 {
			t.Fatalf("round %d: scan returned %d of %d rows, %d older than the version committed before the scan began", round, len(got), rows, stale)
		}
	}
}

func newStressNodeStore(t *testing.T) *Store {
	t.Helper()
	bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 128 })
	for i := 0; i < stressScanRows; i++ {
		if err := bs.PutNode(stressNode(i, 1)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return bs
}

func stressNode(i int, v int64) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(100000+i)), scanFlushEvictLabel, nil)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
	if err := n.SetProperty(scanVersionKey, v); err != nil {
		panic(err)
	}
	return n
}

func nodeVersionsByIndex(nodes []*types.Node) map[int]int64 {
	out := make(map[int]int64, len(nodes))
	for _, n := range nodes {
		v, _ := n.GetProperty(scanVersionKey)
		out[int(n.ID().SnowflakeID().Int64()-100000)] = v.(int64)
	}
	return out
}

func TestNodeScans_ConcurrentReplaceAndFlush_NoDropNoStale(t *testing.T) {
	scans := map[string]func(bs *Store) ([]*types.Node, error){
		"ForEachNodeByLabel": func(bs *Store) ([]*types.Node, error) {
			var out []*types.Node
			err := bs.ForEachNodeByLabel(scanFlushEvictLabel, QueryOpts{}, func(n *types.Node) bool {
				out = append(out, n)
				return true
			})
			return out, err
		},
		"NodesByLabel/parallel": func(bs *Store) ([]*types.Node, error) {
			return bs.NodesByLabel(scanFlushEvictLabel, QueryOpts{})
		},
		"NodesAsOf": func(bs *Store) ([]*types.Node, error) {
			return bs.NodesAsOf(types.Instant(1 << 40))
		},
	}
	for name, scan := range scans {
		t.Run(name, func(t *testing.T) {
			bs := newStressNodeStore(t)
			runScanStress(t, bs, stressScanRows,
				func(i int, v int64) error { return bs.ReplaceNode(stressNode(i, v)) },
				func() (map[int]int64, error) {
					nodes, err := scan(bs)
					return nodeVersionsByIndex(nodes), err
				})
		})
	}
}

func stressRel(i int, v int64) *types.Relationship {
	r := types.NewRelationship(types.RelID(snowflake.ID(200000+i)), scanFlushEvictType, 1, 2)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
	if err := r.SetProperty(scanVersionKey, v); err != nil {
		panic(err)
	}
	return r
}

func relVersionsByIndex(rels []*types.Relationship) map[int]int64 {
	out := make(map[int]int64, len(rels))
	for _, r := range rels {
		v, _ := r.GetProperty(scanVersionKey)
		out[int(r.ID().SnowflakeID().Int64()-200000)] = v.(int64)
	}
	return out
}

func TestRelScans_ConcurrentReplaceAndFlush_NoDropNoStale(t *testing.T) {
	const rows = 1024
	ids := make([]types.RelID, rows)
	for i := range ids {
		ids[i] = types.RelID(snowflake.ID(200000 + i))
	}
	scans := map[string]func(bs *Store) ([]*types.Relationship, error){
		"forEachRelBulk": func(bs *Store) ([]*types.Relationship, error) {
			var out []*types.Relationship
			err := bs.forEachRelBulk(ids, func(r *types.Relationship) bool {
				out = append(out, r)
				return true
			})
			return out, err
		},
		"RelsAsOf": func(bs *Store) ([]*types.Relationship, error) {
			return bs.RelsAsOf(types.Instant(1 << 40))
		},
	}
	for name, scan := range scans {
		t.Run(name, func(t *testing.T) {
			bs := newFlushParkStore(t, func(c *Config) { c.CacheCapacity = 128 })
			for _, id := range []int64{1, 2} {
				n := types.NewNode(types.NodeID(snowflake.ID(id)), scanFlushEvictLabel, nil)
				n.SetTemporal(&types.TemporalMetadata{TxFrom: 1})
				if err := bs.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < rows; i++ {
				if err := bs.PutRelationship(stressRel(i, 1)); err != nil {
					t.Fatal(err)
				}
			}
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
			runScanStress(t, bs, rows,
				func(i int, v int64) error { return bs.ReplaceRelationship(stressRel(i, v)) },
				func() (map[int]int64, error) {
					rels, err := scan(bs)
					return relVersionsByIndex(rels), err
				})
		})
	}
}
