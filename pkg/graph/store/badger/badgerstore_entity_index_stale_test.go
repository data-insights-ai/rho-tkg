package badger

import (
	"runtime"
	"strconv"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestEntityIndexOpenStaleStreamed: a build at open over the entries an
// earlier build left (a label kept on disk, then in RAM, then on disk again)
// holds no more live heap as it starts writing than the first build of the
// same store: the stale entries' deletes stream into the batch instead of the
// plan collecting every key first (STALEBUILD; 31 M stale entries held 2.2 GB). Not parallel:
// it reads the process's live heap.
func TestEntityIndexOpenStaleStreamed(t *testing.T) {
	if testing.Short() {
		t.Skip("200,000 nodes")
	}
	const n = 200_000
	dir := t.TempDir()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	keys := registrypkg.NewPropertyKeyRegistry()
	open := func(listed bool) *Store {
		t.Helper()
		cfg := Config{Dir: dir, PropertyKeyRegistry: keys, LabelNames: labels, MemTableSize: 8 << 20}
		if listed {
			cfg.PropertyIndexOnDiskLabels = []string{"D"}
		}
		bs, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	closeStore := func(bs *Store) {
		t.Helper()
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
	}
	bs := open(false)
	if err := bs.CreateCompositePropertyIndex(d, []string{"a", "b"}); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreatePropertyIndex(d, "a"); err != nil {
		t.Fatal(err)
	}
	batch := make([]*types.Node, 0, 10_000)
	for id := int64(1); id <= n; id++ {
		batch = append(batch, compositeNode(t, id, d, map[string]string{"a": "x" + strconv.FormatInt(id%97, 10), "b": "y"}))
		if len(batch) == cap(batch) {
			if err := bs.PutNodesBatch(batch); err != nil {
				t.Fatal(err)
			}
			batch = batch[:0]
		}
	}
	closeStore(bs)

	var live uint64
	entityIndexOpenCommitHook = func() {
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		live = m.HeapAlloc
	}
	defer func() { entityIndexOpenCommitHook = nil }()
	closeStore(open(true)) // first build: no stale entries
	first := live
	closeStore(open(false)) // in RAM: the markers go, the entries stay
	live = 0
	closeStore(open(true)) // build over 2 x 200,000 stale entries
	if live == 0 {
		t.Fatal("the open built nothing")
	}
	grew := int64(live) - int64(first)
	t.Logf("live heap as the build writes: first %d MB, over stale entries %d MB", first>>20, live>>20)
	if grew > 8<<20 {
		t.Fatalf("live heap as the build writes %d MB over the first build's: the stale keys are held", grew>>20)
	}
}
