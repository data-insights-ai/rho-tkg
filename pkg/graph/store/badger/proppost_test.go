package badger

import (
	"fmt"
	"slices"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RHOPOST (sigma's READBACK, 3 Nov: a block probe spends 75 % in badger's
// Iterator.Next inside the on-disk equality lookup). The lookup sought each
// value on its own, a seek through every table per value, although the
// values of a block lie next to each other in the index: after one value's
// walk the iterator already stands on the next value's first entry.

// propPostCandidates returns the lookup's candidates for the values, sorted
// and not compacted, with the seeks and visits it made.
func propPostCandidates(t *testing.T, bs *Store, values []any) (ids []int64, seeks, visits int) {
	t.Helper()
	keys := make([]string, len(values))
	for i, v := range values {
		keys[i] = types.IndexablePropertyValueKey(v)
	}
	propertyDiskSeekTestHook = func() { seeks++ }
	propertyDiskVisitTestHook = func() { visits++ }
	defer func() { propertyDiskSeekTestHook, propertyDiskVisitTestHook = nil, nil }()
	bs.idxMu.RLock()
	nids, err := bs.propertyIndexDiskKeysCandidatesLocked("k", keys, nil)
	bs.idxMu.RUnlock()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range nids {
		ids = append(ids, int64(id))
	}
	slices.Sort(ids)
	return ids, seeks, visits
}

// propPostStore holds 1,000 nodes of label 1 whose indexed "k" is b<i>; row
// i has node ID existMapBase+i+1, except row 500, which has the lowest ID.
func propPostStore(t testing.TB, n int) (*Store, func(i int) int64) {
	t.Helper()
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		FlushInterval: time.Hour, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	id := func(i int) int64 {
		if i == n/2 {
			return existMapBase
		}
		return existMapBase + int64(i) + 1
	}
	batch := make([]*types.Node, 0, n)
	for i := range n {
		nd := types.NewNode(types.NodeID(id(i)), 1, nil)
		_ = nd.SetProperty("k", fmt.Sprintf("b%06d", i))
		batch = append(batch, nd)
	}
	if err := bs.PutNodesBatch(batch); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	return bs, id
}

// TestPropPostBlockProbeSeeksOnce: a lookup of 64 adjacent values seeks once
// and visits each value's entry and the key after it at most. The block
// ends at the row with the lowest node ID, whose entry is exactly where the
// walk of its value starts.
func TestPropPostBlockProbeSeeksOnce(t *testing.T) {
	const n, block = 1_000, 64
	bs, id := propPostStore(t, n)
	var values []any
	var want []int64
	for i := n/2 - block + 1; i <= n/2; i++ {
		values = append(values, fmt.Sprintf("b%06d", i))
		want = append(want, id(i))
	}
	slices.Sort(want)
	got, seeks, visits := propPostCandidates(t, bs, values)
	t.Logf("%d adjacent values: %d seeks, %d visits", block, seeks, visits)
	if !slices.Equal(got, want) {
		t.Fatalf("candidates %v, want %v", got, want)
	}
	if seeks > 1 {
		t.Errorf("%d adjacent values took %d seeks, want 1", block, seeks)
	}
	if visits > 2*block {
		t.Errorf("%d adjacent values visited %d keys, want at most %d", block, visits, 2*block)
	}
}

// TestPropPostSpreadValuesDoNotWalkBetween: values 100 rows apart are each
// sought; the lookup does not walk the rows between them.
func TestPropPostSpreadValuesDoNotWalkBetween(t *testing.T) {
	const n = 1_000
	bs, id := propPostStore(t, n)
	var values []any
	var want []int64
	for i := 0; i < n; i += 100 {
		values = append(values, fmt.Sprintf("b%06d", i))
		want = append(want, id(i))
	}
	slices.Sort(want)
	got, seeks, visits := propPostCandidates(t, bs, values)
	t.Logf("%d values 100 rows apart: %d seeks, %d visits", len(values), seeks, visits)
	if !slices.Equal(got, want) {
		t.Fatalf("candidates %v, want %v", got, want)
	}
	if visits > 3*len(values) {
		t.Errorf("%d values visited %d keys: the lookup walks the rows between them", len(values), visits)
	}
}

// TestPropPostOverlappingStringsAnswerLikeScan: a string's value prefix has
// no terminator, so one looked-up string can extend another, and its walk
// (from value||lowest ID) can start before the shorter one's. Every subset
// of such values, with an unrelated string and a number beside them, finds
// exactly the nodes a scan of the rows finds, each once, buffered and
// flushed.
func TestPropPostOverlappingStringsAnswerLikeScan(t *testing.T) {
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		FlushInterval: time.Hour, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	// The lowest ID is 1. "x\x00"||2 sorts below "x"||1, where the walk of
	// "x" starts; "x\x00"||0x105 lies inside the walk of "x".
	rows := map[int64]any{
		1:       "x",
		2:       "x\x00",
		0x105:   "x\x00",
		3:       "x\x00\x00",
		0x10006: "x\x00\x00",
		4:       "x\x01",
		5:       "y",
		6:       int64(7),
		1 << 40: "x",
	}
	ids := slices.Sorted(func(yield func(int64) bool) {
		for id := range rows {
			if !yield(id) {
				return
			}
		}
	})
	for _, id := range ids {
		nd := types.NewNode(types.NodeID(id), 1, nil)
		_ = nd.SetProperty("k", rows[id])
		if err := bs.PutNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	values := []any{"x", "x\x00", "x\x00\x00", "x\x01", "y", int64(7), "x\x00\x00\x00"}
	check := func(when string) {
		t.Helper()
		for mask := 1; mask < 1<<len(values); mask++ {
			var asked []any
			for i, v := range values {
				if mask&(1<<i) != 0 {
					asked = append(asked, v)
				}
			}
			var want []int64
			for _, id := range ids {
				if slices.Contains(asked, rows[id]) {
					want = append(want, id)
				}
			}
			got, _, _ := propPostCandidates(t, bs, asked)
			if !slices.Equal(got, want) {
				t.Fatalf("%s: %q finds %v, want %v", when, asked, got, want)
			}
			keys := make([]string, len(asked))
			for i, v := range asked {
				keys[i] = types.IndexablePropertyValueKey(v)
			}
			nids, ok, err := bs.NodeIDsByLabelAndPropertyKeys(1, "k", keys, QueryOpts{})
			if err != nil || !ok {
				t.Fatalf("%s: ID door: %v %v", when, ok, err)
			}
			if len(nids) != len(want) {
				t.Fatalf("%s: %q: the ID door finds %v, want %v", when, asked, nids, want)
			}
		}
	}
	check("buffered")
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	check("flushed")
}

// TestPropPostWalkToKeyspaceEnd: a walk that runs off the end of the
// keyspace leaves the iterator invalid; a later walk inside the range it
// covered counts nothing again. The store's keys after the property index
// (0x0B..0x0F: temporal, entity, external adjacency, flush stage, meta) are
// dropped through the test seam, so the index keys are the last ones.
func TestPropPostWalkToKeyspaceEnd(t *testing.T) {
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		FlushInterval: time.Hour, MaxPendingWrites: -1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err := bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	// The lowest ID is 1 and the highest 1<<40: "x\x00\x00\x00\x80"||5
	// lies inside the walk of "x", and "x"||1<<40 is the last key.
	rows := map[int64]string{1: "x", 5: "x\x00\x00\x00\x80", 1 << 40: "x"}
	for _, id := range []int64{1, 5, 1 << 40} {
		nd := types.NewNode(types.NodeID(id), 1, nil)
		_ = nd.SetProperty("k", rows[id])
		if err := bs.PutNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	db := bs.DBForTest()
	if err := db.DropPrefix([]byte{0x0B}, []byte{0x0C}, []byte{0x0D}, []byte{0x0E}, []byte{0x0F}); err != nil {
		t.Fatal(err)
	}
	if err := db.View(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.AllVersions = true
		it := txn.NewIterator(opts)
		defer it.Close()
		if it.Seek([]byte{0x0B}); it.Valid() {
			t.Fatalf("a key after the property index is left: %x", it.Item().Key())
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	got, _, _ := propPostCandidates(t, bs, []any{"x", "x\x00\x00\x00\x80"})
	if want := []int64{1, 5, 1 << 40}; !slices.Equal(got, want) {
		t.Fatalf("candidates %v, want %v", got, want)
	}
}
