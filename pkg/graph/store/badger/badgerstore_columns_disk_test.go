package badger

import (
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Persisted columns. The contract is that they change NOTHING a caller can observe
// except latency: same rows, same values, and every failure mode is a rebuild.

const cdLabel uint16 = 41

func cdStore(t *testing.T, onDisk bool) *Store {
	t.Helper()
	bs, err := New(Config{InMemory: true, ColumnsOnDisk: onDisk})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

func cdPut(t *testing.T, bs *Store, id, qty int64) {
	t.Helper()
	n := types.NewNode(types.NodeID(id), cdLabel, nil)
	if err := n.SetProperty("qty", qty); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: types.Instant(id * 10)})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
}

func cdRead(t *testing.T, bs *Store) map[int64]int64 {
	t.Helper()
	got := map[int64]int64{}
	_, ok, err := bs.ForEachDocValues(cdLabel, []string{"qty"},
		func(id types.NodeID, vals []any, present []bool) bool {
			if present[0] {
				got[int64(id)] = vals[0].(int64)
			}
			return true
		})
	if err != nil {
		t.Fatalf("ForEachDocValues: %v", err)
	}
	if !ok {
		t.Fatal("declined")
	}
	return got
}

// TestColumnsOnDisk_OffIsAByteForByteNoOp is the most important probe here: with the
// flag off, the on-disk keyspace must be untouched. A cache that writes when it was
// told not to is a behaviour change, not an optimisation.
func TestColumnsOnDisk_OffIsAByteForByteNoOp(t *testing.T) {
	bs := cdStore(t, false)
	for i := int64(1); i <= 5; i++ {
		cdPut(t, bs, i, i*100)
	}
	cdRead(t, bs) // forces a build

	if blob, err := bs.MetaGet(columnDiskKey(cdLabel)); err == nil && len(blob) > 0 {
		t.Fatalf("ColumnsOnDisk=false still wrote %d bytes to the column keyspace", len(blob))
	}
}

// TestColumnsOnDisk_PersistsAndServes pins that a build writes a blob and that a
// later read at the same epoch is served from it rather than rebuilt.
func TestColumnsOnDisk_PersistsAndServes(t *testing.T) {
	bs := cdStore(t, true)
	for i := int64(1); i <= 5; i++ {
		cdPut(t, bs, i, i*100)
	}
	want := map[int64]int64{1: 100, 2: 200, 3: 300, 4: 400, 5: 500}
	got := cdRead(t, bs)
	if len(got) != len(want) {
		t.Fatalf("row count %d, want %d", len(got), len(want))
	}

	blob, err := bs.MetaGet(columnDiskKey(cdLabel))
	if err != nil || len(blob) == 0 {
		t.Fatalf("no column blob persisted (err=%v)", err)
	}

	// Drop the in-RAM snapshot; the next read must come from disk, not a rebuild.
	bs.docMu.Lock()
	delete(bs.docColumns, cdLabel)
	bs.docMu.Unlock()
	rebuilds := bs.ColumnRebuildCount()

	got2 := cdRead(t, bs)
	for id, v := range want {
		if got2[id] != v {
			t.Errorf("id %d: %d, want %d", id, got2[id], v)
		}
	}
	if bs.ColumnRebuildCount() != rebuilds {
		t.Error("the persisted column was not used — the read rebuilt instead of decoding")
	}
}

// TestColumnsOnDisk_StaleBlobIsIgnored pins the epoch gate. A blob written before a
// subsequent write must never be served: it is the difference between a cache and a
// second source of truth.
func TestColumnsOnDisk_StaleBlobIsIgnored(t *testing.T) {
	bs := cdStore(t, true)
	for i := int64(1); i <= 3; i++ {
		cdPut(t, bs, i, i*100)
	}
	cdRead(t, bs) // persists a blob covering 1..3

	cdPut(t, bs, 4, 400) // epoch advances; the blob is now stale

	bs.docMu.Lock()
	delete(bs.docColumns, cdLabel) // force the disk path to be consulted
	bs.docMu.Unlock()

	got := cdRead(t, bs)
	if _, missing := got[4]; !missing {
		t.Fatal("node 4 missing — a STALE persisted column was served over a newer write")
	}
	if len(got) != 4 {
		t.Errorf("row count %d, want 4", len(got))
	}
}

// TestColumnsOnDisk_CorruptBlobRebuilds pins that corruption is a rebuild, never an
// error and never a wrong answer.
func TestColumnsOnDisk_CorruptBlobRebuilds(t *testing.T) {
	bs := cdStore(t, true)
	for i := int64(1); i <= 3; i++ {
		cdPut(t, bs, i, i*100)
	}
	cdRead(t, bs)

	blob, err := bs.MetaGet(columnDiskKey(cdLabel))
	if err != nil || len(blob) == 0 {
		t.Fatalf("no blob to corrupt (err=%v)", err)
	}
	for i := range blob { // shred it
		blob[i] ^= 0xFF
	}
	if err := bs.MetaSet(columnDiskKey(cdLabel), blob); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	bs.docMu.Lock()
	delete(bs.docColumns, cdLabel)
	bs.docMu.Unlock()

	got := cdRead(t, bs)
	want := map[int64]int64{1: 100, 2: 200, 3: 300}
	if len(got) != len(want) {
		t.Fatalf("row count %d, want %d — a corrupt blob must fall back to a rebuild", len(got), len(want))
	}
	for id, v := range want {
		if got[id] != v {
			t.Errorf("id %d: %d, want %d", id, got[id], v)
		}
	}
}

// TestColumnsOnDisk_SameAnswersEitherWay is the differential oracle: a store with
// persistence on and one with it off must expose identical rows.
func TestColumnsOnDisk_SameAnswersEitherWay(t *testing.T) {
	run := func(onDisk bool) map[int64]int64 {
		bs := cdStore(t, onDisk)
		for i := int64(1); i <= 8; i++ {
			cdPut(t, bs, i, i*11)
			if i%3 == 0 {
				cdRead(t, bs)
				bs.docMu.Lock()
				delete(bs.docColumns, cdLabel) // force the disk path where it exists
				bs.docMu.Unlock()
			}
		}
		return cdRead(t, bs)
	}
	on, off := run(true), run(false)
	if len(on) != len(off) {
		t.Fatalf("row counts differ: onDisk=%d off=%d", len(on), len(off))
	}
	for id, v := range off {
		if on[id] != v {
			t.Errorf("id %d: onDisk=%d off=%d", id, on[id], v)
		}
	}
}

// TestColumnsOnDisk_ChunkedBlobRoundTrips pins the fix for a real defect: badger
// refuses a value over 1 MiB, and a 50,000-row column encodes to ~1.6 MB, so an
// unchunked write FAILED for exactly the labels persistence is most worth having —
// and because the failure was swallowed, every read then paid an encode AND a
// rebuild. Strictly worse than not persisting.
//
// columnChunkBytes is shrunk here so a small fixture exercises the multi-chunk path.
func TestColumnsOnDisk_ChunkedBlobRoundTrips(t *testing.T) {
	orig := columnChunkBytes
	columnChunkBytes = 64 // force many chunks
	t.Cleanup(func() { columnChunkBytes = orig })

	bs := cdStore(t, true)
	want := map[int64]int64{}
	for i := int64(1); i <= 200; i++ {
		cdPut(t, bs, i, i*100)
		want[i] = i * 100
	}
	cdRead(t, bs) // builds and persists across many chunks

	head, err := bs.MetaGet(columnDiskKey(cdLabel))
	if err != nil || len(head) == 0 {
		t.Fatalf("no chunk header persisted (err=%v)", err)
	}
	if string(head) == "1" {
		t.Fatal("only one chunk — the fixture did not exercise the chunked path")
	}

	bs.docMu.Lock()
	delete(bs.docColumns, cdLabel)
	bs.docMu.Unlock()
	rebuilds := bs.ColumnRebuildCount()

	got := cdRead(t, bs)
	if len(got) != len(want) {
		t.Fatalf("row count %d, want %d", len(got), len(want))
	}
	for id, v := range want {
		if got[id] != v {
			t.Errorf("id %d: %d, want %d", id, got[id], v)
		}
	}
	if bs.ColumnRebuildCount() != rebuilds {
		t.Error("a chunked column was not reassembled — the read rebuilt instead")
	}
}

// TestColumnsOnDisk_MissingChunkRebuilds pins that a header claiming more chunks than
// exist is a rebuild, not a torn column. Writing chunks BEFORE the header makes this
// the only reachable inconsistency, and it must fail safe.
func TestColumnsOnDisk_MissingChunkRebuilds(t *testing.T) {
	orig := columnChunkBytes
	columnChunkBytes = 64
	t.Cleanup(func() { columnChunkBytes = orig })

	bs := cdStore(t, true)
	want := map[int64]int64{}
	for i := int64(1); i <= 100; i++ {
		cdPut(t, bs, i, i*100)
		want[i] = i * 100
	}
	cdRead(t, bs)

	// Claim one more chunk than was written.
	head, _ := bs.MetaGet(columnDiskKey(cdLabel))
	n, _ := strconv.Atoi(string(head))
	if err := bs.MetaSet(columnDiskKey(cdLabel), []byte(strconv.Itoa(n+1))); err != nil {
		t.Fatalf("MetaSet: %v", err)
	}
	bs.docMu.Lock()
	delete(bs.docColumns, cdLabel)
	bs.docMu.Unlock()

	got := cdRead(t, bs)
	if len(got) != len(want) {
		t.Fatalf("row count %d, want %d — a header naming a missing chunk must rebuild", len(got), len(want))
	}
}
