package badger

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// Backlog 74: one store pass builds every declared property sidecar of a kind,
// and a clean close persists the built sidecars in the open image. The
// cross-backend half (badger, sharded) is
// pkg/graph/store/property_tx_members_one_pass_test.go; these drive the
// badger internals: the scan count, the per-key differential, the write
// buffer, and the image trust rule.

const (
	opT2 uint16 = 32
	opT3 uint16 = 33
	opT4 uint16 = 34 // no index: carries the same values
	opL2 uint16 = 42
)

var (
	opRelKeys = []indexpkg.RelPropertyIndexKey{
		{RelTypeToken: ptxT, PropertyKey: "seat"}, {RelTypeToken: ptxT, PropertyKey: "zone"},
		{RelTypeToken: opT2, PropertyKey: "seat"}, {RelTypeToken: opT3, PropertyKey: "zone"},
	}
	opNodeKeys = []indexpkg.PropertyIndexKey{
		{LabelToken: ptxLabel, PropertyKey: "seat"}, {LabelToken: ptxLabel, PropertyKey: "zone"},
		{LabelToken: opL2, PropertyKey: "zone"},
	}
)

func opRelRow(id int64, typ uint16, seat int64, version uint32, tx types.Instant) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), typ, types.NodeID(1), types.NodeID(2))
	_ = r.SetProperty("seat", seat)
	_ = r.SetProperty("zone", seat+10)
	r.SetVersion(version)
	r.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
	return r
}

func opNodeRow(id int64, labels []uint16, seat int64, version uint32, tx types.Instant) *types.Node {
	n := types.NewNode(types.NodeID(id), labels[0], labels[1:])
	_ = n.SetProperty("seat", seat)
	_ = n.SetProperty("zone", seat+10)
	n.SetVersion(version)
	n.SetTemporal(&types.TemporalMetadata{TxFrom: tx})
	return n
}

// opDeclare declares the given indexes (endpoints first).
func opDeclare(t *testing.T, bs *Store, rel []indexpkg.RelPropertyIndexKey, node []indexpkg.PropertyIndexKey) {
	t.Helper()
	putTestNode(t, bs, 1, 9, nil)
	putTestNode(t, bs, 2, 9, nil)
	for _, k := range rel {
		if err := bs.CreateRelPropertyIndex(k.RelTypeToken, k.PropertyKey); err != nil {
			t.Fatalf("CreateRelPropertyIndex(%v): %v", k, err)
		}
	}
	for _, k := range node {
		if err := bs.CreatePropertyIndex(k.LabelToken, k.PropertyKey); err != nil {
			t.Fatalf("CreatePropertyIndex(%v): %v", k, err)
		}
	}
}

// opWrite writes, per rel type and label set, a value carried only by history
// rows (anchor + delta), a current value, a deleted rel's tombstone and a
// backfilled version; with buffered, the last rows stay in the write buffer
// (half parked in `flushing`, half in `pending`).
func opWrite(t *testing.T, bs *Store, buffered bool) {
	t.Helper()
	must := func(what string, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	for i, typ := range []uint16{ptxT, opT2, opT3, opT4} {
		base := int64(100 * (i + 1))
		must("put", bs.PutRelationship(opRelRow(base+1, typ, 1, 0, 100)))
		must("replace", bs.ReplaceRelWithHistory(opRelRow(base+1, typ, 1, 1, 110), 0, opRelRow(base+1, typ, 1, 0, 100)))
		must("replace", bs.ReplaceRelWithHistory(opRelRow(base+1, typ, 2, 2, 120), 1, opRelRow(base+1, typ, 1, 1, 110)))
		must("put", bs.PutRelationship(opRelRow(base+2, typ, 3, 0, 130)))
		tomb := opRelRow(base+2, typ, 4, 0, 130)
		tomb.Temporal().DeletedAt, tomb.Temporal().TxTo = 140, 140
		must("delete", bs.DeleteRelWithHistory(types.RelID(base+2), 0, tomb))
		must("put", bs.PutRelationship(opRelRow(base+3, typ, 5, 1, 150)))
		must("put version", bs.PutRelVersion(types.RelID(base+3), 0, opRelRow(base+3, typ, 1, 0, 50)))
	}
	for i, labels := range [][]uint16{{ptxLabel}, {opL2}, {ptxLabel, opL2}, {9}} {
		base := int64(1000 + 100*i)
		must("put", bs.PutNode(opNodeRow(base+1, labels, 1, 0, 100)))
		must("replace", bs.ReplaceNodeWithHistory(opNodeRow(base+1, labels, 1, 1, 110), 0, opNodeRow(base+1, labels, 1, 0, 100)))
		must("replace", bs.ReplaceNodeWithHistory(opNodeRow(base+1, labels, 2, 2, 120), 1, opNodeRow(base+1, labels, 1, 1, 110)))
		must("put", bs.PutNode(opNodeRow(base+2, labels, 5, 1, 150)))
		must("put version", bs.PutNodeVersion(types.NodeID(base+2), 0, opNodeRow(base+2, labels, 1, 0, 50)))
	}
	must("flush", bs.Flush())
	if !buffered {
		return
	}
	must("put buffered", bs.PutRelationship(opRelRow(901, ptxT, 6, 0, 200)))
	must("put buffered", bs.PutNode(opNodeRow(1901, []uint16{ptxLabel, opL2}, 6, 0, 200)))
	parkPendingIntoFlushing(t, bs)
	must("put buffered", bs.PutRelationship(opRelRow(902, opT3, 7, 0, 210)))
	must("put version buffered", bs.PutRelVersion(types.RelID(902), 1, opRelRow(902, opT3, 8, 1, 220)))
}

// opValues are every value key opWrite writes (seat 1..8, zone 11..18).
func opValues() []string {
	var out []string
	for v := int64(1); v <= 18; v++ {
		out = append(out, ptxVK(v))
	}
	return out
}

// opDump renders one built sidecar: every value's members, sorted.
func opDump(t *testing.T, bs *Store, rel *indexpkg.RelPropertyIndexKey, node *indexpkg.PropertyIndexKey) string {
	t.Helper()
	var lines []string
	for _, vk := range opValues() {
		got := map[int64]types.Instant{}
		var err error
		if rel != nil {
			err = bs.ForEachRelPropertyTxMember(rel.RelTypeToken, rel.PropertyKey, vk, func(id types.RelID, tx types.Instant) bool {
				got[int64(id.SnowflakeID())] = tx
				return true
			})
		} else {
			err = bs.ForEachNodePropertyTxMember(node.LabelToken, node.PropertyKey, vk, func(id types.NodeID, tx types.Instant) bool {
				got[int64(id.SnowflakeID())] = tx
				return true
			})
		}
		if err != nil {
			t.Fatalf("lookup %q: %v", vk, err)
		}
		if len(got) > 0 {
			lines = append(lines, fmt.Sprintf("%q=%s", vk, fmtPtx(got)))
		}
	}
	sort.Strings(lines)
	return strings.Join(lines, " ")
}

// opCountScans installs a scan counter (the hook runs once per store pass).
func opCountScans(bs *Store) *atomic.Int64 {
	var n atomic.Int64
	bs.propTxScanTestHook = func() { n.Add(1) }
	return &n
}

// TestPropTxBuild_OnePassScanCount: lookups on N declared indexes of one kind
// scan the store once. Faulty build caught: one scan per (type, key) (ai-soc:
// 29 scans for its first page).
func TestPropTxBuild_OnePassScanCount(t *testing.T) {
	bs := newFlushParkStore(t, func(c *Config) { c.HistoryDeltaEncoding, c.HistoryAnchorInterval = true, 2 })
	opDeclare(t, bs, opRelKeys, opNodeKeys)
	opWrite(t, bs, true)
	scans := opCountScans(bs)
	for i := range opRelKeys {
		_ = opDump(t, bs, &opRelKeys[i], nil)
	}
	if n := scans.Load(); n != 1 {
		t.Fatalf("lookups on %d rel indexes scanned the store %d times, want 1", len(opRelKeys), n)
	}
	for i := range opNodeKeys {
		_ = opDump(t, bs, nil, &opNodeKeys[i])
	}
	if n := scans.Load(); n != 2 {
		t.Fatalf("lookups on %d node indexes scanned the store %d times, want 1", len(opNodeKeys), n-1)
	}
	if st, _ := bs.PropertyTxMembershipStats(); st.RelSidecars != len(opRelKeys) || st.NodeSidecars != len(opNodeKeys) || st.Builds != 2 {
		t.Fatalf("stats = %+v, want %d rel and %d node sidecars from 2 passes", st, len(opRelKeys), len(opNodeKeys))
	}
}

// TestPropTxBuild_OnePassEqualsPerKeyBuild: the sidecar a shared pass builds
// equals the one a store declaring only that index builds (the per-key
// build), over committed, delta, `flushing` and `pending` rows. Faulty
// passes caught: keyed by type only (two keys on ptxT), one label per node row
// (rows carrying ptxLabel and opL2), recording the unindexed type opT4 or
// label 9, losing the buffered rows.
func TestPropTxBuild_OnePassEqualsPerKeyBuild(t *testing.T) {
	cfg := func(c *Config) { c.HistoryDeltaEncoding, c.HistoryAnchorInterval = true, 2 }
	all := newFlushParkStore(t, cfg)
	opDeclare(t, all, opRelKeys, opNodeKeys)
	opWrite(t, all, true)
	for i := range opRelKeys {
		one := newFlushParkStore(t, cfg)
		opDeclare(t, one, opRelKeys[i:i+1], nil)
		opWrite(t, one, true)
		want := opDump(t, one, &opRelKeys[i], nil)
		if want == "" {
			t.Fatalf("%v: empty per-key sidecar", opRelKeys[i])
		}
		if got := opDump(t, all, &opRelKeys[i], nil); got != want {
			t.Fatalf("%v: shared pass\n got %s\nwant %s", opRelKeys[i], got, want)
		}
	}
	for i := range opNodeKeys {
		one := newFlushParkStore(t, cfg)
		opDeclare(t, one, nil, opNodeKeys[i:i+1])
		opWrite(t, one, true)
		want := opDump(t, one, nil, &opNodeKeys[i])
		if want == "" {
			t.Fatalf("%v: empty per-key sidecar", opNodeKeys[i])
		}
		if got := opDump(t, all, nil, &opNodeKeys[i]); got != want {
			t.Fatalf("%v: shared pass\n got %s\nwant %s", opNodeKeys[i], got, want)
		}
	}
}

// TestPropTxBuild_CleanReopenRestoresSidecars: a clean close persists every
// built sidecar in the open image; the reopen serves them with no scan, and
// the doors keep recording into them. Faulty implementations caught: an image
// without the sidecars (a scan after the reopen), restoring an unbuilt
// sidecar as built (the index looked up only after the reopen must build, in
// one pass), trusting the image after a later commit (the raw write: the
// sidecars must be rebuilt, not restored), a restored sidecar the doors do not
// record into (rel 950 written after the reopen).
func TestPropTxBuild_CleanReopenRestoresSidecars(t *testing.T) {
	dir := t.TempDir()
	open := func() *Store {
		t.Helper()
		bs, err := New(Config{Dir: dir, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return bs
	}
	bs := open()
	opDeclare(t, bs, opRelKeys, opNodeKeys)
	opWrite(t, bs, false)
	before := map[string]string{}
	for i := range opRelKeys[:3] { // opRelKeys[3] stays unbuilt
		before[fmt.Sprint(opRelKeys[i])] = opDump(t, bs, &opRelKeys[i], nil)
	}
	for i := range opNodeKeys {
		before[fmt.Sprint(opNodeKeys[i])] = opDump(t, bs, nil, &opNodeKeys[i])
	}
	// opRelKeys[3] was claimed by the first pass: drop and re-declare it so
	// it is declared but unbuilt at close.
	if err := bs.DropRelPropertyIndex(opRelKeys[3].RelTypeToken, opRelKeys[3].PropertyKey); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(opRelKeys[3].RelTypeToken, opRelKeys[3].PropertyKey); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs = open()
	scans := opCountScans(bs)
	if st, _ := bs.PropertyTxMembershipStats(); st.RelSidecars != 3 || st.NodeSidecars != len(opNodeKeys) || st.Builds != 0 {
		t.Fatalf("clean reopen stats = %+v, want 3 rel and %d node sidecars restored, no build", st, len(opNodeKeys))
	}
	for i := range opRelKeys[:3] {
		if got, want := opDump(t, bs, &opRelKeys[i], nil), before[fmt.Sprint(opRelKeys[i])]; got != want {
			t.Fatalf("%v after clean reopen\n got %s\nwant %s", opRelKeys[i], got, want)
		}
	}
	for i := range opNodeKeys {
		if got, want := opDump(t, bs, nil, &opNodeKeys[i]), before[fmt.Sprint(opNodeKeys[i])]; got != want {
			t.Fatalf("%v after clean reopen\n got %s\nwant %s", opNodeKeys[i], got, want)
		}
	}
	if n := scans.Load(); n != 0 {
		t.Fatalf("lookups on restored sidecars scanned the store %d times", n)
	}
	if err := bs.PutRelationship(opRelRow(950, ptxT, 3, 0, 300)); err != nil {
		t.Fatal(err)
	}
	if got := opDump(t, bs, &opRelKeys[0], nil); !strings.Contains(got, "950:300") {
		t.Fatalf("a write after the reopen is not recorded in the restored sidecar: %s", got)
	}
	unbuilt := opDump(t, bs, &opRelKeys[3], nil)
	if n := scans.Load(); n != 1 || unbuilt == "" {
		t.Fatalf("the index unbuilt at close: %d scans, members %q; want one pass", n, unbuilt)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	// A commit after the clean close: the image is not trusted.
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
	bs = open()
	defer bs.Close()
	scans = opCountScans(bs)
	if st, _ := bs.PropertyTxMembershipStats(); st.RelSidecars != 0 || st.NodeSidecars != 0 {
		t.Fatalf("an untrusted image restored sidecars: %+v", st)
	}
	for i := range opRelKeys[:2] { // ptxT: rel 950 joined both keys
		if got := opDump(t, bs, &opRelKeys[i], nil); !strings.Contains(got, "950:300") {
			t.Fatalf("rebuilt %v lost rel 950: %s", opRelKeys[i], got)
		}
	}
	if got, want := opDump(t, bs, &opRelKeys[2], nil), before[fmt.Sprint(opRelKeys[2])]; got != want {
		t.Fatalf("%v after an untrusted reopen\n got %s\nwant %s", opRelKeys[2], got, want)
	}
	if n := scans.Load(); n != 1 {
		t.Fatalf("rebuild after an untrusted reopen scanned %d times, want 1", n)
	}
}
