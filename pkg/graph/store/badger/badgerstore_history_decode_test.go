package badger

import (
	"bytes"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	badgerv4 "github.com/dgraph-io/badger/v4"
)

// RELHIST P3 (tasks/evidence/RELHIST/design.txt): a whole-chain history read
// decodes each version inside badger's Item.Value instead of copying every value
// into a map, sorting the keys and deep-copying each decoded row. These tests pin
// that the rows stay those of the implementation before it (legacy*ByPrefix
// below is that implementation, verbatim but for the scan test hook), that they
// alias neither badger's buffers nor each other; the allocation per version is
// bounded at the measurement level (badgerstore_history_decode_measure_test.go,
// MEMGATE).

// legacyRelHistoryByPrefix is getRelHistoryByPrefix as of main befdf434.
func legacyRelHistoryByPrefix(bs *Store, prefix []byte) ([]*types.Relationship, error) {
	expectedID := storepkg.ParseIDFromKey(prefix, 1)
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	entries := make(map[string][]byte)
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			k := string(key)
			if err := item.Value(func(val []byte) error {
				cp := make([]byte, len(val))
				copy(cp, val)
				entries[k] = cp
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for k := range overlayDeletes {
		delete(entries, k)
	}
	for k, v := range overlay {
		entries[k] = v
	}
	if len(entries) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	localAnchor := func(anchorVer uint64) ([]byte, bool) {
		b, ok := entries[string(storepkg.HistRelKey(expectedID, anchorVer))]
		return b, ok
	}
	result := make([]*types.Relationship, 0, len(keys))
	for _, k := range keys {
		version := historyVersionFromKey([]byte(k))
		w, err := bs.reconstructRelHistoryWire(expectedID, version, entries[k], localAnchor)
		if err != nil {
			return nil, fmt.Errorf("graph: unmarshal rel version: %w", err)
		}
		r, err := bs.decodeRelHistoryWireForKey(w, expectedID, version)
		if err != nil {
			return nil, fmt.Errorf("graph: decode rel version: %w", err)
		}
		result = append(result, r.DeepCopy())
	}
	return result, nil
}

// legacyNodeHistoryByPrefix is getNodeHistoryByPrefix as of main befdf434.
func legacyNodeHistoryByPrefix(bs *Store, prefix []byte) ([]*types.Node, error) {
	expectedID := storepkg.ParseIDFromKey(prefix, 1)
	overlay, overlayDeletes := bs.pendingHistoryVersionOverlay(prefix, 0)
	entries := make(map[string][]byte)
	err := bs.view(func(txn *badgerv4.Txn) error {
		opts := badgerv4.DefaultIteratorOptions
		opts.PrefetchValues = false
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			key := item.KeyCopy(nil)
			if len(key) != storepkg.SizeHistKey {
				continue
			}
			k := string(key)
			if err := item.Value(func(val []byte) error {
				cp := make([]byte, len(val))
				copy(cp, val)
				entries[k] = cp
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for k := range overlayDeletes {
		delete(entries, k)
	}
	for k, v := range overlay {
		entries[k] = v
	}
	if len(entries) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	localAnchor := func(anchorVer uint64) ([]byte, bool) {
		b, ok := entries[string(storepkg.HistNodeKey(expectedID, anchorVer))]
		return b, ok
	}
	result := make([]*types.Node, 0, len(keys))
	for _, k := range keys {
		version := historyVersionFromKey([]byte(k))
		w, err := bs.reconstructNodeHistoryWire(expectedID, version, entries[k], localAnchor)
		if err != nil {
			return nil, fmt.Errorf("graph: unmarshal node version: %w", err)
		}
		n, err := bs.decodeNodeHistoryWireForKey(w, expectedID, version)
		if err != nil {
			return nil, fmt.Errorf("graph: decode node version: %w", err)
		}
		result = append(result, n.DeepCopy())
	}
	return result, nil
}

// histDecodeBig is a large property value (60 KB), on every ninth version.
var histDecodeBig = strings.Repeat("large value / ", 60*1024/14)

// histDecodeSkip leaves gaps in the chain.
func histDecodeSkip(v uint32) bool { return v%11 == 10 }

// histDecodeTemporal: every seventh version a delete tombstone (DeletedAt, TxTo),
// the versions after it live again (a restored entity).
func histDecodeTemporal(v uint32, gen int) *types.TemporalMetadata {
	tm := &types.TemporalMetadata{TxFrom: types.Instant(1000 + int64(v)*100 + int64(gen)), ValidFrom: types.Instant(500 + int64(v))}
	if v%7 == 3 {
		tm.DeletedAt = tm.TxFrom
		tm.TxTo = tm.TxFrom + 50
		tm.ValidTo = tm.ValidFrom + 10
	}
	return tm
}

// histDecodeSetProps gives version v (generation gen) a property mix whose
// deltas add, change and remove properties against the anchor.
func histDecodeSetProps(set func(string, any) error, v uint32, gen int) {
	_ = set("blob", deltaBlob)
	_ = set("weight", int64(v)*10+int64(gen))
	_ = set("raw", []byte(fmt.Sprintf("raw-%d-%d", v, gen)))
	if v%5 != 0 {
		_ = set("status", []string{"open", "ack", "closed"}[v%3])
	}
	if v%6 == 1 {
		_ = set("extra", fmt.Sprintf("extra-%d", v))
	}
	if v%9 == 0 {
		_ = set("big", histDecodeBig)
	}
}

func histDecodeRel(id snowflake.ID, v uint32, gen int) *types.Relationship {
	r := types.NewRelationship(types.RelID(id), 5, types.NodeID(snowflake.ID(100)), types.NodeID(snowflake.ID(200)))
	histDecodeSetProps(r.SetProperty, v, gen)
	r.SetVersion(v)
	r.SetTemporal(histDecodeTemporal(v, gen))
	return r
}

func histDecodeNode(id snowflake.ID, v uint32, gen int) *types.Node {
	n := types.NewNode(types.NodeID(id), 10, []uint16{20, 21})
	histDecodeSetProps(n.SetProperty, v, gen)
	n.SetVersion(v)
	n.SetTemporal(histDecodeTemporal(v, gen))
	return n
}

// histDecodePut writes versions [from, to) of both a rel and a node, skipping gaps.
func histDecodePut(t *testing.T, bs *Store, id snowflake.ID, from, to uint32, gen int) {
	t.Helper()
	for v := from; v < to; v++ {
		if histDecodeSkip(v) {
			continue
		}
		if err := bs.PutRelVersion(types.RelID(id), v, histDecodeRel(id, v, gen)); err != nil {
			t.Fatalf("PutRelVersion v%d: %v", v, err)
		}
		if err := bs.PutNodeVersion(types.NodeID(id), v, histDecodeNode(id, v, gen)); err != nil {
			t.Fatalf("PutNodeVersion v%d: %v", v, err)
		}
	}
}

func histDecodeStore(t *testing.T, cfg Config) *Store {
	t.Helper()
	cfg.FlushInterval = 1<<63 - 1
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	return bs
}

// histDecodeRelWires snapshots each row's wire bytes and ordinals.
func histDecodeRelWires(t *testing.T, rows []*types.Relationship) []string {
	t.Helper()
	out := make([]string, len(rows))
	for i, r := range rows {
		out[i] = fmt.Sprintf("%x/%d/%d/%d", relWireBytes(t, r), r.Ordinal(), r.StartOrdinal(), r.EndOrdinal())
	}
	return out
}

func histDecodeNodeWires(t *testing.T, rows []*types.Node) []string {
	t.Helper()
	out := make([]string, len(rows))
	for i, n := range rows {
		out[i] = fmt.Sprintf("%x/%d", nodeWireBytes(t, n), n.Ordinal())
	}
	return out
}

// histDecodeSameRels / histDecodeSameNodes put rows in the legacy rows' form:
// the legacy chain read returned DeepCopy of each fresh decode, which differs
// from the decode only in the copy-on-write flag sharedProps (a node decode
// installs its own property slice flagged shared, wire.go applyNodeWireFields;
// the copy clears it). Every other field, unexported ones included, is compared
// as it is. Aliasing is TestHistDecodeRowsOwnTheirBytes's.
func histDecodeSameRels(rows []*types.Relationship) []*types.Relationship {
	if rows == nil {
		return nil
	}
	out := make([]*types.Relationship, len(rows))
	for i, r := range rows {
		out[i] = r.DeepCopy()
	}
	return out
}

func histDecodeSameNodes(rows []*types.Node) []*types.Node {
	if rows == nil {
		return nil
	}
	out := make([]*types.Node, len(rows))
	for i, n := range rows {
		out[i] = n.DeepCopy()
	}
	return out
}

// histDecodeCheck compares GetRelHistory / GetNodeHistory with the legacy
// implementation (deeply, unexported fields included) and with a point read of
// each version, and asserts the exact version list. It returns the rows.
func histDecodeCheck(t *testing.T, bs *Store, id snowflake.ID, wantVersions []uint32) ([]*types.Relationship, []*types.Node) {
	t.Helper()
	rels, err := bs.GetRelHistory(types.RelID(id))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	legacyRels, err := legacyRelHistoryByPrefix(bs, storepkg.HistRelPrefix(id))
	if err != nil {
		t.Fatalf("legacy rel history: %v", err)
	}
	nodes, err := bs.GetNodeHistory(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	legacyNodes, err := legacyNodeHistoryByPrefix(bs, storepkg.HistNodePrefix(id))
	if err != nil {
		t.Fatalf("legacy node history: %v", err)
	}
	if len(rels) != len(wantVersions) || len(nodes) != len(wantVersions) {
		t.Fatalf("history lengths rel %d node %d, want %d", len(rels), len(nodes), len(wantVersions))
	}
	if !reflect.DeepEqual(histDecodeSameRels(rels), legacyRels) {
		t.Fatalf("GetRelHistory differs from the legacy rows")
	}
	if !reflect.DeepEqual(histDecodeSameNodes(nodes), legacyNodes) {
		t.Fatalf("GetNodeHistory differs from the legacy rows")
	}
	// The paged doors (RelHistoryVersionsFrom / NodeHistoryVersionsFrom) return
	// the same rows as the whole chain from their start, a delta whose anchor
	// precedes the start included.
	for _, pg := range []struct {
		start uint32
		limit int
	}{{0, 0}, {3, 5}, {17, 10}, {wantVersions[len(wantVersions)/2], 0}} {
		var wantR []*types.Relationship
		var wantN []*types.Node
		for i := range legacyRels {
			if legacyRels[i].Version() >= pg.start && (pg.limit == 0 || len(wantR) < pg.limit) {
				wantR = append(wantR, legacyRels[i])
				wantN = append(wantN, legacyNodes[i])
			}
		}
		gotR, err := bs.RelHistoryVersionsFrom(types.RelID(id), pg.start, pg.limit)
		if err != nil {
			t.Fatalf("RelHistoryVersionsFrom(%d, %d): %v", pg.start, pg.limit, err)
		}
		gotN, err := bs.NodeHistoryVersionsFrom(types.NodeID(id), pg.start, pg.limit)
		if err != nil {
			t.Fatalf("NodeHistoryVersionsFrom(%d, %d): %v", pg.start, pg.limit, err)
		}
		if !reflect.DeepEqual(histDecodeSameRels(gotR), wantR) || !reflect.DeepEqual(histDecodeSameNodes(gotN), wantN) {
			t.Fatalf("HistoryVersionsFrom(%d, %d) differs from the legacy chain's rows", pg.start, pg.limit)
		}
	}
	for i, v := range wantVersions {
		if rels[i].Version() != v || nodes[i].Version() != v {
			t.Fatalf("row %d: versions rel %d node %d, want %d", i, rels[i].Version(), nodes[i].Version(), v)
		}
		pr, err := bs.GetRelVersion(types.RelID(id), v)
		if err != nil {
			t.Fatalf("GetRelVersion v%d: %v", v, err)
		}
		if !bytes.Equal(relWireBytes(t, pr), relWireBytes(t, rels[i])) {
			t.Fatalf("rel v%d: history row != point read", v)
		}
		pn, err := bs.GetNodeVersion(types.NodeID(id), v)
		if err != nil {
			t.Fatalf("GetNodeVersion v%d: %v", v, err)
		}
		if !bytes.Equal(nodeWireBytes(t, pn), nodeWireBytes(t, nodes[i])) {
			t.Fatalf("node v%d: history row != point read", v)
		}
	}
	return rels, nodes
}

func histDecodeVersions(from, to uint32) []uint32 {
	var out []uint32
	for v := from; v < to; v++ {
		if !histDecodeSkip(v) {
			out = append(out, v)
		}
	}
	return out
}

// TestHistDecodeRowsEqualLegacy: delta chains (anchors, deltas adding, changing
// and removing properties), tombstones and the live versions after them, 60 KB
// values, gaps, versions across a key byte boundary (255/256), the write buffer
// overlaying committed rows (deletes and re-puts), and anchors in older tables.
func TestHistDecodeRowsEqualLegacy(t *testing.T) {
	id := snowflake.ID(41)
	t.Run("delta_flushed", func(t *testing.T) {
		bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true})
		histDecodePut(t, bs, id, 0, 300, 0)
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		if raw, err := bs.readHistoryRelRaw(id, 5); err != nil || storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
			t.Fatalf("v5 is not a delta (err %v): the chain tests no reconstruction", err)
		}
		// A key under the chain's prefix that is no history key (another
		// length) is skipped, as before.
		for _, p := range [][]byte{storepkg.HistRelPrefix(id), storepkg.HistNodePrefix(id)} {
			foreign := append(append([]byte(nil), p...), 0, 0, 0, 0, 0, 0, 0, 7, 1)
			if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(foreign, []byte{0xc1}) }); err != nil {
				t.Fatal(err)
			}
		}
		histDecodeCheck(t, bs, id, histDecodeVersions(0, 300))
	})
	t.Run("full_flushed", func(t *testing.T) {
		bs := histDecodeStore(t, Config{InMemory: true})
		histDecodePut(t, bs, id, 0, 100, 0)
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		histDecodeCheck(t, bs, id, histDecodeVersions(0, 100))
	})
	t.Run("delta_pending_only", func(t *testing.T) {
		bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		histDecodePut(t, bs, id, 0, 40, 0)
		histDecodeCheck(t, bs, id, histDecodeVersions(0, 40))
	})
	t.Run("delta_overlay_over_committed", func(t *testing.T) {
		bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		histDecodePut(t, bs, id, 0, 70, 0)
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		// Buffered deletes of 40.. mask committed rows; 40..55 come back with
		// other content (a buffered anchor at 40, 44, ... over committed ones).
		if err := bs.TrimRelHistoryFrom(types.RelID(id), 40); err != nil {
			t.Fatal(err)
		}
		if err := bs.TrimNodeHistoryFrom(types.NodeID(id), 40); err != nil {
			t.Fatal(err)
		}
		histDecodePut(t, bs, id, 40, 56, 1)
		histDecodeCheck(t, bs, id, histDecodeVersions(0, 56))
		// The chain with the overlay committed reads the same rows.
		before, _ := histDecodeCheck(t, bs, id, histDecodeVersions(0, 56))
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		after, _ := histDecodeCheck(t, bs, id, histDecodeVersions(0, 56))
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("rows changed when the overlay was committed")
		}
	})
	t.Run("delta_anchor_in_older_table", func(t *testing.T) {
		dir := t.TempDir()
		cfg := Config{Dir: dir, HistoryDeltaEncoding: true}
		open := func() *Store {
			c := cfg
			c.FlushInterval = 1<<63 - 1
			bs, err := New(c)
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			return bs
		}
		bs := open()
		histDecodePut(t, bs, id, 0, 6, 0)
		if err := bs.Close(); err != nil { // the anchor 0 goes to its own L0 table
			t.Fatal(err)
		}
		bs = open()
		histDecodePut(t, bs, id, 6, 40, 0)
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		bs = open()
		t.Cleanup(func() { _ = bs.Close() })
		histDecodeCheck(t, bs, id, histDecodeVersions(0, 40))
	})
}

// TestHistDecodeRowsOwnTheirBytes: the returned rows alias neither badger's
// buffers (an iterator item's value buffer is reused at Next, badger
// iterator.go:139-151, :573-577), nor each other, nor a later read. Rows are
// snapshotted, every row's []byte value is overwritten in place and every row
// gets a new property; the other rows, a fresh read and the rows read before
// committed keys were rewritten must not change.
func TestHistDecodeRowsOwnTheirBytes(t *testing.T) {
	id := snowflake.ID(42)
	bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
	histDecodePut(t, bs, id, 0, 50, 0)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	want := histDecodeVersions(0, 50)
	rels, nodes := histDecodeCheck(t, bs, id, want)
	relWant := histDecodeRelWires(t, rels)
	nodeWant := histDecodeNodeWires(t, nodes)

	// Rewrite every committed key with other content and commit it.
	if err := bs.TrimRelHistoryFrom(types.RelID(id), 0); err != nil {
		t.Fatal(err)
	}
	if err := bs.TrimNodeHistoryFrom(types.NodeID(id), 0); err != nil {
		t.Fatal(err)
	}
	histDecodePut(t, bs, id, 0, 50, 7)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if _, err := bs.GetRelHistory(types.RelID(id)); err != nil {
		t.Fatal(err)
	}
	if got := histDecodeRelWires(t, rels); !reflect.DeepEqual(got, relWant) {
		t.Fatalf("rel rows changed after their keys were rewritten")
	}
	if got := histDecodeNodeWires(t, nodes); !reflect.DeepEqual(got, nodeWant) {
		t.Fatalf("node rows changed after their keys were rewritten")
	}

	// Scribble over each row's bytes; no other row may change.
	for i := range rels {
		if v, ok := rels[i].GetProperty("raw"); ok {
			b := v.([]byte)
			for j := range b {
				b[j] = 'X'
			}
		}
		if err := rels[i].SetProperty("blob", "scribbled"); err != nil {
			t.Fatalf("a history row is not the caller's to write: %v", err)
		}
		if v, ok := nodes[i].GetProperty("raw"); ok {
			b := v.([]byte)
			for j := range b {
				b[j] = 'X'
			}
		}
		if err := nodes[i].SetProperty("blob", "scribbled"); err != nil {
			t.Fatalf("a history row is not the caller's to write: %v", err)
		}
		gotR, gotN := histDecodeRelWires(t, rels), histDecodeNodeWires(t, nodes)
		if gotR[i] == relWant[i] || gotN[i] == nodeWant[i] {
			t.Fatalf("scribbling row %d did not reach it: the test proves nothing", i)
		}
		for j := i + 1; j < len(rels); j++ {
			if gotR[j] != relWant[j] || gotN[j] != nodeWant[j] {
				t.Fatalf("scribbling row %d changed row %d", i, j)
			}
		}
	}
	fresh, freshNodes := histDecodeCheck(t, bs, id, want)
	for i := range fresh {
		if b, _ := fresh[i].GetProperty("raw"); bytes.Contains(b.([]byte), []byte("XX")) {
			t.Fatalf("a fresh rel read sees the scribbled bytes at v%d", fresh[i].Version())
		}
		if b, _ := freshNodes[i].GetProperty("raw"); bytes.Contains(b.([]byte), []byte("XX")) {
			t.Fatalf("a fresh node read sees the scribbled bytes at v%d", freshNodes[i].Version())
		}
	}
}

// TestHistDecodeAnchorOutsideChain: a delta whose anchor is not in the scanned
// chain is decoded after the scan by a point read of the anchor, in its place
// in the result. With the anchor key deleted below rho the chain fails with the
// legacy error; with the key put back inside the scan->decode window
// (historyScanTestHook) the point read finds it and the rows are the original
// ones, in order.
func TestHistDecodeAnchorOutsideChain(t *testing.T) {
	id := snowflake.ID(44)
	bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
	histDecodePut(t, bs, id, 0, 30, 0)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	const anchor = 12 // 13, 14, 15 are its deltas
	rels, nodes := histDecodeCheck(t, bs, id, histDecodeVersions(0, 30))
	var relWant, nodeWant []string // the chain without the anchor's own row (scanned before it is back)
	for i, w := range histDecodeRelWires(t, rels) {
		if rels[i].Version() != anchor {
			relWant = append(relWant, w)
		}
	}
	for i, w := range histDecodeNodeWires(t, nodes) {
		if nodes[i].Version() != anchor {
			nodeWant = append(nodeWant, w)
		}
	}

	relKey, nodeKey := storepkg.HistRelKey(id, anchor), storepkg.HistNodeKey(id, anchor)
	raws := map[string][]byte{}
	for _, key := range [][]byte{relKey, nodeKey} {
		raw, err := bs.readHistoryRaw(key)
		if err != nil {
			t.Fatal(err)
		}
		if storepkg.HistoryValueKindOf(raw) != storepkg.HistoryFull {
			t.Fatalf("v%d is not a full anchor", anchor)
		}
		raws[string(key)] = raw
	}
	setKey := func(key []byte, del bool) {
		t.Helper()
		if err := bs.db.Update(func(txn *badgerv4.Txn) error {
			if del {
				return txn.Delete(key)
			}
			return txn.Set(key, raws[string(key)])
		}); err != nil {
			t.Fatal(err)
		}
	}
	setKey(relKey, true)
	setKey(nodeKey, true)

	// A damaged row after the orphaned deltas: the first failure in version
	// order (v13, the anchor's) is the one reported, as before.
	damaged := storepkg.HistRelKey(id, 20)
	good20, err := bs.readHistoryRaw(damaged)
	if err != nil {
		t.Fatal(err)
	}
	raws[string(damaged)] = []byte{0xc1}
	setKey(damaged, false)
	_, gotErr := bs.GetRelHistory(types.RelID(id))
	_, legacyErr := legacyRelHistoryByPrefix(bs, storepkg.HistRelPrefix(id))
	if gotErr == nil || legacyErr == nil || gotErr.Error() != legacyErr.Error() || !strings.Contains(gotErr.Error(), "anchor v12") {
		t.Fatalf("missing rel anchor and a damaged row: error %v, legacy %v", gotErr, legacyErr)
	}
	if _, err := bs.GetRelVersion(types.RelID(id), 20); err == nil {
		t.Fatalf("v20 is not damaged: the test proves nothing")
	}
	raws[string(damaged)] = good20
	setKey(damaged, false)
	_, gotErr = bs.GetRelHistory(types.RelID(id))
	_, legacyErr = legacyRelHistoryByPrefix(bs, storepkg.HistRelPrefix(id))
	if gotErr == nil || legacyErr == nil || gotErr.Error() != legacyErr.Error() {
		t.Fatalf("missing rel anchor: error %v, legacy %v", gotErr, legacyErr)
	}
	_, gotErr = bs.GetNodeHistory(types.NodeID(id))
	_, legacyErr = legacyNodeHistoryByPrefix(bs, storepkg.HistNodePrefix(id))
	if gotErr == nil || legacyErr == nil || gotErr.Error() != legacyErr.Error() {
		t.Fatalf("missing node anchor: error %v, legacy %v", gotErr, legacyErr)
	}

	defer func() { bs.historyScanTestHook = nil }()
	bs.historyScanTestHook = func() { setKey(relKey, false) }
	got, err := bs.GetRelHistory(types.RelID(id))
	if err != nil {
		t.Fatalf("GetRelHistory, anchor back after the scan: %v", err)
	}
	if !reflect.DeepEqual(histDecodeRelWires(t, got), relWant) {
		t.Fatalf("rel rows, anchor read after the scan, differ")
	}
	bs.historyScanTestHook = func() { setKey(nodeKey, false) }
	gotNodes, err := bs.GetNodeHistory(types.NodeID(id))
	if err != nil {
		t.Fatalf("GetNodeHistory, anchor back after the scan: %v", err)
	}
	if !reflect.DeepEqual(histDecodeNodeWires(t, gotNodes), nodeWant) {
		t.Fatalf("node rows, anchor read after the scan, differ")
	}
}

// histDecodeSameError asserts GetRelHistory and GetNodeHistory fail with the
// legacy implementation's error, and that it contains want.
func histDecodeSameError(t *testing.T, bs *Store, id snowflake.ID, want string) {
	t.Helper()
	_, gotErr := bs.GetRelHistory(types.RelID(id))
	_, legacyErr := legacyRelHistoryByPrefix(bs, storepkg.HistRelPrefix(id))
	if gotErr == nil || legacyErr == nil || gotErr.Error() != legacyErr.Error() || !strings.Contains(gotErr.Error(), want) {
		t.Fatalf("rel: error %v, legacy %v, want one containing %q", gotErr, legacyErr, want)
	}
	_, gotErr = bs.GetNodeHistory(types.NodeID(id))
	_, legacyErr = legacyNodeHistoryByPrefix(bs, storepkg.HistNodePrefix(id))
	if gotErr == nil || legacyErr == nil || gotErr.Error() != legacyErr.Error() || !strings.Contains(gotErr.Error(), want) {
		t.Fatalf("node: error %v, legacy %v, want one containing %q", gotErr, legacyErr, want)
	}
}

// TestHistDecodeOverlayInterleave pins the merge of buffered rows into the
// committed walk (Fable review of histdecode, 3 Nov): (a) buffered versions
// without a committed twin between committed keys come out in version order;
// (b) a buffered delete of an anchor whose deltas stay committed sends those
// deltas to the point read, which sees the delete and fails as before; (c) with
// a damaged row on each side of the overlay boundary, the first in version
// order is the one reported, either way round.
func TestHistDecodeOverlayInterleave(t *testing.T) {
	put := func(t *testing.T, bs *Store, id snowflake.ID, vs ...uint32) {
		t.Helper()
		for _, v := range vs {
			if err := bs.PutRelVersion(types.RelID(id), v, histDecodeRel(id, v, 0)); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNodeVersion(types.NodeID(id), v, histDecodeNode(id, v, 0)); err != nil {
				t.Fatal(err)
			}
		}
	}
	flush := func(t *testing.T, bs *Store) {
		t.Helper()
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
	}
	keys := func(id snowflake.ID, v uint64) [][]byte {
		return [][]byte{storepkg.HistRelKey(id, v), storepkg.HistNodeKey(id, v)}
	}
	t.Run("a_buffered_between_committed", func(t *testing.T) {
		id := snowflake.ID(45)
		bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		var even, odd, all []uint32
		for v := uint32(0); v <= 20; v++ {
			all = append(all, v)
			if v%2 == 0 {
				even = append(even, v)
			} else {
				odd = append(odd, v)
			}
		}
		put(t, bs, id, even...)
		flush(t, bs)
		put(t, bs, id, odd...) // buffered, no committed twin
		for _, k := range keys(id, 7) {
			if op, ok := bs.lookupPending(string(k)); !ok || op.opType != writeOpSet {
				t.Fatalf("v7 is not buffered: the case tests no interleave")
			}
		}
		histDecodeCheck(t, bs, id, all)
	})
	t.Run("b_buffered_delete_of_committed_anchor", func(t *testing.T) {
		id := snowflake.ID(46)
		bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 4})
		put(t, bs, id, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9)
		flush(t, bs)
		if raw, err := bs.readHistoryRelRaw(id, 5); err != nil || storepkg.HistoryValueKindOf(raw) != storepkg.HistoryDelta {
			t.Fatalf("v5 is not a committed delta (err %v)", err)
		}
		for _, k := range keys(id, 4) {
			bs.appendOps(writeOp{opType: writeOpDelete, key: k})
		}
		histDecodeSameError(t, bs, id, "history anchor v4")
	})
	t.Run("c_damage_across_the_overlay_boundary", func(t *testing.T) {
		damage := func(t *testing.T, bs *Store, id snowflake.ID, v uint64, committed bool, val []byte) {
			t.Helper()
			for _, k := range keys(id, v) {
				if committed {
					if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(k, val) }); err != nil {
						t.Fatal(err)
					}
				} else {
					bs.appendOps(writeOp{opType: writeOpSet, key: k, value: val})
				}
			}
		}
		committedBad, bufferedBad := []byte{0xc1}, []byte{0x92, 0x01} // two different decode errors
		for _, tc := range []struct {
			name             string
			committedV, bufV uint64
			want             string
		}{
			{"committed_first", 3, 5, "0xc1"},
			{"buffered_first", 5, 3, "truncated msgpack"},
		} {
			t.Run(tc.name, func(t *testing.T) {
				id := snowflake.ID(47)
				bs := histDecodeStore(t, Config{InMemory: true})
				put(t, bs, id, 0, 1, 2, 3, 4, 5, 6, 7)
				flush(t, bs)
				damage(t, bs, id, tc.committedV, true, committedBad)
				damage(t, bs, id, tc.bufV, false, bufferedBad)
				histDecodeSameError(t, bs, id, tc.want)
			})
		}
	})
}
