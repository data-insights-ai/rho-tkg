package badger

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"runtime"
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
// alias neither badger's buffers nor each other, and the allocation per version.

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

// histDecodeAllocLimits are the per-version ceilings of
// TestHistDecodeAllocsPerVersion (rows of about 400 B, deltaVersionRel /
// deltaVersionNode). Measured 3 Nov (tasks/evidence/RELHIST/p3-red.txt), rel /
// node, N=10 and N=100: before 36.2 / 46.2 and 34.3 / 44.6 allocations, 3,178 /
// 3,618 and 2,990 / 3,460 B; after 30.6 / 39.6 and 28.4 / 37.6, 2,648 / 3,024
// and 2,410 / 2,820 B. The limits sit between: a value copy, a key copy and
// string, a map entry or the row's deep copy per version, back again, fails.
var histDecodeAllocLimits = map[string]struct{ allocs, bytes float64 }{
	"rel":  {allocs: 32, bytes: 2800},
	"node": {allocs: 41, bytes: 3200},
}

// TestHistDecodeAllocsPerVersion measures a whole-chain read on badger (rows
// committed, delta encoding on, anchor interval 16) at 10 and 100 versions:
// allocations and bytes per version, rel and node.
func TestHistDecodeAllocsPerVersion(t *testing.T) {
	if raceEnabled {
		t.Skip("allocation counts under -race are not the product's")
	}
	for _, n := range []uint32{10, 100} {
		id := snowflake.ID(43)
		bs := histDecodeStore(t, Config{InMemory: true, HistoryDeltaEncoding: true})
		for v := uint32(0); v < n; v++ {
			if err := bs.PutRelVersion(types.RelID(id), v, deltaVersionRel(id, v)); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNodeVersion(types.NodeID(id), v, deltaVersionNode(id, v)); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		for _, kind := range []string{"rel", "node"} {
			read := func() {
				var err error
				if kind == "rel" {
					var rows []*types.Relationship
					rows, err = bs.GetRelHistory(types.RelID(id))
					if err == nil && len(rows) != int(n) {
						err = fmt.Errorf("%d rows", len(rows))
					}
				} else {
					var rows []*types.Node
					rows, err = bs.GetNodeHistory(types.NodeID(id))
					if err == nil && len(rows) != int(n) {
						err = fmt.Errorf("%d rows", len(rows))
					}
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			// The counters are process-wide: the least of three rounds.
			const runs = 20
			allocs, perVersion := math.Inf(1), math.Inf(1)
			for round := 0; round < 3; round++ {
				allocs = min(allocs, testing.AllocsPerRun(runs, read)/float64(n))
				var before, after runtime.MemStats
				runtime.GC()
				runtime.ReadMemStats(&before)
				for i := 0; i < runs; i++ {
					read()
				}
				runtime.ReadMemStats(&after)
				perVersion = min(perVersion, float64(after.TotalAlloc-before.TotalAlloc)/runs/float64(n))
			}
			limit := histDecodeAllocLimits[kind]
			t.Logf("%s N=%d: %.1f allocs/version, %.0f B/version", kind, n, allocs, perVersion)
			if allocs > limit.allocs || perVersion > limit.bytes {
				t.Errorf("%s N=%d: %.1f allocs and %.0f B per version, limit %.0f and %.0f",
					kind, n, allocs, perVersion, limit.allocs, limit.bytes)
			}
		}
	}
}
