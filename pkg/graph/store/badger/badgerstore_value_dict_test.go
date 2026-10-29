package badger

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

const (
	vdAction = "connection_accepted"
	vdFolder = `C:\Windows\System32`
)

// vdDicts codes NETWORK.action and Host.folder.
func vdDicts(capacity int) []storecontract.ValueDictionary {
	return []storecontract.ValueDictionary{
		{Name: "action", Cap: capacity, Members: []storecontract.ValueDictionaryMember{{RelType: "NETWORK", Property: "action"}}},
		{Name: "folder", Cap: capacity, Members: []storecontract.ValueDictionaryMember{{Label: "Host", Property: "folder"}}},
	}
}

// vdOpen opens dir with dicts and installs registries naming label 1 "Host"
// and type 1 "NETWORK".
func vdOpen(t *testing.T, dir string, dicts []storecontract.ValueDictionary) *Store {
	t.Helper()
	bs, err := New(Config{Dir: dir, ValueDictionaries: dicts})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	labels := registrypkg.NewLabelRegistry()
	if tok, _ := labels.GetOrCreate("Host"); tok != 1 {
		t.Fatalf("Host token %d", tok)
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if tok, _ := relTypes.GetOrCreate("NETWORK"); tok != 1 {
		t.Fatalf("NETWORK token %d", tok)
	}
	bs.SetNameRegistries(labels, relTypes)
	for _, id := range []int64{7, 9} { // the endpoints of vdRel
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 2, nil)); err != nil {
			t.Fatalf("PutNode endpoint: %v", err)
		}
	}
	return bs
}

func vdRel(t *testing.T, id int64, action any) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(id)), 1, types.NodeID(snowflake.ID(7)), types.NodeID(snowflake.ID(9)))
	if err := r.SetProperty("action", action); err != nil {
		t.Fatal(err)
	}
	return r
}

func vdNode(t *testing.T, id int64, folder string) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)
	if err := n.SetProperty("folder", folder); err != nil {
		t.Fatal(err)
	}
	return n
}

// vdRaw is the stored row under key.
func vdRaw(t *testing.T, bs *Store, key []byte) []byte {
	t.Helper()
	var raw []byte
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		raw, err = item.ValueCopy(nil)
		return err
	}); err != nil {
		t.Fatalf("raw read: %v", err)
	}
	return raw
}

func vdRelAction(t *testing.T, bs *Store, id int64) any {
	t.Helper()
	r, err := bs.GetRelationship(types.RelID(snowflake.ID(id)))
	if err != nil {
		t.Fatalf("GetRelationship(%d): %v", id, err)
	}
	v, _ := r.GetProperty("action")
	return v
}

// TestValueDict_CodesMembersAndReadsPlain (DICT step 2): a member's string
// value is stored once as a dictionary entry and as a token in each row (its
// bytes are not in the rows), a short string, a non-string and a non-member
// stay inline, every value reads back unchanged, also after a reopen with no
// dictionaries configured, and the store is marked 11. Catches an encoder
// that does not code, codes a non-member or a short value, and a decoder
// that needs the plan to read coded rows.
func TestValueDict_CodesMembersAndReadsPlain(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs := vdOpen(t, dir, vdDicts(100))
	for id, action := range map[int64]any{11: vdAction, 12: vdAction, 13: "ok", 14: int64(5)} {
		if err := bs.PutRelationship(vdRel(t, id, action)); err != nil {
			t.Fatalf("PutRelationship(%d): %v", id, err)
		}
	}
	other := vdNode(t, 22, vdFolder)
	if err := other.SetProperty("path", vdFolder); err != nil { // not a member
		t.Fatal(err)
	}
	for _, n := range []*types.Node{vdNode(t, 21, vdFolder), other} {
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode: %v", err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for _, id := range []int64{11, 12} {
		if raw := vdRaw(t, bs, storepkg.RelKey(snowflake.ID(id))); bytes.Contains(raw, []byte(vdAction)) {
			t.Fatalf("rel %d stores the value inline: %q", id, raw)
		}
	}
	if raw := vdRaw(t, bs, storepkg.RelKey(snowflake.ID(13))); !bytes.Contains(raw, []byte("ok")) {
		t.Fatalf("a 2-byte value was coded: %q", raw)
	}
	if raw := vdRaw(t, bs, storepkg.NodeKey(snowflake.ID(22))); bytes.Count(raw, []byte(vdFolder)) != 1 {
		t.Fatalf("node 22 should store the non-member path inline and code folder: %q", raw)
	}
	stats := bs.ValueDictionaryStats()
	if len(stats) != 2 || stats[0].Name != "action" || stats[0].Coded != 2 || stats[0].NewEntries != 1 ||
		stats[1].Coded != 2 || stats[1].NewEntries != 1 || stats[0].InlineFull+stats[1].InlineFull != 0 {
		t.Fatalf("stats %+v, want action coded 2 / 1 entry, folder coded 2 / 1 entry, none inline-full", stats)
	}
	if a, f := stats[0], stats[1]; a.Entries != 1 || a.Bytes != int64(len(vdAction))+vdictEntryOverhead ||
		f.Entries != 1 || f.Bytes != int64(len(vdFolder))+vdictEntryOverhead {
		t.Fatalf("stats %+v, want one resident entry each, its value's length plus %d bytes", stats, vdictEntryOverhead)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if v, _ := readRawMarker(t, dir); v != storepkg.ValueDictStoreFormatVersion {
		t.Fatalf("store marker %d, want %d", v, storepkg.ValueDictStoreFormatVersion)
	}
	plain, err := New(Config{Dir: dir}) // no dictionaries, no registries
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = plain.Close() }()
	for id, want := range map[int64]any{11: vdAction, 12: vdAction, 13: "ok", 14: int64(5)} {
		if got := vdRelAction(t, plain, id); got != want {
			t.Fatalf("rel %d action = %#v, want %#v", id, got, want)
		}
	}
	for _, id := range []int64{21, 22} {
		n, err := plain.GetNode(types.NodeID(snowflake.ID(id)))
		if err != nil {
			t.Fatalf("GetNode(%d): %v", id, err)
		}
		if v, _ := n.GetProperty("folder"); v != vdFolder {
			t.Fatalf("node %d folder = %#v", id, v)
		}
	}
}

// TestValueDict_CapLeavesInlineAndCounts: past its cap a dictionary leaves
// new values inline, counts them, and both read back. Catches a cap that is
// not enforced or not counted.
func TestValueDict_CapLeavesInlineAndCounts(t *testing.T) {
	t.Parallel()
	bs := vdOpen(t, t.TempDir(), vdDicts(1))
	defer func() { _ = bs.Close() }()
	second := "connection_refused"
	for id, a := range map[int64]string{11: vdAction, 12: second} {
		if err := bs.PutRelationship(vdRel(t, id, a)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	inline := 0
	for _, id := range []int64{11, 12} {
		raw := vdRaw(t, bs, storepkg.RelKey(snowflake.ID(id)))
		if bytes.Contains(raw, []byte(vdAction)) || bytes.Contains(raw, []byte(second)) {
			inline++
		}
	}
	if s := bs.ValueDictionaryStats()[0]; inline != 1 || s.InlineFull != 1 || s.NewEntries != 1 {
		t.Fatalf("inline rows %d, stats %+v; want one coded, one inline-full", inline, s)
	}
	if vdRelAction(t, bs, 11) != vdAction || vdRelAction(t, bs, 12) != second {
		t.Fatal("a value did not read back")
	}
}

// TestValueDict_MissingEntryFailsClosed: a coded row whose entry is gone
// reads as ErrCorruptWire, never as a wrong or nil value. Catches a decoder
// that skips the token.
func TestValueDict_MissingEntryFailsClosed(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs := vdOpen(t, dir, vdDicts(100))
	if err := bs.PutRelationship(vdRel(t, 11, vdAction)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := db.DropPrefix(storepkg.MetaKey(vdictEntryMeta)); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(Config{Dir: dir})
	if err != nil {
		return // refusing the store is closed too
	}
	defer func() { _ = reopened.Close() }()
	if _, err := reopened.GetRelationship(types.RelID(snowflake.ID(11))); !errors.Is(err, storecontract.ErrCorruptWire) {
		t.Fatalf("GetRelationship with its entry gone = %v, want ErrCorruptWire", err)
	}
}

// TestValueDict_SurvivesClear: a token handed out before Clear still has its
// entry after it, so a row written after Clear with the same value reads
// back after a reopen, and the store is marked 11 again. Catches a Clear that
// drops the stored entries but keeps the in-memory tokens.
func TestValueDict_SurvivesClear(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs := vdOpen(t, dir, vdDicts(100))
	if err := bs.PutRelationship(vdRel(t, 11, vdAction)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.Clear(); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	for _, id := range []int64{7, 9} {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 2, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.PutRelationship(vdRel(t, 12, vdAction)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if v, _ := readRawMarker(t, dir); v != storepkg.ValueDictStoreFormatVersion {
		t.Fatalf("store marker after Clear %d, want %d", v, storepkg.ValueDictStoreFormatVersion)
	}
	reopened, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	if got := vdRelAction(t, reopened, 12); got != vdAction {
		t.Fatalf("rel written after Clear reads %#v", got)
	}
}

// TestValueDict_HistoryRowsCoded: history rows, full and delta, carry tokens
// and every version reads back its value. Catches a history encoder that
// bypasses the dictionary and a delta whose anchor tokens do not resolve.
func TestValueDict_HistoryRowsCoded(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, ValueDictionaries: vdDicts(100), HistoryDeltaEncoding: true})
	if err != nil {
		t.Fatal(err)
	}
	labels := registrypkg.NewLabelRegistry()
	_, _ = labels.GetOrCreate("Host")
	bs.SetNameRegistries(labels, registrypkg.NewRelTypeRegistry())
	nid := types.NodeID(snowflake.ID(21))
	folders := []string{vdFolder, `C:\Program Files`, vdFolder}
	const versions = 20 // past the anchor interval: delta rows
	for v := 0; v < versions; v++ {
		n := buildNode(nid, asofVersion{version: uint32(v), txFrom: types.Instant(10 * (v + 1)), txTo: types.Instant(10 * (v + 2))})
		if err := n.SetProperty("folder", folders[v%len(folders)]); err != nil {
			t.Fatal(err)
		}
		if err := n.SetProperty("n", int64(v)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNodeVersion(nid, uint32(v), n); err != nil {
			t.Fatalf("PutNodeVersion %d: %v", v, err)
		}
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(Config{Dir: dir, HistoryDeltaEncoding: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	for v := 0; v < versions; v++ {
		n, err := reopened.GetNodeVersion(nid, uint32(v))
		if err != nil {
			t.Fatalf("GetNodeVersion %d: %v", v, err)
		}
		if got, _ := n.GetProperty("folder"); got != folders[v%len(folders)] {
			t.Fatalf("version %d folder = %#v", v, got)
		}
		raw := vdRaw(t, reopened, storepkg.HistNodeKey(snowflake.ID(21), uint64(v)))
		if bytes.Contains(raw, []byte(vdFolder)) || bytes.Contains(raw, []byte(`C:\Program Files`)) {
			t.Fatalf("history row %d stores the value inline", v)
		}
	}
}

// TestValueDict_PreEncodedNodeBodyRecoded: a node body pre-encoded without
// the store's dictionaries (the ingest fast path) is re-encoded, so the row
// is coded. Catches a store that persists the body as given.
func TestValueDict_PreEncodedNodeBodyRecoded(t *testing.T) {
	t.Parallel()
	bs := vdOpen(t, t.TempDir(), vdDicts(100))
	defer func() { _ = bs.Close() }()
	n := vdNode(t, 21, vdFolder)
	body, err := storepkg.MarshalNodeWire(n)
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNodesBatchPreEncoded([]*types.Node{n}, [][]byte{body}); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if raw := vdRaw(t, bs, storepkg.NodeKey(snowflake.ID(21))); bytes.Contains(raw, []byte(vdFolder)) {
		t.Fatalf("pre-encoded body stored as given: %q", raw)
	}
}

// TestValueDict_ConcurrentWritersAndReaders: writers coding a shared pool of
// values, readers and flushes running at once; every row reads back its value
// before and after a reopen. Run under -race: catches a lock-free read racing
// an append and an entry that misses the flush of a row using it.
func TestValueDict_ConcurrentWritersAndReaders(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	bs := vdOpen(t, dir, vdDicts(1000))
	pool := make([]string, 40)
	for i := range pool {
		pool[i] = fmt.Sprintf("action_value_%02d", i)
	}
	const writers, each = 8, 200
	want := make(map[int64]string, writers*each)
	var mu sync.Mutex
	var wg sync.WaitGroup
	stop := make(chan struct{})
	go func() { // flushes while writing
		for {
			select {
			case <-stop:
				return
			default:
				_ = bs.Flush()
			}
		}
	}()
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < each; i++ {
				id := int64(1000 + w*each + i)
				v := pool[(w*7+i)%len(pool)]
				if err := bs.PutRelationship(vdRel(t, id, v)); err != nil {
					t.Errorf("PutRelationship(%d): %v", id, err)
					return
				}
				mu.Lock()
				want[id] = v
				mu.Unlock()
				if r, err := bs.GetRelationship(types.RelID(snowflake.ID(id))); err != nil {
					t.Errorf("GetRelationship(%d): %v", id, err)
				} else if got, _ := r.GetProperty("action"); got != v {
					t.Errorf("rel %d action = %#v, want %q", id, got, v)
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := New(Config{Dir: dir, CacheCapacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close() }()
	for id, v := range want {
		if got := vdRelAction(t, reopened, id); got != v {
			t.Fatalf("after reopen rel %d action = %#v, want %q", id, got, v)
		}
	}
}

// TestValueDictEntryHeap (VDICTDISK): a dictionary entry added while rows are
// written keeps one copy of its value and its map and slice slots, about 118 B
// for a 51 B value: neither the caller's string (a row's decode buffer) nor an
// interned handle (unique, ~214 B more) stays live.
func TestValueDictEntryHeap(t *testing.T) {
	const n = 100_000
	dir := t.TempDir()
	bs := vdOpen(t, dir, vdDicts(n))
	defer bs.Close()
	if err := bs.vdict.ensureLoaded(bs.db); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	d, _ := bs.vdict.plan.Index("folder")
	live := func() uint64 {
		var m runtime.MemStats
		runtime.GC()
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	before := live()
	for i := range n {
		row := fmt.Sprintf(`C:\Users\user%07d\AppData\Local\Temp\file-%07d.tmp%250s`, i, i, "the rest of the row")
		if _, ok := bs.vdict.token(bs, d, row[:51]); !ok { // the value shares the row's ~300 B buffer
			t.Fatal("no token")
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	per := float64(live()-before) / n
	t.Logf("%.0f B live per entry of a 51 B value", per)
	if per > 180 {
		t.Fatalf("%.0f B live per entry of a 51 B value, want at most 180", per)
	}
	// The stats' estimate is the measured heap within a fifth.
	s := bs.ValueDictionaryStats()[d]
	if est := float64(s.Bytes) / n; s.Entries != n || est < 0.8*per || est > 1.2*per {
		t.Fatalf("stats %+v: %.0f B an entry estimated, %.0f B measured; want %d entries, the estimate within a fifth", s, est, per, n)
	}
	// Reopened, the store counts no entry until it loads them, then all.
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	re, err := New(Config{Dir: dir, ValueDictionaries: vdDicts(n)})
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	if got := re.ValueDictionaryStats()[d]; got.Entries != 0 || got.Bytes != 0 {
		t.Fatalf("stats %+v before the load; want none resident", got)
	}
	if err := re.vdict.ensureLoaded(re.db); err != nil {
		t.Fatal(err)
	}
	if got := re.ValueDictionaryStats()[d]; got.Entries != s.Entries || got.Bytes != s.Bytes {
		t.Fatalf("stats %+v after the load; want %+v as before the close", got, s)
	}
}
