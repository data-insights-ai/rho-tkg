package badger

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// entityKeys returns the persisted 0x0C keys under prefix (after a flush).
func entityKeys(t *testing.T, bs *Store, prefix []byte) [][]byte {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	var keys [][]byte
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: prefix})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, it.Item().KeyCopy(nil))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return keys
}

// entityVersions counts the persisted versions of every 0x0C key (after a
// flush), deletes included.
func entityVersions(t *testing.T, bs *Store) int {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	n := 0
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{storepkg.KeyEntityIndex}, AllVersions: true})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			n++
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return n
}

// entityIDs renders the entity IDs of the 0x0C entries of kind under head.
func entityIDs(t *testing.T, bs *Store, kind byte, head []byte) string {
	t.Helper()
	var ids []int64
	for _, k := range entityKeys(t, bs, head) {
		_, id, _, _ := storepkg.EntityIndexEntryOf(k, kind)
		ids = append(ids, int64(id))
	}
	slices.Sort(ids)
	return fmt.Sprint(ids)
}

// TestRelPropertyIndexOnDisk_Lifecycle (RELDISK): a property index on a type
// listed in RelPropertyIndexOnDiskTypes keeps no entry in RAM, one current
// entry per relationship and value and one member entry per value a row
// carried (an update that keeps the value writes neither again, a history
// version adds its own), and answers lookups and membership from them. Range
// counts and elements are refused. The list may change between opens: an
// open without the type keeps the index in RAM and drops the marker, a
// read-only open with it keeps the unbuilt index in RAM, the next writable
// open rebuilds the entries (the stale ones dropped); a drop and Clear remove
// them. The first marker raises the format marker to 13.
func TestRelPropertyIndexOnDisk_Lifecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	names := registrypkg.NewRelTypeRegistry()
	r, _ := names.GetOrCreate("R")
	keys := registrypkg.NewPropertyKeyRegistry()
	open := func(readOnly bool, relTypes ...string) *Store {
		t.Helper()
		bs, err := New(Config{Dir: dir, PropertyKeyRegistry: keys, RelPropertyIndexOnDiskTypes: relTypes, RelTypeNames: names, ReadOnly: readOnly})
		if err != nil {
			t.Fatalf("open %v: %v", relTypes, err)
		}
		return bs
	}
	closeStore := func(bs *Store) {
		t.Helper()
		if !bs.readOnly {
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
	}
	rel := func(id int64, k string) *types.Relationship {
		x := types.NewRelationship(types.RelID(snowflake.ID(id)), r, 1, 2)
		if err := x.SetProperty("k", k); err != nil {
			t.Fatal(err)
		}
		return x
	}
	key := indexpkg.RelPropertyIndexKey{RelTypeToken: r, PropertyKey: "k"}
	tok, _ := keys.GetOrCreate("k")
	current := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelCurrent, r, tok)
	member := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelMember, r, tok)
	marker := storepkg.EntityIndexBuiltKey(storepkg.EntityIndexRelMember, r, "k")
	onDisk := func(bs *Store) bool {
		bs.idxMu.RLock()
		defer bs.idxMu.RUnlock()
		return bs.relPropertyIndexes[key].OnDisk()
	}
	lookup := func(bs *Store, k string) string {
		t.Helper()
		rs, err := bs.RelationshipsByTypeAndProperty(r, "k", k, QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for _, x := range rs {
			ids = append(ids, int64(x.ID()))
		}
		return fmt.Sprint(ids)
	}
	members := func(bs *Store, k string) string {
		t.Helper()
		var ids []int64
		if err := bs.ForEachRelPropertyTxMember(r, "k", types.IndexablePropertyValueKey(k), func(id types.RelID, _ types.Instant) bool {
			ids = append(ids, int64(id))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(ids)
		return fmt.Sprint(ids)
	}

	bs := open(false, "R")
	for _, opts := range []storecontract.PropertyIndexOptions{{RangeCounts: true}, {Elements: true}} {
		if err := bs.CreateRelPropertyIndexWithOptions(r, "k", opts); !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
			t.Fatalf("%+v on a listed type: %v, want ErrCapabilityNotSupported", opts, err)
		}
	}
	for id := int64(1); id <= 2; id++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.PutRelationship(rel(10, "x")); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil { // backfilled from rel 10
		t.Fatal(err)
	}
	stamped := func(x *types.Relationship, tx types.Instant) *types.Relationship {
		x.SetTemporal(&types.TemporalMetadata{TxFrom: tx, ValidFrom: tx})
		return x
	}
	for _, x := range []*types.Relationship{stamped(rel(11, "x"), 5), rel(12, "y"), rel(13, "z")} {
		if err := bs.PutRelationship(x); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.ReplaceRelationship(rel(10, "y")); err != nil { // x -> y
		t.Fatal(err)
	}
	// An unchanged value writes no entry again, unless the row lowers its
	// bound (5 -> 3, 3 -> 0: unknown is lowest).
	for _, tx := range []types.Instant{7, 3, 4, 0} {
		if err := bs.ReplaceRelationship(stamped(rel(11, "x"), tx)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.PutRelVersion(13, 0, rel(13, "w")); err != nil { // a history row of its own value
		t.Fatal(err)
	}
	if !onDisk(bs) {
		t.Fatal("R's index is in RAM, want on disk")
	}
	bs.idxMu.RLock()
	entries := len(bs.relPropertyIndexes[key].Entries)
	bs.idxMu.RUnlock()
	if entries != 0 {
		t.Fatalf("R's index holds %d values in RAM, want none", entries)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexRelCurrent, current); got != "[10 11 12 13]" {
		t.Fatalf("current entries of %s, want one per relationship", got)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexRelMember, member); got != "[10 10 11 11 11 12 13 13]" {
		t.Fatalf("member entries of %s, want one per value a row carried and bound it lowered (10: x, y; 11: x at 5, 3, 0; 13: z, w)", got)
	}
	if err := bs.ForEachRelPropertyTxMember(r, "k", types.IndexablePropertyValueKey("x"), func(id types.RelID, tx types.Instant) bool {
		if id == 11 && tx != 0 {
			t.Errorf("11 joined x at %d, want 0", tx)
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if got := lookup(bs, "y"); got != "[10 12]" {
		t.Fatalf("k=y: %s, want [10 12]", got)
	}
	if got := members(bs, "x"); got != "[10 11]" {
		t.Fatalf("members of x: %s, want [10 11] (10 carried it)", got)
	}
	if got := members(bs, "w"); got != "[13]" {
		t.Fatalf("members of w: %s, want [13] (a history row)", got)
	}
	if st, _ := bs.PropertyTxMembershipStats(); st.RelSidecars != 0 || st.RelPostings != 0 {
		t.Fatalf("%d rel sidecars, %d postings; want none", st.RelSidecars, st.RelPostings)
	}
	// An unchanged update of a persisted row (both unstamped) writes neither
	// entry again: no new badger version.
	before := entityVersions(t, bs)
	if err := bs.ReplaceRelationship(rel(12, "y")); err != nil {
		t.Fatal(err)
	}
	if after := entityVersions(t, bs); after != before {
		t.Fatalf("an unchanged update wrote %d entry versions, want none", after-before)
	}
	// A with-history door whose demoted row differs from the current one
	// writes the demoted row's member entries.
	if err := bs.ReplaceRelWithHistory(rel(12, "y"), 0, rel(12, "p")); err != nil {
		t.Fatal(err)
	}
	if got := members(bs, "p"); got != "[12]" {
		t.Fatalf("members of p: %s, want [12] (the demoted row)", got)
	}

	// Every rel door refuses a value whose member entry passes the key limit,
	// before any mutation.
	long := strings.Repeat("q", bs.maxEntryKey)
	big := rel(20, long)
	for door, err := range map[string]error{
		"PutRelationship":        bs.PutRelationship(big),
		"PutRelationshipsBatch":  bs.PutRelationshipsBatch([]*types.Relationship{rel(21, "ok"), rel(22, long)}),
		"PutRelEntityAndOut":     bs.PutRelEntityAndOut(big),
		"ReplaceRelationship":    bs.ReplaceRelationship(rel(12, long)),
		"ReplaceRelWithHistory":  bs.ReplaceRelWithHistory(rel(12, long), 0, rel(12, "y")),
		"ReplaceRelWithHistory2": bs.ReplaceRelWithHistory(rel(12, "y"), 0, rel(12, long)),
		"PutRelVersion":          bs.PutRelVersion(12, 0, rel(12, long)),
	} {
		if !errors.Is(err, ErrEntryTooLarge) {
			t.Errorf("%s with an oversized key: %v, want ErrEntryTooLarge", door, err)
		}
	}
	for _, id := range []types.RelID{20, 21, 22} {
		if _, err := bs.GetRelationship(id); err == nil {
			t.Errorf("relationship %d written by a refused door", id)
		}
	}
	if got := lookup(bs, "y"); got != "[10 12]" {
		t.Fatalf("k=y after the refused doors: %s, want [10 12]", got)
	}
	// The batch and the partial (sharded) doors write and delete the entries.
	if err := bs.PutRelationshipsBatch([]*types.Relationship{rel(14, "y")}); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelEntityAndOut(rel(15, "y")); err != nil {
		t.Fatal(err)
	}
	if got := lookup(bs, "y"); got != "[10 12 14 15]" {
		t.Fatalf("k=y after the batch and partial doors: %s, want [10 12 14 15]", got)
	}
	if _, err := bs.DeleteRelEntityAndOut(15); err != nil {
		t.Fatal(err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexRelCurrent, current); got != "[10 11 12 13 14]" {
		t.Fatalf("current entries of %s after the partial delete, want [10 11 12 13 14]", got)
	}
	if err := bs.DeleteRelationship(14); err != nil {
		t.Fatal(err)
	}
	closeStore(bs)
	if got, ok := readRawMarker(t, dir); !ok || got != storepkg.EntityIndexStoreFormatVersion {
		t.Fatalf("format marker %d (present %v), want %d", got, ok, storepkg.EntityIndexStoreFormatVersion)
	}

	bs = open(false, "R") // trusted: nothing rebuilt
	if !onDisk(bs) || members(bs, "x") != "[10 11]" || lookup(bs, "y") != "[10 12]" {
		t.Fatalf("reopened: on disk %v, members of x %s, k=y %s", onDisk(bs), members(bs, "x"), lookup(bs, "y"))
	}
	closeStore(bs)

	bs = open(false) // R leaves: in RAM, its marker goes
	if onDisk(bs) || badgerHasKey(bs, marker) {
		t.Fatalf("R left: on disk %v, marker %v; want neither", onDisk(bs), badgerHasKey(bs, marker))
	}
	if err := bs.ReplaceRelationship(rel(13, "x")); err != nil { // z -> x, not in the entries
		t.Fatal(err)
	}
	if err := bs.DeleteRelationship(11); err != nil {
		t.Fatal(err)
	}
	members(bs, "x") // builds R's sidecar in RAM, which the clean close writes into the open image
	closeStore(bs)

	bs = open(true, "R") // read-only over an unbuilt index: in RAM
	if onDisk(bs) || lookup(bs, "x") != "[13]" {
		t.Fatalf("read-only: on disk %v, k=x %s; want RAM, [13]", onDisk(bs), lookup(bs, "x"))
	}
	closeStore(bs)

	bs = open(false, "R") // rebuilt, the stale entries dropped
	if !onDisk(bs) || !badgerHasKey(bs, marker) {
		t.Fatalf("R rejoined: on disk %v, marker %v; want both", onDisk(bs), badgerHasKey(bs, marker))
	}
	if st, _ := bs.PropertyTxMembershipStats(); st.RelSidecars != 0 || st.RelPostings != 0 {
		t.Fatalf("R rejoined: %d rel sidecars, %d postings from the open image; want none", st.RelSidecars, st.RelPostings)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexRelCurrent, current); got != "[10 12 13]" {
		t.Fatalf("current entries of %s after the rebuild, want [10 12 13]", got)
	}
	if got := lookup(bs, "x"); got != "[13]" {
		t.Fatalf("k=x after the rebuild: %s, want [13]", got)
	}
	if got := members(bs, "x"); got != "[13]" {
		t.Fatalf("members of x after the rebuild: %s, want [13] (a rebuild reads the rows: 10 carried x in place, 11 is deleted)", got)
	}
	closeStore(bs)

	bs = open(false) // a drop in RAM leaves the 0x0C entries
	if err := bs.DropRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.ReplaceRelationship(rel(13, "z")); err != nil { // x -> z, not in the entries
		t.Fatal(err)
	}
	closeStore(bs)
	bs = open(false, "R") // a create drops them first
	if n := len(entityKeys(t, bs, []byte{storepkg.KeyEntityIndex})); n == 0 {
		t.Fatal("no entries left by the drop in RAM")
	}
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexRelCurrent, current); got != "[10 12 13]" {
		t.Fatalf("current entries of %s after the create, want [10 12 13]", got)
	}
	if got := members(bs, "x"); got != "[]" {
		t.Fatalf("members of x after the create: %s, want none (13 carries z)", got)
	}
	if err := bs.DropRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelationship(rel(30, "x")); err != nil { // no definition: no entry
		t.Fatal(err)
	}
	if n := len(entityKeys(t, bs, []byte{storepkg.KeyEntityIndex})); n != 0 || badgerHasKey(bs, marker) {
		t.Fatalf("after the drop: %d entries, marker %v; want none", n, badgerHasKey(bs, marker))
	}
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	if n := len(entityKeys(t, bs, []byte{storepkg.KeyEntityIndex})); n == 0 {
		t.Fatal("no entries after the re-create")
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	if n := len(entityKeys(t, bs, []byte{storepkg.KeyEntityIndex})); n != 0 {
		t.Fatalf("after Clear: %d entries, want none", n)
	}
	for id := int64(1); id <= 2; id++ { // Clear drops the definitions too: a write makes no entry
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.PutRelationship(rel(31, "x")); err != nil {
		t.Fatal(err)
	}
	if n := len(entityKeys(t, bs, []byte{storepkg.KeyEntityIndex})); n != 0 {
		t.Fatalf("a write after Clear made %d entries, want none", n)
	}
	closeStore(bs)
}

// TestEntityIndex_PurgeAndErasureRemoveEntries (RELDISK): a retention purge
// and an exact erasure delete every 0x0C entry of the entities they remove,
// history included (a member entry holds a value verbatim), and leave the
// others'.
func TestEntityIndex_PurgeAndErasureRemoveEntries(t *testing.T) {
	t.Parallel()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	keys := registrypkg.NewPropertyKeyRegistry()
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: keys, PropertyIndexOnDiskLabels: []string{"D"}, LabelNames: labels,
		RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	if err := bs.CreatePropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	node := func(id int64, k string) *types.Node {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), d, nil)
		_ = n.SetProperty("k", k)
		return n
	}
	rel := func(id, from, to int64, k string) *types.Relationship {
		x := types.NewRelationship(types.RelID(snowflake.ID(id)), r, types.NodeID(snowflake.ID(from)), types.NodeID(snowflake.ID(to)))
		_ = x.SetProperty("k", k)
		return x
	}
	for id := int64(1); id <= 4; id++ {
		if err := bs.PutNode(node(id, fmt.Sprint("v", id))); err != nil {
			t.Fatal(err)
		}
		if id == 2 {
			// A with-history door whose demoted row differs from the current
			// one writes the demoted row's member entry.
			current := node(2, "v2")
			current.SetVersion(1)
			if err := bs.ReplaceNodeWithHistory(current, 0, node(2, "old2")); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := bs.PutNodeVersion(types.NodeID(snowflake.ID(id)), 0, node(id, fmt.Sprint("old", id))); err != nil {
			t.Fatal(err)
		}
	}
	for _, x := range []*types.Relationship{rel(10, 1, 2, "a"), rel(11, 3, 4, "b"), rel(12, 2, 3, "c"), rel(14, 2, 3, "e")} {
		if err := bs.PutRelationship(x); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutRelVersion(x.ID(), 0, rel(int64(x.ID()), int64(x.StartNodeID()), int64(x.EndNodeID()), "h"+fmt.Sprint(x.ID()))); err != nil {
			t.Fatal(err)
		}
	}
	tok, _ := keys.GetOrCreate("k")
	nodeHead := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexNodeMember, d, tok)
	relHead := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelMember, r, tok)
	curHead := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelCurrent, r, tok)
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeMember, nodeHead); got != "[1 1 2 2 3 3 4 4]" {
		t.Fatalf("node member entries %s, want two per node", got)
	}

	// A relationship purged by its info: its entries go, history included.
	if err := bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 12, TypeToken: r, StartID: 2, EndID: 3}); err != nil {
		t.Fatal(err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexRelMember, relHead); got != "[10 10 11 11 14 14]" {
		t.Fatalf("rel member entries %s after purging 12", got)
	}
	// An exact erasure of node 1 and relationship 10, 10's new value written
	// after the erasure's preliminary flush (so still buffered for its plan).
	bs.exactErasureFlushedTestHook = func() {
		if err := bs.ReplaceRelationship(rel(10, 1, 2, "late")); err != nil {
			t.Error(err)
		}
	}
	if _, err := bs.ExactErase(storecontract.ExactErasureRequest{NodeIDs: []types.NodeID{1}, RelIDs: []types.RelID{10}, Bounds: badgerExactErasureBounds}); err != nil {
		t.Fatal(err)
	}
	bs.exactErasureFlushedTestHook = nil
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeMember, nodeHead); got != "[2 2 3 3 4 4]" {
		t.Fatalf("node member entries %s after erasing 1", got)
	}
	// A cross-shard sweep of node 2's relationships (14).
	if n, err := bs.PurgeAdjacentRelsForNode(2); err != nil || n != 1 {
		t.Fatalf("PurgeAdjacentRelsForNode(2): %d, %v; want 1", n, err)
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexRelMember, relHead) + entityIDs(t, bs, storepkg.EntityIndexRelCurrent, curHead); got != "[11 11][11]" {
		t.Fatalf("rel entries %s after erasing 10 and sweeping 14, want only 11's", got)
	}
	for _, k := range entityKeys(t, bs, []byte{storepkg.KeyEntityIndex}) {
		if bytes.Contains(k, []byte("old1")) || bytes.Contains(k, []byte("s:a")) || bytes.Contains(k, []byte("late")) {
			t.Fatalf("an erased value survives in %q", k)
		}
	}
	// A retention purge of every D node: their entries and their
	// relationships' go.
	if _, err := bs.PurgeNodesByLabelBefore(d, types.Instant(1<<62), 0); err != nil {
		t.Fatal(err)
	}
	if n := len(entityKeys(t, bs, []byte{storepkg.KeyEntityIndex})); n != 0 {
		t.Fatalf("%d entries after purging every node, want none", n)
	}
}

// TestEntityIndex_ExactValueWalk (RELDISK): an equality lookup visits the
// entries of its value only, on disk and in the write buffer: not those of a
// value it is a byte prefix of ("a" of "a\x00b", "ab"), whatever IDs they
// carry. Without the 0x00 escape the entries of "a\x00b" would sort between
// those of "a" with a low ID and a high one, and the walk would stop early.
func TestEntityIndex_ExactValueWalk(t *testing.T) {
	t.Parallel()
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 2; id++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	const high = int64(0x70) << 56
	values := map[string][]int64{"a": {1, high + 1}, "a\x00b": {2, high + 2}, "ab": {3, high + 3}, "a\x00": {4, high + 4}}
	put := func(base int64) {
		t.Helper()
		for v, ids := range values {
			for _, id := range ids {
				x := types.NewRelationship(types.RelID(snowflake.ID(id+base)), r, 1, 2)
				_ = x.SetProperty("k", v)
				if err := bs.PutRelationship(x); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	check := func(stage string, bases ...int64) {
		t.Helper()
		for v, ids := range values {
			var want []int64
			for _, base := range bases {
				for _, id := range ids {
					want = append(want, id+base)
				}
			}
			slices.Sort(want)
			rs, err := bs.RelationshipsByTypeAndProperty(r, "k", v, QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			var got []int64
			for _, x := range rs {
				got = append(got, int64(x.ID()))
			}
			slices.Sort(got)
			var members []int64
			if err := bs.ForEachRelPropertyTxMember(r, "k", types.IndexablePropertyValueKey(v), func(id types.RelID, _ types.Instant) bool {
				members = append(members, int64(id))
				return true
			}); err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(got) != fmt.Sprint(want) || fmt.Sprint(members) != fmt.Sprint(want) {
				t.Errorf("%s, k=%q: lookup %v, members %v; want %v", stage, v, got, members, want)
			}
		}
	}
	put(0)
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	check("on disk", 0)
	put(100) // buffered over the persisted ones
	check("buffered", 0, 100)

	// Purging relationship 1 (k=a) deletes its persisted entries: a delete in
	// the buffer, then a persisted one over an older version, hides them from
	// the equality and the any-value walks.
	if err := bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 1, TypeToken: r, StartID: 1, EndID: 2}); err != nil {
		t.Fatal(err)
	}
	for _, stage := range []string{"purge buffered", "purge persisted"} {
		if stage == "purge persisted" {
			if err := bs.Flush(); err != nil {
				t.Fatal(err)
			}
		}
		seen := map[string]bool{}
		if err := bs.ForEachRelPropertyTxMember(r, "k", types.IndexablePropertyValueKey("a"), func(id types.RelID, _ types.Instant) bool {
			seen[fmt.Sprint("a", id)] = true
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if err := bs.ForEachRelPropertyTxMemberAny(r, "k", func(id types.RelID, _ types.Instant) bool {
			seen[fmt.Sprint("any", id)] = true
			return true
		}); err != nil {
			t.Fatal(err)
		}
		if seen["a1"] || seen["any1"] || !seen[fmt.Sprint("a", high+1)] || !seen[fmt.Sprint("any", high+1)] {
			t.Errorf("%s: members of a hold 1 %v, %d %v; any value 1 %v, %d %v; want only the other", stage,
				seen["a1"], high+1, seen[fmt.Sprint("a", high+1)], seen["any1"], high+1, seen[fmt.Sprint("any", high+1)])
		}
	}
}

// TestEntityIndex_PurgeUnreadableRow (RELDISK): a retention purge of a
// relationship whose current row cannot be read deletes its entries all the
// same, current and member, found by walking the definitions.
func TestEntityIndex_PurgeUnreadableRow(t *testing.T) {
	t.Parallel()
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 2; id++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	x := types.NewRelationship(10, r, 1, 2)
	_ = x.SetProperty("k", "x")
	if err := bs.PutRelationship(x); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(storepkg.RelKey(10), []byte{0xFF, 0x00}) }); err != nil {
		t.Fatal(err)
	}
	bs.relCache.EvictForTest(10)
	if _, err := bs.GetRelationship(10); err == nil {
		t.Fatal("the overwritten row still reads")
	}
	if err := bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 10, TypeToken: r, StartID: 1, EndID: 2}); err != nil {
		t.Fatal(err)
	}
	if keys := entityKeys(t, bs, []byte{storepkg.KeyEntityIndex}); len(keys) != 0 {
		t.Fatalf("%d entries of the purged unreadable row remain, want none", len(keys))
	}
}

// TestRelPropertyIndexOnDisk_NeedsRegistry (RELDISK): without a property-key
// registry an index on a listed type cannot key its entries: the create is
// refused, not an index that answers nothing.
func TestRelPropertyIndexOnDisk_NeedsRegistry(t *testing.T) {
	t.Parallel()
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	bs, err := New(Config{InMemory: true, RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	if err := bs.CreateRelPropertyIndex(r, "k"); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("create without a registry: %v, want ErrInvalidStoreMutation", err)
	}
}

// TestEntityIndex_CreateRacesWrites (RELDISK): writes that land while a
// create scans without a lock (here between the scan's buffer capture and its
// badger view, which therefore misses them) leave exactly the entries of the
// rows as they end: a relationship whose value changed keeps no current entry
// of the old one, a deleted one none, and one rewritten unchanged keeps its
// own (its door writes it again: the update dedupe stays off during a
// create). Membership reads refuse while a definition is being built.
func TestEntityIndex_CreateRacesWrites(t *testing.T) {
	t.Parallel()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	keys := registrypkg.NewPropertyKeyRegistry()
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: keys, PropertyIndexOnDiskLabels: []string{"D"}, LabelNames: labels,
		RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	rel := func(id int64, k string) *types.Relationship {
		x := types.NewRelationship(types.RelID(snowflake.ID(id)), r, 1, 2)
		_ = x.SetProperty("k", k)
		return x
	}
	o, _ := labels.GetOrCreate("O")
	for id := int64(1); id <= 4; id++ {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), d, nil)
		if id == 3 {
			n = types.NewNode(3, o, []uint16{d}) // D as an extra label
		}
		n.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 1}) // the validity purge reads every D node's
		if id == 4 {
			n.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 1, ValidTo: 10}) // closed: purged by validity below
		}
		_ = n.SetProperty("k", "x")
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{10, 11, 13, 14} {
		if err := bs.PutRelationship(rel(id, "x")); err != nil {
			t.Fatal(err)
		}
	}
	vx := types.IndexablePropertyValueKey("x")
	var hooked int
	bs.propTxScanTestHook = func() {
		hooked++
		if hooked != 1 {
			return
		}
		if err := bs.ForEachRelPropertyTxMember(r, "k", vx, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, ErrIndexNotFound) {
			t.Errorf("rel members while built: %v, want ErrIndexNotFound", err)
		}
		if rs, err := bs.RelationshipsByTypeAndProperty(r, "k", "x", QueryOpts{}); err != nil || len(rs) != 4 {
			t.Errorf("k=x while built: %d relationships, %v; want 4 (a scan, not the partial entries)", len(rs), err)
		}
		if err := bs.PurgeRelationshipByInfo(storecontract.PurgedRel{ID: 14, TypeToken: r, StartID: 1, EndID: 2}); err != nil {
			t.Error(err)
		}
		if err := bs.ReplaceRelationship(rel(10, "y")); err != nil {
			t.Error(err)
		}
		if err := bs.DeleteRelationship(11); err != nil {
			t.Error(err)
		}
		if err := bs.ReplaceRelationship(rel(13, "x")); err != nil {
			t.Error(err)
		}
	}
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	if hooked == 0 {
		t.Fatal("the create's scan never ran the hook")
	}
	tok, _ := keys.GetOrCreate("k")
	current := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelCurrent, r, tok)
	if got := entityIDs(t, bs, storepkg.EntityIndexRelCurrent, current); got != "[10 13]" {
		t.Fatalf("current entries of %s, want [10 13] (one each, of the value it ends with)", got)
	}
	member := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexRelMember, r, tok)
	if got := entityIDs(t, bs, storepkg.EntityIndexRelMember, member); strings.Contains(got, "14") {
		t.Fatalf("member entries of %s: 14, purged during the scan, kept one (the scan is redone)", got)
	}
	for k, want := range map[string]string{"x": "[13]", "y": "[10]"} {
		rs, err := bs.RelationshipsByTypeAndProperty(r, "k", k, QueryOpts{})
		if err != nil {
			t.Fatal(err)
		}
		var ids []int64
		for _, x := range rs {
			ids = append(ids, int64(x.ID()))
		}
		if fmt.Sprint(ids) != want {
			t.Fatalf("k=%s: %v, want %s", k, ids, want)
		}
	}

	// Node 4's row back in the write buffer, so the member scan reads it from
	// there although the purge below removes it (and flushes) first.
	n4 := types.NewNode(4, d, nil)
	n4.SetTemporal(&types.TemporalMetadata{TxFrom: 1, ValidFrom: 1, ValidTo: 10})
	_ = n4.SetProperty("k", "x")
	if err := bs.ReplaceNode(n4); err != nil {
		t.Fatal(err)
	}
	hooked = 0
	bs.propTxScanTestHook = func() {
		hooked++
		if hooked != 1 {
			return
		}
		if err := bs.ForEachNodePropertyTxMember(d, "k", vx, func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, ErrIndexNotFound) {
			t.Errorf("node members while built: %v, want ErrIndexNotFound", err)
		}
		// A purge in the write buffer, which the scan's badger view misses.
		if res, err := bs.PurgeNodesByLabelValidToBefore(d, 100, 0); err != nil || res.NodesPurged != 1 {
			t.Errorf("purge of 4 during the scan: %+v, %v; want 1 purged", res, err)
		}
	}
	if err := bs.CreatePropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	if hooked == 0 {
		t.Fatal("the node create's scan never ran the hook")
	}
	nodeHead := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexNodeMember, d, tok)
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeMember, nodeHead); got != "[1 2 3]" {
		t.Fatalf("node member entries of %s, want [1 2 3] (3 carries D as an extra label, 4 purged during the scan)", got)
	}
	bs.propTxScanTestHook = nil
}

// TestEntityIndex_RangeMembersMatchSidecar (RELDISK): the numeric range
// membership door answers from disk what the RAM sidecar answers: the values
// whose magnitude lies in [lo, hi] exactly (none one ulp outside), -0 a bound
// equal to 0, and the NaN values, only those when !(lo <= hi) (a NaN bound or
// an empty range). Type R keeps its index on disk, S the same rows in RAM.
func TestEntityIndex_RangeMembersMatchSidecar(t *testing.T) {
	t.Parallel()
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	s, _ := relTypes.GetOrCreate("S")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(),
		RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	for id := int64(1); id <= 2; id++ {
		if err := bs.PutNode(types.NewNode(types.NodeID(snowflake.ID(id)), 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	for _, tok := range []uint16{r, s} {
		if err := bs.CreateRelPropertyIndex(tok, "n"); err != nil {
			t.Fatal(err)
		}
	}
	values := []any{-5.0, 0.0, math.Nextafter(1, 0), 1.0, int64(2), math.Nextafter(2, 3), 3.5, math.Inf(1), math.NaN()}
	for i, v := range values {
		for j, tok := range []uint16{r, s} {
			x := types.NewRelationship(types.RelID(snowflake.ID(int64(10+i)+int64(j)*100)), tok, 1, 2)
			if err := x.SetProperty("n", v); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutRelationship(x); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	inRange := func(tok uint16, lo, hi float64, base int64) string {
		t.Helper()
		var ids []int64
		if err := bs.ForEachRelPropertyTxMemberInRange(tok, "n", lo, hi, func(id types.RelID, _ types.Instant) bool {
			ids = append(ids, int64(id)-base)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(ids)
		return fmt.Sprint(ids)
	}
	nan := math.NaN()
	for _, b := range [][2]float64{{1, 2}, {2, 2}, {math.Copysign(0, -1), math.Copysign(0, -1)}, {-10, math.Copysign(0, -1)},
		{1, nan}, {nan, 2}, {nan, nan}, {3, 1}, {math.Inf(-1), math.Inf(1)}, {3.5, math.Inf(1)}} {
		disk, ram := inRange(r, b[0], b[1], 0), inRange(s, b[0], b[1], 100)
		if disk != ram {
			t.Errorf("[%v, %v]: disk %s, RAM sidecar %s", b[0], b[1], disk, ram)
		}
	}
	if st, _ := bs.PropertyTxMembershipStats(); st.RelSidecars != 1 {
		t.Fatalf("%d rel sidecars, want 1: S's build claims no definition kept on disk", st.RelSidecars)
	}
}

// TestNodeMemberEntries_Lifecycle (RELDISK): a node index kept on disk keeps
// its member entries in 0x0C: an update that keeps a value writes no entry
// again unless its row lowers the bound, the create sets a built marker and
// drops the entries an earlier definition left, a rejoin builds no sidecar
// from the open image, a drop deletes the entries and the marker.
func TestNodeMemberEntries_Lifecycle(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	keys := registrypkg.NewPropertyKeyRegistry()
	tok, _ := keys.GetOrCreate("k")
	open := func(onDisk ...string) *Store {
		t.Helper()
		bs, err := New(Config{Dir: dir, PropertyKeyRegistry: keys, PropertyIndexOnDiskLabels: onDisk, LabelNames: labels})
		if err != nil {
			t.Fatal(err)
		}
		return bs
	}
	closeStore := func(bs *Store) {
		t.Helper()
		if err := bs.Flush(); err != nil {
			t.Fatal(err)
		}
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
	}
	node := func(id int64, k string, tx types.Instant) *types.Node {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), d, nil)
		_ = n.SetProperty("k", k)
		if tx != 0 {
			n.SetTemporal(&types.TemporalMetadata{TxFrom: tx, ValidFrom: tx})
		}
		return n
	}
	head := storepkg.AppendEntityIndexHead(nil, storepkg.EntityIndexNodeMember, d, tok)
	marker := storepkg.EntityIndexBuiltKey(storepkg.EntityIndexNodeMember, d, "k")
	members := func(bs *Store, k string) string {
		t.Helper()
		var ids []int64
		if err := bs.ForEachNodePropertyTxMember(d, "k", types.IndexablePropertyValueKey(k), func(id types.NodeID, _ types.Instant) bool {
			ids = append(ids, int64(id))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		slices.Sort(ids)
		return fmt.Sprint(ids)
	}

	bs := open("D")
	if err := bs.CreatePropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if !badgerHasKey(bs, marker) {
		t.Fatal("no built marker after the create")
	}
	for _, n := range []*types.Node{node(5, "x", 5), node(6, "y", 0)} {
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	for _, tx := range []types.Instant{7, 3, 4} { // 5 -> 3 lowers the bound
		if err := bs.ReplaceNode(node(5, "x", tx)); err != nil {
			t.Fatal(err)
		}
	}
	if got := entityIDs(t, bs, storepkg.EntityIndexNodeMember, head); got != "[5 5 6]" {
		t.Fatalf("member entries of %s, want [5 5 6] (5: x at 5 and 3)", got)
	}
	before := entityVersions(t, bs)
	if err := bs.ReplaceNode(node(6, "y", 0)); err != nil { // unchanged, unstamped
		t.Fatal(err)
	}
	if after := entityVersions(t, bs); after != before {
		t.Fatalf("an unchanged update wrote %d entry versions, want none", after-before)
	}
	closeStore(bs)

	bs = open() // D leaves: in RAM; a pinned read builds its sidecar, the clean close images it
	members(bs, "x")
	closeStore(bs)
	bs = open("D") // rejoins: rebuilt on disk, no sidecar from the image
	if st, _ := bs.PropertyTxMembershipStats(); st.NodeSidecars != 0 || st.NodePostings != 0 {
		t.Fatalf("D rejoined: %d node sidecars, %d postings from the open image; want none", st.NodeSidecars, st.NodePostings)
	}
	closeStore(bs)

	bs = open() // a drop in RAM leaves the 0x0C entries
	if err := bs.ReplaceNode(node(6, "z", 0)); err != nil {
		t.Fatal(err)
	}
	if err := bs.DropPropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	closeStore(bs)
	bs = open("D") // a create drops them first
	if n := len(entityKeys(t, bs, head)); n == 0 {
		t.Fatal("no entries left by the drop in RAM")
	}
	if err := bs.CreatePropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	if got := members(bs, "y"); got != "[]" {
		t.Fatalf("members of y after the create: %s, want none (6 carries z, no history)", got)
	}
	if err := bs.DropPropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	if n := len(entityKeys(t, bs, head)); n != 0 || badgerHasKey(bs, marker) {
		t.Fatalf("after the drop: %d entries, marker %v; want none", n, badgerHasKey(bs, marker))
	}
	closeStore(bs)
}

// TestEntityIndex_KeyLimitBoundary (RELDISK): the key-size check measures the
// member entry, the longest, with its escaped 0x00 bytes: a value whose entry
// is one byte over the limit is refused at every door that writes one (a
// demoted node row included), one at the limit is written.
func TestEntityIndex_KeyLimitBoundary(t *testing.T) {
	t.Parallel()
	labels := registrypkg.NewLabelRegistry()
	d, _ := labels.GetOrCreate("D")
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), PropertyIndexOnDiskLabels: []string{"D"},
		LabelNames: labels, RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = bs.Close() }()
	if err := bs.CreatePropertyIndex(d, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndex(r, "k"); err != nil {
		t.Fatal(err)
	}
	// A value of one 0x00 byte and q's: its member key is the head (6), the
	// domain tag (1), "s:" (2), the value, the 0x00's escape (1), the
	// terminator (1) and the tail (16).
	value := func(keyLen int) string { return "\x00" + strings.Repeat("q", keyLen-6-1-2-1-1-1-16) }
	if l := storepkg.EntityIndexHeadLen + storepkg.EntityIndexValueLen(types.IndexablePropertyValueKey(value(100))) + 16; l != 100 {
		t.Fatalf("the test's value makes a %d-byte key, want 100", l)
	}
	over, at := value(bs.maxEntryKey+1), value(bs.maxEntryKey)
	node := func(id int64, k string) *types.Node {
		n := types.NewNode(types.NodeID(snowflake.ID(id)), d, nil)
		_ = n.SetProperty("k", k)
		return n
	}
	rel := func(id int64, k string) *types.Relationship {
		x := types.NewRelationship(types.RelID(snowflake.ID(id)), r, 1, 2)
		_ = x.SetProperty("k", k)
		return x
	}
	if err := bs.PutNode(node(1, "a")); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutNode(node(2, "a")); err != nil {
		t.Fatal(err)
	}
	current := node(1, "a")
	current.SetVersion(1)
	for door, err := range map[string]error{
		"PutNode":                bs.PutNode(node(3, over)),
		"PutNodeVersion":         bs.PutNodeVersion(1, 0, node(1, over)),
		"ReplaceNodeWithHistory": bs.ReplaceNodeWithHistory(current, 0, node(1, over)),
		"PutRelationship":        bs.PutRelationship(rel(10, over)),
	} {
		if !errors.Is(err, ErrEntryTooLarge) {
			t.Errorf("%s one byte over the limit: %v, want ErrEntryTooLarge", door, err)
		}
	}
	if err := bs.PutNode(node(3, at)); err != nil {
		t.Errorf("PutNode at the limit: %v", err)
	}
	if err := bs.PutRelationship(rel(10, at)); err != nil {
		t.Errorf("PutRelationship at the limit: %v", err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
}

// TestRelPropertyIndexOnDisk_ElementsRefusedAtOpen (RELDISK): an Elements
// definition made while its type was unlisted fails the open that lists the
// type: its entries are lists, which 0x0C does not key.
func TestRelPropertyIndexOnDisk_ElementsRefusedAtOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	relTypes := registrypkg.NewRelTypeRegistry()
	r, _ := relTypes.GetOrCreate("R")
	keys := registrypkg.NewPropertyKeyRegistry()
	bs, err := New(Config{Dir: dir, PropertyKeyRegistry: keys, RelTypeNames: relTypes})
	if err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndexWithOptions(r, "k", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(Config{Dir: dir, PropertyKeyRegistry: keys, RelPropertyIndexOnDiskTypes: []string{"R"}, RelTypeNames: relTypes})
	if !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
		if err == nil {
			_ = bs.Close()
		}
		t.Fatalf("open listing an Elements index's type: %v, want ErrCapabilityNotSupported", err)
	}
}
