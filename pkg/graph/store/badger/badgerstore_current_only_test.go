package badger

import (
	"errors"
	"fmt"
	"slices"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// currentOnlyFixture is the fixed fixture of PropertyIndexCurrentOnlyLabels
// (CURRENTONLY): label D on disk with three indexed keys (ns, rel, k), 100
// nodes, each written once and replaced once with its history row, as a page
// node of Sigma's carries its identity keys. dir "" opens in memory.
type currentOnlyFixture struct {
	labels *registrypkg.LabelRegistry
	keys   *registrypkg.PropertyKeyRegistry
	d      uint16
}

func newCurrentOnlyFixture() *currentOnlyFixture {
	f := &currentOnlyFixture{labels: registrypkg.NewLabelRegistry(), keys: registrypkg.NewPropertyKeyRegistry()}
	f.d, _ = f.labels.GetOrCreate("D")
	return f
}

func (f *currentOnlyFixture) open(t *testing.T, dir string, currentOnly bool) *Store {
	t.Helper()
	cfg := Config{Dir: dir, InMemory: dir == "", PropertyKeyRegistry: f.keys, PropertyIndexOnDiskLabels: []string{"D"}, LabelNames: f.labels}
	if currentOnly {
		cfg.PropertyIndexCurrentOnlyLabels = []string{"D"}
	}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return bs
}

func (f *currentOnlyFixture) node(id int64, version int) *types.Node {
	n := types.NewNode(types.NodeID(snowflake.ID(id)), f.d, nil)
	_ = n.SetProperty("ns", "n")
	_ = n.SetProperty("rel", fmt.Sprint("r", id%4))
	_ = n.SetProperty("k", fmt.Sprint("v", id))
	n.SetVersion(uint32(version)) // #nosec G115 -- 0 or 1
	return n
}

func (f *currentOnlyFixture) fill(t *testing.T, bs *Store) {
	t.Helper()
	for _, p := range []string{"ns", "rel", "k"} {
		if err := bs.CreatePropertyIndex(f.d, p); err != nil {
			t.Fatal(err)
		}
	}
	for id := int64(1); id <= 100; id++ {
		if err := bs.PutNode(f.node(id, 0)); err != nil {
			t.Fatal(err)
		}
		if err := bs.ReplaceNodeWithHistory(f.node(id, 1), 0, f.node(id, 0)); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
}

// indexEntries counts the persisted 0x0A entries and the 0x0C member entries
// (span and time kinds) after a flush.
func indexEntries(t *testing.T, bs *Store) (current, members int) {
	t.Helper()
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			k := it.Item().Key()
			switch {
			case k[0] == storepkg.KeyPropertyIndex:
				current++
			case k[0] == storepkg.KeyEntityIndex && (k[1] == storepkg.EntityIndexNodeSpan || k[1] == storepkg.EntityIndexNodeTime || k[1] == storepkg.EntityIndexNodeMember):
				members++
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return current, members
}

// currentAnswers is every current lookup of the fixture: per key and value,
// the IDs found.
func (f *currentOnlyFixture) currentAnswers(t *testing.T, bs *Store) map[string][]types.NodeID {
	t.Helper()
	out := map[string][]types.NodeID{}
	probe := func(key string, value any) {
		ns, err := bs.NodesByLabelAndProperty(f.d, key, value, storecontract.QueryOpts{})
		if err != nil {
			t.Fatalf("%s=%v: %v", key, value, err)
		}
		ids := make([]types.NodeID, 0, len(ns))
		for _, n := range ns {
			ids = append(ids, n.ID())
		}
		slices.Sort(ids)
		out[fmt.Sprint(key, "=", value)] = ids
	}
	probe("ns", "n")
	for r := range 4 {
		probe("rel", fmt.Sprint("r", r))
	}
	for id := 1; id <= 101; id++ {
		probe("k", fmt.Sprint("v", id))
	}
	return out
}

// TestCurrentOnlyIndexWritesFewerEntries: a label in
// PropertyIndexCurrentOnlyLabels keeps its 0x0A entries and writes no 0x0C
// member entry; on the fixture its index entries are at least 60% fewer, and
// every current lookup answers as without the option.
func TestCurrentOnlyIndexWritesFewerEntries(t *testing.T) {
	t.Parallel()
	f := newCurrentOnlyFixture()
	full, only := f.open(t, "", false), f.open(t, "", true)
	defer func() { _, _ = full.Close(), only.Close() }()
	f.fill(t, full)
	f.fill(t, only)
	fc, fm := indexEntries(t, full)
	oc, om := indexEntries(t, only)
	if om != 0 || oc != fc || fm == 0 {
		t.Fatalf("current-only: %d current, %d member entries; full: %d current, %d member; want the same current entries and no member entry", oc, om, fc, fm)
	}
	if saved := 1 - float64(oc+om)/float64(fc+fm); saved < 0.6 {
		t.Fatalf("current-only keeps %d of %d index entries: %.0f%% fewer, want at least 60%%", oc+om, fc+fm, 100*saved)
	}
	want, got := f.currentAnswers(t, full), f.currentAnswers(t, only)
	for q, ids := range want {
		if !slices.Equal(got[q], ids) {
			t.Errorf("%s: current-only %v, want %v", q, got[q], ids)
		}
	}
}

// pinnedDoors runs every node membership door (a pinned or windowed read's
// source) on (D, k, "v1") and returns each door's error and whether fn ran.
func pinnedDoors(bs *Store, d uint16) (map[string]error, bool) {
	vk := types.IndexablePropertyValueKey("v1")
	called := false
	fn := func(types.NodeID, types.Instant) bool { called = true; return true }
	out := map[string]error{
		"member":  bs.ForEachNodePropertyTxMember(d, "k", vk, fn),
		"after":   bs.ForEachNodePropertyTxMemberAfter(d, "k", vk, 0, 10, fn),
		"during":  bs.ForEachNodePropertyTxMemberDuring(d, "k", vk, 0, 1<<40, 1<<40, fn),
		"metered": bs.ForEachNodePropertyTxMemberMetered(d, "k", []string{vk}, nil, nil, fn),
		"range":   bs.ForEachNodePropertyTxMemberInRange(d, "k", 0, 1, fn),
		"any":     bs.ForEachNodePropertyTxMemberAny(d, "k", fn),
	}
	return out, called
}

// TestCurrentOnlyIndexRefusesPinnedReads: every membership door of a
// current-only definition is refused with ErrPropertyIndexCurrentOnly before
// any work (fn never runs, no sidecar is built); the store reports the
// definition as serving no pinned read, a full one as serving them.
func TestCurrentOnlyIndexRefusesPinnedReads(t *testing.T) {
	t.Parallel()
	f := newCurrentOnlyFixture()
	only := f.open(t, "", true)
	defer func() { _ = only.Close() }()
	f.fill(t, only)
	errs, called := pinnedDoors(only, f.d)
	if called {
		t.Error("a refused door called fn")
	}
	for door, err := range errs {
		if !errors.Is(err, storecontract.ErrPropertyIndexCurrentOnly) {
			t.Errorf("%s: %v, want ErrPropertyIndexCurrentOnly", door, err)
		}
	}
	if len(only.nodePropTx) != 0 {
		t.Errorf("a refused door built %d membership sidecars", len(only.nodePropTx))
	}
	if !only.PropertyIndexCurrentOnly(f.d, "k") {
		t.Error("current-only definition: PropertyIndexCurrentOnly false")
	}
	if only.PropertyIndexCurrentOnly(f.d, "absent") {
		t.Error("no definition: PropertyIndexCurrentOnly true")
	}
	full := f.open(t, "", false)
	defer func() { _ = full.Close() }()
	f.fill(t, full)
	if full.PropertyIndexCurrentOnly(f.d, "k") {
		t.Error("full definition: PropertyIndexCurrentOnly true")
	}
	errs, called = pinnedDoors(full, f.d)
	if !called {
		t.Error("full definition: no door found v1's member")
	}
	for door, err := range errs {
		if err != nil {
			t.Errorf("full definition, %s: %v", door, err)
		}
	}
}

// TestCurrentOnlyIndexLabelJoinsAndLeaves: the member layout's built marker
// decides each writable open. A label that joins the list loses its member
// entries and marker at the next writable open (pinned reads refused, current
// answers kept); one that leaves gets them built again, with the answers it
// had before it joined.
func TestCurrentOnlyIndexLabelJoinsAndLeaves(t *testing.T) {
	t.Parallel()
	f := newCurrentOnlyFixture()
	dir := t.TempDir()
	members := func(bs *Store) map[types.NodeID]types.Instant {
		out := map[types.NodeID]types.Instant{}
		if err := bs.ForEachNodePropertyTxMemberAny(f.d, "k", func(id types.NodeID, tx types.Instant) bool { out[id] = tx; return true }); err != nil {
			t.Fatal(err)
		}
		return out
	}
	marker := func(bs *Store) bool {
		found := false
		_ = bs.db.View(func(txn *badgerv4.Txn) error {
			_, err := txn.Get(storepkg.EntityIndexBuiltKey(storepkg.EntityIndexNodeMember, f.d, "k"))
			found = err == nil
			return nil
		})
		return found
	}

	bs := f.open(t, dir, false)
	f.fill(t, bs)
	_, fm := indexEntries(t, bs)
	wantMembers, wantCurrent := members(bs), f.currentAnswers(t, bs)
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs = f.open(t, dir, true) // D joins
	if _, m := indexEntries(t, bs); m != 0 || marker(bs) {
		t.Errorf("joined: %d member entries, marker %v; want none", m, marker(bs))
	}
	if err := bs.ForEachNodePropertyTxMemberAny(f.d, "k", func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, storecontract.ErrPropertyIndexCurrentOnly) {
		t.Errorf("joined: pinned read %v, want ErrPropertyIndexCurrentOnly", err)
	}
	if got := f.currentAnswers(t, bs); !mapsEqualIDs(got, wantCurrent) {
		t.Error("joined: current answers changed")
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs = f.open(t, dir, false) // D leaves
	defer func() { _ = bs.Close() }()
	if _, m := indexEntries(t, bs); m != fm || !marker(bs) {
		t.Errorf("left: %d member entries, marker %v; want %d and the marker", m, marker(bs), fm)
	}
	if got := members(bs); len(got) != len(wantMembers) {
		t.Errorf("left: %d members, want %d", len(got), len(wantMembers))
	} else {
		for id, tx := range wantMembers {
			if got[id] != tx {
				t.Errorf("left: member %d first tx %d, want %d", id, got[id], tx)
			}
		}
	}
	if bs.PropertyIndexCurrentOnly(f.d, "k") {
		t.Error("left: still current-only")
	}
}

func mapsEqualIDs(a, b map[string][]types.NodeID) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if !slices.Equal(v, b[k]) {
			return false
		}
	}
	return true
}
