package badger

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// lazyLabelFixture writes a store the shape asks for and returns its directory:
// 60 nodes, labels 1 (nodes 1-40), 2 (41-60 and some of 1-40) and 3 (every
// third), a property index on label 1, relationships of two types among the
// label 1 nodes only (so a shard-drop check by label 1 finds the label 2 nodes
// by their label keys alone), a removed label, a deleted node and a deleted
// relationship, and two stale label keys (a node that is not live).
//
//	clean            closed cleanly: a trusted image with the label counts
//	nocounts         the image lacks the label counts (an image before OPENWALK)
//	noproperty       the image lacks the property index: the open rebuilds it from label 1's members
//	crashed          no image at all (the DB was closed without Close)
//	delta            a trusted image older than the store (a later writer committed)
func lazyLabelFixture(t *testing.T, shape string) string {
	t.Helper()
	dir := t.TempDir()
	s, err := New(Config{Dir: dir})
	if err != nil {
		t.Fatal(err)
	}
	switch shape {
	case "nocounts":
		s.imageSectionFilterForTest = func(kind byte) bool { return kind != imgSecLabelCounts }
	case "noproperty":
		s.imageSectionFilterForTest = func(kind byte) bool { return kind != imgSecNodeProp }
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(s.CreatePropertyIndex(1, "p"))
	for i := int64(1); i <= 60; i++ {
		primary := uint16(1)
		if i > 40 {
			primary = 2
		}
		var extra []uint16
		if i%3 == 0 {
			extra = append(extra, 3)
		}
		if i%7 == 0 && primary == 1 {
			extra = append(extra, 2)
		}
		n := types.NewNode(types.NodeID(i), primary, extra)
		must(n.SetProperty("p", i%5))
		must(n.SetProperty("q", fmt.Sprintf("v%d", i)))
		must(s.PutNode(n))
	}
	for i := int64(1); i < 40; i += 2 {
		must(s.PutRelationship(types.NewRelationship(types.RelID(100+i), 7, types.NodeID(i), types.NodeID(i+1))))
	}
	for i := int64(2); i < 40; i += 4 {
		must(s.PutRelationship(types.NewRelationship(types.RelID(200+i), 8, types.NodeID(i), types.NodeID(i+2))))
	}
	// Stale label keys: nodes 9998 and 9999 have no row. A walk of the label keys
	// skips them (no live node), and so must a map built from them later.
	must(s.DBForTest().Update(func(txn *badgerv4.Txn) error {
		for _, k := range []struct {
			tok uint16
			id  int64
		}{{1, 9999}, {2, 9998}} {
			if err := txn.Set(storepkg.LabelIndexKey(k.tok, snowflake.ID(k.id)), nil); err != nil {
				return err
			}
		}
		return nil
	}))
	// A removed label, a deleted node and a deleted relationship: former
	// members the label maps must not hold.
	must(s.RemoveNodeLabelToken(types.NodeID(6), 3, types.NewNode(types.NodeID(6), 1, nil)))
	must(s.DeleteNode(types.NodeID(59)))
	must(s.DeleteRelationship(types.RelID(103)))
	switch shape {
	case "crashed":
		must(s.Flush())
		s.SetDBClosedForTest(true)
		must(s.DBForTest().Close())
	default:
		must(s.Close())
	}
	if shape == "delta" {
		db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
		must(err)
		must(db.Update(func(txn *badgerv4.Txn) error { return txn.Set([]byte("\xfeforeign"), nil) }))
		must(db.Close())
	}
	return dir
}

// labelDoors renders what the label doors answer: member lists in order, counts,
// the property-filtered reads, the doc-values doors (single label and
// intersections), the shard-drop residue, the former members and the index state.
func labelDoors(t *testing.T, bs *Store) string {
	t.Helper()
	var out []string
	add := func(format string, args ...any) { out = append(out, fmt.Sprintf(format, args...)) }
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	for _, tok := range []uint16{1, 2, 3, 9} {
		ids, err := bs.NodeIDsByLabel(tok)
		must(err)
		slices.Sort(ids)
		add("NodeIDsByLabel %d %v", tok, ids)
		var scan []types.NodeID
		must(bs.ForEachNodeByLabel(tok, storecontract.QueryOpts{}, func(n *types.Node) bool {
			scan = append(scan, n.ID())
			return true
		}))
		add("ForEachNodeByLabel %d %v", tok, scan)
		nodes, err := bs.NodesByLabel(tok, storecontract.QueryOpts{Limit: 7})
		must(err)
		var page []types.NodeID
		for _, n := range nodes {
			page = append(page, n.ID())
		}
		add("NodesByLabel %d limit 7 %v", tok, page)
		c, err := bs.NodeCountByLabel(tok)
		must(err)
		add("NodeCountByLabel %d %d", tok, c)
		byProp, err := bs.NodesByLabelAndProperty(tok, "p", int64(2), storecontract.QueryOpts{})
		must(err)
		var pids []types.NodeID
		for _, n := range byProp {
			pids = append(pids, n.ID())
		}
		slices.Sort(pids)
		add("NodesByLabelAndProperty %d p=2 %v", tok, pids)
		keyed, ok, err := bs.NodeIDsByLabelAndPropertyKeys(tok, "p", []string{indexpkg.PropertyValueKey(int64(2))}, storecontract.QueryOpts{})
		must(err)
		slices.Sort(keyed)
		add("NodeIDsByLabelAndPropertyKeys %d p=2 usable=%v %v", tok, ok, keyed)
		only, nids, rels, err := bs.CollectShardDropResidue(tok)
		must(err)
		slices.Sort(nids)
		add("CollectShardDropResidue %d only=%v nodes=%v rels=%d", tok, only, nids, len(rels))
		var of []string
		var ask []types.NodeID
		for i := types.NodeID(1); i <= 62; i++ {
			ask = append(ask, i)
		}
		must(bs.LabelTxMembersOf(tok, ask, func(id types.NodeID, first types.Instant) bool {
			of = append(of, fmt.Sprintf("%d@%d", id, first))
			return true
		}))
		slices.Sort(of)
		add("LabelTxMembersOf %d %v", tok, of)
		var members []string
		must(bs.ForEachLabelTxMember(tok, func(id types.NodeID, first types.Instant) bool {
			members = append(members, fmt.Sprintf("%d@%d", id, first))
			return true
		}))
		slices.Sort(members)
		add("ForEachLabelTxMember %d %v", tok, members)
	}
	// The streaming scan keeps its sorted member list: the second read of a label
	// gets the list the first installed (the same backing array), which needs the
	// label's member count to equal the list's.
	first, sortedFirst, err := bs.labelScanIDs(1)
	must(err)
	second, sortedSecond, err := bs.labelScanIDs(1)
	must(err)
	add("labelScanIDs 1 sorted %v %v, the second read shares the first's list: %v", sortedFirst, sortedSecond, len(first) > 0 && &first[0] == &second[0])
	rows := func(drain func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error)) string {
		var got []string
		_, ok, err := drain(func(id types.NodeID, vals []any, present []bool) bool {
			got = append(got, fmt.Sprintf("%d%v%v", id, vals, present))
			return true
		})
		must(err)
		slices.Sort(got)
		return fmt.Sprintf("usable=%v %v", ok, got)
	}
	add("ForEachDocValues 1 p,q %s", rows(func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
		return bs.ForEachDocValues(1, []string{"p", "q"}, fn)
	}))
	for _, toks := range [][]uint16{{1, 3}, {2, 3}, {1, 2}, {1, 9}} {
		add("ForEachDocValuesMulti %v %s", toks, rows(func(fn func(types.NodeID, []any, []bool) bool) (uint64, bool, error) {
			return bs.ForEachDocValuesMulti(toks, []string{"p"}, fn)
		}))
	}
	former, err := bs.BuildFormerLabelTokens()
	must(err)
	slices.Sort(former)
	add("BuildFormerLabelTokens %v", former)
	return strings.Join(out, "\n") + "\n" + bs.IndexStateDigestForTest()
}

// TestLazyLabelMapsMatchEagerOnEveryOpenShape: a read-only open with
// Config.TransientRead builds no label map when the image is trusted and holds
// the label counts, and builds each label's map from its keys when a read first
// asks; every door that reads the maps answers what the eager open's maps
// answer, over a store with removed labels, a deleted node, a property index
// and relationships. An image the lazy open cannot trust (no counts, none, an
// older one) walks the label keys as before and is not lazy. Catches a reader
// of the label index left on the empty map, a stale member (a removed label, a
// deleted node), a map built twice and a lazy open on an image it cannot trust.
func TestLazyLabelMapsMatchEagerOnEveryOpenShape(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		shape    string
		wantLazy bool
	}{
		{"clean", true},
		{"noproperty", true},
		{"nocounts", false},
		{"crashed", false},
		{"delta", false},
	} {
		t.Run(c.shape, func(t *testing.T) {
			t.Parallel()
			dir := lazyLabelFixture(t, c.shape)
			open := func(transient bool) *Store {
				s, err := New(Config{Dir: dir, ReadOnly: true, TransientRead: transient})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = s.Close() })
				return s
			}
			eager, lazy := open(false), open(true)
			if on, n := eager.LabelMapStateForTest(); on || n == 0 {
				t.Fatalf("the eager open has lazy label maps %v with %d maps, want the maps built at the open", on, n)
			}
			gotLazy, atOpen := lazy.LabelMapStateForTest()
			if gotLazy != c.wantLazy || (c.wantLazy && atOpen != 0) {
				t.Fatalf("TransientRead open: lazy label maps %v with %d maps after the open, want lazy %v and none built", gotLazy, atOpen, c.wantLazy)
			}
			want, got := labelDoors(t, eager), labelDoors(t, lazy)
			if got != want {
				t.Fatalf("the lazy open answers differ from the eager open's:\n lazy: %.1200s\neager: %.1200s", got, want)
			}
			if !c.wantLazy {
				return
			}
			// Every label read built its map once: labels 1, 2, 3 and the empty 9 (a nil entry).
			if _, inRAM := lazy.LabelMapStateForTest(); inRAM != 4 || lazy.LabelMapBuildsForTest() != 4 {
				t.Fatalf("after the doors: %d label maps in RAM, %d built, want 4 and 4 (one build per label asked, none twice)",
					inRAM, lazy.LabelMapBuildsForTest())
			}
		})
	}
}

// TestLazyLabelMapsServeConcurrentReaders: readers that ask for the same labels
// at once (the first read builds each map under lazyLabelMu) all get the whole
// membership. Run under -race for the lock order idxMu (R), then lazyLabelMu.
func TestLazyLabelMapsServeConcurrentReaders(t *testing.T) {
	t.Parallel()
	dir := lazyLabelFixture(t, "clean")
	s, err := New(Config{Dir: dir, ReadOnly: true, TransientRead: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	want := map[uint16][]types.NodeID{}
	oracle, err := New(Config{Dir: dir, ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []uint16{1, 2, 3} {
		ids, err := oracle.NodeIDsByLabel(tok)
		if err != nil {
			t.Fatal(err)
		}
		slices.Sort(ids)
		want[tok] = ids
	}
	_ = oracle.Close()
	var wg sync.WaitGroup
	errs := make(chan string, 24)
	for w := 0; w < 8; w++ {
		for _, tok := range []uint16{1, 2, 3} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ids, err := s.NodeIDsByLabel(tok)
				slices.Sort(ids)
				if err != nil || !slices.Equal(ids, want[tok]) {
					errs <- fmt.Sprintf("label %d: %d ids, %v, want %d", tok, len(ids), err, len(want[tok]))
				}
			}()
		}
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Error(e)
	}
	if built := s.LabelMapBuildsForTest(); built != 3 {
		t.Fatalf("%d label maps built for 3 labels asked by 24 readers, want 3", built)
	}
}
