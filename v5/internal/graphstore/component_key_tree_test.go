package graphstore

import (
	"encoding/binary"
	"errors"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestComponentKeyTreeNamesUseSemanticOrder(t *testing.T) {
	names := []string{"aa", "a\x00z", "😀", "å", "a", "\x00", "a\x00"}
	keys := make([]graphstate.ComponentKey, 0, len(names))
	for _, name := range names {
		keys = append(keys, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: name})
	}
	slices.SortFunc(keys, compareComponentKeys)
	want := slices.Clone(names)
	slices.Sort(want)
	for i, key := range keys {
		if key.Name != want[i] {
			t.Fatalf("name order used length/framing: got %q want %q", key.Name, want[i])
		}
	}
	// Distinct life/kind/member dimensions must dominate name comparison.
	for _, test := range []struct{ a, b graphstate.ComponentKey }{
		{graphstate.ComponentKey{Owner: 1, Kind: graphstate.Presence}, graphstate.ComponentKey{Owner: 2, Kind: graphstate.Presence}},
		{graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "z"}, graphstate.ComponentKey{Owner: 1, Life: 12, Kind: graphstate.Label, Name: "a"}},
		{graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "z"}, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "a"}},
		{graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99}, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 100}},
	} {
		if compareComponentKeys(test.a, test.b) >= 0 || compareComponentKeys(test.b, test.a) <= 0 {
			t.Fatal("component identity dimensions lost")
		}
	}
}

func TestComponentKeyTreeByteSplitsAndResourceLimits(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	l := componentKeyTreeLimits{64, 3, 8, 2048}
	keys := []graphstate.ComponentKey{}
	for n := range 10 {
		name := string(rune('a'+n)) + strings.Repeat("z", 255)
		keys = append(keys, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: name})
	}
	tree := commitComponentKeys(t, f, componentKeyTreeRoot{}, keys, l)
	if tree.level == 0 {
		t.Fatal("byte-full leaf was not split")
	}
	for _, key := range keys {
		q := readComponentKeyTree(t, f, f.index)
		if found, err := q.hasComponentKey(tree, key, l); err != nil || !found {
			t.Fatal("byte split lost key", err)
		}
		if err := q.q.c.view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for _, limits := range []componentKeyTreeLimits{{}, {1, 3, 8, 2048}, {64, 2, 8, 2048}, {64, 3, 9, 2048}, {64, 3, 8, 1}} {
		if _, _, s, err := stageComponentKeys(t, f, componentKeyTreeRoot{}, nil, limits); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid tree limits accepted", err)
		} else {
			if rows, err := s.Writes(); err != nil || len(rows) != 0 {
				t.Fatal("invalid policy changed stage", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.c.view.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	tight := l
	tight.maxLevels = 1
	if _, err := readComponentKeyTree(t, f, f.index).hasComponentKey(tree, keys[0], tight); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("tighter height became corruption", err)
	}
	if _, _, s, err := stageComponentKeys(t, f, componentKeyTreeRoot{}, keys[:1], componentKeyTreeLimits{64, 3, 8, 64}); !errors.Is(err, ErrResourceLimit) {
		t.Fatal("unsplittable single key accepted", err)
	} else if rows, err := s.Writes(); err != nil || len(rows) != 0 {
		t.Fatal("single-key refusal left empty allocation", err)
	}
	q := readComponentKeyTree(t, f, f.index)
	if _, err := q.hasComponentKey(tree, graphstate.ComponentKey{}, l); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid lookup key accepted", err)
	}
	if err := q.q.c.view.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := q.hasComponentKey(tree, keys[0], l); !errors.Is(err, raftlog.ErrClosed) || errors.Is(err, ErrCorrupt) {
		t.Fatal("closed view became structural corruption", err)
	}
}

func TestComponentKeyTreeChildBoundsAndCountsMustMatchReferencedPages(t *testing.T) {
	for _, mutation := range []string{"missing", "count", "first", "last", "namespace"} {
		t.Run(mutation, func(t *testing.T) {
			f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
			l := componentKeyTreeLimits{3, 3, 8, 4096}
			keys := []graphstate.ComponentKey{}
			for n := range 16 {
				keys = append(keys, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "a" + string(rune('a'+n))})
			}
			tree := commitComponentKeys(t, f, componentKeyTreeRoot{}, keys, l)
			q := readComponentKeyTree(t, f, f.index)
			node, err := q.componentKeyTreeRoot(tree, l)
			if err != nil || node.level < 1 {
				t.Fatal(err)
			}
			root := f.root
			switch mutation {
			case "missing":
				var id uint64
				root, id, err = root.ReservePhysical(1)
				if err != nil {
					t.Fatal(err)
				}
				node.children[0].id = id
			case "count":
				node.children[0].count++
				tree.count++
			case "first":
				node.children[0].first.Name = "a"
			case "last":
				node.children[0].last.Name = node.children[0].first.Name + "\x00"
			}
			c := q.q.c
			if mutation == "namespace" {
				foreignRoot := c.root
				foreignRoot.namespace.Partition++
				c = &Catalog{root: foreignRoot, limits: c.limits}
			}
			wire, err := encodeComponentKeyTreeNode(node, c, l)
			if err != nil {
				t.Fatal(err)
			}
			f.root, f.index = commitRows(t, f.db, root, []raftlog.KV{{Key: physicalKey(root.namespace, componentKeyTreeRecord, node.id), Value: wire}})
			if _, err := readComponentKeyTree(t, f, f.index).hasComponentKey(tree, keys[0], l); !errors.Is(err, ErrCorrupt) {
				t.Fatal("referenced child does not match its canonical metadata", err)
			}
		})
	}
}

func stageComponentKeys(t *testing.T, f *pageFixture, tree componentKeyTreeRoot, keys []graphstate.ComponentKey, limits componentKeyTreeLimits) (componentKeyTreeRoot, Root, *Stage, error) {
	t.Helper()
	c := openCatalog(t, f.db, f.index, Limits{})
	s := stage(t, c)
	var out componentKeyTreeRoot
	var root Root
	err := s.operation(t.Context(), func(base *reader) error {
		q := pageStage{&pageReader{q: base, limits: DefaultPageLimits()}, f.root}
		q.allocation = &q.root
		var err error
		if tree.id == 0 {
			tree, err = q.newComponentKeyTree(limits)
			if err != nil {
				return err
			}
		}
		for _, key := range keys {
			tree, err = q.insertComponentKey(tree, key, limits)
			if err != nil {
				return err
			}
		}
		out, root = tree, q.root
		return nil
	})
	return out, root, s, err
}

func commitComponentKeys(t *testing.T, f *pageFixture, tree componentKeyTreeRoot, keys []graphstate.ComponentKey, limits componentKeyTreeLimits) componentKeyTreeRoot {
	t.Helper()
	out, root, s, err := stageComponentKeys(t, f, tree, keys, limits)
	if err != nil {
		t.Fatal(err)
	}
	f.root, f.index = commitStage(t, f.db, root, s)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.c.view.Close(); err != nil {
		t.Fatal(err)
	}
	return out
}

func readComponentKeyTree(t *testing.T, f *pageFixture, index uint64) *pageReader {
	t.Helper()
	c := openCatalog(t, f.db, index, Limits{})
	base, err := c.reader(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return &pageReader{q: base, limits: DefaultPageLimits()}
}

func TestComponentKeyTreeSplitsRememberHistoricalAbsence(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	l := componentKeyTreeLimits{3, 3, 8, 4096}
	empty := commitComponentKeys(t, f, componentKeyTreeRoot{}, nil, l)
	emptyIndex := f.index
	names := []string{"aa", "a\x00z", "😀", "å", "a", "\x00", "a\x00"}
	keys := []graphstate.ComponentKey{{Owner: 1, Kind: graphstate.Presence}, {Owner: 2, Kind: graphstate.Presence}}
	for _, owner := range []graphstate.EntityID{1, 2} {
		for _, life := range []graphstate.LifeID{11, 12} {
			for _, name := range names {
				keys = append(keys, graphstate.ComponentKey{Owner: owner, Life: life, Kind: graphstate.ScalarProperty, Name: name})
			}
		}
	}
	keys = append(keys, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 99}, graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 100})
	tree := empty
	for start := 0; start < len(keys); start += 4 {
		tree = commitComponentKeys(t, f, tree, keys[start:min(start+4, len(keys))], l)
	}
	if tree.level < 2 || tree.count != uint64(len(keys)) {
		t.Fatalf("split lost keys: %+v", tree)
	}
	current := readComponentKeyTree(t, f, f.index)
	for _, key := range keys {
		found, err := current.hasComponentKey(tree, key, l)
		if err != nil || !found {
			t.Fatalf("lost exact key %+v: %v", key, err)
		}
	}
	for _, key := range []graphstate.ComponentKey{{Owner: 3, Kind: graphstate.Presence}, {Owner: 1, Life: 11, Kind: graphstate.ScalarProperty, Name: "a\x00missing"}, {Owner: 1, Life: 11, Kind: graphstate.SetMember, Name: "set", Member: 101}} {
		if found, err := current.hasComponentKey(tree, key, l); err != nil || found {
			t.Fatalf("prefix/alias phantom %+v: %v", key, err)
		}
	}
	for _, key := range keys {
		old := readComponentKeyTree(t, f, emptyIndex)
		if found, err := old.hasComponentKey(empty, key, l); err != nil || found || old.work.Records != 1 {
			t.Fatalf("historical empty root touched future pages: %+v %v", old.work, err)
		}
		if err := old.q.c.view.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := readComponentKeyTree(t, f, emptyIndex).hasComponentKey(tree, keys[0], l); !errors.Is(err, ErrCorrupt) {
		t.Fatal("latest tree metadata accepted at historical root", err)
	}
}

func TestComponentKeyTreeDuplicateAndLateFailuresPreserveStaging(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	l := componentKeyTreeLimits{2, 3, 1, 4096}
	keys := []graphstate.ComponentKey{{Owner: 1, Kind: graphstate.Presence}, {Owner: 2, Kind: graphstate.Presence}}
	tree := commitComponentKeys(t, f, componentKeyTreeRoot{}, keys[:1], l)
	out, root, s, err := stageComponentKeys(t, f, tree, keys[:1], l)
	if err != nil || out != tree || root != f.root {
		t.Fatal("duplicate allocated/rewrote tree", out, err)
	}
	writes, err := s.Writes()
	if err != nil || len(writes) != 0 {
		t.Fatal("duplicate wrote metadata", err)
	}
	third := graphstate.ComponentKey{Owner: 3, Kind: graphstate.Presence}
	out, root, failed, err := stageComponentKeys(t, f, tree, []graphstate.ComponentKey{keys[1], third}, l)
	if !errors.Is(err, ErrResourceLimit) || out != (componentKeyTreeRoot{}) || root != (Root{}) {
		t.Fatal("height failure returned partial root", out, err)
	}
	writes, err = failed.Writes()
	if err != nil || len(writes) != 0 {
		t.Fatal("late failure retained first insertion", err)
	}
	q := readComponentKeyTree(t, f, f.index)
	if found, err := q.hasComponentKey(tree, keys[1], l); err != nil || found {
		t.Fatal("failed insert leaked", err)
	}
	if _, _, rejected, err := stageComponentKeys(t, f, tree, []graphstate.ComponentKey{keys[1], {}}, l); !errors.Is(err, ErrInvalid) {
		t.Fatal("invalid later key accepted", err)
	} else if rows, err := rejected.Writes(); err != nil || len(rows) != 0 {
		t.Fatal("invalid sequence retained writes", err)
	}
}

func TestComponentKeyTreeCodecRejectsTruncationOrderingAndBounds(t *testing.T) {
	f := fixturePages(t, temporal.ProfileIntegerZ, PageLimits{})
	c := openCatalog(t, f.db, f.index, Limits{})
	l := defaultComponentKeyTreeLimits()
	k := graphstate.ComponentKey{Owner: 1, Life: 11, Kind: graphstate.Label, Name: "a\x00"}
	other := k
	other.Name = "aa"
	nodes := []componentKeyTreeNode{
		{id: 1, keys: []graphstate.ComponentKey{k, other}},
		{id: 1, level: 1, children: []componentKeyTreeChild{{2, 1, k, k}, {3, 1, other, other}}},
	}
	for _, node := range nodes {
		wire, err := encodeComponentKeyTreeNode(node, c, l)
		if err != nil {
			t.Fatal(err)
		}
		decode := func(wire []byte, limits componentKeyTreeLimits) error {
			root := c.root
			root.next = 8
			key := physicalKey(root.namespace, componentKeyTreeRecord, 1)
			base := &reader{c: &Catalog{root: root, limits: c.limits}, ctx: t.Context(), pending: map[string]raftlog.KV{string(key): {Key: key, Value: wire}}}
			q := pageReader{q: base, limits: DefaultPageLimits()}
			_, err := q.componentKeyTreeNode(1, limits)
			return err
		}
		if err := decode(wire, l); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < len(wire); n++ {
			if err := decode(wire[:n], l); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("canonical prefix accepted at %d: %v", n, err)
			}
		}
		if err := decode(append(slices.Clone(wire), 0), l); !errors.Is(err, ErrCorrupt) {
			t.Fatal("trailing bytes accepted", err)
		}
		bad := slices.Clone(wire)
		binary.BigEndian.PutUint32(bad[37:41], math.MaxUint32)
		if err := decode(bad, l); !errors.Is(err, ErrCorrupt) {
			t.Fatal("hostile count accepted", err)
		}
		tight := l
		tight.maxPageBytes = 64
		if err := decode(wire, tight); !errors.Is(err, ErrResourceLimit) {
			t.Fatal("tighter policy became corruption", err)
		}
	}
	for _, node := range []componentKeyTreeNode{
		{id: 1, keys: []graphstate.ComponentKey{other, k}},
		{id: 1, keys: []graphstate.ComponentKey{k, k}},
		{id: 1, keys: []graphstate.ComponentKey{{}}},
		{id: 1, level: 1, children: []componentKeyTreeChild{{2, 1, k, other}, {3, 1, other, other}}},
		{id: 1, level: 1, children: []componentKeyTreeChild{{2, 1, k, k}, {2, 1, other, other}}},
		{id: 1, level: 1, children: []componentKeyTreeChild{{1, 1, k, k}, {3, 1, other, other}}},
	} {
		if _, err := encodeComponentKeyTreeNode(node, c, l); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid sorted/child relation encoded", err)
		}
	}
}
