package badger

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestOpenImageFormat2ElementsIndexRebuiltAfterUpgrade (WP-31 review): an
// Elements index built over existing lists before v4.58.0 kept one element per
// row, and a clean close persisted that in the open image (format 2). The
// first open of the fixed release must not trust a format-2 image: it rebuilds
// from the rows, so every element answers. Control: an image of the current
// format is trusted (the damage survives), so the test sees the image.
func TestOpenImageFormat2ElementsIndexRebuiltAfterUpgrade(t *testing.T) {
	for _, tc := range []struct {
		name    string
		format  byte
		rebuilt bool
	}{
		{"format 2 (pre-fix image)", 2, true},
		{"current format (control)", openImageMagic[len(openImageMagic)-1], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			n := types.NewNode(types.NodeID(1<<40+2), 3, nil)
			if err := n.SetProperty("tags", []any{int64(1), int64(9), int64(4)}); err != nil {
				t.Fatal(err)
			}
			if err := bs.PutNode(n); err != nil {
				t.Fatal(err)
			}
			if err := bs.CreatePropertyIndexWithOptions(3, "tags", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
				t.Fatal(err)
			}
			// The pre-fix build's shape: only the first element keyed.
			bs.idxMu.Lock()
			idx := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: 3, PropertyKey: "tags"}]
			idx.Remove(n.ID().SnowflakeID(), int64(9))
			idx.Remove(n.ID().SnowflakeID(), int64(4))
			bs.idxMu.Unlock()
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			db, err := badgerv4.Open(badgerv4.DefaultOptions(dir).WithLogger(nil))
			if err != nil {
				t.Fatal(err)
			}
			if err := db.Update(func(txn *badgerv4.Txn) error {
				item, err := txn.Get(openImageHeaderKey)
				if err != nil {
					return err
				}
				hdr, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}
				hdr[len(openImageMagic)-1] = tc.format
				return txn.Set(openImageHeaderKey, hdr)
			}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			bs, err = New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()
			for _, v := range []int64{1, 9, 4} {
				ns, err := bs.NodesByLabelAndProperty(3, "tags", v, storecontract.QueryOpts{})
				if err != nil {
					t.Fatal(err)
				}
				want := 1
				if !tc.rebuilt && v != 1 {
					want = 0
				}
				if len(ns) != want {
					t.Errorf("element %d after reopen: %d nodes, want %d", v, len(ns), want)
				}
			}
		})
	}
}

// TestElementsIndexLookupWhileBuilding (WP-31 review): while an Elements index
// is still backfilling, the lookup falls back to the scan; that scan must use
// the declared index's "contains" match, not plain equality (a list holding
// the value matches; a plain index's scan would miss it). Node and rel.
func TestElementsIndexLookupWhileBuilding(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	list := []any{int64(1), int64(9)}
	a := types.NewNode(types.NodeID(1<<40+2), 3, nil)
	if err := a.SetProperty("tags", list); err != nil {
		t.Fatal(err)
	}
	b := types.NewNode(types.NodeID(1<<40+4), 3, nil)
	r := types.NewRelationship(types.RelID(1<<40+3), 5, a.ID(), b.ID())
	if err := r.SetProperty("tags", list); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*types.Node{a, b} {
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.PutRelationship(r); err != nil {
		t.Fatal(err)
	}
	el := storecontract.PropertyIndexOptions{Elements: true}
	if err := bs.CreatePropertyIndexWithOptions(3, "tags", el); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateRelPropertyIndexWithOptions(5, "tags", el); err != nil {
		t.Fatal(err)
	}
	// Back into the build window: a placeholder still tracking writes.
	bs.idxMu.Lock()
	bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: 3, PropertyKey: "tags"}].Mutated = map[snowflake.ID]struct{}{}
	bs.relPropertyIndexes[indexpkg.RelPropertyIndexKey{RelTypeToken: 5, PropertyKey: "tags"}].Mutated = map[snowflake.ID]struct{}{}
	bs.idxMu.Unlock()
	for _, v := range []int64{1, 9} {
		if ns, err := bs.NodesByLabelAndProperty(3, "tags", v, storecontract.QueryOpts{}); err != nil || len(ns) != 1 {
			t.Errorf("node element %d while building: %d nodes, %v; want 1", v, len(ns), err)
		}
		if rs, err := bs.RelationshipsByTypeAndProperty(5, "tags", v, storecontract.QueryOpts{}); err != nil || len(rs) != 1 {
			t.Errorf("rel element %d while building: %d rels, %v; want 1", v, len(rs), err)
		}
	}
	if ns, _ := bs.NodesByLabelAndProperty(3, "tags", int64(5), storecontract.QueryOpts{}); len(ns) != 0 {
		t.Errorf("phantom element 5 while building: %d nodes, want 0", len(ns))
	}
}
