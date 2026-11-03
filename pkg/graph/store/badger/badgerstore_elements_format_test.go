package badger

import (
	"errors"
	"fmt"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// Backlog 63: a store persisting an Elements definition (node or relationship) is
// stamped ElementsStoreFormatVersion so an older reader refuses the open (it would
// load the index plain); a store without Elements keeps StoreFormatVersion.
func TestElementsIndexStampsFormatMarker(t *testing.T) {
	if testing.Short() && !raceEnabled {
		t.Skip("long tier: release gate")
	}
	elements := storecontract.PropertyIndexOptions{Elements: true}
	for _, c := range []struct {
		name   string
		create func(*Store) error
		want   uint16
	}{
		{"plain", func(bs *Store) error {
			return bs.CreatePropertyIndexWithOptions(1, "v", storecontract.PropertyIndexOptions{RangeCounts: true})
		}, openStampVersion()},
		{"node elements", func(bs *Store) error { return bs.CreatePropertyIndexWithOptions(1, "v", elements) }, storepkg.ElementsStoreFormatVersion},
		{"rel elements", func(bs *Store) error { return bs.CreateRelPropertyIndexWithOptions(1, "v", elements) }, storepkg.ElementsStoreFormatVersion},
	} {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			bs, err := New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := c.create(bs); err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if got, ok := readRawMarker(t, dir); !ok || got != c.want {
				t.Fatalf("marker = %d (present %v), want %d", got, ok, c.want)
			}
			// This binary reopens it and keeps the marker.
			bs, err = New(Config{Dir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := bs.Close(); err != nil {
				t.Fatal(err)
			}
			if got, _ := readRawMarker(t, dir); got != c.want {
				t.Fatalf("marker after reopen = %d, want %d", got, c.want)
			}
			// A reader that predates Elements.
			prev := maxReadableStoreFormatVersion
			maxReadableStoreFormatVersion = openStampVersion()
			defer func() { maxReadableStoreFormatVersion = prev }()
			bs, err = New(Config{Dir: dir})
			if c.want == openStampVersion() {
				if err != nil {
					t.Fatalf("older reader on a store without Elements: %v", err)
				}
				_ = bs.Close()
				return
			}
			if err == nil {
				_ = bs.Close()
				t.Fatal("older reader opened an Elements store")
			}
			if !errors.Is(err, storecontract.ErrWireFormatVersionUnsupported) {
				t.Fatalf("older reader: %v, want ErrWireFormatVersionUnsupported", err)
			}
		})
	}
}

// ELEMDISK: an Elements definition on disk keys each element as the scalar of
// the same value, in the keyspace a scalar definition of another label on the
// same key shares. Each answers by its own match (a scalar never matches an
// element, an Elements index matches a scalar as a one-element list), an
// update and the drop of one leave the other's answers, and the Elements
// handle holds no entry in RAM.
func TestElementsIndexOnDiskSharesTheKeyspace(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const elems, plain = uint16(1), uint16(2)
	put := func(id int64, label uint16, extra []uint16, v any) {
		t.Helper()
		n := types.NewNode(types.NodeID(id), label, extra)
		if err := n.SetProperty("k", v); err != nil {
			t.Fatal(err)
		}
		write := bs.PutNode
		if _, err := bs.GetNode(n.ID()); err == nil {
			write = bs.ReplaceNode
		}
		if err := write(n); err != nil {
			t.Fatal(err)
		}
	}
	put(1, elems, []uint16{plain}, []int64{5, 6}) // both labels, a list
	put(2, elems, nil, []int64{5})
	put(3, plain, nil, int64(5))
	put(4, elems, nil, int64(6)) // a scalar under Elements: a one-element list
	if err := bs.CreatePropertyIndexWithOptions(elems, "k", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatalf("Elements on disk: %v", err)
	}
	if err := bs.CreatePropertyIndex(plain, "k"); err != nil {
		t.Fatal(err)
	}
	check := func(step string, label uint16, v int64, want []int64) {
		t.Helper()
		if got := nodeIDsVia(t, bs, label, "k", v); fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("%s: label %d k %d = %v, want %v", step, label, v, got, want)
		}
	}
	check("create", elems, 5, []int64{1, 2})
	check("create", elems, 6, []int64{1, 4})
	check("create", plain, 5, []int64{3})
	check("create", plain, 6, nil)
	put(2, elems, nil, []int64{6, 7})    // an update drops 5, adds 6 and 7
	put(5, elems, nil, []int64{5, 5, 8}) // a write after the create, a duplicate
	check("write", elems, 5, []int64{1, 5})
	check("write", elems, 6, []int64{1, 2, 4})
	check("write", elems, 8, []int64{5})
	check("write", plain, 5, []int64{3})
	bs.idxMu.RLock()
	idx := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: elems, PropertyKey: "k"}]
	inRAM := len(idx.Entries)
	bs.idxMu.RUnlock()
	if !idx.OnDisk() || !idx.Elements() || inRAM != 0 {
		t.Fatalf("Elements handle: on disk %v, elements %v, %d keys in RAM; want on disk, elements, none", idx.OnDisk(), idx.Elements(), inRAM)
	}
	if err := bs.DropPropertyIndex(elems, "k"); err != nil {
		t.Fatal(err)
	}
	check("drop Elements", plain, 5, []int64{3})
	check("drop Elements", plain, 6, nil)
}
