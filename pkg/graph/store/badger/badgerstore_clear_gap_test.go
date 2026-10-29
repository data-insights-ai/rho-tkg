package badger

import (
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// prefixKeys counts the committed keys under prefix.
func prefixKeys(t *testing.T, bs *Store, prefix byte) int {
	t.Helper()
	var n int
	if err := bs.db.View(func(txn *badgerv4.Txn) error {
		it := txn.NewIterator(badgerv4.IteratorOptions{Prefix: []byte{prefix}})
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

// TestClearDropsDiskIndexEntries: Clear removes the on-disk property index
// entries (0x0A) and the temporal-index rebuild rows (0x0B) with the rest of
// the data (CLEARGAP).
func TestClearDropsDiskIndexEntries(t *testing.T) {
	t.Parallel()
	bs, err := New(Config{InMemory: true, PropertyIndexOnDisk: true, TemporalIndexOnDisk: true,
		PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry()})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	const label = uint16(7)
	if err := bs.CreatePropertyIndex(label, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.CreateTemporalIndex(label); err != nil {
		t.Fatal(err)
	}
	n := types.NewNode(types.NodeID(1), label, nil)
	if err := n.SetProperty("k", "v"); err != nil {
		t.Fatal(err)
	}
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: 10})
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []byte{storepkg.KeyPropertyIndex, storepkg.KeyTemporalIndex} {
		if prefixKeys(t, bs, p) == 0 {
			t.Fatalf("setup: no 0x%02X key", p)
		}
	}
	if err := bs.Clear(); err != nil {
		t.Fatal(err)
	}
	for _, p := range []byte{storepkg.KeyPropertyIndex, storepkg.KeyTemporalIndex} {
		if got := prefixKeys(t, bs, p); got != 0 {
			t.Errorf("after Clear: %d keys under 0x%02X", got, p)
		}
	}
}
