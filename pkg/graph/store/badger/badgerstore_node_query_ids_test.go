package badger

import (
	"encoding/binary"
	"fmt"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// ELEMPROBE: the store's own refusals and filter, which the graph's door
// does not reach (it passes no time coordinate and sees no building index).
// NodeIDsByLabelAndPropertyKeys answers from the on-disk index; it refuses a
// time coordinate and an index still building, and an index key whose node
// is gone carries no label and is dropped.
func TestNodeIDsByLabelAndPropertyKeysRefusesAndFilters(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStorePropIdxOnDisk(t)
	const label = uint16(1)
	for id := int64(1); id <= 2; id++ {
		n := types.NewNode(types.NodeID(id), label, nil)
		if err := n.SetProperty("k", int64(5)); err != nil {
			t.Fatal(err)
		}
		if err := bs.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	if err := bs.CreatePropertyIndex(label, "k"); err != nil {
		t.Fatal(err)
	}
	if err := bs.Flush(); err != nil {
		t.Fatal(err)
	}
	keys := []string{indexpkg.PropertyValueKey(int64(5))}
	ask := func(opts QueryOpts) ([]types.NodeID, bool) {
		t.Helper()
		ids, ok, err := bs.NodeIDsByLabelAndPropertyKeys(label, "k", keys, opts)
		if err != nil {
			t.Fatal(err)
		}
		return ids, ok
	}
	if ids, ok := ask(QueryOpts{}); !ok || fmt.Sprint(ids) != "[1 2]" {
		t.Fatalf("plain lookup: %v, ok %v; want [1 2] from the index", ids, ok)
	}

	// An index key whose node is gone (written straight to badger).
	tok, _ := bs.propKeyTokenFor("k")
	orphan, _ := storepkg.AppendPropertyIndexValuePrefix(nil, tok, keys[0])
	orphan = binary.BigEndian.AppendUint64(orphan, 3)
	if err := bs.db.Update(func(txn *badgerv4.Txn) error { return txn.Set(orphan, nil) }); err != nil {
		t.Fatal(err)
	}
	if ids, ok := ask(QueryOpts{}); !ok || fmt.Sprint(ids) != "[1 2]" {
		t.Fatalf("with an orphan key: %v, ok %v; want [1 2]", ids, ok)
	}

	if ids, ok := ask(QueryOpts{ValidAt: 1, ValidAtSet: true}); ok {
		t.Fatalf("a time coordinate answered %v from the index, want a refusal", ids)
	}

	bs.idxMu.Lock()
	idx := bs.propertyIndexes[indexpkg.PropertyIndexKey{LabelToken: label, PropertyKey: "k"}]
	idx.Mutated = map[snowflake.ID]struct{}{}
	bs.idxMu.Unlock()
	if ids, ok := ask(QueryOpts{}); ok {
		t.Fatalf("a building index answered %v, want a refusal", ids)
	}
}
