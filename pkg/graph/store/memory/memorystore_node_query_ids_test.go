package memory

import (
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// RHOMEMIDS: the memory store's NodeIDsByLabelAndPropertyKeys answers from a
// published index only: the label's IDs in the keys' postings, paged by After
// and Limit; ok=false for a time coordinate, an undeclared index, and an
// index still building (Phase 2 runs with ms.mu unheld, its postings partial).
func TestNodeIDsByLabelAndPropertyKeysFromIndex(t *testing.T) {
	ms := New()
	door, ok := any(ms).(storecontract.NodePropertyKeysIDsCapability)
	if !ok {
		t.Fatal("memory.Store has no NodePropertyKeysIDsCapability")
	}
	const label, other = 10, 11
	put := func(id int64, labels []uint16, v any) {
		t.Helper()
		n := types.NewNode(types.NodeID(id), labels[0], labels[1:])
		if err := n.SetProperty("k", v); err != nil {
			t.Fatal(err)
		}
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	put(1, []uint16{label}, []int64{1, 2})
	put(2, []uint16{label}, int64(2))
	put(3, []uint16{other}, int64(2)) // another label, same key and value
	put(4, []uint16{label, other}, []int64{3, 2})
	put(5, []uint16{label}, int64(7))
	key2 := types.IndexablePropertyValueKey(int64(2))
	key7 := types.IndexablePropertyValueKey(int64(7))

	// During the build: ok=false, whatever the postings hold so far.
	var during []bool
	phase2ScanHook = func() {
		_, ok, err := door.NodeIDsByLabelAndPropertyKeys(label, "k", []string{key2}, storecontract.QueryOpts{})
		if err != nil {
			t.Error(err)
		}
		during = append(during, ok)
	}
	t.Cleanup(func() { phase2ScanHook = nil })
	if err := ms.CreatePropertyIndexWithOptions(label, "k", storecontract.PropertyIndexOptions{Elements: true}); err != nil {
		t.Fatal(err)
	}
	phase2ScanHook = nil
	if len(during) == 0 || slices.Contains(during, true) {
		t.Errorf("during the build: ok %v, want false each time", during)
	}
	if err := ms.CreatePropertyIndex(other, "k"); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		name  string
		label uint16
		keys  []string
		opts  storecontract.QueryOpts
		want  []types.NodeID
		ok    bool
	}{
		{"Elements", label, []string{key2}, storecontract.QueryOpts{}, []types.NodeID{1, 2, 4}, true},
		{"two keys, a repeat", label, []string{key7, key2, key2}, storecontract.QueryOpts{}, []types.NodeID{1, 2, 4, 5}, true},
		{"the label filter", other, []string{key2}, storecontract.QueryOpts{}, []types.NodeID{3}, true},
		{"After and Limit", label, []string{key2, key7}, storecontract.QueryOpts{After: 1, Limit: 2}, []types.NodeID{2, 4}, true},
		{"empty key", label, []string{""}, storecontract.QueryOpts{}, nil, true},
		{"no index", 12, []string{key2}, storecontract.QueryOpts{}, nil, false},
		{"time coordinate", label, []string{key2}, storecontract.QueryOpts{ValidAt: 5, ValidAtSet: true}, nil, false},
	} {
		got, ok, err := door.NodeIDsByLabelAndPropertyKeys(c.label, "k", c.keys, c.opts)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if ok != c.ok || !slices.Equal(got, c.want) {
			t.Errorf("%s: %v ok=%v, want %v ok=%v", c.name, got, ok, c.want, c.ok)
		}
		if !ok {
			continue
		}
		rows, err := ms.NodesByLabelAndPropertyKeys(c.label, "k", c.keys, c.opts)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]types.NodeID, len(rows))
		for i, n := range rows {
			ids[i] = n.ID()
		}
		if !slices.Equal(got, ids) {
			t.Errorf("%s: IDs %v, rows %v", c.name, got, ids)
		}
	}
	if err := ms.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := door.NodeIDsByLabelAndPropertyKeys(label, "k", []string{key2}, storecontract.QueryOpts{}); err == nil {
		t.Error("closed store: no error")
	}
}
