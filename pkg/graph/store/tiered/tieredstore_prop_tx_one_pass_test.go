package tiered

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestTieredNodePropertyTxMembersOnePass (backlog 74): the node property
// indexes of a tiered store live on the reference shard, whose sidecars one
// pass builds. Faulty build caught: one reference-shard scan per (label, key)
// (three lookups, three builds); the members must equal the rows.
func TestTieredNodePropertyTxMembersOnePass(t *testing.T) {
	ts := newTestTieredStore(t)
	caseTok, userTok, _ := installDefaultTestLabelRegistry(t, ts)
	keys := []struct {
		label uint16
		key   string
	}{{caseTok, "seat"}, {caseTok, "zone"}, {userTok, "seat"}}
	for _, k := range keys {
		if err := ts.CreatePropertyIndex(k.label, k.key); err != nil {
			t.Fatal(err)
		}
	}
	gen := tieredNodeGen(t)
	want := map[uint16]types.NodeID{}
	for _, label := range []uint16{caseTok, userTok} {
		n := types.NewNode(types.NodeID(gen.Generate()), label, nil)
		_ = n.SetProperty("seat", int64(1))
		_ = n.SetProperty("zone", int64(1))
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
		want[label] = n.ID()
	}
	for _, k := range keys {
		var got []types.NodeID
		if err := ts.ForEachNodePropertyTxMember(k.label, k.key, types.IndexablePropertyValueKey(int64(1)), func(id types.NodeID, _ types.Instant) bool {
			got = append(got, id)
			return true
		}); err != nil {
			t.Fatalf("lookup %v: %v", k, err)
		}
		if len(got) != 1 || got[0] != want[k.label] {
			t.Fatalf("lookup %v = %v, want [%d]", k, got, want[k.label])
		}
	}
	st, err := ts.RefShardForTest().PropertyTxMembershipStats()
	if err != nil {
		t.Fatal(err)
	}
	if st.Builds != 1 || st.NodeSidecars != len(keys) {
		t.Fatalf("reference shard: %+v, want %d sidecars from 1 pass", st, len(keys))
	}
}
