package store_test

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Backlog 33: store.MemberIDsCapability lists a label's / type's current
// members from the store's index without reading their rows; the answer must
// be the IDs NodesByLabel / RelationshipsByType return, ascending, at every
// moment (unflushed writes and deletes included), per token, from every slot.
// Faults it names: the other token's members, a deleted member still listed,
// a buffered write missing, one sharded slot missing, unsorted output.
func TestMemberIDsMatchTheScanDoors(t *testing.T) {
	t.Parallel()
	for _, b := range currentBackends() {
		t.Run(b.name, func(t *testing.T) {
			st := b.open(t)
			t.Cleanup(func() { _ = st.Close() })
			m, ok := st.(storecontract.MemberIDsCapability)
			if !ok {
				return // memory and tiered decline: the core reads the rows
			}
			check := func(step string) {
				t.Helper()
				for _, tok := range []uint16{1, 2} {
					rows, err := st.NodesByLabel(tok, storecontract.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					want := make([]types.NodeID, 0, len(rows))
					for _, n := range rows {
						want = append(want, n.ID())
					}
					got, err := m.NodeIDsByLabel(tok)
					if err != nil || !slices.Equal(got, want) {
						t.Fatalf("%s: NodeIDsByLabel(%d) = (%v, %v), NodesByLabel lists %v", step, tok, got, err, want)
					}
					rels, err := st.RelationshipsByType(tok+4, storecontract.QueryOpts{})
					if err != nil {
						t.Fatal(err)
					}
					wantRels := make([]types.RelID, 0, len(rels))
					for _, r := range rels {
						wantRels = append(wantRels, r.ID())
					}
					gotRels, err := m.RelIDsByType(tok + 4)
					if err != nil || !slices.Equal(gotRels, wantRels) {
						t.Fatalf("%s: RelIDsByType(%d) = (%v, %v), RelationshipsByType lists %v", step, tok+4, gotRels, err, wantRels)
					}
				}
			}
			for id := int64(1); id <= 8; id++ {
				n := currentNodeRow(id, rowSpec{tf: 1})
				if id%2 == 0 {
					n = types.NewNode(currentNodeID(id), 2, nil)
				}
				if err := st.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}
			for id := int64(20); id < 26; id++ {
				r := types.NewRelationship(currentRelID(id), 5+uint16(id%2), currentNodeID(id%8+1), currentNodeID((id+1)%8+1))
				if err := st.PutRelationship(r); err != nil {
					t.Fatal(err)
				}
			}
			check("written")
			if err := st.DeleteRelationship(currentRelID(21)); err != nil {
				t.Fatal(err)
			}
			if err := st.DeleteRelationship(currentRelID(23)); err != nil {
				t.Fatal(err)
			}
			if err := st.DeleteNode(currentNodeID(4)); err != nil {
				t.Fatal(err)
			}
			check("deleted, unflushed")
			if f, ok := st.(interface{ Flush() error }); ok {
				if err := f.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			check("flushed")
			if _, err := m.NodeIDsByLabel(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
				t.Fatalf("NodeIDsByLabel(0) = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := m.RelIDsByType(0); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
				t.Fatalf("RelIDsByType(0) = %v, want ErrInvalidStoreMutation", err)
			}
			if err := st.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := m.NodeIDsByLabel(1); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed NodeIDsByLabel = %v, want ErrStoreClosed", err)
			}
			if _, err := m.RelIDsByType(5); !errors.Is(err, storecontract.ErrStoreClosed) {
				t.Fatalf("closed RelIDsByType = %v, want ErrStoreClosed", err)
			}
		})
	}
	var nb *badger.Store
	if _, err := nb.NodeIDsByLabel(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil badger NodeIDsByLabel = %v", err)
	}
	var ns *sharded.Store
	if _, err := ns.RelIDsByType(1); !errors.Is(err, storecontract.ErrNilStore) {
		t.Fatalf("nil sharded RelIDsByType = %v", err)
	}
}
