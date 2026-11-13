package badger

import (
	"context"
	"errors"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReadBudget_DecodedBytesChargedBeforeDecode (READBUDGET review, 13 Nov):
// the decoded-byte charge comes before the decode, not after. The proof is a
// corrupt current row: unbudgeted, every metered door fails to decode it;
// under a 1-byte budget each must return ErrReadBudget instead, which it can
// only do if it charged the row before handing it to the decoder. A charge
// moved below the decode turns the budget-1 answer into the decode error.
func TestReadBudget_DecodedBytesChargedBeforeDecode(t *testing.T) {
	t.Parallel()
	corrupt := []byte{0x85, 0xc1, 0xc1, 0xc1}
	nid := types.NodeID(snowflake.ID(curID + 10))
	rid := types.RelID(snowflake.ID(curID + 10))
	doors := []struct {
		name string
		kind int // index into curKinds: 0 node, 1 rel
		read func(bs *Store, m *store.ReadMeter) error
	}{
		{"LendNodeMetered", 0, func(bs *Store, m *store.ReadMeter) error {
			_, err := bs.LendNodeMetered(nid, m.Tally("d"))
			return err
		}},
		{"GetNodesByIDsMetered", 0, func(bs *Store, m *store.ReadMeter) error {
			_, err := bs.GetNodesByIDsMetered([]types.NodeID{nid}, m.Tally("d"))
			return err
		}},
		{"LendNodesSortedMetered", 0, func(bs *Store, m *store.ReadMeter) error {
			return bs.LendNodesSortedMetered([]types.NodeID{nid}, make([]*types.Node, 1), m)
		}},
		{"LendRelationshipMetered", 1, func(bs *Store, m *store.ReadMeter) error {
			_, err := bs.LendRelationshipMetered(rid, m.Tally("d"))
			return err
		}},
		{"LendRelationshipsSortedMetered", 1, func(bs *Store, m *store.ReadMeter) error {
			return bs.LendRelationshipsSortedMetered([]types.RelID{rid}, make([]*types.Relationship, 1), m)
		}},
	}
	kinds := curKinds()
	for _, d := range doors {
		t.Run(d.name, func(t *testing.T) {
			k := kinds[d.kind]
			bs := newCurStore(t, Config{})
			if err := k.put(bs, 10, 1, 100, 0, 0); err != nil {
				t.Fatal(err)
			}
			writeRawRow(t, k, bs, 10, corrupt)

			_, open := store.WithReadBudget(context.Background(), store.ReadBudget{})
			err := d.read(bs, open)
			if err == nil || errors.Is(err, store.ErrReadBudget) {
				t.Fatalf("unbudgeted read of a corrupt row = %v; want a decode error", err)
			}
			if open.Work().DecodedBytes != uint64(len(corrupt)) {
				t.Fatalf("unbudgeted: counted %+v; want the row's %d bytes", open.Work(), len(corrupt))
			}

			k.evict(bs, 10)
			_, one := store.WithReadBudget(context.Background(), store.ReadBudget{DecodedBytes: 1})
			err = d.read(bs, one)
			var be *store.ReadBudgetError
			if !errors.As(err, &be) || be.Limit != "decoded bytes" {
				t.Fatalf("budget 1 over a corrupt row = %v; want ErrReadBudget \"decoded bytes\" (charged before the decode)", err)
			}
		})
	}
}
