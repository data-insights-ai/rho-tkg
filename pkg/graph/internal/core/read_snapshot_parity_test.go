package core

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// snapshotReads is the read surface a store.ReadSnapshot shares with its store.
type snapshotReads interface {
	GetNode(types.NodeID) (*types.Node, error)
	HasNodeHistory(types.NodeID) (bool, error)
	NodeHistoryTemporalMeta(types.NodeID) ([]storepkg.VersionTemporalMeta, error)
	GetNodeVersion(types.NodeID, uint32) (*types.Node, error)
	GetNodeHistory(types.NodeID) ([]*types.Node, error)
	GetRelationship(types.RelID) (*types.Relationship, error)
	HasRelHistory(types.RelID) (bool, error)
	RelHistoryTemporalMeta(types.RelID) ([]storepkg.VersionTemporalMeta, error)
	GetRelVersion(types.RelID, uint32) (*types.Relationship, error)
}

// renderEntityReads renders every read of one node and one relationship.
func renderEntityReads(r snapshotReads, lend func(types.NodeID) (*types.Node, error), nid types.NodeID, rid types.RelID) string {
	var b strings.Builder
	row := func(v uint32, tm *types.TemporalMetadata, k any, err error) {
		if err != nil {
			fmt.Fprintf(&b, " err(%v)", err)
			return
		}
		if tm == nil {
			fmt.Fprintf(&b, " v%d k=%v", v, k)
			return
		}
		fmt.Fprintf(&b, " v%d tx[%d,%d) valid[%d,%d) del%d k=%v", v, tm.TxFrom, tm.TxTo, tm.ValidFrom, tm.ValidTo, tm.DeletedAt, k)
	}
	node := func(n *types.Node, err error) {
		if err != nil {
			row(0, nil, nil, err)
			return
		}
		k, _ := n.GetProperty("k")
		row(n.Version(), n.Temporal(), k, nil)
	}
	fmt.Fprintf(&b, "node %d:", nid)
	node(r.GetNode(nid))
	b.WriteString(" lend:")
	node(lend(nid))
	has, err := r.HasNodeHistory(nid)
	fmt.Fprintf(&b, " has=%v/%v meta:", has, err)
	metas, err := r.NodeHistoryTemporalMeta(nid)
	if err != nil {
		fmt.Fprintf(&b, " err(%v)", err)
	}
	for _, m := range metas {
		row(m.Version, m.Temporal, nil, nil)
	}
	for v := range uint32(4) {
		fmt.Fprintf(&b, " @v%d:", v)
		node(r.GetNodeVersion(nid, v))
	}
	hist, err := r.GetNodeHistory(nid)
	fmt.Fprintf(&b, " hist(%v):", err)
	for _, h := range hist {
		node(h, nil)
	}
	fmt.Fprintf(&b, "\nrel %d:", rid)
	rel := func(x *types.Relationship, err error) {
		if err != nil {
			row(0, nil, nil, err)
			return
		}
		k, _ := x.GetProperty("k")
		row(x.Version(), x.Temporal(), k, nil)
	}
	rel(r.GetRelationship(rid))
	has, err = r.HasRelHistory(rid)
	fmt.Fprintf(&b, " has=%v/%v meta:", has, err)
	rmetas, err := r.RelHistoryTemporalMeta(rid)
	if err != nil {
		fmt.Fprintf(&b, " err(%v)", err)
	}
	for _, m := range rmetas {
		row(m.Version, m.Temporal, nil, nil)
	}
	for v := range uint32(3) {
		fmt.Fprintf(&b, " @v%d:", v)
		rel(r.GetRelVersion(rid, v))
	}
	return b.String()
}

// TestReadSnapshotParityAcrossEntities pins row 121's snapshot reads on
// badger, sharded and tiered (the reference shard through one badger
// snapshot, event shards direct): ONE snapshot, kept across the reads of many
// entities in ascending ID order (the forward walk a scan does) and then in a
// shuffled order, answers every node and relationship read exactly as the
// store's method does, LendNode included, for entities with no history, an
// update, a valid-time rewrite, several updates and a delete. Mutation: a
// walk that skips past an entity's history rows (a wrong position after a
// prefix is read), a skeleton memo kept for the wrong entity, or a tiered
// read routed to the wrong shard changes an answer.
func TestReadSnapshotParityAcrossEntities(t *testing.T) {
	t.Parallel()
	stores := map[string]func(t *testing.T) storepkg.MandatoryStore{
		"badger": pageDoorStores["badger"],
		"sharded": func(t *testing.T) storepkg.MandatoryStore {
			st, err := sharded.New(sharded.Config{InMemory: true, BaseSlot: 0, SlotCount: 2})
			if err != nil {
				t.Fatal(err)
			}
			return st
		},
		"tiered": func(t *testing.T) storepkg.MandatoryStore {
			ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"D"}, ShardWindow: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			return ts
		},
	}
	for name, open := range stores {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			st := open(t)
			snaps, ok := st.(storepkg.ReadSnapshotCapability)
			if !ok {
				t.Fatalf("%s offers no read snapshot", name)
			}
			g, err := New(Config{Store: st})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = g.Close() })
			ctx := context.Background()
			var nodes []types.NodeID
			var rels []types.RelID
			var prev *types.Node
			for i := range 48 {
				label := "D"
				if i%3 == 0 {
					label = "Ev" // an event shard on tiered
				}
				n, err := g.Nodes.Add(ctx, []string{label}, map[string]any{"k": i, "tkg_valid_from": types.Instant(1000)})
				if err != nil {
					t.Fatal(err)
				}
				nodes = append(nodes, n.ID())
				if prev != nil {
					r, err := g.Rels.Add(ctx, "R", prev, n, map[string]any{"k": i, "tkg_valid_from": types.Instant(1000)})
					if err != nil {
						t.Fatal(err)
					}
					rels = append(rels, r.ID())
				}
				prev = n
			}
			for i, id := range nodes {
				var err error
				switch i % 6 {
				case 1:
					_, err = g.Nodes.Update(ctx, id, map[string]any{"k": -i})
				case 2:
					_, err = g.Temporal.SetNodeVersionInterval(ctx, id, 1000, 0, map[string]any{"k": 100 + i})
				case 3:
					for u := range 3 {
						if _, err = g.Nodes.Update(ctx, id, map[string]any{"k": 1000*u + i}); err != nil {
							break
						}
					}
				case 4:
					err = g.Nodes.Delete(ctx, id) // its relationships go too
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			for i, id := range rels {
				if i%4 == 1 {
					if _, err := g.Rels.Update(ctx, id, map[string]any{"k": -i}); err != nil && !strings.Contains(err.Error(), "not found") {
						t.Fatal(err)
					}
				}
			}
			if f, ok := st.(interface{ Flush() error }); ok {
				if err := f.Flush(); err != nil {
					t.Fatal(err)
				}
			}
			slices.Sort(nodes)
			lend := st.GetNode // LendNode answers what GetNode answers
			if lender, ok := st.(storepkg.EntityLendCapability); ok {
				lend = lender.LendNode
			}
			oracle, ok := st.(snapshotReads)
			if !ok {
				oracle = historyMetaFromRows{st} // sharded: the skeleton only on its snapshot
			}
			want := make(map[types.NodeID]string, len(nodes))
			for i, id := range nodes {
				want[id] = renderEntityReads(oracle, lend, id, rels[min(i, len(rels)-1)])
			}
			snap := snaps.NewReadSnapshot()
			defer snap.Close()
			snapLend := snap.(storepkg.ReadSnapshotLendCapability)
			orders := [][]types.NodeID{nodes, slices.Clone(nodes)}
			rand.New(rand.NewPCG(1, 2)).Shuffle(len(orders[1]), func(i, j int) { orders[1][i], orders[1][j] = orders[1][j], orders[1][i] })
			for o, order := range orders {
				for _, id := range order {
					i := slices.Index(nodes, id)
					if got := renderEntityReads(snap, snapLend.LendNode, id, rels[min(i, len(rels)-1)]); got != want[id] {
						t.Fatalf("order %d, node %d:\n snapshot %s\n    store %s", o, id, got, want[id])
					}
				}
				snap.Release()
			}
		})
	}
}

// historyMetaFromRows reads a store's history skeleton from its full history
// rows: the oracle for a store that offers the skeleton read only on its
// snapshot.
type historyMetaFromRows struct {
	storepkg.MandatoryStore
}

func (h historyMetaFromRows) HasNodeHistory(id types.NodeID) (bool, error) {
	return h.MandatoryStore.(storepkg.HistoryPresenceCapability).HasNodeHistory(id)
}

func (h historyMetaFromRows) HasRelHistory(id types.RelID) (bool, error) {
	return h.MandatoryStore.(storepkg.HistoryPresenceCapability).HasRelHistory(id)
}

func (h historyMetaFromRows) NodeHistoryTemporalMeta(id types.NodeID) ([]storepkg.VersionTemporalMeta, error) {
	rows, err := h.GetNodeHistory(id)
	if err != nil {
		return nil, err
	}
	var out []storepkg.VersionTemporalMeta
	for _, r := range rows {
		out = append(out, storepkg.VersionTemporalMeta{Version: r.Version(), Temporal: r.Temporal()})
	}
	return out, nil
}

func (h historyMetaFromRows) RelHistoryTemporalMeta(id types.RelID) ([]storepkg.VersionTemporalMeta, error) {
	rows, err := h.GetRelHistory(id)
	if err != nil {
		return nil, err
	}
	var out []storepkg.VersionTemporalMeta
	for _, r := range rows {
		out = append(out, storepkg.VersionTemporalMeta{Version: r.Version(), Temporal: r.Temporal()})
	}
	return out, nil
}
