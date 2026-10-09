package graph_test

import (
	"context"
	"errors"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestReImportContinuesTheChainFacade — backlog 38 through the public API on
// every backend, node and relationship: a re-import of a deleted ID starts one
// above the earlier life's top version and links its PrevHash to that row; an
// Update then keeps every earlier history row (versions 0, 1 and 2 all
// stored); the chain verifies; a backfilled re-import whose tkg_tx_from is at
// or below the delete is refused with errors.Is(graph.ErrTxOrder) and
// errors.Is(graph.ErrInvalidTxFrom) at the public layer (rule 4) and writes
// nothing.
func TestReImportContinuesTheChainFacade(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, b := range allStoreBackendsWith(func(c *graphpkg.Config) { c.AllowTxBackfill = true }) {
		t.Run(b.name, func(t *testing.T) {
			g := b.open(t)
			s, err := g.Nodes().Add(ctx, []string{"Ref"}, map[string]any{"k": "s"})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			e, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"k": "e"})
			if err != nil {
				t.Fatalf("Add: %v", err)
			}
			type kind struct {
				name     string
				add      func() (id int64, err error)
				update   func(id int64) error
				del      func(id int64) error
				reimport func(id int64, props map[string]any) (v uint32, prev string, err error)
				history  func(id int64) ([]uint32, []string, error)
				verify   func(id int64) (bool, error)
			}
			kinds := []kind{
				{
					name: "node",
					add: func() (int64, error) {
						n, err := g.Nodes().Add(ctx, []string{"Ev"}, map[string]any{"w": int64(1)})
						if err != nil {
							return 0, err
						}
						return int64(n.ID()), nil
					},
					update: func(id int64) error {
						_, err := g.Nodes().Update(ctx, types.NodeID(id), map[string]any{"w": int64(2)})
						return err
					},
					del: func(id int64) error { return g.Nodes().Delete(ctx, types.NodeID(id)) },
					reimport: func(id int64, props map[string]any) (uint32, string, error) {
						n, err := g.Nodes().Import(ctx, types.NodeID(id), []string{"Ev"}, props)
						if err != nil {
							return 0, "", err
						}
						return n.Version(), n.Integrity().PrevHash, nil
					},
					history: func(id int64) ([]uint32, []string, error) {
						hs, err := g.Nodes().History(types.NodeID(id))
						var vs []uint32
						var hashes []string
						for _, h := range hs {
							vs = append(vs, h.Version())
							hashes = append(hashes, h.Integrity().Hash)
						}
						return vs, hashes, err
					},
					verify: func(id int64) (bool, error) { return g.Hash().VerifyNodeChain(types.NodeID(id)) },
				},
				{
					name: "rel",
					add: func() (int64, error) {
						r, err := g.Rels().Add(ctx, "LINK", s, e, map[string]any{"w": int64(1)})
						if err != nil {
							return 0, err
						}
						return int64(r.ID()), nil
					},
					update: func(id int64) error {
						_, err := g.Rels().Update(ctx, types.RelID(id), map[string]any{"w": int64(2)})
						return err
					},
					del: func(id int64) error { return g.Rels().Delete(ctx, types.RelID(id)) },
					reimport: func(id int64, props map[string]any) (uint32, string, error) {
						r, err := g.Rels().Import(ctx, types.RelID(id), "LINK", s, e, props)
						if err != nil {
							return 0, "", err
						}
						return r.Version(), r.Integrity().PrevHash, nil
					},
					history: func(id int64) ([]uint32, []string, error) {
						hs, err := g.Rels().History(types.RelID(id))
						var vs []uint32
						var hashes []string
						for _, h := range hs {
							vs = append(vs, h.Version())
							hashes = append(hashes, h.Integrity().Hash)
						}
						return vs, hashes, err
					},
					verify: func(id int64) (bool, error) { return g.Hash().VerifyRelChain(types.RelID(id)) },
				},
			}
			for _, k := range kinds {
				id, err := k.add()
				if err != nil {
					t.Fatalf("%s add: %v", k.name, err)
				}
				if err := k.update(id); err != nil {
					t.Fatalf("%s update: %v", k.name, err)
				}
				if err := k.del(id); err != nil {
					t.Fatalf("%s delete: %v", k.name, err)
				}
				vs, hashes, err := k.history(id)
				if err != nil || len(vs) != 2 {
					t.Fatalf("%s history %v, %v; want versions 0 and 1", k.name, vs, err)
				}
				tomb, err := g.Temporal().NowTx() // above the delete
				if err != nil {
					t.Fatalf("NowTx: %v", err)
				}
				var d types.Instant
				if k.name == "node" {
					hs, _ := g.Nodes().History(types.NodeID(id))
					d = hs[len(hs)-1].Temporal().DeletedAt
				} else {
					hs, _ := g.Rels().History(types.RelID(id))
					d = hs[len(hs)-1].Temporal().DeletedAt
				}
				if d == 0 || d >= tomb {
					t.Fatalf("%s fixture: delete stamp %d, pin %d", k.name, d, tomb)
				}
				for _, at := range []types.Instant{d - 1, d} {
					_, _, err := k.reimport(id, map[string]any{"tkg_tx_from": at, "w": int64(9)})
					if !errors.Is(err, graphpkg.ErrTxOrder) || !errors.Is(err, graphpkg.ErrInvalidTxFrom) {
						t.Fatalf("%s backfilled re-import at %d (delete %d): err = %v; want ErrTxOrder wrapping ErrInvalidTxFrom", k.name, at, d, err)
					}
					if vs2, _, _ := k.history(id); len(vs2) != 2 {
						t.Fatalf("%s refused re-import changed the history: %v", k.name, vs2)
					}
				}
				v, prev, err := k.reimport(id, map[string]any{"w": int64(9)})
				if err != nil {
					t.Fatalf("%s re-import: %v", k.name, err)
				}
				if v != 2 || prev != hashes[1] {
					t.Fatalf("%s re-import: version %d PrevHash %.12s; want 2 and the tombstone's hash %.12s", k.name, v, prev, hashes[1])
				}
				if err := k.update(id); err != nil {
					t.Fatalf("%s update after re-import: %v", k.name, err)
				}
				vs3, hashes3, err := k.history(id)
				if err != nil || len(vs3) != 3 || vs3[0] != 0 || vs3[1] != 1 || vs3[2] != 2 || hashes3[0] != hashes[0] || hashes3[1] != hashes[1] {
					t.Fatalf("%s history after update %v (%v); want versions 0, 1, 2 with the first life's rows unchanged", k.name, vs3, err)
				}
				if ok, err := k.verify(id); err != nil || !ok {
					t.Fatalf("%s Verify*Chain = %v, %v", k.name, ok, err)
				}
			}
		})
	}
}
