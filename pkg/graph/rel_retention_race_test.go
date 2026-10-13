package graph_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestRelRetentionPurgeTxRollbackNoResurrection (backlog 21): a transaction
// that touched an aged-out relationship and then rolls back must not bring it
// back after a relationship purge. Rollback restores the transaction's
// pre-images (the relaxed v4 isolation overwrites a standalone write that
// landed in between), so the purge waits for an open transaction instead of
// removing a row the rollback would restore. Faulty implementation caught: a
// purge chunk that runs while a transaction holds a pre-image of its victim.
func TestRelRetentionPurgeTxRollbackNoResurrection(t *testing.T) {
	ctx := context.Background()
	for _, be := range relRetentionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, _ := be.open(t, graphpkg.Config{AllowRetentionPurge: true})
			a, _ := g.Nodes().Add(ctx, []string{"Machine"}, nil)
			b, _ := g.Nodes().Add(ctx, []string{"Event"}, nil)
			r, err := g.Rels().Add(ctx, "EVENT", a, b, map[string]any{"v": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateRelationship(r.ID(), map[string]any{"v": int64(2)}); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := g.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: farFuture})
				done <- err
			}()
			select {
			case err := <-done:
				// The purge ran under the open transaction (the bug); the
				// rollback below then shows whether it resurrects the row.
				if err != nil {
					t.Fatal(err)
				}
				done <- nil
			case <-time.After(100 * time.Millisecond):
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback of a transaction that touched a purge victim: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Get(ctx, r.ID()); !errors.Is(err, graphpkg.ErrRelNotFound) {
				t.Fatalf("purged relationship after the rollback: err = %v, want ErrRelNotFound", err)
			}
			if has, err := g.Rels().HasHistory(r.ID()); err != nil || has {
				t.Fatalf("purged relationship history after the rollback: %v, %v", has, err)
			}
		})
	}
}

// TestNodeRetentionPurgeTxRollback (backlog 21, found on the way): the node
// purge had the same gap — it ran under an open transaction that updated its
// victim and the Rollback then failed on the purged node. It now waits for
// the transaction like the relationship purge.
func TestNodeRetentionPurgeTxRollback(t *testing.T) {
	ctx := context.Background()
	for _, be := range relRetentionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, _ := be.open(t, graphpkg.Config{AllowRetentionPurge: true})
			n, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"v": int64(1)})
			if err != nil {
				t.Fatal(err)
			}
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateNode(n.ID(), map[string]any{"v": int64(2)}); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture})
				done <- err
			}()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
				done <- nil
			case <-time.After(100 * time.Millisecond):
			}
			if err := tx.Rollback(); err != nil {
				t.Fatalf("Rollback of a transaction that touched a purge victim: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Get(ctx, n.ID()); !errors.Is(err, graphpkg.ErrNodeNotFound) {
				t.Fatalf("purged node after the rollback: err = %v, want ErrNodeNotFound", err)
			}
		})
	}
}

// TestRelRetentionPurgeConcurrent (backlog 21, run under -race): a purge of
// the old EVENT relationships while writers add and delete new ones and
// readers scan both types. Afterwards no relationship minted below the
// boundary is left, every new one is answered, and the count agrees with the
// scan. Faulty implementations caught: a data race in the store purge or the
// watermark advance, a purge that takes a relationship minted after Before.
func TestRelRetentionPurgeConcurrent(t *testing.T) {
	ctx := context.Background()
	for _, be := range relRetentionBackends() {
		t.Run(be.name, func(t *testing.T) {
			g, _ := be.open(t, graphpkg.Config{AllowRetentionPurge: true, AllowTxBackfill: true})
			m, _ := g.Nodes().Add(ctx, []string{"Machine"}, nil)
			e, _ := g.Nodes().Add(ctx, []string{"Event"}, nil)
			ov, _ := g.Nodes().Add(ctx, []string{"Overview"}, nil)
			if _, err := g.Rels().Add(ctx, "OV_PAIR", ov, m, nil); err != nil {
				t.Fatal(err)
			}
			var old []types.RelID
			for i := 0; i < 300; i++ {
				r, err := g.Rels().Add(ctx, "EVENT", e, m, map[string]any{"i": int64(i)})
				if err != nil {
					t.Fatal(err)
				}
				old = append(old, r.ID())
			}
			for hi, _ := g.Temporal().NowTx(); types.Instant(time.Now().UnixMilli()) <= hi+1; {
				time.Sleep(time.Millisecond)
			}
			before := types.Instant(time.Now().UnixMilli())
			for types.Instant(time.Now().UnixMilli()) <= before {
				time.Sleep(time.Millisecond)
			}

			var wg sync.WaitGroup
			stop := make(chan struct{})
			var mu sync.Mutex
			var added []types.RelID
			var deleted = map[types.RelID]bool{}
			wg.Add(3)
			go func() { // writer
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					r, err := g.Rels().Add(ctx, "EVENT", e, m, nil)
					if err != nil {
						t.Error(err)
						return
					}
					mu.Lock()
					added = append(added, r.ID())
					mu.Unlock()
					if i%3 == 0 {
						now, _ := g.Temporal().NowTx()
						if err := g.Rels().DeleteWithTx(ctx, r.ID(), now); err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						deleted[r.ID()] = true
						mu.Unlock()
					}
				}
			}()
			go func() { // EVENT reader at the present
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if _, err := g.Rels().ByType("EVENT", store.QueryOpts{}); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			go func() { // OV_PAIR reader below the EVENT boundary: never refused
				defer wg.Done()
				for {
					select {
					case <-stop:
						return
					default:
					}
					if _, err := g.Rels().ByType("OV_PAIR", store.QueryOpts{TxPin: before - 1}); err != nil {
						t.Error(err)
						return
					}
				}
			}()
			rep, err := g.Admin().PurgeExpiredRels(ctx, adminpkg.RelPurgePolicy{Type: "EVENT", Before: before})
			close(stop)
			wg.Wait()
			if err != nil {
				t.Fatal(err)
			}
			if rep.RelsPurged != len(old) {
				t.Fatalf("RelsPurged = %d, want %d", rep.RelsPurged, len(old))
			}
			for _, id := range old {
				if _, err := g.Rels().Get(ctx, id); !errors.Is(err, graphpkg.ErrRelNotFound) {
					t.Fatalf("old relationship %d survived: %v", id, err)
				}
			}
			live := relSetOf(t)(g.Rels().ByType("EVENT", store.QueryOpts{}))
			for _, id := range added {
				_, got := live[id]
				if got == deleted[id] {
					t.Fatalf("new relationship %d: live=%v, deleted=%v", id, got, deleted[id])
				}
			}
			if n, err := g.Rels().CountByType("EVENT"); err != nil || n != len(live) {
				t.Fatalf("CountByType = %d, %v; scan holds %d", n, err, len(live))
			}
		})
	}
}
