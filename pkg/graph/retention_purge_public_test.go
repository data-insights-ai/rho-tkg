package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	adminpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/admin"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// farFuture is safely above every 2026-epoch snowflake mint-instant, so a purge
// with this boundary selects every currently-minted node.
const farFuture = types.Instant(1 << 50)

// A partial placement removal can leave history and an incoming leg without
// the current relationship. Purging its endpoint must erase that history too,
// while preserving an unrelated relationship on the same history owner.
func TestPurgeExpiredNodesErasesOrphanRelationshipHistory(t *testing.T) {
	for _, be := range relRetentionBackends() {
		if be.name == "memory" {
			continue
		}
		t.Run(be.name, func(t *testing.T) {
			g, st := be.open(t, graphpkg.Config{AllowRetentionPurge: true})
			addNode := func(label string) *types.Node {
				n, err := g.Nodes().Add(t.Context(), []string{label}, nil)
				if err != nil {
					t.Fatal(err)
				}
				return n
			}
			survivor, victim, foreignEnd := addNode("Machine"), addNode("Event"), addNode("Machine")
			// Put the historical pin strictly above the victim's mint instant;
			// the post-purge read must test absence, not the watermark guard.
			time.Sleep(2 * time.Millisecond)
			addRel := func(end types.NodeID) *types.Relationship {
				r, err := g.Rels().AddByID(t.Context(), "ON", survivor.ID(), end, map[string]any{"v": int64(0)})
				if err != nil {
					t.Fatal(err)
				}
				return r
			}
			target, foreign := addRel(victim.ID()), addRel(foreignEnd.ID())
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			for _, r := range []*types.Relationship{target, foreign} {
				if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"v": int64(1)}); err != nil {
					t.Fatal(err)
				}
			}
			flush := func() {
				if err := st.(interface{ Flush() error }).Flush(); err != nil {
					t.Fatal(err)
				}
			}
			flush()
			var owner, victimOwner *badger.Store
			switch s := st.(type) {
			case *badger.Store:
				owner, victimOwner = s, s
			case *sharded.Store:
				owner, err = s.ShardForIDForTest(target.ID().SnowflakeID())
				if err == nil {
					victimOwner, err = s.ShardForIDForTest(victim.ID().SnowflakeID())
				}
			case *tiered.Store:
				var checkin func()
				owner, checkin, err = s.ShardForRelIDCheckedForTest(target.ID())
				if checkin != nil {
					defer checkin()
				}
				if err == nil {
					victimOwner, err = s.ShardForNodeIDForTest(victim.ID())
				}
			}
			if err != nil || owner == nil || victimOwner == nil {
				t.Fatalf("fixture owner lookup: %v", err)
			}
			if be.name != "badger" && owner == victimOwner {
				t.Fatal("fixture must exercise cross-shard orphan cleanup")
			}
			if _, err := owner.RemoveRelEntityAndOutPlacement(target.ID().SnowflakeID()); err != nil {
				t.Fatal(err)
			}
			flush()
			assertOld := func(id types.RelID) {
				t.Helper()
				r, err := g.Temporal().RelAsOf(id, pin)
				if err != nil || r == nil || r.ID() != id || r.Version() != 0 {
					t.Fatalf("historical control %d: %v, %v", id, r, err)
				}
				if value, ok := r.GetProperty("v"); !ok || value != int64(0) {
					t.Fatalf("historical control property: %v, %v", value, ok)
				}
			}
			assertOld(target.ID())
			assertOld(foreign.ID())
			result, err := g.Admin().PurgeExpiredNodes(t.Context(), adminpkg.PurgePolicy{Label: "Event", Before: pin})
			if err != nil || result.NodesPurged != 1 {
				t.Fatalf("purge: %+v, %v", result, err)
			}
			flush()
			if rows, err := g.Rels().History(target.ID()); err != nil || len(rows) != 0 {
				t.Errorf("orphan history survived endpoint purge: %d rows, %v", len(rows), err)
			}
			if row, err := g.Temporal().RelAsOf(target.ID(), pin); row != nil || !errors.Is(err, graphpkg.ErrNoVersionAsOf) {
				t.Errorf("purged old pin still answers: %v, %v", row, err)
			}
			assertOld(foreign.ID())
			if rows, err := g.Rels().History(foreign.ID()); err != nil || len(rows) != 1 || rows[0].Version() != 0 {
				t.Fatalf("foreign history changed: %v, %v", rows, err)
			}
		})
	}
}

// TestPurgeExpiredNodes_Gates covers the door's refusal contract.
func TestPurgeExpiredNodes_Gates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Gate 1: disabled by default.
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if _, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture}); !errors.Is(err, graphpkg.ErrRetentionPurgeDisabled) {
		t.Fatalf("disabled gate err=%v, want ErrRetentionPurgeDisabled", err)
	}

	// Gate 2: enabled but invalid policy.
	ge, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New enabled: %v", err)
	}
	defer ge.Close()
	if _, err := ge.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Before: farFuture}); !errors.Is(err, graphpkg.ErrInvalidPurgePolicy) {
		t.Fatalf("empty-label err=%v, want ErrInvalidPurgePolicy", err)
	}
	if _, err := ge.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: 0}); !errors.Is(err, graphpkg.ErrInvalidPurgePolicy) {
		t.Fatalf("zero-before err=%v, want ErrInvalidPurgePolicy", err)
	}

	// With a change-log enabled, the purge SUCCEEDS (R3) — the native badger store
	// also implements RangePurgeLogCapability, so it emits the predicate record
	// instead of refusing. (The ErrRetentionPurgeChangeLogEnabled refusal now only
	// guards a hypothetical change-log store lacking the log capability.)
	gc, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true, BadgerInMemory: true, ChangeLog: true})
	if err != nil {
		t.Fatalf("New changelog: %v", err)
	}
	defer gc.Close()
	if _, err := gc.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture}); err != nil {
		t.Fatalf("change-log purge should succeed under R3, got %v", err)
	}
}

// TestPurgeExpiredNodes_EndToEnd proves the happy path: below-boundary nodes of
// the target label are hard-removed, other labels survive, the per-label
// retention watermark advances so a temporal read pinned below the boundary fails
// closed with ErrRetentionExpired (the R1 guard, now fired by R2), and a re-run is
// an idempotent no-op.
func TestPurgeExpiredNodes_EndToEnd(t *testing.T) {
	ctx := context.Background()
	g, err := graphpkg.New(graphpkg.Config{AllowRetentionPurge: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	// 5 Event nodes (some with edges to a surviving Machine) + 2 Machine nodes.
	machine, err := g.Nodes().Add(ctx, []string{"Machine"}, map[string]any{"host": "h1"})
	if err != nil {
		t.Fatalf("add machine: %v", err)
	}
	machine2, _ := g.Nodes().Add(ctx, []string{"Machine"}, nil)
	for i := 0; i < 5; i++ {
		e, err := g.Nodes().Add(ctx, []string{"Event"}, map[string]any{"seq": int64(i)})
		if err != nil {
			t.Fatalf("add event: %v", err)
		}
		if _, err := g.Rels().AddByID(ctx, "ON", e.ID(), machine.ID(), nil); err != nil {
			t.Fatalf("add edge: %v", err)
		}
	}

	res, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Mode: adminpkg.PurgeByAge, Before: farFuture})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if res.NodesPurged != 5 {
		t.Fatalf("purged %d Event nodes, want 5", res.NodesPurged)
	}
	if res.RelsPurged != 5 {
		t.Fatalf("purged %d edges, want 5", res.RelsPurged)
	}

	// Event nodes are gone; Machine nodes survive with no phantom incoming edges.
	events, err := g.Nodes().ByLabel("Event", store.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel Event: %v", err)
	}
	if len(events) != 0 {
		t.Fatalf("%d Event nodes survived purge, want 0", len(events))
	}
	machines, _ := g.Nodes().ByLabel("Machine", store.QueryOpts{})
	if len(machines) != 2 {
		t.Fatalf("%d Machine nodes, want 2 (survivors)", len(machines))
	}
	if in, _ := g.Rels().Incoming(machine.ID(), "ON"); len(in) != 0 {
		t.Fatalf("machine has %d phantom incoming edges, want 0", len(in))
	}
	_ = machine2

	// The per-label watermark advanced: a temporal scan pinned below the boundary
	// now fails closed rather than returning a silently-incomplete set.
	if _, err := g.Temporal().NodesAsOf(farFuture - 1); !errors.Is(err, graphpkg.ErrRetentionExpired) {
		t.Fatalf("NodesAsOf below watermark err=%v, want ErrRetentionExpired", err)
	}

	// Idempotent: purging again removes nothing.
	res2, err := g.Admin().PurgeExpiredNodes(ctx, adminpkg.PurgePolicy{Label: "Event", Before: farFuture})
	if err != nil {
		t.Fatalf("re-purge: %v", err)
	}
	if res2.NodesPurged != 0 || res2.RelsPurged != 0 {
		t.Fatalf("re-purge removed %+v, want zero", res2)
	}
}
