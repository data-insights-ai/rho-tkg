package graph_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	tieredpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Tiered-store adversarial battery at the graph level: forced rotations
// fragment event entities across shards, relationships span the
// reference/event boundary, archive/restore moves rows between shards — and
// after ALL of it, every query door must answer exactly as a single-shard
// graph would, hash chains must verify, and RunRepair must report ZERO
// fixes (a repair that "fixes" a healthy graph is corrupting it).
func newTieredGraph(t *testing.T) *graphpkg.Graph {
	t.Helper()
	ts, err := tieredpkg.New(tieredpkg.Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 12, Store: ts})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	t.Cleanup(func() { g.Close() })
	return g
}

func TestTieredRotationCrossShardQueriesAndRepairNoFalsePositives(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := newTieredGraph(t)

	// Reference node + first wave of events, cross-shard rels ref→event.
	caseNode, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{"who": "case"})
	if err != nil {
		t.Fatalf("case: %v", err)
	}
	wave1 := make([]*types.Node, 3)
	for i := range wave1 {
		ev, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{
			"who": fmt.Sprintf("w1-%d", i), "tkg_valid_from": types.Instant(1000 + i),
		})
		if err != nil {
			t.Fatalf("wave1: %v", err)
		}
		wave1[i] = ev
		if _, err := g.Rels().Add(ctx, "EMITS", caseNode, ev, nil); err != nil {
			t.Fatalf("rel w1: %v", err)
		}
	}

	// Force a rotation: wave1's shard goes warm; wave2 lands on a new hot shard.
	if err := g.Tier().ForceRotate(); err != nil {
		t.Fatalf("ForceRotate: %v", err)
	}
	wave2 := make([]*types.Node, 3)
	for i := range wave2 {
		ev, err := g.Nodes().Add(ctx, []string{"Signal"}, map[string]any{
			"who": fmt.Sprintf("w2-%d", i),
		})
		if err != nil {
			t.Fatalf("wave2: %v", err)
		}
		wave2[i] = ev
		if _, err := g.Rels().Add(ctx, "EMITS", caseNode, ev, nil); err != nil {
			t.Fatalf("rel w2: %v", err)
		}
	}
	// Event→event rel ACROSS the rotation boundary (old shard ↔ new shard).
	if _, err := g.Rels().Add(ctx, "FOLLOWS", wave2[0], wave1[0], nil); err != nil {
		t.Fatalf("cross-rotation rel: %v", err)
	}

	// Mutate an OLD-shard event after rotation (writes route by ID timestamp,
	// not to the hot shard).
	if err := g.Nodes().SetProperty(ctx, wave1[1].ID(), "post-rotation", true); err != nil {
		t.Fatalf("mutate old-shard event after rotation: %v", err)
	}
	// Delete an old-shard event; its history must stay queryable.
	if err := g.Nodes().Delete(ctx, wave1[2].ID()); err != nil {
		t.Fatalf("delete old-shard event: %v", err)
	}

	// --- Query doors across shards. ---
	rows, err := g.Nodes().ByLabel("Signal", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("ByLabel: %v", err)
	}
	if len(rows) != 5 { // 6 created − 1 deleted
		t.Fatalf("ByLabel across shards = %d rows, want 5", len(rows))
	}
	// Adjacency from the ref node must span BOTH event shards.
	out, err := g.Rels().Outgoing(caseNode.ID(), "EMITS")
	if err != nil {
		t.Fatalf("Outgoing: %v", err)
	}
	if len(out) != 5 { // 6 − 1 cascade-deleted with wave1[2]
		t.Fatalf("cross-shard adjacency = %d, want 5", len(out))
	}
	// Old-shard point read sees the post-rotation mutation.
	got, err := g.Nodes().Get(ctx, wave1[1].ID())
	if err != nil {
		t.Fatalf("Get old-shard: %v", err)
	}
	if _, ok := got.GetProperty("post-rotation"); !ok {
		t.Fatalf("post-rotation mutation lost — write routed to the wrong shard")
	}
	// Deleted old-shard event: history queryable (B32 across shards).
	if _, err := g.Temporal().NodeAt(wave1[2].ID(), 1500); err != nil {
		t.Fatalf("deleted event history not queryable across shards: %v", err)
	}
	// Hash chains verify across shard boundaries (incl. the deleted one).
	for i, ev := range wave1 {
		valid, err := g.Hash().VerifyNodeChain(ev.ID())
		if err != nil || !valid {
			t.Fatalf("wave1[%d] chain: valid=%v err=%v", i, valid, err)
		}
	}

	// --- Repair must find NOTHING to fix on a healthy graph. ---
	res, err := g.Tier().Repair()
	if err != nil {
		t.Fatalf("Repair: %v", err)
	}
	if res.OrphanedInEntries != 0 || res.StaleInEntries != 0 || res.MissingInEntries != 0 {
		t.Fatalf("repair 'fixed' a healthy cross-shard graph: %+v — false positives corrupt good data", res)
	}
	if res.CrossShardRelsChecked == 0 {
		t.Fatalf("repair checked zero cross-shard rels despite cross-shard topology — the scan is blind")
	}

	// Repair is idempotent on the second pass too.
	res2, err := g.Tier().Repair()
	if err != nil || res2.OrphanedInEntries+res2.StaleInEntries+res2.MissingInEntries != 0 {
		t.Fatalf("second repair not clean: %+v err=%v", res2, err)
	}
}

func TestTieredArchiveRestoreRoundTripUnderCrossShardRels(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := newTieredGraph(t)

	caseNode, err := g.Nodes().Add(ctx, []string{"Case"}, map[string]any{
		"who": "archived-case", "tkg_valid_from": types.Instant(1000),
	})
	if err != nil {
		t.Fatalf("case: %v", err)
	}
	if _, err := g.Nodes().Update(ctx, caseNode.ID(), map[string]any{"state": "updated"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	ev, err := g.Nodes().Add(ctx, []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("event: %v", err)
	}
	// Cross-shard rel that archive/restore must migrate consistently.
	rel, err := g.Rels().Add(ctx, "EMITS", caseNode, ev, nil)
	if err != nil {
		t.Fatalf("rel: %v", err)
	}

	if err := g.Tier().Archive(caseNode.ID()); err != nil {
		t.Fatalf("Archive: %v", err)
	}
	// Archived node must remain fully readable: point read, history,
	// temporal resolution, adjacency, hash chain.
	got, err := g.Nodes().Get(ctx, caseNode.ID())
	if err != nil {
		t.Fatalf("Get archived: %v", err)
	}
	if s, _ := got.GetProperty("state"); s != "updated" {
		t.Fatalf("archived node lost its current state: %v", s)
	}
	if _, err := g.Temporal().NodeAt(caseNode.ID(), 1500); err != nil {
		t.Fatalf("archived node temporal resolution: %v", err)
	}
	out, err := g.Rels().Outgoing(caseNode.ID(), "EMITS")
	if err != nil || len(out) != 1 || out[0].ID() != rel.ID() {
		t.Fatalf("archived node adjacency broken: %v (%d)", err, len(out))
	}
	valid, err := g.Hash().VerifyNodeChain(caseNode.ID())
	if err != nil || !valid {
		t.Fatalf("archived chain: valid=%v err=%v", valid, err)
	}

	// Archiving a NON-reference entity must fail closed with the documented
	// sentinel — never silently move an event row.
	if err := g.Tier().Archive(ev.ID()); !errors.Is(err, tieredpkg.ErrNotReferenceEntity) {
		t.Fatalf("Archive(event) = %v, want ErrNotReferenceEntity", err)
	}

	if err := g.Tier().Restore(caseNode.ID()); err != nil {
		t.Fatalf("Restore: %v", err)
	}
	got, err = g.Nodes().Get(ctx, caseNode.ID())
	if err != nil {
		t.Fatalf("Get restored: %v", err)
	}
	if s, _ := got.GetProperty("state"); s != "updated" {
		t.Fatalf("restored node lost state: %v", s)
	}
	hist, err := g.Nodes().History(caseNode.ID())
	if err != nil || len(hist) == 0 {
		t.Fatalf("restored node lost history: %v (%d)", err, len(hist))
	}
	valid, err = g.Hash().VerifyNodeChain(caseNode.ID())
	if err != nil || !valid {
		t.Fatalf("restored chain: valid=%v err=%v", valid, err)
	}
	// And repair still finds nothing after the round trip.
	res, err := g.Tier().Repair()
	if err != nil || res.OrphanedInEntries+res.StaleInEntries+res.MissingInEntries != 0 {
		t.Fatalf("repair after archive/restore: %+v err=%v", res, err)
	}
}

// The primary-label class is the routing key: flipping a reference node to
// an event class (or vice versa) through ANY label door would fragment its
// version chain across shards. Every door must reject it with the documented
// sentinel.
func TestTieredPrimaryLabelClassMutationRejectedAllDoors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	g := newTieredGraph(t)

	caseNode, err := g.Nodes().Add(ctx, []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("case: %v", err)
	}
	ev, err := g.Nodes().Add(ctx, []string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("event: %v", err)
	}

	// Removing the reference primary in favour of an event label must fail.
	if err := g.Nodes().AddLabel(ctx, caseNode.ID(), "Signal"); err == nil {
		// Adding alone may be legal (primary unchanged); the flip is the
		// REMOVAL of the reference primary.
		err = g.Nodes().RemoveLabel(ctx, caseNode.ID(), "Case")
		if !errors.Is(err, tieredpkg.ErrPrimaryLabelClassMutation) {
			t.Fatalf("reference→event flip = %v, want ErrPrimaryLabelClassMutation", err)
		}
	}
	// Event→reference flip likewise.
	if err := g.Nodes().AddLabel(ctx, ev.ID(), "Case"); err == nil {
		err = g.Nodes().RemoveLabel(ctx, ev.ID(), "Signal")
		if !errors.Is(err, tieredpkg.ErrPrimaryLabelClassMutation) {
			t.Fatalf("event→reference flip = %v, want ErrPrimaryLabelClassMutation", err)
		}
	}
	// Whatever happened above, both nodes must still be readable and intact.
	if _, err := g.Nodes().Get(ctx, caseNode.ID()); err != nil {
		t.Fatalf("case unreadable after rejected flip: %v", err)
	}
	if _, err := g.Nodes().Get(ctx, ev.ID()); err != nil {
		t.Fatalf("event unreadable after rejected flip: %v", err)
	}
}
