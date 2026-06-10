package graph_test

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"testing"
	"time"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	temporalpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// Snapshot/Diff cross-door test: Snapshot(t) composes its node set, applies
// an endpoint-validity filter to rels, and Diff classifies entities across
// two instants — all of which COULD drift from the per-ID resolver
// (NodeAt/RelAt) it is supposed to agree with. Same adversarial dataset as
// the two-doors label test: explicit closed/open intervals, snowflake
// fallback, label churn, deletion with history, and rels whose endpoints
// die before the query instant.
func TestSnapshotAgreesWithPerIDResolver(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	for name, open := range frozenTestBackends(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			g := open(t)
			defer g.Close()

			a, _ := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{
				"tkg_valid_from": types.Instant(1000), "tkg_valid_to": types.Instant(2000), "who": "a"})
			b, _ := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{
				"tkg_valid_from": types.Instant(1500), "who": "b"})
			c, _ := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{"who": "c"})
			d, _ := g.Nodes().Add(ctx, []string{"Thing", "Keep"}, map[string]any{
				"tkg_valid_from": types.Instant(1000), "who": "d"})
			e, _ := g.Nodes().Add(ctx, []string{"Thing"}, map[string]any{
				"tkg_valid_from": types.Instant(1000), "who": "e"})
			if a == nil || b == nil || c == nil || d == nil || e == nil {
				t.Fatalf("setup failed")
			}
			// Rel inside A and B's shared validity window; A dies at VT 2000,
			// so at later instants the endpoint filter must drop it.
			if _, err := g.Rels().Add(ctx, "LINK", a, b, map[string]any{
				"tkg_valid_from": types.Instant(1600)}); err != nil {
				t.Fatalf("rel: %v", err)
			}
			if err := g.Nodes().RemoveLabel(ctx, d.ID(), "Thing"); err != nil {
				t.Fatalf("remove label: %v", err)
			}
			if err := g.Nodes().Delete(ctx, e.ID()); err != nil {
				t.Fatalf("delete: %v", err)
			}

			allNodes := []types.NodeID{a.ID(), b.ID(), c.ID(), d.ID(), e.ID()}
			now := types.Instant(time.Now().UnixMilli())

			for _, at := range []types.Instant{1200, 1700, now + 3_600_000} {
				snap, err := g.Temporal().Snapshot(at)
				if err != nil {
					t.Fatalf("Snapshot(%d): %v", at, err)
				}

				// Door 2: per-ID resolver over the same universe.
				var want []types.NodeID
				for _, id := range allNodes {
					if _, err := g.Temporal().NodeAt(id, at); err == nil {
						want = append(want, id)
					} else if !errors.Is(err, graphpkg.ErrNodeNotFound) && !errors.Is(err, storepkg.ErrNoVersionValidAt) {
						t.Fatalf("NodeAt(%v,%d): %v", id, at, err)
					}
				}
				got := make([]types.NodeID, 0, len(snap.Nodes))
				for _, n := range snap.Nodes {
					got = append(got, n.ID())
				}
				sortNodeIDs(got)
				sortNodeIDs(want)
				if fmt.Sprint(got) != fmt.Sprint(want) {
					t.Errorf("Snapshot(%d) nodes %v != per-ID resolver %v", at, got, want)
				}
				if snap.NodeCount != len(snap.Nodes) || snap.RelCount != len(snap.Relationships) {
					t.Errorf("Snapshot(%d) count fields disagree with slices", at)
				}

				// Endpoint rule: every snapshot rel's endpoints must be in
				// the snapshot's own node set — and the LINK rel must appear
				// exactly when both A and B are alive (1700 only).
				nodeSet := map[types.NodeID]bool{}
				for _, n := range snap.Nodes {
					nodeSet[n.ID()] = true
				}
				for _, r := range snap.Relationships {
					if !nodeSet[r.StartNodeID()] || !nodeSet[r.EndNodeID()] {
						t.Errorf("Snapshot(%d) includes rel with invalid endpoint", at)
					}
				}
				wantRel := at == 1700 // A∈[1000,2000) ∧ B∈[1500,∞) ∧ rel VT≥1600
				if (len(snap.Relationships) == 1) != wantRel {
					t.Errorf("Snapshot(%d) rels = %d, want present=%v", at, len(snap.Relationships), wantRel)
				}
			}

			// Diff(1200, 1700): B appears (starts 1500); nothing else changes
			// version between those instants. Exact-set, not subset.
			diff, err := g.Temporal().Diff(1200, 1700)
			if err != nil {
				t.Fatalf("Diff: %v", err)
			}
			if len(diff.NodesCreated) != 1 || diff.NodesCreated[0].ID() != b.ID() {
				t.Errorf("Diff(1200,1700) created = %v, want exactly [B]", idsOf(diff.NodesCreated))
			}
			if len(diff.NodesUpdated) != 0 || len(diff.NodesDeleted) != 0 {
				t.Errorf("Diff(1200,1700) spurious changes: updated=%v deleted=%v",
					len(diff.NodesUpdated), len(diff.NodesDeleted))
			}

			// DiffCallback must agree with Diff exactly.
			var cbCreated, cbUpdated, cbDeleted int
			err = g.Temporal().DiffCallback(1200, 1700, temporalpkg.DiffHandlers{
				OnNodeCreated: func(*types.Node) error { cbCreated++; return nil },
				OnNodeUpdated: func(_, _ *types.Node) error { cbUpdated++; return nil },
				OnNodeDeleted: func(*types.Node) error { cbDeleted++; return nil },
			})
			if err != nil {
				t.Fatalf("DiffCallback: %v", err)
			}
			if cbCreated != len(diff.NodesCreated) || cbUpdated != len(diff.NodesUpdated) || cbDeleted != len(diff.NodesDeleted) {
				t.Errorf("DiffCallback (%d,%d,%d) disagrees with Diff (%d,%d,%d)",
					cbCreated, cbUpdated, cbDeleted,
					len(diff.NodesCreated), len(diff.NodesUpdated), len(diff.NodesDeleted))
			}

			// Diff(1500, far future): A's interval closed → Deleted; E was
			// deleted → Deleted; D's label changed (new version) → Updated;
			// C appears (snowflake fallback) → Created; B unchanged.
			diff2, err := g.Temporal().Diff(1500, now+3_600_000)
			if err != nil {
				t.Fatalf("Diff2: %v", err)
			}
			assertIDSet(t, "Diff2.Created", idsOf(diff2.NodesCreated), []types.NodeID{c.ID()})
			deleted := idsOf(diff2.NodesDeleted)
			assertIDSet(t, "Diff2.Deleted", deleted, []types.NodeID{a.ID(), e.ID()})
			var updatedIDs []types.NodeID
			for _, u := range diff2.NodesUpdated {
				updatedIDs = append(updatedIDs, u.After.ID())
			}
			assertIDSet(t, "Diff2.Updated", updatedIDs, []types.NodeID{d.ID()})
		})
	}
}

func sortNodeIDs(ids []types.NodeID) {
	sort.Slice(ids, func(i, j int) bool { return ids[i].SnowflakeID() < ids[j].SnowflakeID() })
}

func idsOf(nodes []*types.Node) []types.NodeID {
	out := make([]types.NodeID, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID())
	}
	return out
}

func assertIDSet(t *testing.T, what string, got, want []types.NodeID) {
	t.Helper()
	sortNodeIDs(got)
	sortNodeIDs(want)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}
