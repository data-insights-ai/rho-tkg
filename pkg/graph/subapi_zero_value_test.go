package graph_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	graphpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	adminpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/admin"
	tierpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/tier"
	constraintspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/constraints"
	eventspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/events"
	hashpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/hash"
	indexpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/index"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	nodespkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/nodes"
	relspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/rels"
	resolvepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/resolve"
	statspkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/stats"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	temporalpkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/temporal"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func TestSubAPIZeroValuesReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	now := types.Instant(123)
	later := types.Instant(456)

	for _, tc := range []struct {
		name string
		fn   func(t *testing.T) error
	}{
		{name: "Nodes.Add", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.Add(ctx, nil, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.AddWithContext", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.Add(ctx, nil, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.Get", fn: func(t *testing.T) error { var a nodespkg.API; got, err := a.Get(context.Background(), 0); requireNil(t, got); return err }},
		{name: "Nodes.GetWithContext", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.Get(ctx, 0)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.GetByIDs", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.GetByIDs(nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.Update", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.Update(context.Background(), 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.UpdateWithContext", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.Update(ctx, 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.UpdateInPlace", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.UpdateInPlace(context.Background(), 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.UpdateInPlaceWithContext", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.UpdateInPlace(ctx, 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.Delete", fn: func(t *testing.T) error { var a nodespkg.API; return a.Delete(context.Background(), 0) }},
		{name: "Nodes.DeleteWithContext", fn: func(t *testing.T) error { var a nodespkg.API; return a.Delete(ctx, 0) }},
		{name: "Nodes.Import", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.Import(ctx, 0, nil, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.All", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.All(storepkg.QueryOpts{})
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.ByLabel", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.ByLabel("", storepkg.QueryOpts{})
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.ByLabelAndProperty", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.ByLabelAndProperty("", "", nil, storepkg.QueryOpts{})
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.Count", fn: func(t *testing.T) error { var a nodespkg.API; got, err := a.Count(); requireZero(t, got); return err }},
		{name: "Nodes.CountByLabel", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.CountByLabel("")
			requireZero(t, got)
			return err
		}},
		{name: "Nodes.SetProperty", fn: func(t *testing.T) error { var a nodespkg.API; return a.SetProperty(ctx, 0, "", nil) }},
		{name: "Nodes.DeleteProperty", fn: func(t *testing.T) error { var a nodespkg.API; return a.DeleteProperty(ctx, 0, "") }},
		{name: "Nodes.CompareAndSetProperty", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.CompareAndSetProperty(context.Background(), 0, "", nil, nil)
			requireFalse(t, got)
			return err
		}},
		{name: "Nodes.CompareAndSetPropertyWithContext", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.CompareAndSetProperty(ctx, 0, "", nil, nil)
			requireFalse(t, got)
			return err
		}},
		{name: "Nodes.AddLabel", fn: func(t *testing.T) error { var a nodespkg.API; return a.AddLabel(ctx, 0, "") }},
		{name: "Nodes.RemoveLabel", fn: func(t *testing.T) error { var a nodespkg.API; return a.RemoveLabel(ctx, 0, "") }},
		{name: "Nodes.CloseVersion", fn: func(t *testing.T) error { var a nodespkg.API; return a.CloseVersion(ctx, 0, now) }},
		{name: "Nodes.History", fn: func(t *testing.T) error { var a nodespkg.API; got, err := a.History(0); requireNil(t, got); return err }},
		{name: "Nodes.VersionAfter", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.VersionAfter(0, 0)
			requireNil(t, got)
			return err
		}},
		{name: "Nodes.VersionBefore", fn: func(t *testing.T) error {
			var a nodespkg.API
			got, err := a.VersionBefore(0, 0)
			requireNil(t, got)
			return err
		}},

		{name: "Rels.Add", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Add(context.Background(), "", nil, nil, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.AddWithContext", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Add(ctx, "", nil, nil, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.AddByID", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.AddByID(context.Background(), "", 0, 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.AddByIDWithContext", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.AddByID(ctx, "", 0, 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.AddByIDIfAbsent", fn: func(t *testing.T) error {
			var a relspkg.API
			got, ok, err := a.AddByIDIfAbsent(context.Background(), "", 0, 0, nil)
			requireNil(t, got)
			requireFalse(t, ok)
			return err
		}},
		{name: "Rels.AddByIDIfAbsentWithContext", fn: func(t *testing.T) error {
			var a relspkg.API
			got, ok, err := a.AddByIDIfAbsent(ctx, "", 0, 0, nil)
			requireNil(t, got)
			requireFalse(t, ok)
			return err
		}},
		{name: "Rels.Get", fn: func(t *testing.T) error { var a relspkg.API; got, err := a.Get(context.Background(), 0); requireNil(t, got); return err }},
		{name: "Rels.GetWithContext", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Get(ctx, 0)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.GetByIDs", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.GetByIDs(nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.Update", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Update(context.Background(), 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.UpdateWithContext", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Update(ctx, 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.UpdateInPlace", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.UpdateInPlace(context.Background(), 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.UpdateInPlaceWithContext", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.UpdateInPlace(ctx, 0, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.Delete", fn: func(t *testing.T) error { var a relspkg.API; return a.Delete(context.Background(), 0) }},
		{name: "Rels.DeleteWithContext", fn: func(t *testing.T) error { var a relspkg.API; return a.Delete(ctx, 0) }},
		{name: "Rels.Import", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Import(ctx, 0, "", nil, nil, nil)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.All", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.All(storepkg.QueryOpts{})
			requireNil(t, got)
			return err
		}},
		{name: "Rels.ByType", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.ByType("", storepkg.QueryOpts{})
			requireNil(t, got)
			return err
		}},
		{name: "Rels.Count", fn: func(t *testing.T) error { var a relspkg.API; got, err := a.Count(); requireZero(t, got); return err }},
		{name: "Rels.CountByType", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.CountByType("")
			requireZero(t, got)
			return err
		}},
		{name: "Rels.Outgoing", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Outgoing(0, "")
			requireNil(t, got)
			return err
		}},
		{name: "Rels.Incoming", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.Incoming(0, "")
			requireNil(t, got)
			return err
		}},
		{name: "Rels.OutgoingForNodes", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.OutgoingForNodes(nil, "")
			requireNil(t, got)
			return err
		}},
		{name: "Rels.IncomingForNodes", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.IncomingForNodes(nil, "")
			requireNil(t, got)
			return err
		}},
		{name: "Rels.SetProperty", fn: func(t *testing.T) error { var a relspkg.API; return a.SetProperty(ctx, 0, "", nil) }},
		{name: "Rels.DeleteProperty", fn: func(t *testing.T) error { var a relspkg.API; return a.DeleteProperty(ctx, 0, "") }},
		{name: "Rels.CompareAndSetProperty", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.CompareAndSetProperty(context.Background(), 0, "", nil, nil)
			requireFalse(t, got)
			return err
		}},
		{name: "Rels.CompareAndSetPropertyWithContext", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.CompareAndSetProperty(ctx, 0, "", nil, nil)
			requireFalse(t, got)
			return err
		}},
		{name: "Rels.CloseVersion", fn: func(t *testing.T) error { var a relspkg.API; return a.CloseVersion(ctx, 0, now) }},
		{name: "Rels.History", fn: func(t *testing.T) error { var a relspkg.API; got, err := a.History(0); requireNil(t, got); return err }},
		{name: "Rels.VersionAfter", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.VersionAfter(0, 0)
			requireNil(t, got)
			return err
		}},
		{name: "Rels.VersionBefore", fn: func(t *testing.T) error {
			var a relspkg.API
			got, err := a.VersionBefore(0, 0)
			requireNil(t, got)
			return err
		}},

		{name: "Tier.Archive", fn: func(t *testing.T) error { var a tierpkg.API; return a.Archive(0) }},
		{name: "Tier.Restore", fn: func(t *testing.T) error { var a tierpkg.API; return a.Restore(0) }},
		{name: "Tier.ForceRotate", fn: func(t *testing.T) error { var a tierpkg.API; return a.ForceRotate() }},
		{name: "Tier.ListShards", fn: func(t *testing.T) error {
			var a tierpkg.API
			got, err := a.ListShards()
			requireNil(t, got)
			return err
		}},
		{name: "Tier.RebuildCatalog", fn: func(t *testing.T) error { var a tierpkg.API; return a.RebuildCatalog() }},
		{name: "Tier.Repair", fn: func(t *testing.T) error { var a tierpkg.API; got, err := a.Repair(); requireNil(t, got); return err }},
		{name: "Tier.VerifyShard", fn: func(t *testing.T) error {
			var a tierpkg.API
			got, err := a.VerifyShard("")
			requireNil(t, got)
			return err
		}},
		{name: "Admin.Reset", fn: func(t *testing.T) error { var a adminpkg.API; return a.Reset() }},

		{name: "Constraints.Set", fn: func(t *testing.T) error { var a constraintspkg.API; return a.Set(temporalpkg.ConstraintSet{}) }},
		{name: "Constraints.Add", fn: func(t *testing.T) error { var a constraintspkg.API; return a.Add(temporalpkg.TemporalConstraint{}) }},
		{name: "Events.SetSync", fn: func(t *testing.T) error { var a eventspkg.API; return a.SetSync(nil) }},
		{name: "Events.SetAsync", fn: func(t *testing.T) error { var a eventspkg.API; return a.SetAsync(nil) }},
		{name: "Hash.VerifyNodeChain", fn: func(t *testing.T) error {
			var a hashpkg.API
			got, err := a.VerifyNodeChain(0)
			requireFalse(t, got)
			return err
		}},
		{name: "Hash.VerifyRelChain", fn: func(t *testing.T) error {
			var a hashpkg.API
			got, err := a.VerifyRelChain(0)
			requireFalse(t, got)
			return err
		}},
		{name: "IO.Export", fn: func(t *testing.T) error { var a tkgio.API; return a.Export(nil) }},
		{name: "IO.Import", fn: func(t *testing.T) error { var a tkgio.API; return a.Import(nil, tkgio.ImportOptions{}) }},
		{name: "Stats.NodeCount", fn: func(t *testing.T) error {
			var a statspkg.API
			got, err := a.NodeCount()
			requireZero(t, got)
			return err
		}},
		{name: "Stats.RelCount", fn: func(t *testing.T) error {
			var a statspkg.API
			got, err := a.RelCount()
			requireZero(t, got)
			return err
		}},
		{name: "Stats.NodeCountByLabel", fn: func(t *testing.T) error {
			var a statspkg.API
			got, err := a.NodeCountByLabel("")
			requireZero(t, got)
			return err
		}},
		{name: "Stats.RelCountByType", fn: func(t *testing.T) error {
			var a statspkg.API
			got, err := a.RelCountByType("")
			requireZero(t, got)
			return err
		}},
		{name: "Stats.AllLabelCounts", fn: func(t *testing.T) error {
			var a statspkg.API
			got, err := a.AllLabelCounts()
			requireNil(t, got)
			return err
		}},
		{name: "Stats.AllRelTypeCounts", fn: func(t *testing.T) error {
			var a statspkg.API
			got, err := a.AllRelTypeCounts()
			requireNil(t, got)
			return err
		}},

		{name: "Index.CreateProperty", fn: func(t *testing.T) error { var a indexpkg.API; return a.CreateProperty("", "") }},
		{name: "Index.DeleteProperty", fn: func(t *testing.T) error { var a indexpkg.API; return a.DeleteProperty("", "") }},
		{name: "Index.CreateHighFrequency", fn: func(t *testing.T) error { var a indexpkg.API; return a.CreateHighFrequency("", time.Second) }},
		{name: "Index.DeleteHighFrequency", fn: func(t *testing.T) error { var a indexpkg.API; return a.DeleteHighFrequency("") }},
		{name: "Index.CreateTemporal", fn: func(t *testing.T) error { var a indexpkg.API; return a.CreateTemporal("") }},
		{name: "Index.DeleteTemporal", fn: func(t *testing.T) error { var a indexpkg.API; return a.DeleteTemporal("") }},
		{name: "Index.CreateVector", fn: func(t *testing.T) error {
			var a indexpkg.API
			return a.CreateVector("", "", 0, storepkg.DistanceCosine)
		}},
		{name: "Index.DeleteVector", fn: func(t *testing.T) error { var a indexpkg.API; return a.DeleteVector("", "") }},
		{name: "Index.SearchNearest", fn: func(t *testing.T) error {
			var a indexpkg.API
			got, err := a.SearchNearest("", "", nil, 0, storepkg.QueryOpts{})
			requireNil(t, got)
			return err
		}},
		{name: "Index.RegisterProvider", fn: func(t *testing.T) error { var a indexpkg.API; return a.RegisterProvider(nil) }},
		{name: "Index.UnregisterProvider", fn: func(t *testing.T) error { var a indexpkg.API; return a.UnregisterProvider("") }},

		{name: "Temporal.NodeAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodeAt(0, now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NodesAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodesAt(now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NodesByLabelAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodesByLabelAt("", now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelAt(0, now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelsAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelsAt(now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelsByTypeAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelsByTypeAt("", now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NeighborsAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NeighborsAt(0, now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NodesByLabelPropertyAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodesByLabelPropertyAt("", "", nil, now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelsByTypePropertyAt", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelsByTypePropertyAt("", "", nil, now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NodesDuring", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodesDuring(now, later)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelsDuring", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelsDuring(now, later)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NodesByLabelPropertyDuring", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodesByLabelPropertyDuring("", "", nil, now, later)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelsByTypePropertyDuring", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelsByTypePropertyDuring("", "", nil, now, later)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NodeAsOf", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodeAsOf(0, now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelAsOf", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelAsOf(0, now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.NodesAsOf", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.NodesAsOf(now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.RelsAsOf", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelsAsOf(now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.Snapshot", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.Snapshot(now)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.Diff", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.Diff(now, later)
			requireNil(t, got)
			return err
		}},
		{name: "Temporal.DiffCallback", fn: func(t *testing.T) error {
			var a temporalpkg.API
			return a.DiffCallback(now, later, temporalpkg.DiffHandlers{})
		}},
		{name: "Temporal.NodeInterval", fn: func(t *testing.T) error {
			var a temporalpkg.API
			start, end, err := a.NodeInterval(nil)
			requireZero(t, start)
			requireZero(t, end)
			return err
		}},
		{name: "Temporal.RelInterval", fn: func(t *testing.T) error {
			var a temporalpkg.API
			start, end, err := a.RelInterval(nil)
			requireZero(t, start)
			requireZero(t, end)
			return err
		}},
		{name: "Temporal.RelateNodes", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelateNodes(nil, nil)
			requireZero(t, got)
			return err
		}},
		{name: "Temporal.RelateRels", fn: func(t *testing.T) error {
			var a temporalpkg.API
			got, err := a.RelateRels(nil, nil)
			requireZero(t, got)
			return err
		}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.fn(t); !errors.Is(err, graphpkg.ErrNilGraph) {
				t.Fatalf("%s error = %v, want ErrNilGraph", tc.name, err)
			}
		})
	}
}

func TestSubAPIZeroValuesReturnZeroForNoErrorMethods(t *testing.T) {
	t.Parallel()

	var nodesAPI nodespkg.API
	if nodesAPI.HasLabel(nil, "") {
		t.Fatal("zero Nodes.HasLabel = true, want false")
	}
	if got := nodesAPI.Labels(nil); got != nil {
		t.Fatalf("zero Nodes.Labels = %v, want nil", got)
	}
	if got := nodesAPI.PrimaryLabel(nil); got != "" {
		t.Fatalf("zero Nodes.PrimaryLabel = %q, want empty", got)
	}
	if got := nodesAPI.NextID(); got != 0 {
		t.Fatalf("zero Nodes.NextID = %v, want 0", got)
	}

	var relsAPI relspkg.API
	if relsAPI.HasType(nil, "") {
		t.Fatal("zero Rels.HasType = true, want false")
	}
	if got := relsAPI.Type(nil); got != "" {
		t.Fatalf("zero Rels.Type = %q, want empty", got)
	}
	if got := relsAPI.NextID(); got != 0 {
		t.Fatalf("zero Rels.NextID = %v, want 0", got)
	}

	var adminAPI adminpkg.API
	if got := adminAPI.DecomposeNodeID(0); !reflect.DeepEqual(got, adminpkg.IDComponents{}) {
		t.Fatalf("zero Admin.DecomposeNodeID = %#v, want zero", got)
	}
	if got := adminAPI.DecomposeRelID(0); !reflect.DeepEqual(got, adminpkg.IDComponents{}) {
		t.Fatalf("zero Admin.DecomposeRelID = %#v, want zero", got)
	}

	var constraintsAPI constraintspkg.API
	if got := constraintsAPI.Get(); !reflect.DeepEqual(got, temporalpkg.ConstraintSet{}) {
		t.Fatalf("zero Constraints.Get = %#v, want zero", got)
	}

	var eventsAPI eventspkg.API
	if got := eventsAPI.GetSync(); got != nil {
		t.Fatalf("zero Events.GetSync = %v, want nil", got)
	}
	if got := eventsAPI.GetAsync(); got != nil {
		t.Fatalf("zero Events.GetAsync = %v, want nil", got)
	}

	var indexAPI indexpkg.API
	if got := indexAPI.Providers(); got != nil {
		t.Fatalf("zero Index.Providers = %v, want nil", got)
	}

	var resolveAPI resolvepkg.API
	if got, ok := resolveAPI.NodeProperty(nil, ""); got != nil || ok {
		t.Fatalf("zero Resolve.NodeProperty = (%v, %v), want (nil, false)", got, ok)
	}
	if got, ok := resolveAPI.RelProperty(nil, ""); got != nil || ok {
		t.Fatalf("zero Resolve.RelProperty = (%v, %v), want (nil, false)", got, ok)
	}
	_ = resolveAPI

	var statsAPI statspkg.API
	if got, err := statsAPI.Get(); got != (statspkg.GraphStats{}) || !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("zero Stats.Get = (%#v, %v), want (zero, ErrNilGraph)", got, err)
	}
}

func TestSubAPINilPointerReceiversReturnErrNilGraph(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		fn   func(t *testing.T) error
	}{
		{name: "Nodes", fn: func(t *testing.T) error { var a *nodespkg.API; _, err := a.Get(context.Background(), 0); return err }},
		{name: "Rels", fn: func(t *testing.T) error { var a *relspkg.API; _, err := a.Get(context.Background(), 0); return err }},
		{name: "Admin", fn: func(t *testing.T) error { var a *adminpkg.API; return a.Reset() }},
		{name: "Constraints", fn: func(t *testing.T) error { var a *constraintspkg.API; return a.Set(temporalpkg.ConstraintSet{}) }},
		{name: "Events", fn: func(t *testing.T) error { var a *eventspkg.API; return a.SetSync(nil) }},
		{name: "Hash", fn: func(t *testing.T) error { var a *hashpkg.API; _, err := a.VerifyNodeChain(0); return err }},
		{name: "Index", fn: func(t *testing.T) error { var a *indexpkg.API; return a.CreateProperty("", "") }},
		{name: "IO", fn: func(t *testing.T) error { var a *tkgio.API; return a.Export(nil) }},
		{name: "Resolve", fn: func(t *testing.T) error {
			var a *resolvepkg.API
			if _, ok := a.NodeProperty(nil, ""); ok {
				return errors.New("Resolve.NodeProperty did not return false on nil receiver")
			}
			return graphpkg.ErrNilGraph
		}},
		{name: "Stats", fn: func(t *testing.T) error { var a *statspkg.API; _, err := a.NodeCount(); return err }},
		{name: "Temporal", fn: func(t *testing.T) error { var a *temporalpkg.API; _, err := a.NodesAt(0); return err }},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if err := tc.fn(t); !errors.Is(err, graphpkg.ErrNilGraph) {
				t.Fatalf("%s nil receiver error = %v, want ErrNilGraph", tc.name, err)
			}
		})
	}
}

func TestSubAPINewWithTypedNilOpsReturnsErrNilGraph(t *testing.T) {
	t.Parallel()

	var hashOps *typedNilHashOps
	hashAPI := hashpkg.New(hashOps)
	if got, err := hashAPI.VerifyNodeChain(0); !errors.Is(err, graphpkg.ErrNilGraph) || got {
		t.Fatalf("typed nil Hash.VerifyNodeChain = (%v, %v), want (false, ErrNilGraph)", got, err)
	}

	var eventOps *typedNilEventOps
	eventsAPI := eventspkg.New(eventOps)
	if err := eventsAPI.SetSync(nil); !errors.Is(err, graphpkg.ErrNilGraph) {
		t.Fatalf("typed nil Events.SetSync = %v, want ErrNilGraph", err)
	}
	if got := eventsAPI.GetSync(); got != nil {
		t.Fatalf("typed nil Events.GetSync = %v, want nil", got)
	}
	if got := eventsAPI.GetAsync(); got != nil {
		t.Fatalf("typed nil Events.GetAsync = %v, want nil", got)
	}
}

type typedNilHashOps struct{}

func (*typedNilHashOps) VerifyNodeChain(types.NodeID) (bool, error) {
	panic("typed nil hash ops should not be invoked")
}

func (*typedNilHashOps) VerifyRelChain(types.RelID) (bool, error) {
	panic("typed nil hash ops should not be invoked")
}

type typedNilEventOps struct{}

func (*typedNilEventOps) SetSync(*eventspkg.EventBus) error {
	panic("typed nil event ops should not be invoked")
}

func (*typedNilEventOps) SetAsync(*eventspkg.AsyncEventBus) error {
	panic("typed nil event ops should not be invoked")
}

func (*typedNilEventOps) GetSync() *eventspkg.EventBus {
	panic("typed nil event ops should not be invoked")
}

func (*typedNilEventOps) GetAsync() *eventspkg.AsyncEventBus {
	panic("typed nil event ops should not be invoked")
}

func requireNil[T any](t *testing.T, got T) {
	t.Helper()
	if !reflect.ValueOf(&got).Elem().IsNil() {
		t.Fatalf("got %v, want nil", got)
	}
}

func requireZero[T comparable](t *testing.T, got T) {
	t.Helper()
	var zero T
	if got != zero {
		t.Fatalf("got %v, want zero", got)
	}
}

func requireFalse(t *testing.T, got bool) {
	t.Helper()
	if got {
		t.Fatal("got true, want false")
	}
}
