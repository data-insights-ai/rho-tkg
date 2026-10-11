package badger_test

import (
	"reflect"
	"testing"

	rho "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDiskMembershipRetainsExactOldPinsAfterCorrectionsDeletesAndReopen(t *testing.T) {
	cfg := badger.Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, SyncWrites: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2}
	bs, err := badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := rho.New(rho.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateProperty("Subject", "name"); err != nil {
		t.Fatal(err)
	}
	if err := g.Index().CreateRelProperty("Knows", "name"); err != nil {
		t.Fatal(err)
	}
	a, err := g.Nodes().Add(t.Context(), []string{"Subject"}, map[string]any{"name": "old"})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes().Add(t.Context(), []string{"Subject"}, map[string]any{"name": "deleted"})
	if err != nil {
		t.Fatal(err)
	}
	c, err := g.Nodes().Add(t.Context(), []string{"Other"}, map[string]any{"name": "old"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(t.Context(), "Knows", a, c, map[string]any{"name": "old"})
	if err != nil {
		t.Fatal(err)
	}
	pin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes().Update(t.Context(), a.ID(), map[string]any{"name": "new"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"name": "new"}); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().Delete(t.Context(), b.ID()); err != nil {
		t.Fatal(err)
	}
	progress, err := bs.BuildHistoricalMembership(t.Context(), badger.HistoricalMembershipLimits{MaxRows: 2, MaxBytes: 1 << 20})
	if err != nil || progress.Complete {
		t.Fatal(progress, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err = rho.New(rho.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	for !progress.Complete {
		progress, err = bs.BuildHistoricalMembership(t.Context(), badger.HistoricalMembershipLimits{MaxRows: 2, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
	}
	assertOld := func() {
		t.Helper()
		rows, err := g.Nodes().ByLabelAndProperty("Subject", "name", "old", rho.QueryOpts{TxPin: pin})
		if err != nil || len(rows) != 1 || rows[0].ID() != a.ID() {
			t.Fatal("old property membership changed", rows, err)
		}
		deleted, err := g.Nodes().ByLabelAndProperty("Subject", "name", "deleted", rho.QueryOpts{TxPin: pin})
		if err != nil || len(deleted) != 1 || deleted[0].ID() != b.ID() {
			t.Fatal("deleted historical member lost", deleted, err)
		}
		rels, err := g.Rels().ByTypeAndProperty("Knows", "name", "old", rho.QueryOpts{TxPin: pin})
		if err != nil || len(rels) != 1 || rels[0].ID() != r.ID() {
			t.Fatal("old relationship membership changed", rels, err)
		}
		labels, err := g.Nodes().ByLabel("Subject", rho.QueryOpts{TxPin: pin})
		if err != nil {
			t.Fatal(err)
		}
		ids := []types.NodeID{}
		for _, n := range labels {
			ids = append(ids, n.ID())
		}
		if !reflect.DeepEqual(ids, []types.NodeID{a.ID(), b.ID()}) {
			t.Fatal("pinned label membership changed", ids)
		}
		phantom, err := g.Nodes().ByLabelAndProperty("Subject", "name", "phantom", rho.QueryOpts{TxPin: pin})
		if err != nil || len(phantom) != 0 {
			t.Fatal("phantom value acquired a member", phantom, err)
		}
	}
	assertOld()
	if _, err := g.Nodes().Update(t.Context(), a.ID(), map[string]any{"name": "after-build"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"name": "after-build"}); err != nil {
		t.Fatal(err)
	}
	assertOld()
	current, err := g.Nodes().ByLabelAndProperty("Subject", "name", "after-build", rho.QueryOpts{})
	if err != nil || len(current) != 1 || current[0].ID() != a.ID() {
		t.Fatal(current, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err = rho.New(rho.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	assertOld()
}
