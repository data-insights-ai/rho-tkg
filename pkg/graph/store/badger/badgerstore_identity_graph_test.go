package badger_test

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	rho "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/sharded"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestDiskIdentityGraphHistoryRollbackAndResidentOracle(t *testing.T) {
	var oracle map[string][]string
	for _, disk := range []bool{false, true} {
		config := badger.Config{Dir: t.TempDir(), EntityIdentityOnDisk: disk,
			HistoricalMembershipOnDisk: true, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true,
			SyncWrites: true, DisableOrdinals: true, DisablePlannerStats: true, HistoryPresenceProbeOnly: true,
			CacheCapacity: 2, CacheBudgetBytes: 1 << 20, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2}
		open := func() (*badger.Store, *rho.Graph) {
			t.Helper()
			bs, err := badger.New(config)
			if err != nil {
				t.Fatal(err)
			}
			g, err := rho.New(rho.Config{Store: bs, DurableCommit: true})
			if err != nil {
				t.Fatal(err)
			}
			return bs, g
		}
		bs, g := open()
		if err := g.Index().CreateProperty("Subject", "name"); err != nil {
			t.Fatal(err)
		}
		add := func(label, name string) *types.Node {
			t.Helper()
			n, err := g.Nodes().Add(t.Context(), []string{label}, map[string]any{"name": name})
			if err != nil {
				t.Fatal(err)
			}
			return n
		}
		a := add("Subject", "old")
		b := add("Subject", "deleted")
		c := add("Other", "other")
		rel, err := g.Rels().Add(t.Context(), "Knows", a, c, map[string]any{"name": "old"})
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
		if _, err := g.Rels().Update(t.Context(), rel.ID(), map[string]any{"name": "new"}); err != nil {
			t.Fatal(err)
		}
		if err := g.Nodes().Delete(t.Context(), b.ID()); err != nil {
			t.Fatal(err)
		}
		tx, err := g.Tx().Begin()
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.UpdateNode(a.ID(), map[string]any{"name": "rolled-back"}); err != nil {
			t.Fatal(err)
		}
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		for {
			progress, err := bs.BuildHistoricalMembership(t.Context(), badger.HistoricalMembershipLimits{MaxRows: 2, MaxBytes: 1 << 20})
			if err != nil {
				t.Fatal(err)
			}
			if progress.Complete {
				break
			}
		}
		answers := func() map[string][]string {
			t.Helper()
			out := map[string][]string{}
			for _, query := range []struct {
				key, label, value string
				at                types.Instant
			}{{"old", "Subject", "old", pin}, {"deleted", "Subject", "deleted", pin}, {"current", "Subject", "new", 0}, {"phantom", "Subject", "phantom", pin}, {"rollback", "Subject", "rolled-back", 0}} {
				rows, err := g.Nodes().ByLabelAndProperty(query.label, "name", query.value, rho.QueryOpts{TxPin: query.at})
				if err != nil {
					t.Fatal(query.key, err)
				}
				out[query.key] = []string{}
				for _, row := range rows {
					name, _ := row.GetProperty("name")
					out[query.key] = append(out[query.key], name.(string))
				}
			}
			for _, at := range []types.Instant{pin, 0} {
				key := "rel_old"
				if at == 0 {
					key = "rel_current"
				}
				rows, err := g.Rels().ByType("Knows", rho.QueryOpts{TxPin: at})
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range rows {
					name, _ := row.GetProperty("name")
					out[key] = append(out[key], name.(string))
				}
			}
			rows, err := g.Temporal().NodesAsOf(pin)
			if err != nil {
				t.Fatal(err)
			}
			for _, row := range rows {
				name, _ := row.GetProperty("name")
				out["all_old"] = append(out["all_old"], name.(string))
			}
			sort.Strings(out["all_old"])
			return out
		}
		before := answers()
		want := map[string][]string{"old": {"old"}, "deleted": {"deleted"}, "current": {"new"}, "phantom": {}, "rollback": {}, "rel_old": {"old"}, "rel_current": {"new"}, "all_old": {"deleted", "old", "other"}}
		if !reflect.DeepEqual(before, want) {
			t.Fatal("exact answers changed", disk, before)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		_, g = open()
		after := answers()
		if !reflect.DeepEqual(after, before) {
			t.Fatal("reopen changed retained history", after, before)
		}
		if err := g.Close(); err != nil {
			t.Fatal(err)
		}
		if oracle == nil {
			oracle = after
		} else if !reflect.DeepEqual(oracle, after) {
			t.Fatal("resident and disk identities differ", oracle, after)
		}
	}
}

func TestDiskIdentityUnadaptedWrappersRejectBeforeFilesystemWrites(t *testing.T) {
	for _, name := range []string{"sharded", "tiered"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "must-not-exist")
			var err error
			if name == "sharded" {
				_, err = sharded.New(sharded.Config{Dir: dir, SlotCount: 1, EntityIdentityOnDisk: true})
			} else {
				_, err = tiered.New(tiered.Config{DataDir: dir, EntityIdentityOnDisk: true})
			}
			if !errors.Is(err, storecontract.ErrCapabilityNotSupported) {
				t.Fatal("unadapted wrapper silently accepted disk identity", err)
			}
			if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("unsupported wrapper modified filesystem", err)
			}
		})
	}
}

func TestDiskIdentitySignedZeroHistoryRetractionAndReopen(t *testing.T) {
	config := badger.Config{Dir: t.TempDir(), EntityIdentityOnDisk: true,
		HistoricalMembershipOnDisk: true, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true,
		SyncWrites: true, DisableOrdinals: true, DisablePlannerStats: true, HistoryPresenceProbeOnly: true,
		CacheCapacity: 2, CacheBudgetBytes: 1 << 20}
	open := func() (*badger.Store, *rho.Graph) {
		t.Helper()
		bs, err := badger.New(config)
		if err != nil {
			t.Fatal(err)
		}
		g, err := rho.New(rho.Config{Store: bs, DurableCommit: true})
		if err != nil {
			t.Fatal(err)
		}
		return bs, g
	}
	bs, g := open()
	defer func() { _ = g.Close() }()
	rangeOf := func(from, to types.Instant) types.TemporalRange {
		return types.TemporalRange{From: types.TemporalEndpoint{Value: from, Finite: true}, To: types.TemporalEndpoint{Value: to, Finite: true}}
	}
	if err := g.Index().CreateProperty("Fact", "tag"); err != nil {
		t.Fatal(err)
	}
	n, err := g.Nodes().Add(t.Context(), []string{"Fact"}, map[string]any{"tag": "old", "tkg_valid_bounds": rangeOf(-1000, 1000)})
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := g.Nodes().Add(t.Context(), []string{"Identity"}, map[string]any{"tkg_valid_bounds": types.TemporalRange{}})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels().Add(t.Context(), "MEMBER", n, endpoint, map[string]any{"tag": "old", "tkg_valid_bounds": rangeOf(-1000, 1000)})
	if err != nil {
		t.Fatal(err)
	}
	before, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetNodeVersionBounds(t.Context(), n.ID(), rangeOf(-100, 0), map[string]any{"tag": "fixed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := g.Temporal().SetRelVersionBounds(t.Context(), r.ID(), rangeOf(-100, 0), map[string]any{"tag": "fixed"}); err != nil {
		t.Fatal(err)
	}
	after, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	for {
		progress, err := bs.BuildHistoricalMembership(t.Context(), badger.HistoricalMembershipLimits{MaxRows: 2, MaxBytes: 1 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if progress.Complete {
			break
		}
	}
	assert := func() {
		t.Helper()
		for _, query := range []struct {
			pin types.Instant
			tag string
		}{{before, "old"}, {after, "fixed"}} {
			row, err := g.Temporal().NodeAtTx(n.ID(), -50, query.pin)
			if err != nil {
				t.Fatal(err)
			}
			if tag, _ := row.GetProperty("tag"); tag != query.tag {
				t.Fatal("node correction used latest belief", tag, query.tag)
			}
			rel, err := g.Temporal().RelAtTx(r.ID(), -50, query.pin)
			if err != nil {
				t.Fatal(err)
			}
			if tag, _ := rel.GetProperty("tag"); tag != query.tag {
				t.Fatal("relationship correction used latest belief", tag, query.tag)
			}
		}
		pieces, err := g.Temporal().NodeEffectiveTimeline(n.ID(), after)
		if err != nil || len(pieces) != 3 || pieces[1].Bounds() != rangeOf(-100, 0) {
			t.Fatal("native finite-zero pieces", pieces, err)
		}
		zero, err := g.Nodes().ByLabelAndProperty("Fact", "tag", "fixed", rho.QueryOpts{ValidAt: 0, ValidAtSet: true, TxAt: after})
		if err != nil || len(zero) != 0 {
			t.Fatal("finite zero end became infinity", zero, err)
		}
		window := rangeOf(-200, 200)
		old, err := g.Nodes().ByLabelAndProperty("Fact", "tag", "old", rho.QueryOpts{ValidWindow: &window, TxAt: after})
		if err != nil || len(old) != 1 || old[0].ID() != n.ID() {
			t.Fatal("predicate held during part of window was lost", old, err)
		}
	}
	assert()
	if err := g.Rels().Retract(t.Context(), r.ID()); err != nil {
		t.Fatal(err)
	}
	if err := g.Nodes().Retract(t.Context(), n.ID()); err != nil {
		t.Fatal(err)
	}
	assert()
	currentPin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	if current, err := g.Temporal().NodeEffectiveTimeline(n.ID(), currentPin); err != nil || len(current) != 0 {
		t.Fatal("retracted statement remained current", current, err)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	_, g = open()
	assert()
}
