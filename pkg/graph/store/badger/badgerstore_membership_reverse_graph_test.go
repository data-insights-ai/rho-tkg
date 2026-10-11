package badger_test

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"testing"

	rho "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/badger"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func reverseGraphBootstrap(t *testing.T, bs *badger.Store) {
	t.Helper()
	for {
		p, err := bs.BuildHistoricalMembership(t.Context(), badger.HistoricalMembershipLimits{MaxRows: 1000, MaxBytes: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if p.Complete {
			return
		}
	}
}

func reverseGraphMigrate(t *testing.T, cfg badger.Config) {
	t.Helper()
	m, err := badger.OpenHistoricalReverseMigration(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	for {
		p, err := m.Advance(t.Context(), badger.HistoricalMembershipLimits{MaxRows: 1000, MaxBytes: 8 << 20})
		if err != nil {
			t.Fatal(err)
		}
		if p.Complete {
			return
		}
	}
}

func TestCompactReversePublicOldPinRollbackAndRetrospectionOracle(t *testing.T) {
	for _, migrate := range []bool{false, true} {
		t.Run(map[bool]string{false: "fresh6", true: "migrated5to6"}[migrate], func(t *testing.T) {
			cfg := badger.Config{Dir: t.TempDir(), HistoricalMembershipOnDisk: true, CompactHistoricalReverseOnDisk: !migrate, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, SyncWrites: true, HistoryDeltaEncoding: true, HistoryAnchorInterval: 2}
			bs, err := badger.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			g, err := rho.New(rho.Config{Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateProperty("Subject", "payload"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("Knows", "payload"); err != nil {
				t.Fatal(err)
			}
			a, err := g.Nodes().Add(t.Context(), []string{"Subject"}, map[string]any{"payload": "old", "later_index": "old"})
			if err != nil {
				t.Fatal(err)
			}
			b, err := g.Nodes().Add(t.Context(), []string{"Subject"}, map[string]any{"payload": "deleted"})
			if err != nil {
				t.Fatal(err)
			}
			r, err := g.Rels().Add(t.Context(), "Knows", a, b, map[string]any{"payload": "old", "later_index": "old"})
			if err != nil {
				t.Fatal(err)
			}
			pin, err := g.Temporal().NowTx()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.Nodes().Update(t.Context(), a.ID(), map[string]any{"payload": "new", "later_index": "new"}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Rels().Update(t.Context(), r.ID(), map[string]any{"payload": "new", "later_index": "new"}); err != nil {
				t.Fatal(err)
			}
			reverseGraphBootstrap(t, bs)
			assertOld := func() {
				t.Helper()
				nodes, err := g.Nodes().ByLabelAndProperty("Subject", "payload", "old", rho.QueryOpts{TxPin: pin})
				if err != nil || len(nodes) != 1 || nodes[0].ID() != a.ID() {
					t.Fatal("old nodes", nodes, err)
				}
				rels, err := g.Rels().ByTypeAndProperty("Knows", "payload", "old", rho.QueryOpts{TxPin: pin})
				if err != nil || len(rels) != 1 || rels[0].ID() != r.ID() {
					t.Fatal("old rels", rels, err)
				}
				for _, value := range []string{"new", "phantom"} {
					nodes, err := g.Nodes().ByLabelAndProperty("Subject", "payload", value, rho.QueryOpts{TxPin: pin})
					if err != nil || len(nodes) != 0 {
						t.Fatal("future/phantom nodes", nodes, err)
					}
					rels, err := g.Rels().ByTypeAndProperty("Knows", "payload", value, rho.QueryOpts{TxPin: pin})
					if err != nil || len(rels) != 0 {
						t.Fatal("future/phantom rels", rels, err)
					}
				}
			}
			assertOld()
			if err := g.Close(); err != nil {
				t.Fatal(err)
			}
			if migrate {
				reverseGraphMigrate(t, cfg)
				cfg.CompactHistoricalReverseOnDisk = true
			}
			bs, err = badger.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			g, err = rho.New(rho.Config{Store: bs})
			if err != nil {
				t.Fatal(err)
			}
			assertOld()
			// Registration after old values disappeared must still find them at
			// the old pin. The compact codec preserves undeclared scopes too.
			if err := g.Index().CreateProperty("Subject", "later_index"); err != nil {
				t.Fatal(err)
			}
			if err := g.Index().CreateRelProperty("Knows", "later_index"); err != nil {
				t.Fatal(err)
			}
			lateNodes, err := g.Nodes().ByLabelAndProperty("Subject", "later_index", "old", rho.QueryOpts{TxPin: pin})
			if err != nil || len(lateNodes) != 1 || lateNodes[0].ID() != a.ID() {
				t.Fatal("late node index lost history", lateNodes, err)
			}
			lateRels, err := g.Rels().ByTypeAndProperty("Knows", "later_index", "old", rho.QueryOpts{TxPin: pin})
			if err != nil || len(lateRels) != 1 || lateRels[0].ID() != r.ID() {
				t.Fatal("late relation index lost history", lateRels, err)
			}
			tx, err := g.Tx().Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateNode(a.ID(), map[string]any{"payload": "rolled-back"}); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.UpdateRelationship(r.ID(), map[string]any{"payload": "rolled-back"}); err != nil {
				t.Fatal(err)
			}
			if err := tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			for _, kind := range []bool{false, true} {
				if kind {
					rows, err := g.Rels().ByTypeAndProperty("Knows", "payload", "rolled-back", rho.QueryOpts{})
					if err != nil || len(rows) != 0 {
						t.Fatal("rollback rel phantom", rows, err)
					}
				} else {
					rows, err := g.Nodes().ByLabelAndProperty("Subject", "payload", "rolled-back", rho.QueryOpts{})
					if err != nil || len(rows) != 0 {
						t.Fatal("rollback node phantom", rows, err)
					}
				}
			}
			if _, err := g.Temporal().SetNodeVersionInterval(t.Context(), a.ID(), 10, 20, map[string]any{"payload": "retrospective"}); err != nil {
				t.Fatal(err)
			}
			if _, err := g.Temporal().SetRelVersionInterval(t.Context(), r.ID(), 10, 20, map[string]any{"payload": "retrospective"}); err != nil {
				t.Fatal(err)
			}
			correctedNodes, err := g.Nodes().ByLabelAndProperty("Subject", "payload", "retrospective", rho.QueryOpts{ValidAt: 15})
			if err != nil || len(correctedNodes) != 1 || correctedNodes[0].ID() != a.ID() {
				t.Fatal("retrospective node", correctedNodes, err)
			}
			correctedRels, err := g.Rels().ByTypeAndProperty("Knows", "payload", "retrospective", rho.QueryOpts{ValidAt: 15})
			if err != nil || len(correctedRels) != 1 || correctedRels[0].ID() != r.ID() {
				t.Fatal("retrospective relation", correctedRels, err)
			}
			assertOld()
			if err := g.Rels().Delete(t.Context(), r.ID()); err != nil {
				t.Fatal(err)
			}
			if err := g.Nodes().Delete(t.Context(), b.ID()); err != nil {
				t.Fatal(err)
			}
			assertOld()
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
		})
	}
}

func reverseGenuineQueryDigest(t *testing.T, g *rho.Graph, labels, relTypes []string, pin types.Instant) (string, int, int) {
	t.Helper()
	h := sha256.New()
	nodes, rels := 0, 0
	write := func(v any) {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		h.Write(raw)
		h.Write([]byte("\n"))
	}
	for _, label := range labels {
		if label == "" {
			continue
		}
		rows, err := g.Nodes().ByLabel(label, rho.QueryOpts{TxPin: pin})
		if err != nil {
			t.Fatal(err)
		}
		for _, n := range rows {
			write(struct {
				Label     string
				ID        types.NodeID
				Version   uint32
				Props     map[string]any
				Temporal  *types.TemporalMetadata
				Integrity *types.NodeIntegrity
			}{label, n.ID(), n.Version(), n.PropertiesMap(), n.Temporal(), n.Integrity()})
			nodes++
		}
	}
	for _, name := range relTypes {
		if name == "" {
			continue
		}
		rows, err := g.Rels().ByType(name, rho.QueryOpts{TxPin: pin})
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range rows {
			write(struct {
				Type      string
				ID        types.RelID
				Version   uint32
				From, To  types.NodeID
				Props     map[string]any
				Temporal  *types.TemporalMetadata
				Integrity *types.RelIntegrity
			}{name, r.ID(), r.Version(), r.StartNodeID(), r.EndNodeID(), r.PropertiesMap(), r.Temporal(), r.Integrity()})
			rels++
		}
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nodes, rels
}

func TestCompactReverseGenuineJustizNodeRelationshipOldPinOracle(t *testing.T) {
	source := os.Getenv("RHO_REVERSE_GENUINE_JUSTIZ_FIXTURE")
	if source == "" {
		t.Skip("set to closed genuine Justiz graph; copies only this bounded offline fixture")
	}
	dir := filepath.Join(t.TempDir(), "graph")
	if err := os.CopyFS(dir, os.DirFS(source)); err != nil {
		t.Fatal(err)
	}
	cfg := badger.Config{Dir: dir, HistoricalMembershipOnDisk: true, SyncWrites: true, EntityIdentityOnDisk: true, PropertyIndexOnDisk: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true, HistoryPresenceProbeOnly: true, DisableOrdinals: true, DisablePlannerStats: true, CacheCapacity: 256, CacheBudgetBytes: 8 << 20, MemTableSize: 32 << 20, BlockCacheSize: 8 << 20, IndexCacheSize: 8 << 20}
	bs, err := badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err := rho.New(rho.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	reverseGraphBootstrap(t, bs)
	registry, err := g.Replication().RegistrySnapshot()
	if err != nil {
		t.Fatal(err)
	}
	labels := slices.Clone(registry.Labels)
	relTypes := slices.Clone(registry.RelTypes)
	slices.Sort(labels)
	slices.Sort(relTypes)
	ids, err := bs.AllRelIDs(rho.QueryOpts{})
	if err != nil || len(ids) != 48 {
		t.Fatalf("genuine relation fixture expected48: %d %v", len(ids), err)
	}
	oldPin := types.Instant(0)
	for _, id := range ids {
		r, err := bs.GetRelationship(id)
		if err != nil {
			t.Fatal(err)
		}
		tx := r.Temporal().TxFrom
		if tx > 0 && (oldPin == 0 || tx < oldPin) {
			oldPin = tx
		}
	}
	lastPin, err := g.Temporal().NowTx()
	if err != nil {
		t.Fatal(err)
	}
	pins := []types.Instant{oldPin, lastPin, 0}
	answers := make([]string, len(pins))
	for i, pin := range pins {
		digest, n, r := reverseGenuineQueryDigest(t, g, labels, relTypes, pin)
		answers[i] = digest
		t.Logf("before pin%d nodes%d rels%d sha256%s", pin, n, r, digest)
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	reverseGraphMigrate(t, cfg)
	cfg.CompactHistoricalReverseOnDisk = true
	bs, err = badger.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	g, err = rho.New(rho.Config{Store: bs})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	for i, pin := range pins {
		digest, n, r := reverseGenuineQueryDigest(t, g, labels, relTypes, pin)
		t.Logf("after pin%d nodes%d rels%d sha256%s", pin, n, r, digest)
		if digest != answers[i] {
			t.Fatal("genuine old/current query changed", pin, digest, answers[i])
		}
	}
}
