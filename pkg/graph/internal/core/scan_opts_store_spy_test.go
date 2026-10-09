package core

// Executable audit for item C (scan-temporal-opts): a store answers QueryOpts
// from CURRENT rows only (storeutil.HasTemporalFilter is a valid-time gate on the
// live row and has no notion of TxAt / TxPin). So no graph door may forward an
// ACTIVE temporal filter — ValidAt, ValidStart+ValidEnd, TxAt or TxPin — to a
// store query method; under such opts every door must take the history-aware
// path (nodesByLabelLocked / relsByTypeLocked and their siblings).
//
// The spy embeds the memory store and records every QueryOpts-taking store
// method that receives an active temporal filter. The battery calls every Core
// read door that takes QueryOpts (the facade's Iter / IterByLabel wrap ForEach /
// ForEachByLabel), the GraphTx read mirrors included. Faulty implementation caught:
// any door that hands temporal opts to the store's current-row shortcut — before
// the fix, ScanNodeColumns and ScanRelColumns (named type and every type).

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

type optsSpyStore struct {
	*memory.Store
	mu    sync.Mutex
	leaks map[string]bool
}

func activeTemporal(o storepkg.QueryOpts) bool {
	return o.ValidAt != 0 || (o.ValidStart > 0 && o.ValidEnd > 0) || o.TxAt != 0 || o.TxPin != 0
}

func (s *optsSpyStore) note(door string, o storepkg.QueryOpts) {
	if !activeTemporal(o) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaks[door] = true
}

func (s *optsSpyStore) NodesByLabel(tok uint16, o storepkg.QueryOpts) ([]*types.Node, error) {
	s.note("NodesByLabel", o)
	return s.Store.NodesByLabel(tok, o)
}

func (s *optsSpyStore) RelationshipsByType(tok uint16, o storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.note("RelationshipsByType", o)
	return s.Store.RelationshipsByType(tok, o)
}

func (s *optsSpyStore) AllNodes(o storepkg.QueryOpts) ([]*types.Node, error) {
	s.note("AllNodes", o)
	return s.Store.AllNodes(o)
}

func (s *optsSpyStore) AllRelationships(o storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.note("AllRelationships", o)
	return s.Store.AllRelationships(o)
}

func (s *optsSpyStore) AllNodeIDs(o storepkg.QueryOpts) ([]types.NodeID, error) {
	s.note("AllNodeIDs", o)
	return s.Store.AllNodeIDs(o)
}

func (s *optsSpyStore) AllRelIDs(o storepkg.QueryOpts) ([]types.RelID, error) {
	s.note("AllRelIDs", o)
	return s.Store.AllRelIDs(o)
}

func (s *optsSpyStore) ForEachNodeByLabel(tok uint16, o storepkg.QueryOpts, fn func(*types.Node) bool) error {
	s.note("ForEachNodeByLabel", o)
	return s.Store.ForEachNodeByLabel(tok, o, fn)
}

func (s *optsSpyStore) ForEachRelByType(tok uint16, o storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	s.note("ForEachRelByType", o)
	return s.Store.ForEachRelByType(tok, o, fn)
}

func (s *optsSpyStore) ScanNodeColumns(tok uint16, props []string, o storepkg.QueryOpts, fn func(*storepkg.ColumnBatch) bool) error {
	s.note("ScanNodeColumns", o)
	return s.Store.ScanNodeColumns(tok, props, o, fn)
}

func (s *optsSpyStore) ScanRelColumns(tok uint16, props []string, o storepkg.QueryOpts, fn func(*storepkg.RelColumnBatch) bool) error {
	s.note("ScanRelColumns", o)
	return s.Store.ScanRelColumns(tok, props, o, fn)
}

func (s *optsSpyStore) NodesByLabelAndProperty(tok uint16, key string, value any, o storepkg.QueryOpts) ([]*types.Node, error) {
	s.note("NodesByLabelAndProperty", o)
	return s.Store.NodesByLabelAndProperty(tok, key, value, o)
}

func (s *optsSpyStore) RelationshipsByTypeAndProperty(tok uint16, key string, value any, o storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.note("RelationshipsByTypeAndProperty", o)
	return s.Store.RelationshipsByTypeAndProperty(tok, key, value, o)
}

func (s *optsSpyStore) ForEachRelByTypePropertyRange(tok uint16, key string, lo, hi float64, inclLo, inclHi bool, o storepkg.QueryOpts, fn func(*types.Relationship) bool) error {
	s.note("ForEachRelByTypePropertyRange", o)
	return s.Store.ForEachRelByTypePropertyRange(tok, key, lo, hi, inclLo, inclHi, o, fn)
}

func (s *optsSpyStore) NodesByLabelAndProperties(tok uint16, values map[string]any, o storepkg.QueryOpts) ([]*types.Node, error) {
	s.note("NodesByLabelAndProperties", o)
	return s.Store.NodesByLabelAndProperties(tok, values, o)
}

func (s *optsSpyStore) SearchNearestNodes(tok uint16, key string, query []float32, k int, o storepkg.QueryOpts) ([]*types.Node, error) {
	s.note("SearchNearestNodes", o)
	return s.Store.SearchNearestNodes(tok, key, query, k, o)
}

// The capabilities below are badger-only; the spy offers them so a door that
// would forward temporal opts to them is caught on the memory-backed spy too.

func (s *optsSpyStore) ForEachNodeByLabelPropertyRange(_ uint16, _ string, _, _ float64, _, _ bool, o storepkg.QueryOpts, _ func(*types.Node) bool) error {
	s.note("ForEachNodeByLabelPropertyRange", o)
	return storepkg.ErrIndexNotFound
}

func (s *optsSpyStore) ForEachAdjacentEndpointAt(_ types.NodeID, _ uint16, _ bool, o storepkg.QueryOpts, _ func(types.RelID, types.NodeID) bool) error {
	s.note("ForEachAdjacentEndpointAt", o)
	return nil
}

func (s *optsSpyStore) ForEachAdjacentRelAt(_ types.NodeID, _ uint16, _ bool, o storepkg.QueryOpts, _ func(*types.Relationship) bool) error {
	s.note("ForEachAdjacentRelAt", o)
	return nil
}

func TestScanDoorsNeverForwardTemporalOptsToStore(t *testing.T) {
	ctx := context.Background()
	spy := &optsSpyStore{Store: memory.New(), leaks: map[string]bool{}}
	g, err := New(Config{Store: spy})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	if err := g.Index.CreateVector("S", "emb", 2, storepkg.DistanceCosine); err != nil {
		t.Fatalf("CreateVector: %v", err)
	}
	a, err := g.Nodes.Add(ctx, []string{"S"}, map[string]any{"v": int64(1), "s": "x", "emb": []float32{1, 0}, "tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	b, err := g.Nodes.Add(ctx, []string{"S"}, map[string]any{"v": int64(2), "s": "y", "tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	r, err := g.Rels.Add(ctx, "R", a, b, map[string]any{"v": int64(1), "s": "x", "tkg_valid_from": types.Instant(1000)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := g.Nodes.Update(ctx, a.ID(), map[string]any{"v": int64(3), "tkg_valid_from": types.Instant(2000)}); err != nil {
		t.Fatal(err)
	}
	upd, err := g.Rels.Update(ctx, r.ID(), map[string]any{"v": int64(3), "tkg_valid_from": types.Instant(2000)})
	if err != nil {
		t.Fatal(err)
	}
	pin := upd.Temporal().TxFrom

	optsList := []storepkg.QueryOpts{
		{ValidAt: 1500},
		{ValidStart: 1500, ValidEnd: 2500},
		{TxAt: pin},
		{ValidAt: 1500, TxAt: pin},
		{TxPin: pin},
	}
	ok := func(door string, err error) {
		t.Helper()
		// ErrIndexNotFound: no index on the spy; ErrVectorSearchTxPinUnsupported:
		// the vector doors decline TxPin (fail closed, documented).
		if err != nil && !errors.Is(err, storepkg.ErrIndexNotFound) && !errors.Is(err, ErrVectorSearchTxPinUnsupported) {
			t.Errorf("%s: %v", door, err)
		}
	}
	anyNode := func(*types.Node) bool { return true }
	anyRel := func(*types.Relationship) bool { return true }
	for _, o := range optsList {
		_, err := g.Nodes.ByLabel("S", o)
		ok("Nodes.ByLabel", err)
		ok("Nodes.ForEachByLabel", g.Nodes.ForEachByLabel("S", o, anyNode))
		_, err = g.Nodes.CountByLabelAt("S", o)
		ok("Nodes.CountByLabelAt", err)
		_, err = g.Nodes.ByLabelAndProperty("S", "v", int64(1), o)
		ok("Nodes.ByLabelAndProperty", err)
		_, err = g.Nodes.ByLabelAndProperties("S", map[string]any{"v": int64(1), "s": "x"}, o)
		ok("Nodes.ByLabelAndProperties", err)
		_, err = g.Index.SearchNearest("S", "emb", []float32{1, 0}, 2, o)
		ok("Index.SearchNearest", err)
		_, err = g.Index.SearchNearestScored("S", "emb", []float32{1, 0}, 2, o)
		ok("Index.SearchNearestScored", err)
		_, err = g.Nodes.All(o)
		ok("Nodes.All", err)
		ok("Nodes.ForEach", g.Nodes.ForEach(o, anyNode))
		ok("Nodes.ForEachByLabelPropertyRange", g.Nodes.ForEachByLabelPropertyRange("S", "v", 0, 10, true, true, o, anyNode))
		ok("Nodes.ForEachByLabelPropertyRangeOrdered", g.Nodes.ForEachByLabelPropertyRangeOrdered("S", "v", 0, 10, true, true, false, o, anyNode))
		ok("Nodes.ForEachByLabelPropertyPrefix", g.Nodes.ForEachByLabelPropertyPrefix("S", "s", "", false, o, anyNode))
		_, _, err = g.Nodes.RangeCardinality("S", "v", 0, 10, true, true, o)
		ok("Nodes.RangeCardinality", err)
		_, _, err = g.Rels.RangeCardinality("R", "v", 0, 10, true, true, o)
		ok("Rels.RangeCardinality", err)
		_, err = g.ScanNodeColumns("S", []string{"v"}, o, func(*storepkg.ColumnBatch) bool { return true })
		ok("ScanNodeColumns", err)

		_, err = g.Rels.ByType("R", o)
		ok("Rels.ByType", err)
		ok("Rels.ForEachByType", g.Rels.ForEachByType("R", o, anyRel))
		_, err = g.Rels.CountByTypeAt("R", o)
		ok("Rels.CountByTypeAt", err)
		_, err = g.Rels.ByTypeAndProperty("R", "v", int64(1), o)
		ok("Rels.ByTypeAndProperty", err)
		_, err = g.Rels.All(o)
		ok("Rels.All", err)
		ok("Rels.ForEach", g.Rels.ForEach(o, anyRel))
		ok("Rels.ForEachByTypePropertyRange", g.Rels.ForEachByTypePropertyRange("R", "v", 0, 10, true, true, o, anyRel))
		ok("Rels.ForEachByTypePropertyRangeOrdered", g.Rels.ForEachByTypePropertyRangeOrdered("R", "v", 0, 10, true, true, false, o, anyRel))
		ok("Rels.ForEachByTypePropertyPrefix", g.Rels.ForEachByTypePropertyPrefix("R", "s", "", false, o, anyRel))
		for _, dir := range []bool{false, true} {
			ok("Rels.ForEachAdjacentEndpointAt", g.Rels.ForEachAdjacentEndpointAt(a.ID(), "R", dir, o, func(types.RelID, types.NodeID) bool { return true }))
			ok("Rels.ForEachAdjacentRelAt", g.Rels.ForEachAdjacentRelAt(a.ID(), "R", dir, o, anyRel))
		}
		for _, typ := range []string{"R", ""} {
			_, err = g.ScanRelColumns(typ, []string{"v"}, o, func(*storepkg.RelColumnBatch) bool { return true })
			ok(fmt.Sprintf("ScanRelColumns(%q)", typ), err)
		}

		// GraphTx read mirrors (every GraphTx read taking QueryOpts).
		tx, err := g.BeginTx()
		if err != nil {
			t.Fatalf("BeginTx: %v", err)
		}
		_, err = tx.AllNodes(o)
		ok("GraphTx.AllNodes", err)
		_, err = tx.NodesByLabel("S", o)
		ok("GraphTx.NodesByLabel", err)
		_, err = tx.NodesByLabelAndProperty("S", "v", int64(1), o)
		ok("GraphTx.NodesByLabelAndProperty", err)
		_, err = tx.AllRels(o)
		ok("GraphTx.AllRels", err)
		_, err = tx.RelsByType("R", o)
		ok("GraphTx.RelsByType", err)
		_, err = tx.SearchNearest("S", "emb", []float32{1, 0}, 2, o)
		ok("GraphTx.SearchNearest", err)
		_, err = tx.SearchNearestScored("S", "emb", []float32{1, 0}, 2, o)
		ok("GraphTx.SearchNearestScored", err)
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Rollback: %v", err)
		}
	}

	spy.mu.Lock()
	defer spy.mu.Unlock()
	if len(spy.leaks) > 0 {
		var doors []string
		for d := range spy.leaks {
			doors = append(doors, d)
		}
		sort.Strings(doors)
		t.Errorf("store query methods received an active temporal filter (current-row answer for a temporal opt): %v", doors)
	}
}
