package core

import (
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// newTestGen creates a snowflake generator for tests that previously
// constructed IDs directly. Mirrors the helper from
// pkg/graph/internal/badger.
func newTestGen(t *testing.T, nodeID int64) *snowflake.Node {
	t.Helper()
	gen, err := snowflake.NewNode(nodeID,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return gen
}

// newTestBadgerStore creates an in-memory badger.Store for graph-level
// integration tests that previously lived alongside the badger.Store
// implementation in pkg/graph. The implementation now lives in
// pkg/graph/internal/badgerstore, but the integration tests in pkg/graph
// still need a quick badger.Store factory; this is a re-spelling of the
// helper from internal/badger.
func newTestBadgerStore(t *testing.T) *badger.Store {
	t.Helper()
	bs, err := badger.New(badger.Config{InMemory: true})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}
	t.Cleanup(func() { bs.Close() })
	return bs
}

// putTestNode creates and stores a node with the given ID and labels.
func putTestNode(t *testing.T, bs *badger.Store, id int64, primary uint16, extras []uint16) *types.Node {
	t.Helper()
	n := types.NewNode(types.NodeID(snowflake.ID(id)), primary, extras)
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
	return n
}

// putTestRel creates and stores a relationship.
func putTestRel(t *testing.T, bs *badger.Store, id int64, relType uint16, startID, endID int64) *types.Relationship {
	t.Helper()
	r := types.NewRelationship(types.RelID(snowflake.ID(id)), relType, types.NodeID(snowflake.ID(startID)), types.NodeID(snowflake.ID(endID)))
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
	return r
}

// putBadgerNodeTemporal seeds a node with explicit temporal metadata.
func putBadgerNodeTemporal(t *testing.T, bs *badger.Store, id snowflake.ID, labelToken uint16, validFrom, validTo types.Instant) {
	t.Helper()
	n := types.NewNode(types.NodeID(id), labelToken, nil)
	n.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
	if err := bs.PutNode(n); err != nil {
		t.Fatalf("PutNode(%d): %v", id, err)
	}
}

// putBadgerRelTemporal seeds a relationship with explicit temporal metadata.
func putBadgerRelTemporal(t *testing.T, bs *badger.Store, id snowflake.ID, typeToken uint16, startID, endID snowflake.ID, validFrom, validTo types.Instant) {
	t.Helper()
	r := types.NewRelationship(types.RelID(id), typeToken, types.NodeID(startID), types.NodeID(endID))
	r.SetTemporal(&types.TemporalMetadata{ValidFrom: validFrom, ValidTo: validTo})
	if err := bs.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship(%d): %v", id, err)
	}
}
