package badger

import (
	"errors"
	"testing"
	"time"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestLiveRowMissingIsCorruption pins the miss rule of badgerstore_row_missing.go
// on every read that captures a revision: after a clean reopen (ID sets from the
// open image, empty caches), a node row and a rel row removed under the store
// (as badger reports a block that fails its checksum) read as ErrCorruptWire,
// never as "not found", on each path, and a node deleted through the store
// still reads as "not found". 2,100 labelled nodes, so the unbounded label scan
// takes the parallel decode. Mutation: each site answering ErrNodeNotFound or
// ErrRelNotFound turns its subtest red.
func TestLiveRowMissingIsCorruption(t *testing.T) {
	t.Parallel()
	cfg := Config{Dir: t.TempDir()}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const label, relType = 1, 5
	base := types.NodeID(existMapBase)
	batch := make([]*types.Node, 0, parallelDecodeMinIDs+52)
	for i := range cap(batch) {
		batch = append(batch, types.NewNode(base+types.NodeID(i), label, nil))
	}
	if err := bs.PutNodesBatch(batch); err != nil {
		t.Fatal(err)
	}
	rid := types.RelID(existMapBase + 1<<20)
	if err := bs.PutRelationship(types.NewRelationship(rid, relType, base+1, base+2)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}
	if bs, err = New(cfg); err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	gone := base + 3 // deleted through the store: stays "not found"
	if err := bs.DeleteNode(gone); err != nil {
		t.Fatal(err)
	}
	if err := bs.db.Update(func(txn *badgerv4.Txn) error {
		if err := txn.Delete(storepkg.NodeKey(base.SnowflakeID())); err != nil {
			return err
		}
		return txn.Delete(storepkg.RelKey(rid.SnowflakeID()))
	}); err != nil {
		t.Fatal(err)
	}
	later := types.InstantFromTime(time.Now().Add(time.Hour))
	corrupt := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, storecontract.ErrCorruptWire) {
			t.Fatalf("got %v, want ErrCorruptWire", err)
		}
	}
	for name, read := range map[string]func() error{
		"GetNode":           func() error { _, err := bs.GetNode(base); return err },
		"NodeIntegrityHash": func() error { _, err := bs.NodeIntegrityHash(base); return err },
		"LendNode":          func() error { _, err := bs.LendNode(base); return err },
		"LendNodesSorted":   func() error { return bs.LendNodesSorted([]types.NodeID{base}, make([]*types.Node, 1)) },
		"NodesByLabel":      func() error { _, err := bs.NodesByLabel(label, QueryOpts{Limit: 10}); return err },
		"NodesByLabelParallel": func() error {
			_, err := bs.NodesByLabel(label, QueryOpts{})
			return err
		},
		"NodesAsOf": func() error { _, err := bs.NodesAsOf(later); return err },
		"PutRelationshipEndpoint": func() error {
			return bs.PutRelationship(types.NewRelationship(rid+1, relType, base, base+2))
		},
		"PutRelationshipsBatchEndpoints": func() error {
			return bs.PutRelationshipsBatch([]*types.Relationship{types.NewRelationship(rid+2, relType, base+4, base)})
		},
		"GetRelationship":  func() error { _, err := bs.GetRelationship(rid); return err },
		"LendRelationship": func() error { _, err := bs.LendRelationship(rid); return err },
		"LendRelationshipsSorted": func() error {
			return bs.LendRelationshipsSorted([]types.RelID{rid}, make([]*types.Relationship, 1))
		},
		"RelsAsOf": func() error { _, err := bs.RelsAsOf(later); return err },
		"SnapshotGetNode": func() error {
			s := bs.NewReadSnapshot()
			defer s.Release()
			_, err := s.GetNode(base)
			return err
		},
		"SnapshotGetRelationship": func() error {
			s := bs.NewReadSnapshot()
			defer s.Release()
			_, err := s.GetRelationship(rid)
			return err
		},
		"CreateRelTemporalIndex": func() error { return bs.CreateRelTemporalIndex(relType) },
	} {
		t.Run(name, func(t *testing.T) { corrupt(t, read()) })
	}
	if _, err := bs.GetNode(gone); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("a node deleted through the store: got %v, want ErrNodeNotFound", err)
	}
	// The rule's guards: an ID the set no longer holds, and a revision a
	// write moved between the capture and the miss, both stay "not found".
	if err := bs.nodeRowMissing(gone, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("an ID the set does not hold: got %v, want ErrNodeNotFound", err)
	}
	rev := bs.liveNodeRev(base)
	bs.idxMu.Lock()
	bs.bumpNodeRevLocked(base) // a write between the capture and the miss
	bs.idxMu.Unlock()
	if err := bs.nodeRowMissing(base, rev); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("a write since the capture: got %v, want ErrNodeNotFound", err)
	}
}
