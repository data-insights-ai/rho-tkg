package store_test

import (
	"errors"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/badger"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func directStoreBackends(t *testing.T) map[string]storepkg.Store {
	t.Helper()

	bs, err := badger.New(badger.Config{InMemory: true, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("badger.New: %v", err)
	}

	ts, err := tiered.New(tiered.Config{
		InMemory:      true,
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: time.Hour,
	})
	if err != nil {
		_ = bs.Close()
		t.Fatalf("tiered.New: %v", err)
	}

	backends := map[string]storepkg.Store{
		"memory": memory.New(),
		"badger": bs,
		"tiered": ts,
	}
	t.Cleanup(func() {
		for _, backend := range backends {
			_ = backend.Close()
		}
	})
	return backends
}

func TestDirectStoreHistoryWritesRejectInvalidSnapshots(t *testing.T) {
	for name, backend := range directStoreBackends(t) {
		t.Run(name, func(t *testing.T) {
			n := types.NewNode(types.NodeID(snowflake.ID(101)), 1, nil)
			n.SetTemporal(&types.TemporalMetadata{ValidFrom: 20, ValidTo: 20})
			if err := backend.PutNodeVersion(n.ID(), 0, n); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("PutNodeVersion invalid temporal = %v, want ErrInvalidStoreMutation", err)
			}

			r := types.NewRelationship(types.RelID(snowflake.ID(201)), 1, types.NodeID(snowflake.ID(101)), types.NodeID(snowflake.ID(102)))
			r.SetTemporal(&types.TemporalMetadata{ValidFrom: 30, ValidTo: 10})
			if err := backend.PutRelVersion(r.ID(), 0, r); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("PutRelVersion invalid temporal = %v, want ErrInvalidStoreMutation", err)
			}
		})
	}
}

func TestDirectStoreHistoryWritesRejectPayloadVersionMismatch(t *testing.T) {
	for name, backend := range directStoreBackends(t) {
		t.Run(name, func(t *testing.T) {
			n := types.NewNode(types.NodeID(snowflake.ID(111)), 1, nil)
			n.SetVersion(2)
			if err := backend.PutNodeVersion(n.ID(), 1, n); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("PutNodeVersion mismatched payload version = %v, want ErrInvalidStoreMutation", err)
			}

			r := types.NewRelationship(types.RelID(snowflake.ID(211)), 1, types.NodeID(snowflake.ID(111)), types.NodeID(snowflake.ID(112)))
			r.SetVersion(3)
			if err := backend.PutRelVersion(r.ID(), 2, r); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("PutRelVersion mismatched payload version = %v, want ErrInvalidStoreMutation", err)
			}
		})
	}
}

func TestDirectStoreAtomicHistoryMethodsRejectPayloadVersionMismatch(t *testing.T) {
	for name, backend := range directStoreBackends(t) {
		t.Run(name, func(t *testing.T) {
			n1 := types.NewNode(types.NodeID(snowflake.ID(121)), 1, nil)
			n2 := types.NewNode(types.NodeID(snowflake.ID(122)), 1, nil)
			if err := backend.PutNode(n1); err != nil {
				t.Fatalf("PutNode(n1): %v", err)
			}
			if err := backend.PutNode(n2); err != nil {
				t.Fatalf("PutNode(n2): %v", err)
			}
			rel := types.NewRelationship(types.RelID(snowflake.ID(221)), 1, n1.ID(), n2.ID())
			if err := backend.PutRelationship(rel); err != nil {
				t.Fatalf("PutRelationship: %v", err)
			}

			nodeCurrent := n1.DeepCopy()
			nodeCurrent.SetVersion(1)
			if err := backend.ReplaceNodeWithHistory(nodeCurrent, 1, n1); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("ReplaceNodeWithHistory mismatched prevState version = %v, want ErrInvalidStoreMutation", err)
			}
			nodeAfter, err := backend.GetNode(n1.ID())
			if err != nil {
				t.Fatalf("GetNode after rejected ReplaceNodeWithHistory: %v", err)
			}
			if nodeAfter.Version() != 0 {
				t.Fatalf("node version after rejected ReplaceNodeWithHistory = %d, want 0", nodeAfter.Version())
			}

			relCurrent := rel.DeepCopy()
			relCurrent.SetVersion(1)
			if err := backend.ReplaceRelWithHistory(relCurrent, 1, rel); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("ReplaceRelWithHistory mismatched prevState version = %v, want ErrInvalidStoreMutation", err)
			}
			relAfter, err := backend.GetRelationship(rel.ID())
			if err != nil {
				t.Fatalf("GetRelationship after rejected ReplaceRelWithHistory: %v", err)
			}
			if relAfter.Version() != 0 {
				t.Fatalf("relationship version after rejected ReplaceRelWithHistory = %d, want 0", relAfter.Version())
			}

			if err := backend.DeleteRelWithHistory(rel.ID(), 1, rel); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("DeleteRelWithHistory mismatched tombstone version = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.GetRelationship(rel.ID()); err != nil {
				t.Fatalf("relationship missing after rejected DeleteRelWithHistory: %v", err)
			}

			if err := backend.DeleteRelationship(rel.ID()); err != nil {
				t.Fatalf("DeleteRelationship before node tombstone check: %v", err)
			}
			if err := backend.DeleteNodeWithHistory(n1.ID(), 1, n1, nil); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("DeleteNodeWithHistory mismatched tombstone version = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.GetNode(n1.ID()); err != nil {
				t.Fatalf("node missing after rejected DeleteNodeWithHistory: %v", err)
			}
		})
	}
}

func TestDirectStoreAtomicHistoryMethodsRejectStaleLiveVersion(t *testing.T) {
	for name, backend := range directStoreBackends(t) {
		t.Run(name, func(t *testing.T) {
			nReplace := types.NewNode(types.NodeID(snowflake.ID(131)), 1, nil)
			nDelete := types.NewNode(types.NodeID(snowflake.ID(132)), 1, nil)
			nA := types.NewNode(types.NodeID(snowflake.ID(133)), 1, nil)
			nB := types.NewNode(types.NodeID(snowflake.ID(134)), 1, nil)
			for _, n := range []*types.Node{nReplace, nDelete, nA, nB} {
				if err := backend.PutNode(n); err != nil {
					t.Fatalf("PutNode(%d): %v", n.ID(), err)
				}
			}

			nReplaceV1 := nReplace.DeepCopy()
			nReplaceV1.SetVersion(1)
			if err := backend.ReplaceNodeWithHistory(nReplaceV1, 0, nReplace); err != nil {
				t.Fatalf("initial ReplaceNodeWithHistory: %v", err)
			}
			staleNodeCurrent := nReplace.DeepCopy()
			staleNodeCurrent.SetVersion(1)
			if err := backend.ReplaceNodeWithHistory(staleNodeCurrent, 0, nReplace); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("stale ReplaceNodeWithHistory = %v, want ErrInvalidStoreMutation", err)
			}
			nodeAfter, err := backend.GetNode(nReplace.ID())
			if err != nil {
				t.Fatalf("GetNode after stale replace: %v", err)
			}
			if nodeAfter.Version() != 1 {
				t.Fatalf("node version after stale replace = %d, want 1", nodeAfter.Version())
			}

			nDeleteV1 := nDelete.DeepCopy()
			nDeleteV1.SetVersion(1)
			if err := backend.ReplaceNodeWithHistory(nDeleteV1, 0, nDelete); err != nil {
				t.Fatalf("initial delete-node ReplaceNodeWithHistory: %v", err)
			}
			if err := backend.DeleteNodeWithHistory(nDelete.ID(), 0, nDelete, nil); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("stale DeleteNodeWithHistory = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.GetNode(nDelete.ID()); err != nil {
				t.Fatalf("node missing after stale DeleteNodeWithHistory: %v", err)
			}

			relReplace := types.NewRelationship(types.RelID(snowflake.ID(231)), 1, nA.ID(), nB.ID())
			relDelete := types.NewRelationship(types.RelID(snowflake.ID(232)), 1, nA.ID(), nB.ID())
			for _, r := range []*types.Relationship{relReplace, relDelete} {
				if err := backend.PutRelationship(r); err != nil {
					t.Fatalf("PutRelationship(%d): %v", r.ID(), err)
				}
			}

			relReplaceV1 := relReplace.DeepCopy()
			relReplaceV1.SetVersion(1)
			if err := backend.ReplaceRelWithHistory(relReplaceV1, 0, relReplace); err != nil {
				t.Fatalf("initial ReplaceRelWithHistory: %v", err)
			}
			staleRelCurrent := relReplace.DeepCopy()
			staleRelCurrent.SetVersion(1)
			if err := backend.ReplaceRelWithHistory(staleRelCurrent, 0, relReplace); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("stale ReplaceRelWithHistory = %v, want ErrInvalidStoreMutation", err)
			}
			relAfter, err := backend.GetRelationship(relReplace.ID())
			if err != nil {
				t.Fatalf("GetRelationship after stale replace: %v", err)
			}
			if relAfter.Version() != 1 {
				t.Fatalf("relationship version after stale replace = %d, want 1", relAfter.Version())
			}

			relDeleteV1 := relDelete.DeepCopy()
			relDeleteV1.SetVersion(1)
			if err := backend.ReplaceRelWithHistory(relDeleteV1, 0, relDelete); err != nil {
				t.Fatalf("initial delete-rel ReplaceRelWithHistory: %v", err)
			}
			if err := backend.DeleteRelWithHistory(relDelete.ID(), 0, relDelete); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("stale DeleteRelWithHistory = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.GetRelationship(relDelete.ID()); err != nil {
				t.Fatalf("relationship missing after stale DeleteRelWithHistory: %v", err)
			}
		})
	}
}

func TestDirectStoreDeleteNodeWithHistoryRejectsMalformedNodeTombstone(t *testing.T) {
	for name, backend := range directStoreBackends(t) {
		t.Run(name, func(t *testing.T) {
			n := types.NewNode(types.NodeID(snowflake.ID(301)), 1, nil)
			if err := backend.PutNode(n); err != nil {
				t.Fatalf("PutNode: %v", err)
			}

			tombstone := types.NewNode(n.ID(), 2, nil)
			if err := backend.DeleteNodeWithHistory(n.ID(), n.Version(), tombstone, nil); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("DeleteNodeWithHistory label-mutated tombstone = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.GetNode(n.ID()); err != nil {
				t.Fatalf("node missing after rejected DeleteNodeWithHistory: %v", err)
			}
			history, err := backend.GetNodeHistory(n.ID())
			if err != nil {
				t.Fatalf("GetNodeHistory after rejected DeleteNodeWithHistory: %v", err)
			}
			if len(history) != 0 {
				t.Fatalf("history entries after rejected DeleteNodeWithHistory = %d, want 0", len(history))
			}
		})
	}
}

func TestDirectStoreLabelAndTypeReadsRejectReservedTokens(t *testing.T) {
	for name, backend := range directStoreBackends(t) {
		t.Run(name, func(t *testing.T) {
			if _, err := backend.NodesByLabel(0, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("NodesByLabel(0) = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.RelationshipsByType(0, storepkg.QueryOpts{}); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("RelationshipsByType(0) = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.NodeCountByLabel(0); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("NodeCountByLabel(0) = %v, want ErrInvalidStoreMutation", err)
			}
			if _, err := backend.RelCountByType(0); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
				t.Fatalf("RelCountByType(0) = %v, want ErrInvalidStoreMutation", err)
			}
		})
	}
}

func TestDirectStoreExplicitIDReadsRejectInvalidIDs(t *testing.T) {
	for name, backend := range directStoreBackends(t) {
		t.Run(name, func(t *testing.T) {
			n1 := types.NewNode(types.NodeID(snowflake.ID(401)), 1, nil)
			n2 := types.NewNode(types.NodeID(snowflake.ID(402)), 1, nil)
			if err := backend.PutNode(n1); err != nil {
				t.Fatalf("PutNode(n1): %v", err)
			}
			if err := backend.PutNode(n2); err != nil {
				t.Fatalf("PutNode(n2): %v", err)
			}
			rel := types.NewRelationship(types.RelID(snowflake.ID(501)), 1, n1.ID(), n2.ID())
			if err := backend.PutRelationship(rel); err != nil {
				t.Fatalf("PutRelationship: %v", err)
			}

			checks := []struct {
				name string
				run  func() error
			}{
				{name: "GetNode zero", run: func() error { _, err := backend.GetNode(0); return err }},
				{name: "GetNode negative", run: func() error { _, err := backend.GetNode(types.NodeID(-1)); return err }},
				{name: "GetRelationship zero", run: func() error { _, err := backend.GetRelationship(0); return err }},
				{name: "GetRelationship negative", run: func() error { _, err := backend.GetRelationship(types.RelID(-1)); return err }},
				{name: "GetNodesByIDs zero", run: func() error {
					_, err := backend.GetNodesByIDs([]types.NodeID{n1.ID(), 0})
					return err
				}},
				{name: "GetNodesByIDs negative", run: func() error {
					_, err := backend.GetNodesByIDs([]types.NodeID{n1.ID(), types.NodeID(-1)})
					return err
				}},
				{name: "GetRelationshipsByIDs zero", run: func() error {
					_, err := backend.GetRelationshipsByIDs([]types.RelID{rel.ID(), 0})
					return err
				}},
				{name: "GetRelationshipsByIDs negative", run: func() error {
					_, err := backend.GetRelationshipsByIDs([]types.RelID{rel.ID(), types.RelID(-1)})
					return err
				}},
				{name: "OutgoingRelationships zero", run: func() error { _, err := backend.OutgoingRelationships(0, 0); return err }},
				{name: "OutgoingRelationships negative", run: func() error {
					_, err := backend.OutgoingRelationships(types.NodeID(-1), 0)
					return err
				}},
				{name: "IncomingRelationships zero", run: func() error { _, err := backend.IncomingRelationships(0, 0); return err }},
				{name: "IncomingRelationships negative", run: func() error {
					_, err := backend.IncomingRelationships(types.NodeID(-1), 0)
					return err
				}},
				{name: "OutgoingRelationshipsForNodes zero", run: func() error {
					_, err := backend.OutgoingRelationshipsForNodes([]types.NodeID{n1.ID(), 0}, 0)
					return err
				}},
				{name: "OutgoingRelationshipsForNodes negative", run: func() error {
					_, err := backend.OutgoingRelationshipsForNodes([]types.NodeID{n1.ID(), types.NodeID(-1)}, 0)
					return err
				}},
				{name: "IncomingRelationshipsForNodes zero", run: func() error {
					_, err := backend.IncomingRelationshipsForNodes([]types.NodeID{n2.ID(), 0}, 0)
					return err
				}},
				{name: "IncomingRelationshipsForNodes negative", run: func() error {
					_, err := backend.IncomingRelationshipsForNodes([]types.NodeID{n2.ID(), types.NodeID(-1)}, 0)
					return err
				}},
				{name: "GetNodeVersion zero", run: func() error { _, err := backend.GetNodeVersion(0, 0); return err }},
				{name: "GetNodeVersion negative", run: func() error {
					_, err := backend.GetNodeVersion(types.NodeID(-1), 0)
					return err
				}},
				{name: "GetNodeHistory zero", run: func() error { _, err := backend.GetNodeHistory(0); return err }},
				{name: "GetNodeHistory negative", run: func() error {
					_, err := backend.GetNodeHistory(types.NodeID(-1))
					return err
				}},
				{name: "TruncateNodeHistory zero", run: func() error { return backend.TruncateNodeHistory(0, 0) }},
				{name: "TruncateNodeHistory negative", run: func() error {
					return backend.TruncateNodeHistory(types.NodeID(-1), 0)
				}},
				{name: "GetRelVersion zero", run: func() error { _, err := backend.GetRelVersion(0, 0); return err }},
				{name: "GetRelVersion negative", run: func() error {
					_, err := backend.GetRelVersion(types.RelID(-1), 0)
					return err
				}},
				{name: "GetRelHistory zero", run: func() error { _, err := backend.GetRelHistory(0); return err }},
				{name: "GetRelHistory negative", run: func() error {
					_, err := backend.GetRelHistory(types.RelID(-1))
					return err
				}},
				{name: "TruncateRelHistory zero", run: func() error { return backend.TruncateRelHistory(0, 0) }},
				{name: "TruncateRelHistory negative", run: func() error {
					return backend.TruncateRelHistory(types.RelID(-1), 0)
				}},
			}
			for _, check := range checks {
				if err := check.run(); !errors.Is(err, storepkg.ErrInvalidStoreMutation) {
					t.Fatalf("%s = %v, want ErrInvalidStoreMutation", check.name, err)
				}
			}
		})
	}
}
