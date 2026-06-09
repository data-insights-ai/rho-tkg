package tiered

import (
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/generatedcreate"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestTieredStoreGeneratedRelationshipWithEndpointHashesInvalidProofFallback(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	end := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	rel.SetIntegrity(&types.RelIntegrity{
		Hash:         "rel-hash",
		FromNodeHash: "caller-from-hash",
		ToNodeHash:   "caller-to-hash",
	})
	fromHash, toHash, err := ts.PutRelationshipGeneratedIDWithEndpointHashes(rel, generatedcreate.Proof{})
	if err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashes invalid proof: %v", err)
	}
	if fromHash != "caller-from-hash" || toHash != "caller-to-hash" {
		t.Fatalf("returned hashes = %q, %q; want caller hashes", fromHash, toHash)
	}
	stored, err := ts.GetRelationship(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if ig := stored.Integrity(); ig == nil || ig.FromNodeHash != "caller-from-hash" || ig.ToNodeHash != "caller-to-hash" {
		t.Fatalf("stored integrity = %+v; want caller endpoint hashes", ig)
	}

	noIntegrity := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	fromHash, toHash, err = ts.PutRelationshipGeneratedIDWithEndpointHashes(noIntegrity, generatedcreate.Proof{})
	if err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashes invalid proof no integrity: %v", err)
	}
	if fromHash != "" || toHash != "" {
		t.Fatalf("returned hashes without integrity = %q, %q; want empty", fromHash, toHash)
	}
}

func TestTieredStoreGeneratedRelationshipWithEndpointHashesRejectsInvalidRelationship(t *testing.T) {
	ts, _, _ := setupBatchDelete(t)

	if _, _, err := ts.PutRelationshipGeneratedIDWithEndpointHashes(nil, generatedcreate.FreshGraphID); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashes(nil) = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestTieredStoreGeneratedRelationshipWithEndpointHashesSelfEndpoint(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	node := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	node.SetIntegrity(&types.NodeIntegrity{Hash: "node-hash"})
	if err := ts.PutNode(node); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, node.ID(), node.ID())
	fromHash, toHash, err := ts.PutRelationshipGeneratedIDWithEndpointHashes(rel, generatedcreate.FreshGraphID)
	if err != nil {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashes self endpoint: %v", err)
	}
	if fromHash != "node-hash" || toHash != "node-hash" {
		t.Fatalf("returned hashes = %q, %q; want node-hash twice", fromHash, toHash)
	}
	if ig := rel.Integrity(); ig == nil || ig.FromNodeHash != "node-hash" || ig.ToNodeHash != "node-hash" {
		t.Fatalf("caller integrity = %+v; want captured self endpoint hashes", ig)
	}
}

func TestTieredStoreGeneratedRelationshipWithEndpointHashesRestoresCallerHashesOnError(t *testing.T) {
	ts, _, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	start.SetIntegrity(&types.NodeIntegrity{Hash: "start-hash"})
	if err := ts.PutNode(start); err != nil {
		t.Fatalf("PutNode start: %v", err)
	}

	missingEndID := types.NodeID(nodeGen.Generate())
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), missingEndID)
	rel.SetIntegrity(&types.RelIntegrity{
		Hash:         "rel-hash",
		FromNodeHash: "old-from",
		ToNodeHash:   "old-to",
	})
	_, _, err := ts.PutRelationshipGeneratedIDWithEndpointHashes(rel, generatedcreate.FreshGraphID)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("PutRelationshipGeneratedIDWithEndpointHashes missing end = %v, want ErrNodeNotFound", err)
	}
	if ig := rel.Integrity(); ig == nil || ig.FromNodeHash != "old-from" || ig.ToNodeHash != "old-to" {
		t.Fatalf("caller integrity after error = %+v; want original endpoint hashes", ig)
	}
}

func TestTieredStoreGeneratedRelationshipWithEndpointHashesRollsBackIncomingOnEntityFailure(t *testing.T) {
	if err := types.RegisterPropertyStructType(unmarshalableProperty{}); err != nil {
		t.Fatalf("RegisterPropertyStructType: %v", err)
	}
	ts, caseTok, signalTok := setupBatchDelete(t)
	nodeGen := tieredNodeGen(t)
	relGen := tieredRelGen(t)

	start := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	start.SetIntegrity(&types.NodeIntegrity{Hash: "start-hash"})
	end := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	end.SetIntegrity(&types.NodeIntegrity{Hash: "end-hash"})
	for _, n := range []*types.Node{start, end} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, start.ID(), end.ID())
	rel.SetIntegrity(&types.RelIntegrity{
		Hash:         "rel-hash",
		FromNodeHash: "old-from",
		ToNodeHash:   "old-to",
	})
	if err := rel.SetProperties(types.PropertySlice{{Key: "bad", Value: unmarshalableProperty{Ch: make(chan int)}}}); err != nil {
		t.Fatalf("SetProperties: %v", err)
	}

	_, _, err := ts.PutRelationshipGeneratedIDWithEndpointHashes(rel, generatedcreate.FreshGraphID)
	if err == nil {
		t.Fatal("PutRelationshipGeneratedIDWithEndpointHashes returned nil for unmarshalable relationship")
	}
	if ig := rel.Integrity(); ig == nil || ig.FromNodeHash != "old-from" || ig.ToNodeHash != "old-to" {
		t.Fatalf("caller integrity after failed entity write = %+v; want original endpoint hashes", ig)
	}
	if _, err := ts.GetRelationship(rel.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationship after failed endpoint-hash write = %v, want ErrRelNotFound", err)
	}
	if got := ts.RefShardForTest().IncomingRelIDs(end.ID().SnowflakeID(), 0); len(got) != 0 {
		t.Fatalf("incoming index after failed endpoint-hash write = %v, want empty", got)
	}
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair: %v", err)
	}
	if res.OrphanedInEntries != 0 || res.MissingInEntries != 0 {
		t.Fatalf("RunRepair after failed endpoint-hash write: orphaned in=%d missing in=%d, want 0/0", res.OrphanedInEntries, res.MissingInEntries)
	}
}
