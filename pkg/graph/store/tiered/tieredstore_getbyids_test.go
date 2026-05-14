package tiered

import (
	"errors"
	"testing"

	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/internal/registry"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestTieredStore_GetNodesByIDsShardBatchedContract(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	case1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	case2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{case1, signal, case2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	got, err := ts.GetNodesByIDs([]types.NodeID{signal.ID(), case2.ID(), case1.ID(), signal.ID()})
	if err != nil {
		t.Fatalf("GetNodesByIDs existing: %v", err)
	}
	if len(got) != 4 {
		t.Fatalf("GetNodesByIDs len = %d, want 4", len(got))
	}
	assertSortedTieredNodeIDs(t, got)
	if countTieredNodeID(got, signal.ID()) != 2 {
		t.Fatalf("GetNodesByIDs duplicate signal count = %d, want 2", countTieredNodeID(got, signal.ID()))
	}
	var signalCopies []*types.Node
	for _, n := range got {
		if n.ID() == signal.ID() {
			signalCopies = append(signalCopies, n)
		}
	}
	if len(signalCopies) != 2 || signalCopies[0] == signalCopies[1] {
		t.Fatal("GetNodesByIDs returned aliased pointers for duplicate node IDs")
	}

	missing := types.NodeID(gen.Generate())
	if _, err := ts.GetNodesByIDs([]types.NodeID{case1.ID(), missing}); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNodesByIDs missing err = %v, want ErrNodeNotFound", err)
	}
}

func TestTieredStore_GetByIDsSingleIDFastPathContract(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := a.SetProperty("state", "stored"); err != nil {
		t.Fatalf("SetProperty node: %v", err)
	}
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(a); err != nil {
		t.Fatalf("PutNode(a): %v", err)
	}
	if err := ts.PutNode(b); err != nil {
		t.Fatalf("PutNode(b): %v", err)
	}

	nodes, err := ts.GetNodesByIDs([]types.NodeID{a.ID()})
	if err != nil {
		t.Fatalf("GetNodesByIDs single node: %v", err)
	}
	if len(nodes) != 1 || nodes[0].ID() != a.ID() {
		t.Fatalf("GetNodesByIDs single node = %v, want [%d]", tieredMergeNodeIDs(nodes), a.ID())
	}
	if err := nodes[0].SetProperty("state", "mutated-copy"); err != nil {
		t.Fatalf("mutate returned node: %v", err)
	}
	storedNode, err := ts.GetNode(a.ID())
	if err != nil {
		t.Fatalf("GetNode after returned-node mutation: %v", err)
	}
	if got, _ := storedNode.GetProperty("state"); got != "stored" {
		t.Fatalf("stored node property after returned-node mutation = %v, want stored", got)
	}
	if _, err := ts.GetNodesByIDs([]types.NodeID{types.NodeID(gen.Generate())}); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("GetNodesByIDs single missing err = %v, want ErrNodeNotFound", err)
	}

	relGen := tieredRelGen(t)
	rel := types.NewRelationship(types.RelID(relGen.Generate()), 1, a.ID(), b.ID())
	if err := rel.SetProperty("state", "stored"); err != nil {
		t.Fatalf("SetProperty relationship: %v", err)
	}
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	rels, err := ts.GetRelationshipsByIDs([]types.RelID{rel.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs single relationship: %v", err)
	}
	if len(rels) != 1 || rels[0].ID() != rel.ID() {
		t.Fatalf("GetRelationshipsByIDs single relationship = %v, want [%d]", tieredMergeRelIDs(rels), rel.ID())
	}
	if err := rels[0].SetProperty("state", "mutated-copy"); err != nil {
		t.Fatalf("mutate returned relationship: %v", err)
	}
	storedRel, err := ts.GetRelationship(rel.ID())
	if err != nil {
		t.Fatalf("GetRelationship after returned-relationship mutation: %v", err)
	}
	if got, _ := storedRel.GetProperty("state"); got != "stored" {
		t.Fatalf("stored relationship property after returned-relationship mutation = %v, want stored", got)
	}
	if _, err := ts.GetRelationshipsByIDs([]types.RelID{types.RelID(relGen.Generate())}); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationshipsByIDs single missing err = %v, want ErrRelNotFound", err)
	}
}

func TestTieredStore_GetRelationshipsByIDsShardBatchedContract(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	case1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	case2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{case1, case2, signal} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	relGen := tieredRelGen(t)
	refRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, case1.ID(), case2.ID())
	crossRel := types.NewRelationship(types.RelID(relGen.Generate()), 1, signal.ID(), case1.ID())
	for _, r := range []*types.Relationship{refRel, crossRel} {
		if err := ts.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship(%d): %v", r.ID(), err)
		}
	}

	got, err := ts.GetRelationshipsByIDs([]types.RelID{crossRel.ID(), refRel.ID(), crossRel.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs existing: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("GetRelationshipsByIDs len = %d, want 3", len(got))
	}
	assertSortedTieredRelIDs(t, got)
	if countTieredRelID(got, crossRel.ID()) != 2 {
		t.Fatalf("GetRelationshipsByIDs duplicate crossRel count = %d, want 2", countTieredRelID(got, crossRel.ID()))
	}
	if got[1] == got[2] {
		t.Fatal("GetRelationshipsByIDs returned aliased pointers for duplicate relationship IDs")
	}

	missing := types.RelID(relGen.Generate())
	if _, err := ts.GetRelationshipsByIDs([]types.RelID{refRel.ID(), missing}); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("GetRelationshipsByIDs missing err = %v, want ErrRelNotFound", err)
	}
}

func TestTieredStore_GetRelationshipsByIDsIncludesArchivedRelationships(t *testing.T) {
	ts, caseTok, _ := newArchiveWriteTestStore(t)
	gen := tieredNodeGen(t)
	a := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	b := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{a, b} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	rel := newArchiveWriteRel(t, a.ID(), b.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := ts.ArchiveNode(a.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	got, err := ts.GetRelationshipsByIDs([]types.RelID{rel.ID(), rel.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs archived rel: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetRelationshipsByIDs archived rel len = %d, want 2", len(got))
	}
	if got[0].ID() != rel.ID() || got[1].ID() != rel.ID() {
		t.Fatalf("GetRelationshipsByIDs archived rel IDs = %v", tieredMergeRelIDs(got))
	}
	if got[0] == got[1] {
		t.Fatal("GetRelationshipsByIDs returned aliased pointers for duplicate archived relationship IDs")
	}
}

func assertSortedTieredNodeIDs(t *testing.T, nodes []*types.Node) {
	t.Helper()
	for i := 1; i < len(nodes); i++ {
		if nodes[i-1].ID().SnowflakeID() > nodes[i].ID().SnowflakeID() {
			t.Fatalf("nodes not sorted at %d: %d > %d", i, nodes[i-1].ID(), nodes[i].ID())
		}
	}
}

func assertSortedTieredRelIDs(t *testing.T, rels []*types.Relationship) {
	t.Helper()
	for i := 1; i < len(rels); i++ {
		if rels[i-1].ID().SnowflakeID() > rels[i].ID().SnowflakeID() {
			t.Fatalf("rels not sorted at %d: %d > %d", i, rels[i-1].ID(), rels[i].ID())
		}
	}
}

func countTieredNodeID(nodes []*types.Node, id types.NodeID) int {
	count := 0
	for _, n := range nodes {
		if n.ID() == id {
			count++
		}
	}
	return count
}

func countTieredRelID(rels []*types.Relationship, id types.RelID) int {
	count := 0
	for _, r := range rels {
		if r.ID() == id {
			count++
		}
	}
	return count
}
