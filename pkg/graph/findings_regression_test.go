package graph

import (
	"context"
	"testing"
	"time"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

func containsNodeID(nodes []*types.Node, id types.NodeID) bool {
	for _, n := range nodes {
		if n.ID() == id {
			return true
		}
	}
	return false
}

// assertNodeSet fails the test if the result set is not exactly want
// (same elements, same count). Catches both "missed" and "over-reported" bugs.
func assertNodeSet(t *testing.T, label string, got []*types.Node, want []types.NodeID) {
	t.Helper()
	gotSet := make(map[types.NodeID]struct{}, len(got))
	for _, n := range got {
		gotSet[n.ID()] = struct{}{}
	}
	wantSet := make(map[types.NodeID]struct{}, len(want))
	for _, id := range want {
		wantSet[id] = struct{}{}
	}
	for id := range wantSet {
		if _, ok := gotSet[id]; !ok {
			t.Errorf("%s: missing expected node %d", label, id)
		}
	}
	for id := range gotSet {
		if _, ok := wantSet[id]; !ok {
			t.Errorf("%s: unexpected node %d in result", label, id)
		}
	}
	if len(gotSet) != len(wantSet) {
		t.Errorf("%s: result size = %d, want %d", label, len(gotSet), len(wantSet))
	}
}

func assertRelSet(t *testing.T, label string, got []*types.Relationship, want []types.RelID) {
	t.Helper()
	gotSet := make(map[types.RelID]struct{}, len(got))
	for _, r := range got {
		gotSet[r.ID()] = struct{}{}
	}
	wantSet := make(map[types.RelID]struct{}, len(want))
	for _, id := range want {
		wantSet[id] = struct{}{}
	}
	for id := range wantSet {
		if _, ok := gotSet[id]; !ok {
			t.Errorf("%s: missing expected rel %d", label, id)
		}
	}
	for id := range gotSet {
		if _, ok := wantSet[id]; !ok {
			t.Errorf("%s: unexpected rel %d in result", label, id)
		}
	}
	if len(gotSet) != len(wantSet) {
		t.Errorf("%s: result size = %d, want %d", label, len(gotSet), len(wantSet))
	}
}

// Regression: VerifyNodeHashChain recomputes each version's hash with that
// version's labels — not with current's (or chain[len(chain)-1]'s when current
// is nil). The pre-fix code (B30) extracted labels once and reused them across
// the whole chain, silently mis-verifying any label-mutated history.
//
// Three sub-tests:
//
//  1. Legitimate three-distinct-label-set chain. v0={A}, v1={A,B}, v2={A,C} —
//     every version is a witness, so a single-label-source bug at any slot
//     produces a mismatch (the original v0={A}/v2={A} chain only witnessed at
//     v1).
//
//  2. Discriminating history tamper. Constructs a chain that pre-fix code
//     would (incorrectly) accept but post-fix code must reject — the only
//     tamper shape that distinguishes the two implementations. The current
//     test's "rewrite A→a on current" tamper does not discriminate: both
//     pre-fix and post-fix detect it.
//
//  3. Deleted entity. When current is nil, pre-fix fell back to
//     chain[len(chain)-1]'s labels — same B30 bug, different branch. A
//     legitimate label-mutated chain on a deleted node must still verify.
func TestVerifyNodeHashChain_LabelMutations(t *testing.T) {
	t.Run("three-distinct-label-sets verifies", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		id := n.ID()
		if err := g.AddNodeLabel(id, "B"); err != nil {
			t.Fatalf("AddNodeLabel B: %v", err)
		}
		if err := g.RemoveNodeLabel(id, "B"); err != nil {
			t.Fatalf("RemoveNodeLabel B: %v", err)
		}
		if err := g.AddNodeLabel(id, "C"); err != nil {
			t.Fatalf("AddNodeLabel C: %v", err)
		}

		valid, err := g.VerifyNodeHashChain(id)
		if err != nil {
			t.Fatalf("VerifyNodeHashChain: %v", err)
		}
		if !valid {
			t.Fatal("legitimate label-mutation chain must verify")
		}
	})

	t.Run("history tamper accepted under current-label rule rejected under per-entry rule", func(t *testing.T) {
		// Constructs a tamper that the pre-fix (current-label) rule accepts but
		// the post-fix (per-entry) rule rejects — the only shape that
		// discriminates the two implementations.
		g := newTestGraph(t)

		// MemoryStore-only: the test pokes private history/nodes fields;
		// other backends don't expose them.
		ms, ok := g.store.(*MemoryStore)
		if !ok {
			t.Fatalf("test requires MemoryStore, got %T", g.store)
		}

		// 1. Build a real chain: v0={A}, v1={A,B}, v2={A}.
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		id := n.ID()
		if err := g.AddNodeLabel(id, "B"); err != nil {
			t.Fatalf("AddNodeLabel B: %v", err)
		}
		if err := g.RemoveNodeLabel(id, "B"); err != nil {
			t.Fatalf("RemoveNodeLabel B: %v", err)
		}

		// 2. Resolve raw token IDs — labels are stored as tokens, not strings.
		bTok, ok := g.labels.Lookup("B")
		if !ok {
			t.Fatal("labels.Lookup(B): not registered")
		}
		cTok, err := g.labels.GetOrCreate("C")
		if err != nil {
			t.Fatalf("labels.GetOrCreate(C): %v", err)
		}

		current, err := g.GetNode(id)
		if err != nil {
			t.Fatalf("GetNode: %v", err)
		}
		currentLabels := g.NodeLabels(current) // ["A"]

		v1Orig := ms.GetNodeHistoryEntry(id, 1)

		// 3. Tamper v1: swap B→C so its labels become {A,C}.
		v1Tampered := v1Orig.DeepCopy()
		if !v1Tampered.RemoveLabelTokenRaw(bTok) {
			t.Fatal("v1.RemoveLabelTokenRaw(B): not removed")
		}
		if !v1Tampered.AddLabelTokenRaw(cTok) {
			t.Fatal("v1.AddLabelTokenRaw(C): not added")
		}

		// 4. Compute both candidate hashes:
		//    preFix  = original entry hashed under current's labels  ["A"]
		//    postFix = tampered entry hashed under its own labels    ["A","C"]
		preFixV1Hash := ComputeNodeHash(v1Orig, currentLabels)
		postFixV1Hash := ComputeNodeHash(v1Tampered, g.NodeLabels(v1Tampered))

		// 5. Sanity guard: if the two hashes coincide, the test can't
		//    discriminate — fail loudly rather than silently pass.
		if preFixV1Hash == postFixV1Hash {
			t.Fatalf("pre-fix and post-fix hashes coincide (%s); test cannot discriminate", preFixV1Hash)
		}

		// 6. Stamp the forgery: tampered v1 stores preFixV1Hash; v2 is
		//    re-linked so its PrevHash matches. Chain is now internally
		//    consistent under the pre-fix rule.
		v1Tampered.SetIntegrity(&types.NodeIntegrity{
			Hash:     preFixV1Hash,
			PrevHash: v1Orig.Integrity().PrevHash,
		})
		v2Relinked := current.DeepCopy()
		v2Relinked.SetIntegrity(&types.NodeIntegrity{
			Hash:     current.Integrity().Hash,
			PrevHash: preFixV1Hash,
		})

		// 7. Inject directly under lock — no legitimate API path can
		//    produce this state.
		ms.SetNodeHistoryEntryForTest(id, 1, v1Tampered)
		ms.SetNodeForTest(id, v2Relinked)

		// Assertion: pre-fix would recompute v1 with ["A"], match preFixV1Hash,
		// wrongly accept. Post-fix recomputes with NodeLabels(v1Tampered) =
		// ["A","C"], mismatches, rejects.
		valid, err := g.VerifyNodeHashChain(id)
		if err != nil {
			t.Fatalf("VerifyNodeHashChain: %v", err)
		}
		if valid {
			t.Fatal("tampered v1 (tokens {A,C}, hash computed under current's labels) must NOT verify; pre-fix code would have accepted")
		}
	})

	t.Run("deleted node with divergent history labels verifies", func(t *testing.T) {
		g := newTestGraph(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		id := n.ID()
		if err := g.AddNodeLabel(id, "B"); err != nil {
			t.Fatalf("AddNodeLabel B: %v", err)
		}
		if err := g.RemoveNodeLabel(id, "B"); err != nil {
			t.Fatalf("RemoveNodeLabel B: %v", err)
		}
		if err := g.AddNodeLabel(id, "C"); err != nil {
			t.Fatalf("AddNodeLabel C: %v", err)
		}
		if err := g.DeleteNode(id); err != nil {
			t.Fatalf("DeleteNode: %v", err)
		}

		valid, err := g.VerifyNodeHashChain(id)
		if err != nil {
			t.Fatalf("VerifyNodeHashChain: %v", err)
		}
		if !valid {
			t.Fatal("legitimate label-mutated chain on deleted node must verify (history-only path)")
		}
	})
}

// TestNodeHashChain_InspectsHashValues probes the actual hash bytes rather
// than trusting VerifyNodeHashChain's boolean return. Catches three failure
// modes the soft test above cannot:
//
//  1. ComputeNodeHash returning a constant (e.g. all zeros) — every Hash is
//     non-empty and version-distinct.
//  2. Labels not contributing to the hash input — adding a label must
//     change the Hash bytes.
//  3. PrevHash not being maintained — vN's PrevHash must equal v(N-1)'s Hash.
//
// Walks the full chain (history + current) and verifies link integrity
// directly, independent of VerifyNodeHashChain.
func TestNodeHashChain_InspectsHashValues(t *testing.T) {
	g := newTestGraph(t)

	// v0: genesis version.
	n, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()

	// Integrity() returns *types.NodeIntegrity{Hash, PrevHash}; nil only on entities
	// created without hashing (not possible via AddNode). Genesis: PrevHash must be empty.
	v0 := n.Integrity()
	if v0 == nil || v0.Hash == "" {
		t.Fatalf("v0: integrity = %+v, want non-empty Hash", v0)
	}
	if v0.PrevHash != "" {
		t.Errorf("v0: PrevHash = %q, want empty for genesis", v0.PrevHash)
	}
	v0Hash := v0.Hash

	// v1: AddNodeLabel must change Hash (labels feed input) AND link PrevHash to v0.
	if err := g.AddNodeLabel(id, "B"); err != nil {
		t.Fatalf("AddNodeLabel: %v", err)
	}
	current, err := g.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode after add: %v", err)
	}
	v1 := current.Integrity()
	if v1 == nil || v1.Hash == "" {
		t.Fatalf("v1: integrity = %+v, want non-empty Hash", v1)
	}
	if v1.Hash == v0Hash {
		t.Errorf("v1.Hash == v0.Hash (%q) — adding a label did not change the hash input", v1.Hash)
	}
	if v1.PrevHash != v0Hash {
		t.Errorf("v1.PrevHash = %q, want v0.Hash = %q (chain link broken)", v1.PrevHash, v0Hash)
	}
	v1Hash := v1.Hash

	// v2: RemoveNodeLabel must change Hash AND link PrevHash to v1.
	if err := g.RemoveNodeLabel(id, "B"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}
	current, err = g.GetNode(id)
	if err != nil {
		t.Fatalf("GetNode after remove: %v", err)
	}
	v2 := current.Integrity()
	if v2 == nil || v2.Hash == "" {
		t.Fatalf("v2: integrity = %+v, want non-empty Hash", v2)
	}
	if v2.Hash == v1Hash {
		t.Errorf("v2.Hash == v1.Hash (%q) — removing a label did not change the hash input", v2.Hash)
	}
	if v2.PrevHash != v1Hash {
		t.Errorf("v2.PrevHash = %q, want v1.Hash = %q (chain link broken)", v2.PrevHash, v1Hash)
	}

	// Defense in depth: walk persisted chain (history + current) and re-verify every link.
	history, err := g.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	chain := append(history, current)
	if len(chain) != 3 {
		t.Fatalf("chain length = %d, want 3 (v0 + v1 + v2)", len(chain))
	}
	for i := 1; i < len(chain); i++ {
		prev := chain[i-1].Integrity()
		curr := chain[i].Integrity()
		if curr.PrevHash != prev.Hash {
			t.Errorf("chain[%d].PrevHash = %q, want chain[%d].Hash = %q",
				i, curr.PrevHash, i-1, prev.Hash)
		}
	}
}

func TestGetNodesByLabelValidAt_UsesHistoricalLabelVersion(t *testing.T) {
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"Person", "Legacy"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	queryTime := g.nodeValidFrom(n)

	time.Sleep(2 * time.Millisecond)
	if err := g.RemoveNodeLabel(id, "Legacy"); err != nil {
		t.Fatalf("RemoveNodeLabel: %v", err)
	}

	nodes, err := g.GetNodesByLabelValidAt("Legacy", queryTime)
	if err != nil {
		t.Fatalf("GetNodesByLabelValidAt: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatalf("historical Legacy label missing at %d; got %d nodes", queryTime, len(nodes))
	}
}

func TestNodesByLabelPropertyTemporalQueries_UseHistoricalPropertyVersion(t *testing.T) {
	g := newTestGraph(t)

	n, err := g.AddNode([]string{"Person"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	id := n.ID()
	queryTime := g.nodeValidFrom(n)

	time.Sleep(2 * time.Millisecond)
	updated, err := g.UpdateNode(id, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	nodes, err := g.NodesByLabelPropertyAndTime("Person", "status", "draft", queryTime)
	if err != nil {
		t.Fatalf("NodesByLabelPropertyAndTime: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatalf("historical property value missing at %d; got %d nodes", queryTime, len(nodes))
	}

	end := updated.Temporal().UpdatedAt
	nodes, err = g.NodesByLabelPropertyDuring("Person", "status", "draft", queryTime, end)
	if err != nil {
		t.Fatalf("NodesByLabelPropertyDuring: %v", err)
	}
	if !containsNodeID(nodes, id) {
		t.Fatalf("historical property value missing during [%d, %d); got %d nodes", queryTime, end, len(nodes))
	}
}

func TestGetNeighborsValidAt_UsesHistoricalRelationships(t *testing.T) {
	g := newTestGraph(t)

	a, err := g.AddNode([]string{"Person"}, map[string]any{"name": "A"})
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{"Person"}, map[string]any{"name": "B"})
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}
	r, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}
	queryTime := g.relValidFrom(r)

	time.Sleep(2 * time.Millisecond)
	if err := g.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship: %v", err)
	}

	neighbors, err := g.GetNeighborsValidAt(a.ID(), queryTime)
	if err != nil {
		t.Fatalf("GetNeighborsValidAt: %v", err)
	}
	if !containsNodeID(neighbors, b.ID()) {
		t.Fatalf("historical neighbor missing at %d; got %d neighbors", queryTime, len(neighbors))
	}
}

func TestLabelMutations_UpdateTransactionTimeBounds(t *testing.T) {
	tests := []struct {
		name   string
		labels []string
		mutate func(*Graph, types.NodeID) error
	}{
		{
			name:   "add",
			labels: []string{"A"},
			mutate: func(g *Graph, id types.NodeID) error {
				return g.AddNodeLabel(id, "B")
			},
		},
		{
			name:   "remove",
			labels: []string{"A", "B"},
			mutate: func(g *Graph, id types.NodeID) error {
				return g.RemoveNodeLabel(id, "B")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := newTestGraph(t)
			n, err := g.AddNode(tt.labels, nil)
			if err != nil {
				t.Fatalf("AddNode: %v", err)
			}
			id := n.ID()
			origTxFrom := n.Temporal().TxFrom

			time.Sleep(2 * time.Millisecond)
			if err := tt.mutate(g, id); err != nil {
				t.Fatalf("label mutation: %v", err)
			}

			current, err := g.GetNode(id)
			if err != nil {
				t.Fatalf("GetNode: %v", err)
			}
			currentTM := current.Temporal()
			if currentTM == nil || currentTM.TxFrom <= origTxFrom {
				t.Fatalf("current TxFrom = %v, want > original %v", currentTM, origTxFrom)
			}

			hist, err := g.GetNodeHistory(id)
			if err != nil {
				t.Fatalf("GetNodeHistory: %v", err)
			}
			if len(hist) != 1 {
				t.Fatalf("history entries = %d, want 1", len(hist))
			}
			histTM := hist[0].Temporal()
			if histTM == nil || histTM.TxTo == 0 {
				t.Fatalf("history TxTo = %v, want non-zero", histTM)
			}
			if histTM.TxTo > currentTM.TxFrom {
				t.Fatalf("history TxTo %d should be <= current TxFrom %d", histTM.TxTo, currentTM.TxFrom)
			}
		})
	}
}

func TestNoOpMutations_DoNotPublishUpdateEvents(t *testing.T) {
	t.Run("idempotent add label", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A", "B"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		if err := g.AddNodeLabel(n.ID(), "B"); err != nil {
			t.Fatalf("AddNodeLabel: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("idempotent AddNodeLabel published %d events: %v", len(got), got)
		}
	})

	t.Run("empty node update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		if _, err := g.UpdateNode(n.ID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateNode: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateNode published %d events: %v", len(got), got)
		}
	})

	t.Run("empty relationship update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		a, _ := g.AddNode([]string{"A"}, nil)
		b, _ := g.AddNode([]string{"B"}, nil)
		r, err := g.AddRelationship("REL", a, b, nil)
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		events := collectEvents(g, EventRelUpdate)

		if _, err := g.UpdateRelationship(r.ID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateRelationship: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateRelationship published %d events: %v", len(got), got)
		}
	})

	t.Run("empty node in-place update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		if _, err := g.UpdateNodeInPlace(n.ID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateNodeInPlace: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateNodeInPlace published %d events: %v", len(got), got)
		}
	})

	t.Run("empty relationship in-place update", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		a, _ := g.AddNode([]string{"A"}, nil)
		b, _ := g.AddNode([]string{"B"}, nil)
		r, err := g.AddRelationship("REL", a, b, nil)
		if err != nil {
			t.Fatalf("AddRelationship: %v", err)
		}
		events := collectEvents(g, EventRelUpdate)

		if _, err := g.UpdateRelInPlace(r.ID(), map[string]any{}); err != nil {
			t.Fatalf("UpdateRelInPlace: %v", err)
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("empty UpdateRelInPlace published %d events: %v", len(got), got)
		}
	})

	t.Run("compare-and-set absent delete", func(t *testing.T) {
		g := newTestGraphForEvents(t)
		n, err := g.AddNode([]string{"A"}, nil)
		if err != nil {
			t.Fatalf("AddNode: %v", err)
		}
		events := collectEvents(g, EventNodeUpdate)

		ok, err := g.CompareAndSetProperty(n.ID(), "missing", nil, nil)
		if err != nil {
			t.Fatalf("CompareAndSetProperty: %v", err)
		}
		if !ok {
			t.Fatal("CompareAndSetProperty should report a matched absent property")
		}
		if got := drain(events); len(got) != 0 {
			t.Fatalf("absent-property CAS delete published %d events: %v", len(got), got)
		}
	})
}

func TestImportNodeWithID_MatchesAddNodeMetadataEventsAndStats(t *testing.T) {
	g := newTestGraphForEvents(t)
	events := collectEvents(g, EventNodeCreate)
	before := g.Stats()

	n, err := g.ImportNodeWithID(context.Background(), types.NodeID(12345), []string{"Person"}, map[string]any{
		"name":           "Alice",
		"tkg_valid_from": int64(1000),
		"tkg_created_at": int64(900),
	})
	if err != nil {
		t.Fatalf("ImportNodeWithID: %v", err)
	}

	tm := n.Temporal()
	if tm == nil {
		t.Fatal("imported node missing temporal metadata")
	}
	if tm.TxFrom == 0 {
		t.Fatal("imported node TxFrom should be set")
	}
	if tm.ValidFrom != 1000 || tm.CreatedAt != 900 {
		t.Fatalf("temporal metadata = %+v, want ValidFrom=1000 CreatedAt=900", tm)
	}
	if _, ok := n.GetProperty("tkg_valid_from"); ok {
		t.Fatal("reserved temporal key should not be stored as a normal property")
	}
	if got := drain(events); len(got) != 1 || got[0].Type != EventNodeCreate || got[0].EntityID != types.EntityID(n.ID()) {
		t.Fatalf("expected one EventNodeCreate for imported node, got %v", got)
	}
	after := g.Stats()
	if after.NodesAdded != before.NodesAdded+1 {
		t.Fatalf("NodesAdded = %d, want %d", after.NodesAdded, before.NodesAdded+1)
	}
}

func TestImportRelationshipWithID_MatchesAddRelationshipMetadataEventsAndStats(t *testing.T) {
	g := newTestGraphForEvents(t)
	a, err := g.AddNode([]string{"A"}, nil)
	if err != nil {
		t.Fatalf("AddNode A: %v", err)
	}
	b, err := g.AddNode([]string{"B"}, nil)
	if err != nil {
		t.Fatalf("AddNode B: %v", err)
	}

	events := collectEvents(g, EventRelCreate)
	before := g.Stats()

	r, err := g.ImportRelationshipWithID(context.Background(), types.RelID(54321), "REL", a, b, map[string]any{
		"weight":         int64(1),
		"tkg_valid_from": int64(2000),
		"tkg_created_at": int64(1900),
	})
	if err != nil {
		t.Fatalf("ImportRelationshipWithID: %v", err)
	}

	tm := r.Temporal()
	if tm == nil {
		t.Fatal("imported relationship missing temporal metadata")
	}
	if tm.TxFrom == 0 {
		t.Fatal("imported relationship TxFrom should be set")
	}
	if tm.ValidFrom != 2000 || tm.CreatedAt != 1900 {
		t.Fatalf("temporal metadata = %+v, want ValidFrom=2000 CreatedAt=1900", tm)
	}
	if _, ok := r.GetProperty("tkg_valid_from"); ok {
		t.Fatal("reserved temporal key should not be stored as a normal property")
	}
	if got := drain(events); len(got) != 1 || got[0].Type != EventRelCreate || got[0].EntityID != types.EntityID(r.ID()) {
		t.Fatalf("expected one EventRelCreate for imported relationship, got %v", got)
	}
	after := g.Stats()
	if after.RelsAdded != before.RelsAdded+1 {
		t.Fatalf("RelsAdded = %d, want %d", after.RelsAdded, before.RelsAdded+1)
	}
}

// The next three tests are adversarial: multi-entity scenarios with diverging
// lifecycles, exact-set assertions (catch over-reporting and omission), and
// the decisive "predicate-anywhere-in-interval" case that requires
// findNodeVersionMatchingDuring to scan all overlapping versions.
//
// Timestamp idioms used below:
//   t0   — snowflake-derived valid-from of the last-created entity, which
//          falls within every entity's initial-version window.
//   tNow — wall-clock millisecond + 1, strictly past every mutation timestamp.
//   The 2 ms sleep between t0 and the mutations gives the initial versions a
//   non-zero duration; without it the create and mutation can collide on the
//   same millisecond and v0's interval collapses to empty.

// TestNodesByLabel_TemporalOpts_Adversarial verifies NodesByLabel(opts) is
// history-aware when opts carries a temporal filter.
//
//	     t0                                tNow
//	     │                                  │
//	alice ┤ Pending ────► (label removed) ──┤ no Pending
//	bob   ┤ no Pending ──► (label added) ───┤ Pending
//	carol ┤ Pending ────► (deleted) ✗
func TestNodesByLabel_TemporalOpts_Adversarial(t *testing.T) {
	g := newTestGraph(t)

	alice, err := g.AddNode([]string{"Doc", "Pending"}, nil)
	if err != nil {
		t.Fatalf("AddNode alice: %v", err)
	}
	bob, err := g.AddNode([]string{"Doc"}, nil)
	if err != nil {
		t.Fatalf("AddNode bob: %v", err)
	}
	carol, err := g.AddNode([]string{"Doc", "Pending"}, nil)
	if err != nil {
		t.Fatalf("AddNode carol: %v", err)
	}
	aliceID := alice.ID()
	bobID := bob.ID()
	carolID := carol.ID()

	// t0: carol was created last, so her snowflake time is within v0 of all three.
	t0 := g.nodeValidFrom(carol)

	time.Sleep(2 * time.Millisecond) // give v0 a non-zero duration before mutations
	if err := g.RemoveNodeLabel(aliceID, "Pending"); err != nil {
		t.Fatalf("RemoveNodeLabel alice: %v", err)
	}
	if err := g.AddNodeLabel(bobID, "Pending"); err != nil {
		t.Fatalf("AddNodeLabel bob: %v", err)
	}
	if err := g.DeleteNode(carolID); err != nil {
		t.Fatalf("DeleteNode carol: %v", err)
	}
	// tNow: +1 ms ensures we're strictly past the last mutation's millisecond
	// timestamp, so post-mutation versions overlap [t0, tNow).
	tNow := types.Instant(time.Now().UnixMilli() + 1)

	// At t0: alice and carol carry Pending; bob does not.
	hits, err := g.NodesByLabel("Pending", QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("Pending@t0: %v", err)
	}
	assertNodeSet(t, "Pending@t0", hits, []types.NodeID{aliceID, carolID})

	// At tNow: alice lost Pending, carol is gone, only bob has it.
	hits, err = g.NodesByLabel("Pending", QueryOpts{ValidAt: tNow})
	if err != nil {
		t.Fatalf("Pending@tNow: %v", err)
	}
	assertNodeSet(t, "Pending@tNow", hits, []types.NodeID{bobID})

	// During [t0, tNow): each had Pending at some overlapping moment.
	// Requires findNodeVersionMatchingDuring to scan all overlapping versions.
	hits, err = g.NodesByLabel("Pending", QueryOpts{ValidStart: t0, ValidEnd: tNow})
	if err != nil {
		t.Fatalf("Pending@[t0,tNow): %v", err)
	}
	assertNodeSet(t, "Pending@[t0,tNow)", hits, []types.NodeID{aliceID, bobID, carolID})

	// Pagination on the temporal path: Limit + cursor walks without duplication.
	page1, err := g.NodesByLabel("Pending", QueryOpts{ValidAt: t0, Limit: 1})
	if err != nil || len(page1) != 1 {
		t.Fatalf("page1: got %d nodes, err=%v; want 1, nil", len(page1), err)
	}
	page2, err := g.NodesByLabel("Pending", QueryOpts{ValidAt: t0, After: types.EntityID(page1[0].ID())})
	if err != nil {
		t.Fatalf("page2: %v", err)
	}
	assertNodeSet(t, "paginated Pending@t0", append(page1, page2...), []types.NodeID{aliceID, carolID})

	// A label nobody ever wrote returns empty (and does not error).
	hits, err = g.NodesByLabel("Phantom", QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("Phantom@t0: %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("Phantom@t0 returned %d nodes, want 0", len(hits))
	}
}

// TestNodesByLabelAndProperty_TemporalOpts_Adversarial verifies the generic
// property+label query is history-aware for one label and one string-valued
// property under several lifecycle shapes. Each node has a distinct purpose:
//
//	      t0                                tNow
//	      │                                  │
//	alice  ┤ draft     ──► published ────────┤   discriminator: only entity whose
//	       │                                  │   most-recent overlapping version
//	       │                                  │   FAILS the predicate. A buggy
//	       │                                  │   "most-recent only" implementation
//	       │                                  │   misses her in the during-interval
//	       │                                  │   query.
//	bob    ┤ published ──► draft ────────────┤   inverse path; verifies symmetry
//	carol  ┤ draft (unchanged) ──────────────┤   baseline non-mutated entity
//	dave   ┤ draft     ──► (deleted) ✗       │   tombstone preserves status=draft,
//	                                             so dave is found at t0 but is NOT
//	                                             a discriminator for the bug
//	                                             (tombstone overlaps and matches).
//
// The during-interval assertion is the one that fails under the buggy
// implementation, and only via alice — proven independently by reverting
// findNodeVersionMatchingDuring to most-recent-overlap-only and observing
// "got 3, want 4" with alice missing. Boundary case at the bottom probes the
// half-open semantic of the version interval.
func TestNodesByLabelAndProperty_TemporalOpts_Adversarial(t *testing.T) {
	g := newTestGraph(t)

	alice, err := g.AddNode([]string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode alice: %v", err)
	}
	bob, err := g.AddNode([]string{"Doc"}, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("AddNode bob: %v", err)
	}
	carol, err := g.AddNode([]string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode carol: %v", err)
	}
	dave, err := g.AddNode([]string{"Doc"}, map[string]any{"status": "draft"})
	if err != nil {
		t.Fatalf("AddNode dave: %v", err)
	}
	aliceID := alice.ID()
	bobID := bob.ID()
	carolID := carol.ID()
	daveID := dave.ID()

	// t0: dave was created last, so his snowflake time is within v0 of all four.
	t0 := g.nodeValidFrom(dave)

	time.Sleep(2 * time.Millisecond) // give v0 a non-zero duration before mutations
	updatedAlice, err := g.UpdateNode(aliceID, map[string]any{"status": "published"})
	if err != nil {
		t.Fatalf("UpdateNode alice: %v", err)
	}
	if _, err := g.UpdateNode(bobID, map[string]any{"status": "draft"}); err != nil {
		t.Fatalf("UpdateNode bob: %v", err)
	}
	if err := g.DeleteNode(daveID); err != nil {
		t.Fatalf("DeleteNode dave: %v", err)
	}
	// aliceMutation: the millisecond at which alice's v0 ends and v1 begins.
	// Used for the boundary assertion below.
	aliceMutation := updatedAlice.Temporal().UpdatedAt
	// tNow: +1 ms ensures we're strictly past the last mutation's millisecond.
	tNow := types.Instant(time.Now().UnixMilli() + 1)

	// At t0: alice, carol, dave have status=draft; bob has published.
	hits, err := g.NodesByLabelAndProperty("Doc", "status", "draft", QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("draft@t0: %v", err)
	}
	assertNodeSet(t, "draft@t0", hits, []types.NodeID{aliceID, carolID, daveID})

	// At tNow: alice changed, dave deleted; bob and carol have status=draft.
	hits, err = g.NodesByLabelAndProperty("Doc", "status", "draft", QueryOpts{ValidAt: tNow})
	if err != nil {
		t.Fatalf("draft@tNow: %v", err)
	}
	assertNodeSet(t, "draft@tNow", hits, []types.NodeID{bobID, carolID})

	// During [t0, tNow): every node had status=draft at some overlapping
	// moment. The discriminating case is alice — her most-recent overlapping
	// version (v1, status=published) fails the predicate, so the bug omits
	// her. Bob, carol, and dave are found by both correct and buggy
	// implementations (carol's v0 still matches, bob's v1 matches, dave's
	// tombstone preserves status=draft).
	hits, err = g.NodesByLabelAndProperty("Doc", "status", "draft", QueryOpts{ValidStart: t0, ValidEnd: tNow})
	if err != nil {
		t.Fatalf("draft@[t0,tNow): %v", err)
	}
	assertNodeSet(t, "draft@[t0,tNow)", hits, []types.NodeID{aliceID, bobID, carolID, daveID})

	// Boundary: alice's v0 is valid [aliceCreate, aliceMutation). At ValidAt
	// equal to aliceMutation she is on v1 (published), so draft must NOT
	// include her. One millisecond earlier, she is still on v0, so draft MUST
	// include her. Probes the half-open interval semantic.
	hits, err = g.NodesByLabelAndProperty("Doc", "status", "draft", QueryOpts{ValidAt: aliceMutation})
	if err != nil {
		t.Fatalf("draft@aliceMutation: %v", err)
	}
	if containsNodeID(hits, aliceID) {
		t.Errorf("draft@aliceMutation: alice present, but her v1 (published) starts at this instant")
	}
	hits, err = g.NodesByLabelAndProperty("Doc", "status", "draft", QueryOpts{ValidAt: aliceMutation - 1})
	if err != nil {
		t.Fatalf("draft@aliceMutation-1: %v", err)
	}
	if !containsNodeID(hits, aliceID) {
		t.Errorf("draft@aliceMutation-1: alice absent, but her v0 (draft) is still valid at this instant")
	}

	// Negative: a value nobody ever wrote returns empty.
	hits, err = g.NodesByLabelAndProperty("Doc", "status", "archived", QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("archived@t0: %v", err)
	}
	if len(hits) != 0 {
		t.Errorf("archived@t0 returned %d nodes, want 0", len(hits))
	}
}

// TestRelationshipsByType_TemporalOpts_Adversarial verifies
// RelationshipsByType(opts) with a temporal filter includes deleted
// relationships that were valid at the requested time.
//
//	    t0                          tNow
//	    │                            │
//	r1  ┤ KNOWS      ──► (deleted) ✗
//	r2  ┤ WORKS_WITH ──► (deleted) ✗
//	r3  ┤ KNOWS (still alive) ──────┤
func TestRelationshipsByType_TemporalOpts_Adversarial(t *testing.T) {
	g := newTestGraph(t)

	a, err := g.AddNode([]string{"P"}, nil)
	if err != nil {
		t.Fatalf("AddNode a: %v", err)
	}
	b, err := g.AddNode([]string{"P"}, nil)
	if err != nil {
		t.Fatalf("AddNode b: %v", err)
	}
	r1, err := g.AddRelationship("KNOWS", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship r1: %v", err)
	}
	r2, err := g.AddRelationship("WORKS_WITH", a, b, nil)
	if err != nil {
		t.Fatalf("AddRelationship r2: %v", err)
	}
	r3, err := g.AddRelationship("KNOWS", b, a, nil)
	if err != nil {
		t.Fatalf("AddRelationship r3: %v", err)
	}
	r1ID := r1.ID()
	r2ID := r2.ID()
	r3ID := r3.ID()

	// t0: r3 was created last, so its snowflake time is within v0 of all three.
	t0 := g.relValidFrom(r3)

	time.Sleep(2 * time.Millisecond) // give v0 a non-zero duration before deletes
	if err := g.DeleteRelationship(r1ID); err != nil {
		t.Fatalf("DeleteRelationship r1: %v", err)
	}
	if err := g.DeleteRelationship(r2ID); err != nil {
		t.Fatalf("DeleteRelationship r2: %v", err)
	}
	// tNow: +1 ms ensures we're strictly past the last mutation's millisecond.
	tNow := types.Instant(time.Now().UnixMilli() + 1)

	// At t0: KNOWS = {r1, r3}; WORKS_WITH = {r2}.
	got, err := g.RelationshipsByType("KNOWS", QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("KNOWS@t0: %v", err)
	}
	assertRelSet(t, "KNOWS@t0", got, []types.RelID{r1ID, r3ID})
	got, err = g.RelationshipsByType("WORKS_WITH", QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("WORKS_WITH@t0: %v", err)
	}
	assertRelSet(t, "WORKS_WITH@t0", got, []types.RelID{r2ID})

	// At tNow: r1 and r2 deleted; r3 survives. WORKS_WITH is empty.
	got, err = g.RelationshipsByType("KNOWS", QueryOpts{ValidAt: tNow})
	if err != nil {
		t.Fatalf("KNOWS@tNow: %v", err)
	}
	assertRelSet(t, "KNOWS@tNow", got, []types.RelID{r3ID})
	got, err = g.RelationshipsByType("WORKS_WITH", QueryOpts{ValidAt: tNow})
	if err != nil {
		t.Fatalf("WORKS_WITH@tNow: %v", err)
	}
	assertRelSet(t, "WORKS_WITH@tNow", got, []types.RelID{})

	// A type nobody ever used returns empty.
	got, err = g.RelationshipsByType("Phantom", QueryOpts{ValidAt: t0})
	if err != nil {
		t.Fatalf("Phantom@t0: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Phantom@t0 returned %d rels, want 0", len(got))
	}
}
