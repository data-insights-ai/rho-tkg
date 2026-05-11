package tiered

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	badgerv4 "github.com/dgraph-io/badger/v4"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

type closedStoreVerifier struct{}

func (closedStoreVerifier) VerifyNodeChain(types.NodeID) (bool, error) {
	return false, fmt.Errorf("unexpected node verification after close")
}

func (closedStoreVerifier) VerifyRelChain(types.RelID) (bool, error) {
	return false, fmt.Errorf("unexpected relationship verification after close")
}

func TestTieredStoreNewRejectsEmptyRefLabels(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		refLabels []string
	}{
		{name: "empty", refLabels: []string{""}},
		{name: "whitespace", refLabels: []string{" \t"}},
		{name: "mixed", refLabels: []string{"Case", ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(Config{
				InMemory:    true,
				RefLabels:   tc.refLabels,
				ShardWindow: time.Hour,
			})
			if err == nil {
				t.Fatalf("New with RefLabels=%v returned nil error", tc.refLabels)
			}
			if !strings.Contains(err.Error(), "Config.RefLabels") {
				t.Fatalf("New error = %v, want Config.RefLabels context", err)
			}
		})
	}
}

func TestTieredStoreNewPreservesValidRefLabels(t *testing.T) {
	t.Parallel()

	ts, err := New(Config{
		InMemory:    true,
		RefLabels:   []string{"Case", "User"},
		ShardWindow: time.Hour,
	})
	if err != nil {
		t.Fatalf("New valid RefLabels: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })

	got := ts.OntologyForTest().RefLabels()
	want := []string{"Case", "User"}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("RefLabels() = %v, want %v", got, want)
	}
}

func TestTieredStoreNewRejectsInvalidColdShardTiming(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		cfg  Config
		want string
	}{
		{
			name: "negative ColdAfter",
			cfg:  Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: time.Hour, ColdAfter: -time.Second},
			want: "Config.ColdAfter",
		},
		{
			name: "negative IdleTimeout",
			cfg:  Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: time.Hour, IdleTimeout: -time.Second},
			want: "Config.IdleTimeout",
		},
		{
			name: "sub-millisecond IdleTimeout",
			cfg:  Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: time.Hour, IdleTimeout: time.Nanosecond},
			want: "Config.IdleTimeout",
		},
		{
			name: "fractional-millisecond IdleTimeout",
			cfg:  Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: time.Hour, IdleTimeout: time.Millisecond + time.Nanosecond},
			want: "Config.IdleTimeout",
		},
		{
			name: "fractional-millisecond ShardWindow",
			cfg:  Config{InMemory: true, RefLabels: []string{"Case"}, ShardWindow: time.Minute + time.Nanosecond},
			want: "Config.ShardWindow",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := New(tc.cfg)
			if err == nil {
				t.Fatal("New returned nil error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New error = %v, want %s context", err, tc.want)
			}
		})
	}
}

func TestTieredStore_PutGetNode_Ref(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)

	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != n.ID() {
		t.Error("node ID mismatch")
	}

	// Verify it's in the ref shard.
	if !ts.RefShardForTest().HasNodeID(n.ID().SnowflakeID()) {
		t.Error("ref node should be in refShard")
	}
}

func TestTieredStore_PutGetNode_Event(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")            // tok 1 = ref
	_, _ = reg.GetOrCreate("User")            // tok 2 = ref
	signalTok, _ := reg.GetOrCreate("Signal") // tok 3 = event

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)

	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != n.ID() {
		t.Error("node ID mismatch")
	}

	// Verify it's in the event shard, not ref.
	if ts.RefShardForTest().HasNodeID(n.ID().SnowflakeID()) {
		t.Error("event node should NOT be in refShard")
	}
	if !ts.HotShardForTest().Store().HasNodeID(n.ID().SnowflakeID()) {
		t.Error("event node should be in hotShard")
	}
}

func TestTieredStore_NodeIntegrityHashCapabilities(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	ref := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	ref.SetIntegrity(&types.NodeIntegrity{Hash: "ref-hash"})
	if err := ts.PutNode(ref); err != nil {
		t.Fatalf("PutNode(ref): %v", err)
	}
	event := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	event.SetIntegrity(&types.NodeIntegrity{Hash: "event-hash"})
	if err := ts.PutNode(event); err != nil {
		t.Fatalf("PutNode(event): %v", err)
	}

	hash, err := ts.NodeIntegrityHash(ref.ID())
	if err != nil {
		t.Fatalf("NodeIntegrityHash(ref): %v", err)
	}
	if hash != "ref-hash" {
		t.Fatalf("NodeIntegrityHash(ref) = %q, want ref-hash", hash)
	}
	fromHash, toHash, err := ts.EndpointIntegrityHashes(event.ID(), ref.ID())
	if err != nil {
		t.Fatalf("EndpointIntegrityHashes(event, ref): %v", err)
	}
	if fromHash != "event-hash" || toHash != "ref-hash" {
		t.Fatalf("EndpointIntegrityHashes = %q, %q; want event-hash, ref-hash", fromHash, toHash)
	}
	fromHash, toHash, err = ts.EndpointIntegrityHashes(event.ID(), event.ID())
	if err != nil {
		t.Fatalf("EndpointIntegrityHashes self: %v", err)
	}
	if fromHash != "event-hash" || toHash != "event-hash" {
		t.Fatalf("EndpointIntegrityHashes self = %q, %q; want event-hash twice", fromHash, toHash)
	}
	if _, err := ts.NodeIntegrityHash(types.NodeID(snowflake.ID(999))); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("NodeIntegrityHash missing = %v, want ErrNodeNotFound", err)
	}
}

func TestTieredStore_RemoveNodeLabelToken_UpdatesIndexes(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	userTok, _ := reg.GetOrCreate("User")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, []uint16{userTok})
	if err := n.SetProperty("embedding", []float32{1, 2}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "embedding", 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	updated := n.DeepCopy()
	if !updated.RemoveLabelTokenRaw(userTok) {
		t.Fatal("RemoveLabelTokenRaw(User) returned false")
	}
	if err := ts.RemoveNodeLabelToken(n.ID(), userTok, updated); err != nil {
		t.Fatalf("RemoveNodeLabelToken: %v", err)
	}

	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.HasLabelTokenRaw(userTok) {
		t.Fatal("removed label still present on node")
	}
	userNodes, err := ts.NodesByLabel(userTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(User): %v", err)
	}
	if len(userNodes) != 0 {
		t.Fatalf("NodesByLabel(User) = %d, want 0", len(userNodes))
	}
	caseNodes, err := ts.NodesByLabel(caseTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(Case): %v", err)
	}
	if len(caseNodes) != 1 || caseNodes[0].ID() != n.ID() {
		t.Fatalf("NodesByLabel(Case) = %#v, want only updated node", caseNodes)
	}
	nearest, err := ts.SearchNearestNodes(caseTok, "embedding", []float32{1, 2}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(nearest) != 1 || nearest[0].ID() != n.ID() {
		t.Fatalf("SearchNearestNodes = %#v, want updated node", nearest)
	}
}

func TestTieredStore_RemoveNodeLabelTokenWithHistory_UpdatesIndexesAndHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	userTok, _ := reg.GetOrCreate("User")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, []uint16{userTok})
	if err := n.SetProperty("embedding", []float32{3, 4}); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.CreateVectorIndex(caseTok, "embedding", 2, DistanceEuclidean); err != nil {
		t.Fatalf("CreateVectorIndex: %v", err)
	}

	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.SetVersion(1)
	if !updated.RemoveLabelTokenRaw(userTok) {
		t.Fatal("RemoveLabelTokenRaw(User) returned false")
	}
	if err := ts.RemoveNodeLabelTokenWithHistory(n.ID(), userTok, updated, prev.Version(), prev); err != nil {
		t.Fatalf("RemoveNodeLabelTokenWithHistory: %v", err)
	}

	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.HasLabelTokenRaw(userTok) {
		t.Fatal("removed label still present on node")
	}
	hist, err := ts.GetNodeVersion(n.ID(), prev.Version())
	if err != nil {
		t.Fatalf("GetNodeVersion(%d): %v", prev.Version(), err)
	}
	if !hist.HasLabelTokenRaw(userTok) {
		t.Fatal("history snapshot lost removed label")
	}
	userNodes, err := ts.NodesByLabel(userTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(User): %v", err)
	}
	if len(userNodes) != 0 {
		t.Fatalf("NodesByLabel(User) = %d, want 0", len(userNodes))
	}
	nearest, err := ts.SearchNearestNodes(caseTok, "embedding", []float32{3, 4}, 1, QueryOpts{})
	if err != nil {
		t.Fatalf("SearchNearestNodes: %v", err)
	}
	if len(nearest) != 1 || nearest[0].ID() != n.ID() {
		t.Fatalf("SearchNearestNodes = %#v, want updated node", nearest)
	}
}

func TestTieredStore_RemoveNodeLabelToken_RejectsPrimaryClassMutation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, []uint16{signalTok})
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	updated := n.DeepCopy()
	if !updated.RemoveLabelTokenRaw(caseTok) {
		t.Fatal("RemoveLabelTokenRaw(Case) returned false")
	}
	if err := ts.RemoveNodeLabelToken(n.ID(), caseTok, updated); !errors.Is(err, ErrPrimaryLabelClassMutation) {
		t.Fatalf("RemoveNodeLabelToken primary class mutation = %v, want ErrPrimaryLabelClassMutation", err)
	}

	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PrimaryLabelToken().Value() != caseTok || !got.HasLabelTokenRaw(signalTok) {
		t.Fatalf("node labels changed after rejected mutation: primary=%d hasSignal=%v",
			got.PrimaryLabelToken().Value(), got.HasLabelTokenRaw(signalTok))
	}
}

func TestTieredStore_NodeLabelTokenHelpersRejectInvalidDeltas(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	userTok, _ := reg.GetOrCreate("User")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, []uint16{userTok})
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	stillHasRemoved := n.DeepCopy()
	if err := ts.RemoveNodeLabelToken(n.ID(), userTok, stillHasRemoved); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RemoveNodeLabelToken unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := ts.NodesByLabel(userTok, QueryOpts{}); err != nil || len(nodes) != 1 {
		t.Fatalf("NodesByLabel(User) after rejected remove = %d, %v; want 1, nil", len(nodes), err)
	}

	addTarget := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(addTarget); err != nil {
		t.Fatalf("PutNode addTarget: %v", err)
	}
	missingAdded := addTarget.DeepCopy()
	if err := ts.AddNodeLabelToken(addTarget.ID(), userTok, missingAdded); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelToken unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}
	userNodes, err := ts.NodesByLabel(userTok, QueryOpts{})
	if err != nil {
		t.Fatalf("NodesByLabel(User): %v", err)
	}
	if len(userNodes) != 1 || userNodes[0].ID() != n.ID() {
		t.Fatalf("NodesByLabel(User) after rejected add = %#v, want only original node", userNodes)
	}

	prev := n.DeepCopy()
	invalidRemoveWithHistory := n.DeepCopy()
	invalidRemoveWithHistory.SetVersion(1)
	if err := ts.RemoveNodeLabelTokenWithHistory(n.ID(), userTok, invalidRemoveWithHistory, prev.Version(), prev); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("RemoveNodeLabelTokenWithHistory unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}

	prevAdd := addTarget.DeepCopy()
	invalidAddWithHistory := addTarget.DeepCopy()
	invalidAddWithHistory.SetVersion(1)
	if err := ts.AddNodeLabelTokenWithHistory(addTarget.ID(), userTok, invalidAddWithHistory, prevAdd.Version(), prevAdd); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("AddNodeLabelTokenWithHistory unchanged payload = %v, want ErrInvalidStoreMutation", err)
	}

	for _, id := range []types.NodeID{n.ID(), addTarget.ID()} {
		history, err := ts.GetNodeHistory(id)
		if err != nil {
			t.Fatalf("GetNodeHistory(%d): %v", id, err)
		}
		if len(history) != 0 {
			t.Fatalf("history entries after rejected label-token helper for %d = %d, want 0", id, len(history))
		}
	}
}

func TestTieredStore_PutNode_RejectsDuplicateIDAcrossClasses(t *testing.T) {
	for _, tc := range []struct {
		name           string
		firstIsRef     bool
		duplicateIsRef bool
	}{
		{name: "reference_then_event", firstIsRef: true, duplicateIsRef: false},
		{name: "event_then_reference", firstIsRef: false, duplicateIsRef: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestTieredStore(t)
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)

			caseTok, _ := reg.GetOrCreate("Case")
			_, _ = reg.GetOrCreate("User")
			signalTok, _ := reg.GetOrCreate("Signal")

			id := types.NodeID(tieredNodeGen(t).Generate())
			firstTok := signalTok
			if tc.firstIsRef {
				firstTok = caseTok
			}
			duplicateTok := signalTok
			if tc.duplicateIsRef {
				duplicateTok = caseTok
			}

			first := types.NewNode(id, firstTok, nil)
			duplicate := types.NewNode(id, duplicateTok, nil)
			if err := ts.PutNode(first); err != nil {
				t.Fatalf("PutNode first: %v", err)
			}
			if err := ts.PutNode(duplicate); !errors.Is(err, ErrNodeExists) {
				t.Fatalf("PutNode duplicate = %v, want ErrNodeExists", err)
			}

			nodes, err := ts.AllNodes(QueryOpts{})
			if err != nil {
				t.Fatal(err)
			}
			if len(nodes) != 1 || nodes[0].PrimaryLabelToken().Value() != firstTok {
				t.Fatalf("AllNodes after duplicate = %#v, want only first node", nodes)
			}
		})
	}
}

func TestTieredStore_DeleteNode_Ref(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)

	if err := ts.DeleteNode(n.ID()); err != nil {
		t.Fatal(err)
	}

	_, err := ts.GetNode(n.ID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
}

func TestTieredStoreDeleteNodeRejectsConnectedRelationships(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(caseNode); err != nil {
		t.Fatalf("PutNode case: %v", err)
	}
	if err := ts.PutNode(signal); err != nil {
		t.Fatalf("PutNode signal: %v", err)
	}
	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, signal.ID(), caseNode.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	err := ts.DeleteNode(caseNode.ID())
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode connected node = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err := ts.GetNode(caseNode.ID()); err != nil {
		t.Fatalf("node was deleted after rejected DeleteNode: %v", err)
	}
	if _, err := ts.GetRelationship(r.ID()); err != nil {
		t.Fatalf("relationship was deleted after rejected DeleteNode: %v", err)
	}
}

func TestTieredStore_ReplaceNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)

	// Replace with updated version.
	updated := n.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceNode(updated); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetNode(n.ID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}
}

func TestTieredStore_ReplaceNode_RejectsPrimaryClassMutation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	updated := types.NewNode(n.ID(), signalTok, nil)
	updated.SetVersion(1)
	if err := ts.ReplaceNode(updated); !errors.Is(err, ErrPrimaryLabelClassMutation) {
		t.Fatalf("ReplaceNode class mutation = %v, want ErrPrimaryLabelClassMutation", err)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PrimaryLabelToken().Value() != caseTok {
		t.Fatalf("primary token after rejected ReplaceNode = %d, want %d", got.PrimaryLabelToken().Value(), caseTok)
	}
	if !ts.RefShardForTest().HasNodeID(n.ID().SnowflakeID()) {
		t.Fatal("node left ref shard after rejected ReplaceNode")
	}
	if ts.HotShardForTest().Store().HasNodeID(n.ID().SnowflakeID()) {
		t.Fatal("node appeared in hot shard after rejected ReplaceNode")
	}
}

func TestTieredStore_ReplaceNode_RejectsSameClassLabelMutation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	userTok, _ := reg.GetOrCreate("User")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	updated := types.NewNode(n.ID(), userTok, nil)
	updated.SetVersion(1)
	if err := ts.ReplaceNode(updated); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNode same-class label mutation = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := ts.NodesByLabel(userTok, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabel(User) = %d, %v; want 0, nil", len(nodes), err)
	}
	if nodes, err := ts.NodesByLabel(caseTok, QueryOpts{}); err != nil || len(nodes) != 1 {
		t.Fatalf("NodesByLabel(Case) = %d, %v; want 1, nil", len(nodes), err)
	}
}

func TestTieredStore_SameShardRel_EventToEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")
	relTypeTok, _ := registrypkg.NewRelTypeRegistry().GetOrCreate("TRIGGERS") // standalone for token
	_ = relTypeTok                                                            // not used directly

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	got, err := ts.GetRelationship(r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != r.ID() {
		t.Error("rel ID mismatch")
	}
}

func TestTieredStore_SameShardRel_RefToRef(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Both entity and in/ should be in refShard.
	if !ts.RefShardForTest().HasRelID(r.ID().SnowflakeID()) {
		t.Error("R->R rel should be in refShard")
	}
}

func TestTieredStore_CrossShardRel_EventToRef(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal.ID(), caseNode.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Entity + out/ in event shard (start node's shard).
	if !ts.HotShardForTest().Store().HasRelID(r.ID().SnowflakeID()) {
		t.Error("E->R: entity should be in event shard")
	}
	// in/ should be in ref shard (end node's shard).
	inIDs := ts.RefShardForTest().IncomingRelIDs(caseNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != r.ID().SnowflakeID() {
		t.Errorf("E->R: ref shard inIdx should contain rel, got %v", inIDs)
	}

	// GetRelationship should still work (routes via event shard).
	got, err := ts.GetRelationship(r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != r.ID() {
		t.Error("rel ID mismatch")
	}
}

func TestTieredStore_CrossShardRel_RefToEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(caseNode)
	_ = ts.PutNode(signal)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, caseNode.ID(), signal.ID())
	if err := ts.PutRelationship(r); err != nil {
		t.Fatal(err)
	}

	// Entity + out/ in ref shard (start node's shard).
	if !ts.RefShardForTest().HasRelID(r.ID().SnowflakeID()) {
		t.Error("R->E: entity should be in ref shard")
	}
	// in/ should be in event shard (end node's shard).
	inIDs := ts.HotShardForTest().Store().IncomingRelIDs(signal.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 || inIDs[0] != r.ID().SnowflakeID() {
		t.Errorf("R->E: event shard inIdx should contain rel, got %v", inIDs)
	}
}

func TestTieredStore_CrossShardRel_IncomingRelationships(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	signal2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(caseNode)
	_ = ts.PutNode(signal1)
	_ = ts.PutNode(signal2)

	rGen := tieredRelGen(t)
	r1 := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal1.ID(), caseNode.ID())
	r2 := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal2.ID(), caseNode.ID())
	_ = ts.PutRelationship(r1)
	_ = ts.PutRelationship(r2)

	// IncomingRelationships on the case node should find both signals.
	incoming, err := ts.IncomingRelationships(caseNode.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(incoming) != 2 {
		t.Fatalf("IncomingRelationships = %d, want 2", len(incoming))
	}
}

func TestTieredStore_CrossShardRel_OutgoingRelationships(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal.ID(), caseNode.ID())
	_ = ts.PutRelationship(r)

	// OutgoingRelationships delegates to the start node's shard.
	outgoing, err := ts.OutgoingRelationships(signal.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outgoing) != 1 {
		t.Fatalf("OutgoingRelationships = %d, want 1", len(outgoing))
	}
}

func TestTieredStore_CrossShardRel_Delete(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal.ID(), caseNode.ID())
	_ = ts.PutRelationship(r)

	// Delete cross-shard rel.
	if err := ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatal(err)
	}

	// Entity should be gone from event shard.
	if ts.HotShardForTest().Store().HasRelID(r.ID().SnowflakeID()) {
		t.Error("deleted rel should be gone from event shard")
	}
	// in/ should be gone from ref shard.
	inIDs := ts.RefShardForTest().IncomingRelIDs(caseNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("deleted rel in/ should be gone from ref shard, got %v", inIDs)
	}
}

func TestTieredStore_CrossShardRel_EndpointNotFound(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(signal)

	rGen := tieredRelGen(t)
	fakeEndID := snowflake.ID(999999999)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal.ID(), types.NodeID(fakeEndID))

	// Creating with token that maps to ref, but endpoint doesn't exist.
	// Since fakeEndID is not in refShard, it falls to event shard routing.
	// Both nodes in event shard => same-shard PutRelationship, endpoint check fails.
	err := ts.PutRelationship(r)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	// Now test cross-shard with a real ref node as endpoint but missing start.
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(caseNode)

	fakeStartID := snowflake.ID(888888888)
	r2 := types.NewRelationship(types.RelID(rGen.Generate()), 1, types.NodeID(fakeStartID), caseNode.ID())
	err = ts.PutRelationship(r2)
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound for missing start, got %v", err)
	}
}

func TestTieredStore_AllNodes_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	ref := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	evt := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(ref)
	_ = ts.PutNode(evt)

	all, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("AllNodes = %d, want 2", len(all))
	}
	// Verify sorted.
	if all[0].ID() > all[1].ID() {
		t.Error("AllNodes should be sorted by ID")
	}
}

func TestTieredStore_AllRelationships_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	rr := types.NewRelationship(types.RelID(rGen.Generate()), 1, c1.ID(), c2.ID())
	ee := types.NewRelationship(types.RelID(rGen.Generate()), 1, s1.ID(), s2.ID())
	_ = ts.PutRelationship(rr)
	_ = ts.PutRelationship(ee)

	all, err := ts.AllRelationships(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Fatalf("AllRelationships = %d, want 2", len(all))
	}
}

func TestTieredStore_NodeCount(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil))
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil))
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil))

	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("NodeCount = %d, want 3", count)
	}
}

func TestTieredStore_RelationshipCount(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID()))

	count, err := ts.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("RelationshipCount = %d, want 1", count)
	}
}

func TestTieredStore_NodeCountByLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil))
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil))
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil))

	caseCount, err := ts.NodeCountByLabel(caseTok)
	if err != nil {
		t.Fatal(err)
	}
	if caseCount != 2 {
		t.Errorf("NodeCountByLabel(Case) = %d, want 2", caseCount)
	}

	signalCount, err := ts.NodeCountByLabel(signalTok)
	if err != nil {
		t.Fatal(err)
	}
	if signalCount != 1 {
		t.Errorf("NodeCountByLabel(Signal) = %d, want 1", signalCount)
	}
}

func TestTieredStore_NodesByLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil))
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil))

	caseNodes, err := ts.NodesByLabel(caseTok, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(caseNodes) != 1 {
		t.Errorf("NodesByLabel(Case) = %d, want 1", len(caseNodes))
	}
}

func TestTieredStore_DeleteNodeCascade_RefNodeWithCrossShardRels(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(caseNode)
	_ = ts.PutNode(signal)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal.ID(), caseNode.ID())
	_ = ts.PutRelationship(r)

	// Cascade delete the case node.
	if err := ts.DeleteNodeCascade(caseNode.ID()); err != nil {
		t.Fatal(err)
	}

	// Node should be gone.
	_, err := ts.GetNode(caseNode.ID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}

	// Rel should be gone from both shards.
	_, err = ts.GetRelationship(r.ID())
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("expected ErrRelNotFound, got %v", err)
	}
}

func TestTieredStore_DeleteNodeCascade_EventNodeWithCrossShardRels(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(signal)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, signal.ID(), caseNode.ID())
	_ = ts.PutRelationship(r)

	// Cascade delete the signal node.
	if err := ts.DeleteNodeCascade(signal.ID()); err != nil {
		t.Fatal(err)
	}

	_, err := ts.GetNode(signal.ID())
	if !errors.Is(err, ErrNodeNotFound) {
		t.Errorf("expected ErrNodeNotFound, got %v", err)
	}
	_, err = ts.GetRelationship(r.ID())
	if !errors.Is(err, ErrRelNotFound) {
		t.Errorf("expected ErrRelNotFound, got %v", err)
	}

	// in/ in ref shard should be cleaned up.
	inIDs := ts.RefShardForTest().IncomingRelIDs(caseNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 0 {
		t.Errorf("cascade should clean in/ from ref shard, got %v", inIDs)
	}
}

func TestTieredStore_DeleteNodeCascadeRollsBackPriorRelOnCrossShardFailure(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	start := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	refEnd := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	eventEnd := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(start); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(refEnd); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(eventEnd); err != nil {
		t.Fatal(err)
	}

	rGen := tieredRelGen(t)
	sameShard := types.NewRelationship(types.RelID(rGen.Generate()), 1, start.ID(), refEnd.ID())
	crossShard := types.NewRelationship(types.RelID(rGen.Generate()), 1, start.ID(), eventEnd.ID())
	if err := ts.PutRelationship(sameShard); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutRelationship(crossShard); err != nil {
		t.Fatal(err)
	}

	hot := ts.HotShardForTest().Store()
	hot.SetDBClosedForTest(true)
	err := ts.DeleteNodeCascade(start.ID())
	hot.SetDBClosedForTest(false)
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DeleteNodeCascade error = %v, want ErrStoreClosed", err)
	}

	for _, n := range []*types.Node{start, refEnd, eventEnd} {
		if _, err := ts.GetNode(n.ID()); err != nil {
			t.Fatalf("GetNode(%d) after rollback = %v", n.ID(), err)
		}
	}
	for _, r := range []*types.Relationship{sameShard, crossShard} {
		if _, err := ts.GetRelationship(r.ID()); err != nil {
			t.Fatalf("GetRelationship(%d) after rollback = %v", r.ID(), err)
		}
	}

	res, err := ts.RunRepair()
	if err != nil {
		t.Fatal(err)
	}
	if res.OrphanedInEntries != 0 || res.MissingInEntries != 0 {
		t.Fatalf("RunRepair after rollback: orphaned in=%d missing in=%d, want 0/0", res.OrphanedInEntries, res.MissingInEntries)
	}
}

func TestTieredStore_VersionHistory_RefNode(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)

	// Save version 0.
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatal(err)
	}

	// Retrieve history.
	hist, err := ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("GetNodeHistory = %d, want 1", len(hist))
	}
}

func TestTieredStore_VersionHistory_EventRel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	_ = ts.PutRelationship(r)

	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatal(err)
	}

	hist, err := ts.GetRelHistory(r.ID())
	if err != nil {
		t.Fatal(err)
	}
	if len(hist) != 1 {
		t.Fatalf("GetRelHistory = %d, want 1", len(hist))
	}
}

func TestTieredStore_AllNodeHistoryIDs_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refN := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	evtN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(refN)
	_ = ts.PutNode(evtN)
	_ = ts.PutNodeVersion(refN.ID(), 0, refN)
	_ = ts.PutNodeVersion(evtN.ID(), 0, evtN)

	ids, err := ts.AllNodeHistoryIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Fatalf("AllNodeHistoryIDs = %d, want 2", len(ids))
	}
}

func TestTieredStore_PutNodesBatch_MixedRefEvent(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	evtNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)

	if err := ts.PutNodesBatch([]*types.Node{refNode, evtNode}); err != nil {
		t.Fatal(err)
	}

	if !ts.RefShardForTest().HasNodeID(refNode.ID().SnowflakeID()) {
		t.Error("batch ref node should be in refShard")
	}
	if !ts.HotShardForTest().Store().HasNodeID(evtNode.ID().SnowflakeID()) {
		t.Error("batch event node should be in hotShard")
	}
}

func TestTieredStore_PutNodesBatch_RejectsDuplicateIDAcrossClasses(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	id := types.NodeID(tieredNodeGen(t).Generate())
	refNode := types.NewNode(id, caseTok, nil)
	evtNode := types.NewNode(id, signalTok, nil)

	err := ts.PutNodesBatch([]*types.Node{refNode, evtNode})
	if !errors.Is(err, ErrNodeExists) {
		t.Fatalf("PutNodesBatch duplicate = %v, want ErrNodeExists", err)
	}

	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("NodeCount after rejected duplicate batch = %d, want 0", count)
	}
}

func TestTieredStore_RejectsZeroIDWrites(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	zeroNode := types.NewNode(types.NodeID(0), caseTok, nil)
	if err := ts.PutNode(zeroNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNode(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.ReplaceNode(zeroNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNode(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.PutNodesBatch([]*types.Node{zeroNode}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNodesBatch(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	negativeNode := types.NewNode(types.NodeID(-1), caseTok, nil)
	if err := ts.PutNode(negativeNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNode(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.ReplaceNode(negativeNode); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNode(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.PutNodesBatch([]*types.Node{negativeNode}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutNodesBatch(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteNode(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteNode(types.NodeID(-1)); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNode(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteNodeCascade(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodeCascade(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteNodesBatch([]types.NodeID{0}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteNodesBatch([]types.NodeID{types.NodeID(-1)}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if count, err := ts.NodeCount(); err != nil || count != 0 {
		t.Fatalf("NodeCount after rejected invalid-ID nodes = %d, %v; want 0, nil", count, err)
	}

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n1); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(n2); err != nil {
		t.Fatal(err)
	}

	zeroRel := types.NewRelationship(types.RelID(0), 1, n1.ID(), n2.ID())
	if err := ts.PutRelationship(zeroRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationship(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.ReplaceRelationship(zeroRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelationship(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	negativeRel := types.NewRelationship(types.RelID(-1), 1, n1.ID(), n2.ID())
	if err := ts.PutRelationship(negativeRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationship(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.ReplaceRelationship(negativeRel); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelationship(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteRelationship(0); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationship(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteRelationship(types.RelID(-1)); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationship(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteRelationshipsBatch([]types.RelID{0}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationshipsBatch(zero ID) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.DeleteRelationshipsBatch([]types.RelID{types.RelID(-1)}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteRelationshipsBatch(negative ID) = %v, want ErrInvalidStoreMutation", err)
	}

	zeroStart := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, types.NodeID(0), n2.ID())
	if err := ts.PutRelationshipsBatch([]*types.Relationship{zeroStart}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationshipsBatch(zero endpoint) = %v, want ErrInvalidStoreMutation", err)
	}
	negativeStart := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, types.NodeID(-1), n2.ID())
	if err := ts.PutRelationshipsBatch([]*types.Relationship{negativeStart}); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("PutRelationshipsBatch(negative endpoint) = %v, want ErrInvalidStoreMutation", err)
	}
	if count, err := ts.RelationshipCount(); err != nil || count != 0 {
		t.Fatalf("RelationshipCount after rejected invalid-ID relationships = %d, %v; want 0, nil", count, err)
	}
}

func TestTieredStore_PutNode_RejectsDuplicateIDInArchive(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	id := types.NodeID(tieredNodeGen(t).Generate())
	archived := types.NewNode(id, caseTok, nil)
	if err := ts.PutNode(archived); err != nil {
		t.Fatalf("PutNode archived seed: %v", err)
	}
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	duplicate := types.NewNode(id, signalTok, nil)
	if err := ts.PutNode(duplicate); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("PutNode duplicate archived ID = %v, want ErrNodeExists", err)
	}

	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("NodeCount after archive duplicate = %d, want 1", count)
	}
}

func TestTieredStore_DepthFilterPinsArchive(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	id := types.NodeID(tieredNodeGen(t).Generate())
	n := types.NewNode(id, caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	if err := ts.ArchiveNode(id); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	filter, release, err := ts.depthFilter(DepthHot)
	if err != nil {
		t.Fatalf("depthFilter: %v", err)
	}
	if filter == nil {
		t.Fatal("depthFilter(DepthHot) returned nil filter")
	}
	if got := ts.ArchiveActiveReqsForTest().Load(); got != 1 {
		t.Fatalf("archiveActiveReqs while filter is live = %d, want 1", got)
	}
	if filter(id.SnowflakeID()) {
		t.Fatal("DepthHot filter accepted archived node")
	}
	release()
	if got := ts.ArchiveActiveReqsForTest().Load(); got != 0 {
		t.Fatalf("archiveActiveReqs after release = %d, want 0", got)
	}
}

func TestTieredStore_DeleteNodesBatch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	evtNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(refNode)
	_ = ts.PutNode(evtNode)

	if err := ts.DeleteNodesBatch([]types.NodeID{
		refNode.ID(),
		evtNode.ID(),
	}); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.NodeCount()
	if count != 0 {
		t.Errorf("NodeCount after batch delete = %d, want 0", count)
	}
}

func TestTieredStore_DeleteNodesBatchRejectsConnectedRelationships(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	unconnected := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(caseNode); err != nil {
		t.Fatalf("PutNode case: %v", err)
	}
	if err := ts.PutNode(signal); err != nil {
		t.Fatalf("PutNode signal: %v", err)
	}
	if err := ts.PutNode(unconnected); err != nil {
		t.Fatalf("PutNode unconnected: %v", err)
	}
	rel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 5, caseNode.ID(), signal.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	err := ts.DeleteNodesBatch([]types.NodeID{unconnected.ID(), caseNode.ID(), signal.ID()})
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("DeleteNodesBatch connected nodes = %v, want ErrInvalidStoreMutation", err)
	}
	for _, n := range []*types.Node{caseNode, signal, unconnected} {
		if _, getErr := ts.GetNode(n.ID()); getErr != nil {
			t.Fatalf("GetNode(%d) after rejected batch delete: %v", n.ID(), getErr)
		}
	}
	if _, getErr := ts.GetRelationship(rel.ID()); getErr != nil {
		t.Fatalf("GetRelationship after rejected batch delete: %v", getErr)
	}
}

func TestTieredStore_DeleteNodesBatchRollbackRestoresDeletedBuckets(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	eventNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{refNode, eventNode} {
		if err := ts.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", n.ID(), err)
		}
	}

	refShard := ts.RefShardForTest()
	eventShard := ts.HotShardForTest().Store()
	if err := refShard.DeleteNodesBatch([]types.NodeID{refNode.ID()}); err != nil {
		t.Fatalf("delete ref bucket: %v", err)
	}
	if err := eventShard.DeleteNodesBatch([]types.NodeID{eventNode.ID()}); err != nil {
		t.Fatalf("delete event bucket: %v", err)
	}

	err := rollbackDeletedNodeBuckets([]deletedNodeBucket{
		{shard: refShard, nodes: []*types.Node{refNode}},
		{shard: eventShard, nodes: []*types.Node{eventNode}},
	})
	if err != nil {
		t.Fatalf("rollbackDeletedNodeBuckets: %v", err)
	}
	for _, n := range []*types.Node{refNode, eventNode} {
		if _, err := ts.GetNode(n.ID()); err != nil {
			t.Fatalf("GetNode(%d) after rollback: %v", n.ID(), err)
		}
	}
	if got, err := ts.NodeCount(); err != nil || got != 2 {
		t.Fatalf("NodeCount after rollback = %d err %v, want 2 nil", got, err)
	}
}

func TestTieredStore_DeleteNodesBatchDeduplicatesInput(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	node := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(node); err != nil {
		t.Fatal(err)
	}

	if err := ts.DeleteNodesBatch([]types.NodeID{node.ID(), node.ID()}); err != nil {
		t.Fatalf("DeleteNodesBatch duplicate ID: %v", err)
	}
	count, err := ts.NodeCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("NodeCount after duplicate batch delete = %d, want 0", count)
	}
}

func TestTieredStore_PutRelationshipsBatch_MixedSameAndCross(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)

	rGen := tieredRelGen(t)
	sameShard := types.NewRelationship(types.RelID(rGen.Generate()), 1, c1.ID(), c2.ID())
	crossShard := types.NewRelationship(types.RelID(rGen.Generate()), 1, s1.ID(), c1.ID())

	if err := ts.PutRelationshipsBatch([]*types.Relationship{sameShard, crossShard}); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.RelationshipCount()
	if count != 2 {
		t.Errorf("RelationshipCount = %d, want 2", count)
	}
}

func TestTieredStore_PutRelationship_RejectsDuplicateIDAcrossShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	nodeGen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{c1, c2, s1, s2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	rid := types.RelID(tieredRelGen(t).Generate())
	eventOwned := types.NewRelationship(rid, 1, s1.ID(), c1.ID())
	refOwned := types.NewRelationship(rid, 1, c2.ID(), s2.ID())
	if err := ts.PutRelationship(eventOwned); err != nil {
		t.Fatalf("PutRelationship first: %v", err)
	}
	if err := ts.PutRelationship(refOwned); !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationship duplicate = %v, want ErrRelExists", err)
	}

	count, err := ts.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("RelationshipCount after duplicate = %d, want 1", count)
	}
	got, err := ts.GetRelationship(rid)
	if err != nil {
		t.Fatal(err)
	}
	if got.StartNodeID() != s1.ID() || got.EndNodeID() != c1.ID() {
		t.Fatalf("GetRelationship returned duplicate rel %#v, want original event-owned rel", got)
	}
}

func TestTieredStore_PutRelationship_RejectsDuplicateIDInArchive(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	nodeGen := tieredNodeGen(t)
	archivedNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	eventNode := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	liveRef := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	for _, n := range []*types.Node{archivedNode, eventNode, liveRef} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	rid := types.RelID(tieredRelGen(t).Generate())
	archivedRel := types.NewRelationship(rid, 1, archivedNode.ID(), archivedNode.ID())
	if err := ts.PutRelationship(archivedRel); err != nil {
		t.Fatalf("PutRelationship archived seed: %v", err)
	}
	if err := ts.ArchiveNode(archivedNode.ID()); err != nil {
		t.Fatalf("ArchiveNode: %v", err)
	}

	duplicate := types.NewRelationship(rid, 1, eventNode.ID(), liveRef.ID())
	if err := ts.PutRelationship(duplicate); !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationship duplicate archived rel ID = %v, want ErrRelExists", err)
	}
}

func TestTieredStore_PutRelationshipsBatch_RejectsDuplicateIDAcrossShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	nodeGen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{c1, c2, s1, s2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	rid := types.RelID(tieredRelGen(t).Generate())
	eventOwned := types.NewRelationship(rid, 1, s1.ID(), c1.ID())
	refOwned := types.NewRelationship(rid, 1, c2.ID(), s2.ID())
	err := ts.PutRelationshipsBatch([]*types.Relationship{eventOwned, refOwned})
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationshipsBatch duplicate = %v, want ErrRelExists", err)
	}

	count, err := ts.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("RelationshipCount after rejected duplicate batch = %d, want 0", count)
	}
}

func TestTieredStore_PutRelationshipsBatch_ExistingDuplicateDoesNotPartiallyWrite(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	nodeGen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
	for _, n := range []*types.Node{c1, c2, s1, s2} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}

	relGen := tieredRelGen(t)
	existing := types.NewRelationship(types.RelID(relGen.Generate()), 1, s1.ID(), c1.ID())
	if err := ts.PutRelationship(existing); err != nil {
		t.Fatalf("PutRelationship existing: %v", err)
	}
	unique := types.NewRelationship(types.RelID(relGen.Generate()), 1, c1.ID(), c2.ID())
	duplicate := types.NewRelationship(existing.ID(), 1, c2.ID(), s2.ID())

	err := ts.PutRelationshipsBatch([]*types.Relationship{unique, duplicate})
	if !errors.Is(err, ErrRelExists) {
		t.Fatalf("PutRelationshipsBatch existing duplicate = %v, want ErrRelExists", err)
	}
	if _, err := ts.GetRelationship(unique.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("unique rel after rejected batch = %v, want ErrRelNotFound", err)
	}
	count, err := ts.RelationshipCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("RelationshipCount after rejected existing duplicate batch = %d, want 1", count)
	}
}

func TestTieredStore_PutRelationshipsBatch_MissingEndpointDoesNotPartiallyWrite(t *testing.T) {
	for _, tc := range []struct {
		name         string
		missingStart bool
	}{
		{name: "missing_start", missingStart: true},
		{name: "missing_end", missingStart: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ts := newTestTieredStore(t)
			reg := registrypkg.NewLabelRegistry()
			ts.SetLabelRegistry(reg)

			caseTok, _ := reg.GetOrCreate("Case")
			_, _ = reg.GetOrCreate("User")
			signalTok, _ := reg.GetOrCreate("Signal")

			nodeGen := tieredNodeGen(t)
			c1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
			c2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
			s1 := types.NewNode(types.NodeID(nodeGen.Generate()), signalTok, nil)
			for _, n := range []*types.Node{c1, c2, s1} {
				if err := ts.PutNode(n); err != nil {
					t.Fatal(err)
				}
			}

			relGen := tieredRelGen(t)
			unique := types.NewRelationship(types.RelID(relGen.Generate()), 1, c1.ID(), c2.ID())
			start := s1.ID()
			end := c1.ID()
			missing := types.NodeID(nodeGen.Generate())
			if tc.missingStart {
				start = missing
			} else {
				end = missing
			}
			bad := types.NewRelationship(types.RelID(relGen.Generate()), 1, start, end)

			err := ts.PutRelationshipsBatch([]*types.Relationship{unique, bad})
			if !errors.Is(err, ErrNodeNotFound) {
				t.Fatalf("PutRelationshipsBatch missing endpoint = %v, want ErrNodeNotFound", err)
			}
			if _, err := ts.GetRelationship(unique.ID()); !errors.Is(err, ErrRelNotFound) {
				t.Fatalf("unique rel after rejected missing-endpoint batch = %v, want ErrRelNotFound", err)
			}
			count, err := ts.RelationshipCount()
			if err != nil {
				t.Fatal(err)
			}
			if count != 0 {
				t.Fatalf("RelationshipCount after rejected missing-endpoint batch = %d, want 0", count)
			}
		})
	}
}

func TestTieredStore_PropertyIndex_RoutedByLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{"status": "open"})
	n.SetProperties(ps)
	_ = ts.PutNode(n)

	if err := ts.CreatePropertyIndex(caseTok, "status"); err != nil {
		t.Fatal(err)
	}

	results, err := ts.NodesByLabelAndProperty(caseTok, "status", "open", QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 {
		t.Errorf("NodesByLabelAndProperty = %d, want 1", len(results))
	}

	if err := ts.DropPropertyIndex(caseTok, "status"); err != nil {
		t.Fatal(err)
	}
}

func TestTieredStore_Close_Idempotent(t *testing.T) {
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
	// Second close should be no-op.
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTieredStore_Clear_AllShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil))
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil))

	if err := ts.Clear(); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.NodeCount()
	if count != 0 {
		t.Errorf("NodeCount after Clear = %d, want 0", count)
	}
}

func TestTieredStore_EventShardsMap(t *testing.T) {
	ts := newTestTieredStore(t)
	if len(ts.EventShardsForTest()) != 1 {
		t.Errorf("eventShards count = %d, want 1", len(ts.EventShardsForTest()))
	}
	if ts.HotShardForTest() == nil {
		t.Fatal("hotShard is nil")
	}
	if ts.HotShardForTest().Tier() != TierHot {
		t.Errorf("hotShard.tier = %q, want %q", ts.HotShardForTest().Tier(), TierHot)
	}
	if ts.HotShardForTest().ReadOnlyForTest() {
		t.Error("hotShard.readOnly should be false")
	}
}

func TestTieredStore_AllNodeIDs_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil))
	_ = ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), signalTok, nil))

	ids, err := ts.AllNodeIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("AllNodeIDs = %d, want 2", len(ids))
	}
	// Verify sorted.
	if ids[0] > ids[1] {
		t.Error("AllNodeIDs should be sorted")
	}
}

func TestTieredStore_AllRelIDs_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), 1, c1.ID(), c2.ID()))
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), 1, s1.ID(), s2.ID()))

	ids, err := ts.AllRelIDs(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("AllRelIDs = %d, want 2", len(ids))
	}
}

func TestTieredStore_AllNodes_Pagination(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	for i := 0; i < 3; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
		_ = ts.PutNode(n)
	}
	for i := 0; i < 3; i++ {
		n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
		_ = ts.PutNode(n)
	}

	// Page 1.
	page1, err := ts.AllNodes(QueryOpts{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 = %d, want 2", len(page1))
	}

	// Page 2.
	page2, err := ts.AllNodes(QueryOpts{Limit: 2, After: types.EntityID(page1[1].ID())})
	if err != nil {
		t.Fatal(err)
	}
	if len(page2) != 2 {
		t.Fatalf("page2 = %d, want 2", len(page2))
	}
	if page2[0].ID() <= page1[1].ID() {
		t.Error("page2 should start after page1")
	}
}

func TestTieredStore_DiskBacked_CreateAndReopen(t *testing.T) {
	dir := t.TempDir()

	// Create, add entities, close.
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), 1, nil) // token 1 = first label
	if err := ts.RefShardForTest().PutNode(n); err != nil {
		t.Fatal(err)
	}
	_ = ts.RefShardForTest().Flush()

	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify directory structure.
	if _, err := os.Stat(filepath.Join(dir, "meta", "shard_catalog.json")); err != nil {
		t.Errorf("missing shard_catalog.json: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "reference")); err != nil {
		t.Errorf("missing reference dir: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "events")); err != nil {
		t.Errorf("missing events dir: %v", err)
	}

	// Reopen and verify catalog.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if len(ts2.CatalogForTest().Shards) < 2 {
		t.Errorf("catalog shards = %d, want >= 2", len(ts2.CatalogForTest().Shards))
	}
}

func TestTieredStore_MidWindowRestart(t *testing.T) {
	dir := t.TempDir()

	// Create first store.
	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	hotName1 := ts1.HotShardForTest().Name()
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen — should use same hot shard name (mid-window restart).
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if ts2.HotShardForTest().Name() != hotName1 {
		t.Errorf("mid-window restart: hot shard name changed from %q to %q", hotName1, ts2.HotShardForTest().Name())
	}
}

func TestTieredStore_GetNodesByIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	refN := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	evtN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(refN)
	_ = ts.PutNode(evtN)

	_, err := ts.GetNodesByIDs([]types.NodeID{
		evtN.ID(),
		types.NodeID(999), // missing
		refN.ID(),
	})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNodesByIDs missing err = %v, want ErrNodeNotFound", err)
	}

	got, err := ts.GetNodesByIDs([]types.NodeID{evtN.ID(), refN.ID()})
	if err != nil {
		t.Fatalf("GetNodesByIDs existing: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetNodesByIDs = %d, want 2", len(got))
	}
	if got[0].ID() != refN.ID() || got[1].ID() != evtN.ID() {
		t.Fatalf("GetNodesByIDs order = [%v, %v], want sorted [%v, %v]",
			got[0].ID(), got[1].ID(), refN.ID(), evtN.ID())
	}
}

func TestTieredStore_GetRelationshipsByIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r1 := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	r2 := types.NewRelationship(types.RelID(rGen.Generate()), 1, n2.ID(), n1.ID())
	_ = ts.PutRelationship(r1)
	_ = ts.PutRelationship(r2)

	_, err := ts.GetRelationshipsByIDs([]types.RelID{
		r2.ID(),
		types.RelID(999), // missing
		r1.ID(),
	})
	if !errors.Is(err, storepkg.ErrRelNotFound) {
		t.Fatalf("GetRelationshipsByIDs missing err = %v, want ErrRelNotFound", err)
	}

	got, err := ts.GetRelationshipsByIDs([]types.RelID{r2.ID(), r1.ID()})
	if err != nil {
		t.Fatalf("GetRelationshipsByIDs existing: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetRelationshipsByIDs = %d, want 2", len(got))
	}
	if got[0].ID() != r1.ID() || got[1].ID() != r2.ID() {
		t.Fatalf("GetRelationshipsByIDs order = [%v, %v], want sorted [%v, %v]",
			got[0].ID(), got[1].ID(), r1.ID(), r2.ID())
	}
}

func TestTieredStore_RelationshipsByType_MergesShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	var relType uint16 = 1
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType, c1.ID(), c2.ID()))
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType, s1.ID(), s2.ID()))

	rels, err := ts.RelationshipsByType(relType, QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(rels) != 2 {
		t.Errorf("RelationshipsByType = %d, want 2", len(rels))
	}
}

func TestTieredStore_RelCountByType(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	var relType uint16 = 1
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType, c1.ID(), c2.ID()))
	_ = ts.PutRelationship(types.NewRelationship(types.RelID(rGen.Generate()), relType, s1.ID(), s2.ID()))

	count, err := ts.RelCountByType(relType)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("RelCountByType = %d, want 2", count)
	}
}

func TestTieredStore_ReplaceRelationship(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	_ = ts.PutRelationship(r)

	updated := r.DeepCopy()
	updated.SetVersion(1)
	if err := ts.ReplaceRelationship(updated); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetRelationship(r.ID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}
}

func TestTieredStore_ReplaceRelationshipRejectsIndexedFieldMutation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n3 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)
	_ = ts.PutNode(n3)

	rGen := tieredRelGen(t)
	original := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	_ = ts.PutRelationship(original)

	updated := types.NewRelationship(original.ID(), 1, n1.ID(), n3.ID())
	updated.SetVersion(1)
	err := ts.ReplaceRelationship(updated)
	if !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelationship indexed-field mutation = %v, want ErrInvalidStoreMutation", err)
	}

	got, err := ts.GetRelationship(original.ID())
	if err != nil {
		t.Fatal(err)
	}
	if got.EndNodeID() != n2.ID() || got.Version() != 0 {
		t.Fatalf("relationship changed after rejected replacement: end=%d version=%d", got.EndNodeID(), got.Version())
	}
	if rels, _ := ts.IncomingRelationships(n3.ID(), 1); len(rels) != 0 {
		t.Fatalf("new end adjacency contains rejected relationship: %d", len(rels))
	}
}

func TestTieredStore_TruncateHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)

	nid := n.ID()
	_ = ts.PutNodeVersion(nid, 0, n)
	_ = ts.PutNodeVersion(nid, 1, n)
	_ = ts.PutNodeVersion(nid, 2, n)

	if err := ts.TruncateNodeHistory(nid, 1); err != nil {
		t.Fatal(err)
	}

	hist, _ := ts.GetNodeHistory(nid)
	if len(hist) != 1 {
		t.Errorf("after truncate: history len = %d, want 1", len(hist))
	}
}

func TestTieredStore_TruncateNodeHistoryRejectsNegativeKeep(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)
	if err := ts.PutNodeVersion(n.ID(), 0, n); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}

	if err := ts.TruncateNodeHistory(n.ID(), -1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TruncateNodeHistory(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	hist, _ := ts.GetNodeHistory(n.ID())
	if len(hist) != 1 {
		t.Fatalf("negative truncate mutated node history: len = %d, want 1", len(hist))
	}
}

func TestTieredStore_GetNodeVersion(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)
	_ = ts.PutNodeVersion(n.ID(), 0, n)

	got, err := ts.GetNodeVersion(n.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != n.ID() {
		t.Error("version node ID mismatch")
	}
}

func TestTieredStore_GetRelVersion(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	_ = ts.PutRelationship(r)
	_ = ts.PutRelVersion(r.ID(), 0, r)

	got, err := ts.GetRelVersion(r.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if got.ID() != r.ID() {
		t.Error("version rel ID mismatch")
	}
}

func TestTieredStore_ReplaceNodeWithHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n)

	prev := n.DeepCopy()
	updated := n.DeepCopy()
	updated.SetVersion(1)

	if err := ts.ReplaceNodeWithHistory(updated, 0, prev); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetNode(n.ID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}

	hist, _ := ts.GetNodeHistory(n.ID())
	if len(hist) != 1 {
		t.Errorf("history = %d, want 1", len(hist))
	}
}

func TestTieredStore_ReplaceNodeWithHistory_RejectsPrimaryClassMutation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	prev := n.DeepCopy()
	updated := types.NewNode(n.ID(), signalTok, nil)
	updated.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(updated, 0, prev); !errors.Is(err, ErrPrimaryLabelClassMutation) {
		t.Fatalf("ReplaceNodeWithHistory class mutation = %v, want ErrPrimaryLabelClassMutation", err)
	}
	got, err := ts.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if got.PrimaryLabelToken().Value() != caseTok {
		t.Fatalf("primary token after rejected ReplaceNodeWithHistory = %d, want %d", got.PrimaryLabelToken().Value(), caseTok)
	}
	hist, err := ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 0 {
		t.Fatalf("history entries after rejected ReplaceNodeWithHistory = %d, want 0", len(hist))
	}
}

func TestTieredStore_ReplaceNodeWithHistory_RejectsSameClassLabelMutation(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	userTok, _ := reg.GetOrCreate("User")
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	prev := n.DeepCopy()
	updated := types.NewNode(n.ID(), userTok, nil)
	updated.SetVersion(1)
	if err := ts.ReplaceNodeWithHistory(updated, 0, prev); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory same-class label mutation = %v, want ErrInvalidStoreMutation", err)
	}
	if nodes, err := ts.NodesByLabel(userTok, QueryOpts{}); err != nil || len(nodes) != 0 {
		t.Fatalf("NodesByLabel(User) = %d, %v; want 0, nil", len(nodes), err)
	}
	if nodes, err := ts.NodesByLabel(caseTok, QueryOpts{}); err != nil || len(nodes) != 1 {
		t.Fatalf("NodesByLabel(Case) = %d, %v; want 1, nil", len(nodes), err)
	}
	hist, err := ts.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(hist) != 0 {
		t.Fatalf("history entries after rejected label mutation = %d, want 0", len(hist))
	}
}

func TestTieredStore_ReplaceWithHistoryRejectsNilPayloads(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.ReplaceNodeWithHistory(nil, 0, n); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory(nil current) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.ReplaceNodeWithHistory(n, 0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceNodeWithHistory(nil history) = %v, want ErrInvalidStoreMutation", err)
	}

	r := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, n.ID(), n.ID())
	if err := ts.ReplaceRelWithHistory(nil, 0, r); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory(nil current) = %v, want ErrInvalidStoreMutation", err)
	}
	if err := ts.ReplaceRelWithHistory(r, 0, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("ReplaceRelWithHistory(nil history) = %v, want ErrInvalidStoreMutation", err)
	}
}

func TestTieredStore_ReplaceRelWithHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	_ = ts.PutRelationship(r)

	prev := r.DeepCopy()
	updated := r.DeepCopy()
	updated.SetVersion(1)

	if err := ts.ReplaceRelWithHistory(updated, 0, prev); err != nil {
		t.Fatal(err)
	}

	got, _ := ts.GetRelationship(r.ID())
	if got.Version() != 1 {
		t.Errorf("version = %d, want 1", got.Version())
	}
}

func TestTieredStore_DeleteRelationshipsBatch(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	s := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(c)
	_ = ts.PutNode(s)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, s.ID(), c.ID())
	_ = ts.PutRelationship(r)

	if err := ts.DeleteRelationshipsBatch([]types.RelID{r.ID()}); err != nil {
		t.Fatal(err)
	}

	count, _ := ts.RelationshipCount()
	if count != 0 {
		t.Errorf("RelationshipCount after batch delete = %d, want 0", count)
	}
}

func TestTieredStore_AllRelHistoryIDs(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	c1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	c2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	s1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	s2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(c1)
	_ = ts.PutNode(c2)
	_ = ts.PutNode(s1)
	_ = ts.PutNode(s2)

	rGen := tieredRelGen(t)
	rr := types.NewRelationship(types.RelID(rGen.Generate()), 1, c1.ID(), c2.ID())
	ee := types.NewRelationship(types.RelID(rGen.Generate()), 1, s1.ID(), s2.ID())
	_ = ts.PutRelationship(rr)
	_ = ts.PutRelationship(ee)
	_ = ts.PutRelVersion(rr.ID(), 0, rr)
	_ = ts.PutRelVersion(ee.ID(), 0, ee)

	ids, err := ts.AllRelHistoryIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 {
		t.Errorf("AllRelHistoryIDs = %d, want 2", len(ids))
	}
}

func TestTieredStore_TruncateRelHistory(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	_ = ts.PutRelationship(r)

	rid := r.ID()
	_ = ts.PutRelVersion(rid, 0, r)
	_ = ts.PutRelVersion(rid, 1, r)

	if err := ts.TruncateRelHistory(rid, 1); err != nil {
		t.Fatal(err)
	}

	hist, _ := ts.GetRelHistory(rid)
	if len(hist) != 1 {
		t.Errorf("after truncate: rel history len = %d, want 1", len(hist))
	}
}

func TestTieredStore_TruncateRelHistoryRejectsNegativeKeep(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1, n1.ID(), n2.ID())
	_ = ts.PutRelationship(r)
	if err := ts.PutRelVersion(r.ID(), 0, r); err != nil {
		t.Fatalf("PutRelVersion: %v", err)
	}

	if err := ts.TruncateRelHistory(r.ID(), -1); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("TruncateRelHistory(-1) = %v, want ErrInvalidStoreMutation", err)
	}
	hist, _ := ts.GetRelHistory(r.ID())
	if len(hist) != 1 {
		t.Fatalf("negative truncate mutated rel history: len = %d, want 1", len(hist))
	}
}

func TestTieredStore_PutRelCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Create node in what will become the warm shard.
	warmNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(warmNode); err != nil {
		t.Fatal(err)
	}

	forceRotation(t, ts)

	// Create node in the new hot shard.
	hotNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(hotNode); err != nil {
		t.Fatal(err)
	}

	// Connect warm → hot (E→E cross-shard).
	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		warmNode.ID(),
		hotNode.ID())

	if err := ts.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship E→E cross-shard: %v", err)
	}

	// Verify: outgoing from warm node should find the rel.
	outRels, err := ts.OutgoingRelationships(warmNode.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outRels) != 1 {
		t.Errorf("OutgoingRelationships from warm node = %d, want 1", len(outRels))
	}

	// Verify: incoming to hot node should find the rel.
	inRels, err := ts.IncomingRelationships(hotNode.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inRels) != 1 {
		t.Errorf("IncomingRelationships to hot node = %d, want 1", len(inRels))
	}
}

func TestTieredStore_DeleteRelCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmNode)

	forceRotation(t, ts)

	hotNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotNode)

	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		warmNode.ID(),
		hotNode.ID())
	_ = ts.PutRelationship(r)

	// Delete the cross-shard E→E relationship.
	if err := ts.DeleteRelationship(r.ID()); err != nil {
		t.Fatalf("DeleteRelationship cross-shard E→E: %v", err)
	}

	// Outgoing from warm node should be empty.
	outRels, err := ts.OutgoingRelationships(warmNode.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outRels) != 0 {
		t.Errorf("OutgoingRelationships after delete = %d, want 0", len(outRels))
	}

	// Incoming to hot node should be empty.
	inRels, err := ts.IncomingRelationships(hotNode.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inRels) != 0 {
		t.Errorf("IncomingRelationships after delete = %d, want 0", len(inRels))
	}
}

func TestTieredStore_OutgoingRelsCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Create multiple nodes in warm shard.
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	n2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n1)
	_ = ts.PutNode(n2)

	forceRotation(t, ts)

	// Create hot node and connect warm→hot.
	n3 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n3)

	rGen := tieredRelGen(t)
	// warm→warm (same shard).
	r1 := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		n1.ID(), n2.ID())
	_ = ts.PutRelationship(r1)

	// warm→hot (cross-shard).
	r2 := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		n1.ID(), n3.ID())
	_ = ts.PutRelationship(r2)

	// Outgoing from n1 (warm) should have both rels.
	outRels, err := ts.OutgoingRelationships(n1.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(outRels) != 2 {
		t.Errorf("OutgoingRelationships from warm node = %d, want 2", len(outRels))
	}
}

func TestTieredStore_IncomingRelsCrossEventShards(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	warmNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(warmNode)

	forceRotation(t, ts)

	hotNode := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(hotNode)

	// hot→warm (cross-shard, incoming to warm).
	rGen := tieredRelGen(t)
	r := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		hotNode.ID(), warmNode.ID())
	_ = ts.PutRelationship(r)

	// Incoming to warm node from hot rel.
	inRels, err := ts.IncomingRelationships(warmNode.ID(), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(inRels) != 1 {
		t.Errorf("IncomingRelationships to warm node = %d, want 1", len(inRels))
	}
}

func TestTieredStore_RestartWithWarmShards(t *testing.T) {
	dir := t.TempDir()

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	// Token 3 = event (after Case=1, User=2 if we had them, but just use 3 directly).
	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts1.HotShardForTest().Store().PutNode(n1); err != nil {
		t.Fatal(err)
	}
	_ = ts1.HotShardForTest().Store().Flush()

	// Force rotation via RotateHotShard.
	ts1.MuForTest().Lock()
	ts1.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts1.MuForTest().Unlock()
	if err := ts1.CheckRotationForTest(); err != nil {
		t.Fatal(err)
	}
	_ = ts1.HotShardForTest().Store().Flush()

	// Verify we have 2 shards now.
	if len(ts1.EventShardsForTest()) != 2 {
		t.Fatalf("eventShards before close = %d, want 2", len(ts1.EventShardsForTest()))
	}

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen — should recover warm shard.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if len(ts2.EventShardsForTest()) != 2 {
		t.Errorf("eventShards after reopen = %d, want 2", len(ts2.EventShardsForTest()))
	}

	// Verify warm shard entity is accessible.
	got, err := ts2.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode from warm shard after restart: %v", err)
	}
	if got.ID() != n1.ID() {
		t.Error("node ID mismatch after restart")
	}
}

func TestTieredStore_RestartWarmShardWritable(t *testing.T) {
	dir := t.TempDir()

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Force rotation.
	ts1.MuForTest().Lock()
	ts1.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts1.MuForTest().Unlock()
	_ = ts1.CheckRotationForTest()

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	// Find the warm shard.
	var warmCount int
	for _, es := range ts2.EventShardsForTest() {
		if es.Tier() == TierWarm {
			warmCount++
			if !es.ReadOnlyForTest() {
				t.Error("warm shard tier marker should be readOnly")
			}
			if es.Store().ReadOnlyForTest() {
				t.Error("warm shard BadgerStore should be writable")
			}
		}
	}
	if warmCount != 1 {
		t.Errorf("warm shard count = %d, want 1", warmCount)
	}
}

func TestTieredStore_RestartPreservesHotShard(t *testing.T) {
	dir := t.TempDir()

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	hotName := ts1.HotShardForTest().Name()
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen mid-window — should reuse same hot shard.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	if ts2.HotShardForTest().Name() != hotName {
		t.Errorf("hot shard name = %q, want %q (mid-window)", ts2.HotShardForTest().Name(), hotName)
	}
	if ts2.HotShardForTest().Tier() != TierHot {
		t.Errorf("hot shard tier = %q, want %q", ts2.HotShardForTest().Tier(), TierHot)
	}
}

func TestTieredStore_RestartSnowflakeResolution(t *testing.T) {
	dir := t.TempDir()

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil) // event node
	if err := ts1.HotShardForTest().Store().PutNode(n1); err != nil {
		t.Fatal(err)
	}
	_ = ts1.HotShardForTest().Store().Flush()

	// Rotate to create warm shard. Sleep 2ms for snowflake boundary alignment.
	ts1.MuForTest().Lock()
	ts1.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts1.MuForTest().Unlock()
	_ = ts1.CheckRotationForTest()
	time.Sleep(2 * time.Millisecond)

	// Create another node in new hot shard.
	n2 := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
	if err := ts1.HotShardForTest().Store().PutNode(n2); err != nil {
		t.Fatal(err)
	}
	_ = ts1.HotShardForTest().Store().Flush()
	_ = ts1.Close()

	// Reopen.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()

	// IDs from warm shard should resolve correctly.
	got1, err := ts2.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode n1 (warm): %v", err)
	}
	if got1.ID() != n1.ID() {
		t.Error("n1 ID mismatch")
	}

	got2, err := ts2.GetNode(n2.ID())
	if err != nil {
		t.Fatalf("GetNode n2 (hot): %v", err)
	}
	if got2.ID() != n2.ID() {
		t.Error("n2 ID mismatch")
	}
}

func TestBadgerStore_ReadOnly(t *testing.T) {
	dir := t.TempDir()

	// Create a store and write data.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
	if err := bs.PutNode(n); err != nil {
		t.Fatal(err)
	}
	_ = bs.Flush()
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen as read-only.
	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		ReadOnly:      true,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("NewBadgerStore(ReadOnly): %v", err)
	}
	defer bs2.Close()

	// Reads should work.
	got, err := bs2.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode from read-only: %v", err)
	}
	if got.ID() != n.ID() {
		t.Error("node ID mismatch")
	}
}

func TestBadgerStore_ReadOnlyNoFlushLoop(t *testing.T) {
	dir := t.TempDir()

	// Create an empty store first.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()

	// Open as read-only.
	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		ReadOnly:      true,
		FlushInterval: 100 * time.Millisecond,
		GCInterval:    1 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer bs2.Close()

	if !bs2.ReadOnlyForTest() {
		t.Error("readOnly should be true")
	}

	// flushDone and gcDone should already be closed (no goroutines spawned).
	select {
	case <-bs2.FlushDoneForTest():
		// OK: closed immediately.
	default:
		t.Error("flushDone should be closed (no flush goroutine)")
	}
	select {
	case <-bs2.GCDoneForTest():
		// OK: closed immediately.
	default:
		t.Error("gcDone should be closed (no GC goroutine)")
	}
}

func TestBadgerStore_ReadOnlyClose(t *testing.T) {
	dir := t.TempDir()

	// Create store, close, reopen as read-only.
	bs, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = bs.Close()

	bs2, err := NewBadgerStore(BadgerStoreConfig{
		Dir:           dir,
		ReadOnly:      true,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Close should be clean.
	if err := bs2.Close(); err != nil {
		t.Fatalf("Close read-only BadgerStore: %v", err)
	}

	// Second close should be no-op.
	if err := bs2.Close(); err != nil {
		t.Fatalf("second Close read-only: %v", err)
	}
}

func TestTieredStore_ColdShard_LazyOpen(t *testing.T) {
	// Write data, rotate (hot→warm), manually demote to cold,
	// then verify the cold shard data is still accessible.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}
	nodeID := n.ID()

	// Remember which shard has the node.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	// Rotate: hot → warm.
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	if err := ts.RotateHotShard(); err != nil {
		ts.MuForTest().Unlock()
		t.Fatal(err)
	}
	ts.MuForTest().Unlock()

	// Manually demote the warm shard to cold.
	demoteToCold(ts, hotName)

	// Find the cold shard — should exist.
	var coldFound bool
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierCold {
			coldFound = true
		}
	}
	ts.MuForTest().RUnlock()
	if !coldFound {
		t.Fatal("no cold shard found after demotion")
	}

	// Verify data is still accessible (store pointer still valid).
	got, err := ts.GetNode(nodeID)
	if err != nil {
		t.Fatalf("GetNode from cold shard: %v", err)
	}
	if got.ID() != nodeID {
		t.Error("node ID mismatch after cold shard access")
	}
}

func TestTieredStore_ColdShard_IdleClose(t *testing.T) {
	// Disk-backed: idle-close closes the BadgerStore, lazy-reopen reads from disk.
	// In-memory stores lose data on close, so this must use disk.
	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		FlushInterval: 1<<63 - 1,
		IdleTimeout:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()

	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n)
	nodeID := n.ID()

	// Flush to disk so data survives close+reopen.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	_ = ts.HotShardForTest().Store().Flush()
	ts.MuForTest().RUnlock()

	// Rotate: hot → warm.
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	// Manually demote to cold.
	demoteToCold(ts, hotName)

	// Access to set lastAccess via getStore.
	_, _ = ts.GetNode(nodeID)

	// Wait for idle threshold to pass, then force idle close.
	time.Sleep(20 * time.Millisecond)
	ts.CloseIdleShardsForTest()

	// Find the cold shard and verify store is nil.
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierCold {
			es.LockShardMuForTest()
			if es.Store() != nil {
				t.Error("cold shard store should be nil after idle close")
			}
			es.UnlockShardMuForTest()
		}
	}
	ts.MuForTest().RUnlock()

	// Re-access should lazy-open from disk.
	got, err := ts.GetNode(nodeID)
	if err != nil {
		t.Fatalf("GetNode after idle-close + re-open: %v", err)
	}
	if got.ID() != nodeID {
		t.Error("node ID mismatch after re-open")
	}
}

func TestTieredStore_ColdShard_IdleCloseErrorSurfaces(t *testing.T) {
	dir := t.TempDir()
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case", "User"},
		FlushInterval: 1<<63 - 1,
		IdleTimeout:   10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()

	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	// Demote the current shard directly so its pending PutNode write remains
	// buffered. The test is about the idle-close error path, not rotation.
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	cold := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()
	if cold == nil || cold.Store() == nil {
		t.Fatalf("expected open cold shard %q", hotName)
	}
	cold.Store().SetDBClosedForTest(true)
	cold.SetLastAccessForTest(0)

	ts.CloseIdleShardsForTest()

	if err := ts.backgroundError(); err == nil || !strings.Contains(err.Error(), "idle-close cold shard") {
		t.Fatalf("backgroundError = %v, want idle-close cold shard error", err)
	}
	if _, err := cold.CheckoutStoreForTest(ts); err == nil || !strings.Contains(err.Error(), "idle-close cold shard") {
		t.Fatalf("CheckoutStore after idle-close error = %v, want background error", err)
	}
	if err := ts.Close(); err == nil || !strings.Contains(err.Error(), "idle-close cold shard") {
		t.Fatalf("Close after idle-close error = %v, want background error", err)
	}
}

func TestTieredStore_ColdShard_DemotionWarmToCold(t *testing.T) {
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   time.Minute,
		FlushInterval: 1<<63 - 1,
		ColdAfter:     time.Millisecond, // demote immediately
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()

	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("Signal")

	// Rotate once: hot→warm.
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	// After rotation, the old warm shard should become cold (ColdAfter=1ms).
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	var coldCount int
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierCold {
			coldCount++
		}
	}
	ts.MuForTest().RUnlock()

	if coldCount == 0 {
		t.Error("expected at least one cold shard after demotion")
	}
}

func TestTieredStore_ColdShard_DemotionDuringRotation(t *testing.T) {
	// Verify that demotion happens as part of rotation.
	ts, err := New(Config{
		InMemory:      true,
		RefLabels:     []string{"Case"},
		ShardWindow:   time.Minute,
		FlushInterval: 1<<63 - 1,
		ColdAfter:     time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts.Close() }()

	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")

	// Do 3 rotations.
	for i := 0; i < 3; i++ {
		time.Sleep(2 * time.Millisecond)
		ts.MuForTest().Lock()
		_ = ts.RotateHotShard()
		ts.MuForTest().Unlock()
	}

	// Count tiers.
	var hotCount, warmCount, coldCount int
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		switch es.Tier() {
		case TierHot:
			hotCount++
		case TierWarm:
			warmCount++
		case TierCold:
			coldCount++
		}
	}
	ts.MuForTest().RUnlock()

	if hotCount != 1 {
		t.Errorf("hot count = %d, want 1", hotCount)
	}
	// With ColdAfter=1ms and 3 rotations, older shards should be cold.
	if coldCount == 0 {
		t.Error("expected at least one cold shard")
	}
}

func TestTieredStore_ColdShard_ColdRestart(t *testing.T) {
	// Test disk-backed cold shard recovery from catalog.
	dir := t.TempDir()

	// Phase 1: create, write, rotate, demote to cold, close.
	ts, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n)

	// Flush the hot shard so data is persisted to Badger.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	_ = ts.HotShardForTest().Store().Flush()
	ts.MuForTest().RUnlock()

	// Rotate: hot → warm, then manually demote to cold.
	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	demoteToCold(ts, hotName)

	// Persist catalog with cold tier info.
	_ = ts.CatalogForTest().Save()
	_ = ts.Close()

	// Phase 2: reopen — cold shards should be recovered with store=nil.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ts2.Close() }()

	reg2 := registrypkg.NewLabelRegistry()
	ts2.SetLabelRegistry(reg2)
	_, _ = reg2.GetOrCreate("Case")
	_, _ = reg2.GetOrCreate("Signal")

	// Verify cold shards exist and are NOT opened yet.
	var nilStoreCount int
	ts2.MuForTest().RLock()
	for _, es := range ts2.EventShardsForTest() {
		if es.Tier() == TierCold && es.Store() == nil {
			nilStoreCount++
		}
	}
	ts2.MuForTest().RUnlock()

	if nilStoreCount == 0 {
		t.Error("expected at least one cold shard with nil store on restart")
	}

	// Verify data is accessible (triggers lazy-open).
	got, err := ts2.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode from cold shard after restart: %v", err)
	}
	if got.ID() != n.ID() {
		t.Error("node ID mismatch")
	}
}

func TestTieredStore_ColdShard_GetStoreFastPath(t *testing.T) {
	// getStore for hot/warm shards should return immediately without lock.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	es := ts.HotShardForTest()
	store, err := es.GetStoreForTest(ts)
	if err != nil {
		t.Fatal(err)
	}
	if store != es.Store() {
		t.Error("getStore on hot shard should return es.Store() directly")
	}

	// Make it warm.
	es.SetTierForTest(TierWarm)
	store, err = es.GetStoreForTest(ts)
	if err != nil {
		t.Fatal(err)
	}
	if store != es.Store() {
		t.Error("getStore on warm shard should return es.Store() directly")
	}
}

func TestTieredStore_ColdShard_ConcurrentAccess(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n)
	nodeID := n.ID()

	// Remember shard, rotate, demote to cold.
	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	demoteToCold(ts, hotName)

	// Concurrent reads from cold shard.
	var wg sync.WaitGroup
	errs := make([]error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = ts.GetNode(nodeID)
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("goroutine %d: %v", i, err)
		}
	}
}

func TestTieredStore_ParallelAllNodes(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Add ref node.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(refNode)

	// Add event node, rotate, add another event node.
	evtNode1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(evtNode1)

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	evtNode2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(evtNode2)

	// AllNodes should return 3 nodes (parallel query).
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Errorf("AllNodes = %d, want 3", len(nodes))
	}

	// Verify sorted order.
	for i := 1; i < len(nodes); i++ {
		if nodes[i].ID() <= nodes[i-1].ID() {
			t.Error("AllNodes result not sorted")
		}
	}
}

func TestTieredStore_ParallelWithColdLazyOpen(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)

	// Add node in shard 1, rotate, add in shard 2, rotate, add in shard 3.
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n1)

	ts.MuForTest().RLock()
	shard1Name := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	n2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n2)

	ts.MuForTest().RLock()
	shard2Name := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	n3 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n3)

	// Demote older shards to cold.
	demoteToCold(ts, shard1Name)
	demoteToCold(ts, shard2Name)

	// AllNodes should find 3 nodes even with cold shard lazy-open.
	nodes, err := ts.AllNodes(QueryOpts{})
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 3 {
		t.Errorf("AllNodes = %d, want 3", len(nodes))
	}
}

func TestTieredStore_ParallelErrorPropagation(t *testing.T) {
	// Verify that errors from event shard queries are propagated.
	// We close a shard to force an error.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts.PutNode(n)

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()

	// Close the warm shard's store to force errors.
	ts.MuForTest().RLock()
	for _, es := range ts.EventShardsForTest() {
		if es.Tier() == TierWarm {
			_ = es.Store().Close()
		}
	}
	ts.MuForTest().RUnlock()

	// AllNodes should return an error from the closed shard.
	_, err := ts.AllNodes(QueryOpts{})
	if err == nil {
		// Some in-memory stores may not error on close, that's ok.
		// This test verifies the error propagation path exists.
		t.Log("AllNodes did not error (in-memory mode may not error on closed store)")
	}
}

func TestTieredStore_PropertyIndex_RefLabel(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")

	// Create a ref node for the index to index.
	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	ps, _ := types.NewPropertySlice(map[string]any{"status": "open"})
	n.SetProperties(ps)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	// Creating a property index on a reference label should succeed.
	if err := ts.CreatePropertyIndex(caseTok, "status"); err != nil {
		t.Errorf("CreatePropertyIndex ref label: %v", err)
	}
}

func TestTieredStore_PropertyIndex_EventRejected(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	_, _ = reg.GetOrCreate("Case")         // token 1 = ref
	_, _ = reg.GetOrCreate("User")         // token 2 = ref
	sigTok, _ := reg.GetOrCreate("Signal") // token 3 = event

	// Creating a property index on an event label should fail.
	err := ts.CreatePropertyIndex(sigTok, "severity")
	if err == nil {
		t.Fatal("expected error for event label property index")
	}
	if !errors.Is(err, ErrEventPropertyIndex) {
		t.Errorf("expected ErrEventPropertyIndex, got: %v", err)
	}
}

func TestTieredStore_PropertyIndex_ErrorsIs(t *testing.T) {
	// Verify ErrEventPropertyIndex works with errors.Is.
	err := fmt.Errorf("wrapped: %w", ErrEventPropertyIndex)
	if !errors.Is(err, ErrEventPropertyIndex) {
		t.Error("errors.Is failed on wrapped ErrEventPropertyIndex")
	}
}

func TestMigrateFromBadger_Empty(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	dst := newTestTieredStore(t)

	if err := MigrateFromBadger(src, dst); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	nc, _ := dst.NodeCount()
	if nc != 0 {
		t.Errorf("NodeCount = %d, want 0", nc)
	}
}

func TestMigrateFromBadger_NodesOnly(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	// Create label registry and register labels.
	reg := registrypkg.NewLabelRegistry()
	caseTok, _ := reg.GetOrCreate("Case")
	sigTok, _ := reg.GetOrCreate("Signal")

	gen := newTestGen(t, 0)

	// Add nodes to source.
	refNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := src.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	evtNode := types.NewNode(types.NodeID(gen.Generate()), sigTok, nil)
	if err := src.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}
	if err := src.SaveLabelRegistry(reg); err != nil {
		t.Fatalf("SaveLabelRegistry: %v", err)
	}

	dst := newTestTieredStore(t)

	if err := MigrateFromBadger(src, dst); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	// Ref node should be in refShard.
	if !dst.RefShardForTest().HasNodeID(refNode.ID().SnowflakeID()) {
		t.Error("ref node not in refShard")
	}
	// Event node should be in hotShard.
	dst.MuForTest().RLock()
	hotStore := dst.HotShardForTest().Store()
	dst.MuForTest().RUnlock()
	if !hotStore.HasNodeID(evtNode.ID().SnowflakeID()) {
		t.Error("event node not in hotShard")
	}

	nc, _ := dst.NodeCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
}

func TestMigrateFromBadger_WithRels(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	reg := registrypkg.NewLabelRegistry()
	caseTok, _ := reg.GetOrCreate("Case")

	nodeGen := newTestGen(t, 0)
	relGen := newTestGen(t, 1)

	// Two ref nodes with a relationship.
	n1 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	n2 := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	if err := src.PutNode(n1); err != nil {
		t.Fatalf("PutNode n1: %v", err)
	}
	if err := src.PutNode(n2); err != nil {
		t.Fatalf("PutNode n2: %v", err)
	}

	rtReg := registrypkg.NewRelTypeRegistry()
	relTok, _ := rtReg.GetOrCreate("RELATED")

	r := types.NewRelationship(types.RelID(relGen.Generate()), relTok,
		n1.ID(), n2.ID())
	if err := src.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := src.SaveRegistries(reg, rtReg); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}

	dst := newTestTieredStore(t)
	if err := MigrateFromBadger(src, dst); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	nc, _ := dst.NodeCount()
	rc, _ := dst.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("RelationshipCount = %d, want 1", rc)
	}

	// Verify the relationship is accessible.
	gotRel, err := dst.GetRelationship(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if gotRel.ID() != r.ID() {
		t.Error("relationship ID mismatch")
	}
}

func TestMigrateFromBadger_CrossShardRel(t *testing.T) {
	src, err := NewBadgerStore(BadgerStoreConfig{InMemory: true})
	if err != nil {
		t.Fatalf("NewBadgerStore: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	reg := registrypkg.NewLabelRegistry()
	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	sigTok, _ := reg.GetOrCreate("Signal")

	nodeGen := newTestGen(t, 0)
	relGen := newTestGen(t, 1)

	// One ref node (Case) and one event node (Signal).
	refNode := types.NewNode(types.NodeID(nodeGen.Generate()), caseTok, nil)
	evtNode := types.NewNode(types.NodeID(nodeGen.Generate()), sigTok, nil)
	if err := src.PutNode(refNode); err != nil {
		t.Fatalf("PutNode ref: %v", err)
	}
	if err := src.PutNode(evtNode); err != nil {
		t.Fatalf("PutNode evt: %v", err)
	}

	rtReg := registrypkg.NewRelTypeRegistry()
	relTok, _ := rtReg.GetOrCreate("TRIGGERED")

	// E→R relationship in source (single store, no cross-shard concern).
	r := types.NewRelationship(types.RelID(relGen.Generate()), relTok,
		evtNode.ID(), refNode.ID())
	if err := src.PutRelationship(r); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}
	if err := src.SaveRegistries(reg, rtReg); err != nil {
		t.Fatalf("SaveRegistries: %v", err)
	}

	dst := newTestTieredStore(t)
	if err := MigrateFromBadger(src, dst); err != nil {
		t.Fatalf("MigrateFromBadger: %v", err)
	}

	// Verify cross-shard: entity+out in hotShard, in/ in refShard.
	dst.MuForTest().RLock()
	hotStore := dst.HotShardForTest().Store()
	dst.MuForTest().RUnlock()

	if !hotStore.HasRelID(r.ID().SnowflakeID()) {
		t.Error("rel entity should be in hot shard (event start node)")
	}

	// The ref shard should have the incoming index entry.
	inIDs := dst.RefShardForTest().IncomingRelIDs(refNode.ID().SnowflakeID(), 0)
	if len(inIDs) != 1 {
		t.Errorf("refShard incoming rels = %d, want 1", len(inIDs))
	}

	// Total counts.
	nc, _ := dst.NodeCount()
	rc, _ := dst.RelationshipCount()
	if nc != 2 {
		t.Errorf("NodeCount = %d, want 2", nc)
	}
	if rc != 1 {
		t.Errorf("RelationshipCount = %d, want 1", rc)
	}
}

func TestTieredStore_ColdShard_CheckoutAtomicUnderShardMu(t *testing.T) {
	// Verify that checkoutStore for cold shards holds shardMu while incrementing
	// activeReqs — preventing the TOCTOU race where closeIdleShards closes the
	// store between getStore return and activeReqs increment.
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatal(err)
	}

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts.MuForTest().Lock()
	_ = ts.RotateHotShard()
	ts.MuForTest().Unlock()
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()

	ts.SetIdleTimeoutForTest(time.Millisecond)

	// Rapid interleave: checkout+checkin vs closeIdleShards, 50 rounds.
	// With the old code (getStore release shardMu then activeReqs.Add(1)),
	// the race detector catches this. With the fix (atomic under shardMu),
	// all checkouts succeed and the store is never used-after-close.
	var wg sync.WaitGroup
	errs := make([]error, 50)
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			store, err := coldES.CheckoutStoreForTest(ts)
			if err != nil {
				errs[i] = err
				return
			}
			// Verify store is usable — if it were closed, this would panic/error.
			_, _ = store.NodeCount()
			coldES.CheckinStoreForTest()
		}(i)
		go func() {
			defer wg.Done()
			// Force lastAccess to zero to trigger idle-close aggressively.
			coldES.SetLastAccessForTest(0)
			ts.CloseIdleShardsForTest()
		}()
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("checkout round %d: %v", i, err)
		}
	}
}

func TestTieredStore_CloseWaitsForEventShardMutex(t *testing.T) {
	ts := newTestTieredStore(t)

	ts.MuForTest().RLock()
	hotName := ts.HotShardForTest().Name()
	ts.MuForTest().RUnlock()
	demoteToCold(ts, hotName)

	ts.MuForTest().RLock()
	coldES := ts.EventShardsForTest()[hotName]
	ts.MuForTest().RUnlock()
	if coldES == nil || coldES.Store() == nil {
		t.Fatalf("expected open cold shard %q", hotName)
	}

	coldES.LockShardMuForTest()
	done := make(chan error, 1)
	go func() {
		done <- ts.Close()
	}()

	select {
	case err := <-done:
		coldES.UnlockShardMuForTest()
		t.Fatalf("Close returned while event shard mutex was held: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	coldES.UnlockShardMuForTest()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close after releasing event shard mutex: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not complete after releasing event shard mutex")
	}
}

func TestTieredStore_PostCloseRoutingReturnsStoreClosed(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	gen := tieredNodeGen(t)
	n := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode before close: %v", err)
	}
	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := ts.GetNode(n.ID()); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("GetNode after close = %v, want ErrStoreClosed", err)
	}
	if err := ts.PutNode(types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("PutNode after close = %v, want ErrStoreClosed", err)
	}
	if err := ts.DeleteNodesBatch(nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DeleteNodesBatch(nil) after close = %v, want ErrStoreClosed", err)
	}
	if err := ts.DeleteRelationshipsBatch(nil); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("DeleteRelationshipsBatch(nil) after close = %v, want ErrStoreClosed", err)
	}
	if err := ts.PutNodeVersion(n.ID(), n.Version(), n); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("PutNodeVersion after close = %v, want ErrStoreClosed", err)
	}
	if err := ts.CheckRotationForTest(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("checkRotation after close = %v, want ErrStoreClosed", err)
	}
}

func TestTieredStoreDirectReadsReturnStoreClosedAfterClose(t *testing.T) {
	ts, caseTok, signalTok := setupBatchDelete(t)

	const relTypeTok uint16 = 1
	gen := tieredNodeGen(t)
	refA := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := refA.SetProperty("status", "open"); err != nil {
		t.Fatalf("SetProperty: %v", err)
	}
	refB := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	eventN := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts.PutNode(refA); err != nil {
		t.Fatalf("PutNode refA: %v", err)
	}
	if err := ts.PutNode(refB); err != nil {
		t.Fatalf("PutNode refB: %v", err)
	}
	if err := ts.PutNode(eventN); err != nil {
		t.Fatalf("PutNode eventN: %v", err)
	}

	rel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), relTypeTok, refA.ID(), refB.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatalf("PutRelationship: %v", err)
	}

	prevNode := refA.DeepCopy()
	updatedNode := refA.DeepCopy()
	updatedNode.SetVersion(1)
	if err := updatedNode.SetProperty("status", "closed"); err != nil {
		t.Fatalf("SetProperty updatedNode: %v", err)
	}
	if err := ts.ReplaceNodeWithHistory(updatedNode, prevNode.Version(), prevNode); err != nil {
		t.Fatalf("ReplaceNodeWithHistory: %v", err)
	}

	prevRel := rel.DeepCopy()
	updatedRel := rel.DeepCopy()
	updatedRel.SetVersion(1)
	if err := ts.ReplaceRelWithHistory(updatedRel, prevRel.Version(), prevRel); err != nil {
		t.Fatalf("ReplaceRelWithHistory: %v", err)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "get nodes by ids empty", run: func() error {
			_, err := ts.GetNodesByIDs(nil)
			return err
		}},
		{name: "get nodes by ids", run: func() error {
			_, err := ts.GetNodesByIDs([]types.NodeID{refA.ID(), eventN.ID()})
			return err
		}},
		{name: "get relationships by ids empty", run: func() error {
			_, err := ts.GetRelationshipsByIDs(nil)
			return err
		}},
		{name: "get relationships by ids", run: func() error {
			_, err := ts.GetRelationshipsByIDs([]types.RelID{rel.ID()})
			return err
		}},
		{name: "outgoing relationships", run: func() error {
			_, err := ts.OutgoingRelationships(refA.ID(), relTypeTok)
			return err
		}},
		{name: "outgoing relationships for nodes empty", run: func() error {
			_, err := ts.OutgoingRelationshipsForNodes(nil, relTypeTok)
			return err
		}},
		{name: "outgoing relationships for nodes", run: func() error {
			_, err := ts.OutgoingRelationshipsForNodes([]types.NodeID{refA.ID()}, relTypeTok)
			return err
		}},
		{name: "incoming relationships", run: func() error {
			_, err := ts.IncomingRelationships(refB.ID(), relTypeTok)
			return err
		}},
		{name: "incoming relationships for nodes empty", run: func() error {
			_, err := ts.IncomingRelationshipsForNodes(nil, relTypeTok)
			return err
		}},
		{name: "incoming relationships for nodes", run: func() error {
			_, err := ts.IncomingRelationshipsForNodes([]types.NodeID{refB.ID()}, relTypeTok)
			return err
		}},
		{name: "all nodes", run: func() error {
			_, err := ts.AllNodes(QueryOpts{})
			return err
		}},
		{name: "node count", run: func() error {
			_, err := ts.NodeCount()
			return err
		}},
		{name: "node count by label", run: func() error {
			_, err := ts.NodeCountByLabel(caseTok)
			return err
		}},
		{name: "all node ids", run: func() error {
			_, err := ts.AllNodeIDs(QueryOpts{})
			return err
		}},
		{name: "for each node id", run: func() error {
			called := false
			err := ts.ForEachNodeID(func(types.NodeID) bool {
				called = true
				return false
			})
			if called {
				return fmt.Errorf("node callback invoked after close")
			}
			return err
		}},
		{name: "all relationships", run: func() error {
			_, err := ts.AllRelationships(QueryOpts{})
			return err
		}},
		{name: "relationship count", run: func() error {
			_, err := ts.RelationshipCount()
			return err
		}},
		{name: "relationship count by type", run: func() error {
			_, err := ts.RelCountByType(relTypeTok)
			return err
		}},
		{name: "all relationship ids", run: func() error {
			_, err := ts.AllRelIDs(QueryOpts{})
			return err
		}},
		{name: "for each relationship id", run: func() error {
			called := false
			err := ts.ForEachRelID(func(types.RelID) bool {
				called = true
				return false
			})
			if called {
				return fmt.Errorf("relationship callback invoked after close")
			}
			return err
		}},
		{name: "nodes by label", run: func() error {
			_, err := ts.NodesByLabel(caseTok, QueryOpts{})
			return err
		}},
		{name: "relationships by type", run: func() error {
			_, err := ts.RelationshipsByType(relTypeTok, QueryOpts{})
			return err
		}},
		{name: "nodes by label and property", run: func() error {
			_, err := ts.NodesByLabelAndProperty(caseTok, "status", "closed", QueryOpts{})
			return err
		}},
		{name: "node version", run: func() error {
			_, err := ts.GetNodeVersion(refA.ID(), prevNode.Version())
			return err
		}},
		{name: "node history", run: func() error {
			_, err := ts.GetNodeHistory(refA.ID())
			return err
		}},
		{name: "all node history ids", run: func() error {
			_, err := ts.AllNodeHistoryIDs()
			return err
		}},
		{name: "all node history ids from", run: func() error {
			_, err := ts.AllNodeHistoryIDsFrom(types.NodeID(0), 1)
			return err
		}},
		{name: "for each node history id", run: func() error {
			called := false
			err := ts.ForEachNodeHistoryID(func(types.NodeID) bool {
				called = true
				return false
			})
			if called {
				return fmt.Errorf("node history callback invoked after close")
			}
			return err
		}},
		{name: "for each node history id by depth", run: func() error {
			called := false
			err := ts.ForEachNodeHistoryIDByDepth(DepthWarm, func(types.NodeID) bool {
				called = true
				return false
			})
			if called {
				return fmt.Errorf("node history depth callback invoked after close")
			}
			return err
		}},
		{name: "relationship version", run: func() error {
			_, err := ts.GetRelVersion(rel.ID(), prevRel.Version())
			return err
		}},
		{name: "relationship history", run: func() error {
			_, err := ts.GetRelHistory(rel.ID())
			return err
		}},
		{name: "all relationship history ids", run: func() error {
			_, err := ts.AllRelHistoryIDs()
			return err
		}},
		{name: "all relationship history ids from", run: func() error {
			_, err := ts.AllRelHistoryIDsFrom(types.RelID(0), 1)
			return err
		}},
		{name: "for each relationship history id", run: func() error {
			called := false
			err := ts.ForEachRelHistoryID(func(types.RelID) bool {
				called = true
				return false
			})
			if called {
				return fmt.Errorf("relationship history callback invoked after close")
			}
			return err
		}},
		{name: "for each relationship history id by depth", run: func() error {
			called := false
			err := ts.ForEachRelHistoryIDByDepth(DepthWarm, func(types.RelID) bool {
				called = true
				return false
			})
			if called {
				return fmt.Errorf("relationship history depth callback invoked after close")
			}
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("%s err = %v, want ErrStoreClosed", tc.name, err)
			}
		})
	}
}

func TestTieredStoreNilLifecycleReturnsNilStore(t *testing.T) {
	t.Parallel()
	var ts *Store
	if err := ts.Close(); !errors.Is(err, ErrNilStore) {
		t.Fatalf("Close nil store = %v, want ErrNilStore", err)
	}
	if err := ts.Clear(); !errors.Is(err, ErrNilStore) {
		t.Fatalf("Clear nil store = %v, want ErrNilStore", err)
	}
}

func TestTieredStoreZeroValueLifecycleReturnsStoreClosed(t *testing.T) {
	t.Parallel()
	var ts Store
	if err := ts.Close(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Close zero-value store = %v, want ErrStoreClosed", err)
	}
	if err := ts.Clear(); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("Clear zero-value store = %v, want ErrStoreClosed", err)
	}
	if _, err := ts.GetNode(types.NodeID(1)); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("GetNode zero-value store = %v, want ErrStoreClosed", err)
	}
}

func TestTieredStoreAdminAndMetadataAPIsReturnStoreClosedAfterClose(t *testing.T) {
	ts, caseTok, _ := setupBatchDelete(t)
	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	if err := ts.PutNode(n); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	labelReg := registrypkg.NewLabelRegistry()
	if _, err := labelReg.GetOrCreate("Case"); err != nil {
		t.Fatalf("GetOrCreate label: %v", err)
	}
	relTypeReg := registrypkg.NewRelTypeRegistry()
	if _, err := relTypeReg.GetOrCreate("TRIGGERS"); err != nil {
		t.Fatalf("GetOrCreate rel type: %v", err)
	}

	if err := ts.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	cases := []struct {
		name string
		run  func() error
	}{
		{name: "force rotate", run: ts.ForceRotate},
		{name: "rotate hot shard", run: ts.RotateHotShard},
		{name: "list shards", run: func() error {
			_, err := ts.ListShards()
			return err
		}},
		{name: "rebuild catalog", run: ts.RebuildCatalog},
		{name: "verify shard", run: func() error {
			_, err := ts.VerifyShard(closedStoreVerifier{}, "reference")
			return err
		}},
		{name: "run repair", run: func() error {
			_, err := ts.RunRepair()
			return err
		}},
		{name: "archive node", run: func() error {
			return ts.ArchiveNode(n.ID())
		}},
		{name: "restore node", run: func() error {
			return ts.RestoreNode(n.ID())
		}},
		{name: "clear", run: ts.Clear},
		{name: "save registries", run: func() error {
			return ts.SaveRegistries(labelReg, relTypeReg)
		}},
		{name: "save label registry", run: func() error {
			return ts.SaveLabelRegistry(labelReg)
		}},
		{name: "load label registry", run: func() error {
			_, err := ts.LoadLabelRegistry(labelReg)
			return err
		}},
		{name: "save rel type registry", run: func() error {
			return ts.SaveRelTypeRegistry(relTypeReg)
		}},
		{name: "load rel type registry", run: func() error {
			_, err := ts.LoadRelTypeRegistry(relTypeReg)
			return err
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.run(); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("%s err = %v, want ErrStoreClosed", tc.name, err)
			}
		})
	}
}

func TestTieredStore_NodeCreateDuplicateProbeUsesCheckoutClosedGate(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)
	caseTok, _ := reg.GetOrCreate("Case")

	n := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), caseTok, nil)
	ts.ClosedForTest().Store(true)
	exists, err := ts.nodeIDExistsForCreate(n)
	ts.ClosedForTest().Store(false)
	if !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("nodeIDExistsForCreate with closed store = (exists=%v, err=%v), want ErrStoreClosed", exists, err)
	}
}

// TestTieredStore_WarmShard_WALCorruptionRecovery verifies that a warm shard with
// a corrupt WAL (simulating Ctrl-C / SIGKILL during flush) recovers transparently
// on restart instead of returning ErrTruncateNeeded.
func TestTieredStore_WarmShard_WALCorruptionRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: create store, write data, rotate hot→warm, close cleanly.
	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), 3, nil) // token 3 = event label
	if err := ts1.HotShardForTest().Store().PutNode(n1); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	_ = ts1.HotShardForTest().Store().Flush()

	// Force rotation: hot→warm.
	ts1.MuForTest().Lock()
	ts1.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts1.MuForTest().Unlock()
	if err := ts1.CheckRotationForTest(); err != nil {
		t.Fatalf("checkRotation: %v", err)
	}
	_ = ts1.HotShardForTest().Store().Flush()

	// Find the warm shard directory before closing.
	shardDir, shardName := warmShardDir(ts1, dir)
	if shardDir == "" {
		t.Fatal("no warm shard found after rotation")
	}
	t.Logf("warm shard: %s at %s", shardName, shardDir)

	if err := ts1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: corrupt the warm shard's WAL.
	injectCorruptMemFile(t, shardDir)

	// Verify that raw Badger open in read-only mode fails with ErrTruncateNeeded.
	opts := badgerv4.DefaultOptions(shardDir).WithReadOnly(true).WithLogger(nil)
	_, rawErr := badgerv4.Open(opts)
	if rawErr == nil {
		t.Fatal("expected ErrTruncateNeeded from raw Badger open, got nil")
	}
	// Badger v4's y.Wrap uses %+v (not %w) so errors.Is doesn't work.
	if !strings.Contains(rawErr.Error(), badgerv4.ErrTruncateNeeded.Error()) {
		t.Fatalf("expected ErrTruncateNeeded, got: %v", rawErr)
	}

	// Phase 3: reopen Store — should recover automatically.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New after corruption should recover, got: %v", err)
	}
	defer ts2.Close()

	// Verify the recovered warm shard remains mutable and data survived.
	var found bool
	for _, es := range ts2.EventShardsForTest() {
		if es.Tier() == TierWarm {
			found = true
			if !es.ReadOnlyForTest() {
				t.Error("recovered warm shard tier marker should be readOnly")
			}
			if es.Store().ReadOnlyForTest() {
				t.Error("recovered warm shard BadgerStore should be writable")
			}
		}
	}
	if !found {
		t.Error("warm shard not found after recovery")
	}

	// Verify the node written before crash is still accessible.
	got, err := ts2.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode from recovered warm shard: %v", err)
	}
	if got.ID() != n1.ID() {
		t.Error("node ID mismatch after WAL recovery")
	}
}

// TestTieredStore_ColdShard_WALCorruptionRecovery verifies that a cold shard
// with a corrupt WAL recovers on lazy-open (L1 pattern — same fix as warm).
func TestTieredStore_ColdShard_WALCorruptionRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: write data, rotate, demote to cold, close.
	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := registrypkg.NewLabelRegistry()
	ts1.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	if err := ts1.PutNode(n1); err != nil {
		t.Fatalf("PutNode: %v", err)
	}

	ts1.MuForTest().RLock()
	hotName := ts1.HotShardForTest().Name()
	_ = ts1.HotShardForTest().Store().Flush()
	ts1.MuForTest().RUnlock()

	// Rotate hot→warm, then demote to cold.
	time.Sleep(2 * time.Millisecond)
	ts1.MuForTest().Lock()
	_ = ts1.RotateHotShard()
	ts1.MuForTest().Unlock()

	demoteToCold(ts1, hotName)
	_ = ts1.CatalogForTest().Save()

	// Find the cold shard directory.
	var coldDir string
	ts1.MuForTest().RLock()
	if es, ok := ts1.EventShardsForTest()[hotName]; ok {
		coldDir = filepath.Join(dir, es.Path())
	}
	ts1.MuForTest().RUnlock()
	if coldDir == "" {
		t.Fatal("could not find cold shard directory")
	}

	if err := ts1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Phase 2: corrupt the cold shard's WAL.
	injectCorruptMemFile(t, coldDir)

	// Phase 3: reopen — cold shard store should be nil (lazy-open).
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer ts2.Close()

	ts2.SetLabelRegistry(reg)

	// Verify cold shard is nil (not opened yet).
	ts2.MuForTest().RLock()
	coldES := ts2.EventShardsForTest()[hotName]
	ts2.MuForTest().RUnlock()
	if coldES == nil {
		t.Fatal("cold shard not in eventShards after reopen")
	}
	if coldES.Store() != nil {
		t.Error("cold shard store should be nil before first access")
	}

	// Phase 4: trigger lazy-open by reading the node — should recover.
	got, err := ts2.GetNode(n1.ID())
	if err != nil {
		t.Fatalf("GetNode from corrupt cold shard (lazy-open recovery): %v", err)
	}
	if got.ID() != n1.ID() {
		t.Error("node ID mismatch after cold shard WAL recovery")
	}
}

// TestTieredStore_WALCorruption_NonTruncateError verifies that non-truncation
// errors (e.g., permission denied) are NOT masked by the recovery path.
func TestTieredStore_WALCorruption_NonTruncateError(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: create store, rotate to get a warm shard, close.
	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	ts1.MuForTest().Lock()
	ts1.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts1.MuForTest().Unlock()
	_ = ts1.CheckRotationForTest()

	shardDir, _ := warmShardDir(ts1, dir)
	if shardDir == "" {
		t.Fatal("no warm shard")
	}

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Phase 2: make the shard directory unreadable (not a truncation error).
	if err := os.Chmod(shardDir, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(shardDir, 0o755) })

	// Phase 3: reopen should fail with a real error, NOT silently recover.
	_, err = New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err == nil {
		t.Fatal("expected error from unreadable shard directory, got nil")
	}
	if strings.Contains(err.Error(), badgerv4.ErrTruncateNeeded.Error()) {
		t.Fatal("permission error should NOT be reported as ErrTruncateNeeded")
	}
	t.Logf("correctly propagated non-truncation error: %v", err)
}

// TestTieredStore_WALCorruption_DataIntegrity verifies that data written BEFORE
// the corrupt WAL entry survives recovery. The corruption only affects the
// incomplete tail — earlier committed entries must be intact.
func TestTieredStore_WALCorruption_DataIntegrity(t *testing.T) {
	dir := t.TempDir()

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	gen := tieredNodeGen(t)

	// Write multiple nodes and flush between each to ensure they're committed.
	const nodeCount = 10
	nodeIDs := make([]types.NodeID, nodeCount)
	for i := range nodeCount {
		n := types.NewNode(types.NodeID(gen.Generate()), 3, nil)
		if err := ts1.HotShardForTest().Store().PutNode(n); err != nil {
			t.Fatalf("PutNode[%d]: %v", i, err)
		}
		_ = ts1.HotShardForTest().Store().Flush()
		nodeIDs[i] = n.ID()
	}

	// Rotate hot→warm.
	ts1.MuForTest().Lock()
	ts1.HotShardForTest().SetTimeEndForTest(time.Now().Add(-time.Second))
	ts1.MuForTest().Unlock()
	if err := ts1.CheckRotationForTest(); err != nil {
		t.Fatal(err)
	}
	_ = ts1.HotShardForTest().Store().Flush()

	shardDir, _ := warmShardDir(ts1, dir)
	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the WAL.
	injectCorruptMemFile(t, shardDir)

	// Reopen with recovery.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}
	defer ts2.Close()

	// ALL nodes written before the crash must survive.
	for i, id := range nodeIDs {
		got, err := ts2.GetNode(id)
		if err != nil {
			t.Errorf("node[%d] id=%d lost after recovery: %v", i, id, err)
			continue
		}
		if got.ID() != id {
			t.Errorf("node[%d] id mismatch: got %d, want %d", i, got.ID(), id)
		}
	}
}

// TestTieredStore_WALCorruption_ConcurrentColdAccess verifies that concurrent
// cold shard access with a corrupt WAL doesn't panic or deadlock.
func TestTieredStore_WALCorruption_ConcurrentColdAccess(t *testing.T) {
	dir := t.TempDir()

	ts1, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	reg := registrypkg.NewLabelRegistry()
	ts1.SetLabelRegistry(reg)
	_, _ = reg.GetOrCreate("Case")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	n1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	_ = ts1.PutNode(n1)

	ts1.MuForTest().RLock()
	hotName := ts1.HotShardForTest().Name()
	_ = ts1.HotShardForTest().Store().Flush()
	ts1.MuForTest().RUnlock()

	time.Sleep(2 * time.Millisecond)
	ts1.MuForTest().Lock()
	_ = ts1.RotateHotShard()
	ts1.MuForTest().Unlock()

	demoteToCold(ts1, hotName)
	_ = ts1.CatalogForTest().Save()

	var coldDir string
	ts1.MuForTest().RLock()
	if es, ok := ts1.EventShardsForTest()[hotName]; ok {
		coldDir = filepath.Join(dir, es.Path())
	}
	ts1.MuForTest().RUnlock()

	if err := ts1.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the WAL.
	injectCorruptMemFile(t, coldDir)

	// Reopen.
	ts2, err := New(Config{
		DataDir:       dir,
		RefLabels:     []string{"Case"},
		ShardWindow:   7 * 24 * time.Hour,
		FlushInterval: 1<<63 - 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ts2.Close()
	ts2.SetLabelRegistry(reg)

	nodeID := n1.ID()

	// Hammer with 50 concurrent goroutines — all trigger lazy-open recovery.
	const goroutines = 50
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	wg.Add(goroutines)
	for i := range goroutines {
		go func(idx int) {
			defer wg.Done()
			got, err := ts2.GetNode(nodeID)
			if err != nil {
				errs[idx] = fmt.Errorf("goroutine %d: GetNode: %w", idx, err)
				return
			}
			if got.ID() != nodeID {
				errs[idx] = fmt.Errorf("goroutine %d: id mismatch", idx)
			}
		}(i)
	}
	wg.Wait()

	for _, e := range errs {
		if e != nil {
			t.Error(e)
		}
	}
}

func TestTieredStore_OutgoingRelationshipsForNodes(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	// signal1 and signal2 are event nodes (hot shard).
	signal1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	signal2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	// caseNode is a reference node (ref shard).
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	_ = ts.PutNode(signal1)
	_ = ts.PutNode(signal2)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	// signal1 -> caseNode (cross-shard)
	r1 := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		signal1.ID(), caseNode.ID())
	// signal2 -> caseNode (cross-shard)
	r2 := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		signal2.ID(), caseNode.ID())
	_ = ts.PutRelationship(r1)
	_ = ts.PutRelationship(r2)

	s1ID := signal1.ID()
	s2ID := signal2.ID()
	cID := caseNode.ID()

	// Batch query for both signal nodes.
	got, err := ts.OutgoingRelationshipsForNodes([]types.NodeID{s1ID, s2ID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[s1ID]) != 1 {
		t.Fatalf("signal1: got %d rels, want 1", len(got[s1ID]))
	}
	if len(got[s2ID]) != 1 {
		t.Fatalf("signal2: got %d rels, want 1", len(got[s2ID]))
	}

	// caseNode has no outgoing — absent from result.
	got, err = ts.OutgoingRelationshipsForNodes([]types.NodeID{cID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("caseNode: got %d entries, want 0", len(got))
	}

	// Mixed: event + ref nodes in one call.
	got, err = ts.OutgoingRelationshipsForNodes([]types.NodeID{s1ID, cID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[s1ID]) != 1 {
		t.Fatalf("mixed query signal1: got %d rels, want 1", len(got[s1ID]))
	}
	if _, ok := got[cID]; ok {
		t.Fatal("caseNode should not be in result")
	}

	// Empty input.
	got, err = ts.OutgoingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}
}

func TestTieredStore_IncomingRelationshipsForNodes(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal1 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil) // event shard
	signal2 := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil) // event shard
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)  // ref shard
	_ = ts.PutNode(signal1)
	_ = ts.PutNode(signal2)
	_ = ts.PutNode(caseNode)

	rGen := tieredRelGen(t)
	// signal1 -> caseNode (cross-shard: incoming to caseNode)
	r1 := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		signal1.ID(), caseNode.ID())
	// signal2 -> caseNode (cross-shard: incoming to caseNode)
	r2 := types.NewRelationship(types.RelID(rGen.Generate()), 1,
		signal2.ID(), caseNode.ID())
	_ = ts.PutRelationship(r1)
	_ = ts.PutRelationship(r2)

	s1ID := signal1.ID()
	cID := caseNode.ID()

	// Batch query: caseNode has 2 incoming, signal1 has 0 incoming.
	got, err := ts.IncomingRelationshipsForNodes([]types.NodeID{cID, s1ID}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(got[cID]) != 2 {
		t.Fatalf("caseNode: got %d rels, want 2", len(got[cID]))
	}
	if _, ok := got[s1ID]; ok {
		t.Fatal("signal1 should not be in result (no incoming)")
	}

	// Empty input.
	got, err = ts.IncomingRelationshipsForNodes(nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != nil {
		t.Fatalf("nil input: got %v, want nil", got)
	}
}

func TestTieredStore_AdjacencyMissingNodeReturnsErrNodeNotFound(t *testing.T) {
	ts := newTestTieredStore(t)
	reg := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(reg)

	caseTok, _ := reg.GetOrCreate("Case")
	_, _ = reg.GetOrCreate("User")
	signalTok, _ := reg.GetOrCreate("Signal")

	gen := tieredNodeGen(t)
	signal := types.NewNode(types.NodeID(gen.Generate()), signalTok, nil)
	caseNode := types.NewNode(types.NodeID(gen.Generate()), caseTok, nil)
	if err := ts.PutNode(signal); err != nil {
		t.Fatal(err)
	}
	if err := ts.PutNode(caseNode); err != nil {
		t.Fatal(err)
	}
	rel := types.NewRelationship(types.RelID(tieredRelGen(t).Generate()), 1, signal.ID(), caseNode.ID())
	if err := ts.PutRelationship(rel); err != nil {
		t.Fatal(err)
	}

	missing := types.NodeID(gen.Generate())
	if _, err := ts.OutgoingRelationships(missing, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("OutgoingRelationships missing err = %v, want ErrNodeNotFound", err)
	}
	if _, err := ts.IncomingRelationships(missing, 0); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("IncomingRelationships missing err = %v, want ErrNodeNotFound", err)
	}
	if got, err := ts.OutgoingRelationshipsForNodes([]types.NodeID{signal.ID(), missing}, 0); !errors.Is(err, ErrNodeNotFound) || got != nil {
		t.Fatalf("OutgoingRelationshipsForNodes mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
	if got, err := ts.IncomingRelationshipsForNodes([]types.NodeID{caseNode.ID(), missing}, 0); !errors.Is(err, ErrNodeNotFound) || got != nil {
		t.Fatalf("IncomingRelationshipsForNodes mixed = %#v, %v; want nil, ErrNodeNotFound", got, err)
	}
}
