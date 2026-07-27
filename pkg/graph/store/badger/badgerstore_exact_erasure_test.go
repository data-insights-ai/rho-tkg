package badger

import (
	"errors"
	"testing"

	badgerv4 "github.com/dgraph-io/badger/v4"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

var badgerExactErasureBounds = storecontract.ExactErasureBounds{
	MaxRelationshipIdentities: 32,
	MaxRelationshipVersions:   128,
	MaxEndpointNodeIdentities: 64,
}

func TestBadgerExactEraseIsAtomicIdempotentAndSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, SyncWrites: true, LabelIndexOnDisk: true, AdjacencyIndexOnDisk: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	const label, relType = uint16(7), uint16(9)
	n1, n2, n3 := types.NodeID(1), types.NodeID(2), types.NodeID(3)
	for _, tc := range []struct {
		id    types.NodeID
		value string
	}{{n1, "erased-secret"}, {n2, "survivor-a"}, {n3, "survivor-b"}} {
		n := types.NewNode(tc.id, label, nil)
		n.SetVersion(1)
		_ = n.SetProperty("email", tc.value)
		if err := bs.PutNode(n); err != nil {
			t.Fatalf("PutNode(%d): %v", tc.id, err)
		}
	}
	h := types.NewNode(n1, label, nil)
	_ = h.SetProperty("email", "older-secret")
	if err := bs.PutNodeVersion(n1, 0, h); err != nil {
		t.Fatalf("PutNodeVersion: %v", err)
	}
	r1 := types.NewRelationship(types.RelID(101), relType, n1, n2)
	r2 := types.NewRelationship(types.RelID(102), relType, n1, n3)
	_ = r1.SetProperty("note", "erased-rel-secret")
	for _, r := range []*types.Relationship{r1, r2} {
		r.SetVersion(1)
		if err := bs.PutRelationship(r); err != nil {
			t.Fatalf("PutRelationship: %v", err)
		}
		rh := types.NewRelationship(r.ID(), relType, r.StartNodeID(), r.EndNodeID())
		if err := bs.PutRelVersion(r.ID(), 0, rh); err != nil {
			t.Fatalf("PutRelVersion: %v", err)
		}
	}
	if err := bs.MetaSet("compact_stub_node/1", []byte("erased-stub")); err != nil {
		t.Fatal(err)
	}

	_, err = bs.ExactErase(storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{n1},
		RelIDs:  []types.RelID{r1.ID()},
		Bounds:  badgerExactErasureBounds,
	})
	if !errors.Is(err, storecontract.ErrExactErasureRelationshipEscape) {
		t.Fatalf("scope escape = %v, want ErrExactErasureRelationshipEscape", err)
	}
	if _, err := bs.GetNode(n1); err != nil {
		t.Fatalf("escape refusal mutated node: %v", err)
	}

	req := storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{n1},
		RelIDs:  []types.RelID{r1.ID(), r2.ID()},
		Bounds:  badgerExactErasureBounds,
		MetaWrites: []storecontract.MetaWrite{
			{Key: "compact_stub_node/1"},
		},
	}
	got, err := bs.ExactErase(req)
	if err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	if got.NodesRemoved != 1 || got.RelsRemoved != 2 {
		t.Fatalf("ExactErase result = %+v, want 1/2", got)
	}
	if again, err := bs.ExactErase(req); err != nil || again != (storecontract.ExactErasureResult{}) {
		t.Fatalf("idempotent ExactErase = (%+v, %v)", again, err)
	}
	standalone := types.NewRelationship(types.RelID(103), relType, n2, n3)
	if err := bs.PutRelationship(standalone); err != nil {
		t.Fatal(err)
	}
	if err := bs.PutRelVersion(standalone.ID(), 0, standalone); err != nil {
		t.Fatal(err)
	}
	if got, err := bs.ExactErase(storecontract.ExactErasureRequest{RelIDs: []types.RelID{standalone.ID()}}); err != nil || got.RelsRemoved != 1 {
		t.Fatalf("relationship-only ExactErase = (%+v, %v)", got, err)
	}
	if err := bs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs.Close()
	if _, err := bs.GetNode(n1); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("erased node after restart = %v", err)
	}
	if hist, _ := bs.GetNodeHistory(n1); len(hist) != 0 {
		t.Fatalf("erased node history after restart has %d rows", len(hist))
	}
	for _, rid := range []types.RelID{r1.ID(), r2.ID()} {
		if _, err := bs.GetRelationship(rid); !errors.Is(err, ErrRelNotFound) {
			t.Fatalf("erased rel %d after restart = %v", rid, err)
		}
		if hist, _ := bs.GetRelHistory(rid); len(hist) != 0 {
			t.Fatalf("erased rel %d history after restart has %d rows", rid, len(hist))
		}
	}
	if _, err := bs.GetRelationship(standalone.ID()); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("relationship-only erase after restart = %v", err)
	}
	if hist, _ := bs.GetRelHistory(standalone.ID()); len(hist) != 0 {
		t.Fatalf("relationship-only history after restart has %d rows", len(hist))
	}
	for _, survivor := range []types.NodeID{n2, n3} {
		if _, err := bs.GetNode(survivor); err != nil {
			t.Fatalf("survivor %d after restart: %v", survivor, err)
		}
	}
	if in, err := bs.IncomingRelationships(n2, 0); err != nil || len(in) != 0 {
		t.Fatalf("survivor adjacency after restart = (%d, %v)", len(in), err)
	}
	if v, err := bs.MetaGet("compact_stub_node/1"); err != nil || len(v) != 0 {
		t.Fatalf("erasure metadata after restart = (%q, %v)", v, err)
	}
	stats, err := bs.NodePropertyStats(label, "email")
	if err != nil {
		t.Fatalf("NodePropertyStats: %v", err)
	}
	if stats.Count != 2 || stats.Min == "erased-secret" || stats.Max == "erased-secret" {
		t.Fatalf("planner stats retain erased value after restart: %+v", stats)
	}
	if relStats, err := bs.RelPropertyStats(relType, "note"); err != nil || relStats.Count != 0 || relStats.Min != nil || relStats.Max != nil {
		t.Fatalf("relationship planner stats retain erased value after restart = (%+v, %v)", relStats, err)
	}
}

func TestBadgerExactEraseRefusesRetainedLogAfterDisabledReopen(t *testing.T) {
	dir := t.TempDir()
	bs, err := New(Config{Dir: dir, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := bs.PutNode(types.NewNode(types.NodeID(1), 1, nil)); err != nil {
		t.Fatal(err)
	}
	if err := bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs, err = New(Config{Dir: dir, SyncWrites: true})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer bs.Close()
	_, err = bs.ExactErase(storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds:  badgerExactErasureBounds,
	})
	if !errors.Is(err, storecontract.ErrExactErasureChangeLogRetained) {
		t.Fatalf("ExactErase with retained disabled log = %v, want ErrExactErasureChangeLogRetained", err)
	}
	if _, err := bs.GetNode(types.NodeID(1)); err != nil {
		t.Fatalf("refused erasure mutated node: %v", err)
	}
}

func TestBadgerExactErasePreflightScanFailureLeavesNoMutationOrQueuedOps(t *testing.T) {
	stages := []string{
		"node-history",
		"relationship-history",
		"relationship-current",
		"relationship-index",
		"node-current",
		"node-label-index",
		"node-property-index",
		"node-temporal-index",
	}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			bs, err := New(Config{
				InMemory:             true,
				SyncWrites:           true,
				LabelIndexOnDisk:     true,
				AdjacencyIndexOnDisk: true,
				PropertyIndexOnDisk:  true,
				TemporalIndexOnDisk:  true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer bs.Close()

			node := types.NewNode(types.NodeID(1), 7, nil)
			_ = node.SetProperty("secret", "must-remain-on-refusal")
			if err = bs.PutNode(node); err != nil {
				t.Fatal(err)
			}
			survivor := types.NewNode(types.NodeID(2), 7, nil)
			if err = bs.PutNode(survivor); err != nil {
				t.Fatal(err)
			}
			rel := types.NewRelationship(types.RelID(101), 9, node.ID(), survivor.ID())
			if err = bs.PutRelationship(rel); err != nil {
				t.Fatal(err)
			}
			if err = bs.PutNodeVersion(node.ID(), 0, node.DeepCopy()); err != nil {
				t.Fatal(err)
			}
			if err = bs.PutRelVersion(rel.ID(), 0, rel.DeepCopy()); err != nil {
				t.Fatal(err)
			}
			if got := bs.pendingLen(); got != 0 {
				t.Fatalf("test setup left %d queued ops, want 0", got)
			}

			injected := errors.New("injected exact-erasure scan failure")
			hit := false
			bs.exactErasureScanTestHook = func(gotStage string, _ uint64) error {
				if gotStage != stage {
					return nil
				}
				hit = true
				return injected
			}
			_, err = bs.ExactErase(storecontract.ExactErasureRequest{
				NodeIDs: []types.NodeID{node.ID()},
				RelIDs:  []types.RelID{rel.ID()},
				Bounds:  badgerExactErasureBounds,
			})
			bs.exactErasureScanTestHook = nil
			if !hit {
				t.Fatalf("preflight stage %q was not reached", stage)
			}
			if !errors.Is(err, injected) {
				t.Fatalf("ExactErase error = %v, want injected error", err)
			}
			if got := bs.pendingLen(); got != 0 {
				t.Fatalf("scan refusal queued %d write ops, want 0", got)
			}
			if got, getErr := bs.GetNode(node.ID()); getErr != nil {
				t.Fatalf("scan refusal removed node: %v", getErr)
			} else if secret, _ := got.GetProperty("secret"); secret != "must-remain-on-refusal" {
				t.Fatalf("scan refusal changed node property to %v", secret)
			}
			if _, getErr := bs.GetRelationship(rel.ID()); getErr != nil {
				t.Fatalf("scan refusal removed relationship: %v", getErr)
			}
			if outgoing, outgoingErr := bs.OutgoingRelationships(node.ID(), 0); outgoingErr != nil || len(outgoing) != 1 {
				t.Fatalf("scan refusal changed adjacency = (%d, %v)", len(outgoing), outgoingErr)
			}
			if history, historyErr := bs.GetNodeHistory(node.ID()); historyErr != nil || len(history) != 1 {
				t.Fatalf("scan refusal changed node history = (%d, %v)", len(history), historyErr)
			}
			if history, historyErr := bs.GetRelHistory(rel.ID()); historyErr != nil || len(history) != 1 {
				t.Fatalf("scan refusal changed relationship history = (%d, %v)", len(history), historyErr)
			}
		})
	}
}

func TestBadgerExactErasePurgesPhysicalIndexResidueWithoutActiveDefinition(t *testing.T) {
	bs, err := New(Config{InMemory: true, SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()

	node := types.NewNode(types.NodeID(1), 7, nil)
	if err = bs.PutNode(node); err != nil {
		t.Fatal(err)
	}
	payload, ok := storepkg.PropertyIndexValueBytes("s:orphan")
	if !ok {
		t.Fatal("failed to encode test property-index payload")
	}
	orphanKeys := [][]byte{
		storepkg.LabelIndexKey(65_000, node.ID().SnowflakeID()),
		storepkg.PropertyIndexEntryKey(65_000, payload, node.ID().SnowflakeID()),
		storepkg.TemporalIndexEntryKey(65_000, types.Instant(1), node.ID().SnowflakeID()),
	}
	if err = bs.db.Update(func(txn *badgerv4.Txn) error {
		for _, key := range orphanKeys {
			if setErr := txn.Set(key, []byte{1}); setErr != nil {
				return setErr
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if _, err = bs.ExactErase(storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{node.ID()},
		Bounds:  badgerExactErasureBounds,
	}); err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	if err = bs.db.View(func(txn *badgerv4.Txn) error {
		for _, key := range orphanKeys {
			if _, getErr := txn.Get(key); !errors.Is(getErr, badgerv4.ErrKeyNotFound) {
				t.Fatalf("physical index residue survived for key %x: %v", key, getErr)
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestBadgerExactErasureClosureErasesDeletedHistoricalRelationshipAfterRestart(t *testing.T) {
	dir := t.TempDir()
	cfg := Config{Dir: dir, SyncWrites: true}
	bs, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []types.NodeID{1, 2, 3} {
		if err = bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	const (
		historicalID = types.RelID(101)
		deletedID    = types.RelID(102)
	)
	if err = bs.PutRelVersion(
		historicalID, 0,
		types.NewRelationship(historicalID, 1, 1, 2),
	); err != nil {
		t.Fatal(err)
	}
	if err = bs.PutRelationship(types.NewRelationship(historicalID, 1, 2, 3)); err != nil {
		t.Fatal(err)
	}
	if err = bs.PutRelVersion(
		deletedID, 0,
		types.NewRelationship(deletedID, 1, 1, 3),
	); err != nil {
		t.Fatal(err)
	}
	if err = bs.Close(); err != nil {
		t.Fatal(err)
	}
	bs, err = New(cfg)
	if err != nil {
		t.Fatalf("reopen before closure resolution: %v", err)
	}

	closure, err := bs.ExactErasureRelationshipClosure(storecontract.ExactErasureClosureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds:  badgerExactErasureBounds,
	})
	if err != nil {
		t.Fatalf("ExactErasureRelationshipClosure: %v", err)
	}
	if len(closure.RelationshipIDs) != 2 ||
		closure.RelationshipIDs[0] != historicalID ||
		closure.RelationshipIDs[1] != deletedID {
		t.Fatalf("closure = %v, want [%d %d]", closure, historicalID, deletedID)
	}
	if got := closure.EndpointNodeIDs; len(got) != 3 ||
		got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("endpoint closure = %v, want [1 2 3]", got)
	}
	if _, err = bs.ExactErase(storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds:  badgerExactErasureBounds,
	}); !errors.Is(err, storecontract.ErrExactErasureRelationshipEscape) {
		t.Fatalf("omitted historical closure = %v, want ErrExactErasureRelationshipEscape", err)
	}
	if history, historyErr := bs.GetRelHistory(deletedID); historyErr != nil || len(history) != 1 {
		t.Fatalf("failed preflight mutated deleted history = (%d, %v)", len(history), historyErr)
	}
	if _, err = bs.ExactErase(storecontract.ExactErasureRequest{
		NodeIDs: []types.NodeID{1},
		RelIDs:  closure.RelationshipIDs,
		Bounds:  badgerExactErasureBounds,
	}); err != nil {
		t.Fatalf("ExactErase: %v", err)
	}
	if err = bs.Close(); err != nil {
		t.Fatal(err)
	}

	bs, err = New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	if _, err = bs.GetRelationship(historicalID); !errors.Is(err, ErrRelNotFound) {
		t.Fatalf("current relationship survived restart: %v", err)
	}
	for _, rid := range []types.RelID{historicalID, deletedID} {
		if history, historyErr := bs.GetRelHistory(rid); historyErr != nil || len(history) != 0 {
			t.Fatalf("historical relationship %d survived restart = (%d, %v)", rid, len(history), historyErr)
		}
	}
	if _, err = bs.GetNode(1); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("erased node survived restart: %v", err)
	}
	for _, id := range []types.NodeID{2, 3} {
		if _, err = bs.GetNode(id); err != nil {
			t.Fatalf("survivor %d after restart: %v", id, err)
		}
	}
}

func TestBadgerExactErasureClosureFailsClosedAtBounds(t *testing.T) {
	bs, err := New(Config{InMemory: true, SyncWrites: true})
	if err != nil {
		t.Fatal(err)
	}
	defer bs.Close()
	for _, id := range []types.NodeID{1, 2, 3} {
		if err = bs.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	for rid, end := range map[types.RelID]types.NodeID{101: 2, 102: 3} {
		if err = bs.PutRelationship(types.NewRelationship(rid, 1, 1, end)); err != nil {
			t.Fatal(err)
		}
	}
	_, err = bs.ExactErasureRelationshipClosure(storecontract.ExactErasureClosureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds: storecontract.ExactErasureBounds{
			MaxRelationshipIdentities: 1,
			MaxRelationshipVersions:   8,
			MaxEndpointNodeIdentities: 8,
		},
	})
	if !errors.Is(err, storecontract.ErrExactErasureClosureLimit) {
		t.Fatalf("identity bound = %v, want ErrExactErasureClosureLimit", err)
	}
	_, err = bs.ExactErasureRelationshipClosure(storecontract.ExactErasureClosureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds: storecontract.ExactErasureBounds{
			MaxRelationshipIdentities: 8,
			MaxRelationshipVersions:   1,
			MaxEndpointNodeIdentities: 8,
		},
	})
	if !errors.Is(err, storecontract.ErrExactErasureClosureLimit) {
		t.Fatalf("version bound = %v, want ErrExactErasureClosureLimit", err)
	}
	_, err = bs.ExactErasureRelationshipClosure(storecontract.ExactErasureClosureRequest{
		NodeIDs: []types.NodeID{1},
		Bounds: storecontract.ExactErasureBounds{
			MaxRelationshipIdentities: 8,
			MaxRelationshipVersions:   8,
			MaxEndpointNodeIdentities: 2,
		},
	})
	if !errors.Is(err, storecontract.ErrExactErasureClosureLimit) {
		t.Fatalf("endpoint bound = %v, want ErrExactErasureClosureLimit", err)
	}
}

func TestBadgerExactErasureClosureValidationAndLifecycle(t *testing.T) {
	bs, err := New(Config{InMemory: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = bs.ExactErasureRelationshipClosure(
		storecontract.ExactErasureClosureRequest{},
	); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("empty request = %v, want ErrInvalidStoreMutation", err)
	}
	if _, err = bs.ExactErasureRelationshipClosure(
		storecontract.ExactErasureClosureRequest{
			NodeIDs: []types.NodeID{1, 1},
			Bounds:  badgerExactErasureBounds,
		},
	); !errors.Is(err, storecontract.ErrInvalidStoreMutation) {
		t.Fatalf("duplicate nodes = %v, want ErrInvalidStoreMutation", err)
	}
	if err = bs.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = bs.ExactErasureRelationshipClosure(
		storecontract.ExactErasureClosureRequest{
			NodeIDs: []types.NodeID{1},
			Bounds:  badgerExactErasureBounds,
		},
	); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed store = %v, want ErrStoreClosed", err)
	}
}
