package core

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

func TestR9_ImportReplayErrorRollsBackNewCurrentHistoryAndRegistries(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels:   []string{"", "Person"},
		RelTypes: []string{"", "KNOWS"},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNode, mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           100,
		PrimaryLabel: 1,
		Version:      1,
	}, []string{"Person"}))
	writeImportMsgpackRecord(t, &stream, exportTagNodeHist, mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           100,
		PrimaryLabel: 1,
		Version:      0,
	}, []string{"Person"}))
	writeImportMsgpackRecord(t, &stream, exportTagRel, mustHashedRelWire(t, storeutil.RelWire{
		ID:      300,
		RelType: 1,
		StartID: 100,
		EndID:   200, // missing endpoint: PutRelationship fails after node/history replay
	}, "KNOWS"))

	err = g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Import: got %v, want ErrNodeNotFound", err)
	}
	if _, err := g.store.GetNode(types.NodeID(100)); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode(100): got %v, want ErrNodeNotFound", err)
	}
	history, err := g.store.GetNodeHistory(types.NodeID(100))
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("node history length after rollback = %d, want 0", len(history))
	}
	if _, ok := g.labels.Lookup("Person"); ok {
		t.Fatal("label registry kept imported Person after failed import")
	}
	if _, ok := g.relTypes.Lookup("KNOWS"); ok {
		t.Fatal("reltype registry kept imported KNOWS after failed import")
	}
}

func TestR9_ImportReplayErrorRestoresExistingHistoryAndRegistries(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, err := g.Nodes.Import(context.Background(), types.NodeID(100), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	existingNode, err := g.store.GetNode(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	nodeWire := storeutil.NodeToWire(existingNode)

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels:   []string{"", "Person"},
		RelTypes: []string{"", "KNOWS"},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNode, nodeWire)
	writeImportMsgpackRecord(t, &stream, exportTagNodeHist, nodeWire)
	writeImportMsgpackRecord(t, &stream, exportTagRel, mustHashedRelWire(t, storeutil.RelWire{
		ID:      300,
		RelType: 1,
		StartID: int64(n.ID().SnowflakeID()),
		EndID:   200, // missing endpoint after the history replay
	}, "KNOWS"))

	err = g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Import: got %v, want ErrNodeNotFound", err)
	}
	if _, err := g.store.GetNode(n.ID()); err != nil {
		t.Fatalf("existing node missing after rollback: %v", err)
	}
	history, err := g.store.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("existing node history length after rollback = %d, want 0", len(history))
	}
	if _, ok := g.labels.Lookup("Person"); !ok {
		t.Fatal("label registry lost pre-existing Person after failed import")
	}
	if _, ok := g.relTypes.Lookup("KNOWS"); ok {
		t.Fatal("reltype registry kept imported KNOWS after failed import")
	}
}

func TestR9_ImportRollbackSnapshotsOnlyTouchedHistorySuffixWithNativeTrim(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.historyTrim == nil {
		t.Fatal("memory store did not enable native history trim")
	}

	n, err := g.Nodes.Import(context.Background(), types.NodeID(100), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	other, err := g.Nodes.Import(context.Background(), types.NodeID(200), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed other node: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "KNOWS", n, other, nil); err != nil {
		t.Fatalf("seed rel type: %v", err)
	}

	for _, version := range []uint32{0, 1, 3} {
		h := n.DeepCopy()
		h.SetVersion(version)
		if err := g.store.PutNodeVersion(n.ID(), version, h); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", version, err)
		}
		r := types.NewRelationship(types.RelID(300), 1, n.ID(), other.ID())
		r.SetVersion(version)
		if err := g.store.PutRelVersion(r.ID(), version, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", version, err)
		}
	}

	rb := newImportRollback(g)
	if err := rb.captureNodeHistory(n.ID(), 2); err != nil {
		t.Fatalf("captureNodeHistory: %v", err)
	}
	nodeSnap := rb.nodes[n.ID()]
	if !nodeSnap.useHistoryTrim {
		t.Fatal("node history snapshot did not use trim")
	}
	if nodeSnap.historyTrimFrom != 2 {
		t.Fatalf("node history trim starts at %d, want 2", nodeSnap.historyTrimFrom)
	}
	if got := importRollbackNodeHistoryVersions(nodeSnap.history); len(got) != 1 || got[0] != 3 {
		t.Fatalf("node history snapshot versions = %v, want [3]", got)
	}

	if err := rb.captureRelHistory(types.RelID(300), 2); err != nil {
		t.Fatalf("captureRelHistory: %v", err)
	}
	relSnap := rb.rels[types.RelID(300)]
	if !relSnap.useHistoryTrim {
		t.Fatal("relationship history snapshot did not use trim")
	}
	if relSnap.historyTrimFrom != 2 {
		t.Fatalf("relationship history trim starts at %d, want 2", relSnap.historyTrimFrom)
	}
	if got := importRollbackRelHistoryVersions(relSnap.history); len(got) != 1 || got[0] != 3 {
		t.Fatalf("relationship history snapshot versions = %v, want [3]", got)
	}
}

func TestR9_ImportReplayErrorRestoresHistorySuffixAfterTrimRollback(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()

	n, err := g.Nodes.Import(context.Background(), types.NodeID(100), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	other, err := g.Nodes.Import(context.Background(), types.NodeID(200), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed other node: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "KNOWS", n, other, nil); err != nil {
		t.Fatalf("seed rel type: %v", err)
	}

	for _, version := range []uint32{0, 3} {
		h := n.DeepCopy()
		h.SetVersion(version)
		if err := g.store.PutNodeVersion(n.ID(), version, h); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", version, err)
		}
		r := types.NewRelationship(types.RelID(300), 1, n.ID(), other.ID())
		r.SetVersion(version)
		if err := g.store.PutRelVersion(r.ID(), version, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", version, err)
		}
	}

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion, RelCount: 1})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels:   []string{"", "Person"},
		RelTypes: []string{"", "KNOWS"},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNodeHist, mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           int64(n.ID().SnowflakeID()),
		PrimaryLabel: 1,
		Version:      2,
	}, []string{"Person"}))
	writeImportMsgpackRecord(t, &stream, exportTagRelHist, mustHashedRelWire(t, storeutil.RelWire{
		ID:      300,
		RelType: 1,
		StartID: int64(n.ID().SnowflakeID()),
		EndID:   int64(other.ID().SnowflakeID()),
		Version: 2,
	}, "KNOWS"))
	writeImportMsgpackRecord(t, &stream, exportTagRel, mustHashedRelWire(t, storeutil.RelWire{
		ID:      400,
		RelType: 1,
		StartID: int64(n.ID().SnowflakeID()),
		EndID:   999, // missing endpoint: fail after history replay
	}, "KNOWS"))

	err = g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Import: got %v, want ErrNodeNotFound", err)
	}
	nodeHistory, err := g.store.GetNodeHistory(n.ID())
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if got := importRollbackNodeHistoryVersions(nodeHistory); len(got) != 2 || got[0] != 0 || got[1] != 3 {
		t.Fatalf("node history versions after rollback = %v, want [0 3]", got)
	}
	relHistory, err := g.store.GetRelHistory(types.RelID(300))
	if err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	}
	if got := importRollbackRelHistoryVersions(relHistory); len(got) != 2 || got[0] != 0 || got[1] != 3 {
		t.Fatalf("relationship history versions after rollback = %v, want [0 3]", got)
	}
}

func TestR9_ImportRollbackHistoryFallbacks(t *testing.T) {
	t.Parallel()

	wrapped := &concreteRollbackHistoryFaultStore{Store: memory.New(), err: errors.New("history trim fault")}
	g, err := New(Config{Store: wrapped})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	if g.historyTrim != nil {
		t.Fatal("wrapped memory store unexpectedly enabled native history trim")
	}

	n, err := g.Nodes.Import(context.Background(), types.NodeID(100), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed node: %v", err)
	}
	other, err := g.Nodes.Import(context.Background(), types.NodeID(200), []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("seed other node: %v", err)
	}
	if _, err := g.Rels.Add(context.Background(), "KNOWS", n, other, nil); err != nil {
		t.Fatalf("seed rel type: %v", err)
	}
	for _, version := range []uint32{0, 1, 3} {
		h := n.DeepCopy()
		h.SetVersion(version)
		if err := g.store.PutNodeVersion(n.ID(), version, h); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", version, err)
		}
		r := types.NewRelationship(types.RelID(300), 1, n.ID(), other.ID())
		r.SetVersion(version)
		if err := g.store.PutRelVersion(r.ID(), version, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", version, err)
		}
	}

	rb := newImportRollback(g)
	if err := rb.captureNodeHistory(n.ID(), 2); err != nil {
		t.Fatalf("captureNodeHistory: %v", err)
	}
	nodeSnap := rb.nodes[n.ID()]
	if nodeSnap.useHistoryTrim {
		t.Fatal("wrapped store node history used trim")
	}
	if got := importRollbackNodeHistoryVersions(nodeSnap.history); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 3 {
		t.Fatalf("fallback node history snapshot versions = %v, want [0 1 3]", got)
	}
	importedNodeHist := n.DeepCopy()
	importedNodeHist.SetVersion(2)
	if err := g.store.PutNodeVersion(n.ID(), 2, importedNodeHist); err != nil {
		t.Fatalf("PutNodeVersion imported: %v", err)
	}
	if err := restoreImportNodeHistory(g, n.ID(), nodeSnap); err != nil {
		t.Fatalf("restoreImportNodeHistory fallback: %v", err)
	}
	if history, err := g.store.GetNodeHistory(n.ID()); err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	} else if got := importRollbackNodeHistoryVersions(history); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 3 {
		t.Fatalf("fallback restored node history versions = %v, want [0 1 3]", got)
	}

	if err := rb.captureRelHistory(types.RelID(300), 2); err != nil {
		t.Fatalf("captureRelHistory: %v", err)
	}
	relSnap := rb.rels[types.RelID(300)]
	if relSnap.useHistoryTrim {
		t.Fatal("wrapped store relationship history used trim")
	}
	if got := importRollbackRelHistoryVersions(relSnap.history); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 3 {
		t.Fatalf("fallback relationship history snapshot versions = %v, want [0 1 3]", got)
	}
	importedRelHist := types.NewRelationship(types.RelID(300), 1, n.ID(), other.ID())
	importedRelHist.SetVersion(2)
	if err := g.store.PutRelVersion(importedRelHist.ID(), 2, importedRelHist); err != nil {
		t.Fatalf("PutRelVersion imported: %v", err)
	}
	if err := restoreImportRelHistory(g, types.RelID(300), relSnap); err != nil {
		t.Fatalf("restoreImportRelHistory fallback: %v", err)
	}
	if history, err := g.store.GetRelHistory(types.RelID(300)); err != nil {
		t.Fatalf("GetRelHistory: %v", err)
	} else if got := importRollbackRelHistoryVersions(history); len(got) != 3 || got[0] != 0 || got[1] != 1 || got[2] != 3 {
		t.Fatalf("fallback restored relationship history versions = %v, want [0 1 3]", got)
	}

	if err := restoreImportNodeHistory(g, n.ID(), importNodeSnapshot{}); err != nil {
		t.Fatalf("restoreImportNodeHistory untouched: %v", err)
	}
	if err := restoreImportRelHistory(g, types.RelID(300), importRelSnapshot{}); err != nil {
		t.Fatalf("restoreImportRelHistory untouched: %v", err)
	}

	wrapped.failNodeTrim.Store(true)
	if err := restoreImportNodeHistory(g, n.ID(), importNodeSnapshot{historyTouched: true}); err == nil {
		t.Fatal("restoreImportNodeHistory fallback trim error = nil, want error")
	}
	wrapped.failNodeTrim.Store(false)
	if err := restoreImportNodeHistory(g, types.NodeID(999), importNodeSnapshot{
		historyTouched: true,
		history:        []*types.Node{nil},
	}); err == nil {
		t.Fatal("restoreImportNodeHistory invalid snapshot = nil, want error")
	}

	wrapped.failRelTrim.Store(true)
	if err := restoreImportRelHistory(g, types.RelID(300), importRelSnapshot{historyTouched: true}); err == nil {
		t.Fatal("restoreImportRelHistory fallback trim error = nil, want error")
	}
	wrapped.failRelTrim.Store(false)
	if err := restoreImportRelHistory(g, types.RelID(999), importRelSnapshot{
		historyTouched: true,
		history:        []*types.Relationship{nil},
	}); err == nil {
		t.Fatal("restoreImportRelHistory invalid snapshot = nil, want error")
	}
}

func TestR9_ImportRollbackCopyHistoryFromFiltersWithoutPager(t *testing.T) {
	t.Parallel()

	ms := memory.New()
	c := &Core{store: &mandatoryHistoryFaultStore{MandatoryStore: ms}}
	nid := types.NodeID(100)
	for _, version := range []uint32{0, 1, 3} {
		n := types.NewNode(nid, 1, nil)
		n.SetVersion(version)
		if err := ms.PutNodeVersion(nid, version, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", version, err)
		}
	}
	nodeSuffix, err := c.copyNodeHistoryFrom(nid, 2)
	if err != nil {
		t.Fatalf("copyNodeHistoryFrom: %v", err)
	}
	if got := importRollbackNodeHistoryVersions(nodeSuffix); len(got) != 1 || got[0] != 3 {
		t.Fatalf("node suffix versions = %v, want [3]", got)
	}
	nodeErr := errors.New("node history fault")
	nodeFaultCore := &Core{store: &mandatoryHistoryFaultStore{
		MandatoryStore: ms,
		failID:         nid,
		err:            nodeErr,
	}}
	if _, err := nodeFaultCore.copyNodeHistoryFrom(nid, 2); !errors.Is(err, nodeErr) {
		t.Fatalf("copyNodeHistoryFrom fault = %v, want %v", err, nodeErr)
	}

	rid := types.RelID(300)
	for _, version := range []uint32{0, 1, 3} {
		r := types.NewRelationship(rid, 1, types.NodeID(10), types.NodeID(20))
		r.SetVersion(version)
		if err := ms.PutRelVersion(rid, version, r); err != nil {
			t.Fatalf("PutRelVersion(%d): %v", version, err)
		}
	}
	relSuffix, err := c.copyRelHistoryFrom(rid, 2)
	if err != nil {
		t.Fatalf("copyRelHistoryFrom: %v", err)
	}
	if got := importRollbackRelHistoryVersions(relSuffix); len(got) != 1 || got[0] != 3 {
		t.Fatalf("relationship suffix versions = %v, want [3]", got)
	}
	relErr := errors.New("relationship history fault")
	relFaultCore := &Core{store: &mandatoryRelHistoryFaultStore{
		MandatoryStore: ms,
		failID:         rid,
		err:            relErr,
	}}
	if _, err := relFaultCore.copyRelHistoryFrom(rid, 2); !errors.Is(err, relErr) {
		t.Fatalf("copyRelHistoryFrom fault = %v, want %v", err, relErr)
	}
}

type mandatoryRelHistoryFaultStore struct {
	storepkg.MandatoryStore
	failID types.RelID
	err    error
}

func (s *mandatoryRelHistoryFaultStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	if id == s.failID {
		return nil, s.err
	}
	return s.MandatoryStore.GetRelHistory(id)
}

func TestR9_ImportReplayRollbackUsesImportRegistryBeforeTieredCleanup(t *testing.T) {
	t.Parallel()

	g, _ := newTestTieredGraph(t)
	defer g.Close()

	nodeID := g.Nodes.NextID()
	missingID := g.Nodes.NextID()
	relID := g.Rels.NextID()

	var stream bytes.Buffer
	writeImportMsgpackRecord(t, &stream, exportTagHeader, exportHeader{Version: exportFormatVersion})
	writeImportMsgpackRecord(t, &stream, exportTagRegistry, tiered.RegistryFileData{
		Labels:   []string{"", "Signal"},
		RelTypes: []string{"", "KNOWS"},
	})
	writeImportMsgpackRecord(t, &stream, exportTagNode, mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           int64(nodeID.SnowflakeID()),
		PrimaryLabel: 1,
		Version:      1,
	}, []string{"Signal"}))
	writeImportMsgpackRecord(t, &stream, exportTagNodeHist, mustHashedNodeWire(t, storeutil.NodeWire{
		ID:           int64(nodeID.SnowflakeID()),
		PrimaryLabel: 1,
		Version:      0,
	}, []string{"Signal"}))
	writeImportMsgpackRecord(t, &stream, exportTagRel, mustHashedRelWire(t, storeutil.RelWire{
		ID:      int64(relID.SnowflakeID()),
		RelType: 1,
		StartID: int64(nodeID.SnowflakeID()),
		EndID:   int64(missingID.SnowflakeID()),
	}, "KNOWS"))

	err := g.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("Import: got %v, want ErrNodeNotFound", err)
	}
	if _, err := g.store.GetNode(nodeID); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode(%d): got %v, want ErrNodeNotFound", nodeID, err)
	}
	history, err := g.store.GetNodeHistory(nodeID)
	if err != nil {
		t.Fatalf("GetNodeHistory: %v", err)
	}
	if len(history) != 0 {
		t.Fatalf("tiered node history length after rollback = %d, want 0", len(history))
	}
	if _, ok := g.labels.Lookup("Signal"); ok {
		t.Fatal("label registry kept imported Signal after failed import")
	}
	if _, ok := g.relTypes.Lookup("KNOWS"); ok {
		t.Fatal("reltype registry kept imported KNOWS after failed import")
	}
}

func importRollbackNodeHistoryVersions(history []*types.Node) []uint32 {
	versions := make([]uint32, len(history))
	for i, n := range history {
		versions[i] = n.Version()
	}
	return versions
}

func importRollbackRelHistoryVersions(history []*types.Relationship) []uint32 {
	versions := make([]uint32, len(history))
	for i, r := range history {
		versions[i] = r.Version()
	}
	return versions
}

func writeImportMsgpackRecord(t *testing.T, buf *bytes.Buffer, tag byte, v any) {
	t.Helper()
	body, err := msgpack.Marshal(v)
	if err != nil {
		t.Fatalf("marshal record %x: %v", tag, err)
	}
	if err := writeExportRecord(buf, tag, body); err != nil {
		t.Fatalf("write record %x: %v", tag, err)
	}
}

// mustHashedNodeWire returns w with Hash set to the correct content hash for
// its decoded content. BACKLOG 12d: verifyImportedNodeHash now REJECTS a row
// with no integrity hash (previously silently exempted), so a hand-built
// synthetic NodeWire fixture must carry a real hash to reach whatever OTHER
// behavior the test actually means to exercise (e.g. a later record's
// failure triggering rollback) instead of failing immediately on this one.
func mustHashedNodeWire(t *testing.T, w storeutil.NodeWire, labelNames []string) storeutil.NodeWire {
	t.Helper()
	n, err := storeutil.WireToNodeChecked(w)
	if err != nil {
		t.Fatalf("mustHashedNodeWire: decode: %v", err)
	}
	hash, err := integrity.ComputeNodeHashChecked(n, labelNames)
	if err != nil {
		t.Fatalf("mustHashedNodeWire: compute hash: %v", err)
	}
	w.Hash = hash
	return w
}

// mustHashedRelWire is the relationship counterpart of mustHashedNodeWire.
// Hash computation is pure content (labels/type/properties/temporal); it
// does not require the endpoints to actually exist, so this is safe to use
// even for a wire whose EndID is deliberately a nonexistent node (the usual
// reason these rollback tests build one).
func mustHashedRelWire(t *testing.T, w storeutil.RelWire, typeName string) storeutil.RelWire {
	t.Helper()
	r, err := storeutil.WireToRelChecked(w)
	if err != nil {
		t.Fatalf("mustHashedRelWire: decode: %v", err)
	}
	hash, err := integrity.ComputeRelHashChecked(r, typeName)
	if err != nil {
		t.Fatalf("mustHashedRelWire: compute hash: %v", err)
	}
	w.Hash = hash
	return w
}
