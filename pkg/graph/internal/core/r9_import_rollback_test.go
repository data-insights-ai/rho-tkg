package core

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	writeImportMsgpackRecord(t, &stream, exportTagNode, storeutil.NodeWire{
		ID:           100,
		PrimaryLabel: 1,
		Version:      1,
	})
	writeImportMsgpackRecord(t, &stream, exportTagNodeHist, storeutil.NodeWire{
		ID:           100,
		PrimaryLabel: 1,
		Version:      0,
	})
	writeImportMsgpackRecord(t, &stream, exportTagRel, storeutil.RelWire{
		ID:      300,
		RelType: 1,
		StartID: 100,
		EndID:   200, // missing endpoint: PutRelationship fails after node/history replay
	})

	err = g.IO.Import(bytes.NewReader(stream.Bytes()))
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
	writeImportMsgpackRecord(t, &stream, exportTagRel, storeutil.RelWire{
		ID:      300,
		RelType: 1,
		StartID: int64(n.ID().SnowflakeID()),
		EndID:   200, // missing endpoint after the history replay
	})

	err = g.IO.Import(bytes.NewReader(stream.Bytes()))
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
	writeImportMsgpackRecord(t, &stream, exportTagNode, storeutil.NodeWire{
		ID:           int64(nodeID.SnowflakeID()),
		PrimaryLabel: 1,
		Version:      1,
	})
	writeImportMsgpackRecord(t, &stream, exportTagNodeHist, storeutil.NodeWire{
		ID:           int64(nodeID.SnowflakeID()),
		PrimaryLabel: 1,
		Version:      0,
	})
	writeImportMsgpackRecord(t, &stream, exportTagRel, storeutil.RelWire{
		ID:      int64(relID.SnowflakeID()),
		RelType: 1,
		StartID: int64(nodeID.SnowflakeID()),
		EndID:   int64(missingID.SnowflakeID()),
	})

	err := g.IO.Import(bytes.NewReader(stream.Bytes()))
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
