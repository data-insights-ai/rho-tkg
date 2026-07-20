package core

import (
	"bytes"
	"context"
	"errors"
	"testing"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

type tieredWrapperStore struct {
	*tiered.Store
	setLabelRegistryCalls int
}

func (s *tieredWrapperStore) SetLabelRegistry(reg *registrypkg.LabelRegistry) {
	s.setLabelRegistryCalls++
	s.Store.SetLabelRegistry(reg)
}

var _ tieredAdminStore = (*tieredWrapperStore)(nil)
var _ tieredRegistryLoader = (*tieredWrapperStore)(nil)
var _ tieredLabelRegistrySetter = (*tieredWrapperStore)(nil)

func newTieredWrapperGraph(t *testing.T) (*Core, *tieredWrapperStore) {
	t.Helper()
	wrapper := &tieredWrapperStore{Store: newTestTieredStore(t)}
	g, err := New(Config{Store: wrapper})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g, wrapper
}

func TestTieredWrapper_AdminOperationsUseCapabilities(t *testing.T) {
	g, wrapper := newTieredWrapperGraph(t)
	if wrapper.setLabelRegistryCalls == 0 {
		t.Fatal("New did not wire the label registry through the tiered wrapper")
	}

	n, err := g.Nodes.Add(context.Background(), []string{"Case"}, nil)
	if err != nil {
		t.Fatalf("Add reference node: %v", err)
	}
	if err := g.Admin.Archive(n.ID()); err != nil {
		t.Fatalf("Archive through tiered wrapper: %v", err)
	}
	if err := g.Admin.Restore(n.ID()); err != nil {
		t.Fatalf("Restore through tiered wrapper: %v", err)
	}
	if _, err := g.Admin.ListShards(); err != nil {
		t.Fatalf("ListShards through tiered wrapper: %v", err)
	}
	if _, err := g.Admin.VerifyShard(wrapper.HotShardForTest().Name()); err != nil {
		t.Fatalf("VerifyShard through tiered wrapper: %v", err)
	}
}

func TestTieredWrapper_TxRollbackRewiresLabelRegistry(t *testing.T) {
	g, wrapper := newTieredWrapperGraph(t)
	wrapper.setLabelRegistryCalls = 0

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if wrapper.setLabelRegistryCalls == 0 {
		t.Fatal("Rollback did not rewire the label registry through the tiered wrapper")
	}
}

func TestTieredWrapper_ImportRollbackRewiresLabelRegistry(t *testing.T) {
	g, wrapper := newTieredWrapperGraph(t)
	wrapper.setLabelRegistryCalls = 0

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
	if wrapper.setLabelRegistryCalls == 0 {
		t.Fatal("Import rollback did not rewire the label registry through the tiered wrapper")
	}
	if _, err := g.store.GetNode(nodeID); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("GetNode(%d): got %v, want ErrNodeNotFound", nodeID, err)
	}
	if _, ok := g.labels.Lookup("Signal"); ok {
		t.Fatal("label registry kept imported Signal after failed import")
	}
}
