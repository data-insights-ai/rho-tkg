package core

import (
	"context"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/memory"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/types"
)

func TestImportStageSpillsAndReplaysFromFile(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	headerBody, err := msgpack.Marshal(&exportHeader{Version: exportFormatVersion})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	registryBody, err := msgpack.Marshal(&tiered.RegistryFileData{
		Labels:   []string{"", "SpilledLabel"},
		RelTypes: []string{"", "SPILLED_REL"},
	})
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}

	headerSize := int64(5 + len(headerBody))
	stage := newImportStage(t.TempDir(), headerSize)
	defer stage.close()

	if err := stage.writeRecord(exportTagHeader, headerBody, headerSize); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if stage.file != nil {
		t.Fatal("header record spilled before memory limit was exceeded")
	}

	registrySize := int64(5 + len(registryBody))
	if err := stage.writeRecord(exportTagRegistry, registryBody, registrySize); err != nil {
		t.Fatalf("write registry: %v", err)
	}
	if stage.file == nil {
		t.Fatal("registry record did not spill staging to a file")
	}

	br, err := stage.reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}

	g.mu.Lock()
	rollback := newImportRollback(g)
	rollback.emptyTarget = true
	err = g.importReplayStageLocked(stage, br, rollback)
	g.mu.Unlock()
	if err != nil {
		t.Fatalf("importReplayStageLocked: %v", err)
	}

	if _, ok := g.labels.Lookup("SpilledLabel"); !ok {
		t.Fatal("spilled import replay did not import label registry")
	}
	if _, ok := g.relTypes.Lookup("SPILLED_REL"); !ok {
		t.Fatal("spilled import replay did not import relationship type registry")
	}
}

func TestImportStageMemoryReaderDirectBranches(t *testing.T) {
	t.Parallel()

	headerBody, err := msgpack.Marshal(&exportHeader{Version: exportFormatVersion})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	stage := newImportStage("", int64(5+len(headerBody)+1))
	defer stage.close()

	if err := stage.writeRecord(exportTagHeader, headerBody, int64(5+len(headerBody))); err != nil {
		t.Fatalf("write header: %v", err)
	}
	if stage.file != nil {
		t.Fatal("memory-sized record unexpectedly spilled to file")
	}

	br, err := stage.reader()
	if err != nil {
		t.Fatalf("reader: %v", err)
	}
	tag, data, err := readExportRecord(br)
	if err != nil {
		t.Fatalf("read memory stage: %v", err)
	}
	if tag != exportTagHeader || !bytes.Equal(data, headerBody) {
		t.Fatalf("memory stage record = tag 0x%02x body %x, want tag 0x%02x body %x", tag, data, exportTagHeader, headerBody)
	}
	if _, _, err := readExportRecord(br); !errors.Is(err, io.EOF) {
		t.Fatalf("memory stage trailing read = %v, want EOF", err)
	}
}

func TestImportStageEnsureFileCreateAndCleanupBranches(t *testing.T) {
	t.Parallel()

	stage := newImportStage(t.TempDir(), 0)
	if err := stage.ensureFile(); err != nil {
		t.Fatalf("ensureFile empty stage: %v", err)
	}
	if stage.file == nil || stage.writer == nil || stage.path == "" {
		t.Fatalf("ensureFile did not initialize file-backed stage: file=%v writer=%v path=%q", stage.file, stage.writer, stage.path)
	}
	path := stage.path
	stage.close()
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stage file after close: stat err=%v, want os.ErrNotExist", err)
	}

	missingDir := filepath.Join(t.TempDir(), "missing")
	if err := newImportStage(missingDir, 0).ensureFile(); err == nil {
		t.Fatal("ensureFile with missing staging dir succeeded")
	}
}

func TestImportStageReaderSurfacesFileErrors(t *testing.T) {
	t.Parallel()

	headerBody, err := msgpack.Marshal(&exportHeader{Version: exportFormatVersion})
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	stage := newImportStage(t.TempDir(), 0)
	if err := stage.writeRecord(exportTagHeader, headerBody, int64(5+len(headerBody))); err != nil {
		t.Fatalf("write file-backed header: %v", err)
	}
	path := stage.path
	if err := stage.file.Close(); err != nil {
		t.Fatalf("close staging file before flush: %v", err)
	}
	if _, err := stage.reader(); err == nil {
		t.Fatal("reader succeeded after closing file with buffered data")
	}
	_ = os.Remove(path)

	stage = newImportStage(t.TempDir(), 0)
	if err := stage.ensureFile(); err != nil {
		t.Fatalf("ensure empty file-backed stage: %v", err)
	}
	path = stage.path
	if err := stage.file.Close(); err != nil {
		t.Fatalf("close staging file before rewind: %v", err)
	}
	if _, err := stage.reader(); err == nil {
		t.Fatal("reader succeeded after closing file before rewind")
	}
	_ = os.Remove(path)
}

func TestImportTargetEmptyLockedDirectBranches(t *testing.T) {
	t.Parallel()

	empty := newTestGraph(t)
	empty.mu.Lock()
	got, err := empty.importTargetEmptyLocked()
	empty.mu.Unlock()
	if err != nil || !got {
		t.Fatalf("empty target = (%v, %v), want (true, nil)", got, err)
	}

	withNode := newTestGraph(t)
	if _, err := withNode.Nodes.Add(context.Background(), []string{"Present"}, nil); err != nil {
		t.Fatalf("seed node: %v", err)
	}
	withNode.mu.Lock()
	got, err = withNode.importTargetEmptyLocked()
	withNode.mu.Unlock()
	if err != nil || got {
		t.Fatalf("target with current node = (%v, %v), want (false, nil)", got, err)
	}

	withRel := newTestGraph(t)
	withRel.store = &importTargetProbeStore{
		Store:            memory.New(),
		relCountOverride: intPtr(1),
	}
	withRel.mu.Lock()
	got, err = withRel.importTargetEmptyLocked()
	withRel.mu.Unlock()
	if err != nil || got {
		t.Fatalf("target with current relationship = (%v, %v), want (false, nil)", got, err)
	}

	withNodeHistory := newTestGraph(t)
	withNodeHistory.store = &importTargetProbeStore{
		Store:       memory.New(),
		nodeHistIDs: []types.NodeID{1},
	}
	withNodeHistory.mu.Lock()
	got, err = withNodeHistory.importTargetEmptyLocked()
	withNodeHistory.mu.Unlock()
	if err != nil || got {
		t.Fatalf("target with node history = (%v, %v), want (false, nil)", got, err)
	}

	withRelHistory := newTestGraph(t)
	withRelHistory.store = &importTargetProbeStore{
		Store:      memory.New(),
		relHistIDs: []types.RelID{100},
	}
	withRelHistory.mu.Lock()
	got, err = withRelHistory.importTargetEmptyLocked()
	withRelHistory.mu.Unlock()
	if err != nil || got {
		t.Fatalf("target with relationship history = (%v, %v), want (false, nil)", got, err)
	}
}

func TestImportTargetEmptyLockedWrapsStoreProbeErrors(t *testing.T) {
	t.Parallel()

	probeErr := errors.New("probe error")
	tests := []struct {
		name  string
		store *importTargetProbeStore
	}{
		{
			name: "node count",
			store: &importTargetProbeStore{
				Store:        memory.New(),
				nodeCountErr: probeErr,
			},
		},
		{
			name: "relationship count",
			store: &importTargetProbeStore{
				Store:       memory.New(),
				relCountErr: probeErr,
			},
		},
		{
			name: "node history",
			store: &importTargetProbeStore{
				Store:       memory.New(),
				nodeHistErr: probeErr,
			},
		},
		{
			name: "relationship history",
			store: &importTargetProbeStore{
				Store:      memory.New(),
				relHistErr: probeErr,
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			g, err := New(Config{Store: tc.store})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() { _ = g.Close() })

			g.mu.Lock()
			_, err = g.importTargetEmptyLocked()
			g.mu.Unlock()
			if !errors.Is(err, probeErr) {
				t.Fatalf("importTargetEmptyLocked error = %v, want probe error", err)
			}
		})
	}
}

type importTargetProbeStore struct {
	*memory.Store

	nodeCountOverride *int
	relCountOverride  *int
	nodeHistIDs       []types.NodeID
	relHistIDs        []types.RelID

	nodeCountErr error
	relCountErr  error
	nodeHistErr  error
	relHistErr   error
}

func (s *importTargetProbeStore) NodeCount() (int, error) {
	if s.nodeCountErr != nil {
		return 0, s.nodeCountErr
	}
	if s.nodeCountOverride != nil {
		return *s.nodeCountOverride, nil
	}
	return s.Store.NodeCount()
}

func (s *importTargetProbeStore) RelationshipCount() (int, error) {
	if s.relCountErr != nil {
		return 0, s.relCountErr
	}
	if s.relCountOverride != nil {
		return *s.relCountOverride, nil
	}
	return s.Store.RelationshipCount()
}

func (s *importTargetProbeStore) AllNodeHistoryIDsFrom(after types.NodeID, limit int) ([]types.NodeID, error) {
	if s.nodeHistErr != nil {
		return nil, s.nodeHistErr
	}
	if s.nodeHistIDs != nil {
		return append([]types.NodeID(nil), s.nodeHistIDs...), nil
	}
	return s.Store.AllNodeHistoryIDsFrom(after, limit)
}

func (s *importTargetProbeStore) AllRelHistoryIDsFrom(after types.RelID, limit int) ([]types.RelID, error) {
	if s.relHistErr != nil {
		return nil, s.relHistErr
	}
	if s.relHistIDs != nil {
		return append([]types.RelID(nil), s.relHistIDs...), nil
	}
	return s.Store.AllRelHistoryIDsFrom(after, limit)
}

func intPtr(v int) *int {
	return &v
}
