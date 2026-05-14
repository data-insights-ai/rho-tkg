package core

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"testing"

	"github.com/vmihailenco/msgpack/v5"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/memory"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

type exportPagingTrackingStore struct {
	storepkg.MandatoryStore
	historyPager          storepkg.HistoryVersionPageCapability
	allNodesCalls         atomic.Int64
	allRelationshipsCalls atomic.Int64
	allNodeIDsCalls       atomic.Int64
	allRelIDsCalls        atomic.Int64
	getNodeCalls          atomic.Int64
	getRelCalls           atomic.Int64
	getNodeHistoryCalls   atomic.Int64
	getRelHistoryCalls    atomic.Int64
	nodeHistoryPageCalls  atomic.Int64
	relHistoryPageCalls   atomic.Int64
}

func (s *exportPagingTrackingStore) AllNodes(opts storepkg.QueryOpts) ([]*types.Node, error) {
	s.allNodesCalls.Add(1)
	return s.MandatoryStore.AllNodes(opts)
}

func (s *exportPagingTrackingStore) AllRelationships(opts storepkg.QueryOpts) ([]*types.Relationship, error) {
	s.allRelationshipsCalls.Add(1)
	return s.MandatoryStore.AllRelationships(opts)
}

func (s *exportPagingTrackingStore) AllNodeIDs(opts storepkg.QueryOpts) ([]types.NodeID, error) {
	s.allNodeIDsCalls.Add(1)
	return s.MandatoryStore.AllNodeIDs(opts)
}

func (s *exportPagingTrackingStore) AllRelIDs(opts storepkg.QueryOpts) ([]types.RelID, error) {
	s.allRelIDsCalls.Add(1)
	return s.MandatoryStore.AllRelIDs(opts)
}

func (s *exportPagingTrackingStore) GetNode(id types.NodeID) (*types.Node, error) {
	s.getNodeCalls.Add(1)
	return s.MandatoryStore.GetNode(id)
}

func (s *exportPagingTrackingStore) GetRelationship(id types.RelID) (*types.Relationship, error) {
	s.getRelCalls.Add(1)
	return s.MandatoryStore.GetRelationship(id)
}

func (s *exportPagingTrackingStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	s.getNodeHistoryCalls.Add(1)
	return s.MandatoryStore.GetNodeHistory(id)
}

func (s *exportPagingTrackingStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	s.getRelHistoryCalls.Add(1)
	return s.MandatoryStore.GetRelHistory(id)
}

func (s *exportPagingTrackingStore) NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	s.nodeHistoryPageCalls.Add(1)
	return s.historyPager.NodeHistoryVersionsFrom(id, startVersion, limit)
}

func (s *exportPagingTrackingStore) RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	s.relHistoryPageCalls.Add(1)
	return s.historyPager.RelHistoryVersionsFrom(id, startVersion, limit)
}

func (s *exportPagingTrackingStore) resetExportCounters() {
	s.allNodesCalls.Store(0)
	s.allRelationshipsCalls.Store(0)
	s.allNodeIDsCalls.Store(0)
	s.allRelIDsCalls.Store(0)
	s.getNodeCalls.Store(0)
	s.getRelCalls.Store(0)
	s.getNodeHistoryCalls.Store(0)
	s.getRelHistoryCalls.Store(0)
	s.nodeHistoryPageCalls.Store(0)
	s.relHistoryPageCalls.Store(0)
}

type exportHistoryFallbackStore struct {
	storepkg.MandatoryStore
	getNodeHistoryCalls atomic.Int64
	getRelHistoryCalls  atomic.Int64
}

func (s *exportHistoryFallbackStore) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	s.getNodeHistoryCalls.Add(1)
	return s.MandatoryStore.GetNodeHistory(id)
}

func (s *exportHistoryFallbackStore) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	s.getRelHistoryCalls.Add(1)
	return s.MandatoryStore.GetRelHistory(id)
}

type exportEmbeddedNativeHistoryWrapper struct {
	*memory.Store
	getNodeHistoryCalls atomic.Int64
	getRelHistoryCalls  atomic.Int64
}

func (s *exportEmbeddedNativeHistoryWrapper) GetNodeHistory(id types.NodeID) ([]*types.Node, error) {
	s.getNodeHistoryCalls.Add(1)
	return s.Store.GetNodeHistory(id)
}

func (s *exportEmbeddedNativeHistoryWrapper) GetRelHistory(id types.RelID) ([]*types.Relationship, error) {
	s.getRelHistoryCalls.Add(1)
	return s.Store.GetRelHistory(id)
}

type failingHistoryPager struct {
	err error
}

func (p failingHistoryPager) NodeHistoryVersionsFrom(types.NodeID, uint32, int) ([]*types.Node, error) {
	return nil, p.err
}

func (p failingHistoryPager) RelHistoryVersionsFrom(types.RelID, uint32, int) ([]*types.Relationship, error) {
	return nil, p.err
}

type nonAdvancingHistoryPager struct {
	nodeCalls atomic.Int64
	relCalls  atomic.Int64
}

func (p *nonAdvancingHistoryPager) NodeHistoryVersionsFrom(id types.NodeID, startVersion uint32, limit int) ([]*types.Node, error) {
	p.nodeCalls.Add(1)
	history := make([]*types.Node, limit)
	for i := range history {
		n := types.NewNode(id, 1, nil)
		if startVersion == 0 {
			n.SetVersion(uint32(i))
		}
		history[i] = n
	}
	return history, nil
}

func (p *nonAdvancingHistoryPager) RelHistoryVersionsFrom(id types.RelID, startVersion uint32, limit int) ([]*types.Relationship, error) {
	p.relCalls.Add(1)
	history := make([]*types.Relationship, limit)
	for i := range history {
		r := types.NewRelationship(id, 1, types.NodeID(1), types.NodeID(2))
		if startVersion == 0 {
			r.SetVersion(uint32(i))
		}
		history[i] = r
	}
	return history, nil
}

// buildExportGraph creates a graph with nodes, rels, and history.
// Returns the graph, a node ID and rel ID for later assertions.
func buildExportGraph(t *testing.T) (g *Core, nodeID, relID uint64) {
	t.Helper()
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	start, err := g.Nodes.Add([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	end, err := g.Nodes.Add([]string{"City"}, map[string]any{"city": "Vienna"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r, err := g.Rels.Add("LIVES_IN", start, end, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	// Create a version history entry on the start node.
	_, err = g.Nodes.Update(start.ID(), map[string]any{"name": "Alice Updated"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	return g, uint64(start.ID()), uint64(r.ID())
}

// TestExportImport_RoundTrip_MemoryStore tests a full export→import roundtrip
// using MemoryStore on both sides.
func TestExportImport_RoundTrip_MemoryStore(t *testing.T) {
	src, _, _ := buildExportGraph(t)
	defer src.Close() //nolint:errcheck

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Import into a fresh
	dst, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	// Verify node count.
	srcNC, _ := src.Nodes.Count()
	dstNC, _ := dst.Nodes.Count()
	if dstNC != srcNC {
		t.Errorf("NodeCount: src=%d, dst=%d", srcNC, dstNC)
	}

	// Verify rel count.
	srcRC, _ := src.Rels.Count()
	dstRC, _ := dst.Rels.Count()
	if dstRC != srcRC {
		t.Errorf("RelCount: src=%d, dst=%d", srcRC, dstRC)
	}

	// Verify nodes by label.
	srcPersons, _ := src.Nodes.ByLabel("Person", storepkg.QueryOpts{})
	dstPersons, _ := dst.Nodes.ByLabel("Person", storepkg.QueryOpts{})
	if len(dstPersons) != len(srcPersons) {
		t.Errorf("Person count: src=%d, dst=%d", len(srcPersons), len(dstPersons))
	}

	// Verify a property on an imported node.
	if len(dstPersons) > 0 {
		val, ok := dstPersons[0].GetProperty("name")
		if !ok {
			t.Error("imported Person node missing 'name' property")
		} else if val != "Alice Updated" {
			t.Errorf("name = %q; want %q", val, "Alice Updated")
		}
	}
}

// TestExport_Empty_Graph verifies ExportGraph on a graph with no entities.
func TestExport_Empty_Graph(t *testing.T) {
	g, _ := New(Config{Store: memory.New()})
	defer g.Close() //nolint:errcheck

	var buf bytes.Buffer
	if err := g.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph empty: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("expected non-empty output even for empty graph (header + registry)")
	}

	// Import into a fresh
	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph empty: %v", err)
	}
	nc, _ := dst.Nodes.Count()
	if nc != 0 {
		t.Errorf("NodeCount after empty import = %d; want 0", nc)
	}
}

func TestIOExportNilWriterReturnsSentinel(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	var nilWriter io.Writer
	if err := g.IO.Export(nilWriter); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("Export(nil writer): got %v, want ErrNilWriter", err)
	}

	var typedNilWriter *bytes.Buffer
	if err := g.IO.Export(typedNilWriter); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("Export(typed nil writer): got %v, want ErrNilWriter", err)
	}
}

func TestIOImportNilReaderReturnsSentinel(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	var nilReader io.Reader
	if err := g.IO.Import(nilReader); !errors.Is(err, ErrNilReader) {
		t.Fatalf("Import(nil reader): got %v, want ErrNilReader", err)
	}

	var typedNilReader *bytes.Buffer
	if err := g.IO.ImportWithOptions(typedNilReader, tkgio.ImportOptions{}); !errors.Is(err, ErrNilReader) {
		t.Fatalf("ImportWithOptions(typed nil reader): got %v, want ErrNilReader", err)
	}
}

func TestTxExportNilWriterReturnsSentinel(t *testing.T) {
	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	tx, err := g.BeginTx()
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	defer tx.Rollback() //nolint:errcheck

	var nilWriter io.Writer
	if err := tx.Export(nilWriter); !errors.Is(err, ErrNilWriter) {
		t.Fatalf("tx.Export(nil writer): got %v, want ErrNilWriter", err)
	}
}

// TestExport_WithNodeHistory verifies that node version history survives the roundtrip.
func TestExport_WithNodeHistory(t *testing.T) {
	g, _ := New(Config{Store: memory.New()})
	defer g.Close() //nolint:errcheck

	n, _ := g.Nodes.Add([]string{"Item"}, map[string]any{"v": int64(1)})
	id := n.ID()
	_, _ = g.Nodes.Update(id, map[string]any{"v": int64(2)})
	_, _ = g.Nodes.Update(id, map[string]any{"v": int64(3)})

	srcHistory, _ := g.Nodes.History(id)

	var buf bytes.Buffer
	if err := g.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	dstHistory, err := dst.Nodes.History(id)
	if err != nil {
		t.Fatalf("GetNodeHistory on dst: %v", err)
	}
	if len(dstHistory) != len(srcHistory) {
		t.Errorf("history length: src=%d, dst=%d", len(srcHistory), len(dstHistory))
	}
}

// TestExport_RelHistory verifies that relationship version history survives the roundtrip.
func TestExport_RelHistory(t *testing.T) {
	g, _ := New(Config{Store: memory.New()})
	defer g.Close() //nolint:errcheck

	a, _ := g.Nodes.Add([]string{"A"}, nil)
	b, _ := g.Nodes.Add([]string{"B"}, nil)
	r, _ := g.Rels.Add("EDGE", a, b, map[string]any{"w": int64(1)})
	rid := r.ID()
	_, _ = g.Rels.Update(rid, map[string]any{"w": int64(2)})

	srcHistory, _ := g.Rels.History(rid)

	var buf bytes.Buffer
	if err := g.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	dstHistory, err := dst.Rels.History(rid)
	if err != nil {
		t.Fatalf("GetRelHistory on dst: %v", err)
	}
	if len(dstHistory) != len(srcHistory) {
		t.Errorf("rel history length: src=%d, dst=%d", len(srcHistory), len(dstHistory))
	}
}

// TestImport_IdempotentRegistry verifies that importing into a graph that already
// has the same registries populated does not return an error.
func TestImport_IdempotentRegistry(t *testing.T) {
	src, _ := New(Config{Store: memory.New()})
	defer src.Close() //nolint:errcheck

	src.Nodes.Add([]string{"Foo"}, nil) //nolint:errcheck

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Destination already has the "Foo" label registered (from a prior node add).
	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close()                   //nolint:errcheck
	dst.Nodes.Add([]string{"Foo"}, nil) //nolint:errcheck

	// Importing with a pre-populated registry must not fail.
	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph with existing registry: %v", err)
	}
}

// TestExport_Writer_Error verifies that ExportGraph propagates a write error.
func TestExport_Writer_Error(t *testing.T) {
	g, _ := New(Config{Store: memory.New()})
	defer g.Close()                 //nolint:errcheck
	g.Nodes.Add([]string{"X"}, nil) //nolint:errcheck

	// errWriter fails after 0 bytes written.
	ew := &errWriter{failAfter: 0}
	err := g.IO.Export(ew)
	if err == nil {
		t.Error("expected error from ExportGraph when writer fails")
	}
}

// TestImport_InvalidHeader verifies that ImportGraph returns ErrIncompatibleExport
// when the export header carries an unsupported version.
func TestImport_InvalidHeader(t *testing.T) {
	// Build a valid export.
	src, _ := New(Config{Store: memory.New()})
	defer src.Close() //nolint:errcheck

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Corrupt the version byte in the header body. The header record is the
	// first record: [tag(1)] [len(4)] [msgpack body]. We patch the version
	// field inside the msgpack body by brute-forcing a modified byte slice.
	data := buf.Bytes()
	// Find the version byte by scanning the body: the msgpack-encoded
	// exportHeader starts with a fixmap header. We rely on the fact that
	// the version byte is encoded as a small positive integer and is the
	// first map value. We patch it to 0xFF (unsupported version).
	// Instead of brittle offset math, we just build a deliberately bad stream:
	badBuf := makeBadVersionStream(t)

	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	err := dst.IO.Import(badBuf)
	if !errors.Is(err, ErrIncompatibleExport) {
		t.Errorf("expected ErrIncompatibleExport, got: %v", err)
	}
	_ = data // suppress unused warning
}

// TestExportImport_IntegrityPreserved verifies that node/rel hash chains survive
// the export→import roundtrip.
func TestExportImport_IntegrityPreserved(t *testing.T) {
	src, _ := New(Config{Store: memory.New()})
	defer src.Close() //nolint:errcheck

	a, _ := src.Nodes.Add([]string{"Node"}, map[string]any{"v": int64(1)})
	b, _ := src.Nodes.Add([]string{"Node"}, map[string]any{"v": int64(2)})
	r, _ := src.Rels.Add("EDGE", a, b, nil)

	srcAHash := a.Integrity().Hash
	srcRHash := r.Integrity().Hash

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	// Verify imported node hash is preserved.
	importedA, err := dst.Nodes.Get(a.ID())
	if err != nil {
		t.Fatalf("GetNode after import: %v", err)
	}
	if importedA.Integrity() == nil {
		t.Fatal("imported node has nil Integrity")
	}
	if importedA.Integrity().Hash != srcAHash {
		t.Errorf("node hash: import=%q, src=%q", importedA.Integrity().Hash, srcAHash)
	}

	// Verify imported rel hash is preserved.
	importedR, err := dst.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship after import: %v", err)
	}
	if importedR.Integrity() == nil {
		t.Fatal("imported rel has nil Integrity")
	}
	if importedR.Integrity().Hash != srcRHash {
		t.Errorf("rel hash: import=%q, src=%q", importedR.Integrity().Hash, srcRHash)
	}
}

// TestExportImport_EndpointHashesPreserved verifies FromNodeHash/ToNodeHash survive
// the export→import roundtrip (Phase 4.13 integration test).
func TestExportImport_EndpointHashesPreserved(t *testing.T) {
	src, _ := New(Config{Store: memory.New()})
	defer src.Close() //nolint:errcheck

	a, _ := src.Nodes.Add([]string{"A"}, nil)
	b, _ := src.Nodes.Add([]string{"B"}, nil)
	r, _ := src.Rels.Add("R", a, b, nil)

	srcFrom := r.Integrity().FromNodeHash
	srcTo := r.Integrity().ToNodeHash

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	importedR, err := dst.Rels.Get(r.ID())
	if err != nil {
		t.Fatalf("GetRelationship: %v", err)
	}
	if importedR.Integrity().FromNodeHash != srcFrom {
		t.Errorf("FromNodeHash: import=%q, src=%q", importedR.Integrity().FromNodeHash, srcFrom)
	}
	if importedR.Integrity().ToNodeHash != srcTo {
		t.Errorf("ToNodeHash: import=%q, src=%q", importedR.Integrity().ToNodeHash, srcTo)
	}
}

// TestExportImport_AuthorIDPreserved verifies AuthorID/Signature survive the
// export→import roundtrip (Phase 4.14 integration test).
func TestExportImport_AuthorIDPreserved(t *testing.T) {
	src, _ := New(Config{Store: memory.New()})
	defer src.Close() //nolint:errcheck

	n, _ := src.Nodes.Add([]string{"Doc"}, map[string]any{
		"tkg_author_id": "author@example.com",
		"tkg_signature": []byte("test-sig"),
	})

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	imported, err := dst.Nodes.Get(n.ID())
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	ig := imported.Integrity()
	if ig == nil {
		t.Fatal("imported Integrity is nil")
	}
	if ig.AuthorID != "author@example.com" {
		t.Errorf("AuthorID: import=%q, src=%q", ig.AuthorID, "author@example.com")
	}
	if string(ig.Signature) != "test-sig" {
		t.Errorf("Signature: import=%v, want %q", ig.Signature, "test-sig")
	}
}

// TestExport_ShadowProperty_Survives verifies that shadow property values on
// a node are accessible via ResolveNodeProperty after an import.
func TestExport_ShadowProperty_Survives(t *testing.T) {
	src, _ := New(Config{Store: memory.New()})
	defer src.Close() //nolint:errcheck

	n, _ := src.Nodes.Add([]string{"X"}, map[string]any{"k": int64(7)})
	srcHash, _ := src.Resolve.NodeProperty(n, types.ShadowHash)

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := New(Config{Store: memory.New()})
	defer dst.Close() //nolint:errcheck

	if err := dst.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	imported, _ := dst.Nodes.Get(n.ID())
	dstHash, _ := dst.Resolve.NodeProperty(imported, types.ShadowHash)
	if dstHash != srcHash {
		t.Errorf("tkg_hash: dst=%v, src=%v", dstHash, srcHash)
	}
}

// --- v3.0.53 bug-fix tests ---

// TestImportGraph_IncompatibleLabelRegistry verifies that importing into a graph
// whose label registry was populated with different token mappings returns
// ErrIncompatibleRegistry instead of silently corrupting all entity labels.
func TestImportGraph_IncompatibleLabelRegistry(t *testing.T) {
	// Build source graph with labels "Person" and "City".
	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	start, _ := src.Nodes.Add([]string{"Person"}, nil)
	end, _ := src.Nodes.Add([]string{"City"}, nil)
	_, _ = src.Rels.Add("LIVES_IN", start, end, nil)

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Destination has a DIFFERENT label at token 1 — "Company" instead of "Person".
	dst, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck
	_, _ = dst.Nodes.Add([]string{"Company"}, nil)

	err = dst.IO.Import(&buf)
	if !errors.Is(err, ErrIncompatibleRegistry) {
		t.Errorf("ImportGraph: got %v, want ErrIncompatibleRegistry", err)
	}
}

// TestImportGraph_CompatibleRegistryIdempotent verifies that importing the same
// export twice into the same graph succeeds — the second import detects a
// non-empty but identical registry and continues without error.
func TestImportGraph_CompatibleRegistryIdempotent(t *testing.T) {
	src, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	a, _ := src.Nodes.Add([]string{"Alpha"}, map[string]any{"x": int64(1)})
	b, _ := src.Nodes.Add([]string{"Beta"}, nil)
	_, _ = src.Rels.Add("LINK", a, b, nil)

	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}
	exportedBytes := buf.Bytes()

	dst, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck

	// First import — fresh graph, registries empty.
	if err := dst.IO.Import(bytes.NewReader(exportedBytes)); err != nil {
		t.Fatalf("first ImportGraph: %v", err)
	}

	// Second import — same export bytes, registry now populated with identical mapping.
	if err := dst.IO.Import(bytes.NewReader(exportedBytes)); err != nil {
		t.Fatalf("second ImportGraph (idempotent): %v", err)
	}

	// Node count should be unchanged (re-import skips existing nodes).
	nc, _ := dst.Nodes.Count()
	srcNC, _ := src.Nodes.Count()
	if nc != srcNC {
		t.Errorf("NodeCount after idempotent import: dst=%d, src=%d", nc, srcNC)
	}
}

// TestReadExportRecord_OversizeRecord verifies that a crafted export record
// whose length header claims more than maxExportRecordSize bytes causes
// ImportGraph to return an error without allocating the huge buffer.
func TestReadExportRecord_OversizeRecord(t *testing.T) {
	// Craft a 5-byte "record": tag=exportTagHeader(0x01), length=128MiB+1.
	// No body follows — the size guard must fire before make([]byte, length).
	const oversizeLen = 128*1024*1024 + 1

	var buf bytes.Buffer
	buf.WriteByte(0x01) // exportTagHeader tag
	var lenBytes [4]byte
	binary.BigEndian.PutUint32(lenBytes[:], uint32(oversizeLen))
	buf.Write(lenBytes[:])
	// Deliberately omit the body — guard fires first, no io.ReadFull attempted.

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	err = g.IO.Import(&buf)
	if err == nil {
		t.Fatal("ImportGraph: expected error for oversize record, got nil")
	}
	// Must NOT be io.ErrUnexpectedEOF — that would mean the guard was absent
	// and the body read was attempted (which would have caused OOM first).
	if errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("ImportGraph returned ErrUnexpectedEOF — size guard was not applied before body read")
	}
}

func TestReadExportRecordDirectReaderBranches(t *testing.T) {
	t.Parallel()

	if _, _, err := readExportRecord(bytes.NewReader(nil)); !errors.Is(err, io.EOF) {
		t.Fatalf("empty reader read = %v, want EOF", err)
	}
	if _, _, err := readExportRecord(bytes.NewReader([]byte{exportTagHeader, 0, 0, 0})); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short header read = %v, want ErrUnexpectedEOF", err)
	}

	var truncated bytes.Buffer
	if err := writeExportRecord(&truncated, exportTagNode, []byte{1, 2, 3}); err != nil {
		t.Fatalf("write truncated source: %v", err)
	}
	truncatedBytes := truncated.Bytes()
	if _, _, err := readExportRecord(bytes.NewReader(truncatedBytes[:len(truncatedBytes)-1])); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body read = %v, want ErrUnexpectedEOF", err)
	}
}

func TestReadExportRecordBytesDirectBranches(t *testing.T) {
	t.Parallel()

	offset := 0
	if _, _, err := readExportRecordBytes(nil, &offset); !errors.Is(err, io.EOF) {
		t.Fatalf("empty bytes read = %v, want EOF", err)
	}

	offset = 0
	if _, _, err := readExportRecordBytes([]byte{exportTagHeader, 0, 0, 0}, &offset); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("short header read = %v, want ErrUnexpectedEOF", err)
	}
	if offset != 0 {
		t.Fatalf("short header advanced offset to %d, want 0", offset)
	}

	var oversize [5]byte
	oversize[0] = exportTagNode
	binary.BigEndian.PutUint32(oversize[1:5], maxExportRecordSize+1)
	offset = 0
	if _, _, err := readExportRecordBytes(oversize[:], &offset); err == nil || errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("oversize read = %v, want non-EOF size error", err)
	}
	if offset != 0 {
		t.Fatalf("oversize header advanced offset to %d, want 0", offset)
	}

	var truncated bytes.Buffer
	if err := writeExportRecord(&truncated, exportTagRel, []byte{1, 2, 3}); err != nil {
		t.Fatalf("write truncated source: %v", err)
	}
	truncatedBytes := truncated.Bytes()
	truncatedBytes = truncatedBytes[:len(truncatedBytes)-1]
	offset = 0
	if _, _, err := readExportRecordBytes(truncatedBytes, &offset); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated body read = %v, want ErrUnexpectedEOF", err)
	}
	if offset != 5 {
		t.Fatalf("truncated body offset = %d, want header length 5", offset)
	}

	var zeroBody bytes.Buffer
	if err := writeExportRecord(&zeroBody, exportTagRegistry, nil); err != nil {
		t.Fatalf("write zero-body source: %v", err)
	}
	offset = 0
	tag, data, err := readExportRecordBytes(zeroBody.Bytes(), &offset)
	if err != nil {
		t.Fatalf("zero-body read: %v", err)
	}
	if tag != exportTagRegistry || len(data) != 0 || offset != 5 {
		t.Fatalf("zero-body read = tag 0x%02x len %d offset %d, want tag 0x%02x len 0 offset 5", tag, len(data), offset, exportTagRegistry)
	}
	if _, _, err := readExportRecordBytes(zeroBody.Bytes(), &offset); !errors.Is(err, io.EOF) {
		t.Fatalf("zero-body trailing read = %v, want EOF", err)
	}

	var twoRecords bytes.Buffer
	if err := writeExportRecord(&twoRecords, exportTagHeader, []byte("one")); err != nil {
		t.Fatalf("write first record: %v", err)
	}
	if err := writeExportRecord(&twoRecords, exportTagRelHist, []byte("two")); err != nil {
		t.Fatalf("write second record: %v", err)
	}
	offset = 0
	tag, data, err = readExportRecordBytes(twoRecords.Bytes(), &offset)
	if err != nil || tag != exportTagHeader || string(data) != "one" {
		t.Fatalf("first record = tag 0x%02x data %q err %v, want header/one/nil", tag, data, err)
	}
	tag, data, err = readExportRecordBytes(twoRecords.Bytes(), &offset)
	if err != nil || tag != exportTagRelHist || string(data) != "two" {
		t.Fatalf("second record = tag 0x%02x data %q err %v, want relHist/two/nil", tag, data, err)
	}
	if offset != twoRecords.Len() {
		t.Fatalf("two-record offset = %d, want %d", offset, twoRecords.Len())
	}
}

func TestMarshalAndWriteDirectBranches(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	hdr := exportHeader{Version: exportFormatVersion, NodeCount: 1}
	if err := marshalAndWrite(&out, exportTagHeader, &hdr); err != nil {
		t.Fatalf("marshalAndWrite success: %v", err)
	}
	tag, data, err := readExportRecord(bytes.NewReader(out.Bytes()))
	if err != nil {
		t.Fatalf("read marshaled record: %v", err)
	}
	if tag != exportTagHeader {
		t.Fatalf("marshaled tag = 0x%02x, want 0x%02x", tag, exportTagHeader)
	}
	var got exportHeader
	if err := msgpack.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal marshaled header: %v", err)
	}
	if got.Version != hdr.Version || got.NodeCount != hdr.NodeCount {
		t.Fatalf("marshaled header = %+v, want %+v", got, hdr)
	}

	if err := marshalAndWrite(&out, exportTagHeader, func() {}); err == nil {
		t.Fatal("marshalAndWrite accepted an unsupported function value")
	}

	if err := marshalAndWrite(&errWriter{failAfter: 0}, exportTagHeader, &hdr); err == nil {
		t.Fatal("marshalAndWrite with failing writer returned nil")
	}
}

func TestWriteExportRecordHandlesShortWrites(t *testing.T) {
	t.Parallel()

	sw := &shortChunkWriter{maxChunk: 2}
	if err := writeExportRecord(sw, exportTagRelHist, []byte("payload")); err != nil {
		t.Fatalf("writeExportRecord short chunks: %v", err)
	}
	tag, data, err := readExportRecord(bytes.NewReader(sw.buf.Bytes()))
	if err != nil {
		t.Fatalf("read short-chunk record: %v", err)
	}
	if tag != exportTagRelHist || string(data) != "payload" {
		t.Fatalf("short-chunk record = tag 0x%02x data %q, want relHist/payload", tag, data)
	}

	if err := writeExportRecord(zeroProgressWriter{}, exportTagHeader, []byte("x")); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("writeExportRecord zero-progress writer = %v, want io.ErrShortWrite", err)
	}
}

func TestExportRecordSizeGuardRejectsOversize(t *testing.T) {
	t.Parallel()

	if err := validateExportRecordSize("export", exportTagNode, uint64(maxExportRecordSize)); err != nil {
		t.Fatalf("record at max size rejected: %v", err)
	}
	if err := validateExportRecordSize("export", exportTagNode, uint64(maxExportRecordSize)+1); err == nil {
		t.Fatal("oversize export record accepted")
	}
}

func TestWriteHistoryEntriesRejectNilRows(t *testing.T) {
	t.Parallel()

	var out bytes.Buffer
	if err := writeNodeHistoryEntries(&out, types.NodeID(1), []*types.Node{nil}); !errors.Is(err, ErrNilNode) {
		t.Fatalf("writeNodeHistoryEntries nil row = %v, want ErrNilNode", err)
	}
	if out.Len() != 0 {
		t.Fatalf("writeNodeHistoryEntries wrote %d bytes for nil row", out.Len())
	}

	if err := writeRelHistoryEntries(&out, types.RelID(2), []*types.Relationship{nil}); !errors.Is(err, ErrNilRelationship) {
		t.Fatalf("writeRelHistoryEntries nil row = %v, want ErrNilRelationship", err)
	}
	if out.Len() != 0 {
		t.Fatalf("writeRelHistoryEntries wrote %d bytes for nil row", out.Len())
	}
}

// --- helpers ---

// errWriter is a writer that fails after a given number of bytes.
type errWriter struct {
	failAfter int
	written   int
}

func (ew *errWriter) Write(p []byte) (int, error) {
	if ew.written >= ew.failAfter {
		return 0, errors.New("test write error")
	}
	n := len(p)
	if ew.written+n > ew.failAfter {
		n = ew.failAfter - ew.written
	}
	ew.written += n
	return n, nil
}

type shortChunkWriter struct {
	maxChunk int
	buf      bytes.Buffer
}

func (w *shortChunkWriter) Write(p []byte) (int, error) {
	if len(p) > w.maxChunk {
		p = p[:w.maxChunk]
	}
	return w.buf.Write(p)
}

type zeroProgressWriter struct{}

func (zeroProgressWriter) Write([]byte) (int, error) { return 0, nil }

// makeBadVersionStream creates an export stream with an invalid version byte.
// The version field is the uint8 value 0xFF, which is not a supported format version.
func makeBadVersionStream(t *testing.T) io.Reader {
	t.Helper()
	// We build a minimal valid stream manually:
	// header record with version=0xFF.
	// msgpack: fixmap{1} "v" -> 0xFF
	// Using msgpack encoding: {0x81, 0xa1, 0x76, 0xcc, 0xff} represents
	// a 1-element fixmap with key "v" (1-byte str) and value 255 (uint8).
	body := []byte{0x81, 0xa1, 0x76, 0xcc, 0xff}

	var buf bytes.Buffer
	var header [5]byte
	header[0] = 0x01 // exportTagHeader
	header[1] = 0
	header[2] = 0
	header[3] = 0
	header[4] = byte(len(body))
	buf.Write(header[:]) //nolint:errcheck
	buf.Write(body)      //nolint:errcheck
	return &buf
}

func TestExportGraph_CurrentEntitiesPageIDsBeforeFetching(t *testing.T) {
	base := memory.New()
	store := &exportPagingTrackingStore{MandatoryStore: base, historyPager: base}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	start, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"name": "b"})
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	if _, err := g.Rels.AddByID("LINKS", start.ID(), end.ID(), nil); err != nil {
		t.Fatalf("AddByID: %v", err)
	}

	store.resetExportCounters()
	if err := g.IO.Export(io.Discard); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if got := store.allNodesCalls.Load(); got != 0 {
		t.Fatalf("Export called AllNodes %d times; want ID-page current-node export", got)
	}
	if got := store.allRelationshipsCalls.Load(); got != 0 {
		t.Fatalf("Export called AllRelationships %d times; want ID-page current-relationship export", got)
	}
	if got := store.allNodeIDsCalls.Load(); got == 0 {
		t.Fatal("Export did not call AllNodeIDs")
	}
	if got := store.allRelIDsCalls.Load(); got == 0 {
		t.Fatal("Export did not call AllRelIDs")
	}
	if got := store.getNodeCalls.Load(); got != 2 {
		t.Fatalf("Export GetNode calls = %d, want 2", got)
	}
	if got := store.getRelCalls.Load(); got != 1 {
		t.Fatalf("Export GetRelationship calls = %d, want 1", got)
	}
}

func TestExportGraph_HistoryPagesVersionsWhenCapabilityAvailable(t *testing.T) {
	base := memory.New()
	store := &exportPagingTrackingStore{MandatoryStore: base, historyPager: base}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	start, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"name": "b"})
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	rel, err := g.Rels.AddByID("LINKS", start.ID(), end.ID(), map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	if _, err := g.Nodes.Update(start.ID(), map[string]any{"name": "c"}); err != nil {
		t.Fatalf("Update node: %v", err)
	}
	if _, err := g.Rels.Update(rel.ID(), map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("Update rel: %v", err)
	}

	store.resetExportCounters()
	if err := g.IO.Export(io.Discard); err != nil {
		t.Fatalf("Export: %v", err)
	}

	if got := store.nodeHistoryPageCalls.Load(); got == 0 {
		t.Fatal("Export did not call NodeHistoryVersionsFrom")
	}
	if got := store.relHistoryPageCalls.Load(); got == 0 {
		t.Fatal("Export did not call RelHistoryVersionsFrom")
	}
	if got := store.getNodeHistoryCalls.Load(); got != 0 {
		t.Fatalf("Export called GetNodeHistory %d times despite paged history capability", got)
	}
	if got := store.getRelHistoryCalls.Load(); got != 0 {
		t.Fatalf("Export called GetRelHistory %d times despite paged history capability", got)
	}
}

func TestExportGraph_HistoryVersionPagingSplitsDeepEntity(t *testing.T) {
	base := memory.New()
	store := &exportPagingTrackingStore{MandatoryStore: base, historyPager: base}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	id := types.NodeID(1)
	if err := store.PutNode(types.NewNode(id, 1, nil)); err != nil {
		t.Fatalf("PutNode: %v", err)
	}
	for v := uint32(0); v < exportHistoryVersionBatchSize+1; v++ {
		n := types.NewNode(id, 1, nil)
		n.SetVersion(v)
		if err := store.PutNodeVersion(id, v, n); err != nil {
			t.Fatalf("PutNodeVersion(%d): %v", v, err)
		}
	}

	store.resetExportCounters()
	if err := g.IO.Export(io.Discard); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := store.nodeHistoryPageCalls.Load(); got != 2 {
		t.Fatalf("Export NodeHistoryVersionsFrom calls = %d, want 2 for a %d-version history", got, exportHistoryVersionBatchSize+1)
	}
	if got := store.getNodeHistoryCalls.Load(); got != 0 {
		t.Fatalf("Export called GetNodeHistory %d times despite paged history capability", got)
	}
}

func TestExportGraph_HistoryFallsBackWithoutVersionPager(t *testing.T) {
	base := memory.New()
	store := &exportHistoryFallbackStore{MandatoryStore: base}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	start, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Doc"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	rel, err := g.Rels.AddByID("LINKS", start.ID(), end.ID(), map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	if _, err := g.Nodes.Update(start.ID(), map[string]any{"name": "b"}); err != nil {
		t.Fatalf("Update node: %v", err)
	}
	if _, err := g.Rels.Update(rel.ID(), map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("Update rel: %v", err)
	}

	if err := g.IO.Export(io.Discard); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := store.getNodeHistoryCalls.Load(); got == 0 {
		t.Fatal("Export did not fall back to GetNodeHistory")
	}
	if got := store.getRelHistoryCalls.Load(); got == 0 {
		t.Fatal("Export did not fall back to GetRelHistory")
	}
}

func TestExportGraph_IgnoresInheritedNativeHistoryPagerOnWrapper(t *testing.T) {
	store := &exportEmbeddedNativeHistoryWrapper{Store: memory.New()}
	if _, ok := any(store).(storepkg.HistoryVersionPageCapability); !ok {
		t.Fatal("test wrapper no longer inherits HistoryVersionPageCapability")
	}
	g, err := New(Config{Store: store})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	start, err := g.Nodes.Add([]string{"Doc"}, map[string]any{"name": "a"})
	if err != nil {
		t.Fatalf("Add start: %v", err)
	}
	end, err := g.Nodes.Add([]string{"Doc"}, nil)
	if err != nil {
		t.Fatalf("Add end: %v", err)
	}
	rel, err := g.Rels.AddByID("LINKS", start.ID(), end.ID(), map[string]any{"weight": int64(1)})
	if err != nil {
		t.Fatalf("AddByID: %v", err)
	}
	if _, err := g.Nodes.Update(start.ID(), map[string]any{"name": "b"}); err != nil {
		t.Fatalf("Update node: %v", err)
	}
	if _, err := g.Rels.Update(rel.ID(), map[string]any{"weight": int64(2)}); err != nil {
		t.Fatalf("Update rel: %v", err)
	}

	if err := g.IO.Export(io.Discard); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if got := store.getNodeHistoryCalls.Load(); got == 0 {
		t.Fatal("Export used inherited native NodeHistoryVersionsFrom instead of wrapper GetNodeHistory")
	}
	if got := store.getRelHistoryCalls.Load(); got == 0 {
		t.Fatal("Export used inherited native RelHistoryVersionsFrom instead of wrapper GetRelHistory")
	}
}

func TestExportHistoryHelpersDirectBranches(t *testing.T) {
	t.Parallel()

	node := types.NewNode(types.NodeID(1), 1, nil)
	node.SetVersion(1)
	if err := writeNodeHistoryEntries(io.Discard, node.ID(), []*types.Node{node}); err != nil {
		t.Fatalf("writeNodeHistoryEntries success: %v", err)
	}
	if err := writeNodeHistoryEntries(io.Discard, node.ID(), []*types.Node{nil}); err == nil {
		t.Fatal("writeNodeHistoryEntries accepted invalid node history entry")
	}
	if err := writeNodeHistoryEntries(&errWriter{failAfter: 0}, node.ID(), []*types.Node{node}); err == nil {
		t.Fatal("writeNodeHistoryEntries did not return writer error")
	}

	rel := types.NewRelationship(types.RelID(1), 1, types.NodeID(1), types.NodeID(2))
	rel.SetVersion(1)
	if err := writeRelHistoryEntries(io.Discard, rel.ID(), []*types.Relationship{rel}); err != nil {
		t.Fatalf("writeRelHistoryEntries success: %v", err)
	}
	if err := writeRelHistoryEntries(io.Discard, rel.ID(), []*types.Relationship{nil}); err == nil {
		t.Fatal("writeRelHistoryEntries accepted invalid relationship history entry")
	}
	if err := writeRelHistoryEntries(&errWriter{failAfter: 0}, rel.ID(), []*types.Relationship{rel}); err == nil {
		t.Fatal("writeRelHistoryEntries did not return writer error")
	}
}

func TestExportHistoryPagerErrorBranches(t *testing.T) {
	t.Parallel()

	c, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })

	errBoom := errors.New("history pager failed")
	if err := c.exportNodeHistory(io.Discard, failingHistoryPager{err: errBoom}, types.NodeID(1)); !errors.Is(err, errBoom) {
		t.Fatalf("exportNodeHistory pager error = %v, want %v", err, errBoom)
	}
	if err := c.exportRelHistory(io.Discard, failingHistoryPager{err: errBoom}, types.RelID(1)); !errors.Is(err, errBoom) {
		t.Fatalf("exportRelHistory pager error = %v, want %v", err, errBoom)
	}

	nonAdvancing := &nonAdvancingHistoryPager{}
	if err := c.exportNodeHistory(io.Discard, nonAdvancing, types.NodeID(1)); err == nil {
		t.Fatal("exportNodeHistory accepted a non-advancing history page")
	}
	if got := nonAdvancing.nodeCalls.Load(); got != 2 {
		t.Fatalf("non-advancing node pager calls = %d, want 2", got)
	}
	nonAdvancing = &nonAdvancingHistoryPager{}
	if err := c.exportRelHistory(io.Discard, nonAdvancing, types.RelID(1)); err == nil {
		t.Fatal("exportRelHistory accepted a non-advancing history page")
	}
	if got := nonAdvancing.relCalls.Load(); got != 2 {
		t.Fatalf("non-advancing rel pager calls = %d, want 2", got)
	}
}

// TestExportGraph_PaginatedNodesRoundTrip verifies that ExportGraph correctly
// exports more than exportBatchSize (1024) nodes — i.e. pagination is working.
// Before the OOM fix, all IDs were collected into a single slice; this test
// proves the paginated path emits all entities without loss.
func TestExportGraph_PaginatedNodesRoundTrip(t *testing.T) {
	t.Parallel()

	g, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	// Create 1100 nodes — more than exportBatchSize (1024) to exercise pagination.
	const total = 1100
	for i := 0; i < total; i++ {
		if _, err := g.Nodes.Add([]string{"Batch"}, map[string]any{"i": i}); err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
	}

	// Export.
	var buf bytes.Buffer
	if err := g.IO.Export(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Import into a fresh graph and verify all nodes are present.
	g2, err := New(Config{Store: memory.New()})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}
	defer g2.Close() //nolint:errcheck

	if err := g2.IO.Import(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	nc, err := g2.Nodes.Count()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if nc != total {
		t.Errorf("NodeCount = %d, want %d", nc, total)
	}
}
