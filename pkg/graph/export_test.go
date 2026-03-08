package graph_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// buildExportGraph creates a graph with nodes, rels, and history.
// Returns the graph, a node ID and rel ID for later assertions.
func buildExportGraph(t *testing.T) (g *graph.Graph, nodeID, relID uint64) {
	t.Helper()
	g, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	start, err := g.AddNode([]string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	end, err := g.AddNode([]string{"City"}, map[string]any{"city": "Vienna"})
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	r, err := g.AddRelationship("LIVES_IN", start, end, map[string]any{"since": int64(2020)})
	if err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	// Create a version history entry on the start node.
	_, err = g.UpdateNode(start.InternalID().SnowflakeID(), map[string]any{"name": "Alice Updated"})
	if err != nil {
		t.Fatalf("UpdateNode: %v", err)
	}

	return g, uint64(start.InternalID().SnowflakeID()), uint64(r.InternalID().SnowflakeID())
}

// TestExportImport_RoundTrip_MemoryStore tests a full export→import roundtrip
// using MemoryStore on both sides.
func TestExportImport_RoundTrip_MemoryStore(t *testing.T) {
	src, _, _ := buildExportGraph(t)
	defer src.Close() //nolint:errcheck

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Import into a fresh graph.
	dst, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	// Verify node count.
	srcNC, _ := src.NodeCount()
	dstNC, _ := dst.NodeCount()
	if dstNC != srcNC {
		t.Errorf("NodeCount: src=%d, dst=%d", srcNC, dstNC)
	}

	// Verify rel count.
	srcRC, _ := src.RelationshipCount()
	dstRC, _ := dst.RelationshipCount()
	if dstRC != srcRC {
		t.Errorf("RelCount: src=%d, dst=%d", srcRC, dstRC)
	}

	// Verify nodes by label.
	srcPersons, _ := src.NodesByLabel("Person", graph.QueryOpts{})
	dstPersons, _ := dst.NodesByLabel("Person", graph.QueryOpts{})
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
	g, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer g.Close() //nolint:errcheck

	var buf bytes.Buffer
	if err := g.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph empty: %v", err)
	}
	if buf.Len() == 0 {
		t.Error("expected non-empty output even for empty graph (header + registry)")
	}

	// Import into a fresh graph.
	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph empty: %v", err)
	}
	nc, _ := dst.NodeCount()
	if nc != 0 {
		t.Errorf("NodeCount after empty import = %d; want 0", nc)
	}
}

// TestExport_WithNodeHistory verifies that node version history survives the roundtrip.
func TestExport_WithNodeHistory(t *testing.T) {
	g, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer g.Close() //nolint:errcheck

	n, _ := g.AddNode([]string{"Item"}, map[string]any{"v": int64(1)})
	id := n.InternalID().SnowflakeID()
	_, _ = g.UpdateNode(id, map[string]any{"v": int64(2)})
	_, _ = g.UpdateNode(id, map[string]any{"v": int64(3)})

	srcHistory, _ := g.GetNodeHistory(id)

	var buf bytes.Buffer
	if err := g.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	dstHistory, err := dst.GetNodeHistory(id)
	if err != nil {
		t.Fatalf("GetNodeHistory on dst: %v", err)
	}
	if len(dstHistory) != len(srcHistory) {
		t.Errorf("history length: src=%d, dst=%d", len(srcHistory), len(dstHistory))
	}
}

// TestExport_RelHistory verifies that relationship version history survives the roundtrip.
func TestExport_RelHistory(t *testing.T) {
	g, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer g.Close() //nolint:errcheck

	a, _ := g.AddNode([]string{"A"}, nil)
	b, _ := g.AddNode([]string{"B"}, nil)
	r, _ := g.AddRelationship("EDGE", a, b, map[string]any{"w": int64(1)})
	rid := r.InternalID().SnowflakeID()
	_, _ = g.UpdateRelationship(rid, map[string]any{"w": int64(2)})

	srcHistory, _ := g.GetRelHistory(rid)

	var buf bytes.Buffer
	if err := g.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	dstHistory, err := dst.GetRelHistory(rid)
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
	src, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer src.Close() //nolint:errcheck

	src.AddNode([]string{"Foo"}, nil) //nolint:errcheck

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Destination already has the "Foo" label registered (from a prior node add).
	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close()                 //nolint:errcheck
	dst.AddNode([]string{"Foo"}, nil) //nolint:errcheck

	// Importing with a pre-populated registry must not fail.
	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph with existing registry: %v", err)
	}
}

// TestExport_Writer_Error verifies that ExportGraph propagates a write error.
func TestExport_Writer_Error(t *testing.T) {
	g, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer g.Close()               //nolint:errcheck
	g.AddNode([]string{"X"}, nil) //nolint:errcheck

	// errWriter fails after 0 bytes written.
	ew := &errWriter{failAfter: 0}
	err := g.ExportGraph(ew)
	if err == nil {
		t.Error("expected error from ExportGraph when writer fails")
	}
}

// TestImport_InvalidHeader verifies that ImportGraph returns ErrIncompatibleExport
// when the export header carries an unsupported version.
func TestImport_InvalidHeader(t *testing.T) {
	// Build a valid export.
	src, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer src.Close() //nolint:errcheck

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
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

	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	err := dst.ImportGraph(badBuf)
	if !errors.Is(err, graph.ErrIncompatibleExport) {
		t.Errorf("expected ErrIncompatibleExport, got: %v", err)
	}
	_ = data // suppress unused warning
}

// TestExportImport_IntegrityPreserved verifies that node/rel hash chains survive
// the export→import roundtrip.
func TestExportImport_IntegrityPreserved(t *testing.T) {
	src, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer src.Close() //nolint:errcheck

	a, _ := src.AddNode([]string{"Node"}, map[string]any{"v": int64(1)})
	b, _ := src.AddNode([]string{"Node"}, map[string]any{"v": int64(2)})
	r, _ := src.AddRelationship("EDGE", a, b, nil)

	srcAHash := a.Integrity().Hash
	srcRHash := r.Integrity().Hash

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	// Verify imported node hash is preserved.
	importedA, err := dst.GetNode(a.InternalID().SnowflakeID())
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
	importedR, err := dst.GetRelationship(r.InternalID().SnowflakeID())
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
	src, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer src.Close() //nolint:errcheck

	a, _ := src.AddNode([]string{"A"}, nil)
	b, _ := src.AddNode([]string{"B"}, nil)
	r, _ := src.AddRelationship("R", a, b, nil)

	srcFrom := r.Integrity().FromNodeHash
	srcTo := r.Integrity().ToNodeHash

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	importedR, err := dst.GetRelationship(r.InternalID().SnowflakeID())
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
	src, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer src.Close() //nolint:errcheck

	n, _ := src.AddNode([]string{"Doc"}, map[string]any{
		"tkg_author_id": "author@example.com",
		"tkg_signature": []byte("test-sig"),
	})

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	imported, err := dst.GetNode(n.InternalID().SnowflakeID())
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
	src, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer src.Close() //nolint:errcheck

	n, _ := src.AddNode([]string{"X"}, map[string]any{"k": int64(7)})
	srcHash, _ := src.ResolveNodeProperty(n, types.ShadowHash)

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	dst, _ := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	defer dst.Close() //nolint:errcheck

	if err := dst.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	imported, _ := dst.GetNode(n.InternalID().SnowflakeID())
	dstHash, _ := dst.ResolveNodeProperty(imported, types.ShadowHash)
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
	src, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	start, _ := src.AddNode([]string{"Person"}, nil)
	end, _ := src.AddNode([]string{"City"}, nil)
	_, _ = src.AddRelationship("LIVES_IN", start, end, nil)

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Destination has a DIFFERENT label at token 1 — "Company" instead of "Person".
	dst, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck
	_, _ = dst.AddNode([]string{"Company"}, nil)

	err = dst.ImportGraph(&buf)
	if !errors.Is(err, graph.ErrIncompatibleRegistry) {
		t.Errorf("ImportGraph: got %v, want ErrIncompatibleRegistry", err)
	}
}

// TestImportGraph_CompatibleRegistryIdempotent verifies that importing the same
// export twice into the same graph succeeds — the second import detects a
// non-empty but identical registry and continues without error.
func TestImportGraph_CompatibleRegistryIdempotent(t *testing.T) {
	src, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New src: %v", err)
	}
	defer src.Close() //nolint:errcheck

	a, _ := src.AddNode([]string{"Alpha"}, map[string]any{"x": int64(1)})
	b, _ := src.AddNode([]string{"Beta"}, nil)
	_, _ = src.AddRelationship("LINK", a, b, nil)

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}
	exportedBytes := buf.Bytes()

	dst, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New dst: %v", err)
	}
	defer dst.Close() //nolint:errcheck

	// First import — fresh graph, registries empty.
	if err := dst.ImportGraph(bytes.NewReader(exportedBytes)); err != nil {
		t.Fatalf("first ImportGraph: %v", err)
	}

	// Second import — same export bytes, registry now populated with identical mapping.
	if err := dst.ImportGraph(bytes.NewReader(exportedBytes)); err != nil {
		t.Fatalf("second ImportGraph (idempotent): %v", err)
	}

	// Node count should be unchanged (re-import skips existing nodes).
	nc, _ := dst.NodeCount()
	srcNC, _ := src.NodeCount()
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

	g, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	err = g.ImportGraph(&buf)
	if err == nil {
		t.Fatal("ImportGraph: expected error for oversize record, got nil")
	}
	// Must NOT be io.ErrUnexpectedEOF — that would mean the guard was absent
	// and the body read was attempted (which would have caused OOM first).
	if errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("ImportGraph returned ErrUnexpectedEOF — size guard was not applied before body read")
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

// TestExportGraph_PaginatedNodesRoundTrip verifies that ExportGraph correctly
// exports more than exportBatchSize (1024) nodes — i.e. pagination is working.
// Before the OOM fix, all IDs were collected into a single slice; this test
// proves the paginated path emits all entities without loss.
func TestExportGraph_PaginatedNodesRoundTrip(t *testing.T) {
	t.Parallel()

	g, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	// Create 1100 nodes — more than exportBatchSize (1024) to exercise pagination.
	const total = 1100
	for i := 0; i < total; i++ {
		if _, err := g.AddNode([]string{"Batch"}, map[string]any{"i": i}); err != nil {
			t.Fatalf("AddNode %d: %v", i, err)
		}
	}

	// Export.
	var buf bytes.Buffer
	if err := g.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Import into a fresh graph and verify all nodes are present.
	g2, err := graph.New(graph.Config{Store: graph.NewMemoryStore()})
	if err != nil {
		t.Fatalf("New g2: %v", err)
	}
	defer g2.Close() //nolint:errcheck

	if err := g2.ImportGraph(&buf); err != nil {
		t.Fatalf("ImportGraph: %v", err)
	}

	nc, err := g2.NodeCount()
	if err != nil {
		t.Fatalf("NodeCount: %v", err)
	}
	if nc != total {
		t.Errorf("NodeCount = %d, want %d", nc, total)
	}
}
