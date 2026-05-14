// Tests in this file pin R4-F11 and R4-F12 from the 2026-05-08
// maintainability review:
//
//   - R4-F11: import replay must reject tokenized entity records that
//     arrive before a header AND a registry have been imported.
//     Pre-fix, an out-of-order stream could install nodes/rels with
//     unresolved label/type tokens, leaving label/type queries unable
//     to find the imported data.
//
//   - R4-F12: import replay must reject duplicate current entities
//     whose content differs from the stream. Pre-fix, the import
//     silently swallowed ErrNodeExists / ErrRelExists, kept the
//     existing current entity, then proceeded to write history records
//     on top of it — producing a hybrid current/history graph that
//     never existed in either source.
package core

import (
	"context"
	"bytes"
	"errors"
	"testing"

	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v4/pkg/graph/io"
)

// R4-F11: a stream that emits an entity record before any registry
// must be rejected with ErrCorruptExport.
func TestR4_Import_NodeBeforeRegistry_Rejected(t *testing.T) {
	t.Parallel()
	src := newTestGraph(t)
	defer src.Close()

	if _, err := src.Nodes.Add(context.Background(), []string{"A"}, nil); err != nil {
		t.Fatal(err)
	}

	// Hand-craft a stream that emits the header and the node record
	// but drops the registry. Easiest path: export normally, then
	// strip the registry record.
	var full bytes.Buffer
	if err := src.IO.Export(&full); err != nil {
		t.Fatalf("Export: %v", err)
	}

	stripped := stripRegistryRecord(t, full.Bytes())

	dst := newTestGraph(t)
	defer dst.Close()
	err := dst.IO.Import(bytes.NewReader(stripped), tkgio.ImportOptions{})
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import without registry: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// R4-F11 follow-on: a registry record before any header must also be
// rejected — the header gates all subsequent records, including the
// registry itself.
func TestR4_Import_RegistryBeforeHeader_Rejected(t *testing.T) {
	t.Parallel()
	src := newTestGraph(t)
	defer src.Close()

	if _, err := src.Nodes.Add(context.Background(), []string{"A"}, nil); err != nil {
		t.Fatal(err)
	}

	var full bytes.Buffer
	if err := src.IO.Export(&full); err != nil {
		t.Fatalf("Export: %v", err)
	}

	stripped := stripHeaderRecord(t, full.Bytes())

	dst := newTestGraph(t)
	defer dst.Close()
	err := dst.IO.Import(bytes.NewReader(stripped), tkgio.ImportOptions{})
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import without header: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// R4-F12: importing into a graph that already has the same node ID
// with DIFFERENT properties must fail with ErrCorruptExport, not
// silently accept and skip.
func TestR4_Import_ConflictingDuplicateNode_Rejected(t *testing.T) {
	t.Parallel()
	// Source graph with node A and a property "v": 1.
	src := newTestGraph(t)
	defer src.Close()
	srcNode, err := src.Nodes.Add(context.Background(), []string{"X"}, map[string]any{"v": int64(1)})
	if err != nil {
		t.Fatal(err)
	}
	id := srcNode.ID()

	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Destination graph with the SAME ID but DIFFERENT property value.
	// Use Import (caller-specified ID) to seed the conflicting state.
	dst := newTestGraph(t)
	defer dst.Close()
	if _, err := dst.Nodes.Import(t.Context(), id, []string{"X"}, map[string]any{"v": int64(2)}); err != nil {
		t.Fatalf("seed conflicting node: %v", err)
	}

	err = dst.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{})
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("Import with conflicting duplicate: got %v, want errors.Is ErrCorruptExport", err)
	}
}

// R4-F12 idempotent counterpart: importing into a graph that has the
// SAME node with the SAME content must succeed — pre-existing graphs
// re-importing their own export remains a supported workflow.
func TestR4_Import_IdenticalDuplicateNode_Allowed(t *testing.T) {
	t.Parallel()
	src := newTestGraph(t)
	defer src.Close()
	if _, err := src.Nodes.Add(context.Background(), []string{"X"}, map[string]any{"v": int64(1)}); err != nil {
		t.Fatal(err)
	}

	var stream bytes.Buffer
	if err := src.IO.Export(&stream); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Re-import into the SAME graph (so the existing node is byte-identical
	// to the one in the stream).
	if err := src.IO.Import(bytes.NewReader(stream.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("idempotent re-import: %v", err)
	}
}

// stripRegistryRecord rewrites the export stream, dropping the
// registry record (tag exportTagRegistry = 0x02). The header (tag
// exportTagHeader = 0x01) and entity records remain.
func stripRegistryRecord(t *testing.T, full []byte) []byte {
	t.Helper()
	return stripRecordsByTag(t, full, exportTagRegistry)
}

func stripHeaderRecord(t *testing.T, full []byte) []byte {
	t.Helper()
	return stripRecordsByTag(t, full, exportTagHeader)
}

// stripRecordsByTag walks the export framing (uint8 tag + uint32
// big-endian length + body) and emits every record whose tag != skip.
func stripRecordsByTag(t *testing.T, full []byte, skip uint8) []byte {
	t.Helper()
	var out bytes.Buffer
	r := bytes.NewReader(full)
	for {
		tag, body, err := readExportRecord(r)
		if err != nil {
			break
		}
		if tag == skip {
			continue
		}
		if werr := writeExportRecord(&out, tag, body); werr != nil {
			t.Fatalf("rewrite stream: %v", werr)
		}
	}
	return out.Bytes()
}
