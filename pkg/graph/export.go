package graph

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"github.com/vmihailenco/msgpack/v5"
)

// Export format version. Increment when the record layout changes in a
// backward-incompatible way.
const exportFormatVersion byte = 1

// exportBatchSize is the page size for paginated entity queries during export.
// Caps per-page allocations to ~80-100 KB for nodes, preventing the OOM
// that would result from collecting all IDs into a single monolithic slice.
const exportBatchSize = 1024

// Record type tags for the export stream. Values ≥ 0x80 are reserved for future use.
const (
	exportTagHeader   byte = 0x01 // exportHeader record
	exportTagRegistry byte = 0x02 // registryFileData record
	exportTagNode     byte = 0x03 // current node (nodeWire)
	exportTagNodeHist byte = 0x04 // node history entry (nodeWire)
	exportTagRel      byte = 0x05 // current relationship (relWire)
	exportTagRelHist  byte = 0x06 // relationship history entry (relWire)
)

// exportHeader is the first record written in the export stream.
type exportHeader struct {
	Version    uint8 `msgpack:"v"`
	ExportedAt int64 `msgpack:"at"`
	NodeCount  int64 `msgpack:"nc"`
	RelCount   int64 `msgpack:"rc"`
}

// ErrIncompatibleExport is returned when the export stream carries an
// unsupported format version.
var ErrIncompatibleExport = errors.New("graph: incompatible export format version")

// ErrIncompatibleRegistry is returned by ImportGraph when the export stream
// carries a label or reltype registry that conflicts with an existing non-empty
// registry whose token mappings differ. Importing with a mismatched registry
// would assign wrong labels/types to all entities without any visible error.
var ErrIncompatibleRegistry = errors.New("graph: imported registry conflicts with existing registry")

// maxExportRecordSize caps the per-record allocation in readExportRecord.
// A node with 1000 max-size properties is ~66 MiB; 128 MiB gives safe headroom.
const maxExportRecordSize = 128 * 1024 * 1024 // 128 MiB

// ExportGraph writes a portable snapshot of the graph to w.
//
// The snapshot includes every current node and relationship, their full version
// history, and the label/reltype registries. The format is a sequence of
// length-prefixed msgpack records, each preceded by a 1-byte type tag and a
// 4-byte big-endian body length. This layout allows forward-compatible streaming
// without loading the whole file into memory.
//
// ExportGraph holds g.mu.RLock for the duration — concurrent Snapshot, Reset,
// and individual Add/Update/Delete mutations are all blocked by g.mu.
func (g *Graph) ExportGraph(w io.Writer) error {
	g.mu.RLock()
	defer g.mu.RUnlock()

	// --- Header ---
	nc, err := g.store.NodeCount()
	if err != nil {
		return fmt.Errorf("export: node count: %w", err)
	}
	rc, err := g.store.RelationshipCount()
	if err != nil {
		return fmt.Errorf("export: rel count: %w", err)
	}
	hdr := exportHeader{
		Version:    exportFormatVersion,
		ExportedAt: time.Now().UnixMilli(),
		NodeCount:  int64(nc),
		RelCount:   int64(rc),
	}
	if err := marshalAndWrite(w, exportTagHeader, &hdr); err != nil {
		return fmt.Errorf("export: header: %w", err)
	}

	// --- Registry ---
	reg := registryFileData{
		Labels:   g.labels.ExportNames(),
		RelTypes: g.relTypes.ExportNames(),
	}
	if err := marshalAndWrite(w, exportTagRegistry, &reg); err != nil {
		return fmt.Errorf("export: registry: %w", err)
	}

	// --- Current nodes (paginated) ---
	// AllNodes with cursor-based pagination caps memory to exportBatchSize entities
	// per iteration. Avoids the OOM that results from collecting all IDs into a
	// single monolithic slice before fetching (8+ GB for 1B nodes).
	var nodeCursor snowflake.ID
	for {
		nodes, err := g.store.AllNodes(QueryOpts{Limit: exportBatchSize, After: nodeCursor})
		if err != nil {
			return fmt.Errorf("export: fetch nodes: %w", err)
		}
		for _, n := range nodes {
			w2 := nodeToWire(n)
			if err := marshalAndWrite(w, exportTagNode, &w2); err != nil {
				return fmt.Errorf("export: write node %d: %w", n.InternalID().SnowflakeID(), err)
			}
			nodeCursor = n.InternalID().SnowflakeID()
		}
		if len(nodes) < exportBatchSize {
			break
		}
	}

	// --- Node history ---
	// AllNodeHistoryIDs does not support cursor pagination (no QueryOpts); the full
	// ID slice is loaded once. The history population is typically much smaller than
	// the live entity population, so the memory impact is acceptable.
	// TODO(v3.1.0): add cursor-based AllNodeHistoryIDs(QueryOpts) to the Store interface
	// and all three implementations (MemoryStore, BadgerStore, TieredStore) to eliminate
	// the OOM risk at large history depths (e.g., 10K nodes × 1K versions = 10M IDs).
	nodeHistIDs, err := g.store.AllNodeHistoryIDs()
	if err != nil {
		return fmt.Errorf("export: node history IDs: %w", err)
	}
	for _, id := range nodeHistIDs {
		history, err := g.store.GetNodeHistory(id)
		if err != nil {
			return fmt.Errorf("export: get node history %d: %w", id, err)
		}
		for _, entry := range history {
			w2 := nodeToWire(entry)
			if err := marshalAndWrite(w, exportTagNodeHist, &w2); err != nil {
				return fmt.Errorf("export: write node history %d v%d: %w", id, entry.Version(), err)
			}
		}
	}

	// --- Current relationships (paginated) ---
	var relCursor snowflake.ID
	for {
		rels, err := g.store.AllRelationships(QueryOpts{Limit: exportBatchSize, After: relCursor})
		if err != nil {
			return fmt.Errorf("export: fetch rels: %w", err)
		}
		for _, r := range rels {
			w2 := relToWire(r)
			if err := marshalAndWrite(w, exportTagRel, &w2); err != nil {
				return fmt.Errorf("export: write rel %d: %w", r.InternalID().SnowflakeID(), err)
			}
			relCursor = r.InternalID().SnowflakeID()
		}
		if len(rels) < exportBatchSize {
			break
		}
	}

	// --- Relationship history ---
	relHistIDs, err := g.store.AllRelHistoryIDs()
	if err != nil {
		return fmt.Errorf("export: rel history IDs: %w", err)
	}
	for _, id := range relHistIDs {
		history, err := g.store.GetRelHistory(id)
		if err != nil {
			return fmt.Errorf("export: get rel history %d: %w", id, err)
		}
		for _, entry := range history {
			w2 := relToWire(entry)
			if err := marshalAndWrite(w, exportTagRelHist, &w2); err != nil {
				return fmt.Errorf("export: write rel history %d v%d: %w", id, entry.Version(), err)
			}
		}
	}

	return nil
}

// importRecord holds one decoded record from the export stream.
type importRecord struct {
	tag  byte
	data []byte
}

// ImportGraph reads a portable graph snapshot from r and restores it into g.
//
// Registries are imported if they are empty; if already populated (e.g., the
// graph was loaded from a prior Badger directory), the existing registry is kept
// and the import continues without error (idempotent registry behaviour).
//
// Two-phase implementation:
//   - Phase 1 (no lock): all records are read from r into a []importRecord buffer.
//     io.Reader I/O can be slow (file, network); holding g.mu.Lock for its duration
//     would block all Add/Update/Query callers for potentially minutes.
//   - Phase 2 (under g.mu.Lock): the buffer is processed — msgpack deserialization
//   - store writes. No I/O under the lock; only CPU + in-memory store ops.
//
// Memory: the entire export is buffered in RAM before the write lock is acquired.
// For large exports (> 1 GB) this may be significant. Users restoring multi-GB
// graphs should use an in-memory=false BadgerStore to reduce working-set pressure.
//
// The caller must ensure that entity IDs in the export do not conflict with
// existing IDs in the graph (typical use: import into a freshly created graph).
func (g *Graph) ImportGraph(r io.Reader) error {
	// --- Phase 1: stream all records without any lock ---
	var records []importRecord
	for {
		tag, data, err := readExportRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // clean end of stream
			}
			return fmt.Errorf("import: read record: %w", err)
		}
		records = append(records, importRecord{tag: tag, data: data})
	}

	// --- Phase 2: process buffered records under write lock ---
	// Only CPU deserialization + in-memory store writes happen here.
	// No io.Reader reads, no network or file I/O under the lock.
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, rec := range records {
		switch rec.tag {
		case exportTagHeader:
			var hdr exportHeader
			if err := msgpack.Unmarshal(rec.data, &hdr); err != nil {
				return fmt.Errorf("import: unmarshal header: %w", err)
			}
			if hdr.Version != exportFormatVersion {
				return fmt.Errorf("%w: got %d, want %d", ErrIncompatibleExport, hdr.Version, exportFormatVersion)
			}

		case exportTagRegistry:
			var reg registryFileData
			if err := msgpack.Unmarshal(rec.data, &reg); err != nil {
				return fmt.Errorf("import: unmarshal registry: %w", err)
			}
			if err := g.labels.ImportNames(reg.Labels); err != nil {
				if !errors.Is(err, ErrRegistryNotEmpty) {
					return fmt.Errorf("import: label registry: %w", err)
				}
				// Existing registry: identical token mapping is safe (idempotent re-import).
				// Different mapping would silently corrupt all imported entity labels.
				if existing := g.labels.ExportNames(); !reflect.DeepEqual(existing, reg.Labels) {
					return fmt.Errorf("import: label registry: %w", ErrIncompatibleRegistry)
				}
			}
			if err := g.relTypes.ImportNames(reg.RelTypes); err != nil {
				if !errors.Is(err, ErrRegistryNotEmpty) {
					return fmt.Errorf("import: reltype registry: %w", err)
				}
				if existing := g.relTypes.ExportNames(); !reflect.DeepEqual(existing, reg.RelTypes) {
					return fmt.Errorf("import: reltype registry: %w", ErrIncompatibleRegistry)
				}
			}

		case exportTagNode:
			var wn nodeWire
			if err := msgpack.Unmarshal(rec.data, &wn); err != nil {
				return fmt.Errorf("import: unmarshal node: %w", err)
			}
			n := wireToNode(wn)
			if err := g.store.PutNode(n); err != nil && !errors.Is(err, ErrNodeExists) {
				return fmt.Errorf("import: put node %d: %w", wn.ID, err)
			}

		case exportTagNodeHist:
			var wn nodeWire
			if err := msgpack.Unmarshal(rec.data, &wn); err != nil {
				return fmt.Errorf("import: unmarshal node history: %w", err)
			}
			n := wireToNode(wn)
			id := snowflake.ID(wn.ID) //nolint:gosec — ID from our own serialization
			if err := g.store.PutNodeVersion(id, n.Version(), n); err != nil {
				return fmt.Errorf("import: put node history %d v%d: %w", wn.ID, n.Version(), err)
			}

		case exportTagRel:
			var wr relWire
			if err := msgpack.Unmarshal(rec.data, &wr); err != nil {
				return fmt.Errorf("import: unmarshal rel: %w", err)
			}
			rel := wireToRel(wr)
			if err := g.store.PutRelationship(rel); err != nil && !errors.Is(err, ErrRelExists) {
				return fmt.Errorf("import: put rel %d: %w", wr.ID, err)
			}

		case exportTagRelHist:
			var wr relWire
			if err := msgpack.Unmarshal(rec.data, &wr); err != nil {
				return fmt.Errorf("import: unmarshal rel history: %w", err)
			}
			rel := wireToRel(wr)
			id := snowflake.ID(wr.ID) //nolint:gosec — ID from our own serialization
			if err := g.store.PutRelVersion(id, rel.Version(), rel); err != nil {
				return fmt.Errorf("import: put rel history %d v%d: %w", wr.ID, rel.Version(), err)
			}

		default:
			// Unknown tag — skip for forward compatibility with newer export versions.
		}
	}

	return nil
}

// marshalAndWrite marshals v to msgpack and writes it as a tagged length-prefixed
// record to w.
func marshalAndWrite(w io.Writer, tag byte, v any) error {
	data, err := msgpack.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return writeExportRecord(w, tag, data)
}

// writeExportRecord writes a 1-byte tag, a 4-byte big-endian body length, and
// the body bytes to w.
func writeExportRecord(w io.Writer, tag byte, data []byte) error {
	var header [5]byte
	header[0] = tag
	binary.BigEndian.PutUint32(header[1:5], uint32(len(data))) // #nosec G115 — len fits in uint32 for any reasonable record
	if _, err := w.Write(header[:]); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// readExportRecord reads one tagged length-prefixed record from r.
// Returns (0, nil, io.EOF) when the stream ends cleanly between records.
// Returns (0, nil, io.ErrUnexpectedEOF) when the stream is truncated mid-record.
func readExportRecord(r io.Reader) (tag byte, data []byte, err error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header[1:5])
	if length > maxExportRecordSize {
		return 0, nil, fmt.Errorf("import: record too large (tag=0x%02x, len=%d, max=%d)", header[0], length, maxExportRecordSize)
	}
	data = make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, fmt.Errorf("record body (tag=0x%02x, len=%d): %w", header[0], length, err)
	}
	return header[0], data, nil
}
