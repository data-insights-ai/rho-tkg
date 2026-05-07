package core

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"time"

	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"github.com/vmihailenco/msgpack/v5"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
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
	exportTagRegistry byte = 0x02 // tiered.RegistryFileData record
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

// ErrCorruptExport is returned by ImportGraph when an export record contains
// structurally invalid data — e.g. a node with primary-label token 0 (reserved)
// or a relationship with type token 0. ImportGraph reads from an arbitrary
// io.Reader (untrusted boundary); validation must surface bad records as
// typed errors rather than letting types.NewNode / types.NewRelationship panic
// downstream.
var ErrCorruptExport = errors.New("graph: corrupt export record")

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
// ExportGraph holds c.mu.RLock for the duration — concurrent Snapshot, Reset,
// and individual Add/Update/Delete mutations are all blocked by c.mu.
func (c *Core) ExportGraph(w io.Writer) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	// --- Header ---
	nc, err := c.store.NodeCount()
	if err != nil {
		return fmt.Errorf("export: node count: %w", err)
	}
	rc, err := c.store.RelationshipCount()
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
	reg := tiered.RegistryFileData{
		Labels:   c.labels.ExportNames(),
		RelTypes: c.relTypes.ExportNames(),
	}
	if err := marshalAndWrite(w, exportTagRegistry, &reg); err != nil {
		return fmt.Errorf("export: registry: %w", err)
	}

	// --- Current nodes (paginated) ---
	// AllNodes with cursor-based pagination caps memory to exportBatchSize entities
	// per iteration. Avoids the OOM that results from collecting all IDs into a
	// single monolithic slice before fetching (8+ GB for 1B nodes).
	var nodeCursor types.EntityID
	for {
		nodes, err := c.store.AllNodes(storepkg.QueryOpts{Limit: exportBatchSize, After: nodeCursor})
		if err != nil {
			return fmt.Errorf("export: fetch nodes: %w", err)
		}
		for _, n := range nodes {
			w2 := storeutil.NodeToWire(n)
			if err := marshalAndWrite(w, exportTagNode, &w2); err != nil {
				return fmt.Errorf("export: write node %d: %w", n.ID().SnowflakeID(), err)
			}
			nodeCursor = types.EntityID(n.ID().SnowflakeID())
		}
		if len(nodes) < exportBatchSize {
			break
		}
	}

	// --- Node history ---
	// AllNodeHistoryIDs does not support cursor pagination (no storepkg.QueryOpts); the full
	// ID slice is loaded once. The history population is typically much smaller than
	// the live entity population, so the memory impact is acceptable.
	// TODO(v3.1.0): add cursor-based AllNodeHistoryIDs(storepkg.QueryOpts) to the Store interface
	// and all three implementations (MemoryStore, BadgerStore, tiered.Store) to eliminate
	// the OOM risk at large history depths (e.g., 10K nodes × 1K versions = 10M IDs).
	nodeHistIDs, err := c.store.AllNodeHistoryIDs()
	if err != nil {
		return fmt.Errorf("export: node history IDs: %w", err)
	}
	for _, id := range nodeHistIDs {
		history, err := c.store.GetNodeHistory(id)
		if err != nil {
			return fmt.Errorf("export: get node history %d: %w", id, err)
		}
		for _, entry := range history {
			w2 := storeutil.NodeToWire(entry)
			if err := marshalAndWrite(w, exportTagNodeHist, &w2); err != nil {
				return fmt.Errorf("export: write node history %d v%d: %w", id, entry.Version(), err)
			}
		}
	}

	// --- Current relationships (paginated) ---
	var relCursor types.EntityID
	for {
		rels, err := c.store.AllRelationships(storepkg.QueryOpts{Limit: exportBatchSize, After: relCursor})
		if err != nil {
			return fmt.Errorf("export: fetch rels: %w", err)
		}
		for _, r := range rels {
			w2 := storeutil.RelToWire(r)
			if err := marshalAndWrite(w, exportTagRel, &w2); err != nil {
				return fmt.Errorf("export: write rel %d: %w", r.ID().SnowflakeID(), err)
			}
			relCursor = types.EntityID(r.ID().SnowflakeID())
		}
		if len(rels) < exportBatchSize {
			break
		}
	}

	// --- Relationship history ---
	relHistIDs, err := c.store.AllRelHistoryIDs()
	if err != nil {
		return fmt.Errorf("export: rel history IDs: %w", err)
	}
	for _, id := range relHistIDs {
		history, err := c.store.GetRelHistory(id)
		if err != nil {
			return fmt.Errorf("export: get rel history %d: %w", id, err)
		}
		for _, entry := range history {
			w2 := storeutil.RelToWire(entry)
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

// ImportGraph reads a portable graph snapshot from r and restores it into c.
//
// Registries are imported if they are empty; if already populated (e.g., the
// graph was loaded from a prior Badger directory), the existing registry is kept
// and the import continues without error (idempotent registry behaviour).
//
// Two-phase implementation:
//   - Phase 1 (no lock): all records are read from r into a []importRecord buffer.
//     io.Reader I/O can be slow (file, network); holding c.mu.Lock for its duration
//     would block all Add/Update/Query callers for potentially minutes.
//   - Phase 2 (under c.mu.Lock): the buffer is processed — msgpack deserialization
//   - store writes. No I/O under the lock; only CPU + in-memory store ops.
//
// Memory: the entire export is buffered in RAM before the write lock is acquired.
// For large exports (> 1 GB) this may be significant. Users restoring multi-GB
// graphs should use an in-memory=false BadgerStore to reduce working-set pressure.
//
// The caller must ensure that entity IDs in the export do not conflict with
// existing IDs in the graph (typical use: import into a freshly created graph).
func (c *Core) ImportGraph(r io.Reader) error {
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
	c.mu.Lock()
	defer c.mu.Unlock()

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
			var reg tiered.RegistryFileData
			if err := msgpack.Unmarshal(rec.data, &reg); err != nil {
				return fmt.Errorf("import: unmarshal registry: %w", err)
			}
			if err := c.labels.ImportNames(reg.Labels); err != nil {
				if !errors.Is(err, ErrRegistryNotEmpty) {
					return fmt.Errorf("import: label registry: %w", err)
				}
				// Existing registry: identical token mapping is safe (idempotent re-import).
				// Different mapping would silently corrupt all imported entity labels.
				if existing := c.labels.ExportNames(); !reflect.DeepEqual(existing, reg.Labels) {
					return fmt.Errorf("import: label registry: %w", ErrIncompatibleRegistry)
				}
			}
			if err := c.relTypes.ImportNames(reg.RelTypes); err != nil {
				if !errors.Is(err, ErrRegistryNotEmpty) {
					return fmt.Errorf("import: reltype registry: %w", err)
				}
				if existing := c.relTypes.ExportNames(); !reflect.DeepEqual(existing, reg.RelTypes) {
					return fmt.Errorf("import: reltype registry: %w", ErrIncompatibleRegistry)
				}
			}

		case exportTagNode:
			var wn storeutil.NodeWire
			if err := msgpack.Unmarshal(rec.data, &wn); err != nil {
				return fmt.Errorf("import: unmarshal node: %w", err)
			}
			if err := validateNodeWire(&wn); err != nil {
				return fmt.Errorf("import: node %d: %w", wn.ID, err)
			}
			n := storeutil.WireToNode(wn)
			if err := c.store.PutNode(n); err != nil && !errors.Is(err, storepkg.ErrNodeExists) {
				return fmt.Errorf("import: put node %d: %w", wn.ID, err)
			}

		case exportTagNodeHist:
			var wn storeutil.NodeWire
			if err := msgpack.Unmarshal(rec.data, &wn); err != nil {
				return fmt.Errorf("import: unmarshal node history: %w", err)
			}
			if err := validateNodeWire(&wn); err != nil {
				return fmt.Errorf("import: node history %d: %w", wn.ID, err)
			}
			n := storeutil.WireToNode(wn)
			id := types.NodeID(wn.ID) //nolint:gosec — ID from our own serialization
			if err := c.store.PutNodeVersion(id, n.Version(), n); err != nil {
				return fmt.Errorf("import: put node history %d v%d: %w", wn.ID, n.Version(), err)
			}

		case exportTagRel:
			var wr storeutil.RelWire
			if err := msgpack.Unmarshal(rec.data, &wr); err != nil {
				return fmt.Errorf("import: unmarshal rel: %w", err)
			}
			if err := validateRelWire(&wr); err != nil {
				return fmt.Errorf("import: rel %d: %w", wr.ID, err)
			}
			rel := storeutil.WireToRel(wr)
			if err := c.store.PutRelationship(rel); err != nil && !errors.Is(err, storepkg.ErrRelExists) {
				return fmt.Errorf("import: put rel %d: %w", wr.ID, err)
			}

		case exportTagRelHist:
			var wr storeutil.RelWire
			if err := msgpack.Unmarshal(rec.data, &wr); err != nil {
				return fmt.Errorf("import: unmarshal rel history: %w", err)
			}
			if err := validateRelWire(&wr); err != nil {
				return fmt.Errorf("import: rel history %d: %w", wr.ID, err)
			}
			rel := storeutil.WireToRel(wr)
			id := types.RelID(wr.ID) //nolint:gosec — ID from our own serialization
			if err := c.store.PutRelVersion(id, rel.Version(), rel); err != nil {
				return fmt.Errorf("import: put rel history %d v%d: %w", wr.ID, rel.Version(), err)
			}

		default:
			// Unknown tag — skip for forward compatibility with newer export versions.
		}
	}

	return nil
}

// validateNodeWire defends the import boundary against malformed node records.
// types.NewNode panics on token 0 (primary or extra) — turning a corrupt or
// malicious export into a process crash. ImportGraph reads from an arbitrary
// io.Reader (untrusted input), so we validate before constructing.
//
// Returns ErrCorruptExport (wrapped with detail) on any structural violation:
//   - PrimaryLabel == 0 (token 0 is reserved)
//   - PrimaryLabel outside [1, 65535] (does not fit a uint16 token)
//   - any ExtraLabels element == 0 or outside [1, 65535]
//
// Anything else (id, version, properties, temporal, integrity) is bounded
// by the wire layer's type system at deserialize time and is NOT
// re-validated here. The validators only establish *panic safety* — they
// do not prove semantic correctness of all fields. A negative
// BaseEntityID, a version with no temporal anchor, or a non-snowflake ID
// will round-trip without error and may surface as a logical
// inconsistency later (e.g. failed hash chain verification, lookup
// misses). Treat ImportGraph as "won't crash on a hostile reader, but
// post-import audits are still the caller's responsibility."
func validateNodeWire(w *storeutil.NodeWire) error {
	if w.PrimaryLabel == 0 {
		return fmt.Errorf("%w: primary label token 0 is reserved", ErrCorruptExport)
	}
	if w.PrimaryLabel < 0 || w.PrimaryLabel > 65535 {
		return fmt.Errorf("%w: primary label token %d out of uint16 range", ErrCorruptExport, w.PrimaryLabel)
	}
	for i, t := range w.ExtraLabels {
		if t == 0 {
			return fmt.Errorf("%w: extra label[%d] token 0 is reserved", ErrCorruptExport, i)
		}
		if t < 0 || t > 65535 {
			return fmt.Errorf("%w: extra label[%d] token %d out of uint16 range", ErrCorruptExport, i, t)
		}
	}
	return nil
}

// validateRelWire defends the import boundary against malformed relationship
// records. types.NewRelationship panics on relType 0; surface a typed error
// instead.
func validateRelWire(w *storeutil.RelWire) error {
	if w.RelType == 0 {
		return fmt.Errorf("%w: rel type token 0 is reserved", ErrCorruptExport)
	}
	if w.RelType < 0 || w.RelType > 65535 {
		return fmt.Errorf("%w: rel type token %d out of uint16 range", ErrCorruptExport, w.RelType)
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
