package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"

	"github.com/vmihailenco/msgpack/v5"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// File layout (R5-F9 split):
//   - export.go — wire format + Export path + framing helpers
//   - import.go — Import path + per-record validators

// Export format version. Increment when the record layout changes in a
// backward-incompatible way.
const exportFormatVersion byte = 1

// exportBatchSize is the page size for paginated entity queries during export.
// Caps per-page allocations to ~80-100 KB for nodes, preventing the OOM
// that would result from collecting all IDs into a single monolithic slice.
const exportBatchSize = 1024

// exportHistoryBatchSize is the page size for cursor-paginated history-ID
// scans during export. The per-ID history payload is unbounded (deeply
// versioned nodes carry many entries), so we keep the cursor page narrow:
// at most ~32 KiB of types.NodeID/types.RelID values resident at once.
const exportHistoryBatchSize = 4096

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

// IO sentinels — canonical declarations live in pkg/graph/io so external
// callers can use `errors.Is(err, tkgio.ErrIncompatibleExport)` without
// importing internal/core. Aliasing here keeps internal references on
// the short identifier while the exported identity sits in the public
// package (R4-F8).
var (
	ErrIncompatibleExport   = tkgio.ErrIncompatibleExport
	ErrIncompatibleRegistry = tkgio.ErrIncompatibleRegistry
	ErrCorruptExport        = tkgio.ErrCorruptExport
	ErrNilWriter            = tkgio.ErrNilWriter
)

// maxExportRecordSize caps the per-record allocation in readExportRecord.
// A node with 1000 max-size properties is ~66 MiB; 128 MiB gives safe headroom.
const maxExportRecordSize = 128 * 1024 * 1024 // 128 MiB

// Export writes a portable snapshot of the graph to w under c.mu.Lock.
//
// The snapshot includes every current node and relationship, their full version
// history, and the label/reltype registries. The format is a sequence of
// length-prefixed msgpack records, each preceded by a 1-byte type tag and a
// 4-byte big-endian body length. This layout allows forward-compatible streaming
// without loading the whole file into memory.
//
// The write lock excludes standalone mutations, tx/batch, and Reset for
// the duration of the streamed snapshot. Code that is already inside a
// transaction must call (*GraphTx).Export instead of this method because
// sync.RWMutex is not reentrant.
func (o *IOOps) Export(w io.Writer) error {
	c := o.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if isNilInterfaceValue(w) {
		return ErrNilWriter
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	return c.exportLocked(w)
}

// exportLocked writes the snapshot. Caller must hold c.mu.Lock.
// exportLocked itself takes no graph-level locks.
func (c *Core) exportLocked(w io.Writer) error {
	if isNilInterfaceValue(w) {
		return ErrNilWriter
	}
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
			w2, err := storeutil.NodeToWireChecked(n)
			if err != nil {
				return fmt.Errorf("export: encode node %d: %w", n.ID().SnowflakeID(), err)
			}
			if err := marshalAndWrite(w, exportTagNode, &w2); err != nil {
				return fmt.Errorf("export: write node %d: %w", n.ID().SnowflakeID(), err)
			}
			nodeCursor = types.EntityID(n.ID().SnowflakeID())
		}
		if len(nodes) < exportBatchSize {
			break
		}
	}

	// --- Node history (paginated) ---
	// AllNodeHistoryIDsFrom caps memory to exportHistoryBatchSize IDs per call,
	// eliminating the OOM risk at large history depths (e.g., 10K nodes × 1K
	// versions = 10M IDs). Each iteration loads at most batch-size IDs plus
	// the per-ID history list, never the entire history-ID set.
	{
		var nodeHistCursor types.NodeID
		for {
			nodeHistIDs, err := c.store.AllNodeHistoryIDsFrom(nodeHistCursor, exportHistoryBatchSize)
			if err != nil {
				return fmt.Errorf("export: node history IDs: %w", err)
			}
			if len(nodeHistIDs) == 0 {
				break
			}
			for _, id := range nodeHistIDs {
				history, err := c.store.GetNodeHistory(id)
				if err != nil {
					return fmt.Errorf("export: get node history %d: %w", id, err)
				}
				for _, entry := range history {
					w2, err := storeutil.NodeToWireChecked(entry)
					if err != nil {
						return fmt.Errorf("export: encode node history %d v%d: %w", id, entry.Version(), err)
					}
					if err := marshalAndWrite(w, exportTagNodeHist, &w2); err != nil {
						return fmt.Errorf("export: write node history %d v%d: %w", id, entry.Version(), err)
					}
				}
			}
			if len(nodeHistIDs) < exportHistoryBatchSize {
				break
			}
			nodeHistCursor = nodeHistIDs[len(nodeHistIDs)-1]
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
			w2, err := storeutil.RelToWireChecked(r)
			if err != nil {
				return fmt.Errorf("export: encode rel %d: %w", r.ID().SnowflakeID(), err)
			}
			if err := marshalAndWrite(w, exportTagRel, &w2); err != nil {
				return fmt.Errorf("export: write rel %d: %w", r.ID().SnowflakeID(), err)
			}
			relCursor = types.EntityID(r.ID().SnowflakeID())
		}
		if len(rels) < exportBatchSize {
			break
		}
	}

	// --- Relationship history (paginated) ---
	{
		var relHistCursor types.RelID
		for {
			relHistIDs, err := c.store.AllRelHistoryIDsFrom(relHistCursor, exportHistoryBatchSize)
			if err != nil {
				return fmt.Errorf("export: rel history IDs: %w", err)
			}
			if len(relHistIDs) == 0 {
				break
			}
			for _, id := range relHistIDs {
				history, err := c.store.GetRelHistory(id)
				if err != nil {
					return fmt.Errorf("export: get rel history %d: %w", id, err)
				}
				for _, entry := range history {
					w2, err := storeutil.RelToWireChecked(entry)
					if err != nil {
						return fmt.Errorf("export: encode rel history %d v%d: %w", id, entry.Version(), err)
					}
					if err := marshalAndWrite(w, exportTagRelHist, &w2); err != nil {
						return fmt.Errorf("export: write rel history %d v%d: %w", id, entry.Version(), err)
					}
				}
			}
			if len(relHistIDs) < exportHistoryBatchSize {
				break
			}
			relHistCursor = relHistIDs[len(relHistIDs)-1]
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

func readExportRecordBytes(src []byte, offset *int) (tag byte, data []byte, err error) {
	if *offset == len(src) {
		return 0, nil, io.EOF
	}
	if len(src)-*offset < 5 {
		return 0, nil, io.ErrUnexpectedEOF
	}
	header := src[*offset : *offset+5]
	length := binary.BigEndian.Uint32(header[1:5])
	if length > maxExportRecordSize {
		return 0, nil, fmt.Errorf("import: record too large (tag=0x%02x, len=%d, max=%d)", header[0], length, maxExportRecordSize)
	}
	*offset += 5
	if uint64(len(src)-*offset) < uint64(length) {
		return 0, nil, fmt.Errorf("record body (tag=0x%02x, len=%d): %w", header[0], length, io.ErrUnexpectedEOF)
	}
	bodyStart := *offset
	bodyEnd := bodyStart + int(length)
	*offset = bodyEnd
	return header[0], src[bodyStart:bodyEnd], nil
}
