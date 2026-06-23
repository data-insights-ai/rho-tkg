package core

import (
	"encoding/binary"
	"fmt"
	"io"
	"time"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// File layout (R5-F9 split):
//   - export.go — wire format + Export path + framing helpers
//   - import.go — Import path + per-record validators

// Export format version. Increment when the record layout changes in a
// backward-incompatible way. v2 adds exportHeader.SnapshotLSN (the change-log
// LSN captured atomically with the snapshot, for a gapless replica handoff);
// importers accept BOTH v1 (no SnapshotLSN) and v2.
const exportFormatVersion byte = 2

// exportFormatVersionMin is the oldest export-stream version this build can
// import. v1 snapshots (pre-SnapshotLSN) remain readable.
const exportFormatVersionMin byte = 1

// exportBatchSize is the page size for paginated entity queries during export.
// Caps per-page allocations to ~80-100 KB for nodes, preventing the OOM
// that would result from collecting all IDs into a single monolithic slice.
const exportBatchSize = 1024

// exportHistoryBatchSize is the page size for cursor-paginated history-ID
// scans during export. exportHistoryVersionBatchSize caps the number of
// per-entity history versions materialized when the backend supports paged
// history reads.
const exportHistoryBatchSize = 4096
const exportHistoryVersionBatchSize = 4096

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
	// SnapshotLSN is the change-log LSN captured under the SAME c.mu.Lock as the
	// entity snapshot (gapless — no mutation can interleave), so a bootstrapping
	// replica records it as its initial applied watermark and tails from there
	// without re-applying or gapping. Zero when the source had no change-log
	// (omitted on the wire; absent in v1 snapshots).
	SnapshotLSN uint64 `msgpack:"lsn,omitempty"`
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
	nc, err := c.nodeCount()
	if err != nil {
		return fmt.Errorf("export: node count: %w", err)
	}
	rc, err := c.relCount()
	if err != nil {
		return fmt.Errorf("export: rel count: %w", err)
	}
	hdr := exportHeader{
		Version:    exportFormatVersion,
		ExportedAt: time.Now().UnixMilli(),
		NodeCount:  int64(nc),
		RelCount:   int64(rc),
	}
	// Capture the change-log LSN UNDER the held c.mu.Lock, atomically with the
	// entity snapshot above — so the SnapshotLSN exactly bounds what this export
	// contains and a replica can tail from it gaplessly. Nil change-feed (no
	// change-log) leaves it 0 (omitted on the wire).
	if c.changeFeed != nil {
		lsn, lerr := c.changeFeed.LastCommittedLSN()
		if lerr != nil {
			return fmt.Errorf("export: change-log LSN: %w", lerr)
		}
		hdr.SnapshotLSN = lsn
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
	// Page IDs, then fetch one entity at a time. Paging full entities would
	// still retain exportBatchSize deep-copied payloads, which is too much for
	// large property-heavy graphs.
	var nodeCursor types.EntityID
	for {
		nodeIDs, err := c.allNodeIDs(storepkg.QueryOpts{Limit: exportBatchSize, After: nodeCursor})
		if err != nil {
			return fmt.Errorf("export: fetch node IDs: %w", err)
		}
		for _, id := range nodeIDs {
			n, err := c.getCurrentNode(id)
			if err != nil {
				return fmt.Errorf("export: get node %d: %w", id.SnowflakeID(), err)
			}
			w2, err := storeutil.NodeToWireChecked(n)
			if err != nil {
				return fmt.Errorf("export: encode node %d: %w", n.ID().SnowflakeID(), err)
			}
			if err := marshalAndWrite(w, exportTagNode, &w2); err != nil {
				return fmt.Errorf("export: write node %d: %w", n.ID().SnowflakeID(), err)
			}
			nodeCursor = types.EntityID(id.SnowflakeID())
		}
		if len(nodeIDs) < exportBatchSize {
			break
		}
	}

	// --- Node history (paginated) ---
	// AllNodeHistoryIDsFrom caps the ID set. Backends with
	// HistoryVersionPageCapability also cap each individual entity's version
	// slice so one deeply updated node cannot dominate export memory.
	{
		historyPager := historyVersionPageCapability(c.store)
		var nodeHistCursor types.NodeID
		for {
			nodeHistIDs, err := c.allNodeHistoryIDsFrom(nodeHistCursor, exportHistoryBatchSize)
			if err != nil {
				return fmt.Errorf("export: node history IDs: %w", err)
			}
			if len(nodeHistIDs) == 0 {
				break
			}
			for _, id := range nodeHistIDs {
				if err := c.exportNodeHistory(w, historyPager, id); err != nil {
					return err
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
		relIDs, err := c.allRelIDs(storepkg.QueryOpts{Limit: exportBatchSize, After: relCursor})
		if err != nil {
			return fmt.Errorf("export: fetch rel IDs: %w", err)
		}
		for _, id := range relIDs {
			r, err := c.getCurrentRelationship(id)
			if err != nil {
				return fmt.Errorf("export: get rel %d: %w", id.SnowflakeID(), err)
			}
			w2, err := storeutil.RelToWireChecked(r)
			if err != nil {
				return fmt.Errorf("export: encode rel %d: %w", r.ID().SnowflakeID(), err)
			}
			if err := marshalAndWrite(w, exportTagRel, &w2); err != nil {
				return fmt.Errorf("export: write rel %d: %w", r.ID().SnowflakeID(), err)
			}
			relCursor = types.EntityID(id.SnowflakeID())
		}
		if len(relIDs) < exportBatchSize {
			break
		}
	}

	// --- Relationship history (paginated) ---
	{
		historyPager := historyVersionPageCapability(c.store)
		var relHistCursor types.RelID
		for {
			relHistIDs, err := c.allRelHistoryIDsFrom(relHistCursor, exportHistoryBatchSize)
			if err != nil {
				return fmt.Errorf("export: rel history IDs: %w", err)
			}
			if len(relHistIDs) == 0 {
				break
			}
			for _, id := range relHistIDs {
				if err := c.exportRelHistory(w, historyPager, id); err != nil {
					return err
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

func (c *Core) exportNodeHistory(w io.Writer, pager storepkg.HistoryVersionPageCapability, id types.NodeID) error {
	if pager == nil {
		history, err := c.getNodeHistory(id)
		if err != nil {
			return fmt.Errorf("export: get node history %d: %w", id, err)
		}
		return writeNodeHistoryEntries(w, id, history)
	}

	var startVersion uint32
	for {
		history, err := c.nodeHistoryVersionsFrom(pager, id, startVersion, exportHistoryVersionBatchSize)
		if err != nil {
			return fmt.Errorf("export: get node history %d from v%d: %w", id, startVersion, err)
		}
		if len(history) == 0 {
			return nil
		}
		if err := writeNodeHistoryEntries(w, id, history); err != nil {
			return err
		}
		if len(history) < exportHistoryVersionBatchSize {
			return nil
		}
		lastVersion := history[len(history)-1].Version()
		if lastVersion < startVersion {
			return fmt.Errorf("export: node history %d returned non-advancing page at v%d", id, startVersion)
		}
		if lastVersion == ^uint32(0) {
			return nil
		}
		startVersion = lastVersion + 1
	}
}

func writeNodeHistoryEntries(w io.Writer, id types.NodeID, history []*types.Node) error {
	for i, entry := range history {
		if entry == nil {
			return fmt.Errorf("export: encode node history %d entry %d: %w", id, i, ErrNilNode)
		}
		w2, err := storeutil.NodeToWireChecked(entry)
		if err != nil {
			return fmt.Errorf("export: encode node history %d v%d: %w", id, entry.Version(), err)
		}
		if err := marshalAndWrite(w, exportTagNodeHist, &w2); err != nil {
			return fmt.Errorf("export: write node history %d v%d: %w", id, entry.Version(), err)
		}
	}
	return nil
}

func (c *Core) exportRelHistory(w io.Writer, pager storepkg.HistoryVersionPageCapability, id types.RelID) error {
	if pager == nil {
		history, err := c.getRelHistory(id)
		if err != nil {
			return fmt.Errorf("export: get rel history %d: %w", id, err)
		}
		return writeRelHistoryEntries(w, id, history)
	}

	var startVersion uint32
	for {
		history, err := c.relHistoryVersionsFrom(pager, id, startVersion, exportHistoryVersionBatchSize)
		if err != nil {
			return fmt.Errorf("export: get rel history %d from v%d: %w", id, startVersion, err)
		}
		if len(history) == 0 {
			return nil
		}
		if err := writeRelHistoryEntries(w, id, history); err != nil {
			return err
		}
		if len(history) < exportHistoryVersionBatchSize {
			return nil
		}
		lastVersion := history[len(history)-1].Version()
		if lastVersion < startVersion {
			return fmt.Errorf("export: rel history %d returned non-advancing page at v%d", id, startVersion)
		}
		if lastVersion == ^uint32(0) {
			return nil
		}
		startVersion = lastVersion + 1
	}
}

func writeRelHistoryEntries(w io.Writer, id types.RelID, history []*types.Relationship) error {
	for i, entry := range history {
		if entry == nil {
			return fmt.Errorf("export: encode rel history %d entry %d: %w", id, i, ErrNilRelationship)
		}
		w2, err := storeutil.RelToWireChecked(entry)
		if err != nil {
			return fmt.Errorf("export: encode rel history %d v%d: %w", id, entry.Version(), err)
		}
		if err := marshalAndWrite(w, exportTagRelHist, &w2); err != nil {
			return fmt.Errorf("export: write rel history %d v%d: %w", id, entry.Version(), err)
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
	if err := validateExportRecordSize("export", tag, uint64(len(data))); err != nil {
		return err
	}
	var header [5]byte
	header[0] = tag
	binary.BigEndian.PutUint32(header[1:5], uint32(len(data))) // #nosec G115 — len fits in uint32 for any reasonable record
	if err := writeFull(w, header[:]); err != nil {
		return err
	}
	return writeFull(w, data)
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if err != nil {
			return err
		}
		if n <= 0 || n > len(data) {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
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
	if err := validateExportRecordSize("import", header[0], uint64(length)); err != nil {
		return 0, nil, err
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
	if err := validateExportRecordSize("import", header[0], uint64(length)); err != nil {
		return 0, nil, err
	}
	*offset += 5
	recordLen := int(length) // #nosec G115 -- validateExportRecordSize caps records to 128 MiB, well below int max.
	if len(src)-*offset < recordLen {
		return 0, nil, fmt.Errorf("record body (tag=0x%02x, len=%d): %w", header[0], length, io.ErrUnexpectedEOF)
	}
	bodyStart := *offset
	bodyEnd := bodyStart + recordLen
	*offset = bodyEnd
	return header[0], src[bodyStart:bodyEnd], nil
}

func validateExportRecordSize(operation string, tag byte, length uint64) error {
	if length > maxExportRecordSize {
		return fmt.Errorf("%s: record too large (tag=0x%02x, len=%d, max=%d)", operation, tag, length, maxExportRecordSize)
	}
	return nil
}
