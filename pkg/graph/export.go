package graph

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/vmihailenco/msgpack/v5"
	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Export format version. Increment when the record layout changes in a
// backward-incompatible way.
const exportFormatVersion byte = 1

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

// ExportGraph writes a portable snapshot of the graph to w.
//
// The snapshot includes every current node and relationship, their full version
// history, and the label/reltype registries. The format is a sequence of
// length-prefixed msgpack records, each preceded by a 1-byte type tag and a
// 4-byte big-endian body length. This layout allows forward-compatible streaming
// without loading the whole file into memory.
//
// ExportGraph holds g.mu.RLock for the duration — concurrent Snapshot and Reset
// are blocked, but individual Add/Update mutations are NOT.
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

	// --- Current nodes ---
	// Two-phase (C4): collect IDs in callback (store lock held), fetch entities after.
	// The callback MUST NOT call other store methods (B15 — deadlock via store lock).
	var nodeIDs []snowflake.ID
	if err := g.store.ForEachNodeID(func(id snowflake.ID) bool {
		nodeIDs = append(nodeIDs, id)
		return true
	}); err != nil {
		return fmt.Errorf("export: iterate node IDs: %w", err)
	}
	for _, id := range nodeIDs {
		n, err := g.store.GetNode(id)
		if errors.Is(err, ErrNodeNotFound) {
			continue // concurrently deleted between ForEachNodeID and GetNode
		}
		if err != nil {
			return fmt.Errorf("export: get node %d: %w", id, err)
		}
		w2 := nodeToWire(n)
		if err := marshalAndWrite(w, exportTagNode, &w2); err != nil {
			return fmt.Errorf("export: write node %d: %w", id, err)
		}
	}

	// --- Node history ---
	var nodeHistIDs []snowflake.ID
	if err := g.store.ForEachNodeHistoryID(func(id snowflake.ID) bool {
		nodeHistIDs = append(nodeHistIDs, id)
		return true
	}); err != nil {
		return fmt.Errorf("export: iterate node history IDs: %w", err)
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

	// --- Current relationships ---
	var relIDs []snowflake.ID
	if err := g.store.ForEachRelID(func(id snowflake.ID) bool {
		relIDs = append(relIDs, id)
		return true
	}); err != nil {
		return fmt.Errorf("export: iterate rel IDs: %w", err)
	}
	for _, id := range relIDs {
		r, err := g.store.GetRelationship(id)
		if errors.Is(err, ErrRelNotFound) {
			continue // concurrently deleted
		}
		if err != nil {
			return fmt.Errorf("export: get rel %d: %w", id, err)
		}
		w2 := relToWire(r)
		if err := marshalAndWrite(w, exportTagRel, &w2); err != nil {
			return fmt.Errorf("export: write rel %d: %w", id, err)
		}
	}

	// --- Relationship history ---
	var relHistIDs []snowflake.ID
	if err := g.store.ForEachRelHistoryID(func(id snowflake.ID) bool {
		relHistIDs = append(relHistIDs, id)
		return true
	}); err != nil {
		return fmt.Errorf("export: iterate rel history IDs: %w", err)
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

// ImportGraph reads a portable graph snapshot from r and restores it into g.
//
// Registries are imported if they are empty; if already populated (e.g., the
// graph was loaded from a prior Badger directory), the existing registry is kept
// and the import continues without error (idempotent registry behaviour).
//
// ImportGraph acquires g.mu.Lock — all concurrent Snapshot/Reset operations are
// blocked for the duration. Individual Add/Update callers also block on the graph
// write lock, making import a serialised restore operation.
//
// The caller must ensure that entity IDs in the export do not conflict with
// existing IDs in the graph (typical use: import into a freshly created graph).
func (g *Graph) ImportGraph(r io.Reader) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	for {
		tag, data, err := readExportRecord(r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break // clean end of stream
			}
			return fmt.Errorf("import: read record: %w", err)
		}

		switch tag {
		case exportTagHeader:
			var hdr exportHeader
			if err := msgpack.Unmarshal(data, &hdr); err != nil {
				return fmt.Errorf("import: unmarshal header: %w", err)
			}
			if hdr.Version != exportFormatVersion {
				return fmt.Errorf("%w: got %d, want %d", ErrIncompatibleExport, hdr.Version, exportFormatVersion)
			}

		case exportTagRegistry:
			var reg registryFileData
			if err := msgpack.Unmarshal(data, &reg); err != nil {
				return fmt.Errorf("import: unmarshal registry: %w", err)
			}
			if err := g.labels.ImportNames(reg.Labels); err != nil && !errors.Is(err, ErrRegistryNotEmpty) {
				return fmt.Errorf("import: label registry: %w", err)
			}
			if err := g.relTypes.ImportNames(reg.RelTypes); err != nil && !errors.Is(err, ErrRegistryNotEmpty) {
				return fmt.Errorf("import: reltype registry: %w", err)
			}

		case exportTagNode:
			var wn nodeWire
			if err := msgpack.Unmarshal(data, &wn); err != nil {
				return fmt.Errorf("import: unmarshal node: %w", err)
			}
			n := wireToNode(wn)
			if err := g.store.PutNode(n); err != nil && !errors.Is(err, ErrNodeExists) {
				return fmt.Errorf("import: put node %d: %w", wn.ID, err)
			}

		case exportTagNodeHist:
			var wn nodeWire
			if err := msgpack.Unmarshal(data, &wn); err != nil {
				return fmt.Errorf("import: unmarshal node history: %w", err)
			}
			n := wireToNode(wn)
			id := snowflake.ID(wn.ID) //nolint:gosec — ID from our own serialization
			if err := g.store.PutNodeVersion(id, n.Version(), n); err != nil {
				return fmt.Errorf("import: put node history %d v%d: %w", wn.ID, n.Version(), err)
			}

		case exportTagRel:
			var wr relWire
			if err := msgpack.Unmarshal(data, &wr); err != nil {
				return fmt.Errorf("import: unmarshal rel: %w", err)
			}
			rel := wireToRel(wr)
			if err := g.store.PutRelationship(rel); err != nil && !errors.Is(err, ErrRelExists) {
				return fmt.Errorf("import: put rel %d: %w", wr.ID, err)
			}

		case exportTagRelHist:
			var wr relWire
			if err := msgpack.Unmarshal(data, &wr); err != nil {
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
	binary.BigEndian.PutUint32(header[1:5], uint32(len(data))) //nolint:gosec — len fits in uint32 for any reasonable record
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
	data = make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		return 0, nil, fmt.Errorf("record body (tag=0x%02x, len=%d): %w", header[0], length, err)
	}
	return header[0], data, nil
}
