package core

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
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
)

// maxExportRecordSize caps the per-record allocation in readExportRecord.
// A node with 1000 max-size properties is ~66 MiB; 128 MiB gives safe headroom.
const maxExportRecordSize = 128 * 1024 * 1024 // 128 MiB

// Export writes a portable snapshot of the graph to w.
//
// The snapshot includes every current node and relationship, their full version
// history, and the label/reltype registries. The format is a sequence of
// length-prefixed msgpack records, each preceded by a 1-byte type tag and a
// 4-byte big-endian body length. This layout allows forward-compatible streaming
// without loading the whole file into memory.
//
// Export holds c.mu.RLock for the duration. That excludes tx/batch (which
// take c.mu.Lock) and Reset (which takes c.mu.Lock), but does NOT exclude
// individual Add/Update/Delete mutations, which also use c.mu.RLock and
// can interleave with Export's reads (R4-F4). The streamed snapshot is
// therefore best-effort: header counts, current entities, history
// records, and relationship records can each be observed at slightly
// different points along the standalone-mutation timeline. Callers that
// require a strongly consistent snapshot should drive Export from
// inside a tx/batch (which takes the write lock) or pre-commit a
// snapshot via the Temporal sub-API.
func (o *IOOps) Export(w io.Writer) error {
	c := o.c
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
					w2 := storeutil.NodeToWire(entry)
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
					w2 := storeutil.RelToWire(entry)
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

// ErrImportSizeLimit is the canonical IO size-cap sentinel — declared in
// pkg/graph/io and aliased here so internal references stay on the short
// identifier (R4-F8).
var ErrImportSizeLimit = tkgio.ErrImportSizeLimit

// Import reads a portable graph snapshot from r using default options
// (platform default temp dir, no size cap). See ImportWithOptions for
// the option-bearing variant.
func (o *IOOps) Import(r io.Reader) error {
	return o.ImportWithOptions(r, tkgio.ImportOptions{})
}

// ImportWithOptions reads a portable graph snapshot from r and restores
// it into c, honouring the staging-directory and size-cap options.
//
// Registries are imported if they are empty; if already populated (e.g., the
// graph was loaded from a prior Badger directory), the existing registry is kept
// and the import continues without error (idempotent registry behaviour).
//
// Two-phase implementation with a disk-backed staging buffer:
//   - Phase 1 (no lock): all records are streamed from r into a temporary file
//     under opts.StagingDir (default: platform temp dir). io.Reader I/O can be
//     slow (file, network); holding c.mu.Lock for its duration would block all
//     Add/Update/Query callers for potentially minutes. Memory stays bounded —
//     one record body at a time (capped by maxExportRecordSize) plus a fixed
//     I/O buffer. If opts.MaxStagedBytes > 0 and the staging size would exceed
//     it, Phase 1 returns ErrImportSizeLimit and the graph is unchanged.
//   - Phase 2 (under c.mu.Lock): the staging file is rewound and re-read
//     record-by-record. Only CPU deserialization + in-memory store ops happen
//     here. No network reads under the lock; the staging file is on local
//     disk so reads do not stall on remote latency.
//
// Memory: O(maxExportRecordSize) regardless of export size. Disk: the staging
// file is sized to match the export and is removed via defer at function exit.
//
// Phase-1 errors leave the graph state unchanged. Phase-2 errors may leave a
// partially populated graph — the import is best-effort under the lock and
// not transactional. Callers requiring transactional restore must drive
// Import into a fresh graph and swap stores on success.
//
// Import caller must ensure that entity IDs in the export do not conflict with
// existing IDs in the graph (typical use: import into a freshly created graph).
func (o *IOOps) ImportWithOptions(r io.Reader, opts tkgio.ImportOptions) error {
	c := o.c

	// --- Phase 1: stream all records into a temp staging file (no lock) ---
	staging, err := os.CreateTemp(opts.StagingDir, "tkg-import-*.stage")
	if err != nil {
		return fmt.Errorf("import: create staging file: %w", err)
	}
	stagingPath := staging.Name()
	defer func() {
		_ = staging.Close()
		_ = os.Remove(stagingPath)
	}()

	bw := bufio.NewWriterSize(staging, 1<<20) // 1 MiB write buffer
	var staged int64
	for {
		tag, data, rerr := readExportRecord(r)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break // clean end of stream
			}
			return fmt.Errorf("import: read record: %w", rerr)
		}
		// Each staged record is 5 bytes of header + len(data) bytes
		// of body. The size cap is checked BEFORE writing so a large
		// final record cannot push the staged total above the cap.
		recordSize := int64(5 + len(data))
		if opts.MaxStagedBytes > 0 && staged+recordSize > opts.MaxStagedBytes {
			return fmt.Errorf("%w: would-be %d bytes > cap %d", ErrImportSizeLimit, staged+recordSize, opts.MaxStagedBytes)
		}
		if werr := writeExportRecord(bw, tag, data); werr != nil {
			return fmt.Errorf("import: stage record: %w", werr)
		}
		staged += recordSize
	}
	if err := bw.Flush(); err != nil {
		return fmt.Errorf("import: flush staging: %w", err)
	}
	if _, err := staging.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("import: rewind staging: %w", err)
	}

	// --- Phase 2: replay staged records under write lock ---
	// Reads are from a local temp file (bounded latency, no network) so the
	// time-under-lock cost is dominated by deserialization + store writes,
	// not I/O. A buffered reader keeps the actual disk syscalls cheap.
	br := bufio.NewReaderSize(staging, 1<<20)
	c.mu.Lock()
	defer c.mu.Unlock()

	// R4-F11: track whether we've seen a header and a registry before
	// any tokenized entity record. Tokenized records (nodes / rels and
	// their histories) reference label/rel-type tokens that resolve
	// through the registry — without a compatible registry import, the
	// imported entities have unresolvable labels/types and label/type
	// queries cannot find them.
	var (
		seenHeader   bool
		seenRegistry bool
	)

	for {
		tag, data, rerr := readExportRecord(br)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return fmt.Errorf("import: replay staging: %w", rerr)
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
			seenHeader = true

		case exportTagRegistry:
			if !seenHeader {
				return fmt.Errorf("%w: registry record before header", ErrCorruptExport)
			}
			var reg tiered.RegistryFileData
			if err := msgpack.Unmarshal(data, &reg); err != nil {
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
			seenRegistry = true

		case exportTagNode, exportTagNodeHist, exportTagRel, exportTagRelHist:
			// R4-F11: tokenized entity records require a header AND a
			// registry to be replayed first. Otherwise the entity is
			// stored with token IDs that resolve to empty strings.
			if !seenHeader {
				return fmt.Errorf("%w: entity record before header", ErrCorruptExport)
			}
			if !seenRegistry {
				return fmt.Errorf("%w: entity record before registry", ErrCorruptExport)
			}
			if err := importEntityRecord(c, tag, data); err != nil {
				return err
			}

		default:
			// Unknown tag — skip for forward compatibility with newer export versions.
		}
	}

	return nil
}

// importEntityRecord dispatches the four entity record tags to their
// respective Put paths. Extracted from ImportWithOptions so the header
// and registry preconditions for each tag can be enforced uniformly.
//
// R4-F12: duplicate current entities are rejected unless the existing
// entity is byte-identical to the one being replayed (idempotent
// re-import). Without this guard, a caller could overwrite history
// onto a current entity that has different content, producing a hybrid
// graph that never existed in either source.
func importEntityRecord(c *Core, tag uint8, data []byte) error {
	switch tag {
	case exportTagNode:
		var wn storeutil.NodeWire
		if err := msgpack.Unmarshal(data, &wn); err != nil {
			return fmt.Errorf("import: unmarshal node: %w", err)
		}
		if err := validateNodeWire(&wn); err != nil {
			return fmt.Errorf("import: node %d: %w", wn.ID, err)
		}
		n := storeutil.WireToNode(wn)
		if err := c.store.PutNode(n); err != nil {
			if !errors.Is(err, storepkg.ErrNodeExists) {
				return fmt.Errorf("import: put node %d: %w", wn.ID, err)
			}
			// R4-F12: duplicate node — accept only if existing content matches.
			existing, gerr := c.store.GetNode(n.ID())
			if gerr != nil {
				return fmt.Errorf("import: load existing node %d for conflict check: %w", wn.ID, gerr)
			}
			if !nodeWireMatches(existing, &wn) {
				return fmt.Errorf("import: node %d: %w", wn.ID, ErrCorruptExport)
			}
		}

	case exportTagNodeHist:
		var wn storeutil.NodeWire
		if err := msgpack.Unmarshal(data, &wn); err != nil {
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
		if err := msgpack.Unmarshal(data, &wr); err != nil {
			return fmt.Errorf("import: unmarshal rel: %w", err)
		}
		if err := validateRelWire(&wr); err != nil {
			return fmt.Errorf("import: rel %d: %w", wr.ID, err)
		}
		rel := storeutil.WireToRel(wr)
		if err := c.store.PutRelationship(rel); err != nil {
			if !errors.Is(err, storepkg.ErrRelExists) {
				return fmt.Errorf("import: put rel %d: %w", wr.ID, err)
			}
			// R4-F12: duplicate rel — accept only if existing content matches.
			existing, gerr := c.store.GetRelationship(rel.ID())
			if gerr != nil {
				return fmt.Errorf("import: load existing rel %d for conflict check: %w", wr.ID, gerr)
			}
			if !relWireMatches(existing, &wr) {
				return fmt.Errorf("import: rel %d: %w", wr.ID, ErrCorruptExport)
			}
		}

	case exportTagRelHist:
		var wr storeutil.RelWire
		if err := msgpack.Unmarshal(data, &wr); err != nil {
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
	}
	return nil
}

// nodeWireMatches returns true if the in-store node has the same
// serialized representation as the wire record. We compare the
// canonical msgpack form rather than walking individual fields so the
// invariant is "byte-identical export wire" rather than "fields the
// reviewer happened to think of".
func nodeWireMatches(existing *types.Node, want *storeutil.NodeWire) bool {
	if existing == nil {
		return false
	}
	got := storeutil.NodeToWire(existing)
	gotBytes, err := msgpack.Marshal(&got)
	if err != nil {
		return false
	}
	wantBytes, err := msgpack.Marshal(want)
	if err != nil {
		return false
	}
	return bytes.Equal(gotBytes, wantBytes)
}

func relWireMatches(existing *types.Relationship, want *storeutil.RelWire) bool {
	if existing == nil {
		return false
	}
	got := storeutil.RelToWire(existing)
	gotBytes, err := msgpack.Marshal(&got)
	if err != nil {
		return false
	}
	wantBytes, err := msgpack.Marshal(want)
	if err != nil {
		return false
	}
	return bytes.Equal(gotBytes, wantBytes)
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
