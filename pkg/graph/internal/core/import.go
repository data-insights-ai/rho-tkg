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

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/integrity"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
	"github.com/vmihailenco/msgpack/v5"
)

// ErrImportSizeLimit is the canonical IO size-cap sentinel — declared in
// pkg/graph/io and aliased here so internal references stay on the short
// identifier (R4-F8).
var (
	ErrImportSizeLimit = tkgio.ErrImportSizeLimit
	ErrNilReader       = tkgio.ErrNilReader
)

const importMemoryStageLimit = 8 << 20 // 8 MiB before spilling to temp file.
const importPreallocLimit = 1 << 20    // avoid trusting unbounded header counts for map capacity.

// Import reads a portable graph snapshot from r and restores it into c,
// honouring the staging-directory and size-cap options. Pass tkgio.ImportOptions{}
// for default behavior (platform default temp dir, no size cap).
//
// API 4.0 change: the previous separate Import(r) and ImportWithOptions(r, opts)
// were merged into this single method. Pass tkgio.ImportOptions{} for the
// previous defaults.
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
// Phase-1 memory is O(maxExportRecordSize) regardless of export size. Phase 2
// keeps rollback snapshots plus stream-identity sets for touched rows, so replay
// memory scales with the number of imported entity IDs and history records.
// Disk: the staging file is sized to match the export and is removed via defer
// at function exit.
//
// Phase-1 errors leave the graph state unchanged. During Phase 2 the importer
// snapshots every touched current/history row before replaying it, so replay
// errors restore the graph to its pre-import state unless the underlying store
// also fails while applying the rollback.
//
// Existing current/history records with byte-identical wire content are treated
// as idempotent re-imports. Conflicting duplicate current/history records are
// rejected and rolled back with ErrCorruptExport.
func (o *IOOps) Import(r io.Reader, opts tkgio.ImportOptions) error {
	c := o.c
	if err := c.checkOpen(); err != nil {
		return err
	}
	if isNilInterfaceValue(r) {
		return ErrNilReader
	}
	if opts.MaxStagedBytes < 0 {
		return fmt.Errorf("%w: negative cap %d", ErrImportSizeLimit, opts.MaxStagedBytes)
	}

	// --- Phase 1: stream all records into a bounded stage (no lock) ---
	stage := newImportStage(opts.StagingDir, importMemoryStageLimit)
	defer stage.close()

	var staged int64
	for {
		tag, data, recordSize, rerr := readImportStageRecord(r, staged, opts.MaxStagedBytes)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break // clean end of stream
			}
			return fmt.Errorf("import: read record: %w", rerr)
		}
		if werr := stage.writeRecord(tag, data, recordSize); werr != nil {
			return fmt.Errorf("import: stage record: %w", werr)
		}
		staged += recordSize
	}
	var (
		br  *bufio.Reader
		err error
	)
	if stage.file != nil {
		br, err = stage.reader()
		if err != nil {
			return err
		}
	}

	// --- Phase 2: replay staged records under write lock ---
	// Reads are from a local temp file (bounded latency, no network) so the
	// time-under-lock cost is dominated by deserialization + store writes,
	// not I/O. A buffered reader keeps the actual disk syscalls cheap.
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed.Load() {
		return ErrGraphClosed
	}
	rollback := newImportRollback(c)
	emptyTarget, err := c.importTargetEmptyLocked()
	if err != nil {
		return err
	}
	rollback.emptyTarget = emptyTarget
	if err := c.importReplayStageLocked(stage, br, rollback); err != nil {
		if rbErr := rollback.rollback(); rbErr != nil {
			return fmt.Errorf("%w (rollback failed: %v)", err, rbErr)
		}
		return err
	}
	return nil
}

func importStageCapExceeded(staged, recordSize, cap int64) bool {
	if cap <= 0 {
		return false
	}
	return staged > cap || recordSize > cap-staged
}

func readImportStageRecord(r io.Reader, staged, cap int64) (tag byte, data []byte, recordSize int64, err error) {
	var header [5]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			// Partial header: the stream was cut mid-record — structural
			// corruption, not a clean end-of-stream (io.EOF stays untouched
			// as the caller's termination signal).
			return 0, nil, 0, fmt.Errorf("%w: truncated record header: %v", ErrCorruptExport, err)
		}
		return 0, nil, 0, err
	}
	length := binary.BigEndian.Uint32(header[1:5])
	if length > maxExportRecordSize {
		return 0, nil, 0, fmt.Errorf("import: record too large (tag=0x%02x, len=%d, max=%d)", header[0], length, maxExportRecordSize)
	}
	recordSize = int64(5) + int64(length)
	if importStageCapExceeded(staged, recordSize, cap) {
		return 0, nil, 0, fmt.Errorf("%w: staged %d bytes + record %d bytes > cap %d", ErrImportSizeLimit, staged, recordSize, cap)
	}
	data = make([]byte, length)
	if _, err := io.ReadFull(r, data); err != nil {
		// A record body shorter than its declared length is always
		// structural corruption (truncated or lying stream) — classify it so
		// consumers can errors.Is(err, ErrCorruptExport).
		return 0, nil, 0, fmt.Errorf("%w: record body (tag=0x%02x, len=%d): %v", ErrCorruptExport, header[0], length, err)
	}
	return header[0], data, recordSize, nil
}

// verifyImportedNodeHash recomputes the content hash of an imported node row
// and compares it with the hash the stream claims. Import reads untrusted
// bytes (lesson 6): without this check a transport bit-flip in a property
// value imports cleanly and produces a graph whose hash chain no longer
// verifies. Rows without integrity state are exempt (hashes are unkeyed, so
// this is corruption detection, not tamper-proofing).
func (c *Core) verifyImportedNodeHash(n *types.Node, id int64, kind string) error {
	ig := n.Integrity()
	if ig == nil || ig.Hash == "" {
		return nil
	}
	hash, err := integrity.ComputeNodeHashChecked(n, c.nodeLabelsUnlocked(n))
	if err != nil {
		return fmt.Errorf("import: %s %d: %w: recompute hash: %v", kind, id, ErrCorruptExport, err)
	}
	if hash != ig.Hash {
		return fmt.Errorf("import: %s %d: %w: content does not match its integrity hash", kind, id, ErrCorruptExport)
	}
	return nil
}

// verifyImportedRelHash is the relationship counterpart of
// verifyImportedNodeHash. FromNodeHash/ToNodeHash are not part of the
// content hash and are not checked here; chain links are covered by
// Verify*Chain.
func (c *Core) verifyImportedRelHash(r *types.Relationship, id int64, kind string) error {
	ig := r.Integrity()
	if ig == nil || ig.Hash == "" {
		return nil
	}
	hash, err := integrity.ComputeRelHashChecked(r, c.relTypeUnlocked(r))
	if err != nil {
		return fmt.Errorf("import: %s %d: %w: recompute hash: %v", kind, id, ErrCorruptExport, err)
	}
	if hash != ig.Hash {
		return fmt.Errorf("import: %s %d: %w: content does not match its integrity hash", kind, id, ErrCorruptExport)
	}
	return nil
}

func (c *Core) importTargetEmptyLocked() (bool, error) {
	nodeCount, err := c.nodeCount()
	if err != nil {
		return false, fmt.Errorf("import: node count: %w", err)
	}
	if nodeCount != 0 {
		return false, nil
	}
	relCount, err := c.relCount()
	if err != nil {
		return false, fmt.Errorf("import: relationship count: %w", err)
	}
	if relCount != 0 {
		return false, nil
	}
	nodeHistory, err := c.allNodeHistoryIDsFrom(0, 1)
	if err != nil {
		return false, fmt.Errorf("import: node history probe: %w", err)
	}
	if len(nodeHistory) != 0 {
		return false, nil
	}
	relHistory, err := c.allRelHistoryIDsFrom(0, 1)
	if err != nil {
		return false, fmt.Errorf("import: relationship history probe: %w", err)
	}
	return len(relHistory) == 0, nil
}

type importStage struct {
	stagingDir string
	memLimit   int64
	mem        bytes.Buffer
	file       *os.File
	writer     *bufio.Writer
	path       string
}

func newImportStage(stagingDir string, memLimit int64) *importStage {
	return &importStage{stagingDir: stagingDir, memLimit: memLimit}
}

func (s *importStage) writeRecord(tag byte, data []byte, recordSize int64) error {
	if s.file == nil && int64(s.mem.Len())+recordSize <= s.memLimit {
		return writeExportRecord(&s.mem, tag, data)
	}
	if err := s.ensureFile(); err != nil {
		return err
	}
	return writeExportRecord(s.writer, tag, data)
}

func (s *importStage) ensureFile() error {
	if s.file != nil {
		return nil
	}
	f, err := os.CreateTemp(s.stagingDir, "tkg-import-*.stage")
	if err != nil {
		return fmt.Errorf("create staging file: %w", err)
	}
	s.file = f
	s.path = f.Name()
	s.writer = bufio.NewWriterSize(f, 1<<20)
	if s.mem.Len() == 0 {
		return nil
	}
	if err := writeFull(s.writer, s.mem.Bytes()); err != nil {
		return fmt.Errorf("spill memory stage: %w", err)
	}
	s.mem.Reset()
	return nil
}

func (s *importStage) reader() (*bufio.Reader, error) {
	if s.file == nil {
		return bufio.NewReaderSize(bytes.NewReader(s.mem.Bytes()), 1<<20), nil
	}
	if err := s.writer.Flush(); err != nil {
		return nil, fmt.Errorf("import: flush staging: %w", err)
	}
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("import: rewind staging: %w", err)
	}
	return bufio.NewReaderSize(s.file, 1<<20), nil
}

func (s *importStage) close() {
	if s.file == nil {
		return
	}
	_ = s.file.Close()
	_ = os.Remove(s.path)
}

func (c *Core) importReplayStageLocked(stage *importStage, br *bufio.Reader, rollback *importRollback) error {
	if stage.file != nil {
		return c.importReplayLocked(br, rollback)
	}
	return c.importReplayBytesLocked(stage.mem.Bytes(), rollback)
}

// importReplayLocked replays staged records. Caller must hold c.mu.Lock.
func (c *Core) importReplayLocked(br *bufio.Reader, rollback *importRollback) error {
	return c.importReplayRecordsLocked(func() (byte, []byte, error) {
		return readExportRecord(br)
	}, rollback)
}

func (c *Core) importReplayBytesLocked(data []byte, rollback *importRollback) error {
	offset := 0
	return c.importReplayRecordsLocked(func() (byte, []byte, error) {
		return readExportRecordBytes(data, &offset)
	}, rollback)
}

func (c *Core) importReplayRecordsLocked(readRecord func() (byte, []byte, error), rollback *importRollback) error {
	// R4-F11: track whether we've seen a header and a registry before
	// any tokenized entity record. Tokenized records (nodes / rels and
	// their histories) reference label/rel-type tokens that resolve
	// through the registry — without a compatible registry import, the
	// imported entities have unresolvable labels/types and label/type
	// queries cannot find them.
	var (
		seenHeader   bool
		seenRegistry bool
		header       exportHeader
		seenRecords  = newImportReplaySeen()
	)

	for {
		tag, data, rerr := readRecord()
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			return fmt.Errorf("import: replay staging: %w", rerr)
		}
		switch tag {
		case exportTagHeader:
			if seenHeader {
				return fmt.Errorf("%w: duplicate header record", ErrCorruptExport)
			}
			var hdr exportHeader
			if err := storeutil.SafeUnmarshal(data, &hdr); err != nil {
				return fmt.Errorf("%w: unmarshal header: %v", ErrCorruptExport, err)
			}
			if hdr.Version != exportFormatVersion {
				return fmt.Errorf("%w: got %d, want %d", ErrIncompatibleExport, hdr.Version, exportFormatVersion)
			}
			if hdr.NodeCount < 0 {
				return fmt.Errorf("%w: negative node count %d", ErrCorruptExport, hdr.NodeCount)
			}
			if hdr.RelCount < 0 {
				return fmt.Errorf("%w: negative relationship count %d", ErrCorruptExport, hdr.RelCount)
			}
			header = hdr
			seenRecords.reserve(hdr)
			rollback.reserve(hdr)
			seenHeader = true

		case exportTagRegistry:
			if !seenHeader {
				return fmt.Errorf("%w: registry record before header", ErrCorruptExport)
			}
			if seenRegistry {
				return fmt.Errorf("%w: duplicate registry record", ErrCorruptExport)
			}
			var reg tiered.RegistryFileData
			if err := storeutil.SafeUnmarshal(data, &reg); err != nil {
				return fmt.Errorf("%w: unmarshal registry: %v", ErrCorruptExport, err)
			}
			if err := c.validateRegistryNames("label", reg.Labels); err != nil {
				return fmt.Errorf("%w: label registry: %w", ErrCorruptExport, err)
			}
			if err := c.validateRegistryNames("reltype", reg.RelTypes); err != nil {
				return fmt.Errorf("%w: reltype registry: %w", ErrCorruptExport, err)
			}
			if err := c.labels.ImportNames(reg.Labels); err != nil {
				if !errors.Is(err, ErrRegistryNotEmpty) {
					return fmt.Errorf("%w: label registry: %w", ErrCorruptExport, err)
				}
				// Existing registry: identical token mapping is safe (idempotent re-import).
				// Different mapping would silently corrupt all imported entity labels.
				if existing := c.labels.ExportNames(); !reflect.DeepEqual(existing, reg.Labels) {
					return fmt.Errorf("import: label registry: %w", ErrIncompatibleRegistry)
				}
			}
			if err := c.relTypes.ImportNames(reg.RelTypes); err != nil {
				if !errors.Is(err, ErrRegistryNotEmpty) {
					return fmt.Errorf("%w: reltype registry: %w", ErrCorruptExport, err)
				}
				if existing := c.relTypes.ExportNames(); !reflect.DeepEqual(existing, reg.RelTypes) {
					return fmt.Errorf("import: reltype registry: %w", ErrIncompatibleRegistry)
				}
			}
			if err := c.persistRegistries(); err != nil {
				return fmt.Errorf("import: persist registries: %w", err)
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
			if err := importEntityRecord(c, rollback, seenRecords, tag, data); err != nil {
				return err
			}

		default:
			return fmt.Errorf("%w: unknown record tag 0x%02x", ErrCorruptExport, tag)
		}
	}

	if !seenHeader {
		return fmt.Errorf("%w: missing header record", ErrCorruptExport)
	}
	if !seenRegistry {
		return fmt.Errorf("%w: missing registry record", ErrCorruptExport)
	}
	if seenRecords.nodeRecords != header.NodeCount {
		return fmt.Errorf("%w: header node count %d but stream contained %d current node records",
			ErrCorruptExport, header.NodeCount, seenRecords.nodeRecords)
	}
	if seenRecords.relRecords != header.RelCount {
		return fmt.Errorf("%w: header relationship count %d but stream contained %d current relationship records",
			ErrCorruptExport, header.RelCount, seenRecords.relRecords)
	}

	// Final trust-boundary pass: verify the full hash chain of every imported
	// entity. The per-record content-hash checks cannot see LINK corruption
	// (a transport flip inside a stored PrevHash string leaves every row's
	// content hash valid while the chain no longer verifies) — and an import
	// must never succeed while producing a graph that fails its own
	// integrity verification. History-only entities (deleted on the source)
	// are collected from the history records too.
	verifyNodes := make(map[types.NodeID]struct{}, len(seenRecords.nodes))
	for id := range seenRecords.nodes {
		verifyNodes[id] = struct{}{}
	}
	for key := range seenRecords.nodeHist {
		verifyNodes[key.id] = struct{}{}
	}
	for id := range verifyNodes {
		valid, err := c.verifyNodeChainLocked(id)
		if err != nil {
			return fmt.Errorf("import: verify node chain %d: %w", id.SnowflakeID(), err)
		}
		if !valid {
			return fmt.Errorf("import: node %d: %w: imported hash chain does not verify", id.SnowflakeID(), ErrCorruptExport)
		}
	}
	verifyRels := make(map[types.RelID]struct{}, len(seenRecords.rels))
	for id := range seenRecords.rels {
		verifyRels[id] = struct{}{}
	}
	for key := range seenRecords.relHist {
		verifyRels[key.id] = struct{}{}
	}
	for id := range verifyRels {
		valid, err := c.verifyRelChainLocked(id)
		if err != nil {
			return fmt.Errorf("import: verify rel chain %d: %w", id.SnowflakeID(), err)
		}
		if !valid {
			return fmt.Errorf("import: rel %d: %w: imported hash chain does not verify", id.SnowflakeID(), ErrCorruptExport)
		}
	}
	return nil
}

type importReplaySeen struct {
	nodeRecords int64
	relRecords  int64

	nodes    map[types.NodeID]struct{}
	nodeHist map[importNodeHistKey]struct{}
	rels     map[types.RelID]struct{}
	relHist  map[importRelHistKey]struct{}
}

type importNodeHistKey struct {
	id      types.NodeID
	version uint32
}

type importRelHistKey struct {
	id      types.RelID
	version uint32
}

func newImportReplaySeen() *importReplaySeen {
	return &importReplaySeen{
		nodes:    make(map[types.NodeID]struct{}),
		nodeHist: make(map[importNodeHistKey]struct{}),
		rels:     make(map[types.RelID]struct{}),
		relHist:  make(map[importRelHistKey]struct{}),
	}
}

func (s *importReplaySeen) reserve(h exportHeader) {
	if nodeCap := importCountCap(h.NodeCount); nodeCap > len(s.nodes) {
		s.nodes = make(map[types.NodeID]struct{}, nodeCap)
	}
	if relCap := importCountCap(h.RelCount); relCap > len(s.rels) {
		s.rels = make(map[types.RelID]struct{}, relCap)
	}
}

func (s *importReplaySeen) recordNode(id types.NodeID) error {
	if _, ok := s.nodes[id]; ok {
		return fmt.Errorf("%w: duplicate current node record %d", ErrCorruptExport, id)
	}
	s.nodes[id] = struct{}{}
	s.nodeRecords++
	return nil
}

func (s *importReplaySeen) recordNodeHist(id types.NodeID, version uint32) error {
	key := importNodeHistKey{id: id, version: version}
	if _, ok := s.nodeHist[key]; ok {
		return fmt.Errorf("%w: duplicate node history record %d v%d", ErrCorruptExport, id, version)
	}
	s.nodeHist[key] = struct{}{}
	return nil
}

func (s *importReplaySeen) recordRel(id types.RelID) error {
	if _, ok := s.rels[id]; ok {
		return fmt.Errorf("%w: duplicate current relationship record %d", ErrCorruptExport, id)
	}
	s.rels[id] = struct{}{}
	s.relRecords++
	return nil
}

func (s *importReplaySeen) recordRelHist(id types.RelID, version uint32) error {
	key := importRelHistKey{id: id, version: version}
	if _, ok := s.relHist[key]; ok {
		return fmt.Errorf("%w: duplicate relationship history record %d v%d", ErrCorruptExport, id, version)
	}
	s.relHist[key] = struct{}{}
	return nil
}

// importEntityRecord dispatches the four entity record tags to their
// respective Put paths. Extracted from ImportWithOptions so the header
// and registry preconditions for each tag can be enforced uniformly.
//
// R4-F12: current entities that already existed before this import are allowed
// only when their bytes match the stream (idempotent re-import). Repeated
// records inside the same stream are always corrupt; otherwise one duplicated
// record could hide a missing one while still matching the header count.
func importEntityRecord(c *Core, rollback *importRollback, seen *importReplaySeen, tag uint8, data []byte) error {
	switch tag {
	case exportTagNode:
		var wn storeutil.NodeWire
		if err := storeutil.SafeUnmarshal(data, &wn); err != nil {
			return fmt.Errorf("%w: unmarshal node: %v", ErrCorruptExport, err)
		}
		n, err := storeutil.WireToNodeChecked(wn)
		if err != nil {
			return fmt.Errorf("import: node %d: %w: %v", wn.ID, ErrCorruptExport, err)
		}
		if err := validateNodeTokensInRegistry(&wn, c.labels); err != nil {
			return fmt.Errorf("import: node %d: %w", wn.ID, err)
		}
		if err := c.validatePropertySliceLimits(n.Properties()); err != nil {
			return fmt.Errorf("import: node %d: %w", wn.ID, err)
		}
		if err := c.verifyImportedNodeHash(n, wn.ID, "node"); err != nil {
			return err
		}
		if err := seen.recordNode(n.ID()); err != nil {
			return err
		}
		if err := rollback.captureNode(n.ID()); err != nil {
			return fmt.Errorf("import: snapshot node %d: %w", wn.ID, err)
		}
		if err := c.store.PutNode(n); err != nil {
			if !errors.Is(err, storepkg.ErrNodeExists) {
				return fmt.Errorf("import: put node %d: %w", wn.ID, err)
			}
			// R4-F12: duplicate node — accept only if existing content matches.
			existing, gerr := c.getCurrentNode(n.ID())
			if gerr != nil {
				return fmt.Errorf("import: load existing node %d for conflict check: %w", wn.ID, gerr)
			}
			if !nodeWireMatches(existing, &wn) {
				return fmt.Errorf("import: node %d: %w", wn.ID, ErrCorruptExport)
			}
		}

	case exportTagNodeHist:
		var wn storeutil.NodeWire
		if err := storeutil.SafeUnmarshal(data, &wn); err != nil {
			return fmt.Errorf("%w: unmarshal node history: %v", ErrCorruptExport, err)
		}
		n, err := storeutil.WireToNodeChecked(wn)
		if err != nil {
			return fmt.Errorf("import: node history %d: %w: %v", wn.ID, ErrCorruptExport, err)
		}
		if err := validateNodeTokensInRegistry(&wn, c.labels); err != nil {
			return fmt.Errorf("import: node history %d: %w", wn.ID, err)
		}
		if err := c.validatePropertySliceLimits(n.Properties()); err != nil {
			return fmt.Errorf("import: node history %d: %w", wn.ID, err)
		}
		if err := c.verifyImportedNodeHash(n, wn.ID, "node history"); err != nil {
			return err
		}
		id := types.NodeID(wn.ID) //nolint:gosec — ID from our own serialization
		if err := seen.recordNodeHist(id, n.Version()); err != nil {
			return err
		}
		if err := rollback.captureNodeHistory(id, n.Version()); err != nil {
			return fmt.Errorf("import: snapshot node history %d: %w", wn.ID, err)
		}
		// R5-F4: history records carry the same idempotent /
		// conflict-rejection contract as current entities (R4-F12).
		// PutNodeVersion silently overwrites, so without this check a
		// re-import with diverging history would replace the existing
		// version snapshot in place — exactly the hybrid-graph hazard
		// R4-F12 closed for current entities.
		existing, gerr := c.getNodeVersion(id, n.Version())
		switch {
		case gerr == nil:
			if !nodeWireMatches(existing, &wn) {
				return fmt.Errorf("import: node history %d v%d: %w", wn.ID, n.Version(), ErrCorruptExport)
			}
			// Identical version already present — skip the write.
		case errors.Is(gerr, storepkg.ErrVersionNotFound):
			if err := c.store.PutNodeVersion(id, n.Version(), n); err != nil {
				return fmt.Errorf("import: put node history %d v%d: %w", wn.ID, n.Version(), err)
			}
		default:
			return fmt.Errorf("import: load existing node history %d v%d for conflict check: %w", wn.ID, n.Version(), gerr)
		}

	case exportTagRel:
		var wr storeutil.RelWire
		if err := storeutil.SafeUnmarshal(data, &wr); err != nil {
			return fmt.Errorf("%w: unmarshal rel: %v", ErrCorruptExport, err)
		}
		rel, err := storeutil.WireToRelChecked(wr)
		if err != nil {
			return fmt.Errorf("import: rel %d: %w: %v", wr.ID, ErrCorruptExport, err)
		}
		if err := validateRelTokensInRegistry(&wr, c.relTypes); err != nil {
			return fmt.Errorf("import: rel %d: %w", wr.ID, err)
		}
		if err := c.validatePropertySliceLimits(rel.Properties()); err != nil {
			return fmt.Errorf("import: rel %d: %w", wr.ID, err)
		}
		if err := c.verifyImportedRelHash(rel, wr.ID, "rel"); err != nil {
			return err
		}
		if err := seen.recordRel(rel.ID()); err != nil {
			return err
		}
		if err := rollback.captureRel(rel.ID()); err != nil {
			return fmt.Errorf("import: snapshot rel %d: %w", wr.ID, err)
		}
		if err := c.store.PutRelationship(rel); err != nil {
			if !errors.Is(err, storepkg.ErrRelExists) {
				return fmt.Errorf("import: put rel %d: %w", wr.ID, err)
			}
			// R4-F12: duplicate rel — accept only if existing content matches.
			existing, gerr := c.getCurrentRelationship(rel.ID())
			if gerr != nil {
				return fmt.Errorf("import: load existing rel %d for conflict check: %w", wr.ID, gerr)
			}
			if !relWireMatches(existing, &wr) {
				return fmt.Errorf("import: rel %d: %w", wr.ID, ErrCorruptExport)
			}
		}

	case exportTagRelHist:
		var wr storeutil.RelWire
		if err := storeutil.SafeUnmarshal(data, &wr); err != nil {
			return fmt.Errorf("%w: unmarshal rel history: %v", ErrCorruptExport, err)
		}
		rel, err := storeutil.WireToRelChecked(wr)
		if err != nil {
			return fmt.Errorf("import: rel history %d: %w: %v", wr.ID, ErrCorruptExport, err)
		}
		if err := validateRelTokensInRegistry(&wr, c.relTypes); err != nil {
			return fmt.Errorf("import: rel history %d: %w", wr.ID, err)
		}
		if err := c.validatePropertySliceLimits(rel.Properties()); err != nil {
			return fmt.Errorf("import: rel history %d: %w", wr.ID, err)
		}
		if err := c.verifyImportedRelHash(rel, wr.ID, "rel history"); err != nil {
			return err
		}
		id := types.RelID(wr.ID) //nolint:gosec — ID from our own serialization
		if err := seen.recordRelHist(id, rel.Version()); err != nil {
			return err
		}
		if err := rollback.captureRelHistory(id, rel.Version()); err != nil {
			return fmt.Errorf("import: snapshot rel history %d: %w", wr.ID, err)
		}
		// R5-F4: same idempotent / conflict-rejection contract as
		// rel current entities (R4-F12). PutRelVersion silently
		// overwrites, so a diverging re-import would replace the
		// version in place without this guard.
		existing, gerr := c.getRelVersion(id, rel.Version())
		switch {
		case gerr == nil:
			if !relWireMatches(existing, &wr) {
				return fmt.Errorf("import: rel history %d v%d: %w", wr.ID, rel.Version(), ErrCorruptExport)
			}
			// Identical version already present — skip the write.
		case errors.Is(gerr, storepkg.ErrVersionNotFound):
			if err := c.store.PutRelVersion(id, rel.Version(), rel); err != nil {
				return fmt.Errorf("import: put rel history %d v%d: %w", wr.ID, rel.Version(), err)
			}
		default:
			return fmt.Errorf("import: load existing rel history %d v%d for conflict check: %w", wr.ID, rel.Version(), gerr)
		}
	}
	return nil
}

type importRollback struct {
	c        *Core
	labels   []string
	relTypes []string

	emptyTarget bool
	nodes       map[types.NodeID]importNodeSnapshot
	nodeOrder   []types.NodeID
	rels        map[types.RelID]importRelSnapshot
	relOrder    []types.RelID
}

type importNodeSnapshot struct {
	current         *types.Node
	history         []*types.Node
	historyTrimFrom uint32
	historyTouched  bool
	useHistoryTrim  bool
}

type importRelSnapshot struct {
	current         *types.Relationship
	history         []*types.Relationship
	historyTrimFrom uint32
	historyTouched  bool
	useHistoryTrim  bool
}

func newImportRollback(c *Core) *importRollback {
	return &importRollback{
		c:        c,
		labels:   c.labels.ExportNames(),
		relTypes: c.relTypes.ExportNames(),
		nodes:    make(map[types.NodeID]importNodeSnapshot),
		rels:     make(map[types.RelID]importRelSnapshot),
	}
}

func (rb *importRollback) reserve(h exportHeader) {
	if nodeCap := importCountCap(h.NodeCount); nodeCap > len(rb.nodes) {
		rb.nodes = make(map[types.NodeID]importNodeSnapshot, nodeCap)
		rb.nodeOrder = make([]types.NodeID, 0, nodeCap)
	}
	if relCap := importCountCap(h.RelCount); relCap > len(rb.rels) {
		rb.rels = make(map[types.RelID]importRelSnapshot, relCap)
		rb.relOrder = make([]types.RelID, 0, relCap)
	}
}

func importCountCap(n int64) int {
	if n <= 0 {
		return 0
	}
	if n > importPreallocLimit {
		return importPreallocLimit
	}
	return int(n)
}

func (rb *importRollback) captureNode(id types.NodeID) error {
	if _, ok := rb.nodes[id]; ok {
		return nil
	}
	if rb.emptyTarget {
		rb.nodes[id] = importNodeSnapshot{}
		rb.nodeOrder = append(rb.nodeOrder, id)
		return nil
	}

	var current *types.Node
	n, err := rb.c.getCurrentNode(id)
	switch {
	case err == nil:
		current = n.DeepCopy()
	case errors.Is(err, storepkg.ErrNodeNotFound):
	default:
		return err
	}

	rb.nodes[id] = importNodeSnapshot{current: current}
	rb.nodeOrder = append(rb.nodeOrder, id)
	return nil
}

func (rb *importRollback) captureNodeHistory(id types.NodeID, version uint32) error {
	if err := rb.captureNode(id); err != nil {
		return err
	}
	snap := rb.nodes[id]
	if snap.historyTouched {
		if !snap.useHistoryTrim || version >= snap.historyTrimFrom {
			return nil
		}
	}

	if rb.emptyTarget {
		snap.history = nil
		snap.historyTrimFrom = version
		snap.historyTouched = true
		snap.useHistoryTrim = rb.c.historyTrim != nil
		rb.nodes[id] = snap
		return nil
	}

	if rb.c.historyTrim != nil {
		history, err := rb.c.copyNodeHistoryFrom(id, version)
		if err != nil {
			return err
		}
		snap.history = history
		snap.historyTrimFrom = version
		snap.historyTouched = true
		snap.useHistoryTrim = true
		rb.nodes[id] = snap
		return nil
	}

	history, err := rb.c.copyNodeHistory(id)
	if err != nil {
		return err
	}
	snap.history = history
	snap.historyTouched = true
	rb.nodes[id] = snap
	return nil
}

func (rb *importRollback) captureRel(id types.RelID) error {
	if _, ok := rb.rels[id]; ok {
		return nil
	}
	if rb.emptyTarget {
		rb.rels[id] = importRelSnapshot{}
		rb.relOrder = append(rb.relOrder, id)
		return nil
	}

	var current *types.Relationship
	rel, err := rb.c.getCurrentRelationship(id)
	switch {
	case err == nil:
		current = rel.DeepCopy()
	case errors.Is(err, storepkg.ErrRelNotFound):
	default:
		return err
	}

	rb.rels[id] = importRelSnapshot{current: current}
	rb.relOrder = append(rb.relOrder, id)
	return nil
}

func (rb *importRollback) captureRelHistory(id types.RelID, version uint32) error {
	if err := rb.captureRel(id); err != nil {
		return err
	}
	snap := rb.rels[id]
	if snap.historyTouched {
		if !snap.useHistoryTrim || version >= snap.historyTrimFrom {
			return nil
		}
	}

	if rb.emptyTarget {
		snap.history = nil
		snap.historyTrimFrom = version
		snap.historyTouched = true
		snap.useHistoryTrim = rb.c.historyTrim != nil
		rb.rels[id] = snap
		return nil
	}

	if rb.c.historyTrim != nil {
		history, err := rb.c.copyRelHistoryFrom(id, version)
		if err != nil {
			return err
		}
		snap.history = history
		snap.historyTrimFrom = version
		snap.historyTouched = true
		snap.useHistoryTrim = true
		rb.rels[id] = snap
		return nil
	}

	history, err := rb.c.copyRelHistory(id)
	if err != nil {
		return err
	}
	snap.history = history
	snap.historyTouched = true
	rb.rels[id] = snap
	return nil
}

func (rb *importRollback) rollback() error {
	var firstErr error
	capture := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	for i := len(rb.relOrder) - 1; i >= 0; i-- {
		id := rb.relOrder[i]
		snap := rb.rels[id]
		if snap.current == nil {
			if err := rb.c.store.DeleteRelationship(id); err != nil && !errors.Is(err, storepkg.ErrRelNotFound) {
				capture(err)
			}
		} else {
			if _, err := rb.c.getCurrentRelationship(id); errors.Is(err, storepkg.ErrRelNotFound) {
				capture(rb.c.store.PutRelationship(snap.current))
			} else if err != nil {
				capture(err)
			} else {
				capture(rb.c.store.ReplaceRelationship(snap.current))
			}
		}
		capture(restoreImportRelHistory(rb.c, id, snap))
	}

	for i := len(rb.nodeOrder) - 1; i >= 0; i-- {
		id := rb.nodeOrder[i]
		snap := rb.nodes[id]
		if snap.current == nil {
			if err := rb.c.store.DeleteNodeCascade(id); err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
				capture(err)
			}
		} else {
			if _, err := rb.c.getCurrentNode(id); errors.Is(err, storepkg.ErrNodeNotFound) {
				capture(rb.c.store.PutNode(snap.current))
			} else if err != nil {
				capture(err)
			} else {
				capture(rb.c.store.ReplaceNode(snap.current))
			}
		}
		capture(restoreImportNodeHistory(rb.c, id, snap))
	}

	capture(rb.restoreRegistries())
	return firstErr
}

func (c *Core) copyNodeHistoryFrom(id types.NodeID, version uint32) ([]*types.Node, error) {
	if pager := historyVersionPageCapability(c.store); pager != nil {
		history, err := c.nodeHistoryVersionsFrom(pager, id, version, 0)
		if err != nil {
			return nil, err
		}
		return copyNodeHistoryRows(history), nil
	}
	history, err := c.copyNodeHistory(id)
	if err != nil {
		return nil, err
	}
	return nodeHistoryFromVersion(history, version), nil
}

func (c *Core) copyRelHistoryFrom(id types.RelID, version uint32) ([]*types.Relationship, error) {
	if pager := historyVersionPageCapability(c.store); pager != nil {
		history, err := c.relHistoryVersionsFrom(pager, id, version, 0)
		if err != nil {
			return nil, err
		}
		return copyRelHistoryRows(history), nil
	}
	history, err := c.copyRelHistory(id)
	if err != nil {
		return nil, err
	}
	return relHistoryFromVersion(history, version), nil
}

func nodeHistoryFromVersion(history []*types.Node, minVersion uint32) []*types.Node {
	out := history[:0]
	for _, n := range history {
		if n != nil && n.Version() >= minVersion {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func relHistoryFromVersion(history []*types.Relationship, minVersion uint32) []*types.Relationship {
	out := history[:0]
	for _, r := range history {
		if r != nil && r.Version() >= minVersion {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func restoreImportNodeHistory(c *Core, id types.NodeID, snap importNodeSnapshot) error {
	if !snap.historyTouched {
		return nil
	}
	if snap.useHistoryTrim {
		if err := c.historyTrim.TrimNodeHistoryFrom(id, snap.historyTrimFrom); err != nil {
			return err
		}
	} else if err := c.store.TruncateNodeHistory(id, 0); err != nil {
		return err
	}
	for _, n := range snap.history {
		if err := c.store.PutNodeVersion(id, n.Version(), n); err != nil {
			return err
		}
	}
	return nil
}

func restoreImportRelHistory(c *Core, id types.RelID, snap importRelSnapshot) error {
	if !snap.historyTouched {
		return nil
	}
	if snap.useHistoryTrim {
		if err := c.historyTrim.TrimRelHistoryFrom(id, snap.historyTrimFrom); err != nil {
			return err
		}
	} else if err := c.store.TruncateRelHistory(id, 0); err != nil {
		return err
	}
	for _, r := range snap.history {
		if err := c.store.PutRelVersion(id, r.Version(), r); err != nil {
			return err
		}
	}
	return nil
}

func (rb *importRollback) restoreRegistries() error {
	labels := registrypkg.NewLabelRegistry()
	if err := labels.ImportNames(rb.labels); err != nil {
		return err
	}
	relTypes := registrypkg.NewRelTypeRegistry()
	if err := relTypes.ImportNames(rb.relTypes); err != nil {
		return err
	}
	rb.c.labels = labels
	rb.c.relTypes = relTypes
	rb.c.clearRelTypeCache()
	setTieredLabelRegistryIfSupported(rb.c.store, rb.c.labels)
	return rb.c.persistRegistries()
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
	got, err := storeutil.NodeToWireChecked(existing)
	if err != nil {
		return false
	}
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
	got, err := storeutil.RelToWireChecked(existing)
	if err != nil {
		return false
	}
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

func (c *Core) validatePropertySliceLimits(props types.PropertySlice) error {
	if len(props) > c.validation.MaxPropertiesPerEntity {
		return fmt.Errorf("%w: %d > %d", ErrTooManyProperties, len(props), c.validation.MaxPropertiesPerEntity)
	}
	for _, p := range props {
		if err := c.validatePropertyEntryLimits(p.Key, p.Value); err != nil {
			return err
		}
	}
	return nil
}

// validateNodeTokensInRegistry rejects node records whose label tokens do
// not map to a registered name in lr. storeutil.WireToNodeChecked already
// proved that tokens are non-zero and fit a uint16; this layer proves they were
// actually issued by the registry that accompanied the export. Without
// the check, a corrupt or hostile stream could embed token N where the
// registry only registered M < N labels — the import would succeed and
// every label-based query against the imported entity would silently
// return an empty string instead of the intended label.
//
// Valid tokens are [1, lr.Len()] (token 0 is reserved; Len excludes it).
func validateNodeTokensInRegistry(w *storeutil.NodeWire, lr *registrypkg.LabelRegistry) error {
	max := lr.Len()
	if w.PrimaryLabel > max {
		return fmt.Errorf("%w: primary label token %d not in registry (size %d)", ErrCorruptExport, w.PrimaryLabel, max)
	}
	for i, t := range w.ExtraLabels {
		if t > max {
			return fmt.Errorf("%w: extra label[%d] token %d not in registry (size %d)", ErrCorruptExport, i, t, max)
		}
	}
	return nil
}

// validateRelTokensInRegistry mirrors validateNodeTokensInRegistry for
// relationships. storeutil.WireToRelChecked already proved the token is
// non-zero and fits a uint16; this layer proves it lies within the registry's
// issued range, otherwise type-based queries against the imported edge would
// resolve to an empty string.
func validateRelTokensInRegistry(w *storeutil.RelWire, rr *registrypkg.RelTypeRegistry) error {
	max := rr.Len()
	if w.RelType > max {
		return fmt.Errorf("%w: rel type token %d not in registry (size %d)", ErrCorruptExport, w.RelType, max)
	}
	return nil
}
