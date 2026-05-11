package core

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"

	"github.com/vmihailenco/msgpack/v5"
	registrypkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/registry"
	storeutil "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/storeutil"
	tkgio "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/io"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/store/tiered"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// ErrImportSizeLimit is the canonical IO size-cap sentinel — declared in
// pkg/graph/io and aliased here so internal references stay on the short
// identifier (R4-F8).
var (
	ErrImportSizeLimit = tkgio.ErrImportSizeLimit
	ErrNilReader       = tkgio.ErrNilReader
)

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
func (o *IOOps) ImportWithOptions(r io.Reader, opts tkgio.ImportOptions) error {
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
		if importStageCapExceeded(staged, recordSize, opts.MaxStagedBytes) {
			return fmt.Errorf("%w: staged %d bytes + record %d bytes > cap %d", ErrImportSizeLimit, staged, recordSize, opts.MaxStagedBytes)
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
	if c.closed.Load() {
		return ErrGraphClosed
	}
	rollback := newImportRollback(c)
	if err := c.importReplayLocked(br, rollback); err != nil {
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

// importReplayLocked replays staged records. Caller must hold c.mu.Lock.
func (c *Core) importReplayLocked(br *bufio.Reader, rollback *importRollback) error {
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
		tag, data, rerr := readExportRecord(br)
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
			if err := msgpack.Unmarshal(data, &hdr); err != nil {
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
			seenHeader = true

		case exportTagRegistry:
			if !seenHeader {
				return fmt.Errorf("%w: registry record before header", ErrCorruptExport)
			}
			if seenRegistry {
				return fmt.Errorf("%w: duplicate registry record", ErrCorruptExport)
			}
			var reg tiered.RegistryFileData
			if err := msgpack.Unmarshal(data, &reg); err != nil {
				return fmt.Errorf("%w: unmarshal registry: %v", ErrCorruptExport, err)
			}
			if err := c.validateRegistryNames("label", reg.Labels); err != nil {
				return fmt.Errorf("import: label registry: %w", err)
			}
			if err := c.validateRegistryNames("reltype", reg.RelTypes); err != nil {
				return fmt.Errorf("import: reltype registry: %w", err)
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
		if err := msgpack.Unmarshal(data, &wn); err != nil {
			return fmt.Errorf("%w: unmarshal node: %v", ErrCorruptExport, err)
		}
		if err := validateNodeWire(&wn); err != nil {
			return fmt.Errorf("import: node %d: %w", wn.ID, err)
		}
		if err := validateNodeTokensInRegistry(&wn, c.labels); err != nil {
			return fmt.Errorf("import: node %d: %w", wn.ID, err)
		}
		if err := c.validatePropertyWireLimits(wn.Properties); err != nil {
			return fmt.Errorf("import: node %d: %w", wn.ID, err)
		}
		n, err := storeutil.WireToNodeChecked(wn)
		if err != nil {
			return fmt.Errorf("import: node %d: %w: %v", wn.ID, ErrCorruptExport, err)
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
			return fmt.Errorf("%w: unmarshal node history: %v", ErrCorruptExport, err)
		}
		if err := validateNodeWire(&wn); err != nil {
			return fmt.Errorf("import: node history %d: %w", wn.ID, err)
		}
		if err := validateNodeTokensInRegistry(&wn, c.labels); err != nil {
			return fmt.Errorf("import: node history %d: %w", wn.ID, err)
		}
		if err := c.validatePropertyWireLimits(wn.Properties); err != nil {
			return fmt.Errorf("import: node history %d: %w", wn.ID, err)
		}
		n, err := storeutil.WireToNodeChecked(wn)
		if err != nil {
			return fmt.Errorf("import: node history %d: %w: %v", wn.ID, ErrCorruptExport, err)
		}
		id := types.NodeID(wn.ID) //nolint:gosec — ID from our own serialization
		if err := seen.recordNodeHist(id, n.Version()); err != nil {
			return err
		}
		if err := rollback.captureNode(id); err != nil {
			return fmt.Errorf("import: snapshot node history %d: %w", wn.ID, err)
		}
		// R5-F4: history records carry the same idempotent /
		// conflict-rejection contract as current entities (R4-F12).
		// PutNodeVersion silently overwrites, so without this check a
		// re-import with diverging history would replace the existing
		// version snapshot in place — exactly the hybrid-graph hazard
		// R4-F12 closed for current entities.
		existing, gerr := c.store.GetNodeVersion(id, n.Version())
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
		if err := msgpack.Unmarshal(data, &wr); err != nil {
			return fmt.Errorf("%w: unmarshal rel: %v", ErrCorruptExport, err)
		}
		if err := validateRelWire(&wr); err != nil {
			return fmt.Errorf("import: rel %d: %w", wr.ID, err)
		}
		if err := validateRelTokensInRegistry(&wr, c.relTypes); err != nil {
			return fmt.Errorf("import: rel %d: %w", wr.ID, err)
		}
		if err := c.validatePropertyWireLimits(wr.Properties); err != nil {
			return fmt.Errorf("import: rel %d: %w", wr.ID, err)
		}
		rel, err := storeutil.WireToRelChecked(wr)
		if err != nil {
			return fmt.Errorf("import: rel %d: %w: %v", wr.ID, ErrCorruptExport, err)
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
			return fmt.Errorf("%w: unmarshal rel history: %v", ErrCorruptExport, err)
		}
		if err := validateRelWire(&wr); err != nil {
			return fmt.Errorf("import: rel history %d: %w", wr.ID, err)
		}
		if err := validateRelTokensInRegistry(&wr, c.relTypes); err != nil {
			return fmt.Errorf("import: rel history %d: %w", wr.ID, err)
		}
		if err := c.validatePropertyWireLimits(wr.Properties); err != nil {
			return fmt.Errorf("import: rel history %d: %w", wr.ID, err)
		}
		rel, err := storeutil.WireToRelChecked(wr)
		if err != nil {
			return fmt.Errorf("import: rel history %d: %w: %v", wr.ID, ErrCorruptExport, err)
		}
		id := types.RelID(wr.ID) //nolint:gosec — ID from our own serialization
		if err := seen.recordRelHist(id, rel.Version()); err != nil {
			return err
		}
		if err := rollback.captureRel(id); err != nil {
			return fmt.Errorf("import: snapshot rel history %d: %w", wr.ID, err)
		}
		// R5-F4: same idempotent / conflict-rejection contract as
		// rel current entities (R4-F12). PutRelVersion silently
		// overwrites, so a diverging re-import would replace the
		// version in place without this guard.
		existing, gerr := c.store.GetRelVersion(id, rel.Version())
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

	nodes     map[types.NodeID]importNodeSnapshot
	nodeOrder []types.NodeID
	rels      map[types.RelID]importRelSnapshot
	relOrder  []types.RelID
}

type importNodeSnapshot struct {
	current *types.Node
	history []*types.Node
}

type importRelSnapshot struct {
	current *types.Relationship
	history []*types.Relationship
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

func (rb *importRollback) captureNode(id types.NodeID) error {
	if _, ok := rb.nodes[id]; ok {
		return nil
	}

	var current *types.Node
	n, err := rb.c.store.GetNode(id)
	switch {
	case err == nil:
		current = n.DeepCopy()
	case errors.Is(err, storepkg.ErrNodeNotFound):
	default:
		return err
	}

	history, err := copyNodeHistory(rb.c.store.GetNodeHistory(id))
	if err != nil {
		return err
	}

	rb.nodes[id] = importNodeSnapshot{current: current, history: history}
	rb.nodeOrder = append(rb.nodeOrder, id)
	return nil
}

func (rb *importRollback) captureRel(id types.RelID) error {
	if _, ok := rb.rels[id]; ok {
		return nil
	}

	var current *types.Relationship
	rel, err := rb.c.store.GetRelationship(id)
	switch {
	case err == nil:
		current = rel.DeepCopy()
	case errors.Is(err, storepkg.ErrRelNotFound):
	default:
		return err
	}

	history, err := copyRelHistory(rb.c.store.GetRelHistory(id))
	if err != nil {
		return err
	}

	rb.rels[id] = importRelSnapshot{current: current, history: history}
	rb.relOrder = append(rb.relOrder, id)
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
			if _, err := rb.c.store.GetRelationship(id); errors.Is(err, storepkg.ErrRelNotFound) {
				capture(rb.c.store.PutRelationship(snap.current))
			} else if err != nil {
				capture(err)
			} else {
				capture(rb.c.store.ReplaceRelationship(snap.current))
			}
		}
		capture(restoreImportRelHistory(rb.c, id, snap.history))
	}

	for i := len(rb.nodeOrder) - 1; i >= 0; i-- {
		id := rb.nodeOrder[i]
		snap := rb.nodes[id]
		if snap.current == nil {
			if err := rb.c.store.DeleteNodeCascade(id); err != nil && !errors.Is(err, storepkg.ErrNodeNotFound) {
				capture(err)
			}
		} else {
			if _, err := rb.c.store.GetNode(id); errors.Is(err, storepkg.ErrNodeNotFound) {
				capture(rb.c.store.PutNode(snap.current))
			} else if err != nil {
				capture(err)
			} else {
				capture(rb.c.store.ReplaceNode(snap.current))
			}
		}
		capture(restoreImportNodeHistory(rb.c, id, snap.history))
	}

	capture(rb.restoreRegistries())
	return firstErr
}

func restoreImportNodeHistory(c *Core, id types.NodeID, history []*types.Node) error {
	if err := c.store.TruncateNodeHistory(id, 0); err != nil {
		return err
	}
	for _, n := range history {
		if err := c.store.PutNodeVersion(id, n.Version(), n); err != nil {
			return err
		}
	}
	return nil
}

func restoreImportRelHistory(c *Core, id types.RelID, history []*types.Relationship) error {
	if err := c.store.TruncateRelHistory(id, 0); err != nil {
		return err
	}
	for _, r := range history {
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
	if ts, ok := rb.c.store.(*tiered.Store); ok {
		ts.SetLabelRegistry(rb.c.labels)
	}
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

func (c *Core) validatePropertyWireLimits(props []storeutil.PropertyWire) error {
	if len(props) > c.validation.MaxPropertiesPerEntity {
		return fmt.Errorf("%w: %d > %d", ErrTooManyProperties, len(props), c.validation.MaxPropertiesPerEntity)
	}
	for _, p := range props {
		if err := c.validatePropertyEntry(p.Key, p.Value); err != nil {
			return err
		}
	}
	return nil
}

const maxWireVersion = int64(1<<32 - 1)

// validateNodeWire defends the import boundary against malformed node records.
// ImportGraph reads from an arbitrary io.Reader, so the wire shape is checked
// before any entity is constructed or installed into a store.
//
// Returns ErrCorruptExport (wrapped with detail) on any structural violation:
//   - ID <= 0 (zero is the API sentinel; negative IDs violate the snowflake sign bit)
//   - Version outside [0, math.MaxUint32]
//   - PrimaryLabel == 0 (token 0 is reserved)
//   - PrimaryLabel outside [1, 65535] (does not fit a uint16 token)
//   - any ExtraLabels element == 0, outside [1, 65535], duplicated, or equal to the primary
//   - BaseEntityID < 0
//   - malformed property slices (reserved shadow keys, unsorted/duplicate keys, invalid values)
func validateNodeWire(w *storeutil.NodeWire) error {
	if w.ID <= 0 {
		return fmt.Errorf("%w: node id must be positive, got %d", ErrCorruptExport, w.ID)
	}
	if w.Version < 0 || int64(w.Version) > maxWireVersion {
		return fmt.Errorf("%w: node version %d outside uint32 range", ErrCorruptExport, w.Version)
	}
	if w.PrimaryLabel == 0 {
		return fmt.Errorf("%w: primary label token 0 is reserved", ErrCorruptExport)
	}
	if w.PrimaryLabel < 0 || w.PrimaryLabel > 65535 {
		return fmt.Errorf("%w: primary label token %d out of uint16 range", ErrCorruptExport, w.PrimaryLabel)
	}
	seen := make(map[int]struct{}, len(w.ExtraLabels))
	for i, t := range w.ExtraLabels {
		if t == 0 {
			return fmt.Errorf("%w: extra label[%d] token 0 is reserved", ErrCorruptExport, i)
		}
		if t < 0 || t > 65535 {
			return fmt.Errorf("%w: extra label[%d] token %d out of uint16 range", ErrCorruptExport, i, t)
		}
		if t == w.PrimaryLabel {
			return fmt.Errorf("%w: extra label[%d] duplicates primary label token %d", ErrCorruptExport, i, t)
		}
		if _, ok := seen[t]; ok {
			return fmt.Errorf("%w: extra label[%d] duplicates token %d", ErrCorruptExport, i, t)
		}
		seen[t] = struct{}{}
	}
	if w.BaseEntityID < 0 {
		return fmt.Errorf("%w: base entity id must be non-negative, got %d", ErrCorruptExport, w.BaseEntityID)
	}
	if err := storeutil.ValidatePropertyWireSlice(w.Properties); err != nil {
		return fmt.Errorf("%w: properties: %v", ErrCorruptExport, err)
	}
	return nil
}

// validateRelWire defends the import boundary against malformed relationship
// records before construction.
func validateRelWire(w *storeutil.RelWire) error {
	if w.ID <= 0 {
		return fmt.Errorf("%w: relationship id must be positive, got %d", ErrCorruptExport, w.ID)
	}
	if w.StartID <= 0 {
		return fmt.Errorf("%w: relationship start id must be positive, got %d", ErrCorruptExport, w.StartID)
	}
	if w.EndID <= 0 {
		return fmt.Errorf("%w: relationship end id must be positive, got %d", ErrCorruptExport, w.EndID)
	}
	if w.Version < 0 || int64(w.Version) > maxWireVersion {
		return fmt.Errorf("%w: relationship version %d outside uint32 range", ErrCorruptExport, w.Version)
	}
	if w.RelType == 0 {
		return fmt.Errorf("%w: rel type token 0 is reserved", ErrCorruptExport)
	}
	if w.RelType < 0 || w.RelType > 65535 {
		return fmt.Errorf("%w: rel type token %d out of uint16 range", ErrCorruptExport, w.RelType)
	}
	if w.BaseEntityID < 0 {
		return fmt.Errorf("%w: base entity id must be non-negative, got %d", ErrCorruptExport, w.BaseEntityID)
	}
	if err := storeutil.ValidatePropertyWireSlice(w.Properties); err != nil {
		return fmt.Errorf("%w: properties: %v", ErrCorruptExport, err)
	}
	return nil
}

// validateNodeTokensInRegistry rejects node records whose label tokens do
// not map to a registered name in lr. validateNodeWire already proved that
// tokens are non-zero and fit a uint16; this layer proves they were
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
// relationships. The reltype token must lie within the registry's issued
// range, otherwise type-based queries against the imported edge would
// resolve to an empty string.
func validateRelTokensInRegistry(w *storeutil.RelWire, rr *registrypkg.RelTypeRegistry) error {
	max := rr.Len()
	if w.RelType > max {
		return fmt.Errorf("%w: rel type token %d not in registry (size %d)", ErrCorruptExport, w.RelType, max)
	}
	return nil
}
