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
	if err := c.checkOpen(); err != nil {
		return err
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
		if err := validateNodeTokensInRegistry(&wn, c.labels); err != nil {
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
		if err := validateNodeTokensInRegistry(&wn, c.labels); err != nil {
			return fmt.Errorf("import: node history %d: %w", wn.ID, err)
		}
		n := storeutil.WireToNode(wn)
		id := types.NodeID(wn.ID) //nolint:gosec — ID from our own serialization
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
			return fmt.Errorf("import: unmarshal rel: %w", err)
		}
		if err := validateRelWire(&wr); err != nil {
			return fmt.Errorf("import: rel %d: %w", wr.ID, err)
		}
		if err := validateRelTokensInRegistry(&wr, c.relTypes); err != nil {
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
		if err := validateRelTokensInRegistry(&wr, c.relTypes); err != nil {
			return fmt.Errorf("import: rel history %d: %w", wr.ID, err)
		}
		rel := storeutil.WireToRel(wr)
		id := types.RelID(wr.ID) //nolint:gosec — ID from our own serialization
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
	if int(w.PrimaryLabel) > max {
		return fmt.Errorf("%w: primary label token %d not in registry (size %d)", ErrCorruptExport, w.PrimaryLabel, max)
	}
	for i, t := range w.ExtraLabels {
		if int(t) > max {
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
	if int(w.RelType) > max {
		return fmt.Errorf("%w: rel type token %d not in registry (size %d)", ErrCorruptExport, w.RelType, max)
	}
	return nil
}
