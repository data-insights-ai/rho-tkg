package graph_test

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestErrorsDocumentation verifies that docs/errors.md documents sentinel
// errors from all three inventoried sources:
//  1. pkg/graph re-exports (aliased into pkg/graph/errors.go)
//  2. pkg/graph/store-only sentinels (declared in store, no pkg/graph alias)
//  3. pkg/types sentinels (declared in pkg/types, some also aliased into
//     pkg/graph/internal/core and therefore into source 1)
//
// The list below is the canonical inventory, maintained by hand alongside the
// three declaring files. The test parses docs/errors.md and asserts every
// name appears in column 1 of at least one row of the machine-checkable
// table. Because the check is name-based (not package-qualified), a sentinel
// whose bare identifier is documented under two different sources (e.g.
// pkg/types.ErrNilNode, which is also re-exported as graph.ErrNilNode) is
// satisfied by either row — see the per-list comments below for the two
// deliberate name collisions this file knows about.
func TestErrorsDocumentation(t *testing.T) {
	// ---- Source 1: pkg/graph re-exports -----------------------------------
	// Canonical list of exported sentinels. These must match the exports in
	// pkg/graph/errors.go exactly. Organized by category (same grouping as
	// errors.go and docs/errors.md).

	// Store entity sentinels
	storeEntitySentinels := []string{
		"ErrNodeNotFound",
		"ErrRelNotFound",
		"ErrNodeExists",
		"ErrRelExists",
	}

	// Store index sentinels
	storeIndexSentinels := []string{
		"ErrIndexExists",
		"ErrIndexNotFound",
		"ErrTemporalIndexExists",
		"ErrTemporalIndexNotFound",
		"ErrVectorIndexExists",
		"ErrVectorIndexNotFound",
		"ErrDimensionMismatch",
		"ErrInvalidTemporalIndexConfig",
		"ErrInvalidVectorIndexConfig",
		"ErrInvalidVectorValue",
		"ErrInvalidShardDepth",
		"ErrInvalidQueryLimit",
		"ErrInvalidQueryCursor",
	}

	// Index provider sentinels
	indexProviderSentinels := []string{
		"ErrIndexProviderExists",
		"ErrIndexProviderNotFound",
		"ErrIndexProviderEmptyName",
	}

	// Registry sentinels
	registrySentinels := []string{
		"ErrEmptyName",
		"ErrRegistryNotEmpty",
	}

	// Core node/rel/entity sentinels
	entitySentinels := []string{
		"ErrNoLabels",
		"ErrNilNode",
		"ErrNilRelationship",
		"ErrZeroID",
		"ErrInvalidID",
		"ErrVersionOverflow",
		"ErrNotTieredStore",
		"ErrAlreadyClosed",
		"ErrGraphClosed",
		"ErrReadOnlyReplica",
		"ErrNilGraph",
		"ErrNilStore",
	}

	// Core context/tx/batch sentinels
	contextSentinels := []string{
		"ErrNilContext",
		"ErrNilTxCallback",
	}

	// Core validation sentinels
	validationSentinels := []string{
		"ErrInvalidTimeRange",
		"ErrTooManyLabels",
		"ErrTooManyProperties",
		"ErrKeyTooLong",
		"ErrValueTooLarge",
		"ErrNameTooLong",
	}

	// Core label/rel sentinels
	labelRelSentinels := []string{
		"ErrLabelNotFound",
		"ErrLastLabel",
		"ErrSelfLoop",
	}

	// Core batch sentinels
	batchSentinels := []string{
		"ErrBatchFailed",
		"ErrBatchDone",
	}

	// Temporal sentinels
	temporalSentinels := []string{
		"ErrValidFromBeforePrevious",
		"ErrNoVersionAsOf",
		"ErrConflictingTemporalOpts",
	}

	// Transaction-time backfill sentinels (§4.1)
	backfillSentinels := []string{
		"ErrTxBackfillDisabled",
		"ErrInvalidTxFrom",
	}

	// Named as-of tags sentinels (§4.2)
	asOfTagsSentinels := []string{
		"ErrInvalidAsOfTag",
		"ErrTooManyAsOfTags",
	}

	// Unique property constraint sentinels (ADR-0002)
	uniqueConstraintSentinels := []string{
		"ErrUniqueViolation",
		"ErrUniqueViolationExisting",
		"ErrUniqueConstraintExists",
		"ErrUniqueConstraintNotFound",
		"ErrUniqueUnsupportedType",
		"ErrUniqueEventLabelUnsupported",
	}

	// History retention & compaction (ADR-0001)
	compactionSentinels := []string{
		"ErrHistoryCompacted",
		"ErrCompactionProtectedTag",
		"ErrInvalidRetentionPolicy",
		"ErrCompactionChangeLogEnabled",
	}

	// IO sentinels
	ioSentinels := []string{
		"ErrNilReader",
		"ErrNilWriter",
		"ErrIncompatibleExport",
		"ErrIncompatibleRegistry",
		"ErrCorruptExport",
		"ErrImportSizeLimit",
	}

	// Delta export/merge sentinels
	deltaSentinels := []string{
		"ErrCursorUnknown",
		"ErrDeltaBaseMismatch",
	}

	// Replication sentinels
	replicationSentinels := []string{
		"ErrPrimaryRegistryStale",
		"ErrRegistryDiverged",
	}

	// Capabilities and format sentinels
	capabilitySentinels := []string{
		"ErrCapabilityNotSupported",
		"ErrWireFormatVersionUnsupported",
		"ErrTxDone",
	}

	// Temporal constraints (from pkg/graph/temporal)
	constraintSentinels := []string{
		"ErrTemporalConstraint",
		"ErrInvalidTemporalConstraint",
		"ErrRelBeforeStartNode",
		"ErrRelBeforeEndNode",
		"ErrRelAfterStartNode",
		"ErrRelAfterEndNode",
		"ErrRelExceedsStartNodeValidity",
		"ErrRelExceedsEndNodeValidity",
	}

	// ---- Source 2: pkg/graph/store-only sentinels -------------------------
	// Declared in pkg/graph/store/errors.go but NOT aliased into
	// pkg/graph/errors.go. Regenerate by diffing the two files:
	//   grep -oP '^\s*Err[A-Z][A-Za-z0-9]*(?=\s*=)' pkg/graph/store/errors.go
	// against the re-exports pulled from storepkg in pkg/graph/errors.go —
	// anything present in the former and absent from the latter belongs here.
	// ErrCorruptWire is deliberately grouped with the wire-format sentinel it
	// sits next to in docs/errors.md's "Integrity & Wire" section rather than
	// with its store-only siblings, but it is equally store-only.
	storeOnlySentinels := []string{
		"ErrStoreClosed",
		"ErrVersionNotFound",
		"ErrNoVersionValidAt",
		"ErrInvalidStoreMutation",
		"ErrChangesNotAscending",
		"ErrCorruptWire",
	}

	// ---- Source 3: pkg/types sentinels -------------------------------------
	// Regenerate with: grep -n 'Err[A-Z][A-Za-z0-9]* *=' pkg/types/*.go
	// NOTE on name collisions with Source 1 (both are intentional, documented
	// in docs/errors.md, and harmless to this name-only presence check):
	//   - ErrNilNode / ErrNilRelationship: pkg/types is the CANONICAL
	//     declaration; core and graph alias the same identity verbatim (see
	//     entitySentinels above). One documented row satisfies both list
	//     entries — the doc additionally cross-lists both rows for clarity.
	//   - ErrInvalidTimeRange: pkg/types declares its OWN, DISTINCT sentinel
	//     (recurrence.go) sharing only the bare name with the core/store one
	//     already in validationSentinels above. Because this check is
	//     name-only, it cannot by itself prove BOTH rows are present — see
	//     docs/errors.md's "pkg/types Sentinels" section, which documents the
	//     distinct identity explicitly in prose.
	typesSentinels := []string{
		"ErrNilNode",
		"ErrNilRelationship",
		"ErrNilPropertySlice",
		"ErrFrozenNode",
		"ErrFrozenRelationship",
		"ErrTypeNotHashable",
		"ErrTypeNotDeepCopyable",
		"ErrPropertyTypeNameCollision",
		"ErrOpenInterval",
		"ErrInvalidInterval",
		"ErrReservedPrefix",
		"ErrUnsupportedValueType",
		"ErrUnsupportedMapType",
		"ErrMaxDepthExceeded",
		"ErrInvalidTemporalValue",
		"ErrInvalidTimeRange",
	}

	allSentinels := make(map[string]bool)
	allList := make([]string, 0)
	allList = append(allList, storeEntitySentinels...)
	allList = append(allList, storeIndexSentinels...)
	allList = append(allList, indexProviderSentinels...)
	allList = append(allList, registrySentinels...)
	allList = append(allList, entitySentinels...)
	allList = append(allList, contextSentinels...)
	allList = append(allList, validationSentinels...)
	allList = append(allList, labelRelSentinels...)
	allList = append(allList, batchSentinels...)
	allList = append(allList, temporalSentinels...)
	allList = append(allList, backfillSentinels...)
	allList = append(allList, asOfTagsSentinels...)
	allList = append(allList, uniqueConstraintSentinels...)
	allList = append(allList, compactionSentinels...)
	allList = append(allList, ioSentinels...)
	allList = append(allList, deltaSentinels...)
	allList = append(allList, replicationSentinels...)
	allList = append(allList, capabilitySentinels...)
	allList = append(allList, constraintSentinels...)
	allList = append(allList, storeOnlySentinels...)
	allList = append(allList, typesSentinels...)

	for _, s := range allList {
		allSentinels[s] = false
	}

	// Parse docs/errors.md and verify every sentinel is documented.
	docsPath := filepath.Join("..", "..", "docs", "errors.md")
	f, err := os.Open(docsPath)
	if err != nil {
		t.Fatalf("failed to open docs/errors.md: %v", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	foundCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		// Each row starts with | and contains the backticked sentinel name
		// in the first column (after |). Example: | `ErrNodeNotFound` | store |
		if !strings.HasPrefix(line, "|") {
			continue
		}
		// Split by |, the second element (index 1) is the first column content
		cols := strings.Split(line, "|")
		if len(cols) < 3 {
			continue
		}
		// Extract sentinel name from backticks in first column
		content := strings.TrimSpace(cols[1])
		if strings.HasPrefix(content, "`") && strings.HasSuffix(content, "`") {
			sentinelName := strings.TrimPrefix(strings.TrimSuffix(content, "`"), "`")
			if alreadyFound, found := allSentinels[sentinelName]; found && !alreadyFound {
				allSentinels[sentinelName] = true
				foundCount++
			}
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scanner error: %v", err)
	}

	// Report any undocumented sentinels
	for name, found := range allSentinels {
		if !found {
			t.Errorf("sentinel %s not found in docs/errors.md", name)
		}
	}

	if foundCount == 0 {
		t.Fatal("no sentinels found in docs/errors.md")
	}

	// Quick sanity check: we should have found most of our sentinels
	if foundCount != len(allSentinels) {
		t.Errorf("found %d sentinels but expected %d", foundCount, len(allSentinels))
	}
}
