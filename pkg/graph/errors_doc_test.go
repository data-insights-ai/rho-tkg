package graph_test

import (
	"bufio"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// ---- Source 1: pkg/graph re-exports -----------------------------------
// Canonical lists of exported sentinels. These must match the exports in
// pkg/graph/errors.go exactly. Organized by category (same grouping as
// errors.go and docs/errors.md). Package-level (not function-local) so both
// TestErrorsDocumentation (the docs/errors.md row-presence check) and
// TestGraphErrorsFileInventoryComplete (the go/parser sweep of errors.go
// below) consult exactly the SAME inventory — a re-export added to one check
// and not the other can no longer happen.

// Store entity sentinels
var storeEntitySentinels = []string{
	"ErrNodeNotFound",
	"ErrRelNotFound",
	"ErrNodeExists",
	"ErrRelExists",
}

// Store index sentinels
var storeIndexSentinels = []string{
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
var indexProviderSentinels = []string{
	"ErrIndexProviderExists",
	"ErrIndexProviderNotFound",
	"ErrIndexProviderEmptyName",
	"ErrOrderedScanTemporal",
	"ErrRelPropertyIndexUnsupported",
}

// Registry sentinels
var registrySentinels = []string{
	"ErrEmptyName",
	"ErrRegistryNotEmpty",
}

// Core node/rel/entity sentinels
var entitySentinels = []string{
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
var contextSentinels = []string{
	"ErrNilContext",
	"ErrNilTxCallback",
}

// Core validation sentinels
var validationSentinels = []string{
	"ErrInvalidTimeRange",
	"ErrTooManyLabels",
	"ErrTooManyProperties",
	"ErrKeyTooLong",
	"ErrValueTooLarge",
	"ErrNameTooLong",
}

// Core label/rel sentinels
var labelRelSentinels = []string{
	"ErrLabelNotFound",
	"ErrLastLabel",
	"ErrSelfLoop",
}

// Core batch sentinels
var batchSentinels = []string{
	"ErrBatchFailed",
	"ErrBatchDone",
}

// Cross-machine (foreign-endpoint) edge sentinels (ADR-0010).
var foreignEndpointSentinels = []string{
	"ErrForeignEndpointUnsupported",
	"ErrForeignEndpointConstraint",
	"ErrInvalidForeignEndpoint",
}

// Temporal sentinels. ErrNoVersionValidAt is the store-canonical sentinel
// newly aliased into pkg/graph/errors.go — it used to be store-only (see
// storeOnlySentinels' history note below) because it leaked raw from
// g.Temporal().NodeAt / RelAt / NodeAtTx / RelAtTx with no graph.ErrXxx
// alias; it now has one, like ErrNodeExists before it.
var temporalSentinels = []string{
	"ErrValidFromBeforePrevious",
	"ErrNoVersionAsOf",
	"ErrConflictingTemporalOpts",
	"ErrNoVersionValidAt",
	"ErrInvalidClockAdvance",
}

// Transaction-time backfill sentinels (§4.1)
var backfillSentinels = []string{
	"ErrTxBackfillDisabled",
	"ErrInvalidTxFrom",
}

// Named as-of tags sentinels (§4.2)
var asOfTagsSentinels = []string{
	"ErrInvalidAsOfTag",
	"ErrTooManyAsOfTags",
}

// Unique property constraint sentinels (ADR-0002)
var uniqueConstraintSentinels = []string{
	"ErrUniqueViolation",
	"ErrUniqueViolationExisting",
	"ErrUniqueConstraintExists",
	"ErrUniqueConstraintNotFound",
	"ErrUniqueUnsupportedType",
	"ErrUniqueEventLabelUnsupported",
}

// History retention & compaction (ADR-0001)
var compactionSentinels = []string{
	"ErrHistoryCompacted",
	"ErrCompactionProtectedTag",
	"ErrInvalidRetentionPolicy",
	"ErrCompactionChangeLogEnabled",
}

// Retention purge (ADR-0008)
var retentionSentinels = []string{
	"ErrRetentionExpired",
	"ErrRetentionPurgeDisabled",
	"ErrRetentionPurgeChangeLogEnabled",
	"ErrInvalidPurgePolicy",
}

// Admin.Reset safety valve (BACKLOG 13d)
var adminResetSentinels = []string{
	"ErrResetDisabled",
}

// IO sentinels
var ioSentinels = []string{
	"ErrNilReader",
	"ErrNilWriter",
	"ErrIncompatibleExport",
	"ErrIncompatibleRegistry",
	"ErrCorruptExport",
	"ErrImportSizeLimit",
}

// Backup ergonomics sentinel(s) — BackupTo / BackupDeltaTo, aliased from
// pkg/graph/io into pkg/graph/errors.go. Previously declared in errors.go
// but absent from this inventory (and therefore from docs/errors.md) — the
// one gap TestGraphErrorsFileInventoryComplete below now catches.
var backupSentinels = []string{
	"ErrBackupExists",
}

// Delta export/merge sentinels
var deltaSentinels = []string{
	"ErrCursorUnknown",
	"ErrDeltaBaseMismatch",
}

// Replication sentinels
var replicationSentinels = []string{
	"ErrPrimaryRegistryStale",
	"ErrRegistryDiverged",
}

// Capabilities and format sentinels
var capabilitySentinels = []string{
	"ErrCapabilityNotSupported",
	"ErrWireFormatVersionUnsupported",
	"ErrHistoryAnchorIntervalMismatch",
	"ErrTxDone",
}

// TieredStore reference/event ontology sentinels (ADR-0007). BACKLOG 7c:
// these three are declared in pkg/graph/store/tiered and reachable through
// THREE different sub-APIs (Tier/Index/Nodes), so pkg/graph/errors.go
// re-exports them centrally rather than any single sub-API package growing
// its own errors.go for them.
var tieredOntologySentinels = []string{
	"ErrNotReferenceEntity",
	"ErrEventPropertyIndex",
	"ErrPrimaryLabelClassMutation",
}

// Ingest pipeline sentinels (ADR-0006). BACKLOG 7b: ErrNilSession was
// previously treated as an internal-core-only guard and deliberately
// excluded here — that was wrong. ingest.Session is a TYPE ALIAS for
// core.Session (pkg/graph/ingest/api.go: `type Session = core.Session`), not
// a wrapper, so a pkg/graph caller holding a nil *ingest.Session reaches
// core.Session's own nil-receiver guard (every exported Session method calls
// lockOpen(), which returns ErrNilSession for a nil receiver) directly
// through the public surface. Both sentinels are re-exported.
var ingestSentinels = []string{
	"ErrIngestClosed",
	"ErrNilSession",
}

// Temporal constraints (from pkg/graph/temporal). NOT part of
// graphReexportSentinelNames() below: these are declared directly in
// pkg/graph/temporal (a sibling sub-API package reachable as
// temporal.ErrXxx), never aliased into pkg/graph/errors.go itself, so
// TestGraphErrorsFileInventoryComplete's parse of errors.go will never see
// them. Still part of the docs/errors.md row-presence check below.
var constraintSentinels = []string{
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
//
//	grep -oP '^\s*Err[A-Z][A-Za-z0-9]*(?=\s*=)' pkg/graph/store/errors.go
//
// against the re-exports pulled from storepkg in pkg/graph/errors.go —
// anything present in the former and absent from the latter belongs here.
// ErrCorruptWire is deliberately grouped with the wire-format sentinel it
// sits next to in docs/errors.md's "Integrity & Wire" section rather than
// with its store-only siblings, but it is equally store-only.
var storeOnlySentinels = []string{
	"ErrStoreClosed",
	"ErrVersionNotFound",
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
var typesSentinels = []string{
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

// graphReexportSentinelNames returns every sentinel name that IS aliased
// into pkg/graph/errors.go — i.e. everything reachable via a single
// `graph.ErrXxx` qualifier. constraintSentinels, storeOnlySentinels, and
// typesSentinels are deliberately excluded: none of those three lists are
// declared in pkg/graph/errors.go itself (see each list's own comment).
//
// This is the single source of truth TestGraphErrorsFileInventoryComplete
// checks a go/parser sweep of errors.go against, closing the gap class
// where a future re-export (like ErrBackupExists before this test existed)
// lands in errors.go without ever being added here — and therefore without
// ever being checked against docs/errors.md by TestErrorsDocumentation.
func graphReexportSentinelNames() []string {
	var all []string
	all = append(all, storeEntitySentinels...)
	all = append(all, storeIndexSentinels...)
	all = append(all, indexProviderSentinels...)
	all = append(all, registrySentinels...)
	all = append(all, entitySentinels...)
	all = append(all, contextSentinels...)
	all = append(all, validationSentinels...)
	all = append(all, labelRelSentinels...)
	all = append(all, batchSentinels...)
	all = append(all, foreignEndpointSentinels...)
	all = append(all, temporalSentinels...)
	all = append(all, backfillSentinels...)
	all = append(all, asOfTagsSentinels...)
	all = append(all, uniqueConstraintSentinels...)
	all = append(all, compactionSentinels...)
	all = append(all, retentionSentinels...)
	all = append(all, adminResetSentinels...)
	all = append(all, ioSentinels...)
	all = append(all, backupSentinels...)
	all = append(all, deltaSentinels...)
	all = append(all, replicationSentinels...)
	all = append(all, capabilitySentinels...)
	all = append(all, ingestSentinels...)
	all = append(all, tieredOntologySentinels...)
	return all
}

// TestErrorsDocumentation verifies that docs/errors.md documents sentinel
// errors from all three inventoried sources:
//  1. pkg/graph re-exports (aliased into pkg/graph/errors.go)
//  2. pkg/graph/store-only sentinels (declared in store, no pkg/graph alias)
//  3. pkg/types sentinels (declared in pkg/types, some also aliased into
//     pkg/graph/internal/core and therefore into source 1)
//
// The list below is the canonical inventory, maintained by hand alongside the
// three declaring files (as the package-level *Sentinels vars above). The
// test parses docs/errors.md and asserts every name appears in column 1 of
// at least one row of the machine-checkable table. Because the check is
// name-based (not package-qualified), a sentinel whose bare identifier is
// documented under two different sources (e.g. pkg/types.ErrNilNode, which
// is also re-exported as graph.ErrNilNode) is satisfied by either row — see
// the per-list comments above for the two deliberate name collisions this
// file knows about.
func TestErrorsDocumentation(t *testing.T) {
	allList := make([]string, 0)
	allList = append(allList, graphReexportSentinelNames()...)
	allList = append(allList, constraintSentinels...)
	allList = append(allList, storeOnlySentinels...)
	allList = append(allList, typesSentinels...)

	allSentinels := make(map[string]bool, len(allList))
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

// TestGraphErrorsFileInventoryComplete closes the documentation-gap CLASS
// that let ErrBackupExists sit in pkg/graph/errors.go undocumented: it
// parses errors.go directly with go/parser (the file is small and stable —
// a flat sequence of package-level var blocks, no build tags or generated
// code) and asserts every exported Err*-prefixed variable declared there
// appears in graphReexportSentinelNames() above. That function's result
// feeds directly into TestErrorsDocumentation's docs/errors.md row check, so
// a re-export added to errors.go but never added to the inventory now fails
// HERE — at the source of truth — instead of silently passing every other
// check and only ever being caught by a human noticing a missing doc row.
func TestGraphErrorsFileInventoryComplete(t *testing.T) {
	fset := token.NewFileSet()
	astFile, err := parser.ParseFile(fset, "errors.go", nil, 0)
	if err != nil {
		t.Fatalf("failed to parse errors.go: %v", err)
	}

	inventory := make(map[string]bool, len(graphReexportSentinelNames()))
	for _, name := range graphReexportSentinelNames() {
		inventory[name] = true
	}

	var declared []string
	for _, decl := range astFile.Decls {
		genDecl, ok := decl.(*ast.GenDecl)
		if !ok || genDecl.Tok != token.VAR {
			continue
		}
		for _, spec := range genDecl.Specs {
			valueSpec, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			for _, name := range valueSpec.Names {
				if !name.IsExported() || !strings.HasPrefix(name.Name, "Err") {
					continue
				}
				declared = append(declared, name.Name)
			}
		}
	}

	if len(declared) == 0 {
		t.Fatal("no exported Err*-prefixed var declarations found in errors.go — go/parser regression, or the file was restructured away from plain var blocks (update this test's walk accordingly)")
	}

	for _, name := range declared {
		if !inventory[name] {
			t.Errorf("pkg/graph/errors.go declares %s but it is missing from graphReexportSentinelNames() in errors_doc_test.go — add it to the appropriate *Sentinels list there (and a docs/errors.md row) so TestErrorsDocumentation actually checks it", name)
		}
	}
}
