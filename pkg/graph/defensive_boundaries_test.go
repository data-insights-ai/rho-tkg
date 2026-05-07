package graph

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/vmihailenco/msgpack/v5"
	storepkg "gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/graph/internal/store"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// --- F1: ImportGraph must not panic on malformed records ---
//
// types.NewNode panics on primaryLabel == 0 or any extraLabel == 0.
// types.NewRelationship panics on relType == 0.
// ImportGraph reads from an arbitrary io.Reader (untrusted input). A corrupt or
// malicious export must NOT crash the caller — it must surface a typed error.

// writeImportRecord assembles one tagged length-prefixed record (matching
// writeExportRecord) into buf for use as ImportGraph input.
func writeImportRecord(buf *bytes.Buffer, tag byte, body []byte) {
	var header [5]byte
	header[0] = tag
	binary.BigEndian.PutUint32(header[1:5], uint32(len(body)))
	buf.Write(header[:])
	buf.Write(body)
}

// validImportHeader returns the bytes for a valid header+registry prelude that
// puts ImportGraph into a state where it will attempt to decode a node/rel
// record next.
func validImportPrelude(t *testing.T) []byte {
	t.Helper()
	hdr := exportHeader{
		Version:    exportFormatVersion,
		ExportedAt: 0,
		NodeCount:  0,
		RelCount:   0,
	}
	hdrBody, err := msgpack.Marshal(&hdr)
	if err != nil {
		t.Fatalf("marshal header: %v", err)
	}
	// Index 0 is reserved (token 0 placeholder); ImportNames rejects a
	// non-empty names[0]. Real exports include the reserved slot — match
	// that shape so the prelude is admitted and the test exercises the
	// node/rel record validation, not the registry validation.
	reg := registryFileData{
		Labels:   []string{"", "L1"},
		RelTypes: []string{"", "R1"},
	}
	regBody, err := msgpack.Marshal(&reg)
	if err != nil {
		t.Fatalf("marshal registry: %v", err)
	}

	var buf bytes.Buffer
	writeImportRecord(&buf, exportTagHeader, hdrBody)
	writeImportRecord(&buf, exportTagRegistry, regBody)
	return buf.Bytes()
}

// runImportSafely invokes g.ImportGraph(r) and converts a panic into a t.Fatal.
// The contract is: ImportGraph must surface malformed input as an error, never
// as a panic.
func runImportSafely(t *testing.T, g *Graph, r io.Reader) error {
	t.Helper()
	var importErr error
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				t.Fatalf("ImportGraph panicked on malformed input: %v", rec)
			}
		}()
		importErr = g.ImportGraph(r)
	}()
	return importErr
}

// TestImportGraph_RejectsZeroPrimaryLabel: a corrupt node record with
// primaryLabel = 0 must produce ErrCorruptExport, not a panic from
// types.NewNode("primary label token 0 is reserved").
func TestImportGraph_RejectsZeroPrimaryLabel(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.NodeWire{
		ID:           int64(snowflakeIDForTest()),
		PrimaryLabel: 0, // CORRUPT — token 0 is reserved
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagNode, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for primaryLabel=0, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_RejectsZeroExtraLabel: a corrupt node record with an
// extraLabel of 0 must produce ErrCorruptExport (NewNode panics on extra
// label token 0 too — different code path than primary).
func TestImportGraph_RejectsZeroExtraLabel(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.NodeWire{
		ID:           int64(snowflakeIDForTest()),
		PrimaryLabel: 1,
		ExtraLabels:  []int{2, 0, 3}, // CORRUPT — extra label token 0
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagNode, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for extraLabel=0, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_RejectsZeroRelType: a corrupt relationship record with
// relType = 0 must produce ErrCorruptExport, not a panic from
// types.NewRelationship.
func TestImportGraph_RejectsZeroRelType(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.RelWire{
		ID:      int64(snowflakeIDForTest()),
		RelType: 0, // CORRUPT — token 0 is reserved
		StartID: 100,
		EndID:   200,
		Version: 0,
	})
	if err != nil {
		t.Fatalf("marshal rel: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagRel, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for relType=0, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_RejectsZeroPrimaryLabelInHistory: corruption in node history
// records must also be caught — wireToNode is called for every history entry.
func TestImportGraph_RejectsZeroPrimaryLabelInHistory(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.NodeWire{
		ID:           int64(snowflakeIDForTest()),
		PrimaryLabel: 0,
		Version:      1,
	})
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagNodeHist, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for history primaryLabel=0, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_RejectsZeroRelTypeInHistory: rel history corruption.
func TestImportGraph_RejectsZeroRelTypeInHistory(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.RelWire{
		ID:      int64(snowflakeIDForTest()),
		RelType: 0,
		StartID: 100,
		EndID:   200,
		Version: 1,
	})
	if err != nil {
		t.Fatalf("marshal rel: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagRelHist, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for history relType=0, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_RejectsOutOfRangeLabelToken: a node record whose
// PrimaryLabel value falls outside uint16 (the on-wire token range) must
// be rejected. Without an explicit check, uint16(w.PrimaryLabel) silently
// truncates and a corrupt entity would be admitted under a different label
// token than the producer wrote.
func TestImportGraph_RejectsOutOfRangeLabelToken(t *testing.T) {
	t.Parallel()

	// Token = 70000 doesn't fit in uint16 (max 65535).
	body, err := msgpack.Marshal(&storepkg.NodeWire{
		ID:           int64(snowflakeIDForTest()),
		PrimaryLabel: 70000, // CORRUPT — out of uint16 range
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagNode, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for out-of-range label token, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_RejectsNegativeLabelToken: negative token values are also
// invalid — the wire field is int but tokens are always positive uint16.
func TestImportGraph_RejectsNegativeLabelToken(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.NodeWire{
		ID:           int64(snowflakeIDForTest()),
		PrimaryLabel: -1, // CORRUPT — negative
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagNode, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for negative label token, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// snowflakeIDForTest returns a stable non-zero snowflake-shaped ID for use
// in crafted wire records. The exact value doesn't matter — these tests
// never reach the store layer because validation fires first.
func snowflakeIDForTest() int64 {
	return 1000000
}

// TestImportGraph_RejectsOutOfRangeExtraLabel covers the validator branch
// that rejects out-of-uint16-range tokens in the extra-label list (review
// MEDIUM Q2 — the code path was added by the fix but no test exercised
// it; the primary-label range test at TestImportGraph_RejectsOutOfRangeLabelToken
// only covered the primary-label branch).
func TestImportGraph_RejectsOutOfRangeExtraLabel(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.NodeWire{
		ID:           int64(snowflakeIDForTest()),
		PrimaryLabel: 1,            // valid primary
		ExtraLabels:  []int{70000}, // CORRUPT — out of uint16 range
		Version:      0,
	})
	if err != nil {
		t.Fatalf("marshal node: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagNode, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for out-of-range extra label, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_RejectsOutOfRangeRelType covers the validator branch
// for out-of-uint16-range relType tokens (review MEDIUM Q2 — symmetric
// to the label range test, previously uncovered).
func TestImportGraph_RejectsOutOfRangeRelType(t *testing.T) {
	t.Parallel()

	body, err := msgpack.Marshal(&storepkg.RelWire{
		ID:      int64(snowflakeIDForTest()),
		RelType: 70000, // CORRUPT — out of uint16 range
		StartID: int64(snowflakeIDForTest()) + 1,
		EndID:   int64(snowflakeIDForTest()) + 2,
		Version: 0,
	})
	if err != nil {
		t.Fatalf("marshal rel: %v", err)
	}

	var buf bytes.Buffer
	buf.Write(validImportPrelude(t))
	writeImportRecord(&buf, exportTagRel, body)

	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close() //nolint:errcheck

	importErr := runImportSafely(t, g, &buf)
	if importErr == nil {
		t.Fatal("ImportGraph: expected error for out-of-range rel type, got nil")
	}
	if !errors.Is(importErr, ErrCorruptExport) {
		t.Errorf("ImportGraph: got %v, want ErrCorruptExport", importErr)
	}
}

// TestImportGraph_HappyPathRoundTrip confirms the new validators do NOT
// regress valid imports (review HIGH Q2 — without this regression guard,
// a future tightening of the validator could silently break legitimate
// round-trips and the existing rejection tests wouldn't catch it).
func TestImportGraph_HappyPathRoundTrip(t *testing.T) {
	t.Parallel()

	// Source graph: a Case node, a Signal event, and a relationship.
	src, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New source: %v", err)
	}
	defer src.Close() //nolint:errcheck

	caseNode, err := src.AddNode([]string{"Case", "Tagged"}, map[string]any{"k": "v"})
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalNode, err := src.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	if _, err := src.AddRelationship("RELATES_TO", caseNode, signalNode, nil); err != nil {
		t.Fatalf("AddRelationship: %v", err)
	}

	var buf bytes.Buffer
	if err := src.ExportGraph(&buf); err != nil {
		t.Fatalf("ExportGraph: %v", err)
	}

	// Destination graph: must accept the import and reproduce all entities.
	dst, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		t.Fatalf("New dest: %v", err)
	}
	defer dst.Close() //nolint:errcheck

	if importErr := runImportSafely(t, dst, &buf); importErr != nil {
		t.Fatalf("ImportGraph happy path: %v", importErr)
	}

	got, err := dst.GetNode(caseNode.InternalID())
	if err != nil {
		t.Fatalf("GetNode after import: %v", err)
	}
	if got == nil {
		t.Fatal("imported case node missing")
	}
	gotSig, err := dst.GetNode(signalNode.InternalID())
	if err != nil {
		t.Fatalf("GetNode signal after import: %v", err)
	}
	if gotSig == nil {
		t.Fatal("imported signal node missing")
	}
}

// --- F2: RunRepair Phase 2 must not silently swallow operational errors ---
//
// The bug: r, err := ns.store.GetRelationship(relID); if err != nil { continue }
// This conflates ErrRelNotFound (legitimate skip) with operational errors
// (closed shard, I/O failure, routing failure) — leaving real corruption
// hiding behind a "Repair succeeded" return.
//
// Approach: propagate operational errors from RunRepair so callers can
// distinguish "repair clean" from "repair could not complete safely".

// TestRunRepair_PropagatesOperationalReadError: in Phase 2, RunRepair calls
// ns.store.GetRelationship(relID) for every rel. If that read returns a
// non-ErrRelNotFound error (real I/O failure / closed shard / routing
// failure), the original code silently `continue`s — the repair returns
// success while genuinely needed in/-index repairs were missed.
//
// Expected behaviour after the fix: the operational error surfaces as the
// return error from RunRepair so the caller knows the scan was incomplete.
func TestRunRepair_PropagatesOperationalReadError(t *testing.T) {
	t.Parallel()

	g, ts := newTestTieredGraph(t)

	// Build a cross-shard relationship whose ENTITY lives on the hot
	// event shard (the closeable target). Signal→Case routes:
	//   startShard = hotEventShard (Signal, event-classified)
	//   endShard   = refShard      (Case, reference-classified)
	// PutRelationship Section 12 ordering writes entity+out on startShard
	// (hot event), then in/ on endShard (ref). So the rel ENTITY lives on
	// the hot event shard — which is the shard we'll fault-inject below.
	signalNode, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	caseNode, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	rel, err := g.AddRelationship("LINK", signalNode, caseNode, nil)
	if err != nil {
		t.Fatal(err)
	}

	// Inject an operational-class fault into the rel's entity shard so
	// Phase 2's GetRelationship surfaces a non-ErrRelNotFound error.
	// Pre-fix code did `if err != nil { continue }` and silently swallowed
	// this — returning RepairResult success while genuinely needed in/-index
	// repairs were missed. Post-fix code must propagate.
	//
	// We cannot swap the BadgerStore wholesale (RunRepair calls non-Store
	// methods on the concrete *BadgerStore). The chosen fault: corrupt the
	// rel's stored msgpack bytes. After evicting it from the LRU cache,
	// GetRelationship cache-misses, finds the rel ID in the in-memory index,
	// reads garbage from Badger, and surfaces the unmarshal error — exactly
	// the "operational, non-ErrRelNotFound" class the fix must propagate.
	ts.mu.RLock()
	hot := ts.hotShard
	ts.mu.RUnlock()
	if hot == nil || hot.store == nil {
		t.Fatal("hot shard store missing — cannot inject fault")
	}
	originalStore := hot.store
	if !originalStore.hasRelID(rel.ID().SnowflakeID()) {
		// Sanity: the rel entity must be on the hot shard for this test
		// to exercise the bug. If routing has changed, the test setup
		// needs updating, not the production code.
		t.Fatal("rel entity is not on hot event shard — fault would land elsewhere; revisit Section 12 routing or test setup")
	}
	corruptRelBytesOnDisk(t, originalStore, rel.ID())

	res, err := ts.RunRepair()
	if err == nil {
		t.Fatalf("RunRepair: got nil error, want operational error to be propagated. Result: %+v", res)
	}
	// Must NOT be an ErrRelNotFound — that's the legitimate-skip sentinel
	// the original code conflated with operational errors. The fix must
	// surface real failures distinctly.
	if errors.Is(err, ErrRelNotFound) {
		t.Errorf("RunRepair returned ErrRelNotFound; the fix must surface operational errors as themselves, not as the legitimate-skip sentinel")
	}
}

// corruptRelBytesOnDisk forces a non-ErrRelNotFound failure path on a
// subsequent GetRelationship for relID against bs:
//  1. Flush pending writes so the rel value is durable.
//  2. Evict the rel from the relCache so the read falls through to Badger.
//  3. Overwrite the rel's Badger value with non-msgpack bytes — the next
//     read will surface a msgpack unmarshal error (operational class).
//
// The test cannot use ErrRelNotFound (legitimate-skip sentinel); it
// specifically needs an operational-class error to exercise the F2 fix.
func corruptRelBytesOnDisk(t *testing.T, bs *BadgerStore, relID types.RelID) {
	t.Helper()

	// 1. Flush pending so the value is in Badger, not just the WriteBatch.
	if err := bs.flush(); err != nil {
		t.Fatalf("flush before corruption: %v", err)
	}

	// 2. Evict the rel from the LRU so cacheHit cannot short-circuit the
	//    read. The cache holds a clean copy after PutRelationship/flush;
	//    GetRelationship's cacheHit path returns success even if Badger
	//    is corrupted. Use evictForTest (added by review M5) instead of
	//    reaching into LRU internals.
	id := relID.SnowflakeID()
	bs.relCache.EvictForTest(id)

	// 3. Overwrite the Badger value with non-msgpack bytes.
	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Set(storepkg.RelKey(id), []byte{0xFF, 0xFE, 0xFD, 0xFC})
	})
	if err != nil {
		t.Fatalf("corrupt rel bytes: %v", err)
	}
}

// staleRelIDInAllRelIDs creates the divergent state RunRepair Phase 2
// observes when a rel is deleted between AllRelIDs and GetRelationship:
//   - bs.relIDs still contains the rel (so AllRelIDs surfaces it)
//   - relCache no longer holds it (so cacheHit cannot satisfy the read)
//   - the Badger value is gone (so the disk read returns key-not-found)
//
// Without this divergence, Phase 2 would see a healthy rel and never
// hit the ErrRelNotFound `continue` branch — the test would then pass
// regardless of whether the fix's errors.Is gate is correct.
func staleRelIDInAllRelIDs(t *testing.T, bs *BadgerStore, relID types.RelID) {
	t.Helper()

	// Flush so the rel is persisted to Badger.
	if err := bs.flush(); err != nil {
		t.Fatalf("flush before stale-id setup: %v", err)
	}

	// Drop from cache so cacheHit can't return the stale value.
	bs.relCache.EvictForTest(relID.SnowflakeID())

	// Delete the Badger key so GetRelationship's disk read returns
	// key-not-found, which BadgerStore translates to ErrRelNotFound.
	id := relID.SnowflakeID()
	err := bs.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(storepkg.RelKey(id))
	})
	if err != nil {
		t.Fatalf("delete rel key for stale-id setup: %v", err)
	}
	// Note: bs.relIDs still has the entry. AllRelIDs reads from that
	// in-memory map, so the rel will still be enumerated. This is the
	// divergence that simulates the AllRelIDs-then-delete race.
}

// TestRunRepair_SkipsLegitimateRelNotFound: a Phase 2 read that returns
// ErrRelNotFound (rel deleted between AllRelIDs and GetRelationship — a
// legitimate TOCTOU race) must NOT be propagated. RunRepair should still
// complete successfully. The `staleRelIDInAllRelIDs` helper engineers
// the exact divergence — without it we'd be testing the happy path,
// not the fix's errors.Is(err, ErrRelNotFound) gate (review HIGH Q7).
func TestRunRepair_SkipsLegitimateRelNotFound(t *testing.T) {
	g, ts := newTestTieredGraph(t)

	// Build a cross-shard rel so Phase 2 enters the GetRelationship loop.
	// (Same-shard rels short-circuit before the read.)
	caseRef, err := g.AddNode([]string{"Case"}, nil)
	if err != nil {
		t.Fatalf("AddNode case: %v", err)
	}
	signalEvt, err := g.AddNode([]string{"Signal"}, nil)
	if err != nil {
		t.Fatalf("AddNode signal: %v", err)
	}
	rel, err := g.AddRelationship("TOUCHES", caseRef, signalEvt, nil)
	if err != nil {
		t.Fatalf("AddRelationship cross-shard: %v", err)
	}

	// Engineer the divergence: rel still in AllRelIDs, but
	// GetRelationship will return ErrRelNotFound. The owner shard for a
	// cross-shard ref→event rel is refShard.
	staleRelIDInAllRelIDs(t, ts.refShard, rel.InternalID())

	// RunRepair must NOT propagate ErrRelNotFound — that's the fix's
	// legitimate-skip class.
	res, err := ts.RunRepair()
	if err != nil {
		t.Fatalf("RunRepair must skip ErrRelNotFound silently; got error %v", err)
	}
	if res == nil {
		t.Fatal("RunRepair returned nil result")
	}
}
