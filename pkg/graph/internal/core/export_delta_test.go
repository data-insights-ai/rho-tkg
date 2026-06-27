package core

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// newDeltaGraph builds a change-log-enabled graph (the precondition for
// ExportSince) with the given snowflake slot.
func newDeltaGraph(t *testing.T, slot int64) *Core {
	t.Helper()
	g, err := New(Config{Store: memory.New(memory.WithChangeLog()), SnowflakeNodeID: slot})
	if err != nil {
		t.Fatalf("New(slot=%d): %v", slot, err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// newPlainGraph builds a graph WITHOUT a change-log — a valid ImportMerge target
// (the merge applies through store doors; it needs no change-log of its own).
func newPlainGraph(t *testing.T, slot int64) *Core {
	t.Helper()
	g, err := New(Config{Store: memory.New(), SnowflakeNodeID: slot})
	if err != nil {
		t.Fatalf("New(slot=%d): %v", slot, err)
	}
	t.Cleanup(func() { _ = g.Close() })
	return g
}

// exportBody is the header-stripped full-export bytes of a graph: registry +
// every current/history entity record, deterministic and identical between two
// graphs that hold the same logical state (same IDs, hashes, versions, temporal
// metadata, registry order). The header is skipped because it carries
// per-export, per-graph fields (ExportedAt, SnapshotLSN). This is the same
// equivalence oracle a backup pipeline uses (a content digest over the
// header-stripped stream).
func exportBody(t *testing.T, c *Core) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := c.IO.Export(&buf); err != nil {
		t.Fatalf("export: %v", err)
	}
	r := bytes.NewReader(buf.Bytes())
	if _, _, err := readExportRecord(r); err != nil { // skip the header record
		t.Fatalf("read header: %v", err)
	}
	rest, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return rest
}

func assertEquivalent(t *testing.T, want, got *Core, msg string) {
	t.Helper()
	w := exportBody(t, want)
	g := exportBody(t, got)
	if !bytes.Equal(w, g) {
		t.Fatalf("%s: graphs not equivalent (want %d body bytes, got %d)", msg, len(w), len(g))
	}
}

// fullExportInto restores src's full export into a fresh plain graph (the base of
// a delta chain).
func fullExportInto(t *testing.T, src *Core, dst *Core) {
	t.Helper()
	var buf bytes.Buffer
	if err := src.IO.Export(&buf); err != nil {
		t.Fatalf("full export: %v", err)
	}
	if err := dst.IO.Import(&buf, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("full import: %v", err)
	}
}

func exportSinceBytes(t *testing.T, src *Core, since tkgio.Cursor) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := src.IO.ExportSince(&buf, since); err != nil {
		t.Fatalf("ExportSince: %v", err)
	}
	return buf.Bytes()
}

// --- 1. Round-trip equivalence: full base + delta == full source-after-mutations.
func TestExportSince_RoundTripEquivalence(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)

	a, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	b, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "bob"})
	if _, err := src.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"since": int64(2020)}); err != nil {
		t.Fatalf("rel add: %v", err)
	}

	c0, err := src.IO.Watermark()
	if err != nil {
		t.Fatalf("watermark: %v", err)
	}

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)
	assertEquivalent(t, src, base, "base after full import")

	// Mutate: add a node, update a node, delete a node.
	cNode, _ := src.Nodes.Add(ctx, []string{"Org"}, map[string]any{"name": "acme"})
	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"name": "alice2", "age": int64(30)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := src.Nodes.Delete(ctx, b.ID()); err != nil {
		t.Fatalf("delete b: %v", err)
	}
	_ = cNode

	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertEquivalent(t, src, base, "base after delta merge")
}

// --- 2. Idempotency: applying the same delta twice is a no-op.
func TestImportMerge_Idempotent(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"name": "alice2"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)

	for i := 0; i < 2; i++ {
		if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
			t.Fatalf("ImportMerge #%d: %v", i+1, err)
		}
		assertEquivalent(t, src, base, "after merge")
	}
}

// --- 3. Chaining: base + deltaA + deltaB == source@C2; overlap re-apply is safe.
func TestImportMerge_Chaining(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	// Window A.
	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"name": "alice2"}); err != nil {
		t.Fatalf("update A: %v", err)
	}
	deltaA := exportSinceBytes(t, src, c0)
	var hdrA tkgio.DeltaHeader
	hdrA, err := tkgio.HeaderOf(bytes.NewReader(deltaA))
	if err != nil {
		t.Fatalf("HeaderOf A: %v", err)
	}
	c1 := hdrA.To

	// Window B.
	b, _ := src.Nodes.Add(ctx, []string{"Org"}, map[string]any{"name": "acme"})
	if _, err := src.Rels.Add(ctx, "WORKS_AT", a, b, nil); err != nil {
		t.Fatalf("rel add: %v", err)
	}
	deltaB := exportSinceBytes(t, src, c1)

	// Apply A then B.
	if err := base.IO.ImportMerge(bytes.NewReader(deltaA), tkgio.MergeOptions{ExpectBase: c0}); err != nil {
		t.Fatalf("merge A: %v", err)
	}
	if err := base.IO.ImportMerge(bytes.NewReader(deltaB), tkgio.MergeOptions{ExpectBase: c1}); err != nil {
		t.Fatalf("merge B: %v", err)
	}
	assertEquivalent(t, src, base, "base after A+B")

	// Re-apply A (overlap) — still correct (idempotent).
	if err := base.IO.ImportMerge(bytes.NewReader(deltaA), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("re-merge A: %v", err)
	}
	assertEquivalent(t, src, base, "base after A+B+A")
}

// --- 4. Tombstones: delete in window, and create-then-delete in window.
func TestExportSince_Tombstones(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	keep, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "keep"})
	doomed, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "doomed"})
	_ = keep
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	// Delete a pre-existing node + create-then-delete a fresh one, all in window.
	if err := src.Nodes.Delete(ctx, doomed.ID()); err != nil {
		t.Fatalf("delete doomed: %v", err)
	}
	ephemeral, _ := src.Nodes.Add(ctx, []string{"Temp"}, map[string]any{"name": "ephemeral"})
	if err := src.Nodes.Delete(ctx, ephemeral.ID()); err != nil {
		t.Fatalf("delete ephemeral: %v", err)
	}

	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertEquivalent(t, src, base, "tombstones merged")

	// The deleted nodes must be gone from current state on the merged base.
	if _, err := base.getCurrentNode(doomed.ID()); !errors.Is(err, storepkg.ErrNodeNotFound) {
		t.Fatalf("doomed still current after merge: %v", err)
	}
}

// --- Connected-node cascade delete in window.
func TestExportSince_CascadeDelete(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	if _, err := src.Rels.Add(ctx, "KNOWS", a, b, nil); err != nil {
		t.Fatalf("rel add: %v", err)
	}
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	if err := src.Nodes.Delete(ctx, a.ID()); err != nil { // cascades the KNOWS rel
		t.Fatalf("cascade delete: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertEquivalent(t, src, base, "cascade delete merged")
}

// --- Label change in window (multiple label-token mutations).
func TestExportSince_LabelChange(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	n, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	// Add a label via the label door (the change-log emits a NodePut whose label
	// set differs from the base by one token; the merge routes it to the
	// label-token apply door).
	if err := src.Nodes.AddLabel(ctx, n.ID(), "Admin"); err != nil {
		t.Fatalf("add label: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertEquivalent(t, src, base, "label change merged")
}

// --- 5. ExpectBase mismatch → ErrDeltaBaseMismatch.
func TestImportMerge_ExpectBaseMismatch(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	c0, _ := src.IO.Watermark()
	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)
	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"x": int64(1)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)

	wrong := tkgio.Cursor{LSN: c0.LSN + 999, Epoch: c0.Epoch}
	err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{ExpectBase: wrong})
	if !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("ImportMerge with wrong ExpectBase = %v, want ErrDeltaBaseMismatch", err)
	}
}

// --- 6. Epoch mismatch → ErrCursorUnknown.
func TestExportSince_EpochMismatch(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	if _, err := src.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	c0, _ := src.IO.Watermark()
	foreign := tkgio.Cursor{LSN: c0.LSN, Epoch: c0.Epoch ^ 0xDEAD}
	var buf bytes.Buffer
	err := src.IO.ExportSince(&buf, foreign)
	if !errors.Is(err, ErrCursorUnknown) {
		t.Fatalf("ExportSince(foreign epoch) = %v, want ErrCursorUnknown", err)
	}
}

// --- Cursor ahead of the log → ErrCursorUnknown.
func TestExportSince_CursorAhead(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	if _, err := src.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	c0, _ := src.IO.Watermark()
	ahead := tkgio.Cursor{LSN: c0.LSN + 1000, Epoch: c0.Epoch}
	var buf bytes.Buffer
	if err := src.IO.ExportSince(&buf, ahead); !errors.Is(err, ErrCursorUnknown) {
		t.Fatalf("ExportSince(ahead) = %v, want ErrCursorUnknown", err)
	}
}

// --- 7. No change-log → Watermark/ExportSince decline; consumer falls back.
func TestExportSince_NoChangeLog(t *testing.T) {
	g := newPlainGraph(t, 0)
	if _, err := g.IO.Watermark(); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("Watermark(no change-log) = %v, want ErrCapabilityNotSupported", err)
	}
	var buf bytes.Buffer
	if err := g.IO.ExportSince(&buf, tkgio.Cursor{}); !errors.Is(err, storepkg.ErrCapabilityNotSupported) {
		t.Fatalf("ExportSince(no change-log) = %v, want ErrCapabilityNotSupported", err)
	}
}

// --- 8. Empty delta: no changes in window → header+registry only; merge is a no-op.
func TestExportSince_EmptyDelta(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	if _, err := src.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	c0, _ := src.IO.Watermark()
	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)
	before := exportBody(t, base)

	delta := exportSinceBytes(t, src, c0) // no mutations since c0
	hdr, err := tkgio.HeaderOf(bytes.NewReader(delta))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	if !hdr.IsDelta || hdr.From != c0 {
		t.Fatalf("empty delta header = %+v, want IsDelta with From=%+v", hdr, c0)
	}
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge(empty): %v", err)
	}
	if after := exportBody(t, base); !bytes.Equal(before, after) {
		t.Fatal("empty delta merge changed the graph")
	}
}

// --- 9. HeaderOf distinguishes full vs delta and decodes From/To.
func TestHeaderOf_FullVsDelta(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	if _, err := src.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	c0, _ := src.IO.Watermark()

	var full bytes.Buffer
	if err := src.IO.Export(&full); err != nil {
		t.Fatalf("export: %v", err)
	}
	fh, err := tkgio.HeaderOf(bytes.NewReader(full.Bytes()))
	if err != nil {
		t.Fatalf("HeaderOf(full): %v", err)
	}
	if fh.IsDelta {
		t.Fatal("full export header reports IsDelta=true")
	}
	if fh.NodeCount != 1 {
		t.Fatalf("full header NodeCount = %d, want 1", fh.NodeCount)
	}
	// A full export's To is a complete, gapless base cursor: SnapshotLSN + the
	// lineage epoch, matching Watermark() when nothing has mutated since.
	if fh.To != c0 {
		t.Fatalf("full export To = %+v, want %+v (complete base cursor)", fh.To, c0)
	}
	if fh.To.Epoch == 0 {
		t.Fatal("full export To carries no lineage epoch")
	}

	if _, err := src.Nodes.Add(ctx, []string{"Org"}, nil); err != nil {
		t.Fatalf("add 2: %v", err)
	}
	dh, err := tkgio.HeaderOf(bytes.NewReader(exportSinceBytes(t, src, c0)))
	if err != nil {
		t.Fatalf("HeaderOf(delta): %v", err)
	}
	if !dh.IsDelta {
		t.Fatal("delta header reports IsDelta=false")
	}
	if dh.From != c0 {
		t.Fatalf("delta From = %+v, want %+v", dh.From, c0)
	}
	if dh.To.Epoch != c0.Epoch || dh.To.LSN < c0.LSN {
		t.Fatalf("delta To = %+v, want epoch %d and LSN >= %d", dh.To, c0.Epoch, c0.LSN)
	}
}

// HeaderOf on a non-header first record fails closed.
func TestHeaderOf_NotAHeader(t *testing.T) {
	_, err := tkgio.HeaderOf(bytes.NewReader([]byte{0x03, 0, 0, 0, 0}))
	if !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("HeaderOf(non-header) = %v, want ErrCorruptExport", err)
	}
}

// --- 10. Corruption rollback: a flipped change-record byte fails closed and
// leaves the base unchanged.
func TestImportMerge_CorruptionRollsBack(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	c0, _ := src.IO.Watermark()
	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)
	before := exportBody(t, base)

	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"name": "alice2", "age": int64(40)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := src.Nodes.Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("add: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)

	// Flip a byte in the tail (inside a change-record body, after header+registry)
	// to break a content hash / chain link.
	corrupt := make([]byte, len(delta))
	copy(corrupt, delta)
	corrupt[len(corrupt)-1] ^= 0xFF

	err := base.IO.ImportMerge(bytes.NewReader(corrupt), tkgio.MergeOptions{})
	if err == nil {
		t.Fatal("ImportMerge(corrupt) succeeded, want error")
	}
	if after := exportBody(t, base); !bytes.Equal(before, after) {
		t.Fatal("corrupt delta left partial state (rollback incomplete)")
	}
}

// --- 11. Strict: tombstone/update for a missing entity.
func TestImportMerge_Strict(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	c0, _ := src.IO.Watermark()

	// Mutate a, then delete it — the delta references a.
	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"x": int64(1)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)

	// Strict merge onto an EMPTY base (a absent) → ErrDeltaBaseMismatch.
	emptyBase := newPlainGraph(t, 1)
	if err := emptyBase.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{Strict: true}); !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("Strict merge onto empty base = %v, want ErrDeltaBaseMismatch", err)
	}

	// Lenient merge onto the empty base tolerates the missing entity (no-op-ish):
	// the with-history put for an absent node creates it, so this is not an error.
	lenientBase := newPlainGraph(t, 2)
	if err := lenientBase.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("lenient merge onto empty base: %v", err)
	}
}

// --- Consumer pattern: derive the base cursor from the full export header
// (gapless, self-contained) rather than a separate Watermark() call.
func TestExportSince_BaseCursorFromHeader(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	if _, err := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"}); err != nil {
		t.Fatalf("add: %v", err)
	}

	var full bytes.Buffer
	if err := src.IO.Export(&full); err != nil {
		t.Fatalf("export: %v", err)
	}
	hdr, err := tkgio.HeaderOf(bytes.NewReader(full.Bytes()))
	if err != nil {
		t.Fatalf("HeaderOf: %v", err)
	}
	baseCursor := hdr.To // {SnapshotLSN, Epoch} — the exact point the base is at

	base := newPlainGraph(t, 1)
	if err := base.IO.Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("import: %v", err)
	}

	if _, err := src.Nodes.Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("add 2: %v", err)
	}
	delta := exportSinceBytes(t, src, baseCursor)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{ExpectBase: baseCursor}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertEquivalent(t, src, base, "delta from header-derived base cursor")
}

// --- Cross-backend: badger source delta merged onto a memory base.
func TestExportSince_BadgerSourceCrossBackend(t *testing.T) {
	ctx := context.Background()
	src, err := New(Config{BadgerInMemory: true, ChangeLog: true, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("New badger: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	a, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	b, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "bob"})
	if _, err := src.Rels.Add(ctx, "KNOWS", a, b, nil); err != nil {
		t.Fatalf("rel add: %v", err)
	}
	c0, err := src.IO.Watermark()
	if err != nil {
		t.Fatalf("watermark: %v", err)
	}

	base := newPlainGraph(t, 1) // memory base — proves backend-independent wire
	fullExportInto(t, src, base)
	assertEquivalent(t, src, base, "badger->memory base after full import")

	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"name": "alice2"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if err := src.Nodes.Delete(ctx, b.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := src.Nodes.Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("add: %v", err)
	}

	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{ExpectBase: c0}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertEquivalent(t, src, base, "badger->memory after delta merge")
}

// --- Badger paged chain-verify over a cascade DAG: badger uses
// verifyNodeChainPagedLocked, so this pins the DAG fix on the paged path too,
// plus a full badger->badger round-trip of a cascade-corrected graph.
func TestExportSince_BadgerCascadePagedVerify(t *testing.T) {
	ctx := context.Background()
	src, err := New(Config{BadgerInMemory: true, ChangeLog: true, SnowflakeNodeID: 0})
	if err != nil {
		t.Fatalf("New badger: %v", err)
	}
	t.Cleanup(func() { _ = src.Close() })

	n, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	if _, err := src.Temporal.SetNodeVersionInterval(ctx, n.ID(), 1000, 2000, map[string]any{"name": "v1"}); err != nil {
		t.Fatalf("interval 1: %v", err)
	}
	if _, err := src.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2000, 3000, map[string]any{"name": "v2"}); err != nil {
		t.Fatalf("interval 2: %v", err)
	}

	ok, vErr := src.Hash.VerifyNodeChain(n.ID()) // paged path on badger
	if vErr != nil || !ok {
		t.Fatalf("badger paged cascade verify = (%v, %v), want (true, nil)", ok, vErr)
	}

	var full bytes.Buffer
	if err := src.IO.Export(&full); err != nil {
		t.Fatalf("export: %v", err)
	}
	dst, err := New(Config{BadgerInMemory: true, ChangeLog: true, SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New dst badger: %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	if err := dst.IO.Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("badger import of cascade graph: %v", err)
	}
	assertEquivalent(t, src, dst, "badger->badger cascade round-trip")
}

// --- Regression pin for the shared apply-path TxTo reproduction: a delta-merged
// superseded history row must carry the SAME tkg_tx_to (the supersession instant)
// as a full export, not 0. The integrity hash excludes TxTo, so only a byte/field
// check catches a regression here (the round-trip byte equivalence above also
// covers it; this isolates the field for a clear failure message).
func TestImportMerge_ReproducesSupersededTxTo(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	c0, _ := src.IO.Watermark()
	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	if _, err := src.Nodes.Update(ctx, a.ID(), map[string]any{"name": "alice2"}); err != nil {
		t.Fatalf("update: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}

	srcHist, err := src.getNodeHistory(a.ID())
	if err != nil || len(srcHist) != 1 {
		t.Fatalf("source history = %v (len %d), want 1 entry", err, len(srcHist))
	}
	baseHist, err := base.getNodeHistory(a.ID())
	if err != nil || len(baseHist) != 1 {
		t.Fatalf("merged history = %v (len %d), want 1 entry", err, len(baseHist))
	}
	srcTxTo := srcHist[0].Temporal().TxTo
	baseTxTo := baseHist[0].Temporal().TxTo
	if srcTxTo == 0 {
		t.Fatal("source superseded version has TxTo=0 (test assumption broken)")
	}
	if baseTxTo != srcTxTo {
		t.Fatalf("merged superseded TxTo = %d, want %d (apply path must reproduce it)", baseTxTo, srcTxTo)
	}
}

// --- Relationship parity: rel update + rel delete in window round-trip
// (exercises the ChangeRelPut / ChangeRelDelete capture + apply branches).
func TestExportSince_RelChangesRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	keep, _ := src.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"w": int64(1)})
	doomed, _ := src.Rels.Add(ctx, "KNOWS", b, a, nil)
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	if _, err := src.Rels.Update(ctx, keep.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("rel update: %v", err)
	}
	if err := src.Rels.Delete(ctx, doomed.ID()); err != nil {
		t.Fatalf("rel delete: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge: %v", err)
	}
	assertEquivalent(t, src, base, "rel changes merged")
}

// --- Relationship parity: a corrupt delta touching a relationship rolls the
// relationship back to its pre-merge state (exercises restoreMergeRel + the
// rel capture path in mergeRollback).
func TestImportMerge_RelCorruptionRollsBack(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := src.Rels.Add(ctx, "KNOWS", a, b, map[string]any{"w": int64(1)})
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)
	before := exportBody(t, base)

	// Rel update (lower LSN) then a node add (higher LSN); corrupting the tail
	// fails the node record AFTER the rel record was applied, forcing the rel
	// rollback path.
	if _, err := src.Rels.Update(ctx, r.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("rel update: %v", err)
	}
	if _, err := src.Nodes.Add(ctx, []string{"Org"}, map[string]any{"name": "acme"}); err != nil {
		t.Fatalf("node add: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)
	corrupt := make([]byte, len(delta))
	copy(corrupt, delta)
	corrupt[len(corrupt)-1] ^= 0xFF

	if err := base.IO.ImportMerge(bytes.NewReader(corrupt), tkgio.MergeOptions{}); err == nil {
		t.Fatal("ImportMerge(corrupt) succeeded, want error")
	}
	if after := exportBody(t, base); !bytes.Equal(before, after) {
		t.Fatal("corrupt rel delta left partial state (rel rollback incomplete)")
	}
}

// --- Bitemporal cascade correction in window: SetNodeVersionInterval appends
// version rows whose PrevHash links on the VALID-TIME axis (a hash DAG), which
// the delta carries (ChangeNodeHistoryVersion + a ReplaceNode-based ChangeNodePut)
// and the merge reproduces. This pins the chain-verify DAG fix: before it, a
// cascade-corrected chain failed verification, so neither a full Export+Import
// NOR a delta merge could round-trip it. Now both do.
func TestExportSince_VersionIntervalCascade(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	n, _ := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	if _, err := src.Temporal.SetNodeVersionInterval(ctx, n.ID(), 1000, 2000, map[string]any{"name": "v1"}); err != nil {
		t.Fatalf("set interval 1: %v", err)
	}
	c0, _ := src.IO.Watermark()

	base := newPlainGraph(t, 1)
	fullExportInto(t, src, base)

	if _, err := src.Temporal.SetNodeVersionInterval(ctx, n.ID(), 2000, 3000, map[string]any{"name": "v2"}); err != nil {
		t.Fatalf("set interval 2: %v", err)
	}

	// The cascade-corrected chain now verifies (DAG linkage).
	if ok, err := src.verifyNodeChainLocked(n.ID()); err != nil || !ok {
		t.Fatalf("source cascade chain verify = (%v, %v), want (true, nil)", ok, err)
	}

	// Control: a full Export+Import of the post-cascade source round-trips.
	var full bytes.Buffer
	if err := src.IO.Export(&full); err != nil {
		t.Fatalf("export: %v", err)
	}
	ctrl := newPlainGraph(t, 2)
	if err := ctrl.IO.Import(bytes.NewReader(full.Bytes()), tkgio.ImportOptions{}); err != nil {
		t.Fatalf("control full import of cascade graph: %v", err)
	}
	assertEquivalent(t, src, ctrl, "full import of cascade graph")

	// Delta merge reproduces the cascade correction.
	delta := exportSinceBytes(t, src, c0)
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{}); err != nil {
		t.Fatalf("ImportMerge(cascade delta): %v", err)
	}
	assertEquivalent(t, src, base, "version-interval cascade merged")
}

// --- A delta carrying a ChangeMeta record (no door emits it; not entity state)
// is rejected at the trust boundary.
func TestImportMerge_RejectsMetaRecord(t *testing.T) {
	ctx := context.Background()
	base := newPlainGraph(t, 1)
	if _, err := base.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := exportBody(t, base)

	var buf bytes.Buffer
	hdr := exportHeader{Version: exportFormatVersionDelta, ExportedAt: 1, IsDelta: true, FromLSN: 1, FromEpoch: 7, ToLSN: 5, ToEpoch: 7}
	if err := marshalAndWrite(&buf, exportTagHeader, &hdr); err != nil {
		t.Fatalf("header: %v", err)
	}
	reg := tiered.RegistryFileData{Labels: base.labels.ExportNames(), RelTypes: base.relTypes.ExportNames()}
	if err := marshalAndWrite(&buf, exportTagRegistry, &reg); err != nil {
		t.Fatalf("registry: %v", err)
	}
	meta, err := storeutil.MarshalChangeBody(storeutil.MetaBody{Key: "k", Value: []byte("v")})
	if err != nil {
		t.Fatalf("meta body: %v", err)
	}
	crw := changeRecordWire{LSN: 2, Tag: uint8(storepkg.ChangeMeta), Payload: meta}
	if err := marshalAndWrite(&buf, exportTagChange, &crw); err != nil {
		t.Fatalf("change: %v", err)
	}
	if err := base.IO.ImportMerge(&buf, tkgio.MergeOptions{}); !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("ImportMerge(meta record) = %v, want ErrCorruptExport", err)
	}
	if after := exportBody(t, base); !bytes.Equal(before, after) {
		t.Fatal("rejected meta record left partial state")
	}
}

// --- Trust boundary: a change record whose inner payload is malformed fails
// closed with ErrCorruptExport and leaves the base unchanged (decode is part of
// the trust boundary; lesson 44/47).
func TestImportMerge_MalformedChangePayload(t *testing.T) {
	ctx := context.Background()
	base := newPlainGraph(t, 1)
	if _, err := base.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("seed: %v", err)
	}
	before := exportBody(t, base)

	var buf bytes.Buffer
	hdr := exportHeader{Version: exportFormatVersionDelta, ExportedAt: 1, IsDelta: true, FromLSN: 1, FromEpoch: 7, ToLSN: 5, ToEpoch: 7}
	if err := marshalAndWrite(&buf, exportTagHeader, &hdr); err != nil {
		t.Fatalf("header: %v", err)
	}
	reg := tiered.RegistryFileData{Labels: base.labels.ExportNames(), RelTypes: base.relTypes.ExportNames()}
	if err := marshalAndWrite(&buf, exportTagRegistry, &reg); err != nil {
		t.Fatalf("registry: %v", err)
	}
	// ChangeNodePut with a payload that is not a valid NodePutBody (0xc1 is a
	// never-used msgpack byte → SafeUnmarshal fails inside captureMergeRecord).
	crw := changeRecordWire{LSN: 2, Tag: uint8(storepkg.ChangeNodePut), Payload: []byte{0xc1}}
	if err := marshalAndWrite(&buf, exportTagChange, &crw); err != nil {
		t.Fatalf("change: %v", err)
	}

	if err := base.IO.ImportMerge(&buf, tkgio.MergeOptions{}); !errors.Is(err, ErrCorruptExport) {
		t.Fatalf("ImportMerge(malformed change) = %v, want ErrCorruptExport", err)
	}
	if after := exportBody(t, base); !bytes.Equal(before, after) {
		t.Fatal("malformed change record left partial state")
	}
}

// --- Strict: a delete record for an entity absent from the base.
func TestImportMerge_StrictDeleteMissing(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, _ := src.Nodes.Add(ctx, []string{"Person"}, nil)
	c0, _ := src.IO.Watermark()
	if err := src.Nodes.Delete(ctx, a.ID()); err != nil {
		t.Fatalf("delete: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)

	base := newPlainGraph(t, 1) // a never existed here
	if err := base.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{Strict: true}); !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("Strict delete-missing = %v, want ErrDeltaBaseMismatch", err)
	}
}

// --- Strict, relationship parity: a rel delete + rel update for entities absent
// from the base (covers the ChangeRelDelete / ChangeRelPut Strict branches).
func TestImportMerge_StrictRelMissing(t *testing.T) {
	ctx := context.Background()

	// Rel delete of a rel absent from the base.
	srcDel := newDeltaGraph(t, 0)
	a, _ := srcDel.Nodes.Add(ctx, []string{"Person"}, nil)
	b, _ := srcDel.Nodes.Add(ctx, []string{"Person"}, nil)
	r, _ := srcDel.Rels.Add(ctx, "KNOWS", a, b, nil)
	cDel, _ := srcDel.IO.Watermark()
	if err := srcDel.Rels.Delete(ctx, r.ID()); err != nil {
		t.Fatalf("rel delete: %v", err)
	}
	deltaDel := exportSinceBytes(t, srcDel, cDel)
	if err := newPlainGraph(t, 1).IO.ImportMerge(bytes.NewReader(deltaDel), tkgio.MergeOptions{Strict: true}); !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("Strict rel-delete-missing = %v, want ErrDeltaBaseMismatch", err)
	}

	// Rel update (with-history put) of a rel absent from the base.
	srcUpd := newDeltaGraph(t, 2)
	a2, _ := srcUpd.Nodes.Add(ctx, []string{"Person"}, nil)
	b2, _ := srcUpd.Nodes.Add(ctx, []string{"Person"}, nil)
	r2, _ := srcUpd.Rels.Add(ctx, "KNOWS", a2, b2, map[string]any{"w": int64(1)})
	cUpd, _ := srcUpd.IO.Watermark()
	if _, err := srcUpd.Rels.Update(ctx, r2.ID(), map[string]any{"w": int64(2)}); err != nil {
		t.Fatalf("rel update: %v", err)
	}
	deltaUpd := exportSinceBytes(t, srcUpd, cUpd)
	if err := newPlainGraph(t, 3).IO.ImportMerge(bytes.NewReader(deltaUpd), tkgio.MergeOptions{Strict: true}); !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("Strict rel-update-missing = %v, want ErrDeltaBaseMismatch", err)
	}
}

// --- A full export stream rejected by ImportMerge; a delta rejected by Import.
func TestImportMerge_RejectsFullStream(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	if _, err := src.Nodes.Add(ctx, []string{"Person"}, nil); err != nil {
		t.Fatalf("add: %v", err)
	}
	var full bytes.Buffer
	if err := src.IO.Export(&full); err != nil {
		t.Fatalf("export: %v", err)
	}
	dst := newPlainGraph(t, 1)
	if err := dst.IO.ImportMerge(bytes.NewReader(full.Bytes()), tkgio.MergeOptions{}); !errors.Is(err, ErrIncompatibleExport) {
		t.Fatalf("ImportMerge(full stream) = %v, want ErrIncompatibleExport", err)
	}

	c0, _ := src.IO.Watermark()
	if _, err := src.Nodes.Add(ctx, []string{"Org"}, nil); err != nil {
		t.Fatalf("add 2: %v", err)
	}
	delta := exportSinceBytes(t, src, c0)
	dst2 := newPlainGraph(t, 2)
	if err := dst2.IO.Import(bytes.NewReader(delta), tkgio.ImportOptions{}); !errors.Is(err, ErrIncompatibleExport) {
		t.Fatalf("Import(delta stream) = %v, want ErrIncompatibleExport", err)
	}
}
