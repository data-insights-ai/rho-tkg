package core

import (
	"bytes"
	"context"
	"errors"
	"testing"

	storeutil "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/storeutil"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BACKLOG 12i: strictCheckMergeRecord's wrong-base detector was blind to
// ChangeNodeHistoryVersion / ChangeRelHistoryVersion / ChangeNodeHistoryTruncate /
// ChangeRelHistoryTruncate. These tests exercise the extended switch directly
// (strictCheckMergeRecord/baseKnowsNode/baseKnowsRel are unexported, callable
// in-package) for the individual scenarios the design reasoned through, plus one
// full end-to-end ImportMerge proof that the wiring (decode, capture, apply)
// works together, not just the isolated function.

func mustMarshalChangeBody(t *testing.T, body any) []byte {
	t.Helper()
	b, err := storeutil.MarshalChangeBody(body)
	if err != nil {
		t.Fatalf("MarshalChangeBody: %v", err)
	}
	return b
}

// --- Positive: wrong-base detection fires for each of the four extended tags.

func TestStrictCheckMergeRecord_NodeHistoryVersion_WrongBase(t *testing.T) {
	base := newPlainGraph(t, 0)
	missing := types.NodeID(999888777)
	body := storeutil.HistoryVersionNodeBody{Version: 3, Wire: storeutil.NodeWire{ID: int64(missing.SnowflakeID()), PrimaryLabel: 1}}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeNodeHistoryVersion, Payload: mustMarshalChangeBody(t, body)}

	err := base.strictCheckMergeRecord(rec)
	if !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("strictCheckMergeRecord(wrong-base node history version) = %v, want ErrDeltaBaseMismatch", err)
	}
}

func TestStrictCheckMergeRecord_RelHistoryVersion_WrongBase(t *testing.T) {
	base := newPlainGraph(t, 0)
	missing := types.RelID(999888666)
	body := storeutil.HistoryVersionRelBody{Version: 3, Wire: storeutil.RelWire{ID: int64(missing.SnowflakeID()), RelType: 1, StartID: 1, EndID: 2}}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeRelHistoryVersion, Payload: mustMarshalChangeBody(t, body)}

	err := base.strictCheckMergeRecord(rec)
	if !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("strictCheckMergeRecord(wrong-base rel history version) = %v, want ErrDeltaBaseMismatch", err)
	}
}

func TestStrictCheckMergeRecord_NodeHistoryTruncate_WrongBase(t *testing.T) {
	base := newPlainGraph(t, 0)
	missing := types.NodeID(999888555)
	body := storeutil.HistoryTruncateBody{ID: int64(missing.SnowflakeID()), Bound: 1}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeNodeHistoryTruncate, Payload: mustMarshalChangeBody(t, body)}

	err := base.strictCheckMergeRecord(rec)
	if !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("strictCheckMergeRecord(wrong-base node history truncate) = %v, want ErrDeltaBaseMismatch", err)
	}
}

func TestStrictCheckMergeRecord_RelHistoryTruncate_WrongBase(t *testing.T) {
	base := newPlainGraph(t, 0)
	missing := types.RelID(999888444)
	body := storeutil.HistoryTruncateBody{ID: int64(missing.SnowflakeID()), Bound: 1}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeRelHistoryTruncate, Payload: mustMarshalChangeBody(t, body)}

	err := base.strictCheckMergeRecord(rec)
	if !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("strictCheckMergeRecord(wrong-base rel history truncate) = %v, want ErrDeltaBaseMismatch", err)
	}
}

// --- Negative: legitimate records referencing a base-resident entity must not flag.

func TestStrictCheckMergeRecord_NodeHistoryVersion_KnownCurrentRow(t *testing.T) {
	ctx := context.Background()
	base := newPlainGraph(t, 0)
	n, err := base.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	body := storeutil.HistoryVersionNodeBody{Version: 5, Wire: storeutil.NodeWire{ID: int64(n.ID().SnowflakeID()), PrimaryLabel: 1}}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeNodeHistoryVersion, Payload: mustMarshalChangeBody(t, body)}

	if err := base.strictCheckMergeRecord(rec); err != nil {
		t.Fatalf("strictCheckMergeRecord(known current-row node) = %v, want nil", err)
	}
}

func TestStrictCheckMergeRecord_NodeHistoryTruncate_KnownCurrentRow(t *testing.T) {
	ctx := context.Background()
	base := newPlainGraph(t, 0)
	n, err := base.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	body := storeutil.HistoryTruncateBody{ID: int64(n.ID().SnowflakeID()), Bound: 1}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeNodeHistoryTruncate, Payload: mustMarshalChangeBody(t, body)}

	if err := base.strictCheckMergeRecord(rec); err != nil {
		t.Fatalf("strictCheckMergeRecord(known current-row node truncate) = %v, want nil", err)
	}
}

// TestStrictCheckMergeRecord_NodeHistoryVersion_KnownHistoryOnly exercises the
// SECOND leg of baseKnowsNode (history present, no current row) — a
// deleted-with-history entity. Node.Delete leaves an append-only tombstone
// version behind (CLAUDE.md "Append-only: Delete paths save tombstone
// versions... History is never erased on delete"), so the entity is known to
// the base via history alone.
func TestStrictCheckMergeRecord_NodeHistoryVersion_KnownHistoryOnly(t *testing.T) {
	ctx := context.Background()
	base := newPlainGraph(t, 0)
	n, err := base.Nodes.Add(ctx, []string{"Person"}, nil)
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	id := n.ID()
	if err := base.Nodes.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, gerr := base.getCurrentNode(id); !errors.Is(gerr, storepkg.ErrNodeNotFound) {
		t.Fatalf("test setup invalid: getCurrentNode after delete = %v, want ErrNodeNotFound", gerr)
	}
	hist, herr := base.store.GetNodeHistory(id)
	if herr != nil || len(hist) == 0 {
		t.Fatalf("test setup invalid: GetNodeHistory after delete = (%v, %v), want a tombstone history row", hist, herr)
	}

	body := storeutil.HistoryVersionNodeBody{Version: 9, Wire: storeutil.NodeWire{ID: int64(id.SnowflakeID()), PrimaryLabel: 1}}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeNodeHistoryVersion, Payload: mustMarshalChangeBody(t, body)}

	if err := base.strictCheckMergeRecord(rec); err != nil {
		t.Fatalf("strictCheckMergeRecord(history-only node) = %v, want nil (base knows the entity via history alone)", err)
	}
}

// TestStrictCheckMergeRecord_NodeHistoryVersion_VersionZero_NotFlaggedEvenIfAbsent
// pins the Version>0 gate itself: a Version==0 bare history-version record for a
// totally absent entity must NOT be flagged, regardless of baseKnowsNode's
// verdict — the gate is conservative by design (BACKLOG 12i decision: gate on
// Version>0 rather than checking every version).
func TestStrictCheckMergeRecord_NodeHistoryVersion_VersionZero_NotFlaggedEvenIfAbsent(t *testing.T) {
	base := newPlainGraph(t, 0)
	missing := types.NodeID(999888333)
	body := storeutil.HistoryVersionNodeBody{Version: 0, Wire: storeutil.NodeWire{ID: int64(missing.SnowflakeID()), PrimaryLabel: 1}}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeNodeHistoryVersion, Payload: mustMarshalChangeBody(t, body)}

	if err := base.strictCheckMergeRecord(rec); err != nil {
		t.Fatalf("strictCheckMergeRecord(Version==0, absent entity) = %v, want nil (gate suppresses the check)", err)
	}
}

// TestStrictCheckMergeRecord_ForeignIncoming_PassesThroughUnaffected confirms the
// switch's silent fallthrough for tags with no case (ChangeForeignIncoming,
// ChangeForeignIncomingDelete, ChangeRangePurge — documented as structurally
// unaddressable in the switch's own doc comment) never errors.
func TestStrictCheckMergeRecord_ForeignIncoming_PassesThroughUnaffected(t *testing.T) {
	base := newPlainGraph(t, 0)
	missing := types.RelID(999888222)
	body := storeutil.RelPutBody{Wire: storeutil.RelWire{ID: int64(missing.SnowflakeID()), RelType: 1, StartID: 1, EndID: 2}}
	rec := storepkg.ChangeRecord{Tag: storepkg.ChangeForeignIncoming, Payload: mustMarshalChangeBody(t, body)}

	if err := base.strictCheckMergeRecord(rec); err != nil {
		t.Fatalf("strictCheckMergeRecord(ChangeForeignIncoming) = %v, want nil (documented gap — not addressable)", err)
	}
}

// --- Full end-to-end proof: the wiring (decode + capture + apply + rollback)
// works together for a bare ChangeNodeHistoryVersion record, not just the
// isolated strictCheckMergeRecord function. Mirrors
// TestImportMerge_HistoryVersionCorruptionPreservesEdges's setup shape.
func TestImportMerge_StrictHistoryVersion_EndToEnd(t *testing.T) {
	ctx := context.Background()
	src := newDeltaGraph(t, 0)
	a, err := src.Nodes.Add(ctx, []string{"Person"}, map[string]any{"name": "alice"})
	if err != nil {
		t.Fatalf("Add: %v", err)
	}
	c0, _ := src.IO.Watermark()

	// Bounded PAST interval: appends a history row but leaves a's open-ended
	// current row in place, so the change feed carries ONLY a
	// ChangeNodeHistoryVersion record for a — no ChangeNodePut.
	if _, err := src.Temporal.SetNodeVersionInterval(ctx, a.ID(), 1000, 2000, map[string]any{"name": "v1"}); err != nil {
		t.Fatalf("SetNodeVersionInterval: %v", err)
	}

	recs, err := src.changeFeed.ChangeFeed(c0.LSN, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	sawBareHistoryForA := false
	for _, rec := range recs {
		switch rec.Tag {
		case storepkg.ChangeNodePut:
			t.Fatalf("setup broken: a ChangeNodePut appeared (LSN %d) — the scenario needs a BARE history-version record", rec.LSN)
		case storepkg.ChangeNodeHistoryVersion:
			body, derr := storeutil.DecodeHistoryVersionNode(rec.Payload)
			if derr != nil {
				t.Fatalf("decode: %v", derr)
			}
			if types.NodeID(body.Wire.ID) == a.ID() {
				sawBareHistoryForA = true
			}
		}
	}
	if !sawBareHistoryForA {
		t.Fatal("setup broken: no ChangeNodeHistoryVersion for a in the delta window")
	}

	delta := exportSinceBytes(t, src, c0)

	// Strict merge onto an EMPTY base (a absent) — wrong-base detection fires.
	emptyBase := newPlainGraph(t, 1)
	if err := emptyBase.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{Strict: true}); !errors.Is(err, ErrDeltaBaseMismatch) {
		t.Fatalf("Strict merge onto empty base = %v, want ErrDeltaBaseMismatch", err)
	}

	// Strict merge onto a base that already has a (via a full export first) —
	// legitimate, must succeed.
	knownBase := newPlainGraph(t, 2)
	fullExportInto(t, src, knownBase)
	if err := knownBase.IO.ImportMerge(bytes.NewReader(delta), tkgio.MergeOptions{Strict: true}); err != nil {
		t.Fatalf("Strict merge onto base that already knows a: %v, want nil", err)
	}
}
