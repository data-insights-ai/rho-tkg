package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

func graphColumnTestPinPolicy() graphColumnPinPolicy {
	return graphColumnPinPolicy{MaxRecords: 64, MaxBytes: 1 << 20, PageChunks: 1, PageBytes: 128 << 10, MaxWorkChunks: 8, MaxWorkBytes: 1 << 20, MaxOutputBytes: 1024}
}
func TestGraphColumnPinOwnershipAndCollectorAdmissionGuards(t *testing.T) {
	// Hidden handles remain recoverable for every unsuccessful authenticated
	// cleanup, including a non-native refusal. Successful Close needs no ticket.
	owner := &graphColumnRead{id: graphColumnOwnerID{Request: "read", Generation: "g", Secret: [32]byte{1}}, walking: true}
	err := columnReadCleanup(owner)
	pending, ok := errors.AsType[*graphColumnReadPending](err)
	if !ok || pending.Owner != owner || !errors.Is(err, errColumnInUse) || pending.Cause != errColumnInUse {
		t.Fatal("cleanup refusal lost owner identity/cause", err)
	}
	owner.walking, owner.closed = false, true
	if err := columnReadCleanup(owner); err != nil {
		t.Fatal("successful authenticated close retained pending ticket", err)
	}
	for _, p := range []graphColumnPinPolicy{{}, {MaxRecords: 1}, {MaxRecords: 64, MaxBytes: 1 << 20, PageChunks: 33}} {
		if !errors.Is(p.validate(), errInvalid) {
			t.Fatal("invalid retention policy", p)
		}
	}
	var absent *graphColumnPins
	if _, err := absent.open(t.Context(), graphColumnPinToken{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil pin owner", err)
	}
	s := &graphColumnStore{h: &harness{config: graphCostConfig("pin-guard", graphCostProfile{Version: 1, Facts: 1, Writers: 1})}}
	if _, err := newGraphColumnRead(s, "g", &graphColumnPinToken{Cut: certificate{Scope: make([]uint8, 4096)}}); !errors.Is(err, errInvalid) {
		t.Fatal("oversized token copied before admission", err)
	}
	v := &graphColumnPins{s: s, binding: graphColumnPinBinding{Policy: graphColumnTestPinPolicy()}}
	if _, err := v.collect(t.Context(), "g", graphColumnCollectBudget{Chunks: 1, Bytes: 1, OutputBytes: 1}); !errors.Is(err, errLimit) {
		t.Fatal("output admission reached SDK", err)
	}
	if _, err := v.collect(t.Context(), "g", graphColumnCollectBudget{}); !errors.Is(err, errInvalid) {
		t.Fatal("missing work budget reached SDK", err)
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := v.pin(cancelled, "request", "g", cut{certificate{Scope: []uint8{0, 1}}}); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled acquisition reached SDK", err)
	}
}

// Authored real SDK composition only; no runtime/coverage/GC capacity claim.
func TestGraphColumnPinsActualExpiryHeldReadAndResumableCollection(t *testing.T) {
	h := fixture(t, graphCostConfig(t.Name(), graphCostProfile{Version: 1, Facts: 1, CrossPercent: 100, Writers: 1}))
	initial := mustCut(t, h, []uint8{0, 1}, nil)
	apply := func(phase string, i uint64, command graphCommand, provenance uint64) {
		t.Helper()
		before, _, err := h.graphCostPointSnapshot(command)
		if err != nil {
			t.Fatal(err)
		}
		coordinator := uint8(0)
		if phase == "setup" {
			coordinator = command.Node.Owner
		}
		x := h.graphTransaction(fmt.Sprintf("pins-%s-%d", phase, i), coordinator, command, before)
		x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
		if phase != "setup" {
			x.Lifecycle.EdgeLife = command.Edge.ID + 1
			x.Lifecycle.Definition = &lifecycleDefinition{ID: uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value}
		}
		mustSubmit(t, h, x)
	}
	for i, n := range h.config.Cost.nodes() {
		apply("setup", uint64(i), graphCommand{Kind: "node-create", Node: n}, uint64(i+1))
	}
	apply("create", 0, graphCommand{Kind: "edge-create", Edge: h.config.Cost.edge(0, 5)}, 4)
	old := mustCut(t, h, []uint8{0, 1}, nil)
	policy := graphColumnTestPolicy()
	policy.ChunkRows = 1
	s, err := newGraphColumnStore(h, policy)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.publish(t.Context(), "g", initial, old)
	if err != nil || m.Chunks != 4 {
		t.Fatal("complete source generation", m, err)
	}
	pinPolicy := graphColumnTestPinPolicy()
	pinPolicy.MaxRecords = 7 // Includes the reserved future reference-drop record.
	v, err := newGraphColumnPins(s, pinPolicy)
	if err != nil {
		t.Fatal(err)
	}
	budget := graphColumnCollectBudget{Chunks: 1, Bytes: 128 << 10, OutputBytes: 512}
	guard := func() graphColumnGuard {
		t.Helper()
		raw, err := h.db.ReadTransact(func(tr fdb.ReadTransaction) (any, error) { g, _, err := s.guard(tr, m); return g, err })
		if err != nil {
			t.Fatal(err)
		}
		return raw.(graphColumnGuard)
	}
	ref, err := v.reference(t.Context(), "ref-add", "head", "g", 0)
	if err != nil {
		t.Fatal(err)
	}
	token, err := v.pin(t.Context(), "pin-original", "g", old)
	if err != nil {
		t.Fatal(err)
	}
	if replay, err := v.pin(t.Context(), "pin-original", "g", old); err != nil || !graphColumnSame(replay, token) {
		t.Fatal("immutable pin replay", replay, err)
	}
	released, err := v.pin(t.Context(), "pin-released", "g", old)
	if err != nil {
		t.Fatal(err)
	}
	before := guard()
	if _, err := v.reference(t.Context(), "quota-new-reference", "other", "g", 0); !errors.Is(err, errLimit) || guard() != before {
		t.Fatal("metadata quota admitted unfunded reference", err)
	}
	if _, err := v.collect(t.Context(), "g", budget); !errors.Is(err, errColumnInUse) || guard() != before {
		t.Fatal("referenced/pinned generation collected", err)
	}
	missing := token
	missing.Request = "unissued"
	if _, err := v.open(t.Context(), missing); !errors.Is(err, errUnavailable) || guard() != before {
		t.Fatal("missing pin borrowed authority", err)
	}
	wrong := token
	wrong.Secret[0] ^= 1
	if err := v.finish(t.Context(), "wrong-secret", wrong, false); !errors.Is(err, errMismatch) || guard() != before {
		t.Fatal("unauthenticated release", err)
	}
	if err := v.finish(t.Context(), "release-second", released, false); err != nil {
		t.Fatal(err)
	}
	if err := v.finish(t.Context(), "release-second", released, false); err != nil {
		t.Fatal("release replay", err)
	}
	if _, err := v.open(t.Context(), released); !errors.Is(err, errColumnExpired) {
		t.Fatal("released token admitted new owner", err)
	}
	if _, err := v.reference(t.Context(), "ref-drop", "head", "", ref.Version); err != nil {
		t.Fatal(err)
	}
	owner, err := v.open(t.Context(), token)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := owner.Close(); err != nil {
			t.Error(err)
		}
	})
	apply("update", 0, graphCommand{Kind: "edge-update", Edge: h.config.Cost.edge(0, 6)}, 5)
	current := mustCut(t, h, []uint8{0, 1}, nil)
	if err := v.finish(t.Context(), "expire-original", token, true); err != nil {
		t.Fatal(err)
	}
	if err := v.finish(t.Context(), "expire-original", token, true); err != nil {
		t.Fatal("expiry replay", err)
	}
	if _, err := v.open(t.Context(), token); !errors.Is(err, errColumnExpired) {
		t.Fatal("expired token admitted new owner", err)
	}
	if _, err := v.pin(t.Context(), "pin-original", "g", old); !errors.Is(err, errColumnExpired) {
		t.Fatal("expired request silently renewed", err)
	}
	var found bool
	if _, err := owner.walk(t.Context(), old, nil, func(row graphColumnRow) error {
		if err := owner.Close(); !errors.Is(err, errColumnInUse) {
			t.Fatal("Close under active callback", err)
		}
		if _, err := v.collect(t.Context(), "g", budget); !errors.Is(err, errColumnInUse) {
			t.Fatal("active owner lost physical chunks", err)
		}
		if row.Owner == 7 {
			found = true
			if row.Value != 5 || row.Provenance != 4 {
				t.Fatal("held old cut substituted later value", row)
			}
		}
		return nil
	}); err != nil || !found {
		t.Fatal("expired token invalidated held reader", found, err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	// Missing-owner cancellation is durable and prevents later admission under
	// that SAME request/secret; it is not an inferred timeout or missing=abort.
	cancelled, err := newGraphColumnRead(s, "g", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := cancelled.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cancelled.open(t.Context()); !errors.Is(err, errColumnExpired) {
		t.Fatal("late acquisition passed close tombstone", err)
	}
	before = guard()
	tiny := budget
	tiny.Bytes = 1
	if _, err := v.collect(t.Context(), "g", tiny); !errors.Is(err, errLimit) || guard() != before {
		t.Fatal("byte denial changed durable collection state", err)
	}
	first, err := v.collect(t.Context(), "g", budget)
	if err != nil || first.Done || first.Cursor != 1 || first.WorkChunks != 1 {
		t.Fatal("bounded first collection", first, err)
	}
	if _, err := s.walk(t.Context(), "g", old, nil, func(graphColumnRow) error { return nil }); !errors.Is(err, errColumnExpired) {
		t.Fatal("retiring generation returned current/empty", err)
	}
	if _, err := v.pin(t.Context(), "after-retirement", "g", old); !errors.Is(err, errColumnExpired) {
		t.Fatal("pin published after retirement", err)
	}
	for cursor := uint64(2); cursor <= m.Chunks; cursor++ {
		page, err := v.collect(t.Context(), "g", budget)
		if err != nil || page.Cursor != cursor || page.Done != (cursor == m.Chunks) {
			t.Fatal("durable cursor resume", page, err)
		}
	}
	final := guard()
	if final.State != columnGuardCollected || final.DeletedBytes != m.Bytes {
		t.Fatal("exact derived-byte reclamation", final, m.Bytes)
	}
	if retained, err := s.manifest("g"); err != nil || !graphColumnSame(retained, m) {
		t.Fatal("manifest lineage tombstone removed", err)
	}
	if _, err := s.publish(t.Context(), "g", initial, old); !errors.Is(err, errColumnExpired) {
		t.Fatal("retired publication identity recreated", err)
	}
	view, err := h.graphAt(old)
	if err != nil || graphCostCheckView(view, h.config.Cost, 5) != nil {
		t.Fatal("collector removed source history/cut", err)
	}
	groups, err := h.changes(initial, current, 1)
	if err != nil || len(groups) != 5 {
		t.Fatal("collector removed original CDC", len(groups), err)
	}
}
