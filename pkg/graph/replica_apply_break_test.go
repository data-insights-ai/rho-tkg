package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/vmihailenco/msgpack/v5"
)

// newReplicaPair spins up a primary (ChangeLog on) + a read-only replica
// bootstrapped from a fresh export, with the replica's applied watermark set to
// the snapshot LSN. It returns both graphs and the handoff LSN. Each graph is
// Close-registered on t. badgerDir == "" → in-memory replica; otherwise a
// disk-backed replica at that dir (so persistence across the watermark advance
// is exercised on the real durable path).
func newReplicaPair(t *testing.T, badgerDir string) (primary, replica *graph.Graph, lsn0 uint64) {
	t.Helper()
	var err error
	primary, err = graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	t.Cleanup(func() { _ = primary.Close() })

	// Seed so label "A" AND rel-type "KNOWS" tokens land in the bootstrap snapshot
	// registry; post-bootstrap records then reuse the known tokens (no refetch).
	s1 := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "seed1"})
	s2 := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "seed2"})
	if _, err := primary.Rels().AddByID(context.Background(), "KNOWS", s1.ID(), s2.ID(), nil); err != nil {
		t.Fatalf("seed KNOWS rel: %v", err)
	}

	cfg := graph.Config{SnowflakeNodeID: 2, ReadOnlyReplica: true}
	if badgerDir == "" {
		cfg.BadgerInMemory = true
	} else {
		cfg.BadgerDir = badgerDir
	}
	replica, err = graph.New(cfg)
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	t.Cleanup(func() { _ = replica.Close() })

	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	lsn0, err = primary.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if err := replica.Replication().SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}
	return primary, replica, lsn0
}

// changesSince collects every change record on the primary with LSN > after.
func changesSince(t *testing.T, primary *graph.Graph, after uint64) []store.ChangeRecord {
	t.Helper()
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(after, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange(%d): %v", after, err)
	}
	return recs
}

// TestReplicaApply_ChangeClearWipesAndReanchors pins the SYSTEM PROPERTY:
// applying a ChangeClear record on a read-only replica WIPES all live state,
// re-anchors the replica empty, AND advances the watermark — proving (a) the
// ChangeClear tag is handled (not dropped into the unknown-tag default) and
// (b) the apply path bypasses the read-only-replica write gate that would
// otherwise reject the Clear store door. If either regresses, the assertions
// below fail: a dropped tag errors out (no watermark advance, no wipe); a gate
// regression would make the apply error instead of clearing.
func TestReplicaApply_ChangeClearWipesAndReanchors(t *testing.T) {
	ctx := context.Background()
	primary, replica, lsn0 := newReplicaPair(t, t.TempDir())

	// Populate the replica with >=1 node + 1 rel via the real tail path, so the
	// Clear has something to wipe.
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"})
	b := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "b"})
	if _, err := primary.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), map[string]any{"w": int64(1)}); err != nil {
		t.Fatalf("seed rel: %v", err)
	}
	recs := changesSince(t, primary, lsn0)
	if len(recs) == 0 {
		t.Fatal("no records to tail before Clear")
	}
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges (populate): %v", err)
	}
	// Sanity: the replica is non-empty before the Clear.
	if pn, _ := replica.Nodes().All(graph.QueryOpts{}); len(pn) == 0 {
		t.Fatal("replica empty before Clear — populate failed, test would be vacuous")
	}

	applied, err := replica.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}

	// Apply a synthetic ChangeClear at the next LSN. Payload is empty (ChangeClear
	// carries no body). err==nil catches BOTH the unknown-tag drop AND a read-only
	// gate regression on the Clear store door.
	if err := replica.Replication().ApplyChange(store.ChangeRecord{LSN: applied + 1, Tag: store.ChangeClear}); err != nil {
		t.Fatalf("apply ChangeClear: PROPERTY VIOLATED — Clear must be handled and bypass the read-only gate, got: %v", err)
	}

	rn, err := replica.Nodes().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("replica Nodes().All after Clear: %v", err)
	}
	if len(rn) != 0 {
		t.Fatalf("PROPERTY VIOLATED: ChangeClear left %d nodes live, want 0", len(rn))
	}
	rr, err := replica.Rels().All(graph.QueryOpts{})
	if err != nil {
		t.Fatalf("replica Rels().All after Clear: %v", err)
	}
	if len(rr) != 0 {
		t.Fatalf("PROPERTY VIOLATED: ChangeClear left %d rels live, want 0", len(rr))
	}
	if got, _ := replica.Replication().AppliedLSN(); got != applied+1 {
		t.Fatalf("PROPERTY VIOLATED: watermark after ChangeClear = %d, want %d", got, applied+1)
	}
}

// TestReplicaApply_CorruptRecordFailsClosed pins the SYSTEM PROPERTY: a record
// that fails to decode / validate must FAIL CLOSED and must NOT advance the
// watermark — otherwise the bad LSN is permanently skipped and the replica
// diverges silently from the primary forever. Each sub-case applies a single
// record at LSN W+1 (above the watermark so it reaches the handler) and asserts
// the watermark stays at W.
func TestReplicaApply_CorruptRecordFailsClosed(t *testing.T) {
	primary, replica, lsn0 := newReplicaPair(t, "")

	// One real NodePut whose payload we can tamper for sub-case (b).
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "real"})
	recs := changesSince(t, primary, lsn0)
	var realPut store.ChangeRecord
	for _, r := range recs {
		if r.Tag == store.ChangeNodePut {
			realPut = r
			break
		}
	}
	if realPut.Tag != store.ChangeNodePut {
		t.Fatalf("setup: no NodePut record captured")
	}

	// Tamper the integrity hash inside the captured NodePut payload. The payload is
	// msgpack(NodePutBody{ w: NodeWire, wh: bool }); NodeWire.Hash is tag "h".
	tamperedHashPut := func() store.ChangeRecord {
		var body map[string]any
		if err := msgpack.Unmarshal(realPut.Payload, &body); err != nil {
			t.Fatalf("decode captured NodePut body: %v", err)
		}
		wire, ok := body["w"].(map[string]any)
		if !ok {
			t.Fatalf("captured NodePut body has no wire map, got %T", body["w"])
		}
		if _, ok := wire["h"]; !ok {
			t.Skipf("captured NodePut wire has no integrity hash field — cannot craft a hash-mismatch; skipping sub-case (b)")
		}
		wire["h"] = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef0"
		body["w"] = wire
		payload, err := msgpack.Marshal(body)
		if err != nil {
			t.Fatalf("re-marshal tampered body: %v", err)
		}
		return store.ChangeRecord{Tag: store.ChangeNodePut, Payload: payload}
	}()

	// Tamper the wire ID to 0: the body still decodes (valid msgpack) and the
	// PrimaryLabel token is still valid, so the apply path reaches
	// WireToNodeChecked, whose field validation rejects "id must be positive".
	// That apply-step trust-boundary rejection must classify as ErrCorruptExport
	// — like the bootstrap-import sibling (import.go) and the hash-mismatch case —
	// not escape as a bare unclassified error a consumer's errors.Is misses.
	tamperedZeroIDPut := func() store.ChangeRecord {
		var body map[string]any
		if err := msgpack.Unmarshal(realPut.Payload, &body); err != nil {
			t.Fatalf("decode captured NodePut body: %v", err)
		}
		wire, ok := body["w"].(map[string]any)
		if !ok {
			t.Fatalf("captured NodePut body has no wire map, got %T", body["w"])
		}
		wire["id"] = int64(0)
		body["w"] = wire
		payload, err := msgpack.Marshal(body)
		if err != nil {
			t.Fatalf("re-marshal tampered body: %v", err)
		}
		return store.ChangeRecord{Tag: store.ChangeNodePut, Payload: payload}
	}()

	cases := []struct {
		name     string
		rec      store.ChangeRecord
		wantWrap error  // nil = any non-nil error accepted
		mustHave string // substring the error message must contain (empty = skip)
	}{
		{
			// 0xc1 is the msgpack "never-used" byte → SafeUnmarshal fails closed
			// with ErrCorruptWire (NOT {0xff,0xff}, which decodes as a fixint pair
			// and yields a non-sentinel structural error).
			name:     "malformed_msgpack_is_ErrCorruptWire",
			rec:      store.ChangeRecord{Tag: store.ChangeNodePut, Payload: []byte{0xc1}},
			wantWrap: store.ErrCorruptWire,
		},
		{
			// Decodes fine, but the content no longer matches its integrity hash →
			// ErrCorruptExport family (recompute-AND-COMPARE on apply).
			name:     "wrong_integrity_hash_is_ErrCorruptExport",
			rec:      tamperedHashPut,
			wantWrap: graph.ErrCorruptExport,
		},
		{
			// Decodes fine and the token is valid, but the wire fails field
			// validation (id must be positive) at WireToNodeChecked — an apply-step
			// trust-boundary rejection that must classify as ErrCorruptExport.
			name:     "invalid_wire_field_is_ErrCorruptExport",
			rec:      tamperedZeroIDPut,
			wantWrap: graph.ErrCorruptExport,
		},
		{
			name:     "unknown_tag_errors",
			rec:      store.ChangeRecord{Tag: store.ChangeTag(250), Payload: nil},
			mustHave: "unknown change tag",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w, err := replica.Replication().AppliedLSN()
			if err != nil {
				t.Fatalf("AppliedLSN: %v", err)
			}
			rec := tc.rec
			rec.LSN = w + 1 // above watermark → reaches the handler
			applyErr := replica.Replication().ApplyChange(rec)
			if applyErr == nil {
				t.Fatalf("PROPERTY VIOLATED: corrupt record applied without error (would advance watermark to %d)", rec.LSN)
			}
			if tc.wantWrap != nil && !errors.Is(applyErr, tc.wantWrap) {
				t.Fatalf("err = %v, want wrapping %v", applyErr, tc.wantWrap)
			}
			if tc.mustHave != "" && !bytes.Contains([]byte(applyErr.Error()), []byte(tc.mustHave)) {
				t.Fatalf("err = %q, want substring %q", applyErr.Error(), tc.mustHave)
			}
			got, err := replica.Replication().AppliedLSN()
			if err != nil {
				t.Fatalf("AppliedLSN after fail: %v", err)
			}
			if got != w {
				t.Fatalf("PROPERTY VIOLATED: watermark advanced %d -> %d on a failing record", w, got)
			}
		})
	}
}

// TestReplicaApplyChanges_RejectsDuplicateLSN pins the SYSTEM PROPERTY: the
// ascending-LSN guard is STRICT (>), not merely non-decreasing (>=). A batch
// with two EQUAL consecutive LSNs must be rejected with ErrChangesNotAscending —
// a regression from "<=" to "<" in the guard would silently accept the duplicate
// (the second copy then no-ops on the watermark skip and the misorder is
// swallowed). The strictly-ascending prefix is still flushed and watermarked.
func TestReplicaApplyChanges_RejectsDuplicateLSN(t *testing.T) {
	primary, replica, lsn0 := newReplicaPair(t, "")

	// Three INDEPENDENT NodePuts past the snapshot (no cross-record dependency).
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "x"})
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "y"})
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "z"})

	recs := changesSince(t, primary, lsn0)
	if len(recs) < 3 {
		t.Fatalf("got %d records past snapshot, want >= 3", len(recs))
	}

	// Equal consecutive LSN: recs[1] repeated. The duplicate must trip the strict
	// guard, NOT be tolerated as a stale skip.
	dup := []store.ChangeRecord{recs[0], recs[1], recs[1]}
	applied, err := replica.Replication().ApplyChanges(dup)
	if !errors.Is(err, store.ErrChangesNotAscending) {
		t.Fatalf("PROPERTY VIOLATED: duplicate-LSN batch err = %v, want wrapping ErrChangesNotAscending", err)
	}
	// The strictly-ascending prefix (recs[0], recs[1]) was applied + watermarked.
	if applied != recs[1].LSN {
		t.Fatalf("applied LSN = %d, want %d (ascending prefix watermarked)", applied, recs[1].LSN)
	}
	if got, _ := replica.Replication().AppliedLSN(); got != recs[1].LSN {
		t.Fatalf("persisted watermark = %d, want %d", got, recs[1].LSN)
	}
}

// TestReplicaApplyChanges_GuardTracksAcrossStaleSkips pins the SYSTEM PROPERTY:
// the strict-ascending guard tracks the PREVIOUS RECORD'S LSN, not the applied
// watermark — so it keeps verifying order even across records skipped as stale
// (LSN <= watermark). A misordered record hidden behind stale records must still
// be rejected; and a well-ordered batch whose head is stale must still advance
// to its tail.
func TestReplicaApplyChanges_GuardTracksAcrossStaleSkips(t *testing.T) {
	primary, replica, lsn0 := newReplicaPair(t, "")

	// Apply a full ascending batch so the watermark advances to W.
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "x"})
	mustAdd(t, primary, []string{"A"}, map[string]any{"n": "y"})
	base := changesSince(t, primary, lsn0)
	if len(base) < 2 {
		t.Fatalf("got %d base records, want >= 2", len(base))
	}
	W, err := replica.Replication().ApplyChanges(base)
	if err != nil {
		t.Fatalf("ApplyChanges base: %v", err)
	}
	staleA, staleB := base[0], base[1] // both LSN <= W now

	// One more primary NodePut → a record strictly above W.
	nAbove := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "above"})
	above := changesSince(t, primary, W)
	if len(above) == 0 {
		t.Fatalf("no record above watermark W=%d", W)
	}
	recAboveW := above[len(above)-1]
	if recAboveW.LSN <= W {
		t.Fatalf("setup: recAboveW.LSN %d not > W %d", recAboveW.LSN, W)
	}

	// POSITIVE: [stale, stale, recAboveW] — heads are stale-skipped, tail applies.
	last, err := replica.Replication().ApplyChanges([]store.ChangeRecord{staleA, staleB, recAboveW})
	if err != nil {
		t.Fatalf("PROPERTY VIOLATED: well-ordered batch with stale prefix errored: %v", err)
	}
	if last != recAboveW.LSN {
		t.Fatalf("returned LSN = %d, want %d", last, recAboveW.LSN)
	}
	if got, _ := replica.Replication().AppliedLSN(); got != recAboveW.LSN {
		t.Fatalf("persisted watermark = %d, want %d", got, recAboveW.LSN)
	}
	if _, err := replica.Nodes().Get(context.Background(), nAbove.ID()); err != nil {
		t.Fatalf("recAboveW node not visible on replica: %v", err)
	}

	// NEGATIVE: a later record's LSN <= an earlier one across a stale head must
	// still error, even though both records would otherwise be stale/applied. We
	// build [staleB, staleA] where staleA.LSN < staleB.LSN — descending → the
	// guard (which tracks prevLSN, not the watermark) must fire.
	if !(staleA.LSN < staleB.LSN) {
		t.Fatalf("setup: expected staleA.LSN < staleB.LSN, got %d, %d", staleA.LSN, staleB.LSN)
	}
	_, err = replica.Replication().ApplyChanges([]store.ChangeRecord{staleB, staleA})
	if !errors.Is(err, store.ErrChangesNotAscending) {
		t.Fatalf("PROPERTY VIOLATED: descending stale batch err = %v, want wrapping ErrChangesNotAscending", err)
	}
}

// TestReplicaApply_DeleteOfMissingEntity_PerTag pins the SYSTEM PROPERTY: a
// delete record applied to a replica that does NOT have the entity is a tolerant
// no-op (err==nil), per delete shape — at-least-once redelivery / a replica that
// bootstrapped after the create must not crash on the delete. Drives each shape
// and applies its delete record to a FRESH replica that never saw the create.
//
// The two with-history shapes are captured from real primary mutations
// (Nodes().Delete / Rels().Delete). The two hard-delete shapes
// (DeleteNodeCascade / DeleteRelationship, WithHistory:false) are NOT reachable
// through public graph mutation doors — both standalone and tx delete doors route
// to the with-history path — so they are exercised with hand-crafted minimal
// bodies (msgpack {"id": <int64>}, WithHistory omitted = false), which is exactly
// the on-wire body shape the apply handler decodes.
func TestReplicaApply_DeleteOfMissingEntity_PerTag(t *testing.T) {
	ctx := context.Background()

	// captureWithHistoryDeletes returns the ChangeNodeDelete + ChangeRelDelete
	// records produced by deleting a connected node on a primary.
	captureWithHistoryDeletes := func(t *testing.T) (nodeDel, relDel store.ChangeRecord) {
		t.Helper()
		p, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
		if err != nil {
			t.Fatalf("primary New: %v", err)
		}
		defer p.Close()
		a := mustAdd(t, p, []string{"A"}, map[string]any{"n": "a"})
		b := mustAdd(t, p, []string{"A"}, map[string]any{"n": "b"})
		// Standalone rel delete → with-history ChangeRelDelete.
		r1, err := p.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), nil)
		if err != nil {
			t.Fatalf("seed rel: %v", err)
		}
		beforeRelDel, _ := p.Replication().LastCommittedLSN()
		if err := p.Rels().Delete(ctx, r1.ID()); err != nil {
			t.Fatalf("Rels().Delete: %v", err)
		}
		for _, rec := range changesSince(t, p, beforeRelDel) {
			if rec.Tag == store.ChangeRelDelete {
				relDel = rec
				break
			}
		}
		// Standalone node delete (now unconnected) → with-history ChangeNodeDelete.
		beforeNodeDel, _ := p.Replication().LastCommittedLSN()
		if err := p.Nodes().Delete(ctx, b.ID()); err != nil {
			t.Fatalf("Nodes().Delete: %v", err)
		}
		for _, rec := range changesSince(t, p, beforeNodeDel) {
			if rec.Tag == store.ChangeNodeDelete {
				nodeDel = rec
				break
			}
		}
		if nodeDel.Tag != store.ChangeNodeDelete {
			t.Fatalf("no with-history ChangeNodeDelete captured")
		}
		if relDel.Tag != store.ChangeRelDelete {
			t.Fatalf("no with-history ChangeRelDelete captured")
		}
		return nodeDel, relDel
	}

	nodeDelWH, relDelWH := captureWithHistoryDeletes(t)

	// Hand-crafted hard-delete bodies: msgpack {"id": <int64>}, WithHistory absent.
	hardBody := func(id int64) []byte {
		t.Helper()
		b, err := msgpack.Marshal(map[string]any{"id": id})
		if err != nil {
			t.Fatalf("marshal hard-delete body: %v", err)
		}
		return b
	}

	cases := []struct {
		name string
		rec  store.ChangeRecord
	}{
		{"node_with_history", nodeDelWH},
		{"rel_with_history", relDelWH},
		{"node_hard_cascade", store.ChangeRecord{Tag: store.ChangeNodeDelete, Payload: hardBody(123456789)}},
		{"rel_hard", store.ChangeRecord{Tag: store.ChangeRelDelete, Payload: hardBody(987654321)}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Fresh replica that NEVER applied the create — the entity is missing.
			_, replica, lsn0 := newReplicaPair(t, "")
			rec := tc.rec
			rec.LSN = lsn0 + 1 // above the bootstrap watermark → reaches the handler
			if err := replica.Replication().ApplyChange(rec); err != nil {
				t.Fatalf("PROPERTY VIOLATED: delete of a missing entity must be a tolerant no-op, got: %v", err)
			}
			// Tolerant no-op still advances the watermark (the record was consumed).
			if got, _ := replica.Replication().AppliedLSN(); got != rec.LSN {
				t.Fatalf("watermark = %d, want %d after tolerated delete", got, rec.LSN)
			}
		})
	}
}
