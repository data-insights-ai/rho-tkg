package core

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"

	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/memory"
)

// ── Break-round hardening: NowTx must cover EVERY committed TxFrom, including
// stamps that entered the store by a path other than this session's c.now():
//   (1) persisted stamps reloaded on reopen when a burst's monotonic floor
//       outran the wall clock (this file, _ReopenAfterBurst), and
//   (2) stamps applied verbatim on a read replica from a clock-skewed primary
//       (_ReplicaCoversAppliedFutureTxFrom).
// The existing TestNowTx_ReopenSafe adds a SINGLE node, so its floor never
// inflates and NowTx=wall>=atx passes trivially — the burst case is the one the
// lesson-61 contract ("NowTx >= every committed TxFrom across reopen") actually
// rides on.

// TestNowTx_ReopenAfterBurst_MonotonicFloorAnachronism: a bulk-import burst whose
// throughput exceeds 1 write/ms inflates persisted TxFrom above the wall via the
// per-Core monotonic floor (next=max(wall,last+1)). On reopen lastInstant resets
// to 0 and — unless the floor is reseeded from persisted state — NowTx()=c.now()
// returns just the wall value, BELOW the persisted max TxFrom. A pin at that
// NowTx silently excludes already-committed entities: the exact anachronism the
// pin exists to prevent. A frozen wall clock models the burst deterministically.
func TestNowTx_ReopenAfterBurst_MonotonicFloorAnachronism(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ctx := context.Background()

	g1, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("New g1: %v", err)
	}
	// Freeze the wall clock: every c.now() observes the same instant W, so the
	// per-Core floor inflates each successive TxFrom by +1ms ABOVE W — the same
	// shape a >1 write/ms production burst (bulk import) produces against a
	// slower-moving wall.
	frozen := time.Now().Add(time.Second)
	g1.SetClockForTest(t, func() time.Time { return frozen })

	const burst = 500
	var maxTx types.Instant
	for i := 0; i < burst; i++ {
		n, err := g1.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": i})
		if err != nil {
			t.Fatalf("add %d: %v", i, err)
		}
		maxTx = maxInstant(maxTx, txFromStamp(t, n.Temporal()))
	}
	if maxTx <= types.Instant(frozen.UnixMilli()) {
		t.Fatalf("test precondition broken: burst did not inflate the floor (maxTx=%d, wall=%d)", maxTx, frozen.UnixMilli())
	}
	if err := g1.Close(); err != nil {
		t.Fatalf("close g1: %v", err)
	}

	// Reopen the SAME data dir with the wall still frozen at W — now BELOW the
	// inflated max persisted TxFrom.
	g2, err := New(Config{BadgerDir: dir})
	if err != nil {
		t.Fatalf("reopen g2: %v", err)
	}
	defer g2.Close()
	g2.SetClockForTest(t, func() time.Time { return frozen })

	pin, err := g2.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx after reopen: %v", err)
	}
	if pin < maxTx {
		t.Fatalf("NowTx after reopen = %d < persisted MAX TxFrom %d — a monotonic-floor "+
			"burst outran the wall and the floor reset to 0 on reopen; a pin at NowTx() "+
			"silently excludes committed entities (lesson-61 anachronism)", pin, maxTx)
	}

	// A fresh write must still be stamped strictly greater than the reopened pin
	// AND strictly greater than every pre-reopen stamp (TX-time monotonic across
	// reopen — else a new write is ordered BEFORE an already-committed one).
	b, err := g2.Nodes.Add(ctx, []string{"T"}, map[string]any{"i": "post"})
	if err != nil {
		t.Fatalf("add after reopen: %v", err)
	}
	btx := txFromStamp(t, b.Temporal())
	if btx <= maxTx {
		t.Fatalf("post-reopen write stamped %d <= pre-reopen max %d — TX time went backwards across reopen", btx, maxTx)
	}

	// Downstream consequence: the generic TxAt door must still see the whole burst.
	waitWallPast(maxInstant(maxTx, pin, btx))
	rows, err := g2.Nodes.ByLabel("T", storepkg.QueryOpts{TxAt: pin})
	if err != nil {
		t.Fatalf("ByLabel(TxAt=pin): %v", err)
	}
	if len(rows) != burst {
		t.Fatalf("pin at NowTx dropped %d of %d committed nodes", burst-len(rows), burst)
	}
}

// TestNowTx_ReplicaCoversAppliedFutureTxFrom: on a read replica, a primary's
// change record is applied VERBATIM — its TxFrom is the primary's stamp, which
// under NTP skew (replica behind primary) or the primary's own monotonic-floor
// burst can exceed the replica's wall clock. Apply reproduces the row but must
// also advance the replica's commit-clock floor so NowTx() still covers every
// APPLIED (committed) TxFrom. Without that, NowTx()=replica-wall < applied
// TxFrom, and a pin at NowTx silently excludes already-applied data — the exact
// anachronism the pin exists to prevent, on a supported (replica) configuration
// serving AS-OF reads.
func TestNowTx_ReplicaCoversAppliedFutureTxFrom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	// Primary with a change log; wall clock pinned an hour AHEAD of the replica.
	primary, err := New(Config{Store: memory.New(memory.WithChangeLog()), SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	skew := time.Now().Add(time.Hour)
	primary.SetClockForTest(t, func() time.Time { return skew })

	// Replica on the REAL wall clock — an hour behind the primary's stamps.
	replica, err := New(Config{Store: memory.New(), ReadOnlyReplica: true, SnowflakeNodeID: 2})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()

	// Seed label token "T" into the bootstrap snapshot so the post-bootstrap
	// apply reuses a known token (no registry refetch needed).
	if _, err := primary.Nodes.Add(ctx, []string{"T"}, map[string]any{"seed": true}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Bootstrap the replica from a snapshot + handoff LSN.
	var snap bytes.Buffer
	if err := primary.IO.Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	lsn0, err := primary.Repl.LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if err := replica.IO.Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("replica Import: %v", err)
	}
	if err := replica.Repl.SetAppliedLSN(lsn0); err != nil {
		t.Fatalf("SetAppliedLSN: %v", err)
	}

	// Commit a node on the primary AFTER bootstrap (travels via apply), with an
	// explicit PAST world-time so valid-time never masks the TX-time question.
	vf := types.Instant(1_700_000_000_000)
	a, err := primary.Nodes.Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": vf})
	if err != nil {
		t.Fatalf("primary Add: %v", err)
	}
	aTx := txFromStamp(t, a.Temporal())

	// Tail + apply the record to the replica.
	var recs []storepkg.ChangeRecord
	if err := primary.Repl.ForEachChange(lsn0, func(rec storepkg.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if len(recs) == 0 {
		t.Fatal("no change records past the snapshot LSN — nothing to tail")
	}
	if _, err := replica.Repl.ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges: %v", err)
	}

	// Sanity: apply reproduced the future TxFrom verbatim (not re-stamped).
	ra, err := replica.Nodes.Get(ctx, a.ID())
	if err != nil {
		t.Fatalf("replica Get: %v", err)
	}
	if rt := ra.Temporal(); rt.TxFrom != aTx {
		t.Fatalf("apply did not reproduce TxFrom verbatim: replica %d != primary %d", rt.TxFrom, aTx)
	}

	// The contract: NowTx() on the replica must cover the already-applied TxFrom.
	pin, err := replica.Temporal.NowTx()
	if err != nil {
		t.Fatalf("replica NowTx: %v", err)
	}
	if pin < aTx {
		t.Fatalf("replica NowTx = %d < applied TxFrom %d — apply did not advance the "+
			"commit-clock floor; a pin at NowTx() excludes already-applied committed data", pin, aTx)
	}

	// Downstream: the named AS-OF door pinned at NowTx must include the applied node.
	asof, err := replica.Temporal.NodesAsOf(pin)
	if err != nil {
		t.Fatalf("NodesAsOf(pin): %v", err)
	}
	found := false
	for _, n := range asof {
		if n.ID() == a.ID() {
			found = true
		}
	}
	if !found {
		t.Fatalf("NodesAsOf(NowTx) on the replica excluded the applied node (aTx=%d, pin=%d)", aTx, pin)
	}
}

// TestNowTx_BootstrapImportCoversFutureTxFrom: a graph that bootstraps from a
// snapshot whose rows were stamped by a clock-skewed / bursting primary (TxFrom
// ahead of the importer's wall) must cover those imported stamps via NowTx —
// Import advances the commit-clock floor. This is the sibling of the replica-tail
// case: a bootstrap-only replica serving AS-OF reads before its first tail would
// otherwise silently under-cover every imported row.
func TestNowTx_BootstrapImportCoversFutureTxFrom(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	primary, err := New(Config{Store: memory.New(), SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	skew := time.Now().Add(time.Hour)
	primary.SetClockForTest(t, func() time.Time { return skew })

	a, err := primary.Nodes.Add(ctx, []string{"T"}, map[string]any{"tkg_valid_from": types.Instant(1_700_000_000_000)})
	if err != nil {
		t.Fatalf("primary Add: %v", err)
	}
	aTx := txFromStamp(t, a.Temporal())

	var snap bytes.Buffer
	if err := primary.IO.Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}

	// Import into a fresh graph on the real (behind) wall clock.
	dst, err := New(Config{Store: memory.New(), SnowflakeNodeID: 2})
	if err != nil {
		t.Fatalf("dst New: %v", err)
	}
	defer dst.Close()
	if err := dst.IO.Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}

	pin, err := dst.Temporal.NowTx()
	if err != nil {
		t.Fatalf("NowTx after import: %v", err)
	}
	if pin < aTx {
		t.Fatalf("NowTx after bootstrap import = %d < imported TxFrom %d — Import did not "+
			"advance the commit-clock floor; a pin at NowTx() excludes imported rows", pin, aTx)
	}
	asof, err := dst.Temporal.NodesAsOf(pin)
	if err != nil {
		t.Fatalf("NodesAsOf(pin): %v", err)
	}
	if len(asof) != 1 || asof[0].ID() != a.ID() {
		t.Fatalf("NodesAsOf(NowTx) after import = %d rows, want the imported node", len(asof))
	}
}
