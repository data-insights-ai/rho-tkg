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

// NowTx must cover EVERY committed TxFrom, including stamps that entered the
// store by a path other than this session's c.now(): stamps applied verbatim
// on a read replica from a clock-skewed primary, and stamps imported from such
// a primary's snapshot. Persisted stamps reloaded on reopen after a burst
// outran the wall clock: TestNowTx_ReopenSafe (nowtx_test.go).

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
