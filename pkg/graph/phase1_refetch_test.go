package graph_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// staleSource serves a fixed snapshot — used to drive the stale-LSN guard.
type staleSource struct{ snap *store.RegistrySnapshot }

func (s staleSource) RegistrySnapshot() (*store.RegistrySnapshot, error) { return s.snap, nil }

// bootstrapReplica seeds a fresh read-only replica from the primary's current
// export and wires the primary's g.Replication() as its registry source.
func bootstrapReplica(t *testing.T, primary *graph.Graph, nodeID int64) *graph.Graph {
	t.Helper()
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	replica, err := graph.New(graph.Config{SnowflakeNodeID: nodeID, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	replica.SetReplicationSource(primary.Replication())
	return replica
}

func tailAll(t *testing.T, primary, replica *graph.Graph) error {
	t.Helper()
	from, err := replica.Replication().AppliedLSN()
	if err != nil {
		t.Fatalf("AppliedLSN: %v", err)
	}
	var recs []store.ChangeRecord
	if err := primary.Replication().ForEachChange(from, func(rec store.ChangeRecord) bool {
		recs = append(recs, rec)
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	_, err = replica.Replication().ApplyChanges(recs)
	return err
}

// A label / rel-type the primary registers AFTER the replica's bootstrap is
// resolved by an automatic registry refetch — the apply succeeds and converges
// byte-exact (the refetched tokens land at the same indices as the primary's,
// so the integrity hashes match).
func TestReplicaRefetch_ResolvesPostSnapshotTokens(t *testing.T) {
	ctx := context.Background()
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	a := mustAdd(t, primary, []string{"A"}, map[string]any{"n": "a"}) // registers label A only

	replica := bootstrapReplica(t, primary, 2) // replica registry: A
	defer replica.Close()

	// Post-snapshot the primary registers NEW tokens: label B and rel-type KNOWS.
	b := mustAdd(t, primary, []string{"B"}, map[string]any{"n": "b"})
	if _, err := primary.Rels().AddByID(ctx, "KNOWS", a.ID(), b.ID(), nil); err != nil {
		t.Fatalf("AddByID KNOWS: %v", err)
	}

	if err := tailAll(t, primary, replica); err != nil {
		t.Fatalf("tail apply (refetch should resolve B / KNOWS): %v", err)
	}
	assertConverged(t, "after refetch tail", primary, replica)

	// The replica resolved the new label by name (proves the refetch landed B at
	// the right token), and a fresh ByLabel("B") read sees the node.
	got, err := replica.Nodes().ByLabel("B", graph.QueryOpts{})
	if err != nil || len(got) != 1 || got[0].InternalID() != b.InternalID() {
		t.Fatalf("replica ByLabel(B) = (%d nodes, %v), want the post-snapshot node", len(got), err)
	}
}

// With no ReplicationSource configured, a post-snapshot token fails closed (the
// behaviour before refetch existed) — the apply errors and the watermark does
// not advance past the unresolved record.
func TestReplicaRefetch_NoSourceFailsClosed(t *testing.T) {
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	mustAdd(t, primary, []string{"A"}, nil)

	// Bootstrap WITHOUT wiring a source.
	var snap bytes.Buffer
	if err := primary.IO().Export(&snap); err != nil {
		t.Fatalf("Export: %v", err)
	}
	replica, err := graph.New(graph.Config{SnowflakeNodeID: 2, BadgerInMemory: true, ReadOnlyReplica: true})
	if err != nil {
		t.Fatalf("replica New: %v", err)
	}
	defer replica.Close()
	if err := replica.IO().Import(&snap, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("Import: %v", err)
	}
	before, _ := replica.Replication().AppliedLSN()

	// Post-snapshot new label B with no source to resolve it.
	mustAdd(t, primary, []string{"B"}, nil)
	if err := tailAll(t, primary, replica); err == nil {
		t.Fatal("expected apply to fail closed on an unresolved token with no source")
	}
	if after, _ := replica.Replication().AppliedLSN(); after != before {
		t.Fatalf("watermark advanced past an unresolved record: %d -> %d", before, after)
	}
}

// When the refetch source is behind the record (CapturedAtLSN < rec.LSN), apply
// surfaces the RETRYABLE ErrPrimaryRegistryStale sentinel through the full stack
// (g.Replication().ApplyChanges → core refetch), so a driver can branch on it.
func TestReplicaRefetch_StaleSourceSurfacesSentinel(t *testing.T) {
	primary, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("primary New: %v", err)
	}
	defer primary.Close()
	mustAdd(t, primary, []string{"A"}, nil)

	replica := bootstrapReplica(t, primary, 2)
	defer replica.Close()

	// Post-snapshot new label B (a token the replica lacks).
	mustAdd(t, primary, []string{"B"}, nil)

	// Point the replica at a source carrying the right names but a stale LSN (0).
	real, err := primary.Replication().RegistrySnapshot()
	if err != nil {
		t.Fatalf("RegistrySnapshot: %v", err)
	}
	replica.SetReplicationSource(staleSource{snap: &store.RegistrySnapshot{
		Labels: real.Labels, RelTypes: real.RelTypes, PropKeys: real.PropKeys, CapturedAtLSN: 0,
	}})

	err = tailAll(t, primary, replica)
	if !errors.Is(err, graph.ErrPrimaryRegistryStale) {
		t.Fatalf("tail with stale source = %v, want ErrPrimaryRegistryStale", err)
	}
}

// RegistrySnapshot captures the full token-name slices and anchors them at the
// change-log LSN. On a backend that implements the change-feed capability but
// has the log disabled (badger without ChangeLog), it returns the names with
// CapturedAtLSN 0 — consistent with LastCommittedLSN, and unusable as a refetch
// source (the CapturedAtLSN >= record-LSN guard rejects LSN 0 for any real
// record). The hard decline (ErrCapabilityNotSupported) is reserved for a
// backend lacking the capability entirely (tiered), shared with ChangeFeed.
func TestRegistrySnapshot_NoChangeLogCapturesNamesAtLSNZero(t *testing.T) {
	ctx := context.Background()
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer g.Close()
	mustAdd(t, g, []string{"A"}, nil)
	mustAdd(t, g, []string{"B"}, nil)

	snap, err := g.Replication().RegistrySnapshot()
	if err != nil {
		t.Fatalf("RegistrySnapshot: %v", err)
	}
	if snap.CapturedAtLSN != 0 {
		t.Fatalf("CapturedAtLSN = %d, want 0 (log disabled)", snap.CapturedAtLSN)
	}
	// Labels slice is [reserved, A, B].
	if len(snap.Labels) != 3 || snap.Labels[1] != "A" || snap.Labels[2] != "B" {
		t.Fatalf("Labels = %v, want [\"\" A B]", snap.Labels)
	}
	_ = ctx
}
