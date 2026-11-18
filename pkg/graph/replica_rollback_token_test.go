package graph_test

import (
	"context"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/internal/testtier"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
)

// A rolled-back transaction that allocated a NEW rel-type/label token must NOT
// poison the change-log feed. The tx body emits a durable Put record referencing
// the freshly-allocated token (records are emitted IN-BACKEND, before rollback);
// if Rollback then DE-allocates the token (restoreRegistries rebuilding from the
// pre-tx snapshot), the feed permanently references a token the primary no longer
// holds. A replica — even WITH a ReplicationSource — can never resolve it (the
// refetch finds it absent, since the primary rolled it back too) and is stalled
// forever; worse, the next committed allocation REUSES the token number for a
// different name, silently diverging the replica.
//
// The replication invariant is that registries are APPEND-ONLY. When
// the change-log is enabled, rollback must keep tx-allocated tokens (an
// allocated-but-unused token is harmless) so every emitted record stays
// resolvable. This test pins convergence for a rolled-back NEW-token tx.
func TestReplicaConvergence_RolledBackNewTokenTx(t *testing.T) {
	testtier.Slow(t)
	t.Parallel()
	ctx := context.Background()
	primary, replica, lsn0 := newReplicaPair(t, "")
	// In-process refetch source so the replica CAN resolve post-bootstrap tokens.
	replica.SetReplicationSource(primary.Replication())
	seeds, err := primary.Nodes().All(graph.QueryOpts{})
	if err != nil || len(seeds) != 2 {
		t.Fatalf("seed nodes: %d, %v; want 2", len(seeds), err)
	}
	a, b := seeds[0], seeds[1]

	// Tx allocates rel-type "LINK" (a brand-new token), then ROLLS BACK.
	tx, err := primary.Tx().Begin()
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if _, err := tx.AddRelationshipByID("LINK", a.ID(), b.ID(), nil); err != nil {
		t.Fatalf("tx AddRelationshipByID: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Tail the rolled-back churn. The replica must converge, not stall.
	recs := changesSince(t, primary, lsn0)
	if _, err := replica.Replication().ApplyChanges(recs); err != nil {
		t.Fatalf("ApplyChanges STALLED on rolled-back-token churn (feed poisoned by de-allocated token): %v", err)
	}
	assertConverged(t, "after rolled-back new-token tx", primary, replica)

	// The next COMMITTED allocation must NOT reuse the rolled-back token in a way
	// that diverges the replica. Commit a different rel-type and a node label,
	// then converge again.
	if _, err := primary.Rels().AddByID(ctx, "FRIEND", a.ID(), b.ID(), nil); err != nil {
		t.Fatalf("commit FRIEND: %v", err)
	}
	if err := primary.Nodes().AddLabel(ctx, a.ID(), "Tagged"); err != nil {
		t.Fatalf("commit AddLabel: %v", err)
	}
	applied, _ := replica.Replication().AppliedLSN()
	recs2 := changesSince(t, primary, applied)
	if _, err := replica.Replication().ApplyChanges(recs2); err != nil {
		t.Fatalf("ApplyChanges (post-rollback committed ops): %v", err)
	}
	assertConverged(t, "after post-rollback committed ops", primary, replica)
}
