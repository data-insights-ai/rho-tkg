package core

import (
	"context"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// Backlog 49 review: GraphTx.AddRelationships writes graph-generated IDs,
// which have no earlier life, so on tiered its batch runs no earlier-lives
// walk — the walk would build the relationship history-presence set of every
// shard it asks. Same-shard and cross-shard rows both.
func TestAddRelationshipsGeneratedBatchSkipsEarlierLivesWalk(t *testing.T) {
	ts, err := tiered.New(tiered.Config{InMemory: true, RefLabels: []string{"Ref"}, ShardWindow: 7 * 24 * time.Hour, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	g, err := New(Config{Store: ts})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	ctx := context.Background()
	ref, err := g.Nodes.Add(ctx, []string{"Ref"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ev2, err := g.Nodes.Add(ctx, []string{"Ev"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := g.BeginTx()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.AddRelationships("R", []RelCreate{{StartID: ref.ID(), EndID: ev.ID()}, {StartID: ev.ID(), EndID: ev2.ID()}, {StartID: ev.ID(), EndID: ref.ID()}}); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	for name, s := range map[string]*tiered.BadgerStore{"reference": ts.RefShardForTest(), "hot": ts.HotShardForTest().Store()} {
		if s.HistoryPresenceStats().RelsBuilt {
			t.Errorf("%s shard: relationship history-presence set built by a generated-ID batch", name)
		}
	}
}
