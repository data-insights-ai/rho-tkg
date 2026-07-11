package graph_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store/tiered"
)

// End-to-end: a change-log-enabled graph surfaces its mutations through the
// g.Replication() sub-API, with a gap-free LSN watermark.
func TestReplication_EndToEnd(t *testing.T) {
	g, err := graph.New(graph.Config{SnowflakeNodeID: 1, BadgerInMemory: true, ChangeLog: true, SyncWrites: true})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer g.Close()
	ctx := context.Background()

	a, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Add a: %v", err)
	}
	b, err := g.Nodes().Add(ctx, []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("Add b: %v", err)
	}
	if _, err := g.Rels().Add(ctx, "KNOWS", a, b, map[string]any{"since": int64(2026)}); err != nil {
		t.Fatalf("Add rel: %v", err)
	}

	feed, err := g.Replication().ChangeFeed(0, 0)
	if err != nil {
		t.Fatalf("ChangeFeed: %v", err)
	}
	if len(feed) < 3 {
		t.Fatalf("feed has %d records, want >= 3 (2 node puts + 1 rel put)", len(feed))
	}
	// LSNs are gap-free and ascending.
	for i, r := range feed {
		if r.LSN != uint64(i+1) {
			t.Fatalf("record[%d] LSN = %d, want %d", i, r.LSN, i+1)
		}
	}
	// Watermark matches the feed tail.
	lsn, err := g.Replication().LastCommittedLSN()
	if err != nil {
		t.Fatalf("LastCommittedLSN: %v", err)
	}
	if lsn != uint64(len(feed)) {
		t.Fatalf("LastCommittedLSN = %d, want %d", lsn, len(feed))
	}

	nodePuts, relPuts := 0, 0
	for _, r := range feed {
		switch r.Tag {
		case storepkg.ChangeNodePut:
			nodePuts++
		case storepkg.ChangeRelPut:
			relPuts++
		}
	}
	if nodePuts < 2 || relPuts < 1 {
		t.Fatalf("feed tags: nodePuts=%d relPuts=%d, want >=2 node puts and >=1 rel put", nodePuts, relPuts)
	}

	// ForEachChange streams the same records; afterLSN cursor skips prefixes.
	var streamed int
	if err := g.Replication().ForEachChange(0, func(storepkg.ChangeRecord) bool {
		streamed++
		return true
	}); err != nil {
		t.Fatalf("ForEachChange: %v", err)
	}
	if streamed != len(feed) {
		t.Fatalf("ForEachChange streamed %d, want %d", streamed, len(feed))
	}
}

// A tiered store opened WITHOUT Config.ChangeLog now behaves exactly like a
// change-log-less badger: the capability is present (ADR-0005 §2 admitted tiered
// into changeFeedCapability) but reports an empty, inactive feed — not
// ErrCapabilityNotSupported. Enabling the log (a separate test) makes it emit.
func TestReplication_TieredWithoutChangeLogIsEmpty(t *testing.T) {
	ts, err := tiered.New(tiered.Config{
		InMemory:    true,
		RefLabels:   []string{"Person"},
		ShardWindow: 7 * 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("tiered.New: %v", err)
	}
	g, err := graph.New(graph.Config{SnowflakeNodeID: 2, Store: ts})
	if err != nil {
		t.Fatalf("graph.New: %v", err)
	}
	defer g.Close()

	if lsn, err := g.Replication().LastCommittedLSN(); err != nil || lsn != 0 {
		t.Fatalf("LastCommittedLSN = (%d, %v), want (0, nil)", lsn, err)
	}
	feed, err := g.Replication().ChangeFeed(0, 0)
	if err != nil || len(feed) != 0 {
		t.Fatalf("ChangeFeed = (%d recs, %v), want (0, nil)", len(feed), err)
	}
	if err := g.Replication().ForEachChange(0, func(storepkg.ChangeRecord) bool { return true }); err != nil {
		t.Fatalf("ForEachChange = %v, want nil", err)
	}
}

// The accessor and its methods are nil-safe.
func TestReplication_NilSafe(t *testing.T) {
	var g *graph.Graph
	api := g.Replication()
	if api != nil {
		t.Fatalf("nil graph Replication() = %v, want nil", api)
	}
	// Methods on the nil API fail closed with ErrNilGraph rather than panicking.
	if _, err := api.LastCommittedLSN(); !errors.Is(err, graph.ErrNilGraph) {
		t.Fatalf("nil API LastCommittedLSN = %v, want ErrNilGraph", err)
	}
}
