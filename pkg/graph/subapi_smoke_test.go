package graph_test

import (
	"bytes"
	"context"
	"testing"

	graphpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph"
	tkgio "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/io"
	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
)

// TestSubAPISmoke exercises every sub-API accessor at least once to verify
// (a) the field is wired in Graph.New, (b) the wrapper forwards correctly to
// the underlying *Graph method. This is a compile-and-run sanity test, not a
// substitute for the per-feature test files.
func TestSubAPISmoke(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	// Nodes
	a, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Alice"})
	if err != nil {
		t.Fatalf("Nodes.Add: %v", err)
	}
	b, err := g.Nodes().Add(context.Background(), []string{"Person"}, map[string]any{"name": "Bob"})
	if err != nil {
		t.Fatalf("Nodes.AddWithContext: %v", err)
	}
	if got, _ := g.Nodes().Get(context.Background(), a.ID()); got == nil || got.ID() != a.ID() {
		t.Fatalf("Nodes.Get returned %v", got)
	}
	if cnt, _ := g.Nodes().Count(); cnt != 2 {
		t.Fatalf("Nodes.Count = %d, want 2", cnt)
	}
	if cnt, _ := g.Nodes().CountByLabel("Person"); cnt != 2 {
		t.Fatalf("Nodes.CountByLabel(Person) = %d, want 2", cnt)
	}
	if !g.Nodes().HasLabel(a, "Person") {
		t.Fatalf("Nodes.HasLabel returned false")
	}
	if labels := g.Nodes().Labels(a); len(labels) != 1 || labels[0] != "Person" {
		t.Fatalf("Nodes.Labels = %v", labels)
	}
	if pl := g.Nodes().PrimaryLabel(a); pl != "Person" {
		t.Fatalf("Nodes.PrimaryLabel = %q", pl)
	}
	persons, err := g.Nodes().ByLabel("Person", storepkg.QueryOpts{})
	if err != nil || len(persons) != 2 {
		t.Fatalf("Nodes.ByLabel: %v len=%d", err, len(persons))
	}

	// Rels
	r, err := g.Rels().Add(context.Background(), "KNOWS", a, b, map[string]any{"since": int64(2026)})
	if err != nil {
		t.Fatalf("Rels.Add: %v", err)
	}
	if got, _ := g.Rels().Get(context.Background(), r.ID()); got == nil || got.ID() != r.ID() {
		t.Fatalf("Rels.Get returned %v", got)
	}
	if cnt, _ := g.Rels().Count(); cnt != 1 {
		t.Fatalf("Rels.Count = %d, want 1", cnt)
	}
	if !g.Rels().HasType(r, "KNOWS") {
		t.Fatalf("Rels.HasType returned false")
	}
	if typ := g.Rels().Type(r); typ != "KNOWS" {
		t.Fatalf("Rels.Type = %q", typ)
	}
	out, err := g.Rels().Outgoing(a.ID(), "KNOWS")
	if err != nil || len(out) != 1 {
		t.Fatalf("Rels.Outgoing: %v len=%d", err, len(out))
	}

	// Stats
	if cnt, _ := g.Stats().NodeCount(); cnt != 2 {
		t.Fatalf("Stats.NodeCount = %d", cnt)
	}
	if cnt, _ := g.Stats().RelCount(); cnt != 1 {
		t.Fatalf("Stats.RelCount = %d", cnt)
	}
	if snap, _ := g.Stats().Get(); snap.NodesAdded != 2 || snap.RelsAdded != 1 {
		t.Fatalf("Stats.Get = %+v, want NodesAdded=2 RelsAdded=1", snap)
	}

	// Hash
	ok, err := g.Hash().VerifyNodeChain(a.ID())
	if err != nil || !ok {
		t.Fatalf("Hash.VerifyNodeChain: %v ok=%v", err, ok)
	}
	ok, err = g.Hash().VerifyRelChain(r.ID())
	if err != nil || !ok {
		t.Fatalf("Hash.VerifyRelChain: %v ok=%v", err, ok)
	}

	// Resolve
	if v, ok := g.Resolve().NodeProperty(a, "name"); !ok || v != "Alice" {
		t.Fatalf("Resolve.NodeProperty: ok=%v v=%v", ok, v)
	}

	// Index — create + drop
	if err := g.Index().CreateProperty("Person", "name"); err != nil {
		t.Fatalf("Index.CreateProperty: %v", err)
	}
	if has, err := g.Index().HasProperty("Person", "name"); err != nil || !has {
		t.Fatalf("Index.HasProperty: has=%v err=%v", has, err)
	}
	if err := g.Index().DeleteProperty("Person", "name"); err != nil {
		t.Fatalf("Index.DeleteProperty: %v", err)
	}
	if has, err := g.Index().HasProperty("Person", "name"); err != nil || has {
		t.Fatalf("Index.HasProperty after drop: has=%v err=%v", has, err)
	}

	// Index — rel property introspection.
	if err := g.Index().CreateRelProperty("KNOWS", "since"); err != nil {
		t.Fatalf("Index.CreateRelProperty: %v", err)
	}
	if has, err := g.Index().HasRelProperty("KNOWS", "since"); err != nil || !has {
		t.Fatalf("Index.HasRelProperty: has=%v err=%v", has, err)
	}
	if err := g.Index().DeleteRelProperty("KNOWS", "since"); err != nil {
		t.Fatalf("Index.DeleteRelProperty: %v", err)
	}

	// Index — temporal introspection.
	if err := g.Index().CreateTemporal("Person"); err != nil {
		t.Fatalf("Index.CreateTemporal: %v", err)
	}
	if has, err := g.Index().HasTemporal("Person"); err != nil || !has {
		t.Fatalf("Index.HasTemporal: has=%v err=%v", has, err)
	}
	if err := g.Index().DeleteTemporal("Person"); err != nil {
		t.Fatalf("Index.DeleteTemporal: %v", err)
	}

	// Index — vector introspection.
	if err := g.Index().CreateVector("Person", "embedding", 3, storepkg.DistanceCosine); err != nil {
		t.Fatalf("Index.CreateVector: %v", err)
	}
	if info, found, err := g.Index().VectorIndexInfo("Person", "embedding"); err != nil || !found || info.Dims != 3 {
		t.Fatalf("Index.VectorIndexInfo: info=%+v found=%v err=%v", info, found, err)
	}
	if err := g.Index().DeleteVector("Person", "embedding"); err != nil {
		t.Fatalf("Index.DeleteVector: %v", err)
	}

	// Index — composite create + query + drop.
	if err := g.Index().CreateComposite("Person", []string{"name", "age"}); err != nil {
		t.Fatalf("Index.CreateComposite: %v", err)
	}
	if _, err := g.Nodes().Update(context.Background(), a.ID(), map[string]any{"age": int64(30)}); err != nil {
		t.Fatalf("Nodes.Update: %v", err)
	}
	composite, err := g.Nodes().ByLabelAndProperties("Person", map[string]any{"name": "Alice", "age": int64(30)}, storepkg.QueryOpts{})
	if err != nil || len(composite) != 1 || composite[0].ID() != a.ID() {
		t.Fatalf("Nodes.ByLabelAndProperties: %v len=%d", err, len(composite))
	}
	if err := g.Index().DeleteComposite("Person", []string{"name", "age"}); err != nil {
		t.Fatalf("Index.DeleteComposite: %v", err)
	}

	// IO — round-trip Export/Import via in-memory graph.
	var exported bytes.Buffer
	if err := g.IO().Export(&exported); err != nil {
		t.Fatalf("IO.Export: %v", err)
	}
	dst, err := graphpkg.New(graphpkg.Config{SnowflakeNodeID: 1})
	if err != nil {
		t.Fatalf("New(dst): %v", err)
	}
	t.Cleanup(func() { _ = dst.Close() })
	if err := dst.IO().Import(&exported, tkgio.ImportOptions{}); err != nil {
		t.Fatalf("IO.Import: %v", err)
	}

	// Tx — round trip
	if err := g.Tx().Run(func(tx *graphpkg.GraphTx) error {
		_, err := tx.AddNode([]string{"Place"}, map[string]any{"name": "Vienna"})
		return err
	}); err != nil {
		t.Fatalf("Tx.Run: %v", err)
	}
	if cnt, _ := g.Stats().NodeCount(); cnt != 3 {
		t.Fatalf("after Tx.Run, NodeCount = %d, want 3", cnt)
	}

	// Batch
	bb, err := g.Batch().New()
	if err != nil {
		t.Fatalf("Batch.New: %v", err)
	}
	if _, err := bb.AddNode([]string{"Org"}, map[string]any{"name": "BDS"}); err != nil {
		t.Fatalf("Batch.AddNode: %v", err)
	}
	if _, err := bb.Execute(); err != nil {
		t.Fatalf("Batch.Execute: %v", err)
	}
	if cnt, _ := g.Stats().NodeCount(); cnt != 4 {
		t.Fatalf("after Batch.Execute, NodeCount = %d, want 4", cnt)
	}

	// Admin: DecomposeNodeID — works on any store
	comp := g.Admin().DecomposeNodeID(a.ID())
	if comp.CreatedAt.IsZero() {
		t.Fatalf("Admin.DecomposeNodeID returned zero CreatedAt")
	}

	// Events: install/get sync bus
	_ = g.Events().SetSync(nil) // tolerated; clears
	_ = g.Events().GetSync()

	// Replication: accessor reachable end-to-end. The default (memory) backend
	// implements the capability, so with no change-log enabled the watermark is
	// an empty 0 (not an error — ErrCapabilityNotSupported is the tiered case,
	// covered in replication_test.go).
	if lsn, err := g.Replication().LastCommittedLSN(); err != nil || lsn != 0 {
		t.Errorf("Replication().LastCommittedLSN() = (%d, %v), want (0, nil)", lsn, err)
	}

	// Constraints
	cs := g.Constraints().Get()
	if err := g.Constraints().Set(cs); err != nil {
		t.Fatalf("Constraints.Set: %v", err)
	}

	// Temporal — point-in-time read
	persons2, err := g.Nodes().ByLabel("Person", storepkg.QueryOpts{})
	if err != nil {
		t.Fatalf("Nodes.ByLabel: %v", err)
	}
	if len(persons2) != 2 {
		t.Fatalf("ByLabel(Person) = %d", len(persons2))
	}
	// Snapshot now
	now := persons2[0].Temporal().ValidFrom
	snap, err := g.Temporal().Snapshot(now)
	if err != nil {
		t.Fatalf("Temporal.Snapshot: %v", err)
	}
	if snap == nil {
		t.Fatalf("Temporal.Snapshot returned nil")
	}
}

func TestTxBeginWrapper(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	tx, err := g.Tx().Begin()
	if err != nil {
		t.Fatalf("Tx.Begin: %v", err)
	}
	if _, err := tx.AddNode([]string{"Person"}, map[string]any{"name": "Alice"}); err != nil {
		_ = tx.Rollback()
		t.Fatalf("tx.AddNode: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("tx.Commit: %v", err)
	}
	if cnt, err := g.Nodes().Count(); err != nil || cnt != 1 {
		t.Fatalf("Nodes.Count after tx commit = %d, %v; want 1, nil", cnt, err)
	}
}

func TestNewBatchBuilderWrapper(t *testing.T) {
	t.Parallel()
	g, err := graphpkg.New(graphpkg.Config{})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = g.Close() })

	bb, err := graphpkg.NewBatchBuilder(g)
	if err != nil {
		t.Fatalf("NewBatchBuilder: %v", err)
	}
	if _, err := bb.AddNode([]string{"Person"}, map[string]any{"name": "Bob"}); err != nil {
		t.Fatalf("Batch.AddNode: %v", err)
	}
	if _, err := bb.Execute(); err != nil {
		t.Fatalf("Batch.Execute: %v", err)
	}
	if cnt, err := g.Nodes().Count(); err != nil || cnt != 1 {
		t.Fatalf("Nodes.Count after batch execute = %d, %v; want 1, nil", cnt, err)
	}
}
