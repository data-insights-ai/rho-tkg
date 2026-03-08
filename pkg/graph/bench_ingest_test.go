package graph

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"testing"

	snowflake "github.com/bds421/rho-snowflake-2026"
	"gitlab2024.bds421-cloud.com/bds421/rho/tkg/v3/pkg/types"
)

// benchGraph creates an in-memory graph for benchmarking.
func benchGraph(b *testing.B) *Graph {
	b.Helper()
	g, err := New(Config{SnowflakeNodeID: 1, Store: NewMemoryStore()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })
	return g
}

// benchProps returns a realistic property map with 2 properties.
func benchProps() map[string]any {
	return map[string]any{"seq": 42, "group": "g7"}
}

// --- Component benchmarks: isolate each step of AddNode ---

func BenchmarkSnowflakeIDGen(b *testing.B) {
	g := benchGraph(b)
	b.ResetTimer()
	for b.Loop() {
		g.NextNodeID()
	}
}

func BenchmarkNewPropertySlice(b *testing.B) {
	b.ResetTimer()
	for b.Loop() {
		types.NewPropertySlice(benchProps())
	}
}

func BenchmarkNewNode(b *testing.B) {
	b.ResetTimer()
	for b.Loop() {
		types.NewNode(12345, 1, nil)
	}
}

func BenchmarkComputeNodeHash(b *testing.B) {
	g := benchGraph(b)
	ps, _ := types.NewPropertySlice(benchProps())
	n := types.NewNode(g.NextNodeID(), 1, nil)
	n.SetProperties(ps)
	labels := []string{"LoadTest"}
	b.ResetTimer()
	for b.Loop() {
		ComputeNodeHash(n, labels)
	}
}

func BenchmarkNodeDeepCopy(b *testing.B) {
	g := benchGraph(b)
	ps, _ := types.NewPropertySlice(benchProps())
	n := types.NewNode(g.NextNodeID(), 1, nil)
	n.SetProperties(ps)
	n.SetTemporal(&types.TemporalMetadata{})
	n.SetIntegrity(&types.NodeIntegrity{Hash: "abc123"})
	b.ResetTimer()
	for b.Loop() {
		n.DeepCopy()
	}
}

func BenchmarkMemoryStorePutNode(b *testing.B) {
	ms := NewMemoryStore()
	g := benchGraph(b)
	// Pre-build nodes to isolate PutNode cost
	nodes := make([]*types.Node, b.N)
	for i := range nodes {
		ps, _ := types.NewPropertySlice(benchProps())
		n := types.NewNode(g.NextNodeID(), 1, nil)
		n.SetProperties(ps)
		n.SetTemporal(&types.TemporalMetadata{})
		n.SetIntegrity(&types.NodeIntegrity{Hash: "abc"})
		nodes[i] = n
	}
	b.ResetTimer()
	for i := range b.N {
		if err := ms.PutNode(nodes[i]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkComputeRelHash(b *testing.B) {
	g := benchGraph(b)
	ps, _ := types.NewPropertySlice(map[string]any{"weight": 1})
	r := types.NewRelationship(g.NextRelID(), 1, 100, 200)
	r.SetProperties(ps)
	b.ResetTimer()
	for b.Loop() {
		ComputeRelHash(r, "KNOWS")
	}
}

func BenchmarkMemoryStorePutRel(b *testing.B) {
	ms := NewMemoryStore()
	g := benchGraph(b)
	// Create two anchor nodes
	n1 := types.NewNode(g.NextNodeID(), 1, nil)
	n1.SetTemporal(&types.TemporalMetadata{})
	n1.SetIntegrity(&types.NodeIntegrity{Hash: "a"})
	n2 := types.NewNode(g.NextNodeID(), 1, nil)
	n2.SetTemporal(&types.TemporalMetadata{})
	n2.SetIntegrity(&types.NodeIntegrity{Hash: "b"})
	ms.PutNode(n1)
	ms.PutNode(n2)
	startID := n1.InternalID().SnowflakeID()
	endID := n2.InternalID().SnowflakeID()
	// Pre-build rels
	rels := make([]*types.Relationship, b.N)
	for i := range rels {
		ps, _ := types.NewPropertySlice(map[string]any{"weight": 1})
		r := types.NewRelationship(g.NextRelID(), 1, startID, endID)
		r.SetProperties(ps)
		r.SetTemporal(&types.TemporalMetadata{})
		r.SetIntegrity(&types.RelIntegrity{Hash: "h"})
		rels[i] = r
	}
	b.ResetTimer()
	for i := range b.N {
		if err := ms.PutRelationship(rels[i]); err != nil {
			b.Fatal(err)
		}
	}
}

// --- End-to-end benchmarks ---

func BenchmarkAddNode(b *testing.B) {
	g := benchGraph(b)
	labels := []string{"LoadTest"}
	b.ResetTimer()
	for b.Loop() {
		if _, err := g.AddNode(labels, benchProps()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddNodeMemStore(b *testing.B) {
	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })
	labels := []string{"LoadTest"}
	b.ResetTimer()
	for b.Loop() {
		if _, err := g.AddNode(labels, benchProps()); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddRelationship(b *testing.B) {
	g := benchGraph(b)
	// Pre-create node pairs — realistic: 1 node with up to 3 rels
	type pair struct{ start, end *types.Node }
	pairs := make([]pair, b.N)
	for i := range pairs {
		n1, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i})
		n2, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i + 1000000})
		pairs[i] = pair{n1, n2}
	}
	b.ResetTimer()
	for i := range b.N {
		if _, err := g.AddRelationship("KNOWS", pairs[i].start, pairs[i].end,
			map[string]any{"weight": 1}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAddRelationshipMemStore(b *testing.B) {
	g, err := New(Config{Store: NewMemoryStore()})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { g.Close() })
	type pair struct{ start, end *types.Node }
	pairs := make([]pair, b.N)
	for i := range pairs {
		n1, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i})
		n2, _ := g.AddNode([]string{"Person"}, map[string]any{"seq": i + 1000000})
		pairs[i] = pair{n1, n2}
	}
	b.ResetTimer()
	for i := range b.N {
		if _, err := g.AddRelationship("KNOWS", pairs[i].start, pairs[i].end,
			map[string]any{"weight": 1}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAddNodeTx measures AddNode inside a transaction (amortized tx overhead).
func BenchmarkAddNodeTx(b *testing.B) {
	g := benchGraph(b)
	labels := []string{"LoadTest"}
	batchSize := 2000
	b.ResetTimer()
	for i := 0; i < b.N; i += batchSize {
		count := batchSize
		if i+count > b.N {
			count = b.N - i
		}
		tx := g.BeginTx()
		for j := range count {
			seq := i + j
			if _, err := tx.AddNode(labels, map[string]any{"seq": seq, "group": fmt.Sprintf("g%d", seq%10)}); err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}
	}
}

// --- Raw SHA-256 baseline for comparison ---

func BenchmarkRawSHA256(b *testing.B) {
	data := []byte("LoadTest:12345:0:seq=42,group=g7")
	b.ResetTimer()
	for b.Loop() {
		h := sha256.Sum256(data)
		hex.EncodeToString(h[:])
	}
}

// --- PropertySlice.DeepCopy ---

func BenchmarkPropertySliceDeepCopy(b *testing.B) {
	ps, _ := types.NewPropertySlice(benchProps())
	b.ResetTimer()
	for b.Loop() {
		ps.DeepCopy()
	}
}

// --- Validation only ---

func BenchmarkValidateProperties(b *testing.B) {
	props := benchProps()
	b.ResetTimer()
	for b.Loop() {
		for _, v := range props {
			types.ValidatePropertyValue(v)
		}
	}
}

// --- Label registry lookup ---

func BenchmarkLabelRegistryGetOrCreate(b *testing.B) {
	g := benchGraph(b)
	// Pre-create so it's a lookup, not a create
	g.GetOrCreateLabel("LoadTest")
	b.ResetTimer()
	for b.Loop() {
		g.GetOrCreateLabel("LoadTest")
	}
}

// --- Sort for PropertySlice (the sort.Slice overhead) ---

func BenchmarkSortTwoProps(b *testing.B) {
	b.ResetTimer()
	for b.Loop() {
		ps := []struct{ key string }{{"seq"}, {"group"}}
		sort.Slice(ps, func(i, j int) bool { return ps[i].key < ps[j].key })
	}
}

// --- Entity lock overhead ---

func BenchmarkEntityLockUnlock(b *testing.B) {
	locks := newEntityLockManager()
	var id1, id2 snowflake.ID
	id1 = 100
	id2 = 200
	b.ResetTimer()
	for b.Loop() {
		locks.LockTwo(id1, id2)
		locks.UnlockTwo(id1, id2)
	}
}

// --- context.Background check ---

func BenchmarkCheckCtx(b *testing.B) {
	ctx := context.Background()
	b.ResetTimer()
	for b.Loop() {
		checkCtx(ctx)
	}
}
