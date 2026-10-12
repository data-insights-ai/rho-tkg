package memory

import (
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// BenchmarkAdjSetChurn is the adjacency write cost of round 5 request 2
// (small sets kept in ID order): per op, a node's set of 32 members gets one
// add and one remove, IDs arriving in random order.
func BenchmarkAdjSetChurn(b *testing.B) {
	ids := make([]types.RelID, 4096)
	x := uint64(88172645463325252)
	for i := range ids {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		ids[i] = types.RelID(1 + x%1_000_000)
	}
	s := &adjSet{}
	for _, id := range ids[:32] {
		s.add(id)
	}
	i := 32
	for b.Loop() {
		s.add(ids[i%len(ids)])
		s.remove(ids[(i-32)%len(ids)])
		i++
	}
}

// BenchmarkPutRelationshipSmallSets is the end-to-end write the adjacency
// change sits on: PutRelationship between random nodes of 100,000 (sets stay
// small), one relationship per op.
func BenchmarkPutRelationshipSmallSets(b *testing.B) {
	ms := New()
	b.Cleanup(func() { _ = ms.Close() })
	const nodes = 100_000
	for i := int64(1); i <= nodes; i++ {
		if err := ms.PutNode(types.NewNode(types.NodeID(i*2), 1, nil)); err != nil {
			b.Fatal(err)
		}
	}
	x := uint64(88172645463325252)
	next := func() types.NodeID {
		x ^= x << 13
		x ^= x >> 7
		x ^= x << 17
		return types.NodeID(2 * (1 + int64(x%nodes)))
	}
	id := int64(1)
	for b.Loop() {
		r := types.NewRelationship(types.RelID(id*2+1), 5, next(), next())
		if err := ms.PutRelationship(r); err != nil {
			b.Fatal(err)
		}
		id++
	}
}
