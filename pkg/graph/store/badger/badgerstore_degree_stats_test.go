package badger

import (
	"errors"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestBadgerStoreRelTypeDegreeStats(t *testing.T) {
	t.Parallel()
	for _, onDisk := range []bool{false, true} {
		bs, err := New(Config{InMemory: true, FlushInterval: 1<<63 - 1, AdjacencyIndexOnDisk: onDisk})
		if err != nil {
			t.Fatal(err)
		}
		gen, rgen := newTestGen(t, 0), newTestGen(t, 1)
		var ns []*types.Node
		for i := 0; i < 4; i++ {
			n := types.NewNode(types.NodeID(gen.Generate()), 1, nil)
			if err := bs.PutNode(n); err != nil {
				t.Fatal(err)
			}
			ns = append(ns, n)
		}
		put := func(typ uint16, s, e *types.Node) *types.Relationship {
			r := types.NewRelationship(types.RelID(rgen.Generate()), typ, s.ID(), e.ID())
			if err := bs.PutRelationship(r); err != nil {
				t.Fatal(err)
			}
			return r
		}
		put(1, ns[0], ns[1])
		put(1, ns[0], ns[2])
		gone := put(1, ns[0], ns[3])
		put(1, ns[3], ns[2])
		put(2, ns[1], ns[2])
		if err := bs.DeleteRelationship(gone.ID()); err != nil {
			t.Fatal(err)
		}
		st, ok, err := bs.RelTypeDegreeStats(1)
		if onDisk {
			if ok || err != nil {
				t.Fatalf("AdjacencyIndexOnDisk must decline: ok=%v err=%v", ok, err)
			}
		} else {
			want := storecontract.RelTypeDegreeStats{Rels: 3, Starts: 2, Ends: 2, MaxOut: 2, MaxIn: 2, MaxOutNode: ns[0].ID(), MaxInNode: ns[2].ID()}
			if !ok || err != nil || st != want {
				t.Fatalf("type 1 = %+v ok=%v err=%v, want %+v", st, ok, err, want)
			}
			all, ok, err := bs.RelTypeDegreeStats(0)
			if !ok || err != nil || all.Rels != 4 || all.MaxIn != 3 {
				t.Fatalf("all types = %+v ok=%v err=%v", all, ok, err)
			}
		}
		if err := bs.Close(); err != nil {
			t.Fatal(err)
		}
		if _, _, err := bs.RelTypeDegreeStats(1); !errors.Is(err, storecontract.ErrStoreClosed) {
			t.Fatalf("after Close: %v", err)
		}
	}
}

// TestBadgerRelTypeDegreeCounts (round 7 request 2): the per-node counts fold
// to RelTypeDegreeStats' answer, count only the type's live relationships,
// and decline with AdjacencyIndexOnDisk.
func TestBadgerRelTypeDegreeCounts(t *testing.T) {
	t.Parallel()
	bs := newTestBadgerStoreInMemory(t)
	for i := int64(1); i <= 4; i++ {
		putTestNode(t, bs, i, 1, nil)
	}
	putTestRel(t, bs, 10, 3, 1, 2)
	putTestRel(t, bs, 11, 3, 1, 3)
	putTestRel(t, bs, 12, 3, 4, 3)
	putTestRel(t, bs, 13, 5, 1, 2) // another type
	out, in, rels, ok, err := bs.RelTypeDegreeCounts(3)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if rels != 3 || len(out) != 2 || out[1] != 2 || out[4] != 1 || len(in) != 2 || in[3] != 2 || in[2] != 1 {
		t.Fatalf("counts rels=%d out=%v in=%v", rels, out, in)
	}
	st, ok, err := bs.RelTypeDegreeStats(3)
	if err != nil || !ok || st != storecontract.DegreeStatsFromCounts(rels, out, in) {
		t.Fatalf("stats %+v ok=%v err=%v", st, ok, err)
	}
}
