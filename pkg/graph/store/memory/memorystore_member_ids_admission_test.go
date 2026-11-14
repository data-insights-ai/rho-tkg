package memory

import (
	"errors"
	"slices"
	"testing"

	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

func TestCurrentMemberIDsSealedAdmission(t *testing.T) {
	ms := New()
	t.Cleanup(func() { _ = ms.Close() })
	if err := ms.DeclareRelSegment(segTestDecl(1 << 30)); err != nil {
		t.Fatal(err)
	}
	for _, id := range []types.NodeID{1, 2} {
		if err := ms.PutNode(types.NewNode(id, 1, nil)); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 6 {
		if err := ms.PutRelationship(segHashed(types.NewRelationship(types.RelID(100+i), segTestHOP, 1, 2))); err != nil {
			t.Fatal(err)
		}
	}
	if err := ms.SealRelSegments(segTestHOP); err != nil {
		t.Fatal(err)
	}
	if ms.liveSealedOfLocked(segTestHOP) != 6 {
		t.Fatal("fixture did not seal six rows")
	}
	if err := ms.DeleteRelationship(100); err != nil {
		t.Fatal(err)
	}
	if err := ms.PutRelationship(segHashed(types.NewRelationship(200, segTestHOP, 1, 2))); err != nil {
		t.Fatal(err)
	}
	want := []types.RelID{101, 102, 103, 104, 105, 200}
	copied, stop := CountCurrentMembersCopiedForTest()
	defer stop()
	read := func(b storecontract.ReadBudget) ([]types.RelID, int64, storecontract.ReadWork, error) {
		_, m := storecontract.WithReadBudget(t.Context(), b)
		before := copied()
		ids, e := ms.RelIDsByTypeMetered(segTestHOP, m.Tally("sealed"))
		return ids, copied() - before, m.Work(), e
	}
	got, n, work, err := read(storecontract.ReadBudget{})
	if err != nil || !slices.Equal(got, want) || n != 6 || work.Candidates != 7 || work.BufferBytes != 48 {
		t.Fatalf("control IDs %v, copies %d, work %+v, err %v", got, n, work, err)
	}
	got, n, work, err = read(storecontract.ReadBudget{Candidates: 3})
	var rb *storecontract.ReadBudgetError
	if !errors.As(err, &rb) || rb.Limit != "candidates" || len(got) != 0 || n >= 3 || work.Candidates != 4 {
		t.Fatalf("limit IDs %v, copies %d, work %+v, err %v", got, n, work, err)
	}
	got, n, work, err = read(storecontract.ReadBudget{BufferBytes: 1})
	if !errors.As(err, &rb) || rb.Limit != "buffer bytes" || len(got) != 0 || n != 0 {
		t.Fatalf("buffer IDs %v, copies %d, work %+v, err %v", got, n, work, err)
	}
}
