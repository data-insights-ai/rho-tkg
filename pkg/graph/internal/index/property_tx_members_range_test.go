package index

import (
	"math"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestPropertyTxMembersInRange (round 8 request 2): the numeric view answers
// only once opened; it covers the values recorded before it opened and those
// recorded after; a member of several values in range appears once with the
// merged bound; NaN values always come back; an empty range returns the NaN
// values only.
func TestPropertyTxMembersInRange(t *testing.T) {
	t.Parallel()
	m := NewPropertyTxMembers[types.NodeID]()
	m.Record(PropertyValueKey(int64(3)), 1, 50)
	m.Record(PropertyValueKey(3.0), 2, 40)
	m.Record(PropertyValueKey(float32(0.1)), 3, 30)
	m.Record(PropertyValueKey("s"), 4, 20)
	m.Record(PropertyValueKey(math.NaN()), 5, 10)
	if _, ok := m.MembersInRange(0, 10); ok {
		t.Fatal("answered before OrderNumeric")
	}
	m.OrderNumeric()
	m.Record(PropertyValueKey(int64(7)), 1, 45) // a new value after the view opened
	m.Record(PropertyValueKey(int64(9)), 6, 0)
	ids := func(lo, hi float64) []string {
		out, ok := m.MembersInRange(lo, hi)
		if !ok {
			t.Fatal("not open")
		}
		var s []string
		for _, mb := range out {
			s = append(s, string(rune('0'+int(mb.ID)))+":"+string(rune('0'+int(mb.FirstTx/5))))
		}
		slices.Sort(s)
		return s
	}
	if got, want := ids(0, 10), []string{"1:9", "2:8", "3:6", "5:2", "6:0"}; !slices.Equal(got, want) {
		t.Fatalf("[0,10] = %v, want %v", got, want)
	}
	if got, want := ids(0.1, 0.1), []string{"3:6", "5:2"}; !slices.Equal(got, want) {
		t.Fatalf("[0.1,0.1] = %v, want %v", got, want)
	}
	if got, want := ids(5, 1), []string{"5:2"}; !slices.Equal(got, want) {
		t.Fatalf("empty range = %v, want %v", got, want)
	}
	var nilM *PropertyTxMembers[types.NodeID]
	nilM.OrderNumeric()
	if out, ok := nilM.MembersInRange(0, 1); !ok || out != nil {
		t.Fatal("nil sidecar")
	}
}
