package memory

import (
	"errors"
	"fmt"
	"sort"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMemoryLabelTxMembersOf: the point lookup answers exactly the asked IDs
// the streamed sidecar holds, with their first transaction time, on its
// first (building) call and after a delete and a history version that
// carries a label the current row lacks (append-only); unknown IDs and IDs
// of another label are skipped; fn returning false stops; a nil callback and
// a nil store are refused. Faulty implementations caught: answering the
// whole label, dropping a deleted or history-only member, ignoring the stop.
func TestMemoryLabelTxMembersOf(t *testing.T) {
	t.Parallel()
	ms := New()
	defer ms.Close()
	const tokL, tokK uint16 = 1, 2
	for _, n := range []*types.Node{mustNode(t, 101, []uint16{tokL}, 10), mustNode(t, 102, []uint16{tokL, tokK}, 20), mustNode(t, 103, []uint16{tokK}, 30)} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	lookup := func(tok uint16, ids ...types.NodeID) string {
		t.Helper()
		var got []string
		if err := ms.LabelTxMembersOf(tok, ids, func(id types.NodeID, firstTx types.Instant) bool {
			got = append(got, fmt.Sprintf("%d@%d", id, firstTx))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		return fmt.Sprint(got)
	}
	if got := lookup(tokL, 101, 103, 999); got != "[101@10]" {
		t.Fatalf("first (building) lookup = %s, want [101@10]", got)
	}
	if got := lookup(tokK, 101, 102, 103); got != "[102@20 103@30]" {
		t.Fatalf("K lookup = %s", got)
	}
	if err := ms.DeleteNode(102); err != nil {
		t.Fatal(err)
	}
	hist := mustNode(t, 103, []uint16{tokK, tokL}, 40)
	hist.SetVersion(1)
	if err := ms.PutNodeVersion(103, 1, hist); err != nil {
		t.Fatal(err)
	}
	if got := lookup(tokL, 101, 102, 103); got != "[101@10 102@20 103@40]" {
		t.Fatalf("after delete and history row = %s, want [101@10 102@20 103@40]", got)
	}
	calls := 0
	if err := ms.LabelTxMembersOf(tokL, []types.NodeID{101, 102, 103}, func(types.NodeID, types.Instant) bool {
		calls++
		return false
	}); err != nil || calls != 1 {
		t.Fatalf("stop: %d calls, %v", calls, err)
	}
	if err := ms.LabelTxMembersOf(tokL, nil, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("nil callback: %v", err)
	}
	var nilStore *Store
	if err := nilStore.LabelTxMembersOf(tokL, nil, func(types.NodeID, types.Instant) bool { return true }); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store: %v", err)
	}
}

// TestMemoryRelTypeTxMembersOf is the relationship mirror.
func TestMemoryRelTypeTxMembersOf(t *testing.T) {
	t.Parallel()
	ms := New()
	defer ms.Close()
	for _, n := range []*types.Node{mustNode(t, 1, []uint16{1}, 1), mustNode(t, 2, []uint16{1}, 1)} {
		if err := ms.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	put := func(id int64, typ uint16, tx types.Instant) {
		r := types.NewRelationship(types.RelID(id), typ, 1, 2)
		r.SetTemporal(&types.TemporalMetadata{TxFrom: tx, ValidFrom: tx})
		if err := ms.PutRelationship(r); err != nil {
			t.Fatal(err)
		}
	}
	put(201, 5, 10)
	put(202, 6, 20)
	lookup := func(tok uint16, ids ...types.RelID) string {
		t.Helper()
		var got []string
		if err := ms.RelTypeTxMembersOf(tok, ids, func(id types.RelID, firstTx types.Instant) bool {
			got = append(got, fmt.Sprintf("%d@%d", id, firstTx))
			return true
		}); err != nil {
			t.Fatal(err)
		}
		sort.Strings(got)
		return fmt.Sprint(got)
	}
	if got := lookup(5, 201, 202, 999); got != "[201@10]" {
		t.Fatalf("first lookup = %s", got)
	}
	if err := ms.DeleteRelationship(201); err != nil {
		t.Fatal(err)
	}
	put(203, 5, 30)
	if got := lookup(5, 201, 202, 203); got != "[201@10 203@30]" {
		t.Fatalf("after delete and add = %s", got)
	}
	if err := ms.RelTypeTxMembersOf(5, nil, nil); !errors.Is(err, ErrInvalidStoreMutation) {
		t.Fatalf("nil callback: %v", err)
	}
	var nilStore *Store
	if err := nilStore.RelTypeTxMembersOf(5, nil, func(types.RelID, types.Instant) bool { return true }); !errors.Is(err, ErrNilStore) {
		t.Fatalf("nil store: %v", err)
	}
}
