package badger

import (
	"errors"
	"math"
	"testing"

	indexpkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/index"
	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	storecontract "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestMeteredRAMSidecarWalkIsCharged (R2 cumulative, review rule 4): a
// definition kept in RAM answers the metered capability from its membership
// sidecar, and the sidecar's work is charged to the tally before it is done,
// not only the members it hands on:
//
//   - the snapshot of a value's members (the exact read) counts each member as
//     it is collected and charges its slot in the list, so a value past the
//     limit is copied only up to the member that refuses and hands on nothing;
//   - the one-time open of the value's window view, a pass over every member,
//     is refused before it when the value's members pass the room, and the
//     view stays unopened (the native counter: MembersDuring answers not-ok);
//     the view is a persistent structure, so its build is not BufferBytes, and
//     the first budgeted reader pays for every member copied into the view;
//   - the view's walk counts every entry it examines before it looks at it,
//     and charges the members it adds and the map of decided members as
//     BufferBytes, and stops at the entry that refuses.
//
// 64 members, each valid one instant after the one before, first written at
// tx 100; the window [100, 101) at pin 50 keeps one, after judging all 64.
// Each limit test pairs the exact, non-empty unlimited control.
func TestMeteredRAMSidecarWalkIsCharged(t *testing.T) {
	const n = 64
	vk := types.IndexablePropertyValueKey("v")
	bs, err := New(Config{InMemory: true, PropertyKeyRegistry: registrypkg.NewPropertyKeyRegistry(), MaxPendingWrites: -1, FlushInterval: 1<<63 - 1})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bs.Close() })
	if err = bs.CreatePropertyIndex(1, "k"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < n; i++ {
		nd := memberWorkNode(types.NodeID(i + 1))
		nd.SetTemporal(&types.TemporalMetadata{TxFrom: 100, ValidFrom: types.Instant(100 + i), ValidTo: types.Instant(101 + i)})
		if err = bs.PutNode(nd); err != nil {
			t.Fatal(err)
		}
	}
	if err = bs.Flush(); err != nil {
		t.Fatal(err)
	}
	key := indexpkg.PropertyIndexKey{LabelToken: 1, PropertyKey: "k"}
	read := func(window *storecontract.PropertyTxMemberWindow, tally *storecontract.ReadTally) (int, error) {
		handed := 0
		err := bs.ForEachNodePropertyTxMemberMetered(1, "k", []string{vk}, window, tally, func(types.NodeID, types.Instant) bool { handed++; return true })
		return handed, err
	}
	viewOpen := func() bool { // the native state of the sidecar, which no meter keeps
		bs.idxMu.RLock()
		defer bs.idxMu.RUnlock()
		_, ok := bs.nodePropTx[key].members.MembersDuring(vk, 0, math.MaxInt64, 0)
		return ok
	}
	var rb *storecontract.ReadBudgetError

	// The exact read: each member counted and listed (16 bytes) as the snapshot collects it.
	if got, err := read(nil, nil); err != nil || got != n { // builds the sidecar
		t.Fatalf("unmetered control: %d members, %v; want %d", got, err, n)
	}
	m, tally := cumulativeTally(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	if got, err := read(nil, tally); err != nil || got != n || m.Work().Candidates != n || m.Work().BufferBytes != n*txMemberBytes {
		t.Fatalf("exact control: %d members, %v, work %+v; want %d members, %d counted, %d bytes", got, err, m.Work(), n, n, n*txMemberBytes)
	}
	m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: 3})
	if got, err := read(nil, tally); !errors.As(err, &rb) || rb.Limit != "candidates" || got != 0 || m.Work().Candidates != 4 {
		t.Fatalf("exact, Candidates 3: %d members, %v, work %+v; want the refusal before any member is handed on, at the 4th member the snapshot collects", got, err, m.Work())
	}
	m, tally = cumulativeTally(storecontract.ReadBudget{BufferBytes: n*txMemberBytes - 1})
	if got, err := read(nil, tally); !errors.As(err, &rb) || rb.Limit != "buffer bytes" || got != 0 {
		t.Fatalf("exact, BufferBytes %d: %d members, %v, work %+v; want the buffer bytes refusal before any member", n*txMemberBytes-1, got, err, m.Work())
	}

	// The window read: the view is not open yet.
	window := &storecontract.PropertyTxMemberWindow{ValidStart: 100, ValidEnd: 101, TxAt: 50}
	if viewOpen() {
		t.Fatal("fixture: the window view is open before the first windowed read")
	}
	m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: 5})
	if got, err := read(window, tally); !errors.As(err, &rb) || rb.Limit != "candidates" || got != 0 || m.Work().Candidates != n || viewOpen() {
		t.Fatalf("window, unopened view, Candidates 5: %d members, %v, work %+v, view open %v; want the refusal before the open (the %d members counted, the view left unopened)", got, err, m.Work(), viewOpen(), n)
	}
	// Every entry the walk examines is charged: the 64 of the recent view (63 rejected, 1 kept) and
	// the one the late view visits (decided by the recent view already). The view is a persistent
	// structure, not a buffer of this read: its build costs no BufferBytes, the member (16 bytes)
	// and the map of decided members (24 bytes an entry) do.
	const examined, bytes = n + 1, txMemberBytes + n*foldEntryBytes
	m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: 1 << 20, BufferBytes: 1 << 20})
	if got, err := read(window, tally); err != nil || got != 1 || !viewOpen() || m.Work().Candidates != n+examined || m.Work().BufferBytes != bytes {
		t.Fatalf("window control: %d members, %v, work %+v, view open %v; want the 1 member, %d build and walk entries counted, %d bytes", got, err, m.Work(), viewOpen(), n+examined, bytes)
	}

	// The view is open: the walk is charged entry by entry and stops at the refusal.
	for _, limit := range []uint64{3, 10} {
		m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: limit})
		if got, err := read(window, tally); !errors.As(err, &rb) || rb.Limit != "candidates" || got != 0 || m.Work().Candidates != limit+1 {
			t.Fatalf("window, open view, Candidates %d: %d members, %v, work %+v; want the refusal at the entry past it (%d counted, not the %d the view holds)", limit, got, err, m.Work(), limit+1, n)
		}
	}
	m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: examined})
	if got, err := read(window, tally); err != nil || got != 1 || m.Work().Candidates != examined {
		t.Fatalf("window, Candidates %d: %d members, %v, work %+v; want the 1 member and %d counted", examined, got, err, m.Work(), examined)
	}
	m, tally = cumulativeTally(storecontract.ReadBudget{Candidates: examined - 1})
	if got, err := read(window, tally); !errors.As(err, &rb) || got != 0 || m.Work().Candidates != examined {
		t.Fatalf("window, Candidates %d: %d members, %v, work %+v; want the refusal at the last entry, the one the late view visits", examined-1, got, err, m.Work())
	}
}
