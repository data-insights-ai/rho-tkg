package tiered

import (
	"slices"
	"testing"
	"time"

	registrypkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/internal/registry"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// TestFormerLabels_SealedShardsSkipped (Sigma RH-S7 image restore): the
// label membership enumeration of a reference label opened every cold event
// shard. A closed shard's sealed counts now carry the labels it holds former
// members of (when its membership sidecar was built while open), and
// ForEachLabelTxMember / LabelTxMembersOf skip a sealed shard without the
// label. An event node that carried the reference label as a secondary label
// keeps its shard in the walk: the answer is the same, only the opens fall.
func TestFormerLabels_SealedShardsSkipped(t *testing.T) {
	dir := t.TempDir()
	clock := &tierClock{}
	clock.set(julDay(25, 9))
	cfg := tierClockCfg(dir, clock)
	cfg.RefLabels = []string{"Case", "Ask", "Session"}
	ts, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	labels := registrypkg.NewLabelRegistry()
	ts.SetLabelRegistry(labels)
	ask, _ := labels.GetOrCreate("Ask")
	session, _ := labels.GetOrCreate("Session")
	signal, _ := labels.GetOrCreate("Signal")
	// Reference members: an Ask node that lost its Session label (a former
	// Session member on the reference shard) and a current Ask node.
	refA := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), ask, []uint16{session})
	refB := types.NewNode(types.NodeID(tieredNodeGen(t).Generate()), ask, nil)
	for _, n := range []*types.Node{refA, refB} {
		if err := ts.PutNode(n); err != nil {
			t.Fatal(err)
		}
	}
	refA2 := types.NewNode(refA.ID(), ask, nil)
	refA2.SetVersion(1)
	if err := ts.RemoveNodeLabelTokenWithHistory(refA.ID(), session, refA2, 0, refA); err != nil {
		t.Fatal(err)
	}
	// Event nodes with history on days 20..23; the day-21 one carried Ask as a
	// secondary label and lost it: a former Ask member on an event shard.
	var askOnEvent types.NodeID
	var events []types.NodeID
	for d := 20; d <= 23; d++ {
		id, ok, err := ts.MintNodeIDAt(signal, ms(julDay(d, 3)), true)
		if err != nil || !ok {
			t.Fatalf("MintNodeIDAt day %d: %v, %v", d, ok, err)
		}
		var extra []uint16
		if d == 21 {
			extra, askOnEvent = []uint16{ask}, id
		}
		events = append(events, id)
		v0 := types.NewNode(id, signal, extra)
		if err := ts.PutNode(v0); err != nil {
			t.Fatal(err)
		}
		v1 := types.NewNode(id, signal, nil)
		v1.SetVersion(1)
		if d == 21 {
			err = ts.RemoveNodeLabelTokenWithHistory(id, ask, v1, 0, v0)
		} else {
			err = ts.ReplaceNodeWithHistory(v1, 0, v0)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := ts.Close(); err != nil {
		t.Fatal(err)
	}

	reopen := func() {
		t.Helper()
		if ts, err = New(cfg); err != nil {
			t.Fatal(err)
		}
		ts.SetLabelRegistry(labels)
		ts.SetIdleTimeoutForTest(time.Nanosecond)
	}
	members := func(tok uint16) (ids []types.NodeID, opens uint64) {
		t.Helper()
		before := ts.ShardAccessStats().ColdOpens
		if err := ts.ForEachLabelTxMember(tok, func(id types.NodeID, _ types.Instant) bool {
			ids = append(ids, id)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		ts.CloseIdleShardsForTest()
		slices.Sort(ids)
		return ids, ts.ShardAccessStats().ColdOpens - before
	}
	lookup := func(tok uint16, of []types.NodeID) (ids []types.NodeID, opens uint64) {
		t.Helper()
		before := ts.ShardAccessStats().ColdOpens
		if err := ts.LabelTxMembersOf(tok, of, func(id types.NodeID, _ types.Instant) bool {
			ids = append(ids, id)
			return true
		}); err != nil {
			t.Fatal(err)
		}
		ts.CloseIdleShardsForTest()
		slices.Sort(ids)
		return ids, ts.ShardAccessStats().ColdOpens - before
	}

	reopen()
	firstAsk, firstOpens := members(ask)
	firstSession, _ := members(session)
	if firstOpens == 0 {
		t.Fatal("first enumeration opened no cold shard: the fixture has none closed")
	}
	if !slices.Equal(firstAsk, []types.NodeID{askOnEvent}) || !slices.Equal(firstSession, []types.NodeID{refA.ID()}) {
		t.Fatalf("former members: Ask %v (want %d), Session %v (want %d)", firstAsk, askOnEvent, firstSession, refA.ID())
	}
	// The first read built the sidecars and its closes sealed the sets; also
	// after a reopen, from the catalog.
	for _, when := range []string{"sealed", "reopened"} {
		if when == "reopened" {
			if err := ts.Close(); err != nil {
				t.Fatal(err)
			}
			reopen()
		}
		if got, opens := members(session); opens != 0 || !slices.Equal(got, firstSession) {
			t.Fatalf("%s: Session former members %v with %d cold opens, want %v with 0", when, got, opens, firstSession)
		}
		if got, opens := members(ask); opens != 1 || !slices.Equal(got, firstAsk) {
			t.Fatalf("%s: Ask former members %v with %d cold opens, want %v with 1 (its event shard)", when, got, opens, firstAsk)
		}
		if got, opens := lookup(session, []types.NodeID{refA.ID(), refB.ID(), askOnEvent}); opens != 0 || !slices.Equal(got, []types.NodeID{refA.ID()}) {
			t.Fatalf("%s: LabelTxMembersOf(Session) %v with %d cold opens, want [%d] with 0", when, got, opens, refA.ID())
		}
	}
	// A by-ID read opens the shards read-only without building their
	// sidecars; their close keeps the sealed sets (review note).
	for _, id := range events {
		if _, err := ts.GetNode(id); err != nil {
			t.Fatal(err)
		}
	}
	ts.CloseIdleShardsForTest()
	if got, opens := members(session); opens != 0 || !slices.Equal(got, firstSession) {
		t.Fatalf("after read-only opens: Session former members %v with %d cold opens, want %v with 0", got, opens, firstSession)
	}
	defer func() { _ = ts.Close() }()
}
