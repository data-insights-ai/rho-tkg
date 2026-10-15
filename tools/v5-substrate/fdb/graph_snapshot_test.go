package main

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"maps"
	"testing"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
)

// Authored source guards; no SDK/runtime qualification is claimed.
func TestGraphSnapshotSelectorAdmissionAndOwnedSourceGuards(t *testing.T) {
	p := graphSnapshotPolicy{PageRows: 2, PageBytes: 128 << 10, InventoryBytes: 2 << 20, WorkRows: 4096, WorkBytes: 16 << 20, ChainSteps: 16, Cleanup: graphSnapshotCleanupDefault()}
	if err := p.validate(); err != nil {
		t.Fatal(err)
	}
	for _, request := range []graphSnapshotRequest{
		{Mode: "Stable", Scope: []uint8{0, 1}, At: certificate{Round: 7}},
		{Mode: "At", Scope: []uint8{0, 1}},
		{Mode: "Fresh", Scope: []uint8{1, 0}},
		{Mode: "After", Scope: []uint8{0, 1}},
	} {
		if err := request.validate(); !errors.Is(err, errInvalid) {
			t.Fatal("mixed/missing authority admitted", request, err)
		}
	}
	if err := (graphSnapshotRequest{Mode: "Stable", Scope: []uint8{0, 1}}).validate(); err != nil {
		t.Fatal(err)
	}
	var absent *graphSnapshot
	if _, err := absent.Scan(t.Context(), nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil source published EOF", err)
	}
	if _, err := absent.Points(t.Context(), nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil points published empty", err)
	}
	if err := absent.Close(); !errors.Is(err, errInvalid) {
		t.Fatal("nil owner close", err)
	}
	// Numeric selection must not inherit the double-hex JSON key order.
	a := graphSnapshotCandidate{Cut: certificate{Round: 9}, Raw: []byte("z")}
	b := graphSnapshotCandidate{Cut: certificate{Round: 10}, Raw: []byte("a")}
	if !graphSnapshotPrefer(b, a) || graphSnapshotPrefer(a, b) {
		t.Fatal("lexical key chosen as numeric round")
	}
}

// Direct genuine SDK API composition, authored only. The tiny expected map is
// a test witness, never production reader state. The 130-revision key guards
// against EOF census replaying the entire ancestry separately per old effect.
func TestGraphSnapshotActualFourSelectorsAndOwnedSource(t *testing.T) {
	p := graphCostProfile{Version: 1, Facts: 100, CrossPercent: 50, Writers: 1}
	h := fixture(t, graphCostConfig("snapshot-direct-"+rand.Text(), p))
	t.Cleanup(func() {
		_, err := h.db.Transact(func(tr fdb.Transaction) (any, error) {
			for g := uint8(0); g < 2; g++ {
				tr.ClearRange(prefixRange(h.prefix(g)))
			}
			return nil, nil
		})
		if err != nil {
			t.Error("owned snapshot namespace cleanup", err)
		}
	})
	initial := mustCut(t, h, []uint8{0, 1}, nil)
	historyPolicy := graphHistoryPolicy{MaxOwners: 16, MaxControlBytes: 1 << 20, PageRows: 8, PageBytes: 4 << 20, MaxWorkRows: 1 << 20, MaxWorkBytes: 1 << 32, MaxChainSteps: 128, MaxOutputBytes: 4096}
	source, err := newGraphHistoryRetention(h, initial, historyPolicy)
	if err != nil {
		t.Fatal(err)
	}
	policy := graphSnapshotPolicy{PageRows: 2, PageBytes: 4 << 20, InventoryBytes: 2 << 20, WorkRows: 1 << 20, WorkBytes: 1 << 32, ChainSteps: 128, Cleanup: graphSnapshotCleanupDefault()}
	if r, err := source.OpenGraphSnapshot(t.Context(), graphSnapshotRequest{Mode: "Stable", Scope: []uint8{1}}, policy); r != nil || !errors.Is(err, errUnavailable) {
		t.Fatal("missing exact-scope certificate became exact empty", r, err)
	}
	expected := map[string]value{}
	apply := func(id string, command graphCommand, provenance uint64) graphSnapshotReceipt {
		t.Helper()
		before, _, err := h.graphCostPointSnapshot(command)
		if err != nil {
			t.Fatal(err)
		}
		coordinator := uint8(0)
		if command.Kind == "node-create" {
			coordinator = command.Node.Owner
		}
		x := h.graphTransaction(id, coordinator, command, before)
		x.Lifecycle = &lifecycleMetadata{Provenance: provenance}
		if command.Kind != "node-create" {
			x.Lifecycle.EdgeLife = command.Edge.ID + 1
			x.Lifecycle.Definition = &lifecycleDefinition{ID: uint64(command.Edge.Value), Kind: "i64", I64: command.Edge.Value}
		} // #nosec G115 -- fixture values5/6.
		o := mustSubmit(t, h, x)
		body, err := wire(x)
		if err != nil {
			t.Fatal(err)
		}
		proof, _, err := h.graphCostReadParallelOriginal(t.Context(), x, body)
		if err != nil {
			t.Fatal(err)
		}
		for _, write := range proof.Group.Writes {
			expected[fmt.Sprintf("%d/%s", write.Group, write.Key)] = write.After
		}
		return graphSnapshotReceipt{x, o}
	}
	for i, node := range p.nodes() {
		apply(fmt.Sprintf("direct-node-%d", i), graphCommand{Kind: "node-create", Node: node}, uint64(i+1))
	}
	apply("direct-create", graphCommand{Kind: "edge-create", Edge: p.edge(0, 5)}, 4)
	open := func(q graphSnapshotRequest) *graphSnapshot {
		t.Helper()
		r, err := source.OpenGraphSnapshot(t.Context(), q, policy)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := r.Close(); err != nil {
				t.Error(err)
			}
		})
		return r
	}
	old := open(graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}})
	oldCut, err := old.Cut()
	if err != nil {
		t.Fatal(err)
	}
	oldExpected := maps.Clone(expected)
	oldScope := open(graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0}})
	if err := oldScope.Close(); err != nil {
		t.Fatal(err)
	}
	last := apply("direct-update", graphCommand{Kind: "edge-update", Edge: p.edge(0, 6)}, 4+p.Facts)
	check := func(r *graphSnapshot, want map[string]value) {
		t.Helper()
		remaining := maps.Clone(want)
		end, err := r.Scan(t.Context(), func(row graphSnapshotRow) error {
			key := fmt.Sprintf("%d/%s", row.Group, row.Key)
			v, found := remaining[key]
			if !found || v != row.Value {
				return errMismatch
			}
			delete(remaining, key)
			return nil
		})
		if err != nil || len(remaining) != 0 || end.Rows[0]+end.Rows[1] != uint64(len(want)) {
			t.Fatal("exact old/current set or phantom row", end, remaining, err)
		}
		work := r.Work()
		if work.Callbacks == 0 || work.OpeningBytes == 0 || work.Bytes < work.OpeningBytes {
			t.Fatal("opening/source work omitted", work)
		}
	}
	stable := open(graphSnapshotRequest{Mode: "Stable", Scope: []uint8{0, 1}})
	if c, err := stable.Cut(); err != nil || !sameCertificate(c, oldCut) {
		t.Fatal("Stable created Fresh/current certificate", c, err)
	}
	check(stable, oldExpected)
	check(old, oldExpected)
	at := open(graphSnapshotRequest{Mode: "At", Scope: []uint8{0, 1}, At: oldCut})
	check(at, oldExpected)
	points, err := at.Points(t.Context(), []graphSnapshotKey{{0, edgeKey("edge", p.edge(0, 5))}, {0, edgeKey("edge", p.edge(99, 5))}})
	if err != nil || !points[0].Found || points[0].Value.Value != 5 || points[1].Found {
		t.Fatal("old point/current substitution or phantom", points, err)
	}
	after := open(graphSnapshotRequest{Mode: "After", Scope: []uint8{0, 1}, After: &last})
	check(after, expected)
	if c, err := after.Cut(); err != nil || c.Round < last.Outcome.Round {
		t.Fatal("After omitted original", c, err)
	}
	for i := 0; i < 129; i++ {
		apply(fmt.Sprintf("direct-retained-%d", i), graphCommand{Kind: "edge-update", Edge: p.edge(0, 6)}, 4+p.Facts)
	}
	current := open(graphSnapshotRequest{Mode: "Fresh", Scope: []uint8{0, 1}})
	check(current, expected)
	if _, err := old.Scan(t.Context(), func(graphSnapshotRow) error { return nil }); !errors.Is(err, errLimit) {
		t.Fatal("deep old traversal concealed chain admission", err)
	}
	stop := errors.New("direct consumer stopped")
	var pending *graphSnapshotReadPending
	if end, err := current.Scan(t.Context(), func(graphSnapshotRow) error {
		closeErr := current.Close()
		var ok bool
		pending, ok = errors.AsType[*graphSnapshotReadPending](closeErr)
		if !errors.Is(closeErr, errHistoryInUse) || !ok || !graphColumnSame(pending.Identity(), current.owner.id) {
			t.Fatal("busy cleanup lost exact lifetime/owner", closeErr)
		}
		if err := pending.Close(); !errors.Is(err, errHistoryInUse) {
			t.Fatal("pending handle closed under active scan", err)
		}
		return stop
	}); !errors.Is(err, stop) || end.Cut.Round != 0 {
		t.Fatal("partial scan published EOF", end, err)
	}
	if pending == nil {
		t.Fatal("missing genuine pending handle")
	}
	if err := pending.Close(); err != nil {
		t.Fatal("same bounded pending cleanup did not close original", err)
	}
	if pending.Work().CleanupBytes == 0 || pending.Work().CleanupCallbacks == 0 {
		t.Fatal("pending cleanup lost cumulative allowance", pending.Work())
	}
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := at.Scan(cancelled, func(graphSnapshotRow) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled scan published EOF", err)
	}
	beforeClose := at.Work()
	if err := at.Close(); err != nil {
		t.Fatal(err)
	}
	closedWork := at.Work()
	if closedWork.Rows != beforeClose.Rows || closedWork.Bytes != beforeClose.Bytes || closedWork.CleanupCallbacks == 0 || closedWork.CleanupBytes == 0 {
		t.Fatal("cleanup reset ordinary work or omitted prepaid debit", beforeClose, closedWork)
	}
	if _, err := at.Cut(); !errors.Is(err, errHistoryExpired) {
		t.Fatal("closed cut", err)
	}
	if _, err := at.Scan(context.Background(), func(graphSnapshotRow) error { return nil }); !errors.Is(err, errHistoryExpired) {
		t.Fatal("closed source became empty success", err)
	}
	finalCut, err := current.owner.s.h.fresh([]uint8{0, 1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := source.begin("direct-retirement", finalCut); !errors.Is(err, errHistoryInUse) {
		t.Fatal("retirement ignored genuine held source owners", err)
	}
	for _, r := range []*graphSnapshot{old, stable, after} {
		if err := r.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := source.begin("direct-retirement", finalCut); err != nil {
		t.Fatal(err)
	}
	if r, err := source.OpenGraphSnapshot(t.Context(), graphSnapshotRequest{Mode: "Stable", Scope: []uint8{0}}, policy); r != nil || !errors.Is(err, errHistoryExpired) {
		t.Fatal("expired older-only inventory became fallback/empty", r, err)
	}
	if r, err := source.OpenGraphSnapshot(t.Context(), graphSnapshotRequest{Mode: "Stable", Scope: []uint8{0, 1}}, policy); r != nil || !errors.Is(err, errHistoryBusy) {
		t.Fatal("active retirement became successful new read", r, err)
	}
}
