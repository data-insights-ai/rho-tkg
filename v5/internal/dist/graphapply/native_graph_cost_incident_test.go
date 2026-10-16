package graphapply

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func nativeIncidentRefusal(t *testing.T, n *certifiedProcessNetwork, q nativeGraphCostIncidentRequest, cause error) {
	t.Helper()
	wire, err := nativeGraphCostIncidentInput(q)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	response, err := n.all[2].exchange(ctx, processRequest{op: processCostObserve, data: wire})
	if len(response.aux) != 0 || !errors.Is(errors.Join(err, processResponseError(response)), cause) {
		t.Fatal("incident refusal published partial/EOF", response, err, cause)
	}
}

// Three actual cross facts separate two pages and retain exact cells at the old
// cut after a genuine endpoint close. No large cost cell or timing claim here.
func TestNativeGraphCostIncidentOwnerRetainsOldCutAndRefusesExhaustedLateCookies(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	spec := nativeGraphCostSpec{Version: 1, Facts: 3, CrossPercent: 100, Writers: 1}
	n := newNativeGraphCostNetworkWithBlock(t, spec, spec.homeCount())
	for phase := uint64(1); phase <= 3; phase++ {
		nativeGraphCostSubmit(t, n, phase, 0, false)
	}
	for fact := uint64(0); fact < spec.Facts; fact++ {
		nativeGraphCostSubmit(t, n, 4, fact, true)
	}
	handle, old := nativeGraphCostOpen(t, n, graphReadFresh, graphCut{})
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	opened := nativeGraphCostIncidentOpenAt(t, n, old, 1, 2, 1, 4)
	first := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: opened.Cookie, Sequence: 1})
	if first.Index.Complete || len(first.Index.Rows) != 1 || first.Index.Rows[0].Entity.ID != 7 || !first.Index.Rows[0].Entity.Effective || first.Index.Rows[0].Entity.Property.Revision == 0 || first.Index.Rows[0].Entity.Property.Provenance != 4 {
		t.Fatal("first genuine hydrated cell", first)
	}
	wrong := opened.Cookie
	wrong.Request[0] ^= 1
	nativeIncidentRefusal(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: wrong, Sequence: 2}, errInvalid)
	wire, err := nativeGraphCostIncidentInput(nativeGraphCostIncidentRequest{Action: nativeIncidentCloseEndpoint, Endpoint: spec.remoteNode()})
	if err != nil {
		t.Fatal(err)
	}
	result := n.all[2].call(t, processRequest{op: processCostObserve, data: wire})
	committed, err := decodeGraphProcessCoordinatorResult(result)
	if err != nil || committed.decision.phase != crossDecide || !committed.decision.commit {
		t.Fatal("endpoint mutation lacked native decision", committed, err)
	}
	var last nativeGraphCostIncidentReceipt
	for sequence := uint64(2); sequence <= 3; sequence++ {
		last = nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: opened.Cookie, Sequence: sequence})
		if last.Cookie != opened.Cookie || last.DeadlineUnixNanos != opened.DeadlineUnixNanos || len(last.Index.Rows) != 1 || last.Index.Rows[0].Entity.ID != 5+2*sequence || !last.Index.Rows[0].Entity.Effective || last.Index.Rows[0].Entity.Property.I64 != 5 || last.Index.Rows[0].Entity.Property.Revision == 0 || last.Index.Rows[0].Entity.Property.Provenance != 3+sequence || last.Charged[6] != int(sequence)+1 {
			t.Fatal("original At owner substituted current/state/budgets", last)
		}
	}
	if !last.Index.Complete || last.Index.Next != 0 || last.CompletedSourceWork.Records <= first.CompletedSourceWork.Records || last.RPCs <= first.RPCs || last.RequestBytes == 0 || last.ReplyBytes == 0 || last.Charged[5] <= first.Charged[5] {
		t.Fatal("complete EOF/cumulative metrics absent", last)
	}
	replay := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: opened.Cookie, Sequence: 3})
	if !reflect.DeepEqual(last, replay) {
		t.Fatal("last genuine chunk replay changed", last, replay)
	}
	nativeIncidentRefusal(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: opened.Cookie, Sequence: 1}, errInvalid)
	closed := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentClose, Cookie: opened.Cookie})
	if closed.RPCs <= last.RPCs || closed.RequestBytes <= last.RequestBytes || closed.ReplyBytes <= last.ReplyBytes {
		t.Fatal("close frames unaccounted", closed)
	}
	nativeIncidentRefusal(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: opened.Cookie, Sequence: 3}, errInvalid)
	handle, current := nativeGraphCostOpen(t, n, graphReadFresh, graphCut{})
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	active := nativeGraphCostIncidentOpenAt(t, n, current, 1, 2, 1, 4)
	for sequence := uint64(1); sequence <= 3; sequence++ {
		page := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: active.Cookie, Sequence: sequence})
		if len(page.Index.Rows) != 0 || page.Index.Complete != (sequence == 3) {
			t.Fatal("current mask/final EOF incorrect", page)
		}
	}
	nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentClose, Cookie: active.Cookie})
	exhausted := nativeGraphCostIncidentOpenAt(t, n, old, 1, 2, 1, 1)
	nativeIncidentRefusal(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: exhausted.Cookie, Sequence: 1}, errLimit)
	finish := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentClose, Cookie: exhausted.Cookie})
	if finish.Charged[6] != 1 {
		t.Fatal("refused invocation reset/charged source", finish)
	}
	// Pause an actual remote opening, then cancel that IPC call. The same owner
	// retries sequence1 with the retired attempt still charged, not a new At/ID.
	pending := nativeGraphCostIncidentOpenAt(t, n, old, 1, 2, 1, 5)
	n.all[4].call(t, processRequest{op: processSourcePause, entity: 1})
	request, err := nativeGraphCostIncidentInput(nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: pending.Cookie, Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		response, err := n.all[2].exchange(ctx, processRequest{op: processCostObserve, data: request})
		done <- errors.Join(err, processResponseError(response))
	}()
	n.all[4].call(t, processRequest{op: processSourcePausedBarrier})
	n.all[4].call(t, processRequest{op: processTick})
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled incident owner did not retire IPC worker")
	}
	n.all[4].call(t, processRequest{op: processSourcePause})
	for sequence := uint64(1); sequence <= 3; sequence++ {
		page := nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: pending.Cookie, Sequence: sequence})
		if page.Charged[6] != int(sequence)+2 || len(page.Index.Rows) != 1 || page.Index.Rows[0].Entity.ID != 5+2*sequence || page.Index.Rows[0].Entity.Kind != byte(graphstate.Relationship) {
			t.Fatal("canceled attempt forgot prefix/ledger", page)
		}
	}
	nativeGraphCostIncidentCall(t, n, nativeGraphCostIncidentRequest{Action: nativeIncidentClose, Cookie: pending.Cookie})
	nativeGraphCostDrain(t, n)
	for id, p := range n.all {
		state := p.call(t, processRequest{op: processStateOp})
		if state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 {
			t.Fatal("incident owner residue", id, state.stats)
		}
	}
}

func TestNativeGraphCostIncidentEnvelopeRejectsUnownedSequenceAndExtraInput(t *testing.T) {
	good, err := nativeGraphCostIncidentInput(nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: nativeGraphCostIncidentCookie{1, [16]byte{1}}, Sequence: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nativeGraphCostIncidentDecodeInput(processRequest{data: good}); err != nil {
		t.Fatal(err)
	}
	for _, wire := range [][]byte{nil, good[:len(good)-1], append(bytes.Clone(good), '0'), append([]byte(nativeIncidentMarker), []byte(`{"Action":2,"Extra":true}`)...)} {
		if _, err := nativeGraphCostIncidentDecodeInput(processRequest{data: wire}); !errors.Is(err, errInvalid) {
			t.Fatal("bad incident embedding input", err)
		}
	}

	closeWire, err := nativeGraphCostIncidentInput(nativeGraphCostIncidentRequest{Action: nativeIncidentClose, Cookie: nativeGraphCostIncidentCookie{1, [16]byte{1}}})
	if err != nil {
		t.Fatal(err)
	}
	if !nativeGraphCostIncidentCloseControl(processRequest{op: processCostObserve, data: closeWire}) || nativeGraphCostIncidentCloseControl(processRequest{op: processCostObserve, data: good}) || nativeGraphCostIncidentCloseControl(processRequest{op: processCostObserve, index: 1, data: closeWire}) {
		t.Fatal("owner cleanup lane admission changed ordinary requests or admitted malformed close")
	}
	invalid, _ := nativeGraphCostIncidentInput(nativeGraphCostIncidentRequest{Action: nativeIncidentNext, Cookie: nativeGraphCostIncidentCookie{1, [16]byte{1}}})
	if _, err := nativeGraphCostIncidentDecodeInput(processRequest{data: invalid}); !errors.Is(err, errInvalid) {
		t.Fatal("unbound sequence accepted", err)
	}
	receipt := nativeGraphCostIncidentReceipt{Cookie: nativeGraphCostIncidentCookie{1, [16]byte{1}}, Sequence: 1, DeadlineUnixNanos: 1, Index: nativeGraphCostIndexPage{Kind: 1, Endpoint: 1, Cursor: 1, Next: 2}, Charged: [7]int{1, 1, 1, 1, 1, 1, 1}}
	wire, err := nativeGraphCostIncidentEncodeReceipt(receipt)
	if err != nil {
		t.Fatal(err)
	}
	// Empty residual chunks are genuine progress, not invented EOF.
	decoded, err := nativeGraphCostIncidentDecodeReceipt(wire)
	if err != nil || decoded.Index.Complete {
		t.Fatal(decoded, err)
	}
	if _, err := nativeGraphCostIncidentDecodeReceipt(append(bytes.Clone(wire), 0)); !errors.Is(err, errCorrupt) {
		t.Fatal("trailing incident reply", err)
	}
	receipt.Index.Rows = make([]nativeGraphCostIndexRow, 17)
	if _, err := nativeGraphCostIncidentEncodeReceipt(receipt); !errors.Is(err, errLimit) {
		t.Fatal("row cap bypassed", err)
	}
}
