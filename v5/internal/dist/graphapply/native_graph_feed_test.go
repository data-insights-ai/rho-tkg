package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// Authored source guards only; actual child/matrix qualification is separate.
func TestNativeFeedWorkloadAdmissionAndCompleteIdentity(t *testing.T) {
	s := nativeFeedSpec{Version: 1, Family: nativeFeedFamily, Facts: 40, CrossPercent: 50, Writers: 32, Layout: "columns"}
	if err := nativeFeedValidate(s.cost()); err != nil {
		t.Fatal(err)
	}
	if _, err := newNativeFeedSessions(context.Background(), nil, &nativeGraphCostSpec{Version: 5, Facts: 40, CrossPercent: 50, Writers: 32}); !errors.Is(err, errInvalid) {
		t.Fatal("nil coordinator", err)
	}
	for _, broken := range []nativeGraphCostSpec{{Version: 5, Facts: 20, CrossPercent: 50, Writers: 32}, {Version: 5, Facts: 40, CrossPercent: 1, Writers: 8}, {Version: 5, Facts: 40, CrossPercent: 50, Writers: 2}} {
		if !errors.Is(nativeFeedValidate(broken), errInvalid) {
			t.Fatal("unsupported finite profile")
		}
	}
	d := &nativeFeedDriver{spec: s}
	d.originals = nativeFeedOriginals(s.cost())
	if !errors.Is(d.beginOriginal(), errLimit) || d.originals != nativeFeedOriginals(s.cost()) {
		t.Fatal("factory quota must refuse before effects")
	}
	d.extra = nativeFeedExtraInvocations
	if !errors.Is(d.retry(), errLimit) || d.extra != nativeFeedExtraInvocations {
		t.Fatal("SAME-original retry grant refund")
	}
	for _, facts := range []uint64{10_000, 100_000, 1_000_000} {
		cost := s.cost()
		cost.Facts = facts
		f, err := nativeFeedFrames(cost)
		if err != nil || f <= nativeFeedOriginals(cost)*10 || f > (1<<41)/1024 || f*260+65536 > 1<<39 {
			t.Fatal("checked actual phase/native/packet/record grants", facts, err)
		}
		var total uint64
		for seq := uint64(1); ; seq++ {
			_, count, err := nativeFeedGrantPlan(cost, 1<<20, 3, seq)
			if errors.Is(err, errInvalid) {
				break
			}
			if err != nil || count == 0 {
				t.Fatal(err)
			}
			total += count
		}
		if total != cost.homeCount() {
			t.Fatal("actual contiguous allocator grants")
		}
	}
	body, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var again nativeFeedSpec
	if err := nativeFeedDecodeJSON(body, &again); err != nil || again != s {
		t.Fatal(err)
	}
	if err := nativeFeedDecodeJSON(bytes.Replace(body, []byte(`"writers":32`), []byte(`"writers":true`), 1), &again); err == nil {
		t.Fatal("bool caller count accepted")
	}
	request := nativeFeedInput{Version: 1, Action: "page", Request: [16]byte{1}, Boundary: []byte{1}}
	wire, err := nativeFeedInputWire(request)
	if err != nil {
		t.Fatal(err)
	}
	if q, err := nativeFeedInputDecode(processRequest{op: processCostObserve, data: wire}); err != nil || !bytes.Equal(q.Boundary, request.Boundary) {
		t.Fatal(err)
	}
	for _, q := range []nativeFeedInput{{Version: 1, Action: "page", Boundary: []byte{1}}, {Version: 1, Action: "ack", Request: [16]byte{1}}, {Version: 1, Action: "records", From: 2, Through: 1, Boundary: []byte{1}}} {
		wire, err := nativeFeedInputWire(q)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nativeFeedInputDecode(processRequest{op: processCostObserve, data: wire}); !errors.Is(err, errInvalid) {
			t.Fatal("malformed RPC shape", err)
		}
	}
	probe, err := nativeFeedInputWire(nativeFeedInput{Version: 1, Action: "open", Token: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	r := processRequest{op: processCostObserve, data: probe}
	if nativeFeedProfileResponseCode(r, false, false, errGraphPinReleased) != 0 || nativeFeedProfileResponseCode(r, true, false, errGraphPinReleased) != 8 || nativeFeedProfileResponseCode(r, true, false, nil) != 0 {
		t.Fatal("fresh framing must classify only actual native error")
	}
	if nativeFeedProfileResponseCode(processRequest{op: processCostObserve, data: append(probe, 'x')}, true, false, errGraphPinReleased) != 0 {
		t.Fatal("noncanonical framing acquired status grammar")
	}
	column, err := nativeGraphCostColumnInput(nativeGraphCostColumnRequest{Action: nativeColumnsPinProbe, Generation: [16]byte{1}, RetainedToken: []byte{1}})
	if err != nil {
		t.Fatal(err)
	}
	wrapped, err := nativeGraphEdgeUniqueColumnInput(nativeGraphEdgeUniqueColumnControl{Action: "columns", Body: column})
	if err != nil {
		t.Fatal(err)
	}
	r = processRequest{op: processCostObserve, data: wrapped}
	if nativeFeedProfileResponseCode(r, false, true, errGraphPinReleased) != 8 || nativeFeedProfileResponseCode(r, false, false, errGraphPinReleased) != 0 {
		t.Fatal("strict GEC/default error compatibility")
	}
	if !errors.Is(nativeColumnProcessRemoteError{processRemoteError{code: 8}}, errGraphPinReleased) {
		t.Fatal("fresh remote sentinel parity")
	}
}
