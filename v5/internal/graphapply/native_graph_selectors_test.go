package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

// Authored guards; no source integration is execution or matrix qualification.
func TestNativeSelectorsAdmissionAndPrivateReadIdentity(t *testing.T) {
	s := nativeSelectorsSpec{Version: 1, Family: nativeSelectorsFamily, Facts: 40, CrossPercent: 50, Writers: 32, Layout: "columns"}
	if err := nativeSelectorsValidate(s.cost()); err != nil {
		t.Fatal(err)
	}
	if _, err := newNativeSelectorsSessions(context.Background(), nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil coordinator", err)
	}
	if _, err := nativeSelectorsColumnPolicy(nil, s, nativeSelectorsAdmission{}, [32]byte{}); !errors.Is(err, errInvalid) {
		t.Fatal("nil policy host", err)
	}
	for _, facts := range []uint64{10_000, 100_000, 1_000_000} {
		cost := s.cost()
		cost.Facts = facts
		f, err := nativeSelectorsFrames(cost)
		if err != nil || f <= nativeSelectorsOriginals(cost)*10 || f*260+65536 > 1<<39 || f > (1<<41)/1024 {
			t.Fatal("finite native stages and records", facts, err)
		}
		var home uint64
		for seq := uint64(1); ; seq++ {
			_, count, err := nativeSelectorsGrantPlan(cost, 1<<20, 3, seq)
			if errors.Is(err, errInvalid) {
				break
			}
			if err != nil || count == 0 {
				t.Fatal(err)
			}
			home += count
		}
		if home != cost.homeCount() {
			t.Fatal("genuine complete multi-block identity grants")
		}
	}
	body, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var again nativeSelectorsSpec
	if err := nativeFeedDecodeJSON(bytes.Replace(body, []byte(`"writers":32`), []byte(`"writers":true`), 1), &again); err == nil {
		t.Fatal("bool writer dimension")
	}
	for _, q := range []nativeSelectorsInput{
		{Version: 1, Action: "prepare", Request: [16]byte{1}, Mode: graphReadFresh},
		{Version: 1, Action: "scan", Handle: 1, Sequence: 1},
		{Version: 1, Action: "points", Handle: 1, Sequence: 2, IDs: []uint64{1, 3}},
		{Version: 1, Action: "release", Handle: 1},
	} {
		wire, err := nativeSelectorsInputWire(q)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = nativeSelectorsInputDecode(processRequest{op: processCostObserve, data: wire}); err != nil {
			t.Fatal(err)
		}
		if _, err = nativeSelectorsInputDecode(processRequest{op: processCostObserve, data: append(wire, 'x')}); !errors.Is(err, errInvalid) {
			t.Fatal("noncanonical caller frame", err)
		}
	}
	for _, action := range []string{"close", "release", "publish", "scan"} {
		q := nativeSelectorsInput{Version: 1, Action: action, Handle: 1}
		if action == "scan" {
			q.Sequence = 1
		}
		wire, err := nativeSelectorsInputWire(q)
		if err != nil {
			t.Fatal(err)
		}
		r := processRequest{op: processCostObserve, data: wire}
		if got, want := nativeSelectorsTerminalControl(r), action == "close" || action == "release"; got != want {
			t.Fatal("canonical terminal worker classification", action, got)
		}
		r.data = append(wire, 'x')
		if nativeSelectorsTerminalControl(r) {
			t.Fatal("malformed request obtained terminal worker")
		}
		r = processRequest{op: processSourceServe, data: wire}
		if nativeSelectorsTerminalControl(r) {
			t.Fatal("ordinary source request obtained selector terminal worker")
		}
	}
	for _, q := range []nativeSelectorsInput{
		{Version: 1, Action: "scan", Handle: 1},
		{Version: 1, Action: "scan", Handle: 1, Sequence: 1, IDs: []uint64{7}},
		{Version: 1, Action: "prepare", Request: [16]byte{1}, Mode: graphReadAfter, Coordinator: 3},
		{Version: 1, Action: "prepare", Request: [16]byte{1}, Mode: graphReadAfter, Coordinator: 3, Local: [16]byte{2}, Transaction: [16]byte{3}},
	} {
		wire, err := nativeSelectorsInputWire(q)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = nativeSelectorsInputDecode(processRequest{op: processCostObserve, data: wire}); !errors.Is(err, errInvalid) {
			t.Fatal("missing sequence or forged After identity", err)
		}
	}
	// Normal read cancellation/exhaustion cannot consume the independent finite
	// terminal lane. These owners have no pin: this directly checks local Close
	// admission without substituting a fake native terminal outcome.
	normal, cancelNormal := context.WithCancel(context.Background())
	cancelNormal()
	cleanup, cancelCleanup := context.WithDeadline(context.Background(), time.Now().Add(time.Minute))
	defer cancelCleanup()
	owner := func() *nativeSelectorsQuery { return &nativeSelectorsQuery{} }
	sessions := &nativeSelectorsSessions{ctx: normal, cleanup: cleanup, policy: nativeSelectorsReadPolicy{Calls: 1}, calls: 1, owners: map[uint64]*nativeSelectorsQuery{1: owner(), 2: owner()}}
	probe := &nativeSelectorsSessions{ctx: context.Background(), policy: nativeSelectorsReadPolicy{Calls: 1}, owners: make(map[uint64]*nativeSelectorsQuery)}
	probeWire, err := nativeSelectorsInputWire(nativeSelectorsInput{Version: 1, Action: "prepare", Request: [16]byte{9}, Mode: graphReadFresh})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := probe.serve(normal, processRequest{op: processCostObserve, data: probeWire}); !errors.Is(err, context.Canceled) || probe.starts != 0 || probe.queries != 0 || len(probe.owners) != 0 {
		t.Fatal("already canceled normal call admitted source work", err)
	}
	closeWire, err := nativeSelectorsInputWire(nativeSelectorsInput{Version: 1, Action: "close", Handle: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.serve(normal, processRequest{op: processCostObserve, data: closeWire}); err != nil || sessions.owners[1] != nil || sessions.calls != 1 || sessions.starts != 0 || sessions.cleanupCalls != 1 {
		t.Fatal("canceled/exhausted normal lane stranded local Close", err)
	}
	sessions.cleanupCalls = nativeSelectorsCleanupCalls
	closeWire, err = nativeSelectorsInputWire(nativeSelectorsInput{Version: 1, Action: "release", Handle: 2})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sessions.serve(context.Background(), processRequest{op: processCostObserve, data: closeWire}); !errors.Is(err, errLimit) || sessions.owners[2] == nil {
		t.Fatal("terminal lane exhaustion renewed or discarded owner", err)
	}
	sessions.cleanupCalls = 0
	cancelCleanup()
	if _, err := sessions.serve(context.Background(), processRequest{op: processCostObserve, data: closeWire}); !errors.Is(err, context.Canceled) || sessions.owners[2] == nil {
		t.Fatal("fixed terminal lifetime renewed", err)
	}
	// The native terminal itself uses its genuine original cleanup bank, not
	// the exhausted normal query counters. No constructed owner DTO supplies
	// authority: Begin/Publish install it through the actual six native voters.
	n, publisher, _, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, nil, durableColumnGraphSemanticContractID()))
	pin, err := publisher.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 100)
	if err != nil {
		t.Fatal(err)
	}
	token, err := pin.Publish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// These are actual native certificates, not profile labels grafted onto
	// another agreement. Full and scoped selector coordinates must both decode;
	// the Cost5 wire door must continue to refuse this distinct agreement.
	manifest, err := decodeGraphPinManifest(token.manifest)
	if err != nil {
		t.Fatal(err)
	}
	full := graphCut{certificates: []graphCertificate{manifest.parts[0].certificate, manifest.parts[1].certificate}}
	for _, cut := range []graphCut{full, {certificates: full.certificates[:1]}, {certificates: full.certificates[1:]}} {
		wire, err := nativeFeedCutWire(cut)
		if err != nil {
			t.Fatal(err)
		}
		actual, err := nativeSelectorsCutDecode(wire)
		if err != nil || !feedCutEqual(actual, cut) {
			t.Fatal("exact full/scoped selector cut", err)
		}
		if _, err := nativeFeedCutDecode(wire); !errors.Is(err, errInvalid) {
			t.Fatal("Cost5 accepted another agreement", err)
		}
	}
	for _, cut := range []graphCut{{}, {certificates: []graphCertificate{full.certificates[1], full.certificates[0]}}, {certificates: []graphCertificate{full.certificates[0], full.certificates[0]}}} {
		wire, err := nativeFeedCutWire(cut)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := nativeSelectorsCutDecode(wire); !errors.Is(err, errInvalid) {
			t.Fatal("empty/reordered/duplicate selector scope", err)
		}
	}
	wire, err := nativeFeedCutWire(full)
	if err != nil {
		t.Fatal(err)
	}
	h := n.hosts[0][0]
	if _, err := nativeScalarWorkloadRecords(t.Context(), h, full.certificates[0].index-1, full.certificates[0].index, wire, durableColumnGraphSemanticContractID()); err != nil {
		t.Fatal("actual scalar native records used a different decoder agreement", err)
	}
	// Codec-only legacy regression: these bytes remain inert until native At.
	legacy := graphCut{certificates: append([]graphCertificate(nil), full.certificates...)}
	for i := range legacy.certificates {
		legacy.certificates[i].scope.semantic = retainedFeedSemanticContractID()
	}
	legacyWire, err := nativeFeedCutWire(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if actual, err := nativeFeedCutDecode(legacyWire); err != nil || !feedCutEqual(actual, legacy) {
		t.Fatal("Cost5 exact two-certificate codec regression", err)
	}
	if _, err := nativeSelectorsCutDecode(legacyWire); !errors.Is(err, errInvalid) {
		t.Fatal("selector accepted Cost5 agreement", err)
	}
	liveCleanup, finishCleanup := context.WithTimeout(context.Background(), 30*time.Second)
	defer finishCleanup()
	sessions.cleanup, sessions.cleanupCalls = liveCleanup, 0
	sessions.owners[2] = &nativeSelectorsQuery{pin: pin, token: token}
	if _, err := sessions.serve(normal, processRequest{op: processCostObserve, data: closeWire}); err != nil || sessions.owners[2] != nil || sessions.calls != 1 || sessions.starts != 0 {
		t.Fatal("exhausted normal lane stranded funded native Release", err)
	}
	if _, err := publisher.Open(t.Context(), token); !errors.Is(err, errGraphPinReleased) {
		t.Fatal("cleanup reply did not authenticate actual native terminal", err)
	}
}
