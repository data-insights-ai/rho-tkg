package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

func TestNativeConjunctionRetainedRoutingRequiresFreshCanonicalIdleOwner(t *testing.T) {
	legacy := processRequest{op: processPairedReconnect, data: allocationP08Words(1, 4)}
	decoded, retained, err := nativeConjunctionReconnectRequest(legacy, false)
	if err != nil || retained || !bytes.Equal(decoded.data, legacy.data) {
		t.Fatal("legacy reconnect changed", err)
	}
	fresh := legacy
	fresh.data = append([]byte(nativeConjunctionReconnectPrefix), legacy.data...)
	decoded, retained, err = nativeConjunctionReconnectRequest(fresh, true)
	if err != nil || !retained || !bytes.Equal(decoded.data, legacy.data) {
		t.Fatal("canonical fresh routing", err)
	}
	for _, malformed := range []processRequest{{op: processPairedReconnect, data: fresh.data[:len(fresh.data)-1]}, {op: processPairedReconnect, data: append(fresh.data[:len(fresh.data):len(fresh.data)], 0)}, {op: processPairedReconnect, index: 1, data: fresh.data}, {op: processStateOp, data: fresh.data}} {
		if _, _, err := nativeConjunctionReconnectRequest(malformed, true); !errors.Is(err, errInvalid) {
			t.Fatal("malformed fresh routing admitted", err)
		}
	}
	if _, _, err := nativeConjunctionReconnectRequest(fresh, false); !errors.Is(err, errInvalid) {
		t.Fatal("ordinary child imported fresh routing", err)
	}
	if err := nativeConjunctionRetainRoutesLocked(nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil routing owner", err)
	}
}

func TestNativeConjunctionSameCoordinatorRetainsGenuineUnknownRegisterAndStreams(t *testing.T) {
	n, c, ops, claims, wires := coordinatorNativeFixture(t)
	read, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, []uint64{3, 8})
	if err != nil {
		t.Fatal(err)
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	certified := c.certified
	decisions := 0
	c.beforeDecision = func(context.Context, crossTransaction, allocationProposal) error { decisions++; return nil }
	wires[0].dropRegister = true
	id := n.requestID()
	pending, err := c.Run(t.Context(), id, ops, 37, claims)
	if !errors.Is(err, errCrossProcessPending) || !errors.Is(err, graphstore.ErrSourceUnavailable) || pending.decision != (crossStatus{}) {
		t.Fatal("genuine lost native registration", err)
	}
	run := c.runs[pending.resume.tx]
	if run == nil || run.busy || !run.registrationAttempted {
		t.Fatal("unknown private registration was not retained")
	}
	manifest, digest := run.tx.manifest, sha256.Sum256(run.tx.manifest)
	streamA, streamB := c.streams[3], c.streams[8]
	server, err := newGraphSourceServer(t.Context(), c.host, defaultGraphSourceLimits(c.host.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	sessions := &graphProcessCertifiedSessions{owners: make(map[uint64]graphProcessCertifiedOwner)}
	r := processRequest{op: processPairedReconnect, data: allocationP08Words(1, 4)}
	if same, err := pairedGraphFaultReconnect(t.Context(), c.host, c, sessions, nil, server, r, 0, 0, false); same != c || !errors.Is(err, errInvalid) {
		t.Fatal("ordinary routing retired private pending run", err)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if same, err := pairedGraphFaultReconnect(canceled, c.host, c, sessions, nil, server, r, 0, 0, false, true); same != c || !errors.Is(err, context.Canceled) {
		t.Fatal("expired fresh routing mutated original", err)
	}
	next, err := newCrossProcessCoordinator(c.host, c.transports)
	if err != nil {
		t.Fatal(err)
	}
	next.streams[3], next.streams[8] = streamA, streamB
	apply := func() error { c.mu.Lock(); defer c.mu.Unlock(); return nativeConjunctionRetainRoutesLocked(c, next) }
	run.busy = true
	if err := apply(); !errors.Is(err, errInvalid) {
		t.Fatal("active run reassociated", err)
	}
	run.busy = false
	owner := run.tx.owner
	run.tx.owner = nil
	if err := apply(); !errors.Is(err, errInvalid) {
		t.Fatal("missing original owner was reconstructed", err)
	}
	run.tx.owner = owner
	if err := apply(); err != nil {
		t.Fatal(err)
	}
	if c.runs[pending.resume.tx] != run || run.resume != pending.resume || &run.tx.manifest[0] != &manifest[0] || sha256.Sum256(run.tx.manifest) != digest || c.certified != certified || c.streams[3] != streamA || c.streams[8] != streamB {
		t.Fatal("same coordinator lost private original/engine/stream")
	}
	opens := wires[0].opens + wires[1].opens
	result, err := c.Run(t.Context(), id, ops, 37, claims)
	if err != nil || result.resume != pending.resume || !result.decision.commit || decisions != 1 || len(wires[0].registrations) != 2 || !bytes.Equal(wires[0].registrations[0], wires[0].registrations[1]) || wires[0].opens+wires[1].opens != opens {
		t.Fatal("reassociation replaced original or reset source work", err)
	}
	data, err := c.OpenQuiescentSources(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer data.Close()
	assertCertifiedProjection(t, data.ReadView(), 3, true, nil)
	assertCertifiedIncident(t, data.ReadView(), ops[0].Scope, 1, 2, []graphstate.EntityID{3})
	assertCertifiedIncident(t, data.ReadView(), ops[0].Scope, 17, 18, []graphstate.EntityID{3})
}

func TestNativeConjunctionChangedPeerReplacesOnlyItsStreamAfterValidation(t *testing.T) {
	n, c, ops, claims, wires := coordinatorNativeFixture(t)
	wires[0].dropRegister = true
	pending, err := c.Run(t.Context(), n.requestID(), ops, 37, claims)
	if !errors.Is(err, errCrossProcessPending) {
		t.Fatal(err)
	}
	run := c.runs[pending.resume.tx]
	oldA, oldB := c.streams[3], c.streams[8]
	h := n.hosts[0][1]
	server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	service, err := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: 0, host: h})
	if err != nil {
		t.Fatal(err)
	}
	changed := &nativeGraphTestTransport{service: service}
	next, err := newCrossProcessCoordinator(c.host, map[uint64]graphPeerWireTransport{3: changed, 8: wires[1]})
	if err != nil {
		t.Fatal(err)
	}
	next.streams[8] = oldB
	apply := func() error { c.mu.Lock(); defer c.mu.Unlock(); return nativeConjunctionRetainRoutesLocked(c, next) }
	next.streams[3] = oldA
	if err := apply(); !errors.Is(err, errInvalid) || c.transports[3] != wires[0] {
		t.Fatal("changed peer borrowed prior stream or partially mutated routes", err)
	}
	next.streams[3] = &graphAuthenticatedStream{mu: newGraphRequestGate()}
	if err := apply(); err != nil {
		t.Fatal(err)
	}
	if c.runs[pending.resume.tx] != run || c.transports[3] != changed || c.transports[8] != wires[1] || c.streams[3] == oldA || c.streams[8] != oldB {
		t.Fatal("peer change reset original/unchanged stream")
	}
}
