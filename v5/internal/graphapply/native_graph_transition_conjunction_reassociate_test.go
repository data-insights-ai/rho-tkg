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

func TestNativeP06ReplacementUsesUnusedRemoteServiceForExactOldCut(t *testing.T) {
	n, _, ops, claims, wires := coordinatorNativeFixture(t)
	// coordinatorTestWire's fault injector accepts cross commands only. This
	// read-focused guard also submits real cut commands, so use its underlying
	// native transports from the outset, before either stream has been used.
	c, err := newCrossProcessCoordinator(n.hosts[0][0], map[uint64]graphPeerWireTransport{
		3: wires[0].nativeGraphTestTransport,
		8: wires[1].nativeGraphTestTransport,
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	old, err := before.Cut()
	if err != nil {
		t.Fatal(err)
	}
	if err := before.Close(); err != nil {
		t.Fatal(err)
	}
	result, err := c.Run(t.Context(), n.requestID(), ops, 37, claims)
	if err != nil || !result.decision.commit {
		t.Fatal("actual post-C mutation", err)
	}
	after, err := c.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	current, err := after.Cut()
	if err != nil {
		t.Fatal(err)
	}
	if err := after.Close(); err != nil {
		t.Fatal(err)
	}
	oldB, currentB := old.certificates[1], current.certificates[1]
	if oldB.scope.partition != 8 || currentB.sourceIndex <= oldB.sourceIndex || currentB.image == oldB.image {
		t.Fatal("old source is not distinguished from current")
	}
	oldService := wires[1].service
	last, digest, reply := oldService.last, oldService.digest, bytes.Clone(oldService.reply)
	arena, err := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	stale, err := newGraphAuthenticatedEndpoint(n.d, wires[1], newGraphRemoteBudget(defaultMaterializerLimits(), arena))
	if err != nil {
		t.Fatal(err)
	}
	if source, _, err := stale.openCertifiedSource(t.Context(), oldB); source != nil || !errors.Is(err, errInvalid) || !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("a restarted client reused the unchanged service's sequence", err)
	}
	h := n.hosts[1][1]
	server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	var inspected []uint64
	peer, err := nativeP06InitialRemotePeer(1, 4, func(peer uint64) ([]byte, error) {
		inspected = append(inspected, peer)
		if peer == 4 {
			return pairedGraphFaultConnections(1, map[uint64]*graphPeerService{1: oldService}, oldService.sources)
		}
		if peer != 5 {
			t.Fatal("selection exceeded its first genuine unused source", peer)
		}
		return pairedGraphFaultConnections(1, nil, server)
	})
	if err != nil || peer != 5 || len(inspected) != 2 || inspected[0] != 4 || inspected[1] != 5 {
		t.Fatal("actual service inventory did not select the unused configured voter", peer, inspected, err)
	}
	service, err := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: 1, host: h})
	if err != nil {
		t.Fatal(err)
	}
	arena, err = graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := newGraphAuthenticatedEndpoint(n.d, &nativeGraphTestTransport{service: service}, newGraphRemoteBudget(defaultMaterializerLimits(), arena))
	// This in-process fixture numbers voters 1..3 inside each partition;
	// process member5 is its second partition's native voter2.
	if err != nil || fresh.peer.voter != peer-3 || fresh.peer.scope.partition != 8 {
		t.Fatal("inventory choice is not the actual native endpoint", err)
	}
	source, _, err := fresh.openCertifiedSource(t.Context(), oldB)
	if err != nil {
		t.Fatal("actual unused source must authenticate the stored old certificate", err)
	}
	root, err := source.RootBounded(t.Context(), 172)
	closed := source.Close()
	if err != nil || closed != nil || root.Index != oldB.sourceIndex || !bytes.Equal(root.Image, oldB.image[:]) || bytes.Equal(root.Image, currentB.image[:]) {
		t.Fatal("replacement remote At substituted current for C", err, closed)
	}
	if oldService.last != last || oldService.digest != digest || !bytes.Equal(oldService.reply, reply) {
		t.Fatal("remote selection reset the retained pre-crash service ledger")
	}
	used, err := pairedGraphFaultConnections(1, map[uint64]*graphPeerService{1: oldService}, oldService.sources)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	if got, err := nativeP06InitialRemotePeer(1, 4, func(uint64) ([]byte, error) { calls++; return used, nil }); got != 0 || !errors.Is(err, errLimit) || calls != 3 {
		t.Fatal("exhaustion reset an old stream or searched beyond the three configured peers", got, calls, err)
	}
	if got, err := nativeP06InitialRemotePeer(1, 2, func(uint64) ([]byte, error) { t.Fatal("same-partition remote inspection"); return nil, nil }); got != 0 || !errors.Is(err, errInvalid) {
		t.Fatal("replacement lost its actual local partition source", got, err)
	}
	if got, err := nativeP06InitialRemotePeer(4, 1, func(uint64) ([]byte, error) { return []byte{2}, nil }); got != 0 || !errors.Is(err, errCorrupt) {
		t.Fatal("malformed inventory selected a source", got, err)
	}
}

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
