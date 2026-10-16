package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"sync"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Guards the actual incoming-bank path: replaying the old log, keeping only
// current graph state, or dropping the private prepare/lock cannot satisfy it.
func TestCrossPartitionPreparedFollowerSnapshotKeepsKernelLockAndCompletionAcrossReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	testCrossPartitionPreparedSnapshot(t, false)
}
func TestCertifiedGraphActualAS5ActivationKeepsHeldCertifiedSourceAndReopensCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	testCrossPartitionPreparedSnapshot(t, true)
}
func testCrossPartitionPreparedSnapshot(t *testing.T, certified bool) {
	var n *allocationTestNetwork
	if certified {
		n = newCertifiedGraphNetwork(t)
	} else {
		n = newCrossPartitionNetwork(t)
	}
	ops, claims := crossEdgeFixture(t, n)
	var held *certifiedGraphRead
	var heldCut graphCut
	var coordinator *certifiedReadCoordinator
	var certTransport *certifiedTestTransport
	if certified {
		coordinator, certTransport = certifiedCoordinator(n)
		fresh := certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadFresh}, nil)
		var e error
		heldCut, e = fresh.Cut()
		if e != nil {
			t.Fatal(e)
		}
		if e := fresh.Close(); e != nil {
			t.Fatal(e)
		}
		certTransport.member = 2
		held = certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadAt, cut: heldCut}, nil)
	}
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	g := partitionSnapshotGroup(n, 0)
	transport := &declaredSnapshotTransport{t: t, g: g, dropFollower: true, queue: make([]replica.Packet, 0, 128)}
	oldIndex := g.drivers[2].Applied()
	old, err := g.stores[2].ApplicationView(oldIndex)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	submit := func(p allocationProposal) {
		t.Helper()
		e, err := n.hosts[0][0].Submit(p)
		if err != nil {
			t.Fatal(err)
		}
		transport.enqueue(replica.Output{Packets: e.packets, SnapshotSends: e.snapshotSends})
		transport.drain()
	}
	prepare, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	submit(prepare)
	cutIndex := g.drivers[0].Applied()
	if g.drivers[2].Applied() != oldIndex || cutIndex <= oldIndex {
		t.Fatal("snapshot follower did not remain behind")
	}
	crossAcceptanceState(t, n.hosts[0][0], registered.tx, true)
	if err := g.stores[0].PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	first, err := g.stores[0].FirstIndex()
	if err != nil || first <= oldIndex {
		t.Fatal("log replay could satisfy prepared snapshot", first, oldIndex, err)
	}
	transport.dropFollower = false
	pages := 0
	for range 20 {
		out, err := g.drivers[0].Tick()
		if err != nil {
			t.Fatal(err)
		}
		transport.enqueue(out)
		transport.drain()
		if transport.send != nil {
			break
		}
		offers, err := g.drivers[0].SnapshotSends()
		if err != nil {
			t.Fatal(err)
		}
		for _, send := range offers {
			complete := false
			for !complete && pages < 128 {
				complete, err = send.Build(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
				pages++
				if err != nil {
					t.Fatal(err)
				}
			}
			if !complete {
				t.Fatal("prepared manifest page cap")
			}
			if err := g.drivers[0].SealSnapshotSend(send); err != nil {
				t.Fatal(err)
			}
		}
	}
	if transport.send == nil {
		t.Fatal("actual prepared follower MsgSnap required")
	}
	manifest, err := transport.send.Manifest()
	if err != nil || manifest.Version != 5 || manifest.Contract.Version != 3 || manifest.Index != cutIndex {
		t.Fatal("recovery snapshot format/coordinate", manifest, err)
	}
	imported, err := g.stores[2].BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	defer imported.Abort()
	complete := false
	for !complete && pages < 128 {
		chunk, err := transport.send.Next(t.Context(), raftlog.ReadBudget{Rows: 8, Bytes: 64 << 10})
		pages++
		if err != nil || chunk.Version != 5 {
			t.Fatal(chunk.Version, err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		complete = chunk.Final
	}
	if !complete {
		t.Fatal("prepared stream page cap")
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if g.drivers[2].Applied() != oldIndex || g.stores[2].ApplicationGeneration() != 1 {
		t.Fatal("dormant prepared bank activated early")
	}
	activated, err := g.drivers[2].StepApplicationSnapshot(transport.packet, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if g.drivers[2].Applied() != cutIndex || g.stores[2].ApplicationGeneration() != 2 {
		t.Fatal("prepared generation activation missing")
	}
	feedback, err := g.drivers[0].ReportSnapshotSend(transport.send, true)
	if err != nil {
		t.Fatal(err)
	}
	transport.send, transport.packet = nil, replica.Packet{}
	transport.enqueue(feedback)
	transport.enqueue(activated)
	transport.drain()
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	if err := imported.Abort(); err != nil {
		t.Fatal(err)
	}
	for _, h := range n.hosts[0] {
		crossAcceptanceState(t, h, registered.tx, true)
	}
	if row, found, err := old.Get(t.Context(), crossLockKey(g.ns), 256); err != nil || found && len(row.Value) != 0 {
		t.Fatal("retired pre-YES view acquired lock", found, err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}
	if certified {
		edge, e := certifiedView(t, held).Entity(t.Context(), 3)
		if e != nil || edge.Found {
			t.Fatal("held certified source switched banks/graph roots", edge, e)
		}
		if e := held.Close(); e != nil {
			t.Fatal(e)
		}
	}
	crossReopenAll(t, n)
	crossAcceptanceState(t, n.hosts[0][2], registered.tx, true)
	registration = n.crossReadPhase(0, registered.tx, crossRegister)
	tx, err = n.hosts[0][0].RecoverCrossTransaction(registration)
	if err != nil {
		t.Fatal(err)
	}
	vote := n.crossReadPhase(0, registered.tx, crossPrepare)
	remote, err := n.hosts[0][0].PrepareCrossParticipant(tx, 8, registration, []crossProof{vote})
	if err != nil {
		t.Fatal(err)
	}
	if got := n.crossSubmit(1, remote); !got.yes {
		t.Fatal(got)
	}
	decisionProposal, err := n.hosts[0][0].DecideCrossTransaction(tx, []crossProof{vote, n.crossReadPhase(1, registered.tx, crossPrepare)})
	if err != nil {
		t.Fatal(err)
	}
	if got := n.crossSubmit(0, decisionProposal); !got.commit {
		t.Fatal(got)
	}
	decision := n.crossReadPhase(0, registered.tx, crossDecide)
	n.crossResolve(decision)
	crossReopenAll(t, n)
	for member := range 3 {
		for group := range 2 {
			crossAcceptanceState(t, n.hosts[group][member], registered.tx, false)
		}
		v, close := crossView(t, n, member, [2]uint64{})
		edge, err := v.Entity(t.Context(), 3)
		for _, endpoint := range []struct {
			owner graphstate.EntityID
			life  graphstate.LifeID
		}{{1, 2}, {17, 18}} {
			page, err := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: endpoint.owner, Life: endpoint.life, Window: ops[0].Scope}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
			if err != nil || len(page.Entities) != 1 || page.Entities[0] != 3 {
				t.Fatal("snapshot/reopen lost complete endpoint effects", endpoint, page, err)
			}
		}
		close()
		if err != nil || !edge.Found || edge.Record.Source != 1 || edge.Record.Target != 17 {
			t.Fatal("snapshot lost original resolved cross edge", edge, err)
		}
	}
	if certified {
		reopened := certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadAt, cut: heldCut}, nil)
		edge, e := certifiedView(t, reopened).Entity(t.Context(), 3)
		if e != nil || edge.Found {
			t.Fatal("AS5/reopen certified old cut lost", edge, e)
		}
	}
	previous, _ := partitionProjection(t, n.hosts[0][2], oldIndex, 3, graphGenesisBinding(t).Axis())
	if previous.Exists {
		t.Fatal("retained pre-YES history acquired resolved entity", previous)
	}
}

func crossAcceptanceState(t *testing.T, h *allocationHost, tx [16]byte, prepared bool) {
	t.Helper()
	v, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	for _, tag := range []byte{'R', 'P', 'K'} {
		r, found, _, err := v.GetControl(t.Context(), crossControlKey(h.machine.ns, tx, tag), raftlog.ReadBudget{Rows: 2, Bytes: min(v.ReadLimits().Bytes, h.machine.limits.commandBytes+4096)})
		if err != nil || !found || len(r.Value) == 0 {
			t.Fatal("missing retained prepare authority", tag, found, err)
		}
	}
	r, found, err := v.Get(t.Context(), crossLockKey(h.machine.ns), 256)
	if err != nil || !found || prepared && !bytes.Equal(r.Value, tx[:]) || !prepared && len(r.Value) != 0 {
		t.Fatal("prepared lock changed", r, found, err)
	}
}

func TestCrossPartitionRecoveryVerifierNilAndForeignViewRefuse(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	h := n.hosts[0][0]
	if _, err := h.machine.VerifyApplicationRecovery(t.Context(), nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	v, err := n.hosts[1][0].machine.store.ApplicationView(n.hosts[1][0].driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	if _, err := h.machine.VerifyApplicationRecovery(t.Context(), nil, v); !errors.Is(err, errInvalid) {
		t.Fatal("foreign partition view admitted", err)
	}
	if _, err := v.Root(); err != nil {
		t.Fatal("borrowed view closed by verifier", err)
	}
}

func crossCapacityNetwork(t *testing.T) *allocationTestNetwork {
	t.Helper()
	return newCrossPartitionNetworkWithPolicy(t, func(p *raftlog.ApplicationPolicy) {
		p.MaxInstallBytes = 128 << 10
		p.RetainedApplicationBytes = 384 << 10
	})
}

func TestCrossPartitionReservedCompletionSurvivesOrdinaryCliffForwardingAndExactQueuedRetry(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := crossCapacityNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	votes := []crossProof{}
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[0][0].PrepareCrossParticipant(tx, partition, registration, votes)
		if err != nil {
			t.Fatal(err)
		}
		// A source-owned command submitted through a follower is forwarded to
		// the actual leader; all three voter applications must consume its phase.
		e, err := n.hosts[group][1].Submit(p)
		if err != nil {
			t.Fatal("forwarded prepare", err)
		}
		n.crossPump(group, e)
		vote := n.crossReadPhase(group, registered.tx, crossPrepare)
		if !vote.status.yes {
			t.Fatal(vote.status)
		}
		votes = append(votes, vote)
	}
	for group := range 2 {
		owner, life := graphstate.EntityID(1), graphstate.LifeID(2)
		if group == 1 {
			owner, life = 17, 18
		}
		cliff := false
		for range 128 {
			p, err := n.hosts[group][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: owner, Life: life, Name: "capacity-padding", Scope: ops[0].Scope}}, 25, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := n.hosts[group][0].machine.store.AdmitApplication(len(p.wire)); errors.Is(err, raftlog.ErrLimit) {
				cliff = true
				break
			} else if err != nil {
				t.Fatal(err)
			}
			n.submitPartitionGraph(group, p, reasonPreparedConflict)
		}
		if !cliff {
			t.Fatal("finite ordinary admission cliff not reached", group)
		}
		for _, h := range n.hosts[group] {
			if err := h.machine.store.AdmitApplication(1); !errors.Is(err, raftlog.ErrLimit) {
				t.Fatal("ordinary voter capacity remained", group, err)
			}
			crossAcceptanceState(t, h, registered.tx, true)
		}
	}
	decisionProposal, err := n.hosts[0][0].DecideCrossTransaction(tx, votes)
	if err != nil {
		t.Fatal(err)
	}
	first, err := n.hosts[0][1].Submit(decisionProposal)
	if err != nil {
		t.Fatal("reserved forwarded decision", err)
	}
	second, err := n.hosts[0][1].Submit(decisionProposal)
	if err != nil {
		t.Fatal("exact pending forwarded retry", err)
	}
	first.packets = append(first.packets, second.packets...)
	n.crossPump(0, first)
	decision := n.crossReadPhase(0, registered.tx, crossDecide)
	if !decision.status.commit {
		t.Fatal("protected decision lost", decision.status)
	}
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[0][0].ResolveCrossParticipant(partition, decision)
		if err != nil {
			t.Fatal(err)
		}
		e, err := n.hosts[group][1].Submit(p)
		if err != nil {
			t.Fatal("reserved forwarded resolution", group, err)
		}
		n.crossPump(group, e)
		for _, h := range n.hosts[group] {
			crossAcceptanceState(t, h, registered.tx, false)
		}
	}
	for member := range 3 {
		v, close := crossView(t, n, member, [2]uint64{})
		edge, err := v.Entity(t.Context(), 3)
		close()
		if err != nil || !edge.Found {
			t.Fatal("reserved completion dropped graph effects", member, edge, err)
		}
	}
}

func TestCrossPartitionInsufficientCompletionCapacityRefusesRegistrationBeforeYES(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := crossCapacityNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	h := n.hosts[0][0]
	refused := false
	for range 128 {
		_, err := h.machine.store.AdmitApplicationRecovery(t.Context(), register.wire)
		if errors.Is(err, raftlog.ErrLimit) {
			if ordinary := h.machine.store.AdmitApplication(len(register.wire)); ordinary != nil {
				t.Fatal("ordinary already exhausted; reservation-only cliff not demonstrated", ordinary)
			}
			refused = true
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		// Existing entity rewrite consumes ordinary retained capacity without
		// installing any cross transaction, vote, kernel or recovery owner.
		p, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "before-reserve", Scope: ops[0].Scope}}, 25, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, p, reasonNone)
	}
	if !refused {
		t.Fatal("completion-capacity cliff not reached")
	}
	before := h.driver.Applied()
	if e, err := h.Submit(register); !errors.Is(err, raftlog.ErrLimit) || len(e.packets)+len(e.snapshotSends)+len(e.replies)+len(e.crossReplies) != 0 {
		t.Fatal("insufficient completion admitted", e, err)
	}
	if h.driver.Applied() != before {
		t.Fatal("refusal published a checkpoint")
	}
	for _, voter := range n.hosts[0] {
		v, err := voter.machine.store.ApplicationView(voter.driver.Applied())
		if err != nil {
			t.Fatal(err)
		}
		for _, tag := range []byte{'R', 'P', 'K'} {
			_, found, _, err := v.GetControl(t.Context(), crossControlKey(voter.machine.ns, [16]byte(tx.manifest[4:20]), tag), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
			if err != nil || found {
				t.Fatal("capacity refusal published private authority", tag, found, err)
			}
		}
		if err := v.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCrossPartitionUnissuedCompletedReadSequenceMustNotCreateTransaction(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	sources[0].id.sequence = sources[0].owner.sequence + 1
	tx, proposal, err := n.hosts[0][0].PrepareCrossPartitionGraphOperations(t.Context(), n.requestID(), ops, 24, claims, sources)
	if !errors.Is(err, errInvalid) || tx.owner != nil || len(tx.manifest)+len(proposal.wire) != 0 {
		t.Fatal("unissued completed read created transaction", len(tx.manifest), len(proposal.wire), err)
	}
	sources[0].id.sequence--
	if err := sources[1].owner.Close(); err != nil {
		t.Fatal(err)
	}
	tx, proposal, err = n.hosts[0][0].PrepareCrossPartitionGraphOperations(t.Context(), n.requestID(), ops, 24, claims, sources)
	if !errors.Is(err, errInvalid) || tx.owner != nil || len(tx.manifest)+len(proposal.wire) != 0 {
		t.Fatal("closed source returned transaction", err)
	}
}

func TestCrossPartitionSourceProofsRemainSafeWhileForeignHostCompletesReads(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	var readers sync.WaitGroup
	readers.Go(func() {
		for range 8 {
			n.read(1, allocationQuery{kind: observeConfiguration})
		}
	})
	for i := range 8 {
		if _, _, err := n.hosts[0][0].PrepareCrossPartitionGraphOperations(t.Context(), requestID{200, byte(i + 1)}, ops, 24, claims, sources); err != nil {
			t.Error(err)
		}
	}
	readers.Wait()
}

func TestCrossPartitionMixedPendingReadsMustShareSixteenRequestLimit(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	h := n.hosts[0][0]
	for i := range 16 {
		var event allocationEvent
		var err error
		if i%2 == 0 {
			event, err = h.ReadCrossTransaction([16]byte{1}, [16]byte{byte(i + 1)})
		} else {
			event, err = h.Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{byte(i + 1)}})
		}
		if err != nil || len(event.replies)+len(event.crossReplies) != 0 {
			t.Fatal("unexpected read completion before quorum", i, err)
		}
	}
	event, err := h.Read(allocationQuery{kind: observeConfiguration, nonce: [16]byte{99}})
	if !errors.Is(err, errLimit) || event.readID != (allocationReadID{}) || len(event.packets)+len(event.replies)+len(event.crossReplies) != 0 {
		t.Fatal("mixed pending limit exceeded", len(h.pending)+len(h.crossPending), err)
	}
}

func TestCrossPartitionStatusAndManifestMustShareSourceRowBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	_, p := n.crossPlan(ops, claims, 24)
	status := n.crossSubmit(0, p)
	h := n.hosts[0][0]
	limited := *h.machine
	limited.limits.sourceRows = 1
	got, manifest, err := limited.observeCrossTransaction(crossQuery{tx: status.tx}, h.driver.Applied())
	if !errors.Is(err, errLimit) || got != (crossStatus{}) || len(manifest) != 0 {
		t.Fatal("registration read exceeded remaining source rows", got.phase, len(manifest), err)
	}
}

func TestCrossPartitionPlanningCannotSpendSourceBudgetAgainForEachParticipant(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	h := n.hosts[0][0]
	original := h.machine.limits
	defer func() { h.machine.limits = original }()
	for _, limit := range []func(*materializerLimits){func(l *materializerLimits) { l.sourceRows = 1 }, func(l *materializerLimits) { l.sourceBytes = 172 }, func(l *materializerLimits) { l.outputBytes = 8192 }} {
		h.machine.limits = original
		limit(&h.machine.limits)
		tx, p, err := h.PrepareCrossPartitionGraphOperations(t.Context(), n.requestID(), ops, 24, claims, sources)
		if !errors.Is(err, errLimit) || tx.owner != nil || len(tx.manifest)+len(p.wire) != 0 {
			t.Fatal("aggregate source/output refusal returned transaction", err)
		}
		for _, owner := range []*allocationHost{n.hosts[0][0], n.hosts[1][0]} {
			usage, err := owner.machine.store.ApplicationUsage()
			if err != nil || usage.Views != 0 {
				t.Fatal("failed planning retained source view", usage, err)
			}
		}
	}
}

func TestCrossPartitionRegisteredIdentityCannotEnterLocalGraphDoor(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	original := n.crossReadPhase(0, registered.tx, crossRegister)
	h := n.hosts[0][0]
	local, err := h.PreparePartitionGraphOperations(crossRawRequestKey(t, tx), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "must-not-exist", Scope: ops[0].Scope}}, 25, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, local, reasonMismatch)
	current := n.crossReadPhase(0, registered.tx, crossRegister)
	if current.status != original.status {
		t.Fatal("local reuse changed cross registration", current.status, original.status)
	}
	for _, voter := range n.hosts[0] {
		projection, _ := partitionProjection(t, voter, voter.driver.Applied(), 1, graphGenesisBinding(t).Axis())
		if len(projection.Labels) != 0 {
			t.Fatal("registered identity installed local graph effects", projection)
		}
	}
}

func TestCrossPartitionTwoCoordinatorsMustNotDeadlockOnCompletionReservations(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	ids := crossRequestsWithLoser(n, 1)
	a, aRegister := crossPlanAt(n, 0, ids[0], ops, claims, 24)
	aRegistered := n.crossSubmit(0, aRegister)
	aRegistration := n.crossReadPhase(0, aRegistered.tx, crossRegister)
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	b, bRegister, err := n.hosts[1][0].PrepareCrossPartitionGraphOperations(t.Context(), ids[1], ops, 25, claims, sources)
	if err != nil {
		t.Fatal("second declared coordinator plan", err)
	}
	bRegistered := n.crossSubmit(1, bRegister)
	bRegistration := n.crossReadPhase(1, bRegistered.tx, crossRegister)
	aFirst, err := n.hosts[0][0].PrepareCrossParticipant(a, 3, aRegistration, nil)
	if err != nil {
		t.Fatal(err)
	}
	if vote := n.crossSubmit(0, aFirst); !vote.yes {
		t.Fatal(vote)
	}
	aVote := n.crossReadPhase(0, aRegistered.tx, crossPrepare)
	bFirst, err := n.hosts[1][0].PrepareCrossParticipant(b, 3, bRegistration, nil)
	if err != nil {
		t.Fatal(err)
	}
	aSecond, err := n.hosts[0][0].PrepareCrossParticipant(a, 8, aRegistration, []crossProof{aVote})
	if err != nil {
		t.Fatal(err)
	}
	bEvent, bErr := n.hosts[0][0].Submit(bFirst)
	aEvent, aErr := n.hosts[1][0].Submit(aSecond)
	if errors.Is(aErr, raftlog.ErrLimit) && errors.Is(bErr, raftlog.ErrLimit) {
		if len(aEvent.packets)+len(bEvent.packets) != 0 {
			t.Fatal("refused proposals emitted packets")
		}
		proof := n.capacityRead(1, aSecond)
		abort, existing, err := n.hosts[1][0].AbortCrossTransactionForCapacity(b, proof)
		if err != nil || existing != (crossStatus{}) {
			t.Fatal("loser capacity abort", existing, err)
		}
		if got := n.crossSubmit(1, abort); got.commit {
			t.Fatal("capacity conflict committed", got)
		}
		decision := n.crossReadPhase(1, bRegistered.tx, crossDecide)
		resolve, err := n.hosts[1][0].ResolveCrossParticipant(8, decision)
		if err != nil {
			t.Fatal(err)
		}
		n.crossSubmit(1, resolve)
		if got := n.crossSubmit(1, aSecond); !got.yes {
			t.Fatal("loser did not release winner resource", got)
		}
		votes := []crossProof{aVote, n.crossReadPhase(1, aRegistered.tx, crossPrepare)}
		commit, err := n.hosts[0][0].DecideCrossTransaction(a, votes)
		if err != nil {
			t.Fatal(err)
		}
		if got := n.crossSubmit(0, commit); !got.commit {
			t.Fatal(got)
		}
		n.crossResolve(n.crossReadPhase(0, aRegistered.tx, crossDecide))
		return
	}
	if aErr != nil && !errors.Is(aErr, raftlog.ErrLimit) || bErr != nil && !errors.Is(bErr, raftlog.ErrLimit) {
		t.Fatal("unexpected prepare refusal", aErr, bErr)
	}
	if aErr == nil {
		n.crossPump(1, aEvent)
	}
	if bErr == nil {
		n.crossPump(0, bEvent)
	}
}

func (n *allocationTestNetwork) capacityRead(group int, proposal allocationProposal) crossCapacityProof {
	n.t.Helper()
	event, err := n.hosts[group][0].ReadCrossCapacityConflict(proposal, [16]byte(n.requestID()))
	if err != nil {
		n.t.Fatal(err)
	}
	queue := event.packets
	replies := event.capacityReplies
	for steps := 0; len(queue) > 0; steps++ {
		if steps >= 1024 {
			n.t.Fatal("capacity packet schedule exceeded")
		}
		p := queue[0]
		queue = queue[1:]
		if p.Snapshot || p.To < 1 || p.To > 3 {
			n.t.Fatal("unexpected capacity transport")
		}
		e, err := n.hosts[group][p.To-1].Step(p)
		if err != nil {
			n.t.Fatal(err)
		}
		if len(queue)+len(e.packets) > 128 || len(replies)+len(e.capacityReplies) > 16 {
			n.t.Fatal("capacity transport budget")
		}
		queue = append(queue, e.packets...)
		replies = append(replies, e.capacityReplies...)
	}
	if len(replies) != 1 || replies[0].err != nil {
		n.t.Fatal("capacity completed read", replies)
	}
	return replies[0].proof
}

func TestCrossPartitionAbortBeforeCoordinatorPrepareMustRetireOnlyItsUnusedCredits(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	claims[0].grant.sequence = 2
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	h := n.hosts[1][0]
	tx, register, err := h.PrepareCrossPartitionGraphOperations(t.Context(), n.requestID(), ops, 24, claims, sources)
	if err != nil {
		t.Fatal(err)
	}
	registered := n.crossSubmit(1, register)
	registration := n.crossReadPhase(1, registered.tx, crossRegister)
	prepare, err := h.PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	no := n.crossSubmit(0, prepare)
	if no.yes || no.why != reasonInvalid {
		t.Fatal("fixture needs actual first-participant NO", no)
	}
	decisionProposal, err := h.DecideCrossTransaction(tx, []crossProof{n.crossReadPhase(0, no.tx, crossPrepare)})
	if err != nil {
		t.Fatal(err)
	}
	if status := n.crossSubmit(1, decisionProposal); status.commit {
		t.Fatal("NO committed", status)
	}
	decision := n.crossReadPhase(1, no.tx, crossDecide)
	for group, partition := range []uint64{3, 8} {
		resolve, err := h.ResolveCrossParticipant(partition, decision)
		if err != nil {
			t.Fatal(err)
		}
		n.crossSubmit(group, resolve)
	}
	claims[0].grant.sequence = 1
	sources = []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	_, next, err := h.PrepareCrossPartitionGraphOperations(t.Context(), n.requestID(), ops, 25, claims, sources)
	if err != nil {
		t.Fatal(err)
	}
	event, err := h.Submit(next)
	if err != nil {
		t.Fatal("terminal ABORT left never-used coordinator Prepare credit blocking fresh transaction", err)
	}
	n.crossPump(1, event)
}

func TestCrossPartitionManifestProofAndAggregateRefusalsLeaveNoOutput(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	h := n.hosts[0][0]
	l := h.machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeCrossManifest(tx.manifest, l, arena)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*crossManifest)
	}{
		{"tx", func(m *crossManifest) { m.tx[0] ^= 1 }},
		{"coordinator", func(m *crossManifest) { m.coordinator = 8 }},
		{"configuration", func(m *crossManifest) { m.configuration[0] ^= 1 }},
		{"declaration", func(m *crossManifest) { m.declaration[0] ^= 1 }},
		{"routing", func(m *crossManifest) { m.routing[0] ^= 1 }},
		{"template", func(m *crossManifest) { m.template[0] ^= 1 }},
		{"participant", func(m *crossManifest) {
			m.parts = append([]crossPart(nil), m.parts...)
			m.parts[0].binding.Partition = 99
		}},
		{"floor", func(m *crossManifest) {
			m.parts = append([]crossPart(nil), m.parts...)
			m.parts[0].floor = m.request.revision.ID()
		}},
		{"kernel", func(m *crossManifest) {
			m.parts = append([]crossPart(nil), m.parts...)
			m.parts[0].kernel = bytes.Clone(m.parts[0].kernel)
			m.parts[0].kernel[len(m.parts[0].kernel)-1] ^= 1
		}},
		{"missing-authority", func(m *crossManifest) { m.parts = m.parts[:1] }},
	} {
		t.Run(test.name, func(t *testing.T) {
			bad := m
			test.mutate(&bad)
			wire, err := encodeCrossManifest(bad, l)
			if err != nil {
				t.Fatal(err)
			}
			p, err := h.crossProposal(crossRegister, 3, wire, nil)
			if err != nil {
				if !errors.Is(err, errCorrupt) && !errors.Is(err, errInvalid) && !errors.Is(err, graphstore.ErrCorrupt) {
					t.Fatal(err)
				}
				if len(p.wire) != 0 {
					t.Fatal("invalid manifest returned proposal")
				}
				return
			}
			v, err := h.machine.store.ApplicationView(h.driver.Applied())
			if err != nil {
				t.Fatal(err)
			}
			defer v.Close()
			purpose, err := h.machine.VerifyApplicationRecovery(t.Context(), p.wire, v)
			if !errors.Is(err, errInvalid) || purpose.Reserve != nil || purpose.Use != nil {
				t.Fatal("manifest binding returned capacity", err)
			}
		})
	}
	_, ownedBytes, err := decodeCrossCommand(register.wire, l)
	if err != nil {
		t.Fatal(err)
	}
	for _, cap := range []int{ownedBytes, ownedBytes - 1} {
		tight := l
		tight.commandOwnedBytes = cap
		_, _, err := decodeCrossCommand(register.wire, tight)
		if cap == ownedBytes && err != nil || cap < ownedBytes && !errors.Is(err, errLimit) {
			t.Fatal("cross command ownership boundary", cap, err)
		}
	}
	v, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	for _, mutate := range []func(*materializerLimits){func(l *materializerLimits) { l.outputBytes = 8192 }, func(l *materializerLimits) { l.sourceRows = 1 }, func(l *materializerLimits) { l.sourceBytes = 172 }} {
		copyMachine := *h.machine
		mutate(&copyMachine.limits)
		purpose, err := copyMachine.VerifyApplicationRecovery(t.Context(), register.wire, v)
		if !errors.Is(err, errLimit) && !errors.Is(err, graphstore.ErrResourceLimit) && !errors.Is(err, raftlog.ErrLimit) || purpose.Reserve != nil || purpose.Use != nil {
			t.Fatal("aggregate boundary granted partial recovery", err)
		}
	}
	status := crossStatus{ns: h.machine.ns, tx: m.tx, digest: sha256.Sum256(tx.manifest), phase: crossRegister, index: 1}
	for _, command := range []crossCommand{{ns: h.machine.ns, phase: crossRegister, proofs: []crossStatus{status}}, {ns: h.machine.ns, phase: crossPrepare}, {ns: h.machine.ns, phase: crossResolve, proofs: []crossStatus{status}}, {ns: h.machine.ns, phase: crossDecide}} {
		err := validateCrossPhaseProofs(command, m)
		if !errors.Is(err, errInvalid) && !errors.Is(err, replica.ErrUnavailable) {
			t.Fatal("phase proof accepted", command.phase, err)
		}
	}
}

func TestCrossPartitionCorruptPreparedStatusAndLocksCannotRestoreOrStage(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	prepare, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	yes := n.crossSubmit(0, prepare)
	if !yes.yes {
		t.Fatal(yes)
	}
	h := n.hosts[0][0]
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	q := reader{ctx: t.Context(), view: view, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation}
	if err := h.machine.checkCrossLock(&q); err != nil || q.arena == nil || len(q.writes) != 0 {
		t.Fatal("borrowed prepared-lock validation failed or emitted effects", err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	statusWire, err := encodeCrossStatus(yes)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		row  raftlog.KV
	}{
		{"short-lock", raftlog.KV{Key: crossLockKey(h.machine.ns), Value: []byte{1}}},
		{"zero-lock", raftlog.KV{Key: crossLockKey(h.machine.ns), Value: make([]byte, 16)}},
		{"missing-status", raftlog.KV{Key: crossKey(h.machine.ns, yes.tx), Deleted: true}},
		{"malformed-status", raftlog.KV{Key: crossKey(h.machine.ns, yes.tx), Value: []byte("malformed")}},
		{"unprepared-status", raftlog.KV{Key: crossKey(h.machine.ns, yes.tx), Value: func() []byte { s := yes; s.phase = crossRegister; b, _ := encodeCrossStatus(s); return b }()}},
		{"changed-manifest-status", raftlog.KV{Key: crossKey(h.machine.ns, yes.tx), Value: func() []byte { s := yes; s.digest[0] ^= 1; b, _ := encodeCrossStatus(s); return b }()}},
		{"future-status", raftlog.KV{Key: crossKey(h.machine.ns, yes.tx), Value: func() []byte { s := yes; s.index = ^uint64(0); b, _ := encodeCrossStatus(s); return b }()}},
	} {
		t.Run(test.name, func(t *testing.T) {
			index := corruptDeclaredKV(t, h.machine.store, test.row)
			_, image, err := h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if err := h.machine.Restore(index, image); !errors.Is(err, errCorrupt) {
				t.Fatal("corrupt prepare accepted on restore", err)
			}
			if test.name != "short-lock" && test.name != "zero-lock" && test.name != "missing-status" && test.name != "unprepared-status" {
				batch, err := h.machine.Stage(replica.Entry{Index: index + 1, Generation: h.machine.store.ApplicationGeneration(), Data: prepare.wire}, h.machine.store.ApplicationBudget())
				if !errors.Is(err, errCorrupt) && !errors.Is(err, errInvalid) || len(batch.Writes)+len(batch.ControlPuts)+len(batch.Changes)+len(batch.Outcome) != 0 {
					t.Fatal("corrupt prepare emitted partial batch", err)
				}
			}
			original := raftlog.KV{Key: crossKey(h.machine.ns, yes.tx), Value: statusWire}
			if bytes.Equal(test.row.Key, crossLockKey(h.machine.ns)) {
				original = raftlog.KV{Key: test.row.Key, Value: yes.tx[:]}
			}
			index = corruptDeclaredKV(t, h.machine.store, original)
			_, image, err = h.machine.store.Checkpoint()
			if err != nil {
				t.Fatal(err)
			}
			if err := h.machine.Restore(index, image); err != nil {
				t.Fatal("fixture restoration lost original prepare", err)
			}
		})
	}
}

func TestCrossPartitionStagingRefusalsKeepAllEffectsPrivate(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	_, register := n.crossPlan(ops, claims, 24)
	h := n.hosts[0][0]
	index, _, err := h.machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	entry := replica.Entry{Index: index + 1, Generation: h.machine.store.ApplicationGeneration(), Data: register.wire}
	for _, output := range []int{8192, 64 << 10, 96 << 10, 128 << 10} {
		m := *h.machine
		m.limits.outputBytes = output
		batch, err := m.Stage(entry, h.machine.store.ApplicationBudget())
		if err == nil {
			t.Fatal("insufficient aggregate output admitted staging", output)
		}
		if !errors.Is(err, errLimit) && !errors.Is(err, graphstore.ErrResourceLimit) && !errors.Is(err, raftlog.ErrLimit) || len(batch.Writes)+len(batch.ControlPuts)+len(batch.Outcome)+len(batch.Changes) != 0 {
			t.Fatal("output refusal leaked effects", output, err)
		}
	}
	for _, budget := range []raftlog.ApplicationBudget{{Writes: 1, Bytes: 1 << 20, ImageBytes: 172, ChangeBytes: 1 << 20, OutcomeBytes: 256}, {Writes: 256, Bytes: 2048, ImageBytes: 172, ChangeBytes: 1 << 20, OutcomeBytes: 256}, {Writes: 256, Bytes: 1 << 20, ImageBytes: 171, ChangeBytes: 1 << 20, OutcomeBytes: 256}, {Writes: 256, Bytes: 1 << 20, ImageBytes: 172, ChangeBytes: 1 << 20, OutcomeBytes: 1}} {
		batch, err := h.machine.Stage(entry, budget)
		if !errors.Is(err, errLimit) && !errors.Is(err, raftlog.ErrLimit) || len(batch.Writes)+len(batch.ControlPuts)+len(batch.Outcome) != 0 {
			t.Fatal("batch framing refusal leaked effects", budget, err)
		}
	}
	bad, _, err := decodeCrossCommand(register.wire, h.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(*crossCommand){func(r *crossCommand) { r.ns.partition = 8 }, func(r *crossCommand) { r.phase = crossPrepare }, func(r *crossCommand) { r.manifest = bytes.Clone(r.manifest); r.manifest[len(r.manifest)-1] ^= 1 }} {
		r := bad
		mutation(&r)
		entry.Data, err = encodeCrossCommand(r, h.machine.limits)
		if err != nil {
			t.Fatal(err)
		}
		batch, err := h.machine.Stage(entry, h.machine.store.ApplicationBudget())
		if !errors.Is(err, errInvalid) && !errors.Is(err, errCorrupt) || len(batch.Writes)+len(batch.ControlPuts)+len(batch.Outcome) != 0 {
			t.Fatal("invalid command leaked effects", err)
		}
	}
}

func TestCrossPartitionControlReadersRefusePartialSourceOrOwnedResults(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	prepare, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	yes := n.crossSubmit(0, prepare)
	h := n.hosts[0][0]
	v, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer v.Close()
	base, err := v.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	makeReader := func() reader {
		return reader{ctx: t.Context(), view: v, ns: h.machine.ns, base: base, limits: h.machine.limits.allocation, bytes: cap(base.Image)}
	}
	q := makeReader()
	q.limits.readRows = 0
	if _, err := crossLock(&q); !errors.Is(err, errLimit) {
		t.Fatal("lock read ignored exhausted source rows", err)
	}
	for _, retained := range []int{1, 700} {
		q := makeReader()
		q.arena, err = graphstate.NewOutputBudget(retained)
		if err != nil {
			t.Fatal(err)
		}
		r, found, err := crossControl(&q, crossControlKey(h.machine.ns, yes.tx, 'P'), 256)
		if !errors.Is(err, errLimit) || found || len(r.Key)+len(r.Value) != 0 {
			t.Fatal("control read leaked unadmitted owned result", retained, err)
		}
	}
	q = makeReader()
	r, found, err := crossControl(&q, crossControlKey(h.machine.ns, yes.tx, 'P'), 100)
	if !errors.Is(err, errCorrupt) || found || len(r.Key)+len(r.Value) != 0 {
		t.Fatal("control value ceiling ignored", err)
	}
	q = makeReader()
	q.limits.readBytes = q.bytes + 128 + 4*len(crossControlKey(h.machine.ns, yes.tx, 'P'))
	r, found, err = crossControl(&q, crossControlKey(h.machine.ns, yes.tx, 'P'), 256)
	if !errors.Is(err, raftlog.ErrLimit) || found || len(r.Key)+len(r.Value) != 0 {
		t.Fatal("control source ceiling ignored", err)
	}
	q = makeReader()
	q.arena, err = graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	m, err := decodeCrossManifest(tx.manifest, h.machine.limits, q.arena)
	if err != nil {
		t.Fatal(err)
	}
	part, ok := m.part(3)
	if !ok {
		t.Fatal("fixture lost participant")
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	q.arena, err = graphstate.NewOutputBudget(1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.machine.crossFresh(&q, m, part, cfg); !errors.Is(err, errLimit) {
		t.Fatal("fresh claims exceeded shared output", err)
	}
	if _, err := newCrossPartitionMaterializer(nil, h.machine.ns, n.d, h.machine.limits); !errors.Is(err, errInvalid) {
		t.Fatal("nil store materializer admitted", err)
	}
}
