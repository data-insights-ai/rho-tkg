package graphapply

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type replacementLiveAssociation struct {
	peer        graphPeerIdentity
	incarnation [16]byte
	ctx         context.Context
	cancel      context.CancelFunc
	closes      atomic.Int32
	onClose     func() error
}

func (a *replacementLiveAssociation) Peer() graphPeerIdentity  { return a.peer }
func (a *replacementLiveAssociation) Incarnation() [16]byte    { return a.incarnation }
func (a *replacementLiveAssociation) Context() context.Context { return a.ctx }
func (a *replacementLiveAssociation) Close() error {
	a.closes.Add(1)
	a.cancel()
	if a.onClose != nil {
		return a.onClose()
	}
	return nil
}

func replacementSurvivors(t *testing.T, n *allocationTestNetwork, e allocationEvent) {
	t.Helper()
	queue := e.packets
	for count := 0; len(queue) != 0; count++ {
		if count > 4096 {
			t.Fatal("bounded surviving native transport")
		}
		p := queue[0]
		queue = queue[1:]
		if p.To == 3 {
			continue
		}
		next, err := n.hosts[0][p.To-1].Step(p)
		if err != nil {
			t.Fatal(err)
		}
		queue = append(queue, next.packets...)
	}
}

// Projection dependencies identify a local physical view. Import activates a
// different generation, so compare every logical field/predicate/version while
// checking and removing only that intentionally different local view identity.
func replacementLogicalProjection(t *testing.T, p graphstate.Projection) graphstate.Projection {
	t.Helper()
	if len(p.Dependencies) == 0 || p.Dependencies[0].View == (graphstate.ViewID{}) {
		t.Fatal("projection lacks a genuine local view identity")
	}
	view := p.Dependencies[0].View
	p.Dependencies = slices.Clone(p.Dependencies)
	for i := range p.Dependencies {
		if p.Dependencies[i].View != view {
			t.Fatal("projection mixed immutable local views", i)
		}
		p.Dependencies[i].View = graphstate.ViewID{}
	}
	return p
}

func TestGraphReplacementRevocationFencesHostAndSourceBeforeAsyncClose(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, server, info := certifiedSourceInfoFixture(t)
	h := n.hosts[0][0]
	ctx, cancel := context.WithCancel(t.Context())
	h.replacementCtx = ctx // Published immutable before any concurrent access.
	cancel()               // Deliberately omit async Close: exercise the synchronous fence.
	if h.ended.Load() || !h.issuerUnavailable() {
		t.Fatal("revocation depended on asynchronous issuer cleanup")
	}
	h.mu.Lock()
	blocked := h.unavailableLocked()
	h.mu.Unlock()
	if !blocked {
		t.Fatal("retained-source second capture check ignored revocation")
	}
	got, err := server.openCertified(t.Context(), info.certificate, [16]byte{80})
	assertCertifiedSourceInfoZero(t, got, err, replica.ErrStopped)
	// An already exported immutable certificate remains valid at another live
	// replica; replacement lifetime gates only new operations from its issuer.
	view, err := n.hosts[0][1].AtGraphCertificate(t.Context(), info.certificate)
	if err != nil {
		t.Fatal("revocation invalidated immutable exported certificate", err)
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}

	cross := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, cross)
	sources := []allocationProof{cross.read(0, allocationQuery{kind: observeConfiguration}), cross.read(1, allocationQuery{kind: observeConfiguration})}
	target := cross.hosts[0][0]
	if _, _, err := target.PrepareCrossPartitionGraphOperations(t.Context(), cross.requestID(), ops, 24, claims, sources); err != nil {
		t.Fatal("funded genuine live proof counterpart", err)
	}
	foreign := sources[1].owner
	foreignCtx, foreignCancel := context.WithCancel(t.Context())
	foreign.replacementCtx = foreignCtx
	foreignCancel()
	tx, proposal, err := target.PrepareCrossPartitionGraphOperations(t.Context(), cross.requestID(), ops, 24, claims, sources)
	if !errors.Is(err, errInvalid) || tx.owner != nil || len(tx.manifest)+len(proposal.wire) != 0 || foreign.ended.Load() {
		t.Fatal("foreign proof issuer revocation waited for async Close", tx.owner, err)
	}
}

func TestGraphReplacementActivationNativeHistoryAllocatorAndLifetime(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, name := range []string{"HOME", "non-HOME"} {
		t.Run(name, func(t *testing.T) { graphReplacementNativeWorkflow(t, name == "non-HOME", nil) })
	}
}

func TestGraphReplacementActivationAssociationCloseReentryCachesError(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	graphReplacementNativeWorkflow(t, false, errors.New("replacement association cleanup fixture"))
}

func TestGraphReplacementActivationRefusalsAndReceivingReopen(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, o := range []*graphReplacementReceiver{nil, {}} {
		if packets, err := o.step(replica.Packet{}); packets != nil || !errors.Is(err, errInvalid) {
			t.Fatal(packets, err)
		}
		if host, packets, err := o.activate(t.Context(), replica.Packet{}, nil); host != nil || packets != nil || !errors.Is(err, errInvalid) {
			t.Fatal(host, packets, err)
		}
		if err := o.close(); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
	n, g := replacementNativeNetwork(t, partitionWriteSemanticContractID())
	endpoint, _, _ := replacementEndpointFixture(t, n, 0)
	association := new(replacementLiveAssociation)
	acquire := replacementAssociatorFunc(func(_ context.Context, peer graphPeerIdentity) (graphReplacementAssociation, error) {
		if err := n.hosts[0][2].Close(); err != nil {
			return nil, err
		}
		association.peer, association.incarnation = peer, [16]byte{45}
		association.ctx, association.cancel = context.WithCancel(context.Background())
		return association, nil
	})
	a, err := beginGraphReplacement(t.Context(), endpoint, 3, acquire)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	association.onClose = func() error {
		if err := a.close(); err != nil {
			return err
		}
		return nil
	}
	for _, name := range []string{"nil-admission", "agreement", "voters", "local-voter", "identity", "semantic", "fixed-membership", "limits"} {
		t.Run(name, func(t *testing.T) {
			admission, genesis, limits := a, g, n.hosts[0][0].machine.limits
			c := lifecycleConfig(filepath.Join(t.TempDir(), "refused"), 3)
			c.Create = true
			switch name {
			case "nil-admission":
				admission = nil
			case "agreement":
				genesis.agreement = raftlog.ApplicationSemanticContractID{99}
			case "voters":
				genesis.voters = [3]uint64{1, 2, 4}
			case "local-voter":
				c.Application.LocalVoter = 2
			case "identity":
				c.Transfer.Identity.Graph = [16]byte{99}
			case "semantic":
				c.SemanticContractID = raftlog.ApplicationSemanticContractID{99}
			case "fixed-membership":
				c.Replication.Voters = [3]uint64{1, 2, 4}
			case "limits":
				limits.sourceRows = 0
			}
			if o, err := beginGraphReplacementReceiving(admission, c, genesis, limits); o != nil || !errors.Is(err, errInvalid) {
				t.Fatal(o, err)
			}
		})
	}
	c := lifecycleConfig(filepath.Join(t.TempDir(), "receiving"), 3)
	c.Create = true
	o, err := beginGraphReplacementReceiving(a, c, g, n.hosts[0][0].machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	defer o.close()
	duplicate := lifecycleConfig(filepath.Join(t.TempDir(), "duplicate"), 3)
	duplicate.Create = true
	if next, err := beginGraphReplacementReceiving(a, duplicate, g, o.limits); next != nil || !errors.Is(err, errInvalid) {
		t.Fatal("admission acquired two receiving owners", next, err)
	}
	if host, packets, err := o.activate(t.Context(), replica.Packet{}, nil); host != nil || packets != nil || !errors.Is(err, replica.ErrInvalid) {
		t.Fatal(host, packets, err)
	}
	if packets, err := o.step(replica.Packet{From: 2, To: 3}); packets != nil || !errors.Is(err, replica.ErrInvalid) {
		t.Fatal(packets, err)
	}
	if _, err := n.hosts[0][0].driver.ReportUnreachable(3); err != nil {
		t.Fatal(err)
	}
	n.hosts[0][0].mu.Lock()
	e, err := n.hosts[0][0].event(n.hosts[0][0].driver.Propose(nil))
	n.hosts[0][0].mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var original replica.Packet
	for _, p := range e.packets {
		if p.To == 3 {
			original = p
		}
	}
	replacementSurvivors(t, n, e)
	packets, err := o.step(original)
	if err != nil || len(packets) != 1 {
		t.Fatal("actual receiving term persistence", packets, err)
	}
	state, err := o.store.ApplicationReceivingState()
	if err != nil || state.Term != a.body.term {
		t.Fatal(state, err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	if err := o.close(); err != nil {
		t.Fatal(err)
	}
	if association.closes.Load() != 1 {
		t.Fatal("receiving owner did not close association once", association.closes.Load())
	}
	requestCtx, cancelRequest := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelRequest()
	if err := a.checkContext(requestCtx); !errors.Is(err, graphstore.ErrSourceUnavailable) || !errors.Is(err, context.Canceled) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("retired pending admission dropped owner or request cancellation cause", err)
	}
	if next, err := beginGraphReplacementReceiving(a, duplicate, g, o.limits); next != nil || !errors.Is(err, graphstore.ErrSourceUnavailable) {
		t.Fatal("restart accepted retired pending admission", next, err)
	}
	c.Create = false
	c.Transfer.Identity, c.SemanticContractID = state.Binding.Identity, state.Binding.SemanticContractID
	c.Replication.Voters = state.Voters
	s, err := raftlog.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if actual, err := s.ApplicationReceivingState(); err != nil || actual != state {
		t.Fatal("receiving restart lost known term/binding", actual, state, err)
	}
	if err := s.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("receiving restart reused old identity", err)
	}
}

func graphReplacementNativeWorkflow(t *testing.T, nonHome bool, callbackError error) {
	t.Helper()
	var n *allocationTestNetwork
	var g provisionedLogicalGraph
	group := 0
	if nonHome {
		n, g, _ = lifecycleNetwork(t)
		group = 1
	} else {
		n, g = replacementNativeNetwork(t, partitionWriteSemanticContractID())
	}
	home := n.hosts[0][0]
	source := n.hosts[group][0]
	handle := n.activate(group, [16]byte{77})
	cursor := n.acquire(group, handle, 1, 16)
	first, err := cursor.Next()
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := source.currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	axis := cfg.defaultAxis.Axis()
	scope, err := temporal.All(axis)
	if err != nil {
		t.Fatal(err)
	}
	one, two, edge := graphstate.EntityID(first), graphstate.EntityID(first+2), graphstate.EntityID(first+4)
	life1, life2, edgeLife := graphstate.LifeID(first+1), graphstate.LifeID(first+3), graphstate.LifeID(first+5)
	ops := []graphstate.Operation{
		{Kind: graphstate.CreateNode, Owner: one, Life: life1, Scope: scope, Record: graphstate.EntityRecord{ID: one, Kind: graphstate.Node, Axis: axis, Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}},
		{Kind: graphstate.CreateNode, Owner: two, Life: life2, Scope: scope, Record: graphstate.EntityRecord{ID: two, Kind: graphstate.Node, Axis: axis, Interpretation: graphstate.InterpretationState, TemporalRole: graphstate.TemporalRoleValidity}},
		{Kind: graphstate.CreateRelationship, Owner: edge, Life: edgeLife, Scope: scope, Record: graphstate.EntityRecord{ID: edge, Kind: graphstate.Relationship, Axis: axis, Type: "edge", Source: one, Target: two, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: life1, TargetLife: life2}},
		{Kind: graphstate.Set, Owner: one, Life: life1, Scope: scope, Name: "p", Value: graphstate.I64(10), ValueID: graphstate.ValueID(first + 6)},
		{Kind: graphstate.Set, Owner: edge, Life: edgeLife, Scope: scope, Name: "p", Value: graphstate.I64(30), ValueID: graphstate.ValueID(first + 7)},
	}
	claims := []freshBinding{{role: entityBinding, id: first}, {role: lifeBinding, owner: one, id: first + 1}, {role: entityBinding, id: first + 2}, {role: lifeBinding, owner: two, id: first + 3}, {role: entityBinding, id: first + 4}, {role: lifeBinding, owner: edge, id: first + 5}, {role: valueBinding, id: first + 6}, {role: valueBinding, id: first + 7}}
	for i := range claims {
		claims[i].grant = grantReference{session: handle.session, sequence: 1}
	}
	if nonHome {
		// The genuine lifecycle fixture routes canonical scalar identities to
		// HOME. Non-HOME lifecycle history needs no fabricated dictionary owner.
		ops, claims = ops[:3], claims[:6]
	}
	proposal, err := source.PreparePartitionGraphOperations(n.requestID(), ops, 20, claims)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(group, proposal, reasonNone)
	oldIndex := source.driver.Applied()
	oldNode, _ := partitionProjection(t, source, oldIndex, one, axis)
	oldRel, _ := partitionProjection(t, source, oldIndex, edge, axis)
	update := []graphstate.Operation{{Kind: graphstate.Set, Owner: one, Life: life1, Scope: scope, Name: "p", Value: graphstate.I64(20), ValueID: graphstate.ValueID(first + 8)}, {Kind: graphstate.Set, Owner: edge, Life: edgeLife, Scope: scope, Name: "p", Value: graphstate.I64(40), ValueID: graphstate.ValueID(first + 9)}}
	newClaims := []freshBinding{{role: valueBinding, id: first + 8, grant: grantReference{session: handle.session, sequence: 1}}, {role: valueBinding, id: first + 9, grant: grantReference{session: handle.session, sequence: 1}}}
	if nonHome {
		update = []graphstate.Operation{{Kind: graphstate.Close, Owner: one, Life: life1, Scope: scope}, {Kind: graphstate.Close, Owner: edge, Life: edgeLife, Scope: scope}}
		newClaims = nil
	}
	proposal, err = source.PreparePartitionGraphOperations(n.requestID(), update, 21, newClaims)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(group, proposal, reasonNone)
	currentIndex := source.driver.Applied()
	currentNode, _ := partitionProjection(t, source, currentIndex, one, axis)
	currentRel, _ := partitionProjection(t, source, currentIndex, edge, axis)
	if reflect.DeepEqual(oldNode, currentNode) || reflect.DeepEqual(oldRel, currentRel) {
		t.Fatal("fixture lacks divergent retained node/relationship history")
	}
	allocatorProof := n.read(0, allocationQuery{kind: observeAllocator})
	allocatorBefore := allocatorProof.observation.allocator
	if allocator, err := idalloc.Inspect(&allocatorBefore); err != nil || allocator.HighWater < first+15 {
		t.Fatal("actual allocator highwater absent", allocator, err)
	}
	if nonHome {
		// The existing replacement transport helper routes one selected partition.
		// Keep actual HOME untouched and select the initialized non-HOME group.
		n.hosts[0], n.configs[0] = n.hosts[1], n.configs[1]
	}

	endpoint, _, _ := replacementEndpointFixture(t, n, 0)
	association := &replacementLiveAssociation{}
	requestCtx, requestCancel := context.WithCancel(t.Context())
	defer requestCancel()
	acquire := replacementAssociatorFunc(func(_ context.Context, peer graphPeerIdentity) (graphReplacementAssociation, error) {
		if err := n.hosts[0][2].Close(); err != nil {
			return nil, err
		}
		association.peer, association.incarnation = peer, [16]byte{44}
		association.ctx, association.cancel = context.WithCancel(context.Background())
		return association, nil
	})
	a, err := beginGraphReplacement(requestCtx, endpoint, 3, acquire)
	if err != nil {
		t.Fatal(err)
	}
	defer a.close()
	receivingCfg := lifecycleConfig(filepath.Join(t.TempDir(), "replacement"), 3)
	receivingCfg.Create = true
	owner, err := beginGraphReplacementReceiving(a, receivingCfg, g, source.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	if _, err := source.driver.ReportUnreachable(3); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	out, err := source.event(source.driver.Propose(nil))
	source.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	var original replica.Packet
	for _, p := range out.packets {
		if p.To == 3 {
			original = p
		}
	}
	replacementSurvivors(t, n, out)
	if err := source.machine.store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	reject, err := owner.step(original)
	if err != nil || len(reject) != 1 {
		t.Fatal(reject, err)
	}
	e, err := source.Step(reject[0])
	if err != nil {
		t.Fatal(err)
	}
	replacementSurvivors(t, n, e)
	sends, err := source.driver.SnapshotSends()
	if err != nil || len(sends) != 1 {
		t.Fatal("real source snapshot owner absent", sends, err)
	}
	send := sends[0]
	ready := false
	for range 4096 {
		ready, err = send.Build(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("bounded real source snapshot build")
	}
	manifest, err := send.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	if err := source.driver.SealSnapshotSend(send); err != nil {
		t.Fatal(err)
	}
	source.mu.Lock()
	out, err = source.event(source.driver.Propose(nil))
	source.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	replacementSurvivors(t, n, out)
	var snapshot replica.Packet
	for _, p := range out.packets {
		if p.To == 3 && p.Snapshot {
			snapshot = p
		}
	}
	if len(snapshot.Payload) == 0 || len(out.snapshotSends) != 1 || out.snapshotSends[0] != send {
		t.Fatal("actual MsgSnap/owner pair absent")
	}
	imported, err := owner.store.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	for pages := 0; ; pages++ {
		if pages >= 4096 {
			t.Fatal("bounded genuine import")
		}
		chunk, err := send.Next(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			break
		}
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	// A caller may cancel only this activation operation while another genuine
	// endpoint operation owns the request gate. Pending association remains live.
	if err := endpoint.stream.mu.lock(t.Context()); err != nil {
		t.Fatal(err)
	}
	callCtx, cancelCall := context.WithCancel(t.Context())
	cancelCall()
	type canceledActivation struct {
		host    *allocationHost
		packets []replica.Packet
		err     error
	}
	canceled := make(chan canceledActivation, 1)
	go func() {
		h, packets, err := owner.activate(callCtx, snapshot, prepared)
		canceled <- canceledActivation{h, packets, err}
	}()
	select {
	case result := <-canceled:
		endpoint.stream.mu.unlock()
		if result.host != nil || result.packets != nil || !errors.Is(result.err, context.Canceled) {
			t.Fatal("cancelled activation escaped occupied endpoint gate", result)
		}
	case <-time.After(5 * time.Second):
		endpoint.stream.mu.unlock()
		t.Fatal("activation ignored per-call cancellation while endpoint gate occupied")
	}
	if state, err := owner.store.ApplicationReceivingState(); err != nil || state.Term != manifest.Term || association.Context().Err() != nil {
		t.Fatal("cancelled call activated/closed live association", state, err)
	}
	host, packets, err := owner.activate(t.Context(), snapshot, prepared)
	if err != nil || host == nil || len(packets) != 1 {
		t.Fatal("integrated replacement activation", host, packets, err)
	}
	n.hosts[0][2] = host
	defer host.Close()
	if err := a.close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(); err != nil {
		t.Fatal(err)
	}
	requestCancel()
	if association.closes.Load() != 0 || association.Context().Err() != nil {
		t.Fatal("pending cleanup/request cancellation closed transferred association")
	}
	if _, err := host.Tick(); err != nil {
		t.Fatal("Host did not retain independent incarnation lifetime", err)
	}
	for _, id := range []graphstate.EntityID{one, edge} {
		old, _ := partitionProjection(t, host, oldIndex, id, axis)
		current, _ := partitionProjection(t, host, currentIndex, id, axis)
		wantOld, wantCurrent := oldNode, currentNode
		if id == edge {
			wantOld, wantCurrent = oldRel, currentRel
		}
		if !reflect.DeepEqual(replacementLogicalProjection(t, old), replacementLogicalProjection(t, wantOld)) || !reflect.DeepEqual(replacementLogicalProjection(t, current), replacementLogicalProjection(t, wantCurrent)) {
			t.Fatalf("replacement substituted current/history for %d: old=%+v wantOld=%+v current=%+v wantCurrent=%+v", id, old, wantOld, current, wantCurrent)
		}
	}
	if nonHome {
		if _, err := host.machine.observeAllocation(allocationQuery{kind: observeAllocator, nonce: [16]byte{7}}, manifest.Index); !errors.Is(err, errInvalid) {
			t.Fatal("non-HOME replacement exposed an independent allocator", err)
		}
		view, err := host.machine.store.ApplicationView(manifest.Index)
		if err != nil {
			t.Fatal(err)
		}
		_, found, err := view.Get(t.Context(), allocatorKey(host.machine.ns), 4096)
		closeErr := view.Close()
		if err != nil || closeErr != nil || found {
			t.Fatal("non-HOME import gained allocator record", found, err, closeErr)
		}
		observation, err := home.machine.observeAllocation(allocationQuery{kind: observeAllocator, nonce: [16]byte{7}}, home.driver.Applied())
		if err != nil || !reflect.DeepEqual(observation.allocator, allocatorBefore) {
			t.Fatal("non-HOME replacement mutated HOME allocator", err)
		}
	} else {
		observation, err := host.machine.observeAllocation(allocationQuery{kind: observeAllocator, nonce: [16]byte{7}}, manifest.Index)
		if err != nil || !reflect.DeepEqual(observation.allocator, allocatorBefore) {
			t.Fatal("replacement reset nonzero allocator checkpoint", err)
		}
	}
	actualCfg, err := host.currentConfiguration()
	if err != nil || actualCfg.defaultAxis != cfg.defaultAxis {
		t.Fatal("default axis lost", err)
	}
	if _, err := source.driver.ReportSnapshotSend(send, true); err != nil {
		t.Fatal(err)
	}
	for _, p := range packets {
		e, err := source.Step(p)
		if err != nil {
			t.Fatal(err)
		}
		n.pump(0, e)
	}
	reopenedCfg := receivingCfg
	reopenedCfg.Create = false
	binding := host.machine.store.ApplicationBinding()
	reopenedCfg.Transfer.Identity, reopenedCfg.SemanticContractID = binding.Identity, binding.SemanticContractID
	reopenedCfg.Replication = raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}
	reentered := make(chan error, 1)
	association.onClose = func() error {
		err := host.Close()
		if _, _, storeErr := host.machine.store.InitialState(); !errors.Is(storeErr, raftlog.ErrClosed) {
			err = errors.Join(err, errors.New("association Close reentered before native Store cleanup completed"))
		}
		reentered <- err
		return callbackError
	}
	if callbackError != nil {
		if err := host.Close(); !errors.Is(err, callbackError) {
			t.Fatal("association cleanup error not returned", err)
		}
	} else {
		association.cancel()
	}
	if _, err := host.Tick(); !errors.Is(err, replica.ErrStopped) {
		t.Fatal("revoked association did not immediately fence Host", err)
	}
	if err := host.Close(); callbackError == nil && err != nil || callbackError != nil && !errors.Is(err, callbackError) {
		t.Fatal(err)
	}
	select {
	case err := <-reentered:
		if err != nil {
			t.Fatal("association Close could not reenter completed native Host", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("association Close reentered Host.Close and deadlocked")
	}
	if association.closes.Load() != 1 {
		t.Fatal("transferred association not released once", association.closes.Load())
	}
	retained, err := raftlog.Open(reopenedCfg)
	if err != nil {
		t.Fatal("activated replacement failed retained reopen", err)
	}
	defer retained.Close()
	restored, err := newDeclaredMaterializerWithAgreement(retained, host.machine.ns, host.machine.declaration, host.machine.limits, host.machine.SemanticContractID())
	if err != nil {
		t.Fatal("activated graph retained-state Restore failed", err)
	}
	readOnly := &allocationHost{machine: restored}
	for _, item := range []struct {
		id           graphstate.EntityID
		old, current graphstate.Projection
	}{{one, oldNode, currentNode}, {edge, oldRel, currentRel}} {
		old, _ := partitionProjection(t, readOnly, oldIndex, item.id, axis)
		current, _ := partitionProjection(t, readOnly, currentIndex, item.id, axis)
		if !reflect.DeepEqual(replacementLogicalProjection(t, old), replacementLogicalProjection(t, item.old)) || !reflect.DeepEqual(replacementLogicalProjection(t, current), replacementLogicalProjection(t, item.current)) {
			t.Fatalf("retained replacement reopen changed history for %d: old=%+v expected=%+v current=%+v expected=%+v", item.id, old, item.old, current, item.current)
		}
	}
	if err := retained.Initialize([]uint64{1, 2, 3}, nil); !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal("activated replacement identity reinitialized", err)
	}
}
