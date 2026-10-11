package graphapply

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func newCrossPartitionNetwork(t *testing.T) *allocationTestNetwork {
	return newCrossPartitionNetworkWithPolicy(t, nil)
}
func newCrossPartitionNetworkWithPolicy(t *testing.T, adjust func(*raftlog.ApplicationPolicy)) *allocationTestNetwork {
	t.Helper()
	r := partitionCodecRequest(t, 1, defaultMaterializerLimits())
	n := &allocationTestNetwork{t: t, d: r.declaration, pending: make(map[[16]byte]allocationProof)}
	for group, partition := range []uint64{3, 8} {
		for member := range 3 {
			id := uint64(member + 1)
			p := raftlog.DefaultApplicationPolicy(id)
			p.MaxImageBytes = 172
			p.MaxInstallWrites = 256
			p.RetainedApplicationRecords = 8192
			p.RetainedApplicationBytes = 16 << 20
			if adjust != nil {
				adjust(&p)
			}
			controls := raftlog.ApplicationControlConfig{Version: 1}
			recovery := raftlog.ApplicationRecoveryConfig{Version: 1}
			contract, err := raftlog.ApplicationContractForPolicyWithRecovery(p, controls, recovery)
			if err != nil {
				t.Fatal(err)
			}
			part, _ := n.d.Partition(partition)
			cfg := raftlog.Config{Dir: filepath.Join(t.TempDir(), fmt.Sprintf("partition%d-member%d", partition, id)), Create: true, Application: p, Controls: controls, Recovery: recovery, Transfer: raftlog.ApplicationTransferConfig{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(n.d.Graph()), Partition: partition, Group: part.Group}, Contract: contract, Limits: raftlog.DefaultApplicationTransferLimits()}, Generations: raftlog.ApplicationGenerationLimits{MaxBytes: 32 << 20, MaxRecords: 16384}, PublishedCuts: raftlog.ApplicationPublishedCutLimits{MaxTransferChunks: 4096}, Replication: raftlog.ApplicationReplicationConfig{Voters: [3]uint64{1, 2, 3}}, SemanticContractID: crossPartitionSemanticContractID()}
			n.configs[group][member] = cfg
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Error(err)
				}
			})
			if err := graphstore.BootstrapBoundOwnership(s, s.ApplicationBinding(), n.d, [3]uint64{1, 2, 3}); err != nil {
				t.Fatal(err)
			}
			m, err := newCrossPartitionMaterializer(s, namespace{idalloc.GraphID(n.d.Graph()), partition}, n.d, defaultMaterializerLimits())
			if err != nil {
				t.Fatal(err)
			}
			h, err := openAllocationHost(m, id, replica.Config{Store: s, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			n.hosts[group][member] = h
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		event, err := n.hosts[group][0].Campaign()
		if err != nil {
			t.Fatal(err)
		}
		n.pump(group, event)
	}
	h := n.hosts[0][0]
	p, err := h.PreparePartitionInitialization(3, bootstrapAttemptID{1}, partitionWriteSchemas(), 16, graphGenesisBinding(t), *r.routing, allocationProof{})
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, p)
	proof := n.read(0, allocationQuery{kind: observeConfiguration})
	p, err = h.PreparePartitionInitialization(8, bootstrapAttemptID{2}, partitionWriteSchemas(), 16, graphGenesisBinding(t), *r.routing, proof)
	if err != nil {
		t.Fatal(err)
	}
	n.submit(1, p)
	return n
}
func (n *allocationTestNetwork) crossPump(group int, event allocationEvent) []crossReply {
	n.t.Helper()
	queue := event.packets
	replies := event.crossReplies
	for steps := 0; len(queue) > 0; steps++ {
		if steps >= 1024 {
			n.t.Fatal("cross packet schedule exceeded")
		}
		p := queue[0]
		queue = queue[1:]
		if p.Snapshot || p.To < 1 || p.To > 3 {
			n.t.Fatal("unexpected cross transport")
		}
		e, err := n.hosts[group][p.To-1].Step(p)
		if err != nil {
			n.t.Fatal(err)
		}
		if len(queue)+len(e.packets) > 128 || len(replies)+len(e.crossReplies) > 16 {
			n.t.Fatal("cross transport budget")
		}
		queue = append(queue, e.packets...)
		replies = append(replies, e.crossReplies...)
	}
	return replies
}
func (n *allocationTestNetwork) crossRead(group int, tx [16]byte) crossProof {
	return n.crossReadPhase(group, tx, 0)
}
func (n *allocationTestNetwork) crossReadPhase(group int, tx [16]byte, phase crossPhase) crossProof {
	n.t.Helper()
	nonce := [16]byte(n.requestID())
	event, err := n.hosts[group][0].ReadCrossPhase(tx, phase, nonce)
	if err != nil {
		n.t.Fatal(err)
	}
	replies := n.crossPump(group, event)
	if len(replies) != 1 || replies[0].err != nil {
		n.t.Fatal("cross completed read", replies)
	}
	return replies[0].proof
}
func (n *allocationTestNetwork) crossSubmit(group int, p allocationProposal) crossStatus {
	n.t.Helper()
	event, err := n.hosts[group][0].Submit(p)
	if err != nil {
		n.t.Fatal(err)
	}
	n.crossPump(group, event)
	var out crossStatus
	for member, h := range n.hosts[group] {
		wire, err := h.machine.store.ApplicationRecord(n.t.Context(), h.driver.Applied(), true, 256)
		if err != nil {
			n.t.Fatal(err)
		}
		s, err := decodeCrossStatus(wire)
		if err != nil {
			n.t.Fatal(err)
		}
		if member == 0 {
			out = s
		} else if s != out {
			n.t.Fatal("cross voter result diverged", out, s)
		}
	}
	return out
}
func (n *allocationTestNetwork) crossPlan(ops []graphstate.Operation, claims []freshBinding, provenance uint64) (crossTransaction, allocationProposal) {
	n.t.Helper()
	sources := []allocationProof{n.read(0, allocationQuery{kind: observeConfiguration}), n.read(1, allocationQuery{kind: observeConfiguration})}
	tx, p, err := n.hosts[0][0].PrepareCrossPartitionGraphOperations(n.t.Context(), n.requestID(), ops, provenance, claims, sources)
	if err != nil {
		n.t.Fatal("typed cross plan", err)
	}
	return tx, p
}
func (n *allocationTestNetwork) crossCommit(tx crossTransaction, register allocationProposal) crossProof {
	n.t.Helper()
	n.crossSubmit(0, register)
	id := [16]byte{}
	copy(id[:], tx.manifest[4:20])
	registered := n.crossRead(0, id)
	votes := []crossProof{}
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[0][0].PrepareCrossParticipant(tx, partition, registered, votes)
		if err != nil {
			n.t.Fatal(err)
		}
		vote := n.crossSubmit(group, p)
		if !vote.yes {
			n.t.Fatal("cross prepare NO", vote)
		}
		votes = append(votes, n.crossRead(group, id))
	}
	p, err := n.hosts[0][0].DecideCrossTransaction(tx, votes)
	if err != nil {
		n.t.Fatal(err)
	}
	decision := n.crossSubmit(0, p)
	if !decision.commit {
		n.t.Fatal("cross unexpectedly aborted", decision)
	}
	return n.crossRead(0, id)
}
func (n *allocationTestNetwork) crossResolve(decision crossProof) {
	n.t.Helper()
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[0][0].ResolveCrossParticipant(partition, decision)
		if err != nil {
			n.t.Fatal(err)
		}
		got := n.crossSubmit(group, p)
		if got.phase != crossResolve || got.commit != decision.status.commit || got.round != decision.status.round {
			n.t.Fatal(got, decision.status)
		}
	}
}

func crossEdgeFixture(t *testing.T, n *allocationTestNetwork) ([]graphstate.Operation, []freshBinding) {
	t.Helper()
	handles := []*allocationRecipientHandle{n.activate(0, [16]byte{1}), n.activate(1, [16]byte{2})}
	for g, h := range handles {
		n.acquire(g, h, 1, 16)
	}
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	for group, owner := range []graphstate.EntityID{1, 17} {
		op := graphstate.Operation{Kind: graphstate.CreateNode, Owner: owner, Life: graphstate.LifeID(owner + 1), Scope: scope, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}
		claim := []freshBinding{{role: entityBinding, id: uint64(owner), grant: grantReference{session: handles[group].session, sequence: 1}}, {role: lifeBinding, owner: owner, id: uint64(owner + 1), grant: grantReference{session: handles[group].session, sequence: 1}}}
		p, err := n.hosts[group][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{op}, 23, claim)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(group, p, reasonNone)
	}
	op := graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: scope, Record: graphstate.EntityRecord{Type: "cross-edge", Source: 1, Target: 17, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}
	claims := []freshBinding{{role: entityBinding, id: 3, grant: grantReference{session: handles[0].session, sequence: 1}}, {role: lifeBinding, owner: 3, id: 4, grant: grantReference{session: handles[0].session, sequence: 1}}}
	return []graphstate.Operation{op}, claims
}
func crossView(t *testing.T, n *allocationTestNetwork, member int, indexes [2]uint64) (*graphstore.PartitionTransactionView, func()) {
	t.Helper()
	views := make([]*raftlog.ApplicationView, 2)
	for group, h := range []*allocationHost{n.hosts[0][member], n.hosts[1][member]} {
		index := indexes[group]
		if index == 0 {
			index = h.driver.Applied()
		}
		v, err := h.machine.store.ApplicationView(index)
		if err != nil {
			t.Fatal(err)
		}
		views[group] = v
	}
	cfg, err := n.hosts[0][member].currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	digest, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	l := n.hosts[0][member].machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	v, _, err := graphstore.OpenPartitionTransactionView(t.Context(), views, n.d, cfg.defaultAxis, digest, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: l.outputBytes}, arena)
	if err != nil {
		t.Fatal(err)
	}
	return v, func() {
		if err := v.Close(); err != nil {
			t.Error(err)
		}
		for _, view := range views {
			if err := view.Close(); err != nil {
				t.Error(err)
			}
		}
	}
}

func testCrossPartitionHostTypedCrossEdgeCommitsBothEndpointPostings(t *testing.T) {
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, p := n.crossPlan(ops, claims, 24)
	decision := n.crossCommit(tx, p)
	// Durable YES is private: neither primary nor posting becomes graph state.
	for member := range 3 {
		v, close := crossView(t, n, member, [2]uint64{})
		r, err := v.Entity(t.Context(), 3)
		close()
		if err != nil || r.Found {
			t.Fatal("prepared intent visible", r, err)
		}
	}
	n.crossResolve(decision)
	installed := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	for member := range 3 {
		v, close := crossView(t, n, member, [2]uint64{})
		axis := graphGenesisBinding(t).Axis()
		point, err := temporal.RationalPosition(axis, temporal.RationalInt64(5))
		if err != nil {
			t.Fatal(err)
		}
		projection, err := graphstate.Project(t.Context(), v, 3, point, graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		if err != nil || !projection.Exists || !projection.Active || projection.Record.Interpretation != ops[0].Record.Interpretation || projection.Record.TemporalRole != ops[0].Record.TemporalRole || projection.Record.Source != 1 || projection.Record.Target != 17 || projection.Life != 4 {
			t.Fatal("typed cross-edge", projection, err)
		}
		for _, endpoint := range []struct {
			owner graphstate.EntityID
			life  graphstate.LifeID
		}{{1, 2}, {17, 18}} {
			page, err := v.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: endpoint.owner, Life: endpoint.life, Window: ops[0].Scope}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
			if err != nil || len(page.Entities) != 1 || page.Entities[0] != 3 {
				t.Fatal("complete endpoint posting", endpoint, page, err)
			}
		}
		close()
	}
	// Mutation followed by retained/current exact-set answers catches a current
	// state implementation substituted for history or one missing endpoint side.
	closeNode := []graphstate.Operation{{Kind: graphstate.Close, Owner: 17, Life: 18, Scope: ops[0].Scope}}
	next, nextRegister := n.crossPlan(closeNode, nil, 25)
	nextDecision := n.crossCommit(next, nextRegister)
	n.crossResolve(nextDecision)
	for member := range 3 {
		for _, snapshot := range []struct {
			indexes [2]uint64
			active  bool
		}{{installed, true}, {[2]uint64{}, false}} {
			v, close := crossView(t, n, member, snapshot.indexes)
			point, _ := temporal.RationalPosition(graphGenesisBinding(t).Axis(), temporal.RationalInt64(5))
			r, err := graphstate.Project(t.Context(), v, 3, point, graphstate.Effective, defaultMaterializerLimits().graph.Planner)
			close()
			if err != nil || r.Active != snapshot.active {
				t.Fatal("retained cross history", r, err)
			}
		}
	}
	// Exact immutable retry does not reapply changes or advance a graph round.
	before := decision.status
	n.crossSubmit(0, p)
	after := n.crossRead(0, decision.status.tx)
	if after.status.round != before.round || after.status.digest != before.digest {
		t.Fatal("retry rewrote decision", after.status, before)
	}
}

func TestCrossPartitionNilAndPrivateProofRefusals(t *testing.T) {
	var h *allocationHost
	if _, err := h.ReadCrossTransaction([16]byte{1}, [16]byte{2}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, _, err := h.PrepareCrossPartitionGraphOperations(t.Context(), requestID{1}, nil, 1, nil, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.PrepareCrossParticipant(crossTransaction{}, 3, crossProof{}, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.DecideCrossTransaction(crossTransaction{}, nil); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.ResolveCrossParticipant(3, crossProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.ReadCrossPhase([16]byte{1}, crossPrepare, [16]byte{2}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
	if _, err := h.RecoverCrossTransaction(crossProof{}); !errors.Is(err, errInvalid) {
		t.Fatal(err)
	}
}

func TestCrossPartitionPendingVoteIsNotAbortAndStaleSourceDurablyAborts(t *testing.T) {
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, p := n.crossPlan(ops, claims, 24)
	n.crossSubmit(0, p)
	id := [16]byte{}
	copy(id[:], tx.manifest[4:20])
	registered := n.crossReadPhase(0, id, crossRegister)
	h := n.hosts[0][0]
	bad := registered
	bad.question.nonce = [16]byte{}
	if _, err := h.PrepareCrossParticipant(tx, 3, bad, nil); !errors.Is(err, errInvalid) {
		t.Fatal("unbound nonce", err)
	}
	if _, err := h.PrepareCrossParticipant(tx, 8, registered, nil); !errors.Is(err, errInvalid) {
		t.Fatal("declaration order bypass", err)
	}
	p, err := h.PrepareCrossParticipant(tx, 3, registered, nil)
	if err != nil {
		t.Fatal(err)
	}
	yes := n.crossSubmit(0, p)
	if !yes.yes {
		t.Fatal(yes)
	}
	vote := n.crossReadPhase(0, id, crossPrepare)
	if _, err := h.DecideCrossTransaction(tx, []crossProof{vote}); !errors.Is(err, replica.ErrUnavailable) {
		t.Fatal("missing vote became abort", err)
	}
	current := n.crossRead(0, id)
	if current.status.phase != crossPrepare || !current.status.yes {
		t.Fatal("pending prepare lost", current.status)
	}
	// The local door consults the same durable coarse lock, even with an unrelated
	// request key. No graph/root/round state changes on its committed refusal.
	local, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: "locked", Scope: ops[0].Scope}}, 25, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(0, local, reasonPreparedConflict)
	// A write at an unlocked remote authority invalidates the saved semantic base.
	remote, err := n.hosts[1][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Name: "changed", Scope: ops[0].Scope}}, 25, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(1, remote, reasonNone)
	p, err = h.PrepareCrossParticipant(tx, 8, registered, []crossProof{vote})
	if err != nil {
		t.Fatal(err)
	}
	no := n.crossSubmit(1, p)
	if no.yes || no.why != reasonReadConflict {
		t.Fatal("stale semantic root voted YES", no)
	}
	negative := n.crossReadPhase(1, id, crossPrepare)
	p, err = h.DecideCrossTransaction(tx, []crossProof{vote, negative})
	if err != nil {
		t.Fatal(err)
	}
	aborted := n.crossSubmit(0, p)
	if aborted.commit || aborted.round != 0 {
		t.Fatal("NO not immutable ABORT", aborted)
	}
	decision := n.crossReadPhase(0, id, crossDecide)
	n.crossResolve(decision)
	for member := range 3 {
		v, close := crossView(t, n, member, [2]uint64{})
		edge, err := v.Entity(t.Context(), 3)
		if err != nil || edge.Found {
			t.Fatal("aborted edge installed", edge, err)
		}
		point, _ := temporal.RationalPosition(graphGenesisBinding(t).Axis(), temporal.RationalInt64(5))
		node, err := graphstate.Project(t.Context(), v, 17, point, graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		close()
		if err != nil || len(node.Labels) != 1 || node.Labels[0] != "changed" {
			t.Fatal("abort erased independent mutation", node, err)
		}
	}
	if got := n.crossSubmit(0, p); got != aborted {
		t.Fatal("decision retry changed abort", got, aborted)
	}
}

func TestCrossPartitionCompleteTypedCDCAndSequentialBeforeUseCommitRound(t *testing.T) {
	n := newCrossPartitionNetwork(t)
	local := n.activate(0, [16]byte{1})
	n.acquire(0, local, 1, 16)
	remote := n.activate(1, [16]byte{2})
	n.acquire(1, remote, 1, 16)
	ops, claims := partitionNineOperations(t, 1)
	ops[1].Owner = 17
	ops[1].Life = 18
	ops[1].Record.ID = 17
	for i := range ops {
		if ops[i].Record.Target == 3 {
			ops[i].Record.Target = 17
		}
		if ops[i].Binding.TargetLife == 4 {
			ops[i].Binding.TargetLife = 18
		}
	}
	ops[7].Owner = 17
	ops[7].Life = 18
	for i := range claims {
		claims[i].grant = grantReference{session: local.session, sequence: 1}
		if claims[i].role == entityBinding && claims[i].id == 3 {
			claims[i].id = 17
			claims[i].grant.session = remote.session
		}
		if claims[i].role == lifeBinding && claims[i].owner == 3 {
			claims[i].owner = 17
			claims[i].id = 18
			claims[i].grant.session = remote.session
		}
	}
	d, err := temporal.PreserveOpaqueDescriptor(temporal.OpaqueDescriptorSpec{ID: temporal.DescriptorID{8}, Type: "calendar", SchemaVersion: 999, Payload: []byte("second opaque observation")}, defaultMaterializerLimits().catalog.Temporal)
	if err != nil {
		t.Fatal(err)
	}
	second, err := graphstate.DescriptorValue(d)
	if err != nil {
		t.Fatal(err)
	}
	ops = append(ops, graphstate.Operation{Kind: graphstate.Set, Owner: 1, Life: 2, Name: "descriptor", Value: second, ValueID: 13, Scope: ops[0].Scope})
	claims = append(claims, freshBinding{role: valueBinding, id: 13, grant: grantReference{session: local.session, sequence: 1}})
	tx, p := n.crossPlan(ops, claims, 91)
	decision := n.crossCommit(tx, p)
	n.crossResolve(decision)
	for member := range 3 {
		entities, lives, values, groups := 0, 0, 0, 0
		for group, h := range []*allocationHost{n.hosts[0][member], n.hosts[1][member]} {
			status, _, err := h.machine.observeCrossTransaction(crossQuery{tx: decision.status.tx}, h.driver.Applied())
			if err != nil {
				t.Fatal(err)
			}
			out := outcome{ns: h.machine.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
			wire, err := h.machine.store.ApplicationRecord(t.Context(), status.index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
			if err != nil {
				t.Fatal(err)
			}
			cdc, err := decodeTypedChangeEnvelope(wire, out, h.machine.limits, true)
			if err != nil {
				t.Fatal("typed cross CDC", err)
			}
			entities += len(cdc.entities)
			lives += len(cdc.lives)
			values += len(cdc.values)
			groups += len(cdc.groups)
			for _, record := range cdc.entities {
				if record.Interpretation == 0 || record.TemporalRole == 0 || record.Axis.Descriptor() != graphGenesisBinding(t).Axis().Descriptor() {
					t.Fatal("entity metadata lost", record)
				}
			}
			for _, component := range cdc.groups {
				for _, change := range component.Changes {
					if change.After().Revision().ID() != decision.status.round || change.After().Revision().Provenance() != 91 {
						t.Fatal("CDC provisional round leaked", group, change)
					}
					if component.Key.Owner == 1 && component.Key.Name == "descriptor" && change.Before().Present() {
						if change.Before().Revision().ID() != decision.status.round || change.Before().Value().ID() != 11 || change.After().Value().ID() != 13 {
							t.Fatal("new sequential Before not rebound", change)
						}
					}
				}
			}
		}
		if entities != 5 || lives != 5 || values != 3 || groups != 10 {
			t.Fatal("partial typed CDC", entities, lives, values, groups)
		}
	}
}

func crossReopenAll(t *testing.T, n *allocationTestNetwork) {
	t.Helper()
	for group := range 2 {
		for member := range 3 {
			if err := n.hosts[group][member].Close(); err != nil {
				t.Fatal(err)
			}
			cfg := n.configs[group][member]
			cfg.Create = false
			s, err := raftlog.Open(cfg)
			if err != nil {
				t.Fatal("durable reopen", err)
			}
			part := cfg.Transfer.Identity.Partition
			m, err := newCrossPartitionMaterializer(s, namespace{idalloc.GraphID(n.d.Graph()), part}, n.d, defaultMaterializerLimits())
			if err != nil {
				t.Fatal("prepared graph restore", err)
			}
			h, err := openAllocationHost(m, uint64(member+1), replica.Config{Store: s, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
			if err != nil {
				t.Fatal(err)
			}
			n.hosts[group][member] = h
			t.Cleanup(func() {
				if err := h.Close(); err != nil {
					t.Error(err)
				}
			})
		}
		event, err := n.hosts[group][0].Campaign()
		if err != nil {
			t.Fatal(err)
		}
		n.crossPump(group, event)
	}
}

func TestCrossPartitionPreparedLocksAndOriginalTemplateSurviveLeaderLossReopen(t *testing.T) {
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	tx, p := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, p)
	id := registered.tx
	registration := n.crossReadPhase(0, id, crossRegister)
	votes := []crossProof{}
	for group, partition := range []uint64{3, 8} {
		p, err := n.hosts[0][0].PrepareCrossParticipant(tx, partition, registration, votes)
		if err != nil {
			t.Fatal(err)
		}
		yes := n.crossSubmit(group, p)
		if !yes.yes {
			t.Fatal(yes)
		}
		votes = append(votes, n.crossReadPhase(group, id, crossPrepare))
	}
	original := tx.digest
	crossReopenAll(t, n)
	for group, h := range []*allocationHost{n.hosts[0][0], n.hosts[1][0]} {
		owner := graphstate.EntityID(1)
		life := graphstate.LifeID(2)
		if group == 1 {
			owner = 17
			life = 18
		}
		local, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: owner, Life: life, Name: "after-reopen", Scope: ops[0].Scope}}, 25, nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(group, local, reasonPreparedConflict)
	}
	registration = n.crossReadPhase(0, id, crossRegister)
	recovered, err := n.hosts[0][0].RecoverCrossTransaction(registration)
	if err != nil || recovered.digest != original {
		t.Fatal("original template recovery", recovered.digest, original, err)
	}
	votes = []crossProof{n.crossReadPhase(0, id, crossPrepare), n.crossReadPhase(1, id, crossPrepare)}
	p, err = n.hosts[0][0].DecideCrossTransaction(recovered, votes)
	if err != nil {
		t.Fatal(err)
	}
	committed := n.crossSubmit(0, p)
	if !committed.commit {
		t.Fatal(committed)
	}
	// Another leader loss after the immutable decision must not re-plan against
	// a current graph or lose the still-locked participant kernel.
	crossReopenAll(t, n)
	decision := n.crossReadPhase(0, id, crossDecide)
	n.crossResolve(decision)
	for member := range 3 {
		v, close := crossView(t, n, member, [2]uint64{})
		point, _ := temporal.RationalPosition(graphGenesisBinding(t).Axis(), temporal.RationalInt64(5))
		edge, err := graphstate.Project(t.Context(), v, 3, point, graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		close()
		if err != nil || !edge.Active || edge.Record.Target != 17 {
			t.Fatal("resolved original cross template", edge, err)
		}
	}
}

func TestCrossPartitionActualGrantRefusalHasDurableNOAndImmutableAbort(t *testing.T) {
	n := newCrossPartitionNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	claims[0].grant.sequence = 2
	tx, p := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, p)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	p, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	no := n.crossSubmit(0, p)
	if no.yes || no.why != reasonInvalid {
		t.Fatal("unissued grant voted YES", no)
	}
	vote := n.crossReadPhase(0, no.tx, crossPrepare)
	p, err = n.hosts[0][0].DecideCrossTransaction(tx, []crossProof{vote})
	if err != nil {
		t.Fatal(err)
	}
	aborted := n.crossSubmit(0, p)
	if aborted.commit {
		t.Fatal(aborted)
	}
	decision := n.crossReadPhase(0, no.tx, crossDecide)
	resolve, err := n.hosts[0][0].ResolveCrossParticipant(3, decision)
	if err != nil {
		t.Fatal(err)
	}
	n.crossSubmit(0, resolve)
	view, err := n.hosts[0][0].machine.store.ApplicationView(n.hosts[0][0].driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer view.Close()
	_, found, _, err := view.GetControl(t.Context(), crossControlKey(n.hosts[0][0].machine.ns, no.tx, 'K'), raftlog.ReadBudget{Rows: 2, Bytes: 4096})
	if err != nil || found {
		t.Fatal("NO manufactured kernel authority", found, err)
	}
}
