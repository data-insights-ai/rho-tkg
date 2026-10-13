package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"reflect"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"go.etcd.io/raft/v3"
)

// Only the reply is lost, after the actual service has committed the command.
// Unlike the range-only fault wire this accepts both production protocol kinds.
type p07LostDecisionWire struct {
	graphPeerWireTransport
	limits        materializerLimits
	drop, dropped bool
}

func (w *p07LostDecisionWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	lose := false
	if w.drop && request.kind == sourceSubmit {
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil {
			return nil, err
		}
		if isCrossPartitionWire(proposal.wire) {
			command, _, err := decodeCrossCommand(proposal.wire, w.limits)
			if err != nil {
				return nil, err
			}
			lose = command.phase == crossDecide
		}
	}
	reply, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err != nil {
		return nil, err
	}
	if lose {
		w.drop, w.dropped = false, true
		return nil, graphstore.ErrSourceUnavailable
	}
	return reply, nil
}

type p07RetainedRecord struct {
	index            uint64
	outcome, changes []byte
}

// These are internal source-qualified cursor coordinates from actual coverage,
// not a portable feed lease or a scalar globally comparable cut constructor.
func p07CutIndexes(t *testing.T, cut graphCut) [2]uint64 {
	t.Helper()
	if len(cut.certificates) != 2 {
		t.Fatal("missing complete genuine cut coverage")
	}
	var indexes [2]uint64
	for _, certificate := range cut.certificates {
		group := 0
		if certificate.scope.partition == 8 {
			group = 1
		} else if certificate.scope.partition != 3 {
			t.Fatal("foreign certified feed source")
		}
		if indexes[group] != 0 || certificate.sourceIndex == 0 {
			t.Fatal("duplicate/empty actual cut coordinate")
		}
		indexes[group] = certificate.sourceIndex
	}
	return indexes
}

func p07ReadRetainedPrefix(t *testing.T, n *allocationTestNetwork, starts, ends [2]uint64) [2][]p07RetainedRecord {
	t.Helper()
	var result [2][]p07RetainedRecord
	owned := 0
	for group := range 2 {
		if ends[group] < starts[group] || ends[group]-starts[group] > 256 {
			t.Fatal("bounded retained CDC prefix", starts, ends)
		}
		h := n.hosts[group][0]
		for index := starts[group] + 1; index <= ends[group]; index++ {
			outcome, err := h.machine.store.ApplicationRecord(t.Context(), index, true, h.machine.store.ApplicationLimits().MaxOutcomeBytes)
			if err != nil || outcome == nil {
				t.Fatal("retained outcome gap, including empty control entries", group, index, err)
			}
			changes, err := h.machine.store.ApplicationRecord(t.Context(), index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
			if err != nil || changes == nil {
				t.Fatal("retained CDC gap, including empty control entries", group, index, err)
			}
			owned += len(outcome) + len(changes)
			if owned > 4<<20 {
				t.Fatal("bounded internal feed ownership")
			}
			result[group] = append(result[group], p07RetainedRecord{index, outcome, changes})
		}
	}
	return result
}

func p07ResolvedParts(t *testing.T, n *allocationTestNetwork, records [2][]p07RetainedRecord, decision crossStatus) [2]bool {
	t.Helper()
	var seen [2]bool
	for group, entries := range records {
		for _, entry := range entries {
			status, err := decodeCrossStatus(entry.outcome)
			if err != nil || status.tx != decision.tx || status.phase != crossResolve {
				continue
			}
			if status.ns != n.hosts[group][0].machine.ns || status.digest != decision.digest || !status.commit || status.round != decision.round || status.index > entry.index {
				t.Fatal("feed lost original TxID/participant/decision binding", group, status)
			}
			if seen[group] && len(entry.changes) != 0 {
				t.Fatal("original Resolve replay emitted duplicate business CDC")
			}
			seen[group] = true
		}
	}
	return seen
}

func p07AssertGraphAt(t *testing.T, view graphstate.ReadView, old, afterMove bool, window temporal.Scope) {
	t.Helper()
	var nodes, relationships, activeNodes, activeRelationships []graphstate.EntityID
	for id := graphstate.EntityID(1); id <= 32; id++ {
		record, err := view.Entity(t.Context(), id)
		if err != nil {
			t.Fatal("complete allocated-namespace entity census", id, err)
		}
		if !record.Found {
			continue
		}
		projection, err := graphstate.Project(t.Context(), view, id, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		if err != nil || !projection.Exists || projection.Record.ID != id {
			t.Fatal("same-view projection", id, err)
		}
		switch record.Record.Kind {
		case graphstate.Node:
			nodes = append(nodes, id)
			if projection.Active {
				activeNodes = append(activeNodes, id)
			}
		case graphstate.Relationship:
			relationships = append(relationships, id)
			if projection.Active {
				activeRelationships = append(activeRelationships, id)
			}
		default:
			t.Fatal("unexpected graph record kind", id)
		}
	}
	wantRelationships, wantActive := []graphstate.EntityID{3, 5}, []graphstate.EntityID{5}
	if old {
		wantRelationships, wantActive = []graphstate.EntityID{3}, []graphstate.EntityID{3}
	}
	if !slices.Equal(nodes, []graphstate.EntityID{1, 17}) || !slices.Equal(activeNodes, nodes) || !slices.Equal(relationships, wantRelationships) || !slices.Equal(activeRelationships, wantActive) {
		t.Fatal("exact node/relationship declared and effective sets", nodes, relationships, activeNodes, activeRelationships)
	}
	labels := []string{"after-k"}
	if old {
		labels = []string{"before-p07"}
	} else if afterMove {
		labels = append(labels, "after-move")
	}
	assertCertifiedProjection(t, view, 1, true, labels)
	assertCertifiedProjection(t, view, 17, true, nil)
	for _, id := range []graphstate.EntityID{1, 17} {
		projection, err := graphstate.Project(t.Context(), view, id, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		if err != nil || projection.Life != graphstate.LifeID(id+1) || projection.Record.Interpretation != graphstate.InterpretationObservation || projection.Record.TemporalRole != graphstate.TemporalRoleValidity || projection.Record.Axis.DefinitionHash() != window.Axis().DefinitionHash() {
			t.Fatal("stable native node IDs/lives/axis/declarations", id, projection, err)
		}
		if id == 17 {
			if len(projection.Properties) != 0 {
				t.Fatal("independent node gained phantom properties", projection)
			}
			continue
		}
		wanted := int64(10)
		if old {
			wanted = 9
		}
		if len(projection.Properties) != 1 || projection.Properties[0].Name != "p" || projection.Properties[0].Cardinality != graphstate.ScalarCardinality {
			t.Fatal("exact old/current node property set", projection.Properties)
		}
		same, err := projection.Properties[0].Scalar.Equal(graphstate.I64(wanted), defaultMaterializerLimits().graph.Planner)
		if err != nil || !same {
			t.Fatal("old node projection substituted current scalar", wanted, err)
		}
	}
	for _, id := range wantRelationships {
		projection, err := graphstate.Project(t.Context(), view, id, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		life, lifeErr := view.Life(t.Context(), id, graphstate.LifeID(id+1))
		if err != nil || lifeErr != nil || !life.Found || projection.Record.Type != "cross-edge" || projection.Record.Source != 1 || projection.Record.Target != 17 || projection.Record.Mode != graphstate.LifeBound || projection.Record.Interpretation != graphstate.InterpretationAssertedRelation || projection.Record.TemporalRole != graphstate.TemporalRoleOccurrenceTime || life.Record.SourceLife != 2 || life.Record.TargetLife != 18 {
			t.Fatal("immutable relation IDs/native declarations/endpoint lives", id, projection, life, err, lifeErr)
		}
	}
	// Candidate indexes retain declared supersets. Effective answers hydrate
	// every candidate against this SAME view, never current state for old C.
	for _, endpoint := range []struct {
		id   graphstate.EntityID
		life graphstate.LifeID
	}{{1, 2}, {17, 18}} {
		page, err := view.IncidentRelationships(t.Context(), graphstate.IncidentPredicate{Endpoint: endpoint.id, Life: endpoint.life, Window: window}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		if err != nil || !page.Complete || page.Next != 0 || !slices.Equal(page.Entities, wantRelationships) {
			t.Fatal("exact retained incident candidates", endpoint, page, err)
		}
		var effective []graphstate.EntityID
		for _, id := range page.Entities {
			projection, err := graphstate.Project(t.Context(), view, id, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
			if err != nil {
				t.Fatal(err)
			}
			if projection.Active {
				effective = append(effective, id)
			}
		}
		if !slices.Equal(effective, wantActive) {
			t.Fatal("same-view effective incident set", effective, wantActive)
		}
	}
	oldClaims, newClaims := []graphstate.EntityID{1}, []graphstate.EntityID(nil)
	if !old {
		oldClaims, newClaims = nil, []graphstate.EntityID{1}
	}
	assertCertifiedClaim(t, view, window, 9, []graphstate.EntityID{1})
	assertCertifiedClaim(t, view, window, 10, newClaims)
	assertCertifiedEffectiveClaim(t, view, window, 9, oldClaims)
	assertCertifiedEffectiveClaim(t, view, window, 10, newClaims)
	assertCertifiedClaim(t, view, window, 999, nil)
}

func p07ReopenTargetFollower(t *testing.T, n *allocationTestNetwork) {
	t.Helper()
	const group, member = 1, 2
	before, image, err := n.hosts[group][member].machine.store.Checkpoint()
	if err != nil {
		t.Fatal(err)
	}
	if err := n.hosts[group][member].Close(); err != nil {
		t.Fatal(err)
	}
	cfg := n.configs[group][member]
	cfg.Create = false
	store, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal("existing-only target disk reopen", err)
	}
	machine, err := newDeclaredMaterializerWithAgreement(store, namespace{idalloc.GraphID(n.d.Graph()), cfg.Transfer.Identity.Partition}, n.d, defaultMaterializerLimits(), cfg.SemanticContractID)
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	host, err := openAllocationHost(machine, uint64(member+1), replica.Config{Store: store, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	n.hosts[group][member] = host
	t.Cleanup(func() {
		if err := host.Close(); err != nil {
			t.Error(err)
		}
	})
	index, restored, err := store.Checkpoint()
	if err != nil || index != before || !bytes.Equal(restored, image) || host.driver.Applied() != before {
		t.Fatal("actual target Restore changed durable state", index, before, err)
	}
}

// Validate genuine old coverage through the restored follower itself, not just
// the untouched target leader used by the still-live remote C session.
func p07AssertRestoredFollowerC(t *testing.T, n *allocationTestNetwork, cut graphCut, window temporal.Scope) {
	t.Helper()
	var views []*raftlog.ApplicationView
	defer func() {
		for _, view := range views {
			if err := view.Close(); err != nil {
				t.Error(err)
			}
		}
	}()
	for _, certificate := range cut.certificates {
		group := 0
		if certificate.scope.partition == 8 {
			group = 1
		}
		view, err := n.hosts[group][2].AtGraphCertificate(t.Context(), certificate)
		if err != nil {
			t.Fatal("restored follower lost original certified coverage", group, err)
		}
		views = append(views, view)
	}
	cfg, err := n.hosts[0][2].currentConfiguration()
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := cfg.digest()
	if err != nil {
		t.Fatal(err)
	}
	l := n.hosts[0][2].machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		t.Fatal(err)
	}
	full, _, err := graphstore.OpenPartitionTransactionView(t.Context(), views, n.d, cfg.defaultAxis, configuration, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: l.outputBytes}, arena)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := full.Close(); err != nil {
			t.Error(err)
		}
	}()
	p07AssertGraphAt(t, full, true, false, window)
}

func p07AssertCompleteCDC(t *testing.T, n *allocationTestNetwork, records [2][]p07RetainedRecord, decision crossStatus, intent rangeMoveIntent, moved rangeMoveResult, post crossStatus) {
	t.Helper()
	for _, committed := range []crossStatus{decision, post} {
		if parts := p07ResolvedParts(t, n, records, committed); parts != [2]bool{true, true} {
			t.Fatal("half cross transaction advanced completed feed boundary", committed.tx, parts)
		}
	}
	keys := make(map[graphstate.ComponentKey]bool)
	entities, lives, values, postGroups, postEmptySource := 0, 0, 0, 0, 0
	var kEnvelopes [2]int
	var placements [2][]byte
	for group, entries := range records {
		h := n.hosts[group][0]
		for _, entry := range entries {
			if len(entry.changes) == 0 {
				continue
			}
			if bytes.HasPrefix(entry.changes, []byte{'G', 'M', 'L', 1}) {
				body, err := graphBody(entry.changes, "GML\x01", 2048)
				if err != nil {
					t.Fatal(err)
				}
				cursor := graphCursor{b: body}
				move, intentWire := cursor.array(), cursor.take(32)
				rangeID, source, target, oldEpoch, nextEpoch, round := cursor.u64(), cursor.u64(), cursor.u64(), cursor.u64(), cursor.u64(), cursor.u64()
				digest := cursor.take(32)
				status, err := decodeRangeMoveStatus(entry.outcome)
				if err != nil || cursor.err != nil || len(cursor.b) != 0 || placements[group] != nil || move != moved.resume.move || !bytes.Equal(intentWire, moved.resume.intent[:]) || source != 3 || target != 8 || rangeID != intent.publication.RangeID || oldEpoch != intent.epoch || nextEpoch != oldEpoch+1 || round != moved.decision.round || !bytes.Equal(digest, moved.decision.digest[:]) || status.ns != h.machine.ns || status.phase != rangeMoveActivate || status.move != move || status.intent != moved.resume.intent || status.digest != moved.decision.digest || status.round != round || status.index != entry.index {
					t.Fatal("original complete placement CDC", group, status, err, cursor.err)
				}
				placements[group] = entry.changes
				continue
			}
			status, err := decodeCrossStatus(entry.outcome)
			committed := decision
			if status.tx == post.tx {
				committed = post
			}
			if err != nil || status.phase != crossResolve || status.tx != committed.tx || status.digest != committed.digest || status.round != committed.round || !status.commit {
				t.Fatal("unexpected business transaction in post-C prefix", status, err)
			}
			out := outcome{ns: h.machine.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
			changes, err := decodeTypedChangeEnvelope(entry.changes, out, h.machine.limits, true)
			if err != nil || out.index != entry.index {
				t.Fatal("CDC not bound to actual original outcome/index", group, entry.index, err)
			}
			if out.identity == post.tx {
				if group == 0 {
					// The fixed unique-home validates the real complete footprint
					// but has no logical component mutation. Its canonical empty
					// GCE still binds the original committed transaction/participant.
					if changes.nonempty() {
						t.Fatal("validation-only participant emitted phantom CDC", changes)
					}
					postEmptySource++
					continue
				}
				if len(changes.groups) != 1 || len(changes.entities)+len(changes.lives)+len(changes.values) != 0 || changes.groups[0].Key != (graphstate.ComponentKey{Owner: 1, Life: 2, Kind: graphstate.Label, Name: "after-move"}) || len(changes.groups[0].Changes) != 1 {
					t.Fatal("exact target-only CDC", changes)
				}
				change := changes.groups[0].Changes[0]
				if change.Before().Present() || !change.After().Present() || change.After().Revision().ID() != post.round || change.After().Revision().Provenance() != 203 {
					t.Fatal("target CDC lost exact before/after revision")
				}
				postGroups++
				continue
			}
			kEnvelopes[group]++
			entities += len(changes.entities)
			lives += len(changes.lives)
			values += len(changes.values)
			for _, entity := range changes.entities {
				if entity.ID != 5 || entity.Kind != graphstate.Relationship || entity.Source != 1 || entity.Target != 17 {
					t.Fatal("K gained phantom entity", entity)
				}
			}
			for _, life := range changes.lives {
				if life.Owner != 5 || life.Life != 6 || life.SourceLife != 2 || life.TargetLife != 18 {
					t.Fatal("K changed stable life IDs", life)
				}
			}
			for _, value := range changes.values {
				same, err := value.Value.Equal(graphstate.I64(10), h.machine.limits.graph.Planner)
				if err != nil || value.ID != 8 || !same {
					t.Fatal("K changed issued value ID/content", value, err)
				}
			}
			for _, component := range changes.groups {
				key := component.Key
				if keys[key] || len(component.Changes) != 1 {
					t.Fatal("duplicate/partial K CDC", key)
				}
				keys[key] = true
				change := component.Changes[0]
				if change.After().Present() && (change.After().Revision().ID() != decision.round || change.After().Revision().Provenance() != 202) {
					t.Fatal("K CDC did not retain original commit revision", key)
				}
				switch key {
				case graphstate.ComponentKey{Owner: 1, Life: 2, Kind: graphstate.ScalarProperty, Name: "p"}:
					if !change.Before().Present() || !change.After().Present() || change.Before().Value().ID() != 7 || change.After().Value().ID() != 8 {
						t.Fatal("K lost exact before/after dictionary IDs", key)
					}
				case graphstate.ComponentKey{Owner: 1, Life: 2, Kind: graphstate.Label, Name: "before-p07"}, graphstate.ComponentKey{Owner: 3, Kind: graphstate.Presence}:
					if !change.Before().Present() || change.After().Present() {
						t.Fatal("K removal CDC lost before/after support", key)
					}
				case graphstate.ComponentKey{Owner: 1, Life: 2, Kind: graphstate.Label, Name: "after-k"}, graphstate.ComponentKey{Owner: 5, Kind: graphstate.Presence}:
					if change.Before().Present() || !change.After().Present() {
						t.Fatal("K creation CDC lost before/after support", key)
					}
				default:
					t.Fatal("K emitted phantom component", key)
				}
			}
		}
	}
	want := map[graphstate.ComponentKey]bool{{Owner: 1, Life: 2, Kind: graphstate.ScalarProperty, Name: "p"}: true, {Owner: 1, Life: 2, Kind: graphstate.Label, Name: "before-p07"}: true, {Owner: 1, Life: 2, Kind: graphstate.Label, Name: "after-k"}: true, {Owner: 3, Kind: graphstate.Presence}: true, {Owner: 5, Kind: graphstate.Presence}: true}
	if !reflect.DeepEqual(keys, want) || entities != 1 || lives != 1 || values != 1 || postGroups != 1 || postEmptySource != 1 || kEnvelopes != [2]int{1, 1} || placements[0] == nil || !bytes.Equal(placements[0], placements[1]) {
		t.Fatal("gap/phantom/half logical CDC group", keys, entities, lives, values, postGroups)
	}
}

// One composition over actual disk-backed native Hosts/Drivers (six in-process
// voters). This is not the separate six-OS-child paired oracle or a new feed API.
func TestRangeMoveP07LostCommittedRequestRetainedCutAndCDCThroughReclaim(t *testing.T) {
	n, _, intent, fixtureWires, _ := rangeMoveCoordinatorFixture(t, 64)
	keeper := &rangeMoveKeeperWire{graphPeerWireTransport: fixtureWires[0].nativeGraphTestTransport}
	fault := &p07LostDecisionWire{graphPeerWireTransport: keeper, limits: n.hosts[0][0].machine.limits}
	base, err := newCrossProcessCoordinator(n.hosts[0][0], map[uint64]graphPeerWireTransport{3: fault, 8: fixtureWires[1].nativeGraphTestTransport})
	if err != nil {
		t.Fatal(err)
	}
	move, err := newRangeMoveCoordinatorOn(base)
	if err != nil {
		t.Fatal(err)
	}
	// P07 is a native in-process retention witness. Data reads use the real
	// native certified backend; remote lease-publication/RPC budgets belong to
	// the separate P09 lane. Business/move/keeper transport remains genuine.
	certified, certifiedTransport := certifiedCoordinator(n)
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	handle := n.hosts[0][0].recipients[[16]byte{1}]
	if handle == nil {
		t.Fatal("missing genuine source recipient")
	}
	grant := grantReference{session: handle.session, sequence: 1}
	edge := graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: 3, Life: 4, Scope: window, Record: graphstate.EntityRecord{Type: "cross-edge", Source: 1, Target: 17, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 18}}
	seed := []graphstate.Operation{edge, {Kind: graphstate.Set, Owner: 1, Life: 2, Scope: window, Name: "p", Value: graphstate.I64(9), ValueID: 7}, {Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "before-p07"}}
	seedClaims := []freshBinding{{role: entityBinding, id: 3, grant: grant}, {role: lifeBinding, owner: 3, id: 4, grant: grant}, {role: valueBinding, id: 7, grant: grant}}
	if result, err := base.Run(t.Context(), n.requestID(), seed, 201, seedClaims); err != nil || !result.decision.commit {
		t.Fatal("nonempty initial node/relationship/index state", result, err)
	}
	old, err := certified.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := old.Close(); err != nil {
			t.Error(err)
		}
	})
	cut, err := old.Cut()
	if err != nil {
		t.Fatal(err)
	}
	starts := p07CutIndexes(t, cut)
	oldView, err := old.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	p07AssertGraphAt(t, oldView, true, false, window)
	var initialSourceFirst [3]uint64
	for member, host := range n.hosts[0] {
		first, err := host.machine.store.FirstIndex()
		if err != nil || first > starts[0] {
			t.Fatal("actual C recovery entry was never retained", member, first, err)
		}
		initialSourceFirst[member] = first
		if entries, err := host.machine.store.Entries(starts[0], starts[0]+1, uint64(host.machine.store.Limits().MaxReadBytes)); err != nil || len(entries) != 1 {
			t.Fatal("actual C recovery entry before transfer", member, err)
		}
	}
	// Establish the retain-all internal start while C is live. Native versions,
	// original outcomes and CDC remain quota-backed; no synthetic lease is minted.
	for group := range 2 {
		view, err := n.hosts[group][0].machine.store.ApplicationView(starts[group])
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := view.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	lateID := n.requestID()
	lateWrite, err := n.hosts[0][0].PreparePartitionGraphOperations(lateID, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "must-not-appear"}}, 204, nil)
	if err != nil {
		t.Fatal(err)
	}
	oldService := n.read(0, allocationQuery{kind: observeAllocator})
	serviceBefore, err := idalloc.Inspect(&oldService.observation.allocator)
	if err != nil || serviceBefore.HighWater != 32 {
		t.Fatal("real allocation high-water", serviceBefore, err)
	}
	oldReserve, err := n.hosts[0][0].AuthorizeReserve(n.requestID(), idalloc.GrantRequest{Graph: n.hosts[0][0].machine.ns.graph, Session: handle.session, Sequence: 2, Count: 4}, oldService)
	if err != nil {
		t.Fatal(err)
	}
	created := edge
	created.Owner, created.Life = 5, 6
	ops := []graphstate.Operation{{Kind: graphstate.Close, Owner: 3, Life: 4, Scope: window}, created, {Kind: graphstate.RemoveLabel, Owner: 1, Life: 2, Scope: window, Name: "before-p07"}, {Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "after-k"}, {Kind: graphstate.Set, Owner: 1, Life: 2, Scope: window, Name: "p", Value: graphstate.I64(10), ValueID: 8}}
	claims := []freshBinding{{role: entityBinding, id: 5, grant: grant}, {role: lifeBinding, owner: 5, id: 6, grant: grant}, {role: valueBinding, id: 8, grant: grant}}
	id := n.requestID()
	fault.drop = true
	pending, err := base.Run(t.Context(), id, ops, 202, claims)
	if !fault.dropped || !errors.Is(err, errCrossProcessPending) || !errors.Is(err, graphstore.ErrSourceUnavailable) || pending.decision != (crossStatus{}) {
		t.Fatal("genuine committed K lost reply", pending, err)
	}
	decision := n.crossReadPhase(0, pending.resume.tx, crossDecide)
	if !decision.status.commit || decision.status.round == 0 {
		t.Fatal("missing genuine original COMMIT", decision.status)
	}
	blocked, err := move.Run(t.Context(), intent)
	if !errors.Is(err, errRangeMovePending) || blocked.committed {
		t.Fatal("unsettled original K allowed handoff", blocked, err)
	}
	for group := range 2 {
		if _, found := rangeMoveTransferControl(t, n.hosts[group][0], rangeMoveControlKey(n.hosts[group][0].machine.ns, blocked.resume.move, 'C')); found {
			t.Fatal("blocked move captured unsettled state", group)
		}
	}
	resolve, err := n.hosts[0][0].ResolveCrossParticipant(3, decision)
	if err != nil {
		t.Fatal(err)
	}
	n.crossSubmit(0, resolve)
	partialEnds := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	partial := p07ReadRetainedPrefix(t, n, starts, partialEnds)
	if parts := p07ResolvedParts(t, n, partial, decision.status); parts != [2]bool{true, false} {
		t.Fatal("actual one-sided K prefix must not complete/advance C", parts)
	}
	p07AssertGraphAt(t, oldView, true, false, window)
	settled, err := base.Run(t.Context(), id, ops, 202, claims)
	if err != nil || settled.decision != decision.status || settled.resume != pending.resume {
		t.Fatal("K settlement changed original decision/IDs", settled, err)
	}
	done := rangeMoveKeeperResume(t, move, blocked.resume)
	if done.resume != blocked.resume {
		t.Fatal("settlement replanned handoff")
	}
	p07ReopenTargetFollower(t, n)
	p07AssertRestoredFollowerC(t, n, cut, window)
	// The moved node has a unique property whose fixed unique-home remains3.
	// Its real complete footprint uses both groups even though the new primary
	// owner is8. The local-only door must not stand in for this protocol.
	postResult, err := base.Run(t.Context(), n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "after-move"}}, 203, nil)
	if err != nil || !postResult.decision.commit || postResult.decision.round <= done.decision.round {
		t.Fatal("actual complete moved-node mutation", postResult, err)
	}
	post := postResult.decision
	late := n.submitPartitionGraph(0, lateWrite, reasonRemoteParticipant)
	transfer, err := n.hosts[0][0].TransferAllocator(n.requestID(), n.read(0, allocationQuery{kind: observeAllocator}))
	if err != nil {
		t.Fatal(err)
	}
	n.submit(0, transfer)
	n.submitReason(0, oldReserve, reasonStale)
	if _, err := n.hosts[0][0].AuthorizeReserve(n.requestID(), idalloc.GrantRequest{Graph: n.hosts[0][0].machine.ns.graph, Session: handle.session, Sequence: 2, Count: 4}, oldService); !errors.Is(err, idalloc.ErrStaleAuthority) {
		t.Fatal("old allocator proof regained authority", err)
	}
	service := n.read(0, allocationQuery{kind: observeAllocator})
	after, err := idalloc.Inspect(&service.observation.allocator)
	if err != nil || after.HighWater != serviceBefore.HighWater || after.Authority.Epoch() != serviceBefore.Authority.Epoch()+1 {
		t.Fatal("real transfer reset high-water/failed to fence epoch", after, err)
	}
	current, err := certified.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := current.Close(); err != nil {
			t.Error(err)
		}
	})
	currentView, err := current.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	p07AssertGraphAt(t, currentView, false, true, window)
	boundary, err := current.Cut()
	if err != nil {
		t.Fatal(err)
	}
	ends := p07CutIndexes(t, boundary)
	beforeCDC := p07ReadRetainedPrefix(t, n, starts, ends)
	p07AssertCompleteCDC(t, n, beforeCDC, decision.status, intent, done, post)
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	// Driver watermark reclamation is already genuine during a long handoff.
	// Add one real no-effect fence above the actual later closed round so this
	// explicit publication also has a new committed recovery entry to remove.
	var closed uint64
	for _, certificate := range boundary.certificates {
		closed = max(closed, certificate.round)
	}
	fence, err := n.hosts[0][0].PrepareGraphFence(closed + 1)
	if err != nil {
		t.Fatal(err)
	}
	if result, err := certifiedTransport.Submit(t.Context(), n.hosts[0][0], fence); err != nil || result.phase != graphCutFence || result.status != graphCutApplied || result.round != closed+1 {
		t.Fatal("actual pre-publication fence", result, err)
	}
	for member, host := range n.hosts[0] {
		store := host.machine.store
		first, err := store.FirstIndex()
		if err != nil || first <= initialSourceFirst[member] {
			t.Fatal("actual Driver recovery watermark never advanced", member, first, err)
		}
		newEntry := host.driver.Applied()
		if first > newEntry {
			t.Fatal("new genuine publication predecessor already compacted", member, first, newEntry)
		}
		if entries, err := store.Entries(newEntry, newEntry+1, uint64(store.Limits().MaxReadBytes)); err != nil || len(entries) != 1 {
			t.Fatal("actual new committed fence absent before explicit publication", member, err)
		}
		usage, err := store.ApplicationUsage()
		if err != nil {
			t.Fatal(err)
		}
		if err := store.PublishSnapshot(); err != nil {
			t.Fatal("actual old-owner publication", err)
		}
		if err := store.ReclaimApplication(); err != nil {
			t.Fatal(err)
		}
		published, err := store.PublishedApplicationCut()
		if err != nil || published.Index != host.driver.Applied() || published.Index <= decision.status.index || published.Index <= starts[0] {
			t.Fatal("source recovery Base did not advance past C/K", published.Index, err)
		}
		if first, err := store.FirstIndex(); err != nil || first != published.Index+1 {
			t.Fatal("published Base/first-index mismatch", first, err)
		}
		if entries, err := store.Entries(starts[0], starts[0]+1, uint64(store.Limits().MaxReadBytes)); !errors.Is(err, raft.ErrCompacted) || len(entries) != 0 {
			t.Fatal("old recovery prefix not reclaimed", err)
		}
		// Scrub checks visible first/last entry keys and exact physical log
		// ledger; this does not claim immediate removal of obsolete SST bytes.
		if err := store.Scrub(); err != nil {
			t.Fatal("deleted recovery entry-key bounds", err)
		}
		now, err := store.ApplicationUsage()
		if err != nil || now.RetainedBytes != usage.RetainedBytes || now.RetainedRecords != usage.RetainedRecords || now.GraphBytes != usage.GraphBytes || now.ControlBytes != usage.ControlBytes || now.RecoveryBytes != usage.RecoveryBytes {
			t.Fatal("compaction removed logical history/decision/CDC retention", now, usage, err)
		}
	}
	if retained := p07ReadRetainedPrefix(t, n, starts, ends); !reflect.DeepEqual(retained, beforeCDC) {
		t.Fatal("post-C retained feed changed/gapped after actual reclamation")
	}
	p07AssertGraphAt(t, oldView, true, false, window)
	at, err := certified.Open(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	if err != nil {
		t.Fatal("original C unavailable after handoff/reopen/reclaim", err)
	}
	t.Cleanup(func() {
		if err := at.Close(); err != nil {
			t.Error(err)
		}
	})
	atView, err := at.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	p07AssertGraphAt(t, atView, true, false, window)
	if err := at.Close(); err != nil {
		t.Fatal(err)
	}
	original := n.crossReadPhase(0, decision.status.tx, crossDecide)
	if original.status != decision.status {
		t.Fatal("original keeper lost immutable decision after compaction")
	}
	again, err := base.Run(t.Context(), id, ops, 202, claims)
	if err != nil || again != settled {
		t.Fatal("post-handoff/restart/reclaim K changed original result/IDs", again, settled, err)
	}
	if _, err := base.Run(t.Context(), id, ops, 203, claims); !errors.Is(err, errInvalid) {
		t.Fatal("changed payload/provenance reused K", err)
	}
	lateKey := declaredRequestKey(n.hosts[0][0].machine.ns, graphOperations, [16]byte(lateID))
	lateBytes, found := rangeMoveKeeperStored(t, n.hosts[0][0], lateKey)
	if !found || sha256.Sum256(lateWrite.wire) != late.hash {
		t.Fatal("missing exact uncopied source refusal")
	}
	queries := keeper.originalQueries
	got, err := move.LookupOriginal(t.Context(), done.resume, n.hosts[0][0].machine.ns, false, lateKey)
	if err != nil || !got.found || got.originalIndex != late.index || !bytes.Equal(got.value, lateBytes) || keeper.originalQueries != queries+1 {
		t.Fatal("late original did not use surviving original keeper authority", got, err)
	}
	keeper.keeperUnavailable = true
	got, err = move.LookupOriginal(t.Context(), done.resume, n.hosts[0][0].machine.ns, false, lateKey)
	keeper.keeperUnavailable = false
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || !reflect.DeepEqual(got, rangeMoveOriginal{}) {
		t.Fatal("missing keeper became target substitute/absence", got, err)
	}
	fresh, err := certified.Open(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := fresh.Close(); err != nil {
			t.Error(err)
		}
	})
	freshView, err := fresh.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	p07AssertGraphAt(t, freshView, false, true, window)
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("P07 actual in-process six-voter composition: C=%v, retained feed=%v..%v; K=%x/%d, moved=%x/%d; target follower reopened; three source recovery prefixes reclaimed; exact history/CDC/replay/fencing verified", starts, starts, ends, decision.status.tx, decision.status.round, done.resume.move, done.decision.round)
}
