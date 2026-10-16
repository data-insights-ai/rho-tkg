package graphapply

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// This is a composition guard over six native in-process voters, not six OS
// processes or physical hosts. Service-epoch rotation and fixed allocator HOME
// relocation are distinct schedules: this test changes only one range owner.
func TestAllocationP08CompletedRangeMoveFencesUnusedIDsWithCurrentRecipient(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, coordinator, intent, _, _ := rangeMoveCoordinatorFixture(t, 64)
	source := n.hosts[0][0]
	handle := source.recipients[[16]byte{1}]
	if handle == nil {
		t.Fatal("fixture has no current source recipient")
	}
	all, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	oldGrant := grantReference{session: handle.session, sequence: 1}
	movedGrant := grantReference{session: handle.session, sequence: 2}
	claim := func(role bindingRole, owner graphstate.EntityID, id uint64, grant grantReference) freshBinding {
		return freshBinding{role: role, owner: owner, id: id, grant: grant}
	}
	prepare := func(group int, ops []graphstate.Operation, claims []freshBinding) allocationProposal {
		t.Helper()
		proposal, e := n.hosts[group][0].PreparePartitionGraphOperations(n.requestID(), ops, 181, claims)
		if e != nil {
			t.Fatal(e)
		}
		return proposal
	}
	node := func(owner graphstate.EntityID, life graphstate.LifeID) graphstate.Operation {
		return graphstate.Operation{Kind: graphstate.CreateNode, Owner: owner, Life: life, Scope: all, Record: graphstate.EntityRecord{Interpretation: graphstate.InterpretationObservation, TemporalRole: graphstate.TemporalRoleValidity}}
	}
	// Cursor1 was discarded by the fixture. These claims consume a real retained,
	// still-owned grant; they do not reconstruct or claim to mint from cursor1.
	n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{node(3, 4)}, []freshBinding{claim(entityBinding, 0, 3, oldGrant), claim(lifeBinding, 3, 4, oldGrant)}), reasonNone)

	var reserve, install allocationProposal
	var reserved outcome
	var originalGrant allocationProof
	cursor, err := handle.Acquire(t.Context(), 2, 16, func(ctx context.Context, request idalloc.GrantRequest, query allocationQuery) (allocationReply, error) {
		if e := ctx.Err(); e != nil {
			return allocationReply{}, e
		}
		var e error
		reserve, e = source.AuthorizeReserve(n.requestID(), request, n.read(0, allocationQuery{kind: observeAllocator}))
		if e != nil {
			return allocationReply{}, e
		}
		reserved = n.submit(0, reserve)
		originalGrant = n.read(0, allocationQuery{kind: observeGrant, session: request.Session, sequence: 2, count: 16})
		install, e = source.InstallGrant(n.requestID(), 3, originalGrant)
		if e != nil {
			return allocationReply{}, e
		}
		n.submit(0, install)
		event, e := source.Read(query)
		if e != nil {
			return allocationReply{}, e
		}
		replies := n.pump(0, event)
		if len(replies) != 1 {
			return allocationReply{}, errInvalid
		}
		return replies[0], nil
	})
	if err != nil {
		t.Fatal(err)
	}
	next := func(want uint64) uint64 {
		t.Helper()
		id, e := cursor.Next()
		if e != nil || id != want {
			t.Fatal("actual cached cursor changed opaque IDs", id, want, e)
		}
		return id
	}
	owner, life := graphstate.EntityID(next(33)), graphstate.LifeID(next(34))
	original := prepare(0, []graphstate.Operation{node(owner, life)}, []freshBinding{claim(entityBinding, 0, 33, movedGrant), claim(lifeBinding, owner, 34, movedGrant)})
	committed := n.submitPartitionGraph(0, original, reasonNone)
	for _, h := range n.hosts[0] {
		wire, e := h.machine.store.ApplicationRecord(t.Context(), committed.index, false, h.machine.store.ApplicationLimits().MaxChangeBytes)
		cdc, decodeErr := decodeTypedChangeEnvelope(wire, committed, h.machine.limits, true)
		if e != nil || decodeErr != nil || len(cdc.entities) != 1 || cdc.entities[0].ID != 33 || !slices.Equal(cdc.lives, []graphstate.LifeRecord{{Owner: 33, Life: 34}}) || len(cdc.values) != 0 || len(cdc.groups) != 1 || cdc.groups[0].Key != (graphstate.ComponentKey{Owner: 33, Kind: graphstate.Presence}) {
			t.Fatal("original creation CDC has different exact identities/effects", cdc, e, decodeErr)
		}
	}
	originalKey := declaredRequestKey(source.machine.ns, graphOperations, committed.identity)
	originalBytes, found := rangeMoveTransferData(t, source, originalKey)
	if !found {
		t.Fatal("committed node creation has no original request record")
	}
	publication := originalGrant.observation.grant.publication
	if publication.First != 33 || publication.Last != 48 || publication.SourcePartition != 3 || publication.SourceIndex != reserved.index {
		t.Fatal("fixture did not reserve the literal moved block", publication, reserved)
	}
	intent.publication, intent.epoch = publication, originalGrant.observation.rangeOwner.Epoch
	old, closeOld := crossView(t, n, 0, [2]uint64{})
	defer closeOld()
	beforeRecipient := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: handle.session.ID}}).observation.recipient
	beforeServiceProof := n.read(0, allocationQuery{kind: observeAllocator})
	beforeService, err := idalloc.Inspect(&beforeServiceProof.observation.allocator)
	if err != nil {
		t.Fatal(err)
	}
	done, err := runRangeMoveToCompletion(t, coordinator, intent)
	if err != nil || !done.committed || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish {
		t.Fatal("actual range transfer did not complete", done, err)
	}

	// Control/replay bookkeeping may advance applied indexes. Logical graph root,
	// floor and CDC must remain exact, on every voter of both groups.
	unchanged := func(group int, apply func()) {
		t.Helper()
		var images [2][3][]byte
		var floors [2][3]uint64
		for g, voters := range n.hosts {
			for member, h := range voters {
				_, images[g][member], err = h.machine.store.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
				floors[g][member] = rangeMoveTransferInstalledFloor(t, h, publication.Configuration)
			}
		}
		apply()
		for g, voters := range n.hosts {
			for member, h := range voters {
				_, image, e := h.machine.store.Checkpoint()
				if e != nil || !bytes.Equal(image, images[g][member]) || rangeMoveTransferInstalledFloor(t, h, publication.Configuration) != floors[g][member] {
					t.Fatal("recovery/refusal changed graph effects or floor", g, member, e)
				}
				if g == group {
					cdc, e := h.machine.store.ApplicationRecord(t.Context(), h.driver.Applied(), false, h.machine.store.ApplicationLimits().MaxChangeBytes)
					if e != nil || len(cdc) != 0 {
						t.Fatal("recovery/refusal emitted duplicate graph CDC", member, e)
					}
				}
			}
		}
	}
	unchanged(0, func() {
		replayed := n.submit(0, reserve)
		if replayed.disposition != requestReplay || replayed.index != reserved.index || replayed.identity != reserved.identity || replayed.hash != reserved.hash {
			t.Fatal("exact reservation replay lost its original result", replayed, reserved)
		}
	})
	unchanged(0, func() {
		request := originalGrant.observation.grant.grant.Request
		recovery, e := source.AuthorizeReserve(n.requestID(), request, n.read(0, allocationQuery{kind: observeAllocator}))
		if e != nil || n.submit(0, recovery).disposition != grantRecovery {
			t.Fatal("same grant allocated again instead of recovery", e)
		}
	})
	unchanged(0, func() {
		// This original completed HOME observation predates the move. A new
		// request key forces InstallGrant's transition, rather than request replay.
		delayed, e := source.InstallGrant(n.requestID(), 3, originalGrant)
		if e != nil || n.submit(0, delayed).disposition != controlRecovery {
			t.Fatal("delayed original grant did not recover", e)
		}
	})
	unchanged(0, func() {
		replayed := n.submitPartitionGraph(0, original, reasonNone)
		if replayed.disposition != requestReplay || replayed.index != committed.index || replayed.identity != committed.identity || replayed.hash != committed.hash {
			t.Fatal("exact node creation retry changed original IDs/result", replayed, committed)
		}
	})
	got, err := coordinator.LookupOriginal(t.Context(), done.resume, source.machine.ns, false, originalKey)
	if err != nil || !got.found || got.originalIndex != committed.index || !bytes.Equal(got.value, originalBytes) {
		t.Fatal("target lost exact source-qualified original node creation", got, err)
	}
	currentGrant := n.read(0, allocationQuery{kind: observeGrant, session: handle.session, sequence: 2, count: 16}).observation
	if currentGrant.grant != originalGrant.observation.grant || currentGrant.rangeOwner.Partition != 8 || currentGrant.rangeOwner.Epoch != intent.epoch+1 || currentGrant.rangeOwner.Fenced {
		t.Fatal("grant recovery changed identities or reactivated former range owner", currentGrant)
	}
	for _, voters := range n.hosts {
		for _, h := range voters {
			key := graphstore.RangeOwnershipKey(graphstore.Namespace{Graph: publication.Graph, Partition: h.machine.ns.partition}, publication.RangeID)
			wire, present := rangeMoveTransferData(t, h, key)
			ownership, e := graphstore.DecodeRangeOwnership(wire)
			if e != nil || !present || ownership != currentGrant.rangeOwner {
				t.Fatal("delayed grant reactivated range on a native voter", ownership, e)
			}
		}
	}

	relationship := func(id graphstate.EntityID, life graphstate.LifeID) graphstate.Operation {
		return graphstate.Operation{Kind: graphstate.CreateRelationship, Owner: id, Life: life, Scope: all, Record: graphstate.EntityRecord{Type: "p08-edge", Source: 1, Target: 3, Mode: graphstate.LifeBound, Interpretation: graphstate.InterpretationAssertedRelation, TemporalRole: graphstate.TemporalRoleOccurrenceTime}, Binding: graphstate.LifeRecord{SourceLife: 2, TargetLife: 4}}
	}
	freshEntity, freshLife := next(35), next(36)
	unchanged(0, func() {
		n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{relationship(graphstate.EntityID(freshEntity), graphstate.LifeID(freshLife))}, []freshBinding{claim(entityBinding, 0, freshEntity, movedGrant), claim(lifeBinding, graphstate.EntityID(freshEntity), freshLife, movedGrant)}), reasonRemoteParticipant)
	})
	// Same shape, live endpoints, and still-owned real grant: input validation
	// cannot account for the preceding routing refusal.
	n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{relationship(5, 6)}, []freshBinding{claim(entityBinding, 0, 5, oldGrant), claim(lifeBinding, 5, 6, oldGrant)}), reasonNone)
	n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{{Kind: graphstate.Close, Owner: 1, Life: 2, Scope: all}}, nil), reasonNone)
	unusedLife := next(37)
	unchanged(0, func() {
		// Life routing follows owner1's STILL-LOCAL range, never LifeID37.
		// The current recipient is valid: only moved-grant consumption is stale.
		n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{{Kind: graphstate.Reopen, Owner: 1, Life: graphstate.LifeID(unusedLife), Scope: all}}, []freshBinding{claim(lifeBinding, 1, unusedLife, movedGrant)}), reasonStale)
	})
	n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{{Kind: graphstate.Reopen, Owner: 1, Life: 7, Scope: all}}, []freshBinding{claim(lifeBinding, 1, 7, oldGrant)}), reasonNone)
	unusedValue := next(38)
	viewBeforeValue, closeBeforeValue := crossView(t, n, 0, [2]uint64{})
	phantomValue, err := viewBeforeValue.ValueIdentity(t.Context(), graphstate.I64(181))
	closeBeforeValue()
	if err != nil || phantomValue.Found {
		t.Fatal("refusal must introduce a new typed value", phantomValue, err)
	}
	unchanged(0, func() {
		n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: all, Name: "p", Value: graphstate.I64(181), ValueID: graphstate.ValueID(unusedValue)}}, []freshBinding{claim(valueBinding, 0, unusedValue, movedGrant)}), reasonRemoteParticipant)
	})
	n.submitPartitionGraph(0, prepare(0, []graphstate.Operation{{Kind: graphstate.Set, Owner: 3, Life: 4, Scope: all, Name: "p", Value: graphstate.I64(181), ValueID: 8}}, []freshBinding{claim(valueBinding, 0, 8, oldGrant)}), reasonNone)
	n.submitPartitionGraph(1, prepare(1, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: owner, Life: life, Scope: all, Name: "moved-existing"}}, nil), reasonNone)

	afterRecipient := n.read(0, allocationQuery{kind: observeRecipient, session: idalloc.RecipientSession{ID: handle.session.ID}}).observation.recipient
	serviceProof := n.read(0, allocationQuery{kind: observeAllocator})
	afterService, err := idalloc.Inspect(&serviceProof.observation.allocator)
	if err != nil || beforeRecipient != afterRecipient || afterRecipient.session != handle.session || afterRecipient.homePhase != recipientActive || beforeService != afterService {
		t.Fatal("range-only schedule accidentally invalidated recipient/service", err)
	}
	for member := range 3 {
		view, close := crossView(t, n, member, [2]uint64{})
		var entities []graphstate.EntityID
		// All three actual published blocks lie in[1,48]. Literal complete
		// universe enumeration catches fresh phantom identities and omissions.
		for id := graphstate.EntityID(1); id <= 48; id++ {
			r, e := view.Entity(t.Context(), id)
			if e != nil {
				t.Fatal(e)
			}
			if r.Found {
				entities = append(entities, id)
			}
		}
		if !slices.Equal(entities, []graphstate.EntityID{1, 3, 5, 17, 33}) {
			t.Fatal("exact entity set changed after allocation refusals", entities)
		}
		l, e := view.Life(t.Context(), 1, graphstate.LifeID(unusedLife))
		if e != nil || l.Found {
			t.Fatal("moved unused LifeID was installed", l, e)
		}
		v, e := view.Value(t.Context(), graphstate.ValueID(unusedValue))
		if e != nil || v.Found {
			t.Fatal("moved unused ValueID was installed", v, e)
		}
		identity, e := view.ValueIdentity(t.Context(), graphstate.I64(181))
		if e != nil || !identity.Found || identity.ID != 8 {
			t.Fatal("typed positive control lost canonical value identity", identity, e)
		}
		assertCertifiedProjection(t, view, 33, true, []string{"moved-existing"})
		assertCertifiedProjection(t, view, 1, true, nil)
		assertCertifiedProjection(t, view, 17, true, nil)
		currentLife, e := graphstate.Project(t.Context(), view, 1, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		if e != nil || currentLife.Life != 7 {
			t.Fatal("positive life control did not install exact still-owned LifeID", currentLife, e)
		}
		currentProperty, e := graphstate.Project(t.Context(), view, 3, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		if e != nil || len(currentProperty.Properties) != 1 || currentProperty.Properties[0].Name != "p" {
			t.Fatal("positive value control did not install exact typed property", currentProperty, e)
		}
		equal, e := currentProperty.Properties[0].Scalar.Equal(graphstate.I64(181), defaultMaterializerLimits().graph.Planner)
		if e != nil || !equal {
			t.Fatal("positive value control changed exact typed value", equal, e)
		}
		property, e := view.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: graphstate.ComponentKey{Owner: 3, Life: 4, Kind: graphstate.ScalarProperty, Name: "p"}, Window: all}, 0, graphstate.ReadBudget{Rows: 100, Bytes: 1 << 20})
		pieces := property.Data.Pieces()
		if e != nil || !property.Complete || len(pieces) != 1 || !pieces[0].Cell().Present() || pieces[0].Cell().Value().ID() != 8 {
			t.Fatal("positive property control did not retain exact ValueID8", property, e)
		}
		assertCertifiedIncident(t, view, all, 1, 2, []graphstate.EntityID{5})
		assertCertifiedIncident(t, view, all, 3, 4, []graphstate.EntityID{5})
		close()
	}
	// Mutate-then-old-query: current routing/lifecycle/property state cannot stand
	// in for retained graph state, and the original stable identities survive.
	for _, id := range []graphstate.EntityID{1, 3, 17, 33} {
		assertCertifiedProjection(t, old, id, true, nil)
	}
	oldLife, err := old.Life(t.Context(), 1, 7)
	if err != nil || oldLife.Found {
		t.Fatal("old cut acquired post-move life", oldLife, err)
	}
	oldValue, err := old.Value(t.Context(), 8)
	if err != nil || oldValue.Found {
		t.Fatal("old cut acquired post-move value", oldValue, err)
	}
	oldRelationship, err := old.Entity(t.Context(), 5)
	if err != nil || oldRelationship.Found {
		t.Fatal("old cut acquired post-move relationship", oldRelationship, err)
	}
	oldTypedValue, err := old.ValueIdentity(t.Context(), graphstate.I64(181))
	if err != nil || oldTypedValue.Found {
		t.Fatal("old cut acquired post-move typed value identity", oldTypedValue, err)
	}
}
