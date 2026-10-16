package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

type retainedOnlyFaultWire struct {
	graphPeerWireTransport
	drop bool
}

func (w *retainedOnlyFaultWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	transition := false
	if r.kind == sourceSubmit {
		p, e := decodeGraphSourceProposal(r.command)
		if e != nil {
			return nil, e
		}
		if isRetainedFeedWire(p.wire) {
			c, e := decodeRetainedFeedCommand(p.wire)
			if e != nil {
				return nil, e
			}
			transition = c.phase == feedRetainOnly
		}
	}
	out, err := w.graphPeerWireTransport.RoundTrip(ctx, wire)
	if err == nil && transition && w.drop {
		w.drop = false
		return nil, io.ErrUnexpectedEOF
	}
	return out, err
}

// This parent authors real native work but has not been executed. Native rolling
// source protection is intentionally more conservative than detached FDB CDC.
func TestRetainedOnlyFeedNativeACKAnchorCollectionAndOriginalRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	var absent *retainedFeedOwner
	if err := absent.ToRetainedOnly(t.Context(), [16]byte{1}); !errors.Is(err, errInvalid) {
		t.Fatal("nil owner", err)
	}
	if _, err := newRetainedOnlyFeedMaterializer(nil, namespace{}, graphstore.OwnershipDeclaration{}, defaultMaterializerLimits()); !errors.Is(err, errInvalid) {
		t.Fatal("nil store", err)
	}
	n, p, edge, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithAgreement(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes = 64 << 20
		p.RetainedApplicationRecords = 32768
	}, retainedOnlyFeedSemanticContractID()))
	h := n.hosts[0][0]
	set := func(value int64, id graphstate.ValueID, round uint64) allocationProposal {
		t.Helper()
		grant := grantReference{session: h.recipients[[16]byte{1}].session, sequence: 1}
		proposal, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.Set, Owner: 1, Life: 2, Name: "p", Value: graphstate.I64(value), ValueID: id, Scope: edge[0].Scope}}, round, []freshBinding{{role: valueBinding, id: uint64(id), grant: grant}})
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, proposal, reasonNone)
		return proposal
	}
	before := set(5, 5, 40)
	owner, err := beginRetainedFeed(t.Context(), p, n.requestID(), graphReadRequest{mode: graphReadFresh}, 100, retainedFeedGuardPolicy(128))
	if err != nil {
		t.Fatal(err)
	}
	token, err := owner.Publish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	held, err := owner.OpenSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cut := graphCut{certificates: make([]graphCertificate, len(owner.pin.definition.parts))}
	for i, part := range owner.pin.definition.parts {
		cut.certificates[i] = part.certificate
	}
	// A lost actual reply leaves the first keeper transitioned and the second
	// untouched. A copied recipe supplies no new mode or publication authority.
	action := [16]byte(n.requestID())
	p.coordinator.transports[3] = &retainedOnlyFaultWire{graphPeerWireTransport: p.coordinator.transports[3], drop: true}
	if err = owner.ToRetainedOnly(t.Context(), action); !errors.Is(err, errRetainedFeedPending) || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatal("transition reply was not lost", err)
	}
	for g := range 2 {
		q, e := graphRetentionReader(n.hosts[g][0], t.Context())
		if e != nil {
			t.Fatal(e)
		}
		_, installed, e := n.hosts[g][0].machine.feedRetention(q, owner.pin.definition)
		if e != nil || installed != (g == 0) {
			t.Fatal("partial transition fabricated another keeper", g, installed, e)
		}
		if e = q.view.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err = openRetainedFeed(t.Context(), p, token)
	if err != nil {
		t.Fatal(err)
	}
	if err = owner.ToRetainedOnly(t.Context(), action); err != nil {
		t.Fatal("SAME partial original did not complete", err)
	}
	if err = owner.ToRetainedOnly(t.Context(), action); err != nil {
		t.Fatal("installed transition original did not replay", err)
	}
	// Canonical changed caller bodies cannot turn the installed transition into
	// another agreement/recipe, even at the committed-entry Stage door.
	changed := owner.definition
	changed.policy.maxPages--
	changedDefinition, e := encodeRetainedFeedDefinition(changed)
	if e != nil {
		t.Fatal(e)
	}
	command := owner.command(owner.pin.invocation.endpoints[3], feedRetainOnly, retainedFeedPhaseAction(action, feedRetainOnly), nil)
	command.definition = changedDefinition
	wire, e := encodeRetainedFeedCommand(command)
	if e != nil {
		t.Fatal(e)
	}
	applied := h.driver.Applied()
	batch, e := h.machine.Stage(replica.Entry{Index: applied + 1, Generation: h.machine.store.ApplicationGeneration(), Term: 2, Data: wire}, h.machine.store.ApplicationBudget())
	if !errors.Is(e, errInvalid) || len(batch.Writes)+len(batch.ControlPuts)+len(batch.Changes) != 0 || h.driver.Applied() != applied {
		t.Fatal("changed immutable transition body admitted effects", batch, e)
	}
	if err = owner.ToRetainedOnly(t.Context(), [16]byte(n.requestID())); !errors.Is(err, errInvalid) {
		t.Fatal("another request replaced irreversible transition", err)
	}
	for _, open := range []func() (*certifiedGraphRead, error){func() (*certifiedGraphRead, error) { return owner.OpenSnapshot(t.Context()) }, func() (*certifiedGraphRead, error) { return p.Open(t.Context(), token.pin) }} {
		read, e := open()
		if read != nil || !errors.Is(e, errGraphHistoryExpired) || !errors.Is(e, raftlog.ErrApplicationExpired) {
			t.Fatal("transition admitted a new token snapshot", read, e)
		}
	}
	// Generic At follows actual floors, not a synthetic refusal based on mode.
	ordinary, err := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
	if err != nil {
		t.Fatal("mode forged generic physical expiry", err)
	}
	if err = ordinary.Close(); err != nil {
		t.Fatal(err)
	}
	project := func(read *certifiedGraphRead, want int64) {
		t.Helper()
		got, e := graphstate.Project(t.Context(), certifiedView(t, read), 1, certifiedPoint(t), graphstate.Effective, h.machine.limits.graph.Planner)
		if e != nil || !got.Active || got.Life != 2 || len(got.Properties) != 1 || got.Properties[0].Name != "p" {
			t.Fatal("wrong retained historical scalar/life", got, e)
		}
		if value, ok := got.Properties[0].Scalar.Int64Value(); !ok || value != want {
			t.Fatal("wrong exact scalar", got.Properties[0])
		}
	}
	project(held, 5)
	claims := []freshBinding{{role: entityBinding, id: 3, grant: grantReference{session: h.recipients[[16]byte{1}].session, sequence: 1}}, {role: lifeBinding, owner: 3, id: 4, grant: grantReference{session: h.recipients[[16]byte{1}].session, sequence: 1}}}
	tx, register := n.crossPlan(edge, claims, 50)
	decision := n.crossCommit(tx, register)
	n.crossResolve(decision)
	boundary := func() graphCut {
		t.Helper()
		read, e := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
		if e != nil {
			t.Fatal(e)
		}
		cut, e := read.Cut()
		if e != nil {
			t.Fatal(e)
		}
		if e = read.Close(); e != nil {
			t.Fatal(e)
		}
		return cut
	}
	drain := func(c graphCut, check func(retainedFeedDelivery)) {
		t.Helper()
		for range 64 {
			id := [16]byte(n.requestID())
			d, e := owner.Page(t.Context(), id, c)
			if e != nil {
				t.Fatal(e)
			}
			check(d)
			completed, e := owner.Ack(t.Context(), id, d.digest)
			if e != nil {
				t.Fatal(e)
			}
			if d.page.eof {
				if !feedCutEqual(completed, c) {
					t.Fatal("half boundary completion")
				}
				return
			}
		}
		t.Fatal("bounded complete feed did not finish")
	}
	cross := 0
	drain(boundary(), func(d retainedFeedDelivery) {
		if d.page.kind == feedCrossEnvelope {
			cross++
			if len(d.page.frames) != 2 || len(d.resolved.entities) != 3 || len(d.resolved.lives) != 3 {
				t.Fatal("incomplete actual cross endpoint/life bundle", d)
			}
		}
	})
	if cross != 1 {
		t.Fatal("missing/duplicate cross install", cross)
	}
	// Floor install is not blocked by nondurable held owners. Actual deletion
	// still is; the held pre-transition source keeps its captured old floors.
	var anchors [2]retainedFeedRetention
	var plans [2][32]byte
	for g := range 2 {
		host := n.hosts[g][0]
		q, e := graphRetentionReader(host, t.Context())
		if e != nil {
			t.Fatal(e)
		}
		anchor, found, e := host.machine.feedRetention(q, owner.pin.definition)
		cap, e2 := host.machine.graphRetentionCeiling(q, owner.pin.definition.parts[g].certificate.configuration)
		if e != nil || e2 != nil || !found || anchor.index <= owner.pin.definition.parts[g].certificate.sourceIndex || cap.root != anchor.index || cap.data != anchor.index || cap.recovery != anchor.index {
			t.Fatal("C was not replaced by genuine ACK anchor", anchor, cap, e, e2)
		}
		anchors[g] = anchor
		if e = q.view.Close(); e != nil {
			t.Fatal(e)
		}
		proposal, e := host.PrepareGraphRetention(t.Context(), n.requestID(), cap)
		if e != nil {
			t.Fatal(e)
		}
		event, e := host.Submit(proposal)
		if e != nil {
			t.Fatal(e)
		}
		n.pump(g, event)
		result, found, e := host.GraphRetentionOriginal(t.Context(), proposal)
		if e != nil || !found || result.status != graphRetentionApplied {
			t.Fatal("actual native plan", result, e)
		}
		plans[g] = result.id
		if _, _, e = host.machine.store.CollectApplicationRetention(t.Context(), result.id, raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20}); !errors.Is(e, raftlog.ErrLimit) {
			t.Fatal("held C source did not protect actual deletion", e)
		}
	}
	project(held, 5)
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	for g := range 2 {
		for _, host := range n.hosts[g] {
			progress := raftlog.ApplicationRetentionProgress{}
			for range 256 {
				progress, _, err = host.machine.store.CollectApplicationRetention(t.Context(), plans[g], raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20})
				if err != nil {
					t.Fatal(err)
				}
				if progress.Complete {
					break
				}
			}
			if !progress.Complete {
				t.Fatal("bounded native collection unfinished")
			}
			if view, e := host.machine.store.ApplicationView(owner.pin.definition.parts[g].certificate.sourceIndex); view != nil || !errors.Is(e, raftlog.ErrApplicationExpired) {
				t.Fatal("old physical C unexpectedly available", view, e)
			}
		}
	}
	if read, e := owner.OpenSnapshot(t.Context()); read != nil || !errors.Is(e, errGraphHistoryExpired) || !errors.Is(e, raftlog.ErrApplicationExpired) {
		t.Fatal("collected snapshot token borrowed rolling feed source", read, e)
	}
	if read, e := p.coordinator.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil); read != nil || !errors.Is(e, errGraphHistoryExpired) || !errors.Is(e, raftlog.ErrApplicationExpired) {
		t.Fatal("genuinely collected generic C did not report expiry", read, e)
	}
	// An incoming-bank audit must authenticate the mode/anchor there, while its
	// active receiver contains no corresponding owner, cursor, original or root.
	receiver, prepared, manifest, _, _ := preparedDeclaredAuditFixture(t, h.machine.store, n.configs[0][2])
	machine, err := newDeclaredMaterializerBinding(receiver, h.machine.ns, n.d, h.machine.limits, retainedOnlyFeedSemanticContractID())
	if err != nil {
		t.Fatal(err)
	}
	view, err := prepared.OpenReadView(t.Context(), manifest.Index, 172)
	if err != nil {
		t.Fatal(err)
	}
	base, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	q := feedReader(t.Context(), machine, view, base, nil)
	q.preparedHistory = prepared
	if err = joinGraphOutputBudget(&q, graphRequest{}, 4096, machine.limits); err != nil {
		t.Fatal(err)
	}
	if got, found, e := machine.feedRetention(&q, owner.pin.definition); e != nil || !found || got != anchors[0] {
		t.Fatal("mode/anchor borrowed active-bank authority", got, e)
	}
	if err = view.Close(); err != nil {
		t.Fatal(err)
	}
	// Actual native reopen retains the same mode/original and funded bank.
	prior := n.hosts[0][2]
	if err = prior.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := n.configs[0][2]
	cfg.Create = false
	store, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	machine, err = newRetainedOnlyFeedMaterializer(store, prior.machine.ns, n.d, prior.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := openAllocationHost(machine, 3, replica.Config{Store: store, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	n.hosts[0][2] = replacement
	t.Cleanup(func() {
		if e := replacement.Close(); e != nil {
			t.Error(e)
		}
	})
	owner, err = openRetainedFeed(t.Context(), p, token)
	if err != nil {
		t.Fatal(err)
	}
	// New legitimate replay of an ACKed pre-C original needs no deleted raw
	// sidecar and must not emit a second delta after physical collection.
	event, err := h.Submit(before)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	set(6, 6, 60)
	local := 0
	drain(boundary(), func(d retainedFeedDelivery) {
		if d.page.kind != feedLocalEnvelope {
			return
		}
		local++
		if len(d.page.frames) != 1 || len(d.page.payloads) != 2 || len(d.resolved.values) != 2 || len(d.schemas) != 1 || d.schemas[0].Name != "p" || len(d.resolved.entities) != 1 || len(d.resolved.lives) != 1 {
			t.Fatal("unread next envelope lost real before/after dictionary/life", d)
		}
		var five, six bool
		for _, value := range d.resolved.values {
			n, ok := value.Value.Int64Value()
			five = five || value.ID == 5 && ok && n == 5
			six = six || value.ID == 6 && ok && n == 6
		}
		if !five || !six {
			t.Fatal("wrong exact before/after values")
		}
		remote := n.hosts[1][0]
		q, e := graphRetentionReader(remote, t.Context())
		if e != nil {
			t.Fatal(e)
		}
		payload := d.page.payloads[1]
		if payload.index < anchors[1].index {
			t.Fatal("unchanged participant used collected PLM location", payload.index, anchors[1].index)
		}
		bad := payload
		bad.root[0] ^= 1
		if e = remote.machine.checkFeedPayload(q, owner.pin.definition, bad, d.page.round, cut.certificates[1].configuration); !errors.Is(e, errInvalid) {
			t.Fatal("changed root admitted as ACK anchor", e)
		}
		bad = payload
		bad.index++
		if e = remote.machine.checkFeedPayload(q, owner.pin.definition, bad, d.page.round, cut.certificates[1].configuration); !errors.Is(e, errInvalid) {
			t.Fatal("changed native coordinate admitted", e)
		}
		if _, _, _, e = remote.machine.feedPayloadSource(q, owner.pin.definition, cut.certificates[1].configuration, cut.certificates[1].round); !errors.Is(e, errGraphHistoryExpired) || !errors.Is(e, raftlog.ErrApplicationExpired) {
			t.Fatal("older different PLM round borrowed ACK anchor", e)
		}
		if round, index, _, e := readGraphRoundPredecessor(q, cut.certificates[1].configuration, cut.certificates[1].round); !errors.Is(e, raftlog.ErrApplicationExpired) || round != 0 || index != 0 {
			t.Fatal("ordinary predecessor error leaked decoded scalar coordinates", round, index, e)
		}
		if e = q.view.Close(); e != nil {
			t.Fatal(e)
		}
	})
	if local != 1 {
		t.Fatal("replay duplicated or GC lost business install", local)
	}
	if err = owner.Release(t.Context()); err != nil {
		t.Fatal("retained-only mode stranded funded terminal", err)
	}
	if _, err = openRetainedFeed(t.Context(), p, token); !errors.Is(err, errGraphPinReleased) {
		t.Fatal("released original became a live feed", err)
	}
	if err = owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err = owner.ToRetainedOnly(t.Context(), action); !errors.Is(err, errRetainedFeedClosed) {
		t.Fatal("closed local owner admitted operation work", err)
	}
}

func TestRetainedOnlyFeedWireCapabilitiesPreserveLegacy(t *testing.T) {
	old, fresh := retainedFeedSemanticContractID(), retainedOnlyFeedSemanticContractID()
	if old == fresh || !retainedFeedAgreement(fresh) || !durableColumnGraphAgreement(fresh) || !conditionalGraphAgreement(fresh) || !rangeMoveGraphAgreement(fresh) {
		t.Fatal("fresh capability chain or old identity changed")
	}
	if retainedFeedOutcomeFor(old, []byte("GFO\x02")) || !retainedFeedOutcomeFor(old, []byte("GFO\x01")) || !retainedFeedOutcomeFor(fresh, []byte("GFO\x02")) || retainedFeedOutcomeFor(fresh, []byte("GCD\x05")) {
		t.Fatal("legacy/fresh metadata versus business classification")
	}
	r := retainedFeedResult{original: [32]byte{1}, command: [32]byte{2}, action: [16]byte{3}, index: 4, phase: feedRetainOnly, status: feedApplied}
	wire, err := encodeRetainedFeedResult(r)
	if err != nil || !bytes.HasPrefix(wire, []byte("GFO\x02")) {
		t.Fatal(err)
	}
	if got, e := decodeRetainedFeedResult(wire); e != nil || got != r {
		t.Fatal("fresh immutable result", got, e)
	}
	r.phase = feedAcknowledge
	wire, err = encodeRetainedFeedResult(r)
	if err != nil || !bytes.HasPrefix(wire, []byte("GFO\x01")) {
		t.Fatal("legacy result format changed", err)
	}
}
