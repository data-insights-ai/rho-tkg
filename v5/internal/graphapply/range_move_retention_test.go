package graphapply

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type retainedMoveKeeperFault struct {
	graphPeerWireTransport
	unavailable bool
}

func (w *retainedMoveKeeperFault) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	if w.unavailable {
		return nil, graphstore.ErrSourceUnavailable
	}
	return w.graphPeerWireTransport.RoundTrip(ctx, wire)
}

// One authored six-voter composition. Native controls/roots, the portable pin,
// move and local physical collection are genuine; this is not process acceptance.
func TestCollectedGraphHandoffKeepsOriginalDurablePinAndFrozenLineage(t *testing.T) {
	n, p, edgeOps, wires := retainedPublisherFromNetwork(t, collectedGraphNetworkWithPolicy(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes = 64 << 20
		p.RetainedApplicationRecords = 32768
	}))
	move, err := newRangeMoveCoordinatorOn(p.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	request, err := p.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 100)
	if err != nil {
		t.Fatal(err)
	}
	original, err := request.Original()
	if err != nil {
		t.Fatal(err)
	}
	token, err := request.Publish(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := decodeGraphPinManifest(original)
	if err != nil {
		t.Fatal(err)
	}
	read, err := p.Open(t.Context(), token)
	if err != nil {
		t.Fatal(err)
	}
	cut, err := read.Cut()
	if err != nil {
		t.Fatal(err)
	}
	view, err := read.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, view, 1, true, nil)
	assertCertifiedProjection(t, view, 17, true, nil)
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	h := n.hosts[0][0]
	// Lose only the genuine coordinator decision reply after its durable COMMIT;
	// settle that original before the coarse resolved handoff fence.
	keeper := &retainedMoveKeeperFault{graphPeerWireTransport: p.coordinator.transports[3]}
	lost := &p07LostDecisionWire{graphPeerWireTransport: keeper, limits: h.machine.limits, drop: true}
	p.coordinator.transports[3] = lost
	handle := h.recipients[[16]byte{1}]
	if handle == nil {
		t.Fatal("missing actual original grant recipient")
	}
	claims := []freshBinding{{role: entityBinding, id: 3, grant: grantReference{session: handle.session, sequence: 1}}, {role: lifeBinding, owner: 3, id: 4, grant: grantReference{session: handle.session, sequence: 1}}}
	requestK := n.requestID()
	pending, e := p.coordinator.Run(t.Context(), requestK, edgeOps, 70, claims)
	if !lost.dropped || !errors.Is(e, errCrossProcessPending) {
		t.Fatal("durable decision reply was not actually lost", pending, e)
	}
	settled, e := p.coordinator.Run(t.Context(), requestK, edgeOps, 70, claims)
	if e != nil || !settled.decision.commit || settled.resume != pending.resume {
		t.Fatal("original lost reply recovery", settled, e)
	}
	intent, err := h.PrepareRangeMove(t.Context(), n.requestID(), 1, 8, 32, 64, 8<<10)
	if err != nil {
		t.Fatal(err)
	}
	// Only this authored handoff fixture uses the existing64MiB/32768-record
	// handoff shape (native generations128MiB/65536); production defaults and
	// the ordinary16MiB GC fixture are unchanged.
	t.Log("explicit handoff policy: retained64MiB/32768 records; generation128MiB/65536 records; pages32x64rows/8KiB")
	newRequest, e := p.Begin(t.Context(), n.requestID(), graphReadRequest{mode: graphReadFresh}, 100)
	if e != nil {
		t.Fatal(e)
	}
	intentWire, e := encodeRangeMoveIntent(intent)
	if e != nil {
		t.Fatal(e)
	}
	output, e := graphstate.NewOutputBudget(h.machine.limits.outputBytes)
	if e != nil {
		t.Fatal(e)
	}
	fence, e := h.prepareRangeMoveCommand(intentWire, rangeMoveFence, 3, 0, nil, nil, output)
	if e != nil {
		t.Fatal(e)
	}
	wires[1].beforePublish = func(graphPinCommand) {
		fenced, e := h.Submit(fence)
		if e != nil {
			t.Fatal(e)
		}
		n.pump(0, fenced)
		question := rangeMoveReadBudget(h.machine.limits)
		question.move = rangeMoveID(h.machine.ns, intent.id)
		question.nonce = [16]byte{81}
		reply, e := (&nativeGraphTestPump{n: n, group: 0, host: h}).Move(t.Context(), question)
		if e != nil || reply.err != nil {
			t.Fatal("actual source Fence authority", e, reply.err)
		}
		authority, e := h.exportRangeMoveAuthority(reply.proof)
		if e != nil {
			t.Fatal(e)
		}
		targetFence, e := h.prepareRangeMoveCommand(intentWire, rangeMoveFence, 8, 0, nil, []rangeMoveAuthority{authority}, output)
		if e != nil {
			t.Fatal(e)
		}
		targetEvent, e := n.hosts[1][0].Submit(targetFence)
		if e != nil {
			t.Fatal(e)
		}
		n.pump(1, targetEvent)
	}
	if bad, e := newRequest.Publish(t.Context()); !errors.Is(e, errRetainedPinPending) || len(bad.manifest) != 0 {
		t.Fatal("partial publication crossed frozen phase", bad, e)
	}
	partialManifest := newRequest.definition
	for group := range 2 {
		q, e := graphRetentionReader(n.hosts[group][0], t.Context())
		if e != nil {
			t.Fatal(e)
		}
		_, present, e := readPinOwner(q, partialManifest.id())
		if e != nil || present != (group == 0) {
			t.Fatal("partial publication invented/dropped actual owner", group, present, e)
		}
		c := pinCommandFor(partialManifest, newRequest.manifest, n.hosts[group][0].machine.scope(), pinPublish, partialManifest.request, 0)
		body, e := encodeGraphPinCommand(c)
		if e != nil {
			t.Fatal(e)
		}
		_, remembered, e := pinActionOriginal(q, c, body)
		if e != nil || remembered != (group == 0) {
			t.Fatal("pending publication invented immutable original", group, remembered, e)
		}
		if e := n.hosts[group][0].machine.checkRetainedPinRoster(q, true); e != nil {
			t.Fatal("pending publication invented terminal bank", e)
		}
		if e := q.view.Close(); e != nil {
			t.Fatal(e)
		}
	}
	if again, e := request.Publish(t.Context()); e != nil || !bytes.Equal(again.manifest, token.manifest) {
		t.Fatal("exact installed pin refused during handoff", again, e)
	}
	done, err := move.Run(t.Context(), intent)
	if err != nil || !done.committed || done.source.phase != rangeMoveFinish || done.target.phase != rangeMoveFinish {
		t.Fatal("active-pin transfer", done, err)
	}
	// The exact full-width pin identity lives in a move-qualified shadow packet,
	// not a truncated replay ID or a target native terminal reservation.
	q, err := graphRetentionReader(n.hosts[1][0], t.Context())
	if err != nil {
		t.Fatal(err)
	}
	shadow, present, err := readMoveRetainedLineage(q, done.resume.move, true)
	if err != nil || !present || len(shadow.owners) != 2 || !slices.Contains(shadow.head.owners, manifest.id()) || !slices.Contains(shadow.head.owners, partialManifest.id()) || shadow.scope != intent.source {
		t.Fatal("frozen complete source lineage", shadow, present, err)
	}
	if e := n.hosts[1][0].machine.checkRetainedPinRoster(q, true); e != nil {
		t.Fatal("shadow manufactured native bank owner", e)
	}
	packet, err := encodeMoveRetainedLineage(shadow, q.arena)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	arena, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := decodeMoveRetainedLineage(packet, arena)
	if err != nil || !bytes.Equal(parsed.owners[0], shadow.owners[0]) {
		t.Fatal("exact shadow roundtrip", err)
	}
	for _, mutate := range []func(*rangeMoveRetainedLineage){
		func(p *rangeMoveRetainedLineage) { p.owners = p.owners[:1] },
		func(p *rangeMoveRetainedLineage) { p.owners = append(p.owners, p.owners[0]) },
		func(p *rangeMoveRetainedLineage) { slices.Reverse(p.owners) },
	} {
		altered := parsed
		altered.owners = slices.Clone(parsed.owners)
		mutate(&altered)
		wire, e := encodeMoveRetainedLineage(altered, arena)
		if e != nil {
			t.Fatal(e)
		}
		if _, e := decodeMoveRetainedLineage(wire, arena); !errors.Is(e, errCorrupt) {
			t.Fatal("omitted/extra/reordered owner accepted", e)
		}
	}
	changed := append([]byte(nil), packet...)
	changed[len(changed)/2] ^= 1
	if _, err := decodeMoveRetainedLineage(changed, arena); !errors.Is(err, errCorrupt) {
		t.Fatal("changed shadow accepted", err)
	}
	completedPartial, e := newRequest.Publish(t.Context())
	if e != nil || !bytes.Equal(completedPartial.manifest, newRequest.manifest) || len(completedPartial.publications) != 2 {
		t.Fatal("same exact partial request did not complete after Finish", completedPartial, e)
	}
	if e := newRequest.Close(t.Context()); e != nil {
		t.Fatal("funded partial original cleanup", e)
	}
	// Restore an actual target follower directory with H/J controls and native
	// owner/bank state together. The original logical groups are not retired.
	prior := n.hosts[1][2]
	if err := prior.Close(); err != nil {
		t.Fatal(err)
	}
	cfg := n.configs[1][2]
	cfg.Create = false
	store, err := raftlog.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := newCollectedGraphMaterializer(store, prior.machine.ns, n.d, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	replacement, err := openAllocationHost(machine, 3, replica.Config{Store: store, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal("target restore lost lineage", err)
	}
	n.hosts[1][2] = replacement
	t.Cleanup(func() {
		if err := replacement.Close(); err != nil {
			t.Error(err)
		}
	})
	q, err = graphRetentionReader(replacement, t.Context())
	if err != nil {
		t.Fatal(err)
	}
	restored, present, err := readMoveRetainedLineage(q, done.resume.move, true)
	if err != nil || !present || !bytes.Equal(restored.header, shadow.header) || !reflect.DeepEqual(restored.owners, shadow.owners) {
		t.Fatal("target restart changed frozen lineage", err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	// Change the moved entity through the real complete participant protocol.
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	result, err := p.coordinator.Run(t.Context(), n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "after-retained-move"}}, 71, nil)
	if err != nil || !result.decision.commit {
		t.Fatal("post-move genuine write", result, err)
	}
	starts := p07CutIndexes(t, cut)
	ends := [2]uint64{n.hosts[0][0].driver.Applied(), n.hosts[1][0].driver.Applied()}
	before := p07ReadRetainedPrefix(t, n, starts, ends)
	var source graphCertificate
	for _, cert := range cut.certificates {
		if cert.scope == h.machine.scope() {
			source = cert
		}
	}
	proposal, err := h.PrepareGraphRetention(t.Context(), n.requestID(), graphRetentionFloors{source.sourceIndex, source.sourceIndex, source.sourceIndex + 1, source.sourceIndex, source.sourceIndex, source.index})
	if err != nil {
		t.Fatal("actual pin-safe floor", err)
	}
	event, err := h.Submit(proposal)
	if err != nil {
		t.Fatal(err)
	}
	n.pump(0, event)
	retention, found, err := h.GraphRetentionOriginal(t.Context(), proposal)
	if err != nil || !found || retention.status != graphRetentionApplied {
		t.Fatal("old-owner GC floor", retention, err)
	}
	for _, voter := range n.hosts[0] {
		progress := raftlog.ApplicationRetentionProgress{}
		for step := 0; !progress.Complete && step < 256; step++ {
			progress, _, err = voter.machine.store.CollectApplicationRetention(t.Context(), retention.id, raftlog.ReadBudget{Rows: 16, Bytes: 1 << 20})
			if err != nil {
				t.Fatal("pin-protected native collector", err)
			}
		}
		if !progress.Complete {
			t.Fatal("bounded collector did not complete")
		}
		if err := voter.machine.store.PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		if err := voter.machine.store.ReclaimApplication(); err != nil {
			t.Fatal(err)
		}
		if err := voter.machine.store.ScrubApplication(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if after := p07ReadRetainedPrefix(t, n, starts, ends); !reflect.DeepEqual(before, after) {
		t.Fatal("pinned feed changed or gapped after real collection")
	}
	read, err = p.Open(t.Context(), token)
	if err != nil {
		t.Fatal("original keeper lost pinned cut", err)
	}
	view, err = read.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	assertCertifiedProjection(t, view, 1, true, nil)
	assertCertifiedProjection(t, view, 17, true, nil)
	for id := graphstate.EntityID(1); id <= 32; id++ {
		entity, err := view.Entity(t.Context(), id)
		if err != nil || entity.Found != (id == 1 || id == 17) {
			t.Fatal("exact old entity set after transfer/GC", id, entity, err)
		}
	}
	if err := read.Close(); err != nil {
		t.Fatal(err)
	}
	againK, e := p.coordinator.Run(t.Context(), requestK, edgeOps, 70, claims)
	if e != nil || againK != settled {
		t.Fatal("handoff/restart/GC changed original K", againK, e)
	}
	if _, e := p.coordinator.Run(t.Context(), requestK, edgeOps, 72, claims); !errors.Is(e, errInvalid) {
		t.Fatal("changed K body accepted", e)
	}
	keeper.unavailable = true
	if unavailable, e := p.Open(t.Context(), token); unavailable != nil || !errors.Is(e, errRetainedPinPending) || !errors.Is(e, graphstore.ErrSourceUnavailable) {
		t.Fatal("target shadow replaced unavailable keeper", unavailable, e)
	}
	keeper.unavailable = false
	resumed, err := p.Resume(t.Context(), original)
	if err != nil {
		t.Fatal(err)
	}
	again, err := resumed.Publish(t.Context())
	if err != nil || !bytes.Equal(again.manifest, token.manifest) || !reflect.DeepEqual(again.publications, token.publications) {
		t.Fatal("original pin replay changed publication", again, err)
	}
	changedManifest := manifest
	changedManifest.expires++
	wire, err := encodeGraphPinManifest(changedManifest)
	if err != nil {
		t.Fatal(err)
	}
	command := pinCommandFor(changedManifest, wire, h.machine.scope(), pinPublish, manifest.request, 0)
	bad, err := h.retainedPinProposal(command)
	if err != nil {
		t.Fatal(err)
	}
	beforeIndex := h.driver.Applied()
	if _, err := h.Submit(bad); !errors.Is(err, errInvalid) || h.driver.Applied() != beforeIndex {
		t.Fatal("changed original pin body accepted", err)
	}
	if err := resumed.Release(t.Context()); err != nil {
		t.Fatal("original funded terminal after handoff/GC", err)
	}
	if read, err := p.Open(t.Context(), token); read != nil || !errors.Is(err, errGraphPinReleased) {
		t.Fatal("frozen shadow reactivated released owner", read, err)
	}
}
