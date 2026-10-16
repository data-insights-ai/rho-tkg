package graphapply

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// This helper transfers only the original native MsgSnap and its source-owned
// prepared stream. Publication must have made ordinary old-log catchup impossible.
func p09ActivateActualSnapshot(t *testing.T, transport *declaredSnapshotTransport, oldIndex uint64) raftlog.ApplicationSnapshotManifest {
	t.Helper()
	g := transport.g
	if err := g.stores[0].PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	first, err := g.stores[0].FirstIndex()
	if err != nil || first <= oldIndex {
		t.Fatal("real prefix reclamation required before native snapshot send", first, oldIndex, err)
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
				t.Fatal("native manifest exceeded finite page schedule")
			}
			if err := g.drivers[0].SealSnapshotSend(send); err != nil {
				t.Fatal(err)
			}
		}
	}
	if transport.send == nil {
		t.Fatal("actual source-owned MsgSnap did not appear")
	}
	manifest, err := transport.send.Manifest()
	if err != nil || manifest.Version != 5 || manifest.Index != g.drivers[0].Applied() {
		t.Fatal("actual AS5 capture required", manifest, err)
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
		if err != nil {
			t.Fatal(err)
		}
		if err := imported.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		complete = chunk.Final
	}
	if !complete {
		t.Fatal("actual AS5 stream exceeded finite page schedule")
	}
	if err := imported.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := imported.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	if g.stores[2].ApplicationGeneration() != 1 || g.drivers[2].Applied() != oldIndex {
		t.Fatal("inactive import altered actual old follower")
	}
	activated, err := g.drivers[2].StepApplicationSnapshot(transport.packet, prepared)
	if err != nil || g.stores[2].ApplicationGeneration() != 2 || g.drivers[2].Applied() != manifest.Index {
		t.Fatal("actual snapshot Ready did not activate complete AS5 bank", err)
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
	return manifest
}

func TestGraphSourcePublicationPinsActualRetiredBankAndRetainedCDCStart(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	deadline, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	n := newCertifiedGraphNetwork(t)
	ops, claims := crossEdgeFixture(t, n)
	coordinator, _ := certifiedCoordinator(n)
	initial := certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadFresh}, nil)
	cut, err := initial.Cut()
	if err != nil || len(cut.certificates) != 2 {
		t.Fatal("actual certified snapshot prerequisite", err)
	}
	if err := initial.Close(); err != nil {
		t.Fatal(err)
	}
	tx, register := n.crossPlan(ops, claims, 24)
	registered := n.crossSubmit(0, register)
	registration := n.crossReadPhase(0, registered.tx, crossRegister)
	h := n.hosts[0][2]
	server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
	if err != nil {
		t.Fatal(err)
	}
	defer server.close()
	entered := make(chan *graphSourceLease, 1)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	server.beforePublication = func(lease *graphSourceLease) {
		entered <- lease
		select {
		case <-release:
		case <-t.Context().Done():
		}
	}
	type result struct {
		info graphCertifiedSourceInfo
		err  error
	}
	done := make(chan result, 1)
	go func() {
		info, err := server.openCertified(t.Context(), cut.certificates[0], [16]byte{74})
		done <- result{info, err}
	}()
	var captured *graphSourceLease
	select {
	case captured = <-entered:
	case out := <-done:
		t.Fatal("source creation failed before actual capture", out.err)
	case <-deadline.Done():
		t.Fatal("actual capture never reached final publication")
	}
	oldRoot, err := captured.view.RootBounded(t.Context(), 172)
	if err != nil || oldRoot.Index != cut.certificates[0].sourceIndex || oldRoot.Generation != 1 {
		t.Fatal("capture did not own the exact old certified root", oldRoot, err)
	}
	oldProjection, _ := partitionProjectionFromView(t, h, captured.view, 1, graphGenesisBinding(t).Axis())
	if !oldProjection.Exists || !oldProjection.Active {
		t.Fatal("old cut must be nonempty", oldProjection)
	}
	g := partitionSnapshotGroup(n, 0)
	transport := &declaredSnapshotTransport{t: t, g: g, dropFollower: true, queue: make([]replica.Packet, 0, 128)}
	oldIndex := g.drivers[2].Applied()
	prepare, err := n.hosts[0][0].PrepareCrossParticipant(tx, 3, registration, nil)
	if err != nil {
		t.Fatal(err)
	}
	event, err := n.hosts[0][0].Submit(prepare)
	if err != nil {
		t.Fatal(err)
	}
	transport.enqueue(replica.Output{Packets: event.packets, SnapshotSends: event.snapshotSends})
	transport.drain()
	manifest := p09ActivateActualSnapshot(t, transport, oldIndex)
	// The captured-before-publication lease must be the physical bank owner:
	// another inactive import refuses until that exact owner releases it.
	if extra, err := g.stores[2].BeginApplicationImport(t.Context(), manifest); extra != nil || !errors.Is(err, raftlog.ErrLimit) {
		if extra != nil {
			_ = extra.Abort()
		}
		t.Fatal("retired captured bank was reclaimed/reused before lease publication", err)
	}
	unblock()
	var published result
	select {
	case published = <-done:
	case <-deadline.Done():
		t.Fatal("captured source publication did not finish")
	}
	if published.err != nil || published.info.cookie == (graphSourceCookie{}) {
		t.Fatal("live preserved capture did not publish", published.err)
	}
	retained, err := server.root(t.Context(), published.info.cookie, 172)
	if err != nil || retained.Generation != oldRoot.Generation || retained.Index != oldRoot.Index || retained.ImageHash != oldRoot.ImageHash || !bytes.Equal(retained.Image, oldRoot.Image) {
		t.Fatal("publication retargeted old capture to activated current bank", retained, oldRoot, err)
	}
	// Continue the original prepared transaction after genuine snapshot restore.
	vote := n.crossReadPhase(0, registered.tx, crossPrepare)
	remote, err := n.hosts[0][0].PrepareCrossParticipant(tx, 8, registration, []crossProof{vote})
	if err != nil || !n.crossSubmit(1, remote).yes {
		t.Fatal("original participant could not complete after AS5", err)
	}
	decisionProposal, err := n.hosts[0][0].DecideCrossTransaction(tx, []crossProof{vote, n.crossReadPhase(1, registered.tx, crossPrepare)})
	if err != nil || !n.crossSubmit(0, decisionProposal).commit {
		t.Fatal("original actual decision did not commit", err)
	}
	decision := n.crossReadPhase(0, registered.tx, crossDecide)
	resolve, err := n.hosts[0][0].ResolveCrossParticipant(3, decision)
	if err != nil {
		t.Fatal(err)
	}
	resolvedA := n.crossSubmit(0, resolve)
	pendingB := n.crossReadPhase(1, registered.tx, crossPrepare)
	if resolvedA.phase != crossResolve || pendingB.status.phase != crossPrepare {
		t.Fatal("fixture did not create genuine half-installed envelope schedule")
	}
	// Only the real Fresh coordinator can complete a new certified boundary;
	// it must resolve B rather than advertise A's envelope as complete.
	fresh := certifiedOpen(t, coordinator, graphReadRequest{mode: graphReadFresh}, nil)
	completeCut, err := fresh.Cut()
	if err != nil || len(completeCut.certificates) != 2 || completeCut.certificates[0].round < decision.status.round || completeCut.certificates[1].round != completeCut.certificates[0].round {
		t.Fatal("new certified completion did not include original decision", err)
	}
	if err := fresh.Close(); err != nil {
		t.Fatal(err)
	}
	for group := range 2 {
		leader := n.hosts[group][0]
		status, _, err := leader.machine.observeCrossTransaction(crossQuery{tx: registered.tx}, leader.driver.Applied())
		if err != nil || status.phase != crossResolve || status.round != decision.status.round {
			t.Fatal("completed cut omitted participant resolution", group, status, err)
		}
		outcome := outcome{ns: leader.machine.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
		original, err := leader.machine.store.ApplicationRecord(t.Context(), status.index, false, leader.machine.store.ApplicationLimits().MaxChangeBytes)
		if err != nil || len(original) == 0 {
			t.Fatal("completed participant must retain its full original envelope", group, err)
		}
		if _, err := decodeTypedChangeEnvelope(original, outcome, leader.machine.limits, true); err != nil {
			t.Fatal("retained participant envelope lost original TxID/body", group, err)
		}
		for member, follower := range n.hosts[group] {
			if err := follower.machine.store.PublishSnapshot(); err != nil {
				t.Fatal(err)
			}
			for index := cut.certificates[group].sourceIndex + 1; index <= completeCut.certificates[group].sourceIndex; index++ {
				want, err := leader.machine.store.ApplicationRecord(t.Context(), index, false, leader.machine.store.ApplicationLimits().MaxChangeBytes)
				if err != nil {
					t.Fatal("original snapshot-to-envelope start has a gap", group, index, err)
				}
				got, err := follower.machine.store.ApplicationRecord(t.Context(), index, false, follower.machine.store.ApplicationLimits().MaxChangeBytes)
				if err != nil || !bytes.Equal(got, want) {
					t.Fatal("publication/AS5 lost exact retained envelope prefix", group, member, index, err)
				}
			}
		}
	}
	oldEdge, _ := partitionProjectionFromView(t, h, captured.view, 3, graphGenesisBinding(t).Axis())
	current, closeCurrent := crossView(t, n, 2, [2]uint64{completeCut.certificates[0].sourceIndex, completeCut.certificates[1].sourceIndex})
	defer closeCurrent()
	point, err := temporal.RationalPosition(graphGenesisBinding(t).Axis(), temporal.RationalInt64(5))
	if err != nil {
		t.Fatal(err)
	}
	newEdge, err := graphstate.Project(t.Context(), current, 3, point, graphstate.Effective, h.machine.limits.graph.Planner)
	if err != nil {
		t.Fatal("completed native cut must cover both relationship endpoints", err)
	}
	if oldEdge.Exists || !newEdge.Exists {
		t.Fatal("mutation-then-old-cut changed graph knowledge during publication/GC", oldEdge, newEdge)
	}
	if err := server.closeSource(published.info.cookie); err != nil {
		t.Fatal(err)
	}
	if _, err := captured.view.Root(); !errors.Is(err, raftlog.ErrClosed) {
		t.Fatal("explicit lease release retained its native bank reference", err)
	}
	extra, err := g.stores[2].BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal("last captured owner release did not free obsolete physical bank", err)
	}
	if err := extra.Abort(); err != nil {
		t.Fatal(err)
	}
	if g.stores[2].ApplicationGeneration() != 2 {
		t.Fatal("physical GC changed active generation or graph history")
	}
	reconstructed, err := h.AtGraphCertificate(t.Context(), cut.certificates[0])
	if err != nil {
		t.Fatal("physical GC removed durable old certificate/root lineage", err)
	}
	defer reconstructed.Close()
	node, _ := partitionProjectionFromView(t, h, reconstructed, 1, graphGenesisBinding(t).Axis())
	edge, _ := partitionProjectionFromView(t, h, reconstructed, 3, graphGenesisBinding(t).Axis())
	if !node.Exists || !node.Active || edge.Exists {
		t.Fatal("reconstructed old cut silently returned current or empty graph", node, edge)
	}
}
