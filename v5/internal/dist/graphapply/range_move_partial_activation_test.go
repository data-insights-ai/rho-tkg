package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

type rangeMovePartialActivationWire struct {
	*nativeGraphTestTransport
	dropSourceActivate, commitBeforeDrop, stopped bool
	captures                                      int
}

func (w *rangeMovePartialActivationWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceSubmit {
		p, err := decodeGraphSourceProposal(r.command)
		if err != nil {
			return nil, err
		}
		if isRangeMoveWire(p.wire) {
			command, err := decodeRangeMoveCommand(p.wire)
			if err != nil {
				return nil, err
			}
			if command.phase == rangeMoveCapture {
				w.captures++
			}
			if command.phase == rangeMoveActivate && w.dropSourceActivate && !w.stopped {
				if w.commitBeforeDrop {
					if _, err := w.nativeGraphTestTransport.RoundTrip(ctx, wire); err != nil {
						return nil, err
					}
				}
				w.stopped = true
				return nil, io.EOF
			}
		}
	}
	return w.nativeGraphTestTransport.RoundTrip(ctx, wire)
}

type rangeMovePartialFacts struct {
	identity      [2][]byte
	cells         [6][]byte
	afterRevision uint64
}

func rangeMovePartialObserve(t *testing.T, read *certifiedGraphRead, changed bool) rangeMovePartialFacts {
	t.Helper()
	view, err := read.ReadView()
	if err != nil {
		t.Fatal(err)
	}
	var result rangeMovePartialFacts
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []graphstate.EntityID{1, 17} {
		labels := []string(nil)
		if id == 1 {
			labels = []string{"before-handoff"}
			if changed {
				labels = []string{"after-handoff", "before-handoff"}
			}
		}
		assertCertifiedProjection(t, view, id, true, labels)
		projection, err := graphstate.Project(t.Context(), view, id, certifiedPoint(t), graphstate.Effective, defaultMaterializerLimits().graph.Planner)
		if err != nil || !projection.Exists || projection.Life != graphstate.LifeID(id+1) {
			t.Fatal("stable actual identity/life", err)
		}
		r := projection.Record
		result.identity[i], err = processEncoding(func(w *boundedWriter) {
			w.u64(uint64(r.ID))
			w.tag(byte(r.Kind))
			w.tag(byte(r.Mode))
			w.tag(byte(r.Interpretation))
			w.tag(byte(r.TemporalRole))
			w.text(r.Type)
			w.u64(uint64(r.Source))
			w.u64(uint64(r.Target))
			axis := r.Axis.DefinitionHash()
			w.add(axis[:])
			w.u64(uint64(projection.Life))
		})
		if err != nil {
			t.Fatal(err)
		}
		keys := []graphstate.ComponentKey{{Owner: id, Kind: graphstate.Presence}, {Owner: id, Life: graphstate.LifeID(id + 1), Kind: graphstate.Label, Name: "before-handoff"}, {Owner: id, Life: graphstate.LifeID(id + 1), Kind: graphstate.Label, Name: "after-handoff"}}
		for j, key := range keys {
			page, err := view.ComponentPage(t.Context(), graphstate.ComponentQuery{Key: key, Window: window}, 0, raftlog.ReadBudget{Rows: 64, Bytes: 64 << 10})
			if err != nil || !page.Complete || page.Next != 0 {
				t.Fatal("complete actual old/current cells", err)
			}
			if changed && i == 0 && j == 2 {
				cell, err := page.Data.At(certifiedPoint(t), defaultMaterializerLimits().graph.Planner.Component)
				if err != nil || !cell.Present() || cell.Revision().Provenance() != 162 {
					t.Fatal("actual target mutation cell/revision", err)
				}
				result.afterRevision = cell.Revision().ID()
			}
			result.cells[3*i+j], err = state.AppendState(nil, page.Data, state.CodecLimits{State: defaultMaterializerLimits().graph.Planner.Component, MaxEncodedBytes: 64 << 10})
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if phantom, err := view.Entity(t.Context(), 7); err != nil || phantom.Found {
		t.Fatal("phantom entity after partial activation", err)
	}
	return result
}

func rangeMovePartialPlacement(t *testing.T, h *allocationHost, intent rangeMoveIntent) (graphstore.RangeOwnership, []byte) {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := view.Close(); err != nil {
			t.Error(err)
		}
	}()
	publication, owner, _, err := graphstore.LookupPublishedRange(t.Context(), view, graphstore.Namespace{Graph: graphstate.GraphID(h.machine.ns.graph), Partition: h.machine.ns.partition}, intent.publication.First, intent.publication.Configuration, graphstore.OwnershipBudget{SourceRows: 64, SourceBytes: 1 << 20, OutputBytes: 1 << 20})
	if err != nil || publication != intent.publication {
		t.Fatal("actual stable range publication", err)
	}
	root, err := view.RootBounded(t.Context(), 172)
	if err != nil {
		t.Fatal(err)
	}
	return owner, root.Image
}

func rangeMovePartialAssertOwner(t *testing.T, h *allocationHost, move [16]byte, hash [32]byte, needsActivate bool) raftlog.ApplicationRecoveryReservation {
	t.Helper()
	q := rangeMoveReplayNativeReader(t, h, 16<<20)
	active, err := readRangeMoveActive(q)
	if closeErr := q.view.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || active != move {
		t.Fatal("partial activation released admission fence", active, err)
	}
	owner := rangeMoveAdmissionOwner(t, h)
	reservation := rangeMoveAdmissionReservation(t, h)
	if owner.ID != rangeMoveRecoveryID(h.machine.ns, move) || owner.Digest != hash || reservation.ID != owner.ID || reservation.Digest != hash {
		t.Fatal("partial activation lost original protected owner", owner)
	}
	activate, finish := false, false
	for _, credit := range reservation.Credits {
		if credit.Phase == 2502 {
			activate = true
		}
		if credit.Phase == 5001 {
			finish = true
		}
	}
	if !finish || needsActivate && !activate {
		t.Fatal("partial activation lost reserved completion credits")
	}
	return reservation
}

func TestRangeMovePartialActivationRestoresOriginalCommitAndOldCertifiedCut(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, mode := range []string{"source-not-delivered", "source-committed-lost-reply"} {
		t.Run(mode, func(t *testing.T) {
			n, _, intent, fixtureWires, _ := rangeMoveCoordinatorFixture(t, 64)
			wires := []*rangeMovePartialActivationWire{{nativeGraphTestTransport: fixtureWires[0].nativeGraphTestTransport, dropSourceActivate: true, commitBeforeDrop: mode == "source-committed-lost-reply"}, {nativeGraphTestTransport: fixtureWires[1].nativeGraphTestTransport}}
			transports := map[uint64]graphPeerWireTransport{3: wires[0], 8: wires[1]}
			base, err := newCrossProcessCoordinator(n.hosts[0][0], transports)
			if err != nil {
				t.Fatal(err)
			}
			coordinator, err := newRangeMoveCoordinatorOn(base)
			if err != nil {
				t.Fatal(err)
			}
			window, err := temporal.All(graphGenesisBinding(t).Axis())
			if err != nil {
				t.Fatal(err)
			}
			p, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "before-handoff"}}, 161, nil)
			if err != nil {
				t.Fatal(err)
			}
			n.submitPartitionGraph(0, p, reasonNone)
			old, err := base.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
			if err != nil {
				t.Fatal("actual old certified reader", err)
			}
			defer func() {
				if err := old.Close(); err != nil {
					t.Error(err)
				}
			}()
			cut, err := old.Cut()
			if err != nil {
				t.Fatal(err)
			}
			oldFacts := rangeMovePartialObserve(t, old, false)
			oldOwner, _ := rangeMovePartialPlacement(t, n.hosts[0][0], intent)
			var oldRoots [2][]byte
			for group := range 2 {
				_, oldRoots[group], err = n.hosts[group][0].machine.store.Checkpoint()
				if err != nil {
					t.Fatal(err)
				}
			}
			intentWire, err := encodeRangeMoveIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := runRangeMoveToCompletion(t, coordinator, intent)
			if !wires[0].stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed || pending.resume.intent != sha256.Sum256(intentWire) {
				t.Fatal("actual source Activate seam not reached", pending, err)
			}
			captures := wires[0].captures + wires[1].captures
			if captures != 2 {
				t.Fatal("fixture did not capture exactly once per group", captures)
			}
			wantSource := rangeMoveDecide
			if mode == "source-committed-lost-reply" {
				wantSource = rangeMoveActivate
			}
			var observed [2]rangeMoveAuthority
			var reservations [6]raftlog.ApplicationRecoveryReservation
			for group := range 2 {
				observed[group] = rangeMoveAdmissionRead(t, n, group, pending.resume.move)
				want := rangeMoveActivate
				if group == 0 {
					want = wantSource
				}
				if observed[group].observation.status.phase != want || observed[group].observation.decision.phase != rangeMoveDecide || observed[group].observation.decision.result != rangeMoveApplied {
					t.Fatal("phase was not genuine partial activation", group, observed[group].observation.status)
				}
				for member, h := range n.hosts[group] {
					reservations[3*group+member] = rangeMovePartialAssertOwner(t, h, pending.resume.move, pending.resume.intent, group == 0 && wantSource == rangeMoveDecide)
				}
			}
			if observed[0].observation.decision != observed[1].observation.decision {
				t.Fatal("target did not retain exact immutable source COMMIT")
			}
			for group := range 2 {
				owner, image := rangeMovePartialPlacement(t, n.hosts[group][0], intent)
				if group == 0 && wantSource == rangeMoveDecide {
					if owner != oldOwner || !bytes.Equal(image, oldRoots[0]) {
						t.Fatal("undelivered source Activate published new owner/root", owner)
					}
				} else if owner.Partition != 8 || owner.Epoch != intent.epoch+1 || owner.Fenced || bytes.Equal(image, oldRoots[group]) {
					t.Fatal("actual activation omitted placement/new root", group, owner)
				}
			}
			if fresh, err := base.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil); fresh != nil || !errors.Is(err, errGraphCutPending) {
				t.Fatal("partial activation exposed a mixed Fresh cut", err)
			}
			if !reflect.DeepEqual(rangeMovePartialObserve(t, old, false), oldFacts) {
				t.Fatal("partial activation changed genuinely held old cut")
			}
			at, err := base.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
			if err != nil {
				t.Fatal("real retained At during partial activation", err)
			}
			if !reflect.DeepEqual(rangeMovePartialObserve(t, at, false), oldFacts) {
				t.Fatal("partial activation replaced retained old cells/revisions")
			}
			if err := at.Close(); err != nil {
				t.Fatal(err)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			// Store closure ends live handles. Durable original certificates and
			// move controls are reopened on NEW actual Hosts/services, not reused.
			crossReopenAll(t, n)
			for group, partition := range []uint64{3, 8} {
				h := n.hosts[group][0]
				server, err := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
				if err != nil {
					t.Fatal(err)
				}
				service, err := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: group, host: h})
				if err != nil {
					t.Fatal(err)
				}
				wires[group].service = service
				actual := rangeMoveAdmissionRead(t, n, group, pending.resume.move)
				if actual.observation.status != observed[group].observation.status || actual.observation.decision != observed[group].observation.decision || !bytes.Equal(actual.observation.intent, observed[group].observation.intent) || !bytes.Equal(actual.observation.progress, observed[group].observation.progress) {
					t.Fatal("Restore changed exact native partial state", partition)
				}
				for member, h := range n.hosts[group] {
					if current := rangeMovePartialAssertOwner(t, h, pending.resume.move, pending.resume.intent, group == 0 && wantSource == rangeMoveDecide); !reflect.DeepEqual(current, reservations[3*group+member]) {
						t.Fatal("Restore changed reserved completion credits", group, member)
					}
				}
			}
			restartedBase, err := newCrossProcessCoordinator(n.hosts[0][0], transports)
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := newRangeMoveCoordinatorOn(restartedBase)
			if err != nil {
				t.Fatal(err)
			}
			done := rangeMoveKeeperResume(t, restarted, pending.resume)
			if done.resume != pending.resume || done.decision != observed[0].observation.decision || wires[0].captures+wires[1].captures != captures {
				t.Fatal("Resume recaptured or changed original immutable COMMIT", done)
			}
			mutation, err := n.hosts[1][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "after-handoff"}}, 162, nil)
			if err != nil {
				t.Fatal(err)
			}
			n.submitPartitionGraph(1, mutation, reasonNone)
			current, err := restartedBase.OpenCertified(t.Context(), graphReadRequest{mode: graphReadFresh}, nil)
			if err != nil {
				t.Fatal(err)
			}
			changed := rangeMovePartialObserve(t, current, true)
			if err := current.Close(); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(changed.identity, oldFacts.identity) || bytes.Equal(changed.cells[2], oldFacts.cells[2]) || changed.afterRevision <= done.decision.round {
				t.Fatal("target mutation failed to distinguish old cut/identity")
			}
			for _, i := range []int{0, 1, 3, 4, 5} {
				if !bytes.Equal(changed.cells[i], oldFacts.cells[i]) {
					t.Fatal("target mutation changed unrelated exact cell", i)
				}
			}
			at, err = restartedBase.OpenCertified(t.Context(), graphReadRequest{mode: graphReadAt, cut: cut}, nil)
			if err != nil {
				t.Fatal("new native services could not reopen original persisted cut", err)
			}
			if !reflect.DeepEqual(rangeMovePartialObserve(t, at, false), oldFacts) {
				t.Fatal("reopened At after restart/mutation substituted current cells/revisions")
			}
			if err := at.Close(); err != nil {
				t.Fatal(err)
			}
			for group := range 2 {
				var root []byte
				for _, h := range n.hosts[group] {
					_, image, err := h.machine.store.Checkpoint()
					if err != nil {
						t.Fatal(err)
					}
					if root == nil {
						root = image
					} else if !bytes.Equal(root, image) {
						t.Fatal("final native group roots diverged")
					}
					q := rangeMoveReplayNativeReader(t, h, 16<<20)
					active, err := readRangeMoveActive(q)
					if closeErr := q.view.Close(); closeErr != nil {
						t.Fatal(closeErr)
					}
					if err != nil || active != ([16]byte{}) || rangeMoveAdmissionOwner(t, h) != (raftlog.ApplicationRecoveryOwner{}) {
						t.Fatal("Finish retained native admission/recovery owner", err)
					}
					reservation := rangeMoveAdmissionReservation(t, h)
					if reservation.ID != ([32]byte{}) || len(reservation.Credits) != 0 {
						t.Fatal("Finish retained completion credits")
					}
				}
			}
		})
	}
}
