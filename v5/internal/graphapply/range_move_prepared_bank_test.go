package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// A drop occurs before the real source Decide reaches Host, after the real
// target Seal. No outcome, snapshot, proof or progress is fabricated.
type preparedMoveDecisionGate struct {
	graphPeerWireTransport
	stopped bool
}

func (g *preparedMoveDecisionGate) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	request, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if request.kind == sourceSubmit {
		proposal, err := decodeGraphSourceProposal(request.command)
		if err != nil {
			return nil, err
		}
		command, err := decodeRangeMoveCommand(proposal.wire)
		if err != nil {
			return nil, err
		}
		if command.phase == rangeMoveDecide {
			g.stopped = true
			return nil, io.EOF
		}
	}
	return g.graphPeerWireTransport.RoundTrip(ctx, wire)
}

// The fresh cross request is valid, never submitted and owns no reservation.
// The completed request has genuine votes/COMMIT/Resolve and must retain its
// original no-purpose replay. Prepared incoming state governs both classifiers;
// the recipient's divergent active bank supplies neither baseline nor authority.
func TestPreparedRangeMoveCrossRecoveryUsesIncomingBankNotActiveBaseline(t *testing.T) {
	for _, phase := range []string{"import", "seal", "numbered-abort"} {
		t.Run(phase, func(t *testing.T) {
			n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
			window, err := temporal.All(graphGenesisBinding(t).Axis())
			if err != nil {
				t.Fatal(err)
			}
			completed, replay := crossPlanAt(n, 1, n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: "completed-before-move"}}, nil, 24)
			registered := n.crossSubmit(1, replay)
			registration := n.crossReadPhase(1, registered.tx, crossRegister)
			var votes []crossProof
			for group, partition := range []uint64{3, 8} {
				proposal, err := n.hosts[1][0].PrepareCrossParticipant(completed, partition, registration, votes)
				if err != nil {
					t.Fatal(err)
				}
				vote := n.crossSubmit(group, proposal)
				if !vote.yes {
					t.Fatal("genuine completed replay fixture voted NO", vote)
				}
				votes = append(votes, n.crossReadPhase(group, registered.tx, crossPrepare))
			}
			decisionProposal, err := n.hosts[1][0].DecideCrossTransaction(completed, votes)
			if err != nil {
				t.Fatal(err)
			}
			decisionStatus := n.crossSubmit(1, decisionProposal)
			if !decisionStatus.commit {
				t.Fatal("genuine completed replay fixture did not COMMIT", decisionStatus)
			}
			decision := n.crossReadPhase(1, registered.tx, crossDecide)
			crossResolveAt(n, 1, decision, 0, 1)
			_, fresh := crossPlanAt(n, 1, n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: "fresh-during-move"}}, nil, 25)
			if !isCrossPartitionWire(fresh.wire) || !isCrossPartitionWire(replay.wire) {
				t.Fatal("fixture did not produce valid native cross commands")
			}
			intent.pageBytes = 8 << 10
			importGate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 3}
			decisionGate := &preparedMoveDecisionGate{graphPeerWireTransport: transports[3]}
			if phase == "seal" {
				transports[3] = decisionGate
			} else {
				transports[8] = importGate
			}
			coordinator, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := runRangeMoveToCompletion(t, coordinator, intent)
			if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed || phase == "seal" && !decisionGate.stopped || phase != "seal" && !importGate.stopped {
				t.Fatal("native move did not pause after actual target staging", pending, err)
			}
			move := pending.resume.move
			intentWire, err := encodeRangeMoveIntent(intent)
			if err != nil {
				t.Fatal(err)
			}
			if phase == "numbered-abort" {
				status := rangeMoveAdmissionSubmit(t, n, 0, intentWire, rangeMoveAbort, 0, nil, nil)
				if status.phase != rangeMoveAbort || status.sequence != 0 || status.result != rangeMoveApplied {
					t.Fatal("source abort was not actual native state", status)
				}
				sourceAbort := rangeMoveAdmissionRead(t, n, 0, move)
				status = rangeMoveAdmissionSubmit(t, n, 1, intentWire, rangeMoveAbort, 1, nil, []rangeMoveAuthority{sourceAbort})
				if status.phase != rangeMoveAbort || status.sequence != 1 || status.result != rangeMoveApplied || rangeMoveTerminal(status) {
					t.Fatal("numbered undo did not retain actual nonterminal state", status)
				}
			}
			donor := n.hosts[1][0]
			observed := rangeMoveAdmissionRead(t, n, 1, move)
			status := observed.observation.status
			wantPhase := rangeMoveImport
			switch phase {
			case "seal":
				wantPhase = rangeMoveSeal
			case "numbered-abort":
				wantPhase = rangeMoveAbort
			}
			if status.phase != wantPhase || status.result != rangeMoveApplied || rangeMoveTerminal(status) || phase == "numbered-abort" && status.sequence != 1 {
				t.Fatal("snapshot source did not retain requested real phase", status)
			}
			prepared, recipient, machine, manifest := preparedMoveIncomingFixture(t, n, donor)
			view, err := prepared.OpenReadView(t.Context(), manifest.Index, 172)
			if err != nil {
				t.Fatal(err)
			}
			defer view.Close()
			base, err := view.RootBounded(t.Context(), 172)
			if err != nil || base.Generation == recipient.ApplicationGeneration() || base.Index != manifest.Index || base.ImageHash != manifest.ImageHash || !bytes.Equal(base.Image, manifest.Image) {
				t.Fatal("not a genuine dormant incoming-bank view", base, err)
			}
			actual, found, err := view.Get(t.Context(), rangeMoveStatusKey(machine.ns, move), 4096)
			if err != nil || !found {
				t.Fatal(err)
			}
			importedStatus, err := decodeRangeMoveStatus(actual.Value)
			if err != nil || importedStatus != status {
				t.Fatal("AS5 changed exact staged/undo status", importedStatus, status, err)
			}
			capture, found, _, err := view.GetControl(t.Context(), rangeMoveControlKey(machine.ns, move, 'C'), raftlog.ReadBudget{Rows: 2, Bytes: 16 << 10})
			if err != nil || !found {
				t.Fatal(err)
			}
			captured, _, err := moveCaptureRoot(capture.Value, intent.target)
			if err != nil {
				t.Fatal(err)
			}
			activeIndex, activeImage, err := recipient.Checkpoint()
			if err != nil || activeIndex >= captured.Index || bytes.Equal(activeImage, captured.Image) {
				t.Fatal("recipient active bank is not divergent from actual incoming capture", activeIndex, captured.Index, err)
			}
			owner, _, err := view.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 2, Bytes: 16 << 10})
			if err != nil || owner.ID != rangeMoveRecoveryID(machine.ns, move) || owner.Digest != sha256.Sum256(intentWire) {
				t.Fatal("incoming AS5 lost genuine move recovery ownership", owner, err)
			}
			before, err := recipient.ApplicationTransferUsage()
			if err != nil {
				t.Fatal(err)
			}
			for _, tc := range []struct {
				name string
				wire []byte
				want error
			}{{"fresh-register", fresh.wire, errRangeMovePending}, {"completed-exact-register-replay", replay.wire, nil}} {
				t.Run(tc.name, func(t *testing.T) {
					purpose, err := machine.VerifyApplicationRecovery(t.Context(), tc.wire, view)
					if tc.want == nil && err != nil || tc.want != nil && !errors.Is(err, tc.want) || purpose.Reserve != nil || purpose.Use != nil {
						t.Fatal("prepared move cross classification reopened recipient active bank or granted recovery", phase, err, tc.want, purpose)
					}
				})
			}
			// Range classification already borrows incoming state. Its exact Fence
			// retry must remain a zero-purpose counterpart to the cross door.
			fence, err := encodeRangeMoveCommand(rangeMoveCommand{scope: intent.target, phase: rangeMoveFence, intent: intentWire})
			if err != nil {
				t.Fatal(err)
			}
			purpose, err := machine.VerifyApplicationRecovery(t.Context(), fence, view)
			if err != nil || purpose.Reserve != nil || purpose.Use != nil {
				t.Fatal("same-move Fence retry did not use incoming native status", err, purpose)
			}
			after, err := recipient.ApplicationTransferUsage()
			if err != nil || before != after {
				t.Fatal("classification changed incoming-bank ownership/pins", before, after, err)
			}
			afterIndex, afterImage, err := recipient.Checkpoint()
			if err != nil || afterIndex != activeIndex || !bytes.Equal(afterImage, activeImage) {
				t.Fatal("classification changed or poisoned divergent active bank", afterIndex, err)
			}
			if err := view.Close(); err != nil {
				t.Fatal(err)
			}
			if err := prepared.Close(); err != nil {
				t.Fatal(err)
			}
			if usage, err := recipient.ApplicationTransferUsage(); err != nil || usage.Import || usage.Prepared != 0 || usage.Claims != 0 || usage.PinnedLogicalBytes != 0 {
				t.Fatal("genuine incoming-bank guard leaked snapshot ownership", usage, err)
			}
		})
	}
}

func preparedMoveIncomingFixture(t *testing.T, n *allocationTestNetwork, donor *allocationHost) (*raftlog.PreparedApplicationSnapshot, *raftlog.Store, *declaredMaterializer, raftlog.ApplicationSnapshotManifest) {
	t.Helper()
	store := donor.machine.store
	if err := store.PublishSnapshot(); err != nil {
		t.Fatal(err)
	}
	cut, err := store.PublishedApplicationCut()
	if err != nil {
		t.Fatal(err)
	}
	export, err := store.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer export.Close()
	ready := false
	for range 256 {
		ready, err = export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if ready {
			break
		}
	}
	if !ready {
		t.Fatal("bounded AS5 move manifest did not finish")
	}
	manifest, err := export.Manifest()
	if err != nil || manifest.Version != 5 || manifest.Contract.Version != 3 || manifest.Index != donor.driver.Applied() || manifest.SemanticContractID != rangeMoveSemanticContractID() {
		t.Fatal("not actual AS5 protected move evidence", manifest, err)
	}
	configuration := n.configs[1][2]
	configuration.Dir = t.TempDir()
	configuration.Create = true
	recipient, err := raftlog.Open(configuration)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := recipient.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := graphstore.BootstrapBoundOwnership(recipient, recipient.ApplicationBinding(), n.d, [3]uint64{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	machine, err := newRangeMoveMaterializer(recipient, donor.machine.ns, n.d, donor.machine.limits)
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := recipient.BeginApplicationImport(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := incoming.Abort(); err != nil {
			t.Error(err)
		}
	})
	complete := false
	for range 256 {
		chunk, err := export.Next(t.Context(), raftlog.ReadBudget{Rows: 64, Bytes: 256 << 10})
		if err != nil {
			t.Fatal(err)
		}
		if err := incoming.Append(t.Context(), chunk); err != nil {
			t.Fatal(err)
		}
		if chunk.Final {
			complete = true
			break
		}
	}
	if !complete {
		t.Fatal("bounded AS5 move stream did not finish")
	}
	if err := incoming.Verify(t.Context()); err != nil {
		t.Fatal(err)
	}
	prepared, err := incoming.Prepare()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := prepared.Close(); err != nil {
			t.Error(err)
		}
	})
	return prepared, recipient, machine, manifest
}
