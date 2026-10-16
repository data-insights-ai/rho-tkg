package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func rangeMoveAdmissionOwner(t *testing.T, h *allocationHost) raftlog.ApplicationRecoveryOwner {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	// RecoveryOwner validates/hashes the one native recovery frame plus its
	//24-byte credits. The native metadata ceiling is64KiB; six passes include
	//frame/key/fixed bookkeeping. This is a bounded test observation grant,
	//clamped to the unchanged owning store policy, never a production cap.
	owner, _, err := view.RecoveryOwner(t.Context(), raftlog.ReadBudget{Rows: 1, Bytes: min(6*(64<<10), view.ReadLimits().Bytes)})
	err = errors.Join(err, view.Close())
	if err != nil {
		t.Fatal("actual native recovery owner", err)
	}
	return owner
}

func rangeMoveAdmissionRead(t *testing.T, n *allocationTestNetwork, group int, move [16]byte) rangeMoveAuthority {
	t.Helper()
	h := n.hosts[group][0]
	pump := nativeGraphTestPump{n: n, group: group, host: h}
	q := rangeMoveQuery{move: move, nonce: [16]byte{61}, sourceRows: 512, sourceBytes: 4 << 20, outputBytes: 8 << 20}
	reply, err := pump.Move(t.Context(), q)
	if err != nil || reply.err != nil || reply.question != q {
		t.Fatal("genuine completed move read", err, reply.err)
	}
	authority, err := h.exportRangeMoveAuthority(reply.proof)
	if err != nil || !authority.validSeal() {
		t.Fatal("genuine completed move export", err)
	}
	return authority
}

func rangeMoveAdmissionSubmit(t *testing.T, n *allocationTestNetwork, group int, intent []byte, phase rangeMovePhase, sequence uint64, data []byte, evidence []rangeMoveAuthority) rangeMoveStatus {
	t.Helper()
	output, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	partition := n.hosts[group][0].machine.ns.partition
	proposal, err := n.hosts[0][0].prepareRangeMoveCommand(intent, phase, partition, sequence, data, evidence, output)
	if err != nil {
		t.Fatal("native source-owned proposal", err)
	}
	pump := nativeGraphTestPump{n: n, group: group, host: n.hosts[group][0]}
	wire, err := pump.Submit(t.Context(), proposal)
	if err != nil {
		t.Fatal("actual native phase commit", err)
	}
	status, err := decodeRangeMoveStatus(wire)
	if err != nil {
		t.Fatal(err)
	}
	return status
}

func TestRangeMoveAdmissionWrongSourcePlacementRefusesWithoutRecoveryOwner(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, tc := range []struct {
		name string
		edit func(*rangeMoveIntent)
	}{
		{"wrong-current-epoch", func(r *rangeMoveIntent) { r.epoch++ }},
		{"changed-publication", func(r *rangeMoveIntent) { r.publication.Last++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, _, intent, _, _ := rangeMoveCoordinatorFixture(t, 8)
			intent.pageBytes = 8 << 10
			tc.edit(&intent)
			wire, err := encodeRangeMoveIntent(intent)
			if err != nil {
				t.Fatal("structurally valid adversarial request", err)
			}
			status := rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveFence, 0, nil, nil)
			if status.phase != rangeMoveFence || status.result != rangeMoveMismatch {
				t.Fatal("genuine current placement mismatch was not refused", status)
			}
			for _, h := range n.hosts[0] {
				if owner := rangeMoveAdmissionOwner(t, h); owner != (raftlog.ApplicationRecoveryOwner{}) {
					t.Fatal("refused source Fence orphaned native recovery reservation", owner)
				}
			}
			// Refusal must preserve healthy ordinary writes and durable reopen;
			// seeing only a mismatch outcome is insufficient admission evidence.
			window, err := temporal.All(graphGenesisBinding(t).Axis())
			if err != nil {
				t.Fatal(err)
			}
			proposal, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: "after-refusal"}}, 62, nil)
			if err != nil {
				t.Fatal("refused Fence stranded healthy source write", err)
			}
			n.submitPartitionGraph(0, proposal, reasonNone)
			crossReopenAll(t, n)
			for member := range 3 {
				if owner := rangeMoveAdmissionOwner(t, n.hosts[0][member]); owner != (raftlog.ApplicationRecoveryOwner{}) {
					t.Fatal("reopen restored orphaned refused admission", owner)
				}
				view, close := crossView(t, n, member, [2]uint64{})
				assertCertifiedProjection(t, view, 1, true, []string{"after-refusal"})
				assertCertifiedProjection(t, view, 17, true, nil)
				close()
			}
		})
	}
}

// This gate drops the next target import before delivery. All earlier pages
// are actual native commits; it cannot forge progress, an undo receipt or vote.
type rangeMoveAdmissionImportGate struct {
	graphPeerWireTransport
	sequence uint64
	stopped  bool
}

func (w *rangeMoveAdmissionImportGate) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceSubmit {
		proposal, err := decodeGraphSourceProposal(r.command)
		if err != nil {
			return nil, err
		}
		command, err := decodeRangeMoveCommand(proposal.wire)
		if err != nil {
			return nil, err
		}
		if command.phase == rangeMoveImport && command.sequence == w.sequence {
			w.stopped = true
			return nil, io.EOF
		}
	}
	return w.graphPeerWireTransport.RoundTrip(ctx, wire)
}

func rangeMoveAdmissionReservation(t *testing.T, h *allocationHost) raftlog.ApplicationRecoveryReservation {
	t.Helper()
	s := h.machine.store
	cut, err := s.PublishedApplicationCut()
	if err != nil && !errors.Is(err, raftlog.ErrInvalid) {
		t.Fatal(err)
	}
	// A snapshot is already genuine at this exact applied coordinate; repeated
	// diagnostic observations need no second publication of the same index.
	if err != nil || cut.Index != h.driver.Applied() {
		if err := s.PublishSnapshot(); err != nil {
			t.Fatal(err)
		}
		cut, err = s.PublishedApplicationCut()
		if err != nil {
			t.Fatal(err)
		}
	}
	export, err := s.BeginPublishedApplicationExport(t.Context(), cut.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := export.Close(); err != nil {
			t.Error(err)
		}
	}()
	limits := s.ApplicationLimits()
	for range 256 {
		done, err := export.BuildManifest(t.Context(), raftlog.ReadBudget{Rows: min(64, limits.MaxPageRows), Bytes: min(256<<10, limits.MaxPageBytes)})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			manifest, err := export.Manifest()
			if err != nil || manifest.Index != h.driver.Applied() || manifest.Version != 5 {
				t.Fatal("actual AS5 reservation inspection", err)
			}
			return manifest.RecoveryReservation
		}
	}
	t.Fatal("bounded actual reservation inspection did not complete")
	return raftlog.ApplicationRecoveryReservation{}
}

func rangeMoveAdmissionAssertUndoCredits(t *testing.T, reservation raftlog.ApplicationRecoveryReservation, move [16]byte, ns namespace, intent []byte, undone uint32) {
	t.Helper()
	if reservation.ID != rangeMoveRecoveryID(ns, move) || reservation.Digest != sha256.Sum256(intent) {
		t.Fatal("undo lost its original recovery owner", reservation.ID)
	}
	// Target capture, Import0, Prep1 and Page2 have completed. Each numbered
	// undo consumes only its own phase; all later undo/final credits survive.
	decoded, err := decodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	maxPages := uint32(decoded.maxPages)
	want := make([]uint32, 0, 2*decoded.maxPages+8)
	for phase := uint32(1203); phase <= 1200+maxPages; phase++ {
		want = append(want, phase)
	}
	want = append(want, 2500, 2501, 2502)
	for phase := uint32(3000) + undone; phase < 3000+maxPages; phase++ {
		want = append(want, phase)
	}
	want = append(want, 5001)
	got := make([]uint32, len(reservation.Credits))
	for i, credit := range reservation.Credits {
		got[i] = credit.Phase
	}
	if !slices.Equal(got, want) {
		t.Fatal("numbered undo retired unused completion phases", got, want)
	}
}

func TestRangeMoveAdmissionNumberedUndoRetainsCreditsUntilFinalAbort(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	intent.pageBytes = 8 << 10
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: transports[8], sequence: 3}
	transports[8] = gate
	c, err := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, c, intent)
	if !gate.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) {
		t.Fatal("genuine target staging did not reach import pause", pending, err)
	}
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		t.Fatal(err)
	}
	move := pending.resume.move
	target := rangeMoveAdmissionRead(t, n, 1, move)
	if target.observation.status.phase != rangeMoveImport || target.observation.status.sequence != 2 {
		t.Fatal("pause replaced actual durable import progress", target.observation.status)
	}
	output, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	progress, err := decodeRangeMoveProgress(target.observation.progress, output)
	if err != nil || progress.imported == nil || !progress.imported.PreparationComplete || progress.imported.Sequence != 1 || progress.imported.Rows == 0 {
		t.Fatal("no genuine graph page staged before undo", progress, err)
	}
	targetHost := n.hosts[1][0]
	view, err := targetHost.machine.store.ApplicationView(targetHost.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	receipt, found, _, err := view.GetControl(t.Context(), rangeMoveUndoControlKey(targetHost.machine.ns, move, 2), raftlog.ReadBudget{Rows: 2, Bytes: 64 << 10})
	err = errors.Join(err, view.Close())
	if err != nil || !found {
		t.Fatal("actual staged graph page omitted reciprocal undo receipt", err)
	}
	previous, err := decodeRangeMoveUndo(receipt.Value)
	if err != nil || len(previous) == 0 {
		t.Fatal("actual staged graph page had no inverse writes", err)
	}
	rangeMoveAdmissionAssertUndoCredits(t, rangeMoveAdmissionReservation(t, targetHost), move, targetHost.machine.ns, wire, 0)
	status := rangeMoveAdmissionSubmit(t, n, 0, wire, rangeMoveAbort, 0, nil, nil)
	if status.phase != rangeMoveAbort || status.sequence != 0 || status.result != rangeMoveApplied {
		t.Fatal("actual source pre-COMMIT abort", status)
	}
	sourceAbort := rangeMoveAdmissionRead(t, n, 0, move)
	for sequence := uint64(1); sequence <= 2; sequence++ {
		status := rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, sequence, nil, []rangeMoveAuthority{sourceAbort})
		if status.phase != rangeMoveAbort || status.sequence != sequence || status.result != rangeMoveApplied {
			t.Fatal("numbered native undo did not commit", status)
		}
		for _, h := range n.hosts[1] {
			owner := rangeMoveAdmissionOwner(t, h)
			if owner.ID != rangeMoveRecoveryID(h.machine.ns, move) || owner.Digest != sha256.Sum256(wire) {
				t.Fatal("numbered target ABORT released recovery before final0", sequence, owner)
			}
		}
		rangeMoveAdmissionAssertUndoCredits(t, rangeMoveAdmissionReservation(t, targetHost), move, targetHost.machine.ns, wire, uint32(sequence))
		if sequence == 1 {
			// Restart in the numbered, nonterminal ABORT phase. A generic
			// phase==Abort terminal shortcut would lose the real owner/credits
			// or refuse restored state before the next reverse receipt.
			crossReopenAll(t, n)
			targetHost = n.hosts[1][0]
			sourceAbort = rangeMoveAdmissionRead(t, n, 0, move)
			for _, h := range n.hosts[1] {
				owner := rangeMoveAdmissionOwner(t, h)
				if owner.ID != rangeMoveRecoveryID(h.machine.ns, move) || owner.Digest != sha256.Sum256(wire) {
					t.Fatal("reopen lost nonterminal target undo owner", owner)
				}
			}
			rangeMoveAdmissionAssertUndoCredits(t, rangeMoveAdmissionReservation(t, targetHost), move, targetHost.machine.ns, wire, 1)
		}
	}
	view, err = targetHost.machine.store.ApplicationView(targetHost.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range previous {
		actual, found, err := view.Get(t.Context(), row.Key, 64<<10)
		logicalPresent := found && !actual.Deleted
		if err != nil || logicalPresent == row.Deleted || logicalPresent && !bytes.Equal(actual.Value, row.Value) {
			t.Fatal("reverse receipt did not restore exact original graph row", found, row.Deleted, err)
		}
	}
	if err := view.Close(); err != nil {
		t.Fatal(err)
	}
	status = rangeMoveAdmissionSubmit(t, n, 1, wire, rangeMoveAbort, 0, nil, []rangeMoveAuthority{sourceAbort})
	if status.phase != rangeMoveAbort || status.sequence != 0 || status.result != rangeMoveApplied {
		t.Fatal("native final target abort did not finish", status)
	}
	for _, h := range n.hosts[1] {
		if owner := rangeMoveAdmissionOwner(t, h); owner != (raftlog.ApplicationRecoveryOwner{}) {
			t.Fatal("final target Abort0 retained native recovery owner", owner)
		}
	}
	if reservation := rangeMoveAdmissionReservation(t, targetHost); reservation.ID != ([32]byte{}) || len(reservation.Credits) != 0 {
		t.Fatal("final target Abort0 retained unused recovery credits", reservation.ID)
	}
	crossReopenAll(t, n)
	for member := range 3 {
		view, close := crossView(t, n, member, [2]uint64{})
		assertCertifiedProjection(t, view, 1, true, nil)
		assertCertifiedProjection(t, view, 17, true, nil)
		close()
	}
	// A genuinely new source request may move the same range after the old
	// request durably aborted. Native tombstones are logical absence for undo;
	// they must not poison the new target's fenced preparation.
	transports = make(map[uint64]graphPeerWireTransport)
	for group, partition := range []uint64{3, 8} {
		h := n.hosts[group][0]
		server, e := newGraphSourceServer(t.Context(), h, defaultGraphSourceLimits(h.machine.limits))
		if e != nil {
			t.Fatal(e)
		}
		service, e := newGraphPeerService(h, server, &nativeGraphTestPump{n: n, group: group, host: h})
		if e != nil {
			t.Fatal(e)
		}
		transports[partition] = &nativeGraphTestTransport{service: service}
	}
	fresh, e := newRangeMoveCoordinator(n.hosts[0][0], transports)
	if e != nil {
		t.Fatal(e)
	}
	intent.id = n.requestID()
	done, e := runRangeMoveToCompletion(t, fresh, intent)
	if e != nil || !done.committed {
		t.Fatal("new handoff after durable abort/reopen treated native tombstone as corruption", done, e)
	}
}
