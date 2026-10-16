package graphapply

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func TestNativeProtocolPooledStateLeaderAndMessagePrograms(t *testing.T) {
	for _, layout := range []string{"original", "rotated"} {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			spec := nativeProtocolProgramSpec{Version: 1, Layout: layout, Message: message}
			t.Run(layout+"/"+message, func(t *testing.T) {
				for index, recipe := range nativeProtocolStateInventory() {
					// Accepted P08 issuance and its genuine pooled8 observations
					// retain their exact original parents/profile attribution.
					if recipe.Family == "allocation" {
						continue
					}
					if testing.Short() && index != 0 && index != 15 && index != 31 && index != 47 && index != 67 && index != 68 && index != 99 && index != 127 && index != 128 && index != 136 && index != 138 {
						continue
					}
					t.Run(fmt.Sprintf("%s-phase%d-p%d-page%d-kind%d-%s", recipe.Family, recipe.Phase, recipe.Partition, recipe.Page, recipe.Kind, recipe.Side), func(t *testing.T) {
						t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "4")
						artifact, deadline := nativeProtocolStateCaseAt(t, recipe, spec)
						ctx, cancel := nativeProtocolStateCheckerContext(t, deadline)
						defer cancel()
						command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", "native_protocol_campaign_state_program_check.py"), artifact)
						var diagnostics processStderr
						command.Stdout, command.Stderr = &diagnostics, &diagnostics
						if err := command.Run(); err != nil {
							body, _ := diagnostics.state()
							t.Fatal("independent state roles/messages and full semantic oracle", err, body)
						}
						if _, overflow := diagnostics.state(); overflow {
							t.Fatal("bounded state program diagnostics")
						}
					})
				}
			})
		}
	}
}

func TestNativeProtocolStateProgramsKeepNativeIdentityAndCausalWindows(t *testing.T) {
	ctx, cancel := nativeProtocolStateCheckerContext(t, time.Now().Add(-time.Second).UnixNano())
	defer cancel()
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatal("checker reacquired expired source lifetime", ctx.Err())
	}
	// Inert callbacks exercise ownership only; none supplies a native receipt,
	// source proof, term, role mapping or successful business observation.
	h := &graphProcessHub{}
	before, after := 0, 0
	h.beforeRelay = func(context.Context, uint64, uint64, []byte) error { before++; return nil }
	h.afterRelay = func(context.Context, uint64, uint64, []byte, []byte) error { after++; return nil }
	f := &pairedGraphFaultNetwork{t: t, n: &certifiedProcessNetwork{all: map[uint64]*graphProcess{4: {mux: &graphProcessMux{hub: h}}}}}
	_, release, restore := nativeProtocolStateCommittedGate(f)
	h.mu.Lock()
	activeBefore, activeAfter := h.beforeRelay, h.afterRelay
	h.mu.Unlock()
	if err := activeBefore(t.Context(), 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := activeAfter(t.Context(), 0, 0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if before != 1 || after != 1 {
		t.Fatal("actual observation hooks lost while hold owns its window", before, after)
	}
	release()
	for range 2 {
		if err := restore(); err != nil {
			t.Fatal("idempotent same callback owner restoration", err)
		}
	}
	if err := h.beforeRelay(t.Context(), 0, 0, nil); err != nil {
		t.Fatal(err)
	}
	if err := h.afterRelay(t.Context(), 0, 0, nil, nil); err != nil {
		t.Fatal(err)
	}
	if before != 2 || after != 2 {
		t.Fatal("restored exact callbacks were replaced by no-ops")
	}
	late := nativeProtocolStateSavedHooks(h)
	for range 2 {
		if err := late(); !errors.Is(err, errInvalid) {
			t.Fatal("non-LIFO replacement overwrote active owner", err)
		}
	}
	if err := h.beforeRelay(t.Context(), 0, 0, nil); err != nil || before != 3 {
		t.Fatal("refusal mutated live hooks", err)
	}
	allocation := 0
	for _, recipe := range nativeProtocolStateInventory() {
		if recipe.Family == "allocation" {
			allocation++
		}
	}
	if allocation != 8 {
		t.Fatal("accepted issuance source attribution changed", allocation)
	}
}

func TestNativeProtocolPoolProofDoesNotCancelSharedClientLifetime(t *testing.T) {
	// An expired diagnostic operation must fail before IPC. It cannot replace
	// the shared feed client's independently retained original context.
	original := t.Context()
	expired, cancel := context.WithCancel(original)
	cancel()
	process := &graphProcess{mux: &graphProcessMux{}}
	network := &certifiedProcessNetwork{leaders: [2]uint64{2, 6}, all: map[uint64]*graphProcess{2: process}}
	client := &nativeProtocolStateClient{t: t, ctx: original, n: network, member: 2, cookie: [16]byte{1}}
	capture := &nativeProtocolPoolCapture{t: t, ctx: expired, n: network, clients: map[uint64]*nativeProtocolStateClient{2: client}, processes: map[uint64]*graphProcess{2: process}}
	if _, err := capture.proof(original, 3, nil, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("expired proof did not refuse before native effects", err)
	}
	if client.ctx != original || client.ctx.Err() != nil || client.sequence != 1 {
		t.Fatal("diagnostic replaced client lifetime or refunded failed sequence", client.ctx.Err(), client.sequence)
	}
}

func TestNativeProtocolMoveBoundaryReportsEarlyFailureWithoutRenewingDeadline(t *testing.T) {
	ctx := t.Context()
	done := make(chan graphProcessAnswer, 1)
	done <- graphProcessAnswer{err: errInvalid}
	actual, err := nativeProtocolStateWaitMoveBoundary(ctx, make(chan struct{}), done)
	if !errors.Is(err, errInvalid) || actual == nil || !errors.Is(actual.err, errInvalid) || ctx.Err() != nil {
		t.Fatal("failed invocation hidden until original deadline", err)
	}
	lost := make(chan struct{})
	close(lost)
	if _, err := nativeProtocolStateWaitMoveBoundary(ctx, lost, nil); err != nil {
		t.Fatal("actual observed loss replaced by absent caller reply", err)
	}
}

func TestNativeProtocolPooledMoveFactoryKeepsActualIntentAndFinitePools(t *testing.T) {
	t.Setenv("RHO_GRAPH_FAULT_FAMILY_PROCESS", "1")
	t.Setenv("RHO_GRAPH_DURABLE_MODEL", "off")
	n := collectedGraphNetworkWithRecovery(t, nil, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4})
	_, _, _, _ = retainedPublisherFromNetwork(t, n)
	owner, err := newNativeLogicalTransitionOwner(t.Context(), n.hosts[0][0], 1)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.close()
	family, _, err := newNativeFaultFamilyOwner(t.Context(), t.TempDir(), 1, true)
	if err != nil {
		t.Fatal(err)
	}
	defer family.close()
	input := nativeTransitionMovePrepare{Version: 1, Cookie: owner.cookie, FamilyCookie: family.cookie, RangeID: 1, Target: 8, Pages: graphE17AdmittedMovePages, Rows: 8, Bytes: 16 << 10}
	body, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	request := processRequest{op: processNativeTransitionControl, sequence: 1, data: append([]byte(nativeTransitionMovePrepareMarker), body...)}
	wire, err := owner.prepareMove(t.Context(), family, [16]byte{1}, request)
	if err != nil {
		t.Fatal("actual pooled factory admission", err)
	}
	if family.move == nil || family.move.source != owner.host.machine.scope() || !family.move.pools.valid(true) || family.move.pools != (rangeMovePools{2 << 20, 8 << 20, 8 << 20}) || family.moveCalls != 1 {
		t.Fatal("pooled factory omitted exact finite grants or actual private intent")
	}
	original := family.move
	replay, err := owner.prepareMove(t.Context(), family, [16]byte{1}, request)
	if err != nil || string(replay) != string(wire) || family.move != original || family.moveCalls != 1 {
		t.Fatal("factory replay replaced private intent or repeated admission", err)
	}
}

func TestNativeProtocolStateStartBindsParentAndChildAgreement(t *testing.T) {
	// The parent is an ordinary test process, not an already selected replica.
	// Starting the fixture must bind every required role before it validates
	// its first genuine proposal; child-only extraEnv cannot repair this parent.
	t.Setenv("RHO_GRAPH_CERTIFIED_PROCESS", "")
	t.Setenv("RHO_GRAPH_SOURCE_PROCESS", "")
	t.Setenv("RHO_GRAPH_PAIRED_PROCESS", "")
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "4")
	ctx, run, deadline := nativeProtocolStateStart(t)
	// startGraphProcess inherits this exact role before Open. The actual
	// replacement already selects it; initial voters must own the same bounded
	// snapshot registry before Offers is polled during native catch-up.
	if os.Getenv("RHO_GRAPH_PAIRED_PROCESS") != "1" {
		t.Fatal("initial state cohort lacks replacement snapshot-owner role")
	}
	if run == ([16]byte{}) || ctx.Err() != nil {
		t.Fatal("state start lost original admission", ctx.Err())
	}
	if actual, ok := ctx.Deadline(); !ok || actual.UnixNano() != deadline {
		t.Fatal("parent acquired a different deadline")
	}
	if selected, err := nativeProtocolStateProfileFromEnv(); err != nil || !selected {
		t.Fatal("parent and child profile selection disagree", selected, err)
	}
	if agreement, err := nativeProtocolStateAgreement(); err != nil || agreement != rangeMovePooledSemanticContractID() {
		t.Fatal("parent cannot validate actual pooled child bootstrap", err)
	}
	// The correction supplies missing admission, rather than weakening the
	// selector: either omitted base role must still refuse the pooled contract.
	for _, key := range []string{"RHO_GRAPH_CERTIFIED_PROCESS", "RHO_GRAPH_SOURCE_PROCESS"} {
		t.Setenv(key, "")
		if agreement, err := nativeProtocolStateAgreement(); !errors.Is(err, errInvalid) || agreement != ([32]byte{}) {
			t.Fatal("missing actual base role admitted", key, err)
		}
		t.Setenv(key, "1")
	}
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "")
	if agreement, err := nativeProtocolStateAgreement(); err != nil || agreement != rangeMoveSpanSemanticContractID() {
		t.Fatal("literal Recovery3 agreement changed", err)
	}
}

func TestNativeProtocolStateCompoundCleanupKeepsPrimitiveControlSlot(t *testing.T) {
	for _, action := range []string{"close", "snapshot-close", "release", "expire", "cancel", "pin-close"} {
		body, err := json.Marshal(nativeProtocolStateControl{Version: 1, Cookie: [16]byte{1}, Sequence: 1, Action: action})
		if err != nil {
			t.Fatal(err)
		}
		r := processRequest{op: processNativeTransitionControl, data: append([]byte(nativeProtocolStateMarker), body...)}
		if !nativeProtocolStateTerminalControl(r) {
			t.Fatal("actual encoded terminal not recognized", action)
		}
		control, terminal := nativeProtocolStateWorkerClass(r, true)
		if control || !terminal {
			t.Fatal("compound cleanup occupies primitive slot or loses terminal lifetime", action, control, terminal)
		}
		// A malformed/sibling request cannot adopt NPS terminal lifetime.
		r.index = 1
		if control, terminal = nativeProtocolStateWorkerClass(r, false); control || terminal {
			t.Fatal("malformed terminal admitted", action)
		}
	}
	primitive, err := encodeGraphSourceRequest(graphSourceRequest{kind: sourceClose, sequence: 1, nonce: [16]byte{1}, cookie: graphSourceCookie{issuer: [16]byte{2}, sequence: 3, secret: [16]byte{4}}})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeGraphSourceRequest(primitive)
	if err != nil || decoded.kind != sourceClose {
		t.Fatal("actual native close framing", err)
	}
	if control, terminal := nativeProtocolStateWorkerClass(processRequest{op: processSourceServe, entity: 2, data: primitive}, true); !control || !terminal {
		t.Fatal("nested native close lost reserved lane or cleanup lifetime")
	}
	for _, control := range []bool{false, true} {
		// Primitive native cleanup and all unrelated request classes remain on
		// their existing lane, with exactly the original lifetime selection.
		r := processRequest{op: processSourceServe}
		actual, terminal := nativeProtocolStateWorkerClass(r, control)
		if actual != control || terminal != control {
			t.Fatal("primitive or unrelated lane changed", control)
		}
	}
}

func TestNativeProtocolPooledTailBackpressurePreservesMoveOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	n, publisher, ops, wires := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
		p.RetainedApplicationBytes, p.RetainedApplicationRecords = 256<<20, 131072
		if err := nativeProtocolStatePooledTailPolicy(p); err != nil {
			t.Fatal(err)
		}
	}, rangeMovePooledSemanticContractID(), raftlog.ApplicationRecoveryConfig{Version: 4}))
	policy := retainedFeedGuardPolicy(64)
	deadline, _ := ctx.Deadline()
	policy.deadlineUnixNanos = deadline.UnixNano()
	feed, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	token, err := feed.Publish(ctx)
	if err != nil {
		t.Fatal(err)
	}
	held, err := feed.OpenSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	foreign, err := beginRetainedFeed(ctx, publisher, n.requestID(), graphReadRequest{mode: graphReadFresh}, 1, policy)
	if err != nil {
		t.Fatal(err)
	}
	other, err := foreign.Publish(ctx)
	if err != nil || bytes.Equal(other.definition, token.definition) {
		t.Fatal(err)
	}
	if err = foreign.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err = foreign.Close(); err != nil {
		t.Fatal(err)
	}
	if err = held.Close(); err != nil {
		t.Fatal(err)
	}
	if err = feed.Close(); err != nil {
		t.Fatal(err)
	}
	feed, err = openRetainedFeed(ctx, publisher, token)
	if err != nil {
		t.Fatal(err)
	}
	held, err = feed.OpenSnapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := held.Close(); err != nil {
			t.Error(err)
		}
		if err := feed.Close(); err != nil {
			t.Error(err)
		}
	}()
	intent, err := n.hosts[0][0].PreparePooledRangeMove(ctx, n.requestID(), 1, 8, graphE17AdmittedMovePages, 8, 16<<10, rangeMovePools{2 << 20, 8 << 20, 8 << 20})
	if err != nil {
		t.Fatal(err)
	}
	h := n.hosts[0][0]
	// Queue only genuine pre-fence originals. They must remain unable to
	// mutate the fenced graph while exercising the actual native tail limit.
	queued := make([]allocationProposal, nativeProtocolStateTailEntries)
	for i := range queued {
		queued[i], err = h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "must-not-install"}}, uint64(100+i), nil)
		if err != nil {
			t.Fatal(err)
		}
	}
	publisher.coordinator.transports[8] = &rangeMoveCoordinatorWire{nativeGraphTestTransport: &nativeGraphTestTransport{service: wires[1].service}, dropFence: true}
	move, err := newRangeMoveCoordinatorOn(publisher.coordinator)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := move.Run(ctx, intent)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.source.phase != rangeMoveFence || pending.source.result != rangeMoveApplied {
		t.Fatal("actual source Fence before unknown target", pending, err)
	}
	protected := rangeMoveAdmissionOwner(t, h)
	beforeDrain := rangeMoveImportGuardReader(t, h).base
	var deliveries []replica.Packet
	for i := range nativeProtocolStateTailEntries - 1 {
		event, err := h.Submit(queued[i])
		if err != nil {
			t.Fatal("finite pending tail refused before admitted window", i, err)
		}
		deliveries = append(deliveries, event.packets...)
	}
	if _, err := h.Submit(queued[nativeProtocolStateTailEntries-1]); !errors.Is(err, raftlog.ErrLimit) {
		t.Fatal("native tail did not backpressure", err)
	}
	n.pump(0, allocationEvent{packets: deliveries})
	if owner := rangeMoveAdmissionOwner(t, h); owner != protected {
		t.Fatal("backpressure lost recovery owner", owner, protected)
	}
	afterDrain := rangeMoveImportGuardReader(t, h).base
	if !bytes.Equal(beforeDrain.Image, afterDrain.Image) || beforeDrain.ImageHash != afterDrain.ImageHash {
		t.Fatal("fenced queued writes changed graph image")
	}
	targetWire := publisher.coordinator.transports[8]
	zeroGate := &rangeMoveImportGuardGate{graphPeerWireTransport: targetWire, mode: 2}
	publisher.coordinator.transports[8] = zeroGate
	rangeMoveImportGuardResumeToPause(t, move, pending.resume, zeroGate)
	zeroCommand, err := decodeRangeMoveCommand(zeroGate.importZero.wire)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*rangeMoveStatus){
		func(s *rangeMoveStatus) { s.sequence++ },
	} {
		bad := zeroCommand
		bad.proofs = slices.Clone(zeroCommand.proofs)
		for i := range bad.proofs {
			if bad.proofs[i].ns.partition == intent.source.partition {
				mutate(&bad.proofs[i])
				break
			}
		}
		if _, err := rangeMoveImportGuardStage(t, n.hosts[1][0], bad); !errors.Is(err, errInvalid) {
			t.Fatal("unauthenticated source summary reached Import0 batch", err)
		}
	}
	gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: targetWire, sequence: 1}
	publisher.coordinator.transports[8] = gate
	paused, pauseErr := runMovePoolResume(t, move, pending.resume)
	if !gate.stopped || !errors.Is(pauseErr, io.EOF) || !errors.Is(pauseErr, errRangeMovePending) || paused.resume != pending.resume {
		t.Fatal("genuine Import0 did not reach next-page boundary", pauseErr)
	}
	target := n.hosts[1][2]
	q, cfg := rangeMoveBaselineGuardReader(t, target.machine, target.driver.Applied())
	status, found, err := readRangeMoveStatus(q, pending.resume.move)
	if err != nil || !found || status.phase != rangeMoveImport || status.sequence != 0 {
		t.Fatal("actual installed Import0", status, err)
	}
	progressWire, found, err := q.getBounded(rangeMoveProgressKey(q.ns, status.move), rangeMoveSpanProgressBytes)
	if err != nil || !found {
		t.Fatal(err)
	}
	progress, err := decodeRangeMoveProgress(progressWire, q.arena)
	if err != nil || progress.imported == nil || progress.replayPages != 0 || progress.retentionRows == 0 || status.rows != progress.imported.Rows+progress.retentionRows || status.size != progress.imported.Bytes+progress.retainedBytes() {
		t.Fatal("Import0 published full manifest instead of installed prefix", status, err)
	}
	original, found, err := crossControl(q, rangeMoveControlKey(q.ns, status.move, 'M'), 256)
	if err != nil || !found || original.Index != status.index {
		t.Fatal("actual atomically installed manifest proof", err)
	}
	nativeProtocolAssertManifestProofRefusals(t, intent, status, progress, original)
	baseline, err := target.machine.rangeMoveCatalogBaseline(q, cfg)
	if err != nil || baseline == nil {
		t.Fatal("genuine Import0 baseline", err)
	}
	if err := baseline.Close(); err != nil {
		t.Fatal(err)
	}
	if err := q.view.Close(); err != nil {
		t.Fatal(err)
	}
	configuration := n.configs[1][2]
	configuration.Create = false
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	store, err := raftlog.Open(configuration)
	if err != nil {
		t.Fatal(err)
	}
	machine, err := newRangeMovePooledMaterializer(store, target.machine.ns, n.d, target.machine.limits)
	if err != nil {
		t.Fatal("same-directory Import0 Restore", err)
	}
	reopened, err := openAllocationHost(machine, 3, replica.Config{Store: store, ApplicationSnapshotSends: replica.ApplicationSnapshotSendLimits{MaxOffers: 2, MaxOwnedBytes: 4 << 20}})
	if err != nil {
		t.Fatal(err)
	}
	n.hosts[1][2] = reopened
	t.Cleanup(func() {
		if err := reopened.Close(); err != nil {
			t.Error(err)
		}
	})
	publisher.coordinator.transports[8] = targetWire
	result, err := runMovePoolResume(t, move, pending.resume)
	if err != nil || !result.committed || result.resume != pending.resume {
		t.Fatal("same move did not complete after real tail drain", result.source, result.target, err)
	}
	completedQ := rangeMoveImportGuardReader(t, n.hosts[1][0])
	completedWire, found, err := completedQ.getBounded(rangeMoveProgressKey(completedQ.ns, result.resume.move), rangeMoveSpanProgressBytes)
	if err != nil || !found {
		t.Fatal(err)
	}
	completed, err := decodeRangeMoveProgress(completedWire, completedQ.arena)
	if err != nil {
		t.Fatal(err)
	}
	if digest, err := targetMoveManifestDigest(intent, result.target, completed, raftlog.ApplicationControlRecord{}, false); err != nil || digest != result.target.digest {
		t.Fatal("complete old image lost original full manifest binding", err)
	}
}

func TestNativeProtocolStateTailPolicyIsExplicitAndBounded(t *testing.T) {
	if err := nativeProtocolStatePooledTailPolicy(nil); !errors.Is(err, errInvalid) {
		t.Fatal("nil policy", err)
	}
	base := raftlog.DefaultApplicationPolicy(1)
	base.RetainedApplicationBytes = 256 << 20
	for _, mutate := range []func(*raftlog.ApplicationPolicy){
		func(p *raftlog.ApplicationPolicy) { p.MaxTailEntries-- },
		func(p *raftlog.ApplicationPolicy) { p.MaxInstallBytes-- },
		func(p *raftlog.ApplicationPolicy) { p.RetainedApplicationBytes-- },
	} {
		p := base
		mutate(&p)
		before := p
		if err := nativeProtocolStatePooledTailPolicy(&p); !errors.Is(err, errInvalid) || p != before {
			t.Fatal("foreign policy changed", err)
		}
	}
	p := base
	if err := nativeProtocolStatePooledTailPolicy(&p); err != nil || p.MaxTailEntries != 19 {
		t.Fatal("explicit D-only admission", err, p.MaxTailEntries)
	}
	p.MaxTailEntries = base.MaxTailEntries
	if p != base {
		t.Fatal("other budgets changed")
	}
}

// These are malformed diagnostic records, never native proposals or authority.
func nativeProtocolAssertManifestProofRefusals(t *testing.T, intent rangeMoveIntent, status rangeMoveStatus, progress rangeMoveProgress, original raftlog.ApplicationControlRecord) {
	t.Helper()
	if digest, err := targetMoveManifestDigest(intent, status, progress, original, true); err != nil || digest != status.digest || progress.replayPages != 0 {
		t.Fatal("genuine manifest changed installed cursor", err)
	}
	if _, err := targetMoveManifestDigest(intent, status, progress, original, false); !errors.Is(err, errCorrupt) {
		t.Fatal("missing original accepted incomplete replay", err)
	}
	for _, mutate := range []func(*rangeMoveStatus){
		func(s *rangeMoveStatus) { s.ns.partition = intent.target.partition },
		func(s *rangeMoveStatus) { s.move[0] ^= 1 },
		func(s *rangeMoveStatus) { s.intent[0] ^= 1 },
		func(s *rangeMoveStatus) { s.sequence++ },
		func(s *rangeMoveStatus) { s.phase = rangeMoveCapture },
		func(s *rangeMoveStatus) { s.result = rangeMoveLimit },
		func(s *rangeMoveStatus) { s.rows++ },
		func(s *rangeMoveStatus) { s.size++ },
		func(s *rangeMoveStatus) { s.digest[0] ^= 1 },
	} {
		bad := original
		proof, err := decodeRangeMoveStatus(original.Value)
		if err != nil {
			t.Fatal(err)
		}
		mutate(&proof)
		bad.Value, err = encodeRangeMoveStatus(proof)
		if err != nil {
			t.Fatal("canonical adversarial proof", err)
		}
		if _, err := targetMoveManifestDigest(intent, status, progress, bad, true); !errors.Is(err, errCorrupt) {
			t.Fatal("foreign or resealed original accepted", err)
		}
	}
	for _, mutate := range []func(*raftlog.ApplicationControlRecord){
		func(r *raftlog.ApplicationControlRecord) { r.Key = []byte("foreign") },
		func(r *raftlog.ApplicationControlRecord) { r.Index = 0 },
		func(r *raftlog.ApplicationControlRecord) {
			r.Value = bytes.Clone(r.Value)
			r.Value[len(r.Value)-1] ^= 1
		},
	} {
		bad := original
		mutate(&bad)
		if _, err := targetMoveManifestDigest(intent, status, progress, bad, true); !errors.Is(err, errCorrupt) {
			t.Fatal("foreign control envelope accepted", err)
		}
	}
	for _, mutate := range []func(*rangeMoveProgress){
		func(p *rangeMoveProgress) { p.descriptor.Digest[0] ^= 1 },
		func(p *rangeMoveProgress) { p.reference.packet[0] ^= 1 },
		func(p *rangeMoveProgress) { p.replayEnd.digest[0] ^= 1 },
	} {
		bad := progress
		mutate(&bad)
		wire, err := encodeRangeMoveProgress(bad, moveSpanOutput(t))
		if err != nil {
			t.Fatal("canonical adversarial progress", err)
		}
		bad, err = decodeRangeMoveProgress(wire, moveSpanOutput(t))
		if err != nil {
			t.Fatal("resealed diagnostic framing", err)
		}
		if _, err := targetMoveManifestDigest(intent, status, bad, original, true); !errors.Is(err, errCorrupt) {
			t.Fatal("changed manifest accepted against original proof", err)
		}
	}
	badStatus := status
	badStatus.rows++
	if _, err := targetMoveManifestDigest(intent, badStatus, progress, original, true); !errors.Is(err, errCorrupt) {
		t.Fatal("changed installed prefix accepted", err)
	}
}

func TestNativeCollectedRangeMoveImportZeroPrefixAndRetainedOldView(t *testing.T) {
	for _, profile := range []struct {
		name      string
		agreement raftlog.ApplicationSemanticContractID
		recovery  uint32
	}{{"collected", collectedGraphSemanticContractID(), 2}, {"span", rangeMoveSpanSemanticContractID(), 3}, {"batched", rangeMoveGraphBatchSemanticContractID(), 3}} {
		t.Run(profile.name, func(t *testing.T) {
			n, publisher, ops, _ := retainedPublisherFromNetwork(t, collectedGraphNetworkWithRecovery(t, func(p *raftlog.ApplicationPolicy) {
				p.RetainedApplicationBytes, p.RetainedApplicationRecords, p.MaxTailEntries = 256<<20, 131072, nativeProtocolStateTailEntries
			}, profile.agreement, raftlog.ApplicationRecoveryConfig{Version: profile.recovery}))
			view, closeOld := crossView(t, n, 0, [2]uint64{})
			defer closeOld()
			assertCertifiedProjection(t, view, 1, true, nil)
			intent, err := n.hosts[0][0].PrepareRangeMove(t.Context(), n.requestID(), 1, 8, 32, 32, 16<<10)
			if err != nil {
				t.Fatal(err)
			}
			move, err := newRangeMoveCoordinatorOn(publisher.coordinator)
			if err != nil {
				t.Fatal(err)
			}
			targetWire := publisher.coordinator.transports[8]
			gate := &rangeMoveAdmissionImportGate{graphPeerWireTransport: targetWire, sequence: 1}
			publisher.coordinator.transports[8] = gate
			pending, err := runRangeMoveToCompletion(t, move, intent)
			if !gate.stopped || !errors.Is(err, io.EOF) || !errors.Is(err, errRangeMovePending) {
				t.Fatal("actual Import0 before page1", err)
			}
			h := n.hosts[1][2]
			q, cfg := rangeMoveBaselineGuardReader(t, h.machine, h.driver.Applied())
			status, found, err := readRangeMoveStatus(q, pending.resume.move)
			if err != nil || !found || status.phase != rangeMoveImport || status.sequence != 0 || status.rows == 0 || status.size == 0 {
				t.Fatal("actual retained prefix", status, err)
			}
			baseline, err := h.machine.rangeMoveCatalogBaseline(q, cfg)
			if err != nil || baseline == nil {
				t.Fatal("strict Import0 baseline", err)
			}
			if err := baseline.Close(); err != nil {
				t.Fatal(err)
			}
			if err := h.machine.Restore(q.base.Index, q.base.Image); err != nil {
				t.Fatal("same native Import0 Restore", err)
			}
			if err := q.view.Close(); err != nil {
				t.Fatal(err)
			}
			publisher.coordinator.transports[8] = targetWire
			done, err := runMovePoolResume(t, move, pending.resume)
			if err != nil || !done.committed || done.resume != pending.resume {
				t.Fatal("same original completion", err)
			}
			proposal, err := n.hosts[1][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: ops[0].Scope, Name: "after-handoff"}}, 313, nil)
			if err != nil {
				t.Fatal(err)
			}
			n.submitPartitionGraph(1, proposal, reasonNone)
			current, closeCurrent := crossView(t, n, 0, [2]uint64{})
			assertCertifiedProjection(t, current, 1, true, []string{"after-handoff"})
			closeCurrent()
			assertCertifiedProjection(t, view, 1, true, nil)
		})
	}
}

func TestNativeMoveReplayBatchKeepsDeclarationBindingAndRecordCharges(t *testing.T) {
	n, _, _, _, _ := rangeMoveCoordinatorFixture(t, 32)
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 16 {
		proposal, err := n.hosts[0][0].PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Scope: window, Name: fmt.Sprintf("original-%d", i)}}, uint64(400+i), nil)
		if err != nil {
			t.Fatal(err)
		}
		n.submitPartitionGraph(0, proposal, reasonNone)
	}
	source := rangeMoveReplayNativeReader(t, n.hosts[0][0], 32<<20)
	batch, err := exportMoveReplayBatch(source, source.ns, rangeMoveReplayStart(), 64<<10, 16)
	if err != nil || len(batch.pages) < 16 {
		t.Fatal("actual bounded multi-original export", len(batch.pages), err)
	}
	target := rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	before := target.arena.Used()
	cursor, err := stageMoveReplayBatch(target, source.ns, target.ns, [16]byte{91}, batch, rangeMoveReplayStart())
	if err != nil || !rangeMoveReplaySame(cursor, batch.pages[len(batch.pages)-1].next) || len(target.writes) < 16 {
		t.Fatal("actual bounded target reduction", len(target.writes), err)
	}
	payload := 0
	for _, row := range target.writes {
		if row.Deleted || !bytes.HasPrefix(row.Key, recordKey(target.ns, 20)) {
			t.Fatal("replay reducer changed ownership namespace")
		}
		payload += len(row.Key) + len(row.Value)
	}
	if source.rows == 0 || target.rows == 0 || source.arena.Used() <= 2*rangeMoveReplayRecordBytes+8192 || target.arena.Used() <= before+2*rangeMoveReplayRecordBytes+8192+payload {
		t.Fatal("batch omitted declaration or record work/ownership charges")
	}
	foreign := source.ns
	foreign.graph[0] ^= 1
	refused := rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	if _, err := stageMoveReplayBatch(refused, foreign, refused.ns, [16]byte{91}, batch, rangeMoveReplayStart()); !errors.Is(err, errInvalid) || len(refused.writes) != 0 {
		t.Fatal("foreign source graph reached batch effects", err)
	}
	wrongView := rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	wrongView.ns = source.ns
	if _, err := exportMoveReplayBatch(wrongView, source.ns, rangeMoveReplayStart(), 64<<10, 16); !errors.Is(err, errInvalid) {
		t.Fatal("declaration reused on a different native view", err)
	}
	undeclared := source.ns
	undeclared.partition = 999
	refused = rangeMoveReplayNativeReader(t, n.hosts[1][0], 32<<20)
	if _, err := stageMoveReplayBatch(refused, undeclared, refused.ns, [16]byte{91}, batch, rangeMoveReplayStart()); !errors.Is(err, errInvalid) || len(refused.writes) != 0 {
		t.Fatal("undeclared origin reached batch effects", err)
	}
}
