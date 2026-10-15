package graphapply

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
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
	t.Setenv("RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "4")
	ctx, run, deadline := nativeProtocolStateStart(t)
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
