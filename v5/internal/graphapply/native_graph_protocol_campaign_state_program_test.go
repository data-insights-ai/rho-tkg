package graphapply

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
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
