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

	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func nativeTransitionPinInventory() []nativeTransitionPinRecipe {
	var recipes []nativeTransitionPinRecipe
	for phase := byte(1); phase <= 8; phase++ {
		for _, partition := range []uint64{3, 8} {
			for _, side := range []string{"before-submit", "after-applied"} {
				r := nativeTransitionPinRecipe{Version: 1, Phase: phase, Partition: partition, Side: side}
				if r.valid() {
					recipes = append(recipes, r)
				}
			}
		}
	}
	return recipes
}

func TestNativePinTransitionInventoryKeepsCreationPublicationAndTerminalDistinct(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, actual := range []error{errRetainedPinPending, replica.ErrUnavailable, context.Canceled, context.DeadlineExceeded, errRemoteOperationFailure, fmt.Errorf("wrapped: %w", replica.ErrUnavailable), errors.Join(errRetainedPinPending, replica.ErrUnavailable, context.Canceled)} {
		if nativeTransitionPinErrorClass(actual) != "pending-or-unavailable" {
			t.Fatal("genuine interruption classification", actual)
		}
	}
	for _, actual := range []error{errInvalid, errLimit, errCorrupt, errors.Join(errRetainedPinPending, errInvalid), errors.Join(errRetainedPinPending, errLimit), errors.Join(errRetainedPinPending, errCorrupt), errors.New("unexpected backend error"), errors.Join(errRetainedPinPending, errors.New("unexpected joined backend error")), errors.Join(errGraphPinReleased, errors.New("unexpected terminal backend error"))} {
		if class := nativeTransitionPinErrorClass(actual); class == "pending-or-unavailable" || class == "applied" || class == "released" || class == "expired" {
			t.Fatal("non-interruption qualified cell", actual, class)
		}
	}
	if nativeTransitionPinErrorClass(errGraphPinReleased) != "released" || nativeTransitionPinErrorClass(errGraphPinExpired) != "expired" || nativeTransitionPinErrorClass(nil) != "applied" {
		t.Fatal("terminal classifications conflated")
	}
	seen := make(map[nativeTransitionPinRecipe]bool)
	for _, r := range nativeTransitionPinInventory() {
		if !r.valid() || seen[r] {
			t.Fatal("duplicate or unsupported pin transition")
		}
		seen[r] = true
	}
	if len(seen) != 30 {
		t.Fatal("finite pin inventory including publisher-only Issue")
	}
	for _, r := range []nativeTransitionPinRecipe{{Version: 1, Phase: 7, Partition: 8, Side: "after-applied"}, {Version: 1, Phase: 9, Partition: 3, Side: "before-submit"}, {Version: 1, Phase: 1, Partition: 3, Side: "after-submit"}} {
		if r.valid() {
			t.Fatal("unmatched cell became passing no-op")
		}
	}
	input := nativeTransitionPinControl{Version: 1, Cookie: [16]byte{1}, Sequence: 1, Action: "begin"}
	encode := func(c nativeTransitionPinControl) []byte {
		t.Helper()
		wire, err := json.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		return append([]byte(nativeTransitionPinMarker), wire...)
	}
	if c, err := decodeNativeTransitionPinControl(encode(input)); err != nil || c.Action != "begin" {
		t.Fatal("bounded factory control", err)
	}
	for _, mutate := range []func(*nativeTransitionPinControl){func(c *nativeTransitionPinControl) { c.Sequence = 17 }, func(c *nativeTransitionPinControl) { c.Original = []byte("inert-not-born") }, func(c *nativeTransitionPinControl) { c.Action = "resume" }, func(c *nativeTransitionPinControl) { c.Cookie = [16]byte{} }, func(c *nativeTransitionPinControl) { c.Token = []byte("unbound-token") }} {
		bad := input
		mutate(&bad)
		if _, err := decodeNativeTransitionPinControl(encode(bad)); !errors.Is(err, errInvalid) {
			t.Fatal("foreign/unfunded factory control", err)
		}
	}
	var absent *nativeLogicalTransitionOwner
	if _, err := absent.servePin(t.Context(), nil, [16]byte{}, processRequest{}); !errors.Is(err, errInvalid) {
		t.Fatal("missing genuine owner accepted", err)
	}
}

func TestNativePinTransitionsRecoverExactOriginalThroughActualCohortReplacement(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, recipe := range nativeTransitionPinInventory() {
		if testing.Short() && !(recipe.Phase == pinIssue && recipe.Partition == 3 && recipe.Side == "before-submit" || recipe.Phase == pinPublish && recipe.Partition == 8 && recipe.Side == "after-applied") {
			continue
		}
		t.Run(fmt.Sprintf("phase%d-p%d-%s", recipe.Phase, recipe.Partition, recipe.Side), func(t *testing.T) {
			artifact := nativeTransitionPinCase(t, recipe)
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			command := exec.CommandContext(ctx, "python3", "-B", filepath.Join("..", "..", "..", "tools", "v5-substrate", "native_transition_pin_check.py"), artifact)
			var diagnostics processStderr
			command.Stdout, command.Stderr = &diagnostics, &diagnostics
			if err := command.Run(); err != nil {
				body, _ := diagnostics.state()
				t.Fatal("independent original pin/prefix/serial check", err, body)
			}
			if _, full := diagnostics.state(); full {
				t.Fatal("bounded checker diagnostics")
			}
		})
	}
}
