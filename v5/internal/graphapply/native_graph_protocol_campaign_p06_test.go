package graphapply

import (
	"context"
	"errors"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func TestNativeP06ProgramsPreserveExactExistingSemanticInventory(t *testing.T) {
	counts := map[string]int{}
	seen := map[string]bool{}
	for _, row := range nativeP06ProgramInventory() {
		if !row.valid() || seen[row.name()] {
			t.Fatal("duplicate/foreign existing semantic occurrence", row)
		}
		seen[row.name()] = true
		counts[row.Family]++
	}
	if len(seen) != 184 || counts["decide"] != 16 || counts["recovery"] != 64 || counts["terminal"] != 48 || counts["register"] != 16 || counts["first-prepare"] != 16 || counts["initial-batch"] != 24 {
		t.Fatal("existing literal semantic inventory changed", counts)
	}
	cells := 0
	for _, layout := range []string{"original", "rotated"} {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			if layout == "original" && message == "literal" {
				continue
			}
			for range nativeP06ProgramInventory() {
				cells++
			}
		}
	}
	if cells != 1288 {
		t.Fatal("new source census includes duplicate original literals", cells)
	}
	for _, row := range []nativeP06ProgramRecipe{{}, {"initial-batch", nativeP06ConjunctionRecipe{Coordinator: 8, Commit: true, Phase: crossRegister, Partition: 3, Kind: raftlog.StableStorageReady, Side: "before"}}, {"recovery", nativeP06ConjunctionRecipe{Coordinator: 3, Commit: true, Kind: 1, Side: "before"}}} {
		if row.valid() {
			t.Fatal("unreachable/foreign native occurrence", row)
		}
	}
	var absent *nativeProtocolProgram
	if err := absent.armNative(3, 1, []byte("inert")); !errors.Is(err, errInvalid) {
		t.Fatal("nil state/program attachment", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	p := &nativeProtocolProgram{ctx: ctx}
	if err := p.armNative(3, 1, []byte("inert")); !errors.Is(err, context.Canceled) {
		t.Fatal("expired native owner attachment", err)
	}
}

// Literal original184 remain in their unchanged existing parents. These are
// only the required new leader/message coordinates over those SAME bodies.
func TestNativeP06ExistingTransitionsWithActualLeaderAndMessagePrograms(t *testing.T) {
	for _, layout := range []string{"original", "rotated"} {
		for _, message := range []string{"literal", "delay", "reorder", "duplicate"} {
			if layout == "original" && message == "literal" {
				continue
			}
			for index, row := range nativeP06ProgramInventory() {
				if testing.Short() && index != 0 && index != 1 && index != 2 && index != 48 && index != 112 && index != 160 {
					continue
				}
				t.Run(layout+"/"+message+"/"+row.name(), func(t *testing.T) {
					program := nativeProtocolProgramSpec{1, layout, message}
					switch row.Family {
					case "register":
						nativeP06InitialRegisterCase(t, row.Recipe, program)
					case "first-prepare":
						nativeP06FirstPrepareCase(t, row.Recipe, program)
					case "decide", "recovery", "terminal":
						nativeP06ConjunctionCase(t, row.Recipe, program)
					case "initial-batch":
						nativeBatchCrossCase(t, byte(row.Recipe.Phase), row.Recipe.Partition, row.Recipe.Kind, row.Recipe.Side, program)
					default:
						t.Fatal("missing actual semantic parent")
					}
				})
			}
		}
	}
}
