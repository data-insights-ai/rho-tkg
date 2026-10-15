package graphapply

import (
	"context"
	"fmt"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const nativeP06ProgramProfile = "native-p06-causal-program-v1"

// Each row names an existing semantic parent. This is not another transaction
// factory: the parent still captures, executes and recovers its actual original.
type nativeP06ProgramRecipe struct {
	Family string
	Recipe nativeP06ConjunctionRecipe
}

func (r nativeP06ProgramRecipe) valid() bool {
	x := r.Recipe
	if r.Family == "initial-batch" {
		base := x
		base.Phase, base.Partition = 0, 0
		return base.valid() && x.Coordinator == 8 && x.Commit && x.Stage == "" && (x.Phase >= crossRegister && x.Phase <= crossResolve) && (x.Partition == 3 || x.Partition == 8) && (x.Phase != crossRegister && x.Phase != crossDecide || x.Partition == 8)
	}
	if !r.Recipe.valid() {
		return false
	}
	switch r.Family {
	case "decide":
		return x.Phase == 0 && x.Partition == 0 && x.Stage == ""
	case "recovery":
		return x.Stage == "" && (x.Phase == crossPrepare || x.Phase == crossResolve) && (x.Partition == 3 || x.Partition == 8)
	case "terminal":
		return x.Stage == nativeP06TerminalStage
	case "register", "first-prepare":
		return x.Phase == 0 && x.Partition == 0 && x.Stage == ""
	}
	return false
}

func (r nativeP06ProgramRecipe) name() string {
	if !r.valid() {
		return ""
	}
	switch r.Family {
	case "register":
		return nativeP06InitialName(r.Recipe)
	case "first-prepare":
		return nativeP06FirstPrepareName(r.Recipe)
	case "initial-batch":
		return fmt.Sprintf("batch-cross-%d-p%d-k%d-%s", r.Recipe.Phase, r.Recipe.Partition, r.Recipe.Kind, r.Recipe.Side)
	}
	return r.Recipe.name()
}

func nativeP06ProgramInventory() []nativeP06ProgramRecipe {
	var rows []nativeP06ProgramRecipe
	for _, r := range nativeP06ConjunctionRecipes() {
		for _, family := range []string{"decide", "register", "first-prepare"} {
			rows = append(rows, nativeP06ProgramRecipe{family, r})
		}
	}
	for _, r := range nativeP06RecoveryConjunctionRecipes() {
		rows = append(rows, nativeP06ProgramRecipe{"recovery", r})
	}
	for _, r := range nativeP06TerminalRecipes() {
		rows = append(rows, nativeP06ProgramRecipe{"terminal", r})
	}
	for _, phase := range []crossPhase{crossRegister, crossPrepare, crossDecide, crossResolve} {
		for _, partition := range []uint64{3, 8} {
			if (phase == crossRegister || phase == crossDecide) && partition != 8 {
				continue
			}
			for _, kind := range []raftlog.StableStorageKind{raftlog.StableStorageReady, raftlog.StableStorageApplication} {
				for _, side := range []string{"before", "after"} {
					rows = append(rows, nativeP06ProgramRecipe{"initial-batch", nativeP06ConjunctionRecipe{Coordinator: 8, Commit: true, Kind: kind, Side: side, Phase: phase, Partition: partition}})
				}
			}
		}
	}
	return rows
}

type nativeP06ProgramEvidence struct {
	Profile          string
	Mechanism        string
	Program          nativeProtocolProgramEvidence
	AfterPredecessor []byte
	AfterLoss        *nativeProtocolProgramElection
	Cohort           []pairedFaultProcess
}

// Selection occurs only in these fresh optional parents, before any source or
// command. A local submitting Ready cannot expose that command's Raft frames;
// its explicit nonliteral program uses the already-emitted Tick predecessor.
func nativeP06ProgramStart(t *testing.T, ctx context.Context, f *pairedGraphFaultNetwork, kind raftlog.StableStorageKind, programs ...nativeProtocolProgramSpec) *nativeProtocolProgram {
	t.Helper()
	p := nativeProtocolProgramStart(t, ctx, f, programs...)
	if p != nil {
		p.p06 = true
		p.preReadyPredecessor = kind == raftlog.StableStorageReady && p.spec.Message != "literal"
	}
	return p
}

func nativeP06ProgramLoss(t *testing.T, p *nativeProtocolProgram) {
	t.Helper()
	if p != nil {
		_ = nativeProtocolProgramFinish(t, p.f)
	}
}

func nativeP06ProgramComplete(t *testing.T, p *nativeProtocolProgram) *nativeP06ProgramEvidence {
	t.Helper()
	if p == nil {
		return nil
	}
	if !p.closed || p.afterLoss == nil {
		t.Fatal("P06 native program did not observe actual selected loss/successor")
	}
	evidence, err := p.close()
	if err != nil {
		t.Fatal("P06 original-deadline program", err)
	}
	mechanism := "selected-command-native-packets"
	if p.spec.Message == "literal" {
		mechanism = "literal"
	} else if p.preReadyPredecessor {
		mechanism = "emitted-predecessor-tick-packets"
	}
	return &nativeP06ProgramEvidence{Profile: nativeP06ProgramProfile, Mechanism: mechanism, Program: evidence, AfterPredecessor: p.afterPredecessor, AfterLoss: p.afterLoss, Cohort: p.f.stopAll()}
}

func nativeP06ProgramAlternate(member uint64) uint64 {
	if member <= 3 {
		return 3 - member
	}
	if member == 4 {
		return 6
	}
	return 4
}

func nativeP06ProgramSuccessor(t *testing.T, p *nativeProtocolProgram, prior, next uint64) {
	t.Helper()
	if p != nil {
		if _, err := p.successor(prior, next); err != nil {
			t.Fatal("P06 actual pre-selection successor", err)
		}
	}
}
