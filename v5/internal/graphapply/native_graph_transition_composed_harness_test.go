package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

const nativeTransitionComposedProfile = "native-recorded-conflict-fence-transfer-v1"

type nativeTransitionComposedAction struct {
	At, Kind, Original string
	Member, Ticks      uint64
}

type nativeTransitionComposedRecipe struct {
	Version      uint8
	Seed         uint64
	First, Phase string
	FenceFirst   bool
	Actions      []nativeTransitionComposedAction
}

func (r nativeTransitionComposedRecipe) valid() bool {
	if r.Version != 1 || r.Seed == 0 || r.First != "A" && r.First != "B" || r.Phase != "registered" && r.Phase != "prepared" || r.First == "B" && !r.FenceFirst || len(r.Actions) > 8 {
		return false
	}
	previous, ticks, duplicates := 0, uint64(0), 0
	for _, a := range r.Actions {
		stage := map[string]int{"held": 1, "settled": 2, "transferred": 3}[a.At]
		if stage == 0 || stage < previous {
			return false
		}
		previous = stage
		switch a.Kind {
		case "tick":
			if a.Member < 1 || a.Member > 6 || a.Ticks < 1 || a.Ticks > 3 || a.Original != "" {
				return false
			}
			ticks += a.Ticks
		case "duplicate":
			if stage == 1 || a.Member != 0 || a.Ticks != 0 || a.Original != "A" && a.Original != "B" {
				return false
			}
			duplicates++
		default:
			return false
		}
	}
	return ticks <= 16 && duplicates <= 4
}

type nativeTransitionComposedActionResult struct {
	Sequence      uint64
	Action        nativeTransitionComposedAction
	Before, After [2]uint64
	Ticks         []nativeFaultFamilyTick
	Response      []byte
}

type nativeTransitionComposedEvidence struct {
	Recipe            nativeTransitionComposedRecipe
	RecipeSHA256      [32]byte
	Run               [16]byte
	DeadlineUnixNanos int64
	Actions           []nativeTransitionComposedActionResult
}

type nativeTransitionComposedOwner struct {
	evidence nativeTransitionComposedEvidence
	ctx      context.Context
	arena    *graphstate.OutputBudget
	next     int
}

func nativeTransitionComposedFromEnv(t *testing.T) *nativeTransitionComposedOwner {
	t.Helper()
	raw := os.Getenv("RHO_GRAPH_COMPOSED_RECIPE")
	if raw == "" {
		return nil // Ordinary six-case behavior and artifact bytes are unchanged.
	}
	if t.Name() != "TestNativeComposedTransitionRecordedReplayCell" || len(raw) > 4096 {
		t.Fatal("recorded recipe requires explicit fresh replay entry")
	}
	var recipe nativeTransitionComposedRecipe
	d := json.NewDecoder(bytes.NewBufferString(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&recipe); err != nil || !recipe.valid() {
		t.Fatal("finite composed recipe", err)
	}
	if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
		t.Fatal("extra recorded recipe", err)
	}
	canonical, err := json.Marshal(recipe)
	if err != nil || string(canonical) != raw {
		t.Fatal("canonical original recipe", err)
	}
	var run [16]byte
	wire, err := hex.DecodeString(os.Getenv("RHO_GRAPH_TRANSITION_RUN"))
	if err != nil || len(wire) != len(run) || hex.EncodeToString(wire) != os.Getenv("RHO_GRAPH_TRANSITION_RUN") {
		t.Fatal("original replay run", err)
	}
	copy(run[:], wire)
	deadline, err := strconv.ParseInt(os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"), 10, 64)
	if err != nil || strconv.FormatInt(deadline, 10) != os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE") || run == ([16]byte{}) || deadline <= time.Now().UnixNano() || deadline-time.Now().UnixNano() > int64(1800*time.Second) {
		t.Fatal("fixed whole-replay deadline", err)
	}
	output := os.Getenv("RHO_GRAPH_COMPOSED_OUTPUT")
	if output == "" || !filepath.IsAbs(output) || filepath.Clean(output) != output {
		t.Fatal("fresh owned replay output")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	// Admit maximum ordinary current-frame ownership plus bounded retained
	// diagnostics before any children or optional effects. Actual responses
	// above64KiB refuse this diagnostic slice; they never become a passing cell.
	arena, err := graphstate.NewOutputBudget(8 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if err := arena.Reserve(2*processFrameBytes + 2*16*(64<<10) + (64 << 10)); err != nil {
		t.Fatal("prepaid complete replay diagnostic owner", err)
	}
	ctx, stop := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	t.Cleanup(stop)
	return &nativeTransitionComposedOwner{evidence: nativeTransitionComposedEvidence{Recipe: recipe, RecipeSHA256: sha256.Sum256(canonical), Run: run, DeadlineUnixNanos: deadline}, ctx: ctx, arena: arena}
}

func (o *nativeTransitionComposedOwner) selected(first int, phase string, fenceFirst bool) bool {
	if o == nil {
		return true
	}
	return o.evidence.Recipe.First == []string{"A", "B"}[first] && o.evidence.Recipe.Phase == phase && o.evidence.Recipe.FenceFirst == fenceFirst
}

func (o *nativeTransitionComposedOwner) call(t *testing.T, process *graphProcess, request processRequest) processResponse {
	t.Helper()
	response, transport := process.exchange(o.ctx, request)
	if err := errors.Join(transport, processResponseError(response)); err != nil {
		t.Fatal("actual recorded operation under original deadline", err)
	}
	return response
}

func (o *nativeTransitionComposedOwner) indexes(t *testing.T, f *pairedGraphFaultNetwork) [2]uint64 {
	t.Helper()
	return [2]uint64{o.call(t, f.n.all[f.n.leaders[0]], processRequest{op: processStateOp}).applied,
		o.call(t, f.n.all[f.n.leaders[1]], processRequest{op: processStateOp}).applied}
}

func (o *nativeTransitionComposedOwner) perform(t *testing.T, f *pairedGraphFaultNetwork, at string, actors [2]uint64, proposals [2]allocationProposal, originals [2]processResponse) {
	t.Helper()
	if o == nil {
		return
	}
	for o.next < len(o.evidence.Recipe.Actions) && o.evidence.Recipe.Actions[o.next].At == at {
		if err := o.ctx.Err(); err != nil {
			t.Fatal("original recorded action deadline", err)
		}
		a := o.evidence.Recipe.Actions[o.next]
		result := nativeTransitionComposedActionResult{Sequence: uint64(o.next + 1), Action: a, Before: o.indexes(t, f)}
		if a.Kind == "tick" {
			for tick := uint64(1); tick <= a.Ticks; tick++ {
				before := o.call(t, f.n.all[a.Member], processRequest{op: processStateOp})
				response := o.call(t, f.n.all[a.Member], processRequest{op: processTick})
				f.n.group(a.Member).enqueue(t, response)
				f.n.group(a.Member).drain(t)
				after := o.call(t, f.n.all[a.Member], processRequest{op: processStateOp})
				wire := p06ProcessWire(t, response)
				if len(wire) > 64<<10 || after.applied < before.applied {
					t.Fatal("recorded Tick diagnostic/source refusal")
				}
				result.Ticks = append(result.Ticks, nativeFaultFamilyTick{Member: a.Member, Ordinal: tick, Before: before.applied, After: after.applied, Response: wire})
			}
		} else {
			index := 0
			if a.Original == "B" {
				index = 1
			}
			expected, err := decodeGraphProcessCoordinatorResult(originals[index])
			if err != nil {
				t.Fatal("missing authentic first decision before duplicate", err)
			}
			response, transport := f.n.all[actors[index]].exchange(o.ctx, processRequest{op: processSourceCoordinate, data: proposals[index].wire})
			actual, actualErr := decodeGraphProcessCoordinatorResult(response)
			if err := errors.Join(transport, actualErr); err != nil || actual.decision != expected.decision {
				t.Fatal("same original recorded duplicate changed/refused immutable decision", err)
			}
			result.Response = p06ProcessWire(t, response)
			if len(result.Response) > 64<<10 {
				t.Fatal("recorded duplicate diagnostic frame refusal")
			}
		}
		result.After = o.indexes(t, f)
		o.evidence.Actions = append(o.evidence.Actions, result)
		o.next++
	}
}

func (o *nativeTransitionComposedOwner) finish(t *testing.T) *nativeTransitionComposedEvidence {
	t.Helper()
	if o == nil {
		return nil
	}
	if o.arena == nil || o.next != len(o.evidence.Recipe.Actions) || o.ctx.Err() != nil {
		t.Fatal("incomplete recorded action census/original deadline", o.ctx.Err())
	}
	return &o.evidence
}

func nativeTransitionComposedAssert(t *testing.T, owner *nativeTransitionComposedOwner, correct bool, message string) {
	t.Helper()
	if !correct && owner == nil {
		t.Fatal(message)
	}
	// Fresh recorded captures are classified by the independent checker after
	// all real original/prefix/transfer authentication. This does not turn a
	// failed protocol or unavailable child into a semantic counterexample.
}
