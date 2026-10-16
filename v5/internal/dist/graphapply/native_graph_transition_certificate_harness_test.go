package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

const nativeTransitionCertificateProfile = "native-first-certificate-transition-v1"

type nativeTransitionCertificateOriginal struct {
	Coordinator                  uint64
	Proposal, Manifest, Decision []byte
}

type nativeTransitionCertificateArtifact struct {
	Version                                    uint8
	Profile, Case                              string
	Recipe                                     nativeTransitionCutRecipe
	Run                                        [16]byte
	DeadlineUnixNanos                          int64
	Gate                                       nativeLogicalTransitionReceipt
	KillState, ReadRequest, ReadFailure        []byte
	ReadFailureMask                            uint16
	Initial, Before, Current, Replay, Retained p06Observation
	Reopened, RejoinedCurrent                  p06Observation
	ReopenedCurrent                            *p06Observation
	Originals                                  []nativeTransitionCertificateOriginal
	Processes                                  []pairedFaultProcess
	From, Through                              [2]uint64
	Records                                    []lifecycleRawRecord
}

func nativeTransitionCertificateRecipeValid(recipe nativeTransitionCutRecipe) bool {
	return recipe.valid() && recipe.Phase == graphCutCertify
}

// Record correspondence from an actual returned immutable decision, without
// invoking complete(), whose Fresh would publish the certificate under test.
func nativeTransitionCertificateBusiness(t *testing.T, f *pairedGraphFaultNetwork, stage int) (nativeTransitionCertificateOriginal, crossProcessResult) {
	t.Helper()
	proposal := f.proposal(stage)
	command, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	actor, coordinator, logical := uint64(1), uint64(3), "node-a"
	if stage == 2 {
		actor, coordinator, logical = 4, 8, "node-b"
	}
	tx := crossTransactionID(namespace{command.request.ns.graph, coordinator}, command.request.id)
	hub := f.n.all[actor].mux.hub
	manifest := nativeTransitionCutManifest(t, hub, tx)
	result := f.run(actor, proposal)
	if result.decision.tx != tx || result.decision.ns.partition != coordinator || !result.decision.commit {
		t.Fatal("actual independent business original")
	}
	f.trace.OperationMapping = append(f.trace.OperationMapping, lifecycleOperationMap{Logical: logical, Coordinator: coordinator,
		Request: hex.EncodeToString(command.request.id[:]), Transaction: hex.EncodeToString(result.decision.tx[:]), Round: result.decision.round})
	actualManifest := manifest()
	// run drained the real peer work before this observation hook is removed.
	hub.mu.Lock()
	hub.beforeRelay = nil
	hub.mu.Unlock()
	decision, err := encodeCrossStatus(result.decision)
	if err != nil {
		t.Fatal(err)
	}
	return nativeTransitionCertificateOriginal{coordinator, proposal.wire, actualManifest, decision}, result
}

func nativeTransitionCertificateFirst(records []lifecycleRawRecord, partition, round uint64) (uint64, error) {
	var first uint64
	for _, record := range records {
		if record.Partition != partition || !bytes.HasPrefix(record.Outcome, []byte("GSO\x01")) {
			continue
		}
		result, err := decodeGraphCutResult(record.Outcome)
		if err != nil || result.index != record.Index || len(record.CDC) != 0 {
			return 0, errors.Join(errCorrupt, err)
		}
		if result.phase == graphCutCertify && result.round == round && result.status == graphCutApplied && (first == 0 || result.index < first) {
			first = result.index
		}
	}
	if first == 0 {
		return 0, errInvalid
	}
	return first, nil
}

func nativeTransitionCertificateCDC(t *testing.T, records []lifecycleRawRecord, originals []nativeTransitionCertificateOriginal) {
	t.Helper()
	for stage, original := range originals {
		decision, err := decodeCrossStatus(original.Decision)
		if err != nil {
			t.Fatal(err)
		}
		owner, life := graphstate.EntityID(1), graphstate.LifeID(2)
		if stage == 1 {
			owner, life = 17, 18
		}
		resolved := make(map[uint64]bool)
		for _, record := range records {
			status, err := decodeCrossStatus(record.Outcome)
			if err != nil || status.tx != decision.tx || status.phase != crossResolve || status.index != record.Index {
				continue
			}
			if resolved[record.Partition] || !status.commit || status.digest != decision.digest || status.round != decision.round {
				t.Fatal("actual node original Resolve/CDC identity")
			}
			resolved[record.Partition] = true
			expected := outcome{ns: status.ns, kind: graphOperations, identity: status.tx, hash: status.digest, index: status.index, disposition: applied}
			logical, err := decodeTypedChangeEnvelope(record.CDC, expected, defaultMaterializerLimits(), true)
			if err != nil {
				t.Fatal("complete actual node CDC", err)
			}
			if record.Partition != original.Coordinator {
				if len(logical.groups)+len(logical.entities)+len(logical.lives)+len(logical.values) != 0 {
					t.Fatal("validation-only participant emitted business effects")
				}
				continue
			}
			if len(logical.groups) != 1 || len(logical.entities) != 1 || len(logical.lives) != 1 || len(logical.values) != 0 || logical.entities[0].ID != owner || logical.entities[0].Kind != graphstate.Node || logical.entities[0].Source != 0 || logical.entities[0].Target != 0 || logical.lives[0].Owner != owner || logical.lives[0].Life != life || logical.lives[0].SourceLife != 0 || logical.lives[0].TargetLife != 0 {
				t.Fatal("literal node declarations missing/foreign/duplicated")
			}
			group := logical.groups[0]
			if group.Key != (graphstate.ComponentKey{Owner: owner, Kind: graphstate.Presence}) || len(group.Changes) != 1 {
				t.Fatal("node presence effect footprint")
			}
			change := group.Changes[0]
			if change.Before().Present() || change.Before().Revision().ID() != 0 || !change.After().Present() || change.After().Value().ID() != uint64(life) || change.After().Revision().ID() != decision.round || change.After().Revision().Provenance() != uint64(stage+1) {
				t.Fatal("node original before/after/revision/provenance")
			}
			p06CheckScope(t, change.Scope())
		}
		if len(resolved) != 2 || !resolved[3] || !resolved[8] {
			t.Fatal("complete two-participant original CDC")
		}
	}
}

func nativeTransitionCertificateCase(t *testing.T, recipe nativeTransitionCutRecipe) string {
	t.Helper()
	if !nativeTransitionCertificateRecipeValid(recipe) {
		t.Fatal("finite first-certificate selector")
	}
	var run [16]byte
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_RUN"); raw != "" {
		wire, err := hex.DecodeString(raw)
		if err != nil || len(wire) != len(run) || hex.EncodeToString(wire) != raw {
			t.Fatal("original run identity", err)
		}
		copy(run[:], wire)
	} else if _, err := rand.Read(run[:]); err != nil {
		t.Fatal(err)
	}
	if run == ([16]byte{}) {
		t.Fatal("zero run identity")
	}
	deadline := time.Now().Add(3 * time.Minute).UnixNano()
	if raw := os.Getenv("RHO_GRAPH_TRANSITION_DEADLINE"); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || strconv.FormatInt(value, 10) != raw || value <= time.Now().UnixNano() || value-time.Now().UnixNano() > int64(1800*time.Second) {
			t.Fatal("original whole-run deadline", err)
		}
		deadline = value
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancel()
	t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(deadline, 10))
	t.Setenv("RHO_GRAPH_TRANSITION_PROCESS", "1")
	f := newPairedGraphFaultNetwork(t)
	// Capture before any Fresh. The independently retained prefix must include
	// the initial, old and new certificate controls, not only business entries.
	from := f.indexes()
	initial := p06Observe(f, 6, graphReadFresh, graphCut{})
	originalA, resultA := nativeTransitionCertificateBusiness(t, f, 1)
	before := p06Observe(f, 6, graphReadFresh, graphCut{})
	oldCut, err := pairedFaultCut(before.Cut)
	if err != nil || len(oldCut.certificates) != 2 || oldCut.certificates[0].round < resultA.decision.round {
		t.Fatal("genuine earlier business cut", err)
	}
	originalB, resultB := nativeTransitionCertificateBusiness(t, f, 2)
	if resultB.decision.round <= oldCut.certificates[0].round {
		t.Fatal("later business original did not establish a new observed floor")
	}
	// No Fresh occurs after B and before this arm. The selected command's round
	// comes from the native installed floor; no caller increment creates it.
	member, successor := uint64(1), uint64(2)
	if recipe.Partition == 8 {
		member, successor = 4, 5
	}
	owner := nativeLogicalTransitionCall(t, f.n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	arm := nativeLogicalTransitionControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Family: "cut", Phase: graphCutCertify, Side: recipe.Side, Partition: recipe.Partition}
	nativeLogicalTransitionCall(t, f.n.all[member], arm)
	request, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: graphReadFresh})
	if err != nil {
		t.Fatal(err)
	}
	readCtx, cancelRead := context.WithTimeout(ctx, 20*time.Second)
	defer cancelRead()
	done := make(chan graphProcessAnswer, 1)
	go func() {
		response, actual := f.n.all[6].exchange(readCtx, processRequest{op: processSourceCertifiedOpen, data: request})
		done <- graphProcessAnswer{response: response, err: actual}
	}()
	gate := nativeLogicalTransitionCall(t, f.n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "await", Cookie: owner.Cookie, Sequence: 1})
	command, err := decodeGraphCutCommand(gate.Proposal)
	if err != nil || gate.Control != arm || gate.State != "reached" || gate.ProposalSHA256 != sha256.Sum256(gate.Proposal) || command.phase != graphCutCertify || command.scope.partition != recipe.Partition || command.round != resultB.decision.round {
		t.Fatal("actual first Certify at later installed business floor", err)
	}
	if recipe.Side == "after-applied" {
		actual := f.n.all[member].call(t, processRequest{op: processEntryRecord, index: gate.Index})
		result, err := decodeGraphCutResult(gate.Outcome)
		if err != nil || gate.Index == 0 || gate.Term == 0 || gate.Root.Index != gate.Index || !bytes.Equal(gate.EntryData, gate.Proposal) || !bytes.Equal(actual.outcome, gate.Outcome) || len(actual.changes)+len(gate.CDC) != 0 || result.phase != graphCutCertify || result.round != command.round || result.index != gate.Index || result.status != graphCutApplied {
			t.Fatal("actual first certificate result", err)
		}
	} else if gate.Index != 0 || len(gate.EntryData)+len(gate.Outcome)+len(gate.CDC) != 0 {
		t.Fatal("unsubmitted certificate became durable authority")
	}
	killState := f.n.all[member].call(t, processRequest{op: processStateOp})
	if recipe.Side == "after-applied" && killState.applied < gate.Index {
		t.Fatal("selected actual result not applied before kill")
	}
	killed := f.kill(member, "first-certificate-source")
	var failed processResponse
	var failureMask uint16
	select {
	case answer := <-done:
		failed = answer.response
		failureMask = graphErrorMask(answer.err)
		if nativeTransitionPinErrorClass(answer.err) != "pending-or-unavailable" || failed.code != 3 && failed.code != 6 || failed.causeMask & ^uint16(1<<7) != 0 || len(failed.aux)+len(failed.outcome)+len(failed.changes) != 0 {
			t.Fatal("interrupted Fresh invented a complete new certificate")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("interrupted first-certificate caller not reaped")
	}
	f.elect(member, successor)
	p03Reconnect(f, 6)
	current := p06Observe(f, 6, graphReadFresh, graphCut{})
	newCut, err := pairedFaultCut(current.Cut)
	if err != nil || len(newCut.certificates) != 2 || newCut.certificates[0].round != resultB.decision.round {
		t.Fatal("genuine healed Fresh did not return complete new cut", err)
	}
	replay := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, oldCut)
	var reopened p06Observation
	var reopenedCurrent *p06Observation
	rejoined := f.replaceBeforeHeal(member, func(replacement *graphProcess, _ processResponse) {
		reopened = p06Observe(f, member, graphReadAt, oldCut)
		if recipe.Side == "after-applied" {
			actual := replacement.call(t, processRequest{op: processEntryRecord, index: gate.Index})
			if !bytes.Equal(actual.outcome, gate.Outcome) || !bytes.Equal(actual.changes, gate.CDC) {
				t.Fatal("replacement lost its local first certificate original")
			}
			// This At uses ONLY the full new cut already returned by healed
			// Fresh. It proves this replacement's stored GSC/local old source;
			// it does not synthesize an absent other-participant certificate.
			actualView := p06Observe(f, member, graphReadAt, newCut)
			reopenedCurrent = &actualView
		}
	})
	rejoinedCurrent := p06Observe(f, member, graphReadAt, newCut)
	if !reflect.DeepEqual(before, retained) || !reflect.DeepEqual(before, reopened) || !reflect.DeepEqual(current, replay) || !reflect.DeepEqual(current, rejoinedCurrent) || reopenedCurrent != nil && !reflect.DeepEqual(current, *reopenedCurrent) {
		t.Fatal("first publication/repetition/reopen substituted a different cut or state")
	}
	for stage, observation := range []p06Observation{initial, before, current} {
		if !reflect.DeepEqual(observation.View, literalLifecycleView(stage)) {
			t.Fatal("literal independently declared node lifecycle", stage, observation.View)
		}
	}
	through := f.indexes()
	records := p06Records(f, from, through)
	nativeTransitionCertificateCDC(t, records, []nativeTransitionCertificateOriginal{originalA, originalB})
	for _, certificate := range newCut.certificates {
		index, err := nativeTransitionCertificateFirst(records, certificate.scope.partition, certificate.round)
		if err != nil || index != certificate.index || certificate.scope.partition == recipe.Partition && (recipe.Side == "after-applied" && index != gate.Index || recipe.Side == "before-submit" && index <= killState.applied) {
			t.Fatal("stored new GSC must retain its first actual publication index", err)
		}
	}
	if err := ctx.Err(); err != nil {
		t.Fatal("original whole-run deadline", err)
	}
	artifact := nativeTransitionCertificateArtifact{Version: 1, Profile: nativeTransitionCertificateProfile, Case: fmt.Sprintf("first-certify-p%d-%s", recipe.Partition, recipe.Side), Recipe: recipe, Run: run, DeadlineUnixNanos: deadline, Gate: gate,
		KillState: p06ProcessWire(t, killState), ReadRequest: request, ReadFailure: p06ProcessWire(t, failed), ReadFailureMask: failureMask, Initial: initial, Before: before, Current: current, Replay: replay, Retained: retained, Reopened: reopened, ReopenedCurrent: reopenedCurrent, RejoinedCurrent: rejoinedCurrent,
		Originals: []nativeTransitionCertificateOriginal{originalA, originalB}, Processes: []pairedFaultProcess{killed, rejoined}, From: from, Through: through}
	return nativeTransitionWriteRows(t, filepath.Join(f.n.dir, "native-first-certificate-transition.jsonl"), artifact, records, through)
}
