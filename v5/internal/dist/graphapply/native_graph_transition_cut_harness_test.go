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
	"sync"
	"testing"
	"time"
)

const nativeTransitionCutProfile = "native-owned-cut-transition-v1"

type nativeTransitionCutRecipe struct {
	Version   uint8
	Phase     byte
	Partition uint64
	Side      string
}

func (r nativeTransitionCutRecipe) valid() bool {
	return r.Version == 1 && (r.Phase == graphCutFence || r.Phase == graphCutCertify) && (r.Partition == 3 || r.Partition == 8) && (r.Side == "before-submit" || r.Side == "after-applied")
}

type nativeCutTransitionArtifact struct {
	nativeFaultArtifact
	Recipe                    nativeTransitionCutRecipe
	Gate                      nativeLogicalTransitionReceipt
	HeldAfter                 nativeLogicalTransitionReceipt
	Run                       [16]byte
	DeadlineUnixNanos         int64
	ReadRequest, ReadFailure  []byte
	Concurrent, OriginalRetry []byte
}

// Observe only an actual submitted command of the declared business original.
// The retained bytes are diagnostic evidence, never imported proposal authority.
// One maximum-size manifest is funded before installing the callback; its exact
// capacity is owned once, and repeated genuine stages must carry the same bytes.
func nativeTransitionCutManifest(t *testing.T, hub *graphProcessHub, tx [16]byte) func() []byte {
	t.Helper()
	var mu sync.Mutex
	storage := make([]byte, nativeLogicalTransitionBodyBytes)
	var manifest []byte
	hub.mu.Lock()
	if hub.beforeRelay != nil {
		hub.mu.Unlock()
		t.Fatal("existing directed transport observer")
	}
	hub.beforeRelay = func(_ context.Context, _, _ uint64, request []byte) error {
		command, selected, err := nativeFaultPhase(request)
		if err != nil || !selected {
			return err
		}
		body, err := graphBody(command.manifest, "GCM\x01", nativeLogicalTransitionBodyBytes)
		if err != nil || len(body) < len(tx) {
			return errors.Join(errCorrupt, err)
		}
		if !bytes.Equal(body[:len(tx)], tx[:]) {
			return nil
		}
		mu.Lock()
		defer mu.Unlock()
		if len(command.manifest) > len(storage) || manifest != nil && !bytes.Equal(manifest, command.manifest) {
			return errCorrupt
		}
		if manifest == nil {
			copy(storage, command.manifest)
			manifest = storage[:len(command.manifest):len(command.manifest)]
		}
		return nil
	}
	hub.mu.Unlock()
	t.Cleanup(func() { hub.mu.Lock(); hub.beforeRelay = nil; hub.mu.Unlock() })
	return func() []byte {
		mu.Lock()
		defer mu.Unlock()
		if len(manifest) == 0 {
			t.Fatal("no actual submitted original manifest")
		}
		return manifest
	}
}

func nativeTransitionCutCase(t *testing.T, recipe nativeTransitionCutRecipe) string {
	t.Helper()
	if !recipe.valid() {
		t.Fatal("finite actual cut recipe")
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
	ctx, cancelRun := context.WithDeadline(t.Context(), time.Unix(0, deadline))
	defer cancelRun()
	t.Setenv("RHO_GRAPH_TRANSITION_RUN", hex.EncodeToString(run[:]))
	t.Setenv("RHO_GRAPH_TRANSITION_DEADLINE", strconv.FormatInt(deadline, 10))
	t.Setenv("RHO_GRAPH_TRANSITION_PROCESS", "1")
	f := newPairedGraphFaultNetwork(t)
	before := nativeFaultSeed(t, f)
	old := p06Observe(f, 6, graphReadFresh, graphCut{})
	cut, err := pairedFaultCut(old.Cut)
	if err != nil || len(cut.certificates) != 2 {
		t.Fatal(err)
	}
	p06AssertView(t, old.View, false, false, false)
	proposal, from := f.proposal(3), f.indexes()
	original, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	tx := crossTransactionID(namespace{original.request.ns.graph, 8}, original.request.id)
	manifest := nativeTransitionCutManifest(t, f.n.all[6].mux.hub, tx)
	member, successor := uint64(1), uint64(2)
	if recipe.Partition == 8 {
		member, successor = 4, 5
	}
	owner := nativeLogicalTransitionCall(t, f.n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	arm := nativeLogicalTransitionControl{Version: 1, Action: "arm", Cookie: owner.Cookie, Sequence: 1, Family: "cut", Phase: recipe.Phase, Side: recipe.Side, Partition: recipe.Partition}
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
	if err != nil || gate.Control != arm || gate.State != "reached" || gate.ProposalSHA256 != sha256.Sum256(gate.Proposal) || command.phase != recipe.Phase || command.scope.partition != recipe.Partition || command.round < cut.certificates[0].round {
		t.Fatal("missing genuine source-selected cut command", err)
	}
	if recipe.Side == "after-applied" {
		actual := f.n.all[member].call(t, processRequest{op: processEntryRecord, index: gate.Index})
		result, err := decodeGraphCutResult(gate.Outcome)
		if err != nil || gate.Index == 0 || gate.Term == 0 || gate.Root.Index != gate.Index || !bytes.Equal(gate.EntryData, gate.Proposal) || !bytes.Equal(gate.Outcome, actual.outcome) || len(actual.changes) != 0 || len(gate.CDC) != 0 || result.phase != recipe.Phase || result.round != command.round || result.index != gate.Index || result.status != graphCutApplied {
			t.Fatal("actual durable cut result", err)
		}
	} else if gate.Index != 0 || len(gate.EntryData)+len(gate.Outcome)+len(gate.CDC) != 0 {
		t.Fatal("before-submit command invented durable authority")
	}
	// The genuine Fresh caller remains held. A separate client commits a real
	// whole-edge original, producing a graph different from the held old cut.
	// A before-first-Fence command is not yet a certificate: its selected round
	// need not be strictly below this concurrent decision round. Fresh uses the
	// actual observed floor, so an unchanged graph can reuse the old round and
	// certificate. Its submitted GSO is a real new native entry, not a claim of
	// newly created certificate state or an artificial round increment.
	concurrent, actual := f.n.all[5].exchange(ctx, processRequest{op: processSourceCoordinate, data: proposal.wire})
	result, resultErr := decodeGraphProcessCoordinatorResult(concurrent)
	if err := errors.Join(actual, resultErr); err != nil || !result.decision.commit || result.decision.tx != tx || result.decision.round <= cut.certificates[0].round {
		t.Fatal("genuine concurrent whole transaction", err)
	}
	held := nativeLogicalTransitionCall(t, f.n.all[member], nativeLogicalTransitionControl{Version: 1, Action: "inspect"})
	if !reflect.DeepEqual(held, gate) {
		t.Fatal("selected cut owner stopped holding before concurrent COMMIT")
	}
	killed := f.kill(member, "cut-source")
	var failed processResponse
	select {
	case answer := <-done:
		failed = answer.response
		if errors.Join(answer.err, processResponseError(failed)) == nil || failed.code != 3 && failed.code != 6 || len(failed.aux)+len(failed.outcome)+len(failed.changes) != 0 {
			t.Fatal("interrupted Fresh invented certificate/decision")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("interrupted read not reaped")
	}
	f.elect(member, successor)
	p03Reconnect(f, 6)
	retried, actual := f.n.all[6].exchange(ctx, processRequest{op: processSourceCoordinate, data: proposal.wire})
	replay, replayErr := decodeGraphProcessCoordinatorResult(retried)
	if err := errors.Join(actual, replayErr); err != nil || replay.decision != result.decision {
		t.Fatal("same original recovery changed immutable decision", err)
	}
	_ = p03Complete(f, 6, "edge-create", proposal, replay, from, before)
	recovered := p06Observe(f, 6, graphReadFresh, graphCut{})
	retained := p06Observe(f, 6, graphReadAt, cut)
	var reopened p06Observation
	rejoined := f.replaceBeforeHeal(member, func(_ *graphProcess, _ processResponse) { reopened = p06Observe(f, member, graphReadAt, cut) })
	if !reflect.DeepEqual(old, retained) || !reflect.DeepEqual(old, reopened) {
		t.Fatal("cut/restart substituted current for old source")
	}
	p06AssertView(t, recovered.View, true, false, false)
	through := f.indexes()
	if err := ctx.Err(); err != nil {
		t.Fatal("original cut transition deadline", err)
	}
	artifact := nativeCutTransitionArtifact{nativeFaultArtifact: nativeFaultArtifact{Version: 3, Profile: nativeTransitionCutProfile, Case: fmt.Sprintf("cut-%d-p%d-%s", recipe.Phase, recipe.Partition, recipe.Side), Original: proposal.wire, Manifest: manifest(), Before: old, Retained: retained, Recovered: recovered, Reopened: reopened, Processes: []pairedFaultProcess{killed, rejoined}, From: from, Through: through, Records: p06Records(f, from, through)}, Recipe: recipe, Gate: gate, HeldAfter: held, Run: run, DeadlineUnixNanos: deadline, ReadRequest: request, ReadFailure: p06ProcessWire(t, failed), Concurrent: p06ProcessWire(t, concurrent), OriginalRetry: p06ProcessWire(t, retried)}
	records := artifact.Records
	artifact.Records = nil
	return nativeTransitionWriteRows(t, filepath.Join(f.n.dir, "native-cut-transition.jsonl"), artifact, records, through)
}
