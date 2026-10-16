package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

func graphE17Prepare(t *testing.T, n *certifiedProcessNetwork, actor, kind, owner uint64, data []byte) allocationProposal {
	t.Helper()
	response := n.all[actor].call(t, processRequest{op: processCertifiedAction, entity: kind, index: owner, data: data})
	proposal, err := decodeCertifiedProcessProposal(response.aux)
	partition := uint64(3)
	if actor > 3 {
		partition = 8
	}
	if err != nil || proposal.target.partition != partition || proposal.target.semantic != rangeMoveSemanticContractID() {
		t.Fatal("actual child proposal escaped range-move binding", proposal.target, err)
	}
	return proposal
}

func graphE17SubmitLocal(t *testing.T, n *certifiedProcessNetwork, actor uint64, proposal allocationProposal, want reason) outcome {
	t.Helper()
	wire, err := encodeCertifiedProcessProposal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	before := n.all[actor].call(t, processRequest{op: processStateOp})
	n.group(actor).event(t, actor, processRequest{op: processPropose, data: wire})
	after := n.all[actor].call(t, processRequest{op: processStateOp})
	if after.applied <= before.applied {
		t.Fatal("native proposal did not commit")
	}
	ns, _ := certifiedProcessNamespace(actor)
	original := n.all[actor].call(t, processRequest{op: processEntryRecord, index: after.applied})
	result, err := decodePartitionGraphOutcome(original.outcome, ns)
	if err != nil || result.reason != want || result.disposition != applied || result.hash != sha256.Sum256(proposal.wire) || result.index != after.applied {
		t.Fatal("actual native local outcome", result, want, err)
	}
	if want != reasonNone && (len(original.changes) != 0 || !bytes.Equal(before.image, after.image)) {
		t.Fatal("delayed old-owner write changed graph state or emitted CDC")
	}
	if want == reasonNone && len(original.changes) == 0 {
		t.Fatal("successful mutation emitted no business CDC")
	}
	for id, voter := range n.group(actor).nodes {
		got := voter.call(t, processRequest{op: processEntryRecord, index: result.index})
		state := voter.call(t, processRequest{op: processStateOp})
		if !bytes.Equal(got.outcome, original.outcome) || !bytes.Equal(got.changes, original.changes) || !bytes.Equal(state.image, after.image) {
			t.Fatal("local original outcome/CDC/root differs on voter", id)
		}
	}
	return result
}

func graphE17Cross(t *testing.T, n *certifiedProcessNetwork, actor uint64, proposal allocationProposal) crossProcessResult {
	t.Helper()
	response := n.all[actor].call(t, processRequest{op: processSourceCoordinate, data: proposal.wire})
	result, err := decodeGraphProcessCoordinatorResult(response)
	if err != nil || result.decision.phase != crossDecide || !result.decision.commit || result.decision.round == 0 {
		t.Fatal("actual authenticated cross coordinator did not commit", result, err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	return result
}

func graphE17Open(t *testing.T, n *certifiedProcessNetwork, actor uint64, mode graphReadMode, cut graphCut) (uint64, graphCut) {
	t.Helper()
	wire, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: cut})
	if err != nil {
		t.Fatal(err)
	}
	response := n.all[actor].call(t, processRequest{op: processSourceCertifiedOpen, data: wire})
	handle, actual, err := decodeGraphE15ReadToken(response.aux)
	if err != nil || len(actual.certificates) != 2 {
		t.Fatal("actual persisted certified cut", actual, err)
	}
	for _, certificate := range actual.certificates {
		if certificate.scope.semantic != rangeMoveSemanticContractID() {
			t.Fatal("E17 did not use fresh range agreement")
		}
	}
	return handle, actual
}

func graphE17Observe(t *testing.T, n *certifiedProcessNetwork, actor, handle uint64) map[uint64]graphE17Fact {
	t.Helper()
	n.e17PublicUsed++
	if n.e17PublicUsed > n.e17PublicPhases {
		t.Fatal("undeclared E17 public oracle phase")
	}
	facts := make(map[uint64]graphE17Fact)
	for _, ids := range [][]uint64{{1, 3, 17}, {7}} {
		input, err := encodeGraphProcessObserveIDs(ids)
		if err != nil {
			t.Fatal(err)
		}
		response := n.all[actor].call(t, processRequest{op: processRangeCertifiedDetail, index: handle, data: input})
		actual, err := decodeGraphE17Facts(response.aux)
		if err != nil || len(actual) != len(ids) {
			t.Fatal("bounded native details", err, len(actual), ids)
		}
		for i, fact := range actual {
			if fact.id != ids[i] {
				t.Fatal("exact observation IDs", fact.id, ids[i])
			}
			if _, duplicate := facts[fact.id]; duplicate {
				t.Fatal("duplicate observed ID", fact.id)
			}
			facts[fact.id] = fact
		}
	}
	return facts
}

func graphE17ExpectedIdentity(t *testing.T, id uint64) []byte {
	t.Helper()
	kind, mode := graphstate.Node, graphstate.ReferenceMode(0)
	interpretation, role := graphstate.InterpretationObservation, graphstate.TemporalRoleValidity
	name := ""
	var source, target, sourceLife, targetLife uint64
	if id == 3 {
		kind, mode = graphstate.Relationship, graphstate.LifeBound
		interpretation, role = graphstate.InterpretationAssertedRelation, graphstate.TemporalRoleOccurrenceTime
		name, source, target, sourceLife, targetLife = "cross-edge", 1, 17, 2, 18
	}
	wire, err := processEncoding(func(w *boundedWriter) {
		w.u64(id)
		w.tag(byte(kind))
		w.tag(byte(mode))
		w.tag(byte(interpretation))
		w.tag(byte(role))
		w.text(name)
		w.u64(source)
		w.u64(target)
		axis := graphGenesisBinding(t).Axis().DefinitionHash()
		w.add(axis[:])
		w.u64(id)
		w.u64(id + 1)
		w.u64(sourceLife)
		w.u64(targetLife)
	})
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func graphE17AssertFacts(t *testing.T, facts map[uint64]graphE17Fact, changed bool) {
	t.Helper()
	if len(facts) != 4 {
		t.Fatal("exact cardinality", len(facts))
	}
	oracle := newCertifiedProcessOracle()
	oracle.crossEdge()
	for _, id := range []uint64{1, 3, 17, 7} {
		f, found := facts[id]
		if !found || f.id != id {
			t.Fatal("missing exact ID", id)
		}
		if err := oracle.compare(id, f.fact); err != nil {
			t.Fatal(err)
		}
		if id == 7 {
			if len(f.identity)+len(f.presence)+len(f.property) != 0 || f.valueID != 0 || f.revision != 0 || f.provenance != 0 || f.value != 0 {
				t.Fatal("phantom entity returned metadata/value", f)
			}
			continue
		}
		if !bytes.Equal(f.identity, graphE17ExpectedIdentity(t, id)) || len(f.presence) == 0 {
			t.Fatal("stable native entity/axis/life/endpoint identity changed", id)
		}
		if id == 3 {
			if len(f.property) != 0 || f.valueID != 0 || f.revision != 0 || f.value != 0 {
				t.Fatal("relationship gained phantom property")
			}
			continue
		}
		wantRef, wantValue := uint64(5), int64(7)
		if id == 17 {
			wantRef, wantValue = 19, 13
		}
		if id == 1 && changed {
			wantRef, wantValue = 20, 8
		}
		if f.valueID != wantRef || f.value != wantValue || f.revision == 0 || f.provenance != 23 || len(f.property) == 0 {
			t.Fatal("exact typed value/reference/revision", id, f.valueID, f.value, f.revision, f.provenance)
		}
	}
}

func graphE17Placement(t *testing.T, n *certifiedProcessNetwork, actor, rangeID uint64) (graphstore.RangePublication, graphstore.RangeOwnership) {
	t.Helper()
	response := n.all[actor].call(t, processRequest{op: processRangeEvidenceProbe, entity: rangeID})
	c := graphCursor{b: response.aux}
	publication, err := graphstore.DecodeRangePublication(c.field(256))
	if err != nil {
		t.Fatal(err)
	}
	owner, err := graphstore.DecodeRangeOwnership(c.field(256))
	if err != nil || c.err != nil || len(c.b) != 0 {
		t.Fatal("actual placement", err, c.err)
	}
	return publication, owner
}

func graphE17AssertNativeMoveReceipts(t *testing.T, n *certifiedProcessNetwork, result rangeMoveResult) {
	t.Helper()
	for _, receipt := range []rangeMoveStatus{result.source, result.target, result.decision} {
		leader := uint64(1)
		if receipt.ns.partition == 8 {
			leader = 4
		}
		original := n.all[leader].call(t, processRequest{op: processEntryRecord, index: receipt.index})
		got, err := decodeRangeMoveStatus(original.outcome)
		if err != nil || got != receipt || len(original.changes) != 0 {
			t.Fatal("native finish/decision receipt changed or gained business CDC", got, receipt, err)
		}
		for id, voter := range n.group(leader).nodes {
			actual := voter.call(t, processRequest{op: processEntryRecord, index: receipt.index})
			if !bytes.Equal(actual.outcome, original.outcome) || !bytes.Equal(actual.changes, original.changes) {
				t.Fatal("original handoff receipt differs on voter", id)
			}
		}
	}
}

// This is the successful physical-process handoff gate. Crash/restart phase
// permutations are deliberately separate; reopening At here opens a new actual
// certified read session against persisted old certificates, not a child restart.
func TestGraphRangeMoveSixProcessesActivatesAllocatedRangeWithoutChangingIDs(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n := newCertifiedProcessNetworkProfile(t, true)
	publication, oldOwner := graphE17Placement(t, n, 1, 1)
	otherSource, otherSourceOwner := graphE17Placement(t, n, 1, 2)
	otherTarget, otherTargetOwner := graphE17Placement(t, n, 4, 17)
	if publication.First != 1 || publication.Last != 1 || oldOwner.Partition != 3 || oldOwner.Epoch != 2 || oldOwner.Fenced || otherSource.First != 2 || otherSource.Last != 16 || otherTarget.First != 17 || otherTarget.Last != 32 {
		t.Fatal("actual split source grants and independent target range", publication, oldOwner, otherSource, otherTarget)
	}
	// Every business operation is prepared by its original child Host and
	// admitted through actual replicated native command paths.
	sourceProperty := graphE17Prepare(t, n, 1, certifiedRangeSet, 1, graphE17GrantInput(5, 7))
	graphE17SubmitLocal(t, n, 1, sourceProperty, reasonNone)
	targetProperty := graphE17Prepare(t, n, 4, certifiedRangeSet, 17, graphE17GrantInput(19, 13))
	graphE17Cross(t, n, 4, targetProperty)
	edge := graphE17Prepare(t, n, 1, certifiedCreateCrossEdge, 0, nil)
	graphE17Cross(t, n, 1, edge)
	// Hold a genuinely prepared old-owner mutation across the handoff. It has
	// not been submitted or given any decision/write authority yet.
	delayed := graphE17Prepare(t, n, 1, certifiedHealthyLabel, 1, nil)
	oldHandle, oldCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
	oldClosed := false
	defer func() {
		if !oldClosed {
			n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})
		}
	}()
	oldFacts := graphE17Observe(t, n, 2, oldHandle)
	graphE17LogTraffic(t, n, "old-cut setup")
	graphE17AssertFacts(t, oldFacts, false)

	response := graphE17MoveExchange(t, n.all[1], processRequest{op: processRangeRun, entity: 1, index: 8, data: graphE17MoveBounds(n.e17MovePages, 8, 16<<10)})
	response, completed, err := graphE17CompleteMoveResponse(t, n, response)
	if err != nil || !completed.committed || completed.source.phase != rangeMoveFinish || completed.target.phase != rangeMoveFinish || completed.decision.phase != rangeMoveDecide || completed.decision.result != rangeMoveApplied || completed.source.result != rangeMoveApplied || completed.target.result != rangeMoveApplied || completed.decision.round == 0 || completed.decision.rows == 0 || completed.decision.size == 0 || completed.decision.digest == ([32]byte{}) || completed.source.round != completed.decision.round || completed.target.round != completed.decision.round || completed.source.digest != completed.decision.digest || completed.target.digest != completed.decision.digest {
		t.Fatal("actual six-child range handoff did not finish with original COMMIT", completed, err)
	}
	graphE17LogTraffic(t, n, "move completed")
	graphE17AssertNativeMoveReceipts(t, n, completed)
	again := n.all[1].call(t, processRequest{op: processRangeResume, data: response.response.aux})
	resumed, err := decodeGraphE17MoveResult(again)
	if err != nil || resumed != completed {
		t.Fatal("actual native Resume changed original outcome", resumed, completed, err)
	}
	for actor := uint64(1); actor <= 6; actor++ {
		actualPublication, owner := graphE17Placement(t, n, actor, 1)
		if actualPublication != publication || owner.Graph != oldOwner.Graph || owner.RangeID != oldOwner.RangeID || owner.Configuration != oldOwner.Configuration || owner.Partition != 8 || owner.Epoch != oldOwner.Epoch+1 || owner.Fenced {
			t.Fatal("stable publication changed or placement not activated on native voter", actor, actualPublication, owner)
		}
		if actor <= 3 {
			p, o := graphE17Placement(t, n, actor, 2)
			if p != otherSource || o != otherSourceOwner {
				t.Fatal("unmoved source grant changed", actor, p, o)
			}
		} else {
			p, o := graphE17Placement(t, n, actor, 17)
			if p != otherTarget || o != otherTargetOwner {
				t.Fatal("unrelated target grant changed", actor, p, o)
			}
		}
	}
	currentHandle, currentCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
	currentFacts := graphE17Observe(t, n, 2, currentHandle)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: currentHandle})
	graphE17AssertFacts(t, currentFacts, false)
	if !reflect.DeepEqual(currentFacts, oldFacts) {
		t.Fatal("move changed exact public graph facts")
	}
	for i, certificate := range currentCut.certificates {
		if certificate.scope != oldCut.certificates[i].scope || certificate.configuration != oldCut.certificates[i].configuration {
			t.Fatal("handoff changed immutable cut authority")
		}
	}
	// Native At validates the SAME persisted cut independently on every voter.
	// Complete graph KV equality carries exact typed cells, identities, values,
	// both incidence trees and unrelated data; public facts remain the oracle.
	graphE17AssertNativeVoters(t, n, currentCut, processRangeNativeVoterProbe, nil)

	graphE17LogTraffic(t, n, "public current + six native voters")
	graphE17SubmitLocal(t, n, 1, delayed, reasonRemoteParticipant)
	graphE17LogTraffic(t, n, "delayed refusal")
	mutation := graphE17Prepare(t, n, 4, certifiedRangeSet, 1, graphE17GrantInput(20, 8))
	decision := graphE17Cross(t, n, 4, mutation)
	graphE17LogTraffic(t, n, "new-owner mutation")

	handle, changedCut := graphE17Open(t, n, 2, graphReadFresh, graphCut{})
	facts := graphE17Observe(t, n, 2, handle)
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: handle})
	graphE17AssertFacts(t, facts, true)
	if facts[1].revision != decision.decision.round || facts[1].revision == oldFacts[1].revision || bytes.Equal(facts[1].property, oldFacts[1].property) || !bytes.Equal(facts[1].presence, oldFacts[1].presence) || !bytes.Equal(facts[1].identity, oldFacts[1].identity) {
		t.Fatal("new-owner mutation lost exact revision/identity")
	}
	for _, id := range []uint64{3, 17, 7} {
		if !reflect.DeepEqual(facts[id], oldFacts[id]) {
			t.Fatal("mutation changed unrelated exact graph fact", id)
		}
	}
	graphE17AssertNativeVoters(t, n, changedCut, processRangeNativeVoterProbe, nil)

	graphE17LogTraffic(t, n, "public changed + six native voters")
	// Two independent old-read doors must retain the nonempty old answer after
	// genuine target mutation. Returning current state fails the literal value7
	// oracle and exact original component/revision bytes through both doors.
	held := graphE17Observe(t, n, 2, oldHandle)
	graphE17AssertFacts(t, held, false)
	if !reflect.DeepEqual(held, oldFacts) {
		t.Fatal("held old cut changed after target mutation")
	}
	n.all[2].call(t, processRequest{op: processSourceCertifiedClose, index: oldHandle})
	oldClosed = true
	reopened, reopenedCut := graphE17Open(t, n, 5, graphReadAt, oldCut)
	reopenedFacts := graphE17Observe(t, n, 5, reopened)
	n.all[5].call(t, processRequest{op: processSourceCertifiedClose, index: reopened})
	graphE17AssertFacts(t, reopenedFacts, false)
	if !reflect.DeepEqual(reopenedFacts, oldFacts) || !reflect.DeepEqual(reopenedCut, oldCut) {
		t.Fatal("reopened persisted At substituted current target state")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := drainGraphProcessMuxes(ctx, n.all); err != nil {
		t.Fatal(err)
	}
	for id, child := range n.all {
		state := child.call(t, processRequest{op: processStateOp})
		if state.stats[statViews] != 0 || state.stats[statPinnedBytes] != 0 || len(state.handles) != 2 || state.handles[0] != 0 || state.handles[1] != 0 {
			t.Fatal("completed process move/read leaked owner/workers/views/pins", id, state.stats, state.handles)
		}
	}
	// newCertifiedProcessNetworkProfile retains the existing bounded cleanup,
	// six successful OS-process reaps, pipe closure, and aggregate-delivery gate.
}

func graphE17LogTraffic(t *testing.T, n *certifiedProcessNetwork, phase string) {
	t.Helper()
	var calls uint64
	for _, child := range n.all {
		child.mux.mu.Lock()
		calls += child.sequence
		child.mux.mu.Unlock()
	}
	t.Logf("E17 traffic %s: calls=%d bootstrapPackets=%d", phase, calls, n.groups[0].delivered+n.groups[1].delivered)
}
