package graphapply

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// This wire only disconnects genuine requests. Successful calls delegate to the
// native service, Host ReadIndex and actual voter pump; no reply is synthesized.
type rangeMoveKeeperWire struct {
	graphPeerWireTransport
	pauseDescribe, stopped, keeperUnavailable, allUnavailable bool
	originalQueries, calls                                    int
}

func (w *rangeMoveKeeperWire) RoundTrip(ctx context.Context, wire []byte) ([]byte, error) {
	w.calls++
	if w.allUnavailable {
		return nil, io.EOF
	}
	r, err := decodeGraphSourceRequest(wire)
	if err != nil {
		return nil, err
	}
	if r.kind == sourceMoveRead {
		q, err := decodeRangeMoveQuery(r.command)
		if err != nil {
			return nil, err
		}
		if q.origin != 0 {
			w.originalQueries++
			if w.keeperUnavailable {
				return nil, io.EOF
			}
		}
	}
	if w.pauseDescribe && r.kind == sourceSubmit {
		p, err := decodeGraphSourceProposal(r.command)
		if err != nil {
			return nil, err
		}
		command, err := decodeRangeMoveCommand(p.wire)
		if err != nil {
			return nil, err
		}
		if command.phase == rangeMoveDescribe && command.sequence == 0 {
			w.stopped = true
			return nil, io.EOF
		}
	}
	return w.graphPeerWireTransport.RoundTrip(ctx, wire)
}

func rangeMoveKeeperStored(t *testing.T, h *allocationHost, key []byte) ([]byte, bool) {
	t.Helper()
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	row, found, err := view.Get(t.Context(), key, 4096)
	if closeErr := view.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || found && row.Deleted {
		t.Fatal("genuine original point", found, err)
	}
	return row.Value, found
}

func rangeMoveKeeperResume(t *testing.T, c *rangeMoveCoordinator, resume rangeMoveResume) rangeMoveResult {
	t.Helper()
	var result rangeMoveResult
	var err error
	for range 128 {
		result, err = c.Resume(t.Context(), resume)
		if !errors.Is(err, errRangeMovePending) {
			break
		}
		if !errors.Is(err, errLimit) && !errors.Is(err, graphstate.ErrResourceLimit) && !errors.Is(err, graphstore.ErrResourceLimit) {
			t.Fatal("unexpected native resume refusal", result, err)
		}
	}
	if err != nil || !result.committed || result.source.phase != rangeMoveFinish || result.target.phase != rangeMoveFinish || result.decision.phase != rangeMoveDecide {
		t.Fatal("original move did not actually finish", result, err)
	}
	return result
}

func TestRangeMoveLookupOriginalKeepsLateRefusalAndCapturedSourceCoordinates(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, intent, _, transports := rangeMoveCoordinatorFixture(t, 64)
	source, target := n.hosts[0][0], n.hosts[1][0]
	origin := source.machine.ns
	capturedID, lateID, absentID := requestID{111}, requestID{112}, requestID{113}
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	write := func(group int, id requestID, name string, want reason) outcome {
		h := n.hosts[group][0]
		owner := graphstate.EntityID(1)
		if group == 1 {
			owner = 17
		}
		p, err := h.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: owner, Life: graphstate.LifeID(owner + 1), Scope: window, Name: name}}, 111, nil)
		if err != nil {
			t.Fatal("native original proposal", err)
		}
		return n.submitPartitionGraph(group, p, want)
	}
	captured := write(0, capturedID, "captured-source", reasonNone)
	// The same raw IDs are actual independent successful originals at target.
	for _, id := range []requestID{capturedID, lateID, absentID} {
		write(1, id, "independent-target", reasonNone)
	}
	capturedKey := declaredRequestKey(origin, graphOperations, [16]byte(capturedID))
	capturedBytes, found := rangeMoveKeeperStored(t, source, capturedKey)
	if !found {
		t.Fatal("captured source original missing")
	}
	var targetBytes [3][]byte
	for i, id := range []requestID{capturedID, lateID, absentID} {
		targetBytes[i], found = rangeMoveKeeperStored(t, target, declaredRequestKey(target.machine.ns, graphOperations, [16]byte(id)))
		if !found {
			t.Fatal("independent target original missing", id)
		}
	}
	if bytes.Equal(capturedBytes, targetBytes[0]) {
		t.Fatal("colliding namespaces did not diverge")
	}
	sourceWire := &rangeMoveKeeperWire{graphPeerWireTransport: transports[3], pauseDescribe: true}
	targetWire := &rangeMoveKeeperWire{graphPeerWireTransport: transports[8]}
	transports[3], transports[8] = sourceWire, targetWire
	c, err := newRangeMoveCoordinator(source, transports)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := runRangeMoveToCompletion(t, c, intent)
	if !sourceWire.stopped || !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || pending.committed {
		t.Fatal("native handoff did not pause after actual captures", pending, err)
	}
	observed := rangeMoveAdmissionRead(t, n, 0, pending.resume.move)
	if observed.observation.status.phase != rangeMoveCapture {
		t.Fatal("pause preceded durable source capture")
	}
	view, err := source.machine.store.ApplicationView(source.driver.Applied())
	if err != nil {
		t.Fatal(err)
	}
	capture, present, _, err := view.GetControl(t.Context(), rangeMoveControlKey(origin, pending.resume.move, 'C'), raftlog.ReadBudget{Rows: 2, Bytes: 16 << 10})
	if closeErr := view.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || !present {
		t.Fatal("native captured coordinate missing", present, err)
	}
	capturedRoot, _, err := moveCaptureRoot(capture.Value, intent.source)
	if err != nil {
		t.Fatal(err)
	}
	late := write(0, lateID, "must-stay-refused", reasonPreparedConflict)
	lateKey := declaredRequestKey(origin, graphOperations, [16]byte(lateID))
	lateBytes, found := rangeMoveKeeperStored(t, source, lateKey)
	if !found || late.index <= capturedRoot.Index {
		t.Fatal("refusal is not after actual frozen capture", late.index, capturedRoot.Index)
	}
	decoded, err := decodePartitionMappedOutcome(lateBytes, origin)
	if err != nil || decoded != late || decoded.reason != reasonPreparedConflict {
		t.Fatal("keeper lacks exact committed refusal", decoded, err)
	}
	frozen, err := source.machine.store.ApplicationView(capturedRoot.Index)
	if err != nil {
		t.Fatal(err)
	}
	_, present, err = frozen.Get(t.Context(), lateKey, 4096)
	if closeErr := frozen.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if err != nil || present {
		t.Fatal("late original leaked into frozen captured source", present, err)
	}
	sourceWire.pauseDescribe = false
	done := rangeMoveKeeperResume(t, c, pending.resume)
	for _, voter := range n.hosts[1] {
		_, found := rangeMoveKeeperStored(t, voter, rangeMoveReplayKey(voter.machine.ns, origin.partition, [16]byte(lateID), 4))
		if found {
			t.Fatal("late refusal falsely copied from frozen source")
		}
		value, found := rangeMoveKeeperStored(t, voter, rangeMoveReplayKey(voter.machine.ns, origin.partition, [16]byte(capturedID), 4))
		if !found || !bytes.Equal(value, capturedBytes) {
			t.Fatal("captured original absent from genuine completed transfer")
		}
		for i, id := range []requestID{capturedID, lateID, absentID} {
			value, found := rangeMoveKeeperStored(t, voter, declaredRequestKey(voter.machine.ns, graphOperations, [16]byte(id)))
			if !found || !bytes.Equal(value, targetBytes[i]) {
				t.Fatal("transfer overwrote target-qualified original", id)
			}
		}
	}
	keeperQueries := sourceWire.originalQueries
	got, err := c.LookupOriginal(t.Context(), done.resume, origin, false, lateKey)
	if err != nil || !got.found || got.origin != origin || !bytes.Equal(got.key, lateKey) || !bytes.Equal(got.value, lateBytes) || got.originalIndex != late.index || got.evaluationIndex != source.driver.Applied() || got.evaluationIndex == got.originalIndex || sourceWire.originalQueries != keeperQueries+1 {
		t.Fatal("late original did not use exact real keeper bytes/coordinates", got, err)
	}
	if bytes.Equal(got.value, targetBytes[1]) {
		t.Fatal("same target request ID substituted for source refusal")
	}
	// Refuse only the genuine source original-keeper query. Source handoff
	// observations remain available; a captured shadow needs no keeper query.
	sourceWire.keeperUnavailable = true
	keeperQueries = sourceWire.originalQueries
	got, err = c.LookupOriginal(t.Context(), done.resume, origin, false, capturedKey)
	if err != nil || !got.found || !bytes.Equal(got.value, capturedBytes) || !bytes.Equal(got.key, capturedKey) || got.origin != origin || got.originalIndex != captured.index || got.evaluationIndex != target.driver.Applied() || got.evaluationIndex == got.originalIndex || sourceWire.originalQueries != keeperQueries {
		t.Fatal("captured original queried unavailable original keeper or lost coordinate", got, err)
	}
	got, err = c.LookupOriginal(t.Context(), done.resume, origin, false, lateKey)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || !reflect.DeepEqual(got, rangeMoveOriginal{}) || sourceWire.originalQueries != keeperQueries+1 {
		t.Fatal("unavailable original-keeper query became completed absence/substituted target outcome", got, err)
	}
	// Restore that exact native route and require a completed keeper absence.
	// Target has the same raw ID; its independently stored outcome is not ours.
	sourceWire.keeperUnavailable = false
	keeperQueries = sourceWire.originalQueries
	absentKey := declaredRequestKey(origin, graphOperations, [16]byte(absentID))
	got, err = c.LookupOriginal(t.Context(), done.resume, origin, false, absentKey)
	if err != nil || got.found || got.value != nil || got.originalIndex != 0 || got.origin != origin || !bytes.Equal(got.key, absentKey) || got.evaluationIndex != source.driver.Applied() || sourceWire.originalQueries != keeperQueries+1 {
		t.Fatal("restored native keeper absence confused target collision", got, err)
	}
	// Destination COMMIT discovery is separately supported with the full source
	// endpoint offline. This check does not replace the keeper-only fault above.
	sourceWire.allUnavailable = true
	sourceCalls := sourceWire.calls
	got, err = c.LookupOriginal(t.Context(), done.resume, origin, false, capturedKey)
	if err != nil || !got.found || got.origin != origin || !bytes.Equal(got.key, capturedKey) || !bytes.Equal(got.value, capturedBytes) || got.originalIndex != captured.index || got.evaluationIndex != target.driver.Applied() || got.evaluationIndex == got.originalIndex || sourceWire.calls != sourceCalls {
		t.Fatal("captured shadow needed offline source endpoint or lost coordinates", got, err)
	}
	got, err = c.LookupOriginal(t.Context(), done.resume, origin, false, lateKey)
	if !errors.Is(err, errRangeMovePending) || !errors.Is(err, io.EOF) || !reflect.DeepEqual(got, rangeMoveOriginal{}) || sourceWire.calls != sourceCalls+1 {
		t.Fatal("late miss did not attempt offline source or became completed absence", got, err)
	}
}

func TestRangeMoveLookupOriginalCompletedKeeperAbsenceIgnoresTargetCollision(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, c, intent, _, _ := rangeMoveCoordinatorFixture(t, 64)
	source, target := n.hosts[0][0], n.hosts[1][0]
	id := requestID{114}
	window, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		t.Fatal(err)
	}
	p, err := target.PreparePartitionGraphOperations(id, []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 17, Life: 18, Scope: window, Name: "only-target-original"}}, 114, nil)
	if err != nil {
		t.Fatal(err)
	}
	n.submitPartitionGraph(1, p, reasonNone)
	key := declaredRequestKey(source.machine.ns, graphOperations, [16]byte(id))
	if _, found := rangeMoveKeeperStored(t, source, key); found {
		t.Fatal("absence fixture created source original")
	}
	done, err := runRangeMoveToCompletion(t, c, intent)
	if err != nil || !done.committed {
		t.Fatal("absence guard requires real completed handoff", done, err)
	}
	got, err := c.LookupOriginal(t.Context(), done.resume, source.machine.ns, false, key)
	if err != nil || got.found || got.value != nil || got.originalIndex != 0 || got.origin != source.machine.ns || !bytes.Equal(got.key, key) || got.evaluationIndex != source.driver.Applied() {
		t.Fatal("genuine completed keeper absence confused target sameID", got, err)
	}
	if value, found := rangeMoveKeeperStored(t, target, declaredRequestKey(target.machine.ns, graphOperations, [16]byte(id))); !found || len(value) == 0 {
		t.Fatal("target collision disappeared from absence fixture")
	}
}
