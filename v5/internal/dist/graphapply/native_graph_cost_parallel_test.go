package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"slices"
	"strconv"
	"testing"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

func TestNativeGraphCostIndependentClientsSmallActualProcesses(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	for _, writers := range []int{8, 32} {
		for _, cell := range []struct {
			name  string
			cross uint64
		}{{"local", 0}, {"cross", 100}} {
			t.Run(cell.name+"/"+strconv.Itoa(writers), func(t *testing.T) {
				// Seed plus a complete 32-client window, genuine old/new cuts, exact CDC,
				// physical six-directory reopen and bounded original grant replay.
				// Block64 yields two home grants + one remote grant (six originals).
				runNativeGraphCostWithBlock(t, nativeGraphCostSpec{Version: 1, Facts: 33, CrossPercent: cell.cross, Writers: writers}, 64)
			})
		}
	}
}

func TestNativeGraphCostRequestACKFindsOriginalBehindLaterAppliedEntry(t *testing.T) {
	if testing.Short() {
		t.Skip("long tier: release gate")
	}
	n, _, ops, _, _ := coordinatorNativeFixture(t)
	h := n.hosts[0][0]
	makeProposal := func(label string) allocationProposal {
		t.Helper()
		p, err := h.PreparePartitionGraphOperations(n.requestID(), []graphstate.Operation{{Kind: graphstate.AddLabel, Owner: 1, Life: 2, Name: label, Scope: ops[0].Scope}}, 41, nil)
		if err != nil {
			t.Fatal(err)
		}
		return p
	}
	first, later := makeProposal("ack-first"), makeProposal("ack-later")
	request := func(p allocationProposal) processRequest {
		t.Helper()
		wire, err := encodeCertifiedProcessProposal(p)
		if err != nil {
			t.Fatal(err)
		}
		return processRequest{op: processCostRecord, data: append([]byte(nativeGraphCostRequestRecordTag), wire...)}
	}
	read := func(ctx context.Context, r processRequest) nativeGraphCostRequestRecord {
		t.Helper()
		wire, err := nativeGraphCostRequestRecordWire(ctx, h, r)
		if err != nil {
			t.Fatal(err)
		}
		var out nativeGraphCostRequestRecord
		if err := json.Unmarshal(wire, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	missing := read(t.Context(), request(first))
	if missing.Found || missing.Index != 0 || missing.Outcome != nil || missing.CDC != nil {
		t.Fatal("missing request became ACK", missing)
	}
	original := n.submitPartitionGraph(0, first, reasonNone)
	second := n.submitPartitionGraph(0, later, reasonNone)
	if second.index <= original.index {
		t.Fatal("fixture did not interleave applied originals")
	}
	found := read(t.Context(), request(first))
	outcome, err := decodePartitionGraphOutcome(found.Outcome, h.machine.ns)
	actual, actualErr := h.machine.store.ApplicationRecord(t.Context(), original.index, false, 1<<20)
	if err != nil || actualErr != nil || !found.Found || found.Index != original.index || outcome != original || outcome.hash != sha256.Sum256(first.wire) || len(found.CDC) == 0 || !bytes.Equal(found.CDC, actual) {
		t.Fatal("latest progress substituted original", found, err, actualErr)
	}
	for _, mutate := range []func(*processRequest){
		func(r *processRequest) { r.index = 1 }, func(r *processRequest) { r.entity = 1 },
		func(r *processRequest) { r.data = r.data[:len(r.data)-1] }, func(r *processRequest) { r.packet.From = 1 },
	} {
		bad := request(first)
		mutate(&bad)
		if out, err := nativeGraphCostRequestRecordWire(t.Context(), h, bad); err == nil || out != nil {
			t.Fatal("malformed request admitted", out, err)
		}
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if out, err := nativeGraphCostRequestRecordWire(canceled, h, request(first)); !errors.Is(err, context.Canceled) || out != nil {
		t.Fatal("canceled lookup returned ACK", out, err)
	}
	for _, ctx := range []context.Context{nil, t.Context()} {
		if _, err := nativeGraphCostRequestRecordWire(ctx, nil, request(first)); !errors.Is(err, errInvalid) {
			t.Fatal(err)
		}
	}
}

func TestNativeGraphCostAttemptLineageAndWindowAreBoundedAndRequestJoined(t *testing.T) {
	spec := nativeGraphCostSpec{Version: 1, Facts: 33, Writers: 32}
	op := nativeGraphCostOperationID(spec, 4, 1)
	for _, writers := range []int{1, 8, 32} {
		s := spec
		s.Writers = writers
		if s.validate() != nil || nativeGraphCostOperationID(s, 4, 1) != op {
			t.Fatal("writer changed logical operation", s)
		}
	}
	other := spec
	other.Facts++
	if nativeGraphCostOperationID(other, 4, 1) == op || nativeGraphCostOperationID(spec, 5, 1) == op {
		t.Fatal("different dataset/phase aliased")
	}
	if nativeGraphCostRetrySame(errInvalid) || nativeGraphCostRetrySame(errors.Join(errCorrupt, errLimit)) || nativeGraphCostRetryConflict(outcome{disposition: requestReplay, reason: reasonPreparedConflict}) || !nativeGraphCostRetrySame(errors.Join(errLimit, errCrossProcessPending)) {
		t.Fatal("admission was treated as abort")
	}
	hub := &graphProcessHub{costProgress: make(chan struct{}), packets: make(chan graphProcessPacketJob, 1)}
	a, _ := hub.costProgressSnapshot()
	b, _ := hub.costProgressSnapshot()
	hub.mu.Lock()
	hub.costProgressLocked()
	hub.mu.Unlock()
	for _, wait := range []<-chan struct{}{a, b} {
		select {
		case <-wait:
		default:
			t.Fatal("one client consumed another client's wakeup")
		}
	}
	next, _ := hub.costProgressSnapshot()
	select {
	case <-next:
		t.Fatal("new generation already closed")
	default:
	}
	admissions := newNativeGraphCostAdmissions()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if release, err := admissions.acquire(ctx, true); !errors.Is(err, context.Canceled) || release != nil || len(admissions.local) != 0 || len(admissions.cross) != 0 {
		t.Fatal("canceled admission retained owners", err)
	}
	if nativeGraphCostWindowCommands != 32 || nativeGraphCostBatchCommands != 16 || processSourcePending != 16 || processSourceWorkers != 2 {
		t.Fatal("changed wire/host capacity")
	}
	// Codec guards use inert proposals; actual originals are established only in
	// the native fixture above and independent-client process parent.
	command := partitionGraphCommand{configuration: [32]byte{1}, declaration: [32]byte{2}, routing: [32]byte{3}, request: graphCodecRequest(t)}
	command.request.id = requestID{1}
	command.request.revision, _ = state.NewRevision(1, op.Ordinal)
	wire, err := encodePartitionGraphCommand(command, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	attempt := nativeGraphCostAttempt{Operation: op, Engine: "native-raft", ClientID: 7, AttemptOrdinal: 1, Request: command.request.identity(), Proposal: wire, BodySHA256: sha256.Sum256(wire), Classification: "pending-or-unavailable"}
	first, err := nativeGraphCostJoinAttempt(nativeGraphCostAttemptJoin{}, attempt)
	if err != nil {
		t.Fatal(err)
	}
	command.request.id = requestID{2}
	nextWire, err := encodePartitionGraphCommand(command, defaultMaterializerLimits())
	if err != nil {
		t.Fatal(err)
	}
	retry := attempt
	retry.AttemptOrdinal = 2
	retry.PreviousRequest = attempt.Request
	retry.Request = command.request.identity()
	retry.Proposal = nextWire
	retry.BodySHA256 = sha256.Sum256(nextWire)
	if _, err := nativeGraphCostJoinAttempt(first, retry); !errors.Is(err, errCorrupt) {
		t.Fatal("pending allowed remint", err)
	}
	first.classification = "authoritative-conflict"
	if _, err := nativeGraphCostJoinAttempt(first, retry); err != nil {
		t.Fatal("linked authoritative terminal refused", err)
	}
	for _, mutate := range []func(*nativeGraphCostAttempt){
		func(a *nativeGraphCostAttempt) { a.PreviousRequest = [16]byte{9} }, func(a *nativeGraphCostAttempt) { a.ClientID++ },
		func(a *nativeGraphCostAttempt) { a.AttemptOrdinal++ }, func(a *nativeGraphCostAttempt) { a.BodySHA256[0]++ },
	} {
		bad := retry
		bad.Proposal = slices.Clone(retry.Proposal)
		mutate(&bad)
		if _, err := nativeGraphCostJoinAttempt(first, bad); !errors.Is(err, errCorrupt) {
			t.Fatal("unbound attempt joined", err)
		}
	}
}
