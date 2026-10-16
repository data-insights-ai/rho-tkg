package graphapply

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

const graphE17AdmittedMovePages = 128

// These are explicit allowances for this fixed acceptance fixture, not proved
// production worst-case/SLO bounds. All IPC and replacement incarnations count.
func graphE17FixtureDeliveries(pages, publicPhases, voterPhases int) uint64 {
	return uint64(processDeliveries + pages*2*32 + publicPhases*2048 + voterPhases*6*graphE17NativeVoterPages)
}

// These additions are private test IPC. Only child-local native Hosts and the
// authenticated production coordinators create/consume admission authority.
func graphE17GrantInput(sequence, count uint64) []byte {
	return binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, sequence), count)
}

func graphE17GrantParameters(h *allocationHost, wire []byte) (uint64, uint64, error) {
	if len(wire) == 0 {
		return 1, 16, nil // Preserve the original E15 fixture exactly.
	}
	if h == nil || !h.machine.rangeMoves() || len(wire) != 16 {
		return 0, 0, errInvalid
	}
	sequence, count := binary.BigEndian.Uint64(wire[:8]), binary.BigEndian.Uint64(wire[8:])
	if sequence == 0 || count == 0 || count > 16 {
		return 0, 0, errInvalid
	}
	return sequence, count, nil
}

func graphE17PropertyProposal(t *testing.T, o *certifiedProcessOwners, r processRequest, id requestID) (allocationProposal, error) {
	if o == nil || o.recipient == nil || !o.host.machine.rangeMoves() || len(r.data) != 16 || (r.index != 1 && r.index != 17) {
		return allocationProposal{}, errInvalid
	}
	valueID, value := binary.BigEndian.Uint64(r.data[:8]), binary.BigEndian.Uint64(r.data[8:])
	// The fixture deliberately uses only positive small I64 values.
	if value > 100 || valueID == 0 {
		return allocationProposal{}, errInvalid
	}
	scope, err := temporal.All(graphGenesisBinding(t).Axis())
	if err != nil {
		return allocationProposal{}, err
	}
	grant := grantReference{session: o.recipient.session, sequence: 1}
	if o.host.machine.ns.partition == 3 {
		grant.sequence = 2
	}
	op := graphstate.Operation{Kind: graphstate.Set, Owner: graphstate.EntityID(r.index), Life: graphstate.LifeID(r.index + 1), Scope: scope, Name: "p", Value: graphstate.I64(int64(value)), ValueID: graphstate.ValueID(valueID)}
	return o.host.PreparePartitionGraphOperations(id, []graphstate.Operation{op}, 23, []freshBinding{{role: valueBinding, id: valueID, grant: grant}})
}

func graphE17MoveBounds(maxPages, rows, size int) []byte {
	b := binary.BigEndian.AppendUint32(nil, uint32(maxPages))
	b = binary.BigEndian.AppendUint32(b, uint32(rows))
	return binary.BigEndian.AppendUint32(b, uint32(size))
}

func graphE17RunMove(ctx context.Context, h *allocationHost, incarnation [16]byte, coordinator *rangeMoveCoordinator, request processRequest) ([]byte, error) {
	if coordinator == nil {
		return nil, errInvalid
	}
	var result rangeMoveResult
	var err error
	switch request.op {
	case processRangeRun:
		if len(request.data) != 12 {
			return nil, errInvalid
		}
		id := requestID(incarnation)
		binary.BigEndian.PutUint64(id[8:], request.sequence)
		var intent rangeMoveIntent
		intent, err = h.PrepareRangeMove(ctx, id, request.entity, request.index,
			int(binary.BigEndian.Uint32(request.data[:4])), int(binary.BigEndian.Uint32(request.data[4:8])), int(binary.BigEndian.Uint32(request.data[8:])))
		if err == nil {
			result, err = coordinator.Run(ctx, intent)
		}
	case processRangeResume:
		var previous rangeMoveResult
		previous, err = decodeGraphE17MoveResult(processResponse{aux: request.data})
		if err == nil || errors.Is(err, errRangeMovePending) {
			result, err = coordinator.Resume(ctx, previous.resume)
		}
	default:
		return nil, errInvalid
	}
	if result.resume.request == (requestID{}) {
		return nil, err
	}
	wire, encodeErr := encodeGraphE17MoveResult(result, err)
	return wire, errors.Join(err, encodeErr)
}

func encodeGraphE17MoveResult(r rangeMoveResult, runErr error) ([]byte, error) {
	scope, err := encodeAllocationScope(r.resume.scope)
	if err != nil {
		return nil, err
	}
	var statuses [3][]byte
	for i, status := range []rangeMoveStatus{r.source, r.target, r.decision} {
		if status != (rangeMoveStatus{}) {
			statuses[i], err = encodeRangeMoveStatus(status)
			if err != nil {
				return nil, err
			}
		}
	}
	return processEncoding(func(w *boundedWriter) {
		w.add([]byte("GMR2"))
		w.field(scope)
		w.add(r.resume.request[:])
		w.add(r.resume.move[:])
		w.add(r.resume.intent[:])
		if errors.Is(runErr, errRangeMovePending) {
			w.tag(1)
		} else {
			w.tag(0)
		}
		if r.committed {
			w.tag(1)
		} else {
			w.tag(0)
		}
		for _, status := range statuses {
			w.field(status)
		}
		w.u16(graphErrorMask(runErr))
	})
}

func decodeGraphE17MoveResult(response processResponse) (rangeMoveResult, error) {
	c := graphCursor{b: response.aux}
	if !bytes.Equal(c.take(4), []byte("GMR2")) {
		return rangeMoveResult{}, errors.Join(errCorrupt, processResponseError(response))
	}
	scope := c.field(256)
	r := rangeMoveResult{}
	copy(r.resume.request[:], c.take(16))
	r.resume.move = c.array()
	copy(r.resume.intent[:], c.take(32))
	pending, committed := c.tag(), c.tag()
	r.committed = committed == 1
	var err error
	r.resume.scope, err = decodeAllocationScope(scope)
	if err != nil {
		return rangeMoveResult{}, err
	}
	for _, destination := range []*rangeMoveStatus{&r.source, &r.target, &r.decision} {
		wire := c.field(256)
		if len(wire) != 0 {
			*destination, err = decodeRangeMoveStatus(wire)
			if err != nil || destination.move != r.resume.move || destination.intent != r.resume.intent {
				return rangeMoveResult{}, errors.Join(errCorrupt, err)
			}
		}
	}
	mask := c.u16()
	if c.err != nil || len(c.b) != 0 || pending > 1 || committed > 1 || r.resume.request == (requestID{}) || r.resume.move != rangeMoveID(namespace{r.resume.scope.graph, r.resume.scope.partition}, r.resume.request) || r.resume.intent == ([32]byte{}) {
		return rangeMoveResult{}, errors.Join(errCorrupt, c.err)
	}
	err = processResponseError(response)
	if mask != 0 {
		err = errors.Join(err, graphWireError{mask})
	}
	if pending == 1 {
		err = errors.Join(err, errRangeMovePending)
	}
	return r, err
}

// Detail bytes are inert diagnostic native reads. Canonical component bytes
// preserve every cell/revision; they cannot be submitted as graph deltas.
type graphE17Fact struct {
	id                            uint64
	fact                          certifiedProcessFact
	identity, presence, property  []byte
	valueID, revision, provenance uint64
	value                         int64
}

func graphE17NativeFact(ctx context.Context, h *allocationHost, v graphstate.ReadView, id uint64) (graphE17Fact, error) {
	f := graphE17Fact{id: id}
	var err error
	f.fact, err = graphProcessSourceFact(ctx, h, v, id)
	if err != nil || !f.fact.Exists {
		return f, err
	}
	entity, err := v.Entity(ctx, graphstate.EntityID(id))
	if err != nil {
		return f, err
	}
	life, err := v.Life(ctx, graphstate.EntityID(id), graphstate.LifeID(f.fact.Life))
	if err != nil || !entity.Found || !life.Found {
		return f, errors.Join(errCorrupt, err)
	}
	r := entity.Record
	f.identity, err = processEncoding(func(w *boundedWriter) {
		w.u64(uint64(r.ID))
		w.tag(byte(r.Kind))
		w.tag(byte(r.Mode))
		w.tag(byte(r.Interpretation))
		w.tag(byte(r.TemporalRole))
		w.text(r.Type)
		w.u64(uint64(r.Source))
		w.u64(uint64(r.Target))
		axis := r.Axis.DefinitionHash()
		w.add(axis[:])
		for _, n := range []uint64{uint64(life.Record.Owner), uint64(life.Record.Life), uint64(life.Record.SourceLife), uint64(life.Record.TargetLife)} {
			w.u64(n)
		}
	})
	if err != nil {
		return f, err
	}
	window, err := temporal.All(r.Axis)
	if err != nil {
		return f, err
	}
	point, err := temporal.RationalPosition(r.Axis, temporal.RationalInt64(0))
	if err != nil {
		return f, err
	}
	keys := []graphstate.ComponentKey{{Owner: graphstate.EntityID(id), Kind: graphstate.Presence}}
	if r.Kind == graphstate.Node {
		keys = append(keys, graphstate.ComponentKey{Owner: graphstate.EntityID(id), Life: life.Record.Life, Kind: graphstate.ScalarProperty, Name: "p"})
	}
	for i, key := range keys {
		page, e := v.ComponentPage(ctx, graphstate.ComponentQuery{Key: key, Window: window}, 0, graphstate.ReadBudget{Rows: 64, Bytes: 64 << 10})
		if e != nil || !page.Complete || page.Next != 0 {
			return f, errors.Join(errLimit, e)
		}
		wire, e := state.AppendState(nil, page.Data, state.CodecLimits{State: h.machine.limits.graph.Planner.Component, MaxEncodedBytes: 64 << 10})
		if e != nil {
			return f, e
		}
		if i == 0 {
			f.presence = wire
			continue
		}
		f.property = wire
		cell, e := page.Data.At(point, h.machine.limits.graph.Planner.Component)
		if e != nil || !cell.Present() {
			return f, errors.Join(errCorrupt, e)
		}
		f.valueID, f.revision, f.provenance = cell.Value().ID(), cell.Revision().ID(), cell.Revision().Provenance()
		value, e := v.Value(ctx, graphstate.ValueID(f.valueID))
		if e != nil || !value.Found || uint64(value.ID) != f.valueID {
			return f, errors.Join(errCorrupt, e)
		}
		var ok bool
		f.value, ok = value.Value.Int64Value()
		if !ok {
			return f, errCorrupt
		}
	}
	return f, nil
}

func graphE17SessionObserve(ctx context.Context, sessions *graphProcessCertifiedSessions, request processRequest) ([]byte, error) {
	sessions.mu.Lock()
	owner, found := sessions.owners[request.index]
	sessions.mu.Unlock()
	if !found {
		return nil, errInvalid
	}
	ids, err := decodeGraphProcessObserveIDs(request.data)
	if err != nil {
		return nil, err
	}
	view, err := owner.read.ReadView()
	if err != nil {
		return nil, err
	}
	facts := make([]graphE17Fact, len(ids))
	for i, id := range ids {
		facts[i], err = graphE17NativeFact(ctx, sessions.coordinator.host, view, id)
		if err != nil {
			return nil, err
		}
	}
	return processEncoding(func(w *boundedWriter) {
		w.u32(len(facts))
		for _, f := range facts {
			fact, e := encodeCertifiedProcessFact(f.fact)
			if e != nil {
				w.err = e
				return
			}
			w.u64(f.id)
			w.field(fact)
			w.field(f.identity)
			w.field(f.presence)
			w.field(f.property)
			w.u64(f.valueID)
			w.u64(f.revision)
			w.u64(f.provenance)
			w.u64(uint64(f.value))
		}
	})
}

func decodeGraphE17Facts(wire []byte) ([]graphE17Fact, error) {
	c := graphCursor{b: wire}
	n := c.count(graphProcessObserveMaxIDs, 8+5*4+4*8)
	if c.err != nil {
		return nil, c.err
	}
	facts := make([]graphE17Fact, n)
	for i := range facts {
		f := &facts[i]
		f.id = c.u64()
		var err error
		f.fact, err = decodeCertifiedProcessFact(c.field(64 << 10))
		if err != nil {
			return nil, err
		}
		f.identity, f.presence, f.property = bytes.Clone(c.field(1024)), bytes.Clone(c.field(64<<10)), bytes.Clone(c.field(64<<10))
		f.valueID, f.revision, f.provenance, f.value = c.u64(), c.u64(), c.u64(), int64(c.u64())
	}
	if c.err != nil || len(c.b) != 0 {
		return nil, errors.Join(errCorrupt, c.err)
	}
	return facts, nil
}

// This wrapper carries only a parent-owned invocation interruption after the
// correlated terminal worker result. It grants no durable move authority.
type graphE17MoveAttempt struct {
	response     processResponse
	interruption error
}

func graphE17CompleteMoveResponse(t *testing.T, n *certifiedProcessNetwork, attempt graphE17MoveAttempt) (graphE17MoveAttempt, rangeMoveResult, error) {
	t.Helper()
	result, err := decodeGraphE17MoveResult(attempt.response)
	original := result.resume
	for range 128 {
		if !errors.Is(err, errRangeMovePending) {
			return attempt, result, err
		}
		t.Logf("explicit process Resume source=%d/%d target=%d/%d", result.source.phase, result.source.sequence, result.target.phase, result.target.sequence)
		if !graphE17PermittedResume(err, attempt.interruption) {
			return attempt, result, err
		}
		attempt = graphE17MoveExchange(t, n.all[1], processRequest{op: processRangeResume, data: attempt.response.aux})
		result, err = decodeGraphE17MoveResult(attempt.response)
		if graphE17OriginalResume(result.resume, original) != nil {
			return attempt, result, errors.Join(errInvalid, err)
		}
	}
	return attempt, result, err
}
func graphE17MoveExchange(t *testing.T, child *graphProcess, request processRequest) graphE17MoveAttempt {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	return graphE17MoveExchangeContext(t, ctx, child, request)
}
func graphE17MoveExchangeContext(t *testing.T, ctx context.Context, child *graphProcess, request processRequest) graphE17MoveAttempt {
	t.Helper()
	response, exchangeErr := child.exchange(ctx, request)
	attempt := graphE17MoveAttempt{response: response}
	if exchangeErr != nil {
		if ctx.Err() != nil && errors.Is(exchangeErr, ctx.Err()) {
			attempt.interruption = ctx.Err()
		}
		_, decodeErr := decodeGraphE17MoveResult(response)
		if !graphE17PermittedResume(decodeErr, attempt.interruption) {
			t.Fatal("actual move exchange", exchangeErr, decodeErr)
		}
	}
	if request.op == processRangeResume {
		prior, e := decodeGraphE17MoveResult(processResponse{aux: request.data})
		current, actualErr := decodeGraphE17MoveResult(response)
		if e != nil && !errors.Is(e, errRangeMovePending) || graphE17OriginalResume(current.resume, prior.resume) != nil {
			t.Fatal("foreign/malformed original Resume reply", e, actualErr)
		}
	}
	return attempt
}

func graphE17PermittedResume(err, own error) bool {
	if !errors.Is(err, errRangeMovePending) {
		return false
	}
	// Only the original invocation's own canceled context admits transport
	// unavailability joined with a classified cancellation. No arbitrary remote
	// cancellation, unavailable replica, corruption or unknown error is retried.
	allowed := uint16(1<<1 | 1<<6 | 1<<14 | 1<<15)
	if (errors.Is(own, context.Canceled) || errors.Is(own, context.DeadlineExceeded)) && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		allowed |= 1<<10 | 1<<11 | 1<<12
	}
	mask := graphErrorMask(err)
	return mask != 0 && mask & ^allowed == 0
}

func graphE17OriginalResume(actual, original rangeMoveResume) error {
	if actual.request == (requestID{}) || actual != original {
		return errInvalid
	}
	return nil
}
