package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

const nativeGraphCostLocalAdmissions = 8
const nativeGraphCostMaxAttempts = 4
const nativeGraphCostRequestRecordTag = "GACK1"

// Operation identity is substrate/writer independent. Ordinal is the existing
// actual provenance schedule, never an assumption about installation order.
type nativeGraphCostOperation struct {
	Dataset       [32]byte
	Phase         string
	Fact, Ordinal uint64
}
type nativeGraphCostAttempt struct {
	Operation                                  nativeGraphCostOperation
	Engine                                     string
	ClientID, AttemptOrdinal                   int
	PreviousRequest, Request, Transaction      [16]byte
	BodySHA256, BusinessSHA256, DecisionDigest [32]byte
	OriginalIndex, Round                       uint64
	Classification, Error                      string
	SubmitInvocations                          int
	Proposal, Manifest, Outcome, CDC           []byte
}
type nativeGraphCostClientEvent struct {
	attempt *nativeGraphCostAttempt
	sample  nativeGraphCostSample
	err     error
	done    bool
}

func nativeGraphCostOperationID(s nativeGraphCostSpec, phase, fact uint64) nativeGraphCostOperation {
	if s.Version == 5 || s.Version == 6 || s.Version == 7 || s.Version == 9 || s.Version == 10 || s.Version == 11 {
		s.Version = 1
	} // Fresh feed shares FDB's logical dataset, never its native profile/authority.
	// Fixed fields are encoded explicitly: Writers and engine do not participate.
	var wire [25]byte
	wire[0] = byte(s.Version)
	binary.BigEndian.PutUint64(wire[1:9], s.Facts)
	binary.BigEndian.PutUint64(wire[9:17], s.CrossPercent)
	copy(wire[17:], "graph-v2")
	out := nativeGraphCostOperation{Dataset: sha256.Sum256(wire[:]), Phase: "setup", Fact: fact, Ordinal: phase}
	if phase == 4 {
		out.Phase, out.Ordinal = "create", 4+fact
	}
	if phase == 5 {
		out.Phase, out.Ordinal = "update", 4+s.Facts+fact
	}
	return out
}

// A notification only wakes all current clients. A new channel replaces the
// closed generation under mu; no per-packet history or commit authority exists.
func (h *graphProcessHub) costProgressLocked() {
	if h.costProgress != nil {
		close(h.costProgress)
		h.costProgress = make(chan struct{})
	}
}
func (h *graphProcessHub) costProgressSnapshot() (<-chan struct{}, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.costProgress, h.inFlight == 0 && h.packetBytes == 0 && len(h.packets) == 0
}

// Driver.Applied selects only an available real snapshot. Its retained request
// map identifies the original; the actual ApplicationRecord bytes prove ACK.
type nativeGraphCostRequestRecord struct {
	Found        bool
	Index        uint64
	Outcome, CDC []byte
}

func nativeGraphCostRequestRecordWire(ctx context.Context, h *allocationHost, r processRequest) (wire []byte, err error) {
	if ctx == nil || h == nil || r.op != processCostRecord || r.index != 0 || r.entity != 0 || r.packet.From != 0 || r.packet.To != 0 || r.packet.Snapshot || len(r.packet.Payload) != 0 || !bytes.HasPrefix(r.data, []byte(nativeGraphCostRequestRecordTag)) {
		return nil, errInvalid
	}
	proposal, err := decodeCertifiedProcessProposal(r.data[len(nativeGraphCostRequestRecordTag):])
	if err != nil {
		return nil, err
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.machine == nil || h.driver == nil || proposal.target != h.machine.scope() {
		return nil, errInvalid
	}
	command, err := decodePartitionGraphCommand(proposal.wire, h.machine.limits)
	if err != nil || command.request.ns != h.machine.ns {
		return nil, errors.Join(errInvalid, err)
	}
	view, err := h.machine.store.ApplicationView(h.driver.Applied())
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			wire = nil
		}
	}()
	root, err := view.RootBounded(ctx, 172)
	if err != nil {
		return nil, err
	}
	l := h.machine.limits
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return nil, err
	}
	limits := l.allocation
	limits.readRows, limits.readBytes = min(limits.readRows, l.sourceRows), min(limits.readBytes, l.sourceBytes)
	q := reader{ctx: ctx, view: view, ns: h.machine.ns, base: root, limits: limits, arena: arena, bytes: cap(root.Image)}
	mapped, found, err := q.getBounded(outcomeKey(request{ns: command.request.ns, kind: command.request.kind, id: command.request.id}), graphOutcomeBytes)
	if err != nil {
		return nil, err
	}
	out := nativeGraphCostRequestRecord{Found: found}
	if found {
		original, err := decodePartitionGraphOutcome(mapped, h.machine.ns)
		if err != nil || original.identity != command.request.identity() || original.hash != sha256.Sum256(proposal.wire) || original.kind != command.request.kind || original.index == 0 || original.index > root.Index || original.disposition != applied {
			return nil, errors.Join(errCorrupt, err)
		}
		out.Index = original.index
		out.Outcome, err = view.ApplicationRecord(ctx, original.index, true, 64<<10)
		if err != nil {
			return nil, err
		}
		out.CDC, err = view.ApplicationRecord(ctx, original.index, false, 1<<20)
		if err != nil {
			return nil, err
		}
		if out.Outcome == nil || out.CDC == nil || !bytes.Equal(mapped, out.Outcome) {
			return nil, errCorrupt
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	wire, err = json.Marshal(out)
	if err == nil && len(wire) > nativeGraphCostPageBytes {
		return nil, errLimit
	}
	return wire, err
}

// This cost-only reply owns actual retained decision frames alongside the
// manifest, without changing ordinary coordinator or diagnostic responses.
type nativeGraphCostDecisionProof struct{ Manifest, Outcome, CDC []byte }

func nativeGraphCostCoordinatorProofWire(ctx context.Context, h *allocationHost, result crossProcessResult) ([]byte, error) {
	manifest, err := nativeGraphCostCoordinatorManifest(ctx, h, result)
	if err != nil {
		return nil, err
	}
	proof := nativeGraphCostDecisionProof{Manifest: manifest}
	proof.Outcome, err = h.machine.store.ApplicationRecord(ctx, result.decision.index, true, 64<<10)
	if err != nil {
		return nil, err
	}
	proof.CDC, err = h.machine.store.ApplicationRecord(ctx, result.decision.index, false, 1<<20)
	if err != nil {
		return nil, err
	}
	original, err := decodeCrossStatus(proof.Outcome)
	if err != nil || original != result.decision || proof.CDC == nil || len(proof.CDC) != 0 {
		return nil, errors.Join(errCorrupt, err)
	}
	wire, err := json.Marshal(proof)
	if err == nil && len(wire) > nativeGraphCostPageBytes {
		return nil, errLimit
	}
	return wire, err
}

// Eight shared tokens bound caller RPCs below pending16. A cross client takes
// every token under its own one-client gate, leaving the other source worker
// available for real sourceServe. Local clients remain independent concurrent
// callers; this is an explicit embedding admission, not a host capacity change.
type nativeGraphCostAdmissions struct {
	local chan struct{}
	cross chan struct{}
	// Optional immutable fresh-family policy, installed before any workers.
	originals, same int
	extra           func() error
	beginOriginal   func() error // optional finite fresh whole-cell factory grant, spent before capture
}

func newNativeGraphCostAdmissions() *nativeGraphCostAdmissions {
	return &nativeGraphCostAdmissions{local: make(chan struct{}, nativeGraphCostLocalAdmissions), cross: make(chan struct{}, 1)}
}
func (a *nativeGraphCostAdmissions) acquire(ctx context.Context, cross bool) (func(), error) {
	if a == nil {
		return func() {}, ctx.Err()
	}
	if cross {
		select {
		case a.cross <- struct{}{}:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	count := 1
	if cross {
		count = nativeGraphCostLocalAdmissions
	}
	held := 0
	release := func() {
		for range held {
			<-a.local
		}
		if cross {
			<-a.cross
		}
	}
	for range count {
		select {
		case a.local <- struct{}{}:
			held++
		case <-ctx.Done():
			release()
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}
func nativeGraphCostExchange(ctx context.Context, p *graphProcess, r processRequest) (processResponse, error) {
	response, err := p.exchange(ctx, r)
	return response, errors.Join(err, processResponseError(response), ctx.Err())
}
func nativeGraphCostEmit(ctx context.Context, events chan<- nativeGraphCostClientEvent, attempt nativeGraphCostAttempt) error {
	if events == nil {
		return nil
	}
	select {
	case events <- nativeGraphCostClientEvent{attempt: &attempt}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Only an original retained conflict can authorize a new factory request. A
// pre-admission limit, pending result or unknown reply cannot reach this branch.
func nativeGraphCostRetryConflict(original outcome) bool {
	return original.disposition == applied && (original.reason == reasonPreparedConflict || original.reason == reasonReadConflict)
}
func nativeGraphCostRetrySame(err error) bool {
	return !errors.Is(err, errInvalid) && !errors.Is(err, errCorrupt) && (errors.Is(err, errLimit) || errors.Is(err, errCrossProcessPending))
}

func nativeGraphCostClientSubmit(parent context.Context, n *certifiedProcessNetwork, s nativeGraphCostSpec, phase, fact uint64, cross bool, client int, admissions *nativeGraphCostAdmissions, events chan<- nativeGraphCostClientEvent) (sample nativeGraphCostSample, err error) {
	ctx, cancel := context.WithTimeout(parent, 10*time.Second)
	if admissions != nil && admissions.extra != nil {
		cancel()
		if _, bounded := parent.Deadline(); !bounded {
			return sample, errInvalid
		}
		ctx, cancel = context.WithCancel(parent) // SAME immutable admitted whole-cell deadline; no fresh timeout on retry.
	}
	defer cancel()
	started := time.Now()
	operation := nativeGraphCostOperationID(s, phase, fact)
	sample = nativeGraphCostSample{Phase: operation.Phase, Fact: fact, Cross: cross, phase: phase, Operation: operation, ClientID: client}
	release, err := admissions.acquire(ctx, cross)
	sample.AdmissionNanos = time.Since(started).Nanoseconds()
	if err != nil {
		return sample, err
	}
	defer release()
	actor := uint64(1)
	if phase == 3 {
		actor = 4
	}
	var data []byte
	if phase >= 4 {
		data = binary.BigEndian.AppendUint64(nil, fact)
	}
	var previous [16]byte
	var active *nativeGraphCostAttempt
	published := false
	defer func() {
		if active != nil && !published {
			if active.Classification == "" {
				active.Classification = "pending-or-unavailable"
			}
			if err != nil {
				active.Error = err.Error()
			}
			err = errors.Join(err, nativeGraphCostEmit(context.WithoutCancel(parent), events, *active))
		}
	}()
	maximumOriginals, maximumCalls := nativeGraphCostMaxAttempts, 128
	if admissions != nil && admissions.extra != nil {
		if admissions.originals < 1 || admissions.originals > 64 || admissions.same < 1 || admissions.same > 8 {
			return sample, errInvalid
		}
		maximumOriginals, maximumCalls = admissions.originals, admissions.same
	}
	for ordinal := 1; ordinal <= maximumOriginals; ordinal++ {
		if admissions != nil && admissions.beginOriginal != nil {
			if err := admissions.beginOriginal(); err != nil {
				return sample, err
			}
		}
		proposalStarted := time.Now()
		response, err := nativeGraphCostExchange(ctx, n.all[actor], processRequest{op: processCertifiedAction, entity: certifiedGraphCost, index: phase, data: data})
		if err != nil {
			return sample, err
		} // no frozen attempt existed before factory success
		proposal, err := decodeCertifiedProcessProposal(response.aux)
		if err != nil {
			return sample, err
		}
		command, err := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
		if err != nil {
			return sample, err
		}
		sample.ProposalNanos += time.Since(proposalStarted).Nanoseconds()
		sample.identity, sample.partition, sample.hash = command.request.identity(), proposal.target.partition, sha256.Sum256(proposal.wire)
		attempt := nativeGraphCostAttempt{Operation: operation, Engine: "native-raft", ClientID: client, AttemptOrdinal: ordinal, PreviousRequest: previous, Request: sample.identity, BodySHA256: sample.hash, Proposal: bytes.Clone(proposal.wire)}
		active, published = &attempt, false
		sample.ClientAttempts = ordinal
		ackStarted := time.Now()
		authoritativeAbort := false
		if cross {
			command.version = 0
			business, err := encodePartitionGraphCommand(command, defaultMaterializerLimits())
			if err != nil {
				return sample, err
			}
			attempt.BusinessSHA256 = sha256.Sum256(business)
			// The unchanged request and body are submitted again on pending/busy. Run
			// obtains original registration/decision through the actual source authority.
			for calls := 0; calls < maximumCalls; calls++ {
				if calls > 0 && admissions != nil && admissions.extra != nil {
					if err := admissions.extra(); err != nil {
						return sample, errors.Join(errCrossProcessPending, err)
					}
				}
				attempt.SubmitInvocations++
				response, callErr := nativeGraphCostExchange(ctx, n.all[actor], processRequest{op: processSourceCoordinate, data: proposal.wire})
				var result crossProcessResult
				var decodeErr error
				if len(response.aux) != 0 {
					result, decodeErr = decodeGraphProcessCoordinatorResult(response)
				}
				if result.resume.request != (requestID{}) && (result.resume.business != attempt.BusinessSHA256 || result.resume.scope != proposal.target || result.resume.request != command.request.id || result.resume.tx != crossTransactionID(command.request.ns, command.request.id)) {
					return sample, errCorrupt
				}
				if result.resume.request != (requestID{}) {
					attempt.Transaction = result.resume.tx
				}
				err = errors.Join(callErr, decodeErr)
				if err != nil {
					if nativeGraphCostRetrySame(err) && ctx.Err() == nil {
						continue
					}
					attempt.Classification = "pending-or-unavailable"
					return sample, err
				}
				if result.decision.phase != crossDecide || result.decision.tx != result.resume.tx || result.decision.commit && result.decision.round == 0 {
					return sample, errCorrupt
				}
				l := defaultMaterializerLimits()
				arena, err := graphstate.NewOutputBudget(l.outputBytes)
				if err != nil {
					return sample, err
				}
				var proof nativeGraphCostDecisionProof
				if err := json.Unmarshal(response.changes, &proof); err != nil {
					return sample, err
				}
				manifest, err := decodeCrossManifest(proof.Manifest, l, arena)
				if err != nil || manifest.digest != result.decision.digest || manifest.tx != result.decision.tx || manifest.request.ns != command.request.ns || manifest.request.id != command.request.id || len(manifest.parts) == 0 || len(manifest.parts) > len(sample.participants) {
					return sample, errors.Join(errCorrupt, err)
				}
				// Native decision bytes came from the actual control reply; its full
				// immutable manifest is checked in the child against retained R and D.
				attempt.Transaction, attempt.DecisionDigest, attempt.Round, attempt.OriginalIndex = result.decision.tx, result.decision.digest, result.decision.round, result.decision.index
				attempt.Manifest = bytes.Clone(proof.Manifest)
				original, err := decodeCrossStatus(proof.Outcome)
				if err != nil || original != result.decision || proof.CDC == nil || len(proof.CDC) != 0 {
					return sample, errors.Join(errCorrupt, err)
				}
				attempt.Outcome, attempt.CDC = bytes.Clone(proof.Outcome), bytes.Clone(proof.CDC)
				if !result.decision.commit {
					attempt.Classification = "authoritative-abort"
					authoritativeAbort = true
				} else {
					attempt.Classification = "commit"
					sample.tx, sample.digest, sample.round = result.decision.tx, result.decision.digest, result.decision.round
					for i, part := range manifest.parts {
						sample.participants[i] = part.binding.Partition
					}
				}
				break
			}
			if attempt.Classification == "" {
				return sample, errCrossProcessPending
			}
		} else {
			wire, err := encodeCertifiedProcessProposal(proposal)
			if err != nil {
				return sample, err
			}
			requestRecord := append([]byte(nativeGraphCostRequestRecordTag), wire...)
			proposed := false
			complete := false
			for range 128 {
				if !proposed {
					if attempt.SubmitInvocations >= maximumCalls {
						return sample, errCrossProcessPending
					}
					if attempt.SubmitInvocations > 0 && admissions != nil && admissions.extra != nil {
						if err := admissions.extra(); err != nil {
							return sample, errors.Join(errCrossProcessPending, err)
						}
					}
					callStarted := time.Now()
					response, err = nativeGraphCostExchange(ctx, n.all[actor], processRequest{op: processPropose, data: wire})
					sample.LocalAckTiming.ProposeCount++
					sample.LocalAckTiming.ProposeNanos += time.Since(callStarted).Nanoseconds()
					attempt.SubmitInvocations++
					if err != nil {
						if nativeGraphCostRetrySame(err) && ctx.Err() == nil {
							continue
						}
						return sample, err
					}
					if err = n.all[actor].mux.hub.enqueue(actor, response.packets); err != nil {
						return sample, err
					}
					proposed = true
				}
				hub := n.all[actor].mux.hub
				changed, _ := hub.costProgressSnapshot() // captured before observing application
				callStarted := time.Now()
				_, err = nativeGraphCostExchange(ctx, n.all[actor], processRequest{op: processStateOp, entity: nativeGraphCostAckPollEntity})
				sample.LocalAckTiming.StateCount++
				sample.LocalAckTiming.StateNanos += time.Since(callStarted).Nanoseconds()
				if err != nil {
					return sample, err
				}
				callStarted = time.Now()
				response, err = nativeGraphCostExchange(ctx, n.all[actor], processRequest{op: processCostRecord, data: requestRecord})
				sample.LocalAckTiming.EntryRecordCount++
				sample.LocalAckTiming.EntryRecordNanos += time.Since(callStarted).Nanoseconds()
				if err != nil {
					return sample, err
				}
				var record nativeGraphCostRequestRecord
				if err = json.Unmarshal(response.aux, &record); err != nil {
					return sample, err
				}
				if record.Found {
					original, err := decodePartitionGraphOutcome(record.Outcome, command.request.ns)
					if err != nil || original.identity != sample.identity || original.hash != sample.hash || original.index != record.Index || original.disposition != applied || record.CDC == nil || original.reason == reasonNone && len(record.CDC) == 0 || original.reason != reasonNone && len(record.CDC) != 0 {
						return sample, errors.Join(errCorrupt, err)
					}
					attempt.OriginalIndex, attempt.Outcome, attempt.CDC = original.index, record.Outcome, record.CDC
					if original.reason == reasonNone {
						attempt.Classification = "commit"
						sample.index = original.index
					} else if nativeGraphCostRetryConflict(original) {
						attempt.Classification = "authoritative-conflict"
						authoritativeAbort = true
					} else {
						attempt.Classification = "authoritative-refusal"
					}
					complete = true
					break
				}
				if record.Index != 0 || record.Outcome != nil || record.CDC != nil {
					return sample, errCorrupt
				}
				_, idle := hub.costProgressSnapshot()
				if idle {
					continue
				} // one more actual map lookup, never infer ACK from quietness
				callStarted = time.Now()
				select {
				case <-changed:
				case <-ctx.Done():
					return sample, ctx.Err()
				case <-hub.ctx.Done():
					return sample, hub.ctx.Err()
				}
				sample.LocalAckTiming.PacketWaitCount++
				sample.LocalAckTiming.PacketWaitNanos += time.Since(callStarted).Nanoseconds()
			}
			if !complete {
				return sample, errCrossProcessPending
			}
		}
		sample.AckNanos += time.Since(ackStarted).Nanoseconds()
		if err = nativeGraphCostEmit(ctx, events, attempt); err != nil {
			return sample, err
		}
		published = true
		if attempt.Classification == "commit" {
			sample.ElapsedNanos = time.Since(started).Nanoseconds()
			return sample, nil
		}
		if !authoritativeAbort {
			return sample, fmt.Errorf("cost original refused: %s", attempt.Classification)
		}
		previous = attempt.Request
		// Only the authoritative terminal reaches this fresh actual factory/read.
	}
	return sample, errLimit
}

type nativeGraphCostAttemptJoin struct {
	client, ordinal int
	request, tx     [16]byte
	body, digest    [32]byte
	index, round    uint64
	classification  string
}

func nativeGraphCostJoinAttempt(previous nativeGraphCostAttemptJoin, a nativeGraphCostAttempt) (nativeGraphCostAttemptJoin, error) {
	return nativeGraphCostJoinAttemptBound(previous, a, nativeGraphCostMaxAttempts)
}
func nativeGraphCostJoinAttemptBound(previous nativeGraphCostAttemptJoin, a nativeGraphCostAttempt, maximum int) (nativeGraphCostAttemptJoin, error) {
	if maximum < 1 || maximum > 64 || a.ClientID < 0 || a.ClientID >= nativeGraphCostWindowCommands || a.AttemptOrdinal < 1 || a.AttemptOrdinal > maximum || a.Engine != "native-raft" || a.Request == ([16]byte{}) || sha256.Sum256(a.Proposal) != a.BodySHA256 {
		return previous, errCorrupt
	}
	command, err := decodePartitionGraphCommand(a.Proposal, defaultMaterializerLimits())
	if err != nil || command.request.identity() != a.Request || command.request.revision.Provenance() != a.Operation.Ordinal {
		return previous, errors.Join(errCorrupt, err)
	}
	if previous.ordinal == 0 {
		if a.AttemptOrdinal != 1 || a.PreviousRequest != ([16]byte{}) {
			return previous, errCorrupt
		}
	} else if a.AttemptOrdinal != previous.ordinal+1 || a.ClientID != previous.client || a.PreviousRequest != previous.request || a.Request == previous.request || (previous.classification != "authoritative-abort" && previous.classification != "authoritative-conflict") {
		return previous, errCorrupt
	}
	return nativeGraphCostAttemptJoin{client: a.ClientID, ordinal: a.AttemptOrdinal, request: a.Request, tx: a.Transaction, body: a.BodySHA256, digest: a.DecisionDigest, index: a.OriginalIndex, round: a.Round, classification: a.Classification}, nil
}
func (a nativeGraphCostAttemptJoin) matches(s nativeGraphCostSample) bool {
	return a.classification == "commit" && a.ordinal == s.ClientAttempts && a.client == s.ClientID && a.request == s.identity && a.body == s.hash && (s.Cross && a.tx == s.tx && a.digest == s.digest && a.round == s.round || !s.Cross && a.index == s.index)
}

// One goroutine per declared client with one bounded inbox each. The owner
// alone writes all artifact streams and joins every original before the cut.
func nativeGraphCostRunPhase(t *testing.T, n *certifiedProcessNetwork, s nativeGraphCostSpec, phase uint64, from *[2]uint64, samples, attempts, records *json.Encoder, result *nativeGraphCostResult, started time.Time) graphCut {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events := make(chan nativeGraphCostClientEvent, nativeGraphCostWindowCommands)
	type job struct{ fact uint64 }
	inboxes := make([]chan job, s.Writers)
	admissions := newNativeGraphCostAdmissions()
	var wg sync.WaitGroup
	for client := range s.Writers {
		inboxes[client] = make(chan job, 1)
		wg.Go(func() {
			// Every admitted inbox job gets a terminal even after cancellation;
			// otherwise a queued client could exit before the collector joins it.
			for j := range inboxes[client] {
				sample, err := nativeGraphCostClientSubmit(ctx, n, s, phase, j.fact, s.cross(j.fact), client, admissions, events)
				events <- nativeGraphCostClientEvent{sample: sample, err: err, done: true}
			}

		})
	}
	finish := sync.OnceFunc(func() {
		cancel()
		for _, inbox := range inboxes {
			close(inbox)
		}
		wg.Wait()
	})
	defer finish()
	var cut graphCut
	for first := uint64(0); first < s.Facts; {
		window := nativeGraphCostWindowCommands
		if s.Writers == 1 {
			window = nativeGraphCostBatchCommands
		}
		count := min(uint64(window), s.Facts-first)
		if first == 0 && s.Writers != 1 {
			count = 1
		} // real fact0 defines canonical ValueID5/6 before dependents
		commands := make([]nativeGraphCostSample, 0, count)
		seen := make(map[nativeGraphCostOperation]bool, count)
		joins := make(map[nativeGraphCostOperation]nativeGraphCostAttemptJoin, count)
		assigned := make(map[nativeGraphCostOperation]int, count)
		for offset := uint64(0); offset < count; offset++ {
			joins[nativeGraphCostOperationID(s, phase, first+offset)] = nativeGraphCostAttemptJoin{}
		}
		var startedJobs, completedJobs uint64
		dispatch := func(client int) {
			assigned[nativeGraphCostOperationID(s, phase, first+startedJobs)] = client
			inboxes[client] <- job{fact: first + startedJobs}
			startedJobs++
		}
		initial := min(count, uint64(s.Writers))
		if first == 0 {
			initial = 1
		} // seed must ACK before dependent factories
		for client := range int(initial) {
			dispatch(client)
		}

		var failure error
		for completedJobs < count {
			event := <-events
			if event.attempt != nil {
				expectedClient, known := assigned[event.attempt.Operation]
				if !known || event.attempt.ClientID != expectedClient {
					failure = errors.Join(failure, errCorrupt)
					cancel()
				} else {
					join, err := nativeGraphCostJoinAttempt(joins[event.attempt.Operation], *event.attempt)
					if err != nil {
						failure = errors.Join(failure, err)
						cancel()
					} else {
						joins[event.attempt.Operation] = join
					}
				}
				if err := attempts.Encode(event.attempt); err != nil {
					failure = errors.Join(failure, err)
				}
				result.Attempts++
				if event.attempt.Classification == "authoritative-abort" || event.attempt.Classification == "authoritative-conflict" {
					result.AbortedAttempts++
				}
				if event.attempt.SubmitInvocations > 1 {
					result.SameAttemptRetries += uint64(event.attempt.SubmitInvocations - 1)
				}
				continue
			}
			if !event.done {
				failure = errors.Join(failure, errCorrupt)
				continue
			}
			completedJobs++
			sample := event.sample
			if event.err != nil {
				failure = errors.Join(failure, event.err)
				cancel()
			}
			if failure != nil {
				if completedJobs == startedJobs {
					break
				}
				continue
			}
			if assigned[sample.Operation] != sample.ClientID || sample.Operation != nativeGraphCostOperationID(s, phase, sample.Fact) || sample.Fact < first || sample.Fact >= first+count || seen[sample.Operation] || !joins[sample.Operation].matches(sample) {
				failure = errors.Join(failure, errCorrupt)
				cancel()
				if completedJobs == startedJobs {
					break
				}
				continue
			}
			seen[sample.Operation] = true
			commands = append(commands, sample)
			if err := samples.Encode(sample); err != nil {
				failure = errors.Join(failure, err)
			}
			result.LocalAckTiming.add(sample.LocalAckTiming)
			result.WritesNanos += sample.ElapsedNanos
			result.AdmissionNanos += sample.AdmissionNanos
			result.ClientCompletions[sample.ClientID]++
			if phase == 4 {
				result.CompletedCreates++
			} else {
				result.CompletedUpdates++
			}
			if failure != nil {
				cancel()
				if completedJobs == startedJobs {
					break
				}
				continue
			}
			if first == 0 && completedJobs == 1 && startedJobs == 1 {
				for client := range int(min(count-startedJobs, uint64(s.Writers))) {
					dispatch(client)
				}
			} else if startedJobs < count {
				dispatch(sample.ClientID)
			}
		}
		if failure != nil {
			finish()
			t.Fatal("cost independent client window", failure)
		}
		if len(commands) != int(count) {
			finish()
			t.Fatal("cost window omitted original")
		}
		start := time.Now()
		var c, r uint64
		cut, c, r = nativeGraphCostCheckpoint(t, n, from, records, s, commands)
		result.CheckpointNanos += time.Since(start).Nanoseconds()
		result.CDCChanges += c
		result.CDCRecords += r
		first += count
		if first%256 == 0 || first == s.Facts {
			progress := struct {
				Phase, CompletedCreates, CompletedUpdates, CDCChanges, CDCRecords uint64
				ElapsedNanos                                                      int64
				LocalAckTiming                                                    nativeGraphCostAckTiming
				TransportTiming                                                   graphProcessCostTiming
			}{phase, result.CompletedCreates, result.CompletedUpdates, result.CDCChanges, result.CDCRecords, time.Since(started).Nanoseconds(), result.LocalAckTiming, n.all[1].mux.hub.costTimingSnapshot()}
			wire, err := json.Marshal(progress)
			if err != nil {
				t.Fatal(err)
			}
			t.Log("native-cost-progress", string(wire))
		}
	}
	return cut
}
