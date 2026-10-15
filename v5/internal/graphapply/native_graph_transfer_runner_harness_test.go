package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

func nativeTransferChildEnv(s nativeTransferSpec, a nativeTransferAdmission, reopen bool) ([]string, error) {
	if s != a.Spec || nativeTransferValidate(s.cost()) != nil {
		return nil, errInvalid
	}
	cost, e := json.Marshal(s.cost())
	if e != nil {
		return nil, e
	}
	spec, e := json.Marshal(s)
	if e != nil {
		return nil, e
	}
	transport, e := json.Marshal(a.Transport)
	if e != nil {
		return nil, e
	}
	grants, e := json.Marshal(a.Grants)
	if e != nil {
		return nil, e
	}
	env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", nativeTransferProcessFlag + "=1", "RHO_GRAPH_COST_SPEC=" + string(cost), "RHO_GRAPH_WORKLOAD_SPEC=" + string(spec), "RHO_GRAPH_WORKLOAD_TRANSPORT=" + string(transport), "RHO_GRAPH_TRANSFER_GRANTS=" + string(grants), "RHO_GRAPH_WORKLOAD_RUN=" + a.Run, "RHO_GRAPH_WORKLOAD_SECONDS=" + strconv.FormatUint(a.BudgetSeconds, 10), "RHO_GRAPH_WORKLOAD_DEADLINE=" + strconv.FormatInt(a.DeadlineUnixNanos, 10)}
	for _, flag := range []string{"RHO_GRAPH_NATIVE_WORKLOAD", "RHO_GRAPH_CREATE_UPDATE_PROCESS", "RHO_GRAPH_IMMUTABLE_COLUMNS_PROCESS", "RHO_GRAPH_LATE_CORRECTION_PROCESS", "RHO_GRAPH_SELECTORS_PROCESS", "RHO_GRAPH_FEED_PROCESS", "RHO_GRAPH_FEED_RETAINED_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_EDGE_UNIQUE_PROCESS", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_NATIVE_FAULT_FAMILY", "RHO_GRAPH_PROTOCOL_STATE_PROCESS", "RHO_GRAPH_PROTOCOL_STATE_RECOVERY", "RHO_GRAPH_STATE_RETENTION_PROCESS"} {
		env = append(env, flag+"=")
	}
	if reopen {
		env = append(env, "RHO_GRAPH_PROCESS_REOPEN=1")
	}
	return env, nil
}

func nativeTransferAttach(t *testing.T, n *certifiedProcessNetwork, a nativeTransferAdmission) {
	t.Helper()
	f, e := nativeTransferFrames(a.Spec.cost(), a.Grants)
	if e != nil || f > uint64(^uint(0)>>1)/1024 {
		t.Fatal("finite owned transport", e)
	}
	ctx, cancel := context.WithDeadline(t.Context(), time.Unix(0, a.DeadlineUnixNanos))
	t.Cleanup(cancel)
	w, e := newNativeWorkloadTransport(ctx, a.Transport, f*1024)
	if e != nil {
		t.Fatal(e)
	}
	attachGraphProcessMuxesWithWorkload(t, n.all, int(f*1024), w) // #nosec G115 -- checked native int, numeric limit only.
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}

type nativeTransferTrace struct {
	Stage                        string
	Member, Call                 uint64
	Attempt                      int
	Input                        nativeTransferInput
	Reply                        nativeTransferReply
	Error                        string `json:",omitempty"`
	AttemptNanos, OperationNanos int64
}

// The fresh wrapper owns a finite retry pool. No failed transport response is
// converted into allocation, a page ACK, move completion or an original result.
type nativeTransferClient struct {
	t      *testing.T
	ctx    context.Context
	n      *certifiedProcessNetwork
	member uint64
	stage  string
	stream *json.Encoder
	calls  uint64
	extra  *int
}

func (c *nativeTransferClient) exchange(q nativeTransferInput) (reply nativeTransferReply, err error) {
	q.Version = 1
	wire, err := nativeTransferInputWire(q)
	if err != nil {
		return reply, err
	}
	if _, err := nativeTransferInputDecode(processRequest{op: processCostObserve, data: wire}); err != nil {
		return reply, err
	}
	c.calls++
	started := time.Now()
	for attempt := 1; attempt <= 8; attempt++ {
		if attempt > 1 {
			if c.extra == nil || *c.extra >= 64 {
				return reply, errLimit
			}
			*c.extra++
		}
		begin := time.Now()
		r, cause := nativeGraphCostExchange(c.ctx, c.n.all[c.member], processRequest{op: processCostObserve, data: wire})
		reply = nativeTransferReply{}
		if len(r.aux) > 0 {
			if e := nativeFeedDecodeJSON(r.aux, &reply); e != nil {
				return reply, e
			}
		}
		trace := nativeTransferTrace{Stage: c.stage, Member: c.member, Call: c.calls, Attempt: attempt, Input: q, Reply: reply, AttemptNanos: time.Since(begin).Nanoseconds(), OperationNanos: time.Since(started).Nanoseconds()}
		if cause != nil {
			trace.Error = cause.Error()
		}
		if e := c.stream.Encode(trace); e != nil {
			return reply, e
		}
		if cause == nil {
			if reply.Version != 1 || reply.Action != q.Action || reply.Failure != 0 {
				return reply, errCorrupt
			}
			return reply, nil
		}
		if q.Action == "move-run" || q.Action == "move-resume" {
			// The move driver distinguishes authenticated durable progress from
			// SAME/unknown retry; one large intent is not one transient invocation.
			return reply, cause
		}
		if c.ctx.Err() != nil || errors.Is(cause, errInvalid) || errors.Is(cause, errCorrupt) || errors.Is(cause, errGraphPinReleased) || errors.Is(cause, errGraphPinExpired) || errors.Is(cause, errGraphHistoryExpired) || !nativeGraphCostRetrySame(cause) {
			return reply, cause
		}
	}
	return reply, errCrossProcessPending
}
func (c *nativeTransferClient) must(q nativeTransferInput) nativeTransferReply {
	c.t.Helper()
	r, e := c.exchange(q)
	if e != nil {
		c.t.Fatal("native transfer operation refused", c.stage, q.Action, e)
	}
	return r
}

func (c *nativeTransferClient) move(intentWire []byte, a nativeTransferAdmission) nativeTransferReply {
	c.t.Helper()
	intent, e := decodeRangeMoveIntent(intentWire)
	if e != nil {
		c.t.Fatal(e)
	}
	q := nativeTransferInput{Action: "move-run", Original: intentWire}
	var latest nativeTransferReply
	var previous [2]rangeMoveStatus
	var identity rangeMoveResume
	stalled := 0
	for invocation := uint64(0); invocation < 8*(8*a.Grants.MaxPages+64); invocation++ {
		r, cause := c.exchange(q)
		var result rangeMoveResult
		var decoded error
		if len(r.Body) > 0 {
			result, decoded = decodeGraphE17MoveResult(processResponse{aux: r.Body})
		}
		if result.resume.request != (requestID{}) {
			if result.resume.scope != intent.source || result.resume.request != intent.id || result.resume.intent != sha256.Sum256(intentWire) || identity != (rangeMoveResume{}) && identity != result.resume {
				c.t.Fatal("move retry changed original identity")
			}
			identity = result.resume
		}
		next, progress, progressErr := nativeTransferMoveProgress(intent, sha256.Sum256(intentWire), identity.move, previous, [2]rangeMoveStatus{result.source, result.target})
		if progressErr != nil {
			c.t.Fatal("native move semantic progress regressed or changed intent", progressErr)
		}
		if cause == nil && decoded == nil && result.committed && result.source.phase == rangeMoveFinish && result.target.phase == rangeMoveFinish && result.source.result == rangeMoveApplied && result.target.result == rangeMoveApplied {
			return r
		}
		if c.ctx.Err() != nil || errors.Is(cause, errInvalid) || errors.Is(cause, errCorrupt) || errors.Is(decoded, errInvalid) || errors.Is(decoded, errCorrupt) || result.source.phase == rangeMoveAbort || result.target.phase == rangeMoveAbort || result.source.result == rangeMoveBudgetLimit || result.target.result == rangeMoveBudgetLimit {
			c.t.Fatal("mandatory move capacity/authority failure", cause, decoded)
		}
		if cause == nil && decoded == nil {
			c.t.Fatal("nonterminal move returned nominal success")
		}
		if progress && identity != (rangeMoveResume{}) {
			previous = next
			stalled = 0
			latest = r
			q = nativeTransferInput{Action: "move-resume", Original: bytes.Clone(r.Body)}
			continue
		}
		// Unknown work never obtains a new ID or refunds a previous allowance.
		stalled++
		if stalled >= 8 || c.extra == nil || *c.extra >= 64 {
			c.t.Fatal("SAME move remains unresolved", cause, decoded)
		}
		*c.extra++
		if len(latest.Body) > 0 {
			q = nativeTransferInput{Action: "move-resume", Original: latest.Body}
		}
	}
	c.t.Fatal("finite actual move progression ceiling exhausted")
	return latest
}

// These scalars authorize only another bounded observation of the SAME native
// intent. A newer blocked physical index is not transfer progress or authority.
func nativeTransferMoveProgress(intent rangeMoveIntent, digest [32]byte, move [16]byte, previous, observed [2]rangeMoveStatus) (next [2]rangeMoveStatus, progress bool, err error) {
	next = previous
	for slot, status := range observed {
		if status == (rangeMoveStatus{}) || status.result != rangeMoveApplied {
			continue
		}
		want := intent.source
		if slot == 1 {
			want = intent.target
		}
		if status.format != 3 || status.index == 0 || status.ns != (namespace{want.graph, want.partition}) || status.intent != digest || status.move != move || status.phase < previous[slot].phase || status.phase == previous[slot].phase && status.sequence < previous[slot].sequence || status.index < previous[slot].index || status.sequence > uint64(intent.maxPages) {
			return next, false, errCorrupt
		} // #nosec G115 -- canonical positive page bound.
		if status.phase > previous[slot].phase || status.phase == previous[slot].phase && status.sequence > previous[slot].sequence {
			progress = true
			next[slot] = status
		}
	}
	return next, progress, nil
}
func (c *nativeTransferClient) feed(q nativeFeedInput) nativeFeedReply {
	c.t.Helper()
	q.Version = 1
	r := c.must(nativeTransferInput{Action: "feed", Feed: &q})
	var out nativeFeedReply
	if e := nativeFeedDecodeJSON(r.Body, &out); e != nil || out.Version != 1 || out.Action != q.Action || out.Failure != 0 {
		c.t.Fatal("actual feed reply", e)
	}
	return out
}
func (c *nativeTransferClient) query(q nativeSelectorsInput) nativeSelectorsReply {
	c.t.Helper()
	q.Version = 1
	r := c.must(nativeTransferInput{Action: "query", Query: &q})
	var out nativeSelectorsReply
	if e := nativeFeedDecodeJSON(r.Body, &out); e != nil || out.Version != 1 || out.Action != q.Action || out.Failure != 0 {
		c.t.Fatal("actual query reply", e)
	}
	return out
}

// Consume the actual cursor, in bounded32-ID receipts, BEFORE any factory uses
// its IDs. Repeating the same sequence returns the owned receipt, not Next again.
func (c *nativeTransferClient) acquire(sequence, count uint64) nativeTransferAllocationReceipt {
	c.t.Helper()
	r := c.must(nativeTransferInput{Action: "acquire", Sequence: sequence, Count: count})
	var acquired nativeTransferAllocationReceipt
	if e := nativeFeedDecodeJSON(r.Body, &acquired); e != nil || acquired.Request.Sequence != sequence || acquired.Request.Count != count || acquired.Cursor == 0 || acquired.First == 0 || acquired.Last-acquired.First+1 != count {
		c.t.Fatal("actual Recipient.Acquire", e)
	}
	for consumed, ordinal := uint64(0), uint64(1); consumed < count; ordinal++ {
		q := nativeTransferInput{Action: "next", Cursor: acquired.Cursor, Sequence: ordinal, Count: min(32, count-consumed)}
		r := c.must(q)
		var next nativeTransferAllocationReceipt
		if e := nativeFeedDecodeJSON(r.Body, &next); e != nil || next.Cursor != acquired.Cursor || next.First != acquired.First || next.Last != acquired.Last || uint64(len(next.IDs)) != q.Count || next.Consumed != consumed+q.Count {
			c.t.Fatal("actual bounded Cursor.Next", e)
		}
		for i, id := range next.IDs {
			if id != acquired.First+consumed+uint64(i) {
				c.t.Fatal("allocator gap/duplicate")
			}
		}
		if ordinal == 1 {
			again := c.must(q)
			if !bytes.Equal(again.Body, r.Body) {
				c.t.Fatal("SAME Next consumed IDs twice")
			}
		}
		consumed = next.Consumed
	}
	return acquired
}

type nativeTransferDriver struct {
	mu        sync.Mutex
	extra     int
	originals uint64
	spec      nativeTransferSpec
}

func newNativeTransferAdmissions() *nativeGraphCostAdmissions {
	return &nativeGraphCostAdmissions{local: make(chan struct{}, 8), cross: make(chan struct{}, 2), transfer: true}
}

func (d *nativeTransferDriver) retry() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.extra >= 64 {
		return errLimit
	}
	d.extra++
	return nil
}
func (d *nativeTransferDriver) beginOriginal() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.originals >= nativeTransferOriginals(d.spec.cost()) {
		return errLimit
	}
	d.originals++
	return nil
}

// Post-move business is genuinely local to partition8. This is the existing
// local Submit/immutable-original protocol with the ACTUAL actor and namespace;
// it does not add remote reads merely to preserve a declared logical cross%.
func nativeTransferLocalUpdate(ctx context.Context, n *certifiedProcessNetwork, s nativeTransferSpec, fact uint64, client int, a *nativeGraphCostAdmissions, events chan<- nativeGraphCostClientEvent) (sample nativeGraphCostSample, err error) {
	started := time.Now()
	operation := nativeGraphCostOperationID(s.cost(), 5, fact)
	sample = nativeGraphCostSample{Phase: operation.Phase, Fact: fact, Cross: false, phase: 5, Operation: operation, ClientID: client}
	release, err := a.acquire(ctx, false)
	sample.AdmissionNanos = time.Since(started).Nanoseconds()
	if err != nil {
		return sample, err
	}
	defer release()
	var previous [16]byte
	for ordinal := 1; ordinal <= 64; ordinal++ {
		if err := a.beginOriginal(); err != nil {
			return sample, err
		}
		began := time.Now()
		r, e := nativeGraphCostExchange(ctx, n.all[4], processRequest{op: processCertifiedAction, entity: certifiedGraphCost, index: 5, data: binary.BigEndian.AppendUint64(nil, fact)})
		if e != nil {
			return sample, e
		}
		proposal, e := decodeCertifiedProcessProposal(r.aux)
		if e != nil {
			return sample, e
		}
		command, e := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
		if e != nil || proposal.target.partition != 8 || !nativeTransferFrozenShape(command) {
			return sample, errors.Join(errCorrupt, e)
		}
		sample.ProposalNanos += time.Since(began).Nanoseconds()
		sample.identity, sample.partition, sample.hash = command.request.identity(), 8, sha256.Sum256(proposal.wire)
		attempt := nativeGraphCostAttempt{Operation: operation, Engine: "native-raft", ClientID: client, AttemptOrdinal: ordinal, PreviousRequest: previous, Request: sample.identity, BodySHA256: sample.hash, Proposal: bytes.Clone(proposal.wire)}
		wire, e := encodeCertifiedProcessProposal(proposal)
		if e != nil {
			return sample, e
		}
		ack := time.Now()
		complete := false
		for calls := 0; calls < 8 && !complete; calls++ {
			if calls > 0 {
				if e := a.extra(); e != nil {
					return sample, e
				}
			}
			attempt.SubmitInvocations++
			r, e = nativeGraphCostExchange(ctx, n.all[4], processRequest{op: processPropose, data: wire})
			if e != nil {
				if nativeGraphCostRetrySame(e) && ctx.Err() == nil {
					continue
				}
				return sample, e
			}
			if e := n.all[4].mux.hub.enqueue(4, r.packets, r.handles...); e != nil {
				return sample, e
			}
			for polls := 0; polls < 128; polls++ {
				hub := n.all[4].mux.hub
				changed, _ := hub.costProgressSnapshot()
				r, e = nativeGraphCostExchange(ctx, n.all[4], processRequest{op: processCostRecord, data: append([]byte(nativeGraphCostRequestRecordTag), wire...)})
				if e != nil {
					return sample, e
				}
				var record nativeGraphCostRequestRecord
				if e := nativeFeedDecodeJSON(r.aux, &record); e != nil {
					return sample, e
				}
				if record.Found {
					o, e := decodePartitionGraphOutcome(record.Outcome, command.request.ns)
					if e != nil || o.identity != sample.identity || o.hash != sample.hash || o.index != record.Index || o.disposition != applied || record.CDC == nil || o.reason == reasonNone && len(record.CDC) == 0 || o.reason != reasonNone && len(record.CDC) != 0 {
						return sample, errors.Join(errCorrupt, e)
					}
					attempt.OriginalIndex, attempt.Outcome, attempt.CDC = o.index, record.Outcome, record.CDC
					if o.reason == reasonNone {
						attempt.Classification = "commit"
						sample.index = o.index
					} else if nativeGraphCostRetryConflict(o) {
						attempt.Classification = "authoritative-conflict"
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
				}
				select {
				case <-changed:
				case <-ctx.Done():
					return sample, ctx.Err()
				case <-hub.ctx.Done():
					return sample, hub.ctx.Err()
				}
			}
		}
		if !complete {
			return sample, errCrossProcessPending
		}
		sample.AckNanos += time.Since(ack).Nanoseconds()
		sample.ClientAttempts = ordinal
		if e := nativeGraphCostEmit(ctx, events, attempt); e != nil {
			return sample, e
		}
		if attempt.Classification == "commit" {
			sample.ElapsedNanos = time.Since(started).Nanoseconds()
			return sample, nil
		}
		if attempt.Classification != "authoritative-conflict" {
			return sample, errInvalid
		}
		previous = attempt.Request
	}
	return sample, errLimit
}

// Replace only the real target coordinator process, preserving every other
// voter and the SAME owned delay scheduler/deadline. No token supplies a peer.
func nativeTransferReplaceTarget(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, a nativeTransferAdmission, out *json.Encoder) {
	t.Helper()
	if e := drainGraphProcessMuxes(ctx, n.all); e != nil {
		t.Fatal(e)
	}
	prior := n.all[4]
	before, e := nativeGraphCostExchange(ctx, prior, processRequest{op: processStateOp})
	if e != nil {
		t.Fatal(e)
	}
	cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	cause := prior.shutdown(cleanup, true)
	cancel()
	if prior.cmd.ProcessState == nil {
		t.Fatal("target process not reaped", cause)
	}
	status, ok := prior.cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok || !status.Signaled() || status.Signal() != syscall.SIGKILL {
		t.Fatal("target was not actually SIGKILLed", cause)
	}
	env, e := nativeTransferChildEnv(a.Spec, a, true)
	if e != nil {
		t.Fatal(e)
	}
	replacement := startGraphProcess(t, filepath.Join(n.dir, "4"), 4, env...)
	hub := prior.mux.hub
	mux := &graphProcessMux{p: replacement, id: 4, hub: hub, pending: make(map[uint64]chan graphProcessAnswer), relays: make(map[uint64]context.CancelFunc), relayQueue: make(chan graphProcessRelayJob, processSourceWorkers)}
	replacement.mux = mux
	hub.mu.Lock()
	hub.peers[4] = mux
	hub.mu.Unlock()
	n.all[4], n.groups[1].nodes[4] = replacement, replacement
	hub.wg.Go(mux.readLoop)
	for range processSourceWorkers {
		hub.wg.Go(mux.relayLoop)
	}
	restored, e := nativeGraphCostExchange(ctx, replacement, processRequest{op: processStateOp})
	if e != nil || restored.incarnation == before.incarnation || restored.applied != before.applied || !bytes.Equal(restored.image, before.image) || restored.stats[statViews] != 0 {
		t.Fatal("real same-volume target restore", e)
	}
	left, e := encodeProcessResponse(before)
	if e != nil {
		t.Fatal(e)
	}
	right, e := encodeProcessResponse(restored)
	if e != nil {
		t.Fatal(e)
	}
	if e := out.Encode(struct {
		Stage               string
		Member              uint64
		PID, ReplacementPID int
		Signal              string
		Reaped              bool
		Before, Restored    []byte
	}{"target-replacement", 4, prior.cmd.Process.Pid, replacement.cmd.Process.Pid, "SIGKILL", true, left, right}); e != nil {
		t.Fatal(e)
	}
	nativeTransferNativeEvent(t, ctx, n, 4, processRequest{op: processCampaign})
	nativeTransferNativeEvent(t, ctx, n, 4, processRequest{op: processCertifiedRead, entity: uint64(observeConfiguration)})
	ready, e := nativeGraphCostExchange(ctx, n.all[4], processRequest{op: processStateOp})
	if e != nil || !ready.ready {
		t.Fatal("target actual completed configuration read", e)
	}
}

func nativeTransferNativeEvent(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, actor uint64, q processRequest) processResponse {
	t.Helper()
	r, e := nativeGraphCostExchange(ctx, n.all[actor], q)
	if e != nil {
		t.Fatal("native owned event", e)
	}
	if e := n.all[actor].mux.hub.enqueue(actor, r.packets, r.handles...); e != nil {
		t.Fatal(e)
	}
	if e := drainGraphProcessMuxes(ctx, n.all); e != nil {
		t.Fatal(e)
	}
	return r
}

func nativeTransferRead(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, actor uint64, kind allocationObservationKind, recipient uint64) {
	t.Helper()
	nativeTransferNativeEvent(t, ctx, n, actor, processRequest{op: processCertifiedRead, entity: uint64(kind), index: recipient})
	r, e := nativeGraphCostExchange(ctx, n.all[actor], processRequest{op: processStateOp})
	if e != nil || !r.ready {
		t.Fatal("actual allocation ReadIndex incomplete", e)
	}
}

func nativeTransferPrepare(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, actor, action, index uint64, data []byte) allocationProposal {
	t.Helper()
	r, e := nativeGraphCostExchange(ctx, n.all[actor], processRequest{op: processCertifiedAction, entity: action, index: index, data: data})
	if e != nil {
		t.Fatal(e)
	}
	p, e := decodeCertifiedProcessProposal(r.aux)
	if e != nil || p.target.semantic != rangeMovePooledSemanticContractID() {
		t.Fatal("genuine pooled factory", e)
	}
	return p
}

func nativeTransferInstallControl(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, actor, action uint64, p allocationProposal, want reason, stream *json.Encoder) outcome {
	t.Helper()
	wire, e := encodeCertifiedProcessProposal(p)
	if e != nil {
		t.Fatal(e)
	}
	leader := n.leaders[0]
	if p.target.partition == 8 {
		leader = n.leaders[1]
	} else if p.target.partition != 3 {
		t.Fatal("foreign actual control target")
	}
	nativeTransferNativeEvent(t, ctx, n, leader, processRequest{op: processPropose, data: wire})
	r, e := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processLookup, data: wire})
	if e != nil {
		t.Fatal(e)
	}
	ns, _ := certifiedProcessNamespace(leader)
	o, e := decodeDeclaredOutcome(r.outcome, ns)
	if e != nil || o.hash != sha256.Sum256(p.wire) || o.reason != want || o.disposition != applied && o.disposition != controlRecovery {
		t.Fatal("exact native allocation original", e)
	}
	root, e := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processStateOp})
	if e != nil {
		t.Fatal(e)
	}
	actual, e := nativeGraphCostExchange(ctx, n.all[leader], processRequest{op: processEntryRecord, index: o.index})
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(actual.outcome, r.outcome) {
		t.Fatal("control original index mismatch")
	}
	if e := stream.Encode(struct {
		Actor, Action, Index         uint64
		Proposal, Outcome, CDC, Root []byte
	}{actor, action, o.index, wire, actual.outcome, actual.changes, root.image}); e != nil {
		t.Fatal(e)
	}
	return o
}

// Hold the matching real committed Decide reply, cancel that actual caller,
// then recover only its SAME proposal. Parent quietness is never a decision.
func nativeTransferLostK(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeTransferSpec, d *nativeTransferDriver, admissions *nativeGraphCostAdmissions, started time.Time, attempts, operations *json.Encoder, end *nativeTransferResult) (proposal allocationProposal, original crossProcessResult) {
	t.Helper()
	began := time.Now()
	release, e := admissions.acquire(ctx, true)
	if e != nil {
		t.Fatal(e)
	}
	defer release()
	if e := d.beginOriginal(); e != nil {
		t.Fatal(e)
	}
	factory, e := nativeGraphCostExchange(ctx, n.all[1], processRequest{op: processCertifiedAction, entity: certifiedGraphCost, index: 5, data: binary.BigEndian.AppendUint64(nil, 0)})
	if e != nil {
		t.Fatal(e)
	}
	proposal, e = decodeCertifiedProcessProposal(factory.aux)
	if e != nil {
		t.Fatal(e)
	}
	command, e := decodePartitionGraphCommand(proposal.wire, defaultMaterializerLimits())
	if e != nil {
		t.Fatal(e)
	}
	transaction := crossTransactionID(command.request.ns, command.request.id)
	hub := n.all[2].mux.hub
	entered := make(chan pairedFaultBoundary, 1)
	released := make(chan struct{})
	var once sync.Once
	hub.mu.Lock()
	prior := hub.afterRelay
	hub.afterRelay = func(call context.Context, origin, partition uint64, request, reply []byte) error {
		if prior != nil {
			if e := prior(call, origin, partition, request, reply); e != nil {
				return e
			}
		}
		if origin != 2 || partition != 3 || !pairedFaultIsDecide(request) {
			return nil
		}
		r, e := decodeGraphWireResponse(reply)
		if e != nil {
			return e
		}
		status, e := decodeCrossStatus(r.body)
		if e != nil || status.tx != transaction || !status.commit || status.phase != crossDecide {
			return errors.Join(errCorrupt, e)
		}
		once.Do(func() { entered <- pairedFaultBoundary{Request: bytes.Clone(request), Reply: bytes.Clone(reply)} })
		select {
		case <-released:
			return nil
		case <-call.Done():
			return call.Err()
		}
	}
	hub.mu.Unlock()
	var cleanup sync.Once
	finish := func() { cleanup.Do(func() { close(released); hub.mu.Lock(); hub.afterRelay = prior; hub.mu.Unlock() }) }
	defer finish()
	call, cancel := context.WithCancel(ctx)
	done := make(chan graphProcessAnswer, 1)
	go func() {
		r, e := n.all[2].exchange(call, processRequest{op: processSourceCoordinate, data: proposal.wire})
		done <- graphProcessAnswer{response: r, err: e}
	}()
	select {
	case boundary := <-entered:
		end.LostRequest, end.LostReply = boundary.Request, boundary.Reply
	case <-ctx.Done():
		cancel()
		finish()
		t.Fatal("matching K native reply not reached", ctx.Err())
	}
	cancel()
	// Do not release the committed reply until the actual canceled caller has
	// retired. Releasing concurrently could turn this into an observed success.
	select {
	case actual := <-done:
		cause := errors.Join(actual.err, processResponseError(actual.response))
		if !errors.Is(cause, context.Canceled) {
			t.Fatal("K caller did not lose its held reply", cause)
		}
		if len(actual.response.aux) != 0 {
			if terminal, e := decodeGraphProcessCoordinatorResult(actual.response); e == nil && terminal.decision.phase == crossDecide {
				t.Fatal("K caller received a terminal result before loss")
			}
		}
	case <-ctx.Done():
		finish()
		t.Fatal("lost K caller not retired", ctx.Err())
	}
	finish()
	if e := d.retry(); e != nil {
		t.Fatal(e)
	}
	recovered, e := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCoordinate, data: proposal.wire})
	if e != nil {
		t.Fatal(e)
	}
	original, e = decodeGraphProcessCoordinatorResult(recovered)
	if e != nil || !original.decision.commit || original.decision.tx != transaction {
		t.Fatal("SAME K original recovery", e)
	}
	var proof nativeGraphCostDecisionProof
	if e := nativeFeedDecodeJSON(recovered.changes, &proof); e != nil {
		t.Fatal(e)
	}
	arena, e := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
	if e != nil {
		t.Fatal(e)
	}
	manifest, e := decodeCrossManifest(proof.Manifest, defaultMaterializerLimits(), arena)
	status, e2 := decodeCrossStatus(proof.Outcome)
	if e != nil || e2 != nil || manifest.digest != original.decision.digest || manifest.tx != transaction || status != original.decision || proof.CDC == nil || len(proof.CDC) != 0 {
		t.Fatal("full native K R/D/original proof", e, e2)
	}
	encoded, e := encodeCertifiedProcessProposal(proposal)
	if e != nil {
		t.Fatal(e)
	}
	end.K = encoded
	end.KOriginal, e = encodeProcessResponse(recovered)
	if e != nil {
		t.Fatal(e)
	}
	command.version = 0
	business, e := encodePartitionGraphCommand(command, defaultMaterializerLimits())
	if e != nil {
		t.Fatal(e)
	}
	operation := nativeGraphCostOperationID(s.cost(), 5, 0)
	client := int((operation.Ordinal - 1) % uint64(s.Writers))
	attempt := nativeGraphCostAttempt{Operation: operation, Engine: "native-raft", ClientID: client, AttemptOrdinal: 1, Request: [16]byte(command.request.id), Transaction: transaction, BodySHA256: sha256.Sum256(proposal.wire), BusinessSHA256: sha256.Sum256(business), DecisionDigest: original.decision.digest, OriginalIndex: original.decision.index, Round: original.decision.round, Classification: "commit", SubmitInvocations: 2, Proposal: bytes.Clone(proposal.wire), Manifest: proof.Manifest, Outcome: proof.Outcome, CDC: proof.CDC}
	if e := attempts.Encode(attempt); e != nil {
		t.Fatal(e)
	}
	stop := time.Now()
	sample := nativeGraphCostSample{Phase: "update", Fact: 0, Cross: s.cost().cross(0), ElapsedNanos: stop.Sub(began).Nanoseconds(), ClientAttempts: 1, Operation: operation, ClientID: client, phase: 5, partition: 3, identity: attempt.Request, tx: transaction, digest: original.decision.digest, round: original.decision.round}
	if e := operations.Encode(nativeFeedBusiness{Sample: sample, StartNanos: began.Sub(started).Nanoseconds(), StopNanos: stop.Sub(started).Nanoseconds(), ElapsedNanos: stop.Sub(began).Nanoseconds()}); e != nil {
		t.Fatal(e)
	}
	end.Completed[client]++
	return proposal, original
}
