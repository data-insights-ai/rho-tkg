package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// The owner alone writes artifacts. At most32 independent client goroutines,
// 32 frozen jobs and32 terminal events exist regardless of dataset cardinality.
type nativeContentionTrace struct {
	Kind                         string
	Actor                        uint64
	Reply                        nativeContentionReply
	ElapsedNanos, AdmissionNanos int64
	Invocations                  uint64
	Error                        string `json:",omitempty"`
}

type nativeContentionRunMetrics struct {
	Callers, RunAdmissions                      int
	MaximumActive, Cohorts, PriorityCohorts     uint64
	FactoryCalls, RunCalls, SameOriginalRetries uint64
	NativeRunInvocations                        uint64
	PrePrepareRefusals, NativeAborts, Commits   uint64
	AdmissionNanos, CallNanos                   int64
	Scope                                       string
}

func (m *nativeContentionRunMetrics) addDurations(trace nativeContentionTrace) error {
	if m.AdmissionNanos < 0 || trace.AdmissionNanos < 0 || trace.AdmissionNanos > (1<<63-1)-m.AdmissionNanos || m.CallNanos < 0 || trace.ElapsedNanos < 0 || trace.ElapsedNanos > (1<<63-1)-m.CallNanos {
		return errLimit
	}
	m.AdmissionNanos += trace.AdmissionNanos
	m.CallNanos += trace.ElapsedNanos
	return nil
}

type nativeContentionWork struct {
	job  nativeContentionJob
	slot uint64
}
type nativeContentionClientBatch struct {
	cookie nativeContentionCookie
	jobs   []nativeContentionWork // bounded<=2 for the one-caller empty-range wave
	start  <-chan struct{}
}
type nativeContentionClientEvent struct {
	frozen bool
	work   nativeContentionWork
	reply  nativeContentionReply
	trace  nativeContentionTrace
	err    error
}
type nativeContentionClients struct {
	ctx             context.Context
	cancel          context.CancelFunc
	process         *graphProcess
	actor           uint64
	spec            nativeContentionSpec
	inboxes         [nativeContentionCallers]chan nativeContentionClientBatch
	events          chan nativeContentionClientEvent
	factory, run    chan struct{}
	wg              sync.WaitGroup
	once            sync.Once
	mu              sync.Mutex
	active, maximum uint64
	last            nativeContentionReply // collector-owned, one actual completed original
}

func nativeContentionCall(ctx context.Context, p *graphProcess, q nativeContentionControl) (reply nativeContentionReply, err error) {
	body, err := nativeContentionInput(q)
	if err != nil {
		return reply, err
	}
	response, callErr := p.exchange(ctx, processRequest{op: processCostObserve, data: body})
	if len(response.aux) != 0 {
		if len(response.aux) > nativeGraphCostPageBytes {
			return reply, errors.Join(callErr, errLimit)
		}
		d := json.NewDecoder(bytes.NewReader(response.aux))
		d.DisallowUnknownFields()
		if err := d.Decode(&reply); err != nil {
			return reply, errors.Join(callErr, errCorrupt, err)
		}
		if err := d.Decode(new(any)); !errors.Is(err, io.EOF) {
			return reply, errors.Join(callErr, errCorrupt, err)
		}
		if reply.Run != p.workloadRun || reply.Run == ([16]byte{}) || reply.Source != nil && reply.Source.Run != reply.Run {
			return reply, errors.Join(callErr, errCorrupt)
		}
	}
	return reply, errors.Join(callErr, processResponseError(response), ctx.Err())
}

func newNativeContentionClients(ctx context.Context, p *graphProcess, actor uint64, spec nativeContentionSpec) (*nativeContentionClients, error) {
	if ctx == nil || p == nil || spec.validate() != nil || actor != 2 && actor != 4 {
		return nil, errInvalid
	}
	owner, cancel := context.WithCancel(ctx)
	c := &nativeContentionClients{ctx: owner, cancel: cancel, process: p, actor: actor, spec: spec, events: make(chan nativeContentionClientEvent, 2*nativeContentionCallers), factory: make(chan struct{}, 1), run: make(chan struct{}, nativeContentionActive)}
	for client := range spec.Writers {
		c.inboxes[client] = make(chan nativeContentionClientBatch, 1)
		c.wg.Go(func() {
			for batch := range c.inboxes[client] {
				c.client(batch)
			}
		})
	}
	return c, nil
}

func nativeContentionTake(ctx context.Context, tokens chan struct{}) (func(), error) {
	select {
	case tokens <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-tokens
			return nil, err
		}
		return func() { <-tokens }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *nativeContentionClients) client(batch nativeContentionClientBatch) {
	// Even cancellation publishes one frozen/terminal event for each admitted
	// inbox job. The collector drains them before close joins the workers.
	var frozen [2]nativeContentionReply
	var failures [2]error
	for i, work := range batch.jobs {
		started := time.Now()
		var calls uint64
		release, err := nativeContentionTake(c.ctx, c.factory)
		admission := time.Since(started).Nanoseconds()
		if err == nil {
			calls++
			frozen[i], err = nativeContentionCall(c.ctx, c.process, nativeContentionControl{Action: "freeze", Cookie: batch.cookie, Slot: work.slot, Job: work.job})
			// An uncertain factory never permits a new original until the SAME
			// live child-local slot is inspected. A lost process refuses here.
			if err != nil && c.ctx.Err() == nil {
				calls++
				observed, queryErr := nativeContentionCall(c.ctx, c.process, nativeContentionControl{Action: "frozen", Cookie: batch.cookie, Slot: work.slot})
				if queryErr == nil && observed.Class != "factory-absent" {
					frozen[i], err = observed, nil
				} else {
					err = errors.Join(err, queryErr)
				}
			}
			release()
		}
		if err == nil {
			err = nativeContentionValidateFactory(c.spec, batch.cookie, work, frozen[i])
		}
		failures[i] = err
		trace := nativeContentionTrace{Kind: "frozen", Actor: c.actor, Reply: frozen[i], ElapsedNanos: time.Since(started).Nanoseconds(), AdmissionNanos: admission, Invocations: calls}
		if err != nil {
			trace.Error = err.Error()
		}
		c.events <- nativeContentionClientEvent{frozen: true, work: work, reply: frozen[i], trace: trace, err: err}
	}
	select {
	case <-batch.start:
	case <-c.ctx.Done():
	}
	for i, work := range batch.jobs {
		started := time.Now()
		reply, err := frozen[i], failures[i]
		var admission int64
		var calls uint64
		if err == nil {
			var release func()
			release, err = nativeContentionTake(c.ctx, c.run)
			admission = time.Since(started).Nanoseconds()
			if err == nil {
				c.mu.Lock()
				c.active++
				c.maximum = max(c.maximum, c.active)
				c.mu.Unlock()
				reply, err = c.submit(batch.cookie, work, frozen[i], &calls)
				c.mu.Lock()
				c.active--
				c.mu.Unlock()
				release()
			}
		}
		trace := nativeContentionTrace{Kind: "terminal", Actor: c.actor, Reply: reply, ElapsedNanos: time.Since(started).Nanoseconds(), AdmissionNanos: admission, Invocations: calls}
		if err != nil {
			trace.Error = err.Error()
		}
		c.events <- nativeContentionClientEvent{work: work, reply: reply, trace: trace, err: err}
	}
}

func nativeContentionValidateFactory(spec nativeContentionSpec, cookie nativeContentionCookie, work nativeContentionWork, reply nativeContentionReply) error {
	operation, err := spec.operation(work.job)
	if err != nil || reply.Cookie != cookie || reply.Slot != work.slot || reply.Job != work.job || reply.Operation != operation || reply.Request == ([16]byte{}) || len(reply.Business) == 0 || len(reply.Business) > 8<<10 || sha256.Sum256(reply.Business) != reply.BodySHA256 || reply.Capture.Cookie != cookie || len(reply.Capture.Cut) == 0 || len(reply.Capture.Condition) == 0 {
		return errors.Join(errCorrupt, err)
	}
	command, err := decodePartitionGraphCommand(reply.Business, defaultMaterializerLimits())
	if err != nil || [16]byte(command.request.id) != reply.Request || command.request.revision.Provenance() != operation.Ordinal {
		return errors.Join(errCorrupt, err)
	}
	return nil
}

func (c *nativeContentionClients) submit(cookie nativeContentionCookie, work nativeContentionWork, frozen nativeContentionReply, invocations *uint64) (nativeContentionReply, error) {
	var reply nativeContentionReply
	for range nativeContentionSubmissions {
		*invocations++
		actual, err := nativeContentionCall(c.ctx, c.process, nativeContentionControl{Action: "run", Cookie: cookie, Slot: work.slot})
		if actual.Request != ([16]byte{}) && (actual.Request != frozen.Request || actual.BodySHA256 != frozen.BodySHA256 || actual.Operation != frozen.Operation || actual.Job != frozen.Job || actual.Cookie != cookie || actual.Slot != work.slot) {
			return actual, errCorrupt
		}
		if err != nil {
			// No new body/read is made after uncertainty. A bounded repeat of
			// the SAME slot either recovers its original or remains pending.
			if c.ctx.Err() == nil && !errors.Is(err, errInvalid) && !errors.Is(err, errCorrupt) {
				continue
			}
			return actual, err
		}
		reply = actual
		switch actual.Class {
		case "commit", "native-abort":
			if err := nativeContentionValidateDecision(c.spec, frozen, actual); err != nil {
				return actual, err
			}
			return actual, nil
		case "pre-prepare-stale":
			if actual.Proof != nil || actual.Error == "" {
				return actual, errCorrupt
			}
			return actual, nil
		case "pending-or-admission":
			continue
		default:
			return actual, errCrossProcessPending
		}
	}
	return reply, errCrossProcessPending
}

func nativeContentionValidateDecision(spec nativeContentionSpec, frozen, actual nativeContentionReply) error {
	if actual.Proof == nil || actual.Proof.CDC == nil || len(actual.Proof.CDC) != 0 || actual.Calls == 0 || actual.Calls > nativeContentionSubmissions {
		return errCorrupt
	}
	l := defaultMaterializerLimits()
	arena, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return err
	}
	m, err := decodeCrossManifest(actual.Proof.Manifest, l, arena)
	if err != nil || [16]byte(m.request.id) != frozen.Request || m.request.revision.Provenance() != frozen.Operation.Ordinal || m.condition == nil || len(m.parts) < 1 || len(m.parts) > 2 {
		return errors.Join(errCorrupt, err)
	}
	condition, err := encodeGraphReadConditionValue(*m.condition, l)
	if err != nil || !bytes.Equal(condition, frozen.Capture.Condition) {
		return errors.Join(errCorrupt, err)
	}
	decision, err := decodeCrossStatus(actual.Proof.Outcome)
	if err != nil || decision.tx != m.tx || decision.digest != m.digest || decision.phase != crossDecide || decision.commit != (actual.Class == "commit") || decision.index == 0 || decision.commit && decision.round == 0 {
		return errors.Join(errCorrupt, err)
	}
	base := m.request
	revision, err := state.NewRevision(1, base.revision.Provenance())
	if err != nil {
		return err
	}
	base.revision = revision
	body, err := encodePartitionGraphCommand(partitionGraphCommand{configuration: m.configuration, declaration: m.declaration, routing: m.routing, request: base}, l)
	if err != nil || !bytes.Equal(body, frozen.Business) {
		return errors.Join(errCorrupt, err)
	}
	return nil
}

func (c *nativeContentionClients) close() {
	c.once.Do(func() {
		c.cancel()
		for client := range c.spec.Writers {
			close(c.inboxes[client])
		}
		c.wg.Wait()
	})
}

func (c *nativeContentionClients) cohort(jobs []nativeContentionWork, priority bool, stream *json.Encoder, metrics *nativeContentionRunMetrics) (out [nativeContentionCallers]nativeContentionReply, err error) {
	if len(jobs) == 0 || len(jobs) > nativeContentionCallers || priority && len(jobs) != 1 || stream == nil || metrics == nil {
		return out, errInvalid
	}
	var assigned [nativeContentionCallers][2]nativeContentionWork
	var counts [nativeContentionCallers]int
	var slots [nativeContentionCallers]bool
	for _, work := range jobs {
		client := work.job.Client
		if client < 0 || client >= c.spec.Writers || work.slot == 0 || work.slot > nativeContentionCallers || slots[work.slot-1] || counts[client] == 2 {
			return out, errInvalid
		}
		if _, err := c.spec.operation(work.job); err != nil {
			return out, err
		}
		slots[work.slot-1] = true
		assigned[client][counts[client]] = work
		counts[client]++
	}
	opened, err := nativeContentionCall(c.ctx, c.process, nativeContentionControl{Action: "open"})
	if err != nil || opened.Class != "captured-native-prefix" || opened.Cookie.Owner == 0 || opened.Cookie.Request == ([16]byte{}) {
		return out, errors.Join(errCrossProcessPending, err)
	}
	metrics.Cohorts++
	if priority {
		metrics.PriorityCohorts++
	}
	if err := stream.Encode(nativeContentionTrace{Kind: "cohort-open", Actor: c.actor, Reply: opened}); err != nil {
		return out, err
	}
	start := make(chan struct{})
	for client, count := range counts {
		if count != 0 {
			// Ownership is fixed and bounded; the owner does not overwrite these
			// arrays until every admitted job has produced its terminal event.
			c.inboxes[client] <- nativeContentionClientBatch{opened.Cookie, assigned[client][:count], start}
		}
	}
	var failure error
	for range jobs {
		event := <-c.events
		if !event.frozen || event.work.slot == 0 || event.work.slot > nativeContentionCallers || !slots[event.work.slot-1] || event.reply.Cookie != opened.Cookie && event.err == nil {
			failure = errors.Join(failure, errCorrupt)
		}
		metrics.FactoryCalls += event.trace.Invocations
		failure = errors.Join(failure, event.err, metrics.addDurations(event.trace), stream.Encode(event.trace))
	}
	if failure != nil {
		c.cancel()
	}
	close(start)
	clear(slots[:])
	for range jobs {
		event := <-c.events
		if event.frozen || event.work.slot == 0 || event.work.slot > nativeContentionCallers || slots[event.work.slot-1] {
			failure = errors.Join(failure, errCorrupt)
		} else {
			slots[event.work.slot-1] = true
			out[event.work.slot-1] = event.reply
		}
		failure = errors.Join(failure, metrics.addDurations(event.trace))
		metrics.RunCalls += event.trace.Invocations
		metrics.NativeRunInvocations += uint64(event.reply.Calls)
		if event.trace.Invocations > 1 {
			metrics.SameOriginalRetries += event.trace.Invocations - 1
		}
		switch event.reply.Class {
		case "commit":
			metrics.Commits++
			c.last = event.reply
		case "native-abort":
			metrics.NativeAborts++
		case "pre-prepare-stale":
			metrics.PrePrepareRefusals++
		}
		failure = errors.Join(failure, event.err, stream.Encode(event.trace))
	}
	c.mu.Lock()
	metrics.MaximumActive = max(metrics.MaximumActive, c.maximum)
	c.mu.Unlock()
	if failure != nil {
		c.cancel()
		return out, failure // unknown originals never become replacement permission
	}
	closed, err := nativeContentionCall(c.ctx, c.process, nativeContentionControl{Action: "close", Cookie: opened.Cookie})
	if err != nil || closed.Cookie != opened.Cookie || closed.Class != "closed-native-prefix" {
		return out, errors.Join(errCrossProcessPending, err)
	}
	return out, stream.Encode(nativeContentionTrace{Kind: "cohort-close", Actor: c.actor, Reply: closed})
}

// Both native Run slots remain available. A priority cohort is used only after
// every genuine competing original in the previous cohort returned a known
// native ABORT or pre-prepare stale refusal and no useful operation committed.
func (c *nativeContentionClients) usefulWindow(jobs []nativeContentionWork, claim bool, stream *json.Encoder, metrics *nativeContentionRunMetrics) error {
	if len(jobs) == 0 || len(jobs) > nativeContentionCallers {
		return errInvalid
	}
	remaining := append([]nativeContentionWork(nil), jobs...) // bounded32, not dataset-sized
	for len(remaining) != 0 {
		answers, err := c.cohort(remaining, false, stream, metrics)
		if err != nil {
			return err
		}
		next := remaining[:0]
		commits := 0
		for _, work := range remaining {
			reply := answers[work.slot-1]
			if reply.Class == "commit" {
				commits++
				continue
			}
			if reply.Class != "native-abort" && reply.Class != "pre-prepare-stale" || work.job.Attempt >= nativeContentionAttempts {
				return errCrossProcessPending
			}
			work.job.Previous, work.job.Attempt = reply.Request, work.job.Attempt+1
			next = append(next, work)
		}
		if claim && commits > 1 {
			return errCorrupt
		}
		if claim && commits == 1 {
			return nil // other frozen claimants remain real recorded refusals
		}
		if commits == 0 {
			priority, err := c.cohort(next[:1], true, stream, metrics)
			if err != nil {
				return err
			}
			if priority[next[0].slot-1].Class != "commit" {
				return errCrossProcessPending // never manufacture a useful completion
			}
			if claim {
				return nil
			}
			next = next[1:]
		}
		remaining = next
	}
	return nil
}

func nativeContentionChildEnv(spec nativeContentionSpec, run [16]byte, deadline time.Time, reopen bool) ([]string, error) {
	if spec.validate() != nil || run == ([16]byte{}) || time.Until(deadline) <= 0 || time.Until(deadline) > nativeContentionWholeSeconds*time.Second {
		return nil, errInvalid
	}
	body, err := json.Marshal(spec)
	if err != nil {
		return nil, err
	}
	seconds, err := nativeContentionBudgetSecondsFromEnv()
	if err != nil || time.Until(deadline) > time.Duration(seconds)*time.Second { // #nosec G115 -- checked <=365d.
		return nil, errors.Join(errInvalid, err)
	}
	env := []string{"RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_NATIVE_WORKLOAD=" + nativeContentionFormat, "RHO_GRAPH_WORKLOAD_SPEC=" + string(body), fmt.Sprintf("RHO_GRAPH_WORKLOAD_RUN=%x", run), fmt.Sprintf("RHO_GRAPH_WORKLOAD_SECONDS=%d", seconds), fmt.Sprintf("RHO_GRAPH_WORKLOAD_DEADLINE=%d", deadline.UnixNano())}
	// Explicitly override inherited optional profiles; this selector alone owns
	// the fresh grammar. The child independently refuses profile combinations.
	for _, key := range []string{"RHO_GRAPH_COST_SPEC", "RHO_GRAPH_COST_BLOCK_SIZE", "RHO_GRAPH_RANGE_PROCESS", "RHO_GRAPH_CONDITIONAL_PROCESS", "RHO_GRAPH_RETENTION_PROCESS", "RHO_GRAPH_DURABLE_COLUMN_PROCESS", "RHO_GRAPH_NATIVE_FAULT_PROCESS", "RHO_GRAPH_FAULT_FAMILY_PROCESS", "RHO_GRAPH_DURABLE_MODEL", "RHO_GRAPH_DECISION_FAULT_PROCESS", "RHO_GRAPH_LIFECYCLE_PROCESS"} {
		env = append(env, key+"=")
	}
	if reopen {
		env = append(env, "RHO_GRAPH_PROCESS_REOPEN=1")
	} else {
		env = append(env, "RHO_GRAPH_PROCESS_REOPEN=")
	}
	return env, nil
}

func attachNativeContentionNetwork(t *testing.T, n *certifiedProcessNetwork, spec nativeContentionSpec, transport *nativeWorkloadTransport) {
	t.Helper()
	frames, err := spec.retainedFrames()
	if err != nil || frames > uint64(^uint(0)>>1)/4096 {
		t.Fatal("finite fresh transport admission", err)
	}
	attachGraphProcessMuxesWithWorkload(t, n.all, int(frames*4096), transport) // #nosec G115 -- checked native int bound above.
	n.groups[0] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{1: n.all[1], 2: n.all[2], 3: n.all[3]}}
	n.groups[1] = &tinyGraphTransport{nodes: map[uint64]*graphProcess{4: n.all[4], 5: n.all[5], 6: n.all[6]}}
}

func newNativeContentionNetwork(t *testing.T, ctx context.Context, spec nativeContentionSpec, run [16]byte, shaped nativeWorkloadTransportSpec, grants *json.Encoder) (*certifiedProcessNetwork, *nativeWorkloadTransport, graphCut) {
	t.Helper()
	deadline, _ := ctx.Deadline()
	env, err := nativeContentionChildEnv(spec, run, deadline, false)
	if err != nil || grants == nil {
		t.Fatal("fresh native workload bootstrap", err)
	}
	frames, err := spec.retainedFrames()
	if err != nil || frames > (1<<41)/4096 {
		t.Fatal("fresh native transport quota", err)
	}
	transport, err := newNativeWorkloadTransport(ctx, shaped, frames*4096)
	if err != nil {
		t.Fatal("owned directed native transport", err)
	}
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-native-contention-")
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	// Register before the first child. Any partially failed startup still reaps
	// every process already acquired, and closes every opened pipe.
	t.Cleanup(func() {
		for id := uint64(1); id <= 6; id++ {
			if p := n.all[id]; p != nil {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				err := p.shutdown(cleanup, t.Failed())
				cancel()
				if err != nil && !t.Failed() {
					t.Error("native workload process cleanup", id, err)
				}
			}
		}
	})
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, env...)
	}
	attachNativeContentionNetwork(t, n, spec, transport)
	for g, leader := range n.leaders {
		n.groups[g].event(t, leader, processRequest{op: processCampaign})
	}
	nativeColumnProcessAction(t, n, 1, certifiedInitialize, 3, nil)
	n.read(t, 1, observeConfiguration, 0)
	nativeColumnProcessAction(t, n, 1, certifiedInitialize, 8, nil)
	// Genuine initial Fresh follows graph genesis but precedes all recipient /
	// grant controls and business writes. The raw full prefix therefore contains
	// the actual installed-grant originals, with no pre-baseline exemption.
	initial := nativeContentionCut(t, ctx, n, graphReadFresh, graphCut{})
	block := min(spec.homeCount(), idalloc.MaxBlockSize)
	for _, recipient := range []struct{ actor, id, partition uint64 }{{1, 1, 3}, {4, 2, 8}} {
		n.read(t, recipient.actor, observeConfiguration, 0)
		nativeColumnProcessAction(t, n, recipient.actor, certifiedBeginRecipient, recipient.id, nil)
		n.read(t, 1, observeRecipient, recipient.id)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if recipient.partition != 3 {
			nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, recipient.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		n.read(t, recipient.actor, observeRecipient, recipient.id)
		nativeColumnProcessAction(t, n, recipient.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		n.read(t, 1, observeRecipient, recipient.id)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		n.read(t, 1, observeRecipient, recipient.id)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, recipient.partition, []byte{byte(publishDeclaredRecipient)})
		n.read(t, recipient.actor, observeRecipient, recipient.id)
		nativeColumnProcessAction(t, n, recipient.actor, certifiedOpenRecipient, 0, nil)
		count := uint64(1)
		if recipient.partition == 3 {
			count = (spec.homeCount() + block - 1) / block
		}
		if count > 8 {
			t.Fatal("fresh workload exceeds prepaid grant blocks")
		}
		for sequence := uint64(1); sequence <= count; sequence++ {
			_, size, err := nativeContentionGrantPlan(spec, block, recipient.partition, sequence)
			if err != nil {
				t.Fatal(err)
			}
			n.read(t, 1, observeRecipient, recipient.id)
			n.read(t, 1, observeAllocator, 0)
			input := graphE17GrantInput(sequence, size)
			nativeContentionGrantAction(t, n, certifiedReserve, 0, input, recipient.partition, sequence, size, grants)
			n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
			if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
				t.Fatal("genuine fresh workload grant ReadIndex incomplete")
			}
			nativeContentionGrantAction(t, n, certifiedInstallGrant, recipient.partition, nil, recipient.partition, sequence, size, grants)
		}
	}
	// Run on a genuine HOME follower. Its two coordinator workers therefore do
	// not occupy the leaders' two sourceServe workers and create a self-RPC wait.
	n.read(t, 2, observeRecipient, 1)
	nativeColumnProcessAction(t, n, 2, certifiedOpenRecipient, 0, nil)
	return n, transport, initial
}

type nativeContentionGrantTrace struct {
	Phase                              string
	Action, Partition, Sequence, Count uint64
	Proposal, Outcome, CDC, Root       []byte
	Index                              uint64
}

func nativeContentionGrantAction(t *testing.T, n *certifiedProcessNetwork, action, index uint64, data []byte, partition, sequence, count uint64, stream *json.Encoder) {
	t.Helper()
	if action != certifiedReserve && action != certifiedInstallGrant {
		t.Fatal("unsupported fresh grant action")
	}
	factory := n.all[1].call(t, processRequest{op: processCertifiedAction, entity: action, index: index, data: data})
	proposal, err := decodeCertifiedProcessProposal(factory.aux)
	if err != nil || proposal.target.semantic != durableColumnGraphSemanticContractID() || proposal.target.partition != 3 && proposal.target.partition != 8 {
		t.Fatal("actual fresh grant proposal", err)
	}
	trace := nativeContentionGrantTrace{Phase: "pending-original", Action: action, Partition: partition, Sequence: sequence, Count: count, Proposal: factory.aux}
	// Persist exact factory bytes before an uncertain native submission. The
	// controller cannot recover authority by making a replacement factory call.
	if err := stream.Encode(trace); err != nil {
		t.Fatal(err)
	}
	g := 0
	if proposal.target.partition == 8 {
		g = 1
	}
	leader := n.leaders[g]
	n.groups[g].event(t, leader, processRequest{op: processPropose, data: factory.aux})
	state := n.all[leader].call(t, processRequest{op: processStateOp})
	record := n.all[leader].call(t, processRequest{op: processEntryRecord, index: state.applied})
	ns, _ := certifiedProcessNamespace(leader)
	original, err := decodeDeclaredOutcome(record.outcome, ns)
	if err != nil || original.index != state.applied || original.hash != sha256.Sum256(proposal.wire) || original.reason != reasonNone || original.disposition != applied && (action != certifiedInstallGrant || original.disposition != controlRecovery) || record.outcome == nil || record.changes == nil {
		t.Fatal("authentic grant original", err)
	}
	for _, peer := range n.groups[g].nodes {
		actual := peer.call(t, processRequest{op: processEntryRecord, index: state.applied})
		if !bytes.Equal(actual.outcome, record.outcome) || !bytes.Equal(actual.changes, record.changes) {
			t.Fatal("actual grant voter originals differ")
		}
	}
	trace.Phase, trace.Index, trace.Outcome, trace.CDC, trace.Root = "complete-original", original.index, record.outcome, record.changes, state.image
	if err := stream.Encode(trace); err != nil {
		t.Fatal(err)
	}
}

func nativeContentionCut(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, mode graphReadMode, at graphCut) graphCut {
	t.Helper()
	body, err := encodeGraphE15ReadRequest(graphE15ReadRequest{mode: mode, cut: at})
	if err != nil {
		t.Fatal(err)
	}
	response, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedOpen, data: body})
	if err != nil {
		t.Fatal("genuine workload cut", err)
	}
	handle, actual, err := decodeGraphE15ReadToken(response.aux)
	if err != nil || len(actual.certificates) != 2 {
		t.Fatal("complete workload cut", err)
	}
	for g, cert := range actual.certificates {
		partition := uint64(3)
		if g == 1 {
			partition = 8
		}
		if cert.scope.partition != partition || cert.scope.semantic != durableColumnGraphSemanticContractID() {
			t.Fatal("foreign workload cut")
		}
	}
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	_, closeErr := nativeGraphCostExchange(cleanup, n.all[2], processRequest{op: processSourceCertifiedClose, index: handle})
	cancel()
	if err := errors.Join(closeErr, ctx.Err()); err != nil {
		t.Fatal("workload cut cleanup", err)
	}
	return actual
}

func nativeContentionPhases(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, spec nativeContentionSpec, stream *json.Encoder, metrics *nativeContentionRunMetrics, onOld func(graphCut)) (old, final graphCut, last nativeContentionReply) {
	t.Helper()
	clients, err := newNativeContentionClients(ctx, n.all[2], 2, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer clients.close()
	metrics.Callers, metrics.RunAdmissions = spec.Writers, nativeContentionActive
	metrics.Scope = "Independent bounded caller goroutines; two genuine Run slots on HOME follower2; sourceServe workers remain on leaders1/4. Same-prefix frozen cohorts and actual known-refusal-only priority backoff; registered native concurrency/refusals are reported separately from declared callers. Setup/seed are ordered prerequisites, not a32-writer claim. All protocol participants are actual manifest participants, distinct from useful business cross mix."
	for fact := uint64(0); fact < 3; fact++ {
		owner := clients
		if fact == 2 {
			owner, err = newNativeContentionClients(ctx, n.all[4], 4, spec)
			if err != nil {
				t.Fatal(err)
			}
		}
		job := nativeContentionWork{job: nativeContentionJob{Phase: "setup", Fact: fact, Client: 0, Attempt: 1}, slot: 1}
		answers, err := owner.cohort([]nativeContentionWork{job}, false, stream, metrics)
		if fact == 2 {
			owner.close()
		}
		if err != nil || answers[0].Class != "commit" {
			t.Fatal("actual workload setup original", err)
		}
	}
	for fact := uint64(0); fact < spec.Facts/2; fact++ {
		client := int(fact % uint64(spec.Writers))                                                                                             // #nosec G115 -- admitted1/8/32 callers.
		job := nativeContentionWork{job: nativeContentionJob{Phase: "seed", Fact: fact, Client: client, Attempt: 1}, slot: uint64(client + 1)} // #nosec G115 -- admitted bounded client.
		answers, err := clients.cohort([]nativeContentionWork{job}, false, stream, metrics)
		if err != nil || answers[job.slot-1].Class != "commit" {
			t.Fatal("actual seed original", fact, err)
		}
	}
	old = nativeContentionCut(t, ctx, n, graphReadFresh, graphCut{})
	if onOld != nil {
		onOld(old)
	}
	for first := uint64(0); first < spec.Facts; {
		count := min(uint64(spec.Writers), spec.Facts-first) // #nosec G115 -- admitted positive caller count.
		var jobs [nativeContentionCallers]nativeContentionWork
		for offset := uint64(0); offset < count; offset++ {
			jobs[offset] = nativeContentionWork{job: nativeContentionJob{Phase: "hot", Fact: first + offset, Client: int(offset), Attempt: 1}, slot: offset + 1} // #nosec G115 -- offset<32.
		}
		if err := clients.usefulWindow(jobs[:count], false, stream, metrics); err != nil {
			t.Fatal("actual useful hot increments", first, err)
		}
		first += count
	}
	for fact := uint64(0); fact < spec.Facts/2; fact++ {
		var jobs [nativeContentionCallers]nativeContentionWork
		for claimant := uint64(0); claimant < spec.competitors(); claimant++ {
			jobs[claimant] = nativeContentionWork{job: nativeContentionJob{Phase: "claim", Fact: fact, Claimant: claimant, Client: int(claimant % uint64(spec.Writers)), Attempt: 1}, slot: claimant + 1} // #nosec G115 -- admitted claimant/client<=32.
		}
		if err := clients.usefulWindow(jobs[:spec.competitors()], true, stream, metrics); err != nil {
			t.Fatal("actual empty-ID claimant wave", fact, err)
		}
	}
	final = nativeContentionCut(t, ctx, n, graphReadFresh, graphCut{})
	return old, final, clients.last
}

type nativeContentionSourceTrace struct {
	Stage       string
	Partition   uint64
	Member      uint64
	Incarnation [16]byte
	Action      string
	Receipt     nativeContentionSourceReceipt
}

func nativeContentionExportSources(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, spec nativeContentionSpec, initial, final graphCut, stage string, stream *json.Encoder) {
	t.Helper()
	first := nativeGraphCostColumnCutInput(t, initial)
	last := nativeGraphCostColumnCutInput(t, final)
	for actor := uint64(1); actor <= 6; actor++ {
		group := (actor - 1) / 3
		partition := uint64(3)
		if group == 1 {
			partition = 8
		}
		opened, err := nativeContentionCall(ctx, n.all[actor], nativeContentionControl{Action: "records-open", Initial: first, Final: last})
		if err != nil && opened.Source == nil {
			// The SAME constructor body observes the existing live private owner;
			// it cannot create a fresh At source or reset its cumulative ledger.
			opened, err = nativeContentionCall(ctx, n.all[actor], nativeContentionControl{Action: "records-open", Initial: first, Final: last})
		}
		if opened.Source == nil || opened.Source.Cookie.Owner == 0 || opened.Source.Cookie.Request == ([16]byte{}) {
			t.Fatal("raw source owner unavailable", err)
		}
		owner := *opened.Source
		if err := stream.Encode(nativeContentionSourceTrace{stage, partition, actor, n.all[actor].incarnation, "open", owner}); err != nil {
			t.Fatal(err)
		}
		positions := [2]uint64{initial.certificates[0].sourceIndex, initial.certificates[1].sourceIndex}
		var frames uint64
		maxFrames, err := spec.retainedFrames()
		if err != nil {
			t.Fatal(err)
		}
		complete := false
		for sequence := uint64(1); sequence <= maxFrames+1; sequence++ {
			var answer nativeContentionReply
			for attempt := 0; attempt < 4; attempt++ {
				answer, err = nativeContentionCall(ctx, n.all[actor], nativeContentionControl{Action: "records-next", Cookie: owner.Cookie, Sequence: sequence})
				if err == nil {
					break
				}
				if ctx.Err() != nil || errors.Is(err, errInvalid) || errors.Is(err, errCorrupt) {
					t.Fatal("raw original source refused", err)
				}
			}
			if err != nil || answer.Source == nil {
				t.Fatal("raw original source incomplete", err)
			}
			r := *answer.Source
			if r.Cookie != owner.Cookie || r.Sequence != sequence || r.Closed || r.DeadlineUnixNanos != owner.DeadlineUnixNanos || len(r.Frames) > 16 {
				t.Fatal("raw owner replaced/expired/oversized")
			}
			for _, frame := range r.Frames {
				if frame.Partition != partition || frame.Index != positions[group]+1 || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("raw original prefix gap/foreign/missing")
				}
				positions[group]++
				frames++
			}
			if r.Positions != positions || frames > maxFrames {
				t.Fatal("raw private cursor differs from genuine complete frame runs")
			}
			if err := stream.Encode(nativeContentionSourceTrace{stage, partition, actor, n.all[actor].incarnation, "next", r}); err != nil {
				t.Fatal(err)
			}
			if r.EOF {
				if len(r.Frames) != 0 || positions[group] != final.certificates[group].sourceIndex || frames != final.certificates[group].sourceIndex-initial.certificates[group].sourceIndex {
					t.Fatal("raw false/partial EOF")
				}
				complete = true
				break
			}
		}
		if !complete {
			t.Fatal("raw full source EOF missing")
		}
		closed, err := nativeContentionCall(ctx, n.all[actor], nativeContentionControl{Action: "records-close", Cookie: owner.Cookie})
		if err != nil || closed.Source == nil || !closed.Source.Closed || closed.Source.EOF || closed.Source.Cookie != owner.Cookie {
			t.Fatal("raw source cleanup", err)
		}
		if err := stream.Encode(nativeContentionSourceTrace{stage, partition, actor, n.all[actor].incarnation, "close", *closed.Source}); err != nil {
			t.Fatal(err)
		}
	}
}

type nativeContentionColumnTrace struct {
	Stage    string
	Member   uint64
	Input    nativeGraphCostColumnRequest
	Receipt  *nativeGraphCostColumnReceipt `json:",omitzero"`
	Creation *nativeColumnCreationFailure  `json:",omitzero"`
	Error    string                        `json:",omitempty"`
}
type nativeContentionColumnClient struct {
	t        *testing.T
	ctx      context.Context
	p        *graphProcess
	member   uint64
	stage    string
	stream   *json.Encoder
	cookie   nativeGraphCostColumnCookie
	sequence uint64
	failed   *uint64 // one whole-generation uncertain invocation pool, not per page
	last     nativeGraphCostColumnReceipt
}

func (c *nativeContentionColumnClient) call(q nativeGraphCostColumnRequest) nativeGraphCostColumnReceipt {
	c.t.Helper()
	seconds, err := nativeContentionBudgetSecondsFromEnv()
	if err != nil {
		c.t.Fatal("immutable original column run budget", err)
	}
	if q.Action == nativeColumnsRecoverRetire || q.Action == nativeColumnsPinProbe {
		c.t.Fatal("terminal recovery/probe has its own exact response grammar")
	}
	if q.Action != nativeColumnsOpen && q.Action != nativeColumnsRecoverRetire && q.Action != nativeColumnsPinProbe {
		q.Cookie = c.cookie
		if q.Action != nativeColumnsClose && q.Action != nativeColumnsRetire {
			q.Sequence = c.sequence + 1
		}
	}
	for attempt := uint64(0); attempt < nativeContentionStarts(seconds); attempt++ {
		input, err := nativeGraphCostColumnInput(q)
		if err != nil {
			c.t.Fatal(err)
		}
		// A real full source TTL must be able to expire before this RPC. A
		// shorter call timeout is classified separately and never buys a TTL.
		call, cancel := context.WithTimeout(c.ctx, 75*time.Second)
		reply, err := nativeContentionCall(call, c.p, nativeContentionControl{Action: "columns", Column: input})
		cancel()
		if err == nil {
			r, decodeErr := nativeGraphCostColumnDecodeReceipt(reply.Column)
			if decodeErr != nil {
				c.t.Fatal("actual column receipt", decodeErr)
			}
			if q.Action != nativeColumnsOpen && r.Cookie != c.cookie {
				c.t.Fatal("column query owner substituted")
			}
			if c.cookie != (nativeGraphCostColumnCookie{}) && q.Action != nativeColumnsOpen && r.DeadlineUnixNanos != c.last.DeadlineUnixNanos {
				c.t.Fatal("column whole-query deadline changed")
			}
			if c.last.Cookie == r.Cookie && r.Cookie.Owner != 0 {
				for _, pair := range [][2]nativeColumnWork{{c.last.Charged, r.Charged}, {c.last.DurableWork, r.DurableWork}} {
					if !nativeColumnWorkMonotone(pair[0], pair[1]) {
						c.t.Fatal("column cumulative grant refunded")
					}
				}
			}
			if err := c.stream.Encode(nativeContentionColumnTrace{Stage: c.stage, Member: c.member, Input: q, Receipt: &r}); err != nil {
				c.t.Fatal(err)
			}
			c.cookie, c.last = r.Cookie, r
			if q.Action != nativeColumnsOpen && q.Action != nativeColumnsClose && q.Action != nativeColumnsRetire {
				c.sequence = q.Sequence
			}
			return r
		}
		trace := nativeContentionColumnTrace{Stage: c.stage, Member: c.member, Input: q, Creation: reply.Creation, Error: err.Error()}
		if err := c.stream.Encode(trace); err != nil {
			c.t.Fatal(err)
		}
		if c.ctx.Err() != nil || errors.Is(err, errInvalid) || errors.Is(err, errCorrupt) || errors.Is(err, errGraphPinExpired) || errors.Is(err, errGraphPinReleased) || errors.Is(err, errGraphHistoryExpired) {
			c.t.Fatal("column source unavailable; partial evidence retained", err)
		}
		fullTTL := reply.Creation != nil && reply.Creation.FullTTL && reply.Creation.Expired && reply.Creation.DeadlineUnixNanos-reply.Creation.IssuedUnixNanos == int64(time.Minute)
		if !fullTTL {
			*c.failed++
			if *c.failed > nativeContentionFailedPages(seconds) {
				c.t.Fatal("column whole-generation failed grant pool exhausted", err)
			}
		}
		if q.Action == nativeColumnsOpen {
			// Resolve an uncertain actual owner first. Absence is never inferred
			// from timeout; any later attempt uses the SAME persisted generation.
			if reply.Creation == nil {
				observed, queryErr := nativeContentionCall(c.ctx, c.p, nativeContentionControl{Action: "column-owner", Column: input})
				if queryErr != nil || len(observed.Column) == 0 {
					c.t.Fatal("lost constructor authority; no replacement source", errors.Join(err, queryErr))
				}
				r, decodeErr := nativeGraphCostColumnDecodeReceipt(observed.Column)
				if decodeErr != nil {
					c.t.Fatal(decodeErr)
				}
				if err := c.stream.Encode(nativeContentionColumnTrace{Stage: c.stage, Member: c.member, Input: q, Receipt: &r}); err != nil {
					c.t.Fatal(err)
				}
				c.cookie, c.last = r.Cookie, r
				return r
			}
			q.Create = false
		}
		// Non-open retries retain this exact cookie/sequence/action. Private
		// renewal rebinds its genuine cursor, not an after-ID/current At query.
	}
	c.t.Fatal("column source owner starts exhausted")
	return nativeGraphCostColumnReceipt{}
}

func nativeColumnWorkMonotone(a, b nativeColumnWork) bool {
	return a.Records <= b.Records && a.Bytes <= b.Bytes && a.DecodedRows <= b.DecodedRows && a.OutputBytes <= b.OutputBytes && a.RPCs <= b.RPCs && a.WireBytes <= b.WireBytes
}

// The two real publishers have disjoint declared source masks. Neither the
// parent nor the other partition manufactures the omitted partition's frames.
func nativeContentionColumnClients(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, pair *nativeGraphCostColumnPair, stage string, create bool, failed *uint64, stream *json.Encoder) [2]*nativeContentionColumnClient {
	t.Helper()
	var clients [2]*nativeContentionColumnClient
	for g, member := range [2]uint64{1, 4} {
		c := &nativeContentionColumnClient{t: t, ctx: ctx, p: n.all[member], member: member, stage: stage, stream: stream, failed: failed}
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsOpen, Initial: nativeGraphCostColumnCutInput(t, pair.Initial), Final: nativeGraphCostColumnCutInput(t, pair.Final), Generation: pair.ID, Create: create, RetainedExpires: 20})
		if r.Binding.Version != 2 || r.Binding.Coverage != byte(1<<g) || pair.Bindings[g].Coverage != 0 && !nativeColumnSame(pair.Bindings[g], r.Binding) || r.Retention == nil {
			t.Fatal("actual durable disjoint column owner changed binding")
		}
		pair.Bindings[g], clients[g] = r.Binding, c
	}
	return clients
}

func nativeContentionColumnClose(clients [2]*nativeContentionColumnClient) {
	for _, c := range clients {
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsClose})
		if !r.Closed || r.EOF {
			c.t.Fatal("column Close conferred EOF")
		}
	}
}

func nativeContentionColumnPublish(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, spec nativeContentionSpec, initial, final graphCut, id [16]byte, stage string, create, updated bool, failed *uint64, stream *json.Encoder) nativeGraphCostColumnPair {
	t.Helper()
	pair := nativeGraphCostColumnPair{Initial: initial, Final: final, ID: id, Updated: updated}
	clients := nativeContentionColumnClients(t, ctx, n, &pair, stage, create, failed, stream)
	for g, c := range clients {
		for pages := uint64(0); ; pages++ {
			if pages > c.last.Binding.Policy.MaxChunks {
				t.Fatal("finite genuine column publication exhausted")
			}
			r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsPublish})
			if r.EOF {
				pair.Manifests[g] = r.Manifest
				break
			}
			if r.Manifest.Generation.ID != ([16]byte{}) {
				t.Fatal("partial publisher supplied an immutable seal")
			}
		}
	}
	if err := nativeColumnJoinBoundaries(pair.Bindings, pair.Manifests); err != nil {
		t.Fatal("both genuine sealed full-cut EOFs required", err)
	}
	for _, mode := range []struct {
		name         string
		cached, fill bool
	}{{"native", false, false}, {"cache-fill", false, true}, {"cached", true, false}} {
		for _, c := range clients {
			c.stage = stage + "/" + mode.name
		}
		nativeContentionColumnRead(t, clients, &pair, spec, pair.Final, updated, mode.cached, mode.fill)
	}
	nativeContentionColumnClose(clients)
	return pair
}

func nativeContentionColumnRead(t *testing.T, clients [2]*nativeContentionColumnClient, pair *nativeGraphCostColumnPair, spec nativeContentionSpec, at graphCut, updated, cached, fill bool) {
	t.Helper()
	var census [3]uint64 // presence, initial property, genuine hot increment
	for g, c := range clients {
		r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRead, At: nativeGraphCostColumnCutInput(t, at), Cached: cached, Fill: fill})
		if !nativeColumnSame(r.Manifest, pair.Manifests[g]) || r.Cached != cached || r.Fill != fill || r.EOF || len(r.Body)+len(r.Rows) != 0 {
			t.Fatal("read changed actual immutable generation")
		}
		initial, err := decodeGraphCertificate(r.Manifest.Generation.Sources[g].Initial)
		if err != nil {
			t.Fatal(err)
		}
		selected := at.certificates[g]
		if selected.scope != initial.scope {
			t.Fatal("column selected foreign axis/configuration/cut")
		}
		position, ordinal := initial.sourceIndex, uint64(0)
		var chain [32]byte
		var frames, rows, retained uint64
		for {
			r = c.call(nativeGraphCostColumnRequest{Action: nativeColumnsNext})
			if !nativeColumnSame(r.Manifest, pair.Manifests[g]) || r.Ordinal != ordinal || r.Cached != cached || r.Fill != fill {
				t.Fatal("column continuation changed immutable identity")
			}
			if r.EOF {
				m := pair.Manifests[g]
				if len(r.Body)+len(r.Rows) != 0 || ordinal != m.Chunks || frames != m.Frames || rows != m.Rows || retained != m.Bytes || position != m.Positions[g] || chain != m.Chain {
					t.Fatal("column incomplete genuine EOF")
				}
				break
			}
			if ordinal >= pair.Manifests[g].Chunks {
				t.Fatal("column data beyond sealed prefix")
			}
			decoded, err := decodeNativeColumnChunk(r.Manifest.Generation, ordinal, chain, r.Body, c.last.Binding.Policy)
			if err != nil {
				t.Fatal("actual native numeric columns", err)
			}
			actual := []nativeColumnRow{} // at most the admitted whole chunk, never degree/history
			for _, frame := range decoded {
				if frame.Partition != initial.scope.partition || frame.Index != position+1 || frame.Outcome == nil || frame.CDC == nil {
					t.Fatal("column omitted an authentic source frame")
				}
				position++
				frames++
				rows += uint64(len(frame.Rows))
				if frame.Index <= selected.sourceIndex {
					for _, row := range frame.Rows {
						class, err := nativeContentionColumnRow(spec, row)
						if err != nil {
							t.Fatal("foreign contention column row", err, row)
						}
						census[class]++
						actual = append(actual, row)
					}
				}
			}
			if !reflect.DeepEqual(actual, r.Rows) {
				t.Fatal("selected typed rows differ from actual binary columns")
			}
			retained += uint64(len(nativeColumnChunkKey(pair.ID, ordinal)) + len(r.Body))
			chain = nativeColumnChain(chain, r.Body)
			ordinal++
		}
	}
	want := [3]uint64{3 + spec.Facts/2, spec.Facts / 2, 0}
	if updated {
		want = [3]uint64{3 + spec.Facts, spec.Facts, spec.Facts}
	}
	if census != want {
		t.Fatal("exact contention column change census", census, want)
	}
}

// This checks each real change's semantic coordinates with constant metadata.
// The separate bounded checker joins per-original uniqueness and serial chains;
// neither this census nor a valid column checksum certifies missing originals.
func nativeContentionColumnRow(s nativeContentionSpec, row nativeColumnRow) (int, error) {
	if len(row.Axis) == 0 || len(row.Scope) == 0 || !row.After.Present || row.After.Null || row.After.Revision == 0 {
		return 0, errCorrupt
	}
	if row.Kind == byte(graphstate.Node) {
		phase := uint64(1)
		if row.Owner == 3 {
			phase = 2
		} else if row.Owner == s.remoteNode() {
			phase = 3
		} else if row.Owner != 1 {
			return 0, errCorrupt
		}
		if row.Component != byte(graphstate.Presence) || row.Life != 0 || row.After.Handle != row.Owner+1 || row.After.Provenance != phase || row.Before != (nativeColumnCell{}) || row.Name != "" || row.Type != "" || row.Source != 0 || row.Target != 0 || row.SourceLife != 0 || row.TargetLife != 0 {
			return 0, errCorrupt
		}
		return 0, nil
	}
	if row.Kind != byte(graphstate.Relationship) || row.Owner < 7 || (row.Owner-7)%2 != 0 {
		return 0, errCorrupt
	}
	fact := (row.Owner - 7) / 2
	owner, life, target, targetLife := s.edge(fact)
	if fact >= s.Facts || row.Owner != owner || row.Type != "edge" || row.Mode != byte(graphstate.LifeBound) || row.Source != 1 || row.Target != target || row.SourceLife != 2 || row.TargetLife != targetLife {
		return 0, errCorrupt
	}
	seed := uint64(4) + fact
	if fact >= s.Facts/2 {
		first := uint64(4) + 3*s.Facts/2 + (fact-s.Facts/2)*s.competitors()
		if row.After.Provenance < first || row.After.Provenance >= first+s.competitors() {
			return 0, errCorrupt
		}
		seed = row.After.Provenance
	}
	if row.Component == byte(graphstate.Presence) {
		if row.Life != 0 || row.Name != "" || row.After.Handle != life || row.After.Provenance != seed || row.Before != (nativeColumnCell{}) {
			return 0, errCorrupt
		}
		return 0, nil
	}
	if row.Component != byte(graphstate.ScalarProperty) || row.Life != life || row.Name != "p" {
		return 0, errCorrupt
	}
	value, err := s.valueID(row.After.Value)
	if err != nil || value != row.After.Handle {
		return 0, errCorrupt
	}
	if row.After.Value == 5 && row.After.Provenance == seed && row.Before == (nativeColumnCell{}) {
		return 1, nil
	}
	firstHot := uint64(4) + s.Facts/2
	if row.After.Provenance < firstHot || row.After.Provenance >= firstHot+s.Facts || !row.Before.Present || row.Before.Null || row.Before.Revision == 0 || row.After.Revision <= row.Before.Revision || row.After.Value != row.Before.Value+1 {
		return 0, errCorrupt
	}
	hotFact := row.After.Provenance - firstHot
	actualTarget, err := s.target("hot", hotFact)
	beforeID, beforeErr := s.valueID(row.Before.Value)
	if err != nil || beforeErr != nil || actualTarget != fact || row.Before.Handle != beforeID {
		return 0, errCorrupt
	}
	return 2, nil
}

type nativeContentionPointTrace struct {
	Stage  string
	At     []byte
	Fact   uint64
	Entity nativeLifecycleEntity
}

func nativeContentionExpectedValue(s nativeContentionSpec, fact uint64, updated bool) int64 {
	if !updated {
		return 5
	}
	for candidate := uint64(0); candidate < 10; candidate++ {
		if s.cross(candidate) == s.cross(fact) {
			if fact != candidate {
				return 5
			}
			count := s.Facts * s.CrossPercent / 100
			if !s.cross(fact) {
				count = s.Facts - count
			}
			return 5 + int64(count) // #nosec G115 -- admitted<=1M.
		}
	}
	return 5
}

func nativeContentionPoints(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeContentionSpec, cut graphCut, updated bool, stage string, stream *json.Encoder) {
	t.Helper()
	wire := nativeGraphCostColumnCutInput(t, cut)
	// Each independently bounded point operation owns an ordinary At lease and
	// unchanged SDK limits. The outer fixed deadline and exact N+1 invocation
	// bound cover this whole comparison; none is incident-cursor continuation.
	for fact := uint64(0); fact <= s.Facts; fact++ {
		opened, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processSourceCertifiedOpen, data: wire})
		if err != nil {
			t.Fatal("actual point At open", err)
		}
		handle, actual, err := decodeGraphE15ReadToken(opened.aux)
		if err != nil || !reflect.DeepEqual(actual, cut) {
			t.Fatal("point At substituted current cut", err)
		}
		owner, life, target, _ := s.edge(fact)
		if fact == s.Facts {
			owner = 5 // genuine phantom, outside all entity factories
		}
		response, callErr := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processCostObserve, index: handle, entity: owner})
		cleanup, stop := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
		_, closeErr := nativeGraphCostExchange(cleanup, n.all[2], processRequest{op: processSourceCertifiedClose, index: handle})
		stop()
		if err := errors.Join(callErr, closeErr, ctx.Err()); err != nil {
			t.Fatal("point read/cleanup", err)
		}
		var entity nativeLifecycleEntity
		if err := json.Unmarshal(response.aux, &entity); err != nil || entity.ID != owner {
			t.Fatal("actual point answer", err)
		}
		present := fact < s.Facts && (updated || fact < s.Facts/2)
		if !present {
			if entity.Found || entity.Effective || entity.Property.Present || entity.Presence.Present {
				t.Fatal("phantom/new edge leaked into retained old cut", fact, entity)
			}
		} else {
			want := nativeContentionExpectedValue(s, fact, updated)
			value, valueErr := s.valueID(want)
			if valueErr != nil || !entity.Found || !entity.Effective || entity.Kind != byte(graphstate.Relationship) || entity.Mode != byte(graphstate.LifeBound) || entity.Type != "edge" || entity.Source != 1 || entity.Target != target || entity.EffectiveLife != life || !entity.Presence.Present || entity.Presence.Handle != life || !entity.Property.Present || entity.Property.Null || entity.Property.Kind != "i64" || entity.Property.I64 != want || entity.Property.Handle != value || entity.Property.Revision == 0 || entity.Property.Provenance == 0 {
				t.Fatal("exact useful point semantics", fact, entity, want)
			}
		}
		if err := stream.Encode(nativeContentionPointTrace{stage, wire, fact, entity}); err != nil {
			t.Fatal(err)
		}
	}
}

// A physical restart gets a new genuine query at each original cut. Private
// process-local sources/cursors are never reconstructed from artifact bytes.
func nativeContentionRestart(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, s nativeContentionSpec, run [16]byte, shaped nativeWorkloadTransportSpec, processes, restore *json.Encoder) (*nativeWorkloadTransport, nativeWorkloadTransportReceipt) {
	t.Helper()
	nativeGraphCostDrain(t, n)
	var previous [6]processResponse
	for member := uint64(1); member <= 6; member++ {
		previous[member-1] = n.all[member].call(t, processRequest{op: processStateOp})
		if previous[member-1].stats[statViews] != 0 || previous[member-1].stats[statPinnedBytes] != 0 {
			t.Fatal("live source remains before physical stop", member)
		}
		wire, err := encodeProcessResponse(previous[member-1])
		if err != nil || restore.Encode(struct {
			Stage  string
			Member uint64
			Raw    []byte
		}{"before-stop", member, wire}) != nil {
			t.Fatal("actual pre-stop native state", err)
		}
	}
	initialTransport := n.all[1].mux.hub.workload
	if nativeGraphCostStop(t, n, 1, processes) != 6 {
		t.Fatal("original six-child cohort not reaped")
	}
	initialReceipt := initialTransport.receipt(n.all[1].mux.hub)
	deadline, _ := ctx.Deadline()
	env, err := nativeContentionChildEnv(s, run, deadline, true)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := s.retainedFrames()
	if err != nil || frames > (1<<41)/4096 {
		t.Fatal(errLimit)
	}
	freshTransport, err := newNativeWorkloadTransport(ctx, shaped, frames*4096)
	if err != nil {
		t.Fatal(err)
	}
	fresh := make(map[uint64]*graphProcess, 6)
	for member := uint64(1); member <= 6; member++ {
		fresh[member] = startGraphProcess(t, filepath.Join(n.dir, fmt.Sprint(member)), member, env...)
		// Publish acquired ownership immediately so the already registered cleanup
		// also reaps a partial second-cohort startup failure.
		n.all[member] = fresh[member]
	}
	n.all = fresh
	attachNativeContentionNetwork(t, n, s, freshTransport)
	for member := uint64(1); member <= 6; member++ {
		actual, before := n.all[member].call(t, processRequest{op: processStateOp}), previous[member-1]
		if actual.incarnation == before.incarnation || actual.applied != before.applied || !bytes.Equal(actual.image, before.image) || actual.stats[statGeneration] != before.stats[statGeneration] || actual.stats[statCommit] != before.stats[statCommit] || actual.stats[statViews] != 0 || actual.stats[statPinnedBytes] != 0 {
			t.Fatal("same-directory native restore differs", member)
		}
		wire, err := encodeProcessResponse(actual)
		if err != nil || restore.Encode(struct {
			Stage  string
			Member uint64
			Raw    []byte
		}{"restored-before-campaign", member, wire}) != nil {
			t.Fatal("actual restored native state", err)
		}
	}
	for g, leader := range n.leaders {
		n.groups[g].event(t, leader, processRequest{op: processCampaign})
		n.read(t, leader, observeConfiguration, 0)
	}
	return freshTransport, initialReceipt
}

func nativeContentionReplayGrants(t *testing.T, ctx context.Context, n *certifiedProcessNetwork, input string, stream *json.Encoder) uint64 {
	t.Helper()
	f, err := os.Open(input)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := f.Close(); err != nil {
			t.Error(err)
		}
	}()
	d := json.NewDecoder(f)
	var count uint64
	for {
		var actual nativeContentionGrantTrace
		if err := d.Decode(&actual); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if actual.Phase != "complete-original" {
			continue
		}
		if count >= 18 {
			t.Fatal("bounded original grant inventory")
		}
		proposal, err := decodeCertifiedProcessProposal(actual.Proposal)
		if err != nil || proposal.target.semantic != durableColumnGraphSemanticContractID() {
			t.Fatal("stored authentic grant proposal", err)
		}
		g := 0
		if proposal.target.partition == 8 {
			g = 1
		} else if proposal.target.partition != 3 {
			t.Fatal("foreign grant replay")
		}
		leader := n.leaders[g]
		before := n.all[leader].call(t, processRequest{op: processStateOp})
		n.groups[g].event(t, leader, processRequest{op: processPropose, data: actual.Proposal})
		after := n.all[leader].call(t, processRequest{op: processStateOp})
		record := n.all[leader].call(t, processRequest{op: processEntryRecord, index: after.applied})
		ns, _ := certifiedProcessNamespace(leader)
		o, err := decodeDeclaredOutcome(record.outcome, ns)
		if err != nil || o.disposition != requestReplay || o.index != actual.Index || o.hash != sha256.Sum256(proposal.wire) || o.reason != reasonNone || after.applied <= before.applied || len(record.changes) != 0 || !bytes.Equal(before.image, after.image) {
			t.Fatal("same original grant replay changed native effects", err)
		}
		for _, peer := range n.groups[g].nodes {
			r := peer.call(t, processRequest{op: processEntryRecord, index: after.applied})
			if !bytes.Equal(r.outcome, record.outcome) || !bytes.Equal(r.changes, record.changes) {
				t.Fatal("grant replay differs on native voter")
			}
		}
		actual.Phase, actual.Index, actual.Outcome, actual.CDC, actual.Root = "actual-replay-entry", after.applied, record.outcome, record.changes, after.image
		if err := errors.Join(stream.Encode(actual), ctx.Err()); err != nil {
			t.Fatal(err)
		}
		count++
	}
	return count
}

type nativeContentionResult struct {
	Version                                   int                  `json:"version"`
	Format                                    string               `json:"format"`
	Engine                                    string               `json:"engine"`
	Spec                                      nativeContentionSpec `json:"spec"`
	Run                                       [16]byte             `json:"run"`
	BinarySHA256                              string               `json:"binarySHA256"`
	Directory, ArtifactDirectory              string
	Initial, Old, Final, BeforeStop, Replayed []byte
	DeadlineUnixNanos                         int64
	BudgetSeconds, CausalMinimumMillis        uint64
	Metrics                                   nativeContentionRunMetrics
	Columns                                   [2][2]nativeColumnManifest `json:",omitzero"`
	ColumnBindings                            [2][2]nativeColumnBinding  `json:",omitzero"`
	ColumnFailedInvocations                   [2]uint64                  `json:",omitzero"`
	Transport                                 [2]nativeWorkloadTransportReceipt
	Reaped, ProcessInstances, GrantReplays    int
	OriginalReplay                            nativeContentionReply
	ReopenedOriginal                          p05Attempt
	PersistedFileBytes, AllocatedFileBytes    uint64
	TotalNanos                                int64
	Scope                                     string
}

func nativeContentionExecutableSHA(t *testing.T) string {
	t.Helper()
	path, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.New()
	_, readErr := io.Copy(h, f)
	if err := errors.Join(readErr, f.Close()); err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func TestNativeContentionWorkloadCell(t *testing.T) {
	if testing.Short() {
		t.Skip("explicit fresh workload cell is excluded from routine short CI")
	}
	spec, run, err := nativeContentionSpecFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if spec == nil {
		t.Skip("explicit fresh native workload selector required")
	}
	admission, err := nativeContentionAdmissionFromEnv(*spec, run)
	if err != nil {
		t.Fatal(err)
	}
	deadline, shaped := time.Unix(0, admission.DeadlineUnixNanos), admission.Transport
	output := os.Getenv("RHO_GRAPH_WORKLOAD_OUTPUT")
	if !filepath.IsAbs(output) {
		t.Fatal("explicit absolute fresh artifact directory")
	}
	if err := os.Mkdir(output, 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RHO_GRAPH_PROCESS_ARTIFACT", output)
	ctx, stop := context.WithDeadline(t.Context(), deadline)
	defer stop()
	started := time.Now()
	open := func(name string) (*os.File, *json.Encoder) {
		f, err := os.OpenFile(filepath.Join(output, name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				t.Error(err)
			}
		})
		return f, json.NewEncoder(f)
	}
	grantFile, grants := open("grants.jsonl")
	n, _, initial := newNativeContentionNetwork(t, ctx, *spec, run, shaped, grants)
	if err := grantFile.Close(); err != nil {
		t.Fatal(err)
	}
	_, operations := open("operations.jsonl")
	_, points := open("points.jsonl")
	_, sources := open("sources.jsonl")
	_, cohorts := open("processes.jsonl")
	_, states := open("restore.jsonl")
	var columnStream *json.Encoder
	if spec.Layout == "columns" {
		_, columnStream = open("columns.jsonl")
	}
	result := nativeContentionResult{Version: 1, Format: nativeContentionFormat, Engine: "native-raft", Spec: *spec, Run: run, BinarySHA256: nativeContentionExecutableSHA(t), Directory: n.dir, ArtifactDirectory: output, DeadlineUnixNanos: deadline.UnixNano(), BudgetSeconds: admission.BudgetSeconds, CausalMinimumMillis: admission.CausalMinimumMillis, ProcessInstances: 12}
	result.Initial = nativeGraphCostColumnCutInput(t, initial)
	var failed [2]uint64
	var columnPairs [2]nativeGraphCostColumnPair
	old, final, last := nativeContentionPhases(t, ctx, n, *spec, operations, &result.Metrics, func(old graphCut) {
		if columnStream != nil {
			columnPairs[0] = nativeContentionColumnPublish(t, ctx, n, *spec, initial, old, [16]byte{1}, "old-seal", true, false, &failed[0], columnStream)
		}
	})
	result.Old, result.Final = nativeGraphCostColumnCutInput(t, old), nativeGraphCostColumnCutInput(t, final)
	nativeContentionPoints(t, ctx, n, *spec, old, false, "old-after-mutation", points)
	nativeContentionPoints(t, ctx, n, *spec, final, true, "final", points)
	if columnStream != nil {
		columnPairs[1] = nativeContentionColumnPublish(t, ctx, n, *spec, initial, final, [16]byte{2}, "final-seal", false, true, &failed[1], columnStream)
		clients := nativeContentionColumnClients(t, ctx, n, &columnPairs[0], "old-after-mutation", false, &failed[0], columnStream)
		nativeContentionColumnRead(t, clients, &columnPairs[0], *spec, old, false, false, false)
		nativeContentionColumnClose(clients)
	}
	beforeStop := nativeContentionCut(t, ctx, n, graphReadFresh, graphCut{})
	result.BeforeStop = nativeGraphCostColumnCutInput(t, beforeStop)
	nativeContentionExportSources(t, ctx, n, *spec, initial, beforeStop, "before-stop", sources)
	secondTransport, firstReceipt := nativeContentionRestart(t, ctx, n, *spec, run, shaped, cohorts, states)
	result.Transport[0] = firstReceipt
	nativeContentionPoints(t, ctx, n, *spec, old, false, "reopened-old", points)
	nativeContentionPoints(t, ctx, n, *spec, final, true, "reopened-final", points)
	if columnStream != nil {
		for generation := range columnPairs {
			for _, cached := range []bool{false, true} {
				stage := fmt.Sprintf("reopened-%d-cached-%t", generation, cached)
				clients := nativeContentionColumnClients(t, ctx, n, &columnPairs[generation], stage, false, &failed[generation], columnStream)
				nativeContentionColumnRead(t, clients, &columnPairs[generation], *spec, columnPairs[generation].Final, generation == 1, cached, false)
				nativeContentionColumnClose(clients)
			}
		}
	}
	if len(last.Resume) == 0 || last.Proof == nil {
		t.Fatal("missing genuine final immutable original")
	}
	resumeReply, err := nativeGraphCostExchange(ctx, n.all[2], processRequest{op: processP05Resume, data: last.Resume})
	var replay p05Attempt
	if err != nil || json.Unmarshal(resumeReply.aux, &replay) != nil || replay.Error != "" {
		t.Fatal("same original recovery after replacement", err)
	}
	recovered, err := decodeGraphProcessCoordinatorResult(processResponse{aux: replay.Result})
	prior, priorErr := decodeGraphProcessCoordinatorResult(processResponse{aux: last.Result})
	if err != nil || priorErr != nil || recovered.decision != prior.decision || recovered.resume != prior.resume {
		t.Fatal("reopened original changed first durable decision", err, priorErr)
	}
	result.OriginalReplay = last
	result.ReopenedOriginal = replay
	_, replayGrants := open("grant-replays.jsonl")
	result.GrantReplays = int(nativeContentionReplayGrants(t, ctx, n, filepath.Join(output, "grants.jsonl"), replayGrants)) // #nosec G115 -- explicit<=18.
	if columnStream != nil {
		for generation := range columnPairs {
			clients := nativeContentionColumnClients(t, ctx, n, &columnPairs[generation], fmt.Sprintf("retire-%d", generation), false, &failed[generation], columnStream)
			for _, c := range clients {
				r := c.call(nativeGraphCostColumnRequest{Action: nativeColumnsRetire})
				if r.Retention == nil || !r.Closed {
					t.Fatal("actual durable generation retirement incomplete")
				}
			}
		}
	}
	replayed := nativeContentionCut(t, ctx, n, graphReadFresh, graphCut{})
	result.Replayed = nativeGraphCostColumnCutInput(t, replayed)
	nativeContentionExportSources(t, ctx, n, *spec, initial, replayed, "reopened-replayed", sources)
	result.Reaped = nativeGraphCostStop(t, n, 2, cohorts)
	result.Transport[1] = secondTransport.receipt(n.all[1].mux.hub)
	for member := uint64(1); member <= 6; member++ {
		bytes, allocated, err := nativeGraphCostFileBytes(filepath.Join(n.dir, fmt.Sprint(member)))
		if err != nil || !nativeColumnAdd(result.PersistedFileBytes, bytes) || !nativeColumnAdd(result.AllocatedFileBytes, allocated) {
			t.Fatal("all-child persistent accounting overflow", err)
		}
		result.PersistedFileBytes += bytes
		result.AllocatedFileBytes += allocated
	}
	result.TotalNanos = time.Since(started).Nanoseconds()
	if columnStream != nil {
		for generation := range columnPairs {
			result.Columns[generation], result.ColumnBindings[generation] = columnPairs[generation].Manifests, columnPairs[generation].Bindings
		}
		result.ColumnFailedInvocations = failed
	}
	result.Scope = "One fresh contention-empty-range cell; genuine six-child native sources and same-directory replacement; full originals/ABORT/pending refusal costs retained. Fixed whole deadline, unchanged two Run slots and bounded independent callers. All actual voter/derived/cache files and framed transport counted; injected native delays are not physical LAN/WAN measurements or a seeded D scheduler campaign. Other workload families and full D composition remain separate."
	if result.Reaped != 6 || ctx.Err() != nil {
		t.Fatal("incomplete native workload cleanup", ctx.Err())
	}
	f, terminal := open("result.json")
	if err := errors.Join(terminal.Encode(result), f.Close()); err != nil {
		t.Fatal(err)
	}
}
