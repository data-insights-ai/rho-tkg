package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

// These are fresh workload allowances, not a change to ordinary request limits.
// Extra invocations retire one GLOBAL token before retry, at most8 invocations
// for one original. Exhaustion preserves the exact pending original and fails.
const nativeEdgeExtraInvocations = 64

func nativeGraphEdgeUniqueDeadlineFromEnv() (time.Time, error) {
	secondsText, deadlineText := os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), os.Getenv("RHO_GRAPH_WORKLOAD_DEADLINE")
	seconds, err := strconv.ParseUint(secondsText, 10, 32)
	if err != nil || seconds == 0 || seconds > 365*24*60*60 || strconv.FormatUint(seconds, 10) != secondsText {
		return time.Time{}, errInvalid
	}
	nanos, err := strconv.ParseInt(deadlineText, 10, 64)
	if err != nil || nanos <= 0 || strconv.FormatInt(nanos, 10) != deadlineText {
		return time.Time{}, errInvalid
	}
	deadline := time.Unix(0, nanos)
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > time.Duration(seconds)*time.Second {
		return time.Time{}, errLimit
	}
	return deadline, nil
}

type nativeGraphEdgeUniqueAttempt struct {
	Family, Engine, Classification, Error               string
	Operation                                           nativeGraphEdgeUniqueCommand
	Client, Ordinal, Invocations                        int
	Previous, Request                                   requestID
	Coordinator                                         uint64
	Exchanges                                           []nativeGraphEdgeUniqueExchange
	Target, Proposal, Plan, Reply, Proof, Refusal       []byte
	BodySHA256                                          [32]byte
	Decision                                            crossStatus `json:"-"`
	Role                                                string
	StartNanos, StopNanos, ElapsedNanos, AdmissionNanos int64
}
type nativeGraphEdgeUniqueExchange struct {
	Stage string
	Reply []byte
	Error string
}

func (a *nativeGraphEdgeUniqueAttempt) exchange(stage string, r processResponse, cause error) error {
	if len(a.Exchanges) >= 16 {
		return errLimit
	}
	wire, err := encodeProcessResponse(r)
	if err != nil {
		return err
	}
	e := nativeGraphEdgeUniqueExchange{Stage: stage, Reply: wire}
	if cause != nil {
		e.Error = cause.Error()
	}
	a.Exchanges = append(a.Exchanges, e)
	return nil
}

type nativeGraphEdgeUniqueJob struct {
	Operation nativeGraphEdgeUniqueCommand
	Proposal  allocationProposal
	Plan      []byte
	Attempt   int
	Previous  requestID
	Role      string
	queued    time.Time
}
type nativeGraphEdgeUniqueDriver struct {
	n       *certifiedProcessNetwork
	spec    nativeGraphCostSpec
	ctx     context.Context
	slots   chan uint64
	mu      sync.Mutex
	extra   int
	started time.Time
}

func (d *nativeGraphEdgeUniqueDriver) extraInvocation() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.extra >= nativeEdgeExtraInvocations {
		return errLimit
	}
	d.extra++
	return nil
}
func (d *nativeGraphEdgeUniqueDriver) factory(ctx context.Context, q nativeGraphEdgeUniqueCommand) (allocationProposal, error) {
	input, err := nativeGraphEdgeUniqueInput(q)
	if err != nil {
		return allocationProposal{}, err
	}
	actor := uint64(1)
	if q.Phase == "setup" && q.Fact == 2 || q.Phase == "life" && q.Fact >= 2 {
		actor = 4
	}
	r, err := nativeGraphCostExchange(ctx, d.n.all[actor], processRequest{op: processCertifiedAction, entity: certifiedGraphCost, data: input})
	if err != nil {
		return allocationProposal{}, err
	}
	p, err := decodeCertifiedProcessProposal(r.aux)
	if err != nil || p.target.semantic != durableColumnGraphSemanticContractID() {
		return allocationProposal{}, errors.Join(errInvalid, err)
	}
	return p, nil
}
func (d *nativeGraphEdgeUniqueDriver) freeze(ctx context.Context, p allocationProposal) ([]byte, error) {
	actor := uint64(1)
	if p.target.partition == 8 {
		actor = 4
	}
	r, err := nativeGraphCostExchange(ctx, d.n.all[actor], processRequest{op: processSourceCoordinate, data: append([]byte(nativeEdgeFreeze), p.wire...)})
	if err != nil {
		return nil, err
	}
	var plan nativeGraphEdgeUniquePlan
	if len(r.aux) > nativeEdgeRecipeBytes || json.Unmarshal(r.aux, &plan) != nil || !bytes.Equal(plan.Business, p.wire) || plan.BusinessSHA256 != sha256.Sum256(p.wire) || plan.ManifestSHA256 != sha256.Sum256(plan.Manifest) {
		return nil, errCorrupt
	}
	return bytes.Clone(r.aux), nil
}

// Two distinct coordinator children keep one ordinary worker each available
// for real sourceServe on the leader. Independent declared writers share this
// explicit max2 admission; no extra workers or hidden serialize-all switch.
func (d *nativeGraphEdgeUniqueDriver) submit(job nativeGraphEdgeUniqueJob, client int) (a nativeGraphEdgeUniqueAttempt, err error) {
	queued := job.queued
	if queued.IsZero() {
		queued = time.Now()
	}
	a = nativeGraphEdgeUniqueAttempt{Family: nativeGraphEdgeUniqueFamily, Engine: "native-raft", Operation: job.Operation, Client: client, Ordinal: job.Attempt, Previous: job.Previous}
	a.Role = job.Role
	if a.Role == "" {
		a.Role = job.Operation.Phase
	}
	defer func() {
		ended := time.Now()
		a.StartNanos = queued.Sub(d.started).Nanoseconds()
		a.StopNanos = ended.Sub(d.started).Nanoseconds()
		a.ElapsedNanos = ended.Sub(queued).Nanoseconds()
		if err != nil {
			a.Error = err.Error()
			if a.Classification == "" {
				a.Classification = "pending-or-unavailable"
			}
		}
	}()
	ctx, cancel := context.WithCancel(d.ctx)
	defer cancel()
	var actor uint64
	select {
	case actor = <-d.slots:
	case <-ctx.Done():
		return a, ctx.Err()
	}
	slot := actor
	a.AdmissionNanos = time.Since(queued).Nanoseconds()
	defer func() { d.slots <- slot }()
	p := job.Proposal
	if len(p.wire) == 0 {
		p, err = d.factory(ctx, job.Operation)
		if err != nil {
			return a, err
		}
	}
	if p.target.partition == 8 {
		actor = 4
	}
	a.Coordinator = actor
	a.Target, err = encodeAllocationScope(p.target)
	if err != nil {
		return a, err
	}
	a.Proposal = bytes.Clone(p.wire)
	a.BodySHA256 = sha256.Sum256(p.wire)
	command, err := decodePartitionGraphCommand(p.wire, defaultMaterializerLimits())
	if err != nil {
		return a, err
	}
	ordinary := command
	ordinary.version = 0
	business, err := encodePartitionGraphCommand(ordinary, defaultMaterializerLimits())
	if err != nil {
		return a, err
	}
	a.Request = command.request.id
	a.Plan = bytes.Clone(job.Plan)
	if len(job.Plan) != 0 {
		for invocation := 0; invocation < 8; invocation++ {
			if invocation > 0 {
				if err := d.extraInvocation(); err != nil {
					return a, errors.Join(errCrossProcessPending, err)
				}
			}
			a.Invocations++
			r, e := nativeGraphCostExchange(ctx, d.n.all[actor], processRequest{op: processSourceCoordinate, data: append([]byte(nativeEdgeRegister), job.Plan...)})
			if receiptErr := a.exchange("register", r, e); receiptErr != nil {
				return a, receiptErr
			}
			if e == nil {
				status, de := decodeCrossStatus(r.aux)
				var plan nativeGraphEdgeUniquePlan
				if de != nil || json.Unmarshal(job.Plan, &plan) != nil || status.tx != crossTransactionID(command.request.ns, command.request.id) || status.digest != plan.ManifestSHA256 {
					return a, errCorrupt
				}
				break
			}
			if !nativeGraphCostRetrySame(e) || invocation == 7 {
				return a, e
			}
		}
	}
	for invocation := 0; a.Invocations < 8; invocation++ {
		if invocation > 0 {
			if err := d.extraInvocation(); err != nil {
				return a, errors.Join(errCrossProcessPending, err)
			}
		}
		a.Invocations++
		r, callErr := nativeGraphCostExchange(ctx, d.n.all[actor], processRequest{op: processSourceCoordinate, data: p.wire})
		if receiptErr := a.exchange("run", r, callErr); receiptErr != nil {
			return a, receiptErr
		}
		a.Reply, a.Proof = bytes.Clone(r.aux), bytes.Clone(r.changes)
		if bytes.HasPrefix(r.changes, []byte(nativeEdgeRefusal)) {
			kind, de := nativeGraphEdgeUniqueDecodeRefusal(r.changes, p.wire, p.target)
			if de != nil || callErr == nil {
				return a, errors.Join(errCorrupt, de)
			}
			a.Refusal = bytes.Clone(r.changes)
			a.Classification = "pre-prepare-unique"
			if kind == 2 {
				a.Classification = "pre-prepare-endpoint-life"
			}
			return a, nil
		}
		result, de := decodeGraphProcessCoordinatorResult(r)
		if callErr != nil || de != nil {
			joined := errors.Join(callErr, de)
			if nativeGraphCostRetrySame(joined) && invocation < 7 {
				continue
			}
			return a, joined
		}
		if result.resume.scope != p.target || result.resume.request != command.request.id || result.resume.business != sha256.Sum256(business) || result.decision.phase != crossDecide || result.decision.tx != crossTransactionID(command.request.ns, command.request.id) {
			return a, errCorrupt
		}
		var proof nativeGraphCostDecisionProof
		if err := json.Unmarshal(r.changes, &proof); err != nil {
			return a, err
		}
		original, e := decodeCrossStatus(proof.Outcome)
		if e != nil || original != result.decision || proof.CDC == nil || len(proof.CDC) != 0 {
			return a, errors.Join(errCorrupt, e)
		}
		arena, e := graphstate.NewOutputBudget(defaultMaterializerLimits().outputBytes)
		if e != nil {
			return a, e
		}
		manifest, e := decodeCrossManifest(proof.Manifest, defaultMaterializerLimits(), arena)
		if e != nil || manifest.digest != original.digest || manifest.tx != original.tx || manifest.request.id != command.request.id {
			return a, errors.Join(errCorrupt, e)
		}
		planned := partitionGraphCommand{configuration: manifest.configuration, declaration: manifest.declaration, routing: manifest.routing, request: manifest.request}
		planned.request.revision = ordinary.request.revision
		plannedBytes, e := encodePartitionGraphCommand(planned, defaultMaterializerLimits())
		if e != nil || !bytes.Equal(plannedBytes, business) {
			return a, errors.Join(errCorrupt, e)
		}
		if len(job.Plan) != 0 {
			var frozen nativeGraphEdgeUniquePlan
			if json.Unmarshal(job.Plan, &frozen) != nil || !bytes.Equal(frozen.Manifest, proof.Manifest) {
				return a, errCorrupt
			}
		}
		a.Decision = original
		a.Classification = "abort"
		if original.commit {
			a.Classification = "commit"
		}
		return a, nil
	}
	return a, errCrossProcessPending
}

func (d *nativeGraphEdgeUniqueDriver) window(jobs []nativeGraphEdgeUniqueJob, emit func(nativeGraphEdgeUniqueAttempt) error) ([]nativeGraphEdgeUniqueAttempt, error) {
	if len(jobs) < 1 || len(jobs) > 32 || emit == nil || d.spec.Writers < 1 {
		return nil, errInvalid
	}
	type indexed struct {
		index int
		job   nativeGraphEdgeUniqueJob
	}
	type result struct {
		index   int
		attempt nativeGraphEdgeUniqueAttempt
		err     error
	}
	queues := make([]chan indexed, d.spec.Writers)
	results := make(chan result, 32)
	var workers sync.WaitGroup
	for client := range queues {
		queues[client] = make(chan indexed, 1)
		workers.Go(func() {
			for item := range queues[client] {
				a, e := d.submit(item.job, client)
				results <- result{item.index, a, e}
			}
		})
	}
	for index, job := range jobs {
		job.queued = time.Now()
		queues[(job.Operation.Ordinal-1)%uint64(len(queues))] <- indexed{index, job}
	}
	for _, queue := range queues {
		close(queue)
	}
	actual := make([]nativeGraphEdgeUniqueAttempt, len(jobs))
	var failed error
	for range jobs {
		got := <-results
		actual[got.index] = got.attempt
		failed = errors.Join(failed, got.err, emit(got.attempt))
	}
	workers.Wait()
	return actual, failed
}

// Priority is entered ONLY after the complete max2-Run wave has returned real
// terminal ABORTs. One genuinely new request then plans against the settled
// state; failure stays failure, never an invented successful retry. Unknowns
// cannot reach this seam. Per-job at most one such new original is admitted.
func (d *nativeGraphEdgeUniqueDriver) priority(job nativeGraphEdgeUniqueJob, prior nativeGraphEdgeUniqueAttempt, emit func(nativeGraphEdgeUniqueAttempt) error) (nativeGraphEdgeUniqueAttempt, error) {
	if prior.Classification != "abort" || prior.Decision.commit || prior.Decision.phase != crossDecide || prior.Request == (requestID{}) {
		return nativeGraphEdgeUniqueAttempt{}, errInvalid
	}
	job.Plan = nil
	job.Proposal = allocationProposal{}
	job.Attempt = prior.Ordinal + 1
	job.Previous = prior.Request
	job.queued = time.Time{}
	a, err := d.submit(job, prior.Client)
	err = errors.Join(err, emit(a))
	if err == nil && a.Classification != "commit" {
		err = errInvalid
	}
	return a, err
}

func newNativeGraphEdgeUniqueNetwork(t *testing.T, spec nativeGraphCostSpec, deadline time.Time, seconds uint64, stream *json.Encoder) *certifiedProcessNetwork {
	t.Helper()
	if nativeGraphEdgeUniqueValidate(spec) != nil || seconds == 0 || seconds > 365*24*60*60 || time.Until(deadline) <= 0 {
		t.Fatal("fresh native family admission")
	}
	wire, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	dir, err := os.MkdirTemp(os.Getenv("RHO_GRAPH_PROCESS_ARTIFACT"), "rho-native-edge-unique-")
	if err != nil {
		t.Fatal(err)
	}
	n := &certifiedProcessNetwork{all: make(map[uint64]*graphProcess), dir: dir, leaders: [2]uint64{1, 4}}
	for id := uint64(1); id <= 6; id++ {
		n.all[id] = startGraphProcess(t, filepath.Join(dir, fmt.Sprint(id)), id, "RHO_GRAPH_CERTIFIED_PROCESS=1", "RHO_GRAPH_SOURCE_PROCESS=1", "RHO_GRAPH_EDGE_UNIQUE_PROCESS=1", "RHO_GRAPH_NATIVE_WORKLOAD=", "RHO_GRAPH_COST_SPEC="+string(wire), "RHO_GRAPH_WORKLOAD_RUN="+os.Getenv("RHO_GRAPH_WORKLOAD_RUN"), "RHO_GRAPH_WORKLOAD_SECONDS="+strconv.FormatUint(seconds, 10), "RHO_GRAPH_WORKLOAD_DEADLINE="+strconv.FormatInt(deadline.UnixNano(), 10))
	}
	attachNativeGraphEdgeUniqueNetwork(t, n, spec, deadline)
	t.Cleanup(func() {
		for _, p := range n.all {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			err := p.shutdown(ctx, t.Failed())
			cancel()
			if err != nil && !t.Failed() {
				t.Error(err)
			}
		}
	})
	for g, leader := range n.leaders {
		n.groups[g].event(t, leader, processRequest{op: processCampaign})
	}
	nativeColumnProcessAction(t, n, 1, certifiedInitialize, 3, nil)
	n.read(t, 1, observeConfiguration, 0)
	nativeColumnProcessAction(t, n, 1, certifiedInitialize, 8, nil)
	block, err := nativeGraphCostBlockSize(spec)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct{ actor, recipient, partition uint64 }{{1, 1, 3}, {4, 2, 8}} {
		n.read(t, r.actor, observeConfiguration, 0)
		nativeColumnProcessAction(t, n, r.actor, certifiedBeginRecipient, r.recipient, nil)
		n.read(t, 1, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, 3, []byte{byte(fenceDeclaredRecipient)})
		if r.partition != 3 {
			nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, r.partition, []byte{byte(fenceDeclaredRecipient)})
		}
		n.read(t, r.actor, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, r.actor, certifiedRecipientTransition, 3, []byte{byte(acknowledgeDeclaredHome)})
		n.read(t, 1, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, 3, []byte{byte(activateDeclaredRecipient)})
		n.read(t, 1, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, 1, certifiedRecipientTransition, r.partition, []byte{byte(publishDeclaredRecipient)})
		n.read(t, r.actor, observeRecipient, r.recipient)
		nativeColumnProcessAction(t, n, r.actor, certifiedOpenRecipient, 0, nil)
		grants := uint64(1)
		if r.partition == 3 {
			grants = (spec.homeCount() + block - 1) / block
		}
		for sequence := uint64(1); sequence <= grants; sequence++ {
			_, count, e := nativeGraphEdgeUniqueGrantPlan(spec, block, r.partition, sequence)
			if e != nil {
				t.Fatal(e)
			}
			n.read(t, 1, observeRecipient, r.recipient)
			n.read(t, 1, observeAllocator, 0)
			input := graphE17GrantInput(sequence, count)
			nativeContentionGrantAction(t, n, certifiedReserve, 0, input, r.partition, sequence, count, stream)
			n.group(1).event(t, 1, processRequest{op: processCertifiedRead, entity: uint64(observeGrant), data: input})
			if !n.all[1].call(t, processRequest{op: processStateOp}).ready {
				t.Fatal("actual fresh grant ReadIndex incomplete")
			}
			nativeContentionGrantAction(t, n, certifiedInstallGrant, r.partition, nil, r.partition, sequence, count, stream)
		}
	}
	return n
}

type nativeGraphEdgeUniqueRunBinding struct {
	Version                            uint8
	Family, Run                        string
	Spec                               nativeGraphCostSpec
	Transport                          string
	BudgetSeconds, CausalMinimumMillis uint64
	DeadlineUnixNanos                  int64
	OwnedTransport                     *nativeWorkloadTransportSpec `json:",omitzero"`
	Graph                              [16]byte                     `json:",omitzero"`
	Topology                           uint64                       `json:",omitzero"`
	Declaration                        [32]byte                     `json:",omitzero"`
	BinarySHA256                       string                       `json:",omitempty"`
}

func nativeGraphEdgeUniqueAdmission(spec nativeGraphCostSpec) (binding nativeGraphEdgeUniqueRunBinding, digest [32]byte, err error) {
	if nativeGraphEdgeUniqueValidate(spec) != nil {
		return binding, digest, errInvalid
	}
	deadline, err := nativeGraphEdgeUniqueDeadlineFromEnv()
	if err != nil {
		return binding, digest, err
	}
	seconds, _ := strconv.ParseUint(os.Getenv("RHO_GRAPH_WORKLOAD_SECONDS"), 10, 32)
	run := os.Getenv("RHO_GRAPH_WORKLOAD_RUN")
	nonce, e := hex.DecodeString(run)
	if e != nil || len(nonce) != 16 || hex.EncodeToString(nonce) != run || bytes.Equal(nonce, make([]byte, 16)) {
		return binding, digest, errInvalid
	}
	enabled, e := nativeGraphEdgeUniqueTransportEnabled()
	if e != nil {
		return binding, digest, e
	}
	transport := os.Getenv("RHO_GRAPH_WORKLOAD_TRANSPORT")
	if !enabled && transport != "" && transport != "local" {
		return binding, digest, errInvalid
	}
	binding = nativeGraphEdgeUniqueRunBinding{Version: 1, Family: nativeGraphEdgeUniqueFamily, Run: run, Spec: spec, Transport: "local", BudgetSeconds: seconds, DeadlineUnixNanos: deadline.UnixNano()}
	if enabled {
		binding, e = nativeGraphEdgeUniqueOwnedAdmission(binding)
		if e != nil {
			return binding, digest, e
		}
	}
	body, e := json.Marshal(binding)
	if e != nil {
		return binding, digest, e
	}
	return binding, sha256.Sum256(body), nil
}
