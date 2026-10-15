package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
)

// A resume identifies the original source request. It carries no observation,
// placement, completed page, or decision authority.
type rangeMoveResume struct {
	scope   allocationScope
	request requestID
	move    [16]byte
	intent  [32]byte
}

type rangeMoveResult struct {
	resume         rangeMoveResume
	source, target rangeMoveStatus
	decision       rangeMoveStatus
	committed      bool
}

type rangeMoveRun struct {
	resume          rangeMoveResume
	intent          []byte
	busy, attempted bool
}

type rangeMoveCoordinator struct {
	mu     sync.Mutex
	base   *crossProcessCoordinator
	limits materializerLimits
	runs   map[[16]byte]*rangeMoveRun
}

func newRangeMoveCoordinator(h *allocationHost, transports map[uint64]graphPeerWireTransport) (*rangeMoveCoordinator, error) {
	if h == nil || h.machine == nil || !h.machine.rangeMoves() {
		return nil, errInvalid
	}
	base, err := newCrossProcessCoordinator(h, transports)
	if err != nil {
		return nil, err
	}
	return newRangeMoveCoordinatorOn(base)
}

// Sharing an existing authenticated connection owner preserves its correlation
// sequences; invocation budgets remain independently bounded operation ledgers.
func newRangeMoveCoordinatorOn(base *crossProcessCoordinator) (*rangeMoveCoordinator, error) {
	if base == nil || base.host == nil || base.host.machine == nil || !base.host.machine.rangeMoves() {
		return nil, errInvalid
	}
	return &rangeMoveCoordinator{base: base, limits: base.host.machine.limits, runs: make(map[[16]byte]*rangeMoveRun)}, nil
}

func (c *rangeMoveCoordinator) valid() bool {
	return c != nil && c.base != nil && c.base.host != nil && c.base.host.machine != nil && c.base.host.machine.rangeMoves()
}

// Run retains exact unknown-submission bytes. A changed target, publication,
// epoch, or bound cannot reinterpret a source-qualified retry.
func (c *rangeMoveCoordinator) Run(ctx context.Context, intent rangeMoveIntent) (rangeMoveResult, error) {
	if !c.valid() || ctx == nil || !intent.valid() || intent.source != c.base.host.machine.scope() {
		return rangeMoveResult{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return rangeMoveResult{}, err
	}
	wire, err := encodeRangeMoveIntent(intent)
	if err != nil {
		return rangeMoveResult{}, err
	}
	resume := rangeMoveResume{scope: intent.source, request: intent.id, move: rangeMoveID(namespace{intent.source.graph, intent.source.partition}, intent.id), intent: sha256.Sum256(wire)}
	return c.execute(ctx, resume, wire)
}

// Resume reconstructs progress only from authenticated completed native reads.
// Absent durable registration never supplies replacement intent bytes.
func (c *rangeMoveCoordinator) Resume(ctx context.Context, resume rangeMoveResume) (rangeMoveResult, error) {
	if !c.valid() || ctx == nil || resume.scope != c.base.host.machine.scope() || resume.request == (requestID{}) || resume.intent == ([32]byte{}) || resume.move != rangeMoveID(namespace{resume.scope.graph, resume.scope.partition}, resume.request) {
		return rangeMoveResult{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return rangeMoveResult{}, err
	}
	return c.execute(ctx, resume, nil)
}

func (c *rangeMoveCoordinator) claim(resume rangeMoveResume, wire []byte) (*rangeMoveRun, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	run := c.runs[resume.move]
	if run != nil {
		if run.resume != resume || len(wire) != 0 && len(run.intent) != 0 && !bytes.Equal(run.intent, wire) {
			return nil, errInvalid
		}
		if run.busy {
			return nil, errRangeMovePending
		}
	} else {
		if len(c.runs) >= 2 {
			return nil, errLimit
		}
		run = &rangeMoveRun{resume: resume}
		c.runs[resume.move] = run
	}
	if len(run.intent) == 0 && len(wire) != 0 {
		run.intent = owned(wire)
	}
	run.busy = true
	return run, nil
}

// protectAbort prepays one source ABORT exchange before any fence. The pool
// consumes the same operation ledger; it cannot reset allowances after a page
// exhausts normal work. Target undo continues from its durable reverse cursor.
func rangeMoveProtectAbort(b *graphRemoteBudget) (*graphRemoteBudget, *graphstate.OutputBudget, error) {
	if b == nil || b.output == nil {
		return nil, nil, errInvalid
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	const wire, output = 8192, 64 << 10
	if b.calls >= b.maxCalls || wire > b.maxBytes-b.bytes || output > b.output.Remaining() {
		return nil, nil, errLimit
	}
	arena, err := graphstate.NewOutputBudget(output)
	if err != nil {
		return nil, nil, err
	}
	if err := b.output.Reserve(output); err != nil {
		return nil, nil, outputBudgetFailure(err)
	}
	b.calls++
	b.bytes += wire
	return &graphRemoteBudget{maxCalls: 1, maxBytes: wire, output: arena}, arena, nil
}

type rangeMoveInvocation struct {
	coordinator *rangeMoveCoordinator
	run         *rangeMoveRun
	output      *graphstate.OutputBudget
	budget      *graphRemoteBudget
	abortBudget *graphRemoteBudget
	abortOutput *graphstate.OutputBudget
	endpoints   map[uint64]*graphAuthenticatedEndpoint
	intent      rangeMoveIntent
	wire        []byte
	result      rangeMoveResult
	changed     uint64
	frozen      *remoteGraphSource
}

func (c *rangeMoveCoordinator) execute(ctx context.Context, resume rangeMoveResume, wire []byte) (result rangeMoveResult, err error) {
	result.resume = resume
	run, err := c.claim(resume, wire)
	if err != nil {
		return result, err
	}
	terminal := false
	defer func() {
		c.mu.Lock()
		run.busy = false
		if terminal || !run.attempted {
			delete(c.runs, resume.move)
		}
		c.mu.Unlock()
	}()
	l := c.limits
	output, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return result, err
	}
	if err := output.Reserve(4096 + 8*len(run.intent)); err != nil {
		return result, outputBudgetFailure(err)
	}
	op := rangeMoveInvocation{coordinator: c, run: run, output: output, budget: newGraphRemoteBudget(l, output), result: result}
	defer func() {
		if op.frozen != nil {
			err = errors.Join(err, op.frozen.Close())
		}
	}()
	op.abortBudget, op.abortOutput, err = rangeMoveProtectAbort(op.budget)
	if err != nil {
		return result, err
	}
	op.endpoints, err = c.base.endpoints(op.budget)
	if err != nil {
		return result, err
	}
	source, err := op.read(ctx, resume.scope.partition)
	if err != nil {
		return result, errors.Join(errRangeMovePending, err)
	}
	if source.observation.status.phase != 0 {
		if sha256.Sum256(source.observation.intent) != resume.intent || len(run.intent) != 0 && !bytes.Equal(run.intent, source.observation.intent) {
			return result, errInvalid
		}
		op.wire = source.observation.intent
		run.attempted = true
	} else {
		op.wire = run.intent
	}
	if len(op.wire) == 0 {
		return result, errRangeMovePending
	}
	op.intent, err = decodeRangeMoveIntent(op.wire)
	if err != nil || op.intent.source != resume.scope || op.intent.id != resume.request || sha256.Sum256(op.wire) != resume.intent {
		return result, errors.Join(errInvalid, err)
	}
	target, err := op.read(ctx, op.intent.target.partition)
	if err != nil {
		return op.result, errors.Join(errRangeMovePending, err)
	}
	// Work/call/output ledgers bound a complete invocation, including every
	// page and read. The immutable cap additionally bounds durable total steps.
	for range 8*op.intent.maxPages + 64 {
		if err := ctx.Err(); err != nil {
			return op.result, errors.Join(errRangeMovePending, err)
		}
		op.result.source, op.result.target = source.observation.status, target.observation.status
		op.result.decision = source.observation.decision
		done, err := op.advance(ctx, source, target)
		if done {
			terminal = true
			return op.result, err
		}
		if err != nil {
			return op.result, errors.Join(errRangeMovePending, err)
		}
		// The opposite participant's completed proof owns immutable capture,
		// manifest or decision data. Only the advanced participant needs a new
		// quorum observation; source page data is still obtained by real RPC.
		if op.changed == op.intent.source.partition {
			source, err = op.read(ctx, op.changed)
		} else {
			target, err = op.read(ctx, op.changed)
		}
		if err != nil {
			return op.result, errors.Join(errRangeMovePending, err)
		}
	}
	return op.result, errors.Join(errRangeMovePending, errLimit)
}

func (op *rangeMoveInvocation) read(ctx context.Context, partition uint64) (rangeMoveAuthority, error) {
	endpoint := op.endpoints[partition]
	if endpoint == nil {
		return rangeMoveAuthority{}, errInvalid
	}
	p, err := endpoint.move(ctx, op.run.resume.move)
	if err != nil {
		return rangeMoveAuthority{}, err
	}
	if !p.validSeal() || p.scope != endpoint.peer.scope || p.question.move != op.run.resume.move {
		return rangeMoveAuthority{}, errInvalid
	}
	if p.observation.status.phase != 0 && (p.observation.status.move != op.run.resume.move || p.observation.status.intent != op.run.resume.intent || !bytes.Equal(p.observation.intent, op.wire) && len(op.wire) != 0) {
		return rangeMoveAuthority{}, errInvalid
	}
	if partition == op.run.resume.scope.partition && p.observation.status.phase >= rangeMoveDecide && p.observation.status.phase <= rangeMoveFinish {
		d := p.observation.decision
		if d.phase != rangeMoveDecide || d.result != rangeMoveApplied || d.round == 0 || d.move != op.run.resume.move || d.intent != op.run.resume.intent || d.ns != (namespace{p.scope.graph, partition}) {
			return rangeMoveAuthority{}, errCorrupt
		}
	}
	return p, nil
}

func (op *rangeMoveInvocation) submit(ctx context.Context, partition uint64, phase rangeMovePhase, sequence uint64, data []byte, evidence []rangeMoveAuthority) error {
	proposal, err := op.coordinator.base.host.prepareRangeMoveCommand(op.wire, phase, partition, sequence, data, evidence, op.output)
	if err != nil {
		return err
	}
	// Set before transport: a lost reply can follow actual durable installation.
	op.run.attempted = true
	op.changed = partition
	status, err := op.endpoints[partition].submitMove(ctx, proposal)
	if status.result == rangeMoveLimit {
		return errors.Join(errRangeMoveIntrinsicCapacity, err)
	}
	return err
}

func (op *rangeMoveInvocation) abort(ctx context.Context) error {
	proposal, err := op.coordinator.base.host.prepareRangeMoveCommand(op.wire, rangeMoveAbort, op.intent.source.partition, 0, nil, nil, op.abortOutput)
	if err != nil {
		return err
	}
	endpoint := *op.endpoints[op.intent.source.partition]
	endpoint.budget = op.abortBudget
	op.run.attempted = true
	op.changed = op.intent.source.partition
	_, err = endpoint.submitMove(ctx, proposal)
	return err
}

// This cause is minted only from an authenticated native capacity outcome.
// Ordinary RPC/representation/physical work exhaustion remains resumable.
var errRangeMoveIntrinsicCapacity = errors.New("graphapply: native range move capacity refusal")

func (op *rangeMoveInvocation) step(ctx context.Context, partition uint64, phase rangeMovePhase, sequence uint64, data []byte, evidence []rangeMoveAuthority, source rangeMoveAuthority) error {
	err := op.submit(ctx, partition, phase, sequence, data, evidence)
	if phase == rangeMoveFence && source.observation.status.phase == 0 && errors.Is(err, errRangeMoveIntrinsicCapacity) {
		// The native phase persisted an exact no-effect terminal refusal. Read it
		// through the authenticated endpoint; do not manufacture an ABORT here.
		return nil
	}
	if err != nil && source.observation.status.phase > 0 && source.observation.status.phase < rangeMoveDecide && errors.Is(err, errRangeMoveIntrinsicCapacity) {
		// Native ABORT validates the immutable decision locally. A concurrent
		// completed COMMIT refuses ABORT; transport failure cannot become a vote.
		return op.abort(ctx)
	}
	return err
}

func (op *rangeMoveInvocation) progress(p rangeMoveAuthority) (rangeMoveProgress, error) {
	if len(p.observation.progress) == 0 {
		return rangeMoveProgress{}, errCorrupt
	}
	return decodeRangeMoveProgress(p.observation.progress, op.output)
}

func (op *rangeMoveInvocation) advance(ctx context.Context, source, target rangeMoveAuthority) (bool, error) {
	s, t := source.observation.status, target.observation.status
	if s.result == rangeMoveMismatch {
		return true, errInvalid
	}
	from, to := op.intent.source.partition, op.intent.target.partition
	if s.phase == rangeMoveFinish && t.phase == rangeMoveFinish {
		op.result.committed = true
		return true, nil
	}
	if s.phase == rangeMoveAbort {
		if s.sequence != 0 || source.observation.decision != (rangeMoveStatus{}) || t.phase >= rangeMoveDecide && t.phase != rangeMoveAbort {
			return false, errCorrupt
		}
		if t.phase == 0 {
			if s.result == rangeMoveLimit {
				return true, errLimit
			}
			return false, op.submit(ctx, to, rangeMoveAbort, 0, nil, []rangeMoveAuthority{source})
		}
		if t.phase == rangeMoveAbort && t.sequence == 0 {
			return true, errLimit
		}
		sequence := uint64(0)
		if t.phase == rangeMoveImport || t.phase == rangeMoveSeal || t.phase == rangeMoveAbort {
			progress, err := op.progress(target)
			if err != nil {
				return false, err
			}
			if t.phase != rangeMoveAbort && t.sequence > 0 {
				sequence = 1
			} else if progress.undoNext < progress.undoTotal {
				sequence = progress.undoNext + 1
			}
		}
		return false, op.submit(ctx, to, rangeMoveAbort, sequence, nil, []rangeMoveAuthority{source})
	}
	if t.phase == rangeMoveAbort {
		return false, errCorrupt
	}
	if s.phase == 0 {
		return false, op.step(ctx, from, rangeMoveFence, 0, nil, nil, source)
	}
	if t.phase == 0 {
		return false, op.step(ctx, to, rangeMoveFence, 0, nil, []rangeMoveAuthority{source}, source)
	}
	if s.phase == rangeMoveFence {
		return false, op.step(ctx, from, rangeMoveCapture, 0, nil, nil, source)
	}
	if t.phase == rangeMoveFence {
		return false, op.step(ctx, to, rangeMoveCapture, 0, nil, nil, source)
	}
	if s.phase == rangeMoveCapture {
		return false, op.describe(ctx, source, target)
	}
	if s.phase == rangeMoveDescribe {
		progress, err := op.progress(source)
		if err != nil {
			return false, err
		}
		if !progress.transferComplete() {
			if s.sequence >= uint64(op.intent.maxPages) { // #nosec G115 -- operation intent validates maxPages1..1024 before continuation.
				return false, op.abort(ctx)
			}
			return false, op.step(ctx, from, rangeMoveDescribe, s.sequence+1, nil, nil, source)
		}
		if t.phase == rangeMoveCapture {
			if rangeMoveSpans(op.intent.source.semantic) {
				actual, e := op.endpoints[from].moveQuestion(ctx, rangeMoveQuery{move: op.run.resume.move, lineage: true})
				if e != nil {
					return false, e
				}
				hydrated, e := op.progress(actual)
				if e != nil {
					return false, e
				}
				rows, size, digest := hydrated.transferSummary()
				if !actual.validSeal() || actual.observation.status.intent != op.run.resume.intent || actual.observation.status.rows != rows || actual.observation.status.size != size || actual.observation.status.digest != digest || actual.observation.status != source.observation.status || !hydrated.inlineRetention {
					return false, errInvalid
				}
				progress = hydrated
			}
			wire, err := encodeRangeMoveProgress(progress, op.output)
			if err != nil {
				return false, err
			}
			return false, op.step(ctx, to, rangeMoveImport, 0, wire, []rangeMoveAuthority{source, target}, source)
		}
		if t.phase == rangeMoveImport {
			return false, op.importPage(ctx, source, target, progress)
		}
		if t.phase == rangeMoveSeal {
			return false, op.step(ctx, from, rangeMoveDecide, 0, nil, []rangeMoveAuthority{source, target}, source)
		}
		return false, errCorrupt
	}
	if s.phase == rangeMoveDecide || s.phase == rangeMoveActivate || s.phase == rangeMoveFinish {
		// Activation reaches the target before the source release. Any retained
		// durable COMMIT remains irrevocable across unavailable replies.
		if t.phase == rangeMoveSeal {
			return false, op.submit(ctx, to, rangeMoveActivate, 0, nil, []rangeMoveAuthority{source})
		}
		if s.phase == rangeMoveDecide && t.phase >= rangeMoveActivate && t.phase <= rangeMoveFinish {
			return false, op.submit(ctx, from, rangeMoveActivate, 0, nil, []rangeMoveAuthority{source})
		}
		if s.phase == rangeMoveActivate && t.phase == rangeMoveActivate {
			return false, op.submit(ctx, to, rangeMoveFinish, 0, nil, []rangeMoveAuthority{source, target})
		}
		if s.phase == rangeMoveActivate && t.phase == rangeMoveFinish {
			return false, op.submit(ctx, from, rangeMoveFinish, 0, nil, []rangeMoveAuthority{source, target})
		}
	}
	return false, errCorrupt
}

func (op *rangeMoveInvocation) describe(ctx context.Context, source, target rangeMoveAuthority) (err error) {
	from, to := op.intent.source.partition, op.intent.target.partition
	left, err := op.endpoints[from].openMoveSource(ctx, op.run.resume.move)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, left.Close()) }()
	right, err := op.endpoints[to].openMoveSource(ctx, op.run.resume.move)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, right.Close()) }()
	leftConfig, err := left.move.configuration.digest()
	if err != nil {
		return err
	}
	rightConfig, err := right.move.configuration.digest()
	if err != nil {
		return err
	}
	if leftConfig != op.intent.publication.Configuration || rightConfig != op.intent.publication.Configuration {
		return errInvalid
	}
	p := op.intent.publication
	spec := graphstore.RangeMoveSpec{Publication: p, From: graphstore.RangeOwnership{Graph: p.Graph, RangeID: p.RangeID, Partition: from, Epoch: op.intent.epoch, Configuration: p.Configuration}, To: graphstore.RangeOwnership{Graph: p.Graph, RangeID: p.RangeID, Partition: to, Epoch: op.intent.epoch + 1, Configuration: p.Configuration}}
	op.budget.mu.Lock()
	b := graphstore.OwnershipBudget{SourceRows: op.budget.maxRows - op.budget.rows, SourceBytes: op.budget.maxBytes - op.budget.bytes, OutputBytes: op.output.Remaining()}
	op.budget.mu.Unlock()
	l := op.coordinator.base.host.machine.limits
	descriptor, _, err := graphstore.PlanRangeMove(ctx, left, right, op.coordinator.base.host.machine.declaration, left.move.configuration.defaultAxis, p.Configuration, spec, l.catalog, l.graph, b, op.output)
	if err != nil {
		return err
	}
	wire, err := graphstore.EncodeRangeMoveDescriptor(descriptor, op.output)
	if err != nil {
		return err
	}
	return op.step(ctx, from, rangeMoveDescribe, 0, wire, []rangeMoveAuthority{target}, source)
}

func (op *rangeMoveInvocation) importPage(ctx context.Context, source, target rangeMoveAuthority, manifest rangeMoveProgress) error {
	progress, err := op.progress(target)
	if err != nil || progress.imported == nil || progress.descriptor != manifest.descriptor {
		return errors.Join(errCorrupt, err)
	}
	state := progress.imported
	if state.Complete && progress.replay.complete {
		return op.step(ctx, op.intent.target.partition, rangeMoveSeal, 0, nil, []rangeMoveAuthority{source}, source)
	}
	if target.observation.status.sequence >= uint64(op.intent.maxPages) { // #nosec G115 -- operation intent validates maxPages1..1024 before continuation.
		return op.abort(ctx)
	}
	next := target.observation.status.sequence + 1
	if !state.PreparationComplete {
		return op.step(ctx, op.intent.target.partition, rangeMoveImport, next, []byte{0}, nil, source)
	}
	replayPages := progress.replay.sequence
	manifestPages := manifest.replay.sequence
	if rangeMoveSpans(op.intent.source.semantic) {
		replayPages = progress.replayPages
		manifestPages = manifest.replayPages
	}
	graphPages := state.Sequence
	if rangeMoveGraphBatches(op.intent.source.semantic) {
		graphPages = progress.graphApplied
	}
	pageSequence := graphPages + replayPages + 1
	if pageSequence > manifest.graphPages+manifestPages {
		return errCorrupt
	}

	if op.frozen == nil {
		op.frozen, err = op.endpoints[op.intent.source.partition].openMoveSource(ctx, op.run.resume.move)
		if err != nil {
			return err
		}
	}
	data, err := op.frozen.moveFrozenPage(ctx, pageSequence)
	if err != nil {
		return err
	}

	return op.step(ctx, op.intent.target.partition, rangeMoveImport, next, data, []rangeMoveAuthority{source}, source)
}
