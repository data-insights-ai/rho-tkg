package graphapply

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"slices"
	"sync"
	"time"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

type retainedFeedToken struct {
	definition []byte
	pin        retainedPinToken
}
type retainedFeedOwner struct {
	mu         sync.Mutex
	self       *retainedFeedOwner
	publisher  *retainedPinPublisher
	pin        *retainedPinRequest
	definition retainedFeedDefinition
	wire       []byte
	token      retainedPinToken
	closed     bool
}
type retainedFeedDelivery struct {
	page   retainedFeedPage
	wire   []byte
	digest [32]byte
	// Resolved is a canonical GCD5 catalog bundle (no component effects), with
	// exact schema definitions separately owned. Native root/read witnesses in
	// page.payloads authenticate it; these bytes alone grant no retention rights.
	resolved graphChanges
	schemas  []graphstate.PropertyDefinition
	replay   bool
}

// Bootstrap reads and native issuance share the saved API deadline. It is not
// replicated lease authority, and it never constrains already-funded terminal
// cleanup. Resume does not renew this lifetime or authenticate recipe bytes.
func retainedFeedBootstrapContext(ctx context.Context, p *retainedPinPublisher, policy retainedFeedPolicy) (context.Context, context.CancelFunc, error) {
	if ctx == nil || !p.valid() || !policy.valid() {
		return nil, nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	l := p.coordinator.host.machine.limits
	if !policy.fits(l) || policy.pageBytes > p.coordinator.host.machine.store.ApplicationLimits().MaxValueBytes {
		return nil, nil, errLimit
	}
	deadline := time.Unix(0, policy.deadlineUnixNanos)
	if !time.Now().Before(deadline) {
		return nil, nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	return ctx, cancel, nil
}

// Begin captures the snapshot and allocates its feed coordinates under ONE pin.
// Persist Original before Publish if the embedding needs producer recovery.
// It does not claim publication/issuance merely because this recipe exists.
func beginRetainedFeed(ctx context.Context, p *retainedPinPublisher, id requestID, request graphReadRequest, expires uint64, policy retainedFeedPolicy) (*retainedFeedOwner, error) {
	if p == nil || !p.valid() || !retainedFeedAgreement(p.coordinator.host.machine.SemanticContractID()) || !policy.valid() {
		return nil, errInvalid
	}
	ctx, cancel, err := retainedFeedBootstrapContext(ctx, p, policy)
	if err != nil {
		return nil, err
	}
	defer cancel()
	pin, err := p.Begin(ctx, id, request, expires)
	if err != nil {
		return nil, err
	}
	original, err := pin.Original()
	if err != nil {
		return nil, err
	}
	d := retainedFeedDefinition{manifest: original, policy: policy}
	wire, err := encodeRetainedFeedDefinition(d)
	if err != nil {
		return nil, err
	}
	o := &retainedFeedOwner{publisher: p, pin: pin, definition: d, wire: wire}
	o.self = o
	return o, nil
}
func resumeRetainedFeed(ctx context.Context, p *retainedPinPublisher, original []byte) (*retainedFeedOwner, error) {
	if ctx == nil || p == nil || !p.valid() || !retainedFeedAgreement(p.coordinator.host.machine.SemanticContractID()) {
		return nil, errInvalid
	}
	d, err := decodeRetainedFeedDefinition(original)
	if err != nil {
		return nil, err
	}
	ctx, cancel, err := retainedFeedBootstrapContext(ctx, p, d.policy)
	if err != nil {
		return nil, err
	}
	defer cancel()
	pin, err := p.Resume(ctx, d.manifest)
	if err != nil {
		return nil, err
	}
	if err = pin.invocation.arena.Reserve(2048 + len(original)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	o := &retainedFeedOwner{publisher: p, pin: pin, definition: d, wire: owned(original)}
	o.definition.manifest = pin.manifest
	o.self = o
	return o, nil
}
func (o *retainedFeedOwner) valid() bool {
	return o != nil && o.self == o && o.publisher != nil && o.pin != nil
}

// Each external Page/ACK/Pending operation receives a fresh bounded transient
// ledger ONLY after every participant has durably spent its complete offered
// grant. The original pin invocation (and its separately prepaid cleanup pools)
// is restored afterward. This is not a reset of an OutputBudget or a refund.
// Failed admission messages are bounded observed caller/server cost, not claimed
// native work; partial/unknown installed grants remain spent. An internal fresh
// nonce prevents Resume from reusing an earlier grant to mint another ledger.
func (o *retainedFeedOwner) operation(ctx context.Context) (context.Context, func(), error) {
	ctx, restore, cancel, err := o.admitOperation(ctx)
	if err != nil {
		return nil, nil, err
	}
	return ctx, func() { restore(); cancel() }, nil
}

func (o *retainedFeedOwner) admitOperation(ctx context.Context) (context.Context, func(), context.CancelFunc, error) {
	p := o.definition.policy
	if !p.valid() || ctx == nil {
		return nil, nil, nil, errInvalid
	}
	deadline := time.Unix(0, p.deadlineUnixNanos)
	if !time.Now().Before(deadline) {
		return nil, nil, nil, context.DeadlineExceeded
	}
	ctx, cancel := context.WithDeadline(ctx, deadline)
	fail := func(err error) (context.Context, func(), context.CancelFunc, error) {
		cancel()
		return nil, nil, nil, err
	}
	l := o.publisher.coordinator.host.machine.limits
	if p.operationRows > l.sourceRows || p.operationBytes > l.sourceBytes || p.operationOutput > l.outputBytes {
		return fail(errLimit)
	}
	parts := len(o.pin.definition.parts)
	// Variable canonical command ownership plus fixed native original lookup
	// and bounded packet framing, before any operation-source read is authorized.
	grantOutput := parts * (pinLookupOutput + 32*(len(o.wire)+4096))
	grantBytes := parts * (pinLookupBytes + 8*(len(o.wire)+4096))
	grantRows := parts * (pinLookupRows + 8)
	if grantOutput > l.outputBytes || grantBytes > l.sourceBytes || grantRows > l.sourceRows {
		return fail(errLimit)
	}
	arena, err := graphstate.NewOutputBudget(grantOutput)
	if err != nil {
		return fail(err)
	}
	admission := &graphRemoteBudget{maxCalls: parts, maxBytes: grantBytes, maxRows: grantRows, output: arena}
	endpoints, err := o.publisher.coordinator.endpoints(admission)
	if err != nil {
		return fail(err)
	}
	if err = arena.Reserve(4096 + 512*parts); err != nil {
		return fail(outputBudgetFailure(err))
	}
	var attempt [16]byte
	if _, err = rand.Read(attempt[:]); err != nil {
		return fail(err)
	}
	if attempt == ([16]byte{}) {
		return fail(errInvalid)
	}
	for _, part := range o.pin.definition.parts {
		e := endpoints[part.certificate.scope.partition]
		r, err := e.submitRetainedFeed(ctx, o.command(e, feedAdmitOperation, attempt, nil))
		if err != nil {
			if errors.Is(err, errLimit) || errors.Is(err, raftlog.ErrLimit) {
				return fail(err)
			}
			return fail(errors.Join(errRetainedFeedPending, err))
		}
		if err = retainedFeedStatusError(r.status); err != nil {
			return fail(err)
		}
		if r.original != o.pin.definition.id() || r.sequence == 0 || r.sequence > p.maxAttempts {
			return fail(errInvalid)
		}
	}
	// Full policy ceilings were charged on EVERY keeper, including unknown prior
	// attempts. No source request precedes this all-participant barrier.
	arena, err = graphstate.NewOutputBudget(p.operationOutput)
	if err != nil {
		return fail(err)
	}
	budget := &graphRemoteBudget{maxCalls: 1024, maxRows: p.operationRows, maxBytes: p.operationBytes, output: arena}
	endpoints, err = o.publisher.coordinator.endpoints(budget)
	if err != nil {
		return fail(err)
	}
	if err = arena.Reserve(4096 + 512*parts); err != nil {
		return fail(outputBudgetFailure(err))
	}
	previous := o.pin.invocation
	o.pin.invocation = &retainedPinInvocation{endpoints: endpoints, budget: budget, arena: arena, cleanup: previous.cleanup}
	return ctx, func() { o.pin.invocation = previous }, cancel, nil
}
func (o *retainedFeedOwner) Original() ([]byte, error) {
	if !o.valid() {
		return nil, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.pin.invocation.arena.Reserve(len(o.wire)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return owned(o.wire), nil
}
func (o *retainedFeedOwner) command(endpoint *graphAuthenticatedEndpoint, phase byte, action [16]byte, page []byte) retainedFeedCommand {
	return retainedFeedCommand{scope: endpoint.peer.scope, issuer: o.pin.definition.issuer, phase: phase, action: action, definition: o.wire, page: page, original: o.pin.definition.id()}
}
func (o *retainedFeedOwner) states(ctx context.Context) ([]retainedFeedObservation, error) {
	if !o.valid() || ctx == nil {
		return nil, errInvalid
	}
	if err := o.pin.invocation.arena.Reserve(1024 + 512*len(o.pin.definition.parts)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	states := make([]retainedFeedObservation, len(o.pin.definition.parts))
	for i, part := range o.pin.definition.parts {
		endpoint := o.pin.invocation.endpoints[part.certificate.scope.partition]
		r, err := endpoint.retainedFeedRead(ctx, retainedFeedQuestion{kind: feedReadCursor, definition: o.wire})
		if err != nil {
			return nil, errors.Join(errRetainedFeedPending, err)
		}
		if err = retainedFeedStatusError(r.status); err != nil {
			return nil, err
		}
		states[i] = r
	}
	return states, nil
}
func (o *retainedFeedOwner) Publish(ctx context.Context) (retainedFeedToken, error) {
	if !o.valid() || ctx == nil {
		return retainedFeedToken{}, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return retainedFeedToken{}, errRetainedFeedClosed
	}
	ctx, cancel, err := retainedFeedBootstrapContext(ctx, o.publisher, o.definition.policy)
	if err != nil {
		return retainedFeedToken{}, err
	}
	defer cancel()
	// An unissued recovered request uses Issue's genuine full At capture on
	// this SAME invocation before its first native intent can be installed.
	if err := o.pin.Issue(ctx, o.publisher); err != nil {
		return retainedFeedToken{}, err
	}
	token, err := o.pin.Publish(ctx)
	if err != nil {
		return retainedFeedToken{}, err
	}
	for _, part := range o.pin.definition.parts {
		e := o.pin.invocation.endpoints[part.certificate.scope.partition]
		r, err := e.submitRetainedFeed(ctx, o.command(e, feedInitialize, retainedFeedPhaseAction(o.pin.definition.request, feedInitialize), nil))
		if err != nil {
			return retainedFeedToken{}, errors.Join(errRetainedFeedPending, err)
		}
		if err = retainedFeedStatusError(r.status); err != nil {
			return retainedFeedToken{}, err
		}
	}
	// Every pin and cursor is installed BEFORE any final live observation. A
	// participant read taken before a later install is never reused here.
	if _, err = o.states(ctx); err != nil {
		return retainedFeedToken{}, err
	}
	o.token = token
	if err = o.pin.invocation.arena.Reserve(len(o.wire) + 8*len(token.publications) + len(token.manifest)); err != nil {
		return retainedFeedToken{}, outputBudgetFailure(err)
	}
	return retainedFeedToken{definition: owned(o.wire), pin: retainedPinToken{manifest: owned(token.manifest), publications: slices.Clone(token.publications)}}, nil
}
func openRetainedFeed(ctx context.Context, p *retainedPinPublisher, token retainedFeedToken) (*retainedFeedOwner, error) {
	o, err := resumeRetainedFeed(ctx, p, token.definition)
	if err != nil {
		return nil, err
	}
	ctx, cancel, err := retainedFeedBootstrapContext(ctx, p, o.definition.policy)
	if err != nil {
		return nil, err
	}
	defer cancel()
	if !bytes.Equal(token.pin.manifest, o.definition.manifest) || len(token.pin.publications) != len(o.pin.definition.parts) {
		return nil, errInvalid
	}
	if err = o.pin.invocation.arena.Reserve(len(token.pin.manifest) + 8*len(token.pin.publications)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	ownedToken := retainedPinToken{manifest: owned(token.pin.manifest), publications: slices.Clone(token.pin.publications)}
	// The SAME invocation authenticates exact original publication IDs, current
	// native bank, roots and lease through each actual keeper. No transient
	// snapshot or second default invocation is needed merely to reopen cursor.
	states, err := o.states(ctx)
	if err != nil {
		return nil, err
	}
	for i, state := range states {
		if state.publication == 0 || state.publication != ownedToken.publications[i] {
			return nil, errInvalid
		}
	}
	o.token = ownedToken
	return o, nil
}
func (o *retainedFeedOwner) OpenSnapshot(ctx context.Context) (*certifiedGraphRead, error) {
	if !o.valid() || ctx == nil {
		return nil, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return nil, errRetainedFeedClosed
	}
	operationCtx, restore, cancel, operationErr := o.admitOperation(ctx)
	if operationErr != nil {
		return nil, operationErr
	}
	defer restore()
	if _, err := o.states(operationCtx); err != nil {
		cancel()
		return nil, err
	}
	lifetime := &retainedFeedSnapshotLifetime{ctx: operationCtx, remaining: len(o.pin.definition.parts), cancel: cancel}
	read, err := o.publisher.openWithInvocation(operationCtx, o.token, o.pin.definition, o.pin.invocation, lifetime)
	if err != nil {
		cancel()
		return nil, err
	}
	return read, nil
}

// The returned read owns this SAME admitted context/ledger after OpenSnapshot
// restores the owner's pin invocation. Every source closes once; the last Close
// cancels the shared context. Deadline/cancellation can expire it earlier.
type retainedFeedSnapshotLifetime struct {
	ctx       context.Context
	mu        sync.Mutex
	remaining int
	cancel    context.CancelFunc
}
type retainedFeedSnapshotSource struct {
	certifiedGraphSource
	lifetime *retainedFeedSnapshotLifetime
	once     sync.Once
	closeErr error
}

func (s *retainedFeedSnapshotSource) cancelCertifiedReads() {
	if s == nil || s.certifiedGraphSource == nil {
		return
	}
	if source, ok := s.certifiedGraphSource.(interface{ cancelCertifiedReads() }); ok {
		source.cancelCertifiedReads()
	}
}
func (s *retainedFeedSnapshotSource) Close() error {
	if s == nil || s.certifiedGraphSource == nil || s.lifetime == nil {
		return errInvalid
	}
	s.once.Do(func() {
		s.closeErr = s.certifiedGraphSource.Close()
		s.lifetime.mu.Lock()
		defer s.lifetime.mu.Unlock()
		s.lifetime.remaining--
		if s.lifetime.remaining == 0 {
			s.lifetime.cancel()
		}
	})
	return s.closeErr
}

func (o *retainedFeedOwner) pending(states []retainedFeedObservation, request [16]byte) (retainedFeedPage, []byte, bool, error) {
	var chosen []byte
	var page retainedFeedPage
	for _, state := range states {
		cursor, err := decodeRetainedFeedCursor(state.cursor)
		if err != nil {
			return page, nil, false, err
		}
		if cursor.pending == ([32]byte{}) {
			continue
		}
		if len(state.page) == 0 || sha256.Sum256(state.page) != cursor.pending {
			return page, nil, false, errCorrupt
		}
		if chosen != nil && !bytes.Equal(chosen, state.page) {
			return page, nil, false, errInvalid
		}
		chosen = state.page
	}
	if chosen == nil {
		return page, nil, false, nil
	}
	var err error
	page, err = decodeRetainedFeedPage(chosen, o.definition.policy.pageBytes, o.pin.invocation.arena)
	if err != nil {
		return page, nil, false, err
	}
	if request != page.request {
		return page, nil, false, errRetainedFeedPending
	}
	return page, chosen, true, nil
}

// Immutable originals outlive the moving cursor and last-page shortcut. Every
// keeper joins the requested phase GFX hash to its exact retained full GFP; a
// copied page or completed local absence cannot substitute for this join.
func (o *retainedFeedOwner) originalPage(ctx context.Context, request [16]byte, phase byte) (retainedFeedPage, []byte, bool, bool, error) {
	var page retainedFeedPage
	var wire []byte
	all := true
	for _, part := range o.pin.definition.parts {
		e := o.pin.invocation.endpoints[part.certificate.scope.partition]
		actual, err := e.retainedFeedRead(ctx, retainedFeedQuestion{kind: feedReadOriginal, definition: o.wire, request: request, phase: phase})
		if err != nil {
			return page, nil, false, false, errors.Join(errRetainedFeedPending, err)
		}
		if err = retainedFeedStatusError(actual.status); err != nil {
			return page, nil, false, false, err
		}
		if len(actual.page) == 0 {
			all = false
			continue
		}
		if wire != nil && !bytes.Equal(wire, actual.page) {
			return page, nil, false, false, errCorrupt
		}
		wire = actual.page
	}
	if wire == nil {
		return page, nil, false, false, nil
	}
	var err error
	page, err = decodeRetainedFeedPage(wire, o.definition.policy.pageBytes, o.pin.invocation.arena)
	if err != nil || page.request != request || page.original != o.pin.definition.id() {
		return page, nil, false, false, errors.Join(errCorrupt, err)
	}
	return page, wire, all, true, nil
}
func (o *retainedFeedOwner) seal(ctx context.Context, page retainedFeedPage, wire []byte) error {
	if err := o.publisher.coordinator.host.machine.checkFeedPageShape(page, o.pin.invocation.arena); err != nil {
		return err
	}
	for _, part := range o.pin.definition.parts {
		e := o.pin.invocation.endpoints[part.certificate.scope.partition]
		result, err := e.submitRetainedFeed(ctx, o.command(e, feedSealPage, page.request, wire))
		if err != nil {
			return errors.Join(errRetainedFeedPending, err)
		}
		if err = retainedFeedStatusError(result.status); err != nil {
			return err
		}
		if result.page != sha256.Sum256(wire) || result.sequence != page.sequence {
			return errInvalid
		}
	}
	states, err := o.states(ctx)
	if err != nil {
		return err
	}
	for _, state := range states {
		cursor, err := decodeRetainedFeedCursor(state.cursor)
		if err != nil || cursor.pending != sha256.Sum256(wire) || cursor.sequence != page.sequence || !feedVectorEqual(cursor.positions, page.from) || !bytes.Equal(state.page, wire) {
			return errors.Join(errRetainedFeedPending, err)
		}
	}
	return nil
}

// Page consumes one business head or a bounded contiguous protocol census per
// participant. Protocol-only heads make explicit progress first. Business heads are merged only when ALL
// participants have either their next business head or authenticated boundary
// EOF; no unseen head can be skipped by round sorting.
func (o *retainedFeedOwner) Page(ctx context.Context, request [16]byte, boundary graphCut) (retainedFeedDelivery, error) {
	var zero retainedFeedDelivery
	if !o.valid() || ctx == nil || request == ([16]byte{}) {
		return zero, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return zero, errRetainedFeedClosed
	}
	if len(boundary.certificates) != len(o.pin.definition.parts) || len(boundary.certificates) > maxGraphPins {
		return zero, errGraphCutScope
	}
	operationCtx, finish, operationErr := o.operation(ctx)
	if operationErr != nil {
		return zero, operationErr
	}
	defer finish()
	ctx = operationCtx
	if err := o.pin.invocation.arena.Reserve(512 * len(boundary.certificates)); err != nil {
		return zero, outputBudgetFailure(err)
	}
	boundary = graphCut{certificates: slices.Clone(boundary.certificates)}
	states, err := o.states(ctx)
	if err != nil {
		return zero, err
	}
	original, originalWire, allOriginal, anyOriginal, err := o.originalPage(ctx, request, feedSealPage)
	if err != nil {
		return zero, err
	}
	if anyOriginal {
		if !feedCutEqual(original.boundary, boundary) {
			return zero, errInvalid
		}
		if !allOriginal {
			if err = o.seal(ctx, original, originalWire); err != nil {
				return zero, err
			}
		}
		return o.delivery(ctx, original, originalWire, true)
	}
	page, wire, replay, err := o.pending(states, request)
	if err != nil {
		return zero, err
	}
	if replay {
		if !feedCutEqual(page.boundary, boundary) {
			return zero, errInvalid
		}
		if err = o.seal(ctx, page, wire); err != nil {
			return zero, err
		}
		return o.delivery(ctx, page, wire, true)
	}
	var cursor retainedFeedCursor
	for i, state := range states {
		actual, err := decodeRetainedFeedCursor(state.cursor)
		if err != nil {
			return zero, err
		}
		if i == 0 {
			cursor = actual
		} else if actual.sequence != cursor.sequence || actual.acknowledged != cursor.acknowledged || !feedVectorEqual(actual.positions, cursor.positions) || !feedCutEqual(actual.completed, cursor.completed) {
			return zero, errRetainedFeedPending
		}
	}
	if len(boundary.certificates) != len(cursor.positions) {
		return zero, errGraphCutScope
	}
	if cursor.sequence >= o.definition.policy.maxPages {
		return zero, errLimit
	}
	arena := o.pin.invocation.arena
	if err = arena.Reserve(8192 + 1024*len(cursor.positions)); err != nil {
		return zero, outputBudgetFailure(err)
	}
	page = retainedFeedPage{request: request, sequence: cursor.sequence + 1, original: o.pin.definition.id(), previous: cursor.acknowledged, from: slices.Clone(cursor.positions), next: slices.Clone(cursor.positions), boundary: boundary, priorCut: cursor.completed, tips: make([]retainedFeedTip, len(states)), future: make([]bool, len(states))}
	for i, s := range states {
		page.tips[i] = retainedFeedTip{index: s.index, root: s.root}
	}
	frames := make([]*retainedFeedFrame, len(cursor.positions))
	heads := make([]retainedFeedEnvelope, len(frames))
	parts := make([]uint64, len(frames))
	progress := false
	for i, part := range o.pin.definition.parts {
		parts[i] = part.certificate.scope.partition
		if boundary.certificates[i].scope != part.certificate.scope || boundary.certificates[i].configuration != part.certificate.configuration || boundary.certificates[i].round < cursor.completed.certificates[i].round {
			return zero, errInvalid
		}
		e := o.pin.invocation.endpoints[parts[i]]
		state, err := e.retainedFeedRead(ctx, retainedFeedQuestion{kind: feedReadFrame, definition: o.wire, boundary: boundary.certificates[i], tip: page.tips[i], index: cursor.positions[i]})
		if err != nil {
			return zero, err
		}
		if err = retainedFeedStatusError(state.status); err != nil {
			return zero, err
		}
		frames[i] = state.frame
		if state.frame == nil {
			if cursor.positions[i] <= page.tips[i].index {
				return zero, errInvalid
			}
			continue
		}
		local := *o.publisher.coordinator.host.machine
		local.ns.partition = parts[i]
		heads[i], err = local.feedEnvelope(*state.frame, arena)
		if err != nil {
			return zero, err
		}
		if heads[i].kind != 0 && heads[i].round > boundary.certificates[i].round {
			page.future[i] = true
			frames[i] = nil
			heads[i] = retainedFeedEnvelope{}
			continue
		}
		if heads[i].kind == 0 {
			progress = true
		}
	}
	if progress {
		for i, f := range frames {
			if f != nil && heads[i].kind == 0 {
				if f.progressNext <= f.index || f.progressNext-f.index > retainedFeedProgressBatch {
					return zero, errInvalid
				}
				page.frames = append(page.frames, *f)
				page.next[i] = f.progressNext
			}
		}
	} else {
		head, consume, err := completeFeedEnvelope(heads, parts)
		if err != nil {
			return zero, err
		}
		page.kind, page.tx, page.round = head.kind, head.tx, head.round
		for i, yes := range consume {
			if yes {
				page.frames = append(page.frames, *frames[i])
				page.next[i]++
			}
		}
		if head.kind != 0 {
			if err = o.capturePayload(ctx, &page); err != nil {
				return zero, err
			}
		}
	}
	page.eof = true
	for i, n := range page.next {
		if n <= boundary.certificates[i].sourceIndex || n <= page.tips[i].index && !page.future[i] {
			page.eof = false
		}
	}
	if len(page.frames) == 0 && !page.eof {
		return zero, errRetainedFeedPending
	}
	if err = arena.Reserve(4096 + 4*o.definition.policy.pageBytes); err != nil {
		return zero, outputBudgetFailure(err)
	}
	wire, err = encodeRetainedFeedPage(page, o.definition.policy.pageBytes)
	if err != nil {
		return zero, err
	}
	if err = o.seal(ctx, page, wire); err != nil {
		return zero, err
	}
	return o.delivery(ctx, page, wire, false)
}

// ACK uses a deterministic distinct action in the existing native pin protocol.
// Its full page survives partial AdvanceFeed and partial cursor installation.
// No subsequent page can be admitted until every exact ACK original is observed.
func (o *retainedFeedOwner) Ack(ctx context.Context, request [16]byte, digest [32]byte) (graphCut, error) {
	if !o.valid() || ctx == nil || request == ([16]byte{}) || digest == ([32]byte{}) {
		return graphCut{}, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return graphCut{}, errRetainedFeedClosed
	}
	operationCtx, finish, operationErr := o.operation(ctx)
	if operationErr != nil {
		return graphCut{}, operationErr
	}
	defer finish()
	ctx = operationCtx
	page, wire, all, found, err := o.originalPage(ctx, request, feedAcknowledge)
	if err != nil {
		return graphCut{}, err
	}
	if found && sha256.Sum256(wire) != digest {
		return graphCut{}, errInvalid
	}
	if all && found {
		if page.eof {
			return page.boundary, nil
		}
		return page.priorCut, nil
	}
	page, wire, all, found, err = o.originalPage(ctx, request, feedSealPage)
	if err != nil {
		return graphCut{}, err
	}
	if !all || !found {
		return graphCut{}, errRetainedFeedPending
	}
	if sha256.Sum256(wire) != digest {
		return graphCut{}, errInvalid
	}
	if err = o.pin.AdvanceFeed(ctx, page.next, retainedFeedPhaseAction(request, 128)); err != nil {
		return graphCut{}, errors.Join(errRetainedFeedPending, err)
	}
	for _, part := range o.pin.definition.parts {
		e := o.pin.invocation.endpoints[part.certificate.scope.partition]
		r, err := e.submitRetainedFeed(ctx, o.command(e, feedAcknowledge, retainedFeedPhaseAction(request, feedAcknowledge), wire))
		if err != nil {
			return graphCut{}, errors.Join(errRetainedFeedPending, err)
		}
		if err = retainedFeedStatusError(r.status); err != nil {
			return graphCut{}, err
		}
		if r.page != digest || r.sequence != page.sequence {
			return graphCut{}, errInvalid
		}
	}
	// These POST-install completed observations authenticate ALL immutable ACKs.
	// A newer cursor is allowed; it cannot rewrite this original page/result.
	actual, actualWire, all, found, err := o.originalPage(ctx, request, feedAcknowledge)
	if err != nil {
		return graphCut{}, err
	}
	if !all || !found || !bytes.Equal(actualWire, wire) {
		return graphCut{}, errRetainedFeedPending
	}
	if actual.eof {
		return actual.boundary, nil
	}
	return actual.priorCut, nil
}

func (o *retainedFeedOwner) Close() error {
	if !o.valid() {
		return errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = true
	return nil
}
func (o *retainedFeedOwner) Release(ctx context.Context) error {
	if !o.valid() || ctx == nil {
		return errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.terminal(ctx, pinRelease); err != nil {
		return err
	}
	o.closed = true
	return nil
}
func (o *retainedFeedOwner) Expire(ctx context.Context) error {
	if !o.valid() || ctx == nil {
		return errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if err := o.terminal(ctx, pinExpire); err != nil {
		return err
	}
	o.closed = true
	return nil
}

// Native Stage authenticates each exact owner/bank/template. Spending the
// original prepaid terminal exchanges must not require another ordinary source
// read or successful operation admission after cumulative work is exhausted.
// An uncertain exchange stays pending; a separate explicit reopen has its own
// finite recovery admission, never a refund to this owner.
func (o *retainedFeedOwner) terminal(ctx context.Context, phase byte) error {
	var combined error
	r := o.pin
	for i, part := range r.definition.parts {
		if r.completed[i] {
			continue
		}
		e := r.invocation.endpoints[part.certificate.scope.partition]
		result, err := e.submitRetainedPin(ctx, pinCommandFor(r.definition, r.manifest, e.peer.scope, phase, pinTerminalAction(r.definition, r.manifest, phase), 0), r.invocation.cleanup[part.certificate.scope.partition])
		if err != nil || result.publication == 0 || result.status != pinApplied && result.status != pinReleased && result.status != pinExpired {
			combined = errors.Join(combined, err, errRetainedPinPending)
			continue
		}
		r.completed[i] = true
	}
	if combined != nil {
		return combined
	}
	r.closed = true
	return nil
}

// The immutable lookup transcript is useful as data only after every keeper has
// authenticated and sealed this exact full page. Unrecorded reads fail instead
// of turning an incomplete dictionary proof into absence/current-state fallback.
type retainedFeedReplaySource struct {
	ctx        context.Context
	payload    retainedFeedPayload
	scope      allocationScope
	arena      *graphstate.OutputBudget
	used       []bool
	generation uint64
}

func (s *retainedFeedReplaySource) Root() (raftlog.ApplicationRoot, error) {
	if s == nil {
		return raftlog.ApplicationRoot{}, errInvalid
	}
	return s.RootBounded(s.ctx, 172)
}
func (s *retainedFeedReplaySource) RootBounded(ctx context.Context, maximum int) (raftlog.ApplicationRoot, error) {
	if s == nil || ctx == nil {
		return raftlog.ApplicationRoot{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return raftlog.ApplicationRoot{}, err
	}
	if maximum < 172 {
		return raftlog.ApplicationRoot{}, errLimit
	}
	if err := s.arena.Reserve(512); err != nil {
		return raftlog.ApplicationRoot{}, outputBudgetFailure(err)
	}
	image := owned(s.payload.root[:])
	return raftlog.ApplicationRoot{Generation: s.generation, Index: s.payload.index, Image: image, ImageHash: sha256.Sum256(image)}, nil
}
func (s *retainedFeedReplaySource) ApplicationBinding(ctx context.Context) (raftlog.ApplicationBinding, error) {
	if s == nil || ctx == nil {
		return raftlog.ApplicationBinding{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return raftlog.ApplicationBinding{}, err
	}
	return raftlog.ApplicationBinding{Identity: raftlog.ApplicationIdentity{Graph: [16]byte(s.scope.graph), Partition: s.scope.partition, Group: s.scope.group}, SemanticContractID: s.scope.semantic}, nil
}
func (s *retainedFeedReplaySource) ReadLimits() raftlog.ReadBudget {
	return raftlog.ReadBudget{Rows: 4096, Bytes: retainedFeedMaxBytes}
}
func feedLookupQueryEqual(a, b retainedFeedLookup) bool {
	return a.kind == b.kind && a.rows == b.rows && a.bytes == b.bytes && bytes.Equal(a.key, b.key) && bytes.Equal(a.upper, b.upper) && bytes.Equal(a.after, b.after)
}
func (s *retainedFeedReplaySource) lookup(ctx context.Context, q retainedFeedLookup) (retainedFeedLookup, error) {
	if s == nil || ctx == nil {
		return retainedFeedLookup{}, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return retainedFeedLookup{}, err
	}
	for i, actual := range s.payload.lookups {
		if !s.used[i] && feedLookupQueryEqual(actual, q) {
			s.used[i] = true
			return actual, nil
		}
	}
	return retainedFeedLookup{}, graphstore.ErrSourceUnavailable
}
func (s *retainedFeedReplaySource) Get(ctx context.Context, key []byte, maximum int) (raftlog.KV, bool, error) {
	q, err := s.lookup(ctx, retainedFeedLookup{kind: 1, key: key, rows: 1, bytes: maximum})
	if err != nil {
		return raftlog.KV{}, false, err
	}
	if !q.found {
		return raftlog.KV{}, false, nil
	}
	if len(q.values) != 1 {
		return raftlog.KV{}, false, errInvalid
	}
	return q.values[0], true, nil
}
func (s *retainedFeedReplaySource) Scan(ctx context.Context, lower, upper, after []byte, budget raftlog.ReadBudget) (raftlog.ApplicationPage, error) {
	q, err := s.lookup(ctx, retainedFeedLookup{kind: 2, key: lower, upper: upper, after: after, rows: budget.Rows, bytes: budget.Bytes})
	if err != nil {
		return raftlog.ApplicationPage{}, err
	}
	return raftlog.ApplicationPage{Rows: q.values, Next: q.next, Complete: q.complete, Visited: q.visited, Bytes: q.workBytes}, nil
}

func (o *retainedFeedOwner) pageCatalog(ctx context.Context, page retainedFeedPage, sources []graphstore.ApplicationReadSource) (*graphstore.PartitionTransactionView, error) {
	if len(sources) != len(o.pin.definition.parts) {
		return nil, errInvalid
	}
	h := o.publisher.coordinator.host
	l := h.machine.limits
	arena := o.pin.invocation.arena
	row, found, err := sources[0].Get(ctx, genesisConfigurationKey(namespace{h.machine.ns.graph, page.boundary.certificates[0].scope.partition}), 8192)
	if err != nil || !found {
		return nil, errors.Join(errInvalid, err)
	}
	cfg, err := decodeGenesisAllocationConfig(row.Value, h.machine.ns.graph)
	if err != nil {
		return nil, err
	}
	digest, err := cfg.digest()
	if err != nil || digest != page.boundary.certificates[0].configuration {
		return nil, errors.Join(errInvalid, err)
	}
	b := o.pin.invocation.budget
	b.mu.Lock()
	rows, bytes := b.maxRows-b.rows, b.maxBytes-b.bytes
	b.mu.Unlock()
	if rows < 1 || bytes < 1 {
		return nil, errLimit
	}
	v, _, err := graphstore.OpenPartitionTransactionSources(ctx, sources, h.machine.declaration, cfg.defaultAxis, digest, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: rows, SourceBytes: bytes, OutputBytes: arena.Remaining()}, arena)
	return v, err
}

// resolve walks the complete canonical CDC footprint in deterministic order.
// It resolves both before and after values, previous endpoint lives, and exact
// owner-kind schema at the envelope's original composite round sources.
func (o *retainedFeedOwner) resolve(ctx context.Context, page retainedFeedPage, v *graphstore.PartitionTransactionView) (graphChanges, []graphstate.PropertyDefinition, error) {
	out := graphChanges{ns: o.publisher.coordinator.host.machine.ns}
	var schemas []graphstate.PropertyDefinition
	if page.kind == 0 || page.kind == feedMoveEnvelope {
		return out, schemas, nil
	}
	arena := o.pin.invocation.arena
	l := o.publisher.coordinator.host.machine.limits
	rows := 0
	entity := func(id graphstate.EntityID) (graphstate.EntityRecord, error) {
		for _, r := range out.entities {
			if r.ID == id {
				return r, nil
			}
		}
		if rows >= o.definition.policy.pageRows || len(out.entities) >= l.maxClaims {
			return graphstate.EntityRecord{}, errLimit
		}
		if err := arena.Reserve(1024); err != nil {
			return graphstate.EntityRecord{}, outputBudgetFailure(err)
		}
		r, err := v.Entity(ctx, id)
		if err != nil || !r.Found {
			return graphstate.EntityRecord{}, errors.Join(graphstore.ErrSourceUnavailable, err)
		}
		out.entities = append(out.entities, r.Record)
		rows++
		return r.Record, nil
	}
	life := func(owner graphstate.EntityID, id graphstate.LifeID) error {
		if id == 0 {
			return nil
		}
		for _, r := range out.lives {
			if r.Owner == owner && r.Life == id {
				return nil
			}
		}
		if rows >= o.definition.policy.pageRows || len(out.lives) >= l.maxClaims {
			return errLimit
		}
		if err := arena.Reserve(512); err != nil {
			return outputBudgetFailure(err)
		}
		r, err := v.Life(ctx, owner, id)
		if err != nil || !r.Found {
			return errors.Join(graphstore.ErrSourceUnavailable, err)
		}
		out.lives = append(out.lives, r.Record)
		rows++
		return nil
	}
	value := func(id graphstate.ValueID) error {
		if id == 0 {
			return nil
		}
		for _, r := range out.values {
			if r.ID == id {
				return nil
			}
		}
		if rows >= o.definition.policy.pageRows || len(out.values) >= l.maxClaims {
			return errLimit
		}
		if err := arena.Reserve(1024); err != nil {
			return outputBudgetFailure(err)
		}
		r, err := v.Value(ctx, id)
		if err != nil || !r.Found || r.ID != id {
			return errors.Join(graphstore.ErrSourceUnavailable, err)
		}
		out.values = append(out.values, graphstate.ValueWrite{ID: id, Value: r.Value})
		rows++
		return nil
	}
	for _, frame := range page.frames {
		local := *o.publisher.coordinator.host.machine
		local.ns.partition = frame.partition
		var bound outcome
		if page.kind == feedCrossEnvelope {
			s, err := decodeCrossStatus(frame.outcome)
			if err != nil {
				return out, nil, err
			}
			bound = outcome{ns: local.ns, kind: graphOperations, identity: s.tx, hash: s.digest, index: frame.index, disposition: applied}
		} else {
			var err error
			bound, err = decodePartitionMappedOutcome(frame.outcome, local.ns)
			if err != nil {
				return out, nil, err
			}
		}
		changes, err := decodeTypedChangeEnvelope(frame.changes, bound, l, true)
		if err != nil {
			return out, nil, err
		}
		for _, r := range changes.entities {
			if _, err = entity(r.ID); err != nil {
				return out, nil, err
			}
		}
		for _, r := range changes.lives {
			if err = life(r.Owner, r.Life); err != nil {
				return out, nil, err
			}
		}
		for _, r := range changes.values {
			if err = value(r.ID); err != nil {
				return out, nil, err
			}
		}
		for _, group := range changes.groups {
			r, err := entity(group.Key.Owner)
			if err != nil {
				return out, nil, err
			}
			if err = life(group.Key.Owner, group.Key.Life); err != nil {
				return out, nil, err
			}
			if group.Key.Kind == graphstate.ScalarProperty || group.Key.Kind == graphstate.SetMember {
				present := false
				for _, d := range schemas {
					if d.Owner == r.Kind && d.Name == group.Key.Name {
						present = true
						break
					}
				}
				if !present {
					if rows >= o.definition.policy.pageRows || len(schemas) >= l.maxSchemas {
						return out, nil, errLimit
					}
					if err = arena.Reserve(512 + 2*len(group.Key.Name)); err != nil {
						return out, nil, outputBudgetFailure(err)
					}
					d, err := v.Property(ctx, r.Kind, group.Key.Name)
					if err != nil || !d.Found {
						return out, nil, errors.Join(graphstore.ErrSourceUnavailable, err)
					}
					schemas = append(schemas, d.Record)
					rows++
				}
			}
			if group.Key.Kind == graphstate.SetMember {
				if err = value(group.Key.Member); err != nil {
					return out, nil, err
				}
			}
			for _, change := range group.Changes {
				if rows >= o.definition.policy.pageRows {
					return out, nil, errLimit
				}
				rows++
				for _, cell := range []state.Cell{change.Before(), change.After()} {
					if !cell.Present() || cell.Value().IsNull() {
						continue
					}
					if group.Key.Kind == graphstate.Presence {
						if err = life(group.Key.Owner, graphstate.LifeID(cell.Value().ID())); err != nil {
							return out, nil, err
						}
					} else if group.Key.Kind == graphstate.ScalarProperty {
						if err = value(graphstate.ValueID(cell.Value().ID())); err != nil {
							return out, nil, err
						}
					}
				}
			}
		}
	}
	// Life-bound relationships retain the exact endpoint identities and lives,
	// including a closed before-image's endpoints; no current-life substitution.
	for i := 0; i < len(out.lives); i++ {
		binding := out.lives[i]
		owner, err := entity(binding.Owner)
		if err != nil {
			return out, nil, err
		}
		if owner.Kind == graphstate.Relationship && owner.Mode == graphstate.LifeBound {
			if _, err = entity(owner.Source); err != nil {
				return out, nil, err
			}
			if _, err = entity(owner.Target); err != nil {
				return out, nil, err
			}
			if err = life(owner.Source, binding.SourceLife); err != nil {
				return out, nil, err
			}
			if err = life(owner.Target, binding.TargetLife); err != nil {
				return out, nil, err
			}
		}
	}
	return out, schemas, nil
}
func (o *retainedFeedOwner) capturePayload(ctx context.Context, page *retainedFeedPage) (err error) {
	if page == nil || page.kind == 0 {
		return errInvalid
	}
	if err = o.pin.invocation.arena.Reserve(1024 + 512*len(o.pin.definition.parts)); err != nil {
		return outputBudgetFailure(err)
	}
	data := make([]graphstore.ApplicationReadSource, len(o.pin.definition.parts))
	recorders := make([]*retainedFeedRecordingSource, len(data))
	for i, part := range o.pin.definition.parts {
		e := o.pin.invocation.endpoints[part.certificate.scope.partition]
		recorders[i], err = newRetainedFeedRecordingSource(ctx, e, retainedFeedQuestion{definition: o.wire, boundary: page.boundary.certificates[i], tip: page.tips[i], round: page.round}, o.pin.invocation.arena)
		if err != nil {
			return err
		}
		data[i] = recorders[i]
	}
	v, err := o.pageCatalog(ctx, *page, data)
	if err != nil {
		return err
	}
	_, _, err = o.resolve(ctx, *page, v)
	err = errors.Join(err, v.Close())
	if err != nil {
		return err
	}
	page.payloads = make([]retainedFeedPayload, len(data))
	for i, r := range recorders {
		page.payloads[i] = r.payload
	}
	return nil
}
func (o *retainedFeedOwner) delivery(ctx context.Context, page retainedFeedPage, wire []byte, replay bool) (out retainedFeedDelivery, err error) {
	if err = o.publisher.coordinator.host.machine.checkFeedPageShape(page, o.pin.invocation.arena); err != nil {
		return retainedFeedDelivery{}, err
	}
	out = retainedFeedDelivery{page: page, wire: wire, digest: sha256.Sum256(wire), replay: replay}
	if page.kind == 0 {
		return out, nil
	}
	if len(page.payloads) != len(o.pin.definition.parts) {
		return retainedFeedDelivery{}, errInvalid
	}
	arena := o.pin.invocation.arena
	if err = arena.Reserve(2048 + 256*len(page.payloads)); err != nil {
		return retainedFeedDelivery{}, outputBudgetFailure(err)
	}
	data := make([]graphstore.ApplicationReadSource, len(page.payloads))
	sources := make([]*retainedFeedReplaySource, len(data))
	states, err := o.states(ctx)
	if err != nil {
		return retainedFeedDelivery{}, err
	}
	for i, p := range page.payloads {
		scope := o.pin.definition.parts[i].certificate.scope
		if p.partition != scope.partition {
			return retainedFeedDelivery{}, errInvalid
		}
		if err = arena.Reserve(len(p.lookups)); err != nil {
			return retainedFeedDelivery{}, outputBudgetFailure(err)
		}
		sources[i] = &retainedFeedReplaySource{ctx: ctx, payload: p, scope: scope, arena: arena, used: make([]bool, len(p.lookups)), generation: states[i].generation}
		data[i] = sources[i]
	}
	v, err := o.pageCatalog(ctx, page, data)
	if err != nil {
		return retainedFeedDelivery{}, err
	}
	out.resolved, out.schemas, err = o.resolve(ctx, page, v)
	err = errors.Join(err, v.Close())
	if err != nil {
		return retainedFeedDelivery{}, err
	}
	for _, s := range sources {
		for _, used := range s.used {
			if !used {
				return retainedFeedDelivery{}, errInvalid
			}
		}
	}
	return out, nil
}

var _ graphstore.ApplicationReadSource = (*retainedFeedReplaySource)(nil)

type retainedFeedPendingPage struct {
	request       [16]byte
	digest        [32]byte
	acknowledging bool
}

// Pending returns only recovery coordinates, not a partially sealed delivery or
// a completed cut. All coordinates come from actual keeper controls. A new
// client can finish the SAME Page or ACK without the former client's handle.
func (o *retainedFeedOwner) Pending(ctx context.Context) (retainedFeedPendingPage, bool, error) {
	if !o.valid() || ctx == nil {
		return retainedFeedPendingPage{}, false, errInvalid
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return retainedFeedPendingPage{}, false, errRetainedFeedClosed
	}
	operationCtx, finish, operationErr := o.operation(ctx)
	if operationErr != nil {
		return retainedFeedPendingPage{}, false, operationErr
	}
	defer finish()
	ctx = operationCtx
	states, err := o.states(ctx)
	if err != nil {
		return retainedFeedPendingPage{}, false, err
	}
	var result retainedFeedPendingPage
	found := false
	for _, s := range states {
		c, err := decodeRetainedFeedCursor(s.cursor)
		if err != nil {
			return result, false, err
		}
		if c.pending == ([32]byte{}) {
			continue
		}
		p, err := decodeRetainedFeedPage(s.page, o.definition.policy.pageBytes, o.pin.invocation.arena)
		if err != nil || sha256.Sum256(s.page) != c.pending {
			return result, false, errors.Join(errCorrupt, err)
		}
		if found && (result.request != p.request || result.digest != c.pending) {
			return result, false, errInvalid
		}
		result.request, result.digest, found = p.request, c.pending, true
	}
	if !found {
		return result, false, nil
	}
	for _, s := range states {
		c, err := decodeRetainedFeedCursor(s.cursor)
		if err != nil {
			return result, false, err
		}
		if c.pending == ([32]byte{}) && c.acknowledged == result.digest {
			result.acknowledging = true
		}
	}
	return result, true, nil
}
