package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
)

var errRetainedPinPending = errors.New("graphapply: durable pin pending; recover exact original manifest")

// A portable value is inert after decoding. Every use rechecks the actual
// immutable originals, live owners, native bank and lease through authenticated
// completed reads. It does not authorize an arbitrary hash or future lifetime.
type retainedPinToken struct {
	manifest     []byte
	publications []uint64
}
type retainedPinInvocation struct {
	endpoints map[uint64]*graphAuthenticatedEndpoint
	budget    *graphRemoteBudget
	arena     *graphstate.OutputBudget
	cleanup   map[uint64]*graphRemoteBudget
}
type retainedPinPublisher struct{ coordinator *crossProcessCoordinator }
type retainedPinRequest struct {
	mu         sync.Mutex
	self       *retainedPinRequest
	manifest   []byte
	definition graphPinManifest
	invocation *retainedPinInvocation
	born       bool
	closed     bool
	completed  [maxGraphPins]bool
}

func newRetainedPinPublisher(c *crossProcessCoordinator) (*retainedPinPublisher, error) {
	if c == nil || c.host == nil || c.host.machine == nil || !c.host.machine.retainedPins() || len(c.transports) != c.host.machine.declaration.Len() {
		return nil, errInvalid
	}
	return &retainedPinPublisher{coordinator: c}, nil
}
func (p *retainedPinPublisher) valid() bool {
	return p != nil && p.coordinator != nil && p.coordinator.host != nil && p.coordinator.host.machine != nil && p.coordinator.host.machine.retainedPins()
}
func (p *retainedPinPublisher) invocation() (*retainedPinInvocation, error) {
	if !p.valid() {
		return nil, errInvalid
	}
	c := p.coordinator
	l := c.host.machine.limits
	arena, err := graphstate.NewOutputBudget(min(l.outputBytes, l.sourceBytes))
	if err != nil {
		return nil, err
	}
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		return nil, err
	}
	if err := arena.Reserve(4096 + 512*len(endpoints)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	invocation := &retainedPinInvocation{endpoints: endpoints, budget: budget, arena: arena, cleanup: make(map[uint64]*graphRemoteBudget, len(endpoints))}
	// One known Close exchange per participant is funded BEFORE source capture or
	// publication. This private pool consumes the original invocation allowance.
	for i := range c.host.machine.declaration.Len() {
		part, _ := c.host.machine.declaration.PartitionAt(i)
		pool, err := retainedPinTerminalBudget(budget)
		if err != nil {
			return nil, err
		}
		invocation.cleanup[part.Partition] = pool
	}
	return invocation, nil
}

// Begin captures a complete authenticated cut on one invocation ledger. The
// returned exact request can remain pending; it is not a portable live token.
func (p *retainedPinPublisher) Begin(ctx context.Context, id requestID, request graphReadRequest, expires uint64) (result *retainedPinRequest, err error) {
	if !p.valid() || ctx == nil || id == (requestID{}) || expires == 0 || expires == math.MaxUint64 {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if p.coordinator.host.issuerUnavailable() {
		return nil, replica.ErrStopped
	}
	invocation, err := p.invocation()
	if err != nil {
		return nil, err
	}
	c := p.coordinator
	engine := &certifiedReadCoordinator{declaration: c.host.machine.declaration}
	read, err := engine.openBackend(ctx, request, nil, &remoteCertifiedGraphBackend{coordinator: c, endpoints: invocation.endpoints, budget: invocation.budget, arena: invocation.arena})
	if err != nil {
		return nil, err
	}
	defer func() {
		err = errors.Join(err, read.Close())
		if err != nil {
			result = nil
		}
	}()
	cut, err := read.Cut()
	if err != nil {
		return nil, err
	}
	if len(cut.certificates) != c.host.machine.declaration.Len() {
		return nil, errGraphCutScope
	}
	manifest := graphPinManifest{publisher: c.host.machine.scope(), issuer: c.host.session, request: [16]byte(id), expires: expires}
	if err := invocation.arena.Reserve(4096 + 1024*len(cut.certificates)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	for i, cert := range cut.certificates {
		observed, err := invocation.endpoints[cert.scope.partition].retainedPinRead(ctx, nil)
		if err != nil {
			return nil, err
		}
		if observed.configuration != cert.configuration || i > 0 && observed.epoch != manifest.epoch {
			return nil, errors.Join(errRetainedPinPending, replica.ErrUnavailable)
		}
		manifest.epoch = observed.epoch
		part := graphPinPart{certificate: cert, feedStart: cert.sourceIndex + 1}
		part.lineage, err = pinLineage(part)
		if err != nil {
			return nil, err
		}
		manifest.parts = append(manifest.parts, part)
	}
	wire, err := encodeGraphPinManifest(manifest)
	if err != nil {
		return nil, err
	}
	if err := c.host.machine.bindPinManifest(manifest); err != nil {
		return nil, err
	}
	result = &retainedPinRequest{manifest: wire, definition: manifest, invocation: invocation, born: true}
	result.self = result
	return result, nil
}

// Resume accepts inert canonical bytes and recovers their ACTUAL retained
// original. If no participant authenticates that original, it cannot fabricate
// a new issuer/request or certify an earlier caller's lost reasoning.
func (p *retainedPinPublisher) Resume(ctx context.Context, wire []byte) (*retainedPinRequest, error) {
	if !p.valid() || ctx == nil {
		return nil, errInvalid
	}
	manifest, err := decodeGraphPinManifest(wire)
	if err != nil {
		return nil, err
	}
	if err := p.coordinator.host.machine.bindPinManifest(manifest); err != nil {
		return nil, err
	}
	invocation, err := p.invocation()
	if err != nil {
		return nil, err
	}
	if err := invocation.arena.Reserve(4096 + 8*len(wire)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	request := &retainedPinRequest{manifest: owned(wire), definition: manifest, invocation: invocation}
	request.self = request
	return request, nil
}
func (r *retainedPinRequest) Original() ([]byte, error) {
	if r == nil || r.self != r {
		return nil, errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.invocation.arena.Reserve(len(r.manifest) + 64); err != nil {
		return nil, outputBudgetFailure(err)
	}
	return owned(r.manifest), nil
}
func pinCommandFor(manifest graphPinManifest, wire []byte, scope allocationScope, phase byte, action [16]byte, feed uint64) graphPinCommand {
	c := graphPinCommand{scope: scope, issuer: manifest.issuer, action: action, id: manifest.id(), digest: sha256.Sum256(wire), phase: phase, feedStart: feed}
	if phase == pinPublish || phase == pinIssue || phase == pinCancelIssue {
		if phase == pinPublish {
			c.action = manifest.request
		}
		c.manifest = wire
		c.epoch = manifest.epoch
	}
	if phase == pinExpire {
		c.epoch = manifest.expires
	}
	return c
}
func pinTerminalAction(m graphPinManifest, wire []byte, phase byte) [16]byte {
	hash := sha256.New()
	_, _ = hash.Write([]byte("rho-tkg:pin-terminal-action:v1\x00"))
	_, _ = hash.Write([]byte{phase})
	_, _ = hash.Write(wire)
	var sum [32]byte
	hash.Sum(sum[:0])
	var action [16]byte
	copy(action[:], sum[:16])
	if action == ([16]byte{}) || action == m.request {
		action[0] ^= 1
	}
	return action
}
func (e *graphAuthenticatedEndpoint) submitRetainedPin(ctx context.Context, c graphPinCommand, budget *graphRemoteBudget) (graphPinResult, error) {
	if e == nil || c.scope != e.peer.scope {
		return graphPinResult{}, errInvalid
	}
	wire, err := encodeGraphPinCommand(c)
	if err != nil {
		return graphPinResult{}, err
	}
	proposal, err := encodeGraphSourceProposal(allocationProposal{target: c.scope, wire: wire})
	if err != nil {
		return graphPinResult{}, err
	}
	// Native lookup work is prepaid separately from packet ownership. Even a
	// missing/cancelled reply cannot refund uncertain work on this invocation.
	if err := reserveRetainedPinLookup(budget, len(wire)); err != nil {
		return graphPinResult{}, err
	}
	response, err := e.callUsing(ctx, graphSourceRequest{kind: sourceSubmit, command: proposal}, 1024, budget)
	if err != nil {
		return graphPinResult{}, err
	}
	body, err := graphBody(response.body, "GPR\x01", 1024)
	if err != nil {
		return graphPinResult{}, err
	}
	cursor := graphCursor{b: body}
	_ = cursor.count(pinLookupRows, 0)
	_ = cursor.count(pinLookupBytes, 0)
	_ = cursor.count(pinLookupOutput, 0)
	outcome := cursor.field(256)
	if cursor.err != nil || len(cursor.b) != 0 {
		return graphPinResult{}, errors.Join(errCorrupt, cursor.err)
	}
	result, err := decodeGraphPinResult(outcome)
	if err != nil || result.id != c.id || result.digest != c.digest || result.action != c.action || result.phase != c.phase {
		return graphPinResult{}, errors.Join(errInvalid, err)
	}
	return result, nil
}

func (r *retainedPinRequest) Publish(ctx context.Context) (token retainedPinToken, err error) {
	if r == nil || r.self != r || ctx == nil {
		return token, errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return token, errGraphPinReleased
	}
	manifest := r.definition
	invocation := r.invocation
	var installed [maxGraphPins]bool
	if !r.born {
		authenticated := false
		for i, part := range manifest.parts {
			observed, e := invocation.endpoints[part.certificate.scope.partition].retainedPinRead(ctx, r.manifest)
			if e != nil {
				return token, errors.Join(errRetainedPinPending, e)
			}
			if observed.issued.index != 0 {
				authenticated = true
			}
			if observed.result.index != 0 {
				authenticated = true
				installed[i] = observed.result.status == pinApplied && observed.result.publication != 0
			}
			if observed.status == pinExpired {
				return token, errGraphPinExpired
			}
			if observed.status == pinReleased {
				return token, errGraphPinReleased
			}
		}
		if !authenticated {
			return token, errors.Join(errRetainedPinPending, replica.ErrUnavailable)
		}
	}
	// First finish EVERY participant installation. No early live-owner proof is
	// retained for the token; an uncertain result preserves this exact request.
	for i, part := range manifest.parts {
		if installed[i] {
			continue // authenticated immutable original; no new ordinary log credit
		}
		endpoint := invocation.endpoints[part.certificate.scope.partition]
		result, e := endpoint.submitRetainedPin(ctx, pinCommandFor(manifest, r.manifest, endpoint.peer.scope, pinPublish, manifest.request, 0), invocation.budget)
		if e != nil {
			return token, errors.Join(errRetainedPinPending, e)
		}
		if result.status == pinExpired {
			return token, errGraphPinExpired
		}
		if result.status == pinReleased {
			return token, errGraphPinReleased
		}
		if result.status != pinApplied || result.publication == 0 {
			return token, errors.Join(errRetainedPinPending, replica.ErrUnavailable)
		}
	}
	if err := invocation.arena.Reserve(4096 + 2*len(r.manifest) + 64*len(manifest.parts)); err != nil {
		return token, outputBudgetFailure(err)
	}
	publications := make([]uint64, len(manifest.parts))
	// Only this POST-INSTALL pass qualifies publication. Release/expiry never
	// reactivate an original owner and epochs are monotone: the live proofs have
	// a common live point after all installs, not a promise of future availability.
	for i, part := range manifest.parts {
		observed, e := invocation.endpoints[part.certificate.scope.partition].retainedPinRead(ctx, r.manifest)
		if e != nil {
			return token, errors.Join(errRetainedPinPending, e)
		}
		if e := retainedPinLive(observed); e != nil {
			return token, e
		}
		if observed.configuration != part.certificate.configuration || observed.result.digest != sha256.Sum256(r.manifest) || observed.epoch >= manifest.expires {
			return token, errInvalid
		}
		publications[i] = observed.owner.publication
	}
	token = retainedPinToken{manifest: owned(r.manifest), publications: publications}
	return token, nil
}

// The dependency prepass supplies genuine native indexes. Re-encoding spends
// the SAME original invocation allowance; this helper grants no authority.
func (r *retainedPinRequest) columnFeeds(feed [2]uint64) error {
	if r == nil || r.self != r {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.born || r.closed || len(r.definition.parts) != 2 || !durableColumnGraphAgreement(r.definition.publisher.semantic) {
		return errInvalid
	}
	if err := r.invocation.arena.Reserve(4096 + 8*len(r.manifest)); err != nil {
		return outputBudgetFailure(err)
	}
	for g := range 2 {
		if feed[g] == 0 || feed[g] > r.definition.parts[g].certificate.sourceIndex+1 {
			return errInvalid
		}
	}
	for g := range 2 {
		r.definition.parts[g].feedStart = feed[g]
		lineage, err := pinLineage(r.definition.parts[g])
		if err != nil {
			return err
		}
		r.definition.parts[g].lineage = lineage
	}
	wire, err := encodeGraphPinManifest(r.definition)
	if err == nil {
		r.manifest = wire
	}
	return err
}

// Issuance is durable creation intent, never a live token. A recovered recipe
// first reacquires the exact native cut on this invocation ledger; local journal
// bytes cannot bless a reclaimed source or a made-up issuer/certificate.
func (r *retainedPinRequest) Issue(ctx context.Context, p *retainedPinPublisher) error {
	if r == nil || r.self != r || ctx == nil || !p.valid() {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.definition
	if r.closed || !durableColumnGraphAgreement(m.publisher.semantic) {
		return errInvalid
	}
	endpoint := r.invocation.endpoints[m.publisher.partition]
	if endpoint == nil || endpoint.peer.scope != m.publisher {
		return errGraphCutScope
	}
	observed, err := endpoint.retainedPinRead(ctx, r.manifest)
	if err != nil {
		return errors.Join(errRetainedPinPending, err)
	}
	if observed.cancelled || observed.status == pinReleased {
		return errGraphPinReleased
	}
	if observed.status == pinExpired {
		return errGraphPinExpired
	}
	if observed.issued.index != 0 {
		return nil
	}
	if !r.born {
		cut := graphCut{certificates: make([]graphCertificate, len(m.parts))}
		if err := r.invocation.arena.Reserve(4096 + 1024*len(m.parts)); err != nil {
			return outputBudgetFailure(err)
		}
		for i, part := range m.parts {
			cut.certificates[i] = part.certificate
		}
		engine := &certifiedReadCoordinator{declaration: p.coordinator.host.machine.declaration}
		read, err := engine.openBackend(ctx, graphReadRequest{mode: graphReadAt, cut: cut}, nil, &remoteCertifiedGraphBackend{coordinator: p.coordinator, endpoints: r.invocation.endpoints, budget: r.invocation.budget, arena: r.invocation.arena})
		if err != nil {
			return err
		}
		if err := read.Close(); err != nil {
			return err
		}
	}
	result, err := endpoint.submitRetainedPin(ctx, pinCommandFor(m, r.manifest, m.publisher, pinIssue, pinTerminalAction(m, r.manifest, pinIssue), 0), r.invocation.budget)
	if err != nil {
		return errors.Join(errRetainedPinPending, err)
	}
	if result.status == pinReleased {
		return errGraphPinReleased
	}
	if result.status == pinExpired {
		return errGraphPinExpired
	}
	if result.status != pinApplied {
		return errors.Join(errRetainedPinPending, replica.ErrUnavailable)
	}
	return nil
}

// Every participant receives an actual ordered cancellation fence. Completed
// absence alone is never terminal. Existing owners spend their original Close
// alternative first; a publication racing that observation makes Cancel Pending.
func (r *retainedPinRequest) CancelIssue(ctx context.Context) error {
	if r == nil || r.self != r || ctx == nil {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.definition
	if !durableColumnGraphAgreement(m.publisher.semantic) {
		return errInvalid
	}
	if r.closed {
		return nil
	}
	var combined error
	for i, part := range m.parts {
		if r.completed[i] {
			continue
		}
		endpoint := r.invocation.endpoints[part.certificate.scope.partition]
		observed, err := endpoint.retainedPinRead(ctx, r.manifest)
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		if observed.owner.publication != 0 && observed.owner.terminal == 0 {
			close := pinCommandFor(m, r.manifest, endpoint.peer.scope, pinClose, pinTerminalAction(m, r.manifest, pinClose), 0)
			result, err := endpoint.submitRetainedPin(ctx, close, r.invocation.cleanup[part.certificate.scope.partition])
			if err != nil || result.publication == 0 || result.status != pinApplied && result.status != pinReleased && result.status != pinExpired {
				combined = errors.Join(combined, replica.ErrUnavailable, err)
				continue
			}
		}
		cancel := pinCommandFor(m, r.manifest, endpoint.peer.scope, pinCancelIssue, pinTerminalAction(m, r.manifest, pinCancelIssue), 0)
		result, err := endpoint.submitRetainedPin(ctx, cancel, r.invocation.budget)
		if err != nil || result.status != pinReleased {
			combined = errors.Join(combined, replica.ErrUnavailable, err)
			continue
		}
		r.completed[i] = true
	}
	if combined != nil {
		return errors.Join(errRetainedPinPending, combined)
	}
	r.closed = true
	return nil
}

func encodeRetainedPinToken(token retainedPinToken) ([]byte, error) {
	manifest, err := decodeGraphPinManifest(token.manifest)
	if err != nil || len(token.publications) != len(manifest.parts) {
		return nil, errors.Join(errInvalid, err)
	}
	for _, index := range token.publications {
		if index == 0 {
			return nil, errInvalid
		}
	}
	return boundedEncoding(graphPinManifestBytes+1024, func(w *boundedWriter) {
		w.add([]byte{'G', 'P', 'T', 1})
		w.field(token.manifest)
		w.u32(len(token.publications))
		for _, index := range token.publications {
			w.u64(index)
		}
	})
}
func decodeRetainedPinToken(wire []byte) (retainedPinToken, error) {
	body, err := graphBody(wire, "GPT\x01", graphPinManifestBytes+1024)
	if err != nil {
		return retainedPinToken{}, err
	}
	c := graphCursor{b: body}
	token := retainedPinToken{manifest: c.field(graphPinManifestBytes)}
	count := c.count(maxGraphPins, 8)
	if c.err != nil {
		return retainedPinToken{}, c.err
	}
	token.publications = make([]uint64, count)
	for i := range count {
		token.publications[i] = c.u64()
	}
	canonical, err := encodeRetainedPinToken(token)
	if c.err != nil || len(c.b) != 0 || err != nil || !bytes.Equal(canonical, wire) {
		return retainedPinToken{}, errors.Join(errCorrupt, c.err, err)
	}
	token.manifest = owned(token.manifest)
	return token, nil
}

// Cleanup never clears a process-local owner merely because a send failed.
// Every participant must return its actual terminal outcome; otherwise recovery
// remains pending and already installed pins keep protecting their data.
func (r *retainedPinRequest) Close(ctx context.Context) error   { return r.terminal(ctx, pinClose) }
func (r *retainedPinRequest) Release(ctx context.Context) error { return r.terminal(ctx, pinRelease) }
func (r *retainedPinRequest) Expire(ctx context.Context) error  { return r.terminal(ctx, pinExpire) }
func (r *retainedPinRequest) terminal(ctx context.Context, phase byte) error {
	if r == nil || r.self != r || ctx == nil {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	manifest := r.definition
	var combined error
	for i, part := range manifest.parts {
		if r.completed[i] {
			continue
		}
		endpoint := r.invocation.endpoints[part.certificate.scope.partition]
		if !r.born {
			// Recovery has a new, bounded invocation for authentication. Original
			// owners do not need ordinary read credit to spend their prepaid terminal
			// exchange after an uncertain/exhausted publication attempt.
			observed, readErr := endpoint.retainedPinRead(ctx, r.manifest)
			if readErr != nil {
				combined = errors.Join(combined, readErr)
				continue
			}
			if observed.owner.terminal != 0 {
				r.completed[i] = true
				continue
			}
			if observed.result.index == 0 || observed.owner.publication == 0 {
				combined = errors.Join(combined, replica.ErrUnavailable)
				continue
			}
		}
		result, err := endpoint.submitRetainedPin(ctx, pinCommandFor(manifest, r.manifest, endpoint.peer.scope, phase, pinTerminalAction(manifest, r.manifest, phase), 0), r.invocation.cleanup[part.certificate.scope.partition])
		if err != nil {
			combined = errors.Join(combined, err)
			continue
		}
		if result.publication == 0 || result.status != pinApplied && result.status != pinReleased && result.status != pinExpired {
			combined = errors.Join(combined, replica.ErrUnavailable)
			continue
		}
		r.completed[i] = true
	}
	if combined != nil {
		return errors.Join(errRetainedPinPending, combined)
	}
	r.closed = true
	return nil
}
func (r *retainedPinRequest) AdvanceFeed(ctx context.Context, positions []uint64, action [16]byte) error {
	if r == nil || r.self != r || ctx == nil || action == ([16]byte{}) {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return errGraphPinReleased
	}
	manifest := r.definition
	if len(positions) != len(manifest.parts) || action == manifest.request {
		return errInvalid
	}
	for _, position := range positions {
		if position == 0 {
			return errInvalid
		}
	}
	for i, part := range manifest.parts {
		endpoint := r.invocation.endpoints[part.certificate.scope.partition]
		result, err := endpoint.submitRetainedPin(ctx, pinCommandFor(manifest, r.manifest, endpoint.peer.scope, pinAdvanceFeed, action, positions[i]), r.invocation.budget)
		if err != nil {
			return errors.Join(errRetainedPinPending, err)
		}
		if result.status == pinExpired {
			return errGraphPinExpired
		}
		if result.status != pinApplied {
			return errors.Join(errRetainedPinPending, replica.ErrUnavailable)
		}
	}
	return nil
}
func (p *retainedPinPublisher) AdvanceEpoch(ctx context.Context, issuer, action [16]byte, expected uint64) error {
	if !p.valid() || ctx == nil || issuer == ([16]byte{}) || action == ([16]byte{}) || expected >= math.MaxUint64-1 {
		return errInvalid
	}
	if p.coordinator.host.issuerUnavailable() {
		return replica.ErrStopped
	}
	invocation, err := p.invocation()
	if err != nil {
		return err
	}
	for i := range p.coordinator.host.machine.declaration.Len() {
		part, _ := p.coordinator.host.machine.declaration.PartitionAt(i)
		endpoint := invocation.endpoints[part.Partition]
		result, err := endpoint.submitRetainedPin(ctx, graphPinCommand{scope: endpoint.peer.scope, issuer: issuer, action: action, phase: pinAdvanceEpoch, expectedEpoch: expected, epoch: expected + 1}, invocation.budget)
		if err != nil {
			return errors.Join(errRetainedPinPending, err)
		}
		if result.status != pinApplied || result.epoch != expected+1 {
			return errors.Join(errRetainedPinPending, replica.ErrUnavailable)
		}
	}
	return nil
}

type retainedPinReadBackend struct {
	*remoteCertifiedGraphBackend
	manifest     []byte
	feedLifetime *retainedFeedSnapshotLifetime
}

func (b *retainedPinReadBackend) Capture(ctx context.Context, cert graphCertificate) (certifiedGraphSource, genesisAllocationConfig, materializerLimits, error) {
	endpoint := b.endpoints[cert.scope.partition]
	if endpoint == nil {
		return nil, genesisAllocationConfig{}, materializerLimits{}, errGraphCutScope
	}
	opened, cfg, err := endpoint.openRetainedPinSource(ctx, b.manifest)
	var source certifiedGraphSource = opened
	if err == nil && b.feedLifetime != nil {
		source = &retainedFeedSnapshotSource{certifiedGraphSource: source, lifetime: b.feedLifetime}
	}
	return source, cfg, b.coordinator.host.machine.limits, err
}
func (p *retainedPinPublisher) Open(ctx context.Context, token retainedPinToken) (*certifiedGraphRead, error) {
	if !p.valid() || ctx == nil {
		return nil, errInvalid
	}
	manifest, err := decodeGraphPinManifest(token.manifest)
	if err != nil {
		return nil, err
	}
	if _, err := encodeRetainedPinToken(token); err != nil {
		return nil, err
	}
	if err := p.coordinator.host.machine.bindPinManifest(manifest); err != nil {
		return nil, err
	}
	invocation, err := p.invocation()
	if err != nil {
		return nil, err
	}
	return p.openWithInvocation(ctx, token, manifest, invocation, nil)
}

// The feed door supplies its already durably admitted operation ledger and
// lifetime. Ordinary Open above retains exactly its existing default recipe.
func (p *retainedPinPublisher) openWithInvocation(ctx context.Context, token retainedPinToken, manifest graphPinManifest, invocation *retainedPinInvocation, lifetime *retainedFeedSnapshotLifetime) (*certifiedGraphRead, error) {
	if !p.valid() || ctx == nil || invocation == nil || invocation.arena == nil || invocation.budget == nil || len(manifest.parts) == 0 || len(token.publications) != len(manifest.parts) || len(token.manifest) == 0 {
		return nil, errInvalid
	}
	if err := invocation.arena.Reserve(4096 + 1024*len(manifest.parts)); err != nil {
		return nil, outputBudgetFailure(err)
	}
	cut := graphCut{certificates: make([]graphCertificate, len(manifest.parts))}
	for i, part := range manifest.parts {
		observed, err := invocation.endpoints[part.certificate.scope.partition].retainedPinRead(ctx, token.manifest)
		if err != nil {
			return nil, errors.Join(errRetainedPinPending, err)
		}
		if err := retainedPinLive(observed); err != nil {
			return nil, err
		}
		if observed.owner.publication != token.publications[i] {
			return nil, errInvalid
		}
		cut.certificates[i] = part.certificate
	}
	c := p.coordinator
	engine := &certifiedReadCoordinator{declaration: c.host.machine.declaration}
	backend := &retainedPinReadBackend{remoteCertifiedGraphBackend: &remoteCertifiedGraphBackend{coordinator: c, endpoints: invocation.endpoints, budget: invocation.budget, arena: invocation.arena}, manifest: token.manifest, feedLifetime: lifetime}
	return engine.openBackend(ctx, graphReadRequest{mode: graphReadAt, cut: cut}, nil, backend)
}

var _ certifiedGraphBackend = (*retainedPinReadBackend)(nil)

// An interrupted prepass has no native issuance yet. Only the genuine held
// initial/final native sources can admit repeating that prepass; a journal copy
// or decoded GPM alone cannot mark a recovered request born.
func (r *retainedPinRequest) bindUnissuedColumnSource(source *nativeColumnSource) error {
	if r == nil || r.self != r || source == nil {
		return errInvalid
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	source.mu.Lock()
	defer source.mu.Unlock()
	if err := source.check(source.ctx); err != nil {
		return err
	}
	if r.closed || r.born || len(r.definition.parts) != 2 || source.binding.Version != 2 || !durableColumnGraphAgreement(r.definition.publisher.semantic) {
		return errInvalid
	}
	for g, part := range r.definition.parts {
		if part.certificate != source.initial[g] || source.initial[g].sourceIndex > source.final[g].sourceIndex {
			return errInvalid
		}
	}
	r.born = true
	return nil
}
