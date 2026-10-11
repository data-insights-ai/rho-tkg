package graphapply

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

var errCrossProcessPending = errors.New("graphapply: cross-process request pending; resume original identity")

// The fixed resume identity is not success, absence or decision authority.
type crossProcessResume struct {
	scope    allocationScope
	request  requestID
	tx       [16]byte
	business [32]byte
}
type crossProcessResult struct {
	resume   crossProcessResume
	decision crossStatus
}
type crossProcessRun struct {
	resume                crossProcessResume
	tx                    crossTransaction
	busy                  bool
	registrationAttempted bool
}
type crossProcessCoordinator struct {
	mu         sync.Mutex
	host       *allocationHost
	transports map[uint64]graphPeerWireTransport
	streams    map[uint64]*graphAuthenticatedStream
	runs       map[[16]byte]*crossProcessRun
	certified  *certifiedReadCoordinator
}

func newCrossProcessCoordinator(h *allocationHost, transports map[uint64]graphPeerWireTransport) (*crossProcessCoordinator, error) {
	if h == nil || h.machine == nil || !crossGraphAgreement(h.machine.SemanticContractID()) || len(transports) != h.machine.declaration.Len() {
		return nil, errInvalid
	}
	owned := make(map[uint64]graphPeerWireTransport, len(transports))
	for i := range h.machine.declaration.Len() {
		part, _ := h.machine.declaration.PartitionAt(i)
		transport := transports[part.Partition]
		if transport == nil {
			return nil, errInvalid
		}
		peer := transport.Peer()
		if peer.scope != mustProcessScope(h, part.Partition) || peer.voter == 0 {
			return nil, errInvalid
		}
		owned[part.Partition] = transport
	}
	streams := make(map[uint64]*graphAuthenticatedStream, len(owned))
	for partition := range owned {
		streams[partition] = &graphAuthenticatedStream{mu: newGraphRequestGate()}
	}
	return &crossProcessCoordinator{host: h, transports: owned, streams: streams, runs: make(map[[16]byte]*crossProcessRun)}, nil
}
func mustProcessScope(h *allocationHost, partition uint64) allocationScope {
	scope, _ := h.target(partition)
	return scope
}

func (c *crossProcessCoordinator) business(id requestID, ops []graphstate.Operation, provenance uint64, claims []freshBinding) ([]byte, error) {
	h := c.host
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return nil, errInvalid
	}
	cfg, err := h.currentConfiguration()
	if err != nil {
		return nil, err
	}
	configuration, err := cfg.digest()
	if err != nil {
		return nil, err
	}
	revision, err := state.NewRevision(1, provenance)
	if err != nil {
		return nil, err
	}
	return encodePartitionGraphCommand(partitionGraphCommand{configuration: configuration, declaration: h.machine.declaration.Digest(), routing: cfg.routingDigest, request: graphRequest{ns: h.machine.ns, kind: graphOperations, id: id, revision: revision, operations: ops, claims: claims}}, h.machine.limits)
}
func (c *crossProcessCoordinator) endpoints(budget *graphRemoteBudget) (map[uint64]*graphAuthenticatedEndpoint, error) {
	endpoints := make(map[uint64]*graphAuthenticatedEndpoint, len(c.transports))
	for partition, transport := range c.transports {
		endpoint, err := newGraphAuthenticatedEndpoint(c.host.machine.declaration, transport, budget)
		if err != nil {
			return nil, err
		}
		endpoint.stream = c.streams[partition]
		endpoints[partition] = endpoint
	}
	return endpoints, nil
}
func (c *crossProcessCoordinator) matchBusiness(manifest []byte, wanted []byte, arena *graphstate.OutputBudget) error {
	m, err := decodeCrossManifest(manifest, c.host.machine.limits, arena)
	if err != nil {
		return err
	}
	revision, err := state.NewRevision(1, m.request.revision.Provenance())
	if err != nil {
		return err
	}
	m.request.revision = revision
	encoded, err := encodePartitionGraphCommand(partitionGraphCommand{configuration: m.configuration, declaration: m.declaration, routing: m.routing, request: m.request}, c.host.machine.limits)
	if err != nil {
		return err
	}
	if !bytes.Equal(encoded, wanted) {
		return errInvalid
	}
	return nil
}

// Run never holds a Host event mutex across transport or source planning. Same
// qualified requests coalesce; unknown registration retains its exact original
// bytes even if an applied quorum lookup presently reports absence.
func (c *crossProcessCoordinator) Run(ctx context.Context, id requestID, ops []graphstate.Operation, provenance uint64, claims []freshBinding) (result crossProcessResult, err error) {
	if c == nil || c.host == nil || c.host.machine == nil || ctx == nil || id == (requestID{}) {
		return result, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	business, err := c.business(id, ops, provenance, claims)
	if err != nil {
		return result, err
	}
	l := c.host.machine.limits
	output, err := graphstate.NewOutputBudget(l.outputBytes)
	if err != nil {
		return result, err
	}
	// Conservative shared source/output child: network, physical reads, decode,
	// staging and private templates cannot each spend a fresh source allowance.
	arena, err := output.Child(min(l.sourceBytes, l.outputBytes))
	if err != nil {
		return result, err
	}
	if err := arena.Reserve(1024 + 8*len(business)); err != nil {
		return result, outputBudgetFailure(err)
	}
	resume := crossProcessResume{scope: c.host.machine.scope(), request: id, tx: crossTransactionID(c.host.machine.ns, id), business: sha256.Sum256(business)}
	result.resume = resume
	c.mu.Lock()
	run := c.runs[resume.tx]
	if run != nil && run.resume != resume {
		c.mu.Unlock()
		return result, errInvalid
	}
	if run != nil && run.busy {
		c.mu.Unlock()
		return result, errCrossProcessPending
	}
	if run == nil {
		if len(c.runs) >= 2 {
			c.mu.Unlock()
			return result, errLimit
		}
		run = &crossProcessRun{resume: resume}
		c.runs[resume.tx] = run
	}
	run.busy = true
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		run.busy = false
		if err != nil && run.registrationAttempted {
			err = errors.Join(errCrossProcessPending, err)
		}
		if err == nil || !run.registrationAttempted {
			delete(c.runs, resume.tx)
		}
		c.mu.Unlock()
	}()
	budget := newGraphRemoteBudget(l, arena)
	endpoints, err := c.endpoints(budget)
	if err != nil {
		return result, err
	}
	coordinator := endpoints[c.host.machine.ns.partition]
	lookup, err := coordinator.lookup(ctx, resume.tx)
	if err != nil {
		return result, errors.Join(errCrossProcessPending, err)
	}
	if lookup.present {
		if err := c.matchBusiness(lookup.manifest, business, arena); err != nil {
			return result, err
		}
		if len(run.tx.manifest) != 0 && !bytes.Equal(run.tx.manifest, lookup.manifest) {
			return result, errInvalid
		}
		run.tx = crossTransaction{owner: c.host, manifest: owned(lookup.manifest), digest: lookup.status.digest}
		run.registrationAttempted = true
	} else if run.registrationAttempted && len(run.tx.manifest) == 0 {
		return result, errCrossProcessPending
	}
	if len(run.tx.manifest) == 0 {
		sources := make([]*remoteGraphSource, 0, c.host.machine.declaration.Len())
		defer func() {
			for _, source := range sources {
				err = errors.Join(err, source.Close())
			}
		}()
		captured := make([]capturedGraphSource, c.host.machine.declaration.Len())
		for i := range captured {
			part, _ := c.host.machine.declaration.PartitionAt(i)
			source, e := endpoints[part.Partition].openSource(ctx)
			if e != nil {
				return result, e
			}
			sources = append(sources, source)
			captured[i] = capturedGraphSource{observation: source.info.observation, root: source.info.root, floor: source.info.floor, data: source}
		}
		manifest, e := c.host.planCrossCaptured(ctx, id, ops, provenance, claims, captured, arena, graphstore.PageWork{Records: 8 * len(captured), Bytes: 4096 * len(captured)})
		if e != nil {
			return result, e
		}
		for _, source := range sources {
			if e := source.Close(); e != nil {
				return result, e
			}
		}
		sources = nil
		run.tx = crossTransaction{owner: c.host, manifest: manifest, digest: sha256.Sum256(manifest)}
	}
	m, err := decodeCrossManifest(run.tx.manifest, l, arena)
	if err != nil {
		return result, err
	}
	// Protect the finite remaining control path before any durable YES. This
	// admitted pool is independent of source sessions and includes every part.
	completion, err := budget.protectCompletion(len(m.parts), len(run.tx.manifest))
	if err != nil {
		return result, err
	}
	arena = completion
	if !lookup.present {
		proposal, e := c.host.proposalFromAuthority(crossRegister, m.coordinator, run.tx, nil, arena)
		if e != nil {
			return result, fmt.Errorf("registration proposal: %w", e)
		}
		run.registrationAttempted = true
		status, e := coordinator.submit(ctx, proposal)
		if e != nil {
			return result, errors.Join(errCrossProcessPending, e)
		}
		if status.tx != m.tx || status.digest != m.digest {
			return result, errInvalid
		}
	}
	registration, err := coordinator.cross(ctx, m.tx, crossRegister, len(run.tx.manifest))
	if err != nil {
		return result, errors.Join(errCrossProcessPending, err)
	}
	var decision crossAuthority
	if lookup.present && lookup.status.phase >= crossDecide {
		decision, err = coordinator.cross(ctx, m.tx, crossDecide, len(run.tx.manifest))
	} else {
		votes := make([]crossAuthority, 0, len(m.parts))
		for _, part := range m.parts {
			evidence := append([]crossAuthority{registration}, votes...)
			proposal, e := c.host.proposalFromAuthority(crossPrepare, part.binding.Partition, run.tx, evidence, arena)
			if e != nil {
				return result, fmt.Errorf("prepare proposal partition%d: %w", part.binding.Partition, e)
			}
			status, e := endpoints[part.binding.Partition].submit(ctx, proposal)
			if e != nil {
				return result, errors.Join(errCrossProcessPending, e)
			}
			if status.tx != m.tx || status.digest != m.digest {
				return result, errInvalid
			}
			vote, e := endpoints[part.binding.Partition].cross(ctx, m.tx, crossPrepare, len(run.tx.manifest))
			if e != nil {
				return result, errors.Join(errCrossProcessPending, e)
			}
			votes = append(votes, vote)
			if !vote.status.yes {
				break
			}
		}
		proposal, e := c.host.proposalFromAuthority(crossDecide, m.coordinator, run.tx, votes, arena)
		if e != nil {
			return result, fmt.Errorf("decision proposal: %w", e)
		}
		if _, e := coordinator.submit(ctx, proposal); e != nil {
			return result, errors.Join(errCrossProcessPending, e)
		}
		decision, err = coordinator.cross(ctx, m.tx, crossDecide, len(run.tx.manifest))
	}
	if err != nil {
		return result, errors.Join(errCrossProcessPending, err)
	}
	for _, part := range m.parts {
		if !decision.status.commit {
			participant, e := endpoints[part.binding.Partition].lookup(ctx, m.tx)
			if e != nil {
				return result, e
			}
			if participant.configuration != m.configuration {
				return result, errInvalid
			}
			if !participant.present {
				continue
			}
			if participant.status.digest != m.digest || !bytes.Equal(participant.manifest, run.tx.manifest) {
				return result, errInvalid
			}
			if participant.status.phase == crossResolve {
				continue
			}
		}
		proposal, e := c.host.proposalFromAuthority(crossResolve, part.binding.Partition, run.tx, []crossAuthority{decision}, arena)
		if e != nil {
			return result, fmt.Errorf("resolve proposal partition%d: %w", part.binding.Partition, e)
		}
		status, e := endpoints[part.binding.Partition].submit(ctx, proposal)
		if e != nil {
			return result, errors.Join(errCrossProcessPending, e)
		}
		if status.tx != m.tx || status.digest != m.digest || status.phase != crossResolve || status.commit != decision.status.commit || status.round != decision.status.round {
			return result, errInvalid
		}
	}
	result.decision = decision.status
	return result, nil
}

type crossProcessRead struct {
	full    *graphstore.PartitionTransactionView
	sources []*remoteGraphSource
}

func (r *crossProcessRead) ReadView() graphstate.ReadView {
	if r == nil {
		return nil
	}
	return r.full
}
func (r *crossProcessRead) Close() error {
	if r == nil {
		return errInvalid
	}
	var err error
	if r.full != nil {
		err = r.full.Close()
	}
	for _, s := range r.sources {
		err = errors.Join(err, s.Close())
	}
	return err
}

// OpenQuiescentSources binds actual per-source quorum positions. It is explicitly
// NOT a certified cut; the finite process acceptance calls it after all resolves.
func (c *crossProcessCoordinator) OpenQuiescentSources(ctx context.Context) (read *crossProcessRead, err error) {
	if c == nil || c.host == nil || c.host.machine == nil || ctx == nil {
		return nil, errInvalid
	}
	l := c.host.machine.limits
	output, err := graphstate.NewOutputBudget(min(l.outputBytes, l.sourceBytes))
	if err != nil {
		return nil, err
	}
	endpoints, err := c.endpoints(newGraphRemoteBudget(l, output))
	if err != nil {
		return nil, err
	}
	read = &crossProcessRead{sources: make([]*remoteGraphSource, 0, c.host.machine.declaration.Len())}
	defer func() {
		if err != nil {
			err = errors.Join(err, read.Close())
			read = nil
		}
	}()
	data := make([]graphstore.ApplicationReadSource, c.host.machine.declaration.Len())
	for i := range data {
		part, _ := c.host.machine.declaration.PartitionAt(i)
		source, e := endpoints[part.Partition].openSource(ctx)
		if e != nil {
			return read, e
		}
		read.sources = append(read.sources, source)
		data[i] = source
	}
	first := read.sources[0].info.observation
	read.full, _, err = graphstore.OpenPartitionTransactionSources(ctx, data, c.host.machine.declaration, first.config.defaultAxis, first.configuration, l.catalog, l.graph, graphstore.OwnershipBudget{SourceRows: l.sourceRows, SourceBytes: l.sourceBytes, OutputBytes: output.Remaining()}, output)
	return read, err
}
