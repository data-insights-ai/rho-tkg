package graphapply

import (
	"context"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

// Operations here are completed native/authenticated protocol operations. Data
// sources supply no decision/cut authority. Both transports use one cut engine.
type certifiedGraphSource interface {
	graphstore.ApplicationReadSource
	Close() error
}
type certifiedGraphBackend interface {
	Read(context.Context, uint64, graphCutQuery) (graphCutObservation, error)
	Write(context.Context, uint64, byte, uint64) (graphCutResult, error)
	Resolve(context.Context, graphCutObservation) error
	Capture(context.Context, graphCertificate) (certifiedGraphSource, genesisAllocationConfig, materializerLimits, error)
	ReceiptRound(context.Context, graphCommitReceipt) (uint64, error)
}

type nativeCertifiedGraphBackend struct{ coordinator *certifiedReadCoordinator }

func (b nativeCertifiedGraphBackend) Read(ctx context.Context, partition uint64, q graphCutQuery) (graphCutObservation, error) {
	c := b.coordinator
	h, err := c.host(ctx, partition)
	if err != nil {
		return graphCutObservation{}, err
	}
	nonce, err := graphReadNonce()
	if err != nil {
		return graphCutObservation{}, err
	}
	q.nonce = nonce
	r, err := c.transport.Read(ctx, h, q)
	if err != nil || r.err != nil {
		return graphCutObservation{}, errors.Join(err, r.err)
	}
	if r.question != q || r.id != r.proof.id || r.proof.owner != h {
		return graphCutObservation{}, errInvalid
	}
	if err := checkGraphCutProof(r.proof, q); err != nil {
		return graphCutObservation{}, err
	}
	return r.proof.observation, nil
}
func (b nativeCertifiedGraphBackend) Write(ctx context.Context, partition uint64, phase byte, round uint64) (graphCutResult, error) {
	c := b.coordinator
	h, err := c.host(ctx, partition)
	if err != nil {
		return graphCutResult{}, err
	}
	p, err := h.prepareGraphCut(round, phase)
	if err != nil {
		return graphCutResult{}, err
	}
	return c.transport.Submit(ctx, h, p)
}
func (b nativeCertifiedGraphBackend) Resolve(ctx context.Context, o graphCutObservation) error {
	return b.coordinator.resolvePending(ctx, o)
}
func (b nativeCertifiedGraphBackend) Capture(ctx context.Context, cert graphCertificate) (source certifiedGraphSource, cfg genesisAllocationConfig, l materializerLimits, err error) {
	h, err := b.coordinator.host(ctx, cert.scope.partition)
	if err != nil {
		return nil, cfg, l, err
	}
	source, err = h.AtGraphCertificate(ctx, cert)
	if err != nil {
		return nil, cfg, l, err
	}
	cfg, err = h.currentConfiguration()
	if err != nil {
		err = errors.Join(err, source.Close())
		source = nil
	}
	return source, cfg, h.machine.limits, err
}
func (b nativeCertifiedGraphBackend) ReceiptRound(ctx context.Context, r graphCommitReceipt) (uint64, error) {
	return b.coordinator.receiptRound(ctx, r)
}

func freshCertifiedGraphCut(ctx context.Context, backend certifiedGraphBackend, scope []uint64, after uint64) (graphCut, error) {
	bound := max(uint64(1), after)
	for _, partition := range scope {
		o, err := backend.Read(ctx, partition, graphCutQuery{kind: 1})
		if err != nil {
			return graphCut{}, err
		}
		bound = max(bound, o.floor)
		if o.pending != ([16]byte{}) {
			if err := backend.Resolve(ctx, o); err != nil {
				return graphCut{}, err
			}
			o, err = backend.Read(ctx, partition, graphCutQuery{kind: 1})
			if err != nil {
				return graphCut{}, err
			}
			bound = max(bound, o.floor)
		}
	}
	if bound == math.MaxUint64 {
		return graphCut{}, errLimit
	}
	for _, partition := range scope {
		r, err := backend.Write(ctx, partition, graphCutFence, bound)
		if err != nil {
			return graphCut{}, err
		}
		if r.phase != graphCutFence || r.round != bound || r.index == 0 {
			return graphCut{}, errInvalid
		}
		if err := graphResultError(r.status); err != nil {
			return graphCut{}, err
		}
	}
	cut := graphCut{certificates: make([]graphCertificate, len(scope))}
	for i, partition := range scope {
		o, err := backend.Read(ctx, partition, graphCutQuery{kind: 1})
		if err != nil {
			return graphCut{}, err
		}
		if err := backend.Resolve(ctx, o); err != nil {
			return graphCut{}, err
		}
		r, err := backend.Write(ctx, partition, graphCutCertify, bound)
		if err != nil {
			return graphCut{}, err
		}
		if r.phase != graphCutCertify || r.round != bound || r.index == 0 {
			return graphCut{}, errInvalid
		}
		if err := graphResultError(r.status); err != nil {
			return graphCut{}, err
		}
		o, err = backend.Read(ctx, partition, graphCutQuery{kind: 2, round: bound})
		if err != nil {
			return graphCut{}, err
		}
		cut.certificates[i] = o.certificate
	}
	return cut, nil
}

func (c *certifiedReadCoordinator) openBackend(ctx context.Context, request graphReadRequest, scope []uint64, backend certifiedGraphBackend) (read *certifiedGraphRead, err error) {
	if c == nil || ctx == nil || backend == nil {
		return nil, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	scope, err = certifiedScope(c.declaration, scope)
	if err != nil {
		return nil, err
	}
	cut := request.cut
	switch request.mode {
	case graphReadAt:
	case graphReadStable:
		if len(cut.certificates) == 0 {
			c.mu.Lock()
			cut = c.stable
			c.mu.Unlock()
			if len(cut.certificates) == 0 {
				return nil, replica.ErrUnavailable
			}
		}
	case graphReadFresh, graphReadAfter:
		after := uint64(0)
		if request.mode == graphReadAfter {
			after, err = backend.ReceiptRound(ctx, request.receipt)
			if err != nil {
				return nil, err
			}
		}
		cut, err = freshCertifiedGraphCut(ctx, backend, scope, after)
		if err != nil {
			return nil, err
		}
	default:
		return nil, errInvalid
	}
	if len(cut.certificates) != len(scope) {
		return nil, errGraphCutScope
	}
	sources := make([]certifiedGraphSource, len(scope))
	var full *graphstore.PartitionTransactionView
	defer func() {
		if err != nil {
			if full != nil {
				err = errors.Join(err, full.Close())
			}
			for _, source := range sources {
				if source != nil {
					err = errors.Join(err, source.Close())
				}
			}
			read = nil
		}
	}()
	var cfg genesisAllocationConfig
	var limits materializerLimits
	for i, cert := range cut.certificates {
		if cert.scope.partition != scope[i] || cert.scope.declaration != c.declaration.Digest() || graphstate.GraphID(cert.scope.graph) != c.declaration.Graph() || i > 0 && (cert.round != cut.certificates[0].round || cert.configuration != cut.certificates[0].configuration) {
			return nil, errGraphCutScope
		}
		var capture genesisAllocationConfig
		var child materializerLimits
		sources[i], capture, child, err = backend.Capture(ctx, cert)
		if err != nil {
			return nil, err
		}
		config, e := capture.digest()
		if e != nil || config != cert.configuration {
			return nil, errors.Join(errInvalid, e)
		}
		if i == 0 {
			cfg, limits = capture, child
		} else {
			limits = intersectCertifiedReadLimits(limits, child)
		}
	}
	protocol := graphstore.PageWork{}
	var arena *graphstate.OutputBudget
	if remote, ok := backend.(interface {
		operationBudget() (*graphstate.OutputBudget, graphstore.PageWork)
	}); ok {
		arena, protocol = remote.operationBudget()
	} else {
		arena, err = graphstate.NewOutputBudget(limits.outputBytes)
	}
	if err != nil {
		return nil, err
	}
	retained := graphReadSessionBytes + 4096*len(sources)
	if err := arena.Reserve(retained); err != nil {
		return nil, outputBudgetFailure(err)
	}
	work := graphstore.PageWork{Records: 8*len(sources) + protocol.Records, Bytes: retained + protocol.Bytes}
	budget := graphstore.OwnershipBudget{SourceRows: limits.sourceRows - work.Records, SourceBytes: limits.sourceBytes - work.Bytes, OutputBytes: min(limits.outputBytes-retained, arena.Remaining())}
	data := make([]graphstore.ApplicationReadSource, len(sources))
	for i, source := range sources {
		data[i] = source
	}
	full, _, err = graphstore.OpenPartitionScopedReadSources(ctx, data, scope, c.declaration, cfg.defaultAxis, cut.certificates[0].configuration, limits.catalog, limits.graph, budget, arena)
	if err != nil {
		return nil, err
	}
	read = &certifiedGraphRead{cut: graphCut{certificates: slices.Clone(cut.certificates)}, full: full, sources: sources, initial: work}
	if request.mode == graphReadFresh || request.mode == graphReadAfter {
		c.mu.Lock()
		c.stable = graphCut{certificates: slices.Clone(cut.certificates)}
		c.mu.Unlock()
	}
	return read, nil
}
