package graphstore

import (
	"bytes"
	"context"
	"errors"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// BootstrapBoundOwnership installs only a fresh, semantic-bound pending seed.
// The exact configured membership and immutable declaration identify local
// placement; they supply neither allocator readiness nor a distributed lease.
func BootstrapBoundOwnership(s *raftlog.Store, expected raftlog.ApplicationBinding, declaration OwnershipDeclaration, voters [3]uint64) error {
	if s == nil {
		return ErrInvalid
	}
	p := s.ApplicationLimits()
	if !p.Enabled() {
		return ErrTopologyUnsupported
	}
	if err := expected.Validate(); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	actual := s.ApplicationBinding()
	if err := actual.Validate(); err != nil {
		return errors.Join(ErrInvalid, err)
	}
	if actual.Identity.Graph != expected.Identity.Graph || actual.Identity.Partition != expected.Identity.Partition {
		return ErrNamespace
	}
	if actual != expected || !declaration.valid() || ownershipDigest(declaration.graph, declaration.epoch, declaration.entries) != declaration.digest || voters[0] == 0 || voters[1] <= voters[0] || voters[2] <= voters[1] || !slices.Contains(voters[:], p.LocalVoter) {
		return ErrInvalid
	}
	r, err := NewOwnershipRoot(Namespace{actual.Identity.Graph, actual.Identity.Partition}, declaration)
	if err != nil {
		return err
	}
	if err := checkOwnershipEntry(r, actual, declaration); err != nil {
		return err
	}
	image, err := EncodeRoot(r)
	if err != nil {
		return err
	}
	if err := s.Initialize(voters[:], image); err != nil {
		if errors.Is(err, raftlog.ErrInvalid) {
			return errors.Join(ErrInvalid, err)
		}
		return ownershipOperational(err)
	}
	return nil
}

const partitionProofScratchBytes = 512

// CheckDeclaredPartitionSeed proves the current pending empty or published
// declaration-only bank against the exact immutable declaration and view binding.
// It returns the original captured base and seed, not readiness, graph coverage,
// allocator/grant authority or a lease. It allocates no physical index roots;
// failures return zero outputs and all consumed work, leaving the borrow open.
func CheckDeclaredPartitionSeed(ctx context.Context, view *raftlog.ApplicationView, declaration OwnershipDeclaration, limits Limits, budget OwnershipBudget) (base raftlog.ApplicationRoot, root Root, work PageWork, err error) {
	base, root, work, _, err = checkDeclaredPartitionSeed(ctx, view, declaration, limits, budget)
	return base, root, work, err
}

func checkDeclaredPartitionSeed(ctx context.Context, view *raftlog.ApplicationView, declaration OwnershipDeclaration, limits Limits, budget OwnershipBudget) (base raftlog.ApplicationRoot, root Root, work PageWork, expected raftlog.KV, err error) {
	q, base, root, binding, err := startOwnershipReader(ctx, view, limits, budget)
	if err != nil {
		return raftlog.ApplicationRoot{}, Root{}, ownershipWork(q), raftlog.KV{}, err
	}
	if err = q.output(ownershipMetadataBytes + ownershipRootBytes); err != nil {
		return raftlog.ApplicationRoot{}, Root{}, q.work, raftlog.KV{}, err
	}
	defer func() {
		if err != nil {
			base, root, expected = raftlog.ApplicationRoot{}, Root{}, raftlog.KV{}
		}
	}()
	if root.ownershipMode == ownershipInitialized {
		return base, root, q.work, expected, ErrTopologyUnsupported
	}
	if len(declaration.entries) > (q.limits.MaxRecordBytes-ownershipRecordFixedBytes)/ownershipEntryBytes {
		return base, root, q.work, expected, ErrResourceLimit
	}
	if err = q.charge(partitionProofScratchBytes + ownershipEntryBytes*len(declaration.entries)); err != nil {
		return base, root, q.work, expected, err
	}
	if !declaration.valid() || ownershipDigest(declaration.graph, declaration.epoch, declaration.entries) != declaration.digest {
		return base, root, q.work, expected, ErrInvalid
	}
	if declaration.digest != root.ownershipDigest {
		return base, root, q.work, expected, ErrRebinding
	}
	if err = checkOwnershipEntry(root, binding, declaration); err != nil {
		return base, root, q.work, expected, err
	}
	seed, err := NewOwnershipRoot(root.namespace, declaration)
	if err != nil {
		return base, root, q.work, expected, err
	}
	seed.ownershipMode = root.ownershipMode
	if root != seed {
		return base, root, q.work, expected, ErrTopologyUnsupported
	}
	actual, found, err := q.readDeclaration(root)
	if err != nil {
		return base, root, q.work, expected, err
	}
	if root.ownershipMode == ownershipPending && found || root.ownershipMode == ownershipPublished && (!found || actual.digest != declaration.digest) {
		return base, root, q.work, expected, ErrCorrupt
	}
	// Exact expected backing is admitted before encoding. It coexists with the
	// declaration read and backend proof's independently charged frame read.
	recordBytes := ownershipRecordFixedBytes + ownershipEntryBytes*len(declaration.entries)
	if err = q.charge(ownershipKeyBytes + recordBytes + 64); err != nil {
		return base, root, q.work, expected, err
	}
	wire, err := encodeOwnershipDeclaration(declaration, q.limits)
	if err != nil {
		return base, root, q.work, expected, err
	}
	expected = raftlog.KV{Key: ownershipKey(root.namespace.Graph, root.topology.epoch, root.ownershipDigest), Value: wire}
	var proof raftlog.ApplicationRoot
	if root.ownershipMode == ownershipPending {
		if err = q.charge(ownershipRootBytes); err != nil {
			return base, root, q.work, expected, err
		}
		proof, err = view.ProveNoApplicationData(ctx)
	} else {
		if q.work.Records >= q.budget.SourceRows {
			return base, root, q.work, expected, ErrResourceLimit
		}
		var used raftlog.ApplicationProofWork
		proof, used, err = view.ProveOnlyApplicationKV(ctx, expected, ownershipRootBytes, min(q.budget.SourceBytes-q.work.Bytes, view.ReadLimits().Bytes))
		q.work.Records += used.Records
		if chargeErr := q.charge(used.Bytes); chargeErr != nil {
			return base, root, q.work, expected, errors.Join(err, chargeErr)
		}
	}
	if err != nil {
		return base, root, q.work, expected, ownershipOperational(err)
	}
	if proof.Generation != base.Generation || proof.Index != base.Index || proof.ImageHash != base.ImageHash || !bytes.Equal(proof.Image, base.Image) {
		return base, root, q.work, expected, ErrInvalid
	}
	return base, root, q.work, expected, nil
}

// InitializeDeclaredPartition computes local schema and five real empty index
// roots after proving a pending empty bank or a published declaration-only bank.
// It installs nothing, preserves the original base and does not advance logical
// effects or grant complete graph coverage. The outer materializer co-composes
// allocator/configuration/request/CDC effects and performs one installation.
func InitializeDeclaredPartition(ctx context.Context, view *raftlog.ApplicationView, declaration OwnershipDeclaration, schemas []graphstate.PropertyDefinition, limits Limits, graphLimits GraphLimits, budget OwnershipBudget) (effects GraphEffects, work PageWork, err error) {
	gl, err := graphLimits.resolve()
	if err != nil {
		return effects, work, err
	}
	resolved, err := limits.resolve()
	if err != nil {
		return effects, work, err
	}
	if err := budget.validate(); err != nil {
		return effects, work, err
	}
	budget.SourceRows = min(budget.SourceRows, resolved.MaxReadRows, gl.MaxSourceRows, gl.Pages.MaxWorkRecords)
	budget.SourceBytes = min(budget.SourceBytes, resolved.MaxReadBytes, gl.MaxSourceBytes, gl.Pages.MaxWorkBytes)
	budget.OutputBytes = min(budget.OutputBytes, resolved.MaxReadBytes, gl.MaxOutputBytes)
	base, root, work, declarationKV, err := checkDeclaredPartitionSeed(ctx, view, declaration, resolved, budget)
	if err != nil {
		return effects, work, err
	}
	if len(schemas) > resolved.MaxStageRecords-7 {
		return effects, work, ErrResourceLimit
	}
	pending := root.ownershipMode == ownershipPending
	root.ownershipMode, root.topology.index = ownershipInitialized, 3
	c := &Catalog{view: view, root: root, rootImageBytes: ownershipRootBytes, limits: resolved, hash: equalityDigest}
	initial, err := c.reader(ctx)
	if err != nil {
		return effects, work, err
	}
	initial.maxRows, initial.maxBytes = budget.SourceRows-work.Records, budget.SourceBytes-work.Bytes
	initial.denyReads = initial.maxRows == 0
	if initial.maxBytes < ownershipRootBytes {
		return effects, work, ErrResourceLimit
	}
	if _, exists, err := initial.get(componentIndexDescriptorKey(root.namespace)); err != nil {
		return effects, addWork(work, PageWork{Records: initial.rows, Bytes: initial.bytes}), c.failure(err)
	} else if exists {
		return effects, addWork(work, PageWork{Records: initial.rows, Bytes: initial.bytes}), ErrCorrupt
	}
	prior := addWork(work, PageWork{Records: initial.rows, Bytes: initial.bytes})
	pages := gl.Pages
	gl.MaxSourceRows, gl.MaxSourceBytes = budget.SourceRows, budget.SourceBytes
	gl.MaxOutputBytes = budget.OutputBytes
	var extra []raftlog.KV
	if pending {
		extra = []raftlog.KV{declarationKV}
	}
	return initializeFullStorage(ctx, c, schemas, base, pages, gl, prior, extra)
}

// OpenPartitionCatalog validates one installed local partition's declaration,
// descriptor and physical roots. Its point APIs confer no complete graph view,
// ownership lease, allocator readiness, certified cut or mutation admission.
func OpenPartitionCatalog(ctx context.Context, view *raftlog.ApplicationView, local Namespace, ownershipEpoch uint64, limits Limits, graphLimits GraphLimits, budget OwnershipBudget) (*Catalog, PageWork, error) {
	if err := local.validate(); err != nil {
		return nil, PageWork{}, err
	}
	if ownershipEpoch == 0 {
		return nil, PageWork{}, ErrInvalid
	}
	gl, err := graphLimits.resolve()
	if err != nil {
		return nil, PageWork{}, err
	}
	if err := budget.validate(); err != nil {
		return nil, PageWork{}, err
	}
	budget.SourceRows = min(budget.SourceRows, gl.MaxSourceRows, gl.Pages.MaxWorkRecords)
	budget.SourceBytes = min(budget.SourceBytes, gl.MaxSourceBytes, gl.Pages.MaxWorkBytes)
	budget.OutputBytes = min(budget.OutputBytes, gl.MaxOutputBytes)
	q, _, root, binding, err := startOwnershipReader(ctx, view, limits, budget)
	if err != nil {
		return nil, ownershipWork(q), err
	}
	if root.namespace != local {
		return nil, q.work, ErrNamespace
	}
	if root.owner != ownershipEpoch {
		return nil, q.work, ErrStaleOwner
	}
	if root.ownershipMode != ownershipInitialized || root.epoch == 0 {
		return nil, q.work, ErrTopologyUnsupported
	}
	d, found, err := q.readDeclaration(root)
	if err != nil {
		return nil, q.work, err
	}
	if !found {
		return nil, q.work, ErrCorrupt
	}
	if err := checkOwnershipEntry(root, binding, d); err != nil {
		return nil, q.work, err
	}
	if err := q.output(ownershipMetadataBytes + fullStageMetadataBytes); err != nil {
		return nil, q.work, err
	}
	c := &Catalog{view: view, root: root, rootImageBytes: ownershipRootBytes, limits: q.limits, hash: equalityDigest}
	reader, err := c.reader(ctx)
	if err != nil {
		return nil, q.work, err
	}
	reader.maxRows, reader.maxBytes = q.budget.SourceRows-q.work.Records, q.budget.SourceBytes-q.work.Bytes
	reader.denyReads = reader.maxRows == 0
	if reader.maxBytes < ownershipRootBytes+fullStageMetadataBytes {
		return nil, q.work, ErrResourceLimit
	}
	p := &pageReader{q: reader, limits: gl.Pages}
	defer func() { _ = p.budget() }()
	if err = reader.materialize(fullStageMetadataBytes); err != nil {
		return nil, addWork(q.work, PageWork{Records: reader.rows, Bytes: reader.bytes}), err
	}
	descriptor, exists, err := reader.fullDescriptor(root)
	if err == nil && !exists {
		err = ErrCorrupt
	}
	if err == nil {
		reader.fullView = &descriptor
		err = p.validateFullRoots(descriptor)
	}
	_ = p.budget()
	work := addWork(q.work, p.work)
	if err == nil {
		err = p.budget()
	}
	if err != nil {
		return nil, work, c.failure(err)
	}
	if err := ctx.Err(); err != nil {
		return nil, work, err
	}
	c.localFull = &descriptor
	return c, work, nil
}
