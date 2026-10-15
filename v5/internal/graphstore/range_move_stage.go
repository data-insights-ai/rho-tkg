package graphstore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

func rangeMoveTargetReader(ctx context.Context, target *raftlog.ApplicationView, d RangeMoveDescriptor, prior *RangeMoveImportState, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (*pageStage, raftlog.ApplicationRoot, error) {
	if ctx == nil || target == nil || output == nil || !d.valid() || !d.Sealed || budget.SourceRows < 1 || budget.SourceBytes < 1 || budget.OutputBytes < 1 {
		return nil, raftlog.ApplicationRoot{}, ErrInvalid
	}
	if err := budget.validate(); err != nil {
		return nil, raftlog.ApplicationRoot{}, err
	}
	var err error
	l, err = l.resolve()
	if err != nil {
		return nil, raftlog.ApplicationRoot{}, err
	}
	g, err = g.resolve()
	if err != nil {
		return nil, raftlog.ApplicationRoot{}, err
	}
	arena, err := output.Child(min(g.MaxOutputBytes, budget.OutputBytes))
	if err != nil {
		return nil, raftlog.ApplicationRoot{}, callerError(err)
	}
	if err = arena.Reserve(rangeMoveFixedBytes); err != nil {
		return nil, raftlog.ApplicationRoot{}, callerError(err)
	}
	base, err := rangeMoveCheckCapture(ctx, target, d.Target, true)
	if err != nil {
		return nil, raftlog.ApplicationRoot{}, err
	}
	root, err := DecodeRoot(base.Image)
	if err != nil {
		return nil, base, err
	}
	c := &Catalog{view: target, data: rangeMoveMaintenanceSource{target}, root: root, rootImageBytes: len(base.Image), limits: l, hash: equalityDigest}
	q, err := c.readerWithOutputBudget(ctx, arena)
	if err != nil {
		return nil, base, err
	}
	q.maxRows = min(g.MaxSourceRows, g.Pages.MaxWorkRecords, budget.SourceRows)
	q.maxBytes = min(g.MaxSourceBytes, g.Pages.MaxWorkBytes, budget.SourceBytes)
	if prior == nil {
		index, found, e := q.fullDescriptor(root)
		if e != nil || !found {
			return nil, base, errors.Join(ErrCorrupt, e)
		}
		q.full = &fullStageState{root: root, descriptor: index, pages: g.Pages}
	} else {
		if prior.Cursor.Binding != d.binding() || !rangeMoveValidImportState(*prior) {
			return nil, base, ErrInvalid
		}

		want := root
		want.next = prior.FutureRoot.next
		if want != prior.FutureRoot || want.next < root.next || !validFullDescriptor(prior.index, want) {
			return nil, base, ErrInvalid
		}
		c.root = want
		q.full = &fullStageState{root: want, descriptor: prior.index, pages: g.Pages}
	}
	q.pending = make(map[string]raftlog.KV)
	p := &pageStage{pageReader: &pageReader{q: q, limits: g.Pages}, root: q.full.root}
	p.allocation = &p.root
	if err = p.validateFullRoots(q.full.descriptor); err != nil {
		return nil, base, err
	}
	return p, base, nil
}

// BeginRangeMoveImport captures the actual target's unchanged active root. The
// returned state is inert until its outer native control is durably installed.
func BeginRangeMoveImport(ctx context.Context, target *raftlog.ApplicationView, d RangeMoveDescriptor, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (RangeMoveImportState, PageWork, error) {
	p, _, err := rangeMoveTargetReader(ctx, target, d, nil, l, g, budget, output)
	if err != nil {
		return RangeMoveImportState{}, PageWork{}, err
	}
	_ = p.budget()
	c := rangeMoveStart(d)
	return RangeMoveImportState{Cursor: c, Digest: c.Digest, FutureRoot: p.root, index: p.q.full.descriptor}, p.work, nil
}

func rangeMovePageResult(p *pageStage, base raftlog.ApplicationRoot, next RangeMoveImportState) (RangeMovePageEffects, PageWork, error) {
	if err := p.budget(); err != nil {
		return RangeMovePageEffects{}, p.work, err
	}
	next.FutureRoot, next.index = p.root, p.q.full.descriptor
	cost := rangeMoveFixedBytes + len(base.Image) + 64*len(p.q.pending) + len(next.Cursor.After) + len(next.Cursor.Key) + len(next.component) + len(next.fragmentKey)
	for _, row := range p.q.pending {
		cost += len(row.Key) + len(row.Value)
	}
	if err := p.q.arena.Reserve(cost); err != nil {
		return RangeMovePageEffects{}, p.work, callerError(err)
	}
	writes := make([]raftlog.KV, 0, len(p.q.pending))
	for _, row := range p.q.pending {
		writes = append(writes, raftlog.KV{Key: exactCopy(row.Key), Value: exactCopy(row.Value)})
	}
	slices.SortFunc(writes, func(a, b raftlog.KV) int { return bytes.Compare(a.Key, b.Key) })
	base.Image = exactCopy(base.Image)
	next.Cursor = rangeMoveCloneCursor(next.Cursor)
	next.component = exactCopy(next.component)
	next.fragmentKey = exactCopy(next.fragmentKey)
	if err := p.q.ctx.Err(); err != nil {
		return RangeMovePageEffects{}, p.work, err
	}
	return RangeMovePageEffects{Base: base, Writes: writes, Next: next}, p.work, nil
}

// AdvanceRangeMoveImportPreparation retires one incoming-range current-presence
// atom per durable step. Historical catalogs and all unrelated atoms survive.
// The outer coordinator charges these steps against the same immutable page cap.
func AdvanceRangeMoveImportPreparation(ctx context.Context, target *raftlog.ApplicationView, d RangeMoveDescriptor, prior RangeMoveImportState, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (RangeMovePageEffects, PageWork, error) {
	if prior.PreparationComplete || prior.Sequence != 0 || prior.Complete || prior.Sealed || prior.PreparationSteps == math.MaxUint64 {
		return RangeMovePageEffects{}, PageWork{}, ErrInvalid
	}
	p, base, err := rangeMoveTargetReader(ctx, target, d, &prior, l, g, budget, output)
	if err != nil {
		return RangeMovePageEffects{}, PageWork{}, err
	}
	next := prior
	next.PreparationSteps++
	c := RangeMoveCursor{Family: rangeMovePresence}
	a, found, err := rangeMovePresenceNext(p.pageReader, d, &c)
	if err != nil {
		return RangeMovePageEffects{}, p.work, err
	}
	if !found {
		next.PreparationComplete = true
	} else {
		staged, e := stageCurrentPresenceInOperation(p, p.q.full.descriptor.own, []currentPresenceAtom{a}, nil, fullPresenceLimits(p.pageReader))
		if e != nil {
			return RangeMovePageEffects{}, p.work, e
		}
		p.q.full.descriptor.own = staged.tree
	}
	return rangeMovePageResult(p, base, next)
}

// StageRangeMovePage computes only the current page's bounded target writes.
// Exact native progress must be co-committed with them; it admits no caller Delta.
func StageRangeMovePage(ctx context.Context, target *raftlog.ApplicationView, d RangeMoveDescriptor, prior RangeMoveImportState, page RangeMovePage, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (RangeMovePageEffects, PageWork, error) {
	if !prior.PreparationComplete || prior.Complete || prior.Sealed || !rangeMoveValidPage(page) || !rangeMoveCursorEqual(page.Before, prior.Cursor) {
		return RangeMovePageEffects{}, PageWork{}, ErrInvalid
	}
	p, base, err := rangeMoveTargetReader(ctx, target, d, &prior, l, g, budget, output)
	if err != nil {
		return RangeMovePageEffects{}, PageWork{}, err
	}
	next, err := stageRangeMovePageIn(p, d, prior, page)
	if err != nil {
		return RangeMovePageEffects{}, p.work, err
	}
	return rangeMovePageResult(p, base, next)
}

// One private reducer serves the historical single page and fresh batches.
// The caller owns the shared stage, arena and final effects publication.
func stageRangeMovePageIn(p *pageStage, d RangeMoveDescriptor, prior RangeMoveImportState, page RangeMovePage) (RangeMoveImportState, error) {
	if !prior.PreparationComplete || prior.Complete || prior.Sealed || !rangeMoveValidPage(page) || !rangeMoveCursorEqual(page.Before, prior.Cursor) {
		return RangeMoveImportState{}, ErrInvalid
	}
	var err error
	next := prior
	if page.Kind == 0 && !page.Complete {
		if prior.fragmentOffset != 0 || !rangeMoveScanProgress(page) {
			return RangeMoveImportState{}, ErrInvalid
		}
		next.Cursor = page.Next
		next.Sequence = page.Next.Sequence
		return next, nil
	}
	if page.Complete {
		if prior.fragmentOffset > 0 || page.Next.Rows != prior.Rows || page.Next.Bytes != prior.Bytes || page.Next.Digest != prior.Digest {
			return RangeMoveImportState{}, ErrInvalid
		}
		next.Cursor = page.Next
		next.Sequence = page.Next.Sequence
		next.Complete = true
		return next, nil
	}
	if int(page.RecordBytes) > rangeMoveRecordLimit(p.q.c.limits, p.limits) || len(page.Key) > rangeMoveMaxCursorBytes {
		return RangeMoveImportState{}, ErrResourceLimit
	}
	if page.Offset == 0 {
		if prior.fragmentOffset != 0 {
			return RangeMoveImportState{}, ErrInvalid
		}
	} else if prior.fragmentKind != page.Kind || !bytes.Equal(prior.fragmentKey, page.Key) || prior.fragmentBytes != page.RecordBytes || prior.fragmentOffset != page.Offset {
		return RangeMoveImportState{}, ErrInvalid
	}
	length := int(page.Offset) + len(page.Payload)
	var record []byte
	if page.RecordDone {
		if err = p.q.arena.Reserve(256 + length + len(page.Key)); err != nil {
			return RangeMoveImportState{}, callerError(err)
		}
		record = make([]byte, length)
		offset := 0
		if prior.fragmentOffset > 0 {
			if prior.fragmentStart == 0 || prior.fragmentStart > prior.Sequence || prior.Sequence-prior.fragmentStart >= uint64(p.q.maxRows) { // #nosec G115 -- resolved caller/page budgets establish positive maxRows<=1<<20 before importer construction.
				return RangeMoveImportState{}, ErrResourceLimit
			}
			for sequence := prior.fragmentStart; sequence <= prior.Sequence; sequence++ {
				fragment, found, e := p.get(rangeMoveFragmentKey(p.root.namespace, d.binding(), sequence))
				if e != nil {
					return RangeMoveImportState{}, e
				}
				if !found || len(fragment) == 0 || len(fragment) > int(page.Offset)-offset {
					return RangeMoveImportState{}, ErrCorrupt
				}
				copy(record[offset:], fragment)
				offset += len(fragment)
			}
			if offset != int(page.Offset) {
				return RangeMoveImportState{}, ErrCorrupt
			}
		}
		copy(record[int(page.Offset):], page.Payload)
	}

	if page.RecordDone {
		if page.Next.Rows != prior.Rows+1 || uint64(len(page.Key)+length+17) > math.MaxUint64-prior.Bytes || page.Next.Bytes != prior.Bytes+uint64(len(page.Key)+length+17) || page.Next.Digest != rangeMoveRowDigest(prior.Digest, page.Kind, page.Key, record) { // #nosec G115 -- validated key<=128KiB and record length<=rangeMoveRecordLimit<=16MiB; sum is positive and fits native int.
			return RangeMoveImportState{}, ErrInvalid
		}
		if err = rangeMoveInstallRow(p, d, &next, page.Kind, page.Key, record); err != nil {
			return RangeMoveImportState{}, err
		}
		next.fragmentOffset = 0
		next.fragmentStart = 0
		next.fragmentKey = nil
		next.fragmentBytes = 0
		next.fragmentKind = 0
	} else {
		if page.Next.Rows != prior.Rows || page.Next.Bytes != prior.Bytes || page.Next.Digest != prior.Digest || int(page.Next.Offset) != length || !bytes.Equal(page.Next.Key, page.Key) {
			return RangeMoveImportState{}, ErrInvalid
		}
		if prior.fragmentOffset == 0 {
			next.fragmentStart = page.Next.Sequence
		}
		if err = p.q.put(rangeMoveFragmentKey(p.root.namespace, d.binding(), page.Next.Sequence), page.Payload); err != nil {
			return RangeMoveImportState{}, err
		}
		next.fragmentOffset = uint32(length) // #nosec G115 -- record bounded before accumulation.
		next.fragmentKey = exactCopy(page.Key)
		next.fragmentKind = page.Kind
		next.fragmentBytes = page.RecordBytes
	}
	next.Cursor = page.Next
	next.Sequence, next.Rows, next.Bytes, next.Digest = page.Next.Sequence, page.Next.Rows, page.Next.Bytes, page.Next.Digest
	return next, nil
}

// A complete logical catalog record/physical leaf is bounded independently of
// total range size. Its durable fragment state must fit existing record policy.
func rangeMoveRecordLimit(l Limits, p PageLimits) int {
	return min(rangeMoveMaxLogicalBytes, max(l.MaxRecordBytes, p.codecLimits(&Catalog{limits: l}).MaxEncodedBytes)+2*l.Temporal.MaxValueBytes+4096)
}

func rangeMoveInstallRow(p *pageStage, d RangeMoveDescriptor, next *RangeMoveImportState, kind byte, key, wire []byte) error {
	source := Namespace{d.Spec.Publication.Graph, d.Spec.From.Partition}
	target := p.root.namespace
	if err := p.q.materialize(2048 + 8*len(wire) + 4*len(key)); err != nil {
		return err
	}
	switch kind {
	case rangeMoveValues:
		entry, k, _, err := readValue(wire, source, p.q.c.limits)
		if err != nil {
			return err
		}
		if !d.Spec.Publication.Contains(uint64(entry.Ref.ID)) || !bytes.Equal(key, valueKey(source, entry.Ref.ID)) {
			return ErrInvalid
		}
		return p.q.stageValue(ValueRef{target.Graph, entry.Ref.ID}, entry.Value, string(k))
	case rangeMoveEntities:
		entity, err := readEntity(wire, source, p.q.c.limits)
		if err != nil {
			return err
		}
		if !d.Spec.Publication.Contains(uint64(entity.ID)) || !bytes.Equal(key, entityKey(source, entity.ID)) {
			return ErrInvalid
		}
		return p.q.stageEntity(EntityRef{target.Graph, entity.ID}, entity)
	case rangeMoveLives:
		life, err := readLife(wire, source, p.q.c.limits)
		if err != nil {
			return err
		}
		if !d.Spec.Publication.Contains(uint64(life.Owner)) || !bytes.Equal(key, lifeKey(source, life.Owner, life.Life)) {
			return ErrInvalid
		}
		return p.q.stageLife(LifeRef{target.Graph, life.Owner, life.Life}, life)
	case rangeMoveComponents:
		return rangeMoveInstallComponent(p, d, next, key, wire)
	case rangeMoveCanonical, rangeMoveDeclared:
		c := cursor{src: wire}
		recordKind := canonicalIncidentRecord
		if kind == rangeMoveDeclared {
			recordKind = declaredIncidentRecord
		}
		posting, err := decodePostingKey(&c, recordKind, p.q.c.limits)
		if err != nil || c.done() != nil || !bytes.Equal(key, wire) || !d.Spec.Publication.Contains(uint64(posting.endpoint)) {
			return errors.Join(ErrInvalid, err)
		}
		tree := p.q.full.descriptor.canonical
		if kind == rangeMoveDeclared {
			tree = p.q.full.descriptor.declared
		}
		tree, err = p.insertPostingKey(tree, posting, keyTreeLimits(p.limits))
		if err != nil {
			return err
		}
		if kind == rangeMoveDeclared {
			p.q.full.descriptor.declared = tree
		} else {
			p.q.full.descriptor.canonical = tree
		}
		return nil
	case rangeMovePresence:
		atom, err := rangeMoveReadAtom(wire)
		if err != nil || !d.Spec.Publication.Contains(uint64(atom.endpoint)) {
			return errors.Join(ErrInvalid, err)
		}
		expected := binary.BigEndian.AppendUint64(nil, uint64(atom.endpoint))
		expected = append(expected, atom.axis.id[:]...)
		expected = binary.BigEndian.AppendUint64(expected, uint64(atom.relationship))
		expected = binary.BigEndian.AppendUint64(expected, uint64(atom.life))
		if !bytes.Equal(expected, key) {
			return ErrInvalid
		}
		staged, err := stageCurrentPresenceInOperation(p, p.q.full.descriptor.own, nil, []currentPresenceAtom{atom}, fullPresenceLimits(p.pageReader))
		if err != nil {
			return err
		}
		p.q.full.descriptor.own = staged.tree
		return nil
	}
	return ErrInvalid
}

// FinishRangeMoveImport validates the complete stream and actual future tree
// roots. Sealing this inert state grants no coordinator decision authority.
func FinishRangeMoveImport(ctx context.Context, target *raftlog.ApplicationView, d RangeMoveDescriptor, prior RangeMoveImportState, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (RangeMoveImportState, PageWork, error) {
	if !prior.PreparationComplete || !prior.Complete || prior.Sealed || prior.fragmentOffset != 0 || prior.Rows != d.Rows || prior.Bytes != d.Bytes || prior.Digest != d.Digest {
		return RangeMoveImportState{}, PageWork{}, ErrInvalid
	}
	p, base, err := rangeMoveTargetReader(ctx, target, d, &prior, l, g, budget, output)
	if err != nil {
		return RangeMoveImportState{}, PageWork{}, err
	}
	prior.Sealed = true
	effects, work, err := rangeMovePageResult(p, base, prior)
	if err != nil {
		return RangeMoveImportState{}, work, err
	}
	return effects.Next, work, nil
}

// ActivateRangeMoveImport computes the sealed target root/index and placement
// effects. The outer native materializer alone validates COMMIT and installs it.
func ActivateRangeMoveImport(ctx context.Context, target *raftlog.ApplicationView, d RangeMoveDescriptor, sealed RangeMoveImportState, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (GraphEffects, PageWork, error) {
	if !sealed.Sealed || !sealed.Complete || !sealed.PreparationComplete || sealed.Rows != d.Rows || sealed.Bytes != d.Bytes || sealed.Digest != d.Digest || sealed.fragmentOffset != 0 {
		return GraphEffects{}, PageWork{}, ErrInvalid
	}
	p, base, err := rangeMoveTargetReader(ctx, target, d, &sealed, l, g, budget, output)
	if err != nil {
		return GraphEffects{}, PageWork{}, err
	}
	if err = p.q.materialize(2048); err != nil {
		return GraphEffects{}, p.work, err
	}
	wire, err := encodeFullDescriptor(sealed.index, sealed.FutureRoot, p.q.c.limits)
	if err != nil {
		return GraphEffects{}, p.work, err
	}
	if err = p.q.put(componentIndexDescriptorKey(p.root.namespace), wire); err != nil {
		return GraphEffects{}, p.work, err
	}
	placement, err := EncodeRangeOwnership(d.Spec.To)
	if err != nil {
		return GraphEffects{}, p.work, err
	}
	if err = p.q.put(RangeOwnershipKey(p.root.namespace, d.Spec.Publication.RangeID), placement); err != nil {
		return GraphEffects{}, p.work, err
	}
	effects, work, err := rangeMovePageResult(p, base, sealed)
	if err != nil {
		return GraphEffects{}, work, err
	}
	return GraphEffects{Base: effects.Base, Root: sealed.FutureRoot, Writes: effects.Writes, Work: work}, work, nil
}

// Private fragment rows are part of this page's ordinary fenced MVCC writes,
// never globally meaningful identity, business CDC or handoff authority.
func rangeMoveFragmentKey(n Namespace, binding [32]byte, sequence uint64) []byte {
	key := append(keyPrefix(n, recordKind(0xf0)), binding[:]...)
	return binary.BigEndian.AppendUint64(key, sequence)
}
