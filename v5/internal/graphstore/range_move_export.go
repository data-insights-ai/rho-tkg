package graphstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

func rangeMoveCaptureSource(ctx context.Context, source ApplicationReadSource) (RangeMoveCapture, error) {
	if ctx == nil || nilApplicationSource(source) {
		return RangeMoveCapture{}, ErrInvalid
	}
	binding, e := source.ApplicationBinding(ctx)
	if e != nil {
		return RangeMoveCapture{}, e
	}
	root, e := source.RootBounded(ctx, 172)
	if e != nil {
		return RangeMoveCapture{}, e
	}
	if len(root.Image) != 172 || sha256.Sum256(root.Image) != root.ImageHash {
		return RangeMoveCapture{}, ErrCorrupt
	}
	c := RangeMoveCapture{Binding: binding, Generation: root.Generation, Index: root.Index, ImageHash: root.ImageHash}
	copy(c.Image[:], root.Image)
	return c, nil
}
func rangeMoveCheckCapture(ctx context.Context, source ApplicationReadSource, want RangeMoveCapture, allowLater bool) (raftlog.ApplicationRoot, error) {
	actual, e := rangeMoveCaptureSource(ctx, source)
	if e != nil {
		return raftlog.ApplicationRoot{}, e
	}
	if actual.Binding != want.Binding || actual.Image != want.Image || actual.ImageHash != want.ImageHash || !allowLater && actual.Index != want.Index || allowLater && actual.Index < want.Index {
		return raftlog.ApplicationRoot{}, ErrRebinding
	}
	return raftlog.ApplicationRoot{Generation: actual.Generation, Index: actual.Index, ImageHash: actual.ImageHash, Image: exactCopy(actual.Image[:])}, nil
}

// PlanRangeMove validates only fixed source/target bindings and placement. It
// performs no whole-range scan or allocation. The source's portable manifest
// cursor can be co-committed by the outer protocol across bounded invocations.
func PlanRangeMove(ctx context.Context, source, target ApplicationReadSource, d OwnershipDeclaration, binding types.DefaultAxisBinding, cfg [32]byte, spec RangeMoveSpec, l Limits, g GraphLimits, budget OwnershipBudget, output *graphstate.OutputBudget) (descriptor RangeMoveDescriptor, work PageWork, err error) {
	if ctx == nil || nilApplicationSource(source) || nilApplicationSource(target) || !d.valid() || !spec.valid() || output == nil || cfg == ([32]byte{}) || cfg != spec.Publication.Configuration || spec.Publication.Graph != d.Graph() {
		return descriptor, work, ErrInvalid
	}
	if e := budget.validate(); e != nil {
		return descriptor, work, e
	}
	if e := ctx.Err(); e != nil {
		return descriptor, work, e
	}
	if err = output.Reserve(rangeMoveFixedBytes); err != nil {
		return descriptor, work, callerError(err)
	}
	for i, s := range []ApplicationReadSource{source, target} {
		capture, e := rangeMoveCaptureSource(ctx, s)
		if e != nil {
			return RangeMoveDescriptor{}, work, e
		}
		placement := spec.From
		if i == 1 {
			placement = spec.To
		}
		entry, found := d.Partition(placement.Partition)
		root, e := DecodeRoot(capture.Image[:])
		if e != nil || !found || capture.Binding.Identity.Graph != [16]byte(d.Graph()) || capture.Binding.Identity.Partition != placement.Partition || capture.Binding.Identity.Group != entry.Group || root.owner != entry.OwnershipEpoch || root.ownershipDigest != d.Digest() {
			return RangeMoveDescriptor{}, work, errors.Join(ErrNamespace, e)
		}
		remaining := budget
		remaining.SourceRows -= work.Records
		remaining.SourceBytes -= work.Bytes
		remaining.OutputBytes = min(remaining.OutputBytes, output.Remaining())
		if remaining.SourceRows < 1 || remaining.SourceBytes < 1 || remaining.OutputBytes < 1 {
			return RangeMoveDescriptor{}, work, ErrResourceLimit
		}
		view, w, e := openPartitionGraphReadViewBudget(ctx, s, d, binding, cfg, l, g, remaining, output)
		work = addWork(work, w)
		if e != nil {
			return RangeMoveDescriptor{}, work, e
		}
		if e = view.Close(); e != nil {
			return RangeMoveDescriptor{}, work, e
		}
		remaining = budget
		remaining.SourceRows -= work.Records
		remaining.SourceBytes -= work.Bytes
		remaining.OutputBytes = min(remaining.OutputBytes, output.Remaining())
		if remaining.SourceRows < 1 || remaining.SourceBytes < 1 || remaining.OutputBytes < 1 {
			return RangeMoveDescriptor{}, work, ErrResourceLimit
		}
		p, o, w, e := lookupPublishedRangeBudget(ctx, s, Namespace{d.Graph(), placement.Partition}, spec.Publication.First, cfg, remaining, output)
		work = addWork(work, w)
		if e != nil {
			return RangeMoveDescriptor{}, work, e
		}
		if p != spec.Publication || o != spec.From {
			return RangeMoveDescriptor{}, work, ErrRebinding
		}
		capture.Generation = 0 // Local physical generations do not enter shared capture identity.
		if i == 0 {
			descriptor.Source = capture
		} else {
			descriptor.Target = capture
		}
	}
	descriptor.Spec, descriptor.Declaration, descriptor.Configuration, descriptor.DefaultAxis = spec, d.Digest(), cfg, binding
	if !descriptor.valid() {
		return RangeMoveDescriptor{}, work, ErrInvalid
	}
	return descriptor, work, nil
}

// SealRangeMoveDescriptor copies exact complete manifest totals. Its caller
// must independently prove that the cursor was obtained from the actual source.
func SealRangeMoveDescriptor(d RangeMoveDescriptor, final RangeMoveCursor, output *graphstate.OutputBudget) (RangeMoveDescriptor, error) {
	if output == nil || !d.valid() || d.Sealed || !rangeMoveValidCursor(final) || final.Binding != d.binding() || final.Family != rangeMoveDone || final.Offset != 0 || len(final.Key) != 0 {
		return RangeMoveDescriptor{}, ErrInvalid
	}
	if e := output.Reserve(rangeMoveFixedBytes); e != nil {
		return RangeMoveDescriptor{}, callerError(e)
	}
	d.Rows, d.Bytes, d.Digest, d.Sealed = final.Rows, final.Bytes, final.Digest, true
	return d, nil
}

func rangeMoveExportReader(ctx context.Context, source ApplicationReadSource, d RangeMoveDescriptor, l Limits, g GraphLimits, output *graphstate.OutputBudget) (*pageReader, error) {
	if ctx == nil || output == nil || !d.valid() {
		return nil, ErrInvalid
	}
	if _, e := rangeMoveCheckCapture(ctx, source, d.Source, false); e != nil {
		return nil, e
	}
	var e error
	l, e = l.resolve()
	if e != nil {
		return nil, e
	}
	g, e = g.resolve()
	if e != nil {
		return nil, e
	}
	root, e := DecodeRoot(d.Source.Image[:])
	if e != nil {
		return nil, e
	}
	c := &Catalog{data: source, root: root, rootImageBytes: 172, limits: l, hash: equalityDigest}
	q, e := c.readerWithOutputBudget(ctx, output)
	if e != nil {
		return nil, e
	}
	q.maxRows = min(g.MaxSourceRows, g.Pages.MaxWorkRecords)
	q.maxBytes = min(g.MaxSourceBytes, g.Pages.MaxWorkBytes)
	index, found, e := q.fullDescriptor(root)
	if e != nil || !found {
		return nil, errors.Join(ErrCorrupt, e)
	}
	q.fullView = &index
	c.localFull = &index
	p := &pageReader{q: q, limits: g.Pages}
	if e = p.validateFullRoots(index); e != nil {
		return nil, e
	}
	return p, nil
}

func rangeMoveCatalogNext(q *pageReader, d RangeMoveDescriptor, c *RangeMoveCursor, kind recordKind) (raftlog.KV, bool, error) {
	n := q.q.c.root.namespace
	prefix := keyPrefix(n, kind)
	if q.q.rows >= q.q.maxRows {
		return raftlog.KV{}, false, ErrResourceLimit
	}
	if c.Offset > 0 {
		if len(c.Key) < 33 || !bytes.HasPrefix(c.Key, prefix) || !d.Spec.Publication.Contains(binary.BigEndian.Uint64(c.Key[25:33])) {
			return raftlog.KV{}, false, ErrInvalid
		}
		maximum := min(q.q.c.limits.MaxRecordBytes+len(c.Key), q.q.c.source().ReadLimits().Bytes, q.q.maxBytes-q.q.bytes-64, q.q.arena.Remaining()-64)
		if maximum < len(c.Key) {
			return raftlog.KV{}, false, ErrResourceLimit
		}
		row, found, e := q.q.c.source().Get(q.q.ctx, c.Key, maximum)
		q.q.rows++
		q.q.bytes += cap(row.Key) + cap(row.Value) + 64
		if e != nil {
			return raftlog.KV{}, false, e
		}
		if !found || row.Deleted || !bytes.Equal(row.Key, c.Key) {
			return raftlog.KV{}, false, ErrCorrupt
		}
		if cap(row.Key)+cap(row.Value) > maximum {
			return raftlog.KV{}, false, ErrCorrupt
		}
		if e = q.q.arena.Reserve(cap(row.Key) + cap(row.Value) + 64); e != nil {
			return raftlog.KV{}, false, callerError(e)
		}
		return row, true, q.budget()
	}
	if e := q.q.materialize(256 + 3*len(prefix)); e != nil {
		return raftlog.KV{}, false, e
	}
	lower := binary.BigEndian.AppendUint64(prefix, d.Spec.Publication.First)
	upper := keyPrefix(n, kind)
	if d.Spec.Publication.Last < math.MaxUint64 {
		upper = binary.BigEndian.AppendUint64(upper, d.Spec.Publication.Last+1)
	} else {
		upper = append(upper, bytes.Repeat([]byte{255}, 17)...)
	}
	// Scan returns at most one catalog record. Admit both its native work and
	// owned copied backing, including continuation framing, before provider copy.
	maximum := min(q.q.c.source().ReadLimits().Bytes, (q.q.maxBytes-q.q.bytes-128)/2, q.q.arena.Remaining()-128)
	if maximum < 128+len(lower)+len(upper)+len(c.After) {
		return raftlog.KV{}, false, ErrResourceLimit
	}
	page, e := q.q.c.source().Scan(q.q.ctx, lower, upper, c.After, raftlog.ReadBudget{Rows: 1, Bytes: maximum})
	q.q.rows += page.Visited
	q.q.bytes += page.Bytes
	if e != nil {
		return raftlog.KV{}, false, e
	}
	if e = q.budget(); e != nil {
		return raftlog.KV{}, false, e
	}
	if len(page.Rows) > 1 || cap(page.Rows) > 1 || page.Visited > 1 || page.Bytes < 0 {
		return raftlog.KV{}, false, ErrCorrupt
	}
	owned := 128 + cap(page.Next) + 64*cap(page.Rows)
	for _, row := range page.Rows {
		owned += cap(row.Key) + cap(row.Value)
	}
	if owned > maximum+128 {
		return raftlog.KV{}, false, ErrCorrupt
	}
	if e = q.q.materialize(owned); e != nil {
		return raftlog.KV{}, false, e
	}
	if len(page.Rows) == 0 {
		if !page.Complete {
			if len(page.Next) < 33 || len(page.Next) > rangeMoveMaxCursorBytes || !bytes.HasPrefix(page.Next, prefix) || !d.Spec.Publication.Contains(binary.BigEndian.Uint64(page.Next[25:33])) || bytes.Compare(page.Next, c.After) <= 0 || bytes.Compare(page.Next, upper) >= 0 {
				return raftlog.KV{}, false, ErrCorrupt
			}
			// Native scans budget invisible/tombstoned physical keys too. Keep
			// their exact bounded continuation instead of retrying this hole.
			c.After = page.Next
		}
		return raftlog.KV{}, false, nil
	}
	row := page.Rows[0]
	if row.Deleted || len(row.Key) < 33 || !bytes.HasPrefix(row.Key, keyPrefix(n, kind)) {
		return raftlog.KV{}, false, ErrCorrupt
	}
	id := binary.BigEndian.Uint64(row.Key[25:33])
	if !d.Spec.Publication.Contains(id) || len(c.After) > 0 && bytes.Compare(row.Key, c.After) <= 0 || cap(row.Key)+cap(row.Value) > maximum {
		return raftlog.KV{}, false, ErrCorrupt
	}
	return row, true, nil
}

func rangeMoveNextDirectory(q *pageReader, m componentMeta, c *RangeMoveCursor) (directoryPage, bool, error) {
	if c.Depth == 0 {
		c.Depth = 1
		c.Path[0] = m.Root
		c.Slots[0] = 0
	}
	for c.Depth > 0 {
		n := int(c.Depth) - 1
		node, e := q.directory(c.Path[n], m.Key, m.Axis)
		if e != nil {
			return directoryPage{}, false, e
		}
		if node.Level == 0 {
			c.Depth--
			c.Path[n] = 0
			c.Slots[n] = 0
			return node, true, nil
		}
		if int(c.Slots[n]) >= len(node.Children) {
			c.Depth--
			c.Path[n] = 0
			c.Slots[n] = 0
			continue
		}
		slot := int(c.Slots[n])
		c.Slots[n]++
		child := node.Children[slot]
		if int(c.Depth) >= len(c.Path) {
			return directoryPage{}, false, ErrCorrupt
		}
		c.Path[c.Depth] = child.ID
		c.Slots[c.Depth] = 0
		c.Depth++
	}
	return directoryPage{}, false, nil
}

// rangeMoveState reads and replays a single bounded frozen leaf. Cell handles
// are preserved exactly; target-side reference validation occurs after range
// values and immutable catalogs have been imported, rather than inventing local
// source ownership for an outside-range value referenced by this component.
func rangeMoveState(q *pageReader, leaf directoryPage) (state.State, error) {
	axis, k := leaf.Owned.Axis(), leaf.Key
	current, e := state.New(axis, q.limits.stateLimits(q.q.c))
	if e != nil {
		return current, e
	}
	if leaf.Base != 0 {
		b, found, e := q.get(physicalKey(q.q.c.root.namespace, checkpointRecord, leaf.Base))
		if e != nil || !found {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
		c, e := inspectRecord(b, q.q.c.root.namespace, checkpointRecord, q.q.c.limits)
		if e != nil {
			return state.State{}, e
		}
		id, e := c.number()
		if e != nil {
			return state.State{}, e
		}
		key, e := decodeComponent(&c, q.q.c.limits)
		if e != nil {
			return state.State{}, e
		}
		wire, e := c.field(1 << 20)
		if e != nil || id != leaf.Base || key != k || c.done() != nil || !q.validPhysical(id) {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
		current, e = state.DecodeState(wire, axis, q.limits.codecLimits(q.q.c))
		if e != nil {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
		q.work.CheckpointPages++
	}
	tail := make([]patchPage, 0, leaf.TailRecords)
	id := leaf.Head
	totalBytes, totalAtoms := 0, 0
	for range leaf.TailRecords {
		if id == 0 || !q.validPhysical(id) {
			return state.State{}, ErrCorrupt
		}
		b, found, e := q.get(physicalKey(q.q.c.root.namespace, patchRecord, id))
		if e != nil || !found {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
		c, e := inspectRecord(b, q.q.c.root.namespace, patchRecord, q.q.c.limits)
		if e != nil {
			return state.State{}, e
		}
		self, e := c.number()
		if e != nil {
			return state.State{}, e
		}
		key, e := decodeComponent(&c, q.q.c.limits)
		if e != nil {
			return state.State{}, e
		}
		previous, e := c.number()
		if e != nil {
			return state.State{}, e
		}
		owned, e := readScopeField(&c, axis, q.q.c.limits)
		if e != nil {
			return state.State{}, e
		}
		wire, e := c.field(1 << 20)
		if e != nil || self != id || key != k || c.done() != nil {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
		same, e := sameScope(owned, leaf.Owned, q.q.c.limits.Temporal)
		if e != nil || !same {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
		changes, _, e := state.DecodeChanges(wire, axis, q.limits.codecLimits(q.q.c))
		if e != nil {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
		tail = append(tail, patchPage{ID: id, Previous: previous, Key: key, Owned: owned, Changes: changes})
		id = previous
		totalBytes += len(b)
		totalAtoms += len(changes)
		q.work.PatchPages++
	}
	if id != 0 || totalBytes != leaf.TailBytes || totalAtoms != leaf.TailAtoms {
		return state.State{}, ErrCorrupt
	}
	for i := len(tail) - 1; i >= 0; i-- {
		for _, change := range tail[i].Changes {
			if e = verifyBefore(current, change.Scope(), change.Before(), q.limits.stateLimits(q.q.c)); e != nil {
				return state.State{}, errors.Join(ErrCorrupt, e)
			}
			next, e := applyPageCell(current, change.Scope(), change.After(), q.limits.stateLimits(q.q.c))
			if e != nil {
				return state.State{}, e
			}
			current = next.State()
			if e = q.q.materialize(current.Usage().MetadataBytes()); e != nil {
				return state.State{}, e
			}
			q.work.DecodedCells++
		}
	}
	if current.Usage().Pieces() != leaf.Cells {
		return state.State{}, ErrCorrupt
	}
	for _, piece := range current.Pieces() {
		ok, e := within(piece.Scope(), leaf.Owned, q.q.c.limits.Temporal)
		if e != nil || !ok {
			return state.State{}, errors.Join(ErrCorrupt, e)
		}
	}
	if e = q.q.materialize(current.Usage().MetadataBytes()); e != nil {
		return state.State{}, e
	}
	return current, q.budget()
}

func rangeMoveComponentNext(q *pageReader, d RangeMoveDescriptor, c RangeMoveCursor) (key, payload []byte, next RangeMoveCursor, found bool, err error) {
	next = c
	var meta componentMeta
	if len(c.Key) > 0 {
		if len(c.Key) < 66 || !bytes.HasPrefix(c.Key, keyPrefix(q.q.c.root.namespace, componentRecord)) {
			return nil, nil, next, false, ErrInvalid
		}
		cc := cursor{src: c.Key[25 : len(c.Key)-12]}
		k, e := decodeComponent(&cc, q.q.c.limits)
		if e != nil || cc.done() != nil {
			return nil, nil, next, false, ErrCorrupt
		}
		meta, found, err = q.readMeta(k)
		if err != nil || !found {
			return nil, nil, next, false, errors.Join(ErrCorrupt, err)
		}
	} else {
		row, ok, e := rangeMoveCatalogNext(q, d, &next, componentRecord)
		if e != nil || !ok {
			return nil, nil, next, false, e
		}
		cc := cursor{src: row.Key[25:]}
		k, e := decodeComponent(&cc, q.q.c.limits)
		if e != nil || cc.done() != nil {
			return nil, nil, next, false, ErrCorrupt
		}
		meta, ok, e = q.readMeta(k)
		if e != nil || !ok {
			return nil, nil, next, false, errors.Join(ErrCorrupt, e)
		}
		next.Key = exactCopy(row.Key)
	}
	var leaf directoryPage
	if c.Record > 0 {
		if binary.BigEndian.Uint64(c.Key[len(c.Key)-12:len(c.Key)-4]) != c.Record || binary.BigEndian.Uint32(c.Key[len(c.Key)-4:]) != c.Cell {
			return nil, nil, next, false, ErrInvalid
		}
		leaf, err = q.directory(c.Record, meta.Key, meta.Axis)
	} else {
		var ok bool
		leaf, ok, err = rangeMoveNextDirectory(q, meta, &next)
		if err == nil && !ok {
			next.After = componentKey(q.q.c.root.namespace, meta.Key)
			next.Key = nil
			next.Record = 0
			return rangeMoveComponentNext(q, d, next)
		}
	}
	if err != nil {
		return nil, nil, next, false, err
	}
	current, e := rangeMoveState(q, leaf)
	if e != nil {
		return nil, nil, next, false, e
	}
	pieces := current.Pieces()
	if int(c.Cell) > len(pieces) || len(pieces) > 0 && int(c.Cell) == len(pieces) {
		return nil, nil, next, false, ErrInvalid
	}
	ownedScope := leaf.Owned
	rowState := current
	if len(pieces) > 0 {
		ownedScope = pieces[c.Cell].Scope()
		rowState, e = current.Slice(ownedScope, q.limits.stateLimits(q.q.c))
		if e != nil {
			return nil, nil, next, false, e
		}
	}
	stateWire, e := state.AppendState(nil, rowState, q.limits.codecLimits(q.q.c))
	if e != nil {
		return nil, nil, next, false, e
	}
	axisWire, e := appendAxis(nil, meta.Axis, q.q.c.limits)
	if e != nil {
		return nil, nil, next, false, e
	}
	owned, e := temporal.AppendScope(nil, ownedScope, q.q.c.limits.Temporal)
	if e != nil {
		return nil, nil, next, false, e
	}
	payload, e = rangeMoveBuild(q.q.arena, rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) { w.field(axisWire); w.field(owned); w.field(stateWire) })
	if e != nil {
		return nil, nil, next, false, e
	}
	baseKey := componentKey(q.q.c.root.namespace, meta.Key)
	key = binary.BigEndian.AppendUint64(baseKey, leaf.ID)
	key = binary.BigEndian.AppendUint32(key, c.Cell)
	next.Key = key
	if len(pieces) > 0 && int(c.Cell)+1 < len(pieces) {
		next.Record = leaf.ID
		next.Cell = c.Cell + 1
	} else {
		next.Record = 0
		next.Cell = 0
	}
	return key, payload, next, true, nil
}

func rangeMovePostingNext(q *pageReader, d RangeMoveDescriptor, c *RangeMoveCursor, root postingTreeRoot) (postingKey, bool, error) {
	l := keyTreeLimits(q.limits)
	if c.Depth == 0 {
		c.Depth = 1
		c.Path[0] = root.id
		c.Slots[0] = 0
	}
	var frames [32]postingTreeNode
	// Reconstruct only the frozen path, not a source-handle cursor/cache.
	for i := 0; i < int(c.Depth); i++ {
		var e error
		if i == 0 {
			frames[i], e = q.postingTreeRoot(root, l)
		} else {
			if c.Slots[i-1] == 0 {
				return postingKey{}, false, ErrCorrupt
			}
			frames[i], e = q.postingTreeChild(frames[i-1], int(c.Slots[i-1])-1, l) // #nosec G602 -- validated cursor Depth<=32 bounds the fixed array; this else branch has i>=1 and nonzero previous slot.
		}
		if e != nil {
			return postingKey{}, false, e
		}
		if frames[i].id != c.Path[i] {
			return postingKey{}, false, ErrCorrupt
		}
	}
	if c.Offset > 0 {
		n := int(c.Depth) - 1
		if n < 0 || c.Slots[n] == 0 || int(c.Slots[n]) > len(frames[n].keys) {
			return postingKey{}, false, ErrCorrupt
		}
		return frames[n].keys[c.Slots[n]-1], true, nil
	}
	for c.Depth > 0 {
		n := int(c.Depth) - 1
		var node postingTreeNode
		var e error
		if n == 0 {
			node, e = q.postingTreeRoot(root, l)
		} else {
			parent := frames[n-1]
			node, e = q.postingTreeChild(parent, int(c.Slots[n-1])-1, l)
		}
		if e != nil {
			return postingKey{}, false, e
		}
		if node.id != c.Path[n] {
			return postingKey{}, false, ErrCorrupt
		}
		frames[n] = node
		if node.level == 0 {
			for int(c.Slots[n]) < len(node.keys) {
				k := node.keys[c.Slots[n]]
				c.Slots[n]++
				if d.Spec.Publication.Contains(uint64(k.endpoint)) {
					return k, true, nil
				}
			}
			c.Depth--
			c.Path[n] = 0
			c.Slots[n] = 0
			continue
		}
		if int(c.Slots[n]) >= len(node.children) {
			c.Depth--
			c.Path[n] = 0
			c.Slots[n] = 0
			continue
		}
		slot := int(c.Slots[n])
		c.Slots[n]++
		child := node.children[slot]
		if uint64(child.last.endpoint) < d.Spec.Publication.First || uint64(child.first.endpoint) > d.Spec.Publication.Last {
			continue
		}
		if int(c.Depth) >= len(c.Path) {
			return postingKey{}, false, ErrCorrupt
		}
		c.Path[c.Depth] = child.id
		c.Slots[c.Depth] = 0
		c.Depth++
	}
	return postingKey{}, false, nil
}

func rangeMovePresenceNext(q *pageReader, d RangeMoveDescriptor, c *RangeMoveCursor) (currentPresenceAtom, bool, error) {
	index, e := q.fullDescriptor()
	if e != nil {
		return currentPresenceAtom{}, false, e
	}
	op := cpTreeOperation{pageStage: &pageStage{pageReader: q, root: q.q.c.root}, limits: fullPresenceLimits(q), cache: make(map[uint64]currentPresencePage), dirty: make(map[uint64][]byte)}
	root := index.own
	if c.Depth == 0 {
		c.Depth = 1
		c.Path[0] = root.id
		c.Slots[0] = 0
	}
	var frames [32]currentPresencePage
	for i := 0; i < int(c.Depth); i++ {
		if i == 0 {
			frames[i], e = op.load(root)
		} else {
			if c.Slots[i-1] == 0 {
				return currentPresenceAtom{}, false, ErrCorrupt
			}
			frames[i], e = op.child(frames[i-1], int(c.Slots[i-1])-1) // #nosec G602 -- validated cursor Depth<=32 bounds the fixed array; this else branch has i>=1 and nonzero previous slot.
		}
		if e != nil {
			return currentPresenceAtom{}, false, e
		}
		if frames[i].id != c.Path[i] {
			return currentPresenceAtom{}, false, ErrCorrupt
		}
	}
	if c.Offset > 0 {
		n := int(c.Depth) - 1
		if n < 0 || c.Slots[n] == 0 || int(c.Slots[n]) > len(frames[n].rows) {
			return currentPresenceAtom{}, false, ErrCorrupt
		}
		a, e := cpAtomFromRow(frames[n], frames[n].rows[c.Slots[n]-1], op.limits.codec)
		return a, e == nil, e
	}
	for c.Depth > 0 {
		n := int(c.Depth) - 1
		var node currentPresencePage
		if n == 0 {
			node, e = op.load(root)
		} else {
			parent := frames[n-1]
			node, e = op.child(parent, int(c.Slots[n-1])-1)
		}
		if e != nil {
			return currentPresenceAtom{}, false, e
		}
		if node.id != c.Path[n] {
			return currentPresenceAtom{}, false, ErrCorrupt
		}
		frames[n] = node
		if node.level == 0 {
			for int(c.Slots[n]) < len(node.rows) {
				row := node.rows[c.Slots[n]]
				c.Slots[n]++
				atom, e := cpAtomFromRow(node, row, op.limits.codec)
				if e != nil {
					return currentPresenceAtom{}, false, e
				}
				if d.Spec.Publication.Contains(uint64(atom.endpoint)) {
					return atom, true, nil
				}
			}
			c.Depth--
			c.Path[n] = 0
			c.Slots[n] = 0
			delete(op.cache, node.id)
			continue
		}
		if int(c.Slots[n]) >= len(node.children) {
			c.Depth--
			c.Path[n] = 0
			c.Slots[n] = 0
			delete(op.cache, node.id)
			continue
		}
		slot := int(c.Slots[n])
		c.Slots[n]++
		child := node.children[slot]
		first, e := cpAtomFromFence(node, child.first, op.limits.codec)
		if e != nil {
			return currentPresenceAtom{}, false, e
		}
		last, e := cpAtomFromFence(node, child.last, op.limits.codec)
		if e != nil {
			return currentPresenceAtom{}, false, e
		}
		if uint64(last.endpoint) < d.Spec.Publication.First || uint64(first.endpoint) > d.Spec.Publication.Last {
			continue
		}
		if int(c.Depth) >= len(c.Path) {
			return currentPresenceAtom{}, false, ErrCorrupt
		}
		c.Path[c.Depth] = child.id
		c.Slots[c.Depth] = 0
		c.Depth++
	}
	return currentPresenceAtom{}, false, nil
}

func rangeMoveEmitAtom(a currentPresenceAtom, output *graphstate.OutputBudget) ([]byte, error) {
	return rangeMoveBuild(output, rangeMoveMaxLogicalBytes, func(w *rangeMoveWriter) {
		w.add(a.axis.id[:])
		w.add(a.axis.hash[:])
		w.tag(byte(a.axis.profile))
		w.number(uint64(a.endpoint))
		w.number(uint64(a.bound))
		w.tag(byte(a.mode))
		w.number(uint64(a.relationship))
		w.number(uint64(a.life))
		w.tag(a.flags)
		w.field(a.lower)
		w.field(a.upper)
	})
}
func rangeMoveReadAtom(wire []byte) (a currentPresenceAtom, e error) {
	c := cursor{src: wire}
	if e = rangeMoveArray(&c, 16, a.axis.id[:]); e != nil {
		return a, e
	}
	if e = rangeMoveArray(&c, 32, a.axis.hash[:]); e != nil {
		return a, e
	}
	profile, e := c.tag()
	if e != nil {
		return a, e
	}
	a.axis.profile = temporal.Profile(profile)
	n, e := c.number()
	if e != nil {
		return a, e
	}
	a.endpoint = graphstate.EntityID(n)
	n, e = c.number()
	if e != nil {
		return a, e
	}
	a.bound = graphstate.LifeID(n)
	mode, e := c.tag()
	if e != nil {
		return a, e
	}
	a.mode = graphstate.ReferenceMode(mode)
	n, e = c.number()
	if e != nil {
		return a, e
	}
	a.relationship = graphstate.EntityID(n)
	n, e = c.number()
	if e != nil {
		return a, e
	}
	a.life = graphstate.LifeID(n)
	if a.flags, e = c.tag(); e != nil {
		return a, e
	}
	if a.lower, e = c.field(1 << 20); e != nil {
		return a, e
	}
	if a.upper, e = c.field(1 << 20); e != nil {
		return a, e
	}
	if c.done() != nil {
		return a, ErrCorrupt
	}
	return a, nil
}

func rangeMoveNextRow(q *pageReader, d RangeMoveDescriptor, c RangeMoveCursor) (key, payload []byte, next RangeMoveCursor, found bool, err error) {
	next = c
	for next.Family < rangeMoveDone {
		switch next.Family {
		case rangeMoveValues, rangeMoveEntities, rangeMoveLives:
			kind := valueRecord
			switch next.Family {
			case rangeMoveEntities:
				kind = entityRecord
			case rangeMoveLives:
				kind = lifeRecord
			}
			beforeAfter := next.After
			row, ok, e := rangeMoveCatalogNext(q, d, &next, kind)
			if e != nil {
				return nil, nil, next, false, e
			}
			if ok {
				return row.Key, row.Value, next, true, nil
			}
			if bytes.Compare(next.After, beforeAfter) > 0 {
				return nil, nil, next, false, nil
			}
		case rangeMoveComponents:
			key, payload, after, ok, e := rangeMoveComponentNext(q, d, next)
			if e != nil {
				return nil, nil, next, false, e
			}
			if ok {
				return key, payload, after, true, nil
			}
			if bytes.Compare(after.After, next.After) > 0 {
				return nil, nil, after, false, nil
			}
		case rangeMoveCanonical, rangeMoveDeclared:
			index, e := q.fullDescriptor()
			if e != nil {
				return nil, nil, next, false, e
			}
			root := index.canonical
			if next.Family == rangeMoveDeclared {
				root = index.declared
			}
			k, ok, e := rangeMovePostingNext(q, d, &next, root)
			if e != nil {
				return nil, nil, next, false, e
			}
			if ok {
				data := appendPostingKey(nil, k)
				return data, data, next, true, nil
			}
		case rangeMovePresence:
			a, ok, e := rangeMovePresenceNext(q, d, &next)
			if e != nil {
				return nil, nil, next, false, e
			}
			if ok {
				payload, e := rangeMoveEmitAtom(a, q.q.arena)
				if e != nil {
					return nil, nil, next, false, e
				}
				key := binary.BigEndian.AppendUint64(nil, uint64(a.endpoint))
				key = append(key, a.axis.id[:]...)
				key = binary.BigEndian.AppendUint64(key, uint64(a.relationship))
				key = binary.BigEndian.AppendUint64(key, uint64(a.life))
				return key, payload, next, true, nil
			}
		}
		next.Family++
		next.After = nil
		next.Key = nil
		next.Path = [32]uint64{}
		next.Slots = [32]uint16{}
		next.Depth = 0
		next.Record = 0
		next.Cell = 0
		next.Offset = 0
	}
	return nil, nil, next, false, nil
}

// ExportRangeMovePage owns at most one bounded record fragment and transient
// frozen tree paths. Count/digest progress belongs to the portable cursor, not a
// retained exporter map, so the manifest scan and transfer resume after restart.
func ExportRangeMovePage(ctx context.Context, source ApplicationReadSource, d RangeMoveDescriptor, c RangeMoveCursor, budget raftlog.ReadBudget, l Limits, g GraphLimits, output *graphstate.OutputBudget) (page RangeMovePage, work PageWork, err error) {
	if ctx == nil || output == nil || budget.Rows < 1 || budget.Bytes < rangeMoveFixedBytes || !d.valid() {
		return page, work, ErrInvalid
	}
	fresh := c.Family == 0
	if fresh {
		if !rangeMoveCursorEqual(c, RangeMoveCursor{}) {
			return page, work, ErrInvalid
		}
		c = rangeMoveStart(d)
	}
	if !rangeMoveValidCursor(c) || c.Binding != d.binding() || c.Family == rangeMoveDone || c.Sequence == math.MaxUint64 {
		return page, work, ErrInvalid
	}
	q, e := rangeMoveExportReader(ctx, source, d, l, g, output)
	if e != nil {
		return page, work, e
	}
	defer func() {
		if e := q.budget(); err == nil && e != nil {
			err = e
		}
		work = q.work
		if err == nil {
			cost := 1024 + len(page.Before.After) + len(page.Before.Key) + len(page.Next.After) + len(page.Next.Key)
			if e := output.Reserve(cost); e != nil {
				err = callerError(e)
			} else {
				page.Before = rangeMoveCloneCursor(page.Before)
				page.Next = rangeMoveCloneCursor(page.Next)
			}
		}
		if err != nil {
			page = RangeMovePage{}
		}
	}()
	if fresh {
		// Recorded evidence is not carried by range moves yet: refuse rather than
		// move the range and silently drop it. Index rows are derived data: the
		// destination re-derives them from the moved components.
		var probe RangeMoveCursor
		if _, held, e := rangeMoveCatalogNext(q, d, &probe, evidenceRecord); e != nil {
			return page, work, e
		} else if held {
			return page, work, callerError(graphstate.ErrUnsupported)
		}
	}
	key, payload, next, found, e := rangeMoveNextRow(q, d, c)
	if e != nil {
		return page, work, e
	}
	page.Before = c
	next.Sequence = c.Sequence + 1
	if !found {
		if next.Family < rangeMoveDone {
			page.Next = next
			return page, work, nil
		}
		next.Family = rangeMoveDone
		next.Key = nil
		next.Offset = 0
		next.Record = 0
		page.Next = next
		page.Complete = true
		return page, work, nil
	}
	if len(payload) > rangeMoveRecordLimit(q.q.c.limits, q.limits) || len(payload) == 0 || len(key) > rangeMoveMaxCursorBytes || uint64(c.Offset) > uint64(len(payload)) {
		return RangeMovePage{}, work, ErrResourceLimit
	}
	offset := int(c.Offset)
	available := min(budget.Bytes-rangeMoveFixedBytes, output.Remaining()/8)
	if available < 1 {
		return RangeMovePage{}, work, ErrResourceLimit
	}
	end := offset + min(available, len(payload)-offset)
	if e = output.Reserve(rangeMoveFixedBytes + 4*(len(key)+end-offset)); e != nil {
		return RangeMovePage{}, work, callerError(e)
	}
	page.Kind, page.Key, page.Payload, page.RecordBytes, page.Offset, page.RecordDone = next.Family, exactCopy(key), exactCopy(payload[offset:end]), uint32(len(payload)), c.Offset, end == len(payload) // #nosec G115 -- record length admitted <=16MiB.
	if page.RecordDone {
		if c.Rows == math.MaxUint64 || uint64(len(key)+len(payload)+17) > math.MaxUint64-c.Bytes {
			return RangeMovePage{}, work, ErrResourceLimit
		}
		next.Rows = c.Rows + 1
		next.Bytes = c.Bytes + uint64(len(key)+len(payload)+17)
		next.Digest = rangeMoveRowDigest(c.Digest, page.Kind, key, payload)
		next.Offset = 0
		if next.Family == rangeMoveComponents {
			if next.Record > 0 {
				next.Key = exactCopy(key)
				binary.BigEndian.PutUint32(next.Key[len(next.Key)-4:], next.Cell)
			} else if next.Depth == 0 {
				next.After = exactCopy(key[:len(key)-12])
				next.Key = nil
				next.Record = 0
			} else {
				next.Key = exactCopy(key)
				next.Record = 0
			}
		} else {
			next.After = exactCopy(key)
			next.Key = nil
			next.Record = 0
		}
	} else {
		next.Key = exactCopy(key)
		if next.Family == rangeMoveComponents {
			next.Record = binary.BigEndian.Uint64(key[len(key)-12 : len(key)-4])
			next.Cell = c.Cell
		}
		next.Offset = uint32(end)
	} // #nosec G115 -- bounded end <=16MiB.
	page.Next = next
	return page, work, nil
}
