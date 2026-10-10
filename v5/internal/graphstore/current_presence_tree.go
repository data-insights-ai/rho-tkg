package graphstore

import (
	"bytes"
	"cmp"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Native edits are private index maintenance, not graph operation admission.
// Each atom is canonical own support for ONE qualified relationship life and
// endpoint role run. Callers must supply both endpoint roles atomically.
type currentPresenceAtom struct {
	axis         currentPresenceAxis
	endpoint     graphstate.EntityID
	bound        graphstate.LifeID
	mode         graphstate.ReferenceMode
	relationship graphstate.EntityID
	life         graphstate.LifeID
	flags        byte
	lower, upper []byte
}
type currentPresenceTreeRoot struct {
	id, count uint64
	digest    [32]byte
	level     uint8
}
type currentPresenceTreeLimits struct {
	codec                                 currentPresenceLimits
	pages                                 PageLimits
	targetBytes, maxEdits, maxOutputBytes int
}
type currentPresenceTreeWork struct {
	PageWork
	LoadedPages, EncodedPages, FinalPageWrites, Rebalances, Splits int
	NativeRows, LogicalRewriteBytes                                int // repeated encoded materialization, NOT disk I/O
	WitnessEntityCalls, WitnessLifeCalls, ResidualRecords          int // calls vs actual source records, not decoded entity counts
}
type stagedCurrentPresence struct {
	root Root
	tree currentPresenceTreeRoot
	work currentPresenceTreeWork
}
type cpReference struct {
	child currentPresenceChild
	page  currentPresencePage
	level uint8
}
type cpTreeOperation struct {
	*pageStage
	limits   currentPresenceTreeLimits
	cache    map[uint64]currentPresencePage // bounded touched PAGE cache, never per-fact resident state
	dirty    map[uint64][]byte
	counters currentPresenceTreeWork
}

const (
	cpAtomOwned          = 192
	cpReferenceOwned     = 384
	cpCacheEntryOwned    = 512
	cpTreeOperationOwned = 4096
	cpTreeResultOwned    = 384
)

func defaultCurrentPresenceTreeLimits() currentPresenceTreeLimits {
	return currentPresenceTreeLimits{defaultCurrentPresenceLimits(), DefaultPageLimits(), 4096, 128, 16 << 20}
}
func (l currentPresenceTreeLimits) validate() error {
	if err := l.codec.validate(); err != nil {
		return err
	}
	if _, err := l.pages.resolve(); err != nil {
		return err
	}
	if l.targetBytes < currentPresenceHeaderBytes || l.targetBytes > l.codec.maxPageBytes || l.maxEdits < 1 || l.maxEdits > 4096 || l.maxOutputBytes < 1 || l.maxOutputBytes > 64<<20 {
		return ErrInvalid
	}
	return nil
}
func cpAtomCompare(a, b currentPresenceAtom, l currentPresenceLimits) (int, error) {
	if c := cmp.Or(cmp.Compare(a.endpoint, b.endpoint), cpAxisCompare(a.axis, b.axis), cmp.Compare(a.mode, b.mode), cmp.Compare(a.bound, b.bound)); c != 0 {
		return c, nil
	}
	c, err := cpCompareBound(currentPresencePage{wire: a.lower}, 0, a.flags&cpLowerInfinite, currentPresencePage{wire: b.lower}, 0, b.flags&cpLowerInfinite, a.axis.profile, l)
	if err != nil {
		return 0, err
	}
	closed := func(a currentPresenceAtom) byte {
		if a.flags&cpPoint != 0 {
			return cpLowerClosed
		}
		return a.flags & cpLowerClosed
	}
	if c == 0 {
		c = cmp.Compare(closed(b), closed(a))
	}
	return cmp.Or(c, cmp.Compare(a.relationship, b.relationship), cmp.Compare(a.life, b.life)), nil
}
func cpAtomEqual(a, b currentPresenceAtom) bool {
	return a.axis == b.axis && a.endpoint == b.endpoint && a.bound == b.bound && a.mode == b.mode && a.relationship == b.relationship && a.life == b.life && a.flags == b.flags && bytes.Equal(a.lower, b.lower) && bytes.Equal(a.upper, b.upper)
}
func cpAtomFromFence(p currentPresencePage, f currentPresenceFence, l currentPresenceLimits) (currentPresenceAtom, error) {
	g := p.groups[f.group]
	a := currentPresenceAtom{axis: p.axes[g.axis], endpoint: g.endpoint, bound: g.bound, mode: g.mode, relationship: f.relationship, life: f.life, flags: f.flags}
	if f.flags&cpLowerInfinite == 0 {
		c, err := cpReadCoordinate(p.wire, f.lower, a.axis.profile, l.temporal)
		if err != nil {
			return currentPresenceAtom{}, err
		}
		a.lower = p.wire[int(f.lower):c.end]
	}
	return a, nil
}
func cpAtomFromRow(p currentPresencePage, r currentPresenceRow, l currentPresenceLimits) (currentPresenceAtom, error) {
	a, err := cpAtomFromFence(p, cpRowFence(r), l)
	if err != nil {
		return currentPresenceAtom{}, err
	}
	a.flags = r.flags
	if r.flags&(cpPoint|cpUpperInfinite) == 0 {
		c, err := cpReadCoordinate(p.wire, r.upper, a.axis.profile, l.temporal)
		if err != nil {
			return currentPresenceAtom{}, err
		}
		a.upper = p.wire[int(r.upper):c.end]
	}
	return a, nil
}
func (q *cpTreeOperation) charge(n int) error {
	if err := q.q.ctx.Err(); err != nil {
		return err
	}
	if err := q.q.materialize(n); err != nil {
		return err
	}
	return q.budget()
}
func (q *cpTreeOperation) load(root currentPresenceTreeRoot) (currentPresencePage, error) {
	if p, ok := q.cache[root.id]; ok {
		if p.digest != root.digest || p.level != root.level {
			return currentPresencePage{}, ErrCorrupt
		}
		s, e := p.summary(q.limits.codec)
		if e != nil || s.count != root.count {
			return currentPresencePage{}, ErrCorrupt
		}
		return p, nil
	}
	if !q.validPhysical(root.id) {
		return currentPresencePage{}, ErrCorrupt
	}
	wire, found, err := q.get(physicalKey(q.root.namespace, currentPresenceRecord, root.id))
	if err != nil {
		return currentPresencePage{}, err
	}
	if !found {
		return currentPresencePage{}, ErrCorrupt
	}
	// Decode caps include remaining aggregate work before allocating tables or
	// numeric scratch. The source KV capacity is already charged by get.
	limits := q.limits.codec
	remaining := q.remainingBytes()
	if remaining < cpPageOwned+4096+cpCacheEntryOwned {
		return currentPresencePage{}, ErrResourceLimit
	}
	if len(wire) < currentPresenceHeaderBytes {
		return currentPresencePage{}, ErrCorrupt
	}
	count := int(binary.BigEndian.Uint16(wire[38:40]))
	if count > 128 || wire[37] >= 8 || wire[37] > 0 && (count < 2 || count > 64) || wire[40] > 128 || binary.BigEndian.Uint16(wire[41:43]) > 128 {
		return currentPresencePage{}, ErrCorrupt
	}
	axes, groups := int(wire[40]), int(binary.BigEndian.Uint16(wire[41:43]))
	rows, children := count, 0
	if wire[37] > 0 {
		rows, children = 0, count
	}
	owned := cpOwnedBytes(len(wire), axes, groups, rows, children)
	if owned > remaining-4096-cpCacheEntryOwned {
		return currentPresencePage{}, ErrResourceLimit
	}
	limits.maxOwnedBytes = min(limits.maxOwnedBytes, owned)
	limits.maxScratchBytes = min(limits.maxScratchBytes, remaining-owned-cpCacheEntryOwned)
	p, u, err := decodeCurrentPresencePage(q.q.ctx, q.root.namespace, root.id, root.digest, wire, limits)
	if err != nil {
		return currentPresencePage{}, err
	}
	if err := q.charge(u.OwnedBytes + u.ScratchBytes + cpCacheEntryOwned); err != nil {
		return currentPresencePage{}, err
	}
	s, err := p.summary(limits)
	if err != nil {
		return currentPresencePage{}, err
	}
	if p.level != root.level || s.count != root.count {
		return currentPresencePage{}, ErrCorrupt
	}
	q.cache[p.id] = p
	q.counters.LoadedPages++
	q.counters.NativeRows += len(p.rows)
	q.work.DirectoryPages++
	return p, nil
}
func (q *cpTreeOperation) child(p currentPresencePage, i int) (currentPresencePage, error) {
	c := p.children[i]
	n, err := q.load(currentPresenceTreeRoot{c.id, c.count, c.digest, p.level - 1})
	if err != nil {
		return currentPresencePage{}, err
	}
	if err := verifyCurrentPresenceChild(p, c, n, q.limits.codec); err != nil {
		return currentPresencePage{}, err
	}
	return n, nil
}

// Coordinate copies are preflighted together with the worst-case compact typed
// tables. Actual capacities remain charged even after dictionary compaction.
func (q *cpTreeOperation) packRows(id uint64, atoms []currentPresenceAtom) (currentPresencePage, error) {
	if len(atoms) > q.limits.codec.maxRows {
		return currentPresencePage{}, ErrResourceLimit
	}
	size := 0
	for _, a := range atoms {
		size += len(a.lower) + len(a.upper)
	}
	if err := q.charge(cpOwnedBytes(size, len(atoms), len(atoms), len(atoms), 0)); err != nil {
		return currentPresencePage{}, err
	}
	p := currentPresencePage{id: id, axes: make([]currentPresenceAxis, 0, len(atoms)), groups: make([]currentPresenceGroup, 0, len(atoms)), rows: make([]currentPresenceRow, len(atoms)), wire: make([]byte, 0, size)}
	for _, a := range atoms {
		p.axes = append(p.axes, a.axis)
	}
	slices.SortFunc(p.axes, cpAxisCompare)
	p.axes = slices.Compact(p.axes)
	for _, a := range atoms {
		axis, found := slices.BinarySearchFunc(p.axes, a.axis, cpAxisCompare)
		if !found {
			return currentPresencePage{}, ErrCorrupt
		}
		p.groups = append(p.groups, currentPresenceGroup{a.endpoint, a.bound, uint8(axis), a.mode})
	}
	slices.SortFunc(p.groups, func(a, b currentPresenceGroup) int { return cpGroupCompare(a, b, p.axes) })
	p.groups = slices.Compact(p.groups)
	for i, a := range atoms {
		axis, _ := slices.BinarySearchFunc(p.axes, a.axis, cpAxisCompare)
		g := currentPresenceGroup{a.endpoint, a.bound, uint8(axis), a.mode}
		group, found := slices.BinarySearchFunc(p.groups, g, func(a, b currentPresenceGroup) int { return cpGroupCompare(a, b, p.axes) })
		if !found {
			return currentPresencePage{}, ErrCorrupt
		}
		r := currentPresenceRow{relationship: a.relationship, life: a.life, group: uint16(group), flags: a.flags}
		if a.flags&cpLowerInfinite == 0 {
			r.lower = uint32(len(p.wire))
			p.wire = append(p.wire, a.lower...)
		}
		if a.flags&(cpPoint|cpUpperInfinite) == 0 {
			r.upper = uint32(len(p.wire))
			p.wire = append(p.wire, a.upper...)
		}
		p.rows[i] = r
	}
	return p, nil
} // #nosec G115 -- dictionary/row limits<=128, coordinate backing<=work cap64MiB.
func (q *cpTreeOperation) packChildren(id uint64, level uint8, refs []cpReference) (currentPresencePage, error) {
	if len(refs) < 2 || len(refs) > q.limits.codec.maxChildren {
		return currentPresencePage{}, ErrResourceLimit
	}
	// Shared dictionaries come from fence references; mixed children carry no
	// hidden axis metadata/extrema and never permit cross-axis pruning.
	if err := q.charge(cpAtomOwned * 2 * len(refs)); err != nil {
		return currentPresencePage{}, err
	}
	atoms := make([]currentPresenceAtom, 0, 2*len(refs))
	for _, ref := range refs {
		for _, f := range []currentPresenceFence{ref.child.first, ref.child.last} {
			a, err := cpAtomFromFence(ref.page, f, q.limits.codec)
			if err != nil {
				return currentPresencePage{}, err
			}
			atoms = append(atoms, a)
		}
	}
	size := 0
	for _, a := range atoms {
		size += len(a.lower)
	}
	for _, r := range refs {
		if !r.child.mixed {
			for _, e := range []currentPresenceExtent{r.child.min, r.child.max} {
				if e.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
					c, err := cpReadCoordinate(r.page.wire, e.offset, r.page.axes[r.child.axis].profile, q.limits.codec.temporal)
					if err != nil {
						return currentPresencePage{}, err
					}
					size += c.end - int(e.offset)
				}
			}
		}
	}
	if err := q.charge(cpOwnedBytes(size, len(atoms), len(atoms), 0, len(refs))); err != nil {
		return currentPresencePage{}, err
	}
	p := currentPresencePage{id: id, level: level, axes: make([]currentPresenceAxis, 0, len(atoms)), groups: make([]currentPresenceGroup, 0, len(atoms)), children: make([]currentPresenceChild, len(refs)), wire: make([]byte, 0, size)}
	for _, a := range atoms {
		p.axes = append(p.axes, a.axis)
	}
	slices.SortFunc(p.axes, cpAxisCompare)
	p.axes = slices.Compact(p.axes)
	for _, a := range atoms {
		axis, _ := slices.BinarySearchFunc(p.axes, a.axis, cpAxisCompare)
		p.groups = append(p.groups, currentPresenceGroup{a.endpoint, a.bound, uint8(axis), a.mode})
	}
	slices.SortFunc(p.groups, func(a, b currentPresenceGroup) int { return cpGroupCompare(a, b, p.axes) })
	p.groups = slices.Compact(p.groups)
	for i, ref := range refs {
		ch := ref.child
		for j, f := range []currentPresenceFence{ch.first, ch.last} {
			a := atoms[2*i+j]
			axis, _ := slices.BinarySearchFunc(p.axes, a.axis, cpAxisCompare)
			group, _ := slices.BinarySearchFunc(p.groups, currentPresenceGroup{a.endpoint, a.bound, uint8(axis), a.mode}, func(a, b currentPresenceGroup) int { return cpGroupCompare(a, b, p.axes) })
			f.group = uint16(group)
			f.lower = 0
			if f.flags&cpLowerInfinite == 0 {
				f.lower = uint32(len(p.wire))
				p.wire = append(p.wire, a.lower...)
			}
			if j == 0 {
				ch.first = f
			} else {
				ch.last = f
			}
		}
		if !ch.mixed {
			axis, _ := slices.BinarySearchFunc(p.axes, ref.page.axes[ref.child.axis], cpAxisCompare)
			ch.axis = uint8(axis)
			for _, pair := range [][2]*currentPresenceExtent{{&ch.min, &ref.child.min}, {&ch.max, &ref.child.max}} {
				e := pair[1]
				pair[0].offset = 0
				if e.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
					coord, err := cpReadCoordinate(ref.page.wire, e.offset, ref.page.axes[ref.child.axis].profile, q.limits.codec.temporal)
					if err != nil {
						return currentPresencePage{}, err
					}
					pair[0].offset = uint32(len(p.wire))
					p.wire = append(p.wire, ref.page.wire[int(e.offset):coord.end]...)
				}
			}
		}
		p.children[i] = ch
	}
	return p, nil
} // #nosec G115 -- dictionaries<=128; coordinate backing bounded by aggregate work cap.
func (q *cpTreeOperation) encode(p currentPresencePage) ([]byte, error) {
	scratch, err := cpScratchBytes(p, q.limits.codec)
	if err != nil {
		return nil, err
	}
	if err := q.charge(scratch); err != nil {
		return nil, err
	}
	u, err := cpValidatePage(q.q.ctx, p, q.limits.codec)
	if err != nil {
		return nil, err
	}
	remaining := q.remainingBytes()
	l := q.limits.codec
	l.maxOwnedBytes = min(l.maxOwnedBytes, remaining)
	wire, usage, err := encodeCurrentPresencePage(q.q.ctx, q.root.namespace, p, l)
	if err != nil {
		return nil, err
	}
	// Input page ownership was charged by pack/load. Encoder reports both input
	// and output retention; only new output capacity is additional here.
	if err := q.charge(usage.OwnedBytes - u.OwnedBytes); err != nil {
		return nil, err
	}
	q.counters.EncodedPages++
	q.counters.LogicalRewriteBytes += len(wire)
	return wire, nil
}
func (q *cpTreeOperation) retain(p currentPresencePage, wire []byte) (cpReference, error) {
	// Cache already owns compact packed coordinates/tables. Keep wire output as
	// the final pending version; it is charged separately from page backing.
	if _, ok := q.cache[p.id]; !ok {
		if err := q.charge(cpCacheEntryOwned); err != nil {
			return cpReference{}, err
		}
	}
	p.digest = currentPresenceDigest(wire)
	q.cache[p.id] = p
	q.dirty[p.id] = wire
	summary, err := p.summary(q.limits.codec)
	return cpReference{summary, p, p.level}, err
}
func (q *cpTreeOperation) leafParts(id uint64, atoms []currentPresenceAtom) ([]cpReference, error) {
	if len(atoms) == 0 {
		return nil, nil
	}
	if len(atoms) <= q.limits.codec.maxRows {
		p, err := q.packRows(id, atoms)
		if err != nil {
			return nil, err
		}
		wire, err := q.encode(p)
		if err == nil && (len(wire) <= q.limits.targetBytes || len(atoms) == 1) {
			ref, e := q.retain(p, wire)
			if e != nil {
				return nil, e
			}
			if err := q.charge(cpReferenceOwned); err != nil {
				return nil, err
			}
			return []cpReference{ref}, nil
		}
		if err != nil && !errors.Is(err, ErrResourceLimit) {
			return nil, err
		}
		if len(atoms) == 1 {
			return nil, ErrResourceLimit
		}
	}
	// Deterministic coordinate-byte pivot, not numerical narrowing. Both sides
	// independently recheck their actual compact encoding/hard limits.
	total := 0
	for _, a := range atoms {
		total += 17 + len(a.lower) + len(a.upper)
	}
	mid, seen := 1, 0
	for i, a := range atoms[:len(atoms)-1] {
		seen += 17 + len(a.lower) + len(a.upper)
		mid = i + 1
		if seen >= total/2 {
			break
		}
	}
	rightID, err := q.reserve()
	if err != nil {
		return nil, err
	}
	q.counters.Splits++
	left, err := q.leafParts(id, atoms[:mid])
	if err != nil {
		return nil, err
	}
	right, err := q.leafParts(rightID, atoms[mid:])
	if err != nil {
		return nil, err
	}
	return q.concatReferences(left, right)
}
func (q *cpTreeOperation) branchParts(id uint64, level uint8, refs []cpReference) ([]cpReference, error) {
	if len(refs) == 0 {
		return nil, nil
	}
	if len(refs) == 1 {
		return refs, nil
	}
	if len(refs) <= q.limits.codec.maxChildren {
		p, err := q.packChildren(id, level, refs)
		if err != nil {
			return nil, err
		}
		wire, err := q.encode(p)
		if err == nil && (len(wire) <= q.limits.targetBytes || len(refs) <= 3) {
			ref, e := q.retain(p, wire)
			if e != nil {
				return nil, e
			}
			if err := q.charge(cpReferenceOwned); err != nil {
				return nil, err
			}
			return []cpReference{ref}, nil
		}
		if err != nil && !errors.Is(err, ErrResourceLimit) {
			return nil, err
		}
		if len(refs) < 4 {
			return nil, ErrResourceLimit
		}
	}
	if len(refs) < 4 {
		return nil, ErrResourceLimit
	}
	if err := q.charge(8 * len(refs)); err != nil {
		return nil, err
	}
	weights := make([]int, len(refs))
	total := 0
	for i, ref := range refs {
		weight := 49
		for _, f := range []currentPresenceFence{ref.child.first, ref.child.last} {
			a, e := cpAtomFromFence(ref.page, f, q.limits.codec)
			if e != nil {
				return nil, e
			}
			weight += 19 + len(a.lower)
		}
		if !ref.child.mixed {
			weight += 3
			for _, ex := range []currentPresenceExtent{ref.child.min, ref.child.max} {
				if ex.flags&(cpLowerInfinite|cpUpperInfinite) == 0 {
					c, e := cpReadCoordinate(ref.page.wire, ex.offset, ref.page.axes[ref.child.axis].profile, q.limits.codec.temporal)
					if e != nil {
						return nil, e
					}
					weight += c.end - int(ex.offset)
				}
			}
		}
		weights[i] = weight
		total += weight
	}
	mid, seen := 2, 0
	for i, weight := range weights[:len(refs)-2] {
		seen += weight
		if i+1 >= 2 {
			mid = i + 1
			if seen >= total/2 {
				break
			}
		}
	}
	rightID, err := q.reserve()
	if err != nil {
		return nil, err
	}
	q.counters.Splits++
	left, err := q.branchParts(id, level, refs[:mid])
	if err != nil {
		return nil, err
	}
	right, err := q.branchParts(rightID, level, refs[mid:])
	if err != nil {
		return nil, err
	}
	return q.concatReferences(left, right)
}
func (q *cpTreeOperation) findAtom(n currentPresencePage, a currentPresenceAtom) (currentPresenceAtom, bool, error) {
	for n.level > 0 {
		i, err := q.childIndex(n, a)
		if err != nil {
			return currentPresenceAtom{}, false, err
		}
		n, err = q.child(n, i)
		if err != nil {
			return currentPresenceAtom{}, false, err
		}
	}
	for _, r := range n.rows {
		v, e := cpAtomFromRow(n, r, q.limits.codec)
		if e != nil {
			return currentPresenceAtom{}, false, e
		}
		order, e := cpAtomCompare(v, a, q.limits.codec)
		if e != nil {
			return currentPresenceAtom{}, false, e
		}
		if order == 0 {
			return v, true, nil
		}
		if order > 0 {
			break
		}
	}
	return currentPresenceAtom{}, false, nil
}
func (q *cpTreeOperation) childIndex(n currentPresencePage, a currentPresenceAtom) (int, error) {
	index := 0
	for i, ch := range n.children {
		first, e := cpAtomFromFence(n, ch.first, q.limits.codec)
		if e != nil {
			return 0, e
		}
		order, e := cpAtomCompare(first, a, q.limits.codec)
		if e != nil {
			return 0, e
		}
		if order > 0 {
			break
		}
		index = i
	}
	return index, nil
}
func (q *cpTreeOperation) mutate(n currentPresencePage, before, after []currentPresenceAtom) ([]cpReference, error) {
	if err := q.q.ctx.Err(); err != nil {
		return nil, err
	}
	if len(before)+len(after) == 0 {
		s, e := n.summary(q.limits.codec)
		if e != nil {
			return nil, e
		}
		if err := q.charge(cpReferenceOwned); err != nil {
			return nil, err
		}
		return []cpReference{{s, n, n.level}}, nil
	}
	if n.level == 0 {
		if err := q.charge(cpAtomOwned * (len(n.rows) + len(after))); err != nil {
			return nil, err
		}
		atoms := make([]currentPresenceAtom, 0, len(n.rows)+len(after))
		removed := 0
		for _, r := range n.rows {
			a, e := cpAtomFromRow(n, r, q.limits.codec)
			if e != nil {
				return nil, e
			}
			keep := true
			for _, old := range before {
				c, e := cpAtomCompare(a, old, q.limits.codec)
				if e != nil {
					return nil, e
				}
				if c == 0 {
					if !cpAtomEqual(a, old) {
						return nil, errors.Join(ErrInvalid, ErrPatchConflict)
					}
					keep = false
					removed++
					break
				}
			}
			if keep {
				atoms = append(atoms, a)
			}
		}
		if removed != len(before) {
			return nil, errors.Join(ErrInvalid, ErrPatchConflict)
		}
		for _, a := range after {
			index := 0
			for index < len(atoms) {
				c, e := cpAtomCompare(atoms[index], a, q.limits.codec)
				if e != nil {
					return nil, e
				}
				if c >= 0 {
					if c == 0 {
						return nil, ErrRebinding
					}
					break
				}
				index++
			}
			atoms = slices.Insert(atoms, index, a)
		}
		return q.leafParts(n.id, atoms)
	}
	if err := q.charge(cpReferenceOwned * (len(n.children) + len(after) + len(before))); err != nil {
		return nil, err
	}
	refs := make([]cpReference, 0, len(n.children)+len(after)+len(before))
	beforeAt, afterAt := 0, 0
	var err error
	for i, ch := range n.children {
		nextBefore, nextAfter := beforeAt, afterAt
		for nextBefore < len(before) {
			index, e := q.childIndex(n, before[nextBefore])
			if e != nil {
				return nil, e
			}
			if index != i {
				break
			}
			nextBefore++
		}
		for nextAfter < len(after) {
			index, e := q.childIndex(n, after[nextAfter])
			if e != nil {
				return nil, e
			}
			if index != i {
				break
			}
			nextAfter++
		}
		if nextBefore == beforeAt && nextAfter == afterAt {
			refs, err = q.appendReference(refs, cpReference{ch, n, n.level - 1})
			if err != nil {
				return nil, err
			}
		} else {
			child, e := q.child(n, i)
			if e != nil {
				return nil, e
			}
			replacement, e := q.mutate(child, before[beforeAt:nextBefore], after[afterAt:nextAfter])
			if e != nil {
				return nil, e
			}
			refs, e = q.appendReferences(refs, replacement)
			if e != nil {
				return nil, e
			}
		}
		beforeAt, afterAt = nextBefore, nextAfter
	}
	if beforeAt != len(before) || afterAt != len(after) {
		return nil, ErrCorrupt
	}
	// Rebalance adjacent leaves/branches after deletes. Every extra sibling load,
	// representation copy and encoding uses the same aggregate cap.
	if len(before) > 0 {
		var err error
		refs, err = q.normalizeLevels(refs)
		if err != nil {
			return nil, err
		}
		if len(refs) > 1 {
			refs, err = q.rebalance(refs, refs[0].level)
		}
		if err != nil {
			return nil, err
		}
	}
	if len(refs) < 2 {
		return refs, nil
	}
	return q.branchParts(n.id, refs[0].level+1, refs)
}
func (q *cpTreeOperation) rebalance(refs []cpReference, level uint8) ([]cpReference, error) {
	for i := 0; i+1 < len(refs); i++ {
		underfull := func(ref cpReference) bool {
			p, known := q.cache[ref.child.id]
			wire, changed := q.dirty[ref.child.id]
			if !known || !changed || len(wire) >= q.limits.targetBytes/2 {
				return false
			}
			if level == 0 {
				return len(p.rows) < max(1, q.limits.codec.maxRows/2)
			}
			return len(p.children) < max(2, q.limits.codec.maxChildren/2)
		}
		if !underfull(refs[i]) && !underfull(refs[i+1]) {
			continue
		}
		left, err := q.load(currentPresenceTreeRoot{refs[i].child.id, refs[i].child.count, refs[i].child.digest, level})
		if err != nil {
			return nil, err
		}
		right, err := q.load(currentPresenceTreeRoot{refs[i+1].child.id, refs[i+1].child.count, refs[i+1].child.digest, level})
		if err != nil {
			return nil, err
		}
		q.counters.Rebalances++
		var merged []cpReference
		if level == 0 {
			if err := q.charge(cpAtomOwned * (len(left.rows) + len(right.rows))); err != nil {
				return nil, err
			}
			atoms := make([]currentPresenceAtom, 0, len(left.rows)+len(right.rows))
			for _, p := range []currentPresencePage{left, right} {
				for _, r := range p.rows {
					a, e := cpAtomFromRow(p, r, q.limits.codec)
					if e != nil {
						return nil, e
					}
					atoms = append(atoms, a)
				}
			}
			merged, err = q.leafParts(left.id, atoms)
		} else {
			if err := q.charge(cpReferenceOwned * (len(left.children) + len(right.children))); err != nil {
				return nil, err
			}
			children := make([]cpReference, 0, len(left.children)+len(right.children))
			for _, p := range []currentPresencePage{left, right} {
				for _, ch := range p.children {
					children = append(children, cpReference{ch, p, p.level - 1})
				}
			}
			merged, err = q.branchParts(left.id, level, children)
		}
		if err != nil {
			return nil, err
		}
		refs, err = q.replaceReferences(refs, i, i+2, merged)
		if err != nil {
			return nil, err
		}
	}
	return refs, nil
}
func cpCloneEdits(q *cpTreeOperation, input []currentPresenceAtom) ([]currentPresenceAtom, error) {
	valueCap := min(cmpDefaultTemporalValueBytes(q.limits.codec.temporal), cmpDefaultTemporalInputBytes(q.limits.codec.temporal))
	for _, a := range input {
		if err := q.q.ctx.Err(); err != nil {
			return nil, err
		}
		for _, body := range []struct {
			wire    []byte
			present bool
		}{{a.lower, a.flags&cpLowerInfinite == 0}, {a.upper, a.flags&(cpPoint|cpUpperInfinite) == 0}} {
			if !body.present {
				if len(body.wire) != 0 {
					return nil, ErrInvalid
				}
				continue
			}
			if len(body.wire) > valueCap-52 {
				return nil, ErrResourceLimit
			}
			c := cursor{src: body.wire}
			parts := 1
			if a.axis.profile == temporal.ProfileRationalQ {
				parts = 2
			} else if a.axis.profile == temporal.ProfileLexicographicQN {
				parts = 3
			} else if a.axis.profile != temporal.ProfileIntegerZ {
				return nil, ErrInvalid
			}
			for range parts {
				if _, err := cpReadInteger(&c, q.limits.codec.temporal); err != nil {
					return nil, callerError(err)
				}
			}
			if c.done() != nil {
				return nil, ErrInvalid
			}
		}
	}
	cost := cpAtomOwned * len(input)
	for _, a := range input {
		cost += len(a.lower) + len(a.upper)
	}
	if err := q.charge(cost); err != nil {
		return nil, err
	}
	out := make([]currentPresenceAtom, len(input))
	for i, a := range input {
		if err := q.q.ctx.Err(); err != nil {
			return nil, err
		}
		a.lower = exactCopy(a.lower)
		a.upper = exactCopy(a.upper)
		p, err := q.packRows(1, []currentPresenceAtom{a})
		if err != nil {
			return nil, err
		}
		scratch, e := cpScratchBytes(p, q.limits.codec)
		if e != nil {
			return nil, callerError(e)
		}
		if err := q.charge(scratch); err != nil {
			return nil, err
		}
		if _, err := cpValidatePage(q.q.ctx, p, q.limits.codec); err != nil {
			if errors.Is(err, ErrResourceLimit) {
				return nil, err
			}
			return nil, ErrInvalid
		}
		for _, body := range []struct {
			wire    []byte
			present bool
		}{{a.lower, a.flags&cpLowerInfinite == 0}, {a.upper, a.flags&(cpPoint|cpUpperInfinite) == 0}} {
			if !body.present {
				if len(body.wire) != 0 {
					return nil, ErrInvalid
				}
				continue
			}
			c, e := cpReadCoordinate(body.wire, 0, a.axis.profile, q.limits.codec.temporal)
			if e != nil {
				return nil, callerError(e)
			}
			if c.end != len(body.wire) {
				return nil, ErrInvalid
			}
		}
		if i > 0 {
			c, e := cpAtomCompare(out[i-1], a, q.limits.codec)
			if e != nil {
				return nil, callerError(e)
			}
			if c >= 0 {
				return nil, ErrInvalid
			}
		}
		for _, prior := range out[:i] {
			if prior.axis.id == a.axis.id && prior.axis != a.axis {
				return nil, ErrInvalid
			}
		}
		out[i] = a
	}
	return out, nil
}

// Entire checked edit collections are one atomic private staging operation.
// The returned physical reservations are not logical effects or a graph root
// capability. Tree descriptors/physical Root integration are deliberately absent.
func stageCurrentPresenceAtoms(ctx context.Context, s *Stage, root Root, tree currentPresenceTreeRoot, before, after []currentPresenceAtom, l currentPresenceTreeLimits) (stagedCurrentPresence, error) {
	if err := cpCheckContext(ctx); err != nil {
		return stagedCurrentPresence{}, err
	}
	if s == nil {
		return stagedCurrentPresence{}, ErrInvalid
	}
	if err := l.validate(); err != nil {
		return stagedCurrentPresence{}, err
	}
	if uint64(len(after)) > math.MaxUint64-tree.count {
		return stagedCurrentPresence{}, ErrResourceLimit
	}
	if len(before)+len(after) > l.maxEdits {
		return stagedCurrentPresence{}, ErrResourceLimit
	}
	pages, err := l.pages.resolve()
	if err != nil {
		return stagedCurrentPresence{}, err
	}
	l.pages = pages
	var result stagedCurrentPresence
	err = s.operation(ctx, func(base *reader) error {
		base.maxRows, base.maxBytes = l.pages.MaxWorkRecords, l.pages.MaxWorkBytes
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := base.materialize(cpTreeOperationOwned + cpTreeResultOwned); err != nil {
			return err
		}
		q := cpTreeOperation{pageStage: &pageStage{pageReader: &pageReader{q: base, limits: l.pages}, root: root}, limits: l, cache: make(map[uint64]currentPresencePage), dirty: make(map[uint64][]byte)}
		q.allocation = &q.root
		if err := q.budget(); err != nil {
			return err
		}
		if err := validatePrivateRoot(s, root); err != nil {
			return err
		}
		for _, row := range s.writes {
			if len(row.Key) == 33 && row.Key[0] == byte(currentPresenceRecord) && binary.BigEndian.Uint64(row.Key[25:]) >= root.next {
				return ErrInvalid
			}
		}
		old, err := cpCloneEdits(&q, before)
		if err != nil {
			return err
		}
		fresh, err := cpCloneEdits(&q, after)
		if err != nil {
			return err
		}
		var n currentPresencePage
		if tree.id == 0 {
			if tree != (currentPresenceTreeRoot{}) || len(old) != 0 {
				return ErrInvalid
			}
			id, err := q.reserve()
			if err != nil {
				return err
			}
			n = currentPresencePage{id: id}
			if err := q.charge(cpPageOwned); err != nil {
				return err
			}
		} else {
			n, err = q.load(tree)
			if err != nil {
				return err
			}
		}
		// Verify all before images before mutation/reservation. No arbitrary Delta
		// entrypoint exists; native edits must match the captured tree exactly.
		for _, a := range old {
			actual, found, e := q.findAtom(n, a)
			if e != nil {
				return e
			}
			if !found || !cpAtomEqual(actual, a) {
				return errors.Join(ErrInvalid, ErrPatchConflict)
			}
		}
		refs, err := q.mutate(n, old, fresh)
		if err != nil {
			return err
		}
		if len(refs) == 0 || tree.id == 0 && len(old)+len(fresh) == 0 {
			empty := currentPresencePage{id: n.id}
			wire, e := q.encode(empty)
			if e != nil {
				return e
			}
			ref, e := q.retain(empty, wire)
			if e != nil {
				return e
			}
			if err := q.charge(cpReferenceOwned); err != nil {
				return err
			}
			refs = []cpReference{ref}
		}
		for len(refs) > 1 {
			level := refs[0].level + 1
			if int(level) >= l.codec.maxLevels {
				return ErrResourceLimit
			}
			id, e := q.reserve()
			if e != nil {
				return e
			}
			refs, e = q.branchParts(id, level, refs)
			if e != nil {
				return e
			}
		}
		final := refs[0]
		if final.child.count == math.MaxUint64 {
			return ErrResourceLimit
		}
		// Write only reachable dirty pages. Unreachable split/rebalance temporaries
		// consumed reservations/work but never become additional retained KV rows.
		if err := q.charge(128 + cpCacheEntryOwned*len(q.cache)); err != nil {
			return err
		}
		reachable := map[uint64]bool{}
		var visit func(cpReference) error
		visit = func(ref cpReference) error {
			if reachable[ref.child.id] {
				return ErrCorrupt
			}
			reachable[ref.child.id] = true
			p, dirty := q.cache[ref.child.id]
			if !dirty {
				return nil
			}
			if _, changed := q.dirty[p.id]; !changed {
				return nil
			}
			for _, ch := range p.children {
				if sub, ok := q.cache[ch.id]; ok {
					if err := visit(cpReference{ch, sub, sub.level}); err != nil {
						return err
					}
				}
			}
			return nil
		}
		if err := visit(final); err != nil {
			return err
		}
		for id, wire := range q.dirty {
			if !reachable[id] {
				continue
			}
			key := physicalKey(q.root.namespace, currentPresenceRecord, id)
			if err := q.charge(2*len(key) + len(wire) + 64); err != nil {
				return err
			}
			if err := base.put(key, wire); err != nil {
				return err
			}
			q.counters.FinalPageWrites++
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := q.budget(); err != nil {
			return err
		}
		q.counters.PageWork = q.work
		result = stagedCurrentPresence{q.root, currentPresenceTreeRoot{final.child.id, final.child.count, final.child.digest, final.level}, q.counters}
		return nil
	})
	if err != nil {
		return stagedCurrentPresence{}, err
	}
	return result, nil
}

func (q *cpTreeOperation) resolve(ref cpReference) (currentPresencePage, error) {
	p, err := q.load(currentPresenceTreeRoot{ref.child.id, ref.child.count, ref.child.digest, ref.level})
	if err != nil {
		return currentPresencePage{}, err
	}
	if ref.page.id != ref.child.id {
		if err := verifyCurrentPresenceChild(ref.page, ref.child, p, q.limits.codec); err != nil {
			return currentPresencePage{}, err
		}
	}
	return p, nil
}

// A deletion can collapse a subtree. Join only its adjacent sibling boundary,
// preserving uniform child heights without scanning unrelated subtrees.
func (q *cpTreeOperation) join(left, right cpReference) ([]cpReference, error) {
	a, err := q.resolve(left)
	if err != nil {
		return nil, err
	}
	b, err := q.resolve(right)
	if err != nil {
		return nil, err
	}
	q.counters.Rebalances++
	if left.level == right.level {
		if left.level == 0 {
			if err := q.charge(cpAtomOwned * (len(a.rows) + len(b.rows))); err != nil {
				return nil, err
			}
			atoms := make([]currentPresenceAtom, 0, len(a.rows)+len(b.rows))
			for _, p := range []currentPresencePage{a, b} {
				for _, r := range p.rows {
					v, e := cpAtomFromRow(p, r, q.limits.codec)
					if e != nil {
						return nil, e
					}
					atoms = append(atoms, v)
				}
			}
			return q.leafParts(a.id, atoms)
		}
		if err := q.charge(cpReferenceOwned * (len(a.children) + len(b.children))); err != nil {
			return nil, err
		}
		refs := make([]cpReference, 0, len(a.children)+len(b.children))
		for _, p := range []currentPresencePage{a, b} {
			for _, ch := range p.children {
				refs = append(refs, cpReference{ch, p, p.level - 1})
			}
		}
		return q.branchParts(a.id, a.level, refs)
	}
	high := a
	low := right
	boundary := len(high.children) - 1
	if left.level < right.level {
		high = b
		low = left
		boundary = 0
	}
	child := cpReference{high.children[boundary], high, high.level - 1}
	var replacement []cpReference
	if left.level < right.level {
		replacement, err = q.join(low, child)
	} else {
		replacement, err = q.join(child, low)
	}
	if err != nil {
		return nil, err
	}
	if err := q.charge(cpReferenceOwned * (len(high.children) + len(replacement))); err != nil {
		return nil, err
	}
	refs := make([]cpReference, 0, len(high.children)+len(replacement))
	for i, ch := range high.children {
		if i == boundary {
			refs, err = q.appendReferences(refs, replacement)
			if err != nil {
				return nil, err
			}
		} else {
			refs, err = q.appendReference(refs, cpReference{ch, high, high.level - 1})
			if err != nil {
				return nil, err
			}
		}
	}
	return q.branchParts(high.id, high.level, refs)
}
func (q *cpTreeOperation) normalizeLevels(refs []cpReference) ([]cpReference, error) {
	for i := 0; i+1 < len(refs); {
		if refs[i].level == refs[i+1].level {
			i++
			continue
		}
		joined, err := q.join(refs[i], refs[i+1])
		if err != nil {
			return nil, err
		}
		if err := q.charge(cpReferenceOwned * (len(refs) + len(joined))); err != nil {
			return nil, err
		}
		refs, err = q.replaceReferences(refs, i, i+2, joined)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			i--
		}
	}
	return refs, nil
}

// Each recursive array is independently owned while its inputs remain live.
// Allocate exact capacity so the representation ledger never assumes append's
// growth policy or releases a still-borrowed split half early.
func (q *cpTreeOperation) concatReferences(a, b []cpReference) ([]cpReference, error) {
	size := len(a) + len(b)
	if err := q.charge(cpReferenceOwned * size); err != nil {
		return nil, err
	}
	out := make([]cpReference, size)
	copy(out, a)
	copy(out[len(a):], b)
	return out, nil
}
func (q *cpTreeOperation) replaceReferences(refs []cpReference, start, end int, replacement []cpReference) ([]cpReference, error) {
	size := len(refs) - (end - start) + len(replacement)
	if size <= cap(refs) {
		return slices.Replace(refs, start, end, replacement...), nil
	}
	if err := q.charge(cpReferenceOwned * size); err != nil {
		return nil, err
	}
	out := make([]cpReference, size)
	copy(out, refs[:start])
	copy(out[start:], replacement)
	copy(out[start+len(replacement):], refs[end:])
	return out, nil
}

func (q *cpTreeOperation) remainingBytes() int {
	limit := min(q.q.c.limits.MaxReadBytes, q.limits.pages.MaxWorkBytes)
	if q.q.maxBytes > 0 {
		limit = min(limit, q.q.maxBytes)
	}
	return limit - q.q.bytes
}

// Rebalance may visit several adjacent pairs for K edits. All cache/array/
// serialization work is cumulative under finite fanout, height and source caps;
// the two-path estimate does not describe arbitrary moves or regional edits.

func (q *cpTreeOperation) appendReferences(refs, additional []cpReference) ([]cpReference, error) {
	needed := len(refs) + len(additional)
	if needed <= cap(refs) {
		return append(refs, additional...), nil
	}
	if err := q.charge(cpReferenceOwned * needed); err != nil {
		return nil, err
	}
	out := make([]cpReference, len(refs), needed)
	copy(out, refs)
	return append(out, additional...), nil
}

func (q *cpTreeOperation) appendReference(refs []cpReference, ref cpReference) ([]cpReference, error) {
	needed := len(refs) + 1
	if needed <= cap(refs) {
		return append(refs, ref), nil
	}
	if err := q.charge(cpReferenceOwned * needed); err != nil {
		return nil, err
	}
	out := make([]cpReference, len(refs), needed)
	copy(out, refs)
	return append(out, ref), nil
}
