package graphstore

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"maps"
	"strings"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// This private door returns life-qualified OWN-presence candidates. It does not
// test either endpoint's temporal effectivity, and cannot certify an effective
// neighborhood. Type/witness residual costs and endpoint masking remain open.
type currentPresenceQuery struct {
	endpoint  graphstate.EntityID
	life      graphstate.LifeID // optional; restricts LifeBound only, never IdentityReference
	at        temporal.Position
	mode      graphstate.ReferenceMode // zero selects both modes
	direction byte                     // cpSource, cpTarget, or both
	typ       string                   // optional immutable relationship type; residual, not indexed
}
type cpPredicate struct {
	endpoint   graphstate.EntityID
	life       graphstate.LifeID
	axis       currentPresenceAxis
	mode       graphstate.ReferenceMode
	direction  byte
	typ        string
	coordinate []byte
}
type currentPresenceCandidate struct {
	relationship graphstate.EntityID
	life         graphstate.LifeID
	roles        byte
}
type currentPresenceCandidatePage struct {
	candidates []currentPresenceCandidate
	next       graphstate.Cursor
	complete   bool
	work       currentPresenceTreeWork
	visited    int
}
type cpIteratorFrame struct {
	page  currentPresencePage
	index int
}
type currentPresenceIterator struct {
	mu                    sync.Mutex
	c                     *Catalog
	op                    *cpTreeOperation
	tree                  currentPresenceTreeRoot
	predicate             cpPredicate
	binding               [32]byte
	generation, index     uint64
	imageHash             [32]byte
	frames                []cpIteratorFrame
	last                  currentPresenceAtom // full consumed atom key, not merely relationship/life
	cursor                graphstate.Cursor
	started, done, closed bool
	outputBytes           int
}

const (
	cpIteratorOwned        = 768
	cpIteratorFrameOwned   = 256
	cpPredicateOwned       = 256
	cpCandidateOwned       = 32
	cpCandidatePageOwned   = 256
	cpIteratorAttemptOwned = 4096
)

func cpPredicateHash(n Namespace, tree currentPresenceTreeRoot, p cpPredicate, generation, index uint64, imageHash [32]byte) [32]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("rho-tkg:own-presence-query:v1\x00"))
	_, _ = h.Write(n.Graph[:])
	var buf [8]byte
	for _, value := range []uint64{n.Partition, tree.id, tree.count, generation, index, uint64(p.endpoint), uint64(p.life)} {
		binary.BigEndian.PutUint64(buf[:], value)
		_, _ = h.Write(buf[:])
	}
	_, _ = h.Write([]byte{tree.level, byte(p.mode), p.direction, byte(p.axis.profile)})
	_, _ = h.Write(tree.digest[:])
	_, _ = h.Write(imageHash[:])
	_, _ = h.Write(p.axis.id[:])
	_, _ = h.Write(p.axis.hash[:])
	binary.BigEndian.PutUint64(buf[:], uint64(len(p.coordinate)))
	_, _ = h.Write(buf[:])
	_, _ = h.Write(p.coordinate)
	binary.BigEndian.PutUint64(buf[:], uint64(len(p.typ)))
	_, _ = h.Write(buf[:])
	_, _ = h.Write([]byte(p.typ))
	var out [32]byte
	h.Sum(out[:0])
	return out
}
func newCurrentPresenceIterator(ctx context.Context, c *Catalog, tree currentPresenceTreeRoot, query currentPresenceQuery, l currentPresenceTreeLimits) (*currentPresenceIterator, error) {
	if err := cpCheckContext(ctx); err != nil {
		return nil, err
	}
	if c == nil {
		return nil, ErrInvalid
	}
	if err := l.validate(); err != nil {
		return nil, err
	}
	pages, err := l.pages.resolve()
	if err != nil {
		return nil, err
	}
	l.pages = pages
	if query.endpoint == 0 || query.mode != 0 && query.mode != graphstate.LifeBound && query.mode != graphstate.IdentityReference || query.direction == 0 || query.direction & ^(cpSource|cpTarget) != 0 || query.typ != "" && !validName(query.typ, c.limits) {
		return nil, ErrInvalid
	}
	if tree.id == 0 || tree.digest == ([32]byte{}) || tree.id >= c.root.next {
		return nil, ErrInvalid
	}
	if err := query.at.Axis().Validate(l.codec.temporal); err != nil {
		return nil, callerError(err)
	}
	r, err := c.reader(ctx)
	if err != nil {
		return nil, err
	}
	r.maxRows, r.maxBytes = l.pages.MaxWorkRecords, l.pages.MaxWorkBytes
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := r.materialize(cpIteratorOwned + cpTreeOperationOwned + cpPredicateOwned + cpIteratorFrameOwned*l.codec.maxLevels + 2*len(query.typ)); err != nil {
		return nil, err
	}
	q := &cpTreeOperation{pageStage: &pageStage{pageReader: &pageReader{q: r, limits: l.pages}, root: c.root}, limits: l, cache: make(map[uint64]currentPresencePage), dirty: make(map[uint64][]byte)}

	d := query.at.Axis().Descriptor()
	if err := q.charge(96); err != nil {
		return nil, err
	}
	axis, found, err := r.axis(d.ID)
	if err != nil {
		return nil, c.failure(err)
	}
	if !found || axis.Descriptor() != d || axis.DefinitionHash() != query.at.Axis().DefinitionHash() {
		return nil, errors.Join(ErrInvalid, temporal.ErrAxisMismatch)
	}
	remaining := q.remainingBytes()
	if remaining < 2*(52+3) {
		return nil, ErrResourceLimit
	}
	tl := l.codec.temporal
	tl.MaxValueBytes = min(remaining/2, cmpDefaultTemporalValueBytes(tl))
	tl.MaxInputBytes = min(remaining/2, cmpDefaultTemporalInputBytes(tl))
	encoded, err := temporal.AppendPosition(nil, query.at, tl)
	if err != nil {
		return nil, callerError(err)
	}
	queryScratch := 2048 + 64*(len(encoded)-52+32)
	if queryScratch > l.codec.maxScratchBytes {
		return nil, ErrResourceLimit
	}
	if err := q.charge(cap(encoded) + len(encoded) - 52); err != nil {
		return nil, err
	}
	p := cpPredicate{query.endpoint, query.life, currentPresenceAxis{d.ID, axis.DefinitionHash(), d.Profile}, query.mode, query.direction, strings.Clone(query.typ), exactCopy(encoded[52:])}
	app, err := c.view.Root()
	if err != nil {
		return nil, err
	}
	if err := q.charge(cap(app.Image)); err != nil {
		return nil, err
	}
	it := &currentPresenceIterator{c: c, op: q, tree: tree, predicate: p, generation: app.Generation, index: app.Index, imageHash: app.ImageHash, frames: make([]cpIteratorFrame, 0, l.codec.maxLevels)}
	it.binding = cpPredicateHash(c.root.namespace, tree, p, app.Generation, app.Index, app.ImageHash)
	if it.retainedBytes(nil, nil) > l.pages.MaxCursorBytes {
		return nil, ErrResourceLimit
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return it, nil
}
func cmpDefaultTemporalValueBytes(l temporal.Limits) int {
	if l.MaxValueBytes == 0 {
		return temporal.DefaultLimits().MaxValueBytes
	}
	return l.MaxValueBytes
}
func cmpDefaultTemporalInputBytes(l temporal.Limits) int {
	if l.MaxInputBytes == 0 {
		return temporal.DefaultLimits().MaxInputBytes
	}
	return l.MaxInputBytes
}
func (it *currentPresenceIterator) retainedBytes(cache map[uint64]currentPresencePage, last *currentPresenceAtom) int {
	bytes := cpIteratorOwned + cpTreeOperationOwned + cpPredicateOwned + cap(it.frames)*cpIteratorFrameOwned + len(it.predicate.typ) + cap(it.predicate.coordinate)
	for _, p := range cache {
		bytes += cpCacheEntryOwned + cpOwnedBytes(cap(p.wire), cap(p.axes), cap(p.groups), cap(p.rows), cap(p.children))
	}
	if last != nil {
		bytes += cpAtomOwned + cap(last.lower)
	}
	return bytes
}
func (it *currentPresenceIterator) close() error {
	if it == nil {
		return ErrInvalid
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	it.closed = true
	it.frames = nil
	it.op.cache = nil
	it.op.dirty = nil
	it.last = currentPresenceAtom{}
	it.predicate.coordinate = nil
	it.predicate.typ = ""
	return nil
}
func cpPrefixCompare(a currentPresenceAtom, p cpPredicate) int {
	if a.endpoint < p.endpoint {
		return -1
	}
	if a.endpoint > p.endpoint {
		return 1
	}
	if c := cpAxisCompare(a.axis, p.axis); c != 0 {
		return c
	}
	if p.mode != 0 {
		if a.mode < p.mode {
			return -1
		}
		if a.mode > p.mode {
			return 1
		}
		if p.mode == graphstate.LifeBound && p.life != 0 {
			if a.bound < p.life {
				return -1
			}
			if a.bound > p.life {
				return 1
			}
		}
	}
	return 0
}
func cpAtomMatchesPrefix(a currentPresenceAtom, p cpPredicate) bool {
	return a.endpoint == p.endpoint && a.axis == p.axis && (p.mode == 0 || p.mode == a.mode) && (a.mode == graphstate.IdentityReference || p.life == 0 || p.life == a.bound) && a.flags&p.direction != 0
}
func cpContainsAtom(a currentPresenceAtom, p cpPredicate, l currentPresenceLimits) (bool, error) {
	query := currentPresencePage{wire: p.coordinate}
	lower := currentPresencePage{wire: a.lower}
	c, err := cpCompareBound(lower, 0, a.flags&cpLowerInfinite, query, 0, 0, p.axis.profile, l)
	if err != nil {
		return false, err
	}
	if a.flags&cpPoint != 0 {
		return c == 0, nil
	}
	if c > 0 || c == 0 && a.flags&cpLowerClosed == 0 {
		return false, nil
	}
	c, err = cpCompareBound(currentPresencePage{wire: a.upper}, 0, a.flags&cpUpperInfinite, query, 0, 0, p.axis.profile, l)
	if err != nil {
		return false, err
	}
	return c > 0 || c == 0 && a.flags&cpUpperClosed != 0, nil
}
func (q *cpTreeOperation) candidateWitness(a currentPresenceAtom, p cpPredicate) (currentPresenceCandidate, bool, error) {
	// Residual metadata checks occur only after compact own-support membership.
	// On primitive fixture roots fixed metadata is charged here; Full integration
	// must reuse the complete view's accounting instead of double charging.
	if err := q.charge(192); err != nil {
		return currentPresenceCandidate{}, false, err
	}
	q.counters.WitnessEntityCalls++
	beforeRecords := q.q.rows
	defer func() { q.counters.ResidualRecords += q.q.rows - beforeRecords }()
	entity, found, err := q.q.entity(EntityRef{q.root.namespace.Graph, a.relationship})
	if err != nil {
		return currentPresenceCandidate{}, false, err
	}
	if !found || entity.Kind != graphstate.Relationship || entity.Mode != a.mode || entity.Axis.Descriptor().ID != a.axis.id || entity.Axis.DefinitionHash() != a.axis.hash {
		return currentPresenceCandidate{}, false, ErrCorrupt
	}
	if p.typ != "" && entity.Type != p.typ {
		return currentPresenceCandidate{}, false, nil
	}
	if err := q.charge(256); err != nil {
		return currentPresenceCandidate{}, false, err
	}
	q.counters.WitnessLifeCalls++
	life, found, err := q.q.life(LifeRef{q.root.namespace.Graph, a.relationship, a.life})
	if err != nil {
		return currentPresenceCandidate{}, false, err
	}
	if !found {
		return currentPresenceCandidate{}, false, ErrCorrupt
	}
	roles := a.flags & (cpSource | cpTarget)
	if roles&cpSource != 0 && (entity.Source != a.endpoint || a.mode == graphstate.LifeBound && life.SourceLife != a.bound) || roles&cpTarget != 0 && (entity.Target != a.endpoint || a.mode == graphstate.LifeBound && life.TargetLife != a.bound) {
		return currentPresenceCandidate{}, false, ErrCorrupt
	}
	matched := roles & p.direction
	if entity.Source == entity.Target && p.direction == cpSource|cpTarget {
		// For Both/all-life, one deterministic source role witnesses the self-loop,
		// independent of group ordering/page boundaries. Qualified life can instead
		// select only the target run. No lifetime-sized seen-ID set is retained.
		if a.mode == graphstate.IdentityReference || p.life == 0 || life.SourceLife == p.life {
			if roles&cpSource == 0 {
				return currentPresenceCandidate{}, false, nil
			}
			matched = cpSource | cpTarget
			if a.mode == graphstate.LifeBound && p.life != 0 && life.TargetLife != p.life {
				matched = cpSource
			}
		}
	}
	return currentPresenceCandidate{a.relationship, a.life, matched}, matched != 0, nil
}
func (q *cpTreeOperation) childMayMatch(parent currentPresencePage, ch currentPresenceChild, p cpPredicate) (bool, error) {
	first, err := cpAtomFromFence(parent, ch.first, q.limits.codec)
	if err != nil {
		return false, err
	}
	last, err := cpAtomFromFence(parent, ch.last, q.limits.codec)
	if err != nil {
		return false, err
	}
	prefixes := [2]cpPredicate{p, p}
	total := 1
	if p.mode == 0 && p.life != 0 {
		prefixes[0].mode = graphstate.LifeBound
		prefixes[1].mode = graphstate.IdentityReference
		total = 2
	}
	selected := false
	for _, prefix := range prefixes[:total] {
		if cpPrefixCompare(first, prefix) <= 0 && cpPrefixCompare(last, prefix) >= 0 {
			selected = true
			break
		}
	}
	if !selected {
		return false, nil
	}
	scratch, e := cpScratchBytes(parent, q.limits.codec)
	if e != nil {
		return false, e
	}
	if err := q.charge(max(scratch, 2048+64*(len(p.coordinate)+32)) + 256 + len(p.coordinate)); err != nil {
		return false, err
	}
	return cpSummaryMayContain(parent, ch, p.axis, p.coordinate, q.limits.codec)
}
func (it *currentPresenceIterator) next(ctx context.Context, token graphstate.Cursor, budget graphstate.ReadBudget) (currentPresenceCandidatePage, error) {
	if it == nil {
		return currentPresenceCandidatePage{}, ErrInvalid
	}
	it.mu.Lock()
	defer it.mu.Unlock()
	if err := cpCheckContext(ctx); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	if it.closed {
		return currentPresenceCandidatePage{}, ErrClosed
	}
	if token != it.cursor || budget.Rows < 1 || budget.Bytes < cpCandidatePageOwned {
		return currentPresenceCandidatePage{}, ErrInvalid
	}
	if budget.Bytes < cpCandidatePageOwned+cpCandidateOwned && it.tree.count != 0 {
		return currentPresenceCandidatePage{}, ErrResourceLimit
	}
	if err := it.c.check(ctx); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	app, err := it.c.view.Root()
	if err != nil {
		return currentPresenceCandidatePage{}, err
	}
	it.op.q.ctx = ctx
	if err := it.op.charge(256 + len(it.predicate.typ)); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	if app.Generation != it.generation || app.Index != it.index || app.ImageHash != it.imageHash || cpPredicateHash(it.c.root.namespace, it.tree, it.predicate, app.Generation, app.Index, app.ImageHash) != it.binding {
		return currentPresenceCandidatePage{}, ErrInvalid
	}
	it.op.q.ctx = ctx
	// Attempt state shares immutable page backing, but all additional map/frame
	// headers are charged before copying. Source work remains charged on failure;
	// output quota/cursor/path publication is transactional.
	attempt := *it.op
	defer func() {
		_ = attempt.budget()
		attempt.counters.PageWork = attempt.work
		it.op.counters = attempt.counters
		it.op.work = attempt.work
	}()
	if err := attempt.charge(cpIteratorAttemptOwned + cpCacheEntryOwned*len(it.op.cache) + cpIteratorFrameOwned*cap(it.frames)); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	attempt.cache = maps.Clone(it.op.cache)
	frames := make([]cpIteratorFrame, len(it.frames), cap(it.frames))
	copy(frames, it.frames)
	last := it.last
	started, done := it.started, it.done
	if !started {
		if e := it.admitPage(&attempt, last); e != nil {
			return currentPresenceCandidatePage{}, e
		}
		root, e := attempt.load(it.tree)
		if e != nil {
			return currentPresenceCandidatePage{}, it.c.failure(e)
		}
		frames = append(frames, cpIteratorFrame{page: root})
		started = true
	}
	count := min(budget.Rows, attempt.limits.codec.maxRows)
	outputCost := cpCandidatePageOwned + cpCandidateOwned*count
	if outputCost > budget.Bytes {
		count = (budget.Bytes - cpCandidatePageOwned) / cpCandidateOwned
		outputCost = cpCandidatePageOwned + cpCandidateOwned*count
	}
	if count < 1 && !done && it.tree.count != 0 {
		return currentPresenceCandidatePage{}, ErrResourceLimit
	}
	if outputCost > attempt.limits.maxOutputBytes-it.outputBytes {
		return currentPresenceCandidatePage{}, ErrResourceLimit
	}
	if err := attempt.charge(outputCost); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	out := make([]currentPresenceCandidate, 0, count)
	visited := 0
	for !done && len(out) < count && visited < attempt.limits.codec.maxRows {
		if err := ctx.Err(); err != nil {
			return currentPresenceCandidatePage{}, err
		}
		if len(frames) == 0 {
			break
		}
		frame := &frames[len(frames)-1]
		n := frame.page
		if n.level > 0 {
			if frame.index == len(n.children) {
				delete(attempt.cache, n.id)
				frames = frames[:len(frames)-1]
				continue
			}
			childIndex := frame.index
			frame.index++
			ch := n.children[childIndex]
			ok, e := attempt.childMayMatch(n, ch, it.predicate)
			if e != nil {
				return currentPresenceCandidatePage{}, it.c.failure(e)
			}
			if !ok {
				continue
			}
			if e := it.admitPage(&attempt, last); e != nil {
				return currentPresenceCandidatePage{}, e
			}
			child, e := attempt.child(n, childIndex)
			if e != nil {
				return currentPresenceCandidatePage{}, it.c.failure(e)
			}
			if len(frames) >= attempt.limits.codec.maxLevels {
				return currentPresenceCandidatePage{}, ErrCorrupt
			}
			frames = append(frames, cpIteratorFrame{page: child})
			continue
		}
		if frame.index == len(n.rows) {
			delete(attempt.cache, n.id)
			frames = frames[:len(frames)-1]
			continue
		}
		row := n.rows[frame.index]
		frame.index++
		visited++
		a, e := cpAtomFromRow(n, row, attempt.limits.codec)
		if e != nil {
			return currentPresenceCandidatePage{}, it.c.failure(e)
		}
		last = a
		if !cpAtomMatchesPrefix(a, it.predicate) {
			continue
		}
		scratch := 2048 + 64*(max(len(a.lower), len(a.upper), len(it.predicate.coordinate))+32)
		if scratch > attempt.limits.codec.maxScratchBytes {
			return currentPresenceCandidatePage{}, ErrResourceLimit
		}
		if err := attempt.charge(scratch + 256 + len(a.lower) + len(a.upper) + len(it.predicate.coordinate)); err != nil {
			return currentPresenceCandidatePage{}, err
		}
		ok, e := cpContainsAtom(a, it.predicate, attempt.limits.codec)
		if e != nil {
			return currentPresenceCandidatePage{}, it.c.failure(e)
		}
		if !ok {
			continue
		}
		candidate, ok, e := attempt.candidateWitness(a, it.predicate)
		if e != nil {
			return currentPresenceCandidatePage{}, it.c.failure(e)
		}
		if ok {
			out = append(out, candidate)
		}
	}
	// An exhausted final leaf is complete even when it exactly fills the budget.
	for len(frames) > 0 {
		f := frames[len(frames)-1]
		size := len(f.page.rows)
		if f.page.level > 0 {
			size = len(f.page.children)
		}
		if f.index < size {
			break
		}
		delete(attempt.cache, f.page.id)
		frames = frames[:len(frames)-1]
	}
	done = len(frames) == 0
	if err := attempt.charge(cpAtomOwned + len(last.lower)); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	last.lower = exactCopy(last.lower)
	last.upper = nil
	if it.retainedBytes(attempt.cache, &last) > attempt.limits.pages.MaxCursorBytes {
		return currentPresenceCandidatePage{}, ErrResourceLimit
	}
	if err := ctx.Err(); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	if err := attempt.budget(); err != nil {
		return currentPresenceCandidatePage{}, err
	}
	next := graphstate.Cursor(0)
	if !done {
		next, err = allocatePageCursor()
		if err != nil {
			return currentPresenceCandidatePage{}, err
		}
	}
	it.frames, it.last, it.started, it.done, it.cursor = frames, last, started, done, next
	it.op.cache = attempt.cache
	it.outputBytes += cpCandidatePageOwned + cpCandidateOwned*cap(out)
	attempt.counters.PageWork = attempt.work
	return currentPresenceCandidatePage{out, next, done, attempt.counters, visited}, nil
}

// Cursor retention has its own preflight, independently of cumulative source
// work. Source KV copies are transient; decoded path pages become retained.
func (it *currentPresenceIterator) admitPage(q *cpTreeOperation, last currentPresenceAtom) error {
	remaining := q.limits.pages.MaxCursorBytes - it.retainedBytes(q.cache, nil) - cpAtomOwned - len(last.lower) - cpCacheEntryOwned
	if remaining < cpPageOwned {
		return ErrResourceLimit
	}
	q.limits.codec.maxOwnedBytes = min(it.op.limits.codec.maxOwnedBytes, remaining)
	return nil
}
