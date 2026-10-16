package graph

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// reader is one borrowed application view at a fixed index plus the checked
// partition read view over it. It lives for one door call.
type reader struct {
	e    *pebbleEngine
	ctx  context.Context
	view *raftlog.ApplicationView
	rv   *graphstore.ReadView
}

func (e *pebbleEngine) readerAt(ctx context.Context, index uint64) (*reader, error) {
	if e.closed.Load() {
		return nil, ErrClosed
	}
	e.readers.Add(1)
	v, err := e.store.ApplicationView(index)
	if err != nil {
		if errors.Is(err, raftlog.ErrApplicationExpired) {
			return nil, ErrSnapshotExpired
		}
		return nil, err
	}
	rv, _, err := graphstore.OpenPartitionGraphReadView(ctx, v, e.decl, e.binding, e.conf, graphstore.Limits{}, graphstore.GraphLimits{}, readBudget())
	if err != nil {
		_ = v.Close()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: open read view: %v", ErrInvalidRequest, err)
	}
	return &reader{e: e, ctx: ctx, view: v, rv: rv}, nil
}

func (r *reader) close() {
	_ = r.rv.Close()
	_ = r.view.Close()
}

// pageBudget bounds the source rows of one component page: an entity with a few
// dozen revisions of one property needs more than the 256 it once had.
var pageBudget = graphstate.ReadBudget{Rows: 4096, Bytes: 8 << 20}

// readsPerView bounds the entities one fill reads through one read view.
const readsPerView = 128

func (r *reader) pieces(key graphstate.ComponentKey, window temporal.Scope) ([]graphstate.ComponentPage, error) {
	var out []graphstate.ComponentPage
	var cur graphstate.Cursor
	for guard := 0; guard < 1<<16; guard++ {
		pg, err := r.rv.ComponentPage(r.ctx, graphstate.ComponentQuery{Key: key, Window: window}, cur, pageBudget)
		if err != nil {
			return nil, err
		}
		out = append(out, pg)
		if pg.Next == 0 {
			return out, nil
		}
		cur = pg.Next
	}
	return nil, ErrInvalidRequest
}

func (r *reader) keys(id graphstate.EntityID) ([]graphstate.ComponentKey, error) {
	out := []graphstate.ComponentKey{{Owner: id, Kind: graphstate.Presence}}
	var cur graphstate.Cursor
	for guard := 0; guard < 1<<16; guard++ {
		pg, err := r.rv.ComponentKeys(r.ctx, graphstate.KeyPredicate{Owner: id, Life: lifeID}, cur, pageBudget)
		if err != nil {
			return nil, err
		}
		out = append(out, pg.Keys...)
		if pg.Next == 0 {
			return out, nil
		}
		cur = pg.Next
	}
	return nil, ErrInvalidRequest
}

// exists reports the entity record; ids outside the published range or absent
// are not found.
func (r *reader) record(id graphstate.EntityID) (graphstate.EntityRecord, bool, error) {
	er, err := r.rv.Entity(r.ctx, id)
	if err != nil {
		if errors.Is(err, graphstore.ErrRoutingUnknown) || errors.Is(err, graphstore.ErrScopeUnavailable) {
			return graphstate.EntityRecord{}, false, nil
		}
		return graphstate.EntityRecord{}, false, err
	}
	return er.Record, er.Found, nil
}

type segment struct {
	at    temporal.Position
	scope temporal.Scope
}

// segments splits window at every piece boundary of the entity's components.
// Segments are lower-inclusive, upper-exclusive; a Point window is one segment.
func (r *reader) segments(id graphstate.EntityID, window temporal.Scope) ([]segment, error) {
	l := r.e.cfg.Limits
	ax := window.Axis()
	if window.Kind() == temporal.ScopeEmpty {
		return nil, nil
	}
	if window.Kind() == temporal.ScopePoint {
		lo, _, _ := window.Bounds()
		p, _ := lo.Position()
		return []segment{{p, window}}, nil
	}
	keys, err := r.keys(id)
	if err != nil {
		return nil, err
	}
	var cuts []temporal.Position
	for _, k := range keys {
		pages, err := r.pieces(k, window)
		if err != nil {
			return nil, err
		}
		for _, pg := range pages {
			for _, pc := range pg.Data.Pieces() {
				for _, part := range pc.Scope().Parts() {
					lo, hi, ok := part.Bounds()
					if !ok {
						continue
					}
					for _, b := range []temporal.Bound{lo, hi} {
						if p, fin := b.Position(); fin {
							cuts = append(cuts, p)
						}
					}
				}
			}
		}
	}
	wlo, whi, _ := window.Bounds()
	if p, fin := wlo.Position(); fin {
		cuts = append(cuts, p)
	}
	if p, fin := whi.Position(); fin {
		cuts = append(cuts, p)
	}
	var cmpErr error
	slices.SortFunc(cuts, func(a, b temporal.Position) int {
		o, err := temporal.ComparePositions(a, b, l)
		if err != nil {
			cmpErr = err
		}
		return int(o)
	})
	if cmpErr != nil {
		return nil, cmpErr
	}
	cuts = slices.CompactFunc(cuts, func(a, b temporal.Position) bool {
		o, _ := temporal.ComparePositions(a, b, l)
		return o == temporal.Equal
	})
	inside := func(p temporal.Position) bool {
		if lp, fin := wlo.Position(); fin {
			if o, _ := temporal.ComparePositions(p, lp, l); o == temporal.Less {
				return false
			}
		}
		if hp, fin := whi.Position(); fin {
			if o, _ := temporal.ComparePositions(p, hp, l); o == temporal.Greater {
				return false
			}
		}
		return true
	}
	cuts = slices.DeleteFunc(cuts, func(p temporal.Position) bool { return !inside(p) })
	zero, err := types.InstantPosition(ax, 0, l)
	if err != nil {
		return nil, err
	}
	if len(cuts) == 0 {
		return []segment{{zero, window}}, nil
	}
	var out []segment
	mk := func(lo temporal.Bound, hi temporal.Bound) (temporal.Scope, error) {
		return temporal.Span(ax, lo, hi, l)
	}
	if _, fin := wlo.Position(); !fin {
		if pre, err := before(cuts[0], l); err == nil {
			s, err := mk(temporal.NegativeInfinity(), mustBound(cuts[0], false))
			if err != nil {
				return nil, err
			}
			out = append(out, segment{pre, s})
		}
	}
	for i, c := range cuts {
		lo := mustBound(c, true)
		var hi temporal.Bound
		switch {
		case i+1 < len(cuts):
			hi = mustBound(cuts[i+1], false)
		default:
			if _, fin := whi.Position(); fin {
				continue // the window ends at the last cut
			}
			hi = temporal.PositiveInfinity()
		}
		s, err := mk(lo, hi)
		if err != nil {
			return nil, err
		}
		out = append(out, segment{c, s})
	}
	return out, nil
}

func mustBound(p temporal.Position, inclusive bool) temporal.Bound {
	b, _ := temporal.FiniteBound(p, inclusive)
	return b
}

func visibility(v View) graphstate.Visibility {
	if v == ViewEffective {
		return graphstate.Effective
	}
	return graphstate.Declared
}

// rows projects entity id under sel at the reader's index: one Entity per
// segment where it is active, adjacent equal rows merged.
func (r *reader) rows(ref Ref, sel Selector, proj Projection, m *matcher) ([]Entity, error) {
	id, wantNode := target(ref)
	rec, found, err := r.record(id)
	if err != nil || !found || (rec.Kind == graphstate.Node) != wantNode {
		return nil, err
	}
	if m != nil && !m.entity(rec) {
		return nil, nil
	}
	segs, err := r.segments(id, sel.Valid)
	if err != nil {
		return nil, err
	}
	var out []Entity
	lastSeg := -2
	for si, sg := range segs {
		pr, err := graphstate.Project(r.ctx, r.rv, id, sg.at, visibility(sel.View), graphstate.Limits{})
		if err != nil {
			return nil, err
		}
		if !pr.Exists || !pr.Active || m != nil && !m.state(pr) {
			continue
		}
		adjacent := lastSeg == si-1
		lastSeg = si
		ent := Entity{Ref: ref, Labels: slices.Clone(pr.Labels), Valid: sg.scope, Known: sel.Known}
		if !wantNode {
			ent.RelType, ent.From, ent.To = rec.Type, types.NodeID(rec.Source), types.NodeID(rec.Target)
		}
		if len(proj.Properties) > 0 || proj.AllProperties {
			ent.Props = map[string]Value{}
			for _, pv := range pr.Properties {
				if !proj.AllProperties && !slices.Contains(proj.Properties, pv.Name) {
					continue
				}
				v, err := scalarToValue(pv.Scalar)
				if err != nil {
					return nil, err
				}
				ent.Props[pv.Name] = v
			}
		}
		if n := len(out); n > 0 && adjacent && sameContent(out[n-1], ent) {
			if merged, ok := mergeScopes(out[n-1].Valid, ent.Valid); ok {
				out[n-1].Valid = merged
				continue
			}
		}
		out = append(out, ent)
	}
	return out, nil
}

func sameContent(a, b Entity) bool {
	return slices.Equal(a.Labels, b.Labels) && reflect.DeepEqual(a.Props, b.Props)
}

func mergeScopes(a, b temporal.Scope) (temporal.Scope, bool) {
	alo, _, ok1 := a.Bounds()
	_, bhi, ok2 := b.Bounds()
	if !ok1 || !ok2 {
		return a, false
	}
	s, err := temporal.Span(a.Axis(), alo, bhi, temporal.Limits{})
	return s, err == nil
}

func entityBytes(e Entity) int {
	n := 64 + len(e.RelType)
	for _, l := range e.Labels {
		n += 16 + len(l)
	}
	for k, v := range e.Props {
		s, _ := v.AsString()
		n += 24 + len(k) + len(s)
	}
	return n
}

// findClaim looks for the live node holding the unique claim (def, value) with
// label at the scope's sample position.
func (e *pebbleEngine) findClaim(ctx context.Context, index uint64, def graphstate.PropertyDefinition, value graphstate.Scalar, label string, scope temporal.Scope) (types.NodeID, bool, error) {
	r, err := e.readerAt(ctx, index)
	if err != nil {
		return 0, false, err
	}
	defer r.close()
	var cur graphstate.Cursor
	var holder types.NodeID
	for guard := 0; guard < 1<<16; guard++ {
		pg, err := r.rv.UniqueCandidates(ctx, graphstate.UniquePredicate{Definition: def, Value: value, Window: scope}, cur, pageBudget)
		if err != nil {
			return 0, false, mapStageErr(err)
		}
		for _, cl := range pg.Claims {
			segs, err := r.segments(cl.Owner, scope)
			if err != nil {
				return 0, false, err
			}
			for _, sg := range segs {
				pr, err := graphstate.Project(ctx, r.rv, cl.Owner, sg.at, graphstate.Effective, graphstate.Limits{})
				if err != nil || !pr.Exists || !pr.Active {
					continue
				}
				for _, pv := range pr.Properties {
					if pv.Name != def.Name {
						continue
					}
					if eq, _ := pv.Scalar.Equal(value, graphstate.Limits{}); !eq {
						continue
					}
					if slices.Contains(pr.Labels, label) {
						return types.NodeID(cl.Owner), true, nil
					}
					holder = types.NodeID(cl.Owner)
				}
			}
		}
		if pg.Next == 0 {
			break
		}
		cur = pg.Next
	}
	if holder != 0 {
		return 0, false, fmt.Errorf("%w: the claim is held by a node without the label", ErrUniqueViolation)
	}
	return 0, false, nil
}

// ---- snapshot engine ----

type pageKind uint8

const (
	pageLookup pageKind = iota + 1
	pageTimeline
	pageScan
)

type pageState struct {
	kind   pageKind
	sel    Selector
	proj   Projection
	refs   []Ref
	next   int                   // lookup: next ref; scan: next candidate
	cands  []graphstate.EntityID // scan: index candidates, ascending
	nextID uint64                // scan without an index: next ID of the range
	lastID uint64                // scan without an index: last minted ID at the cut
	match  *matcher
	rows   []Entity // timeline: the current chunk; lookup, scan: carry-over
	off    int
	kb     *knownBound // nil: no Known filtering
	tlID   graphstate.EntityID
	tlRef  Ref
	tlAt   uint64 // timeline: last revision read
	tlDone bool
}

type engineSnap struct {
	e      *pebbleEngine
	index  uint64
	nonce  uint64
	cuts   Cut
	vInst  int64 // knowledge instant of the view's newest commit (-1: none)
	mu     sync.Mutex
	closed bool
	seq    uint64
	pages  map[uint64]*pageState
}

var _ snapshotEngine = (*engineSnap)(nil)

const maxOpenCursors = 1024

func (s *engineSnap) cut() Cut { return s.cuts }

func (s *engineSnap) close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	s.closed = true
	s.pages = nil
	return nil
}

// scan pages the entities matching r.Selector in ID order. Candidates come
// from the equality index (first Equals) or the label index (first label);
// without either it walks the minted ID range at the cut. The index lists
// owners at the latest knowledge in any valid time, so every candidate is
// rechecked per segment (labels, type and values at that position). A Known
// bound below the view skips the index (its postings hold the latest knowledge).
func (s *engineSnap) scan(ctx context.Context, r ScanRequest) (Page, error) {
	if err := s.live(); err != nil {
		return Page{}, err
	}
	kb, err := s.bound(r.Selector)
	if err != nil {
		return Page{}, err
	}
	if r.Projection.Provenance || r.Projection.Revisions {
		return Page{}, fmt.Errorf("%w: provenance and revisions projections are not implemented for Scan", ErrNotImplemented)
	}
	m, err := newMatcher(r.Selector)
	if err != nil {
		return Page{}, err
	}
	st := &pageState{kind: pageScan, sel: r.Selector, proj: r.Projection, match: m, kb: kb}
	if m.none {
		return Page{}, nil
	}
	rd, err := s.e.readerAt(ctx, s.index)
	if err != nil {
		return Page{}, err
	}
	err = rd.candidates(r.Selector, m, st)
	rd.close()
	if err != nil {
		return Page{}, err
	}
	return s.fill(ctx, st, r.Paging)
}

// candidates fills st from one index, or with the minted ID range at the cut.
func (r *reader) candidates(sel Selector, m *matcher, st *pageState) error {
	page := func(read func(graphstate.Cursor) (graphstate.EntityPage, error)) ([]graphstate.EntityID, error) {
		var out []graphstate.EntityID
		var cur graphstate.Cursor
		for guard := 0; guard < 1<<16; guard++ {
			pg, err := read(cur)
			if err != nil {
				if errors.Is(err, graphstore.ErrResourceLimit) {
					return nil, fmt.Errorf("%w: the scan's index candidates exceed one read budget (narrow the selector)", ErrInvalidRequest)
				}
				return nil, err
			}
			out = append(out, pg.Entities...)
			if pg.Complete || pg.Next == 0 {
				return out, nil
			}
			cur = pg.Next
		}
		return nil, ErrInvalidRequest
	}
	switch {
	case st.kb != nil:
		// Postings hold the latest knowledge: a bound below the view walks every entity.
		return r.mintedRange(st)
	case len(m.eq) > 0:
		owners := []graphstate.EntityKind{graphstate.Node, graphstate.Relationship}
		if len(sel.Labels) > 0 {
			owners = owners[:1]
		} else if sel.RelType != "" {
			owners = owners[1:]
		}
		for _, o := range owners {
			ids, err := page(func(c graphstate.Cursor) (graphstate.EntityPage, error) {
				return r.rv.EqualityMembers(r.ctx, graphstate.EqualityPredicate{Owner: o, Name: m.eq[0].name, Value: m.eq[0].value, Visibility: visibility(sel.View)}, c, pageBudget)
			})
			if err != nil {
				return err
			}
			st.cands = append(st.cands, ids...)
		}
		slices.Sort(st.cands)
		st.cands = slices.Compact(st.cands)
	case len(sel.Labels) > 0:
		ids, err := page(func(c graphstate.Cursor) (graphstate.EntityPage, error) {
			return r.rv.LabelMembers(r.ctx, sel.Labels[0], c, pageBudget)
		})
		if err != nil {
			return err
		}
		st.cands = ids
	default:
		return r.mintedRange(st)
	}
	return nil
}

// mintedRange sets st to walk the IDs minted at the cut.
func (r *reader) mintedRange(st *pageState) error {
	row, found, err := r.view.Get(r.ctx, allocName, 4096)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}
	a, err := decodeAllocState(row.Value)
	if err != nil {
		return err
	}
	lo, hi, err := a.minted()
	if err != nil {
		return err
	}
	st.nextID, st.lastID = lo, hi
	return nil
}

// candidate returns the next candidate ref, or ok=false when none is left.
// Index candidates count toward the false-candidate statistics.
func (st *pageState) candidate(rd *reader) (Ref, bool, error) {
	for {
		var id graphstate.EntityID
		switch {
		case st.next < len(st.cands):
			id = st.cands[st.next]
			st.next++
			rd.e.scanCandidates.Add(1)
		case st.nextID != 0 && st.nextID <= st.lastID:
			id = graphstate.EntityID(st.nextID)
			st.nextID++
		default:
			return Ref{}, false, nil
		}
		rec, found, err := rd.record(id)
		if err != nil {
			return Ref{}, false, err
		}
		if !found {
			continue
		}
		if rec.Kind == graphstate.Node {
			ref, err := NodeRef(types.NodeID(id))
			return ref, err == nil, err
		}
		ref, err := RelRef(types.RelID(id))
		return ref, err == nil, err
	}
}

// matcher is a Selector's row predicate: kind, type, every label and every
// equality, rechecked at each segment's position.
type matcher struct {
	labels  []string
	relType string
	eq      []eqProp
	none    bool // a predicate no value can satisfy (Null)
}

type eqProp struct {
	name  string
	value graphstate.Scalar
}

func newMatcher(sel Selector) (*matcher, error) {
	m := &matcher{labels: sel.Labels, relType: sel.RelType}
	for _, e := range sel.Equals {
		sc, err := valueToScalar(e.Value)
		if err != nil {
			return nil, err
		}
		if sc.Kind() == graphstate.ScalarNull {
			m.none = true
			continue
		}
		m.eq = append(m.eq, eqProp{e.Key, sc})
	}
	return m, nil
}

func (m *matcher) entity(rec graphstate.EntityRecord) bool {
	switch {
	case len(m.labels) > 0:
		return rec.Kind == graphstate.Node
	case m.relType != "":
		return rec.Kind == graphstate.Relationship && rec.Type == m.relType
	}
	return true
}

func (m *matcher) state(pr graphstate.Projection) bool {
	for _, l := range m.labels {
		if !slices.Contains(pr.Labels, l) {
			return false
		}
	}
	for _, e := range m.eq {
		hit := false
		for _, pv := range pr.Properties {
			if pv.Name != e.name {
				continue
			}
			// Scalar.Equal is the rule of Value.Equal (EqualityKey finds the candidates).
			eq, err := e.value.Equal(pv.Scalar, graphstate.Limits{})
			hit = hit || err == nil && eq
		}
		if !hit {
			return false
		}
	}
	return true
}

// knownBound is the upper bound of a Known scope: the state as known up to hi.
// A revision is known when its recorded evidence (the latest instant of its
// Recorded values), else its commit knowledge instant, is within the bound. The
// lower bound is not applied: knowledge accumulates, so "known at t" is
// "known by t".
type knownBound struct {
	hi        temporal.Position
	inclusive bool
	limits    temporal.Limits
}

func (b *knownBound) allows(k temporal.Position) bool {
	o, err := temporal.ComparePositions(k, b.hi, b.limits)
	return err == nil && (o == temporal.Less || o == temporal.Equal && b.inclusive)
}

// bound returns the filter of sel.Known, or nil when it admits everything the
// snapshot's view holds (All, an unbounded top, or a bound at or above the view's
// newest commit instant).
func (s *engineSnap) bound(sel Selector) (*knownBound, error) {
	k := sel.Known
	if k.Kind() == temporal.ScopeAll {
		return nil, nil
	}
	var hi temporal.Bound
	switch k.Kind() {
	case temporal.ScopePoint, temporal.ScopeSpan, temporal.ScopeRegion:
		_, h, ok := k.Bounds()
		if !ok {
			return nil, fmt.Errorf("%w: Known scope has no bounds", ErrInvalidScope)
		}
		hi = h
	default:
		return nil, fmt.Errorf("%w: Known must be All, a point or a span", ErrInvalidScope)
	}
	p, fin := hi.Position()
	if !fin {
		return nil, nil
	}
	kb := &knownBound{hi: p, inclusive: hi.Inclusive() || k.Kind() == temporal.ScopePoint, limits: s.e.cfg.Limits}
	if s.vInst >= 0 {
		if vp, err := s.e.instantPosition(s.vInst); err == nil && kb.allows(vp) {
			return nil, nil
		}
	}
	return kb, nil
}

// known is when the revision became known, from its revision row.
func markKnown(m graphstore.RevisionMark) (temporal.Position, error) {
	if m.Known.Profile() == 0 {
		return temporal.Position{}, fmt.Errorf("%w: revision %d has no knowledge instant", ErrCommitNotDurable, m.Revision)
	}
	return m.Known, nil
}

const revisionChunk = 64

// knownIndex is the view index that shows id as known under kb: the newest
// revision such that it and every earlier revision of id are known. (A later
// revision recorded earlier than a predecessor stays hidden rather than
// exposing the predecessor.) Zero: nothing of id is known. rd reads the
// snapshot's view.
func (rd *reader) knownIndex(id graphstate.EntityID, kb *knownBound) (uint64, error) {
	var eff, after uint64
	for {
		revs, complete, err := rd.rv.Revisions(rd.ctx, id, after, revisionChunk)
		if err != nil {
			return 0, err
		}
		for _, m := range revs {
			k, err := markKnown(m)
			if err != nil {
				return 0, err
			}
			if !kb.allows(k) {
				return eff, nil
			}
			eff = m.Revision
		}
		if complete || len(revs) == 0 {
			return eff, nil
		}
		after = revs[len(revs)-1].Revision
	}
}

// readerCache shares one reader per index across the entities of one fill,
// rotating each after readsPerView uses (a view's source budget is bounded).
type readerCache struct {
	e    *pebbleEngine
	ctx  context.Context
	byAt map[uint64]*cachedReader
}

type cachedReader struct {
	rd   *reader
	uses int
}

func (c *readerCache) get(index uint64) (*reader, error) {
	if cr := c.byAt[index]; cr != nil {
		if cr.uses < readsPerView {
			cr.uses++
			return cr.rd, nil
		}
		cr.rd.close()
		delete(c.byAt, index)
	}
	rd, err := c.e.readerAt(c.ctx, index)
	if err != nil {
		return nil, err
	}
	if c.byAt == nil {
		c.byAt = map[uint64]*cachedReader{}
	}
	c.byAt[index] = &cachedReader{rd: rd, uses: 1}
	return rd, nil
}

func (c *readerCache) closeAll() {
	for i, cr := range c.byAt {
		cr.rd.close()
		delete(c.byAt, i)
	}
}

// rowsFor is rd.rows, at the view that shows the entity as known under kb.
func (s *engineSnap) rowsFor(ctx context.Context, rd *reader, ref Ref, st *pageState, cache *readerCache) ([]Entity, error) {
	m := st.match
	if st.kb == nil {
		return rd.rows(ref, st.sel, st.proj, m)
	}
	id, _ := target(ref)
	eff, err := rd.knownIndex(id, st.kb)
	if err != nil || eff == 0 {
		return nil, err
	}
	at, err := cache.get(eff)
	if err != nil {
		return nil, err
	}
	return at.rows(ref, st.sel, st.proj, m)
}

func (s *engineSnap) lookup(ctx context.Context, r LookupRequest) (Page, error) {
	if err := s.live(); err != nil {
		return Page{}, err
	}
	kb, err := s.bound(r.Selector)
	if err != nil {
		return Page{}, err
	}
	if r.Projection.Provenance || r.Projection.Revisions {
		return Page{}, fmt.Errorf("%w: provenance and revisions projections are not implemented for Lookup", ErrNotImplemented)
	}
	var refs []Ref
	for _, n := range r.Nodes {
		ref, _ := NodeRef(n)
		refs = append(refs, ref)
	}
	for _, id := range r.Rels {
		ref, _ := RelRef(id)
		refs = append(refs, ref)
	}
	return s.fill(ctx, &pageState{kind: pageLookup, sel: r.Selector, proj: r.Projection, refs: refs, kb: kb}, r.Paging)
}

func (s *engineSnap) timeline(ctx context.Context, r TimelineRequest) (Page, error) {
	if err := s.live(); err != nil {
		return Page{}, err
	}
	kb, err := s.bound(r.Selector)
	if err != nil {
		return Page{}, err
	}
	if r.Projection.Provenance {
		return Page{}, fmt.Errorf("%w: the provenance projection is not implemented", ErrNotImplemented)
	}
	id, _ := target(r.Ref)
	return s.fill(ctx, &pageState{kind: pageTimeline, sel: r.Selector, proj: r.Projection, kb: kb, tlID: id, tlRef: r.Ref}, r.Paging)
}

func (s *engineSnap) resume(ctx context.Context, c Cursor, p PageLimits) (Page, error) {
	if err := s.live(); err != nil {
		return Page{}, err
	}
	nonce, tok, ok := parseCursor(c)
	if !ok || nonce != s.nonce {
		return Page{}, fmt.Errorf("%w: not a cursor of this snapshot", ErrInvalidCursor)
	}
	s.mu.Lock()
	orig := s.pages[tok]
	s.mu.Unlock()
	if orig == nil {
		return Page{}, fmt.Errorf("%w: cursor already used or unknown", ErrInvalidCursor)
	}
	// The page is built on a copy; the cursor is spent only once the page exists.
	st := *orig
	page, err := s.fill(ctx, &st, p)
	if err != nil {
		return Page{}, err
	}
	s.mu.Lock()
	delete(s.pages, tok)
	s.mu.Unlock()
	return page, nil
}

func (s *engineSnap) live() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return ErrClosed
	}
	return nil
}

func (s *engineSnap) fill(ctx context.Context, st *pageState, p PageLimits) (Page, error) {
	cache := &readerCache{e: s.e, ctx: ctx}
	defer cache.closeAll()
	var rd *reader
	defer func() {
		if rd != nil {
			rd.close()
		}
	}()
	reads := 0
	// open (re)opens the reader: one view's source budget covers a bounded run
	// of entities, so a long scan of false candidates rotates views.
	open := func() error {
		if rd != nil && reads < readsPerView {
			return nil
		}
		if rd != nil {
			rd.close()
			rd = nil
		}
		var err error
		rd, err = s.e.readerAt(ctx, s.index)
		reads = 0
		return err
	}
	var page Page
	bytesUsed := 0
	take := func(e Entity) (bool, error) {
		n := entityBytes(e)
		if len(page.Rows) >= p.Rows || (len(page.Rows) > 0 && bytesUsed+n > p.MaxBytes) {
			return false, nil
		}
		if n > p.MaxBytes {
			return false, fmt.Errorf("%w: one row exceeds the page byte limit", ErrInvalidRequest)
		}
		page.Rows = append(page.Rows, e)
		bytesUsed += n
		return true, nil
	}
	for {
		for st.off < len(st.rows) {
			ok, err := take(st.rows[st.off])
			if err != nil {
				return Page{}, err
			}
			if !ok {
				return s.more(page, st)
			}
			st.off++
		}
		st.rows, st.off = nil, 0
		if st.kind == pageTimeline {
			if st.tlDone {
				return page, nil
			}
			if err := s.timelineChunk(ctx, st); err != nil {
				return Page{}, err
			}
			continue
		}
		if st.kind == pageLookup && st.next >= len(st.refs) {
			return page, nil
		}
		if err := open(); err != nil {
			return Page{}, err
		}
		reads++
		var ref Ref
		if st.kind == pageLookup {
			ref = st.refs[st.next]
			st.next++
		} else {
			fromIndex := st.next < len(st.cands)
			var ok bool
			var err error
			if ref, ok, err = st.candidate(rd); err != nil {
				return Page{}, err
			} else if !ok {
				return page, nil
			}
			rows, err := s.rowsFor(ctx, rd, ref, st, cache)
			if err != nil {
				return Page{}, err
			}
			if fromIndex && len(rows) > 0 {
				s.e.scanHits.Add(1)
			}
			st.rows = rows
			if err := ctx.Err(); err != nil {
				return Page{}, err
			}
			continue
		}
		rows, err := s.rowsFor(ctx, rd, ref, st, cache)
		if err != nil {
			return Page{}, err
		}
		st.rows = rows
		if err := ctx.Err(); err != nil {
			return Page{}, err
		}
	}
}

func (s *engineSnap) more(page Page, st *pageState) (Page, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.pages) >= maxOpenCursors {
		return Page{}, fmt.Errorf("%w: too many open cursors on one snapshot", ErrInvalidRequest)
	}
	s.seq++
	s.pages[s.seq] = st
	b := []byte{'R', 'V', 'C', 1}
	b = binary.BigEndian.AppendUint64(b, s.nonce)
	b = binary.BigEndian.AppendUint64(b, s.seq)
	page.Next = Cursor{b: b}
	return page, nil
}

func parseCursor(c Cursor) (nonce, tok uint64, ok bool) {
	if len(c.b) != 4+16 || !bytes.Equal(c.b[:4], []byte{'R', 'V', 'C', 1}) {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(c.b[4:]), binary.BigEndian.Uint64(c.b[12:]), true
}

// timelineChunk reads the next revisions of the timeline's entity from the
// per-owner revision rows (one chunk, not the whole history) and turns each into
// its rows at the view of that commit, oldest first.
func (s *engineSnap) timelineChunk(ctx context.Context, st *pageState) error {
	rd, err := s.e.readerAt(ctx, s.index)
	if err != nil {
		return err
	}
	revs, complete, err := rd.rv.Revisions(ctx, st.tlID, st.tlAt, revisionChunk)
	if err == nil && st.kb != nil {
		for i, m := range revs {
			var k temporal.Position
			if k, err = markKnown(m); err != nil {
				break
			}
			if !st.kb.allows(k) {
				revs, complete = revs[:i], true // the known prefix ends here
				break
			}
		}
	}
	rd.close()
	if err != nil {
		return err
	}
	for _, m := range revs {
		rev := m.Revision
		at, err := s.e.readerAt(ctx, rev)
		if err != nil {
			return err
		}
		rows, err := at.rows(st.tlRef, st.sel, st.proj, nil)
		if err == nil {
			var known temporal.Scope
			if known, err = at.knownOf(st.tlID, rev, st.sel.Known); err == nil {
				for i := range rows {
					rows[i].Known = known
					if st.proj.Revisions {
						rows[i].Revision = rev
					}
				}
			}
		}
		at.close()
		if err != nil {
			return err
		}
		st.rows = append(st.rows, rows...)
		st.tlAt = rev
	}
	st.tlDone = complete || len(revs) == 0
	return nil
}

// before returns a position strictly earlier than p: the immediate predecessor
// where the axis has one, else p minus one unit on the dense rational axis.
func before(p temporal.Position, l temporal.Limits) (temporal.Position, error) {
	if q, ok := p.Rational(); ok {
		n, nok := q.Numerator().Int64()
		d, dok := q.Denominator().Int64()
		if !nok || !dok {
			return temporal.Position{}, ErrInvalidScope
		}
		r, err := temporal.Fraction(temporal.Int64(n-d), temporal.Int64(d), l)
		if err != nil {
			return temporal.Position{}, err
		}
		return temporal.RationalPosition(p.Axis(), r)
	}
	return p.Predecessor(l)
}

// knownOf is the knowledge support of one revision: All when the commit carried
// no Recorded evidence for the entity, the instant (or the hull of the instants)
// of its evidence otherwise.
func (r *reader) knownOf(id graphstate.EntityID, rev uint64, all temporal.Scope) (temporal.Scope, error) {
	ev, err := r.rv.Evidence(r.ctx, id, rev)
	if err != nil {
		return all, err
	}
	var lo, hi temporal.Position
	l := r.e.cfg.Limits
	for i, e := range ev {
		if i == 0 {
			lo, hi = e.Recorded, e.Recorded
			continue
		}
		if o, err := temporal.ComparePositions(e.Recorded, lo, l); err == nil && o == temporal.Less {
			lo = e.Recorded
		}
		if o, err := temporal.ComparePositions(e.Recorded, hi, l); err == nil && o == temporal.Greater {
			hi = e.Recorded
		}
	}
	if len(ev) == 0 {
		return all, nil
	}
	if o, _ := temporal.ComparePositions(lo, hi, l); o == temporal.Equal {
		return temporal.Point(lo)
	}
	return temporal.Span(lo.Axis(), mustBound(lo, true), mustBound(hi, true), l)
}

// scanCounts counts index candidates and the candidates that produced a row;
// tests read it to report the false-candidate ratio.
type scanCounts struct{ candidates, hits uint64 }

func (a scanCounts) minus(b scanCounts) scanCounts {
	return scanCounts{a.candidates - b.candidates, a.hits - b.hits}
}

func (e *pebbleEngine) scanStats() scanCounts {
	return scanCounts{e.scanCandidates.Load(), e.scanHits.Load()}
}
