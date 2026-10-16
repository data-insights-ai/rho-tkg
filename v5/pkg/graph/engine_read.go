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
	e.mu.Lock()
	err := e.check()
	e.mu.Unlock()
	if err != nil {
		return nil, err
	}
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
		return nil, fmt.Errorf("%w: open read view: %v", ErrInvalidRequest, err)
	}
	return &reader{e: e, ctx: ctx, view: v, rv: rv}, nil
}

func (r *reader) close() {
	_ = r.rv.Close()
	_ = r.view.Close()
}

var pageBudget = graphstate.ReadBudget{Rows: 256, Bytes: 1 << 20}

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
// revisions collects the revision IDs of the surviving pieces (Timeline).
func (r *reader) segments(id graphstate.EntityID, window temporal.Scope, revisions map[uint64]bool) ([]segment, error) {
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
				if revisions != nil {
					if rev := pc.Cell().Revision().ID(); rev != 0 {
						revisions[rev] = true
					}
				}
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
		if pre, err := cuts[0].Predecessor(l); err == nil {
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
func (r *reader) rows(ref Ref, sel Selector, proj Projection) ([]Entity, error) {
	id, wantNode := target(ref)
	rec, found, err := r.record(id)
	if err != nil || !found || (rec.Kind == graphstate.Node) != wantNode {
		return nil, err
	}
	segs, err := r.segments(id, sel.Valid, nil)
	if err != nil {
		return nil, err
	}
	var out []Entity
	for _, sg := range segs {
		pr, err := graphstate.Project(r.ctx, r.rv, id, sg.at, visibility(sel.View), graphstate.Limits{})
		if err != nil {
			return nil, err
		}
		if !pr.Exists || !pr.Active {
			continue
		}
		ent := Entity{Ref: ref, Labels: slices.Clone(pr.Labels), Valid: sg.scope, Known: sel.Known}
		if !wantNode {
			ent.RelType, ent.From, ent.To = rec.Type, types.NodeID(rec.Source), types.NodeID(rec.Target)
		}
		if len(proj.Properties) > 0 {
			ent.Props = map[string]Value{}
			for _, pv := range pr.Properties {
				if !slices.Contains(proj.Properties, pv.Name) {
					continue
				}
				v, err := scalarToValue(pv.Scalar)
				if err != nil {
					return nil, err
				}
				ent.Props[pv.Name] = v
			}
		}
		if n := len(out); n > 0 && sameContent(out[n-1], ent) {
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
			segs, err := r.segments(cl.Owner, scope, nil)
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
)

type pageState struct {
	kind pageKind
	sel  Selector
	proj Projection
	refs []Ref
	next int      // lookup: next ref
	rows []Entity // timeline: all rows; lookup: carry-over
	off  int
}

type engineSnap struct {
	e      *pebbleEngine
	index  uint64
	nonce  uint64
	cuts   Cut
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

func (s *engineSnap) scan(context.Context, ScanRequest) (Page, error) {
	return Page{}, fmt.Errorf("%w: Scan needs the label and equality index pages (WP-08)", ErrNotImplemented)
}

func (s *engineSnap) lookup(ctx context.Context, r LookupRequest) (Page, error) {
	if err := s.live(); err != nil {
		return Page{}, err
	}
	if r.Projection.Provenance || r.Projection.Revisions {
		return Page{}, fmt.Errorf("%w: provenance and revisions projections are not implemented", ErrNotImplemented)
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
	return s.fill(ctx, &pageState{kind: pageLookup, sel: r.Selector, proj: r.Projection, refs: refs}, r.Paging)
}

func (s *engineSnap) timeline(ctx context.Context, r TimelineRequest) (Page, error) {
	if err := s.live(); err != nil {
		return Page{}, err
	}
	if r.Projection.Provenance || r.Projection.Revisions {
		return Page{}, fmt.Errorf("%w: provenance and revisions projections are not implemented", ErrNotImplemented)
	}
	rows, err := s.timelineRows(ctx, r)
	if err != nil {
		return Page{}, err
	}
	return s.fill(ctx, &pageState{kind: pageTimeline, rows: rows}, r.Paging)
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
	st := s.pages[tok]
	delete(s.pages, tok) // a cursor is used once; the next page mints a new one
	s.mu.Unlock()
	if st == nil {
		return Page{}, fmt.Errorf("%w: cursor already used or unknown", ErrInvalidCursor)
	}
	return s.fill(ctx, st, p)
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
	var rd *reader
	if st.kind == pageLookup {
		var err error
		if rd, err = s.e.readerAt(ctx, s.index); err != nil {
			return Page{}, err
		}
		defer rd.close()
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
		if st.kind != pageLookup || st.next >= len(st.refs) {
			return page, nil
		}
		rows, err := rd.rows(st.refs[st.next], st.sel, st.proj)
		if err != nil {
			return Page{}, err
		}
		st.next++
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

// timelineRows: one row set per commit that touched the entity, oldest first.
// The touched commits are found from the revision IDs of the surviving pieces
// at the cut, then at the index before each of them (superseded pieces are gone
// from the component state, but each earlier view still holds them).
func (s *engineSnap) timelineRows(ctx context.Context, r TimelineRequest) ([]Entity, error) {
	id, _ := target(r.Ref)
	touched := map[uint64]bool{}
	todo := []uint64{s.index}
	seenView := map[uint64]bool{}
	window, err := temporal.All(r.Selector.Valid.Axis())
	if err != nil {
		return nil, err
	}
	for len(todo) > 0 {
		at := todo[len(todo)-1]
		todo = todo[:len(todo)-1]
		if at == 0 || seenView[at] {
			continue
		}
		seenView[at] = true
		rd, err := s.e.readerAt(ctx, at)
		if err != nil {
			return nil, err
		}
		revs := map[uint64]bool{}
		_, found, err := rd.record(id)
		if err == nil && found {
			_, err = rd.segments(id, window, revs)
		}
		rd.close()
		if err != nil {
			return nil, err
		}
		for rev := range revs {
			if rev <= at && !touched[rev] {
				touched[rev] = true
			}
			if rev > 1 {
				todo = append(todo, rev-1)
			}
		}
	}
	order := make([]uint64, 0, len(touched))
	for rev := range touched {
		order = append(order, rev)
	}
	slices.Sort(order)
	var out []Entity
	for _, rev := range order {
		rd, err := s.e.readerAt(ctx, rev)
		if err != nil {
			return nil, err
		}
		rows, err := rd.rows(r.Ref, r.Selector, r.Projection)
		rd.close()
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}
