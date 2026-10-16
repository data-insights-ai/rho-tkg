package graph

import (
	"context"
	"fmt"
	"slices"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	v5t "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

var paging = v5.PageLimits{Rows: v5.MaxPageRows, MaxBytes: 8 << 20}

// pinKnown resolves a v4 transaction pin to a Known scope: the state as known up
// to the pin (pin 0 is the present: everything). tooNew reports a pin above the
// commit clock for the doors that refuse it (ErrTxPinTooNew).
func (g *Graph) pinKnown(pin int64) (known temporal.Scope, tooNew bool) {
	if pin <= 0 {
		return g.ax.known(), false
	}
	return g.ax.knownTo(pin), pin > max(nowMs(), g.committedTx())
}

func (g *Graph) selector(known temporal.Scope, view v5.View, valid span, eq []v5.PropertyEq, labels []string, relType string) v5.Selector {
	return v5.Selector{View: view, Valid: g.ax.scope(valid), Known: known, Labels: labels, RelType: relType, Equals: eq}
}

func drain(ctx context.Context, s *v5.Snapshot, p v5.Page, err error) ([]v5.Entity, error) {
	var rows []v5.Entity
	for {
		if err != nil {
			return nil, mapV5(err)
		}
		rows = append(rows, p.Rows...)
		if p.Next.IsZero() {
			return rows, nil
		}
		p, err = s.Resume(ctx, p.Next, paging)
	}
}

func idErr(id int64) error {
	switch {
	case id == 0:
		return ErrZeroID
	case id < 0:
		return fmt.Errorf("%w: %d", ErrInvalidID, id)
	}
	return nil
}

func (g *Graph) ref(id int64, rel bool) (v5.Ref, error) {
	if err := idErr(id); err != nil {
		return v5.Ref{}, err
	}
	if rel {
		return v5.RelRef(v5t.RelID(id))
	}
	return v5.NodeRef(v5t.NodeID(id))
}

// revRows is one commit's picture of an entity: every valid-time segment as of
// that commit, and the commit's latest Recorded instant for the entity.
type revRows struct {
	at   int64
	rows []row
}

// hist is the Timeline of one entity read at a Known bound, with the stamps the
// v4 rows carry: tf per version (the instant of the first commit that holds it),
// the end instant (the first commit that carries the ~dr marker) and last, the
// newest instant written to the entity.
type hist struct {
	revs  []revRows
	tfOf  map[int64]int64
	end   int64
	last  int64
	xlMax string
}

func (g *Graph) readHist(ctx context.Context, known temporal.Scope, id int64, rel bool) (*hist, error) {
	ref, err := g.ref(id, rel)
	if err != nil {
		return nil, err
	}
	s, err := g.g.OpenSnapshot(ctx, v5.Fresh())
	if err != nil {
		return nil, mapV5(err)
	}
	defer s.Close()
	p, err := s.Timeline(ctx, v5.TimelineRequest{Ref: ref, Selector: g.selector(known, v5.ViewDeclared, fullSpan(), nil, nil, ""),
		Projection: v5.Projection{AllProperties: true, Revisions: true}, Paging: paging})
	es, err := drain(ctx, s, p, err)
	if err != nil {
		return nil, err
	}
	h := &hist{tfOf: map[int64]int64{}}
	var rev uint64
	for _, e := range es {
		if len(h.revs) == 0 || e.Revision != rev {
			rev = e.Revision
			h.revs = append(h.revs, revRows{at: g.ax.knownMs(e.Known)})
		}
		r := g.ax.decode(e)
		cur := &h.revs[len(h.revs)-1]
		cur.rows = append(cur.rows, r)
		if _, seen := h.tfOf[r.ver]; !seen {
			h.tfOf[r.ver] = cur.at
		}
		if r.ended && h.end == 0 {
			h.end = cur.at
		}
		h.last = max(h.last, cur.at)
		if len(r.xl) > len(h.xlMax) {
			h.xlMax = r.xl
		}
	}
	for _, v := range parseXl(h.xlMax) {
		h.last = max(h.last, v.tx)
	}
	return h, nil
}

// stamp completes a decoded row with the stamps of its version.
func (h *hist) stamp(r *row) {
	r.tf = h.tfOf[r.ver]
	if r.ended {
		r.dt = h.end
	}
}

// current is the newest commit's rows (every segment, oldest first), stamped.
func (h *hist) current() []row {
	if len(h.revs) == 0 {
		return nil
	}
	rows := slices.Clone(h.revs[len(h.revs)-1].rows)
	for i := range rows {
		h.stamp(&rows[i])
	}
	return rows
}

// rowsOfLast returns every valid-time segment of the entity as known up to the
// Known bound, oldest first, and the newest instant written to it.
func (g *Graph) rowsOfLast(ctx context.Context, known temporal.Scope, id int64, rel bool) ([]row, int64, error) {
	h, err := g.readHist(ctx, known, id, rel)
	if err != nil {
		return nil, 0, err
	}
	return h.current(), h.last, nil
}

func (g *Graph) rowsOf(ctx context.Context, known temporal.Scope, id int64, rel bool) ([]row, error) {
	rows, _, err := g.rowsOfLast(ctx, known, id, rel)
	return rows, err
}

// stampRows completes scan rows (which carry no revision data) with the stamps of
// their versions: one Timeline read per entity.
func (g *Graph) stampRows(ctx context.Context, known temporal.Scope, rows []row) error {
	hs := map[int64]*hist{}
	for i := range rows {
		r := &rows[i]
		h := hs[r.id]
		if h == nil {
			var err error
			if h, err = g.readHist(ctx, known, r.id, r.rel); err != nil {
				return err
			}
			hs[r.id] = h
		}
		h.stamp(r)
	}
	return nil
}

// newest is the row of the highest version (the first segment of it).
func newest(rows []row) (row, bool) {
	if len(rows) == 0 {
		return row{}, false
	}
	best := rows[0]
	for _, r := range rows[1:] {
		if r.ver > best.ver {
			best = r
		}
	}
	return best, true
}

// ended reports whether any segment carries the delete/retract marker.
func ended(rows []row) (dt int64, retracted, ok bool) {
	for _, r := range rows {
		if r.ended {
			return r.dt, r.dr, true
		}
	}
	return 0, false, false
}

func notFound(rel bool) error {
	if rel {
		return ErrRelNotFound
	}
	return ErrNodeNotFound
}

func uniqueIDs(rows []row) []int64 {
	var out []int64
	for _, r := range rows {
		if !slices.Contains(out, r.id) {
			out = append(out, r.id)
		}
	}
	return out
}
