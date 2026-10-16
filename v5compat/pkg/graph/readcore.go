package graph

import (
	"context"
	"fmt"
	"slices"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	v5t "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

var paging = v5.PageLimits{Rows: v5.MaxPageRows, MaxBytes: 8 << 20}

// pinSpec resolves a v4 transaction pin to a snapshot. pin 0 is the present.
// A pin below the first commit reads the genesis cut (the empty graph). A pin
// above the commit clock reads the present; tooNew reports it for the doors that
// refuse it (ErrTxPinTooNew).
func (g *Graph) pinSpec(pin int64) (spec v5.SnapshotSpec, tooNew bool) {
	if pin <= 0 {
		return v5.Fresh(), false
	}
	if cut, ok := g.led.cutFor(pin); ok {
		return v5.At(cut), pin > max(nowMs(), g.led.last())
	}
	return v5.Fresh(), false
}

func (g *Graph) selector(view v5.View, valid span, eq []v5.PropertyEq, labels []string, relType string) v5.Selector {
	return v5.Selector{View: view, Valid: g.ax.scope(valid), Known: g.ax.known(), Labels: labels, RelType: relType, Equals: eq}
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

// rowsOf returns every valid-time segment of the entity at spec, oldest first.
func (g *Graph) rowsOf(ctx context.Context, spec v5.SnapshotSpec, id int64, rel bool) ([]row, error) {
	ref, err := g.ref(id, rel)
	if err != nil {
		return nil, err
	}
	s, err := g.g.OpenSnapshot(ctx, spec)
	if err != nil {
		return nil, mapV5(err)
	}
	defer s.Close()
	req := v5.LookupRequest{Selector: g.selector(v5.ViewDeclared, fullSpan(), nil, nil, ""), Projection: v5.Projection{AllProperties: true}, Paging: paging}
	if rel {
		req.Rels = []v5t.RelID{ref.Rel()}
	} else {
		req.Nodes = []v5t.NodeID{ref.Node()}
	}
	p, err := s.Lookup(ctx, req)
	es, err := drain(ctx, s, p, err)
	if err != nil {
		return nil, err
	}
	out := make([]row, len(es))
	for i, e := range es {
		out[i] = g.ax.decode(e)
	}
	return out, nil
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
		if r.dt != 0 {
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
