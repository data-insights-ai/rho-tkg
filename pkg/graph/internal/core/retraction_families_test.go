package core

import (
	"errors"
	"fmt"
	"strings"

	storepkg "github.com/data-insights-ai/rho-tkg/v4/pkg/graph/store"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/graph/temporal"
	"github.com/data-insights-ai/rho-tkg/v4/pkg/types"
)

// rtFamily is one read-door family of the retraction matrix. pinned families
// answer at a transaction-time pin; the others read the current belief (the
// pin is ignored). q returns applicable == false when the backend does not
// offer the door (an optional capability), so the family is skipped there.
type rtFamily struct {
	name   string
	pinned bool
	q      func(g *Core, w *rtWorld, pin, v types.Instant) (a answer, applicable bool, err error)
}

// absentErr reports a "not there" answer of a point door.
func absentErr(err error) bool {
	return errors.Is(err, storepkg.ErrNodeNotFound) || errors.Is(err, storepkg.ErrRelNotFound) ||
		errors.Is(err, storepkg.ErrNoVersionValidAt) || errors.Is(err, ErrNoVersionAsOf)
}

func (w *rtWorld) nodeIDs() []types.NodeID { return []types.NodeID{w.a, w.x, w.y} }

// joining is the world's one relationship between two nodes (0: none).
func (w *rtWorld) joining(a, b types.NodeID) types.RelID {
	pair := func(p, q types.NodeID) bool { return (a == p && b == q) || (a == q && b == p) }
	switch {
	case pair(w.a, w.x):
		return w.rx
	case pair(w.a, w.y):
		return w.ry
	case pair(w.x, w.y):
		return w.rxy
	}
	return 0
}
func (w *rtWorld) relIDs() []types.RelID { return []types.RelID{w.rx, w.ry, w.rxy} }

// pointNodes runs a point door over every node of the world.
func pointNodes(w *rtWorld, get func(id types.NodeID) (*types.Node, error)) (answer, bool, error) {
	out := answer{}
	for _, id := range w.nodeIDs() {
		n, err := get(id)
		if absentErr(err) {
			continue
		}
		if err != nil {
			return nil, true, err
		}
		out[nodeKey(id)] = nodeRowString(n)
	}
	return out, true, nil
}

func pointRels(w *rtWorld, get func(id types.RelID) (*types.Relationship, error)) (answer, bool, error) {
	out := answer{}
	for _, id := range w.relIDs() {
		r, err := get(id)
		if absentErr(err) {
			continue
		}
		if err != nil {
			return nil, true, err
		}
		out[relKey(id)] = relRowString(r)
	}
	return out, true, nil
}

func nodesOrErr(ns []*types.Node, err error) (answer, bool, error) {
	if err != nil {
		return nil, true, err
	}
	return nodesAnswer(ns), true, nil
}

func relsOrErr(rs []*types.Relationship, err error) (answer, bool, error) {
	if err != nil {
		return nil, true, err
	}
	return relsAnswer(rs), true, nil
}

// merge unions answers (keys are entity keys, so a union of per-value lookups
// is the set the reader saw).
func merge(as ...answer) answer {
	out := answer{}
	for _, a := range as {
		for k, v := range a {
			out[k] = v
		}
	}
	return out
}

func nodeSegmentsString(segs []temporal.NodeSegment) string {
	parts := make([]string, 0, len(segs))
	for _, s := range segs {
		parts = append(parts, fmt.Sprintf("[%d,%d)=%s", s.ValidFrom, s.ValidTo, nodeRowString(s.Node)))
	}
	return strings.Join(parts, " ")
}

func relSegmentsString(segs []temporal.RelSegment) string {
	parts := make([]string, 0, len(segs))
	for _, s := range segs {
		parts = append(parts, fmt.Sprintf("[%d,%d)=%s", s.ValidFrom, s.ValidTo, relRowString(s.Rel)))
	}
	return strings.Join(parts, " ")
}

const rtSpan = 1000 // the interval doors probe [v, v+rtSpan)

func rtFamilies() []rtFamily {
	all := types.AllRelations()
	st := func(v, pin types.Instant) storepkg.QueryOpts { return storepkg.QueryOpts{ValidAt: v, TxAt: pin} }
	iv := func(v, pin types.Instant) storepkg.QueryOpts {
		return storepkg.QueryOpts{ValidStart: v, ValidEnd: v + rtSpan, TxAt: pin}
	}
	pinOnly := func(pin types.Instant) storepkg.QueryOpts { return storepkg.QueryOpts{TxPin: pin} }
	return []rtFamily{
		// ---------------------------------------------------------- nodes
		{"NodeAt", false, func(g *Core, w *rtWorld, _, v types.Instant) (answer, bool, error) {
			return pointNodes(w, func(id types.NodeID) (*types.Node, error) { return g.Temporal.NodeAt(id, v) })
		}},
		{"NodeAtTx", true, func(g *Core, w *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return pointNodes(w, func(id types.NodeID) (*types.Node, error) { return g.Temporal.NodeAtTx(id, v, pin) })
		}},
		{"NodeAsOf", true, func(g *Core, w *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return pointNodes(w, func(id types.NodeID) (*types.Node, error) { return g.Temporal.NodeAsOf(id, pin) })
		}},
		{"NodesAt", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Temporal.NodesAt(v))
		}},
		{"NodesByLabelAt", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Temporal.NodesByLabelAt("Pers", v))
		}},
		{"NodesAtTx", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Temporal.NodesAtTx(v, pin))
		}},
		{"NodesAsOf", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Temporal.NodesAsOf(pin))
		}},
		{"NodesDuring", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Temporal.NodesDuring(v, v+rtSpan))
		}},
		{"NodesDuringTx", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Temporal.NodesDuringTx(v, v+rtSpan, pin))
		}},
		{"NodesRelating", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Temporal.NodesRelating(v, v+rtSpan, all))
		}},
		{"NodesByLabelPropertyAt", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			var out []answer
			for _, k := range []string{"x", "y"} {
				a, _, err := nodesOrErr(g.Temporal.NodesByLabelPropertyAt("Pers", "k", k, v))
				if err != nil {
					return nil, true, err
				}
				out = append(out, a)
			}
			return merge(out...), true, nil
		}},
		{"NodesByLabelPropertyDuring", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			var out []answer
			for _, k := range []string{"x", "y"} {
				a, _, err := nodesOrErr(g.Temporal.NodesByLabelPropertyDuring("Pers", "k", k, v, v+rtSpan))
				if err != nil {
					return nil, true, err
				}
				out = append(out, a)
			}
			return merge(out...), true, nil
		}},
		{"ByLabel{ValidAt,TxAt}+CountByLabelAt", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return nodesCounted(g, st(v, pin))
		}},
		{"ByLabel{TxPin}+CountByLabelAt", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return nodesCounted(g, pinOnly(pin))
		}},
		{"ByLabel{ValidStart,ValidEnd,TxAt}+CountByLabelAt", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return nodesCounted(g, iv(v, pin))
		}},
		{"Nodes.All{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Nodes.All(st(v, pin)))
		}},
		{"Nodes.All{TxPin}", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return nodesOrErr(g.Nodes.All(pinOnly(pin)))
		}},
		{"ForEachByLabel{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			var ns []*types.Node
			err := g.Nodes.ForEachByLabel("Pers", st(v, pin), func(n *types.Node) bool { ns = append(ns, n); return true })
			return nodesOrErr(ns, err)
		}},
		{"ByLabelAndProperty{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return nodePropLookup(g, st(v, pin))
		}},
		{"ByLabelAndProperty{TxPin}", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return nodePropLookup(g, pinOnly(pin))
		}},
		{"ForEachByLabelPropertyRange{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			var ns []*types.Node
			err := g.Nodes.ForEachByLabelPropertyRange("Pers", "n", 0, 1000, true, true, st(v, pin), func(n *types.Node) bool { ns = append(ns, n); return true })
			return nodesOrErr(ns, err)
		}},
		{"ScanNodeColumns{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			out := answer{}
			ok, err := g.ScanNodeColumns("Pers", []string{"n"}, st(v, pin), func(b *storepkg.ColumnBatch) bool {
				for i, id := range b.IDs {
					out[nodeKey(id)] = fmt.Sprintf("[%d,%d)", b.ValidFrom[i], b.ValidTo[i])
				}
				return true
			})
			return out, ok, err
		}},
		{"ForEachDocValuesAsOf", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			out := answer{}
			_, ok, err := g.Nodes.ForEachDocValuesAsOf("Pers", []string{"k", "n"}, pin, func(id types.NodeID, vals []any, present []bool) bool {
				out[nodeKey(id)] = fmt.Sprintf("%v %v", vals, present)
				return true
			})
			return out, ok, err
		}},
		{"NodeEffectiveTimeline", true, func(g *Core, w *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			out := answer{}
			for _, id := range w.nodeIDs() {
				segs, err := g.Temporal.NodeEffectiveTimeline(id, pin)
				if err != nil {
					return nil, true, err
				}
				if len(segs) > 0 {
					out[nodeKey(id)] = nodeSegmentsString(segs)
				}
			}
			return out, true, nil
		}},
		{"ForEachNodeEffectiveByLabel", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			by := map[types.NodeID][]temporal.NodeSegment{}
			err := g.Temporal.ForEachNodeEffectiveByLabel("Pers", pin, func(s temporal.NodeSegment) bool {
				by[s.Node.ID()] = append(by[s.Node.ID()], s)
				return true
			})
			if err != nil {
				return nil, true, err
			}
			out := answer{}
			for id, segs := range by {
				out[nodeKey(id)] = nodeSegmentsString(segs)
			}
			return out, true, nil
		}},
		{"Snapshot", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			s, err := g.Temporal.Snapshot(v)
			if err != nil {
				return nil, true, err
			}
			return merge(nodesAnswer(s.Nodes), relsAnswer(s.Relationships)), true, nil
		}},
		{"Diff", false, func(g *Core, w *rtWorld, _, v types.Instant) (answer, bool, error) {
			d, err := g.Temporal.Diff(w.v0-10, v)
			if err != nil {
				return nil, true, err
			}
			out := answer{}
			for _, n := range d.NodesCreated {
				out[nodeKey(n.ID())] = "created " + nodeRowString(n)
			}
			for _, n := range d.NodesDeleted {
				out[nodeKey(n.ID())] = "deleted " + nodeRowString(n)
			}
			for _, u := range d.NodesUpdated {
				out[nodeKey(u.After.ID())] = "updated " + nodeRowString(u.After)
			}
			for _, r := range d.RelsCreated {
				out[relKey(r.ID())] = "created " + relRowString(r)
			}
			for _, r := range d.RelsDeleted {
				out[relKey(r.ID())] = "deleted " + relRowString(r)
			}
			for _, u := range d.RelsUpdated {
				out[relKey(u.After.ID())] = "updated " + relRowString(u.After)
			}
			return out, true, nil
		}},
		{"NeighborsAt", false, func(g *Core, w *rtWorld, _, v types.Instant) (answer, bool, error) {
			out := answer{}
			for _, id := range w.nodeIDs() {
				ns, err := g.Temporal.NeighborsAt(id, v)
				if absentErr(err) {
					continue
				}
				if err != nil {
					return nil, true, err
				}
				for _, n := range ns {
					// keyed by the neighbor, the node and the one relationship
					// joining them in the world: a retracted neighbor or a
					// retracted joining relationship removes the entry.
					out[nodeKey(n.ID())+"<-"+nodeKey(id)+"<-"+relKey(w.joining(n.ID(), id))] = nodeRowString(n)
				}
			}
			return out, true, nil
		}},

		// ---------------------------------------------------------- relationships
		{"RelAt", false, func(g *Core, w *rtWorld, _, v types.Instant) (answer, bool, error) {
			return pointRels(w, func(id types.RelID) (*types.Relationship, error) { return g.Temporal.RelAt(id, v) })
		}},
		{"RelAtTx", true, func(g *Core, w *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return pointRels(w, func(id types.RelID) (*types.Relationship, error) { return g.Temporal.RelAtTx(id, v, pin) })
		}},
		{"RelAsOf", true, func(g *Core, w *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return pointRels(w, func(id types.RelID) (*types.Relationship, error) { return g.Temporal.RelAsOf(id, pin) })
		}},
		{"RelsAt", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return relsOrErr(g.Temporal.RelsAt(v))
		}},
		{"RelsByTypeAt", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return relsOrErr(g.Temporal.RelsByTypeAt("LINK", v))
		}},
		{"RelsAtTx", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return relsOrErr(g.Temporal.RelsAtTx(v, pin))
		}},
		{"RelsAsOf", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return relsOrErr(g.Temporal.RelsAsOf(pin))
		}},
		{"RelsDuring", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return relsOrErr(g.Temporal.RelsDuring(v, v+rtSpan))
		}},
		{"RelsDuringTx", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return relsOrErr(g.Temporal.RelsDuringTx(v, v+rtSpan, pin))
		}},
		{"RelsRelating", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			return relsOrErr(g.Temporal.RelsRelating(v, v+rtSpan, all))
		}},
		{"RelsByTypePropertyAt", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			var out []answer
			for _, k := range []string{"rx", "ry", "rxy"} {
				a, _, err := relsOrErr(g.Temporal.RelsByTypePropertyAt("LINK", "k", k, v))
				if err != nil {
					return nil, true, err
				}
				out = append(out, a)
			}
			return merge(out...), true, nil
		}},
		{"RelsByTypePropertyDuring", false, func(g *Core, _ *rtWorld, _, v types.Instant) (answer, bool, error) {
			var out []answer
			for _, k := range []string{"rx", "ry", "rxy"} {
				a, _, err := relsOrErr(g.Temporal.RelsByTypePropertyDuring("LINK", "k", k, v, v+rtSpan))
				if err != nil {
					return nil, true, err
				}
				out = append(out, a)
			}
			return merge(out...), true, nil
		}},
		{"ByType{ValidAt,TxAt}+CountByTypeAt", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return relsCounted(g, st(v, pin))
		}},
		{"ByType{TxPin}+CountByTypeAt", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return relsCounted(g, pinOnly(pin))
		}},
		{"ByType{ValidStart,ValidEnd,TxAt}+CountByTypeAt", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return relsCounted(g, iv(v, pin))
		}},
		{"Rels.All{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return relsOrErr(g.Rels.All(st(v, pin)))
		}},
		{"ForEachByType{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			var rs []*types.Relationship
			err := g.Rels.ForEachByType("LINK", st(v, pin), func(r *types.Relationship) bool { rs = append(rs, r); return true })
			return relsOrErr(rs, err)
		}},
		{"ByTypeAndProperty{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			return relPropLookup(g, st(v, pin))
		}},
		{"ByTypeAndProperty{TxPin}", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			return relPropLookup(g, pinOnly(pin))
		}},
		{"ForEachByTypePropertyRange{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			var rs []*types.Relationship
			err := g.Rels.ForEachByTypePropertyRange("LINK", "w", 0, 1000, true, true, st(v, pin), func(r *types.Relationship) bool { rs = append(rs, r); return true })
			return relsOrErr(rs, err)
		}},
		{"ScanRelColumns{ValidAt,TxAt}", true, func(g *Core, _ *rtWorld, pin, v types.Instant) (answer, bool, error) {
			out := answer{}
			ok, err := g.ScanRelColumns("LINK", []string{"w"}, st(v, pin), func(b *storepkg.RelColumnBatch) bool {
				for i, id := range b.IDs {
					out[relKey(id)] = fmt.Sprintf("%d->%d", b.StartIDs[i], b.EndIDs[i])
				}
				return true
			})
			return out, ok, err
		}},
		{"RelEffectiveTimeline", true, func(g *Core, w *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			out := answer{}
			for _, id := range w.relIDs() {
				segs, err := g.Temporal.RelEffectiveTimeline(id, pin)
				if err != nil {
					return nil, true, err
				}
				if len(segs) > 0 {
					out[relKey(id)] = relSegmentsString(segs)
				}
			}
			return out, true, nil
		}},
		{"ForEachRelEffectiveByType", true, func(g *Core, _ *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			by := map[types.RelID][]temporal.RelSegment{}
			err := g.Temporal.ForEachRelEffectiveByType("LINK", pin, func(s temporal.RelSegment) bool {
				by[s.Rel.ID()] = append(by[s.Rel.ID()], s)
				return true
			})
			if err != nil {
				return nil, true, err
			}
			out := answer{}
			for id, segs := range by {
				out[relKey(id)] = relSegmentsString(segs)
			}
			return out, true, nil
		}},
		{"OutgoingRelsAt+IncomingRelsAt", false, func(g *Core, w *rtWorld, _, v types.Instant) (answer, bool, error) {
			var out []answer
			for _, id := range []types.NodeID{w.a, w.y} {
				o, _, err := relsOrErr(g.Temporal.OutgoingRelsAt(id, v))
				if err != nil && !absentErr(err) {
					return nil, true, err
				}
				in, _, err := relsOrErr(g.Temporal.IncomingRelsAt(id, v))
				if err != nil && !absentErr(err) {
					return nil, true, err
				}
				out = append(out, o, in)
			}
			return merge(out...), true, nil
		}},
		{"ForEachAdjacentRelAt{ValidAt,TxAt}", true, func(g *Core, w *rtWorld, pin, v types.Instant) (answer, bool, error) {
			var rs []*types.Relationship
			collect := func(r *types.Relationship) bool { rs = append(rs, r); return true }
			if err := g.Rels.ForEachAdjacentRelAt(w.a, "", false, st(v, pin), collect); err != nil {
				return nil, true, err
			}
			if err := g.Rels.ForEachAdjacentRelAt(w.y, "", true, st(v, pin), collect); err != nil {
				return nil, true, err
			}
			return relsAnswer(rs), true, nil
		}},
		{"ForEachAdjacentEndpointAt{ValidAt,TxAt}", true, func(g *Core, w *rtWorld, pin, v types.Instant) (answer, bool, error) {
			out := answer{}
			collect := func(rel types.RelID, other types.NodeID) bool {
				out[relKey(rel)] = nodeKey(other)
				return true
			}
			if err := g.Rels.ForEachAdjacentEndpointAt(w.a, "", false, st(v, pin), collect); err != nil {
				return nil, true, err
			}
			if err := g.Rels.ForEachAdjacentEndpointAt(w.y, "", true, st(v, pin), collect); err != nil {
				return nil, true, err
			}
			return out, true, nil
		}},
		{"OutgoingForNodesAtTx+IncomingForNodesAtTx", true, func(g *Core, w *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			o, err := g.Rels.OutgoingForNodesAtTx([]types.NodeID{w.a}, "", pin)
			if err != nil {
				return nil, true, err
			}
			in, err := g.Rels.IncomingForNodesAtTx([]types.NodeID{w.y}, "", pin)
			if err != nil {
				return nil, true, err
			}
			return merge(relsAnswer(o[w.a]), relsAnswer(in[w.y])), true, nil
		}},
		{"OutgoingForNodesAtPin+IncomingForNodesAtPin", true, func(g *Core, w *rtWorld, pin, _ types.Instant) (answer, bool, error) {
			o, err := g.Rels.OutgoingForNodesAtPin([]types.NodeID{w.a}, "", pin)
			if err != nil {
				return nil, true, err
			}
			in, err := g.Rels.IncomingForNodesAtPin([]types.NodeID{w.y}, "", pin)
			if err != nil {
				return nil, true, err
			}
			return merge(relsAnswer(o[w.a]), relsAnswer(in[w.y])), true, nil
		}},
	}
}

// nodesCounted is ByLabel("Pers", opts), checked against CountByLabelAt with
// the same opts (the count door must count exactly the scan's set).
func nodesCounted(g *Core, opts storepkg.QueryOpts) (answer, bool, error) {
	ns, err := g.Nodes.ByLabel("Pers", opts)
	if err != nil {
		return nil, true, err
	}
	n, err := g.Nodes.CountByLabelAt("Pers", opts)
	if err != nil {
		return nil, true, err
	}
	if n != len(ns) {
		return nil, true, fmt.Errorf("CountByLabelAt = %d, ByLabel returned %d rows", n, len(ns))
	}
	return nodesAnswer(ns), true, nil
}

func relsCounted(g *Core, opts storepkg.QueryOpts) (answer, bool, error) {
	rs, err := g.Rels.ByType("LINK", opts)
	if err != nil {
		return nil, true, err
	}
	n, err := g.Rels.CountByTypeAt("LINK", opts)
	if err != nil {
		return nil, true, err
	}
	if n != len(rs) {
		return nil, true, fmt.Errorf("CountByTypeAt = %d, ByType returned %d rows", n, len(rs))
	}
	return relsAnswer(rs), true, nil
}

func nodePropLookup(g *Core, opts storepkg.QueryOpts) (answer, bool, error) {
	var out []answer
	for _, k := range []string{"x", "y"} {
		a, _, err := nodesOrErr(g.Nodes.ByLabelAndProperty("Pers", "k", k, opts))
		if err != nil {
			return nil, true, err
		}
		out = append(out, a)
	}
	return merge(out...), true, nil
}

func relPropLookup(g *Core, opts storepkg.QueryOpts) (answer, bool, error) {
	var out []answer
	for _, k := range []string{"rx", "ry", "rxy"} {
		a, _, err := relsOrErr(g.Rels.ByTypeAndProperty("LINK", "k", k, opts))
		if err != nil {
			return nil, true, err
		}
		out = append(out, a)
	}
	return merge(out...), true, nil
}
