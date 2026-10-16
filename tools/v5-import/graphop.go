package importmap

import (
	"fmt"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Graph builds the v5 op for c: the scope on c.DefaultAxis, Recorded on
// c.KnowledgeAxis (both millisecond axes). It does not validate; graph.CheckOp does.
func (o Op) Graph(c graph.Config) (graph.Op, error) {
	scope, err := o.Scope.temporal(c)
	if err != nil {
		return nil, err
	}
	rec, err := types.InstantPosition(c.KnowledgeAxis.Axis(), types.Instant(o.Recorded), c.Limits)
	if err != nil {
		return nil, fmt.Errorf("recorded: %w", err)
	}
	props := make(map[string]graph.Value, len(o.Props))
	for k, v := range o.Props {
		props[k] = v.Value
	}
	var ref graph.Ref
	switch {
	case o.Op == "create_node" || o.Op == "create_rel":
	case o.Entity == "rel":
		ref, err = graph.RelRef(types.RelID(o.ID))
	default:
		ref, err = graph.NodeRef(types.NodeID(o.ID))
	}
	if err != nil {
		return nil, err
	}
	switch o.Op {
	case "create_node":
		return graph.CreateNode{Labels: o.Labels, Props: props, Scope: scope, Recorded: rec}, nil
	case "create_rel":
		return graph.CreateRel{Type: o.Type, From: types.NodeID(o.Start), To: types.NodeID(o.End), Props: props, Scope: scope, Recorded: rec}, nil
	case "correct":
		return graph.Correct{Target: ref, Set: props, Scope: scope, Recorded: rec}, nil
	case "unset":
		return graph.Unset{Target: ref, Key: o.Key, Scope: scope, Recorded: rec}, nil
	case "add_label":
		return graph.AddLabel{Target: types.NodeID(o.ID), Label: o.Label, Scope: scope, Recorded: rec}, nil
	case "remove_label":
		return graph.RemoveLabel{Target: types.NodeID(o.ID), Label: o.Label, Scope: scope, Recorded: rec}, nil
	case "end":
		return graph.End{Target: ref, Scope: scope, Recorded: rec}, nil
	case "retract":
		return graph.Retract{Target: ref, Scope: scope, Recorded: rec}, nil
	}
	return nil, fmt.Errorf("importmap: unknown op %q", o.Op)
}

func (s Scope) temporal(c graph.Config) (temporal.Scope, error) {
	axis := c.DefaultAxis.Axis()
	edge := func(e Edge, lower bool) (temporal.Bound, error) {
		switch e.Inf {
		case -1:
			return temporal.NegativeInfinity(), nil
		case 1:
			return temporal.PositiveInfinity(), nil
		}
		p, err := types.InstantPosition(axis, types.Instant(e.MS), c.Limits)
		if err != nil {
			return temporal.Bound{}, err
		}
		return temporal.FiniteBound(p, lower) // lower bound inclusive, upper exclusive
	}
	if s.From.Inf == -1 && s.To.Inf == 1 {
		return temporal.All(axis)
	}
	lo, err := edge(s.From, true)
	if err != nil {
		return temporal.Scope{}, err
	}
	hi, err := edge(s.To, false)
	if err != nil {
		return temporal.Scope{}, err
	}
	return temporal.Span(axis, lo, hi, c.Limits)
}
