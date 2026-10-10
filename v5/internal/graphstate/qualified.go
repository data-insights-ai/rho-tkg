package graphstate

import (
	"context"

	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func checkQualifiedView(ctx context.Context, v ReadView, graph GraphID, l Limits) error {
	if _, err := l.resolve(); err != nil {
		return err
	}
	if ctx == nil || graph == (GraphID{}) {
		return ErrInvalidInput
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if nilProvider(v) {
		return ErrNilView
	}
	if v.Graph() == (GraphID{}) || v.Identity() == (ViewID{}) {
		return ErrInvalidView
	}
	if graph != v.Graph() {
		return ErrNamespace
	}
	return nil
}

// PlanQualified checks the explicit graph scope against the actual view before
// any touched reads, then uses Plan's one atomic algorithm. It supplies no routing,
// complete-read certificate, namespace conversion or installation authority.
func PlanQualified(ctx context.Context, v ReadView, graph GraphID, ops []Operation, revision state.Revision, l Limits) (Delta, error) {
	if err := checkQualifiedView(ctx, v, graph, l); err != nil {
		return Delta{}, err
	}
	delta, err := Plan(ctx, v, ops, revision, l)
	if err != nil {
		return Delta{}, err
	}
	if delta.Graph != graph || v.Graph() != graph {
		return Delta{}, ErrInvalidView
	}
	return delta, nil
}

// ProjectQualified checks graph scope before resolving a local entity handle.
// Historical meaning is still exactly the supplied immutable view's meaning.
func ProjectQualified(ctx context.Context, v ReadView, graph GraphID, id EntityID, at temporal.Position, mode Visibility, l Limits) (Projection, error) {
	if err := checkQualifiedView(ctx, v, graph, l); err != nil {
		return Projection{}, err
	}
	result, err := Project(ctx, v, id, at, mode, l)
	if err != nil {
		return Projection{}, err
	}
	if v.Graph() != graph {
		return Projection{}, ErrInvalidView
	}
	return result, nil
}
