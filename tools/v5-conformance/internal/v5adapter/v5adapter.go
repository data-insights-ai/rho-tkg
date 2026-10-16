// Package v5adapter plugs the native v5/pkg/graph skeleton into the
// conformance suite. While v5/pkg/graph has no engine, Open returns an error
// that is both graph.ErrNotImplemented and conformance.ErrNotImplemented, so
// the suite skips every case with that reason. The Graph type below is the
// place the door mappings go once Open can succeed (instants map to
// temporal.Scope on the graph's default axis; MinInt64/0 sentinels map to
// temporal.Bound infinities).
package v5adapter

import (
	"context"
	"errors"
	"fmt"
	"testing"

	conformance "github.com/data-insights-ai/rho-tkg/tools/v5-conformance"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Adapter opens v5 graphs.
type Adapter struct{}

var _ conformance.Adapter = Adapter{}

// Config builds the graph.Config for the suite options. Exported so a test can
// check the mapping without an engine.
func Config(t testing.TB, o conformance.Options) (graph.Config, error) {
	gid := types.GraphID{1}
	b, err := types.NewDefaultAxisBinding(gid, temporal.AxisID{1}, temporal.Limits{})
	if err != nil {
		return graph.Config{}, err
	}
	c := graph.Config{GraphID: gid, DefaultAxis: b, Durability: graph.DurabilityCommit}
	if o.Dir == "" {
		c.Memory, c.Durability = true, graph.DurabilityBuffered
	} else {
		c.Dir = o.Dir
	}
	return c, nil
}

// notImplemented joins both sentinels so errors.Is matches either.
type notImplemented struct{ err error }

func (e notImplemented) Error() string { return e.err.Error() }
func (e notImplemented) Unwrap() []error {
	return []error{e.err, conformance.ErrNotImplemented}
}

// Open implements conformance.Adapter.
func (Adapter) Open(t testing.TB, o conformance.Options) (conformance.Graph, error) {
	cfg, err := Config(t, o)
	if err != nil {
		return nil, err
	}
	g, err := graph.Open(context.Background(), cfg)
	if errors.Is(err, graph.ErrNotImplemented) {
		return nil, notImplemented{err}
	}
	if err != nil {
		return nil, err
	}
	w := &Graph{g: g, cfg: cfg}
	t.Cleanup(func() { _ = w.g.Close() })
	return w, nil
}

// Graph will map the suite doors onto *graph.Graph. Every door reports the
// not-implemented class until its mapping lands with the engine.
type Graph struct {
	g   *graph.Graph
	cfg graph.Config
}

var _ conformance.Graph = (*Graph)(nil)

func (w *Graph) Close() error { return w.g.Close() }

func (w *Graph) Reopen() error {
	if err := w.g.Close(); err != nil {
		return err
	}
	g, err := graph.Open(context.Background(), w.cfg)
	if err != nil {
		return err
	}
	w.g = g
	return nil
}

func nope(door string) error {
	return notImplemented{fmt.Errorf("v5adapter: door %s not mapped yet: %w", door, graph.ErrNotImplemented)}
}

type C = context.Context

func (w *Graph) AddNode(C, conformance.NodeSpec) (conformance.Node, error) {
	return conformance.Node{}, nope("AddNode")
}
func (w *Graph) UpdateNode(C, uint64, map[string]any, int64) (conformance.Node, error) {
	return conformance.Node{}, nope("UpdateNode")
}
func (w *Graph) DeleteNode(C, uint64, int64) error { return nope("DeleteNode") }
func (w *Graph) GetOrCreateByKey(C, string, string, any, map[string]any) (conformance.Node, bool, error) {
	return conformance.Node{}, false, nope("GetOrCreateByKey")
}
func (w *Graph) AddRel(C, conformance.RelSpec) (conformance.Rel, error) {
	return conformance.Rel{}, nope("AddRel")
}
func (w *Graph) UpdateRel(C, uint64, map[string]any, int64) (conformance.Rel, error) {
	return conformance.Rel{}, nope("UpdateRel")
}
func (w *Graph) DeleteRel(C, uint64, int64) error { return nope("DeleteRel") }
func (w *Graph) SetNodeVersionInterval(C, uint64, int64, int64, map[string]any, int64) (conformance.Node, error) {
	return conformance.Node{}, nope("SetNodeVersionInterval")
}
func (w *Graph) SetRelVersionInterval(C, uint64, int64, int64, map[string]any, int64) (conformance.Rel, error) {
	return conformance.Rel{}, nope("SetRelVersionInterval")
}
func (w *Graph) CloseNodeVersion(C, uint64, int64) error { return nope("CloseNodeVersion") }
func (w *Graph) CloseRelVersion(C, uint64, int64) error  { return nope("CloseRelVersion") }
func (w *Graph) Tx(C, func(conformance.Tx) error) error  { return nope("Tx") }
func (w *Graph) GetNode(C, uint64) (conformance.Node, error) {
	return conformance.Node{}, nope("GetNode")
}
func (w *Graph) GetRel(C, uint64) (conformance.Rel, error) { return conformance.Rel{}, nope("GetRel") }
func (w *Graph) NodesByLabel(string, conformance.Query) ([]conformance.Node, error) {
	return nil, nope("NodesByLabel")
}
func (w *Graph) RelsByType(string, conformance.Query) ([]conformance.Rel, error) {
	return nil, nope("RelsByType")
}
func (w *Graph) NodesByLabelAndProperty(string, string, any, conformance.Query) ([]conformance.Node, error) {
	return nil, nope("NodesByLabelAndProperty")
}
func (w *Graph) NodeAsOf(uint64, int64) (conformance.Node, error) {
	return conformance.Node{}, nope("NodeAsOf")
}
func (w *Graph) RelAsOf(uint64, int64) (conformance.Rel, error) {
	return conformance.Rel{}, nope("RelAsOf")
}
func (w *Graph) NodeAt(uint64, int64) (conformance.Node, error) {
	return conformance.Node{}, nope("NodeAt")
}
func (w *Graph) NodeAtTx(uint64, int64, int64) (conformance.Node, error) {
	return conformance.Node{}, nope("NodeAtTx")
}
func (w *Graph) RelAtTx(uint64, int64, int64) (conformance.Rel, error) {
	return conformance.Rel{}, nope("RelAtTx")
}
func (w *Graph) NodeHistory(uint64) ([]conformance.Node, error) { return nil, nope("NodeHistory") }
func (w *Graph) RelHistory(uint64) ([]conformance.Rel, error)   { return nil, nope("RelHistory") }
func (w *Graph) CommittedTx() (int64, error)                    { return 0, nope("CommittedTx") }
func (w *Graph) RelsEffectivePage(C, conformance.Pin, conformance.Filter, uint64, int) ([]conformance.RelSegment, uint64, error) {
	return nil, 0, nope("RelsEffectivePage")
}
