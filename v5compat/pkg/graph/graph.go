// Package graph mirrors github.com/data-insights-ai/rho-tkg/v4/pkg/graph over
// v5/pkg/graph (WP-11): v4 names (New(Config), Nodes/Rels/Temporal/Tx/Batch
// sub-APIs, QueryOpts, the v4 sentinels) on the default axis, with `any` property
// values converted to graph.Value once at the edge (internal/convert).
//
// Deliberate differences, all documented where they act: IDs are plain int64
// (no snowflake clock or slot); caller instants (the *WithTx doors) are resolved
// by a commit-instant ledger (clock.go) because v5 caller stamps are evidence,
// never cuts; a property key holds one shadow property per value kind (codec.go);
// a Tx that carries several distinct caller instants commits as one v5 commit per
// instant (validated first, not crash-atomic across instants), and a Tx that
// creates entities has exactly one instant. The X list (store backends, declined
// Config knobs, DecomposeNodeID, IDSlotLease, foreign rels, the async LSN
// follower, UntimedFromTxFrom, runtime caller IDs, the v4 export format, tier
// admin, the vector index, MintInstant) returns ErrNotSupportedInV5.
package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	v5t "github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// idSeed offsets the v5 ID counter of a new compat graph (first ID idSeed+1).
const idSeed = 1 << 32

// Graph is an open compat graph.
type Graph struct {
	cfg    Config
	v5cfg  v5.Config
	ax     axes
	g      *v5.Graph
	led    *ledger
	mu     sync.Mutex // serialises writers (single process, as v4's tx lock)
	closed atomic.Bool
}

// New opens (or creates) the graph.
func New(cfg Config) (*Graph, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	v5cfg, err := v5Config(cfg)
	if err != nil {
		return nil, err
	}
	ctx := context.Background()
	g, err := v5.Open(ctx, v5cfg)
	if err != nil {
		return nil, mapV5(err)
	}
	led, err := openLedger(cfg.BadgerDir)
	if err != nil {
		_ = g.Close()
		return nil, err
	}
	w := &Graph{cfg: cfg, v5cfg: v5cfg, ax: axes{v5cfg}, g: g, led: led}
	if err := w.syncLedger(ctx); err != nil {
		_ = w.g.Close()
		_ = led.close()
		return nil, err
	}
	return w, nil
}

func v5Config(c Config) (v5.Config, error) {
	gid := v5t.GraphID{1}
	b, err := v5t.NewDefaultAxisBinding(gid, temporal.AxisID{1}, temporal.Limits{})
	if err != nil {
		return v5.Config{}, err
	}
	k, err := v5t.NewDefaultAxisBinding(gid, temporal.AxisID{2}, temporal.Limits{})
	if err != nil {
		return v5.Config{}, err
	}
	// IDs start above small integers: v4 IDs were large snowflakes, so consumers
	// (and the suite) treat a small literal such as a cursor of 1 as "before the
	// first ID". The seed is read only when the graph is created.
	out := v5.Config{GraphID: gid, DefaultAxis: b, KnowledgeAxis: k, Allocation: v5.Allocation{Seed: idSeed}}
	if c.memory() {
		out.Memory, out.Durability = true, v5.DurabilityBuffered
	} else {
		out.Dir, out.Durability = c.BadgerDir, v5.DurabilityCommit
	}
	return out, nil
}

// syncLedger gives a new graph its genesis entry and a reopened graph an entry
// for any commit the ledger missed.
func (g *Graph) syncLedger(ctx context.Context) error {
	s, err := g.g.OpenSnapshot(ctx, v5.Fresh())
	if err != nil {
		return mapV5(err)
	}
	cut := s.Cut()
	_ = s.Close()
	last, have := g.led.lastCut()
	switch {
	case !have:
		return g.led.add(0, cut)
	case string(last.Bytes()) != string(cut.Bytes()):
		return g.led.add(g.led.last()+1, cut)
	}
	return nil
}

// Close releases the graph; a second Close is ErrGraphClosed.
func (g *Graph) Close() error {
	if g == nil {
		return ErrNilGraph
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed.Swap(true) {
		return ErrGraphClosed
	}
	err := g.g.Close()
	return errors.Join(mapV5(err), g.led.close())
}

func (g *Graph) alive() error {
	if g == nil {
		return ErrNilGraph
	}
	if g.closed.Load() {
		return ErrGraphClosed
	}
	return nil
}

// mapV5 turns v5 errors into the v4 sentinels (the v5 error stays in the chain).
func mapV5(err error) error {
	if err == nil {
		return nil
	}
	pairs := []struct{ from, to error }{
		{v5.ErrClosed, ErrGraphClosed}, {v5.ErrUniqueViolation, ErrUniqueUnsupportedType},
	}
	for _, p := range pairs {
		if errors.Is(err, p.from) {
			return fmt.Errorf("%w (%w)", p.to, err)
		}
	}
	return err
}

// ---- sub-API accessors (v4 shape) ----

// NodesAPI is Graph.Nodes().
type NodesAPI struct{ g *Graph }

// RelsAPI is Graph.Rels().
type RelsAPI struct{ g *Graph }

// TemporalAPI is Graph.Temporal().
type TemporalAPI struct{ g *Graph }

// TxAPI is Graph.Tx().
type TxAPI struct{ g *Graph }

// BatchAPI is Graph.Batch().
type BatchAPI struct{ g *Graph }

// Nodes returns the node sub-API.
func (g *Graph) Nodes() *NodesAPI { return &NodesAPI{g} }

// Rels returns the relationship sub-API.
func (g *Graph) Rels() *RelsAPI { return &RelsAPI{g} }

// Temporal returns the temporal sub-API.
func (g *Graph) Temporal() *TemporalAPI { return &TemporalAPI{g} }

// Tx returns the transaction sub-API.
func (g *Graph) Tx() *TxAPI { return &TxAPI{g} }

// Batch returns the batch sub-API.
func (g *Graph) Batch() *BatchAPI { return &BatchAPI{g} }
