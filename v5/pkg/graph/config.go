package graph

import (
	"context"
	"fmt"
	"strings"

	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/types"
)

// Durability selects when a committed Update is crash-safe (gap row 20).
// The zero value is invalid: durability is always chosen.
type Durability uint8

// Durability levels.
const (
	// DurabilityBuffered acknowledges before the log is synced. Memory graphs
	// may only use this level.
	DurabilityBuffered Durability = iota + 1
	// DurabilityCommit syncs before Update returns; failure is ErrCommitNotDurable.
	DurabilityCommit
)

// Config describes one graph. Exactly one of Dir and Memory is set.
type Config struct {
	// Dir is the storage directory (non-blank, no NUL).
	Dir string
	// Memory selects a non-persistent graph.
	Memory bool
	// GraphID identifies the graph; it must be non-zero and match DefaultAxis.
	GraphID types.GraphID
	// DefaultAxis binds this graph's default valid-time axis.
	DefaultAxis types.DefaultAxisBinding
	// KnowledgeAxis binds the axis of caller-recorded instants (Op.Recorded) and
	// of the Known scope of a Selector (millisecond instants; the v4 TxFrom/TxPin
	// values live on it in the compat layer). It is required and must be a
	// different axis than DefaultAxis, so a valid-time scope can never be passed
	// where knowledge is meant. Known filters by recorded evidence; the snapshot's
	// cut bounds what exists at all.
	KnowledgeAxis types.DefaultAxisBinding
	// Durability is required.
	Durability Durability
	// Limits bounds temporal values; the zero value means temporal defaults.
	Limits temporal.Limits
}

// Validate checks the config without touching storage. Errors wrap
// ErrInvalidConfig and never echo Dir.
func (c Config) Validate() error {
	bad := func(why string) error { return fmt.Errorf("%w: %s", ErrInvalidConfig, why) }
	switch {
	case c.Memory && c.Dir != "":
		return bad("Dir and Memory are mutually exclusive")
	case !c.Memory && strings.TrimSpace(c.Dir) == "":
		return bad("one of Dir or Memory is required")
	case strings.ContainsRune(c.Dir, 0):
		return bad("Dir contains NUL")
	}
	if err := c.Limits.Validate(); err != nil {
		return fmt.Errorf("%w: limits: %v", ErrInvalidConfig, err)
	}
	if c.GraphID == (types.GraphID{}) {
		return bad("GraphID is zero")
	}
	if err := c.DefaultAxis.Check(c.GraphID, c.DefaultAxis.Axis(), c.Limits); err != nil {
		return fmt.Errorf("%w: default axis: %v", ErrInvalidConfig, err)
	}
	if err := c.KnowledgeAxis.Check(c.GraphID, c.KnowledgeAxis.Axis(), c.Limits); err != nil {
		return fmt.Errorf("%w: knowledge axis: %v", ErrInvalidConfig, err)
	}
	if sameAxis(c.KnowledgeAxis.Axis(), c.DefaultAxis.Axis()) {
		return bad("KnowledgeAxis must differ from DefaultAxis")
	}
	switch c.Durability {
	case DurabilityBuffered:
	case DurabilityCommit:
		if c.Memory {
			return bad("a memory graph cannot be DurabilityCommit")
		}
	default:
		return bad("Durability is required")
	}
	return nil
}

// engine is the seam the future implementation fills. Unexported on purpose:
// the public surface is Graph, Tx and Snapshot.
type engine interface {
	update(ctx context.Context, fn func(Tx) error) (Receipt, error)
	openSnapshot(ctx context.Context, spec SnapshotSpec) (snapshotEngine, error)
	close() error
}

// Graph is an open graph. Until an engine exists every door validates its
// arguments and then returns ErrNotImplemented; a nil *Graph is ErrClosed.
type Graph struct {
	cfg Config
	eng engine
}

// Open validates cfg and opens the graph. Until an engine exists it returns a
// nil Graph and ErrNotImplemented (after validation, so config errors win).
func Open(ctx context.Context, cfg Config) (*Graph, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidConfig)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return nil, ErrNotImplemented
}

// Close releases the graph. Closing a nil Graph is ErrClosed.
func (g *Graph) Close() error {
	if g == nil {
		return ErrClosed
	}
	if g.eng == nil {
		return ErrNotImplemented
	}
	return g.eng.close()
}

// Update runs fn as one serializable write transaction and returns its Receipt
// (gap rows 2-7, 14-15). fn must not retain the Tx. If fn returns an error the
// transaction is discarded and the error is returned.
func (g *Graph) Update(ctx context.Context, fn func(Tx) error) (Receipt, error) {
	if g == nil {
		return Receipt{}, ErrClosed
	}
	if ctx == nil || fn == nil {
		return Receipt{}, fmt.Errorf("%w: nil context or function", ErrInvalidRequest)
	}
	if g.eng == nil {
		return Receipt{}, ErrNotImplemented
	}
	return g.eng.update(ctx, fn)
}

// OpenSnapshot opens a read view (gap rows 8-12, 21-22). The mode implies no
// valid-time coordinate; each read names its own scopes.
func (g *Graph) OpenSnapshot(ctx context.Context, spec SnapshotSpec) (*Snapshot, error) {
	if g == nil {
		return nil, ErrClosed
	}
	if ctx == nil {
		return nil, fmt.Errorf("%w: nil context", ErrInvalidRequest)
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	if g.eng == nil {
		return nil, ErrNotImplemented
	}
	se, err := g.eng.openSnapshot(ctx, spec)
	if err != nil {
		return nil, err
	}
	return &Snapshot{eng: se, cfg: g.cfg}, nil
}
