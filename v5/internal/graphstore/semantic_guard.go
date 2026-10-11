package graphstore

import (
	"context"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
)

// ErrReadConflict means a checked Full root does not satisfy the supplied
// logical predicate. It is not a storage failure or a transaction decision.
var ErrReadConflict = errors.New("graphstore: prior graph read conflicts")

// SemanticGuard is a whole-partition conditional predicate. It is not a Full
// coverage proof, certified cut, lease or authority to install caller effects.
// Physical formats/handles, applied index and local generation are excluded.
// The outer application separately binds group incarnation and admission scope.
type SemanticGuard struct {
	Namespace                                                   Namespace
	OwnershipEpoch, TopologyEpoch, SchemaVersion, SemanticEpoch uint64
	EffectDigest                                                [32]byte
}

// NewSemanticGuard constructs checked logical metadata from a declared positive
// semantic epoch. The caller must obtain Root from its actual retained Catalog
// and read through that same Full view; this pure constructor does not prove
// coverage or store lifetime and does not scan application data.
func NewSemanticGuard(root Root) (SemanticGuard, error) {
	topology, err := root.SinglePartition()
	if err != nil {
		return SemanticGuard{}, err
	}
	guard := SemanticGuard{root.Namespace(), topology.OwnershipEpoch, topology.TopologyEpoch, topology.SchemaVersion, root.SemanticEpoch(), root.EffectDigest()}
	if err := guard.validate(); err != nil {
		return SemanticGuard{}, err
	}
	return guard, nil
}

// Conservative fixed representation ledger charges, not measured heap/RSS.
// The guard is charged once to source work; the separately returned Work is
// charged once beyond GraphEffects (which already embeds its own Work copy).
const semanticGuardMetadataBytes = 128
const semanticGuardWorkBytes = 64

func (g SemanticGuard) validate() error {
	if err := g.Namespace.validate(); err != nil {
		return err
	}
	if g.OwnershipEpoch == 0 || g.TopologyEpoch == 0 || g.SchemaVersion == 0 || g.SemanticEpoch == 0 || g.EffectDigest == ([32]byte{}) {
		return ErrInvalid
	}
	return nil
}
func (g SemanticGuard) compare(root Root) error {
	// Full physical coverage alone cannot manufacture logical initialization.
	if root.SemanticEpoch() == 0 {
		return ErrCorrupt
	}
	actual, err := NewSemanticGuard(root)
	if err != nil {
		return err
	}
	if g != actual {
		return ErrReadConflict
	}
	return nil
}

// StageGuardedOperations opens actual Full coverage once, compares logical
// metadata before cloning input/Plan, then uses StageOperations' typed stager.
// Every error returns zero GraphEffects. Work reports cumulative consumption
// after the Full reader opens, including conflict and later failures. If opening
// itself refuses, its existing API cannot expose partial work: Work is zero
// (unavailable), the operational error propagates and its internal caps apply.
// It neither installs nor turns a conflict into a durable request outcome.
func StageGuardedOperations(ctx context.Context, c *Catalog, guard SemanticGuard, ops []graphstate.Operation, revision state.Revision, l GraphLimits) (GraphEffects, PageWork, error) {
	return stageOperations(ctx, c, ops, revision, l, &guard)
}

// StageGuardedOperationsWithOutputBudget preserves the same logical predicate,
// coverage and failure semantics while admitting retained backing on a shared
// caller ledger. Nil/exhausted budget never opts out or resurrects defaults.
func StageGuardedOperationsWithOutputBudget(ctx context.Context, c *Catalog, guard SemanticGuard, ops []graphstate.Operation, revision state.Revision, l GraphLimits, budget *graphstate.OutputBudget) (GraphEffects, PageWork, error) {
	if budget == nil {
		return GraphEffects{}, PageWork{}, ErrInvalid
	}
	return stageOperationsWithOutputBudget(ctx, c, ops, revision, l, &guard, budget)
}
