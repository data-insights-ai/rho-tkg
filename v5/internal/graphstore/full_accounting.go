package graphstore

import (
	"encoding/binary"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// Fixed allowances cover the materialized structs, independently tested with
// unsafe.Sizeof. Variable ownership additionally covers exact-cap decoded
// interval/piece arrays, immutable axis strings and conservative wide-coordinate
// backing. These are representation allowances, not exact allocator/RSS sizes.
const (
	fullEntityOutputBytes     = 192
	fullLifeOutputBytes       = 64
	fullPropertyOutputBytes   = 64
	fullValueOutputBytes      = 256
	fullComponentOutputBytes  = 512
	fullPageOutputBytes       = 128
	fullDependencyOutputBytes = 1024
	fullPatchOutputBytes      = 512
	fullGroupOutputBytes      = 256
	scopeIntervalOwnedBytes   = 768
)

func axisVariableBytes(axis temporal.Axis) int {
	d := axis.Descriptor()
	return len(d.Reference) + len(d.CanonicalUnit)
}

// Scope v1 has a fixed53-byte header; only Region adds a uint32 atom count.
// This is accounting for bytes produced by AppendScope, not an index predicate
// or substitute decoder. Unknown future framing refuses rather than undercounts.
func scopeOwnedBacking(wire []byte) (int, error) {
	if len(wire) < 53 || wire[0] != 'T' || wire[1] != 'S' || wire[2] != 1 {
		return 0, ErrCorrupt
	}
	pieces := 0
	switch temporal.ScopeKind(wire[52]) {
	case temporal.ScopePoint, temporal.ScopeSpan, temporal.ScopeAll:
		pieces = 1
	case temporal.ScopeRegion:
		if len(wire) < 57 {
			return 0, ErrCorrupt
		}
		n := binary.BigEndian.Uint32(wire[53:57])
		if n > 65536 {
			return 0, ErrCorrupt
		}
		pieces = int(n)
	case temporal.ScopeEmpty, temporal.ScopeUnplaced:
	default:
		return 0, ErrCorrupt
	}
	return scopeIntervalOwnedBytes*pieces + 4*len(wire), nil
} // #nosec G115 -- atom count is checked <=65536 before conversion.
func stateOwnedBacking(s state.State) int {
	// Exact normalization below guarantees cap(pieces)==Usage.Pieces(). Every
	// state piece owns one atomic scope; Usage covers its canonical coordinates
	// and complete shared axis definition in addition to these array allowances.
	return (256+scopeIntervalOwnedBytes)*s.Usage().Pieces() + 4*s.Usage().MetadataBytes()
}
func componentOwnedBytes(s state.State, w temporal.Scope, wire []byte) (int, error) {
	backing, err := scopeOwnedBacking(wire)
	return fullComponentOutputBytes + backing + axisVariableBytes(w.Axis()) + stateOwnedBacking(s), err
}
func (q *pageReader) exactComponentOutput(s state.State, w temporal.Scope, budget graphstate.ReadBudget) (state.State, temporal.Scope, error) {
	scopeWire, err := q.scopeWire(w)
	if err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	cost, err := componentOwnedBytes(s, w, scopeWire)
	if err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	if cost > budget.Bytes {
		return state.State{}, temporal.Scope{}, ErrResourceLimit
	}
	if err := q.q.materialize(cost + 2*cap(scopeWire)); err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	// Encode before decode bounds actual output before allocating its exact arrays.
	codec := q.limits.codecLimits(q.q.c)
	admitted, err := q.reserveStateCodec(s, codec)
	if err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	wire, err := state.AppendState(nil, s, codec)
	if err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	if err := q.q.materializeReserved(2*cap(wire), admitted); err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	s, err = state.DecodeState(wire, w.Axis(), codec)
	if err != nil {
		return state.State{}, temporal.Scope{}, err
	}
	w, err = temporal.DecodeScope(scopeWire, w.Axis(), q.q.c.limits.Temporal)
	return s, w, err
}
func scalarVariableOwned(value graphstate.Scalar, keyBytes int, l Limits) (int, error) {
	if value.Kind() == graphstate.ScalarDescriptor {
		key, err := value.EqualityKey(l.valueLimits())
		if err != nil {
			return 0, err
		}
		return descriptorOwnedBacking(len(key) - 1), nil
	}
	if text, ok := value.StringValue(); ok {
		return len(text), nil
	}
	if scope, ok := value.Scope(); ok {
		wire, err := temporal.AppendScope(nil, scope, l.Temporal)
		if err != nil {
			return 0, err
		}
		backing, err := scopeOwnedBacking(wire)
		return backing + axisVariableBytes(scope.Axis()), err
	}
	return keyBytes, nil
}

// Inspect the already delivered value envelope before DecodeScope allocates
// compact-region interval/wide-coordinate backing. Primitive catalogs keep
// their existing wire ledger; Full source budgets also charge materialization.
func (q *reader) preflightFullValue(src []byte) error {
	c, err := inspectRecord(src, q.c.root.namespace, valueRecord, q.c.limits)
	if err != nil {
		return err
	}
	if _, err := c.take(16); err != nil {
		return err
	}
	key, err := c.field(q.c.limits.MaxValueBytes)
	if err != nil {
		return err
	}
	if len(key) > 0 && graphstate.ScalarKind(key[0]) == graphstate.ScalarDescriptor {
		return q.materialize(descriptorOwnedBacking(len(key) - 1))
	}
	if q.full == nil && q.fullView == nil && !isFullTopology(q.c.root.topology) {
		return nil
	}
	if len(key) > 0 && graphstate.ScalarKind(key[0]) == graphstate.ScalarScope {
		backing, err := scopeOwnedBacking(key[1:])
		if err != nil {
			return err
		}
		return q.materialize(backing)
	}
	return nil
}

// DO1 references use at least62 wire bytes each. Eight times delivered bytes
// plus512 covers owned Spec/reference slots, name/payload copies and bounded
// codec scratch conservatively, without calling Spec (which itself copies).
// This is a representation/work allowance, not measured heap or RSS.
func descriptorOwnedBacking(wireBytes int) int { return 512 + 8*wireBytes }
