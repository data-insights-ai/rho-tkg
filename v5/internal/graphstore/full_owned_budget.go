package graphstore

import (
	"github.com/data-insights-ai/rho-tkg/v5/internal/state"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

func (q *pageReader) scopeWire(scope temporal.Scope) ([]byte, error) {
	if q.q.arena != nil {
		return q.q.arena.ScopeBytes(scope, q.q.c.limits.Temporal)
	}
	return temporal.AppendScope(nil, scope, q.q.c.limits.Temporal)
}
func (q *pageReader) reserveStateCodec(s state.State, l state.CodecLimits) (int, error) {
	if q.q.arena == nil {
		return 0, nil
	}
	admitted, err := q.q.arena.ReserveStateEncoding(s, l)
	return admitted, callerError(err)
}
func (q *pageReader) reserveChangeCodec(changes []state.Change, l state.CodecLimits) (int, error) {
	if q.q.arena == nil {
		return 0, nil
	}
	admitted, err := q.q.arena.ReserveChangeEncoding(changes, l)
	return admitted, callerError(err)
}
