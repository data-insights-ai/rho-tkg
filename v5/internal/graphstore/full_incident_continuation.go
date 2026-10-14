package graphstore

import (
	"context"
	"crypto/sha256"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/pkg/temporal"
)

// IncidentContinuation carries only a genuine canonical incident continuation.
// Its private state has no decoder, constructor, last-ID setter or portable
// authority. It may outlive its issuing handle in the same process. Import still
// requires a separately authenticated retained source with the identical root.
type IncidentContinuation struct{ issued *incidentContinuation }
type incidentContinuation struct {
	view   graphstate.ViewID
	cursor fullContinuation
}

func (v *ReadView) incidentFingerprint(q *pageReader, p graphstate.IncidentPredicate) ([32]byte, error) {
	if p.Endpoint == 0 || p.Window.Kind() == temporal.ScopeInvalid || p.Window.Kind() == temporal.ScopeUnplaced {
		return [32]byte{}, ErrInvalid
	}
	window, err := q.queryScope(p.Window)
	if err != nil {
		return [32]byte{}, err
	}
	if err := q.q.materialize(256); err != nil {
		return [32]byte{}, err
	}
	data := append([]byte("full:incident:v1\x00"), v.id[:]...)
	data = appendComponent(data, graphstate.ComponentKey{Owner: p.Endpoint, Life: p.Life})
	data = appendField(data, window)
	return sha256.Sum256(data), nil
}

// ExportIncidentContinuation checks the actual private cursor, including its
// exact endpoint/life/axis/window fingerprint. A start/end token cannot mint one.
func (v *ReadView) ExportIncidentContinuation(ctx context.Context, p graphstate.IncidentPredicate, token graphstate.Cursor) (IncidentContinuation, error) {
	if v == nil || token == 0 {
		return IncidentContinuation{}, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return IncidentContinuation{}, err
	}
	hash, err := v.incidentFingerprint(q, p)
	var old fullContinuation
	if err == nil {
		old, err = v.oldCursor(token, canonicalIncidentRecord, hash)
	}
	if err == nil {
		err = q.q.materialize(320)
	}
	if err = v.finish(q, err); err != nil {
		return IncidentContinuation{}, err
	}
	if err = v.output(320); err != nil {
		return IncidentContinuation{}, err
	}
	return IncidentContinuation{issued: &incidentContinuation{view: v.id, cursor: old}}, nil
}

// ImportIncidentContinuation spends the reopened view's ordinary source/output
// and cursor allowances. Root identity includes namespace, full descriptor and
// retained generation/index/image. Matching an ID or time alone is insufficient.
func (v *ReadView) ImportIncidentContinuation(ctx context.Context, p graphstate.IncidentPredicate, capsule IncidentContinuation) (graphstate.Cursor, error) {
	if v == nil || capsule.issued == nil {
		return 0, ErrInvalid
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	q, err := v.begin(ctx, graphstate.ReadBudget{})
	if err != nil {
		return 0, err
	}
	if v.route != nil {
		err = v.route.owner(q.q, uint64(p.Endpoint))
	}
	var hash [32]byte
	if err == nil {
		hash, err = v.incidentFingerprint(q, p)
	}
	old := capsule.issued.cursor
	if err == nil && (capsule.issued.view != v.id || old.kind != canonicalIncidentRecord || old.query != hash || old.relationship == 0 || old.bytes != 256 || old.current != nil) {
		err = ErrInvalid
	}
	if err == nil {
		err = q.q.materialize(256)
	}
	var token graphstate.Cursor
	if err == nil {
		token, err = v.storeCursor(0, fullContinuation{}, old)
	}
	if err = v.finish(q, err); err != nil {
		return 0, err
	}
	if err = v.output(256); err != nil {
		return 0, err
	}
	v.advanceCursor(0, token, fullContinuation{}, old)
	return token, nil
}
