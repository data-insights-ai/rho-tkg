package graphapply

import (
	"crypto/sha256"
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// This explicitly selected agreement adds bounded graph-page composition to
// B23; it never upgrades an existing store or changes the B23 hash/encoding.
func rangeMoveGraphBatchSemanticContractID() raftlog.ApplicationSemanticContractID {
	base := rangeMoveSpanSemanticContractID()
	return raftlog.ApplicationSemanticContractID(sha256.Sum256(append(base[:], []byte("graph=bounded-native-rmp-batches32-component8-family-boundary-v1\x00progress=gmp5-native-graph-count-v1\x00")...)))
}
func rangeMoveGraphBatches(id raftlog.ApplicationSemanticContractID) bool {
	return id == rangeMoveGraphBatchSemanticContractID()
}
func newRangeMoveGraphBatchMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationRecovery().Version != 3 || s.ApplicationRetention().Version != 1 || s.ApplicationControls().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, rangeMoveGraphBatchSemanticContractID())
}

func moveGraphCountValid(batches, logical uint64) bool {
	return batches == 0 && logical == 0 || batches > 0 && batches <= rangeMoveSpanMaxPages && logical >= batches && logical <= graphstore.RangeMoveBatchMaxPages*batches
}
func moveBodyKindValid(id raftlog.ApplicationSemanticContractID, kind byte) bool {
	if rangeMoveGraphBatches(id) {
		return kind == 3 || kind == 4
	}
	if rangeMoveSpans(id) {
		return kind == 1 || kind == 3
	}
	return kind == 1 || kind == 2
}

func (m *declaredMaterializer) exportMoveGraphBatch(q *reader, p rangeMoveProgress, before graphstore.RangeMoveCursor, intent rangeMoveIntent) (batch graphstore.RangeMoveBatch, err error) {
	limits := m.limits.graph
	limits.MaxSourceRows = min(limits.MaxSourceRows, q.limits.readRows-q.rows)
	limits.MaxSourceBytes = min(limits.MaxSourceBytes, q.limits.readBytes-q.bytes)
	if !rangeMoveGraphBatches(m.SemanticContractID()) || limits.MaxSourceRows < 1 || limits.MaxSourceBytes < 1 {
		return batch, errLimit
	}
	if err = reserveComposer(q, 2048); err != nil {
		return batch, err
	}
	view, err := m.store.ApplicationView(p.descriptor.Source.Index)
	if err != nil {
		return batch, err
	}
	defer func() {
		err = errors.Join(err, view.Close())
		if err != nil {
			batch = graphstore.RangeMoveBatch{}
		}
	}()
	batch, work, err := graphstore.ExportRangeMoveBatch(q.ctx, view, p.descriptor, before, raftlog.ReadBudget{Rows: intent.pageRows, Bytes: intent.pageBytes}, m.limits.catalog, limits, q.arena)
	return batch, errors.Join(err, chargeGraphWork(q, work))
}
