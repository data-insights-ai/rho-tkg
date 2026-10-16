package graphapply

import (
	"errors"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

// Called only after complete immutable scope/configuration/command/proof checks.
// The observed owner supplies no credit or decision. Store must locate its exact
// earlier canonical terminal Use, prove the predecessor actually committed, and
// reclassify against the state after ordinary Stage/install/Restore completes.
func (m *declaredMaterializer) recoveryAfterRangeMove(q *reader) (raftlog.ApplicationRecoveryPurpose, error) {
	remaining := raftlog.ReadBudget{Rows: min(q.view.ReadLimits().Rows, q.limits.readRows-q.rows), Bytes: min(q.view.ReadLimits().Bytes, q.limits.readBytes-q.bytes)}
	owner, work, err := q.view.RecoveryOwner(q.ctx, remaining)
	if charged := chargeGraphWork(q, graphstore.PageWork{Records: work.Records, Bytes: work.Bytes}); charged != nil {
		return raftlog.ApplicationRecoveryPurpose{}, errors.Join(charged, err)
	}
	if err != nil {
		return raftlog.ApplicationRecoveryPurpose{}, err
	}
	owned, err := m.moveOwnsRecovery(q, owner)
	if err != nil || !owned {
		return raftlog.ApplicationRecoveryPurpose{}, errors.Join(errCorrupt, err)
	}
	return raftlog.ApplicationRecoveryPurpose{ApplyBefore: &raftlog.ApplicationRecoveryApplyBefore{Owner: owner, Refusal: errRangeMovePending}}, nil
}
