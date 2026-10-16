package graphapply

import (
	"crypto/sha256"
	"errors"
)

// Capacity classification checks immutable intent and proof shape. Current
// capture/progress/decision dependencies remain Stage's responsibility; Store
// grants Use only against the exact genuine applied or earlier queued Reserve.
func (m *declaredMaterializer) checkRangeMoveRecoveryBinding(q *reader, c rangeMoveCommand, r rangeMoveIntent) error {
	cfg, err := m.readGenesisConfig(q)
	if err != nil {
		return errors.Join(errInvalid, err)
	}
	configuration, err := cfg.digest()
	if err != nil || configuration != r.publication.Configuration || r.source.check(m.declaration, cfg) != nil || r.target.check(m.declaration, cfg) != nil || c.scope != m.scope() {
		return errors.Join(errInvalid, err)
	}
	move := rangeMoveID(namespace{r.source.graph, r.source.partition}, r.id)
	digest := sha256.Sum256(c.intent)
	var seen [2]bool
	for _, p := range c.proofs {
		position := 0
		if p.ns == (namespace{r.target.graph, r.target.partition}) {
			position = 1
		} else if p.ns != (namespace{r.source.graph, r.source.partition}) {
			return errInvalid
		}
		if seen[position] || p.move != move || p.intent != digest || p.result != rangeMoveApplied || p.sequence > uint64(r.maxPages) { // #nosec G115 -- decoded intent bounds maxPages1..1024.
			return errInvalid
		}
		seen[position] = true
	}
	proof := func(partition uint64, first, last rangeMovePhase) bool {
		p, found := rangeMoveFindProof(c.proofs, partition)
		return found && p.phase >= first && p.phase <= last
	}
	valid := false
	switch c.phase {
	case rangeMoveFence:
		valid = c.sequence == 0 && len(c.data) == 0 && (c.scope == r.source && len(c.proofs) == 0 || c.scope == r.target && proof(r.source.partition, rangeMoveFence, rangeMoveDescribe))
	case rangeMoveCapture:
		valid = c.sequence == 0 && len(c.data) == 0 && len(c.proofs) == 0
	case rangeMoveDescribe:
		valid = c.scope == r.source && (c.sequence == 0 && len(c.data) != 0 && len(c.proofs) == 1 && proof(r.target.partition, rangeMoveCapture, rangeMoveCapture) || c.sequence > 0 && len(c.data) == 0 && len(c.proofs) == 0)
	case rangeMoveImport:
		preparation := c.sequence > 0 && len(c.data) == 1 && c.data[0] == 0 && len(c.proofs) == 0
		valid = c.scope == r.target && len(c.data) != 0 && (preparation || proof(r.source.partition, rangeMoveDescribe, rangeMoveDescribe) && (c.sequence > 0 || proof(r.target.partition, rangeMoveCapture, rangeMoveCapture)))
	case rangeMoveSeal:
		valid = c.scope == r.target && c.sequence == 0 && len(c.data) == 0 && proof(r.source.partition, rangeMoveDescribe, rangeMoveDescribe)
	case rangeMoveDecide:
		valid = c.scope == r.source && c.sequence == 0 && len(c.data) == 0 && proof(r.target.partition, rangeMoveSeal, rangeMoveSeal)
	case rangeMoveActivate:
		p, found := rangeMoveFindProof(c.proofs, r.source.partition)
		valid = c.sequence == 0 && len(c.data) == 0 && found && p.phase >= rangeMoveDecide && p.phase <= rangeMoveFinish && p.round > 0 && p.digest != ([32]byte{})
	case rangeMoveFinish:
		left, leftFound := rangeMoveFindProof(c.proofs, r.source.partition)
		right, rightFound := rangeMoveFindProof(c.proofs, r.target.partition)
		valid = c.sequence == 0 && len(c.data) == 0 && len(c.proofs) == 2 && leftFound && rightFound && left.phase >= rangeMoveActivate && left.phase <= rangeMoveFinish && right.phase >= rangeMoveActivate && right.phase <= rangeMoveFinish && left.round > 0 && left.round == right.round && left.digest != ([32]byte{}) && left.digest == right.digest
	case rangeMoveAbort:
		p, found := rangeMoveFindProof(c.proofs, r.source.partition)
		valid = len(c.data) == 0 && (c.scope == r.source && c.sequence == 0 && len(c.proofs) == 0 || c.scope == r.target && found && p.phase == rangeMoveAbort && p.sequence == 0 && p.round == 0)
	}
	if !valid {
		return errInvalid
	}
	return nil
}
