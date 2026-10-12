package graphapply

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"slices"

	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstate"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

func newCrossPartitionMaterializer(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits) (*declaredMaterializer, error) {
	if s == nil || s.ApplicationControls().Version != 1 || s.ApplicationRecovery().Version != 1 {
		return nil, errInvalid
	}
	return newDeclaredMaterializerWithAgreement(s, n, d, l, crossPartitionSemanticContractID())
}

func (m *declaredMaterializer) crossManifest(q *reader, wire []byte) (crossManifest, error) {
	manifest, err := decodeCrossManifest(wire, m.limits, q.arena)
	if err != nil {
		return manifest, err
	}
	cfg, err := m.initialized(q)
	if err != nil {
		return manifest, err
	}
	return manifest, m.validateCrossManifestBinding(manifest, cfg)
}

// Immutable configuration/namespace validation is separate from current graph
// catalog validation. Completed replay can inspect a supplied incoming-bank
// view without borrowing the active store's historical catalog.
func (m *declaredMaterializer) validateCrossManifestBinding(manifest crossManifest, cfg genesisAllocationConfig) error {
	digest, err := cfg.digest()
	if err != nil {
		return err
	}
	if manifest.configuration != digest || manifest.declaration != m.declaration.Digest() || manifest.routing != cfg.routingDigest || manifest.request.ns.graph != m.ns.graph || len(manifest.parts) != m.declaration.Len() {
		return errInvalid
	}
	for i, p := range manifest.parts {
		d, _ := m.declaration.PartitionAt(i)
		if p.binding.Partition != d.Partition || p.binding.OwnershipEpoch != d.OwnershipEpoch || p.binding.Graph != graphstate.GraphID(m.ns.graph) {
			return errInvalid
		}
	}
	if _, ok := m.declaration.Partition(manifest.coordinator); !ok {
		return errInvalid
	}
	return nil
}
func crossControl(q *reader, key []byte, max int) (raftlog.ApplicationControlRecord, bool, error) {
	remaining := min(q.limits.readBytes-q.bytes, q.view.ReadLimits().Bytes)
	if q.rows >= q.limits.readRows || remaining < 128+4*len(key) {
		return raftlog.ApplicationControlRecord{}, false, errLimit
	}
	if err := reserveComposer(q, 256+4*len(key)); err != nil {
		return raftlog.ApplicationControlRecord{}, false, err
	}
	r, found, w, err := q.view.GetControl(q.ctx, key, raftlog.ReadBudget{Rows: min(2, q.limits.readRows-q.rows), Bytes: min(remaining, 2*max+2048+8*len(key))})
	q.rows += w.Rows
	q.bytes += w.Bytes
	if err != nil {
		return raftlog.ApplicationControlRecord{}, false, err
	}
	if len(r.Value) > max {
		return raftlog.ApplicationControlRecord{}, false, errCorrupt
	}
	if err := reserveComposer(q, 128+cap(r.Key)+cap(r.Value)); err != nil {
		return raftlog.ApplicationControlRecord{}, false, err
	}
	return r, found, nil
}
func crossCurrent(q *reader, tx [16]byte) (crossStatus, bool, error) {
	wire, found, err := q.getBounded(crossKey(q.ns, tx), 256)
	if err != nil || !found {
		return crossStatus{}, found, err
	}
	s, err := decodeCrossStatus(wire)
	if err != nil || s.ns != q.ns || s.tx != tx || s.index > q.base.Index {
		return crossStatus{}, false, errors.Join(errCorrupt, err)
	}
	return s, true, nil
}
func crossLock(q *reader) ([16]byte, error) {
	wire, found, err := q.getBounded(crossLockKey(q.ns), 16)
	if err != nil {
		return [16]byte{}, err
	}
	if !found || len(wire) == 0 {
		return [16]byte{}, nil
	}
	if len(wire) != 16 {
		return [16]byte{}, errCorrupt
	}
	var tx [16]byte
	copy(tx[:], wire)
	if tx == ([16]byte{}) {
		return tx, errCorrupt
	}
	return tx, nil
}
func (m *declaredMaterializer) checkCrossLock(q *reader) error {
	tx, err := crossLock(q)
	if err != nil {
		return err
	}
	if tx == ([16]byte{}) {
		return nil
	}
	current, found, err := crossCurrent(q, tx)
	if err != nil || !found || current.phase < crossPrepare || current.phase == crossResolve {
		return errors.Join(errCorrupt, err)
	}
	r, found, err := crossControl(q, crossControlKey(q.ns, tx, 'R'), m.limits.commandBytes)
	if err != nil || !found || sha256.Sum256(r.Value) != current.digest {
		return errors.Join(errCorrupt, err)
	}
	if q.arena == nil {
		q.arena, err = graphstate.NewOutputBudget(m.limits.outputBytes)
		if err != nil {
			return err
		}
	}
	manifest, err := m.crossManifest(q, r.Value)
	if err != nil {
		return err
	}
	part, ok := manifest.part(q.ns.partition)
	if !ok {
		return errCorrupt
	}
	_, err = m.restoreCrossKernel(q, manifest, part)
	return err
}
func (m *declaredMaterializer) restoreCrossKernel(q *reader, manifest crossManifest, part crossPart) (*graphstore.PreparedPartitionKernel, error) {
	prepare, found, err := crossControl(q, crossControlKey(q.ns, manifest.tx, 'P'), 256)
	if err != nil || !found {
		return nil, errors.Join(errCorrupt, err)
	}
	status, err := decodeCrossStatus(prepare.Value)
	if err != nil || status.ns != q.ns || status.tx != manifest.tx || status.digest != manifest.digest || !status.yes || status.phase != crossPrepare || status.index != prepare.Index {
		return nil, errors.Join(errCorrupt, err)
	}
	proof := graphstore.PreparedKernelRestoreProof{PrepareKey: prepare.Key, PrepareDigest: sha256.Sum256(prepare.Value), PrepareIndex: prepare.Index}
	b := graphstore.OwnershipBudget{SourceRows: m.limits.sourceRows - q.rows, SourceBytes: m.limits.sourceBytes - q.bytes, OutputBytes: q.arena.Remaining()}
	k, w, err := graphstore.ReadPreparedPartitionKernel(q.ctx, q.view, proof, part.binding, m.limits.catalog, m.limits.graph, b, q.arena)
	if charge := chargeGraphWork(q, w); charge != nil {
		return nil, errors.Join(charge, err)
	}
	return k, err
}

func (m *declaredMaterializer) crossFresh(q *reader, manifest crossManifest, part crossPart, cfg genesisAllocationConfig) (reason, error) {
	identities := part.identities
	delta := graphstate.Delta{Entities: make([]graphstate.EntityRecord, len(identities.Entities)), Lives: identities.Lives, Values: make([]graphstate.ValueWrite, len(identities.Values))}
	if err := reserveComposer(q, 256+256*len(delta.Entities)+256*len(delta.Values)); err != nil {
		return reasonNone, err
	}
	for i, id := range identities.Entities {
		delta.Entities[i].ID = id
	}
	for i, id := range identities.Values {
		delta.Values[i].ID = id
	}
	a := allocationRecordReader{q: q, scope: m.scope(), config: cfg, declaration: m.declaration}
	return a.admitGraphDelta(manifest.request, delta)
}

func (m *declaredMaterializer) stageCrossPartition(q *reader, info graphstore.OwnershipRead, entry replica.Entry, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	if !crossGraphAgreement(m.SemanticContractID()) || info.Root().SemanticEpoch() == 0 {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	r, decoded, err := decodeCrossCommand(entry.Data, m.limits)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if r.ns != m.ns {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if err := joinGraphOutputBudget(q, graphRequest{}, decoded, m.limits); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	manifest, err := m.crossManifest(q, r.manifest)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := validateCrossPhaseProofs(r, manifest); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if r.conflict != nil {
		if err := m.checkCrossCapacityBindings(r.conflict, manifest); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	digest := sha256.Sum256(r.manifest)
	part, found := manifest.part(m.ns.partition)
	if !found {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	current, present, err := crossCurrent(q, manifest.tx)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if present && current.digest != digest {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	// Exact completed phases remain immutable, including a retry delivered after
	// later phases. No repeated control install, graph effect or round advance.
	if present && current.phase >= r.phase {
		if r.phase == crossDecide {
			record, ok, e := crossControl(q, crossControlKey(q.ns, manifest.tx, 'D'), 256)
			if e != nil || !ok {
				return raftlog.ApplicationBatch{}, errors.Join(errCorrupt, e)
			}
			current, e = decodeCrossStatus(record.Value)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
		}
		b := batchAt(q.base)
		b.Outcome, err = encodeCrossStatus(current)
		return b, err
	}
	floor, err := readPartitionRoundFloor(q, manifest.configuration)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	status := crossStatus{ns: m.ns, tx: manifest.tx, digest: digest, phase: r.phase, index: entry.Index, floor: floor}
	if r.phase == crossPrepare {
		status.floor, err = m.allocationFloor(q, manifest.configuration, floor)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	puts := []raftlog.ApplicationControlPut{}
	registration := func() error {
		if present {
			return nil
		}
		if err := reserveComposer(q, 128+len(r.manifest)); err != nil {
			return err
		}
		puts = append(puts, raftlog.ApplicationControlPut{Key: crossControlKey(q.ns, manifest.tx, 'R'), Value: owned(r.manifest)})
		return nil
	}
	var effects *graphstore.GraphEffects
	switch r.phase {
	case crossRegister:
		if m.ns.partition != manifest.coordinator || present || len(r.proofs) != 0 {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		if err := registration(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := writeCrossResourceOwner(q, manifest.tx, digest); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	case crossPrepare:
		position := -1
		for i, p := range manifest.parts {
			if p.binding.Partition == m.ns.partition {
				position = i
			}
		}
		if len(r.proofs) != position+1 {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		registered := r.proofs[0]
		if registered.ns.partition != manifest.coordinator || registered.ns.graph != m.ns.graph || registered.tx != manifest.tx || registered.digest != digest || registered.phase != crossRegister {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		for i, p := range r.proofs[1:] {
			if p.ns.partition != manifest.parts[i].binding.Partition || p.ns.graph != m.ns.graph || p.tx != manifest.tx || p.digest != digest || p.phase != crossPrepare || !p.yes {
				return raftlog.ApplicationBatch{}, errInvalid
			}
		}
		lock, e := crossLock(q)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		status.yes = true
		if lock != ([16]byte{}) && lock != manifest.tx || !bytes.Equal(q.base.Image, part.binding.CapturedBase.Image) || floor != part.floor {
			status.yes = false
			status.why = reasonReadConflict
		}
		if status.yes {
			cfg, e := m.initialized(q)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			why, e := m.crossFresh(q, manifest, part, cfg)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			if why != reasonNone {
				status.yes = false
				status.why = why
			}
		}
		if err := registration(); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		if err := writeCrossResourceOwner(q, manifest.tx, digest); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
		voteWire, e := encodeCrossStatus(status)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		puts = append(puts, raftlog.ApplicationControlPut{Key: crossControlKey(q.ns, manifest.tx, 'P'), Value: voteWire})
		if status.yes {
			if err := reserveComposer(q, 512+len(part.kernel)); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			puts = append(puts, raftlog.ApplicationControlPut{Key: crossControlKey(q.ns, manifest.tx, 'K'), Value: owned(part.kernel)})
			if err := q.put(crossLockKey(q.ns), manifest.tx[:]); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}
	case crossDecide:
		if !present || m.ns.partition != manifest.coordinator || r.conflict == nil && len(r.proofs) == 0 || len(r.proofs) > len(manifest.parts) {
			return raftlog.ApplicationBatch{}, replica.ErrUnavailable
		}
		status.commit = r.conflict == nil
		if r.conflict != nil {
			wire, e := encodeCrossCapacityObservation(*r.conflict)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			puts = append(puts, raftlog.ApplicationControlPut{Key: crossControlKey(q.ns, manifest.tx, 'A'), Value: wire})
		}
		maximum := manifest.request.revision.ID()
		for i, p := range r.proofs {
			if p.ns.graph != m.ns.graph || p.ns.partition != manifest.parts[i].binding.Partition || p.tx != manifest.tx || p.digest != digest || p.phase != crossPrepare {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			maximum = max(maximum, p.floor)
			if !p.yes {
				status.commit = false
			}
		}
		if status.commit && len(r.proofs) != len(manifest.parts) {
			return raftlog.ApplicationBatch{}, replica.ErrUnavailable
		}
		if status.commit {
			if maximum == math.MaxUint64 {
				return raftlog.ApplicationBatch{}, errLimit
			}
			status.round = maximum + 1
		}
		wire, e := encodeCrossStatus(status)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		puts = append(puts, raftlog.ApplicationControlPut{Key: crossControlKey(q.ns, manifest.tx, 'D'), Value: wire})
	case crossResolve:
		if !present || len(r.proofs) != 1 {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		decision := r.proofs[0]
		if decision.ns != manifest.request.ns || decision.tx != manifest.tx || decision.digest != digest || decision.phase != crossDecide || decision.commit && decision.round <= max(manifest.request.revision.ID(), floor) {
			return raftlog.ApplicationBatch{}, errInvalid
		}
		status.commit, status.round = decision.commit, decision.round
		lock, e := crossLock(q)
		if e != nil {
			return raftlog.ApplicationBatch{}, e
		}
		if decision.commit {
			if lock != manifest.tx {
				return raftlog.ApplicationBatch{}, errCorrupt
			}
			kernel, e := m.restoreCrossKernel(q, manifest, part)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			revision, e := manifest.revision(decision.round)
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			effect, w, e := graphstore.StagePreparedPartitionKernel(q.ctx, q.view, kernel, revision, m.limits.catalog, m.limits.graph, graphstore.OwnershipBudget{SourceRows: m.limits.sourceRows - q.rows, SourceBytes: m.limits.sourceBytes - q.bytes, OutputBytes: q.arena.Remaining()}, q.arena)
			if charge := chargeGraphWork(q, w); charge != nil {
				return raftlog.ApplicationBatch{}, errors.Join(charge, e)
			}
			if e != nil {
				return raftlog.ApplicationBatch{}, e
			}
			effects = &effect
			if err := writePartitionRoundFloor(q, manifest.configuration, decision.round); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}
		if lock == manifest.tx {
			if err := q.put(crossLockKey(q.ns), nil); err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		}
		if err := q.put(crossResourceOwnerKey(q.ns), nil); err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	wire, err := encodeCrossStatus(status)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := q.put(crossKey(q.ns, manifest.tx), wire); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	b := batchAt(q.base)
	b.Writes = q.writes
	b.Outcome = wire
	b.ControlPuts = puts
	if effects != nil {
		b, err = m.composeCrossEffects(q, manifest, status, *effects, b, budget)
		if err != nil {
			return raftlog.ApplicationBatch{}, err
		}
	}
	slices.SortFunc(b.ControlPuts, func(a, b raftlog.ApplicationControlPut) int { return bytes.Compare(a.Key, b.Key) })
	if err := sortPartitionWrites(b.Writes); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	purpose, err := m.crossRecovery(q, r, manifest)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	b.RecoveryReserve, b.RecoveryUse = purpose.Reserve, purpose.Use
	if err := m.preflight(b, budget); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	return b, nil
}

func (m *declaredMaterializer) composeCrossEffects(q *reader, manifest crossManifest, status crossStatus, effect graphstore.GraphEffects, b raftlog.ApplicationBatch, budget raftlog.ApplicationBudget) (raftlog.ApplicationBatch, error) {
	changes := graphChanges{ns: m.ns, entities: effect.Delta.Entities, lives: effect.Delta.Lives, values: effect.Delta.Values, groups: effect.Groups}
	if err := reserveLogicalEncoding(q, changes, m.limits); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	logical, err := encodeTypedGraphChanges(changes, m.limits, true)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if err := reserveComposer(q, 4096+8*len(logical)+128*(len(b.Writes)+len(effect.Writes))); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	root, err := effect.Root.AdvanceEffects(graphEffectDigest(effect.Root.EffectDigest(), logical))
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	b.Image, err = graphstore.EncodeRoot(root)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	outcome := outcome{ns: m.ns, kind: graphOperations, identity: manifest.tx, hash: status.digest, index: status.index, disposition: applied}
	b.Changes, err = encodeChangeEnvelope(outcome, logical, m.limits)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	b.Writes = append(b.Writes, effect.Writes...)
	return m.finishPartitionRoundMap(q, b, manifest.configuration, status.round, status.index, budget, 0)
}
