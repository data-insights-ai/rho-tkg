package graphapply

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"

	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
	"github.com/data-insights-ai/rho-tkg/v5/internal/replica"
)

var errRegisteredStageUnavailable = errors.New("registered committed command Stage unavailable")

type registeredMaterializer struct {
	store  *raftlog.Store
	source registeredGenesisSource
	limits registeredOperationLimits
	origin registeredOrigin
}

var _ replica.ApplicationMachine = (*registeredMaterializer)(nil)
var _ replica.ApplicationSemanticProvider = (*registeredMaterializer)(nil)

func newRegisteredMaterializer(s *raftlog.Store, l registeredOperationLimits) (*registeredMaterializer, error) {
	if err := l.reserve(s, 0); err != nil {
		return nil, err
	}
	usage, err := s.ApplicationControlUsage()
	if err != nil {
		return nil, err
	}
	var source registeredGenesisSource
	var origin registeredOrigin
	if usage.Records == 0 && usage.Bytes == 0 {
		source, err = captureRegisteredGenesisSource(s, l)
	} else {
		_, _, origin, err = registeredCurrentState(s, l, 0)
		source = origin.source
		if err == nil && origin.index == 0 {
			err = errInvalid
		}
	}
	if err != nil {
		return nil, err
	}
	return &registeredMaterializer{store: s, source: source, limits: l, origin: origin}, nil
}

func (m *registeredMaterializer) SemanticContractID() raftlog.ApplicationSemanticContractID {
	if m == nil {
		return raftlog.ApplicationSemanticContractID{}
	}
	return registeredSemanticContractID()
}
func (m *registeredMaterializer) Restore(index uint64, image []byte) error {
	if m == nil || m.store == nil || index == 0 {
		return errInvalid
	}
	base, root, origin, err := registeredCurrentState(m.store, m.limits, cap(image))
	if err != nil {
		return err
	}
	if base.Index != index || !bytes.Equal(image, base.Image) || base.ImageHash != m.source.ImageHash || root.OwnershipEpoch() != m.source.InitialOwnerEpoch || origin.index != 0 && origin.source != m.source || m.origin.index != 0 && origin != m.origin {
		return errInvalid
	}
	m.origin = origin
	return nil
}
func (m *registeredMaterializer) Stage(entry replica.Entry, budget raftlog.ApplicationBudget) (batch raftlog.ApplicationBatch, err error) {
	if m == nil || m.store == nil || entry.Term == 0 || budget.Writes < 1 || budget.Bytes < 1 || budget.ImageBytes < 1 || budget.ChangeBytes < 1 || budget.OutcomeBytes < 1 {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	if len(entry.Data) > (4<<20)+11 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	base, root, origin, err := registeredCurrentState(m.store, m.limits, cap(entry.Data))
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if base.Index == math.MaxUint64 || entry.Index != base.Index+1 || entry.Generation != base.Generation || base.ImageHash != m.source.ImageHash || root.OwnershipEpoch() != m.source.InitialOwnerEpoch || origin.index != 0 && origin.source != m.source || m.origin.index != 0 && origin != m.origin {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	term, err := m.store.Term(entry.Index)
	if err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	if term != entry.Term {
		return raftlog.ApplicationBatch{}, errInvalid
	}
	policy := m.store.ApplicationLimits()
	policy.MaxInstallWrites = min(policy.MaxInstallWrites, budget.Writes)
	policy.MaxInstallBytes = min(policy.MaxInstallBytes, budget.Bytes)
	policy.MaxImageBytes = min(policy.MaxImageBytes, budget.ImageBytes)
	policy.MaxChangeBytes = min(policy.MaxChangeBytes, budget.ChangeBytes)
	policy.MaxOutcomeBytes = min(policy.MaxOutcomeBytes, budget.OutcomeBytes)
	if policy.MaxImageBytes < 140 {
		return raftlog.ApplicationBatch{}, errLimit
	}
	batch = batchAt(base)
	if len(entry.Data) != 0 {
		if len(entry.Data) < 11 || binary.BigEndian.Uint16(entry.Data[4:6]) != 1 || entry.Data[6] != 0 || uint64(binary.BigEndian.Uint32(entry.Data[7:11])) != uint64(len(entry.Data)-11) { // #nosec G115 -- prior4MiB+11 ceiling and left len>=11 guard bound remainder0..4MiB before widening.
			return raftlog.ApplicationBatch{}, errInvalid
		}
		switch string(entry.Data[:4]) {
		case "GJQ1":
			source, err := decodeGenesisCommand(entry.Data, registeredJournalBudget{walkBytes: 3059})
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
			if source != m.source {
				return raftlog.ApplicationBatch{}, errInvalid
			}
			// Admit actual narrowed output/install caps before allocating any GJQ output.
			key, _ := registeredGenesisIdentity(source)
			physical := len(key) + bytes.Count(key[:], []byte{0}) + 11
			work := 2*420 + 140 + 1024
			if origin.index == 0 {
				work += 2*(physical+36+378) + 64 + 128 + 4*physical
			}
			if policy.MaxOutcomeBytes < 145 || policy.MaxInstallBytes < work || origin.index == 0 && (policy.MaxKeyBytes < 47 || policy.MaxValueBytes < 378 || policy.MaxInstallWrites < 1) {
				return raftlog.ApplicationBatch{}, errLimit
			}
			status := byte(5)
			if origin.index == 0 {
				key, value, err := encodeRegisteredOrigin(source, entry.Index, entry.Term, registeredJournalBudget{4086})
				if err != nil {
					return raftlog.ApplicationBatch{}, err
				}
				batch.ControlPuts = []raftlog.ApplicationControlPut{{Key: key, Value: value}}
				origin = registeredOrigin{source, entry.Index, entry.Term}
				status = 4
			}
			batch.Outcome, err = encodeRegisteredGenesisOutcome(source, origin.index, origin.term, entry.Index, status, registeredJournalBudget{3340})
			if err != nil {
				return raftlog.ApplicationBatch{}, err
			}
		case "RJQ1":
			// Recognized envelope is unsupported; no Register acceptance or effect.
			return raftlog.ApplicationBatch{}, errRegisteredStageUnavailable
		default:
			return raftlog.ApplicationBatch{}, errInvalid
		}
	}
	if _, err := policy.PreflightControlBatch(batch, m.store.Limits()); err != nil {
		return raftlog.ApplicationBatch{}, err
	}
	// The actual Driver owns InstallApplication/Restore and durable advancement.
	return batch, nil
}
