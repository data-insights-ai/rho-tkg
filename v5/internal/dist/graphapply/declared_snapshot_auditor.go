package graphapply

import (
	"context"
	"errors"
	"sync"

	"github.com/data-insights-ai/rho-tkg/v5/internal/dist/replica"
	"github.com/data-insights-ai/rho-tkg/v5/internal/graphstore"
	"github.com/data-insights-ai/rho-tkg/v5/internal/idalloc"
	"github.com/data-insights-ai/rho-tkg/v5/internal/raftlog"
)

const declaredAuditFixedBytes uint64 = 4096

// This immutable adapter grants semantic inspection only. It is not an
// ApplicationMachine; its private validation configuration has no live Store,
// and no constructor, Restore, Stage, Driver or Host is invoked.
type declaredSnapshotAuditor struct {
	validation declaredMaterializer
	contract   raftlog.ApplicationContract
	advance    replica.ApplicationAuditBudget
	poolPolicy raftlog.ApplicationPolicy
}

var _ replica.ApplicationSnapshotAuditor = (*declaredSnapshotAuditor)(nil)

func newDeclaredSnapshotAuditor(s *raftlog.Store, n namespace, d graphstore.OwnershipDeclaration, l materializerLimits, agreement raftlog.ApplicationSemanticContractID) (*declaredSnapshotAuditor, error) {
	if s == nil || !n.valid() || idalloc.GraphID(d.Graph()) != n.graph || agreement != declaredSemanticContractID() && agreement != graphGenesisSemanticContractID() && !partitionGraphAgreement(agreement) {
		return nil, errInvalid
	}
	if rangeMoveSpans(agreement) {
		recovery := uint32(3)
		if rangeMovePoolsEnabled(agreement) {
			recovery = 4
		}
		if s.ApplicationControls().Version != 1 || s.ApplicationRecovery().Version != recovery || s.ApplicationRetention().Version != 1 {
			return nil, errInvalid
		}
	} else if crossGraphAgreement(agreement) && (s.ApplicationControls().Version != 1 || s.ApplicationRecovery().Version != 1) {
		return nil, errInvalid
	}
	if err := l.validate(); err != nil {
		return nil, err
	}
	entry, found := d.Partition(n.partition)
	policy := s.ApplicationLimits()
	if !found || !policy.Enabled() {
		return nil, errInvalid
	}
	binding := s.ApplicationBinding()
	if err := binding.Validate(); err != nil || binding.SemanticContractID != agreement {
		return nil, errors.Join(errInvalid, err)
	}
	if binding.Identity.Graph != [16]byte(n.graph) || binding.Identity.Partition != n.partition || binding.Identity.Group != entry.Group {
		return nil, graphstore.ErrNamespace
	}
	if policy.MaxImageBytes > l.sourceBytes {
		return nil, errLimit
	}
	var contract raftlog.ApplicationContract
	var err error
	if rangeMoveSpans(agreement) {
		contract, err = raftlog.ApplicationContractForPolicyWithRetention(policy, s.ApplicationControls(), s.ApplicationRecovery(), s.ApplicationRetention())
	} else {
		contract, err = raftlog.ApplicationContractForPolicyWithRecovery(policy, s.ApplicationControls(), s.ApplicationRecovery())
	}
	if err != nil {
		return nil, err
	}
	readBytes := l.sourceBytes
	// Validation has at most three initialized-catalog traversals: the direct
	// one, a completion owner and a YES lock. Each can inspect one historic root
	// and one range-move baseline. RootAt's complete source ceiling is
	// 1082+4*policyImage; opening a baseline additionally hashes/copies its frame.
	// Existing reader/composer ledgers bound semantic/decode and owned work.
	advance := replica.ApplicationAuditBudget{
		WorkBytes:  declaredAuditFixedBytes + uint64(readBytes) + 6*(4096+4*uint64(policy.MaxImageBytes)),
		OwnedBytes: declaredAuditFixedBytes + uint64(readBytes) + uint64(l.outputBytes) + 4*172,
	} // #nosec G115 -- limits/policy are validated positive <=64 MiB; six fixed terms and sums fit uint64.
	return &declaredSnapshotAuditor{validation: declaredMaterializer{ns: n, owner: entry.OwnershipEpoch, declaration: d, binding: binding, limits: l, agreement: agreement}, contract: contract, advance: advance, poolPolicy: policy}, nil
}

func declaredAuditAdmit(b, required replica.ApplicationAuditBudget) error {
	if b.WorkBytes == 0 || b.OwnedBytes == 0 {
		return errors.Join(errInvalid, replica.ErrInvalid)
	}
	if b.WorkBytes < required.WorkBytes || b.OwnedBytes < required.OwnedBytes {
		return errors.Join(errLimit, replica.ErrLimit)
	}
	return nil
}

// Context is the embedding's already authenticated snapshot input. It is not
// an independently authenticated Raft message; Claim/Ready still decide whether
// this same prepared handle may activate. All audit reads use that handle.
func (a *declaredSnapshotAuditor) BeginApplicationSnapshotAudit(p *raftlog.PreparedApplicationSnapshot, c replica.ApplicationSnapshotContext, b replica.ApplicationAuditBudget) (replica.ApplicationSnapshotAudit, error) {
	if a == nil || p == nil || c.Index == 0 || c.Term == 0 || c.DescriptorHash == [32]byte{} || c.CutID == [32]byte{} || c.ManifestID == [32]byte{} || c.Voters[0] == 0 || c.Voters[0] >= c.Voters[1] || c.Voters[1] >= c.Voters[2] {
		return nil, errInvalid
	}
	required := replica.ApplicationAuditBudget{WorkBytes: declaredAuditFixedBytes + 1024 + 4*uint64(a.contract.MaxImageBytes), OwnedBytes: declaredAuditFixedBytes + 1024 + 2*172} // #nosec G115 -- constructor captures actual validated contract image1..64 MiB.
	if err := declaredAuditAdmit(b, required); err != nil {
		return nil, err
	}
	contract, err := p.ApplicationContract(context.Background())
	if err != nil {
		return nil, err
	}
	if contract != a.contract {
		return nil, errInvalid
	}
	v, err := p.OpenReadView(context.Background(), c.Index, 172)
	if err != nil {
		return nil, err
	}
	binding, err := v.ApplicationBinding(context.Background())
	if err != nil || binding != a.validation.binding {
		return nil, errors.Join(errInvalid, err, v.Close())
	}
	owner := &declaredSnapshotAudit{auditor: a, prepared: p, view: v, snapshot: c}
	owner.self = owner
	return owner, nil
}

type declaredSnapshotAudit struct {
	mu                sync.Mutex
	self              *declaredSnapshotAudit
	auditor           *declaredSnapshotAuditor
	prepared          *raftlog.PreparedApplicationSnapshot
	view              *raftlog.ApplicationView
	snapshot          replica.ApplicationSnapshotContext
	validated         bool
	baselineValidated bool
	poolAudit         movePoolAudit
	sealed            bool
	closed            bool
	closeErr          error
}

var _ replica.ApplicationSnapshotAudit = (*declaredSnapshotAudit)(nil)

func (a *declaredSnapshotAudit) Advance(ctx context.Context, b replica.ApplicationAuditBudget) (bool, error) {
	if a == nil || a.self != a || ctx == nil {
		return false, errInvalid
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return false, raftlog.ErrClosed
	}
	if a.sealed {
		if err := declaredAuditAdmit(b, replica.ApplicationAuditBudget{WorkBytes: declaredAuditFixedBytes, OwnedBytes: declaredAuditFixedBytes}); err != nil {
			return false, err
		}
		return true, nil
	}
	if a.auditor == nil || a.view == nil || a.prepared == nil {
		return false, errInvalid
	}
	if err := declaredAuditAdmit(b, a.auditor.advance); err != nil {
		return false, err
	}
	base, err := a.view.RootBounded(ctx, 172)
	if err != nil {
		return false, err
	}
	if base.Index != a.snapshot.Index || len(base.Image) != 172 {
		return false, errCorrupt
	}
	m := &a.auditor.validation
	l := m.limits.allocation
	l.readRows, l.readBytes = min(l.readRows, m.limits.sourceRows), min(l.readBytes, m.limits.sourceBytes)
	q := reader{ctx: ctx, view: a.view, preparedHistory: a.prepared, ns: m.ns, base: base, limits: l, bytes: cap(base.Image)}
	if !a.baselineValidated {
		if err := m.validateRestoredState(&q, true); err != nil {
			return false, err
		}
		a.baselineValidated = true
		if rangeMovePoolsEnabled(m.SemanticContractID()) {
			return false, nil
		}
	}
	if rangeMovePoolsEnabled(m.SemanticContractID()) {
		if err = joinGraphOutputBudget(&q, graphRequest{}, 2048, m.limits); err != nil {
			return false, err
		}
		next := a.poolAudit
		done, e := next.advance(&q, m, a.auditor.poolPolicy)
		if e != nil {
			return false, e
		}
		a.poolAudit = next
		if !done {
			return false, nil
		}
	}
	a.validated = true
	return true, nil
}

// Finish releases the last prepared reader before any Claim can be attempted.
// Historical baselines close within the shared validator; no graph reader,
// prepared/configuration/declaration or image reference remains after success.
func (a *declaredSnapshotAudit) Finish(ctx context.Context, b replica.ApplicationAuditBudget) error {
	if a == nil || a.self != a || ctx == nil {
		return errInvalid
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return raftlog.ErrClosed
	}
	if err := declaredAuditAdmit(b, replica.ApplicationAuditBudget{WorkBytes: declaredAuditFixedBytes, OwnedBytes: declaredAuditFixedBytes}); err != nil {
		return err
	}
	if a.sealed {
		return a.closeErr
	}
	if !a.validated || a.view == nil {
		return errInvalid
	}
	a.releaseLocked()
	if a.closeErr == nil {
		a.sealed = true
	}
	return a.closeErr
}

func (a *declaredSnapshotAudit) releaseLocked() {
	if a.view != nil {
		a.closeErr = errors.Join(a.closeErr, a.view.Close())
		a.view = nil
	}
	a.auditor, a.prepared = nil, nil
	a.poolAudit = movePoolAudit{}
}

func (a *declaredSnapshotAudit) Close() error {
	if a == nil || a.self != a {
		return errInvalid
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.closed {
		a.releaseLocked()
		a.closed = true
	}
	return a.closeErr
}
